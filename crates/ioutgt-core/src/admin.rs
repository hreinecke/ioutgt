//! Admin command handlers: Identify, Features, Log pages, Keep Alive,
//! Async Event Requests. Values mirror kernel nvmet where interop
//! depends on them.

use std::rc::Rc;
use std::sync::Arc;

use ioutgt_nvme::fabrics::{self, DiscoveryLogEntry, DiscoveryLogHeader};
use ioutgt_nvme::identify::{
    IdentifyController, IdentifyNamespace, SGLS_BYTE_ALIGNED, SGLS_KEYED, SGLS_SAOS, cmic, nmic,
    oncs,
};
use ioutgt_nvme::spec::{Sqe, admin_opcode, cns, feat, log_page};
use ioutgt_nvme::status;
use tracing::debug;
use zerocopy::IntoBytes;

use crate::backend::Backend;
use crate::dispatch::{AdminState, ConnCtx, Outcome};
use crate::subsystem::Subsystem;

/// KAS granularity: 10 seconds in 100ms units, as nvmet.
const KAS_UNITS: u16 = 100;

fn ascii_pad(dst: &mut [u8], src: &str) {
    dst.fill(b' ');
    let n = src.len().min(dst.len());
    dst[..n].copy_from_slice(&src.as_bytes()[..n]);
}

/// Route one admin-queue command to its handler.
pub async fn execute<B: Backend>(
    ctx: &Rc<ConnCtx<B>>,
    admin: &AdminState<B>,
    tag: u16,
    sqe: &Sqe,
) -> Outcome {
    match sqe.opcode {
        admin_opcode::IDENTIFY => identify(ctx, admin, tag, sqe),
        admin_opcode::GET_FEATURES => get_features(ctx, admin, sqe),
        admin_opcode::SET_FEATURES => set_features(ctx, admin, sqe),
        admin_opcode::GET_LOG_PAGE => get_log_page(ctx, admin, tag, sqe),
        admin_opcode::KEEP_ALIVE => Outcome::status(ctx.cqe(0, sqe.cid.get(), status::SUCCESS)),
        admin_opcode::ASYNC_EVENT => {
            // Task-per-tag parking: this future resolves only when an
            // event fires, so the AER occupies its slot until then.
            let result = std::future::poll_fn(|cx| {
                if admin.closing.get() {
                    // Teardown: resolve with a dummy event; the response
                    // is never sent (the connection is gone).
                    return std::task::Poll::Ready(0);
                }
                if let Some(event) = admin.events.borrow_mut().pop_front() {
                    return std::task::Poll::Ready(event);
                }
                admin.aer_wakers.borrow_mut().push(cx.waker().clone());
                std::task::Poll::Pending
            })
            .await;
            Outcome::status(ctx.cqe(result, sqe.cid.get(), status::SUCCESS))
        }
        _ => {
            debug!(opcode = sqe.opcode, "unsupported admin command");
            Outcome::status(ctx.cqe(0, sqe.cid.get(), status::INVALID_OPCODE | status::DNR))
        }
    }
}

/// Copy `data` into a freshly leased slot buffer, capped at the admin
/// data limit (the admin pool is sized so this lease never blocks).
fn fill_slot<B: Backend>(ctx: &Rc<ConnCtx<B>>, tag: u16, data: &[u8]) -> u32 {
    let n = data.len().min(crate::ADMIN_DATA_MAX);
    ctx.queue.lease_or_owned(tag, n.max(1));
    let slot = ctx.queue.slot(tag);
    slot.data().write_at(0, &data[..n]);
    u32::try_from(n).expect("slot buffers < 4G")
}

fn identify<B: Backend>(
    ctx: &Rc<ConnCtx<B>>,
    admin: &AdminState<B>,
    tag: u16,
    sqe: &Sqe,
) -> Outcome {
    let cid = sqe.cid.get();
    let which = (sqe.cdw10.get() & 0xFF) as u8;
    match which {
        cns::CONTROLLER => {
            let id = build_id_ctrl(ctx, admin);
            let len = fill_slot(ctx, tag, id.as_bytes());
            Outcome::with_data(ctx.cqe(0, cid, status::SUCCESS), len)
        }
        cns::NAMESPACE => {
            let subsys = admin.subsys.borrow();
            let Some(subsys) = subsys.as_ref() else {
                return Outcome::status(ctx.cqe(0, cid, status::INVALID_NS | status::DNR));
            };
            let table = subsys.snapshot();
            match table.get(&sqe.nsid.get()) {
                Some(ns) => {
                    let id = build_id_ns(ns.backend.as_ref());
                    let len = fill_slot(ctx, tag, id.as_bytes());
                    Outcome::with_data(ctx.cqe(0, cid, status::SUCCESS), len)
                }
                // Inactive NSID: all-zero structure, per spec.
                None if sqe.nsid.get() <= subsys.max_nsid() => {
                    let id = IdentifyNamespace::zeroed();
                    let len = fill_slot(ctx, tag, id.as_bytes());
                    Outcome::with_data(ctx.cqe(0, cid, status::SUCCESS), len)
                }
                None => Outcome::status(ctx.cqe(0, cid, status::INVALID_NS | status::DNR)),
            }
        }
        cns::ACTIVE_NS_LIST => {
            let mut list = [0u8; 4096];
            if let Some(subsys) = admin.subsys.borrow().as_ref() {
                let start = sqe.nsid.get();
                let table = subsys.snapshot();
                for (i, nsid) in table.keys().filter(|&&n| n > start).take(1024).enumerate() {
                    list[i * 4..i * 4 + 4].copy_from_slice(&nsid.to_le_bytes());
                }
            }
            let len = fill_slot(ctx, tag, &list);
            Outcome::with_data(ctx.cqe(0, cid, status::SUCCESS), len)
        }
        cns::NS_DESC_LIST => {
            let mut desc = [0u8; 4096];
            let nsid = sqe.nsid.get();
            let uuid = admin
                .subsys
                .borrow()
                .as_ref()
                .and_then(|s| s.snapshot().get(&nsid).map(|ns| ns.uuid));
            match uuid {
                Some(uuid) => {
                    desc[0] = 3; // NIDT: UUID
                    desc[1] = 16; // NIDL
                    desc[4..20].copy_from_slice(&uuid);
                    let len = fill_slot(ctx, tag, &desc);
                    Outcome::with_data(ctx.cqe(0, cid, status::SUCCESS), len)
                }
                None => Outcome::status(ctx.cqe(0, cid, status::INVALID_NS | status::DNR)),
            }
        }
        _ => Outcome::status(ctx.cqe(0, cid, status::INVALID_FIELD | status::DNR)),
    }
}

fn build_id_ctrl<B: Backend>(
    ctx: &Rc<ConnCtx<B>>,
    admin: &AdminState<B>,
) -> Box<IdentifyController> {
    let mut id = Box::new(IdentifyController::zeroed());
    let discovery = admin.discovery.get();
    let subsys = admin.subsys.borrow();

    id.vid.set(0);
    id.ssvid.set(0);
    ascii_pad(&mut id.fr, "1.0");
    ascii_pad(&mut id.mn, "ioutgt");
    match subsys.as_ref() {
        Some(s) => {
            ascii_pad(&mut id.sn, &s.serial);
            ascii_pad(&mut id.subnqn, &s.nqn);
            // subnqn is NUL-terminated, not space-padded.
            nul_terminate(&mut id.subnqn, &s.nqn);
        }
        None => {
            ascii_pad(&mut id.sn, "ioutgt-disc");
            nul_terminate(&mut id.subnqn, fabrics::DISCOVERY_NQN);
        }
    }
    id.cntlid.set(admin.cntlid.get());
    id.ver.set(0x0001_0300);
    // OAES: the host masks its AEC against this; without the NS_ATTR
    // bit it never enables namespace-change notices.
    id.oaes.set(crate::AEN_CFG_NS_ATTR);
    id.cntrltype = if discovery { 2 } else { 1 };
    if !discovery {
        // Advertise multi-controller capability so the host's NVMe-multipath
        // layer builds a namespace head plus a per-controller path device
        // (/dev/nvmeXcYnZ), as it does for kernel nvmet. ANA (CMIC bit 3) is
        // deliberately left clear — we serve no ANA log page. Discovery
        // controllers have no namespaces, so (like nvmet) they advertise no
        // CMIC.
        id.cmic = cmic::MULTI_CTRL;
    }
    id.kas.set(KAS_UNITS);
    id.sqes = 0x66;
    id.cqes = 0x44;
    // Advertise the configured IO queue-depth ceiling, not the admin
    // queue's size: the host clamps every IO queue down to MAXCMD, so
    // pinning it to the admin depth (NVME_AQ_DEPTH = 32) would cap IO
    // queues there too.
    id.maxcmd.set(ctx.port.io_queue_size);
    id.acl = 3;
    id.aerl = 3;
    // MDTS: slot buffer / CAP.MPSMIN(4K) pages: 128K = 2^5 * 4K.
    id.mdts = 5;
    // RDMA hosts require keyed SGL support (the command capsule carries the
    // host's addr+rkey+len) plus the address-as-offset bit — nvme-rdma's
    // use_inline_data is gated on SAOS, so without it the host ignores
    // IOCCSZ and never sends in-capsule write data. TCP uses byte-aligned
    // in-capsule SGLs only.
    let mut sgls = SGLS_BYTE_ALIGNED;
    if matches!(ctx.port.trtype, crate::subsystem::TransportType::Rdma) {
        sgls |= SGLS_KEYED | SGLS_SAOS;
    }
    id.sgls.set(sgls);

    if discovery {
        // Discovery controllers: no namespaces, no IO command set.
        id.nn.set(0);
    } else {
        id.nn.set(subsys.as_ref().map_or(0, |s| s.max_nsid()));
        id.oncs.set(oncs::DSM | oncs::WRITE_ZEROES);
        // IOCCSZ: (64B SQE + in-capsule data) / 16; IORCSZ: one CQE. RDMA
        // advertises one page of in-capsule data (nvmet parity): small write
        // payloads then arrive inside the command capsule and skip the
        // per-write RDMA READ round trip; larger IO stays on keyed SGLs.
        let inline = if matches!(ctx.port.trtype, crate::subsystem::TransportType::Rdma) {
            crate::RDMA_INLINE_DATA_SIZE
        } else {
            crate::INLINE_DATA_SIZE
        };
        id.ioccsz.set((64 + inline) / 16);
        id.iorcsz.set(1);
        id.icdoff.set(0);
    }
    id
}

fn nul_terminate(dst: &mut [u8; 256], s: &str) {
    dst.fill(0);
    let n = s.len().min(255);
    dst[..n].copy_from_slice(&s.as_bytes()[..n]);
}

fn build_id_ns<B: Backend>(backend: &B) -> Box<IdentifyNamespace> {
    let mut id = Box::new(IdentifyNamespace::zeroed());
    let blocks = backend.nr_blocks();
    id.nsze.set(blocks);
    id.ncap.set(blocks);
    id.nuse.set(blocks);
    id.nlbaf = 0;
    id.flbas = 0;
    // Shared namespace: it may be attached to multiple controllers at once
    // (every ioutgt connection is its own controller serving this backend),
    // so the host folds the paths into one multipath head.
    id.nmic = nmic::SHARED;
    id.dlfeat = 0x01; // deallocated blocks read zeroes
    id.lbaf[0].lbads = backend.block_shift();
    id.lbaf[0].ms.set(0);
    id.anagrpid.set(0);
    id
}

fn get_features<B: Backend>(ctx: &Rc<ConnCtx<B>>, admin: &AdminState<B>, sqe: &Sqe) -> Outcome {
    let cid = sqe.cid.get();
    let fid = (sqe.cdw10.get() & 0xFF) as u8;
    match fid {
        feat::NUM_QUEUES => {
            let queues = u32::from(io_queue_count(admin)) - 1;
            Outcome::status(ctx.cqe(queues | (queues << 16), cid, status::SUCCESS))
        }
        feat::KATO => Outcome::status(ctx.cqe(admin.kato_ms.get(), cid, status::SUCCESS)),
        feat::ASYNC_EVENT_CONFIG => Outcome::status(ctx.cqe(admin.aec.get(), cid, status::SUCCESS)),
        _ => Outcome::status(ctx.cqe(0, cid, status::INVALID_FIELD | status::DNR)),
    }
}

fn set_features<B: Backend>(ctx: &Rc<ConnCtx<B>>, admin: &AdminState<B>, sqe: &Sqe) -> Outcome {
    let cid = sqe.cid.get();
    let fid = (sqe.cdw10.get() & 0xFF) as u8;
    match fid {
        feat::NUM_QUEUES => {
            // Grant min(requested, offered); 0-based in both directions.
            let offered = u32::from(io_queue_count(admin)) - 1;
            let requested = sqe.cdw11.get() & 0xFFFF;
            let granted = requested.min(offered);
            debug!(requested, granted, "set features NUM_QUEUES");
            Outcome::status(ctx.cqe(granted | (granted << 16), cid, status::SUCCESS))
        }
        feat::KATO => {
            admin.kato_ms.set(sqe.cdw11.get());
            Outcome::status(ctx.cqe(0, cid, status::SUCCESS))
        }
        feat::ASYNC_EVENT_CONFIG => {
            admin.aec.set(sqe.cdw11.get());
            Outcome::status(ctx.cqe(0, cid, status::SUCCESS))
        }
        feat::HOST_ID => Outcome::status(ctx.cqe(0, cid, status::SUCCESS)),
        _ => Outcome::status(ctx.cqe(0, cid, status::FEATURE_NOT_CHANGEABLE | status::DNR)),
    }
}

/// IO queues this controller may use (subsystem max_qid; the discovery
/// subsystem has none but hosts never ask).
fn io_queue_count<B: Backend>(admin: &AdminState<B>) -> u16 {
    admin
        .subsys
        .borrow()
        .as_ref()
        .map_or(1, |s: &Arc<Subsystem<B>>| s.max_qid.max(1))
}

fn get_log_page<B: Backend>(
    ctx: &Rc<ConnCtx<B>>,
    admin: &AdminState<B>,
    tag: u16,
    sqe: &Sqe,
) -> Outcome {
    let cid = sqe.cid.get();
    let lid = (sqe.cdw10.get() & 0xFF) as u8;
    // NUMD (0-based dwords, split across cdw10/11) and LPO.
    let numdl = sqe.cdw10.get() >> 16;
    let numdu = sqe.cdw11.get() & 0xFFFF;
    let len = ((u64::from(numdu) << 16 | u64::from(numdl)) + 1) * 4;
    let offset = u64::from(sqe.cdw13.get()) << 32 | u64::from(sqe.cdw12.get());

    match lid {
        log_page::DISCOVERY if admin.discovery.get() => {
            let log = build_discovery_log(ctx);
            let end = offset.saturating_add(len).min(log.len() as u64);
            let start = offset.min(end);
            let window = &log[usize::try_from(start).expect("log fits")
                ..usize::try_from(end).expect("log fits")];
            let n = fill_slot(ctx, tag, window);
            Outcome::with_data(ctx.cqe(0, cid, status::SUCCESS), n)
        }
        log_page::CHANGED_NS => {
            // 0xFFFFFFFF in the first entry: "more changed than fits";
            // the Linux host rescans everything. Reading clears it.
            let mut page = [0u8; 4096];
            if admin.ns_changed.replace(false) {
                page[..4].copy_from_slice(&u32::MAX.to_le_bytes());
            }
            let n = len.min(4096);
            #[allow(clippy::cast_possible_truncation)]
            let n32 = n as u32;
            let take = usize::try_from(n).expect("<=4096");
            let written = fill_slot(ctx, tag, &page[..take]);
            debug_assert_eq!(written, n32);
            Outcome::with_data(ctx.cqe(0, cid, status::SUCCESS), n32)
        }
        log_page::ERROR | log_page::SMART | log_page::FW_SLOT => {
            // Zero-filled pages: nothing to report yet.
            let n = len.min(4096);
            let take = usize::try_from(n).expect("<=4096");
            ctx.queue.lease_or_owned(tag, take.max(1));
            ctx.queue.slot(tag).data().as_mut_slice()[..take].fill(0);
            #[allow(clippy::cast_possible_truncation)]
            Outcome::with_data(ctx.cqe(0, cid, status::SUCCESS), n as u32)
        }
        _ => Outcome::status(ctx.cqe(0, cid, status::INVALID_LOG_PAGE | status::DNR)),
    }
}

/// Discovery log: header + one entry per NVM subsystem on this port.
fn build_discovery_log<B: Backend>(ctx: &Rc<ConnCtx<B>>) -> Vec<u8> {
    let subsystems = &ctx.port.subsystems;
    let mut log = Vec::with_capacity(1024 * (1 + subsystems.len()));

    let header = DiscoveryLogHeader {
        genctr: 1.into(),
        numrec: (subsystems.len() as u64).into(),
        recfmt: 0.into(),
        resv: [0; 1006],
    };
    log.extend_from_slice(header.as_bytes());

    for (index, (nqn, _subsys)) in subsystems.iter().enumerate() {
        let mut entry = DiscoveryLogEntry::zeroed();
        entry.trtype = ctx.port.trtype.trtype();
        entry.adrfam = 1; // IPv4
        entry.subtype = fabrics::subtype::NVM;
        entry.treq = 0;
        entry.portid.set(u16::try_from(index).unwrap_or(0));
        entry.cntlid.set(0xFFFF); // dynamic controllers
        entry.asqsz.set(32);
        ascii_pad(&mut entry.trsvcid, &ctx.port.trsvcid);
        ascii_pad(&mut entry.traddr, &ctx.port.traddr);
        entry.subnqn.fill(0);
        let n = nqn.len().min(255);
        entry.subnqn[..n].copy_from_slice(&nqn.as_bytes()[..n]);
        log.extend_from_slice(entry.as_bytes());
    }
    log
}
