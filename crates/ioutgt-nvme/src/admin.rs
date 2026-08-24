//! Admin command handlers: Identify, Features, Log pages, Keep Alive,
//! Async Event Requests. Values mirror kernel nvmet where interop
//! depends on them.

use std::rc::Rc;

use crate::fabrics::{self, DiscoveryLogEntry, DiscoveryLogHeader};
use crate::identify::{
    IdentifyController, IdentifyNamespace, SGLS_BYTE_ALIGNED, SGLS_KEYED, SGLS_SAOS, anacap, cmic,
    ctratt, nsfeat, nmic, oncs, u128_le,
};
use crate::spec::{Sqe, admin_opcode, ana, cns, feat, log_page};
use crate::status;
use tracing::debug;
use zerocopy::IntoBytes;

use crate::dispatch::{AdminState, ConnCtx, Outcome};
use ioutgt_core::backend::Backend;
use ioutgt_core::subsystem::{Namespace, Subsystem, SubsystemPort, TransportType};

/// KAS granularity: 10 seconds in 100ms units, as nvmet.
const KAS_UNITS: u16 = 100;

/// ANATT: seconds a namespace may spend in the ANA Change state before the
/// host gives up on the transition. We never report ANA Change — a group move
/// is atomic here — so this only bounds the host's patience; 10 s, as nvmet.
const ANATT_SECS: u8 = 10;

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
                    let id = build_id_ns(subsys, ns);
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
    // NN doubles as the ceiling on MNAN, which reporting ANA makes
    // load-bearing (below), so an ANA subsystem that has lost its last
    // namespace reports no ANA rather than an MNAN the host must reject.
    let nn = if discovery {
        0
    } else {
        subsys.as_ref().map_or(0, |s| s.max_nsid())
    };
    let ana = nn > 0 && subsys.as_ref().is_some_and(|s| s.ana());

    id.vid.set(0);
    id.ssvid.set(0);
    ascii_pad(&mut id.fr, "1.0");
    ascii_pad(&mut id.mn, "ioutgt");
    match subsys.as_ref() {
        Some(s) => {
            ascii_pad(&mut id.sn, &s.serial);
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
    // OAES: the host masks its AEC against this; without the NS_ATTR bit it
    // never enables namespace-change notices, and without DISC_CHANGE a
    // persistent discovery controller parks an AER it would never get back.
    // The two are disjoint, as in nvmet (NVMET_AEN_CFG_OPTIONAL vs
    // NVMET_DISC_AEN_CFG_OPTIONAL): a discovery controller has no namespaces
    // to report changes to, and an NVM controller no discovery log page.
    let mut oaes = if discovery {
        crate::AEN_CFG_DISC_CHANGE
    } else {
        crate::AEN_CFG_NS_ATTR
    };
    id.cntrltype = if discovery { 2 } else { 1 };
    if !discovery {
        // Advertise multi-controller capability so the host's NVMe-multipath
        // layer builds a namespace head plus a per-controller path device
        // (/dev/nvmeXcYnZ), as it does for kernel nvmet. Discovery
        // controllers have no namespaces, so (like nvmet) they advertise no
        // CMIC.
        id.cmic = cmic::MULTI_CTRL;
    }
    if ana {
        // The whole ANA feature set moves together: the host validates these
        // fields, sizes its log buffer from them, and rejects the controller
        // if they disagree (`nvme_mpath_init_identify`).
        id.cmic |= cmic::ANA_REPORTING;
        id.anatt = ANATT_SECS;
        // ANACAP bit 6 stays clear: a namespace changes group exactly when
        // the cluster's topology does, never gradually.
        id.anacap = anacap::OPTIMIZED | anacap::NON_OPTIMIZED;
        // NANAGRPID is a count (how many groups exist); ANAGRPMAX is the
        // largest valid ANAGRPID value a descriptor may carry. For the old
        // fixed {1, 2} groups the two coincided; a Sheepdog zone id is an
        // arbitrary u32 (by default the node's own IPv4 address), so they no
        // longer do — the host only needs `grpid <= anagrpmax` and `grpid !=
        // 0` per descriptor (`nvme_parse_ana_log`), not a dense range.
        let zones = subsys
            .as_ref()
            .map_or_else(Default::default, |s| s.ana_zones());
        id.nanagrpid
            .set(u32::try_from(zones.len()).unwrap_or(u32::MAX));
        id.anagrpmax.set(zones.iter().copied().max().unwrap_or(0));
        oaes |= crate::AEN_CFG_ANA_CHANGE;
    }
    id.oaes.set(oaes);
    id.kas.set(KAS_UNITS);
    // TBKAS: IO traffic keeps the controller alive, so a busy host stops
    // sending Keep Alive commands entirely. Only claim it where every queue
    // publishes its traffic to the admin queue's watchdog — TCP does (the
    // per-queue traffic beacon in ioutgt-nvme-tcp), RDMA does not yet, and
    // claiming it there would tear down a controller whose admin queue is
    // idle while its IO queues are busy. Discovery controllers have no IO
    // queues and follow nvmet, which leaves their CTRATT at zero.
    if !discovery && matches!(ctx.port.trtype, TransportType::Tcp) {
        id.ctratt.set(ctratt::TBKAS);
    }
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
    if matches!(ctx.port.trtype, ioutgt_core::subsystem::TransportType::Rdma) {
        sgls |= SGLS_KEYED | SGLS_SAOS;
    }
    id.sgls.set(sgls);

    id.nn.set(nn);
    if discovery {
        // Discovery controllers: no namespaces, no IO command set.
    } else {
        // MNAN: how many namespaces the subsystem actually holds, where the
        // storage knows (a Sheepdog ACL). With sparse NSIDs, NN — the highest
        // valid one — says nothing about the count; 0 means "no more than NN".
        //
        // Reporting ANA takes that freedom away: the host sizes its ANA log
        // buffer as 16 + NANAGRPID*32 + MNAN*4 and refuses a controller whose
        // MNAN is zero or above NN. So there MNAN is pinned into a range that
        // both passes that check and leaves room for every NSID we list in
        // the log page.
        let mut mnan = subsys.as_ref().map_or(0, |s| s.mnan());
        if ana {
            let count = subsys
                .as_ref()
                .map_or(0, |s| u32::try_from(s.snapshot().len()).unwrap_or(nn));
            mnan = mnan.clamp(count.clamp(1, nn), nn);
        }
        id.mnan.set(mnan);
        // TNVMCAP: bytes of NVM in the subsystem, which for us is exactly the
        // sum of the attached backends — there is no spare pool a namespace
        // could grow into, so UNVMCAP stays 0. Both fields move with the
        // namespace table: a hot-add grows TNVMCAP on the next Identify.
        id.tnvmcap = u128_le(subsys.as_ref().map_or(0, |s| s.total_capacity()));
        id.unvmcap = u128_le(0);
        id.oncs.set(oncs::DSM | oncs::WRITE_ZEROES);
        // IOCCSZ: (64B SQE + in-capsule data) / 16; IORCSZ: one CQE. RDMA
        // advertises one page of in-capsule data (nvmet parity): small write
        // payloads then arrive inside the command capsule and skip the
        // per-write RDMA READ round trip; larger IO stays on keyed SGLs.
        let inline = if matches!(ctx.port.trtype, ioutgt_core::subsystem::TransportType::Rdma) {
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

fn build_id_ns<B: Backend>(subsys: &Subsystem<B>, ns: &Namespace<B>) -> Box<IdentifyNamespace> {
    let mut id = Box::new(IdentifyNamespace::zeroed());
    let backend = ns.backend.as_ref();
    let blocks = backend.nr_blocks();
    id.nsze.set(blocks);
    id.ncap.set(blocks);
    id.nuse.set(blocks);
    // NVMCAP: the same capacity NSZE states, but in bytes — the namespace's
    // share of the subsystem's TNVMCAP. Nothing here is thin-provisioned, so
    // allocated and total are one number.
    id.nvmcap = u128_le(ns.capacity());
    id.nlbaf = 0;
    id.flbas = 0;
    id.nsfeat = nsfeat::THINP;
    // Shared namespace: it may be attached to multiple controllers at once
    // (every ioutgt connection is its own controller serving this backend),
    // so the host folds the paths into one multipath head.
    id.nmic = nmic::SHARED;
    id.dlfeat = 0x01; // deallocated blocks read zeroes
    id.lbaf[0].lbads = backend.block_shift();
    id.lbaf[0].ms.set(0);
    // The namespace's ANA group — which for us *is* its state. Zero (no
    // group) on a subsystem that does not report ANA, where the host ignores
    // the field anyway.
    id.anagrpid
        .set(if subsys.ana() { ns.ana_grpid() } else { 0 });
    id
}

fn get_features<B: Backend>(ctx: &Rc<ConnCtx<B>>, admin: &AdminState<B>, sqe: &Sqe) -> Outcome {
    let cid = sqe.cid.get();
    let fid = (sqe.cdw10.get() & 0xFF) as u8;
    match fid {
        feat::NUM_QUEUES => {
            let queues = u32::from(io_queue_count(ctx, admin)) - 1;
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
            let offered = u32::from(io_queue_count(ctx, admin)) - 1;
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

/// IO queues this controller may use (the port's max_qid; the
/// discovery subsystem has none but hosts never ask).
fn io_queue_count<B: Backend>(ctx: &ConnCtx<B>, admin: &AdminState<B>) -> u16 {
    if admin.subsys.borrow().is_some() {
        ctx.port.max_qid.max(1)
    } else {
        1
    }
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
        log_page::ANA => {
            let subsys = admin.subsys.borrow();
            let Some(subsys) = subsys.as_ref().filter(|s| s.ana()) else {
                return Outcome::status(ctx.cqe(0, cid, status::INVALID_LOG_PAGE | status::DNR));
            };
            // LSP (cdw10 bits 11:8) bit 0 = RGO: group states without the
            // NSID lists, which is what the host polls on an ANA change it
            // only needs the states from.
            let lsp = u8::try_from((sqe.cdw10.get() >> 8) & 0xF).expect("four bits");
            let rgo = lsp & ana::LSP_RGO != 0;
            let log = build_ana_log(subsys, rgo);
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

/// ANA log page: the header, then one descriptor per ANA group listing the
/// NSIDs currently in it (ascending, as the host's walk assumes).
///
/// Every group in [`Subsystem::ana_zones`] is always reported, empty ones
/// included, so the log's shape never depends on where the namespaces happen
/// to sit and `NANAGRPID` — from which the host sizes its buffer once, at
/// Identify time — always matches. A group's `state` comes from whichever of
/// this subsystem's namespaces sits in it (uniform in the common case of one
/// cluster, since it reduces to "is our one gateway in this zone"); an empty
/// group defaults to Optimized, a value no NSID list ever makes the host act
/// on. `rgo` drops the NSID lists.
fn build_ana_log<B: Backend>(subsys: &Subsystem<B>, rgo: bool) -> Vec<u8> {
    let table = subsys.snapshot();
    let chgcnt = subsys.ana_chgcnt();
    let zones = subsys.ana_zones();
    let mut log = Vec::with_capacity(
        size_of::<ana::LogHeader>() + zones.len() * size_of::<ana::GroupDesc>() + table.len() * 4,
    );
    let header = ana::LogHeader {
        chgcnt: chgcnt.into(),
        ngrps: u16::try_from(zones.len()).unwrap_or(u16::MAX).into(),
        rsvd10: Default::default(),
    };
    log.extend_from_slice(header.as_bytes());
    for &grpid in zones.iter() {
        let members: Vec<&std::sync::Arc<Namespace<B>>> = table
            .values()
            .filter(|ns| ns.ana_grpid() == grpid)
            .collect();
        let state = members
            .first()
            .map_or(ioutgt_core::subsystem::ANA_STATE_OPTIMIZED, |ns| {
                ns.ana_state_code()
            });
        let nsids: Vec<u32> = if rgo {
            Vec::new()
        } else {
            members.iter().map(|ns| ns.nsid).collect()
        };
        let desc = ana::GroupDesc {
            grpid: grpid.into(),
            nnsids: u32::try_from(nsids.len())
                .expect("namespace count fits")
                .into(),
            chgcnt: chgcnt.into(),
            state,
            rsvd17: [0; 15],
        };
        log.extend_from_slice(desc.as_bytes());
        for nsid in nsids {
            log.extend_from_slice(&nsid.to_le_bytes());
        }
    }
    log
}

/// Discovery log: header + one entry per path to each NVM subsystem on this
/// port.
///
/// A subsystem usually has exactly one path — this target's own port — and
/// then this is nvmet's one-entry-per-subsystem log. A subsystem the control
/// plane gave a path list (`Subsystem::set_ports`) instead contributes one
/// entry per path, so a host learns every target serving it, not only the one
/// it happened to ask. For Sheepdog that list is the set of targets
/// registered on the subsystem's cluster ACL.
///
/// `GENCTR` is the sum of the subsystems' own discovery generations
/// (`Subsystem::disc_genctr`): a host that reads the log in parts can tell the
/// read raced a change, and one that parked an AER re-reads when the Discovery
/// Log Page Change notice tells it the value moved.
fn build_discovery_log<B: Backend>(ctx: &Rc<ConnCtx<B>>) -> Vec<u8> {
    let subsystems = &ctx.port.subsystems;
    // The port's own transport and address, for subsystems with no path list.
    let local = SubsystemPort {
        traddr: ctx.port.traddr.clone(),
        trsvcid: ctx.port.trsvcid.clone(),
        trtype: ctx.port.trtype,
        portid: 0,
    };

    let mut entries = Vec::with_capacity(subsystems.len());
    let mut genctr = 0u64;
    for (nqn, subsys) in subsystems {
        // Each subsystem versions its own entries; the log's GENCTR is their
        // sum, which moves whenever any of them does and — for the usual
        // one-subsystem discovery port — *is* the subsystem's own counter.
        genctr = genctr.wrapping_add(subsys.disc_genctr());
        let ports = subsys.ports();
        if ports.is_empty() {
            entries.push(discovery_entry(nqn, &local));
        } else {
            entries.extend(ports.iter().map(|port| discovery_entry(nqn, port)));
        }
    }

    let mut log = Vec::with_capacity(1024 * (1 + entries.len()));
    let header = DiscoveryLogHeader {
        genctr: genctr.into(),
        numrec: (entries.len() as u64).into(),
        recfmt: 0.into(),
        resv: [0; 1006],
    };
    log.extend_from_slice(header.as_bytes());
    for entry in &entries {
        log.extend_from_slice(entry.as_bytes());
    }
    log
}

/// One discovery log entry: subsystem `nqn` reachable over `port`.
fn discovery_entry(nqn: &str, port: &SubsystemPort) -> DiscoveryLogEntry {
    let mut entry = DiscoveryLogEntry::zeroed();
    // The model's transport enum is protocol-neutral; the NVMe-oF
    // TRTYPE byte it maps to is this crate's concern.
    entry.trtype = match port.trtype {
        TransportType::Tcp => fabrics::trtype::TCP,
        TransportType::Rdma => fabrics::trtype::RDMA,
    };
    // A traddr that does not parse is a hostname, which the spec allows only
    // for FC/loop; IPv4 is the better guess for one on an IP transport.
    entry.adrfam = match port.traddr.parse::<std::net::IpAddr>() {
        Ok(std::net::IpAddr::V6(_)) => fabrics::adrfam::IPV6,
        _ => fabrics::adrfam::IPV4,
    };
    entry.subtype = fabrics::subtype::NVM;
    entry.treq = 0;
    entry.portid.set(port.portid);
    entry.cntlid.set(0xFFFF); // dynamic controllers
    entry.asqsz.set(32);
    ascii_pad(&mut entry.trsvcid, &port.trsvcid);
    ascii_pad(&mut entry.traddr, &port.traddr);
    entry.subnqn.fill(0);
    let n = nqn.len().min(255);
    entry.subnqn[..n].copy_from_slice(&nqn.as_bytes()[..n]);
    entry
}
