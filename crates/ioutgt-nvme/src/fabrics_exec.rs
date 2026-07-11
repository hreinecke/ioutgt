//! Fabrics command execution: Connect (admin + IO) and Property Get/Set.

use std::rc::Rc;
use std::sync::Arc;

use crate::fabrics::{self, ConnectCommand, PropertyCommand, fctype, prop};
use crate::spec::Sqe;
use crate::status;
use tracing::{debug, info, warn};
use zerocopy::{FromBytes, IntoBytes};

use crate::controller::CcEffect;
use crate::dispatch::{ConnCtx, Outcome, Role};
use ioutgt_core::backend::Backend;
use ioutgt_core::registry::QueueInfo;
use ioutgt_cpus::thread::{current_cpus, current_tid};

/// NUL/space-trimmed string from a fixed NQN field.
pub fn nqn_str(raw: &[u8]) -> &str {
    let end = raw.iter().position(|&b| b == 0).unwrap_or(raw.len());
    std::str::from_utf8(&raw[..end]).unwrap_or("").trim_end()
}

/// This queue's identity for the registry. Connect executes on the
/// owning queue thread, so the tid recorded here is the thread serving
/// this queue.
fn queue_info<B: Backend>(ctx: &ConnCtx<B>) -> QueueInfo {
    QueueInfo {
        qid: ctx.queue.qid,
        sqsize: ctx.queue.sqsize,
        tid: current_tid(),
        cpus: current_cpus(),
        peer: ctx.peer.clone(),
    }
}

/// Route one fabrics command (Connect / Property Get / Property Set).
pub fn execute<B: Backend>(ctx: &Rc<ConnCtx<B>>, tag: u16, sqe: &Sqe) -> Outcome {
    let _ = tag;
    let cmd_bytes = sqe.as_bytes();
    let fct = cmd_bytes[4];
    match fct {
        fctype::CONNECT => connect(ctx, sqe),
        fctype::PROPERTY_GET | fctype::PROPERTY_SET => property(ctx, sqe, fct),
        _ => {
            warn!(
                qid = ctx.queue.qid,
                fctype = fct,
                "unsupported fabrics command"
            );
            Outcome::status(ctx.cqe(0, sqe.cid.get(), status::INVALID_OPCODE | status::DNR))
        }
    }
}

fn connect<B: Backend>(ctx: &Rc<ConnCtx<B>>, sqe: &Sqe) -> Outcome {
    let cmd = ConnectCommand::ref_from_bytes(sqe.as_bytes()).expect("64B, align 1");
    let cid = cmd.cid.get();
    let data = &ctx.connect_data;
    let subsysnqn = nqn_str(&data.subsysnqn);
    let hostnqn = nqn_str(&data.hostnqn);

    if cmd.recfmt.get() != 0 {
        return Outcome::status(ctx.cqe(0, cid, status::CONNECT_FORMAT | status::DNR));
    }

    match &ctx.role {
        Role::Admin(admin) => {
            // One Connect per queue: a second Connect on an already-bound
            // admin queue would mint and leak another cntlid and silently
            // overwrite the controller's identity (the connect_data is
            // fixed at queue setup, so it would also ignore the new
            // capsule). Reject, as nvmet does.
            if admin.cntlid.get() != 0 {
                return Outcome::status(ctx.cqe(
                    0,
                    cid,
                    status::CONNECT_INVALID_PARAM | status::DNR,
                ));
            }
            // Resolve the subsystem: the well-known discovery NQN or a
            // configured NVM subsystem.
            let discovery = subsysnqn == fabrics::DISCOVERY_NQN;
            if discovery {
                admin.discovery.set(true);
            } else {
                let Some(subsys) = ctx.port.subsystem(subsysnqn) else {
                    info!(subsysnqn, "connect to unknown subsystem");
                    return Outcome::status(ctx.cqe(
                        status::connect_invalid_param_result(true, 256),
                        cid,
                        status::CONNECT_INVALID_PARAM | status::DNR,
                    ));
                };
                if !subsys.allow_any_host {
                    // Host ACLs arrive with the control plane.
                    return Outcome::status(ctx.cqe(
                        0,
                        cid,
                        status::CONNECT_INVALID_HOST | status::DNR,
                    ));
                }
                *admin.subsys.borrow_mut() = Some(Arc::clone(subsys));
            }
            if data.cntlid.get() != 0xFFFF {
                // Dynamic controller model only.
                return Outcome::status(ctx.cqe(
                    status::connect_invalid_param_result(true, 16),
                    cid,
                    status::CONNECT_INVALID_PARAM | status::DNR,
                ));
            }
            // IO queues the controller may install: the subsystem's
            // offered count (discovery controllers get none).
            let max_qid = admin.subsys.borrow().as_ref().map_or(0, |s| s.max_qid);
            // Discovery controllers default to 120s KATO, others take the
            // host's value.
            let kato = if cmd.kato.get() == 0 && discovery {
                120_000
            } else {
                cmd.kato.get()
            };
            let Some(cntlid) = ctx.registry.allocate(
                subsysnqn,
                hostnqn,
                max_qid,
                kato,
                queue_info(ctx),
                discovery,
            ) else {
                return Outcome::status(ctx.cqe(0, cid, status::CONNECT_CTRL_BUSY | status::DNR));
            };
            admin.cntlid.set(cntlid);
            ctx.queue.stats.cntlid.set(cntlid);
            admin.kato_ms.set(kato);
            info!(cntlid, subsysnqn, hostnqn, kato, "controller created");
            Outcome::status(ctx.cqe(u32::from(cntlid), cid, status::SUCCESS))
        }
        Role::Io(io) => {
            if io.cntlid.get() != 0 {
                // Already connected: reject a duplicate Connect.
                return Outcome::status(ctx.cqe(
                    0,
                    cid,
                    status::CONNECT_INVALID_PARAM | status::DNR,
                ));
            }
            let cntlid = data.cntlid.get();
            match ctx
                .registry
                .install_io_queue(cntlid, hostnqn, queue_info(ctx))
            {
                Ok(entry) => {
                    io.cntlid.set(cntlid);
                    ctx.queue.stats.cntlid.set(cntlid);
                    if !entry.discovery
                        && let Some(subsys) = ctx.port.subsystem(&entry.subsys_nqn)
                    {
                        let _ = io.subsys.set(Arc::clone(subsys));
                    }
                    debug!(cntlid, qid = ctx.queue.qid, "io queue connected");
                    Outcome::status(ctx.cqe(u32::from(cntlid), cid, status::SUCCESS))
                }
                Err(err) => {
                    warn!(cntlid, qid = ctx.queue.qid, ?err, "io connect rejected");
                    Outcome::status(ctx.cqe(
                        status::connect_invalid_param_result(true, 16),
                        cid,
                        status::CONNECT_INVALID_PARAM | status::DNR,
                    ))
                }
            }
        }
    }
}

fn property<B: Backend>(ctx: &Rc<ConnCtx<B>>, sqe: &Sqe, fct: u8) -> Outcome {
    let cmd = PropertyCommand::ref_from_bytes(sqe.as_bytes()).expect("64B, align 1");
    let cid = cmd.cid.get();
    let Role::Admin(admin) = &ctx.role else {
        return Outcome::status(ctx.cqe(0, cid, status::INVALID_FIELD | status::DNR));
    };
    let offset = cmd.offset.get();

    if fct == fctype::PROPERTY_GET {
        let value: u64 = match offset {
            prop::CAP => admin.regs.borrow().cap,
            // NVMe 1.3.0, as kernel nvmet.
            prop::VS => 0x0001_0300,
            prop::CC => u64::from(admin.regs.borrow().cc()),
            prop::CSTS => u64::from(admin.regs.borrow().csts()),
            _ => {
                return Outcome::status(ctx.cqe(0, cid, status::INVALID_FIELD | status::DNR));
            }
        };
        let mut cqe = ctx.cqe(0, cid, status::SUCCESS);
        // Property Get returns the value in DW0+DW1.
        #[allow(clippy::cast_possible_truncation)]
        cqe.result.set(value as u32);
        #[allow(clippy::cast_possible_truncation)]
        cqe.rsvd.set((value >> 32) as u32);
        return Outcome::status(cqe);
    }

    // Property Set.
    let value = cmd.value.get();
    match offset {
        prop::CC => {
            // CC writes are only valid after Connect has bound the
            // controller; enabling an unbound controller (cntlid 0,
            // no subsystem) violates the fabrics enable sequence.
            if admin.cntlid.get() == 0 {
                return Outcome::status(ctx.cqe(0, cid, status::INVALID_FIELD | status::DNR));
            }
            #[allow(clippy::cast_possible_truncation)]
            let effect = admin.regs.borrow_mut().write_cc(value as u32);
            match effect {
                CcEffect::Enabled => debug!(cntlid = admin.cntlid.get(), "controller enabled"),
                CcEffect::Shutdown => {
                    info!(cntlid = admin.cntlid.get(), "controller shutdown requested");
                }
                CcEffect::Disabled => {
                    debug!(cntlid = admin.cntlid.get(), "controller disabled");
                }
                CcEffect::None => {}
            }
            Outcome::status(ctx.cqe(0, cid, status::SUCCESS))
        }
        _ => Outcome::status(ctx.cqe(0, cid, status::INVALID_FIELD | status::DNR)),
    }
}
