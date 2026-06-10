//! Controller state: CC/CSTS register machine, cntlid allocation, and
//! the cross-thread registry used to route IO-queue Connects.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

/// Fabrics register state for one controller, per the NVMe enable
/// sequence: host writes CC.EN, controller raises CSTS.RDY; shutdown via
/// CC.SHN → CSTS.SHST_COMPLETE.
///
/// Lives on the admin queue thread; not `Send`.
#[derive(Debug)]
pub struct RegisterState {
    cc: u32,
    csts: u32,
    /// CAP advertised to the host (MQES in entries-1, CQR, TO, etc.).
    pub cap: u64,
}

/// Outcome of a CC write the surrounding controller must act on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CcEffect {
    /// No state change.
    None,
    /// EN 0→1: controller becomes ready.
    Enabled,
    /// Shutdown notification: tear down queues, then report complete.
    Shutdown,
    /// EN 1→0 (controller reset).
    Disabled,
}

impl RegisterState {
    /// CAP value per nvmet: MQES = qsize-1, CQR set, timeout 15s
    /// (units of 500ms), no DSTRD.
    pub fn new(max_queue_entries: u16) -> Self {
        let mqes = u64::from(max_queue_entries - 1);
        let cap = mqes | (1 << 16) | (30 << 24);
        RegisterState {
            cc: 0,
            csts: 0,
            cap,
        }
    }

    /// Current CC register value.
    pub fn cc(&self) -> u32 {
        self.cc
    }

    /// Current CSTS register value.
    pub fn csts(&self) -> u32 {
        self.csts
    }

    /// Apply a Property Set of CC.
    pub fn write_cc(&mut self, value: u32) -> CcEffect {
        use ioutgt_nvme::fabrics::{cc, csts};
        let was_enabled = self.cc & cc::EN != 0;
        let now_enabled = value & cc::EN != 0;
        let shutdown = value & cc::SHN_MASK != 0;
        self.cc = value;
        if shutdown {
            self.csts |= csts::SHST_COMPLETE;
            return CcEffect::Shutdown;
        }
        if !was_enabled && now_enabled {
            self.csts |= csts::RDY;
            return CcEffect::Enabled;
        }
        if was_enabled && !now_enabled {
            self.csts &= !csts::RDY;
            return CcEffect::Disabled;
        }
        CcEffect::None
    }

    /// Latch a fatal error (CSTS.CFS), e.g. keep-alive expiry.
    pub fn fatal_error(&mut self) {
        self.csts |= ioutgt_nvme::fabrics::csts::CFS;
    }

    /// CSTS.RDY is set.
    pub fn ready(&self) -> bool {
        self.csts & ioutgt_nvme::fabrics::csts::RDY != 0
    }
}

/// A live controller's routing info, visible to all threads.
#[derive(Debug, Clone)]
#[allow(missing_docs)] // routing record; fields named per NVMe terms
pub struct ControllerEntry {
    pub cntlid: u16,
    pub subsys_nqn: String,
    pub hostnqn: String,
    /// IO queues installed so far (Connect-time duplicate detection).
    pub installed_qids: Vec<u16>,
}

/// Cross-thread controller registry. Control-plane rate only (Connect /
/// teardown); a mutex is fine.
#[derive(Default)]
pub struct Registry {
    inner: Mutex<RegistryInner>,
}

#[derive(Default)]
struct RegistryInner {
    next_cntlid: u16,
    controllers: HashMap<u16, ControllerEntry>,
}

#[allow(missing_docs)]
impl Registry {
    pub fn new() -> Arc<Registry> {
        Arc::new(Registry {
            inner: Mutex::new(RegistryInner {
                next_cntlid: 1,
                controllers: HashMap::new(),
            }),
        })
    }

    /// Allocate a cntlid for a new controller (admin Connect).
    pub fn allocate(&self, subsys_nqn: &str, hostnqn: &str) -> Option<u16> {
        let mut inner = self.inner.lock().expect("registry poisoned");
        // Linear scan for a free id: controller counts are tiny.
        let start = inner.next_cntlid.max(1);
        for offset in 0..u16::MAX - 1 {
            let cntlid = 1 + (start.wrapping_add(offset).wrapping_sub(1) % (u16::MAX - 1));
            let inserted = match inner.controllers.entry(cntlid) {
                std::collections::hash_map::Entry::Vacant(slot) => {
                    slot.insert(ControllerEntry {
                        cntlid,
                        subsys_nqn: subsys_nqn.to_owned(),
                        hostnqn: hostnqn.to_owned(),
                        installed_qids: vec![0],
                    });
                    true
                }
                std::collections::hash_map::Entry::Occupied(_) => false,
            };
            if inserted {
                inner.next_cntlid = cntlid.wrapping_add(1);
                return Some(cntlid);
            }
        }
        None
    }

    /// Validate an IO-queue Connect: cntlid exists, same host, qid fresh.
    pub fn install_io_queue(
        &self,
        cntlid: u16,
        hostnqn: &str,
        qid: u16,
    ) -> Result<ControllerEntry, IoConnectError> {
        let mut inner = self.inner.lock().expect("registry poisoned");
        let entry = inner
            .controllers
            .get_mut(&cntlid)
            .ok_or(IoConnectError::UnknownController)?;
        if entry.hostnqn != hostnqn {
            return Err(IoConnectError::HostMismatch);
        }
        if entry.installed_qids.contains(&qid) {
            return Err(IoConnectError::QueueExists);
        }
        entry.installed_qids.push(qid);
        Ok(entry.clone())
    }

    /// Remove a controller (shutdown, keep-alive expiry, admin
    /// disconnect).
    pub fn remove(&self, cntlid: u16) -> Option<ControllerEntry> {
        self.inner
            .lock()
            .expect("registry poisoned")
            .controllers
            .remove(&cntlid)
    }

    pub fn len(&self) -> usize {
        self.inner
            .lock()
            .expect("registry poisoned")
            .controllers
            .len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// IO-queue Connect failure reasons (mapped to fabrics status by M4).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(missing_docs)]
pub enum IoConnectError {
    UnknownController,
    HostMismatch,
    QueueExists,
}

#[cfg(test)]
mod tests {
    use super::*;
    use ioutgt_nvme::fabrics::cc;

    #[test]
    fn enable_sequence() {
        let mut regs = RegisterState::new(128);
        assert_eq!(regs.cap & 0xFFFF, 127); // MQES 0-based
        assert!(!regs.ready());
        // Host programs IOSQES/IOCQES then sets EN.
        let value = cc::EN | (6 << cc::IOSQES_SHIFT) | (4 << cc::IOCQES_SHIFT);
        assert_eq!(regs.write_cc(value), CcEffect::Enabled);
        assert!(regs.ready());
        assert_eq!(regs.write_cc(value), CcEffect::None); // idempotent
        // Reset.
        assert_eq!(regs.write_cc(0), CcEffect::Disabled);
        assert!(!regs.ready());
    }

    #[test]
    fn shutdown_reports_complete() {
        let mut regs = RegisterState::new(128);
        regs.write_cc(cc::EN);
        assert_eq!(regs.write_cc(cc::EN | cc::SHN_NORMAL), CcEffect::Shutdown);
        assert!(regs.csts() & ioutgt_nvme::fabrics::csts::SHST_COMPLETE != 0);
    }

    #[test]
    fn registry_allocates_unique_cntlids() {
        let registry = Registry::new();
        let a = registry.allocate("nqn.test", "nqn.host").unwrap();
        let b = registry.allocate("nqn.test", "nqn.host").unwrap();
        assert_ne!(a, b);
        assert!(a >= 1 && b >= 1);

        // IO queue install: unknown controller rejected, dup qid rejected.
        assert_eq!(
            registry
                .install_io_queue(0xBEEF, "nqn.host", 1)
                .unwrap_err(),
            IoConnectError::UnknownController
        );
        registry.install_io_queue(a, "nqn.host", 1).unwrap();
        assert_eq!(
            registry.install_io_queue(a, "nqn.host", 1).unwrap_err(),
            IoConnectError::QueueExists
        );
        assert_eq!(
            registry.install_io_queue(a, "nqn.other", 2).unwrap_err(),
            IoConnectError::HostMismatch
        );
        registry.remove(a).unwrap();
        assert_eq!(registry.len(), 1);
    }
}
