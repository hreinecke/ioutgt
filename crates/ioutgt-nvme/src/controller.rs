//! Controller register state: the CC/CSTS enable/shutdown machine
//! for one fabrics controller. (The cross-thread controller registry
//! lives in `ioutgt_core::registry`.)

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
        use crate::fabrics::{cc, csts};
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

    /// CSTS.RDY is set.
    pub fn ready(&self) -> bool {
        self.csts & crate::fabrics::csts::RDY != 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fabrics::cc;

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
        assert!(regs.csts() & crate::fabrics::csts::SHST_COMPLETE != 0);
    }
}
