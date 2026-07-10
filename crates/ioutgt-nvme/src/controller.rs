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

/// One installed queue's identity (recorded at Connect time, on the
/// owning queue thread).
#[derive(Debug, Clone)]
pub struct QueueInfo {
    /// Queue id (0 = admin, 1..=max_qid = IO).
    pub qid: u16,
    /// Queue depth in entries (wire sqsize + 1).
    pub sqsize: u16,
    /// Kernel thread id of the queue thread serving this queue.
    /// Meaningful only while the controller lives: the entry is reaped
    /// with its admin connection, before any tid could be reused.
    pub tid: i32,
    /// The serving thread's CPU affinity at Connect time: kernel
    /// cpulist ("3", "0-3,8"), or "*" when the mask covers every
    /// online CPU (unpinned).
    pub cpus: String,
    /// Peer (remote) address "ip:port" of this queue's TCP connection,
    /// so the harness can steer the flow's NIC RX to this queue.
    pub peer: String,
}

/// Peer (remote) address of socket `fd` as `"ip:port"`, `"?"` on failure.
/// Used by `LIST_CONTROLLER` so the harness can map a connection's source
/// port to its qid for hardware NIC flow steering.
pub fn peer_of(fd: std::os::fd::RawFd) -> String {
    // SAFETY: a zeroed sockaddr_storage is a valid buffer for getpeername to
    // overwrite; `len` matches its size.
    let mut ss: libc::sockaddr_storage = unsafe { std::mem::zeroed() };
    let mut len = libc::socklen_t::try_from(std::mem::size_of::<libc::sockaddr_storage>())
        .expect("sockaddr_storage fits socklen_t");
    // SAFETY: `fd` is a socket; ss/len describe a valid writable buffer of the
    // stated size.
    let rc = unsafe { libc::getpeername(fd, std::ptr::addr_of_mut!(ss).cast(), &mut len) };
    if rc != 0 {
        return "?".to_owned();
    }
    match i32::from(ss.ss_family) {
        libc::AF_INET => {
            // SAFETY: family is AF_INET, so `ss` is a sockaddr_in.
            let a = unsafe { &*std::ptr::addr_of!(ss).cast::<libc::sockaddr_in>() };
            let ip = std::net::Ipv4Addr::from(u32::from_be(a.sin_addr.s_addr));
            format!("{ip}:{}", u16::from_be(a.sin_port))
        }
        libc::AF_INET6 => {
            // SAFETY: family is AF_INET6, so `ss` is a sockaddr_in6.
            let a = unsafe { &*std::ptr::addr_of!(ss).cast::<libc::sockaddr_in6>() };
            let ip = std::net::Ipv6Addr::from(a.sin6_addr.s6_addr);
            format!("[{ip}]:{}", u16::from_be(a.sin6_port))
        }
        _ => "?".to_owned(),
    }
}

/// Kernel thread id of the calling thread.
pub fn current_tid() -> i32 {
    // SAFETY: gettid has no preconditions and cannot fail.
    unsafe { libc::gettid() }
}

/// Current thread's CPU affinity (see [`cpus_of`]).
pub fn current_cpus() -> String {
    cpus_of(0)
}

/// CPU affinity of thread `tid` (0 = calling thread) as a kernel cpulist
/// ("3", "0-3,8"), "*" when the mask covers every online CPU, "?" if the
/// query fails. Reads the *live* affinity, so it reflects any re-pinning
/// done after the thread started.
pub fn cpus_of(tid: i32) -> String {
    // SAFETY: a zeroed cpu_set_t is a valid value for the call to
    // overwrite; sched_getaffinity writes within size_of::<cpu_set_t>().
    let mut set: libc::cpu_set_t = unsafe { std::mem::zeroed() };
    // SAFETY: `tid` names a thread in this process (0 = calling thread);
    // the buffer is a real cpu_set_t and the size passed matches it.
    let rc =
        unsafe { libc::sched_getaffinity(tid, std::mem::size_of::<libc::cpu_set_t>(), &mut set) };
    if rc != 0 {
        return "?".to_owned();
    }
    // SAFETY: `set` was initialized by sched_getaffinity above.
    let count = unsafe { libc::CPU_COUNT(&set) };
    // SAFETY: sysconf has no preconditions.
    let online = unsafe { libc::sysconf(libc::_SC_NPROCESSORS_ONLN) };
    if online > 0 && i64::from(count) >= online {
        return "*".to_owned();
    }
    fn flush(out: &mut String, run: (usize, usize)) {
        use std::fmt::Write;
        if !out.is_empty() {
            out.push(',');
        }
        let _ = if run.0 == run.1 {
            write!(out, "{}", run.0)
        } else {
            write!(out, "{}-{}", run.0, run.1)
        };
    }
    let mut out = String::new();
    let mut run: Option<(usize, usize)> = None;
    #[allow(clippy::cast_sign_loss)] // CPU_SETSIZE is a positive constant
    for cpu in 0..(libc::CPU_SETSIZE as usize) {
        // SAFETY: cpu < CPU_SETSIZE bounds the bit lookup.
        if unsafe { libc::CPU_ISSET(cpu, &set) } {
            run = match run {
                Some((start, end)) if end + 1 == cpu => Some((start, cpu)),
                Some(prev) => {
                    flush(&mut out, prev);
                    Some((cpu, cpu))
                }
                None => Some((cpu, cpu)),
            };
        }
    }
    if let Some(prev) = run {
        flush(&mut out, prev);
    }
    out
}

/// A live controller's routing info, visible to all threads.
#[derive(Debug, Clone)]
#[allow(missing_docs)] // routing record; fields named per NVMe terms
pub struct ControllerEntry {
    pub cntlid: u16,
    pub subsys_nqn: String,
    pub hostnqn: String,
    /// Highest IO qid this controller may install (offered queue count).
    pub max_qid: u16,
    /// Keep-alive timeout granted at Connect (ms).
    pub kato_ms: u32,
    /// Installed queues, admin first (Connect-time duplicate detection
    /// and LIST_CONTROLLER reporting).
    pub queues: Vec<QueueInfo>,
}

impl ControllerEntry {
    /// The controller is bound to the well-known discovery subsystem.
    pub fn is_discovery(&self) -> bool {
        self.subsys_nqn == crate::fabrics::DISCOVERY_NQN
    }
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

impl Registry {
    /// Create a new, empty registry wrapped in an `Arc`.
    pub fn new() -> Arc<Registry> {
        Arc::new(Registry {
            inner: Mutex::new(RegistryInner {
                next_cntlid: 1,
                controllers: HashMap::new(),
            }),
        })
    }

    /// Allocate a cntlid for a new controller (admin Connect). `max_qid`
    /// is the highest IO queue id the controller may later install;
    /// `admin_queue` is the admin queue's identity (qid 0).
    pub fn allocate(
        &self,
        subsys_nqn: &str,
        hostnqn: &str,
        max_qid: u16,
        kato_ms: u32,
        admin_queue: QueueInfo,
    ) -> Option<u16> {
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
                        max_qid,
                        kato_ms,
                        queues: vec![admin_queue.clone()],
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
        queue: QueueInfo,
    ) -> Result<ControllerEntry, IoConnectError> {
        let mut inner = self.inner.lock().expect("registry poisoned");
        let entry = inner
            .controllers
            .get_mut(&cntlid)
            .ok_or(IoConnectError::UnknownController)?;
        if entry.hostnqn != hostnqn {
            return Err(IoConnectError::HostMismatch);
        }
        // qid 0 is the admin queue; IO qids must be 1..=max_qid (the
        // count granted via Set Features NUM_QUEUES). Rejecting out-of-
        // range qids bounds the queue list and prevents a host creating
        // more queues than advertised.
        if queue.qid == 0 || queue.qid > entry.max_qid {
            return Err(IoConnectError::InvalidQid);
        }
        if entry.queues.iter().any(|q| q.qid == queue.qid) {
            return Err(IoConnectError::QueueExists);
        }
        entry.queues.push(queue);
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

    /// Whether a controller is still live (IO-queue liveness watchdogs poll
    /// this to follow their controller down when its admin queue is gone).
    pub fn contains(&self, cntlid: u16) -> bool {
        self.inner
            .lock()
            .expect("registry poisoned")
            .controllers
            .contains_key(&cntlid)
    }

    /// Number of live controllers.
    pub fn len(&self) -> usize {
        self.inner
            .lock()
            .expect("registry poisoned")
            .controllers
            .len()
    }

    /// True when no controllers are registered.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Clone out all live controllers, sorted by cntlid (control API).
    pub fn snapshot(&self) -> Vec<ControllerEntry> {
        let inner = self.inner.lock().expect("registry poisoned");
        let mut entries: Vec<ControllerEntry> = inner.controllers.values().cloned().collect();
        entries.sort_unstable_by_key(|e| e.cntlid);
        entries
    }
}

/// IO-queue Connect failure reasons (mapped to fabrics status by M4).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(missing_docs)]
pub enum IoConnectError {
    UnknownController,
    HostMismatch,
    QueueExists,
    InvalidQid,
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

    fn qi(qid: u16) -> QueueInfo {
        QueueInfo {
            qid,
            sqsize: 32,
            tid: current_tid(),
            cpus: current_cpus(),
            peer: "127.0.0.1:0".to_owned(),
        }
    }

    #[test]
    fn discovery_entries_flagged() {
        let registry = Registry::new();
        registry
            .allocate(crate::fabrics::DISCOVERY_NQN, "nqn.host", 0, 120_000, qi(0))
            .unwrap();
        let snap = registry.snapshot();
        assert!(snap[0].is_discovery());
        assert_eq!(snap[0].kato_ms, 120_000);
        // max_qid 0: any IO-queue Connect must be rejected.
        assert_eq!(
            registry
                .install_io_queue(snap[0].cntlid, "nqn.host", qi(1))
                .unwrap_err(),
            IoConnectError::InvalidQid
        );
    }

    #[test]
    fn registry_allocates_unique_cntlids() {
        let registry = Registry::new();
        let a = registry
            .allocate("nqn.test", "nqn.host", 4, 60_000, qi(0))
            .unwrap();
        let b = registry
            .allocate("nqn.test", "nqn.host", 4, 60_000, qi(0))
            .unwrap();
        assert_ne!(a, b);
        assert!(a >= 1 && b >= 1);

        // IO queue install: unknown controller rejected, dup qid rejected.
        assert_eq!(
            registry
                .install_io_queue(0xBEEF, "nqn.host", qi(1))
                .unwrap_err(),
            IoConnectError::UnknownController
        );
        registry.install_io_queue(a, "nqn.host", qi(1)).unwrap();
        assert_eq!(
            registry.install_io_queue(a, "nqn.host", qi(1)).unwrap_err(),
            IoConnectError::QueueExists
        );
        assert_eq!(
            registry
                .install_io_queue(a, "nqn.other", qi(2))
                .unwrap_err(),
            IoConnectError::HostMismatch
        );
        // qid 0 and qid > max_qid are rejected.
        assert_eq!(
            registry.install_io_queue(a, "nqn.host", qi(0)).unwrap_err(),
            IoConnectError::InvalidQid
        );
        assert_eq!(
            registry.install_io_queue(a, "nqn.host", qi(5)).unwrap_err(),
            IoConnectError::InvalidQid
        );
        registry.remove(a).unwrap();
        assert_eq!(registry.len(), 1);
    }

    #[test]
    fn snapshot_reports_queues_and_kato() {
        let registry = Registry::new();
        let a = registry
            .allocate("nqn.test", "nqn.host", 4, 5000, qi(0))
            .unwrap();
        registry.install_io_queue(a, "nqn.host", qi(1)).unwrap();
        let snap = registry.snapshot();
        assert_eq!(snap.len(), 1);
        assert_eq!(snap[0].cntlid, a);
        assert_eq!(snap[0].kato_ms, 5000);
        let qids: Vec<u16> = snap[0].queues.iter().map(|q| q.qid).collect();
        assert_eq!(qids, vec![0, 1]);
        // Single-threaded test and qi() records current_tid(), so every
        // queue must carry exactly this thread's tid.
        assert!(snap[0].queues.iter().all(|q| q.tid == current_tid()));
        // Affinity is recorded the same way; single-threaded test, so
        // every queue carries this thread's (non-empty) cpulist.
        assert!(snap[0].queues.iter().all(|q| q.cpus == current_cpus()));
        assert!(!snap[0].queues[0].cpus.is_empty());
        assert!(!snap[0].is_discovery());
    }

    #[test]
    fn snapshot_sorts_by_cntlid() {
        let registry = Registry::new();
        let mut ids: Vec<u16> = (0..3)
            .map(|_| {
                registry
                    .allocate("nqn.test", "nqn.host", 4, 60_000, qi(0))
                    .unwrap()
            })
            .collect();
        ids.sort_unstable();
        // HashMap iteration order is random; snapshot() must still come
        // back sorted ascending by cntlid.
        let snap_ids: Vec<u16> = registry.snapshot().iter().map(|e| e.cntlid).collect();
        assert_eq!(snap_ids, ids);
        assert!(snap_ids.windows(2).all(|w| w[0] < w[1]));
    }
}
