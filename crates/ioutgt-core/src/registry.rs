//! Cross-thread controller registry: which controllers live on this
//! port, which queues each has installed, and their routing identity
//! (thread, CPUs, peer). Control-plane rate only (Connect / teardown).

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

/// Traffic-based keep-alive liveness bit, shared by every queue of one
/// controller (Identify Controller `CTRATT.TBKAS`).
///
/// A controller's queues live on different threads, so an IO queue cannot
/// touch its admin queue's keep-alive deadline directly — this flag is the
/// one place they meet. Queues [`set`](Self::set) it when they have seen
/// command traffic; the admin queue's watchdog [`take`](Self::take)s it and
/// treats a set flag as "the host is alive", exactly as nvmet's
/// `reset_tbkas` does.
///
/// Deliberately a flag and not a counter or a timestamp: it is written off
/// the IO path (a queue publishes at most once per keep-alive tick, never
/// per command), and "was there traffic since the last check" is all the
/// watchdog asks.
#[derive(Debug, Default)]
pub struct TrafficFlag(AtomicBool);

impl TrafficFlag {
    /// Record that this controller's host has been heard from.
    pub fn set(&self) {
        self.0.store(true, Ordering::Relaxed);
    }

    /// Consume the flag: whether traffic was recorded since the last take.
    pub fn take(&self) -> bool {
        self.0.swap(false, Ordering::Relaxed)
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
    /// Bound to the protocol's well-known discovery subsystem (serves
    /// log pages, no storage). Decided by the protocol layer at
    /// [`Registry::allocate`] time.
    pub discovery: bool,
    /// Traffic-based keep-alive liveness, shared by every queue of this
    /// controller: an IO queue sets it, the admin queue's keep-alive
    /// watchdog takes it. Cloned along with the entry, so an IO queue gets
    /// the flag straight out of [`Registry::install_io_queue`].
    pub traffic: Arc<TrafficFlag>,
}

/// Cross-thread controller registry. Control-plane rate only (Connect /
/// teardown); a mutex is fine.
pub struct Registry {
    /// Allocatable cntlid range, inclusive. CNTLIDs are unique per
    /// subsystem on the wire; a target split across processes (one per
    /// port) gives each process a disjoint slice so two paths to the
    /// same subsystem can never mint the same cntlid (Linux hosts
    /// reject a duplicate: `nvme_validate_cntlid`).
    cntlid_min: u16,
    cntlid_max: u16,
    inner: Mutex<RegistryInner>,
}

#[derive(Default)]
struct RegistryInner {
    next_cntlid: u16,
    controllers: HashMap<u16, ControllerEntry>,
}

/// Highest allocatable cntlid: 0xFFF0..=0xFFFF are reserved (0xFFFF is
/// the dynamic-controller wildcard in Connect), matching kernel nvmet.
pub const CNTLID_MAX: u16 = 0xFFEF;

impl Registry {
    /// Create a new, empty registry allocating cntlids from the
    /// inclusive `[cntlid_min, cntlid_max]` slice (callers serving a
    /// whole target pass `1..=`[`CNTLID_MAX`]).
    pub fn new(cntlid_min: u16, cntlid_max: u16) -> Arc<Registry> {
        assert!(
            1 <= cntlid_min && cntlid_min <= cntlid_max && cntlid_max <= CNTLID_MAX,
            "cntlid range {cntlid_min}..={cntlid_max} invalid"
        );
        Arc::new(Registry {
            cntlid_min,
            cntlid_max,
            inner: Mutex::new(RegistryInner {
                next_cntlid: cntlid_min,
                controllers: HashMap::new(),
            }),
        })
    }

    /// Allocate a cntlid for a new controller (admin Connect). `max_qid`
    /// is the highest IO queue id the controller may later install;
    /// `admin_queue` is the admin queue's identity (qid 0); `discovery`
    /// marks a controller bound to the well-known discovery subsystem.
    pub fn allocate(
        &self,
        subsys_nqn: &str,
        hostnqn: &str,
        max_qid: u16,
        kato_ms: u32,
        admin_queue: QueueInfo,
        discovery: bool,
    ) -> Option<u16> {
        let mut inner = self.inner.lock().expect("registry poisoned");
        // Linear scan for a free id within the slice: controller counts
        // are tiny. u32 arithmetic so start+offset cannot wrap.
        let span = u32::from(self.cntlid_max - self.cntlid_min) + 1;
        let start = u32::from(inner.next_cntlid.clamp(self.cntlid_min, self.cntlid_max));
        for offset in 0..span {
            let index = (start - u32::from(self.cntlid_min) + offset) % span;
            let cntlid = self.cntlid_min + u16::try_from(index).expect("index < span <= u16 range");
            let inserted = match inner.controllers.entry(cntlid) {
                std::collections::hash_map::Entry::Vacant(slot) => {
                    slot.insert(ControllerEntry {
                        cntlid,
                        subsys_nqn: subsys_nqn.to_owned(),
                        hostnqn: hostnqn.to_owned(),
                        max_qid,
                        kato_ms,
                        queues: vec![admin_queue.clone()],
                        discovery,
                        traffic: Arc::new(TrafficFlag::default()),
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

    /// This controller's traffic-based keep-alive flag (the admin queue
    /// picks it up right after [`allocate`](Self::allocate); IO queues get
    /// it from their [`install_io_queue`](Self::install_io_queue) entry).
    pub fn traffic(&self, cntlid: u16) -> Option<Arc<TrafficFlag>> {
        self.inner
            .lock()
            .expect("registry poisoned")
            .controllers
            .get(&cntlid)
            .map(|entry| Arc::clone(&entry.traffic))
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

    /// Discovery well-known NQN (NVMe-oF); tests exercise the flag, the
    /// registry itself treats the name as opaque.
    const DISCOVERY_NQN: &str = "nqn.2014-08.org.nvmexpress.discovery";

    fn qi(qid: u16) -> QueueInfo {
        QueueInfo {
            qid,
            sqsize: 32,
            tid: 42,
            cpus: "0-3".to_owned(),
            peer: "127.0.0.1:0".to_owned(),
        }
    }

    #[test]
    fn discovery_entries_flagged() {
        let registry = Registry::new(1, super::CNTLID_MAX);
        registry
            .allocate(DISCOVERY_NQN, "nqn.host", 0, 120_000, qi(0), true)
            .unwrap();
        let snap = registry.snapshot();
        assert!(snap[0].discovery);
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
    fn cntlid_slice_respected_and_exhausted() {
        // A narrow slice: every id stays inside it, allocation wraps to
        // reclaim freed ids, and an exhausted slice refuses.
        let registry = Registry::new(10, 12);
        let ids: Vec<u16> = (0..3)
            .map(|_| {
                registry
                    .allocate("nqn.a", "nqn.host", 1, 0, qi(0), false)
                    .unwrap()
            })
            .collect();
        assert_eq!(ids, [10, 11, 12]);
        assert!(
            registry
                .allocate("nqn.a", "nqn.host", 1, 0, qi(0), false)
                .is_none(),
            "slice exhausted"
        );
        registry.remove(11).unwrap();
        assert_eq!(
            registry
                .allocate("nqn.a", "nqn.host", 1, 0, qi(0), false)
                .unwrap(),
            11,
            "freed id reclaimed within the slice"
        );
    }

    #[test]
    fn registry_allocates_unique_cntlids() {
        let registry = Registry::new(1, super::CNTLID_MAX);
        let a = registry
            .allocate("nqn.test", "nqn.host", 4, 60_000, qi(0), false)
            .unwrap();
        let b = registry
            .allocate("nqn.test", "nqn.host", 4, 60_000, qi(0), false)
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
        let registry = Registry::new(1, super::CNTLID_MAX);
        let a = registry
            .allocate("nqn.test", "nqn.host", 4, 5000, qi(0), false)
            .unwrap();
        registry.install_io_queue(a, "nqn.host", qi(1)).unwrap();
        let snap = registry.snapshot();
        assert_eq!(snap.len(), 1);
        assert_eq!(snap[0].cntlid, a);
        assert_eq!(snap[0].kato_ms, 5000);
        let qids: Vec<u16> = snap[0].queues.iter().map(|q| q.qid).collect();
        assert_eq!(qids, vec![0, 1]);
        // Snapshot echoes the identity recorded at install time.
        assert!(snap[0].queues.iter().all(|q| q.tid == 42));
        assert!(snap[0].queues.iter().all(|q| q.cpus == "0-3"));
        assert!(!snap[0].queues[0].cpus.is_empty());
        assert!(!snap[0].discovery);
    }

    #[test]
    fn traffic_flag_shared_by_all_queues() {
        let registry = Registry::new(1, super::CNTLID_MAX);
        let a = registry
            .allocate("nqn.test", "nqn.host", 4, 5000, qi(0), false)
            .unwrap();
        let admin = registry.traffic(a).unwrap();
        // The IO queue's entry carries the very same flag, which is the
        // whole point: setting it there is visible to the admin watchdog.
        let io = registry.install_io_queue(a, "nqn.host", qi(1)).unwrap();
        assert!(!admin.take());
        io.traffic.set();
        assert!(admin.take(), "io traffic visible to the admin queue");
        assert!(!admin.take(), "take consumes the flag");
        // Each controller gets its own flag.
        let b = registry
            .allocate("nqn.test", "nqn.host", 4, 5000, qi(0), false)
            .unwrap();
        registry.traffic(b).unwrap().set();
        assert!(!admin.take());
        assert!(registry.traffic(0xBEEF).is_none());
    }

    #[test]
    fn snapshot_sorts_by_cntlid() {
        let registry = Registry::new(1, super::CNTLID_MAX);
        let mut ids: Vec<u16> = (0..3)
            .map(|_| {
                registry
                    .allocate("nqn.test", "nqn.host", 4, 60_000, qi(0), false)
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
