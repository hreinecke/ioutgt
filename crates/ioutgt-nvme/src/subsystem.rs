//! Subsystems, namespaces, and the port configuration shared with queue
//! threads.
//!
//! The namespace table supports runtime add/remove while IO queues stay
//! lock-free: readers cache an `Arc` snapshot and revalidate it with one
//! relaxed atomic generation load per command, refreshing only when the
//! control plane changed something (the userspace analog of nvmet's
//! xarray + RCU table).

use std::cell::{Cell, RefCell};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

use ioutgt_core::backend::Backend;

/// Fabric transport serving a port; selects the TRTYPE byte in
/// discovery log entries (NVMe-oF: RDMA = 1, TCP = 3).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransportType {
    /// NVMe/TCP.
    Tcp,
    /// NVMe/RDMA (no transport implementation yet; the discovery
    /// plumbing is transport-complete ahead of it).
    Rdma,
}

impl TransportType {
    /// Discovery-log TRTYPE encoding.
    pub fn trtype(self) -> u8 {
        match self {
            TransportType::Tcp => crate::fabrics::trtype::TCP,
            TransportType::Rdma => crate::fabrics::trtype::RDMA,
        }
    }
}

/// One namespace: an NSID bound to a backend.
#[allow(missing_docs)]
pub struct Namespace<B> {
    pub nsid: u32,
    pub backend: Arc<B>,
    /// Namespace UUID (Identify CNS 0x03 descriptor).
    pub uuid: [u8; 16],
}

/// Derive a namespace's 16-byte UUID (Identify CNS 03h descriptor) from its
/// owning subsystem NQN and its NSID.
///
/// The NVMe host dedups namespaces by this identifier across the *whole host*,
/// not per subsystem — so it must be unique per `(subsystem, nsid)`, otherwise
/// two ioutgt subsystems serving the same NSID collide and the host keeps only
/// one block device (`ignoring nsid N because of duplicate IDs`). This is how
/// nvmet behaves too (each namespace gets its own `device_uuid`).
///
/// Deterministic — stable across restarts so persistent naming and multipath
/// stay consistent: an FNV-1a hash of the NQN fills the high 8 bytes, a marker
/// byte follows, and the NSID occupies the low 4 bytes.
pub fn namespace_uuid(nqn: &str, nsid: u32) -> [u8; 16] {
    // FNV-1a, 64-bit: deterministic and dependency-free.
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for &b in nqn.as_bytes() {
        hash ^= u64::from(b);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    let mut uuid = [0u8; 16];
    uuid[0..8].copy_from_slice(&hash.to_be_bytes());
    uuid[8] = 0x80;
    uuid[12..16].copy_from_slice(&nsid.to_be_bytes());
    uuid
}

/// Immutable namespace-table snapshot.
pub type NsMap<B> = Arc<BTreeMap<u32, Arc<Namespace<B>>>>;

/// An NVM subsystem. Identity is immutable; the namespace table is
/// versioned (see module docs).
pub struct Subsystem<B> {
    /// Subsystem NQN.
    pub nqn: String,
    /// Serial number (Identify Controller `sn`, ≤ 20 ASCII chars).
    pub serial: String,
    /// Model number (`mn`, ≤ 40 ASCII chars).
    pub model: String,
    /// Highest IO queue id offered to controllers (≤ IO threads).
    pub max_qid: u16,
    /// Accept any hostnqn (host ACLs are future control-plane work).
    pub allow_any_host: bool,
    namespaces: RwLock<NsMap<B>>,
    generation: AtomicU64,
}

impl<B: Backend> Subsystem<B> {
    /// Build with an initial namespace table.
    pub fn new(
        nqn: String,
        serial: String,
        model: String,
        max_qid: u16,
        allow_any_host: bool,
        namespaces: BTreeMap<u32, Arc<Namespace<B>>>,
    ) -> Self {
        Subsystem {
            nqn,
            serial,
            model,
            max_qid,
            allow_any_host,
            namespaces: RwLock::new(Arc::new(namespaces)),
            generation: AtomicU64::new(1),
        }
    }

    /// Current table snapshot (control plane and admin/cold paths).
    pub fn snapshot(&self) -> NsMap<B> {
        Arc::clone(&self.namespaces.read().expect("ns table poisoned"))
    }

    /// Table version; bumped on every change.
    pub fn generation(&self) -> u64 {
        self.generation.load(Ordering::Acquire)
    }

    /// Add a namespace. Errors if the NSID is taken.
    pub fn add_namespace(&self, ns: Namespace<B>) -> Result<(), String> {
        let mut guard = self.namespaces.write().expect("ns table poisoned");
        if guard.contains_key(&ns.nsid) {
            return Err(format!("nsid {} already exists", ns.nsid));
        }
        let mut table = BTreeMap::clone(guard.as_ref());
        table.insert(ns.nsid, Arc::new(ns));
        *guard = Arc::new(table);
        self.generation.fetch_add(1, Ordering::Release);
        Ok(())
    }

    /// Remove a namespace; in-flight IO holding the old snapshot
    /// completes against the still-alive backend Arc.
    pub fn remove_namespace(&self, nsid: u32) -> Result<(), String> {
        let mut guard = self.namespaces.write().expect("ns table poisoned");
        if !guard.contains_key(&nsid) {
            return Err(format!("nsid {nsid} not found"));
        }
        let mut table = BTreeMap::clone(guard.as_ref());
        table.remove(&nsid);
        *guard = Arc::new(table);
        self.generation.fetch_add(1, Ordering::Release);
        Ok(())
    }

    /// Highest allocated NSID (Identify Controller `nn`).
    pub fn max_nsid(&self) -> u32 {
        self.snapshot().keys().next_back().copied().unwrap_or(0)
    }
}

/// Per-connection generation-validated cache of a subsystem's table:
/// one atomic generation load per command, an `Arc` refresh only when
/// the control plane changed the table.
pub struct NsCache<B> {
    generation: Cell<u64>,
    map: RefCell<Option<NsMap<B>>>,
}

impl<B: Backend> Default for NsCache<B> {
    fn default() -> Self {
        NsCache {
            generation: Cell::new(0),
            map: RefCell::new(None),
        }
    }
}

impl<B: Backend> NsCache<B> {
    /// Current table for `subsys`, refreshed if stale.
    pub fn get(&self, subsys: &Subsystem<B>) -> NsMap<B> {
        let generation = subsys.generation();
        if self.generation.get() != generation || self.map.borrow().is_none() {
            *self.map.borrow_mut() = Some(subsys.snapshot());
            self.generation.set(generation);
        }
        self.map.borrow().as_ref().expect("filled above").clone()
    }
}

/// Everything a queue thread needs to serve one port: the subsystems
/// reachable through it. Shared read-only across threads.
pub struct PortConfig<B> {
    /// Listen address, as advertised in the discovery log.
    pub traddr: String,
    /// Port number as a string (`trsvcid`).
    pub trsvcid: String,
    /// Transport serving this port (TRTYPE in discovery entries).
    pub trtype: TransportType,
    /// Advertised IO MAXCMD ceiling (Identify Controller): the maximum
    /// IO queue depth in entries the host may use. The host clamps each
    /// IO queue to `min(its queue-size, this)`; the admin queue is
    /// unaffected. Bounded by `MAX_QUEUE_ENTRIES`.
    pub io_queue_size: u16,
    /// Per-IO-queue data-buffer pool size in bytes. Slots lease their
    /// read/write buffers from this shared arena on demand.
    pub queue_buf_bytes: usize,
    /// Per-CONNECTION receive-ring size in bytes (`0` = ring off, the classic
    /// per-recv scratch buffer). When non-zero and the kernel supports
    /// provided-buffer rings, each IO connection owns a ring of this size and
    /// recv draws chunks from it, retaining write payloads zero-copy; memory
    /// scales as (connections × this).
    pub recv_buf_bytes: usize,
    /// Poll mode: the transport busy-polls its completion sources on the
    /// queue thread instead of sleeping on events (one core per IO thread,
    /// SPDK-style; latency over CPU). Wired from the binary's `--poll`.
    pub poll: bool,
    /// NQN → subsystem.
    pub subsystems: BTreeMap<String, Arc<Subsystem<B>>>,
}

impl<B: Backend> PortConfig<B> {
    /// Look up a subsystem by NQN.
    pub fn subsystem(&self, nqn: &str) -> Option<&Arc<Subsystem<B>>> {
        self.subsystems.get(nqn)
    }
}

#[cfg(test)]
mod tests {
    use super::namespace_uuid;

    #[test]
    fn namespace_uuid_is_deterministic() {
        assert_eq!(
            namespace_uuid("nqn.2026-06.io.ioutgt:a", 1),
            namespace_uuid("nqn.2026-06.io.ioutgt:a", 1),
        );
    }

    #[test]
    fn namespace_uuid_differs_by_subsystem_and_nsid() {
        let a1 = namespace_uuid("nqn.2026-06.io.ioutgt:a", 1);
        let b1 = namespace_uuid("nqn.2026-06.io.ioutgt:b", 1);
        let a2 = namespace_uuid("nqn.2026-06.io.ioutgt:a", 2);
        // Same nsid, different subsystem must not collide (the host dedups by
        // this identifier across the whole host — the two-ioutgt-target case).
        assert_ne!(a1, b1);
        // Same subsystem, different nsid also distinct.
        assert_ne!(a1, a2);
        // Never the all-zero UUID (which the host treats as "no identifier").
        assert_ne!(a1, [0u8; 16]);
    }

    #[test]
    fn namespace_uuid_encodes_nsid_in_low_bytes() {
        let u = namespace_uuid("nqn.2026-06.io.ioutgt:a", 0x0102_0304);
        assert_eq!(&u[12..16], &[0x01, 0x02, 0x03, 0x04]);
        assert_eq!(u[8], 0x80);
    }
}
