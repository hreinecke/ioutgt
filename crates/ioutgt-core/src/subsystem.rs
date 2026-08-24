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
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

use crate::backend::Backend;

/// Fabric transport serving a port. Protocol wire encodings (e.g. the
/// NVMe-oF discovery-log TRTYPE byte) are the protocol layer's concern.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransportType {
    /// NVMe/TCP.
    Tcp,
    /// NVMe/RDMA (no transport implementation yet; the discovery
    /// plumbing is transport-complete ahead of it).
    Rdma,
}

/// The wire value of an ANA group descriptor's `01h` (Optimized) state.
pub const ANA_STATE_OPTIMIZED: u8 = 0x01;
/// The wire value of an ANA group descriptor's `02h` (Non-Optimized) state.
pub const ANA_STATE_NON_OPTIMIZED: u8 = 0x02;

/// Placeholder ANA group id a namespace starts in, before the control plane
/// has ever computed its real one (a Sheepdog zone id — see
/// `ioutgt-backend::cluster_ana_state`). Any nonzero value works: it is only
/// ever visible in the brief window between a subsystem turning ANA on and
/// its first (synchronous, pre-serving) placement refresh completing, or if
/// that refresh cannot reach the cluster at startup — the same best-effort
/// class of gap the path and host-ACL refreshes have.
const ANA_GRPID_PLACEHOLDER: u32 = 1;

/// One namespace: an NSID bound to a backend.
#[allow(missing_docs)]
pub struct Namespace<B> {
    pub nsid: u32,
    pub backend: Arc<B>,
    /// Namespace UUID (Identify CNS 0x03 descriptor).
    pub uuid: [u8; 16],
    /// ANA group id (Identify Namespace `ANAGRPID`): which zone of the
    /// cluster owns this namespace's placement. Fixed by the cluster's
    /// topology and the object id — the same value however many targets ask,
    /// and through whichever gateway (`ioutgt-backend::cluster_ana_state`) —
    /// so unlike `ana_optimized` it does not depend on which path this is.
    /// Written by the control plane as the ring reshapes under a topology
    /// change, read by Identify Namespace and the ANA log page — never on
    /// the IO path.
    ana_grpid: AtomicU32,
    /// Whether *this* path is a preferred one to the namespace's group: the
    /// gateway this target's cluster connection reaches is itself in
    /// `ana_grpid`'s zone, so reaching the object here costs no extra hop.
    /// Per path by nature — two targets fronting the same cluster through
    /// different gateways can and normally do disagree on this while still
    /// agreeing on `ana_grpid`.
    ana_optimized: AtomicBool,
}

impl<B> Namespace<B> {
    /// Bind `nsid` to `backend`. Starts in the placeholder group, optimized:
    /// until something knows better, this path is as good as any (and for a
    /// subsystem that never reports ANA, neither field is ever looked at).
    pub fn new(nsid: u32, backend: Arc<B>, uuid: [u8; 16]) -> Self {
        Namespace {
            nsid,
            backend,
            uuid,
            ana_grpid: AtomicU32::new(ANA_GRPID_PLACEHOLDER),
            ana_optimized: AtomicBool::new(true),
        }
    }

    /// Whether this path is a preferred one to the namespace (see
    /// `ana_optimized`'s field doc).
    pub fn ana_optimized(&self) -> bool {
        self.ana_optimized.load(Ordering::Relaxed)
    }

    /// The wire value of [`Namespace::ana_optimized`] in an ANA group
    /// descriptor's state field.
    pub fn ana_state_code(&self) -> u8 {
        if self.ana_optimized() {
            ANA_STATE_OPTIMIZED
        } else {
            ANA_STATE_NON_OPTIMIZED
        }
    }

    /// Current ANA group id (Identify Namespace `ANAGRPID`).
    pub fn ana_grpid(&self) -> u32 {
        self.ana_grpid.load(Ordering::Relaxed)
    }
}

impl<B: Backend> Namespace<B> {
    /// Capacity in bytes (Identify Namespace `NVMCAP`).
    ///
    /// 128-bit because the NVMe field is: a byte count in `u64` overflows
    /// long before the spec's does, and summing namespaces
    /// ([`Subsystem::total_capacity`]) only brings that closer.
    pub fn capacity(&self) -> u128 {
        u128::from(self.backend.nr_blocks()) << self.backend.block_shift()
    }
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

/// Parse a hyphenated UUID string (8-4-4-4-12 hex) into its 16 bytes.
pub fn parse_uuid(text: &str) -> Option<[u8; 16]> {
    let b = text.as_bytes();
    if b.len() != 36 || [8, 13, 18, 23].iter().any(|&i| b[i] != b'-') {
        return None;
    }
    let hex: String = text.split('-').collect();
    // The ascii guard also rejects the leading `+` from_str_radix allows;
    // 32 hex digits fit u128 exactly, so the parse cannot fail.
    hex.bytes().all(|c| c.is_ascii_hexdigit()).then(|| {
        u128::from_str_radix(&hex, 16)
            .expect("checked hex")
            .to_be_bytes()
    })
}

/// Format 16 UUID bytes as the hyphenated string form.
pub fn format_uuid(bytes: &[u8; 16]) -> String {
    let v = u128::from_be_bytes(*bytes);
    format!(
        "{:08x}-{:04x}-{:04x}-{:04x}-{:012x}",
        v >> 96,
        (v >> 80) & 0xffff,
        (v >> 64) & 0xffff,
        (v >> 48) & 0xffff,
        v & 0xffff_ffff_ffff
    )
}

/// Immutable namespace-table snapshot.
pub type NsMap<B> = Arc<BTreeMap<u32, Arc<Namespace<B>>>>;

/// One path to a subsystem — a target that serves it, addressed the way a
/// discovery-log entry advertises it.
///
/// A subsystem served by several targets (Sheepdog: every target registered on
/// the cluster ACL of that name) has one of these per target, so a host that
/// discovers it learns every path, not just the one it happened to ask.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubsystemPort {
    /// Transport address (`TRADDR`) the target listens on.
    pub traddr: String,
    /// Service id (`TRSVCID`) — the port, as a string.
    pub trsvcid: String,
    /// Transport serving this path (`TRTYPE`).
    pub trtype: TransportType,
    /// `PORTID`: distinguishes this path from the subsystem's others. Only
    /// its distinctness matters to a host — it keys the ANA/path bookkeeping.
    pub portid: u16,
}

/// Immutable path-list snapshot.
pub type PortList = Arc<Vec<SubsystemPort>>;

/// Who may connect to a subsystem, in nvmet's terms: any host at all, or the
/// hostnqns on a list.
///
/// Kept as one value because the two halves only mean anything together —
/// `allow_any_host` is exactly "ignore `hosts`" — and because a control plane
/// that re-reads the ACL from somewhere else (a Sheepdog ACL object's member
/// names) must be able to swap both at once, with no instant in which a
/// subsystem is open but list-less or closed but not yet populated.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct HostAcl {
    /// Accept any hostnqn, ignoring `hosts`.
    pub allow_any_host: bool,
    /// Hostnqns admitted when `allow_any_host` is off.
    pub hosts: Vec<String>,
}

impl HostAcl {
    /// The ACL that admits everybody — a subsystem nobody has restricted.
    #[must_use]
    pub fn any() -> HostAcl {
        HostAcl {
            allow_any_host: true,
            hosts: Vec::new(),
        }
    }

    /// The ACL admitting exactly `hosts`.
    #[must_use]
    pub fn listed(hosts: Vec<String>) -> HostAcl {
        HostAcl {
            allow_any_host: false,
            hosts,
        }
    }
}

/// An NVM subsystem. Identity is immutable; the namespace table is
/// versioned (see module docs).
pub struct Subsystem<B> {
    /// Subsystem NQN.
    pub nqn: String,
    /// Serial number (Identify Controller `sn`, ≤ 20 ASCII chars).
    pub serial: String,
    /// Model number (`mn`, ≤ 40 ASCII chars).
    pub model: String,
    hosts: RwLock<Arc<HostAcl>>,
    mnan: Option<u32>,
    ana: bool,
    ana_chgcnt: AtomicU64,
    /// Every ANA group id this subsystem might report, ascending and
    /// deduplicated (`NANAGRPID`/`ANAGRPMAX`) — a cluster's zones, which
    /// unlike the fixed 2-group design this replaced can be any count and any
    /// value, so unlike `ports` or `hosts` it can only ever grow (see
    /// [`Subsystem::merge_ana_zones`]).
    ana_zones: RwLock<Arc<Vec<u32>>>,
    namespaces: RwLock<NsMap<B>>,
    generation: AtomicU64,
    ports: RwLock<PortList>,
    disc_genctr: AtomicU64,
}

impl<B: Backend> Subsystem<B> {
    /// Build with an initial namespace table and host ACL.
    pub fn new(
        nqn: String,
        serial: String,
        model: String,
        hosts: HostAcl,
        namespaces: BTreeMap<u32, Arc<Namespace<B>>>,
    ) -> Self {
        Subsystem {
            nqn,
            serial,
            model,
            hosts: RwLock::new(Arc::new(hosts)),
            mnan: None,
            ana: false,
            ana_chgcnt: AtomicU64::new(1),
            ana_zones: RwLock::new(Arc::new(Vec::new())),
            namespaces: RwLock::new(Arc::new(namespaces)),
            generation: AtomicU64::new(1),
            ports: RwLock::new(Arc::new(Vec::new())),
            disc_genctr: AtomicU64::new(1),
        }
    }

    /// Report `mnan` namespaces as Identify Controller `MNAN` (Maximum Number
    /// of Allocated Namespaces).
    ///
    /// Set it where the storage carries its own notion of how many namespaces
    /// the subsystem holds — a Sheepdog ACL object, whose inode sizes its
    /// member table with `max_data_id_nr` — which for sparse NSIDs is the only
    /// count a host can get: `NN` is the highest valid NSID, not a count.
    /// `None` leaves the field zero, the spec's "no more than `NN`".
    #[must_use]
    pub fn with_mnan(mut self, mnan: Option<u32>) -> Self {
        self.mnan = mnan;
        self
    }

    /// Report Asymmetric Namespace Access for this subsystem: CMIC bit 3, the
    /// Identify Controller ANA fields, per-namespace `ANAGRPID`, the ANA log
    /// page, and the ANA Change async event.
    ///
    /// Enable it where the paths to a namespace are genuinely unequal and we
    /// can tell which is which — Sheepdog, where a namespace's ANA group is
    /// the zone of the cluster that owns its placement, and this path is
    /// optimized for it exactly when the gateway we talk to is in that zone.
    /// A subsystem with no such knowledge leaves it off: advertising ANA with
    /// every namespace optimized tells the host nothing and only adds a log
    /// page to poll.
    ///
    /// Seeds [`Subsystem::ana_zones`] with the same placeholder group every
    /// new [`Namespace`] starts in, so `NANAGRPID`/`ANAGRPMAX` are never zero
    /// even in the brief window before the first placement refresh lands (see
    /// `ANA_GRPID_PLACEHOLDER`'s doc).
    #[must_use]
    pub fn with_ana(mut self, ana: bool) -> Self {
        self.ana = ana;
        if ana {
            self.ana_zones = RwLock::new(Arc::new(vec![ANA_GRPID_PLACEHOLDER]));
        }
        self
    }

    /// Whether this subsystem reports ANA ([`Subsystem::with_ana`]).
    pub fn ana(&self) -> bool {
        self.ana
    }

    /// ANA change count: bumped whenever a namespace's group or state
    /// changes, or the group set itself grows, and reported in the log page
    /// header and every group descriptor so a host can tell a re-read raced a
    /// change.
    pub fn ana_chgcnt(&self) -> u64 {
        self.ana_chgcnt.load(Ordering::Acquire)
    }

    /// Every ANA group id this subsystem might report
    /// ([`Subsystem::merge_ana_zones`]), ascending and deduplicated.
    pub fn ana_zones(&self) -> Arc<Vec<u32>> {
        Arc::clone(&self.ana_zones.read().expect("ana zones poisoned"))
    }

    /// Union `zones` into the group set every ANA log page descriptor is
    /// built from. Returns whether this grew the set — the caller turns that
    /// into an ANA Change notice, since a host that already read the log
    /// needs to know a group it never saw now exists.
    ///
    /// Union rather than replace: a subsystem's cluster namespaces can span
    /// more than one Sheepdog cluster, each refreshed independently and each
    /// knowing only its own zones, so no single refresh may drop what another
    /// one contributed. This means the set does not shrink when a cluster's
    /// own zone count does — a rarer event than growth (nodes joining a
    /// cluster is routine; zones disappearing implies nodes leaving for
    /// good), and one a host copes with the same way it copes with any group
    /// a subsystem's current namespaces simply do not use: an empty
    /// descriptor.
    pub fn merge_ana_zones(&self, zones: &[u32]) -> bool {
        let mut guard = self.ana_zones.write().expect("ana zones poisoned");
        if zones.iter().all(|z| guard.contains(z)) {
            return false;
        }
        let mut merged = (**guard).clone();
        merged.extend(zones);
        merged.sort_unstable();
        merged.dedup();
        *guard = Arc::new(merged);
        self.ana_chgcnt.fetch_add(1, Ordering::Release);
        true
    }

    /// Set `ns`'s ANA group and per-path state, bumping the change count if
    /// either changed. Returns whether anything changed — the caller uses
    /// that to decide whether hosts need an ANA Change notice.
    pub fn set_ana_state(&self, ns: &Namespace<B>, grpid: u32, optimized: bool) -> bool {
        let prev_grpid = ns.ana_grpid.swap(grpid, Ordering::Relaxed);
        let prev_optimized = ns.ana_optimized.swap(optimized, Ordering::Relaxed);
        let changed = prev_grpid != grpid || prev_optimized != optimized;
        if changed {
            self.ana_chgcnt.fetch_add(1, Ordering::Release);
        }
        changed
    }

    /// The paths this subsystem is reachable by ([`Subsystem::set_ports`]);
    /// empty until something tells us, which is every subsystem whose paths
    /// are not published anywhere.
    pub fn ports(&self) -> PortList {
        Arc::clone(&self.ports.read().expect("port list poisoned"))
    }

    /// Replace the path list, as the control plane learns it — for Sheepdog,
    /// each refresh of the holders registered on the subsystem's cluster ACL.
    /// Returns whether this changed anything, which the caller turns into a
    /// discovery-log generation bump and a Discovery Log Page Change notice.
    ///
    /// The bump is the caller's, not this method's: seeding the list at startup
    /// is a change too, and there the counter should still read as the version
    /// the storage itself states ([`Subsystem::observe_disc_genctr`]) rather
    /// than one past it.
    ///
    /// Cold path: discovery reads a snapshot per Get Log Page, and IO never
    /// looks at all, so the list needs no generation dance like the namespace
    /// table's.
    pub fn set_ports(&self, ports: Vec<SubsystemPort>) -> bool {
        let mut guard = self.ports.write().expect("port list poisoned");
        if **guard == ports {
            return false;
        }
        *guard = Arc::new(ports);
        true
    }

    /// Builder form of [`Subsystem::set_ports`], for a list known at startup.
    #[must_use]
    pub fn with_ports(self, ports: Vec<SubsystemPort>) -> Self {
        self.set_ports(ports);
        self
    }

    /// This subsystem's share of the discovery log's `GENCTR`: a counter a host
    /// re-reads the log on a change of, and compares across a multi-part read
    /// to tell that one raced a change.
    ///
    /// Monotonic, and moved by both of the things that can change what a
    /// discovery entry for this subsystem says: the version the storage keeps
    /// itself ([`Subsystem::observe_disc_genctr`] — a Sheepdog ACL object's
    /// inode `vdi_epoch`) and the path list this target discovers on its own
    /// ([`Subsystem::bump_disc_genctr`], from `set_ports`).
    pub fn disc_genctr(&self) -> u64 {
        self.disc_genctr.load(Ordering::Acquire)
    }

    /// Take up a generation the storage states for itself, keeping the counter
    /// monotonic: a Sheepdog ACL object's `vdi_epoch`, which the cluster bumps
    /// when the ACL's volume membership changes. Returns whether the counter
    /// moved.
    ///
    /// Monotonic rather than authoritative because it shares the counter with
    /// the local bumps below, and every target must be free to advance it
    /// without the cluster's help: an epoch that has fallen behind the local
    /// count is not applied. Hosts only need `GENCTR` to *change* when the log
    /// does, never to mean anything in particular.
    pub fn observe_disc_genctr(&self, epoch: u64) -> bool {
        self.disc_genctr.fetch_max(epoch, Ordering::Release) < epoch
    }

    /// Note a discovery-log change only this target can see — a peer joining or
    /// leaving the subsystem's path list.
    pub fn bump_disc_genctr(&self) {
        self.disc_genctr.fetch_add(1, Ordering::Release);
    }

    /// The host ACL in force ([`Subsystem::set_host_acl`]).
    pub fn host_acl(&self) -> Arc<HostAcl> {
        Arc::clone(&self.hosts.read().expect("host acl poisoned"))
    }

    /// Replace the host ACL, as the control plane learns it — for a Sheepdog
    /// cluster subsystem, each refresh of its ACL object's member names.
    /// Returns whether this changed anything, which the caller uses to decide
    /// whether the change is worth reporting.
    ///
    /// Takes effect on the next Connect, not on the connections already up:
    /// admission is decided once, when a controller is created, so a host
    /// dropped from the ACL keeps the controller it has until it goes away.
    /// That is nvmet's behaviour too — unlinking a host from a subsystem's
    /// `allowed_hosts` leaves its live controllers alone.
    ///
    /// Cold path, like [`Subsystem::set_ports`]: only Connect reads this.
    pub fn set_host_acl(&self, hosts: HostAcl) -> bool {
        let mut guard = self.hosts.write().expect("host acl poisoned");
        if **guard == hosts {
            return false;
        }
        *guard = Arc::new(hosts);
        true
    }

    /// Host admission (nvmet semantics): any host, or membership in the ACL's
    /// host list.
    pub fn admits(&self, hostnqn: &str) -> bool {
        let acl = self.host_acl();
        acl.allow_any_host || acl.hosts.iter().any(|h| h == hostnqn)
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

    /// Highest allocated NSID (Identify Controller `nn`): the largest NSID a
    /// host may ask about.
    pub fn max_nsid(&self) -> u32 {
        self.snapshot().keys().next_back().copied().unwrap_or(0)
    }

    /// Identify Controller `MNAN` ([`Subsystem::with_mnan`]); `0` where the
    /// storage supplies no count, meaning "no more than `NN`".
    pub fn mnan(&self) -> u32 {
        self.mnan.unwrap_or(0)
    }

    /// Bytes of NVM the subsystem holds (Identify Controller `TNVMCAP`): the
    /// sum of every attached namespace's [`Namespace::capacity`]. Read from
    /// the current snapshot, so an add or remove moves it.
    pub fn total_capacity(&self) -> u128 {
        self.snapshot().values().map(|ns| ns.capacity()).sum()
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
    /// Highest IO queue id offered to controllers (= the port's
    /// IO-thread count; every subsystem on the port shares it).
    pub max_qid: u16,
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

    /// The endpoint this port serves on, put back together from the strings
    /// the discovery log carries. `None` for a transport whose `traddr` is
    /// not an IP address at all (nothing built today), or an unparseable
    /// `trsvcid`.
    pub fn listen_addr(&self) -> Option<std::net::SocketAddr> {
        Some(std::net::SocketAddr::new(
            self.traddr.parse().ok()?,
            self.trsvcid.parse().ok()?,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::{format_uuid, namespace_uuid, parse_uuid};

    #[test]
    fn parse_uuid_accepts_hyphenated_and_rejects_malformed() {
        const TEXT: &str = "00112233-4455-6677-8899-aabbccddeeff";
        const BYTES: [u8; 16] = [
            0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd,
            0xee, 0xff,
        ];
        assert_eq!(parse_uuid(TEXT), Some(BYTES));
        assert_eq!(format_uuid(&BYTES), TEXT);
        for bad in [
            "",
            "00112233-4455-6677-8899-aabbccddeef",   // short
            "001122334455-6677-8899-aabbccddeeffaa", // hyphen misplaced
            "0011223g-4455-6677-8899-aabbccddeeff",  // non-hex
            "+0112233-4455-6677-8899-aabbccddeeff",  // sign accepted by from_str_radix
        ] {
            assert_eq!(parse_uuid(bad), None, "{bad:?}");
        }
    }

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
