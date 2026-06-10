//! Subsystems, namespaces, and the port configuration handed to queue
//! threads at startup.

use std::collections::BTreeMap;
use std::sync::Arc;

use crate::backend::Backend;

/// One namespace: an NSID bound to a backend.
#[allow(missing_docs)]
pub struct Namespace<B> {
    pub nsid: u32,
    pub backend: Arc<B>,
    /// Namespace UUID (Identify CNS 0x03 descriptor).
    pub uuid: [u8; 16],
}

/// An NVM subsystem definition (immutable snapshot; runtime namespace
/// changes arrive as fresh snapshots via queue-thread mailboxes in a
/// later milestone).
pub struct Subsystem<B> {
    /// Subsystem NQN.
    pub nqn: String,
    /// Serial number (Identify Controller `sn`, ≤ 20 ASCII chars).
    pub serial: String,
    /// Model number (`mn`, ≤ 40 ASCII chars).
    pub model: String,
    /// NSID → namespace, ordered (Identify active-NS list requirement).
    pub namespaces: BTreeMap<u32, Arc<Namespace<B>>>,
    /// Highest IO queue id offered to controllers (≤ number of IO
    /// threads).
    pub max_qid: u16,
    /// Accept any hostnqn (host ACLs arrive with the control plane).
    pub allow_any_host: bool,
}

impl<B: Backend> Subsystem<B> {
    /// Look up a namespace by NSID.
    pub fn namespace(&self, nsid: u32) -> Option<&Arc<Namespace<B>>> {
        self.namespaces.get(&nsid)
    }

    /// Highest allocated NSID (Identify Controller `nn`).
    pub fn max_nsid(&self) -> u32 {
        self.namespaces.keys().next_back().copied().unwrap_or(0)
    }
}

/// Everything a queue thread needs to serve one port: the subsystems
/// reachable through it. Shared read-only across threads.
pub struct PortConfig<B> {
    /// Listen address, as advertised in the discovery log.
    pub traddr: String,
    /// Port number as a string (`trsvcid`).
    pub trsvcid: String,
    /// NQN → subsystem.
    pub subsystems: BTreeMap<String, Arc<Subsystem<B>>>,
}

impl<B: Backend> PortConfig<B> {
    /// Look up a subsystem by NQN.
    pub fn subsystem(&self, nqn: &str) -> Option<&Arc<Subsystem<B>>> {
        self.subsystems.get(nqn)
    }
}
