//! Target-model configuration: the structures an nvmetcli-format
//! config file ([`crate::nvmet`]) loads into, and the control-API
//! backend payload.

#![allow(missing_docs)] // schema: field names are the documented format

use std::path::PathBuf;

use serde::{Deserialize, Serialize};

/// One NVM subsystem.
#[derive(Debug, Clone)]
pub struct SubsystemConfig {
    pub nqn: String,
    pub serial: String,
    pub model: String,
    pub allow_any_host: bool,
    /// Hostnqns admitted when `allow_any_host` is off (nvmet-style ACL).
    pub allowed_hosts: Vec<String>,
    pub namespaces: Vec<NamespaceConfig>,
}

pub(crate) fn default_serial() -> String {
    "IOUTGT0001".into()
}

pub(crate) fn default_model() -> String {
    "ioutgt".into()
}

/// One namespace.
#[derive(Debug, Clone)]
pub struct NamespaceConfig {
    pub nsid: u32,
    pub backend: BackendConfig,
    /// Namespace UUID (nvmet's `device.uuid`); `None` derives one from
    /// (subsystem NQN, nsid). Set to keep host-visible identity
    /// (`/dev/disk/by-id`) across targets.
    pub uuid: Option<[u8; 16]>,
}

/// Backend selection (the ADD_NAMESPACE control payload; config-file
/// namespaces are always file/bdev-backed, as in the kernel).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase", deny_unknown_fields)]
pub enum BackendConfig {
    /// RAM-backed.
    Memory { size_mb: u64 },
    /// Discard writes, zero reads.
    Null { size_mb: u64 },
    /// O_DIRECT file or block device.
    File { path: PathBuf },
    /// A VDI on a Sheepdog cluster (`addr` is `host:port`; `tag` selects a
    /// snapshot, read-only).
    Sheepdog {
        addr: String,
        vdi: String,
        #[serde(default)]
        tag: Option<String>,
        /// ACL object the VDI belongs to, by name. The cluster resolves a
        /// VDI's name only for a lookup naming the ACL its inode records, so
        /// this must match or the namespace cannot be opened at all; `None`
        /// addresses a VDI in no ACL.
        #[serde(default)]
        acl: Option<String>,
        /// Take the cluster's VDI lock (default). Under an ACL the lock is
        /// shared: other targets serving the same ACL may hold the same VDI —
        /// two of them exporting it for multipath, say — while a client
        /// holding it exclusively, such as a running QEMU guest, keeps this
        /// one out. Without an ACL the lock is itself exclusive. Turn it off
        /// where exclusion is arranged some other way. Snapshots are read-only
        /// and never locked.
        #[serde(default = "lock_by_default")]
        lock: bool,
    },
}

/// serde default for `Sheepdog { lock }`: locking is on.
fn lock_by_default() -> bool {
    true
}

/// Structural validation of a subsystem list, whatever source built it.
pub fn validate_subsystems(subsystems: &[SubsystemConfig]) -> Result<(), String> {
    if subsystems.is_empty() {
        return Err("at least one subsystem is required".into());
    }
    for subsys in subsystems {
        if subsys.nqn.is_empty() || subsys.nqn.len() > 223 {
            return Err(format!(
                "subsystem nqn '{}' invalid (1..=223 chars)",
                subsys.nqn
            ));
        }
        let mut seen = std::collections::BTreeSet::new();
        for ns in &subsys.namespaces {
            if ns.nsid == 0 || ns.nsid == u32::MAX {
                return Err(format!("{}: nsid {} is reserved", subsys.nqn, ns.nsid));
            }
            if !seen.insert(ns.nsid) {
                return Err(format!("{}: duplicate nsid {}", subsys.nqn, ns.nsid));
            }
            if let BackendConfig::Memory { size_mb } | BackendConfig::Null { size_mb } = &ns.backend
                && *size_mb == 0
            {
                return Err(format!(
                    "{}: nsid {}: size_mb must be > 0",
                    subsys.nqn, ns.nsid
                ));
            }
            if let BackendConfig::Sheepdog { addr, vdi, .. } = &ns.backend
                && (addr.is_empty() || vdi.is_empty())
            {
                return Err(format!(
                    "{}: nsid {}: sheepdog addr and vdi must be non-empty",
                    subsys.nqn, ns.nsid
                ));
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn subsystem(namespaces: Vec<NamespaceConfig>) -> SubsystemConfig {
        SubsystemConfig {
            nqn: "nqn.2026-06.io.ioutgt:a".into(),
            serial: default_serial(),
            model: default_model(),
            allow_any_host: true,
            allowed_hosts: vec![],
            namespaces,
        }
    }

    fn mem(nsid: u32, size_mb: u64) -> NamespaceConfig {
        NamespaceConfig {
            nsid,
            backend: BackendConfig::Memory { size_mb },
            uuid: None,
        }
    }

    #[test]
    fn valid_subsystems_pass() {
        validate_subsystems(&[subsystem(vec![mem(1, 64), mem(2, 64)])]).unwrap();
    }

    #[test]
    fn structural_defects_rejected() {
        assert!(
            validate_subsystems(&[])
                .unwrap_err()
                .contains("at least one subsystem")
        );
        assert!(
            validate_subsystems(&[subsystem(vec![mem(1, 1), mem(1, 1)])])
                .unwrap_err()
                .contains("duplicate nsid")
        );
        assert!(
            validate_subsystems(&[subsystem(vec![mem(0, 1)])])
                .unwrap_err()
                .contains("reserved")
        );
        assert!(
            validate_subsystems(&[subsystem(vec![mem(1, 0)])])
                .unwrap_err()
                .contains("size_mb")
        );
        let mut long_nqn = subsystem(vec![mem(1, 1)]);
        long_nqn.nqn = "n".repeat(224);
        assert!(
            validate_subsystems(&[long_nqn])
                .unwrap_err()
                .contains("nqn")
        );
    }
}
