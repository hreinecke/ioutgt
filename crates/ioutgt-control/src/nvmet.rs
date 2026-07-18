//! nvmetcli-compatible JSON configuration.
//!
//! nvmetcli's `save`/`restore` serializes the kernel nvmet configfs
//! tree: top-level `hosts`/`ports`/`subsystems` arrays whose attribute
//! groups (`attr`, `addr`, `device`, …) hold string-typed configfs
//! attributes. This module maps that schema onto [`FileConfig`] so an
//! existing `/etc/nvmet/config.json` drives ioutgt unchanged.
//!
//! Attributes with no ioutgt counterpart (`param`, `ana_groups`,
//! `referrals`, PI/cntlid tuning, …) are accepted and ignored, like
//! nvmetcli's own error-skipping restore. Engine tuning (io_threads,
//! buffer sizes, …) has no configfs home and keeps its defaults.

use std::collections::BTreeMap;

use ioutgt_core::subsystem::TransportType;
use serde::Deserialize;

use crate::config::{BackendConfig, FileConfig, NamespaceConfig, SubsystemConfig};

#[derive(Deserialize)]
struct NvmetConfig {
    #[serde(default)]
    ports: Vec<Port>,
    #[serde(default)]
    subsystems: Vec<Subsystem>,
    // `hosts` only declares NQNs; `allowed_hosts` entries carry them.
}

#[derive(Deserialize)]
struct Port {
    addr: Addr,
    #[serde(default)]
    subsystems: Vec<String>,
}

#[derive(Deserialize)]
struct Addr {
    #[serde(default)]
    traddr: String,
    #[serde(default)]
    trsvcid: String,
    #[serde(default)]
    trtype: String,
}

#[derive(Deserialize)]
struct Subsystem {
    nqn: String,
    #[serde(default)]
    attr: BTreeMap<String, String>,
    #[serde(default)]
    allowed_hosts: Vec<String>,
    #[serde(default)]
    namespaces: Vec<Namespace>,
}

#[derive(Deserialize)]
struct Namespace {
    nsid: u32,
    #[serde(default)]
    device: BTreeMap<String, String>,
    #[serde(default = "default_enable")]
    enable: u8,
}

fn default_enable() -> u8 {
    1
}

/// Convert a parsed nvmetcli-format document into the [`FileConfig`]
/// for the port serving `trtype`.
pub(crate) fn to_file_config(
    value: serde_json::Value,
    trtype: TransportType,
) -> Result<FileConfig, String> {
    let config: NvmetConfig =
        serde_json::from_value(value).map_err(|e| format!("nvmet-format config: {e}"))?;
    let want = match trtype {
        TransportType::Tcp => "tcp",
        TransportType::Rdma => "rdma",
    };
    let mut matching = config.ports.iter().filter(|p| p.addr.trtype == want);
    let Some(port) = matching.next() else {
        return Err(format!("nvmet config has no {want} port"));
    };
    if matching.next().is_some() {
        return Err(format!(
            "nvmet config has multiple {want} ports; ioutgt serves one port per process"
        ));
    }
    // An IPv6 traddr needs brackets to parse as a SocketAddr.
    let traddr = &port.addr.traddr;
    let listen = if traddr.contains(':') {
        format!("[{traddr}]:{}", port.addr.trsvcid)
    } else {
        format!("{traddr}:{}", port.addr.trsvcid)
    };
    // Only subsystems exported on the port are reachable (in configfs,
    // the port holds symlinks to them).
    let mut subsystems = Vec::new();
    for nqn in &port.subsystems {
        let Some(subsys) = config.subsystems.iter().find(|s| &s.nqn == nqn) else {
            return Err(format!("port exports undefined subsystem '{nqn}'"));
        };
        subsystems.push(subsys.to_config()?);
    }
    Ok(FileConfig::engine_defaults(listen, subsystems))
}

impl Subsystem {
    fn to_config(&self) -> Result<SubsystemConfig, String> {
        let mut namespaces = Vec::new();
        for ns in &self.namespaces {
            // Kernel semantics: a disabled namespace exists in configfs
            // but is invisible to hosts.
            if ns.enable == 0 {
                continue;
            }
            let Some(path) = ns.device.get("path") else {
                return Err(format!("{}: nsid {}: no device path", self.nqn, ns.nsid));
            };
            namespaces.push(NamespaceConfig {
                nsid: ns.nsid,
                backend: BackendConfig::File { path: path.into() },
                // nguid has no ioutgt counterpart and is not conflated.
                uuid: ns.device.get("uuid").cloned(),
            });
        }
        Ok(SubsystemConfig {
            nqn: self.nqn.clone(),
            serial: self
                .attr
                .get("serial")
                .cloned()
                .unwrap_or_else(crate::config::default_serial),
            model: self
                .attr
                .get("model")
                .cloned()
                .unwrap_or_else(crate::config::default_model),
            // Kernel default: deny unless listed.
            allow_any_host: self.attr.get("allow_any_host").is_some_and(|v| v == "1"),
            allowed_hosts: self.allowed_hosts.clone(),
            namespaces,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(json: &str, trtype: TransportType) -> Result<FileConfig, String> {
        FileConfig::parse(json, trtype)
    }

    /// A full `nvmetcli save`-shaped document — every field the kernel
    /// dumps for an rdma port (attr groups, portid, referrals, treq).
    const RDMA_EXAMPLE: &str = r#"{
      "hosts": [ { "nqn": "nqn.2014-08.org.nvmexpress:uuid:host-1" } ],
      "ports": [
        {
          "addr": { "adrfam": "ipv4", "traddr": "10.0.0.7",
                    "treq": "not specified", "trsvcid": "4420", "trtype": "rdma" },
          "portid": 2, "referrals": [], "subsystems": [ "nqn.2026-06.io.ioutgt:rd" ]
        }
      ],
      "subsystems": [
        {
          "allowed_hosts": [],
          "attr": { "allow_any_host": "1" },
          "namespaces": [
            { "device": { "nguid": "0a651b8c-2d13-44e4-9c7f-5e00d1f0a923",
                          "path": "/dev/nvme9n1" },
              "enable": 1, "nsid": 1 }
          ],
          "nqn": "nqn.2026-06.io.ioutgt:rd"
        }
      ]
    }"#;

    #[test]
    fn nvmetcli_rdma_example_loads() {
        let config = parse(RDMA_EXAMPLE, TransportType::Rdma).unwrap();
        assert_eq!(config.listen, "10.0.0.7:4420");
        assert_eq!(config.subsystems.len(), 1);
        let subsys = &config.subsystems[0];
        assert_eq!(subsys.nqn, "nqn.2026-06.io.ioutgt:rd");
        assert!(subsys.allow_any_host);
        assert_eq!(subsys.namespaces.len(), 1);
        assert_eq!(subsys.namespaces[0].nsid, 1);
        let BackendConfig::File { path } = &subsys.namespaces[0].backend else {
            panic!("expected file backend");
        };
        assert_eq!(path.to_str(), Some("/dev/nvme9n1"));
    }

    #[test]
    fn transport_mismatch_rejected() {
        assert!(
            parse(RDMA_EXAMPLE, TransportType::Tcp)
                .unwrap_err()
                .contains("no tcp port")
        );
    }

    #[test]
    fn acl_and_serial_attrs_map() {
        let config = parse(
            r#"{ "ports": [ { "addr": { "traddr": "127.0.0.1", "trsvcid": "4420",
                              "trtype": "tcp" }, "subsystems": [ "nqn.a" ] } ],
                 "subsystems": [ { "nqn": "nqn.a",
                   "attr": { "allow_any_host": "0", "serial": "SN123", "model": "Linux" },
                   "allowed_hosts": [ "hostnqn" ],
                   "namespaces": [ { "nsid": 1, "device": { "path": "/dev/sda" } } ] } ] }"#,
            TransportType::Tcp,
        )
        .unwrap();
        let subsys = &config.subsystems[0];
        assert!(!subsys.allow_any_host);
        assert_eq!(subsys.allowed_hosts, ["hostnqn"]);
        assert_eq!(subsys.serial, "SN123");
        assert_eq!(subsys.model, "Linux");
    }

    #[test]
    fn device_uuid_mapped_nguid_ignored() {
        let config = parse(
            r#"{ "ports": [ { "addr": { "traddr": "127.0.0.1", "trsvcid": "4420",
                              "trtype": "tcp" }, "subsystems": [ "nqn.a" ] } ],
                 "subsystems": [ { "nqn": "nqn.a", "attr": { "allow_any_host": "1" },
                   "namespaces": [ { "nsid": 1,
                     "device": { "path": "/dev/sda",
                                 "uuid": "00112233-4455-6677-8899-aabbccddeeff",
                                 "nguid": "5b1e6a44-97f2-40e9-b3d1-0c88a1c0d201" } } ] } ] }"#,
            TransportType::Tcp,
        )
        .unwrap();
        assert_eq!(
            config.subsystems[0].namespaces[0].uuid.as_deref(),
            Some("00112233-4455-6677-8899-aabbccddeeff")
        );
    }

    #[test]
    fn disabled_namespace_skipped() {
        let config = parse(
            r#"{ "ports": [ { "addr": { "traddr": "127.0.0.1", "trsvcid": "4420",
                              "trtype": "tcp" }, "subsystems": [ "nqn.a" ] } ],
                 "subsystems": [ { "nqn": "nqn.a", "attr": { "allow_any_host": "1" },
                   "namespaces": [
                     { "nsid": 1, "device": { "path": "/dev/sda" }, "enable": 0 },
                     { "nsid": 2, "device": { "path": "/dev/sdb" }, "enable": 1 } ] } ] }"#,
            TransportType::Tcp,
        )
        .unwrap();
        let nsids: Vec<u32> = config.subsystems[0]
            .namespaces
            .iter()
            .map(|n| n.nsid)
            .collect();
        assert_eq!(nsids, [2]);
    }

    #[test]
    fn ipv6_traddr_bracketed() {
        let config = parse(
            r#"{ "ports": [ { "addr": { "adrfam": "ipv6", "traddr": "::1",
                              "trsvcid": "4420", "trtype": "tcp" },
                              "subsystems": [ "nqn.a" ] } ],
                 "subsystems": [ { "nqn": "nqn.a", "attr": { "allow_any_host": "1" },
                   "namespaces": [ { "nsid": 1, "device": { "path": "/dev/sda" } } ] } ] }"#,
            TransportType::Tcp,
        )
        .unwrap();
        assert_eq!(config.listen, "[::1]:4420");
    }

    #[test]
    fn bad_shapes_rejected() {
        // Two ports for the serving transport: ambiguous, refuse.
        assert!(
            parse(
                r#"{ "ports": [
                   { "addr": { "traddr": "127.0.0.1", "trsvcid": "4420", "trtype": "tcp" },
                     "subsystems": [ "nqn.a" ] },
                   { "addr": { "traddr": "127.0.0.1", "trsvcid": "4421", "trtype": "tcp" },
                     "subsystems": [ "nqn.a" ] } ],
                 "subsystems": [ { "nqn": "nqn.a", "namespaces": [] } ] }"#,
                TransportType::Tcp,
            )
            .unwrap_err()
            .contains("multiple tcp ports")
        );
        // Port symlink to a subsystem the file never defines.
        assert!(
            parse(
                r#"{ "ports": [ { "addr": { "traddr": "127.0.0.1", "trsvcid": "4420",
                              "trtype": "tcp" }, "subsystems": [ "nqn.ghost" ] } ],
                 "subsystems": [] }"#,
                TransportType::Tcp,
            )
            .unwrap_err()
            .contains("undefined subsystem")
        );
        // Enabled namespace without a backing device path.
        assert!(
            parse(
                r#"{ "ports": [ { "addr": { "traddr": "127.0.0.1", "trsvcid": "4420",
                              "trtype": "tcp" }, "subsystems": [ "nqn.a" ] } ],
                 "subsystems": [ { "nqn": "nqn.a",
                   "namespaces": [ { "nsid": 1 } ] } ] }"#,
                TransportType::Tcp,
            )
            .unwrap_err()
            .contains("no device path")
        );
    }
}
