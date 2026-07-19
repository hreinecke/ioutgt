//! The JSON configuration schema: kernel nvmet's, as written by
//! `nvmetcli save` and restored by `nvmetcli restore`.
//!
//! That format serializes the nvmet configfs tree — top-level
//! `hosts`/`ports`/`subsystems` arrays whose attribute groups (`attr`,
//! `addr`, `device`, …) hold string-typed configfs attributes — so an
//! existing `/etc/nvmet/config.json` drives ioutgt unchanged. The file
//! supplies only the target model (listen address, subsystems, host
//! ACLs, namespaces); engine tuning stays with the CLI flags, the way
//! configfs and module parameters split in the kernel.
//!
//! Attributes with no ioutgt counterpart (`param`, `ana_groups`,
//! `referrals`, PI/cntlid tuning, …) are accepted and ignored, like
//! nvmetcli's own error-skipping restore.

use std::collections::BTreeMap;
use std::net::SocketAddr;

use ioutgt_core::subsystem::TransportType;
use serde::Deserialize;

use crate::config::{BackendConfig, NamespaceConfig, SubsystemConfig};

/// The target model one port yields: its listen address and exported
/// subsystems. A config may define several ports for a fabric; each is
/// served by its own process (one process = one port).
#[derive(Debug)]
pub struct NvmetTarget {
    /// The port's configfs id (unique per config; names derived
    /// per-port resources such as the control socket).
    pub portid: u16,
    /// `addr.traddr:addr.trsvcid` of the port.
    pub listen: SocketAddr,
    /// The subsystems the port exports. A subsystem exported on
    /// several ports is instantiated independently by each port's
    /// process.
    pub subsystems: Vec<SubsystemConfig>,
}

/// Load and validate a config file: one [`NvmetTarget`] per port
/// serving `trtype`, sorted by portid. Errors if the file defines none.
pub fn load(path: &std::path::Path, trtype: TransportType) -> Result<Vec<NvmetTarget>, String> {
    let text = std::fs::read_to_string(path).map_err(|e| format!("{}: {e}", path.display()))?;
    from_str(&text, trtype).map_err(|e| format!("{}: {e}", path.display()))
}

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
    portid: u16,
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

fn from_str(text: &str, trtype: TransportType) -> Result<Vec<NvmetTarget>, String> {
    let config: NvmetConfig = serde_json::from_str(text).map_err(|e| e.to_string())?;
    let want = match trtype {
        TransportType::Tcp => "tcp",
        TransportType::Rdma => "rdma",
    };
    let mut ports: Vec<&Port> = config
        .ports
        .iter()
        .filter(|p| p.addr.trtype == want)
        .collect();
    if ports.is_empty() {
        return Err(format!("nvmet config has no {want} port"));
    }
    // Deterministic serving order, and unique ids for the per-port
    // resources (control socket) derived from portid.
    ports.sort_by_key(|p| p.portid);
    if ports.windows(2).any(|w| w[0].portid == w[1].portid) {
        return Err(format!("duplicate {want} portid in nvmet config"));
    }
    let mut targets = Vec::new();
    for port in ports {
        // SocketAddr's Display owns the IPv6 bracketing.
        let ip: std::net::IpAddr = port
            .addr
            .traddr
            .parse()
            .map_err(|_| format!("port traddr '{}' is not an IP address", port.addr.traddr))?;
        let svc: u16 =
            port.addr.trsvcid.parse().map_err(|_| {
                format!("port trsvcid '{}' is not a port number", port.addr.trsvcid)
            })?;
        // Only subsystems exported on the port are reachable (in
        // configfs, the port holds symlinks to them).
        let mut subsystems = Vec::new();
        for nqn in &port.subsystems {
            let Some(subsys) = config.subsystems.iter().find(|s| &s.nqn == nqn) else {
                return Err(format!("port exports undefined subsystem '{nqn}'"));
            };
            subsystems.push(subsys.to_config()?);
        }
        crate::config::validate_subsystems(&subsystems)?;
        targets.push(NvmetTarget {
            portid: port.portid,
            listen: SocketAddr::new(ip, svc),
            subsystems,
        });
    }
    Ok(targets)
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
            // nguid has no ioutgt counterpart and is not conflated.
            let uuid = match ns.device.get("uuid") {
                Some(text) => Some(ioutgt_core::subsystem::parse_uuid(text).ok_or_else(|| {
                    format!(
                        "{}: nsid {}: device uuid '{text}' is not a hyphenated UUID",
                        self.nqn, ns.nsid
                    )
                })?),
                None => None,
            };
            namespaces.push(NamespaceConfig {
                nsid: ns.nsid,
                backend: BackendConfig::File { path: path.into() },
                uuid,
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

    /// Parse a document expected to define exactly one `trtype` port.
    fn parse(json: &str, trtype: TransportType) -> Result<NvmetTarget, String> {
        from_str(json, trtype).map(|mut targets| {
            assert_eq!(targets.len(), 1, "single-port doc");
            targets.remove(0)
        })
    }

    /// A one-tcp-port document exporting "nqn.a"; each test supplies
    /// only its distinguishing subsystems array.
    fn tcp_doc(subsystems: &str) -> String {
        format!(
            r#"{{ "ports": [ {{ "addr": {{ "traddr": "127.0.0.1", "trsvcid": "4420",
                              "trtype": "tcp" }}, "subsystems": [ "nqn.a" ] }} ],
                 "subsystems": {subsystems} }}"#
        )
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
        assert_eq!(config.listen, "10.0.0.7:4420".parse().unwrap());
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
            &tcp_doc(
                r#"[ { "nqn": "nqn.a",
                   "attr": { "allow_any_host": "0", "serial": "SN123", "model": "Linux" },
                   "allowed_hosts": [ "hostnqn" ],
                   "namespaces": [ { "nsid": 1, "device": { "path": "/dev/sda" } } ] } ]"#,
            ),
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
            &tcp_doc(
                r#"[ { "nqn": "nqn.a", "attr": { "allow_any_host": "1" },
                   "namespaces": [ { "nsid": 1,
                     "device": { "path": "/dev/sda",
                                 "uuid": "00112233-4455-6677-8899-aabbccddeeff",
                                 "nguid": "5b1e6a44-97f2-40e9-b3d1-0c88a1c0d201" } } ] } ]"#,
            ),
            TransportType::Tcp,
        )
        .unwrap();
        assert_eq!(
            config.subsystems[0].namespaces[0].uuid,
            ioutgt_core::subsystem::parse_uuid("00112233-4455-6677-8899-aabbccddeeff"),
        );
    }

    #[test]
    fn disabled_namespace_skipped() {
        let config = parse(
            &tcp_doc(
                r#"[ { "nqn": "nqn.a", "attr": { "allow_any_host": "1" },
                   "namespaces": [
                     { "nsid": 1, "device": { "path": "/dev/sda" }, "enable": 0 },
                     { "nsid": 2, "device": { "path": "/dev/sdb" }, "enable": 1 } ] } ]"#,
            ),
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
        assert_eq!(config.listen, "[::1]:4420".parse().unwrap());
    }

    #[test]
    fn multiple_ports_yield_one_target_each() {
        // Two tcp ports with disjoint exports and one shared subsystem:
        // each becomes its own target (sorted by portid) carrying only
        // its port's exports; the shared subsystem appears in both.
        let targets = from_str(
            r#"{ "ports": [
                   { "addr": { "traddr": "127.0.0.1", "trsvcid": "4421", "trtype": "tcp" },
                     "portid": 2, "subsystems": [ "nqn.b", "nqn.shared" ] },
                   { "addr": { "traddr": "127.0.0.1", "trsvcid": "4420", "trtype": "tcp" },
                     "portid": 1, "subsystems": [ "nqn.a", "nqn.shared" ] } ],
                 "subsystems": [
                   { "nqn": "nqn.a",
                     "namespaces": [ { "nsid": 1, "device": { "path": "/dev/sda" } } ] },
                   { "nqn": "nqn.b",
                     "namespaces": [ { "nsid": 1, "device": { "path": "/dev/sdb" } } ] },
                   { "nqn": "nqn.shared",
                     "namespaces": [ { "nsid": 1, "device": { "path": "/dev/sdc" } } ] } ] }"#,
            TransportType::Tcp,
        )
        .unwrap();
        assert_eq!(targets.len(), 2);
        assert_eq!(targets[0].portid, 1);
        assert_eq!(targets[0].listen, "127.0.0.1:4420".parse().unwrap());
        let nqns = |t: &NvmetTarget| -> Vec<String> {
            t.subsystems.iter().map(|s| s.nqn.clone()).collect()
        };
        assert_eq!(nqns(&targets[0]), ["nqn.a", "nqn.shared"]);
        assert_eq!(targets[1].portid, 2);
        assert_eq!(targets[1].listen, "127.0.0.1:4421".parse().unwrap());
        assert_eq!(nqns(&targets[1]), ["nqn.b", "nqn.shared"]);
    }

    #[test]
    fn bad_shapes_rejected() {
        // Two ports for the serving transport with colliding portids:
        // per-port resource names (control socket) need distinct ids.
        assert!(
            from_str(
                r#"{ "ports": [
                   { "addr": { "traddr": "127.0.0.1", "trsvcid": "4420", "trtype": "tcp" },
                     "portid": 1, "subsystems": [ "nqn.a" ] },
                   { "addr": { "traddr": "127.0.0.1", "trsvcid": "4421", "trtype": "tcp" },
                     "portid": 1, "subsystems": [ "nqn.a" ] } ],
                 "subsystems": [ { "nqn": "nqn.a", "namespaces": [
                   { "nsid": 1, "device": { "path": "/dev/sda" } } ] } ] }"#,
                TransportType::Tcp,
            )
            .unwrap_err()
            .contains("duplicate tcp portid")
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
                &tcp_doc(r#"[ { "nqn": "nqn.a", "namespaces": [ { "nsid": 1 } ] } ]"#),
                TransportType::Tcp,
            )
            .unwrap_err()
            .contains("no device path")
        );
        // Malformed device uuid is a load error, not a silent fallback.
        assert!(
            parse(
                &tcp_doc(
                    r#"[ { "nqn": "nqn.a", "namespaces": [ { "nsid": 1,
                       "device": { "path": "/dev/sda", "uuid": "not-a-uuid" } } ] } ]"#,
                ),
                TransportType::Tcp,
            )
            .unwrap_err()
            .contains("hyphenated UUID")
        );
        // Structural validation runs on the loaded model.
        assert!(
            parse(
                &tcp_doc(
                    r#"[ { "nqn": "nqn.a", "namespaces": [
                       { "nsid": 1, "device": { "path": "/dev/sda" } },
                       { "nsid": 1, "device": { "path": "/dev/sdb" } } ] } ]"#,
                ),
                TransportType::Tcp,
            )
            .unwrap_err()
            .contains("duplicate nsid")
        );
    }
}
