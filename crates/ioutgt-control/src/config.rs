//! Target configuration schema (JSON file and control-API payloads).

#![allow(missing_docs)] // serde schema: field names are the documented wire format

use std::path::PathBuf;

use serde::{Deserialize, Serialize};

/// Top-level target configuration file.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FileConfig {
    /// Listen address, e.g. "0.0.0.0:4420".
    pub listen: String,
    /// IO queue threads (the admin thread is implicit).
    #[serde(default = "default_io_threads")]
    pub io_threads: usize,
    /// Allow header-digest negotiation.
    #[serde(default = "default_true")]
    pub header_digest: bool,
    /// Allow data-digest negotiation.
    #[serde(default = "default_true")]
    pub data_digest: bool,
    /// Topology-aware IO thread pinning (default on, like the CLI;
    /// set false to opt out).
    #[serde(default = "default_true")]
    pub pin_threads: bool,
    /// Zero-copy sends (SENDMSG_ZC) with notification-gated buffer
    /// reuse; off by default.
    #[serde(default)]
    pub send_zc: bool,
    /// Advertised IO MAXCMD ceiling (entries): max IO queue depth the
    /// host may use. Admin queue unaffected. Default 128.
    #[serde(default = "default_io_queue_size")]
    pub io_queue_size: u16,
    /// Per-IO-queue data-buffer pool size in MiB (slots lease on
    /// demand). Default 8 MiB.
    #[serde(default = "default_queue_buf_mb")]
    pub queue_buf_mb: usize,
    /// Per-CONNECTION receive-ring size in MiB for zero-copy receive; 0 = off
    /// (classic per-recv scratch). Each ring-enabled connection owns its ring,
    /// so memory scales as (connections × this). Default 0.
    #[serde(default = "default_recv_buf_mb")]
    pub recv_buf_mb: usize,
    /// Unix socket path for the runtime control API.
    #[serde(default)]
    pub control_socket: Option<PathBuf>,
    /// Tear the queue-thread pool down after this many seconds with zero
    /// active connections, respawning it on the next connect; `0` keeps
    /// the pool alive for the process lifetime once spawned. Default 30.
    #[serde(default = "default_idle_teardown_secs")]
    pub idle_teardown_secs: u64,
    /// At least one subsystem.
    pub subsystems: Vec<SubsystemConfig>,
}

fn default_io_threads() -> usize {
    2
}

fn default_io_queue_size() -> u16 {
    128
}

fn default_queue_buf_mb() -> usize {
    ioutgt_core::pool::DEFAULT_POOL_MB
}

fn default_recv_buf_mb() -> usize {
    0
}

fn default_idle_teardown_secs() -> u64 {
    30
}

fn default_true() -> bool {
    true
}

/// One NVM subsystem.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SubsystemConfig {
    pub nqn: String,
    #[serde(default = "default_serial")]
    pub serial: String,
    #[serde(default = "default_model")]
    pub model: String,
    #[serde(default = "default_true")]
    pub allow_any_host: bool,
    /// Hostnqns admitted when `allow_any_host` is off (nvmet-style ACL).
    #[serde(default)]
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
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NamespaceConfig {
    pub nsid: u32,
    pub backend: BackendConfig,
    /// Namespace UUID, hyphenated ("xxxxxxxx-xxxx-…"). Defaults to an
    /// identity derived from (subsystem NQN, nsid); set it to keep
    /// host-visible identity (`/dev/disk/by-id`) across targets, e.g.
    /// when restoring a kernel-nvmet save.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub uuid: Option<String>,
}

/// Parse a hyphenated UUID string (8-4-4-4-12 hex) into its 16 bytes.
pub fn parse_uuid(text: &str) -> Option<[u8; 16]> {
    if text.len() != 36 {
        return None;
    }
    let mut bytes = [0u8; 16];
    let mut idx = 0;
    let mut hi = None;
    for (pos, ch) in text.bytes().enumerate() {
        if matches!(pos, 8 | 13 | 18 | 23) {
            if ch != b'-' {
                return None;
            }
            continue;
        }
        let nibble = u8::try_from((ch as char).to_digit(16)?).ok()?;
        match hi.take() {
            None => hi = Some(nibble),
            Some(h) => {
                bytes[idx] = h << 4 | nibble;
                idx += 1;
            }
        }
    }
    Some(bytes)
}

/// Backend selection (also the ADD_NAMESPACE payload).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase", deny_unknown_fields)]
pub enum BackendConfig {
    /// RAM-backed.
    Memory { size_mb: u64 },
    /// Discard writes, zero reads.
    Null { size_mb: u64 },
    /// O_DIRECT file or block device.
    File { path: PathBuf },
}

impl FileConfig {
    /// Parse and validate a config file. `trtype` is the fabric the
    /// loading binary serves; an nvmetcli-format config uses it to pick
    /// the matching port.
    pub fn load(
        path: &std::path::Path,
        trtype: ioutgt_core::subsystem::TransportType,
    ) -> Result<FileConfig, String> {
        let text = std::fs::read_to_string(path).map_err(|e| format!("{}: {e}", path.display()))?;
        Self::parse(&text, trtype).map_err(|e| format!("{}: {e}", path.display()))
    }

    /// Parse and validate either accepted schema: ioutgt's native
    /// engine-shaped config (keyed by its required `listen` field) or
    /// nvmetcli's configfs-shaped save format (see [`crate::nvmet`]).
    pub(crate) fn parse(
        text: &str,
        trtype: ioutgt_core::subsystem::TransportType,
    ) -> Result<FileConfig, String> {
        let value: serde_json::Value = serde_json::from_str(text).map_err(|e| e.to_string())?;
        let config: FileConfig = if value.get("listen").is_some() {
            serde_json::from_value(value).map_err(|e| e.to_string())?
        } else {
            crate::nvmet::to_file_config(value, trtype)?
        };
        config.validate()?;
        Ok(config)
    }

    /// Engine defaults around the given endpoint and subsystems — the
    /// construction path for schemas that carry no engine tuning.
    pub(crate) fn engine_defaults(listen: String, subsystems: Vec<SubsystemConfig>) -> FileConfig {
        FileConfig {
            listen,
            io_threads: default_io_threads(),
            header_digest: true,
            data_digest: true,
            pin_threads: true,
            send_zc: false,
            io_queue_size: default_io_queue_size(),
            queue_buf_mb: default_queue_buf_mb(),
            recv_buf_mb: default_recv_buf_mb(),
            control_socket: None,
            idle_teardown_secs: default_idle_teardown_secs(),
            subsystems,
        }
    }

    /// Structural validation beyond what serde enforces.
    pub fn validate(&self) -> Result<(), String> {
        self.listen
            .parse::<std::net::SocketAddr>()
            .map_err(|e| format!("listen '{}': {e}", self.listen))?;
        if self.io_threads == 0 {
            return Err("io_threads must be >= 1".into());
        }
        // Mirror the CLI's clap range: the advertised MAXCMD must stay
        // within [2, CAP.MQES]. Without this the JSON path would bypass
        // the connect-time memory-amplification guard (a huge value lets a
        // host preallocate oversized IO queues; < 2 rejects every connect).
        if !(2..=ioutgt_core::MAX_QUEUE_ENTRIES).contains(&self.io_queue_size) {
            return Err(format!(
                "io_queue_size {} out of range (2..={})",
                self.io_queue_size,
                ioutgt_core::MAX_QUEUE_ENTRIES
            ));
        }
        // The pool must hold at least one max-size transfer (MDTS); cap it
        // so a typo can't reserve absurd amounts of RAM per IO queue.
        const MAX_POOL_MB: usize = 1024; // 1 GiB
        let min_pool_mb = (ioutgt_core::MDTS_BYTES as usize)
            .div_ceil(1024 * 1024)
            .max(1);
        if !(min_pool_mb..=MAX_POOL_MB).contains(&self.queue_buf_mb) {
            return Err(format!(
                "queue_buf_mb {} out of range ({min_pool_mb}..={MAX_POOL_MB})",
                self.queue_buf_mb,
            ));
        }
        // Zero-copy receive ring: 0 = off. Otherwise each of the 2 sub-
        // buffers (recv_buf_mb*MiB/2) must hold a max transfer (MDTS); 1 MiB
        // → 512 KiB/sub-buffer clears the 128 KiB MDTS. Cap at 256 MiB so a
        // typo can't reserve absurd per-thread RAM.
        const MAX_RECV_BUF_MB: usize = 256;
        if self.recv_buf_mb != 0 && !(1..=MAX_RECV_BUF_MB).contains(&self.recv_buf_mb) {
            return Err(format!(
                "recv_buf_mb {} out of range (0 = off, else 1..={MAX_RECV_BUF_MB})",
                self.recv_buf_mb,
            ));
        }
        if self.subsystems.is_empty() {
            return Err("at least one subsystem is required".into());
        }
        for subsys in &self.subsystems {
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
                if let Some(uuid) = &ns.uuid
                    && parse_uuid(uuid).is_none()
                {
                    return Err(format!(
                        "{}: nsid {}: uuid '{uuid}' is not a hyphenated UUID",
                        subsys.nqn, ns.nsid
                    ));
                }
                if let BackendConfig::Memory { size_mb } | BackendConfig::Null { size_mb } =
                    &ns.backend
                    && *size_mb == 0
                {
                    return Err(format!(
                        "{}: nsid {}: size_mb must be > 0",
                        subsys.nqn, ns.nsid
                    ));
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(json: &str) -> Result<(), String> {
        let config: FileConfig = serde_json::from_str(json).map_err(|e| e.to_string())?;
        config.validate()
    }

    #[test]
    fn minimal_config_parses() {
        parse(
            r#"{ "listen": "127.0.0.1:4420",
                 "subsystems": [ { "nqn": "nqn.2026-06.io.ioutgt:a",
                   "namespaces": [ { "nsid": 1, "backend": { "type": "memory", "size_mb": 64 } } ] } ] }"#,
        )
        .unwrap();
    }

    #[test]
    fn rejects_bad_configs() {
        // Duplicate nsid.
        assert!(
            parse(
                r#"{ "listen": "127.0.0.1:4420",
                 "subsystems": [ { "nqn": "nqn.x", "namespaces": [
                   { "nsid": 1, "backend": { "type": "memory", "size_mb": 1 } },
                   { "nsid": 1, "backend": { "type": "null", "size_mb": 1 } } ] } ] }"#
            )
            .unwrap_err()
            .contains("duplicate nsid")
        );
        // Bad listen address.
        assert!(parse(r#"{ "listen": "nope", "subsystems": [] }"#).is_err());
        // Unknown field caught by serde.
        assert!(parse(r#"{ "listen": "1.2.3.4:1", "bogus": 1, "subsystems": [] }"#).is_err());
        // io_queue_size above CAP.MQES must be rejected, not silently let
        // through to advertise MAXCMD > MQES / oversize IO queues.
        assert!(
            parse(
                r#"{ "listen": "127.0.0.1:4420", "io_queue_size": 1000,
                 "subsystems": [ { "nqn": "nqn.x", "namespaces": [
                   { "nsid": 1, "backend": { "type": "memory", "size_mb": 1 } } ] } ] }"#
            )
            .unwrap_err()
            .contains("io_queue_size")
        );
        // nsid 0 reserved.
        assert!(
            parse(
                r#"{ "listen": "127.0.0.1:4420",
                 "subsystems": [ { "nqn": "nqn.x", "namespaces": [
                   { "nsid": 0, "backend": { "type": "memory", "size_mb": 1 } } ] } ] }"#
            )
            .unwrap_err()
            .contains("reserved")
        );
        // queue_buf_mb below one MDTS is rejected (can't hold a max IO).
        assert!(
            parse(
                r#"{ "listen": "127.0.0.1:4420", "queue_buf_mb": 0,
                 "subsystems": [ { "nqn": "nqn.x", "namespaces": [
                   { "nsid": 1, "backend": { "type": "memory", "size_mb": 1 } } ] } ] }"#
            )
            .unwrap_err()
            .contains("queue_buf_mb")
        );
        // recv_buf_mb out of range (above the 256 MiB cap) is rejected; 0
        // (off) and small values are fine.
        assert!(
            parse(
                r#"{ "listen": "127.0.0.1:4420", "recv_buf_mb": 9999,
                 "subsystems": [ { "nqn": "nqn.x", "namespaces": [
                   { "nsid": 1, "backend": { "type": "memory", "size_mb": 1 } } ] } ] }"#
            )
            .unwrap_err()
            .contains("recv_buf_mb")
        );
    }

    #[test]
    fn parse_uuid_accepts_hyphenated_and_rejects_malformed() {
        assert_eq!(
            parse_uuid("00112233-4455-6677-8899-aabbccddeeff"),
            Some([
                0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd,
                0xee, 0xff
            ])
        );
        for bad in [
            "",
            "00112233-4455-6677-8899-aabbccddeef",   // short
            "001122334455-6677-8899-aabbccddeeffaa", // hyphen misplaced
            "0011223g-4455-6677-8899-aabbccddeeff",  // non-hex
        ] {
            assert_eq!(parse_uuid(bad), None, "{bad:?}");
        }
    }

    #[test]
    fn malformed_namespace_uuid_rejected() {
        assert!(
            parse(
                r#"{ "listen": "127.0.0.1:4420",
                 "subsystems": [ { "nqn": "nqn.x", "namespaces": [
                   { "nsid": 1, "backend": { "type": "memory", "size_mb": 1 },
                     "uuid": "not-a-uuid" } ] } ] }"#
            )
            .unwrap_err()
            .contains("uuid")
        );
    }

    #[test]
    fn recv_buf_mb_off_and_in_range_ok() {
        // 0 = off (default) and a small in-range value both validate.
        for mb in [0, 1, 256] {
            parse(&format!(
                r#"{{ "listen": "127.0.0.1:4420", "recv_buf_mb": {mb},
                 "subsystems": [ {{ "nqn": "nqn.x", "namespaces": [
                   {{ "nsid": 1, "backend": {{ "type": "memory", "size_mb": 1 }} }} ] }} ] }}"#
            ))
            .unwrap();
        }
    }
}
