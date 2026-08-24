//! The `--backend` spec grammar the transport binaries share.
//!
//! One flag string selects the whole target model of a flag-built port
//! ([`subsystems`]): `memory`, `null` and a file/bdev path each give one
//! subsystem — the `--subsys-nqn` one — with a single namespace, and so does
//! a `sheepdog:` spec naming one VDI. A `sheepdog:` spec naming no VDI maps
//! the **whole cluster**: one subsystem per ACL object, named by the ACL, and
//! one namespace per VDI in it, each under the cluster's shared VDI lock
//! unless the spec ends in `?nolock`, and each admitting only the hostnqns
//! the ACL lists as members — for as long as it lists them, since the target
//! keeps re-reading that list. The config-file path
//! ([`crate::nvmet`]) is unaffected: it always spells the model out.

use std::net::SocketAddr;

use ioutgt_backend::{AclInfo, list_acls};
use tracing::{info, warn};

use crate::config::{
    BackendConfig, NamespaceConfig, SheepdogAcl, SubsystemConfig, default_model, default_serial,
};

/// A parsed `sheepdog:` spec: one named VDI, or a whole cluster.
#[derive(Debug)]
enum Sheepdog {
    /// `sheepdog:HOST[:PORT]/VDI[@TAG][%ACL]` — a single namespace.
    Vdi(BackendConfig),
    /// `sheepdog:HOST[:PORT]` — every ACL on the cluster at this address and
    /// the VDIs in it, each locked (shared) unless `?nolock` said otherwise.
    Cluster { addr: String, lock: bool },
}

/// Resolve a `--backend` spec into the port's subsystem list.
///
/// `memory`/`null` (sized by `mem_size_mb`), `sheepdog:…` (see
/// [`parse_sheepdog`]) and otherwise a file or block-device path. Every form
/// but a cluster-wide `sheepdog:` yields exactly one subsystem, called `nqn`,
/// holding one namespace with nsid 1. A cluster spec contacts the cluster
/// here, on the way in — the same startup-time, off-the-IO-path handshake
/// `ioutgt_backend::SheepdogBackend::open` does — and ignores `nqn`, since
/// the cluster's own ACL objects name the subsystems it builds.
pub fn subsystems(spec: &str, mem_size_mb: u64, nqn: &str) -> Result<Vec<SubsystemConfig>, String> {
    let backend = match spec {
        "memory" => BackendConfig::Memory {
            size_mb: mem_size_mb,
        },
        "null" => BackendConfig::Null {
            size_mb: mem_size_mb,
        },
        spec if spec.starts_with("sheepdog:") => match parse_sheepdog(spec)? {
            Sheepdog::Vdi(backend) => backend,
            Sheepdog::Cluster { addr, lock } => return cluster_subsystems(&addr, lock),
        },
        path => BackendConfig::File { path: path.into() },
    };
    Ok(vec![SubsystemConfig {
        nqn: nqn.to_string(),
        serial: default_serial(),
        model: default_model(),
        allow_any_host: true,
        allowed_hosts: vec![],
        // Not a cluster ACL's subsystem, even for a single VDI addressed
        // through one: nothing re-reads its host list.
        sheepdog_acl: None,
        mnan: None,
        namespaces: vec![NamespaceConfig {
            nsid: 1,
            backend,
            uuid: None,
        }],
    }])
}

/// One subsystem per ACL object on the cluster at `addr`, holding one
/// namespace per writable VDI in that ACL.
///
/// An ACL object *is* the cluster's notion of "which volumes belong
/// together, reachable by whom", which is what a subsystem is on the NVMe
/// side — so the ACL's name becomes the subsystem NQN verbatim, its member
/// names become the hostnqns that subsystem admits, and hosts
/// find every one of them in the discovery log this port serves. Volumes
/// belonging to no ACL are exported by no subsystem: they are unreachable
/// through this target by construction, since the cluster will not even
/// resolve their names for a lookup carrying an ACL.
///
/// Each namespace keeps the VDI's own id as its nsid. A vid *is* the VDI's
/// position in the cluster's VDI bitmap, fixed for the life of the volume —
/// so the namespace map is a stable function of the cluster: creating or
/// deleting a VDI never renumbers the others, and two targets fronting the
/// same cluster hand a host the same nsid for the same volume. The resulting
/// nsids are sparse and large (a vid is a 24-bit hash of the VDI name), which
/// hosts discover through the Active Namespace List; `Identify Controller`'s
/// NN, the highest nsid, is correspondingly large.
///
/// Snapshots are skipped: they are frozen and would only ever be servable
/// read-only — name one explicitly (`sheepdog:HOST/VDI@TAG%ACL`) to export
/// it. A namespace's UUID is the VDI's own — the `uuid[16]` `sheep` put in
/// its inode when the volume was created — rather than anything derived from
/// the subsystem NQN, so the volume keeps one host-visible identity
/// (`/dev/disk/by-id`) no matter which subsystem or which target exports it.
///
/// `lock` (on unless the spec said `?nolock`) reaches every exported VDI:
/// each is opened under the cluster's VDI lock, shared with the other targets
/// serving the same ACL, so a volume some other client holds *exclusively* —
/// a running QEMU guest — fails the target's startup rather than being served
/// into a data race.
fn cluster_subsystems(addr: &str, lock: bool) -> Result<Vec<SubsystemConfig>, String> {
    let sockaddr = resolve(addr)?;
    let acls =
        list_acls(sockaddr).map_err(|e| format!("sheepdog {addr}: ACL enumeration failed: {e}"))?;
    if acls.is_empty() {
        return Err(format!(
            "sheepdog {addr}: the cluster has no ACL objects, so there is nothing \
             to name a subsystem after — create one with `dog acl create <nqn>` and \
             add VDIs to it with `dog acl add <nqn> <vdi>`, or name a single VDI \
             (sheepdog:HOST/VDI%ACL)"
        ));
    }

    let subsystems: Vec<SubsystemConfig> = acls
        .iter()
        .map(|acl| acl_subsystem(addr, sockaddr, acl, lock))
        .collect();
    let exported: usize = subsystems.iter().map(|s| s.namespaces.len()).sum();
    if exported == 0 {
        return Err(format!(
            "sheepdog {addr}: none of the {} ACL object(s) holds a writable VDI",
            acls.len()
        ));
    }
    info!(
        cluster = addr,
        subsystems = subsystems.len(),
        namespaces = exported,
        "sheepdog cluster mapped to subsystems"
    );
    Ok(subsystems)
}

/// One ACL object as a subsystem: its name as the NQN, its writable VDIs as
/// namespaces, its member names as the hostnqns allowed to connect.
///
/// The cluster's ACL is the access-control scope on both sides, so the host
/// list is the cluster's too: the names `dog acl add member <acl> <hostnqn>`
/// wrote into the ACL inode become the subsystem's `allowed_hosts`, and
/// nothing else may connect (Connect answers CONNECT_INVALID_HOST).
///
/// An ACL with no members is an ACL that expresses no opinion about hosts,
/// not one that refuses them all: its subsystem takes `allow_any_host`, as a
/// group unconstrained on the cluster side is unconstrained here. Access
/// control starts with the first `dog acl add member`, which is also when the
/// subsystem stops admitting everyone.
///
/// This is only the first reading of that list: the ACL object is recorded in
/// [`SubsystemConfig::sheepdog_acl`], and the target's refresh thread re-reads
/// it every few seconds, so an administrator's `dog acl add member` /
/// `remove member` reaches a running target without a restart.
fn acl_subsystem(addr: &str, cluster: SocketAddr, acl: &AclInfo, lock: bool) -> SubsystemConfig {
    if !acl.name.starts_with("nqn.") {
        warn!(
            acl = %acl.name,
            "sheepdog: ACL name is not an NVMe qualified name; hosts will refuse \
             to connect to this subsystem"
        );
    }
    let namespaces: Vec<NamespaceConfig> = acl
        .vdis
        .iter()
        .filter(|vdi| !vdi.snapshot)
        .map(|vdi| {
            // list_acls never reports vid 0 (the "no VDI" sentinel) and a vid
            // is 24-bit, so it can be neither reserved nsid (0 / u32::MAX).
            let nsid = vdi.vid;
            info!(nsid, vdi = %vdi.name, acl = %acl.name, bytes = vdi.size,
                  "sheepdog VDI exported");
            NamespaceConfig {
                nsid,
                backend: BackendConfig::Sheepdog {
                    addr: addr.to_string(),
                    vdi: vdi.name.clone(),
                    tag: None,
                    acl: Some(acl.name.clone()),
                    lock,
                },
                // The cluster's own UUID for the volume, and — for an inode
                // written by a sheep predating the field — one derived from
                // the VDI's name and vid, which are equally cluster-wide.
                uuid: Some(vdi.uuid.unwrap_or_else(|| {
                    ioutgt_core::subsystem::namespace_uuid(
                        &format!("sheepdog:{}", vdi.name),
                        vdi.vid,
                    )
                })),
            }
        })
        .collect();
    let snapshots = acl.vdis.len() - namespaces.len();
    if namespaces.is_empty() {
        warn!(acl = %acl.name, snapshots,
              "sheepdog: ACL holds no writable VDI; its subsystem is empty");
    }
    if acl.hosts.is_empty() {
        info!(
            acl = %acl.name,
            "sheepdog: ACL has no members; its subsystem admits any host until one \
             is added with `dog acl add member <acl> <hostnqn>`"
        );
    } else {
        info!(acl = %acl.name, hosts = ?acl.hosts, "sheepdog ACL members admitted");
    }
    let hosts = acl.host_acl();
    SubsystemConfig {
        nqn: acl.name.clone(),
        // The ACL's vid is its cluster-wide identity and outlives any target,
        // so every target fronting this cluster reports one serial per volume
        // group — as two paths to one subsystem must.
        serial: format!("SHEEPDOG{:06X}", acl.vid),
        model: default_model(),
        // The cluster's ACL *is* the host ACL: once it names members, only
        // they may connect. An empty member list names nobody to keep out
        // either, so it leaves the subsystem open rather than sealed.
        allow_any_host: hosts.allow_any_host,
        allowed_hosts: hosts.hosts,
        // ...and the ACL object it was read from, for the refresh to re-read.
        sheepdog_acl: Some(SheepdogAcl {
            cluster,
            vid: acl.vid,
            epoch: acl.epoch,
            lock,
        }),
        // The cluster's own count of the volumes in this ACL
        // (`max_data_id_nr`), rather than one derived from the namespaces
        // this target managed to build out of them. NN cannot carry it: with
        // a vid for an NSID, NN is the highest vid, not a count.
        mnan: Some(acl.max_data_id_nr),
        namespaces,
    }
}

/// Parse `sheepdog:HOST[:PORT][/VDI[@TAG][%ACL]][?nolock]`. The port defaults
/// to the `sheep` client port (7000); `@TAG` selects a (read-only) snapshot;
/// `%ACL` names the ACL object the VDI belongs to (required for a VDI that is
/// in one — the cluster will not resolve its name otherwise); an omitted or
/// empty `/VDI` means the whole cluster; `?nolock` gives up the VDI lock the
/// backend otherwise takes. IPv6 hosts must be bracketed
/// (`sheepdog:[::1]:7000/vdi`).
fn parse_sheepdog(spec: &str) -> Result<Sheepdog, String> {
    let rest = spec.strip_prefix("sheepdog:").expect("checked by caller");
    let (rest, lock) = match rest.split_once('?') {
        Some((rest, "nolock")) => (rest, false),
        Some((_, opt)) => return Err(format!("unknown sheepdog option '?{opt}' (only '?nolock')")),
        None => (rest, true),
    };
    let (addr_part, vdi_part) = rest.split_once('/').unwrap_or((rest, ""));
    if addr_part.is_empty() {
        return Err("sheepdog backend needs a host: sheepdog:HOST[:PORT][/VDI[@TAG][%ACL]]".into());
    }
    let addr = with_default_port(addr_part)?;
    if vdi_part.is_empty() {
        return Ok(Sheepdog::Cluster { addr, lock });
    }
    // `%ACL` is stripped first: a tag may not contain '%', but an ACL name is
    // an NQN and freely contains '@' (`nqn.2014-08.org.nvmexpress:uuid:…`).
    let (vdi_part, acl) = match vdi_part.split_once('%') {
        Some((vdi_part, acl)) if !acl.is_empty() => (vdi_part, Some(acl.to_string())),
        Some((_, _)) => return Err("sheepdog backend needs a non-empty ACL name after '%'".into()),
        None => (vdi_part, None),
    };
    let (vdi, tag) = match vdi_part.split_once('@') {
        Some((vdi, tag)) if !tag.is_empty() => (vdi, Some(tag.to_string())),
        Some((vdi, _)) => (vdi, None),
        None => (vdi_part, None),
    };
    if vdi.is_empty() {
        return Err("sheepdog backend needs a non-empty VDI name before '@'".into());
    }
    Ok(Sheepdog::Vdi(BackendConfig::Sheepdog {
        addr,
        vdi: vdi.to_string(),
        tag,
        acl,
        lock,
    }))
}

/// Append the default `sheep` port to a host that carries none.
fn with_default_port(host: &str) -> Result<String, String> {
    let has_port = if let Some(rest) = host.strip_prefix('[') {
        // Bracketed IPv6: port present iff `]:` follows the literal.
        rest.contains("]:")
    } else {
        match host.matches(':').count() {
            0 => false, // bare host / IPv4, no port
            1 => true,  // host:port / IPv4:port
            _ => {
                return Err(
                    "IPv6 sheepdog host must be bracketed, e.g. sheepdog:[::1]:7000/vdi".into(),
                );
            }
        }
    };
    Ok(if has_port {
        host.to_string()
    } else {
        format!("{host}:{}", ioutgt_backend::SD_LISTEN_PORT)
    })
}

/// Resolve a `host:port` string to one socket address.
pub fn resolve(addr: &str) -> Result<SocketAddr, String> {
    std::net::ToSocketAddrs::to_socket_addrs(addr)
        .map_err(|e| format!("sheepdog addr '{addr}': {e}"))?
        .next()
        .ok_or_else(|| format!("sheepdog addr '{addr}' resolved to no address"))
}

#[cfg(test)]
mod tests {
    use super::*;

    const NQN: &str = "nqn.2026-06.io.ioutgt:flag";

    /// The single-subsystem forms, none of which touch the network: one
    /// namespace under the NQN the flag named.
    #[test]
    fn simple_specs_give_one_namespace() {
        let mem = subsystems("memory", 64, NQN).unwrap();
        assert_eq!(mem.len(), 1);
        assert_eq!(mem[0].nqn, NQN);
        assert_eq!(mem[0].namespaces.len(), 1);
        assert_eq!(mem[0].namespaces[0].nsid, 1);
        assert!(matches!(
            mem[0].namespaces[0].backend,
            BackendConfig::Memory { size_mb: 64 }
        ));
        assert!(matches!(
            subsystems("null", 8, NQN).unwrap()[0].namespaces[0].backend,
            BackendConfig::Null { size_mb: 8 }
        ));
        match &subsystems("/dev/nvme0n1", 64, NQN).unwrap()[0].namespaces[0].backend {
            BackendConfig::File { path } => assert_eq!(path.to_str(), Some("/dev/nvme0n1")),
            other => panic!("expected a file backend, got {other:?}"),
        }
    }

    fn vdi_spec(spec: &str) -> (String, String, Option<String>) {
        match parse_sheepdog(spec) {
            Ok(Sheepdog::Vdi(BackendConfig::Sheepdog { addr, vdi, tag, .. })) => (addr, vdi, tag),
            _ => panic!("{spec}: expected a single-VDI spec"),
        }
    }

    /// The ACL a single-VDI spec names, if any.
    fn vdi_acl(spec: &str) -> Option<String> {
        match parse_sheepdog(spec) {
            Ok(Sheepdog::Vdi(BackendConfig::Sheepdog { acl, .. })) => acl,
            _ => panic!("{spec}: expected a single-VDI spec"),
        }
    }

    fn cluster_spec(spec: &str) -> String {
        match parse_sheepdog(spec) {
            Ok(Sheepdog::Cluster { addr, .. }) => addr,
            _ => panic!("{spec}: expected a cluster spec"),
        }
    }

    /// Whether `spec` asks for the VDI lock, either spec shape.
    fn locks(spec: &str) -> bool {
        match parse_sheepdog(spec) {
            Ok(Sheepdog::Vdi(BackendConfig::Sheepdog { lock, .. })) => lock,
            Ok(Sheepdog::Cluster { lock, .. }) => lock,
            other => panic!("{spec}: unexpected parse {other:?}"),
        }
    }

    #[test]
    fn sheepdog_vdi_specs_parse() {
        assert_eq!(
            vdi_spec("sheepdog:sheep0/vol"),
            ("sheep0:7000".into(), "vol".into(), None)
        );
        assert_eq!(
            vdi_spec("sheepdog:10.0.0.1:7001/vol@daily"),
            ("10.0.0.1:7001".into(), "vol".into(), Some("daily".into()))
        );
        assert_eq!(
            vdi_spec("sheepdog:[::1]:7000/vol"),
            ("[::1]:7000".into(), "vol".into(), None)
        );
        assert_eq!(
            vdi_spec("sheepdog:[fe80::1]/vol"),
            ("[fe80::1]:7000".into(), "vol".into(), None)
        );
        // An empty tag is the head, not a snapshot named "".
        assert_eq!(vdi_spec("sheepdog:sheep0/vol@").2, None);
    }

    #[test]
    fn sheepdog_acl_scoping_parses() {
        assert_eq!(vdi_acl("sheepdog:sheep0/vol"), None);
        assert_eq!(
            vdi_acl("sheepdog:sheep0/vol%nqn.2026-06.io.ioutgt:grp"),
            Some("nqn.2026-06.io.ioutgt:grp".into())
        );
        // The ACL is stripped before the tag, and an NQN may hold '@'.
        let (addr, vdi, tag) = vdi_spec("sheepdog:sheep0/vol@daily%nqn.2026-06.io:a@b");
        assert_eq!(
            (addr, vdi, tag),
            ("sheep0:7000".into(), "vol".into(), Some("daily".into()))
        );
        assert_eq!(
            vdi_acl("sheepdog:sheep0/vol@daily%nqn.2026-06.io:a@b"),
            Some("nqn.2026-06.io:a@b".into())
        );
        // ACL scoping is orthogonal to `?nolock`.
        assert!(!locks("sheepdog:sheep0/vol%grp?nolock"));
        assert_eq!(
            vdi_acl("sheepdog:sheep0/vol%grp?nolock"),
            Some("grp".into())
        );
    }

    #[test]
    fn sheepdog_cluster_specs_parse() {
        assert_eq!(cluster_spec("sheepdog:sheep0"), "sheep0:7000");
        assert_eq!(cluster_spec("sheepdog:sheep0/"), "sheep0:7000");
        assert_eq!(cluster_spec("sheepdog:10.0.0.1:7001"), "10.0.0.1:7001");
        assert_eq!(cluster_spec("sheepdog:[::1]:7000/"), "[::1]:7000");
    }

    #[test]
    fn vdi_locking_is_on_unless_waived() {
        assert!(locks("sheepdog:sheep0/vol"));
        assert!(locks("sheepdog:sheep0"));
        assert!(!locks("sheepdog:sheep0/vol?nolock"));
        assert!(!locks("sheepdog:sheep0/vol@daily?nolock"));
        assert!(!locks("sheepdog:sheep0?nolock"));
        // `?nolock` is stripped before the address, not left in the host.
        assert_eq!(
            cluster_spec("sheepdog:10.0.0.1:7001?nolock"),
            "10.0.0.1:7001"
        );
        assert_eq!(vdi_spec("sheepdog:sheep0/vol?nolock").1, "vol");
    }

    #[test]
    fn malformed_sheepdog_specs_rejected() {
        for spec in ["sheepdog:", "sheepdog:/vol", "sheepdog:/"] {
            assert!(parse_sheepdog(spec).is_err(), "{spec} should be rejected");
        }
        assert!(
            parse_sheepdog("sheepdog:::1:7000/vol")
                .unwrap_err()
                .contains("bracketed")
        );
        assert!(
            parse_sheepdog("sheepdog:sheep0/@daily")
                .unwrap_err()
                .contains("VDI name")
        );
        assert!(
            parse_sheepdog("sheepdog:sheep0/vol?lock=0")
                .unwrap_err()
                .contains("unknown sheepdog option")
        );
        assert!(
            parse_sheepdog("sheepdog:sheep0/vol%")
                .unwrap_err()
                .contains("ACL name")
        );
    }
}
