//! Newline-delimited JSON control API over a Unix domain socket.
//!
//! Runs on the control thread. Namespace mutations go through the
//! subsystem's versioned table (IO threads pick them up via their
//! generation caches) and nudge connected controllers' AERs through the
//! provided callback (a mailbox send to the admin thread).

#![allow(missing_docs)] // request/response fields mirror the JSON protocol

use std::sync::Arc;

use ioutgt_backend::{AnyBackend, FileBackend, MemoryBackend, NullBackend};
use ioutgt_core::Backend;
use ioutgt_core::controller::Registry;
use ioutgt_core::subsystem::{Namespace, PortConfig};
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tracing::{info, warn};

use crate::config::BackendConfig;

/// Control request (one JSON object per line).
#[derive(Debug, Deserialize)]
#[serde(tag = "op", deny_unknown_fields)]
pub enum Request {
    #[serde(rename = "ADD_NAMESPACE")]
    AddNamespace {
        /// Defaults to the only subsystem when just one is configured.
        #[serde(default)]
        subsysnqn: Option<String>,
        nsid: u32,
        backend: BackendConfig,
    },
    #[serde(rename = "REMOVE_NAMESPACE")]
    RemoveNamespace {
        #[serde(default)]
        subsysnqn: Option<String>,
        nsid: u32,
    },
    #[serde(rename = "LIST_NAMESPACE")]
    ListNamespace {
        #[serde(default)]
        subsysnqn: Option<String>,
    },
    #[serde(rename = "GET_STATS")]
    GetStats,
    #[serde(rename = "LIST_CONTROLLER")]
    ListController,
}

/// Control response (one JSON object per line).
#[derive(Debug, Serialize)]
pub struct Response {
    pub ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<serde_json::Value>,
}

impl Response {
    fn ok(data: Option<serde_json::Value>) -> Response {
        Response {
            ok: true,
            error: None,
            data,
        }
    }

    fn err(message: impl Into<String>) -> Response {
        Response {
            ok: false,
            error: Some(message.into()),
            data: None,
        }
    }
}

/// Shared state the API operates on.
pub struct CtlState {
    pub port: Arc<PortConfig<AnyBackend>>,
    pub registry: Arc<Registry>,
    /// Invoked after any namespace change: routes an AER nudge to the
    /// admin queue thread.
    pub notify_ns_changed: Box<dyn Fn() + Send + Sync>,
}

/// Serve the control API until the listener fails. Spawn on the control
/// thread's LocalSet.
pub async fn serve(listener: tokio::net::UnixListener, state: Arc<CtlState>) {
    loop {
        let (stream, _) = match listener.accept().await {
            Ok(pair) => pair,
            Err(err) => {
                warn!("control accept failed: {err}");
                return;
            }
        };
        let state = Arc::clone(&state);
        tokio::task::spawn_local(async move {
            let (read, mut write) = stream.into_split();
            let mut lines = BufReader::new(read).lines();
            while let Ok(Some(line)) = lines.next_line().await {
                if line.trim().is_empty() {
                    continue;
                }
                let response = match serde_json::from_str::<Request>(&line) {
                    Ok(request) => handle(&state, request),
                    Err(err) => Response::err(format!("bad request: {err}")),
                };
                let mut out = serde_json::to_string(&response).expect("serializable");
                out.push('\n');
                if write.write_all(out.as_bytes()).await.is_err() {
                    return;
                }
            }
        });
    }
}

fn resolve<'a>(
    state: &'a CtlState,
    subsysnqn: Option<&str>,
) -> Result<&'a Arc<ioutgt_core::subsystem::Subsystem<AnyBackend>>, Response> {
    match subsysnqn {
        Some(nqn) => state
            .port
            .subsystem(nqn)
            .ok_or_else(|| Response::err(format!("unknown subsystem '{nqn}'"))),
        None if state.port.subsystems.len() == 1 => {
            Ok(state.port.subsystems.values().next().expect("len 1"))
        }
        None => Err(Response::err("subsysnqn required (multiple subsystems)")),
    }
}

/// Build a backend from its config (block shift 9, as the static path).
pub fn build_backend(config: &BackendConfig) -> Result<AnyBackend, String> {
    const BLOCK_SHIFT: u8 = 9;
    Ok(match config {
        BackendConfig::Memory { size_mb } => {
            AnyBackend::Memory(MemoryBackend::new(size_mb << 20, BLOCK_SHIFT))
        }
        BackendConfig::Null { size_mb } => {
            AnyBackend::Null(NullBackend::new(size_mb << 20, BLOCK_SHIFT))
        }
        BackendConfig::File { path } => {
            let file = FileBackend::open(path, BLOCK_SHIFT).map_err(|e| e.to_string())?;
            if !file.is_direct() {
                warn!(?path, "O_DIRECT unavailable; using buffered IO");
            }
            AnyBackend::File(file)
        }
    })
}

/// Namespace JSON body shared by `LIST_NAMESPACE` and `LIST_CONTROLLER`.
fn ns_json(ns: &Namespace<AnyBackend>) -> serde_json::Value {
    json!({
        "nsid": ns.nsid,
        "blocks": ns.backend.nr_blocks(),
        "block_shift": ns.backend.block_shift(),
    })
}

fn handle(state: &CtlState, request: Request) -> Response {
    match request {
        Request::AddNamespace {
            subsysnqn,
            nsid,
            backend,
        } => {
            let subsys = match resolve(state, subsysnqn.as_deref()) {
                Ok(subsys) => subsys,
                Err(response) => return response,
            };
            if nsid == 0 || nsid == u32::MAX {
                return Response::err("nsid reserved");
            }
            let backend = match build_backend(&backend) {
                Ok(backend) => backend,
                Err(err) => return Response::err(err),
            };
            let mut uuid = [0u8; 16];
            uuid[..4].copy_from_slice(&nsid.to_be_bytes());
            uuid[8] = 0x80;
            let ns = Namespace {
                nsid,
                backend: Arc::new(backend),
                uuid,
            };
            if let Err(err) = subsys.add_namespace(ns) {
                return Response::err(err);
            }
            info!(nsid, subsys = %subsys.nqn, "namespace added");
            (state.notify_ns_changed)();
            Response::ok(None)
        }
        Request::RemoveNamespace { subsysnqn, nsid } => {
            let subsys = match resolve(state, subsysnqn.as_deref()) {
                Ok(subsys) => subsys,
                Err(response) => return response,
            };
            if let Err(err) = subsys.remove_namespace(nsid) {
                return Response::err(err);
            }
            info!(nsid, subsys = %subsys.nqn, "namespace removed");
            (state.notify_ns_changed)();
            Response::ok(None)
        }
        Request::ListNamespace { subsysnqn } => {
            let subsys = match resolve(state, subsysnqn.as_deref()) {
                Ok(subsys) => subsys,
                Err(response) => return response,
            };
            let table = subsys.snapshot();
            let list: Vec<_> = table.values().map(|ns| ns_json(ns)).collect();
            Response::ok(Some(json!({ "namespaces": list })))
        }
        Request::GetStats => {
            let subsystems: Vec<_> = state
                .port
                .subsystems
                .values()
                .map(|subsys| {
                    json!({
                        "nqn": subsys.nqn,
                        "namespaces": subsys.snapshot().len(),
                    })
                })
                .collect();
            // Per-queue IO counters land with the performance milestone.
            Response::ok(Some(json!({
                "controllers": state.registry.len(),
                "subsystems": subsystems,
            })))
        }
        Request::ListController => {
            let controllers: Vec<_> = state
                .registry
                .snapshot()
                .into_iter()
                .map(|entry| {
                    let namespaces: Vec<_> = state
                        .port
                        .subsystem(&entry.subsys_nqn)
                        .map(|subsys| subsys.snapshot().values().map(|ns| ns_json(ns)).collect())
                        .unwrap_or_default();
                    let queues: Vec<_> = entry
                        .queues
                        .iter()
                        .map(|q| json!({ "qid": q.qid, "depth": q.sqsize, "tid": q.tid }))
                        .collect();
                    json!({
                        "cntlid": entry.cntlid,
                        "subsysnqn": entry.subsys_nqn,
                        "hostnqn": entry.hostnqn,
                        "discovery": entry.is_discovery(),
                        "kato_ms": entry.kato_ms,
                        "queues": queues,
                        "namespaces": namespaces,
                    })
                })
                .collect();
            // Discoverable inventory: what the discovery log would
            // advertise, reported even with no controller connected.
            let port_subsystems: Vec<_> = state
                .port
                .subsystems
                .values()
                .map(|subsys| {
                    let namespaces: Vec<_> =
                        subsys.snapshot().values().map(|ns| ns_json(ns)).collect();
                    json!({ "nqn": subsys.nqn, "namespaces": namespaces })
                })
                .collect();
            // An array from day one: multi-port is on the roadmap and
            // the wire shape shouldn't need a breaking rename then.
            Response::ok(Some(json!({
                "pid": std::process::id(),
                "ports": [{
                    "traddr": state.port.traddr,
                    "trsvcid": state.port.trsvcid,
                    "subsystems": port_subsystems,
                }],
                "controllers": controllers,
            })))
        }
    }
}
