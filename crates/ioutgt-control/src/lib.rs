//! Control plane.
//!
//! [`nvmet`]: the config-file schema — kernel nvmet's, as written by
//! `nvmetcli save`. [`config`]: the target-model structures it loads
//! into, also the wire form for runtime namespace operations.
//! [`server`]: the
//! newline-delimited JSON API over a Unix domain socket
//! (ADD_NAMESPACE / REMOVE_NAMESPACE / LIST_NAMESPACE / GET_STATS),
//! running on the control thread; queue threads are reached only
//! through their mailboxes (namespace changes propagate via the
//! versioned table + an AER nudge).

pub mod config;
pub mod nvmet;
pub mod server;
