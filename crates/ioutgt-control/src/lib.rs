//! Control plane.
//!
//! Newline-delimited JSON over a Unix domain socket (ADD_NAMESPACE,
//! REMOVE_NAMESPACE, LIST_NAMESPACE, GET_STATS) plus the JSON configuration
//! schema and validation used to create a controller entirely from a config
//! file. Runs on the control thread; talks to queue threads only through
//! their mailboxes.
