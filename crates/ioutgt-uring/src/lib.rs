//! Per-thread io_uring reactor and operation futures.
//!
//! One ring per queue thread, created with `SINGLE_ISSUER | DEFER_TASKRUN`.
//! Operations are futures whose state lives in a per-thread slab; the slab
//! key is the io_uring `user_data`. The reactor integrates with a Tokio
//! current-thread runtime via the `on_thread_park` hook: `io_uring_enter`
//! is the park primitive, so submission is batched and the steady-state
//! syscall count approaches zero under load.
//!
//! See `docs/architecture.md` ("Reactor") for the full design.
