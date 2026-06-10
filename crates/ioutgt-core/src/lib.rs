//! Transport-independent NVMe target core.
//!
//! Owns the subsystem/controller/namespace model, the per-queue command-slot
//! array with one persistent async task per tag, admin and IO command
//! dispatch, and the `Backend` and `Transport` traits that backends and
//! transports plug into. Mirrors the role of `core.c`/`nvmet.h` in the Linux
//! kernel nvmet target: no transport or backend specifics live here.
