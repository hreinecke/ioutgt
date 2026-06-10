//! Storage backends implementing `ioutgt_core::Backend`.
//!
//! `NullBackend` (discard writes, zero reads), `MemoryBackend` (RAM-backed,
//! for tests and protocol bring-up), `FileBackend` (O_DIRECT with buffered
//! fallback), and `BlockBackend` (raw block device). Disk IO is issued on
//! the owning queue thread's io_uring; backends have no protocol awareness.
