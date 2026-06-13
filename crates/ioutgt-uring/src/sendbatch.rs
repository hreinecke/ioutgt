//! Gather-send staging: a header arena plus an iovec list referencing
//! payload bytes in place, shipped as one vectored send and resumed
//! in place after short sends. Pure byte/pointer plumbing — no
//! protocol knowledge; callers encode their own headers into the
//! arena. Any stream transport reuses this (NVMe/TCP today, NBD
//! next).

/// Kernel cap on `msg_iovlen`.
pub const UIO_MAXIOV: usize = libc::UIO_MAXIOV as usize;

/// One batch's gather state. All memory is preallocated at
/// construction; `reset()` recycles everything.
pub struct GatherBatch {
    arena: Box<[u8]>,
    arena_used: usize,
    iovs: Vec<libc::iovec>,
    /// Hard entry cap (≤ UIO_MAXIOV). `Vec::with_capacity` may
    /// over-allocate, so the fit check must not use `capacity()`.
    iov_cap: usize,
    /// First entry not yet fully sent (short-send resume point).
    live: usize,
    msghdr: Box<libc::msghdr>,
}

impl GatherBatch {
    /// Preallocate `arena_bytes` (≥ 4 KiB) of header arena and up to
    /// `iov_cap` iovec entries (clamped to [`UIO_MAXIOV`]).
    pub fn new(arena_bytes: usize, iov_cap: usize) -> GatherBatch {
        let iov_cap = iov_cap.min(UIO_MAXIOV);
        GatherBatch {
            arena: vec![0u8; arena_bytes.max(4096)].into_boxed_slice(),
            arena_used: 0,
            iovs: Vec::with_capacity(iov_cap),
            iov_cap,
            live: 0,
            // SAFETY: a zeroed msghdr is a valid value; msg_iov[len]
            // are set in msghdr() before every submit.
            msghdr: Box::new(unsafe { std::mem::zeroed() }),
        }
    }

    /// Recycle for the next staging round.
    pub fn reset(&mut self) {
        self.arena_used = 0;
        self.iovs.clear();
        self.live = 0;
    }

    /// Headroom for one more item of `arena_need` header bytes and
    /// `iovs_need` (unmerged) iovec entries?
    #[inline]
    pub fn fits(&self, arena_need: usize, iovs_need: usize) -> bool {
        self.arena_used + arena_need <= self.arena.len()
            && self.iovs.len() + iovs_need <= self.iov_cap
    }

    /// Unused arena to encode the next header piece into.
    #[inline]
    pub fn arena_tail(&mut self) -> &mut [u8] {
        &mut self.arena[self.arena_used..]
    }

    /// Publish `len` bytes just written at the arena tail.
    #[inline]
    pub fn push_arena(&mut self, len: usize) {
        let start = self.arena_used;
        self.arena_used += len;
        let ptr = self.arena[start..].as_ptr();
        self.push_raw(ptr, len);
    }

    /// Append a wire chunk; merges with the previous entry when
    /// byte-contiguous (consecutive arena pieces collapse, so pure
    /// header batches degenerate to a single entry).
    #[inline]
    pub fn push_raw(&mut self, ptr: *const u8, len: usize) {
        if len == 0 {
            return;
        }
        if let Some(last) = self.iovs.last_mut() {
            // SAFETY: one-past-the-end pointer, used only for
            // equality.
            let end = unsafe { last.iov_base.cast::<u8>().add(last.iov_len) };
            if std::ptr::eq(end, ptr) {
                last.iov_len += len;
                return;
            }
        }
        self.iovs.push(libc::iovec {
            iov_base: ptr.cast_mut().cast(),
            iov_len: len,
        });
    }

    /// msghdr describing the unsent suffix; call before each submit.
    #[inline]
    pub fn msghdr(&mut self) -> *const libc::msghdr {
        self.msghdr.msg_iov = self.iovs[self.live..].as_mut_ptr();
        self.msghdr.msg_iovlen = self.iovs.len() - self.live;
        &raw const *self.msghdr
    }

    /// Consume `sent` bytes; true when the whole batch hit the
    /// socket.
    #[inline]
    pub fn advance(&mut self, sent: usize) -> bool {
        advance_iovecs(&mut self.iovs, &mut self.live, sent)
    }

    /// The staged iovec list, in gather order — the exact bytes the
    /// kernel would put on the wire. Read-only; for linearization and
    /// merge assertions in a transport's send-path tests (the fields
    /// are otherwise private to this crate).
    pub fn iovs(&self) -> &[libc::iovec] {
        &self.iovs
    }
}

/// Skip fully-sent entries, bump the partial one in place. Returns
/// true when `sent` consumed everything from `live` onward.
/// (`#[inline]` so an `advance` inlined into a consumer crate does
/// not bottom out in a cross-crate call here.)
#[inline]
fn advance_iovecs(iovs: &mut [libc::iovec], live: &mut usize, mut sent: usize) -> bool {
    while *live < iovs.len() {
        let e = &mut iovs[*live];
        if sent < e.iov_len {
            // SAFETY: stays within the entry's own chunk.
            e.iov_base = unsafe { e.iov_base.cast::<u8>().add(sent).cast() };
            e.iov_len -= sent;
            return false;
        }
        sent -= e.iov_len;
        *live += 1;
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn contiguous_arena_chunks_merge() {
        let mut b = GatherBatch::new(4096, 8);
        b.arena_tail()[..4].copy_from_slice(b"abcd");
        b.push_arena(4);
        b.arena_tail()[..2].copy_from_slice(b"ef");
        b.push_arena(2);
        // Two arena pushes, byte-contiguous: one iovec entry.
        assert_eq!(b.iovs.len(), 1);
        assert_eq!(b.iovs[0].iov_len, 6);
    }

    #[test]
    fn short_send_advances_in_place() {
        let mut b = GatherBatch::new(4096, 8);
        b.arena_tail()[..8].copy_from_slice(b"01234567");
        b.push_arena(8);
        let payload = [9u8; 16];
        b.push_raw(payload.as_ptr(), 16);
        assert_eq!(b.iovs.len(), 2);

        assert!(!b.advance(8 + 4)); // header + 4 payload bytes sent
        assert_eq!(b.live, 1);
        assert_eq!(b.iovs[1].iov_len, 12);
        assert!(b.advance(12));
    }

    #[test]
    fn advance_walks_partial_entry_twice() {
        // Two non-contiguous chunks; advancing nibbles the second
        // entry across multiple short sends. Heap buffers keep them
        // from merging.
        let mut b = GatherBatch::new(4096, 8);
        let a_data: Box<[u8]> = vec![1u8, 2, 3, 4].into_boxed_slice();
        let b_data: Box<[u8]> = vec![5u8, 6, 7].into_boxed_slice();
        b.push_raw(a_data.as_ptr(), 4);
        b.push_raw(b_data.as_ptr(), 3);
        assert_eq!(b.iovs.len(), 2);

        assert!(!b.advance(5)); // all of a + 1 byte of b
        assert_eq!(b.live, 1);
        assert_eq!(b.iovs[1].iov_len, 2);
        assert!(!b.advance(1)); // bump the same partial entry again
        assert_eq!(b.iovs[1].iov_len, 1);
        assert!(b.advance(1));
    }
}
