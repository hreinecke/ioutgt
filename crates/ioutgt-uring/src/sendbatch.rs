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
    /// Header arena: `[arena_ptr, arena_ptr+arena_len)`. Either heap-owned
    /// (`_arena_owner` holds the `Box`) or a borrowed region of a registered
    /// pool buffer (owner `None`; the caller keeps it alive). A raw pointer
    /// either way so both cases share one code path.
    arena_ptr: *mut u8,
    arena_len: usize,
    _arena_owner: Option<Box<[u8]>>,
    /// io_uring fixed-buffer index covering the arena — and, by construction,
    /// every payload staged into this batch (both live in the one registered
    /// data pool). `Some` enables a vectored fixed-buffer ZC send (no per-send
    /// page-pin/IOMMU map); `None` (heap arena) ⇒ plain SENDMSG_ZC.
    buf_index: Option<u16>,
    arena_used: usize,
    iovs: Vec<libc::iovec>,
    /// Hard entry cap (≤ UIO_MAXIOV). `Vec::with_capacity` may
    /// over-allocate, so the fit check must not use `capacity()`.
    iov_cap: usize,
    /// First entry not yet fully sent (short-send resume point).
    live: usize,
    msghdr: Box<libc::msghdr>,
    /// Total payload (non-header) bytes staged this round, and the number
    /// of payload-bearing items they came from. Their ratio is the average
    /// per-item payload — the signal a caller uses to choose copy vs
    /// zero-copy for the whole batch (small items: copy beats ZC's per-send
    /// page-pin + IOMMU map). Headers (`push_arena`) are excluded.
    payload_bytes: usize,
    payload_items: usize,
}

impl GatherBatch {
    /// Preallocate `arena_bytes` (≥ 4 KiB) of header arena and up to
    /// `iov_cap` iovec entries (clamped to [`UIO_MAXIOV`]).
    pub fn new(arena_bytes: usize, iov_cap: usize) -> GatherBatch {
        let mut owner = vec![0u8; arena_bytes.max(4096)].into_boxed_slice();
        let arena_ptr = owner.as_mut_ptr();
        let arena_len = owner.len();
        GatherBatch::build(arena_ptr, arena_len, Some(owner), None, iov_cap)
    }

    /// A batch whose header arena is a borrowed region `[ptr, ptr+len)` of the
    /// registered data pool, covered by fixed-buffer index `buf_index`. The
    /// region — and the pool registration — must outlive this `GatherBatch`.
    /// Lets a vectored fixed-buffer ZC send ship the whole gather (arena
    /// headers + pool payloads, all under one `buf_index`).
    ///
    /// # Safety
    ///
    /// `ptr..ptr+len` must be a valid, exclusively-owned region of the buffer
    /// registered at `buf_index`, living at least as long as this batch.
    pub unsafe fn from_pool_arena(
        ptr: *mut u8,
        len: usize,
        buf_index: u16,
        iov_cap: usize,
    ) -> GatherBatch {
        GatherBatch::build(ptr, len, None, Some(buf_index), iov_cap)
    }

    fn build(
        arena_ptr: *mut u8,
        arena_len: usize,
        owner: Option<Box<[u8]>>,
        buf_index: Option<u16>,
        iov_cap: usize,
    ) -> GatherBatch {
        let iov_cap = iov_cap.min(UIO_MAXIOV);
        GatherBatch {
            arena_ptr,
            arena_len,
            _arena_owner: owner,
            buf_index,
            arena_used: 0,
            iovs: Vec::with_capacity(iov_cap),
            iov_cap,
            live: 0,
            // SAFETY: a zeroed msghdr is a valid value; msg_iov[len]
            // are set in msghdr() before every submit.
            msghdr: Box::new(unsafe { std::mem::zeroed() }),
            payload_bytes: 0,
            payload_items: 0,
        }
    }

    /// Recycle for the next staging round.
    pub fn reset(&mut self) {
        self.arena_used = 0;
        self.iovs.clear();
        self.live = 0;
        self.payload_bytes = 0;
        self.payload_items = 0;
    }

    /// Record one payload-bearing item of `bytes` total payload (call once
    /// per item, after its `push_raw` segments). Feeds [`Self::avg_payload`].
    #[inline]
    pub fn note_payload(&mut self, bytes: usize) {
        self.payload_bytes += bytes;
        self.payload_items += 1;
    }

    /// Total payload (non-header) bytes staged this round. Drives the ZC
    /// gather cap: stop gathering a large-payload batch once it holds this
    /// much, bounding the pages a single SENDMSG_ZC pins/maps.
    #[inline]
    pub fn payload_bytes(&self) -> usize {
        self.payload_bytes
    }

    /// Average per-item payload bytes this round, or 0 when no item carried
    /// payload (e.g. a batch of R2Ts or capsule-only responses). The
    /// copy-vs-ZC discriminator: small average ⇒ copy beats ZC.
    #[inline]
    pub fn avg_payload(&self) -> usize {
        if self.payload_items == 0 {
            return 0;
        }
        self.payload_bytes / self.payload_items
    }

    /// Headroom for one more item of `arena_need` header bytes and
    /// `iovs_need` (unmerged) iovec entries?
    #[inline]
    pub fn fits(&self, arena_need: usize, iovs_need: usize) -> bool {
        self.arena_used + arena_need <= self.arena_len
            && self.iovs.len() + iovs_need <= self.iov_cap
    }

    /// Unused arena to encode the next header piece into.
    #[inline]
    pub fn arena_tail(&mut self) -> &mut [u8] {
        // SAFETY: `[arena_ptr, arena_ptr+arena_len)` is the valid arena (heap
        // box or borrowed registered pool region, both alive for `self`), and
        // `arena_used <= arena_len`, so the tail slice stays in bounds.
        unsafe {
            std::slice::from_raw_parts_mut(
                self.arena_ptr.add(self.arena_used),
                self.arena_len - self.arena_used,
            )
        }
    }

    /// Publish `len` bytes just written at the arena tail.
    #[inline]
    pub fn push_arena(&mut self, len: usize) {
        let start = self.arena_used;
        self.arena_used += len;
        // SAFETY: `start <= arena_len`; the pointer is within the arena.
        let ptr = unsafe { self.arena_ptr.add(start) };
        self.push_raw(ptr, len);
    }

    /// The fixed-buffer index covering this batch's arena and (by
    /// construction) its payloads — `Some` enables a vectored fixed-buffer
    /// ZC send; `None` (heap arena) means plain SENDMSG_ZC.
    #[inline]
    pub fn buf_index(&self) -> Option<u16> {
        self.buf_index
    }

    /// The unsent iovec suffix as a raw `(ptr, count)` — for a vectored
    /// fixed-buffer ZC send (`SEND_ZC | VECTORIZED | FIXED_BUF`), which takes
    /// the iovec array directly rather than a `msghdr`. Mirrors [`Self::msghdr`]
    /// and is advanced the same way by [`Self::advance`] on short sends.
    #[inline]
    pub fn live_iov(&self) -> (*const libc::iovec, u32) {
        let live = &self.iovs[self.live..];
        #[allow(clippy::cast_possible_truncation)] // len <= iov_cap <= UIO_MAXIOV
        (live.as_ptr(), live.len() as u32)
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
