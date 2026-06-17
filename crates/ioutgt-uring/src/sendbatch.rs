//! Gather-send staging: an iovec list referencing caller-owned bytes in
//! place (PDU headers and payload alike), shipped as one vectored send
//! and resumed in place after short sends. Pure byte/pointer plumbing —
//! no protocol knowledge and no owned buffer; callers encode their own
//! headers into their own storage (a slot's header scratch) and hand us
//! pointers. Any stream transport reuses this (NVMe/TCP today, NBD next).

/// Kernel cap on `msg_iovlen`.
pub const UIO_MAXIOV: usize = libc::UIO_MAXIOV as usize;

/// One batch's gather state. The iovec list is preallocated at
/// construction; `reset()` recycles it.
pub struct GatherBatch {
    iovs: Vec<libc::iovec>,
    /// Hard entry cap (≤ UIO_MAXIOV). `Vec::with_capacity` may
    /// over-allocate, so the fit check must not use `capacity()`.
    iov_cap: usize,
    /// First entry not yet fully sent (short-send resume point).
    live: usize,
    msghdr: Box<libc::msghdr>,
}

impl GatherBatch {
    /// Preallocate up to `iov_cap` iovec entries (clamped to
    /// [`UIO_MAXIOV`]).
    pub fn new(iov_cap: usize) -> GatherBatch {
        let iov_cap = iov_cap.min(UIO_MAXIOV);
        GatherBatch {
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
        self.iovs.clear();
        self.live = 0;
    }

    /// Headroom for one more item of `iovs_need` (unmerged) iovec
    /// entries?
    #[inline]
    pub fn fits(&self, iovs_need: usize) -> bool {
        self.iovs.len() + iovs_need <= self.iov_cap
    }

    /// Append a wire chunk (a pointer into caller-owned storage); merges
    /// with the previous entry when byte-contiguous, so adjacent header
    /// pieces in the same scratch collapse to one entry.
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
    fn contiguous_chunks_merge() {
        let mut b = GatherBatch::new(8);
        let buf: Box<[u8]> = vec![0u8; 6].into_boxed_slice();
        b.push_raw(buf.as_ptr(), 4);
        // SAFETY: same allocation, one-past the first 4 bytes.
        b.push_raw(unsafe { buf.as_ptr().add(4) }, 2);
        // Two byte-contiguous pushes collapse to one iovec entry.
        assert_eq!(b.iovs.len(), 1);
        assert_eq!(b.iovs[0].iov_len, 6);
    }

    #[test]
    fn short_send_advances_in_place() {
        let mut b = GatherBatch::new(8);
        // Distinct heap allocations so the two chunks never merge.
        let hdr: Box<[u8]> = vec![0u8; 8].into_boxed_slice();
        let payload: Box<[u8]> = vec![9u8; 16].into_boxed_slice();
        b.push_raw(hdr.as_ptr(), 8);
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
        let mut b = GatherBatch::new(8);
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
