//! Command data buffers viewed as one or more physical segments.
//!
//! A command's payload no longer has to be one contiguous allocation: it
//! can be a contiguous run leased from a shared per-queue pool or a scatter
//! list when the pool is fragmented. Consumers — backend IO, the gather-send
//! staging, digest passes — go through the segment API ([`SlotData::segs`],
//! [`SlotData::write_at`], [`SlotData::for_each_seg`], [`SlotData::as_slice`])
//! and never name the backing.
//!
//! [`SlotData`] has three backings (`Owner`): `Empty` (resting between
//! commands), `Owned` (one page-aligned `AlignedBuf` — admin buffers and the
//! never-block write fallback), and `Pool` (pages leased from a [`BufPool`],
//! returned on drop).

use crate::buf::AlignedBuf;

/// Max physical segments a single command buffer can span: MDTS
/// (128 KiB) divided by the 4 KiB page granule.
pub const MAX_SEGS: usize = 32;

/// One physically-contiguous run of a command's data buffer.
///
/// The pointer may target a shared pool slab or kernel-owned ring
/// memory, so it is raw; the owning [`SlotData`] keeps the backing alive
/// for the segment's lifetime.
#[derive(Clone, Copy, Debug)]
pub struct Seg {
    /// Start of the run.
    pub ptr: *mut u8,
    /// Length of the run in bytes.
    pub len: usize,
}

/// A command's data buffer, as a (possibly scattered) segment list.
pub struct SlotData {
    /// Owns the backing allocation; the `segs` point into it.
    #[allow(dead_code)]
    inner: Inner,
    segs: [Seg; MAX_SEGS],
    nsegs: u8,
    len: usize,
}

enum Inner {
    /// A single owned page-aligned allocation, held for the slot's life
    /// (the `Seg` points into it; the field exists only to own the drop).
    Owned(#[allow(dead_code)] AlignedBuf),
}

const NULL_SEG: Seg = Seg {
    ptr: std::ptr::null_mut(),
    len: 0,
};

#[allow(missing_docs)] // accessor names mirror the field semantics
impl SlotData {
    /// A single owned buffer of `len` bytes (rounded up to a page).
    pub fn owned(len: usize) -> SlotData {
        let buf = AlignedBuf::zeroed(len);
        let n = buf.len();
        let ptr = buf.as_ptr().cast_mut();
        let mut segs = [NULL_SEG; MAX_SEGS];
        segs[0] = Seg { ptr, len: n };
        SlotData {
            inner: Inner::Owned(buf),
            segs,
            nsegs: 1,
            len: n,
        }
    }

    /// Logical capacity in bytes (sum of segment lengths).
    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// True when the buffer is one contiguous run.
    pub fn is_contiguous(&self) -> bool {
        self.nsegs == 1
    }

    /// The physical segments backing this buffer (1 entry when contiguous).
    pub fn segs(&self) -> &[Seg] {
        &self.segs[..self.nsegs as usize]
    }

    /// Contiguous read view. Panics (debug) if the buffer is scattered.
    pub fn as_slice(&self) -> &[u8] {
        // A hard assert, not debug-only: on a scattered buffer `seg[0]` is
        // one page but `self.len` may be larger, so the slice would read
        // past it. Contiguity guarantees `self.len <= segs[0].len`.
        assert!(self.is_contiguous(), "as_slice on a scattered buffer");
        // SAFETY: seg[0] is our exclusively-owned run of `self.len` bytes,
        // alive for as long as `self` (and thus this borrow).
        unsafe { std::slice::from_raw_parts(self.segs[0].ptr, self.len) }
    }

    /// Contiguous write view. Panics (debug) if the buffer is scattered.
    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        // Hard assert for the same reason as [`Self::as_slice`].
        assert!(self.is_contiguous(), "as_mut_slice on a scattered buffer");
        // SAFETY: as `as_slice`, plus `&mut self` gives exclusivity.
        unsafe { std::slice::from_raw_parts_mut(self.segs[0].ptr, self.len) }
    }

    /// Copy `src` into the buffer at logical offset `off`, crossing
    /// segment boundaries as needed.
    pub fn write_at(&mut self, mut off: usize, mut src: &[u8]) {
        for seg in &self.segs[..self.nsegs as usize] {
            if src.is_empty() {
                return;
            }
            if off >= seg.len {
                off -= seg.len;
                continue;
            }
            let take = (seg.len - off).min(src.len());
            // SAFETY: ptr.add(off)..+take stays within this owned segment
            // (off < seg.len, take <= seg.len - off); src is disjoint.
            unsafe { std::ptr::copy_nonoverlapping(src.as_ptr(), seg.ptr.add(off), take) };
            src = &src[take..];
            off = 0;
        }
        debug_assert!(src.is_empty(), "write_at past end of buffer");
    }

    /// Invoke `f` with each contiguous sub-slice of the logical range
    /// `[off, off+len)`, in order.
    pub fn for_each_seg(&self, mut off: usize, mut len: usize, mut f: impl FnMut(&[u8])) {
        for seg in &self.segs[..self.nsegs as usize] {
            if len == 0 {
                return;
            }
            if off >= seg.len {
                off -= seg.len;
                continue;
            }
            let take = (seg.len - off).min(len);
            // SAFETY: ptr.add(off)..+take stays within this owned segment.
            let chunk = unsafe { std::slice::from_raw_parts(seg.ptr.add(off), take) };
            f(chunk);
            len -= take;
            off = 0;
        }
        debug_assert_eq!(len, 0, "for_each_seg past end of buffer");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn owned_is_contiguous_and_page_sized() {
        let d = SlotData::owned(8 * 1024);
        assert!(d.is_contiguous());
        assert_eq!(d.segs().len(), 1);
        assert_eq!(d.len(), 8 * 1024);
        assert_eq!(d.segs()[0].len, 8 * 1024);
    }

    #[test]
    fn owned_rounds_up_to_page() {
        let d = SlotData::owned(100);
        assert_eq!(d.len(), 4096);
    }

    #[test]
    fn write_at_then_read_back_via_slice() {
        let mut d = SlotData::owned(4096);
        d.write_at(10, &[1, 2, 3, 4]);
        assert_eq!(&d.as_slice()[10..14], &[1, 2, 3, 4]);
    }

    #[test]
    #[allow(clippy::cast_possible_truncation)] // 0..256 fits u8 exactly
    fn for_each_seg_visits_requested_range_contiguous() {
        let mut d = SlotData::owned(4096);
        for i in 0..256u32 {
            d.write_at(i as usize, &[i as u8]);
        }
        let mut seen = Vec::new();
        d.for_each_seg(64, 32, |chunk| seen.extend_from_slice(chunk));
        let want: Vec<u8> = (64u32..96).map(|i| i as u8).collect();
        assert_eq!(seen, want);
    }

    #[test]
    fn contiguous_view_matches_manual_slice() {
        let mut d = SlotData::owned(4096);
        d.as_mut_slice()[..5].copy_from_slice(b"hello");
        let mut viaseg = Vec::new();
        d.for_each_seg(0, 5, |c| viaseg.extend_from_slice(c));
        assert_eq!(&viaseg, b"hello");
        assert_eq!(&d.as_slice()[..5], b"hello");
    }
}
