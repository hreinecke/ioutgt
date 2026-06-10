//! Page-aligned byte buffers.
//!
//! Slot data buffers must satisfy O_DIRECT alignment (logical block
//! size; we use 4 KiB which covers every device) — `Vec<u8>` allocations
//! only guarantee alignment 1.

use std::alloc::{Layout, alloc_zeroed, dealloc};
use std::ops::{Deref, DerefMut};

/// Alignment for all data buffers: max page / LBA size we support.
pub const BUF_ALIGN: usize = 4096;

/// A heap buffer aligned to [`BUF_ALIGN`].
pub struct AlignedBuf {
    ptr: *mut u8,
    len: usize,
}

impl AlignedBuf {
    /// Allocate `len` zeroed bytes (rounded up to the alignment).
    pub fn zeroed(len: usize) -> AlignedBuf {
        let size = len.next_multiple_of(BUF_ALIGN).max(BUF_ALIGN);
        let layout = Layout::from_size_align(size, BUF_ALIGN).expect("valid layout");
        // SAFETY: non-zero size, valid layout; null checked below.
        let ptr = unsafe { alloc_zeroed(layout) };
        assert!(!ptr.is_null(), "aligned allocation failed");
        AlignedBuf { ptr, len: size }
    }

    /// Allocated length (the requested size rounded up to alignment).
    pub fn len(&self) -> usize {
        self.len
    }

    /// Never true in practice (allocations are at least one page).
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }
}

impl Deref for AlignedBuf {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        // SAFETY: ptr/len describe our exclusive allocation.
        unsafe { std::slice::from_raw_parts(self.ptr, self.len) }
    }
}

impl DerefMut for AlignedBuf {
    fn deref_mut(&mut self) -> &mut [u8] {
        // SAFETY: as above; &mut self gives exclusivity.
        unsafe { std::slice::from_raw_parts_mut(self.ptr, self.len) }
    }
}

impl Drop for AlignedBuf {
    fn drop(&mut self) {
        let layout = Layout::from_size_align(self.len, BUF_ALIGN).expect("valid layout");
        // SAFETY: allocated with this exact layout in `zeroed`.
        unsafe { dealloc(self.ptr, layout) };
    }
}

// SAFETY: AlignedBuf is a plain owned allocation; sending it between
// threads is as safe as sending a Vec<u8>.
unsafe impl Send for AlignedBuf {}
// SAFETY: &AlignedBuf only exposes &[u8].
unsafe impl Sync for AlignedBuf {}
