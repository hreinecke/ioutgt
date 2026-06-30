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

use std::cell::{Cell, RefCell};
use std::rc::Rc;
use std::task::{Poll, Waker};

use crate::buf::AlignedBuf;

/// Allocation granule and minimum segment size.
pub const PAGE: usize = 4096;

/// Default per-IO-queue data-buffer pool size, in MiB. The single source for
/// the `--queue-buf-mb` CLI default, the JSON `queue_buf_mb` default, and the
/// in-process `TargetConfig` default.
pub const DEFAULT_POOL_MB: usize = 8;

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

/// A provider of recv-ring sub-buffers, implemented outside core (in the
/// transport, which owns the `ioutgt-uring` ring) so `ioutgt-core` keeps its
/// no-`ioutgt-uring`-dependency invariant. A ring-backed [`SlotData`] holds an
/// `Rc<dyn RingProvider>` (a refcount clone of one long-lived handle — no
/// per-write allocation) and returns the borrowed sub-buffer on drop.
pub trait RingProvider {
    /// The io_uring fixed-buffer index of sub-buffer `bid`, if registered, so
    /// the backend can `WRITE_FIXED` straight from ring memory.
    fn buf_index(&self, bid: u16) -> Option<u16>;
    /// Release one borrow of sub-buffer `bid` (a retained write finished).
    fn release(&self, bid: u16);
}

/// A command's data buffer, as a (possibly scattered) segment list.
///
/// The `segs` are the view every consumer uses; `owner` keeps the backing
/// alive and, for a pool lease, returns the pages on drop.
pub struct SlotData {
    segs: [Seg; MAX_SEGS],
    nsegs: u8,
    len: usize,
    owner: Owner,
}

enum Owner {
    /// No buffer leased (the resting state between commands).
    Empty,
    /// A single owned page-aligned allocation (drops itself).
    Owned(#[allow(dead_code)] AlignedBuf),
    /// Pages leased from a [`BufPool`]; returned on drop.
    Pool(Rc<BufPool>),
    /// A retained region of a recv-ring sub-buffer (zero-copy write receive):
    /// the segment points into ring memory the `provider` keeps borrowed until
    /// this drops, when `provider.release(bid)` runs.
    Ring {
        provider: Rc<dyn RingProvider>,
        bid: u16,
    },
}

const NULL_SEG: Seg = Seg {
    ptr: std::ptr::null_mut(),
    len: 0,
};

impl Drop for SlotData {
    fn drop(&mut self) {
        match &self.owner {
            Owner::Pool(pool) => pool.free_segs(&self.segs[..self.nsegs as usize]),
            Owner::Ring { provider, bid } => provider.release(*bid),
            Owner::Empty | Owner::Owned(_) => {}
        }
    }
}

#[allow(missing_docs)] // accessor names mirror the field semantics
impl SlotData {
    /// No buffer leased. Accessors that need bytes panic until a lease
    /// replaces this (the recv path / dispatch lease before use).
    pub fn empty() -> SlotData {
        SlotData {
            segs: [NULL_SEG; MAX_SEGS],
            nsegs: 0,
            len: 0,
            owner: Owner::Empty,
        }
    }

    /// A single owned buffer of `len` bytes (rounded up to a page).
    pub fn owned(len: usize) -> SlotData {
        let buf = AlignedBuf::zeroed(len);
        let n = buf.len();
        let ptr = buf.as_ptr().cast_mut();
        let mut segs = [NULL_SEG; MAX_SEGS];
        segs[0] = Seg { ptr, len: n };
        SlotData {
            segs,
            nsegs: 1,
            len: n,
            owner: Owner::Owned(buf),
        }
    }

    /// A zero-copy lease of one contiguous region `ptr..ptr+len` of recv-ring
    /// sub-buffer `bid`. The caller must have already taken the borrow
    /// (`provider`-side `borrow(bid)`); the matching release runs on drop.
    ///
    /// # Safety
    /// `ptr..ptr+len` must be a valid region of sub-buffer `bid` that the
    /// `provider`'s borrow keeps alive for this `SlotData`'s whole lifetime.
    pub unsafe fn ring(
        provider: Rc<dyn RingProvider>,
        bid: u16,
        ptr: *mut u8,
        len: usize,
    ) -> SlotData {
        let mut segs = [NULL_SEG; MAX_SEGS];
        segs[0] = Seg { ptr, len };
        SlotData {
            segs,
            nsegs: 1,
            len,
            owner: Owner::Ring { provider, bid },
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

    /// Whether the backing is a registered pool lease (vs owned/empty/ring).
    /// The NVMe/RDMA transport RDMA-READs host write-data straight into the slot
    /// using the pool arena's MR key, which is valid only for pool-leased
    /// segments; an owned fallback (pool momentarily full) must take another path.
    pub fn is_pool(&self) -> bool {
        matches!(self.owner, Owner::Pool(_))
    }

    /// The io_uring fixed-buffer index covering these segments, when this is a
    /// registered pool lease (`None` for owned/admin buffers). The backend
    /// uses it to pick `READV_FIXED`/`WRITEV_FIXED` over plain readv/writev.
    pub fn buf_index(&self) -> Option<u16> {
        match &self.owner {
            Owner::Pool(pool) => pool.buf_index(),
            Owner::Ring { provider, bid } => provider.buf_index(*bid),
            Owner::Owned(_) | Owner::Empty => None,
        }
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

/// A free run of contiguous pages (page units, not bytes).
#[derive(Clone, Copy)]
struct Run {
    start: u32,
    len: u32,
}

/// A per-queue contiguous data-buffer arena with a coalescing free-run
/// allocator. Hands out [`SlotData`] leases — contiguous when a single
/// run fits, otherwise a scatter list of up to [`MAX_SEGS`] runs. Single
/// threaded (`Cell`/`RefCell`, no atomics), like the rest of core.
pub struct BufPool {
    base: AlignedBuf,
    npages: u32,
    /// Free runs, kept sorted by `start` and coalesced.
    free: RefCell<Vec<Run>>,
    free_pages: Cell<u32>,
    /// Tasks parked waiting for pages; woken on any free.
    waiters: RefCell<Vec<Waker>>,
    /// io_uring fixed-buffer index for the whole arena, set once the
    /// transport registers it (`None` until then, or when the kernel lacks
    /// fixed-buffer support). Pool leases carry it to the backend so disk IO
    /// can use `READV_FIXED`/`WRITEV_FIXED`.
    buf_index: Cell<Option<u16>>,
    /// Page-aligned region carved from the arena for the send path's header
    /// gather (`reserve_arena`), shared with payloads under one `buf_index` so
    /// a vectored fixed-buffer ZC send needs no per-send page map. Never freed
    /// back to `free`; lives as long as the pool.
    send_arena: Cell<Option<(*mut u8, usize)>>,
}

/// Insert `run` into the sorted free list, coalescing with neighbors.
fn insert_run(free: &mut Vec<Run>, run: Run) {
    let pos = free.partition_point(|r| r.start < run.start);
    free.insert(pos, run);
    // Merge with the following run, then the preceding one.
    if pos + 1 < free.len() && free[pos].start + free[pos].len == free[pos + 1].start {
        free[pos].len += free[pos + 1].len;
        free.remove(pos + 1);
    }
    if pos > 0 && free[pos - 1].start + free[pos - 1].len == free[pos].start {
        free[pos - 1].len += free[pos].len;
        free.remove(pos);
    }
}

#[allow(missing_docs)] // method names mirror the semantics
impl BufPool {
    /// A pool of `bytes` rounded up to a whole number of pages (min 1).
    pub fn new(bytes: usize) -> Rc<BufPool> {
        let npages = u32::try_from(bytes.div_ceil(PAGE).max(1)).expect("pool page count fits u32");
        let base = AlignedBuf::zeroed(npages as usize * PAGE);
        Rc::new(BufPool {
            base,
            npages,
            free: RefCell::new(vec![Run {
                start: 0,
                len: npages,
            }]),
            free_pages: Cell::new(npages),
            waiters: RefCell::new(Vec::new()),
            buf_index: Cell::new(None),
            send_arena: Cell::new(None),
        })
    }

    /// The whole arena as `(ptr, len)` — what the transport registers as one
    /// io_uring fixed buffer.
    pub fn arena(&self) -> (*const u8, usize) {
        (self.base.as_ptr(), self.npages as usize * PAGE)
    }

    /// Record the fixed-buffer index the transport registered the arena under.
    pub fn set_buf_index(&self, idx: u16) {
        self.buf_index.set(Some(idx));
    }

    /// The arena's fixed-buffer index, if registered.
    pub fn buf_index(&self) -> Option<u16> {
        self.buf_index.get()
    }

    /// Clear and return the fixed-buffer index (teardown, before unregister).
    pub fn take_buf_index(&self) -> Option<u16> {
        self.buf_index.take()
    }

    /// Permanently reserve `bytes` (rounded up to whole pages) of contiguous
    /// arena for non-slot use — the send-path header arena — returning its
    /// `(ptr, len)`. Unlike [`Self::alloc`] the pages are never returned to
    /// the free list; the region lives as long as the pool. Because it lies
    /// inside the one registered fixed buffer, iovecs into it share the pool's
    /// `buf_index`, so a vectored fixed-buffer ZC send can cover arena headers
    /// and slot payloads in one op. `None` when the pool is not registered
    /// (no `buf_index`) or has no contiguous run that large.
    pub fn reserve_arena(&self, bytes: usize) -> Option<(*mut u8, usize)> {
        self.buf_index.get()?;
        let pages = u32::try_from(bytes.div_ceil(PAGE).max(1)).ok()?;
        let mut free = self.free.borrow_mut();
        let i = free.iter().position(|r| r.len >= pages)?;
        let start = free[i].start;
        free[i].start += pages;
        free[i].len -= pages;
        if free[i].len == 0 {
            free.remove(i);
        }
        drop(free);
        self.free_pages.set(self.free_pages.get() - pages);
        let region = (self.page_ptr(start), pages as usize * PAGE);
        self.send_arena.set(Some(region));
        Some(region)
    }

    /// The reserved send-arena region, if [`Self::reserve_arena`] has run.
    pub fn send_arena(&self) -> Option<(*mut u8, usize)> {
        self.send_arena.get()
    }

    pub fn capacity_pages(&self) -> u32 {
        self.npages
    }

    pub fn free_pages(&self) -> u32 {
        self.free_pages.get()
    }

    fn page_ptr(&self, page: u32) -> *mut u8 {
        // SAFETY: `page < npages`; the result stays within the slab.
        unsafe { self.base.as_ptr().cast_mut().add(page as usize * PAGE) }
    }

    fn make_lease(self: &Rc<Self>, segs: [Seg; MAX_SEGS], nsegs: u8, pages: u32) -> SlotData {
        self.free_pages.set(self.free_pages.get() - pages);
        SlotData {
            segs,
            nsegs,
            len: pages as usize * PAGE,
            owner: Owner::Pool(Rc::clone(self)),
        }
    }

    /// Lease `len` bytes (rounded up to whole pages): a contiguous run if
    /// one is free, else a scatter list, else `None` (insufficient free
    /// space, or the request would need more than [`MAX_SEGS`] runs).
    pub fn alloc(self: &Rc<Self>, len: usize) -> Option<SlotData> {
        let pages = u32::try_from(len.div_ceil(PAGE).max(1)).ok()?;
        if self.free_pages.get() < pages {
            return None;
        }
        let mut free = self.free.borrow_mut();
        let mut segs = [NULL_SEG; MAX_SEGS];

        // Fast path: a single run that fits (keeps the buffer contiguous).
        if let Some(i) = free.iter().position(|r| r.len >= pages) {
            let start = free[i].start;
            free[i].start += pages;
            free[i].len -= pages;
            if free[i].len == 0 {
                free.remove(i);
            }
            segs[0] = Seg {
                ptr: self.page_ptr(start),
                len: pages as usize * PAGE,
            };
            drop(free);
            return Some(self.make_lease(segs, 1, pages));
        }

        // Scatter path: confirm it fits in ≤ MAX_SEGS runs before mutating.
        let mut need = pages;
        let mut count = 0;
        for r in free.iter() {
            if need == 0 {
                break;
            }
            count += 1;
            if count > MAX_SEGS {
                return None;
            }
            need = need.saturating_sub(r.len);
        }
        if need > 0 {
            return None; // (unreachable given the free_pages check)
        }

        // Carve from the front; every taken run is whole except the last.
        let mut need = pages;
        let mut nsegs = 0usize;
        while need > 0 {
            let take = need.min(free[0].len);
            segs[nsegs] = Seg {
                ptr: self.page_ptr(free[0].start),
                len: take as usize * PAGE,
            };
            nsegs += 1;
            free[0].start += take;
            free[0].len -= take;
            need -= take;
            if free[0].len == 0 {
                free.remove(0);
            }
        }
        drop(free);
        #[allow(clippy::cast_possible_truncation)] // nsegs <= MAX_SEGS (32)
        Some(self.make_lease(segs, nsegs as u8, pages))
    }

    /// Lease `len` bytes, parking until enough pages free up. Backpressure
    /// for an oversubscribed pool; never deadlocks because frees never
    /// depend on the parked task.
    pub async fn alloc_await(self: &Rc<Self>, len: usize) -> SlotData {
        std::future::poll_fn(|cx| match self.alloc(len) {
            Some(d) => Poll::Ready(d),
            None => {
                self.waiters.borrow_mut().push(cx.waker().clone());
                Poll::Pending
            }
        })
        .await
    }

    /// Return leased pages to the free list (called from `SlotData::drop`).
    fn free_segs(&self, segs: &[Seg]) {
        let base = self.base.as_ptr() as usize;
        let mut free = self.free.borrow_mut();
        let mut freed = 0u32;
        for seg in segs {
            let start = u32::try_from((seg.ptr as usize - base) / PAGE).expect("page index");
            let len = u32::try_from(seg.len / PAGE).expect("page count");
            freed += len;
            insert_run(&mut free, Run { start, len });
        }
        drop(free);
        self.free_pages.set(self.free_pages.get() + freed);
        // Wake everyone parked; they re-contend (bounded by queue depth).
        let woken: Vec<Waker> = self.waiters.borrow_mut().drain(..).collect();
        for w in woken {
            w.wake();
        }
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

    fn poll<T>(fut: std::pin::Pin<&mut impl Future<Output = T>>) -> Poll<T> {
        let waker = Waker::noop();
        let mut cx = std::task::Context::from_waker(waker);
        fut.poll(&mut cx)
    }
    use std::future::Future;

    #[test]
    fn pool_rounds_to_pages() {
        let p = BufPool::new(4 * 1024 * 1024);
        assert_eq!(p.capacity_pages(), 1024);
        assert_eq!(p.free_pages(), 1024);
        let p2 = BufPool::new(100);
        assert_eq!(p2.capacity_pages(), 1);
    }

    #[test]
    fn alloc_contiguous_then_free_coalesces() {
        let p = BufPool::new(4 * PAGE);
        {
            let d = p.alloc(2 * PAGE).unwrap();
            assert!(d.is_contiguous());
            assert_eq!(d.len(), 2 * PAGE);
            assert_eq!(p.free_pages(), 2);
        }
        // Drop returned the pages and coalesced back to one full run.
        assert_eq!(p.free_pages(), 4);
        let d = p.alloc(4 * PAGE).unwrap();
        assert!(d.is_contiguous(), "fully coalesced run is contiguous again");
    }

    #[test]
    fn alloc_returns_scatter_when_fragmented() {
        let p = BufPool::new(4 * PAGE);
        let a = p.alloc(PAGE).unwrap(); // page 0
        let b = p.alloc(PAGE).unwrap(); // page 1
        let c = p.alloc(PAGE).unwrap(); // page 2
        let _d = p.alloc(PAGE).unwrap(); // page 3
        assert_eq!(p.free_pages(), 0);
        // Free pages 0 and 2 → two 1-page holes, non-adjacent.
        drop(a);
        drop(c);
        let _ = b; // keep page 1 held so the holes stay split
        assert_eq!(p.free_pages(), 2);
        let scattered = p.alloc(2 * PAGE).unwrap();
        assert!(!scattered.is_contiguous());
        assert_eq!(scattered.segs().len(), 2);
        assert_eq!(scattered.len(), 2 * PAGE);
    }

    #[test]
    fn alloc_none_when_exhausted() {
        let p = BufPool::new(2 * PAGE);
        let _a = p.alloc(2 * PAGE).unwrap();
        assert!(p.alloc(PAGE).is_none());
    }

    #[test]
    #[allow(clippy::cast_possible_truncation)] // i % 256 fits u8
    fn scatter_roundtrips_bytes_across_segments() {
        let p = BufPool::new(4 * PAGE);
        let a = p.alloc(PAGE).unwrap();
        let b = p.alloc(PAGE).unwrap();
        let c = p.alloc(PAGE).unwrap();
        let _d = p.alloc(PAGE).unwrap();
        drop(a);
        drop(c);
        let _ = b;
        let mut s = p.alloc(2 * PAGE).unwrap();
        assert!(!s.is_contiguous());
        // Write a pattern spanning the seam and read it back.
        let src: Vec<u8> = (0..2 * PAGE).map(|i| (i % 256) as u8).collect();
        s.write_at(0, &src);
        let mut back = Vec::new();
        s.for_each_seg(0, 2 * PAGE, |chunk| back.extend_from_slice(chunk));
        assert_eq!(back, src);
    }

    #[test]
    fn alloc_await_wakes_on_free() {
        let p = BufPool::new(PAGE);
        let held = p.alloc(PAGE).unwrap();
        assert_eq!(p.free_pages(), 0);

        let fut = p.alloc_await(PAGE);
        let mut fut = std::pin::pin!(fut);
        assert!(poll(fut.as_mut()).is_pending());

        drop(held); // frees the page and wakes waiters
        assert!(matches!(poll(fut.as_mut()), Poll::Ready(d) if d.len() == PAGE));
    }

    // A ring-backed SlotData reports the provider's fixed-buffer index and
    // returns the borrowed sub-buffer (release(bid)) exactly on drop.
    #[test]
    fn ring_slotdata_reports_index_and_releases_on_drop() {
        struct MockProvider {
            released: Cell<Option<u16>>,
            idx: Option<u16>,
        }
        impl RingProvider for MockProvider {
            fn buf_index(&self, _bid: u16) -> Option<u16> {
                self.idx
            }
            fn release(&self, bid: u16) {
                self.released.set(Some(bid));
            }
        }
        let provider = Rc::new(MockProvider {
            released: Cell::new(None),
            idx: Some(3),
        });
        let mut buf = vec![0u8; 4096];
        // SAFETY: `buf` outlives `d` below.
        let d = unsafe {
            SlotData::ring(
                Rc::clone(&provider) as Rc<dyn RingProvider>,
                1,
                buf.as_mut_ptr(),
                4096,
            )
        };
        assert_eq!(d.buf_index(), Some(3), "ring buf_index comes from provider");
        assert!(d.is_contiguous());
        assert_eq!(d.len(), 4096);
        assert_eq!(provider.released.get(), None, "no release while alive");
        drop(d);
        assert_eq!(provider.released.get(), Some(1), "release(bid) ran on drop");
    }
}
