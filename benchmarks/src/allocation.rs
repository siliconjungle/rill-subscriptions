//! Separate audit build only: counting is absent from the timing executable.
#[cfg(feature = "allocation_audit")]
mod enabled {
    use serde::Serialize;
    use std::{
        alloc::{GlobalAlloc, Layout, System},
        sync::atomic::{AtomicBool, AtomicUsize, Ordering::Relaxed},
    };
    struct Counting;
    #[global_allocator]
    static ALLOCATOR: Counting = Counting;
    static ACTIVE: AtomicBool = AtomicBool::new(false);
    static LIVE: AtomicUsize = AtomicUsize::new(0);
    static PEAK: AtomicUsize = AtomicUsize::new(0);
    static CALLS: AtomicUsize = AtomicUsize::new(0);
    static REALLOCS: AtomicUsize = AtomicUsize::new(0);
    static BYTES: AtomicUsize = AtomicUsize::new(0);
    static BASELINE: AtomicUsize = AtomicUsize::new(0);
    fn added(n: usize, realloc: bool) {
        let live = LIVE.fetch_add(n, Relaxed) + n;
        if ACTIVE.load(Relaxed) {
            PEAK.fetch_max(live, Relaxed);
            if !realloc {
                CALLS.fetch_add(1, Relaxed);
                BYTES.fetch_add(n, Relaxed);
            }
        }
    }
    // Each operation forwards exactly to the System allocator with its original
    // pointer/layout. Only successful allocations change requested-byte counts.
    unsafe impl GlobalAlloc for Counting {
        unsafe fn alloc(&self, l: Layout) -> *mut u8 {
            let p = unsafe { System.alloc(l) };
            if !p.is_null() {
                added(l.size(), false);
            }
            p
        }
        unsafe fn alloc_zeroed(&self, l: Layout) -> *mut u8 {
            let p = unsafe { System.alloc_zeroed(l) };
            if !p.is_null() {
                added(l.size(), false);
            }
            p
        }
        unsafe fn dealloc(&self, p: *mut u8, l: Layout) {
            unsafe { System.dealloc(p, l) };
            LIVE.fetch_sub(l.size(), Relaxed);
        }
        unsafe fn realloc(&self, p: *mut u8, l: Layout, n: usize) -> *mut u8 {
            let p = unsafe { System.realloc(p, l, n) };
            if !p.is_null() {
                if n >= l.size() {
                    added(n - l.size(), true);
                } else {
                    LIVE.fetch_sub(l.size() - n, Relaxed);
                }
                if ACTIVE.load(Relaxed) {
                    REALLOCS.fetch_add(1, Relaxed);
                    BYTES.fetch_add(n, Relaxed);
                }
            }
            p
        }
    }
    #[derive(Serialize)]
    pub struct Audit {
        allocation_calls: usize,
        reallocation_calls: usize,
        requested_bytes: usize,
        live_bytes_before: usize,
        live_bytes_after: usize,
        peak_live_requested_bytes: usize,
    }
    pub fn begin() {
        ACTIVE.store(false, Relaxed);
        CALLS.store(0, Relaxed);
        REALLOCS.store(0, Relaxed);
        BYTES.store(0, Relaxed);
        let live = LIVE.load(Relaxed);
        BASELINE.store(live, Relaxed);
        PEAK.store(live, Relaxed);
        ACTIVE.store(true, Relaxed);
    }
    pub fn end() -> Option<Audit> {
        ACTIVE.store(false, Relaxed);
        Some(Audit {
            allocation_calls: CALLS.load(Relaxed),
            reallocation_calls: REALLOCS.load(Relaxed),
            requested_bytes: BYTES.load(Relaxed),
            live_bytes_before: BASELINE.load(Relaxed),
            live_bytes_after: LIVE.load(Relaxed),
            peak_live_requested_bytes: PEAK.load(Relaxed),
        })
    }
}
#[cfg(feature = "allocation_audit")]
pub use enabled::*;
#[cfg(not(feature = "allocation_audit"))]
mod disabled {
    use serde::Serialize;
    #[derive(Serialize)]
    pub enum Audit {}
    #[inline]
    pub fn begin() {}
    #[inline]
    pub fn end() -> Option<Audit> {
        None
    }
}
#[cfg(not(feature = "allocation_audit"))]
pub use disabled::*;
