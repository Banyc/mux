//! Test-only allocation instrumentation shared by `mux` unit tests.
//!
//! A binary may install only one `#[global_allocator]`, so the unit-test
//! binary installs it here and every allocation-sensitive test observes the
//! same counters. `MAX_SINGLE_ALLOC` is process-global for the
//! large-single-allocation memory guard; `thread_alloc_count` is thread-local
//! so a tight measurement is not perturbed by unrelated tests allocating
//! concurrently on other threads.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::sync::atomic::{AtomicUsize, Ordering};

/// Largest single allocation requested by any thread since it was last reset.
pub(crate) static MAX_SINGLE_ALLOC: AtomicUsize = AtomicUsize::new(0);

thread_local! {
    /// Number of allocating calls made on the current thread. A `const`
    /// initializer keeps the instrumentation from recursively allocating.
    static THREAD_ALLOC_COUNT: Cell<usize> = const { Cell::new(0) };
    /// Bytes allocated on the current thread since the process started.
    static THREAD_ALLOC_BYTES: Cell<usize> = const { Cell::new(0) };
    /// Bytes deallocated on the current thread since the process started.
    static THREAD_DEALLOC_BYTES: Cell<usize> = const { Cell::new(0) };
}

/// Allocation calls (`alloc` / `alloc_zeroed` / `realloc`) observed on the
/// current thread.
pub(crate) fn thread_alloc_count() -> usize {
    THREAD_ALLOC_COUNT.with(Cell::get)
}

/// Bytes of live (still-allocated) state whose allocation and deallocation
/// both happened on the current thread: `alloc`/`realloc` credit their sizes,
/// `dealloc` debits the freed layout's size. On a single-threaded runtime
/// (a `current_thread` tokio test) every allocation a session makes is
/// observed here, and a symmetric teardown returns the reading to baseline.
pub(crate) fn thread_live_bytes() -> usize {
    THREAD_ALLOC_BYTES
        .with(Cell::get)
        .saturating_sub(THREAD_DEALLOC_BYTES.with(Cell::get))
}

/// The process-wide allocator that feeds the counters above.
struct CountingAllocator;

impl CountingAllocator {
    fn note(layout: Layout) {
        MAX_SINGLE_ALLOC.fetch_max(layout.size(), Ordering::Relaxed);
        THREAD_ALLOC_COUNT.with(|count| count.set(count.get().saturating_add(1)));
        THREAD_ALLOC_BYTES.with(|bytes| bytes.set(bytes.get().saturating_add(layout.size())));
    }

    fn release(layout: Layout) {
        THREAD_DEALLOC_BYTES.with(|bytes| bytes.set(bytes.get().saturating_add(layout.size())));
    }
}

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        Self::note(layout);
        unsafe { System.alloc(layout) }
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        Self::note(layout);
        unsafe { System.alloc_zeroed(layout) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        Self::release(layout);
        unsafe { System.dealloc(ptr, layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        Self::release(layout);
        let new_layout = unsafe { Layout::from_size_align_unchecked(new_size, layout.align()) };
        Self::note(new_layout);
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static COUNTING_ALLOCATOR: CountingAllocator = CountingAllocator;
