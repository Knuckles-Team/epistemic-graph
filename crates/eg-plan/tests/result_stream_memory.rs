//! A separate test executable keeps the allocation probe scoped to this scan.
//! Host RSS and the eg-plan unit-test process include unrelated concurrent work.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::rc::Rc;
use std::sync::atomic::{AtomicUsize, Ordering};

use eg_modality::OpaqueRef;
use eg_plan::knowledge_batch::KnowledgeBatchRow;
use eg_plan::result_stream::{graph_result_stream, KnowledgeStreamContext};

static LIVE_BYTES: AtomicUsize = AtomicUsize::new(0);
static PEAK_BYTES: AtomicUsize = AtomicUsize::new(0);

struct CountingAllocator;

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

fn record_allocation(bytes: usize) {
    let live = LIVE_BYTES.fetch_add(bytes, Ordering::Relaxed) + bytes;
    PEAK_BYTES.fetch_max(live, Ordering::Relaxed);
}

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let pointer = unsafe { System.alloc(layout) };
        if !pointer.is_null() {
            record_allocation(layout.size());
        }
        pointer
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let pointer = unsafe { System.alloc_zeroed(layout) };
        if !pointer.is_null() {
            record_allocation(layout.size());
        }
        pointer
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        LIVE_BYTES.fetch_sub(layout.size(), Ordering::Relaxed);
        unsafe { System.dealloc(pointer, layout) };
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let resized = unsafe { System.realloc(pointer, layout, new_size) };
        if !resized.is_null() {
            if new_size >= layout.size() {
                record_allocation(new_size - layout.size());
            } else {
                LIVE_BYTES.fetch_sub(layout.size() - new_size, Ordering::Relaxed);
            }
        }
        resized
    }
}

fn reference(namespace: &str, suffix: u8) -> OpaqueRef {
    OpaqueRef::scoped(namespace, &format!("00000000000000{suffix:02x}")).unwrap()
}

fn context() -> KnowledgeStreamContext {
    KnowledgeStreamContext::from_refs([
        reference("tenant", 1),
        reference("policy", 2),
        reference("placement", 7),
        reference("snapshot", 3),
        reference("query", 4),
        reference("derivation", 5),
        reference("evidenceset", 6),
    ])
}

struct CountingRows {
    next: usize,
    total: usize,
    pulled: Rc<Cell<usize>>,
}

impl Iterator for CountingRows {
    type Item = KnowledgeBatchRow;

    fn next(&mut self) -> Option<Self::Item> {
        if self.next >= self.total {
            return None;
        }
        let index = self.next;
        self.next += 1;
        self.pulled.set(self.next);
        Some(KnowledgeBatchRow {
            id: format!("eg:node:{index:016x}"),
            kind: "result".to_string(),
            scores: vec![("score".to_string(), Some(index as f32 / 10.0))],
            confidence: 0.9,
            ..KnowledgeBatchRow::default()
        })
    }
}

#[test]
fn million_row_scan_streams_with_flat_memory() {
    const ROWS: usize = 1_000_000;
    const BATCH: usize = 1_024;
    const MAX_PEAK_GROWTH: usize = 64 * 1024 * 1024;

    let pulled = Rc::new(Cell::new(0));
    let source = CountingRows {
        next: 0,
        total: ROWS,
        pulled: pulled.clone(),
    };
    let mut stream =
        graph_result_stream(context(), vec!["score".to_string()], source, BATCH).unwrap();
    let baseline = LIVE_BYTES.load(Ordering::Relaxed);
    PEAK_BYTES.store(baseline, Ordering::Relaxed);

    let mut emitted = 0;
    let mut batches = 0u64;
    while let Some(envelope) = stream.next_batch().unwrap() {
        let rows = envelope.batch.len();
        assert!(rows <= BATCH, "resident batch {rows} exceeded {BATCH}");
        emitted += rows;
        batches += 1;
        assert!(
            pulled.get() <= emitted + BATCH,
            "producer pulled {} rows after emitting {emitted}",
            pulled.get()
        );
    }

    assert_eq!(emitted, ROWS);
    assert_eq!(batches, (ROWS as u64).div_ceil(BATCH as u64));
    assert!(stream.cursor().exhausted);
    let peak_growth = PEAK_BYTES.load(Ordering::Relaxed).saturating_sub(baseline);
    assert!(
        peak_growth < MAX_PEAK_GROWTH,
        "stream allocation peak grew {peak_growth} bytes over 1M rows"
    );
}
