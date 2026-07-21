//! Payload overflow-threshold arithmetic, reimplemented EXACTLY from the public SQLite
//! file format (the formulas in `turso/core/storage/btree.rs` /
//! `sqlite3_ondisk.rs::payload_overflows`). An off-by-one here silently corrupts only
//! near a threshold boundary, which is why the writer is proved with a real `sqlite3`
//! `PRAGMA integrity_check` over a large-payload fixture.

use crate::page::PageType;

/// Maximum payload (X) storable directly on a b-tree page without spilling to overflow.
/// Table pages: `usable - 35`. Index pages: `((usable-12)*64/255) - 23`.
pub fn payload_overflow_threshold_max(page_type: PageType, usable_space: usize) -> usize {
    match page_type {
        PageType::IndexInterior | PageType::IndexLeaf => (usable_space - 12) * 64 / 255 - 23,
        PageType::TableInterior | PageType::TableLeaf => usable_space - 35,
    }
}

/// Minimum payload (M) that must stay on the page before spilling is allowed
/// (same for all page types): `((usable-12)*32/255) - 23`.
pub fn payload_overflow_threshold_min(usable_space: usize) -> usize {
    (usable_space - 12) * 32 / 255 - 23
}

/// Decide the local-vs-overflow split for a payload on a table-leaf page.
///
/// Returns `(overflows, local_payload_bytes)` where, when `overflows`, `local_payload_bytes`
/// of the payload are stored inline and a trailing 4-byte overflow-page pointer follows;
/// the remaining `payload_size - local_payload_bytes` bytes go to the overflow chain.
pub fn table_leaf_split(payload_size: usize, usable_space: usize) -> (bool, usize) {
    let max_local = payload_overflow_threshold_max(PageType::TableLeaf, usable_space);
    if payload_size <= max_local {
        return (false, payload_size);
    }
    let min_local = payload_overflow_threshold_min(usable_space);
    let mut space_left = min_local + (payload_size - min_local) % (usable_space - 4);
    if space_left > max_local {
        space_left = min_local;
    }
    (true, space_left)
}
