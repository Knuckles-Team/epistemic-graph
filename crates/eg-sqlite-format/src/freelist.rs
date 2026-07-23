//! Freelist trunk-page layout constants (public SQLite file format). The bulk-load
//! writer never produces a freelist (it only ever appends pages), so `header.freelist_*`
//! are always `0`; these constants exist for read-side validation and are asserted `0`
//! in the reader.
#![allow(dead_code)] // read-side validation + future incremental-writer constants

pub const FREELIST_TRUNK_OFFSET_NEXT_TRUNK_PTR: usize = 0;
pub const FREELIST_TRUNK_OFFSET_LEAF_COUNT: usize = 4;
pub const FREELIST_TRUNK_OFFSET_FIRST_LEAF_PTR: usize = 8;
pub const FREELIST_TRUNK_HEADER_SIZE: usize = 8;
pub const FREELIST_LEAF_PTR_SIZE: usize = 4;
