//! `eg-viz-columnstore` — the native visualization ColumnStore (D-VZ-1, lane V1).
//!
//! Implements the trait V0 (`eg-viz-core`) defined for this lane
//! ([`eg_viz_core::ColumnStoreIngest`]) and adds the real values-carrying ingest
//! surface that abstract trait deliberately left out:
//!
//! - [`store::ColumnStore::ingest_columns`] — the base API: any typed column
//!   batch ([`types::ColumnInput`]/[`types::ColumnData`]).
//! - [`store::ColumnStore::ingest_rowset`] — the `eg-plan::RowSet` bridge (id +
//!   optional score, that crate's own `Row` shape).
//! - [`store::ColumnStore::ingest_f64_array`] — the `eg-numeric` bridge (a flat
//!   `&[f64]`, what `ArrayD<f64>` materializes to).
//! - [`arrow_ipc::ingest_stream`] (feature `arrow-ipc`) — real Arrow IPC stream
//!   ingestion via the `arrow` crate's `StreamReader`.
//!
//! **Canonical columns** ([`store::Column`]) are chunked into fixed-size row
//! ranges ([`chunk::CHUNK_ROWS`]), each with a [`chunk::ZoneMap`]
//! (row/null count + numeric min/max) and a content-addressed
//! [`chunk::Chunk::content_id`] (`sha256`-keyed, deduplicating byte-identical
//! chunks across columns/datasets — see [`store::ColumnStore::stats`] and the
//! module tests for a direct proof). Nullable columns carry a bit-packed
//! validity bitmap; `Categorical` columns are dictionary-encoded
//! ([`dictionary::Dictionary`]) so a finite-cardinality string column costs
//! `O(distinct + rows*4)` rather than `O(sum of string lengths)`.
//!
//! ★ **Pure Rust on the hot path — no Python.** Every ingest/encode/decode path
//! in this crate is plain Rust over byte buffers; there is no PyO3 boundary and
//! no per-row Python callback anywhere in `ingest_columns`/`materialize_*`.
//!
//! ★ **This store never re-derives its own row/primitive budget.** It exposes
//! `row_count`/`range` so a caller feeds [`eg_viz_core::select_tier`] and gets
//! a [`eg_viz_core::TierDecision`] back — the store answers "how much data is
//! there" and "what does it contain", never "how much should I draw". See
//! `eg-viz-export`'s backend, which is the thing that actually honors a tier.

pub mod chunk;
pub mod dictionary;
pub mod error;
pub mod store;
pub mod types;

#[cfg(feature = "arrow-ipc")]
pub mod arrow_ipc;

pub use chunk::{Chunk, ZoneMap, CHUNK_ROWS};
pub use dictionary::Dictionary;
pub use error::ColumnStoreError;
pub use store::{Column, ColumnStore, Dataset, StoreStats};
pub use types::{ColumnData, ColumnInput};
