//! eg-tsdb — native time-series store + TS query primitives for the epistemic-graph
//! engine (CONCEPT:KG-2.210 store + primitives, CONCEPT:KG-2.211 decay unification +
//! Time-ops sketch).
//!
//! Sits ABOVE eg-compute in the DAG (`eg-types → eg-core → eg-compute → eg-tsdb →
//! epistemic-graph`): the store needs redb (an eg-core/persistence concern) and the
//! primitives REUSE the eg-compute finance kernels, so it depends on both — parallel
//! to where eg-query sits. Imports point LEFT only.
//!
//! Provides:
//!  1. `store::SeriesStore` — a time-partitioned series store over redb composite-key
//!     `(series_id, bucket_start)` chunks, beside the engine's nodes/edges tables
//!     (feature `redb-store`).
//!  2. `query::{time_bucket, asof_join_backward, gap_fill_locf, ohlc_bars, downsample,
//!     decay_weighted_mean}` + finance-kernel reuse — native, NO DataFusion (Pi path).
//!  3. `time_op` — Time as score-producing Ops over the unified RowSet algebra
//!     (sketch + the seam eg-plan binds later).
//!  4. `arrow_seg` — the DataFusion `time_bucket = GROUP BY (ts/w)*w` scan path
//!     (feature `arrow-seg`).
//!
//! The ONE decay curve both the series `decay_weighted_mean` and the engine's
//! semantic-memory confidence decay use lives in `eg_core::decay` (CONCEPT:KG-2.211).

pub mod point;
pub mod query;
pub mod time_op;

#[cfg(feature = "redb-store")]
pub mod store;

#[cfg(feature = "arrow-seg")]
pub mod arrow_seg;
