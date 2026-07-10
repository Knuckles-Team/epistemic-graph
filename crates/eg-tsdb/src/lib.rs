//! eg-tsdb — native time-series store + TS query primitives for the epistemic-graph
//! engine (CONCEPT:AU-KG.retrieval.god-nodes-communities store + primitives, CONCEPT:EG-KG.compute.handled-outside-single-anchor decay unification +
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
//! semantic-memory confidence decay use lives in `eg_core::decay` (CONCEPT:EG-KG.compute.handled-outside-single-anchor).

// CONCEPT:EG-KG.temporal.native-columnar-store — the native columnar (struct-of-arrays) segment for analytical
// scans. Pure-Rust + serde only (no Arrow/DataFusion/redb), so it compiles in the
// lean / Pi build exactly like `point`/`query` — always on, no feature gate.
pub mod columnar;
pub mod point;

// CONCEPT:EG-KG.ingest.vrl-ingest-transform — VRL-style ingest transform pipelines (OpenObserve/Vector-style
// parse_json / filter / set / rename / coerce / route stages applied to a log/event
// record BEFORE it lands in a series), plus a cross-modal `enrich` hook that resolves
// a field from a caller-injected graph lookup (the "surpass OpenObserve" angle).
// Pure-Rust + serde only (a tiny hand-rolled JSON reader — no serde_json), so it
// compiles in the lean / Pi build exactly like `point`/`columnar` — always on.
pub mod pipeline;

pub mod query;
pub mod time_op;

// CONCEPT:EG-KG.query.multi-rate-sensor-stream — multi-rate sensor-stream alignment for multimodal fusion: resample N
// different-rate sensor series onto ONE target grid with per-channel Nearest/Linear/
// AsofHold interpolation (AsofHold reuses `query::asof_join_backward`). Pure-Rust (no
// redb/Arrow), always on — the time half of fusion; `eg-tensor::fusion` stacks the
// aligned frame into a tensor.
pub mod fusion;

#[cfg(feature = "promql")]
pub mod promql;

// CONCEPT:EG-OS.observability.trace-assembly — distributed span model + indexed span store + trace assembly +
// service-dependency graph. Dependency-free (no redb/Arrow), so it links wherever the
// facade's `traces` sub-feature is on. The HTTP glue lives in `src/server/traces`.
#[cfg(feature = "traces")]
pub mod traces;

#[cfg(feature = "redb-store")]
pub mod store;

#[cfg(feature = "arrow-seg")]
pub mod arrow_seg;

// ModalityContract retrofit (CONCEPT:E4/X1): `impl ModalityContract for
// store::SeriesMeta` AND `impl ModalityContract for traces::Span`, behind the
// crate's own opt-in `contract` feature (default OFF, implying `redb-store` since
// `SeriesMeta` lives there AND `traces` since `Span` lives there). See module docs.
#[cfg(feature = "contract")]
mod contract;
