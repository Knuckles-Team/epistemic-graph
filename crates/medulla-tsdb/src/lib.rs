//! medulla-tsdb — native time-series store + TS query primitives for the medulla
//! engine (CONCEPT:AU-KG.retrieval.god-nodes-communities store + primitives, CONCEPT:MEDULLA-KG.compute.handled-outside-single-anchor decay unification +
//! Time-ops sketch).
//!
//! Sits ABOVE medulla-compute in the DAG (`medulla-types → medulla-core → medulla-compute → medulla-tsdb →
//! medulla`): the store needs redb (an medulla-core/persistence concern) and the
//! primitives REUSE the medulla-compute finance kernels, so it depends on both — parallel
//! to where medulla-query sits. Imports point LEFT only.
//!
//! Provides:
//!  1. `store::SeriesStore` — a time-partitioned series store over redb composite-key
//!     `(series_id, bucket_start)` chunks, beside the engine's nodes/edges tables
//!     (feature `redb-store`).
//!  2. `query::{time_bucket, asof_join_backward, gap_fill_locf, ohlc_bars, downsample,
//!     decay_weighted_mean}` + finance-kernel reuse — native, NO DataFusion (Pi path).
//!  3. `time_op` — Time as score-producing Ops over the unified RowSet algebra
//!     (sketch + the seam medulla-plan binds later).
//!  4. `arrow_seg` — the DataFusion `time_bucket = GROUP BY (ts/w)*w` scan path
//!     (feature `arrow-seg`).
//!
//! The ONE decay curve both the series `decay_weighted_mean` and the engine's
//! semantic-memory confidence decay use lives in `medulla_core::decay` (CONCEPT:MEDULLA-KG.compute.handled-outside-single-anchor).

// CONCEPT:MEDULLA-KG.temporal.native-columnar-store — the native columnar (struct-of-arrays) segment for analytical
// scans. Pure-Rust + serde only (no Arrow/DataFusion/redb), so it compiles in the
// lean / Pi build exactly like `point`/`query` — always on, no feature gate.
pub mod bars;
pub mod columnar;
pub mod point;

// CONCEPT:MEDULLA-KG.ingest.vrl-ingest-transform — VRL-style ingest transform pipelines (OpenObserve/Vector-style
// parse_json / filter / set / rename / coerce / route stages applied to a log/event
// record BEFORE it lands in a series), plus a cross-modal `enrich` hook that resolves
// a field from a caller-injected graph lookup (the "surpass OpenObserve" angle).
// Pure-Rust + serde only (a tiny hand-rolled JSON reader — no serde_json), so it
// compiles in the lean / Pi build exactly like `point`/`columnar` — always on.
pub mod pipeline;

pub mod query;
pub mod time_op;

// CONCEPT:MEDULLA-KG.query.multi-rate-sensor-stream — multi-rate sensor-stream alignment for multimodal fusion: resample N
// different-rate sensor series onto ONE target grid with per-channel Nearest/Linear/
// AsofHold interpolation (AsofHold reuses `query::asof_join_backward`). Pure-Rust (no
// redb/Arrow), always on — the time half of fusion; `medulla-tensor::fusion` stacks the
// aligned frame into a tensor, and `medulla-plan`'s `Op::SensorAlign` executor arm is what
// drives the pair from a wire plan.
pub mod fusion;

// CONCEPT:MEDULLA-KG.temporal depth (per-modality) — CUSUM + PELT-lite change-point
// detection over a stored series, plus a root-cause join correlating a change
// point to a graph-linked deploy/incident event (reuses `query::asof_join_backward`
// rather than a second nearest-neighbour implementation). Pure-Rust + no new deps
// (prefix-sum DP + a control-chart accumulator), so it compiles in the lean / Pi
// build exactly like `point`/`query` — always on, no feature gate.
pub mod changepoint;

#[cfg(feature = "promql")]
pub mod promql;
// EH-522 / EH-524 — derived series: expressions compiled to checkpointed kernel trees.
#[cfg(feature = "derive")]
pub mod derive;

// CONCEPT:MEDULLA-OS.observability.trace-assembly — distributed span model + indexed span store + trace assembly +
// service-dependency graph. Dependency-free (no redb/Arrow), so it links wherever the
// facade's `traces` sub-feature is on. The HTTP glue lives in `src/server/traces`.
#[cfg(feature = "traces")]
pub mod traces;

#[cfg(feature = "redb-store")]
pub mod store;

// Development/test composition root for the store's scope grants (see the module
// doc). Never linked by a production build.
#[cfg(all(feature = "redb-store", any(test, feature = "dev-scope-grant")))]
pub mod dev_scope_grant;

#[cfg(feature = "arrow-seg")]
pub mod arrow_seg;

// ModalityContract retrofit (CONCEPT:E4/X1): `impl ModalityContract for
// store::SeriesMeta` AND `impl ModalityContract for traces::Span`, behind the
// crate's own opt-in `contract` feature (default OFF, implying `redb-store` since
// `SeriesMeta` lives there AND `traces` since `Span` lives there). See module docs.
#[cfg(feature = "contract")]
mod contract;
