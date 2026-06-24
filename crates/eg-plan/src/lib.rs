//! # eg-plan — the unified cross-modal query planner (CONCEPT:KG-2.208/209)
//!
//! ONE query plan that *filters (relational) → traverses (graph) → ranks (vector)*
//! over the SAME off-lock snapshot in a single execution — instead of three siloed
//! surfaces (DataFusion SQL, petgraph BFS, vector kNN) stitched together by the
//! caller across three round-trips. This is the production increment of the Wave-7
//! feasibility spike (`~/workspace/reports/spike-unified-findings.md`).
//!
//! ## The closed algebra
//!
//! The intermediate that flows between operators is a [`RowSet`] — an ordered id set
//! carrying optional scores. Graph, table and vector results are all reducible to
//! "ids + score", so that is the shared currency, and every operator is
//! `(RowSet) -> RowSet`. A plan is just an ordered `Vec<Op>`
//! ([`algebra::Op`]: `Scan | Filter | Traverse | Rank | Limit`), so any op's output
//! is a legal input to any op — a CLOSED algebra. That is what a SQL+graph+vector
//! unification needs and what a federation-of-three-engines cannot give you (it has
//! to round-trip through three result encodings).
//!
//! ## Reusing the existing legs (NOT a new engine)
//!
//! This is a thin BESPOKE planner SEQUENCING the legs the engine already has — it
//! does NOT reimplement SQL or the graph/vector engines:
//!  * `Filter`   runs **real DataFusion** (`eg_query::exec_sql` over the
//!    schema-on-read `nodes` provider) — DataFusion stays the relational sub-engine.
//!  * `Traverse` is petgraph BFS over the `GraphView` topology (the relationship is
//!    read from the edge property blob, matching eg-query/cypher).
//!  * `Rank`     is the `SemanticStore` vector kNN.
//!
//! ## Cost-based reorder (CONCEPT:KG-2.209)
//!
//! [`cost::CostModel`] orders an adjacent `(Filter, Rank)` pair filter-first vs
//! vector-first by selectivity — the cross-modal reorder a unified planner exists to
//! make. Both orders return the same set; only the work differs.
//!
//! ## Feature gating
//!
//! The DataFusion `Filter` leg — and [`Plan::execute`], which needs it — live behind
//! the `query` cargo feature. A default build links NO DataFusion: the algebra, the
//! cost model, the IR and the dep-free graph/vector legs all compile (the Pi
//! contract). The facade gates this crate's `query` exactly as it gates `eg-query`.
//!
//! ## Explicitly deferred to later increments
//!
//! Per the spike's incremental binding order, these are NOT in this increment:
//!  * **Reason-in-plan** — a datalog fixpoint `Op::Reason` (transitive closure with
//!    iteration bounds / semi-naïve evaluation).
//!  * **Projected columns** — carrying full property rows across the boundary
//!    (projection pushdown) instead of re-materializing from the snapshot.
//!  * **Blob / object source ops** — another leaf source joined by id.
//!  * **Cross-modal write ACID** — running the plan inside one `GraphTxn` and writing
//!    back inferred edges/embeddings atomically.
//!  * **Cross-shard** — per-shard sub-plans + a coordinator merge + traversal
//!    frontier forwarding (the multi-quarter piece).

pub mod algebra;
pub mod cost;

#[cfg(feature = "query")]
pub mod exec;
#[cfg(feature = "query")]
pub mod oracle;

pub use algebra::{Op, Plan, Pred};
pub use cost::{CostModel, Order, Stats};
pub use rowset::RowSet;

pub mod rowset;

#[cfg(feature = "query")]
pub use exec::{execute, PlanCtx, PlanExt};

#[cfg(all(test, feature = "query"))]
mod fixture;
#[cfg(all(test, feature = "query"))]
mod tests;
