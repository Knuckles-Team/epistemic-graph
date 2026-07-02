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

/// The UQL text front-end (CONCEPT:KG-2.214) — `uql::parse(text) -> wire::Plan`. It
/// is a pure parser (lexer + recursive descent, NO DataFusion), so it ships in a
/// default/Pi build alongside the algebra/cost/IR; only EXECUTION of the resulting
/// Plan is `query`-gated.
pub mod uql;

#[cfg(feature = "query")]
pub mod exec;
// The natural-language → query planning seam (CONCEPT:EG-078) + the concrete standalone
// `UreqNlPlanner` (CONCEPT:EG-080). The `NlPlanner` trait + the `plan_and_execute*`
// helpers are pure (only touch the existing `uql::parse` + `execute`), so they ride the
// `nl-query` feature which IMPLIES `query`; the concrete `UreqNlPlanner` inside is
// additionally gated on `nl-query` (it pulls the shared `ureq` rustls client). Kept OUT
// of pi.
#[cfg(feature = "nl-query")]
pub mod nl;
// The federation foreign-source seam (CONCEPT:KG-2.232) — the `ForeignSource` trait +
// the remote-engine / HTTP-JSON kinds backing `Op::ForeignScan`. Implies `query`.
#[cfg(feature = "federation")]
pub mod federation;
#[cfg(feature = "query")]
pub mod oracle;

pub use algebra::{Op, Plan, Pred};
pub use cost::{CostModel, Order, Stats};
pub use rowset::RowSet;

pub mod rowset;

#[cfg(feature = "query")]
pub use exec::{execute, PlanCtx, PlanExt};

// The NL→query seam surface (CONCEPT:EG-078/EG-080): the trait + the LLM-optional
// `Option<&dyn NlPlanner>` entry point, and the concrete `UreqNlPlanner`.
#[cfg(feature = "nl-query")]
pub use nl::{plan_and_execute, plan_and_execute_opt, NlPlanner, UreqNlPlanner};

// Re-export the federation surface so a caller naming a foreign source goes through
// eg-plan: the trait + the spec-dispatcher.
#[cfg(feature = "federation")]
pub use federation::{source_for, ForeignSource};

// Re-export the lexical surface (CONCEPT:KG-2.215) so a caller wiring a text plan
// names them through eg-plan: the BM25 index, the hit row, and the RRF helper.
#[cfg(feature = "text")]
pub use eg_text::{rrf_fuse, TextHit, TextIndex, RRF_K};

#[cfg(all(test, feature = "query"))]
mod fixture;
#[cfg(all(test, feature = "query"))]
mod tests;

// The lexical BM25 `RankText` + RRF `FuseRrf` hybrid proofs (CONCEPT:KG-2.215).
#[cfg(all(test, feature = "text"))]
mod text_tests;

// The OWL `Reason` + SPARQL `SparqlBgp` source-op compose-oracle proofs
// (CONCEPT:KG-2.220).
#[cfg(all(test, feature = "owl"))]
mod owl_tests;

// The federation foreign-scan + compose-join-with-local proofs (CONCEPT:KG-2.232):
// a mock HTTP/JSON source joined with the local graph in ONE plan == the manual join.
#[cfg(all(test, feature = "federation"))]
mod federation_tests;

// The external-SQL federation proofs (CONCEPT:KG-2.239): an external relational-SQL
// source (`ForeignSourceSpec::Sql`) joined with the local graph in ONE plan == the
// manual join, plus the real sqlx DSN path errors cleanly when unreachable.
#[cfg(all(test, feature = "federation-sql"))]
mod federation_sql_tests;

// The spatial `SpatialScan` + `Spatial{Within,DWithin}` executor proofs
// (CONCEPT:EG-083): a bbox R-tree scan + geometry filters compose with the graph/
// vector legs in ONE plan.
#[cfg(all(test, feature = "geo"))]
mod geo_tests;

// The document/JSON `Pred::JsonPath` executor proofs (CONCEPT:EG-084): deep JSONPath
// existence / `->>`-equality / `@>`-containment filters apply per-row against the stored
// JSON and compose with the graph/vector legs in ONE plan.
#[cfg(all(test, feature = "query"))]
mod docjson_tests;

// The array/tensor `TensorScan` + `TensorOp` executor proofs (CONCEPT:EG-085): a
// layer scan seeds tensor-bearing nodes, then slice/reduce/elementwise ops apply
// per-row and compose with the graph/vector legs in ONE plan.
#[cfg(all(test, feature = "tensor"))]
mod tensor_tests;

// The event-stream / CEP `Op::Cep` executor proofs (CONCEPT:EG-088): a layer scan
// seeds a time-ordered event stream, then the bounded NFA (sequence/within/absence over
// sliding/tumbling windows) narrows it — cross-modal graph→stream in ONE plan.
#[cfg(all(test, feature = "stream"))]
mod stream_tests;

// The multimodal sensor-fusion `Op::SensorFuse` executor proofs (CONCEPT:EG-098): three
// heterogeneous sensor layers (scalar + tensor-frame blob) at different rates are
// resolved off the snapshot, time-aligned to a common clock via eg-tsdb's ASOF-backed
// `sensor_fuse`, and emitted as fused rows that compose with the graph/vector legs.
#[cfg(all(test, feature = "timeseries"))]
mod sensor_fuse_tests;

// The probabilistic `Op::Probabilistic` executor proofs (CONCEPT:EG-086): a layer scan
// seeds distribution-bearing nodes, then a closed-form probabilistic query (expectation /
// marginal / conditional posterior / seeded sample) scores + ranks each row's stored
// `Distribution`, composing uncertainty with the graph/vector legs in ONE plan.
#[cfg(all(test, feature = "probabilistic"))]
mod probabilistic_tests;
