//! # eg-plan — the unified cross-modal query planner (CONCEPT:AU-KG.compute.vector/209)
//!
//! ONE query plan that *filters (relational) → traverses (graph) → ranks (vector)*
//! over the SAME off-lock snapshot in a single execution — instead of three siloed
//! surfaces (DataFusion SQL, petgraph BFS, vector kNN) stitched together by the
//! caller across three round-trips. This is the production increment of the Wave-7
//! feasibility spike (`repo://reports/spike-unified-findings.md`).
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
//! ## Cost-based reorder (CONCEPT:EG-KG.query.concept-14)
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

/// Structured hierarchical retrieval (CONCEPT:EG-KG.retrieval.bounded-drill) — the LeanRAG method as a
/// library API over the EG-220 summary tier: retrieve at the summary/abstraction
/// level, then drill down `SUMMARIZES`/`CONSOLIDATES` provenance to the supporting
/// leaves, assembling a de-duplicated multi-level context that beats flat top-k on
/// coverage. Pure-Rust + dep-free (Pi-tier clean): it reads the graph and ANN index
/// through the small [`leanrag::GraphTopology`] / [`leanrag::AnnIndex`] accessor
/// traits, so it links no DataFusion and edits no eg-core. It is a retrieval helper
/// a caller composes ABOVE the planner, NOT a wire `Op`.
pub mod leanrag;

/// The UQL text front-end (CONCEPT:AU-KG.query.top-nodes-by-degree) — `uql::parse(text) -> wire::Plan`. It
/// is a pure parser (lexer + recursive descent, NO DataFusion), so it ships in a
/// default/Pi build alongside the algebra/cost/IR; only EXECUTION of the resulting
/// Plan is `query`-gated.
pub mod uql;

/// The typed DAG plan representation (CONCEPT:EG-KG.query.plan-dag, E5 phase 1): [`dag::PlanDag`]
/// generalizes the linear [`Plan`] into a real graph of operators (a node's `inputs` name
/// its dependency nodes), with a lossless conversion from every existing linear `Plan` (a
/// degenerate chain). `Op` is unchanged; execution over a `PlanDag` lives in
/// [`dag_exec`]. Sits beside `exec`/`knowledge` under the same `query` gate (a `PlanNode`
/// carries an optional `knowledge::ProjectionSchema`).
#[cfg(feature = "query")]
pub mod dag;
/// Physical execution of a [`dag::PlanDag`] (CONCEPT:EG-KG.query.plan-dag-exec, E5 phase 2) —
/// [`dag_exec::execute_dag`] runs ALONGSIDE the untouched [`exec::execute`]; for a
/// degenerate linear-chain dag it is byte-identical to it (the differential oracle proves
/// this over the whole fixture suite).
#[cfg(feature = "query")]
pub mod dag_exec;
#[cfg(feature = "query")]
pub mod exec;
/// RowSet v2, additive (CONCEPT:EG-KG.query.knowledge-set): [`knowledge::KnowledgeSet`] is the
/// enriched shape (kind/confidence/bitemporal window/column-projection/provenance+policy
/// frames) a caller builds from a FINISHED `RowSet` + the `GraphView` it ran over — a
/// terminal step above the op loop, exactly like `leanrag`. `RowSet` and `Op::execute`
/// are unchanged; this module is only exercised when a caller explicitly asks for it.
/// Sits under the existing `query` feature (no new cargo feature) because it borrows
/// `GraphView::node_row_object`'s decode.
#[cfg(feature = "query")]
pub mod knowledge;
/// The Arrow-columnar `KnowledgeBatch` (CONCEPT:EG-P1-2, P1 roadmap feedback) — a
/// RecordBatch-backed columnar projection of `KnowledgeSet`: entity/artifact id,
/// N named score columns, calibrated confidence, typed evidence-ref columns,
/// bitemporal valid/tx-time, provenance/policy-label list columns, lazy
/// blob-handle columns and native bounded result streams. Behind the
/// `knowledge-batch` feature (implies `query` and is enabled by facade `full`) so a
/// default/Pi build still links no Arrow. Family adapters cover graph, SQL, RDF,
/// vector, time-series, jobs, and cross-modal results.
#[cfg(feature = "knowledge-batch")]
pub mod knowledge_batch;
#[cfg(feature = "knowledge-batch")]
pub mod result_stream;
/// The physical EXECUTION runtime (CONCEPT:EG-KG.query.parallel-runtime) — Lane B. The
/// `execute_ops` driver-dispatch `execute` routes through + the rayon-morsel, memory-accounted,
/// spilling `ParallelDriver` (feature `par-runtime`). Gated on `query` (it schedules the
/// `query`-tier physical fns); a default/Pi build dispatches to the Lane-0 `SerialDriver`.
#[cfg(feature = "query")]
pub mod runtime;
// The natural-language → query planning seam (CONCEPT:EG-KG.query.core-query-input) + the concrete standalone
// `UreqNlPlanner` (CONCEPT:EG-KG.query.fence-stripper). The `NlPlanner` trait + the `plan_and_execute*`
// helpers are pure (only touch the existing `uql::parse` + `execute`), so they ride the
// `nl-query` feature which IMPLIES `query`; the concrete `UreqNlPlanner` inside is
// additionally gated on `nl-query` (it pulls the shared `ureq` rustls client). Kept OUT
// of pi.
#[cfg(feature = "nl-query")]
pub mod nl;
// The federation foreign-source seam (CONCEPT:EG-KG.query.query-federation) — the `ForeignSource` trait +
// the remote-engine / HTTP-JSON kinds backing `Op::ForeignScan`. Implies `query`.
#[cfg(feature = "federation")]
pub mod federation;
/// The cross-modal cost-based optimizer (CONCEPT:EG-KG.query.xmodal-cost-optimizer) — Lane A's
/// rule engine over the logical `Vec<Op>` that [`exec::plan_optimize`] calls to reorder
/// operators across modalities into a cheaper-but-equivalent plan. Compiled under `query`;
/// runtime kill-switch `EPISTEMIC_GRAPH_COST_OPT=0` → identity passthrough.
#[cfg(feature = "query")]
pub mod optimizer;
#[cfg(feature = "query")]
pub mod oracle;

pub use algebra::{Op, Plan, Pred};

// The hierarchical-retrieval surface (CONCEPT:EG-KG.retrieval.bounded-drill): the retriever + its accessor
// traits + the structured result + the flat-top-k baseline, so a caller names them
// through eg-plan.
pub use cost::{CostModel, Order, Stats};

/// The Lane-0 shared cost traits A's optimizer + B's runtime both read
/// (CONCEPT:EG-KG.query.cost-cardinality-traits): the per-op [`cost::Cardinality`] estimator
/// and the [`cost::CostEstimate`] triple. Gated on `query` (the estimator borrows `PlanCtx`);
/// this tier only defines the shape — no estimator is wired, so behavior is unchanged.
#[cfg(feature = "query")]
pub use cost::{Cardinality, CostEstimate};
pub use leanrag::{
    flat_top_k, AnnIndex, GraphTopology, HierResult, HierarchicalRetriever, RetrievalParams, Scored,
};
pub use rowset::RowSet;

pub mod rowset;

/// The in-txn tsdb read-your-own-writes staged-series overlay (CONCEPT:EG-KG.query.txn-tsdb-read-your).
#[cfg(feature = "timeseries")]
pub use exec::StagedSeries;
#[cfg(feature = "query")]
pub use exec::{execute, PlanCtx, PlanExt};

/// The typed DAG plan surface (CONCEPT:EG-KG.query.plan-dag, E5): the node/edge shape plus
/// its physical executor, so a caller building a genuine multi-branch plan names them
/// through `eg_plan` directly (mirroring how `Op`/`Plan` are re-exported from `algebra`).
#[cfg(feature = "query")]
pub use dag::{NodeId, PlanDag, PlanNode};
#[cfg(feature = "query")]
pub use dag_exec::{execute_dag, execute_dag_with};

/// The RowSet v2 surface (CONCEPT:EG-KG.query.knowledge-set): the enriched, ready-to-consume
/// row/set shape a caller builds from a finished `RowSet` + the `GraphView` it ran over.
#[cfg(feature = "query")]
pub use knowledge::{
    KnowledgeRow, KnowledgeSet, PayloadRef, PolicyFrame, ProjectionSchema, ProvenanceFrame,
};

/// The Arrow-columnar `KnowledgeBatch` surface (CONCEPT:EG-P1-2): the RecordBatch-
/// backed columnar projection of a `KnowledgeSet` and its row type.
#[cfg(feature = "knowledge-batch")]
pub use knowledge_batch::{KnowledgeBatch, KnowledgeBatchRow};
#[cfg(feature = "knowledge-batch")]
pub use result_stream::{
    cross_modal_result_stream, graph_result_stream, job_result_stream, rdf_result_stream,
    sql_result_stream, time_series_result_stream, validate_native_batch, vector_result_stream,
    KnowledgeBatchEnvelope, KnowledgeBatchStream, KnowledgeRowResult, KnowledgeStreamContext,
    KnowledgeStreamCursor, KnowledgeStreamError, ServedResultFamily,
};

/// The Lane-0 foundation seams the follow-on lanes hang off (append-only): the logical-plan
/// optimization hook `plan_optimize` (CONCEPT:EG-KG.query.plan-optimize-seam — Lane A's cost
/// optimizer fills its body) and the physical `Driver` execution trait + its `SerialDriver`
/// impl (CONCEPT:EG-KG.query.exec-driver-seam — Lane B swaps in a parallel driver). Both are
/// identity/serial in this tier, so `execute` is behavior-identical.
#[cfg(feature = "query")]
pub use exec::{plan_optimize, Driver, SerialDriver};

/// The Lane B physical runtime surface (CONCEPT:EG-KG.query.parallel-runtime): the
/// `execute_ops` driver-dispatch `execute` routes through (always, on `query`), plus — only
/// on the opt-in `par-runtime` feature — the rayon-morsel `ParallelDriver` and its
/// `RuntimeConfig` (pool width / spill budget, from `EPISTEMIC_GRAPH_EXEC_THREADS` /
/// `_SPILL_MB`). A default build has neither; `execute_ops` dispatches to `SerialDriver`,
/// byte-for-byte the prior fold.
#[cfg(feature = "query")]
pub use runtime::execute_ops;
#[cfg(feature = "par-runtime")]
pub use runtime::{ParallelDriver, RuntimeConfig};

/// Lane A's cross-modal optimizer surface (CONCEPT:EG-KG.query.xmodal-cost-optimizer): the
/// `optimize(plan, ctx) -> Plan` rule-engine entry point `plan_optimize` drives, its
/// `enabled()` runtime kill-switch, and the plan-time cardinality/cost estimators the rules
/// read (`ModalityCardinality` + the snapshot-memoized `PlanStats` catalog).
#[cfg(feature = "query")]
pub use cost::{ModalityCardinality, PlanStats};
/// The DAG-aware optimizer (CONCEPT:EG-KG.query.dag-optimizer, E5 phase 3) — [`optimizer::optimize_dag`]
/// reorders ops within each maximal linear chain segment of a [`dag::PlanDag`] through the
/// SAME rule engine `optimize` drives, never crossing a branch/fan-out boundary (the DAG
/// generalization of the EG-405 adjacency guard).
#[cfg(feature = "query")]
pub use optimizer::optimize_dag;
#[cfg(feature = "query")]
pub use optimizer::{enabled as cost_opt_enabled, optimize, rule_names as cost_opt_rule_names};
/// The adaptive re-optimization hook (CONCEPT:EG-KG.query.adaptive-reoptimization): re-cost and,
/// if warranted, re-order the not-yet-executed tail of a plan once an earlier op's ACTUAL
/// output cardinality diverges from what plan-time estimation predicted — the runtime feedback
/// loop beyond the static, once-per-`optimize()` cost decision. See
/// [`optimizer::reoptimize_remaining`] for the divergence threshold and today's caller-driven
/// wiring (a future `Driver` can call it automatically between ops).
#[cfg(feature = "query")]
pub use optimizer::{reoptimize_remaining, ADAPTIVE_REOPT_THRESHOLD};

/// The server-side text→vector embedder seam (CONCEPT:EG-KG.compute.no-embedder-bound-op): the `TextEmbedder` trait
/// backing the UQL `RANK BY ~ "text"` (`Op::RankEmbed`) NL→vector resolver, plus the
/// deterministic `HashEmbedder` fallback for tests/offline use. A facade binds a concrete
/// model onto a `PlanCtx` via `with_embedder`.
#[cfg(feature = "query")]
pub use exec::{HashEmbedder, TextEmbedder};

/// The full, un-flattened E1 justification tree behind `EXPLAIN BELIEF` (E5 phase 4,
/// CONCEPT:EG-KG.epistemic.epistemic-substrate) — mirrors `Op::ExplainBelief`'s RowSet
/// projection but returns the verbatim `eg_epistemic::JustificationGraph` a standalone
/// `Method::ExplainBelief` wire-projects (mirroring `Method::OwlExplain`'s `ProofNodeWire`).
#[cfg(feature = "epistemic")]
pub use exec::explain_belief_tree;

// The NL→query seam surface (CONCEPT:EG-KG.query.core-query-input/EG-080): the trait + the LLM-optional
// `Option<&dyn NlPlanner>` entry point, and the concrete `UreqNlPlanner`.
#[cfg(feature = "nl-query")]
pub use nl::{
    plan_and_execute, plan_and_execute_opt, NlPlanner, OAuth2ClientCredentials, TokenAuthStyle,
    UreqNlPlanner,
};

// Re-export the federation surface so a caller naming a foreign source goes through
// eg-plan: the trait + the spec-dispatcher.
#[cfg(feature = "federation")]
pub use federation::{source_for, ForeignSource};

// The federation NAME-RESOLUTION seam (CONCEPT:EG-KG.query.closure-backed-source): the by-name registry + the
// registerable owned source kinds (spec-backed / table / closure) + the shared handle.
// These complete `Op::Foreign` / a `Named` `Op::ForeignScan` — a name → live source →
// rows resolution the executor consults via `PlanCtx::with_foreign`.
#[cfg(feature = "federation")]
pub use federation::{
    ClosureSource, ForeignSourceRegistry, SharedForeignSource, SpecSource, TableSource,
};

// Re-export the lexical surface (CONCEPT:AU-KG.query.text-spatial-time) so a caller wiring a text plan
// names them through eg-plan: the BM25 index, the hit row, and the RRF helper.
#[cfg(feature = "text")]
pub use eg_text::{rrf_fuse, TextHit, TextIndex, RRF_K};
// The `PlanCtx::with_text` search-surface trait (CONCEPT:EG-KG.query.served-text-index-binding) — a facade
// implements this over its OWN maintained persistent index (rather than a plain
// `eg_text::TextIndex`) to push a served `RankText`/`FuseRrf` leg down into it.
#[cfg(feature = "text")]
pub use exec::TextSource;
// The `PlanCtx::with_spatial` search-surface trait (CONCEPT:EG-KG.storage.incremental-spatial, L37) — mirrors
// `TextSource`: a facade implements this over its OWN maintained persistent spatial
// index to push a served `Op::SpatialScan` down into it.
#[cfg(feature = "geo")]
pub use exec::SpatialSource;

#[cfg(all(test, feature = "query"))]
mod fixture;
#[cfg(all(test, feature = "query"))]
mod tests;

// The server-side embedder seam proofs (CONCEPT:EG-KG.compute.no-embedder-bound-op/412): `RANK BY ~ "text"`
// (`Op::RankEmbed`) resolves the text to a query vector via the embedder bound on the
// `PlanCtx` and ranks identically to the literal-vector oracle; an unbound embedder is a
// clean typed error; the `HashEmbedder` fallback is deterministic. Gated on `query`.
#[cfg(all(test, feature = "query"))]
mod embedder_tests;

// The lexical BM25 `RankText` + RRF `FuseRrf` hybrid proofs (CONCEPT:AU-KG.query.text-spatial-time).
#[cfg(all(test, feature = "text"))]
mod text_tests;

// The OWL `Reason` + SPARQL `SparqlBgp` source-op compose-oracle proofs
// (CONCEPT:EG-KG.ontology.concept-12).
#[cfg(all(test, feature = "owl"))]
mod owl_tests;

// The federation foreign-scan + compose-join-with-local proofs (CONCEPT:EG-KG.query.query-federation):
// a mock HTTP/JSON source joined with the local graph in ONE plan == the manual join.
#[cfg(all(test, feature = "federation"))]
mod federation_tests;

// The federation NAME-REGISTRY proofs (CONCEPT:EG-KG.query.closure-backed-source): a registered source resolves by
// name (`Named` `Op::ForeignScan` + `Op::Foreign`), composes with a local Join, an
// unbound name errors cleanly, and an empty/absent registry keeps the prior behavior.
#[cfg(all(test, feature = "federation"))]
mod federation_registry_tests;

// The external-SQL federation proofs (CONCEPT:EG-KG.query.feature): an external relational-SQL
// source (`ForeignSourceSpec::Sql`) joined with the local graph in ONE plan == the
// manual join, plus the real sqlx DSN path errors cleanly when unreachable.
#[cfg(all(test, feature = "federation-sql"))]
mod federation_sql_tests;

// The spatial `SpatialScan` + `Spatial{Within,DWithin}` executor proofs
// (CONCEPT:EG-KG.ontology.singles-concept): a bbox R-tree scan + geometry filters compose with the graph/
// vector legs in ONE plan.
#[cfg(all(test, feature = "geo"))]
mod geo_tests;

// The document/JSON `Pred::JsonPath` executor proofs (CONCEPT:EG-KG.compute.json-deep-indexing): deep JSONPath
// existence / `->>`-equality / `@>`-containment filters apply per-row against the stored
// JSON and compose with the graph/vector legs in ONE plan.
#[cfg(all(test, feature = "query"))]
mod docjson_tests;

// The array/tensor `TensorScan` + `TensorOp` executor proofs (CONCEPT:EG-KG.storage.content-addressed-dedup): a
// layer scan seeds tensor-bearing nodes, then slice/reduce/elementwise ops apply
// per-row and compose with the graph/vector legs in ONE plan.
#[cfg(all(test, feature = "tensor"))]
mod tensor_tests;

// The event-stream / CEP `Op::Cep` executor proofs (CONCEPT:EG-KG.query.pipelined-execution): a layer scan
// seeds a time-ordered event stream, then the bounded NFA (sequence/within/absence over
// sliding/tumbling windows) narrows it — cross-modal graph→stream in ONE plan.
#[cfg(all(test, feature = "stream"))]
mod stream_tests;

// The multimodal sensor-fusion `Op::SensorFuse` executor proofs (CONCEPT:EG-KG.query.multi-rate-sensor-stream): three
// heterogeneous sensor layers (scalar + tensor-frame blob) at different rates are
// resolved off the snapshot, time-aligned to a common clock via eg-tsdb's ASOF-backed
// `sensor_fuse`, and emitted as fused rows that compose with the graph/vector legs.
#[cfg(all(test, feature = "timeseries"))]
mod sensor_fuse_tests;

// Cross-modal SEAM regression proofs (CONCEPT:EG-KG.ingest.timeseries-source-mid-pipeline): mid-pipeline composition of a
// semantic (`Reason`), sensor (`SensorFuse`) or tensor (`TensorScan`) SOURCE op feeding
// a downstream `Traverse`/`Rank`/reorder in ONE plan == the hand-wired oracle. Gated at
// the module level on `query`; each proof is additionally `cfg`-gated on its modality
// feature (`owl`/`timeseries`/`tensor`), so the module compiles under any `query` subset.
#[cfg(all(test, feature = "query"))]
mod cross_modal_seam_tests;

// Vector ⇄ reasoning cross-txn write→read consistency (CONCEPT:EG-KG.compute.concept-6): a freshly
// written node (embedding + reasoner-subsumed type) is BOTH ANN-ranked AND
// reasoner-included by the next `[Reason |> Rank]` plan; an embedding update is reflected
// by the next ANN pass. Needs the OWL reasoner + the vector ranker, so it is gated on `owl`.
#[cfg(all(test, feature = "owl"))]
mod vector_reasoning_tests;

// The probabilistic `Op::Probabilistic` executor proofs (CONCEPT:EG-KG.compute.uncertainty-values): a layer scan
// seeds distribution-bearing nodes, then a closed-form probabilistic query (expectation /
// marginal / conditional posterior / seeded sample) scores + ranks each row's stored
// `Distribution`, composing uncertainty with the graph/vector legs in ONE plan.
#[cfg(all(test, feature = "probabilistic"))]
mod probabilistic_tests;

// The epistemic belief/evidence executor proofs (CONCEPT:EG-KG.epistemic.epistemic-substrate,
// E2): `EvidenceFor`/`Contradicts`/`SupportedBy` seed-or-filter by classified edge kind,
// `ConfidenceOp`/`SourceReliability` re-score by propagated belief, `BeliefAsOf` composes the
// bitemporal filter with propagation (diverging from the pure-alias `VALID AS OF` on a
// bitemporal fixture), and `ExplainBelief` flattens the justification tree — composing with
// the graph/vector legs in ONE plan (`[Scan, EvidenceFor, Rank]`).
#[cfg(all(test, feature = "epistemic"))]
mod epistemic_tests;

// Lane C proofs (CONCEPT:EG-KG.query.native-time-series): mid-pipeline OWL `Op::Reason` (a confidence-preserving
// FILTER, not only a leaf source — `Rank → Reason → Traverse`) + the native eg-tsdb
// `Op::TsScan` SOURCE (tsdb-in-plan fusion), over a hand-built `PlanCtx`. Compiles when
// EITHER feature is on; each test is additionally gated on the one it needs.
#[cfg(all(test, any(feature = "owl", feature = "timeseries")))]
mod tsdb_scan_tests;

// The ADVANCED cross-modal seam proofs (CONCEPT:EG-KG.compute.asof-valid-t-sources..EG-389): the richest 3+-modality
// fused plans over a hand-built `PlanCtx` — bitemporal reason→vector→traverse (owl),
// federation fusion + fail-closed named source (federation), geo×vector×temporal (geo),
// tensor×graph×vector + CAS dedup (tensor), CEP×graph×tsdb window (stream+timeseries), and
// probabilistic×owl×vector + MMR (owl+probabilistic). Module-gated on `query`; each proof
// is additionally `cfg`-gated on the modality feature(s) it needs.
#[cfg(all(test, feature = "query"))]
mod advanced_crossmodal_tests;
