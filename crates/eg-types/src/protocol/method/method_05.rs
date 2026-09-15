macro_rules! __eg_method_chunk_5 {
    (@acc [$($variants:tt)*]) => {
        __eg_method_chunk_6!(@acc [
$($variants)*


    /// Native visualization render surface (D-VZ-1 lanes V4 "engine integration" /
    /// V6 "graph-native marks"): resolve a caller-provided `eg_viz_core::ViewSpec`
    /// against a dataset (caller-supplied inline columns, or deterministic
    /// engine-side synthetic data) and render it to static PNG/SVG/PDF bytes, or
    /// fetch the mark x surface capability matrix. ONE variant wrapping an
    /// internal op enum — mirrors `AnalyticsJob { op }`/`Statechart { op }` above
    /// — so the whole render surface costs exactly one `Method` arm. Gated `viz`;
    /// the handler (`src/server/handlers/viz.rs`, facade feature
    /// `viz-static-export`) self-routes in `dispatch.rs` before the per-graph
    /// chain — a render is NOT graph-scoped (it resolves a FRESH per-request
    /// `ColumnStore`, never a live graph read), exactly like `AnalyticsJob`/
    /// `Statechart` above. V4-LITE, not full V4: no tile cache, no provenance
    /// inherited from a durable job, no view over a resident `GraphCore` — see
    /// `crate::viz`'s module doc.
    #[cfg(feature = "viz")]
    Viz {
        op: crate::viz::VizOp,
    },


    // ── Query (SQL + Cypher) ──────────────────────────────────────────
    // Read-only relational query surface (CONCEPT:EG-KG.query.read-only-sql-query). `SELECT … FROM
    // nodes …` over ONE graph via DataFusion, gated behind the facade `query`
    // feature; in a slim build the variant falls to the not-built catch-all.
    // `params_msgpack` is reserved for future bound parameters.
    Sql {
        query: String,
        #[cfg_attr(feature = "contract-schema", schemars(with = "Vec<u8>"))]
        #[serde(default, with = "serde_bytes")]
        params_msgpack: Vec<u8>,
    },

    // Read-only Cypher query surface (CONCEPT:EG-KG.query.dep-free-behind). A `MATCH … WHERE … RETURN
    // … LIMIT …` over ONE graph, compiled to the engine's own primitives (the
    // eg-core label index, `vf2_subgraph_match`, and petgraph BFS) — NO DataFusion,
    // so it ships in the lean Pi build behind the facade `cypher` feature. Reuses
    // the same `QueryResult` carrier as `Sql` (returned via `ResultPayload::raw`):
    // a Cypher RETURN is the same columns+row-blobs shape, so no new payload
    // variant. In a build without `cypher` the variant falls to the not-built
    // catch-all.
    CypherQuery {
        query: String,
        /// Exact requested execution authority; no implicit or inferred mode.
        mode: CypherMode,
    },

    // Read-only GraphQL query surface (CONCEPT:EG-KG.query.sparql-completeness). A GraphQL `query`
    // operation whose root fields are node TYPES (label-scan + `first`/`limit` +
    // property-equality args) with nested EDGE selections (relationship traversal),
    // compiled to scans + BFS over the SAME GraphView the Cypher executor reads
    // (eg-graphql — pure-Rust, NO async-graphql/DataFusion). Returns the GraphQL
    // `{"data": …}` JSON via `ResultPayload::raw`. Gated behind the facade `graphql`
    // feature (kept OUT of pi/default — async-graphql-free but still node/cluster/
    // full only); in a build without it the variant falls to the not-built catch-all.
    #[cfg(feature = "graphql")]
    GraphQl {
        query: String,
        /// Optional GraphQL `$variables` — a JSON object bound at execution
        /// (CONCEPT:EG-KG.query.fragments-variables-directives variables, wired through the wire path as an EG-064
        /// follow-up). The handler binds these via `execute_with_variables`
        /// (`@skip`/`@include` + `$var` args). `None` is encoded explicitly and means
        /// an empty binding.
        #[serde(deserialize_with = "deserialize_required_option")]
        variables: Option<serde_json::Value>,
    },


    /// Pull one bounded Arrow `KnowledgeBatch` from any served query family.
    /// `cursor=None` opens a snapshot; passing the returned cursor resumes only
    /// when authority, graph snapshot, query, schema and batch size still match.
    /// This is the sole native result contract and returns bounded Arrow IPC.
    #[cfg(feature = "knowledge-batch")]
    KnowledgeStream {
        request: crate::knowledge_stream::KnowledgeStreamRequest,
    },


    // ── Unified cross-modal query (CONCEPT:AU-KG.compute.vector/209) ──────────────────
    // ONE plan that filters (relational/DataFusion) → traverses (graph/BFS) →
    // ranks (vector/kNN) over the SAME off-lock snapshot, instead of three siloed
    // round-trips. The `plan` is the serializable [`crate::wire::Plan`] AST (an
    // ordered list of `Scan|Filter|Traverse|Rank|Limit` ops over a shared RowSet);
    // the bespoke planner (eg-plan) sequences the existing legs and applies a
    // cost-based filter-vs-vector reorder (CONCEPT:EG-KG.query.concept-14). Read-only this
    // increment. Gated behind the facade `query` feature (the FILTER leg needs
    // DataFusion); in a slim build the variant falls to the not-built catch-all.
    // Result via `ResultPayload::raw` — a list of `[id, score|nil]` rows.
    #[cfg(feature = "query")]
    UnifiedQuery {
        plan: crate::wire::Plan,
    },


    // ── Unified query, TEXT surface — UQL (CONCEPT:AU-KG.query.top-nodes-by-degree) ────────────────
    // The human/agent-writable counterpart of `UnifiedQuery`: a UQL `text` string
    // (e.g. `MATCH (:Doc) WHERE year > 2024 |> TRAVERSE -[:CITES]->{1,2} |> RANK BY
    // ~[…] |> LIMIT 10`) that the handler PARSES (eg_plan::uql::parse) into the SAME
    // `wire::Plan` AST `UnifiedQuery` carries, then runs through the IDENTICAL
    // `run_unified` executor — NO new execution path, just a front-end. A parse error
    // becomes a clear error Response. Same `query`-gating + `ResultPayload::raw`
    // (`[id, score|nil]` rows) as `UnifiedQuery`.
    #[cfg(feature = "query")]
    UnifiedQueryText {
        text: String,
    },


    // ── EXPLAIN surfaces (CONCEPT:EG-KG.query.plan-dag, E5 phase 4) ──────────────────
    // Diagnostics over the SAME `wire::Plan` `UnifiedQuery` carries — no new execution
    // path, just introspection into what the planner did / would do. Read-only.
    /// `EXPLAIN PLAN` — serialize `plan` as a [`crate::wire::Plan`]::PlanDag conversion
    /// (a linear plan is a degenerate chain, CONCEPT:EG-KG.query.plan-dag) both BEFORE and
    /// AFTER the DAG-aware cost optimizer (`eg_plan::optimizer::optimize_dag`), plus the
    /// active rule set (`eg_plan::cost_opt_rule_names()`) — the optimizer rewrite trace.
    /// Returns an `ExplainPlanResult` via `ResultPayload::raw`. Gated `query` (same as
    /// `UnifiedQuery`).
    #[cfg(feature = "query")]
    ExplainPlan {
        plan: crate::wire::Plan,
    },

    /// `EXPLAIN PROVENANCE` — run `plan` and, for each result row, resolve its
    /// EVIDENCE-FOR provenance (the SAME belief-substrate `EvidenceFor` resolution E2's
    /// `Op::EvidenceFor` op runs) over the `KnowledgeSet` (E3) row shape. With the
    /// `epistemic` feature OFF (or absent at runtime) every row's provenance is empty and
    /// `resolved` is `false` — the documented "no epistemic resolution ran" behavior E3's
    /// `KnowledgeSet` already carries (CONCEPT:EG-KG.query.knowledge-set). Returns an
    /// schema-generated `EvidenceBundle` via `ResultPayload::raw`. Gated `query`.
    #[cfg(feature = "query")]
    ExplainProvenance {
        plan: crate::wire::Plan,
    },

    /// `EXPLAIN PROVENANCE BY IDS` (CONCEPT:EG-KB-CURRENCY) — the ID-seeded sibling of
    /// `ExplainProvenance`: skip the `Plan`/`Op` algebra entirely and resolve the SAME
    /// protocol evidence claims directly for `ids` — the
    /// shape a caller that already has a set of node ids from ANY other read path
    /// (a Cypher `MATCH`, a SQL `SELECT`, a prior `UnifiedQuery`) needs to "currency-
    /// upgrade" a plain id list into calibrated, cited, time-versioned rows without
    /// hand-building an `Op` plan first. `ids` is deduplicated, first-occurrence order
    /// preserved (mirrors `RowSet::from_ids`); an id absent from the graph is silently
    /// skipped (never fabricated). Returns an `EvidenceBundle` via
    /// `ResultPayload::raw`, byte-identical in shape to `ExplainProvenance`'s. Gated
    /// `query` (same as `ExplainProvenance`).
    #[cfg(feature = "query")]
    ExplainProvenanceByIds {
        ids: Vec<String>,
    },

    /// `EXPLAIN POLICY` — run `plan` against BOTH the caller's RLS-filtered snapshot and
    /// the UNFILTERED snapshot (reusing the SAME `eg_core::isolation::IsolationLayer`
    /// `filter_view` every read path already applies), reporting which result rows the
    /// policy DENIED. With the `security` feature off (or no caller/RLS configured), no
    /// filtering applies and `policy_denied_ids` is always empty. Returns an
    /// `ExplainPolicyResult` via `ResultPayload::raw`. Gated `query`.
    #[cfg(feature = "query")]
    ExplainPolicy {
        plan: crate::wire::Plan,
    },

    /// `EXPLAIN BELIEF <node_id>` — the FULL, un-flattened E1 justification tree
    /// (`eg_epistemic::JustificationGraph`, via `eg_plan::explain_belief_tree`) rooted at
    /// `node_id` — the standalone verbatim-tree surface E2's plan-`Op::ExplainBelief`
    /// (a flat `RowSet` projection) documents as a follow-up, mirroring
    /// `Method::OwlExplain`'s `ProofNodeWire`. Returns an `ExplainBeliefResult` via
    /// `ResultPayload::raw`. Gated `epistemic` (which implies `query`).
    ///
    /// `disclosure_level` (EPI-P3-4, L51) is `None` by default — the DEFAULT PATH IS
    /// UNCHANGED: the handler runs the classic un-redacted `explain_belief` and returns
    /// `ExplainBeliefResult` exactly as before this field existed. When `Some(_)`, the
    /// caller opts INTO the policy-aware, RLS-redacted proof
    /// (`eg_epistemic::redact::explain_belief_redacted`, feature `epistemic-redaction`
    /// on the facade, which pulls `eg-core/security`) — the handler then returns an
    /// `ExplainBeliefRedactedResult` INSTEAD of `ExplainBeliefResult` in the SAME
    /// `ResultPayload::raw` slot (the caller who set this field knows to decode the
    /// other type). The requested level is a CAP, never a grant: a caller may ask for a
    /// STRICTER view than their own RLS access earns (e.g. always request
    /// `ExistenceOnly` for a privacy-conscious display) but can never loosen what
    /// `explain_belief_redacted` computes from their actual access — see
    /// `eg_epistemic::redact` module docs. If `epistemic-redaction` is OFF at build
    /// time, a request naming `Some(_)` gets an explicit error response (never a silent
    /// fall-back to the un-redacted tree — that would leak exactly what redaction
    /// exists to hide).
    #[cfg(feature = "epistemic")]
    ExplainBelief {
        node_id: String,
        // `skip_serializing_if` matches the client: it omits the key entirely
        // when the caller doesn't pass `disclosure_level` -- without this, the
        // server's own re-serialization (`Method::canonical_body_bytes`, used
        // to recompute the `eg2.` MAC) would emit an explicit `null` the
        // client never hashed, failing every un-redacted `explain_belief`
        // call with "Authentication failed" before it reaches this handler.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        disclosure_level: Option<DisclosureLevelWire>,
    },

    /// The Phase-3 acceptance capstone (EPI-P3-5, L53): "what do we believe, why, on
    /// exactly which evidence, under whose authority, at what time, with what
    /// uncertainty, and what would invalidate it" — for `node_id`, in ONE typed call
    /// (`eg_epistemic::epistemic_status`, feature `epistemic-tms`). Composes belief
    /// (`is_believed`/confidence), the proof tree (`why`), the diagnostic (`why_not`,
    /// populated iff not believed), the counterfactual (`what_evidence_would_change_this`),
    /// and this claim's own bitemporal window — every sibling facet
    /// `eg_epistemic::query` exposes for ONE claim, so those are not separately wired
    /// as their own `Method`s (a caller wanting just one gets it off this result).
    /// Returns an `EpistemicStatusResult` via `ResultPayload::raw`. Gated `epistemic`
    /// at the wire level; the HANDLER additionally requires `epistemic-tms` — a build
    /// with `epistemic` but not `epistemic-tms` falls to the graph_ops "not available
    /// in this build" catch-all (same convention as every other feature-gated arm).
    #[cfg(feature = "epistemic")]
    EpistemicStatus {
        node_id: String,
    },

    /// **what_changed**(tx_from, tx_to) (EPI-P3-5, L53): between two transaction times,
    /// which beliefs changed and why (`eg_epistemic::what_changed`, feature
    /// `epistemic-tms`) — the one acceptance-query facet that is NOT a sub-field of
    /// `EpistemicStatus` (it is a whole-graph temporal DIFF, not a single claim's
    /// status), so it gets its own `Method`. Returns a `WhatChangedResult` via
    /// `ResultPayload::raw`. Same build-tier fallback convention as `EpistemicStatus`.
    #[cfg(feature = "epistemic")]
    WhatChanged {
        tx_from: u64,
        tx_to: u64,
    },

    /// Fenced recompute/writeback for one stale materialization. The expected source
    /// graph version must exactly match the durable reasoning projection watermark.
    /// Replicated serving commits an opaque recompute intent with the authoritative
    /// graph version fence, then the durable outbox worker resolves provenance from
    /// the graph post-image and fsyncs the side projection before acknowledging it.
    /// A late recompute fails with `STALE_RECOMPUTE_FENCE` rather than overwriting a
    /// newer invalidation.
    #[cfg(feature = "epistemic")]
    RecomputeMaterialization {
        derived_id: String,
        expected_source_graph_version: u64,
    },

    /// Seam 3 — query the CURRENT status (`"Fresh"`/`"Stale"`/`"Retracted"`, or
    /// absent if never registered) of a materialization tracked on the SAME
    /// per-graph durable incremental reasoning projection. Read-only — does not
    /// itself recompute anything. Returns a
    /// `MaterializationStatusResult` via `ResultPayload::raw`. Same build-tier
    /// fallback convention as `RecomputeMaterialization`.
    #[cfg(feature = "epistemic")]
    MaterializationStatus {
        id: String,
    },

    /// Seam 3 follow-up (SURPASS gap-closure: "give staleness a consumer") — the bulk
    /// counterpart of [`Method::MaterializationStatus`]: every opaque materialization
    /// reference CURRENTLY `Stale` in this graph's durable projection.
    /// Same build-tier fallback convention as `MaterializationStatus`.
    #[cfg(feature = "epistemic")]
    StaleMaterializations,

    /// EPI-P3-7 (gap-fill) — standalone paraconsistent conflict resolution: run Dung
    /// abstract-argumentation semantics (`eg_epistemic::tms`, feature `epistemic-tms`)
    /// over a `BeliefGraph` built from the caller's `GraphView`, and report — for each
    /// of `node_ids` — whether it SURVIVES, is DEFEATED, or stays UNDECIDED under
    /// `semantics` (`"grounded"` (default) | `"preferred"` | `"stable"`). This is the
    /// SAME grounded/preferred/stable extension machinery `Method::EpistemicStatus`
    /// already composes internally (via `is_skeptically_accepted`) for a single claim's
    /// acceptance — reachable here as a standalone, multi-claim, semantics-selectable
    /// op instead of only inside that capstone. Returns a `ResolveConflictResult`
    /// (surviving/defeated/undecided id lists + the raw extension set(s) the verdict
    /// was computed from) via `ResultPayload::raw`. Gated `epistemic` at the wire
    /// level; the HANDLER additionally requires `epistemic-tms` — a build with
    /// `epistemic` but not `epistemic-tms` falls to the graph_ops "not available in
    /// this build" catch-all (same convention as `EpistemicStatus`/`WhatChanged`).
    #[cfg(feature = "epistemic")]
    ResolveConflict {
        node_ids: Vec<String>,
        #[serde(default = "default_argumentation_semantics")]
        semantics: String,
    },

    /// X-1 (CONCEPT:EG-X1) — resolve `node_id`'s cited multimodal evidence: build a
    /// `BeliefGraph` off the caller's `GraphView` and walk the SAME support/
    /// contradiction/attack topology `ExplainBelief` walks, returning every
    /// transitively-reachable node that carries one complete governed `EvidenceLocus`
    /// (page region, audio/video interval, row version, code range, trace span, …).
    /// The locus itself carries the opaque subject, policy, and derivation references
    /// (`eg_epistemic::evidence_citations`, feature `evidence-graph`) — "here is
    /// exactly where in the source this claim's evidence came from." Returns an
    /// `ExplainEvidenceResult` via `ResultPayload::raw`. Gated `epistemic` at the wire
    /// level (implies `query`); the HANDLER additionally requires `evidence-graph` —
    /// a build with `epistemic` but not `evidence-graph` falls to the graph_ops "not
    /// available in this build" catch-all (same convention as `EpistemicStatus`/
    /// `epistemic-tms`).
    #[cfg(feature = "epistemic")]
    ExplainEvidence {
        node_id: String,
    },

    /// EPI-P3-3 — a request-carried linear-Gaussian structural causal model query:
    /// `variables` defines the DAG's `StructuralEquation`s in topological
    /// (parents-before-children) order — the SAME invariant
    /// `eg_epistemic::CausalGraph::add_variable` enforces at construction. `mode`
    /// (EPI-P3-6) selects which of `eg_epistemic::CausalGraph`'s two
    /// non-counterfactual queries `do_values` feeds:
    ///
    /// * `CausalQueryModeWire::Intervene` — a **do-calculus intervention**
    ///   `P(· | do(X₁=x₁, X₂=x₂, …))`:
    ///   `do_values` fixes the named variables via graph surgery
    ///   (`CausalGraph::intervene`) — incoming edges are CUT, not conditioned on.
    /// * `CausalQueryModeWire::Observe` — the **observational** query
    ///   `P(· | X₁=x₁, X₂=x₂, …)`: ordinary multivariate-Gaussian conditioning on
    ///   the UNMUTILATED joint (`CausalGraph::observe`). Unlike `Intervene`,
    ///   evidence propagates BACKWARD to ancestors too (e.g. a confounder) — the
    ///   mechanism a naive "condition on the evidence" read of a causal question
    ///   gets wrong, and exactly what distinguishes "seeing X=x" from "doing X=x".
    ///
    /// Either way, returns a calibrated `CausalEstimateResult` (mean/variance/
    /// credible-interval per variable, in `variables` order) via
    /// `ResultPayload::raw`. A pure function over request-carried inputs — no graph
    /// snapshot is read. Gated `epistemic` at the wire level; the HANDLER
    /// additionally requires `epistemic-causal` — same build-tier fallback
    /// convention as `ExplainEvidence`.
    ///
    /// The crate's Pearl point-counterfactual (`CausalGraph::counterfactual`) is a
    /// distinct, DETERMINISTIC (not distributional) query with its own request
    /// shape — see `CausalCounterfactual` below, not this variant.
    #[cfg(feature = "epistemic")]
    CausalEstimate {
        variables: Vec<StructuralEquationWire>,
        do_values: std::collections::BTreeMap<String, f64>,
        mode: CausalQueryModeWire,
    },

    /// EPI-P3-6 — Pearl's point-**counterfactual** recipe
    /// (`eg_epistemic::CausalGraph::counterfactual`, feature `epistemic-causal`):
    /// "given that unit `actual` (a FULLY-observed assignment of every variable in
    /// `variables`) really happened, what would its variables have been had
    /// `do_values` held instead?" — the three-step abduction/action/prediction
    /// recipe (Pearl, *Causality*, ch. 7), replaying the SAME inferred exogenous
    /// noise forward through the (surgered) structural equations.
    ///
    /// DETERMINISTIC given `actual` — not a calibrated distribution like
    /// `CausalEstimate` — so it returns a `CausalCounterfactualResult` (one POINT
    /// value per variable, in `variables` order) via `ResultPayload::raw` instead
    /// of a `CausalEstimateResult`. A pure function over request-carried inputs —
    /// no graph snapshot is read. Gated `epistemic` at the wire level; the HANDLER
    /// additionally requires `epistemic-causal` — same build-tier fallback
    /// convention as `CausalEstimate`.
    #[cfg(feature = "epistemic")]
    CausalCounterfactual {
        variables: Vec<StructuralEquationWire>,
        actual: std::collections::BTreeMap<String, f64>,
        do_values: std::collections::BTreeMap<String, f64>,
    },

    /// EPI-P3-3 — provenance-aware retrieval ranking: order request-carried
    /// `candidates` by a weighted blend of similarity AND evidence quality/
    /// provenance (source reliability, corroboration, calibration precision,
    /// freshness) rather than similarity alone (`eg_epistemic::rank`, feature
    /// `epistemic-causal`). A pure function over request-carried inputs — no graph
    /// snapshot is read. Returns a `RankByProvenanceResult` via `ResultPayload::raw`.
    /// Same build-tier fallback convention as `ExplainEvidence`/`CausalEstimate`.
    #[cfg(feature = "epistemic")]
    RankByProvenance {
        candidates: Vec<RetrievalCandidateWire>,
        #[serde(default)]
        weights: RankWeightsWire,
    },


    // ── Natural-language query (CONCEPT:EG-KG.query.core-query-input/EG-080) ─────────────────────
    /// Natural-language → executable query → rows. `text` is the NL request, `graph`
    /// the target graph (the `/nl` HTTP facade path has no request envelope, so the
    /// graph rides the method; over the wire an empty `graph` falls back to the request
    /// envelope's graph). The handler resolves a configured/injected `NlPlanner`, turns
    /// the NL into a UQL query string, and runs it through the IDENTICAL deterministic
    /// `UnifiedQueryText` pipeline (`eg_plan::uql::parse` → the fused executor) — NO new
    /// execution path, and no LLM in the engine core. Result via `ResultPayload::raw` —
    /// the SAME `[id, score|nil]` rows as `UnifiedQuery`.
    ///
    /// UNCONDITIONAL in the enum (like `RbacAdmin`); the HANDLER is gated behind the
    /// facade `nl-query` feature. A build WITHOUT `nl-query` falls to the dispatch "not
    /// available in this build" catch-all — so the wire stays compatible while the NL
    /// surface is a build-tier choice.
    NlQuery {
        text: String,
        #[serde(default)]
        graph: String,
    },


    // ── Query federation / foreign sources (CONCEPT:EG-KG.query.query-federation, Lane P) ───────
    // Register a named EXTERNAL source so a UnifiedQuery `Op::ForeignScan` can read it
    // as a RowSet and compose it with the local graph/vector/SQL ops in ONE plan. The
    // actual cross-engine/HTTP transport lives in eg-plan behind the `federation` gate;
    // this is the registration surface. Gated behind the facade `federation` feature;
    // in a slim/Pi build the variant falls to the not-built catch-all.
    /// Register (or replace) a foreign RowSet source under `name`. `source` is the
    /// [`crate::wire::ForeignSourceSpec`] (a remote engine or an HTTP/JSON API). A
    /// later `ForeignScan` can name this registered source by id (the registry-backed
    /// form) instead of inlining the whole spec. Returns the name on success.
    #[cfg(feature = "federation")]
    RegisterForeignSource {
        name: String,
        source: crate::wire::ForeignSourceSpec,
    },


    // ── WASM-sandboxed UDF / extension model (CONCEPT:EG-KG.query.rowset-execution) ─────────────
    // An agent pushes a custom compute function as a WebAssembly module the engine
    // runs SANDBOXED (wasmtime, fuel + memory limits, NO host capabilities). Gated
    // behind the facade `wasm-udf` feature (wasmtime is heavy); in a slim/Pi build the
    // variants fall to the not-built catch-all.
    /// Register (compile + cache) a WASM UDF under `id`. `wasm` is the module bytes
    /// (the `.wasm` binary). The module must export `memory`/`alloc`/`udf` and import
    /// NOTHING (the empty linker rejects any host import). Replaces a prior UDF of the
    /// same id. Returns the id on success.
    #[cfg(feature = "wasm-udf")]
    RegisterUdf {
        id: String,
        #[cfg_attr(feature = "contract-schema", schemars(with = "Vec<u8>"))]
        #[serde(with = "serde_bytes")]
        wasm: Vec<u8>,
    },

    /// Run a registered WASM UDF `id` over an opaque `input` payload, returning the
    /// UDF's output bytes (`ResultPayload::Raw`). Sandboxed + fuel-limited: an
    /// infinite-loop UDF is KILLED (a trap error), never a hang. The bytes are opaque
    /// to the engine — the caller serializes/deserializes its own row payload.
    #[cfg(feature = "wasm-udf")]
    RunUdf {
        id: String,
        #[cfg_attr(feature = "contract-schema", schemars(with = "Vec<u8>"))]
        #[serde(with = "serde_bytes")]
        input: Vec<u8>,
    },


    // ── Distributed graph compute (CONCEPT:EG-KG.storage.feature) ───────────────────────
    // A Pregel/GAS vertex-centric superstep engine that runs an algorithm ACROSS a
    // SET of graphs spanning multiple Raft groups/shards. Gated behind `compute-dist`
    // (which needs `raft`); in a non-cluster build the variants fall to the not-built
    // catch-all. The single-shard fast path stays the always-on `PageRank` etc.
    /// Run a distributed graph algorithm across `graphs` (each a shard/partition),
    /// with `algo` selecting PageRank / ConnectedComponents / Bfs. Result is
    /// `ResultPayload::Raw` — `[id, score]` rows for PageRank, `[id, label]` for CC/BFS.
    #[cfg(feature = "compute-dist")]
    DistributedCompute {
        graphs: Vec<String>,
        algo: DistAlgo,
    },

    /// Create (or replace) a named, incrementally-maintained MATERIALIZED VIEW of a
    /// distributed-compute result over `graphs`. The view is computed once, persisted,
    /// and refreshed incrementally on a delta (CONCEPT:EG-KG.storage.feature). Returns the row count.
    #[cfg(feature = "compute-dist")]
    CreateMatView {
        name: String,
        graphs: Vec<String>,
        algo: DistAlgo,
    },

    /// Read a materialized view's current rows by name (`ResultPayload::Raw`).
    #[cfg(feature = "compute-dist")]
    GetMatView {
        name: String,
    },

    /// Incrementally refresh a materialized view after the underlying graphs changed —
    /// recomputes only the affected vertices on the delta. Returns the row count.
    #[cfg(feature = "compute-dist")]
    RefreshMatView {
        name: String,
    },


    // ── Plan-backed materialized views (CONCEPT:EG-KG.storage.plan-backed-matview) ───────
    // GENERALIZES the algo-only matview above: a matview is a NAMED, DURABLE `wire::Plan`
    // (the same cross-modal AST `UnifiedQuery` carries) over ONE `graph`. Defining it
    // executes the plan once via the runtime and caches the RESULT in the version-keyed,
    // RLS-aware result cache; a committed write bumps the graph version (and the CDC hub
    // marks the view stale), so the next `Get` recomputes — never serves a stale result.
    // Gated behind the facade `matview` feature (which needs `query` for the `Plan` AST
    // and routes through the `compute-dist` dispatch line); in a build without it these
    // variants fall to the dispatch "not available in this build" catch-all.
    /// Define (or replace) a plan-backed materialized view `name` over `graph`, whose
    /// definition is the cross-modal `plan`. Executes the plan once, caches the result,
    /// and persists the definition durably. Returns the row count of the first
    /// materialization.
    #[cfg(feature = "matview")]
    PlanMatViewDefine {
        name: String,
        graph: String,
        plan: crate::wire::Plan,
    },
        ]);
    };
}

pub(crate) use __eg_method_chunk_5;
