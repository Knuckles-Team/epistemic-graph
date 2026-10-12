# Implementation plan

## Sequence

1. **Freeze the baseline.** Record the exact public default-branch SHA, method registry and feature profiles. Record source kinds, index generations and existing tests. Document compatible code and missing or incompatible implementations. Record acceptance evidence separately. Historical delivery claims do not establish current acceptance.
2. **Close the read contract.** Make UQL grammar, lowering, printer, method registry, generated Python sender, proof and error codes agree. Add `LET`/`JOIN`, named score channels and `EXPLAIN`/`PROFILE` only where they can execute; otherwise return `UQL_UNSUPPORTED` with a stable span. Preserve the shared read snapshot and typed parameter path.
3. **Qualify federation phase 1.** Run full-fetch versus optimized oracles for SQL, HTTP, OBDA and registered foreign sources. Repair any unsound pushdown or missing residual. Validate read-only rendering and single outbound guard. Attach budget/trace to the served plan.
4. **Complete phase 2.** Add column-carrying rows, direct OBDA solutions, per-source concurrency, SPARQL `SERVICE` bind joins, Iceberg manifest pruning and RemoteEngine pushdown. Keep each capability off until its conformance oracle passes. The attached-source owner supplies registry identity and approved mappings.
5. **Complete phase 3.** Add persisted source statistics with provenance, freshness/owner-bound fragment caching and additional source kinds. Cache only when the source watermark is known and current. Retire alternate path-specific guards and source catalogs once callers use the common route.
6. **Close semantic reads.** Finish generation fencing, restart/replay, filtered text/vector/edge retrieval and mixed traversal top-k. Run corpus reasoning and proof verification, including disjointness and skipped-event rebuild. Expose index fallback in explain output.
7. **Close numerical surfaces.** Implement exact/golden series, attribution, propagation, barrier and motif kernels behind one public syntax and shared compute code. Add deterministic seed and budget handling before enabling a new operator on the served wire.
8. **Publish evidence.** For each stable ID, record these in the native spec evidence. Record the merged public SHA, exact test profile, positive and negative results, and quality gate results. Record client and generator versions. Require exact-head gates and affected downstream client compatibility checks before claiming acceptance.

9. **Ontology source selection.** Add a read method that takes question classes and returns the class path and per-class approved mappings with premises. Reuse the reasoner for subclass entailment and the unified-data-plane mapping approval state. Add operation-bound source kinds to the foreign source registry under the shared outbound guard. (EG-FEDERATED-QUERY-R072, EG-FEDERATED-QUERY-R073)

## Incremental delivery slices

| Slice | Depends on | Exit artifact |
|---|---|---|
| UQL/served contract | Existing `eg-plan`, method registry | Canonical grammar + generated sender + served positive/negative tests |
| Federation common core | Owner-scoped foreign catalog | Recorded remote requests, exact residual, typed budget/SSRF refusals |
| OBDA and SERVICE | Approved mappings and source identity | Direct solution and bind-join differential fixtures |
| Search and reasoning | Fixed GraphSchema and index authority | Restart/replay, prefilter/top-k, corpus consistency and proof fixtures |
| Query numerics | `eg-numeric`, `eg-tsdb`, `eg-compute` | Shared kernel goldens, deterministic CI/seed and bounded execution |

## Decisions and risk controls

- Favor narrow extensions to existing `eg-plan`, `eg-rdf`, `eg-query`, `eg-numeric` and server paths. No second query planner, registry, schema authority, index lifecycle or statistical kernel.
- Keep optimizer fallback reachable for differential testing and incident mitigation. Never bypass authorization or an outbound refusal.
- Treat SQL without explicit ordering as an unordered relation; tests compare multisets. With explicit ordering, compare ordered rows and top-N exactly.
- Treat source-feature claims as untrusted until probes or conformance establish them; a failed probe lowers capability.
- A missing watermark disables foreign-result cache. A failed semantic generation activation leaves the prior valid generation or uses a correct fallback. It does not return silently stale rows.
- Keep optional source kinds independently gated so a missing driver or live service does not block pure parser, planner, and mock tests for contributors.

The [architecture](plan.md) defines interfaces and ownership. The [test specification](test-spec.md) is the release contract, and [tasks](tasks.md) maps stable work IDs to reviewable deliveries.

## Architecture

### Existing components to reuse

| Layer | Existing owner and path | Required extension |
|---|---|---|
| UQL syntax and semantic lowering | `crates/eg-plan/src/uql/{grammar,lexer,parser,serve,print}.rs` | Add syntax at the grammar table, parser, canonical printer and execution map together. Retain one text method. |
| Query execution and optimizer | `crates/eg-plan/src/{exec,optimizer,federation}.rs`, `federation_opt/` | Build one remote-fragment planner and exact residual, not one optimizer per wire. |
| Foreign security | `crates/eg-plan/src/federation_ssrf.rs`, server owner-scoped foreign catalog | Resolve the registered grant before planning; DNS-pin and validate every outbound request. |
| SQL and Iceberg | `crates/eg-query/src/sql/{mod,iceberg_federation}.rs` | Use DataFusion provider pushdown and shared SQL renderer; do not construct an independent SQL authority. |
| RDF/OBDA | `crates/eg-rdf/src/{sparql,obda,owl}.rs`, `src/server/handlers/rdf/` | Lower approved virtual graphs and `SERVICE` through the same grants, budgets and trace. |
| Search and indexes | `crates/eg-query/src/tables/{ann_authority,index}.rs`, `crates/eg-core/src/compute/semantic_ann_*` | Keep generation authority, fences and maintenance under existing index lifecycle. |
| Numerical kernels | `crates/eg-numeric/`, `crates/eg-tsdb/`, `crates/eg-compute/` | Reuse deterministic statistical kernels from every query surface. |
| Public wire | `src/server/handlers/query/`, `epistemic_graph/generated/query.py` | Generate contracts from Rust ownership; add served conformance, no hand-edited generated client. |

### Data flow

```text
authenticated request + owner scope + snapshot
  -> parse / typed bind / capability gate
  -> canonical plan + authorization-filtered source resolution
  -> optimizer: local residual + permitted remote fragments
  -> guarded source request / native index and reasoning generations
  -> exact residual + budget meter + proof/score channels
  -> typed rows + EXPLAIN/PROFILE + stable error code
```

No optimizer step may widen the set of sources, columns, rows visible to the caller, or outbound destinations. Remote fragments receive only keys from the caller's already authorized candidate set. Proof labels, source statistics and cached rows carry tenant, owner, generation and visibility identity. A grant revocation or schema generation change invalidates affected plans and fragments.

### Query and federation contract

`SourceCapabilities` declares projection, filter precision (`Exact`, `Inexact`, `Unsupported`), order, limit, aggregates, same-source join, batched key lookup, paging, full-fetch allowance, rate and statistics. The planner may rely only on verified capabilities from the registered source, never a caller assertion. `RemoteRequest` carries canonical keys, projection, predicate, order, limit and page. `FederationSession` records request/row/key/wall budgets and a sanitized `FederationTrace` for every attempt and fallback. A remote result may be a superset when a local exact residual remains; it may never omit a required row. `FEDERATION_BUDGET_EXCEEDED:<dimension>` and `FEDERATION_SOURCE_REQUIRES_KEYS` are terminal typed refusals, not partial success.

For SQL, render identifiers/literals once through the shared renderer and revalidate the complete read-only statement. A pushed limit must respect `ORDER BY`, existing `LIMIT`, and dialect semantics. For HTTP, placeholders are allowed only in query values. Percent-encode them, validate the fixed authority at every page, and continue until short page, declared limit or budget. For SPARQL `SERVICE`, send bounded `VALUES` batches and retain exact local filters. For OBDA, derive filters per predicate-object mapping. A condition valid for one triple pattern must not filter a different pattern from the same row. For Iceberg, manifest pruning must be measured after DataFusion has supplied filters and projection.

### Semantic, reasoning and numeric contract

Search indexes bind stable node or parallel-edge IDs and a generation. Tenant/purpose/RLS predicates constrain the candidate set before BM25 or ANN top-k. Backfill activates atomically only after a durable, verified generation; on restart or blocked generation, planner chooses a correct scan or typed unavailable result. No stale index result is presented as fresh. Fixed schema generations compose EL/RL and tableau under explicit step/work budgets; inconsistency names unsatisfiable classes, and a skipped reasoning projection event requires rebuild. `WITH PROOF` exposes derivation premises and source/generation IDs for verifiable results.

Time-series operators preserve `series@timestamp` identity and f64 channel values. Implement rolling statistics with O(1) update plus an exact constant-window result. Use a re-anchor strategy and compare to a recomputed oracle. Attribution gives exact Shapley only through 16 factors, otherwise seeded sampling with confidence interval. Impact propagation is exact on a DAG and bounded on a cycle, or seeded Monte Carlo with a CI. Motif/discord algorithms consume query budgets and expose partial *progress* only when their result type explicitly says so. Probability tails call one `eg-numeric` CDF/survival implementation.

### Interface to the data-plane owner

#### EG-FEDERATED-QUERY-R045 / EG-FEDERATED-QUERY-R052 implementation seam

Extend `crates/eg-plan/src/federation.rs`, `federation_opt/` and the existing `ForeignSource` capability/result contract so `ForeignRows` carries columns through planning and execution. Keep exact residual evaluation in the shared optimizer, then project final requested columns after residual, order and proof needs are satisfied. Use the registered `RemoteEngineSource::fetch_uql` path and `Method::Uql` contract for pushed remote fragments. Resolve signed context and owner grant in the server foreign catalog before the request; do not create a second remote client, parser or source registry. Generated Python and wire shapes change only from Rust contract sources.

The local `unified-data-plane` spec supplies `AttachedSourceIdentity`, owner grants, dialect capability evidence, source watermark, and approved named virtual-graph mapping. This spec consumes those values via the existing foreign catalog and planner. Mapping proposal approval, catalog extraction, change capture, dialect certification and native/accelerated route policy stay there. Query tests must cover an attached-source implementation without depending on a private deployment.

### Compatibility and observability

Keep generated Rust/Python/wire method contracts synchronized. A grammar or error-code change requires canonical parser fixtures and generated client parity. Preserve read-only snapshot behavior; a new modality cannot bypass `Method::Uql` authorization or mutate state. Explain/profiling use registered source label, kind and short opaque fingerprint, never URL, DSN, credential or raw untrusted label. Metrics count budget refusals, source failures, fallback paths, remote bytes/rows, index-generation fallback, and reasoning rebuilds without high-cardinality secrets.

## Audit record

### 2026-10-11 — EG-FEDERATED-QUERY-R056 numerical tail acceptance

At main `6bf4ea1c7e80a375e7053e677b0ed28c162131ec`, the complete production
`crates/eg-compute/src/mining/causal_impact.rs` module and exact production
`eg-numeric` error, math, reduce, validate and kernels modules compiled in a
standalone Rust 1.96.0 test harness linked to the cached real `libm` dependency.
All five causal-impact tests passed. The acceptance test compares two-sided
probabilities at both signs of z = 3, 6, 8, 10 and 20. It uses checked-in
mpmath 1.3.0 50-digit references at relative tolerance below 1e-12. It also covers
zero and positive-infinity boundaries. The production path calls the shared
`normal_sf`; no copied approximation, substitute kernel or mock was used.

The literal test binding records this pure-module leaf only. The standalone
harness does not establish the full eg-compute crate, server, client, cross-host
or hosted release gates. No broader acceptance is claimed.

## Amendments

### 2026-10-11 — EG-FEDERATED-QUERY-R058.3.1 source-guard repair

Scope the no-cache guard to production NL handlers, excluding the adjacent
transaction handler and the test module's own forbidden-reference fixtures.
Missing source boundaries fail closed. Negative controls inject `ResultCache`,
a direct `served_response` call, and a generic `served_response` call into
the production NL section. Preserve the uncached response-path assertion.
Standalone exact-source Rust tests establish this structural guard only; they
do not establish full facade compilation or end-to-end NL query behavior.

## Recovered requirement: vectorized SQL columns

Data flow: SQL row write → `ColumnVectorBinding` check → queued embed → ANN index → nearest-neighbour query.
Attach point: `crates/eg-query/src/sql/ann.rs` and `ann_pushdown.rs`, which already own the ANN path.
Reuses the existing ANN index and embedding call. Source data stays in the SQL table.
Decision: opt-in binding per column. Rejected: embedding every text column, which spends embedder budget on unused columns.

## EG-FEDERATED-QUERY-R078 tenant predicate inside the plan

Data flow: served UQL statement → `dispatch_query_family` (`src/server/handlers/query/dispatch.rs:66`) → `handle_uql` (`src/server/handlers/query/uql_statement.rs:17`) → `inject_scope` → scan nodes → scoped rows.
Types: `PlanScope{tenant, clearance_labels, verified}` in `crates/eg-query/src/plan_scope.rs`. The predicate set is closed: tenant equality and label subset.
Attach point: after planning, before execution, so no scan runs unscoped. Production caller is `dispatch_query_family`.
Failure: an unverified caller gets `PLAN_SCOPE_UNVERIFIED` and no read runs.
Reuses the scope check pattern in `src/server/semantic_index.rs:145`. New: the plan-level predicate.
Decision: scope inside the plan. Rejected: filtering result rows after retrieval, which leaks through counts, timing and ranking.
Cross-repository: `AU-SEMANTIC` rows pass the verified tenant through the request context; owner prefix `AU-SEMANTIC`, contract `VerifiedRequestContext`.

References
- No paper is cited.

## EG-FEDERATED-QUERY-R079 Iceberg v2 manifest publication

Candidate EG:2 requires REST catalog schema evolution, time travel, and both reading and writing Iceberg v2 manifests. R079 defines compatibility; R080 defines write authority. The graph remains the sole authoritative write boundary. External analytic engines consume its projections.

Data flow: served authoritative graph mutation -> committed graph version and source LSN -> `LakeManager::drain_series` (`src/server/lake/mod.rs:602`) -> `materialize_batch` (`:537`) -> `prepare_materialization` (`src/server/lake/catalog_ops.rs:104`) -> `LakeTable::iceberg_manifests` (`crates/eg-lake/src/lib.rs:299`) -> `build_iceberg_manifests` (`crates/eg-lake/src/iceberg_avro.rs:371`) -> durable engine artifact store. Read flow: committed catalog metadata -> authenticated REST load/time-travel -> external reader.

Types: extend `SchemaChangeKind` and introduce `ManifestPublication` in `crates/eg-types/src/lake_catalog.rs`. Bind tenant, graph version and source LSN to schema and snapshot IDs. Include format version, artifact locations and digests. Schema changes preserve field IDs and retained schema history.

Existing code writes Avro manifests and stages Parquet, manifests, manifest lists and metadata in `prepare_materialization`. `publish_artifacts` publishes artifacts; the table builder supports add-column evolution and retained snapshots. Reuse these owners. Complete graph-version binding, drop/rename/reorder handling and independent-reader acceptance remain unverified. The current REST CommitTable handler compacts; its existence is not permission for external schema or data writes. No complete served graph-to-manifest compatibility proof is established here.

Failure: missing authority/version binding, unsupported format features and incomplete publication must leave the prior visible snapshot intact. Destructive schema changes require authoritative approval. R080 refuses all projection data and metadata mutation before publication. Acceptance must exercise append, compaction, copy-on-write deletion, schema changes and retained snapshots with a pinned independent reader. The engine's own decoder alone is insufficient.

Decision: engine-owned serialization of committed graph state writes the projection artifacts. A client cannot bypass graph authority by submitting an Iceberg commit. This replaces the earlier contradictory REST schema-write scenario while retaining the schema-evolution obligation.

Cross-repository: `EG-UNIFIED-DATA-PLANE-R039` owns catalog authentication, tenant isolation and partition metadata. `EG-UNIFIED-DATA-PLANE-R012` and `SDK-GOVERNED-WRITEBACK-R002` through `R004` govern effects on externally owned sources: credentials, approval, idempotency, audit reservation and adapter execution remain SDK responsibilities. This requirement creates no direct external-source write or unmanaged export path. Engine artifact publication is not source writeback.

References
- Existing engine catalog, manifest writer and governed-writeback contracts are the implementation anchors. Candidate EG:2 has no paper citation.

## EG-FEDERATED-QUERY-R080 read-only table projections

Data flow: graph write → graph version → projection materialization (`crates/eg-core/src/projection_catalog.rs:75`) → lake table → Iceberg REST read; a projected write → `refuse_projection_write` → `PROJECTION_READ_ONLY`.
Types: `ProjectionAccess{ReadOnly, Authoritative}` on every `TableEntry` (`crates/eg-lake/src/catalog.rs:27`).
Attach point: the listener dispatch in `src/server/lake/rest/mod.rs`, before `commit_table` and `drop_table`. No production caller refuses a projection write today.
Failure: a projected write returns `PROJECTION_READ_ONLY` and the graph is unchanged.
Existing code: `crates/eg-lake/src/catalog.rs:27`; `crates/eg-core/src/projection_catalog.rs:75`; `crates/eg-plan/src/federation.rs:933`; `src/server/lake/rest/catalog.rs:318`, `:349`. Reachable today: SQL write refusal yes; projection write refusal no.
Reuses the shared read-only SQL check pattern. New: the typed access value and the route refusal.
Decision: one refusal at the listener. Rejected: per-route checks, which a new route may skip.
Cross-repository: R079 may write engine-owned manifest artifacts from committed graph state. External source effects retain the SDK-governed boundary described above.

References
- No paper is cited.

## EG-FEDERATED-QUERY-R081 Retrieval sufficiency verdict with repair step, fallback policy and round limit

Data flow: served cross-modal query -> `eg_plan::execute` (`crates/eg-plan/src/exec.rs:536`) -> `fuse_rrf` (`crates/eg-plan/src/exec.rs:848`) -> `judge_sufficiency` -> `SufficiencyVerdict` with repair and fallback -> harness applies the next round.
Types: `SufficiencyVerdict`, `SufficiencyRule`, `RepairStep`, `FallbackPolicy`. The call carries `round` and `round_limit`; the harness owns the loop counter and EG owns the limit it is given.
Attach point: `execute` at `crates/eg-plan/src/exec.rs:536`. Production caller: `crates/eg-graphql/src/crossmodal.rs:429`.
Failure: A missing round limit returns `RETRIEVAL_ROUND_LIMIT_REQUIRED`. A strict insufficient verdict abstains.
Consumers: AU-RETRIEVAL (R004)
Moves from agent-utilities: executable_rag.py:87 (sufficiency rule), executable_rag.py:198 (repair choice), hybrid_retriever.py:1957 (fallback policy)
Existing code: `crates/eg-plan/src/exec.rs:536`; `crates/eg-plan/src/exec.rs:848`; `crates/eg-plan/src/exec.rs:1436` (diversified ranking); `crates/eg-plan/src/runtime.rs:61`; `crates/eg-compute/src/assemble/conclude.rs:69` (typed abstention pattern). Reachable today: no.
Existing-code search: no sufficiency check and no fallback policy exist in the engine. Search terms: `sufficien`, `fallback_policy`, `FallbackPolicy`, `extractive`, `round_limit`, `max_rounds`.
Reuses: the ranked `RowSet` from `fuse_rrf` and the typed abstention pattern. New: the verdict type, the rule, the repair order.
Decision: EG decides sufficiency, repair and fallback per call and hands them to the harness. Rejected alternative: keep the rule in the harness, because each harness must otherwise carry its own threshold.
Cross-repository: `AU-RETRIEVAL-R004` consumes the verdict and applies the repair. `EG-DECISION-ENGINE-R100` receives the premise. The decision engine spec is edited by another lane; this rollup adds no row there.

References
- No paper is cited.

## EG-FEDERATED-QUERY-R082 Iterative retrieval stop rule with calibrated confidence and bounded termination

Data flow: served cross-modal query with round history -> `eg_plan::execute` (`crates/eg-plan/src/exec.rs:536`) -> `decide_stop` -> `StopVerdict` with reason -> harness stops or runs the next round.
Types: `CalibrationTable`, `RoundCalibration`, `StopContext`, `StopVerdict`, `StopReason`. Context carries validated progress and budgets. Verdict carries a terminal or continuation reason separately from a missing-calibration refusal. Margin and threshold are absent without calibration.
Attach point: `execute` at `crates/eg-plan/src/exec.rs:536`, beside the sufficiency judgement. Production caller: `crates/eg-graphql/src/crossmodal.rs:429`.
Failure: A round outside the table retains `ROUND_NOT_CALIBRATED`. It may continue only while progress and all limits permit. Exhaustion and validated progress termination take precedence without claiming calibrated confidence or enough evidence.
Consumers: AU-RETRIEVAL (R005)
Moves from agent-utilities: adaptive_stopping.py:151 (stop rule), adaptive_stopping.py:80 (calibration table)
Existing code: `crates/eg-plan/src/exec.rs:536`; `crates/eg-compute/src/finance/market/confidence.rs:113` (calibrated confidence with abstention, a separate finance rule). Reachable today: no.
Existing-code search: no stop rule exists in the engine. Search terms: `calibrat`, `confidence_margin`, `stop_reason`, `StopReason`, `round_limit`. The only `stop_reason` hit is the job lease cycle in `src/server/handlers/jobs/executor_claim.rs:82`, which is unrelated.
Reuses: the round history sent by the caller. New: the table, the rule, the verdict.
Decision: One fixed rule over a calibration table with no learned part. Rejected alternative: a learned stopping classifier, because it needs labelled data and drifts without notice.
Cross-repository: `AU-RETRIEVAL-R005` consumes the verdict and ends the loop. `EG-FEDERATED-QUERY-R081` supplies the round limit.

References
- arXiv:2606.13814 — Kieback et al., "TASR: Training-Free Adaptive Stopping for Iterative Retrieval" (2026)

## EG-FEDERATED-QUERY-R083 Arbitration between model prior and evidence

Data flow: harness prior probe -> served cross-modal query carrying the prior `Candidate` -> `eg_plan::execute` (`crates/eg-plan/src/exec.rs:536`) -> `arbitrate` -> `ArbitrationVerdict` -> harness answers or reports the abstention.
Types: `Candidate`, `EvidenceClass`, `ArbitrationVerdict`, `ArbitrationChoice`. The harness runs the probe; EG decides.
Attach point: `execute` at `crates/eg-plan/src/exec.rs:536`. Production caller: `crates/eg-graphql/src/crossmodal.rs:429`.
Failure: A gap below the margin returns `ARBITRATION_ABSTAIN` with both candidates. A candidate without a class returns `CANDIDATE_CLASS_REQUIRED`.
Consumers: AU-RETRIEVAL (R006)
Moves from agent-utilities: shortcut_risks.py:42, agent_control_contracts.py:73 (prior probe and evidence classes)
Existing code: `crates/eg-plan/src/exec.rs:536`; `crates/eg-compute/src/assemble/conclude.rs:69` (typed abstention pattern). Reachable today: no.
Existing-code search: no arbitration exists in the engine. Search terms: `arbitrat`, `abstain`, `margin`, `parametric`, `prior_probe`.
Reuses: the typed abstention pattern and the ranked evidence side. New: the candidate type, the margin rule, the verdict.
Decision: One margin rule in the engine over two candidates. Rejected alternative: a learned arbiter, because it needs labelled disagreement data.
Cross-repository: `AU-RETRIEVAL-R006` runs the prior probe and applies the verdict. `EG-FEDERATED-QUERY-R081` and `R082` share the premise record.

References
- arXiv:2606.08397 — Xu et al., "TrustMargin: Training-Free Arbitration between Parametric Memory and Retrieved Evidence in Large Language Models" (2026)

## R081.7 through R081.9: retrieval synthesis

The existing R081 verdict owns these extensions. EG keeps score selection, query reformulation, feedback classification and hierarchy decisions. Applications execute bounded engine decisions and read evidence. They do not gain a parallel planner or reranker. Existing R078 tenant predicates, R081 fallback and R082 stopping rules remain binding.

Current production path: `crates/eg-graphql/src/crossmodal.rs:429` invokes `eg_plan::execute` in `crates/eg-plan/src/exec.rs:536`. `fuse_rrf` at line 848 combines ranks. It does not carry paired cross-encoder scores, iterative expansion feedback or hierarchy-stage receipts. Add the missing operators and typed `PlanCtx` scoring/summary bindings at these owners. The served acceptance tests must exercise these bindings, not inject a finished result after execution.

Selection policy is versioned and benchmark-gated. Normalize scores with declared finite-domain rules; constant-score inputs require an explicit policy rather than division by zero. Define `f = w*s + (1-w)*r`, calibrated thresholds and stable ID ties. Dual-high passes; dual-low fails. The low-embedding/high-relevance region is eligible for rescue before the maximum-count cap. The remaining region uses its declared fusion threshold. Rank eligible chunks by fused score and cap the result without padding. Selection adds no inference; existing candidate scoring is measured separately. A failed activation receipt leaves the prior approved policy active or refuses the requested policy.

Feedback starts from the original information need. EG generates diverse perspectives, measures returned relevance and updates bounded term weights. R082 validates feedback first and checks the round limit and other budgets next. It then checks no new evidence, declared saturation and calibrated confidence, in that order. Policy revisions declare exact saturation thresholds. Natural-language critique is recorded advice; it cannot overwrite scores, tenant predicates or stop limits. Generation and assessment calls consume explicit budgets even though pure cardinality selection adds none.

Hierarchical retrieval has three explicit stages: community-summary filtering, community refinement and entity search. Each stage records scoring, pruning, visited IDs and cost. Descent uses pinned summaries and graph revisions. Reject invalid hierarchy links and stop before spending beyond the declared bounds. No flat-search substitution counts as completion of the hierarchy contract.

Activation acceptance pins corpus, labels, model artifacts, policy and baseline. The proposed dynamic-count policy stays inactive until its benchmark passes declared quality/cost thresholds. Instrument actual model invocations and compare the same scored candidates. Do not copy a paper's reported quality or timing into EG evidence.

### Source-to-requirement mapping

| Inputs | Owner extension | Preserved residual clauses | Primary research |
|---|---|---|---|
| AU:72, AU:73 | R081.7 | Adaptive count, score fusion, reranker rescue, dual-low rejection and no added selection inference. | [arXiv:2606.14269v1](https://arxiv.org/abs/2606.14269v1) |
| AU:74, AU:75 | R081.8 | Corpus-grounded term feedback, diverse perspectives and reinforce/missing/suppress tiers. | [arXiv:2606.13905v1](https://arxiv.org/abs/2606.13905v1), [arXiv:2606.17209v1](https://arxiv.org/abs/2606.17209v1) |
| AU:76 | R081.9 | Community-to-entity descent, stage reranking and breadth/cost control. | [arXiv:2601.11144v3](https://arxiv.org/abs/2601.11144v3) |
| AU:127 | R081.8 | Traceable natural-language critique can guide another retrieval step without replacing measured relevance. | [arXiv:2606.00590v1](https://arxiv.org/abs/2606.00590v1) |

Primary research informs the mechanisms; EG-specific policies and acceptance remain requirements, not measured results. The source of diverse query initialization has verified authors Murali et al.; the earlier generic author label is not authoritative. The critique source includes additional training mechanisms outside this assigned residual scope.

### R081.8 and R082 termination precedence

Invalid feedback returns `RETRIEVAL_FEEDBACK_INVALID` before further work. A round at its limit returns terminal `round_limit`; other exhausted limits return terminal `budget_exhausted`. Both carry `RETRIEVAL_BUDGET_EXCEEDED` and the exhausted dimension. If two or more other limits are exhausted, choose the primary dimension in this order: query, term, token, inference. Record all exhausted dimensions. Check limits before any retrieval, generation or assessment dispatch.

Only valid completed-round evidence can establish progress termination. No new evidence means zero newly accepted document IDs relative to validated history at the same tenant and corpus revision. Saturation means measured quality improvement meets the policy's declared threshold and observation window. Missing or invalid progress evidence refuses as invalid feedback; a critique assertion is never proof. No-new-evidence takes precedence over saturation. Neither means the answer has enough evidence.

A missing calibration row always remains visible as `ROUND_NOT_CALIBRATED`, including on a terminal verdict. It disables only the calibrated confidence branch. A valid progressing round continues within remaining limits with `continue_uncalibrated`. Saturated, empty-progress or exhausted rounds stop with their specific reason. Margin and threshold stay absent; the calibration refusal remains. The R081 sufficiency verdict remains independent.

## Retrieval policy extensions: R039, R040, R077 and R081.9

These definitions extend existing retrieval owners. The acceptance scenarios in this amendment have not been executed.
EG owns context, constraints, retrieval choices, encoding policy, scoring and evidence.
The SDK owns ingestion execution and provider transport.
The harness executes bounded engine decisions and acceptance journeys.
The host composes authenticated service access.
No consumer gains a second planner.

Dependencies include [EG-TYPED-PACKS-R043 and R048](../typed-packs-and-catalog/requirements.md), for modality annotations and model facts.
Ingestion uses [SDK-SOURCE-INGEST](https://github.com/Knuckles-Team/agent-connector-sdk/tree/main/specs/SDK-SOURCE-INGEST).
Harness evaluation uses [AU-RETRIEVAL-R001](https://github.com/Knuckles-Team/agent-utilities/tree/main/specs/retrieval-evaluation).
Hosting uses [host-composition-boundary](https://github.com/Knuckles-Team/graph-os/tree/main/specs/host-composition-boundary).
These are dependencies, not delivery claims against those owners.

Common served path: `Method::UnifiedQuery` → `src/server/handlers/query/dispatch.rs:133` → `src/server/handlers/query/sql_read.rs:305` → `src/server/handlers/query/planning.rs:854`.
Binding continues through `src/server/handlers/query/planning.rs:424` → `src/server/handlers/query/planning.rs:411` → `crates/eg-plan/src/exec.rs:536`.
The GraphQL caller at `crates/eg-graphql/src/crossmodal.rs:429` must consume equivalent typed policies when enabled.
New typed operators below are proposed additions to the structured plan contract.
They require generated contract and client updates during implementation.
They do not require new textual grammar.

Policy lookup must bind verified tenant, graph, corpus revision and policy revision before reading candidates.
Constraint filters and R078 tenant predicates must execute before ranking, fusion and every truncation.
Receipts must preserve authorized evidence only.
The R081.8/R082 precedence and independent sufficiency verdict remain unchanged.
New ranking components cannot reinterpret uncalibrated continuation, progress termination or exhausted budgets.

### R039.1: shared multimodal retrieval

Existing code: `crates/eg-types/src/embedding.rs:47` defines EmbeddingSpaceRef.
`crates/eg-plan/src/exec.rs:278` defines the text-only TextEmbedder.
`src/server/handlers/query/planning.rs:11` currently binds only an optional deterministic hash embedder.
`crates/eg-plan/src/exec/vector_rank.rs:65` and `crates/eg-plan/src/exec/vector_rank.rs:97` rank authorized candidate vectors.
Reachable today: no, for the complete four-modality contract.
The generic rank path is reachable; the hash embedder proves no semantic or cross-modal quality.

Proposed `RankModality` must carry a typed query input, target modality set, requested count and policy reference.
Text inputs carry bounded text; other inputs carry authorized content references and media metadata.
The modality contract must resolve approved encoders into one pinned EmbeddingSpaceRef.
Each binding must declare model revision, preprocessing digest, output width, normalization and supported input kinds.
Different modality encoders may share a coordinate space only through that explicit compatibility contract.
The engine must compare the full validated tuple, not trust an opaque digest or equal width alone.

The compatible_space_only default must score every supported query-to-target direction by the declared shared metric.
Target items without vectors must contribute authorized missing counts.
Shortfall must carry requested count, returned count, missing embeddings, eligible count, corpus exhaustion and exhausted budget dimensions.
A shortfall may report multiple causes; it must never count invisible items or fabricate zero vectors.
Missing query bindings must refuse before candidate scoring.
Evidence must distinguish actual inference from supplied vectors.

Decision: reuse the stamped vector store and extend its binding contract.
Reject separate indexes whose coordinates merely have equal dimensions.
Acceptance must cover four same-modality and twelve cross-modality pairs with real approved model artifacts.

### R040.1 and R039.3: declared hard constraints

Existing code: `crates/eg-types/src/wire_query_core.rs:27` defines Pred; `crates/eg-plan/src/algebra.rs:21` re-exports it.
`crates/eg-plan/src/exec.rs:536` invokes optimization and execution.
`crates/eg-plan/src/exec/vector_rank.rs:97` accepts a constrained candidate set.
Reachable today: no, for compilation of the declared entity/relationship contract.
Generic predicates and filtered ranking exist.

RetrievalConstraints must carry entity predicates and directed edge predicates over the same pinned graph snapshot.
Entity predicates must identify an approved property, typed operator and typed value.
Relationship predicates must identify edge kind, direction, target predicate and `exists` or `not_exists`.
Entity operators must be eq, ne, lt, le, gt, ge or in, subject to the property's declared type.
All constraints use conjunction; unsupported operators must refuse rather than weaken the predicate.
The require_all default must intersect these sets before either text or vector ranking.
The optimizer must preserve that semantic barrier during reordering and replenishment.
Absence under an authorized graph view cannot establish a hidden relationship's existence.
Constraint evaluation must never fetch unauthorized edges to resolve an assertion.

A valid contradiction yields zero eligible candidates and typed shortfall.
Invalid syntax, unknown property types, stale snapshots and unsupported semantics yield refusal before ranking.
Evidence must bind the normalized constraint digest and authorized predicate outcomes.
R039.3 closes the replenishment path: exhausted constraints never permit unconstrained replacement candidates.

Decision: compile into the existing predicate and candidate-set machinery.
Reject soft penalties because a highly similar prohibited candidate may still survive.

### R081.9.7: typed document-section hierarchy

Existing code: `crates/eg-plan/src/leanrag.rs:66` exposes GraphTopology.
Its HierarchicalRetriever at `crates/eg-plan/src/leanrag.rs:125` depends on AnnIndex and per-node embeddings.
`crates/eg-plan/src/exec.rs:84` supplies PlanCtx bindings.
Reachable today: no, for vector-free reasoning traversal.
R081.9.1 through R081.9.6 retain their community-summary, community-refinement and entity-search semantics.

HierarchyRetrievalPolicy must distinguish `community` from `document_section`.
A section record must carry document identity/revision, section ID, one parent, ordered children, granularity and source span.
Its summary must bind the source revision and summary-generation digest.
A document must have one root; child spans must lie within their parent's span.
The engine must admit structurally valid section records received through governed ingestion.
A query must bind one immutable document/tree revision.

The section_beam default must rank authorized child summaries using the approved reasoning binding.
It must descend in score order, break ties by section ID and retain every visited root-to-section path.
A selected leaf must resolve to its exact source span; a summary alone is not leaf evidence.
Every inference must consume declared token, call and cost budgets before dispatch.
Breadth, depth and visited-node limits must stop traversal before the next expansion.
The result must mark incomplete traversal and preserve all completed authorized paths.
The tree kind must work when no vector store exists.
Untrusted source text must remain evidence, never authority to change limits or access.

Decision: extend the existing hierarchy policy with a discriminated section variant.
Reject treating document sections as graph communities or using an absent vector as minimum relevance.
Reuse graph reads and bounded model execution, with a new section decision operator before terminal fusion.
Record bounded selection premises; do not request hidden reasoning traces.

### R039.2 and R040.3: document encoding policy

Existing code: EmbeddingSpaceRef at `crates/eg-types/src/embedding.rs:47` pins vector identity.
EmbeddingBinding at `crates/eg-query/src/tables/embedding_binding.rs:398` owns column bindings.
`src/server/handlers/semantic_index/dispatch.rs:14` dispatches governed ingestion stages.
`src/server/handlers/semantic_index/worker.rs:175` completes a generation stage.
`src/server/handlers/query/planning.rs:611` binds query encoding.
Reachable today: no, for complete independent chunk encoding and aggregate validation.

DocumentEncodingPolicy must pin tokenizer digest, model revision, chunk size, overlap, position mode and aggregation revision.
Its initial mean_chunks default uses 1024 tokens, zero overlap and local_reset positions.
These replaceable defaults are engineering starting values inspired by research, not measured optimal settings.
For size S and overlap O, windows start at 0 and advance by S-O. Stop after the first window whose end reaches the document end.
Require S greater than zero and 0 less than or equal to O less than S.
Tokenize once; preserve token IDs and include the final partial window exactly once.
Absolute positions retain original token offsets; local positions start at zero for each window.
Unsupported absolute offsets must refuse, never reset silently.

The SDK ingestion worker must execute the EG-approved recipe through its existing provider binding.
Each chunk must use a fresh encoder context.
The initial aggregate must be the arithmetic mean of raw chunk vectors, accumulated in ordinal order.
Normalize only the final aggregate when the pinned space requires normalization.
The recipe must be query-independent; it must leave query encoding unchanged.
Empty input, failed chunks and zero-norm normalized aggregates cannot publish a successful document vector.
Completion must validate the full receipt before publishing one document vector into the maintained generation.
R040.3 must prevent queries from mixing generations encoded under different policy identities.
Policy changes require a complete replacement generation; serving retains the prior compatible generation until activation.

Decision: one stored vector per document, with explicit chunk provenance.
Reject independently ranked chunk entries and full-document encoding presented as independent encoding.
The paper supports this mechanism; its benchmark gains are not acceptance evidence.

### R077.6: workload-based column advice

Existing code: `crates/eg-query/src/tables/embedding_binding.rs:398` defines EmbeddingBinding.
`src/server/handlers/query/sql_read.rs:100` handles SQL reads.
Its catalog response at `src/server/handlers/query/sql_read.rs:379` invokes the authorized table store and typed SQL executor.
`crates/eg-query/src/sql/ann.rs:116` supplies ANN ranking, not a recommendation service.
Reachable today: no, for workload-based recommendation and recommendation-specific refusal.
Search terms included ColumnVector, recommendation, workload and column statistics.

ColumnVectorAdvice must carry stable table/column identities, schema revision and an observation interval.
Observation counts must derive from successfully authorized, resolved query plans, not guesses from raw SQL strings.
The record must count text-search uses separately from exact filters, projections and joins.
Text-search uses include resolved pattern predicates, full-text queries and vector similarity queries.
Pattern predicates supply workload evidence before a column has any vector binding.
The workload_first default must require at least one observed text-search use for a positive recommendation.
It must sort by text-search-use count, then average text length, then distinct ratio, then stable column ID.
A versioned policy may replace those weights or thresholds only with explicit evidence.
Eligible columns without enough observations return insufficient_observations, not a fabricated positive score.

The initial eligibility rule permits embeddable text only.
It must reject numeric, boolean, timestamp, foreign-key, enumeration and unsupported binary data before recommendation scoring.
This recommendation refusal is separate from R077.2 binding refusal.
An unavailable approved encoder also prevents a positive recommendation.
Advice must consume schema and workload from the same authorized scope and declare both revisions.

Proposed read-only catalog relation: `column_vector_advice`.
A served `Sql` SELECT over this relation must invoke the advisor through the existing catalog response.
An implementation must register and authorize that relation before claiming serving.
The advisor must record only bounded counts and feature contributions; it must retain no raw query text.
Advice must not bind columns, schedule embedding work or modify existing binding lifecycle state.

Decision: extend R077.4 with measured workload input through R077.6.
Reject a second binding registry and recommendations based only on global query popularity.

### R040.2: topic/dense fusion

Existing code: `crates/eg-plan/src/exec.rs:84` defines PlanCtx.
`crates/eg-plan/src/exec.rs:848` implements fuse_rrf over ranked branches.
`crates/eg-plan/src/exec/vector_rank.rs:65` performs vector scoring.
EmbeddingSpaceRef at `crates/eg-types/src/embedding.rs:47` already defines coordinate identity.
Reachable today: no, for topic-backend ranking fusion.
Search terms included topic backend, topic centroid and aggregate embedding.

TopicBackend must return bounded topic responsibilities, centroids, backend revision and corpus generation.
Every centroid must carry the retriever's validated full space tuple.
Empirical centroids must derive only from authorized vectors in that same generation.
Native centroids require the same compatibility proof; equal dimensionality is insufficient.

The weighted_topic_dense default must use weight w in [0,1], initially 0.5.
Dense score D is (1+cosine(query,document))/2.
Topic score T is the responsibility-weighted mean of (1+cosine(query,centroid))/2.
Responsibilities must be nonnegative, finite and sum to one within absolute tolerance 1e-6.
Final score is (1-w)*D+w*T; stable document ID resolves ties.
Hard constraints and tenant filters must run before either component and final truncation.
Missing topic metadata, incompatible centroids and zero-norm inputs must refuse when topic fusion is requested.
A caller may explicitly choose a separately declared dense-only policy; silent fallback is forbidden.

Decision: bind replaceable topic providers into the existing planner.
Reject an in-process cosine-prototype helper as proof of production topic fusion.
No model training or teacher pipeline is required by this extension.

### Input mapping and delivery slices

| Accepted input | Extended requirement | Residual covered |
|---|---|---|
| EG:32 | R039.1 | Shared four-modality coordinate space and missing-embedding shortfall. |
| EG:33 | R040.1, R039.3 | Declared hard exclusions before ranking and constrained replenishment. |
| EG:38 | R081.9.7 | Distinct vector-free section hierarchy and complete authorized search paths. |
| EG:39 | R039.2, R040.3 | Configurable independent encoding, one aggregate and generation-safe filtered ranking. |
| EG:40 | R077.6 | Observed per-column workload and recommendation-specific type refusal. |
| EG:42 | R040.2 | Replaceable topic scoring with centroid-space compatibility. |

Each leaf is an implementation checkpoint: model, behavior, wiring, refusal, evidence, then served proof.
Chunk encoding separates window construction, independent inference and aggregation into individual leaves.
Cross-owner worker, harness and hosting work must land through their existing contracts.
No row becomes delivered merely because its definition or planned test path exists.

### References

- arXiv:2605.08384v2 — Hönicke et al., "jina-embeddings-v5-omni: Geometry-preserving Embeddings via Locked Aligned Towers" (2026).
- arXiv:2606.18781v1 — Lyu et al., "Lost in a Single Vector: Improving Long-Document Retrieval with Chunk Evidence Aggregation" (2026).

These papers inspire the space-alignment and chunk-aggregation mechanisms.
The requirements remain product-neutral and require independent served measurements.


### R039.4 and R040.4: retained parent acceptance

The child requirements must retain every original parent behavior and acceptance condition.
R039.4 retains multi-hop traversal with vector similarity, candidate replenishment, exactly k permitted results or an explicit counted shortfall.
Its S-01 reference-corpus oracle retains two-hop traversal, parallel edges and stable edge identity. Include ten permitted matches below unfiltered top-k. It remains required independently of modality and hard-constraint extensions.
R040.4 retains filter-aware ANN and BM25 scans over pre-filtered candidates or bitmap filters, with filtered-search correctness.
Its fixed-corpus before/after latency benchmark and correctness oracle remain required independently of constraint, encoding and fusion extensions.
The planned served cases in `test-spec.md` bind both residuals to the existing dispatch and planner paths.
No numerical latency target, runtime evidence or delivery receipt is invented.
Each residual case requires its own execution evidence to satisfy the original contract.
