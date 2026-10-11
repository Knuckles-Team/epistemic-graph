# Implementation plan

## Sequence

1. **Freeze shared contracts.** Audit current `eg-types` decision, connector-pack and generated Python/wire surfaces. Keep one component schema bump and one method inventory update. Add missing typed errors and versioned vectors without changing old record bytes. Gate: A1–A2.
2. **Close exact-solve and commit gaps.** Reuse `eg-compute/solve`, `decision/derivation`, server decide handlers and Agent Library transaction. Make certificate verification independent, unknown-fact policy explicit, no-good-cut bounded, and commit a re-derived CAS operation. Gate: A3–A5 and B.
3. **Qualify pack fact admission.** Consume validated component revisions only. Preserve claim provenance, reject decision-owned pack kinds, exclude withdrawn entries and refuse stale projection facts. Gate: E.
4. **Complete statistical boundary.** Reuse `eg-numeric/decision`, decision jobs, and record v2; make visible-only features, exact propensity logging, independent-outcome admission, calibration, replay and receipt-bound promotion testable. Gate: C–D.
5. **Add question adapters.** Implement typed retrieval, routing, enrichment, entity-resolution, schema mapping, topology and informational recommendation questions through the same ladder. No adapter owns a second policy or decision store. Gate: D and question-specific negative tests.
6. **Publish evidence.** Run quality and hosted checks on the exact branch head, land on main, rerun or cite hosted main checks, and update each requirement's delivery and acceptance states. A branch green status is not a main acceptance verdict.

## Task-plan facts

Add the coverage and guardrail queries as typed read methods beside `AgentAssemble`. Reuse the capability closure that assembly already derives. Reuse the A2A agent-card ingestion and the governance shapes in the core ontology corpus. Add no second catalog and no ranking. Generate the Python client methods from the same method registry. Gate: D2. (EG-DECISION-ENGINE-R126, EG-DECISION-ENGINE-R127)

### Guardrail entailment read

`GuardrailEntailment` accepts a tenant and task IRI, verifies the tenant boundary, and reads published rule heads through `AgentLibraryStore::published_guardrail_rules`. The existing pure query returns advisory entailment paths and source generations. It wraps them with the native ontology digest in a versioned result. The dispatcher uses the existing `agent:assemble-read` read policy and tenant snapshot classification. No decision record, enforcement policy, or persistent credential is created. Generated client artifacts follow the published Rust source.

## Parallelism and dependencies

The contract bump precedes decision handler integration. Exact solver and pack fact-admission work may proceed independently once DTO ownership is frozen. Statistical jobs consume the committed record and visibility rules; question adapters consume the stable decision method. Coordinate shared edits to `agent_component.rs`, method descriptors, access classification and generated contract files. This prevents competing schema versions.

### EG-DECISION-ENGINE-R116.2: runtime admission prerequisite

`crates/eg-capabilities/src/contract/access_path.rs::admit_call_path` is a Rust-only gate under the `contract-schema` feature. The generated Python senders, including operation-specific adapters and `_runtime.send_by_id`, dispatch to an arbitrary `client._send`. The existing native binding in `crates/eg-numeric/src/py/method_body.rs` canonicalizes request bodies; it does not expose or invoke the admission gate. The wire `Request` and its signed envelope do not identify a consumer call path.

The missing prerequisite is an owned runtime boundary. It must invoke the existing Rust gate and bind successful admission to the actual dispatch. Make the gate available to that runtime without requiring the schema generator. Reuse the published client and its native codec boundary; consumers must not import an in-process engine. Route every generated sender, typed adapter and dynamic dispatch through the admitted boundary. Change the generator and regenerate its outputs together.

A Python copy of the gate, an unchecked `Contract` label, or a caller-provided contract version cannot establish that binding. A codec call alone cannot prevent an arbitrary `_send` implementation from dispatching elsewhere. Define how the owned adapter supplies the call-path classification and prevents bypass before changing the wire contract. R116.2 is pending until that binding and the served contract-only acceptance fixture exist; the R116.1 unit tests prove only the typed gate.

## Review rules

Review each changed module for reuse of existing transactions, outbox, Blob CAS, generated DTOs and visibility filters. Require a negative proof for every new authority boundary, persistence mutation and candidate source. Check CCCC, Dupehound, KISS and zero-new-pair jscpd before landing; resolve findings in the owning module rather than adding a duplicate helper. Hosted baseline checks build disposable local fixtures. Record live-deployment evidence separately.

## Architecture

### 1. Boundaries and reuse

The canonical decision DTOs live in `crates/eg-types/src/decision/` (`request`, `record`, `derivation`, `policy`, `statistical`, `jobs`, `topology`, `digest`, `errors`, `replay`). The exact 0–1 engine and certificate verifier live in `crates/eg-compute/src/solve/`; statistical feature extraction, scoring, calibration, replay and promotion live in `crates/eg-numeric/src/decision/`. The server's `src/server/handlers/decide/` evaluates requests, while `src/server/persistence/decision_record/` and `decision_jobs.rs` own durable records and jobs. Python wrappers in `epistemic_graph/decision_client.py`, `decision_stat.py` and generated contract modules call the same served methods. Extend these surfaces; do not build a parallel Python or application-side decision engine.

The Agent Library owner store is the write authority for library decision records and component revisions. Graph `MutationBatch` is the write authority for graph-derived summary nodes. Blob CAS holds bulky feature inputs and draft heads with a held reference. The normal guarded component publish path is the sole promotion authority for a `DecisionHead` after a matching `DecisionEval` receipt. A connector pack cannot publish `DecisionRecord`, `DecisionHead` or `DecisionPolicy` entries.

### 2. Evaluation pipeline

```text
Authenticated request + tenant scope
  -> candidate source read (RLS before shortlist, scoring, counts)
  -> policy tightening + hard constraint filter
  -> native ontology closure with derivation edges
  -> bounded exact 0–1 optimization of legal candidates
  -> optional calibrated statistical ranking / selective-risk abstention
  -> immutable record + certificate + visible why-not explanations
  -> optional snapshot-checked DecisionCommit
```

The request carries a question kind, typed constraints, candidate source, policy reference and work budget. Candidate facts pin component ID, kind, definition digest, lifecycle, capability and model facts, provenance and evidence class. The input digest covers every value read by the decision function, including quantized feature matrix, time used for freshness, embedder/head/schema/policy digests, shortlist mode and exploration propensity when applicable. A live graph recomputation is a new decision, not replay of the old one. `verify-replay` runs the pure function over stored inputs and compares canonical bytes across supported targets. Integer `i128` arithmetic is checked; statistical math uses pinned deterministic implementations and quantizes once before digesting. `DecisionRecord` v1 is integer assembly, v2 statistical and v3 topology; decoder compatibility and golden vectors are versioned.

Policy order is monotone: a request may reduce risk, spending and latency budgets, increase minimum support, add denials and tighten the accepted optimality gap; a relaxation returns a typed error. Hard constraints cannot be bypassed by the scorer. The legal set comes from EG-held policy and facts rather than a caller-supplied arbitrary option list. A zero-cost unknown is prohibited; policy may exclude it, require a ceiling, or abstain. Visible constraints and derivations are returned; invisible candidates never appear in counts, digests, why-not or errors.

### 3. Exact solve and certificate

Create binary selection variables for candidate components and, where required, template/slot choices. Constraints include the following. Each required capability is covered by an eligible selected component. Required dependencies imply selection. Incompatible options cannot coexist. Each required slot has exactly one assignment. Cardinality and integer budget bounds hold. Model and tool modality/schema constraints hold. The objective is a sequence of integer levels: first hard feasibility, then policy-defined utility, resource use and stable tie-break. Preprocess dominated or impossible options and seed an incumbent greedily. Then branch and bound with an admissible lower bound until optimality or node budget. A budget-exhausted answer may return an incumbent and proven gap only if policy permits that gap; otherwise abstain. No fixed maximum number of feasible assemblies substitutes for work budgeting.

The independent certificate verifier checks input digest, sorted variable mapping, all constraints against the proposed assignment, objective recomputation, lower-bound validity, gap and solver version. It must not trust a claimed feasible assignment or bound. Why-not for a visible excluded option fixes that option on, re-solves under a separate bounded budget, and reports the resulting violation, worse objective, gap or budget exhaustion. A failed post-solve template/graph validator adds a no-good cut and re-solves within the original budget.

### 4. Statistical decision and evidence

Feature extraction is over the RLS-filtered candidate set. Candidate-local BM25 and visible-only vector scoring avoid corpus statistics and overfetch effects from invisible rows. The feature schema is versioned, records missingness and provenance, and includes only policy-approved facts: similarity, entailed coverage, evidence quality, belief/confidence, cost and latency. The resident option-marker scorer can encode state once and score each structured option in process; it must retain deterministic fixed-point output and CPU portability. Statistical heads are bounded, fitted components, not generative models; text generation and LLM weights stay outside EG.

Gold-labelled data and executed-option-only bandit data are different datasets. The latter requires exact action propensities and support diagnostics. It also requires inverse propensity or doubly robust estimators, effective sample size and censoring. Self-reported run outcomes cannot promote a head. Independent evaluations with sufficient trace fidelity and approved principal are joined to records. Hierarchical pooling requires minimum support and coarse output to avoid individual-run inference. Conformal and selective-risk calibration use held-out or walk-forward folds. They account for drift and explicitly abstain when their stated bound lacks support. `DecisionEval` receipts pin head, dataset window, policy, feature schema, metrics and thresholds. A policy change or head digest change invalidates the receipt for promotion.

### 5. Connector pack interaction

The existing pack types in `crates/eg-types/src/connector_pack/`, handlers in `src/server/handlers/admin/connector_pack/`, persistence in `src/server/persistence/connector_pack/` and Python `epistemic_graph/connector_pack.py` form one owner. The decision engine consumes published, tenant-visible component revisions and their annotations. It does not parse raw packs or trust client-provided digests. A pack entry's `provides`, `requires_capabilities`, modality and schema digest are publisher claims. So are its cost, latency, model profile and `contract_pin`. Native capability IRIs can enter ontology derivation with that claim premise. Unknown foreign IRIs are claim-only exact terms. Malformed native IRIs are refused at import. Re-import does not alter a component revision when its own entry content is byte-identical. `Withdrawn` members become ineligible immediately. A pack cannot insert decision-owned component kinds. The pack head and projection readiness marker must match before a projected ontology/shape fact is used as current.

Decide's contract at this boundary is narrow: it sees only validated component revisions and preserves publisher-claim class in every derivation, score and record. It rejects reserved decision-owned kinds and stale projection facts. Pack import atomicity, body storage and parser refusals are specified and tested by the pack owner independently.

### 6. Question-specific extensions

Typed question adapters translate retrieval, lane, enrichment, entity-resolution, schema-mapping, routing, risk, topology and recommendation requests into the same legal-set/evidence pipeline. `DecideText` is a dedicated UQL-like front end with quoted candidate blocks and typed parameters. Malformed syntax returns a structured span/error. It never falls through to ordinary query parsing. Topology adds bounded integer shape variables (width, rounds, role, capacity) and a composed-schema digest. It then outputs a proposed `AgentGraph`. Capacity acquisition and execution remain separate atomic operations. Entity resolution emits a proposed link and confidence/abstention. It never emits an automatic identity merge. Finance recommendations are informational snapshots with strategy version, evidence and horizon. They cannot place trades. Graph-sourced record views must apply the same RLS as their candidate source.

### 7. Operational and quality rules

Expose metrics for resolution kind, evidence class, abstention reason, solver nodes and gap, stale catalog refusals, record replay failures, calibration coverage, drift, ESS and latency. Do not put raw tenant facts in metric labels. Contract generation, CCCC, KISS, Dupehound and zero-new-pair jscpd are repository quality checks; Rust formatting, Clippy and focused unit/served tests validate behavior. Hosted CI should create its own local tenant, seed fixtures and, when needed, start an ephemeral EG server. A pre-existing deployment, identity provider or secret is not a baseline PR prerequisite.
## EG-DECISION-ENGINE-R118 and EG-DECISION-ENGINE-R119 implementation seam

Reuse `crates/eg-numeric/src/risk/reputation.rs` for posterior estimation, `crates/eg-epistemic/src/ranking.rs` for evidence-aware ranking, and `crates/eg-plan/src/exec/reliability.rs` for UQL consumption. The server materialization joins approved, independently labeled decision outcomes under tenant/RLS scope before these pure kernels run. It stores the input revision and calibration policy with the view. Replace request-only reliability authority rather than adding a parallel score table.

Reuse `crates/eg-numeric/src/decision/replay/` for fold construction and shared-cap arithmetic, `src/server/handlers/decide/stat_walk_forward.rs` and `stat_jobs.rs` for persisted execution, and the typed `EvaluationRun` contract in `eg-types`. The same record sealer and no-look-ahead verifier should serve finance specialization. Generated method/client contracts follow the Rust DTOs. Fold checkpoints contain immutable digests and resume only when all source revisions and policy versions still match. They never turn a partial run into a sealed result.

## Coverage

This register inventories all 118 decision-owned obligations. `Historical label` records a prior delivery claim; it is not acceptance evidence. Every row starts with current acceptance `NOT_RUN` until a published-main SHA and its exact tests are recorded in an evidence update. `—` means the invariant spans the whole sequence below. Sequence labels express order, not completeness.

| ID | Sequence | Historical delivery label | Acceptance review | Self-contained obligation |
|---|---|---|---|---|
| EG-DECISION-ENGINE-R001 | T4 | BUILT | NOT_RUN | Decision ladder with resolution kinds (constraint / entailment / optimization / statistical / abstention) |
| EG-DECISION-ENGINE-R002 | T2 | LANDED | NOT_RUN | The architecture decision record's evidence classes (proof / observation / claim) recorded on every decision record |
| EG-DECISION-ENGINE-R003 | T2 | LANDED | NOT_RUN | Proofs for entailed (rung-2) answers — is_a derivation records with per-edge source/premise class |
| EG-DECISION-ENGINE-R004 | T1 | LANDED | NOT_RUN | Exact 0-1 integer solver with an independently verified certificate |
| EG-DECISION-ENGINE-R005 | T2 | LANDED | NOT_RUN | Conformal / risk-controlled guarantees (acceptability-set conformal, selective risk control / Learn-then-Test, adaptive conformal for drift) |
| EG-DECISION-ENGINE-R006 | T2 | LANDED | NOT_RUN | Exact propensities, off-policy evaluation (clipped/SWITCH-IPS, doubly robust, ESS) and safe promotion gated on minimum ESS |
| EG-DECISION-ENGINE-R007 | T2 | LANDED | NOT_RUN | Deterministic verify-replay of committed records across hosts (pinned soft libm, quantised fixed-point digests, five-target golden vectors) |
| EG-DECISION-ENGINE-R008 | T2 | LANDED | NOT_RUN | Explanations — why chosen / why-not, budgeted re-solve per excluded option |
| EG-DECISION-ENGINE-R009 | T2 | CLOSED | NOT_RUN | Monotone safety — learned rungs only narrow or abstain, never widen or authorize |
| EG-DECISION-ENGINE-R010 | T4 | BUILT | NOT_RUN | Typed abstention (AbstainReason enum) naming the missing capability |
| EG-DECISION-ENGINE-R011 | T1 | LANDED | NOT_RUN | No cardinality cap on solved assemblies (deterministic-work-budgeted search, no arbitrary result-count limit) |
| EG-DECISION-ENGINE-R012 | T2 | LANDED | NOT_RUN | Multi-question shared state (assembly-level slate crediting; outcome aggregate keyed across questions) |
| EG-DECISION-ENGINE-R013 | T2 | LANDED | NOT_RUN | Tenant/RLS-safe decisions — visibility filtered per candidate source before features are computed, no leak via reports or aggregates |
| EG-DECISION-ENGINE-R014 | T1 | LANDED | NOT_RUN | Composition as constraint optimisation (bounded 0-1 IP over covering/implication/exactly-one/knapsack/feature-compat/cardinality) rather than a single LLM choice |
| EG-DECISION-ENGINE-R015 | — | CLOSED | NOT_RUN | Invariant: no LLM weights or text generation inside EG; only small fitted statistical heads |
| EG-DECISION-ENGINE-R016 | T2 | LANDED | NOT_RUN | Invariant: training uses only independently evaluated outcomes (exact propensities, approved commit principals; self-reported/LLM-resolved outcomes excluded) |
| EG-DECISION-ENGINE-R017 | — | CLOSED | NOT_RUN | Invariant: one write authority per kind of durable decision state (Agent Library kernel / graph MutationBatch / component-publish+receipt) |
| EG-DECISION-ENGINE-R018 | — | CLOSED | NOT_RUN | Invariant: unknown facts are never treated as zero in the solver objective |
| EG-DECISION-ENGINE-R019 | — | CLOSED | NOT_RUN | Invariant: commits are snapshot-checked (re-derive, verify certificate, catalog compare-and-set) |
| EG-DECISION-ENGINE-R020 | T2 | LANDED | NOT_RUN | Invariant: exploration needs an explicit budget and an unpredictable HMAC-keyed seed; forbidden for security/policy/write-back/irreversible questions |
| EG-DECISION-ENGINE-R021 | T2 | LANDED | NOT_RUN | Versioned feature schema over caller-visible candidates. Features: vector similarity, candidate-local BM25, entailed coverage, provenance quality, belief confidence, cost/latency, freshness, outcome stats. |
| EG-DECISION-ENGINE-R022 | T2 | LANDED | NOT_RUN | Full-label vs bandit-label regime separation (gold-labelled questions vs executed-option-only outcomes) |
| EG-DECISION-ENGINE-R023 | T2 | LANDED | NOT_RUN | Hierarchical Beta-Binomial pooling of outcome statistics (kind → capability class → option) with n_min gating |
| EG-DECISION-ENGINE-R024 | T2 | LANDED | NOT_RUN | Multi-class calibration (temperature/vector/Dirichlet scaling; isotonic for binary Noul only) — general calibration utilities |
| EG-DECISION-ENGINE-R025 | T2 | LANDED | NOT_RUN | Drift detection: out-of-distribution feature checks plus adaptive conformal inference miscoverage bound |
| EG-DECISION-ENGINE-R026 | T2 | LANDED | NOT_RUN | Audit sampling with inverse-probability weighting (5% of acted decisions) |
| EG-DECISION-ENGINE-R027 | T2 | LANDED | NOT_RUN | Fitted decision heads: WeightedFeatures (advisory), ListwiseLogistic (deterministic optimiser); KanListwise deferred |
| EG-DECISION-ENGINE-R028 | T2 | LANDED | NOT_RUN | NL template routing without an LLM (NlTemplate matched via candidate-local BM25/embedding/ontology-label similarity) |
| EG-DECISION-ENGINE-R029 | T4 | BUILT | NOT_RUN | Retrieval-plan choice (LeanRAG vs FuseRrf vs SPARQL) |
| EG-DECISION-ENGINE-R030 | T4 | BUILT | NOT_RUN | Ingestion lane routing (fast/medium/slow) via Decide |
| EG-DECISION-ENGINE-R031 | T4 | BUILT | NOT_RUN | Enrichment-plane scheduling (expected value vs cost) |
| EG-DECISION-ENGINE-R032 | T3 | LANDED | NOT_RUN | Entity resolution / owl:sameAs candidates (Noul) with gold-set conformal guarantees |
| EG-DECISION-ENGINE-R033 | T3 | LANDED | NOT_RUN | Source schema mapping for SQL ingestion and packs (columns → ontology properties) |
| EG-DECISION-ENGINE-R034 | T4 | BUILT | NOT_RUN | TMS contradiction-handling suggestions (proposal-only, never automatic retraction) |
| EG-DECISION-ENGINE-R035 | T4 | BUILT | NOT_RUN | AU model/prompt/skill/tool/harness/account-mode/sandbox routing via graph.decide() |
| EG-DECISION-ENGINE-R036 | T4 | BUILT | NOT_RUN | graph.assemble() replaces LLM agent composition (architecture decision record §5 amendment) |
| EG-DECISION-ENGINE-R037 | T4 | BUILT | NOT_RUN | Abstention escalation to LLM/human re-enters as a claim |
| EG-DECISION-ENGINE-R038 | T4 | BUILT | NOT_RUN | Advisory pre-tool risk scoring |
| EG-DECISION-ENGINE-R039 | T4 | BUILT | NOT_RUN | Cost-aware routing from L5 accounting |
| EG-DECISION-ENGINE-R040 | T2 | LANDED | NOT_RUN | Post-run promotion of fitted heads via component publish + DecisionEval receipt |
| EG-DECISION-ENGINE-R041 | T4 | BUILT | NOT_RUN | Inbound connector event triage via Decide (evaluate-only, sampled logging) |
| EG-DECISION-ENGINE-R042 | T4 | BUILT | NOT_RUN | Connector-internal tool choice via Decide (evaluate-only; D18 write-back stays deterministic) |
| EG-DECISION-ENGINE-R043 | T4 | BUILT | NOT_RUN | Write-back proposals through Decide (deterministic authorization modes, D18) |
| EG-DECISION-ENGINE-R044 | T4 | BUILT | NOT_RUN | A2A inbound task routing to agent graphs |
| EG-DECISION-ENGINE-R045 | T4 | BUILT | NOT_RUN | Multiplexer tool-subset exposure (smallest covering tool subset within a context budget — token savings) |
| EG-DECISION-ENGINE-R046 | T6 | BUILT | NOT_RUN | Swarm topology decisions via Decide |
| EG-DECISION-ENGINE-R047 | T4 | BUILT | NOT_RUN | WP A1 — Contract: DecisionRecord v1 types, AgentAssemble/DecisionCommit methods, DecisionRecord component kind, decide feature, count-pin migration |
| EG-DECISION-ENGINE-R048 | T2 | LANDED | NOT_RUN | WP A2 — Derivation: is_a records, weakest-premise evidence class, task→capability closure, claim-premised task mappings, verifier |
| EG-DECISION-ENGINE-R049 | T4 | BUILT | NOT_RUN | WP A3 — Solver: preprocessing, greedy incumbent, B&B, Lagrangian/dual covering bound, node budget, certificate, verify_certificate, template enumeration, no-good loop, brute-force oracle |
| EG-DECISION-ENGINE-R050 | T2 | LANDED | NOT_RUN | WP A4 — Facts: typed optional ModelProfile/tool facts (modality, cost, latency, schema digests); delete unread provides |
| EG-DECISION-ENGINE-R051 | T2 | LANDED | NOT_RUN | WP A5 — Executor and commit: AgentAssemble/DecisionCommit handlers, one-agent-graph rule, refuse DecisionRecord in packs, audit lines |
| EG-DECISION-ENGINE-R052 | T2 | LANDED | NOT_RUN | WP A6 — Acceptance: routing fixture with acceptability sets, planted bad inputs, oracle, golden vectors, latency report, outcome-link test |
| EG-DECISION-ENGINE-R053 | T2 | LANDED | NOT_RUN | G0 — Gold set: ≈50 labelled typed-requirements→acceptable-assemblies items for 2.27.x, growing to ≈500 (≈100/class) before any 2.28 calibration claim |
| EG-DECISION-ENGINE-R054 | T2 | LANDED | NOT_RUN | DL-1 Contract (statistical) — Decide/DecisionEval/DecisionFit methods, record v2, generated TTL/SHACL |
| EG-DECISION-ENGINE-R055 | T1 | LANDED | NOT_RUN | DL-2 Numeric — pinned libm, quantised fixed-point, calibration/conformal/risk-control/OPE numeric stack |
| EG-DECISION-ENGINE-R056 | T4 | BUILT | NOT_RUN | DL-3 Solver (statistical release) |
| EG-DECISION-ENGINE-R057 | T2 | LANDED | NOT_RUN | DL-4 Executor — CandidateSource (library+graph), dedicated decision executor, DecideText parser, typed params, WeightedFeatures head |
| EG-DECISION-ENGINE-R058 | T2 | LANDED | NOT_RUN | DL-5 Records and commit — graph-sourced summary records, row visibility, Blob CAS, retention, RLS-routed reads |
| EG-DECISION-ENGINE-R059 | T2 | LANDED | NOT_RUN | DL-5b Outcome aggregate — admission link, outcome aggregate index, independent-evaluation join, censoring, fidelity floor |
| EG-DECISION-ENGINE-R060 | T2 | LANDED | NOT_RUN | DL-6 Learning — DecisionFit/DecisionEval jobs, ListwiseLogistic, exploration budget, audit sampling, promotion by publish+receipt |
| EG-DECISION-ENGINE-R061 | T2 | LANDED | NOT_RUN | DL-7 Assembly facts (statistical extension of A2+A4) |
| EG-DECISION-ENGINE-R062 | T4 | BUILT | NOT_RUN | DL-8 NL templates — NlTemplate, template choice, slot filling, LLM fallback recording |
| EG-DECISION-ENGINE-R063 | T2 | LANDED | NOT_RUN | DL-9 Client and docs — typed wrappers, docs, examples, acceptance tests |
| EG-DECISION-ENGINE-R064 | T4 | BUILT | NOT_RUN | DL-9b Views (optional) — SPARQL/GraphQL/pgwire views of decision records |
| EG-DECISION-ENGINE-R065 | T2 | LANDED | NOT_RUN | Record size cap (64 KiB / ≤64 candidates / ≤32 capabilities) with RecordTooLarge abstention and root-bound fallback |
| EG-DECISION-ENGINE-R066 | T2 | LANDED | NOT_RUN | Retention policy split: library-sourced records kept forever; graph-sourced statistical records get policy-set windows; high-frequency low-stakes decisions are evaluate-only |
| EG-DECISION-ENGINE-R067 | T2 | LANDED | NOT_RUN | Record versioning discipline (every version decodable forever, golden vectors per version, fail-closed version mismatch, drained rolling redeploys) |
| EG-DECISION-ENGINE-R068 | T2 | LANDED | NOT_RUN | Outcome link via the existing AgentGraphDraft.synthesis_evidence pin (no new RunEvent/CommitOutcomeBundle fields) |
| EG-DECISION-ENGINE-R069 | T2 | LANDED | NOT_RUN | DecisionShape SHACL enforcement, fail-closed inside the commit handler, independent of the opt-in ICV guard |
| EG-DECISION-ENGINE-R070 | T2 | LANDED | NOT_RUN | No-good-cut re-solve loop for post-solve validators (template/graph validation, L4 negotiation) |
| EG-DECISION-ENGINE-R071 | T2 | LANDED | NOT_RUN | DecideText UQL-like front end, kept out of the ordinary UQL parser and wire::Op/wire::Plan |
| EG-DECISION-ENGINE-R072 | T2 | LANDED | NOT_RUN | Observability: OTel spans/metrics for resolution-kind distribution, abstention rate, evidence-class distribution, solver nodes/gap, ESS, latency percentiles |
| EG-DECISION-ENGINE-R073 | T3 | LANDED | NOT_RUN | PA2 — Store: five ConnectorPack tables within the 21-table Agent Library registry. Lineage-based open-time refusal, N-intent receipts, batch pins, Withdrawn handling, head CAS, and per-component outbox events. |
| EG-DECISION-ENGINE-R074 | `tests/test_decision_static_contracts.py` | CLOSED | PASS | The closure rule lists every component kind. The consistency test checks enum and slot references. |
| EG-DECISION-ENGINE-R075 | T4 | BUILT | NOT_RUN | G6 — Component-id uniqueness and forbidden (Decide-owned) kinds |
| EG-DECISION-ENGINE-R076 | T4 | BUILT | NOT_RUN | Opt-in nested Pydantic models for decision-type result markers only (2.27.x-scoped subset, precedes the full migration) |
| EG-DECISION-ENGINE-R077 | T3 | LANDED | NOT_RUN | Shared component schema bump 2→3 (Decide owns numbering; pack adds Withdrawn + new kinds) — one co-landing step |
| EG-DECISION-ENGINE-R078 | T4 | CLOSED | NOT_RUN | Contract freeze re-issued with the decide feature |
| EG-DECISION-ENGINE-R079 | AU-batch | BUILT | NOT_RUN | **Embedding admission classifier.** Ingest-time embedding is applied to essentially everything crossing the ChangeEnvelope boundary by explicit design (envelope_ingest.py:2753 — the chokepoint exists "so no future connector can bypass it"). The only gates are four cheap predicates: upsert+typed-payload, text-unchanged short-circuit, empty-derived-text, and a global KG_INGEST_AUTO_EMBED flag defaulting ON. No content-type policy, no per-column decision, no cost budget |
| EG-DECISION-ENGINE-R080 | AU-batch | BUILT | NOT_RUN | **Ingestion cost ladder with provenance**, mirroring the approved Decide decision ladder: rung 0 AST (EXTRACTED), 1 symbol/type resolution (INFERRED), 2 statistical/community (DERIVED), 3 classical ML/NER (MODELED), 4 embeddings (EMBEDDED), 5 LLM (ASSERTED). A higher rung runs only when every lower rung abstains and may never overwrite a deterministic fact — the same monotone-safety property ruled for Decide |
| EG-DECISION-ENGINE-R081 | AU-batch | BUILT | NOT_RUN | **Weight community-detection edges by the resolver confidence we ALREADY compute, and stop letting similar_to vote.** The kernel consumes undirected_weighted_adjacency() so weights are supported; ingestion supplies none, discarding the exact signal that makes communities trustworthy. A scoped call (0.95) should bind a community harder than a unique guess (0.60) — see resolve.rs:663-672. Separately, MinHash similar_to is a RESEMBLANCE edge, not a structural one: including it conflates "looks alike" with "works together" (two similarly-shaped utility files are not a module). Type-weight edges — calls/imports/inherits structural, similar_to excluded or heavily discounted |
| EG-DECISION-ENGINE-R082 | T4 | BUILT | NOT_RUN | **Option-marker head geometry for eg-decision** — encode state once, score each option at its own marker, softmax in-process, one forward pass serving several questions; no decoder loop. Adopted from the Laya encoder design |
| EG-DECISION-ENGINE-R083 | T4 | BUILT | NOT_RUN | **Structured option encoding instead of textual — surpasses the external design by construction.** Laya scores 0.425 on Banking77 (vs 0.870 for the closed baseline) purely because 77 label strings exhaust head_max_len; that is a TOKENISATION limit, not a decision limit. Our options are IRIs carrying SHACL shapes, declared capabilities and cost/latency facts, so an option is encoded from its STRUCTURED features and cardinality bounds on feature-vector width rather than token budget. The failure mode that caps their usable option sets does not exist in ours |
| EG-DECISION-ENGINE-R084 | T4 | BUILT | NOT_RUN | Conformal coverage and abstention are the primary calibration rung across changing option-set sizes. Temperature scaling is a fallback when exchangeability fails. Report both expected calibration error and empirical coverage. |
| EG-DECISION-ENGINE-R085 | T4 | BUILT | NOT_RUN | **Derive the legal option set; never merely receive it.** Laya scores the A you hand it and cannot say which options are legal. We generate A from the ontology (SHACL shapes + SPARQL shortlist) so only type-valid, policy-admissible candidates are ever scored. This is what makes monotone safety ENFORCEABLE rather than aspirational: an illegal option is never in A, so a learned rung cannot introduce one. It is also the scorer/planner distinction |
| EG-DECISION-ENGINE-R086 | T4 | BUILT | NOT_RUN | **DecisionFit promotion evaluation protocol.** Adopt their four metrics (accuracy, soft accuracy, Brier, score MAE) and add five: empirical coverage alongside ECE. Abstention rate AND accuracy-on-answered (a good abstainer must be more accurate on what it answers, else the abstention is noise). Cost per decision (an unmeasured rung will not stay cheap). Decision stability (same state + same A -> same answer, since decisions are recorded in the graph and must be reproducible). And calibration drift over time, which bitemporal makes measurable. Frozen workflow set, per tenant and per domain |
| EG-DECISION-ENGINE-R087 | T4 | BUILT | NOT_RUN | **Cardinality rule: shortlist before scoring.** Never hand a whole catalog to a scorer; SPARQL shortlist with a bounded LIMIT first. Their Banking77 collapse is the empirical case for it, and EG-DECISION-ENGINE-R083 raises the ceiling rather than removing the rule |
| EG-DECISION-ENGINE-R088 | T4 | BUILT | NOT_RUN | **Trajectory-prefix belief (the one durable idea from SalesRLAgent).** Turn-by-turn P over conversation PREFIXES is a special case of BELIEF AS OF t over temporal slices — a capability we already have bitemporally and neither external system does. Expose it as a Decide primitive rather than a bespoke sales model |
| EG-DECISION-ENGINE-R089 | T4 | BUILT | NOT_RUN | **External validation of the decision ladder, to be cited in the design rationale.** Laya base scores 0.362 on typed decisions against 0.318 random and 0.461 majority-class — near chance, and BELOW majority. That is strong empirical evidence that a learned rung cannot be the primary decision mechanism without task labels, which is exactly the approved ladder's rule (Constrained -> Entailed -> Solved -> Scored -> Abstained, learned rungs may only narrow or abstain). Best external corroboration of monotone safety we have |
| EG-DECISION-ENGINE-R090 | T4 | BUILT | NOT_RUN | **Resident native decision scorer in eg-decision — re-implement, do not vendor.** cua-ai/cua-s1-forms (MIT, open weights) proves the option-attention head geometry works at **706,048 params / 2.8 MB / 7-9 ms on CPU**: byte embedding (vocab 257) -> 2-layer transformer encoder (width 128, 4 heads) -> option-attention head, trained by plain cross-entropy on 10k synthetic episodes in ~30 minutes. That is small enough to be RESIDENT — compiled into the engine, no Python runtime, no sidecar, no IPC hop, no GPU scheduling. This reverses the Laya ruling (421M, do not adopt) on size grounds, not principle |
| EG-DECISION-ENGINE-R091 | T4 | BUILT | NOT_RUN | **Bit-reproducible decisions via fixed-point arithmetic — the strongest reason to build rather than borrow.** EG records decisions as DecisionRecord graph objects; a nondeterministic scorer makes that record unreproducible, defeating the point of recording it. Jev is hosted (irreproducible by construction), Laya is 421M floats, cua-s1 is PyTorch float — none can offer this at any size. eg-numeric's detkernel already enforces a no-f64-transcendental discipline for exactly this class of reason, so a 2-layer encoder plus attention head in FIXED-POINT INTEGER arithmetic is bit-identical across machines and across time |
| EG-DECISION-ENGINE-R092 | T4 | BUILT | NOT_RUN | **Their limitations, correctly reframed.** (a) "Cannot invent a fill value — only chooses among extractor output" is not a weakness but **monotone safety stated in domain terms**: the scorer SELECTS from a set derived by the deterministic rungs and can never introduce an option — the same rule as EG-DECISION-ENGINE-R085, now independently corroborated. (b) "English-centric bytes, 224-byte context" is a consequence of encoding TEXT; encoding structured IRI/SHACL/capability features directly removes both the language dependence and the byte budget — the same conclusion as EG-DECISION-ENGINE-R083, now confirmed by a second system hitting the same wall |
| EG-DECISION-ENGINE-R093 | T4 | BUILT | NOT_RUN | **Train the resident scorer from the FX-SYNTH corpus.** cua-s1 trained on 10k synthetic episodes in ~30 minutes. Our synthetic e2e generator (landed, 105 tests) already produces seeded, ground-truth-bearing episodes through the REAL served path, which is a better training source than hand-built synthetic forms — the labels come from the same engine the scorer will run inside. Split: `.1` below is net-new and built; `.2` (wiring `export_gold_corpus` through a served client) remains unbuilt |
| EG-DECISION-ENGINE-R093.1 | T4 | BUILT | NOT_RUN | Typed `SOURCE_SERVED`/`SOURCE_IN_PROCESS` provenance on gold-corpus items plus `verify_ground_truth`, which refuses a `SOURCE_SERVED` item with an empty or candidate-leaking acceptable set |
| EG-DECISION-ENGINE-R094 | T4 | BUILT | NOT_RUN | **Operational consequence: a CPU-resident decision plane deploys everywhere the engine does.** A scorer needing a GPU sidecar cannot run in every k8s pod; one compiled into the binary can. 706k params at 2.8 MB needs no GPU, no vLLM, no model server, and retraining a specialist is a ~30-minute job rather than a cluster activity. This is a deployment-topology argument, not a modelling one, and it is why resident beats sidecar even at equal accuracy |
| EG-DECISION-ENGINE-R095 | T3 | LANDED | NOT_RUN | **The solver's committed replay contract is BROKEN on main, and no publication gate runs the test that proves it.** crates/eg-compute/tests/solve/determinism.rs::the_certificate_digest_matches_the_committed_golden_vector fails on clean main @ be0f1acb0: computed 1bc99ac3ed5d12211a9074b23cafde1f35f7ed90d70608f7e47c4a0429f4ea88 vs committed GOLDEN_DIGEST = 71a64f581943d7cadfaecbcba26644c7d2fd2176ff714a9c1b840dc6659d8f4e. The other two tests in the file PASS, including repeated_solves_are_byte_identical (4 solves compared), so the solver is stable **within** a run — what moved is the value relative to the committed constant. Bit-reproducible certificates are the whole reason decisions are recorded as graph objects (cf. EG-DECISION-ENGINE-R091), and FX-SOLVER shipped in 2.27.x. **Discovered incidentally** by an unrelated grammar change's batched cargo test -p eg-compute; ruled out as that change's own regression by an authoritative Cargo.lock package-set diff (5 added, 0 removed, **0 version-changed**) and confirmed by running the test on clean main. cargo check --locked passes on base |
| EG-DECISION-ENGINE-R096 | T2 | LANDED | NOT_RUN | **The dominant community-detection wire method cannot carry edge weights or a quality function, so EG-REPO-INGEST-R011 and related rows (see `repository-index-and-ingestion/requirements.md`) are unreachable in production as built.** CommunityDetectEphemeral (crates/eg-types/src/protocol/method/method_02.rs:115-119) is { node_ids: Vec<String>, edges: Vec<(String, String)>, resolution: f64 } — the edge tuple has **no weight/properties slot** and there is **no quality-function selector**, only a modularity resolution. This is the stateless path tried FIRST and taken almost always in production. Consequence: EG-DECISION-ENGINE-R081's confidence weighting and EG-FEDERATED-QUERY-R060's CPM both land correctly in the kernel but are reachable only through the rarer tenant-load fallback branch. **This corrects EG-DECISION-ENGINE-R081's own premise** — the register says "the kernel change is zero, only the weight supply changes", and the weight supply turns out to require a wire change as well as the AU-side change. The AU half is done and correct: confidence was already extracted into EnrichmentEdge.props by enrichment/extractors/code_test.py:274-279 and was being discarded by enrichment/features.py's cluster_features(); the fix now carries it as (source, target, confidence) and never fabricates a value for the Python name-only fallback resolver, which has no tiered confidence model. similar_to is confirmed MOOT on this path — cluster_features/resolve_call_edges only ever build CALLS edges |
| EG-DECISION-ENGINE-R097 | T1 | LANDED | NOT_RUN | **CORRECTED: these are DETERMINISTIC stale-test-assumption defects, NOT load-sensitive races.** Filed originally as EG-DURABLE-KERNEL-R014's family because they surfaced in the constrained gate; A6 root-caused both statically and an independent main-verify run on be0f1acb0 reproduced them unconstrained (1936 passed, 3 failed). **(a)** every_declared_surface_reaches_its_stub — the test's signed() helper hardcoded ONE literal nonce ("05".repeat(32)) for every request while looping over contract_wave_samples(); the transport replay record is a single global OnceLock in test builds and correctly rejects the second non-mutating request presenting that nonce. AgentAssemble records it first, Decide collides. 100% deterministic, present since the test's introduction in a4804f87c. **(b)** roundtrip_dedup_bounded_memory_and_gc — the X2 blob-GC hardening commit 62a2471e1 introduced a default **24-hour GC grace period** and never updated this dispatch-level test, which uses the real wall clock and asserts immediate reclaim after BlobUnref; grace cannot elapse inside a millisecond test, so blobs_reclaimed is always 0. Neither shares any mechanism with EG-DURABLE-KERNEL-R014. Original text follows. ~~Two --lib tests fail under 2-core constrained execution, shape-consistent with EG-DURABLE-KERNEL-R014's load-sensitive family.~~ (a) server::contract_wave::dispatch_reachability_tests::every_declared_surface_reaches_its_stub panics "Decide must reach its contract-wave stub, got nonce already used (replay rejected)" (src/server/contract_wave.rs:184); (b) server::dispatch::graph_pipeline::blob_dispatch_tests::roundtrip_dedup_bounded_memory_and_gc fails "unreferenced blob reclaimed, left: 0, right: 1" (graph_pipeline.rs:2028). Both are exactly what the GOC-70 constrained-parallelism gate exists to catch. **Neither was diagnosed** — the review that found them correctly flagged rather than guessed |
| EG-DECISION-ENGINE-R098 | — | DEFERRED | NOT_RUN | Defer a Rust microgpt, in-engine autograd, generative UQL model and KLPO loss in EG pending a separate canonical Gap and frozen held-out evidence. Reuse the approved typed Decide ladder for legal-option derivation, calibrated probabilities, cost-sensitive optimal choice, provenance, abstention, NlTemplate and constrained DecideText/UQL; the bounded resident scorer is non-generative and does not authorize a GPT |
| EG-DECISION-ENGINE-R099 | T3 | LANDED | NOT_RUN | Wikidata alignment connector (wikidataId, skos:exactMatch) feeding EG-DECISION-ENGINE-R032 entity resolution keys for Region/Country/Taxon/Food/Nutrient. EG-DECISION-ENGINE-R033 proposes connector schema_mappings against world_model-v1 properties with the world-model SHACL shapes as acceptance |
| EG-DECISION-ENGINE-R100 | T6 | BUILT | NOT_RUN | Retrieval outcome join (cited evidence + independent evaluation) on retrieval DecisionRecords; durable hard negatives |
| EG-DECISION-ENGINE-R101 | T6 | BUILT | NOT_RUN | Query-side low-rank embedding adapter head, post-RLS in ANN probe, promoted by DecisionEval receipt |
| EG-DECISION-ENGINE-R102 | T6 | BUILT | NOT_RUN | Schema repair: Decide mapping -> candidate GraphSchema/pack -> ABox check -> shadow branch ingest -> approved activation |
| EG-DECISION-ENGINE-R103 | T6 | BUILT | NOT_RUN | Calibrated flip confidence per horizon with conformal abstention |
| EG-DECISION-ENGINE-R104 | T4 | BUILT | NOT_RUN | DecideText joins the UQL lexer/grammar family ($param, quote-aware candidate block, structured errors, grammar table) |
| EG-DECISION-ENGINE-R105 | T6 | BUILT | NOT_RUN | **ST-1** — swarm-topology TBox + shapes. Publisher through GraphSchema.attach. SHACL positive/negative fixtures — AU agent_utilities/knowledge_graph/ontology/swarm_topology/{topology.ttl,shapes.ttl}, decide/topology/schema_source.py |
| EG-DECISION-ENGINE-R106 | T6 | BUILT | NOT_RUN | **ST-2** — Reference templates (single, pipeline, fan-out/join, supervisor→workers, critique loop, council) as AgentGraph templates with TopologyFacts — AU decide/topology/templates.py |
| EG-DECISION-ENGINE-R107 | T6 | BUILT | NOT_RUN | **ST-3** — Contract: topology.rs types (≤12), AssemblyRequirements.topology, TopologyFacts on template facts, record version increment + v1 goldens, generated Python, one pin move — EG crates/eg-types/src/decision/topology.rs (new), decision/request.rs, decision/record/outcome.rs, agent_component.rs; eg-capabilities pins |
| EG-DECISION-ENGINE-R108 | T6 | BUILT | NOT_RUN | **ST-4** — Publish-time TemplateTopologyShape validation, programmatic and fail-closed — EG src/server/handlers/admin/agent.rs hook + src/server/handlers/decide/topology_shape.rs (new) |
| EG-DECISION-ENGINE-R109 | T6 | BUILT | NOT_RUN | **ST-5** — Admissibility entailment stage: composed-schema digest in inputs, OWL derivation, TopologyAdmissible derivation, elimination — EG crates/eg-compute/src/assemble/topology/admissible.rs (new) |
| EG-DECISION-ENGINE-R110 | T6 | BUILT | NOT_RUN | **ST-6** — Model rows: one-hot width/rounds, lease/deadline/demand/verify/cap rows, precomputed tables, brute-force oracle ≤20 vars — EG crates/eg-compute/src/assemble/topology/{model.rs,tables.rs} (new), assemble/template.rs (call site) |
| EG-DECISION-ENGINE-R111 | T6 | BUILT | NOT_RUN | **ST-7** — AU convergence: topology.py asks the topology question; delete TopologyEngine.record_outcome/EMA and KGTeamComposer success-rate reuse; build ElasticTopologyAdmission from the plan; source gate bans Cypher TopologyTemplate writes — AU decide/consumers/topology.py, graph/{subagent_patterns,topology_engine,team_composer}.py, scripts/ gate |
| EG-DECISION-ENGINE-R112 | T6 | BUILT | NOT_RUN | **ST-8** — Capacity premise: CapacityStatus → headroom observations at the request priority. Infeasible{lease:*} naming — EG src/server/handlers/decide/capacity_premise.rs (new) |
| EG-DECISION-ENGINE-R113 | T6 | BUILT | NOT_RUN | **ST-9** — graph-os orchestration: evaluate → commit → publish with synthesis_evidence → all-or-nothing AcquireCapacity → KgDelegate. One re-decide on denial. Release on stop — graph-os graph_os/a2a/{routing,composition}.py, control_plane/runs/admission.py |
| EG-DECISION-ENGINE-R114 | T6 | BUILT | NOT_RUN | **ST-10** — Harness allowance: RunSpec.subagents: SubagentAllowance, a negotiation rule, adapter enforcement flags — AU agent_utilities/layers/{contracts,negotiation}.py, adapters/* descriptors |
| EG-DECISION-ENGINE-R115 | T6 | BUILT | NOT_RUN | **ST-11** — Continuation/stop: au.swarm.continue evaluate-only point, narrow-only options, StopRule evaluator over L5, ReleaseCapacity on narrow — AU decide/consumers/continuation.py (new), decide/points.py |
| EG-DECISION-ENGINE-R116 | T5 | QUEUED | NOT_RUN | Replace the engine facade with the EG client. Delete kg/core/{engine,engine_tasks (7,715),engine_ingestion,engine_memory,engine_task_query,engine_resolver,engine_transport,engine_lock,engine_breaker}.py, kg/facade.py, kg/orchestration/engine_*.py (4,887 LOC). |
| EG-DECISION-ENGINE-R117 | T6 | QUEUED | NOT_RUN | Series-expression operators DERIVE (lag/diff/ret/logret/rolling mean·std·sum·min·max·rank·corr/ewma/zscore/ratio/weighted sums) as checkpointed incremental kernels in eg-tsdb/src/derive; one kernel behind UQL DERIVE, SQL window UDFs and matching PromQL functions; feature predictive power (rolling rank IC, IC decay, IR with effective-N, bootstrap CI) as a DecisionEval FeatureSkill report; dedupe rolling_zscore/ewma (D2); Python reference goldens |
| EG-DECISION-ENGINE-R120 | T4 | BUILT | NOT_RUN | tests/test_pinned_reference_resolution.py::test_each_layer_is_anchored_on_its_own_admission_step red on main since d6baf6dfc: the gate counts two non-writing helpers (a read and a replay) as COMPONENT admission points. Decide the rule (admission = persisted write path only?) and fix gate or code; prove both ways |
| EG-DECISION-ENGINE-R126 | T6 | SPECIFIED | NOT_RUN | Capability coverage query: per task, the capability closure and the visible Agent Library templates and A2A agent cards covering each capability, with premises |
| EG-DECISION-ENGINE-R127 | T6 | SPECIFIED | NOT_RUN | Guardrail entailment query: governance rules, standards and shapes that apply to a task class, with entailment paths |

### 2026-10-11: R090.1 resident-budget acceptance

The native harness compiled the production scorer module with real cached `eg-types` and `serde` dependencies.
All six budget tests passed under Rust 1.96.0.
Tests cover the exact ceiling, each independent limit, zero declarations, and maximum-integer refusals.
The leaf covers declared budgets only; it does not establish measured resident-scorer latency or parent R090 acceptance.

The ceiling now refuses 10,000 microseconds, which exceeds the single-digit-millisecond contract.

R090.2 retains compiled execution acceptance. R090.3 retains measured resource acceptance. Both children remain specified until their checks pass.

## Amendments

### 2026-10-11: retain R093 served acceptance

The previous R093 rollup recorded only its provenance-type child, R093.1.
Its text explicitly left served export unbuilt, but the generated parent state was VERIFIED.
R093.2 now records that remainder with three planned children.
The original parent acceptance remains unchanged.
`export_gold_corpus` currently calls `generate_gold_set` and exports `SOURCE_IN_PROCESS` labels.
This correction claims no served implementation or runtime test result.

### Operation-selected job decoding (R063)

DecisionFit and DecisionEval result models derive from each operation's existing
Rust marker. Their typed decoders require an explicit request operation.
Unknown operations, wrong method identity, unexpected fields and another
operation's result shape fail closed. Nullable status and receipt results are
valid only for their corresponding operations. Heterogeneous JSON test payloads
have explicit type annotations, including fixtures that inject nested unknown
fields to exercise strict validation.

### 2026-10-11: repair the R083 option-count fixture

The large-option scorer fixture estimated 120,000 tokens but required more than 128,000.
Increase its option count from 4,000 to 5,000, yielding 150,000 estimated tokens.
Keep the label estimate, token budget, feature-width checks, and bounded-shortlist assertions unchanged.
This test repair changes no runtime behavior or acceptance state.
The bound test covers the large-option scorer fixture only; R083.2 still requires real OptionFacts integration.

## Retry allowance model

The dependency-free `topology/retry_allowance.rs` owns the checked local 1-to-0 transition. `capacity_commit.rs` re-exports its public types, preserving the existing caller path. Invalid counts and exhausted allowances fail without mutation. Durable consumption and handler admission remain separate requirements; this copyable model is not a shared authorization store.

## Canonical source contracts

`eg-types/src/solve` owns solver DTO definitions. Compute modules re-export those types.
The duplicate-declaration scan covers Rust files under `src` and `crates`.
The scan permits one declaration for each explicitly listed unrelated domain homonym.
`AgentComponentKind` documents every enum variant in its slot-or-catalog closure rule.
The consistency check compares enum variants and agent-slot references with that rule.

### 2026-10-11: variable-option calibration acceptance fixture

R084.1 gains a native integration fixture fitting one mixed-size resident scorer
and measuring served ECE and empirical coverage separately at 2, 3, 5, and 8 options.
The same fitted calibration is reused without size-specific temperature tuning.
This is deterministic synthetic test evidence, not a production-data claim.
Native execution remains pending after the local eg-types compile was SIGKILLed;
formatting and fixture checks do not satisfy native acceptance.
The test marker binds the existing fixture to R084.1.
The generated lifecycle records test existence, independently of execution results.

## Ingestion and schema completion boundaries

R079.1 proves the content taxonomy only. R079.2 through R079.5 retain the parent ingestion, duplicate/size, SQL, and retrieval-feedback obligations. R080.1 proves the typed provenance rule only; R080.2 requires the real six-rung integration fixture. R102.1 proves the typed activation sequence only; R102.2 requires a real shadow ingestion and approval-gated activation. These requirements use the existing source-ingestion, enrichment, and approved-schema owners. They introduce no second write authority. Their planned fixtures are not execution evidence.

## Retention boundaries

Library records retain their complete inputs indefinitely. Graph input retention uses the existing Blob CAS lifecycle.
Compaction starts at the committed-record age. Input retirement starts at the compaction age.
The implementation retains graph summaries indefinitely. R066.4 tracks the remaining record-expiry requirement.
R066 remains incomplete until that requirement has implementation and acceptance coverage.

### 2026-10-11: bounded topology oracle coverage for R110.1

Extend the assembly topology tests with a fixed-seed property fixture using five
mandatory components in each of two slots and varied width/round choices. Count
the actual returned model variables and certificate selection entries, including
an exactly twenty-variable case. Independently enumerate all topology tuples;
the ten candidate bits are mandatory, leaving at most sixteen feasible one-hot
combinations per fixture. Check feasibility and both topology objective levels,
including demand, capacity, deadline and token caps. Native execution remains
pending; formatting and standalone fixture arithmetic are not native acceptance.
The test binds R110.1 under the published spec standard.
Generated delivery state records test existence, not successful native execution.

### 2026-10-11: bounded solver completion telemetry (R072.1)

Emit one `eg_compute::solve` completion event after the actual search returns its
certificate. Report `solver_nodes`, `optimality_gap_available`, and, only when
both endpoints exist and checked subtraction yields a nonnegative value,
`optimality_gap`. The gap is the absolute scalarized minimization objective gap,
exported as an approximate f64 for telemetry; certificate integers stay exact.
Missing endpoints, invalid ordering, and arithmetic overflow omit the gap rather
than imply optimality. No model inputs, identifiers, selections, or tenant data
are recorded. Add helper and actual solver-path capture regressions without
changing search behavior or certificate serialization. Native solver-path
test was not run. A rustc test harness importing the exact production metrics
module with the existing tracing rlib passed all 3 helper/capture tests. This
command does not execute the native solver-entry regression. The regression
source is bound to R072.1; generated spec state and test execution are separate.

### 2026-10-11: typed decision provenance acceptance

R121.1 exercises the existing typed `PremiseRef` owner through assembly and replay.
The test varies misleading labels independently from premise classes.
R121.2 retains graph candidate provenance, record sealing, and replay integration.
Graph candidates currently discard typed Claim/Evidence provenance while retaining scalar features.
This assembly test does not establish graph-source acceptance.
Native execution remains pending through an authorized dependency route.
The existing scoped result is excluded because its dependency cache was outside the permitted task scope.
The full Cargo suite and hosted CI remain unrun for this change.

### 2026-10-11 amendment: bounded R073 storage acceptance

Add R073.1 and R073.2 for the existing served import path's durable per-component
outbox and competing head compare-and-set behavior. These child tests do not
complete R073: direct multi-intent receipt digest coverage and an owner-table /
current-format refusal audit remain separate acceptance work. Native execution
is reported separately from the existence of these acceptance tests.

## Retrieval refusal envelope (R029)

The served refusal test inspects `Response.error` and `Response.error_detail` separately.
The error code is `PARAMETER_INVALID`; the detail names `UNSUPPORTED_RETRIEVAL_PLAN` and the rejected option.
The test also requires an absent result.
This fixture covers registry refusal only; representative retrieval-plan selection requires the integration test described in R029.


### 2026-10-11: R090.2 compiled wire fixture

The acceptance driver reuses the authenticated Python client and bounded native server lifecycle.
The engine fits, evaluates, publishes, and executes its Rust option-attention head through real wire requests.
The driver compares fixed-point probability bytes across repeated requests and a durable engine restart.
The engine receives an empty executable search path and no inherited runtime configuration.
Linux syscall tracing rejects subprocesses, Python runtime loading, outgoing connections, and GPU device access.
The fixture checks repeat and restart consistency; it does not claim independent cross-build golden-vector equivalence.
The fixture waits for the tracing process before reading its complete output.
This fixture requires a prebuilt engine and Linux strace.
Native execution remains unrun until that fixture runs against the compiled artifact.
R090.3 retains all measured resource obligations.

## Unsupported topology commit boundary (R113.2.1)

DecisionCommit refuses solved plans with positive capacity demand before either owner writes.
The guard uses the declared DERIVATION_REJECTED code and preserves the existing tenant check.
Topology inputs alone do not trigger this guard. AgentAssemble continues to return advisory plans.
The handler fixture checks both owner files and the ordinary commit path.
Native handler execution remains pending. R113.2 still requires the shared transaction and recovery design.

## Synthetic aggregate separation (R122)

The served outcome aggregate excludes synthetic-origin decisions before counting labels and pooling rates.
The full fit, evaluate, publish, decide, and log fixture checks synthetic-only and mixed-origin aggregates.
Logged fit and evaluation keep their conservative cohort flag: any synthetic record marks the dataset synthetic.
This preserves synthetic-only evaluation and prevents mixed cohorts from publishing real-world calibration claims.
Inline datasets remain unverified and synthetic.
Native fixture execution remains pending; source bindings and formatting do not prove execution.

The reputation reader also excludes synthetic decisions before learning option reliability or scoring forecasters.
The query-enabled pipeline fixture checks forecaster metrics, reputation counts, and the learned reliability provider.

### R118.1.2 source reliability consumers

`crates/eg-epistemic/src/model.rs::AuthorityPolicy` stores a global `f64` reliability multiplier.
`src/server/handlers/query/explain_wire_a.rs::epistemic_status_wire` selects the default policy without reading source reputation.
`crates/eg-epistemic/src/ranking.rs::RetrievalCandidate` also requires a scalar reliability.
`src/server/handlers/query/explain_wire_b.rs::rank_by_provenance_wire` copies that scalar from the caller's request.
Neither served consumer resolves a governed source identity through the visible decision log.

`src/server/handlers/decide/stat_reputation.rs::learned_reliability` discards `Reputation::InsufficientHistory` and returns `None`.
`crates/eg-plan/src/exec/reliability.rs::reliability_of` interprets `None` as permission to retain the graph prior.
The result therefore loses the distinction between absent evidence and a visible sparse pool's abstention.
Dense UQL results also use `anchored_reliability` with the graph prior instead of the materialized pooled estimate.

A source-identity adapter must resolve subject kind, subject identity, and caller visibility before each consumer reads reputation.
The read contract must distinguish absent evidence, insufficient history, and an estimated reliability.
Each consumer must propagate the optional or typed result without coercing abstention into a scalar.
The consumers must share the materialized estimate instead of recomputing it from different priors.
`crates/eg-types/src/protocol/support_02.rs::AuthorityPolicyWire` and `support_03.rs::RetrievalCandidateWire` currently require scalar reliability fields.
Any public contract change must preserve compatibility and regenerate its derived artifacts.
Whole-query refusal, a NaN sentinel, and a helper-only test do not establish cross-surface abstention.

## HarnessConfig decision (R128)

Data flow: `Method::Decide` (`src/server/dispatch/router/decision_plane.rs:95`) → `handle_decide` (`src/server/handlers/decide/statistical.rs:11`) → `stat_decide::handle_decide` (`stat_decide.rs:48`) → `harness_config_outcome` (new, beside the `tool_subset_outcome` match at `stat_decide.rs:134`) → `seal` (`stat_decide.rs:372`) → `handle_decision_commit` → committed record.

Types, from the cross-repository brief:

- Input params: `model_profile_ref`, `task_class`, `usable_context_budget_tokens`, `priority`, and an optional `expected_trajectory_tokens`.
- Output `HarnessConfigAnswer`: `context_policy` with `soft_threshold`, `hard_threshold` and `recent_window_fraction`; `plan_scaffold`; `action_space` with `tool_subset`.
- Each part is `Decided` or `Abstained { missing_fact, conservative_default }`.

Attach point: the `ToolSubset` kind is the template.

- `QuestionKind::ToolSubset` is defined at `statistical.rs:95`.
- Its handler `stat_tool_subset.rs:33` returns `Some(outcome)` only for its own kind.
- `stat_decide.rs:134` seals that outcome in place of the scored ladder.
- `resolution_kind` and `evidence_class` are computed generically from the outcome (`stat_decide.rs:353-369`).
- `HarnessConfig` repeats this shape and adds no `Method` variant.

Decision rules, each a named default stated as a premise:

- Context policy: pressure is `expected_trajectory_tokens` over `contextWindowTokens`. The result is `none` only when overflow is impossible for the task class. Otherwise it is `elide_then_summarize` with soft 0.6, hard 0.85 and recent 0.3.
- Plan scaffold: `on` for `stops_early` (accuracy) and `over_verifies` (cost). `off` only for `balanced` with a measured no-gain ablation on the cited evaluation run.
- Action space: `shell_only` only when `shellProficiency` is `high` and the task class is `shellCentric`. Otherwise `tool_set`.
- A missing measured fact abstains for that part and names its conservative default: `elide_then_summarize`, `on`, `tool_set`.

Failure and refusal: an unknown profile reference, a negative budget or a wrong param type refuses with `StatisticalErrorCode::ParameterInvalid`, as `stat_tool_subset.rs:41-80` does. A `ToolSubsetRefusal` (`tool_subset.rs:39`) abstains for the `tool_subset` field only.

Reused:

- The decision ladder and the `seal` path.
- `smallest_covering_subset` (`tool_subset.rs:59`).
- `AgentLibraryStore` reads (`candidates.rs:415`).
- `resolve_policy` for overrides.
- `StatisticalOutcome::Advisory` and `::Abstained` (`outcome.rs:101`, `outcome.rs:107`).
- `ObservationRef` (`facts.rs:142`).

New: the question kind, the request and answer types, the three pure rule functions, the handler, the SHACL shapes and the generated-contract entries.

Decisions:

- One `Advisory` outcome with `<part>:<value>` options. Rejected alternative: a new `StatisticalOutcome` variant. That variant forces a change to every exhaustive match in `stat_decide.rs:353-369` and to the sealed digest.
- Per-part abstention rather than whole-answer abstention. A caller can then apply the decided parts and the named defaults for the rest.
- Rule thresholds live in `HarnessDefaults` and tenant policy. Rejected alternative: constants in the rule functions. A measured crossover must replace them without a code change.
- `expected_trajectory_tokens` is an optional caller param. The brief names no source for it. The code holds no stored trajectory estimate. A stored per-task-class estimate needs a later row.
- The record stores the answer as scored options. The code shows no typed answer field on `StatisticalDecisionRecord`. This design adds none.

Contract: `QuestionKind` derives `JsonSchema`, so `gen_contract` lists the kind in `contract/schemas/method.request.json` and the `result.*.json` schemas, as it lists `tool_subset` today. Python and JS/Go clients regenerate from those schemas.

Cross-repository rows:

- The `AU-CONTROL` row for the signed dispatch envelope `harness_config` calls this decision and signs the committed answer.
- The `AU-HARNESS` row for the evaluation runner writes the measured facts that `EG-TYPED-PACKS-R048.3` and `R048.4` store.
- The `GRAPHOS-A2A` row for admission refuses a task without a committed `HarnessConfig` answer.
- The `SDK-CONNECTOR-CONTROL` row for tool `read_only` and `side_effects` facts feeds the tool-subset candidates.

### Committed pack receipt binding (R073.4)

The signed served fixture commits three components and one pack intent.
The fixture reads the typed replay receipt, committed batch and physical outbox through scoped owner APIs.
An independent digest oracle checks every intent and header against the committed receipt.
The fixture retries through `commit_pack_write` with a fresh nonce and refuses any new owner application.
The handler catalog shortcut answers repeated imports before owner admission; it does not exercise typed receipt replay.
The fixture checks that shortcut separately.

### Production open and withdrawal acceptance

R073.3 uses `AgentLibraryStore::open`, the production entry point in `src/server/persistence/agent_library.rs`.
The current Agent Library registry declares 21 owner tables, including five ConnectorPack tables.
Those tables are heads, members, imports, body holders, and bindings.
The registry, layout digest, and declared predecessors govern compatibility; a standalone format-v3 label does not.
Fresh-open tests must read every pack table, inspect an empty catalog, and repeat after reopen.
Genuine predecessor files must fail through this entry point without changing their bytes.

R073.5 reuses the served transitive-chain and diamond-pin tests in `content_model.rs`.
A signed Search test must exclude a withdrawn tool while retaining a published positive control.
Signed Content must still return the withdrawn tool's historical pinned body.
A new dependency on that withdrawn tool must fail with `COMPONENT_WITHDRAWN`.
The Publish request must pass structural validation before served admission.
Assert the stable refusal code separately from its human-readable detail.
`AgentComponent` must declare `COMPONENT_WITHDRAWN` in its exact operation error
set so the served boundary preserves that owner refusal. Unrelated methods must
continue to refuse that code; a broad error-family allowance is unnecessary.

### Competing pack import acceptance (R073.2)

Upload both archives before racing the two actual Import requests over one
expected head. Preserve the one-winner, `PACK_HEAD_CONFLICT`, single revision
advance, committed-head identity, and zero-loser-effects assertions, including
a stale-head retry. Served refusal diagnostics must retain the full response.
Concurrent `BlobBegin` admission is a separate unresolved concern; isolating
pack Import admission does not establish a production Blob CAS fix.

## Invariant gate on promoted rules (R129)

Current delivery is limited to EG-DECISION-ENGINE-R129.2: the typed
`RulePromotionVerdict` in `crates/eg-types/src/decision/guardrail.rs` contains
`Promotable` and `RejectedInvariant { invariant_iri: String }`, following the
existing string IRI convention and snake_case serde representation. Positive
roundtrips and malformed-payload refusals bind to the literal child spec ID.
This enum-only delivery does not implement the invariant gate.

The proposed R034 attach point is not established. The current R034 test at
`crates/eg-epistemic/src/query.rs:661` proves a read-only evidence-removal
`MinimalFlipSet` proposal; it does not promote a rule. `GuardrailRule` in
`crates/eg-types/src/decision/guardrail.rs:62` contains task applicability
metadata (`rule_id`, `applies_to_class`, `source_generation`), with no rule
predicate or contradiction semantics. `entailed_guardrails` establishes task
applicability only, so it cannot implement an invariant contradiction check.

EG-DECISION-ENGINE-R129 and children .1/.3/.4/.5/.6 remain open. The promotion
owner and rule/invariant semantics must be established before wiring a pure
check, confidence ordering, audit persistence, or served acceptance. The
intended invariant check must precede any confidence threshold; consensus
cannot grant a bypass. No new promotion machinery is introduced by .2.

### Current harness producer boundary (R128.2, R128.3, R128.7)

`harness_config` owns `HarnessConfigRequestBody` with params named
`model_profile_ref`, `task_class`, `usable_context_budget_tokens`, `priority`
and optional `expected_trajectory_tokens`. References reuse the opaque strings
of `TypedValue::Iri`; this body adds no IRI syntax parser. Token counts reuse
the nonnegative `u32` range of `ToolSubsetRequestBody`, including zero; decoding
rejects negatives, overflow and non-integers. Priority reuses `LeasePriority`.
All four required fields reject omission and null; an absent or null trajectory
decodes to `None` and is omitted on serialization. Unknown fields are refused.

The module also owns `HarnessConfigAnswer`, per-part `HarnessPart` decisions,
and pure `decide_action_space`. Fractions reuse checked `UnitRationalWire` with
a required Q32 denominator; tool subsets and missing-fact lists use `BoundedVec`.
The selector accepts optional measured proficiency and optional resolved task
boolean, refuses invalid evaluation references, and names every missing input
in a conservative `tool_set` abstention. Abstentions reject empty missing-fact
lists and blank names during decode and explicit validation. All twelve presence/value combinations
are covered. QuestionKind, profile resolution, handlers, persistence and served
acceptance remain open under their separate leaves.

## Calibrated candidate ranking (R130)

Data flow: `Decide` wire method -> `stat_decide` (`crates/eg-compute/src/assemble/conclude.rs`) -> `rank` (`crates/eg-epistemic/src/ranking.rs:116`) -> options and premises in the record.

Types: `RankedCandidate`, `TieBreakPolicy`. Attach point: `rank`. Production caller: none exists today; the Wiring row creates it in the statistical `Decide` path. Failure behavior: an uncalibrated candidate abstains with `uncalibrated_candidate`.

Reused: `RankWeights` (`ranking.rs:47`) and `evidence_quality`. New: the calibrated-first order and the item-level tie-break.

Decision: tie-break by an item-level score. Rejected alternative: share one score across colliding codes, because it hides real differences between items.

Cross-repository: `AU-` rows may show the premises; they own no ranking rule.

References:
- arXiv:2606.08604 — Tikhonovich et al., "Gryphon: A Unified Architecture for Semantic-ID Generation and Item-Level Scoring in Industrial Recommendations" (2026)

## Limit guard pipeline (R131)

Data flow: `Decide` wire method -> `conclude.rs` chokepoint -> `check_limits` (`crates/eg-types/src/decision/limit_guard.rs`) -> abstention `limit_breach` plus an observation node linked to the signal.

Types: `LimitRule`, `LimitBreach`. Attach point: the stored-rule read at `guardrail.rs:201`, plus the chokepoint in `conclude.rs`. Production caller: the Wiring row creates the second call site. Failure behavior: a breach abstains; a missing rule set admits nothing new.

Reused: `GuardrailRule` storage and `guardrail_entailment_query` (`guardrail.rs:201`). New: numeric limit metrics and the breach observation.

Decision: refuse at the decision chokepoint, so no caller can skip the check. Rejected alternative: check inside each caller, because one missed caller bypasses the limit.

Cross-repository: `AU-` rows may surface the refusal; the owning policy gate keeps enforcement.

## Retrieved-entry trust (R132)

Data flow: `Decide` wire method -> `handle_decide` (`src/server/handlers/decide/stat_decide.rs:48`) -> `decide_one` (`crates/eg-compute/src/assemble/conclude.rs:93`) -> `admit_entries` at `conclude.rs:180` -> premises -> record with refused entries.

Types: `TrustTier`, `EntryTrust`, `AbstainReason::PoisonedEntry`. Attach point: premise construction at `conclude.rs:180`. Production caller: the Wiring row creates the call site. Failure behavior: a refused entry never becomes a premise; no admitted entry for a required premise abstains.

Existing code: `crates/eg-types/src/decision/record/premise.rs:17`, `premise.rs:49`, `premise.rs:89`, `crates/eg-types/src/decision/ingestion_ladder.rs:46`, `crates/eg-types/src/decision/derivation.rs:310`, `crates/eg-modality/src/provenance.rs:22`. Reachable today: no for tier and poison check; `weakest` at `conclude.rs:192` is reachable but grades premise class only. Search terms used with no match: trust_tier, TrustTier, trust_level, poison, quarantine, untrusted.

Decision: gate entries before they become premises, so the record never cites a refused entry. Rejected alternative: filter after the solver, because a refused entry must not shape the shortlist.

Cross-repository: `AU-` memory callers may attach a tier to an entry; owner prefix `AU-MEMORY`, contract `EntryTrust`.

References
- No paper is cited.

## Run trace and runtime quota (R133)

Consumers: AU-CONTROL-R040 (run trace), AU-CONTROL-R041 (runtime quota).

Data flow: `Decide` wire method -> `handle_decide` (`src/server/handlers/decide/stat_decide.rs:48`) -> `runtime_quota_outcome` at `stat_decide.rs:134` -> `decide_runtime_quota` (`crates/eg-types/src/decision/run_governance.rs`, new) -> sealed `Advisory` with one option per value. Trace: harness run end -> `RunTraceRecord` -> evidence participant (`crates/eg-types/src/outcome_bundle.rs:56`) -> `RunTrace` node linked by `project` (`crates/eg-durable/src/project.rs:68`).

Types: `RunTraceRecord`, `RuntimeQuota`, `QuotaDefaults`, `QuestionKind::RuntimeQuota`. Attach point: the dispatch match at `stat_decide.rs:134`. Production caller: the Wiring row creates the match arm. Failure behavior: an over-ceiling request refuses; a missing fact abstains to the defaults.

Existing code: `crates/eg-types/src/outcome_bundle.rs:56`, `crates/eg-durable/src/project.rs:68`, `crates/eg-core/ontology/orchestration-v1.ttl:250`, `crates/eg-types/src/decision/topology.rs:75`. Reachable today: no for the quota decision and for an agent-run trace write; the `RunTrace` class exists in the ontology only. Search terms used with no match: `quota`, `wall_time`, `process_limit`, `memory_limit`, `RuntimeQuota`. Moves from agent-utilities: none named.

Decision: the engine fixes the numbers and the harness enforces them, so a tenant policy changes limits without a harness release. Rejected alternative: constants in the harness, because they cannot differ per tenant or task class.

Cross-repository: `AU-CONTROL-R041` receives `RuntimeQuota` and enforces it; `AU-CONTROL-R040` emits the run events that become the trace.

References
- No paper is cited.

## Task planning and selection (R134)

Consumers: AU-CONTROL-R042 (tool selection), AU-CONTROL-R043 (decomposition and routing). The skill-library plan has no named consumer yet.

Data flow: `Decide` wire method -> `handle_decide` (`src/server/handlers/decide/stat_decide.rs:48`) -> `task_plan_outcome` / `tool_relevance_outcome` at `stat_decide.rs:134` -> `plan_over_skills`, `decompose_task`, `rank_tools_by_relevance` (`crates/eg-types/src/decision/task_plan.rs`, new) -> `smallest_covering_subset` (`tool_subset.rs:58`) -> sealed answer.

Types: `Subtask`, `AggregationOp`, `SkillStep`, `TaskPlan`. Attach point: the dispatch match at `stat_decide.rs:134`. Production caller: the Wiring row creates the arms. Failure behavior: an uncovered subtask or empty catalog abstains; a cycle refuses.

Moves from agent-utilities: `agent_utilities/capabilities/fleet_tool_search.py:54`, `agent_utilities/graph/planning/__init__.py:165`, `agent_utilities/decide/consumers/task_planner.py:211`.

Existing code: `crates/eg-types/src/decision/statistical/tool_subset.rs:58`, `crates/eg-types/src/decision/topology.rs:110`, `crates/eg-compute/src/assemble/topology.rs:54`, `crates/eg-compute/src/assemble/conclude.rs:99`, `src/server/handlers/decide/stat_tool_subset.rs:33`. Reachable today: yes for the covering subset (`tool_subset_outcome` is matched at `stat_decide.rs:134`) and for topology `pose`; no for relevance search, decomposition with an independence flag and the skill plan. Search terms used with no match: `skill_plan`, `SkillPlan`, `plan_skills`, `decompose`, `aggregation`. The existing `EG-DECISION-ENGINE-R045` owns the covering subset and is reused, not extended, because it selects by coverage and has no relevance score.

Decision: one `TaskPlan` question kind returns subtasks, skills and operator together, so the harness applies one record. Rejected alternative: separate kinds per step, because the harness must stitch plans and loses the shared premises.

Cross-repository: `AU-CONTROL-R042` and `AU-CONTROL-R043` receive the tool list and the plan and apply them.

References
- arXiv:2605.15132 — Rose et al., "APWA: A Distributed Architecture for Parallelizable Agentic Workflows" (2026)
- arXiv:2606.18051 — Gao, "Compositional Skill Routing for LLM Agents: Decompose, Retrieve, and Compose" (2026)

## Compiled program store (R135)

Consumers: AU-HARNESS-R023.

Data flow: harness run end -> `compile_program` (`crates/eg-types/src/decision/program_store.rs`, new) -> commit through `handle_decision_commit` -> `AgentLibraryStore`; later `Decide` wire method -> `handle_decide` (`src/server/handlers/decide/stat_decide.rs:48`) -> `program_lookup_outcome` at `stat_decide.rs:134` -> `lookup_program` -> stored program.

Types: `CompiledProgram`, `ProgramStep`, `MAX_PROGRAM_STEPS`. Attach point: the dispatch match at `stat_decide.rs:134`. Production caller: the Wiring row creates the arm. Failure behavior: an unsuccessful run or an overlong trace refuses; a missing program abstains.

Moves from agent-utilities: `agent_utilities/observability/replay_engine.py:48`.

Existing code: `src/server/handlers/decide/candidates.rs:415`, `src/server/handlers/decide/stat_tool_subset.rs:33`. Reachable today: no. `crates/eg-tsdb/src/derive/mod.rs:152` defines a `Program` for derived series, which is a different type. Search terms used with no match: `task_signature`, `TaskSignature`, `compiled_program`, `store_program`, `lookup_program`, `MAX_PROGRAM_STEPS`.

Decision: the engine owns the program store, so every harness replica shares one program per signature. Rejected alternative: a harness-local cache, because replicas compile and store duplicates.

Cross-repository: `AU-HARNESS-R023` replays the returned program; it does not store or choose one.

References
- arXiv:2606.17929 — Li, "PreAct: Computer-Using Agents that Get Faster on Repeated Tasks" (2026)

## Evolution round verdict and stage model allocation (R136)

Consumers: AU-HARNESS-R025 (round verdict), AU-SEMANTIC-R029 (stage allocation).

Data flow: `Decide` wire method -> `handle_decide` (`src/server/handlers/decide/stat_decide.rs:48`) -> `round_verdict_outcome` / `stage_allocation_outcome` at `stat_decide.rs:134` -> `decide_round_verdict` / `allocate_stage_models` (`crates/eg-types/src/decision/evolution_round.rs`, new) -> sealed answer.

Types: `PredictionDeclaration`, `RoundVerdict`, `EvolutionDefaults`, `StageAllocation`, `RunBudget`. Attach point: the dispatch match at `stat_decide.rs:134`. Production caller: the Wiring row creates the arms. Failure behavior: an empty prediction abstains; an unaffordable budget refuses.

Moves from agent-utilities: `agent_utilities/decide/consumers/routing.py:58` (allocation). The verdict has no code to move.

Existing code: `src/server/handlers/decide/stat_tool_subset.rs:33`, `src/server/handlers/decide/candidates.rs:415`. Reachable today: no. `EG-DECISION-ENGINE-R035` owns routing among models, but it chooses one option per request and has no budget across stages, so this rollup is separate. Search terms used with no match: `falsified`, `flip_fraction`, `PredictionVerdict`, `stage_budget`, `ModelAllocation`, `model_per_stage`.

Decision: the engine returns a verdict and an allocation as records, so the harness keeps or reverts a change by reading one field. Rejected alternative: the harness compares outcomes itself, because each harness applies a different threshold.

Cross-repository: `AU-HARNESS-R025` keeps or reverts on the verdict; `AU-SEMANTIC-R029` applies the allocation and the limit.

References
- arXiv:2604.25850 — Lin et al., "Agentic Harness Engineering: Observability-Driven Automatic Evolution of Coding-Agent Harnesses" (2026)
- arXiv:2604.07725 — Maheswaran et al., "Squeeze Evolve: Unified Multi-Model Orchestration for Verifier-Free Evolution" (2026)

## Context budget and parallel-map limits (R137)

Consumers: AU-CONTEXT-R004 (context budget and plan), AU-HARNESS-R026 (parallel-map limits).

Data flow: `Decide` wire method -> `handle_decide` (`src/server/handlers/decide/stat_decide.rs:48`) -> `context_budget_outcome` at `stat_decide.rs:134` -> `decide_over_window` (`crates/eg-types/src/decision/context_budget.rs`, new) -> sealed answer carrying policy, budget, limits and plan.

Types: `ParallelMapLimits`, `WindowBudget`, `OverWindowPlan`; `ContextPolicy` is reused. Attach point: the dispatch match at `stat_decide.rs:134`. Production caller: the Wiring row creates the arm. Failure behavior: an exhausted budget or an item count over the limit refuses; a missing window abstains to the default.

Moves from agent-utilities: `agent_utilities/rlm/config.py:60`, `agent_utilities/knowledge_graph/retrieval/context_knapsack.py:422`.

Existing code: `crates/eg-types/src/decision/statistical/harness_config.rs:81`, `crates/eg-compute/src/assemble/eliminate.rs:97`, `crates/eg-compute/src/assemble/eliminate.rs:138`, `src/server/handlers/decide/candidates.rs:415`. Reachable today: yes for the window elimination in `eliminate.rs:97`, which discards a candidate whose window is too small; no for the over-window plan and the limits. `EG-DECISION-ENGINE-R128` owns the context policy and thresholds and is reused; this rollup adds the recursive plan and the limits that `EG-DECISION-ENGINE-R128` does not cover. Search terms used with no match: `over_window`, `window_fraction`, `max_items`, `OverWindow`.

Decision: the engine returns the plan and the limits together, so the harness never exceeds a budget it did not receive. Rejected alternative: limits set in harness configuration, because they cannot follow the task class.

Cross-repository: `AU-CONTEXT-R004` assembles context from the plan; `AU-HARNESS-R026` enforces the limits in fan-out.

References
- arXiv:2512.24601 — Zhang et al., "Recursive Language Models" (2026)

## Red-team records and probe verdict (R138)

Consumers: AU-QUAL-R009.

Data flow: `Decide` wire method -> `handle_decide` (`src/server/handlers/decide/stat_decide.rs:48`) -> `probe_verdict_outcome` at `stat_decide.rs:134` -> `select_scorers`, `decide_probe_verdict` (`crates/eg-types/src/decision/red_team.rs`, new) -> sealed answer.

Types: `RedTeamAttack`, `RedTeamTransform`, `RedTeamScorer`, `ProbeVerdict`. Attach point: the dispatch match at `stat_decide.rs:134`. Production caller: the Wiring row creates the arm. Failure behavior: a category with no scorer abstains; a missing score is inconclusive.

Moves from agent-utilities: `agent_utilities/harness/reliability_scorers.py:1`.

Existing code: `src/server/handlers/decide/candidates.rs:415`, `src/server/handlers/decide/stat_tool_subset.rs:33`. Reachable today: no. Search terms used with no match: `red_team`, `redteam`, `RedTeam`, `jailbreak`, `prompt_injection`, `attack`, `score_probe`.

Decision: the engine decides the verdict from stored records, so every harness reads one result for one probe. Rejected alternative: scoring in the harness, because the records and thresholds differ per harness.

Cross-repository: `AU-QUAL-R009` runs the probe, submits the scorer results and reports the verdict.

References
- arXiv:2605.04019 — Dheekonda et al., "Redefining AI Red Teaming in the Agentic Era: From Weeks to Hours" (2026)
