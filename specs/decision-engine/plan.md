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

Typed question adapters translate these requests into the same legal-set/evidence pipeline. The requests are retrieval, lane, enrichment, entity-resolution, schema-mapping, routing, risk, topology and recommendation. `DecideText` is a dedicated UQL-like front end with quoted candidate blocks and typed parameters. Malformed syntax returns a structured span/error. It never falls through to ordinary query parsing. Topology adds bounded integer shape variables (width, rounds, role, capacity) and a composed-schema digest. It then outputs a proposed `AgentGraph`. Capacity acquisition and execution remain separate atomic operations. Entity resolution emits a proposed link and confidence/abstention. It never emits an automatic identity merge. Finance recommendations are informational snapshots with strategy version, evidence and horizon. They cannot place trades. Graph-sourced record views must apply the same RLS as their candidate source.

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
| EG-DECISION-ENGINE-R049 | T4 | BUILT | NOT_RUN | WP A3 — Solver: preprocess; find a greedy incumbent; use branch-and-bound and a Lagrangian/dual covering bound. Set a node budget, emit a certificate, and check it with verify_certificate. List templates, run no-good cuts, and compare with a brute-force oracle |
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
| EG-DECISION-ENGINE-R080 | AU-batch | BUILT | NOT_RUN | **Ingestion cost ladder with provenance** mirrors the approved Decide ladder. Rung 0 uses AST and records EXTRACTED. Rung 1 uses symbol/type resolution and records INFERRED. Rung 2 uses statistical/community derivation and records DERIVED. Rung 3 uses classical ML/NER and records MODELED. Rung 4 uses embeddings and records EMBEDDED. Rung 5 uses an LLM and records ASSERTED. Run a higher rung only when every lower rung abstains. Never overwrite a deterministic fact with a higher rung. This preserves Decide’s monotone-safety rule |
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
| EG-DECISION-ENGINE-R091 | T4 | BUILT | NOT_RUN | **Bit-reproducible decisions via fixed-point arithmetic — the strongest reason to build rather than borrow.** EG records decisions as DecisionRecord graph objects. A nondeterministic scorer makes records unreproducible. That defeats the purpose of recording them. Jev is hosted and irreproducible. Laya uses 421M floats; cua-s1 uses PyTorch floats. Neither offers this at any size. eg-numeric’s detkernel enforces a no-f64-transcendental discipline for this reason. A 2-layer encoder with an attention head using FIXED-POINT INTEGER arithmetic produces bit-identical results across machines and time |
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

R090.2 has scoped compiled-runtime acceptance in the successful Python job of
Release run 38148086774. R090.3 has the paired native receipts linked by the test
specification; the standalone run meets its measured limits. Neither result
claims that every served parent path or the complete Release workflow passed.

## Amendments

### 2026-10-12: context pressure without treating estimates as bounds (R128.5)

Compute pressure from the caller's trajectory estimate and the profile window
using unsigned Q32 arithmetic. Pressure can exceed one; never clamp it.
An estimate, including zero, does not prove that overflow is impossible.
The pure rule permits `none` only when a hard bound on total task tokens fits
the window. The bound must cover the whole context, including generated and tool
tokens; an observed maximum is insufficient. Contradictory estimates refuse.
Missing windows or estimates abstain with `elide_then_summarize`.
The rule carries checked `HarnessDefaults` thresholds supplied by its caller.
R128.4 remains open for tenant-policy resolution and threshold premises.
No hard-bound storage, served handler, or client acceptance is claimed here.

### 2026-10-11: measured plan-scaffold selection (R128.6)

Implement the missing pure plan-scaffold rule beside the existing harness
action-space rule. Early stopping selects `on` for accuracy; over-verification
selects `on` for cost. Balanced stopping selects `off` only with a measured
no-gain ablation. Missing required evidence abstains with conservative `on`.
The result retains the rule reason and exact evaluation references; malformed
references are refused through `MeasuredFact::validate`. Native tests qualify
this producer only. Profile lookup, served dispatch, committed premises and
generated-client acceptance remain the separate, open R128 wiring leaves.

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

### Resident scorer resource measurement (R090.3)

The benchmark lives in `crates/eg-numeric/benches/decision_resident_scorer.rs`, beside the production scorer. It reuses the promotion fit recipe and maximum legal head dimensions at 256 rows, timing individual production forward calls with Linux thread CPU time under the existing release profile. Whole-runner peak RSS is a conservative upper bound: above 8 MiB leaves scorer-specific memory inconclusive. Runner executable size is reported separately from server footprint. Acceptance requires an external receipt of the effective optimized build and passing measured limits; source publication alone does not verify the requirement.

Native measurement on 2026-10-11 met both parameter and CPU latency limits but
reported 124,788,736 bytes of whole-runner peak RSS against an 8 MiB ceiling.
That first Cargo-launched result is inconclusive and remains recorded. A direct
run of the identical executable subsequently met all measured limits, with
2,850,816 bytes peak RSS independently corroborated by external time. The test
specification links both structured receipts and exact release provenance. The
launch-dependent difference is observed; its precise cause is not established.

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

Data flow: `Decide` wire method -> `handle_decide` (`src/server/handlers/decide/stat_decide.rs:48`) -> `runtime_quota_outcome` at `stat_decide.rs:134` -> `decide_runtime_quota` (`crates/eg-types/src/decision/run_governance.rs`, new) -> sealed `Advisory` with one option per value. Trace: harness run end -> new `RunTraceRecord` adapter -> served `CommitWorkItemResult` terminal outcome extension -> `lower_terminal_outcome_extensions` (`src/server/mutation_batch/terminal_outcome.rs:22`) -> `apply_commit_work_item_result_row` (`src/redb_store/work_item/commit.rs:211`) -> receipt materialization after the successful fenced terminal CAS (`commit.rs:343`) in the same transaction. New produced-link materialization reuses the pure descriptors from `project` (`crates/eg-durable/src/project.rs:68`); that function performs no writes. `Decide` remains read-only and cannot persist the trace. `outcome_bundle.rs:56` declares participant domains, not a write call.

Types: `RunTraceRecord`, `RuntimeQuota`, `QuotaDefaults`, `QuestionKind::RuntimeQuota`. Attach point: the dispatch match at `stat_decide.rs:134`. Production caller: the Wiring row creates the match arm. Failure behavior: resolve each effective default D and hard tenant ceiling C with D <= C. `decide_runtime_quota(task_class, policy, request)` first refuses any requested R > C, then returns min(D, R), or D on omission. Increases above D through C are ignored; lower requests tighten. A missing task class abstains to effective defaults under C only after the ceiling check.

Existing code: `crates/eg-types/src/outcome_bundle.rs:56`, `crates/eg-durable/src/project.rs:68`, `crates/eg-core/ontology/orchestration-v1.ttl:250`, `crates/eg-types/src/decision/topology.rs:75`. Reachable today: the fenced terminal outcome extension and receipt writes already exist. RuntimeQuota dispatch, RunTraceRecord adaptation, and produced-link materialization are new work. The end-to-end fixture queries Decide first, commits the summary through CommitWorkItemResult second, then queries the persisted produced link; it also proves invalid fences write neither trace nor link. Search terms used with no match: `quota`, `wall_time`, `process_limit`, `memory_limit`, `RuntimeQuota`. Moves from agent-utilities: none named.

Decision: the engine fixes the numbers and the harness enforces them, so a tenant policy changes limits without a harness release. Rejected alternative: constants in the harness, because they cannot differ per tenant or task class.

Cross-repository: `AU-CONTROL-R041` receives `RuntimeQuota` and enforces it; `AU-CONTROL-R040` emits the run events that become the trace.

References
- No paper is cited.

## Task planning and selection (R134)

Consumers: AU-CONTROL-R042 (tool selection), AU-CONTROL-R043 (decomposition and routing). The skill-library plan has no named consumer yet.

Data flow: `Decide` wire method -> `handle_decide` (`src/server/handlers/decide/stat_decide.rs:48`) -> `task_plan_outcome` / `tool_relevance_outcome` at `stat_decide.rs:134` -> `plan_over_skills`, `decompose_task`, `rank_tools_by_relevance` (`crates/eg-types/src/decision/task_plan.rs`, new) -> `smallest_covering_subset` (`tool_subset.rs:58`) -> sealed answer.

Types: `Subtask`, `AggregationOp`, `SkillStep`, `TaskPlan`. Attach point: the dispatch match at `stat_decide.rs:134`. Production caller: the Wiring row creates the arms. Failure behavior: an uncovered subtask or empty catalog abstains; a cycle refuses.

Moves from agent-utilities: `agent_utilities/capabilities/fleet_tool_search.py:54`, `agent_utilities/graph/planning/__init__.py:165`, `agent_utilities/decide/consumers/task_planner.py:211`.

Existing code: `crates/eg-types/src/decision/statistical/tool_subset.rs:58`, `crates/eg-types/src/decision/topology.rs:110`, `crates/eg-compute/src/assemble/topology.rs:54`, `crates/eg-compute/src/assemble/conclude.rs:99`, `src/server/handlers/decide/stat_tool_subset.rs:33`. Reachable today: yes for the covering subset (`tool_subset_outcome` is matched at `stat_decide.rs:134`) and for topology `pose`; no for relevance search, decomposition with an independence flag and the skill plan. Search terms used with no match: `skill_plan`, `SkillPlan`, `plan_skills`, `decompose`, `aggregation`. The existing `EG-DECISION-ENGINE-R045` owns the covering subset and is reused unchanged. `rank_tools_by_relevance(query, catalog, required_capabilities, context_budget_tokens, policy)` first applies relevance_floor, descending relevance with tool-id ties, and top_k. It passes the retained candidates and explicit capability/budget inputs to R045, which selects by uncovered-capability gain per token, then lower token cost and tool id. It preserves coverage/budget refusals; it does not optimize relevance. Acceptance includes a retained lower-relevance coverage winner and loss of required coverage after filtering.

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

Types: `PredictionDeclaration`, `RoundVerdict`, `EvolutionDefaults`, `StageAllocation`, `RunBudget`. Attach point: the dispatch match at `stat_decide.rs:134`. Production caller: the Wiring row creates the arms. Failure behavior: an empty prediction abstains; an unaffordable budget refuses. Evaluate declared-direction and opposite-direction thresholds independently. Only the declared threshold met gives `verified`; only the opposite threshold met gives `falsified`; both or neither gives `unverified`. At flip_fraction 0.5 a five/five split is `unverified`, with an explicit simultaneous-threshold test.

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

## R137 package-selection contract

This extends the existing R137 owner. The first six children retain their acceptance. R137.7–.12 specify the certified package producer required by [AU-CONTEXT-R004](https://github.com/Knuckles-Team/agent-utilities/blob/50c313ab01871ff7e10aee2a9d0254fa55d71f07/specs/agent-context-and-finance/requirements.md#L8). Acceptance covers exact-token multi-resolution selection within window, cost and latency limits. It also requires preserved cited ranges/source ids and abstention when no admissible package fits. Existing R128 policy rows are dependencies and are not changed. R045 tool set-cover and EG-INGEST-WORKER-R003.2 relevance selection do not own this optimization.

Source inventory (EG `fe6982b29d0b60ad2a10092c959c238eeb74e919`, AU `50c313ab01871ff7e10aee2a9d0254fa55d71f07`):

- AU [Capacity.tokens, context_knapsack.py:98](https://github.com/Knuckles-Team/agent-utilities/blob/50c313ab01871ff7e10aee2a9d0254fa55d71f07/agent_utilities/knowledge_graph/retrieval/context_knapsack.py#L98) computes window minus reserved output, money/price, latency times throughput and caller ceiling. Its current truthiness checks omit unknown/zero rates. The certified contract explicitly distinguishes known zero from unknown under R018. Capacity rounding must stay within the original physical ceilings. Source `_whole` uses a float tolerance. That tolerance cannot permit exceeding a ceiling.
- AU [knapsack_model:161](https://github.com/Knuckles-Team/agent-utilities/blob/50c313ab01871ff7e10aee2a9d0254fa55d71f07/agent_utilities/knowledge_graph/retrieval/context_knapsack.py#L161) emits at-most-one resolution groups, token capacity and lexicographic net-value/token objectives. `_net_value:157` uses scale 1,000,000 and ties-to-even integer rounding. [fit_to_budget:422](https://github.com/Knuckles-Team/agent-utilities/blob/50c313ab01871ff7e10aee2a9d0254fa55d71f07/agent_utilities/knowledge_graph/retrieval/context_knapsack.py#L422), called at `context_compiler.py:1128`, builds full/summary slices and applies their selected resolutions. `select_slices:272` can currently use an explicitly uncertified greedy fallback; that path is not proof of the new producer contract.
- EG [stat_decide.rs:134](https://github.com/Knuckles-Team/epistemic-graph/blob/fe6982b29d0b60ad2a10092c959c238eeb74e919/src/server/handlers/decide/stat_decide.rs#L134) dispatches `tool_subset_outcome`; it has no `ContextBudget` arm. `QuestionKind` at `crates/eg-types/src/decision/statistical.rs:58` also lacks that variant. The planned records extend the already specified `context_budget.rs`, not a second selector/store.
- EG [solve/mod.rs:1](https://github.com/Knuckles-Team/epistemic-graph/blob/fe6982b29d0b60ad2a10092c959c238eeb74e919/crates/eg-compute/src/solve/mod.rs#L1) exposes integer model/constraint primitives, `solve` and independent `verify`. R014's existing assembly lowering at `crates/eg-compute/src/assemble/model.rs:96` is assembly-specific: reuse its constraint model/primitives, not an assumed generic package call. R004/R049 verification is `crates/eg-compute/src/solve/verify/mod.rs:23`; R011 bounds deterministic work, and R018 prevents false confidence from unknown facts. None of these reuse dependencies proves package wiring.

The planned flow starts at authorized served `Decide(ContextBudget, package)` with existing tenant/policy/candidate checks. Package lowering produces EG's bounded 0-1 model. `solve` and independent `verify` produce a sealed package result. AU-CONTEXT-R004 applies the result with citations intact. Keep package token totals distinct from the whole serialized prompt. Package certification cannot assert full-prompt fit, a latency service guarantee, or completion of recursive decomposition. Monetary/latency totals mean input-token cost and processing estimates under the bound rates. They do not silently absorb other charges or runtime overhead. Optional absent ceilings do not invent limits; active ceilings require their rate facts. A known zero price imposes no monetary token restriction. Known zero throughput with an active latency ceiling allows zero tokens. Reservations above the window clamp capacity to zero.

Certification requires an independently verified optimal, zero-gap result. R011 may still return unresolved or gap evidence. That evidence cannot become this package's certified optimum. Bind the complete input universe and revisions. A solver certificate over an integer model cannot certify different evidence. Candidate weights are exact for their declared serialized slices/tokenizer. The separate whole-prompt consumer checks delimiters, fixed prompt content and aggregate serialization. No implementation or test receipt is added by these definitions.

<!-- spec-definition-patch:comparative-analysis-r139 -->
## Comparative analysis plan producer (R139)

The EG producer consumes only caller-supplied, revision-pinned `UnifiedQuery` results and vectors, filters to caller-visible candidates, caps the universe at 64, and emits stable order. Versioned default importance thresholds are 0.50 for selective synthesis and 0.80 for deep extraction. Stage budgets are exactly 0 discovery, 1 or 2 synthesis calls, and 0–64 extraction calls; the explicit total must cover their sum. No stage retries outside budget. The plan contains no model-generated text.

The real EG path is public `Method::Decide` through `src/server/handlers/decide/statistical.rs::handle_decide` and `src/server/handlers/decide/stat_decide.rs::handle_decide`; add the `ComparativeAnalysisPlan` arm and make zero model calls. The AU consumer may invoke only after receiving the plan, through `agent_utilities/knowledge_graph/retrieval/context_compiler_serving.py::compiled_chat_completion`; AU binds actual calls, run ID and request/result digests to a separate receipt. `AU-HARNESS-R024.3` is trace-digest support only. The AU consumer contract and served receipt remain an explicit external dependency, not covered by EG changes.

Dependency order: typed request/plan/refusal/evidence round trip; deterministic threshold and budget behavior; real Decide wiring; served stale-input and budget refusals; then independent AU served consumer and receipt. Keep R080.1, R136.3, R137, R138 and existing producer obligations intact.
<!-- /spec-definition-patch -->

<!-- spec-definition-patch:owner-batch-20261012-decision-engine -->
## Reviewed residual closure plan — owner batch 2026-10-12

### `EG-DECISION-ENGINE-R135.7` — AU:18

- Existing rollup: `EG-DECISION-ENGINE-R135`; preserve `.2`, `.3`, `.5` and adjacent `AU-HARNESS-R022.2`.
- EG contract: accept the closed `PrePlanTestReceiptV1` fields named in requirements, bind the same receipt to planning and verification, score `tests_ran` and `artifacts_exist` separately, and promote only a zero-exit, artifact-readable successful cycle.
- Production attach point: existing `compile_program` / `AgentLibraryStore` / `handle_decide` `ProgramLookup`; retain `CompiledProgram` and current store, with no second reuse model.
- Pending owner contract: exact AU pre-plan test producer and verifier receipt ID/schema. `AU-HARNESS-R022.2` is not sufficient evidence for those fields.
- Planned positive/negative served acceptance: valid receipt is read back from plan, verifier and promoted pattern; missing, late, stale, changed-revision/command, failed test or unreadable artifact evidence blocks promotion.
### `EG-DECISION-ENGINE-R106.1` — AU:19

- Existing rollup: `EG-DECISION-ENGINE-R106`; preserve topology shape and `EG-ORCH-VOCAB-R006.3` loop-back semantics.
- EG contract: `CouncilCoordinationV1` accepts a complete, non-duplicated set of revision-bound `CouncilPositionV1` records and a pinned quorum/conflict policy; output retains all participants, accepted/rejected positions, outcome, abstention reason and digests.
- Production attach point: typed `Method::Decide` / `handle_decide` result sealing; validate the referenced council shape through `AgentGraphDraft.topology` and `topology_shape.rs`.
- Pending owner contract: AU persona-position producer ID/schema; `AU-CONTROL-R010` is topology input only.
- Planned served acceptance: coherent quorum, policy-resolved conflict, explicit abstention, missing/duplicate/stale/unauthorized position and topology mismatch cases; independently read the sealed decision and all position refs.
### `EG-DECISION-ENGINE-R137.13` — AU:25

- Existing rollup: `EG-DECISION-ENGINE-R137`; preserve `.1-.3` and keep package selection `.7-.12` separate.
- EG contract: `ContextBudgetSignalsV1` binds tool-output bytes/digest and graph-node count/snapshot to a run and observation time; `ContextBudgetPolicyV1` versions thresholds and ladder mapping; result binds the observations and returns direct/recursive mode, selected rung and reason codes.
- Production attach point: add the `QuestionKind::ContextBudget` branch in `Method::Decide`, beside `tool_subset_outcome` in `stat_decide.rs::handle_decide`; AU `run_full_rlm` consumes the sealed `DecisionBatch` before execution.
- Pending owner contract: exact AU producer for both observations and its decision consumer binding; `AU-HARNESS-R026.3/.4` does not define these fields.
- Planned served acceptance: vary each signal independently and together around boundaries; missing, stale, altered-digest, invalid and cross-tenant signals refuse before execution.
### `EG-DECISION-ENGINE-R134.9` — AU:34

- Existing EG rollup: `EG-DECISION-ENGINE-R134`; append-only residual ID: `EG-DECISION-ENGINE-R134.9`.
- Original objective: The agent selects several relevant skills from a skill tree for one task, and selection is tuned from run outcomes, so exploration is not locked to a single skill.
- Missing EG obligation: An explicit skill tree and outcome-tuned EG exploration over that tree are absent. AU-HARNESS-R022.3 says retrieval ranks locally, while AU-CONTEXT-R001 assigns ranking/feedback to EG; resolve this authority conflict.
- Producer/consumer route: EG plan_over_skills -> AgentLibraryStore -> served TaskPlan; AU experience feedback lacks a named EG selection-policy update consumer.
- Proposed definition: Define TaskPlanSkillNodeV1 with task_plan_id/revision/digest, skill_id, nullable parent_skill_id, context_match_policy_revision and evidence refs, prior_outcome_refs with source revisions/digests, score, rank, and ranking_policy_revision. EG emits multiple relevant skills in a deterministic tree under a versioned score/rank policy that binds the comparator and tie rule; missing or unpinned policy refuses. Outcome feedback changes later breadth only through R140.102 and its pinned policy/outcome set.
- Planned acceptance and owner hold: Case R134.9-ranked-plan: served Decide(TaskPlan) returns multiple skill nodes with stable parent links, cited context matches, pinned outcome refs, score/rank and plan digest; equal scores use the declared tie-break. Refuse missing/stale context or outcome evidence and malformed/cyclic parent links. AU application of this rank is not asserted. Attach the proposed result at plan_over_skills -> AgentLibraryStore -> served TaskPlan. AU-HARNESS-R022.3 local ranking and AU-CONTEXT-R001 feedback authority remain existing adjacent obligations; the AU consumer that applies EG rank and the feedback-to-R140.102 receipt contract are not yet specified. Keep SPECIFIED pending those owner definitions and served evidence. No implementation, served test, or receipt is claimed by this SPECIFIED definition.
- Preserve covered clauses: `EG-DECISION-ENGINE-R134.4`, `EG-DECISION-ENGINE-R134.5`, `AU-HARNESS-R022.3`, `AU-CONTEXT-R001`.
- Source review: `ced4d0bb75fa260e4291aea5f71c28b407a95fc3e06047666e1278b043a3cdbd`; evidence: epistemic-graph:21a11ecc04e95ed3e297ddd762f29b7fa5524254:specs/decision-engine/requirements.md:283; epistemic-graph:21a11ecc04e95ed3e297ddd762f29b7fa5524254:specs/decision-engine/requirements.md:284; agent-utilities:50c313ab01871ff7e10aee2a9d0254fa55d71f07:specs/harness-evolution/requirements.md:112; agent-utilities:50c313ab01871ff7e10aee2a9d0254fa55d71f07:specs/agent-context-and-finance/requirements.md:5.
- Cross-owner behavior and receipts remain with the named owner; no runtime evidence is claimed.

### `EG-DECISION-ENGINE-R137.14` — AU:37

- Existing EG rollup: `EG-DECISION-ENGINE-R137`; append-only residual ID: `EG-DECISION-ENGINE-R137.14`.
- Original objective: A run's long context may be compressed into an updatable memory state only when answer quality on held-out questions stays within a declared tolerance; otherwise the full context is retained.
- Missing EG obligation: The judge checks retained values/methods/validation steps, not answer quality on held-out questions compared with full-context answers under a declared tolerance.
- Producer/consumer route: AU ContextManager.before_model_request -> run_agent replaces context only after its summary gate; no held-out paired-answer verdict is a named prerequisite.
- Proposed definition: Define PairedContextEvaluationV1 with held_out_query_set id/revision/digest, raw_context revision/digest, candidate_context revision/digest, evaluator/model revision, per-query raw and candidate answer digests, declared quality metric, aggregation rule, tolerance and policy revision, plus a complete per-query verdict set. Replace raw context only when all queries are present and the aggregate satisfies the declared tolerance; any failed or incomplete evaluation retains the raw revision.
- Planned acceptance and owner hold: Case R137.14-paired-context: a complete held-out set within the declared tolerance permits the replacement verdict; a beyond-tolerance result, missing query, stale context/evaluator digest, undefined metric/aggregation/tolerance, or incomplete verdict set refuses and leaves the active raw-context revision unchanged. AU ContextManager.before_model_request -> run_agent is the existing replacement point. The AU held-out evaluator and receipt schema plus the consumer that atomically applies EG acceptance are absent; R013.5/.10/.12/.13 remain adjacent coverage, not evidence of this gate. No implementation, served test, or receipt is claimed by this SPECIFIED definition.
- Preserve covered clauses: `AU-HARNESS-R013.5`, `AU-HARNESS-R013.10`, `AU-HARNESS-R013.12`, `AU-HARNESS-R013.13`.
- Source review: `7e1405981aa8b697a73eb7e8c3c3170be8edefc674c23beef740884450fc1add`; evidence: agent-utilities:50c313ab01871ff7e10aee2a9d0254fa55d71f07:specs/harness-evolution/requirements.md:28; agent-utilities:50c313ab01871ff7e10aee2a9d0254fa55d71f07:specs/harness-evolution/requirements.md:33; agent-utilities:50c313ab01871ff7e10aee2a9d0254fa55d71f07:specs/harness-evolution/requirements.md:35; agent-utilities:50c313ab01871ff7e10aee2a9d0254fa55d71f07:specs/harness-evolution/requirements.md:36.
- Cross-owner behavior and receipts remain with the named owner; no runtime evidence is claimed.

### `EG-DECISION-ENGINE-R134.10` — AU:39

- Existing EG rollup: `EG-DECISION-ENGINE-R134`; append-only residual ID: `EG-DECISION-ENGINE-R134.10`.
- Original objective: A main run delegates subtasks to sub-runs that return only summaries within a declared contract, conserving the main context budget, and records delegation decisions as trajectories.
- Missing EG obligation: There is no summary-only sub-run return schema, parent-remaining-context bound or explicit rejection of raw/oversized returns. Generic spans do not bind delegation decision, child run and accepted summary as one trajectory.
- Producer/consumer route: AU _fork_fanout -> FanoutResult and parallel workspace synthesis; EG decides plans. The bounded child-summary producer/parent consumer boundary is absent.
- Proposed definition: Define ChildSummaryV1 with plan/decision id and digest, parent run id, child run id, expected child id set, summary artifact kind/schema/version/digest, exact tokenizer revision, summary token count, and source/result evidence refs. A parent budget record binds remaining context tokens at dispatch and the maximum aggregate child-summary tokens; child output exposed to the parent contains only the declared summary artifact, never raw transcript. The accepted fan-out receipt accounts for every child as one summary or named failure gap.
- Planned acceptance and owner hold: Case R134.10-bounded-child-summary: valid summary-only children whose checked token total fits the parent remaining budget are linked to decision, child and summary digests. Refuse raw/oversized output, missing/duplicate/unknown children, tokenizer or budget revision mismatch, and any total above the bound before parent synthesis. AU _fork_fanout -> FanoutResult and parallel workspace synthesis are the observed attachment points; EG owns TaskPlan authority. AU summary producer, parent remaining-context budget field and consumer validation/readback receipt are missing; AU-CONTROL-R043.2/.5 and AU-HARNESS-R020.4 and AU-HARNESS-R024.1 remain adjacent. No implementation, served test, or receipt is claimed by this SPECIFIED definition.
- Preserve covered clauses: `AU-CONTROL-R043.2`, `AU-CONTROL-R043.5`, `AU-HARNESS-R020.4`, `AU-HARNESS-R024.1`.
- Source review: `742906721351687d032991b4e9c0255baf7a6c480e21ac7a616975d40e38ce84`; evidence: agent-utilities:50c313ab01871ff7e10aee2a9d0254fa55d71f07:specs/agent-control-plane/requirements.md:100; agent-utilities:50c313ab01871ff7e10aee2a9d0254fa55d71f07:specs/agent-control-plane/requirements.md:103; agent-utilities:50c313ab01871ff7e10aee2a9d0254fa55d71f07:specs/harness-evolution/requirements.md:95; agent-utilities:50c313ab01871ff7e10aee2a9d0254fa55d71f07:specs/harness-evolution/requirements.md:126.
- Cross-owner behavior and receipts remain with the named owner; no runtime evidence is claimed.

### `EG-DECISION-ENGINE-R137.15` — AU:40

- Existing EG rollup: `EG-DECISION-ENGINE-R137`; append-only residual ID: `EG-DECISION-ENGINE-R137.15`.
- Original objective: A recursive runtime lets a model call sub-models with typed signatures and tools, recording interpretable trajectories in the run trace.
- Missing EG obligation: A primitive/function handle and span digests do not define each recursive sub-model's typed input/output signature, tool binding and boundary validation/refusal. The recorded signature/model/tool linkage remains missing.
- Producer/consumer route: AU run_full_rlm -> RLMEnvironment.run_parallel_sub_calls executes EG limits; span instrumentation records calls. No recursive-call signature producer/validator is named.
- Proposed definition: Define RecursiveCallV1 request/result records with parent run/span, call id, signature id/revision/digest, typed input schema and digest, admitted tool id/revision set, selected model id/revision, typed output schema and digest, and terminal status. Validate input and tool membership before dispatch and output against the same signature before recording success; bind all revisions/digests in the recursive trace.
- Planned acceptance and owner hold: Case R137.15-signature-boundary: an admitted typed call records matching signature/model/tool/input/output digests. Refuse unknown or changed signatures, unadmitted tools, malformed input/output, missing parent span, or revision mismatch before accepting the call result. AU run_full_rlm -> RLMEnvironment.run_parallel_sub_calls is the observed execution point; EG resource limits and span instrumentation are adjacent only. No AU recursive signature producer/validator receipt or exact EG-to-AU call contract is named. No implementation, served test, or receipt is claimed by this SPECIFIED definition.
- Preserve covered clauses: `AU-HARNESS-R026.1`, `AU-HARNESS-R026.4`, `AU-HARNESS-R024.1`, `AU-HARNESS-R024.3`, `EG-ORCH-VOCAB-R005`.
- Source review: `4edb15328650ebf084f0ab1d073b092e5b684aafad9b7b1ca7f412859390b923`; evidence: agent-utilities:50c313ab01871ff7e10aee2a9d0254fa55d71f07:specs/harness-evolution/requirements.md:142; agent-utilities:50c313ab01871ff7e10aee2a9d0254fa55d71f07:specs/harness-evolution/requirements.md:145; agent-utilities:50c313ab01871ff7e10aee2a9d0254fa55d71f07:specs/harness-evolution/requirements.md:126; agent-utilities:50c313ab01871ff7e10aee2a9d0254fa55d71f07:specs/harness-evolution/requirements.md:128.
- Cross-owner behavior and receipts remain with the named owner; no runtime evidence is claimed.

### `EG-DECISION-ENGINE-R134.11` — AU:45

- Existing EG rollup: `EG-DECISION-ENGINE-R134`; append-only residual ID: `EG-DECISION-ENGINE-R134.11`.
- Original objective: Long tasks decompose into sub-horizons under a declared per-step budget with checkpointed credit; a plan whose step count exceeds the configured horizon is split before execution, not attempted whole.
- Missing EG obligation: Depth/item bounds do not define a configured execution horizon, per-step budget, pre-execution splitting of excess steps or checkpointed credit across sub-horizons.
- Producer/consumer route: EG decompose_task/TaskPlan -> AU bounded map primitives; the horizon partition and durable credit receipt are not a named producer/consumer interface.
- Proposed definition: Define HorizonPartitionV1 over one committed TaskPlan revision with ordered step ids, dependency edges, positive max_steps_per_horizon, per-step budget policy id/revision/unit and credit ceiling. Partition only at dependency-safe boundaries before launch. Each completed horizon returns a durable credit checkpoint with plan/horizon ids, consumed/reserved/remaining amounts, budget revision and digest; require a versioned accounting rule for debits.
- Planned acceptance and owner hold: Case R134.11-horizon-credit: a plan within the limit remains one horizon; a longer valid plan is split at dependency-safe boundaries and its checkpoint balances reconcile exactly. Refuse an unsplittable dependency component, absent/invalid budget rule, over-credit horizon, missing checkpoint, or stale plan revision before the next launch. EG decompose_task/TaskPlan feeds AU bounded map primitives, but no named partition/credit receipt interface exists. AU durable checkpoint/debit semantics and the consumer that blocks launch on a missing receipt remain unspecified; do not substitute Finance forecast horizons or solver budgets. No implementation, served test, or receipt is claimed by this SPECIFIED definition.
- Preserve covered clauses: `AU-HARNESS-R026.1`, `AU-HARNESS-R026.2`, `EG-DECISION-ENGINE-R134.3`, `EG-DECISION-ENGINE-R134.4`.
- Source review: `c1bfd61241a0fd02ada2816f173a3b9568da5b48e490e76058fd740b356886da`; evidence: agent-utilities:50c313ab01871ff7e10aee2a9d0254fa55d71f07:specs/harness-evolution/requirements.md:142; agent-utilities:50c313ab01871ff7e10aee2a9d0254fa55d71f07:specs/harness-evolution/requirements.md:143; epistemic-graph:21a11ecc04e95ed3e297ddd762f29b7fa5524254:specs/decision-engine/requirements.md:282; epistemic-graph:21a11ecc04e95ed3e297ddd762f29b7fa5524254:specs/decision-engine/requirements.md:283.
- Cross-owner behavior and receipts remain with the named owner; no runtime evidence is claimed.

### `EG-DECISION-ENGINE-R059.1` — AU:57

- Existing EG rollup: `EG-DECISION-ENGINE-R059`; append-only residual ID: `EG-DECISION-ENGINE-R059.1`.
- Original objective: Skill candidates from several models are merged into one skill tree whose scores aggregate independent evaluations, giving a robust estimate of effectiveness and transferability across model backbones.
- Missing EG obligation: No merged skill tree binds candidates from several model backbones to independent per-skill evaluations or produces the requested robustness/transferability estimate across those backbones.
- Producer/consumer route: AU experience/skill candidates -> EG admission-linked outcome aggregate; the shared tree and backbone-bound per-skill evaluation join remain unnamed.
- Proposed definition: Define BackboneSkillEvaluationV1 rows keyed by candidate id, canonical skill identity, source model/backbone id/revision, task-distribution id/revision, evaluation dataset id/revision/digest, metric id/revision, sample count and result digest. An aggregate may be emitted only with complete independent rows for every declared backbone and a versioned transferability policy; publish the full per-backbone evidence with the aggregate, not a bare score.
- Planned acceptance and owner hold: Case R059.1-backbone-evidence: complete independent evaluations for the same canonical skill identity produce a result that lists every backbone and policy revision. Refuse missing/duplicate backbone rows, candidate identity mismatch, reused non-independent evidence, incomplete dataset coverage or absent metric/transferability aggregation policy. AU supplies experience/skill candidates; EG R059 is the existing independent-outcome admission path. No shared canonical skill merge key, robust aggregation/threshold decision, or AU multi-backbone evaluation producer is specified; these decisions are explicit holds. No implementation, served test, or receipt is claimed by this SPECIFIED definition.
- Preserve covered clauses: `AU-HARNESS-R022.1`, `AU-HARNESS-R022.2`, `EG-DECISION-ENGINE-R059`.
- Source review: `0c7c2eed61e3cfe6fe6c863c75fb66092f3a8bea97e3942b635c7b69f715d3a9`; evidence: agent-utilities:50c313ab01871ff7e10aee2a9d0254fa55d71f07:specs/harness-evolution/requirements.md:110; agent-utilities:50c313ab01871ff7e10aee2a9d0254fa55d71f07:specs/harness-evolution/requirements.md:111; epistemic-graph:21a11ecc04e95ed3e297ddd762f29b7fa5524254:specs/decision-engine/requirements.md:90.
- Cross-owner behavior and receipts remain with the named owner; no runtime evidence is claimed.

### `EG-DECISION-ENGINE-R134.12` — AU:85

- Existing EG rollup: `EG-DECISION-ENGINE-R134`; append-only residual ID: `EG-DECISION-ENGINE-R134.12`.
- Original objective: Hard tasks run in two stages: parallel reasoning over several trajectories, then a summarization step aggregating them; width and depth are chosen by the decision ladder and re-measured against a single-trajectory baseline.
- Missing EG obligation: Hard-task alternative trajectories and deliberative summary aggregation; explicit width/depth decision ladder and remeasurement against a single-trajectory baseline.
- Producer/consumer route: R134 plan explicitly connects Decide TaskPlan to AU-CONTROL-R042/R043; R137 plan connects ContextBudget to AU-CONTEXT-R004/AU-HARNESS-R026. AU-HARNESS-R018 has run_agent paired arms. These dependencies lack the alternative-trajectory/summarization/baseline binding.
- Proposed definition: Define TrajectoryDecisionV1 with one task/plan digest, policy revision, chosen width/depth, ordered trajectory ids, identical input snapshot digest per arm, single-trajectory baseline id, per-arm outcome/evaluation refs, and paired comparison receipt. The decision names the selection and evaluation policies; it does not prescribe an unsupported scoring formula. A pass requires complete paired arms over the same task/input and an explicit R018 comparison verdict.
- Planned acceptance and owner hold: Case R134.12-paired-trajectories: selected width/depth and all trajectory/baseline ids bind to the same plan and input snapshot; each arm has an outcome and the paired R018 verdict is readable. Refuse missing/mismatched arm, baseline, policy revision or result, and refuse any claim of improvement without the declared paired verdict. Existing points are Decide(TaskPlan), AU-CONTROL-R042/R043, AU-HARNESS-R020.4, and paired arms in AU-HARNESS-R018.1/.2/.5. AU trajectory generation, summary aggregation and EG selection-policy consumer receipt are absent. No implementation, served test, or receipt is claimed by this SPECIFIED definition.
- Preserve covered clauses: `AU-HARNESS-R020.4`, `EG-DECISION-ENGINE-R134.1`, `EG-DECISION-ENGINE-R134.3`, `AU-CONTROL-R007`, `EG-DECISION-ENGINE-R137.1`, `EG-DECISION-ENGINE-R137.3`, `AU-HARNESS-R018.1`, `AU-HARNESS-R018.2`, `AU-HARNESS-R018.5`.
- Source review: `a28929a50cbfcb691664403ce456fbf9dc38d6bd64b34424932e4fa22f9cbda1`; evidence: agent-utilities:50c313ab01871ff7e10aee2a9d0254fa55d71f07:specs/agent-control-plane/requirements.md:12; agent-utilities:50c313ab01871ff7e10aee2a9d0254fa55d71f07:specs/harness-evolution/requirements.md:71; agent-utilities:50c313ab01871ff7e10aee2a9d0254fa55d71f07:specs/harness-evolution/requirements.md:72; agent-utilities:50c313ab01871ff7e10aee2a9d0254fa55d71f07:specs/harness-evolution/requirements.md:75.
- Cross-owner behavior and receipts remain with the named owner; no runtime evidence is claimed.

### `EG-DECISION-ENGINE-R134.13` — AU:91

- Existing EG rollup: `EG-DECISION-ENGINE-R134`; append-only residual ID: `EG-DECISION-ENGINE-R134.13`.
- Original objective: Parallel analyst roles each produce structured, cited findings that flow to one decision role, which abstains when findings conflict or lack citations.
- Missing EG obligation: Distinct analyst roles provide cited findings to one decision role; required abstention on unresolved conflict or missing citations.
- Producer/consumer route: EG R134.5 plans TaskPlan through served Decide; AU R020.5 constructs worker slots through _dispatch_parallel_batch. The selected aggregation consumer lacks a cited-finding/conflict abstention contract.
- Proposed definition: Define AnalystFindingV1 with task/plan digest, expected analyst id and role, finding id, claim text/digest, cited evidence ids/revisions/digests and source spans. Define SynthesisResultV1 with the complete expected analyst-id set, all finding ids, outcome per analyst, cited evidence set, conflicts, final decision-role id and either a supported conclusion or a typed abstention reason. Never drop a failed or absent analyst from accounting.
- Planned acceptance and owner hold: Case R134.13-cited-synthesis: cited compatible findings produce a result that accounts for every expected analyst. Refuse missing/duplicate analyst, unsupported finding, unresolved conflict, invalid citation, plan mismatch or absent decision role with explicit abstention and no recommendation. EG R134.5 served Decide(TaskPlan) is the planning point; AU R020.5 _dispatch_parallel_batch constructs worker slots. The exact AU finding producer/collector and aggregation consumer contract is missing; existing fan-out clauses do not prove it. No implementation, served test, or receipt is claimed by this SPECIFIED definition.
- Preserve covered clauses: `EG-DECISION-ENGINE-R134.3`, `AU-HARNESS-R020.1`, `AU-HARNESS-R020.4`, `AU-HARNESS-R020.2`, `EG-DECISION-ENGINE-R134.6`.
- Source review: `f5ddb0ac9b0329ac031c01c56c49ee03c922a991dbf2d0e29c111515424d3455`; evidence: agent-utilities:50c313ab01871ff7e10aee2a9d0254fa55d71f07:specs/harness-evolution/requirements.md:92; agent-utilities:50c313ab01871ff7e10aee2a9d0254fa55d71f07:specs/harness-evolution/requirements.md:93; agent-utilities:50c313ab01871ff7e10aee2a9d0254fa55d71f07:specs/harness-evolution/requirements.md:95; epistemic-graph:21a11ecc04e95ed3e297ddd762f29b7fa5524254:specs/decision-engine/requirements.md:282.
- Cross-owner behavior and receipts remain with the named owner; no runtime evidence is claimed.

### `EG-DECISION-ENGINE-R029.1` — AU:93

- Existing EG rollup: `EG-DECISION-ENGINE-R029`; append-only residual ID: `EG-DECISION-ENGINE-R029.1`.
- Original objective: Search grounding is a replaceable provider selected through the committed routing decision, independent of the reasoning model; an unavailable provider is refused, not silently substituted.
- Missing EG obligation: Replaceable search-grounding provider independently selected from reasoning model and bound to a committed route; unavailable committed provider must refuse rather than substitute.
- Producer/consumer route: Preserve GRAPHOS-OPS-R021.2.1 retrieval.search and its tenant-scoped retriever. EG must produce the committed provider binding and the SDK/GraphOS consumer must execute that exact provider; that edge is missing.
- Proposed definition: Define RetrievalProviderBindingV1 in the committed retrieval plan with provider id/revision/config digest, query/request digest, tenant scope, and a separately identified reasoning model id/revision. The binding has an explicit no-substitution policy. Execution evidence records the exact provider, binding digest, request/result digests and provider revision.
- Planned acceptance and owner hold: Case R029.1-exact-provider: independently vary provider and reasoning-model ids while verifying the committed provider is executed. If the bound provider is unavailable, stale, unauthorized or mismatched, refuse before retrieval and record zero calls to alternate providers. Preserve GRAPHOS-OPS-R021.2.1 retrieval.search and its tenant-scoped retriever. EG must commit the provider binding; the SDK/GraphOS consumer contract that executes and receipts that exact binding is not defined. No implementation, served test, or receipt is claimed by this SPECIFIED definition.
- Preserve covered clauses: `EG-DECISION-ENGINE-R029`, `GRAPHOS-OPS-R021.2.1`.
- Source review: `c9552392eb813763a83fc96fb48ad1bd18fd805ef608736f1dae5d48ab12c984`; evidence: epistemic-graph:21a11ecc04e95ed3e297ddd762f29b7fa5524254:specs/decision-engine/requirements.md:41; graph-os:069eebc785afb9fb26bbbcc907cf5a2a4bcab5c5:specs/hosted-api-operations/requirements.md:42.
- Cross-owner behavior and receipts remain with the named owner; no runtime evidence is claimed.

### `EG-DECISION-ENGINE-R134.14` — AU:105

- Existing EG rollup: `EG-DECISION-ENGINE-R134`; append-only residual ID: `EG-DECISION-ENGINE-R134.14`.
- Original objective: A search-based planner explores candidate solution branches with an experience memory and fuses results across branches, recording each branch outcome for later retrieval.
- Missing EG obligation: Search planner explores candidate solution branches using experience memory, fuses branch solutions and records every branch outcome for later retrieval.
- Producer/consumer route: Use the same missing branch-experience producer and committed planning consumer identified for AU:84. Existing attachment points are R134.5 Decide, R020.5 worker execution and R022.4 run outcome distillation.
- Proposed definition: Define BranchOutcomeV1 with branch id, parent task/plan digest, branch input digest, experience snapshot revision, ordered fusion position, terminal outcome or named failure gap, and immutable result digest. The fusion receipt lists every planned branch in deterministic order, the versioned fusion-policy id/revision, all outcome refs and the fused result digest. A later branch retrieval records the exact outcome refs and revisions it consumed.
- Planned acceptance and owner hold: Case R134.14-branch-readback: a complete branch set produces an ordered fusion receipt and independent readback for every outcome; a later branch cites the exact pinned prior outcome. Refuse missing/duplicate branch, changed parent/input/snapshot, omitted failure gap, absent fusion policy, or stale readback. Existing points are Decide R134.5, AU R020.5 worker execution and AU R022.4 outcome distillation. The AU branch-experience producer, committed-plan consumer and durable per-outcome receipt schema remain missing; do not substitute R140.102 breadth adaptation. No implementation, served test, or receipt is claimed by this SPECIFIED definition.
- Preserve covered clauses: `EG-DECISION-ENGINE-R134.3`, `AU-HARNESS-R022.3`, `AU-HARNESS-R020.4`, `AU-HARNESS-R022.1`, `AU-HARNESS-R022.6`.
- Source review: `4fd31dd504f158f43bcf77d81a39c2419303a538b32553a77a39f6b5a575e9d2`; evidence: agent-utilities:50c313ab01871ff7e10aee2a9d0254fa55d71f07:specs/harness-evolution/requirements.md:95; agent-utilities:50c313ab01871ff7e10aee2a9d0254fa55d71f07:specs/harness-evolution/requirements.md:110; agent-utilities:50c313ab01871ff7e10aee2a9d0254fa55d71f07:specs/harness-evolution/requirements.md:112; agent-utilities:50c313ab01871ff7e10aee2a9d0254fa55d71f07:specs/harness-evolution/requirements.md:115.
- Cross-owner behavior and receipts remain with the named owner; no runtime evidence is claimed.

### `EG-DECISION-ENGINE-R020.1` — AU:110

- Existing EG rollup: `EG-DECISION-ENGINE-R020`; append-only residual ID: `EG-DECISION-ENGINE-R020.1`.
- Original objective: Each agent keeps an exploitation pool of past trajectories and an exploration pool of generated candidates, and draws on the exploration pool only for contexts with no matching trajectory.
- Missing EG obligation: Separate per-agent exploitation trajectory pool and generated exploration pool; explore only when context has no matching past trajectory.
- Producer/consumer route: EG owns candidate selection and orchestration evidence; AU R022 supplies run experience. No per-agent two-pool producer or context-miss-only selection contract exists.
- Proposed definition: Define CandidatePoolV1 with tenant/agent id, task/context feature digest, policy revision, candidate ids and per-candidate source revisions. The selection receipt partitions candidates into exploitation (matched prior trajectories) and exploration (eligible only when no match exists), records the context-match policy revision and evidence refs, and includes a deterministic tie-break. Cross-agent history is never eligible.
- Planned acceptance and owner hold: Case R020.1-agent-isolated-pools: a same-agent matching trajectory selects from the exploitation pool and excludes exploration; a declared no-match case may select from exploration. Refuse missing match policy/evidence and cross-tenant or cross-agent history; bind result to the candidate snapshot. EG owns candidate selection and AU-HARNESS-R022.1/.3 provide adjacent experience inputs. No AU per-agent pool producer or context-match evidence contract exists; exact match dimensions/threshold must be supplied by that owner policy before implementation. No implementation, served test, or receipt is claimed by this SPECIFIED definition.
- Preserve covered clauses: `AU-HARNESS-R022.1`, `EG-DECISION-ENGINE-R020`, `AU-HARNESS-R022.3`.
- Source review: `dafa8396b5a7c2a168253c7be219718c78186266610e5a91e7dc318d35fc552b`; evidence: agent-utilities:50c313ab01871ff7e10aee2a9d0254fa55d71f07:specs/harness-evolution/requirements.md:110; agent-utilities:50c313ab01871ff7e10aee2a9d0254fa55d71f07:specs/harness-evolution/requirements.md:112; epistemic-graph:21a11ecc04e95ed3e297ddd762f29b7fa5524254:specs/decision-engine/requirements.md:31.
- Cross-owner behavior and receipts remain with the named owner; no runtime evidence is claimed.

### `EG-DECISION-ENGINE-R133.7` — EG:13

- Existing EG rollup: `EG-DECISION-ENGINE-R133.1`; append-only residual ID: `EG-DECISION-ENGINE-R133.7`.
- Original objective: Each recursive-reasoning trajectory (code run, sub-calls, intermediates) is stored as typed graph nodes so verifier and improvement loops can critique it.
- Missing EG obligation: Define recursive code/subcall/intermediate node/edge identity and verifier/improvement read-and-critique contracts.
- Producer/consumer route: Terminal CommitWorkItemResult adapts traces after fenced CAS; AddNode validates traces; graph queries read them. Recursive hierarchy production and critique consumption are absent.
- Proposed definition: Define RecursiveTraceNodeV1 with run id/revision, step id/revision, parent node id, ordered child ids, input/output/evidence refs and digests, source revision, and trace snapshot id. Define TraceCritiqueQueryV1 with root node, authorized projection, snapshot revision, filters and opaque cursor; the response includes the complete selected ancestry and source refs. Parent/child links must be complete and acyclic within the selected snapshot.
- Planned acceptance and owner hold: Case R133.7-trace-critique: ingest and independently read back a nested trace, then critique it at the same authorized snapshot with complete ancestry. Refuse incomplete ancestry, cycle, stale cursor/snapshot, missing digest or unauthorized projection without returning protected fields. Terminal CommitWorkItemResult adapts traces, AddNode validates records and graph queries read them. The recursive node producer and named authorized critique-query operation/consumer are absent; existing fencing/projection rules remain binding. No implementation, served test, or receipt is claimed by this SPECIFIED definition.
- Preserve covered clauses: `EG-DECISION-ENGINE-R133.1`, `EG-ORCH-VOCAB-R013.1`, `EG-DECISION-ENGINE-R133.5`, `EG-ORCH-VOCAB-R013.5`, `EG-ORCH-VOCAB-R009.15`, `EG-ORCH-VOCAB-R013.6`.
- Source review: `9e0be988c182da3c7f440c14d2fc8e56f60426e543e3f53ba39f250f235e6c8d`; evidence: epistemic-graph:21a11ecc04e95ed3e297ddd762f29b7fa5524254:specs/decision-engine/requirements.md:273; epistemic-graph:21a11ecc04e95ed3e297ddd762f29b7fa5524254:specs/orchestration-vocabulary/requirements.md:97; epistemic-graph:21a11ecc04e95ed3e297ddd762f29b7fa5524254:specs/decision-engine/requirements.md:277; epistemic-graph:21a11ecc04e95ed3e297ddd762f29b7fa5524254:specs/orchestration-vocabulary/requirements.md:101.
- Cross-owner behavior and receipts remain with the named owner; no runtime evidence is claimed.

### `EG-DECISION-ENGINE-R100.1` — EG:16

- Existing EG rollup: `EG-DECISION-ENGINE-R100`; append-only residual ID: `EG-DECISION-ENGINE-R100.1`.
- Original objective: Embeddings are adjusted from feedback signals (agent errors, human corrections) through a promoted adapter, closing a feedback loop into vector space; unpromoted adjustments never alter served results.
- Missing EG obligation: Define attributed agent-error/human-correction ingestion and lineage from examples to fitted adapters before existing promotion.
- Producer/consumer route: R100 persists examples, R101 applies promoted post-RLS/pre-ANN query adapters, R040 gates publish. Signal production and example-to-fit linkage are absent.
- Proposed definition: Define FeedbackEventV1 with stable event id, decision id/revision/digest, option/action id, actor id, outcome/evidence refs and digests, event time and source revision. A versioned mapping policy converts only a complete joined event into a fit example; deduplication is by event id plus source revision. Bind example-set digest to R100 fitting, resulting adapter id/revision to R101, and publish admission to R040.
- Planned acceptance and owner hold: Case R100.1-feedback-join: an authorized complete event is read back as exactly one fit example bound to the expected R100 dataset digest; replaying the same event is idempotent. Refuse duplicate identity with changed content, missing decision/option/outcome join, invalid mapping policy, stale source revision or evidence mismatch before fitting. R100 persists examples, R101 applies a promoted adapter post-RLS/pre-ANN and R040 gates publish. The AU signal producer and outcome-to-label mapping policy are not specified; do not treat an independent outcome record as the producer contract. No implementation, served test, or receipt is claimed by this SPECIFIED definition.
- Preserve covered clauses: `EG-DECISION-ENGINE-R100`, `EG-DECISION-ENGINE-R101`, `EG-DECISION-ENGINE-R040`.
- Source review: `eca485ffdff9cea5394130b53e12a0edc235e342ab01accc36ad9d06f496daa8`; evidence: epistemic-graph:21a11ecc04e95ed3e297ddd762f29b7fa5524254:specs/decision-engine/requirements.md:168; epistemic-graph:21a11ecc04e95ed3e297ddd762f29b7fa5524254:specs/decision-engine/requirements.md:169; epistemic-graph:21a11ecc04e95ed3e297ddd762f29b7fa5524254:specs/decision-engine/requirements.md:59.
- Cross-owner behavior and receipts remain with the named owner; no runtime evidence is claimed.

### `EG-DECISION-ENGINE-R088.2` — EG:17

- Existing EG rollup: `EG-DECISION-ENGINE-R088`; append-only residual ID: `EG-DECISION-ENGINE-R088.2`.
- Original objective: Evaluation replays real-world events in chronological order over a simulated period while the agent forecasts outcomes and adapts as information arrives; no future event is visible before its turn.
- Missing EG obligation: Define event cursor/period, forecasts, adaptation and no-future-event visibility at the simulator/harness boundary.
- Producer/consumer route: DecisionEval owns AS OF and fold replay. A real-world event simulator feeding an AU adaptive harness is undefined; policy-dependent environments must respect R119 off-policy routing.
- Proposed definition: Define ReplayEventV1 with event id, strictly ordered sequence number, event time, state snapshot id/revision/digest, observed action id, outcome/evidence refs and source revision. Replay uses an AS OF snapshot and opaque deterministic cursor; the sealed ReplayReceipt binds ordered event digests, cursor, policy id/revision and result digest. Identical duplicate delivery is idempotent; a conflicting duplicate or sequence gap refuses. When the environment is policy-dependent, route to R119 off-policy evaluation.
- Planned acceptance and owner hold: Case R088.2-ordered-replay: replay a fixed event sequence and resume from its receipt/cursor to the same event order and sealed digest. Refuse gaps, out-of-order events, conflicting duplicate ids, changed snapshots or policy-dependent on-policy claims without R119 routing. DecisionEval provides AS OF and fold replay. The real-world event simulator and AU adaptive-harness producer/consumer are undefined; this row specifies EG replay envelope and refusal only, not an external event source. No implementation, served test, or receipt is claimed by this SPECIFIED definition.
- Preserve covered clauses: `EG-DECISION-ENGINE-R088`, `EG-DECISION-ENGINE-R119`, `EG-DECISION-ENGINE-R088.1`.
- Source review: `d01db99df0c6c22b37f82cd815e129956fa151e7da2b670b73348ca5058e629e`; evidence: epistemic-graph:21a11ecc04e95ed3e297ddd762f29b7fa5524254:specs/decision-engine/requirements.md:146; epistemic-graph:21a11ecc04e95ed3e297ddd762f29b7fa5524254:specs/decision-engine/requirements.md:208; epistemic-graph:21a11ecc04e95ed3e297ddd762f29b7fa5524254:specs/decision-engine/requirements.md:147.
- Cross-owner behavior and receipts remain with the named owner; no runtime evidence is claimed.

### `EG-DECISION-ENGINE-R137.16` — EG:26

- Existing EG rollup: `EG-DECISION-ENGINE-R137.1`; append-only residual ID: `EG-DECISION-ENGINE-R137.16`.
- Original objective: Retrieval payload size adapts per request to task complexity, remaining token budget and model cost instead of a fixed window, and never exceeds the remaining budget.
- Missing EG obligation: The historical statement that only window/chunking budgets exist is stale: R137.7-.12 now cover exact-token package sizing and monetary/model-price bounds. The remaining gap is an explicit task-complexity input/selection policy and binding the caller ceiling to the current request remaining-token budget, including already-consumed/reserved tokens. R128 task-class pressure does not itself prove that joint payload-sizing linkage.
- Producer/consumer route: Planned served Decide(ContextBudget) -> existing solver -> independent certificate verifier -> sealed ContextPackageSelection -> AU-CONTEXT-R004 context-compile consumer. R137.11/.12 explicitly name the consumer; current dispatch absence is stated in R137.9.
- Proposed definition: Extend ContextBudgetSignalsV1 with task-complexity feature set/digest and complexity-policy id/revision. Derive remaining tokens at a pinned snapshot as total token ceiling minus committed and reserved token consumption in the same tokenizer revision; return both operands and the checked result. Bind the complexity decision and remaining budget to the sealed ContextPackageSelection and its R137.7-.12 solver certificate; R128.5 task-class pressure remains a separate input.
- Planned acceptance and owner hold: Case R137.16-complexity-and-remaining-budget: vary complexity while holding model cost and remaining tokens fixed, then vary remaining tokens while holding complexity fixed; verify the declared policy and certified package bind both decisions. Refuse missing/invalid policy, tokenizer or snapshot mismatch, negative/overflowing remainder, or package cost above remaining tokens. Planned Decide(ContextBudget) -> existing solver -> independent certificate verifier -> sealed ContextPackageSelection -> AU-CONTEXT-R004. R137.11/.12 name the consumer; the dispatch and receipt binding remain pending as recorded in R137.9. No implementation, served test, or receipt is claimed by this SPECIFIED definition.
- Preserve covered clauses: `EG-DECISION-ENGINE-R137.7`, `EG-DECISION-ENGINE-R137.8`, `EG-DECISION-ENGINE-R137.10`, `EG-DECISION-ENGINE-R137.9`, `EG-DECISION-ENGINE-R137.11`, `EG-DECISION-ENGINE-R137.12`.
- Source review: `ccbc35ba3595763696c824434868079aeb401ce8b601041e913b323a2d2fb575`; evidence: epistemic-graph:21a11ecc04e95ed3e297ddd762f29b7fa5524254:specs/decision-engine/requirements.md:310; epistemic-graph:21a11ecc04e95ed3e297ddd762f29b7fa5524254:specs/decision-engine/requirements.md:311; epistemic-graph:21a11ecc04e95ed3e297ddd762f29b7fa5524254:specs/decision-engine/requirements.md:313; epistemic-graph:21a11ecc04e95ed3e297ddd762f29b7fa5524254:specs/decision-engine/requirements.md:312.
- Cross-owner behavior and receipts remain with the named owner; no runtime evidence is claimed.

### `EG-DECISION-ENGINE-R029.2` — EG:30

- Existing EG rollup: `EG-DECISION-ENGINE-R029`; append-only residual ID: `EG-DECISION-ENGINE-R029.2`.
- Original objective: An agent loads only context relevant to its task, selected from available sources within a stated budget, instead of the full persistent memory; omitted sources are reported.
- Missing EG obligation: The complete candidate universe is not necessarily the complete available-source universe. No exhaustive, explicit omitted-source report with relevance/budget reasons is required. Displaced authority-conflict sources cover only one omission cause.
- Producer/consumer route: Served Decide(ContextBudget) -> certified package -> AU-CONTEXT-R004 context compile; ontology source selection R072 and source-authority conflict handling supply distinct inputs.
- Proposed definition: Define AvailableContextSourceV1 with authorized source id/revision/digest, relevance evidence, authority class, token/cost estimate and enumeration snapshot. ContextSourceSelectionV1 returns complete selected and omitted partitions with reason enum IRRELEVANT, OVER_BUDGET, AUTHORITY_DISPLACED, or UNAUTHORIZED, plus deterministic policy revision and source snapshot digest. Every enumerated source appears exactly once; protected source identifiers are omitted from unauthorized caller-visible details.
- Planned acceptance and owner hold: Case R029.2-complete-source-partition: the same served compile journey includes relevant selected sources and omitted examples for irrelevance, over-budget and authority displacement; every authorized source is accounted for once. Refuse incomplete enumeration, duplicate source, unknown reason, stale budget/authority snapshot or unauthorized disclosure. Served Decide(ContextBudget) -> certified package -> AU-CONTEXT-R004 context compile; R072 ontology selection and EG-SRC-AUTH-R010.6 provide distinct source inputs. The AU consumer receipt for the exhaustive partition is not named; preserve the parent retrieval/authorization obligations. No implementation, served test, or receipt is claimed by this SPECIFIED definition.
- Preserve covered clauses: `EG-DECISION-ENGINE-R029`, `EG-DECISION-ENGINE-R137.7`, `EG-DECISION-ENGINE-R137.8`, `EG-DECISION-ENGINE-R137.11`, `EG-DECISION-ENGINE-R137.12`, `EG-FEDERATED-QUERY-R072`, `EG-SRC-AUTH-R010.6`.
- Source review: `05fcbe47e588141000ec329927406270abd30b590d2624f22e9239079f829d38`; evidence: epistemic-graph:21a11ecc04e95ed3e297ddd762f29b7fa5524254:specs/decision-engine/requirements.md:41; epistemic-graph:21a11ecc04e95ed3e297ddd762f29b7fa5524254:specs/decision-engine/requirements.md:310; epistemic-graph:21a11ecc04e95ed3e297ddd762f29b7fa5524254:specs/decision-engine/requirements.md:311; epistemic-graph:21a11ecc04e95ed3e297ddd762f29b7fa5524254:specs/decision-engine/requirements.md:314.
- Cross-owner behavior and receipts remain with the named owner; no runtime evidence is claimed.

### `EG-DECISION-ENGINE-R134.15` — EG:36

- Existing EG rollup: `EG-DECISION-ENGINE-R134.4`; append-only residual ID: `EG-DECISION-ENGINE-R134.15`.
- Original objective: Task scheduling computes critical paths over plan graphs using topological order and longest path, and reports the critical chain; cyclic plans are rejected.
- Missing EG obligation: No duration/weight-based longest-path calculation or critical-chain output is required. A DAG and dependency order alone do not define critical-path scheduling.
- Producer/consumer route: Served Decide(TaskPlan) -> plan_over_skills -> typed TaskPlan -> execution consumer; critical-chain fields and computation remain to be defined.
- Proposed definition: For each TaskPlan DAG step require positive duration_ms, nonnegative integer priority_weight and source evidence, all bound to a weight-policy id/revision and input digest. Compute checked integer path totals from duration_ms multiplied by priority_weight using the declared weight scale; return ordered critical_chain_step_ids, total_weighted_duration_ms, policy revision, plan revision and input digest. Ties choose the lexicographically smallest step-id sequence.
- Planned acceptance and owner hold: Case R134.15-weighted-critical-chain: a branched DAG returns the independently calculated longest weighted path and total, source-duration/weight provenance, and deterministic tie result. Refuse a cycle, missing/invalid duration or weight, absent scale policy, stale source evidence, or checked multiplication/sum overflow before emitting TaskPlan. Attach the proposed fields to served Decide(TaskPlan) -> plan_over_skills -> typed TaskPlan. The execution consumer schema for critical-chain fields is not defined; the decision producer and consumer remain SPECIFIED pending that schema. No implementation, served test, or receipt is claimed by this SPECIFIED definition.
- Preserve covered clauses: `EG-DECISION-ENGINE-R134.1`, `EG-DECISION-ENGINE-R134.4`, `EG-DECISION-ENGINE-R134.5`, `EG-DECISION-ENGINE-R134.6`, `EG-DECISION-ENGINE-R134.8`, `EG-ORCH-VOCAB-R006`.
- Source review: `6e1f0c3ab1626a00c8392e8a304b4c4a28a86d0aee8fff1933317bbdb4893966`; evidence: epistemic-graph:21a11ecc04e95ed3e297ddd762f29b7fa5524254:specs/decision-engine/requirements.md:280; epistemic-graph:21a11ecc04e95ed3e297ddd762f29b7fa5524254:specs/decision-engine/requirements.md:283; epistemic-graph:21a11ecc04e95ed3e297ddd762f29b7fa5524254:specs/decision-engine/requirements.md:284; epistemic-graph:21a11ecc04e95ed3e297ddd762f29b7fa5524254:specs/decision-engine/requirements.md:285.
- Cross-owner behavior and receipts remain with the named owner; no runtime evidence is claimed.

### `EG-DECISION-ENGINE-R135.8` — EG:52

- Existing EG rollup: `EG-DECISION-ENGINE-R135`; append-only residual ID: `EG-DECISION-ENGINE-R135.8`.
- Original objective: Workflows written in natural language or stored in the knowledge graph compile on demand to typed graph flows; compiled steps and agent configurations are saved for fast routing on reuse, and a compile failure names the missing step.
- Missing EG obligation: Successful-trace compilation is not on-demand compilation from natural-language or graph-stored workflow definitions. Persisted ProgramStep lacks an explicit reusable agent-configuration contract, and the compile failure names run failure/size rather than the missing workflow step.
- Producer/consumer route: Served Decide(TaskPlan) and ProgramLookup -> AgentLibraryStore/handle_decision_commit -> harness; SourceIngest is the workflow ingestion authority. A definition-to-program compiler between those surfaces remains unspecified.
- Proposed definition: Define WorkflowCompileRequestV1 with source kind NL or graph, exact source revision/digest, workflow identifier, compiler id/revision and referenced step/agent-config ids. The deduplicated CompiledWorkflowV1 binds the canonical typed step graph digest and each agent configuration id/revision/digest. Define `semantic_reuse_key_v1` as `{canonical_typed_step_graph_digest, compiler_id, compiler_revision, sorted_agent_config_refs}`, with config refs `{id, revision, digest}` sorted lexicographically by id. Source kind, source revision/digest and source-to-step mapping are excluded from the key and retained in a separate WorkflowCompileReceiptV1 binding source provenance to the key and program id. Reuse requires equality of every key component; text similarity alone is insufficient. A missing referenced step returns MISSING_WORKFLOW_STEP with its exact id/source location and publishes no partial program.
- Planned acceptance and owner hold: Case R135.8-workflow-compile: NL and graph inputs sharing the canonical typed graph digest, compiler id/revision and sorted config refs reuse one complete program/key but each has a distinct source-bound receipt. A new source revision with unchanged semantic inputs reuses the program and emits new provenance; changing graph, compiler or config refs changes the key. Refuse missing steps with exact location, stale source/config/compiler revisions, or partial publication. Proposed attach points are served Decide(TaskPlan), ProgramLookup and AgentLibraryStore/handle_decision_commit; SourceIngest remains the workflow ingestion authority. The compiler interface and stored configuration/receipt schema between these surfaces remain unspecified. No implementation, served test, or receipt is claimed by this SPECIFIED definition.
- Preserve covered clauses: `EG-DECISION-ENGINE-R134.1`, `EG-DECISION-ENGINE-R134.4`, `EG-DECISION-ENGINE-R134.5`, `EG-DECISION-ENGINE-R134.6`, `EG-DECISION-ENGINE-R135.1`, `EG-DECISION-ENGINE-R135.2`, `EG-DECISION-ENGINE-R135.3`, `EG-DECISION-ENGINE-R135.4`, `EG-DECISION-ENGINE-R135.5`, `EG-DECISION-ENGINE-R135.6`, `EG-ORCH-VOCAB-R006`, `EG-REPO-INGEST-R002`.
- Source review: `1ee5f2b5bd5bc45c9b9ff8ccaab8de944ed41eceab17a8c1e88cc86d4725e2dc`; evidence: epistemic-graph:21a11ecc04e95ed3e297ddd762f29b7fa5524254:specs/decision-engine/requirements.md:280; epistemic-graph:21a11ecc04e95ed3e297ddd762f29b7fa5524254:specs/decision-engine/requirements.md:283; epistemic-graph:21a11ecc04e95ed3e297ddd762f29b7fa5524254:specs/decision-engine/requirements.md:284; epistemic-graph:21a11ecc04e95ed3e297ddd762f29b7fa5524254:specs/decision-engine/requirements.md:285.
- Cross-owner behavior and receipts remain with the named owner; no runtime evidence is claimed.
<!-- /spec-definition-patch -->

<!-- spec-definition-patch:reviewed-mapped-subsets-20261012 -->
## Mapped definition implementation plan — 2026-10-12

### `EG-DECISION-ENGINE-R042.100` / AU:28

- Extend existing parent `R042` without removing its existing behavior.
- Specify: Model `ToolPathHistory` by tenant, connector, capability, stable path id/version, observation window, sample count, success/failure counts, as-of time and source invocation ids. Behavior ranks only already-admitted equivalent paths by observed success rate; skip a path only when the versioned decision policy declares a minimum sample and failure threshold and both are met. Missing, stale or undersampled history is `unknown` and cannot suppress a path. Wiring adds the evidence to the existing R042 Decide advisory and consumes current attempt outcomes from SDK `ordered_equivalent_fallback`; it must not replace the SDK declared fallback order or execute writes. Evidence binds policy, candidate paths and observation window.
- Producer/consumer boundary: EG R042 remains evaluate-only connector tool-choice advisory; SDK owns ordered transport fallback and current attempt outcomes.
- Source record SHA-256: `9dac8e5f41f6948885215836733ce3116b8246ca5c9c4725a12a62bdb11cb400`.
- Keep status SPECIFIED until the EG served producer and each separately owned consumer are implemented, served-tested and independently read back.

### `EG-DECISION-ENGINE-R042.101` / AU:86

- Extend existing parent `R042` without removing its existing behavior.
- Specify: Define `ToolNecessityMeasurement` bound to model id/revision, tool/capability id, task-cohort digest, paired tool-enabled/tool-disabled trial identities, measured outcome metric, sample counts, policy threshold, measurement time and digest. A measurement is `necessary`, `not_necessary` or `unknown` only under the declared minimum-sample and outcome-delta policy; no data, stale model revision or insufficient pairs yields `unknown`. Compare every AU tool-call decision to the current model/tool measurement and emit a mismatch trace event with run, step, decision, model revision, measurement digest and mismatch kind (`unnecessary_call` or `omitted_necessary_tool`). The EG R042 decision remains authoritative; AU consumes at the existing per-tool span path.
- Producer/consumer boundary: R134.2 relevance is only a candidate prefilter and AU-HARNESS-R018.5 generic profile metrics do not measure tool necessity; AU run_agent per-tool spans are the consumer boundary.
- Source record SHA-256: `38315c5f9a088c33c4b81b88b813ed851f97f454d788c9fb07185c88c752f979`.
- Keep status SPECIFIED until the EG served producer and each separately owned consumer are implemented, served-tested and independently read back.

### `EG-DECISION-ENGINE-R129.100` / AU:38

- Extend existing parent `R129` without removing its existing behavior.
- Specify: Bind a correction to tenant, run, original output id/digest and correction author/time. A proposal carries rule id, proposed version, scope, source output, predecessor version and lifecycle state. It remains inactive until explicit approval and the existing R129 invariant gate both pass; invariant compliance is necessary but never substitutes for approval. Activation supersedes the declared prior version atomically; revocation names the active version and prevents future use while retaining audit history. AU future invocations consume only the approved current version and return its id/version in the run trace.
- Producer/consumer boundary: Extend R129 promotion authority and AU future-invocation application; R034 contradiction-retraction remains separate.
- Source record SHA-256: `1d54abebd7cbc92bd3d6c397f334b42d602b4c3b9f77e0091f1c683774701d72`.
- Keep status SPECIFIED until the EG served producer and each separately owned consumer are implemented, served-tested and independently read back.

### `EG-DECISION-ENGINE-R134.100` / AU:30

- Extend existing parent `R134` without removing its existing behavior.
- Specify: Define the generated `AggregateFanout/v1` request with the selected existing `AggregationOp`, TaskPlan digest, AU `FanoutResult` identity, expected subtask ids, and GraphOS durable instance-receipt ids/digests. Define the result with operation/version, ordered input receipts, one typed aggregate artifact/schema version, result digest and graph revision. EG applies the already-selected operator to the complete receipt set; it adds no fan-out width, concurrency policy or second aggregation authority. A served independent readback resolves the returned artifact by tenant/id/revision and compares the digest before success.
- Producer/consumer boundary: AU owns FanoutResult production; GraphOS `agents.run` owns durable instance receipts and capacity/refusal. EG owns selected operator and public application/readback contract only. Preserve existing R134.1/.3 and GRAPHOS-FLEET-R024.5-.9.
- Source record SHA-256: `8f2ff3030a71d67b20cd19301f0ed8cef75e975403595cec4060abf9fa8952c1`.
- Keep status SPECIFIED until the EG served producer and each separately owned consumer are implemented, served-tested and independently read back.

### `EG-DECISION-ENGINE-R134.101` / AU:94

- Extend existing parent `R134` without removing its existing behavior.
- Specify: Extend the existing task plan with one designated `coordinator_id` and a typed final-artifact declaration (schema id/version and artifact kind). The coordinator receives the complete FanoutResult, produces exactly one artifact, and binds each expected subtask id to its result or the named failure gap; no result may disappear in synthesis. The artifact and delivery receipt bind plan digest, coordinator, child receipt ids, schema version, artifact digest and final readback identity. Reuse R134 aggregation and AU fan-out; do not add a second orchestrator.
- Producer/consumer boundary: EG-selected TaskPlan/AggregationOp -> AU `_fork_fanout` FanoutResult -> named coordinator -> one final artifact; retains AU-CONTROL-R043.2/.4 named gaps.
- Source record SHA-256: `19331af6d24dacefccebddcae637919b39cb111b800e3bd239a68b42b8ad10c7`.
- Keep status SPECIFIED until the EG served producer and each separately owned consumer are implemented, served-tested and independently read back.

### `EG-DECISION-ENGINE-R137.100` / AU:59

- Extend existing parent `R137` without removing its existing behavior.
- Specify: Define an EG disclosure decision/event bound to instruction-set id, immutable version/digest, relevance trigger, decision id and scope. Initial rules are a distinct phase delivered before the first agent action; runtime refinements are disclosed only when their declared relevance trigger occurs. Each event records phase, trigger, instruction version/digest, run and sequence, plus applied/refused outcome. EG owns relevance selection; AU run loop applies the exact selected version and records the event. No duplicate local context selector is introduced.
- Producer/consumer boundary: Existing EG context/decision authority -> AU run loop; general context selection, reminder and trace counters do not cover disclosure. AU sole writer owns runtime application.
- Source record SHA-256: `0b55041f0517baa9de449e3d712356a0e252cd194365e09ae559e9067cc8eb1f`.
- Keep status SPECIFIED until the EG served producer and each separately owned consumer are implemented, served-tested and independently read back.

### `EG-DECISION-ENGINE-R134.102` / AU:24

- Extend existing parent `R134` without removing its existing behavior.
- Specify: Define an EG delegation-admission verdict bound to task class, quality metric, declared threshold, observation window start/end, sample count, outcome-set digest and policy revision. Admit delegation only when the measured quality over the complete stable window meets the declared threshold; insufficient samples, gaps, mixed revisions or stale evidence return typed not-ready/refusal. AU executes delegation only against the committed verdict. Do not substitute retrieval saturation, generic test coverage or a free-floating data-plane window.
- Producer/consumer boundary: EG owns authoritative TaskPlan/delegation admission; AU executes the admitted plan. Existing retrieval quality-saturation logic is adjacent only.
- Source record SHA-256: `7cbecf865c6185e23ec202d541c5f8d33388338af413ac06209709c619fbe200`.
- Keep status SPECIFIED until the EG served producer and each separately owned consumer are implemented, served-tested and independently read back.

### `EG-DECISION-ENGINE-R136.100` / AU:32

- Extend existing parent `R136` without removing its existing behavior.
- Specify: Define a correction proposal bound to failed-run trace digest, proposed fix revision, exact regression-test identity and test environment. Admission requires a before receipt showing that same test fails against the declared pre-fix revision and an after receipt showing it passes against the declared post-fix revision. Both receipts bind test digest, source revision and run identity; mismatch, missing receipt or reversed outcome refuses. Reuse existing correction/admission authority and preserve propose-only flow.
- Producer/consumer boundary: AU CorrectionProposal/study_admit supplies trace and benchmark; EG decides admission. Generic benchmark lift does not replace paired proof.
- Source record SHA-256: `1d12449ecff9c30c5999933324f1f705fd826539eba6182f3e6d2773c263fec3`.
- Keep status SPECIFIED until the EG served producer and each separately owned consumer are implemented, served-tested and independently read back.

### `EG-DECISION-ENGINE-R031.100` / AU:41

- Extend existing parent `R031` without removing its existing behavior.
- Specify: Define an EG per-paper admission request/decision binding paper locator/content digest, summary digest, platform-goal revision, graph snapshot revision, relevance dimensions/scores, declared threshold and selected claim ids. A paper is selected for ingestion only when it meets the configured relevance rule against both platform goals and current graph content; the verdict names the scored evidence and selected claims. Excluded papers produce no graph write. This definition covers scoring/admission only, not scheduled fetching.
- Producer/consumer boundary: GraphOS owns six-hour durable scheduler; SDK owns paper fetching/provider receipts; EG owns relevance scoring/admission and selected-claim contract.
- Source record SHA-256: `8c69b66d62c833b4da482e38a51efa624c3e3fc924d6fd6d3e2ef114378a8527`.
- Keep status SPECIFIED until the EG served producer and each separately owned consumer are implemented, served-tested and independently read back.

### `EG-DECISION-ENGINE-R036.100` / AU:46

- Extend existing parent `R036` without removing its existing behavior.
- Specify: Extend graph.assemble primitives to a closed typed union covering prompt, tool, memory and control-flow components. Each primitive exposes identity/version, input/output schema digest, capability/effect declaration and substitution compatibility contract. A candidate harness graph is accepted only when every edge type-checks and replacement preserves declared capabilities/effects; recorded run traces can create improvement proposals for each primitive kind, but proposals never mutate active composition.
- Producer/consumer boundary: EG graph.assemble is composition authority; AU HarnessPort executes selected composition. Existing prompt-only optimization proposal path remains and typed HarnessPort is preserved.
- Source record SHA-256: `04556089bf2790a52c59ede225b6212ccd90c0f7ca1dfad07b869ac9de6a19a1`.
- Keep status SPECIFIED until the EG served producer and each separately owned consumer are implemented, served-tested and independently read back.

### `EG-DECISION-ENGINE-R136.101` / AU:47

- Extend existing parent `R136` without removing its existing behavior.
- Specify: Bind each report revision to a stable rubric digest, criterion ids, prior satisfaction states, new evidence/outcome ids and independent grader revision. For every previously satisfied criterion, the new revision must preserve satisfaction; a regression blocks promotion and records the failing criterion. Self-reflection text is not a measured outcome. Use AU ContinuousEvaluationEngine reports as inputs and preserve EG R136 round verdict authority.
- Producer/consumer boundary: AU ContinuousEvaluationEngine produces per-criterion evidence/report revisions; EG evaluates the bound round verdict. Aggregate divergence and self-reflection are not criterion-level nonregression.
- Source record SHA-256: `9f72c0e6cb8d476050ffde54043ba0e37a063bfda05eb3e27020312cd9eeb69b`.
- Keep status SPECIFIED until the EG served producer and each separately owned consumer are implemented, served-tested and independently read back.

### `EG-DECISION-ENGINE-R121.100` / AU:54

- Extend existing parent `R121` without removing its existing behavior.
- Specify: Define a `StepVerification` bound to run/step id, input and output digests, evidence ids, verifier/policy revision, uncertainty interval and proof/trace digest. Each reasoning step receives a typed verdict (`verified`, `failed`, `uncertain`); only `verified` steps may feed dependent conclusions. A failed or uncertain step and all derived dependents are withheld/refused with the dependency chain named. Reuse typed Claim/Evidence provenance; a Claim class alone does not imply step verification.
- Producer/consumer boundary: EG owns proof/evidence/verdict; AU run execution gates continuation on the returned verdict. Existing Claim/Evidence and ordered trace digest semantics remain intact.
- Source record SHA-256: `78c1b58ee2c97f19ad4834981d1b75adeb89c82881710aed055a0cc6f5c36760`.
- Keep status SPECIFIED until the EG served producer and each separately owned consumer are implemented, served-tested and independently read back.

### `EG-DECISION-ENGINE-R138.100` / AU:55

- Extend existing parent `R138` without removing its existing behavior.
- Specify: Define a changeset-bound adversarial verifier request over generated artifact digest, declared tests, threat/attack policy revision and verifier revision. Its receipt enumerates attempted break cases and edge-case ids with fixed/retested state. Admission of generated code requires a complete verifier receipt and every surfaced edge case fixed against a new artifact digest and rechecked; unresolved, missing, stale or mismatched cases block acceptance. Keep materialization under existing governed write authority.
- Producer/consumer boundary: AU generates/proposes code and verifier evidence; EG admission consumes it; existing governed write path retains materialization authority. ProbeVerdict R138.1-.6 is a separate red-team probe capability.
- Source record SHA-256: `3d07d9e40d19b8d4502fcbf3fc2e682f67bcdeaa0730d5d698f7af32767bb785`.
- Keep status SPECIFIED until the EG served producer and each separately owned consumer are implemented, served-tested and independently read back.

### `EG-DECISION-ENGINE-R136.102` / AU:56

- Extend existing parent `R136` without removing its existing behavior.
- Specify: Require proposal evidence to bind a declared outcome, immutable rubric id/version/digest, exact graded artifact id/digest, grader revision, and one grade record per rubric criterion containing score/verdict and evidence reference. Every required criterion must be graded; missing, ungraded, mismatched or stale artifacts return a typed refusal before any proposal is admitted. Aggregate score cannot hide a missing criterion.
- Producer/consumer boundary: AU study_admit/ContinuousEvaluationEngine supplies the report; EG decision verdict controls admission. Preserve R019/R021 evidence and per-criterion R136.101 behavior.
- Source record SHA-256: `712793000145f8e6b1c3ac1de647e91256312eb601def299653a9671de2757ba`.
- Keep status SPECIFIED until the EG served producer and each separately owned consumer are implemented, served-tested and independently read back.

### `EG-DECISION-ENGINE-R138.101` / AU:83

- Extend existing parent `R138` without removing its existing behavior.
- Specify: Define a review request bound to base/head revisions and changed-file digest. The semantic review evaluates injection, authentication bypass, data exposure, cryptographic flaws and business-logic flaws. Each finding carries class, affected path/range, exploit/impact evidence, severity, confidence and trace to reviewed artifact. A versioned impact policy filters low-impact false positives with a retained disposition; no high-impact or unreviewed class may be silently dropped. This is distinct from R138 probe scorer selection.
- Producer/consumer boundary: EG owns review verdict/schema; named GraphOS authenticated changeset-review producer/consumer must be served and linked separately. Existing R138.3 probe endpoint is not that consumer.
- Source record SHA-256: `0c5f2631be262ba69472eb94841b7acfbcfd6d0cb379382c528ccd0efb4d22e7`.
- Keep status SPECIFIED until the EG served producer and each separately owned consumer are implemented, served-tested and independently read back.

### `EG-DECISION-ENGINE-R036.101` / AU:107

- Extend existing parent `R036` without removing its existing behavior.
- Specify: Extend graph.assemble request with verified coding-agent facts (identity, capability, path-discovery receipt and revision) and ordered skill transformation ids/versions with input/output schema digests. EG chooses only from supplied verified facts and emits a deterministic composition plan binding agent, skills and artifact contract; it never scans PATH or renders a UI. AU owns executable discovery; WebUI owns sandboxed frame rendering.
- Producer/consumer boundary: EG composition authority -> AU PATH discovery and skill execution -> WebUI isolated generated-artifact frame. Existing DESIGN guidance does not provide these producer/consumer receipts.
- Source record SHA-256: `d179fc0eb33039df3a87efe8a59ce2a635f0e39f360c58f925011448d0401cdc`.
- Keep status SPECIFIED until the EG served producer and each separately owned consumer are implemented, served-tested and independently read back.

### `EG-DECISION-ENGINE-R014.100` / AU:108

- Extend existing parent `R014` without removing its existing behavior.
- Specify: Extend the existing bounded composition objective with candidate source-size facts (exact artifact bytes and line count), measured per-run cost, adequacy verdict and legal/policy constraints, each bound to candidate and revision. First exclude candidates failing required behavior, tests, policy or legal constraints; among adequate candidates apply the declared size/cost objective and emit each measured fact and tie-break. Unknown size/cost or adequacy evidence cannot be treated as zero. This child does not settle the shortcut-comment policy, which remains held for coordinator resolution.
- Producer/consumer boundary: Reuse R014 exact bounded solver/graph.assemble authority. AU records execution cost; code-size measurement and candidate adequacy feed EG. Preserve legal/policy constraints and hold comment-style conflict.
- Source record SHA-256: `2dfb2ab7469f19faf03d91782fa276ed2c613dec2e1d1fce2c629e5b8974b476`.
- Keep status SPECIFIED until the EG served producer and each separately owned consumer are implemented, served-tested and independently read back.
<!-- /spec-definition-patch -->

<!-- spec-definition-patch:final-mappings-20261012 -->
## Final mapping implementation plan — 2026-10-12

### `EG-DECISION-ENGINE-R140` — Trajectory refinement and ranking family

- Add the root row and link every child in generated status.
- Preserve existing parent contracts and owner boundaries: Groups complexity-gated K-trajectory deliberation, provenance-preserving experience reinsertion, outcome-driven branch breadth and verifier-free pairwise ranking. Preserve calibrated-ranking requirements in R130 and AU generation/execution ownership.

### `EG-DECISION-ENGINE-R141` — Memory-change evaluation and rewrite admission

- Add the root row and link every child in generated status.
- Preserve existing parent contracts and owner boundaries: Requires fidelity and memory-quality evidence before activating a memory change; failed candidates preserve the prior active memory. R060 fitted-head promotion remains a separate model gate.

### `EG-DECISION-ENGINE-R142` — Atomic-before-workflow local-model certification

- Add the root row and link every child in generated status.
- Preserve existing parent contracts and owner boundaries: Defines the EG attestation producer, AU local-model evaluation consumer and GraphOS validation gate. Existing GraphOS R034 identity bindings and host policy remain authoritative.

### `EG-DECISION-ENGINE-R139.100` / AU:58

- Preserve parent `EG-DECISION-ENGINE-R139` and its existing clauses.
- Define: Extend comparative `StudySource` admission with a `graph_ref` containing tenant, graph, node, revision and content digest. Resolve the exact revision through the authorized graph read/query path; compare papers and codebases from graph refs alongside existing filesystem sources without copying source bodies. Record ordered source kind, locator/reference, exact revision, digest and comparison membership in the comparative result. Missing, stale, invisible or digest-mismatched graph references refuse before comparison; preserve filesystem intake and AU study/feature ownership.
- Producer/consumer: EG owns authorized graph resolution and returns pinned source evidence; AU owns StudySource intake, comparison execution and proposal generation. Preserve AU-HARNESS-R019.2/.3/.5/.6 and do not treat capability lookup as graph-source intake.
- Source record SHA-256: `f13717cacb9e87ff76591467e17b5f9d76d44577f62d71b57d57c42bc89d37ba`.
- Keep status SPECIFIED until each named producer and consumer is separately implemented, served-tested and independently read back.

### `EG-DECISION-ENGINE-R019.100` / GRAPHOS:17

- Preserve parent `EG-DECISION-ENGINE-R019` and its existing clauses.
- Define: Bind each decision record to its decision id/revision and the simulation, inspection and process-knowledge stream ids, revisions and digests used. Expose an authorized reviewer-readable provenance read. Before any recommendation commit, require an approval receipt bound to the decision revision and recommendation digest with reviewer principal, approval time and approval-record revision; missing, stale or mismatched approval refuses before commit. Retain existing R019 re-derivation/CAS and R047 reference semantics; do not duplicate governed-effect audit rows.
- Producer/consumer: EG produces decision/provenance and approval-binding inputs; GraphOS owns shared invoke/A2A approval admission; SDK owns governed source effects and effect receipts. R047 remains the related producer reference.
- Source record SHA-256: `0e4858352b73b18661ed21ef503cc4c4f36e72045c3ffeff8d2094d9f8f8e50a`.
- Keep status SPECIFIED until each named producer and consumer is separately implemented, served-tested and independently read back.

### `EG-DECISION-ENGINE-R029.100` / PIPE:22

- Preserve parent `EG-DECISION-ENGINE-R029` and its existing clauses.
- Define: Define a versioned typed command-config document per retrieval command with command id/schema version, typed parameters and a referenced inference-mode id/version/digest. EG Decide binds the selected retrieval plan and inference-mode digest to its decision; AU loads the immutable command file, validates its schema and requires its referenced mode digest to match the EG decision before applying it. AU run evidence binds command/config digest, mode digest, input digest, selected-plan revision and result digest. Missing, malformed, stale or mismatched configuration refuses before execution; file loading remains AU-owned.
- Producer/consumer: EG owns the typed plan/mode decision and digest; AU owns command-file loading and execution. Do not conflate plan selection with file loading or add a pipeline runtime orchestrator.
- Source record SHA-256: `0d3964cba3359a3b454f2fd7b61f2b893b3aec0d58613062b859c2201344980c`.
- Keep status SPECIFIED until each named producer and consumer is separately implemented, served-tested and independently read back.

### `EG-DECISION-ENGINE-R140.100` / AU:44

- Preserve parent `EG-DECISION-ENGINE-R140` and its existing clauses.
- Define: For each task, emit a versioned complexity decision with task digest, scored features, score, threshold/policy revision, branch (`simple` or `complex`) and permitted K. Below the policy threshold, choose the single-trajectory path and make no deliberation call. At or above it, return only the permitted K and decision digest in the plan; AU alone generates the K alternatives and executes deliberation, binding its receipt to that decision digest. Missing/stale score or policy, invalid K, or a complex decision with K below two refuses before AU execution. The threshold and K limits are explicit versioned policy inputs, not silently selected constants.
- Producer/consumer: EG owns score/policy/branch decision; AU owns trajectory generation and deliberation execution. AU-CONTROL-R043 independent-subtask fan-out remains distinct.
- Source record SHA-256: `74416937af9a4934ae1f5d1a02e30336deb1535189a07f53fb81fb352f81779f`.
- Keep status SPECIFIED until each named producer and consumer is separately implemented, served-tested and independently read back.


- **Operational wiring / exact hold:** Wire the complexity score/policy input and decision into the existing R134.5 TaskPlan/Decide route; assert EG returns a plan only. Record AU’s exact decision-digest/K consumer contract as a prerequisite, not inferred from R043.
### `EG-DECISION-ENGINE-R140.101` / AU:51

- Preserve parent `EG-DECISION-ENGINE-R140` and its existing clauses.
- Define: Represent each source trajectory with run/candidate ids, source revision and trace digest. The experience projection records the redaction-policy revision and removed reasoning-token spans, retains task/result/evidence spans, and records a deterministic shuffle seed plus ordered projected ids. Deliberation output is a new candidate whose parent list names every consumed experience id and digest; append it as one extra refinement trajectory without replacing source records. Missing provenance, invalid redaction policy or an output not linked to the consumed inputs refuses reinsertion.
- Producer/consumer: AU record_outcome/SkillGraphPipeline owns experience production and deliberation; EG owns the refinement decision/result binding. Preserve AU-HARNESS-R022 experience identities and source trace provenance.
- Source record SHA-256: `be286cbd079c08493be1dcc0512aa562b6e2f2fec8d3566437837d91b5f36ba0`.
- Keep status SPECIFIED until each named producer and consumer is separately implemented, served-tested and independently read back.

### `EG-DECISION-ENGINE-R140.102` / AU:84

- Preserve parent `EG-DECISION-ENGINE-R140` and its existing clauses.
- Define: Before each candidate branch starts, retrieve authorized prior attempts for the same search id and bind their branch ids, outcome revisions and digests into the branch request. EG emits a versioned breadth decision with retrieved outcome ids, prior breadth, selected breadth and policy revision; AU executes only that breadth and returns outcomes linked to the decision. A later branch must be able to change breadth from an earlier outcome under the declared policy. Missing, stale or cross-tenant experience is not treated as a zero outcome and cannot drive adaptation.
- Producer/consumer: EG TaskPlan/Decide owns committed breadth decisions; AU owns branch execution and attempt outcomes. Preserve AU-HARNESS-R020 worker slots and R022 outcome storage; this child covers long-horizon candidate branches only.
- Source record SHA-256: `b56fbf074624786b4a8c2b499f985ecc2d14d1e8349d08781cf28aba07c3abef`.
- Keep status SPECIFIED until each named producer and consumer is separately implemented, served-tested and independently read back.


- **Operational wiring / exact hold:** Wire branch-start through the existing TaskPlan/Decide handler, specify AU R020.5/R022.4 decision-id/outcome fields and test sequential adaptation. Do not count worker slots or generic run outcomes as this receipt.
### `EG-DECISION-ENGINE-R140.103` / AU:87

- Preserve parent `EG-DECISION-ENGINE-R140` and its existing clauses.
- Define: When no verifier is available, bind an ordered candidate set and candidate digests to pairwise comparison records; each pair record carries both ids, judge/model revision, verdict and evidence digest. Do not aggregate into a global ranking until an owner-approved, versioned policy defines cycle, incomplete-pair and tie behavior; Bradley–Terry is not selected by the source. Until the owner-approved aggregation policy defines complete-pair behavior, cycle and tie resolution, bounded keep-count/revision limits, and AU receipts, refuse to emit a global ranking, retained partition, or revision request. No positive ranking behavior is specified by this source record. An available verifier routes through its existing verifier contract instead of this fallback.
- Producer/consumer: EG owns pairwise evidence aggregation, ranking and retention decision; AU owns comparison execution and revisions. Preserve calibrated-ranking constraints in R130 and AU generation ownership.
- Source record SHA-256: `6f67238075c0bf262188d97a1813b18bfb0c71e13c6689a583c788d956287612`.
- Keep status SPECIFIED until each named producer and consumer is separately implemented, served-tested and independently read back.

### `EG-DECISION-ENGINE-R140.104` — Wiring row

- Production call sites: src/server/dispatch/router/decision_plane.rs:107 -> src/server/handlers/decide/stat_decide.rs:48.
- Bind every R140 decision digest and policy/source revision into the existing TaskPlan result; do not move AU model or trajectory execution into EG.
- Owner hold: AU's exact decision-digest/K, projection, breadth and pairwise execution receipt fields remain pending; R140.103 policy selection remains pending.
- Planned proving test: tests/decide/test_trajectory_refinement_served.py::test_real_decide_route_emits_pinned_refinement_decisions.

### `EG-DECISION-ENGINE-R140.105` — Hot-path proof row

- Planned proving integration test: tests/decide/test_trajectory_refinement_served.py::test_taskplan_decisions_reach_au_and_receipts_read_back.
- It enters Method::Decide, observes the TaskPlan binding through AU generation/refinement and independently reads back every receipt; refusal is required for a missing/mismatched digest.
- Explicit AU holds: consumer contracts for R140.100/.101/.102 and pairwise aggregation/receipt for R140.103. No successful proof is claimed until these are reviewed and served.

### `EG-DECISION-ENGINE-R141.100` / EG:23

- Preserve parent `EG-DECISION-ENGINE-R141` and its existing clauses.
- Define: A rewrite proposal binds tenant, memory id, active source revision/digest, candidate revision/digest, fidelity metric/policy revision and metric-spec digest, measured fidelity, signal and token counts, signal-per-token value and degradation-history window. Admit only when the declared fidelity threshold passes and the policy’s signal-efficiency/degradation checks pass. On any failed gate, discard the candidate and retain the original active revision unchanged; do not infer rewrite admission from R059 aggregate outcome fidelity or R128 elide/summarize selection.
- Producer/consumer: EG owns the rewrite evaluation decision; AU may generate candidate rewrites. The source does not identify the fidelity formula/normalization, so an immutable reviewed metric specification is required before an Admit verdict. The authoritative active-memory store/write/CAS path is also not identified and remains a prerequisite to an activation claim. Retain the original as the rollback source.
- Source record SHA-256: `ac1b5df0212beaade03e78502a494148692e1a0007c3fa68ee37a2bb6dbe8cce`.
- Keep status SPECIFIED until each named producer and consumer is separately implemented, served-tested and independently read back.


- **Operational wiring / exact hold:** Add the `MemoryRewriteSubmit` arm under the current DecisionEval job and bind its immutable verdict to the candidate. Map every active-memory write entrypoint and receipt/CAS transaction before claiming universal admission.
### `EG-DECISION-ENGINE-R141.101` / AU:125

- Preserve parent `EG-DECISION-ENGINE-R141` and its existing clauses.
- Define: Before any memory mutation is activated, bind the baseline and candidate memory snapshot digests to a versioned reference dataset containing query ids, expected evidence ids and reference answers. Report recall@k and answer accuracy with numerator, positive denominator, metric specification revision and dataset digest; the referenced metric specifications must define exact scoring, query aggregation and rounding. The source does not supply those formulas, so missing/unreviewed semantics refuse before positive admission; compare both only under a reviewed policy. Admission requires both thresholds to pass on the same dataset and snapshot pair. Missing dataset rows, metric inputs, denominator or policy revision refuse before activation. This gate covers add/edit/delete/rewrite/consolidation changes; R060 fitted-head promotion remains a separate model-promotion gate.
- Producer/consumer: AU executes the repeatable benchmark and returns its receipt; EG validates the receipt against the candidate and baseline. Universal activation refusal remains pending the authoritative mutation/write path. Reuse existing evaluation/activation mechanisms without substituting R060.
- Source record SHA-256: `27a6a74820b2b646163d8f7bd58b94167b9f0a3e423b8bf5210fe6e758deb440`.
- Keep status SPECIFIED until each named producer and consumer is separately implemented, served-tested and independently read back.

### `EG-DECISION-ENGINE-R141.102` — Wiring row

- Production call sites: src/server/dispatch/router/decision_plane.rs:119 -> src/server/handlers/decide/jobs.rs:30.
- Bind R141.100/.101 evidence into DecisionEval receipts before any active-memory mutation.
- Explicit owner hold: no authoritative memory store, full write-entrypoint list, or atomic receipt/CAS check is identified. Do not claim the current evaluation handler universally gates writes.
- Planned proving test: tests/decide/test_memory_change_admission_served.py::test_real_decision_eval_binds_memory_candidate_and_metrics.

### `EG-DECISION-ENGINE-R141.103` — Hot-path proof row

- Planned proving integration test: tests/decide/test_memory_change_admission_served.py::test_memory_mutation_requires_current_decision_eval_receipt.
- Enter DecisionEval, cross the authoritative write/CAS gate, and independently read back the active revision; failed or stale receipts must preserve the baseline.
- Explicit owner holds: reviewed fidelity/benchmark formulas and the mutation/CAS map are prerequisites; no positive activation is claimed.

### `EG-DECISION-ENGINE-R142.100` / GRAPHOS:8

- Preserve parent `EG-DECISION-ENGINE-R142` and its existing clauses.
- Define: For each skill revision, bind an atomic/workflow classification, local model id/revision, evaluation-set digest, criteria/policy revision, result and attestation digest. EG emits the certification attestation only after AU executes the declared local-model evaluation. GraphOS validates the attestation before normal (non-certification) `orchestrate` or `agents.run`; a separately authorized evaluation run may produce a new attestation. Atomic skills are evaluated first; a workflow skill is certifiable only when every referenced atomic dependency has a valid current attestation. Missing, stale, failed or mismatched dependencies refuse certification/invocation before effects. EG attests evaluation outcome; AU executes; GraphOS owns invocation gate and host policy.
- Producer/consumer: Extend existing GRAPHOS-FLEET-R034.7-.12 identity bindings; do not add host scoring authority or infer certification from host prerequisites alone.
- Source record SHA-256: `5df3f5a45d486aeba5765a31b6c6283637adf2bd8ce63a87938eddd28b90f082`.
- Keep status SPECIFIED until each named producer and consumer is separately implemented, served-tested and independently read back.
<!-- /spec-definition-patch -->


<!-- spec-definition-patch:owner-dispatch-20261012-plan -->

- **Operational wiring / exact hold:** Bind AU’s local-model evaluation receipt to EG DecisionEval, then bind the resulting attestation to GraphOS R034.7-.12. Keep AU producer and GraphOS consumer evidence pending; do not claim that the existing identity checks consume an EG attestation.
### `EG-DECISION-ENGINE-R142.101` — Wiring row

- Production call sites: src/server/dispatch/router/decision_plane.rs:119 -> src/server/handlers/decide/jobs.rs:30.
- The EG attestation must bind the AU evaluation receipt and exact skill/model/dataset/criteria revisions; GraphOS consumption is at GRAPHOS-FLEET-R034.7-.12.
- Explicit owner holds: AU's per-criterion evaluation receipt and GraphOS's attestation-consumer operation are not defined; R034 identity checks alone do not close this dependency.
- Planned proving test: tests/decide/test_local_skill_certification_served.py::test_decision_eval_attestation_binds_au_receipt.

### `EG-DECISION-ENGINE-R142.102` — Hot-path proof row

- Planned proving integration test: tests/decide/test_local_skill_certification_served.py::test_current_attestation_is_checked_before_graphos_workflow_run.
- Enter the actual evaluation/normal-invocation path and verify missing/stale attestation refusal before effects, then accept only the exact current attestation.
- Explicit owner holds: AU must serve the receipt; GraphOS must serve the R034 invocation check. No end-to-end evidence is claimed.

## Cross-owner retrieval evaluation plan — 2026-10-12

- **`EG-DECISION-ENGINE-R100.100` / WEBUI:6:** Extend the existing served `DecisionEval` job with `RetrievalRankingV1` input/result identity and `RetrievalContextSearchV1` bounded exact-span reads. Bind corpus, binary labels, query set, k, algorithms/settings, evaluator, result digests and numerator/denominator. Compare only identical inputs; refuse missing labels or incompatible revisions. Preserve R100 hard-negative learning from independently evaluated outcomes. Consumer: WebUI R001.1 side-by-side comparison. Source SHA-256 `3dc3c464a10a5178a413463f2b43baeb015732f5b8df508f6dbab9c37cc1165d`.
- Use `Method::UnifiedQuery` as retrieval producer, `Method::DecisionEval` as sealed metric receipt path and the authenticated query/read handler for n-gram spans; do not add a second ranking or truth source.
- Keep SPECIFIED until fixed-corpus served positive/negative cases and independent receipt/context readback pass.
<!-- /spec-definition-patch -->

## Grounded-answer and pre-surface safety plans — 2026-10-12

- **`EG-DECISION-ENGINE-R100.101` / EG:18:** Bind a complete expected topic set and a per-claim, label-blind predicted unsupported probability from a pinned scorer artifact, separately from independent annotator labels. Define covered topics by a policy threshold over supporting citation votes; return expected/covered ids and aggregate summed topic counts per scope. Define weighted Brier as `sum(vote_count_i * (p_i-q_i)^2) / sum(vote_count_i)` over the complete required claim set, where `q_i=unsupported_votes_i/vote_count_i`; seal the positive denominator and rounding policy. Refuse missing topics/predictions/labels, duplicate annotators and stale digests. Source SHA-256 `d978b4b4201059c2e7bcc7a78def0d5845cf7db1e361f2f50ee57f8bcc5733ae`.
- **`EG-DECISION-ENGINE-R001.100` / GRAPHOS:19:** Extend `Method::Decide` with `RecommendationSafetyV1` and per-bound typed evidence/result. Refuse missing, stale, non-finite, incompatible or over-limit facts with the exact violated bound. GraphOS's pre-surface invocation/validation receipt remains a separate pending prerequisite. Preserve partial AU-HARNESS-R021.4/.6/.7. Source SHA-256 `cd33b64c1d759f3c7dd0a83112b77955d25a49c69d20288fe4c917716c711ff6`.
