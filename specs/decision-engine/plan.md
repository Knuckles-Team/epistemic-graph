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

## Parallelism and dependencies

The contract bump precedes decision handler integration. Exact solver and pack fact-admission work may proceed independently once DTO ownership is frozen. Statistical jobs consume the committed record and visibility rules; question adapters consume the stable decision method. Shared edits to `agent_component.rs`, method descriptors, access classification and generated contract files are one coordinated integration unit to avoid competing schema versions.

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

Create binary selection variables for candidate components and, where required, template/slot choices. Constraints include: each required capability is covered by an eligible selected component; required dependencies imply selection; incompatible options cannot coexist; each required slot has exactly one assignment; cardinality and integer budget bounds hold; model and tool modality/schema constraints hold. The objective is a sequence of integer levels: first hard feasibility, then policy-defined utility, resource use and stable tie-break. Preprocess dominated or impossible options, seed an incumbent greedily, then branch and bound with an admissible lower bound until optimality or node budget. A budget-exhausted answer may return an incumbent and proven gap only if policy permits that gap; otherwise abstain. No fixed maximum number of feasible assemblies substitutes for work budgeting.

The independent certificate verifier checks input digest, sorted variable mapping, all constraints against the proposed assignment, objective recomputation, lower-bound validity, gap and solver version. It must not trust a claimed feasible assignment or bound. Why-not for a visible excluded option fixes that option on, re-solves under a separate bounded budget, and reports the resulting violation, worse objective, gap or budget exhaustion. A failed post-solve template/graph validator adds a no-good cut and re-solves within the original budget.

### 4. Statistical decision and evidence

Feature extraction is over the RLS-filtered candidate set. Candidate-local BM25 and visible-only vector scoring avoid corpus statistics and overfetch effects from invisible rows. The feature schema is versioned, records missingness and provenance, and includes only policy-approved facts: similarity, entailed coverage, evidence quality, belief/confidence, cost and latency. The resident option-marker scorer can encode state once and score each structured option in process; it must retain deterministic fixed-point output and CPU portability. Statistical heads are bounded, fitted components, not generative models; text generation and LLM weights stay outside EG.

Gold-labelled data and executed-option-only bandit data are different datasets. The latter requires exact action propensities, support diagnostics, inverse propensity or doubly robust estimators, effective sample size and censoring. Self-reported run outcomes cannot promote a head. Independent evaluations with sufficient trace fidelity and approved principal are joined to records; hierarchical pooling requires minimum support and coarse output to avoid individual-run inference. Conformal and selective-risk calibration use held-out or walk-forward folds, account for drift and explicitly abstain when their stated bound lacks support. `DecisionEval` receipts pin head, dataset window, policy, feature schema, metrics and thresholds. A policy change or head digest change invalidates the receipt for promotion.

### 5. Connector pack interaction

The existing pack types in `crates/eg-types/src/connector_pack/`, handlers in `src/server/handlers/admin/connector_pack/`, persistence in `src/server/persistence/connector_pack/` and Python `epistemic_graph/connector_pack.py` form one owner. The decision engine consumes published, tenant-visible component revisions and their annotations. It does not parse raw packs or trust client-provided digests. A pack entry's `provides`, `requires_capabilities`, modality, schema digest, cost, latency, model profile and `contract_pin` are publisher claims. Native capability IRIs can enter ontology derivation with that claim premise; unknown foreign IRIs are claim-only exact terms; malformed native IRIs are refused at import. Re-import does not alter a component revision when its own entry content is byte-identical, and `Withdrawn` members become ineligible immediately. A pack cannot insert decision-owned component kinds. The pack head and projection readiness marker must match before a projected ontology/shape fact is used as current.

Decide's contract at this boundary is narrow: it sees only validated component revisions and preserves publisher-claim class in every derivation, score and record. It rejects reserved decision-owned kinds and stale projection facts. Pack import atomicity, body storage and parser refusals are specified and tested by the pack owner independently.

### 6. Question-specific extensions

Typed question adapters translate retrieval, lane, enrichment, entity-resolution, schema-mapping, routing, risk, topology and recommendation requests into the same legal-set/evidence pipeline. `DecideText` is a dedicated UQL-like front end with quoted candidate blocks and typed parameters; malformed syntax returns a structured span/error and never falls through to ordinary query parsing. Topology adds bounded integer shape variables (width, rounds, role, capacity) and a composed-schema digest, then outputs a proposed `AgentGraph`; capacity acquisition and execution remain separate atomic operations. Entity resolution emits a proposed link and confidence/abstention, never an automatic identity merge. Finance recommendations are informational snapshots with strategy version, evidence and horizon; they cannot place trades. Graph-sourced record views must apply the same RLS as their candidate source.

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
| EG-DECISION-ENGINE-R021 | T2 | LANDED | NOT_RUN | Versioned feature schema over caller-visible candidates (vector similarity, candidate-local BM25, entailed coverage, provenance quality, belief confidence, cost/latency, freshness, outcome stats) |
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
| EG-DECISION-ENGINE-R073 | T3 | LANDED | NOT_RUN | PA2 — Store: four new owner tables, format-v3 open-time refusal, N-intent receipt, batch pin resolver, Withdrawn handling, head CAS, per-component outbox events |
| EG-DECISION-ENGINE-R074 | `tests/test_decision_static_contracts.py` | CLOSED | PASS | The closure rule lists every component kind. The consistency test checks enum and slot references. |
| EG-DECISION-ENGINE-R075 | T4 | BUILT | NOT_RUN | G6 — Component-id uniqueness and forbidden (Decide-owned) kinds |
| EG-DECISION-ENGINE-R076 | T4 | BUILT | NOT_RUN | Opt-in nested Pydantic models for decision-type result markers only (2.27.x-scoped subset, precedes the full migration) |
| EG-DECISION-ENGINE-R077 | T3 | LANDED | NOT_RUN | Shared component schema bump 2→3 (Decide owns numbering; pack adds Withdrawn + new kinds) — one co-landing step |
| EG-DECISION-ENGINE-R078 | T4 | CLOSED | NOT_RUN | Contract freeze re-issued with the decide feature |
| EG-DECISION-ENGINE-R079 | AU-batch | BUILT | NOT_RUN | **Embedding admission classifier.** Ingest-time embedding is applied to essentially everything crossing the ChangeEnvelope boundary by explicit design (envelope_ingest.py:2753 — the chokepoint exists "so no future connector can bypass it"). The only gates are four cheap predicates: upsert+typed-payload, text-unchanged short-circuit, empty-derived-text, and a global KG_INGEST_AUTO_EMBED flag defaulting ON. No content-type policy, no per-column decision, no cost budget |
| EG-DECISION-ENGINE-R080 | AU-batch | BUILT | NOT_RUN | **Ingestion cost ladder with provenance**, mirroring the approved Decide decision ladder: rung 0 AST (EXTRACTED), 1 symbol/type resolution (INFERRED), 2 statistical/community (DERIVED), 3 classical ML/NER (MODELED), 4 embeddings (EMBEDDED), 5 LLM (ASSERTED). A higher rung runs only when every lower rung abstains and may never overwrite a deterministic fact — the same monotone-safety property ruled for Decide |
| EG-DECISION-ENGINE-R081 | AU-batch | BUILT | NOT_RUN | **Weight community-detection edges by the resolver confidence we ALREADY compute, and stop letting similar_to vote.** The kernel consumes undirected_weighted_adjacency() so weights are supported; ingestion supplies none, discarding the exact signal that makes communities trustworthy. A scoped call (0.95) should bind a community harder than a unique guess (0.60) — see resolve.rs:663-672. Separately, MinHash similar_to is a RESEMBLANCE edge, not a structural one: including it conflates "looks alike" with "works together" (two similarly-shaped utility files are not a module). Type-weight edges — calls/imports/inherits structural, similar_to excluded or heavily discounted |
| EG-DECISION-ENGINE-R082 | T4 | BUILT | NOT_RUN | **Option-marker head geometry for eg-decision** — encode state once, score each option at its own marker, softmax in-process, one forward pass serving several questions; no decoder loop. Adopted from the Laya encoder design |
| EG-DECISION-ENGINE-R083 | T4 | BUILT | NOT_RUN | **Structured option encoding instead of textual — surpasses the external design by construction.** Laya scores 0.425 on Banking77 (vs 0.870 for the closed baseline) purely because 77 label strings exhaust head_max_len; that is a TOKENISATION limit, not a decision limit. Our options are IRIs carrying SHACL shapes, declared capabilities and cost/latency facts, so an option is encoded from its STRUCTURED features and cardinality bounds on feature-vector width rather than token budget. The failure mode that caps their usable option sets does not exi…35152 tokens truncated…FORBIDDEN_COMPONENT_KIND` refusal (`validate_publish` in `crates/eg-types/src/agent_component.rs`), exercised through both the bare validator and the public `AgentComponentOp::Publish.validate()` surface, so the previously untested bypass path is covered. Test: `crates/eg-types/src/agent_component.rs::tests::a_decision_record_cannot_be_published_directly`.

- [x] D15 **EG-DECISION-ENGINE-R030 (first slice):** Add the typed `IngestionLane` registry (fast/medium/slow) and `IngestionLaneRequest::check()` refusal for an unregistered lane id (`crates/eg-types/src/decision/statistical/ingestion_lane.rs`). `QuestionKind::IngestionLane` candidates still route through the ordinary generic `Decide`/declared-candidate executor (no kind-specific scoring path).

- [x] D15b **EG-DECISION-ENGINE-R030 (wiring):** Wire the registry into the real `Decide` dispatch chokepoint: `candidates::declared_candidates` (the one function every `CandidateSource::Declared` request passes through, called from `candidates::read_candidates`, called from `stat_decide::decide_blocking`, which `handlers::decide::handle_decide` runs -- the handler `Method::Decide` dispatches to) now refuses an unregistered lane id before any feature schema is resolved, gated on `QuestionKind::IngestionLane`. Dispatch-level regression test through the served path: `src/server/handlers/decide/stat_tests/consumer_tests.rs::an_unregistered_ingestion_lane_is_refused_through_decide`. Remaining for this requirement: a real ingestion call site (e.g. `src/server/dispatch/router/source_ingest.rs`) that builds registry options for a request and calls `Decide`, plus an integration test selecting the expected lane for a representative input under a published feature schema/head (the requirement's acceptance test; needs ranking, not just refusal).

- [x] D16 **EG-DECISION-ENGINE-R031 (first slice):** Add the typed `EnrichmentCandidate`/`EnrichmentScheduleRequest` shape (expected value and cost on the Q32 scale) and `EnrichmentScheduleRequest::check()` refusal for a negative cost or a duplicate work id (`crates/eg-types/src/decision/statistical/enrichment_schedule.rs`). `as_declared_options()` turns a checked request into the `CandidateSource::Declared` set a `QuestionKind::EnrichmentSchedule` decision reads; no scheduling-specific comparison path is added. Remaining for this requirement: a real enrichment-scheduler call site that calls `Decide` over these options and prefers higher net value, plus an integration test confirming the higher-value candidate is selected under representative cost scenarios (the requirement's acceptance test).

- [x] D16 **EG-DECISION-ENGINE-R029 (first slice):** Add the typed `RetrievalPlanKind` registry (leanrag/reciprocal-rank-fusion/direct-sparql) and `RetrievalPlanRequest::check()` refusal for an unregistered plan id (`crates/eg-types/src/decision/statistical/retrieval_plan.rs`). `QuestionKind::RetrievalPlan` candidates still route through the ordinary generic `Decide`/declared-candidate executor (no kind-specific comparison path).

- [x] D16b **EG-DECISION-ENGINE-R029 (wiring):** Wire the registry into the real `Decide` dispatch chokepoint: `candidates::declared_candidates` now refuses an option id that claims the registry's reserved `retrieval-plan:` namespace (`RetrievalPlanKind::claims_registry_namespace`) but does not name a registered plan, gated on `QuestionKind::RetrievalPlan`, before any feature schema is resolved. The check is namespace-scoped rather than exhaustive like R030's: `QuestionKind::RetrievalPlan` is reused across the existing decide-consumer test suite (`declared_abstention`'s "plan-deep"/"plan-hyde" fixtures, reused by `reputation_tests.rs`/`retrieval_tests.rs`/`evaluator_tests.rs`) for ordinary declared-option mechanics unrelated to plan selection; gating on the whole option-id space would have refused all of them. Dispatch-level regression test: `src/server/handlers/decide/stat_tests/consumer_tests.rs::an_unregistered_retrieval_plan_is_refused_through_decide`. Remaining for this requirement: a real retrieval-dispatch call site (selecting among `RetrievalPathTemplate`s, `retrieval.rs`) that builds registry options for a request and calls `Decide`, plus an integration test selecting the expected plan for a representative request shape under a published feature schema/head (the requirement's acceptance test; needs ranking, not just refusal).

- [x] D17 **EG-DECISION-ENGINE-R106:** Add `every_standard_shape_has_a_conforming_template` to `src/server/graph_schema/swarm_topology_tests.rs`, proving a minimal template for each of the six standard shapes (single-agent, pipeline, fan-out/join, supervisor-to-workers, critique-loop, council) validates against the `core:swarm-topology-shapes@1` SHACL shapes landed for EG-DECISION-ENGINE-R105. No new Rust topology-contract type was needed; the gap was test coverage of the existing vocabulary and shapes, not missing fact-carrying capacity.

- [x] D18 **EG-DECISION-ENGINE-R075:** Add `two_entries_minting_the_same_component_id_are_rejected` (`src/server/handlers/admin/connector_pack/tests/content_model.rs`), proving the already-implemented `PackViolationCode::DuplicateComponentId` check (`validate_identity` in `import/validation.rs`) refuses two pack entries of the same kind and name but different URIs, before any state change. `PackEntryKind` structurally excludes `DecisionRecord`/`DecisionHead`/`DecisionPolicy` already (no variant exists to import); no code change was needed for the reserved-kind half of this requirement, only the duplicate-id test.

- [x] D19 **EG-DECISION-ENGINE-R059:** Add `only_independent_outcomes_join_and_a_pool_below_min_support_is_omitted` (`crates/eg-numeric/src/decision/aggregate.rs::tests`), the first test of `aggregate()`/`aggregate_log()` itself. Proves a self-reported evaluation is refused rather than counted as a trial, an independent one is counted, and a pool below `min_support` is omitted entirely from the output rather than reported with low support. The independent-evaluation label rule (`label_use`) was already tested via its two consumers (`stat_reputation.rs`, `stat_slate_attribution.rs`); this closes the gap in the aggregate function itself.

- [x] D16 **EG-DECISION-ENGINE-R061.1:** Add the shared `test-support` fixture `tool_entry_with_cost_latency` (`crates/eg-types/src/test_support/decision.rs`), with a unit test proving its declared cost/latency survive `CandidateFacts::from_entry`. **EG-DECISION-ENGINE-R061:** build on this fixture with a cross-path test asserting the exact-solver path and `CandidateView::from_component` read the same declared values from the same entry.

- [x] D16 **EG-DECISION-ENGINE-R078 split (2026-10-09):** `feature_profile()` in `crates/eg-capabilities/src/contract.rs` never named the `decide` feature, so a reissued contract freeze would silently omit it; `package_surface_is_complete_identical_and_receipt_bound` would fail under `--all-features` against the committed artifacts, which an authorized generation ceremony must resolve. Split into **EG-DECISION-ENGINE-R078.1** (typed `MissingFrozenFeature`/`require_feature_frozen` refusal, plus the `decide` entry, producer) and **EG-DECISION-ENGINE-R078.2** (reissue `contract/FREEZE-RECEIPT.json` and `contract/receipt.json`, consumer, blocked on R078.1).

- [x] D16 **EG-DECISION-ENGINE-R079.1 (producer slice of R079):** Add the typed `ContentClass` taxonomy (prose/document/comment eligible; generated/lockfile/vendored/minified excluded) and its pure `admits()` gate (`crates/eg-types/src/decision/ingestion_admission.rs`). No real ingestion call site, embedding client, cheaper-index skip, entropy/hash dedup, size bound, SQL-column cardinality rule, or retrieval-telemetry feedback is wired yet -- those remain open under `EG-DECISION-ENGINE-R079` as later `.2`/`.3`+ slices.

- [x] D16 **EG-DECISION-ENGINE-R080.1 (first slice of R080):** Add the typed `ProvenanceRung` six-level enum and the pure `resolve_rung` overwrite-protection rule (`crates/eg-types/src/decision/ingestion_ladder.rs`). Remaining for `EG-DECISION-ENGINE-R080`: the real per-rung extractors (AST walking, symbol resolution, statistical/community derivation, classical ML/NER, embeddings, an LLM call) and an ingestion call site that tags each derived fact through `resolve_rung`.

- [x] D16 **EG-DECISION-ENGINE-R085.1:** Add the typed `derive_legal_options` filter and `CandidateOption` (`crates/eg-numeric/src/decision/legal_option_filter.rs`), proving the type-level contract that a caller-supplied candidate reaches the derived set only if an admissibility predicate accepts it. The predicate here is a fixture closure; the real SHACL-shape / SPARQL-shortlist-backed predicate is `EG-DECISION-ENGINE-R085.2` (not in scope for this slice). Test: `crates/eg-numeric/src/decision/legal_option_filter.rs::tests::a_caller_supplied_option_that_fails_the_admissibility_predicate_is_excluded`.
- [x] D16b **EG-DECISION-ENGINE-R031 (wiring):** Wire the shape into the real `Decide` dispatch chokepoint: `candidates::declared_candidates` now refuses a `QuestionKind::EnrichmentSchedule` candidate carrying a negative `enrichment.cost_q32` fact, before any feature schema is resolved. Dispatch-level regression test through the served path: `src/server/handlers/decide/stat_tests/consumer_tests.rs::a_negative_enrichment_cost_is_refused_through_decide`. Remaining for this requirement: a real enrichment-scheduler call site that calls `Decide` over these options and prefers higher net value, plus an integration test confirming the higher-value candidate wins under representative cost scenarios (ranking, not just refusal -- this requirement's acceptance test).

- [x] D17 **EG-DECISION-ENGINE-R035 (first slice):** Add the typed `ExecutionRoutingCategory` enum naming all 7 routing categories the requirement enumerates (model/prompt/skill/tool/harness mode/account mode/sandbox configuration -- the last 3 had no type anywhere) and `ExecutionRoutingRequest::check()` refusal for an empty or duplicate candidate option id (`crates/eg-types/src/decision/statistical/execution_routing.rs`). Routing candidates still route through the ordinary generic `Decide`/declared-candidate executor. Remaining for this requirement: a contract test confirming `graph.decide()` returns a valid typed choice and evidence class for each of the 7 categories (the requirement's acceptance test).

- [x] D17b **EG-DECISION-ENGINE-R035 (closed on the contract-test reading):** Checked `spec.md`/`plan.md`/`requirements.md`/`architecture.md` for a routing-category field on the `Route` question or request: none exists -- all 7 categories share one `QuestionKind::Route`, so there is nothing a chokepoint could gate on the way `IngestionLane`/`EnrichmentSchedule` can (adding such a field would be a wire-protocol change, not a wiring fix, so it is out of scope here). Per the acceptance criterion's own wording ("a contract test confirms graph.decide() returns a valid typed choice and evidence class for each supported routing category"), added that contract test: `src/server/handlers/decide/stat_tests/consumer_tests.rs::graph_decide_answers_every_execution_routing_category` calls the real `handle_decide` once per category with representative declared candidates and asserts a typed `Explored` outcome with `EvidenceClass::Claim`. `ExecutionRoutingCategory` (D17) is kept as the typed name for the 7 categories; no further requirement-specific product code is planned for R035.

- [x] D18 **EG-DECISION-ENGINE-R015.1 (producer slice):** Add the typed, closed `StatisticalHeadKind::parse` (`crates/eg-types/src/decision/rapid_r015.rs`), refusing any head-kind name outside the two fitted heads the ladder actually uses -- in particular a generative-model label. Test: `crates/eg-types/src/decision/rapid_r015.rs::tests::refuses_a_generative_model_name`.

- [ ] D18 **EG-DECISION-ENGINE-R015.2:** Remaining scope from the R015 split (2026-10-09): wiring the ladder's head registry to construct heads only through this parse.

- [x] D18 **EG-DECISION-ENGINE-R044.1 (producer slice):** Add the typed `A2ARoutingResolution::new` refusal (`crates/eg-types/src/decision/rapid_r044.rs`) over the existing `ResolutionKind`/`EvidenceClass`, refusing an empty graph id and an abstention claiming proof-grade evidence. Remaining: the A2A task-submission entry point that constructs one of these per inbound task. Test: `crates/eg-types/src/decision/rapid_r044.rs::tests::refuses_an_abstention_claiming_proof_grade_evidence`.

- [ ] D18 **EG-DECISION-ENGINE-R044.2:** Remaining scope from the R044 split (2026-10-09): the A2A task-submission entry point that constructs one of these per inbound task and an integration test over the served path.

- [x] D18 **EG-DECISION-ENGINE-R056.1 (producer slice):** Add the typed `StatisticalSolverCertificate::verify` refusal on a stale model digest (`crates/eg-types/src/decision/rapid_r056.rs`), giving the statistical path the same certificate-verification shape as the core assembly solver. Remaining: routing an actual statistical-path solve through the exact 0-1 solver and this certificate. Test: `crates/eg-types/src/decision/rapid_r056.rs::tests::refuses_a_stale_model_digest`.

- [ ] D18 **EG-DECISION-ENGINE-R056.2:** Remaining scope from the R056 split (2026-10-09): routing an actual statistical-path solve through the exact 0-1 solver and this certificate shape.

- [x] D18 **EG-DECISION-ENGINE-R075.1 (producer slice):** Add `check_unique_component_ids` (`crates/eg-types/src/connector_pack/rapid_r075.rs`), the duplicate-component-id rejection the existing `PackViolationCode::DuplicateComponentId` code names but no test previously exercised. `PackEntryKind` already structurally excludes decision-owned kinds (no `DecisionRecord`/`DecisionHead`/`DecisionPolicy` variant exists), so that half of the requirement needed no new code. Remaining: wire the check into the real pack-import entry point. Test: `crates/eg-types/src/connector_pack/rapid_r075.rs::tests::refuses_a_duplicate_component_id`.

- [x] D18 **EG-DECISION-ENGINE-R075.2:** Wire `eg_types::connector_pack::rapid_r075::check_unique_component_ids` into the real pack-import entry point, `validate_index` (`src/server/handlers/admin/connector_pack/import/validation.rs`), via a new `check_duplicate_component_ids` call computing every entry's actual minted component id. Test: `src/server/handlers/admin/connector_pack/import/validation.rs::duplicate_component_id_wiring_tests::the_real_entry_point_calls_the_shared_duplicate_id_check`.

- [x] D18 **EG-DECISION-ENGINE-R083.1 (producer slice):** Add the typed `OptionFacts::encode` (`crates/eg-numeric/src/decision/rapid_r083.rs`), bounded by the declared `MAX_OPTION_CAPABILITIES` cardinality rather than by any text-token budget. Remaining: wire this encoding into the real `FeatureMatrix` computation in `decision::features`. Test: `crates/eg-numeric/src/decision/rapid_r083.rs::tests::scores_an_option_whose_capability_text_would_exceed_a_token_budget`.

- [ ] D18 **EG-DECISION-ENGINE-R083.2:** Remaining scope from the R083 split (2026-10-09): wiring this encoding into the real `FeatureMatrix` computation in `decision::features`.

- [x] D18 **EG-DECISION-ENGINE-R078.2:** Skipped this lane (2026-10-09) -- it requires the authorized contract-generation ceremony to regenerate and commit `contract/FREEZE-RECEIPT.json`/`contract/receipt.json` under `--all-features`, which needs a cargo build this lane does not run. R078.1 (its blocker) is confirmed `LANDED` on `origin/main` (commit `1527a97f211825f02feaa1f9475c494a822918f5`), so R078.2 is now unblocked for whichever lane next runs that ceremony.

- [ ] D20 **EG-DECISION-ENGINE-R001.1 (reopened 2026-10-09, audit gap):** Constraint and Entailment resolution kinds are actually constructed. `graph.assemble()` and `stat_decide()` must construct `ResolutionKind::Constraint` and `ResolutionKind::Entailment` for representative cases. Today assemble only emits Optimization/Abstention and stat_decide only Statistical/Abstention, so the two declared kinds are never produced. Test: `crates/eg-types/src/decision/rapid_r001.rs::tests::assemble_and_stat_decide_each_construct_a_constraint_and_an_entailment_resolution`.
- [ ] D21 **EG-DECISION-ENGINE-R012.1 (reopened 2026-10-09, audit gap):** Slot-level crediting across questions sharing an assembly template. When multiple questions are answered against the same assembly template, outcomes must be credited at the slot level across the shared template; today only whole- or sub-slate crediting exists. Test: `crates/eg-types/src/decision/rapid_r012.rs::tests::credits_outcomes_at_the_slot_level_across_a_shared_assembly_template`.
- [ ] D22 **EG-DECISION-ENGINE-R026.1 (reopened 2026-10-09, audit gap):** Calibration-weighting half of audit sampling is tested in-crate. The audit-flag sampling half of inverse-probability-weighted audit sampling has a bound test, but the calibration-weighting half (`pub(crate) audit_weight`/targets) has no in-crate `#[cfg(test)]` test confirming calibration correctly applies the inverse-probability weight. Test: `crates/eg-numeric/src/decision/audit.rs::tests::calibration_applies_the_pub_crate_inverse_probability_audit_weight`.
- [ ] D23 **EG-DECISION-ENGINE-R032.1 (reopened 2026-10-09, audit gap):** QuestionKind::ResolveEntity path with isotonic calibration and a conformal guarantee. No `QuestionKind::ResolveEntity` path exists wiring the binary entity-matching head with isotonic calibration and a conformal guarantee validated against a gold label set. Test: `src/server/handlers/decide/stat_tests/consumer_tests.rs::resolve_entity_question_applies_isotonic_calibration_and_reports_a_conformal_guarantee`.
- [ ] D24 **EG-DECISION-ENGINE-R033.1 (reopened 2026-10-09, audit gap):** QuestionKind::SchemaMapping path from a SQL column to an ontology property. No `QuestionKind::SchemaMapping` path exists routing a SQL-column-to-ontology-property mapping decision through the typed decision mechanism; today such mappings are unverified direct assignments. Test: `src/server/handlers/decide/stat_tests/consumer_tests.rs::schema_mapping_question_maps_a_sql_column_to_the_expected_ontology_property_with_evidence_class`.
- [ ] D25 **EG-DECISION-ENGINE-R038.1 (reopened 2026-10-09, audit gap):** Typed ToolInvocation / pre-tool risk concept exists. No risk/pretool/ToolInvocation concept exists in the engine. Add the typed representation a caller can score before dispatching a tool invocation, advisory only. Test: `crates/eg-types/src/decision/rapid_r038.rs::tests::returns_an_advisory_risk_score_for_a_candidate_tool_invocation_without_gating_execution`.
- [ ] D26 **EG-DECISION-ENGINE-R039.1 (reopened 2026-10-09, audit gap):** Routing prefers the lower-cost legal option when accounting data distinguishes candidates. Only abstain-on-missing-cost exists today. Routing has no preference for the lower-cost legal option when usage/cost accounting data is actually present and distinguishes candidates. Test: `src/server/handlers/decide/stat_tests/consumer_tests.rs::routing_prefers_the_lower_cost_legal_option_when_accounting_data_distinguishes_candidates`.
- [ ] D27 **EG-DECISION-ENGINE-R041.1 (reopened 2026-10-09, audit gap):** Connector-event evaluate-only triage path with sampled audit logging. No connector-event evaluate-only triage path or sampled-audit mechanism exists. Inbound connector events must be classified via the decision ladder in evaluate-only mode with a sampled fraction logged for audit. Test: `src/server/handlers/decide/stat_tests/consumer_tests.rs::connector_event_triage_is_evaluate_only_and_logs_a_sampled_fraction_for_audit`.
- [ ] D28 **EG-DECISION-ENGINE-R054.1 (reopened 2026-10-09, audit gap):** TTL/SHACL generation artifact for the Decide/DecisionFit/DecisionEval contract exists. No TTL/SHACL generation artifact exists for the Decide, DecisionFit, and DecisionEval contract and DecisionRecord v2 schema. Test: `contract/tests/decide_contract_generation.rs::ttl_and_shacl_artifacts_are_generated_and_match_the_decide_fit_eval_contract`.
- [ ] D29 **EG-DECISION-ENGINE-R072.1 (reopened 2026-10-09, audit gap):** Solver node count and optimality gap are emitted on the Assemble/solver path. Completion telemetry and capture regression implemented; native solver-path acceptance execution remains pending. Exact production telemetry helper tests pass independently. Test: `crates/eg-compute/tests/solve/instrumentation.rs::assemble_emits_solver_node_count_and_optimality_gap_metrics`.
- [ ] D30 **EG-DECISION-ENGINE-R084.1 (reopened 2026-10-09, audit gap):** ECE/coverage measurement across option sets of varying size. No expected-calibration-error/coverage measurement exists across option sets of varying size. Fixtures are fixed at 3 options, so the declared coverage-holds-without-a-size-specific-parameter claim is untested. Test: `tests/decision/calibration_coverage.rs::measures_ece_and_empirical_coverage_across_option_sets_of_varying_size`.
- [ ] D31 **EG-DECISION-ENGINE-R088.1 (reopened 2026-10-09, audit gap):** Belief-as-of-t uses the bitemporal AS OF mechanism, not a bespoke per-feature recompute. Belief-as-of-t is currently a bespoke per-feature recompute rather than going through the required bitemporal AS OF query mechanism over temporal slices. Test: `crates/eg-types/src/decision/rapid_r088.rs::tests::belief_as_of_successive_prefixes_matches_direct_bitemporal_as_of_queries`.
- [ ] D32 **EG-DECISION-ENGINE-R099.1 (reopened 2026-10-09, audit gap):** Wikidata alignment connector and schema_mappings SHACL validation exist. No Wikidata connector, schema_mappings data, or SHACL test exists supplying the Region/Country/Taxon/Food/Nutrient entity-resolution keys EG-DECISION-ENGINE-R032 consumes. Test: `tests/connectors/wikidata_alignment.rs::schema_mappings_validate_against_world_model_shacl_shapes_and_resolve_expected_entities`.
- [ ] D33 **EG-DECISION-ENGINE-R109.1 (reopened 2026-10-09, audit gap):** Topology admissibility entailment test varies the composed schema digest. Entailment lives in `topology_schema.rs::entail`, not the named `admissible.rs`, and no test varies the composed schema digest to show the derived TopologyAdmissible facts and eliminated options actually change. Test: `crates/eg-compute/src/assemble/topology/admissible.rs::tests::varying_the_composed_schema_digest_changes_derived_topology_admissibility`.
- [ ] D34 **EG-DECISION-ENGINE-R110.1 (reopened 2026-10-09, audit gap):** Topology brute-force oracle property test covers n<=20 variables. No n<=20 property oracle for topology exists; only 9 fixed cases on one template are tested today. Test: `crates/eg-compute/src/assemble/topology/model.rs::tests::property_oracle_agrees_with_the_solver_for_instances_up_to_twenty_variables`.
- [ ] D35 **EG-DECISION-ENGINE-R111.1 (reopened 2026-10-09, audit gap):** Topology selection is exposed as its own standalone typed Decide question. No dispatch-level standalone typed Topology Decide question exists; topology selection is still only reachable inside `graph.assemble()`. Test: `src/server/handlers/decide/stat_tests/consumer_tests.rs::a_standalone_topology_decide_question_returns_a_typed_decision_record`.
- [ ] D36 **EG-DECISION-ENGINE-R117.1 (reopened 2026-10-09, audit gap):** UQL/SQL/PromQL golden-equivalence and 4-metric DecisionEval report test. No UQL/SQL/PromQL golden-equivalence test or 4-metric DecisionEval report test exists for the DERIVE series functions. Test: `crates/eg-tsdb/tests/derive_golden.rs::uql_sql_promql_outputs_agree_on_golden_reference_and_decisioneval_reports_all_four_feature_power_metrics`.
- [ ] D37 **EG-DECISION-ENGINE-R118.1 (reopened 2026-10-09, audit gap):** Reputation-view test exists outside the unrelated privacy regression fixture. No real test of the reputation materialization exists. Only incidental fixture text inside an unrelated privacy regression test currently touches this path. Test: `crates/eg-numeric/src/decision/reputation.rs::tests::sparse_pool_abstains_and_authority_policy_retrieval_candidate_and_uql_source_reliability_agree`.
- [ ] D38 **EG-DECISION-ENGINE-R126.2.1 (reopened 2026-10-09, audit gap):** [VISION-Q PRIORITY] Capability-coverage query is served as an RPC with a generated client. `capability_coverage_query` exists but there is no served RPC beside `AgentAssemble` and no generated client method, so no consumer can actually call it. Test: `tests/contract/capability_coverage_served.rs::capability_coverage_is_served_beside_agent_assemble_with_a_generated_client_method`.
- [ ] D39 **EG-DECISION-ENGINE-R127.2.1 (reopened 2026-10-09, audit gap):** [VISION-Q PRIORITY] GuardrailRule storage/publish path and served guardrail-entailment RPC. `guardrail_entailment_query` has no `GuardrailRule` storage/publish path and no served RPC, so there is nothing a tenant can publish rules into or call. Test: `tests/contract/guardrail_entailment_served.rs::guardrail_entailment_is_served_from_a_published_rule_set_with_a_generated_client_method`.

## Evidence format

For each stable ID, record: `ID | sequence | delivery | acceptance | main SHA | tests/workflow | reviewed date | notes`. Until a row has an exact commit and passing required tests, use `WAITING / NOT_RUN`, `IN_PROGRESS / PARTIAL`, or `SOURCE_LANDED / PARTIAL` as observed. Do not infer acceptance from a sequence label. `RETIRED` requires the superseding contract and removal test. Keep evidence records in this repository alongside the spec so external contributors can audit them.

## Initial known evidence and unresolved qualification

Existing source modules and tests provide foundations, but this spec does not assign new `ACCEPTED` states from historical branch notes. The first implementation task must inspect current published main and fill the evidence table. The decision ladder may already be source landed in part; served, hosted, replay and security proofs remain separate checks.

## Decomposition children (tracked)

- [x] **EG-DECISION-ENGINE-R015:** No generative model weights inside the decision engine (rollup)
- [x] **EG-DECISION-ENGINE-R044:** A2A task routing through the decision ladder (rollup)
- [x] **EG-DECISION-ENGINE-R045:** Smallest covering tool subset within a context budget
- [x] **EG-DECISION-ENGINE-R056:** Statistical-path release of the exact solver (rollup)
- [x] **EG-DECISION-ENGINE-R083:** Options encode structured facts, not option-label text (rollup)
- [x] **EG-DECISION-ENGINE-R085:** Legal option sets are derived from the ontology, never received as-is
- [ ] **EG-DECISION-ENGINE-R090:** Resident native decision-scorer head in eg-decision
- [x] **EG-DECISION-ENGINE-R090.1:** Typed resident-scorer budget (producer-first split of R090)
- [ ] **EG-DECISION-ENGINE-R093:** Resident scorer is trained from the served synthetic-episode generator
- [x] **EG-DECISION-ENGINE-R093.1:** Typed served-path provenance and ground-truth verification
- [ ] **EG-DECISION-ENGINE-R093.2:** Complete served corpus export and training acceptance.
- [ ] **EG-DECISION-ENGINE-R093.2.1:** Collect seeded results through the existing served client.
- [ ] **EG-DECISION-ENGINE-R093.2.2:** Bind served labels to collected results and refuse invalid provenance.
- [ ] **EG-DECISION-ENGINE-R093.2.3:** Verify training consumes real served episodes.
- [ ] **EG-DECISION-ENGINE-R102:** Schema repair proposals pass a decided mapping and shadow-branch check
- [x] **EG-DECISION-ENGINE-R102.1:** Typed schema-repair activation gate (producer-first split of R102)
- [ ] **EG-DECISION-ENGINE-R113:** EG commits capacity acquisition atomically with synthesis evidence
- [x] **EG-DECISION-ENGINE-R113.1:** Typed atomic capacity/synthesis-evidence commit outcome (producer-first split of R113)
- [ ] **EG-DECISION-ENGINE-R113.2:** Atomic commit transaction (planned; not run)
- [ ] **EG-DECISION-ENGINE-R113.2.1:** Refuse an unsupported transaction boundary (planned; not run)
- [ ] **EG-DECISION-ENGINE-R113.2.2:** Accepted transaction fixture (planned; not run)
- [ ] **EG-DECISION-ENGINE-R113.2.3:** Denied transaction fixture (planned; not run)
- [ ] **EG-DECISION-ENGINE-R113.2.4:** Transaction recovery fixture (planned; not run)
- [ ] **EG-DECISION-ENGINE-R113.3:** One retry after capacity denial (planned; not run)
- [x] **EG-DECISION-ENGINE-R113.3.1:** Consume the typed allowance once; direct native module tests pass. Durable and handler enforcement remain open.
- [ ] **EG-DECISION-ENGINE-R113.3.2:** Durable consumption fixture (planned; not run)
- [ ] **EG-DECISION-ENGINE-R113.3.3:** Handler retry fixture (planned; not run)
- [ ] **EG-DECISION-ENGINE-R116:** Consumers reach EG only through the versioned client contract
- [x] **EG-DECISION-ENGINE-R116.1:** A typed gate admits only the published client-contract call path
- [ ] **EG-DECISION-ENGINE-R116.2:** Every generated client method is wired through the contract-only gate

- [x] EG-DECISION-ENGINE-R090.1: Run six native budget tests and bind exact boundary/refusal coverage.
- [ ] EG-DECISION-ENGINE-R090: Retain the separate compiled-scorer benchmark obligation.

- [ ] EG-DECISION-ENGINE-R090.2: Verify compiled in-process scoring without external runtimes.
- [ ] EG-DECISION-ENGINE-R090.3: Measure parameter count, memory, binary footprint, and p99 CPU latency.

- [x] EG-DECISION-ENGINE-R074: Check component-kind closure documentation with positive and adversarial source fixtures.
- [x] EG-DECISION-ENGINE-R123: Scan canonical solve DTO declarations and reject planted duplicates.

## Remaining integration acceptance

- [ ] **EG-DECISION-ENGINE-R079.2:** Embedding admission at the ingestion boundary; producer tests do not close this integration requirement.
- [ ] **EG-DECISION-ENGINE-R079.3:** Duplicate and size admission; producer tests do not close this integration requirement.
- [ ] **EG-DECISION-ENGINE-R079.4:** SQL column admission; producer tests do not close this integration requirement.
- [ ] **EG-DECISION-ENGINE-R079.5:** Retrieval feedback admission; producer tests do not close this integration requirement.
- [ ] **EG-DECISION-ENGINE-R080.2:** Served enrichment ladder qualification; producer tests do not close this integration requirement.
- [ ] **EG-DECISION-ENGINE-R102.2:** Served shadow repair qualification; producer tests do not close this integration requirement.
