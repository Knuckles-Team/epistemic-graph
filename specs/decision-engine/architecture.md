# Architecture and contracts

## 1. Boundaries and reuse

The canonical decision DTOs live in `crates/eg-types/src/decision/` (`request`, `record`, `derivation`, `policy`, `statistical`, `jobs`, `topology`, `digest`, `errors`, `replay`). The exact 0–1 engine and certificate verifier live in `crates/eg-compute/src/solve/`; statistical feature extraction, scoring, calibration, replay and promotion live in `crates/eg-numeric/src/decision/`. The server's `src/server/handlers/decide/` evaluates requests, while `src/server/persistence/decision_record/` and `decision_jobs.rs` own durable records and jobs. Python wrappers in `epistemic_graph/decision_client.py`, `decision_stat.py` and generated contract modules call the same served methods. Extend these surfaces; do not build a parallel Python or application-side decision engine.

The Agent Library owner store is the write authority for library decision records and component revisions. Graph `MutationBatch` is the write authority for graph-derived summary nodes. Blob CAS holds bulky feature inputs and draft heads with a held reference. The normal guarded component publish path is the sole promotion authority for a `DecisionHead` after a matching `DecisionEval` receipt. A connector pack cannot publish `DecisionRecord`, `DecisionHead` or `DecisionPolicy` entries.

## 2. Evaluation pipeline

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

## 3. Exact solve and certificate

Create binary selection variables for candidate components and, where required, template/slot choices. Constraints include: each required capability is covered by an eligible selected component; required dependencies imply selection; incompatible options cannot coexist; each required slot has exactly one assignment; cardinality and integer budget bounds hold; model and tool modality/schema constraints hold. The objective is a sequence of integer levels: first hard feasibility, then policy-defined utility, resource use and stable tie-break. Preprocess dominated or impossible options, seed an incumbent greedily, then branch and bound with an admissible lower bound until optimality or node budget. A budget-exhausted answer may return an incumbent and proven gap only if policy permits that gap; otherwise abstain. No fixed maximum number of feasible assemblies substitutes for work budgeting.

The independent certificate verifier checks input digest, sorted variable mapping, all constraints against the proposed assignment, objective recomputation, lower-bound validity, gap and solver version. It must not trust a claimed feasible assignment or bound. Why-not for a visible excluded option fixes that option on, re-solves under a separate bounded budget, and reports the resulting violation, worse objective, gap or budget exhaustion. A failed post-solve template/graph validator adds a no-good cut and re-solves within the original budget.

## 4. Statistical decision and evidence

Feature extraction is over the RLS-filtered candidate set. Candidate-local BM25 and visible-only vector scoring avoid corpus statistics and overfetch effects from invisible rows. The feature schema is versioned, records missingness and provenance, and includes only policy-approved facts: similarity, entailed coverage, evidence quality, belief/confidence, cost and latency. The resident option-marker scorer can encode state once and score each structured option in process; it must retain deterministic fixed-point output and CPU portability. Statistical heads are bounded, fitted components, not generative models; text generation and LLM weights stay outside EG.

Gold-labelled data and executed-option-only bandit data are different datasets. The latter requires exact action propensities, support diagnostics, inverse propensity or doubly robust estimators, effective sample size and censoring. Self-reported run outcomes cannot promote a head. Independent evaluations with sufficient trace fidelity and approved principal are joined to records; hierarchical pooling requires minimum support and coarse output to avoid individual-run inference. Conformal and selective-risk calibration use held-out or walk-forward folds, account for drift and explicitly abstain when their stated bound lacks support. `DecisionEval` receipts pin head, dataset window, policy, feature schema, metrics and thresholds. A policy change or head digest change invalidates the receipt for promotion.

## 5. Connector pack interaction

The existing pack types in `crates/eg-types/src/connector_pack/`, handlers in `src/server/handlers/admin/connector_pack/`, persistence in `src/server/persistence/connector_pack/` and Python `epistemic_graph/connector_pack.py` form one owner. The decision engine consumes published, tenant-visible component revisions and their annotations. It does not parse raw packs or trust client-provided digests. A pack entry's `provides`, `requires_capabilities`, modality, schema digest, cost, latency, model profile and `contract_pin` are publisher claims. Native capability IRIs can enter ontology derivation with that claim premise; unknown foreign IRIs are claim-only exact terms; malformed native IRIs are refused at import. Re-import does not alter a component revision when its own entry content is byte-identical, and `Withdrawn` members become ineligible immediately. A pack cannot insert decision-owned component kinds. The pack head and projection readiness marker must match before a projected ontology/shape fact is used as current.

Decide's contract at this boundary is narrow: it sees only validated component revisions and preserves publisher-claim class in every derivation, score and record. It rejects reserved decision-owned kinds and stale projection facts. Pack import atomicity, body storage and parser refusals are specified and tested by the pack owner independently.

## 6. Question-specific extensions

Typed question adapters translate retrieval, lane, enrichment, entity-resolution, schema-mapping, routing, risk, topology and recommendation requests into the same legal-set/evidence pipeline. `DecideText` is a dedicated UQL-like front end with quoted candidate blocks and typed parameters; malformed syntax returns a structured span/error and never falls through to ordinary query parsing. Topology adds bounded integer shape variables (width, rounds, role, capacity) and a composed-schema digest, then outputs a proposed `AgentGraph`; capacity acquisition and execution remain separate atomic operations. Entity resolution emits a proposed link and confidence/abstention, never an automatic identity merge. Finance recommendations are informational snapshots with strategy version, evidence and horizon; they cannot place trades. Graph-sourced record views must apply the same RLS as their candidate source.

## 7. Operational and quality rules

Expose metrics for resolution kind, evidence class, abstention reason, solver nodes and gap, stale catalog refusals, record replay failures, calibration coverage, drift, ESS and latency. Do not put raw tenant facts in metric labels. Contract generation, CCCC, KISS, Dupehound and zero-new-pair jscpd are repository quality checks; Rust formatting, Clippy and focused unit/served tests validate behavior. Hosted CI should create its own local tenant, seed fixtures and, when needed, start an ephemeral EG server. A pre-existing deployment, identity provider or secret is not a baseline PR prerequisite.
