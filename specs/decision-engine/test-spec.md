# Test specification

Tests are keyed to the published commit and record command, result, fixture version and workflow URL in an acceptance record. The baseline suite starts a disposable local EG instance and creates its own tenant, pack and graph fixtures. External services are optional deployment qualification, not a PR prerequisite.

## A. Contract and deterministic records

| Case | Setup and assertion |
|---|---|
| A1 method/profile parity | Generate Rust/Python/contract descriptors. `AgentAssemble`, `DecisionCommit`, `Decide`, `DecisionFit`, `DecisionEval`, pack import and content reads have exactly one reachable handler, stated scope, stability and feature profile. Generated files are fresh. |
| A2 canonical replay | Commit v1, v2 and v3 records with golden byte vectors on all supported release targets. Decode, re-encode, verify and compare digest/answer bytes. Unknown future version fails closed. Replaying stored inputs is stable even when live catalog changes. |
| A3 forged record | Mutate a candidate fact, premise class, solver bound, objective, policy digest or outcome while retaining an old digest. `DecisionCommit` refuses; no record/component/outbox write occurs. |
| A4 snapshot race | Publish or withdraw a candidate after evaluate and before commit. Expected catalog digest mismatch returns stale-catalog; unrelated publish outside the candidate universe does not. Retry same committed record is idempotent. |
| A5 limits | 64 KiB/64 candidate/32 capability limits accept boundary values and return typed `RecordTooLarge` or bounded refusal above them. Check no partial write. |

## B. Solver, entailment and explanation

Generate small 0–1 models with covering, implication, exactly-one, knapsack, compatibility and cardinality constraints; compare optimum and infeasibility to exhaustive enumeration. Repeat with shuffled input order and assert byte-identical answer/certificate. Corrupt each certificate field and confirm the independent verifier rejects it. Exhaust work budget with and without a policy-approved gap; confirm incumbent/gap reporting or typed abstention. Test no artificial result-count cutoff. Plant unknown cost and missing capability facts; neither may be treated as zero or proven true. Construct `is_a` paths whose weakest premise is claim or observation; derivation class remains proof while conclusion class weakens. A why-not re-solve must name a visible violation or worse objective and must not leak invisible alternatives. Fail a post-solve template validator and verify no-good-cut re-solve or budget abstention.

## C. Visibility, policy and statistics

Use two principals in one tenant plus a separate tenant. Seed an invisible candidate that would dominate a visible one; verify it changes neither shortlist, candidate-local BM25/vector score, count, digest, explanation, aggregate nor abstention reason. Test policy tightening accepted and each relaxation field rejected. A fitted head may reorder only legal candidates and may abstain; it cannot restore a denied candidate or authorize an irreversible action. With synthetic full-label and bandit datasets, assert exact propensities, zero-support refusal, clipped/SWITCH-IPS/doubly-robust calculations, ESS threshold, minimum support, censoring and drift behavior. A self-reported outcome, low-fidelity trace or unapproved commit principal cannot enter training. `DecisionFit` yields a draft; promotion without a matching `DecisionEval` receipt, or with changed head/policy/schema/dataset, is refused. Check HMAC exploration unpredictability to callers, exact logged propensity, budget enforcement and forbidden question classes. Walk-forward folds must purge and embargo as configured; a shared cap may not be exceeded across concurrent options.

## D. Served decision journey

Against a locally started server, publish a typed component with claim-class capabilities, call Python `AgentAssemble`, inspect record/certificate/why-not, call `DecisionCommit`, read it back, and pin it as graph synthesis evidence. Repeat through the generated Python method wrapper and a wire client. Call `DecideText` with quoted candidate blocks, typed parameters and malformed spans. Run a fit/evaluate/publish/decide cycle with an independently evaluated outcome; assert receipt and record lineage. A second tenant sees no record. For topology, verify capacity-premise inputs, bounded shape variables, fail-closed template shape, proposed graph and no capacity lease before separate acquisition. For entity resolution and finance, verify proposals/analysis only and no write or trade side effect.

## E. Pack facts at the decision boundary

Publish one pack-provided component with native and foreign capability annotations, cost, latency, modality, schema digest, model facts and `contract_pin`. Assemble from that component and assert every such premise remains a publisher claim in the record and explanation. The native capability may participate in an `is_a` derivation whose conclusion class is still claim; the foreign IRI remains an exact unentailed claim. Omit the member in a later valid pack and assert `Withdrawn` makes it ineligible. Re-import byte-identical entry content and assert its candidate digest and decision result do not churn. A stale projection marker must prevent a projected ontology or shape fact from entering a decision. Plant `DecisionRecord`, `DecisionHead` and `DecisionPolicy` pack entries and assert refusal before any decision-owned state changes. These probes use the pack API as a fixture; pack parser, body and crash recovery have their own acceptance suite.

Check metric labels omit raw facts and errors omit invisible component identifiers. Every negative probe asserts no durable state change. A served test uses disposable credentials and a local policy fixture.

## H. Quality and acceptance matrix

### EG-DECISION-ENGINE-R118 and EG-DECISION-ENGINE-R119 focused acceptance

| ID | Positive fixture | Refusal and boundary fixture |
|---|---|---|
| EG-DECISION-ENGINE-R118 | Two source pools with independently labeled successes/failures, one sparse pool and two tenant scopes yield reproducible posterior, Brier/log/decomposition/calibration scores; the governed value agrees in decision policy, retrieval rank and UQL. | Self-reported labels, invisible outcomes and missing pool history cannot improve another tenant's score; a caller-supplied weight cannot override the materialization; sparse history abstains. |
| EG-DECISION-ENGINE-R119 | A fixed bitemporal series produces exact purged/embargoed walk-forward folds, proportional shared-cap allocations, real trial-count DSR, split PBO, incumbent DM, option contributions and byte-identical sealed replay after checkpoint resume. | Future-known rows, overlap, changed source/head/policy digest, over-cap allocation, policy-dependent environment, bad supersedes digest and malformed `REPLAY WALK FORWARD` refuse without a sealed run or head promotion. |


For each changed code path run focused Rust/Python/client tests, `cargo fmt --check`, workspace Clippy with applicable features, generated contract freshness, CCCC, KISS, Dupehound and jscpd differential with zero new pairs. Hosted CI runs build/package tests for affected features and the locally provisioned served journey. Optional cloud/cluster/identity-provider tests run in a separate deployment qualification and cannot block normal external PRs merely because no live environment exists. An acceptance record is complete only when every relevant A–E case and gate has a result tied to the same published commit; `SOURCE_LANDED` stays distinct from `ACCEPTED`.
