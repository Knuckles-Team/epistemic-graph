# Native DSPy/DSRs integration analysis

Date: 2026-07-14

Validation update: 2026-07-16

Project: Epistemic Graph

Scope: DSRs architectural audit, current DSPy parity, clean-room native program
optimization substrate, cross-modal evidence validation, and durable job integration.

## Outcome

Do **not** vendor, semi-fork, or directly depend on DSRs. Its useful abstractions
should be implemented clean-room on top of Epistemic Graph's existing authority,
evidence, job, mutation, policy, and provider seams. The complete native program
layer is implemented as `eg-program` and wired into `AnalyticsJob` behind
`program-optimization`, which is included in `full`. Pure Rust owns selection,
search identity, composition, and promotion; provider-dependent work is expressed as
governed executable plans for the engine's existing runtimes.

This choice eliminates a second provider stack and avoids adding LiteLLM. It also
prevents a second raw-prompt cache, a second trace model, a second tool authority,
and a second program persistence format. The native crate and feature graph have no
runtime dependency on DSPy, DSRs, or LiteLLM.

## Audited upstreams

### DSRs

- Repository: <https://github.com/krypticmouse/DSRs>
- Audited commit: [`5bb65ca514dfc8240955dd38c870fba77a0bd629`](https://github.com/krypticmouse/DSRs/commit/5bb65ca514dfc8240955dd38c870fba77a0bd629)
- License: [Apache-2.0](https://github.com/krypticmouse/DSRs/blob/5bb65ca514dfc8240955dd38c870fba77a0bd629/LICENSE)
- State at audit: beta Rust port, crate version 0.7.3.

The repository landing page is informational and mutable. Every DSRs architectural,
dependency, safety, cache, and optimizer observation in this report applies only to
the audited commit above.

No DSRs source was copied. The implementation in this change is clean-room and uses
Epistemic Graph's existing public contracts. If code is ever vendored or forked
later, Apache-2.0 license/notice preservation and modification notices become
mandatory; that is not required by the present implementation.

### DSPy

Primary references used for the parity review are pinned to the audited stable tag:

- [DSPy 3.2.1 release](https://github.com/stanfordnlp/dspy/releases/tag/3.2.1)
- [DSPy 3.2.1 modules](https://github.com/stanfordnlp/dspy/blob/3.2.1/docs/docs/learn/programming/modules.md)
- [DSPy 3.2.1 optimizers](https://github.com/stanfordnlp/dspy/blob/3.2.1/docs/docs/learn/optimization/optimizers.md)
- [DSPy 3.2.1 language-model configuration](https://github.com/stanfordnlp/dspy/blob/3.2.1/docs/docs/learn/programming/language_models.md)
- [DSPy 3.3.0b1 release notes](https://github.com/stanfordnlp/dspy/releases/tag/3.3.0b1)

Stable DSPy was 3.2.1 during the audit. The 3.3.0b1 beta release described direction
such as typed provider-neutral request/response contracts, ReAct improvements,
parallel native tool calls, and lazy LiteLLM imports. Beta behavior was treated as a
directional signal, not as a stable API to copy. No claim in this report automatically
extends to later DSPy or DSRs revisions; upstream changes require a new, commit-pinned
parity review.

## Why DSRs should not become an engine dependency

| Concern | DSRs audit finding | Native Epistemic Graph decision |
|---|---|---|
| Provider transport | Uses Rig-backed OpenAI, Anthropic, Gemini, Groq, OpenRouter, and Ollama clients | One injected `ModelTransport`; no HTTP client or provider SDK in `eg-program` |
| Dependency stability | Git dependencies include pinned Facet/Rig revisions and a Minijinja branch reference | Existing workspace dependencies only; no Git dependency |
| Type/reflection safety | Typed signatures rely on Facet/BAML/jsonish; predictor reflection includes unsafe pointer work | Serde data contracts and opaque schema references; no unsafe code |
| Cache/privacy | Optional cache budgets 256 MiB memory and 1 GiB disk and can retain raw prompt/prediction material | No cache in `eg-program`; durable state cannot represent raw prompts or responses |
| External data loading | URL loading uses blocking HTTP without engine egress/SSRF governance | No URL field and no network client |
| Tool authority | ReAct tools execute outside Epistemic Graph policy/mutation authority | Tools are opaque references resolved by the verified engine boundary |
| Trace model | In-memory mutex trace is primarily debugging state | Opaque trace/evidence references feed durable job and evidence contracts |
| Persistence | Predictor state is library-local; no graph-native governed revision ledger | Candidate output is a typed job result; promotion can commit only through `ChangeEnvelope`/`MutationBatch` |
| Evaluation | Evaluation is sequential and library-local | Candidate-specific evaluation summaries require evidence and per-modality scores |
| Optimizer completeness | COPRO, GEPA, instruction-only MIPROv2, and Pareto helpers exist; important DSPy optimizers are absent or partial | All 13 families have native program-layer behavior; external inference/similarity/evaluation/training is an explicit governed plan, never a fallback |

## Implemented architecture

### Durable program contract

`eg-program` defines:

- typed input/output signatures backed by opaque schema and instruction references;
- module contracts for Predict, ChainOfThought, ReAct, ProgramOfThought, BestOfN,
  Refine, RLM, Parallel, MultiChainComparison, and CodeAct;
- adapter contracts for Chat, JSON, XML, TwoStep, Document, and Citations;
- immutable program revision lineage and opaque tool references;
- governed training examples, train/validation/test splits, outcome/score metadata,
  privacy attestation, and exact evidence loci;
- deterministic candidate/checkpoint/result identities;
- candidate-specific evaluation evidence and promotion policy;
- an injected, provider-neutral, reference-only `ModelTransport`;
- a `GovernedPromotionSink` that accepts only a validated `ChangeEnvelope` and
  returns the existing `MutationBatchCommit`.

The contract has no endpoint, credential, local path, host/user identity, raw prompt,
raw output, or arbitrary response-body field.

### Native optimizer surface

| Optimizer | Status | Notes |
|---|---|---|
| LabeledFewShot | Native | Evidence-aware, deterministic demonstration selection |
| BootstrapFewShot | Native | Selects successful observed traces only |
| BootstrapFewShotWithRandomSearch | Native | Deterministic seeded candidates, bounded heap selection |
| Avatar | Governed model-transport plan | Comparator-driven positive/negative tool-trace analysis materializes an opaque tool-policy artifact |
| COPRO | Native plan + materializer | Instruction-proposal steps use the existing model transport; ranked artifacts become candidates |
| MIPROv2 | Native plan + search | Instruction proposals combine with seeded demonstration search |
| SIMBA | Native plan + materializer | Trace reflection produces evidence-backed reflection artifacts |
| GEPA | Native plan + materializer | Pareto reflection preserves artifact modality coverage |
| InferRules | Native plan + materializer | Governed rule proposals become deterministic candidates |
| BetterTogether | Native composite plan | Instruction and trainer outputs join through native composition |
| BootstrapFinetune | Native trainer plan | Emits an opaque model-profile artifact; weights never enter the contract |
| KnnFewShot | Native graph-kernel plan | Evidence-backed similarity observations are deterministically ranked |
| Ensemble | Native | Component candidates and member lineage are composed in Rust |

There are no aliases, compatibility fallbacks, or silent substitutions. A plan is
the exact provider-free execution contract for an existing engine runtime. Each
step includes its executor, opaque inputs/outputs/dependencies, modalities, and hard
operation budget. Resubmission with materialized `OptimizerArtifact` values completes
candidate construction deterministically.

### Cross-modal coverage

| Modality | Native evidence contract | Source fixture present |
|---|---:|---:|
| Text | Character range | Yes |
| Document | Character range | Yes |
| Image | Rectangle | Yes |
| Audio | Time range | Yes |
| Video | Time/frame range | Yes |
| Graph | Versioned row | Yes |
| Table | Cell range/versioned row | Yes |
| Time-series | Time range | Yes |
| Vector | Versioned row | Yes |
| Spatial | Rectangle/point | Yes |
| Tensor | Cell range/versioned row | Yes |
| Code | Revision-scoped symbol | Yes |
| Trace | Trace/span | Yes |
| Binary | Versioned row | Yes |

The fixtures build one governed training example for every modality, compile and
promote an evidence-backed candidate, round-trip the result through MessagePack,
validate deterministic random search and authority rebinding, and verify that every
optimizer returns candidates or governed steps covering all fourteen modalities.
Model calls remain the responsibility of the single engine transport; there is
intentionally no second provider integration to test.

### Job and mutation integration

`JobKind::ProgramOptimize` carries a bounded nested MessagePack
`OptimizationRequest`. At submission, the server:

1. performs graph ACL validation and stamps the live graph snapshot;
2. bounded-decodes the typed request;
3. validates optimizer-specific model/evaluator/trainer budgets and artifact kinds;
4. replaces caller program, evidence, and artifact scope with verified authority;
5. re-encodes only the governed reference-only request;
6. injects the mandatory `program.optimization` worker capability;
7. submits through the existing analytics-job MutationBatch.

The local worker uses the existing durable lease/fencing state machine, observes
cancellation/deadline/CPU budget, stages a typed KnowledgeBatch result, validates that
the result contains only governed references and fixed modality/optimizer/step
labels, and publishes candidate or executable plan-step rows through the existing
MutationBatch coordinator. A selected program
revision has no direct write API: `PromotionCommit` requires candidate identity and
content digest to match a valid `ChangeEnvelope` before the existing mutation
authority may commit it.

### Complexity and resource posture

- Opaque identifiers and content identities use deterministic SHA-256.
- Policy, modality, evaluation, and candidate indexes use ordered maps/sets.
- Random-search top-k selection is `O(n log k)` memory-bounded heap work per
  candidate (`k <= 256`), rather than an `O(n log n)` full sort and retained copy.
- Hard limits cap request bytes/depth/items, corpus examples, signature fields,
  tool/input/evidence references, optimizer artifacts, candidates, demonstrations,
  plan steps, model calls, evaluator calls, and trainer steps.
- Candidate generation is cancellable and executes on one blocking worker, while
  the async coordinator renews its lease and applies job CPU/deadline/cancel policy.
- No cache or model runtime is allocated by deterministic native optimization.

## Security and privacy review

- No raw PII, local identifier, path, endpoint, credential, prompt, completion, or
  arbitrary evaluator rationale is representable in durable program contracts.
- `PrivacyAttestation` must explicitly confirm that raw PII and local identifiers
  were not persisted.
- Every example's complete policy envelope must equal the program policy; matching
  only a tenant or access-policy identifier is insufficient.
- Every evidence address must match its declared modality.
- Verified server authority replaces every caller policy envelope and evidence
  policy reference before persistence.
- Candidate promotion requires non-empty evidence, aggregate improvement,
  per-modality non-regression, and all observed modalities by default.
- Candidate evaluations for unknown candidate identities are rejected.
- Model calls are opaque-reference-only and must be implemented by the engine's
  governed provider adapter.
- Plan step DAGs must be topologically ordered, reference-only, bounded,
  unique-output, and use the fixed executor for their step kind.
- Optimizer artifacts outside the requested optimizer vocabulary are rejected, as
  are KNN observations for examples outside the governed corpus.

## Program-layer parity boundary

The new plane unifies program identity, examples, evidence, evaluation, all thirteen
optimizer families, job execution, and promotion authority with Epistemic Graph.
Instruction search, reflective search, KNN, ensemble composition, and finetune
planning are present at the native program layer. Inference and weight updates remain
operations of the engine's existing runtimes by design; exact work is represented by
governed plan-step rows and evidence-backed artifacts. Adding DSRs, DSPy, LiteLLM,
or a parallel provider path would violate this architecture.

ReAct, ProgramOfThought, BestOfN, Refine, RLM, Parallel, MultiChainComparison, and
CodeAct are typed module kinds. Their tool/model execution continues through the
engine's existing tool and model authorities rather than an optimizer-owned client.

This is deliberately a program-layer and governance claim, not a claim of complete
behavioral parity with every DSPy runtime, provider, teleprompter implementation, or
future release. The focused tests establish deterministic native contracts and
authority boundaries. They do not establish end-to-end semantic quality for a live
model, provider-specific equivalence, distributed-scale behavior, or full DSPy
compatibility.

## Validation status and serialized commands

The later serialized validation lane ran the focused Rust targets with one build job:

```bash
CARGO_BUILD_JOBS=1 CARGO_INCREMENTAL=0 cargo test -p eg-program
```

The `eg-program` target passed its five native integration cases, covering governed
contracts, every optimizer family, all fourteen modalities, plan materialization,
deterministic search, authority rebinding, serialization, and promotion validation.
Focused server job-handler regressions for program-optimization authority and bounded
complexity also passed. The feature is included by the repository's `full` feature.

### Certified 2.23.0 artifact evidence

The subsequent serialized artifact lane certified the Epistemic Graph 2.23.0 wheel.
The privacy-clean archive contained exactly 20 members, an exact `RECORD` with its
self-row and archive member last, and one CycloneDX SBOM with no build-local
`path+file` reference. The three installed console scripts retained executable
mode, and the folded ABI3 numeric extension was present with executable mode. Wheel
metadata exposed the `full` self-requirement through
`epistemic-graph[owl,lmcache,numeric]`. The accompanying release-contract,
server no-PyO3 boundary, documentation-contract, and source/privacy gates passed.

This artifact result is packaging and static release evidence. It does not establish
a live optimizer job, semantic quality, provider equivalence, distributed behavior,
or complete DSPy compatibility.

### Deterministic native build-path defense

The release pipeline now normalizes retained native build paths after the numeric
extension and SBOM are folded into the wheel and before the byte-level privacy audit.
`scripts/normalize_wheel_build_paths.py` derives concrete build roots in memory,
matches only exact path-prefix boundaries, and replaces UTF-8/native and UTF-16LE
forms with deterministic identity-neutral aliases of identical width. Fixed-width
replacement preserves native layout; ZIP member metadata and executable bits are
preserved; and `RECORD` is rebuilt exactly with its own row last. The normalizer
never prints a concrete source value.

Focused tests prove that a retained path in a native member is removed, an unrelated
identifier such as `root_cause.rs` remains unchanged, Windows wide-character paths
are handled without resizing payloads, executable mode survives, `RECORD` hashes and
sizes are rebuilt, and a second normalization is idempotent. Rust path remapping
remains defense in depth; the artifact normalizer covers C/C++ dependencies whose
`__FILE__` expansion is outside `rustc` remapping.

No broad workspace test, all-features check, complete server integration suite,
distributed runtime campaign, or live-model semantic comparison was established by
this evidence. In particular, this report does **not** claim that
`cargo check --workspace --all-features --locked` passed or that native behavior is
fully equivalent to DSPy. Those remain distinct release and semantic certification
gates. Future native commands must continue to run serially with one build job.

Runtime acceptance should submit each optimizer, poll it to `Succeeded`, execute any
returned plan-step DAG through the named existing runtime, resubmit with governed
artifacts, attach candidate evaluation evidence, and confirm that only the selected
result can form a valid `PromotionCommit`/`ChangeEnvelope`.

## Files changed by this workstream

### Epistemic Graph

- `Cargo.toml`
- `README.md`
- `mkdocs.yml`
- `crates/eg-types/Cargo.toml`
- `crates/eg-types/src/jobs.rs`
- `crates/eg-program/Cargo.toml`
- `crates/eg-program/README.md`
- `crates/eg-program/src/lib.rs`
- `crates/eg-program/src/contracts.rs`
- `crates/eg-program/src/optimizer.rs`
- `crates/eg-program/src/plan.rs`
- `crates/eg-program/src/transport.rs`
- `crates/eg-program/src/commit.rs`
- `crates/eg-program/tests/native_program.rs`
- `src/server/handlers/jobs.rs`
- `epistemic_graph/client.py`
- `docs/architecture/native-program-optimization.md`
- `docs/capabilities.md`
- `.github/workflows/release-build.yml`
- `scripts/normalize_wheel_build_paths.py`
- `tests/test_release_full_wheel_contract.py`
- `tests/test_wheel_privacy.py`
- `reports/native-dspy-dsrs-integration-analysis-2026-07-14.md`
