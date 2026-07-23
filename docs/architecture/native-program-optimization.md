# Native program optimization

Epistemic Graph has one graph-native plane for typed LM programs and governed
self-improvement: `eg-program`, enabled by `program-optimization` and included in
`full`. It replaces a separate Python DSPy/LiteLLM optimizer and provider plane.

## Authority and data flow

```text
verified request authority
          |
          v
ProgramOptimize -- bounded MessagePack --> durable fenced analytics job
          |
          +--> Rust selection/composition kernel --> program candidates
          |
          +--> governed plan steps --> existing similarity/model/evaluator/trainer runtime
                                              |
                                              v
                                  opaque optimizer artifacts
                                              |
                                              v
                              deterministic candidate materialization
                                              |
                                              v
                    evaluation evidence --> ChangeEnvelope / MutationBatch
```

The plan route is a supported execution contract, not an alternate or downgraded
optimizer. It keeps provider calls and model-weight training behind the engine's
existing policy, egress, secret, timeout, redaction, and trace authorities. Neither
the request nor a durable result can contain an endpoint, credential, prompt,
response body, model weight, user/host identity, or local path.

## Program and evidence contract

A program revision contains a typed input/output signature, module kind, adapter,
opaque tool references, revision lineage, and policy. Examples contain only opaque
input/output/feedback/trace references, bounded scores, and located evidence. The
corpus carries a privacy attestation proving that raw PII and local identifiers were
not persisted. Server ingress replaces all caller policy scope, including optimizer
artifact scope, with verified authority.

| Program modality | Required evidence address |
|---|---|
| Text, document | Character range |
| Image | Rectangle |
| Audio | Time range |
| Video | Time or frame range |
| Graph, vector, binary | Versioned row reference |
| Table, tensor | Cell range or versioned row |
| Time-series | Time range |
| Spatial | Rectangle or point |
| Code | Revision-scoped symbol |
| Trace | Trace/span reference |

Every optimizer preserves this modality set on candidates and plan steps. Promotion
can require coverage and non-regression for every modality observed in the corpus.

## Optimizer surface

The wire contract exposes exactly **13 optimizer families**:
`labeled_few_shot`, `bootstrap_few_shot`,
`bootstrap_few_shot_with_random_search`, `avatar`, `knn_few_shot`,
`ensemble`, `copro`, `mipro_v2`, `simba`, `gepa`, `infer_rules`,
`bootstrap_finetune`, and `better_together`.

| Family | Native program-layer behavior |
|---|---|
| LabeledFewShot, BootstrapFewShot | Deterministic, evidence-aware covering selection |
| BootstrapFewShotWithRandomSearch | Seeded bounded-heap search over demonstration counts and sets |
| Avatar | Comparator plan contrasts successful and failed governed tool traces, emits a `tool_policy` artifact, then deterministically materializes a policy-referencing candidate |
| KNNFewShot | Graph-similarity plan followed by deterministic score-ranked selection |
| Ensemble | Native candidate composition with explicit member lineage |
| COPRO, MIPROv2 | Governed instruction-proposal plan followed by deterministic candidate search |
| SIMBA | Governed trace-reflection plan followed by deterministic materialization |
| GEPA | Governed Pareto-reflection plan with modality-preserving artifacts |
| InferRules | Governed rule-proposal plan followed by deterministic materialization |
| BootstrapFinetune | Governed trainer plan yielding an opaque model-profile artifact |
| BetterTogether | Model-proposal and trainer steps joined by native composition |

The engine never silently substitutes an optimizer. Provider-free
means the Rust compiler emits exact governed work for the engine runtime; it does
not mean the optimizer is unavailable.

Avatar follows the comparator-driven tool-use semantics of
[AvaTaR](https://arxiv.org/abs/2406.11200):
the corpus must contain both successful and failed training traces and the program
must name at least one opaque tool reference. A `compare_tool_use` step sends only
the program, corpus, and tool references through the existing governed
`ModelTransport`. Its `tool_policy` output is scoped to the corpus and policy; no
prompt, action body, tool endpoint, credential, or provider configuration enters
the durable contract. The materialized candidate binds that artifact through the
distinct `tool_policy_ref` field while retaining its full modality and evidence
lineage; `instruction_ref` remains reserved for instruction artifacts.

The evidence contract exposes exactly **14 modalities**: `text`, `document`,
`image`, `audio`, `video`, `graph`, `table`, `time_series`, `vector`, `spatial`,
`tensor`, `code`, `trace`, and `binary`. The request baseline must score every
modality observed in its corpus, and candidate promotion applies the configured
coverage and regression policy to that same observed set.

## Operational submission contract

`OptimizationRequest` is encoded as named-field MessagePack and submitted through
the ordinary durable analytics-job plane. Its top-level fields are
`schema_version`, `request_ref`, `program`, `corpus`, `optimizer`, `budget`,
`promotion`, `baseline`, `optimizer_artifacts`, and `candidate_evaluations`.
Unknown fields, unsupported schema versions, unlocated evidence, invalid opaque
references, unbounded inputs, and caller-supplied policy scope fail closed. The
client and server both cap the nested request at 16 MiB; the server also applies
item and nesting limits before deserialization.

```python
import msgpack

# request is a schema-versioned, reference-only OptimizationRequest mapping.
request_bytes = msgpack.packb(request, use_bin_type=True)
submitted = await client.jobs.submit_program_optimization(
    "knowledge",
    request_bytes,
    deadline_unix_ms=deadline_unix_ms,
    quota_cpu_ms=cpu_budget_ms,
    output_bytes=output_budget_bytes,
)

status = await client.jobs.status(submitted["job_id"])
rows = (status.get("output") or {}).get("rows", [])
candidates = [row for row in rows if row["kind"] == "program_candidate"]
plan_steps = [
    row for row in rows if row["kind"] == "program_optimization_plan_step"
]
```

Submission durably records a `Submitted` job and automatically requires the
`program.optimization` worker capability. Poll `client.jobs.status(job_id)` until
the state is `Succeeded`, `Failed`, or `Cancelled`; use the existing job
`cancel`/`resume` operations rather than a second optimizer-specific control
surface. Successful output uses the normal typed-job schema. Candidate rows carry
deterministic candidate references and selected state. Plan-step rows carry the
dependency-ordered executor, step kind, opaque inputs/outputs, modalities, and
operation ceiling.

For a provider-dependent family, execute returned plan steps only through the
named governed engine runtime. Add the resulting reference-only
`optimizer_artifacts` to the same request and resubmit. After evaluating a
candidate, add an `EvaluationSummary` to `candidate_evaluations` and resubmit;
promotion occurs only when aggregate improvement, per-modality non-regression,
coverage, and minimum-evidence requirements all pass. Promotion still commits
through `ChangeEnvelope` and `MutationBatch`.

For an Agent Utilities or GraphOS deployment, run the
[live deployment doctor](https://github.com/Knuckles-Team/agent-utilities/blob/main/docs/guides/self-setup.md#8-verify)
with `agent-utilities-doctor --live`; it verifies that the connected engine
advertises and executes the native optimization capability.

## Plan and candidate lifecycle

Plan steps are durable typed result rows. Each row has an opaque step and parent
plan reference, executor (`native_kernel`, `graph_similarity`, `model_transport`,
`evaluator`, or `trainer`), fixed step kind, exact opaque inputs/outputs/dependencies,
modalities, and a hard operation budget. A runtime materializes only governed
`OptimizerArtifact` values (`instruction_proposal`, `rule_set`, `reflection`,
`tool_policy`, `neighbor_score`, `ensemble_member`, or `finetuned_model`).
Resubmitting the same
request with those artifacts deterministically produces candidates.

A candidate-specific `EvaluationSummary` then supplies per-modality scores and
opaque evidence. A candidate promotes only if it clears aggregate improvement,
per-modality regression, evidence-count, and coverage policy. Internal ensemble
members cannot promote independently. A promoted revision can commit only through
the existing `ChangeEnvelope` and `MutationBatch` authority.

## Operational behavior

Program jobs reuse durable idempotent submission, quota/placement policy, renewable
leases, fencing, cancellation, deadline and CPU-budget checks, typed result staging,
and MutationBatch publication. The server injects `program.optimization` so an
unqualified worker cannot claim the job. Request budgets separately cap candidates,
demonstrations, model calls, evaluator calls, and trainer steps.

`eg-program` has no network client, provider SDK, disk cache, dynamic code execution,
or direct graph-write path. `ModelTransport` is a provider-neutral engine injection
point and is the only model-call boundary.
