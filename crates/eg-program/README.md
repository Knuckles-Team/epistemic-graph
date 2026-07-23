# eg-program

`eg-program` is Epistemic Graph's clean-room Rust substrate for typed LM programs
and governed self-improvement. It implements signatures, modules, demonstrations,
evaluation, optimization, and promotion without embedding DSPy, DSRs, LiteLLM, a
provider SDK, or a second persistence path.

Durable values are limited to opaque references, numeric evidence coordinates,
typed identifiers, scores, budgets, and policy. Raw prompts, responses, source
bytes, URLs, credentials, user/host names, and local paths are not representable.

## Execution model

1. Submit a versioned `OptimizationRequest` through `ProgramOptimize`.
2. The server authority-rebinds the program, evidence, and optimizer artifacts.
3. Pure Rust kernels select demonstrations, perform seeded search, rank KNN
   observations, compose ensembles, and materialize deterministic candidates.
4. Avatar, COPRO/MIPRO/SIMBA/GEPA/rule inference, evaluation, and finetuning emit
   exact governed plan steps for the existing model, evaluator, similarity, and
   trainer runtimes. Avatar's comparator contrasts positive and negative tool
   traces and emits a corpus-scoped `tool_policy`; every runtime returns only
   opaque, evidence-backed optimizer artifacts.
5. Promotion requires candidate-specific evidence, aggregate improvement,
   per-modality non-regression, and observed-modality coverage by default.
6. Commit remains exclusive to `ChangeEnvelope` and `MutationBatch`.

Plans are the provider-free execution mechanism, not an unsupported fallback. All
thirteen optimizer families have native program-layer behavior and preserve text,
document, image, audio, video, graph, table, time-series, vector, spatial, tensor,
code, trace, and binary evidence.

## Complexity and limits

- Candidate, plan, and result identity is deterministic SHA-256.
- Candidate lookups and policy sets use ordered maps/sets.
- Random search uses a bounded heap, `O(n log k)` per candidate, `k <= 256`.
- KNN consumes bounded, evidence-backed similarity observations without importing a
  second vector or embedding client.
- Requests, examples, artifacts, demonstrations, candidates, plan steps, model
  calls, evaluator calls, trainer steps, and references have hard caps.
- Long-running compilation observes cancellation, deadline, CPU budget, and lease
  fencing.

This crate has no network client, disk cache, dynamic code execution, unsafe code,
or provider-specific dependency.
