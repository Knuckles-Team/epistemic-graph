# EG-DECISION-ENGINE — Decision engine

## Outcome

Epistemic Graph (EG) evaluates typed decisions from caller-visible facts. It records how each conclusion was reached and can independently verify a committed result. Connector pack annotations enter that decision pipeline as **claims**, not proof of a capability. Consumers can assemble an agent graph, ask a statistical question, and inspect a certificate and evidence. They do not rely on a private documentation service.

This specification owns EG's decision contracts, solver, evidence record and jobs. It defines the decision-facing semantics of published pack facts. Agent execution, connector discovery and ingestion orchestration are consumers. Their adapters may call these methods but cannot become a second authority for decision records or policy.

## State legend

Each requirement has two independent states. **Delivery:** `WAITING` means no reviewed source evidence; `IN_PROGRESS` means an implementation branch exists; `SOURCE_LANDED` means the exact implementation is reachable from published `main`; `RETIRED` means a recorded decision deliberately removed it. **Acceptance:** `NOT_RUN`, `FAILED`, `PARTIAL`, or `ACCEPTED` records test and release evidence for that exact published commit. A green unit test on a branch can support `IN_PROGRESS` but cannot imply `SOURCE_LANDED`; source landing cannot imply `ACCEPTED`. The coverage matrix below names scope, not a blanket acceptance claim. An evidence record must contain the commit, workflow run, test command, observed result and date. Rows with no current evidence remain `WAITING / NOT_RUN` until checked.

## Required behavior

1. **Decision ladder.** Filter by tenant and row visibility before feature extraction or counting. Apply policy, pins, denials and hard compatibility constraints. Derive native ontology entailments. Solve the legal set by bounded exact integer optimization. Use fitted statistics only to rank legal options or abstain. Every answer exposes `resolution_kind` (`constraint`, `entailment`, `optimization`, `statistical`, `abstention`) and `evidence_class` (`proof`, `observation`, `claim`). The conclusion takes the weakest premise class. The derivation itself can be proof even when a pack-provided capability is a claim. Statistics can never authorize a denied option. (EG-DECISION-ENGINE-R001–003, EG-DECISION-ENGINE-R009, EG-DECISION-ENGINE-R013, EG-DECISION-ENGINE-R015–020, EG-DECISION-ENGINE-R048)
2. **Assembly and solver.** `AgentAssemble` accepts typed tasks/capabilities and constraints. EG derives capability closure and returns one legal `AgentGraphDraft` or a typed `AbstainReason`. The 0–1 model covers capability coverage, implications, exactly-one slots, compatibility, knapsack budgets, optional template shape and cardinality. Optimize lexicographically with checked integer arithmetic, deterministic branching and a work-node budget. There is no arbitrary answer-count cap. A certificate names feasibility, objective, lower bound, gap, solver version and input digest; a separate verifier recomputes them. Unknown cost, latency or capability facts cannot silently become zero or true. Bounded why-not re-solves explain excluded visible options. (EG-DECISION-ENGINE-R004, EG-DECISION-ENGINE-R008, EG-DECISION-ENGINE-R010–014, EG-DECISION-ENGINE-R018–019, EG-DECISION-ENGINE-R036, EG-DECISION-ENGINE-R047–054, EG-DECISION-ENGINE-R065, EG-DECISION-ENGINE-R070)
3. **Decision record and commit.** Records pin the request, visible candidate facts and provenance, ontology/policy/catalog/head digests, quantized feature matrix when used, solver identity, eliminated constraints, derivations, result and digest. `DecisionCommit` re-derives the result, verifies the certificate, checks every pinned component revision and compares the candidate-universe catalog digest within one Agent Library transaction. It stores a `DecisionRecord` component under `decision:<digest>`; a byte-identical retry is idempotent, while stale catalogs or fabricated inputs fail closed. Agent graph synthesis evidence references the committed record. Record versions remain decodable; unknown versions fail closed. Library-sourced records persist; graph-sourced statistical summaries use RLS-routed graph writes and policy-set retention. Maximum record size is 64 KiB, 64 candidates and 32 required capabilities, with typed size abstention. (EG-DECISION-ENGINE-R007, EG-DECISION-ENGINE-R012, EG-DECISION-ENGINE-R017, EG-DECISION-ENGINE-R019, EG-DECISION-ENGINE-R047, EG-DECISION-ENGINE-R051, EG-DECISION-ENGINE-R058, EG-DECISION-ENGINE-R065–071, EG-DECISION-ENGINE-R095)
4. **Statistics and evaluation.** `Decide` shortlists only caller-visible candidates, computes versioned features, separates full-label from bandit-label data, and records exact propensities. Calibration, conformal coverage, drift and selective-risk gates can abstain. Off-policy estimates report effective sample size and refuse promotion below policy thresholds. Training uses only independently evaluated, sufficiently traced outcomes from approved commit principals; self-reported outcomes are claims. Exploration requires an explicit budget and server HMAC seed, and is unavailable for policy, security, write-back and irreversible actions. `DecisionFit` emits a draft blob; `DecisionEval` emits a digest-bound receipt; only the guarded component publish path promotes a fitted head. Fixed-point/soft-float replay must produce identical bytes on supported targets. (EG-DECISION-ENGINE-R005–007, EG-DECISION-ENGINE-R016, EG-DECISION-ENGINE-R020–028, EG-DECISION-ENGINE-R040, EG-DECISION-ENGINE-R053–062, EG-DECISION-ENGINE-R066–074, EG-DECISION-ENGINE-R082 et seq. (see `requirements.md`), EG-DECISION-ENGINE-R100, EG-DECISION-ENGINE-R103, EG-DECISION-ENGINE-R118, EG-DECISION-ENGINE-R119)
5. **Question families.** Reuse the same ladder for retrieval-plan choice, ingestion lane, enrichment scheduling, entity-resolution proposals, schema mapping, contradiction proposals, routing, risk, budget-aware option selection, connector event and tool proposals, A2A routing, context-bounded tool subsets, topology and finance recommendations. An advisory question returns a proposal and evidence only; execution and irreversible write-back still require their own deterministic authorization. `DecideText` has its own quote-aware, typed-parameter parser. (EG-DECISION-ENGINE-R029–045, EG-DECISION-ENGINE-R046, EG-DECISION-ENGINE-R062, EG-DECISION-ENGINE-R071, EG-DECISION-ENGINE-R100, EG-DECISION-ENGINE-R102, EG-DECISION-ENGINE-R103, EG-DECISION-ENGINE-R104 et seq. (see `requirements.md`), EG-FINANCE-PRIMITIVES-R009)
6. **Pack fact admission.** Decision candidates come only from published, tenant-visible component revisions. Pack-provided capability, modality, schema digest, price, latency, model and `contract_pin` facts carry claim provenance. Unknown foreign capability IRIs remain exact claim terms; malformed native capability IRIs are refused upstream. Withdrawn components are ineligible. A stale ontology/shapes projection cannot supply current facts. Packs cannot publish `DecisionRecord`, `DecisionHead` or `DecisionPolicy` kinds. Re-import of identical content cannot change the decision candidate digest. (EG-DECISION-ENGINE-R050, EG-DECISION-ENGINE-R061, EG-DECISION-ENGINE-R075, EG-TYPED-PACKS-R032, EG-TYPED-PACKS-R042 et seq. (see `typed-packs-and-catalog/requirements.md`), EG-TYPED-PACKS-R066)

## Acceptance boundary

### Learned source reputation and replay (EG-DECISION-ENGINE-R118, EG-DECISION-ENGINE-R119)

**EG-DECISION-ENGINE-R118.** EG derives a read-only, tenant-visible `reputation` relation from independently evaluated outcomes linked to approved decision commits. Use hierarchical Beta–Binomial option pools and proper-score forecaster records (Brier, log score, Murphy decomposition and calibration error), with a declared time-discount rule and minimum-sample gate. A sparse or unverified pool abstains; it cannot receive a fabricated neutral reliability. `AuthorityPolicy.source_reliability`, retrieval candidate reliability and UQL `SOURCE RELIABILITY` read the same versioned materialization. Visibility is applied before pooling, and an outcome supplied only by the candidate or its publisher is a claim, not an independent label. Evidence fusion belongs to `eg-epistemic`; the finance signal method remains a typed adapter to that one kernel. A request-carried reliability weight cannot stand in for the governed materialization.

**EG-DECISION-ENGINE-R119.** `DecisionEval` replay takes `WalkForward(train,test,step,purge,embargo)`, `SharedCap(cap,Proportional)` and a policy-independent environment. Every fold reads a bitemporal `AS OF` snapshot, refits on training history only, and records input/head/policy digests, option contributions and a resumable checkpoint. The same sealed, content-addressed `EvaluationRun` records the exact trial count used by deflated Sharpe, split-derived PBO and Diebold–Mariano against a pinned incumbent. A superseding run references the prior digest without rewriting history. Finance `BacktestRun` is a specialization of the shared seal, verification and no-look-ahead rules. A policy-dependent environment must refuse replay and use the separately identified off-policy evaluation path. `DecideText` parses `REPLAY WALK FORWARD` through its typed lexer; parser success alone cannot authorize a job or publish a fitted head.

`ACCEPTED` requires published-main contract generation, deterministic solver and verifier proof, served client requests, versioned replay vectors, negative authorization/visibility and pack-claim probes, and hosted CI at the exact commit. Each affected requirement ID gets its own evidence row; completing other requirements in the same implementation sequence does not automatically close unrelated IDs. CCCC, jscpd (zero new clone pairs), Dupehound and KISS checks apply to changed code alongside formatting, Clippy, package tests and generated contract freshness. Hosted checks must provision disposable fixtures; credentials or an existing live deployment cannot be required to run the baseline PR gate. Optional deployment qualification is recorded separately.

### Task-plan facts (EG-DECISION-ENGINE-R126, EG-DECISION-ENGINE-R127)

An agent-utilities task planner answers "How can I execute task X?" as one plan. EG supplies the facts behind that plan; the planner only composes them. `AgentAssemble` (EG-DECISION-ENGINE-R036) remains the authority for agent composition. The coverage query (EG-DECISION-ENGINE-R126) names Agent Library templates and A2A agents that already serve each capability. The guardrail query (EG-DECISION-ENGINE-R127) names the rules each task class must follow. Both answers carry premises, so the planner cites ontology facts for every plan element. Neither answer authorizes execution. The consumer contract is AU-CONTROL-R027 in [agent-utilities](https://github.com/Knuckles-Team/agent-utilities/tree/main/specs/agent-control-plane).

## Coverage map

The stable IDs identify obligations even if implementation moves. T1–T9 are sequence labels; a dash means a cross-cutting invariant. This spec contains the full normative behavior and test design; no other repository is required to understand or implement it.

| Area | IDs and sequence | Implementation section |
|---|---|---|
| Core decision/solver | EG-DECISION-ENGINE-R001–046, EG-DECISION-ENGINE-R046–074 (T1, T2, T3, T4; EG-DECISION-ENGINE-R015/017–019 cross-cutting), EG-DECISION-ENGINE-R076 (T4), EG-DECISION-ENGINE-R082 et seq. (T4; see `requirements.md`), EG-DECISION-ENGINE-R095 (T3) | Behavior 1–5; architecture §§1–4; tests A–D |
| Pack facts consumed by Decide | EG-DECISION-ENGINE-R050, EG-DECISION-ENGINE-R061, EG-DECISION-ENGINE-R075, EG-TYPED-PACKS-R032, EG-TYPED-PACKS-R042 et seq. (see `typed-packs-and-catalog/requirements.md`), EG-TYPED-PACKS-R066 | Behavior 6; architecture §5; tests E |
| Decision consumers and future questions | EG-DECISION-ENGINE-R100 (T6), EG-DECISION-ENGINE-R104 (T4), EG-DECISION-ENGINE-R105 et seq. (T6; see `requirements.md`), EG-DECISION-ENGINE-R118 (T6) | Behavior 5; architecture §6; tests D |
| Task-plan facts | EG-DECISION-ENGINE-R126, EG-DECISION-ENGINE-R127 (T6) | Behavior 2 and 5; acceptance "Task-plan facts"; tests D2 |

The exact decision-owned ID inventory is in `coverage.md`; it includes each title, sequence label and historical delivery claim for review. Pack storage and import failure rules have their own owner spec. The range EG-DECISION-ENGINE-R001–074 excludes an end-to-end decision journey tested as an integration fixture by the web UI's own spec. Historical decisions may be `RETIRED` only with a recorded superseding choice.

Adjacent numerical attribution and motif search (EG-FEDERATED-QUERY-R031/EG-FEDERATED-QUERY-R034) are owned by `federated-query-and-reasoning`. KISS parser compatibility (EG-CONTRACT-R012) is owned by `public-engine-contract-and-release`. Cross-app entity resolution (EG-UNIFIED-DATA-PLANE-R030) is owned by `unified-data-plane`. Decision methods consume their typed results without claiming a second implementation.

Requirement IDs are defined in [requirements.md](requirements.md); delivery state per ID is in `status.json`.

## Certified context packages (R137.7–R137.12)

The engine owns certified multi-resolution context-package selection under the tightest applicable window, reserved-output, caller-token, monetary and latency constraints. It preserves exact tokenizer weights, net-value optimization, fewer-token ties and cited source/range/revision identity. AU-CONTEXT-R004 consumes the typed selection without reranking. Full-prompt fit and the existing recursive decomposition and parallel-map obligations remain separate and unchanged. These additive definitions do not claim a served package handler or runtime verification.

<!-- spec-definition-patch:comparative-analysis-r139 -->
## Comparative analysis plan (EG-DECISION-ENGINE-R139)

EG produces a read-only, revision-pinned `ComparativeAnalysisPlan` through public `Method::Decide`. It reuses caller-supplied query-vector and completed `UnifiedQuery` results for zero-model vector discovery. It selects normalized-importance `>= 0.50` candidates for one or two AU synthesis calls. It selects only `>= 0.80` candidates for budgeted deep extraction. The versioned thresholds, candidate order/universe, query and space revisions, and each stage and total call ceiling are bound into the plan. The existing 64-candidate decision limit applies.

EG returns a typed refusal before exposing a plan for stale or malformed inputs or impossible budgets. EG never generates text or invokes a model. AU consumes the plan and calls its configured service only afterward at `agent_utilities/knowledge_graph/retrieval/context_compiler_serving.py::compiled_chat_completion`; AU owns a separate invocation receipt bound to the plan digest. `AU-HARNESS-R024.3` trace digests are not a comparative-analysis consumer contract. This EG-only slice does not add an AU requirement or runtime receipt; the served AU consumer remains a dependency hold. R080.1 and R136.3 are adjacent mechanisms only. These definitions remain SPECIFIED until both EG and AU served receipts exist.
<!-- /spec-definition-patch -->

<!-- spec-definition-patch:owner-batch-20261012-decision-engine -->
## Reviewed owner residuals — 2026-10-12

### `EG-DECISION-ENGINE-R135.7` — AU:18

Before planning, the consumer must bind one `PrePlanTestReceiptV1` to the run, repository revision, configured command and digest. The closed receipt includes start/completion time, exit code, artifact-manifest digest/count and result digest. The same receipt is present in both `TaskPlan` and verifier output; the verifier records `tests_ran`, `artifacts_exist`, evidence refs and policy revision. Only a zero-exit run with both checks true may persist as a reusable pattern in the existing `CompiledProgram`/`AgentLibraryStore` path, indexed through `ProgramLookup`. Missing, late, stale, mismatched or unreadable evidence blocks promotion. AU's exact pre-plan producer/verifier contract remains pending; `AU-HARNESS-R022.2` is preserved as adjacent coverage only.
### `EG-DECISION-ENGINE-R106.1` — AU:19

`CouncilCoordinationV1` supplies closed `CouncilPositionV1` records carrying council/topology revision, position/persona revision, option, claim digest and revisioned evidence/constraint refs, plus a pinned `CouncilPolicyV1` with quorum and deterministic conflict rule. The sealed decision retains all input position ids, accepted/rejected ids, selected-or-abstained outcome, closed abstention reason, policy revision and input/decision digests. The typed branch attaches to `Method::Decide`/`handle_decide`; existing `AgentGraphDraft.topology` and `topology_shape.rs` validate council shape, while `swarm:loopsBackTo` remains topology vocabulary only. AU's persona-position producer contract is missing; `AU-CONTROL-R010` does not cover it.
### `EG-DECISION-ENGINE-R137.13` — AU:25

`QuestionKind::ContextBudget` binds a run to tool-output byte count/digest, tool-call ref, graph snapshot ref, node count, observation time and signal digest, plus a versioned threshold policy. The existing `Method::Decide` ladder returns direct/recursive mode, selected rung, reason codes and all input/policy digests; it is the sole mode selector. Wire beside `tool_subset_outcome` in `stat_decide.rs::handle_decide`; AU `run_full_rlm` consumes the result before execution. This mode decision is independent of R137.7-.12 package selection. AU's exact observation-producer contract is pending; `AU-HARNESS-R026.3/.4` remains adjacent coverage only.
### `EG-DECISION-ENGINE-R134.9` — AU:34

Original objective: The agent selects several relevant skills from a skill tree for one task, and selection is tuned from run outcomes, so exploration is not locked to a single skill.

EG-owned residual: An explicit skill tree and outcome-tuned EG exploration over that tree are absent. AU-HARNESS-R022.3 says retrieval ranks locally, while AU-CONTEXT-R001 assigns ranking/feedback to EG; resolve this authority conflict.

Producer/consumer boundary: EG plan_over_skills -> AgentLibraryStore -> served TaskPlan; AU experience feedback lacks a named EG selection-policy update consumer.

Proposed contract: Define TaskPlanSkillNodeV1 with task_plan_id/revision/digest, skill_id, nullable parent_skill_id, context_match_policy_revision and evidence refs, prior_outcome_refs with source revisions/digests, score, rank, and ranking_policy_revision. EG emits multiple relevant skills in a deterministic tree under a versioned score/rank policy that binds the comparator and tie rule; missing or unpinned policy refuses. Outcome feedback changes later breadth only through R140.102 and its pinned policy/outcome set.

Planned acceptance: Case R134.9-ranked-plan: served Decide(TaskPlan) returns multiple skill nodes with stable parent links, cited context matches, pinned outcome refs, score/rank and plan digest; equal scores use the declared tie-break. Refuse missing/stale context or outcome evidence and malformed/cyclic parent links. AU application of this rank is not asserted.

Exact attachment and hold: Attach the proposed result at plan_over_skills -> AgentLibraryStore -> served TaskPlan. AU-HARNESS-R022.3 local ranking and AU-CONTEXT-R001 feedback authority remain existing adjacent obligations; the AU consumer that applies EG rank and the feedback-to-R140.102 receipt contract are not yet specified. Keep SPECIFIED pending those owner definitions and served evidence.

This is a SPECIFIED definition only; implementation and served evidence remain pending.

Preserve existing coverage: `EG-DECISION-ENGINE-R134.4`, `EG-DECISION-ENGINE-R134.5`, `AU-HARNESS-R022.3`, `AU-CONTEXT-R001`.

Owner source hash: `ced4d0bb75fa260e4291aea5f71c28b407a95fc3e06047666e1278b043a3cdbd`.

### `EG-DECISION-ENGINE-R137.14` — AU:37

Original objective: A run's long context may be compressed into an updatable memory state only when answer quality on held-out questions stays within a declared tolerance; otherwise the full context is retained.

EG-owned residual: The judge checks retained values/methods/validation steps, not answer quality on held-out questions compared with full-context answers under a declared tolerance.

Producer/consumer boundary: AU ContextManager.before_model_request -> run_agent replaces context only after its summary gate; no held-out paired-answer verdict is a named prerequisite.

Proposed contract: Define PairedContextEvaluationV1 with held_out_query_set id/revision/digest, raw_context revision/digest, candidate_context revision/digest, evaluator/model revision, per-query raw and candidate answer digests, declared quality metric, aggregation rule, tolerance and policy revision, plus a complete per-query verdict set. Replace raw context only when all queries are present and the aggregate satisfies the declared tolerance; any failed or incomplete evaluation retains the raw revision.

Planned acceptance: Case R137.14-paired-context: a complete held-out set within the declared tolerance permits the replacement verdict; a beyond-tolerance result, missing query, stale context/evaluator digest, undefined metric/aggregation/tolerance, or incomplete verdict set refuses and leaves the active raw-context revision unchanged.

Exact attachment and hold: AU ContextManager.before_model_request -> run_agent is the existing replacement point. The AU held-out evaluator and receipt schema plus the consumer that atomically applies EG acceptance are absent; R013.5/.10/.12/.13 remain adjacent coverage, not evidence of this gate.

This is a SPECIFIED definition only; implementation and served evidence remain pending.

Preserve existing coverage: `AU-HARNESS-R013.5`, `AU-HARNESS-R013.10`, `AU-HARNESS-R013.12`, `AU-HARNESS-R013.13`.

Owner source hash: `7e1405981aa8b697a73eb7e8c3c3170be8edefc674c23beef740884450fc1add`.

### `EG-DECISION-ENGINE-R134.10` — AU:39

Original objective: A main run delegates subtasks to sub-runs that return only summaries within a declared contract, conserving the main context budget, and records delegation decisions as trajectories.

EG-owned residual: There is no summary-only sub-run return schema, parent-remaining-context bound or explicit rejection of raw/oversized returns. Generic spans do not bind delegation decision, child run and accepted summary as one trajectory.

Producer/consumer boundary: AU _fork_fanout -> FanoutResult and parallel workspace synthesis; EG decides plans. The bounded child-summary producer/parent consumer boundary is absent.

Proposed contract: Define ChildSummaryV1 with plan/decision id and digest, parent run id, child run id, expected child id set, summary artifact kind/schema/version/digest, exact tokenizer revision, summary token count, and source/result evidence refs. A parent budget record binds remaining context tokens at dispatch and the maximum aggregate child-summary tokens; child output exposed to the parent contains only the declared summary artifact, never raw transcript. The accepted fan-out receipt accounts for every child as one summary or named failure gap.

Planned acceptance: Case R134.10-bounded-child-summary: valid summary-only children whose checked token total fits the parent remaining budget are linked to decision, child and summary digests. Refuse raw/oversized output, missing/duplicate/unknown children, tokenizer or budget revision mismatch, and any total above the bound before parent synthesis.

Exact attachment and hold: AU _fork_fanout -> FanoutResult and parallel workspace synthesis are the observed attachment points; EG owns TaskPlan authority. AU summary producer, parent remaining-context budget field and consumer validation/readback receipt are missing; AU-CONTROL-R043.2/.5 and AU-HARNESS-R020.4 and AU-HARNESS-R024.1 remain adjacent.

This is a SPECIFIED definition only; implementation and served evidence remain pending.

Preserve existing coverage: `AU-CONTROL-R043.2`, `AU-CONTROL-R043.5`, `AU-HARNESS-R020.4`, `AU-HARNESS-R024.1`.

Owner source hash: `742906721351687d032991b4e9c0255baf7a6c480e21ac7a616975d40e38ce84`.

### `EG-DECISION-ENGINE-R137.15` — AU:40

Original objective: A recursive runtime lets a model call sub-models with typed signatures and tools, recording interpretable trajectories in the run trace.

EG-owned residual: A primitive/function handle and span digests do not define each recursive sub-model's typed input/output signature, tool binding and boundary validation/refusal. The recorded signature/model/tool linkage remains missing.

Producer/consumer boundary: AU run_full_rlm -> RLMEnvironment.run_parallel_sub_calls executes EG limits; span instrumentation records calls. No recursive-call signature producer/validator is named.

Proposed contract: Define RecursiveCallV1 request/result records with parent run/span, call id, signature id/revision/digest, typed input schema and digest, admitted tool id/revision set, selected model id/revision, typed output schema and digest, and terminal status. Validate input and tool membership before dispatch and output against the same signature before recording success; bind all revisions/digests in the recursive trace.

Planned acceptance: Case R137.15-signature-boundary: an admitted typed call records matching signature/model/tool/input/output digests. Refuse unknown or changed signatures, unadmitted tools, malformed input/output, missing parent span, or revision mismatch before accepting the call result.

Exact attachment and hold: AU run_full_rlm -> RLMEnvironment.run_parallel_sub_calls is the observed execution point; EG resource limits and span instrumentation are adjacent only. No AU recursive signature producer/validator receipt or exact EG-to-AU call contract is named.

This is a SPECIFIED definition only; implementation and served evidence remain pending.

Preserve existing coverage: `AU-HARNESS-R026.1`, `AU-HARNESS-R026.4`, `AU-HARNESS-R024.1`, `AU-HARNESS-R024.3`, `EG-ORCH-VOCAB-R005`.

Owner source hash: `4edb15328650ebf084f0ab1d073b092e5b684aafad9b7b1ca7f412859390b923`.

### `EG-DECISION-ENGINE-R134.11` — AU:45

Original objective: Long tasks decompose into sub-horizons under a declared per-step budget with checkpointed credit; a plan whose step count exceeds the configured horizon is split before execution, not attempted whole.

EG-owned residual: Depth/item bounds do not define a configured execution horizon, per-step budget, pre-execution splitting of excess steps or checkpointed credit across sub-horizons.

Producer/consumer boundary: EG decompose_task/TaskPlan -> AU bounded map primitives; the horizon partition and durable credit receipt are not a named producer/consumer interface.

Proposed contract: Define HorizonPartitionV1 over one committed TaskPlan revision with ordered step ids, dependency edges, positive max_steps_per_horizon, per-step budget policy id/revision/unit and credit ceiling. Partition only at dependency-safe boundaries before launch. Each completed horizon returns a durable credit checkpoint with plan/horizon ids, consumed/reserved/remaining amounts, budget revision and digest; require a versioned accounting rule for debits.

Planned acceptance: Case R134.11-horizon-credit: a plan within the limit remains one horizon; a longer valid plan is split at dependency-safe boundaries and its checkpoint balances reconcile exactly. Refuse an unsplittable dependency component, absent/invalid budget rule, over-credit horizon, missing checkpoint, or stale plan revision before the next launch.

Exact attachment and hold: EG decompose_task/TaskPlan feeds AU bounded map primitives, but no named partition/credit receipt interface exists. AU durable checkpoint/debit semantics and the consumer that blocks launch on a missing receipt remain unspecified; do not substitute Finance forecast horizons or solver budgets.

This is a SPECIFIED definition only; implementation and served evidence remain pending.

Preserve existing coverage: `AU-HARNESS-R026.1`, `AU-HARNESS-R026.2`, `EG-DECISION-ENGINE-R134.3`, `EG-DECISION-ENGINE-R134.4`.

Owner source hash: `c1bfd61241a0fd02ada2816f173a3b9568da5b48e490e76058fd740b356886da`.

### `EG-DECISION-ENGINE-R059.1` — AU:57

Original objective: Skill candidates from several models are merged into one skill tree whose scores aggregate independent evaluations, giving a robust estimate of effectiveness and transferability across model backbones.

EG-owned residual: No merged skill tree binds candidates from several model backbones to independent per-skill evaluations or produces the requested robustness/transferability estimate across those backbones.

Producer/consumer boundary: AU experience/skill candidates -> EG admission-linked outcome aggregate; the shared tree and backbone-bound per-skill evaluation join remain unnamed.

Proposed contract: Define BackboneSkillEvaluationV1 rows keyed by candidate id, canonical skill identity, source model/backbone id/revision, task-distribution id/revision, evaluation dataset id/revision/digest, metric id/revision, sample count and result digest. An aggregate may be emitted only with complete independent rows for every declared backbone and a versioned transferability policy; publish the full per-backbone evidence with the aggregate, not a bare score.

Planned acceptance: Case R059.1-backbone-evidence: complete independent evaluations for the same canonical skill identity produce a result that lists every backbone and policy revision. Refuse missing/duplicate backbone rows, candidate identity mismatch, reused non-independent evidence, incomplete dataset coverage or absent metric/transferability aggregation policy.

Exact attachment and hold: AU supplies experience/skill candidates; EG R059 is the existing independent-outcome admission path. No shared canonical skill merge key, robust aggregation/threshold decision, or AU multi-backbone evaluation producer is specified; these decisions are explicit holds.

This is a SPECIFIED definition only; implementation and served evidence remain pending.

Preserve existing coverage: `AU-HARNESS-R022.1`, `AU-HARNESS-R022.2`, `EG-DECISION-ENGINE-R059`.

Owner source hash: `0c7c2eed61e3cfe6fe6c863c75fb66092f3a8bea97e3942b635c7b69f715d3a9`.

### `EG-DECISION-ENGINE-R134.12` — AU:85

Original objective: Hard tasks run in two stages: parallel reasoning over several trajectories, then a summarization step aggregating them; width and depth are chosen by the decision ladder and re-measured against a single-trajectory baseline.

EG-owned residual: Hard-task alternative trajectories and deliberative summary aggregation; explicit width/depth decision ladder and remeasurement against a single-trajectory baseline.

Producer/consumer boundary: R134 plan explicitly connects Decide TaskPlan to AU-CONTROL-R042/R043; R137 plan connects ContextBudget to AU-CONTEXT-R004/AU-HARNESS-R026. AU-HARNESS-R018 has run_agent paired arms. These dependencies lack the alternative-trajectory/summarization/baseline binding.

Proposed contract: Define TrajectoryDecisionV1 with one task/plan digest, policy revision, chosen width/depth, ordered trajectory ids, identical input snapshot digest per arm, single-trajectory baseline id, per-arm outcome/evaluation refs, and paired comparison receipt. The decision names the selection and evaluation policies; it does not prescribe an unsupported scoring formula. A pass requires complete paired arms over the same task/input and an explicit R018 comparison verdict.

Planned acceptance: Case R134.12-paired-trajectories: selected width/depth and all trajectory/baseline ids bind to the same plan and input snapshot; each arm has an outcome and the paired R018 verdict is readable. Refuse missing/mismatched arm, baseline, policy revision or result, and refuse any claim of improvement without the declared paired verdict.

Exact attachment and hold: Existing points are Decide(TaskPlan), AU-CONTROL-R042/R043, AU-HARNESS-R020.4, and paired arms in AU-HARNESS-R018.1/.2/.5. AU trajectory generation, summary aggregation and EG selection-policy consumer receipt are absent.

This is a SPECIFIED definition only; implementation and served evidence remain pending.

Preserve existing coverage: `AU-HARNESS-R020.4`, `EG-DECISION-ENGINE-R134.1`, `EG-DECISION-ENGINE-R134.3`, `AU-CONTROL-R007`, `EG-DECISION-ENGINE-R137.1`, `EG-DECISION-ENGINE-R137.3`, `AU-HARNESS-R018.1`, `AU-HARNESS-R018.2`, `AU-HARNESS-R018.5`.

Owner source hash: `a28929a50cbfcb691664403ce456fbf9dc38d6bd64b34424932e4fa22f9cbda1`.

### `EG-DECISION-ENGINE-R134.13` — AU:91

Original objective: Parallel analyst roles each produce structured, cited findings that flow to one decision role, which abstains when findings conflict or lack citations.

EG-owned residual: Distinct analyst roles provide cited findings to one decision role; required abstention on unresolved conflict or missing citations.

Producer/consumer boundary: EG R134.5 plans TaskPlan through served Decide; AU R020.5 constructs worker slots through _dispatch_parallel_batch. The selected aggregation consumer lacks a cited-finding/conflict abstention contract.

Proposed contract: Define AnalystFindingV1 with task/plan digest, expected analyst id and role, finding id, claim text/digest, cited evidence ids/revisions/digests and source spans. Define SynthesisResultV1 with the complete expected analyst-id set, all finding ids, outcome per analyst, cited evidence set, conflicts, final decision-role id and either a supported conclusion or a typed abstention reason. Never drop a failed or absent analyst from accounting.

Planned acceptance: Case R134.13-cited-synthesis: cited compatible findings produce a result that accounts for every expected analyst. Refuse missing/duplicate analyst, unsupported finding, unresolved conflict, invalid citation, plan mismatch or absent decision role with explicit abstention and no recommendation.

Exact attachment and hold: EG R134.5 served Decide(TaskPlan) is the planning point; AU R020.5 _dispatch_parallel_batch constructs worker slots. The exact AU finding producer/collector and aggregation consumer contract is missing; existing fan-out clauses do not prove it.

This is a SPECIFIED definition only; implementation and served evidence remain pending.

Preserve existing coverage: `EG-DECISION-ENGINE-R134.3`, `AU-HARNESS-R020.1`, `AU-HARNESS-R020.4`, `AU-HARNESS-R020.2`, `EG-DECISION-ENGINE-R134.6`.

Owner source hash: `f5ddb0ac9b0329ac031c01c56c49ee03c922a991dbf2d0e29c111515424d3455`.

### `EG-DECISION-ENGINE-R029.1` — AU:93

Original objective: Search grounding is a replaceable provider selected through the committed routing decision, independent of the reasoning model; an unavailable provider is refused, not silently substituted.

EG-owned residual: Replaceable search-grounding provider independently selected from reasoning model and bound to a committed route; unavailable committed provider must refuse rather than substitute.

Producer/consumer boundary: Preserve GRAPHOS-OPS-R021.2.1 retrieval.search and its tenant-scoped retriever. EG must produce the committed provider binding and the SDK/GraphOS consumer must execute that exact provider; that edge is missing.

Proposed contract: Define RetrievalProviderBindingV1 in the committed retrieval plan with provider id/revision/config digest, query/request digest, tenant scope, and a separately identified reasoning model id/revision. The binding has an explicit no-substitution policy. Execution evidence records the exact provider, binding digest, request/result digests and provider revision.

Planned acceptance: Case R029.1-exact-provider: independently vary provider and reasoning-model ids while verifying the committed provider is executed. If the bound provider is unavailable, stale, unauthorized or mismatched, refuse before retrieval and record zero calls to alternate providers.

Exact attachment and hold: Preserve GRAPHOS-OPS-R021.2.1 retrieval.search and its tenant-scoped retriever. EG must commit the provider binding; the SDK/GraphOS consumer contract that executes and receipts that exact binding is not defined.

This is a SPECIFIED definition only; implementation and served evidence remain pending.

Preserve existing coverage: `EG-DECISION-ENGINE-R029`, `GRAPHOS-OPS-R021.2.1`.

Owner source hash: `c9552392eb813763a83fc96fb48ad1bd18fd805ef608736f1dae5d48ab12c984`.

### `EG-DECISION-ENGINE-R134.14` — AU:105

Original objective: A search-based planner explores candidate solution branches with an experience memory and fuses results across branches, recording each branch outcome for later retrieval.

EG-owned residual: Search planner explores candidate solution branches using experience memory, fuses branch solutions and records every branch outcome for later retrieval.

Producer/consumer boundary: Use the same missing branch-experience producer and committed planning consumer identified for AU:84. Existing attachment points are R134.5 Decide, R020.5 worker execution and R022.4 run outcome distillation.

Proposed contract: Define BranchOutcomeV1 with branch id, parent task/plan digest, branch input digest, experience snapshot revision, ordered fusion position, terminal outcome or named failure gap, and immutable result digest. The fusion receipt lists every planned branch in deterministic order, the versioned fusion-policy id/revision, all outcome refs and the fused result digest. A later branch retrieval records the exact outcome refs and revisions it consumed.

Planned acceptance: Case R134.14-branch-readback: a complete branch set produces an ordered fusion receipt and independent readback for every outcome; a later branch cites the exact pinned prior outcome. Refuse missing/duplicate branch, changed parent/input/snapshot, omitted failure gap, absent fusion policy, or stale readback.

Exact attachment and hold: Existing points are Decide R134.5, AU R020.5 worker execution and AU R022.4 outcome distillation. The AU branch-experience producer, committed-plan consumer and durable per-outcome receipt schema remain missing; do not substitute R140.102 breadth adaptation.

This is a SPECIFIED definition only; implementation and served evidence remain pending.

Preserve existing coverage: `EG-DECISION-ENGINE-R134.3`, `AU-HARNESS-R022.3`, `AU-HARNESS-R020.4`, `AU-HARNESS-R022.1`, `AU-HARNESS-R022.6`.

Owner source hash: `4fd31dd504f158f43bcf77d81a39c2419303a538b32553a77a39f6b5a575e9d2`.

### `EG-DECISION-ENGINE-R020.1` — AU:110

Original objective: Each agent keeps an exploitation pool of past trajectories and an exploration pool of generated candidates, and draws on the exploration pool only for contexts with no matching trajectory.

EG-owned residual: Separate per-agent exploitation trajectory pool and generated exploration pool; explore only when context has no matching past trajectory.

Producer/consumer boundary: EG owns candidate selection and orchestration evidence; AU R022 supplies run experience. No per-agent two-pool producer or context-miss-only selection contract exists.

Proposed contract: Define CandidatePoolV1 with tenant/agent id, task/context feature digest, policy revision, candidate ids and per-candidate source revisions. The selection receipt partitions candidates into exploitation (matched prior trajectories) and exploration (eligible only when no match exists), records the context-match policy revision and evidence refs, and includes a deterministic tie-break. Cross-agent history is never eligible.

Planned acceptance: Case R020.1-agent-isolated-pools: a same-agent matching trajectory selects from the exploitation pool and excludes exploration; a declared no-match case may select from exploration. Refuse missing match policy/evidence and cross-tenant or cross-agent history; bind result to the candidate snapshot.

Exact attachment and hold: EG owns candidate selection and AU-HARNESS-R022.1/.3 provide adjacent experience inputs. No AU per-agent pool producer or context-match evidence contract exists; exact match dimensions/threshold must be supplied by that owner policy before implementation.

This is a SPECIFIED definition only; implementation and served evidence remain pending.

Preserve existing coverage: `AU-HARNESS-R022.1`, `EG-DECISION-ENGINE-R020`, `AU-HARNESS-R022.3`.

Owner source hash: `dafa8396b5a7c2a168253c7be219718c78186266610e5a91e7dc318d35fc552b`.

### `EG-DECISION-ENGINE-R133.7` — EG:13

Original objective: Each recursive-reasoning trajectory (code run, sub-calls, intermediates) is stored as typed graph nodes so verifier and improvement loops can critique it.

EG-owned residual: Define recursive code/subcall/intermediate node/edge identity and verifier/improvement read-and-critique contracts.

Producer/consumer boundary: Terminal CommitWorkItemResult adapts traces after fenced CAS; AddNode validates traces; graph queries read them. Recursive hierarchy production and critique consumption are absent.

Proposed contract: Define RecursiveTraceNodeV1 with run id/revision, step id/revision, parent node id, ordered child ids, input/output/evidence refs and digests, source revision, and trace snapshot id. Define TraceCritiqueQueryV1 with root node, authorized projection, snapshot revision, filters and opaque cursor; the response includes the complete selected ancestry and source refs. Parent/child links must be complete and acyclic within the selected snapshot.

Planned acceptance: Case R133.7-trace-critique: ingest and independently read back a nested trace, then critique it at the same authorized snapshot with complete ancestry. Refuse incomplete ancestry, cycle, stale cursor/snapshot, missing digest or unauthorized projection without returning protected fields.

Exact attachment and hold: Terminal CommitWorkItemResult adapts traces, AddNode validates records and graph queries read them. The recursive node producer and named authorized critique-query operation/consumer are absent; existing fencing/projection rules remain binding.

This is a SPECIFIED definition only; implementation and served evidence remain pending.

Preserve existing coverage: `EG-DECISION-ENGINE-R133.1`, `EG-ORCH-VOCAB-R013.1`, `EG-DECISION-ENGINE-R133.5`, `EG-ORCH-VOCAB-R013.5`, `EG-ORCH-VOCAB-R009.15`, `EG-ORCH-VOCAB-R013.6`.

Owner source hash: `9e0be988c182da3c7f440c14d2fc8e56f60426e543e3f53ba39f250f235e6c8d`.

### `EG-DECISION-ENGINE-R100.1` — EG:16

Original objective: Embeddings are adjusted from feedback signals (agent errors, human corrections) through a promoted adapter, closing a feedback loop into vector space; unpromoted adjustments never alter served results.

EG-owned residual: Define attributed agent-error/human-correction ingestion and lineage from examples to fitted adapters before existing promotion.

Producer/consumer boundary: R100 persists examples, R101 applies promoted post-RLS/pre-ANN query adapters, R040 gates publish. Signal production and example-to-fit linkage are absent.

Proposed contract: Define FeedbackEventV1 with stable event id, decision id/revision/digest, option/action id, actor id, outcome/evidence refs and digests, event time and source revision. A versioned mapping policy converts only a complete joined event into a fit example; deduplication is by event id plus source revision. Bind example-set digest to R100 fitting, resulting adapter id/revision to R101, and publish admission to R040.

Planned acceptance: Case R100.1-feedback-join: an authorized complete event is read back as exactly one fit example bound to the expected R100 dataset digest; replaying the same event is idempotent. Refuse duplicate identity with changed content, missing decision/option/outcome join, invalid mapping policy, stale source revision or evidence mismatch before fitting.

Exact attachment and hold: R100 persists examples, R101 applies a promoted adapter post-RLS/pre-ANN and R040 gates publish. The AU signal producer and outcome-to-label mapping policy are not specified; do not treat an independent outcome record as the producer contract.

This is a SPECIFIED definition only; implementation and served evidence remain pending.

Preserve existing coverage: `EG-DECISION-ENGINE-R100`, `EG-DECISION-ENGINE-R101`, `EG-DECISION-ENGINE-R040`.

Owner source hash: `eca485ffdff9cea5394130b53e12a0edc235e342ab01accc36ad9d06f496daa8`.

### `EG-DECISION-ENGINE-R088.2` — EG:17

Original objective: Evaluation replays real-world events in chronological order over a simulated period while the agent forecasts outcomes and adapts as information arrives; no future event is visible before its turn.

EG-owned residual: Define event cursor/period, forecasts, adaptation and no-future-event visibility at the simulator/harness boundary.

Producer/consumer boundary: DecisionEval owns AS OF and fold replay. A real-world event simulator feeding an AU adaptive harness is undefined; policy-dependent environments must respect R119 off-policy routing.

Proposed contract: Define ReplayEventV1 with event id, strictly ordered sequence number, event time, state snapshot id/revision/digest, observed action id, outcome/evidence refs and source revision. Replay uses an AS OF snapshot and opaque deterministic cursor; the sealed ReplayReceipt binds ordered event digests, cursor, policy id/revision and result digest. Identical duplicate delivery is idempotent; a conflicting duplicate or sequence gap refuses. When the environment is policy-dependent, route to R119 off-policy evaluation.

Planned acceptance: Case R088.2-ordered-replay: replay a fixed event sequence and resume from its receipt/cursor to the same event order and sealed digest. Refuse gaps, out-of-order events, conflicting duplicate ids, changed snapshots or policy-dependent on-policy claims without R119 routing.

Exact attachment and hold: DecisionEval provides AS OF and fold replay. The real-world event simulator and AU adaptive-harness producer/consumer are undefined; this row specifies EG replay envelope and refusal only, not an external event source.

This is a SPECIFIED definition only; implementation and served evidence remain pending.

Preserve existing coverage: `EG-DECISION-ENGINE-R088`, `EG-DECISION-ENGINE-R119`, `EG-DECISION-ENGINE-R088.1`.

Owner source hash: `d01db99df0c6c22b37f82cd815e129956fa151e7da2b670b73348ca5058e629e`.

### `EG-DECISION-ENGINE-R137.16` — EG:26

Original objective: Retrieval payload size adapts per request to task complexity, remaining token budget and model cost instead of a fixed window, and never exceeds the remaining budget.

EG-owned residual: The historical statement that only window/chunking budgets exist is stale: R137.7-.12 now cover exact-token package sizing and monetary/model-price bounds. The remaining gap is an explicit task-complexity input/selection policy and binding the caller ceiling to the current request remaining-token budget, including already-consumed/reserved tokens. R128 task-class pressure does not itself prove that joint payload-sizing linkage.

Producer/consumer boundary: Planned served Decide(ContextBudget) -> existing solver -> independent certificate verifier -> sealed ContextPackageSelection -> AU-CONTEXT-R004 context-compile consumer. R137.11/.12 explicitly name the consumer; current dispatch absence is stated in R137.9.

Proposed contract: Extend ContextBudgetSignalsV1 with task-complexity feature set/digest and complexity-policy id/revision. Derive remaining tokens at a pinned snapshot as total token ceiling minus committed and reserved token consumption in the same tokenizer revision; return both operands and the checked result. Bind the complexity decision and remaining budget to the sealed ContextPackageSelection and its R137.7-.12 solver certificate; R128.5 task-class pressure remains a separate input.

Planned acceptance: Case R137.16-complexity-and-remaining-budget: vary complexity while holding model cost and remaining tokens fixed, then vary remaining tokens while holding complexity fixed; verify the declared policy and certified package bind both decisions. Refuse missing/invalid policy, tokenizer or snapshot mismatch, negative/overflowing remainder, or package cost above remaining tokens.

Exact attachment and hold: Planned Decide(ContextBudget) -> existing solver -> independent certificate verifier -> sealed ContextPackageSelection -> AU-CONTEXT-R004. R137.11/.12 name the consumer; the dispatch and receipt binding remain pending as recorded in R137.9.

This is a SPECIFIED definition only; implementation and served evidence remain pending.

Preserve existing coverage: `EG-DECISION-ENGINE-R137.7`, `EG-DECISION-ENGINE-R137.8`, `EG-DECISION-ENGINE-R137.10`, `EG-DECISION-ENGINE-R137.9`, `EG-DECISION-ENGINE-R137.11`, `EG-DECISION-ENGINE-R137.12`.

Owner source hash: `ccbc35ba3595763696c824434868079aeb401ce8b601041e913b323a2d2fb575`.

### `EG-DECISION-ENGINE-R029.2` — EG:30

Original objective: An agent loads only context relevant to its task, selected from available sources within a stated budget, instead of the full persistent memory; omitted sources are reported.

EG-owned residual: The complete candidate universe is not necessarily the complete available-source universe. No exhaustive, explicit omitted-source report with relevance/budget reasons is required. Displaced authority-conflict sources cover only one omission cause.

Producer/consumer boundary: Served Decide(ContextBudget) -> certified package -> AU-CONTEXT-R004 context compile; ontology source selection R072 and source-authority conflict handling supply distinct inputs.

Proposed contract: Define AvailableContextSourceV1 with authorized source id/revision/digest, relevance evidence, authority class, token/cost estimate and enumeration snapshot. ContextSourceSelectionV1 returns complete selected and omitted partitions with reason enum IRRELEVANT, OVER_BUDGET, AUTHORITY_DISPLACED, or UNAUTHORIZED, plus deterministic policy revision and source snapshot digest. Every enumerated source appears exactly once; protected source identifiers are omitted from unauthorized caller-visible details.

Planned acceptance: Case R029.2-complete-source-partition: the same served compile journey includes relevant selected sources and omitted examples for irrelevance, over-budget and authority displacement; every authorized source is accounted for once. Refuse incomplete enumeration, duplicate source, unknown reason, stale budget/authority snapshot or unauthorized disclosure.

Exact attachment and hold: Served Decide(ContextBudget) -> certified package -> AU-CONTEXT-R004 context compile; R072 ontology selection and EG-SRC-AUTH-R010.6 provide distinct source inputs. The AU consumer receipt for the exhaustive partition is not named; preserve the parent retrieval/authorization obligations.

This is a SPECIFIED definition only; implementation and served evidence remain pending.

Preserve existing coverage: `EG-DECISION-ENGINE-R029`, `EG-DECISION-ENGINE-R137.7`, `EG-DECISION-ENGINE-R137.8`, `EG-DECISION-ENGINE-R137.11`, `EG-DECISION-ENGINE-R137.12`, `EG-FEDERATED-QUERY-R072`, `EG-SRC-AUTH-R010.6`.

Owner source hash: `05fcbe47e588141000ec329927406270abd30b590d2624f22e9239079f829d38`.

### `EG-DECISION-ENGINE-R134.15` — EG:36

Original objective: Task scheduling computes critical paths over plan graphs using topological order and longest path, and reports the critical chain; cyclic plans are rejected.

EG-owned residual: No duration/weight-based longest-path calculation or critical-chain output is required. A DAG and dependency order alone do not define critical-path scheduling.

Producer/consumer boundary: Served Decide(TaskPlan) -> plan_over_skills -> typed TaskPlan -> execution consumer; critical-chain fields and computation remain to be defined.

Proposed contract: For each TaskPlan DAG step require positive duration_ms, nonnegative integer priority_weight and source evidence, all bound to a weight-policy id/revision and input digest. Compute checked integer path totals from duration_ms multiplied by priority_weight using the declared weight scale; return ordered critical_chain_step_ids, total_weighted_duration_ms, policy revision, plan revision and input digest. Ties choose the lexicographically smallest step-id sequence.

Planned acceptance: Case R134.15-weighted-critical-chain: a branched DAG returns the independently calculated longest weighted path and total, source-duration/weight provenance, and deterministic tie result. Refuse a cycle, missing/invalid duration or weight, absent scale policy, stale source evidence, or checked multiplication/sum overflow before emitting TaskPlan.

Exact attachment and hold: Attach the proposed fields to served Decide(TaskPlan) -> plan_over_skills -> typed TaskPlan. The execution consumer schema for critical-chain fields is not defined; the decision producer and consumer remain SPECIFIED pending that schema.

This is a SPECIFIED definition only; implementation and served evidence remain pending.

Preserve existing coverage: `EG-DECISION-ENGINE-R134.1`, `EG-DECISION-ENGINE-R134.4`, `EG-DECISION-ENGINE-R134.5`, `EG-DECISION-ENGINE-R134.6`, `EG-DECISION-ENGINE-R134.8`, `EG-ORCH-VOCAB-R006`.

Owner source hash: `6e1f0c3ab1626a00c8392e8a304b4c4a28a86d0aee8fff1933317bbdb4893966`.

### `EG-DECISION-ENGINE-R135.8` — EG:52

Original objective: Workflows written in natural language or stored in the knowledge graph compile on demand to typed graph flows; compiled steps and agent configurations are saved for fast routing on reuse, and a compile failure names the missing step.

EG-owned residual: Successful-trace compilation is not on-demand compilation from natural-language or graph-stored workflow definitions. Persisted ProgramStep lacks an explicit reusable agent-configuration contract, and the compile failure names run failure/size rather than the missing workflow step.

Producer/consumer boundary: Served Decide(TaskPlan) and ProgramLookup -> AgentLibraryStore/handle_decision_commit -> harness; SourceIngest is the workflow ingestion authority. A definition-to-program compiler between those surfaces remains unspecified.

Proposed contract: Define WorkflowCompileRequestV1 with source kind NL or graph, exact source revision/digest, workflow identifier, compiler id/revision and referenced step/agent-config ids. The output CompiledWorkflowV1 binds canonical typed step graph digest, each agent configuration id/revision/digest, source-to-step mapping, and reuse key (source digest, compiler revision, configuration digest). A missing referenced step returns MISSING_WORKFLOW_STEP with the exact id/source location and publishes no partial program. Equivalent NL/graph sources share a canonical result only when their typed graph digests match.

Planned acceptance: Case R135.8-workflow-compile: equivalent NL and graph inputs with the same canonical typed graph produce the same reuse key and read back a complete stored configuration; a repeated exact input reuses it. Refuse missing step with exact diagnostic, stale source/config/compiler revision, or partial program publication.

Exact attachment and hold: Proposed attach points are served Decide(TaskPlan), ProgramLookup and AgentLibraryStore/handle_decision_commit; SourceIngest remains the workflow ingestion authority. The compiler interface and stored configuration schema between these surfaces are unspecified.

This is a SPECIFIED definition only; implementation and served evidence remain pending.

Preserve existing coverage: `EG-DECISION-ENGINE-R134.1`, `EG-DECISION-ENGINE-R134.4`, `EG-DECISION-ENGINE-R134.5`, `EG-DECISION-ENGINE-R134.6`, `EG-DECISION-ENGINE-R135.1`, `EG-DECISION-ENGINE-R135.2`, `EG-DECISION-ENGINE-R135.3`, `EG-DECISION-ENGINE-R135.4`, `EG-DECISION-ENGINE-R135.5`, `EG-DECISION-ENGINE-R135.6`, `EG-ORCH-VOCAB-R006`, `EG-REPO-INGEST-R002`.

Owner source hash: `1ee5f2b5bd5bc45c9b9ff8ccaab8de944ed41eceab17a8c1e88cc86d4725e2dc`.
<!-- /spec-definition-patch -->

<!-- spec-definition-patch:reviewed-mapped-subsets-20261012 -->
## Reviewed mapped children — 2026-10-12

The children below extend existing rollups. Existing parent requirements, children, policy authority and refusal behavior remain in force. These are SPECIFIED definitions only; no code, producer receipt, consumer receipt or served result is claimed.

### Extension of R042

Preserve the existing `R042` obligation and all prior children; add `EG-DECISION-ENGINE-R042.100`, `EG-DECISION-ENGINE-R042.101`. The child definitions below state only the mapped residual and its exact owner boundary.

#### `EG-DECISION-ENGINE-R042.100` — AU:28

Model `ToolPathHistory` by tenant, connector, capability, stable path id/version, observation window, sample count, success/failure counts, as-of time and source invocation ids. Behavior ranks only already-admitted equivalent paths by observed success rate; skip a path only when the versioned decision policy declares a minimum sample and failure threshold and both are met. Missing, stale or undersampled history is `unknown` and cannot suppress a path. Wiring adds the evidence to the existing R042 Decide advisory and consumes current attempt outcomes from SDK `ordered_equivalent_fallback`; it must not replace the SDK declared fallback order or execute writes. Evidence binds policy, candidate paths and observation window.

Producer/consumer: EG R042 remains evaluate-only connector tool-choice advisory; SDK owns ordered transport fallback and current attempt outcomes.

Source record SHA-256: `9dac8e5f41f6948885215836733ce3116b8246ca5c9c4725a12a62bdb11cb400`. Verification remains planned until independently served by each named owner.

#### `EG-DECISION-ENGINE-R042.101` — AU:86

Define `ToolNecessityMeasurement` bound to model id/revision, tool/capability id, task-cohort digest, paired tool-enabled/tool-disabled trial identities, measured outcome metric, sample counts, policy threshold, measurement time and digest. A measurement is `necessary`, `not_necessary` or `unknown` only under the declared minimum-sample and outcome-delta policy; no data, stale model revision or insufficient pairs yields `unknown`. Compare every AU tool-call decision to the current model/tool measurement and emit a mismatch trace event with run, step, decision, model revision, measurement digest and mismatch kind (`unnecessary_call` or `omitted_necessary_tool`). The EG R042 decision remains authoritative; AU consumes at the existing per-tool span path.

Producer/consumer: R134.2 relevance is only a candidate prefilter and AU-HARNESS-R018.5 generic profile metrics do not measure tool necessity; AU run_agent per-tool spans are the consumer boundary.

Source record SHA-256: `38315c5f9a088c33c4b81b88b813ed851f97f454d788c9fb07185c88c752f979`. Verification remains planned until independently served by each named owner.

### Extension of R129

Preserve the existing `R129` obligation and all prior children; add `EG-DECISION-ENGINE-R129.100`. The child definitions below state only the mapped residual and its exact owner boundary.

#### `EG-DECISION-ENGINE-R129.100` — AU:38

Bind a correction to tenant, run, original output id/digest and correction author/time. A proposal carries rule id, proposed version, scope, source output, predecessor version and lifecycle state. It remains inactive until explicit approval and the existing R129 invariant gate both pass; invariant compliance is necessary but never substitutes for approval. Activation supersedes the declared prior version atomically; revocation names the active version and prevents future use while retaining audit history. AU future invocations consume only the approved current version and return its id/version in the run trace.

Producer/consumer: Extend R129 promotion authority and AU future-invocation application; R034 contradiction-retraction remains separate.

Source record SHA-256: `1d54abebd7cbc92bd3d6c397f334b42d602b4c3b9f77e0091f1c683774701d72`. Verification remains planned until independently served by each named owner.

### Extension of R134

Preserve the existing `R134` obligation and all prior children; add `EG-DECISION-ENGINE-R134.100`, `EG-DECISION-ENGINE-R134.101`, `EG-DECISION-ENGINE-R134.102`. The child definitions below state only the mapped residual and its exact owner boundary.

#### `EG-DECISION-ENGINE-R134.100` — AU:30

Define the generated `AggregateFanout/v1` request with the selected existing `AggregationOp`, TaskPlan digest, AU `FanoutResult` identity, expected subtask ids, and GraphOS durable instance-receipt ids/digests. Define the result with operation/version, ordered input receipts, one typed aggregate artifact/schema version, result digest and graph revision. EG applies the already-selected operator to the complete receipt set; it adds no fan-out width, concurrency policy or second aggregation authority. A served independent readback resolves the returned artifact by tenant/id/revision and compares the digest before success.

Producer/consumer: AU owns FanoutResult production; GraphOS `agents.run` owns durable instance receipts and capacity/refusal. EG owns selected operator and public application/readback contract only. Preserve existing R134.1/.3 and GRAPHOS-FLEET-R024.5-.9.

Source record SHA-256: `8f2ff3030a71d67b20cd19301f0ed8cef75e975403595cec4060abf9fa8952c1`. Verification remains planned until independently served by each named owner.

#### `EG-DECISION-ENGINE-R134.101` — AU:94

Extend the existing task plan with one designated `coordinator_id` and a typed final-artifact declaration (schema id/version and artifact kind). The coordinator receives the complete FanoutResult, produces exactly one artifact, and binds each expected subtask id to its result or the named failure gap; no result may disappear in synthesis. The artifact and delivery receipt bind plan digest, coordinator, child receipt ids, schema version, artifact digest and final readback identity. Reuse R134 aggregation and AU fan-out; do not add a second orchestrator.

Producer/consumer: EG-selected TaskPlan/AggregationOp -> AU `_fork_fanout` FanoutResult -> named coordinator -> one final artifact; retains AU-CONTROL-R043.2/.4 named gaps.

Source record SHA-256: `19331af6d24dacefccebddcae637919b39cb111b800e3bd239a68b42b8ad10c7`. Verification remains planned until independently served by each named owner.

#### `EG-DECISION-ENGINE-R134.102` — AU:24

Define an EG delegation-admission verdict bound to task class, quality metric, declared threshold, observation window start/end, sample count, outcome-set digest and policy revision. Admit delegation only when the measured quality over the complete stable window meets the declared threshold; insufficient samples, gaps, mixed revisions or stale evidence return typed not-ready/refusal. AU executes delegation only against the committed verdict. Do not substitute retrieval saturation, generic test coverage or a free-floating data-plane window.

Producer/consumer: EG owns authoritative TaskPlan/delegation admission; AU executes the admitted plan. Existing retrieval quality-saturation logic is adjacent only.

Source record SHA-256: `7cbecf865c6185e23ec202d541c5f8d33388338af413ac06209709c619fbe200`. Verification remains planned until independently served by each named owner.

### Extension of R137

Preserve the existing `R137` obligation and all prior children; add `EG-DECISION-ENGINE-R137.100`. The child definitions below state only the mapped residual and its exact owner boundary.

#### `EG-DECISION-ENGINE-R137.100` — AU:59

Define an EG disclosure decision/event bound to instruction-set id, immutable version/digest, relevance trigger, decision id and scope. Initial rules are a distinct phase delivered before the first agent action; runtime refinements are disclosed only when their declared relevance trigger occurs. Each event records phase, trigger, instruction version/digest, run and sequence, plus applied/refused outcome. EG owns relevance selection; AU run loop applies the exact selected version and records the event. No duplicate local context selector is introduced.

Producer/consumer: Existing EG context/decision authority -> AU run loop; general context selection, reminder and trace counters do not cover disclosure. AU sole writer owns runtime application.

Source record SHA-256: `0b55041f0517baa9de449e3d712356a0e252cd194365e09ae559e9067cc8eb1f`. Verification remains planned until independently served by each named owner.

### Extension of R136

Preserve the existing `R136` obligation and all prior children; add `EG-DECISION-ENGINE-R136.100`, `EG-DECISION-ENGINE-R136.101`, `EG-DECISION-ENGINE-R136.102`. The child definitions below state only the mapped residual and its exact owner boundary.

#### `EG-DECISION-ENGINE-R136.100` — AU:32

Define a correction proposal bound to failed-run trace digest, proposed fix revision, exact regression-test identity and test environment. Admission requires a before receipt showing that same test fails against the declared pre-fix revision and an after receipt showing it passes against the declared post-fix revision. Both receipts bind test digest, source revision and run identity; mismatch, missing receipt or reversed outcome refuses. Reuse existing correction/admission authority and preserve propose-only flow.

Producer/consumer: AU CorrectionProposal/study_admit supplies trace and benchmark; EG decides admission. Generic benchmark lift does not replace paired proof.

Source record SHA-256: `1d12449ecff9c30c5999933324f1f705fd826539eba6182f3e6d2773c263fec3`. Verification remains planned until independently served by each named owner.

#### `EG-DECISION-ENGINE-R136.101` — AU:47

Bind each report revision to a stable rubric digest, criterion ids, prior satisfaction states, new evidence/outcome ids and independent grader revision. For every previously satisfied criterion, the new revision must preserve satisfaction; a regression blocks promotion and records the failing criterion. Self-reflection text is not a measured outcome. Use AU ContinuousEvaluationEngine reports as inputs and preserve EG R136 round verdict authority.

Producer/consumer: AU ContinuousEvaluationEngine produces per-criterion evidence/report revisions; EG evaluates the bound round verdict. Aggregate divergence and self-reflection are not criterion-level nonregression.

Source record SHA-256: `9f72c0e6cb8d476050ffde54043ba0e37a063bfda05eb3e27020312cd9eeb69b`. Verification remains planned until independently served by each named owner.

#### `EG-DECISION-ENGINE-R136.102` — AU:56

Require proposal evidence to bind a declared outcome, immutable rubric id/version/digest, exact graded artifact id/digest, grader revision, and one grade record per rubric criterion containing score/verdict and evidence reference. Every required criterion must be graded; missing, ungraded, mismatched or stale artifacts return a typed refusal before any proposal is admitted. Aggregate score cannot hide a missing criterion.

Producer/consumer: AU study_admit/ContinuousEvaluationEngine supplies the report; EG decision verdict controls admission. Preserve R019/R021 evidence and per-criterion R136.101 behavior.

Source record SHA-256: `712793000145f8e6b1c3ac1de647e91256312eb601def299653a9671de2757ba`. Verification remains planned until independently served by each named owner.

### Extension of R031

Preserve the existing `R031` obligation and all prior children; add `EG-DECISION-ENGINE-R031.100`. The child definitions below state only the mapped residual and its exact owner boundary.

#### `EG-DECISION-ENGINE-R031.100` — AU:41

Define an EG per-paper admission request/decision binding paper locator/content digest, summary digest, platform-goal revision, graph snapshot revision, relevance dimensions/scores, declared threshold and selected claim ids. A paper is selected for ingestion only when it meets the configured relevance rule against both platform goals and current graph content; the verdict names the scored evidence and selected claims. Excluded papers produce no graph write. This definition covers scoring/admission only, not scheduled fetching.

Producer/consumer: GraphOS owns six-hour durable scheduler; SDK owns paper fetching/provider receipts; EG owns relevance scoring/admission and selected-claim contract.

Source record SHA-256: `8c69b66d62c833b4da482e38a51efa624c3e3fc924d6fd6d3e2ef114378a8527`. Verification remains planned until independently served by each named owner.

### Extension of R036

Preserve the existing `R036` obligation and all prior children; add `EG-DECISION-ENGINE-R036.100`, `EG-DECISION-ENGINE-R036.101`. The child definitions below state only the mapped residual and its exact owner boundary.

#### `EG-DECISION-ENGINE-R036.100` — AU:46

Extend graph.assemble primitives to a closed typed union covering prompt, tool, memory and control-flow components. Each primitive exposes identity/version, input/output schema digest, capability/effect declaration and substitution compatibility contract. A candidate harness graph is accepted only when every edge type-checks and replacement preserves declared capabilities/effects; recorded run traces can create improvement proposals for each primitive kind, but proposals never mutate active composition.

Producer/consumer: EG graph.assemble is composition authority; AU HarnessPort executes selected composition. Existing prompt-only optimization proposal path remains and typed HarnessPort is preserved.

Source record SHA-256: `04556089bf2790a52c59ede225b6212ccd90c0f7ca1dfad07b869ac9de6a19a1`. Verification remains planned until independently served by each named owner.

#### `EG-DECISION-ENGINE-R036.101` — AU:107

Extend graph.assemble request with verified coding-agent facts (identity, capability, path-discovery receipt and revision) and ordered skill transformation ids/versions with input/output schema digests. EG chooses only from supplied verified facts and emits a deterministic composition plan binding agent, skills and artifact contract; it never scans PATH or renders a UI. AU owns executable discovery; WebUI owns sandboxed frame rendering.

Producer/consumer: EG composition authority -> AU PATH discovery and skill execution -> WebUI isolated generated-artifact frame. Existing DESIGN guidance does not provide these producer/consumer receipts.

Source record SHA-256: `d179fc0eb33039df3a87efe8a59ce2a635f0e39f360c58f925011448d0401cdc`. Verification remains planned until independently served by each named owner.

### Extension of R121

Preserve the existing `R121` obligation and all prior children; add `EG-DECISION-ENGINE-R121.100`. The child definitions below state only the mapped residual and its exact owner boundary.

#### `EG-DECISION-ENGINE-R121.100` — AU:54

Define a `StepVerification` bound to run/step id, input and output digests, evidence ids, verifier/policy revision, uncertainty interval and proof/trace digest. Each reasoning step receives a typed verdict (`verified`, `failed`, `uncertain`); only `verified` steps may feed dependent conclusions. A failed or uncertain step and all derived dependents are withheld/refused with the dependency chain named. Reuse typed Claim/Evidence provenance; a Claim class alone does not imply step verification.

Producer/consumer: EG owns proof/evidence/verdict; AU run execution gates continuation on the returned verdict. Existing Claim/Evidence and ordered trace digest semantics remain intact.

Source record SHA-256: `78c1b58ee2c97f19ad4834981d1b75adeb89c82881710aed055a0cc6f5c36760`. Verification remains planned until independently served by each named owner.

### Extension of R138

Preserve the existing `R138` obligation and all prior children; add `EG-DECISION-ENGINE-R138.100`, `EG-DECISION-ENGINE-R138.101`. The child definitions below state only the mapped residual and its exact owner boundary.

#### `EG-DECISION-ENGINE-R138.100` — AU:55

Define a changeset-bound adversarial verifier request over generated artifact digest, declared tests, threat/attack policy revision and verifier revision. Its receipt enumerates attempted break cases and edge-case ids with fixed/retested state. Admission of generated code requires a complete verifier receipt and every surfaced edge case fixed against a new artifact digest and rechecked; unresolved, missing, stale or mismatched cases block acceptance. Keep materialization under existing governed write authority.

Producer/consumer: AU generates/proposes code and verifier evidence; EG admission consumes it; existing governed write path retains materialization authority. ProbeVerdict R138.1-.6 is a separate red-team probe capability.

Source record SHA-256: `3d07d9e40d19b8d4502fcbf3fc2e682f67bcdeaa0730d5d698f7af32767bb785`. Verification remains planned until independently served by each named owner.

#### `EG-DECISION-ENGINE-R138.101` — AU:83

Define a review request bound to base/head revisions and changed-file digest. The semantic review evaluates injection, authentication bypass, data exposure, cryptographic flaws and business-logic flaws. Each finding carries class, affected path/range, exploit/impact evidence, severity, confidence and trace to reviewed artifact. A versioned impact policy filters low-impact false positives with a retained disposition; no high-impact or unreviewed class may be silently dropped. This is distinct from R138 probe scorer selection.

Producer/consumer: EG owns review verdict/schema; named GraphOS authenticated changeset-review producer/consumer must be served and linked separately. Existing R138.3 probe endpoint is not that consumer.

Source record SHA-256: `0c5f2631be262ba69472eb94841b7acfbcfd6d0cb379382c528ccd0efb4d22e7`. Verification remains planned until independently served by each named owner.

### Extension of R014

Preserve the existing `R014` obligation and all prior children; add `EG-DECISION-ENGINE-R014.100`. The child definitions below state only the mapped residual and its exact owner boundary.

#### `EG-DECISION-ENGINE-R014.100` — AU:108

Extend the existing bounded composition objective with candidate source-size facts (exact artifact bytes and line count), measured per-run cost, adequacy verdict and legal/policy constraints, each bound to candidate and revision. First exclude candidates failing required behavior, tests, policy or legal constraints; among adequate candidates apply the declared size/cost objective and emit each measured fact and tie-break. Unknown size/cost or adequacy evidence cannot be treated as zero. This child does not settle the shortcut-comment policy, which remains held for coordinator resolution.

Producer/consumer: Reuse R014 exact bounded solver/graph.assemble authority. AU records execution cost; code-size measurement and candidate adequacy feed EG. Preserve legal/policy constraints and hold comment-style conflict.

Source record SHA-256: `2dfb2ab7469f19faf03d91782fa276ed2c613dec2e1d1fce2c629e5b8974b476`. Verification remains planned until independently served by each named owner.
<!-- /spec-definition-patch -->

<!-- spec-definition-patch:final-mappings-20261012 -->
## Final mapped contract extensions — 2026-10-12

These definitions are planned contracts; they do not claim implementation or served evidence.

### New rollup `EG-DECISION-ENGINE-R140` — Trajectory refinement and ranking family

Groups complexity-gated K-trajectory deliberation, provenance-preserving experience reinsertion, outcome-driven branch breadth and verifier-free pairwise ranking. Preserve calibrated-ranking requirements in R130 and AU generation/execution ownership.

### New rollup `EG-DECISION-ENGINE-R141` — Memory-change evaluation and rewrite admission

Requires fidelity and memory-quality evidence before activating a memory change; failed candidates preserve the prior active memory. R060 fitted-head promotion remains a separate model gate.

### New rollup `EG-DECISION-ENGINE-R142` — Atomic-before-workflow local-model certification

Defines the EG attestation producer, AU local-model evaluation consumer and GraphOS validation gate. Existing GraphOS R034 identity bindings and host policy remain authoritative.

### `EG-DECISION-ENGINE-R139.100` — AU:58

Extend comparative `StudySource` admission with a `graph_ref` containing tenant, graph, node, revision and content digest. Resolve the exact revision through the authorized graph read/query path; compare papers and codebases from graph refs alongside existing filesystem sources without copying source bodies. Record ordered source kind, locator/reference, exact revision, digest and comparison membership in the comparative result. Missing, stale, invisible or digest-mismatched graph references refuse before comparison; preserve filesystem intake and AU study/feature ownership.

**Producer/consumer boundary:** EG owns authorized graph resolution and returns pinned source evidence; AU owns StudySource intake, comparison execution and proposal generation. Preserve AU-HARNESS-R019.2/.3/.5/.6 and do not treat capability lookup as graph-source intake.

**Source record SHA-256:** `f13717cacb9e87ff76591467e17b5f9d76d44577f62d71b57d57c42bc89d37ba`.

**Preserved parent:** `EG-DECISION-ENGINE-R139` remains unchanged.

### `EG-DECISION-ENGINE-R019.100` — GRAPHOS:17

Bind each decision record to its decision id/revision and the simulation, inspection and process-knowledge stream ids, revisions and digests used. Expose an authorized reviewer-readable provenance read. Before any recommendation commit, require an approval receipt bound to the decision revision and recommendation digest with reviewer principal, approval time and approval-record revision; missing, stale or mismatched approval refuses before commit. Retain existing R019 re-derivation/CAS and R047 reference semantics; do not duplicate governed-effect audit rows.

**Producer/consumer boundary:** EG produces decision/provenance and approval-binding inputs; GraphOS owns shared invoke/A2A approval admission; SDK owns governed source effects and effect receipts. R047 remains the related producer reference.

**Source record SHA-256:** `0e4858352b73b18661ed21ef503cc4c4f36e72045c3ffeff8d2094d9f8f8e50a`.

**Preserved parent:** `EG-DECISION-ENGINE-R019` remains unchanged.

### `EG-DECISION-ENGINE-R029.100` — PIPE:22

Define a versioned typed command-config document per retrieval command with command id/schema version, typed parameters and a referenced inference-mode id/version/digest. EG Decide binds the selected retrieval plan and inference-mode digest to its decision; AU loads the immutable command file, validates its schema and requires its referenced mode digest to match the EG decision before applying it. AU run evidence binds command/config digest, mode digest, input digest, selected-plan revision and result digest. Missing, malformed, stale or mismatched configuration refuses before execution; file loading remains AU-owned.

**Producer/consumer boundary:** EG owns the typed plan/mode decision and digest; AU owns command-file loading and execution. Do not conflate plan selection with file loading or add a pipeline runtime orchestrator.

**Source record SHA-256:** `0d3964cba3359a3b454f2fd7b61f2b893b3aec0d58613062b859c2201344980c`.

**Preserved parent:** `EG-DECISION-ENGINE-R029` remains unchanged.

### `EG-DECISION-ENGINE-R140.100` — AU:44

For each task, emit a versioned complexity decision with task digest, scored features, score, threshold/policy revision, branch (`simple` or `complex`) and permitted K. Below the policy threshold, choose the single-trajectory path and make no deliberation call. At or above it, return only the permitted K and decision digest in the plan; AU alone generates the K alternatives and executes deliberation, binding its receipt to that decision digest. Missing/stale score or policy, invalid K, or a complex decision with K below two refuses before AU execution. The threshold and K limits are explicit versioned policy inputs, not silently selected constants.

**Producer/consumer boundary:** EG owns score/policy/branch decision; AU owns trajectory generation and deliberation execution. AU-CONTROL-R043 independent-subtask fan-out remains distinct.

**Source record SHA-256:** `74416937af9a4934ae1f5d1a02e30336deb1535189a07f53fb81fb352f81779f`.

**Preserved parent:** `EG-DECISION-ENGINE-R140` remains unchanged.


**Operational wiring / exact hold:** The existing production route is `Method::Decide` with `QuestionKind::TaskPlan` in `handle_decide` (R134.5); its response contains the complexity decision and plan only. AU execution must consume that decision digest and return the same K in its run receipt. AU-CONTROL-R043 is adjacent fan-out, not this receipt; AU consumer definition is a pending prerequisite.
### `EG-DECISION-ENGINE-R140.101` — AU:51

Represent each source trajectory with run/candidate ids, source revision and trace digest. The experience projection records the redaction-policy revision and removed reasoning-token spans, retains task/result/evidence spans, and records a deterministic shuffle seed plus ordered projected ids. Deliberation output is a new candidate whose parent list names every consumed experience id and digest; append it as one extra refinement trajectory without replacing source records. Missing provenance, invalid redaction policy or an output not linked to the consumed inputs refuses reinsertion.

**Producer/consumer boundary:** AU record_outcome/SkillGraphPipeline owns experience production and deliberation; EG owns the refinement decision/result binding. Preserve AU-HARNESS-R022 experience identities and source trace provenance.

**Source record SHA-256:** `be286cbd079c08493be1dcc0512aa562b6e2f2fec8d3566437837d91b5f36ba0`.

**Preserved parent:** `EG-DECISION-ENGINE-R140` remains unchanged.

### `EG-DECISION-ENGINE-R140.102` — AU:84

Before each candidate branch starts, retrieve authorized prior attempts for the same search id and bind their branch ids, outcome revisions and digests into the branch request. EG emits a versioned breadth decision with retrieved outcome ids, prior breadth, selected breadth and policy revision; AU executes only that breadth and returns outcomes linked to the decision. A later branch must be able to change breadth from an earlier outcome under the declared policy. Missing, stale or cross-tenant experience is not treated as a zero outcome and cannot drive adaptation.

**Producer/consumer boundary:** EG TaskPlan/Decide owns committed breadth decisions; AU owns branch execution and attempt outcomes. Preserve AU-HARNESS-R020 worker slots and R022 outcome storage; this child covers long-horizon candidate branches only.

**Source record SHA-256:** `b56fbf074624786b4a8c2b499f985ecc2d14d1e8349d08781cf28aba07c3abef`.

**Preserved parent:** `EG-DECISION-ENGINE-R140` remains unchanged.


**Operational wiring / exact hold:** Use the R134.5 TaskPlan/Decide route for the branch-start `BreadthDecisionV1`. History comes from AU `record_outcome -> SkillGraphPipeline` (R022.4); AU `_dispatch_parallel_batch -> parallel_batch_processor` (R020.5) must consume the committed decision id/digest and return outcome refs bound to it. Neither AU row currently binds this decision; the branch-start/result receipt is a pending owner dependency.
### `EG-DECISION-ENGINE-R140.103` — AU:87

When no verifier is available, bind an ordered candidate set and candidate digests to pairwise comparison records; each pair record carries both ids, judge/model revision, verdict and evidence digest. Do not aggregate into a global ranking until an owner-approved, versioned policy defines cycle, incomplete-pair and tie behavior; Bradley–Terry is not selected by the source. Until the owner-approved aggregation policy defines complete-pair behavior, cycle and tie resolution, bounded keep-count/revision limits, and AU receipts, refuse to emit a global ranking, retained partition, or revision request. No positive ranking behavior is specified by this source record. An available verifier routes through its existing verifier contract instead of this fallback.

**Producer/consumer boundary:** EG owns pairwise evidence aggregation, ranking and retention decision; AU owns comparison execution and revisions. Preserve calibrated-ranking constraints in R130 and AU generation ownership.

**Source record SHA-256:** `6f67238075c0bf262188d97a1813b18bfb0c71e13c6689a583c788d956287612`.

**Preserved parent:** `EG-DECISION-ENGINE-R140` remains unchanged.

### `EG-DECISION-ENGINE-R141.100` — EG:23

A rewrite proposal binds tenant, memory id, active source revision/digest, candidate revision/digest, fidelity metric/policy revision and metric-spec digest, measured fidelity, signal and token counts, signal-per-token value and degradation-history window. Admit only when the declared fidelity threshold passes and the policy’s signal-efficiency/degradation checks pass. On any failed gate, discard the candidate and retain the original active revision unchanged; do not infer rewrite admission from R059 aggregate outcome fidelity or R128 elide/summarize selection.

**Producer/consumer boundary:** EG owns the rewrite evaluation decision; AU may generate candidate rewrites. The source does not identify the fidelity formula/normalization, so an immutable reviewed metric specification is required before an Admit verdict. The authoritative active-memory store/write/CAS path is also not identified and remains a prerequisite to an activation claim. Retain the original as the rollback source.

**Source record SHA-256:** `ac1b5df0212beaade03e78502a494148692e1a0007c3fa68ee37a2bb6dbe8cce`.

**Preserved parent:** `EG-DECISION-ENGINE-R141` remains unchanged.


**Operational wiring / exact hold:** Attach evaluation to the existing `Method::DecisionEval` job (`DecisionEvalOp::MemoryRewriteSubmit`). Its receipt decides admit/discard from the pinned fidelity, signal-per-token and degradation policy; it does not itself claim an active-memory write. The source does not identify the authoritative memory store, complete mutation entrypoint set or atomic CAS that must require the receipt. Those are explicit prerequisites before any end-to-end rewrite admission claim.
### `EG-DECISION-ENGINE-R141.101` — AU:125

Before any memory mutation is activated, bind the baseline and candidate memory snapshot digests to a versioned reference dataset containing query ids, expected evidence ids and reference answers. Report recall@k and answer accuracy with numerator, positive denominator, metric specification revision and dataset digest; the referenced metric specifications must define exact scoring, query aggregation and rounding. The source does not supply those formulas, so missing/unreviewed semantics refuse before positive admission; compare both only under a reviewed policy. Admission requires both thresholds to pass on the same dataset and snapshot pair. Missing dataset rows, metric inputs, denominator or policy revision refuse before activation. This gate covers add/edit/delete/rewrite/consolidation changes; R060 fitted-head promotion remains a separate model-promotion gate.

**Producer/consumer boundary:** AU executes the repeatable benchmark and returns its receipt; EG validates the receipt against the candidate and baseline. Universal activation refusal remains pending the authoritative mutation/write path. Reuse existing evaluation/activation mechanisms without substituting R060.

**Source record SHA-256:** `27a6a74820b2b646163d8f7bd58b94167b9f0a3e423b8bf5210fe6e758deb440`.

**Preserved parent:** `EG-DECISION-ENGINE-R141` remains unchanged.

### `EG-DECISION-ENGINE-R142.100` — GRAPHOS:8

For each skill revision, bind an atomic/workflow classification, local model id/revision, evaluation-set digest, criteria/policy revision, result and attestation digest. EG emits the certification attestation only after AU executes the declared local-model evaluation. GraphOS validates the attestation before normal (non-certification) `orchestrate` or `agents.run`; a separately authorized evaluation run may produce a new attestation. Atomic skills are evaluated first; a workflow skill is certifiable only when every referenced atomic dependency has a valid current attestation. Missing, stale, failed or mismatched dependencies refuse certification/invocation before effects. EG attests evaluation outcome; AU executes; GraphOS owns invocation gate and host policy.

**Producer/consumer boundary:** Extend existing GRAPHOS-FLEET-R034.7-.12 identity bindings; do not add host scoring authority or infer certification from host prerequisites alone.

**Source record SHA-256:** `5df3f5a45d486aeba5765a31b6c6283637adf2bd8ce63a87938eddd28b90f082`.

**Preserved parent:** `EG-DECISION-ENGINE-R142` remains unchanged.
<!-- /spec-definition-patch -->


<!-- spec-definition-patch:owner-dispatch-20261012-spec -->

**Operational wiring / exact hold:** The EG producer attaches to `Method::DecisionEval` and consumes an AU `orchestrate` certification-evaluation receipt; it emits a digest-bound attestation only for a full per-criterion pass. Certification evaluation must be distinguishable from normal skill execution so an uncertified atomic can be evaluated without opening the normal invocation path. GraphOS remains the normal invocation gate at `GRAPHOS-FLEET-R034.7-.12` before `orchestrate`/`agents.run`; AU execution and GraphOS validation remain separate owners. Neither the AU receipt schema nor GraphOS consumption of this EG attestation is currently evidenced, so both are explicit prerequisites.
## Cross-owner retrieval evaluation and context

The existing `DecisionEval` job is the production evaluation attach point. Its new `RetrievalRankingV1` variant pins corpus, label set, query set, k, evaluator and two algorithm configurations; it seals exact hits-at-k numerator/denominator values and refuses comparisons over unequal inputs or incomplete labels. `Method::UnifiedQuery` remains the retrieval result source, and R100's independent-outcome/hard-negative persistence remains unchanged.

`RetrievalContextSearchV1` reads authorized document context through the existing query path and returns exact NFC UTF-8 half-open byte offsets, document/index revisions and bounded spans. It never treats local RRF, ranking order or absent labels as relevance truth. WebUI R001.1 consumes the shared immutable evaluation receipt; no test or served result is claimed by this definition.
<!-- /spec-definition-patch -->

## Reviewed mapped definitions — grounded answers and safety gates

### `EG-DECISION-ENGINE-R100.101` — EG:18

The existing `DecisionEval` job gains `DecisionEvalOp::GroundedAnswerSubmit` with `GroundedAnswerEvaluationV1`. Every query binds a nonempty expected-topic set; a topic counts as covered only when a mapped answer claim cites independently labeled supporting evidence above the pinned minimum support-vote fraction. The receipt returns exact expected and covered topic ids, and aggregates topic counts across the full pinned query set per single-document, local multi-document and global-corpus scope. For unsupported-claim calibration, each claim has a separately sourced, label-blind fixed-point prediction `p_i` bound to the scorer artifact/revision and answer/input digests, plus independent votes whose soft target is `q_i = unsupported_votes / valid_votes_i`. The scored set is the complete required claim set; each claim must have one prediction and one or more unique annotator votes. The weighted Brier score is `sum(valid_votes_i * (p_i-q_i)^2) / sum(valid_votes_i)`, with the positive vote-count denominator and final rounding policy sealed in the receipt. Votes are not predictions. Empty/incomplete populations, duplicate annotators, invalid probabilities and stale bindings refuse. `UnifiedQuery` supplies retrieval evidence; the scorer/AU producers supply bound prediction, answer, topic and label records; WebUI consumes the sealed result. Preserve R100 independent-outcome hard-negative behavior. This definition is SPECIFIED; no producer receipt is claimed.

### `EG-DECISION-ENGINE-R001.100` — GRAPHOS:19

The existing `Method::Decide` request carries `RecommendationSafetyV1` and returns a bound verdict for every required physical/safety constraint. Each `SafetyBoundV1` records bound identity/revision/digest, metric and unit, comparator, limit and measured value/source revision. Missing or invalid measurements, stale policy/recommendation, non-finite values, unit mismatch or a violated bound withhold the recommendation with the bound id, limit and value. GraphOS must validate the result before it surfaces any recommendation. The exact GraphOS pre-surface operation and served receipt are an explicit companion dependency and remain pending; partial AU-HARNESS-R021 cases do not cover that boundary. This definition is SPECIFIED and does not claim the GraphOS gate exists.
