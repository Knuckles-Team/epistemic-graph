# MEDULLA-CQ-ORCH — Test spec

Positive: each question returns a non-empty answer with provenance on its fixture. Negative: unconnected source returns a typed abstention; planted-invalid shape input is rejected. Integration: composed answer through assemble. Cross-repo: R009 probes run through the Arbora intent tools.
Architecture view: a fixture repository plus one diagram yields containers with support ids; an empty scope abstains.

<!-- spec-definition-patch:owner-batch-20261012-orchestration-competency -->
## Reviewed residual acceptance targets — owner batch 2026-10-12

All targets below are planned acceptance, not completed tests or receipts. Use each named producer/consumer boundary. Retain existing covered behavior.

- **`MEDULLA-CQ-ORCH-R010.7` / GRAPHOS:20:** Send served architecture view and pinned Meristem study-gap evidence through the existing Decide TaskPlan path. Every feature binds an attach point and wiring edge. Missing edge/attach, stale revisions, and absent Meristem evidence return named refusals with no plan. Exact tests are in requirements.md. Confirm Meristem fields before integration closure.
<!-- /spec-definition-patch -->

## MEDULLA-CQ-ORCH-R011 acceptance

| Case | Row | Proving test and observation |
|---|---|---|

| Question | `MEDULLA-CQ-ORCH-R011.1` | `crates/medulla-core/tests/orchestration_competency.rs::q11_load_set`: The answer is non-empty, bounded and carries one premise per row. |

| Question | `MEDULLA-CQ-ORCH-R011.2` | `crates/medulla-core/tests/orchestration_competency.rs::q11_downtime_next`: The answer lists jobs in priority order with reasons for skipped types. |

| Question | `MEDULLA-CQ-ORCH-R011.3` | `crates/medulla-core/tests/orchestration_competency.rs::q11_best_plan`: The answer names the kept plan and its measured gain over the baseline. |

| Question | `MEDULLA-CQ-ORCH-R011.4` | `crates/medulla-core/tests/orchestration_competency.rs::q11_usage_guidance`: The answer lists ordered steps with citations, and an unknown connector abstains. |

| Question | `MEDULLA-CQ-ORCH-R011.5` | `crates/medulla-core/tests/orchestration_competency.rs::q11_seed_changes`: The answer lists each part outcome of the last import. |

| Hot-path proof | `MEDULLA-CQ-ORCH-R011.6` | `tests/competency/test_job_downtime_plan_served.py::test_five_questions_through_intent_surface`: Each answer is non-empty and carries provenance. |


Hot-path proof for `MEDULLA-CQ-ORCH-R011`: `tests/competency/test_job_downtime_plan_served.py::test_five_questions_through_intent_surface`.
