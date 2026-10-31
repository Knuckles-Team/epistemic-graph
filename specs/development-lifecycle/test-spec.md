# MEDULLA-SDLC — Test specification

Governing spec: [spec.md](spec.md).

| Requirement | Name | Proving test |
|---|---|---|
| `MEDULLA-SDLC-R001` | Lifecycle record vocabulary and derived edges (rollup) | See each child. |
| `MEDULLA-SDLC-R001.1` | Model | `crates/medulla-core/tests/lifecycle_vocab.rs::edge_set_is_closed_and_shaped` |
| `MEDULLA-SDLC-R001.2` | Behavior | `crates/medulla-core/tests/lifecycle_derive.rs::rules_emit_expected_edges` |
| `MEDULLA-SDLC-R001.3` | Wiring | `tests/test_sdlc_derive_served.py::test_accept_writes_derived_edges` |
| `MEDULLA-SDLC-R001.4` | Refusal | `crates/medulla-core/tests/lifecycle_derive.rs::manual_edge_write_is_refused` |
| `MEDULLA-SDLC-R001.5` | Evidence | `crates/medulla-core/tests/lifecycle_derive.rs::rebuild_equals_incremental` |
| `MEDULLA-SDLC-R001.6` | Hot-path proof | `cargo bench --bench lifecycle_derive` |
| `MEDULLA-SDLC-R002` | Release and deployment records (rollup) | See each child. |
| `MEDULLA-SDLC-R002.1` | Model | `crates/medulla-types/tests/lifecycle_records.rs::records_round_trip` |
| `MEDULLA-SDLC-R002.2` | Behavior | `crates/medulla-types/tests/lifecycle_records.rs::unknown_digest_stays_unattributed` |
| `MEDULLA-SDLC-R002.3` | Wiring | `tests/test_sdlc_release_served.py::test_release_then_deployment_links` |
| `MEDULLA-SDLC-R002.4` | Refusal | `tests/test_sdlc_release_served.py::test_release_refusals` |
| `MEDULLA-SDLC-R002.5` | Evidence | `tests/test_sdlc_release_served.py::test_release_is_sealed` |
| `MEDULLA-SDLC-R002.6` | Hot-path proof | `tests/test_sdlc_release_served.py::test_deployment_record_latency` |
| `MEDULLA-SDLC-R003` | Defect records and intake (rollup) | See each child. |
| `MEDULLA-SDLC-R003.1` | Model | `crates/medulla-types/tests/lifecycle_records.rs::defect_round_trips` |
| `MEDULLA-SDLC-R003.2` | Behavior | `crates/medulla-core/tests/defect_intake.rs::same_signature_reuses_open_defect` |
| `MEDULLA-SDLC-R003.3` | Wiring | `tests/test_sdlc_defect_served.py::test_failed_gate_opens_defect` |
| `MEDULLA-SDLC-R003.4` | Refusal | `tests/test_sdlc_defect_served.py::test_illegal_transitions_refuse` |
| `MEDULLA-SDLC-R003.5` | Evidence | `tests/test_sdlc_defect_served.py::test_symptom_evidence_reads_back` |
| `MEDULLA-SDLC-R003.6` | Hot-path proof | `cargo bench --bench defect_intake` |
| `MEDULLA-SDLC-R004` | Defect triage decision (rollup) | See each child. |
| `MEDULLA-SDLC-R004.1` | Model | `crates/medulla-types/src/decision/defect_triage.rs::tests::decision_round_trips` |
| `MEDULLA-SDLC-R004.2` | Behavior | `crates/medulla-types/src/decision/defect_triage.rs::tests::culprit_outranks_blame` |
| `MEDULLA-SDLC-R004.3` | Wiring | `tests/decide/test_defect_triage_served.py::test_triage_records_decision` |
| `MEDULLA-SDLC-R004.4` | Refusal | `tests/decide/test_defect_triage_served.py::test_triage_refusals` |
| `MEDULLA-SDLC-R004.5` | Evidence | `tests/decide/test_defect_triage_served.py::test_triage_replays` |
| `MEDULLA-SDLC-R004.6` | Hot-path proof | `cargo bench -p medulla-numeric --bench defect_triage` |
| `MEDULLA-SDLC-R005` | Requirement state derived from the graph (rollup) | See each child. |
| `MEDULLA-SDLC-R005.1` | Model | `crates/medulla-types/tests/lifecycle_records.rs::status_answer_round_trips` |
| `MEDULLA-SDLC-R005.2` | Behavior | `crates/medulla-core/tests/spec_status_derive.rs::rules_match_the_standard` |
| `MEDULLA-SDLC-R005.3` | Wiring | `tests/test_sdlc_spec_status_served.py::test_served_status_reads_graph` |
| `MEDULLA-SDLC-R005.4` | Refusal | `tests/test_sdlc_spec_status_served.py::test_status_refusals` |
| `MEDULLA-SDLC-R005.5` | Evidence | `tests/test_sdlc_spec_status_served.py::test_matches_shared_generator` |
| `MEDULLA-SDLC-R005.6` | Hot-path proof | `tests/test_sdlc_spec_status_served.py::test_status_latency` |
| `MEDULLA-SDLC-R006` | Lifecycle competency queries (rollup) | See each child. |
| `MEDULLA-SDLC-R006.1` | Model | `crates/medulla-types/tests/lifecycle_records.rs::trace_request_bounds` |
| `MEDULLA-SDLC-R006.2` | Behavior | `crates/medulla-core/tests/lifecycle_trace.rs::trace_respects_depth_and_budget` |
| `MEDULLA-SDLC-R006.3` | Wiring | `tests/test_sdlc_trace_served.py::test_competency_questions_answer` |
| `MEDULLA-SDLC-R006.4` | Refusal | `tests/test_sdlc_trace_served.py::test_trace_refusals` |
| `MEDULLA-SDLC-R006.5` | Evidence | `tests/test_sdlc_trace_served.py::test_edges_name_rules` |
| `MEDULLA-SDLC-R006.6` | Hot-path proof | `cargo bench --bench lifecycle_trace` |
| `MEDULLA-SDLC-R004.7` | Model: triage knobs | `crates/medulla-types/src/decision/defect_triage.rs::tests::knob_specs_lie_within_bounds_and_codes_map_abstain_reasons` ; `src/server/handlers/decide/stat_tests/defect_triage_tests.rs::triage_journey` |
| `MEDULLA-SDLC-R004.8` | Behaviour: kernel in the numeric crate, bounded by knobs | `crates/medulla-numeric/src/decision/defect_triage.rs::tests::window_cap_escalation_and_duplicate_follow_the_knobs` ; `src/server/handlers/decide/stat_tests/defect_triage_tests.rs::triage_journey` |
| `MEDULLA-SDLC-R005.7` | Task state computed from task records | `tests/sdlc/test_task_state_served.py::test_task_state_follows_checkbox_and_blockers` |
| `MEDULLA-SDLC-R006.7` | Model: trace limits are knobs | `crates/medulla-types/tests/lifecycle_records.rs::trace_limits_come_from_knobs` ; `tests/test_sdlc_trace_served.py::test_competency_questions_answer` |
| `MEDULLA-SDLC-R007` | Combined quality-gate script | `tests/test_quality_gate_script.py::test_exit_code_follows_every_scanner` |
| `MEDULLA-SDLC-R008` | Zero-backlog census gates | `scripts/ci_parity.sh` |
| `MEDULLA-SDLC-R009` | Governed documentation projection | `tests/test_spec_projection_served.py::test_spec_document_projects_sections_with_spans` |
| `MEDULLA-SDLC-R010` | Landing gate runs the complete test suite | `tests/test_ci_full_suite.py::test_gate_fails_when_a_discovered_test_has_no_verdict` |
| `MEDULLA-SDLC-R011` | Signal-killed test binaries rerun in isolation | `tests/test_ci_full_suite.py::test_signal_killed_binary_reruns_per_test_process` |
| `MEDULLA-SDLC-R012` | Graph-native development: the ecosystem is developed inside the engine  | `tests/sdlc/test_graph_native_development_served.py::test_ingest_plan_change_gate_and_land_one_row` |
| `MEDULLA-SDLC-R012.1` | Every ecosystem repository is ingested | `tests/sdlc/test_graph_native_development_served.py::test_listed_repositories_ingest_and_unlisted_refuse` |
| `MEDULLA-SDLC-R012.2` | Spec rows are requirement records with caller and proof edges | `tests/sdlc/test_graph_native_development_served.py::test_spec_rows_become_requirement_nodes_with_edges` |
| `MEDULLA-SDLC-R012.3` | Changes are changesets bound to requirement rows | `tests/sdlc/test_graph_native_development_served.py::test_changeset_binds_requirements_and_refuses_unbound` |
| `MEDULLA-SDLC-R012.4` | Reachability and impact are graph queries | `tests/sdlc/test_graph_native_development_served.py::test_orphan_rows_and_change_impact_answer_from_the_graph` |
| `MEDULLA-SDLC-R012.5` | Delivery slices come from the graph | `tests/sdlc/test_graph_native_development_served.py::test_task_plan_returns_the_next_unblocked_slice` |
| `MEDULLA-SDLC-R012.6` | Specs change through the graph | `tests/sdlc/test_graph_native_development_served.py::test_concurrent_row_edits_refuse` |
| `MEDULLA-SDLC-R012.7` | Lifecycle evidence lives with the requirement | `tests/sdlc/test_graph_native_development_served.py::test_requirement_links_gate_release_and_deployment_records` |
| `MEDULLA-SDLC-R012.8` | Project mode record | `tests/sdlc/test_project_mode_served.py::test_evolving_needs_operator_approval_and_defaults_to_reference` |
| `MEDULLA-SDLC-R013` | Scenario runs gate phases and release bundles | `tests/sdlc/test_scenario_runs_served.py::test_bundle_completes_when_rows_delivered_and_scenario_passed` |
| `MEDULLA-SDLC-R014` | Spec artifacts are graph records  | `tests/sdlc/test_spec_artifact_graph_served.py::test_artifacts_ingest_and_answer_next_task` |
| `MEDULLA-SDLC-R014.1` | Deterministic parser for spec artifacts | `crates/medulla-compute/tests/spec_artifacts.rs::same_bytes_give_same_triples_without_model_calls` ; `crates/medulla-compute/tests/spec_artifacts.rs::task_line_fields_are_extracted` |
| `MEDULLA-SDLC-R014.2` | Incremental ingest by file digest | `tests/sdlc/test_spec_artifact_graph_served.py::test_one_file_change_reingests_only_that_file_within_budget` |
| `MEDULLA-SDLC-R014.3` | Records link to code items and commits | `tests/sdlc/test_spec_artifact_graph_served.py::test_bindings_files_and_trailers_become_edges` |
| `MEDULLA-SDLC-R014.4` | Contracts form the seam graph | `tests/sdlc/test_seam_graph_served.py::test_concept_resolves_to_owning_contract_and_both_repositories` |
| `MEDULLA-SDLC-R014.5` | Spec-driven development ontology pack | `crates/medulla-core/tests/sdd_spec_pack.rs::every_class_extends_a_core_class_and_none_imports_core` |
| `MEDULLA-SDLC-R014.6` | Competency questions over the artifact graph | `tests/sdlc/test_spec_competency_served.py::test_unreachable_requirements` ; `tests/sdlc/test_spec_competency_served.py::test_uncovered_code_items` ; `tests/sdlc/test_spec_competency_served.py::test_tasks_blocked_by_contract` ; `tests/sdlc/test_spec_competency_served.py::test_principles_a_change_breaks` ; `tests/sdlc/test_spec_competency_served.py::test_changes_advancing_goal` ; `tests/sdlc/test_spec_competency_served.py::test_next_unblocked_task_for_phase` |
| `MEDULLA-SDLC-R014.7` | Plans are first-class records | `tests/sdlc/test_plan_records_served.py::test_plan_sections_and_program_plans_answer_plan_to_phase` |
| `MEDULLA-SDLC-R014.8` | Wiring: spec artifacts ingest with the repository code graph | `tests/sdlc/test_spec_artifact_graph_served.py::test_repository_ingest_includes_spec_artifacts` |
| `MEDULLA-SDLC-R014.9` | Hot-path proof: served artifact graph | `tests/sdlc/test_spec_artifact_graph_served.py::test_artifacts_ingest_and_answer_next_task` |
| `MEDULLA-SDLC-R015` | Task-dispatch decision  | `tests/sdlc/test_task_dispatch_served.py::test_dispatch_runs_layers_and_lands_through_gates` |
| `MEDULLA-SDLC-R015.1` | Dispatch layers from dependencies and parallel markers | `tests/sdlc/test_task_dispatch_served.py::test_layers_follow_depends_on_and_parallel_marker` |
| `MEDULLA-SDLC-R015.2` | Run-context assembly | `tests/sdlc/test_task_dispatch_served.py::test_context_holds_row_plan_contracts_and_neighbourhood` |
| `MEDULLA-SDLC-R015.3` | Wiring: hand each task to an agent run | `tests/sdlc/test_task_dispatch_served.py::test_dispatch_hands_context_to_a_run_and_logs_it` |
| `MEDULLA-SDLC-R015.4` | Results are accepted only through gates and landing | `tests/sdlc/test_task_dispatch_served.py::test_result_counts_only_after_gates_and_landing` |
| `MEDULLA-SDLC-R015.5` | Failures feed the evolution loop | `tests/sdlc/test_task_dispatch_served.py::test_failed_task_opens_defect_and_evidence_for_proposal` |
| `MEDULLA-SDLC-R015.6` | Hot-path proof: served dispatch | `tests/sdlc/test_task_dispatch_served.py::test_dispatch_runs_layers_and_lands_through_gates` |
| `MEDULLA-SDLC-R016` | Research loop for plan unknowns  | `tests/sdlc/test_research_loop_served.py::test_unknown_triggers_research_and_cited_entry_lands` |
| `MEDULLA-SDLC-R016.1` | Unknowns trigger research runs | `tests/sdlc/test_research_loop_served.py::test_only_development_and_evolving_codebases_trigger_research` |
| `MEDULLA-SDLC-R016.2` | Findings are cited entries in research.md | `tests/sdlc/test_research_loop_served.py::test_uncited_entry_is_refused` |
| `MEDULLA-SDLC-R016.3` | Wiring: research runs use the connectors through arbora | `tests/sdlc/test_research_loop_served.py::test_research_reaches_connectors_through_arbora` |
| `MEDULLA-SDLC-R016.4` | Connector operations needed by the loop exist | `tests/sdlc/test_research_loop_served.py::test_connector_operations_cover_the_loop` |
| `MEDULLA-SDLC-R016.5` | Hot-path proof: served research loop | `tests/sdlc/test_research_loop_served.py::test_unknown_triggers_research_and_cited_entry_lands` |
| `MEDULLA-SDLC-R017` | Goals and typed proposals  | `tests/sdlc/test_proposals_served.py::test_goal_drives_typed_proposal_to_pull_request` |
| `MEDULLA-SDLC-R017.1` | Goal record | `tests/sdlc/test_proposals_served.py::test_goal_needs_a_target` |
| `MEDULLA-SDLC-R017.2` | Proposal kinds | `crates/medulla-types/tests/lifecycle_records.rs::proposal_kind_set_is_closed` |
| `MEDULLA-SDLC-R017.3` | Proposal evidence | `tests/sdlc/test_proposals_served.py::test_proposal_without_evidence_is_refused` |
| `MEDULLA-SDLC-R017.4` | Proposals land only as pull requests | `tests/sdlc/test_proposals_served.py::test_proposal_lands_only_through_changeset_and_gates` |
| `MEDULLA-SDLC-R017.5` | Proposals follow the project mode | `tests/sdlc/test_proposals_served.py::test_mode_gates_proposals` |
| `MEDULLA-SDLC-R017.6` | Proposals are reviewed in the interface and the spec-delivery page | `tests/sdlc/test_proposals_served.py::test_review_list_and_decision_round_trip` |
| `MEDULLA-SDLC-R017.7` | Hot-path proof: goal to pull request | `tests/sdlc/test_proposals_served.py::test_goal_drives_typed_proposal_to_pull_request` |
| `MEDULLA-SDLC-R018` | Release graph is ingested  | See each child. |
| `MEDULLA-SDLC-R018.1` | Bundle plans ingest as records | `tests/sdlc/test_release_graph_served.py::test_bundle_plan_becomes_records_with_in_bundle_edges` |
| `MEDULLA-SDLC-R018.2` | Hot-path proof: release graph competency | `tests/sdlc/test_release_graph_served.py::test_open_tasks_of_bundle_in_train_order_match_plan` |

## Negative and boundary cases

- A direct write of a derived edge is refused. The graph stays unchanged.
- A deployment with an unknown image digest stays `unattributed`.
- A duplicate release version is refused.
- A defect status move that skips a state is refused.
- A move to `verified` without a passing bound test is refused.
- A triage of a closed defect is refused and writes no decision.
- A status read at an unknown revision is refused.
- A trace anchor outside the caller tenant returns an empty answer.

## Integration

- Submit a changeset with a `spec_ids` trailer. Read `implements` and `citesRequirement` edges back.
- Record a failed gate verdict. Read the opened defect, its triage decision and its `introducedBy` edge.
- Record a release and a deployment. Walk the trace from a requirement to the deployment.
- Compare the served requirement state with the shared status generator on the same revision.

## Quality and release proof

- Clippy, tests, jscpd, CCCC, dupehound and KISS pass on the lifecycle code.
- The no-duplicate-authority check confirms one derivation pass and one status derivation.
- Each benchmark gates a 10 percent regression against its stored baseline.
