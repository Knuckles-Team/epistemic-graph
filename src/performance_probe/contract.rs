//! Private request, ledger, and dispatch contracts for the G-37 performance probe.
//!
//! The tables here are the stable contract boundary: row IDs, scenario membership,
//! expected equivalence checks, and the exact dispatch family remain centralized while
//! probe implementations stay in the parent module.

use std::collections::HashSet;
use std::path::Path;

use super::{
    Observation, ProbeError, ProbeRequest, RequestedRow, MAX_REPETITIONS, MAX_SCALE, PROTOCOL,
    SCHEMA_VERSION,
};

/// The top-level contract shape check: schema/protocol version, repetition bounds,
/// exactly 3 strictly-increasing scales within bound, and a well-formed hex
/// `workload_sha256`. Extracted from [`super::validate_request`].
pub(super) fn contract_shape_valid(request: &ProbeRequest) -> bool {
    !(request.schema_version != SCHEMA_VERSION
        || request.protocol != PROTOCOL
        || request.repetitions == 0
        || request.repetitions > MAX_REPETITIONS
        || request.scales.len() != 3
        || request
            .scales
            .iter()
            .any(|scale| *scale == 0 || *scale > MAX_SCALE)
        || request.scales.windows(2).any(|pair| pair[0] >= pair[1])
        || request.workload_sha256.len() != 64
        || !request
            .workload_sha256
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)))
}

/// Validate one row's `equivalence_checks` against its ledger contract: non-empty, ≤8
/// entries, exactly the expected check SET in order (against
/// [`row_equivalence_contract`]), each a well-formed lowercase/digit/`_` token ≤96
/// bytes, with no duplicates. Extracted from [`validate_request`]'s per-row loop.
pub(super) fn validate_row_equivalence_checks(row: &RequestedRow) -> Result<(), ProbeError> {
    let mut checks: HashSet<&String> = HashSet::new();
    let expected_checks =
        row_equivalence_contract(&row.row_id).ok_or("unknown exact performance ledger row")?;
    if row.equivalence_checks.is_empty()
        || row.equivalence_checks.len() > 8
        || row
            .equivalence_checks
            .iter()
            .map(String::as_str)
            .ne(expected_checks.iter().copied())
        || row
            .equivalence_checks
            .iter()
            .any(|check| is_invalid_equivalence_check(check, &mut checks))
    {
        return Err("invalid exact performance equivalence inventory".into());
    }
    Ok(())
}

/// One equivalence-check token's validity: non-empty, ≤96 bytes, lowercase/digit/`_`
/// only, and not a duplicate (`checks` accumulates seen tokens). Extracted from
/// [`validate_row_equivalence_checks`].
fn is_invalid_equivalence_check<'a>(check: &'a String, checks: &mut HashSet<&'a String>) -> bool {
    check.is_empty()
        || check.len() > 96
        || !check
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
        || !checks.insert(check)
}

pub(super) fn row_equivalence_contract(row_id: &str) -> Option<&'static [&'static str]> {
    Some(match row_id {
        "G37-HP-001" => &["stream_matches_sequential", "rollback_restores_snapshot"],
        "G37-HP-002" => &["cursor_page_matches_reference"],
        "G37-HP-003" => &["cdc_suffix_matches_reference"],
        "G37-HP-004" => &["snapshot_roundtrip", "idempotency_outcomes_preserved"],
        "G37-HP-005" => &["active_count_matches_scan"],
        "G37-HP-006" => &["restart_next_id_monotonic", "scheduler_index_marker_valid"],
        "G37-HP-007" => &["repeated_claim_same_fence", "worker_index_matches_job"],
        "G37-HP-008" => &[
            "retired_generator_changes_exact_closure",
            "unrelated_materializations_unchanged",
        ],
        "G37-HP-009" => &[
            "canonical_target_order_independent",
            "mixed_provenance_ignored",
        ],
        "G37-HP-010" => &[
            "selected_job_matches_full_reference",
            "unsatisfied_anchors_not_decoded",
        ],
        "G37-HP-011" => &["tenant_counters_match_active_jobs"],
        "G37-HP-036" => &[
            "placement_match_equivalent",
            "candidate_loop_allocation_free",
        ],
        "G37-HP-012" => &["lru_victim_matches_reference", "capacity_never_exceeded"],
        "G37-HP-013" => &["hit_payload_exact", "payload_copy_outside_lock"],
        "G37-HP-014" => &[
            "tail_matches_binary_search_reference",
            "defensive_wide_slice_matches",
        ],
        "G37-HP-015" => &["edge_count_matches_enumeration", "parallel_edges_counted"],
        "G37-HP-016" => &[
            "logical_delete_matches_reference",
            "unrelated_edges_preserved",
        ],
        "G37-HP-017" => &["resident_evict_matches_reference", "logical_rows_preserved"],
        "G37-HP-018" => &["all_parallel_rows_removed_once", "adjacency_consistent"],
        "G37-HP-019" => &[
            "induced_nodes_edges_match_reference",
            "parallel_rows_preserved",
        ],
        "G37-HP-020" => &[
            "posting_intersection_matches_hash_reference",
            "output_sorted_unique",
        ],
        "G37-HP-021" => &[
            "warm_page_matches_cold_reference",
            "cursor_has_no_duplicates",
        ],
        "G37-HP-022" => &[
            "edge_write_retains_node_caches",
            "field_write_invalidates_covering_only",
        ],
        "G37-HP-023" => &[
            "lookup_delete_matches_scan_reference",
            "rerank_matches_full_sort",
        ],
        "G37-HP-024" => &[
            "reverse_append_matches_sorted_reference",
            "equal_timestamp_order_preserved",
        ],
        "G37-HP-025" => &[
            "bounded_range_matches_filter_reference",
            "future_chunks_not_examined",
        ],
        "G37-HP-026" => &["all_aggregates_match_reference", "constant_scratch_bound"],
        "G37-HP-027" => &[
            "fusion_matches_sort_asof_reference",
            "output_clock_sorted_unique",
        ],
        "G37-HP-028" => &["topk_matches_full_total_order", "nan_last_order_preserved"],
        "G37-HP-029" => &[
            "selected_prefix_matches_full_total_order",
            "adc_and_refined_modes_covered",
        ],
        "G37-HP-030" => &[
            "neighbor_prefix_matches_full_order",
            "result_prefix_matches_full_order",
        ],
        "G37-HP-031" => &[
            "small_set_exact_matches_reference",
            "cosine_order_deterministic",
        ],
        "G37-HP-032" => &["bounded_prefix_matches_full_order", "leaf_budget_respected"],
        "G37-HP-033" => &[
            "paged_recovery_matches_full_scan",
            "composite_cursor_no_revisit",
        ],
        "G37-HP-037" => &[
            "cold_seed_returns_next_ordinal",
            "cache_invalidation_scope_exact",
        ],
        "G37-HP-034" => &[
            "delta_commit_matches_full_reference",
            "replay_idempotent",
            "untouched_rows_byte_stable",
        ],
        "G37-HP-035" => &[
            "winners_match_full_stable_sort",
            "priority_deadline_fifo_order",
        ],
        "G37-HP-038" => &[
            "newest_results_match_full_reference",
            "equal_time_order_deterministic",
            "assembled_traces_exact",
        ],
        "G37-HP-039" => &[
            "conflict_outcomes_match_scan_reference",
            "rollback_preserves_table",
        ],
        "G37-HP-040" => &[
            "warm_lookup_matches_linear_reference",
            "invalid_schema_rejected",
        ],
        "G37-HP-041" => &[
            "fanout_exactly_once",
            "reentrant_subscribe_no_deadlock",
            "slow_sink_does_not_hold_registry_lock",
        ],
        "G37-HP-042" => &[
            "projection_matches_name_reference",
            "missing_reordered_duplicate_cases_checked",
        ],
        "G37-HP-043" => &[
            "wildcards_match_dynamic_reference",
            "adversarial_hash_chain_terminates",
        ],
        "G37-HP-044" => &[
            "queue_set_matches_reference",
            "first_binding_order_preserved",
        ],
        "G37-HP-045" => &["bounded_page_matches_full_order", "offset_limit_exact"],
        "G37-HP-046" => &[
            "retention_set_matches_reference",
            "deletion_order_deterministic",
        ],
        "G37-HP-047" => &["hash_set_zset_match_reference", "last_update_wins"],
        "G37-HP-048" => &["lpush_order_matches_redis", "prior_tail_preserved"],
        "G37-HP-049" => &["extensions_match_reference", "canonical_forms_stable"],
        "G37-HP-050" => &[
            "prepared_neighbors_match_reference",
            "directed_and_undirected_covered",
        ],
        "G37-HP-051" => &["similarity_prefix_matches_full_order"],
        "G37-HP-052" => &[
            "semantic_prefix_matches_full_order",
            "stale_and_nonfinite_cases_checked",
        ],
        "G37-HP-053" => &[
            "observability_prefix_matches_full_order",
            "equal_timestamp_stability",
        ],
        "G37-HP-054" => &["callsite_prefix_matches_full_set", "deduplication_exact"],
        _ => return None,
    })
}

pub(super) fn scenario_contract(scenario_id: &str) -> Option<(&'static str, Vec<&'static str>)> {
    let value = match scenario_id {
        "g37-s01-modality-streaming" => (
            "modality_streaming",
            vec![
                "G37-HP-001",
                "G37-HP-002",
                "G37-HP-003",
                "G37-HP-004",
                "G37-HP-005",
            ],
        ),
        "g37-s02-analytics-restart-claim" => {
            ("analytics_restart_claim", vec!["G37-HP-006", "G37-HP-007"])
        }
        "g37-s03-tms-retirement" => ("tms_retirement", vec!["G37-HP-008"]),
        "g37-s04-generated-by-reconciliation" => {
            ("generated_by_reconciliation", vec!["G37-HP-009"])
        }
        "g37-s05-scheduler-placement-quota" => (
            "scheduler_placement_quota",
            vec!["G37-HP-010", "G37-HP-011", "G37-HP-036"],
        ),
        "g37-s06-result-cache" => ("result_cache", vec!["G37-HP-012", "G37-HP-013"]),
        "g37-s07-promql-predecessor" => ("promql_predecessor", vec!["G37-HP-014"]),
        "g37-s08-edge-cardinality-delete" => (
            "edge_cardinality_delete",
            vec!["G37-HP-015", "G37-HP-016", "G37-HP-017"],
        ),
        "g37-s09-parallel-edge-removal" => ("parallel_edge_removal", vec!["G37-HP-018"]),
        "g37-s10-subgraph-property-postings" => (
            "subgraph_property_postings",
            vec!["G37-HP-019", "G37-HP-020"],
        ),
        "g37-s11-keyset-cache-invalidation" => (
            "keyset_cache_invalidation",
            vec!["G37-HP-021", "G37-HP-022"],
        ),
        "g37-s12-flat-vector-directory" => ("flat_vector_directory", vec!["G37-HP-023"]),
        "g37-s13-tsdb-append-range-bucket" => (
            "tsdb_append_range_bucket",
            vec!["G37-HP-024", "G37-HP-025", "G37-HP-026"],
        ),
        "g37-s14-sensor-fusion" => ("sensor_fusion", vec!["G37-HP-027"]),
        "g37-s15-flat-exact-vector" => ("flat_exact_vector", vec!["G37-HP-028"]),
        "g37-s16-ivfpq-selection" => ("ivfpq_selection", vec!["G37-HP-029"]),
        "g37-s17-hnsw-selection" => ("hnsw_selection", vec!["G37-HP-030", "G37-HP-031"]),
        "g37-s18-leanrag-ranking" => ("leanrag_ranking", vec!["G37-HP-032"]),
        "g37-s19-redb-recovery-edge-ordinal" => (
            "redb_recovery_edge_ordinal",
            vec!["G37-HP-033", "G37-HP-037"],
        ),
        "g37-s20-mutation-batch" => ("mutation_batch", vec!["G37-HP-034"]),
        "g37-s21-qos-admission" => ("qos_admission", vec!["G37-HP-035"]),
        "g37-s22-trace-index-search" => ("trace_index_search", vec!["G37-HP-038"]),
        "g37-s23-sql-conflict-schema" => ("sql_conflict_schema", vec!["G37-HP-039", "G37-HP-040"]),
        "g37-s24-change-notification" => ("change_notification", vec!["G37-HP-041"]),
        "g37-s25-knowledge-batch" => ("knowledge_batch", vec!["G37-HP-042"]),
        "g37-s26-broker-topic-route" => ("broker_topic_route", vec!["G37-HP-043", "G37-HP-044"]),
        "g37-s27-appendlog-retention" => ("appendlog_retention", vec!["G37-HP-045", "G37-HP-046"]),
        "g37-s28-redis-collections" => ("redis_collections", vec!["G37-HP-047", "G37-HP-048"]),
        "g37-s29-mining-similarity-semantic" => (
            "mining_similarity_semantic",
            vec!["G37-HP-049", "G37-HP-050", "G37-HP-051", "G37-HP-052"],
        ),
        "g37-s30-observability-symbol" => {
            ("observability_symbol", vec!["G37-HP-053", "G37-HP-054"])
        }
        _ => return None,
    };
    Some(value)
}

pub(super) fn probe_row(
    row_id: &str,
    scale: usize,
    seed: u64,
    repetition: usize,
    probe_root: &Path,
) -> Result<Observation, ProbeError> {
    match row_id {
        "G37-HP-001" | "G37-HP-002" | "G37-HP-003" | "G37-HP-004" | "G37-HP-005" => {
            super::modality::probe_modality_kernel(row_id, scale)
        }
        "G37-HP-006" | "G37-HP-007" => {
            super::storage::probe_analytics(row_id, scale, seed, repetition, probe_root)
        }
        "G37-HP-008" => super::storage::probe_tms(scale),
        "G37-HP-009" => super::storage::probe_generated_by(scale),
        "G37-HP-010" | "G37-HP-011" | "G37-HP-036" => {
            super::storage::probe_scheduler(row_id, scale)
        }
        "G37-HP-012" | "G37-HP-013" => super::storage::probe_result_cache(row_id, scale),
        "G37-HP-014" => super::query::probe_promql(scale),
        "G37-HP-015" | "G37-HP-016" | "G37-HP-017" | "G37-HP-018" | "G37-HP-019" | "G37-HP-020"
        | "G37-HP-021" | "G37-HP-022" => super::storage::probe_graph(row_id, scale),
        "G37-HP-023" | "G37-HP-028" => super::query::probe_flat_vector(row_id, scale),
        "G37-HP-024" | "G37-HP-025" | "G37-HP-026" | "G37-HP-027" => {
            super::query::probe_time(row_id, scale)
        }
        "G37-HP-029" => super::query::probe_ivfpq(scale, seed),
        "G37-HP-030" | "G37-HP-031" => super::query::probe_hnsw(row_id, scale, seed),
        "G37-HP-032" => super::query::probe_rank_selection(scale),
        "G37-HP-033" | "G37-HP-037" => {
            super::storage::probe_recovery_ordinal(row_id, scale, seed, repetition, probe_root)
        }
        "G37-HP-034" => super::storage::probe_mutation_batch(scale),
        "G37-HP-035" => super::storage::probe_qos(scale),
        "G37-HP-038" => super::analytics::probe_traces(scale),
        "G37-HP-039" | "G37-HP-040" => super::query::probe_sql_kernel(row_id, scale),
        "G37-HP-041" => super::wire::probe_notifications(scale),
        "G37-HP-042" => super::analytics::probe_knowledge_projection(scale),
        "G37-HP-043" | "G37-HP-044" => super::wire::probe_broker(row_id, scale),
        "G37-HP-045" | "G37-HP-046" => super::wire::probe_appendlog(row_id, scale),
        "G37-HP-047" | "G37-HP-048" => super::wire::probe_redis_kernel(row_id, scale),
        "G37-HP-049" | "G37-HP-050" | "G37-HP-051" | "G37-HP-052" => {
            super::analytics::probe_mining_similarity(row_id, scale)
        }
        "G37-HP-053" | "G37-HP-054" => super::analytics::probe_observability_symbol(row_id, scale),
        _ => Err("unknown exact performance ledger row".into()),
    }
}
