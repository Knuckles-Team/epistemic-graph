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

type RowScaleRunner = fn(&str, usize) -> Result<Observation, ProbeError>;
type ScaleRunner = fn(usize) -> Result<Observation, ProbeError>;
type FullContextRunner = fn(&str, usize, u64, usize, &Path) -> Result<Observation, ProbeError>;
type ScaleSeedRunner = fn(usize, u64) -> Result<Observation, ProbeError>;
type RowScaleSeedRunner = fn(&str, usize, u64) -> Result<Observation, ProbeError>;

#[derive(Clone, Copy)]
enum ProbeRunner {
    RowScale(RowScaleRunner),
    Scale(ScaleRunner),
    FullContext(FullContextRunner),
    ScaleSeed(ScaleSeedRunner),
    RowScaleSeed(RowScaleSeedRunner),
}

impl ProbeRunner {
    fn run(
        self,
        row_id: &str,
        scale: usize,
        seed: u64,
        repetition: usize,
        probe_root: &Path,
    ) -> Result<Observation, ProbeError> {
        match self {
            Self::RowScale(run) => run(row_id, scale),
            Self::Scale(run) => run(scale),
            Self::FullContext(run) => run(row_id, scale, seed, repetition, probe_root),
            Self::ScaleSeed(run) => run(scale, seed),
            Self::RowScaleSeed(run) => run(row_id, scale, seed),
        }
    }
}

struct RowContract {
    id: &'static str,
    equivalence: &'static [&'static str],
    runner: ProbeRunner,
}

static ROW_CONTRACTS: &[RowContract] = &[
    RowContract {
        id: "G37-HP-001",
        equivalence: &["stream_matches_sequential", "rollback_restores_snapshot"],
        runner: ProbeRunner::RowScale(super::modality::probe_modality_kernel),
    },
    RowContract {
        id: "G37-HP-002",
        equivalence: &["cursor_page_matches_reference"],
        runner: ProbeRunner::RowScale(super::modality::probe_modality_kernel),
    },
    RowContract {
        id: "G37-HP-003",
        equivalence: &["cdc_suffix_matches_reference"],
        runner: ProbeRunner::RowScale(super::modality::probe_modality_kernel),
    },
    RowContract {
        id: "G37-HP-004",
        equivalence: &["snapshot_roundtrip", "idempotency_outcomes_preserved"],
        runner: ProbeRunner::RowScale(super::modality::probe_modality_kernel),
    },
    RowContract {
        id: "G37-HP-005",
        equivalence: &["active_count_matches_scan"],
        runner: ProbeRunner::RowScale(super::modality::probe_modality_kernel),
    },
    RowContract {
        id: "G37-HP-006",
        equivalence: &["restart_next_id_monotonic", "scheduler_index_marker_valid"],
        runner: ProbeRunner::FullContext(super::storage::probe_analytics),
    },
    RowContract {
        id: "G37-HP-007",
        equivalence: &["repeated_claim_same_fence", "worker_index_matches_job"],
        runner: ProbeRunner::FullContext(super::storage::probe_analytics),
    },
    RowContract {
        id: "G37-HP-008",
        equivalence: &[
            "retired_generator_changes_exact_closure",
            "unrelated_materializations_unchanged",
        ],
        runner: ProbeRunner::Scale(super::storage::probe_tms),
    },
    RowContract {
        id: "G37-HP-009",
        equivalence: &[
            "canonical_target_order_independent",
            "mixed_provenance_ignored",
        ],
        runner: ProbeRunner::Scale(super::storage::probe_generated_by),
    },
    RowContract {
        id: "G37-HP-010",
        equivalence: &[
            "selected_job_matches_full_reference",
            "unsatisfied_anchors_not_decoded",
        ],
        runner: ProbeRunner::RowScale(super::storage::probe_scheduler),
    },
    RowContract {
        id: "G37-HP-011",
        equivalence: &["tenant_counters_match_active_jobs"],
        runner: ProbeRunner::RowScale(super::storage::probe_scheduler),
    },
    RowContract {
        id: "G37-HP-036",
        equivalence: &[
            "placement_match_equivalent",
            "candidate_loop_allocation_free",
        ],
        runner: ProbeRunner::RowScale(super::storage::probe_scheduler),
    },
    RowContract {
        id: "G37-HP-012",
        equivalence: &["lru_victim_matches_reference", "capacity_never_exceeded"],
        runner: ProbeRunner::RowScale(super::storage::probe_result_cache),
    },
    RowContract {
        id: "G37-HP-013",
        equivalence: &["hit_payload_exact", "payload_copy_outside_lock"],
        runner: ProbeRunner::RowScale(super::storage::probe_result_cache),
    },
    RowContract {
        id: "G37-HP-014",
        equivalence: &[
            "tail_matches_binary_search_reference",
            "defensive_wide_slice_matches",
        ],
        runner: ProbeRunner::Scale(super::query::probe_promql),
    },
    RowContract {
        id: "G37-HP-015",
        equivalence: &["edge_count_matches_enumeration", "parallel_edges_counted"],
        runner: ProbeRunner::RowScale(super::storage::probe_graph),
    },
    RowContract {
        id: "G37-HP-016",
        equivalence: &[
            "logical_delete_matches_reference",
            "unrelated_edges_preserved",
        ],
        runner: ProbeRunner::RowScale(super::storage::probe_graph),
    },
    RowContract {
        id: "G37-HP-017",
        equivalence: &["resident_evict_matches_reference", "logical_rows_preserved"],
        runner: ProbeRunner::RowScale(super::storage::probe_graph),
    },
    RowContract {
        id: "G37-HP-018",
        equivalence: &["all_parallel_rows_removed_once", "adjacency_consistent"],
        runner: ProbeRunner::RowScale(super::storage::probe_graph),
    },
    RowContract {
        id: "G37-HP-019",
        equivalence: &[
            "induced_nodes_edges_match_reference",
            "parallel_rows_preserved",
        ],
        runner: ProbeRunner::RowScale(super::storage::probe_graph),
    },
    RowContract {
        id: "G37-HP-020",
        equivalence: &[
            "posting_intersection_matches_hash_reference",
            "output_sorted_unique",
        ],
        runner: ProbeRunner::RowScale(super::storage::probe_graph),
    },
    RowContract {
        id: "G37-HP-021",
        equivalence: &[
            "warm_page_matches_cold_reference",
            "cursor_has_no_duplicates",
        ],
        runner: ProbeRunner::RowScale(super::storage::probe_graph),
    },
    RowContract {
        id: "G37-HP-022",
        equivalence: &[
            "edge_write_retains_node_caches",
            "field_write_invalidates_covering_only",
        ],
        runner: ProbeRunner::RowScale(super::storage::probe_graph),
    },
    RowContract {
        id: "G37-HP-023",
        equivalence: &[
            "lookup_delete_matches_scan_reference",
            "rerank_matches_full_sort",
        ],
        runner: ProbeRunner::RowScale(super::query::probe_flat_vector),
    },
    RowContract {
        id: "G37-HP-024",
        equivalence: &[
            "reverse_append_matches_sorted_reference",
            "equal_timestamp_order_preserved",
        ],
        runner: ProbeRunner::RowScale(super::query::probe_time),
    },
    RowContract {
        id: "G37-HP-025",
        equivalence: &[
            "bounded_range_matches_filter_reference",
            "future_chunks_not_examined",
        ],
        runner: ProbeRunner::RowScale(super::query::probe_time),
    },
    RowContract {
        id: "G37-HP-026",
        equivalence: &["all_aggregates_match_reference", "constant_scratch_bound"],
        runner: ProbeRunner::RowScale(super::query::probe_time),
    },
    RowContract {
        id: "G37-HP-027",
        equivalence: &[
            "fusion_matches_sort_asof_reference",
            "output_clock_sorted_unique",
        ],
        runner: ProbeRunner::RowScale(super::query::probe_time),
    },
    RowContract {
        id: "G37-HP-028",
        equivalence: &["topk_matches_full_total_order", "nan_last_order_preserved"],
        runner: ProbeRunner::RowScale(super::query::probe_flat_vector),
    },
    RowContract {
        id: "G37-HP-029",
        equivalence: &[
            "selected_prefix_matches_full_total_order",
            "adc_and_refined_modes_covered",
        ],
        runner: ProbeRunner::ScaleSeed(super::query::probe_ivfpq),
    },
    RowContract {
        id: "G37-HP-030",
        equivalence: &[
            "neighbor_prefix_matches_full_order",
            "result_prefix_matches_full_order",
        ],
        runner: ProbeRunner::RowScaleSeed(super::query::probe_hnsw),
    },
    RowContract {
        id: "G37-HP-031",
        equivalence: &[
            "small_set_exact_matches_reference",
            "cosine_order_deterministic",
        ],
        runner: ProbeRunner::RowScaleSeed(super::query::probe_hnsw),
    },
    RowContract {
        id: "G37-HP-032",
        equivalence: &["bounded_prefix_matches_full_order", "leaf_budget_respected"],
        runner: ProbeRunner::Scale(super::query::probe_rank_selection),
    },
    RowContract {
        id: "G37-HP-033",
        equivalence: &[
            "paged_recovery_matches_full_scan",
            "composite_cursor_no_revisit",
        ],
        runner: ProbeRunner::FullContext(super::storage::probe_recovery_ordinal),
    },
    RowContract {
        id: "G37-HP-037",
        equivalence: &[
            "cold_seed_returns_next_ordinal",
            "cache_invalidation_scope_exact",
        ],
        runner: ProbeRunner::FullContext(super::storage::probe_recovery_ordinal),
    },
    RowContract {
        id: "G37-HP-034",
        equivalence: &[
            "delta_commit_matches_full_reference",
            "replay_idempotent",
            "untouched_rows_byte_stable",
        ],
        runner: ProbeRunner::Scale(super::storage::probe_mutation_batch),
    },
    RowContract {
        id: "G37-HP-035",
        equivalence: &[
            "winners_match_full_stable_sort",
            "priority_deadline_fifo_order",
        ],
        runner: ProbeRunner::Scale(super::storage::probe_qos),
    },
    RowContract {
        id: "G37-HP-038",
        equivalence: &[
            "newest_results_match_full_reference",
            "equal_time_order_deterministic",
            "assembled_traces_exact",
        ],
        runner: ProbeRunner::Scale(super::analytics::probe_traces),
    },
    RowContract {
        id: "G37-HP-039",
        equivalence: &[
            "conflict_outcomes_match_scan_reference",
            "rollback_preserves_table",
        ],
        runner: ProbeRunner::RowScale(super::query::probe_sql_kernel),
    },
    RowContract {
        id: "G37-HP-040",
        equivalence: &[
            "warm_lookup_matches_linear_reference",
            "invalid_schema_rejected",
        ],
        runner: ProbeRunner::RowScale(super::query::probe_sql_kernel),
    },
    RowContract {
        id: "G37-HP-041",
        equivalence: &[
            "fanout_exactly_once",
            "reentrant_subscribe_no_deadlock",
            "slow_sink_does_not_hold_registry_lock",
        ],
        runner: ProbeRunner::Scale(super::wire::probe_notifications),
    },
    RowContract {
        id: "G37-HP-042",
        equivalence: &[
            "projection_matches_name_reference",
            "missing_reordered_duplicate_cases_checked",
        ],
        runner: ProbeRunner::Scale(super::analytics::probe_knowledge_projection),
    },
    RowContract {
        id: "G37-HP-043",
        equivalence: &[
            "wildcards_match_dynamic_reference",
            "adversarial_hash_chain_terminates",
        ],
        runner: ProbeRunner::RowScale(super::wire::probe_broker),
    },
    RowContract {
        id: "G37-HP-044",
        equivalence: &[
            "queue_set_matches_reference",
            "first_binding_order_preserved",
        ],
        runner: ProbeRunner::RowScale(super::wire::probe_broker),
    },
    RowContract {
        id: "G37-HP-045",
        equivalence: &["bounded_page_matches_full_order", "offset_limit_exact"],
        runner: ProbeRunner::RowScale(super::wire::probe_appendlog),
    },
    RowContract {
        id: "G37-HP-046",
        equivalence: &[
            "retention_set_matches_reference",
            "deletion_order_deterministic",
        ],
        runner: ProbeRunner::RowScale(super::wire::probe_appendlog),
    },
    RowContract {
        id: "G37-HP-047",
        equivalence: &["hash_set_zset_match_reference", "last_update_wins"],
        runner: ProbeRunner::RowScale(super::wire::probe_redis_kernel),
    },
    RowContract {
        id: "G37-HP-048",
        equivalence: &["lpush_order_matches_redis", "prior_tail_preserved"],
        runner: ProbeRunner::RowScale(super::wire::probe_redis_kernel),
    },
    RowContract {
        id: "G37-HP-049",
        equivalence: &["extensions_match_reference", "canonical_forms_stable"],
        runner: ProbeRunner::RowScale(super::analytics::probe_mining_similarity),
    },
    RowContract {
        id: "G37-HP-050",
        equivalence: &[
            "prepared_neighbors_match_reference",
            "directed_and_undirected_covered",
        ],
        runner: ProbeRunner::RowScale(super::analytics::probe_mining_similarity),
    },
    RowContract {
        id: "G37-HP-051",
        equivalence: &["similarity_prefix_matches_full_order"],
        runner: ProbeRunner::RowScale(super::analytics::probe_mining_similarity),
    },
    RowContract {
        id: "G37-HP-052",
        equivalence: &[
            "semantic_prefix_matches_full_order",
            "stale_and_nonfinite_cases_checked",
        ],
        runner: ProbeRunner::RowScale(super::analytics::probe_mining_similarity),
    },
    RowContract {
        id: "G37-HP-053",
        equivalence: &[
            "observability_prefix_matches_full_order",
            "equal_timestamp_stability",
        ],
        runner: ProbeRunner::RowScale(super::analytics::probe_observability_symbol),
    },
    RowContract {
        id: "G37-HP-054",
        equivalence: &["callsite_prefix_matches_full_set", "deduplication_exact"],
        runner: ProbeRunner::RowScale(super::analytics::probe_observability_symbol),
    },
];

fn row_contract(row_id: &str) -> Option<&'static RowContract> {
    ROW_CONTRACTS.iter().find(|contract| contract.id == row_id)
}

pub(super) fn row_equivalence_contract(row_id: &str) -> Option<&'static [&'static str]> {
    row_contract(row_id).map(|contract| contract.equivalence)
}

struct ScenarioContract {
    id: &'static str,
    driver: &'static str,
    rows: &'static [&'static str],
}

static SCENARIO_CONTRACTS: &[ScenarioContract] = &[
    ScenarioContract {
        id: "g37-s01-modality-streaming",
        driver: "modality_streaming",
        rows: &[
            "G37-HP-001",
            "G37-HP-002",
            "G37-HP-003",
            "G37-HP-004",
            "G37-HP-005",
        ],
    },
    ScenarioContract {
        id: "g37-s02-analytics-restart-claim",
        driver: "analytics_restart_claim",
        rows: &["G37-HP-006", "G37-HP-007"],
    },
    ScenarioContract {
        id: "g37-s03-tms-retirement",
        driver: "tms_retirement",
        rows: &["G37-HP-008"],
    },
    ScenarioContract {
        id: "g37-s04-generated-by-reconciliation",
        driver: "generated_by_reconciliation",
        rows: &["G37-HP-009"],
    },
    ScenarioContract {
        id: "g37-s05-scheduler-placement-quota",
        driver: "scheduler_placement_quota",
        rows: &["G37-HP-010", "G37-HP-011", "G37-HP-036"],
    },
    ScenarioContract {
        id: "g37-s06-result-cache",
        driver: "result_cache",
        rows: &["G37-HP-012", "G37-HP-013"],
    },
    ScenarioContract {
        id: "g37-s07-promql-predecessor",
        driver: "promql_predecessor",
        rows: &["G37-HP-014"],
    },
    ScenarioContract {
        id: "g37-s08-edge-cardinality-delete",
        driver: "edge_cardinality_delete",
        rows: &["G37-HP-015", "G37-HP-016", "G37-HP-017"],
    },
    ScenarioContract {
        id: "g37-s09-parallel-edge-removal",
        driver: "parallel_edge_removal",
        rows: &["G37-HP-018"],
    },
    ScenarioContract {
        id: "g37-s10-subgraph-property-postings",
        driver: "subgraph_property_postings",
        rows: &["G37-HP-019", "G37-HP-020"],
    },
    ScenarioContract {
        id: "g37-s11-keyset-cache-invalidation",
        driver: "keyset_cache_invalidation",
        rows: &["G37-HP-021", "G37-HP-022"],
    },
    ScenarioContract {
        id: "g37-s12-flat-vector-directory",
        driver: "flat_vector_directory",
        rows: &["G37-HP-023"],
    },
    ScenarioContract {
        id: "g37-s13-tsdb-append-range-bucket",
        driver: "tsdb_append_range_bucket",
        rows: &["G37-HP-024", "G37-HP-025", "G37-HP-026"],
    },
    ScenarioContract {
        id: "g37-s14-sensor-fusion",
        driver: "sensor_fusion",
        rows: &["G37-HP-027"],
    },
    ScenarioContract {
        id: "g37-s15-flat-exact-vector",
        driver: "flat_exact_vector",
        rows: &["G37-HP-028"],
    },
    ScenarioContract {
        id: "g37-s16-ivfpq-selection",
        driver: "ivfpq_selection",
        rows: &["G37-HP-029"],
    },
    ScenarioContract {
        id: "g37-s17-hnsw-selection",
        driver: "hnsw_selection",
        rows: &["G37-HP-030", "G37-HP-031"],
    },
    ScenarioContract {
        id: "g37-s18-leanrag-ranking",
        driver: "leanrag_ranking",
        rows: &["G37-HP-032"],
    },
    ScenarioContract {
        id: "g37-s19-redb-recovery-edge-ordinal",
        driver: "redb_recovery_edge_ordinal",
        rows: &["G37-HP-033", "G37-HP-037"],
    },
    ScenarioContract {
        id: "g37-s20-mutation-batch",
        driver: "mutation_batch",
        rows: &["G37-HP-034"],
    },
    ScenarioContract {
        id: "g37-s21-qos-admission",
        driver: "qos_admission",
        rows: &["G37-HP-035"],
    },
    ScenarioContract {
        id: "g37-s22-trace-index-search",
        driver: "trace_index_search",
        rows: &["G37-HP-038"],
    },
    ScenarioContract {
        id: "g37-s23-sql-conflict-schema",
        driver: "sql_conflict_schema",
        rows: &["G37-HP-039", "G37-HP-040"],
    },
    ScenarioContract {
        id: "g37-s24-change-notification",
        driver: "change_notification",
        rows: &["G37-HP-041"],
    },
    ScenarioContract {
        id: "g37-s25-knowledge-batch",
        driver: "knowledge_batch",
        rows: &["G37-HP-042"],
    },
    ScenarioContract {
        id: "g37-s26-broker-topic-route",
        driver: "broker_topic_route",
        rows: &["G37-HP-043", "G37-HP-044"],
    },
    ScenarioContract {
        id: "g37-s27-appendlog-retention",
        driver: "appendlog_retention",
        rows: &["G37-HP-045", "G37-HP-046"],
    },
    ScenarioContract {
        id: "g37-s28-redis-collections",
        driver: "redis_collections",
        rows: &["G37-HP-047", "G37-HP-048"],
    },
    ScenarioContract {
        id: "g37-s29-mining-similarity-semantic",
        driver: "mining_similarity_semantic",
        rows: &["G37-HP-049", "G37-HP-050", "G37-HP-051", "G37-HP-052"],
    },
    ScenarioContract {
        id: "g37-s30-observability-symbol",
        driver: "observability_symbol",
        rows: &["G37-HP-053", "G37-HP-054"],
    },
];

pub(super) fn scenario_contract(scenario_id: &str) -> Option<(&'static str, Vec<&'static str>)> {
    SCENARIO_CONTRACTS
        .iter()
        .find(|contract| contract.id == scenario_id)
        .map(|contract| (contract.driver, contract.rows.to_vec()))
}

pub(super) fn probe_row(
    row_id: &str,
    scale: usize,
    seed: u64,
    repetition: usize,
    probe_root: &Path,
) -> Result<Observation, ProbeError> {
    let Some(contract) = row_contract(row_id) else {
        return Err("unknown exact performance ledger row".into());
    };
    contract
        .runner
        .run(row_id, scale, seed, repetition, probe_root)
}
