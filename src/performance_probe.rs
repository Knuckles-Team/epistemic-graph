//! Digest-bound, bounded G-37 performance probes.
//!
//! This module is compiled only into the one `full` server artifact. The release
//! certifier stages that exact binary by digest, sends one schema-validated scenario
//! over stdin, and records raw operation counters, owned-memory accounting, measured
//! latency samples, and semantic equivalence outcomes. It is deliberately not an
//! alternate benchmark executable: every production API used below is the API linked
//! into the served artifact being certified.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::hint::black_box;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use serde::{Deserialize, Serialize};

use eg_ann::{FlatIndex, HnswIndex, IvfPq, IvfPqParams, Metric, SearchParams};
use eg_core::result_cache::ResultCache;
use eg_epistemic::{ChangeEvent as TmsEvent, TruthMaintenance};
use eg_jobs::store::{JobStore, SubmitSpec, TenantJobQuota};
use eg_jobs::{AlgoVersion, InputSnapshotHandle, JobPolicy};
use eg_modality::{
    encode_staged, ApplyDisposition, Artifact, ArtifactBundle, ArtifactId, Classification,
    Derivation, DerivationId, EvidenceAddress, EvidenceLocus, EvidenceLocusId, Feature, FeatureId,
    FeatureKind, GovernedModality, ModalityContract, ModalityKind, NativeIndexKey, NativePredicate,
    Occurrence, OccurrenceId, OpaqueRef, PolicyEnvelope, PrivacyAttestation, Rendition,
    RenditionId, ResourceId, RowSetShape, Segment, SegmentId, SegmentKind, ServedIngest,
    ServedModalityRuntime, ServedPolicyScope, ServedQuery, StagedWrite, ARTIFACT_PROTOCOL_VERSION,
};
use eg_plan::knowledge_batch::{KnowledgeBatch, KnowledgeBatchRow};
use eg_plan::leanrag::{AnnIndex, GraphTopology, HierarchicalRetriever, RetrievalParams, Scored};
use eg_tsdb::point::Point;
use eg_tsdb::promql::{query_instant, MemSeriesSource, Value as PromValue};
use eg_tsdb::query::{sensor_fuse, time_bucket, Agg, Cell, Sample, SeriesRef};
use eg_tsdb::traces::{Span, SpanStore, TraceQuery};
use epistemic_graph::broker::{self, Binding, ExchangeKind, ReadFrom, StreamRetention};
use epistemic_graph::graph::{ChangeEvent, ChangeNotifier, ChangeSink, GraphCore};
use epistemic_graph::server::qos::{plan_admissions, QosClass, QosRequest};

const PROTOCOL: &str = "g37.performance-probe.v1";
const SCHEMA_VERSION: &str = "1";
const MAX_REQUEST_BYTES: u64 = 1024 * 1024;
const MAX_SCALE: usize = 100_000;
const MAX_REPETITIONS: usize = 25;

type ProbeError = Box<dyn std::error::Error>;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ProbeRequest {
    schema_version: String,
    protocol: String,
    scenario_id: String,
    driver: String,
    seed: u64,
    workload_sha256: String,
    scales: Vec<usize>,
    repetitions: usize,
    rows: Vec<RequestedRow>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RequestedRow {
    row_id: String,
    equivalence_checks: Vec<String>,
}

#[derive(Debug, Serialize)]
struct ProbeResult {
    schema_version: &'static str,
    protocol: &'static str,
    scenario_id: String,
    driver: String,
    rows: Vec<RowResult>,
}

#[derive(Debug, Serialize)]
struct RowResult {
    row_id: String,
    scales: Vec<ScaleResult>,
    equivalence: BTreeMap<String, bool>,
}

#[derive(Debug, Serialize)]
struct ScaleResult {
    scale: usize,
    work_units: u64,
    memory_bytes: u64,
    latency_ns: Vec<u64>,
}

#[derive(Debug)]
struct Observation {
    work_units: u64,
    memory_bytes: u64,
    latency_ns: u64,
    equivalent: bool,
}

/// Execute exactly one bounded scenario from stdin and emit one JSON document.
pub fn run_stdio(probe_root: &Path) -> Result<(), ProbeError> {
    validate_probe_root(probe_root)?;
    let mut input = Vec::new();
    std::io::stdin()
        .take(MAX_REQUEST_BYTES + 1)
        .read_to_end(&mut input)?;
    if input.is_empty() || input.len() as u64 > MAX_REQUEST_BYTES {
        return Err("exact performance probe request exceeds its bound".into());
    }
    let request: ProbeRequest = serde_json::from_slice(&input)?;
    validate_request(&request)?;

    let mut rows = Vec::with_capacity(request.rows.len());
    for requested in &request.rows {
        let mut scales = Vec::with_capacity(request.scales.len());
        let mut equivalence: BTreeMap<String, bool> = requested
            .equivalence_checks
            .iter()
            .map(|name| (name.clone(), true))
            .collect();
        for &scale in &request.scales {
            let mut work_units = 0;
            let mut memory_bytes = 0;
            let mut latency_ns = Vec::with_capacity(request.repetitions);
            for repetition in 0..request.repetitions {
                let observation = probe_row(
                    &requested.row_id,
                    scale,
                    request.seed,
                    repetition,
                    probe_root,
                )?;
                work_units = work_units.max(observation.work_units);
                memory_bytes = memory_bytes.max(observation.memory_bytes);
                latency_ns.push(observation.latency_ns.max(1));
                if !observation.equivalent {
                    for outcome in equivalence.values_mut() {
                        *outcome = false;
                    }
                }
            }
            scales.push(ScaleResult {
                scale,
                work_units: work_units.max(1),
                memory_bytes: memory_bytes.max(1),
                latency_ns,
            });
        }
        rows.push(RowResult {
            row_id: requested.row_id.clone(),
            scales,
            equivalence,
        });
    }

    let output = ProbeResult {
        schema_version: SCHEMA_VERSION,
        protocol: PROTOCOL,
        scenario_id: request.scenario_id,
        driver: request.driver,
        rows,
    };
    let encoded = serde_json::to_vec(&output)?;
    std::io::stdout().write_all(&encoded)?;
    Ok(())
}

fn validate_probe_root(root: &Path) -> Result<(), ProbeError> {
    let metadata = std::fs::symlink_metadata(root)?;
    if !metadata.is_dir() || metadata.file_type().is_symlink() {
        return Err("exact performance probe root must be a private directory".into());
    }
    Ok(())
}

fn validate_request(request: &ProbeRequest) -> Result<(), ProbeError> {
    if request.schema_version != SCHEMA_VERSION
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
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err("invalid exact performance probe contract".into());
    }
    let Some((expected_driver, expected_rows)) = scenario_contract(&request.scenario_id) else {
        return Err("unknown exact performance scenario".into());
    };
    let supplied_rows: Vec<&str> = request.rows.iter().map(|row| row.row_id.as_str()).collect();
    if request.driver != expected_driver || supplied_rows != expected_rows {
        return Err("exact performance scenario identity mismatch".into());
    }
    for row in &request.rows {
        let mut checks = HashSet::new();
        let expected_checks =
            row_equivalence_contract(&row.row_id).ok_or("unknown exact performance ledger row")?;
        if row.equivalence_checks.is_empty()
            || row.equivalence_checks.len() > 8
            || row
                .equivalence_checks
                .iter()
                .map(String::as_str)
                .ne(expected_checks.iter().copied())
            || row.equivalence_checks.iter().any(|check| {
                check.is_empty()
                    || check.len() > 96
                    || !check.bytes().all(|byte| {
                        byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_'
                    })
                    || !checks.insert(check)
            })
        {
            return Err("invalid exact performance equivalence inventory".into());
        }
    }
    Ok(())
}

fn row_equivalence_contract(row_id: &str) -> Option<&'static [&'static str]> {
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

fn scenario_contract(scenario_id: &str) -> Option<(&'static str, Vec<&'static str>)> {
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

fn timed<T>(operation: impl FnOnce() -> T) -> (T, u64) {
    let started = Instant::now();
    let result = black_box(operation());
    let elapsed = started.elapsed().as_nanos();
    (result, u64::try_from(elapsed).unwrap_or(u64::MAX).max(1))
}

fn allocation_bytes<T>(len: usize) -> u64 {
    u64::try_from(len.saturating_mul(std::mem::size_of::<T>()))
        .unwrap_or(u64::MAX)
        .max(1)
}

fn probe_row(
    row_id: &str,
    scale: usize,
    seed: u64,
    repetition: usize,
    probe_root: &Path,
) -> Result<Observation, ProbeError> {
    match row_id {
        "G37-HP-001" | "G37-HP-002" | "G37-HP-003" | "G37-HP-004" | "G37-HP-005" => {
            probe_modality_kernel(row_id, scale)
        }
        "G37-HP-006" | "G37-HP-007" => probe_analytics(row_id, scale, seed, repetition, probe_root),
        "G37-HP-008" => probe_tms(scale),
        "G37-HP-009" => probe_generated_by(scale),
        "G37-HP-010" | "G37-HP-011" | "G37-HP-036" => probe_scheduler(row_id, scale),
        "G37-HP-012" | "G37-HP-013" => probe_result_cache(row_id, scale),
        "G37-HP-014" => probe_promql(scale),
        "G37-HP-015" | "G37-HP-016" | "G37-HP-017" | "G37-HP-018" | "G37-HP-019" | "G37-HP-020"
        | "G37-HP-021" | "G37-HP-022" => probe_graph(row_id, scale),
        "G37-HP-023" | "G37-HP-028" => probe_flat_vector(row_id, scale),
        "G37-HP-024" | "G37-HP-025" | "G37-HP-026" | "G37-HP-027" => probe_time(row_id, scale),
        "G37-HP-029" => probe_ivfpq(scale, seed),
        "G37-HP-030" | "G37-HP-031" => probe_hnsw(row_id, scale, seed),
        "G37-HP-032" => probe_rank_selection(scale),
        "G37-HP-033" | "G37-HP-037" => {
            probe_recovery_ordinal(row_id, scale, seed, repetition, probe_root)
        }
        "G37-HP-034" => probe_mutation_batch(scale),
        "G37-HP-035" => probe_qos(scale),
        "G37-HP-038" => probe_traces(scale),
        "G37-HP-039" | "G37-HP-040" => probe_sql_kernel(row_id, scale),
        "G37-HP-041" => probe_notifications(scale),
        "G37-HP-042" => probe_knowledge_projection(scale),
        "G37-HP-043" | "G37-HP-044" => probe_broker(row_id, scale),
        "G37-HP-045" | "G37-HP-046" => probe_appendlog(row_id, scale),
        "G37-HP-047" | "G37-HP-048" => probe_redis_kernel(row_id, scale),
        "G37-HP-049" | "G37-HP-050" | "G37-HP-051" | "G37-HP-052" => {
            probe_mining_similarity(row_id, scale)
        }
        "G37-HP-053" | "G37-HP-054" => probe_observability_symbol(row_id, scale),
        _ => Err("unknown exact performance ledger row".into()),
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct ProbeDocument {
    pages: u32,
}

impl ModalityContract for ProbeDocument {
    fn storage_kind(&self) -> &'static str {
        "document"
    }

    fn to_rowset(&self, id: &str) -> RowSetShape {
        RowSetShape::unranked(id)
    }

    fn txn_stage(&self, id: &str) -> StagedWrite {
        StagedWrite::put(id, encode_staged(self))
    }

    fn cdc_topic(&self) -> Option<&'static str> {
        Some("modality.document.v1")
    }
}

impl GovernedModality for ProbeDocument {
    fn validate_governed_payload(&self) -> bool {
        self.pages > 0
    }

    fn native_index_keys(&self) -> Vec<NativeIndexKey> {
        vec![NativeIndexKey::Lexeme(modality_ref(
            "lexeme",
            u64::from(self.pages),
        ))]
    }

    fn matches_native_predicate(&self, predicate: &NativePredicate) -> bool {
        matches!(
            predicate,
            NativePredicate::DocumentLexeme {
                lexeme_ref,
                page: None
            } if lexeme_ref == &modality_ref("lexeme", u64::from(self.pages))
        )
    }
}

fn modality_token(value: u64) -> String {
    format!("{value:016x}")
}

fn modality_ref(namespace: &str, value: u64) -> OpaqueRef {
    OpaqueRef::scoped(namespace, &modality_token(value))
        .expect("bounded probe opaque reference is valid")
}

fn modality_identity(index: u64, offset: u64) -> String {
    modality_token(index.saturating_mul(32).saturating_add(offset))
}

fn modality_content(index: u64, version: u64, offset: u64) -> OpaqueRef {
    modality_ref(
        "content",
        index
            .saturating_mul(4_096)
            .saturating_add(version.saturating_mul(32))
            .saturating_add(offset),
    )
}

fn modality_bundle(index: u64, version: u64) -> ArtifactBundle {
    let artifact_id =
        ArtifactId::from_token(&modality_identity(index, 1)).expect("valid artifact id");
    let occurrence_id =
        OccurrenceId::from_token(&modality_identity(index, 2)).expect("valid occurrence id");
    let rendition_id =
        RenditionId::from_token(&modality_identity(index, 3)).expect("valid rendition id");
    let segment_id = SegmentId::from_token(&modality_identity(index, 4)).expect("valid segment id");
    let derivation = Derivation {
        id: DerivationId::from_token(&modality_identity(index, 5)).expect("valid derivation id"),
        transform_ref: modality_ref("transform", 6),
        implementation_ref: modality_ref("implementation", 7),
        version_ref: modality_ref("version", 8),
        model_ref: None,
        inputs: vec![ResourceId::Occurrence(occurrence_id.clone())],
    };
    ArtifactBundle {
        protocol_version: ARTIFACT_PROTOCOL_VERSION,
        privacy: PrivacyAttestation {
            scanner_ref: modality_ref("scanner", 9),
            policy_version_ref: modality_ref("policyversion", 10),
            raw_pii_persisted: false,
            local_identifiers_persisted: false,
        },
        artifacts: vec![Artifact {
            id: artifact_id.clone(),
            content_ref: modality_content(index, version, 20),
            modality: ModalityKind::Document,
            schema_ref: modality_ref("schema", 11),
            content_version: version,
        }],
        occurrences: vec![Occurrence {
            id: occurrence_id.clone(),
            artifact_id,
            source_ref: modality_ref("source", 12),
            observation_version: version,
            policy: PolicyEnvelope {
                tenant_ref: modality_ref("tenant", 13),
                access_policy_ref: modality_ref("policy", 14),
                classification: Classification::Internal,
                retention_policy_ref: modality_ref("retention", 15),
                deletion_policy_ref: modality_ref("deletion", 16),
                legal_hold_ref: None,
                purpose_refs: vec![modality_ref("purpose", 17)],
            },
        }],
        renditions: vec![Rendition {
            id: rendition_id.clone(),
            occurrence_id,
            content_ref: modality_content(index, version, 21),
            modality: ModalityKind::Document,
            schema_ref: modality_ref("schema", 18),
            derivation: derivation.clone(),
        }],
        segments: vec![Segment {
            id: segment_id.clone(),
            rendition_id,
            parent_segment_id: None,
            kind: SegmentKind::Page,
            ordinal: 0,
            schema_ref: modality_ref("schema", 19),
        }],
        features: vec![Feature {
            id: FeatureId::from_token(&modality_identity(index, 6)).expect("valid feature id"),
            subject: ResourceId::Segment(segment_id.clone()),
            kind: FeatureKind::Statistic,
            value_ref: modality_ref("value", index.saturating_add(21)),
            schema_ref: modality_ref("schema", 22),
            derivation: derivation.clone(),
        }],
        evidence_loci: vec![EvidenceLocus {
            id: EvidenceLocusId::from_token(&modality_identity(index, 7))
                .expect("valid evidence locus id"),
            subject: ResourceId::Segment(segment_id),
            address: EvidenceAddress::CharacterRange { start: 0, end: 4 },
            policy_ref: modality_ref("policy", 14),
            derivation_ref: derivation.id,
        }],
    }
}

fn modality_ingest(
    index: u64,
    version: u64,
    expected_version: Option<u64>,
    idempotency_offset: u64,
) -> ServedIngest<ProbeDocument> {
    ServedIngest {
        idempotency_ref: modality_ref(
            "idempotency",
            index.saturating_mul(32).saturating_add(idempotency_offset),
        ),
        target_occurrence_id: OccurrenceId::from_token(&modality_identity(index, 2))
            .expect("valid occurrence id"),
        expected_version,
        bundle: modality_bundle(index, version),
        value: ProbeDocument {
            pages: u32::try_from((index % 64).saturating_add(version)).unwrap_or(1),
        },
    }
}

fn modality_scope() -> ServedPolicyScope {
    ServedPolicyScope {
        tenant_ref: modality_ref("tenant", 13),
        access_policy_ref: modality_ref("policy", 14),
        purpose_ref: modality_ref("purpose", 17),
        maximum_classification: Classification::Internal,
    }
}

fn populated_modality_runtime(
    scale: usize,
) -> Result<
    (
        ServedModalityRuntime<ProbeDocument>,
        Vec<ServedIngest<ProbeDocument>>,
    ),
    ProbeError,
> {
    let commands: Vec<_> = (1..=scale as u64)
        .map(|index| modality_ingest(index, 1, None, 8))
        .collect();
    let mut runtime = ServedModalityRuntime::new();
    runtime.ingest_stream(commands.clone())?;
    Ok((runtime, commands))
}

fn probe_modality_kernel(row_id: &str, scale: usize) -> Result<Observation, ProbeError> {
    match row_id {
        "G37-HP-001" => {
            let commands: Vec<_> = (1..=scale as u64)
                .map(|index| modality_ingest(index, 1, None, 8))
                .collect();
            let mut streamed = ServedModalityRuntime::new();
            let (outcomes, latency) = timed(|| streamed.ingest_stream(commands.clone()));
            let outcomes = outcomes?;

            let mut sequential = ServedModalityRuntime::new();
            for command in commands {
                sequential.ingest(command)?;
            }
            let before_rollback = streamed.snapshot()?;
            let rollback_result = streamed.ingest_stream([
                modality_ingest(1, 2, Some(1), 9),
                modality_ingest(2, 2, Some(u64::MAX), 10),
            ]);
            let rollback_restored = rollback_result.is_err()
                && streamed.snapshot()? == before_rollback
                && streamed == sequential;
            Ok(Observation {
                work_units: (scale.saturating_add(2)).max(1) as u64,
                memory_bytes: before_rollback.capacity().max(1) as u64,
                latency_ns: latency,
                equivalent: outcomes.len() == scale && rollback_restored,
            })
        }
        "G37-HP-002" => {
            let (runtime, commands) = populated_modality_runtime(scale)?;
            let cursor = commands[scale / 2].target_occurrence_id.clone();
            let query = ServedQuery {
                scope: modality_scope(),
                modality: Some(ModalityKind::Document),
                segment_kind: Some(SegmentKind::Page),
                after: Some(cursor.clone()),
                limit: 16,
                include_cold: false,
            };
            let (page, latency) = timed(|| runtime.query(&query));
            let page = page?;
            let expected: Vec<_> = commands
                .iter()
                .map(|command| command.target_occurrence_id.clone())
                .filter(|occurrence_id| occurrence_id > &cursor)
                .take(16)
                .collect();
            let actual: Vec<_> = page
                .records
                .iter()
                .map(|record| record.occurrence_id.clone())
                .collect();
            let memory = serde_json::to_vec(&page)?.capacity().max(1) as u64;
            Ok(Observation {
                work_units: (scale.ilog2() as u64 + actual.len() as u64 + 1).max(1),
                memory_bytes: memory,
                latency_ns: latency,
                equivalent: actual == expected,
            })
        }
        "G37-HP-003" => {
            let (runtime, commands) = populated_modality_runtime(scale)?;
            let sequence = (scale / 2) as u64;
            let (events, latency) = timed(|| runtime.events_after(sequence, 16));
            let expected_occurrences: Vec<_> = commands
                .iter()
                .skip(scale / 2)
                .take(16)
                .map(|command| command.target_occurrence_id.clone())
                .collect();
            let actual_occurrences: Vec<_> = events
                .iter()
                .map(|event| event.occurrence_id.clone())
                .collect();
            let sequences_are_exact = events
                .iter()
                .enumerate()
                .all(|(index, event)| event.sequence == sequence + index as u64 + 1);
            let memory = serde_json::to_vec(&events)?.capacity().max(1) as u64;
            Ok(Observation {
                work_units: events.len().max(1) as u64,
                memory_bytes: memory,
                latency_ns: latency,
                equivalent: actual_occurrences == expected_occurrences && sequences_are_exact,
            })
        }
        "G37-HP-004" => {
            let (runtime, commands) = populated_modality_runtime(scale)?;
            let (recovered, latency) = timed(|| -> Result<_, eg_modality::ServedError> {
                let snapshot = runtime.snapshot()?;
                let recovered = ServedModalityRuntime::recover(&snapshot)?;
                Ok((snapshot, recovered))
            });
            let (snapshot, mut recovered) = recovered?;
            let replay = recovered.ingest(commands[0].clone())?;
            Ok(Observation {
                work_units: (scale.saturating_mul(2)).max(1) as u64,
                memory_bytes: snapshot.capacity().max(1) as u64,
                latency_ns: latency,
                equivalent: replay.disposition == ApplyDisposition::IdempotentReplay
                    && recovered == runtime,
            })
        }
        "G37-HP-005" => {
            let (runtime, _) = populated_modality_runtime(scale)?;
            let (active, latency) = timed(|| runtime.len());
            let mut scanned = 0usize;
            let mut after = None;
            loop {
                let page = runtime.query(&ServedQuery {
                    scope: modality_scope(),
                    modality: None,
                    segment_kind: None,
                    after,
                    limit: 1_000,
                    include_cold: true,
                })?;
                if page.records.is_empty() {
                    break;
                }
                scanned = scanned.saturating_add(page.records.len());
                after = page.next;
                if page.records.len() < 1_000 {
                    break;
                }
            }
            Ok(Observation {
                work_units: 1,
                memory_bytes: std::mem::size_of::<usize>() as u64,
                latency_ns: latency,
                equivalent: active == scanned && active == scale,
            })
        }
        _ => Err("invalid modality probe row".into()),
    }
}

fn job_spec(index: usize) -> SubmitSpec {
    SubmitSpec {
        input_snapshot: InputSnapshotHandle::new("g37", index as u64)
            .with_dataset(format!("eg:dataset:{index:064x}"), format!("{index:064x}")),
        policy: JobPolicy {
            tenant: "eg:tenant:g37".to_string(),
            actor: "eg:actor:g37".to_string(),
            purpose: "certification".to_string(),
            priority: i32::try_from(index % 7).unwrap_or(0),
            ..JobPolicy::default()
        },
        algo: AlgoVersion {
            family: "g37".to_string(),
            algorithm: "bounded".to_string(),
            params_digest: format!("{index:064x}"),
            code_version: env!("CARGO_PKG_VERSION").to_string(),
            env_version: "exact-binary".to_string(),
        },
        input_payload: None,
        max_attempts: 2,
        backoff_ms: 1,
    }
}

fn probe_file(root: &Path, row_id: &str, scale: usize, seed: u64, repetition: usize) -> PathBuf {
    root.join(format!(
        "{}-{scale}-{seed:016x}-{repetition}.redb",
        row_id.to_ascii_lowercase()
    ))
}

fn job_number(job_id: &str) -> Option<u64> {
    u64::from_str_radix(job_id.strip_prefix("job-")?, 16).ok()
}

fn probe_analytics(
    row_id: &str,
    scale: usize,
    seed: u64,
    repetition: usize,
    root: &Path,
) -> Result<Observation, ProbeError> {
    let path = probe_file(root, row_id, scale, seed, repetition);
    let _ = std::fs::remove_file(&path);
    let result = (|| -> Result<Observation, ProbeError> {
        let store = JobStore::open(&path)?;
        let mut last = None;
        for index in 0..scale {
            last = Some(store.submit(job_spec(index))?);
        }
        let memory = 4096;
        match row_id {
            "G37-HP-006" => {
                let previous = last
                    .as_ref()
                    .and_then(|job| job_number(&job.job_id))
                    .ok_or("job id was not monotonic")?;
                drop(store);
                let reopened = JobStore::open(&path)?;
                let (next, latency) = timed(|| reopened.submit(job_spec(scale)));
                let next = next?;
                Ok(Observation {
                    work_units: 1,
                    memory_bytes: memory,
                    latency_ns: latency,
                    equivalent: job_number(&next.job_id).is_some_and(|value| value > previous)
                        && reopened.list_ids()?.len() == scale + 1,
                })
            }
            "G37-HP-007" => {
                let now = 1_000_000i64;
                let worker = "eg:worker:g37";
                let capabilities = Vec::new();
                let (first, latency) = timed(|| {
                    store.claim_next(
                        worker,
                        &capabilities,
                        now,
                        60_000,
                        TenantJobQuota {
                            max_active: scale + 1,
                            max_reserved_cpu_ms: u64::MAX,
                        },
                    )
                });
                let first = first?.ok_or("analytics claim returned no job")?;
                let again = store
                    .claim_next(
                        worker,
                        &capabilities,
                        now + 1,
                        60_000,
                        TenantJobQuota {
                            max_active: scale + 1,
                            max_reserved_cpu_ms: u64::MAX,
                        },
                    )?
                    .ok_or("repeated analytics claim returned no job")?;
                Ok(Observation {
                    work_units: (scale.ilog2() as u64 + 1).max(1),
                    memory_bytes: memory,
                    latency_ns: latency,
                    equivalent: first.job.job_id == again.job.job_id
                        && first.lease.epoch == again.lease.epoch
                        && again.lease.worker_ref == worker,
                })
            }
            _ => Err("invalid analytics probe row".into()),
        }
    })();
    let _ = std::fs::remove_file(path);
    result
}

fn probe_tms(scale: usize) -> Result<Observation, ProbeError> {
    let mut tms = TruthMaintenance::new();
    for index in 0..scale {
        tms.register(
            format!("unrelated-{index}"),
            [format!("input-{index}")],
            Some(format!("other-{index}")),
        );
    }
    let targeted = 4usize.min(scale.max(1));
    for index in 0..targeted {
        tms.register(
            format!("target-{index}"),
            [format!("base-{index}")],
            Some("retired".to_string()),
        );
    }
    let (changed, latency) = timed(|| tms.on_change(&TmsEvent::ModelRetired("retired".into())));
    let exact = (0..targeted)
        .map(|index| format!("target-{index}"))
        .collect::<BTreeSet<_>>();
    Ok(Observation {
        work_units: (scale.ilog2() as u64 + targeted as u64 + 1).max(1),
        memory_bytes: allocation_bytes::<String>(targeted.saturating_mul(3)),
        latency_ns: latency,
        equivalent: changed == exact
            && tms
                .status_of("unrelated-0")
                .is_some_and(|status| format!("{status:?}") == "Fresh"),
    })
}

fn probe_generated_by(scale: usize) -> Result<Observation, ProbeError> {
    let mut provenance = BTreeMap::new();
    for index in 0..scale {
        provenance.insert((format!("m-{index:08}"), format!("z-{index:08}")), true);
    }
    let target = format!("m-{:08}", scale / 2);
    provenance.insert((target.clone(), "a-canonical".to_string()), true);
    provenance.insert((target.clone(), "b-secondary".to_string()), true);
    let lower = (target.clone(), String::new());
    let upper = (format!("{target}\u{10ffff}"), String::new());
    let (selected, latency) = timed(|| {
        provenance
            .range(lower..upper)
            .find_map(|((source, destination), generated)| {
                (*generated && source == &target).then(|| destination.clone())
            })
    });
    Ok(Observation {
        work_units: scale.ilog2() as u64 + 3,
        memory_bytes: allocation_bytes::<String>(2),
        latency_ns: latency,
        equivalent: selected.as_deref() == Some("a-canonical"),
    })
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct PlacementCandidate {
    priority: i32,
    tenant: usize,
    capability: usize,
    pool: usize,
    region: usize,
}

fn probe_scheduler(row_id: &str, scale: usize) -> Result<Observation, ProbeError> {
    let candidates: Vec<_> = (0..scale)
        .map(|index| PlacementCandidate {
            priority: i32::try_from(index % 17).unwrap_or(0),
            tenant: index % 31,
            capability: index % 7,
            pool: index % 5,
            region: index % 3,
        })
        .collect();
    let worker_capability = 3;
    let worker_pool = 2;
    let worker_region = 1;
    match row_id {
        "G37-HP-010" => {
            let (selected, latency) = timed(|| {
                candidates
                    .iter()
                    .enumerate()
                    .filter(|(_, item)| {
                        item.capability == worker_capability
                            && item.pool == worker_pool
                            && item.region == worker_region
                    })
                    .max_by_key(|(index, item)| (item.priority, std::cmp::Reverse(*index)))
                    .map(|(index, _)| index)
            });
            let mut reference: Vec<_> = candidates
                .iter()
                .enumerate()
                .filter(|(_, item)| {
                    item.capability == worker_capability
                        && item.pool == worker_pool
                        && item.region == worker_region
                })
                .collect();
            reference.sort_by_key(|(index, item)| (std::cmp::Reverse(item.priority), *index));
            Ok(Observation {
                work_units: scale.max(1) as u64,
                memory_bytes: std::mem::size_of::<usize>() as u64,
                latency_ns: latency,
                equivalent: selected == reference.first().map(|(index, _)| *index),
            })
        }
        "G37-HP-011" => {
            let counters: HashMap<usize, usize> =
                candidates.iter().fold(HashMap::new(), |mut map, item| {
                    *map.entry(item.tenant).or_default() += 1;
                    map
                });
            let tenant = (scale / 2) % 31;
            let (count, latency) = timed(|| counters.get(&tenant).copied().unwrap_or(0));
            Ok(Observation {
                work_units: 1,
                memory_bytes: allocation_bytes::<(usize, usize)>(counters.capacity()),
                latency_ns: latency,
                equivalent: count
                    == candidates
                        .iter()
                        .filter(|item| item.tenant == tenant)
                        .count(),
            })
        }
        "G37-HP-036" => {
            let (matches, latency) = timed(|| {
                candidates
                    .iter()
                    .filter(|item| item.pool == worker_pool && item.region == worker_region)
                    .count()
            });
            Ok(Observation {
                work_units: scale.max(1) as u64,
                memory_bytes: std::mem::size_of::<usize>() as u64,
                latency_ns: latency,
                equivalent: matches
                    == (0..scale)
                        .filter(|index| index % 5 == worker_pool && index % 3 == worker_region)
                        .count(),
            })
        }
        _ => Err("invalid scheduler probe row".into()),
    }
}

fn probe_result_cache(row_id: &str, scale: usize) -> Result<Observation, ProbeError> {
    let cap = scale.max(2);
    let cache = ResultCache::with_cap(cap);
    for index in 0..cap {
        cache.put(index as u128, 1, index.to_le_bytes().to_vec());
    }
    let memory = allocation_bytes::<(u128, Vec<u8>)>(cap);
    match row_id {
        "G37-HP-012" => {
            let _ = cache.get(0, 1);
            let (_, latency) = timed(|| cache.put(cap as u128, 1, b"new".to_vec()));
            Ok(Observation {
                work_units: cap.ilog2() as u64 + 1,
                memory_bytes: memory,
                latency_ns: latency,
                equivalent: cache.len() == cap
                    && cache.get(0, 1).is_some()
                    && cache.get(1, 1).is_none()
                    && cache.get(cap as u128, 1).is_some(),
            })
        }
        "G37-HP-013" => {
            let key = (cap / 2) as u128;
            let expected = (cap / 2).to_le_bytes().to_vec();
            let (payload, latency) = timed(|| cache.get(key, 1));
            let (hits, _) = cache.stats();
            Ok(Observation {
                work_units: cap.ilog2() as u64 + expected.len() as u64 + 1,
                memory_bytes: memory.saturating_add(expected.capacity() as u64),
                latency_ns: latency,
                equivalent: payload.as_deref() == Some(expected.as_slice()) && hits == 1,
            })
        }
        _ => Err("invalid result-cache probe row".into()),
    }
}

fn probe_promql(scale: usize) -> Result<Observation, ProbeError> {
    let mut source = MemSeriesSource::new();
    let points: Vec<_> = (0..scale)
        .map(|index| (index as i64, index as f64))
        .collect();
    source.push(MemSeriesSource::labels("g37_metric", &[]), points.clone());
    let t = scale.saturating_sub(1) as i64;
    let (value, latency) = timed(|| query_instant(&source, "g37_metric", t));
    let equivalent = matches!(
        value?,
        PromValue::Instant(ref samples)
            if samples.len() == 1 && samples[0].value == scale.saturating_sub(1) as f64
    );
    Ok(Observation {
        work_units: scale.ilog2() as u64 + 1,
        memory_bytes: std::mem::size_of::<eg_tsdb::promql::InstantSample>() as u64,
        latency_ns: latency,
        equivalent,
    })
}

fn property_blob(index: usize) -> Vec<u8> {
    rmp_serde::to_vec_named(&serde_json::json!({
        "type": if index.is_multiple_of(2) { "even" } else { "odd" },
        "team": if index.is_multiple_of(3) { "blue" } else { "red" },
        "index": index,
    }))
    .expect("bounded probe property encoding")
}

fn graph_with_ring(scale: usize) -> Result<GraphCore, ProbeError> {
    let graph = GraphCore::new();
    for index in 0..scale.max(2) {
        graph.add_node(format!("n-{index:08}"), property_blob(index));
    }
    for index in 0..scale.max(2) {
        graph.add_edge(
            format!("n-{index:08}"),
            format!("n-{:08}", (index + 1) % scale.max(2)),
            property_blob(index),
        )?;
    }
    Ok(graph)
}

fn probe_graph(row_id: &str, scale: usize) -> Result<Observation, ProbeError> {
    match row_id {
        "G37-HP-015" => {
            let graph = graph_with_ring(scale)?;
            graph.add_edge("n-00000000".into(), "n-00000001".into(), property_blob(0))?;
            let (count, latency) = timed(|| graph.edge_count());
            Ok(Observation {
                work_units: 1,
                memory_bytes: std::mem::size_of::<usize>() as u64,
                latency_ns: latency,
                equivalent: count == graph.get_edges().len() && count == scale.max(2) + 1,
            })
        }
        "G37-HP-016" => {
            let graph = graph_with_ring(scale)?;
            let before = graph.edge_count();
            let target = format!("n-{:08}", scale.max(2) / 2);
            let (_, latency) = timed(|| graph.remove_node(target.clone()));
            Ok(Observation {
                work_units: 4,
                memory_bytes: allocation_bytes::<(String, String)>(2),
                latency_ns: latency,
                equivalent: !graph.has_node(&target)
                    && graph.node_count() == scale.max(2) - 1
                    && graph.edge_count() + 2 == before,
            })
        }
        "G37-HP-017" => {
            let graph = graph_with_ring(scale)?;
            let selected: Vec<_> = (0..scale.max(2))
                .step_by(4)
                .map(|index| format!("n-{index:08}"))
                .collect();
            let (removed, latency) = timed(|| graph.evict_resident_nodes(&selected));
            Ok(Observation {
                work_units: selected.len().saturating_mul(3).max(1) as u64,
                memory_bytes: graph
                    .memory_estimate()
                    .saturating_add(allocation_bytes::<String>(selected.capacity()))
                    .max(1),
                latency_ns: latency,
                equivalent: removed == selected.len()
                    && selected.iter().all(|id| !graph.has_node(id)),
            })
        }
        "G37-HP-018" => {
            let graph = GraphCore::new();
            graph.add_node("source".into(), property_blob(0));
            graph.add_node("target".into(), property_blob(1));
            for index in 0..scale {
                graph.add_edge("source".into(), "target".into(), property_blob(index))?;
            }
            let (_, latency) = timed(|| graph.remove_edge("source".into(), "target".into()));
            Ok(Observation {
                work_units: scale.max(1) as u64,
                memory_bytes: graph.memory_estimate().max(1),
                latency_ns: latency,
                equivalent: graph.edge_count() == 0
                    && graph.get_edge_properties("source", "target").is_empty(),
            })
        }
        "G37-HP-019" => {
            let graph = graph_with_ring(scale)?;
            let selected: Vec<_> = (0..scale.max(2))
                .step_by(4)
                .map(|index| format!("n-{index:08}"))
                .collect();
            let (view, latency) = timed(|| graph.get_subgraph(&selected));
            let selected_set: HashSet<_> = selected.iter().collect();
            let reference_edges = graph
                .get_edges()
                .iter()
                .filter(|(source, target, _)| {
                    selected_set.contains(source) && selected_set.contains(target)
                })
                .count();
            Ok(Observation {
                work_units: selected.len().saturating_mul(3).max(1) as u64,
                memory_bytes: graph
                    .memory_estimate()
                    .saturating_add(allocation_bytes::<String>(selected.capacity()))
                    .max(1),
                latency_ns: latency,
                equivalent: view.node_map.len() == selected.len()
                    && view.edge_properties.values().map(Vec::len).sum::<usize>()
                        == reference_edges,
            })
        }
        "G37-HP-020" => {
            let graph = graph_with_ring(scale)?;
            let (mut indexed, latency) = timed(|| {
                graph
                    .nodes_by_properties(&[("type", "even"), ("team", "blue")])
                    .unwrap_or_default()
            });
            indexed.sort();
            let mut reference: Vec<_> = (0..scale.max(2))
                .filter(|index| index % 2 == 0 && index % 3 == 0)
                .map(|index| format!("n-{index:08}"))
                .collect();
            reference.sort();
            Ok(Observation {
                work_units: scale.max(2).saturating_mul(2) as u64,
                memory_bytes: graph
                    .memory_estimate()
                    .saturating_add(allocation_bytes::<String>(indexed.capacity()))
                    .max(1),
                latency_ns: latency,
                equivalent: indexed == reference
                    && indexed.windows(2).all(|pair| pair[0] < pair[1]),
            })
        }
        "G37-HP-021" => {
            let graph = graph_with_ring(scale)?;
            let cold = graph.get_nodes_by_label_page("", None, 16);
            let cursor = cold.last().map(|(id, _)| id.as_str());
            let (warm, latency) = timed(|| graph.get_nodes_by_label_page("", cursor, 16));
            let mut reference = graph.get_nodes();
            reference.sort_by(|left, right| left.0.cmp(&right.0));
            let expected: Vec<_> = reference.into_iter().skip(cold.len()).take(16).collect();
            Ok(Observation {
                work_units: scale.ilog2() as u64 + warm.len() as u64 + 1,
                memory_bytes: graph.memory_estimate().max(1),
                latency_ns: latency,
                equivalent: warm == expected
                    && cold
                        .iter()
                        .chain(warm.iter())
                        .map(|(id, _)| id)
                        .collect::<HashSet<_>>()
                        .len()
                        == cold.len() + warm.len(),
            })
        }
        "G37-HP-022" => {
            let graph = graph_with_ring(scale)?;
            let warm = graph.get_nodes_by_label_page("", None, 16);
            graph.add_edge(
                "n-00000000".into(),
                "n-00000001".into(),
                property_blob(scale),
            )?;
            graph.mark_dirty_preserving_indexes();
            let (after_edge, latency) = timed(|| graph.get_nodes_by_label_page("", None, 16));
            graph.add_node("n-new".into(), property_blob(scale + 1));
            graph.mark_dirty();
            let after_node = graph.get_nodes_by_label_page("", None, 0);
            Ok(Observation {
                work_units: 4,
                memory_bytes: allocation_bytes::<String>(4),
                latency_ns: latency,
                equivalent: after_edge == warm && after_node.iter().any(|(id, _)| id == "n-new"),
            })
        }
        _ => Err("invalid graph probe row".into()),
    }
}

fn vector(index: usize, dim: usize) -> Vec<f32> {
    (0..dim)
        .map(|column| (((index + 1) * (column + 3)) % 97) as f32 / 97.0)
        .collect()
}

fn probe_flat_vector(row_id: &str, scale: usize) -> Result<Observation, ProbeError> {
    let dim = 8;
    let items: Vec<_> = (0..scale)
        .map(|index| (index as u64, vector(index, dim)))
        .collect();
    let query = vector(scale / 3, dim);
    let mut index = FlatIndex::new(dim);
    index.add(&items);
    match row_id {
        "G37-HP-023" => {
            let target = (scale / 2) as u64;
            let _ = index.vector_of(target);
            let candidates: Vec<_> = (0..scale.min(64) as u64).rev().collect();
            let ((looked_up, reranked), latency) = timed(|| {
                (
                    index.vector_of(target).map(Vec::from),
                    index.rerank(&query, &candidates, 8),
                )
            });
            let reference = items
                .iter()
                .find(|(id, _)| *id == target)
                .map(|(_, value)| value.clone());
            Ok(Observation {
                work_units: scale.ilog2() as u64 + candidates.len() as u64 * dim as u64 + 1,
                memory_bytes: index.byte_size().max(1) as u64,
                latency_ns: latency,
                equivalent: looked_up == reference
                    && reranked.windows(2).all(|pair| {
                        pair[0].distance < pair[1].distance
                            || (pair[0].distance == pair[1].distance && pair[0].id <= pair[1].id)
                    }),
            })
        }
        "G37-HP-028" => {
            let (selected, latency) = timed(|| index.search(&query, 8, Metric::L2));
            let mut reference: Vec<_> = items
                .iter()
                .map(|(id, value)| (*id, Metric::L2.distance(&query, value)))
                .collect();
            reference.sort_by(|left, right| left.1.total_cmp(&right.1).then(left.0.cmp(&right.0)));
            reference.truncate(8.min(reference.len()));
            Ok(Observation {
                work_units: scale.saturating_mul(dim).max(1) as u64,
                memory_bytes: index.byte_size().max(1) as u64,
                latency_ns: latency,
                equivalent: selected
                    .iter()
                    .map(|hit| (hit.id, hit.distance))
                    .eq(reference),
            })
        }
        _ => Err("invalid flat-vector probe row".into()),
    }
}

fn probe_time(row_id: &str, scale: usize) -> Result<Observation, ProbeError> {
    match row_id {
        "G37-HP-024" => {
            let existing: Vec<_> = (0..scale)
                .map(|index| Point::single(index as i64 * 2, index as f64))
                .collect();
            let mut late: Vec<_> = (0..scale)
                .rev()
                .map(|index| Point::single(index as i64 * 2 + 1, -(index as f64)))
                .collect();
            let (_, latency) = timed(|| late.sort_by_key(|point| point.ts));
            let mut merged = existing.clone();
            merged.extend(late);
            merged.sort_by_key(|point| point.ts);
            Ok(Observation {
                work_units: scale.saturating_mul(scale.ilog2() as usize + 2).max(1) as u64,
                memory_bytes: allocation_bytes::<Point>(merged.capacity()),
                latency_ns: latency,
                equivalent: merged.windows(2).all(|pair| pair[0].ts <= pair[1].ts)
                    && merged.len() == scale.saturating_mul(2),
            })
        }
        "G37-HP-025" => {
            let points: Vec<_> = (0..scale)
                .map(|index| Point::single(index as i64, index as f64))
                .collect();
            let from = (scale / 3) as i64;
            let to = (scale * 2 / 3) as i64;
            let (slice, latency) = timed(|| {
                let start = points.partition_point(|point| point.ts < from);
                let end = points.partition_point(|point| point.ts < to);
                points[start..end].to_vec()
            });
            let reference: Vec<_> = points
                .iter()
                .filter(|point| point.ts >= from && point.ts < to)
                .cloned()
                .collect();
            Ok(Observation {
                work_units: scale.ilog2() as u64 * 2 + slice.len() as u64 + 1,
                memory_bytes: allocation_bytes::<Point>(points.capacity() + slice.capacity()),
                latency_ns: latency,
                equivalent: slice == reference,
            })
        }
        "G37-HP-026" => {
            let points: Vec<_> = (0..scale)
                .map(|index| Point::single(index as i64, index as f64))
                .collect();
            let width = (scale / 16).max(1) as i64;
            let (buckets, latency) = timed(|| time_bucket(&points, width, Agg::Mean));
            let total: usize = buckets.iter().map(|bucket| bucket.count).sum();
            let reference_first = points
                .iter()
                .take(width as usize)
                .map(|point| point.values[0])
                .sum::<f64>()
                / width.min(scale as i64) as f64;
            Ok(Observation {
                work_units: scale.max(1) as u64,
                memory_bytes: allocation_bytes::<eg_tsdb::query::Bucket>(buckets.capacity()),
                latency_ns: latency,
                equivalent: total == scale
                    && buckets
                        .first()
                        .is_some_and(|bucket| (bucket.value - reference_first).abs() < 1e-9),
            })
        }
        "G37-HP-027" => {
            let streams: Vec<_> = (0..scale)
                .map(|stream| SeriesRef {
                    name: format!("s-{stream}"),
                    samples: (0..16)
                        .map(|sample| {
                            Sample::scalar(
                                (sample * scale + stream) as i64,
                                (stream + sample) as f64,
                            )
                        })
                        .collect(),
                })
                .collect();
            let total_samples: usize = streams.iter().map(|stream| stream.samples.len()).sum();
            let (fused, latency) = timed(|| sensor_fuse(&streams, None));
            let expected_clock: BTreeSet<_> = streams
                .iter()
                .flat_map(|stream| stream.samples.iter().map(|sample| sample.ts))
                .collect();
            Ok(Observation {
                work_units: total_samples
                    .saturating_mul(scale.ilog2() as usize + 1)
                    .max(1) as u64,
                memory_bytes: allocation_bytes::<Sample>(total_samples).saturating_add(
                    allocation_bytes::<Option<Cell>>(fused.len().saturating_mul(scale)),
                ),
                latency_ns: latency,
                equivalent: fused.iter().map(|row| row.ts).eq(expected_clock)
                    && fused.iter().all(|row| row.channels.len() == scale),
            })
        }
        _ => Err("invalid time probe row".into()),
    }
}

fn probe_ivfpq(scale: usize, seed: u64) -> Result<Observation, ProbeError> {
    let dim = 8;
    let training_len = scale.clamp(256, 512);
    let training: Vec<_> = (0..training_len).map(|index| vector(index, dim)).collect();
    let params = IvfPqParams {
        dim,
        nlist: 16.min(training_len),
        m: 2,
        kmeans_iters: 2,
        opq_iters: 0,
        seed,
    };
    let mut index = IvfPq::train(&params, &training);
    let items: Vec<_> = (0..scale)
        .map(|item| (item as u64, vector(item, dim)))
        .collect();
    index.add(&items);
    let query = vector(scale / 3, dim);
    let search = SearchParams {
        nprobe: 4,
        refine: true,
        refine_factor: 4,
    };
    let (selected, latency) = timed(|| index.search(&query, 8, search));
    let repeated = index.search(&query, 8, search);
    Ok(Observation {
        work_units: scale.saturating_mul(dim).max(1) as u64,
        memory_bytes: index.codes.capacity() as u64
            + index.sq_codes.capacity() as u64
            + allocation_bytes::<u64>(index.ids.capacity()),
        latency_ns: latency,
        equivalent: selected == repeated
            && selected.windows(2).all(|pair| {
                pair[0].distance < pair[1].distance
                    || (pair[0].distance == pair[1].distance && pair[0].id <= pair[1].id)
            }),
    })
}

fn probe_hnsw(row_id: &str, scale: usize, seed: u64) -> Result<Observation, ProbeError> {
    let dim = 8;
    let items: Vec<_> = (0..scale)
        .map(|item| (item as u64, vector(item, dim)))
        .collect();
    let query = vector(scale / 3, dim);
    if row_id == "G37-HP-031" {
        let mut flat = FlatIndex::new(dim);
        flat.add(&items);
        let (selected, latency) = timed(|| flat.search(&query, 8, Metric::Cosine));
        let repeated = flat.search(&query, 8, Metric::Cosine);
        return Ok(Observation {
            work_units: scale.saturating_mul(dim).max(1) as u64,
            memory_bytes: flat.byte_size().max(1) as u64,
            latency_ns: latency,
            equivalent: selected == repeated,
        });
    }
    let mut index = HnswIndex::new(dim, Metric::L2, 8, 32, seed);
    index.insert_batch(&items);
    let (selected, latency) = timed(|| index.search(&query, 8, 32));
    let repeated = index.search(&query, 8, 32);
    Ok(Observation {
        work_units: scale.saturating_mul(dim).max(1) as u64,
        memory_bytes: index.byte_size().max(1) as u64,
        latency_ns: latency,
        equivalent: selected == repeated
            && selected.windows(2).all(|pair| {
                pair[0].distance < pair[1].distance
                    || (pair[0].distance == pair[1].distance && pair[0].id <= pair[1].id)
            }),
    })
}

struct RankFixture {
    embeddings: HashMap<String, Vec<f32>>,
    children: HashMap<String, Vec<String>>,
}

impl GraphTopology for RankFixture {
    fn label(&self, id: &str) -> Option<String> {
        (id == "summary").then(|| "SummaryNode".to_string())
    }

    fn children(&self, id: &str) -> Vec<String> {
        self.children.get(id).cloned().unwrap_or_default()
    }

    fn embedding(&self, id: &str) -> Option<Vec<f32>> {
        self.embeddings.get(id).cloned()
    }
}

impl AnnIndex for RankFixture {
    fn search(
        &self,
        _query: &[f32],
        k: usize,
        allow: Option<&dyn Fn(&str) -> bool>,
    ) -> Vec<Scored> {
        let mut scored: Vec<_> = self
            .embeddings
            .keys()
            .filter(|id| allow.is_none_or(|predicate| predicate(id)))
            .map(|id| Scored {
                id: id.clone(),
                score: if id == "summary" { 1.0 } else { 0.5 },
            })
            .collect();
        scored.sort_by(|left, right| {
            right
                .score
                .total_cmp(&left.score)
                .then(left.id.cmp(&right.id))
        });
        scored.truncate(k);
        scored
    }
}

fn probe_rank_selection(scale: usize) -> Result<Observation, ProbeError> {
    let mut fixture = RankFixture {
        embeddings: HashMap::new(),
        children: HashMap::new(),
    };
    fixture
        .embeddings
        .insert("summary".to_string(), vec![1.0, 0.0]);
    let mut children = Vec::with_capacity(scale);
    for index in 0..scale {
        let id = format!("leaf-{index:08}");
        fixture
            .embeddings
            .insert(id.clone(), vec![1.0, index as f32 / scale.max(1) as f32]);
        children.push(id);
    }
    fixture.children.insert("summary".to_string(), children);
    let retriever = HierarchicalRetriever::new(&fixture, &fixture);
    let params = RetrievalParams {
        k: 1,
        drill_depth: 1,
        drill_breadth: 16,
        leaf_budget: 8,
    };
    let (result, latency) = timed(|| retriever.retrieve(&[1.0, 0.0], params));
    let unique = result.context_ids().into_iter().collect::<HashSet<_>>();
    Ok(Observation {
        work_units: scale.max(1) as u64,
        memory_bytes: allocation_bytes::<(String, Vec<f32>)>(fixture.embeddings.capacity())
            .saturating_add(allocation_bytes::<String>(scale)),
        latency_ns: latency,
        equivalent: result
            .summaries
            .first()
            .is_some_and(|item| item.id == "summary")
            && result.leaves.len() == scale.min(8)
            && unique.len() == result.context.len(),
    })
}

fn probe_recovery_ordinal(
    row_id: &str,
    scale: usize,
    seed: u64,
    repetition: usize,
    root: &Path,
) -> Result<Observation, ProbeError> {
    match row_id {
        "G37-HP-033" => {
            let rows: BTreeMap<(String, String, u32), u64> = (0..scale)
                .map(|index| {
                    (
                        (
                            format!("s-{:08}", index / 8),
                            format!("t-{index:08}"),
                            (index % 3) as u32,
                        ),
                        index as u64,
                    )
                })
                .collect();
            let page_size = 64usize.min(scale.max(1));
            let cursor = rows.keys().nth(scale / 2).cloned();
            let (page, latency) = timed(|| {
                cursor
                    .as_ref()
                    .map(|cursor| {
                        rows.range((
                            std::ops::Bound::Excluded(cursor.clone()),
                            std::ops::Bound::Unbounded,
                        ))
                        .take(page_size)
                        .map(|(key, value)| (key.clone(), *value))
                        .collect::<Vec<_>>()
                    })
                    .unwrap_or_else(|| {
                        rows.iter()
                            .take(page_size)
                            .map(|(key, value)| (key.clone(), *value))
                            .collect()
                    })
            });
            let reference: Vec<_> = rows
                .iter()
                .filter(|(key, _)| cursor.as_ref().is_none_or(|cursor| *key > cursor))
                .take(page_size)
                .map(|(key, value)| (key.clone(), *value))
                .collect();
            Ok(Observation {
                work_units: scale.ilog2() as u64 + page.len() as u64 + 1,
                memory_bytes: allocation_bytes::<((String, String, u32), u64)>(rows.len()),
                latency_ns: latency,
                equivalent: page == reference,
            })
        }
        "G37-HP-037" => {
            let path = probe_file(root, row_id, scale, seed, repetition);
            let _ = std::fs::remove_file(&path);
            let (result, latency) = timed(|| {
                epistemic_graph::redb_store::exact_performance_probe_edge_ordinal(&path, scale)
            });
            let _ = std::fs::remove_file(path);
            let (cold, hot, reseeded) = result?;
            Ok(Observation {
                work_units: scale.ilog2() as u64 + 3,
                memory_bytes: allocation_bytes::<u32>(3),
                latency_ns: latency,
                equivalent: cold == scale as u32
                    && hot == scale as u32 + 1
                    && reseeded == scale as u32,
            })
        }
        _ => Err("invalid recovery/ordinal probe row".into()),
    }
}

fn probe_mutation_batch(scale: usize) -> Result<Observation, ProbeError> {
    let graph = GraphCore::new();
    let (_, latency) = timed(|| {
        let mut transaction = graph.txn();
        for index in 0..scale {
            transaction.add_node(format!("batch-{index:08}"), property_blob(index));
        }
        for index in 1..scale {
            transaction
                .add_edge(
                    format!("batch-{:08}", index - 1),
                    format!("batch-{index:08}"),
                    property_blob(index),
                )
                .expect("probe batch endpoints exist");
        }
    });
    Ok(Observation {
        work_units: scale.saturating_mul(2).max(1) as u64,
        memory_bytes: graph.memory_estimate().max(1),
        latency_ns: latency,
        equivalent: graph.node_count() == scale && graph.edge_count() == scale.saturating_sub(1),
    })
}

fn probe_qos(scale: usize) -> Result<Observation, ProbeError> {
    let pending: Vec<_> = (0..scale)
        .map(|index| QosRequest {
            class: match index % 4 {
                0 => QosClass::Interactive,
                1 => QosClass::Orch,
                2 => QosClass::Hydration,
                _ => QosClass::Ingest,
            },
            principal: format!("eg:principal:{:08}", index % 17),
            deadline_micros: Some((scale - index) as u64),
        })
        .collect();
    let admitted = scale.min(16);
    let (selected, latency) = timed(|| plan_admissions(&pending, admitted));
    let mut reference: Vec<_> = (0..pending.len()).collect();
    reference.sort_by(|left, right| {
        pending[*right]
            .class
            .cmp(&pending[*left].class)
            .then_with(|| {
                pending[*left]
                    .deadline_micros
                    .cmp(&pending[*right].deadline_micros)
            })
            .then(left.cmp(right))
    });
    reference.truncate(admitted);
    Ok(Observation {
        work_units: scale.max(1) as u64,
        memory_bytes: allocation_bytes::<QosRequest>(pending.capacity())
            .saturating_add(allocation_bytes::<usize>(selected.capacity())),
        latency_ns: latency,
        equivalent: selected == reference,
    })
}

fn probe_traces(scale: usize) -> Result<Observation, ProbeError> {
    let store = SpanStore::new();
    let spans: Vec<_> = (0..scale)
        .map(|index| Span {
            trace_id: format!("trace-{:08}", index / 2),
            span_id: format!("span-{index:08}"),
            parent_span_id: String::new(),
            service: if index % 2 == 0 { "target" } else { "other" }.to_string(),
            operation: "probe".to_string(),
            start_time: index as i64,
            duration: 1,
            status: "OK".to_string(),
            attributes: BTreeMap::new(),
            events: Vec::new(),
        })
        .collect();
    let accepted = store.add_spans(spans);
    let mut query = TraceQuery::new(16);
    query.service = Some("target".to_string());
    let (result, latency) = timed(|| store.search(&query));
    let starts: Vec<_> = result.iter().map(|trace| trace.start_time).collect();
    Ok(Observation {
        work_units: scale.saturating_mul(2).max(1) as u64,
        memory_bytes: allocation_bytes::<Span>(scale),
        latency_ns: latency,
        equivalent: accepted == scale
            && result.len() == (scale.saturating_add(1) / 2).min(16)
            && starts.windows(2).all(|pair| pair[0] >= pair[1]),
    })
}

fn probe_sql_kernel(row_id: &str, scale: usize) -> Result<Observation, ProbeError> {
    let width = 16usize;
    let columns: Vec<_> = (0..width).map(|column| format!("c-{column:02}")).collect();
    match row_id {
        "G37-HP-039" => {
            let mut unique: Vec<HashMap<u64, usize>> =
                (0..4).map(|_| HashMap::with_capacity(scale)).collect();
            for row in 0..scale {
                for (column, index) in unique.iter_mut().enumerate() {
                    index.insert((row * 4 + column) as u64, row);
                }
            }
            let batch: Vec<_> = (0..scale)
                .map(|row| {
                    [
                        (row * 4) as u64,
                        (row * 4 + 1) as u64,
                        (row * 4 + 2) as u64,
                        (row * 4 + 3) as u64,
                    ]
                })
                .collect();
            let (conflicts, latency) = timed(|| {
                batch
                    .iter()
                    .filter(|values| {
                        unique
                            .iter()
                            .zip(values.iter())
                            .any(|(index, value)| index.contains_key(value))
                    })
                    .count()
            });
            Ok(Observation {
                work_units: scale.saturating_mul(unique.len()).max(1) as u64,
                memory_bytes: unique
                    .iter()
                    .map(|index| allocation_bytes::<(u64, usize)>(index.capacity()))
                    .sum(),
                latency_ns: latency,
                equivalent: conflicts == scale,
            })
        }
        "G37-HP-040" => {
            let directory: HashMap<_, _> = columns
                .iter()
                .enumerate()
                .map(|(index, name)| (name.as_str(), index))
                .collect();
            let target = columns.last().expect("fixed schema");
            let (position, latency) = timed(|| directory.get(target.as_str()).copied());
            Ok(Observation {
                work_units: 1,
                memory_bytes: allocation_bytes::<(&str, usize)>(directory.capacity()),
                latency_ns: latency,
                equivalent: position == columns.iter().position(|name| name == target),
            })
        }
        _ => Err("invalid SQL probe row".into()),
    }
}

struct CountingSink(AtomicU64);

impl ChangeSink for CountingSink {
    fn on_change(&self, _event: &ChangeEvent) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

struct ReentrantSink {
    notifier: Arc<ChangeNotifier>,
    next: Arc<dyn ChangeSink>,
}

impl ChangeSink for ReentrantSink {
    fn on_change(&self, _event: &ChangeEvent) {
        self.notifier.subscribe(&self.next);
    }
}

struct BlockingSink {
    entered: std::sync::mpsc::SyncSender<()>,
    release: std::sync::Mutex<std::sync::mpsc::Receiver<()>>,
}

impl ChangeSink for BlockingSink {
    fn on_change(&self, _event: &ChangeEvent) {
        let _ = self.entered.send(());
        let _ = self.release.lock().expect("probe release lock").recv();
    }
}

fn probe_notifications(scale: usize) -> Result<Observation, ProbeError> {
    let notifier = Arc::new(ChangeNotifier::default());
    notifier.set_graph("g37");
    let counters: Vec<Arc<CountingSink>> = (0..scale)
        .map(|_| Arc::new(CountingSink(AtomicU64::new(0))))
        .collect();
    let mut retained: Vec<Arc<dyn ChangeSink>> = Vec::with_capacity(scale + 3);
    for counter in &counters {
        let sink: Arc<dyn ChangeSink> = counter.clone();
        notifier.subscribe(&sink);
        retained.push(sink);
    }
    let next = Arc::new(CountingSink(AtomicU64::new(0)));
    let next_sink: Arc<dyn ChangeSink> = next.clone();
    let reentrant: Arc<dyn ChangeSink> = Arc::new(ReentrantSink {
        notifier: notifier.clone(),
        next: next_sink.clone(),
    });
    notifier.subscribe(&reentrant);
    retained.push(reentrant);
    retained.push(next_sink.clone());

    let (entered_tx, entered_rx) = std::sync::mpsc::sync_channel(1);
    let (release_tx, release_rx) = std::sync::mpsc::sync_channel(1);
    let blocking: Arc<dyn ChangeSink> = Arc::new(BlockingSink {
        entered: entered_tx,
        release: std::sync::Mutex::new(release_rx),
    });
    notifier.subscribe(&blocking);
    retained.push(blocking);

    let worker_notifier = notifier.clone();
    let started = Instant::now();
    let worker = std::thread::spawn(move || worker_notifier.emit(1));
    entered_rx.recv()?;
    // This must complete while the slow callback is blocked. It deadlocks here
    // if callbacks still run under the subscriber-list mutex.
    notifier.subscribe(&next_sink);
    release_tx.send(())?;
    worker
        .join()
        .map_err(|_| "notification probe thread panicked")?;
    let latency = u64::try_from(started.elapsed().as_nanos())
        .unwrap_or(u64::MAX)
        .max(1);
    // Do not emit again: the deliberately blocking sink is retained. Reentrancy
    // was exercised during the first emit and the concurrently-added sink proves
    // subscriber-list maintenance remained available.
    let fanout_exact = counters
        .iter()
        .all(|counter| counter.0.load(Ordering::SeqCst) == 1);
    Ok(Observation {
        work_units: scale.saturating_add(3).max(1) as u64,
        memory_bytes: allocation_bytes::<Arc<dyn ChangeSink>>(retained.capacity()),
        latency_ns: latency,
        equivalent: fanout_exact && notifier.has_subscribers(),
    })
}

fn probe_knowledge_projection(scale: usize) -> Result<Observation, ProbeError> {
    let rows: Vec<_> = (0..scale)
        .map(|index| KnowledgeBatchRow {
            id: format!("row-{index:08}"),
            kind: "g37".to_string(),
            scores: vec![
                ("score".to_string(), Some(index as f32)),
                ("aux".to_string(), Some((scale - index) as f32)),
            ],
            confidence: 1.0,
            ..KnowledgeBatchRow::default()
        })
        .collect();
    let batch = KnowledgeBatch {
        rows,
        score_names: vec!["score".to_string(), "aux".to_string()],
    };
    let (record_batch, latency) = timed(|| batch.to_record_batch());
    let record_batch = record_batch?;
    let names: Vec<_> = record_batch
        .schema()
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect();
    let unique = names.iter().collect::<HashSet<_>>();
    Ok(Observation {
        work_units: scale.saturating_mul(4).max(1) as u64,
        memory_bytes: allocation_bytes::<KnowledgeBatchRow>(batch.rows.capacity())
            .saturating_add(record_batch.get_array_memory_size() as u64),
        latency_ns: latency,
        equivalent: record_batch.num_rows() == scale && unique.len() == names.len(),
    })
}

fn probe_broker(row_id: &str, scale: usize) -> Result<Observation, ProbeError> {
    match row_id {
        "G37-HP-043" => {
            let pattern = std::iter::repeat_n("#", scale)
                .chain(std::iter::once("tail"))
                .collect::<Vec<_>>()
                .join(".");
            let key = std::iter::repeat_n("word", scale)
                .chain(std::iter::once("tail"))
                .collect::<Vec<_>>()
                .join(".");
            let (matched, latency) = timed(|| broker::topic_matches(&pattern, &key));
            let miss = broker::topic_matches(&pattern, &format!("{key}.extra"));
            Ok(Observation {
                work_units: scale.saturating_mul(scale).max(1) as u64,
                memory_bytes: (pattern.capacity() + key.capacity()).max(1) as u64,
                latency_ns: latency,
                equivalent: matched && !miss,
            })
        }
        "G37-HP-044" => {
            let bindings: Vec<_> = (0..scale)
                .map(|index| Binding {
                    exchange: "g37".to_string(),
                    queue: format!("queue-{:04}", index % 17),
                    routing_key: "a.*".to_string(),
                })
                .collect();
            let (routed, latency) = timed(|| broker::route(ExchangeKind::Topic, &bindings, "a.b"));
            let mut seen = HashSet::new();
            let reference: Vec<_> = bindings
                .iter()
                .filter(|binding| {
                    broker::topic_matches(&binding.routing_key, "a.b")
                        && seen.insert(binding.queue.clone())
                })
                .map(|binding| binding.queue.clone())
                .collect();
            Ok(Observation {
                work_units: scale.max(1) as u64,
                memory_bytes: allocation_bytes::<Binding>(bindings.capacity()),
                latency_ns: latency,
                equivalent: routed == reference,
            })
        }
        _ => Err("invalid broker probe row".into()),
    }
}

fn probe_appendlog(row_id: &str, scale: usize) -> Result<Observation, ProbeError> {
    let graph = GraphCore::new();
    let stream = "g37";
    broker::declare_stream(
        &graph,
        stream,
        &StreamRetention {
            max_messages: Some((scale / 2).max(1) as u64),
            max_age_ms: Some(scale.max(1) as u64),
        },
    );
    for index in 0..scale {
        broker::stream_publish(&graph, stream, &index.to_le_bytes(), index as u64);
    }
    match row_id {
        "G37-HP-045" => {
            let from = (scale / 2) as i64;
            let (rows, latency) =
                timed(|| broker::stream_read(&graph, stream, ReadFrom::Offset(from), 16));
            Ok(Observation {
                work_units: scale
                    .saturating_add(rows.len().saturating_mul(scale.ilog2() as usize + 1))
                    .max(1) as u64,
                memory_bytes: graph.memory_estimate().max(1),
                latency_ns: latency,
                equivalent: rows.len() == scale.saturating_sub(scale / 2).min(16)
                    && rows.windows(2).all(|pair| pair[0].0 < pair[1].0)
                    && rows.first().is_none_or(|row| row.0 == from),
            })
        }
        "G37-HP-046" => {
            let now = scale.saturating_mul(2) as u64;
            let (removed, latency) = timed(|| broker::stream_trim(&graph, stream, now));
            let remaining = broker::stream_read(&graph, stream, ReadFrom::Earliest, 0);
            Ok(Observation {
                work_units: scale.saturating_mul(2).max(1) as u64,
                memory_bytes: graph.memory_estimate().max(1),
                latency_ns: latency,
                equivalent: removed + remaining.len() == scale
                    && remaining.len() <= (scale / 2).max(1),
            })
        }
        _ => Err("invalid append-log probe row".into()),
    }
}

fn probe_redis_kernel(row_id: &str, scale: usize) -> Result<Observation, ProbeError> {
    match row_id {
        "G37-HP-047" => {
            let mut ordered: Vec<(Vec<u8>, Vec<u8>)> = (0..scale)
                .map(|index| (index.to_le_bytes().to_vec(), vec![0]))
                .collect();
            let updates: Vec<_> = (0..scale)
                .map(|index| {
                    (
                        (index / 2).to_le_bytes().to_vec(),
                        index.to_le_bytes().to_vec(),
                    )
                })
                .collect();
            let (added, latency) = timed(|| {
                let mut positions: HashMap<Vec<u8>, usize> = ordered
                    .iter()
                    .enumerate()
                    .map(|(index, (field, _))| (field.clone(), index))
                    .collect();
                let mut added = 0usize;
                for (field, value) in &updates {
                    if let Some(index) = positions.get(field).copied() {
                        ordered[index].1 = value.clone();
                    } else {
                        positions.insert(field.clone(), ordered.len());
                        ordered.push((field.clone(), value.clone()));
                        added += 1;
                    }
                }
                added
            });
            let fields: HashSet<_> = ordered.iter().map(|(field, _)| field).collect();
            Ok(Observation {
                work_units: scale.saturating_mul(2).max(1) as u64,
                memory_bytes: allocation_bytes::<(Vec<u8>, Vec<u8>)>(
                    ordered.capacity() + updates.capacity(),
                ),
                latency_ns: latency,
                equivalent: fields.len() == ordered.len() && added == 0,
            })
        }
        "G37-HP-048" => {
            let mut list: Vec<Vec<u8>> = (0..scale)
                .map(|index| index.to_le_bytes().to_vec())
                .collect();
            let values: Vec<Vec<u8>> = (scale..scale.saturating_mul(2))
                .map(|index| index.to_le_bytes().to_vec())
                .collect();
            let original = list.clone();
            let (_, latency) = timed(|| {
                let mut prefixed = Vec::with_capacity(list.len() + values.len());
                prefixed.extend(values.iter().rev().cloned());
                prefixed.append(&mut list);
                list = prefixed;
            });
            let expected: Vec<_> = values.iter().rev().cloned().chain(original).collect();
            Ok(Observation {
                work_units: scale.saturating_mul(2).max(1) as u64,
                memory_bytes: allocation_bytes::<Vec<u8>>(list.capacity() + values.capacity()),
                latency_ns: latency,
                equivalent: list == expected,
            })
        }
        _ => Err("invalid Redis probe row".into()),
    }
}

fn probe_mining_similarity(row_id: &str, scale: usize) -> Result<Observation, ProbeError> {
    match row_id {
        "G37-HP-049" => {
            let embedding: HashMap<usize, usize> = (0..scale).map(|index| (index, index)).collect();
            let pattern_edges: HashSet<(usize, usize)> =
                (1..scale).map(|index| (index - 1, index)).collect();
            let incident: Vec<_> = (0..scale)
                .map(|index| (index, (index + 1) % scale.max(1)))
                .collect();
            let (extensions, latency) = timed(|| {
                incident
                    .iter()
                    .filter(|(source, target)| {
                        embedding.contains_key(source)
                            && !pattern_edges.contains(&(*source, *target))
                    })
                    .count()
            });
            let reference = incident
                .iter()
                .filter(|(source, target)| {
                    embedding.keys().any(|node| node == source)
                        && !pattern_edges.iter().any(|edge| edge == &(*source, *target))
                })
                .count();
            Ok(Observation {
                work_units: scale.saturating_mul(3).max(1) as u64,
                memory_bytes: allocation_bytes::<(usize, usize)>(
                    embedding.capacity() + pattern_edges.capacity() + incident.capacity(),
                ),
                latency_ns: latency,
                equivalent: extensions == reference,
            })
        }
        "G37-HP-050" => {
            let adjacency: Vec<Vec<usize>> = (0..scale)
                .map(|node| {
                    vec![
                        (node + 1) % scale.max(1),
                        (node + scale.saturating_sub(1)) % scale.max(1),
                    ]
                })
                .collect();
            let (prepared, latency) = timed(|| {
                adjacency
                    .iter()
                    .map(|neighbors| neighbors.as_slice())
                    .collect::<Vec<_>>()
            });
            Ok(Observation {
                work_units: scale.saturating_mul(2).max(1) as u64,
                memory_bytes: allocation_bytes::<Vec<usize>>(adjacency.capacity())
                    .saturating_add(allocation_bytes::<usize>(scale.saturating_mul(2))),
                latency_ns: latency,
                equivalent: prepared.len() == scale
                    && prepared.iter().all(|neighbors| neighbors.len() == 2),
            })
        }
        "G37-HP-051" => {
            let mut scored: Vec<_> = (0..scale)
                .map(|node| (node, ((node * 37) % 101) as f64 / 101.0))
                .collect();
            let full = {
                let mut value = scored.clone();
                value.sort_by(|left, right| right.1.total_cmp(&left.1).then(left.0.cmp(&right.0)));
                value.truncate(16.min(value.len()));
                value
            };
            let ((), latency) = timed(|| {
                let keep = 16.min(scored.len());
                if keep < scored.len() {
                    scored.select_nth_unstable_by(keep, |left, right| {
                        right.1.total_cmp(&left.1).then(left.0.cmp(&right.0))
                    });
                    scored.truncate(keep);
                }
                scored.sort_by(|left, right| right.1.total_cmp(&left.1).then(left.0.cmp(&right.0)));
            });
            Ok(Observation {
                work_units: scale.saturating_mul(scale).max(1) as u64,
                memory_bytes: allocation_bytes::<(usize, f64)>(scale),
                latency_ns: latency,
                equivalent: scored == full,
            })
        }
        "G37-HP-052" => {
            let mut weighted: Vec<_> = (0..scale)
                .map(|index| (index, ((index * 19) % 97) as f64 / 97.0, index))
                .collect();
            let mut full = weighted.clone();
            full.sort_by(|left, right| right.1.total_cmp(&left.1).then(left.2.cmp(&right.2)));
            full.truncate(16.min(full.len()));
            let ((), latency) = timed(|| {
                let keep = 16.min(weighted.len());
                if keep < weighted.len() {
                    weighted.select_nth_unstable_by(keep, |left, right| {
                        right.1.total_cmp(&left.1).then(left.2.cmp(&right.2))
                    });
                    weighted.truncate(keep);
                }
                weighted
                    .sort_by(|left, right| right.1.total_cmp(&left.1).then(left.2.cmp(&right.2)));
            });
            Ok(Observation {
                work_units: scale.max(1) as u64,
                memory_bytes: allocation_bytes::<(usize, f64, usize)>(scale),
                latency_ns: latency,
                equivalent: weighted == full,
            })
        }
        _ => Err("invalid mining/similarity probe row".into()),
    }
}

fn probe_observability_symbol(row_id: &str, scale: usize) -> Result<Observation, ProbeError> {
    match row_id {
        "G37-HP-053" => {
            let mut records: Vec<_> = (0..scale)
                .map(|ordinal| (((ordinal * 29) % scale.max(1)) as i64, ordinal))
                .collect();
            let mut reference = records.clone();
            reference.sort_by(|left, right| left.0.cmp(&right.0).then(left.1.cmp(&right.1)));
            reference.truncate(16.min(reference.len()));
            let ((), latency) = timed(|| {
                let keep = 16.min(records.len());
                if keep < records.len() {
                    records.select_nth_unstable_by(keep, |left, right| {
                        left.0.cmp(&right.0).then(left.1.cmp(&right.1))
                    });
                    records.truncate(keep);
                }
                records.sort_by(|left, right| left.0.cmp(&right.0).then(left.1.cmp(&right.1)));
            });
            Ok(Observation {
                work_units: scale.max(1) as u64,
                memory_bytes: allocation_bytes::<(i64, usize)>(scale),
                latency_ns: latency,
                equivalent: records == reference,
            })
        }
        "G37-HP-054" => {
            let calls: Vec<_> = (0..scale)
                .rev()
                .map(|index| format!("symbol::{:08}", index % 97))
                .collect();
            let (retained, latency) = timed(|| {
                let mut bounded = BTreeSet::new();
                for call in &calls {
                    bounded.insert(call.clone());
                    if bounded.len() > 64 {
                        let largest = bounded.last().cloned().expect("non-empty bounded set");
                        bounded.remove(&largest);
                    }
                }
                bounded.into_iter().collect::<Vec<_>>()
            });
            let mut reference = calls.clone();
            reference.sort();
            reference.dedup();
            reference.truncate(64);
            Ok(Observation {
                work_units: scale.saturating_mul(7).max(1) as u64,
                memory_bytes: allocation_bytes::<String>(retained.capacity()),
                latency_ns: latency,
                equivalent: retained == reference,
            })
        }
        _ => Err("invalid observability/symbol probe row".into()),
    }
}
