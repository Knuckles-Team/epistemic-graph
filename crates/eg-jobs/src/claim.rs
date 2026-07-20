//! Result commit: turning a succeeded [`AnalyticsJob`] into a provenance'd epistemic
//! claim (CONCEPT:INT-P2-1).
//!
//! Mirrors the SAME typed-node-by-convention pattern
//! `src/server/handlers/mining.rs`'s `materialize_claim` established for the
//! synchronous `Mine*` writeback path (E6): a `:Claim` node + a `:Evidence` node +
//! two `SUPPORTS` edges (`relationship = "SUPPORTS"`, the canonical key
//! `eg_epistemic::classify_relationship` reads), so `eg-epistemic`'s
//! `BeliefGraph`/`propagate_confidence` treat a job's result exactly like any other
//! mined finding. This crate does not depend on `eg-epistemic` itself (that crate has
//! NO write path by design — "no new persistence", see its module docs) — it writes
//! the SAME convention `eg-epistemic` reads, via the SAME `eg-core::GraphCore`
//! primitive `materialize_claim` uses.
//!
//! The one thing this adds beyond the synchronous mining writeback: FULL job
//! lineage on the claim (`job_id`, the `InputSnapshotHandle`, and the complete
//! `AlgoVersion`) — a "versioned derived claim" a caller can trace back to exactly
//! which job, over which graph version, running which algorithm build, produced it.
//!
//! CONCEPT:EG-P3-1 (universal writeback lineage) additionally materializes:
//!
//!  * a `:Activity` node holding the job's FULL `AlgoVersion` + `InputSnapshotHandle`
//!    (the complete algo/model/code/env-version + params-digest + input-snapshot
//!    lineage), linked from the claim via `claim --GENERATED_BY--> activity` — the
//!    SAME PROV-style convention `src/server/handlers/mining.rs`'s `materialize_claim`
//!    writes for the synchronous path, so `eg-plan`'s `KnowledgeSet::from_rowset`
//!    resolves it into `KnowledgeRow::transformation_ids` regardless of which
//!    writeback path produced the claim;
//!  * `invalidation_deps` on the claim — the ids whose change/removal invalidates it
//!    (the input-snapshot handle string + the evidence node id);
//!  * a `calibration` slot — `null` when the caller passes `None` (no calibration
//!    signal computed for this result), or the caller-supplied [`CalibrationInput`]
//!    otherwise (L52, CONCEPT:EG-KG.epistemic.epistemic-substrate wiring). This crate
//!    deliberately does NOT depend on `eg-epistemic` (see the module doc above — that
//!    crate has NO write path by design, and this crate's whole job is writing), so
//!    `CalibrationInput` is a PLAIN, locally-defined data mirror of
//!    `eg_epistemic::model::Calibration`'s shape (`interval`/`level`/`evidence_count`)
//!    rather than a re-export of that type — a caller who computed a real
//!    `eg_epistemic::Calibration` (e.g. from `propagate_confidence`'s `BeliefState`)
//!    converts it into this crate's plain fields at the call site.
//!
//! `GENERATED_BY` is deliberately NOT one of `eg_epistemic::classify_relationship`'s
//! whitelisted values, so it is automatically epistemically NEUTRAL — ignored by
//! `BeliefGraph`/`propagate_confidence`, exactly like the `SUPPORTS` edges above are
//! deliberately IN that whitelist.

use eg_core::graph::GraphCore;
use eg_types::protocol::Method;

use crate::model::AnalyticsJob;

/// `validation_state` seeded on every fresh job-committed `:Claim`/`:Evidence` —
/// same convention as `mining.rs`'s `CLAIM_VALIDATION_STATE`: asserted, not yet
/// independently validated.
pub const CLAIM_VALIDATION_STATE: &str = "unvalidated";

/// Outcome of a [`commit_result_claim`] call.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClaimCommitOutcome {
    /// This call performed the write — first time this `result_ref` was committed.
    Committed { claim_id: String },
    /// A claim for this `result_ref` already existed (this job, a retry of it, or a
    /// DIFFERENT job with identical input-snapshot+algo lineage) — no graph mutation
    /// happened.
    AlreadyCommitted { claim_id: String },
}

impl ClaimCommitOutcome {
    pub fn claim_id(&self) -> &str {
        match self {
            ClaimCommitOutcome::Committed { claim_id }
            | ClaimCommitOutcome::AlreadyCommitted { claim_id } => claim_id,
        }
    }
}

/// Deterministic claim node id for a `result_ref` — re-committing (this job, a
/// retry, or a different job with identical lineage) always names the SAME claim.
pub fn claim_node_id(result_ref: &str) -> String {
    format!("jobclaim:{result_ref}")
}

/// Deterministic evidence node id — one evidence node per `(result_ref, job_id)`, so
/// a DIFFERENT job that independently reproduces the same `result_ref` corroborates
/// the SAME claim with an ADDITIONAL supporting evidence node (mirrors mining.rs's
/// provenance-keyed evidence id: distinct provenance ⇒ corroboration, not overwrite).
pub fn evidence_node_id(result_ref: &str, job_id: &str) -> String {
    format!("jobevidence:{result_ref}:{job_id}")
}

/// Deterministic `:Activity` node id (CONCEPT:EG-P3-1) — one Activity per
/// `result_ref` (the SAME deterministic-lineage hash the claim id folds in), so a
/// re-commit (same job retried, or a different job with identical lineage) converges
/// on the SAME Activity rather than accumulating a fresh one per call.
pub fn activity_node_id(result_ref: &str) -> String {
    format!("jobactivity:{result_ref}")
}

/// Deterministic node for the immutable typed result dataset.
pub fn dataset_node_id(result_ref: &str) -> String {
    format!("jobdataset:{result_ref}")
}

/// A plain-data mirror of `eg_epistemic::model::Calibration`'s shape (L52): the
/// central credible interval, its probability mass, and the evidence count that fed
/// it. This crate deliberately does not depend on `eg-epistemic` (see module docs),
/// so this is NOT that type — a caller holding a real `eg_epistemic::Calibration`
/// converts it into this at the call site (`interval`/`level`/`evidence_count` line up
/// field-for-field).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct CalibrationInput {
    /// Central credible interval `(lower, upper) ⊆ [0, 1]` at `level`.
    pub interval: (f64, f64),
    /// The probability mass the interval covers (e.g. `0.95`).
    pub level: f64,
    /// How many pieces of evidence (rules/observations/samples) fed this calibration.
    pub evidence_count: usize,
}

/// Deterministic graph write-set for one succeeded job result.  The server uses
/// this representation to commit the claim, evidence and activity through its
/// authoritative MutationBatch gateway instead of letting a background executor
/// mutate the live `GraphCore` directly.
#[derive(Debug, Clone)]
pub struct ClaimWritePlan {
    pub claim_id: String,
    pub methods: Vec<Method>,
}

/// Lower a durably staged result to canonical graph methods without applying them.
/// The write-set is deterministic for `(result_ref, job_id)` and therefore safe to
/// stage, digest, retry and replay through the engine's universal mutation kernel.
pub fn plan_result_claim(
    job: &AnalyticsJob,
    confidence: f64,
    calibration: Option<CalibrationInput>,
) -> Result<ClaimWritePlan, String> {
    let result_ref = match &job.state {
        crate::model::JobState::Publishing { result_ref, .. }
        | crate::model::JobState::Succeeded { result_ref, .. } => result_ref.clone(),
        other => {
            return Err(format!(
                "plan_result_claim requires a Publishing or Succeeded job, got {}",
                other.label()
            ))
        }
    };
    debug_assert_eq!(result_ref, job.result_ref());
    let output = job
        .output
        .as_ref()
        .ok_or_else(|| "a published job claim requires its durable typed result".to_string())?;
    output.validate()?;

    let claim_id = claim_node_id(&result_ref);
    let evidence_id = evidence_node_id(&result_ref, &job.job_id);
    let activity_id = activity_node_id(&result_ref);
    let dataset_id = dataset_node_id(&result_ref);
    let confidence = confidence.clamp(0.0, 1.0);
    let snapshot_handle = job.input_snapshot.dataset_ref.clone();
    let claim_props = serde_json::json!({
        "type": "Claim",
        "family": job.algo.family,
        "about": result_ref.clone(),
        "confidence": confidence,
        "validation_state": CLAIM_VALIDATION_STATE,
        "job_id": job.job_id,
        "input_dataset_ref": job.input_snapshot.dataset_ref,
        "input_content_digest": job.input_snapshot.content_digest,
        "input_snapshot_version": job.input_snapshot.version,
        "algo_family": job.algo.family,
        "algo_algorithm": job.algo.algorithm,
        "algo_params_digest": job.algo.params_digest,
        "algo_code_version": job.algo.code_version,
        "algo_env_version": job.algo.env_version,
        "result_ref": result_ref.clone(),
        "calibration": calibration.map(|c| serde_json::json!({
            "interval": [c.interval.0, c.interval.1],
            "level": c.level,
            "evidence_count": c.evidence_count,
        })).unwrap_or(serde_json::Value::Null),
        "invalidation_deps": [snapshot_handle.as_str(), evidence_id.as_str()],
    });
    let evidence_props = serde_json::json!({
        "type": "Evidence",
        "family": job.algo.family,
        "about": result_ref.clone(),
        "provenance": format!("job:{}", job.job_id),
        "confidence": confidence,
        "validation_state": CLAIM_VALIDATION_STATE,
        "job_id": job.job_id,
        "tenant": job.policy.tenant,
        "actor": job.policy.actor,
        "purpose": job.policy.purpose,
    });
    let activity_props = serde_json::json!({
        "type": "Activity",
        "job_id": job.job_id,
        "input_dataset_ref": job.input_snapshot.dataset_ref,
        "input_content_digest": job.input_snapshot.content_digest,
        "input_snapshot_version": job.input_snapshot.version,
        "algo_family": job.algo.family,
        "algo_algorithm": job.algo.algorithm,
        "algo_params_digest": job.algo.params_digest,
        "algo_code_version": job.algo.code_version,
        "algo_env_version": job.algo.env_version,
    });
    let supports = rmp_serde::to_vec_named(&serde_json::json!({ "relationship": "SUPPORTS" }))
        .map_err(|error| error.to_string())?;
    let generated_by =
        rmp_serde::to_vec_named(&serde_json::json!({ "relationship": "GENERATED_BY" }))
            .map_err(|error| error.to_string())?;
    let contains = rmp_serde::to_vec_named(&serde_json::json!({ "relationship": "CONTAINS" }))
        .map_err(|error| error.to_string())?;

    let mut methods = vec![
        Method::AddNode {
            node_id: claim_id.clone(),
            properties_msgpack: rmp_serde::to_vec_named(&claim_props)
                .map_err(|error| error.to_string())?,
        },
        Method::AddNode {
            node_id: evidence_id.clone(),
            properties_msgpack: rmp_serde::to_vec_named(&evidence_props)
                .map_err(|error| error.to_string())?,
        },
        Method::AddNode {
            node_id: activity_id.clone(),
            properties_msgpack: rmp_serde::to_vec_named(&activity_props)
                .map_err(|error| error.to_string())?,
        },
    ];

    if let Some(output) = &job.output {
        let dataset_props = serde_json::json!({
            "type": "KnowledgeBatch",
            "schema_version": output.schema_version,
            "dataset_ref": output.dataset_ref,
            "content_digest": output.content_digest,
            "schema": output.schema,
            "row_count": output.rows.len(),
            "evidence_refs": output.evidence_refs,
            "counterexample_refs": output.counterexample_refs,
            "uncertainty": output.uncertainty,
            "calibration": output.calibration,
            "reproducibility": output.reproducibility,
        });
        methods.push(Method::AddNode {
            node_id: dataset_id.clone(),
            properties_msgpack: rmp_serde::to_vec_named(&dataset_props)
                .map_err(|error| error.to_string())?,
        });
        methods.push(Method::AddEdge {
            source_id: claim_id.clone(),
            target_id: dataset_id.clone(),
            properties_msgpack: generated_by.clone(),
        });

        for (index, row) in output.rows.iter().enumerate() {
            let row_ref = row
                .get("id")
                .and_then(serde_json::Value::as_str)
                .map(str::to_string)
                .unwrap_or_else(|| format!("row-{index}"));
            let row_claim_id = format!("jobclaim:{result_ref}:{row_ref}");
            let row_evidence_id = format!("jobevidence:{result_ref}:{row_ref}");
            let row_confidence = row
                .get("confidence")
                .and_then(serde_json::Value::as_f64)
                .unwrap_or(confidence)
                .clamp(0.0, 1.0);
            let row_claim_props = serde_json::json!({
                "type": "Claim",
                "family": job.algo.family,
                "about": row_ref,
                "confidence": row_confidence,
                "validation_state": CLAIM_VALIDATION_STATE,
                "result_ref": result_ref,
                "dataset_ref": output.dataset_ref,
                "knowledge": row,
                "evidence_refs": row.get("evidence_refs").cloned().unwrap_or_default(),
                "source_refs": row.get("source_refs").cloned().unwrap_or_default(),
                "proof_ids": row.get("proof_ids").cloned().unwrap_or_default(),
                "contradiction_ids": row.get("contradiction_ids").cloned().unwrap_or_default(),
                "invalidation_deps": [job.input_snapshot.dataset_ref.as_str(), row_evidence_id.as_str()],
            });
            let row_evidence_props = serde_json::json!({
                "type": "Evidence",
                "about": row_ref,
                "dataset_ref": output.dataset_ref,
                "evidence_refs": row.get("evidence_refs").cloned().unwrap_or_default(),
                "source_refs": row.get("source_refs").cloned().unwrap_or_default(),
                "confidence": row_confidence,
                "validation_state": CLAIM_VALIDATION_STATE,
            });
            methods.extend([
                Method::AddNode {
                    node_id: row_claim_id.clone(),
                    properties_msgpack: rmp_serde::to_vec_named(&row_claim_props)
                        .map_err(|error| error.to_string())?,
                },
                Method::AddNode {
                    node_id: row_evidence_id.clone(),
                    properties_msgpack: rmp_serde::to_vec_named(&row_evidence_props)
                        .map_err(|error| error.to_string())?,
                },
                Method::AddEdge {
                    source_id: row_evidence_id,
                    target_id: row_claim_id.clone(),
                    properties_msgpack: supports.clone(),
                },
                Method::AddEdge {
                    source_id: dataset_id.clone(),
                    target_id: row_claim_id,
                    properties_msgpack: contains.clone(),
                },
            ]);
        }
    }

    methods.extend([
        Method::AddEdge {
            source_id: evidence_id,
            target_id: claim_id.clone(),
            properties_msgpack: supports,
        },
        Method::AddEdge {
            source_id: claim_id.clone(),
            target_id: activity_id,
            properties_msgpack: generated_by,
        },
    ]);

    Ok(ClaimWritePlan {
        claim_id: claim_id.clone(),
        methods,
    })
}

/// Commit `job`'s result (`job.state` must be `Succeeded`) as a provenance'd claim
/// into `core` (CONCEPT:INT-P2-1). Transactional (each write is a single
/// `GraphCore::add_node`/`add_edge` call — atomic per the engine's own txn
/// convention) and IDEMPOTENT: if the deterministic claim id already exists in
/// `core`, this is a no-op that returns [`ClaimCommitOutcome::AlreadyCommitted`]
/// without writing anything (not even a duplicate `SUPPORTS` edge).
///
/// `confidence` is the caller-computed quality score for this result (e.g. mean
/// `support * confidence` over mined association rules), clamped to `[0,1]` — the
/// SAME "quality score seeds claim confidence" convention `mining.rs` uses.
///
/// `calibration` (L52) is `None` when the caller has no calibration signal for this
/// result (the claim's `calibration` property lands an honest `null`, byte-identical
/// to this function's behavior before this parameter existed); `Some(CalibrationInput)`
/// carries it through onto the SAME `calibration` slot the universal writeback-lineage
/// tuple already reserved.
pub fn commit_result_claim(
    core: &GraphCore,
    job: &AnalyticsJob,
    confidence: f64,
    calibration: Option<CalibrationInput>,
) -> Result<ClaimCommitOutcome, String> {
    let result_ref = match &job.state {
        crate::model::JobState::Succeeded { result_ref, .. } => result_ref.clone(),
        other => {
            return Err(format!(
                "commit_result_claim requires a Succeeded job, got {}",
                other.label()
            ))
        }
    };
    // Defensive: the caller should already know result_ref == job.result_ref() —
    // the deterministic lineage hash — but a caller can't accidentally desync a
    // claim's lineage from its own id since we recompute it here rather than trust
    // the stored string alone.
    debug_assert_eq!(result_ref, job.result_ref());

    let claim_id = claim_node_id(&result_ref);
    if core.has_node(&claim_id) {
        return Ok(ClaimCommitOutcome::AlreadyCommitted { claim_id });
    }

    let plan = plan_result_claim(job, confidence, calibration)?;
    for method in plan.methods {
        match method {
            Method::AddNode {
                node_id,
                properties_msgpack,
            } => core.add_node(node_id, properties_msgpack),
            Method::AddEdge {
                source_id,
                target_id,
                properties_msgpack,
            } => core.add_edge(source_id, target_id, properties_msgpack)?,
            _ => return Err("claim write plan contained a non-graph operation".to_string()),
        }
    }

    Ok(ClaimCommitOutcome::Committed { claim_id })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{AlgoVersion, InputSnapshotHandle, JobPolicy, JobState, RetryPolicy};
    use crate::result::{ReproducibilityManifest, ResultColumn, TypedJobResult};

    fn succeeded_job(job_id: &str, graph: &str, version: u64) -> AnalyticsJob {
        let input_snapshot = InputSnapshotHandle::new(graph, version).with_dataset(
            "eg:job_input:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        );
        let algo = AlgoVersion {
            family: "mining.association".into(),
            algorithm: "fpgrowth".into(),
            params_digest: "deadbeef".into(),
            code_version: "test".into(),
            env_version: "test".into(),
        };
        let result_ref = crate::model::compute_result_ref(&input_snapshot, &algo);
        AnalyticsJob {
            job_id: job_id.to_string(),
            input_snapshot,
            policy: JobPolicy {
                tenant: "tenant:sha256:test".into(),
                actor: "principal:sha256:test".into(),
                purpose: "purpose:sha256:test".into(),
                priority: 0,
                quota_cpu_ms: None,
                deadline_unix_ms: None,
                ..JobPolicy::default()
            },
            algo,
            input_payload: None,
            retry: RetryPolicy::default(),
            state: JobState::Succeeded {
                result_ref,
                checkpoint: crate::model::Checkpoint {
                    progress: 1.0,
                    stage: "done".into(),
                    state_blob: None,
                    updated_at_ms: 0,
                },
            },
            cancel_requested: false,
            lease_epoch: 0,
            lease: None,
            last_worker_ref: String::new(),
            not_before_ms: 0,
            output: Some(
                TypedJobResult::new(
                    [
                        "id",
                        "kind",
                        "confidence",
                        "evidence_refs",
                        "source_refs",
                        "proof_ids",
                        "contradiction_ids",
                    ]
                    .into_iter()
                    .map(|name| ResultColumn {
                        name: name.to_string(),
                        logical_type: "json".to_string(),
                        nullable: false,
                    })
                    .collect(),
                    Vec::new(),
                    vec![
                        "eg:job_input:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                            .to_string(),
                    ],
                    Vec::new(),
                    None,
                    None,
                    ReproducibilityManifest::default(),
                )
                .unwrap(),
            ),
            created_at_ms: 0,
            updated_at_ms: 0,
        }
    }

    #[test]
    fn commit_writes_claim_with_full_lineage() {
        let core = GraphCore::new();
        let job = succeeded_job("job-0000000000000001", "g1", 5);

        let outcome = commit_result_claim(&core, &job, 0.87, None).unwrap();
        let claim_id = match outcome {
            ClaimCommitOutcome::Committed { claim_id } => claim_id,
            other => panic!("expected Committed, got {other:?}"),
        };
        assert!(core.has_node(&claim_id));

        let blob = core.get_node_properties(&claim_id).unwrap();
        let props: serde_json::Value = rmp_serde::from_slice(&blob).unwrap();
        assert_eq!(props["type"], "Claim");
        assert_eq!(
            props["input_dataset_ref"],
            "eg:job_input:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        );
        assert_eq!(props["input_snapshot_version"], 5);
        assert_eq!(props["algo_family"], "mining.association");
        assert_eq!(props["algo_algorithm"], "fpgrowth");
        assert_eq!(props["algo_params_digest"], "deadbeef");
        assert_eq!(props["job_id"], "job-0000000000000001");
        assert_eq!(props["confidence"], 0.87);
        assert_eq!(props["validation_state"], CLAIM_VALIDATION_STATE);

        let evidence_id = evidence_node_id(&job.result_ref(), &job.job_id);
        assert!(core.has_node(&evidence_id));
        assert!(core.has_edge(&evidence_id, &claim_id));

        // CONCEPT:EG-P3-1 — the universal writeback-lineage tuple: `calibration` is
        // an honest `null` (no signal computed here), `invalidation_deps` names the
        // input-snapshot handle + this claim's own evidence node, and a
        // `claim --GENERATED_BY--> activity` edge points at an Activity node
        // carrying the FULL `AlgoVersion` + `InputSnapshotHandle`.
        assert!(props["calibration"].is_null());
        let deps = props["invalidation_deps"].as_array().unwrap();
        assert_eq!(deps.len(), 2);
        assert_eq!(
            deps[0],
            "eg:job_input:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        );
        assert_eq!(deps[1], evidence_id);

        let activity_id = activity_node_id(&job.result_ref());
        assert!(core.has_node(&activity_id));
        assert!(core.has_edge(&claim_id, &activity_id));
        let activity_blob = core.get_node_properties(&activity_id).unwrap();
        let activity_props: serde_json::Value = rmp_serde::from_slice(&activity_blob).unwrap();
        assert_eq!(activity_props["type"], "Activity");
        assert_eq!(activity_props["job_id"], "job-0000000000000001");
        assert_eq!(
            activity_props["input_dataset_ref"],
            "eg:job_input:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        );
        assert_eq!(activity_props["input_snapshot_version"], 5);
        assert_eq!(activity_props["algo_family"], "mining.association");
        assert_eq!(activity_props["algo_algorithm"], "fpgrowth");
        assert_eq!(activity_props["algo_params_digest"], "deadbeef");
        assert_eq!(activity_props["algo_code_version"], "test");
        assert_eq!(activity_props["algo_env_version"], "test");
    }

    #[test]
    fn duplicate_result_commit_is_a_no_op() {
        let core = GraphCore::new();
        let job_a = succeeded_job("job-0000000000000001", "g1", 5);
        // A DIFFERENT job with IDENTICAL input-snapshot + algo lineage.
        let job_b = succeeded_job("job-0000000000000002", "g1", 5);
        assert_eq!(job_a.result_ref(), job_b.result_ref());

        let first = commit_result_claim(&core, &job_a, 0.87, None).unwrap();
        assert!(matches!(first, ClaimCommitOutcome::Committed { .. }));

        let second = commit_result_claim(&core, &job_b, 0.5, None).unwrap();
        assert!(matches!(
            second,
            ClaimCommitOutcome::AlreadyCommitted { .. }
        ));
        assert_eq!(first.claim_id(), second.claim_id());

        // Exactly ONE claim node — the second commit never touched the graph.
        let claim_nodes: Vec<_> = core
            .get_nodes()
            .into_iter()
            .filter(|(id, _)| id.starts_with("jobclaim:"))
            .collect();
        assert_eq!(claim_nodes.len(), 1);
        // Confidence stayed at the FIRST commit's value (0.87), proving the second
        // call really was a no-op rather than overwriting with 0.5.
        let props: serde_json::Value =
            rmp_serde::from_slice(&core.get_node_properties(first.claim_id()).unwrap()).unwrap();
        assert_eq!(props["confidence"], 0.87);

        // Re-running commit AGAIN for job_a itself is also a no-op (same job, twice).
        let third = commit_result_claim(&core, &job_a, 0.1, None).unwrap();
        assert!(matches!(third, ClaimCommitOutcome::AlreadyCommitted { .. }));
    }

    #[test]
    fn commit_rejects_a_non_succeeded_job() {
        let core = GraphCore::new();
        let mut job = succeeded_job("job-0000000000000001", "g1", 5);
        job.state = JobState::Submitted;
        assert!(commit_result_claim(&core, &job, 0.5, None).is_err());
    }

    // A completed job's claim carries a real calibration value when the caller
    // supplies one (L52) — proving the writeback threads `CalibrationInput` through
    // onto the SAME `calibration` slot that stayed an honest `null` above.
    #[test]
    fn commit_with_calibration_lands_a_real_calibration_value() {
        let core = GraphCore::new();
        let job = succeeded_job("job-0000000000000001", "g1", 5);
        let calibration = CalibrationInput {
            interval: (0.72, 0.94),
            level: 0.95,
            evidence_count: 12,
        };

        let outcome = commit_result_claim(&core, &job, 0.87, Some(calibration)).unwrap();
        let claim_id = outcome.claim_id();
        let props: serde_json::Value =
            rmp_serde::from_slice(&core.get_node_properties(claim_id).unwrap()).unwrap();

        assert!(!props["calibration"].is_null());
        assert_eq!(props["calibration"]["level"], 0.95);
        assert_eq!(props["calibration"]["evidence_count"], 12);
        let interval = props["calibration"]["interval"].as_array().unwrap();
        assert_eq!(interval[0].as_f64().unwrap(), 0.72);
        assert_eq!(interval[1].as_f64().unwrap(), 0.94);
    }
}
