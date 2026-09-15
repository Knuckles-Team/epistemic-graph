//! Private implementation module for the analytics job handler.

use super::prelude_jobs::*;
use super::prelude_server::*;
use super::prelude_std::*;
use super::*;

#[cfg(feature = "program-optimization")]
pub(super) fn typed_program_result(
    job: &eg_jobs::AnalyticsJob,
    optimization: &eg_program::OptimizationResult,
    promotion_identity: Option<&ProgramRevisionIdentity>,
    expected_policy: &PolicyEnvelope,
) -> Result<TypedJobResult, String> {
    let mut rows = optimization
        .candidates
        .iter()
        .map(|candidate| program_candidate_row(job, optimization, candidate, promotion_identity))
        .collect::<Result<Vec<_>, String>>()?;
    rows.extend(optimization.plans.iter().flat_map(|plan| {
        plan.steps
            .iter()
            .map(|step| program_plan_step_row(job, optimization, plan, step))
    }));
    let (confidence, calibration) = program_score_summary(optimization);
    let result = TypedJobResult::new(
        program_result_columns(),
        rows,
        vec![job.input_snapshot.dataset_ref.clone()],
        Vec::new(),
        confidence,
        calibration,
        reproducibility_manifest(&job.input_snapshot, &job.algo, &job.policy),
    )?;
    validate_program_result_privacy(&result, expected_policy)?;
    #[cfg(feature = "knowledge-batch")]
    validate_native_job_result(job, &result)?;
    Ok(result)
}

#[cfg(feature = "program-optimization")]
fn program_candidate_row(
    job: &eg_jobs::AnalyticsJob,
    optimization: &eg_program::OptimizationResult,
    candidate: &eg_program::ProgramCandidate,
    promotion_identity: Option<&ProgramRevisionIdentity>,
) -> Result<BTreeMap<String, serde_json::Value>, String> {
    let evidence_refs = program_evidence_refs(job, candidate);
    let confidence = program_candidate_confidence(candidate);
    let promotion_value = program_promotion_value(optimization, candidate, promotion_identity)?;
    Ok(BTreeMap::from([
        (
            "id".to_string(),
            serde_json::json!(candidate.candidate_ref.as_str()),
        ),
        ("kind".to_string(), serde_json::json!("program_candidate")),
        ("confidence".to_string(), serde_json::json!(confidence)),
        (
            "evidence_refs".to_string(),
            serde_json::json!(evidence_refs),
        ),
        (
            "source_refs".to_string(),
            serde_json::json!([job.input_snapshot.dataset_ref.clone()]),
        ),
        ("proof_ids".to_string(), serde_json::json!([])),
        ("contradiction_ids".to_string(), serde_json::json!([])),
        (
            "program_ref".to_string(),
            serde_json::json!(candidate.program_ref.as_str()),
        ),
        (
            "optimizer".to_string(),
            serde_json::json!(candidate.optimizer.as_str()),
        ),
        (
            "execution".to_string(),
            serde_json::json!(candidate.optimizer.execution().as_str()),
        ),
        (
            "candidate_role".to_string(),
            serde_json::json!(candidate.role.as_str()),
        ),
        (
            "demonstration_refs".to_string(),
            program_opaque_ref_list(&candidate.demonstration_refs),
        ),
        (
            "artifact_refs".to_string(),
            program_opaque_ref_list(&candidate.artifact_refs),
        ),
        (
            "composition_refs".to_string(),
            program_opaque_ref_list(&candidate.composition_refs),
        ),
        (
            "instruction_ref".to_string(),
            program_optional_opaque_ref(candidate.instruction_ref.as_ref()),
        ),
        (
            "tool_policy_ref".to_string(),
            program_optional_opaque_ref(candidate.tool_policy_ref.as_ref()),
        ),
        (
            "model_profile_ref".to_string(),
            program_optional_opaque_ref(candidate.model_profile_ref.as_ref()),
        ),
        ("policy".to_string(), program_policy_value(candidate)?),
        (
            "modalities".to_string(),
            program_modalities(&candidate.modalities),
        ),
        ("plan_ref".to_string(), serde_json::Value::Null),
        ("plan_step_kinds".to_string(), serde_json::json!([])),
        ("plan_executors".to_string(), serde_json::json!([])),
        ("plan_input_refs".to_string(), serde_json::json!([])),
        ("plan_output_refs".to_string(), serde_json::json!([])),
        ("plan_depends_on".to_string(), serde_json::json!([])),
        ("max_operations".to_string(), serde_json::Value::Null),
        (
            "selected".to_string(),
            serde_json::json!(
                optimization.selected_candidate_ref.as_ref() == Some(&candidate.candidate_ref)
            ),
        ),
        ("promotion_identity".to_string(), promotion_value),
    ]))
}

#[cfg(feature = "program-optimization")]
fn program_evidence_refs(
    job: &eg_jobs::AnalyticsJob,
    candidate: &eg_program::ProgramCandidate,
) -> Vec<String> {
    candidate
        .evaluation
        .as_ref()
        .map(|evaluation| {
            evaluation
                .evidence_refs
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
        })
        .filter(|references| !references.is_empty())
        .unwrap_or_else(|| vec![job.input_snapshot.dataset_ref.clone()])
}

#[cfg(feature = "program-optimization")]
fn program_candidate_confidence(candidate: &eg_program::ProgramCandidate) -> f64 {
    candidate
        .evaluation
        .as_ref()
        .map(|evaluation| evaluation.aggregate_score)
        .unwrap_or(0.0)
}

#[cfg(feature = "program-optimization")]
fn program_promotion_value(
    optimization: &eg_program::OptimizationResult,
    candidate: &eg_program::ProgramCandidate,
    promotion_identity: Option<&ProgramRevisionIdentity>,
) -> Result<serde_json::Value, String> {
    if optimization.selected_candidate_ref.as_ref() != Some(&candidate.candidate_ref) {
        return Ok(serde_json::Value::Null);
    }
    promotion_identity
        .map(serde_json::to_value)
        .transpose()
        .map_err(|error| error.to_string())
        .map(|value| value.unwrap_or(serde_json::Value::Null))
}

#[cfg(feature = "program-optimization")]
fn program_opaque_ref_list(refs: &[OpaqueRef]) -> serde_json::Value {
    serde_json::json!(refs.iter().map(ToString::to_string).collect::<Vec<_>>())
}

#[cfg(feature = "program-optimization")]
fn program_optional_opaque_ref(reference: Option<&OpaqueRef>) -> serde_json::Value {
    reference.map_or(serde_json::Value::Null, |value| {
        serde_json::json!(value.as_str())
    })
}

#[cfg(feature = "program-optimization")]
fn program_modalities(
    modalities: &std::collections::BTreeSet<ProgramModality>,
) -> serde_json::Value {
    serde_json::json!(modalities
        .iter()
        .map(|modality| modality.as_str())
        .collect::<Vec<_>>())
}

#[cfg(feature = "program-optimization")]
fn program_policy_value(
    candidate: &eg_program::ProgramCandidate,
) -> Result<serde_json::Value, String> {
    serde_json::to_value(&candidate.policy).map_err(|error| error.to_string())
}

#[cfg(feature = "program-optimization")]
fn program_plan_step_row(
    job: &eg_jobs::AnalyticsJob,
    optimization: &eg_program::OptimizationResult,
    plan: &eg_program::OptimizationPlan,
    step: &eg_program::PlanStep,
) -> BTreeMap<String, serde_json::Value> {
    BTreeMap::from([
        ("id".to_string(), serde_json::json!(step.step_ref.as_str())),
        (
            "kind".to_string(),
            serde_json::json!("program_optimization_plan_step"),
        ),
        ("confidence".to_string(), serde_json::json!(0.0)),
        (
            "evidence_refs".to_string(),
            serde_json::json!([job.input_snapshot.dataset_ref.clone()]),
        ),
        (
            "source_refs".to_string(),
            serde_json::json!([job.input_snapshot.dataset_ref.clone()]),
        ),
        ("proof_ids".to_string(), serde_json::json!([])),
        ("contradiction_ids".to_string(), serde_json::json!([])),
        (
            "program_ref".to_string(),
            serde_json::json!(optimization.program_ref.as_str()),
        ),
        (
            "optimizer".to_string(),
            serde_json::json!(plan.optimizer.as_str()),
        ),
        (
            "execution".to_string(),
            serde_json::json!(plan.optimizer.execution().as_str()),
        ),
        ("candidate_role".to_string(), serde_json::Value::Null),
        ("demonstration_refs".to_string(), serde_json::json!([])),
        ("artifact_refs".to_string(), serde_json::json!([])),
        ("composition_refs".to_string(), serde_json::json!([])),
        ("instruction_ref".to_string(), serde_json::Value::Null),
        ("tool_policy_ref".to_string(), serde_json::Value::Null),
        ("model_profile_ref".to_string(), serde_json::Value::Null),
        ("policy".to_string(), serde_json::Value::Null),
        (
            "modalities".to_string(),
            serde_json::json!(step
                .modalities
                .iter()
                .map(|modality| modality.as_str())
                .collect::<Vec<_>>()),
        ),
        (
            "plan_ref".to_string(),
            serde_json::json!(plan.plan_ref.as_str()),
        ),
        (
            "plan_step_kinds".to_string(),
            serde_json::json!([step.kind.as_str()]),
        ),
        (
            "plan_executors".to_string(),
            serde_json::json!([step.executor.as_str()]),
        ),
        (
            "plan_input_refs".to_string(),
            serde_json::json!(step
                .input_refs
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()),
        ),
        (
            "plan_output_refs".to_string(),
            serde_json::json!(step
                .output_refs
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()),
        ),
        (
            "plan_depends_on".to_string(),
            serde_json::json!(step
                .depends_on
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()),
        ),
        (
            "max_operations".to_string(),
            serde_json::json!(step.max_operations),
        ),
        ("selected".to_string(), serde_json::json!(false)),
        ("promotion_identity".to_string(), serde_json::Value::Null),
    ])
}

#[cfg(feature = "program-optimization")]
fn program_score_summary(
    optimization: &eg_program::OptimizationResult,
) -> (Option<f64>, Option<(f64, f64)>) {
    let scores = optimization
        .candidates
        .iter()
        .filter_map(|candidate| {
            candidate
                .evaluation
                .as_ref()
                .map(|evaluation| evaluation.aggregate_score)
        })
        .collect::<Vec<_>>();
    if scores.is_empty() {
        return (None, None);
    }
    let calibration = (
        scores.iter().copied().fold(f64::INFINITY, f64::min),
        scores.iter().copied().fold(f64::NEG_INFINITY, f64::max),
    );
    let confidence =
        Some(scores.iter().map(|score| 1.0 - score).sum::<f64>() / scores.len() as f64);
    (confidence, Some(calibration))
}

#[cfg(feature = "program-optimization")]
fn program_result_columns() -> Vec<ResultColumn> {
    [
        ("id", "string", false),
        ("kind", "string", false),
        ("confidence", "float64", false),
        ("evidence_refs", "list<string>", false),
        ("source_refs", "list<string>", false),
        ("proof_ids", "list<string>", false),
        ("contradiction_ids", "list<string>", false),
        ("program_ref", "string", false),
        ("optimizer", "string", false),
        ("execution", "string", false),
        ("candidate_role", "string", true),
        ("demonstration_refs", "list<string>", false),
        ("artifact_refs", "list<string>", false),
        ("composition_refs", "list<string>", false),
        ("instruction_ref", "string", true),
        ("tool_policy_ref", "string", true),
        ("model_profile_ref", "string", true),
        ("policy", "object", true),
        ("modalities", "list<string>", false),
        ("plan_ref", "string", true),
        ("plan_step_kinds", "list<string>", false),
        ("plan_executors", "list<string>", false),
        ("plan_input_refs", "list<string>", false),
        ("plan_output_refs", "list<string>", false),
        ("plan_depends_on", "list<string>", false),
        ("max_operations", "uint64", true),
        ("selected", "bool", false),
        ("promotion_identity", "object", true),
    ]
    .into_iter()
    .map(|(name, logical_type, nullable)| ResultColumn {
        name: name.to_string(),
        logical_type: logical_type.to_string(),
        nullable,
    })
    .collect()
}

#[cfg(test)]
mod privacy_tests {
    use super::super::{native_opaque_ref, opaque_placement_value, opaque_worker_capability};

    #[test]
    fn job_placement_values_are_opaque_and_idempotent() {
        let capability = opaque_worker_capability("accelerator");
        let pool = opaque_worker_capability("pool:interactive");
        let region = opaque_worker_capability("region:zone-a");
        assert!(capability.starts_with("eg:job_capability:"));
        assert!(pool.starts_with("pool:eg:job_pool:"));
        assert!(region.starts_with("region:eg:job_region:"));
        assert!(!capability.contains("accelerator"));
        assert_eq!(
            opaque_placement_value("job_capability", &capability),
            capability
        );
    }

    #[test]
    fn authorized_graph_comparison_uses_the_persisted_graph_reference() {
        let stored = native_opaque_ref("graph", "authorized-graph");
        assert_eq!(stored, native_opaque_ref("graph", "authorized-graph"));
        assert_ne!(stored, "authorized-graph");
    }

    #[cfg(feature = "program-optimization")]
    #[test]
    fn program_result_producer_and_privacy_validator_bind_policy() {
        use super::super::validate_program_result_privacy;
        use super::typed_program_result;
        use eg_jobs::model::{
            AlgoVersion, AnalyticsJob, InputSnapshotHandle, JobPolicy, JobState, RetryPolicy,
        };
        use eg_modality::{Classification, OpaqueRef, PolicyEnvelope};
        use eg_program::{
            CandidateRole, OptimizationCheckpoint, OptimizationResult, OptimizerKind,
            ProgramCandidate, ProgramModality, PROGRAM_SCHEMA_VERSION,
        };
        use sha2::{Digest, Sha256};
        use std::collections::BTreeSet;

        let opaque = |namespace: &str, value: &str| {
            OpaqueRef::new(native_opaque_ref(namespace, value)).expect("opaque test reference")
        };
        let policy = PolicyEnvelope {
            tenant_ref: opaque("tenant", "program-result-tenant"),
            access_policy_ref: opaque("policy", "program-result-access"),
            classification: Classification::Internal,
            retention_policy_ref: opaque("retention", "program-result-retention"),
            deletion_policy_ref: opaque("deletion", "program-result-deletion"),
            legal_hold_ref: None,
            purpose_refs: vec![opaque("purpose", "program-result-purpose")],
        };
        let program_ref = opaque("program", "program-result-program");
        let content_digest = hex::encode(Sha256::digest(b"program-result-candidate"));
        let candidate = ProgramCandidate {
            candidate_ref: OpaqueRef::scoped("program_candidate", &content_digest).unwrap(),
            program_ref: program_ref.clone(),
            optimizer: OptimizerKind::LabeledFewShot,
            role: CandidateRole::Proposal,
            demonstration_refs: vec![opaque("example", "program-result-example")],
            artifact_refs: Vec::new(),
            composition_refs: Vec::new(),
            instruction_ref: None,
            tool_policy_ref: None,
            model_profile_ref: None,
            modalities: BTreeSet::from([ProgramModality::Text]),
            policy: policy.clone(),
            content_digest,
            evaluation: None,
        };
        let candidate_ref = candidate.candidate_ref.clone();
        let optimization = OptimizationResult {
            schema_version: PROGRAM_SCHEMA_VERSION,
            request_ref: opaque("optimization_request", "program-result-request"),
            program_ref,
            optimizer: OptimizerKind::LabeledFewShot,
            candidates: vec![candidate],
            plans: Vec::new(),
            selected_candidate_ref: None,
            promoted: false,
            checkpoint: OptimizationCheckpoint {
                request_ref: opaque("optimization_request", "program-result-request"),
                corpus_ref: opaque("corpus", "program-result-corpus"),
                snapshot_version: 1,
                optimizer: OptimizerKind::LabeledFewShot,
                generated_candidates: 1,
                generated_plans: 0,
                planned_steps: 0,
                evaluated_candidates: 0,
            },
        };
        let dataset_ref = native_opaque_ref("dataset", "program-result-input");
        let job = AnalyticsJob {
            job_id: "program-result-job".to_string(),
            input_snapshot: InputSnapshotHandle::new("eg:graph:program-result", 1).with_dataset(
                dataset_ref.as_str().to_string(),
                hex::encode(Sha256::digest(b"input")),
            ),
            policy: JobPolicy::default(),
            algo: AlgoVersion {
                family: "program.optimization".to_string(),
                algorithm: "labeled_few_shot".to_string(),
                params_digest: "params".to_string(),
                code_version: "test".to_string(),
                env_version: "test".to_string(),
            },
            input_payload: None,
            retry: RetryPolicy::default(),
            state: JobState::Submitted,
            cancel_requested: false,
            lease_epoch: 0,
            lease: None,
            last_worker_ref: String::new(),
            not_before_ms: 0,
            output: None,
            created_at_ms: 0,
            updated_at_ms: 0,
        };

        let result = typed_program_result(&job, &optimization, None, &policy)
            .expect("the producer output passes its privacy validator");
        assert_eq!(
            result.rows[0].get("id").and_then(serde_json::Value::as_str),
            Some(candidate_ref.as_str())
        );
        assert_eq!(
            result.rows[0].get("policy"),
            Some(&serde_json::to_value(&policy).unwrap())
        );
        validate_program_result_privacy(&result, &policy)
            .expect("the emitted policy remains bound at the validator");

        let mut tampered = result;
        let mut tampered_policy = serde_json::to_value(&policy).unwrap();
        tampered_policy["access_policy_ref"] =
            serde_json::json!(opaque("policy", "program-result-other-access").as_str());
        tampered.rows[0].insert("policy".to_string(), tampered_policy);
        assert!(validate_program_result_privacy(&tampered, &policy).is_err());
    }
}
