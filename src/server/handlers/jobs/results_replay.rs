//! Typed publication of a sealed Decision replay run.

use super::prelude_jobs::*;
use super::*;

#[cfg(all(feature = "decide", feature = "finance"))]
pub(super) fn typed_replay_result(
    job: &eg_jobs::AnalyticsJob,
    request_ref: &str,
    run: &eg_types::decision::replay::EvaluationRun,
) -> Result<TypedJobResult, String> {
    if !run.verify() || run.run_digest.is_empty() {
        return Err("decision replay run seal is invalid".to_string());
    }
    let dataset_ref = job.input_snapshot.dataset_ref.clone();
    let run_ref = native_opaque_ref("evaluation_run", &run.run_digest);
    let request_evidence_ref = native_opaque_ref("decision_replay_request", request_ref);
    let row = BTreeMap::from([
        ("id".to_string(), serde_json::json!(run_ref)),
        ("kind".to_string(), serde_json::json!("evaluation_run")),
        ("confidence".to_string(), serde_json::json!(1.0)),
        (
            "evidence_refs".to_string(),
            serde_json::json!([request_evidence_ref.clone()]),
        ),
        ("source_refs".to_string(), serde_json::json!([dataset_ref])),
        ("proof_ids".to_string(), serde_json::json!([])),
        ("contradiction_ids".to_string(), serde_json::json!([])),
        ("run_digest".to_string(), serde_json::json!(run.run_digest)),
    ]);
    let result = TypedJobResult::new(
        [
            ("id", "string"),
            ("kind", "string"),
            ("confidence", "float64"),
            ("evidence_refs", "list<string>"),
            ("source_refs", "list<string>"),
            ("proof_ids", "list<string>"),
            ("contradiction_ids", "list<string>"),
            ("run_digest", "string"),
        ]
        .into_iter()
        .map(|(name, logical_type)| ResultColumn {
            name: name.to_string(),
            logical_type: logical_type.to_string(),
            nullable: false,
        })
        .collect(),
        vec![row],
        vec![request_evidence_ref],
        Vec::new(),
        None,
        None,
        reproducibility_manifest(&job.input_snapshot, &job.algo, &job.policy),
    )?;
    #[cfg(feature = "knowledge-batch")]
    validate_native_job_result(job, &result)?;
    Ok(result)
}
