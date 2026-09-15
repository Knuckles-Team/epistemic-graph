//! Private implementation module for the analytics job handler.

use super::prelude_jobs::*;
use super::*;

#[cfg(feature = "knowledge-batch")]
pub(super) fn validate_native_job_result(
    job: &eg_jobs::AnalyticsJob,
    result: &TypedJobResult,
) -> Result<(), String> {
    use eg_plan::job_result_stream;

    let rows = result
        .rows
        .iter()
        .map(|row| native_knowledge_row(job, row))
        .collect::<Result<Vec<_>, _>>()?;
    let context = native_stream_context(job, result)?;
    let mut stream = job_result_stream(
        context,
        vec!["support".to_string(), "lift".to_string()],
        rows,
        256,
    )
    .map_err(|error| error.to_string())?;
    while stream
        .next_batch()
        .map_err(|error| error.to_string())?
        .is_some()
    {}
    Ok(())
}

#[cfg(feature = "knowledge-batch")]
fn native_opaque(namespace: &str, value: &str) -> Result<eg_modality::OpaqueRef, String> {
    eg_modality::OpaqueRef::new(native_opaque_ref(namespace, value))
        .map_err(|error| error.to_string())
}

#[cfg(feature = "knowledge-batch")]
fn native_knowledge_row(
    job: &eg_jobs::AnalyticsJob,
    row: &BTreeMap<String, serde_json::Value>,
) -> Result<eg_plan::KnowledgeBatchRow, String> {
    let id = row
        .get("id")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default()
        .to_string();
    let locus = native_evidence_locus(job, &id)?;
    Ok(eg_plan::KnowledgeBatchRow {
        id: id.clone(),
        kind: row
            .get("kind")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("analytics_result")
            .to_string(),
        scores: vec![
            (
                "support".to_string(),
                row.get("support")
                    .and_then(serde_json::Value::as_f64)
                    .map(|value| value as f32),
            ),
            (
                "lift".to_string(),
                row.get("lift")
                    .and_then(serde_json::Value::as_f64)
                    .map(|value| value as f32),
            ),
        ],
        confidence: row
            .get("confidence")
            .and_then(serde_json::Value::as_f64)
            .unwrap_or(0.0),
        evidence_refs: vec![locus],
        source_refs: vec![job.input_snapshot.dataset_ref.clone()],
        proof_ids: json_string_list(row.get("proof_ids")),
        contradiction_ids: json_string_list(row.get("contradiction_ids")),
        ..eg_plan::KnowledgeBatchRow::default()
    })
}

#[cfg(feature = "knowledge-batch")]
fn native_evidence_locus(
    job: &eg_jobs::AnalyticsJob,
    id: &str,
) -> Result<eg_modality::EvidenceLocus, String> {
    use eg_modality::{ArtifactId, DerivationId, EvidenceAddress, EvidenceLocusId, ResourceId};

    Ok(eg_modality::EvidenceLocus {
        id: EvidenceLocusId::from_opaque(native_opaque("locus", &format!("{}:{id}", job.job_id))?)
            .map_err(|error| error.to_string())?,
        subject: ResourceId::Artifact(
            ArtifactId::from_opaque(native_opaque("artifact", &job.input_snapshot.dataset_ref)?)
                .map_err(|error| error.to_string())?,
        ),
        address: EvidenceAddress::RowVersion {
            row_ref: native_opaque("row", id)?,
            version: job.input_snapshot.version,
        },
        policy_ref: native_opaque("policy", &job.policy.policy_fingerprint)?,
        derivation_ref: DerivationId::from_opaque(native_opaque("derivation", &job.result_ref())?)
            .map_err(|error| error.to_string())?,
    })
}

#[cfg(feature = "knowledge-batch")]
fn native_stream_context(
    job: &eg_jobs::AnalyticsJob,
    result: &TypedJobResult,
) -> Result<eg_plan::KnowledgeStreamContext, String> {
    Ok(eg_plan::KnowledgeStreamContext {
        tenant_ref: native_opaque("tenant", &job.policy.tenant)?,
        access_policy_ref: native_opaque("policy", &job.policy.policy_fingerprint)?,
        placement_ref: native_opaque("placement", &job.input_snapshot.graph)?,
        snapshot_ref: eg_modality::OpaqueRef::new(job.input_snapshot.dataset_ref.clone())
            .map_err(|error| error.to_string())?,
        query_ref: native_opaque("query", &job.algo.params_digest)?,
        derivation_ref: native_opaque("derivation", &job.result_ref())?,
        evidence_set_ref: native_opaque("evidence_set", &result.dataset_ref)?,
    })
}

#[cfg(feature = "knowledge-batch")]
pub(super) fn json_string_list(value: Option<&serde_json::Value>) -> Vec<String> {
    value
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(serde_json::Value::as_str)
        .map(str::to_string)
        .collect()
}
