use serde::Deserialize;

use super::*;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CertificationFaultSpec {
    schema_version: u8,
    nonce: String,
    request_id: u64,
    domain: DurabilityDomain,
    phase: MutationCommitPhase,
}

/// Abort at one exact MutationBatch boundary when the release-certification
/// process has been explicitly armed.
#[doc(hidden)]
pub fn apply_certification_fault(
    batch: &MutationBatch,
    phase: MutationCommitPhase,
) -> Result<(), String> {
    const ENV: &str = "EPISTEMIC_GRAPH_CERTIFICATION_FAULT";
    const SCHEMA_VERSION: u8 = 1;

    let Some(raw) = std::env::var_os(ENV) else {
        return Ok(());
    };
    let raw = raw
        .into_string()
        .map_err(|_| "certification fault configuration is not UTF-8".to_string())?;
    let spec: CertificationFaultSpec = serde_json::from_str(&raw)
        .map_err(|_| "certification fault configuration is invalid".to_string())?;
    if spec.schema_version != SCHEMA_VERSION
        || spec.request_id == 0
        || spec.nonce.len() != 64
        || !spec
            .nonce
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
    {
        return Err("certification fault configuration is invalid".to_string());
    }
    if spec.phase != phase
        || spec.request_id != batch.context.request_id
        || !batch
            .operations
            .iter()
            .any(|operation| operation.domain == spec.domain)
    {
        return Ok(());
    }

    eprintln!(
        "EG_CERTIFICATION_FAULT phase={:?} domain={:?} request_id={}",
        phase, spec.domain, spec.request_id
    );
    std::process::abort();
}
