//! Private implementation module for the analytics job handler.

use super::prelude_jobs::*;
use super::prelude_server::*;
use super::prelude_std::*;
use super::*;

pub(super) fn typed_result_from_wire(result: JobResult) -> TypedJobResult {
    TypedJobResult {
        schema_version: result.schema_version,
        dataset_ref: result.dataset_ref,
        content_digest: result.content_digest,
        schema: result
            .schema
            .into_iter()
            .map(|column| ResultColumn {
                name: column.name,
                logical_type: column.logical_type,
                nullable: column.nullable,
            })
            .collect(),
        rows: result.rows,
        evidence_refs: result.evidence_refs,
        counterexample_refs: result.counterexample_refs,
        uncertainty: result.uncertainty,
        calibration: result.calibration,
        reproducibility: ReproducibilityManifest {
            input_dataset_ref: result.reproducibility.input_dataset_ref,
            input_content_digest: result.reproducibility.input_content_digest,
            input_snapshot_version: result.reproducibility.input_snapshot_version,
            algorithm_ref: result.reproducibility.algorithm_ref,
            params_digest: result.reproducibility.params_digest,
            implementation_version: result.reproducibility.implementation_version,
            environment_version: result.reproducibility.environment_version,
            policy_fingerprint: result.reproducibility.policy_fingerprint,
        },
    }
}

pub(super) fn is_opaque_result_ref(value: &str) -> bool {
    let mut parts = value.split(':');
    let scheme = parts.next();
    let namespace = parts.next();
    let digest = parts.next();
    scheme == Some("eg")
        && namespace.is_some_and(|value| {
            !value.is_empty()
                && value
                    .chars()
                    .all(|character| character.is_ascii_alphanumeric() || character == '_')
        })
        && digest.is_some_and(|value| {
            matches!(value.len(), 32 | 64)
                && value.chars().all(|character| character.is_ascii_hexdigit())
        })
        && parts.next().is_none()
}

pub(super) fn json_refs(value: Option<&serde_json::Value>, allow_empty: bool) -> bool {
    value
        .and_then(serde_json::Value::as_array)
        .is_some_and(|values| {
            (allow_empty || !values.is_empty())
                && values
                    .iter()
                    .all(|value| value.as_str().is_some_and(is_opaque_result_ref))
        })
}

pub(super) fn association_rule_id(
    antecedent: &[String],
    consequent: &[String],
    support: f64,
    confidence: f64,
    lift: f64,
) -> String {
    use sha2::{Digest, Sha256};

    let mut digest = Sha256::new();
    digest.update(b"eg-jobs.association-rule.v1\0");
    for values in [antecedent, consequent] {
        digest.update((values.len() as u64).to_le_bytes());
        for value in values {
            digest.update((value.len() as u64).to_le_bytes());
            digest.update(value.as_bytes());
        }
    }
    for value in [support, confidence, lift] {
        digest.update(value.to_bits().to_le_bytes());
    }
    format!("eg:rule:{}", hex::encode(digest.finalize()))
}

pub(super) fn association_rule_id_from_row(
    row: &BTreeMap<String, serde_json::Value>,
) -> Option<String> {
    let strings = |field: &str| {
        row.get(field)?
            .as_array()?
            .iter()
            .map(|value| value.as_str().map(str::to_string))
            .collect::<Option<Vec<_>>>()
    };
    Some(association_rule_id(
        &strings("antecedent")?,
        &strings("consequent")?,
        row.get("support")?.as_f64()?,
        row.get("confidence")?.as_f64()?,
        row.get("lift")?.as_f64()?,
    ))
}

/// The only shipped remote kernel is association mining. Fail closed on extra
/// free-text fields so a compromised worker cannot use result rows as a durable
/// PII, prompt, endpoint, or local-path channel.
pub(super) fn validate_remote_result_privacy(result: &TypedJobResult) -> Result<(), String> {
    let expected = std::collections::BTreeSet::from([
        "id",
        "kind",
        "confidence",
        "evidence_refs",
        "source_refs",
        "proof_ids",
        "contradiction_ids",
        "antecedent",
        "consequent",
        "support",
        "lift",
    ]);
    let actual = result
        .schema
        .iter()
        .map(|column| column.name.as_str())
        .collect::<std::collections::BTreeSet<_>>();
    let valid_columns = result.schema.iter().all(|column| {
        !column.nullable
            && matches!(
                (column.name.as_str(), column.logical_type.as_str()),
                ("id" | "kind", "string")
                    | ("confidence" | "support" | "lift", "float64")
                    | (
                        "evidence_refs"
                            | "source_refs"
                            | "proof_ids"
                            | "contradiction_ids"
                            | "antecedent"
                            | "consequent",
                        "list<string>"
                    )
            )
    });
    if actual != expected
        || !valid_columns
        || !result
            .evidence_refs
            .iter()
            .all(|value| is_opaque_result_ref(value))
        || !result
            .counterexample_refs
            .iter()
            .all(|value| is_opaque_result_ref(value))
    {
        return Err("remote analytics result schema/references are not governed".to_string());
    }
    for row in &result.rows {
        let expected_rule_id = association_rule_id_from_row(row);
        if association_row_identity_invalid(row, &expected, expected_rule_id.as_deref())
            || association_row_metrics_invalid(row)
        {
            return Err("remote analytics result contains non-governed row data".to_string());
        }
    }
    Ok(())
}

/// The row-field-set, `kind`, `id`, and evidence/source-ref shape a governed
/// `association_rule` row must have.
pub(super) fn association_row_identity_invalid(
    row: &BTreeMap<String, serde_json::Value>,
    expected: &std::collections::BTreeSet<&str>,
    expected_rule_id: Option<&str>,
) -> bool {
    row.keys()
        .map(String::as_str)
        .collect::<std::collections::BTreeSet<_>>()
        != *expected
        || row.get("kind").and_then(serde_json::Value::as_str) != Some("association_rule")
        || !row
            .get("id")
            .and_then(serde_json::Value::as_str)
            .is_some_and(|value| value.starts_with("eg:rule:") && is_opaque_result_ref(value))
        || row.get("id").and_then(serde_json::Value::as_str) != expected_rule_id
        || !json_refs(row.get("evidence_refs"), false)
        || !json_refs(row.get("source_refs"), false)
}

/// The proof/contradiction/antecedent/consequent refs and the
/// confidence/support/lift numeric bounds a governed `association_rule` row
/// must have.
pub(super) fn association_row_metrics_invalid(row: &BTreeMap<String, serde_json::Value>) -> bool {
    !json_refs(row.get("proof_ids"), true)
        || !json_refs(row.get("contradiction_ids"), true)
        || !json_refs(row.get("antecedent"), false)
        || !json_refs(row.get("consequent"), false)
        || !row
            .get("confidence")
            .and_then(serde_json::Value::as_f64)
            .is_some_and(|value| (0.0..=1.0).contains(&value))
        || !row
            .get("support")
            .and_then(serde_json::Value::as_f64)
            .is_some_and(|value| (0.0..=1.0).contains(&value))
        || !row
            .get("lift")
            .and_then(serde_json::Value::as_f64)
            .is_some_and(|value| value.is_finite() && value >= 0.0)
}
