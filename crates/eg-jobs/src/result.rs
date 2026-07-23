//! Durable, typed KnowledgeBatch-shaped analytics results.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

const MAX_RESULT_COLUMNS: usize = 4_096;
const MAX_RESULT_ROWS: usize = 100_000;
const MAX_RESULT_REFS: usize = 100_000;
const MAX_RESULT_ITEMS: usize = 1_000_000;
const MAX_RESULT_BYTES: usize = 64 * 1024 * 1024;
const MAX_RESULT_STRING_BYTES: usize = 64 * 1024;
const MAX_RESULT_DEPTH: usize = 64;

fn account(items: &mut usize, bytes: &mut usize, added_bytes: usize) -> Result<(), String> {
    *items = items
        .checked_add(1)
        .filter(|count| *count <= MAX_RESULT_ITEMS)
        .ok_or_else(|| "typed job result exceeds structural limits".to_string())?;
    *bytes = bytes
        .checked_add(added_bytes)
        .filter(|count| *count <= MAX_RESULT_BYTES)
        .ok_or_else(|| "typed job result exceeds structural limits".to_string())?;
    Ok(())
}

fn account_string(value: &str, items: &mut usize, bytes: &mut usize) -> Result<(), String> {
    if value.len() > MAX_RESULT_STRING_BYTES {
        return Err("typed job result string exceeds limits".to_string());
    }
    account(items, bytes, value.len())
}

fn account_json(
    value: &serde_json::Value,
    depth: usize,
    items: &mut usize,
    bytes: &mut usize,
) -> Result<(), String> {
    if depth > MAX_RESULT_DEPTH {
        return Err("typed job result nesting exceeds limits".to_string());
    }
    match value {
        serde_json::Value::Null | serde_json::Value::Bool(_) => account(items, bytes, 1),
        serde_json::Value::Number(_) => account(items, bytes, 16),
        serde_json::Value::String(value) => account_string(value, items, bytes),
        serde_json::Value::Array(values) => {
            account(items, bytes, 0)?;
            for value in values {
                account_json(value, depth + 1, items, bytes)?;
            }
            Ok(())
        }
        serde_json::Value::Object(values) => {
            account(items, bytes, 0)?;
            for (key, value) in values {
                account_string(key, items, bytes)?;
                account_json(value, depth + 1, items, bytes)?;
            }
            Ok(())
        }
    }
}

fn preflight_result(
    schema: &[ResultColumn],
    rows: &[BTreeMap<String, serde_json::Value>],
    evidence_refs: &[String],
    counterexample_refs: &[String],
    reproducibility: &ReproducibilityManifest,
) -> Result<(), String> {
    if schema.len() > MAX_RESULT_COLUMNS
        || rows.len() > MAX_RESULT_ROWS
        || evidence_refs.len() > MAX_RESULT_REFS
        || counterexample_refs.len() > MAX_RESULT_REFS
    {
        return Err("typed job result exceeds collection limits".to_string());
    }
    let mut items = 0usize;
    let mut bytes = 0usize;
    for column in schema {
        account_string(&column.name, &mut items, &mut bytes)?;
        account_string(&column.logical_type, &mut items, &mut bytes)?;
    }
    for row in rows {
        account(&mut items, &mut bytes, 0)?;
        for (key, value) in row {
            account_string(key, &mut items, &mut bytes)?;
            account_json(value, 1, &mut items, &mut bytes)?;
        }
    }
    for reference in evidence_refs.iter().chain(counterexample_refs) {
        account_string(reference, &mut items, &mut bytes)?;
    }
    for value in [
        reproducibility.input_dataset_ref.as_str(),
        reproducibility.input_content_digest.as_str(),
        reproducibility.algorithm_ref.as_str(),
        reproducibility.params_digest.as_str(),
        reproducibility.implementation_version.as_str(),
        reproducibility.environment_version.as_str(),
        reproducibility.policy_fingerprint.as_str(),
    ] {
        account_string(value, &mut items, &mut bytes)?;
    }
    Ok(())
}

/// One typed column in the portable job result schema.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResultColumn {
    pub name: String,
    pub logical_type: String,
    pub nullable: bool,
}

/// Reproducibility and lineage for one immutable result dataset.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReproducibilityManifest {
    pub input_dataset_ref: String,
    pub input_content_digest: String,
    pub input_snapshot_version: u64,
    pub algorithm_ref: String,
    pub params_digest: String,
    pub implementation_version: String,
    pub environment_version: String,
    pub policy_fingerprint: String,
}

/// A versioned result dataset whose rows map losslessly to KnowledgeBatch fields.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TypedJobResult {
    pub schema_version: u16,
    pub dataset_ref: String,
    pub content_digest: String,
    pub schema: Vec<ResultColumn>,
    pub rows: Vec<BTreeMap<String, serde_json::Value>>,
    pub evidence_refs: Vec<String>,
    pub counterexample_refs: Vec<String>,
    pub uncertainty: Option<f64>,
    pub calibration: Option<(f64, f64)>,
    pub reproducibility: ReproducibilityManifest,
}

impl TypedJobResult {
    pub const SCHEMA_VERSION: u16 = 1;

    pub fn new(
        schema: Vec<ResultColumn>,
        rows: Vec<BTreeMap<String, serde_json::Value>>,
        evidence_refs: Vec<String>,
        counterexample_refs: Vec<String>,
        uncertainty: Option<f64>,
        calibration: Option<(f64, f64)>,
        reproducibility: ReproducibilityManifest,
    ) -> Result<Self, String> {
        preflight_result(
            &schema,
            &rows,
            &evidence_refs,
            &counterexample_refs,
            &reproducibility,
        )?;
        let content_digest = content_digest(&schema, &rows)?;
        Ok(Self {
            schema_version: Self::SCHEMA_VERSION,
            dataset_ref: format!("eg:knowledge_batch:{content_digest}"),
            content_digest,
            schema,
            rows,
            evidence_refs,
            counterexample_refs,
            uncertainty,
            calibration,
            reproducibility,
        })
    }

    pub fn validate(&self) -> Result<(), String> {
        preflight_result(
            &self.schema,
            &self.rows,
            &self.evidence_refs,
            &self.counterexample_refs,
            &self.reproducibility,
        )?;
        if self.schema_version != Self::SCHEMA_VERSION {
            return Err("typed job result requires a supported schema".to_string());
        }
        let names: std::collections::BTreeSet<_> = self
            .schema
            .iter()
            .map(|column| column.name.as_str())
            .collect();
        if names.len() != self.schema.len() {
            return Err("typed job result schema contains duplicate columns".to_string());
        }
        for required in [
            "id",
            "kind",
            "confidence",
            "evidence_refs",
            "source_refs",
            "proof_ids",
            "contradiction_ids",
        ] {
            if !names.contains(required) {
                return Err(format!(
                    "typed job result is missing KnowledgeBatch column {required}"
                ));
            }
            if self.rows.iter().any(|row| !row.contains_key(required)) {
                return Err(format!(
                    "typed job result row is missing KnowledgeBatch field {required}"
                ));
            }
        }
        if self.evidence_refs.is_empty() {
            return Err("typed job result requires at least one evidence reference".to_string());
        }
        if !self.uncertainty.map_or(true, |value| {
            value.is_finite() && (0.0..=1.0).contains(&value)
        }) || !self.calibration.map_or(true, |(lower, upper)| {
            lower.is_finite()
                && upper.is_finite()
                && (0.0..=1.0).contains(&lower)
                && (0.0..=1.0).contains(&upper)
                && lower <= upper
        }) {
            return Err("typed job result uncertainty/calibration is invalid".to_string());
        }
        for row in &self.rows {
            let id = row.get("id").and_then(serde_json::Value::as_str);
            let confidence = row.get("confidence").and_then(serde_json::Value::as_f64);
            let evidence = row
                .get("evidence_refs")
                .and_then(serde_json::Value::as_array);
            let sources = row.get("source_refs").and_then(serde_json::Value::as_array);
            if !id.is_some_and(|value| !value.is_empty())
                || !confidence.is_some_and(|value| (0.0..=1.0).contains(&value))
                || !evidence.is_some_and(|values| !values.is_empty())
                || !sources.is_some_and(|values| !values.is_empty())
            {
                return Err(
                    "typed job result rows require an id, bounded confidence, evidence and sources"
                        .to_string(),
                );
            }
        }
        let expected_digest = content_digest(&self.schema, &self.rows)?;
        if expected_digest != self.content_digest
            || self.dataset_ref != format!("eg:knowledge_batch:{expected_digest}")
        {
            return Err("typed job result content identity does not match its rows".to_string());
        }
        Ok(())
    }
}

fn content_digest(
    schema: &[ResultColumn],
    rows: &[BTreeMap<String, serde_json::Value>],
) -> Result<String, String> {
    let schema_bytes = rmp_serde::to_vec_named(schema).map_err(|e| e.to_string())?;
    let row_bytes = rmp_serde::to_vec_named(rows).map_err(|e| e.to_string())?;
    if !matches!(
        schema_bytes.len().checked_add(row_bytes.len()),
        Some(bytes) if bytes <= MAX_RESULT_BYTES
    ) {
        return Err("typed job result exceeds encoded size limits".to_string());
    }
    let mut digest = Sha256::new();
    digest.update(b"eg-jobs.knowledge-batch.v1\0");
    digest.update(&schema_bytes);
    digest.update([0]);
    digest.update(&row_bytes);
    Ok(hex::encode(digest.finalize()))
}
