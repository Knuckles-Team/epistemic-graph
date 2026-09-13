//! Private implementation module for the analytics job handler.

use super::prelude_jobs::*;
use super::prelude_server::*;
use super::prelude_std::*;
use super::*;

pub(super) fn typed_association_result(
    job: &eg_jobs::AnalyticsJob,
    rules: &[association::LabeledRule],
) -> Result<TypedJobResult, String> {
    let rows = association_rows(job, rules);
    let scores = rules
        .iter()
        .map(|rule| rule.support * rule.confidence)
        .collect::<Vec<_>>();
    let calibration = (!scores.is_empty()).then(|| {
        (
            scores.iter().copied().fold(f64::INFINITY, f64::min),
            scores.iter().copied().fold(f64::NEG_INFINITY, f64::max),
        )
    });
    let result = TypedJobResult::new(
        [
            ("id", "string", false),
            ("kind", "string", false),
            ("confidence", "float64", false),
            ("evidence_refs", "list<string>", false),
            ("source_refs", "list<string>", false),
            ("proof_ids", "list<string>", false),
            ("contradiction_ids", "list<string>", false),
            ("antecedent", "list<string>", false),
            ("consequent", "list<string>", false),
            ("support", "float64", false),
            ("lift", "float64", false),
        ]
        .into_iter()
        .map(|(name, logical_type, nullable)| ResultColumn {
            name: name.to_string(),
            logical_type: logical_type.to_string(),
            nullable,
        })
        .collect(),
        rows,
        vec![job.input_snapshot.dataset_ref.clone()],
        Vec::new(),
        (!rules.is_empty()).then(|| {
            rules.iter().map(|rule| 1.0 - rule.confidence).sum::<f64>() / rules.len() as f64
        }),
        calibration,
        reproducibility_manifest(&job.input_snapshot, &job.algo, &job.policy),
    )?;
    #[cfg(feature = "knowledge-batch")]
    validate_native_job_result(job, &result)?;
    Ok(result)
}

fn association_rows(
    job: &eg_jobs::AnalyticsJob,
    rules: &[association::LabeledRule],
) -> Vec<BTreeMap<String, serde_json::Value>> {
    rules
        .iter()
        .map(|rule| {
            let id = association_rule_id(
                &rule.antecedent,
                &rule.consequent,
                rule.support,
                rule.confidence,
                rule.lift,
            );
            BTreeMap::from([
                ("id".to_string(), serde_json::json!(id)),
                ("kind".to_string(), serde_json::json!("association_rule")),
                ("confidence".to_string(), serde_json::json!(rule.confidence)),
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
                ("antecedent".to_string(), serde_json::json!(rule.antecedent)),
                ("consequent".to_string(), serde_json::json!(rule.consequent)),
                ("support".to_string(), serde_json::json!(rule.support)),
                ("lift".to_string(), serde_json::json!(rule.lift)),
            ])
        })
        .collect::<Vec<_>>()
}
