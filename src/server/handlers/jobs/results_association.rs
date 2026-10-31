//! Private implementation module for the analytics job handler.

use super::prelude_jobs::*;
use super::prelude_std::*;
use super::*;

pub(super) fn typed_association_result(
    job: &medulla_jobs::AnalyticsJob,
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
    job: &medulla_jobs::AnalyticsJob,
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

#[cfg(test)]
mod rule_identity_tests {
    use super::association_rule_id;

    // Expected IDs were produced by executing the actual deleted Python function
    // from Knuckles-Team/agent-utilities commit 92974bae8c3ed3a270f582300b7458d4f53571c4.
    // The fixture records its source; no Rust output was used as the oracle.
    // spec: MEDULLA-DURABLE-KERNEL-R082.4
    #[test]
    fn rule_identity_matches_deleted_python_worker_golden_vectors() {
        let fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../tests/fixtures/association_rule_identity.json"
        ))
        .unwrap();
        let vectors = fixture["vectors"].as_array().unwrap();
        assert_eq!(vectors.len(), 8);
        for vector in vectors {
            let strings = |field: &str| {
                vector[field]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|value| value.as_str().unwrap().to_owned())
                    .collect::<Vec<_>>()
            };
            assert_eq!(
                association_rule_id(
                    &strings("antecedent"),
                    &strings("consequent"),
                    vector["support"].as_f64().unwrap(),
                    vector["confidence"].as_f64().unwrap(),
                    vector["lift"].as_f64().unwrap(),
                ),
                vector["id"].as_str().unwrap(),
                "{}",
                vector["name"]
            );
        }
        // Length-prefixing, ordered labels and IEEE-754 signed zero are identity.
        for (left, right) in [(2, 3), (4, 5), (6, 7)] {
            assert_ne!(vectors[left]["id"], vectors[right]["id"]);
        }
    }
}
