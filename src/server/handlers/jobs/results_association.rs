//! Private implementation module for the analytics job handler.

use super::prelude_jobs::*;
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

#[cfg(test)]
mod rule_identity_tests {
    use super::association_rule_id;

    // Expected IDs were produced by executing the actual deleted Python function
    // from Knuckles-Team/agent-utilities commit 92974bae8c3ed3a270f582300b7458d4f53571c4.
    // The fixture records its source; no Rust output was used as the oracle.
    // spec: EG-DURABLE-KERNEL-R082.4
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

#[cfg(test)]
mod staged_result_tests {
    use super::*;
    use sha2::{Digest, Sha256};

    fn job() -> eg_jobs::AnalyticsJob {
        let input_snapshot = InputSnapshotHandle::new("g1", 7).with_dataset(
            "eg:job_input:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
        );
        let algo = AlgoVersion {
            family: "mining.association".into(),
            algorithm: "fpgrowth".into(),
            params_digest: "cafe".into(),
            code_version: "code-1".into(),
            env_version: "env-1".into(),
        };
        let mut policy = JobPolicy::default();
        policy.policy_fingerprint = "policy-fp".into();
        eg_jobs::AnalyticsJob {
            job_id: "job-0000000000000001".into(),
            state: JobState::Running {
                checkpoint: Checkpoint {
                    progress: 0.1,
                    stage: "mining".into(),
                    state_blob: None,
                    updated_at_ms: 0,
                },
            },
            input_snapshot,
            policy,
            algo,
            input_payload: None,
            retry: Default::default(),
            cancel_requested: false,
            lease_epoch: 0,
            lease: None,
            last_worker_ref: String::new(),
            not_before_ms: 0,
            output: None,
            created_at_ms: 0,
            updated_at_ms: 0,
        }
    }

    // spec: EG-DURABLE-KERNEL-R082.5
    #[test]
    fn staged_result_has_the_eleven_column_knowledge_batch_shape() {
        let job = job();
        let rules = vec![
            association::LabeledRule {
                antecedent: vec!["a".into()],
                consequent: vec!["b".into()],
                support: 0.5,
                confidence: 0.8,
                lift: 1.2,
            },
            association::LabeledRule {
                antecedent: vec!["b".into()],
                consequent: vec!["a".into()],
                support: 0.25,
                confidence: 0.6,
                lift: 1.1,
            },
        ];
        let result = typed_association_result(&job, &rules).unwrap();
        let columns: Vec<&str> = result.schema.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(
            columns,
            [
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
                "lift"
            ]
        );
        let input_ref = job.input_snapshot.dataset_ref.clone();
        assert_eq!(result.rows.len(), 2);
        for row in &result.rows {
            assert_eq!(row["evidence_refs"], serde_json::json!([input_ref]));
            assert_eq!(row["source_refs"], serde_json::json!([input_ref]));
        }
        // Independent recomputation of the content digest over schema and rows.
        let mut digest = Sha256::new();
        digest.update(b"eg-jobs.knowledge-batch.v1\0");
        digest.update(rmp_serde::to_vec_named(&result.schema).unwrap());
        digest.update([0]);
        digest.update(rmp_serde::to_vec_named(&result.rows).unwrap());
        let expected = hex::encode(digest.finalize());
        assert_eq!(result.content_digest, expected);
        assert_eq!(result.dataset_ref, format!("eg:knowledge_batch:{expected}"));
        assert!((result.uncertainty.unwrap() - ((0.2 + 0.4) / 2.0)).abs() < 1e-12);
        assert_eq!(result.calibration, Some((0.25 * 0.6, 0.5 * 0.8)));
        let repro = &result.reproducibility;
        assert_eq!(repro.input_dataset_ref, input_ref);
        assert_eq!(repro.input_content_digest, job.input_snapshot.content_digest);
        assert_eq!(repro.input_snapshot_version, 7);
        assert_eq!(repro.algorithm_ref, "mining.association:fpgrowth");
        assert_eq!(repro.params_digest, "cafe");
        assert_eq!(repro.implementation_version, "code-1");
        assert_eq!(repro.environment_version, "env-1");
        assert_eq!(repro.policy_fingerprint, "policy-fp");
    }
}
