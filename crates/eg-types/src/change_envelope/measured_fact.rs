//! Evidence rules for a measured model fact (EG-ORCH-VOCAB-R011). The rule
//! itself runs in `ChangeEnvelope::validate`
//! (`validation::validate_measured_facts`); this module holds its proof.

#[cfg(test)]
mod tests {
    use crate::change_envelope::{ChangeEnvelope, EvidenceRecord, MaterialOperation};
    use crate::test_support::change_envelope::minimal_envelope;

    fn envelope_writing_a_measured_fact() -> ChangeEnvelope {
        let mut envelope = minimal_envelope();
        envelope.mutation.operations[0].method = crate::protocol::Method::AddNode {
            node_id: "fact:model-a:latency".into(),
            properties_msgpack: rmp_serde::to_vec_named(&serde_json::json!({
                "node_type": "MeasuredModelFact",
                "property": "latency_ms",
                "value": 12.5,
            }))
            .unwrap(),
        };
        envelope
            .mutation
            .reseal_envelope(crate::contract::Digest256::from_bytes([1_u8; 32]))
            .expect("a fixture batch reseals");
        envelope
    }

    fn run_evidence(run: Option<&str>) -> EvidenceRecord {
        EvidenceRecord {
            evidence_id: "ev-run-1".into(),
            operation: MaterialOperation::Upsert,
            object_id: "object-1".into(),
            modality: "evaluation_run".into(),
            locus_msgpack: rmp_serde::to_vec_named(&serde_json::json!({"run": 1})).unwrap(),
            content_digest: "d".repeat(64),
            evaluation_run: run.map(str::to_string),
        }
    }

    // spec: EG-ORCH-VOCAB-R011.8
    #[test]
    fn missing_evidence_is_refused() {
        let mut envelope = envelope_writing_a_measured_fact();
        let error = envelope.validate().unwrap_err();
        assert!(error.contains("MeasuredFactWithoutEvidence"), "{error}");
        // Evidence that names no evaluation run and not the fact does not count.
        envelope.evidence.push(run_evidence(None));
        let error = envelope.validate().unwrap_err();
        assert!(error.contains("MeasuredFactWithoutEvidence"), "{error}");
        // Evidence naming an evaluation run clears the rule.
        envelope.evidence[0] = run_evidence(Some("run:eval-1"));
        envelope.validate().unwrap();
    }
}
