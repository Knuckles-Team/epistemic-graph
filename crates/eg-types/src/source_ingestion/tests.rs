use serde_json::json;

use super::*;

fn digest(byte: u8) -> Digest256 {
    Digest256::from_bytes([byte; 32])
}

fn request() -> SourceIngestionBatch {
    SourceIngestionBatch {
        connector: ResourceId::new("demo-connector").unwrap(),
        mode: SourceIngestionMode::Delta,
        strict_schema: true,
        records: BoundedVec::new(vec![SourceRecord {
            stream: ResourceId::new("items").unwrap(),
            record_id: "item-1".into(),
            mapping_reference: "manifest:demo-connector#schema_mappings/item".into(),
            payload: SourceJson::new(json!({"name": "one", "id": 1})).unwrap(),
            updated_at: Some("2026-09-20T00:00:00Z".into()),
            provenance: SourceRecordProvenance {
                connector: ResourceId::new("demo-connector").unwrap(),
                adapter_kind: ResourceId::new("mcp").unwrap(),
                server: "demo-server".into(),
                tool: "list_items".into(),
                tool_schema_sha256: digest(7),
                source_uri: "demo://items/item-1".into(),
            },
        }])
        .unwrap(),
        relationships: BoundedVec::new(Vec::new()).unwrap(),
        provider_checkpoint: SourceCheckpoint {
            stream: ResourceId::new("items").unwrap(),
            position: SourceJson::new(json!({"page": 2})).unwrap(),
            content_hash: Some(digest(8)),
            watermark: Some("2026-09-20T00:00:00Z".into()),
            pending_watermark: None,
        },
        expected_previous_checkpoint: Some(SourceCheckpoint {
            stream: ResourceId::new("items").unwrap(),
            position: SourceJson::new(json!({"page": 1})).unwrap(),
            content_hash: Some(digest(6)),
            watermark: None,
            pending_watermark: None,
        }),
        authoritative_live_ids: None,
        empty_authoritative_approval: None,
        withdrawals: BoundedVec::new(Vec::new()).unwrap(),
    }
}

#[test]
fn canonical_digest_is_stable_across_payload_key_order() {
    let first = SourceIngestionRequest::new(request()).unwrap();
    let mut reordered = request();
    reordered.records = BoundedVec::new(vec![SourceRecord {
        payload: SourceJson::new(json!({"id": 1, "name": "one"})).unwrap(),
        ..reordered.records.as_slice()[0].clone()
    }])
    .unwrap();
    let second = SourceIngestionRequest::new(reordered).unwrap();
    assert_eq!(
        first.batch_digest().unwrap(),
        second.batch_digest().unwrap()
    );
}

#[test]
fn rejects_duplicate_source_identity() {
    let mut batch = request();
    batch.records = BoundedVec::new(vec![
        batch.records.as_slice()[0].clone(),
        batch.records.as_slice()[0].clone(),
    ])
    .unwrap();
    assert!(SourceIngestionRequest::new(batch)
        .unwrap_err()
        .contains("duplicate source identity"));
}

#[test]
fn rejects_cross_connector_provenance_and_cross_stream_cursor() {
    let mut connector = request();
    let mut record = connector.records.as_slice()[0].clone();
    record.provenance.connector = ResourceId::new("other").unwrap();
    connector.records = BoundedVec::new(vec![record]).unwrap();
    assert!(SourceIngestionRequest::new(connector)
        .unwrap_err()
        .contains("provenance connector"));

    let mut stream = request();
    stream.provider_checkpoint.stream = ResourceId::new("other-stream").unwrap();
    stream.expected_previous_checkpoint = None;
    assert!(SourceIngestionRequest::new(stream)
        .unwrap_err()
        .contains("record stream"));
}

#[test]
fn request_wire_never_contains_mapping_content_or_authority_fields() {
    let value = serde_json::to_value(SourceIngestionRequest::new(request()).unwrap()).unwrap();
    let object = value.as_object().unwrap();
    assert!(object["records"][0].get("mapping_reference").is_some());
    assert!(!object.contains_key("mapping"));
    assert!(!object.contains_key("tenant"));
    assert!(!object.contains_key("principal"));
    assert!(!object.contains_key("mutation"));
    assert!(object["records"][0]["payload"].is_object());
    assert!(object["provider_checkpoint"]["position"].is_object());
    assert!(object["expected_previous_checkpoint"]["position"].is_object());
}

#[test]
fn named_msgpack_projects_source_json_as_objects_not_binary_wrappers() {
    let request = SourceIngestionRequest::new(request()).unwrap();
    let wire: serde_json::Value =
        rmp_serde::from_slice(&request.canonical_bytes().unwrap()).unwrap();
    assert!(wire["records"][0]["payload"].is_object());
    assert!(wire["provider_checkpoint"]["position"].is_object());
    assert!(wire["expected_previous_checkpoint"]["position"].is_object());
}

#[test]
fn initial_cursor_emits_nil_while_optional_leaves_are_omitted() {
    let mut batch = request();
    batch.expected_previous_checkpoint = None;
    let mut record = batch.records.as_slice()[0].clone();
    record.updated_at = None;
    batch.records = BoundedVec::new(vec![record]).unwrap();
    batch.provider_checkpoint.watermark = None;
    batch.provider_checkpoint.pending_watermark = None;
    let request = SourceIngestionRequest::new(batch).unwrap();
    let wire: serde_json::Value =
        rmp_serde::from_slice(&request.canonical_bytes().unwrap()).unwrap();

    assert_eq!(
        wire.get("expected_previous_checkpoint"),
        Some(&serde_json::Value::Null)
    );
    assert!(wire["records"][0].get("updated_at").is_none());
    assert!(wire["provider_checkpoint"].get("watermark").is_none());
    assert!(wire["provider_checkpoint"]
        .get("pending_watermark")
        .is_none());
}

#[test]
fn mode_contract_separates_provider_withdrawals_from_authoritative_reconcile() {
    let mut delta = request();
    delta.withdrawals = BoundedVec::new(vec![SourceWithdrawal {
        entity: SourceEntityRef {
            stream: ResourceId::new("items").unwrap(),
            record_id: "retired".into(),
        },
        reason: "provider_deleted".into(),
    }])
    .unwrap();
    assert!(SourceIngestionRequest::new(delta).is_ok());

    let mut reconcile = request();
    reconcile.mode = SourceIngestionMode::Reconcile;
    reconcile.authoritative_live_ids = Some(
        BoundedVec::new(vec![SourceEntityRef {
            stream: ResourceId::new("items").unwrap(),
            record_id: "item-1".into(),
        }])
        .unwrap(),
    );
    assert!(SourceIngestionRequest::new(reconcile).is_ok());

    let mut invalid = request();
    invalid.mode = SourceIngestionMode::Reconcile;
    assert!(SourceIngestionRequest::new(invalid)
        .unwrap_err()
        .contains("authoritative_live_ids"));
}

#[test]
fn empty_reconcile_requires_an_explicit_approval_reference() {
    let mut batch = request();
    batch.mode = SourceIngestionMode::Reconcile;
    batch.records = BoundedVec::new(Vec::new()).unwrap();
    batch.authoritative_live_ids = Some(BoundedVec::new(Vec::new()).unwrap());
    assert!(SourceIngestionRequest::new(batch.clone())
        .unwrap_err()
        .contains("governed approval"));
    batch.empty_authoritative_approval = Some("approval:empty-source-snapshot".into());
    assert!(SourceIngestionRequest::new(batch).is_ok());
}

#[test]
fn full_is_non_authoritative_and_provider_content_hash_is_optional() {
    let mut batch = request();
    batch.mode = SourceIngestionMode::Full;
    batch.provider_checkpoint.content_hash = None;
    assert!(SourceIngestionRequest::new(batch.clone()).is_ok());
    batch.authoritative_live_ids = Some(
        BoundedVec::new(vec![SourceEntityRef {
            stream: ResourceId::new("items").unwrap(),
            record_id: "item-1".into(),
        }])
        .unwrap(),
    );
    assert!(SourceIngestionRequest::new(batch)
        .unwrap_err()
        .contains("non-authoritative"));
}

#[test]
fn relationships_are_top_level_typed_observations() {
    let mut batch = request();
    batch.records = BoundedVec::new(vec![
        batch.records.as_slice()[0].clone(),
        SourceRecord {
            record_id: "item-2".into(),
            payload: SourceJson::new(json!({"name": "two", "id": 2})).unwrap(),
            ..batch.records.as_slice()[0].clone()
        },
    ])
    .unwrap();
    batch.relationships = BoundedVec::new(vec![SourceRelationship {
        source: SourceEntityRef {
            stream: ResourceId::new("items").unwrap(),
            record_id: "item-1".into(),
        },
        target: SourceEntityRef {
            stream: ResourceId::new("items").unwrap(),
            record_id: "item-2".into(),
        },
        relation_reference: "manifest:demo-connector#resources/Item/relations/contains".into(),
        properties: Some(SourceJson::new(json!({"ordinal": 1})).unwrap()),
        provenance: batch.records.as_slice()[0].provenance.clone(),
    }])
    .unwrap();
    let request = SourceIngestionRequest::new(batch).unwrap();
    let wire: serde_json::Value =
        rmp_serde::from_slice(&request.canonical_bytes().unwrap()).unwrap();
    assert_eq!(wire["relationships"][0]["source"]["record_id"], "item-1");
    assert!(wire["relationships"][0].get("relationship_id").is_none());
    assert!(wire["relationships"][0]["properties"].is_object());
}

#[test]
fn reconcile_relationship_endpoints_must_belong_to_the_authoritative_live_set() {
    let mut batch = request();
    let first = SourceEntityRef {
        stream: ResourceId::new("items").unwrap(),
        record_id: "item-1".into(),
    };
    let second = SourceEntityRef {
        stream: ResourceId::new("items").unwrap(),
        record_id: "item-2".into(),
    };
    batch.mode = SourceIngestionMode::Reconcile;
    batch.authoritative_live_ids = Some(BoundedVec::new(vec![first.clone()]).unwrap());
    batch.relationships = BoundedVec::new(vec![SourceRelationship {
        source: first,
        target: second,
        relation_reference: "manifest:demo-connector#resources/Item/relations/contains".into(),
        properties: None,
        provenance: batch.records.as_slice()[0].provenance.clone(),
    }])
    .unwrap();

    assert!(SourceIngestionRequest::new(batch)
        .unwrap_err()
        .contains("authoritative live ids"));
}

#[test]
fn delta_relationship_cannot_reference_an_entity_withdrawn_in_the_same_commit() {
    let mut batch = request();
    let source = SourceEntityRef {
        stream: ResourceId::new("items").unwrap(),
        record_id: "prior-item".into(),
    };
    let target = SourceEntityRef {
        stream: ResourceId::new("items").unwrap(),
        record_id: "retired-item".into(),
    };
    batch.records = BoundedVec::new(Vec::new()).unwrap();
    batch.relationships = BoundedVec::new(vec![SourceRelationship {
        source,
        target: target.clone(),
        relation_reference: "manifest:demo-connector#resources/Item/relations/contains".into(),
        properties: None,
        provenance: request().records.as_slice()[0].provenance.clone(),
    }])
    .unwrap();
    batch.withdrawals = BoundedVec::new(vec![SourceWithdrawal {
        entity: target,
        reason: "provider_deleted".into(),
    }])
    .unwrap();

    assert!(SourceIngestionRequest::new(batch)
        .unwrap_err()
        .contains("withdrawn entity"));
}

#[test]
fn checkpoint_content_hash_is_omitted_but_expected_checkpoint_nil_is_preserved() {
    let mut batch = request();
    batch.provider_checkpoint.content_hash = None;
    batch.expected_previous_checkpoint = None;
    let request = SourceIngestionRequest::new(batch).unwrap();
    let wire: serde_json::Value =
        rmp_serde::from_slice(&request.canonical_bytes().unwrap()).unwrap();
    assert!(wire["provider_checkpoint"].get("content_hash").is_none());
    assert_eq!(
        wire["expected_previous_checkpoint"],
        serde_json::Value::Null
    );
}

#[test]
fn empty_delta_advances_checkpoint_but_empty_full_requires_approval() {
    let mut delta = request();
    delta.records = BoundedVec::new(Vec::new()).unwrap();
    assert!(SourceIngestionRequest::new(delta).is_ok());

    let mut full = request();
    full.mode = SourceIngestionMode::Full;
    full.records = BoundedVec::new(Vec::new()).unwrap();
    assert!(SourceIngestionRequest::new(full.clone())
        .unwrap_err()
        .contains("authoritative-empty approval"));
    full.empty_authoritative_approval = Some("approval:empty-full-snapshot".into());
    assert!(SourceIngestionRequest::new(full).is_ok());
}

#[test]
fn exact_checkpoint_no_op_is_rejected() {
    let mut batch = request();
    batch.records = BoundedVec::new(Vec::new()).unwrap();
    batch.provider_checkpoint = batch.expected_previous_checkpoint.clone().unwrap();
    assert!(SourceIngestionRequest::new(batch)
        .unwrap_err()
        .contains("must differ"));
}

#[test]
fn provider_checkpoint_position_accepts_scalar_json() {
    let mut batch = request();
    batch.provider_checkpoint.position = SourceJson::new(json!("opaque-checkpoint-2")).unwrap();
    assert!(SourceIngestionRequest::new(batch).is_ok());

    let mut null_position = request();
    null_position.provider_checkpoint.position = SourceJson::new(serde_json::Value::Null).unwrap();
    assert!(SourceIngestionRequest::new(null_position).is_ok());
}

// EG-REPO-INGEST-R003.1 — standardization processing (excluding drift
// detection) is the first migrated root claimed through EG's own typed
// interface rather than left to its retiring legacy module.

#[test]
fn r003_1_standardization_processing_round_trips_through_the_typed_root() {
    let root = MigratedIngestionCapabilityRoot::StandardizationProcessing;
    let wire = serde_json::to_string(&root).unwrap();
    assert_eq!(wire, "\"standardization_processing\"");
    let back: MigratedIngestionCapabilityRoot = serde_json::from_str(&wire).unwrap();
    assert_eq!(back, root);
}

#[test]
fn r003_1_a_fresh_claim_on_standardization_processing_succeeds() {
    let root = MigratedIngestionCapabilityRoot::StandardizationProcessing;
    assert!(root.claim(&[]).is_ok());
}

#[test]
fn r003_1_a_duplicate_claim_on_standardization_processing_is_refused() {
    let root = MigratedIngestionCapabilityRoot::StandardizationProcessing;
    let already_claimed = vec![root];
    let err = root
        .claim(&already_claimed)
        .expect_err("a second writer for the same migrated root must be refused");
    assert!(err.contains("already claimed"));
}

#[test]
fn r003_1_a_different_root_is_not_blocked_by_an_unrelated_claim() {
    let standardization = MigratedIngestionCapabilityRoot::StandardizationProcessing;
    let already_claimed = vec![MigratedIngestionCapabilityRoot::QuantumProcessing];
    assert!(standardization.claim(&already_claimed).is_ok());
}

// EG-REPO-INGEST-R003.2 — infrastructure processing is claimed through EG's
// own typed interface rather than left to its retiring legacy module.

#[test]
fn r003_2_infrastructure_processing_round_trips_through_the_typed_root() {
    let root = MigratedIngestionCapabilityRoot::InfrastructureProcessing;
    let wire = serde_json::to_string(&root).unwrap();
    assert_eq!(wire, "\"infrastructure_processing\"");
    let back: MigratedIngestionCapabilityRoot = serde_json::from_str(&wire).unwrap();
    assert_eq!(back, root);
}

#[test]
fn r003_2_a_fresh_claim_on_infrastructure_processing_succeeds() {
    let root = MigratedIngestionCapabilityRoot::InfrastructureProcessing;
    assert!(root.claim(&[]).is_ok());
}

#[test]
fn r003_2_a_duplicate_claim_on_infrastructure_processing_is_refused() {
    let root = MigratedIngestionCapabilityRoot::InfrastructureProcessing;
    let already_claimed = vec![root];
    let err = root
        .claim(&already_claimed)
        .expect_err("a second writer for the same migrated root must be refused");
    assert!(err.contains("already claimed"));
}

#[test]
fn r003_2_a_different_root_is_not_blocked_by_an_unrelated_claim() {
    let infrastructure = MigratedIngestionCapabilityRoot::InfrastructureProcessing;
    let already_claimed = vec![MigratedIngestionCapabilityRoot::SecurityProcessing];
    assert!(infrastructure.claim(&already_claimed).is_ok());
}

// EG-REPO-INGEST-R003.3 — quantum processing is claimed through EG's own
// typed interface rather than left to its retiring legacy module.

// spec: EG-REPO-INGEST-R003.3
#[test]
fn r003_3_quantum_processing_round_trips_through_the_typed_root() {
    let root = MigratedIngestionCapabilityRoot::QuantumProcessing;
    let wire = serde_json::to_string(&root).unwrap();
    assert_eq!(wire, "\"quantum_processing\"");
    let back: MigratedIngestionCapabilityRoot = serde_json::from_str(&wire).unwrap();
    assert_eq!(back, root);
}

#[test]
fn r003_3_a_fresh_claim_on_quantum_processing_succeeds() {
    let root = MigratedIngestionCapabilityRoot::QuantumProcessing;
    assert!(root.claim(&[]).is_ok());
}

#[test]
fn r003_3_a_duplicate_claim_on_quantum_processing_is_refused() {
    let root = MigratedIngestionCapabilityRoot::QuantumProcessing;
    let already_claimed = vec![root];
    let err = root
        .claim(&already_claimed)
        .expect_err("a second writer for the same migrated root must be refused");
    assert!(err.contains("already claimed"));
}

#[test]
fn r003_3_a_different_root_is_not_blocked_by_an_unrelated_claim() {
    let quantum = MigratedIngestionCapabilityRoot::QuantumProcessing;
    let already_claimed = vec![MigratedIngestionCapabilityRoot::EnterpriseProtocolHandling];
    assert!(quantum.claim(&already_claimed).is_ok());
}

// EG-REPO-INGEST-R003.4 — security processing is claimed through EG's own
// typed interface rather than left to its retiring legacy module.

// spec: EG-REPO-INGEST-R003.4
#[test]
fn r003_4_security_processing_round_trips_through_the_typed_root() {
    let root = MigratedIngestionCapabilityRoot::SecurityProcessing;
    let wire = serde_json::to_string(&root).unwrap();
    assert_eq!(wire, "\"security_processing\"");
    let back: MigratedIngestionCapabilityRoot = serde_json::from_str(&wire).unwrap();
    assert_eq!(back, root);
}

#[test]
fn r003_4_a_fresh_claim_on_security_processing_succeeds() {
    let root = MigratedIngestionCapabilityRoot::SecurityProcessing;
    assert!(root.claim(&[]).is_ok());
}

#[test]
fn r003_4_a_duplicate_claim_on_security_processing_is_refused() {
    let root = MigratedIngestionCapabilityRoot::SecurityProcessing;
    let already_claimed = vec![root];
    let err = root
        .claim(&already_claimed)
        .expect_err("a second writer for the same migrated root must be refused");
    assert!(err.contains("already claimed"));
}

#[test]
fn r003_4_a_different_root_is_not_blocked_by_an_unrelated_claim() {
    let security = MigratedIngestionCapabilityRoot::SecurityProcessing;
    let already_claimed = vec![MigratedIngestionCapabilityRoot::EcosystemConceptExtraction];
    assert!(security.claim(&already_claimed).is_ok());
}

// EG-REPO-INGEST-R003.5 — enterprise protocol handling is claimed through
// EG's own typed interface rather than left to its retiring legacy module.

// spec: EG-REPO-INGEST-R003.5
#[test]
fn r003_5_enterprise_protocol_handling_round_trips_through_the_typed_root() {
    let root = MigratedIngestionCapabilityRoot::EnterpriseProtocolHandling;
    let wire = serde_json::to_string(&root).unwrap();
    assert_eq!(wire, "\"enterprise_protocol_handling\"");
    let back: MigratedIngestionCapabilityRoot = serde_json::from_str(&wire).unwrap();
    assert_eq!(back, root);
}

#[test]
fn r003_5_a_fresh_claim_on_enterprise_protocol_handling_succeeds() {
    let root = MigratedIngestionCapabilityRoot::EnterpriseProtocolHandling;
    assert!(root.claim(&[]).is_ok());
}

#[test]
fn r003_5_a_duplicate_claim_on_enterprise_protocol_handling_is_refused() {
    let root = MigratedIngestionCapabilityRoot::EnterpriseProtocolHandling;
    let already_claimed = vec![root];
    let err = root
        .claim(&already_claimed)
        .expect_err("a second writer for the same migrated root must be refused");
    assert!(err.contains("already claimed"));
}

#[test]
fn r003_5_a_different_root_is_not_blocked_by_an_unrelated_claim() {
    let enterprise = MigratedIngestionCapabilityRoot::EnterpriseProtocolHandling;
    let already_claimed = vec![MigratedIngestionCapabilityRoot::InfrastructureProcessing];
    assert!(enterprise.claim(&already_claimed).is_ok());
}

// EG-REPO-INGEST-R003.6 — ecosystem concept extraction is claimed through
// EG's own typed interface rather than left to its retiring legacy module.

#[test]
fn r003_6_ecosystem_concept_extraction_round_trips_through_the_typed_root() {
    let root = MigratedIngestionCapabilityRoot::EcosystemConceptExtraction;
    let wire = serde_json::to_string(&root).unwrap();
    assert_eq!(wire, "\"ecosystem_concept_extraction\"");
    let back: MigratedIngestionCapabilityRoot = serde_json::from_str(&wire).unwrap();
    assert_eq!(back, root);
}

#[test]
fn r003_6_a_fresh_claim_on_ecosystem_concept_extraction_succeeds() {
    let root = MigratedIngestionCapabilityRoot::EcosystemConceptExtraction;
    assert!(root.claim(&[]).is_ok());
}

#[test]
fn r003_6_a_duplicate_claim_on_ecosystem_concept_extraction_is_refused() {
    let root = MigratedIngestionCapabilityRoot::EcosystemConceptExtraction;
    let already_claimed = vec![root];
    let err = root
        .claim(&already_claimed)
        .expect_err("a second writer for the same migrated root must be refused");
    assert!(err.contains("already claimed"));
}

#[test]
fn r003_6_a_different_root_is_not_blocked_by_an_unrelated_claim() {
    let ecosystem = MigratedIngestionCapabilityRoot::EcosystemConceptExtraction;
    let already_claimed = vec![MigratedIngestionCapabilityRoot::QuantumProcessing];
    assert!(ecosystem.claim(&already_claimed).is_ok());
}

// EG-REPO-INGEST-R003.7 — trace-ontology observability is claimed through
// EG's own typed interface rather than left to its retiring legacy module.

#[test]
fn r003_7_trace_ontology_observability_round_trips_through_the_typed_root() {
    let root = MigratedIngestionCapabilityRoot::TraceOntologyObservability;
    let wire = serde_json::to_string(&root).unwrap();
    assert_eq!(wire, "\"trace_ontology_observability\"");
    let back: MigratedIngestionCapabilityRoot = serde_json::from_str(&wire).unwrap();
    assert_eq!(back, root);
}

#[test]
fn r003_7_a_fresh_claim_on_trace_ontology_observability_succeeds() {
    let root = MigratedIngestionCapabilityRoot::TraceOntologyObservability;
    assert!(root.claim(&[]).is_ok());
}

#[test]
fn r003_7_a_duplicate_claim_on_trace_ontology_observability_is_refused() {
    let root = MigratedIngestionCapabilityRoot::TraceOntologyObservability;
    let already_claimed = vec![root];
    let err = root
        .claim(&already_claimed)
        .expect_err("a second writer for the same migrated root must be refused");
    assert!(err.contains("already claimed"));
}

#[test]
fn r003_7_a_different_root_is_not_blocked_by_an_unrelated_claim() {
    let trace_ontology = MigratedIngestionCapabilityRoot::TraceOntologyObservability;
    let already_claimed = vec![MigratedIngestionCapabilityRoot::SelfIngestionObservability];
    assert!(trace_ontology.claim(&already_claimed).is_ok());
}

// EG-REPO-INGEST-R003.8 — self-ingestion observability is claimed through
// EG's own typed interface rather than left to its retiring legacy module.

#[test]
fn r003_8_self_ingestion_observability_round_trips_through_the_typed_root() {
    let root = MigratedIngestionCapabilityRoot::SelfIngestionObservability;
    let wire = serde_json::to_string(&root).unwrap();
    assert_eq!(wire, "\"self_ingestion_observability\"");
    let back: MigratedIngestionCapabilityRoot = serde_json::from_str(&wire).unwrap();
    assert_eq!(back, root);
}

#[test]
fn r003_8_a_fresh_claim_on_self_ingestion_observability_succeeds() {
    let root = MigratedIngestionCapabilityRoot::SelfIngestionObservability;
    assert!(root.claim(&[]).is_ok());
}

#[test]
fn r003_8_a_duplicate_claim_on_self_ingestion_observability_is_refused() {
    let root = MigratedIngestionCapabilityRoot::SelfIngestionObservability;
    let already_claimed = vec![root];
    let err = root
        .claim(&already_claimed)
        .expect_err("a second writer for the same migrated root must be refused");
    assert!(err.contains("already claimed"));
}

#[test]
fn r003_8_a_different_root_is_not_blocked_by_an_unrelated_claim() {
    let self_ingestion = MigratedIngestionCapabilityRoot::SelfIngestionObservability;
    let already_claimed = vec![MigratedIngestionCapabilityRoot::AuditLoggingObservability];
    assert!(self_ingestion.claim(&already_claimed).is_ok());
}

// EG-REPO-INGEST-R003.9 — audit-logging observability is claimed through
// EG's own typed interface rather than left to its retiring legacy module.

#[test]
fn r003_9_audit_logging_observability_round_trips_through_the_typed_root() {
    let root = MigratedIngestionCapabilityRoot::AuditLoggingObservability;
    let wire = serde_json::to_string(&root).unwrap();
    assert_eq!(wire, "\"audit_logging_observability\"");
    let back: MigratedIngestionCapabilityRoot = serde_json::from_str(&wire).unwrap();
    assert_eq!(back, root);
}

#[test]
fn r003_9_a_fresh_claim_on_audit_logging_observability_succeeds() {
    let root = MigratedIngestionCapabilityRoot::AuditLoggingObservability;
    assert!(root.claim(&[]).is_ok());
}

#[test]
fn r003_9_a_duplicate_claim_on_audit_logging_observability_is_refused() {
    let root = MigratedIngestionCapabilityRoot::AuditLoggingObservability;
    let already_claimed = vec![root];
    let err = root
        .claim(&already_claimed)
        .expect_err("a second writer for the same migrated root must be refused");
    assert!(err.contains("already claimed"));
}

#[test]
fn r003_9_a_different_root_is_not_blocked_by_an_unrelated_claim() {
    let audit_logging = MigratedIngestionCapabilityRoot::AuditLoggingObservability;
    let already_claimed = vec![MigratedIngestionCapabilityRoot::RelationalAuthorityGovernance];
    assert!(audit_logging.claim(&already_claimed).is_ok());
}

// EG-REPO-INGEST-R003.10 — relational-authority governance is claimed
// through EG's own typed interface rather than left to its retiring legacy
// module.

#[test]
fn r003_10_relational_authority_governance_round_trips_through_the_typed_root() {
    let root = MigratedIngestionCapabilityRoot::RelationalAuthorityGovernance;
    let wire = serde_json::to_string(&root).unwrap();
    assert_eq!(wire, "\"relational_authority_governance\"");
    let back: MigratedIngestionCapabilityRoot = serde_json::from_str(&wire).unwrap();
    assert_eq!(back, root);
}

#[test]
fn r003_10_a_fresh_claim_on_relational_authority_governance_succeeds() {
    let root = MigratedIngestionCapabilityRoot::RelationalAuthorityGovernance;
    assert!(root.claim(&[]).is_ok());
}

#[test]
fn r003_10_a_duplicate_claim_on_relational_authority_governance_is_refused() {
    let root = MigratedIngestionCapabilityRoot::RelationalAuthorityGovernance;
    let already_claimed = vec![root];
    let err = root
        .claim(&already_claimed)
        .expect_err("a second writer for the same migrated root must be refused");
    assert!(err.contains("already claimed"));
}

#[test]
fn r003_10_a_different_root_is_not_blocked_by_an_unrelated_claim() {
    let relational_authority = MigratedIngestionCapabilityRoot::RelationalAuthorityGovernance;
    let already_claimed = vec![MigratedIngestionCapabilityRoot::PlacementMiningResearch];
    assert!(relational_authority.claim(&already_claimed).is_ok());
}

// EG-REPO-INGEST-R003.11 — placement-mining research is claimed through
// EG's own typed interface rather than left to its retiring legacy module.

#[test]
fn r003_11_placement_mining_research_round_trips_through_the_typed_root() {
    let root = MigratedIngestionCapabilityRoot::PlacementMiningResearch;
    let wire = serde_json::to_string(&root).unwrap();
    assert_eq!(wire, "\"placement_mining_research\"");
    let back: MigratedIngestionCapabilityRoot = serde_json::from_str(&wire).unwrap();
    assert_eq!(back, root);
}

#[test]
fn r003_11_a_fresh_claim_on_placement_mining_research_succeeds() {
    let root = MigratedIngestionCapabilityRoot::PlacementMiningResearch;
    assert!(root.claim(&[]).is_ok());
}

#[test]
fn r003_11_a_duplicate_claim_on_placement_mining_research_is_refused() {
    let root = MigratedIngestionCapabilityRoot::PlacementMiningResearch;
    let already_claimed = vec![root];
    let err = root
        .claim(&already_claimed)
        .expect_err("a second writer for the same migrated root must be refused");
    assert!(err.contains("already claimed"));
}

#[test]
fn r003_11_a_different_root_is_not_blocked_by_an_unrelated_claim() {
    let placement_mining = MigratedIngestionCapabilityRoot::PlacementMiningResearch;
    let already_claimed = vec![MigratedIngestionCapabilityRoot::StandardizationProcessing];
    assert!(placement_mining.claim(&already_claimed).is_ok());
}
