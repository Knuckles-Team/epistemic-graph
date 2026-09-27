use super::*;

#[test]
fn newer_authoritative_version_and_tombstone_keep_the_same_entity() {
    let record = dirty_record(41, 1, 2);
    let revision = record_revision(&record);
    let binding = binding(&revision);
    let newer_revision = sql_source_revision_for_epoch(digest(170), 3).unwrap();
    let tombstone = source_record(
        &binding,
        11,
        &newer_revision,
        SemanticSqlSourceValue::Tombstone {
            deletion_proof_digest: digest(99),
        },
    );
    let scope_digest = SemanticDigest::from_bytes(*record.identity.binding_digest().as_bytes());
    assert!(tombstone.is_tombstone());
    let intent =
        coalesce_sql_source_record_to_s1(&binding, scope_digest, &record, &tombstone).unwrap();
    let entity_id = tombstone.source_entity_id();
    assert_eq!(intent.scope.source_entity_id(), Some(entity_id.as_str()));
    assert_eq!(intent.source_revision, tombstone.source_revision);
    assert_eq!(intent.input_digest, digest(99));
}

#[test]
fn newer_foreign_authority_is_rejected_before_counter_ordering() {
    let record = dirty_record(45, 1, 2);
    let revision = record_revision(&record);
    let binding = binding(&revision);
    let foreign_revision = sql_source_revision_for_epoch(digest(171), 9).unwrap();
    let source = source_record(
        &binding,
        11,
        &foreign_revision,
        SemanticSqlSourceValue::Present {
            source_bytes: b"foreign authority".to_vec(),
        },
    );
    let scope_digest = SemanticDigest::from_bytes(*record.identity.binding_digest().as_bytes());
    assert!(coalesce_sql_source_record_to_s1(&binding, scope_digest, &record, &source).is_err());
}

#[test]
fn caller_can_route_distinct_source_entities_from_one_authoritative_page() {
    let record = dirty_record(51, 1, 2);
    let revision = record_revision(&record);
    let binding = binding(&revision);
    let expected = source_record(
        &binding,
        11,
        &revision,
        SemanticSqlSourceValue::Present {
            source_bytes: b"expected article body".to_vec(),
        },
    );
    let wrong_identity = source_record(
        &binding_for("tenant-b", "binding:articles-body", &revision),
        12,
        &revision,
        SemanticSqlSourceValue::Present {
            source_bytes: b"other article body".to_vec(),
        },
    );
    let scope_digest = SemanticDigest::from_bytes(*record.identity.binding_digest().as_bytes());
    assert!(
        coalesce_sql_source_record_to_s1(&binding, scope_digest, &record, &wrong_identity).is_err()
    );
    assert!(coalesce_sql_source_record_to_s1(&binding, scope_digest, &record, &expected).is_ok());
}

#[test]
fn partial_pages_cannot_turn_missing_rows_into_tombstones() {
    let binding = binding(&sql_source_revision_for_epoch(digest(170), 1).unwrap());
    let tombstone = source_record(
        &binding,
        11,
        &binding.source_revision,
        SemanticSqlSourceValue::Tombstone {
            deletion_proof_digest: digest(88),
        },
    );
    let partial = super::SemanticSqlSourceReadPage {
        source_revision: binding.source_revision.clone(),
        complete_snapshot_receipt_digest: None,
        sources: vec![tombstone],
        next_cursor: Some(b"next".to_vec()),
        complete: false,
    };
    assert!(partial.validate().is_err());
}

#[test]
fn complete_empty_page_requires_a_snapshot_receipt_and_deletion_proof_is_scoped() {
    let revision = sql_source_revision_for_epoch(digest(170), 7).unwrap();
    let missing_receipt = super::SemanticSqlSourceReadPage {
        source_revision: revision.clone(),
        complete_snapshot_receipt_digest: None,
        sources: Vec::new(),
        next_cursor: None,
        complete: true,
    };
    assert!(missing_receipt.validate().is_err());
    let complete = super::SemanticSqlSourceReadPage {
        source_revision: revision.clone(),
        complete_snapshot_receipt_digest: Some(digest(77)),
        sources: Vec::new(),
        next_cursor: None,
        complete: true,
    };
    assert!(complete.validate().is_ok());
    assert_ne!(
            sql_source_deletion_proof(
                "semantic-sql-source:sha256:1111111111111111111111111111111111111111111111111111111111111111",
                &revision,
                digest(77),
            ),
            sql_source_deletion_proof(
                "semantic-sql-source:sha256:2222222222222222222222222222222222222222222222222222222222222222",
                &revision,
                digest(77),
            )
        );
}

#[test]
fn empty_source_pages_still_fence_authority_and_epoch() {
    let binding_revision = sql_source_revision_for_epoch(digest(170), 7).unwrap();
    let binding = binding(&binding_revision);
    let empty = SemanticSqlSourceReadPage {
        source_revision: sql_source_revision_for_epoch(digest(171), 99).unwrap(),
        complete_snapshot_receipt_digest: Some(digest(77)),
        sources: Vec::new(),
        next_cursor: None,
        complete: true,
    };
    assert!(validate_page_revision_against_binding(&binding, &empty.source_revision).is_err());

    let stale = SemanticSqlSourceReadPage {
        source_revision: sql_source_revision_for_epoch(digest(170), 6).unwrap(),
        ..empty.clone()
    };
    assert!(validate_page_revision_against_binding(&binding, &stale.source_revision).is_err());
    let current = SemanticSqlSourceReadPage {
        source_revision: binding_revision,
        ..empty
    };
    assert!(validate_page_revision_against_binding(&binding, &current.source_revision).is_ok());
}

#[test]
fn source_page_byte_cap_rejects_an_oversized_present_value() {
    let revision = sql_source_revision_for_epoch(digest(170), 8).unwrap();
    let binding = binding(&revision);
    let source = source_record(
        &binding,
        11,
        &revision,
        SemanticSqlSourceValue::Present {
            source_bytes: vec![b'x'; super::SemanticSqlSourceReadPage::MAX_SOURCE_BYTES + 1],
        },
    );
    let page = super::SemanticSqlSourceReadPage {
        source_revision: revision,
        complete_snapshot_receipt_digest: Some(digest(77)),
        sources: vec![source],
        next_cursor: None,
        complete: true,
    };
    assert!(page.validate().is_err());
}
