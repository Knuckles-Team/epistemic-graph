//! Branch proofs for the pure source-revision rules and the reconciliation
//! checkpoint codec.
//!
//! These functions decide refusals without touching a store, so every exit
//! is driven directly: each malformed encoding marker, each validation
//! refusal, and both arms of the refresh revision rule (a SQL-sourced live
//! head and a non-SQL one).

use eg_types::semantic_index::SemanticDigest;
use std::cmp::Ordering;

use super::reconciliation::{
    compare_source_revision, sql_source_revision_parts, validate_sql_source_revision,
    SemanticSourceReconciliationCheckpoint, SemanticSourceReconciliationPhase,
    SEMANTIC_RECONCILIATION_CHECKPOINT_MAGIC, SEMANTIC_RECONCILIATION_MAX_CURSOR_BYTES,
};
use super::reconciliation_codec::{
    decode_reconciliation_checkpoint, encode_reconciliation_checkpoint,
    validate_reconciliation_checkpoint,
};
use super::refresh::ensure_newer_source_revision;
use super::SemanticCodeError;

fn digest(byte: u8) -> SemanticDigest {
    SemanticDigest::from_bytes([byte; 32])
}

fn revision_with(authority_hex: char, epoch: &str) -> String {
    format!(
        "sql-source:sha256:{}:epoch:{epoch}",
        authority_hex.to_string().repeat(64)
    )
}

fn revision(epoch: u64) -> String {
    revision_with('a', &epoch.to_string())
}

fn entity(seed: u8) -> String {
    format!("semantic-sql-source:{}", digest(seed))
}

fn scanning() -> SemanticSourceReconciliationCheckpoint {
    SemanticSourceReconciliationCheckpoint {
        source_wakeup_digest: digest(7),
        source_revision: revision(3),
        phase: SemanticSourceReconciliationPhase::Scanning,
        source_cursor: Some(b"cursor".to_vec()),
        prior_cursor: None,
        rows_seen: 1,
        source_bytes_seen: 2,
        pages_seen: 3,
        complete_snapshot_receipt_digest: None,
    }
}

fn finalizing() -> SemanticSourceReconciliationCheckpoint {
    SemanticSourceReconciliationCheckpoint {
        phase: SemanticSourceReconciliationPhase::FinalizingTombstones,
        source_cursor: None,
        prior_cursor: Some(entity(5)),
        complete_snapshot_receipt_digest: Some(digest(9)),
        ..scanning()
    }
}

/// Offset of the phase byte: magic, wakeup digest, length-prefixed revision.
fn phase_offset(checkpoint: &SemanticSourceReconciliationCheckpoint) -> usize {
    SEMANTIC_RECONCILIATION_CHECKPOINT_MAGIC.len() + 32 + 4 + checkpoint.source_revision.len()
}

fn refused(result: Result<(), SemanticCodeError>) -> String {
    match result {
        Err(SemanticCodeError::Refused(message)) => message,
        other => panic!("expected a refusal, got {other:?}"),
    }
}

fn corrupt(bytes: &[u8]) -> String {
    match decode_reconciliation_checkpoint(bytes) {
        Err(SemanticCodeError::Corrupt(message)) => message,
        other => panic!("expected corrupt checkpoint bytes, got {other:?}"),
    }
}

#[test]
fn both_checkpoint_phases_round_trip_through_the_codec() {
    for checkpoint in [scanning(), finalizing()] {
        let bytes = encode_reconciliation_checkpoint(&checkpoint).unwrap();
        assert_eq!(
            decode_reconciliation_checkpoint(&bytes).unwrap(),
            checkpoint
        );
    }
}

#[test]
fn decode_refuses_an_unknown_or_oversized_encoding() {
    let mut bytes = encode_reconciliation_checkpoint(&scanning()).unwrap();
    bytes[0] ^= 0xff;
    assert!(corrupt(&bytes).contains("unknown encoding"));
    let mut oversized = SEMANTIC_RECONCILIATION_CHECKPOINT_MAGIC.to_vec();
    oversized.resize(16 * 1024 + 1, 0);
    assert!(corrupt(&oversized).contains("unknown encoding"));
}

#[test]
fn decode_refuses_truncated_and_trailing_bytes() {
    let bytes = encode_reconciliation_checkpoint(&finalizing()).unwrap();
    assert!(corrupt(&bytes[..bytes.len() - 1]).contains("is truncated"));
    let magic_only = SEMANTIC_RECONCILIATION_CHECKPOINT_MAGIC.to_vec();
    assert!(corrupt(&magic_only).contains("is truncated"));
    let mut trailing = bytes.clone();
    trailing.push(0);
    assert!(corrupt(&trailing).contains("trailing bytes"));
}

#[test]
fn decode_refuses_unknown_phase_optional_and_proof_markers() {
    let checkpoint = finalizing();
    let bytes = encode_reconciliation_checkpoint(&checkpoint).unwrap();
    let phase = phase_offset(&checkpoint);

    let mut bad_phase = bytes.clone();
    bad_phase[phase] = 7;
    assert!(corrupt(&bad_phase).contains("unknown phase"));

    let mut bad_optional = bytes.clone();
    bad_optional[phase + 1] = 9;
    assert!(corrupt(&bad_optional).contains("optional field has an unknown marker"));

    let mut bad_proof = bytes.clone();
    let proof_marker = bytes.len() - 33;
    bad_proof[proof_marker] = 5;
    assert!(corrupt(&bad_proof).contains("unknown proof marker"));
}

#[test]
fn decode_refuses_oversized_fields_and_non_utf8_text() {
    let checkpoint = finalizing();
    let bytes = encode_reconciliation_checkpoint(&checkpoint).unwrap();
    let revision_length = SEMANTIC_RECONCILIATION_CHECKPOINT_MAGIC.len() + 32;

    let mut oversized = bytes.clone();
    oversized[revision_length..revision_length + 4].copy_from_slice(&257u32.to_be_bytes());
    assert!(corrupt(&oversized).contains("field exceeds the bounded size"));

    let mut bad_revision = bytes.clone();
    bad_revision[revision_length + 4] = 0xff;
    assert!(corrupt(&bad_revision).contains("revision is not UTF-8"));

    let mut bad_cursor = bytes.clone();
    // phase, absent source cursor marker, prior marker, 4-byte length, text
    let prior_text = phase_offset(&checkpoint) + 1 + 1 + 1 + 4;
    bad_cursor[prior_text] = 0xff;
    assert!(corrupt(&bad_cursor).contains("cursor is not UTF-8"));
}

#[test]
fn decode_rejects_well_formed_bytes_that_violate_the_checkpoint_rules() {
    let checkpoint = finalizing();
    let mut bytes = encode_reconciliation_checkpoint(&checkpoint).unwrap();
    // Re-label a finalizing checkpoint as scanning: it has no source cursor.
    bytes[phase_offset(&checkpoint)] = 0;
    assert!(corrupt(&bytes).contains("scanning reconciliation checkpoint has invalid phase fields"));
}

#[test]
fn validation_refuses_zero_identities_and_bad_revisions() {
    let zero_wakeup = SemanticSourceReconciliationCheckpoint {
        source_wakeup_digest: SemanticDigest::from_bytes([0; 32]),
        ..scanning()
    };
    assert!(refused(validate_reconciliation_checkpoint(&zero_wakeup))
        .contains("wakeup identity is zero"));
    let bad_revision = SemanticSourceReconciliationCheckpoint {
        source_revision: "sql-source:not-an-authority".to_string(),
        ..scanning()
    };
    assert!(refused(validate_reconciliation_checkpoint(&bad_revision))
        .contains("canonical source authority"));
    let long_revision = SemanticSourceReconciliationCheckpoint {
        source_revision: revision_with('a', &format!("{}5", "0".repeat(300))),
        ..scanning()
    };
    assert!(refused(validate_reconciliation_checkpoint(&long_revision))
        .contains("revision exceeds the bounded size"));
    let zero_proof = SemanticSourceReconciliationCheckpoint {
        complete_snapshot_receipt_digest: Some(SemanticDigest::from_bytes([0; 32])),
        ..finalizing()
    };
    assert!(
        refused(validate_reconciliation_checkpoint(&zero_proof)).contains("snapshot proof is zero")
    );
}

#[test]
fn validation_refuses_unbounded_or_foreign_cursors() {
    for cursor in [
        Vec::new(),
        vec![1; SEMANTIC_RECONCILIATION_MAX_CURSOR_BYTES + 1],
    ] {
        let checkpoint = SemanticSourceReconciliationCheckpoint {
            source_cursor: Some(cursor),
            ..scanning()
        };
        assert!(refused(validate_reconciliation_checkpoint(&checkpoint))
            .contains("cursor exceeds the bounded size"));
    }
    let foreign_prior = SemanticSourceReconciliationCheckpoint {
        prior_cursor: Some("not-a-source".to_string()),
        ..finalizing()
    };
    assert!(refused(validate_reconciliation_checkpoint(&foreign_prior))
        .contains("prior cursor is not a source identity"));
}

#[test]
fn validation_refuses_fields_that_belong_to_the_other_phase() {
    let scanning_with_prior = SemanticSourceReconciliationCheckpoint {
        prior_cursor: Some(entity(5)),
        ..scanning()
    };
    let scanning_with_proof = SemanticSourceReconciliationCheckpoint {
        complete_snapshot_receipt_digest: Some(digest(9)),
        ..scanning()
    };
    for checkpoint in [scanning_with_prior, scanning_with_proof] {
        assert!(refused(validate_reconciliation_checkpoint(&checkpoint))
            .contains("scanning reconciliation checkpoint has invalid phase fields"));
    }
    let finalizing_with_cursor = SemanticSourceReconciliationCheckpoint {
        source_cursor: Some(b"cursor".to_vec()),
        ..finalizing()
    };
    let finalizing_without_proof = SemanticSourceReconciliationCheckpoint {
        complete_snapshot_receipt_digest: None,
        ..finalizing()
    };
    for checkpoint in [finalizing_with_cursor, finalizing_without_proof] {
        assert!(refused(validate_reconciliation_checkpoint(&checkpoint))
            .contains("tombstone reconciliation checkpoint has invalid phase fields"));
    }
}

#[test]
fn sql_source_revisions_parse_only_the_canonical_shape() {
    let canonical = revision(12);
    let parts = sql_source_revision_parts(&canonical).unwrap();
    assert_eq!(
        (parts.authority.len(), parts.epoch),
        ("sha256:".len() + 64, 12)
    );
    for invalid in [
        "source:sha256:aaaa:epoch:1".to_string(),
        format!("sql-source:sha256:{}", "a".repeat(64)),
        format!("sql-source:md5:{}:epoch:1", "a".repeat(64)),
        format!("sql-source:sha256:{}:epoch:1", "a".repeat(63)),
        revision_with('A', "1"),
        revision_with('a', "0"),
        revision_with('a', "one"),
    ] {
        assert!(sql_source_revision_parts(&invalid).is_none(), "{invalid}");
        assert!(validate_sql_source_revision(&invalid).is_err(), "{invalid}");
    }
    assert!(validate_sql_source_revision(&revision(1)).is_ok());
}

#[test]
fn source_revisions_compare_by_epoch_within_one_authority_only() {
    assert_eq!(
        compare_source_revision(&revision(2), &revision(10)),
        Ordering::Less
    );
    assert_eq!(
        compare_source_revision(&revision(10), &revision(2)),
        Ordering::Greater
    );
    let other_authority = revision_with('b', "2");
    assert_eq!(
        compare_source_revision(&other_authority, &revision(10)),
        other_authority.as_str().cmp(revision(10).as_str()),
        "revisions of different authorities fall back to a lexical order"
    );
    assert_eq!(compare_source_revision("rev-b", "rev-a"), Ordering::Greater);
}

#[test]
fn a_sql_live_head_refreshes_only_to_a_newer_epoch_of_its_authority() {
    assert!(ensure_newer_source_revision(&revision(1), &revision(2)).is_ok());
    for (current, replacement, reason) in [
        (
            revision(2),
            revision(2),
            "not a newer epoch in the live SQL authority",
        ),
        (
            revision(2),
            revision_with('b', "3"),
            "not a newer epoch in the live SQL authority",
        ),
        (
            "sql-source:broken".to_string(),
            revision(2),
            "live head has an invalid SQL source revision",
        ),
        (
            revision(2),
            "sql-source:broken".to_string(),
            "replacement has an invalid SQL source revision",
        ),
    ] {
        assert!(
            refused(ensure_newer_source_revision(&current, &replacement)).contains(reason),
            "{current} -> {replacement}"
        );
    }
}

#[test]
fn a_non_sql_live_head_refreshes_to_any_revision_that_sorts_after_it() {
    assert!(ensure_newer_source_revision("rev-1", &revision(1)).is_ok());
    for (current, replacement) in [("tz-9", revision(1)), ("rev-1", "rev-1".to_string())] {
        assert!(
            refused(ensure_newer_source_revision(current, &replacement))
                .contains("not newer than the live head"),
            "{current} -> {replacement}"
        );
    }
}
