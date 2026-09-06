//! Transplanted verbatim from the group-routed leaf. The concrete `ValueCipher`
//! it used is replaced by a deterministic in-test [`SealedPayloadOpener`] with
//! the same framing, so the assertions -- which are about record shape,
//! grouping, retention and transition legality, not about AEAD -- are unchanged.

use super::model::*;
use super::sealed::{SealedPayloadOpener, SEALED_PAYLOAD_MAGIC, SEALED_PAYLOAD_NONCE_BYTES};
use super::validate::{digest, exact_encoded_record_len, RecordByteBudget};
use super::*;
use std::io::Write;

/// Deterministic stand-in for the composition root's AEAD.
///
/// It reproduces the two properties the transplanted assertions depend on: the
/// sealed blob carries the same `[MAGIC | nonce | ...]` framing, and it hides
/// its plaintext and fails to open under a different key. The keystream and tag
/// are SHA-256 over the nonce, so this crate links no AEAD implementation.
struct TestCipher {
    nonce: [u8; SEALED_PAYLOAD_NONCE_BYTES],
}

const TEST_TAG_BYTES: usize = 32;

impl TestCipher {
    fn from_key_material(material: &[u8]) -> Self {
        let mut nonce = [0u8; SEALED_PAYLOAD_NONCE_BYTES];
        nonce.copy_from_slice(&digest(material)[..SEALED_PAYLOAD_NONCE_BYTES]);
        Self { nonce }
    }

    fn keystream(&self, len: usize) -> Vec<u8> {
        let mut out = Vec::with_capacity(len + TEST_TAG_BYTES);
        let mut counter: u64 = 0;
        while out.len() < len {
            let mut block = self.nonce.to_vec();
            block.extend_from_slice(&counter.to_be_bytes());
            out.extend_from_slice(&digest(&block));
            counter += 1;
        }
        out.truncate(len);
        out
    }

    fn mask(&self, bytes: &[u8]) -> Vec<u8> {
        self.keystream(bytes.len())
            .into_iter()
            .zip(bytes)
            .map(|(key, byte)| key ^ byte)
            .collect()
    }

    fn tag(&self, plaintext: &[u8]) -> [u8; 32] {
        let mut framed = self.nonce.to_vec();
        framed.extend_from_slice(plaintext);
        digest(&framed)
    }

    fn seal(&self, plaintext: &[u8]) -> Vec<u8> {
        let mut out =
            Vec::with_capacity(1 + SEALED_PAYLOAD_NONCE_BYTES + TEST_TAG_BYTES + plaintext.len());
        out.push(SEALED_PAYLOAD_MAGIC);
        out.extend_from_slice(&self.nonce);
        out.extend_from_slice(&self.tag(plaintext));
        out.extend_from_slice(&self.mask(plaintext));
        out
    }
}

impl SealedPayloadOpener for TestCipher {
    fn unseal(&self, sealed: &[u8]) -> Result<Vec<u8>, String> {
        let body = 1 + SEALED_PAYLOAD_NONCE_BYTES + TEST_TAG_BYTES;
        if !is_sealed_payload(sealed) || sealed.len() < body {
            return Err("sealed payload framing is invalid".to_string());
        }
        let plaintext = self.mask(&sealed[body..]);
        if self.tag(&plaintext) != sealed[1 + SEALED_PAYLOAD_NONCE_BYTES..body] {
            return Err("sealed payload does not open under this key".to_string());
        }
        Ok(plaintext)
    }
}

#[test]
fn record_keys_are_discriminated_without_a_parent_sentinel() {
    let parent = ConsensusTransactionRecordKey::Parent {
        coordinator_id: "coord/one".to_string(),
    };
    let participant = ConsensusTransactionRecordKey::Participant {
        coordinator_id: "coord/one".to_string(),
        participant_id: 7,
    };
    for key in [parent, participant] {
        let encoded = encode_record_key(&key).unwrap();
        assert_eq!(decode_record_key(&encoded).unwrap(), key);
    }
}

#[test]
fn record_counting_writer_accepts_exact_limit_and_rejects_one_more_byte() {
    let mut exact = RecordByteBudget {
        written: MAX_CONSENSUS_RECORD_BYTES - 1,
        limit: MAX_CONSENSUS_RECORD_BYTES,
    };
    assert_eq!(exact.write(&[1]).unwrap(), 1);
    assert_eq!(exact.written, MAX_CONSENSUS_RECORD_BYTES);
    assert!(exact.write(&[2]).is_err());

    let cipher = TestCipher::from_key_material(b"record-length-test");
    let record = ConsensusTransactionRecord::Participant {
        schema_version: CONSENSUS_TRANSACTION_SCHEMA_VERSION,
        group_id: 1,
        coordinator_id: "length".to_string(),
        participant_id: 1,
        retention_key: 1,
        state: ConsensusTransactionParticipantState::Prepared {
            sealed_plan: cipher.seal(b"plan"),
        },
    };
    let exact_len = exact_encoded_record_len(&record, usize::MAX).unwrap();
    assert!(exact_encoded_record_len(&record, exact_len).is_ok());
    assert!(exact_encoded_record_len(&record, exact_len - 1).is_err());
}

#[test]
fn parent_and_participant_groups_are_disjoint() {
    let cipher = TestCipher::from_key_material(b"group-role-test");
    let participant = ConsensusTransactionRecord::Participant {
        schema_version: CONSENSUS_TRANSACTION_SCHEMA_VERSION,
        group_id: 0,
        coordinator_id: "group-role".to_string(),
        participant_id: 1,
        retention_key: 1,
        state: ConsensusTransactionParticipantState::Prepared {
            sealed_plan: cipher.seal(b"plan"),
        },
    };
    assert!(participant.validate().is_err());
    let parent = ConsensusTransactionRecord::Parent {
        schema_version: CONSENSUS_TRANSACTION_SCHEMA_VERSION,
        group_id: 1,
        coordinator_id: "group-role".to_string(),
        retention_key: 1,
        binding: Box::new(ConsensusTransactionBinding {
            logical_graph: format!("sha256:{}", "1".repeat(64)),
            effective_graph: format!("sha256:{}", "2".repeat(64)),
            tenant: "tenant".to_string(),
            origin: format!("principal:sha256:{}", "3".repeat(64)),
            effective_actor_fingerprint: format!("principal:sha256:{}", "4".repeat(64)),
            operation_kind: ConsensusParentOperationKind::Transaction,
            participant_count: 1,
        }),
        state: ConsensusTransactionParentState::Prepared {
            sealed_parent_authority: cipher.seal(b"authority"),
        },
    };
    assert!(parent.validate().is_err());
}

#[test]
fn collected_drops_the_plan_but_retains_exact_child_and_parent_proofs() {
    let cipher = TestCipher::from_key_material(b"participant-test-key");
    let plan = cipher.seal(b"private plan");
    let proof = cipher.seal(b"private proof");
    let parent_proof = cipher.seal(b"parent proof");
    let proof_digest = sealed_blob_digest(&proof);
    let prepared = ConsensusTransactionRecord::Participant {
        schema_version: CONSENSUS_TRANSACTION_SCHEMA_VERSION,
        group_id: 3,
        coordinator_id: "coord".to_string(),
        participant_id: 9,
        retention_key: 11,
        state: ConsensusTransactionParticipantState::Prepared {
            sealed_plan: plan.clone(),
        },
    };
    let resolved = ConsensusTransactionRecord::Participant {
        schema_version: CONSENSUS_TRANSACTION_SCHEMA_VERSION,
        group_id: 3,
        coordinator_id: "coord".to_string(),
        participant_id: 9,
        retention_key: 11,
        state: ConsensusTransactionParticipantState::Resolved {
            decision: ConsensusTransactionDecision::Commit,
            sealed_plan: plan,
            sealed_terminal_proof: proof.clone(),
            terminal_proof_sha256: proof_digest,
        },
    };
    let collected = ConsensusTransactionRecord::Participant {
        schema_version: CONSENSUS_TRANSACTION_SCHEMA_VERSION,
        group_id: 3,
        coordinator_id: "coord".to_string(),
        participant_id: 9,
        retention_key: 11,
        state: ConsensusTransactionParticipantState::Collected {
            decision: ConsensusTransactionDecision::Commit,
            sealed_terminal_proof: proof,
            sealed_parent_terminal_proof: parent_proof.clone(),
            parent_proof_sha256: sealed_blob_digest(&parent_proof),
            retention_fence: ConsensusTransactionRetentionFence {
                applied_index: 12,
                retain_through_snapshot_index: 13,
            },
        },
    };
    assert_eq!(
        validate_transition(Some(&prepared), &resolved).unwrap(),
        ConsensusTransactionCasOutcome::Applied
    );
    assert_eq!(
        validate_transition(Some(&resolved), &collected).unwrap(),
        ConsensusTransactionCasOutcome::Applied
    );
    let encoded = encode_record(&collected).unwrap();
    assert!(!encoded.windows(12).any(|window| window == b"private plan"));
    assert!(!encoded.windows(13).any(|window| window == b"private proof"));
    collected.authenticate(&cipher).unwrap();

    let mut changed_parent = collected.clone();
    if let ConsensusTransactionRecord::Participant {
        state:
            ConsensusTransactionParticipantState::Collected {
                sealed_parent_terminal_proof,
                ..
            },
        ..
    } = &mut changed_parent
    {
        *sealed_parent_terminal_proof = cipher.seal(b"different parent proof");
    }
    assert!(changed_parent.validate().is_err());
}

#[test]
fn parent_terminalization_requires_exact_pending_crash_recovery_evidence() {
    let cipher = TestCipher::from_key_material(b"parent-finalizing-test");
    let certificate = cipher.seal(b"decision");
    let parent_authority = cipher.seal(b"parent authority");
    let terminal = cipher.seal(b"terminal");
    let binding = ConsensusTransactionBinding {
        logical_graph: format!("sha256:{}", "1".repeat(64)),
        effective_graph: format!("sha256:{}", "2".repeat(64)),
        tenant: "tenant".to_string(),
        origin: format!("principal:sha256:{}", "3".repeat(64)),
        effective_actor_fingerprint: format!("principal:sha256:{}", "4".repeat(64)),
        operation_kind: ConsensusParentOperationKind::Transaction,
        participant_count: 1,
    };
    let parent = |state| ConsensusTransactionRecord::Parent {
        schema_version: CONSENSUS_TRANSACTION_SCHEMA_VERSION,
        group_id: 0,
        coordinator_id: "coordinator".to_string(),
        retention_key: 7,
        binding: Box::new(binding.clone()),
        state,
    };
    let prepared = parent(ConsensusTransactionParentState::Prepared {
        sealed_parent_authority: parent_authority.clone(),
    });
    let decided = parent(ConsensusTransactionParentState::Decided {
        decision: ConsensusTransactionDecision::Commit,
        parent_authority_sha256: sealed_blob_digest(&parent_authority),
        sealed_parent_authority: parent_authority.clone(),
        decision_certificate_sha256: sealed_blob_digest(&certificate),
        sealed_decision_certificate: certificate.clone(),
        pending_finalization: None,
    });
    let fence = ConsensusTransactionRetentionFence {
        applied_index: 12,
        retain_through_snapshot_index: 12,
    };
    let pending = parent(ConsensusTransactionParentState::Decided {
        decision: ConsensusTransactionDecision::Commit,
        parent_authority_sha256: sealed_blob_digest(&parent_authority),
        sealed_parent_authority: parent_authority,
        decision_certificate_sha256: sealed_blob_digest(&certificate),
        sealed_decision_certificate: certificate,
        pending_finalization: Some(ConsensusTransactionPendingFinalization {
            terminal_proof_sha256: sealed_blob_digest(&terminal),
            sealed_terminal_proof: terminal.clone(),
            retention_fence: fence,
        }),
    });
    let finalized = parent(ConsensusTransactionParentState::Finalized {
        decision: ConsensusTransactionDecision::Commit,
        terminal_proof_sha256: sealed_blob_digest(&terminal),
        sealed_terminal_proof: terminal,
        retention_fence: fence,
    });
    assert_eq!(
        validate_transition(Some(&prepared), &decided).unwrap(),
        ConsensusTransactionCasOutcome::Applied
    );
    let mut rebound_authority = decided.clone();
    if let ConsensusTransactionRecord::Parent {
        state:
            ConsensusTransactionParentState::Decided {
                sealed_parent_authority,
                parent_authority_sha256,
                ..
            },
        ..
    } = &mut rebound_authority
    {
        *sealed_parent_authority = cipher.seal(b"different authority");
        *parent_authority_sha256 = sealed_blob_digest(sealed_parent_authority);
    }
    assert!(validate_transition(Some(&prepared), &rebound_authority).is_err());
    assert!(validate_transition(Some(&decided), &finalized).is_err());
    assert_eq!(
        validate_transition(Some(&decided), &pending).unwrap(),
        ConsensusTransactionCasOutcome::Applied
    );
    assert_eq!(
        validate_transition(Some(&pending), &finalized).unwrap(),
        ConsensusTransactionCasOutcome::Applied
    );
    let mut changed = finalized;
    if let ConsensusTransactionRecord::Parent {
        state:
            ConsensusTransactionParentState::Finalized {
                sealed_terminal_proof,
                ..
            },
        ..
    } = &mut changed
    {
        *sealed_terminal_proof = cipher.seal(b"changed");
    }
    assert!(validate_transition(Some(&pending), &changed).is_err());
}
