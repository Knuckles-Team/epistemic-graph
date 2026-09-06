//! Exact compare-and-swap transitions between durable record states.

use super::key_codec::{decode_record_key, encode_record_key};
use super::model::{
    ConsensusTransactionCasOutcome, ConsensusTransactionParentState,
    ConsensusTransactionParticipantState, ConsensusTransactionRecord,
};

pub fn validate_key_matches_record(
    group_id: u64,
    encoded_key: &str,
    record: &ConsensusTransactionRecord,
) -> Result<(), String> {
    if record.group_id() != group_id
        || decode_record_key(encoded_key)? != record.key()
        || encode_record_key(&record.key())? != encoded_key
    {
        return Err(
            "consensus transaction record key does not match its typed identity".to_string(),
        );
    }
    Ok(())
}

pub fn validate_transition(
    current: Option<&ConsensusTransactionRecord>,
    replacement: &ConsensusTransactionRecord,
) -> Result<ConsensusTransactionCasOutcome, String> {
    replacement.validate()?;
    if current == Some(replacement) {
        return Ok(ConsensusTransactionCasOutcome::Replayed);
    }
    let Some(current) = current else {
        return match replacement {
            ConsensusTransactionRecord::Parent {
                state: ConsensusTransactionParentState::Prepared { .. },
                ..
            }
            | ConsensusTransactionRecord::Participant {
                state: ConsensusTransactionParticipantState::Prepared { .. },
                ..
            } => Ok(ConsensusTransactionCasOutcome::Applied),
            _ => Err("consensus transaction record must begin prepared".to_string()),
        };
    };
    if current.key() != replacement.key() || current.group_id() != replacement.group_id() {
        return Err("consensus transaction transition changed identity".to_string());
    }
    if current.retention_key() != replacement.retention_key() {
        return Err("consensus transaction transition changed retention identity".to_string());
    }
    if let (
        ConsensusTransactionRecord::Parent {
            binding: current_binding,
            ..
        },
        ConsensusTransactionRecord::Parent {
            binding: replacement_binding,
            ..
        },
    ) = (current, replacement)
    {
        if current_binding != replacement_binding {
            return Err(
                "consensus transaction coordinator key changed logical binding".to_string(),
            );
        }
    }
    let valid = match (current, replacement) {
        (
            ConsensusTransactionRecord::Parent {
                state:
                    ConsensusTransactionParentState::Prepared {
                        sealed_parent_authority,
                    },
                ..
            },
            ConsensusTransactionRecord::Parent {
                state:
                    ConsensusTransactionParentState::Decided {
                        sealed_parent_authority: next_parent_authority,
                        pending_finalization: None,
                        ..
                    },
                ..
            },
        ) => sealed_parent_authority == next_parent_authority,
        (
            ConsensusTransactionRecord::Parent {
                state:
                    ConsensusTransactionParentState::Decided {
                        sealed_parent_authority,
                        parent_authority_sha256,
                        decision,
                        sealed_decision_certificate,
                        decision_certificate_sha256,
                        pending_finalization: None,
                    },
                ..
            },
            ConsensusTransactionRecord::Parent {
                state:
                    ConsensusTransactionParentState::Decided {
                        decision: next_decision,
                        sealed_parent_authority: next_parent_authority,
                        parent_authority_sha256: next_parent_authority_sha256,
                        sealed_decision_certificate: next_certificate,
                        decision_certificate_sha256: next_certificate_sha256,
                        pending_finalization: Some(_),
                    },
                ..
            },
        ) => {
            decision == next_decision
                && sealed_parent_authority == next_parent_authority
                && parent_authority_sha256 == next_parent_authority_sha256
                && sealed_decision_certificate == next_certificate
                && decision_certificate_sha256 == next_certificate_sha256
        }
        (
            ConsensusTransactionRecord::Parent {
                state:
                    ConsensusTransactionParentState::Decided {
                        decision,
                        decision_certificate_sha256,
                        pending_finalization: Some(pending),
                        ..
                    },
                ..
            },
            ConsensusTransactionRecord::Parent {
                state:
                    ConsensusTransactionParentState::Finalized {
                        decision: next_decision,
                        sealed_terminal_proof,
                        terminal_proof_sha256,
                        retention_fence,
                        ..
                    },
                ..
            },
        ) => {
            decision == next_decision
                && !decision_certificate_sha256.iter().all(|byte| *byte == 0)
                && pending.sealed_terminal_proof.as_slice()
                    == sealed_terminal_proof.as_slice()
                && pending.terminal_proof_sha256 == *terminal_proof_sha256
                && pending.retention_fence == *retention_fence
        }
        (
            ConsensusTransactionRecord::Participant {
                state: ConsensusTransactionParticipantState::Prepared { sealed_plan },
                ..
            },
            ConsensusTransactionRecord::Participant {
                state:
                    ConsensusTransactionParticipantState::Resolved {
                        sealed_plan: next_plan,
                        ..
                    },
                ..
            },
        ) => sealed_plan == next_plan,
        (
            ConsensusTransactionRecord::Participant {
                state:
                    ConsensusTransactionParticipantState::Resolved {
                        decision,
                        sealed_terminal_proof,
                        ..
                    },
                ..
            },
            ConsensusTransactionRecord::Participant {
                state:
                    ConsensusTransactionParticipantState::Collected {
                        decision: next_decision,
                        sealed_terminal_proof: next_proof,
                        ..
                    },
                ..
            },
        ) => decision == next_decision && sealed_terminal_proof == next_proof,
        _ => false,
    };
    valid
        .then_some(ConsensusTransactionCasOutcome::Applied)
        .ok_or_else(|| "invalid consensus transaction state transition".to_string())
}
