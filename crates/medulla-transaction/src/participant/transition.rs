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

/// Validate one compare-and-swap between two durable record states.
///
/// Split into the four truthful phases the original single function ran in
/// sequence: exact replay, first write, identity invariance, and the legal
/// state step. Behaviour is unchanged -- each phase is the original code of
/// that phase, in the original order.
pub fn validate_transition(
    current: Option<&ConsensusTransactionRecord>,
    replacement: &ConsensusTransactionRecord,
) -> Result<ConsensusTransactionCasOutcome, String> {
    replacement.validate()?;
    if current == Some(replacement) {
        return Ok(ConsensusTransactionCasOutcome::Replayed);
    }
    let Some(current) = current else {
        return validate_first_write(replacement);
    };
    validate_invariant_identity(current, replacement)?;
    validate_state_step(current, replacement)
        .then_some(ConsensusTransactionCasOutcome::Applied)
        .ok_or_else(|| "invalid consensus transaction state transition".to_string())
}

/// A record may only enter the store prepared.
fn validate_first_write(
    replacement: &ConsensusTransactionRecord,
) -> Result<ConsensusTransactionCasOutcome, String> {
    match replacement {
        ConsensusTransactionRecord::Parent {
            state: ConsensusTransactionParentState::Prepared { .. },
            ..
        }
        | ConsensusTransactionRecord::Participant {
            state: ConsensusTransactionParticipantState::Prepared { .. },
            ..
        } => Ok(ConsensusTransactionCasOutcome::Applied),
        _ => Err("consensus transaction record must begin prepared".to_string()),
    }
}

/// Key, group, retention key and (for a parent) the logical binding are the
/// record's identity and may never change across a transition.
fn validate_invariant_identity(
    current: &ConsensusTransactionRecord,
    replacement: &ConsensusTransactionRecord,
) -> Result<(), String> {
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
    Ok(())
}

/// Is this an accepted state step, with every field the step must carry forward
/// carried forward exactly?
fn validate_state_step(
    current: &ConsensusTransactionRecord,
    replacement: &ConsensusTransactionRecord,
) -> bool {
    match (current, replacement) {
        (
            ConsensusTransactionRecord::Parent {
                state: current_state,
                ..
            },
            ConsensusTransactionRecord::Parent {
                state: next_state, ..
            },
        ) => validate_parent_step(current_state, next_state),
        (
            ConsensusTransactionRecord::Participant {
                state: current_state,
                ..
            },
            ConsensusTransactionRecord::Participant {
                state: next_state, ..
            },
        ) => validate_participant_step(current_state, next_state),
        _ => false,
    }
}

/// Which of the three legal parent steps `current -> replacement` shapes as, dispatched
/// on variant and `pending_finalization` alone; each shape's own field-level equality
/// checks live in its own helper below purely to keep this dispatcher under the
/// complexity cap. The transition rules themselves are unchanged.
fn validate_parent_step(
    current: &ConsensusTransactionParentState,
    replacement: &ConsensusTransactionParentState,
) -> bool {
    match (current, replacement) {
        (
            ConsensusTransactionParentState::Prepared { .. },
            ConsensusTransactionParentState::Decided {
                pending_finalization: None,
                ..
            },
        ) => validate_prepared_to_decided(current, replacement),
        (
            ConsensusTransactionParentState::Decided {
                pending_finalization: None,
                ..
            },
            ConsensusTransactionParentState::Decided {
                pending_finalization: Some(_),
                ..
            },
        ) => validate_decided_to_pending_finalization(current, replacement),
        (
            ConsensusTransactionParentState::Decided {
                pending_finalization: Some(_),
                ..
            },
            ConsensusTransactionParentState::Finalized { .. },
        ) => validate_decided_to_finalized(current, replacement),
        _ => false,
    }
}

/// `Prepared -> Decided` (the first decision): the sealed parent authority is carried
/// forward exactly.
fn validate_prepared_to_decided(
    current: &ConsensusTransactionParentState,
    replacement: &ConsensusTransactionParentState,
) -> bool {
    let (
        ConsensusTransactionParentState::Prepared {
            sealed_parent_authority,
        },
        ConsensusTransactionParentState::Decided {
            sealed_parent_authority: next_parent_authority,
            pending_finalization: None,
            ..
        },
    ) = (current, replacement)
    else {
        unreachable!("validate_prepared_to_decided routed a non prepared->decided pair here");
    };
    sealed_parent_authority == next_parent_authority
}

/// `Decided -> Decided` (finalization begins pending): every already-decided field is
/// carried forward exactly; only `pending_finalization` moves from `None` to `Some`.
fn validate_decided_to_pending_finalization(
    current: &ConsensusTransactionParentState,
    replacement: &ConsensusTransactionParentState,
) -> bool {
    let (
        ConsensusTransactionParentState::Decided {
            sealed_parent_authority,
            parent_authority_sha256,
            decision,
            sealed_decision_certificate,
            decision_certificate_sha256,
            pending_finalization: None,
        },
        ConsensusTransactionParentState::Decided {
            decision: next_decision,
            sealed_parent_authority: next_parent_authority,
            parent_authority_sha256: next_parent_authority_sha256,
            sealed_decision_certificate: next_certificate,
            decision_certificate_sha256: next_certificate_sha256,
            pending_finalization: Some(_),
        },
    ) = (current, replacement)
    else {
        unreachable!(
            "validate_decided_to_pending_finalization routed a non decided->decided pair here"
        );
    };
    decision == next_decision
        && sealed_parent_authority == next_parent_authority
        && parent_authority_sha256 == next_parent_authority_sha256
        && sealed_decision_certificate == next_certificate
        && decision_certificate_sha256 == next_certificate_sha256
}

/// `Decided -> Finalized`: the decision and the already-computed pending-finalization
/// proof are carried forward exactly, and the parent's decision-certificate digest is
/// non-zero (a zero digest means it was never sealed).
fn validate_decided_to_finalized(
    current: &ConsensusTransactionParentState,
    replacement: &ConsensusTransactionParentState,
) -> bool {
    let (
        ConsensusTransactionParentState::Decided {
            decision,
            decision_certificate_sha256,
            pending_finalization: Some(pending),
            ..
        },
        ConsensusTransactionParentState::Finalized {
            decision: next_decision,
            sealed_terminal_proof,
            terminal_proof_sha256,
            retention_fence,
        },
    ) = (current, replacement)
    else {
        unreachable!("validate_decided_to_finalized routed a non decided->finalized pair here");
    };
    decision == next_decision
        && !decision_certificate_sha256.iter().all(|byte| *byte == 0)
        && pending.sealed_terminal_proof.as_slice() == sealed_terminal_proof.as_slice()
        && pending.terminal_proof_sha256 == *terminal_proof_sha256
        && pending.retention_fence == *retention_fence
}

fn validate_participant_step(
    current: &ConsensusTransactionParticipantState,
    replacement: &ConsensusTransactionParticipantState,
) -> bool {
    match (current, replacement) {
        (
            ConsensusTransactionParticipantState::Prepared { sealed_plan },
            ConsensusTransactionParticipantState::Resolved {
                sealed_plan: next_plan,
                ..
            },
        ) => sealed_plan == next_plan,
        (
            ConsensusTransactionParticipantState::Resolved {
                decision,
                sealed_terminal_proof,
                ..
            },
            ConsensusTransactionParticipantState::Collected {
                decision: next_decision,
                sealed_terminal_proof: next_proof,
                ..
            },
        ) => decision == next_decision && sealed_terminal_proof == next_proof,
        _ => false,
    }
}
