//! Bounded field validation shared by the record model and its codecs.

use super::model::{
    ConsensusTransactionParentState, ConsensusTransactionParticipantState,
    ConsensusTransactionRecord,
};
use super::sealed::is_sealed_payload;
use super::{MAX_CONSENSUS_RECORD_BYTES, MAX_COORDINATOR_ID_BYTES};
use sha2::{Digest, Sha256};
use std::io::Write;

pub(super) fn validate_coordinator_id(coordinator_id: &str) -> Result<(), String> {
    if coordinator_id.is_empty() || coordinator_id.len() > MAX_COORDINATOR_ID_BYTES {
        return Err("consensus coordinator id exceeds limits".to_string());
    }
    Ok(())
}

pub(super) fn validate_blob(value: &[u8], what: &str) -> Result<(), String> {
    if value.is_empty() || value.len() > MAX_CONSENSUS_RECORD_BYTES {
        return Err(format!("consensus transaction {what} exceeds limits"));
    }
    Ok(())
}

pub(super) fn digest(value: &[u8]) -> [u8; 32] {
    Sha256::digest(value).into()
}

pub(super) fn validate_sealed(value: &[u8], what: &str) -> Result<(), String> {
    validate_blob(value, what)?;
    if !is_sealed_payload(value) {
        return Err(format!(
            "consensus transaction {what} is not authenticated ciphertext"
        ));
    }
    Ok(())
}

pub(super) fn validate_digest(value: &[u8], expected: &[u8; 32], what: &str) -> Result<(), String> {
    if digest(value) != *expected {
        return Err(format!("consensus transaction {what} digest changed"));
    }
    Ok(())
}

pub(super) fn validate_parent_state(state: &ConsensusTransactionParentState) -> Result<(), String> {
    match state {
        ConsensusTransactionParentState::Prepared {
            sealed_parent_authority,
        } => validate_sealed(sealed_parent_authority, "parent authority"),
        ConsensusTransactionParentState::Decided {
            sealed_parent_authority,
            parent_authority_sha256,
            sealed_decision_certificate,
            decision_certificate_sha256,
            pending_finalization,
            ..
        } => {
            validate_sealed(sealed_parent_authority, "parent authority")?;
            validate_digest(
                sealed_parent_authority,
                parent_authority_sha256,
                "parent authority",
            )?;
            validate_sealed(sealed_decision_certificate, "decision certificate")?;
            validate_digest(
                sealed_decision_certificate,
                decision_certificate_sha256,
                "decision certificate",
            )?;
            if let Some(pending) = pending_finalization {
                pending.retention_fence.validate()?;
                validate_sealed(
                    &pending.sealed_terminal_proof,
                    "pending parent terminal proof",
                )?;
                validate_digest(
                    &pending.sealed_terminal_proof,
                    &pending.terminal_proof_sha256,
                    "pending parent terminal proof",
                )?;
            }
            Ok(())
        }
        ConsensusTransactionParentState::Finalized {
            sealed_terminal_proof,
            terminal_proof_sha256,
            retention_fence,
            ..
        } => {
            retention_fence.validate()?;
            validate_sealed(sealed_terminal_proof, "parent terminal proof")?;
            validate_digest(
                sealed_terminal_proof,
                terminal_proof_sha256,
                "parent terminal proof",
            )
        }
    }
}

pub(super) fn validate_participant_state(
    state: &ConsensusTransactionParticipantState,
) -> Result<(), String> {
    match state {
        ConsensusTransactionParticipantState::Prepared { sealed_plan } => {
            validate_sealed(sealed_plan, "participant plan")
        }
        ConsensusTransactionParticipantState::Resolved {
            sealed_plan,
            sealed_terminal_proof,
            terminal_proof_sha256,
            ..
        } => {
            validate_sealed(sealed_plan, "participant plan")?;
            validate_sealed(sealed_terminal_proof, "participant terminal proof")?;
            validate_digest(
                sealed_terminal_proof,
                terminal_proof_sha256,
                "participant terminal proof",
            )
        }
        ConsensusTransactionParticipantState::Collected {
            sealed_terminal_proof,
            sealed_parent_terminal_proof,
            parent_proof_sha256,
            retention_fence,
            ..
        } => {
            retention_fence.validate()?;
            validate_sealed(
                sealed_terminal_proof,
                "collected participant terminal proof",
            )?;
            validate_sealed(sealed_parent_terminal_proof, "parent terminal proof")?;
            validate_digest(
                sealed_parent_terminal_proof,
                parent_proof_sha256,
                "parent terminal proof",
            )
        }
    }
}

pub(super) fn exact_encoded_record_len(
    record: &ConsensusTransactionRecord,
    limit: usize,
) -> Result<usize, String> {
    let mut budget = RecordByteBudget { written: 0, limit };
    rmp_serde::encode::write_named(&mut budget, record)
        .map_err(|_| "consensus transaction record exceeds limits".to_string())?;
    Ok(budget.written)
}

pub(super) struct RecordByteBudget {
    pub(super) written: usize,
    pub(super) limit: usize,
}

impl Write for RecordByteBudget {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.written = self
            .written
            .checked_add(bytes.len())
            .filter(|written| *written <= self.limit)
            .ok_or_else(|| std::io::Error::other("consensus record budget exceeded"))?;
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
