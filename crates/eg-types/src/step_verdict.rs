//! Typed per-step verification verdict (EG-DURABLE-KERNEL-R089.1).
//!
//! A verdict binds one reasoning step's identity and the SHA-256 of its output to
//! a success or failure state, an uncertainty bound in `[0, 1]`, and a proof
//! reference. Consumer refusal of conclusions derived from a failed step is
//! EG-DURABLE-KERNEL-R089.2.

use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum VerdictState {
    Success,
    Failure,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VerificationVerdict {
    pub step_id: String,
    /// Lowercase hex SHA-256 of the step's reasoning output.
    pub reasoning_output_hash: String,
    pub state: VerdictState,
    /// Upper bound on the verdict's uncertainty, in `[0, 1]`.
    pub uncertainty_bound: f64,
    pub proof_ref: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum VerdictError {
    #[error("verification verdict names no step")]
    EmptyStepId,
    #[error("reasoning output hash is not a lowercase hex SHA-256")]
    MalformedOutputHash,
    #[error("uncertainty bound must be a finite value in [0, 1]")]
    UncertaintyOutOfBounds,
    #[error("verification verdict names no proof reference")]
    EmptyProofRef,
}

impl VerificationVerdict {
    pub fn validate(&self) -> Result<(), VerdictError> {
        if self.step_id.trim().is_empty() {
            return Err(VerdictError::EmptyStepId);
        }
        let hash = &self.reasoning_output_hash;
        if hash.len() != 64 || !hash.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f')) {
            return Err(VerdictError::MalformedOutputHash);
        }
        if !(0.0..=1.0).contains(&self.uncertainty_bound) {
            return Err(VerdictError::UncertaintyOutOfBounds);
        }
        if self.proof_ref.trim().is_empty() {
            return Err(VerdictError::EmptyProofRef);
        }
        Ok(())
    }

    pub fn is_success(&self) -> bool {
        self.state == VerdictState::Success
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn verdict(state: VerdictState) -> VerificationVerdict {
        VerificationVerdict {
            step_id: "run-7/step-3".into(),
            reasoning_output_hash: "ab".repeat(32),
            state,
            uncertainty_bound: 0.05,
            proof_ref: "proof:sha256:feed".into(),
        }
    }

    // spec: EG-DURABLE-KERNEL-R089.1
    #[test]
    fn both_states_validate_and_round_trip_with_their_proof_reference() {
        for state in [VerdictState::Success, VerdictState::Failure] {
            let v = verdict(state);
            v.validate().unwrap();
            assert_eq!(v.is_success(), state == VerdictState::Success);
            let packed = rmp_serde::to_vec_named(&v).unwrap();
            let back: VerificationVerdict = rmp_serde::from_slice(&packed).unwrap();
            assert_eq!(back, v);
            assert_eq!(back.proof_ref, "proof:sha256:feed");
        }
        let json = serde_json::to_value(verdict(VerdictState::Failure)).unwrap();
        assert_eq!(json["state"], "failure");
    }

    // spec: EG-DURABLE-KERNEL-R089.1
    #[test]
    fn malformed_verdicts_are_refused() {
        for bound in [-0.01, 1.01, f64::NAN, f64::INFINITY] {
            let mut v = verdict(VerdictState::Success);
            v.uncertainty_bound = bound;
            assert_eq!(v.validate(), Err(VerdictError::UncertaintyOutOfBounds));
        }
        for bound in [0.0, 1.0] {
            let mut v = verdict(VerdictState::Success);
            v.uncertainty_bound = bound;
            v.validate().unwrap();
        }
        let mut v = verdict(VerdictState::Success);
        v.step_id = " ".into();
        assert_eq!(v.validate(), Err(VerdictError::EmptyStepId));
        for hash in ["AB".repeat(32), "ab".repeat(31), "zz".repeat(32)] {
            let mut v = verdict(VerdictState::Success);
            v.reasoning_output_hash = hash;
            assert_eq!(v.validate(), Err(VerdictError::MalformedOutputHash));
        }
        let mut v = verdict(VerdictState::Failure);
        v.proof_ref.clear();
        assert_eq!(v.validate(), Err(VerdictError::EmptyProofRef));
    }
}
