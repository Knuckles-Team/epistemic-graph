//! The typed consensus-transaction record model.
//!
//! A parent row lives in its control group; each participant row lives in its
//! participant group. Keys are explicitly discriminated, so a parent never
//! borrows a sentinel participant id. Values are typed and bounded.

use super::sealed::SealedPayloadOpener;
use super::validate::{
    exact_encoded_record_len, validate_blob, validate_coordinator_id, validate_parent_state,
    validate_participant_state,
};
use super::{
    CONSENSUS_TRANSACTION_SCHEMA_VERSION, MAX_CONSENSUS_RECORD_BYTES, MAX_COORDINATOR_ID_BYTES,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConsensusTransactionDecision {
    Commit,
    Abort,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConsensusParentOperationKind {
    Transaction,
    Sparql,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConsensusTransactionRecordKey {
    Parent {
        coordinator_id: String,
    },
    Participant {
        coordinator_id: String,
        participant_id: u64,
    },
}

/// Public routing/idempotency identity retained after the private parent payload
/// is discarded. A same coordinator key cannot be rebound during its retention
/// window, even when the caller presents different encrypted bytes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConsensusTransactionBinding {
    pub logical_graph: String,
    pub effective_graph: String,
    pub tenant: String,
    pub origin: String,
    pub effective_actor_fingerprint: String,
    pub operation_kind: ConsensusParentOperationKind,
    pub participant_count: u32,
}

/// A terminal row is eligible for collection only after both replicated apply
/// and a snapshot covering that apply point have crossed these durable fences.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConsensusTransactionRetentionFence {
    pub applied_index: u64,
    pub retain_through_snapshot_index: u64,
}

/// Exact leader-supplied terminal bytes retained before the generic saga drops
/// its private payload. If a process dies between the two physical database
/// commits, startup can authenticate the committed saga result against this
/// evidence and deterministically finish the typed parent transition.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConsensusTransactionPendingFinalization {
    #[serde(with = "serde_bytes")]
    pub sealed_terminal_proof: Vec<u8>,
    pub terminal_proof_sha256: [u8; 32],
    pub retention_fence: ConsensusTransactionRetentionFence,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum ConsensusTransactionParentState {
    Prepared {
        #[serde(with = "serde_bytes")]
        sealed_parent_authority: Vec<u8>,
    },
    Decided {
        decision: ConsensusTransactionDecision,
        #[serde(with = "serde_bytes")]
        sealed_parent_authority: Vec<u8>,
        parent_authority_sha256: [u8; 32],
        #[serde(with = "serde_bytes")]
        sealed_decision_certificate: Vec<u8>,
        decision_certificate_sha256: [u8; 32],
        pending_finalization: Option<ConsensusTransactionPendingFinalization>,
    },
    Finalized {
        decision: ConsensusTransactionDecision,
        #[serde(with = "serde_bytes")]
        sealed_terminal_proof: Vec<u8>,
        terminal_proof_sha256: [u8; 32],
        retention_fence: ConsensusTransactionRetentionFence,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum ConsensusTransactionParticipantState {
    Prepared {
        #[serde(with = "serde_bytes")]
        sealed_plan: Vec<u8>,
    },
    Resolved {
        decision: ConsensusTransactionDecision,
        #[serde(with = "serde_bytes")]
        sealed_plan: Vec<u8>,
        #[serde(with = "serde_bytes")]
        sealed_terminal_proof: Vec<u8>,
        terminal_proof_sha256: [u8; 32],
    },
    Collected {
        decision: ConsensusTransactionDecision,
        #[serde(with = "serde_bytes")]
        sealed_terminal_proof: Vec<u8>,
        #[serde(with = "serde_bytes")]
        sealed_parent_terminal_proof: Vec<u8>,
        parent_proof_sha256: [u8; 32],
        retention_fence: ConsensusTransactionRetentionFence,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum ConsensusTransactionRecord {
    Parent {
        schema_version: u16,
        group_id: u64,
        coordinator_id: String,
        retention_key: u64,
        // Boxed only to keep the two variants' sizes comparable; `Box<T>`
        // serializes exactly as `T`, so the durable record format is unchanged.
        binding: Box<ConsensusTransactionBinding>,
        state: ConsensusTransactionParentState,
    },
    Participant {
        schema_version: u16,
        group_id: u64,
        coordinator_id: String,
        participant_id: u64,
        retention_key: u64,
        state: ConsensusTransactionParticipantState,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConsensusTransactionCasOutcome {
    Applied,
    Replayed,
    Retired,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConsensusTransactionGroupMeta {
    pub record_count: u64,
    pub record_bytes: u64,
    pub retention_floor: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConsensusTransactionGcRequest {
    pub retention_floor: u64,
    pub applied_index: u64,
    pub snapshot_index: u64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ConsensusTransactionGcOutcome {
    pub removed_records: u64,
    pub removed_bytes: u64,
    pub retention_floor: u64,
}

/// Resume token for bounded startup/recovery traversal. The next call begins
/// strictly after this physical shard/group pair.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConsensusTransactionScanCursor {
    pub physical_shard_index: usize,
    pub group_id: u64,
}

/// At most one admitted group (192 MiB) is resident during recovery.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsensusTransactionScanPage {
    pub cursor: ConsensusTransactionScanCursor,
    pub records: Vec<ConsensusTransactionRecord>,
}

/// Exact opaque group image carried in a Raft snapshot. `replace_raw` validates
/// every key/value pair before atomically replacing this group's rows.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConsensusTransactionRawSnapshot {
    pub schema_version: u16,
    pub group_id: u64,
    pub meta: ConsensusTransactionGroupMeta,
    pub records: Vec<(String, Vec<u8>)>,
}

impl ConsensusTransactionRecord {
    pub fn group_id(&self) -> u64 {
        match self {
            Self::Parent { group_id, .. } | Self::Participant { group_id, .. } => *group_id,
        }
    }

    pub fn key(&self) -> ConsensusTransactionRecordKey {
        match self {
            Self::Parent { coordinator_id, .. } => ConsensusTransactionRecordKey::Parent {
                coordinator_id: coordinator_id.clone(),
            },
            Self::Participant {
                coordinator_id,
                participant_id,
                ..
            } => ConsensusTransactionRecordKey::Participant {
                coordinator_id: coordinator_id.clone(),
                participant_id: *participant_id,
            },
        }
    }

    pub fn retention_key(&self) -> u64 {
        match self {
            Self::Parent { retention_key, .. } | Self::Participant { retention_key, .. } => {
                *retention_key
            }
        }
    }

    pub fn terminal_retention_fence(&self) -> Option<ConsensusTransactionRetentionFence> {
        match self {
            Self::Parent {
                state:
                    ConsensusTransactionParentState::Finalized {
                        retention_fence, ..
                    },
                ..
            }
            | Self::Participant {
                state:
                    ConsensusTransactionParticipantState::Collected {
                        retention_fence, ..
                    },
                ..
            } => Some(*retention_fence),
            _ => None,
        }
    }

    pub fn validate(&self) -> Result<(), String> {
        let (schema_version, coordinator_id, retention_key) = match self {
            Self::Parent {
                schema_version,
                group_id,
                coordinator_id,
                retention_key,
                binding,
                state,
                ..
            } => {
                if *group_id != 0 {
                    return Err("consensus parent record must live in control group 0".to_string());
                }
                binding.validate()?;
                validate_parent_state(state)?;
                (*schema_version, coordinator_id, *retention_key)
            }
            Self::Participant {
                schema_version,
                group_id,
                coordinator_id,
                participant_id,
                retention_key,
                state,
                ..
            } => {
                if *group_id == 0 || *participant_id == 0 {
                    return Err("consensus participant group and id must be positive".to_string());
                }
                validate_participant_state(state)?;
                (*schema_version, coordinator_id, *retention_key)
            }
        };
        if schema_version != CONSENSUS_TRANSACTION_SCHEMA_VERSION {
            return Err("unsupported consensus transaction record schema".to_string());
        }
        if retention_key == 0 {
            return Err("consensus transaction retention key must be positive".to_string());
        }
        validate_coordinator_id(coordinator_id)?;
        exact_encoded_record_len(self, MAX_CONSENSUS_RECORD_BYTES)?;
        Ok(())
    }

    /// Authenticate every sealed field before a row can authorize recovery or
    /// enter a direct snapshot replacement. Certificate bodies stay opaque to
    /// storage; successful AEAD open plus the stored digest is its boundary.
    pub fn authenticate(&self, cipher: &dyn SealedPayloadOpener) -> Result<(), String> {
        self.validate()?;
        let authenticate = |sealed: &[u8], what: &str| -> Result<(), String> {
            let plaintext = cipher
                .unseal(sealed)
                .map_err(|_| format!("consensus transaction {what} authentication failed"))?;
            validate_blob(&plaintext, what)
        };
        match self {
            Self::Parent { state, .. } => match state {
                ConsensusTransactionParentState::Prepared {
                    sealed_parent_authority,
                } => authenticate(sealed_parent_authority, "parent authority"),
                ConsensusTransactionParentState::Decided {
                    sealed_parent_authority,
                    sealed_decision_certificate,
                    pending_finalization,
                    ..
                } => {
                    authenticate(sealed_parent_authority, "parent authority")?;
                    authenticate(sealed_decision_certificate, "decision certificate")?;
                    if let Some(pending) = pending_finalization {
                        authenticate(
                            &pending.sealed_terminal_proof,
                            "pending parent terminal proof",
                        )?;
                    }
                    Ok(())
                }
                ConsensusTransactionParentState::Finalized {
                    sealed_terminal_proof,
                    ..
                } => authenticate(sealed_terminal_proof, "parent terminal proof"),
            },
            Self::Participant { state, .. } => match state {
                ConsensusTransactionParticipantState::Prepared { sealed_plan } => {
                    authenticate(sealed_plan, "participant plan")
                }
                ConsensusTransactionParticipantState::Resolved {
                    sealed_plan,
                    sealed_terminal_proof,
                    ..
                } => {
                    authenticate(sealed_plan, "participant plan")?;
                    authenticate(sealed_terminal_proof, "participant terminal proof")
                }
                ConsensusTransactionParticipantState::Collected {
                    sealed_terminal_proof,
                    sealed_parent_terminal_proof,
                    ..
                } => {
                    authenticate(
                        sealed_terminal_proof,
                        "collected participant terminal proof",
                    )?;
                    authenticate(sealed_parent_terminal_proof, "parent terminal proof")
                }
            },
        }
    }
}

impl ConsensusTransactionBinding {
    pub(super) fn validate(&self) -> Result<(), String> {
        for (value, what) in [
            (&self.logical_graph, "logical graph"),
            (&self.effective_graph, "effective graph"),
            (&self.tenant, "tenant"),
            (&self.origin, "origin"),
        ] {
            if value.is_empty() || value.len() > MAX_COORDINATOR_ID_BYTES {
                return Err(format!("consensus transaction {what} exceeds limits"));
            }
        }
        let fingerprint = self.effective_actor_fingerprint.as_str();
        if fingerprint.len() != "principal:sha256:".len() + 64
            || !fingerprint.starts_with("principal:sha256:")
            || !fingerprint["principal:sha256:".len()..]
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err("invalid consensus transaction effective actor fingerprint".to_string());
        }
        if self.participant_count == 0 {
            return Err("invalid consensus transaction participant count".to_string());
        }
        Ok(())
    }
}

impl ConsensusTransactionRetentionFence {
    pub(super) fn validate(&self) -> Result<(), String> {
        if self.applied_index == 0 || self.retain_through_snapshot_index < self.applied_index {
            return Err("invalid consensus transaction retention fence".to_string());
        }
        Ok(())
    }
}
