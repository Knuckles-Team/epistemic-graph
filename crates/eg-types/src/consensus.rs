//! Durable transaction coordination records. These DTOs encode the only legal
//! lifecycle: parent `prepared -> decided -> finalized`, participant
//! `prepared -> resolved -> collected`.

use serde::{Deserialize, Serialize};

use crate::authority::AuthorityScopeV1;
use crate::contract::{
    BoundedVecV1, Digest256V1, OpaqueIdV1, ResourceIdV1, UtcUnixNanosV1, MAX_PARTICIPANTS,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TransactionDecisionV1 {
    Commit,
    Abort,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case", deny_unknown_fields)]
pub enum ParentTransactionStateV1 {
    Prepared,
    Decided { decision: TransactionDecisionV1 },
    Finalized { decision: TransactionDecisionV1 },
}

impl ParentTransactionStateV1 {
    pub fn validate_successor(self, next: Self) -> Result<(), String> {
        match (self, next) {
            (Self::Prepared, Self::Decided { .. }) => Ok(()),
            (Self::Decided { decision: current }, Self::Finalized { decision: next })
                if current == next =>
            {
                Ok(())
            }
            (current, next) if current == next => Ok(()),
            _ => Err("invalid parent transaction state transition".into()),
        }
    }

    fn decision(self) -> Option<TransactionDecisionV1> {
        match self {
            Self::Prepared => None,
            Self::Decided { decision } | Self::Finalized { decision } => Some(decision),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case", deny_unknown_fields)]
pub enum ParticipantTransactionStateV1 {
    Prepared,
    Resolved { decision: TransactionDecisionV1 },
    Collected { decision: TransactionDecisionV1 },
}

impl ParticipantTransactionStateV1 {
    pub fn validate_successor(self, next: Self) -> Result<(), String> {
        match (self, next) {
            (Self::Prepared, Self::Resolved { .. }) => Ok(()),
            (Self::Resolved { decision: current }, Self::Collected { decision: next })
                if current == next =>
            {
                Ok(())
            }
            (current, next) if current == next => Ok(()),
            _ => Err("invalid participant transaction state transition".into()),
        }
    }

    fn decision(self) -> Option<TransactionDecisionV1> {
        match self {
            Self::Prepared => None,
            Self::Resolved { decision } | Self::Collected { decision } => Some(decision),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TransactionParticipantV1 {
    pub participant_id: ResourceIdV1,
    pub storage_scope_id: ResourceIdV1,
    pub route_epoch: u64,
    pub expected_fence: u64,
    pub slice_digest: Digest256V1,
}

impl TransactionParticipantV1 {
    fn digest(&self) -> Result<Digest256V1, String> {
        Digest256V1::framed(
            b"eg/transaction-participant-descriptor/v1",
            &[
                self.participant_id.as_str().as_bytes(),
                self.storage_scope_id.as_str().as_bytes(),
                &self.route_epoch.to_be_bytes(),
                &self.expected_fence.to_be_bytes(),
                self.slice_digest.as_bytes(),
            ],
        )
    }
}

/// Coordinator-owned parent. Storage persists this record but cannot construct
/// or advance its decision.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ParentTransactionRecordV1 {
    pub transaction_id: OpaqueIdV1,
    pub mutation_id: OpaqueIdV1,
    pub scope: AuthorityScopeV1,
    pub authority_receipt_id: OpaqueIdV1,
    pub operation_replay_digest: Digest256V1,
    pub nonce_replay_digest: Digest256V1,
    pub envelope_digest: Digest256V1,
    pub policy_digest: Digest256V1,
    pub participants: BoundedVecV1<TransactionParticipantV1, MAX_PARTICIPANTS>,
    pub state: ParentTransactionStateV1,
    pub expires_at: UtcUnixNanosV1,
    pub state_digest: Digest256V1,
}

impl ParentTransactionRecordV1 {
    pub fn validate(&self) -> Result<(), String> {
        self.scope.validate()?;
        if self.participants.is_empty() {
            return Err("transaction must contain 1..=64 participants".into());
        }
        if self
            .participants
            .windows(2)
            .any(|pair| pair[0].participant_id >= pair[1].participant_id)
        {
            return Err("transaction participants must be strictly sorted and unique".into());
        }
        if self.state_digest != self.recompute_state_digest()? {
            return Err("parent transaction state digest mismatch".into());
        }
        Ok(())
    }

    pub fn recompute_state_digest(&self) -> Result<Digest256V1, String> {
        let mut participant_digests = Vec::with_capacity(self.participants.len());
        for participant in &self.participants {
            participant_digests.push(participant.digest()?);
        }
        let mut fields: Vec<&[u8]> = Vec::with_capacity(participant_digests.len() + 10);
        fields.push(self.transaction_id.as_str().as_bytes());
        fields.push(self.mutation_id.as_str().as_bytes());
        let scope = self.scope.digest()?;
        fields.push(scope.as_bytes());
        fields.push(self.authority_receipt_id.as_str().as_bytes());
        fields.push(self.operation_replay_digest.as_bytes());
        fields.push(self.nonce_replay_digest.as_bytes());
        fields.push(self.envelope_digest.as_bytes());
        fields.push(self.policy_digest.as_bytes());
        let expires_at = self.expires_at.get().to_be_bytes();
        fields.push(&expires_at);
        let state = match self.state {
            ParentTransactionStateV1::Prepared => b"prepared".as_slice(),
            ParentTransactionStateV1::Decided {
                decision: TransactionDecisionV1::Commit,
            } => b"decided_commit".as_slice(),
            ParentTransactionStateV1::Decided {
                decision: TransactionDecisionV1::Abort,
            } => b"decided_abort".as_slice(),
            ParentTransactionStateV1::Finalized {
                decision: TransactionDecisionV1::Commit,
            } => b"finalized_commit".as_slice(),
            ParentTransactionStateV1::Finalized {
                decision: TransactionDecisionV1::Abort,
            } => b"finalized_abort".as_slice(),
        };
        fields.push(state);
        for digest in &participant_digests {
            fields.push(digest.as_bytes());
        }
        Digest256V1::framed(b"eg/parent-transaction-record/v1", &fields)
    }

    pub fn recompute_binding_digest(&self) -> Result<Digest256V1, String> {
        let mut participant_digests = Vec::with_capacity(self.participants.len());
        for participant in &self.participants {
            participant_digests.push(participant.digest()?);
        }
        let scope = self.scope.digest()?;
        let expires_at = self.expires_at.get().to_be_bytes();
        let mut fields: Vec<&[u8]> = Vec::with_capacity(participant_digests.len() + 10);
        fields.push(self.transaction_id.as_str().as_bytes());
        fields.push(self.mutation_id.as_str().as_bytes());
        fields.push(scope.as_bytes());
        fields.push(self.authority_receipt_id.as_str().as_bytes());
        fields.push(self.operation_replay_digest.as_bytes());
        fields.push(self.nonce_replay_digest.as_bytes());
        fields.push(self.envelope_digest.as_bytes());
        fields.push(self.policy_digest.as_bytes());
        fields.push(&expires_at);
        for digest in &participant_digests {
            fields.push(digest.as_bytes());
        }
        Digest256V1::framed(b"eg/parent-transaction-binding/v1", &fields)
    }

    fn immutable_binding_is_unchanged(&self, next: &Self) -> bool {
        (
            &self.transaction_id,
            &self.mutation_id,
            &self.scope,
            &self.authority_receipt_id,
            &self.operation_replay_digest,
            &self.nonce_replay_digest,
            &self.envelope_digest,
            &self.policy_digest,
            &self.participants,
            &self.expires_at,
        ) == (
            &next.transaction_id,
            &next.mutation_id,
            &next.scope,
            &next.authority_receipt_id,
            &next.operation_replay_digest,
            &next.nonce_replay_digest,
            &next.envelope_digest,
            &next.policy_digest,
            &next.participants,
            &next.expires_at,
        )
    }

    pub fn validate_successor(&self, next: &Self) -> Result<(), String> {
        self.validate()?;
        next.validate()?;
        if !self.immutable_binding_is_unchanged(next) {
            return Err("parent transaction immutable binding changed".into());
        }
        self.state.validate_successor(next.state)
    }
}

/// Participant-local prepare/resolve record. A participant may only resolve to
/// the exact durable parent decision and never invent a local decision.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ParticipantTransactionRecordV1 {
    pub transaction_id: OpaqueIdV1,
    pub mutation_id: OpaqueIdV1,
    pub participant: TransactionParticipantV1,
    pub scope: AuthorityScopeV1,
    pub authority_receipt_id: OpaqueIdV1,
    pub operation_replay_digest: Digest256V1,
    pub nonce_replay_digest: Digest256V1,
    pub envelope_digest: Digest256V1,
    pub policy_digest: Digest256V1,
    pub parent_binding_digest: Digest256V1,
    pub state: ParticipantTransactionStateV1,
    pub prepared_effect_digest: Digest256V1,
    pub expires_at: UtcUnixNanosV1,
    pub state_digest: Digest256V1,
}

impl ParticipantTransactionRecordV1 {
    pub fn validate(&self) -> Result<(), String> {
        self.scope.validate()?;
        if self.state_digest != self.recompute_state_digest()? {
            return Err("participant transaction state digest mismatch".into());
        }
        Ok(())
    }

    pub fn recompute_state_digest(&self) -> Result<Digest256V1, String> {
        let state = match self.state {
            ParticipantTransactionStateV1::Prepared => b"prepared".as_slice(),
            ParticipantTransactionStateV1::Resolved {
                decision: TransactionDecisionV1::Commit,
            } => b"resolved_commit".as_slice(),
            ParticipantTransactionStateV1::Resolved {
                decision: TransactionDecisionV1::Abort,
            } => b"resolved_abort".as_slice(),
            ParticipantTransactionStateV1::Collected {
                decision: TransactionDecisionV1::Commit,
            } => b"collected_commit".as_slice(),
            ParticipantTransactionStateV1::Collected {
                decision: TransactionDecisionV1::Abort,
            } => b"collected_abort".as_slice(),
        };
        let participant = self.participant.digest()?;
        let scope = self.scope.digest()?;
        Digest256V1::framed(
            b"eg/participant-transaction-record/v1",
            &[
                self.transaction_id.as_str().as_bytes(),
                self.mutation_id.as_str().as_bytes(),
                participant.as_bytes(),
                scope.as_bytes(),
                self.authority_receipt_id.as_str().as_bytes(),
                self.operation_replay_digest.as_bytes(),
                self.nonce_replay_digest.as_bytes(),
                self.envelope_digest.as_bytes(),
                self.policy_digest.as_bytes(),
                self.parent_binding_digest.as_bytes(),
                self.prepared_effect_digest.as_bytes(),
                &self.expires_at.get().to_be_bytes(),
                state,
            ],
        )
    }

    fn immutable_binding_is_unchanged(&self, next: &Self) -> bool {
        (
            &self.transaction_id,
            &self.mutation_id,
            &self.participant,
            &self.scope,
            &self.authority_receipt_id,
            &self.operation_replay_digest,
            &self.nonce_replay_digest,
            &self.envelope_digest,
            &self.policy_digest,
            &self.parent_binding_digest,
            &self.prepared_effect_digest,
            &self.expires_at,
        ) == (
            &next.transaction_id,
            &next.mutation_id,
            &next.participant,
            &next.scope,
            &next.authority_receipt_id,
            &next.operation_replay_digest,
            &next.nonce_replay_digest,
            &next.envelope_digest,
            &next.policy_digest,
            &next.parent_binding_digest,
            &next.prepared_effect_digest,
            &next.expires_at,
        )
    }

    fn matches_exact_parent(&self, parent: &ParentTransactionRecordV1) -> Result<bool, String> {
        let exact_parent_binding = (
            &parent.transaction_id,
            &parent.mutation_id,
            &parent.scope,
            &parent.authority_receipt_id,
            &parent.operation_replay_digest,
            &parent.nonce_replay_digest,
            &parent.envelope_digest,
            &parent.policy_digest,
            &parent.expires_at,
            parent.recompute_binding_digest()?,
        ) == (
            &self.transaction_id,
            &self.mutation_id,
            &self.scope,
            &self.authority_receipt_id,
            &self.operation_replay_digest,
            &self.nonce_replay_digest,
            &self.envelope_digest,
            &self.policy_digest,
            &self.expires_at,
            self.parent_binding_digest,
        );
        let exact_member = parent
            .participants
            .iter()
            .any(|member| member == &self.participant);
        Ok(exact_parent_binding && exact_member)
    }

    fn decision_matches_parent(&self, parent: &ParentTransactionRecordV1) -> bool {
        match self.state.decision() {
            Some(participant_decision) => parent.state.decision() == Some(participant_decision),
            None => true,
        }
    }

    pub fn validate_successor(
        &self,
        next: &Self,
        parent: &ParentTransactionRecordV1,
    ) -> Result<(), String> {
        self.validate()?;
        next.validate()?;
        parent.validate()?;
        if !self.immutable_binding_is_unchanged(next) {
            return Err("participant transaction immutable binding changed".into());
        }
        self.state.validate_successor(next.state)?;
        if !next.matches_exact_parent(parent)? {
            return Err("participant does not match its exact parent or membership".into());
        }
        if !next.decision_matches_parent(parent) {
            return Err("participant decision differs from durable parent decision".into());
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::authority::ScopeKindV1;
    use crate::contract::{TenantIdV1, MAX_SCOPE_COMPONENTS};

    fn digest(byte: u8) -> Digest256V1 {
        Digest256V1::from_bytes([byte; 32])
    }

    fn scope() -> AuthorityScopeV1 {
        AuthorityScopeV1 {
            kind: ScopeKindV1::new("graph").unwrap(),
            scope_id: ResourceIdV1::new("graph:tenant:a/g").unwrap(),
            tenant: Some(TenantIdV1::new("tenant:a").unwrap()),
            parent_scope_ids: BoundedVecV1::<ResourceIdV1, MAX_SCOPE_COMPONENTS>::new(vec![
                ResourceIdV1::new("tenant:a").unwrap(),
            ])
            .unwrap(),
            graph_incarnation: None,
        }
    }

    fn participant(id: &str) -> TransactionParticipantV1 {
        TransactionParticipantV1 {
            participant_id: ResourceIdV1::new(id).unwrap(),
            storage_scope_id: ResourceIdV1::new("store:a").unwrap(),
            route_epoch: 3,
            expected_fence: 4,
            slice_digest: digest(5),
        }
    }

    fn parent(state: ParentTransactionStateV1) -> ParentTransactionRecordV1 {
        let mut parent = ParentTransactionRecordV1 {
            transaction_id: OpaqueIdV1::new("transaction:1").unwrap(),
            mutation_id: OpaqueIdV1::new("mutation:1").unwrap(),
            scope: scope(),
            authority_receipt_id: OpaqueIdV1::new("authority:1").unwrap(),
            operation_replay_digest: digest(6),
            nonce_replay_digest: digest(7),
            envelope_digest: digest(8),
            policy_digest: digest(9),
            participants: BoundedVecV1::new(vec![participant("participant:a")]).unwrap(),
            state,
            expires_at: UtcUnixNanosV1::new(1_000),
            state_digest: digest(0),
        };
        parent.state_digest = parent.recompute_state_digest().unwrap();
        parent
    }

    fn participant_record(
        parent: &ParentTransactionRecordV1,
        state: ParticipantTransactionStateV1,
    ) -> ParticipantTransactionRecordV1 {
        let mut record = ParticipantTransactionRecordV1 {
            transaction_id: parent.transaction_id.clone(),
            mutation_id: parent.mutation_id.clone(),
            participant: parent.participants.as_slice()[0].clone(),
            scope: parent.scope.clone(),
            authority_receipt_id: parent.authority_receipt_id.clone(),
            operation_replay_digest: parent.operation_replay_digest,
            nonce_replay_digest: parent.nonce_replay_digest,
            envelope_digest: parent.envelope_digest,
            policy_digest: parent.policy_digest,
            parent_binding_digest: parent.recompute_binding_digest().unwrap(),
            state,
            prepared_effect_digest: digest(10),
            expires_at: parent.expires_at,
            state_digest: digest(0),
        };
        record.state_digest = record.recompute_state_digest().unwrap();
        record
    }

    #[test]
    fn parent_and_participant_lifecycles_are_closed() {
        assert!(ParentTransactionStateV1::Prepared
            .validate_successor(ParentTransactionStateV1::Decided {
                decision: TransactionDecisionV1::Commit,
            })
            .is_ok());
        assert!(ParentTransactionStateV1::Prepared
            .validate_successor(ParentTransactionStateV1::Finalized {
                decision: TransactionDecisionV1::Commit,
            })
            .is_err());
        assert!(ParticipantTransactionStateV1::Prepared
            .validate_successor(ParticipantTransactionStateV1::Collected {
                decision: TransactionDecisionV1::Abort,
            })
            .is_err());
    }

    #[test]
    fn state_digests_cover_scope_expiry_and_all_participants() {
        let parent = parent(ParentTransactionStateV1::Prepared);
        let mut changed_expiry = parent.clone();
        changed_expiry.expires_at = UtcUnixNanosV1::new(2_000);
        assert_ne!(
            parent.state_digest,
            changed_expiry.recompute_state_digest().unwrap()
        );

        let mut changed_scope = parent.clone();
        changed_scope.scope.scope_id = ResourceIdV1::new("graph:tenant:a/other").unwrap();
        assert_ne!(
            parent.state_digest,
            changed_scope.recompute_state_digest().unwrap()
        );
    }

    #[test]
    fn participant_rejects_parent_substitution_and_missing_membership() {
        let prepared_parent = parent(ParentTransactionStateV1::Prepared);
        let current = participant_record(&prepared_parent, ParticipantTransactionStateV1::Prepared);
        let decided_parent = parent(ParentTransactionStateV1::Decided {
            decision: TransactionDecisionV1::Commit,
        });
        let next = participant_record(
            &decided_parent,
            ParticipantTransactionStateV1::Resolved {
                decision: TransactionDecisionV1::Commit,
            },
        );
        assert!(current.validate_successor(&next, &decided_parent).is_ok());

        let mut substituted = decided_parent.clone();
        substituted.mutation_id = OpaqueIdV1::new("mutation:other").unwrap();
        substituted.state_digest = substituted.recompute_state_digest().unwrap();
        assert!(current.validate_successor(&next, &substituted).is_err());

        let mut missing_member = decided_parent;
        missing_member.participants =
            BoundedVecV1::new(vec![participant("participant:other")]).unwrap();
        missing_member.state_digest = missing_member.recompute_state_digest().unwrap();
        assert!(current.validate_successor(&next, &missing_member).is_err());
    }
}
