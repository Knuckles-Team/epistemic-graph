//! Typed outbox records owned semantically by the mutation kernel and persisted
//! through the storage kernel's internal transaction port.

use serde::{Deserialize, Serialize};

use crate::authority::AuthorityScope;
use crate::contract::{
    BoundedVec, Digest256, OpaqueId, RecordBytes, ResourceId, TenantId, UtcUnixNanos,
};
use crate::mutation::{MutationEnvelope, MutationReceipt};

pub const MAX_OUTBOX_HEADERS: usize = 64;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OutboxHeader {
    pub name: ResourceId,
    pub value: OpaqueId,
}

/// An event requested by a domain mutation. The mutation kernel assigns its
/// immutable event identity and commit binding; domains cannot mark delivery.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OutboxIntent {
    pub tenant: TenantId,
    pub destination_scope_digest: Digest256,
    pub topic: ResourceId,
    pub partition_key: ResourceId,
    pub event_schema: ResourceId,
    pub payload: RecordBytes,
    pub payload_digest: Digest256,
    pub headers: BoundedVec<OutboxHeader, MAX_OUTBOX_HEADERS>,
}

impl OutboxIntent {
    pub fn validate(&self) -> Result<(), String> {
        if self.payload.digest()? != self.payload_digest {
            return Err("outbox payload digest does not match its bytes".into());
        }
        if self
            .headers
            .windows(2)
            .any(|pair| pair[0].name >= pair[1].name)
        {
            return Err("outbox headers must be strictly sorted and unique".into());
        }
        Ok(())
    }

    pub(crate) fn validate_for_scope(
        &self,
        tenant: &TenantId,
        scope: &AuthorityScope,
    ) -> Result<(), String> {
        self.validate()?;
        if &self.tenant != tenant
            || scope.tenant.as_ref() != Some(tenant)
            || self.destination_scope_digest != scope.digest()?
        {
            return Err("outbox destination differs from authorized tenant/scope".into());
        }
        Ok(())
    }

    pub fn digest(&self) -> Result<Digest256, String> {
        self.validate()?;
        let payload_digest = self.payload.digest()?;
        let mut fields: Vec<&[u8]> = Vec::with_capacity(self.headers.len() * 2 + 6);
        fields.push(self.topic.as_str().as_bytes());
        fields.push(self.tenant.as_str().as_bytes());
        fields.push(self.destination_scope_digest.as_bytes());
        fields.push(self.partition_key.as_str().as_bytes());
        fields.push(self.event_schema.as_str().as_bytes());
        fields.push(payload_digest.as_bytes());
        for header in &self.headers {
            fields.push(header.name.as_str().as_bytes());
            fields.push(header.value.as_str().as_bytes());
        }
        Digest256::framed(b"eg/outbox-intent/v1", &fields)
    }

    /// Exact policy-facing egress destination, excluding payload and headers.
    /// The enclosing authority evidence binds the ordered set of these values.
    pub fn destination_authorization_digest(&self) -> Result<Digest256, String> {
        Digest256::framed(
            b"eg/outbox-destination-authorization/v1",
            &[
                self.tenant.as_str().as_bytes(),
                self.destination_scope_digest.as_bytes(),
                self.topic.as_str().as_bytes(),
                self.partition_key.as_str().as_bytes(),
                self.event_schema.as_str().as_bytes(),
            ],
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OutboxDeliveryState {
    Pending,
    Leased,
    Delivered,
    FailedRetryable,
    FailedTerminal,
}

/// Durable outbox row committed atomically with its canonical mutation effect.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OutboxRecord {
    pub event_id: OpaqueId,
    pub mutation_id: OpaqueId,
    pub ordinal: u32,
    pub event_ordinal: u64,
    pub scope: AuthorityScope,
    pub authority_receipt_id: OpaqueId,
    pub authority_evidence_digest: Digest256,
    pub operation_replay_digest: Digest256,
    pub nonce_replay_digest: Digest256,
    pub effect_digest: Digest256,
    pub envelope_digest: Digest256,
    pub commit_id: OpaqueId,
    pub intent: OutboxIntent,
    pub state: OutboxDeliveryState,
    pub committed_at: UtcUnixNanos,
}

impl OutboxRecord {
    pub fn validate(&self) -> Result<(), String> {
        self.scope.validate()?;
        let tenant = self
            .scope
            .tenant
            .as_ref()
            .ok_or_else(|| "outbox record requires a tenant-scoped destination".to_string())?;
        self.intent.validate_for_scope(tenant, &self.scope)?;
        if self.state != OutboxDeliveryState::Pending {
            return Err("a newly committed outbox record must start pending".into());
        }
        Ok(())
    }

    pub fn digest(&self) -> Result<Digest256, String> {
        self.validate()?;
        let scope = self.scope.digest()?;
        let intent = self.intent.digest()?;
        Digest256::framed(
            b"eg/outbox-record/v1",
            &[
                self.event_id.as_str().as_bytes(),
                self.mutation_id.as_str().as_bytes(),
                &self.ordinal.to_be_bytes(),
                &self.event_ordinal.to_be_bytes(),
                scope.as_bytes(),
                self.authority_receipt_id.as_str().as_bytes(),
                self.authority_evidence_digest.as_bytes(),
                self.operation_replay_digest.as_bytes(),
                self.nonce_replay_digest.as_bytes(),
                self.effect_digest.as_bytes(),
                self.envelope_digest.as_bytes(),
                self.commit_id.as_str().as_bytes(),
                intent.as_bytes(),
                b"pending",
                &self.committed_at.get().to_be_bytes(),
            ],
        )
    }

    pub fn validate_against(
        &self,
        envelope: &MutationEnvelope,
        mutation_receipt: &MutationReceipt,
        intent_index: usize,
    ) -> Result<(), String> {
        self.validate()?;
        validate_original_mutation_commit(mutation_receipt)?;
        mutation_receipt.validate_against(envelope)?;
        let expected_ordinal = u32::try_from(intent_index)
            .map_err(|_| "outbox intent ordinal exceeds u32".to_string())?;
        let intent = envelope
            .outbox_intent(intent_index)
            .ok_or_else(|| "outbox record has no matching envelope intent".to_string())?;
        self.validate_envelope_binding(envelope, intent, expected_ordinal)?;
        self.validate_mutation_receipt_binding(mutation_receipt)
    }

    fn validate_envelope_binding(
        &self,
        envelope: &MutationEnvelope,
        intent: &OutboxIntent,
        expected_ordinal: u32,
    ) -> Result<(), String> {
        if &self.mutation_id != envelope.mutation_id()
            || self.ordinal != expected_ordinal
            || &self.intent != intent
            || &self.scope != envelope.scope()
            || &self.authority_receipt_id != envelope.authority_receipt_id()
        {
            return Err("outbox record differs from its mutation/receipt/intent".into());
        }
        if self.authority_evidence_digest != envelope.authority_evidence_digest()
            || self.operation_replay_digest != envelope.operation_replay_digest()
            || self.nonce_replay_digest != envelope.nonce_replay_digest()
            || self.envelope_digest != envelope.envelope_digest()
        {
            return Err("outbox record differs from its mutation/receipt/intent".into());
        }
        Ok(())
    }

    fn validate_mutation_receipt_binding(
        &self,
        mutation_receipt: &MutationReceipt,
    ) -> Result<(), String> {
        if Some(self.effect_digest) != mutation_receipt.effect_digest
            || Some(&self.commit_id) != mutation_receipt.commit_id.as_ref()
            || mutation_receipt.recorded_at < self.committed_at
        {
            return Err("outbox record differs from its mutation/receipt/intent".into());
        }
        Ok(())
    }
}

fn validate_original_mutation_commit(receipt: &MutationReceipt) -> Result<(), String> {
    receipt.validate()?;
    if receipt.disposition.as_str() != "committed" {
        return Err("outbox records can only be minted by the original mutation commit".into());
    }
    Ok(())
}

/// An attempt-specific delivery update. Delivery never changes the immutable
/// event or mutation identity and cannot advance a projection cursor by itself.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OutboxDeliveryReceipt {
    pub receipt_id: OpaqueId,
    pub event_id: OpaqueId,
    pub mutation_id: OpaqueId,
    pub scope: AuthorityScope,
    pub effect_digest: Digest256,
    pub commit_id: OpaqueId,
    pub outbox_record_digest: Digest256,
    pub consumer_id: ResourceId,
    pub previous_receipt_id: Option<OpaqueId>,
    pub lease_epoch: u64,
    pub attempt: u32,
    pub state: OutboxDeliveryState,
    pub result_digest: Option<Digest256>,
    pub recorded_at: UtcUnixNanos,
}

impl OutboxDeliveryReceipt {
    pub fn validate(&self) -> Result<(), String> {
        if self.previous_receipt_id.as_ref() == Some(&self.receipt_id) {
            return Err("outbox delivery receipt cannot reference itself".into());
        }
        match (self.state, self.result_digest.is_some()) {
            (OutboxDeliveryState::Pending, _) => {
                Err("a delivery receipt cannot use the record-only pending state".into())
            }
            (OutboxDeliveryState::Leased, true) => {
                Err("nonterminal outbox delivery cannot carry a result digest".into())
            }
            (OutboxDeliveryState::Delivered, false) => {
                Err("delivered outbox event requires a result digest".into())
            }
            _ => Ok(()),
        }
    }

    pub fn validate_against(
        &self,
        record: &OutboxRecord,
        previous: Option<&Self>,
    ) -> Result<(), String> {
        self.validate()?;
        record.validate()?;
        self.validate_binding(record)?;
        self.validate_record_time(record)?;
        match previous {
            None => self.validate_first_receipt(),
            Some(previous) => self.validate_successor_receipt(record, previous),
        }
    }

    fn validate_record_time(&self, record: &OutboxRecord) -> Result<(), String> {
        if self.recorded_at < record.committed_at {
            return Err("delivery receipt predates its committed outbox record".into());
        }
        Ok(())
    }

    fn validate_first_receipt(&self) -> Result<(), String> {
        if self.previous_receipt_id.is_some()
            || self.attempt != 1
            || self.lease_epoch == 0
            || self.state != OutboxDeliveryState::Leased
        {
            return Err("first delivery receipt must be the first lease".into());
        }
        Ok(())
    }

    fn validate_successor_receipt(
        &self,
        record: &OutboxRecord,
        previous: &Self,
    ) -> Result<(), String> {
        previous.validate()?;
        previous.validate_binding(record)?;
        if previous.recorded_at < record.committed_at {
            return Err("delivery predecessor predates its committed outbox record".into());
        }
        self.validate_receipt_continuity(previous)?;
        self.validate_state_transition(previous)
    }

    fn validate_receipt_continuity(&self, previous: &Self) -> Result<(), String> {
        if self.receipt_id == previous.receipt_id
            || self.previous_receipt_id.as_ref() != Some(&previous.receipt_id)
            || self.consumer_id != previous.consumer_id
            || self.recorded_at <= previous.recorded_at
        {
            return Err("delivery receipt continuity binding changed".into());
        }
        Ok(())
    }

    fn validate_state_transition(&self, previous: &Self) -> Result<(), String> {
        match (previous.state, self.state) {
            (OutboxDeliveryState::Leased, OutboxDeliveryState::Delivered)
            | (OutboxDeliveryState::Leased, OutboxDeliveryState::FailedRetryable)
            | (OutboxDeliveryState::Leased, OutboxDeliveryState::FailedTerminal) => {
                self.validate_same_attempt_transition(previous)
            }
            (OutboxDeliveryState::FailedRetryable, OutboxDeliveryState::Leased) => {
                self.validate_retry_transition(previous)
            }
            _ => Err("invalid outbox delivery state transition".into()),
        }
    }

    fn validate_same_attempt_transition(&self, previous: &Self) -> Result<(), String> {
        if self.attempt != previous.attempt || self.lease_epoch != previous.lease_epoch {
            return Err("invalid outbox delivery state transition".into());
        }
        Ok(())
    }

    fn validate_retry_transition(&self, previous: &Self) -> Result<(), String> {
        let expected_attempt = previous
            .attempt
            .checked_add(1)
            .ok_or_else(|| "outbox delivery attempt overflow".to_string())?;
        if self.attempt != expected_attempt || self.lease_epoch <= previous.lease_epoch {
            return Err("invalid outbox delivery state transition".into());
        }
        Ok(())
    }

    fn validate_binding(&self, record: &OutboxRecord) -> Result<(), String> {
        if self.event_id != record.event_id
            || self.mutation_id != record.mutation_id
            || self.scope != record.scope
            || self.effect_digest != record.effect_digest
            || self.commit_id != record.commit_id
            || self.outbox_record_digest != record.digest()?
        {
            return Err("delivery receipt differs from its exact outbox record".into());
        }
        Ok(())
    }
}

/// Monotonic projection position advanced in the same transaction that records
/// successful delivery.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OutboxProjectionCursor {
    pub projection_id: ResourceId,
    pub scope: AuthorityScope,
    pub event_id: OpaqueId,
    pub event_ordinal: u64,
    pub delivery_receipt_id: OpaqueId,
    pub commit_id: OpaqueId,
    pub outbox_record_digest: Digest256,
    pub advanced_at: UtcUnixNanos,
}

impl OutboxProjectionCursor {
    pub fn validate_successor(
        &self,
        previous: Option<&Self>,
        record: &OutboxRecord,
        delivery: &OutboxDeliveryReceipt,
        delivery_predecessor: &OutboxDeliveryReceipt,
    ) -> Result<(), String> {
        self.scope.validate()?;
        delivery.validate_against(record, Some(delivery_predecessor))?;
        self.validate_record_binding(record)?;
        self.validate_delivery_binding(record, delivery)?;
        self.validate_cursor_position(previous)
    }

    fn validate_record_binding(&self, record: &OutboxRecord) -> Result<(), String> {
        if self.scope != record.scope
            || self.event_id != record.event_id
            || self.event_ordinal != record.event_ordinal
            || self.commit_id != record.commit_id
            || self.outbox_record_digest != record.digest()?
        {
            return Err("projection cursor differs from delivered event continuity".into());
        }
        Ok(())
    }

    fn validate_delivery_binding(
        &self,
        record: &OutboxRecord,
        delivery: &OutboxDeliveryReceipt,
    ) -> Result<(), String> {
        if delivery.state != OutboxDeliveryState::Delivered
            || self.delivery_receipt_id != delivery.receipt_id
            || self.outbox_record_digest != delivery.outbox_record_digest
            || self.projection_id != delivery.consumer_id
            || self.advanced_at < delivery.recorded_at
        {
            return Err("projection cursor differs from delivered event continuity".into());
        }
        if self.scope != delivery.scope
            || self.event_id != delivery.event_id
            || self.commit_id != delivery.commit_id
            || record.scope != delivery.scope
        {
            return Err("projection cursor differs from delivered event continuity".into());
        }
        Ok(())
    }

    fn validate_cursor_position(&self, previous: Option<&Self>) -> Result<(), String> {
        match previous {
            Some(previous) => self.validate_contiguous_position(previous),
            None => self.validate_first_position(),
        }
    }

    fn validate_first_position(&self) -> Result<(), String> {
        if self.event_ordinal != 0 {
            return Err("first projection cursor ordinal must be zero".into());
        }
        Ok(())
    }

    fn validate_contiguous_position(&self, previous: &Self) -> Result<(), String> {
        if self.projection_id != previous.projection_id || self.scope != previous.scope {
            return Err("projection cursor identity changed".into());
        }
        let expected_ordinal = previous
            .event_ordinal
            .checked_add(1)
            .ok_or_else(|| "projection cursor ordinal overflow".to_string())?;
        if self.event_ordinal != expected_ordinal {
            return Err("projection cursor must advance contiguously".into());
        }
        if self.advanced_at <= previous.advanced_at {
            return Err("projection cursor time must advance strictly".into());
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::authority::ScopeKind;
    use crate::contract::{MutationDisposition, TenantId, MAX_SCOPE_COMPONENTS};
    use crate::mutation::{MutationReceipt, MutationResult};

    fn digest(byte: u8) -> Digest256 {
        Digest256::from_bytes([byte; 32])
    }

    fn scope() -> AuthorityScope {
        AuthorityScope {
            kind: ScopeKind::new("graph").unwrap(),
            scope_id: ResourceId::new("graph:tenant:a/g").unwrap(),
            tenant: Some(TenantId::new("tenant:a").unwrap()),
            parent_scope_ids: BoundedVec::<ResourceId, MAX_SCOPE_COMPONENTS>::new(vec![
                ResourceId::new("tenant:a").unwrap(),
            ])
            .unwrap(),
            graph_incarnation: None,
        }
    }

    fn intent() -> OutboxIntent {
        let payload = RecordBytes::new(vec![1, 2, 3]).unwrap();
        OutboxIntent {
            tenant: TenantId::new("tenant:a").unwrap(),
            destination_scope_digest: scope().digest().unwrap(),
            topic: ResourceId::new("topic").unwrap(),
            partition_key: ResourceId::new("key").unwrap(),
            event_schema: ResourceId::new("event.v1").unwrap(),
            payload_digest: payload.digest().unwrap(),
            payload,
            headers: BoundedVec::new(vec![]).unwrap(),
        }
    }

    fn record() -> OutboxRecord {
        OutboxRecord {
            event_id: OpaqueId::new("event:1").unwrap(),
            mutation_id: OpaqueId::new("mutation:1").unwrap(),
            ordinal: 0,
            event_ordinal: 0,
            scope: scope(),
            authority_receipt_id: OpaqueId::new("authority:1").unwrap(),
            authority_evidence_digest: digest(9),
            operation_replay_digest: digest(1),
            nonce_replay_digest: digest(2),
            effect_digest: digest(3),
            envelope_digest: digest(8),
            commit_id: OpaqueId::new("commit:1").unwrap(),
            intent: intent(),
            state: OutboxDeliveryState::Pending,
            committed_at: UtcUnixNanos::new(10),
        }
    }

    fn lease(record: &OutboxRecord) -> OutboxDeliveryReceipt {
        OutboxDeliveryReceipt {
            receipt_id: OpaqueId::new("delivery:lease").unwrap(),
            event_id: record.event_id.clone(),
            mutation_id: record.mutation_id.clone(),
            scope: record.scope.clone(),
            effect_digest: record.effect_digest,
            commit_id: record.commit_id.clone(),
            outbox_record_digest: record.digest().unwrap(),
            consumer_id: ResourceId::new("projection:a").unwrap(),
            previous_receipt_id: None,
            lease_epoch: 1,
            attempt: 1,
            state: OutboxDeliveryState::Leased,
            result_digest: None,
            recorded_at: UtcUnixNanos::new(11),
        }
    }

    fn delivered(lease: &OutboxDeliveryReceipt) -> OutboxDeliveryReceipt {
        OutboxDeliveryReceipt {
            receipt_id: OpaqueId::new("delivery:done").unwrap(),
            previous_receipt_id: Some(lease.receipt_id.clone()),
            state: OutboxDeliveryState::Delivered,
            result_digest: Some(digest(4)),
            recorded_at: UtcUnixNanos::new(12),
            ..lease.clone()
        }
    }

    #[test]
    fn outbox_headers_are_ordered_and_payload_is_bound() {
        let payload = RecordBytes::new(vec![1, 2, 3]).unwrap();
        let intent = OutboxIntent {
            tenant: TenantId::new("tenant:a").unwrap(),
            destination_scope_digest: scope().digest().unwrap(),
            topic: ResourceId::new("topic").unwrap(),
            partition_key: ResourceId::new("key").unwrap(),
            event_schema: ResourceId::new("event.v1").unwrap(),
            payload_digest: payload.digest().unwrap(),
            payload,
            headers: BoundedVec::new(vec![
                OutboxHeader {
                    name: ResourceId::new("a").unwrap(),
                    value: OpaqueId::new("1").unwrap(),
                },
                OutboxHeader {
                    name: ResourceId::new("b").unwrap(),
                    value: OpaqueId::new("2").unwrap(),
                },
            ])
            .unwrap(),
        };
        assert!(intent.validate().is_ok());
        assert!(intent
            .validate_for_scope(&TenantId::new("tenant:a").unwrap(), &scope())
            .is_ok());
        assert!(intent
            .validate_for_scope(&TenantId::new("tenant:b").unwrap(), &scope())
            .is_err());
    }

    #[test]
    fn delivery_and_cursor_bind_exact_event_continuity() {
        let record = record();
        let lease = lease(&record);
        assert!(lease.validate_against(&record, None).is_ok());
        let mut altered_record = record.clone();
        altered_record.ordinal = 1;
        assert!(lease.validate_against(&altered_record, None).is_err());
        let mut early_lease = lease.clone();
        early_lease.recorded_at = UtcUnixNanos::new(9);
        assert!(early_lease.validate_against(&record, None).is_err());
        let delivered_after_early_lease = delivered(&early_lease);
        assert!(delivered_after_early_lease
            .validate_against(&record, Some(&early_lease))
            .is_err());
        let delivered = delivered(&lease);
        assert!(delivered.validate_against(&record, Some(&lease)).is_ok());
        assert!(delivered.validate_against(&record, None).is_err());
        let cursor = OutboxProjectionCursor {
            projection_id: ResourceId::new("projection:a").unwrap(),
            scope: record.scope.clone(),
            event_id: record.event_id.clone(),
            event_ordinal: 0,
            delivery_receipt_id: delivered.receipt_id.clone(),
            commit_id: record.commit_id.clone(),
            outbox_record_digest: record.digest().unwrap(),
            advanced_at: UtcUnixNanos::new(13),
        };
        assert!(cursor
            .validate_successor(None, &record, &delivered, &lease)
            .is_ok());

        let mut early_cursor = cursor.clone();
        early_cursor.advanced_at = UtcUnixNanos::new(11);
        assert!(early_cursor
            .validate_successor(None, &record, &delivered, &lease)
            .is_err());

        let mut arbitrary_first = cursor.clone();
        arbitrary_first.event_ordinal = 9;
        assert!(arbitrary_first
            .validate_successor(None, &record, &delivered, &lease)
            .is_err());

        let mut altered_predecessor = lease.clone();
        altered_predecessor.receipt_id = OpaqueId::new("delivery:other-lease").unwrap();
        assert!(cursor
            .validate_successor(None, &record, &delivered, &altered_predecessor)
            .is_err());

        let mut substituted_delivery = delivered;
        substituted_delivery.commit_id = OpaqueId::new("commit:other").unwrap();
        assert!(cursor
            .validate_successor(None, &record, &substituted_delivery, &lease)
            .is_err());
    }

    #[test]
    fn retry_delivery_requires_the_exact_immediate_lease() {
        let record = record();
        let first_lease = lease(&record);
        let retryable = OutboxDeliveryReceipt {
            receipt_id: OpaqueId::new("delivery:retryable").unwrap(),
            previous_receipt_id: Some(first_lease.receipt_id.clone()),
            state: OutboxDeliveryState::FailedRetryable,
            result_digest: Some(digest(5)),
            recorded_at: UtcUnixNanos::new(12),
            ..first_lease.clone()
        };
        assert!(retryable
            .validate_against(&record, Some(&first_lease))
            .is_ok());
        let retry_lease = OutboxDeliveryReceipt {
            receipt_id: OpaqueId::new("delivery:retry-lease").unwrap(),
            previous_receipt_id: Some(retryable.receipt_id.clone()),
            lease_epoch: 2,
            attempt: 2,
            state: OutboxDeliveryState::Leased,
            result_digest: None,
            recorded_at: UtcUnixNanos::new(13),
            ..retryable.clone()
        };
        assert!(retry_lease
            .validate_against(&record, Some(&retryable))
            .is_ok());
        let retried_delivery = OutboxDeliveryReceipt {
            receipt_id: OpaqueId::new("delivery:retry-done").unwrap(),
            previous_receipt_id: Some(retry_lease.receipt_id.clone()),
            state: OutboxDeliveryState::Delivered,
            result_digest: Some(digest(6)),
            recorded_at: UtcUnixNanos::new(14),
            ..retry_lease.clone()
        };
        let cursor = OutboxProjectionCursor {
            projection_id: ResourceId::new("projection:a").unwrap(),
            scope: record.scope.clone(),
            event_id: record.event_id.clone(),
            event_ordinal: 0,
            delivery_receipt_id: retried_delivery.receipt_id.clone(),
            commit_id: record.commit_id.clone(),
            outbox_record_digest: record.digest().unwrap(),
            advanced_at: UtcUnixNanos::new(15),
        };
        assert!(cursor
            .validate_successor(None, &record, &retried_delivery, &retry_lease)
            .is_ok());
        assert!(cursor
            .validate_successor(None, &record, &retried_delivery, &retryable)
            .is_err());
    }

    #[test]
    fn delivery_receipt_chain_rejects_self_cycles_and_reused_ids() {
        let record = record();
        let lease = lease(&record);
        let delivered = delivered(&lease);

        let mut self_cycle = delivered.clone();
        self_cycle.previous_receipt_id = Some(self_cycle.receipt_id.clone());
        assert!(self_cycle.validate().is_err());
        assert!(self_cycle.validate_against(&record, Some(&lease)).is_err());

        let mut reused_predecessor_id = delivered;
        reused_predecessor_id.receipt_id = lease.receipt_id.clone();
        assert!(reused_predecessor_id.validate().is_err());
        assert!(reused_predecessor_id
            .validate_against(&record, Some(&lease))
            .is_err());
    }

    #[test]
    fn replayed_mutation_cannot_mint_another_outbox_record() {
        let record = record();
        let result = MutationResult::ReceiptOnly;
        let replayed = MutationReceipt {
            receipt_id: OpaqueId::new("receipt:replayed").unwrap(),
            mutation_id: record.mutation_id.clone(),
            scope: record.scope.clone(),
            authority_receipt_id: record.authority_receipt_id.clone(),
            authority_evidence_digest: record.authority_evidence_digest,
            disposition: MutationDisposition::new("replayed").unwrap(),
            operation_replay_digest: record.operation_replay_digest,
            nonce_replay_digest: record.nonce_replay_digest,
            envelope_digest: record.envelope_digest,
            effect_id: Some(OpaqueId::new("effect:1").unwrap()),
            effect_digest: Some(record.effect_digest),
            result_digest: result.digest().unwrap(),
            commit_id: Some(record.commit_id.clone()),
            result,
            recorded_at: UtcUnixNanos::new(10),
        };
        assert!(replayed.validate().is_ok());
        assert!(validate_original_mutation_commit(&replayed).is_err());
    }
}
