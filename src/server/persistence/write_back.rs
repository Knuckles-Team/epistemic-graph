//! D18 persistence in the existing tenant-scoped Agent Library control owner.
//!
//! Change sets and receipts share `agent_library.redb` and its mutation kernel.
//! There is no second database, ledger, writer, or source-side transport here.

use eg_storage::{
    ledger_scope_key, AgentLibraryOwner, WRITE_BACK_CHANGE_SETS, WRITE_BACK_IDEMPOTENCY,
    WRITE_BACK_RECEIPTS, WRITE_BACK_RECEIPT_HEADS,
};
use eg_transaction::{AdmittedOwnerWrite, Begin, MaintenanceBatch};
use eg_types::write_back::{
    ReconciliationObservation, ReconciliationReceipt, SourceChangeSet, WriteBackAttempt,
    WriteBackAttemptKind, WriteBackEffectStatus, WriteBackOutcome, WriteBackReceipt,
    WriteBackReceiptPage, WriteBackReceiptRecord, MAX_WRITE_BACK_RECEIPTS,
    WRITE_BACK_SCHEMA_VERSION,
};
use redb::ReadableTable;

use super::agent_library::AgentLibraryStore;

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
struct IdempotencyBinding {
    change_set_id: String,
    change_set_digest: eg_types::contract::Digest256,
}

impl AgentLibraryStore {
    /// Apply one bounded Agent Library control-plane write for `tenant_id`.
    /// Shared by the write-back and decision-artifact rows.
    pub(super) fn maintain_control_rows<F>(
        &self,
        tenant_id: &str,
        event: &str,
        apply: F,
    ) -> Result<(), String>
    where
        F: FnOnce(&AdmittedOwnerWrite<'_, AgentLibraryOwner>) -> Result<(), String>,
    {
        let owner = self.scope_handle(tenant_id)?;
        let subject = ledger_scope_key(owner.identity());
        let write = MaintenanceBatch::new(
            eg_types::mutation_batch::DurabilityDomain::ControlPlane,
            event,
            &subject,
        );
        let (txn, batch, begun) = self
            .mutations
            .admit_current(&owner, |version| write.for_scope_version(&owner, version))?;
        let source_version = match begun {
            Begin::Replay(_) => return txn.abort(),
            Begin::Apply { source_version } => source_version,
        };
        let owner_write = txn.owner_rows(&owner, &batch)?;
        if let Err(error) = apply(&owner_write) {
            drop(owner_write);
            txn.abort()?;
            return Err(error);
        }
        owner_write.finish_owner()?;
        self.mutations
            .finish(&txn, &batch, None, 0, source_version)?;
        self.mutations.commit(txn, &batch)
    }

    pub fn create_change_set(
        &self,
        change_set: SourceChangeSet,
    ) -> Result<SourceChangeSet, String> {
        change_set.validate()?;
        let encoded = rmp_serde::to_vec_named(&change_set).map_err(|error| error.to_string())?;
        let binding_bytes = encode_idempotency_binding(&change_set)?;
        self.maintain_control_rows(&change_set.tenant_id, "write_back_create", |owner| {
            persist_change_set(owner, &change_set, &encoded, &binding_bytes)
        })?;
        Ok(change_set)
    }

    pub fn write_back_change_set(
        &self,
        tenant_id: &str,
        change_set_id: &str,
    ) -> Result<Option<SourceChangeSet>, String> {
        let read = self.read()?;
        let table = read.open_owner_table(WRITE_BACK_CHANGE_SETS)?;
        table
            .get((tenant_id, change_set_id))
            .map_err(|error| error.to_string())?
            .map(|row| decode(row.value(), "write-back change set"))
            .transpose()
    }

    pub fn record_write_back_attempt(
        &self,
        attempt: WriteBackAttempt,
        actor: String,
        recorded_at_ms: u64,
    ) -> Result<WriteBackReceipt, String> {
        let change_set = self.require_change_set(
            &attempt.tenant_id,
            &attempt.change_set_id,
            attempt.change_set_digest,
            &attempt.idempotency_key,
            recorded_at_ms,
        )?;
        validate_attempt(&attempt)?;
        if attempt.applied_field_digest != change_set.patch_digest()? {
            return Err(
                "write-back applied-field digest does not match the authorized patch".to_string(),
            );
        }
        if attempt.pre_source_version != change_set.base_source_version {
            return Err(
                "write-back attempt does not start from the authorized source version".to_string(),
            );
        }
        if attempt.kind == WriteBackAttemptKind::DryRun
            && attempt.post_source_version != attempt.pre_source_version
        {
            return Err("write-back dry-run must not advance the source version".to_string());
        }
        let mut committed = None;
        self.maintain_control_rows(&attempt.tenant_id, "write_back_attempt", |owner| {
            let stream = (attempt.tenant_id.as_str(), attempt.change_set_id.as_str());
            if let Some(receipt) = find_attempt_receipt(owner, stream, &attempt)? {
                committed = Some(receipt);
                return Ok(());
            }
            let (sequence, previous) = next_receipt(owner, stream)?;
            validate_attempt_transition(previous.as_ref(), &attempt)?;
            let receipt = WriteBackReceipt {
                schema_version: WRITE_BACK_SCHEMA_VERSION,
                receipt_id: receipt_id("attempt", &attempt.change_set_digest, sequence)?,
                sequence,
                change_set_id: attempt.change_set_id.clone(),
                change_set_digest: attempt.change_set_digest,
                tenant_id: attempt.tenant_id.clone(),
                actor,
                authorization: change_set.authorization.clone(),
                policy_digest: change_set.policy_digest,
                idempotency_key: attempt.idempotency_key.clone(),
                kind: attempt.kind,
                input_digest: attempt.input_digest,
                output_digest: attempt.output_digest,
                pre_source_version: attempt.pre_source_version.clone(),
                post_source_version: attempt.post_source_version.clone(),
                applied_field_digest: attempt.applied_field_digest,
                outcome: attempt.outcome,
                effect_status: attempt.effect_status,
                connector_observation_digest: attempt.connector_observation_digest,
                provenance_digest: attempt.provenance_digest,
                recorded_at_ms,
            };
            append_receipt(
                owner,
                stream,
                sequence,
                &WriteBackReceiptRecord::Attempt(receipt.clone()),
            )?;
            committed = Some(receipt);
            Ok(())
        })?;
        committed.ok_or_else(|| "write-back attempt committed no receipt".to_string())
    }

    pub fn record_write_back_reconciliation(
        &self,
        observation: ReconciliationObservation,
        actor: String,
        recorded_at_ms: u64,
    ) -> Result<ReconciliationReceipt, String> {
        let change_set = self.require_change_set(
            &observation.tenant_id,
            &observation.change_set_id,
            observation.change_set_digest,
            &observation.idempotency_key,
            recorded_at_ms,
        )?;
        validate_reconciliation(&observation)?;
        let mut committed = None;
        self.maintain_control_rows(
            &observation.tenant_id,
            "write_back_reconciliation",
            |owner| {
                let stream = (
                    observation.tenant_id.as_str(),
                    observation.change_set_id.as_str(),
                );
                if let Some(receipt) = find_reconciliation_receipt(owner, stream, &observation)? {
                    committed = Some(receipt);
                    return Ok(());
                }
                let (sequence, previous) = next_receipt(owner, stream)?;
                require_uncertain(previous.as_ref())?;
                let receipt = ReconciliationReceipt {
                    schema_version: WRITE_BACK_SCHEMA_VERSION,
                    receipt_id: receipt_id(
                        "reconciliation",
                        &observation.change_set_digest,
                        sequence,
                    )?,
                    sequence,
                    change_set_id: observation.change_set_id.clone(),
                    change_set_digest: observation.change_set_digest,
                    tenant_id: observation.tenant_id.clone(),
                    actor,
                    authorization: change_set.authorization.clone(),
                    policy_digest: change_set.policy_digest,
                    idempotency_key: observation.idempotency_key.clone(),
                    observed_source_version: observation.observed_source_version.clone(),
                    effect_status: observation.effect_status,
                    retry_allowed: observation.retry_allowed,
                    evidence_digest: observation.evidence_digest,
                    connector_observation_digest: observation.connector_observation_digest,
                    provenance_digest: observation.provenance_digest,
                    recorded_at_ms,
                };
                append_receipt(
                    owner,
                    stream,
                    sequence,
                    &WriteBackReceiptRecord::Reconciliation(receipt.clone()),
                )?;
                committed = Some(receipt);
                Ok(())
            },
        )?;
        committed.ok_or_else(|| "write-back reconciliation committed no receipt".to_string())
    }

    pub fn write_back_receipts(
        &self,
        tenant_id: &str,
        change_set_id: &str,
        after_sequence: u64,
        limit: u16,
    ) -> Result<WriteBackReceiptPage, String> {
        let limit = usize::from(limit);
        if limit == 0 || limit > MAX_WRITE_BACK_RECEIPTS {
            return Err("write-back receipt page exceeds resource limits".to_string());
        }
        let read = self.read()?;
        let heads = read.open_owner_table(WRITE_BACK_RECEIPT_HEADS)?;
        let head = heads
            .get((tenant_id, change_set_id))
            .map_err(|error| error.to_string())?
            .map(|row| row.value())
            .unwrap_or(0);
        let receipts_table = read.open_owner_table(WRITE_BACK_RECEIPTS)?;
        let mut receipts = Vec::new();
        let start = after_sequence.saturating_add(1);
        for sequence in start..=head {
            if receipts.len() == limit {
                break;
            }
            let row = receipts_table
                .get((tenant_id, change_set_id, sequence))
                .map_err(|error| error.to_string())?
                .ok_or_else(|| "CORRUPT_WRITE_BACK: receipt stream has a gap".to_string())?;
            receipts.push(decode(row.value(), "write-back receipt")?);
        }
        let last = receipts
            .last()
            .map(receipt_sequence)
            .unwrap_or(after_sequence);
        Ok(WriteBackReceiptPage {
            receipts,
            next_sequence: (last < head).then_some(last),
        })
    }

    fn require_change_set(
        &self,
        tenant_id: &str,
        change_set_id: &str,
        digest: eg_types::contract::Digest256,
        idempotency_key: &str,
        now_ms: u64,
    ) -> Result<SourceChangeSet, String> {
        let change_set = self
            .write_back_change_set(tenant_id, change_set_id)?
            .ok_or_else(|| "write-back change set does not exist".to_string())?;
        if change_set.change_set_digest != digest || change_set.idempotency_key != idempotency_key {
            return Err("IDEMPOTENCY_CONFLICT: observation binding mismatch".to_string());
        }
        if now_ms >= change_set.expires_at_ms {
            return Err("write-back change set has expired".to_string());
        }
        if !change_set.authorization.authorized {
            return Err("ACCESS_DENIED: write-back authorization was denied".to_string());
        }
        Ok(change_set)
    }
}

fn decode<T: serde::de::DeserializeOwned>(bytes: &[u8], noun: &str) -> Result<T, String> {
    eg_types::msgpack::decode_bounded(
        bytes,
        eg_types::msgpack::MsgpackLimits::new(8 * 1024 * 1024, 100_000, 64),
    )
    .map_err(|_| format!("CORRUPT_WRITE_BACK: invalid {noun}"))
}

fn encode_idempotency_binding(change_set: &SourceChangeSet) -> Result<Vec<u8>, String> {
    let binding = IdempotencyBinding {
        change_set_id: change_set.change_set_id.clone(),
        change_set_digest: change_set.change_set_digest,
    };
    rmp_serde::to_vec_named(&binding).map_err(|error| error.to_string())
}

fn persist_change_set(
    owner: &AdmittedOwnerWrite<'_, AgentLibraryOwner>,
    change_set: &SourceChangeSet,
    encoded: &[u8],
    binding_bytes: &[u8],
) -> Result<(), String> {
    let key = (
        change_set.tenant_id.as_str(),
        change_set.change_set_id.as_str(),
    );
    let idem_key = (
        change_set.tenant_id.as_str(),
        change_set.idempotency_key.as_str(),
    );
    let mut sets = owner.open_table(WRITE_BACK_CHANGE_SETS)?;
    let mut idempotency = owner.open_table(WRITE_BACK_IDEMPOTENCY)?;
    let existing_set = sets
        .get(key)
        .map_err(|error| error.to_string())?
        .map(|row| row.value().to_vec());
    let existing_binding = idempotency
        .get(idem_key)
        .map_err(|error| error.to_string())?
        .map(|row| row.value().to_vec());
    validate_existing_change_set(
        existing_set.as_deref(),
        existing_binding.as_deref(),
        encoded,
        binding_bytes,
    )?;
    if existing_set.is_none() {
        sets.insert(key, encoded)
            .map_err(|error| error.to_string())?;
    }
    if existing_binding.is_none() {
        idempotency
            .insert(idem_key, binding_bytes)
            .map_err(|error| error.to_string())?;
    }
    Ok(())
}

fn validate_existing_change_set(
    existing_set: Option<&[u8]>,
    existing_binding: Option<&[u8]>,
    encoded: &[u8],
    binding_bytes: &[u8],
) -> Result<(), String> {
    match (existing_set, existing_binding) {
        (Some(stored), Some(stored_binding))
            if stored == encoded && stored_binding == binding_bytes =>
        {
            Ok(())
        }
        (Some(stored), _) if stored != encoded => {
            Err("IDEMPOTENCY_CONFLICT: change_set_id names different bytes".to_string())
        }
        (_, Some(stored)) if stored != binding_bytes => {
            Err("IDEMPOTENCY_CONFLICT: write-back key names another change set".to_string())
        }
        (Some(_), None) | (None, Some(_)) => {
            Err("CORRUPT_WRITE_BACK: change set and idempotency binding disagree".to_string())
        }
        (None, None) => Ok(()),
        (Some(_), Some(_)) => unreachable!("mismatches returned above"),
    }
}

fn receipt_id(
    kind: &str,
    change_set_digest: &eg_types::contract::Digest256,
    sequence: u64,
) -> Result<String, String> {
    let sequence = sequence.to_be_bytes();
    Ok(format!(
        "write-back-{kind}-{}",
        eg_types::contract::Digest256::framed(
            b"eg/write-back-receipt-id/v1",
            &[kind.as_bytes(), change_set_digest.as_bytes(), &sequence],
        )?
        .to_hex()
    ))
}

fn next_receipt(
    owner: &AdmittedOwnerWrite<'_, AgentLibraryOwner>,
    stream: (&str, &str),
) -> Result<(u64, Option<WriteBackReceiptRecord>), String> {
    let heads = owner.open_table(WRITE_BACK_RECEIPT_HEADS)?;
    let head = heads
        .get(stream)
        .map_err(|error| error.to_string())?
        .map(|row| row.value())
        .unwrap_or(0);
    if head as usize >= MAX_WRITE_BACK_RECEIPTS {
        return Err("write-back receipt stream exceeds resource limits".to_string());
    }
    let previous = if head == 0 {
        None
    } else {
        let receipts = owner.open_table(WRITE_BACK_RECEIPTS)?;
        let row = receipts
            .get((stream.0, stream.1, head))
            .map_err(|error| error.to_string())?
            .ok_or_else(|| "CORRUPT_WRITE_BACK: receipt head names a missing row".to_string())?;
        Some(decode(row.value(), "write-back receipt")?)
    };
    Ok((head + 1, previous))
}

fn find_attempt_receipt(
    owner: &AdmittedOwnerWrite<'_, AgentLibraryOwner>,
    stream: (&str, &str),
    attempt: &WriteBackAttempt,
) -> Result<Option<WriteBackReceipt>, String> {
    find_receipt(owner, stream, |record| match record {
        WriteBackReceiptRecord::Attempt(receipt) if attempt_matches_receipt(&receipt, attempt) => {
            Some(receipt)
        }
        _ => None,
    })
}

fn find_reconciliation_receipt(
    owner: &AdmittedOwnerWrite<'_, AgentLibraryOwner>,
    stream: (&str, &str),
    observation: &ReconciliationObservation,
) -> Result<Option<ReconciliationReceipt>, String> {
    find_receipt(owner, stream, |record| match record {
        WriteBackReceiptRecord::Reconciliation(receipt)
            if reconciliation_matches_receipt(&receipt, observation) =>
        {
            Some(receipt)
        }
        _ => None,
    })
}

fn find_receipt<T, F>(
    owner: &AdmittedOwnerWrite<'_, AgentLibraryOwner>,
    stream: (&str, &str),
    mut matches: F,
) -> Result<Option<T>, String>
where
    F: FnMut(WriteBackReceiptRecord) -> Option<T>,
{
    let head = receipt_head(owner, stream)?;
    let receipts = owner.open_table(WRITE_BACK_RECEIPTS)?;
    for sequence in 1..=head {
        let row = receipts
            .get((stream.0, stream.1, sequence))
            .map_err(|error| error.to_string())?
            .ok_or_else(|| "CORRUPT_WRITE_BACK: receipt stream has a gap".to_string())?;
        if let Some(receipt) = matches(decode(row.value(), "write-back receipt")?) {
            return Ok(Some(receipt));
        }
    }
    Ok(None)
}

fn receipt_head(
    owner: &AdmittedOwnerWrite<'_, AgentLibraryOwner>,
    stream: (&str, &str),
) -> Result<u64, String> {
    let heads = owner.open_table(WRITE_BACK_RECEIPT_HEADS)?;
    heads
        .get(stream)
        .map_err(|error| error.to_string())
        .map(|row| row.map(|value| value.value()).unwrap_or(0))
}

fn attempt_matches_receipt(receipt: &WriteBackReceipt, attempt: &WriteBackAttempt) -> bool {
    (
        &receipt.change_set_digest,
        &receipt.idempotency_key,
        receipt.kind,
        &receipt.input_digest,
        &receipt.output_digest,
        &receipt.pre_source_version,
        &receipt.post_source_version,
        &receipt.applied_field_digest,
        receipt.outcome,
        receipt.effect_status,
        &receipt.connector_observation_digest,
        &receipt.provenance_digest,
    ) == (
        &attempt.change_set_digest,
        &attempt.idempotency_key,
        attempt.kind,
        &attempt.input_digest,
        &attempt.output_digest,
        &attempt.pre_source_version,
        &attempt.post_source_version,
        &attempt.applied_field_digest,
        attempt.outcome,
        attempt.effect_status,
        &attempt.connector_observation_digest,
        &attempt.provenance_digest,
    )
}

enum ReconciliationReplaySource<'a> {
    Receipt(&'a ReconciliationReceipt),
    Observation(&'a ReconciliationObservation),
}

fn reconciliation_replay_equal(
    left: ReconciliationReplaySource<'_>,
    right: ReconciliationReplaySource<'_>,
) -> bool {
    match (left, right) {
        (
            ReconciliationReplaySource::Receipt(receipt),
            ReconciliationReplaySource::Observation(observation),
        ) => {
            (
                &receipt.change_set_digest,
                &receipt.idempotency_key,
                &receipt.observed_source_version,
                receipt.effect_status,
                receipt.retry_allowed,
                &receipt.evidence_digest,
                &receipt.connector_observation_digest,
                &receipt.provenance_digest,
            ) == (
                &observation.change_set_digest,
                &observation.idempotency_key,
                &observation.observed_source_version,
                observation.effect_status,
                observation.retry_allowed,
                &observation.evidence_digest,
                &observation.connector_observation_digest,
                &observation.provenance_digest,
            )
        }
        _ => false,
    }
}

fn reconciliation_matches_receipt(
    receipt: &ReconciliationReceipt,
    observation: &ReconciliationObservation,
) -> bool {
    reconciliation_replay_equal(
        ReconciliationReplaySource::Receipt(receipt),
        ReconciliationReplaySource::Observation(observation),
    )
}

fn append_receipt(
    owner: &AdmittedOwnerWrite<'_, AgentLibraryOwner>,
    stream: (&str, &str),
    sequence: u64,
    receipt: &WriteBackReceiptRecord,
) -> Result<(), String> {
    let bytes = rmp_serde::to_vec_named(receipt).map_err(|error| error.to_string())?;
    let mut receipts = owner.open_table(WRITE_BACK_RECEIPTS)?;
    if receipts
        .get((stream.0, stream.1, sequence))
        .map_err(|error| error.to_string())?
        .is_some()
    {
        return Err("CORRUPT_WRITE_BACK: append position is already occupied".to_string());
    }
    receipts
        .insert((stream.0, stream.1, sequence), bytes.as_slice())
        .map_err(|error| error.to_string())?;
    let mut heads = owner.open_table(WRITE_BACK_RECEIPT_HEADS)?;
    heads
        .insert(stream, sequence)
        .map_err(|error| error.to_string())?;
    Ok(())
}

fn validate_attempt(attempt: &WriteBackAttempt) -> Result<(), String> {
    if attempt.pre_source_version.is_empty() || attempt.post_source_version.is_empty() {
        return Err("write-back attempt requires source versions".to_string());
    }
    let valid = matches!(
        (attempt.kind, attempt.outcome, attempt.effect_status),
        (
            WriteBackAttemptKind::DryRun,
            WriteBackOutcome::DryRunReady,
            WriteBackEffectStatus::NoEffect,
        ) | (
            WriteBackAttemptKind::DryRun,
            WriteBackOutcome::Conflict,
            WriteBackEffectStatus::NoEffect,
        ) | (
            WriteBackAttemptKind::DryRun,
            WriteBackOutcome::Rejected,
            WriteBackEffectStatus::NoEffect,
        ) | (
            WriteBackAttemptKind::Apply,
            WriteBackOutcome::Applied,
            WriteBackEffectStatus::Applied,
        ) | (
            WriteBackAttemptKind::Apply,
            WriteBackOutcome::Conflict,
            WriteBackEffectStatus::NoEffect,
        ) | (
            WriteBackAttemptKind::Apply,
            WriteBackOutcome::Rejected,
            WriteBackEffectStatus::NoEffect,
        ) | (
            WriteBackAttemptKind::Apply,
            WriteBackOutcome::OutcomeUncertain,
            WriteBackEffectStatus::OutcomeUncertain,
        )
    );
    valid
        .then_some(())
        .ok_or_else(|| "write-back outcome/effect combination is invalid".to_string())
}

fn validate_attempt_transition(
    previous: Option<&WriteBackReceiptRecord>,
    attempt: &WriteBackAttempt,
) -> Result<(), String> {
    if attempt.kind == WriteBackAttemptKind::DryRun {
        return previous
            .is_none()
            .then_some(())
            .ok_or_else(|| "dry-run must be the first write-back receipt".to_string());
    }
    match previous {
        Some(WriteBackReceiptRecord::Attempt(receipt))
            if receipt.kind == WriteBackAttemptKind::DryRun
                && receipt.outcome == WriteBackOutcome::DryRunReady =>
        {
            Ok(())
        }
        Some(WriteBackReceiptRecord::Reconciliation(receipt))
            if receipt.effect_status == WriteBackEffectStatus::NoEffect
                && receipt.retry_allowed =>
        {
            Ok(())
        }
        Some(WriteBackReceiptRecord::Attempt(receipt))
            if receipt.effect_status == WriteBackEffectStatus::OutcomeUncertain =>
        {
            Err("uncertain write-back outcome must reconcile before retry".to_string())
        }
        _ => Err(
            "write-back apply requires a successful dry-run or retryable reconciliation"
                .to_string(),
        ),
    }
}

fn require_uncertain(previous: Option<&WriteBackReceiptRecord>) -> Result<(), String> {
    match previous {
        Some(WriteBackReceiptRecord::Attempt(receipt))
            if receipt.effect_status == WriteBackEffectStatus::OutcomeUncertain =>
        {
            Ok(())
        }
        Some(WriteBackReceiptRecord::Reconciliation(receipt))
            if receipt.effect_status == WriteBackEffectStatus::OutcomeUncertain =>
        {
            Ok(())
        }
        _ => Err("reconciliation requires an uncertain source effect".to_string()),
    }
}

fn validate_reconciliation(observation: &ReconciliationObservation) -> Result<(), String> {
    if observation.observed_source_version.is_empty() {
        return Err("reconciliation requires an observed source version".to_string());
    }
    match (observation.effect_status, observation.retry_allowed) {
        (WriteBackEffectStatus::Applied, false)
        | (WriteBackEffectStatus::NoEffect, true)
        | (WriteBackEffectStatus::OutcomeUncertain, false) => Ok(()),
        _ => Err("reconciliation effect/retry combination is invalid".to_string()),
    }
}

fn receipt_sequence(receipt: &WriteBackReceiptRecord) -> u64 {
    match receipt {
        WriteBackReceiptRecord::Attempt(receipt) => receipt.sequence,
        WriteBackReceiptRecord::Reconciliation(receipt) => receipt.sequence,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use eg_types::contract::Digest256;
    use eg_types::write_back::{WriteBackAuthorizationDecision, WriteBackAuthorizationMode};

    use super::*;

    fn digest(byte: u8) -> Digest256 {
        Digest256::from_bytes([byte; 32])
    }

    fn change_set() -> SourceChangeSet {
        let mut value = SourceChangeSet {
            schema_version: WRITE_BACK_SCHEMA_VERSION,
            change_set_id: "change-1".to_string(),
            change_set_digest: digest(0),
            tenant_id: "tenant-1".to_string(),
            actor: format!("principal:sha256:{}", "a".repeat(64)),
            purpose: "approved maintenance".to_string(),
            connector_id: "connector-1".to_string(),
            source_instance_id: "source-1".to_string(),
            entity_id: "entity-1".to_string(),
            field_scope: vec!["priority".to_string()],
            base_source_version: "etag-1".to_string(),
            desired_patch: BTreeMap::from([(
                "priority".to_string(),
                serde_json::Value::String("high".to_string()),
            )]),
            source_of_truth_rule: "source_wins_outside_scope".to_string(),
            field_provenance: BTreeMap::from([("priority".to_string(), "approval:42".to_string())]),
            required_capability: "ticket:update".to_string(),
            policy_digest: digest(2),
            authorization: WriteBackAuthorizationDecision {
                mode: WriteBackAuthorizationMode::ProposalApproval,
                authorization_ref: "approval:42".to_string(),
                decision_digest: digest(3),
                input_digest: digest(4),
                output_digest: digest(5),
                authorized: true,
            },
            idempotency_key: "idempotency-1".to_string(),
            expires_at_ms: 4_000_000_000_000,
            reconciliation_procedure: "read_by_idempotency_and_version".to_string(),
        };
        value.change_set_digest = value.canonical_digest().unwrap();
        value
    }

    fn attempt(
        change_set: &SourceChangeSet,
        kind: WriteBackAttemptKind,
        outcome: WriteBackOutcome,
        effect_status: WriteBackEffectStatus,
        nonce: u8,
    ) -> WriteBackAttempt {
        WriteBackAttempt {
            tenant_id: change_set.tenant_id.clone(),
            change_set_id: change_set.change_set_id.clone(),
            change_set_digest: change_set.change_set_digest,
            idempotency_key: change_set.idempotency_key.clone(),
            kind,
            input_digest: digest(nonce),
            output_digest: digest(nonce.wrapping_add(1)),
            pre_source_version: change_set.base_source_version.clone(),
            post_source_version: if kind == WriteBackAttemptKind::DryRun {
                change_set.base_source_version.clone()
            } else {
                format!("etag-{nonce}")
            },
            applied_field_digest: change_set.patch_digest().unwrap(),
            outcome,
            effect_status,
            connector_observation_digest: digest(nonce.wrapping_add(2)),
            provenance_digest: digest(nonce.wrapping_add(3)),
        }
    }

    #[test]
    fn uncertain_attempt_requires_reconciliation_before_retry() {
        let directory = tempfile::tempdir().unwrap();
        let store = AgentLibraryStore::open(directory.path().to_str().unwrap()).unwrap();
        let change_set = store.create_change_set(change_set()).unwrap();
        let actor = change_set.actor.clone();
        store
            .record_write_back_attempt(
                attempt(
                    &change_set,
                    WriteBackAttemptKind::DryRun,
                    WriteBackOutcome::DryRunReady,
                    WriteBackEffectStatus::NoEffect,
                    10,
                ),
                actor.clone(),
                1,
            )
            .unwrap();
        let uncertain = attempt(
            &change_set,
            WriteBackAttemptKind::Apply,
            WriteBackOutcome::OutcomeUncertain,
            WriteBackEffectStatus::OutcomeUncertain,
            20,
        );
        store
            .record_write_back_attempt(uncertain.clone(), actor.clone(), 2)
            .unwrap();
        assert!(store
            .record_write_back_attempt(
                attempt(
                    &change_set,
                    WriteBackAttemptKind::Apply,
                    WriteBackOutcome::Applied,
                    WriteBackEffectStatus::Applied,
                    30,
                ),
                actor.clone(),
                3,
            )
            .is_err());
        store
            .record_write_back_reconciliation(
                ReconciliationObservation {
                    tenant_id: change_set.tenant_id.clone(),
                    change_set_id: change_set.change_set_id.clone(),
                    change_set_digest: change_set.change_set_digest,
                    idempotency_key: change_set.idempotency_key.clone(),
                    observed_source_version: change_set.base_source_version.clone(),
                    effect_status: WriteBackEffectStatus::NoEffect,
                    retry_allowed: true,
                    evidence_digest: digest(40),
                    connector_observation_digest: digest(41),
                    provenance_digest: digest(42),
                },
                actor.clone(),
                4,
            )
            .unwrap();
        store
            .record_write_back_attempt(
                attempt(
                    &change_set,
                    WriteBackAttemptKind::Apply,
                    WriteBackOutcome::Applied,
                    WriteBackEffectStatus::Applied,
                    50,
                ),
                actor,
                5,
            )
            .unwrap();
        let page = store
            .write_back_receipts(&change_set.tenant_id, &change_set.change_set_id, 0, 16)
            .unwrap();
        assert_eq!(page.receipts.len(), 4);
        assert!(page.next_sequence.is_none());
    }

    #[test]
    fn exact_attempt_replay_returns_the_original_append() {
        let directory = tempfile::tempdir().unwrap();
        let store = AgentLibraryStore::open(directory.path().to_str().unwrap()).unwrap();
        let change_set = store.create_change_set(change_set()).unwrap();
        let attempt = attempt(
            &change_set,
            WriteBackAttemptKind::DryRun,
            WriteBackOutcome::DryRunReady,
            WriteBackEffectStatus::NoEffect,
            10,
        );
        let first = store
            .record_write_back_attempt(attempt.clone(), change_set.actor.clone(), 1)
            .unwrap();
        let replay = store
            .record_write_back_attempt(attempt, change_set.actor.clone(), 2)
            .unwrap();
        assert_eq!(first, replay);
        assert_eq!(
            store
                .write_back_receipts(&change_set.tenant_id, &change_set.change_set_id, 0, 16)
                .unwrap()
                .receipts
                .len(),
            1
        );
    }
}
