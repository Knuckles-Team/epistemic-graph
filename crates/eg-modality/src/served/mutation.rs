use serde::Serialize;
use sha2::{Digest, Sha256};

use super::indexes::bundle_matches_modality;
use super::{
    ApplyDisposition, ApplyOutcome, IdempotencyEntry, IngestUndo, LifecycleState, ServedDelete,
    ServedError, ServedEvent, ServedEventKind, ServedIngest, ServedModalityRuntime,
    ServedPolicyScope, ServedRecord,
};
use crate::artifact::{Occurrence, OccurrenceId, OpaqueRef};

impl<T> ServedModalityRuntime<T>
where
    T: crate::GovernedModality
        + Clone
        + PartialEq
        + std::fmt::Debug
        + serde::Serialize
        + serde::de::DeserializeOwned,
{
    pub fn ingest(&mut self, command: ServedIngest<T>) -> Result<ApplyOutcome, ServedError> {
        let occurrence = validate_ingest_command(&command)?;
        let fingerprint = stable_fingerprint(&command).map_err(|_| ServedError::Codec)?;
        if let Some(outcome) = self.replay_or_conflict(&command.idempotency_ref, fingerprint)? {
            return Ok(outcome);
        }
        let kind = self.ingest_event_kind(
            &command.target_occurrence_id,
            command.expected_version,
            occurrence.observation_version,
        )?;

        let occurrence_id = command.target_occurrence_id.clone();
        let observation_version = occurrence.observation_version;
        let tenant_ref = occurrence.policy.tenant_ref.clone();
        let access_policy_ref = occurrence.policy.access_policy_ref.clone();
        self.ensure_active_count();
        let activates_record = self
            .records
            .get(&occurrence_id)
            .is_none_or(|record| record.lifecycle == LifecycleState::Tombstoned);
        let record = ServedRecord {
            occurrence_id: occurrence_id.clone(),
            observation_version,
            lifecycle: LifecycleState::Active,
            bundle: command.bundle,
            value: Some(command.value),
        };
        self.remove_from_indexes(&occurrence_id);
        self.records.insert(occurrence_id.clone(), record);
        self.add_to_indexes(&occurrence_id);
        if activates_record {
            *self
                .active_count
                .as_mut()
                .expect("active count was initialized") += 1;
        }
        let outcome = self.push_event(
            occurrence_id,
            observation_version,
            kind,
            tenant_ref,
            access_policy_ref,
        );
        self.idempotency.insert(
            command.idempotency_ref,
            IdempotencyEntry {
                fingerprint,
                outcome,
            },
        );
        Ok(outcome)
    }

    /// Apply a sequence atomically. This is both the batch-ingest path and the
    /// bounded streaming-ingest primitive: the caller may feed any iterator, while a
    /// failure leaves the original runtime untouched. Atomicity uses a touched-row
    /// undo journal rather than cloning total runtime state, so staging/rollback is
    /// proportional to the batch and each touched bundle's index fan-out.
    pub fn ingest_stream<I>(&mut self, commands: I) -> Result<Vec<ApplyOutcome>, ServedError>
    where
        I: IntoIterator<Item = ServedIngest<T>>,
    {
        let mut outcomes = Vec::new();
        let mut undo = Vec::new();
        for command in commands {
            let occurrence_id = command.target_occurrence_id.clone();
            let idempotency_ref = command.idempotency_ref.clone();
            let checkpoint = IngestUndo {
                occurrence_id,
                previous_record: self.records.get(&command.target_occurrence_id).cloned(),
                idempotency_ref: idempotency_ref.clone(),
                previous_idempotency: self.idempotency.get(&idempotency_ref).cloned(),
                events_len: self.events.len(),
                next_sequence: self.next_sequence,
                active_count: self.active_count,
            };
            match self.ingest(command) {
                Ok(outcome) => {
                    if outcome.disposition == ApplyDisposition::Applied {
                        undo.push(checkpoint);
                    }
                    outcomes.push(outcome);
                }
                Err(error) => {
                    for entry in undo.into_iter().rev() {
                        self.rollback_ingest(entry);
                    }
                    return Err(error);
                }
            }
        }
        Ok(outcomes)
    }

    pub fn delete(
        &mut self,
        scope: &ServedPolicyScope,
        command: ServedDelete,
    ) -> Result<ApplyOutcome, ServedError> {
        let fingerprint = stable_fingerprint(&command).map_err(|_| ServedError::Codec)?;
        if let Some(outcome) = self.replay_or_conflict(&command.idempotency_ref, fingerprint)? {
            return Ok(outcome);
        }
        self.ensure_active_count();
        let record = self
            .records
            .get(&command.occurrence_id)
            .ok_or(ServedError::NotFound)?;
        let occurrence = record.occurrence().ok_or(ServedError::InvalidBundle)?;
        if !scope.authorizes_occurrence(occurrence) {
            return Err(ServedError::Forbidden);
        }
        if occurrence.policy.legal_hold_ref.is_some() {
            return Err(ServedError::LegalHold);
        }
        if command.expected_version != record.observation_version
            || record.lifecycle == LifecycleState::Tombstoned
        {
            return Err(ServedError::VersionConflict);
        }
        let tenant_ref = occurrence.policy.tenant_ref.clone();
        let access_policy_ref = occurrence.policy.access_policy_ref.clone();
        self.remove_from_indexes(&command.occurrence_id);
        let record = self
            .records
            .get_mut(&command.occurrence_id)
            .ok_or(ServedError::NotFound)?;
        record.observation_version = record.observation_version.saturating_add(1);
        record.lifecycle = LifecycleState::Tombstoned;
        record.value = None;
        let version = record.observation_version;
        *self
            .active_count
            .as_mut()
            .expect("active count was initialized") -= 1;
        let outcome = self.push_event(
            command.occurrence_id,
            version,
            ServedEventKind::Deleted,
            tenant_ref,
            access_policy_ref,
        );
        self.idempotency.insert(
            command.idempotency_ref,
            IdempotencyEntry {
                fingerprint,
                outcome,
            },
        );
        Ok(outcome)
    }

    pub fn move_to_cold(
        &mut self,
        scope: &ServedPolicyScope,
        occurrence_id: &OccurrenceId,
    ) -> Result<ApplyOutcome, ServedError> {
        self.transition_lifecycle(
            scope,
            occurrence_id,
            LifecycleState::Cold,
            ServedEventKind::MovedToCold,
        )
    }

    pub fn restore(
        &mut self,
        scope: &ServedPolicyScope,
        occurrence_id: &OccurrenceId,
    ) -> Result<ApplyOutcome, ServedError> {
        self.transition_lifecycle(
            scope,
            occurrence_id,
            LifecycleState::Active,
            ServedEventKind::Restored,
        )
    }

    fn transition_lifecycle(
        &mut self,
        scope: &ServedPolicyScope,
        occurrence_id: &OccurrenceId,
        target: LifecycleState,
        event_kind: ServedEventKind,
    ) -> Result<ApplyOutcome, ServedError> {
        let record = self
            .records
            .get_mut(occurrence_id)
            .ok_or(ServedError::NotFound)?;
        let occurrence = record.occurrence().ok_or(ServedError::InvalidBundle)?;
        if !scope.authorizes_occurrence(occurrence) {
            return Err(ServedError::Forbidden);
        }
        if record.lifecycle == LifecycleState::Tombstoned {
            return Err(ServedError::InvalidLifecycle);
        }
        let allowed = matches!(
            (record.lifecycle, target),
            (LifecycleState::Active, LifecycleState::Cold)
                | (LifecycleState::Cold, LifecycleState::Active)
        );
        if !allowed {
            return Err(ServedError::InvalidLifecycle);
        }
        let tenant_ref = occurrence.policy.tenant_ref.clone();
        let access_policy_ref = occurrence.policy.access_policy_ref.clone();
        record.lifecycle = target;
        record.observation_version = record.observation_version.saturating_add(1);
        let version = record.observation_version;
        Ok(self.push_event(
            occurrence_id.clone(),
            version,
            event_kind,
            tenant_ref,
            access_policy_ref,
        ))
    }

    fn push_event(
        &mut self,
        occurrence_id: OccurrenceId,
        observation_version: u64,
        kind: ServedEventKind,
        tenant_ref: OpaqueRef,
        access_policy_ref: OpaqueRef,
    ) -> ApplyOutcome {
        let sequence = self.next_sequence;
        self.next_sequence = self.next_sequence.saturating_add(1);
        self.events.push(ServedEvent {
            sequence,
            occurrence_id,
            observation_version,
            kind,
            tenant_ref,
            access_policy_ref,
        });
        ApplyOutcome {
            disposition: ApplyDisposition::Applied,
            observation_version,
            event_sequence: sequence,
        }
    }

    fn rollback_ingest(&mut self, undo: IngestUndo<T>) {
        self.remove_from_indexes(&undo.occurrence_id);
        match undo.previous_record {
            Some(record) => {
                self.records.insert(undo.occurrence_id.clone(), record);
                self.add_to_indexes(&undo.occurrence_id);
            }
            None => {
                self.records.remove(&undo.occurrence_id);
            }
        }
        match undo.previous_idempotency {
            Some(entry) => {
                self.idempotency.insert(undo.idempotency_ref, entry);
            }
            None => {
                self.idempotency.remove(&undo.idempotency_ref);
            }
        }
        self.events.truncate(undo.events_len);
        self.next_sequence = undo.next_sequence;
        self.active_count = undo.active_count;
    }
}

fn validate_ingest_command<T>(command: &ServedIngest<T>) -> Result<&Occurrence, ServedError>
where
    T: crate::GovernedModality,
{
    command
        .bundle
        .validate_certified()
        .map_err(|_| ServedError::InvalidBundle)?;
    if !command.value.validate_governed_payload() {
        return Err(ServedError::UnsafePayload);
    }
    let occurrence = command
        .bundle
        .occurrences
        .iter()
        .find(|occurrence| occurrence.id == command.target_occurrence_id)
        .ok_or(ServedError::InvalidBundle)?;
    if occurrence.observation_version == 0
        || !bundle_matches_modality(&command.bundle, occurrence, command.value.storage_kind())
    {
        return Err(ServedError::InvalidBundle);
    }
    Ok(occurrence)
}

impl<T> ServedModalityRuntime<T>
where
    T: crate::GovernedModality
        + Clone
        + PartialEq
        + std::fmt::Debug
        + serde::Serialize
        + serde::de::DeserializeOwned,
{
    fn replay_or_conflict(
        &self,
        idempotency_ref: &OpaqueRef,
        fingerprint: [u8; 32],
    ) -> Result<Option<ApplyOutcome>, ServedError> {
        match self.idempotency.get(idempotency_ref) {
            None => Ok(None),
            Some(entry) if entry.fingerprint == fingerprint => Ok(Some(ApplyOutcome {
                disposition: ApplyDisposition::IdempotentReplay,
                ..entry.outcome
            })),
            Some(_) => Err(ServedError::IdempotencyConflict),
        }
    }

    fn ingest_event_kind(
        &self,
        occurrence_id: &OccurrenceId,
        expected_version: Option<u64>,
        observation_version: u64,
    ) -> Result<ServedEventKind, ServedError> {
        match self.records.get(occurrence_id) {
            None if expected_version.is_some() => Err(ServedError::VersionConflict),
            None => Ok(ServedEventKind::Ingested),
            Some(current)
                if expected_version != Some(current.observation_version)
                    || observation_version <= current.observation_version =>
            {
                Err(ServedError::VersionConflict)
            }
            Some(_) => Ok(ServedEventKind::Updated),
        }
    }
}

fn stable_fingerprint<T: Serialize>(value: &T) -> Result<[u8; 32], ()> {
    let bytes = serde_json::to_vec(value).map_err(|_| ())?;
    Ok(Sha256::digest(bytes).into())
}
