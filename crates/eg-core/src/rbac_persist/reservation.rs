//! Durable whole-image read reservation for a native coordinator.
//! No grant or profile-admission result is produced here.

use super::*;
use eg_storage::RBAC_PARTICIPANTS;
use eg_transaction::{
    AdmittedMutation, Begin, CoordinatorDecisionBinding, CoordinatorDecisionCertificate,
};
use redb::ReadableTable;
use serde::{Deserialize, Serialize};

const ACTIVE: &str = "active";
const MAX_RECORD_BYTES: usize = 4096;

fn canonical_digest(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

/// Exact authority preimage and native coordinator operation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RbacReservationRequest {
    binding: CoordinatorDecisionBinding,
    transaction_id: String,
    request_digest: String,
    revision: u64,
    authority_digest: String,
    bootstrap: IdentityBootstrapState,
}

impl RbacReservationRequest {
    pub fn new(
        snapshot: &RbacAuthoritySnapshot,
        binding: CoordinatorDecisionBinding,
    ) -> Result<Self, RbacPersistError> {
        binding.validate().map_err(RbacPersistError::Redb)?;
        let request = Self {
            transaction_id: binding.transaction_id.clone(),
            request_digest: hex::encode(binding.request_digest),
            binding,
            revision: snapshot.revision(),
            authority_digest: full_image_digest(
                snapshot.policy(),
                snapshot.identities(),
                snapshot.bootstrap(),
            )?,
            bootstrap: snapshot.bootstrap(),
        };
        request.validate()?;
        Ok(request)
    }

    fn validate(&self) -> Result<(), RbacPersistError> {
        self.binding.validate().map_err(RbacPersistError::Redb)?;
        if self.transaction_id != self.binding.transaction_id
            || self.request_digest != hex::encode(self.binding.request_digest)
            || self.transaction_id.is_empty()
            || self.transaction_id.len() > 128
            || self.transaction_id.chars().any(char::is_control)
            || !canonical_digest(&self.request_digest)
            || !canonical_digest(&self.authority_digest)
        {
            return Err(RbacPersistError::IncompleteState(
                "invalid RBAC participant identity",
            ));
        }
        Ok(())
    }

    pub fn binding(&self) -> &CoordinatorDecisionBinding {
        &self.binding
    }

    pub fn transaction_id(&self) -> &str {
        &self.transaction_id
    }
    pub fn request_digest(&self) -> &str {
        &self.request_digest
    }
}

/// Native participant receipt; contains no session bearer material.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RbacReservation {
    schema_version: u16,
    owner: String,
    scope: eg_types::MutationScopeIdentity,
    request: RbacReservationRequest,
    prepared_revision: u64,
}

impl RbacReservation {
    pub fn request(&self) -> &RbacReservationRequest {
        &self.request
    }
}

/// Native coordinator decision source. Missing evidence never means abort.
pub trait RbacReservationDecisionSource {
    fn terminal_decision(
        &self,
        request: &RbacReservationRequest,
    ) -> Result<Option<CoordinatorDecisionCertificate>, String>;
}

fn decode_active(bytes: Option<&[u8]>) -> Result<Option<RbacReservation>, RbacPersistError> {
    let Some(bytes) = bytes else { return Ok(None) };
    if bytes.len() > MAX_RECORD_BYTES {
        return Err(RbacPersistError::IncompleteState(
            "oversized RBAC reservation",
        ));
    }
    let result: Option<RbacReservation> = serde_json::from_slice(bytes)?;
    if let Some(record) = &result {
        record.request.validate()?;
        if record.schema_version != 1
            || record.owner != RBAC_PHYSICAL_STORE
            || record.scope != native_security_control_identity()?
            || record.request.revision.checked_add(1) != Some(record.prepared_revision)
        {
            return Err(RbacPersistError::IncompleteState(
                "invalid RBAC reservation binding",
            ));
        }
    }
    Ok(result)
}

fn read_active(
    write: &AdmittedMutation<'_, RbacOwner>,
) -> Result<Option<RbacReservation>, RbacPersistError> {
    let table = write
        .open_read_table(RBAC_PARTICIPANTS)
        .map_err(RbacPersistError::Redb)?;
    let value = table
        .get(ACTIVE)
        .map_err(|e| RbacPersistError::Redb(e.to_string()))?;
    decode_active(value.as_ref().map(|v| v.value()))
}

pub(super) fn refuse_reserved(
    write: &AdmittedMutation<'_, RbacOwner>,
) -> Result<(), RbacPersistError> {
    if let Some(record) = read_active(write)? {
        return Err(RbacPersistError::Reserved {
            transaction_id: record.request.transaction_id,
        });
    }
    Ok(())
}

fn batch(
    store: &RbacStore,
    record: &RbacReservation,
    phase: &str,
    expected: u64,
) -> Result<eg_types::MutationBatch, RbacPersistError> {
    let digest = hex::encode(Sha256::digest(serde_json::to_vec(&(phase, record))?));
    let target = expected
        .checked_add(1)
        .ok_or(RbacPersistError::IncompleteState("RBAC revision overflow"))?;
    let mut batch = super::durable_write::authority_batch(store, expected, target, &digest);
    // The existing maintenance ledger permanently binds one transaction identity,
    // even after the active reservation clears and the owner version advances.
    let transaction = hex::encode(Sha256::digest(record.request.transaction_id.as_bytes()));
    let stage = if phase == "prepare" {
        "prepare"
    } else {
        "resolve"
    };
    batch.envelope = eg_types::mutation_batch::MutationEnvelope::maintenance(
        store.owner.principal(),
        "rbac_participant",
        &transaction,
        &format!("rbac-participant:{stage}:{transaction}"),
    )
    .map_err(RbacPersistError::Redb)?;
    batch.operations[0].method = eg_types::protocol::Method::ApplyMutation {
        event_type: format!("rbac_participant_{phase}"),
        query: format!("sha256:{digest}"),
    };
    Ok(batch)
}

/// Include the entire policy image, especially its private identity store and
/// governed ledger; the public policy-decision digest deliberately omits them.
pub(super) fn full_image_digest(
    policy: &RbacPolicy,
    identities: &BTreeMap<String, AgentIdentity>,
    bootstrap: IdentityBootstrapState,
) -> Result<String, RbacPersistError> {
    let mut value = serde_json::to_value((policy, identities, bootstrap))?;
    value.sort_all_objects();
    let mut digest = Sha256::new();
    digest.update(b"eg/rbac-participant-image/v1\0");
    digest.update(serde_json::to_vec(&value)?);
    Ok(hex::encode(digest.finalize()))
}

fn validate_image(
    write: &AdmittedMutation<'_, RbacOwner>,
    request: &RbacReservationRequest,
) -> Result<(), RbacPersistError> {
    let table = write
        .open_read_table(RBAC_TABLE)
        .map_err(RbacPersistError::Redb)?;
    let bytes = |key| -> Result<Vec<u8>, RbacPersistError> {
        table
            .get(key)
            .map_err(|e| RbacPersistError::Redb(e.to_string()))?
            .map(|value| value.value().to_vec())
            .ok_or(RbacPersistError::IncompleteState(
                "missing reserved authority image",
            ))
    };
    let policy: RbacPolicy = serde_json::from_slice(&bytes(POLICY_KEY)?)?;
    let identities: BTreeMap<String, AgentIdentity> =
        serde_json::from_slice(&bytes(IDENTITIES_KEY)?)?;
    let bootstrap: IdentityBootstrapState = serde_json::from_slice(&bytes(BOOTSTRAP_KEY)?)?;
    if full_image_digest(&policy, &identities, bootstrap)? != request.authority_digest
        || bootstrap != request.bootstrap
    {
        return Err(RbacPersistError::IncompleteState(
            "reserved authority image changed",
        ));
    }
    Ok(())
}

fn finish(
    store: &RbacStore,
    write: AdmittedMutation<'_, RbacOwner>,
    batch: &eg_types::MutationBatch,
    active: Option<&RbacReservation>,
    source_version: Option<u64>,
    result: Vec<u8>,
) -> Result<(), RbacPersistError> {
    let owner = write
        .owner_rows(&store.owner, batch)
        .map_err(RbacPersistError::Redb)?;
    {
        let mut table = owner
            .open_table(RBAC_PARTICIPANTS)
            .map_err(RbacPersistError::Redb)?;
        let bytes = serde_json::to_vec(&active)?;
        if bytes.len() > MAX_RECORD_BYTES {
            return Err(RbacPersistError::IncompleteState(
                "oversized RBAC reservation",
            ));
        }
        table
            .insert(ACTIVE, bytes.as_slice())
            .map_err(|e| RbacPersistError::Redb(e.to_string()))?;
    }
    owner.finish_owner().map_err(RbacPersistError::Redb)?;
    store
        .mutations
        .finish(&write, batch, Some(result), 0, source_version)
        .map_err(RbacPersistError::Redb)?;
    store
        .mutations
        .commit(write, batch)
        .map_err(RbacPersistError::Redb)
}

impl RbacStore {
    /// Read terminal participant evidence from the existing mutation ledger.
    /// This is not an authorization or a profile-admission receipt.
    pub fn reservation_outcome(
        &self,
        request: &RbacReservationRequest,
    ) -> Result<Option<bool>, RbacPersistError> {
        request.validate()?;
        let record = RbacReservation {
            schema_version: 1,
            owner: RBAC_PHYSICAL_STORE.into(),
            scope: self.owner.identity().clone(),
            prepared_revision: request
                .revision
                .checked_add(1)
                .ok_or(RbacPersistError::IncompleteState("RBAC revision overflow"))?,
            request: request.clone(),
        };
        let read = self.scoped_read()?;
        let mut outcome = None;
        for (phase, committed) in [("commit", true), ("abort", false)] {
            let expected = batch(self, &record, phase, record.prepared_revision)?;
            if let Some(receipt) = eg_transaction::read_ledger(&read, &expected.batch_id)
                .map_err(RbacPersistError::Redb)?
            {
                if receipt.status != eg_types::MutationBatchStatus::Committed
                    || serde_json::to_vec(&receipt.batch)? != serde_json::to_vec(&expected)?
                    || outcome.is_some()
                {
                    return Err(RbacPersistError::IncompleteState(
                        "invalid RBAC participant terminal receipt",
                    ));
                }
                outcome = Some(committed);
            }
        }
        Ok(outcome)
    }

    /// Compare and persist the reservation in one admitted owner transaction.
    pub fn prepare_reservation(
        &self,
        request: RbacReservationRequest,
    ) -> Result<RbacReservation, RbacPersistError> {
        request.validate()?;
        let record = RbacReservation {
            schema_version: 1,
            owner: RBAC_PHYSICAL_STORE.into(),
            scope: self.owner.identity().clone(),
            prepared_revision: request
                .revision
                .checked_add(1)
                .ok_or(RbacPersistError::IncompleteState("RBAC revision overflow"))?,
            request,
        };
        let encoded = serde_json::to_vec(&Some(&record))?;
        decode_active(Some(&encoded))?;
        let batch = batch(self, &record, "prepare", record.request.revision)?;
        let (write, begun) = self
            .mutations
            .admit(&self.owner, &batch)
            .map_err(RbacPersistError::Redb)?;
        match begun {
            Begin::Replay(receipt) => {
                if serde_json::to_vec(&receipt.batch)? != serde_json::to_vec(&batch)? {
                    return Err(RbacPersistError::IncompleteState(
                        "RBAC transaction identity was reused",
                    ));
                }
                if read_active(&write)?.as_ref() != Some(&record) {
                    return Err(RbacPersistError::IncompleteState(
                        "RBAC reservation is no longer pending",
                    ));
                }
                validate_image(&write, &record.request)?;
                write.abort().map_err(RbacPersistError::Redb)?;
            }
            Begin::Apply { source_version } => {
                refuse_reserved(&write)?;
                validate_image(&write, &record.request)?;
                finish(
                    self,
                    write,
                    &batch,
                    Some(&record),
                    source_version,
                    rmp_serde::to_vec_named(&record)
                        .map_err(|error| RbacPersistError::Redb(error.to_string()))?,
                )?;
            }
        }
        Ok(record)
    }

    pub fn pending_reservation(&self) -> Result<Option<RbacReservation>, RbacPersistError> {
        let read = self.scoped_read()?;
        let table = read
            .open_owner_table(RBAC_PARTICIPANTS)
            .map_err(RbacPersistError::Redb)?;
        let value = table
            .get(ACTIVE)
            .map_err(|e| RbacPersistError::Redb(e.to_string()))?;
        decode_active(value.as_ref().map(|v| v.value()))
    }

    /// Resolve an exact prepare only through the native durable decision oracle.
    pub fn resolve_reservation(
        &self,
        record: &RbacReservation,
        source: &dyn RbacReservationDecisionSource,
    ) -> Result<bool, RbacPersistError> {
        let encoded = serde_json::to_vec(&Some(record))?;
        decode_active(Some(&encoded))?;
        let Some(certificate) = source
            .terminal_decision(&record.request)
            .map_err(RbacPersistError::Redb)?
        else {
            return Ok(false);
        };
        certificate
            .matches(&record.request.binding, certificate.commit)
            .map_err(RbacPersistError::Redb)?;
        let commit = certificate.commit;
        let batch = batch(
            self,
            record,
            if commit { "commit" } else { "abort" },
            record.prepared_revision,
        )?;
        let (write, begun) = self
            .mutations
            .admit(&self.owner, &batch)
            .map_err(RbacPersistError::Redb)?;
        match begun {
            Begin::Replay(receipt) => {
                if serde_json::to_vec(&receipt.batch)? != serde_json::to_vec(&batch)? {
                    return Err(RbacPersistError::IncompleteState(
                        "RBAC terminal decision conflicts with its receipt",
                    ));
                }
                write.abort().map_err(RbacPersistError::Redb)?;
            }
            Begin::Apply { source_version } => {
                if read_active(&write)?.as_ref() != Some(record) {
                    return Err(RbacPersistError::IncompleteState(
                        "RBAC resolution does not match pending reservation",
                    ));
                }
                validate_image(&write, &record.request)?;
                finish(
                    self,
                    write,
                    &batch,
                    None,
                    source_version,
                    rmp_serde::to_vec_named(&(record, commit))
                        .map_err(|error| RbacPersistError::Redb(error.to_string()))?,
                )?;
            }
        }
        Ok(true)
    }
}
