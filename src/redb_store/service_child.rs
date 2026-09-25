//! EH-611: durable one-shot SERVICE child reservations in the graph shard.
//!
//! The journal row and its audit prerequisite share a native graph owner.
//! Dispatch is permitted only after a newly-created reservation is committed;
//! replay returns `created=false`, including after process restart. A terminal
//! outcome is a compare-and-set and cannot authorize a second child attempt.

use super::shard::{Shard, ShardWrite};
use super::*;
use eg_storage::ScopedOwnerTableMut;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// Caller-bound authority for one SERVICE child. All digests are lowercase
/// SHA-256, and neither arguments nor child result bodies enter this table.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ServiceChildBinding {
    pub tenant: String,
    pub owner_principal: String,
    pub owner_ref: String,
    pub server: String,
    pub tool: String,
    pub subject_id: String,
    pub argument_sha256: String,
    /// GraphOS `params_digest({server, tool, arguments})` in the caller audit.
    pub audit_params_sha256: String,
    pub request_id: String,
    pub policy_revision: String,
    pub registry_revision: String,
    pub scopes_sha256: String,
}

impl ServiceChildBinding {
    fn validate(&self) -> Result<(), String> {
        fn token(value: &str, max: usize) -> bool {
            !value.is_empty()
                && value.len() <= max
                && value
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b"._:-/".contains(&b))
        }
        fn digest(value: &str) -> bool {
            value.len() == 64 && value.bytes().all(|b| b.is_ascii_hexdigit())
        }
        if !token(&self.tenant, 128)
            || !token(&self.owner_principal, 256)
            || !token(&self.owner_ref, 128)
            || !token(&self.server, 128)
            || !token(&self.tool, 128)
            || !token(&self.subject_id, 256)
            || !token(&self.request_id, 128)
            || !token(&self.policy_revision, 128)
            || !token(&self.registry_revision, 128)
            || !digest(&self.argument_sha256)
            || !digest(&self.audit_params_sha256)
            || !digest(&self.scopes_sha256)
        {
            return Err("SERVICE_CHILD_INVALID_BINDING".into());
        }
        let owner_hash = hex::encode(Sha256::digest(self.owner_principal.as_bytes()));
        if self.owner_ref != format!("principal:sha256:{owner_hash}") {
            return Err("SERVICE_CHILD_OWNER_MISMATCH".into());
        }
        Ok(())
    }

    /// Stable idempotency key for caller/request. Changed target or payload under
    /// this key is a conflict rather than a second reservation.
    pub(crate) fn record_id(&self) -> String {
        let mut hasher = Sha256::new();
        for part in [
            self.tenant.as_str(),
            self.owner_principal.as_str(),
            self.request_id.as_str(),
        ] {
            hasher.update((part.len() as u64).to_be_bytes());
            hasher.update(part.as_bytes());
        }
        hex::encode(hasher.finalize())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub(crate) enum ServiceChildOutcome {
    Reserved,
    Succeeded { result_sha256: String },
    OutcomeUnknown { reason_code: String },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ServiceChildRecord {
    pub record_id: String,
    pub binding: ServiceChildBinding,
    pub audit_seq: u64,
    pub audit_entry_sha256: String,
    pub outcome: ServiceChildOutcome,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ServiceChildReservation {
    pub record: ServiceChildRecord,
    pub created: bool,
}

fn decode(bytes: &[u8]) -> Result<ServiceChildRecord, String> {
    rmp_serde::from_slice(bytes).map_err(|_| "SERVICE_CHILD_CORRUPT_ROW".into())
}

fn audit_reservation(
    audit: &eg_storage::ScopedOwnerTableMut<'_, (&'static str, u64), &'static [u8]>,
    graph: &str,
    binding: &ServiceChildBinding,
    audit_seq: u64,
    audit_entry_sha256: &str,
) -> Result<(), String> {
    let row = audit
        .get((graph, audit_seq))?
        .ok_or("SERVICE_CHILD_AUDIT_REQUIRED")?;
    let (_, hash, line) =
        crate::audit::decode_entry(row.value()).ok_or("SERVICE_CHILD_AUDIT_CORRUPT")?;
    if hex::encode(hash) != audit_entry_sha256 {
        return Err("SERVICE_CHILD_AUDIT_MISMATCH".into());
    }
    let expected = format!(
        "OP_AUDIT|v1|tenant={}|principal={}|op=fleet.call|surface=",
        binding.tenant, binding.owner_principal
    );
    let line = std::str::from_utf8(line).map_err(|_| "SERVICE_CHILD_AUDIT_CORRUPT")?;
    if !line.starts_with(&expected)
        || !line.contains(&format!(
            "|params_sha256={}|status=reserved|request_id={}|identity=0",
            binding.audit_params_sha256, binding.request_id
        ))
    {
        return Err("SERVICE_CHILD_AUDIT_MISMATCH".into());
    }
    Ok(())
}

/// Atomically check the existing caller audit and insert the child row. The
/// verified carrier owner must be supplied by the served adapter, never copied
/// from untrusted method parameters. GraphOS must use the returned `created`.
pub(crate) fn service_child_reserve(
    shard: &Shard,
    graph: &str,
    binding: ServiceChildBinding,
    verified_tenant: &str,
    verified_owner: &str,
    audit_ref: &str,
) -> Result<ServiceChildReservation, String> {
    binding.validate()?;
    if binding.tenant != verified_tenant || binding.owner_principal != verified_owner {
        return Err("SERVICE_CHILD_CARRIER_MISMATCH".into());
    }
    if audit_ref != binding.request_id {
        return Err("SERVICE_CHILD_AUDIT_MISMATCH".into());
    }
    let record_id = binding.record_id();
    let members = shard.graph_members(&[graph])?;
    let (group, batches) =
        shard.admit_maintenance(&members, &format!("service_child/reserve/{record_id}"))?;
    let write = ShardWrite::open(shard, &group, &members, &batches)?;
    let applied = (|| {
        let request_identity = format!(
            "{}\0{}\0{}\0fleet.call",
            binding.tenant, binding.owner_principal, binding.request_id
        );
        let request_key = hex::encode(Sha256::digest(format!("{request_identity}\0reserve")));
        let requests = write.graph(graph)?.open_scoped_table(AUDIT_REQUESTS)?;
        let request_row = requests
            .get((graph, request_key.as_str()))?
            .ok_or("SERVICE_CHILD_AUDIT_REQUIRED")?;
        let audit_record: super::audit::AuditRequestRecord =
            rmp_serde::from_slice(request_row.value())
                .map_err(|_| "SERVICE_CHILD_AUDIT_CORRUPT")?;
        let audit_seq = audit_record.seq;
        let audit_entry_sha256 = hex::encode(audit_record.entry_hash);
        let mut children = write.graph(graph)?.open_scoped_table(SERVICE_CHILDREN)?;
        if let Some(row) = children.get((graph, record_id.as_str()))? {
            let existing = decode(row.value())?;
            if existing.binding != binding
                || existing.audit_seq != audit_seq
                || existing.audit_entry_sha256 != audit_entry_sha256
            {
                return Err("SERVICE_CHILD_IDEMPOTENCY_CONFLICT".into());
            }
            return Ok(ServiceChildReservation {
                record: existing,
                created: false,
            });
        }
        let audit = write.graph(graph)?.open_scoped_table(AUDIT)?;
        audit_reservation(&audit, graph, &binding, audit_seq, &audit_entry_sha256)?;
        let record = ServiceChildRecord {
            record_id,
            binding,
            audit_seq,
            audit_entry_sha256,
            outcome: ServiceChildOutcome::Reserved,
        };
        let encoded = rmp_serde::to_vec_named(&record).map_err(|e| e.to_string())?;
        children.insert((graph, record.record_id.as_str()), encoded.as_slice())?;
        Ok(ServiceChildReservation {
            record,
            created: true,
        })
    })();
    let result = match (applied, write.finish()) {
        (Ok(result), Ok(())) => result,
        (Err(error), _) | (_, Err(error)) => {
            shard.mutations().abort_group(group)?;
            return Err(error);
        }
    };
    shard.commit_drain(group, &batches, 0)?;
    Ok(result)
}

pub(crate) fn service_child_get(
    shard: &Shard,
    graph: &str,
    record_id: &str,
    verified_tenant: &str,
    verified_owner: &str,
) -> Result<Option<ServiceChildRecord>, String> {
    let handle = shard.graph(graph)?;
    let read = shard.read(&handle)?;
    let table = read.scoped_owner_table(SERVICE_CHILDREN)?;
    let Some(row) = table.get((graph, record_id))? else {
        return Ok(None);
    };
    let record = decode(row.value())?;
    if record.binding.tenant != verified_tenant || record.binding.owner_principal != verified_owner
    {
        return Ok(None);
    }
    Ok(Some(record))
}

/// Compare-and-set terminal outcome. A duplicate of the same outcome returns
/// the committed record; a different terminal result is a conflict.
pub(crate) fn service_child_finish(
    shard: &Shard,
    graph: &str,
    record_id: &str,
    verified_tenant: &str,
    verified_owner: &str,
    outcome: ServiceChildOutcome,
) -> Result<ServiceChildRecord, String> {
    match &outcome {
        ServiceChildOutcome::Reserved => return Err("SERVICE_CHILD_INVALID_OUTCOME".into()),
        ServiceChildOutcome::Succeeded { result_sha256 }
            if result_sha256.len() != 64
                || !result_sha256.bytes().all(|b| b.is_ascii_hexdigit()) =>
        {
            return Err("SERVICE_CHILD_INVALID_OUTCOME".into())
        }
        ServiceChildOutcome::OutcomeUnknown { reason_code }
            if reason_code.is_empty()
                || reason_code.len() > 128
                || !reason_code
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b"._:-/".contains(&b)) =>
        {
            return Err("SERVICE_CHILD_INVALID_OUTCOME".into())
        }
        _ => {}
    }
    let members = shard.graph_members(&[graph])?;
    let (group, batches) =
        shard.admit_maintenance(&members, &format!("service_child/finish/{record_id}"))?;
    let write = ShardWrite::open(shard, &group, &members, &batches)?;
    let applied = (|| {
        let mut children = write.graph(graph)?.open_scoped_table(SERVICE_CHILDREN)?;
        let row = children
            .get((graph, record_id))?
            .ok_or("SERVICE_CHILD_NOT_FOUND")?;
        let mut record = decode(row.value())?;
        if record.binding.tenant != verified_tenant
            || record.binding.owner_principal != verified_owner
        {
            return Err("SERVICE_CHILD_NOT_FOUND".into());
        }
        if record.outcome != ServiceChildOutcome::Reserved {
            return if record.outcome == outcome {
                Ok(record)
            } else {
                Err("SERVICE_CHILD_OUTCOME_CONFLICT".into())
            };
        }
        record.outcome = outcome;
        let encoded = rmp_serde::to_vec_named(&record).map_err(|e| e.to_string())?;
        children.insert((graph, record_id), encoded.as_slice())?;
        Ok(record)
    })();
    let result = match (applied, write.finish()) {
        (Ok(result), Ok(())) => result,
        (Err(error), _) | (_, Err(error)) => {
            shard.mutations().abort_group(group)?;
            return Err(error);
        }
    };
    shard.commit_drain(group, &batches, 0)?;
    Ok(result)
}
