//! Two-identity mutation replay (RF-RULING-004).
//!
//! Replay has two identities and never one overloaded context digest.
//! [`OperationReplayIdentityV1`] is the stable operation identity over tenant,
//! actor, authority scope and purpose, method, canonical payload digest, policy
//! digest, and idempotency key; it excludes nonce, request/trace IDs and
//! timestamps. [`NonceReplayKeyV1`] is the attempt-specific nonce identity.
//! `AuthorityContextV1::context_digest` includes the nonce and is therefore
//! attempt-specific, so it can never decide operation replay.
//!
//! The four outcomes are exactly:
//!
//! * the same nonce is rejected ([`ReplayResolution::NonceRejected`]);
//! * a fresh nonce plus the exact stable operation identity replays the prior
//!   result ([`ReplayResolution::ReplayedResult`]);
//! * a changed payload, scope, policy or method under the same idempotency key
//!   conflicts ([`ReplayResolution::Conflict`]);
//! * anything else is fresh ([`ReplayResolution::Fresh`]).

use crate::admitted::AdmittedMutation;
use crate::tables::{REPLAY_NONCES, REPLAY_OPERATIONS};
use eg_storage::{
    decode_ledger_record, encode_bounded, ledger_scope_key, OperationReplayRow, OwnerDomain,
};
use eg_types::authority::{NonceReplayKeyV1, OperationReplayIdentityV1};
use eg_types::contract::Digest256V1;
use eg_types::mutation::MutationReceiptV1;
use redb::{ReadableTable, WriteTransaction};

/// The one durable replay decision for one proposed attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReplayResolution {
    /// No nonce row and no operation row: the attempt may execute.
    Fresh,
    /// This exact attempt nonce was already consumed under `idempotency_key`.
    NonceRejected { idempotency_key: String },
    /// A fresh nonce over a byte-identical stable operation identity: the
    /// recorded receipt is returned instead of executing again.
    ReplayedResult(Box<MutationReceiptV1>),
    /// The same idempotency key was already used by a different stable
    /// operation identity -- a changed method, payload, scope, or policy.
    Conflict {
        recorded: Digest256V1,
        proposed: Digest256V1,
    },
}

pub(crate) fn resolve_replay_in<D: OwnerDomain>(
    write: &AdmittedMutation<'_, D>,
    operation: &OperationReplayIdentityV1,
    nonce: &NonceReplayKeyV1,
) -> Result<ReplayResolution, String> {
    let scope_key = ledger_scope_key(write.scope());
    let proposed = operation.digest()?;
    let nonce_digest = nonce.digest()?.to_hex();
    if let Some(idempotency_key) = read_nonce(write.transaction(), &scope_key, &nonce_digest)? {
        return Ok(ReplayResolution::NonceRejected { idempotency_key });
    }
    let Some(row) = read_operation(
        write.transaction(),
        &scope_key,
        operation.idempotency_key.as_str(),
    )?
    else {
        return Ok(ReplayResolution::Fresh);
    };
    if row.operation_replay_digest != proposed {
        return Ok(ReplayResolution::Conflict {
            recorded: row.operation_replay_digest,
            proposed,
        });
    }
    Ok(ReplayResolution::ReplayedResult(Box::new(row.receipt)))
}

/// Consume one attempt nonce and record its operation receipt inside the same
/// admitted write as the effect, so a crash can never leave an effect without
/// its receipt or a consumed nonce without its operation row.
pub(crate) fn record_replay_in<D: OwnerDomain>(
    write: &AdmittedMutation<'_, D>,
    operation: &OperationReplayIdentityV1,
    nonce: &NonceReplayKeyV1,
    receipt: &MutationReceiptV1,
) -> Result<(), String> {
    receipt.validate()?;
    let operation_replay_digest = operation.digest()?;
    let nonce_replay_digest = nonce.digest()?;
    if receipt.operation_replay_digest != operation_replay_digest
        || receipt.nonce_replay_digest != nonce_replay_digest
    {
        return Err("mutation receipt does not bind both replay digests".to_string());
    }
    let scope_key = ledger_scope_key(write.scope());
    let idempotency_key = operation.idempotency_key.as_str();
    if let Some(existing) = read_operation(write.transaction(), &scope_key, idempotency_key)? {
        if existing.operation_replay_digest != operation_replay_digest {
            return Err(
                "IDEMPOTENCY_CONFLICT: key was already used by a different operation".to_string(),
            );
        }
    }
    let row = OperationReplayRow {
        identity: write.scope().clone(),
        idempotency_key: idempotency_key.to_string(),
        operation_replay_digest,
        nonce_replay_digest,
        receipt: receipt.clone(),
    };
    let bytes = encode_bounded(&row, "mutation replay operation row")?;
    write
        .transaction()
        .open_table(REPLAY_OPERATIONS)
        .map_err(|error| error.to_string())?
        .insert((scope_key.as_str(), idempotency_key), bytes.as_slice())
        .map_err(|error| error.to_string())?;
    let nonce_digest = nonce_replay_digest.to_hex();
    write
        .transaction()
        .open_table(REPLAY_NONCES)
        .map_err(|error| error.to_string())?
        .insert((scope_key.as_str(), nonce_digest.as_str()), idempotency_key)
        .map_err(|error| error.to_string())?;
    Ok(())
}

pub(crate) fn remove_replay_rows(wtx: &WriteTransaction, scope_key: &str) -> Result<(), String> {
    let mut nonces = wtx
        .open_table(REPLAY_NONCES)
        .map_err(|error| error.to_string())?;
    let mut nonce_keys = Vec::new();
    for row in nonces.iter().map_err(|error| error.to_string())? {
        let (key, _) = row.map_err(|error| error.to_string())?;
        if key.value().0 == scope_key {
            nonce_keys.push(key.value().1.to_string());
        }
    }
    for key in nonce_keys {
        nonces
            .remove((scope_key, key.as_str()))
            .map_err(|error| error.to_string())?;
    }
    let mut operations = wtx
        .open_table(REPLAY_OPERATIONS)
        .map_err(|error| error.to_string())?;
    let mut operation_keys = Vec::new();
    for row in operations.iter().map_err(|error| error.to_string())? {
        let (key, _) = row.map_err(|error| error.to_string())?;
        if key.value().0 == scope_key {
            operation_keys.push(key.value().1.to_string());
        }
    }
    for key in operation_keys {
        operations
            .remove((scope_key, key.as_str()))
            .map_err(|error| error.to_string())?;
    }
    Ok(())
}

fn read_nonce(
    wtx: &WriteTransaction,
    scope_key: &str,
    nonce_digest: &str,
) -> Result<Option<String>, String> {
    let table = wtx
        .open_table(REPLAY_NONCES)
        .map_err(|error| error.to_string())?;
    let found = table
        .get((scope_key, nonce_digest))
        .map_err(|error| error.to_string())?
        .map(|value| value.value().to_string());
    Ok(found)
}

fn read_operation(
    wtx: &WriteTransaction,
    scope_key: &str,
    idempotency_key: &str,
) -> Result<Option<OperationReplayRow>, String> {
    let table = wtx
        .open_table(REPLAY_OPERATIONS)
        .map_err(|error| error.to_string())?;
    let row = table
        .get((scope_key, idempotency_key))
        .map_err(|error| error.to_string())?
        .map(|value| decode_ledger_record::<OperationReplayRow>(value.value()))
        .transpose()?;
    Ok(row)
}
