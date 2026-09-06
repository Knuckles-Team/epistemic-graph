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
    decode_ledger_record, encode_bounded, ledger_scope_key, MutationClass, OperationReplayRow,
    OwnerDomain,
};
use eg_types::authority::{NonceReplayKeyV1, OperationReplayIdentityV1};
use eg_types::contract::Digest256V1;
use eg_types::mutation::MutationReceiptV1;

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
    if let Some(idempotency_key) = read_nonce(write, &scope_key, &nonce_digest)? {
        return Ok(ReplayResolution::NonceRejected { idempotency_key });
    }
    let Some(row) = read_operation(write, &scope_key, operation.idempotency_key.as_str())? else {
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
/// Consume one attempt nonce and record its operation receipt inside the same
/// admitted write as the effect, so a crash can never leave an effect without
/// its receipt or a consumed nonce without its operation row.
///
/// Fails closed on **any** pre-existing row for this idempotency key, not only
/// on a differing digest. An identical digest means the caller resolved `Fresh`
/// against a stale view and re-executed an operation it should have replayed;
/// letting it through would apply the effect twice and overwrite the recorded
/// receipt with the second result. Because resolution and recording now share
/// one write transaction, redb's write serialization makes the loser of two
/// concurrent attempts fail here rather than commit.
pub(crate) fn record_replay_in<D: OwnerDomain>(
    write: &AdmittedMutation<'_, D>,
    operation: &OperationReplayIdentityV1,
    nonce: &NonceReplayKeyV1,
    receipt: &MutationReceiptV1,
) -> Result<(), String> {
    if write.admitted_class()? == MutationClass::Maintenance {
        return Err(
            "MAINTENANCE_HAS_NO_REPLAY_IDENTITY: an owner-maintenance write carries no caller \
             operation identity and cannot consume an attempt nonce"
                .to_string(),
        );
    }
    receipt.validate()?;
    let operation_replay_digest = operation.digest()?;
    let nonce_replay_digest = nonce.digest()?;
    if receipt.operation_replay_digest != operation_replay_digest
        || receipt.nonce_replay_digest != nonce_replay_digest
    {
        return Err("mutation receipt does not bind both replay digests".to_string());
    }
    if receipt.scope != operation.authority_scope {
        return Err("mutation receipt names a different scope than its operation".to_string());
    }
    let scope_key = ledger_scope_key(write.scope());
    let idempotency_key = operation.idempotency_key.as_str();
    let nonce_digest = nonce_replay_digest.to_hex();
    if read_nonce(write, &scope_key, &nonce_digest)?.is_some() {
        return Err("REPLAY_NONCE_CONSUMED: this attempt nonce was already recorded".to_string());
    }
    if let Some(existing) = read_operation(write, &scope_key, idempotency_key)? {
        return Err(if existing.operation_replay_digest == operation_replay_digest {
            "REPLAY_ALREADY_RECORDED: this operation was recorded and must be replayed, not re-executed"
                .to_string()
        } else {
            "IDEMPOTENCY_CONFLICT: key was already used by a different operation".to_string()
        });
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
        .scoped_table(REPLAY_OPERATIONS)?
        .insert((scope_key.as_str(), idempotency_key), bytes.as_slice())?;
    write
        .scoped_table(REPLAY_NONCES)?
        .insert((scope_key.as_str(), nonce_digest.as_str()), idempotency_key)
}

fn read_nonce<D: OwnerDomain>(
    write: &AdmittedMutation<'_, D>,
    scope_key: &str,
    nonce_digest: &str,
) -> Result<Option<String>, String> {
    let table = write.scoped_table(REPLAY_NONCES)?;
    let found = table
        .get((scope_key, nonce_digest))?
        .map(|value| value.value().to_string());
    Ok(found)
}

fn read_operation<D: OwnerDomain>(
    write: &AdmittedMutation<'_, D>,
    scope_key: &str,
    idempotency_key: &str,
) -> Result<Option<OperationReplayRow>, String> {
    let table = write.scoped_table(REPLAY_OPERATIONS)?;
    let row = table
        .get((scope_key, idempotency_key))?
        .map(|value| decode_ledger_record::<OperationReplayRow>(value.value()))
        .transpose()?;
    Ok(row)
}
