//! Retention of logged decision inputs (EH-060, DECIDE-LAYER-DESIGN §4.5).
//!
//! A committed record is digest-pinned evidence, so its digest is never
//! recomputed. Compaction moves the bulky feature matrix into one engine-owned
//! Blob CAS body keyed by its content digest; the record keeps its original
//! digest and its matrix field becomes a blob pin, and the entry records
//! `Compacted`. Verification restores the matrix from CAS, re-checks the
//! record digest and replays the decision. Past `drop_blob_after_ms` the blob
//! holder is released and verification answers `INPUTS_RETIRED` -- the digest
//! is still the attested one, but nothing can be re-derived. Summaries (the
//! record minus its matrix) are kept for ever.

use eg_types::decision::statistical::body::{canonical_body_bytes, content_digest_of};
use eg_types::decision::statistical::log::{
    DecisionLogCompacted, DecisionLogEntry, DecisionLogVerification, EntryInputs, InputsBlob,
    MAX_COMPACT_PER_CALL,
};
use eg_types::decision::statistical::{
    FeatureMatrixRef, StatisticalDecisionRecord, StatisticalErrorCode,
};
use eg_types::decision::{DecisionPolicyRef, StatisticalPolicy};

use super::stat_executor::ExecutionContext;
use super::stat_log::{visible_entry, LogReader, MAX_LOG_ROWS};
use super::stat_replay::replay;
use super::stat_support::{refusal, resolve_policy};
use crate::server::persistence::decision_jobs::{decode_artifact, encode_artifact};

/// The blob store and the authority its holder changes are made under.
pub(super) struct Retention {
    #[cfg(feature = "blob")]
    pub(super) blob: Option<std::sync::Arc<crate::server::blob::BlobCursors>>,
    #[cfg(feature = "blob")]
    pub(super) authority: Option<crate::server::access::CarrierAuthority>,
}

impl Retention {
    /// No blob access: compacted inputs cannot be restored through it.
    pub(super) fn none() -> Self {
        Self {
            #[cfg(feature = "blob")]
            blob: None,
            #[cfg(feature = "blob")]
            authority: None,
        }
    }

    /// The served blob store and the caller's carrier authority.
    pub(super) fn of(
        state: &crate::server::state::ServerState,
        verified: &crate::server::auth::VerifiedRequestContext,
    ) -> Self {
        #[cfg(not(feature = "blob"))]
        let _ = (state, verified);
        Self {
            #[cfg(feature = "blob")]
            blob: state.blob.clone(),
            #[cfg(feature = "blob")]
            authority: crate::server::access::CarrierAuthority::from_verified(verified).ok(),
        }
    }
}

/// A logged record with its full inputs: inline as stored, restored from CAS
/// when compacted, or `None` when retention retired them.
pub(super) fn full_record(
    retention: &Retention,
    tenant_id: &str,
    entry: &DecisionLogEntry,
) -> Result<Option<StatisticalDecisionRecord>, String> {
    match &entry.inputs {
        EntryInputs::Inline => Ok(Some(entry.record.as_ref().clone())),
        EntryInputs::Compacted { blob, .. } => {
            restored(retention, tenant_id, &entry.record, blob).map(Some)
        }
        EntryInputs::Retired { .. } => Ok(None),
    }
}

fn unavailable() -> String {
    refusal(
        StatisticalErrorCode::ParameterInvalid,
        "decision-log retention needs the engine's Blob CAS",
    )
}

#[cfg(feature = "blob")]
fn store_inputs(
    retention: &Retention,
    tenant_id: &str,
    body: Vec<u8>,
    now_ms: u64,
) -> Result<InputsBlob, String> {
    use sha2::{Digest, Sha256};
    let blob = retention.blob.as_ref().ok_or_else(unavailable)?;
    let sha256 = eg_types::contract::Digest256::from_bytes(Sha256::digest(&body).into());
    let engine_body = crate::server::blob::engine_bodies::EngineBody { sha256, body };
    let stored = blob
        .store
        .put_engine_bodies(tenant_id, &[engine_body], now_ms)?;
    let stored = stored.into_iter().next().ok_or_else(unavailable)?;
    Ok(InputsBlob {
        sha256: format!("sha256:{}", stored.sha256.to_hex()),
        manifest_digest: stored.manifest_digest,
        holder_id: stored.holder_id,
        length: stored.length,
    })
}

#[cfg(feature = "blob")]
fn load_inputs(
    retention: &Retention,
    tenant_id: &str,
    pin: &InputsBlob,
) -> Result<Vec<u8>, String> {
    let blob = retention.blob.as_ref().ok_or_else(unavailable)?;
    let sha256 = eg_types::contract::Digest256::parse(pin.sha256.trim_start_matches("sha256:"))?;
    crate::server::blob::engine_bodies::read_engine_body(
        blob.store.as_ref(),
        tenant_id,
        &pin.manifest_digest,
        sha256,
        pin.length,
    )
}

#[cfg(feature = "blob")]
fn release_inputs(retention: &Retention, pin: &InputsBlob, request_id: u64) -> Result<(), String> {
    use crate::server::blob::store::{HolderChange, HolderId};
    let (Some(blob), Some(authority)) = (retention.blob.as_ref(), retention.authority.as_ref())
    else {
        return Err(unavailable());
    };
    let method = crate::protocol::Method::BlobUnref {
        digest: pin.manifest_digest.clone(),
    };
    let (batch, now) = crate::server::handlers::blob::compile_blob_batch(
        blob.store.as_ref(),
        request_id,
        authority,
        &method,
    )?;
    let change = HolderChange::release(&pin.manifest_digest, HolderId::new(&pin.holder_id)?)?;
    blob.store.holder_batch(&change, &batch, now).map(|_| ())
}

#[cfg(not(feature = "blob"))]
fn store_inputs(_: &Retention, _: &str, _: Vec<u8>, _: u64) -> Result<InputsBlob, String> {
    Err(unavailable())
}

#[cfg(not(feature = "blob"))]
fn load_inputs(_: &Retention, _: &str, _: &InputsBlob) -> Result<Vec<u8>, String> {
    Err(unavailable())
}

#[cfg(not(feature = "blob"))]
fn release_inputs(_: &Retention, _: &InputsBlob, _: u64) -> Result<(), String> {
    Err(unavailable())
}

/// Which retention step one entry is due for, if any.
enum Step {
    Compact,
    Retire(InputsBlob, u64),
}

fn due(entry: &DecisionLogEntry, policy: &StatisticalPolicy, now_ms: u64) -> Option<Step> {
    let inline = matches!(
        entry.record.inputs.feature_matrix,
        FeatureMatrixRef::Inline { .. }
    );
    match &entry.inputs {
        EntryInputs::Inline => {
            let age = now_ms.saturating_sub(entry.committed_at_ms);
            (inline && policy.compact_after_ms.is_some_and(|after| age >= after))
                .then_some(Step::Compact)
        }
        EntryInputs::Compacted {
            blob,
            compacted_at_ms,
        } => {
            let age = now_ms.saturating_sub(*compacted_at_ms);
            policy
                .drop_blob_after_ms
                .is_some_and(|after| age >= after)
                .then(|| Step::Retire(blob.clone(), *compacted_at_ms))
        }
        EntryInputs::Retired { .. } => None,
    }
}

fn matrix_pin(matrix: &FeatureMatrixRef, blob: &InputsBlob) -> FeatureMatrixRef {
    let (rows, columns) = match matrix {
        FeatureMatrixRef::Inline {
            candidate_ids,
            feature_names,
            ..
        } => (candidate_ids.len(), feature_names.len()),
        FeatureMatrixRef::Blob { rows, columns, .. } => (*rows as usize, *columns as usize),
    };
    FeatureMatrixRef::Blob {
        sha256: blob.sha256.clone(),
        length: blob.length,
        rows: rows as u32,
        columns: columns as u32,
    }
}

fn apply(
    ctx: &ExecutionContext,
    retention: &Retention,
    mut entry: DecisionLogEntry,
    step: Step,
) -> Result<DecisionLogEntry, String> {
    match step {
        Step::Compact => {
            let body = canonical_body_bytes(&entry.record.inputs.feature_matrix)?;
            let blob = store_inputs(retention, ctx.tenant_id, body, ctx.now_ms)?;
            entry.record.inputs.feature_matrix =
                matrix_pin(&entry.record.inputs.feature_matrix, &blob);
            entry.inputs = EntryInputs::Compacted {
                blob,
                compacted_at_ms: ctx.now_ms,
            };
        }
        Step::Retire(blob, compacted_at_ms) => {
            release_inputs(retention, &blob, ctx.now_ms)?;
            entry.inputs = EntryInputs::Retired {
                blob,
                compacted_at_ms,
                retired_at_ms: ctx.now_ms,
            };
        }
    }
    Ok(entry)
}

/// Apply the policy's retention to at most `limit` due entries of the tenant.
pub(super) fn compact(
    ctx: &ExecutionContext,
    retention: &Retention,
    policy: &DecisionPolicyRef,
    limit: u32,
) -> Result<DecisionLogCompacted, String> {
    let policy = resolve_policy(ctx.store, ctx.tenant_id, policy)?.statistical;
    let limit = limit.clamp(1, MAX_COMPACT_PER_CALL);
    let mut done = DecisionLogCompacted {
        compacted: 0,
        retired: 0,
        more: false,
    };
    for (key, bytes) in
        ctx.store
            .decision_artifacts_with_prefix(ctx.tenant_id, "record:", MAX_LOG_ROWS)?
    {
        let entry: DecisionLogEntry = decode_artifact(&bytes, "decision log entry")?;
        let Some(step) = due(&entry, &policy, ctx.now_ms) else {
            continue;
        };
        if done.compacted + done.retired == limit {
            done.more = true;
            break;
        }
        match step {
            Step::Compact => done.compacted += 1,
            Step::Retire(..) => done.retired += 1,
        }
        let updated = apply(ctx, retention, entry, step)?;
        ctx.store
            .replace_decision_artifact(ctx.tenant_id, &key, encode_artifact(&updated)?)?;
    }
    Ok(done)
}

fn restored(
    retention: &Retention,
    tenant_id: &str,
    record: &StatisticalDecisionRecord,
    pin: &InputsBlob,
) -> Result<StatisticalDecisionRecord, String> {
    let bytes = load_inputs(retention, tenant_id, pin)?;
    if content_digest_of(&bytes) != pin.sha256 {
        return Err(refusal(
            StatisticalErrorCode::DatasetInvalid,
            "the inputs blob does not match its pin",
        ));
    }
    let matrix: FeatureMatrixRef = serde_json::from_slice(&bytes)
        .map_err(|error| refusal(StatisticalErrorCode::DatasetInvalid, error))?;
    let mut full = record.clone();
    full.inputs.feature_matrix = matrix;
    Ok(full)
}

/// Re-verify one logged record against its inputs, wherever they now live.
pub(super) fn verify(
    ctx: &ExecutionContext,
    reader: &LogReader,
    retention: &Retention,
    record_id: &str,
) -> Result<DecisionLogVerification, String> {
    let entry = visible_entry(ctx.store, reader, record_id)?.ok_or_else(|| {
        refusal(
            StatisticalErrorCode::ParameterInvalid,
            "no committed record with that id is visible",
        )
    })?;
    let Some(record) = full_record(retention, ctx.tenant_id, &entry)? else {
        let EntryInputs::Retired { blob, .. } = &entry.inputs else {
            return Err(refusal(
                StatisticalErrorCode::ParameterInvalid,
                "inputs unavailable",
            ));
        };
        return Ok(DecisionLogVerification::InputsRetired {
            record_digest: entry.record.record_digest.clone(),
            blob_sha256: blob.sha256.clone(),
        });
    };
    replay(ctx, &record)?;
    Ok(DecisionLogVerification::Verified {
        record_digest: record.record_digest,
    })
}
