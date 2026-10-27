//! Terminal-only inline checkpoints share the WorkItem's native transaction.
//!
//! No generic transcript authority or second database is introduced. Every
//! retained byte and quota counter is in the same graph-owner-scoped native
//! private table. V1 refuses scope retirement while any profile is retained.
use super::*;
use eg_types::inline_history::InlineCheckpointRef;
use eg_types::native_control::SubmitWorkItemRequest;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

pub(crate) const INLINE_HISTORY: redb::TableDefinition<(&str, &str), &[u8]> =
    redb::TableDefinition::new("work_item_inline_history");

pub(crate) const POINTER: &str = "inline_checkpoint";
const MAX_RETAINED_BYTES: u64 = 64 * 1024 * 1024;
const MAX_RETAINED_RECORDS: u64 = 4096;
type Row = serde_json::Map<String, serde_json::Value>;

/// A constant-size, content-free marker makes even uncompleted admissions
/// visible to lifecycle guards without decrypting public WorkItem rows.
const PROFILE_PRESENT: &str = "inline-history-profile-present";

/// The first profile admission changes the graph lifecycle policy permanently
/// for this version. Only authenticated operator authority may make that choice.
pub(super) fn authorize_profile_presence(graph: &str, request: &SubmitWorkItemRequest,
    history: &NativeNodeRows<'_>, authorized: bool) -> Result<(), String> {
    if !request.conversation.as_ref().is_some_and(|turn| turn.history_profile.is_some()) {
        return Ok(());
    }
    match history.get((graph, PROFILE_PRESENT))? {
        Some(marker) if marker.value() == b"1" => Ok(()),
        Some(_) => Err("inline history policy marker is invalid".into()),
        None if authorized => Ok(()),
        None => Err("ACCESS_DENIED: inline history requires operator graph enablement".into()),
    }
}

pub(super) fn record_profile_presence(graph: &str, request: &SubmitWorkItemRequest,
    history: &mut NativeNodeRows<'_>) -> Result<(), String> {
    if request.conversation.as_ref().is_some_and(|turn| turn.history_profile.is_some()) {
        history.insert((graph, PROFILE_PRESENT), b"1".as_slice())?;
    }
    Ok(())
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct StoredCheckpoint {
    pub node_type: String,
    pub tenant: String,
    pub graph: String,
    pub owner: String,
    pub run_id: String,
    pub work_item_id: String,
    pub context_id: String,
    pub admission_revision: u64,
    pub checkpoint: InlineCheckpointRef,
    pub content: String,
    pub lease_epoch: u64,
    pub fencing_token: u64,
    pub batch_id: String,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Quota {
    node_type: String,
    tenant: String,
    bytes: u64,
    records: u64,
}

fn quota_id(tenant: &str) -> String {
    format!("inline-history-quota:{}", hex::encode(Sha256::digest(tenant.as_bytes())))
}

fn charge(quota: &mut Quota, bytes: usize) -> Result<(), String> {
    let next_bytes = quota.bytes.checked_add(bytes as u64)
        .filter(|n| *n <= MAX_RETAINED_BYTES)
        .ok_or("INLINE_HISTORY_CAPACITY: retained bytes exhausted")?;
    let next_records = quota.records.checked_add(1)
        .filter(|n| *n <= MAX_RETAINED_RECORDS)
        .ok_or("INLINE_HISTORY_CAPACITY: retained revisions exhausted")?;
    quota.bytes = next_bytes;
    quota.records = next_records;
    Ok(())
}

/// Called only after the existing terminal fence check, before status changes.
/// Any later error rolls these writes back with the rest of the MutationBatch.
pub(super) fn attach(
    input: &mut CommitWorkItemResultInput<'_, '_, '_>,
    props: &mut Row,
) -> Result<(), String> {
    let Some(stored) = prepare(input, props)? else { return Ok(()); };
    if props.contains_key(POINTER)
        || input.inline_history.get((input.graph, stored.checkpoint.document_id.as_str()))?.is_some()
    {
        return Err("INLINE_HISTORY_CONFLICT: checkpoint is immutable".into());
    }
    persist(input, &stored)?;
    props.insert(POINTER.into(), serde_json::to_value(stored.checkpoint).map_err(|e| e.to_string())?);
    Ok(())
}

fn prepare(input: &CommitWorkItemResultInput<'_, '_, '_>, props: &Row)
    -> Result<Option<StoredCheckpoint>, String>
{
    let admission = props.get(run_owner::ADMISSION_REQUEST)
        .cloned().map(serde_json::from_value::<SubmitWorkItemRequest>).transpose()
        .map_err(|_| "INLINE_HISTORY_UNAVAILABLE: original admission is invalid")?;
    let profile = admission.as_ref().and_then(|a| a.conversation.as_ref())
        .and_then(|c| c.history_profile.as_ref());
    let candidate = input.run_completion.and_then(|c| c.completed_checkpoint.as_ref());
    if profile.is_none() && candidate.is_none() { return Ok(None); }
    let admission = admission.as_ref()
        .ok_or("INLINE_HISTORY_UNAVAILABLE: original admission is absent")?;
    let turn = admission.conversation.as_ref()
        .ok_or("INLINE_HISTORY_UNAVAILABLE: ordered admission is absent")?;
    let profile = profile.ok_or("INLINE_HISTORY_UNSUPPORTED: admission has no text profile")?;
    let owner = admitted_owner(input, props, admission)?;
    if input.outcome != "succeeded" {
        return if candidate.is_some() {
            Err("INLINE_HISTORY_INVALID: non-success cannot commit history".into())
        } else { Ok(None) };
    }
    let completion = input.run_completion
        .ok_or("INLINE_HISTORY_UNAVAILABLE: success requires a completion receipt")?;
    let candidate = candidate
        .ok_or("INLINE_HISTORY_UNAVAILABLE: success requires completed own-turn text")?;
    candidate.validate(&completion.run_id)?;
    validate_binding(input, props, admission, profile, candidate)?;
    Ok(Some(StoredCheckpoint {
        node_type: "InlineRunCheckpoint".into(), tenant: input.tenant.into(), graph: input.graph.into(),
        owner: owner.into(), run_id: completion.run_id.clone(), work_item_id: input.work_item_id.into(),
        context_id: turn.context_id.clone(), admission_revision: turn.expected_revision.checked_add(1)
            .ok_or("INLINE_HISTORY_INVALID: admission revision overflow")?,
        checkpoint: candidate.checkpoint_ref(input.work_item_id), content: candidate.content(&completion.run_id)?,
        lease_epoch: input.lease_epoch, fencing_token: input.fencing_token, batch_id: input.batch_id.into(),
    }))
}


fn admitted_owner<'a>(input: &CommitWorkItemResultInput<'_, '_, '_>, props: &'a Row,
    admission: &SubmitWorkItemRequest) -> Result<&'a str, String> {
    let owner = props.get("run_owner_binding").and_then(|v| v.get("principal"))
        .and_then(|v| v.as_str()).ok_or("INLINE_HISTORY_UNAVAILABLE: owner is absent")?;
    let run = admission.run_id.as_deref().ok_or("INLINE_HISTORY_UNAVAILABLE: run is absent")?;
    if input.admitted_actor != Some(owner)
        || !run_owner::owned(props, input.graph, input.tenant, run, input.work_item_id, owner)
    {
        return Err("INLINE_HISTORY_UNAVAILABLE: immutable owner mismatch".into());
    }
    Ok(owner)
}

fn validate_binding(
    input: &CommitWorkItemResultInput<'_, '_, '_>, props: &Row,
    admission: &SubmitWorkItemRequest,
    profile: &eg_types::inline_history::CompletedTextAdmission,
    candidate: &eg_types::inline_history::CompletedTextInput,
) -> Result<(), String> {
    let run = input.run_completion.map(|value| value.run_id.as_str());
    if profile.version != 1
        || profile.user_text_digest != eg_types::inline_history::content_digest(&candidate.user_text)
        || admission.run_id.as_deref() != run
        || admission.context.tenant_id != input.tenant
        || sanitize(&admission.context.graph) != input.graph
        || property_u64(props, "attempt") != 1
        || property_f64(props, "lease_expires_at") <= input.now_ms as f64 / 1000.0
        || input.retryable
    {
        return Err("INLINE_HISTORY_INVALID: admission, own turn or first live fence mismatch".into());
    }
    Ok(())
}

fn persist(input: &mut CommitWorkItemResultInput<'_, '_, '_>, stored: &StoredCheckpoint)
    -> Result<(), String>
{
    let key = quota_id(input.tenant);
    let existing = input.inline_history.get((input.graph, key.as_str()))?
        .map(|v| decode_durable::<Quota>(&input.crypto.unseal(v.value())?)).transpose()?;
    let mut quota = existing.unwrap_or(Quota {
        node_type: "InlineHistoryQuota".into(), tenant: input.tenant.into(), bytes: 0, records: 0,
    });
    if quota.node_type != "InlineHistoryQuota" || quota.tenant != input.tenant {
        return Err("INLINE_HISTORY_UNAVAILABLE: quota identity mismatch".into());
    }
    charge(&mut quota, stored.content.len())?;
    let bytes = rmp_serde::to_vec_named(stored).map_err(|e| e.to_string())?;
    let sealed = input.crypto.seal(&bytes);
    input.inline_history.insert((input.graph, stored.checkpoint.document_id.as_str()), sealed.as_ref())?;
    let bytes = rmp_serde::to_vec_named(&quota).map_err(|e| e.to_string())?;
    let sealed = input.crypto.seal(&bytes);
    input.inline_history.insert((input.graph, key.as_str()), sealed.as_ref())?;
    Ok(())
}

/// The terminal-only profile has no snapshot import/export authority. Replacing
/// public rows must not strand retained private content or forge its pointers.
/// Refuse until a future reviewed replacement protocol preserves both atomically.
pub(crate) fn refuse_snapshot_replacement(
    write: &ShardWrite<'_>, graph: &str, incoming: &[(String, Vec<u8>)], crypto: DurableCrypto<'_>,
) -> Result<(), String> {
    let member = write.graph(graph)?;
    let history = member.open_scoped_table(INLINE_HISTORY)?;
    let nodes = member.open_scoped_table(NODES)?;
    refuse_profile_rows(&nodes, &history, crypto)?;
    for (_, bytes) in incoming {
        if let Ok(row) = decode_durable::<Row>(bytes) {
            refuse_profile_image(&row)?;
        }
    }
    Ok(())
}

/// Row-level clear/delete does not prove scope retirement and therefore cannot
/// reset profiled run IDs/lease counters. Actual GraphShardRetirement is separate.
pub(crate) fn guard_clear(method: &Method, nodes: &NativeNodeRows<'_>,
    history: &NativeNodeRows<'_>, crypto: DurableCrypto<'_>) -> Result<(), String> {
    if !matches!(method, Method::ClearGraph | Method::DeleteGraph { .. }) { return Ok(()); }
    refuse_profile_rows(nodes, history, crypto)
}

fn refuse_profile_rows(nodes: &NativeNodeRows<'_>, history: &NativeNodeRows<'_>,
    crypto: DurableCrypto<'_>) -> Result<(), String> {
    if history.scope_rows()?.next().transpose()?.is_some() {
        return Err("inline history destructive replacement is unsupported".into());
    }
    // Pending, failed and cancelled runs also retain admitted identities.
    for entry in nodes.scope_rows()? {
        let (_, value) = entry?;
        if let Ok(row) = decode_durable::<Row>(&crypto.unseal(value.value())?) {
            refuse_profile_image(&row)?;
        }
    }
    Ok(())
}

fn refuse_profile_image(row: &Row) -> Result<(), String> {
    let profile = row.get(run_owner::ADMISSION_REQUEST)
        .and_then(|value| value.get("conversation"))
        .and_then(|value| value.get("history_profile"));
    if row.contains_key(POINTER) || profile.is_some_and(|value| !value.is_null()) {
        return Err("inline history destructive replacement is unsupported".into());
    }
    Ok(())
}

#[cfg(test)]
mod tests;
