//! Immutable run ownership is admitted with the new WorkItem row.

use super::*;

pub(super) use medulla_types::work_item_read::RUN_ADMISSION_REQUEST as ADMISSION_REQUEST;

pub(super) fn work_item_id(graph: &str, tenant: &str, run_id: &str) -> Result<String, String> {
    use sha2::{Digest, Sha256};
    if run_id.trim().is_empty() || run_id.len() > 512 {
        return Err("run admission requires a bounded nonempty run_id".into());
    }
    let mut digest = Sha256::new();
    for field in ["run-admission", graph, tenant, run_id] {
        digest.update((field.len() as u64).to_be_bytes());
        digest.update(field.as_bytes());
    }
    Ok(format!("work-item:{}", hex::encode(digest.finalize())))
}

pub(super) fn admit_owner(
    graph: &str,
    request: &medulla_types::native_control::SubmitWorkItemRequest,
    actor: Option<&str>,
) -> Result<Option<serde_json::Value>, String> {
    let Some(run_id) = request.run_id.as_deref() else {
        return Ok(None);
    };
    if request.work_item_id.is_some() {
        return Err("run admission cannot attach a caller-selected WorkItem".into());
    }
    let work_item_id = work_item_id(graph, &request.context.tenant_id, run_id)?;
    let actor = actor
        .filter(|actor| !actor.is_empty())
        .ok_or("ACCESS_DENIED: run admission requires verified caller attribution")?;
    Ok(Some(
        serde_json::json!({"graph": graph, "tenant": request.context.tenant_id,
        "run_id": run_id, "work_item_id": work_item_id, "principal": actor}),
    ))
}

pub(super) fn binding_matches(
    row: &serde_json::Map<String, serde_json::Value>,
    graph: &str,
    tenant: &str,
    run_id: &str,
    id: &str,
    actor: &str,
) -> bool {
    row.get("run_owner_binding")
        == Some(&serde_json::json!({
            "graph": graph, "tenant": tenant, "run_id": run_id, "work_item_id": id, "principal": actor
        }))
        && property_string(row, "tenant") == tenant
        && !actor.is_empty()
}

pub(super) fn owned(
    row: &serde_json::Map<String, serde_json::Value>,
    graph: &str,
    tenant: &str,
    run_id: &str,
    id: &str,
    actor: &str,
) -> bool {
    binding_matches(row, graph, tenant, run_id, id, actor)
        && property_string(row, "node_type") == "WorkItem"
}

/// Historical owner-bound evidence, independent of current execution authority.
pub(crate) fn read_admission(
    shard: &Shard,
    graph: &str,
    tenant: &str,
    run_id: &str,
    actor: &str,
    crypto: DurableCrypto<'_>,
) -> Result<Option<medulla_types::work_item_read::RunWorkItemView>, String> {
    use medulla_types::native_control::SubmitWorkItemRequest;
    use medulla_types::work_item_read::{validate_work_item_get, RunWorkItemView, WorkItemView};
    validate_work_item_get(tenant, run_id)?;
    let id = work_item_id(graph, tenant, run_id)?;
    let handle = shard.graph(graph)?;
    let snapshot = shard.read(&handle)?;
    let nodes = snapshot.scoped_owner_table(NODES)?;
    let row = nodes
        .get((graph, id.as_str()))?
        .map(|value| {
            crypto.unseal(value.value()).and_then(|bytes| {
                decode_durable::<serde_json::Map<String, serde_json::Value>>(&bytes)
            })
        })
        .transpose()?;
    let Some(row) = row.filter(|row| owned(row, graph, tenant, run_id, &id, actor)) else {
        return Ok(None);
    };
    let unavailable = format!(
        "{}: immutable original submission is unavailable",
        medulla_types::contract::ServerErrorCode::RunAdmissionUnavailable
    );
    let unavailable = unavailable.as_str();
    let admission: SubmitWorkItemRequest =
        serde_json::from_value(row.get(ADMISSION_REQUEST).cloned().ok_or(unavailable)?)
            .map_err(|_| unavailable)?;
    if admission.run_id.as_deref() != Some(run_id)
        || admission.context.tenant_id != tenant
        || sanitize(&admission.context.graph) != graph
        || admission.work_item_id.is_some()
    {
        return Err(unavailable.into());
    }
    let work_item = WorkItemView::from_tenant_row(&id, &row, tenant)?.ok_or(unavailable)?;
    let run_completion = completion_from_snapshot(&snapshot, &row, tenant, run_id, &id)?;
    Ok(Some(RunWorkItemView {
        run_completion,
        work_item,
        admission,
    }))
}

#[cfg(test)]
mod tests;

/// A completion may only bind the original native admitted run.
pub(super) fn validate_completion_owner(
    row: &serde_json::Map<String, serde_json::Value>,
    graph: &str,
    tenant: &str,
    run: &str,
    id: &str,
) -> Result<(), String> {
    let actor = row
        .get("run_owner_binding")
        .and_then(|v| v.get("principal"))
        .and_then(|v| v.as_str())
        .ok_or("missing immutable run owner")?;
    if !owned(row, graph, tenant, run, id, actor) || work_item_id(graph, tenant, run)? != id {
        return Err("run completion owner mismatch".into());
    }
    let admission: medulla_types::native_control::SubmitWorkItemRequest = serde_json::from_value(
        row.get(ADMISSION_REQUEST)
            .cloned()
            .ok_or("missing run admission")?,
    )
    .map_err(|_| "invalid run admission")?;
    if admission.run_id.as_deref() != Some(run)
        || admission.context.tenant_id != tenant
        || sanitize(&admission.context.graph) != graph
        || admission.work_item_id.is_some()
    {
        return Err("run completion admission mismatch".into());
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn project_completion(
    scope: &medulla_types::MutationScopeIdentity,
    tenant: &str,
    run: &str,
    id: &str,
    status: &str,
    batch: &str,
    digest: &str,
    receipt: &medulla_types::MutationBatchRecord,
    records: &[medulla_types::MutationOutboxRecord],
) -> Result<medulla_types::run_completion::CommittedRunCompletion, String> {
    use medulla_types::run_completion::*;
    let mut matching = records
        .iter()
        .filter(|r| r.intent.topic == RUN_COMPLETION_TOPIC);
    let event = matching.next().ok_or("missing committed run completion")?;
    if matching.next().is_some() {
        return Err("run completion ledger/outbox binding mismatch".into());
    }
    validate_completion_event(scope, batch, digest, receipt, event)?;
    let payload: RunCompletionPayload = rmp_serde::from_slice(&event.intent.payload)
        .map_err(|_| "malformed run completion payload")?;
    payload.completion.validate()?;
    validate_completion_payload(&payload, tenant, run, id, status, batch)?;
    let sequence = event
        .commit_sequence
        .filter(|n| *n > 0)
        .ok_or("run completion sequence unavailable")?;
    if event
        .committed_version
        .target()
        .is_some_and(|n| n != sequence)
    {
        return Err("run completion sequence mismatch".into());
    }
    Ok(CommittedRunCompletion {
        payload,
        scope: scope.clone(),
        commit_sequence: sequence,
        ordinal: event.ordinal,
        payload_digest: digest.into(),
    })
}

fn validate_completion_payload(
    payload: &medulla_types::run_completion::RunCompletionPayload,
    tenant: &str,
    run: &str,
    id: &str,
    status: &str,
    batch: &str,
) -> Result<(), String> {
    if payload.tenant != tenant
        || payload.completion.run_id != run
        || payload.work_item_id != id
        || payload.batch_id != batch
        || payload.outcome != status
        || !matches!(status, "succeeded" | "failed" | "cancelled")
        || (status != "succeeded" && payload.result_ref.is_some())
    {
        return Err("run completion task/status mismatch".into());
    }
    Ok(())
}

fn validate_completion_event(
    scope: &medulla_types::MutationScopeIdentity,
    batch: &str,
    digest: &str,
    receipt: &medulla_types::MutationBatchRecord,
    event: &medulla_types::MutationOutboxRecord,
) -> Result<(), String> {
    use sha2::{Digest, Sha256};
    if event.schema_version != medulla_types::mutation_batch::MUTATION_BATCH_VERSION
        || &event.identity != scope
        || event.batch_id != batch
        || event.intent.key != batch
        || hex::encode(Sha256::digest(&event.intent.payload)) != digest
    {
        return Err("run completion ledger/outbox binding mismatch".into());
    }
    validate_completion_ledger(scope, batch, receipt, event)
}

fn validate_completion_ledger(
    scope: &medulla_types::MutationScopeIdentity,
    batch: &str,
    receipt: &medulla_types::MutationBatchRecord,
    event: &medulla_types::MutationOutboxRecord,
) -> Result<(), String> {
    if receipt.status != medulla_types::MutationBatchStatus::Committed
        || &receipt.identity != scope
        || receipt.batch.batch_id != batch
        || receipt.batch.outbox.get(event.ordinal as usize) != Some(&event.intent)
        || receipt.committed_version != event.committed_version
    {
        return Err("run completion ledger/outbox binding mismatch".into());
    }
    Ok(())
}

/// Project a row binding through the same immutable ledger and outbox snapshot.
pub(super) fn completion_from_snapshot(
    snapshot: &medulla_storage::ScopedRead<'_, medulla_storage::GraphShardOwner>,
    row: &serde_json::Map<String, serde_json::Value>,
    tenant: &str,
    run_id: &str,
    id: &str,
) -> Result<Option<medulla_types::run_completion::CommittedRunCompletion>, String> {
    Ok(
        match (
            row.get(medulla_types::run_completion::RUN_COMPLETION_BATCH),
            row.get(medulla_types::run_completion::RUN_COMPLETION_DIGEST),
        ) {
            (None, None) => None,
            (Some(batch), Some(digest)) => {
                let batch = batch.as_str().ok_or("invalid run completion batch")?;
                let digest = digest.as_str().ok_or("invalid run completion digest")?;
                let receipt = medulla_transaction::read_ledger(snapshot, batch)?
                    .ok_or("missing run completion receipt")?;
                let records = medulla_transaction::read_outbox(snapshot, batch)?;
                Some(project_completion(
                    snapshot.scope(),
                    tenant,
                    run_id,
                    id,
                    property_string(row, "status"),
                    batch,
                    digest,
                    &receipt,
                    &records,
                )?)
            }
            _ => return Err("incomplete run completion binding".into()),
        },
    )
}
