//! Private current plans share the admitted WorkItem transaction and read snapshot.
use super::*;
use medulla_types::current_plan::{
    CurrentPlanPublished, CurrentPlanSnapshot, PublishCurrentPlanRequest,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
pub(crate) const CURRENT_PLANS: redb::TableDefinition<(&str, &str), &[u8]> =
    redb::TableDefinition::new("work_item_current_plans");

type Row = serde_json::Map<String, serde_json::Value>;

#[derive(Serialize, Deserialize)]
struct StoredPlan {
    snapshot: CurrentPlanSnapshot,
    worker_id: String,
    lease_epoch: u64,
    fencing_token: u64,
    payload_digest: String,
}

pub(super) fn live(row: &Row, worker: &str, epoch: u64, token: u64, now_ms: u64) -> bool {
    matches!(property_string(row, "status"), "leased" | "running")
        && row.get("lease_epoch").and_then(serde_json::Value::as_u64) == Some(epoch)
        && row.get("fencing_token").and_then(serde_json::Value::as_u64) == Some(token)
        && property_string(row, "lease_owner") == worker
        && !worker.is_empty()
        && property_f64(row, "lease_expires_at") > now_ms as f64 / 1000.0
}

pub(crate) fn publish(
    graph: &str,
    request: &PublishCurrentPlanRequest,
    actor: Option<&str>,
    tables: &mut crate::redb_store::store_rows::GraphRowTables<'_>,
    crypto: DurableCrypto<'_>,
    now_ms: u64,
) -> Result<CurrentPlanPublished, String> {
    let nodes = &tables.nodes;
    let plans = &mut tables.current_plans;
    let native_work_items = &tables.native_work_items;
    request.validate()?;
    let id = run_owner::work_item_id(graph, &request.tenant, &request.snapshot.run_id)?;
    let bytes = nodes
        .get((graph, id.as_str()))?
        .ok_or("current-plan owner unavailable")?;
    let row: Row = decode_durable(&crypto.unseal(bytes.value())?)?;
    drop(bytes);
    if !run_owner::binding_matches(
        &row,
        graph,
        &request.tenant,
        &request.snapshot.run_id,
        &id,
        actor.unwrap_or_default(),
    ) {
        return Err("ACCESS_DENIED: current-plan owner unavailable".into());
    }
    work_item_capability::require_native_claim(native_work_items, graph, &id, crypto)?;
    let previous = plans
        .get((graph, id.as_str()))?
        .map(|bytes| decode_durable::<StoredPlan>(&crypto.unseal(bytes.value())?))
        .transpose()?;
    validate_publication(&row, previous.as_ref(), request, now_ms)?;
    // Scaffold-only publishers cannot erase an already bound executable plan.
    // Bind the effective snapshot in the publication receipt, while the native
    // MutationBatch separately binds the exact submitted request for replay.
    let mut resolved = request.clone();
    if resolved.snapshot.task_plan.is_none() {
        resolved.snapshot.task_plan = previous
            .as_ref()
            .and_then(|old| old.snapshot.task_plan.clone());
    }
    let digest = hex::encode(Sha256::digest(
        serde_json::to_vec(&resolved).map_err(|e| e.to_string())?,
    ));
    let stored = StoredPlan {
        snapshot: resolved.snapshot,
        worker_id: request.worker_id.clone(),
        lease_epoch: request.lease_epoch,
        fencing_token: request.fencing_token,
        payload_digest: digest.clone(),
    };
    let bytes = rmp_serde::to_vec_named(&stored).map_err(|e| e.to_string())?;
    let sealed = crypto.seal(&bytes);
    plans.insert((graph, id.as_str()), sealed.as_ref())?;
    Ok(CurrentPlanPublished {
        run_id: request.snapshot.run_id.clone(),
        revision: request.snapshot.revision,
        payload_digest: digest,
        changed_work_item_ids: vec![],
    })
}

fn validate_publication(
    row: &Row,
    previous: Option<&StoredPlan>,
    request: &PublishCurrentPlanRequest,
    now_ms: u64,
) -> Result<(), String> {
    let snapshot = &request.snapshot;
    if !live(
        row,
        &request.worker_id,
        request.lease_epoch,
        request.fencing_token,
        now_ms,
    ) || snapshot.observed_at_ms > now_ms
        || snapshot.expires_at_ms <= now_ms
        || snapshot.expires_at_ms as f64 / 1000.0 > property_f64(row, "lease_expires_at")
    {
        return Err("current-plan fence or validity conflict".into());
    }
    if let (Some(bound), Some(proposed)) = (
        previous.and_then(|old| old.snapshot.task_plan.as_ref()),
        snapshot.task_plan.as_ref(),
    ) {
        // Equality includes input_digest: declaration reordering cannot replace
        // immutable provenance even if the executable plan_digest is unchanged.
        if bound != proposed {
            return Err("current-plan executable attachment is immutable".into());
        }
    }
    match previous {
        None if snapshot.revision == 0 && snapshot.items.is_empty() => Ok(()),
        Some(old) if snapshot.revision > old.snapshot.revision => Ok(()),
        Some(_) => Err("current-plan revision must increase".into()),
        None => Err("initial current plan must be empty revision zero".into()),
    }
}

/// Owner-only preflight, including before a historical receipt is disclosed.
/// The write repeats this check atomically; the immutable binding cannot change.
pub(crate) fn is_owner(
    shard: &Shard,
    graph: &str,
    tenant: &str,
    run_id: &str,
    actor: &str,
    crypto: DurableCrypto<'_>,
) -> Result<bool, String> {
    let id = run_owner::work_item_id(graph, tenant, run_id)?;
    let (_, row) = read::snapshot_row(shard, graph, &id, crypto)?;
    Ok(row.is_some_and(|row| run_owner::binding_matches(&row, graph, tenant, run_id, &id, actor)))
}

/// Owner, snapshot and lease are decoded from one authoritative MVCC row.
pub(crate) fn read_current(
    shard: &Shard,
    graph: &str,
    tenant: &str,
    run_id: &str,
    actor: &str,
    crypto: DurableCrypto<'_>,
    now_ms: u64,
) -> Result<Option<CurrentPlanSnapshot>, String> {
    let id = run_owner::work_item_id(graph, tenant, run_id)?;
    let handle = shard.graph(graph)?;
    let read = shard.read(&handle)?;
    let nodes = read.scoped_owner_table(NODES)?;
    let row = nodes
        .get((graph, id.as_str()))?
        .map(|value| decode_durable::<Row>(&crypto.unseal(value.value())?))
        .transpose()?;
    let Some(row) = row else {
        return Ok(None);
    };
    if !run_owner::binding_matches(&row, graph, tenant, run_id, &id, actor) {
        return Ok(None);
    }
    let plans = read.scoped_owner_table(CURRENT_PLANS)?;
    let Some(value) = plans.get((graph, id.as_str()))? else {
        return Ok(None);
    };
    let Ok(stored) = decode_durable::<StoredPlan>(&crypto.unseal(value.value())?) else {
        return Ok(None);
    };
    if stored.snapshot.run_id != run_id {
        return Ok(None);
    }
    if !live(
        &row,
        &stored.worker_id,
        stored.lease_epoch,
        stored.fencing_token,
        now_ms,
    ) || stored.snapshot.observed_at_ms > now_ms
        || stored.snapshot.expires_at_ms <= now_ms
    {
        return Ok(None);
    }
    Ok(Some(stored.snapshot))
}

#[cfg(test)]
mod tests;
