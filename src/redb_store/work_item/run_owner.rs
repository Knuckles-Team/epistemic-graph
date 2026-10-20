//! Immutable run ownership is admitted with the new WorkItem row.

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
    request: &eg_types::native_control::SubmitWorkItemRequest,
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

use super::{property_f64, property_string};
type Row = serde_json::Map<String, serde_json::Value>;

pub(super) fn is_owned(
    row: &Row,
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

pub(super) fn has_live_claim(row: &Row, worker: &str, epoch: u64, token: u64, now_ms: u64) -> bool {
    matches!(property_string(row, "status"), "leased" | "running")
        && row.get("lease_epoch").and_then(serde_json::Value::as_u64) == Some(epoch)
        && row.get("fencing_token").and_then(serde_json::Value::as_u64) == Some(token)
        && property_string(row, "lease_owner") == worker
        && !worker.is_empty()
        && property_f64(row, "lease_expires_at") > now_ms as f64 / 1000.0
}
