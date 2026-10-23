//! Shared owner-bound run admission and live-claim fixtures for private views.

use super::*;
use crate::redb_store::store_rows::GraphRowTables;
use eg_types::native_control::SubmitWorkItemRequest;

type Row = serde_json::Map<String, serde_json::Value>;

pub(in crate::redb_store::work_item) fn admission(
    metadata: serde_json::Value,
) -> SubmitWorkItemRequest {
    serde_json::from_value(serde_json::json!({
        "schema_version":"1", "run_id":"run", "work_item_id":null,
        "context": {"schema_version":"2", "request_id":"request", "subject_id":"owner",
            "tenant_id":"tenant", "agent_id":"worker", "scopes":["work:*"],
            "audience":"engine", "authentication_method":"local_process", "policy_version":"v1",
            "graph":GRAPH, "placement_epoch":null, "trace_id":"trace", "issued_at_ms":1,
            "expires_at_ms":2},
        "idempotency_key":"run-key", "command_digest":"A".repeat(64), "kind":"test",
        "priority":2, "depends_on":[], "input_ref":"input", "policy_digest":"policy",
        "catalog_digest":"catalog", "model_digest":"model", "max_attempts":3,
        "deadline_unix":null, "metadata":metadata, "provenance_refs":["source"],
        "max_tenant_in_flight":0
    }))
    .unwrap()
}

/// Open graph-row tables inside the same admitted transaction used by native
/// transitions. Errors skip finish/commit, preserving rollback coverage.
pub(in crate::redb_store::work_item) fn with_rows<R>(
    shard: &Shard,
    tag: &str,
    now: u64,
    write: impl FnOnce(&mut GraphRowTables<'_>) -> Result<R, String>,
) -> Result<R, String> {
    with_write(shard, tag, now, |admitted| {
        let member = admitted.graph(GRAPH)?;
        let mut rows = GraphRowTables::open(member)?;
        write(&mut rows)
    })
}

/// Seed the canonical owner-bound live lease, including its private native
/// claim record. Each view supplies only the additional row fields it needs.
pub(in crate::redb_store::work_item) fn seed_claim(
    shard: &Shard,
    customize: impl FnOnce(&mut Row),
) {
    let id = run_owner::work_item_id(GRAPH, "tenant", "run").unwrap();
    with_rows(shard, "seed", 1000, |rows| {
        let mut row = serde_json::json!({"node_type":"WorkItem", "tenant":"tenant", "status":"running", "lease_owner":"worker", "lease_epoch":1, "fencing_token":2, "lease_expires_at":10.0, "run_owner_binding":{"graph":GRAPH,"tenant":"tenant","run_id":"run","work_item_id":id,"principal":"owner"}}).as_object().unwrap().clone();
        customize(&mut row);
        work_item_capability::record_native_claim_in_wtx(
            &mut rows.native_work_items, GRAPH, &id, &row, 1000, DurableCrypto::none(),
        )?;
        write_work_item_props(&mut rows.nodes, GRAPH, &id, &mut row, DurableCrypto::none())
    }).unwrap();
}
