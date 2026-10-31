//! Shared owner-bound run admission and live-claim fixtures for private views.

use super::*;
use crate::redb_store::store_rows::GraphRowTables;
use medulla_types::native_control::SubmitWorkItemRequest;

type Row = serde_json::Map<String, serde_json::Value>;

pub(in crate::redb_store::work_item) fn admission(
    metadata: serde_json::Value,
) -> SubmitWorkItemRequest {
    let mut request = super::run_request();
    request.metadata = serde_json::from_value(metadata).unwrap();
    request
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
