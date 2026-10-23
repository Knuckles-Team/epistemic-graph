//! A throwaway graph shard for WorkItem-row store tests: rows are written in
//! one admitted maintenance transaction through the SAME scoped node table the
//! native transitions use.

use super::*;

pub(super) const GRAPH: &str = "graph-a";

pub(super) struct TempShard {
    path: std::path::PathBuf,
    pub(super) shard: Shard,
}

impl Drop for TempShard {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

pub(super) fn open(tag: &str) -> TempShard {
    let path = crate::redb_store::temp_path("eg-work-item-rows", tag);
    let shard = Shard::open(&path).expect("temp shard opens");
    TempShard { path, shard }
}

pub(super) fn with_write<R>(
    shard: &Shard,
    op_id: &str,
    now_ms: u64,
    write: impl FnOnce(&ShardWrite<'_>) -> Result<R, String>,
) -> Result<R, String> {
    let members = shard.graph_members(&[GRAPH]).unwrap();
    let (group, batches) = shard.admit_maintenance(&members, op_id).unwrap();
    let admitted = ShardWrite::open(shard, &group, &members, &batches).unwrap();
    let outcome = write(&admitted)?;
    admitted.finish().unwrap();
    shard.commit_drain(group, &batches, now_ms).unwrap();
    Ok(outcome)
}

/// Run `write` against `GRAPH`'s node table in one committed transaction.
pub(super) fn with_nodes<R>(
    shard: &Shard,
    tag: &str,
    write: impl FnOnce(
        &mut ScopedOwnerTableMut<'_, (&'static str, &'static str), &'static [u8]>,
    ) -> Result<R, String>,
) -> R {
    let op_id = format!("work-item-rows-test/{tag}");
    with_write(shard, &op_id, 0, |admitted| {
        let member = admitted.graph(GRAPH).unwrap();
        let mut nodes = member.open_scoped_table(NODES).unwrap();
        write(&mut nodes)
    })
    .unwrap()
}

pub(super) fn check_guard(shard: &Shard, tag: &str, method: Method) -> Result<(), String> {
    with_nodes(shard, tag, |nodes| {
        Ok(super::row_guard::refuse_generic_native_row_write(
            GRAPH,
            &method,
            nodes,
            DurableCrypto::none(),
        ))
    })
}

/// Apply a native WorkItem method atomically through the production row dispatch.
pub(super) fn apply(
    shard: &Shard,
    tag: &str,
    method: Method,
    now_ms: u64,
) -> Result<crate::protocol::ResultPayload, String> {
    let op_id = format!("work-item-test/{tag}");
    with_write(shard, &op_id, now_ms, |admitted| {
        let member = admitted.graph(GRAPH).unwrap();
        let mut graph = crate::redb_store::store_rows::GraphRowTables::open(member).unwrap();
        let (mut holds, index, mut counters, mut pressure, policies) =
            crate::redb_store::store_state_tables::open_native_operation_lane_tables(
                admitted, GRAPH,
            )
            .unwrap();
        apply_work_item_rows(WorkItemApplyRequest {
            graph: GRAPH,
            batch_id: tag,
            method: &method,
            nodes: &mut graph.nodes,
            holds: &mut holds,
            work_item_index: &index,
            counters: &mut counters,
            pressure_index: &mut pressure,
            policies: &policies,
            native_work_items: &mut graph.native_work_items,
            edges: &mut graph.edges,
            command_sequences: &mut graph.command_sequences,
            committed_at_ms: now_ms,
            crypto: DurableCrypto::none(),
        })
        .map(|result| result.expect("a WorkItem-family write always answers"))
    })
}

/// Shared native run-admission fixture, with no conversation by default.
pub(super) fn run_request() -> eg_types::native_control::SubmitWorkItemRequest {
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
        "deadline_unix":null, "metadata":{"original":true}, "provenance_refs":["source"],
        "max_tenant_in_flight":0
    }))
    .unwrap()
}

/// Submit fixture rows within the caller's existing admitted transaction.
pub(super) fn submit_run_rows(
    admitted: &ShardWrite<'_>,
    request: &eg_types::native_control::SubmitWorkItemRequest,
    owner: &str,
) -> Result<eg_types::native_control::SubmitWorkItemResult, String> {
    let member = admitted.graph(GRAPH)?;
    let mut rows = crate::redb_store::store_rows::GraphRowTables::open(member)?;
    super::apply_submit_work_item_rows(
        GRAPH,
        request,
        &mut rows.nodes,
        &mut rows.edges,
        &mut rows.command_sequences,
        WorkItemCommitScope {
            crypto: DurableCrypto::none(),
            authoritative_now_ms: 1000,
            admitted_actor: Some(owner),
            outbox_id: "outbox",
        },
    )
}
