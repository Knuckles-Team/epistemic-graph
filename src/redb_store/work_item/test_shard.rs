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
