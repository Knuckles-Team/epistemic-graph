use super::*;

/// Execute a classified `Method::Sql` WRITE (CONCEPT:EG-KG.query.mirrors-pgwire). Mirrors the pgwire shim's
/// classify→execute write path, reusing the SAME engine primitives:
///   * graph-node DML (`INSERT`/`UPDATE`/`DELETE` on `nodes`) → the live `GraphCore`
///     write ops (`add_node` / `compare_and_set_fields` / `remove_node`) — the dispatch
///     shell then `mark_dirty`s the graph (this method classified Write) so the next
///     checkpoint persists it;
///   * user-table DDL/DML → the shared durable `TableStore`, where rows/catalog and
///     universal batch status/fence/idempotency/outbox share one commit-before-ack
///     redb transaction.
///
/// Blocking work (redb commits, the node scan) runs on the blocking pool via
/// `compute_off_lock`. Returns a `QueryResult`-shaped ack (`[tag]` column, one
/// rows-affected row) so the client decodes a write response exactly like a read.
/// The verified actor/scope fields for [`exec_sql_write`], bundled so the
/// function stays under the clippy argument-count ceiling.
#[cfg(feature = "query")]
#[derive(Clone, Copy)]
pub(crate) struct SqlWriteScope<'a> {
    pub(crate) graph_name: &'a str,
    pub(crate) tenant_scope: &'a str,
    pub(crate) caller: Option<&'a str>,
    pub(crate) authority: &'a crate::server::access::CarrierAuthority,
    pub(crate) persist_dir: &'a std::path::Path,
}

/// Execute `Method::Sql`'s `INSERT INTO nodes …` DML — pure extract-method out of
/// `exec_sql_write`'s `K::InsertNodes` arm, no behaviour change.
#[cfg(feature = "query")]
pub(crate) async fn exec_sql_write_insert_nodes(
    req_id: u64,
    core: &Arc<GraphCore>,
    read_core: &Arc<GraphCore>,
    ins: eg_query::InsertNodes,
) -> Response {
    let core = core.clone();
    let visible = read_core.clone();
    let r = compute_off_lock(req_id, move || {
        let mut n = 0usize;
        for node in ins.rows {
            if core.has_node(&node.node_id) && !visible.has_node(&node.node_id) {
                crate::metrics::access_denied();
                return Err(
                    "ACCESS_DENIED: node write is outside the visible row scope".to_string()
                );
            }
            let blob = rmp_serde::to_vec_named(&serde_json::Value::Object(node.properties))
                .map_err(|e| format!("encode node properties: {e}"))?;
            core.add_node(node.node_id, blob);
            n += 1;
        }
        Ok::<usize, String>(n)
    })
    .await;
    sql_write_ack(req_id, "INSERT", r)
}

/// Execute `Method::Sql`'s `UPDATE nodes …` DML — pure extract-method out of
/// `exec_sql_write`'s `K::UpdateNodes` arm, no behaviour change.
#[cfg(feature = "query")]
pub(crate) async fn exec_sql_write_update_nodes(
    req_id: u64,
    core: &Arc<GraphCore>,
    read_core: Arc<GraphCore>,
    upd: eg_query::UpdateNodes,
) -> Response {
    let core = core.clone();
    let r = compute_off_lock(req_id, move || {
        let ids = matched_node_ids(&read_core, &upd.selector);
        let conditions = serde_json::Map::new();
        // CONCEPT:EG-KG.query.compound-predicate-decode — re-check a compound predicate under the write guard.
        let pred = match &upd.selector {
            eg_query::WhereEq::Predicate { pred, .. } => Some(pred.clone()),
            eg_query::WhereEq::Id(_) => None,
        };
        let mut n = 0usize;
        for id in ids {
            let applied = match &pred {
                Some(p) => core.compare_and_set_fields_if(&id, p, &conditions, &upd.set),
                None => core.compare_and_set_fields(&id, &conditions, &upd.set),
            };
            if applied {
                n += 1;
            }
        }
        Ok::<usize, String>(n)
    })
    .await;
    sql_write_ack(req_id, "UPDATE", r)
}

/// Execute `Method::Sql`'s `DELETE FROM nodes …` DML — pure extract-method out of
/// `exec_sql_write`'s `K::DeleteNodes` arm, no behaviour change.
#[cfg(feature = "query")]
pub(crate) async fn exec_sql_write_delete_nodes(
    req_id: u64,
    core: &Arc<GraphCore>,
    read_core: Arc<GraphCore>,
    del: eg_query::DeleteNodes,
) -> Response {
    let core = core.clone();
    let r = compute_off_lock(req_id, move || {
        let ids = matched_node_ids(&read_core, &del.selector);
        // CONCEPT:EG-KG.query.compound-predicate-decode — re-check a compound predicate under the write guard.
        let pred = match &del.selector {
            eg_query::WhereEq::Predicate { pred, .. } => Some(pred.clone()),
            eg_query::WhereEq::Id(_) => None,
        };
        let mut n = 0usize;
        for id in ids {
            let removed = match &pred {
                Some(p) => core.remove_node_if(&id, p),
                None => {
                    core.remove_node(id);
                    true
                }
            };
            if removed {
                n += 1;
            }
        }
        Ok::<usize, String>(n)
    })
    .await;
    sql_write_ack(req_id, "DELETE", r)
}

/// Execute `Method::Sql`'s `INSERT … SELECT` DML (into a user table) — pure
/// extract-method out of `exec_sql_write`'s `K::InsertSelect` arm, no behaviour
/// change. The SELECT half runs through the SAME tables-aware DataFusion path (so
/// it can JOIN user tables AND the graph); its projected rows are then durably
/// inserted. Column COUNT must match the insert column list.
#[cfg(feature = "query")]
pub(crate) async fn exec_sql_write_insert_select(
    req_id: u64,
    scope: SqlWriteScope<'_>,
    sql_method: Method,
    read_core: Arc<GraphCore>,
    store: &eg_query::TableStore,
    read_store: &eg_query::TableStore,
    ins: eg_query::InsertSelect,
) -> Response {
    let eg_query::InsertSelect {
        table,
        columns,
        select_sql,
    } = ins;
    let read_store = read_store.clone();
    let snap = read_core.analysis_snapshot();
    let expected_columns = columns.len();
    let r = compute_off_lock(req_id, move || {
        let read = eg_query::exec_sql_typed_with_tables(&snap, &read_store, &select_sql)?;
        if read.columns.len() != expected_columns {
            return Err(format!(
                "INSERT … SELECT column count mismatch: {} target columns, {} selected",
                expected_columns,
                read.columns.len()
            ));
        }
        Ok::<_, String>(read.rows)
    })
    .await;
    let rows = match r {
        Ok(Ok(rows)) => rows,
        Ok(Err(error)) => return Response::err(req_id, format!("SQL error: {error}")),
        Err(response) => return response,
    };
    let mut txn = eg_query::TableTxn::new();
    txn.push(eg_query::TxnOp::Insert {
        table,
        col_order: columns,
        rows,
    });
    commit_sql_catalog_txn(req_id, scope, sql_method, store, txn, "INSERT").await
}

/// Execute `Method::Sql`'s `INSERT INTO nodes (id, …) SELECT …` DML — pure
/// extract-method out of `exec_sql_write`'s `K::InsertNodesSelect` arm, no
/// behaviour change.
#[cfg(feature = "query")]
/// Apply one resolved `INSERT INTO nodes … SELECT` row: build `id`/`props` from
/// the row, enforce the visible-row-scope `ACCESS_DENIED` guard, and honor
/// `ON CONFLICT`. Returns `1` if the row counted as written (a fresh insert or a
/// `DO UPDATE`), `0` if skipped (`DO NOTHING`). Pure extract-method out of
/// `exec_sql_write_insert_nodes_select`'s off-lock closure loop body, no
/// behaviour change.
#[cfg(feature = "query")]
pub(crate) fn apply_insert_nodes_select_row(
    core: &Arc<GraphCore>,
    visible: &Arc<GraphCore>,
    id_pos: usize,
    columns: &[String],
    on_conflict: Option<&eg_query::OnConflict>,
    row: Vec<serde_json::Value>,
) -> Result<usize, String> {
    let node_id = cell_to_node_id(&row[id_pos])?;
    let mut props = serde_json::Map::new();
    for (i, col) in columns.iter().enumerate() {
        if i != id_pos {
            props.insert(col.clone(), row[i].clone());
        }
    }
    if core.has_node(&node_id) && !visible.has_node(&node_id) {
        crate::metrics::access_denied();
        return Err("ACCESS_DENIED: node write is outside the visible row scope".to_string());
    }
    if visible.has_node(&node_id) {
        match on_conflict.map(|oc| &oc.action) {
            Some(eg_query::OnConflictAction::DoNothing) => return Ok(0),
            Some(eg_query::OnConflictAction::DoUpdate(set)) => {
                let empty = serde_json::Map::new();
                core.compare_and_set_fields(&node_id, &empty, set);
                return Ok(1);
            }
            None => {}
        }
    }
    let blob = rmp_serde::to_vec_named(&serde_json::Value::Object(props))
        .map_err(|e| format!("encode node properties: {e}"))?;
    core.add_node(node_id, blob);
    Ok(1)
}

/// Resolve the `SELECT` half and apply every row via [`apply_insert_nodes_select_row`] — pure extract-method out of
/// `exec_sql_write_insert_nodes_select`'s off-lock closure, no behaviour change.
#[cfg(feature = "query")]
pub(crate) fn resolve_insert_nodes_select(
    core: &Arc<GraphCore>,
    visible: &Arc<GraphCore>,
    snap: &crate::graph::GraphView,
    store: &eg_query::TableStore,
    ins: eg_query::InsertNodesSelect,
) -> Result<usize, String> {
    let read = eg_query::exec_sql_typed_with_tables(snap, store, &ins.select_sql)?;
    if read.columns.len() != ins.columns.len() {
        return Err(format!(
            "INSERT INTO nodes … SELECT column count mismatch: {} target columns, {} selected",
            ins.columns.len(),
            read.columns.len()
        ));
    }
    let id_pos = ins
        .columns
        .iter()
        .position(|c| c.eq_ignore_ascii_case("id"))
        .ok_or("INSERT INTO nodes … SELECT must include the `id` column")?;
    let mut n = 0usize;
    for row in read.rows {
        n += apply_insert_nodes_select_row(
            core,
            visible,
            id_pos,
            &ins.columns,
            ins.on_conflict.as_ref(),
            row,
        )?;
    }
    Ok(n)
}

#[cfg(feature = "query")]
pub(crate) async fn exec_sql_write_insert_nodes_select(
    req_id: u64,
    core: &Arc<GraphCore>,
    read_core: Arc<GraphCore>,
    store: &eg_query::TableStore,
    ins: eg_query::InsertNodesSelect,
) -> Response {
    let core = core.clone();
    let store = store.clone();
    let visible = read_core;
    let snap = visible.analysis_snapshot();
    let r = compute_off_lock(req_id, move || {
        resolve_insert_nodes_select(&core, &visible, &snap, &store, ins)
    })
    .await;
    sql_write_ack(req_id, "INSERT", r)
}

/// Execute `Method::Sql`'s `UPDATE nodes … FROM …` DML — pure extract-method out
/// of `exec_sql_write`'s `K::UpdateNodesJoin` arm, no behaviour change.
#[cfg(feature = "query")]
pub(crate) async fn exec_sql_write_update_nodes_join(
    req_id: u64,
    core: &Arc<GraphCore>,
    read_core: &Arc<GraphCore>,
    store: &eg_query::TableStore,
    upd: eg_query::UpdateNodesJoin,
) -> Response {
    let core = core.clone();
    let store = store.clone();
    let snap = read_core.analysis_snapshot();
    let r = compute_off_lock(req_id, move || {
        let read = eg_query::exec_sql_typed_with_tables(&snap, &store, &upd.resolve_sql)?;
        if read.columns.len() != upd.set_targets.len() + 1 {
            return Err(format!(
                "UPDATE … FROM resolution shape mismatch: expected id + {} set columns, got {}",
                upd.set_targets.len(),
                read.columns.len()
            ));
        }
        let empty = serde_json::Map::new();
        let mut seen = std::collections::HashSet::new();
        let mut n = 0usize;
        for row in read.rows {
            let id = cell_to_node_id(&row[0])?;
            if !seen.insert(id.clone()) {
                continue;
            }
            let mut updates = serde_json::Map::new();
            for (i, col) in upd.set_targets.iter().enumerate() {
                updates.insert(col.clone(), row[i + 1].clone());
            }
            if core.compare_and_set_fields(&id, &empty, &updates) {
                n += 1;
            }
        }
        Ok::<usize, String>(n)
    })
    .await;
    sql_write_ack(req_id, "UPDATE", r)
}

/// Execute `Method::Sql`'s `DELETE FROM nodes … USING …` DML — pure
/// extract-method out of `exec_sql_write`'s `K::DeleteNodesJoin` arm, no
/// behaviour change.
#[cfg(feature = "query")]
pub(crate) async fn exec_sql_write_delete_nodes_join(
    req_id: u64,
    core: &Arc<GraphCore>,
    read_core: &Arc<GraphCore>,
    store: &eg_query::TableStore,
    del: eg_query::DeleteNodesJoin,
) -> Response {
    let core = core.clone();
    let store = store.clone();
    let snap = read_core.analysis_snapshot();
    let r = compute_off_lock(req_id, move || {
        let read = eg_query::exec_sql_typed_with_tables(&snap, &store, &del.resolve_sql)?;
        let mut seen = std::collections::HashSet::new();
        let mut n = 0usize;
        for row in read.rows {
            let id = cell_to_node_id(&row[0])?;
            if !seen.insert(id.clone()) {
                continue;
            }
            core.remove_node(id);
            n += 1;
        }
        Ok::<usize, String>(n)
    })
    .await;
    sql_write_ack(req_id, "DELETE", r)
}

/// Execute `Method::Sql`'s `CREATE TABLE …` DDL — pure extract-method out of
/// `exec_sql_write`'s `K::CreateTable` arm, no behaviour change.
#[cfg(feature = "query")]
pub(crate) async fn exec_sql_write_create_table(
    req_id: u64,
    scope: SqlWriteScope<'_>,
    sql_method: Method,
    store: &eg_query::TableStore,
    plan: eg_query::CreateTablePlan,
) -> Response {
    let columns = match to_store_columns(&plan.columns) {
        Ok(columns) => columns,
        Err(error) => return Response::err(req_id, format!("SQL error: {error}")),
    };
    let mut txn = eg_query::TableTxn::new();
    txn.push(eg_query::TxnOp::CreateTable {
        schema: eg_query::TableSchema::new(plan.name, columns),
        if_not_exists: plan.if_not_exists,
    });
    commit_sql_catalog_txn(req_id, scope, sql_method, store, txn, "CREATE TABLE").await
}

/// Execute `Method::Sql`'s `ALTER TABLE …` DDL — pure extract-method out of
/// `exec_sql_write`'s `K::AlterTable` arm, no behaviour change.
#[cfg(feature = "query")]
pub(crate) async fn exec_sql_write_alter_table(
    req_id: u64,
    scope: SqlWriteScope<'_>,
    sql_method: Method,
    store: &eg_query::TableStore,
    plan: eg_query::AlterTablePlan,
) -> Response {
    let op = match alter_table_txn_op(plan) {
        Ok(op) => op,
        Err(error) => return Response::err(req_id, format!("SQL error: {error}")),
    };
    let mut txn = eg_query::TableTxn::new();
    txn.push(op);
    commit_sql_catalog_txn(req_id, scope, sql_method, store, txn, "ALTER TABLE").await
}
