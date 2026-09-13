use super::*;

#[cfg(feature = "query")]
struct SqlDispatchCtx<'a> {
    req_id: u64,
    scope: SqlWriteScope<'a>,
    read_authority: &'a GraphReadAuthority,
    sql_method: Method,
    core: &'a Arc<GraphCore>,
    store: &'a eg_query::TableStore,
    read_core: Arc<GraphCore>,
    read_store: &'a eg_query::TableStore,
}

#[cfg(feature = "query")]
fn sql_needs_source_read(kind: &eg_query::StatementKind) -> bool {
    use eg_query::StatementKind as K;
    matches!(
        kind,
        K::InsertSelect(_)
            | K::InsertNodesSelect(_)
            | K::UpdateNodesJoin(_)
            | K::DeleteNodesJoin(_)
            | K::GraphTableReadRequiresCatalogAdmission(_)
    )
}

#[cfg(feature = "query")]
async fn authorized_sql_read_store<'a>(
    req_id: u64,
    scope: SqlWriteScope<'a>,
    kind: &eg_query::StatementKind,
    fallback: &'a eg_query::TableStore,
) -> Result<
    (
        Option<crate::server::sql_catalog_acl::AuthorizedReadStore>,
        &'a eg_query::TableStore,
    ),
    Response,
> {
    if !sql_needs_source_read(kind) {
        return Ok((None, fallback));
    }
    let authority = scope.authority.clone();
    let persist_dir = scope.persist_dir.to_path_buf();
    let authorized = match compute_off_lock(req_id, move || {
        crate::server::sql_catalog_acl::authorized_read_store(&authority, &persist_dir)
    })
    .await
    {
        Ok(Ok(store)) => store,
        Ok(Err(error)) => return Err(Response::err(req_id, format!("SQL error: {error}"))),
        Err(response) => return Err(response),
    };
    let read_store = authorized.store();
    Ok((Some(authorized), read_store))
}

#[cfg(feature = "query")]
fn sql_is_graph_statement(kind: &eg_query::StatementKind) -> bool {
    use eg_query::StatementKind as K;
    matches!(
        kind,
        K::InsertNodes(_)
            | K::UpdateNodes(_)
            | K::DeleteNodes(_)
            | K::InsertNodesSelect(_)
            | K::UpdateNodesJoin(_)
            | K::DeleteNodesJoin(_)
    )
}

#[cfg(feature = "query")]
pub(crate) async fn exec_sql_write(
    req_id: u64,
    scope: SqlWriteScope<'_>,
    read_authority: &GraphReadAuthority,
    sql_method: Method,
    core: &Arc<GraphCore>,
    store: &eg_query::TableStore,
    kind: eg_query::StatementKind,
) -> Response {
    let read_core = read_authority.project_core(core);
    let (authorized, read_store) =
        match authorized_sql_read_store(req_id, scope, &kind, store).await {
            Ok(result) => result,
            Err(response) => return response,
        };
    let dispatch = SqlDispatchCtx {
        req_id,
        scope,
        read_authority,
        sql_method,
        core,
        store,
        read_core,
        read_store,
    };
    if sql_is_graph_statement(&kind) {
        exec_sql_graph_statement(dispatch, kind).await
    } else {
        exec_sql_catalog_statement(dispatch, kind).await
    }
}

#[cfg(feature = "query")]
async fn exec_sql_graph_statement(
    ctx: SqlDispatchCtx<'_>,
    kind: eg_query::StatementKind,
) -> Response {
    use eg_query::StatementKind as K;
    let SqlDispatchCtx {
        req_id,
        core,
        read_core,
        read_store,
        ..
    } = ctx;
    match kind {
        K::InsertNodes(ins) => exec_sql_write_insert_nodes(req_id, core, &read_core, ins).await,
        K::UpdateNodes(upd) => {
            exec_sql_write_update_nodes(req_id, core, read_core.clone(), upd).await
        }
        K::DeleteNodes(del) => {
            exec_sql_write_delete_nodes(req_id, core, read_core.clone(), del).await
        }
        K::InsertNodesSelect(ins) => {
            exec_sql_write_insert_nodes_select(req_id, core, read_core, read_store, ins).await
        }
        K::UpdateNodesJoin(upd) => {
            exec_sql_write_update_nodes_join(req_id, core, &read_core, read_store, upd).await
        }
        K::DeleteNodesJoin(del) => {
            exec_sql_write_delete_nodes_join(req_id, core, &read_core, read_store, del).await
        }
        _ => Response::err(req_id, "SQL error: unsupported graph write".to_string()),
    }
}

#[cfg(feature = "query")]
async fn commit_catalog_op(
    req_id: u64,
    scope: SqlWriteScope<'_>,
    sql_method: Method,
    store: &eg_query::TableStore,
    op: eg_query::TxnOp,
    tag: &'static str,
) -> Response {
    let mut txn = eg_query::TableTxn::new();
    txn.push(op);
    commit_sql_catalog_txn(req_id, scope, sql_method, store, txn, tag).await
}

#[cfg(feature = "query")]
async fn exec_sql_catalog_statement(
    ctx: SqlDispatchCtx<'_>,
    kind: eg_query::StatementKind,
) -> Response {
    if sql_catalog_table_kind(&kind) {
        return exec_sql_catalog_table(ctx, kind).await;
    }
    if sql_catalog_insert_kind(&kind) {
        return exec_sql_catalog_insert(ctx, kind).await;
    }
    if sql_catalog_row_kind(&kind) {
        return exec_sql_catalog_row(ctx, kind).await;
    }
    if sql_catalog_drop_kind(&kind) {
        return exec_sql_catalog_drop(ctx, kind).await;
    }
    if sql_catalog_create_kind(&kind) {
        return exec_sql_catalog_create(ctx, kind).await;
    }
    exec_sql_catalog_terminal(ctx, kind).await
}

#[cfg(feature = "query")]
fn sql_catalog_table_kind(kind: &eg_query::StatementKind) -> bool {
    use eg_query::StatementKind as K;
    matches!(kind, K::CreateTable(_) | K::AlterTable(_))
}

#[cfg(feature = "query")]
fn sql_catalog_insert_kind(kind: &eg_query::StatementKind) -> bool {
    use eg_query::StatementKind as K;
    matches!(kind, K::InsertTable(_) | K::InsertSelect(_))
}

#[cfg(feature = "query")]
fn sql_catalog_row_kind(kind: &eg_query::StatementKind) -> bool {
    use eg_query::StatementKind as K;
    matches!(kind, K::UpdateTable(_) | K::DeleteTable(_))
}

#[cfg(feature = "query")]
fn sql_catalog_drop_kind(kind: &eg_query::StatementKind) -> bool {
    use eg_query::StatementKind as K;
    matches!(
        kind,
        K::DropTable(_) | K::DropView(_) | K::DropExtension { .. } | K::DropFunction(_)
    )
}

#[cfg(feature = "query")]
fn sql_catalog_create_kind(kind: &eg_query::StatementKind) -> bool {
    use eg_query::StatementKind as K;
    matches!(
        kind,
        K::CreateView(_)
            | K::CreateExtension { .. }
            | K::CreateFunction(_)
            | K::CreateAnnIndex(_)
            | K::CreateHypertable(_)
            | K::CreateContinuousAggregate(_)
    )
}

#[cfg(feature = "query")]
async fn exec_sql_catalog_table(
    ctx: SqlDispatchCtx<'_>,
    kind: eg_query::StatementKind,
) -> Response {
    use eg_query::StatementKind as K;
    let SqlDispatchCtx {
        req_id,
        scope,
        sql_method,
        store,
        ..
    } = ctx;
    match kind {
        K::CreateTable(plan) => {
            exec_sql_write_create_table(req_id, scope, sql_method, store, plan).await
        }
        K::AlterTable(plan) => {
            exec_sql_write_alter_table(req_id, scope, sql_method, store, plan).await
        }
        _ => unreachable!("catalog table statement was classified before dispatch"),
    }
}

#[cfg(feature = "query")]
async fn exec_sql_catalog_insert(
    ctx: SqlDispatchCtx<'_>,
    kind: eg_query::StatementKind,
) -> Response {
    use eg_query::StatementKind as K;
    let SqlDispatchCtx {
        req_id,
        scope,
        sql_method,
        store,
        read_core,
        read_store,
        ..
    } = ctx;
    match kind {
        K::InsertTable(ins) => {
            commit_catalog_op(
                req_id,
                scope,
                sql_method,
                store,
                eg_query::TxnOp::Insert {
                    table: ins.table,
                    col_order: ins.columns,
                    rows: ins.rows,
                },
                "INSERT",
            )
            .await
        }
        K::InsertSelect(ins) => {
            exec_sql_write_insert_select(
                req_id, scope, sql_method, read_core, store, read_store, ins,
            )
            .await
        }
        _ => unreachable!("catalog insert statement was classified before dispatch"),
    }
}

#[cfg(feature = "query")]
async fn exec_sql_catalog_row(ctx: SqlDispatchCtx<'_>, kind: eg_query::StatementKind) -> Response {
    use eg_query::StatementKind as K;
    let SqlDispatchCtx {
        req_id,
        scope,
        sql_method,
        store,
        ..
    } = ctx;
    match kind {
        K::UpdateTable(upd) => {
            commit_catalog_op(
                req_id,
                scope,
                sql_method,
                store,
                eg_query::TxnOp::Update {
                    table: upd.table,
                    set: upd.set,
                    selector: upd.selector.pred,
                },
                "UPDATE",
            )
            .await
        }
        K::DeleteTable(del) => {
            commit_catalog_op(
                req_id,
                scope,
                sql_method,
                store,
                eg_query::TxnOp::Delete {
                    table: del.table,
                    selector: del.selector.pred,
                },
                "DELETE",
            )
            .await
        }
        _ => unreachable!("catalog row statement was classified before dispatch"),
    }
}

#[cfg(feature = "query")]
async fn exec_sql_catalog_drop(ctx: SqlDispatchCtx<'_>, kind: eg_query::StatementKind) -> Response {
    use eg_query::StatementKind as K;
    let SqlDispatchCtx {
        req_id,
        scope,
        sql_method,
        store,
        ..
    } = ctx;
    match kind {
        K::DropTable(plan) => {
            commit_catalog_op(
                req_id,
                scope,
                sql_method,
                store,
                eg_query::TxnOp::DropTable {
                    name: plan.name,
                    if_exists: plan.if_exists,
                },
                "DROP TABLE",
            )
            .await
        }
        K::DropView(plan) => {
            commit_catalog_op(
                req_id,
                scope,
                sql_method,
                store,
                eg_query::TxnOp::DropView {
                    name: plan.name,
                    if_exists: plan.if_exists,
                },
                "DROP VIEW",
            )
            .await
        }
        K::DropExtension { name, if_exists } => {
            commit_catalog_op(
                req_id,
                scope,
                sql_method,
                store,
                eg_query::TxnOp::DropExtension { name, if_exists },
                "DROP EXTENSION",
            )
            .await
        }
        K::DropFunction(plan) => {
            commit_catalog_op(
                req_id,
                scope,
                sql_method,
                store,
                eg_query::TxnOp::DropFunction {
                    name: plan.name,
                    if_exists: plan.if_exists,
                },
                "DROP FUNCTION",
            )
            .await
        }
        _ => unreachable!("catalog drop statement was classified before dispatch"),
    }
}

#[cfg(feature = "query")]
async fn exec_sql_catalog_create(
    ctx: SqlDispatchCtx<'_>,
    kind: eg_query::StatementKind,
) -> Response {
    use eg_query::StatementKind as K;
    let SqlDispatchCtx {
        req_id,
        scope,
        sql_method,
        store,
        ..
    } = ctx;
    match kind {
        K::CreateView(plan) => {
            commit_catalog_op(
                req_id,
                scope,
                sql_method,
                store,
                eg_query::TxnOp::CreateView {
                    name: plan.name,
                    select_sql: plan.select_sql,
                    or_replace: plan.or_replace,
                },
                "CREATE VIEW",
            )
            .await
        }
        K::CreateExtension {
            name,
            if_not_exists,
        } => {
            commit_catalog_op(
                req_id,
                scope,
                sql_method,
                store,
                eg_query::TxnOp::CreateExtension {
                    name,
                    if_not_exists,
                },
                "CREATE EXTENSION",
            )
            .await
        }
        K::CreateFunction(plan) => {
            commit_catalog_op(
                req_id,
                scope,
                sql_method,
                store,
                eg_query::TxnOp::CreateFunction {
                    function: plan.func,
                    or_replace: plan.or_replace,
                },
                "CREATE FUNCTION",
            )
            .await
        }
        K::CreateAnnIndex(plan) => {
            commit_catalog_op(
                req_id,
                scope,
                sql_method,
                store,
                eg_query::TxnOp::IndexCatalog(eg_query::IndexCatalogTxnOp::PutAnnIndex { plan }),
                "CREATE INDEX",
            )
            .await
        }
        K::CreateHypertable(plan) => {
            commit_catalog_op(
                req_id,
                scope,
                sql_method,
                store,
                eg_query::TxnOp::IndexCatalog(eg_query::IndexCatalogTxnOp::PutHypertable { plan }),
                "CREATE TABLE",
            )
            .await
        }
        K::CreateContinuousAggregate(plan) => {
            commit_catalog_op(
                req_id,
                scope,
                sql_method,
                store,
                eg_query::TxnOp::CreateView {
                    name: plan.name,
                    select_sql: plan.select_sql,
                    or_replace: true,
                },
                "CREATE MATERIALIZED VIEW",
            )
            .await
        }
        _ => unreachable!("catalog create statement was classified before dispatch"),
    }
}

#[cfg(feature = "query")]
async fn exec_sql_catalog_terminal(
    ctx: SqlDispatchCtx<'_>,
    kind: eg_query::StatementKind,
) -> Response {
    use eg_query::StatementKind as K;
    let SqlDispatchCtx {
        req_id,
        scope,
        sql_method,
        core: _,
        read_authority: _,
        store,
        read_core,
        read_store,
    } = ctx;
    match kind {
        K::Begin | K::Commit | K::Rollback => Response::err(
            req_id,
            "SQL error: transaction control requires a stateful SQL wire connection"
                .to_string(),
        ),
        K::CopyIn(_) => Response::err(
            req_id,
            "SQL error: COPY … FROM STDIN is a streaming pgwire operation, not available over Method::Sql"
                .to_string(),
        ),
        graph_kind @ (K::Read
        | K::PropertyGraphDdlRequiresCatalogAdmission(_)
        | K::PropertyGraphPrivilegeRequiresCatalogAdmission(_)
        | K::GraphTableReadRequiresCatalogAdmission(_)) => {
            exec_sql_property_graph(
                req_id,
                scope,
                sql_method,
                store,
                read_store,
                &read_core,
                graph_kind,
            )
            .await
        }
    }
}
