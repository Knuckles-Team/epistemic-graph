use super::*;

pub(crate) async fn exec_sql_property_graph(
    req_id: u64,
    scope: SqlWriteScope<'_>,
    sql_method: Method,
    store: &eg_query::TableStore,
    read_store: &eg_query::TableStore,
    read_core: &Arc<GraphCore>,
    kind: eg_query::StatementKind,
) -> Response {
    use eg_query::StatementKind as K;
    match kind {
        K::PropertyGraphDdlRequiresCatalogAdmission(_) => {
            exec_sql_property_graph_ddl(req_id, scope, sql_method, store).await
        }
        K::PropertyGraphPrivilegeRequiresCatalogAdmission(_) => {
            exec_sql_property_graph_privilege(req_id, scope, sql_method, store).await
        }
        K::GraphTableReadRequiresCatalogAdmission(query) => {
            exec_sql_graph_table_read(req_id, read_store.clone(), read_core.clone(), query).await
        }
        _ => Response::err(req_id, "SQL error: read routed to write path".to_string()),
    }
}

/// Bind a property-graph privilege change to the current object identity before
/// the shared transaction authorizer checks owner/admin authority and commits.
#[cfg(feature = "query")]
pub(crate) async fn exec_sql_property_graph_privilege(
    req_id: u64,
    scope: SqlWriteScope<'_>,
    sql_method: Method,
    store: &eg_query::TableStore,
) -> Response {
    let Method::Sql { query, .. } = &sql_method else {
        return Response::err(
            req_id,
            "SQL error: property-graph privilege change needs its SQL text".to_string(),
        );
    };
    let statement = match eg_query::sql::parse_property_graph_privilege(query) {
        Ok(statement) => statement,
        Err(error) => return Response::err(req_id, format!("SQL error: {error}")),
    };
    let record = match store.property_graph(scope.tenant_scope, &statement.name) {
        Ok(Some(record)) => record,
        Ok(None) => {
            return Response::err(
                req_id,
                format!(
                    "SQL error: {}",
                    crate::server::sql_catalog_acl::ACCESS_DENIED
                ),
            );
        }
        Err(error) => return Response::err(req_id, format!("SQL error: {error}")),
    };
    let (op, tag) = eg_query::PropertyGraphTxnOp::from_privilege_statement(
        statement,
        scope.tenant_scope,
        record.object_id,
    );
    let mut txn = eg_query::TableTxn::new();
    txn.push(eg_query::TxnOp::PropertyGraphDdl(op));
    commit_sql_catalog_txn(req_id, scope, sql_method.clone(), store, txn, tag).await
}

/// Commit one `CREATE`/`ALTER`/`DROP PROPERTY GRAPH` through the SQL catalog
/// MutationBatch kernel every other catalog DDL commits through.
#[cfg(feature = "query")]
pub(crate) async fn exec_sql_property_graph_ddl(
    req_id: u64,
    scope: SqlWriteScope<'_>,
    sql_method: Method,
    store: &eg_query::TableStore,
) -> Response {
    let Method::Sql { query, .. } = &sql_method else {
        return Response::err(
            req_id,
            "SQL error: property-graph DDL needs its SQL text".to_string(),
        );
    };
    let actor = scope.caller.unwrap_or(scope.tenant_scope).to_string();
    let statement = match eg_query::sql::parse_property_graph_ddl(query, scope.tenant_scope) {
        Ok(statement) => statement,
        Err(error) => return Response::err(req_id, format!("SQL error: {error}")),
    };
    let (op, tag) = eg_query::PropertyGraphTxnOp::from_statement(statement, &actor);
    let mut txn = eg_query::TableTxn::new();
    txn.push(eg_query::TxnOp::PropertyGraphDdl(op));
    commit_sql_catalog_txn(req_id, scope, sql_method.clone(), store, txn, tag).await
}

/// Execute a SQL/PGQ `GRAPH_TABLE` read.
///
/// The read store is the same per-request authorized projection used for plain
/// SQL reads. It contains a property-graph definition only when the caller has
/// graph SELECT and SELECT/RLS access to every pinned base table, so lowering
/// cannot bypass the tenant catalog's source authorization.
///
/// Lowering runs on the blocking pool, exactly like every other SQL read on
/// this route (`handle_sql`'s read arm, `exec_sql_write_insert_select`, and the
/// pgwire `run_read` path all wrap their call the same way). `eg_query`'s
/// executor builds and `block_on`s its OWN current-thread runtime, which panics
/// with "Cannot start a runtime from within a runtime" if it is entered from a
/// reactor worker -- so calling it inline here made every `GRAPH_TABLE` query
/// panic rather than answer.
#[cfg(feature = "query")]
pub(crate) async fn exec_sql_graph_table_read(
    req_id: u64,
    store: eg_query::TableStore,
    read_core: Arc<GraphCore>,
    query: eg_query::GraphTableQuery,
) -> Response {
    let rows = compute_off_lock(req_id, move || graph_table_rows(&store, &read_core, &query)).await;
    match rows {
        Ok(Ok(typed)) => match typed.rows.iter().map(msgpack_bytes).collect() {
            Ok(rows) => dynamic_response::<query_results::Sql, _>(
                req_id,
                &crate::protocol::QueryResult {
                    columns: typed.columns.iter().map(|c| c.name.clone()).collect(),
                    rows,
                },
            ),
            Err(error) => Response::err(req_id, error),
        },
        Ok(Err(error)) => Response::err(req_id, format!("SQL error: {error}")),
        Err(response) => response,
    }
}

#[cfg(feature = "query")]
pub(crate) fn graph_table_rows(
    store: &eg_query::TableStore,
    read_core: &Arc<GraphCore>,
    query: &eg_query::GraphTableQuery,
) -> Result<eg_query::TypedQueryResult, String> {
    // The authorized projection re-admits visible graphs under its own private
    // index scope. Resolve and lower against that scope rather than the source
    // tenant name, which intentionally is not copied into the projection.
    let projection_scope = store.index_scope();
    // A graph absent from this caller's authorized projection resolves exactly
    // like a graph that does not exist; the raw tenant catalog is never read.
    let record = store
        .property_graph(projection_scope, &query.graph)?
        .ok_or_else(|| {
            format!(
                "property graph `{}` does not exist",
                query.graph.leaf().value()
            )
        })?;
    // Read-time proof that every pinned base relation still has its admitted
    // schema digest -- defence in depth behind the base-DDL fence.
    store.verify_property_graph_dependencies(&record)?;
    eg_query::exec_graph_table_typed_with_tables(
        &read_core.analysis_snapshot(),
        store,
        query,
        &record.accepted_definition,
        projection_scope,
    )
}

/// A scalar cell (from a resolved SELECT row) coerced to the string node-id form the
/// engine stores (CONCEPT:EG-KG.query.insert-into-nodes-select/047).
#[cfg(feature = "query")]
pub(crate) fn cell_to_node_id(v: &serde_json::Value) -> Result<String, String> {
    match v {
        serde_json::Value::String(s) => Ok(s.clone()),
        serde_json::Value::Number(n) => Ok(n.to_string()),
        serde_json::Value::Bool(b) => Ok(b.to_string()),
        serde_json::Value::Null => Err("resolved a NULL `id` for a node write".to_string()),
        other => Err(format!("`id` must be a scalar, got {other}")),
    }
}

/// Build a `QueryResult`-shaped write ack and map the off-lock outcome (CONCEPT:EG-KG.query.mirrors-pgwire).
#[cfg(feature = "query")]
pub(crate) fn sql_write_ack(
    req_id: u64,
    tag: &str,
    outcome: Result<Result<usize, String>, Response>,
) -> Response {
    match outcome {
        Ok(Ok(n)) => {
            let row = match msgpack_bytes(&vec![serde_json::Value::from(n as u64)]) {
                Ok(row) => row,
                Err(error) => return Response::err(req_id, error),
            };
            let result = crate::protocol::QueryResult {
                columns: vec![tag.to_string()],
                rows: vec![row],
            };
            dynamic_response::<query_results::Sql, _>(req_id, &result)
        }
        Ok(Err(msg)) => Response::err(req_id, format!("SQL error: {msg}")),
        Err(resp) => resp,
    }
}

/// Commit the SQL catalog mutation inside the source-authority guard. Keeping this
/// synchronous helper separate leaves the async coordinator with one blocking
/// closure and keeps the authority callback at one nesting level.
#[cfg(feature = "query")]
struct SqlCatalogWork<'a> {
    req_id: u64,
    tenant_scope: &'a str,
    graph_name: &'a str,
    caller: Option<&'a str>,
    authority: &'a crate::server::access::CarrierAuthority,
    persist_dir: &'a std::path::Path,
    store: &'a eg_query::TableStore,
    txn: &'a mut eg_query::TableTxn,
    sql_method: &'a Method,
}

#[cfg(feature = "query")]
fn commit_sql_catalog_work(work: SqlCatalogWork<'_>) -> Result<usize, String> {
    let SqlCatalogWork {
        req_id,
        tenant_scope,
        graph_name,
        caller,
        authority,
        persist_dir,
        store,
        txn,
        sql_method,
    } = work;
    let batch_id = crate::server::mutation_batch::opaque_coordinator_key(
        "sql-catalog",
        authority.owner_scope(),
        authority.idempotency_key(),
    );
    let created_at_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or(0);
    crate::server::sql_catalog_acl::with_source_authority_write(persist_dir, authority, |source| {
        let expected_version = store.mutation_version(tenant_scope, graph_name)?;
        let batch = crate::server::mutation_batch::compile_opaque_method(
            crate::server::mutation_batch::CompileBatch {
                batch_id: &batch_id,
                request_id: req_id,
                attempt_nonce: authority.attempt_nonce(),
                principal: caller,
                tenant: tenant_scope,
                graph: graph_name,
                placement_epoch: 0,
                idempotency_key: authority.idempotency_key(),
                expected_graph_version: Some(expected_version),
                fencing_token: None,
                created_at_ms,
                default_surface: crate::mutation_batch::MutationSurface::Query,
                authoritative_state: None,
            },
            sql_method,
            crate::mutation_batch::MutationSurface::Query,
            crate::mutation_batch::DurabilityDomain::SqlCatalog,
            "sql_catalog_operation",
        )?;
        let committed_replay =
            crate::server::wire::committed_sql_replay_receipt(store, authority, &batch)?.is_some();
        let created_tables =
            crate::server::wire::authorize_table_txn(source, store, txn, committed_replay)
                .map_err(|error| error.message)?;
        let committed = match store.commit_txn_batch(txn, &batch, created_at_ms) {
            Ok(committed) => committed.record,
            Err(message) if message.contains("IDEMPOTENCY_CONFLICT") => {
                crate::server::wire::committed_sql_replay_receipt(store, authority, &batch)?
                    .ok_or(message)?
            }
            Err(message) => return Err(message),
        };
        let owner_operation_id =
            crate::server::sql_catalog_acl::stable_source_operation_id(&batch_id);
        for schema in &created_tables {
            crate::server::sql_catalog_acl::register_owner_after_create_in(
                source,
                &schema.name,
                owner_operation_id,
            )
            .map_err(|error| format!("SQL_OWNER_REPAIR_PENDING: {error}"))?;
        }
        let bytes = committed
            .result_msgpack
            .as_deref()
            .ok_or_else(|| "committed SQL MutationBatch has no result".to_string())?;
        eg_types::msgpack::decode_bounded::<usize>(
            bytes,
            eg_types::msgpack::MsgpackLimits::new(64, 1, 1),
        )
        .map_err(|_| "committed SQL result is corrupt".to_string())
    })
}

/// SQL catalog/table native coordinator. The user-table rows/catalog, terminal
/// MutationBatch record, SQL-domain OCC/fence, idempotency result and outbox land
/// in one tenant-scoped SQL-catalog transaction. Query text and parameters are represented
/// only by a SHA-256 operation digest in durable metadata.
#[cfg(feature = "query")]
pub(crate) async fn commit_sql_catalog_txn(
    req_id: u64,
    scope: SqlWriteScope<'_>,
    sql_method: Method,
    store: &eg_query::TableStore,
    txn: eg_query::TableTxn,
    tag: &'static str,
) -> Response {
    let SqlWriteScope {
        graph_name,
        tenant_scope,
        caller,
        authority,
        persist_dir,
    } = scope;
    let tenant_scope = tenant_scope.to_string();
    let graph_name = graph_name.to_string();
    let caller = caller.map(ToOwned::to_owned);
    let authority = authority.clone();
    let persist_dir = persist_dir.to_path_buf();
    let store = store.clone();
    let mut txn = txn;
    let outcome = compute_off_lock(req_id, move || {
        commit_sql_catalog_work(SqlCatalogWork {
            req_id,
            tenant_scope: &tenant_scope,
            graph_name: &graph_name,
            caller: caller.as_deref(),
            authority: &authority,
            persist_dir: &persist_dir,
            store: &store,
            txn: &mut txn,
            sql_method: &sql_method,
        })
    })
    .await;
    sql_write_ack(req_id, tag, outcome)
}

/// Resolve the node ids a WHERE selects (CONCEPT:EG-KG.query.mirrors-pgwire). `Id` is the fast path (the
/// node if it exists); `Predicate` (CONCEPT:EG-KG.query.compound-predicate-decode) scans the node store once,
/// decodes each blob to a row map (with the synthetic `id` column injected) and
/// evaluates the compound predicate. The matched ids are re-checked under the write
/// guard by the caller (`compare_and_set_fields_if`/`remove_node_if`).
#[cfg(feature = "query")]
pub(crate) fn matched_node_ids(core: &GraphCore, selector: &eg_query::WhereEq) -> Vec<String> {
    match selector {
        eg_query::WhereEq::Id(id) => {
            if core.has_node(id) {
                vec![id.clone()]
            } else {
                Vec::new()
            }
        }
        eg_query::WhereEq::Predicate { pred, .. } => {
            let mut out = Vec::new();
            for (id, blob) in core.get_nodes() {
                if let Ok(mut obj) = eg_types::msgpack::decode_property_object(&blob) {
                    obj.entry("id".to_string())
                        .or_insert_with(|| serde_json::Value::String(id.clone()));
                    if pred.eval(&obj) {
                        out.push(id);
                    }
                }
            }
            out
        }
    }
}

/// Resolve classify `ColumnDef`s (raw SQL type spellings) into store `Column`s
/// (CONCEPT:EG-KG.query.mirrors-pgwire — mirrors the pgwire `to_store_columns`).
#[cfg(feature = "query")]
pub(crate) fn to_store_columns(
    cols: &[eg_query::ColumnDef],
) -> Result<Vec<eg_query::Column>, String> {
    cols.iter()
        .map(|c| {
            let ty = eg_query::ColumnType::parse(&c.type_name)?;
            Ok(eg_query::Column {
                name: c.name.clone(),
                ty,
                nullable: c.nullable,
                primary_key: c.primary_key,
                unique: c.unique,
                serial: c.serial,
                default: c.default.clone(),
                check: c.check.clone(),
            })
        })
        .collect()
}

/// Lower a decoded `ALTER TABLE` action into the shared transactional table-store
/// operation. The SQL native coordinator can then apply the catalog change and its
/// MutationBatch metadata in one redb transaction.
#[cfg(feature = "query")]
pub(crate) fn alter_table_txn_op(
    plan: eg_query::AlterTablePlan,
) -> Result<eg_query::TxnOp, String> {
    use eg_query::AlterTableAction as A;
    match plan.action {
        A::AddColumn(col) => {
            let columns = to_store_columns(std::slice::from_ref(&col))?;
            let column = columns.into_iter().next().ok_or("ALTER TABLE: no column")?;
            Ok(eg_query::TxnOp::AddColumn {
                table: plan.name,
                column,
            })
        }
        A::DropColumn { column, if_exists } => Ok(eg_query::TxnOp::DropColumn {
            table: plan.name,
            column,
            if_exists,
        }),
        A::RenameColumn { from, to } => Ok(eg_query::TxnOp::RenameColumn {
            table: plan.name,
            from,
            to,
        }),
        A::RenameTable { new_name } => Ok(eg_query::TxnOp::RenameTable {
            table: plan.name,
            new_name,
        }),
        A::AlterColumnType { column, new_type } => {
            let ty = eg_query::ColumnType::parse(&new_type)?;
            Ok(eg_query::TxnOp::AlterColumnType {
                table: plan.name,
                column,
                new_type: ty,
            })
        }
        A::DropConstraint {
            constraint,
            if_exists,
        } => Ok(eg_query::TxnOp::DropConstraint {
            table: plan.name,
            constraint,
            if_exists,
        }),
    }
}

/// Produce the off-lock `GraphView` the query planner consumes, with per-agent
/// Row-Level Security applied IN the read/plan path (CONCEPT:EG-KG.sharding.row-level-security). Under the
/// `security` feature the owned snapshot is filtered down to the rows `caller` may
/// see BEFORE it reaches any query surface (SQL/Cypher/unified), so no surface can
/// exfiltrate a forbidden row. Without the feature this is exactly
/// `core.analysis_snapshot()` (zero overhead, behavior unchanged). Used on the
/// `not(result-cache)` path; with the result cache the same `filter_view` is applied
/// inline on the versioned snapshot so the version pairs atomically with the filter.
/// CONCEPT:EG-KG.query.fence-stripper — build the `schema_hint` fed to the NL planner: the distinct node
/// LABELS present in the target graph (capped so a huge graph stays cheap), so the model
/// targets real labels. Scans up to a bound of the snapshot's node blobs for their
/// `type`/`node_type`/`label` field (mirroring `get_nodes_by_label`). Best-effort — an
/// empty hint (no labels found) is fine; the planner's system prompt carries the grammar.
#[cfg(feature = "nl-query")]
pub(crate) fn nl_schema_hint(core: &Arc<GraphCore>) -> String {
    use std::collections::BTreeSet;
    const SCAN_CAP: usize = 512;
    let snap = core.analysis_snapshot();
    let mut labels: BTreeSet<String> = BTreeSet::new();
    for blob in snap.node_properties.values().take(SCAN_CAP) {
        if let Ok(v) = eg_types::msgpack::decode_property_value(blob) {
            for key in ["type", "node_type", "label"] {
                if let Some(s) = v.get(key).and_then(|x| x.as_str()) {
                    labels.insert(s.to_string());
                    break;
                }
            }
        }
        if labels.len() >= 64 {
            break;
        }
    }
    if labels.is_empty() {
        "Available node labels: (none discovered)".to_string()
    } else {
        format!(
            "Available node labels: {}",
            labels.into_iter().collect::<Vec<_>>().join(", ")
        )
    }
}

/// The per-(actor,version) `FilteredViewCache` probe-then-build used by every
/// `result-cache` MISS across the Sql/UnifiedQuery/UnifiedQueryText/GraphQl/
/// CypherQuery arms (mirrors `rls_snapshot` below, which is the SAME idiom for a
/// `not(result-cache)` build) — pure extract-method out of each of those arms'
/// bodies, no behaviour change. The `not(feature = "security")` fallback
/// (`core.analysis_snapshot_versioned()`, no filtering) stays inline at each call
/// site — it is a single unbranched call, so extracting it here would only add an
/// indirection, not remove any complexity.
#[cfg(all(
    feature = "result-cache",
    any(feature = "query", feature = "cypher", feature = "graphql")
))]
pub(crate) fn versioned_rls_snapshot(
    core: &Arc<GraphCore>,
    caller: &str,
    rls: &Arc<crate::isolation::IsolationLayer>,
) -> (Arc<crate::graph::GraphView>, u64) {
    let probe_version = core.version();
    match core.cached_filtered_view(caller, probe_version) {
        Some(cached) => (cached, probe_version),
        None => {
            let generation = core.filtered_view_cache_generation();
            let (mut fresh, built_version) = core.analysis_snapshot_versioned();
            rls.filter_view(caller, &mut fresh);
            let fresh = Arc::new(fresh);
            core.put_cached_filtered_view(
                caller.to_string(),
                built_version,
                generation,
                fresh.clone(),
            );
            (fresh, built_version)
        }
    }
}

#[cfg(all(
    any(feature = "query", feature = "cypher", feature = "graphql"),
    not(feature = "result-cache")
))]
pub(crate) fn rls_snapshot(
    core: &Arc<GraphCore>,
    #[cfg(feature = "security")] caller: &str,
    #[cfg(feature = "security")] rls: &Arc<crate::isolation::IsolationLayer>,
) -> Arc<crate::graph::GraphView> {
    // perf/row-visibility-index (B-sweep): this ONE helper backs FIVE `Method`
    // arms in a `not(result-cache)` build (Sql/UnifiedQuery/UnifiedQueryText/
    // GraphQl/CypherQuery — every `rls_snapshot(...)` call site in this file), so
    // wiring the per-(actor,version) `FilteredViewCache` here amortizes the
    // per-node RLS decode for all of them in one place, mirroring EXACTLY
    // `Method::CypherQuery`'s own result-cache-build probe-then-build shape
    // above (same cache, same RLS-safety argument: keyed by (actor, version),
    // never a cross-actor share, invalidated on every whole-image transition
    // alongside `project_core`'s own cache).
    //
    // ONE deliberate difference from that reference implementation:
    // `analysis_snapshot_versioned` (an atomically-paired (view, version) read
    // under one topology lock) is itself gated `feature = "result-cache"` — and
    // this whole function only compiles when `result-cache` is OFF — so it is
    // unavailable here. `probe_version` is instead read via a separate
    // `core.version()` call BEFORE building the plain `analysis_snapshot()`,
    // exactly the same "captured before the scan so the stamp is a safe LOWER
    // BOUND" idiom `get_nodes_by_label_page` already documents for its own
    // index stamp: a concurrent write racing between the two reads can only
    // make the snapshot reflect content NEWER than `probe_version` claims,
    // never older. Since `GraphCore::version()` is monotonic, no future caller
    // ever probes at that same (now-stale) version again, so a mis-stamped
    // entry is simply orphaned (an inert cache slot, evicted eventually by
    // LRU) — never served to a caller who didn't ask for exactly that version.
    // The unsafe direction (reading version AFTER the snapshot, which could
    // label OLDER content with a NEWER version and later serve a stale RLS
    // decision to a caller who has since had access revoked) is never taken.
    #[cfg(feature = "security")]
    {
        let probe_version = core.version();
        if let Some(cached) = core.cached_filtered_view(caller, probe_version) {
            return cached;
        }
        let generation = core.filtered_view_cache_generation();
        let mut fresh = core.analysis_snapshot();
        rls.filter_view(caller, &mut fresh);
        let fresh = Arc::new(fresh);
        core.put_cached_filtered_view(caller.to_string(), probe_version, generation, fresh.clone());
        fresh
    }
    #[cfg(not(feature = "security"))]
    {
        Arc::new(core.analysis_snapshot())
    }
}

/// ⚠ THE RLS-AWARE RESULT-CACHE KEY (CONCEPT:EG-KG.coordination.distributed-cache-coherence × KG-2.231 — the headline
/// reconciliation). RLS makes a query's RESULT agent-specific: agent A and agent B
/// running the SAME query text see DIFFERENT rows (A cannot see B's private nodes).
/// The result cache is keyed by `(query-hash, version)`; if that hash ignored the
/// caller, agent A's cached (A-filtered) result could be served to agent B for the
/// same query text — a cross-agent data leak.
///
/// The caller's RLS context is always folded into the hash
/// `kind` so a different caller keys to a different cache slot. The agent_id IS the
/// complete RLS visibility key: `IsolationLayer::filter_view`/`can_see_row` resolve
/// a row's visibility for a caller PURELY from that caller's agent_id against the
/// registered identities (owner / explicit grants / manager-of / System role), so
/// two requests with the same agent_id always get the byte-identical filtered view,
/// and two with different agent_ids may not — exactly the cache-key equivalence we
/// need. A build without the `security` feature uses the plain `(kind, payload)`.
#[cfg(all(
    feature = "result-cache",
    any(feature = "query", feature = "cypher", feature = "graphql")
))]
pub(crate) fn rls_cache_hash(
    kind: &str,
    payload: &[u8],
    #[cfg(feature = "security")] caller: &str,
    #[cfg(feature = "security")] _rls: &Arc<crate::isolation::IsolationLayer>,
) -> u128 {
    #[cfg(feature = "security")]
    {
        let salted_kind = format!("rls:{caller}:{kind}");
        ResultCache::hash_query(&salted_kind, payload)
    }
    #[cfg(not(feature = "security"))]
    {
        ResultCache::hash_query(kind, payload)
    }
}
