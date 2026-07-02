//! Postgres wire-protocol shim (CONCEPT:KG-2.189) — a thin facade over the
//! engine's internal SQL surface that lets `psql`, BI tools, and ORMs connect and
//! run SQL against a graph.
//!
//! ## What this is (and is NOT)
//! This is a SHIM, not a second SQL engine. A SELECT arriving over the wire is
//! parsed/planned/executed by the SAME DataFusion path `Method::Sql` uses
//! (`eg_query::exec_sql_typed` over an off-lock `GraphView`); a write is classified
//! (`eg_query::classify`) and routed through the engine's `GraphTxn` write path so
//! it gets `mark_dirty` + durability for free. No SQL grammar, planner, or executor
//! is reimplemented here.
//!
//! ## Protocols supported
//! BOTH Postgres query protocols (CONCEPT:KG-2.197):
//!   * **Simple query** (`SimpleQueryHandler`) — a single text query string, one
//!     round-trip. What raw `psql` and `client.simple_query(...)` use.
//!   * **Extended / prepared** (`ExtendedQueryHandler`) — the Parse / Bind /
//!     Describe / Execute / Sync / Close flow with prepared statements, parameter
//!     binding (`$1`, `$2`, …), and portals. This is what psycopg3, asyncpg, JDBC,
//!     sqlx, SQLAlchemy, and `tokio-postgres::prepare`/`query`/`execute` use by
//!     DEFAULT — so it is the unlock that makes real drivers (not just `psql`)
//!     work. Both protocols funnel into the SAME `execute_sql` core: the extended
//!     path substitutes bound parameters into the SQL as literals first
//!     (`substitute_params`), then runs the identical classify → read/write path.
//!
//! ## DML supported (CONCEPT:KG-2.198)
//! Over the `nodes` table: single- and multi-row `INSERT`, `UPDATE … SET … WHERE
//! id = '…'` (or a simple `WHERE <prop> = <literal>`), `DELETE … WHERE …`, and a
//! `RETURNING` clause on any of them (the affected rows are returned as a result
//! set). Parameterized DML works via the extended protocol. Deferred follow-ups:
//! complex WHERE (AND/OR/ranges/IN), joins/subqueries in DML, `INSERT … SELECT`.
//!
//! ## Connected graph
//! The graph a connection runs against is selected, in order: (1) the libpq
//! `database` startup parameter (e.g. `psql -d 'team:alpha'`), (2) else
//! `EPISTEMIC_GRAPH_PGWIRE_GRAPH` (a server default), (3) else `__commons__`. It
//! can be changed mid-session with `SET graph = '<name>'` (or `SET graph TO
//! '<name>'`). A missing graph yields a clean error, not a panic.
//!
//! ## Auth model (CONCEPT:KG-2.202)
//! Opt-in via `EPISTEMIC_GRAPH_PGWIRE_AUTH` = `trust` | `scram` (see `auth.rs`):
//!   * `trust` — no authentication (`NoopHandler`); the zero-infra/dev path. The
//!     DEFAULT only when no `GRAPH_SERVICE_AUTH_SECRET` is configured.
//!   * `scram` — SCRAM-SHA-256 bridged to the engine secret. The DEFAULT when an
//!     engine secret IS set. The pg `user` maps to an engine `agent_id` and the
//!     password is derived from the shared secret; on a successful login the
//!     connection's ACTOR is set to that user, so subsequent queries run under the
//!     engine ACL (`IsolationLayer::check_access`) for the mapped identity.
//!
//! The listener still only binds when an operator sets `EPISTEMIC_GRAPH_PGWIRE_ADDR`
//! (documented loopback `127.0.0.1:5433`).
//!
//! ## Arrow → pg type-OID mapping
//! Result columns are described from the Arrow result schema via
//! `eg_query::PgColType`: `Int8 → INT8`, `Float8 → FLOAT8`, `Bool → BOOL`,
//! everything else `TEXT` (JSON-stringified) — so a column is never lossy-dropped.

use std::sync::Arc;

use async_trait::async_trait;
use futures::{stream, Stream};
use tokio::net::TcpListener;
use tokio::sync::RwLock;

use pgwire::api::auth::StartupHandler;
use pgwire::api::portal::{Format, Portal};
use pgwire::api::query::{ExtendedQueryHandler, SimpleQueryHandler};
use pgwire::api::results::{
    DataRowEncoder, DescribePortalResponse, DescribeStatementResponse, FieldInfo, QueryResponse,
    Response, Tag,
};
use pgwire::api::stmt::{QueryParser, StoredStatement};
use pgwire::api::{ClientInfo, PgWireServerHandlers, Type};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use pgwire::messages::data::DataRow;
use pgwire::tokio::process_socket;

use eg_query::{
    AlterTablePlan, Column, ColumnType, CopyFormat, CopyPlan, CreateTablePlan, CreateViewPlan,
    DeleteNodes, DeleteNodesJoin, DeleteTable, DropTablePlan, DropViewPlan, InsertNodes,
    InsertNodesSelect, InsertSelect, InsertTable, OnConflictAction, PgColType, StatementKind,
    TableSchema, TableStore, TableTxn, TxnOp, TypedColumn, TypedQueryResult, UpdateNodes,
    UpdateNodesJoin, UpdateTable, WhereEq,
};

use crate::server::ServerState;

mod auth;

pub use auth::{derive_pg_password, PgWireAuthMode, PGWIRE_AUTH_ENV};

use crate::isolation::AccessLevel;

/// Env var: when set (and the binary is built `--features pgwire`), the pgwire
/// listener binds this address (e.g. `127.0.0.1:5433`). Unset → no listener.
pub const PGWIRE_ADDR_ENV: &str = "EPISTEMIC_GRAPH_PGWIRE_ADDR";
/// Env var: the default graph a fresh connection runs against when the libpq
/// `database` parameter is not supplied. Defaults to `__commons__`.
pub const PGWIRE_GRAPH_ENV: &str = "EPISTEMIC_GRAPH_PGWIRE_GRAPH";
/// Env var: explicit path to the user-defined SQL table store redb file
/// (CONCEPT:EG-018). When unset it derives from `GRAPH_SERVICE_PERSIST_DIR`
/// (`<persist_dir>/sql_tables.redb`) so user tables live beside the graph durable
/// tier; absent that, a process-temp file (in-memory-ish, lost on restart).
pub const PGWIRE_SQL_TABLES_ENV: &str = "EPISTEMIC_GRAPH_SQL_TABLES_PATH";

/// The shared user-table store, opening it on first use (CONCEPT:EG-018). Delegates to
/// the process-wide `crate::server::sql_tables` singleton (CONCEPT:EG-023) so the pgwire
/// shim and the wire `Method::Sql` DDL/DML path open the SAME redb file exactly once —
/// redb permits one handle per file per process, so they MUST share.
fn user_table_store() -> PgWireResult<TableStore> {
    crate::server::sql_tables::user_table_store().map_err(user_err)
}

/// Resolve a classify `ColumnDef` (raw SQL type spelling) into a store [`Column`].
fn to_store_columns(cols: &[eg_query::ColumnDef]) -> PgWireResult<Vec<Column>> {
    cols.iter()
        .map(|c| {
            let ty = ColumnType::parse(&c.type_name).map_err(user_err)?;
            Ok(Column {
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

/// Per-connection `COPY … FROM STDIN` state (CONCEPT:EG-020): the resolved target,
/// the streamed bytes accumulated across `CopyData` frames, and the decode format.
/// Lives in `EngineBackend::copy` between the `CopyIn` response and `on_copy_done`.
struct CopyState {
    table: String,
    /// The resolved insert column list (the COPY column list, or all schema columns).
    columns: Vec<String>,
    format: CopyFormat,
    delimiter: Option<char>,
    header: bool,
    /// Accumulated raw bytes of the copy-in body.
    buf: Vec<u8>,
}

/// Per-connection backend handler. Holds the shared `ServerState` and the current
/// target graph (mutated by `SET graph = …`). One instance per connection so the
/// `SET graph` selection is connection-scoped.
struct EngineBackend {
    state: Arc<RwLock<ServerState>>,
    /// Current connected graph for this connection. `parking_lot::Mutex` keeps the
    /// handler `Send + Sync` without an async lock on the (synchronous) SET path.
    graph: parking_lot::Mutex<String>,
    /// False until the first query resolves the connection's target from the libpq
    /// `database` startup parameter (priority 1). The handler is built before
    /// startup, so the `database` param is only readable from `ClientInfo` once the
    /// first query arrives — at which point this latches the graph (unless an
    /// explicit `SET graph` already chose one).
    startup_resolved: std::sync::atomic::AtomicBool,
    /// The SQL parser used for the extended protocol's Parse step. Stateless.
    parser: Arc<EngineQueryParser>,
    /// The authenticated actor (engine `agent_id`) for this connection
    /// (CONCEPT:KG-2.202). `None` = anonymous (trust mode / no auth). Latched once
    /// from the libpq `user` startup parameter on the first query AFTER auth has
    /// succeeded — the startup handler guarantees we only reach the query phase
    /// authenticated, so the metadata user is the authenticated identity. Every
    /// graph access then enforces the engine ACL under this actor.
    actor: parking_lot::Mutex<Option<String>>,
    /// The resolved pgwire auth mode. Under TRUST the actor stays anonymous (no ACL
    /// identity); under SCRAM the actor is the authenticated user.
    auth_mode: PgWireAuthMode,
    /// The OPEN multi-statement transaction's buffered user-table ops (CONCEPT:EG-020),
    /// or `None` when no `BEGIN` is active. `COMMIT` applies the buffer in ONE redb
    /// write txn; `ROLLBACK` drops it. Scoped per connection.
    txn: parking_lot::Mutex<Option<TableTxn>>,
    /// In-flight `COPY … FROM STDIN` state (CONCEPT:EG-020), set between the `CopyIn`
    /// response and `on_copy_done`/`on_copy_fail`.
    copy: parking_lot::Mutex<Option<CopyState>>,
}

impl EngineBackend {
    fn new(
        state: Arc<RwLock<ServerState>>,
        default_graph: String,
        auth_mode: PgWireAuthMode,
    ) -> Self {
        Self {
            state,
            graph: parking_lot::Mutex::new(default_graph),
            startup_resolved: std::sync::atomic::AtomicBool::new(false),
            parser: Arc::new(EngineQueryParser),
            actor: parking_lot::Mutex::new(None),
            auth_mode,
            txn: parking_lot::Mutex::new(None),
            copy: parking_lot::Mutex::new(None),
        }
    }

    /// Whether a multi-statement transaction is currently open.
    fn in_txn(&self) -> bool {
        self.txn.lock().is_some()
    }

    /// Buffer a user-table op into the open transaction (panics if none open — callers
    /// guard with [`Self::in_txn`]).
    fn buffer(&self, op: TxnOp) {
        if let Some(t) = self.txn.lock().as_mut() {
            t.push(op);
        }
    }

    fn current_graph(&self) -> String {
        self.graph.lock().clone()
    }

    /// One-time, on the first query: (1) adopt the libpq `database` startup
    /// parameter as the target graph (priority 1), and (2) latch the authenticated
    /// actor from the libpq `user` (CONCEPT:KG-2.202). Both are only readable from
    /// `ClientInfo` once startup completes, so they latch on the first query.
    ///
    /// The graph adoption is skipped if a `SET graph` already chose one, if the
    /// param is absent, or if it equals the user name (libpq's default when `dbname`
    /// is unset — not a deliberate pick). The actor is set to the connecting `user`
    /// ONLY under SCRAM (an authenticated identity); under TRUST it stays anonymous
    /// (`None`) so the back-compat single-tenant path is unchanged. Idempotent.
    fn resolve_startup_graph<C: ClientInfo>(&self, client: &C) {
        use std::sync::atomic::Ordering;
        if self.startup_resolved.swap(true, Ordering::AcqRel) {
            return;
        }
        let meta = client.metadata();
        let user = meta.get(pgwire::api::METADATA_USER).cloned();
        // Under SCRAM the connection only reaches here authenticated, so the libpq
        // user IS the validated engine identity — adopt it as the ACL actor.
        if self.auth_mode == PgWireAuthMode::Scram {
            if let Some(u) = user.as_ref().filter(|u| !u.is_empty()) {
                *self.actor.lock() = Some(u.clone());
            }
        }
        let db = match meta.get(pgwire::api::METADATA_DATABASE) {
            Some(d) if !d.is_empty() => d,
            _ => return,
        };
        // libpq defaults `database` to the user name when `dbname` is unset; that
        // is not a deliberate graph pick, so leave the server default in place.
        if user.as_deref() == Some(db.as_str()) {
            return;
        }
        *self.graph.lock() = db.clone();
    }

    /// The authenticated actor (engine `agent_id`) for this connection, or `None`
    /// for an anonymous (trust) connection.
    fn actor(&self) -> Option<String> {
        self.actor.lock().clone()
    }
}

/// Map an internal engine error string to a pgwire user error (SQLSTATE 58000 —
/// system error — keeps psql/ORMs reporting a clean message instead of a drop).
fn user_err(msg: impl Into<String>) -> PgWireError {
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "ERROR".to_owned(),
        "58000".to_owned(),
        msg.into(),
    )))
}

/// `eg_query::PgColType` → a Postgres wire `Type` (the OID the client sees).
fn pg_type(t: PgColType) -> Type {
    match t {
        PgColType::Int8 => Type::INT8,
        PgColType::Float8 => Type::FLOAT8,
        PgColType::Bool => Type::BOOL,
        PgColType::Text => Type::TEXT,
    }
}

/// Build the `RowDescription` fields from a typed result, honouring the requested
/// per-column wire format (text for the simple-query protocol; the extended
/// protocol's `result_column_format` for prepared portals). A `None` format
/// defaults to text — the universally-compatible path.
fn field_defs(result: &TypedQueryResult, format: Option<&Format>) -> Arc<Vec<FieldInfo>> {
    Arc::new(
        result
            .columns
            .iter()
            .enumerate()
            .map(|(i, c)| {
                let fmt = format
                    .map(|f| f.format_for(i))
                    .unwrap_or(pgwire::api::results::FieldFormat::Text);
                FieldInfo::new(c.name.clone(), None, None, pg_type(c.ty), fmt)
            })
            .collect(),
    )
}

/// Render one decoded JSON cell as the representation Postgres expects for the
/// column's type, honouring the encoder's per-field text/binary format. NULL stays
/// NULL; strings pass through; numbers/bools render canonically; anything
/// structural is JSON-stringified. The `DataRowEncoder` chooses text vs binary
/// from the `FieldInfo` format it was built with, so this single path serves both
/// the simple (text) and extended (text-or-binary) protocols.
fn encode_cell(
    encoder: &mut DataRowEncoder,
    ty: PgColType,
    cell: &serde_json::Value,
) -> PgWireResult<()> {
    use serde_json::Value;
    if cell.is_null() {
        return encoder.encode_field(&None::<&str>);
    }
    match ty {
        PgColType::Int8 => match cell.as_i64() {
            Some(i) => encoder.encode_field(&i),
            None => encoder.encode_field(&cell.to_string()),
        },
        PgColType::Float8 => match cell.as_f64() {
            Some(f) => encoder.encode_field(&f),
            None => encoder.encode_field(&cell.to_string()),
        },
        PgColType::Bool => match cell.as_bool() {
            Some(b) => encoder.encode_field(&b),
            None => encoder.encode_field(&cell.to_string()),
        },
        PgColType::Text => match cell {
            Value::String(s) => encoder.encode_field(&s.as_str()),
            other => encoder.encode_field(&other.to_string()),
        },
    }
}

/// Build the streamed `DataRow`s for a typed result, encoding each cell in the
/// schema's per-column format.
fn rows_stream(
    result: TypedQueryResult,
    schema: Arc<Vec<FieldInfo>>,
) -> impl Stream<Item = PgWireResult<DataRow>> {
    let col_types: Vec<PgColType> = result.columns.iter().map(|c| c.ty).collect();
    let mut out = Vec::with_capacity(result.rows.len());
    for row in &result.rows {
        let mut encoder = DataRowEncoder::new(schema.clone());
        let mut err = None;
        for (i, cell) in row.iter().enumerate() {
            let ty = col_types.get(i).copied().unwrap_or(PgColType::Text);
            if let Err(e) = encode_cell(&mut encoder, ty, cell) {
                err = Some(e);
                break;
            }
        }
        match err {
            Some(e) => out.push(Err(e)),
            None => out.push(Ok(encoder.take_row())),
        }
    }
    stream::iter(out)
}

/// Turn a [`TypedQueryResult`] into a pgwire `QueryResponse` honouring the result
/// column format (text by default; the portal's `result_column_format` for the
/// extended protocol).
fn query_response(result: TypedQueryResult, format: Option<&Format>) -> QueryResponse {
    let schema = field_defs(&result, format);
    let data = rows_stream(result, schema.clone());
    QueryResponse::new(schema, data)
}

/// The PgColType for a single JSON value (RETURNING result-set schema inference).
fn col_type_of(v: &serde_json::Value) -> PgColType {
    match v {
        serde_json::Value::Number(n) if n.is_i64() || n.is_u64() => PgColType::Int8,
        serde_json::Value::Number(_) => PgColType::Float8,
        serde_json::Value::Bool(_) => PgColType::Bool,
        _ => PgColType::Text,
    }
}

/// The deterministic column order a `RETURNING` clause projects, EXACTLY matching
/// what the extended-protocol Describe step reports (so the executed DataRow field
/// count never disagrees with the described `RowDescription`). A NAMED projection
/// (`RETURNING id, rank`) yields those columns; `RETURNING *` (or no statically
/// nameable list) yields `id` followed by the property columns in `prop_cols`
/// order. Both paths compute this from the SAME inputs so they agree.
fn returning_projection(sql: &str, prop_cols: &[String]) -> Vec<String> {
    if let Some(named) = eg_query::returning_columns(sql) {
        return named;
    }
    // `RETURNING *` (or unnameable): `id` + every known property column.
    let mut cols = vec!["id".to_string()];
    for c in prop_cols {
        if c != "id" {
            cols.push(c.clone());
        }
    }
    cols
}

/// Build a RETURNING result set from the affected `(id, properties)` nodes,
/// projecting EXACTLY `col_names` (the columns the Describe step reported) and typing
/// each from `col_type_map` — the SAME `name → PgColType` schema the Describe step
/// uses — so the executed DataRows' types ALWAYS match the described `RowDescription`
/// (a mismatch would break client-side cell decode). A named column missing from a
/// node's properties is NULL; `id` is filled from the node id. A column absent from
/// the type map defaults to TEXT; `id` is TEXT.
fn returning_result(
    affected: &[(String, serde_json::Map<String, serde_json::Value>)],
    col_names: &[String],
    col_type_map: &std::collections::HashMap<String, PgColType>,
) -> TypedQueryResult {
    let columns: Vec<TypedColumn> = col_names
        .iter()
        .map(|name| {
            let ty = if name == "id" {
                PgColType::Text
            } else {
                col_type_map.get(name).copied().unwrap_or(PgColType::Text)
            };
            TypedColumn {
                name: name.clone(),
                ty,
            }
        })
        .collect();
    let mut rows = Vec::with_capacity(affected.len());
    for (id, props) in affected {
        let mut row = Vec::with_capacity(col_names.len());
        for name in col_names {
            if name == "id" {
                row.push(serde_json::Value::String(id.clone()));
            } else {
                row.push(props.get(name).cloned().unwrap_or(serde_json::Value::Null));
            }
        }
        rows.push(row);
    }
    TypedQueryResult { columns, rows }
}

impl EngineBackend {
    /// Resolve `SET graph = '<name>'` / `SET graph TO <name>`. Returns `Some(Tag)`
    /// when the statement IS a graph SET (handled here), else `None` (not ours).
    fn try_set_graph(&self, sql: &str) -> Option<PgWireResult<Tag>> {
        let trimmed = sql.trim().trim_end_matches(';').trim();
        let lower = trimmed.to_ascii_lowercase();
        let rest = lower.strip_prefix("set ")?;
        // Accept `graph = x` or `graph to x`.
        let after = rest.strip_prefix("graph")?.trim_start();
        let value_part = after
            .strip_prefix('=')
            .or_else(|| after.strip_prefix("to "))
            .or_else(|| after.strip_prefix("to"))?;
        // Recover the original-case value from the trimmed statement by length.
        let value_raw = &trimmed[trimmed.len() - value_part.trim_start().len()..];
        let name = value_raw
            .trim()
            .trim_matches(['\'', '"'].as_ref())
            .to_string();
        if name.is_empty() {
            return Some(Err(user_err("SET graph requires a graph name")));
        }
        *self.graph.lock() = name;
        Some(Ok(Tag::new("SET")))
    }

    /// Clone the target graph's `Arc<GraphCore>` out of the registry (read lock),
    /// or a clean error if it doesn't exist.
    async fn graph_core(&self, graph: &str) -> PgWireResult<Arc<crate::graph::GraphCore>> {
        let s = self.state.read().await;
        match s.registry.get(graph) {
            Some(e) => Ok(e.core.clone()),
            None => Err(user_err(format!("graph '{graph}' not found"))),
        }
    }

    /// Enforce the engine ACL for this connection's actor against `graph` at the
    /// requested `access` level (CONCEPT:KG-2.202). Reuses the SAME
    /// `IsolationLayer::check_access` the engine's MessagePack dispatch uses, with
    /// the same back-compat invariant: while NO identities are registered the layer
    /// has no rules and everything is allowed (single-tenant / trust deployments are
    /// unchanged). Once rules exist, an authenticated pg user is mapped to its
    /// `agent_id` and peer-graph isolation / team scoping / manager reach all apply
    /// exactly as they do over the native protocol. An anonymous (trust) connection
    /// is treated as the empty actor — denied any graph that has rules beyond the
    /// open `__commons__`/global-read, matching native-anonymous behavior.
    async fn check_access(&self, graph: &str, access: AccessLevel) -> PgWireResult<()> {
        let actor = self.actor();
        let s = self.state.read().await;
        if !s.isolation.has_rules() {
            return Ok(());
        }
        let (graph_type, owner) = match s.registry.get(graph) {
            Some(e) => (e.graph_type, e.owner.clone()),
            // A missing graph is reported by the caller's own resolve; allow here so
            // the not-found error surfaces instead of a misleading ACCESS_DENIED.
            None => return Ok(()),
        };
        let agent = actor.as_deref().unwrap_or("");
        if s.isolation
            .check_access(agent, graph, graph_type, owner.as_deref(), access)
        {
            Ok(())
        } else {
            crate::metrics::access_denied();
            Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                // 42501 — insufficient_privilege (what a real pg ACL denial reports).
                "42501".to_owned(),
                format!(
                    "permission denied for graph '{}' (agent '{}', {:?})",
                    graph,
                    if agent.is_empty() {
                        "<anonymous>"
                    } else {
                        agent
                    },
                    access
                ),
            ))))
        }
    }

    /// Execute a read (`SELECT`/`WITH`/…) over `graph` by reusing the EXACT
    /// DataFusion path `Method::Sql` uses: take the owned off-lock
    /// `analysis_snapshot()` and run `eg_query::exec_sql_typed` on the blocking
    /// pool (DataFusion's executor must not run on a reactor worker).
    async fn run_read(&self, graph: &str, sql: String) -> PgWireResult<TypedQueryResult> {
        let core = self.graph_core(graph).await?;
        let snap = core.analysis_snapshot();
        // CONCEPT:EG-018: register the user tables alongside the graph projection so a
        // SELECT can read a user table, JOIN it to `nodes`/`edges`, or both in ONE plan.
        let store = user_table_store()?;
        tokio::task::spawn_blocking(move || {
            eg_query::exec_sql_typed_with_tables(&snap, &store, &sql)
        })
        .await
        .map_err(|e| user_err(format!("query task failed: {e}")))?
        .map_err(|msg| user_err(format!("SQL error: {msg}")))
    }

    /// The shared SQL execution core for BOTH protocols (CONCEPT:KG-2.197). `sql`
    /// is already a complete literal statement (the extended path has substituted
    /// any `$N` parameters before calling). Classifies and routes:
    ///   * `SET graph = …` → connection-scoped graph switch (Execution tag).
    ///   * a read → the DataFusion path, returned as a Query response honouring the
    ///     requested `result_format`.
    ///   * a write → the GraphTxn + durability path; a `RETURNING` write yields a
    ///     Query response of the affected rows, otherwise a CommandComplete tag.
    async fn execute_sql(
        &self,
        sql: &str,
        result_format: Option<&Format>,
    ) -> PgWireResult<Response> {
        if let Some(res) = self.try_set_graph(sql) {
            return res.map(Response::Execution);
        }
        let graph = self.current_graph();
        let kind = eg_query::classify(sql).map_err(user_err)?;

        // ── transaction control (CONCEPT:EG-020) — no graph access needed ──────────
        match &kind {
            StatementKind::Begin => {
                *self.txn.lock() = Some(TableTxn::new());
                return Ok(Response::Execution(Tag::new("BEGIN")));
            }
            StatementKind::Commit => return self.run_commit().await,
            StatementKind::Rollback => {
                self.txn.lock().take();
                return Ok(Response::Execution(Tag::new("ROLLBACK")));
            }
            _ => {}
        }

        // Enforce the engine ACL under the connection's authenticated actor
        // (CONCEPT:KG-2.202) BEFORE touching the graph: a read needs Read access, any
        // DML needs Write. A no-rules deployment is a no-op (back-compat).
        let access = match kind {
            StatementKind::Read => AccessLevel::Read,
            _ => AccessLevel::Write,
        };
        self.check_access(&graph, access).await?;

        // While a transaction is OPEN, buffer user-table DDL/DML into it (applied
        // atomically at COMMIT). Reads and graph-node DML still execute immediately —
        // the SQL transaction is scoped to the durable relational user-table store.
        let in_txn = self.in_txn();

        match kind {
            StatementKind::Read => {
                let result = self.run_read(&graph, sql.to_string()).await?;
                Ok(Response::Query(query_response(result, result_format)))
            }
            StatementKind::InsertNodes(ins) => {
                self.run_insert(&graph, sql, ins, result_format).await
            }
            StatementKind::UpdateNodes(upd) => {
                self.run_update(&graph, sql, upd, result_format).await
            }
            StatementKind::DeleteNodes(del) => {
                self.run_delete(&graph, sql, del, result_format).await
            }
            // ── arbitrary user-defined relational tables (CONCEPT:EG-018/EG-020) ───
            StatementKind::CreateTable(plan) if in_txn => {
                let columns = to_store_columns(&plan.columns)?;
                self.buffer(TxnOp::CreateTable {
                    schema: TableSchema {
                        name: plan.name,
                        columns,
                    },
                    if_not_exists: plan.if_not_exists,
                });
                Ok(Response::Execution(Tag::new("CREATE TABLE")))
            }
            StatementKind::CreateTable(plan) => self.run_create_table(plan).await,
            StatementKind::DropTable(plan) if in_txn => {
                self.buffer(TxnOp::DropTable {
                    name: plan.name,
                    if_exists: plan.if_exists,
                });
                Ok(Response::Execution(Tag::new("DROP TABLE")))
            }
            StatementKind::DropTable(plan) => self.run_drop_table(plan).await,
            StatementKind::AlterTable(plan) if in_txn => {
                let columns = to_store_columns(std::slice::from_ref(&plan.add_column))?;
                let column = columns.into_iter().next().expect("one column");
                self.buffer(TxnOp::AddColumn {
                    table: plan.name,
                    column,
                });
                Ok(Response::Execution(Tag::new("ALTER TABLE")))
            }
            StatementKind::AlterTable(plan) => self.run_alter_table(plan).await,
            StatementKind::InsertTable(ins) if in_txn => {
                let n = ins.rows.len();
                self.buffer(TxnOp::Insert {
                    table: ins.table,
                    col_order: ins.columns,
                    rows: ins.rows,
                });
                Ok(Response::Execution(Tag::new("INSERT").with_rows(n)))
            }
            StatementKind::InsertTable(ins) => self.run_insert_table(ins).await,
            StatementKind::InsertSelect(ins) if in_txn => {
                // The SELECT half is a read (runs immediately); only the INSERT is
                // buffered into the transaction.
                let result = self.run_read(&graph, ins.select_sql).await?;
                if result.columns.len() != ins.columns.len() {
                    return Err(user_err(format!(
                        "INSERT … SELECT column count mismatch: {} target columns, {} selected",
                        ins.columns.len(),
                        result.columns.len()
                    )));
                }
                let n = result.rows.len();
                self.buffer(TxnOp::Insert {
                    table: ins.table,
                    col_order: ins.columns,
                    rows: result.rows,
                });
                Ok(Response::Execution(Tag::new("INSERT").with_rows(n)))
            }
            StatementKind::InsertSelect(ins) => self.run_insert_select(&graph, ins).await,
            StatementKind::UpdateTable(upd) if in_txn => {
                self.buffer(TxnOp::Update {
                    table: upd.table,
                    set: upd.set,
                    selector: upd.selector.pred,
                });
                Ok(Response::Execution(Tag::new("UPDATE")))
            }
            StatementKind::UpdateTable(upd) => self.run_update_table(upd).await,
            StatementKind::DeleteTable(del) if in_txn => {
                self.buffer(TxnOp::Delete {
                    table: del.table,
                    selector: del.selector.pred,
                });
                Ok(Response::Execution(Tag::new("DELETE")))
            }
            StatementKind::DeleteTable(del) => self.run_delete_table(del).await,
            // `COPY … FROM STDIN` (CONCEPT:EG-020): switch the connection into copy-in
            // mode; the streamed rows are ingested in `on_copy_done`.
            // CONCEPT:EG-046 — INSERT INTO nodes … SELECT (facade dispatch).
            StatementKind::InsertNodesSelect(ins) => {
                self.run_insert_nodes_select(&graph, sql, ins, result_format).await
            }
            // CONCEPT:EG-047 — UPDATE nodes … FROM … / DELETE FROM nodes … USING … .
            StatementKind::UpdateNodesJoin(upd) => {
                self.run_update_nodes_join(&graph, sql, upd, result_format).await
            }
            StatementKind::DeleteNodesJoin(del) => {
                self.run_delete_nodes_join(&graph, sql, del, result_format).await
            }
            // CONCEPT:EG-072 — CREATE/DROP VIEW over the durable view catalog.
            StatementKind::CreateView(plan) => self.run_create_view(plan).await,
            StatementKind::DropView(plan) => self.run_drop_view(plan).await,
            StatementKind::CopyIn(plan) => self.start_copy(plan).await,
            // Transaction-control statements are handled above.
            StatementKind::Begin | StatementKind::Commit | StatementKind::Rollback => {
                unreachable!("transaction control handled before dispatch")
            }
        }
    }

    /// `COMMIT` (CONCEPT:EG-020): apply the open transaction's buffered ops in ONE redb
    /// write transaction (atomic — a constraint violation rolls the whole batch back).
    /// A `COMMIT` with no open transaction is a no-op (Postgres-compatible).
    async fn run_commit(&self) -> PgWireResult<Response> {
        let txn = match self.txn.lock().take() {
            Some(t) => t,
            None => return Ok(Response::Execution(Tag::new("COMMIT"))),
        };
        let store = user_table_store()?;
        tokio::task::spawn_blocking(move || store.commit_txn(&txn))
            .await
            .map_err(|e| user_err(format!("commit task failed: {e}")))?
            .map_err(user_err)?;
        Ok(Response::Execution(Tag::new("COMMIT")))
    }

    /// `COPY <table> [(cols…)] FROM STDIN` (CONCEPT:EG-020): resolve the target schema,
    /// stash the copy state, and return a `CopyIn` response so the client streams rows.
    async fn start_copy(&self, plan: CopyPlan) -> PgWireResult<Response> {
        let store = user_table_store()?;
        let table = plan.table.clone();
        let schema = tokio::task::spawn_blocking(move || store.get_schema(&table))
            .await
            .map_err(|e| user_err(format!("copy schema task failed: {e}")))?
            .map_err(user_err)?
            .ok_or_else(|| user_err(format!("table `{}` does not exist", plan.table)))?;
        // Resolve the insert column list: the COPY column list, or all columns in order.
        let columns: Vec<String> = if plan.columns.is_empty() {
            schema.columns.iter().map(|c| c.name.clone()).collect()
        } else {
            plan.columns.clone()
        };
        let ncols = columns.len();
        // pg copy format code: 0 = text/csv, 1 = binary.
        let fmt_code: i8 = if plan.format == CopyFormat::Binary {
            1
        } else {
            0
        };
        *self.copy.lock() = Some(CopyState {
            table: plan.table,
            columns,
            format: plan.format,
            delimiter: plan.delimiter,
            header: plan.header,
            buf: Vec::new(),
        });
        Ok(Response::CopyIn(pgwire::api::results::CopyResponse::new(
            fmt_code,
            ncols,
            futures::stream::empty(),
        )))
    }

    /// `CREATE TABLE` (CONCEPT:EG-018): record the schema in the durable redb table
    /// catalog. The whole DDL commits (commit-before-ack) before the tag is returned.
    async fn run_create_table(&self, plan: CreateTablePlan) -> PgWireResult<Response> {
        let columns = to_store_columns(&plan.columns)?;
        let schema = TableSchema {
            name: plan.name,
            columns,
        };
        let store = user_table_store()?;
        let if_not_exists = plan.if_not_exists;
        tokio::task::spawn_blocking(move || store.create_table(&schema, if_not_exists))
            .await
            .map_err(|e| user_err(format!("create table task failed: {e}")))?
            .map_err(user_err)?;
        Ok(Response::Execution(Tag::new("CREATE TABLE")))
    }

    /// `DROP TABLE` (CONCEPT:EG-018): remove the catalog entry + every row.
    async fn run_drop_table(&self, plan: DropTablePlan) -> PgWireResult<Response> {
        let store = user_table_store()?;
        tokio::task::spawn_blocking(move || store.drop_table(&plan.name, plan.if_exists))
            .await
            .map_err(|e| user_err(format!("drop table task failed: {e}")))?
            .map_err(user_err)?;
        Ok(Response::Execution(Tag::new("DROP TABLE")))
    }

    /// CONCEPT:EG-072 — `CREATE [OR REPLACE] VIEW name AS SELECT …`: persist the view
    /// text in the durable view catalog (commit-before-ack); `build_ctx` expands it on read.
    async fn run_create_view(&self, plan: CreateViewPlan) -> PgWireResult<Response> {
        let store = user_table_store()?;
        tokio::task::spawn_blocking(move || {
            store.create_view(&plan.name, &plan.select_sql, plan.or_replace)
        })
        .await
        .map_err(|e| user_err(format!("create view task failed: {e}")))?
        .map_err(user_err)?;
        Ok(Response::Execution(Tag::new("CREATE VIEW")))
    }

    /// CONCEPT:EG-072 — `DROP VIEW [IF EXISTS] name`.
    async fn run_drop_view(&self, plan: DropViewPlan) -> PgWireResult<Response> {
        let store = user_table_store()?;
        tokio::task::spawn_blocking(move || store.drop_view(&plan.name, plan.if_exists))
            .await
            .map_err(|e| user_err(format!("drop view task failed: {e}")))?
            .map_err(user_err)?;
        Ok(Response::Execution(Tag::new("DROP VIEW")))
    }

    /// A scalar cell coerced to the string node-id form the engine stores.
    fn cell_to_node_id(v: &serde_json::Value) -> PgWireResult<String> {
        match v {
            serde_json::Value::String(s) => Ok(s.clone()),
            serde_json::Value::Number(n) => Ok(n.to_string()),
            serde_json::Value::Bool(b) => Ok(b.to_string()),
            serde_json::Value::Null => Err(user_err("resolved a NULL `id` for a node write")),
            other => Err(user_err(format!("`id` must be a scalar, got {other}"))),
        }
    }

    /// CONCEPT:EG-046 — `INSERT INTO nodes (cols…) SELECT …`: run the SELECT through the
    /// read path, then materialize each row as a node (the `id` column → node id, the rest
    /// → properties), honoring `ON CONFLICT` (CONCEPT:EG-048). RETURNING optional.
    async fn run_insert_nodes_select(
        &self,
        graph: &str,
        sql: &str,
        ins: InsertNodesSelect,
        result_format: Option<&Format>,
    ) -> PgWireResult<Response> {
        let result = self.run_read(graph, ins.select_sql).await?;
        if result.columns.len() != ins.columns.len() {
            return Err(user_err(format!(
                "INSERT INTO nodes … SELECT column count mismatch: {} target columns, {} selected",
                ins.columns.len(),
                result.columns.len()
            )));
        }
        let id_pos = ins
            .columns
            .iter()
            .position(|c| c.eq_ignore_ascii_case("id"))
            .ok_or_else(|| user_err("INSERT INTO nodes … SELECT must include the `id` column"))?;
        let core = self.graph_core(graph).await?;
        let empty = serde_json::Map::new();
        let cond_blob = rmp_serde::to_vec_named(&serde_json::Value::Object(empty.clone()))
            .map_err(|e| user_err(format!("encode CAS conditions: {e}")))?;
        let mut affected: Vec<(String, serde_json::Map<String, serde_json::Value>)> = Vec::new();
        let mut n = 0usize;
        for row in result.rows {
            let node_id = Self::cell_to_node_id(&row[id_pos])?;
            let mut props = serde_json::Map::new();
            for (i, col) in ins.columns.iter().enumerate() {
                if i != id_pos {
                    props.insert(col.clone(), row[i].clone());
                }
            }
            // ON CONFLICT — conflict key is the node id.
            if core.has_node(&node_id) {
                match ins.on_conflict.as_ref().map(|oc| &oc.action) {
                    Some(OnConflictAction::DoNothing) => continue,
                    Some(OnConflictAction::DoUpdate(set)) => {
                        core.compare_and_set_fields(&node_id, &empty, set);
                        let upd_blob =
                            rmp_serde::to_vec_named(&serde_json::Value::Object(set.clone()))
                                .map_err(|e| user_err(format!("encode CAS updates: {e}")))?;
                        let method = crate::protocol::Method::CompareAndSetNodeFields {
                            node_id: node_id.clone(),
                            conditions_msgpack: cond_blob.clone(),
                            updates_msgpack: upd_blob,
                        };
                        self.record_durable_write(graph, &method).await?;
                        if ins.returning {
                            affected.push((node_id, set.clone()));
                        }
                        n += 1;
                        continue;
                    }
                    None => {} // no ON CONFLICT → overwrite (add_node semantics)
                }
            }
            let blob = rmp_serde::to_vec_named(&serde_json::Value::Object(props.clone()))
                .map_err(|e| user_err(format!("encode node properties: {e}")))?;
            core.add_node(node_id.clone(), blob.clone());
            let method = crate::protocol::Method::AddNode {
                node_id: node_id.clone(),
                properties_msgpack: blob,
            };
            self.record_durable_write(graph, &method).await?;
            if ins.returning {
                affected.push((node_id, props));
            }
            n += 1;
        }
        core.mark_dirty();
        if ins.returning {
            let (cols, types) = self.returning_cols(graph, sql).await?;
            Ok(Response::Query(query_response(
                returning_result(&affected, &cols, &types),
                result_format,
            )))
        } else {
            Ok(Response::Execution(Tag::new("INSERT").with_rows(n)))
        }
    }

    /// CONCEPT:EG-047 — `UPDATE nodes SET … FROM … WHERE …`: the classifier rendered a
    /// resolution `SELECT nodes.id, <set-exprs…> FROM …`; each row gives the matched id
    /// plus its per-row SET values. Applied via the serializable merge under the guard.
    async fn run_update_nodes_join(
        &self,
        graph: &str,
        sql: &str,
        upd: UpdateNodesJoin,
        result_format: Option<&Format>,
    ) -> PgWireResult<Response> {
        let result = self.run_read(graph, upd.resolve_sql).await?;
        if result.columns.len() != upd.set_targets.len() + 1 {
            return Err(user_err(format!(
                "UPDATE … FROM resolution shape mismatch: expected id + {} set columns, got {}",
                upd.set_targets.len(),
                result.columns.len()
            )));
        }
        let core = self.graph_core(graph).await?;
        let empty = serde_json::Map::new();
        let cond_blob = rmp_serde::to_vec_named(&serde_json::Value::Object(empty.clone()))
            .map_err(|e| user_err(format!("encode CAS conditions: {e}")))?;
        let mut affected: Vec<(String, serde_json::Map<String, serde_json::Value>)> = Vec::new();
        let mut seen = std::collections::HashSet::new();
        for row in result.rows {
            let id = Self::cell_to_node_id(&row[0])?;
            if !seen.insert(id.clone()) {
                continue; // a join can resolve the same id twice — apply once
            }
            let mut updates = serde_json::Map::new();
            for (i, col) in upd.set_targets.iter().enumerate() {
                updates.insert(col.clone(), row[i + 1].clone());
            }
            if core.compare_and_set_fields(&id, &empty, &updates) {
                let upd_blob = rmp_serde::to_vec_named(&serde_json::Value::Object(updates.clone()))
                    .map_err(|e| user_err(format!("encode CAS updates: {e}")))?;
                let method = crate::protocol::Method::CompareAndSetNodeFields {
                    node_id: id.clone(),
                    conditions_msgpack: cond_blob.clone(),
                    updates_msgpack: upd_blob,
                };
                self.record_durable_write(graph, &method).await?;
                if upd.returning {
                    affected.push((id, updates));
                }
            }
        }
        core.mark_dirty();
        let n = affected.len();
        if upd.returning {
            let (cols, types) = self.returning_cols(graph, sql).await?;
            Ok(Response::Query(query_response(
                returning_result(&affected, &cols, &types),
                result_format,
            )))
        } else {
            Ok(Response::Execution(Tag::new("UPDATE").with_rows(n)))
        }
    }

    /// CONCEPT:EG-047 — `DELETE FROM nodes USING … WHERE …`: the classifier rendered a
    /// resolution `SELECT nodes.id FROM … USING …`; remove each resolved node.
    async fn run_delete_nodes_join(
        &self,
        graph: &str,
        sql: &str,
        del: DeleteNodesJoin,
        result_format: Option<&Format>,
    ) -> PgWireResult<Response> {
        let result = self.run_read(graph, del.resolve_sql).await?;
        let core = self.graph_core(graph).await?;
        let mut affected: Vec<(String, serde_json::Map<String, serde_json::Value>)> = Vec::new();
        let mut seen = std::collections::HashSet::new();
        let mut n = 0usize;
        for row in result.rows {
            let id = Self::cell_to_node_id(&row[0])?;
            if !seen.insert(id.clone()) {
                continue;
            }
            if del.returning {
                let props = self.node_props(graph, &id).await?;
                affected.push((id.clone(), props));
            }
            core.remove_node(id.clone());
            let method = crate::protocol::Method::RemoveNode {
                node_id: id.clone(),
            };
            self.record_durable_write(graph, &method).await?;
            n += 1;
        }
        core.mark_dirty();
        if del.returning {
            let (cols, types) = self.returning_cols(graph, sql).await?;
            Ok(Response::Query(query_response(
                returning_result(&affected, &cols, &types),
                result_format,
            )))
        } else {
            Ok(Response::Execution(Tag::new("DELETE").with_rows(n)))
        }
    }

    /// `ALTER TABLE … ADD COLUMN` (CONCEPT:EG-018).
    async fn run_alter_table(&self, plan: AlterTablePlan) -> PgWireResult<Response> {
        let columns = to_store_columns(std::slice::from_ref(&plan.add_column))?;
        let column = columns.into_iter().next().expect("one column");
        let store = user_table_store()?;
        let table = plan.name;
        tokio::task::spawn_blocking(move || store.add_column(&table, column))
            .await
            .map_err(|e| user_err(format!("alter table task failed: {e}")))?
            .map_err(user_err)?;
        Ok(Response::Execution(Tag::new("ALTER TABLE")))
    }

    /// `INSERT INTO <user_table> … VALUES …` (CONCEPT:EG-018). Commit-before-ack.
    async fn run_insert_table(&self, ins: InsertTable) -> PgWireResult<Response> {
        let store = user_table_store()?;
        let n = tokio::task::spawn_blocking(move || {
            store.insert_rows(&ins.table, &ins.columns, &ins.rows)
        })
        .await
        .map_err(|e| user_err(format!("insert task failed: {e}")))?
        .map_err(user_err)?;
        Ok(Response::Execution(Tag::new("INSERT").with_rows(n)))
    }

    /// `INSERT INTO <user_table> (cols…) SELECT …` (CONCEPT:EG-018). Runs the SELECT
    /// through the SAME DataFusion path (so it can JOIN user tables AND the graph),
    /// then durably inserts the projected rows. The SELECT result column COUNT must
    /// match the insert column list.
    async fn run_insert_select(&self, graph: &str, ins: InsertSelect) -> PgWireResult<Response> {
        let result = self.run_read(graph, ins.select_sql).await?;
        if result.columns.len() != ins.columns.len() {
            return Err(user_err(format!(
                "INSERT … SELECT column count mismatch: {} target columns, {} selected",
                ins.columns.len(),
                result.columns.len()
            )));
        }
        let store = user_table_store()?;
        let rows = result.rows;
        let n =
            tokio::task::spawn_blocking(move || store.insert_rows(&ins.table, &ins.columns, &rows))
                .await
                .map_err(|e| user_err(format!("insert-select task failed: {e}")))?
                .map_err(user_err)?;
        Ok(Response::Execution(Tag::new("INSERT").with_rows(n)))
    }

    /// `UPDATE <user_table> SET … WHERE <predicate>` (CONCEPT:EG-018, compound
    /// predicate CONCEPT:EG-045). The store evaluates the predicate per row inside
    /// its redb write transaction (serializable).
    async fn run_update_table(&self, upd: UpdateTable) -> PgWireResult<Response> {
        let store = user_table_store()?;
        let pred = upd.selector.pred;
        let n =
            tokio::task::spawn_blocking(move || store.update_where(&upd.table, &upd.set, &pred))
                .await
                .map_err(|e| user_err(format!("update task failed: {e}")))?
                .map_err(user_err)?;
        Ok(Response::Execution(Tag::new("UPDATE").with_rows(n)))
    }

    /// `DELETE FROM <user_table> WHERE <predicate>` (CONCEPT:EG-018, compound
    /// predicate CONCEPT:EG-045).
    async fn run_delete_table(&self, del: DeleteTable) -> PgWireResult<Response> {
        let store = user_table_store()?;
        let pred = del.selector.pred;
        let n = tokio::task::spawn_blocking(move || store.delete_where(&del.table, &pred))
            .await
            .map_err(|e| user_err(format!("delete task failed: {e}")))?
            .map_err(user_err)?;
        Ok(Response::Execution(Tag::new("DELETE").with_rows(n)))
    }

    /// Resolve the candidate node ids a WHERE selects. `Id` is the fast path (one id,
    /// only included if the node exists). `Predicate` (CONCEPT:EG-045) re-runs the
    /// WHERE text through the SAME DataFusion read path as a `SELECT id FROM nodes
    /// WHERE …`, so any compound `AND`/`OR`/`IN`/`BETWEEN`/range predicate the read
    /// surface understands resolves the candidate set. These ids are then re-checked
    /// under the write guard (`compare_and_set_fields_if`/`remove_node_if`) for
    /// serializable semantics, so a row that changed between the read and the write is
    /// skipped.
    async fn matched_ids(&self, graph: &str, selector: &WhereEq) -> PgWireResult<Vec<String>> {
        match selector {
            WhereEq::Id(id) => {
                let core = self.graph_core(graph).await?;
                Ok(if core.has_node(id) {
                    vec![id.clone()]
                } else {
                    Vec::new()
                })
            }
            WhereEq::Predicate { where_sql, .. } => {
                let sql = format!("SELECT id FROM nodes WHERE {where_sql}");
                let result = self.run_read(graph, sql).await?;
                let id_col = result
                    .columns
                    .iter()
                    .position(|c| c.name.eq_ignore_ascii_case("id"))
                    .ok_or_else(|| user_err("predicate read did not return an `id` column"))?;
                let mut out = Vec::with_capacity(result.rows.len());
                for row in result.rows {
                    match row.get(id_col) {
                        Some(serde_json::Value::String(s)) => out.push(s.clone()),
                        Some(serde_json::Value::Number(n)) => out.push(n.to_string()),
                        _ => {}
                    }
                }
                Ok(out)
            }
        }
    }

    /// Read the current decoded property object for a node (post-write, for
    /// RETURNING). Missing/undecodable → an empty object.
    async fn node_props(
        &self,
        graph: &str,
        id: &str,
    ) -> PgWireResult<serde_json::Map<String, serde_json::Value>> {
        let core = self.graph_core(graph).await?;
        match core.get_node_properties(id) {
            Some(blob) => match rmp_serde::from_slice::<serde_json::Value>(&blob) {
                Ok(serde_json::Value::Object(o)) => Ok(o),
                _ => Ok(serde_json::Map::new()),
            },
            None => Ok(serde_json::Map::new()),
        }
    }

    /// `INSERT INTO nodes …` — single or multi-row (CONCEPT:KG-2.198). Each row is
    /// applied as an `AddNode` (the same write path `Method::AddNode` dispatch uses)
    /// and durably recorded via [`Self::record_durable_write`]. With `RETURNING`,
    /// the inserted rows are returned as a result set; otherwise a CommandComplete
    /// tag with the row count.
    /// The RETURNING projection columns for `sql` (in the SAME deterministic order
    /// the Describe step reports) PLUS the graph's `name → PgColType` schema map used
    /// to type them. Both are computed from the SAME inputs the Describe path uses, so
    /// a `RETURNING` write's DataRows always match the described `RowDescription` in
    /// both column NAMES and TYPES. Resolves `RETURNING *` against the known property
    /// columns (sorted for determinism).
    async fn returning_cols(
        &self,
        graph: &str,
        sql: &str,
    ) -> PgWireResult<(Vec<String>, std::collections::HashMap<String, PgColType>)> {
        let type_map = self.column_types(graph).await?;
        let cols = if eg_query::returning_columns(sql).is_some() {
            returning_projection(sql, &[])
        } else {
            let mut prop_cols: Vec<String> = type_map.keys().cloned().collect();
            prop_cols.sort(); // deterministic `RETURNING *` order
            returning_projection(sql, &prop_cols)
        };
        Ok((cols, type_map))
    }

    async fn run_insert(
        &self,
        graph: &str,
        sql: &str,
        ins: InsertNodes,
        result_format: Option<&Format>,
    ) -> PgWireResult<Response> {
        let core = self.graph_core(graph).await?;
        let mut affected: Vec<(String, serde_json::Map<String, serde_json::Value>)> = Vec::new();
        let n = ins.rows.len();
        for node in ins.rows {
            let props = node.properties.clone();
            let blob = rmp_serde::to_vec_named(&serde_json::Value::Object(node.properties))
                .map_err(|e| user_err(format!("encode node properties: {e}")))?;
            let node_id = node.node_id.clone();
            core.add_node(node.node_id, blob.clone());
            let method = crate::protocol::Method::AddNode {
                node_id: node_id.clone(),
                properties_msgpack: blob,
            };
            self.record_durable_write(graph, &method).await?;
            if ins.returning {
                affected.push((node_id, props));
            }
        }
        core.mark_dirty();
        if ins.returning {
            let (cols, types) = self.returning_cols(graph, sql).await?;
            Ok(Response::Query(query_response(
                returning_result(&affected, &cols, &types),
                result_format,
            )))
        } else {
            Ok(Response::Execution(Tag::new("INSERT").with_rows(n)))
        }
    }

    /// `UPDATE nodes SET … WHERE …` (CONCEPT:KG-2.198). Resolves the matched ids,
    /// then for each runs `compare_and_set_fields` with EMPTY conditions (an
    /// unconditional merge of the SET map — a single atomic read-modify-write per
    /// node under the topology write guard) and durably records the resulting
    /// `CompareAndSetNodeFields` method (the same durable Method a CAS dispatch
    /// records). With `RETURNING`, the post-update rows are returned.
    async fn run_update(
        &self,
        graph: &str,
        sql: &str,
        upd: UpdateNodes,
        result_format: Option<&Format>,
    ) -> PgWireResult<Response> {
        let ids = self.matched_ids(graph, &upd.selector).await?;
        let core = self.graph_core(graph).await?;
        let conditions = serde_json::Map::new();
        let updates = upd.set;
        let cond_blob = rmp_serde::to_vec_named(&serde_json::Value::Object(conditions.clone()))
            .map_err(|e| user_err(format!("encode CAS conditions: {e}")))?;
        let upd_blob = rmp_serde::to_vec_named(&serde_json::Value::Object(updates.clone()))
            .map_err(|e| user_err(format!("encode CAS updates: {e}")))?;
        // CONCEPT:EG-045 — when the WHERE is a compound predicate, re-check it under
        // the write guard so a row that changed between candidate resolution and the
        // write is skipped (serializable). The `id` fast path has no predicate.
        let pred = match &upd.selector {
            WhereEq::Predicate { pred, .. } => Some(pred.clone()),
            WhereEq::Id(_) => None,
        };
        let mut affected_ids = Vec::new();
        for id in ids {
            let applied = match &pred {
                Some(p) => core.compare_and_set_fields_if(&id, p, &conditions, &updates),
                None => core.compare_and_set_fields(&id, &conditions, &updates),
            };
            if applied {
                let method = crate::protocol::Method::CompareAndSetNodeFields {
                    node_id: id.clone(),
                    conditions_msgpack: cond_blob.clone(),
                    updates_msgpack: upd_blob.clone(),
                };
                self.record_durable_write(graph, &method).await?;
                affected_ids.push(id);
            }
        }
        core.mark_dirty();
        let n = affected_ids.len();
        if upd.returning {
            let mut affected = Vec::with_capacity(n);
            for id in affected_ids {
                let props = self.node_props(graph, &id).await?;
                affected.push((id, props));
            }
            let (cols, types) = self.returning_cols(graph, sql).await?;
            Ok(Response::Query(query_response(
                returning_result(&affected, &cols, &types),
                result_format,
            )))
        } else {
            Ok(Response::Execution(Tag::new("UPDATE").with_rows(n)))
        }
    }

    /// `DELETE FROM nodes WHERE …` (CONCEPT:KG-2.198). Resolves the matched ids
    /// (capturing each node's properties FIRST when RETURNING is requested, since
    /// the row is gone after removal), removes each via `remove_node` (the same
    /// write path `Method::RemoveNode` dispatch uses) under its one-shot txn, and
    /// durably records each `RemoveNode`.
    async fn run_delete(
        &self,
        graph: &str,
        sql: &str,
        del: DeleteNodes,
        result_format: Option<&Format>,
    ) -> PgWireResult<Response> {
        let ids = self.matched_ids(graph, &del.selector).await?;
        // Snapshot properties AND resolve the RETURNING projection BEFORE removal
        // (both read live node data, which is gone after the delete).
        let mut affected: Vec<(String, serde_json::Map<String, serde_json::Value>)> = Vec::new();
        let returning_cols = if del.returning {
            for id in &ids {
                let props = self.node_props(graph, id).await?;
                affected.push((id.clone(), props));
            }
            Some(self.returning_cols(graph, sql).await?)
        } else {
            None
        };
        // CONCEPT:EG-045 — re-check a compound predicate under the write guard so a
        // row that changed between candidate resolution and removal is skipped
        // (serializable). The `id` fast path has no predicate.
        let pred = match &del.selector {
            WhereEq::Predicate { pred, .. } => Some(pred.clone()),
            WhereEq::Id(_) => None,
        };
        let core = self.graph_core(graph).await?;
        let mut removed_ids = Vec::new();
        for id in ids {
            let removed = match &pred {
                Some(p) => core.remove_node_if(&id, p),
                None => {
                    core.remove_node(id.clone());
                    true
                }
            };
            if removed {
                let method = crate::protocol::Method::RemoveNode {
                    node_id: id.clone(),
                };
                self.record_durable_write(graph, &method).await?;
                removed_ids.push(id);
            }
        }
        core.mark_dirty();
        let n = removed_ids.len();
        if let Some((cols, types)) = returning_cols {
            // Keep only the snapshots of rows actually removed (a gated predicate may
            // have skipped some candidates).
            affected.retain(|(id, _)| removed_ids.contains(id));
            Ok(Response::Query(query_response(
                returning_result(&affected, &cols, &types),
                result_format,
            )))
        } else {
            Ok(Response::Execution(Tag::new("DELETE").with_rows(n)))
        }
    }

    /// Record an applied pgwire DATA mutation durably — REPLICATES the post-write
    /// durable-record block in `dispatch::dispatch_request` (the source of truth;
    /// see `src/server/dispatch.rs`, the `record_method` / `redb_authoritative`
    /// block). M8's original pgwire write path only did `add_node` + `mark_dirty`,
    /// so under `EPISTEMIC_GRAPH_REDB_AUTHORITATIVE` a wire write was acked
    /// (CommandComplete) before it durably committed → crash = silent loss. This
    /// closes that gap by running the IDENTICAL two-regime logic dispatch uses:
    ///
    ///   * NOT authoritative (default): write-BEHIND — `record()` is fire-and-forget
    ///     (hand to the off-reactor writer, no await). Byte-for-byte today's pgwire
    ///     behavior plus the WAL/backend `record()` the M8 NOTE flagged as missing.
    ///   * AUTHORITATIVE: commit-BEFORE-ACK — AWAIT `record_durable` (group-commit +
    ///     fsync, coalescing with concurrent writers) BEFORE returning Ok. A durable
    ///     commit FAILURE returns a pgwire error: the caller never gets a
    ///     CommandComplete for a write that is not on disk.
    ///
    /// Reads `persistence` + `redb_authoritative` once under the same registry lock
    /// dispatch reads them under. Applies to EVERY durable DML the wire path emits —
    /// `AddNode` (INSERT), `CompareAndSetNodeFields` (UPDATE), `RemoveNode`
    /// (DELETE) — all of which `is_durable_mutation`.
    async fn record_durable_write(
        &self,
        graph: &str,
        method: &crate::protocol::Method,
    ) -> PgWireResult<()> {
        let (persistence, redb_authoritative) = {
            let s = self.state.read().await;
            (s.persistence.clone(), s.redb_authoritative)
        };
        // No durable backend configured (no persist dir) → in-memory only, nothing
        // to record. Matches dispatch (`record_method` is None when persistence is
        // None), so the cache-only default path is unchanged.
        let Some(p) = persistence else {
            return Ok(());
        };
        // Only durable DATA mutations are recorded — the same guard dispatch applies
        // via `is_durable_mutation`.
        if !crate::wal::is_durable_mutation(method) {
            return Ok(());
        }
        let fname = crate::persist::sanitize(graph);
        if redb_authoritative {
            if let Err(e) = p.record_durable(&fname, method).await {
                tracing::error!(
                    "pgwire redb authoritative: durable commit FAILED for graph '{}': {} — \
                     returning error (write not acked)",
                    graph,
                    e
                );
                return Err(user_err(format!(
                    "durable commit failed (write not acknowledged): {e}"
                )));
            }
        } else {
            p.record(&fname, method);
        }
        Ok(())
    }

    /// Build a `column name → PgColType` map for the current graph by sampling node
    /// property blobs (CONCEPT:KG-2.197 describe support). Used to resolve the type
    /// of a parameter that sits opposite a property column (`WHERE rank > $1`), and
    /// to type a RETURNING result column, WITHOUT a DataFusion round-trip. Schema-
    /// on-read: the first non-null value seen for a column wins (matching the
    /// inference `exec_sql_typed` does). `id` is always TEXT.
    async fn column_types(
        &self,
        graph: &str,
    ) -> PgWireResult<std::collections::HashMap<String, PgColType>> {
        let core = self.graph_core(graph).await?;
        let mut map: std::collections::HashMap<String, PgColType> =
            std::collections::HashMap::new();
        map.insert("id".to_string(), PgColType::Text);
        for (_, blob) in core.get_nodes() {
            let Ok(serde_json::Value::Object(obj)) =
                rmp_serde::from_slice::<serde_json::Value>(&blob)
            else {
                continue;
            };
            for (k, v) in obj {
                if v.is_null() {
                    continue;
                }
                map.entry(k).or_insert_with(|| col_type_of(&v));
            }
        }
        Ok(map)
    }

    /// Resolve the wire `Type` OIDs for a statement's `$N` parameters
    /// (CONCEPT:KG-2.197). Locates each param via `eg_query::infer_param_sites` (a
    /// column / `id` / literal site), then types it: an `id` site → TEXT; a column
    /// site → that column's type from [`Self::column_types`]; a literal site → its
    /// directly-derived type. A column with no observed type defaults to TEXT.
    /// Reporting CONCRETE OIDs (not UNKNOWN) is what lets a real driver
    /// (tokio-postgres/psycopg/asyncpg) encode a typed parameter — UNKNOWN is
    /// rejected client-side as `WrongType`.
    async fn param_type_oids(&self, graph: &str, sql: &str) -> PgWireResult<Vec<Type>> {
        let sites = match eg_query::infer_param_sites(sql) {
            Ok(s) => s,
            Err(_) => return Ok(Vec::new()),
        };
        if sites.is_empty() {
            return Ok(Vec::new());
        }
        let cols = self.column_types(graph).await?;
        let mut out = Vec::with_capacity(sites.len());
        for site in sites {
            let ty = match site {
                eg_query::ParamSite::IdColumn => PgColType::Text,
                eg_query::ParamSite::Column(name) => {
                    cols.get(&name).copied().unwrap_or(PgColType::Text)
                }
                eg_query::ParamSite::Literal(lt) => match lt {
                    eg_query::ParamLiteralType::Int => PgColType::Int8,
                    eg_query::ParamLiteralType::Float => PgColType::Float8,
                    eg_query::ParamLiteralType::Bool => PgColType::Bool,
                    eg_query::ParamLiteralType::Text => PgColType::Text,
                },
            };
            out.push(pg_type(ty));
        }
        Ok(out)
    }

    /// Derive the result-set `FieldInfo`s a statement will produce, for the Describe
    /// step (CONCEPT:KG-2.197) — WITHOUT mutating state. The extended protocol sends
    /// the `RowDescription` from Describe (not from Execute), so a wrong/empty schema
    /// here makes the client miscount DataRow fields. `sql` must already have any
    /// bound params substituted (portal describe) or `$N`→`NULL` (statement describe).
    ///   * Read → run the SAME `exec_sql_typed` read path and report its typed
    ///     columns (the real schema-on-read result schema).
    ///   * Write with a NAMED `RETURNING` list → report those columns, typed from the
    ///     node column-type map (no execution, no side effect).
    ///   * Write without RETURNING (or `RETURNING *`) → no result columns.
    async fn describe_result_columns(
        &self,
        graph: &str,
        sql: &str,
    ) -> PgWireResult<Vec<FieldInfo>> {
        match eg_query::classify(sql) {
            Ok(StatementKind::Read) => {
                // Derive the schema from a PROBE form (WHERE/LIMIT dropped) so the
                // engine's schema-on-read path always sees rows — a filtered query
                // matching ZERO rows can otherwise lose its column schema, which would
                // make the described `RowDescription` disagree with the executed
                // DataRows. The probe keeps the projection/FROM/GROUP BY, so the
                // columns are identical to the real query.
                let probe = eg_query::schema_probe_sql(sql).unwrap_or_else(|| sql.to_string());
                let result = self.run_read(graph, probe).await?;
                Ok(field_defs(&result, None).as_ref().clone())
            }
            Ok(StatementKind::InsertNodes(InsertNodes { returning, .. }))
            | Ok(StatementKind::UpdateNodes(UpdateNodes { returning, .. }))
            | Ok(StatementKind::DeleteNodes(DeleteNodes { returning, .. })) => {
                // A write. Only a RETURNING write yields a result set. Compute the
                // EXACT same projection + types the execute path will (via the shared
                // `returning_cols`), so Describe and Execute never disagree.
                if !returning {
                    return Ok(Vec::new());
                }
                let (cols, types) = self.returning_cols(graph, sql).await?;
                Ok(cols
                    .into_iter()
                    .map(|name| {
                        let ty = if name == "id" {
                            PgColType::Text
                        } else {
                            types.get(&name).copied().unwrap_or(PgColType::Text)
                        };
                        FieldInfo::new(
                            name,
                            None,
                            None,
                            pg_type(ty),
                            pgwire::api::results::FieldFormat::Text,
                        )
                    })
                    .collect())
            }
            // DDL / user-table DML (CONCEPT:EG-018) → no result columns (like a
            // non-RETURNING write); and unclassifiable (e.g. SET graph) → none either.
            Ok(_) | Err(_) => Ok(Vec::new()),
        }
    }
}

#[async_trait]
impl SimpleQueryHandler for EngineBackend {
    async fn do_query<C>(&self, client: &mut C, query: &str) -> PgWireResult<Vec<Response>>
    where
        C: ClientInfo
            + pgwire::api::ClientPortalStore
            + futures::Sink<pgwire::messages::PgWireBackendMessage>
            + Unpin
            + Send
            + Sync,
        C::PortalStore: pgwire::api::store::PortalStore,
        C::Error: std::fmt::Debug,
        PgWireError: From<<C as futures::Sink<pgwire::messages::PgWireBackendMessage>>::Error>,
    {
        // On the first query, adopt the libpq `database` startup parameter as the
        // target graph (priority 1) — readable only now that startup has completed.
        self.resolve_startup_graph(client);
        // Simple query: the unified TEXT wire format (no per-column format codes).
        let resp = self.execute_sql(query, None).await?;
        Ok(vec![resp])
    }
}

// ── COPY … FROM STDIN handler (CONCEPT:EG-020) ───────────────────────────────

#[async_trait]
impl pgwire::api::copy::CopyHandler for EngineBackend {
    /// Accumulate a `CopyData` frame's bytes into the per-connection copy buffer.
    async fn on_copy_data<C>(
        &self,
        _client: &mut C,
        copy_data: pgwire::messages::copy::CopyData,
    ) -> PgWireResult<()>
    where
        C: ClientInfo + futures::Sink<pgwire::messages::PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: std::fmt::Debug,
        PgWireError: From<<C as futures::Sink<pgwire::messages::PgWireBackendMessage>>::Error>,
    {
        if let Some(state) = self.copy.lock().as_mut() {
            state.buf.extend_from_slice(copy_data.data.as_ref());
            Ok(())
        } else {
            Err(user_err("COPY data received with no COPY in progress"))
        }
    }

    /// Decode the accumulated copy body, durably ingest the rows, then complete the
    /// command (CommandComplete `COPY n` + ReadyForQuery — the protocol makes this the
    /// copy handler's responsibility once copy-in mode is entered).
    async fn on_copy_done<C>(
        &self,
        client: &mut C,
        _done: pgwire::messages::copy::CopyDone,
    ) -> PgWireResult<()>
    where
        C: ClientInfo + futures::Sink<pgwire::messages::PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: std::fmt::Debug,
        PgWireError: From<<C as futures::Sink<pgwire::messages::PgWireBackendMessage>>::Error>,
    {
        use futures::SinkExt;
        let state = self
            .copy
            .lock()
            .take()
            .ok_or_else(|| user_err("COPY done with no COPY in progress"))?;
        let store = user_table_store()?;
        let schema = store
            .get_schema(&state.table)
            .map_err(user_err)?
            .ok_or_else(|| user_err(format!("table `{}` does not exist", state.table)))?;
        let rows = decode_copy_rows(&state, &schema).map_err(user_err)?;
        let table = state.table.clone();
        let columns = state.columns.clone();
        let n = tokio::task::spawn_blocking(move || store.insert_rows(&table, &columns, &rows))
            .await
            .map_err(|e| user_err(format!("copy insert task failed: {e}")))?
            .map_err(user_err)?;

        // Complete the copy: CommandComplete then ReadyForQuery (the socket loop does
        // not emit these while the connection is in copy-in mode).
        let tag = Tag::new("COPY").with_rows(n);
        client
            .send(pgwire::messages::PgWireBackendMessage::CommandComplete(
                tag.into(),
            ))
            .await?;
        client.set_state(pgwire::api::PgWireConnectionState::ReadyForQuery);
        pgwire::api::query::send_ready_for_query(
            client,
            pgwire::messages::response::TransactionStatus::Idle,
        )
        .await?;
        Ok(())
    }
}

/// Decode a `COPY … FROM STDIN` body into typed rows aligned to `state.columns`
/// (CONCEPT:EG-020). Supports the Postgres TEXT, CSV, and BINARY formats; each field
/// is coerced to its target column's [`ColumnType`] so the store's typed insert path
/// accepts it (and SERIAL/DEFAULT fill any column the COPY omits).
fn decode_copy_rows(
    state: &CopyState,
    schema: &TableSchema,
) -> Result<Vec<Vec<serde_json::Value>>, String> {
    // The declared type of each COPY target column.
    let mut types = Vec::with_capacity(state.columns.len());
    for name in &state.columns {
        let col = schema
            .columns
            .iter()
            .find(|c| &c.name == name)
            .ok_or_else(|| format!("COPY column `{name}` does not exist in `{}`", state.table))?;
        types.push(col.ty);
    }

    match state.format {
        CopyFormat::Binary => decode_copy_binary(&state.buf, &types),
        CopyFormat::Csv | CopyFormat::Text => {
            let is_csv = state.format == CopyFormat::Csv;
            let delim = state.delimiter.unwrap_or(if is_csv { ',' } else { '\t' });
            let text = std::str::from_utf8(&state.buf)
                .map_err(|e| format!("COPY body is not valid UTF-8: {e}"))?;
            let mut out = Vec::new();
            for (li, raw_line) in text.split('\n').enumerate() {
                let line = raw_line.strip_suffix('\r').unwrap_or(raw_line);
                // Text format terminates on a `\.` sentinel line; skip a trailing blank.
                if line == "\\." {
                    break;
                }
                if line.is_empty() {
                    continue;
                }
                if is_csv && state.header && li == 0 {
                    continue; // skip the header row
                }
                let fields = if is_csv {
                    parse_csv_line(line, delim)
                } else {
                    parse_text_line(line, delim)
                };
                if fields.len() != types.len() {
                    return Err(format!(
                        "COPY row has {} fields, expected {}",
                        fields.len(),
                        types.len()
                    ));
                }
                let mut row = Vec::with_capacity(types.len());
                for (f, ty) in fields.iter().zip(types.iter()) {
                    row.push(copy_field_to_value(f.as_deref(), *ty)?);
                }
                out.push(row);
            }
            Ok(out)
        }
    }
}

/// One Postgres TEXT-format line → fields (`\N` ⇒ NULL; `\t`/`\n`/`\\` unescaped).
fn parse_text_line(line: &str, delim: char) -> Vec<Option<String>> {
    line.split(delim)
        .map(|raw| {
            if raw == "\\N" {
                None
            } else {
                Some(unescape_text(raw))
            }
        })
        .collect()
}

/// Unescape the Postgres TEXT-format backslash sequences in a field.
fn unescape_text(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut chars = s.chars();
    while let Some(c) = chars.next() {
        if c == '\\' {
            match chars.next() {
                Some('t') => out.push('\t'),
                Some('n') => out.push('\n'),
                Some('r') => out.push('\r'),
                Some('\\') => out.push('\\'),
                Some(other) => out.push(other),
                None => out.push('\\'),
            }
        } else {
            out.push(c);
        }
    }
    out
}

/// One CSV line → fields, honouring `"`-quoting with `""` escapes (no embedded
/// newlines). An empty UNQUOTED field is NULL; an empty QUOTED field (`""`) is "".
fn parse_csv_line(line: &str, delim: char) -> Vec<Option<String>> {
    let mut out = Vec::new();
    let chars: Vec<char> = line.chars().collect();
    let mut i = 0;
    while i <= chars.len() {
        // Parse one field starting at i.
        let mut field = String::new();
        let mut was_quoted = false;
        if i < chars.len() && chars[i] == '"' {
            was_quoted = true;
            i += 1;
            while i < chars.len() {
                if chars[i] == '"' {
                    if i + 1 < chars.len() && chars[i + 1] == '"' {
                        field.push('"');
                        i += 2;
                    } else {
                        i += 1; // closing quote
                        break;
                    }
                } else {
                    field.push(chars[i]);
                    i += 1;
                }
            }
        } else {
            while i < chars.len() && chars[i] != delim {
                field.push(chars[i]);
                i += 1;
            }
        }
        if was_quoted {
            out.push(Some(field));
        } else if field.is_empty() {
            out.push(None); // empty unquoted ⇒ NULL
        } else {
            out.push(Some(field));
        }
        if i < chars.len() && chars[i] == delim {
            i += 1;
            if i == chars.len() {
                // trailing delimiter ⇒ one more empty field
                out.push(None);
                break;
            }
            continue;
        }
        break;
    }
    out
}

/// Coerce a decoded text/csv field to a JSON value of the target column type so the
/// store's typed insert accepts it. `None` ⇒ JSON null.
fn copy_field_to_value(field: Option<&str>, ty: ColumnType) -> Result<serde_json::Value, String> {
    use serde_json::Value;
    let Some(s) = field else {
        return Ok(Value::Null);
    };
    let v = match ty {
        ColumnType::Int | ColumnType::BigInt | ColumnType::Timestamp => Value::Number(
            s.trim()
                .parse::<i64>()
                .map_err(|_| format!("invalid integer `{s}`"))?
                .into(),
        ),
        ColumnType::Float | ColumnType::Double => serde_json::Number::from_f64(
            s.trim()
                .parse::<f64>()
                .map_err(|_| format!("invalid float `{s}`"))?,
        )
        .map(Value::Number)
        .ok_or_else(|| format!("non-finite float `{s}`"))?,
        ColumnType::Bool => match s.trim().to_ascii_lowercase().as_str() {
            "t" | "true" | "1" | "y" | "yes" => Value::Bool(true),
            "f" | "false" | "0" | "n" | "no" => Value::Bool(false),
            other => return Err(format!("invalid boolean `{other}`")),
        },
        ColumnType::Json => serde_json::from_str(s).unwrap_or(Value::String(s.to_string())),
        ColumnType::Text | ColumnType::Bytes => Value::String(s.to_string()),
    };
    Ok(v)
}

/// Decode the Postgres BINARY COPY format into typed rows (CONCEPT:EG-020). Parses the
/// 11-byte `PGCOPY` signature + flags + header extension, then per row a 2-byte field
/// count (`-1` ⇒ the trailer) and per field a 4-byte length (`-1` ⇒ NULL) + bytes
/// decoded by the target column's type (the common scalar widths).
fn decode_copy_binary(
    buf: &[u8],
    types: &[ColumnType],
) -> Result<Vec<Vec<serde_json::Value>>, String> {
    use serde_json::Value;
    const SIG: &[u8] = b"PGCOPY\n\xff\r\n\0";
    let mut p = 0usize;
    let need = |p: usize, n: usize| -> Result<(), String> {
        if p + n > buf.len() {
            Err("truncated COPY BINARY stream".to_string())
        } else {
            Ok(())
        }
    };
    need(p, SIG.len())?;
    if &buf[..SIG.len()] != SIG {
        return Err("bad COPY BINARY signature".to_string());
    }
    p += SIG.len();
    need(p, 8)?; // 4-byte flags + 4-byte header-extension length
    let ext_len = u32::from_be_bytes(buf[p + 4..p + 8].try_into().unwrap()) as usize;
    p += 8 + ext_len;

    let mut out = Vec::new();
    loop {
        need(p, 2)?;
        let fcount = i16::from_be_bytes(buf[p..p + 2].try_into().unwrap());
        p += 2;
        if fcount == -1 {
            break; // trailer
        }
        if fcount as usize != types.len() {
            return Err(format!(
                "COPY BINARY row has {fcount} fields, expected {}",
                types.len()
            ));
        }
        let mut row = Vec::with_capacity(types.len());
        for ty in types {
            need(p, 4)?;
            let flen = i32::from_be_bytes(buf[p..p + 4].try_into().unwrap());
            p += 4;
            if flen == -1 {
                row.push(Value::Null);
                continue;
            }
            let flen = flen as usize;
            need(p, flen)?;
            let bytes = &buf[p..p + flen];
            p += flen;
            row.push(decode_binary_field(bytes, *ty)?);
        }
        out.push(row);
    }
    Ok(out)
}

/// Decode one BINARY-format field's bytes to a typed JSON value.
fn decode_binary_field(bytes: &[u8], ty: ColumnType) -> Result<serde_json::Value, String> {
    use serde_json::Value;
    let int = |b: &[u8]| -> Result<i64, String> {
        Ok(match b.len() {
            1 => b[0] as i8 as i64,
            2 => i16::from_be_bytes(b.try_into().unwrap()) as i64,
            4 => i32::from_be_bytes(b.try_into().unwrap()) as i64,
            8 => i64::from_be_bytes(b.try_into().unwrap()),
            n => return Err(format!("unexpected {n}-byte integer field")),
        })
    };
    let v = match ty {
        ColumnType::Int | ColumnType::BigInt | ColumnType::Timestamp => {
            Value::Number(int(bytes)?.into())
        }
        ColumnType::Float | ColumnType::Double => {
            let f = match bytes.len() {
                4 => f32::from_be_bytes(bytes.try_into().unwrap()) as f64,
                8 => f64::from_be_bytes(bytes.try_into().unwrap()),
                n => return Err(format!("unexpected {n}-byte float field")),
            };
            serde_json::Number::from_f64(f)
                .map(Value::Number)
                .ok_or("non-finite float")?
        }
        ColumnType::Bool => Value::Bool(bytes.first().copied().unwrap_or(0) != 0),
        ColumnType::Bytes => {
            Value::Array(bytes.iter().map(|b| Value::Number((*b).into())).collect())
        }
        ColumnType::Text | ColumnType::Json => {
            let s =
                std::str::from_utf8(bytes).map_err(|e| format!("invalid utf8 text field: {e}"))?;
            if ty == ColumnType::Json {
                serde_json::from_str(s).unwrap_or(Value::String(s.to_string()))
            } else {
                Value::String(s.to_string())
            }
        }
    };
    Ok(v)
}

/// A prepared statement parsed at `Parse` time (CONCEPT:KG-2.197). Holds the raw
/// SQL (with `$N` placeholders intact) and the count of distinct parameters so the
/// `Bind` step can validate and the describe step can report a `ParameterDescription`.
#[derive(Debug, Clone)]
struct PreparedStatement {
    sql: String,
    param_count: usize,
}

/// The extended-protocol SQL parser. Stateless: `parse_sql` records the raw SQL and
/// counts `$N` placeholders. It does NOT type-resolve params or result columns
/// up-front (the engine is schema-on-read), so `get_parameter_types` reports the
/// client-supplied/`UNKNOWN` types and `get_result_schema` reports an empty schema
/// — the actual `RowDescription` is emitted from the executed result, which the
/// real drivers (tokio-postgres/psycopg/asyncpg) accept.
#[derive(Debug)]
struct EngineQueryParser;

/// Count the distinct `$N` placeholders in a SQL string (the parameter count a
/// `ParameterDescription` reports). Scans for `$` followed by one or more ASCII
/// digits, tracking the max N seen (Postgres params are 1-based and dense in
/// practice; max-N is the conventional count). Skips `$` inside single-quoted
/// string literals so a literal `'$1'` is not miscounted.
fn count_params(sql: &str) -> usize {
    let bytes = sql.as_bytes();
    let mut i = 0usize;
    let mut max_n = 0usize;
    let mut in_str = false;
    while i < bytes.len() {
        let b = bytes[i];
        if in_str {
            if b == b'\'' {
                // Doubled '' is an escaped quote inside the literal.
                if i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                    i += 2;
                    continue;
                }
                in_str = false;
            }
            i += 1;
            continue;
        }
        if b == b'\'' {
            in_str = true;
            i += 1;
            continue;
        }
        if b == b'$' && i + 1 < bytes.len() && bytes[i + 1].is_ascii_digit() {
            let mut j = i + 1;
            let mut n = 0usize;
            while j < bytes.len() && bytes[j].is_ascii_digit() {
                n = n * 10 + (bytes[j] - b'0') as usize;
                j += 1;
            }
            if n > max_n {
                max_n = n;
            }
            i = j;
            continue;
        }
        i += 1;
    }
    max_n
}

#[async_trait]
impl QueryParser for EngineQueryParser {
    type Statement = PreparedStatement;

    async fn parse_sql<C>(
        &self,
        _client: &C,
        sql: &str,
        _types: &[Option<Type>],
    ) -> PgWireResult<Self::Statement>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        Ok(PreparedStatement {
            sql: sql.to_owned(),
            param_count: count_params(sql),
        })
    }

    fn get_parameter_types(&self, stmt: &Self::Statement) -> PgWireResult<Vec<Type>> {
        // Schema-on-read: we don't statically resolve parameter types. Report each
        // as UNKNOWN so the client (and `Bind`'s typed decode) drives the type.
        Ok(vec![Type::UNKNOWN; stmt.param_count])
    }

    fn get_result_schema(
        &self,
        _stmt: &Self::Statement,
        _column_format: Option<&Format>,
    ) -> PgWireResult<Vec<FieldInfo>> {
        // The result schema is known only after execution (schema-on-read). Return
        // empty; the real `RowDescription` is sent with the executed result. Real
        // drivers (tokio-postgres/psycopg/asyncpg) describe via the executed rows.
        Ok(vec![])
    }
}

/// Render a bound parameter (already decoded from the wire by the typed-decode
/// ladder) as a SQL literal to splice into the statement. NULL → `NULL`; strings
/// are single-quoted with `'` doubled; numbers/bools render canonically.
fn json_to_sql_literal(v: &serde_json::Value) -> String {
    use serde_json::Value;
    match v {
        Value::Null => "NULL".to_string(),
        Value::Bool(b) => {
            if *b {
                "TRUE".to_string()
            } else {
                "FALSE".to_string()
            }
        }
        Value::Number(n) => n.to_string(),
        Value::String(s) => format!("'{}'", s.replace('\'', "''")),
        // Arrays/objects: pass as a quoted JSON text literal (TEXT column).
        other => format!("'{}'", other.to_string().replace('\'', "''")),
    }
}

/// Decode the `idx`-th bound parameter from a portal into a JSON value, trying the
/// common Postgres types in turn (the formats psycopg3/asyncpg/JDBC/sqlx send for
/// int/float/text/bool). `Portal::parameter` handles BOTH the text and binary wire
/// formats per the portal's `parameter_format`, so this serves both. A NULL
/// parameter (`None`) decodes to JSON null. Unrecognized types fall back to a TEXT
/// decode so an exotic type still binds as its text rendering.
fn decode_param<S>(portal: &Portal<S>, idx: usize) -> PgWireResult<serde_json::Value>
where
    S: Clone,
{
    // Try INT8, then INT4, then FLOAT8, FLOAT4, BOOL, finally TEXT. The first type
    // whose decode succeeds wins; a NULL value short-circuits to JSON null.
    macro_rules! try_as {
        ($t:ty, $pgty:expr, $conv:expr) => {
            match portal.parameter::<$t>(idx, &$pgty) {
                Ok(Some(v)) => return Ok($conv(v)),
                Ok(None) => return Ok(serde_json::Value::Null),
                Err(_) => {}
            }
        };
    }
    try_as!(i64, Type::INT8, |v: i64| serde_json::Value::Number(
        v.into()
    ));
    try_as!(i32, Type::INT4, |v: i32| serde_json::Value::Number(
        (v as i64).into()
    ));
    try_as!(f64, Type::FLOAT8, |v: f64| serde_json::Number::from_f64(v)
        .map(serde_json::Value::Number)
        .unwrap_or(serde_json::Value::Null));
    try_as!(f32, Type::FLOAT4, |v: f32| serde_json::Number::from_f64(
        v as f64
    )
    .map(serde_json::Value::Number)
    .unwrap_or(serde_json::Value::Null));
    try_as!(bool, Type::BOOL, serde_json::Value::Bool);
    // Final fallback: TEXT.
    match portal.parameter::<String>(idx, &Type::TEXT) {
        Ok(Some(s)) => Ok(serde_json::Value::String(s)),
        Ok(None) => Ok(serde_json::Value::Null),
        Err(e) => Err(e),
    }
}

/// Substitute the portal's bound parameters into the prepared SQL, replacing each
/// `$N` placeholder with its bound value rendered as a SQL literal
/// (CONCEPT:KG-2.197). This is what lets the extended protocol reuse the EXACT
/// simple-query classify → read/write path: after substitution the statement is a
/// plain literal SQL string, identical to what `psql` would send. Scans the SQL
/// once, skipping `$N` inside single-quoted string literals so a literal `'$1'` is
/// left intact.
fn substitute_params<S>(sql: &str, portal: &Portal<S>) -> PgWireResult<String>
where
    S: Clone,
{
    let bytes = sql.as_bytes();
    let mut out = String::with_capacity(sql.len());
    let mut i = 0usize;
    let mut in_str = false;
    while i < bytes.len() {
        let b = bytes[i];
        if in_str {
            out.push(b as char);
            if b == b'\'' {
                if i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                    out.push('\'');
                    i += 2;
                    continue;
                }
                in_str = false;
            }
            i += 1;
            continue;
        }
        if b == b'\'' {
            in_str = true;
            out.push('\'');
            i += 1;
            continue;
        }
        if b == b'$' && i + 1 < bytes.len() && bytes[i + 1].is_ascii_digit() {
            let mut j = i + 1;
            let mut n = 0usize;
            while j < bytes.len() && bytes[j].is_ascii_digit() {
                n = n * 10 + (bytes[j] - b'0') as usize;
                j += 1;
            }
            if n == 0 {
                return Err(user_err("parameter placeholders are 1-based ($1, $2, …)"));
            }
            let val = decode_param(portal, n - 1)?;
            out.push_str(&json_to_sql_literal(&val));
            i = j;
            continue;
        }
        // Non-ASCII bytes pass through faithfully (we operate on raw bytes here).
        out.push(b as char);
        i += 1;
    }
    Ok(out)
}

#[async_trait]
impl ExtendedQueryHandler for EngineBackend {
    type Statement = PreparedStatement;
    type QueryParser = EngineQueryParser;

    fn query_parser(&self) -> Arc<Self::QueryParser> {
        self.parser.clone()
    }

    /// Execute a bound portal (CONCEPT:KG-2.197). Substitutes the portal's
    /// parameters into the prepared SQL, then runs the SAME `execute_sql` core the
    /// simple-query path uses — so a prepared/parameterized statement from a real
    /// driver takes the identical classify → DataFusion-read / GraphTxn-write path.
    /// The portal's `result_column_format` is honoured so a binary-format client
    /// gets binary cells.
    async fn do_query<C>(
        &self,
        client: &mut C,
        portal: &Portal<Self::Statement>,
        _max_rows: usize,
    ) -> PgWireResult<Response>
    where
        C: ClientInfo
            + pgwire::api::ClientPortalStore
            + futures::Sink<pgwire::messages::PgWireBackendMessage>
            + Unpin
            + Send
            + Sync,
        C::PortalStore: pgwire::api::store::PortalStore<Statement = Self::Statement>,
        C::Error: std::fmt::Debug,
        PgWireError: From<<C as futures::Sink<pgwire::messages::PgWireBackendMessage>>::Error>,
    {
        self.resolve_startup_graph(client);
        let sql = substitute_params(&portal.statement.statement.sql, portal)?;
        self.execute_sql(&sql, Some(&portal.result_column_format))
            .await
    }

    /// Describe a parsed-but-unbound statement (CONCEPT:KG-2.197): report concrete
    /// parameter type OIDs (resolved against the node column schema, so a real
    /// driver can encode typed params) AND the result column schema. Params are not
    /// yet bound, so the result schema is derived from the SQL with each `$N`
    /// replaced by `NULL` (a runnable read shape) — schema-on-read needs to execute
    /// to know a read's columns, and a placeholder NULL does not change the columns.
    async fn do_describe_statement<C>(
        &self,
        _client: &mut C,
        target: &StoredStatement<Self::Statement>,
    ) -> PgWireResult<DescribeStatementResponse>
    where
        C: ClientInfo
            + pgwire::api::ClientPortalStore
            + futures::Sink<pgwire::messages::PgWireBackendMessage>
            + Unpin
            + Send
            + Sync,
        C::PortalStore: pgwire::api::store::PortalStore<Statement = Self::Statement>,
        C::Error: std::fmt::Debug,
        PgWireError: From<<C as futures::Sink<pgwire::messages::PgWireBackendMessage>>::Error>,
    {
        let graph = self.current_graph();
        let sql = &target.statement.sql;
        let param_types = self.param_type_oids(&graph, sql).await?;
        // Substitute `$N` → a TYPED dummy literal for the schema-derivation run
        // (params unbound here), so the read keeps its real projection schema.
        let dummy_sql = replace_placeholders_with_dummy(sql, &param_types);
        let fields = self.describe_result_columns(&graph, &dummy_sql).await?;
        Ok(DescribeStatementResponse::new(param_types, fields))
    }

    /// Describe a bound portal (CONCEPT:KG-2.197): report the result column schema
    /// for THIS binding. The params ARE bound, so we substitute them into the SQL
    /// (the same `substitute_params` the execute path uses) and derive the columns
    /// from that concrete statement, honouring the portal's result column format.
    async fn do_describe_portal<C>(
        &self,
        _client: &mut C,
        target: &Portal<Self::Statement>,
    ) -> PgWireResult<DescribePortalResponse>
    where
        C: ClientInfo
            + pgwire::api::ClientPortalStore
            + futures::Sink<pgwire::messages::PgWireBackendMessage>
            + Unpin
            + Send
            + Sync,
        C::PortalStore: pgwire::api::store::PortalStore<Statement = Self::Statement>,
        C::Error: std::fmt::Debug,
        PgWireError: From<<C as futures::Sink<pgwire::messages::PgWireBackendMessage>>::Error>,
    {
        let graph = self.current_graph();
        let sql = substitute_params(&target.statement.statement.sql, target)?;
        let mut fields = self.describe_result_columns(&graph, &sql).await?;
        // Re-stamp each field with the portal's requested wire format (text/binary).
        for (i, f) in fields.iter_mut().enumerate() {
            let fmt = target.result_column_format.format_for(i);
            *f = FieldInfo::new(f.name().to_owned(), None, None, f.datatype().clone(), fmt);
        }
        Ok(DescribePortalResponse::new(fields))
    }
}

/// Replace each `$N` placeholder with a TYPED dummy literal for the unbound-statement
/// Describe path (CONCEPT:KG-2.197). The result columns of a read depend only on its
/// projection + FROM, not the param VALUES — but a degenerate placeholder like `NULL`
/// changes the query's typing (e.g. `WHERE rank > NULL` makes DataFusion fold the
/// scan to an EMPTY, column-less result, which would mis-describe the schema). A typed
/// dummy (`0` / `0.0` / `FALSE` / `''`) keeps the predicate well-typed so the real
/// projection schema survives. `param_oids[k]` is the resolved OID of `$(k+1)`; an
/// out-of-range / unknown param defaults to the empty-text literal. Skips `$N` inside
/// single-quoted string literals.
fn replace_placeholders_with_dummy(sql: &str, param_oids: &[Type]) -> String {
    let dummy = |idx: usize| -> &'static str {
        match param_oids.get(idx) {
            Some(t) if *t == Type::INT8 || *t == Type::INT4 => "0",
            Some(t) if *t == Type::FLOAT8 || *t == Type::FLOAT4 => "0.0",
            Some(t) if *t == Type::BOOL => "FALSE",
            _ => "''",
        }
    };
    let bytes = sql.as_bytes();
    let mut out = String::with_capacity(sql.len());
    let mut i = 0usize;
    let mut in_str = false;
    while i < bytes.len() {
        let b = bytes[i];
        if in_str {
            out.push(b as char);
            if b == b'\'' {
                if i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                    out.push('\'');
                    i += 2;
                    continue;
                }
                in_str = false;
            }
            i += 1;
            continue;
        }
        if b == b'\'' {
            in_str = true;
            out.push('\'');
            i += 1;
            continue;
        }
        if b == b'$' && i + 1 < bytes.len() && bytes[i + 1].is_ascii_digit() {
            let mut j = i + 1;
            let mut n = 0usize;
            while j < bytes.len() && bytes[j].is_ascii_digit() {
                n = n * 10 + (bytes[j] - b'0') as usize;
                j += 1;
            }
            // `$N` is 1-based.
            out.push_str(dummy(n.saturating_sub(1)));
            i = j;
            continue;
        }
        out.push(b as char);
        i += 1;
    }
    out
}

/// The PER-CONNECTION handler factory. pgwire calls `simple_query_handler()` and
/// `extended_query_handler()` ONCE EACH per connection, so to keep the connection's
/// `SET graph` selection (and the one-time startup-graph latch) consistent across
/// BOTH protocols, this factory holds a SINGLE shared `EngineBackend` and returns
/// it from both slots. A fresh factory is built per accepted connection in `serve`,
/// so two connections never share a backend (their `SET graph` stays isolated).
struct EngineBackendFactory {
    backend: Arc<EngineBackend>,
    /// The resolved auth mode + the engine secret, used to build the per-connection
    /// startup handler (CONCEPT:KG-2.202). The SCRAM handler holds per-connection
    /// SASL state, so a FRESH one is built in `startup_handler()` (called once per
    /// connection — a fresh factory is created per accepted connection in `serve`).
    auth_mode: PgWireAuthMode,
    auth_secret: String,
}

impl EngineBackendFactory {
    fn new(
        state: Arc<RwLock<ServerState>>,
        default_graph: String,
        auth_mode: PgWireAuthMode,
        auth_secret: String,
    ) -> Self {
        Self {
            backend: Arc::new(EngineBackend::new(state, default_graph, auth_mode)),
            auth_mode,
            auth_secret,
        }
    }
}

impl PgWireServerHandlers for EngineBackendFactory {
    fn simple_query_handler(&self) -> Arc<impl SimpleQueryHandler> {
        self.backend.clone()
    }

    fn extended_query_handler(&self) -> Arc<impl ExtendedQueryHandler> {
        // SAME instance as the simple handler — prepared statements/portals live in
        // the per-connection `PortalStore` pgwire threads through, while the shared
        // backend keeps `SET graph` / startup resolution consistent across protocols.
        self.backend.clone()
    }

    fn startup_handler(&self) -> Arc<impl StartupHandler> {
        // TRUST or SCRAM (CONCEPT:KG-2.202), resolved once at serve() startup. A
        // fresh handler per connection: the SCRAM SASL state machine is per-conn.
        Arc::new(auth::EngineStartupHandler::new(
            self.auth_mode,
            &self.auth_secret,
        ))
    }

    fn copy_handler(&self) -> Arc<impl pgwire::api::copy::CopyHandler> {
        // SAME shared backend instance (CONCEPT:EG-020) so `COPY … FROM STDIN`'s
        // per-connection copy state is the one the query handler set up.
        self.backend.clone()
    }

    // `error_handler` and `cancel_handler` use the `PgWireServerHandlers` trait
    // defaults (NoopHandler).
}

/// Bind `addr` and serve pgwire connections until the process exits. Spawned by
/// `main.rs` only when built `--features pgwire` AND `EPISTEMIC_GRAPH_PGWIRE_ADDR`
/// is set. The default graph is read once from `EPISTEMIC_GRAPH_PGWIRE_GRAPH`
/// (falling back to `__commons__`).
pub async fn serve(addr: &str, state: Arc<RwLock<ServerState>>) -> std::io::Result<()> {
    // Resolve the auth mode once from the engine secret + env (CONCEPT:KG-2.202).
    let auth_secret = state.read().await.auth_secret.clone();
    let auth_mode = PgWireAuthMode::resolve(&auth_secret);
    serve_with_auth(addr, state, auth_mode).await
}

/// `serve` with an EXPLICIT auth mode (CONCEPT:KG-2.202). `serve` resolves the mode
/// from the env + engine secret and delegates here; integration tests call this
/// directly so they pin trust vs scram deterministically without a process-global
/// env toggle (tests run in parallel).
pub async fn serve_with_auth(
    addr: &str,
    state: Arc<RwLock<ServerState>>,
    auth_mode: PgWireAuthMode,
) -> std::io::Result<()> {
    let default_graph =
        std::env::var(PGWIRE_GRAPH_ENV).unwrap_or_else(|_| "__commons__".to_string());
    let auth_secret = state.read().await.auth_secret.clone();
    let listener = TcpListener::bind(addr).await?;
    tracing::info!(
        "pgwire: serving Postgres wire protocol on {} (default graph '{}', auth={}, \
         simple+extended)",
        addr,
        default_graph,
        auth_mode.as_str()
    );
    loop {
        let (socket, peer) = listener.accept().await?;
        // A FRESH factory (and thus a fresh shared `EngineBackend`) per connection,
        // so each connection's `SET graph` is isolated.
        let factory = Arc::new(EngineBackendFactory::new(
            state.clone(),
            default_graph.clone(),
            auth_mode,
            auth_secret.clone(),
        ));
        tokio::spawn(async move {
            if let Err(e) = process_socket(socket, None, factory).await {
                tracing::warn!("pgwire connection from {peer} ended with error: {e}");
            }
        });
    }
}

#[cfg(test)]
mod copy_tests {
    //! Unit tests for the `COPY … FROM STDIN` decoders + a full decode→ingest proving
    //! "COPY ingests N rows" deterministically (CONCEPT:EG-020), without a socket.
    use super::*;
    use eg_query::{ColumnType, TableSchema};

    fn items_schema() -> TableSchema {
        TableSchema {
            name: "items".into(),
            columns: vec![
                {
                    let mut c = Column::new("id", ColumnType::BigInt, false, true);
                    c.serial = true;
                    c
                },
                {
                    let mut c = Column::new("sku", ColumnType::Text, false, false);
                    c.unique = true;
                    c
                },
                Column::new("qty", ColumnType::Int, true, false),
            ],
        }
    }

    fn copy_state(format: CopyFormat, buf: &[u8], header: bool) -> CopyState {
        CopyState {
            table: "items".into(),
            columns: vec!["sku".into(), "qty".into()],
            format,
            delimiter: None,
            header,
            buf: buf.to_vec(),
        }
    }

    #[test]
    fn csv_decode_and_ingest_n_rows() {
        let schema = items_schema();
        let st = copy_state(
            CopyFormat::Csv,
            b"sku,qty\nAAPL,1\nMSFT,2\n\"x,y\",3\n",
            true,
        );
        let rows = decode_copy_rows(&st, &schema).unwrap();
        assert_eq!(rows.len(), 3, "header skipped, 3 data rows");
        assert_eq!(rows[0][0], serde_json::json!("AAPL"));
        assert_eq!(rows[0][1], serde_json::json!(1));
        assert_eq!(
            rows[2][0],
            serde_json::json!("x,y"),
            "quoted comma preserved"
        );

        // Ingest into a real store: SERIAL id auto-fills, all 3 rows land.
        let (store, _p) = eg_query::TableStore::open_temp().unwrap();
        store.create_table(&schema, false).unwrap();
        let n = store.insert_rows("items", &st.columns, &rows).unwrap();
        assert_eq!(n, 3, "COPY ingested 3 rows");
        let scanned = store.scan("items").unwrap();
        assert_eq!(scanned.len(), 3);
    }

    #[test]
    fn text_format_null_marker() {
        let schema = items_schema();
        let st = copy_state(CopyFormat::Text, b"AAPL\t1\nMSFT\t\\N\n", false);
        let rows = decode_copy_rows(&st, &schema).unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[1][1], serde_json::Value::Null, "\\N ⇒ NULL");
    }

    #[test]
    fn binary_format_decodes_rows() {
        // Two rows of (text sku, int4 qty) in the PGCOPY binary format.
        let mut buf: Vec<u8> = Vec::new();
        buf.extend_from_slice(b"PGCOPY\n\xff\r\n\0");
        buf.extend_from_slice(&0u32.to_be_bytes()); // flags
        buf.extend_from_slice(&0u32.to_be_bytes()); // header ext length
        for (sku, qty) in [("AAPL", 1i32), ("MSFT", 2)] {
            buf.extend_from_slice(&2i16.to_be_bytes()); // field count
            buf.extend_from_slice(&(sku.len() as i32).to_be_bytes());
            buf.extend_from_slice(sku.as_bytes());
            buf.extend_from_slice(&4i32.to_be_bytes());
            buf.extend_from_slice(&qty.to_be_bytes());
        }
        buf.extend_from_slice(&(-1i16).to_be_bytes()); // trailer

        let schema = items_schema();
        let st = copy_state(CopyFormat::Binary, &buf, false);
        let rows = decode_copy_rows(&st, &schema).unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0][0], serde_json::json!("AAPL"));
        assert_eq!(rows[1][1], serde_json::json!(2));
    }
}
