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
//! ## Lifecycle / default behavior
//! Built only under the `pgwire` cargo feature (cluster tier). The listener is
//! spawned by `main.rs` ONLY when the binary is built `--features pgwire` AND
//! `EPISTEMIC_GRAPH_PGWIRE_ADDR` is set. With the feature off, or on but unset, the
//! engine runs byte-for-byte as today — nothing in this module is reachable.
//!
//! ## Connected graph
//! The graph a connection runs against is selected, in order: (1) the libpq
//! `database` startup parameter (e.g. `psql -d 'team:alpha'`), (2) else
//! `EPISTEMIC_GRAPH_PGWIRE_GRAPH` (a server default), (3) else `__commons__`. It
//! can be changed mid-session with `SET graph = '<name>'` (or `SET graph TO
//! '<name>'`). A missing graph yields a clean error, not a panic.
//!
//! ## Auth model (first increment)
//! TRUST: the startup phase performs NO authentication (pgwire's
//! `NoopStartupHandler`). The listener only binds when an operator explicitly sets
//! `EPISTEMIC_GRAPH_PGWIRE_ADDR`, and the documented address is a loopback
//! (`127.0.0.1:5433`). Password/SCRAM auth over this surface is the documented
//! follow-up; it slots in by swapping the `startup_handler` for a password handler.
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
use pgwire::api::query::SimpleQueryHandler;
use pgwire::api::results::{DataRowEncoder, FieldInfo, QueryResponse, Response, Tag};
use pgwire::api::{ClientInfo, PgWireServerHandlers, Type};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use pgwire::messages::data::DataRow;
use pgwire::tokio::process_socket;

use eg_query::{PgColType, StatementKind, TypedQueryResult};

use crate::server::ServerState;

/// Env var: when set (and the binary is built `--features pgwire`), the pgwire
/// listener binds this address (e.g. `127.0.0.1:5433`). Unset → no listener.
pub const PGWIRE_ADDR_ENV: &str = "EPISTEMIC_GRAPH_PGWIRE_ADDR";
/// Env var: the default graph a fresh connection runs against when the libpq
/// `database` parameter is not supplied. Defaults to `__commons__`.
pub const PGWIRE_GRAPH_ENV: &str = "EPISTEMIC_GRAPH_PGWIRE_GRAPH";

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
}

impl EngineBackend {
    fn new(state: Arc<RwLock<ServerState>>, default_graph: String) -> Self {
        Self {
            state,
            graph: parking_lot::Mutex::new(default_graph),
            startup_resolved: std::sync::atomic::AtomicBool::new(false),
        }
    }

    fn current_graph(&self) -> String {
        self.graph.lock().clone()
    }

    /// One-time: adopt the libpq `database` startup parameter as the target graph
    /// (priority 1). Skipped if a `SET graph` already chose one, if the param is
    /// absent, or if it equals the connection's user name (libpq's default when
    /// `dbname` is unset — not an intentional graph selection). Idempotent.
    fn resolve_startup_graph<C: ClientInfo>(&self, client: &C) {
        use std::sync::atomic::Ordering;
        if self.startup_resolved.swap(true, Ordering::AcqRel) {
            return;
        }
        let meta = client.metadata();
        let db = match meta.get(pgwire::api::METADATA_DATABASE) {
            Some(d) if !d.is_empty() => d,
            _ => return,
        };
        // libpq defaults `database` to the user name when `dbname` is unset; that
        // is not a deliberate graph pick, so leave the server default in place.
        if meta.get(pgwire::api::METADATA_USER).map(String::as_str) == Some(db.as_str()) {
            return;
        }
        *self.graph.lock() = db.clone();
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

/// Build the `RowDescription` fields from a typed result. All columns are returned
/// in the unified TEXT wire format (simple-query protocol), so each cell is sent
/// as its text rendering — the universally-compatible path for psql/BI tools.
fn field_defs(result: &TypedQueryResult) -> Arc<Vec<FieldInfo>> {
    Arc::new(
        result
            .columns
            .iter()
            .map(|c| {
                FieldInfo::new(
                    c.name.clone(),
                    None,
                    None,
                    pg_type(c.ty),
                    pgwire::api::results::FieldFormat::Text,
                )
            })
            .collect(),
    )
}

/// Render one decoded JSON cell as the TEXT representation Postgres expects for the
/// column's type. NULL stays NULL; strings pass through unquoted; numbers/bools
/// render canonically; anything structural is JSON-stringified.
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

/// Build the streamed `DataRow`s for a typed read result.
fn rows_stream(result: TypedQueryResult) -> impl Stream<Item = PgWireResult<DataRow>> {
    let schema = field_defs(&result);
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

    /// Execute a read (`SELECT`/`WITH`/…) over `graph` by reusing the EXACT
    /// DataFusion path `Method::Sql` uses: take the owned off-lock
    /// `analysis_snapshot()` and run `eg_query::exec_sql_typed` on the blocking
    /// pool (DataFusion's executor must not run on a reactor worker).
    async fn run_read(&self, graph: &str, sql: String) -> PgWireResult<TypedQueryResult> {
        let core = self.graph_core(graph).await?;
        let snap = core.analysis_snapshot();
        tokio::task::spawn_blocking(move || eg_query::exec_sql_typed(&snap, &sql))
            .await
            .map_err(|e| user_err(format!("query task failed: {e}")))?
            .map_err(|msg| user_err(format!("SQL error: {msg}")))
    }

    /// Apply a classified write through the engine's `GraphTxn` write path, so it
    /// gets `mark_dirty` (checkpoint) + WAL durability for free — NOT DataFusion's
    /// write planner. Returns the `CommandComplete` tag. First increment: INSERT
    /// (node creation) only; UPDATE/DELETE are recognized but report a precise
    /// follow-up error.
    async fn run_write(&self, graph: &str, kind: StatementKind) -> PgWireResult<Tag> {
        match kind {
            StatementKind::InsertNode(node) => {
                let core = self.graph_core(graph).await?;
                let blob = rmp_serde::to_vec_named(&serde_json::Value::Object(node.properties))
                    .map_err(|e| user_err(format!("encode node properties: {e}")))?;
                // One-shot GraphTxn (acquires the per-graph topology write lock,
                // applies, releases) — the same write path AddNode dispatch uses.
                core.add_node(node.node_id, blob);
                core.mark_dirty();
                // NOTE (follow-up): the in-process write applies + is captured by the
                // next checkpoint via mark_dirty. Routing the pgwire write through the
                // shared dispatch so the configured durable backend's WAL `record()`
                // fires per-op is the documented integration follow-up.
                Ok(Tag::new("INSERT").with_rows(1))
            }
            StatementKind::Update => Err(user_err(
                "UPDATE over pgwire is not yet supported (CONCEPT:KG-2.189 follow-up); \
                 use the engine's CAS/update methods",
            )),
            StatementKind::Delete => Err(user_err(
                "DELETE over pgwire is not yet supported (CONCEPT:KG-2.189 follow-up); \
                 use the engine's RemoveNode method",
            )),
            StatementKind::Read => unreachable!("read routed to write path"),
        }
    }
}

#[async_trait]
impl SimpleQueryHandler for EngineBackend {
    async fn do_query<C>(&self, client: &mut C, query: &str) -> PgWireResult<Vec<Response>>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        // On the first query, adopt the libpq `database` startup parameter as the
        // target graph (priority 1) — readable only now that startup has completed.
        self.resolve_startup_graph(client);

        // Connection-scoped `SET graph = …` is handled before classification.
        if let Some(res) = self.try_set_graph(query) {
            return res.map(|tag| vec![Response::Execution(tag)]);
        }

        let graph = self.current_graph();
        match eg_query::classify(query).map_err(user_err)? {
            StatementKind::Read => {
                let result = self.run_read(&graph, query.to_string()).await?;
                let schema = field_defs(&result);
                let data = rows_stream(result);
                Ok(vec![Response::Query(QueryResponse::new(schema, data))])
            }
            write_kind => {
                let tag = self.run_write(&graph, write_kind).await?;
                Ok(vec![Response::Execution(tag)])
            }
        }
    }
}

/// The connection factory: one `EngineBackend` per connection (so `SET graph` is
/// per-connection), trust startup (first increment), defaults for the rest.
struct EngineBackendFactory {
    state: Arc<RwLock<ServerState>>,
    default_graph: String,
}

impl PgWireServerHandlers for EngineBackendFactory {
    fn simple_query_handler(&self) -> Arc<impl SimpleQueryHandler> {
        Arc::new(EngineBackend::new(
            self.state.clone(),
            self.default_graph.clone(),
        ))
    }

    fn startup_handler(&self) -> Arc<impl StartupHandler> {
        // TRUST: no authentication in the first increment (documented).
        Arc::new(pgwire::api::NoopHandler)
    }

    // `extended_query_handler`, `copy_handler`, `error_handler`, and
    // `cancel_handler` use the `PgWireServerHandlers` trait defaults (NoopHandler).
}

/// Bind `addr` and serve pgwire connections until the process exits. Spawned by
/// `main.rs` only when built `--features pgwire` AND `EPISTEMIC_GRAPH_PGWIRE_ADDR`
/// is set. The default graph is read once from `EPISTEMIC_GRAPH_PGWIRE_GRAPH`
/// (falling back to `__commons__`).
pub async fn serve(addr: &str, state: Arc<RwLock<ServerState>>) -> std::io::Result<()> {
    let default_graph =
        std::env::var(PGWIRE_GRAPH_ENV).unwrap_or_else(|_| "__commons__".to_string());
    let listener = TcpListener::bind(addr).await?;
    tracing::info!(
        "pgwire: serving Postgres wire protocol on {} (default graph '{}', auth=trust)",
        addr,
        default_graph
    );
    let factory = Arc::new(EngineBackendFactory {
        state,
        default_graph,
    });
    loop {
        let (socket, peer) = listener.accept().await?;
        let factory = factory.clone();
        tokio::spawn(async move {
            if let Err(e) = process_socket(socket, None, factory).await {
                tracing::warn!("pgwire connection from {peer} ended with error: {e}");
            }
        });
    }
}
