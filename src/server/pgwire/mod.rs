//! Postgres wire-protocol shim (CONCEPT:KG-2.189) — the FIRST `WireProtocol` adapter
//! (CONCEPT:EG-074). A thin facade over the engine's internal SQL surface that lets
//! `psql`, BI tools, and ORMs connect and run SQL against a graph.
//!
//! ## What this is (and is NOT)
//! This is a SHIM, not a second SQL engine, and — since EG-074 — not even a second
//! copy of the query orchestration: the wire-agnostic `classify → dispatch → exec`
//! pipeline, the per-connection session state, and the mixed-store transaction logic
//! all live in [`crate::server::wire`] ([`WireSession`], the one shared
//! [`WireProtocol`] impl). THIS module is now purely the Postgres-SPECIFIC adapter:
//!   * the TCP listener + `process_socket` framing,
//!   * SCRAM/trust startup auth (see `auth.rs`),
//!   * the simple + extended (prepared-statement) query protocols, parameter binding,
//!   * the Arrow→OID result encoding and the `COPY … FROM STDIN` wire decoders,
//!
//! all of which sit ON TOP of [`WireSession`] via [`WireProtocol::execute`], turning a
//! wire-neutral [`WireOutcome`] / [`WireError`] into Postgres bytes.
//!
//! A SELECT arriving over the wire is parsed/planned/executed by the SAME DataFusion
//! path `Method::Sql` uses; a write is classified (`eg_query::classify`) and routed
//! through the engine's `GraphTxn` write path so it gets `mark_dirty` + durability for
//! free. No SQL grammar, planner, or executor is reimplemented here.
//!
//! ## Protocols supported
//! BOTH Postgres query protocols (CONCEPT:KG-2.197):
//!   * **Simple query** (`SimpleQueryHandler`) — a single text query string, one
//!     round-trip. What raw `psql` and `client.simple_query(...)` use.
//!   * **Extended / prepared** (`ExtendedQueryHandler`) — the Parse / Bind /
//!     Describe / Execute / Sync / Close flow with prepared statements, parameter
//!     binding (`$1`, `$2`, …), and portals. This is what psycopg3, asyncpg, JDBC,
//!     sqlx, SQLAlchemy, and `tokio-postgres::prepare`/`query`/`execute` use by
//!     DEFAULT. Both protocols funnel into the SAME [`WireProtocol::execute`] core:
//!     the extended path substitutes bound parameters into the SQL as literals first
//!     (`substitute_params`), then runs the identical classify → read/write path.
//!
//! ## DML / transactions / connected graph / auth / OID mapping
//! Unchanged from KG-2.189/KG-2.198/EG-020/EG-045..049/EG-072/EG-102/EG-115 — the
//! behavior now lives in [`crate::server::wire`]; this adapter only frames it. See the
//! `wire` module header for the mixed-store transaction and durability semantics, and
//! `auth.rs` for the `trust`/`scram` model (CONCEPT:KG-2.202).
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
    ColumnType, CopyFormat, DeleteNodes, InsertNodes, PgColType, StatementKind, TableSchema,
    TypedQueryResult, UpdateNodes,
};

use crate::server::wire::{CopyState, WireError, WireOutcome, WireProtocol, WireSession};
use crate::server::ServerState;

mod auth;

pub use auth::{derive_pg_password, PgWireAuthMode, PGWIRE_AUTH_ENV};

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

// ── error + outcome adaptation (CONCEPT:EG-074) ───────────────────────────────

/// Map an internal error string to a pgwire user error (SQLSTATE 58000 — system
/// error) for the Postgres-SPECIFIC surfaces of this adapter (COPY framing, etc.).
fn user_err(msg: impl Into<String>) -> PgWireError {
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "ERROR".to_owned(),
        "58000".to_owned(),
        msg.into(),
    )))
}

/// Map a wire-neutral [`WireError`] onto a Postgres error frame, preserving the exact
/// SQLSTATE + message the shared core produced (so a client sees byte-for-byte the
/// same `ERROR` it did before EG-074 — e.g. `25P02` aborted-txn, `42501` denied).
fn wire_err_to_pg(e: WireError) -> PgWireError {
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "ERROR".to_owned(),
        e.code,
        e.message,
    )))
}

/// Encode a wire-neutral [`WireOutcome`] into the Postgres `Response` the protocol
/// sends, honouring the requested per-column result `format` (text by default; the
/// extended protocol's `result_column_format` for prepared portals). This is the ONE
/// place Postgres framing is applied over the shared execution core.
fn outcome_to_response(outcome: WireOutcome, format: Option<&Format>) -> Response {
    match outcome {
        WireOutcome::Rows(result) => Response::Query(query_response(result, format)),
        WireOutcome::Command { tag, rows } => {
            let mut t = Tag::new(tag);
            if let Some(n) = rows {
                t = t.with_rows(n);
            }
            Response::Execution(t)
        }
        WireOutcome::TxnStart => Response::TransactionStart(Tag::new("BEGIN")),
        WireOutcome::TxnEnd { tag } => Response::TransactionEnd(Tag::new(tag)),
        WireOutcome::CopyIn {
            format_code,
            num_columns,
        } => Response::CopyIn(pgwire::api::results::CopyResponse::new(
            format_code,
            num_columns,
            futures::stream::empty(),
        )),
    }
}

// ── Arrow → Postgres result encoding ──────────────────────────────────────────

/// `eg_query::PgColType` → a Postgres wire `Type` (the OID the client sees).
fn pg_type(t: PgColType) -> Type {
    match t {
        PgColType::Int8 => Type::INT8,
        PgColType::Float8 => Type::FLOAT8,
        PgColType::Bool => Type::BOOL,
        PgColType::Text => Type::TEXT,
        // CONCEPT:EG-115 — pgvector `vector`. pgvector's own OID is dynamically assigned
        // by the extension, so we report the stable, always-present float4-array OID
        // (`_float4` = 1021, "float-array-ish"): a client without the vector type
        // registered still resolves a sane type, and the value is sent as the pgvector
        // text form `[1,2,3]` (see `encode_cell`), which pgvector clients parse.
        PgColType::Vector => Type::FLOAT4_ARRAY,
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
        // CONCEPT:EG-115 — render a vector (a JSON array of numbers) as the pgvector
        // text literal `[1,2,3]`; a non-array value falls back to its JSON text.
        PgColType::Vector => match cell {
            Value::Array(items) => {
                let parts: Vec<String> = items
                    .iter()
                    .map(|v| match v {
                        Value::Number(n) => n.to_string(),
                        Value::Null => "0".to_string(),
                        other => other.to_string(),
                    })
                    .collect();
                encoder.encode_field(&format!("[{}]", parts.join(",")))
            }
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

// ── the Postgres wire adapter ──────────────────────────────────────────────────

/// Per-connection Postgres wire handler (CONCEPT:EG-074). Owns the wire-agnostic
/// [`WireSession`] that does the actual SQL work and adds ONLY the Postgres framing:
/// the simple/extended protocol handlers, the extended-protocol Describe support (OID
/// resolution + result-column schema), and the COPY wire decoders. One instance per
/// connection (a fresh factory per accepted connection in `serve`), so each
/// connection's `SET graph` / txn state stays isolated.
struct EngineBackend {
    /// The shared, wire-agnostic execution core (CONCEPT:EG-074).
    session: Arc<WireSession>,
    /// The SQL parser used for the extended protocol's Parse step. Stateless.
    parser: Arc<EngineQueryParser>,
}

impl EngineBackend {
    fn new(
        state: Arc<RwLock<ServerState>>,
        default_graph: String,
        auth_mode: PgWireAuthMode,
    ) -> Self {
        // Under SCRAM the authenticated libpq `user` becomes the ACL actor; under
        // TRUST the connection stays anonymous (CONCEPT:KG-2.202).
        let auth_maps_actor = matches!(auth_mode, PgWireAuthMode::Scram);
        Self {
            session: Arc::new(WireSession::new(state, default_graph, auth_maps_actor)),
            parser: Arc::new(EngineQueryParser),
        }
    }

    /// Latch the connection's startup graph + actor from the libpq `database`/`user`
    /// startup parameters (readable only once startup completes, so on the first
    /// query). Delegates the rules to [`WireProtocol::resolve_startup`].
    fn resolve_startup_from_client<C: ClientInfo>(&self, client: &C) {
        let meta = client.metadata();
        let user = meta.get(pgwire::api::METADATA_USER).cloned();
        let db = meta.get(pgwire::api::METADATA_DATABASE).cloned();
        self.session.resolve_startup(user, db);
    }

    /// Resolve the wire `Type` OIDs for a statement's `$N` parameters
    /// (CONCEPT:KG-2.197). Locates each param via `eg_query::infer_param_sites` (a
    /// column / `id` / literal site), then types it: an `id` site → TEXT; a column
    /// site → that column's type from the shared node column-type map; a literal site
    /// → its directly-derived type. A column with no observed type defaults to TEXT.
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
        let cols = self
            .session
            .column_types(graph)
            .await
            .map_err(wire_err_to_pg)?;
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
    /// bound params substituted (portal describe) or `$N`→dummy (statement describe).
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
                let result = self
                    .session
                    .run_read(graph, probe)
                    .await
                    .map_err(wire_err_to_pg)?;
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
                let (cols, types) = self
                    .session
                    .returning_cols(graph, sql)
                    .await
                    .map_err(wire_err_to_pg)?;
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
        self.resolve_startup_from_client(client);
        // Simple query: the unified TEXT wire format (no per-column format codes).
        let outcome = self.session.execute(query).await.map_err(wire_err_to_pg)?;
        Ok(vec![outcome_to_response(outcome, None)])
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
        self.session
            .append_copy_data(copy_data.data.as_ref())
            .map_err(wire_err_to_pg)
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
            .session
            .take_copy_state()
            .ok_or_else(|| user_err("COPY done with no COPY in progress"))?;
        let store = crate::server::wire::user_table_store().map_err(wire_err_to_pg)?;
        let schema = store
            .get_schema(state.table())
            .map_err(user_err)?
            .ok_or_else(|| user_err(format!("table `{}` does not exist", state.table())))?;
        let rows = decode_copy_rows(&state, &schema).map_err(user_err)?;
        let table = state.table().to_string();
        let columns = state.columns().to_vec();
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

/// Decode a `COPY … FROM STDIN` body into typed rows aligned to the copy target's
/// columns (CONCEPT:EG-020). Supports the Postgres TEXT, CSV, and BINARY formats; each
/// field is coerced to its target column's [`ColumnType`] so the store's typed insert
/// path accepts it (and SERIAL/DEFAULT fill any column the COPY omits).
fn decode_copy_rows(
    state: &CopyState,
    schema: &TableSchema,
) -> Result<Vec<Vec<serde_json::Value>>, String> {
    // The declared type of each COPY target column.
    let columns = state.columns();
    let mut types = Vec::with_capacity(columns.len());
    for name in columns {
        let col = schema
            .columns
            .iter()
            .find(|c| &c.name == name)
            .ok_or_else(|| format!("COPY column `{name}` does not exist in `{}`", state.table()))?;
        types.push(col.ty);
    }

    match state.format() {
        CopyFormat::Binary => decode_copy_binary(state.buf(), &types),
        CopyFormat::Csv | CopyFormat::Text => {
            let is_csv = state.format() == CopyFormat::Csv;
            let delim = state.delimiter().unwrap_or(if is_csv { ',' } else { '\t' });
            let text = std::str::from_utf8(state.buf())
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
                if is_csv && state.header() && li == 0 {
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
        // CONCEPT:EG-115 — a vector arrives as pgvector text `[1,2,3]`; pass it through
        // as a string so the store's `Cell::coerce` parses + dimension-checks it.
        ColumnType::Text | ColumnType::Bytes | ColumnType::Vector(_) => Value::String(s.to_string()),
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
        // CONCEPT:EG-115 — the pgvector BINARY wire format is a distinct later item;
        // for now a vector must be sent via TEXT/CSV COPY (or INSERT).
        ColumnType::Vector(_) => {
            return Err("binary COPY of a vector column is not supported (use TEXT COPY or \
                        INSERT)"
                .to_string())
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
    /// parameters into the prepared SQL, then runs the SAME [`WireProtocol::execute`]
    /// core the simple-query path uses — so a prepared/parameterized statement from a
    /// real driver takes the identical classify → DataFusion-read / GraphTxn-write
    /// path. The portal's `result_column_format` is honoured so a binary-format client
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
        self.resolve_startup_from_client(client);
        let sql = substitute_params(&portal.statement.statement.sql, portal)?;
        let outcome = self.session.execute(&sql).await.map_err(wire_err_to_pg)?;
        Ok(outcome_to_response(
            outcome,
            Some(&portal.result_column_format),
        ))
    }

    /// Describe a parsed-but-unbound statement (CONCEPT:KG-2.197): report concrete
    /// parameter type OIDs (resolved against the node column schema, so a real
    /// driver can encode typed params) AND the result column schema. Params are not
    /// yet bound, so the result schema is derived from the SQL with each `$N`
    /// replaced by a typed dummy (a runnable read shape) — schema-on-read needs to
    /// execute to know a read's columns, and a placeholder does not change the columns.
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
        let graph = self.session.current_graph();
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
        let graph = self.session.current_graph();
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
    use eg_query::{Column, ColumnType, TableSchema};

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
        let n = store.insert_rows("items", st.columns(), &rows).unwrap();
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
