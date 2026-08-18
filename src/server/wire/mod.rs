//! Wire-agnostic SQL execution core (CONCEPT:EG-KG.compute.subsystems-reference) — the multi-wire keystone.
//!
//! ## What this is
//! This module extracts the WIRE-NEUTRAL half of a database wire protocol out of the
//! Postgres shim (`crate::server::pgwire`) so that EVERY present and future wire
//! (Postgres today; SQLite/MySQL/MSSQL in Phase J — EG-075/076/077; an AMQP/broker
//! wire in Phase Y) reuses ONE `classify → dispatch → exec` path against the engine.
//!
//! The split is:
//!   * **wire-neutral (here):** the per-connection session state (current graph,
//!     authenticated actor, the mixed-store transaction buffers), and the whole
//!     `execute → classify → read/write/DDL dispatch → RowSet-or-tag` pipeline. A
//!     read reuses the EXACT DataFusion path `Method::Sql` uses; a write is routed
//!     through the engine's `GraphTxn` + durability path. The result is a
//!     wire-NEUTRAL [`WireOutcome`] (a typed result set, a command tag, a
//!     transaction-status change, or a copy-in request) and a wire-NEUTRAL
//!     [`WireError`] (a SQLSTATE + message).
//!   * **wire-specific (each wire's own module):** the listener + framing, the
//!     handshake/auth, parameter binding, and the encoding of a [`WireOutcome`] into
//!     that protocol's bytes (OIDs, `DataRow`s, tags, error frames). For Postgres
//!     that adapter is `crate::server::pgwire`, which is now a THIN shim over
//!     [`WireSession`].
//!
//! ## The [`WireProtocol`] trait — the seam
//! [`WireProtocol`] is the contract a wire drives: latch the connection's startup
//! identity, ask the current graph / txn / actor, and `execute` one complete literal
//! SQL statement into a [`WireOutcome`]. [`WireSession`] is the one concrete
//! implementation (shared by all wires).
//!
//! ## Adding a new wire (Phase J / Phase Y)
//! A new wire lives in its own module (e.g. `src/server/mysqlwire/`) and:
//!   1. Holds an `Arc<WireSession>` (built with [`WireSession::new`]).
//!   2. Runs its own listener + protocol handshake + auth, mapping the authenticated
//!      identity to an engine actor. A request cannot execute until that binding exists.
//!   3. On each statement: (optionally) substitute its own parameter form into a
//!      literal SQL string, then call [`WireProtocol::execute`] and encode the
//!      returned [`WireOutcome`] / [`WireError`] into its own wire bytes.
//!
//! NOTHING in the classify/dispatch/exec/txn/durability path is reimplemented — the
//! new wire only adds framing + encoding. This is the EG-074 promise.

use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::RwLock;

use eg_query::{
    AlterTableAction, AlterTablePlan, AnnIndexPlan, Column, ColumnType, ContinuousAggPlan,
    CopyFormat, CopyPlan, CreateFunctionPlan, CreateTablePlan, CreateViewPlan, CypherCallPlan,
    DeleteNodes, DeleteNodesJoin, DeleteTable, DropFunctionPlan, DropTablePlan, DropViewPlan,
    HypertablePlan, InsertNodes, InsertNodesSelect, InsertSelect, InsertTable, OnConflictAction,
    PgColType, StatementKind, TableSchema, TableStore, TableTxn, TxnOp, TypedColumn,
    TypedQueryResult, UpdateNodes, UpdateNodesJoin, UpdateTable, WhereEq,
};

use crate::isolation::AccessLevel;
use crate::protocol::Request;
use crate::server::access::CarrierAuthority;
use crate::server::ServerState;

// ── wire-neutral currency ────────────────────────────────────────────────────

/// A wire-NEUTRAL execution error (CONCEPT:EG-KG.compute.subsystems-reference): a SQLSTATE `code` + a `message`.
/// Each wire maps this to its own error frame. Postgres maps it 1:1 to a
/// `PgWireError::UserError(ErrorInfo{ severity:"ERROR", code, message })`, so the
/// exact SQLSTATE and text a client sees are preserved byte-for-byte.
#[derive(Debug, Clone)]
pub struct WireError {
    /// The SQLSTATE code (e.g. `58000` system error, `25P02` aborted-txn, `42501`
    /// insufficient-privilege).
    pub code: String,
    /// The human-readable error message.
    pub message: String,
}

impl std::fmt::Display for WireError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[{}] {}", self.code, self.message)
    }
}

impl std::error::Error for WireError {}

/// The wire-neutral result alias for the execution core.
pub type WireResult<T> = Result<T, WireError>;

/// The wire-NEUTRAL outcome of executing one statement (CONCEPT:EG-KG.compute.subsystems-reference). Each wire
/// encodes this into its own protocol bytes; the core never constructs
/// protocol-specific responses.
#[derive(Debug)]
pub enum WireOutcome {
    /// A result set (a read, or a `RETURNING` write). The wire encodes the typed
    /// rows in its own row format.
    Rows(TypedQueryResult),
    /// A command completed with a `tag` (e.g. `SET`, `CREATE TABLE`, `INSERT`) and an
    /// optional affected-row count.
    Command {
        /// The command tag.
        tag: &'static str,
        /// The affected-row count, when the command reports one.
        rows: Option<usize>,
    },
    /// `BEGIN` — a transaction started (the wire reports its "in transaction" status).
    TxnStart,
    /// `COMMIT`/`ROLLBACK` — a transaction ended (`tag` distinguishes which).
    TxnEnd {
        /// `COMMIT` or `ROLLBACK`.
        tag: &'static str,
    },
    /// `COPY … FROM STDIN` — the wire enters bulk copy-in mode. `format_code` is the
    /// pg copy format (0 = text/csv, 1 = binary) and `num_columns` the target column
    /// count; the resolved copy target is stashed in the session (see
    /// [`WireSession::take_copy_state`]).
    CopyIn {
        /// The copy wire format code (0 text/csv, 1 binary).
        format_code: i8,
        /// The number of target columns.
        num_columns: usize,
    },
}

impl WireOutcome {
    /// A command tag with no row count.
    pub fn command(tag: &'static str) -> Self {
        WireOutcome::Command { tag, rows: None }
    }

    /// A command tag with an affected-row count.
    pub fn command_rows(tag: &'static str, rows: usize) -> Self {
        WireOutcome::Command {
            tag,
            rows: Some(rows),
        }
    }
}

/// The wire-agnostic contract every wire drives (CONCEPT:EG-KG.compute.subsystems-reference). A wire owns its
/// framing/auth/encoding; the query semantics live entirely behind this trait.
#[async_trait]
pub trait WireProtocol: Send + Sync {
    /// The connection's currently-selected graph.
    fn current_graph(&self) -> String;
    /// Whether a multi-statement transaction is currently open.
    fn in_txn(&self) -> bool;
    /// The authenticated actor (engine `agent_id`) for this connection. `None`
    /// is observable only while the transport object is still unbound; execution
    /// rejects that state before admission, ACL evaluation, or data access.
    fn actor(&self) -> Option<String>;
    /// One-time: latch the target graph (`database`) from handshake metadata.
    /// Identity is bound separately, and only after the protocol's cryptographic
    /// authentication succeeds. Idempotent.
    fn resolve_startup(&self, user: Option<String>, database: Option<String>);
    /// Execute one COMPLETE literal SQL statement (any wire-specific parameter form
    /// already substituted) through the shared classify → dispatch → exec pipeline,
    /// producing a wire-neutral [`WireOutcome`].
    async fn execute(&self, sql: &str) -> WireResult<WireOutcome>;
}

// ── errors ───────────────────────────────────────────────────────────────────

/// Map an internal engine error string to a wire user error (SQLSTATE 58000 —
/// system error) — keeps clients reporting a clean message instead of a drop.
pub(crate) fn user_err(msg: impl Into<String>) -> WireError {
    WireError {
        code: "58000".to_owned(),
        message: msg.into(),
    }
}

/// The "current transaction is aborted" error (SQLSTATE 25P02, CONCEPT:EG-KG.compute.kg-transaction-is-pinned) —
/// what a SQL client is told for every statement issued inside a failed transaction
/// block until it is ended with COMMIT/ROLLBACK.
pub(crate) fn aborted_txn_err() -> WireError {
    WireError {
        code: "25P02".to_owned(),
        message: "current transaction is aborted, commands ignored until end of transaction block"
            .to_owned(),
    }
}

/// Map a standard SQL isolation-level spelling onto the engine's ACTUAL
/// levels (CONCEPT:EG-KG.txn.serializable-zero-cost — NE-005). `SERIALIZABLE` maps 1:1 (this engine really has
/// one). `REPEATABLE READ` / `READ COMMITTED` / `READ UNCOMMITTED` all map
/// onto `Snapshot` — never a downgrade: Postgres' OWN `REPEATABLE READ`
/// already IS snapshot isolation (not the stricter textbook definition), and
/// giving MORE protection than a weaker request asked for is standard SQL
/// behavior — Postgres itself silently upgrades `READ UNCOMMITTED` to `READ
/// COMMITTED` for the identical reason. Anything else is REJECTED with a
/// typed error (SQLSTATE `0A000`, feature_not_supported) — an isolation
/// level this engine cannot actually provide is NEVER silently accepted and
/// quietly downgraded.
fn parse_sql_isolation_level(text: &str) -> WireResult<crate::server::txn::IsolationLevel> {
    let norm = text.trim().trim_end_matches(';').trim().to_ascii_uppercase();
    let collapsed = norm.split_whitespace().collect::<Vec<_>>().join(" ");
    match collapsed.as_str() {
        "SERIALIZABLE" => Ok(crate::server::txn::IsolationLevel::Serializable),
        "REPEATABLE READ" | "READ COMMITTED" | "READ UNCOMMITTED" => {
            Ok(crate::server::txn::IsolationLevel::Snapshot)
        }
        other => Err(WireError {
            code: "0A000".to_string(),
            message: format!("isolation level '{other}' is not supported by this engine"),
        }),
    }
}

/// Extract an inline `ISOLATION LEVEL <level>` clause from a `BEGIN`/`START
/// TRANSACTION` statement's raw text (CONCEPT:EG-KG.txn.serializable-zero-cost — NE-005). `eg_query::classify`
/// already accepts `Statement::StartTransaction { .. }` regardless of its
/// `modes` (a `sqlparser` field it discards — see `classify.rs`, owned by a
/// different track), so the clause parses fine there but is never surfaced;
/// this local, purely textual re-scan is the only change wire/mod.rs needs to
/// read it. `None` when the statement has no isolation clause at all (a bare
/// `BEGIN`); `Some(Err(..))` for an unsupported spelling.
fn parse_begin_isolation_clause(
    sql: &str,
) -> Option<WireResult<crate::server::txn::IsolationLevel>> {
    let upper = sql.to_ascii_uppercase();
    let idx = upper.find("ISOLATION LEVEL")?;
    let rest = &sql[idx + "ISOLATION LEVEL".len()..];
    // Stop at the next clause keyword/separator — the level name itself is
    // one or two words (`SERIALIZABLE`, `REPEATABLE READ`, …).
    let end = rest.find([',', ';']).unwrap_or(rest.len());
    Some(parse_sql_isolation_level(&rest[..end]))
}

/// Best-effort extraction of `<col> = '<value>'` equality filters on a
/// label-shaped column (`type`/`label`/`labels`) from a read statement's raw
/// SQL text (CONCEPT:EG-KG.txn.serializable-zero-cost — NE-005 auto predicate tracking). This mirrors the ONE
/// predicate shape [`crate::server::txn::PredicateRead`] can express
/// (`get_nodes_by_label`), so a SQL `SERIALIZABLE` transaction's own reads
/// seed genuine phantom protection for exactly the reads the engine's
/// commit-time predicate check can protect. A read outside this shape
/// (arbitrary `WHERE`, a join, an aggregate) is simply NOT captured here —
/// this can only ADD protection beyond `Snapshot`, never remove it, so it
/// never turns a `SERIALIZABLE` request into a silent `Snapshot` lie; it
/// documents the actual (narrower-than-general-SQL) coverage instead of
/// over-claiming it. Char-indexed (never byte-slices mid-codepoint) so a
/// non-ASCII value never panics.
fn extract_label_predicates(sql: &str) -> Vec<String> {
    let chars: Vec<(usize, char)> = sql.char_indices().collect();
    let n = chars.len();
    const COLUMNS: [&str; 3] = ["labels", "label", "type"];
    let mut out = Vec::new();
    let mut i = 0;
    while i < n {
        let matched = COLUMNS.iter().find(|col| {
            let len = col.chars().count();
            i + len <= n
                && chars[i..i + len]
                    .iter()
                    .zip(col.chars())
                    .all(|((_, c), cc)| c.eq_ignore_ascii_case(&cc))
        });
        let Some(col) = matched else {
            i += 1;
            continue;
        };
        let col_len = col.chars().count();
        let before_ok = i == 0 || !(chars[i - 1].1.is_alphanumeric() || chars[i - 1].1 == '_');
        let mut j = i + col_len;
        while j < n && chars[j].1.is_whitespace() {
            j += 1;
        }
        if before_ok && j < n && chars[j].1 == '=' {
            let mut k = j + 1;
            while k < n && chars[k].1.is_whitespace() {
                k += 1;
            }
            if k < n && chars[k].1 == '\'' {
                let val_start = k + 1;
                let mut e = val_start;
                while e < n && chars[e].1 != '\'' {
                    e += 1;
                }
                if e < n {
                    let start_byte = chars[val_start].0;
                    // `e` may equal `n` only when the loop ran off the end,
                    // which the `e < n` guard above already excludes here.
                    let end_byte = chars[e].0;
                    out.push(sql[start_byte..end_byte].to_string());
                    i = e + 1;
                    continue;
                }
            }
        }
        i += col_len;
    }
    out
}

/// Resolve a classify `ColumnDef` (raw SQL type spelling) into a store [`Column`].
pub(crate) fn to_store_columns(cols: &[eg_query::ColumnDef]) -> WireResult<Vec<Column>> {
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

/// Lower a decoded `ALTER TABLE` plan into the matching buffered [`TxnOp`] (CONCEPT:EG-KG.query.register-user-tables-alongside
/// ADD COLUMN + CONCEPT:EG-KG.query.rename-table-moves-catalog the rest), so a `BEGIN … ALTER … COMMIT` applies it in the
/// SAME redb write txn as the surrounding statements.
pub(crate) fn alter_txn_op(plan: AlterTablePlan) -> WireResult<TxnOp> {
    let table = plan.name;
    let op = match plan.action {
        AlterTableAction::AddColumn(col) => {
            let column = to_store_columns(std::slice::from_ref(&col))?
                .into_iter()
                .next()
                .expect("one column");
            TxnOp::AddColumn { table, column }
        }
        AlterTableAction::DropColumn { column, if_exists } => TxnOp::DropColumn {
            table,
            column,
            if_exists,
        },
        AlterTableAction::RenameColumn { from, to } => TxnOp::RenameColumn { table, from, to },
        AlterTableAction::RenameTable { new_name } => TxnOp::RenameTable { table, new_name },
        AlterTableAction::AlterColumnType { column, new_type } => TxnOp::AlterColumnType {
            table,
            column,
            new_type: ColumnType::parse(&new_type).map_err(user_err)?,
        },
        AlterTableAction::DropConstraint {
            constraint,
            if_exists,
        } => TxnOp::DropConstraint {
            table,
            constraint,
            if_exists,
        },
    };
    Ok(op)
}

/// The PgColType for a single JSON value (RETURNING result-set schema inference).
pub(crate) fn col_type_of(v: &serde_json::Value) -> PgColType {
    match v {
        serde_json::Value::Number(n) if n.is_i64() || n.is_u64() => PgColType::Int8,
        serde_json::Value::Number(_) => PgColType::Float8,
        serde_json::Value::Bool(_) => PgColType::Bool,
        _ => PgColType::Text,
    }
}

/// The deterministic column order a `RETURNING` clause projects, EXACTLY matching
/// what the extended-protocol Describe step reports (so the executed row field count
/// never disagrees with the described schema). A NAMED projection
/// (`RETURNING id, rank`) yields those columns; `RETURNING *` (or no statically
/// nameable list) yields `id` followed by the property columns in `prop_cols`
/// order. Both paths compute this from the SAME inputs so they agree.
pub(crate) fn returning_projection(sql: &str, prop_cols: &[String]) -> Vec<String> {
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
/// uses — so the executed rows' types ALWAYS match the described schema. A named
/// column missing from a node's properties is NULL; `id` is filled from the node id.
/// A column absent from the type map defaults to TEXT; `id` is TEXT.
pub(crate) fn returning_result(
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

/// A single-column, single-row text result (CONCEPT:EG-KG.query.continuous-aggregate-lowering) — the shape a scalar
/// set-returning function like `create_hypertable(...)` returns to a client.
pub(crate) fn single_text_result(col: &str, val: &str) -> TypedQueryResult {
    TypedQueryResult {
        columns: vec![TypedColumn {
            name: col.to_string(),
            ty: PgColType::Text,
        }],
        rows: vec![vec![serde_json::Value::String(val.to_string())]],
    }
}

// ── per-connection copy + transaction state ───────────────────────────────────

/// Per-connection `COPY … FROM STDIN` state (CONCEPT:EG-KG.query.register-each-user-table): the resolved target,
/// the streamed bytes accumulated across copy frames, and the decode format. Lives in
/// [`WireSession::copy`] between the copy-in response and the wire's copy-done hook.
pub struct CopyState {
    pub(crate) table: String,
    /// The resolved insert column list (the COPY column list, or all schema columns).
    pub(crate) columns: Vec<String>,
    pub(crate) format: CopyFormat,
    pub(crate) delimiter: Option<char>,
    pub(crate) header: bool,
    /// Accumulated raw bytes of the copy-in body.
    pub(crate) buf: Vec<u8>,
}

impl CopyState {
    /// The resolved target table.
    pub fn table(&self) -> &str {
        &self.table
    }
    /// The resolved insert column list (the COPY column list, or all schema columns).
    pub fn columns(&self) -> &[String] {
        &self.columns
    }
    /// The declared copy wire format.
    pub fn format(&self) -> CopyFormat {
        self.format
    }
    /// The explicit field delimiter, if any.
    pub fn delimiter(&self) -> Option<char> {
        self.delimiter
    }
    /// Whether the copy body carries a header row (CSV).
    pub fn header(&self) -> bool {
        self.header
    }
    /// The accumulated copy-in body bytes.
    pub fn buf(&self) -> &[u8] {
        &self.buf
    }
}

/// The buffered graph-node ops of an OPEN wire transaction (CONCEPT:EG-KG.compute.kg-transaction-is-pinned).
/// Compiled into one authoritative `MutationBatch` at `COMMIT`; dropped on `ROLLBACK`.
#[derive(Default)]
struct GraphTxnBuffer {
    ops: Vec<NodeOp>,
}

/// One buffered graph-node mutation inside an open wire transaction
/// (CONCEPT:EG-KG.compute.kg-transaction-is-pinned). Resolved (against a read-your-own-writes overlaid snapshot)
/// at statement time and compiled at `COMMIT`. Carries exactly what the RYOW
/// overlay and canonical durable `Method` need.
enum NodeOp {
    /// `AddNode` — `blob` is the MessagePack-encoded property object.
    Add { id: String, blob: Vec<u8> },
    /// `CompareAndSetNodeFields` — merge `updates` when every `conditions` field
    /// matches (an empty `conditions` is an unconditional merge, matching the
    /// single-statement `UPDATE nodes` path).
    Cas {
        id: String,
        conditions: serde_json::Map<String, serde_json::Value>,
        updates: serde_json::Map<String, serde_json::Value>,
    },
    /// `RemoveNode`.
    Remove { id: String },
}

/// The CROSS-MODAL write-set staged inside an open wire transaction (CONCEPT:EG-KG.txn.isolation-ryow-begin-set).
/// Parallel to the graph-node [`GraphTxnBuffer`] + user-table `TableTxn` buffers, this
/// holds the NON-graph-topology modalities a pgwire `SET EMBEDDING` / `INSERT INTO
/// series` / `SPARQL UPDATE` / `SPARQL CONSTRUCT` statement stages while a `BEGIN` is
/// open. At `COMMIT` they are handed to the SHARED RPC cross-modal commit
/// ([`crate::server::handlers::txn::commit_cross_modal_txn`]) so graph + vector + OWL
/// modalities land atomically in ONE redb `WriteTransaction` — the SAME seam the RPC
/// `TxnAddEmbedding`/`TxnAxiom`/`TxnConstruct` path commits through (no logic duplicated).
#[cfg(feature = "query")]
#[derive(Default)]
struct XmodalStaged {
    /// Staged `(node_id, embedding)` vector upserts (from `SET EMBEDDING FOR …`).
    vectors: Vec<(String, Vec<f32>)>,
    /// Staged time-series measurement batches (from `INSERT INTO series …`). The
    /// `StagedMeasurement` type is ungated (the durable write is tsdb-gated in the
    /// backend); a build without a tsdb backend drops the points at commit.
    measurements: Vec<crate::server::txn::StagedMeasurement>,
    /// OWL-axiom (`SPARQL UPDATE INSERT DATA`) and `SPARQL CONSTRUCT` triples, already
    /// LOWERED to graph-native `AddNode`/`AddEdge` methods at stage time (reusing the
    /// SAME `triples_to_methods` lowering the RPC stagers use). Fed to the cross-modal
    /// commit as the txn's `axioms`, and overlaid onto an in-txn UQL read for RYOW.
    owl_methods: Vec<crate::protocol::Method>,
}

#[cfg(feature = "query")]
impl XmodalStaged {
    /// True when no cross-modal modality was staged — the txn is an ordinary
    /// graph/user-table txn and takes the byte-for-byte-unchanged commit path.
    fn is_empty(&self) -> bool {
        self.vectors.is_empty() && self.measurements.is_empty() && self.owl_methods.is_empty()
    }
}

// ── the shared session core ────────────────────────────────────────────────────

/// A per-connection, wire-agnostic SQL session (CONCEPT:EG-KG.compute.subsystems-reference). Holds the shared
/// `ServerState`, the current target graph (mutated by `SET graph = …`), the
/// authenticated actor, and the open mixed-store transaction buffers. One instance
/// per connection so the `SET graph` selection is connection-scoped. Shared by every
/// wire (Postgres today, more in Phase J/Y).
pub struct WireSession {
    state: Arc<RwLock<ServerState>>,
    /// Current connected graph for this connection. `parking_lot::Mutex` keeps the
    /// session `Send + Sync` without an async lock on the (synchronous) SET path.
    graph: parking_lot::Mutex<String>,
    /// False until the first query resolves the connection's target from the wire's
    /// startup metadata (priority 1). The session is built before startup, so this
    /// latches the graph once metadata is available (unless an explicit `SET graph`
    /// already chose one).
    startup_resolved: std::sync::atomic::AtomicBool,
    /// The authenticated actor (engine `agent_id`) for this connection
    /// (CONCEPT:EG-KG.query.concept-13). `None` means no authenticated wire identity
    /// has been mapped. Latched only from a verified request or the server-owned
    /// authority produced after a native wire cryptographic proof succeeds.
    actor: parking_lot::Mutex<Option<String>>,
    /// Opaque tenant/principal authority derived from a current signed request or
    /// from a server-owned native SQL proxy context after cryptographic login.
    authority: parking_lot::Mutex<Option<CarrierAuthority>>,
    /// The OPEN multi-statement transaction's buffered user-table ops (CONCEPT:EG-KG.query.register-each-user-table),
    /// or `None` when no `BEGIN` is active. `COMMIT` applies the buffer in ONE redb
    /// write txn; `ROLLBACK` drops it. Scoped per connection.
    txn: parking_lot::Mutex<Option<TableTxn>>,
    /// The OPEN transaction's buffered GRAPH-NODE ops (CONCEPT:EG-KG.compute.kg-transaction-is-pinned), committed as
    /// one authoritative MutationBatch before RAM publication. Empty when no
    /// `BEGIN` is active or the txn touched no nodes.
    graph_txn: parking_lot::Mutex<GraphTxnBuffer>,
    /// The graph a mixed-store transaction is pinned to, captured at `BEGIN`
    /// (CONCEPT:EG-KG.compute.kg-transaction-is-pinned / KG-2.207): a txn stays within ONE graph / redb shard, so
    /// `SET graph` is rejected while a txn is open. `None` when no txn is active.
    txn_graph: parking_lot::Mutex<Option<String>>,
    /// Whether the OPEN transaction has entered the ABORTED state (CONCEPT:EG-KG.compute.kg-transaction-is-pinned):
    /// a statement inside the txn errored, so every subsequent statement except
    /// `COMMIT`/`ROLLBACK` is rejected with SQLSTATE 25P02 until the block ends.
    /// Cleared on `BEGIN`/`COMMIT`/`ROLLBACK`.
    txn_failed: parking_lot::Mutex<bool>,
    /// In-flight `COPY … FROM STDIN` state (CONCEPT:EG-KG.query.register-each-user-table), set between the copy-in
    /// response and the wire's copy-done/copy-fail hook.
    copy: parking_lot::Mutex<Option<CopyState>>,
    /// The OPEN transaction's staged CROSS-MODAL write-set (CONCEPT:EG-KG.txn.isolation-ryow-begin-set) — vectors,
    /// measurements, and lowered OWL/CONSTRUCT methods from the pgwire cross-modal
    /// statements. Empty when no `BEGIN` is active or the txn touched no cross-modal
    /// modality; when non-empty at `COMMIT` the whole txn commits atomically through the
    /// shared RPC cross-modal seam.
    #[cfg(feature = "query")]
    xmodal: parking_lot::Mutex<XmodalStaged>,
    /// The isolation level resolved for the CURRENTLY open (or most recently
    /// closed) transaction (CONCEPT:EG-KG.txn.serializable-zero-cost — NE-005): from an inline
    /// `BEGIN … ISOLATION LEVEL …`, else a prior bare `SET TRANSACTION ISOLATION
    /// LEVEL …`, else the session default, else `Snapshot`. Deliberately NOT
    /// reset by `take_txn()` — the COMMIT path reads it AFTER `take_txn()` has
    /// already drained the buffers, so it must survive that call; only
    /// `begin_txn()` resets it (nothing off-txn ever reads it: a lone
    /// autocommit statement always commits under `Snapshot`, which is already
    /// exactly as strong as a single statement needs).
    txn_isolation: parking_lot::Mutex<crate::server::txn::IsolationLevel>,
    /// Session-level isolation default from `SET SESSION CHARACTERISTICS AS
    /// TRANSACTION ISOLATION LEVEL …` (CONCEPT:EG-KG.txn.serializable-zero-cost — NE-005). Applied to every
    /// subsequent `BEGIN` that does not name a level explicitly. `None` means
    /// the engine default (`Snapshot`).
    session_isolation_default: parking_lot::Mutex<Option<crate::server::txn::IsolationLevel>>,
    /// A bare `SET TRANSACTION ISOLATION LEVEL …` issued OUTSIDE an open
    /// transaction (CONCEPT:EG-KG.txn.serializable-zero-cost — NE-005, matching Postgres: it applies to the
    /// NEXT transaction only). Consumed (and cleared) by the next `BEGIN`.
    pending_isolation: parking_lot::Mutex<Option<crate::server::txn::IsolationLevel>>,
    /// Predicate reads this OPEN transaction's own reads performed (CONCEPT:EG-KG.txn.serializable-zero-cost — NE-005
    /// auto predicate tracking), fingerprinted AT READ TIME (never deferred to
    /// commit — see [`crate::server::txn::GraphTxnState::add_predicate_read`]'s
    /// doc for why the timing matters) and captured only while `txn_isolation`
    /// is `Serializable`. Merged directly into the freshly-built
    /// `GraphTxnState.predicate_reads` at commit (the wire session has no live
    /// `GraphTxnState` before then — its transaction is buffered, not staged)
    /// so a SQL `SERIALIZABLE` transaction gets GENUINE phantom protection for
    /// the read shape the engine's predicate machinery can express, instead of
    /// silently behaving like `Snapshot`. Reset by `begin_txn()`.
    serializable_predicate_reads:
        parking_lot::Mutex<Vec<(crate::server::txn::PredicateRead, u64)>>,
    /// The OPEN transaction's table-store statement replay log (CONCEPT:EG-TXN.mixed-commit-intent — NE-004):
    /// one entry per buffered table DML/DDL statement (or decoded `COPY`
    /// batch), in order. Used ONLY when a transaction mixes graph-node ops
    /// with table ops, to durably record how to rebuild the table-side
    /// `TableTxn` on crash recovery (`eg_query::TxnOp` is not `Serialize`).
    /// Reset by `begin_txn()`.
    txn_replay_log: parking_lot::Mutex<Vec<crate::server::txn_intent::ReplayStep>>,
    /// Set once this connection has swept its owner's leftover commit-intent
    /// files (CONCEPT:EG-TXN.mixed-commit-intent — NE-004 crash recovery). Checked lazily on first use rather
    /// than at connection setup so it costs nothing on a connection that never
    /// touches a mixed transaction.
    recovered_intents: std::sync::atomic::AtomicBool,
}

impl WireSession {
    /// Build a fresh per-connection session. `default_graph` is the graph a new
    /// connection runs against until `SET graph`/startup overrides it. The session
    /// remains unusable until the wire binds an authenticated actor.
    pub fn new(state: Arc<RwLock<ServerState>>, default_graph: String) -> Self {
        Self {
            state,
            graph: parking_lot::Mutex::new(default_graph),
            startup_resolved: std::sync::atomic::AtomicBool::new(false),
            actor: parking_lot::Mutex::new(None),
            authority: parking_lot::Mutex::new(None),
            txn: parking_lot::Mutex::new(None),
            graph_txn: parking_lot::Mutex::new(GraphTxnBuffer::default()),
            txn_graph: parking_lot::Mutex::new(None),
            txn_failed: parking_lot::Mutex::new(false),
            copy: parking_lot::Mutex::new(None),
            #[cfg(feature = "query")]
            xmodal: parking_lot::Mutex::new(XmodalStaged::default()),
            txn_isolation: parking_lot::Mutex::new(crate::server::txn::IsolationLevel::Snapshot),
            session_isolation_default: parking_lot::Mutex::new(None),
            pending_isolation: parking_lot::Mutex::new(None),
            serializable_predicate_reads: parking_lot::Mutex::new(Vec::new()),
            txn_replay_log: parking_lot::Mutex::new(Vec::new()),
            recovered_intents: std::sync::atomic::AtomicBool::new(false),
        }
    }

    /// Verify and bind a current signed request for an auxiliary SQL carrier.
    /// The first request fixes the connection actor; later requests must carry
    /// the same verified actor and current graph.
    pub(crate) async fn authenticate_request(&self, request: &Request) -> WireResult<()> {
        let (secret, persist_dir) = {
            let state = self.state.read().await;
            (state.auth_secret.clone(), state.persist_dir.clone())
        };
        let context = crate::server::auth::verify_request_with_security_dir(
            &secret,
            request,
            persist_dir.as_deref(),
        )
        .map_err(|message| {
            crate::metrics::auth_failure();
            WireError {
                code: "28000".to_string(),
                message,
            }
        })?;
        let verified_authority = CarrierAuthority::from_verified(&context).map_err(|message| {
            crate::metrics::access_denied();
            WireError {
                code: "28000".to_string(),
                message,
            }
        })?;

        let policy = eg_capabilities::policy(&request.method);
        if !context.allows_method(policy.authz_action, policy.mutates) {
            crate::metrics::access_denied();
            return Err(WireError {
                code: "42501".to_string(),
                message: format!(
                    "verified request context lacks required scope '{}'",
                    policy.authz_action
                ),
            });
        }
        if request.graph != self.current_graph() {
            return Err(WireError {
                code: "3D000".to_string(),
                message: "signed request graph does not match the connection graph".to_string(),
            });
        }

        let verified_actor = context.agent_id();
        self.bind_authority(verified_authority, verified_actor)
    }

    /// Bind the server-owned carrier for a native SQL connection after its
    /// protocol adapter has verified the mandatory SCRAM/HMAC password proof.
    pub(crate) async fn bind_authenticated_sql_actor(
        &self,
        protocol: &str,
        actor: &str,
    ) -> WireResult<()> {
        let secret = self.state.read().await.auth_secret.clone();
        let context = crate::server::auth::VerifiedRequestContext::authenticated_sql_wire_actor(
            &secret, protocol, actor,
        )
        .map_err(|message| WireError {
            code: "28000".to_string(),
            message,
        })?;
        let authority = CarrierAuthority::from_verified(&context).map_err(|message| WireError {
            code: "28000".to_string(),
            message,
        })?;
        self.bind_authority(authority, context.agent_id())
    }

    fn bind_authority(
        &self,
        verified_authority: CarrierAuthority,
        verified_actor: &str,
    ) -> WireResult<()> {
        if verified_actor.trim().is_empty() {
            crate::metrics::access_denied();
            return Err(WireError {
                code: "28000".to_string(),
                message: "verified wire identity is required".to_string(),
            });
        }
        {
            let mut authority = self.authority.lock();
            match authority.as_ref() {
                Some(bound) if bound != &verified_authority => {
                    return Err(WireError {
                        code: "28000".to_string(),
                        message: "verified authority cannot change within a connection".to_string(),
                    })
                }
                Some(_) => {}
                None => *authority = Some(verified_authority),
            }
        }
        let mut actor = self.actor.lock();
        match actor.as_deref() {
            Some(bound) if bound != verified_actor => Err(WireError {
                code: "28000".to_string(),
                message: "verified actor cannot change within a connection".to_string(),
            }),
            Some(_) => Ok(()),
            None => {
                *actor = Some(verified_actor.to_string());
                Ok(())
            }
        }
    }

    fn carrier_authority(&self) -> WireResult<CarrierAuthority> {
        self.authority.lock().clone().ok_or_else(|| WireError {
            code: "28000".to_string(),
            message: "current signed tenant authority is required".to_string(),
        })
    }

    /// Resolve the actor bound by the same verified authority as the carrier.
    /// Transport sessions may exist before authentication, but no stateful
    /// operation may turn that absence into an empty ACL identity.
    fn verified_actor(&self) -> WireResult<String> {
        self.carrier_authority()?;
        self.actor
            .lock()
            .as_deref()
            .filter(|actor| !actor.trim().is_empty())
            .map(str::to_owned)
            .ok_or_else(|| WireError {
                code: "28000".to_string(),
                message: "verified wire identity is required".to_string(),
            })
    }

    #[cfg(feature = "security")]
    async fn filter_view_for_verified_actor(
        &self,
        view: &mut crate::graph::GraphView,
    ) -> WireResult<()> {
        let actor = self.verified_actor()?;
        self.state.read().await.isolation.filter_view(&actor, view);
        Ok(())
    }

    /// Resolve this connection's owner-scoped SQL catalog.  A verified carrier and
    /// the served engine's configured persistence directory are both mandatory.
    pub(crate) async fn user_table_store(&self) -> WireResult<TableStore> {
        let authority = self.carrier_authority()?;
        let persist_dir = self.state.read().await.persist_dir.clone();
        crate::server::sql_tables::user_table_store(
            &authority,
            persist_dir.as_deref().map(std::path::Path::new),
        )
        .map_err(user_err)
    }

    /// Commit a decoded native-wire COPY batch through the same owner-scoped
    /// SQL MutationBatch kernel as ordinary table DML.
    pub(crate) async fn commit_copy_rows(
        &self,
        table: String,
        columns: Vec<String>,
        rows: Vec<Vec<serde_json::Value>>,
    ) -> WireResult<usize> {
        let count = rows.len();
        if self.in_txn() {
            // NE-004: a `COPY … FROM STDIN` batch arrives as raw wire frames,
            // never as a single literal SQL statement, so it cannot be
            // recorded in the replay log as `ReplayStep::Sql` the way an
            // ordinary buffered statement is — record its decoded shape
            // directly instead (see `ReplayStep::CopyRows`'s doc).
            self.txn_replay_log
                .lock()
                .push(crate::server::txn_intent::ReplayStep::CopyRows {
                    table: table.clone(),
                    columns: columns.clone(),
                    rows: rows.clone(),
                });
            self.buffer(TxnOp::Insert {
                table,
                col_order: columns,
                rows,
            });
            return Ok(count);
        }
        let mut txn = TableTxn::new();
        txn.push(TxnOp::Insert {
            table,
            col_order: columns,
            rows,
        });
        let graph = self.current_graph();
        self.commit_table_txn(&graph, "COPY", txn).await
    }

    /// Buffer a user-table op into the open transaction (panics if none open — callers
    /// guard with [`WireProtocol::in_txn`]).
    fn buffer(&self, op: TxnOp) {
        if let Some(t) = self.txn.lock().as_mut() {
            t.push(op);
        }
    }

    /// Buffer a graph-node op into the open transaction's node buffer (CONCEPT:EG-KG.compute.kg-transaction-is-pinned).
    fn buffer_node(&self, op: NodeOp) {
        self.graph_txn.lock().ops.push(op);
    }

    /// Whether the OPEN transaction is in the ABORTED state (CONCEPT:EG-KG.compute.kg-transaction-is-pinned).
    fn txn_aborted(&self) -> bool {
        *self.txn_failed.lock()
    }

    /// Open a fresh transaction (CONCEPT:EG-KG.compute.kg-transaction-is-pinned): reset both buffers + the aborted
    /// flag and pin the txn to the current graph (a txn stays within one shard).
    fn begin_txn(&self) {
        *self.txn.lock() = Some(TableTxn::new());
        self.graph_txn.lock().ops.clear();
        *self.txn_failed.lock() = false;
        *self.txn_graph.lock() = Some(self.current_graph());
        #[cfg(feature = "query")]
        {
            *self.xmodal.lock() = XmodalStaged::default();
        }
        // NE-005: baseline reset — `execute()`'s `StatementKind::Begin` arm
        // immediately overwrites this with the resolved level (inline clause,
        // else `pending_isolation`, else the session default, else Snapshot).
        *self.txn_isolation.lock() = crate::server::txn::IsolationLevel::Snapshot;
        self.serializable_predicate_reads.lock().clear();
        self.txn_replay_log.lock().clear();
    }

    /// Drain the open transaction's staged CROSS-MODAL write-set (CONCEPT:EG-KG.txn.isolation-ryow-begin-set),
    /// leaving it empty. Called by `COMMIT` (to hand the modalities to the shared
    /// cross-modal commit) and `ROLLBACK` (drain-and-drop).
    #[cfg(feature = "query")]
    fn take_xmodal(&self) -> XmodalStaged {
        std::mem::take(&mut self.xmodal.lock())
    }

    /// End the OPEN transaction (CONCEPT:EG-KG.compute.kg-transaction-is-pinned): drop both buffers, the pinned
    /// graph, and the aborted flag. Shared by COMMIT and ROLLBACK. Returns the
    /// user-table `TableTxn` (if any) and the buffered node ops, so COMMIT can
    /// apply them.
    fn take_txn(&self) -> (Option<TableTxn>, Vec<NodeOp>) {
        let table = self.txn.lock().take();
        let nodes = std::mem::take(&mut self.graph_txn.lock().ops);
        *self.txn_graph.lock() = None;
        *self.txn_failed.lock() = false;
        (table, nodes)
    }

    /// Append a copy-in data frame's bytes into the per-connection copy buffer
    /// (CONCEPT:EG-KG.query.register-each-user-table). The wire's copy-data hook calls this; a frame with no COPY in
    /// progress is a protocol error.
    pub(crate) fn append_copy_data(&self, data: &[u8]) -> WireResult<()> {
        if let Some(state) = self.copy.lock().as_mut() {
            state.buf.extend_from_slice(data);
            Ok(())
        } else {
            Err(user_err("COPY data received with no COPY in progress"))
        }
    }

    /// Take the in-flight copy state (CONCEPT:EG-KG.query.register-each-user-table) at copy-done, leaving `None`.
    pub(crate) fn take_copy_state(&self) -> Option<CopyState> {
        self.copy.lock().take()
    }

    /// Resolve `SET graph = '<name>'` / `SET graph TO <name>`. Returns `Some(outcome)`
    /// when the statement IS a graph SET (handled here), else `None` (not ours).
    fn try_set_graph(&self, sql: &str) -> Option<WireResult<WireOutcome>> {
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
        // CONCEPT:EG-KG.compute.kg-transaction-is-pinned / KG-2.207 — a transaction is pinned to ONE graph (redb
        // shard) at BEGIN; switching graphs mid-txn would split a supposedly-atomic
        // commit across shards, so reject it.
        if self.in_txn() {
            return Some(Err(user_err(
                "cannot SET graph inside a transaction (a transaction is scoped to one graph)",
            )));
        }
        *self.graph.lock() = name;
        Some(Ok(WireOutcome::command("SET")))
    }

    /// Resolve `SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL …`
    /// (the session default for every future `BEGIN`) or `SET TRANSACTION
    /// ISOLATION LEVEL …` (CONCEPT:EG-KG.txn.serializable-zero-cost — NE-005: applies to the CURRENTLY open
    /// transaction if one exists, else to the NEXT `BEGIN` — matching
    /// Postgres). Neither has a `sqlparser` AST arm `eg_query::classify`
    /// recognizes (no `Statement::SetTransaction`/`Statement::SetSessionParam`
    /// case — everything unmatched falls to its `other => Err(...)`
    /// catch-all), so — exactly like `try_set_graph` above — these are
    /// intercepted TEXTUALLY BEFORE the classifier ever sees them. Returns
    /// `Some(outcome)` when `sql` IS one of these two statements (handled
    /// here), else `None` (not ours).
    fn try_set_isolation(&self, sql: &str) -> Option<WireResult<WireOutcome>> {
        let trimmed = sql.trim().trim_end_matches(';').trim();
        let lower = trimmed.to_ascii_lowercase();

        if let Some(rest) =
            lower.strip_prefix("set session characteristics as transaction isolation level")
        {
            let level_text = &trimmed[trimmed.len() - rest.len()..];
            return Some(match parse_sql_isolation_level(level_text) {
                Ok(level) => {
                    *self.session_isolation_default.lock() = Some(level);
                    Ok(WireOutcome::command("SET"))
                }
                Err(e) => Err(e),
            });
        }
        if let Some(rest) = lower.strip_prefix("set transaction isolation level") {
            let level_text = &trimmed[trimmed.len() - rest.len()..];
            return Some(match parse_sql_isolation_level(level_text) {
                Ok(level) => {
                    if self.in_txn() {
                        *self.txn_isolation.lock() = level;
                    } else {
                        *self.pending_isolation.lock() = Some(level);
                    }
                    Ok(WireOutcome::command("SET"))
                }
                Err(e) => Err(e),
            });
        }
        None
    }

    /// Clone the target graph's `Arc<GraphCore>` out of the registry (read lock),
    /// or a clean error if it doesn't exist.
    pub(crate) async fn graph_core(&self, graph: &str) -> WireResult<Arc<crate::graph::GraphCore>> {
        let s = self.state.read().await;
        match s.registry.get(graph) {
            Some(e) => Ok(e.core.clone()),
            None => Err(user_err(format!("graph '{graph}' not found"))),
        }
    }

    /// Enforce the engine ACL for this connection's authenticated actor against
    /// `graph` at the requested `access` level (CONCEPT:EG-KG.query.concept-13).
    /// Unbound and unprovisioned actors are denied.
    async fn check_access(&self, graph: &str, access: AccessLevel) -> WireResult<()> {
        let actor = self.verified_actor()?;
        let s = self.state.read().await;
        let (graph_type, owner) = match s.registry.get(graph) {
            Some(e) => (e.graph_type, e.owner.clone()),
            // A missing graph is reported by the caller's own resolve; allow here so
            // the not-found error surfaces instead of a misleading ACCESS_DENIED.
            None => return Ok(()),
        };
        if s.isolation
            .check_access(&actor, graph, graph_type, owner.as_deref(), access)
        {
            Ok(())
        } else {
            crate::metrics::access_denied();
            Err(WireError {
                // 42501 — insufficient_privilege (what a real pg ACL denial reports).
                code: "42501".to_owned(),
                message: format!("permission denied for requested graph access ({access:?})"),
            })
        }
    }

    /// Execute a read (`SELECT`/`WITH`/…) over `graph` by reusing the EXACT
    /// DataFusion path `Method::Sql` uses: take the owned off-lock
    /// `analysis_snapshot()` and run the served, context-cached SQL exec on the
    /// blocking pool (DataFusion's executor must not run on a reactor worker).
    pub(crate) async fn run_read(&self, graph: &str, sql: String) -> WireResult<TypedQueryResult> {
        let core = self.graph_core(graph).await?;
        // `analysis_snapshot_versioned` (not the bare `analysis_snapshot`) so the OCC
        // version keying the served context cache below is taken ATOMICALLY with the
        // snapshot it describes.
        let (mut snap, graph_version) = core.analysis_snapshot_versioned();
        // W1.6/P7 site 3: node epoch for the SQL-context node-batch sub-cache (see the RPC SQL
        // handler for the rationale). Folds in the coarse floor when result-cache is on; else the
        // graph version (correct, no reuse).
        #[cfg(feature = "result-cache")]
        let node_epoch = core.dep_clock().node_epoch();
        #[cfg(not(feature = "result-cache"))]
        let node_epoch = graph_version;
        let in_txn = self.in_txn();
        // CONCEPT:EG-KG.compute.kg-transaction-is-pinned — read-your-own-writes: overlay this connection's buffered
        // graph-node ops onto the snapshot so a SELECT (or a candidate-id / RETURNING
        // read) issued INSIDE an open transaction observes the transaction's own
        // uncommitted inserts/updates/deletes. Off-txn reads are byte-for-byte
        // unchanged (the buffer is empty).
        if in_txn {
            self.apply_node_buffer(&mut snap);
        }
        #[cfg(feature = "security")]
        self.filter_view_for_verified_actor(&mut snap).await?;
        // CONCEPT:EG-KG.query.register-user-tables-alongside: register the user tables alongside the graph projection so a
        // SELECT can read a user table, JOIN it to `nodes`/`edges`, or both in ONE plan.
        let store = self.user_table_store().await?;

        // An in-txn overlaid snapshot carries THIS connection's own buffered
        // (uncommitted) writes — content [`SqlContextEpoch`] cannot see (staged
        // writes don't bump `version()`), so it must never be served from, or land
        // in, the shared served context cache. Mirrors the identical precedent
        // `run_unified_overlaid`'s result-cache skip already sets: "no result cache
        // on this path". Off-txn (the overwhelming common case) uses the amortized
        // cached path below.
        if in_txn {
            return tokio::task::spawn_blocking(move || {
                eg_query::exec_sql_typed_with_tables(&snap, &store, &sql)
            })
            .await
            .map_err(|e| user_err(format!("query task failed: {e}")))?
            .map_err(|msg| user_err(format!("SQL error: {msg}")));
        }

        // CONCEPT:EG-KG.query.served-context-cache — the whole-`SessionContext` cache (UDFs, durable
        // views, synthesized system catalogs), amortized across every served SQL read
        // for this owner. Same registry `sql_tables::sql_context_cache` resolves by
        // as `user_table_store` above, so repeated reads from the SAME tenant+actor
        // reuse the SAME instance.
        let authority = self.carrier_authority()?;
        let tenant_scope = authority.tenant_scope().to_string();
        let caller = self.verified_actor()?;
        let graph_owned = graph.to_string();
        let persist_dir = self.state.read().await.persist_dir.clone();
        let cache = crate::server::sql_tables::sql_context_cache(
            &authority,
            persist_dir.as_deref().map(std::path::Path::new),
        )
        .map_err(user_err)?;
        tokio::task::spawn_blocking(move || {
            eg_query::exec_sql_typed_with_tables_cached_cancellable(
                &snap,
                graph_version,
                node_epoch,
                &tenant_scope,
                &graph_owned,
                &caller,
                &store,
                &cache,
                &sql,
                &eg_query::CancellationToken::new(),
            )
        })
        .await
        .map_err(|e| user_err(format!("query task failed: {e}")))?
        .map_err(|msg| user_err(format!("SQL error: {msg}")))
    }

    /// Replay this connection's buffered graph-node ops onto `view` (CONCEPT:EG-KG.compute.kg-transaction-is-pinned),
    /// in statement order, so a read over `view` reflects the open transaction's own
    /// writes. Never touches the live `GraphCore`.
    fn apply_node_buffer(&self, view: &mut crate::graph::GraphView) {
        for op in &self.graph_txn.lock().ops {
            match op {
                NodeOp::Add { id, blob } => view.overlay_add_node(id.clone(), blob.clone()),
                NodeOp::Cas {
                    id,
                    conditions,
                    updates,
                } => {
                    view.overlay_compare_and_set_fields(id, conditions, updates);
                }
                NodeOp::Remove { id } => view.overlay_remove_node(id),
            }
        }
    }

    /// Build a read-your-own-writes overlaid snapshot of `graph` for the OPEN
    /// transaction (CONCEPT:EG-KG.compute.kg-transaction-is-pinned) — the live snapshot plus every buffered node op.
    /// Used to resolve RETURNING rows / ON CONFLICT checks against the txn's own
    /// uncommitted state.
    async fn overlaid_snapshot(&self, graph: &str) -> WireResult<crate::graph::GraphView> {
        let core = self.graph_core(graph).await?;
        let mut view = core.analysis_snapshot();
        self.apply_node_buffer(&mut view);
        #[cfg(feature = "security")]
        self.filter_view_for_verified_actor(&mut view).await?;
        Ok(view)
    }

    /// The classify → read/write dispatch (CONCEPT:EG-KG.query.describe / EG-049), factored out
    /// of [`WireSession::execute`] so the caller can latch the aborted-txn state on a
    /// returned error. When `in_txn`, graph-node and user-table DML buffer instead of
    /// applying; otherwise behavior is the immediate path.
    async fn dispatch_kind(
        &self,
        graph: &str,
        sql: &str,
        kind: StatementKind,
        in_txn: bool,
    ) -> WireResult<WireOutcome> {
        match kind {
            StatementKind::Read => {
                let result = self.run_read(graph, sql.to_string()).await?;
                Ok(WireOutcome::Rows(result))
            }
            // ── graph-node DML (CONCEPT:EG-KG.compute.kg-transaction-is-pinned): buffer when in a txn ──────────────
            StatementKind::InsertNodes(ins) if in_txn => self.buffer_insert(graph, sql, ins).await,
            StatementKind::InsertNodes(ins) => self.run_insert(graph, sql, ins).await,
            StatementKind::UpdateNodes(upd) if in_txn => self.buffer_update(graph, sql, upd).await,
            StatementKind::UpdateNodes(upd) => self.run_update(graph, sql, upd).await,
            StatementKind::DeleteNodes(del) if in_txn => self.buffer_delete(graph, sql, del).await,
            StatementKind::DeleteNodes(del) => self.run_delete(graph, sql, del).await,
            // ── arbitrary user-defined relational tables (CONCEPT:EG-KG.query.register-user-tables-alongside/EG-020) ───
            StatementKind::CreateTable(plan) if in_txn => {
                let columns = to_store_columns(&plan.columns)?;
                self.buffer(TxnOp::CreateTable {
                    schema: TableSchema::new(plan.name, columns),
                    if_not_exists: plan.if_not_exists,
                });
                Ok(WireOutcome::command("CREATE TABLE"))
            }
            StatementKind::CreateTable(plan) => self.run_create_table(graph, sql, plan).await,
            StatementKind::DropTable(plan) if in_txn => {
                self.buffer(TxnOp::DropTable {
                    name: plan.name,
                    if_exists: plan.if_exists,
                });
                Ok(WireOutcome::command("DROP TABLE"))
            }
            StatementKind::DropTable(plan) => self.run_drop_table(graph, sql, plan).await,
            // CONCEPT:EG-KG.query.register-user-tables-alongside ADD COLUMN + CONCEPT:EG-KG.query.rename-table-moves-catalog the rest — staged into the txn.
            StatementKind::AlterTable(plan) if in_txn => {
                self.buffer(alter_txn_op(plan)?);
                Ok(WireOutcome::command("ALTER TABLE"))
            }
            StatementKind::AlterTable(plan) => self.run_alter_table(graph, sql, plan).await,
            StatementKind::InsertTable(ins) if in_txn => {
                let n = ins.rows.len();
                self.buffer(TxnOp::Insert {
                    table: ins.table,
                    col_order: ins.columns,
                    rows: ins.rows,
                });
                Ok(WireOutcome::command_rows("INSERT", n))
            }
            StatementKind::InsertTable(ins) => self.run_insert_table(graph, sql, ins).await,
            StatementKind::InsertSelect(ins) if in_txn => {
                // The SELECT half is a read (runs immediately); only the INSERT is
                // buffered into the transaction.
                let result = self.run_read(graph, ins.select_sql).await?;
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
                Ok(WireOutcome::command_rows("INSERT", n))
            }
            StatementKind::InsertSelect(ins) => self.run_insert_select(graph, sql, ins).await,
            StatementKind::UpdateTable(upd) if in_txn => {
                self.buffer(TxnOp::Update {
                    table: upd.table,
                    set: upd.set,
                    selector: upd.selector.pred,
                });
                Ok(WireOutcome::command("UPDATE"))
            }
            StatementKind::UpdateTable(upd) => self.run_update_table(graph, sql, upd).await,
            StatementKind::DeleteTable(del) if in_txn => {
                self.buffer(TxnOp::Delete {
                    table: del.table,
                    selector: del.selector.pred,
                });
                Ok(WireOutcome::command("DELETE"))
            }
            StatementKind::DeleteTable(del) => self.run_delete_table(graph, sql, del).await,
            // CONCEPT:EG-KG.query.insert-into-nodes-select — INSERT INTO nodes … SELECT (facade dispatch).
            StatementKind::InsertNodesSelect(ins) if in_txn => {
                self.buffer_insert_nodes_select(graph, sql, ins).await
            }
            StatementKind::InsertNodesSelect(ins) => {
                self.run_insert_nodes_select(graph, sql, ins).await
            }
            // CONCEPT:EG-KG.query.update-delete-from — UPDATE nodes … FROM … / DELETE FROM nodes … USING … .
            StatementKind::UpdateNodesJoin(upd) if in_txn => {
                self.buffer_update_nodes_join(graph, sql, upd).await
            }
            StatementKind::UpdateNodesJoin(upd) => {
                self.run_update_nodes_join(graph, sql, upd).await
            }
            StatementKind::DeleteNodesJoin(del) if in_txn => {
                self.buffer_delete_nodes_join(graph, sql, del).await
            }
            StatementKind::DeleteNodesJoin(del) => {
                self.run_delete_nodes_join(graph, sql, del).await
            }
            // CONCEPT:EG-KG.query.create-drop-view — CREATE/DROP VIEW over the durable view catalog.
            StatementKind::CreateView(plan) if in_txn => {
                self.buffer(TxnOp::CreateView {
                    name: plan.name,
                    select_sql: plan.select_sql,
                    or_replace: plan.or_replace,
                });
                Ok(WireOutcome::command("CREATE VIEW"))
            }
            StatementKind::CreateView(plan) => self.run_create_view(graph, sql, plan).await,
            StatementKind::DropView(plan) if in_txn => {
                self.buffer(TxnOp::DropView {
                    name: plan.name,
                    if_exists: plan.if_exists,
                });
                Ok(WireOutcome::command("DROP VIEW"))
            }
            StatementKind::DropView(plan) => self.run_drop_view(graph, sql, plan).await,
            // CONCEPT:EG-KG.query.create-drop-extension-over — CREATE/DROP EXTENSION over the durable extension catalog.
            StatementKind::CreateExtension {
                name,
                if_not_exists,
            } if in_txn => {
                self.buffer(TxnOp::CreateExtension {
                    name,
                    if_not_exists,
                });
                Ok(WireOutcome::command("CREATE EXTENSION"))
            }
            StatementKind::CreateExtension {
                name,
                if_not_exists,
            } => {
                self.run_create_extension(graph, sql, name, if_not_exists)
                    .await
            }
            StatementKind::DropExtension { name, if_exists } if in_txn => {
                self.buffer(TxnOp::DropExtension { name, if_exists });
                Ok(WireOutcome::command("DROP EXTENSION"))
            }
            StatementKind::DropExtension { name, if_exists } => {
                self.run_drop_extension(graph, sql, name, if_exists).await
            }
            // CONCEPT:EG-KG.query.create-drop-function — CREATE/DROP FUNCTION over the durable function catalog.
            StatementKind::CreateFunction(plan) if in_txn => {
                self.buffer(TxnOp::CreateFunction {
                    function: plan.func,
                    or_replace: plan.or_replace,
                });
                Ok(WireOutcome::command("CREATE FUNCTION"))
            }
            StatementKind::CreateFunction(plan) => self.run_create_function(graph, sql, plan).await,
            StatementKind::DropFunction(plan) if in_txn => {
                self.buffer(TxnOp::DropFunction {
                    name: plan.name,
                    if_exists: plan.if_exists,
                });
                Ok(WireOutcome::command("DROP FUNCTION"))
            }
            StatementKind::DropFunction(plan) => self.run_drop_function(graph, sql, plan).await,
            // ── Postgres-family extension parity (wave 19) ──────────────────────────
            // CONCEPT:EG-KG.query.postgres-family-extension-plan — Apache AGE cypher() set-returning function.
            StatementKind::CypherCall(plan) => self.run_cypher_call(graph, plan).await,
            // CONCEPT:EG-KG.query.real-ann-top-k — pgvector ANN index registration.
            StatementKind::CreateAnnIndex(plan) if in_txn => {
                self.buffer(TxnOp::PutAnnIndex { plan });
                Ok(WireOutcome::command("CREATE INDEX"))
            }
            StatementKind::CreateAnnIndex(plan) => {
                self.run_create_ann_index(graph, sql, plan).await
            }
            // CONCEPT:EG-KG.query.continuous-aggregate-lowering — TimescaleDB hypertable + continuous aggregate.
            StatementKind::CreateHypertable(plan) if in_txn => {
                let text = format!("public.{}", plan.table);
                self.buffer(TxnOp::PutHypertable { plan });
                Ok(WireOutcome::Rows(single_text_result(
                    "create_hypertable",
                    &text,
                )))
            }
            StatementKind::CreateHypertable(plan) => {
                self.run_create_hypertable(graph, sql, plan).await
            }
            StatementKind::CreateContinuousAggregate(plan) if in_txn => {
                self.buffer(TxnOp::CreateView {
                    name: plan.name,
                    select_sql: plan.select_sql,
                    or_replace: true,
                });
                Ok(WireOutcome::command("CREATE MATERIALIZED VIEW"))
            }
            StatementKind::CreateContinuousAggregate(plan) => {
                self.run_create_continuous_aggregate(graph, sql, plan).await
            }
            // `COPY … FROM STDIN` (CONCEPT:EG-KG.query.register-each-user-table): switch into copy-in mode; the
            // streamed rows are ingested by the wire's copy-done hook.
            StatementKind::CopyIn(plan) => self.start_copy(plan).await,
            // Transaction-control statements are handled before dispatch.
            StatementKind::Begin | StatementKind::Commit | StatementKind::Rollback => {
                unreachable!("transaction control handled before dispatch")
            }
        }
    }

    /// `COMMIT` a wire transaction (CONCEPT:EG-KG.compute.kg-transaction-is-pinned).
    /// Graph-node and cross-modal operations commit through the graph
    /// MutationBatch kernel; user-table/catalog operations commit through the
    /// SQL MutationBatch kernel — two INDEPENDENT redb authorities. A
    /// transaction that mixes a CROSS-MODAL write with user-table ops is
    /// rejected before either commits (see the `has_table_ops && has_xmodal`
    /// guard below — that combination stays unimplemented). A transaction
    /// that mixes plain GRAPH-NODE ops with user-table ops is fully
    /// supported and atomic across the two stores (CONCEPT:EG-TXN.mixed-commit-intent — NE-004):
    /// [`Self::commit_mixed_txn`] commits them through a durable commit-intent
    /// record with idempotent crash recovery, never landing one durably
    /// without the other even across a process crash between the two commits.
    ///
    /// An aborted transaction (a statement inside it errored, CONCEPT:EG-KG.compute.kg-transaction-is-pinned) commits
    /// as a ROLLBACK — nothing is applied. A `COMMIT` with no open transaction is a
    /// no-op (Postgres-compatible).
    async fn run_commit(&self) -> WireResult<WireOutcome> {
        // A COMMIT while the txn is aborted behaves as ROLLBACK (drop everything).
        if self.in_txn() && self.txn_aborted() {
            self.take_txn();
            #[cfg(feature = "query")]
            let _ = self.take_xmodal();
            return Ok(WireOutcome::TxnEnd { tag: "ROLLBACK" });
        }
        // The graph a txn was pinned to at BEGIN (node ops are scoped to it).
        let graph = self.txn_graph.lock().clone();
        // Drain the staged cross-modal write-set (CONCEPT:EG-KG.txn.isolation-ryow-begin-set) BEFORE the buffers.
        #[cfg(feature = "query")]
        let xmodal = self.take_xmodal();
        #[cfg(feature = "query")]
        let has_xmodal = !xmodal.is_empty();
        #[cfg(not(feature = "query"))]
        let has_xmodal = false;
        let (table_txn, node_ops) = self.take_txn();
        // Postgres-compatible no-op: COMMIT with no BEGIN.
        if table_txn.is_none() && node_ops.is_empty() && graph.is_none() && !has_xmodal {
            return Ok(WireOutcome::TxnEnd { tag: "COMMIT" });
        }

        let has_table_ops = table_txn
            .as_ref()
            .is_some_and(|transaction| !transaction.ops.is_empty());
        // Only the CROSS-MODAL + user-table combination is unimplemented: the
        // `has_xmodal` branch just below commits through the shared cross-modal
        // seam and returns before ever reaching the sequenced user-table commit
        // at the bottom of this function, so a staged `table_txn` would be
        // silently dropped. Plain graph-node ops + user-table ops are NOT
        // mutually exclusive — they are deliberately committed sequenced (see the
        // "ordinary mixed-store path" below), so this guard must not reject that
        // combination (it previously did, contradicting the sequencing code it
        // guards and the docs immediately below).
        if has_table_ops && has_xmodal {
            return Err(user_err(
                "one SQL transaction cannot mix a cross-modal write and user-table durability domains",
            ));
        }

        // ── EG-372 cross-modal COMMIT: when the txn staged any cross-modal modality
        // (vector / measurement / OWL / CONSTRUCT), hand the WHOLE txn — graph nodes
        // PLUS every cross-modal modality — to the SHARED RPC cross-modal commit so all
        // land atomically in ONE redb WriteTransaction (the SAME seam the RPC
        // TxnAddEmbedding/TxnAxiom/TxnConstruct path commits through). The user-table
        // ops (which the graph cross-modal commit does not cover) are then committed
        // sequenced, exactly like the ordinary mixed-store path. ──
        #[cfg(feature = "query")]
        if has_xmodal {
            let graph = graph
                .clone()
                .ok_or_else(|| user_err("cross-modal transaction has no pinned graph"))?;
            // `new_txn_state` resolves the pinned graph's core (surfacing a not-found).
            let mixed_isolation = *self.txn_isolation.lock();
            let mut ts = self.new_txn_state(&graph, mixed_isolation).await?;
            ts.write_set = Self::node_ops_to_methods(&node_ops)?;
            ts.vectors = xmodal.vectors;
            #[cfg(feature = "tsdb")]
            {
                ts.measurements = xmodal
                    .measurements
                    .into_iter()
                    .map(|measurement| self.scope_measurement(&graph, measurement))
                    .collect::<WireResult<Vec<_>>>()?;
            }
            #[cfg(not(feature = "tsdb"))]
            if !xmodal.measurements.is_empty() {
                return Err(user_err(
                    "time-series transaction requires the tsdb feature",
                ));
            }
            ts.axioms = xmodal.owl_methods;
            self.commit_txn_state(ts).await?;
            return Ok(WireOutcome::TxnEnd { tag: "COMMIT" });
        }

        let has_node_ops = !node_ops.is_empty();
        // NE-004: a transaction that staged BOTH graph-node ops AND user-table
        // ops must land on both stores or neither, including across a crash
        // between the two commits — route it through the durable
        // commit-intent path instead of the plain sequential one below.
        if has_node_ops && has_table_ops {
            let graph = graph
                .clone()
                .ok_or_else(|| user_err("transaction has node ops but no pinned graph"))?;
            let table_txn = table_txn.filter(|t| !t.ops.is_empty()).ok_or_else(|| {
                user_err("internal error: has_table_ops without a table transaction")
            })?;
            return self.commit_mixed_txn(&graph, node_ops, table_txn).await;
        }

        // Graph store: compile the buffered methods into one authoritative batch.
        // The commit kernel writes durable state before publishing RAM.
        if has_node_ops {
            let graph = graph
                .clone()
                .ok_or_else(|| user_err("transaction has node ops but no pinned graph"))?;
            let methods = Self::node_ops_to_methods(&node_ops)?;
            let graph_isolation = *self.txn_isolation.lock();
            self.commit_graph_methods_with_op(&graph, methods, uuid::Uuid::new_v4(), graph_isolation)
                .await?;
        }

        // SQL catalog/table store: rows, result, OCC/fence, idempotency, and outbox
        // share one native redb transaction.
        if let Some(txn) = table_txn.filter(|transaction| !transaction.ops.is_empty()) {
            let graph = graph.unwrap_or_else(|| self.current_graph());
            self.commit_table_txn(&graph, "transaction", txn).await?;
        }
        Ok(WireOutcome::TxnEnd { tag: "COMMIT" })
    }

    /// The `node_id` a buffered graph-node method targets (CONCEPT:EG-TXN.mixed-commit-intent — NE-004
    /// compensation). `Self::node_ops_to_methods` only ever produces `AddNode`
    /// / `CompareAndSetNodeFields` / `RemoveNode`, so those are the only
    /// variants this needs to recognize.
    fn method_node_id(method: &crate::protocol::Method) -> Option<&str> {
        match method {
            crate::protocol::Method::AddNode { node_id, .. }
            | crate::protocol::Method::CompareAndSetNodeFields { node_id, .. }
            | crate::protocol::Method::RemoveNode { node_id } => Some(node_id.as_str()),
            _ => None,
        }
    }

    /// Snapshot the pre-txn durable state of every node `methods` targets, and
    /// build the inverse write that restores it (CONCEPT:EG-TXN.mixed-commit-intent — NE-004). MUST be called
    /// BEFORE the graph side commits, so it always sees the true
    /// pre-transaction image (a node present becomes an `AddNode` full
    /// overwrite restoring its EXACT prior property blob; a node absent
    /// becomes a `RemoveNode`). Only the FIRST (pre-txn) state per distinct
    /// node id is kept — a node touched by more than one buffered op in this
    /// txn only needs ONE restore step, order-independent across ids.
    async fn compensating_methods(
        &self,
        graph: &str,
        methods: &[crate::protocol::Method],
    ) -> WireResult<Vec<crate::protocol::Method>> {
        let core = self.graph_core(graph).await?;
        let mut seen = std::collections::HashSet::new();
        let mut compensating = Vec::new();
        for method in methods {
            let Some(node_id) = Self::method_node_id(method) else {
                continue;
            };
            if !seen.insert(node_id.to_string()) {
                continue;
            }
            match core.get_node_properties(node_id) {
                Some(bytes) => compensating.push(crate::protocol::Method::AddNode {
                    node_id: node_id.to_string(),
                    properties_msgpack: bytes,
                }),
                None => compensating.push(crate::protocol::Method::RemoveNode {
                    node_id: node_id.to_string(),
                }),
            }
        }
        Ok(compensating)
    }

    /// Commit a transaction that staged BOTH graph-node ops AND user-table ops
    /// (CONCEPT:EG-TXN.mixed-commit-intent — NE-004). Graph and user tables live in two INDEPENDENT redb
    /// databases — a separate redb file per owner for tables (see
    /// `crate::server::sql_tables`'s module doc; that per-owner catalog
    /// layout is a file this track does not own, and folding the two stores
    /// under one shared redb environment would mean changing it) — so a
    /// single shared `WriteTransaction` (option (a)) is not available here.
    /// Instead:
    ///
    ///   1. Snapshots the CURRENT durable state of every node the graph side
    ///      touches into `compensating_methods` — the exact inverse write.
    ///   2. Writes a durable, fsynced commit-intent record BEFORE either side
    ///      commits ([`crate::server::txn_intent::write_intent`]).
    ///   3. Commits the graph side, then the table side, both keyed by the
    ///      intent's own `operation_id` so either is idempotently replay-safe
    ///      (the SAME coordinator/idempotency keys `commit_cross_modal_txn` /
    ///      `TableStore::commit_txn_batch` already dedupe commits on).
    ///   4. On full success, deletes the intent — nothing left to recover.
    ///   5. On a CLEAN (non-crash) table-commit rejection, synchronously
    ///      COMPENSATES (undoes the already-committed graph write) before
    ///      returning the error, so the caller observes ZERO graph writes
    ///      applied.
    ///   6. On a CRASH between the two commits, the intent survives on disk;
    ///      the next `WireSession::recover_owner_intents_once` on this owner
    ///      resolves it the SAME way (steps 3-5) — self-healing, no torn
    ///      state, without needing the process to "restart" in any special
    ///      way — the very next statement this owner runs (on ANY
    ///      connection, including a freshly reconnected one after a real
    ///      process restart) triggers it.
    async fn commit_mixed_txn(
        &self,
        graph: &str,
        node_ops: Vec<NodeOp>,
        table_txn: TableTxn,
    ) -> WireResult<WireOutcome> {
        let methods = Self::node_ops_to_methods(&node_ops)?;
        let isolation = *self.txn_isolation.lock();
        // MUST run before either commit: it reads the durable pre-txn state.
        let compensating = self.compensating_methods(graph, &methods).await?;
        let table_steps = std::mem::take(&mut *self.txn_replay_log.lock());
        let operation_id = uuid::Uuid::new_v4();
        let intent = crate::server::txn_intent::CommitIntent::new(
            graph.to_string(),
            operation_id,
            methods,
            compensating,
            table_steps,
            crate::server::txn::now_ms(),
        );
        let authority = self.carrier_authority()?;
        let persist_dir_buf = self.state.read().await.persist_dir.clone();
        let persist_dir_buf = persist_dir_buf.ok_or_else(|| {
            user_err(
                "mixed graph+table transactions require the configured persistence directory",
            )
        })?;
        let persist_dir = std::path::Path::new(&persist_dir_buf);
        crate::server::txn_intent::write_intent(&authority, persist_dir, &intent)
            .map_err(user_err)?;
        // The live path already HAS the real, already-buffered `TableTxn` —
        // commit it directly rather than re-deriving it from the replay log
        // (that reconstruction is for RECOVERY, which has no live `TableTxn`
        // to work from).
        self.resolve_commit_intent(&authority, persist_dir, intent, isolation, Some(table_txn))
            .await?;
        Ok(WireOutcome::TxnEnd { tag: "COMMIT" })
    }

    /// Resolve one commit-intent to completion: replay the graph side
    /// (idempotent), then the table side (idempotent). On full success the
    /// intent is deleted. On a table-commit rejection, synchronously
    /// compensates the graph side (restores its pre-txn state) under a
    /// DETERMINISTIC child operation id, then deletes the intent and returns
    /// the ORIGINAL table error. Used by BOTH the live synchronous COMMIT
    /// path ([`Self::commit_mixed_txn`]) and the lazy crash-recovery sweep
    /// ([`Self::recover_owner_intents_once`]) — identically, since both are
    /// simply "finish (or undo) a transaction whose graph side may already
    /// be durably applied."
    async fn resolve_commit_intent(
        &self,
        authority: &CarrierAuthority,
        persist_dir: &Path,
        intent: crate::server::txn_intent::CommitIntent,
        isolation: crate::server::txn::IsolationLevel,
        live_table_txn: Option<TableTxn>,
    ) -> WireResult<()> {
        let operation_id = intent.operation_id();
        // Phase 1: graph side. Idempotent — a crash-recovery replay of an
        // ALREADY-applied write is a safe no-op (same coordinator key).
        if let Err(e) = self
            .commit_graph_methods_with_op(
                &intent.graph,
                intent.forward_methods.clone(),
                operation_id,
                isolation,
            )
            .await
        {
            // `commit_cross_modal_txn` commits through ONE redb
            // `WriteTransaction`: this failure means NOTHING landed on
            // either store yet, so there is nothing to compensate — just
            // discard the intent and surface the error.
            let _ =
                crate::server::txn_intent::delete_intent(authority, persist_dir, operation_id);
            return Err(e);
        }
        // Phase 2: table side. Idempotent — same reasoning, keyed the same
        // way. The live caller already has the real buffered `TableTxn`
        // (`live_table_txn`); a recovery sweep does not, and rebuilds an
        // equivalent one by replaying `intent.table_steps`.
        let table_result = match live_table_txn {
            Some(table_txn) => {
                self.commit_table_txn_with_op(&intent.graph, "transaction", table_txn, operation_id)
                    .await
            }
            None => self.replay_table_steps_and_commit(&intent).await,
        };
        match table_result {
            Ok(_) => {
                let _ = crate::server::txn_intent::delete_intent(
                    authority,
                    persist_dir,
                    operation_id,
                );
                Ok(())
            }
            Err(table_err) => {
                let compensate_id = intent.compensation_operation_id();
                if let Err(comp_err) = self
                    .commit_graph_methods_with_op(
                        &intent.graph,
                        intent.compensating_methods.clone(),
                        compensate_id,
                        crate::server::txn::IsolationLevel::Snapshot,
                    )
                    .await
                {
                    // Could not undo an already-applied graph write. Leave the
                    // intent file in place — the NEXT recovery sweep (this
                    // connection's next statement, or another connection for
                    // the same owner) retries this exact resolution, keeping
                    // the failure loudly visible instead of silently eating a
                    // torn state.
                    return Err(user_err(format!(
                        "table commit failed ({table_err}) and undoing the graph write ALSO \
                         failed ({comp_err}); the transaction remains unresolved and will be \
                         retried"
                    )));
                }
                let _ = crate::server::txn_intent::delete_intent(
                    authority,
                    persist_dir,
                    operation_id,
                );
                Err(table_err)
            }
        }
    }

    /// Rebuild the table-side `TableTxn` by replaying `intent.table_steps`
    /// through the ordinary buffering dispatch, into an ISOLATED scratch
    /// buffer — never this connection's own live `self.txn` (which, at every
    /// call site, is already empty: the live COMMIT path drained it via
    /// `take_txn()` before building the intent, and a recovering session
    /// never opened a txn of its own) — then commits it under the intent's
    /// `operation_id`.
    async fn replay_table_steps_and_commit(
        &self,
        intent: &crate::server::txn_intent::CommitIntent,
    ) -> WireResult<usize> {
        let saved = self.txn.lock().take();
        *self.txn.lock() = Some(TableTxn::new());
        let replay_result = self
            .replay_table_steps(&intent.graph, &intent.table_steps)
            .await;
        let rebuilt = self.txn.lock().take();
        *self.txn.lock() = saved;
        replay_result?;
        let rebuilt = rebuilt.unwrap_or_default();
        self.commit_table_txn_with_op(&intent.graph, "transaction", rebuilt, intent.operation_id())
            .await
    }

    /// Replay one recorded table-side step (CONCEPT:EG-TXN.mixed-commit-intent — NE-004) into the scratch `self.txn`
    /// buffer `replay_table_steps_and_commit` set up. A `Sql` step re-runs the
    /// ORIGINAL literal statement through the ordinary `in_txn = true`
    /// buffering dispatch (re-deriving the exact same `TxnOp` the original
    /// statement did); a `CopyRows` step re-applies the decoded batch
    /// directly (a `COPY` has no equivalent SQL text to re-parse).
    async fn replay_table_steps(
        &self,
        graph: &str,
        steps: &[crate::server::txn_intent::ReplayStep],
    ) -> WireResult<()> {
        for step in steps {
            match step {
                crate::server::txn_intent::ReplayStep::Sql(sql) => {
                    let kind = eg_query::classify(sql).map_err(user_err)?;
                    self.dispatch_kind(graph, sql, kind, true).await?;
                }
                crate::server::txn_intent::ReplayStep::CopyRows {
                    table,
                    columns,
                    rows,
                } => {
                    self.buffer(TxnOp::Insert {
                        table: table.clone(),
                        col_order: columns.clone(),
                        rows: rows.clone(),
                    });
                }
            }
        }
        Ok(())
    }

    /// Lazily sweep this connection's owner directory for a leftover
    /// commit-intent left by a crash (CONCEPT:EG-TXN.mixed-commit-intent — NE-004 crash recovery), resolving each
    /// exactly like a live synchronous table-commit rejection would. Runs at
    /// most once per connection (cheap after the first call: a
    /// directory-listing miss when there is nothing to recover).
    async fn recover_owner_intents_once(&self) -> WireResult<()> {
        if self
            .recovered_intents
            .swap(true, std::sync::atomic::Ordering::AcqRel)
        {
            return Ok(());
        }
        let Some(authority) = self.authority.lock().clone() else {
            // Not yet authenticated: nothing owned to check yet. Un-latch so
            // the FIRST call after authentication still sweeps.
            self.recovered_intents
                .store(false, std::sync::atomic::Ordering::Release);
            return Ok(());
        };
        let persist_dir_buf = self.state.read().await.persist_dir.clone();
        let Some(persist_dir_buf) = persist_dir_buf else {
            return Ok(());
        };
        let persist_dir = std::path::Path::new(&persist_dir_buf);
        for intent in crate::server::txn_intent::list_intents(&authority, persist_dir) {
            // Recovery is completing (or undoing) an ALREADY-decided
            // transaction, not evaluating a fresh one — there is no NEW read
            // to protect, so `Snapshot` (no predicate re-check) is correct
            // here regardless of the crashed transaction's own isolation.
            self.resolve_commit_intent(
                &authority,
                persist_dir,
                intent,
                crate::server::txn::IsolationLevel::Snapshot,
                None,
            )
            .await?;
        }
        Ok(())
    }

    /// `COPY <table> [(cols…)] FROM STDIN` (CONCEPT:EG-KG.query.register-each-user-table): resolve the target schema,
    /// stash the copy state, and return a copy-in outcome so the wire streams rows.
    async fn start_copy(&self, plan: CopyPlan) -> WireResult<WireOutcome> {
        let store = self.user_table_store().await?;
        let table = plan.table.clone();
        let schema = tokio::task::spawn_blocking(move || store.get_schema(&table))
            .await
            .map_err(|e| user_err(format!("copy schema task failed: {e}")))?
            .map_err(user_err)?
            .ok_or_else(|| user_err(format!("table `{}` does not exist", plan.table)))?;
        // Resolve the insert column list: the COPY column list, or all columns in order.
        let columns: Vec<String> = if plan.columns.is_empty() {
            schema.columns().iter().map(|c| c.name.clone()).collect()
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
        Ok(WireOutcome::CopyIn {
            format_code: fmt_code,
            num_columns: ncols,
        })
    }

    /// `CREATE TABLE` (CONCEPT:EG-KG.query.register-user-tables-alongside): record the schema in the durable redb table
    /// catalog. The whole DDL commits (commit-before-ack) before the tag is returned.
    async fn run_create_table(
        &self,
        graph: &str,
        sql: &str,
        plan: CreateTablePlan,
    ) -> WireResult<WireOutcome> {
        let columns = to_store_columns(&plan.columns)?;
        let mut txn = TableTxn::new();
        txn.push(TxnOp::CreateTable {
            schema: TableSchema::new(plan.name, columns),
            if_not_exists: plan.if_not_exists,
        });
        self.commit_table_txn(graph, sql, txn).await?;
        Ok(WireOutcome::command("CREATE TABLE"))
    }

    /// `DROP TABLE` (CONCEPT:EG-KG.query.register-user-tables-alongside): remove the catalog entry + every row.
    async fn run_drop_table(
        &self,
        graph: &str,
        sql: &str,
        plan: DropTablePlan,
    ) -> WireResult<WireOutcome> {
        let mut txn = TableTxn::new();
        txn.push(TxnOp::DropTable {
            name: plan.name,
            if_exists: plan.if_exists,
        });
        self.commit_table_txn(graph, sql, txn).await?;
        Ok(WireOutcome::command("DROP TABLE"))
    }

    /// CONCEPT:EG-KG.query.create-drop-view — `CREATE [OR REPLACE] VIEW name AS SELECT …`: persist the view
    /// text in the durable view catalog (commit-before-ack); `build_ctx` expands it on read.
    async fn run_create_view(
        &self,
        graph: &str,
        sql: &str,
        plan: CreateViewPlan,
    ) -> WireResult<WireOutcome> {
        let mut txn = TableTxn::new();
        txn.push(TxnOp::CreateView {
            name: plan.name,
            select_sql: plan.select_sql,
            or_replace: plan.or_replace,
        });
        self.commit_table_txn(graph, sql, txn).await?;
        Ok(WireOutcome::command("CREATE VIEW"))
    }

    /// CONCEPT:EG-KG.query.create-drop-view — `DROP VIEW [IF EXISTS] name`.
    async fn run_drop_view(
        &self,
        graph: &str,
        sql: &str,
        plan: DropViewPlan,
    ) -> WireResult<WireOutcome> {
        let mut txn = TableTxn::new();
        txn.push(TxnOp::DropView {
            name: plan.name,
            if_exists: plan.if_exists,
        });
        self.commit_table_txn(graph, sql, txn).await?;
        Ok(WireOutcome::command("DROP VIEW"))
    }

    /// CONCEPT:EG-KG.query.create-drop-extension-over — `CREATE EXTENSION [IF NOT EXISTS] name`: record the enablement
    /// in the durable extension catalog (commit-before-ack) so a client's setup script
    /// proceeds; the extension's concrete surface lands in its own later item.
    async fn run_create_extension(
        &self,
        graph: &str,
        sql: &str,
        name: String,
        if_not_exists: bool,
    ) -> WireResult<WireOutcome> {
        let mut txn = TableTxn::new();
        txn.push(TxnOp::CreateExtension {
            name,
            if_not_exists,
        });
        self.commit_table_txn(graph, sql, txn).await?;
        Ok(WireOutcome::command("CREATE EXTENSION"))
    }

    /// CONCEPT:EG-KG.query.create-drop-extension-over — `DROP EXTENSION [IF EXISTS] name`.
    async fn run_drop_extension(
        &self,
        graph: &str,
        sql: &str,
        name: String,
        if_exists: bool,
    ) -> WireResult<WireOutcome> {
        let mut txn = TableTxn::new();
        txn.push(TxnOp::DropExtension { name, if_exists });
        self.commit_table_txn(graph, sql, txn).await?;
        Ok(WireOutcome::command("DROP EXTENSION"))
    }

    /// CONCEPT:EG-KG.query.create-drop-function — `CREATE [OR REPLACE] FUNCTION … LANGUAGE sql`: persist the SQL
    /// stored function in the durable function catalog (commit-before-ack). A later
    /// `SELECT fn(args)` / `FROM fn(args)` expands it during the read path.
    async fn run_create_function(
        &self,
        graph: &str,
        sql: &str,
        plan: CreateFunctionPlan,
    ) -> WireResult<WireOutcome> {
        let mut txn = TableTxn::new();
        txn.push(TxnOp::CreateFunction {
            function: plan.func,
            or_replace: plan.or_replace,
        });
        self.commit_table_txn(graph, sql, txn).await?;
        Ok(WireOutcome::command("CREATE FUNCTION"))
    }

    /// CONCEPT:EG-KG.query.create-drop-function — `DROP FUNCTION [IF EXISTS] name`.
    async fn run_drop_function(
        &self,
        graph: &str,
        sql: &str,
        plan: DropFunctionPlan,
    ) -> WireResult<WireOutcome> {
        let mut txn = TableTxn::new();
        txn.push(TxnOp::DropFunction {
            name: plan.name,
            if_exists: plan.if_exists,
        });
        self.commit_table_txn(graph, sql, txn).await?;
        Ok(WireOutcome::command("DROP FUNCTION"))
    }

    /// CONCEPT:EG-KG.query.postgres-family-extension-plan — `SELECT … FROM cypher('graph', $$ … $$) AS (cols…)`: run the
    /// inner Cypher on the named graph over its off-lock snapshot, then project the
    /// agtype (JSON) result onto the typed `AS` columns. Behind the `cypher` feature.
    async fn run_cypher_call(&self, graph: &str, plan: CypherCallPlan) -> WireResult<WireOutcome> {
        #[cfg(feature = "cypher")]
        {
            // AGE always names a graph; fall back to the session graph if blank.
            let target = if plan.graph.is_empty() {
                graph.to_string()
            } else {
                plan.graph.clone()
            };
            self.check_access(&target, AccessLevel::Read).await?;
            let core = self.graph_core(&target).await?;
            let mut snap = core.analysis_snapshot();
            #[cfg(feature = "security")]
            self.filter_view_for_verified_actor(&mut snap).await?;
            let cypher = plan.cypher.clone();
            let result = tokio::task::spawn_blocking(move || eg_query::exec_cypher(&snap, &cypher))
                .await
                .map_err(|e| user_err(format!("cypher task failed: {e}")))?
                .map_err(|msg| user_err(format!("cypher error: {msg}")))?;
            let projected =
                eg_query::project_cypher_rows(&result, &plan.columns, plan.projection.as_deref())
                    .map_err(user_err)?;
            Ok(WireOutcome::Rows(projected))
        }
        #[cfg(not(feature = "cypher"))]
        {
            let _ = (graph, plan);
            Err(user_err(
                "cypher() (Apache AGE) requires the engine's `cypher` feature",
            ))
        }
    }

    /// CONCEPT:EG-KG.query.real-ann-top-k — persist a pgvector ANN index definition
    /// through the authoritative SQL MutationBatch kernel before acknowledging it.
    async fn run_create_ann_index(
        &self,
        graph: &str,
        sql: &str,
        plan: AnnIndexPlan,
    ) -> WireResult<WireOutcome> {
        let mut txn = TableTxn::new();
        txn.push(TxnOp::PutAnnIndex { plan });
        self.commit_table_txn(graph, sql, txn).await?;
        Ok(WireOutcome::command("CREATE INDEX"))
    }

    /// CONCEPT:EG-KG.query.continuous-aggregate-lowering — persist native
    /// hypertable metadata after validating the table and timestamp column.
    async fn run_create_hypertable(
        &self,
        graph: &str,
        sql: &str,
        plan: HypertablePlan,
    ) -> WireResult<WireOutcome> {
        let text = format!("public.{}", plan.table);
        let mut txn = TableTxn::new();
        txn.push(TxnOp::PutHypertable { plan });
        self.commit_table_txn(graph, sql, txn).await?;
        Ok(WireOutcome::Rows(single_text_result(
            "create_hypertable",
            &text,
        )))
    }

    /// CONCEPT:EG-KG.query.continuous-aggregate-lowering — lower a continuous
    /// aggregate onto the authoritative durable view catalog.
    async fn run_create_continuous_aggregate(
        &self,
        graph: &str,
        sql: &str,
        plan: ContinuousAggPlan,
    ) -> WireResult<WireOutcome> {
        let mut txn = TableTxn::new();
        txn.push(TxnOp::CreateView {
            name: plan.name,
            select_sql: plan.select_sql,
            or_replace: true,
        });
        self.commit_table_txn(graph, sql, txn).await?;
        Ok(WireOutcome::command("CREATE MATERIALIZED VIEW"))
    }

    /// A scalar cell coerced to the string node-id form the engine stores.
    fn cell_to_node_id(v: &serde_json::Value) -> WireResult<String> {
        match v {
            serde_json::Value::String(s) => Ok(s.clone()),
            serde_json::Value::Number(n) => Ok(n.to_string()),
            serde_json::Value::Bool(b) => Ok(b.to_string()),
            serde_json::Value::Null => Err(user_err("resolved a NULL `id` for a node write")),
            other => Err(user_err(format!("`id` must be a scalar, got {other}"))),
        }
    }

    /// CONCEPT:EG-KG.query.insert-into-nodes-select — `INSERT INTO nodes (cols…) SELECT …`: run the SELECT through the
    /// read path, then materialize each row as a node (the `id` column → node id, the rest
    /// → properties), honoring `ON CONFLICT` (CONCEPT:EG-KG.query.delete-returning-sees-row). RETURNING optional.
    async fn run_insert_nodes_select(
        &self,
        graph: &str,
        sql: &str,
        ins: InsertNodesSelect,
    ) -> WireResult<WireOutcome> {
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
        let mut view = self.overlaid_snapshot(graph).await?;
        let empty = serde_json::Map::new();
        let cond_blob = rmp_serde::to_vec_named(&serde_json::Value::Object(empty.clone()))
            .map_err(|e| user_err(format!("encode CAS conditions: {e}")))?;
        let mut affected: Vec<(String, serde_json::Map<String, serde_json::Value>)> = Vec::new();
        let returning = if ins.returning {
            Some(self.returning_cols(graph, sql).await?)
        } else {
            None
        };
        let mut methods = Vec::with_capacity(result.rows.len());
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
            if view.has_node(&node_id) {
                match ins.on_conflict.as_ref().map(|oc| &oc.action) {
                    Some(OnConflictAction::DoNothing) => continue,
                    Some(OnConflictAction::DoUpdate(set)) => {
                        let upd_blob =
                            rmp_serde::to_vec_named(&serde_json::Value::Object(set.clone()))
                                .map_err(|e| user_err(format!("encode CAS updates: {e}")))?;
                        methods.push(crate::protocol::Method::CompareAndSetNodeFields {
                            node_id: node_id.clone(),
                            conditions_msgpack: cond_blob.clone(),
                            updates_msgpack: upd_blob,
                        });
                        view.overlay_compare_and_set_fields(&node_id, &empty, set);
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
            methods.push(crate::protocol::Method::AddNode {
                node_id: node_id.clone(),
                properties_msgpack: blob.clone(),
            });
            view.overlay_add_node(node_id.clone(), blob);
            if ins.returning {
                affected.push((node_id, props));
            }
            n += 1;
        }
        self.commit_graph_methods(graph, methods).await?;
        if let Some((cols, types)) = returning {
            Ok(WireOutcome::Rows(returning_result(
                &affected, &cols, &types,
            )))
        } else {
            Ok(WireOutcome::command_rows("INSERT", n))
        }
    }

    /// CONCEPT:EG-KG.query.update-delete-from — `UPDATE nodes SET … FROM … WHERE …`: the classifier rendered a
    /// resolution `SELECT nodes.id, <set-exprs…> FROM …`; each row gives the matched id
    /// plus its per-row SET values. Applied via the serializable merge under the guard.
    async fn run_update_nodes_join(
        &self,
        graph: &str,
        sql: &str,
        upd: UpdateNodesJoin,
    ) -> WireResult<WireOutcome> {
        let result = self.run_read(graph, upd.resolve_sql).await?;
        if result.columns.len() != upd.set_targets.len() + 1 {
            return Err(user_err(format!(
                "UPDATE … FROM resolution shape mismatch: expected id + {} set columns, got {}",
                upd.set_targets.len(),
                result.columns.len()
            )));
        }
        let empty = serde_json::Map::new();
        let cond_blob = rmp_serde::to_vec_named(&serde_json::Value::Object(empty.clone()))
            .map_err(|e| user_err(format!("encode CAS conditions: {e}")))?;
        let mut affected: Vec<(String, serde_json::Map<String, serde_json::Value>)> = Vec::new();
        let returning = if upd.returning {
            Some(self.returning_cols(graph, sql).await?)
        } else {
            None
        };
        let mut methods = Vec::new();
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
            let upd_blob = rmp_serde::to_vec_named(&serde_json::Value::Object(updates.clone()))
                .map_err(|e| user_err(format!("encode CAS updates: {e}")))?;
            methods.push(crate::protocol::Method::CompareAndSetNodeFields {
                node_id: id.clone(),
                conditions_msgpack: cond_blob.clone(),
                updates_msgpack: upd_blob,
            });
            if upd.returning {
                affected.push((id, updates));
            }
        }
        let n = methods.len();
        self.commit_graph_methods(graph, methods).await?;
        if let Some((cols, types)) = returning {
            Ok(WireOutcome::Rows(returning_result(
                &affected, &cols, &types,
            )))
        } else {
            Ok(WireOutcome::command_rows("UPDATE", n))
        }
    }

    /// CONCEPT:EG-KG.query.update-delete-from — `DELETE FROM nodes USING … WHERE …`: the classifier rendered a
    /// resolution `SELECT nodes.id FROM … USING …`; remove each resolved node.
    async fn run_delete_nodes_join(
        &self,
        graph: &str,
        sql: &str,
        del: DeleteNodesJoin,
    ) -> WireResult<WireOutcome> {
        let result = self.run_read(graph, del.resolve_sql).await?;
        let mut affected: Vec<(String, serde_json::Map<String, serde_json::Value>)> = Vec::new();
        let returning = if del.returning {
            Some(self.returning_cols(graph, sql).await?)
        } else {
            None
        };
        let mut seen = std::collections::HashSet::new();
        let mut methods = Vec::new();
        for row in result.rows {
            let id = Self::cell_to_node_id(&row[0])?;
            if !seen.insert(id.clone()) {
                continue;
            }
            if del.returning {
                let props = self.node_props(graph, &id).await?;
                affected.push((id.clone(), props));
            }
            methods.push(crate::protocol::Method::RemoveNode {
                node_id: id.clone(),
            });
        }
        let n = methods.len();
        self.commit_graph_methods(graph, methods).await?;
        if let Some((cols, types)) = returning {
            Ok(WireOutcome::Rows(returning_result(
                &affected, &cols, &types,
            )))
        } else {
            Ok(WireOutcome::command_rows("DELETE", n))
        }
    }

    /// `ALTER TABLE …` (CONCEPT:EG-KG.query.register-user-tables-alongside ADD COLUMN + CONCEPT:EG-KG.query.rename-table-moves-catalog DROP/RENAME COLUMN,
    /// RENAME TABLE, ALTER COLUMN TYPE, DROP CONSTRAINT). Lowered to a single buffered
    /// [`TxnOp`] and applied in one redb write txn (atomic row migration).
    async fn run_alter_table(
        &self,
        graph: &str,
        sql: &str,
        plan: AlterTablePlan,
    ) -> WireResult<WireOutcome> {
        let op = alter_txn_op(plan)?;
        let mut txn = TableTxn::new();
        txn.push(op);
        self.commit_table_txn(graph, sql, txn).await?;
        Ok(WireOutcome::command("ALTER TABLE"))
    }

    /// `INSERT INTO <user_table> … VALUES …` (CONCEPT:EG-KG.query.register-user-tables-alongside). Commit-before-ack.
    async fn run_insert_table(
        &self,
        graph: &str,
        sql: &str,
        ins: InsertTable,
    ) -> WireResult<WireOutcome> {
        let mut txn = TableTxn::new();
        txn.push(TxnOp::Insert {
            table: ins.table,
            col_order: ins.columns,
            rows: ins.rows,
        });
        let n = self.commit_table_txn(graph, sql, txn).await?;
        Ok(WireOutcome::command_rows("INSERT", n))
    }

    /// `INSERT INTO <user_table> (cols…) SELECT …` (CONCEPT:EG-KG.query.register-user-tables-alongside). Runs the SELECT
    /// through the SAME DataFusion path (so it can JOIN user tables AND the graph),
    /// then durably inserts the projected rows. The SELECT result column COUNT must
    /// match the insert column list.
    async fn run_insert_select(
        &self,
        graph: &str,
        sql: &str,
        ins: InsertSelect,
    ) -> WireResult<WireOutcome> {
        let result = self.run_read(graph, ins.select_sql).await?;
        if result.columns.len() != ins.columns.len() {
            return Err(user_err(format!(
                "INSERT … SELECT column count mismatch: {} target columns, {} selected",
                ins.columns.len(),
                result.columns.len()
            )));
        }
        let mut txn = TableTxn::new();
        txn.push(TxnOp::Insert {
            table: ins.table,
            col_order: ins.columns,
            rows: result.rows,
        });
        let n = self.commit_table_txn(graph, sql, txn).await?;
        Ok(WireOutcome::command_rows("INSERT", n))
    }

    /// `UPDATE <user_table> SET … WHERE <predicate>` (CONCEPT:EG-KG.query.register-user-tables-alongside, compound
    /// predicate CONCEPT:EG-KG.query.compound-predicate-decode). The store evaluates the predicate per row inside
    /// its redb write transaction (serializable).
    async fn run_update_table(
        &self,
        graph: &str,
        sql: &str,
        upd: UpdateTable,
    ) -> WireResult<WireOutcome> {
        let mut txn = TableTxn::new();
        txn.push(TxnOp::Update {
            table: upd.table,
            set: upd.set,
            selector: upd.selector.pred,
        });
        let n = self.commit_table_txn(graph, sql, txn).await?;
        Ok(WireOutcome::command_rows("UPDATE", n))
    }

    /// `DELETE FROM <user_table> WHERE <predicate>` (CONCEPT:EG-KG.query.register-user-tables-alongside, compound
    /// predicate CONCEPT:EG-KG.query.compound-predicate-decode).
    async fn run_delete_table(
        &self,
        graph: &str,
        sql: &str,
        del: DeleteTable,
    ) -> WireResult<WireOutcome> {
        let mut txn = TableTxn::new();
        txn.push(TxnOp::Delete {
            table: del.table,
            selector: del.selector.pred,
        });
        let n = self.commit_table_txn(graph, sql, txn).await?;
        Ok(WireOutcome::command_rows("DELETE", n))
    }

    /// Resolve the candidate node ids a WHERE selects. `Id` is the fast path (one id,
    /// only included if the node exists). `Predicate` (CONCEPT:EG-KG.query.compound-predicate-decode) re-runs the
    /// WHERE text through the SAME DataFusion read path as a `SELECT id FROM nodes
    /// WHERE …`, so any compound `AND`/`OR`/`IN`/`BETWEEN`/range predicate the read
    /// surface understands resolves the candidate set. These ids are then re-checked
    /// under the write guard (`compare_and_set_fields_if`/`remove_node_if`) for
    /// serializable semantics, so a row that changed between the read and the write is
    /// skipped.
    async fn matched_ids(&self, graph: &str, selector: &WhereEq) -> WireResult<Vec<String>> {
        match selector {
            WhereEq::Id(id) => {
                let view = self.overlaid_snapshot(graph).await?;
                Ok(if view.has_node(id) {
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
    ) -> WireResult<serde_json::Map<String, serde_json::Value>> {
        let core = self.graph_core(graph).await?;
        match core.get_node_properties(id) {
            Some(blob) => Ok(eg_types::msgpack::decode_property_object(&blob).unwrap_or_default()),
            None => Ok(serde_json::Map::new()),
        }
    }

    /// The RETURNING projection columns for `sql` (in the SAME deterministic order
    /// the Describe step reports) PLUS the graph's `name → PgColType` schema map used
    /// to type them. Both are computed from the SAME inputs the Describe path uses, so
    /// a `RETURNING` write's rows always match the described schema in both column
    /// NAMES and TYPES. Resolves `RETURNING *` against the known property columns
    /// (sorted for determinism).
    pub(crate) async fn returning_cols(
        &self,
        graph: &str,
        sql: &str,
    ) -> WireResult<(Vec<String>, std::collections::HashMap<String, PgColType>)> {
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

    // ── buffered graph-node DML inside an open transaction (CONCEPT:EG-KG.compute.kg-transaction-is-pinned) ──────
    // These mirror the immediate `run_*` node-DML methods but BUFFER a `NodeOp`
    // instead of applying it — the buffer is replayed atomically at COMMIT
    // (`run_commit`). Candidate-id / SELECT resolution runs immediately over a
    // read-your-own-writes overlay (`run_read` overlays the buffer when in a txn),
    // so an in-txn statement sees earlier statements' buffered writes. RETURNING is
    // resolved against the post-buffer overlaid snapshot.

    /// Buffered `INSERT INTO nodes …` (CONCEPT:EG-KG.compute.kg-transaction-is-pinned).
    async fn buffer_insert(
        &self,
        graph: &str,
        sql: &str,
        ins: InsertNodes,
    ) -> WireResult<WireOutcome> {
        let n = ins.rows.len();
        let mut affected: Vec<(String, serde_json::Map<String, serde_json::Value>)> = Vec::new();
        for node in ins.rows {
            let props = node.properties.clone();
            let blob = rmp_serde::to_vec_named(&serde_json::Value::Object(node.properties))
                .map_err(|e| user_err(format!("encode node properties: {e}")))?;
            let node_id = node.node_id.clone();
            self.buffer_node(NodeOp::Add {
                id: node.node_id,
                blob,
            });
            if ins.returning {
                affected.push((node_id, props));
            }
        }
        if ins.returning {
            let (cols, types) = self.returning_cols(graph, sql).await?;
            Ok(WireOutcome::Rows(returning_result(
                &affected, &cols, &types,
            )))
        } else {
            Ok(WireOutcome::command_rows("INSERT", n))
        }
    }

    /// Buffered `UPDATE nodes SET … WHERE …` (CONCEPT:EG-KG.compute.kg-transaction-is-pinned). Candidate ids resolve
    /// over the RYOW overlay; each becomes an unconditional-merge `Cas`. RETURNING
    /// reads back the post-update rows from the overlaid snapshot (which now includes
    /// the just-buffered merges).
    async fn buffer_update(
        &self,
        graph: &str,
        sql: &str,
        upd: UpdateNodes,
    ) -> WireResult<WireOutcome> {
        let ids = self.matched_ids(graph, &upd.selector).await?;
        let updates = upd.set;
        for id in &ids {
            self.buffer_node(NodeOp::Cas {
                id: id.clone(),
                conditions: serde_json::Map::new(),
                updates: updates.clone(),
            });
        }
        let n = ids.len();
        if upd.returning {
            let view = self.overlaid_snapshot(graph).await?;
            let mut affected = Vec::with_capacity(ids.len());
            for id in &ids {
                affected.push((id.clone(), view.node_row_object(id).unwrap_or_default()));
            }
            let (cols, types) = self.returning_cols(graph, sql).await?;
            Ok(WireOutcome::Rows(returning_result(
                &affected, &cols, &types,
            )))
        } else {
            Ok(WireOutcome::command_rows("UPDATE", n))
        }
    }

    /// Buffered `DELETE FROM nodes WHERE …` (CONCEPT:EG-KG.compute.kg-transaction-is-pinned). RETURNING captures the
    /// rows from the overlaid snapshot BEFORE the removes are buffered (they are gone
    /// after).
    async fn buffer_delete(
        &self,
        graph: &str,
        sql: &str,
        del: DeleteNodes,
    ) -> WireResult<WireOutcome> {
        let ids = self.matched_ids(graph, &del.selector).await?;
        let returning = if del.returning {
            let view = self.overlaid_snapshot(graph).await?;
            let affected: Vec<(String, serde_json::Map<String, serde_json::Value>)> = ids
                .iter()
                .map(|id| (id.clone(), view.node_row_object(id).unwrap_or_default()))
                .collect();
            Some((affected, self.returning_cols(graph, sql).await?))
        } else {
            None
        };
        for id in &ids {
            self.buffer_node(NodeOp::Remove { id: id.clone() });
        }
        let n = ids.len();
        match returning {
            Some((affected, (cols, types))) => Ok(WireOutcome::Rows(returning_result(
                &affected, &cols, &types,
            ))),
            None => Ok(WireOutcome::command_rows("DELETE", n)),
        }
    }

    /// Buffered `INSERT INTO nodes … SELECT …` (CONCEPT:EG-KG.compute.kg-transaction-is-pinned). The SELECT resolves
    /// over the RYOW overlay; `ON CONFLICT` is evaluated against the txn's own evolving
    /// buffered state (a local overlaid view advanced per row), so a conflict against a
    /// row inserted earlier in the SAME transaction is honored.
    async fn buffer_insert_nodes_select(
        &self,
        graph: &str,
        sql: &str,
        ins: InsertNodesSelect,
    ) -> WireResult<WireOutcome> {
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
        // A local overlaid view, advanced as we buffer, so ON CONFLICT sees this
        // statement's own buffered writes (and the txn's earlier ones).
        let mut view = self.overlaid_snapshot(graph).await?;
        let empty = serde_json::Map::new();
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
            if view.has_node(&node_id) {
                match ins.on_conflict.as_ref().map(|oc| &oc.action) {
                    Some(OnConflictAction::DoNothing) => continue,
                    Some(OnConflictAction::DoUpdate(set)) => {
                        self.buffer_node(NodeOp::Cas {
                            id: node_id.clone(),
                            conditions: empty.clone(),
                            updates: set.clone(),
                        });
                        view.overlay_compare_and_set_fields(&node_id, &empty, set);
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
            self.buffer_node(NodeOp::Add {
                id: node_id.clone(),
                blob: blob.clone(),
            });
            view.overlay_add_node(node_id.clone(), blob);
            if ins.returning {
                affected.push((node_id, props));
            }
            n += 1;
        }
        if ins.returning {
            let (cols, types) = self.returning_cols(graph, sql).await?;
            Ok(WireOutcome::Rows(returning_result(
                &affected, &cols, &types,
            )))
        } else {
            Ok(WireOutcome::command_rows("INSERT", n))
        }
    }

    /// Buffered `UPDATE nodes SET … FROM … WHERE …` (CONCEPT:EG-KG.compute.kg-transaction-is-pinned). The resolution
    /// SELECT resolves over the RYOW overlay; each matched id becomes an
    /// unconditional-merge `Cas`. Duplicate ids from the join are applied once.
    async fn buffer_update_nodes_join(
        &self,
        graph: &str,
        sql: &str,
        upd: UpdateNodesJoin,
    ) -> WireResult<WireOutcome> {
        let result = self.run_read(graph, upd.resolve_sql).await?;
        if result.columns.len() != upd.set_targets.len() + 1 {
            return Err(user_err(format!(
                "UPDATE … FROM resolution shape mismatch: expected id + {} set columns, got {}",
                upd.set_targets.len(),
                result.columns.len()
            )));
        }
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
            self.buffer_node(NodeOp::Cas {
                id: id.clone(),
                conditions: serde_json::Map::new(),
                updates: updates.clone(),
            });
            affected.push((id, updates));
        }
        let n = affected.len();
        if upd.returning {
            let (cols, types) = self.returning_cols(graph, sql).await?;
            Ok(WireOutcome::Rows(returning_result(
                &affected, &cols, &types,
            )))
        } else {
            Ok(WireOutcome::command_rows("UPDATE", n))
        }
    }

    /// Buffered `DELETE FROM nodes USING … WHERE …` (CONCEPT:EG-KG.compute.kg-transaction-is-pinned). RETURNING
    /// captures rows from the overlaid snapshot before the removes are buffered.
    async fn buffer_delete_nodes_join(
        &self,
        graph: &str,
        sql: &str,
        del: DeleteNodesJoin,
    ) -> WireResult<WireOutcome> {
        let result = self.run_read(graph, del.resolve_sql).await?;
        let mut ids: Vec<String> = Vec::new();
        let mut seen = std::collections::HashSet::new();
        for row in result.rows {
            let id = Self::cell_to_node_id(&row[0])?;
            if seen.insert(id.clone()) {
                ids.push(id);
            }
        }
        let returning = if del.returning {
            let view = self.overlaid_snapshot(graph).await?;
            let affected: Vec<(String, serde_json::Map<String, serde_json::Value>)> = ids
                .iter()
                .map(|id| (id.clone(), view.node_row_object(id).unwrap_or_default()))
                .collect();
            Some((affected, self.returning_cols(graph, sql).await?))
        } else {
            None
        };
        for id in &ids {
            self.buffer_node(NodeOp::Remove { id: id.clone() });
        }
        let n = ids.len();
        match returning {
            Some((affected, (cols, types))) => Ok(WireOutcome::Rows(returning_result(
                &affected, &cols, &types,
            ))),
            None => Ok(WireOutcome::command_rows("DELETE", n)),
        }
    }

    async fn run_insert(
        &self,
        graph: &str,
        sql: &str,
        ins: InsertNodes,
    ) -> WireResult<WireOutcome> {
        let mut affected: Vec<(String, serde_json::Map<String, serde_json::Value>)> = Vec::new();
        let returning = if ins.returning {
            Some(self.returning_cols(graph, sql).await?)
        } else {
            None
        };
        let mut methods = Vec::with_capacity(ins.rows.len());
        for node in ins.rows {
            let props = node.properties.clone();
            let blob = rmp_serde::to_vec_named(&serde_json::Value::Object(node.properties))
                .map_err(|e| user_err(format!("encode node properties: {e}")))?;
            let node_id = node.node_id.clone();
            methods.push(crate::protocol::Method::AddNode {
                node_id: node_id.clone(),
                properties_msgpack: blob,
            });
            if ins.returning {
                affected.push((node_id, props));
            }
        }
        let n = methods.len();
        self.commit_graph_methods(graph, methods).await?;
        if let Some((cols, types)) = returning {
            Ok(WireOutcome::Rows(returning_result(
                &affected, &cols, &types,
            )))
        } else {
            Ok(WireOutcome::command_rows("INSERT", n))
        }
    }

    /// `UPDATE nodes SET … WHERE …` (CONCEPT:EG-KG.query.follow-up). Resolves the matched ids,
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
    ) -> WireResult<WireOutcome> {
        let ids = self.matched_ids(graph, &upd.selector).await?;
        let conditions = serde_json::Map::new();
        let updates = upd.set;
        let cond_blob = rmp_serde::to_vec_named(&serde_json::Value::Object(conditions.clone()))
            .map_err(|e| user_err(format!("encode CAS conditions: {e}")))?;
        let upd_blob = rmp_serde::to_vec_named(&serde_json::Value::Object(updates.clone()))
            .map_err(|e| user_err(format!("encode CAS updates: {e}")))?;
        let returning = if upd.returning {
            Some(self.returning_cols(graph, sql).await?)
        } else {
            None
        };
        let methods = ids
            .iter()
            .map(|id| crate::protocol::Method::CompareAndSetNodeFields {
                node_id: id.clone(),
                conditions_msgpack: cond_blob.clone(),
                updates_msgpack: upd_blob.clone(),
            })
            .collect();
        let n = ids.len();
        self.commit_graph_methods(graph, methods).await?;
        if let Some((cols, types)) = returning {
            let mut affected = Vec::with_capacity(n);
            for id in ids {
                let props = self.node_props(graph, &id).await?;
                affected.push((id, props));
            }
            Ok(WireOutcome::Rows(returning_result(
                &affected, &cols, &types,
            )))
        } else {
            Ok(WireOutcome::command_rows("UPDATE", n))
        }
    }

    /// `DELETE FROM nodes WHERE …` (CONCEPT:EG-KG.query.follow-up). Resolves the matched ids
    /// (capturing each node's properties FIRST when RETURNING is requested, since
    /// the row is gone after removal), removes each via `remove_node` (the same
    /// write path `Method::RemoveNode` dispatch uses) under its one-shot txn, and
    /// durably records each `RemoveNode`.
    async fn run_delete(
        &self,
        graph: &str,
        sql: &str,
        del: DeleteNodes,
    ) -> WireResult<WireOutcome> {
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
        let methods = ids
            .iter()
            .map(|id| crate::protocol::Method::RemoveNode {
                node_id: id.clone(),
            })
            .collect();
        let n = ids.len();
        self.commit_graph_methods(graph, methods).await?;
        if let Some((cols, types)) = returning_cols {
            Ok(WireOutcome::Rows(returning_result(
                &affected, &cols, &types,
            )))
        } else {
            Ok(WireOutcome::command_rows("DELETE", n))
        }
    }

    /// Commit graph methods through the universal cross-modal MutationBatch kernel,
    /// under `Snapshot` isolation and a fresh `operation_id` — the ordinary
    /// off-txn single-statement path. See [`Self::commit_graph_methods_with_op`]
    /// for the isolation-aware, replay-safe variant the multi-statement `COMMIT`
    /// paths use.
    async fn commit_graph_methods(
        &self,
        graph: &str,
        methods: Vec<crate::protocol::Method>,
    ) -> WireResult<()> {
        self.commit_graph_methods_with_op(
            graph,
            methods,
            uuid::Uuid::new_v4(),
            crate::server::txn::IsolationLevel::Snapshot,
        )
        .await
    }

    /// Commit graph methods through the universal cross-modal MutationBatch kernel.
    /// The kernel stages against authority, commits graph rows/result/outbox in one
    /// redb transaction, and only then publishes the serving projection.
    ///
    /// `operation_id` is the SAME id every retry of this exact commit must use
    /// (CONCEPT:EG-TXN.mixed-commit-intent — NE-004): it seeds `commit_cross_modal_txn`'s dedup coordinator key, so
    /// a caller that persisted `operation_id` durably before calling this (a
    /// commit-intent record) can safely call it AGAIN after a crash — the
    /// second call is a no-op replay of the first, never a double-apply.
    ///
    /// `isolation` (CONCEPT:EG-KG.txn.serializable-zero-cost — NE-005): when `Serializable`, this drains and
    /// attaches this connection's [`WireSession::serializable_predicate_reads`]
    /// (each already fingerprinted AT READ TIME — see `execute()`'s read
    /// hook) so the commit gets genuine phantom protection for the reads this
    /// txn actually performed, instead of silently behaving like `Snapshot`.
    async fn commit_graph_methods_with_op(
        &self,
        graph: &str,
        methods: Vec<crate::protocol::Method>,
        operation_id: uuid::Uuid,
        isolation: crate::server::txn::IsolationLevel,
    ) -> WireResult<()> {
        if methods.is_empty() {
            return Ok(());
        }
        if methods
            .iter()
            .any(|method| !crate::mutation_apply::is_durable_mutation(method))
        {
            return Err(user_err(
                "wire graph batch contains a non-durable operation",
            ));
        }
        let core = self.graph_core(graph).await?;
        let authority = self.carrier_authority()?;
        let request_id = u64::from_be_bytes(
            operation_id.as_bytes()[..8]
                .try_into()
                .expect("UUID prefix is eight bytes"),
        );
        let mut txn = crate::server::txn::GraphTxnState::new(
            &core,
            crate::server::txn::NewTxnArgs {
                graph: graph.to_string(),
                tenant_scope: authority.tenant_scope().to_string(),
                begin_version: core.version(),
                isolation,
                predicate: None,
                agent: authority.owner_scope().to_string(),
                now_ms: crate::server::txn::now_ms(),
            },
        );
        if isolation == crate::server::txn::IsolationLevel::Serializable {
            // Already fingerprinted at read time (`execute()`'s read hook) —
            // extend directly, never recompute (recomputing here, right
            // before `validate()` re-checks it, would compare a baseline
            // against itself and protect nothing).
            let captured = std::mem::take(&mut *self.serializable_predicate_reads.lock());
            txn.predicate_reads.extend(captured);
        }
        for method in methods {
            txn.stage(&core, method, crate::server::txn::now_ms());
        }
        let coordinator_id = crate::server::mutation_batch::opaque_coordinator_key(
            "wire-graph-owner",
            authority.owner_scope(),
            &operation_id.simple().to_string(),
        );
        // `commit_cross_modal_txn`'s "Re-check Write access at commit" evaluates
        // `caller` against `IsolationLayer::check_access`, which is keyed by
        // `AgentIdentity.agent_id` (the SAME identity space `execute()`'s own
        // pre-check at the top of this dispatch uses via `verified_actor()` /
        // `bind_authenticated_sql_actor`'s `agent_id` param). `actor_scope()` is a
        // durable-ownership hash for an UNRELATED purpose (opaque KV/coordinator
        // namespacing) and is never a registered RBAC identity, so passing it here
        // made every wire-native graph-node commit fail this recheck whenever
        // `security` is on and no raft consensus is active (`consensus_apply_is_authorized`
        // is false, so the recheck always runs).
        match crate::server::handlers::txn::commit_cross_modal_txn(
            &self.state,
            request_id,
            Some(authority.agent_id()),
            &coordinator_id,
            txn,
        )
        .await
        .map_err(user_err)?
        {
            true => Ok(()),
            false => Err(WireError {
                code: "40001".to_string(),
                message: "wire graph transaction conflicted; retry the statement".to_string(),
            }),
        }
    }

    /// Commit one user-table/catalog transaction through the SQL-native
    /// MutationBatch kernel, under a fresh `operation_id` — the ordinary path.
    /// See [`Self::commit_table_txn_with_op`] for the replay-safe variant
    /// NE-004's mixed-commit path uses.
    async fn commit_table_txn(
        &self,
        graph: &str,
        operation: &str,
        txn: TableTxn,
    ) -> WireResult<usize> {
        self.commit_table_txn_with_op(graph, operation, txn, uuid::Uuid::new_v4())
            .await
    }

    /// Commit one user-table/catalog transaction through the SQL-native
    /// MutationBatch kernel. The supplied operation descriptor is hashed by the
    /// compiler and is never stored as plaintext coordinator metadata.
    ///
    /// `operation_id` is the SAME id every retry of this exact commit must use
    /// (CONCEPT:EG-TXN.mixed-commit-intent — NE-004): it seeds the `idempotency_key`
    /// `TableStore::commit_txn_batch` already dedupes on, so a caller that
    /// persisted `operation_id` durably before calling this can safely call it
    /// AGAIN after a crash — the second call replays the stored result instead
    /// of re-executing the mutation.
    async fn commit_table_txn_with_op(
        &self,
        graph: &str,
        operation: &str,
        txn: TableTxn,
        operation_id: uuid::Uuid,
    ) -> WireResult<usize> {
        if txn.ops.is_empty() {
            return Ok(0);
        }
        let authority = self.carrier_authority()?;
        let request_id = u64::from_be_bytes(
            operation_id.as_bytes()[..8]
                .try_into()
                .expect("UUID prefix is eight bytes"),
        );
        let batch_id = crate::server::mutation_batch::opaque_coordinator_key(
            "wire-sql-owner",
            authority.owner_scope(),
            &operation_id.simple().to_string(),
        );
        let tenant = authority.tenant_scope().to_string();
        let graph = graph.to_string();
        let principal = authority.actor_scope().to_string();
        let operation = crate::protocol::Method::Sql {
            query: operation.to_string(),
            params_msgpack: Vec::new(),
        };
        let store = self.user_table_store().await?;
        tokio::task::spawn_blocking(move || {
            let created_at_ms = crate::server::txn::now_ms();
            let expected_version = store.mutation_version(&tenant, &graph)?;
            let batch = crate::server::mutation_batch::compile_opaque_method(
                crate::server::mutation_batch::CompileBatch {
                    batch_id: &batch_id,
                    request_id,
                    principal: Some(&principal),
                    tenant: &tenant,
                    graph: &graph,
                    placement_epoch: 0,
                    idempotency_key: &batch_id,
                    expected_graph_version: Some(expected_version),
                    fencing_token: None,
                    created_at_ms,
                    default_surface: crate::mutation_batch::MutationSurface::Query,
                    authoritative_state: None,
                },
                &operation,
                crate::mutation_batch::MutationSurface::Query,
                crate::mutation_batch::MutationDomain::SqlCatalog,
                "sql_catalog_operation",
            )?;
            let committed = store.commit_txn_batch(&txn, &batch, created_at_ms)?;
            let bytes = committed
                .record
                .result_msgpack
                .as_deref()
                .ok_or_else(|| "committed SQL MutationBatch has no result".to_string())?;
            eg_types::msgpack::decode_bounded::<usize>(
                bytes,
                eg_types::msgpack::MsgpackLimits::new(64, 1, 1),
            )
            .map_err(|_| "committed SQL result is corrupt".to_string())
        })
        .await
        .map_err(|error| user_err(format!("SQL MutationBatch task failed: {error}")))?
        .map_err(user_err)
    }

    /// Build a `column name → PgColType` map for the current graph by sampling node
    /// property blobs (CONCEPT:EG-KG.query.describe describe support). Used to resolve the type
    /// of a parameter that sits opposite a property column (`WHERE rank > $1`), and
    /// to type a RETURNING result column, WITHOUT a DataFusion round-trip. Schema-
    /// on-read: the first non-null value seen for a column wins (matching the
    /// inference `exec_sql_typed` does). `id` is always TEXT.
    pub(crate) async fn column_types(
        &self,
        graph: &str,
    ) -> WireResult<std::collections::HashMap<String, PgColType>> {
        let core = self.graph_core(graph).await?;
        let mut map: std::collections::HashMap<String, PgColType> =
            std::collections::HashMap::new();
        map.insert("id".to_string(), PgColType::Text);
        for (_, blob) in core.get_nodes() {
            let Ok(obj) = eg_types::msgpack::decode_property_object(&blob) else {
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

    // ── pgwire CROSS-MODAL transaction seam (CONCEPT:EG-KG.txn.isolation-ryow-begin-set) ──────────────────────
    // The PGWIRE TEXT entrypoint for the EG-359..363 in-txn cross-modal seam. These
    // recognize the cross-modal verbs (`UQL …`, `SET EMBEDDING FOR …`, `INSERT INTO
    // series …`, `SPARQL UPDATE …`, `SPARQL CONSTRUCT …`) and route them onto the
    // EXISTING RPC machinery — the `src/server/txn.rs` `GraphTxnState` staging fields +
    // the extracted `run_unified` overlay + `commit_cross_modal_txn` — so a psql/DBeaver
    // `BEGIN … <mutate> … <UQL read> … COMMIT` gets read-your-own-writes across
    // OWL+vector+graph and commits every modality atomically. No plan/txn logic is
    // duplicated here; the wire is a thin parser/router. Because it lives on the SHARED
    // `WireSession` core it lights up for the whole EG-074 multi-wire family.

    /// Lower buffered graph-node ops (CONCEPT:EG-KG.compute.kg-transaction-is-pinned) to the durable `Method`s that both
    /// the in-txn UQL overlay and the cross-modal COMMIT consume, so the NodeOp→Method
    /// mapping is never re-derived.
    #[cfg(feature = "query")]
    fn node_ops_to_methods(node_ops: &[NodeOp]) -> WireResult<Vec<crate::protocol::Method>> {
        let mut methods = Vec::with_capacity(node_ops.len());
        for op in node_ops {
            match op {
                NodeOp::Add { id, blob } => methods.push(crate::protocol::Method::AddNode {
                    node_id: id.clone(),
                    properties_msgpack: blob.clone(),
                }),
                NodeOp::Cas {
                    id,
                    conditions,
                    updates,
                } => {
                    let cond_blob =
                        rmp_serde::to_vec_named(&serde_json::Value::Object(conditions.clone()))
                            .map_err(|e| user_err(format!("encode CAS conditions: {e}")))?;
                    let upd_blob =
                        rmp_serde::to_vec_named(&serde_json::Value::Object(updates.clone()))
                            .map_err(|e| user_err(format!("encode CAS updates: {e}")))?;
                    methods.push(crate::protocol::Method::CompareAndSetNodeFields {
                        node_id: id.clone(),
                        conditions_msgpack: cond_blob,
                        updates_msgpack: upd_blob,
                    });
                }
                NodeOp::Remove { id } => methods.push(crate::protocol::Method::RemoveNode {
                    node_id: id.clone(),
                }),
            }
        }
        Ok(methods)
    }

    /// Detect a cross-modal verb (CONCEPT:EG-KG.txn.isolation-ryow-begin-set). Cheap prefix classification only —
    /// the heavy parse (vector literal, series tuple, SPARQL) happens in
    /// [`WireSession::exec_crossmodal`] so a parse error becomes a clean `WireError`.
    #[cfg(feature = "query")]
    fn detect_crossmodal(sql: &str) -> Option<XmodalStmt> {
        let trimmed = sql.trim().trim_end_matches(';').trim();
        let lower = trimmed.to_ascii_lowercase();
        if lower == "uql" || lower.starts_with("uql ") {
            return Some(XmodalStmt::Uql(trimmed[3..].trim_start().to_string()));
        }
        if lower.starts_with("set embedding") {
            return Some(XmodalStmt::SetEmbedding(trimmed.to_string()));
        }
        if lower.starts_with("insert into series") {
            return Some(XmodalStmt::InsertSeries(trimmed.to_string()));
        }
        #[cfg(feature = "sparql")]
        {
            // `SPARQL UPDATE <update>` must be checked before the generic `SPARQL <query>`.
            if lower.starts_with("sparql update ") {
                return Some(XmodalStmt::SparqlUpdate(
                    trimmed["sparql update".len()..].trim_start().to_string(),
                ));
            }
            if lower.starts_with("sparql ") {
                return Some(XmodalStmt::SparqlConstruct(
                    trimmed["sparql".len()..].trim_start().to_string(),
                ));
            }
        }
        None
    }

    /// Access level a cross-modal verb needs: a UQL read needs Read, every cross-modal
    /// write needs Write.
    #[cfg(feature = "query")]
    fn crossmodal_access(stmt: &XmodalStmt) -> AccessLevel {
        match stmt {
            XmodalStmt::Uql(_) => AccessLevel::Read,
            _ => AccessLevel::Write,
        }
    }

    /// Execute a detected cross-modal verb (CONCEPT:EG-KG.txn.isolation-ryow-begin-set). A UQL read runs over the
    /// RYOW overlay (in a txn) or the committed snapshot (off-txn); a cross-modal write
    /// stages into the open txn's [`XmodalStaged`] buffer, or — off-txn — auto-commits as
    /// ONE atomic cross-modal statement so the seam is seamless at both surfaces.
    #[cfg(feature = "query")]
    async fn exec_crossmodal(
        &self,
        graph: &str,
        stmt: XmodalStmt,
        in_txn: bool,
    ) -> WireResult<WireOutcome> {
        match stmt {
            XmodalStmt::Uql(text) => self.exec_uql(graph, &text, in_txn).await,
            XmodalStmt::SetEmbedding(sql) => {
                let (id, vec) = parse_set_embedding(&sql)?;
                if in_txn {
                    self.xmodal.lock().vectors.push((id, vec));
                    Ok(WireOutcome::command("SET EMBEDDING"))
                } else {
                    let mut ts = self
                        .new_txn_state(graph, crate::server::txn::IsolationLevel::Snapshot)
                        .await?;
                    ts.vectors.push((id, vec));
                    self.commit_txn_state(ts).await?;
                    Ok(WireOutcome::command("SET EMBEDDING"))
                }
            }
            XmodalStmt::InsertSeries(sql) => {
                #[cfg(not(feature = "tsdb"))]
                {
                    let _ = sql;
                    Err(user_err("time-series operations require the tsdb feature"))
                }
                #[cfg(feature = "tsdb")]
                {
                    let m = parse_insert_series(&sql)?;
                    if in_txn {
                        // Keep the local series id for read-your-own-writes; commit
                        // converts it once through the verified authority below.
                        self.carrier_authority()?;
                        self.xmodal.lock().measurements.push(m);
                        Ok(WireOutcome::command_rows("INSERT", 1))
                    } else {
                        let mut ts = self
                        .new_txn_state(graph, crate::server::txn::IsolationLevel::Snapshot)
                        .await?;
                        ts.measurements.push(self.scope_measurement(graph, m)?);
                        self.commit_txn_state(ts).await?;
                        Ok(WireOutcome::command_rows("INSERT", 1))
                    }
                }
            }
            #[cfg(feature = "sparql")]
            XmodalStmt::SparqlUpdate(update) => {
                let (methods, schema_refs) =
                    crate::server::handlers::txn::sparql_update_to_methods(&update)
                        .map_err(user_err)?;
                self.stage_or_commit_owl(graph, methods, schema_refs, in_txn)
                    .await
            }
            #[cfg(feature = "sparql")]
            XmodalStmt::SparqlConstruct(query) => {
                let core = self.graph_core(graph).await?;
                let mut view = core.analysis_snapshot();
                #[cfg(feature = "security")]
                {
                    let actor = self.actor().ok_or_else(|| {
                        user_err("SPARQL transaction derivation requires an authenticated actor")
                    })?;
                    self.state
                        .read()
                        .await
                        .isolation
                        .filter_view(&actor, &mut view);
                }
                let (methods, schema_refs) =
                    crate::server::handlers::txn::construct_view_to_methods(&view, &query)
                        .map_err(user_err)?;
                self.stage_or_commit_owl(graph, methods, schema_refs, in_txn)
                    .await
            }
        }
    }

    /// Stage (in a txn) or auto-commit (off-txn) OWL/CONSTRUCT-lowered graph methods.
    ///
    /// `schema_refs` (BUG A3, 2026-08-12) — node ids [`triples_to_methods`]
    /// identified as schema-defining-triple occurrences, see that function's
    /// doc. The OFF-TXN (auto-commit) branch marks them on the live core
    /// immediately after `methods` commits, so a lone `SPARQL UPDATE INSERT
    /// DATA { ... }` statement (no explicit `BEGIN`/`COMMIT TRANSACTION`) —
    /// the common case, and what a bare `simple_query` always is — gets the
    /// TBox exemption exactly like the native `eg_rdf::update::insert_triple`
    /// path does. The IN-TXN branch stages `methods` for a LATER, SEPARATE
    /// commit this seam does not yet track `schema_refs` through (a larger,
    /// separately-scoped change — the ids are dropped, fail-closed: a node
    /// inserted inside an explicit multi-statement transaction does not get
    /// the exemption yet, never a widened one).
    #[cfg(feature = "sparql")]
    async fn stage_or_commit_owl(
        &self,
        graph: &str,
        methods: Vec<crate::protocol::Method>,
        schema_refs: Vec<String>,
        in_txn: bool,
    ) -> WireResult<WireOutcome> {
        if in_txn {
            let _ = schema_refs; // KNOWN GAP (BUG A3) -- see this fn's doc.
            self.xmodal.lock().owl_methods.extend(methods);
            Ok(WireOutcome::command("SPARQL"))
        } else {
            let mut ts = self
                        .new_txn_state(graph, crate::server::txn::IsolationLevel::Snapshot)
                        .await?;
            ts.axioms.extend(methods);
            self.commit_txn_state(ts).await?;
            if !schema_refs.is_empty() {
                let core = self.graph_core(graph).await?;
                for id in &schema_refs {
                    core.mark_schema_ref(id);
                }
            }
            Ok(WireOutcome::command("SPARQL"))
        }
    }

    /// Run a UQL unified cross-modal read over the current graph (CONCEPT:EG-KG.txn.isolation-ryow-begin-set). In a
    /// txn the committed snapshot is OVERLAID with the txn's staged graph writes (the
    /// wire node buffer + lowered OWL/CONSTRUCT methods) and staged vectors, giving
    /// read-your-own-writes; off-txn it reads the committed store (the overlay is empty).
    /// Reuses the EXTRACTED `run_unified` executor + `overlay_write_set`/`semantic_overlay`
    /// — no plan logic duplicated. In-txn tsdb read-your-own-writes (CONCEPT:EG-KG.query.txn-tsdb-read-your) is
    /// wired too: the txn's staged, uncommitted `measurements` are overlaid into `Op::TsScan`
    /// via a `StagedSeries` so an in-txn UQL reads its own points; off-txn `TsScan` reads
    /// committed series only.
    #[cfg(feature = "query")]
    async fn exec_uql(&self, graph: &str, text: &str, in_txn: bool) -> WireResult<WireOutcome> {
        let plan = eg_plan::uql::parse(text).map_err(|e| user_err(e.render(text)))?;
        #[cfg(feature = "tsdb")]
        let tsdb_scope = if crate::server::handlers::query::plan_needs_tsdb(&plan.ops) {
            let authority = self.carrier_authority()?;
            Some((
                authority.tenant_scope().to_string(),
                authority.namespace("timeseries-graph", graph),
            ))
        } else {
            None
        };
        let core = self.graph_core(graph).await?;
        let mut view = core.analysis_snapshot();
        let (overlay_methods, vectors) = if in_txn {
            let mut methods = Self::node_ops_to_methods(&self.graph_txn.lock().ops)?;
            let xmodal = self.xmodal.lock();
            methods.extend(xmodal.owl_methods.iter().cloned());
            (methods, xmodal.vectors.clone())
        } else {
            (Vec::new(), Vec::new())
        };
        // CONCEPT:EG-KG.query.txn-tsdb-read-your — build the in-txn staged-series RYOW overlay from the wire txn's
        // staged, uncommitted measurements (empty off-txn ⇒ committed series only).
        #[cfg(feature = "tsdb")]
        let staged_series = {
            let mut staged = eg_plan::StagedSeries::new();
            if in_txn {
                for m in &self.xmodal.lock().measurements {
                    staged.push_points(&m.series, m.points.iter().cloned());
                }
            }
            staged
        };
        crate::server::handlers::query::overlay_write_set(&mut view, &overlay_methods);
        #[cfg(feature = "security")]
        self.filter_view_for_verified_actor(&mut view).await?;
        #[cfg(feature = "tsdb")]
        let tsdb = self.state.read().await.tsdb_store.clone();
        #[cfg(feature = "tsdb")]
        let (tsdb_tenant, tsdb_graph) = match tsdb_scope {
            Some((tenant, graph)) => (Some(tenant), Some(graph)),
            None => (None, None),
        };
        // CONCEPT:EG-KG.query.served-vector-index-binding / served-text-index-binding — push the
        // vector + lexical legs into the LIVE persistent indexes via a guard/adapter built
        // INSIDE the off-lock closure, instead of pre-cloning the whole `SemanticStore` here
        // (see `handlers::query`'s `UnifiedQuery` arm for the full rationale). The
        // `semantic_overlay` clone is paid ONLY when the txn actually staged embeddings.
        let core_for_ctx = core.clone();
        let rows = tokio::task::spawn_blocking(move || {
            #[cfg(feature = "text")]
            let served_text =
                crate::server::secondary_indexes::ServedTextIndex::new(core_for_ctx.clone());
            #[cfg(feature = "geo")]
            let served_spatial =
                crate::server::secondary_indexes::ServedSpatialIndex::new(core_for_ctx.clone());
            if vectors.is_empty() {
                let semantic_guard = core_for_ctx.semantic_store.read();
                crate::server::handlers::query::run_unified(
                    plan,
                    &view,
                    &semantic_guard,
                    crate::server::handlers::query::ServedIndexes {
                        #[cfg(feature = "text")]
                        text: Some(&served_text),
                        #[cfg(feature = "geo")]
                        spatial: Some(&served_spatial),
                        #[cfg(not(any(feature = "text", feature = "geo")))]
                        _marker: std::marker::PhantomData,
                    },
                    #[cfg(feature = "tsdb")]
                    crate::server::handlers::query::TsdbLegBind {
                        tsdb: tsdb.as_deref(),
                        tsdb_tenant: tsdb_tenant.as_deref(),
                        tsdb_graph: tsdb_graph.as_deref(),
                        staged_series: Some(&staged_series),
                    },
                )
            } else {
                let committed = core_for_ctx.semantic_store.read().clone();
                let semantic = eg_core::compute::semantic::semantic_overlay(committed, &vectors);
                crate::server::handlers::query::run_unified(
                    plan,
                    &view,
                    &semantic,
                    crate::server::handlers::query::ServedIndexes {
                        #[cfg(feature = "text")]
                        text: Some(&served_text),
                        #[cfg(feature = "geo")]
                        spatial: Some(&served_spatial),
                        #[cfg(not(any(feature = "text", feature = "geo")))]
                        _marker: std::marker::PhantomData,
                    },
                    #[cfg(feature = "tsdb")]
                    crate::server::handlers::query::TsdbLegBind {
                        tsdb: tsdb.as_deref(),
                        tsdb_tenant: tsdb_tenant.as_deref(),
                        tsdb_graph: tsdb_graph.as_deref(),
                        staged_series: Some(&staged_series),
                    },
                )
            }
        })
        .await
        .map_err(|e| user_err(format!("UQL task failed: {e}")))?
        .map_err(|msg| user_err(format!("UQL error: {msg}")))?;
        Ok(WireOutcome::Rows(uql_rows_to_result(rows)))
    }

    /// Build a fresh single-statement [`crate::server::txn::GraphTxnState`] pinned to
    /// `graph` for an off-txn cross-modal auto-commit (CONCEPT:EG-KG.txn.isolation-ryow-begin-set).
    ///
    /// `isolation` (CONCEPT:EG-KG.txn.serializable-zero-cost — NE-005): the OPEN wire transaction's resolved
    /// level for the has-xmodal `COMMIT` path, or `Snapshot` for an off-txn
    /// single-statement auto-commit (a lone statement is already fully
    /// protected by the per-node OCC check regardless of declared level —
    /// there is no cross-statement anomaly window to widen). When
    /// `Serializable`, this drains and attaches this connection's
    /// [`WireSession::serializable_predicate_reads`] (each already
    /// fingerprinted AT READ TIME) so a SQL `SERIALIZABLE` transaction gets
    /// genuine phantom protection instead of silently behaving like
    /// `Snapshot`.
    #[cfg(feature = "query")]
    async fn new_txn_state(
        &self,
        graph: &str,
        isolation: crate::server::txn::IsolationLevel,
    ) -> WireResult<crate::server::txn::GraphTxnState> {
        let core = self.graph_core(graph).await?;
        let authority = self.carrier_authority()?;
        let mut ts = crate::server::txn::GraphTxnState::new(
            &core,
            crate::server::txn::NewTxnArgs {
                graph: graph.to_string(),
                tenant_scope: authority.tenant_scope().to_string(),
                begin_version: core.version(),
                isolation,
                predicate: None,
                agent: authority.owner_scope().to_string(),
                now_ms: crate::server::txn::now_ms(),
            },
        );
        if isolation == crate::server::txn::IsolationLevel::Serializable {
            let captured = std::mem::take(&mut *self.serializable_predicate_reads.lock());
            ts.predicate_reads.extend(captured);
        }
        Ok(ts)
    }

    #[cfg(all(feature = "query", feature = "tsdb"))]
    fn scope_measurement(
        &self,
        graph: &str,
        mut measurement: crate::server::txn::StagedMeasurement,
    ) -> WireResult<crate::server::txn::StagedMeasurement> {
        let authority = self.carrier_authority()?;
        measurement.series = eg_tsdb::store::SeriesKey::new(
            authority.tenant_scope(),
            authority.namespace("timeseries-graph", graph),
            measurement.series,
        )
        .encode();
        Ok(measurement)
    }

    /// Commit an assembled [`crate::server::txn::GraphTxnState`] through the SHARED RPC
    /// cross-modal commit (CONCEPT:EG-KG.txn.isolation-ryow-begin-set) — the SAME `commit_cross_modal_txn` the RPC
    /// `Method::Commit` drives, so graph + vector + OWL modalities land atomically in ONE
    /// authoritative redb transaction. Used by both
    /// the off-txn auto-commit and the wire `COMMIT` of a cross-modal txn.
    #[cfg(feature = "query")]
    async fn commit_txn_state(&self, ts: crate::server::txn::GraphTxnState) -> WireResult<()> {
        let authority = self.carrier_authority()?;
        let coordinator_id = crate::server::mutation_batch::opaque_coordinator_key(
            "wire-crossmodal-owner",
            authority.owner_scope(),
            &uuid::Uuid::new_v4().simple().to_string(),
        );
        // See the identical note in `commit_graph_methods` above: the RBAC recheck
        // inside `commit_cross_modal_txn` is keyed by `agent_id`, not `actor_scope`.
        let committed = crate::server::handlers::txn::commit_cross_modal_txn(
            &self.state,
            0,
            Some(authority.agent_id()),
            &coordinator_id,
            ts,
        )
        .await
        .map_err(user_err)?;
        if !committed {
            return Err(user_err("cross-modal transaction conflict"));
        }
        Ok(())
    }
}

/// A detected cross-modal wire statement (CONCEPT:EG-KG.txn.isolation-ryow-begin-set) — the parser/router output of
/// [`WireSession::detect_crossmodal`]. Carries the raw statement text; the heavy parse
/// happens in [`WireSession::exec_crossmodal`].
#[cfg(feature = "query")]
enum XmodalStmt {
    /// `UQL <plan text>` — a unified cross-modal read (RYOW in a txn).
    Uql(String),
    /// `SET EMBEDDING FOR <id> = <vector>` — stage/commit a vector upsert.
    SetEmbedding(String),
    /// `INSERT INTO series (id, ts, value) VALUES (…)` — stage/commit a measurement.
    InsertSeries(String),
    /// `SPARQL UPDATE <update>` — stage/commit OWL axioms (INSERT DATA lowered to methods).
    #[cfg(feature = "sparql")]
    SparqlUpdate(String),
    /// `SPARQL <CONSTRUCT/DESCRIBE query>` — stage/commit the CONSTRUCT'd triples.
    #[cfg(feature = "sparql")]
    SparqlConstruct(String),
}

/// Project a unified-query result (`[(id, score)]`) into a two-column typed row set
/// (`id TEXT`, `score FLOAT8`) so the wire encodes it exactly like any other read.
#[cfg(feature = "query")]
fn uql_rows_to_result(rows: Vec<(String, Option<f32>)>) -> TypedQueryResult {
    let columns = vec![
        TypedColumn {
            name: "id".to_string(),
            ty: PgColType::Text,
        },
        TypedColumn {
            name: "score".to_string(),
            ty: PgColType::Float8,
        },
    ];
    let rows = rows
        .into_iter()
        .map(|(id, score)| {
            vec![
                serde_json::Value::String(id),
                match score {
                    Some(s) => serde_json::json!(s),
                    None => serde_json::Value::Null,
                },
            ]
        })
        .collect();
    TypedQueryResult { columns, rows }
}

/// Parse `SET EMBEDDING FOR <id> = <vector>` (CONCEPT:EG-KG.txn.isolation-ryow-begin-set) into `(node_id, embedding)`.
/// `<id>` and `<vector>` may be single/double-quoted; `<vector>` is a `[a, b, c]` literal.
#[cfg(feature = "query")]
fn parse_set_embedding(sql: &str) -> WireResult<(String, Vec<f32>)> {
    let lower = sql.to_ascii_lowercase();
    let for_pos = lower
        .find(" for ")
        .ok_or_else(|| user_err("SET EMBEDDING requires `FOR <id> = <vector>`"))?;
    let after_for = &sql[for_pos + 5..];
    let eq_pos = after_for
        .find('=')
        .ok_or_else(|| user_err("SET EMBEDDING requires `= <vector>`"))?;
    let id = after_for[..eq_pos]
        .trim()
        .trim_matches(['\'', '"'].as_ref())
        .to_string();
    if id.is_empty() {
        return Err(user_err("SET EMBEDDING requires a node id"));
    }
    let vec_part = after_for[eq_pos + 1..]
        .trim()
        .trim_matches(['\'', '"'].as_ref());
    let mut out = Vec::new();
    for tok in vec_part
        .trim()
        .trim_start_matches('[')
        .trim_end_matches(']')
        .split(',')
    {
        let t = tok.trim();
        if t.is_empty() {
            continue;
        }
        out.push(
            t.parse::<f32>()
                .map_err(|_| user_err(format!("invalid embedding component '{t}'")))?,
        );
    }
    if out.is_empty() {
        return Err(user_err("embedding vector is empty"));
    }
    Ok((id, out))
}

/// Parse `INSERT INTO series (cols) VALUES (vals)` (CONCEPT:EG-KG.txn.isolation-ryow-begin-set) into a
/// [`crate::server::txn::StagedMeasurement`] — one point. Columns `id`/`series`,
/// `ts`/`time`, `value`/`val` (any order); values may be quoted.
#[cfg(feature = "query")]
fn parse_insert_series(sql: &str) -> WireResult<crate::server::txn::StagedMeasurement> {
    let lower = sql.to_ascii_lowercase();
    let values_pos = lower
        .find("values")
        .ok_or_else(|| user_err("INSERT INTO series requires VALUES"))?;
    let paren_slice = |s: &str, what: &str| -> WireResult<String> {
        let open = s
            .find('(')
            .ok_or_else(|| user_err(format!("INSERT INTO series: missing {what}")))?;
        let close = s[open..]
            .find(')')
            .ok_or_else(|| user_err(format!("INSERT INTO series: unterminated {what}")))?
            + open;
        Ok(s[open + 1..close].to_string())
    };
    let cols_raw = paren_slice(&sql[..values_pos], "column list")?;
    let vals_raw = paren_slice(&sql[values_pos..], "VALUES tuple")?;
    let cols: Vec<String> = cols_raw
        .split(',')
        .map(|c| c.trim().to_ascii_lowercase())
        .collect();
    let vals: Vec<&str> = vals_raw.split(',').map(|v| v.trim()).collect();
    if cols.len() != vals.len() {
        return Err(user_err("INSERT INTO series column/value count mismatch"));
    }
    let (mut id, mut ts, mut value): (Option<String>, Option<i64>, Option<f64>) =
        (None, None, None);
    for (c, v) in cols.iter().zip(vals.iter()) {
        let cleaned = v.trim_matches(['\'', '"'].as_ref());
        match c.as_str() {
            "id" | "series" => id = Some(cleaned.to_string()),
            "ts" | "time" | "timestamp" => {
                ts = Some(
                    cleaned
                        .parse::<i64>()
                        .map_err(|_| user_err(format!("invalid series ts '{cleaned}'")))?,
                )
            }
            "value" | "val" => {
                value = Some(
                    cleaned
                        .parse::<f64>()
                        .map_err(|_| user_err(format!("invalid series value '{cleaned}'")))?,
                )
            }
            other => {
                return Err(user_err(format!(
                    "unknown series column '{other}' (expected id, ts, value)"
                )))
            }
        }
    }
    Ok(crate::server::txn::StagedMeasurement {
        series: id.ok_or_else(|| user_err("INSERT INTO series requires an `id` column"))?,
        n_fields: 1,
        bucket_ns: 3_600_000_000_000,
        field_names: vec!["f0".to_string()],
        points: vec![(
            ts.ok_or_else(|| user_err("INSERT INTO series requires a `ts` column"))?,
            vec![value.ok_or_else(|| user_err("INSERT INTO series requires a `value` column"))?],
        )],
    })
}

#[async_trait]
impl WireProtocol for WireSession {
    fn current_graph(&self) -> String {
        self.graph.lock().clone()
    }

    fn in_txn(&self) -> bool {
        self.txn.lock().is_some()
    }

    fn actor(&self) -> Option<String> {
        self.actor.lock().clone()
    }

    /// One-time, on the first query: adopt the wire's startup `database` as the
    /// target graph (priority 1). The startup `user` is used only to distinguish
    /// libpq's implicit `database=user` default; it never establishes authority.
    ///
    /// The graph adoption is skipped if a `SET graph` already chose one, if the param
    /// is absent, or if it equals the user name (libpq's default when `dbname` is
    /// unset — not a deliberate pick). Identity is established exclusively through
    /// `authenticate_request` or `bind_authenticated_sql_actor`; execution rejects an
    /// actor-only connection. Idempotent.
    fn resolve_startup(&self, user: Option<String>, database: Option<String>) {
        use std::sync::atomic::Ordering;
        if self.startup_resolved.swap(true, Ordering::AcqRel) {
            return;
        }
        let db = match database {
            Some(ref d) if !d.is_empty() => d,
            _ => return,
        };
        // libpq defaults `database` to the user name when `dbname` is unset; that
        // is not a deliberate graph pick, so leave the server default in place.
        if user.as_deref() == Some(db.as_str()) {
            return;
        }
        *self.graph.lock() = db.clone();
    }

    /// The shared SQL execution core for every wire (CONCEPT:EG-KG.compute.subsystems-reference / KG-2.197).
    /// `sql` is already a complete literal statement (any wire-specific parameter form
    /// has been substituted before calling). Classifies and routes:
    ///   * `SET graph = …` → connection-scoped graph switch.
    ///   * a read → the DataFusion path, returned as a typed row set.
    ///   * a write → the GraphTxn + durability path; a `RETURNING` write yields a row
    ///     set, otherwise a command tag.
    ///   * transaction control (`BEGIN`/`COMMIT`/`ROLLBACK`) → a txn-status change.
    async fn execute(&self, sql: &str) -> WireResult<WireOutcome> {
        // No SQL statement, transaction-control command, graph switch, or catalog
        // probe may touch state until a current signed request or a verified native
        // SQL proxy handshake has bound tenant+actor authority to the connection.
        self.carrier_authority()?;
        if let Some(res) = self.try_set_graph(sql) {
            return res;
        }
        // NE-005: `SET TRANSACTION ISOLATION LEVEL …` / `SET SESSION
        // CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL …` — neither has a
        // `sqlparser` AST node this classifier recognizes (see
        // `try_set_isolation`'s doc), so intercept them textually BEFORE the
        // classifier, exactly like `try_set_graph` does for `SET graph`.
        if let Some(res) = self.try_set_isolation(sql) {
            return res;
        }
        let graph = self.current_graph();
        // NE-004: a mixed graph+table transaction from a PRIOR connection on
        // this same owner may have crashed between its two commits; resolve
        // it before this connection stages any NEW work of its own. Cheap
        // when there is nothing to recover (a directory-listing miss).
        self.recover_owner_intents_once().await?;

        // ── pgwire CROSS-MODAL transaction seam (CONCEPT:EG-KG.txn.isolation-ryow-begin-set) ──────────────────
        // Recognize the cross-modal verbs BEFORE the SQL classifier (a UQL query / a
        // `SET EMBEDDING` / an `INSERT INTO series` / a `SPARQL …` statement is not SQL)
        // and route them onto the committed RPC seam. The same aborted-txn gate,
        // ACL check, and aborted-latch-on-error the SQL dispatch applies wrap them.
        #[cfg(feature = "query")]
        if let Some(stmt) = Self::detect_crossmodal(sql) {
            if self.in_txn() && self.txn_aborted() {
                return Err(aborted_txn_err());
            }
            self.check_access(&graph, Self::crossmodal_access(&stmt))
                .await?;
            let in_txn = self.in_txn();
            let result = self.exec_crossmodal(&graph, stmt, in_txn).await;
            if in_txn && result.is_err() {
                *self.txn_failed.lock() = true;
            }
            return result;
        }

        let kind = eg_query::classify(sql).map_err(user_err)?;

        // ── transaction control (CONCEPT:EG-KG.query.register-each-user-table / EG-049) — no graph access needed ──
        // BEGIN/COMMIT/ROLLBACK report a txn-status change so the wire reports the
        // correct in-transaction status to the driver.
        match &kind {
            StatementKind::Begin => {
                self.begin_txn();
                // NE-005: resolve this txn's isolation level — an inline
                // `BEGIN … ISOLATION LEVEL …` clause wins; otherwise a prior
                // bare `SET TRANSACTION ISOLATION LEVEL …` (Postgres: applies
                // to the NEXT transaction only); otherwise the session
                // default; otherwise `Snapshot`.
                let resolved = match parse_begin_isolation_clause(sql) {
                    Some(Ok(level)) => level,
                    Some(Err(e)) => {
                        // Malformed/unsupported clause: the transaction never
                        // really opened — undo `begin_txn()`'s state and
                        // reject with a typed error (never silently accept an
                        // isolation level we cannot provide).
                        self.take_txn();
                        #[cfg(feature = "query")]
                        let _ = self.take_xmodal();
                        return Err(e);
                    }
                    None => self
                        .pending_isolation
                        .lock()
                        .take()
                        .or_else(|| *self.session_isolation_default.lock())
                        .unwrap_or(crate::server::txn::IsolationLevel::Snapshot),
                };
                *self.txn_isolation.lock() = resolved;
                return Ok(WireOutcome::TxnStart);
            }
            StatementKind::Commit => return self.run_commit().await,
            StatementKind::Rollback => {
                // ROLLBACK drops both buffers + the cross-modal staging + the RYOW
                // overlay (nothing was applied in-memory), and always ends the block.
                self.take_txn();
                #[cfg(feature = "query")]
                let _ = self.take_xmodal();
                self.serializable_predicate_reads.lock().clear();
                self.txn_replay_log.lock().clear();
                return Ok(WireOutcome::TxnEnd { tag: "ROLLBACK" });
            }
            _ => {}
        }

        // ── aborted-transaction gate (CONCEPT:EG-KG.compute.kg-transaction-is-pinned) ──────────────────────────────
        // Once a statement inside an open txn has errored, every subsequent statement
        // except COMMIT/ROLLBACK (handled above) is rejected with 25P02 until the
        // block ends — matching Postgres' failed-transaction semantics.
        if self.in_txn() && self.txn_aborted() {
            return Err(aborted_txn_err());
        }

        // Enforce the engine ACL under the connection's authenticated actor
        // (CONCEPT:EG-KG.query.concept-13) BEFORE touching the graph: a read needs Read access, any
        // DML needs Write. The authenticated actor must exist in durable policy.
        let access = match kind {
            // CONCEPT:EG-KG.query.postgres-family-extension-plan — an AGE cypher() call is a read.
            StatementKind::Read | StatementKind::CypherCall(_) => AccessLevel::Read,
            _ => AccessLevel::Write,
        };
        self.check_access(&graph, access).await?;

        // While a transaction is OPEN, buffer BOTH user-table DDL/DML (into `txn`)
        // and graph-node DML (into `graph_txn`); the buffers are applied at COMMIT.
        // Reads run immediately but over a read-your-own-writes overlay (CONCEPT:EG-KG.compute.kg-transaction-is-pinned)
        // so they observe the txn's own buffered writes.
        let in_txn = self.in_txn();

        // NE-005 auto predicate tracking: a `Serializable` transaction's OWN
        // reads seed the predicate read-set the commit-time check protects —
        // see `extract_label_predicates`'s doc for exactly what shape this
        // captures (and does not). Fingerprinted RIGHT NOW, before the read
        // itself runs — the "captured at begin" baseline `validate()`
        // re-checks at commit MUST be from read time, never recomputed later
        // (a baseline captured seconds — or even microseconds — before
        // `validate()` re-checks it protects nothing; see
        // `GraphTxnState::add_predicate_read`'s doc).
        if in_txn
            && matches!(kind, StatementKind::Read)
            && *self.txn_isolation.lock() == crate::server::txn::IsolationLevel::Serializable
        {
            let labels = extract_label_predicates(sql);
            if !labels.is_empty() {
                if let Ok(core) = self.graph_core(&graph).await {
                    let mut captured = self.serializable_predicate_reads.lock();
                    for label in labels {
                        let predicate = crate::server::txn::PredicateRead::Label(label);
                        let fp = predicate.fingerprint(&core);
                        captured.push((predicate, fp));
                    }
                }
            }
        }

        // NE-004: how many table ops this txn has buffered BEFORE dispatch —
        // compared against the count AFTER, below, to detect (without
        // enumerating every table-touching `StatementKind`) whether THIS
        // statement just buffered a table op, so its literal SQL text can be
        // recorded into the replay log a crash-recovered mixed transaction
        // rebuilds its `TableTxn` from.
        let table_ops_before = if in_txn {
            self.txn.lock().as_ref().map(|t| t.ops.len()).unwrap_or(0)
        } else {
            0
        };

        // CONCEPT:EG-OS.observability.slow-query-descriptor — slow-query timing for the wire SQL path (psql/BI/ORM).
        // `None` (zero cost) unless EPISTEMIC_GRAPH_SLOW_QUERY_MS is set.
        let slow = crate::slow_query::describe_sql(sql);
        let slow_start = slow.as_ref().map(|_| std::time::Instant::now());

        // Run the dispatch, latching the transaction into the aborted state on any
        // error so subsequent statements are rejected with 25P02 (CONCEPT:EG-KG.compute.kg-transaction-is-pinned).
        let result = self.dispatch_kind(&graph, sql, kind, in_txn).await;
        if let (Some(slow), Some(start)) = (slow, slow_start) {
            slow.log_if_slow(start.elapsed());
        }
        if in_txn && result.is_ok() {
            let table_ops_after = self.txn.lock().as_ref().map(|t| t.ops.len()).unwrap_or(0);
            if table_ops_after > table_ops_before {
                self.txn_replay_log
                    .lock()
                    .push(crate::server::txn_intent::ReplayStep::Sql(sql.to_string()));
            }
        }
        if in_txn && result.is_err() {
            *self.txn_failed.lock() = true;
        }
        result
    }
}

#[cfg(test)]
mod ne_004_ne_005_tests {
    //! Unit tests for NE-004 (mixed graph+table transaction atomicity via a
    //! durable commit-intent) and NE-005 (SQL-selectable isolation level).
    //!
    //! These construct a `WireSession` + `ServerState` directly (in-crate, so
    //! `pub(crate)`/private items are reachable) rather than driving a real
    //! wire listener — the SAME lighter-weight approach `server::mod::tests`
    //! uses for its own dispatch-level tests. `bind_authenticated_sql_actor`
    //! is the SAME native-SQL-identity seam `sqlite_wire`/`mysql_wire` use;
    //! it is not a test-only shortcut.
    use super::*;
    use crate::isolation::{AgentIdentity, AgentRole, IsolationLayer};
    use crate::protocol::{GraphType, Method};
    use crate::registry::GraphRegistry;
    use dashmap::DashMap;
    use tokio::sync::Semaphore;

    const SECRET: &str = "eg-txn-atomicity-test-secret";
    const AGENT: &str = "txn-test-agent";

    fn ensure_env() {
        static ONCE: std::sync::Once = std::sync::Once::new();
        ONCE.call_once(|| {
            std::env::set_var("EPISTEMIC_GRAPH_AUDIENCE", "epistemic-graph-test");
            std::env::set_var("EPISTEMIC_GRAPH_TENANT", "tenant-shared");
            std::env::set_var("EPISTEMIC_GRAPH_POLICY_VERSION", "policy-test");
            #[cfg(feature = "security")]
            std::env::set_var(
                crate::crypto::ENCRYPTION_KEY_ENV,
                "eg-txn-atomicity-test-recovery-key",
            );
        });
    }

    /// A real durable `ServerState` on its own uniquely-named temp dir (mirrors
    /// `server::mod::tests::test_state`, trimmed to nothing this module's
    /// tests don't need). `AGENT` is registered `AgentRole::System` so every
    /// access check trivially passes regardless of the `security` feature —
    /// this module tests transaction ATOMICITY, not RBAC (that is covered
    /// elsewhere).
    fn test_state() -> Arc<RwLock<ServerState>> {
        ensure_env();
        let mut isolation = IsolationLayer::new();
        isolation.register_agent(AgentIdentity {
            agent_id: AGENT.into(),
            role: AgentRole::System,
            teams: Vec::new(),
            roles: Vec::new(),
        });
        Arc::new(RwLock::new(ServerState {
            #[cfg(feature = "redb")]
            cold_tracker: std::sync::Arc::new(
                crate::server::persistence::cold_offload::ColdTenantTracker::new(),
            ),
            registry: GraphRegistry::new(),
            isolation,
            channels: crate::channels::ChannelManager::new(),
            #[cfg(feature = "viz-static-export")]
            viz_engine: None,
            auth_secret: SECRET.to_string(),
            #[cfg(feature = "query")]
            persist_dir: Some(
                crate::server::sql_tables::test_persist_dir()
                    .to_string_lossy()
                    .into_owned(),
            ),
            #[cfg(not(feature = "query"))]
            persist_dir: None,
            #[cfg(feature = "redb")]
            persistence: Some(std::sync::Arc::new(
                crate::server::persistence::redb_backend::RedbBackend::open(
                    crate::server::unique_temp_dir("eg-wire-txn-atomicity-test")
                        .to_string_lossy()
                        .into_owned(),
                    crate::durability::DurabilityPolicy::Each,
                    256,
                )
                .expect("open test redb backend"),
            )),
            #[cfg(not(feature = "redb"))]
            persistence: None,
            max_in_flight: Arc::new(Semaphore::new(16)),
            read_admission: Arc::new(Semaphore::new(16)),
            per_graph_inflight: Arc::new(DashMap::new()),
            per_graph_inflight_limit: 8,
            write_coalescer: Arc::new(crate::write_coalescer::WriteCoalescerRegistry::new()),
            routed_write_coalescer: Arc::new(
                crate::server::routed_write_coalescer::RoutedWriteCoalescerRegistry::new(),
            ),
            open_txns: Arc::new(DashMap::new()),
            txn_id_gen: Arc::new(crate::server::txn::TxnIdGen),
            txn_ttl_secs: 300,
            txn_max_per_graph: 256,
            txn_max_per_agent: 256,
            #[cfg(feature = "blob")]
            blob: None,
            #[cfg(feature = "blob")]
            blob_cursor_ttl_secs: 300,
            #[cfg(feature = "raft")]
            raft: None,
            #[cfg(feature = "raft")]
            multi_raft: None,
            #[cfg(feature = "tsdb")]
            tsdb_store: Some(Arc::new(
                eg_tsdb::store::SeriesStore::open(&std::env::temp_dir().join(format!(
                    "eg-wire-txn-atomicity-tsdb-test-{}-{}.redb",
                    std::process::id(),
                    std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .map(|d| d.as_nanos() as u64)
                        .unwrap_or(0)
                )))
                .expect("open test series store"),
            )),
            #[cfg(feature = "streaming")]
            cdc: Some(std::sync::Arc::new(crate::server::cdc::CdcHub::new())),
            #[cfg(feature = "wasm-udf")]
            udf_registry: std::sync::Arc::new(eg_wasm::UdfRegistry::new()),
            #[cfg(feature = "compute-dist")]
            matviews: std::sync::Arc::new(parking_lot::Mutex::new(
                crate::raft::pregel::MatViewStore::new(),
            )),
            #[cfg(feature = "federation")]
            foreign_sources: std::sync::Arc::new(DashMap::new()),
            #[cfg(feature = "kv")]
            kv: None,
            #[cfg(feature = "lake")]
            lake: std::sync::Arc::new(crate::server::lake::LakeManager::new()),
        }))
    }

    /// Build a signed `Request` exactly like `server::mod::tests::request` —
    /// only used here to drive the ONE real `Method::CreateGraph` RPC so a
    /// test graph is durably initialized the SAME way production does it
    /// (never by poking the registry directly).
    fn request(id: u64, graph: &str, method: Method) -> Request {
        static NONCE: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
        let claims = crate::acl::RequestContextClaims {
            principal: AGENT.to_string(),
            tenant: "tenant-shared".to_string(),
            audience: "epistemic-graph-test".to_string(),
            agent_id: AGENT.to_string(),
            roles: vec!["test".to_string()],
            scopes: vec!["*".to_string()],
            policy_version: "policy-test".to_string(),
            delegation: Vec::new(),
            node: None,
            priority: None,
        };
        let mut req = Request {
            id,
            graph: graph.to_string(),
            auth_token: String::new(),
            agent_id: Some(AGENT.to_string()),
            method,
        };
        let nonce = format!(
            "ne004-005-{}-{}",
            std::process::id(),
            NONCE.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        );
        req.auth_token = crate::server::compute_verified_envelope_token(
            SECRET,
            &req,
            &crate::server::VerifiedEnvelopeParams {
                context: &claims,
                timestamp: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .expect("system clock")
                    .as_secs(),
                nonce: &nonce,
                idempotency_key: &format!("ne004-005-request-{id}"),
            },
        );
        req
    }

    async fn create_test_graph(state: &Arc<RwLock<ServerState>>, graph: &str, id: u64) {
        let resp = crate::server::dispatch(
            state,
            request(
                id,
                graph,
                Method::CreateGraph {
                    graph_name: graph.to_string(),
                    graph_type: GraphType::Agent,
                },
            ),
        )
        .await;
        assert!(
            resp.error.is_none(),
            "CreateGraph failed: {:?}",
            resp.error
        );
    }

    /// A fresh, authenticated `WireSession` — a new "connection" sharing
    /// `state`, exactly the shape a crash-then-reconnect looks like: no
    /// in-memory buffer carries over, only what is durable on disk does.
    async fn new_session(state: Arc<RwLock<ServerState>>, graph: &str) -> WireSession {
        let session = WireSession::new(state, graph.to_string());
        session
            .bind_authenticated_sql_actor("pgwire", AGENT)
            .await
            .expect("bind test identity");
        session
    }

    async fn node_count(session: &WireSession, graph: &str, id: &str) -> usize {
        match session
            .run_read(graph, format!("SELECT id FROM nodes WHERE id = '{id}'"))
            .await
        {
            Ok(result) => result.rows.len(),
            Err(_) => 0,
        }
    }

    async fn table_row_count(session: &WireSession, graph: &str, sql: &str) -> usize {
        match session.run_read(graph, sql.to_string()).await {
            Ok(result) => result.rows.len(),
            Err(_) => 0,
        }
    }

    /// NE-004, DoD item 1: a mixed graph+table transaction commits
    /// atomically — both sides are visible after `COMMIT`.
    #[tokio::test]
    async fn mixed_txn_commits_atomically() {
        let state = test_state();
        let graph = "ne004-atomic";
        create_test_graph(&state, graph, 1).await;
        let session = new_session(state.clone(), graph).await;
        session
            .execute("CREATE TABLE ledger (id INT, note TEXT)")
            .await
            .expect("create table");

        session.execute("BEGIN").await.expect("begin");
        session
            .execute("INSERT INTO nodes (id) VALUES ('atomic-node')")
            .await
            .expect("buffer graph insert");
        session
            .execute("INSERT INTO ledger (id, note) VALUES (1, 'paired')")
            .await
            .expect("buffer table insert");
        session.execute("COMMIT").await.expect("commit");

        assert_eq!(node_count(&session, graph, "atomic-node").await, 1);
        assert_eq!(
            table_row_count(&session, graph, "SELECT id FROM ledger WHERE id = 1").await,
            1
        );
    }

    /// NE-004, DoD item 2: an injected failure AT the table-commit step
    /// (here, a natural one — the target table does not exist, which
    /// `eg_query::classify` cannot detect syntactically, only the table
    /// store's own commit-time schema lookup can) leaves ZERO graph writes
    /// applied. The graph side already landed durably by the time the table
    /// commit is attempted; this proves the synchronous COMPENSATION path
    /// (not just the crash-recovery path) actually undoes it.
    #[tokio::test]
    async fn mixed_txn_table_failure_leaves_zero_graph_writes() {
        let state = test_state();
        let graph = "ne004-compensate";
        create_test_graph(&state, graph, 2).await;
        let session = new_session(state.clone(), graph).await;

        session.execute("BEGIN").await.expect("begin");
        session
            .execute("INSERT INTO nodes (id) VALUES ('compensated-node')")
            .await
            .expect("buffer graph insert");
        session
            .execute("INSERT INTO nonexistent_table (id) VALUES (1)")
            .await
            .expect("buffering a DML statement never validates the target table");
        let commit_result = session.execute("COMMIT").await;
        assert!(
            commit_result.is_err(),
            "COMMIT against a nonexistent table must fail"
        );

        assert_eq!(
            node_count(&session, graph, "compensated-node").await,
            0,
            "the graph write must be compensated away, not left applied"
        );
    }

    /// NE-004, DoD item 3: an injected failure AFTER the graph commit but
    /// BEFORE the table commit is detected and resolved on restart with no
    /// torn state.
    ///
    /// Simulates the crash by running exactly the first half of
    /// `commit_mixed_txn` (write the durable intent, then commit the graph
    /// side) and deliberately stopping there — never calling the table
    /// commit or deleting the intent, exactly the state a real process death
    /// in that window leaves on disk. The graph write already went through
    /// the SAME `commit_cross_modal_txn` / redb `WriteTransaction::commit()`
    /// path an ordinary commit uses, so it is genuinely durable, not merely
    /// in-memory. A brand-new `WireSession` (no buffers/state carried over —
    /// the connection-level analogue of a process restart) then runs one
    /// ordinary statement, which lazily triggers recovery.
    #[tokio::test]
    async fn mixed_txn_crash_between_commits_recovers_on_restart() {
        let state = test_state();
        let graph = "ne004-crash-recovery";
        create_test_graph(&state, graph, 3).await;
        let session = new_session(state.clone(), graph).await;
        session
            .execute("CREATE TABLE audit (id INT, note TEXT)")
            .await
            .expect("create table");

        session.execute("BEGIN").await.expect("begin");
        session
            .execute("INSERT INTO nodes (id) VALUES ('crash-node')")
            .await
            .expect("buffer graph insert");
        session
            .execute("INSERT INTO audit (id, note) VALUES (7, 'pending')")
            .await
            .expect("buffer table insert");

        // ---- inline, truncated `commit_mixed_txn`: stop after phase 1 ----
        let (table_txn, node_ops) = session.take_txn();
        let table_txn = table_txn.expect("table ops were buffered");
        let methods = WireSession::node_ops_to_methods(&node_ops).expect("lower node ops");
        let isolation = *session.txn_isolation.lock();
        let compensating = session
            .compensating_methods(graph, &methods)
            .await
            .expect("snapshot pre-txn state");
        let table_steps = std::mem::take(&mut *session.txn_replay_log.lock());
        let operation_id = uuid::Uuid::new_v4();
        let intent = crate::server::txn_intent::CommitIntent::new(
            graph.to_string(),
            operation_id,
            methods.clone(),
            compensating,
            table_steps,
            crate::server::txn::now_ms(),
        );
        let authority = session.carrier_authority().expect("bound authority");
        let persist_dir_buf = state
            .read()
            .await
            .persist_dir
            .clone()
            .expect("persist dir configured");
        let persist_dir = std::path::Path::new(&persist_dir_buf);
        crate::server::txn_intent::write_intent(&authority, persist_dir, &intent)
            .expect("write durable intent");
        session
            .commit_graph_methods_with_op(graph, methods, operation_id, isolation)
            .await
            .expect("phase 1: graph commit");
        // `table_txn` is deliberately never committed and the intent is
        // deliberately never deleted — this IS the crash.
        drop(table_txn);
        drop(session);

        // The torn state is real: the graph write is durable, the table
        // write is not, and the intent survives on disk.
        assert_eq!(
            crate::server::txn_intent::list_intents(&authority, persist_dir).len(),
            1,
            "the crashed intent must still be on disk"
        );
        let probe = new_session(state.clone(), graph).await;
        assert_eq!(node_count(&probe, graph, "crash-node").await, 1);
        assert_eq!(
            table_row_count(&probe, graph, "SELECT id FROM audit WHERE id = 7").await,
            0,
            "the table side must NOT be applied yet — this is the torn state"
        );

        // A brand-new connection for the SAME owner: its first statement
        // lazily sweeps and resolves the leftover intent.
        let fresh = new_session(state.clone(), graph).await;
        fresh
            .execute("SELECT id FROM nodes WHERE id = 'unrelated-probe'")
            .await
            .expect("a trivial read triggers recovery as a side effect");

        assert!(
            crate::server::txn_intent::list_intents(&authority, persist_dir).is_empty(),
            "recovery must clear the resolved intent"
        );
        assert_eq!(
            node_count(&fresh, graph, "crash-node").await,
            1,
            "the graph write must remain applied EXACTLY once (idempotent replay, no duplication)"
        );
        assert_eq!(
            table_row_count(&fresh, graph, "SELECT id FROM audit WHERE id = 7").await,
            1,
            "recovery must complete the table side — no torn state survives"
        );
    }

    /// `ROLLBACK` still drops everything, including a mixed graph+table
    /// transaction's NE-004 bookkeeping (the replay log / serializable
    /// label set) — no commit-intent is ever written for a rolled-back txn.
    #[tokio::test]
    async fn rollback_drops_mixed_transaction_entirely() {
        let state = test_state();
        let graph = "ne004-rollback";
        create_test_graph(&state, graph, 4).await;
        let session = new_session(state.clone(), graph).await;
        session
            .execute("CREATE TABLE ledger (id INT)")
            .await
            .expect("create table");

        session.execute("BEGIN").await.expect("begin");
        session
            .execute("INSERT INTO nodes (id) VALUES ('rolled-back-node')")
            .await
            .expect("buffer graph insert");
        session
            .execute("INSERT INTO ledger (id) VALUES (1)")
            .await
            .expect("buffer table insert");
        let outcome = session.execute("ROLLBACK").await.expect("rollback");
        assert!(matches!(
            outcome,
            WireOutcome::TxnEnd { tag: "ROLLBACK" }
        ));

        assert_eq!(node_count(&session, graph, "rolled-back-node").await, 0);
        assert_eq!(
            table_row_count(&session, graph, "SELECT id FROM ledger WHERE id = 1").await,
            0
        );
    }

    /// The `25P02` aborted-transaction-block behaviour is unchanged: a
    /// statement error inside an open txn latches it aborted; every
    /// subsequent statement except COMMIT/ROLLBACK is rejected `25P02` until
    /// the block ends, and COMMIT while aborted behaves as ROLLBACK.
    #[tokio::test]
    async fn aborted_transaction_block_still_rejects_with_25p02() {
        let state = test_state();
        let graph = "ne004-aborted";
        create_test_graph(&state, graph, 5).await;
        let session = new_session(state.clone(), graph).await;

        session.execute("BEGIN").await.expect("begin");
        let bad = session.execute("SELECT * FROM this_table_does_not_exist").await;
        assert!(bad.is_err(), "a bad statement must fail");

        let next = session.execute("SELECT 1").await;
        let err = next.expect_err("a statement after an error must be rejected");
        assert_eq!(err.code, "25P02");

        let commit_outcome = session.execute("COMMIT").await.expect("commit-as-rollback");
        assert!(matches!(
            commit_outcome,
            WireOutcome::TxnEnd { tag: "ROLLBACK" }
        ));
    }

    /// NE-005: an unsupported isolation-level spelling is REJECTED with a
    /// typed error (SQLSTATE `0A000`) — never silently accepted.
    #[tokio::test]
    async fn unsupported_isolation_level_is_rejected_not_downgraded() {
        let state = test_state();
        let graph = "ne005-reject";
        create_test_graph(&state, graph, 6).await;
        let session = new_session(state.clone(), graph).await;

        let err = session
            .execute("SET TRANSACTION ISOLATION LEVEL BOGUS")
            .await
            .expect_err("an unsupported level must be rejected");
        assert_eq!(err.code, "0A000");
    }

    /// NE-005: `REPEATABLE READ` / `READ COMMITTED` / `READ UNCOMMITTED` are
    /// accepted (mapped upward onto `Snapshot`, never rejected — this engine
    /// always provides AT LEAST that much).
    #[tokio::test]
    async fn weaker_standard_isolation_levels_are_accepted() {
        let state = test_state();
        let graph = "ne005-weaker";
        create_test_graph(&state, graph, 7).await;
        let session = new_session(state.clone(), graph).await;

        for level in ["REPEATABLE READ", "READ COMMITTED", "READ UNCOMMITTED"] {
            session
                .execute(&format!("BEGIN ISOLATION LEVEL {level}"))
                .await
                .unwrap_or_else(|e| panic!("{level} must be accepted, got {e:?}"));
            assert_eq!(
                *session.txn_isolation.lock(),
                crate::server::txn::IsolationLevel::Snapshot
            );
            session.execute("ROLLBACK").await.expect("rollback");
        }
    }

    /// NE-005's central rule: a requested `SERIALIZABLE` either genuinely
    /// serializes or is rejected — never silently downgraded to `Snapshot`.
    /// Proved with a concurrent read-modify-write write-skew scenario a
    /// per-node OCC check (which `Snapshot` already has) does NOT catch: two
    /// transactions each scan `label = 'pool'` nodes and each insert one
    /// more IF the scanned count is below a cap — neither writes to a node
    /// the other reads, so only the AUTO predicate-label tracking
    /// (`extract_label_predicates` + `GraphTxnState::add_predicate_read`)
    /// can catch the phantom. Under `SERIALIZABLE`, the second committer
    /// MUST be rejected `40001`; the SAME scenario under the engine default
    /// (`Snapshot`) is proved to let both through, showing this is a REAL
    /// difference in behaviour, not a coincidental failure.
    #[tokio::test]
    async fn serializable_genuinely_catches_a_write_skew_snapshot_misses() {
        let state = test_state();
        let graph = "ne005-serializable";
        create_test_graph(&state, graph, 8).await;
        let seed = new_session(state.clone(), graph).await;
        for i in 0..2 {
            seed.execute(&format!(
                "INSERT INTO nodes (id, type) VALUES ('pool-{i}', 'pool')"
            ))
            .await
            .expect("seed pool nodes");
        }

        // ---- SERIALIZABLE: the second committer must be rejected. ----
        let a = new_session(state.clone(), graph).await;
        let b = new_session(state.clone(), graph).await;
        a.execute("BEGIN ISOLATION LEVEL SERIALIZABLE")
            .await
            .expect("begin a");
        b.execute("BEGIN ISOLATION LEVEL SERIALIZABLE")
            .await
            .expect("begin b");
        assert_eq!(a.run_read(graph, "SELECT id FROM nodes WHERE type = 'pool'".to_string()).await.unwrap().rows.len(), 2);
        assert_eq!(b.run_read(graph, "SELECT id FROM nodes WHERE type = 'pool'".to_string()).await.unwrap().rows.len(), 2);
        a.execute("INSERT INTO nodes (id, type) VALUES ('pool-from-a', 'pool')")
            .await
            .expect("buffer a's insert");
        b.execute("INSERT INTO nodes (id, type) VALUES ('pool-from-b', 'pool')")
            .await
            .expect("buffer b's insert");
        a.execute("COMMIT").await.expect("a commits first");
        let b_commit = b.execute("COMMIT").await;
        assert!(
            b_commit.is_err(),
            "SERIALIZABLE must reject the phantom-inducing second commit"
        );
        assert_eq!(b_commit.unwrap_err().code, "40001");

        // ---- Snapshot (the default): the SAME scenario is let through,
        // proving SERIALIZABLE is not a no-op relabeling. ----
        let c = new_session(state.clone(), graph).await;
        let d = new_session(state.clone(), graph).await;
        c.execute("BEGIN").await.expect("begin c");
        d.execute("BEGIN").await.expect("begin d");
        assert_eq!(c.run_read(graph, "SELECT id FROM nodes WHERE type = 'pool'".to_string()).await.unwrap().rows.len(), 3);
        assert_eq!(d.run_read(graph, "SELECT id FROM nodes WHERE type = 'pool'".to_string()).await.unwrap().rows.len(), 3);
        c.execute("INSERT INTO nodes (id, type) VALUES ('pool-from-c', 'pool')")
            .await
            .expect("buffer c's insert");
        d.execute("INSERT INTO nodes (id, type) VALUES ('pool-from-d', 'pool')")
            .await
            .expect("buffer d's insert");
        c.execute("COMMIT").await.expect("c commits");
        d.execute("COMMIT")
            .await
            .expect("Snapshot does NOT catch this write skew — both commit");
    }
}
