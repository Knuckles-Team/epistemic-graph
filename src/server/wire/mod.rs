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
        let operation = TxnOp::Insert {
            table,
            col_order: columns,
            rows,
        };
        if self.in_txn() {
            self.buffer(operation);
            return Ok(count);
        }
        let mut txn = TableTxn::new();
        txn.push(operation);
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
    /// A transaction belongs to exactly one authoritative durability domain: graph
    /// and cross-modal operations commit through the graph MutationBatch kernel;
    /// user-table/catalog operations commit through the SQL MutationBatch kernel.
    /// A transaction that mixes those two independent redb authorities is rejected
    /// before either commits, eliminating the former partial-commit window.
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
            let mut ts = self.new_txn_state(&graph).await?;
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

        // Graph store: compile the buffered methods into one authoritative batch.
        // The commit kernel writes durable state before publishing RAM.
        if !node_ops.is_empty() {
            let graph = graph
                .clone()
                .ok_or_else(|| user_err("transaction has node ops but no pinned graph"))?;
            let methods = Self::node_ops_to_methods(&node_ops)?;
            self.commit_graph_methods(&graph, methods).await?;
        }

        // SQL catalog/table store: rows, result, OCC/fence, idempotency, and outbox
        // share one native redb transaction.
        if let Some(txn) = table_txn.filter(|transaction| !transaction.ops.is_empty()) {
            let graph = graph.unwrap_or_else(|| self.current_graph());
            self.commit_table_txn(&graph, "transaction", txn).await?;
        }
        Ok(WireOutcome::TxnEnd { tag: "COMMIT" })
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

    /// Commit graph methods through the universal cross-modal MutationBatch kernel.
    /// The kernel stages against authority, commits graph rows/result/outbox in one
    /// redb transaction, and only then publishes the serving projection.
    async fn commit_graph_methods(
        &self,
        graph: &str,
        methods: Vec<crate::protocol::Method>,
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
        let operation_id = uuid::Uuid::new_v4();
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
                isolation: crate::server::txn::IsolationLevel::Snapshot,
                predicate: None,
                agent: authority.owner_scope().to_string(),
                now_ms: crate::server::txn::now_ms(),
            },
        );
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
    /// MutationBatch kernel. The supplied operation descriptor is hashed by the
    /// compiler and is never stored as plaintext coordinator metadata.
    async fn commit_table_txn(
        &self,
        graph: &str,
        operation: &str,
        txn: TableTxn,
    ) -> WireResult<usize> {
        if txn.ops.is_empty() {
            return Ok(0);
        }
        let authority = self.carrier_authority()?;
        let operation_id = uuid::Uuid::new_v4();
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
                    let mut ts = self.new_txn_state(graph).await?;
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
                        let mut ts = self.new_txn_state(graph).await?;
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
            let mut ts = self.new_txn_state(graph).await?;
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
    #[cfg(feature = "query")]
    async fn new_txn_state(&self, graph: &str) -> WireResult<crate::server::txn::GraphTxnState> {
        let core = self.graph_core(graph).await?;
        let authority = self.carrier_authority()?;
        Ok(crate::server::txn::GraphTxnState::new(
            &core,
            crate::server::txn::NewTxnArgs {
                graph: graph.to_string(),
                tenant_scope: authority.tenant_scope().to_string(),
                begin_version: core.version(),
                isolation: crate::server::txn::IsolationLevel::Snapshot,
                predicate: None,
                agent: authority.owner_scope().to_string(),
                now_ms: crate::server::txn::now_ms(),
            },
        ))
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
        let graph = self.current_graph();

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
                return Ok(WireOutcome::TxnStart);
            }
            StatementKind::Commit => return self.run_commit().await,
            StatementKind::Rollback => {
                // ROLLBACK drops both buffers + the cross-modal staging + the RYOW
                // overlay (nothing was applied in-memory), and always ends the block.
                self.take_txn();
                #[cfg(feature = "query")]
                let _ = self.take_xmodal();
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
        if in_txn && result.is_err() {
            *self.txn_failed.lock() = true;
        }
        result
    }
}
