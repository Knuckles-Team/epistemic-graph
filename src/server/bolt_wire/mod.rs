//! Neo4j Bolt wire-protocol listener (CONCEPT:EG-KG.query.bolt-wire-protocol) — a native Bolt v4.4 server so
//! Neo4j drivers / tools / the `cypher-shell` connect DIRECTLY to the engine's Cypher
//! surface, with NO Neo4j server in the loop.
//!
//! ## What this is (and is NOT)
//! Unlike the SQL wires (pgwire / mysql-wire / mssql-wire, CONCEPT:EG-KG.compute.subsystems-reference) this adapter
//! does NOT drive the shared `WireSession` SQL `classify → dispatch → exec` core — Bolt
//! speaks **Cypher**, not SQL. A `RUN` message's Cypher string is routed straight to the
//! eg-query cypher engine (`exec_cypher_write_params`, the SAME entry-point the native
//! `Method::CypherQuery` handler uses), so a Bolt client runs the EXACT Cypher surface
//! the rest of the engine exposes — no second Cypher planner/executor here.
//!
//! This module is purely the Bolt-SPECIFIC adapter, all hand-rolled (the Pi-contract
//! idiom — links NO third-party bolt/packstream crate):
//!   * the [`packstream`] PackStream v2 codec + Bolt chunked framing,
//!   * the Bolt handshake (`0x6060B017` magic + version negotiation → Bolt 4.4),
//!   * the message state machine (`HELLO` / `LOGON` / `LOGOFF` / `RUN` / `PULL` /
//!     `DISCARD` / `BEGIN` / `COMMIT` / `ROLLBACK` / `RESET` / `GOODBYE`), streaming
//!     `RECORD`s + `SUCCESS`, or `FAILURE` on error,
//!   * mapping a Cypher result (`QueryResult { columns, rows }`, each row a MessagePack
//!     `Vec<serde_json::Value>`) into PackStream `RECORD` structures.
//!
//! ## Protocol subset (CONCEPT:EG-KG.query.bolt-wire-protocol)
//! LANDED: Bolt 4.4 handshake + the request/response messages above, auto-commit
//! (`RUN`+`PULL`) AND explicit transactions (`BEGIN`/`RUN`/`COMMIT`|`ROLLBACK`), the
//! Bolt-5 `LOGON`/`LOGOFF` messages (accepted, minimal), and the FAILED→`RESET` recovery
//! flow (post-failure messages are `IGNORED` until `RESET`). DEFERRED: real
//! authentication (any credentials are accepted — the engine ACL still gates graph
//! access), true transactional rollback of Cypher writes (the cypher engine applies
//! writes eagerly to the live `GraphCore`, so `ROLLBACK` ends the message flow but does
//! not undo already-applied writes), Bolt routing / cluster discovery (`ROUTE`), and the
//! full Bolt 5.x feature set (notifications config, element-id, etc.).

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::RwLock;

use crate::server::ServerState;

pub mod packstream;

use packstream::PackValue;

/// Env var: when set (and the binary is built `--features bolt-wire`), the Bolt listener
/// binds this address (documented Neo4j default loopback `127.0.0.1:7687`). Unset ⇒ no
/// listener.
pub const BOLT_ADDR_ENV: &str = "EPISTEMIC_GRAPH_BOLT_ADDR";
/// Env var: the default graph a fresh Bolt connection runs Cypher against when the
/// HELLO/BEGIN `db` is not supplied. Defaults to `__commons__`.
pub const BOLT_GRAPH_ENV: &str = "EPISTEMIC_GRAPH_BOLT_GRAPH";

/// The 4-byte Bolt handshake magic preamble (`0x6060B017`).
const BOLT_MAGIC: [u8; 4] = [0x60, 0x60, 0xB0, 0x17];
/// The Bolt version this server speaks: 4.4 → wire bytes `00 00 04 04`.
const BOLT_VERSION_4_4: [u8; 4] = [0x00, 0x00, 0x04, 0x04];

// ── Bolt message tags (structure tag byte) ──────────────────────────────────────
const MSG_HELLO: u8 = 0x01;
const MSG_GOODBYE: u8 = 0x02;
const MSG_RESET: u8 = 0x0F;
const MSG_RUN: u8 = 0x10;
const MSG_BEGIN: u8 = 0x11;
const MSG_COMMIT: u8 = 0x12;
const MSG_ROLLBACK: u8 = 0x13;
const MSG_DISCARD: u8 = 0x2F;
const MSG_PULL: u8 = 0x3F;
const MSG_LOGON: u8 = 0x6A;
const MSG_LOGOFF: u8 = 0x6B;

// Server → client response tags.
const MSG_SUCCESS: u8 = 0x70;
const MSG_RECORD: u8 = 0x71;
const MSG_IGNORED: u8 = 0x7E;
const MSG_FAILURE: u8 = 0x7F;

/// The advertised server-agent string (Bolt clients parse this in HELLO SUCCESS).
const SERVER_AGENT: &str = "Neo4j/4.4.0-epistemic-graph";

static CONN_ID: AtomicU64 = AtomicU64::new(1);

fn next_conn_id() -> u64 {
    CONN_ID.fetch_add(1, Ordering::Relaxed)
}

/// A buffered, ready-to-stream Cypher result (its column names + the per-row cell values
/// already decoded into PackStream form). Held between a `RUN`'s SUCCESS and the `PULL`
/// that drains it.
struct PendingResult {
    fields: Vec<String>,
    records: Vec<Vec<PackValue>>,
    /// `"r"` (read) or `"w"` (write) — reported in the final SUCCESS `type` metadata.
    query_type: &'static str,
}

/// Per-connection Bolt session state (CONCEPT:EG-KG.query.bolt-wire-protocol).
struct BoltSession {
    state: Arc<RwLock<ServerState>>,
    /// The graph this connection runs Cypher against (default, or HELLO/BEGIN `db`).
    graph: String,
    /// The authenticated principal (from HELLO/LOGON `principal`), for the engine ACL.
    actor: Option<String>,
    /// FAILED state: after a FAILURE, every message except RESET/GOODBYE is IGNORED.
    failed: bool,
    /// Whether an explicit transaction is open (BEGIN … COMMIT/ROLLBACK).
    in_txn: bool,
    /// A RUN result awaiting its PULL/DISCARD.
    pending: Option<PendingResult>,
}

impl BoltSession {
    fn new(state: Arc<RwLock<ServerState>>, default_graph: String) -> Self {
        Self {
            state,
            graph: default_graph,
            actor: None,
            failed: false,
            in_txn: false,
            pending: None,
        }
    }
}

// ── error currency ───────────────────────────────────────────────────────────────

/// A Bolt failure: a Neo4j status `code` + human `message`, encoded into a FAILURE
/// structure. Client status codes follow the `Neo.ClientError.*` namespace.
struct BoltFailure {
    code: String,
    message: String,
}

impl BoltFailure {
    fn client(code: &str, message: impl Into<String>) -> Self {
        Self {
            code: code.to_string(),
            message: message.into(),
        }
    }
}

// ── PackStream / JSON bridge ───────────────────────────────────────────────────

/// Map a decoded PackStream value to a `serde_json::Value` (RUN parameter binding).
fn pack_to_json(v: &PackValue) -> serde_json::Value {
    use serde_json::Value as J;
    match v {
        PackValue::Null => J::Null,
        PackValue::Bool(b) => J::Bool(*b),
        PackValue::Int(i) => J::Number((*i).into()),
        PackValue::Float(f) => serde_json::Number::from_f64(*f)
            .map(J::Number)
            .unwrap_or(J::Null),
        PackValue::String(s) => J::String(s.clone()),
        PackValue::Bytes(b) => J::Array(b.iter().map(|x| J::Number((*x as i64).into())).collect()),
        PackValue::List(items) => J::Array(items.iter().map(pack_to_json).collect()),
        PackValue::Map(pairs) => {
            let mut m = serde_json::Map::new();
            for (k, val) in pairs {
                m.insert(k.clone(), pack_to_json(val));
            }
            J::Object(m)
        }
        PackValue::Structure { .. } => J::Null,
    }
}

/// Map a `serde_json::Value` (a Cypher result cell) into a PackStream value (RECORD).
fn json_to_pack(v: &serde_json::Value) -> PackValue {
    use serde_json::Value as J;
    match v {
        J::Null => PackValue::Null,
        J::Bool(b) => PackValue::Bool(*b),
        J::Number(n) => {
            if let Some(i) = n.as_i64() {
                PackValue::Int(i)
            } else if let Some(u) = n.as_u64() {
                // A u64 beyond i64 range degrades to float (Bolt Int is signed 64-bit).
                i64::try_from(u)
                    .map(PackValue::Int)
                    .unwrap_or_else(|_| PackValue::Float(u as f64))
            } else {
                PackValue::Float(n.as_f64().unwrap_or(0.0))
            }
        }
        J::String(s) => PackValue::String(s.clone()),
        J::Array(items) => PackValue::List(items.iter().map(json_to_pack).collect()),
        J::Object(map) => PackValue::Map(
            map.iter()
                .map(|(k, val)| (k.clone(), json_to_pack(val)))
                .collect(),
        ),
    }
}

/// Convert a Bolt params map into the cypher engine's `Params` (`serde_json::Map`).
fn params_to_cypher(
    params: &HashMap<String, PackValue>,
) -> serde_json::Map<String, serde_json::Value> {
    params
        .iter()
        .map(|(k, v)| (k.clone(), pack_to_json(v)))
        .collect()
}

// ── response encoders ────────────────────────────────────────────────────────────

/// A SUCCESS structure with `metadata`.
fn success(metadata: Vec<(&str, PackValue)>) -> PackValue {
    PackValue::Structure {
        tag: MSG_SUCCESS,
        fields: vec![PackValue::map(metadata)],
    }
}

/// A FAILURE structure carrying `{code, message}`.
fn failure(f: &BoltFailure) -> PackValue {
    PackValue::Structure {
        tag: MSG_FAILURE,
        fields: vec![PackValue::map(vec![
            ("code", PackValue::String(f.code.clone())),
            ("message", PackValue::String(f.message.clone())),
        ])],
    }
}

/// A RECORD structure wrapping one row's cells.
fn record(cells: Vec<PackValue>) -> PackValue {
    PackValue::Structure {
        tag: MSG_RECORD,
        fields: vec![PackValue::List(cells)],
    }
}

/// An IGNORED structure (sent in the FAILED state until RESET).
fn ignored() -> PackValue {
    PackValue::Structure {
        tag: MSG_IGNORED,
        fields: vec![],
    }
}

// ── Cypher routing ─────────────────────────────────────────────────────────────

/// Run `cypher` (with `params`) against this connection's current graph via the eg-query
/// cypher engine, materializing a [`PendingResult`] ready to stream (CONCEPT:EG-KG.query.bolt-wire-protocol).
async fn run_cypher(
    session: &BoltSession,
    cypher: &str,
    params: &HashMap<String, PackValue>,
) -> Result<PendingResult, BoltFailure> {
    // Resolve the target graph's live core.
    let core = {
        let s = session.state.read().await;
        match s.registry.get(&session.graph) {
            Some(e) => e.core.clone(),
            None => {
                return Err(BoltFailure::client(
                    "Neo.ClientError.Database.DatabaseNotFound",
                    format!("graph '{}' not found", session.graph),
                ))
            }
        }
    };

    // Engine ACL enforcement (CONCEPT:EG-KG.query.concept-13): mirror the SQL wires' check — while no
    // identities are registered the layer allows everything (single-tenant/trust), else
    // the authenticated actor is checked at the statement's read/write level.
    let is_write = crate::server::access::cypher_is_write(cypher);
    {
        let s = session.state.read().await;
        if s.isolation.has_rules() {
            if let Some(e) = s.registry.get(&session.graph) {
                let access = if is_write {
                    crate::isolation::AccessLevel::Write
                } else {
                    crate::isolation::AccessLevel::Read
                };
                let agent = session.actor.as_deref().unwrap_or("");
                if !s.isolation.check_access(
                    agent,
                    &session.graph,
                    e.graph_type,
                    e.owner.as_deref(),
                    access,
                ) {
                    crate::metrics::access_denied();
                    return Err(BoltFailure::client(
                        "Neo.ClientError.Security.Forbidden",
                        format!("permission denied for graph '{}'", session.graph),
                    ));
                }
            }
        }
    }

    // Route to the cypher engine on the blocking pool (it is synchronous + dep-free, and
    // a read takes an off-lock snapshot exactly like `Method::CypherQuery`).
    let cypher_owned = cypher.to_string();
    let cy_params = params_to_cypher(params);
    let result = tokio::task::spawn_blocking(move || {
        eg_query::exec_cypher_write_params(&core, &cypher_owned, &cy_params)
    })
    .await
    .map_err(|e| {
        BoltFailure::client(
            "Neo.DatabaseError.General.UnknownError",
            format!("cypher task failed: {e}"),
        )
    })?
    .map_err(|msg| BoltFailure::client("Neo.ClientError.Statement.SyntaxError", msg))?;

    // A `QueryResult` row is a MessagePack `Vec<serde_json::Value>` aligned to `columns`.
    let mut records = Vec::with_capacity(result.rows.len());
    for blob in &result.rows {
        let cells: Vec<serde_json::Value> = rmp_serde::from_slice(blob).map_err(|e| {
            BoltFailure::client(
                "Neo.DatabaseError.General.UnknownError",
                format!("decode row: {e}"),
            )
        })?;
        records.push(cells.iter().map(json_to_pack).collect());
    }
    Ok(PendingResult {
        fields: result.columns,
        records,
        query_type: if is_write { "w" } else { "r" },
    })
}

// ── the per-connection protocol driver ──────────────────────────────────────────

/// Drive ONE Bolt connection: the handshake, then the message loop until GOODBYE or the
/// socket closes. Generic over the byte stream so an in-process test can drive it over
/// any duplex transport (CONCEPT:EG-KG.query.bolt-wire-protocol).
async fn handle_connection<S>(s: &mut S, mut session: BoltSession) -> std::io::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    // ── handshake: 4-byte magic + four 4-byte version proposals ─────────────────
    let mut magic = [0u8; 4];
    s.read_exact(&mut magic).await?;
    if magic != BOLT_MAGIC {
        return Ok(()); // not a Bolt client — drop.
    }
    let mut proposals = [0u8; 16];
    s.read_exact(&mut proposals).await?;
    // We speak Bolt 4.4. Accept if the client proposed a 4.x version (major byte == 4);
    // otherwise still answer 4.4 (the driver will disconnect if it can't speak it).
    let mut chosen = BOLT_VERSION_4_4;
    let client_supports_4 = proposals.chunks(4).any(|v| v[3] == 4);
    if !client_supports_4 {
        // No overlap we can guarantee — reply 4.4 anyway (best-effort single-version offer).
        chosen = BOLT_VERSION_4_4;
    }
    s.write_all(&chosen).await?;
    s.flush().await?;

    // ── message loop ────────────────────────────────────────────────────────────
    loop {
        let body = match read_message(s).await {
            Ok(Some(b)) => b,
            Ok(None) => break, // clean EOF
            Err(_) => break,   // socket error / client gone
        };
        let msg = match packstream::decode(&body) {
            Ok(PackValue::Structure { tag, fields }) => (tag, fields),
            _ => {
                // Malformed frame → FAILURE + enter FAILED state.
                let f = BoltFailure::client(
                    "Neo.ClientError.Request.Invalid",
                    "malformed Bolt message (expected a structure)",
                );
                write_msg(s, &failure(&f)).await?;
                session.failed = true;
                continue;
            }
        };
        let (tag, fields) = msg;

        // GOODBYE always closes, even in FAILED state (no response).
        if tag == MSG_GOODBYE {
            break;
        }
        // RESET clears FAILED state + any pending result + open txn.
        if tag == MSG_RESET {
            session.failed = false;
            session.pending = None;
            session.in_txn = false;
            write_msg(s, &success(vec![])).await?;
            continue;
        }
        // In FAILED state, every other message is IGNORED until RESET.
        if session.failed {
            write_msg(s, &ignored()).await?;
            continue;
        }

        match tag {
            MSG_HELLO => {
                // extra map may carry `principal` (auth) + `db`/routing hints.
                if let Some(PackValue::Map(_)) = fields.first() {
                    let extra = fields[0].clone().into_map();
                    if let Some(p) = extra.get("principal").and_then(|v| v.as_str()) {
                        session.actor = Some(p.to_string());
                    }
                }
                let meta = vec![
                    ("server", PackValue::String(SERVER_AGENT.to_string())),
                    (
                        "connection_id",
                        PackValue::String(format!("bolt-{}", next_conn_id())),
                    ),
                ];
                write_msg(s, &success(meta)).await?;
            }
            MSG_LOGON => {
                // Bolt 5 explicit auth — accept any credentials (real auth is deferred;
                // the engine ACL still gates graph access via the actor).
                if let Some(PackValue::Map(_)) = fields.first() {
                    let extra = fields[0].clone().into_map();
                    if let Some(p) = extra.get("principal").and_then(|v| v.as_str()) {
                        session.actor = Some(p.to_string());
                    }
                }
                write_msg(s, &success(vec![])).await?;
            }
            MSG_LOGOFF => {
                session.actor = None;
                write_msg(s, &success(vec![])).await?;
            }
            MSG_BEGIN => {
                // Honor an explicit `db` selection in the BEGIN extra map.
                if let Some(PackValue::Map(_)) = fields.first() {
                    let extra = fields[0].clone().into_map();
                    if let Some(db) = extra.get("db").and_then(|v| v.as_str()) {
                        session.graph = db.to_string();
                    }
                }
                session.in_txn = true;
                write_msg(s, &success(vec![])).await?;
            }
            MSG_COMMIT => {
                // Cypher writes are applied eagerly to the live GraphCore, so COMMIT just
                // ends the txn flow (see module header — true rollback is deferred).
                session.in_txn = false;
                write_msg(
                    s,
                    &success(vec![(
                        "bookmark",
                        PackValue::String("eg:bookmark:0".to_string()),
                    )]),
                )
                .await?;
            }
            MSG_ROLLBACK => {
                session.in_txn = false;
                write_msg(s, &success(vec![])).await?;
            }
            MSG_RUN => {
                // fields: [query: String, params: Map, extra: Map]
                let query = fields
                    .first()
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                let params = fields
                    .get(1)
                    .cloned()
                    .map(|v| v.into_map())
                    .unwrap_or_default();
                // An auto-commit RUN may carry a `db` in its extra map.
                if let Some(extra) = fields.get(2) {
                    if let Some(db) = extra.get("db").and_then(|v| v.as_str()) {
                        session.graph = db.to_string();
                    }
                }
                match run_cypher(&session, &query, &params).await {
                    Ok(pending) => {
                        let fields_meta = PackValue::List(
                            pending
                                .fields
                                .iter()
                                .map(|f| PackValue::String(f.clone()))
                                .collect(),
                        );
                        session.pending = Some(pending);
                        write_msg(s, &success(vec![("fields", fields_meta)])).await?;
                    }
                    Err(f) => {
                        write_msg(s, &failure(&f)).await?;
                        session.failed = true;
                    }
                }
            }
            MSG_PULL | MSG_DISCARD => {
                // extra: {n: i64, qid: i64}. n == -1 means "all". DISCARD drops records.
                let n = fields
                    .first()
                    .and_then(|v| v.get("n"))
                    .and_then(|v| v.as_int())
                    .unwrap_or(-1);
                match session.pending.take() {
                    Some(pending) => {
                        if tag == MSG_PULL {
                            let limit = if n < 0 {
                                pending.records.len()
                            } else {
                                (n as usize).min(pending.records.len())
                            };
                            for cells in pending.records.into_iter().take(limit) {
                                write_msg(s, &record(cells)).await?;
                            }
                        }
                        write_msg(
                            s,
                            &success(vec![
                                ("type", PackValue::String(pending.query_type.to_string())),
                                ("t_last", PackValue::Int(0)),
                            ]),
                        )
                        .await?;
                    }
                    None => {
                        // No streamed result open — a benign empty SUCCESS.
                        write_msg(s, &success(vec![])).await?;
                    }
                }
            }
            other => {
                let f = BoltFailure::client(
                    "Neo.ClientError.Request.Invalid",
                    format!("unsupported Bolt message tag {other:#04x}"),
                );
                write_msg(s, &failure(&f)).await?;
                session.failed = true;
            }
        }
    }
    Ok(())
}

/// Read ONE complete chunked Bolt message off the stream (CONCEPT:EG-KG.query.bolt-wire-protocol): read
/// length-prefixed chunks, appending bodies, until a zero-length chunk ends the message.
/// Returns `Ok(None)` on a clean EOF before any chunk.
async fn read_message<S: AsyncRead + Unpin>(s: &mut S) -> std::io::Result<Option<Vec<u8>>> {
    let mut body = Vec::new();
    let mut first = true;
    loop {
        let mut hdr = [0u8; 2];
        match s.read_exact(&mut hdr).await {
            Ok(_) => {}
            Err(e) if first && e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(e) => return Err(e),
        }
        first = false;
        let len = u16::from_be_bytes(hdr) as usize;
        if len == 0 {
            break; // end-of-message
        }
        let start = body.len();
        body.resize(start + len, 0);
        s.read_exact(&mut body[start..]).await?;
    }
    Ok(Some(body))
}

/// Encode + chunk-frame `msg` and write it to the stream.
async fn write_msg<S: AsyncWrite + Unpin>(s: &mut S, msg: &PackValue) -> std::io::Result<()> {
    let framed = packstream::encode_chunked(msg);
    s.write_all(&framed).await?;
    s.flush().await
}

// ── the listener ────────────────────────────────────────────────────────────────

/// Bind `addr` and serve Bolt connections until the process exits (CONCEPT:EG-KG.query.bolt-wire-protocol).
/// Spawned by `main.rs` only when built `--features bolt-wire` AND
/// `EPISTEMIC_GRAPH_BOLT_ADDR` is set. Each connection gets a fresh [`BoltSession`] so its
/// graph selection / actor / txn stay isolated (mirrors the SQL wires' per-connection state).
pub async fn serve(addr: &str, state: Arc<RwLock<ServerState>>) -> std::io::Result<()> {
    let default_graph = std::env::var(BOLT_GRAPH_ENV).unwrap_or_else(|_| "__commons__".to_string());
    let listener = TcpListener::bind(addr).await?;
    tracing::info!(
        "bolt-wire: serving Neo4j Bolt v4.4 wire protocol on {} (default graph '{}')",
        addr,
        default_graph
    );
    loop {
        let (socket, peer) = listener.accept().await?;
        let session = BoltSession::new(state.clone(), default_graph.clone());
        tokio::spawn(async move {
            let mut socket = socket;
            if let Err(e) = handle_connection(&mut socket, session).await {
                tracing::debug!("bolt-wire connection from {peer} ended: {e}");
            }
        });
    }
}

#[cfg(test)]
mod tests {
    //! Message-level Bolt driver tests (CONCEPT:EG-KG.query.bolt-wire-protocol): an in-process duplex stream
    //! drives the real `handle_connection` through the handshake + a full HELLO → RUN →
    //! PULL → SUCCESS auto-commit round-trip against the Cypher engine, plus BEGIN/COMMIT
    //! and FAILURE/RESET flows — no external Neo4j driver.
    use super::*;
    use crate::channels::ChannelManager;
    use crate::isolation::IsolationLayer;
    use crate::registry::GraphRegistry;
    use crate::server::ServerState;
    use dashmap::DashMap;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::sync::Semaphore;

    /// A `ServerState` seeded with three nodes so a Cypher `MATCH (n) RETURN n.id` returns
    /// rows over the default `__commons__` graph.
    fn seeded_state() -> Arc<RwLock<ServerState>> {
        let registry = GraphRegistry::new();
        {
            let core = registry.get("__commons__").unwrap().core.clone();
            for (id, ty) in [("n1", "Agent"), ("n2", "Agent"), ("n3", "Tool")] {
                let blob =
                    rmp_serde::to_vec_named(&serde_json::json!({"type": ty, "id": id})).unwrap();
                core.add_node(id.to_string(), blob);
            }
        }
        Arc::new(RwLock::new(ServerState {
            #[cfg(feature = "redb")]
            cold_tracker: std::sync::Arc::new(
                crate::server::persistence::cold_offload::ColdTenantTracker::new(),
            ),
            registry,
            isolation: IsolationLayer::new(),
            channels: ChannelManager::new(),
            auth_secret: "test".to_string(),
            #[cfg(feature = "kv")]
            kv: None,
            persist_dir: None,
            persistence: None,
            redb_authoritative: false,
            max_in_flight: Arc::new(Semaphore::new(16)),
            read_admission: Arc::new(Semaphore::new(16)),
            per_graph_inflight: Arc::new(DashMap::new()),
            per_graph_inflight_limit: 8,
            write_coalescer: Arc::new(crate::write_coalescer::WriteCoalescerRegistry::from_env()),
            open_txns: Arc::new(DashMap::new()),
            txn_id_gen: Arc::new(crate::server::txn::TxnIdGen::default()),
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
            tsdb_store: None,
            #[cfg(feature = "rdf-redb")]
            rdf_quads: None,
            #[cfg(feature = "streaming")]
            cdc: Some(Arc::new(crate::server::cdc::CdcHub::new())),
            #[cfg(feature = "wasm-udf")]
            udf_registry: Arc::new(eg_wasm::UdfRegistry::new()),
            #[cfg(feature = "compute-dist")]
            matviews: Arc::new(parking_lot::Mutex::new(
                crate::raft::pregel::MatViewStore::new(),
            )),
            #[cfg(feature = "federation")]
            foreign_sources: Arc::new(DashMap::new()),
            #[cfg(feature = "dataset-handle")]
            dataset_handles: Arc::new(
                crate::server::dataset_handle::DatasetHandleRegistry::new(),
            ),
        }))
    }

    /// A test client half of a `tokio::io::duplex` pair, with helpers to complete the
    /// handshake and send/recv chunked Bolt messages.
    struct TestClient {
        stream: tokio::io::DuplexStream,
    }

    impl TestClient {
        async fn handshake(&mut self) {
            self.stream.write_all(&BOLT_MAGIC).await.unwrap();
            // Propose 4.4 then zeros.
            let mut proposals = [0u8; 16];
            proposals[0..4].copy_from_slice(&BOLT_VERSION_4_4);
            self.stream.write_all(&proposals).await.unwrap();
            self.stream.flush().await.unwrap();
            let mut chosen = [0u8; 4];
            self.stream.read_exact(&mut chosen).await.unwrap();
            assert_eq!(chosen, BOLT_VERSION_4_4, "server negotiates Bolt 4.4");
        }

        async fn send(&mut self, msg: &PackValue) {
            let framed = packstream::encode_chunked(msg);
            self.stream.write_all(&framed).await.unwrap();
            self.stream.flush().await.unwrap();
        }

        async fn recv(&mut self) -> PackValue {
            let body = read_message(&mut self.stream).await.unwrap().unwrap();
            packstream::decode(&body).unwrap()
        }

        fn tag_of(v: &PackValue) -> u8 {
            match v {
                PackValue::Structure { tag, .. } => *tag,
                _ => panic!("expected a structure, got {v:?}"),
            }
        }
    }

    fn hello() -> PackValue {
        PackValue::Structure {
            tag: MSG_HELLO,
            fields: vec![PackValue::map(vec![(
                "user_agent",
                PackValue::String("test/1.0".into()),
            )])],
        }
    }

    fn run(query: &str) -> PackValue {
        PackValue::Structure {
            tag: MSG_RUN,
            fields: vec![
                PackValue::String(query.into()),
                PackValue::map(vec![]),
                PackValue::map(vec![]),
            ],
        }
    }

    fn pull_all() -> PackValue {
        PackValue::Structure {
            tag: MSG_PULL,
            fields: vec![PackValue::map(vec![("n", PackValue::Int(-1))])],
        }
    }

    /// Spawn `handle_connection` over an in-process duplex and return the client half.
    fn spawn_conn(state: Arc<RwLock<ServerState>>) -> TestClient {
        let (client, server) = tokio::io::duplex(64 * 1024);
        let session = BoltSession::new(state, "__commons__".to_string());
        tokio::spawn(async move {
            let mut server = server;
            let _ = handle_connection(&mut server, session).await;
        });
        TestClient { stream: client }
    }

    #[tokio::test]
    async fn bolt_hello_run_pull_roundtrip_against_cypher_engine() {
        let mut c = spawn_conn(seeded_state());
        c.handshake().await;

        // HELLO → SUCCESS with a server agent.
        c.send(&hello()).await;
        let hello_ok = c.recv().await;
        assert_eq!(TestClient::tag_of(&hello_ok), MSG_SUCCESS);
        assert!(hello_ok.get("metadata").is_none() /* metadata is the map itself */);

        // RUN a read Cypher → SUCCESS carrying the `fields` (column names).
        c.send(&run("MATCH (n) RETURN n.id AS id")).await;
        let run_ok = c.recv().await;
        assert_eq!(TestClient::tag_of(&run_ok), MSG_SUCCESS);

        // PULL all → three RECORDs then a final SUCCESS with type "r".
        c.send(&pull_all()).await;
        let mut records = 0;
        loop {
            let m = c.recv().await;
            match TestClient::tag_of(&m) {
                MSG_RECORD => records += 1,
                MSG_SUCCESS => break,
                other => panic!("unexpected tag {other:#04x} during PULL"),
            }
        }
        assert_eq!(records, 3, "three seeded nodes stream back as RECORDs");
    }

    #[tokio::test]
    async fn bolt_explicit_transaction_begin_run_commit() {
        let mut c = spawn_conn(seeded_state());
        c.handshake().await;
        c.send(&hello()).await;
        assert_eq!(TestClient::tag_of(&c.recv().await), MSG_SUCCESS);

        // BEGIN → SUCCESS
        c.send(&PackValue::Structure {
            tag: MSG_BEGIN,
            fields: vec![PackValue::map(vec![])],
        })
        .await;
        assert_eq!(TestClient::tag_of(&c.recv().await), MSG_SUCCESS);

        // RUN + PULL inside the txn.
        c.send(&run("MATCH (n) RETURN n.id AS id")).await;
        assert_eq!(TestClient::tag_of(&c.recv().await), MSG_SUCCESS);
        c.send(&pull_all()).await;
        loop {
            if TestClient::tag_of(&c.recv().await) == MSG_SUCCESS {
                break;
            }
        }

        // COMMIT → SUCCESS (with a bookmark).
        c.send(&PackValue::Structure {
            tag: MSG_COMMIT,
            fields: vec![],
        })
        .await;
        assert_eq!(TestClient::tag_of(&c.recv().await), MSG_SUCCESS);
    }

    #[tokio::test]
    async fn bolt_failure_then_ignored_until_reset() {
        let mut c = spawn_conn(seeded_state());
        c.handshake().await;
        c.send(&hello()).await;
        assert_eq!(TestClient::tag_of(&c.recv().await), MSG_SUCCESS);

        // A syntactically invalid Cypher → FAILURE.
        c.send(&run("THIS IS NOT CYPHER")).await;
        assert_eq!(TestClient::tag_of(&c.recv().await), MSG_FAILURE);

        // A following RUN is IGNORED until RESET.
        c.send(&run("MATCH (n) RETURN n")).await;
        assert_eq!(TestClient::tag_of(&c.recv().await), MSG_IGNORED);

        // RESET clears the FAILED state → SUCCESS, and queries work again.
        c.send(&PackValue::Structure {
            tag: MSG_RESET,
            fields: vec![],
        })
        .await;
        assert_eq!(TestClient::tag_of(&c.recv().await), MSG_SUCCESS);
        c.send(&run("MATCH (n) RETURN n.id AS id")).await;
        assert_eq!(TestClient::tag_of(&c.recv().await), MSG_SUCCESS);
    }
}
