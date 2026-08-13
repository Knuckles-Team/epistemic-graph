//! Neo4j Bolt wire-protocol listener (CONCEPT:EG-KG.query.bolt-wire-protocol) — a native Bolt v4.4 server so
//! Neo4j drivers with custom auth-token support connect DIRECTLY to the engine's
//! Cypher surface, with NO Neo4j server in the loop.
//!
//! ## What this is (and is NOT)
//! Unlike the SQL wires (pgwire / mysql-wire / mssql-wire, CONCEPT:EG-KG.compute.subsystems-reference), Bolt
//! speaks **Cypher**, not SQL. `RUN` uses the same eg-query Cypher executor as
//! `Method::CypherQuery`; every write is applied to a detached graph and committed
//! through the common authoritative `MutationBatch` gateway before acknowledgement.
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
//! Bolt-5 `LOGON`/`LOGOFF` messages, current `eg2.` signed-session authentication,
//! atomic explicit transactions with rollback-by-discard, and the FAILED→`RESET`
//! recovery flow (post-failure messages are `IGNORED` until `RESET`). There is no
//! anonymous, actor-only, password-derived, or unsigned graph-selection path.
//! Bolt routing / cluster discovery (`ROUTE`) and the full Bolt 5.x feature set
//! (notifications config, element-id, etc.) are not implemented.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::RwLock;

use crate::graph::{GraphCore, GraphSnapshot};
use crate::protocol::{Method, QueryResult, Response, ResultPayload};
use crate::server::access::{check_graph_access, CarrierAuthority, GraphReadAuthority};
use crate::server::auth::{verify_request_with_security_dir, VerifiedRequestContext};
use crate::server::mutation::{MutationCtx, MutationPlan};
use crate::server::ServerState;

mod auth;
pub mod packstream;

pub use auth::{encode_session_credential, BOLT_AUTH_SCHEME};

use packstream::PackValue;

/// Env var: when set (and the binary is built `--features bolt-wire`), the Bolt listener
/// binds this address (documented Neo4j default loopback `127.0.0.1:7687`). Unset ⇒ no
/// listener.
pub const BOLT_ADDR_ENV: &str = "EPISTEMIC_GRAPH_BOLT_ADDR";
/// Fail-closed startup policy for the direct Bolt listener. Bolt does not
/// terminate TLS, so it is a loopback backend only. The non-empty engine key is
/// used solely by the shared current-envelope verifier.
pub fn validate_startup_policy(addr: &str, auth_secret: &str) -> std::io::Result<()> {
    crate::server::validate_direct_wire_security(addr, "bolt-wire", !auth_secret.is_empty())
}

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

#[derive(Clone)]
struct BufferedCypher {
    query: String,
    params: eg_query::Params,
}

struct BoltTransaction {
    base_snapshot: GraphSnapshot,
    base_version: u64,
    writes: Vec<BufferedCypher>,
}

/// Per-connection Bolt session state (CONCEPT:EG-KG.query.bolt-wire-protocol).
struct BoltSession {
    state: Arc<RwLock<ServerState>>,
    /// Graph cryptographically bound by the current signed session request.
    graph: Option<String>,
    /// Authority produced only by the shared `eg2.` verifier.
    authority: Option<VerifiedRequestContext>,
    /// Privacy-safe request-id seed from the verified session idempotency key.
    request_seed: Option<String>,
    request_sequence: u64,
    /// FAILED state: after a FAILURE, every message except RESET/GOODBYE is IGNORED.
    failed: bool,
    /// Detached explicit transaction. Its writes never touch the live graph
    /// before the single authoritative COMMIT barrier.
    transaction: Option<BoltTransaction>,
    /// A RUN result awaiting its PULL/DISCARD.
    pending: Option<PendingResult>,
}

impl BoltSession {
    fn new(state: Arc<RwLock<ServerState>>) -> Self {
        Self {
            state,
            graph: None,
            authority: None,
            request_seed: None,
            request_sequence: 0,
            failed: false,
            transaction: None,
            pending: None,
        }
    }

    fn next_request_id(&mut self) -> Result<u64, BoltFailure> {
        let seed = self.request_seed.as_deref().ok_or_else(|| {
            BoltFailure::client(
                "Neo.ClientError.Security.Unauthorized",
                "authentication required",
            )
        })?;
        self.request_sequence = self.request_sequence.saturating_add(1);
        use sha2::{Digest, Sha256};
        let mut digest = Sha256::new();
        digest.update(b"bolt-request-id\0");
        digest.update(seed.as_bytes());
        digest.update(self.request_sequence.to_be_bytes());
        let bytes = digest.finalize();
        Ok(u64::from_be_bytes(
            bytes[..8].try_into().expect("SHA-256 prefix"),
        ))
    }
}

// ── error currency ───────────────────────────────────────────────────────────────

/// A Bolt failure: a Neo4j status `code` + human `message`, encoded into a FAILURE
/// structure. Client status codes follow the `Neo.ClientError.*` namespace.
#[derive(Debug)]
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

/// Verify a current signed session request and derive its graph authority.
async fn authenticate_session(
    state: &Arc<RwLock<ServerState>>,
    extra: &HashMap<String, PackValue>,
) -> Result<(VerifiedRequestContext, String, String), BoltFailure> {
    let scheme = extra.get("scheme").and_then(PackValue::as_str);
    let credential = extra.get("credentials").and_then(PackValue::as_str);
    if scheme != Some(BOLT_AUTH_SCHEME) || credential.is_none() {
        return Err(BoltFailure::client(
            "Neo.ClientError.Security.Unauthorized",
            "authentication failed",
        ));
    }
    let request =
        auth::decode_session_credential(credential.unwrap_or_default()).map_err(|_| {
            BoltFailure::client(
                "Neo.ClientError.Security.Unauthorized",
                "authentication failed",
            )
        })?;
    if !matches!(request.method, Method::Health) || request.graph.trim().is_empty() {
        return Err(BoltFailure::client(
            "Neo.ClientError.Security.Unauthorized",
            "authentication failed",
        ));
    }
    let (secret, persist_dir) = {
        let state = state.read().await;
        (state.auth_secret.clone(), state.persist_dir.clone())
    };
    let verified = verify_request_with_security_dir(&secret, &request, persist_dir.as_deref())
        .map_err(|_| {
            BoltFailure::client(
                "Neo.ClientError.Security.Unauthorized",
                "authentication failed",
            )
        })?;
    CarrierAuthority::from_verified(&verified).map_err(|_| {
        BoltFailure::client(
            "Neo.ClientError.Security.Unauthorized",
            "authentication failed",
        )
    })?;
    let probe = Method::CypherQuery {
        query: "MATCH (n) RETURN n LIMIT 0".to_string(),
        mode: crate::protocol::CypherMode::Read,
    };
    let action = eg_capabilities::policy(&probe).authz_action;
    if !verified.allows_method(action, false) && !verified.allows_method(action, true) {
        return Err(BoltFailure::client(
            "Neo.ClientError.Security.Forbidden",
            "Cypher scope is required",
        ));
    }
    let request_seed = verified.idempotency_key().to_string();
    Ok((verified, request.graph, request_seed))
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

struct BoltGraphContext {
    core: Arc<GraphCore>,
    materialization_manifest:
        Option<Arc<std::sync::RwLock<crate::registry::MaterializationManifest>>>,
    graph_type: crate::protocol::GraphType,
    owner: Option<String>,
    isolation: crate::isolation::IsolationLayer,
    persistence: Option<Arc<dyn crate::server::persistence::PersistenceBackend>>,
    #[cfg(feature = "streaming")]
    cdc: Option<Arc<crate::server::cdc::CdcHub>>,
}

async fn graph_context(session: &BoltSession) -> Result<(String, BoltGraphContext), BoltFailure> {
    let graph = session.graph.clone().ok_or_else(|| {
        BoltFailure::client(
            "Neo.ClientError.Security.Unauthorized",
            "authentication required",
        )
    })?;
    let state = session.state.read().await;
    let entry = state.registry.get(&graph).ok_or_else(|| {
        BoltFailure::client(
            "Neo.ClientError.Database.DatabaseNotFound",
            "signed graph is not available",
        )
    })?;
    let materialization_manifest = state.registry.materialization_handle(&graph);
    Ok((
        graph,
        BoltGraphContext {
            core: entry.core.clone(),
            materialization_manifest,
            graph_type: entry.graph_type,
            owner: entry.owner.clone(),
            isolation: state.isolation.clone(),
            persistence: state.persistence.clone(),
            #[cfg(feature = "streaming")]
            cdc: state.cdc.clone(),
        },
    ))
}

fn authorize_cypher(
    verified: &VerifiedRequestContext,
    graph: &str,
    context: &BoltGraphContext,
    is_write: bool,
) -> Result<(CarrierAuthority, GraphReadAuthority), BoltFailure> {
    let method = Method::CypherQuery {
        query: "BOLT CYPHER AUTHORIZATION".to_string(),
        mode: if is_write {
            crate::protocol::CypherMode::Write
        } else {
            crate::protocol::CypherMode::Read
        },
    };
    let action = eg_capabilities::policy(&method).authz_action;
    if !verified.allows_method(action, is_write) {
        crate::metrics::access_denied();
        return Err(BoltFailure::client(
            "Neo.ClientError.Security.Forbidden",
            "Cypher scope is not authorized",
        ));
    }
    let level = if is_write {
        crate::isolation::AccessLevel::Write
    } else {
        crate::isolation::AccessLevel::Read
    };
    check_graph_access(
        &context.isolation,
        Some(verified.agent_id()),
        graph,
        context.graph_type,
        context.owner.as_deref(),
        level,
    )
    .map_err(|_| {
        BoltFailure::client(
            "Neo.ClientError.Security.Forbidden",
            "signed authority cannot access the graph",
        )
    })?;
    let carrier = CarrierAuthority::from_verified(verified).map_err(|_| {
        BoltFailure::client(
            "Neo.ClientError.Security.Forbidden",
            "signed carrier authority is invalid",
        )
    })?;
    let read = GraphReadAuthority::from_verified(verified, &context.isolation).map_err(|_| {
        BoltFailure::client(
            "Neo.ClientError.Security.Forbidden",
            "signed row authority is invalid",
        )
    })?;
    Ok((carrier, read))
}

fn query_result_to_pending(
    result: QueryResult,
    query_type: &'static str,
) -> Result<PendingResult, BoltFailure> {
    let mut records = Vec::with_capacity(result.rows.len());
    for blob in &result.rows {
        let cells: Vec<serde_json::Value> = eg_types::msgpack::decode_bounded(
            blob,
            eg_types::msgpack::MsgpackLimits::new(
                eg_types::msgpack::MAX_PROPERTY_BYTES,
                eg_types::msgpack::MAX_PROPERTY_ITEMS,
                eg_types::msgpack::DEFAULT_MAX_DEPTH,
            ),
        )
        .map_err(|_| {
            BoltFailure::client(
                "Neo.DatabaseError.General.UnknownError",
                "decode row failed",
            )
        })?;
        records.push(cells.iter().map(json_to_pack).collect());
    }
    Ok(PendingResult {
        fields: result.columns,
        records,
        query_type,
    })
}

fn response_query_result(response: Response) -> Result<QueryResult, BoltFailure> {
    if let Some(error) = response.error {
        return Err(BoltFailure::client(
            "Neo.TransientError.Transaction.Terminated",
            error,
        ));
    }
    let Some(ResultPayload::Raw(bytes)) = response.result else {
        return Err(BoltFailure::client(
            "Neo.DatabaseError.General.UnknownError",
            "Cypher commit returned an invalid result",
        ));
    };
    eg_types::msgpack::decode_bounded(
        &bytes,
        eg_types::msgpack::MsgpackLimits::new(
            64 * 1024 * 1024,
            eg_types::msgpack::MAX_PROPERTY_ITEMS,
            eg_types::msgpack::DEFAULT_MAX_DEPTH,
        ),
    )
    .map_err(|_| {
        BoltFailure::client(
            "Neo.DatabaseError.General.UnknownError",
            "Cypher commit result is corrupt",
        )
    })
}

fn receipt_method(writes: &[BufferedCypher]) -> Method {
    use sha2::{Digest, Sha256};
    let mut digest = Sha256::new();
    digest.update(b"bolt-cypher-transaction\0");
    for write in writes {
        digest.update((write.query.len() as u64).to_be_bytes());
        digest.update(write.query.as_bytes());
        let params = rmp_serde::to_vec_named(&write.params).unwrap_or_default();
        digest.update((params.len() as u64).to_be_bytes());
        digest.update(params);
    }
    Method::CypherQuery {
        query: format!("BOLT CYPHER sha256:{}", hex::encode(digest.finalize())),
        mode: crate::protocol::CypherMode::Write,
    }
}

async fn commit_writes(
    session: &mut BoltSession,
    writes: Vec<BufferedCypher>,
    expected_version: Option<u64>,
) -> Result<QueryResult, BoltFailure> {
    let verified = session.authority.clone().ok_or_else(|| {
        BoltFailure::client(
            "Neo.ClientError.Security.Unauthorized",
            "authentication required",
        )
    })?;
    let request_id = session.next_request_id()?;
    let (graph, graph_context) = graph_context(session).await?;
    let (carrier, _) = authorize_cypher(&verified, &graph, &graph_context, true)?;
    let method = receipt_method(&writes);
    let plan = MutationPlan::for_method(&method);
    let ctx = MutationCtx {
        req_id: request_id,
        caller: Some(verified.agent_id()),
        tenant_scope: carrier.tenant_scope(),
        graph_name: &graph,
        graph_type: graph_context.graph_type,
        owner: graph_context.owner.as_deref(),
        isolation: &graph_context.isolation,
        core: &graph_context.core,
        persistence: graph_context.persistence.as_ref(),
        #[cfg(feature = "streaming")]
        cdc: graph_context.cdc.as_ref(),
        materialization_manifest: graph_context.materialization_manifest.as_ref(),
        write_coalescer: None,
    };
    let response = crate::server::mutation::commit_conditional_mutation_async(
        &ctx,
        &plan,
        &method,
        true,
        move |staged| async move {
            tokio::task::spawn_blocking(move || {
                if expected_version.is_some_and(|expected| staged.version() != expected) {
                    return Err("transaction conflict: graph changed after BEGIN".to_string());
                }
                let mut last = None;
                for write in writes {
                    last = Some(eg_query::exec_cypher_write_params(
                        &staged,
                        &write.query,
                        &write.params,
                    )?);
                }
                let result = last.ok_or_else(|| "transaction has no writes".to_string())?;
                let bytes = rmp_serde::to_vec_named(&result)
                    .map_err(|_| "Cypher result could not be encoded".to_string())?;
                Ok(ResultPayload::Raw(bytes))
            })
            .await
            .map_err(|_| "Cypher commit task failed".to_string())?
        },
    )
    .await;
    response_query_result(response)
}

async fn run_read(
    session: &BoltSession,
    query: String,
    params: eg_query::Params,
) -> Result<QueryResult, BoltFailure> {
    let verified = session.authority.as_ref().ok_or_else(|| {
        BoltFailure::client(
            "Neo.ClientError.Security.Unauthorized",
            "authentication required",
        )
    })?;
    let (graph, context) = graph_context(session).await?;
    let (_, read_authority) = authorize_cypher(verified, &graph, &context, false)?;
    let mut view = context.core.analysis_snapshot();
    read_authority.filter_view(&mut view);
    tokio::task::spawn_blocking(move || eg_query::exec_cypher_params(&view, &query, &params))
        .await
        .map_err(|_| {
            BoltFailure::client(
                "Neo.DatabaseError.General.UnknownError",
                "Cypher read task failed",
            )
        })?
        .map_err(|error| BoltFailure::client("Neo.ClientError.Statement.SyntaxError", error))
}

async fn run_transaction_statement(
    session: &mut BoltSession,
    statement: BufferedCypher,
    is_write: bool,
) -> Result<QueryResult, BoltFailure> {
    let verified = session.authority.as_ref().ok_or_else(|| {
        BoltFailure::client(
            "Neo.ClientError.Security.Unauthorized",
            "authentication required",
        )
    })?;
    let (graph, context) = graph_context(session).await?;
    let (_, read_authority) = authorize_cypher(verified, &graph, &context, is_write)?;
    let transaction = session.transaction.as_ref().ok_or_else(|| {
        BoltFailure::client(
            "Neo.ClientError.Transaction.InvalidBookmark",
            "no explicit transaction is active",
        )
    })?;
    let snapshot = transaction.base_snapshot.clone();
    let version = transaction.base_version;
    let prior = transaction.writes.clone();
    let current = statement.clone();
    let result = tokio::task::spawn_blocking(move || {
        let staged = GraphCore::from_snapshot(snapshot, version)?;
        for write in prior {
            eg_query::exec_cypher_write_params(&staged, &write.query, &write.params)?;
        }
        if is_write {
            eg_query::exec_cypher_write_params(&staged, &current.query, &current.params)
        } else {
            let mut view = staged.analysis_snapshot();
            read_authority.filter_view(&mut view);
            eg_query::exec_cypher_params(&view, &current.query, &current.params)
        }
    })
    .await
    .map_err(|_| {
        BoltFailure::client(
            "Neo.DatabaseError.General.UnknownError",
            "Cypher transaction task failed",
        )
    })?
    .map_err(|error| BoltFailure::client("Neo.ClientError.Statement.SyntaxError", error))?;
    if is_write {
        session
            .transaction
            .as_mut()
            .expect("transaction checked above")
            .writes
            .push(statement);
    }
    Ok(result)
}

async fn begin_transaction(session: &BoltSession) -> Result<BoltTransaction, BoltFailure> {
    let verified = session.authority.as_ref().ok_or_else(|| {
        BoltFailure::client(
            "Neo.ClientError.Security.Unauthorized",
            "authentication required",
        )
    })?;
    let (graph, context) = graph_context(session).await?;
    authorize_cypher(verified, &graph, &context, false)?;
    let _guard = crate::server::mutation_batch::lock_graph(&graph).await;
    Ok(BoltTransaction {
        base_snapshot: context.core.snapshot(),
        base_version: context.core.version(),
        writes: Vec::new(),
    })
}

/// Run `cypher` against the signed graph, materializing a result ready to stream.
async fn run_cypher(
    session: &mut BoltSession,
    cypher: &str,
    params: &HashMap<String, PackValue>,
) -> Result<PendingResult, BoltFailure> {
    let is_write = match eg_query::classify_cypher(cypher) {
        Ok(eg_query::CypherStatementKind::Read) => false,
        Ok(eg_query::CypherStatementKind::Write) => true,
        Err(message) => {
            return Err(BoltFailure::client(
                "Neo.ClientError.Statement.SyntaxError",
                &message,
            ))
        }
    };
    let statement = BufferedCypher {
        query: cypher.to_string(),
        params: params_to_cypher(params),
    };
    let result = if session.transaction.is_some() {
        run_transaction_statement(session, statement, is_write).await?
    } else if is_write {
        commit_writes(session, vec![statement], None).await?
    } else {
        run_read(session, statement.query, statement.params).await?
    };
    query_result_to_pending(result, if is_write { "w" } else { "r" })
}

fn require_signed_graph(
    session: &BoltSession,
    metadata: &HashMap<String, PackValue>,
) -> Result<(), BoltFailure> {
    let Some(requested) = metadata.get("db").and_then(PackValue::as_str) else {
        return Ok(());
    };
    if session.graph.as_deref() == Some(requested) {
        Ok(())
    } else {
        Err(BoltFailure::client(
            "Neo.ClientError.Security.Forbidden",
            "database selection must match the signed session graph",
        ))
    }
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
            session.transaction = None;
            write_msg(s, &success(vec![])).await?;
            continue;
        }
        // In FAILED state, every other message is IGNORED until RESET.
        if session.failed {
            write_msg(s, &ignored()).await?;
            continue;
        }
        if session.authority.is_none() && !matches!(tag, MSG_HELLO | MSG_LOGON | MSG_LOGOFF) {
            let denied = BoltFailure::client(
                "Neo.ClientError.Security.Unauthorized",
                "authentication required",
            );
            write_msg(s, &failure(&denied)).await?;
            session.failed = true;
            continue;
        }

        match tag {
            MSG_HELLO => {
                let extra = fields
                    .first()
                    .cloned()
                    .map(PackValue::into_map)
                    .unwrap_or_default();
                session.authority = None;
                session.graph = None;
                match authenticate_session(&session.state, &extra).await {
                    Ok((authority, graph, request_seed)) => {
                        session.authority = Some(authority);
                        session.graph = Some(graph);
                        session.request_seed = Some(request_seed);
                        session.request_sequence = 0;
                        let meta = vec![
                            ("server", PackValue::String(SERVER_AGENT.to_string())),
                            (
                                "connection_id",
                                PackValue::String(format!("bolt-{}", next_conn_id())),
                            ),
                        ];
                        write_msg(s, &success(meta)).await?;
                    }
                    Err(denied) => {
                        write_msg(s, &failure(&denied)).await?;
                        session.failed = true;
                    }
                }
            }
            MSG_LOGON => {
                let extra = fields
                    .first()
                    .cloned()
                    .map(PackValue::into_map)
                    .unwrap_or_default();
                if session.transaction.is_some() {
                    let denied = BoltFailure::client(
                        "Neo.ClientError.Transaction.TransactionAccessedConcurrently",
                        "cannot replace authority during a transaction",
                    );
                    write_msg(s, &failure(&denied)).await?;
                    session.failed = true;
                    continue;
                }
                session.authority = None;
                session.graph = None;
                match authenticate_session(&session.state, &extra).await {
                    Ok((authority, graph, request_seed)) => {
                        session.authority = Some(authority);
                        session.graph = Some(graph);
                        session.request_seed = Some(request_seed);
                        session.request_sequence = 0;
                        write_msg(s, &success(vec![])).await?;
                    }
                    Err(denied) => {
                        write_msg(s, &failure(&denied)).await?;
                        session.failed = true;
                    }
                }
            }
            MSG_LOGOFF => {
                session.authority = None;
                session.graph = None;
                session.request_seed = None;
                session.transaction = None;
                session.pending = None;
                write_msg(s, &success(vec![])).await?;
            }
            MSG_BEGIN => {
                let extra = fields
                    .first()
                    .cloned()
                    .map(PackValue::into_map)
                    .unwrap_or_default();
                let started = if session.transaction.is_some() {
                    Err(BoltFailure::client(
                        "Neo.ClientError.Transaction.TransactionAccessedConcurrently",
                        "an explicit transaction is already active",
                    ))
                } else {
                    match require_signed_graph(&session, &extra) {
                        Ok(()) => begin_transaction(&session).await,
                        Err(error) => Err(error),
                    }
                };
                match started {
                    Ok(transaction) => {
                        session.transaction = Some(transaction);
                        write_msg(s, &success(vec![])).await?;
                    }
                    Err(error) => {
                        write_msg(s, &failure(&error)).await?;
                        session.failed = true;
                    }
                }
            }
            MSG_COMMIT => {
                let Some(transaction) = session.transaction.take() else {
                    let error = BoltFailure::client(
                        "Neo.ClientError.Transaction.InvalidBookmark",
                        "no explicit transaction is active",
                    );
                    write_msg(s, &failure(&error)).await?;
                    session.failed = true;
                    continue;
                };
                let committed = if transaction.writes.is_empty() {
                    Ok(())
                } else {
                    commit_writes(
                        &mut session,
                        transaction.writes,
                        Some(transaction.base_version),
                    )
                    .await
                    .map(|_| ())
                };
                match committed {
                    Ok(()) => {
                        write_msg(
                            s,
                            &success(vec![(
                                "bookmark",
                                PackValue::String(format!(
                                    "eg:bookmark:{:016x}",
                                    session.request_sequence
                                )),
                            )]),
                        )
                        .await?;
                    }
                    Err(error) => {
                        write_msg(s, &failure(&error)).await?;
                        session.failed = true;
                    }
                }
            }
            MSG_ROLLBACK => {
                session.transaction = None;
                session.pending = None;
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
                let extra = fields
                    .get(2)
                    .cloned()
                    .map(PackValue::into_map)
                    .unwrap_or_default();
                if let Err(error) = require_signed_graph(&session, &extra) {
                    write_msg(s, &failure(&error)).await?;
                    session.failed = true;
                    continue;
                }
                match run_cypher(&mut session, &query, &params).await {
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
    const MAX_BOLT_MESSAGE_BYTES: usize = 64 * 1024 * 1024;
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
        let end = start.checked_add(len).ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Bolt message size overflow",
            )
        })?;
        if end > MAX_BOLT_MESSAGE_BYTES {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Bolt message exceeds the resource limit",
            ));
        }
        body.resize(end, 0);
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
/// signed graph / authority / transaction stay isolated.
pub async fn serve(addr: &str, state: Arc<RwLock<ServerState>>) -> std::io::Result<()> {
    let auth_secret = state.read().await.auth_secret.clone();
    validate_startup_policy(addr, &auth_secret)?;
    let listener = TcpListener::bind(addr).await?;
    tracing::info!(
        "bolt-wire: serving Neo4j Bolt v4.4 on configured loopback with signed-session authentication"
    );
    loop {
        let (socket, peer) = listener.accept().await?;
        let session = BoltSession::new(state.clone());
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

    /// A `ServerState` seeded with public nodes so a signed Cypher read returns
    /// rows over `__commons__` even under default-deny row isolation.
    fn test_state(seed_public_nodes: bool) -> Arc<RwLock<ServerState>> {
        let registry = GraphRegistry::new();
        if seed_public_nodes {
            let core = registry.get("__commons__").unwrap().core.clone();
            for (id, ty) in [("n1", "Agent"), ("n2", "Agent"), ("n3", "Tool")] {
                let blob = rmp_serde::to_vec_named(&serde_json::json!({
                    "type": ty,
                    "id": id,
                    "_visibility": "public"
                }))
                .unwrap();
                core.add_node(id.to_string(), blob);
            }
        }
        let mut isolation = IsolationLayer::new();
        #[cfg(feature = "security")]
        {
            use crate::acl::{Grant, GrantEffect, RbacAction, ResourceSelector, Role};
            isolation.add_role(Role::new("commons-user"));
            isolation.add_grant(Grant {
                role: "commons-user".to_string(),
                resource: ResourceSelector::Graph("__commons__".to_string()),
                action: RbacAction::Read,
                effect: GrantEffect::Allow,
            });
            isolation.add_grant(Grant {
                role: "commons-user".to_string(),
                resource: ResourceSelector::Graph("__commons__".to_string()),
                action: RbacAction::Write,
                effect: GrantEffect::Allow,
            });
        }
        isolation.register_agent(crate::isolation::AgentIdentity {
            agent_id: "service:bolt-test".to_string(),
            role: crate::isolation::AgentRole::Agent,
            teams: Vec::new(),
            #[cfg(feature = "security")]
            roles: vec!["commons-user".to_string()],
            #[cfg(not(feature = "security"))]
            roles: Vec::new(),
        });
        Arc::new(RwLock::new(ServerState {
            #[cfg(feature = "redb")]
            cold_tracker: std::sync::Arc::new(
                crate::server::persistence::cold_offload::ColdTenantTracker::new(),
            ),
            registry,
            isolation,
            channels: ChannelManager::new(),
            auth_secret: "test".to_string(),
            #[cfg(feature = "kv")]
            kv: None,
            persist_dir: None,
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
            tsdb_store: None,
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
            #[cfg(feature = "lake")]
            lake: std::sync::Arc::new(crate::server::lake::LakeManager::new()),
        }))
    }

    fn seeded_state() -> Arc<RwLock<ServerState>> {
        test_state(true)
    }

    #[cfg(feature = "redb")]
    fn temp_dir(tag: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "eg-bolt-test-{tag}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        dir
    }

    #[cfg(feature = "redb")]
    async fn durable_state(
        dir: &std::path::Path,
    ) -> (
        Arc<RwLock<ServerState>>,
        Arc<dyn crate::server::persistence::PersistenceBackend>,
    ) {
        use crate::durability::DurabilityPolicy;
        use crate::protocol::GraphType;
        use crate::server::persistence::redb_backend::RedbBackend;

        let backend = RedbBackend::open(
            dir.to_string_lossy().to_string(),
            DurabilityPolicy::Each,
            64,
        )
        .expect("open Bolt test backend");
        let persistence: Arc<dyn crate::server::persistence::PersistenceBackend> =
            Arc::new(backend);
        persistence
            .register_graph("__commons__", "__commons__", GraphType::Commons)
            .await
            .unwrap();
        let state = test_state(false);
        {
            let mut state_guard = state.write().await;
            state_guard.persist_dir = Some(dir.to_string_lossy().to_string());
            state_guard.persistence = Some(persistence.clone());
        }
        (state, persistence)
    }

    fn signed_session_request(graph: &str) -> crate::protocol::Request {
        crate::server::auth::sign_current_test_request(
            "test",
            crate::protocol::Request {
                id: next_conn_id(),
                graph: graph.to_string(),
                auth_token: String::new(),
                agent_id: Some("service:bolt-test".to_string()),
                method: Method::Health,
            },
        )
    }

    fn auth_map(request: &crate::protocol::Request) -> HashMap<String, PackValue> {
        PackValue::map(vec![
            ("scheme", PackValue::String(BOLT_AUTH_SCHEME.into())),
            (
                "principal",
                PackValue::String("principal:opaque".to_string()),
            ),
            (
                "credentials",
                PackValue::String(encode_session_credential(request).unwrap()),
            ),
        ])
        .into_map()
    }

    #[test]
    fn signed_session_listener_requires_key_material_and_loopback() {
        assert!(validate_startup_policy("127.0.0.1:7687", "test").is_ok());
        assert!(validate_startup_policy("127.0.0.1:7687", "").is_err());
        assert!(validate_startup_policy("0.0.0.0:7687", "test").is_err());
    }

    #[tokio::test]
    async fn signed_session_verifies_current_context_and_rejects_tampering() {
        let state = seeded_state();
        let request = signed_session_request("__commons__");
        let valid = auth_map(&request);
        let (verified, graph, _) = authenticate_session(&state, &valid).await.unwrap();
        assert_eq!(verified.agent_id(), "service:bolt-test");
        assert_eq!(graph, "__commons__");

        let mut forged = valid;
        forged.insert(
            "credentials".to_string(),
            PackValue::String("00".repeat(32)),
        );
        let error = authenticate_session(&state, &forged).await.unwrap_err();
        assert_eq!(error.code, "Neo.ClientError.Security.Unauthorized");
        assert!(!error.message.contains("service:bolt-test"));
        assert!(!error.message.contains(&request.auth_token));
    }

    #[tokio::test]
    async fn signed_session_rejects_cross_tenant_claims() {
        use crate::acl::RequestContextClaims;
        use crate::server::auth::{compute_verified_envelope_token, VerifiedEnvelopeParams};

        let mut request = crate::protocol::Request {
            id: next_conn_id(),
            graph: "__commons__".to_string(),
            auth_token: String::new(),
            agent_id: Some("service:bolt-test".to_string()),
            method: Method::Health,
        };
        let claims = RequestContextClaims {
            principal: "service:bolt-test".to_string(),
            tenant: "tenant-other".to_string(),
            audience: "epistemic-graph-test".to_string(),
            agent_id: "service:bolt-test".to_string(),
            roles: vec!["test".to_string()],
            scopes: vec!["query:cypher".to_string()],
            policy_version: "policy-test".to_string(),
            delegation: Vec::new(),
            node: None,
            priority: None,
        };
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let nonce = format!("bolt-cross-tenant-{}", next_conn_id());
        request.auth_token = compute_verified_envelope_token(
            "test",
            &request,
            &VerifiedEnvelopeParams {
                context: &claims,
                timestamp,
                nonce: &nonce,
                idempotency_key: "bolt-cross-tenant",
            },
        );
        let error = authenticate_session(&seeded_state(), &auth_map(&request))
            .await
            .unwrap_err();
        assert_eq!(error.code, "Neo.ClientError.Security.Unauthorized");
        assert!(!error.message.contains("tenant-other"));
    }

    /// A test client half of a `tokio::io::duplex` pair, with helpers to complete the
    /// handshake and send/recv chunked Bolt messages.
    struct TestClient {
        stream: tokio::io::DuplexStream,
        server_task: tokio::task::JoinHandle<()>,
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

        async fn close(self) {
            let TestClient {
                stream,
                server_task,
            } = self;
            drop(stream);
            server_task.await.unwrap();
        }

        fn tag_of(v: &PackValue) -> u8 {
            match v {
                PackValue::Structure { tag, .. } => *tag,
                _ => panic!("expected a structure, got {v:?}"),
            }
        }
    }

    fn hello() -> PackValue {
        let request = signed_session_request("__commons__");
        PackValue::Structure {
            tag: MSG_HELLO,
            fields: vec![PackValue::map(vec![
                ("user_agent", PackValue::String("test/1.0".into())),
                ("scheme", PackValue::String(BOLT_AUTH_SCHEME.into())),
                (
                    "principal",
                    PackValue::String("principal:opaque".to_string()),
                ),
                (
                    "credentials",
                    PackValue::String(encode_session_credential(&request).unwrap()),
                ),
            ])],
        }
    }

    fn forged_hello() -> PackValue {
        PackValue::Structure {
            tag: MSG_HELLO,
            fields: vec![PackValue::map(vec![
                ("user_agent", PackValue::String("test/1.0".into())),
                ("scheme", PackValue::String(BOLT_AUTH_SCHEME.into())),
                (
                    "principal",
                    PackValue::String("principal:opaque".to_string()),
                ),
                ("credentials", PackValue::String("00".repeat(32))),
            ])],
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

    fn run_on_graph(query: &str, graph: &str) -> PackValue {
        PackValue::Structure {
            tag: MSG_RUN,
            fields: vec![
                PackValue::String(query.into()),
                PackValue::map(vec![]),
                PackValue::map(vec![("db", PackValue::String(graph.to_string()))]),
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
        let session = BoltSession::new(state);
        let server_task = tokio::spawn(async move {
            let mut server = server;
            let _ = handle_connection(&mut server, session).await;
        });
        TestClient {
            stream: client,
            server_task,
        }
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
    async fn forged_hello_principal_is_rejected_before_query_execution() {
        let mut c = spawn_conn(seeded_state());
        c.handshake().await;
        c.send(&forged_hello()).await;
        assert_eq!(TestClient::tag_of(&c.recv().await), MSG_FAILURE);
        c.send(&run("MATCH (n) RETURN n")).await;
        assert_eq!(TestClient::tag_of(&c.recv().await), MSG_IGNORED);
    }

    #[tokio::test]
    async fn unsigned_run_database_cannot_replace_signed_graph() {
        let mut c = spawn_conn(seeded_state());
        c.handshake().await;
        c.send(&hello()).await;
        assert_eq!(TestClient::tag_of(&c.recv().await), MSG_SUCCESS);
        c.send(&run_on_graph(
            "MATCH (n) RETURN n.id AS id",
            "tenant-other-graph",
        ))
        .await;
        assert_eq!(TestClient::tag_of(&c.recv().await), MSG_FAILURE);
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
    async fn bolt_rollback_discards_staged_write_without_touching_live_graph() {
        let state = seeded_state();
        let core = state
            .read()
            .await
            .registry
            .get("__commons__")
            .unwrap()
            .core
            .clone();
        let mut c = spawn_conn(state);
        c.handshake().await;
        c.send(&hello()).await;
        assert_eq!(TestClient::tag_of(&c.recv().await), MSG_SUCCESS);
        c.send(&PackValue::Structure {
            tag: MSG_BEGIN,
            fields: vec![PackValue::map(vec![])],
        })
        .await;
        assert_eq!(TestClient::tag_of(&c.recv().await), MSG_SUCCESS);

        c.send(&run(
            "CREATE (n:Transient {id: 'rolled-back'}) RETURN n.id AS id",
        ))
        .await;
        assert_eq!(TestClient::tag_of(&c.recv().await), MSG_SUCCESS);
        assert!(
            !core.has_node("rolled-back"),
            "RUN must not publish a transaction write"
        );
        c.send(&pull_all()).await;
        loop {
            if TestClient::tag_of(&c.recv().await) == MSG_SUCCESS {
                break;
            }
        }
        c.send(&PackValue::Structure {
            tag: MSG_ROLLBACK,
            fields: vec![],
        })
        .await;
        assert_eq!(TestClient::tag_of(&c.recv().await), MSG_SUCCESS);
        assert!(!core.has_node("rolled-back"));
    }

    #[cfg(feature = "redb")]
    #[tokio::test(flavor = "multi_thread")]
    async fn bolt_atomic_commit_survives_backend_restart() {
        use crate::durability::DurabilityPolicy;
        use crate::server::persistence::redb_backend::RedbBackend;

        // Held for the whole test: this test opens a `RedbBackend`, then DROPS it and
        // reopens the SAME persist dir below ("survives_backend_restart"). A
        // `RedbBackend` resolves+caches its cipher ONCE per `open()` call from
        // `EPISTEMIC_GRAPH_ENCRYPTION_KEY`, so the two opens must observe the SAME env
        // var value or the reopen panics with "encrypted durable value is missing
        // sealed framing" (or, with a different key, "decryption failed"). Without
        // this lock, a concurrent `crypto::tests::EnvGuard`-protected test elsewhere in
        // the crate can transiently flip that process-global var between the two
        // opens. See `crate::crypto::acquire_test_env_lock`'s doc for the full
        // mechanism (this was the actual root cause of this test's parallel flake).
        #[cfg(feature = "security")]
        let _env_lock = crate::crypto::acquire_test_env_lock();
        let dir = temp_dir("restart");
        let (state, persistence) = durable_state(&dir).await;
        let core = state
            .read()
            .await
            .registry
            .get("__commons__")
            .unwrap()
            .core
            .clone();
        let mut c = spawn_conn(state.clone());
        c.handshake().await;
        c.send(&hello()).await;
        assert_eq!(TestClient::tag_of(&c.recv().await), MSG_SUCCESS);
        c.send(&PackValue::Structure {
            tag: MSG_BEGIN,
            fields: vec![PackValue::map(vec![])],
        })
        .await;
        assert_eq!(TestClient::tag_of(&c.recv().await), MSG_SUCCESS);

        for query in [
            "CREATE (n:Durable {id: 'durable-a'}) RETURN n.id AS id",
            "CREATE (n:Durable {id: 'durable-b'}) RETURN n.id AS id",
        ] {
            c.send(&run(query)).await;
            assert_eq!(TestClient::tag_of(&c.recv().await), MSG_SUCCESS);
            c.send(&pull_all()).await;
            loop {
                if TestClient::tag_of(&c.recv().await) == MSG_SUCCESS {
                    break;
                }
            }
            assert_eq!(core.node_count(), 0, "RUN remains detached before COMMIT");
        }
        c.send(&PackValue::Structure {
            tag: MSG_COMMIT,
            fields: vec![],
        })
        .await;
        assert_eq!(TestClient::tag_of(&c.recv().await), MSG_SUCCESS);
        assert_eq!(core.node_count(), 2, "one commit publishes both rows");

        c.close().await;
        drop(state);
        persistence.shutdown();
        drop(persistence);

        let reopened: Arc<dyn crate::server::persistence::PersistenceBackend> = Arc::new(
            RedbBackend::open(
                dir.to_string_lossy().to_string(),
                DurabilityPolicy::Each,
                64,
            )
            .expect("reopen Bolt test backend"),
        );
        let recovered = test_state(false);
        reopened.load_all(&recovered).await.unwrap();
        let recovered_core = recovered
            .read()
            .await
            .registry
            .get("__commons__")
            .unwrap()
            .core
            .clone();
        assert_eq!(recovered_core.node_count(), 2);
        reopened.shutdown();
        drop(reopened);
        drop(recovered);
        let _ = std::fs::remove_dir_all(dir);
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
