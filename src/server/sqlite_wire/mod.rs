//! SQLite-compatible served surface (CONCEPT:EG-KG.query.concept-3) — Phase J of the Universal-DB
//! multi-wire program. The SECOND consumer of the wire-agnostic execution core
//! (CONCEPT:EG-KG.compute.subsystems-reference), after the Postgres shim (`crate::server::pgwire`).
//!
//! ## Why this is not a "wire" like the others
//! SQLite has **no client/server wire protocol** — it is an embedded library that
//! links into a process and reads/writes a single `.db` **file format**. There is no
//! socket handshake to emulate. So "SQLite-compatible" is delivered as TWO
//! complementary things:
//!
//!   1. **SQLite-dialect SQL over a lightweight served surface (this module).** A
//!      dependency-free NDJSON-over-TCP request/response endpoint (the SAME hand-rolled
//!      tokio-listener idiom as `sparql_http`/`metrics` — NO axum/hyper, so the Pi
//!      contract holds) that accepts SQLite-dialect SQL, runs the SQLite-isms through
//!      [`dialect::translate_sqlite_sql`], and executes the result through the ONE
//!      shared [`WireSession`] (`WireProtocol::execute` → classify → read/write →
//!      durability). An application that "speaks SQLite SQL" points at this endpoint and
//!      its `CREATE TABLE … INTEGER PRIMARY KEY AUTOINCREMENT` / `INSERT` / `SELECT …
//!      a || b` / `PRAGMA` statements work against the engine.
//!
//!   2. **`.db` file export/import** — producing / loading a real SQLite database file.
//!      SHIPPED as `CONCEPT:EG-KG.query.eg-feature`/`CONCEPT:EG-KG.query.full-protocol` behind the SEPARATE `sqlite-file`
//!      feature (`src/server/handlers/sqlite_file.rs` + the `ImportSqliteFile`/
//!      `ExportSqliteFile` methods). It reimplements the SQLite b-tree page format from
//!      scratch in `eg-sqlite-format` (a pure-Rust reader + bulk-load writer — NO rusqlite,
//!      NO libsqlite3-sys, NO C toolchain), so BOTH the `sqlite-wire` surface here AND the
//!      `.db` file leg stay pure-Rust and the Pi contract holds everywhere.
//!
//! ## The wire-neutral promise (CONCEPT:EG-KG.compute.subsystems-reference)
//! NOTHING about SQL classification, read execution (DataFusion), the graph write path,
//! transactions, ACL, or durability is reimplemented here. This module is purely: the
//! TCP framing (newline-delimited JSON), the SQLite-dialect rewrite, and the encoding of
//! a wire-neutral [`WireOutcome`]/[`WireError`] into a JSON response object. Every
//! statement goes through the identical `WireSession` the Postgres shim and the native
//! `Method::Sql` path use.
//!
//! ## The protocol (deliberately tiny)
//! One JSON object per line, one JSON response line back, over a persistent TCP
//! connection (so `SET graph = …` and `BEGIN`/`COMMIT` are connection-scoped, exactly
//! like a pgwire connection):
//!   * request:  `{"id":1,"graph":"<graph>","auth_token":"eg2.…","sql":"<statement>"}`
//!   * rows:     `{"columns":[{"name":..,"type":..}, …], "rows":[[..], …]}`
//!   * command:  `{"tag":"INSERT", "rows_affected": 1}`  (`rows_affected` omitted when none)
//!   * txn:      `{"tag":"BEGIN"|"COMMIT"|"ROLLBACK"}`
//!   * pragma:   `{"tag":"PRAGMA"}`  (a no-op ack)
//!   * error:    `{"error":{"code":"58000","message":"…"}}`
//!
//! Column `type` is reported as the SQLite storage-class name (`INTEGER`/`REAL`/`TEXT`)
//! so a SQLite-minded client sees familiar types.
//!
//! ## `.db` file export/import — SHIPPED (CONCEPT:EG-KG.query.eg-feature/EG-332)
//! Delivered in `src/server/handlers/sqlite_file.rs` behind the `sqlite-file` feature:
//! import reads a `.db`'s tables+rows into the `TableStore`; export writes a `TableStore`
//! selection out to a valid `sqlite3`-readable `.db`. The SQLite b-tree page format is
//! serialized WITHOUT any C dependency by the pure-Rust `eg-sqlite-format` crate (reader +
//! bottom-up bulk-load writer), whose output passes a real `sqlite3 PRAGMA integrity_check`
//! — so the whole `sqlite-file` leg is pure-Rust and the Pi contract is preserved.

use std::sync::Arc;

use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;
use tokio::sync::RwLock;

use eg_query::PgColType;
use serde::Deserialize;

use crate::protocol::{Method, Request};
use crate::server::wire::{WireError, WireOutcome, WireProtocol, WireSession};
use crate::server::ServerState;

pub mod dialect;

pub use dialect::{translate_sqlite_sql, Translated};

/// Env var carrying the bind address (`host:port`) for the SQLite-dialect served
/// surface. Unset → no listener (opt-in), matching every other interop endpoint.
pub const SQLITE_ADDR_ENV: &str = "EPISTEMIC_GRAPH_SQLITE_ADDR";
/// Env var: the default graph a fresh connection runs against until a `SET graph`
/// overrides it. Defaults to `__commons__`.
pub const SQLITE_GRAPH_ENV: &str = "EPISTEMIC_GRAPH_SQLITE_GRAPH";

#[derive(Deserialize)]
struct SignedSqliteRequest {
    id: u64,
    graph: String,
    auth_token: String,
    #[serde(default)]
    agent_id: Option<String>,
    sql: String,
}

/// The largest single request line accepted (guards against an unbounded line flood on
/// the persistent connection). One SQL statement per line; 8 MiB is generous.
const MAX_LINE_BYTES: u64 = 8 * 1024 * 1024;

/// Serve the SQLite-dialect NDJSON surface on `listener`, backed by the engine `state`.
/// One [`WireSession`] per accepted connection (so `SET graph` / transaction state stays
/// isolated per connection), exactly like a pgwire connection. Runs until the process
/// exits. Spawned by `main.rs` only when built `--features sqlite-wire` AND
/// `EPISTEMIC_GRAPH_SQLITE_ADDR` is set.
pub async fn serve(listener: TcpListener, state: Arc<RwLock<ServerState>>) {
    let persist_dir = state.read().await.persist_dir.clone();
    if crate::server::sql_tables::validate_served_configuration(
        persist_dir.as_deref().map(std::path::Path::new),
    )
    .is_err()
    {
        tracing::error!("sqlite-wire disabled: owner-scoped SQL catalog is not configured");
        return;
    }
    let default_graph =
        std::env::var(SQLITE_GRAPH_ENV).unwrap_or_else(|_| "__commons__".to_string());
    loop {
        let Ok((stream, _peer)) = listener.accept().await else {
            continue;
        };
        let session = Arc::new(WireSession::new(state.clone(), default_graph.clone()));
        tokio::spawn(async move {
            handle_conn(stream, session).await;
        });
    }
}

/// Drive one persistent connection: read newline-delimited JSON requests, answer each
/// with a newline-delimited JSON response, until EOF or a fatal I/O error.
async fn handle_conn(stream: tokio::net::TcpStream, session: Arc<WireSession>) {
    let (read_half, mut write_half) = stream.into_split();
    let mut reader = BufReader::new(read_half);
    let mut line = String::new();
    loop {
        line.clear();
        // Bound the per-line read so a client cannot exhaust memory with one huge line.
        let n = {
            let mut limited = (&mut reader).take(MAX_LINE_BYTES);
            match limited.read_line(&mut line).await {
                Ok(n) => n,
                Err(_) => break,
            }
        };
        if n == 0 {
            break; // EOF
        }
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue; // tolerate blank keep-alive lines
        }
        let response = execute_request(session.as_ref(), trimmed).await;
        if write_half.write_all(response.as_bytes()).await.is_err() {
            break;
        }
        if write_half.write_all(b"\n").await.is_err() {
            break;
        }
    }
    let _ = write_half.shutdown().await;
}

/// Parse one JSON request line, translate the SQLite-dialect SQL, run it through the
/// shared [`WireSession`], and encode the outcome as a JSON response STRING (the ONE
/// place SQLite-surface framing meets the wire-neutral core). This is the testable
/// heart of the surface — a socket is not required to exercise it.
pub async fn execute_request(session: &WireSession, line: &str) -> String {
    let request: SignedSqliteRequest = match serde_json::from_str(line) {
        Ok(v) => v,
        Err(e) => return error_json("22000", &format!("malformed JSON request: {e}")),
    };
    let signed = Request {
        id: request.id,
        graph: request.graph,
        auth_token: request.auth_token,
        agent_id: request.agent_id,
        method: Method::Sql {
            query: request.sql.clone(),
            params_msgpack: Vec::new(),
        },
    };
    if let Err(error) = session.authenticate_request(&signed).await {
        return wire_error_json(&error);
    }

    match translate_sqlite_sql(&request.sql) {
        // A PRAGMA is an authenticated no-op acknowledgement.
        Translated::Noop { tag } => serde_json::json!({ "tag": tag }).to_string(),
        Translated::Sql(engine_sql) => match session.execute(&engine_sql).await {
            Ok(outcome) => outcome_json(outcome),
            Err(e) => wire_error_json(&e),
        },
    }
}

/// Encode a wire-neutral [`WireOutcome`] into the SQLite-surface JSON response.
fn outcome_json(outcome: WireOutcome) -> String {
    match outcome {
        WireOutcome::Rows(result) => {
            let columns: Vec<serde_json::Value> = result
                .columns
                .iter()
                .map(|c| {
                    serde_json::json!({
                        "name": c.name,
                        "type": sqlite_type_name(c.ty),
                    })
                })
                .collect();
            // `result.rows` is already `Vec<Vec<serde_json::Value>>` — pass it through.
            serde_json::json!({
                "columns": columns,
                "rows": result.rows,
            })
            .to_string()
        }
        WireOutcome::Command { tag, rows } => match rows {
            Some(n) => serde_json::json!({ "tag": tag, "rows_affected": n }).to_string(),
            None => serde_json::json!({ "tag": tag }).to_string(),
        },
        WireOutcome::TxnStart => serde_json::json!({ "tag": "BEGIN" }).to_string(),
        WireOutcome::TxnEnd { tag } => serde_json::json!({ "tag": tag }).to_string(),
        // `COPY … FROM STDIN` has no analogue in the SQLite surface (SQLite has no COPY);
        // it cannot arise from SQLite-dialect SQL, so report it as unsupported rather
        // than entering a copy-in mode this line protocol does not model.
        WireOutcome::CopyIn { .. } => error_json(
            "0A000",
            "COPY FROM STDIN is not supported over the SQLite surface",
        ),
    }
}

/// Map a wire-neutral [`WireError`] (SQLSTATE + message) to the JSON error object.
fn wire_error_json(e: &WireError) -> String {
    error_json(&e.code, &e.message)
}

/// Build a `{"error":{"code","message"}}` response.
fn error_json(code: &str, message: &str) -> String {
    serde_json::json!({ "error": { "code": code, "message": message } }).to_string()
}

/// The SQLite storage-class name for an engine result-column type, so a SQLite-minded
/// client sees a familiar type. SQLite has five storage classes (NULL/INTEGER/REAL/
/// TEXT/BLOB); booleans and vectors have no dedicated class, so a bool is reported as
/// INTEGER (SQLite's own convention) and a vector as TEXT (its `[..]` rendering).
fn sqlite_type_name(t: PgColType) -> &'static str {
    match t {
        PgColType::Int8 => "INTEGER",
        PgColType::Float8 => "REAL",
        PgColType::Bool => "INTEGER",
        PgColType::Text => "TEXT",
        PgColType::Vector => "TEXT",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::isolation::{AgentIdentity, AgentRole, IsolationLayer};
    #[cfg(feature = "redb")]
    use crate::server::persistence::{redb_backend::RedbBackend, PersistenceBackend};

    /// A minimal `ServerState` with a pre-created `__commons__` graph (the registry
    /// creates it), mirroring `tests/pgwire_roundtrip.rs::state_with`. Feature-gated
    /// fields track whatever tier the test build enables.
    fn test_state() -> Arc<RwLock<ServerState>> {
        let mut isolation = IsolationLayer::new();
        isolation.register_agent(AgentIdentity {
            agent_id: "system".to_string(),
            role: AgentRole::System,
            teams: Vec::new(),
            roles: Vec::new(),
        });
        isolation.register_agent(AgentIdentity {
            agent_id: "peer".to_string(),
            role: AgentRole::Agent,
            teams: Vec::new(),
            roles: Vec::new(),
        });
        let mut state = ServerState::new_for_test("test-sqlite-wire-secret", isolation);
        state.persist_dir = Some(
            crate::server::sql_tables::test_persist_dir()
                .to_string_lossy()
                .into_owned(),
        );
        Arc::new(RwLock::new(state))
    }

    fn test_session(state: &Arc<RwLock<ServerState>>) -> WireSession {
        WireSession::new(state.clone(), "__commons__".to_string())
    }

    #[cfg(feature = "redb")]
    async fn durable_test_state() -> (
        Arc<RwLock<ServerState>>,
        Arc<RedbBackend>,
        std::path::PathBuf,
    ) {
        let state = test_state();
        let persist_dir = state
            .read()
            .await
            .persist_dir
            .as_deref()
            .map(std::path::PathBuf::from)
            .expect("SQLite durability fixture has a persistence directory");
        std::fs::create_dir_all(&persist_dir).expect("create SQLite durability directory");
        let backend = Arc::new(
            RedbBackend::open_with_shards(persist_dir.to_string_lossy().into_owned(), 64, 1)
                .expect("open SQLite durability backend"),
        );
        backend
            .register_graph(
                "__commons__",
                "__commons__",
                crate::protocol::GraphType::Commons,
            )
            .await
            .expect("register SQLite durability graph");
        state.write().await.persistence = Some(backend.clone());
        (state, backend, persist_dir)
    }

    #[cfg(feature = "redb")]
    async fn reopen_durable_test_backend(
        state: &Arc<RwLock<ServerState>>,
        backend: Arc<RedbBackend>,
        persist_dir: &std::path::Path,
    ) -> Arc<RedbBackend> {
        backend.shutdown();
        state.write().await.persistence = None;
        drop(backend);

        for _ in 0..50 {
            match RedbBackend::open_with_shards(persist_dir.to_string_lossy().into_owned(), 64, 1) {
                Ok(reopened) => {
                    let reopened = Arc::new(reopened);
                    state.write().await.persistence = Some(reopened.clone());
                    return reopened;
                }
                Err(_) => tokio::task::yield_now().await,
            }
        }
        panic!("reopen SQLite durability backend");
    }

    /// A unique user-table name — the user-table SQL store is a process-global
    /// singleton, so a fixed name could collide with a sibling test / prior run.
    fn unique_table() -> String {
        use std::time::{SystemTime, UNIX_EPOCH};
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        format!("sqlt_{nanos}")
    }

    fn req(sql: &str) -> String {
        static REQUEST_ID: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
        let id = REQUEST_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let signed = crate::server::auth::sign_current_test_request(
            "test-sqlite-wire-secret",
            Request {
                id,
                graph: "__commons__".to_string(),
                auth_token: String::new(),
                agent_id: Some("system".to_string()),
                method: Method::Sql {
                    query: sql.to_string(),
                    params_msgpack: Vec::new(),
                },
            },
        );
        serde_json::json!({
            "id": signed.id,
            "graph": signed.graph,
            "auth_token": signed.auth_token,
            "agent_id": signed.agent_id,
            "sql": sql,
        })
        .to_string()
    }

    #[cfg(feature = "redb")]
    fn signed_req(sql: &str, id: u64, agent: &str, nonce: &str, idempotency_key: &str) -> String {
        let context = crate::acl::RequestContextClaims {
            principal: agent.to_string(),
            tenant: "tenant-shared".to_string(),
            audience: "epistemic-graph-test".to_string(),
            agent_id: agent.to_string(),
            roles: vec!["test".to_string()],
            scopes: vec!["*".to_string()],
            policy_version: "policy-test".to_string(),
            delegation: Vec::new(),
            node: None,
            priority: None,
        };
        let mut signed = Request {
            id,
            graph: "__commons__".to_string(),
            auth_token: String::new(),
            agent_id: Some(agent.to_string()),
            method: Method::Sql {
                query: sql.to_string(),
                params_msgpack: Vec::new(),
            },
        };
        signed.auth_token = crate::server::compute_verified_envelope_token(
            "test-sqlite-wire-secret",
            &signed,
            &crate::server::VerifiedEnvelopeParams {
                context: &context,
                timestamp: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .expect("system clock")
                    .as_secs(),
                nonce,
                idempotency_key,
            },
        );
        serde_json::json!({
            "id": signed.id,
            "graph": signed.graph,
            "auth_token": signed.auth_token,
            "agent_id": signed.agent_id,
            "sql": sql,
        })
        .to_string()
    }

    #[cfg(feature = "redb")]
    fn response(line: &str) -> serde_json::Value {
        serde_json::from_str(line).expect("SQLite wire response is JSON")
    }

    #[tokio::test]
    async fn actor_only_session_cannot_execute_or_change_graph() {
        // Reads the ambient encryption env at its durable open, so the env must hold
        // still for this whole body. READ guard: it excludes only a key MUTATOR, never
        // another opener. See `crate::crypto::acquire_test_env_read_lock`'s doc.
        let _env_read_lock = crate::crypto::acquire_test_env_read_lock().await;
        let state = test_state();
        let session = test_session(&state);
        session.resolve_startup(
            Some("unverified-actor".to_string()),
            Some("__commons__".to_string()),
        );

        let read = match session.execute("SELECT id FROM nodes").await {
            Ok(_) => panic!("actor-only SQL read must fail closed"),
            Err(error) => error,
        };
        assert_eq!(read.code, "28000");
        let graph_switch = match session.execute("SET graph = '__commons__'").await {
            Ok(_) => panic!("actor-only graph switch must fail closed"),
            Err(error) => error,
        };
        assert_eq!(graph_switch.code, "28000");
    }

    #[tokio::test]
    async fn served_catalog_is_actor_isolated_and_cross_protocol_for_same_actor() {
        // Reads the ambient encryption env at its durable open, so the env must hold
        // still for this whole body. READ guard: it excludes only a key MUTATOR, never
        // another opener. See `crate::crypto::acquire_test_env_read_lock`'s doc.
        let _env_read_lock = crate::crypto::acquire_test_env_read_lock().await;
        let state = test_state();
        let creator = test_session(&state);
        creator
            .bind_authenticated_sql_actor("pgwire", "system")
            .await
            .unwrap();
        creator
            .execute("CREATE TABLE owner_probe (value TEXT)")
            .await
            .unwrap_or_else(|error| panic!("owner create failed: {error}"));

        let same_actor = test_session(&state);
        same_actor
            .bind_authenticated_sql_actor("mysql-wire", "system")
            .await
            .unwrap();
        assert!(same_actor
            .execute("SELECT value FROM owner_probe")
            .await
            .is_ok());

        let peer = test_session(&state);
        peer.bind_authenticated_sql_actor("mssql-wire", "peer")
            .await
            .unwrap();
        assert!(peer.execute("SELECT value FROM owner_probe").await.is_err());
    }

    /// The full SQLite-dialect statement set from the correctness bar, executed IN
    /// PROCESS through `execute_request` against the shared `WireSession`: a
    /// `CREATE TABLE … INTEGER PRIMARY KEY AUTOINCREMENT`, `INSERT`s (id auto-assigned),
    /// a `SELECT` using `||` concatenation, and a `PRAGMA` no-op — proving the dialect
    /// translation + the served encoding end-to-end.
    #[tokio::test]
    async fn sqlite_dialect_statements_execute_through_wire_session() {
        // Reads the ambient encryption env at its durable open, so the env must hold
        // still for this whole body. READ guard: it excludes only a key MUTATOR, never
        // another opener. See `crate::crypto::acquire_test_env_read_lock`'s doc.
        let _env_read_lock = crate::crypto::acquire_test_env_read_lock().await;
        let state = test_state();
        let session = test_session(&state);
        let table = unique_table();

        // 1. CREATE TABLE with the SQLite rowid-alias primary key.
        let create = execute_request(
            &session,
            &req(&format!(
                "CREATE TABLE {table} (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT)"
            )),
        )
        .await;
        let v: serde_json::Value = serde_json::from_str(&create).unwrap();
        assert_eq!(v["tag"], "CREATE TABLE", "create response: {create}");

        // 2. INSERTs — the id is omitted and auto-assigned (SQLite AUTOINCREMENT semantics
        //    carried by the engine's SERIAL path).
        for name in ["alice", "bob"] {
            let ins = execute_request(
                &session,
                &req(&format!("INSERT INTO {table} (name) VALUES ('{name}')")),
            )
            .await;
            let v: serde_json::Value = serde_json::from_str(&ins).unwrap();
            assert_eq!(v["tag"], "INSERT", "insert response: {ins}");
            assert_eq!(v["rows_affected"], 1, "one row inserted: {ins}");
        }

        // 3. SELECT with `||` string concatenation over the user table.
        let sel = execute_request(
            &session,
            &req(&format!(
                "SELECT id, name || '!' AS greeting FROM {table} ORDER BY id"
            )),
        )
        .await;
        let v: serde_json::Value = serde_json::from_str(&sel).unwrap();
        let cols: Vec<String> = v["columns"]
            .as_array()
            .unwrap()
            .iter()
            .map(|c| c["name"].as_str().unwrap().to_string())
            .collect();
        assert_eq!(
            cols,
            vec!["id".to_string(), "greeting".to_string()],
            "{sel}"
        );
        // id column reports the SQLite INTEGER storage class.
        assert_eq!(v["columns"][0]["type"], "INTEGER", "{sel}");
        let greetings: Vec<String> = v["rows"]
            .as_array()
            .unwrap()
            .iter()
            .map(|r| r[1].as_str().unwrap().to_string())
            .collect();
        assert_eq!(
            greetings,
            vec!["alice!".to_string(), "bob!".to_string()],
            "|| concatenation must run: {sel}"
        );

        // 4. PRAGMA → a no-op ack, engine untouched.
        let pragma = execute_request(&session, &req("PRAGMA foreign_keys = ON")).await;
        let v: serde_json::Value = serde_json::from_str(&pragma).unwrap();
        assert_eq!(v["tag"], "PRAGMA", "pragma response: {pragma}");

        // Cleanup: the user-table store is process-global.
        let _ = execute_request(&session, &req(&format!("DROP TABLE {table}"))).await;
    }

    #[cfg(feature = "redb")]
    #[tokio::test]
    async fn signed_sqlite_requests_pin_identity_and_replay_by_caller_key() {
        #[cfg(feature = "security")]
        let _env_lock = crate::crypto::acquire_test_env_lock().await;
        let (state, backend, persist_dir) = durable_test_state().await;
        let session = test_session(&state);
        let table = unique_table();
        let node_id = format!("sqlite_replay_node_{table}");

        // Graph DML uses the same signed carrier. A fresh nonce with the same
        // key replays, while a changed payload under that key is a conflict.
        let graph_sql = format!("INSERT INTO nodes (id, type) VALUES ('{node_id}', 'first')");
        let graph_first = response(
            &execute_request(
                &session,
                &signed_req(&graph_sql, 10, "system", "graph-nonce-1", "graph-key"),
            )
            .await,
        );
        assert_eq!(graph_first["rows_affected"], 1, "{graph_first}");

        // Close and reopen the authoritative backend before simulating the
        // lost acknowledgement. The fresh nonce must reach the durable replay
        // authority; an in-memory response cache cannot satisfy this attempt.
        let backend = reopen_durable_test_backend(&state, backend, &persist_dir).await;
        let graph_replay = response(
            &execute_request(
                &session,
                &signed_req(&graph_sql, 11, "system", "graph-nonce-2", "graph-key"),
            )
            .await,
        );
        assert_eq!(graph_replay["rows_affected"], 1, "{graph_replay}");
        let graph_changed = response(
            &execute_request(
                &session,
                &signed_req(
                    &format!("INSERT INTO nodes (id, type) VALUES ('{node_id}', 'changed')"),
                    12,
                    "system",
                    "graph-nonce-3",
                    "graph-key",
                ),
            )
            .await,
        );
        assert!(
            graph_changed["error"]["message"]
                .as_str()
                .is_some_and(|message| message.contains("IDEMPOTENCY_CONFLICT")),
            "changed graph payload must conflict: {graph_changed}"
        );

        let create = response(
            &execute_request(
                &session,
                &signed_req(
                    &format!("CREATE TABLE {table} (id INTEGER PRIMARY KEY, value TEXT)"),
                    20,
                    "system",
                    "table-create-nonce",
                    "table-create-key",
                ),
            )
            .await,
        );
        assert_eq!(create["tag"], "CREATE TABLE", "{create}");

        // Two fresh attempts with the same stable authority but distinct keys
        // both execute on one persistent session.
        for (id, nonce, key, value) in [
            (21, "distinct-nonce-1", "distinct-key-1", "one"),
            (22, "distinct-nonce-2", "distinct-key-2", "two"),
        ] {
            let result = response(
                &execute_request(
                    &session,
                    &signed_req(
                        &format!("INSERT INTO {table} (id, value) VALUES ({id}, '{value}')"),
                        id,
                        "system",
                        nonce,
                        key,
                    ),
                )
                .await,
            );
            assert_eq!(result["rows_affected"], 1, "{result}");
        }

        // A different verified actor cannot take over the already-bound
        // connection, even when its envelope is otherwise valid.
        let identity_change = response(
            &execute_request(
                &session,
                &signed_req(
                    "PRAGMA foreign_keys = ON",
                    23,
                    "peer",
                    "identity-change-nonce",
                    "identity-change-key",
                ),
            )
            .await,
        );
        assert_eq!(
            identity_change["error"]["code"], "28000",
            "{identity_change}"
        );

        let exact = signed_req(
            &format!("INSERT INTO {table} (id, value) VALUES (24, 'exact')"),
            24,
            "system",
            "exact-nonce",
            "exact-key",
        );
        let exact_first = response(&execute_request(&session, &exact).await);
        assert_eq!(exact_first["rows_affected"], 1, "{exact_first}");
        let exact_replay = response(&execute_request(&session, &exact).await);
        assert!(
            exact_replay["error"]["message"]
                .as_str()
                .is_some_and(|message| message.contains("REPLAY_NONCE_CONSUMED")),
            "exact nonce replay must be rejected: {exact_replay}"
        );

        let lost_ack_sql = format!("INSERT INTO {table} (id, value) VALUES (25, 'lost-ack')");
        let lost_ack_first = response(
            &execute_request(
                &session,
                &signed_req(
                    &lost_ack_sql,
                    25,
                    "system",
                    "lost-ack-nonce-1",
                    "lost-ack-key",
                ),
            )
            .await,
        );
        assert_eq!(lost_ack_first["rows_affected"], 1, "{lost_ack_first}");
        let lost_ack_retry = response(
            &execute_request(
                &session,
                &signed_req(
                    &lost_ack_sql,
                    26,
                    "system",
                    "lost-ack-nonce-2",
                    "lost-ack-key",
                ),
            )
            .await,
        );
        assert_eq!(lost_ack_retry["rows_affected"], 1, "{lost_ack_retry}");

        let changed_key_sql = format!("INSERT INTO {table} (id, value) VALUES (27, 'changed')");
        let changed_key_first = response(
            &execute_request(
                &session,
                &signed_req(
                    &changed_key_sql,
                    27,
                    "system",
                    "changed-key-nonce-1",
                    "changed-key",
                ),
            )
            .await,
        );
        assert_eq!(changed_key_first["rows_affected"], 1, "{changed_key_first}");
        let changed_key_conflict = response(
            &execute_request(
                &session,
                &signed_req(
                    &format!("INSERT INTO {table} (id, value) VALUES (28, 'different')"),
                    28,
                    "system",
                    "changed-key-nonce-2",
                    "changed-key",
                ),
            )
            .await,
        );
        assert!(
            changed_key_conflict["error"]["message"]
                .as_str()
                .is_some_and(|message| message.contains("IDEMPOTENCY_CONFLICT")),
            "changed table payload must conflict: {changed_key_conflict}"
        );

        let rows = response(
            &execute_request(
                &session,
                &signed_req(
                    &format!("SELECT id, value FROM {table} ORDER BY id"),
                    29,
                    "system",
                    "select-nonce",
                    "select-key",
                ),
            )
            .await,
        );
        assert_eq!(
            rows["rows"].as_array().map(|rows| rows.len()),
            Some(5),
            "{rows}"
        );

        let _ = execute_request(
            &session,
            &signed_req(
                &format!("DROP TABLE {table}"),
                30,
                "system",
                "drop-nonce",
                "drop-key",
            ),
        )
        .await;
        backend.shutdown();
        state.write().await.persistence = None;
    }

    #[cfg(feature = "redb")]
    #[tokio::test]
    async fn signed_sqlite_explicit_commits_replay_by_commit_key_after_reconnect() {
        #[cfg(feature = "security")]
        let _env_lock = crate::crypto::acquire_test_env_lock().await;
        let (state, backend, persist_dir) = durable_test_state().await;
        let table = unique_table();
        let graph_node = format!("sqlite_explicit_graph_{table}");
        let mixed_node = format!("sqlite_explicit_mixed_{table}");
        let changed_node = format!("sqlite_explicit_changed_{table}");

        let first = test_session(&state);
        let begin = response(
            &execute_request(
                &first,
                &signed_req("BEGIN", 100, "system", "explicit-begin-1", "begin-1"),
            )
            .await,
        );
        assert_eq!(begin["tag"], "BEGIN", "{begin}");
        let staged = response(
            &execute_request(
                &first,
                &signed_req(
                    &format!("INSERT INTO nodes (id, type) VALUES ('{graph_node}', 'first')"),
                    101,
                    "system",
                    "explicit-statement-1",
                    "statement-1",
                ),
            )
            .await,
        );
        assert_eq!(staged["rows_affected"], 1, "{staged}");
        let committed = response(
            &execute_request(
                &first,
                &signed_req("COMMIT", 102, "system", "explicit-commit-1", "commit-key"),
            )
            .await,
        );
        assert_eq!(committed["tag"], "COMMIT", "{committed}");

        // Reopen the authoritative store, then rebuild the transaction on a
        // fresh signed SQLite session. Fresh nonce and statement keys change
        // attempt metadata, while the COMMIT key reconstructs the same durable
        // operation id and must replay from the persistence kernel.
        let backend = reopen_durable_test_backend(&state, backend, &persist_dir).await;
        let retry = test_session(&state);
        let _ = execute_request(
            &retry,
            &signed_req("BEGIN", 110, "system", "explicit-begin-2", "begin-2"),
        )
        .await;
        let _ = execute_request(
            &retry,
            &signed_req(
                &format!("INSERT INTO nodes (id, type) VALUES ('{graph_node}', 'first')"),
                111,
                "system",
                "explicit-statement-2",
                "statement-2",
            ),
        )
        .await;
        let replay = response(
            &execute_request(
                &retry,
                &signed_req("COMMIT", 112, "system", "explicit-commit-2", "commit-key"),
            )
            .await,
        );
        assert_eq!(replay["tag"], "COMMIT", "{replay}");

        // A changed graph payload with the same commit key is a durable
        // conflict, so reconnect cannot silently apply a second operation.
        let changed = test_session(&state);
        let _ = execute_request(
            &changed,
            &signed_req("BEGIN", 120, "system", "explicit-begin-3", "begin-3"),
        )
        .await;
        let _ = execute_request(
            &changed,
            &signed_req(
                &format!("INSERT INTO nodes (id, type) VALUES ('{graph_node}', 'changed')"),
                121,
                "system",
                "explicit-statement-3",
                "statement-3",
            ),
        )
        .await;
        let changed_commit = response(
            &execute_request(
                &changed,
                &signed_req("COMMIT", 122, "system", "explicit-commit-3", "commit-key"),
            )
            .await,
        );
        assert!(
            changed_commit["error"]["message"]
                .as_str()
                .is_some_and(|message| message.contains("IDEMPOTENCY_CONFLICT")),
            "changed explicit graph payload must conflict: {changed_commit}"
        );

        let setup = test_session(&state);
        let create = response(
            &execute_request(
                &setup,
                &signed_req(
                    &format!("CREATE TABLE {table} (id INTEGER PRIMARY KEY, note TEXT)"),
                    130,
                    "system",
                    "explicit-create-1",
                    "create-key",
                ),
            )
            .await,
        );
        assert_eq!(create["tag"], "CREATE TABLE", "{create}");

        let mixed = test_session(&state);
        let _ = execute_request(
            &mixed,
            &signed_req("BEGIN", 140, "system", "mixed-begin-1", "mixed-begin-1"),
        )
        .await;
        let _ = execute_request(
            &mixed,
            &signed_req(
                &format!("INSERT INTO nodes (id, type) VALUES ('{mixed_node}', 'first')"),
                141,
                "system",
                "mixed-node-1",
                "mixed-node-key-1",
            ),
        )
        .await;
        let _ = execute_request(
            &mixed,
            &signed_req(
                &format!("INSERT INTO {table} (id, note) VALUES (1, 'paired')"),
                142,
                "system",
                "mixed-table-1",
                "mixed-table-key-1",
            ),
        )
        .await;
        let mixed_commit = response(
            &execute_request(
                &mixed,
                &signed_req(
                    "COMMIT",
                    143,
                    "system",
                    "mixed-commit-1",
                    "mixed-commit-key",
                ),
            )
            .await,
        );
        assert_eq!(mixed_commit["tag"], "COMMIT", "{mixed_commit}");

        // The same graph+table recipe on a new connection reuses the persisted
        // operation identity. This is the reconnect analogue of recovery after
        // the intent's table phase has already landed.
        let mixed_retry = test_session(&state);
        let _ = execute_request(
            &mixed_retry,
            &signed_req("BEGIN", 150, "system", "mixed-begin-2", "mixed-begin-2"),
        )
        .await;
        let _ = execute_request(
            &mixed_retry,
            &signed_req(
                &format!("INSERT INTO nodes (id, type) VALUES ('{mixed_node}', 'first')"),
                151,
                "system",
                "mixed-node-2",
                "mixed-node-key-2",
            ),
        )
        .await;
        let _ = execute_request(
            &mixed_retry,
            &signed_req(
                &format!("INSERT INTO {table} (id, note) VALUES (1, 'paired')"),
                152,
                "system",
                "mixed-table-2",
                "mixed-table-key-2",
            ),
        )
        .await;
        let mixed_replay = response(
            &execute_request(
                &mixed_retry,
                &signed_req(
                    "COMMIT",
                    153,
                    "system",
                    "mixed-commit-2",
                    "mixed-commit-key",
                ),
            )
            .await,
        );
        assert_eq!(mixed_replay["tag"], "COMMIT", "{mixed_replay}");

        // Rebuild the same graph recipe but change only the table payload
        // under the same commit key. The graph phase is an exact durable
        // replay; the table phase must report the conflict without running
        // graph compensation, which would remove the original graph result.
        let mixed_table_changed = test_session(&state);
        let _ = execute_request(
            &mixed_table_changed,
            &signed_req(
                "BEGIN",
                154,
                "system",
                "mixed-begin-table-change",
                "mixed-begin-table-change",
            ),
        )
        .await;
        let _ = execute_request(
            &mixed_table_changed,
            &signed_req(
                &format!("INSERT INTO nodes (id, type) VALUES ('{mixed_node}', 'first')"),
                155,
                "system",
                "mixed-node-table-change",
                "mixed-node-table-change",
            ),
        )
        .await;
        let _ = execute_request(
            &mixed_table_changed,
            &signed_req(
                &format!("INSERT INTO {table} (id, note) VALUES (2, 'different')"),
                156,
                "system",
                "mixed-table-change",
                "mixed-table-change",
            ),
        )
        .await;
        let mixed_table_conflict = response(
            &execute_request(
                &mixed_table_changed,
                &signed_req(
                    "COMMIT",
                    157,
                    "system",
                    "mixed-commit-table-change",
                    "mixed-commit-key",
                ),
            )
            .await,
        );
        assert!(
            mixed_table_conflict["error"]["message"]
                .as_str()
                .is_some_and(|message| message.contains("IDEMPOTENCY_CONFLICT")),
            "same graph plus changed table payload must conflict: {mixed_table_conflict}"
        );
        let mixed_after_conflict = test_session(&state);
        let graph_after_conflict = response(
            &execute_request(
                &mixed_after_conflict,
                &signed_req(
                    &format!("SELECT id, type FROM nodes WHERE id = '{mixed_node}'"),
                    158,
                    "system",
                    "mixed-after-graph",
                    "mixed-after-graph",
                ),
            )
            .await,
        );
        assert_eq!(
            graph_after_conflict["rows"]
                .as_array()
                .map(|rows| rows.len()),
            Some(1),
            "same graph replay must remain committed: {graph_after_conflict}"
        );
        assert_eq!(
            graph_after_conflict["rows"][0][1], "first",
            "same graph replay must preserve its original payload: {graph_after_conflict}"
        );
        let table_after_conflict = response(
            &execute_request(
                &mixed_after_conflict,
                &signed_req(
                    &format!("SELECT id, note FROM {table} ORDER BY id"),
                    159,
                    "system",
                    "mixed-after-table",
                    "mixed-after-table",
                ),
            )
            .await,
        );
        assert_eq!(
            table_after_conflict["rows"]
                .as_array()
                .map(|rows| rows.len()),
            Some(1),
            "changed table payload must not be applied: {table_after_conflict}"
        );
        assert_eq!(
            table_after_conflict["rows"][0][0], 1,
            "the original table row must remain: {table_after_conflict}"
        );
        assert_eq!(
            table_after_conflict["rows"][0][1], "paired",
            "the original table payload must remain: {table_after_conflict}"
        );

        let mixed_changed = test_session(&state);
        let _ = execute_request(
            &mixed_changed,
            &signed_req("BEGIN", 160, "system", "mixed-begin-3", "mixed-begin-3"),
        )
        .await;
        let _ = execute_request(
            &mixed_changed,
            &signed_req(
                &format!("INSERT INTO nodes (id, type) VALUES ('{changed_node}', 'changed')"),
                161,
                "system",
                "mixed-node-3",
                "mixed-node-key-3",
            ),
        )
        .await;
        let _ = execute_request(
            &mixed_changed,
            &signed_req(
                &format!("INSERT INTO {table} (id, note) VALUES (2, 'different')"),
                162,
                "system",
                "mixed-table-3",
                "mixed-table-key-3",
            ),
        )
        .await;
        let mixed_conflict = response(
            &execute_request(
                &mixed_changed,
                &signed_req(
                    "COMMIT",
                    163,
                    "system",
                    "mixed-commit-3",
                    "mixed-commit-key",
                ),
            )
            .await,
        );
        assert!(
            mixed_conflict["error"]["message"]
                .as_str()
                .is_some_and(|message| message.contains("IDEMPOTENCY_CONFLICT")),
            "changed explicit mixed payload must conflict: {mixed_conflict}"
        );

        let _ = execute_request(
            &setup,
            &signed_req(
                &format!("DROP TABLE {table}"),
                170,
                "system",
                "explicit-drop",
                "explicit-drop-key",
            ),
        )
        .await;
        backend.shutdown();
        state.write().await.persistence = None;
    }

    /// A malformed request and an engine error both surface as `{"error":{code,message}}`.
    #[tokio::test]
    async fn errors_are_reported_as_json() {
        // Reads the ambient encryption env at its durable open, so the env must hold
        // still for this whole body. READ guard: it excludes only a key MUTATOR, never
        // another opener. See `crate::crypto::acquire_test_env_read_lock`'s doc.
        let _env_read_lock = crate::crypto::acquire_test_env_read_lock().await;
        let state = test_state();
        let session = test_session(&state);

        let bad_json = execute_request(&session, "not json").await;
        let v: serde_json::Value = serde_json::from_str(&bad_json).unwrap();
        assert!(
            v["error"]["code"].is_string(),
            "malformed JSON → error: {bad_json}"
        );

        let no_sql = execute_request(&session, r#"{"query":"SELECT 1"}"#).await;
        let v: serde_json::Value = serde_json::from_str(&no_sql).unwrap();
        assert!(
            v["error"].is_object(),
            "missing sql field → error: {no_sql}"
        );

        let engine_err = execute_request(&session, &req("SELECT * FROM no_such_table")).await;
        let v: serde_json::Value = serde_json::from_str(&engine_err).unwrap();
        assert!(
            v["error"]["message"].is_string(),
            "engine error → JSON error: {engine_err}"
        );
    }

    /// The SERVED round-trip: bind a real TCP listener, run `serve`, connect a raw
    /// socket, and exercise the NDJSON framing (CREATE / INSERT / SELECT-with-`||` /
    /// PRAGMA) over the wire — proving the whole surface, not just its core function.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn served_ndjson_round_trip_over_tcp() {
        // Reads the ambient encryption env at its durable open, so the env must hold
        // still for this whole body. READ guard: it excludes only a key MUTATOR, never
        // another opener. See `crate::crypto::acquire_test_env_read_lock`'s doc.
        let _env_read_lock = crate::crypto::acquire_test_env_read_lock().await;
        let state = test_state();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            serve(listener, state).await;
        });

        let stream = tokio::net::TcpStream::connect(addr).await.unwrap();
        let (rh, mut wh) = stream.into_split();
        let mut reader = BufReader::new(rh);

        // A tiny NDJSON client: write one request line, read one response line.
        async fn call(
            wh: &mut tokio::net::tcp::OwnedWriteHalf,
            reader: &mut BufReader<tokio::net::tcp::OwnedReadHalf>,
            sql: &str,
        ) -> serde_json::Value {
            let line = format!("{}\n", req(sql));
            wh.write_all(line.as_bytes()).await.unwrap();
            let mut resp = String::new();
            reader.read_line(&mut resp).await.unwrap();
            serde_json::from_str(resp.trim()).unwrap()
        }

        let table = unique_table();
        let create = call(
            &mut wh,
            &mut reader,
            &format!("CREATE TABLE {table} (id INTEGER PRIMARY KEY AUTOINCREMENT, v TEXT)"),
        )
        .await;
        assert_eq!(create["tag"], "CREATE TABLE", "{create}");

        let ins = call(
            &mut wh,
            &mut reader,
            &format!("INSERT INTO {table} (v) VALUES ('hi')"),
        )
        .await;
        assert_eq!(ins["rows_affected"], 1, "{ins}");

        let sel = call(
            &mut wh,
            &mut reader,
            &format!("SELECT v || '!' AS g FROM {table}"),
        )
        .await;
        assert_eq!(sel["rows"][0][0], "hi!", "served || round-trip: {sel}");

        let pragma = call(&mut wh, &mut reader, "PRAGMA journal_mode = WAL").await;
        assert_eq!(pragma["tag"], "PRAGMA", "{pragma}");

        let _ = call(&mut wh, &mut reader, &format!("DROP TABLE {table}")).await;
    }
}
