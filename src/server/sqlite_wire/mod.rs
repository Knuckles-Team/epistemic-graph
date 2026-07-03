//! SQLite-compatible served surface (CONCEPT:EG-075) — Phase J of the Universal-DB
//! multi-wire program. The SECOND consumer of the wire-agnostic execution core
//! (CONCEPT:EG-074), after the Postgres shim (`crate::server::pgwire`).
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
//!      SHIPPED as `CONCEPT:EG-331`/`CONCEPT:EG-332` behind the SEPARATE `sqlite-file`
//!      feature (`src/server/handlers/sqlite_file.rs` + the `ImportSqliteFile`/
//!      `ExportSqliteFile` methods). A spec-correct SQLite b-tree file cannot be written
//!      without reimplementing the page format (the blocker below), so `sqlite-file`
//!      pulls `rusqlite` with the BUNDLED C sqlite3 — and is therefore kept OUT of
//!      `pi`/`default` (folded only into `full`/`node`), so the Pi contract still holds:
//!      a `--features pi` build links NO rusqlite/libsqlite3-sys. That C leg is gated to
//!      the non-Pi tiers ONLY; the `sqlite-wire` surface here stays pure-Rust.
//!
//! ## The wire-neutral promise (CONCEPT:EG-074)
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
//!   * request:  `{"sql": "<one statement>"}`
//!   * rows:     `{"columns":[{"name":..,"type":..}, …], "rows":[[..], …]}`
//!   * command:  `{"tag":"INSERT", "rows_affected": 1}`  (`rows_affected` omitted when none)
//!   * txn:      `{"tag":"BEGIN"|"COMMIT"|"ROLLBACK"}`
//!   * pragma:   `{"tag":"PRAGMA"}`  (a no-op ack)
//!   * error:    `{"error":{"code":"58000","message":"…"}}`
//!
//! Column `type` is reported as the SQLite storage-class name (`INTEGER`/`REAL`/`TEXT`)
//! so a SQLite-minded client sees familiar types.
//!
//! ## `.db` file export/import — SHIPPED (CONCEPT:EG-331/EG-332)
//! Delivered in `src/server/handlers/sqlite_file.rs` behind the `sqlite-file` feature:
//! import reads a `.db`'s tables+rows into the `TableStore`; export writes a `TableStore`
//! selection out to a valid `sqlite3`-readable `.db`. Serializing the SQLite b-tree page
//! format WITHOUT a C dependency proved infeasible (a pure-Rust writer would reimplement
//! the whole format), so the feature pulls the BUNDLED C sqlite3 via `rusqlite` and is
//! kept OUT of `pi` (folded only into `full`/`node`) — the Pi contract is preserved.

use std::sync::Arc;

use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;
use tokio::sync::RwLock;

use eg_query::PgColType;

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

/// The largest single request line accepted (guards against an unbounded line flood on
/// the persistent connection). One SQL statement per line; 8 MiB is generous.
const MAX_LINE_BYTES: u64 = 8 * 1024 * 1024;

/// Serve the SQLite-dialect NDJSON surface on `listener`, backed by the engine `state`.
/// One [`WireSession`] per accepted connection (so `SET graph` / transaction state stays
/// isolated per connection), exactly like a pgwire connection. Runs until the process
/// exits. Spawned by `main.rs` only when built `--features sqlite-wire` AND
/// `EPISTEMIC_GRAPH_SQLITE_ADDR` is set.
pub async fn serve(listener: TcpListener, state: Arc<RwLock<ServerState>>) {
    let default_graph =
        std::env::var(SQLITE_GRAPH_ENV).unwrap_or_else(|_| "__commons__".to_string());
    loop {
        let Ok((stream, _peer)) = listener.accept().await else {
            continue;
        };
        // A trust/interop surface: the connection is anonymous (no handshake identity),
        // so `auth_maps_actor = false` — the same back-compat single-tenant behavior the
        // SPARQL/native surfaces use when no identity is presented.
        let session = Arc::new(WireSession::new(
            state.clone(),
            default_graph.clone(),
            false,
        ));
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
    let req: serde_json::Value = match serde_json::from_str(line) {
        Ok(v) => v,
        Err(e) => return error_json("22000", &format!("malformed JSON request: {e}")),
    };
    let Some(sql) = req.get("sql").and_then(|v| v.as_str()) else {
        return error_json(
            "22000",
            "request must be a JSON object with a string `sql` field",
        );
    };

    match translate_sqlite_sql(sql) {
        // A PRAGMA is a no-op ack — the engine is never touched.
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
    use dashmap::DashMap;
    use tokio::sync::Semaphore;

    use crate::channels::ChannelManager;
    use crate::isolation::IsolationLayer;
    use crate::registry::GraphRegistry;
    use crate::server::txn::TxnIdGen;

    /// A minimal `ServerState` with a pre-created `__commons__` graph (the registry
    /// creates it), mirroring `tests/pgwire_roundtrip.rs::state_with`. Feature-gated
    /// fields track whatever tier the test build enables.
    fn test_state() -> Arc<RwLock<ServerState>> {
        Arc::new(RwLock::new(ServerState {
            #[cfg(feature = "redb")]
            cold_tracker: Arc::new(
                crate::server::persistence::cold_offload::ColdTenantTracker::new(),
            ),
            registry: GraphRegistry::new(),
            isolation: IsolationLayer::new(),
            channels: ChannelManager::new(),
            auth_secret: "sqlite-wire-test".to_string(), // # sanitizer:ignore
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
            txn_id_gen: Arc::new(TxnIdGen::default()),
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
        }))
    }

    fn test_session(state: &Arc<RwLock<ServerState>>) -> WireSession {
        WireSession::new(state.clone(), "__commons__".to_string(), false)
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
        serde_json::json!({ "sql": sql }).to_string()
    }

    /// The full SQLite-dialect statement set from the correctness bar, executed IN
    /// PROCESS through `execute_request` against the shared `WireSession`: a
    /// `CREATE TABLE … INTEGER PRIMARY KEY AUTOINCREMENT`, `INSERT`s (id auto-assigned),
    /// a `SELECT` using `||` concatenation, and a `PRAGMA` no-op — proving the dialect
    /// translation + the served encoding end-to-end.
    #[tokio::test]
    async fn sqlite_dialect_statements_execute_through_wire_session() {
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

    /// A malformed request and an engine error both surface as `{"error":{code,message}}`.
    #[tokio::test]
    async fn errors_are_reported_as_json() {
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
            let line = format!("{}\n", serde_json::json!({ "sql": sql }));
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
