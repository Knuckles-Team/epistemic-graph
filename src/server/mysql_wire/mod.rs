//! MySQL / MariaDB wire-protocol listener (CONCEPT:EG-076) — the SECOND `WireProtocol`
//! adapter (CONCEPT:EG-074). A HAND-ROLLED MySQL client/server protocol that lets a
//! MySQL/MariaDB driver, ORM, or the `mysql` CLI connect and run SQL against a graph.
//!
//! ## What this is (and is NOT)
//! Like the pgwire shim, this is an ADAPTER, not a second SQL engine — and not even a
//! second copy of the query orchestration. The wire-agnostic `classify → dispatch →
//! exec` pipeline, the per-connection session state, and the mixed-store transaction
//! logic all live in [`crate::server::wire`] ([`WireSession`], the one shared
//! [`WireProtocol`] impl). THIS module is purely the MySQL-SPECIFIC adapter:
//!   * the TCP listener + the length-prefixed packet framing over `tokio::net`,
//!   * the Handshake v10 + `mysql_native_password` auth (see `auth.rs`), bridged to the
//!     engine secret / ACL identity exactly as pgwire's SCRAM path is (CONCEPT:KG-2.202),
//!   * the command phase (`COM_QUERY` / `COM_PING` / `COM_QUIT` / `COM_INIT_DB`),
//!   * the encoding of a wire-neutral [`WireOutcome`] / [`WireError`] into MySQL packets
//!     (column-count + column-definition + text rows + EOF/OK, or an ERR frame).
//!
//! It links NO server-side mysql protocol crate — every byte layout is hand-rolled in
//! `packets.rs` against the documented MySQL protocol (the Pi-contract idiom pgwire /
//! sparql_http use). A `COM_QUERY` runs straight through [`WireProtocol::execute`], so a
//! SELECT takes the SAME DataFusion path `Method::Sql` uses and a write is routed through
//! the engine's `GraphTxn` + durability path — no SQL grammar/planner/executor here.
//!
//! ## Protocol subset (CONCEPT:EG-076)
//! LANDED: connection phase (Handshake v10 + Handshake-Response-41), `mysql_native_password`
//! (or a trust/no-auth mode when no engine secret is set), and the TEXT-protocol command
//! phase — `COM_QUERY` (result-set OR OK/ERR), `COM_PING`, `COM_QUIT`, `COM_INIT_DB`
//! (mapped to `SET graph`). DEFERRED: the binary prepared-statement protocol
//! (`COM_STMT_PREPARE`/`_EXECUTE`), `LOAD DATA LOCAL INFILE`, TLS (`CLIENT_SSL`), and
//! compression — a driver negotiating those falls back to the text protocol, which is
//! the universally-supported path.

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use rand::Rng;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::RwLock;

use eg_query::PgColType;

use crate::server::wire::{WireError, WireOutcome, WireProtocol, WireSession};
use crate::server::ServerState;

mod auth;
mod packets;

pub use auth::{derive_mysql_password, MysqlAuthMode, MYSQL_AUTH_ENV};

use packets::{
    build_column_count, build_column_def, build_eof, build_err, build_handshake, build_ok,
    parse_handshake_response, put_text_row, CLIENT_CONNECT_WITH_DB, CLIENT_LONG_PASSWORD,
    CLIENT_PLUGIN_AUTH, CLIENT_PROTOCOL_41, CLIENT_SECURE_CONNECTION, CLIENT_TRANSACTIONS,
    SERVER_STATUS_AUTOCOMMIT, SERVER_STATUS_IN_TRANS,
};

/// Env var: when set (and the binary is built `--features mysql-wire`), the MySQL wire
/// listener binds this address (documented loopback `127.0.0.1:3306`). Unset ⇒ no listener.
pub const MYSQL_ADDR_ENV: &str = "EPISTEMIC_GRAPH_MYSQL_ADDR";
/// Env var: the default graph a fresh connection runs against when the MySQL `database`
/// (schema) is not supplied at connect / via `USE`. Defaults to `__commons__`.
pub const MYSQL_GRAPH_ENV: &str = "EPISTEMIC_GRAPH_MYSQL_GRAPH";

/// The advertised server version string (clients parse the leading `X.Y.Z`).
const SERVER_VERSION: &str = "8.0.0-epistemic-graph";

// MySQL command bytes (the subset this adapter handles).
const COM_QUIT: u8 = 0x01;
const COM_INIT_DB: u8 = 0x02;
const COM_QUERY: u8 = 0x03;
const COM_FIELD_LIST: u8 = 0x04;
const COM_PING: u8 = 0x0e;

static CONN_ID: AtomicU32 = AtomicU32::new(1);

fn next_conn_id() -> u32 {
    CONN_ID.fetch_add(1, Ordering::Relaxed)
}

/// The capability flags this server advertises (CONCEPT:EG-076). Protocol 4.1 with the
/// secure-connection + plugin-auth handshake, connect-with-DB, and transaction status
/// tracking. Deliberately NOT `CLIENT_DEPRECATE_EOF` — we send explicit EOF packets, the
/// universally-compatible framing.
fn server_capabilities() -> u32 {
    CLIENT_LONG_PASSWORD
        | CLIENT_PROTOCOL_41
        | CLIENT_TRANSACTIONS
        | CLIENT_SECURE_CONNECTION
        | CLIENT_PLUGIN_AUTH
        | CLIENT_CONNECT_WITH_DB
}

/// The current server status flags for an OK/EOF packet — reports the connection's
/// transaction state so a driver tracks BEGIN/COMMIT correctly.
fn status_flags(session: &WireSession) -> u16 {
    if session.in_txn() {
        SERVER_STATUS_IN_TRANS
    } else {
        SERVER_STATUS_AUTOCOMMIT
    }
}

// ── packet framing over tokio::io ──────────────────────────────────────────────

/// Read one COMPLETE logical message, reassembling the MySQL 16 MiB multi-packet split
/// (a payload of exactly `0xffffff` bytes signals a continuation frame). Returns the
/// sequence id of the LAST frame + the concatenated payload.
async fn read_message<S: AsyncRead + Unpin>(s: &mut S) -> std::io::Result<(u8, Vec<u8>)> {
    let mut out = Vec::new();
    let mut seq;
    loop {
        let mut hdr = [0u8; 4];
        s.read_exact(&mut hdr).await?;
        let len = u32::from_le_bytes([hdr[0], hdr[1], hdr[2], 0]) as usize;
        seq = hdr[3];
        let start = out.len();
        out.resize(start + len, 0);
        s.read_exact(&mut out[start..]).await?;
        if len < 0xff_ffff {
            break;
        }
    }
    Ok((seq, out))
}

/// Write one logical payload as one-or-more MySQL packets starting at `seq`, splitting a
/// payload ≥ 16 MiB into `0xffffff`-byte frames (with the required trailing empty frame
/// when the last chunk is exactly full). Returns the NEXT sequence id to use, so a
/// caller emitting a multi-packet response threads a contiguous sequence.
async fn write_packet<S: AsyncWrite + Unpin>(
    s: &mut S,
    seq: u8,
    payload: &[u8],
) -> std::io::Result<u8> {
    let mut seq = seq;
    let mut remaining = payload;
    loop {
        let take = remaining.len().min(0xff_ffff);
        let len = (take as u32).to_le_bytes();
        s.write_all(&[len[0], len[1], len[2], seq]).await?;
        s.write_all(&remaining[..take]).await?;
        seq = seq.wrapping_add(1);
        remaining = &remaining[take..];
        if take < 0xff_ffff {
            break;
        }
        if remaining.is_empty() {
            // A trailing empty packet terminates a stream whose last chunk was exactly full.
            s.write_all(&[0, 0, 0, seq]).await?;
            seq = seq.wrapping_add(1);
            break;
        }
    }
    Ok(seq)
}

// ── WireError / WireOutcome → MySQL packets ─────────────────────────────────────

/// Map a wire-neutral SQLSTATE to a MySQL error code. The SQLSTATE itself is preserved
/// verbatim in the ERR frame (via the `#` marker), so a client sees the exact
/// engine-produced state; the numeric code is a best-effort MySQL equivalent.
fn mysql_error_code(sqlstate: &str) -> u16 {
    match sqlstate {
        "42501" => 1142, // ER_TABLEACCESS_DENIED_ERROR (insufficient privilege)
        "25P02" => 1064, // aborted-txn — closest MySQL code is the generic parse/reject
        _ if sqlstate.starts_with("42") => 1064, // ER_PARSE_ERROR / access class
        _ => 1105,       // ER_UNKNOWN_ERROR
    }
}

/// A wire-neutral [`WireError`] → a MySQL ERR packet payload, preserving the SQLSTATE.
fn wire_err_to_packet(e: &WireError) -> Vec<u8> {
    build_err(mysql_error_code(&e.code), &e.code, &e.message)
}

/// Encode a wire-neutral [`WireOutcome`] into the ORDERED MySQL packet payloads that
/// follow a command (starting after the command's response sequence id). This is the ONE
/// place MySQL result framing is applied over the shared execution core:
///   * `Rows` → column-count packet + one column-definition per column + EOF + one
///     text-protocol row per row + a final EOF.
///   * `Command` → a single OK packet carrying the affected-row count.
///   * `TxnStart`/`TxnEnd` → an OK packet (the status flags carry the txn state).
///   * `CopyIn` → an ERR packet: `COPY … FROM STDIN` has no MySQL text-protocol analogue.
fn encode_outcome(outcome: WireOutcome, status: u16) -> Vec<Vec<u8>> {
    match outcome {
        WireOutcome::Rows(result) => {
            let mut packets = Vec::with_capacity(result.columns.len() + result.rows.len() + 3);
            packets.push(build_column_count(result.columns.len() as u64));
            for c in &result.columns {
                packets.push(build_column_def(&c.name, c.ty));
            }
            packets.push(build_eof(status));
            let types: Vec<PgColType> = result.columns.iter().map(|c| c.ty).collect();
            for row in &result.rows {
                packets.push(put_text_row(&types, row));
            }
            packets.push(build_eof(status));
            packets
        }
        WireOutcome::Command { tag: _, rows } => {
            vec![build_ok(rows.unwrap_or(0) as u64, 0, status, "")]
        }
        WireOutcome::TxnStart | WireOutcome::TxnEnd { .. } => {
            vec![build_ok(0, 0, status, "")]
        }
        WireOutcome::CopyIn { .. } => vec![build_err(
            1148,
            "42000",
            "COPY FROM STDIN is not supported over the MySQL wire (use INSERT)",
        )],
    }
}

// ── the per-connection protocol driver ──────────────────────────────────────────

/// Drive ONE MySQL connection through the connection phase (handshake + auth) then the
/// command phase, until the client quits or the socket closes. Generic over the byte
/// stream so an in-process test can drive it over any duplex transport.
async fn handle_connection<S>(
    s: &mut S,
    session: Arc<WireSession>,
    mode: MysqlAuthMode,
    secret: String,
    conn_id: u32,
) -> std::io::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    // ── connection phase: send Handshake v10 with a fresh 20-byte scramble ──────
    let mut seed = [0u8; 20];
    {
        let mut rng = rand::thread_rng();
        // Keep scramble bytes in 1..=0x7e (never NUL, printable-ish) — the classic
        // server behavior, and NUL would truncate the auth-plugin-data string.
        for b in seed.iter_mut() {
            *b = rng.gen_range(1..=0x7e);
        }
    }
    let caps = server_capabilities();
    write_packet(s, 0, &build_handshake(conn_id, &seed, caps, SERVER_VERSION)).await?;

    // ── read the Handshake Response ─────────────────────────────────────────────
    let (rseq, payload) = read_message(s).await?;
    let resp = match parse_handshake_response(&payload) {
        Some(r) => r,
        None => {
            write_packet(
                s,
                rseq.wrapping_add(1),
                &build_err(1043, "08S01", "Bad handshake (protocol 4.1 required)"),
            )
            .await?;
            return Ok(());
        }
    };

    // ── authenticate (CONCEPT:KG-2.202) ─────────────────────────────────────────
    let authed = match mode {
        MysqlAuthMode::Trust => true,
        MysqlAuthMode::Native => {
            auth::verify_login(&secret, &resp.username, &resp.auth_response, &seed)
        }
    };
    if !authed {
        write_packet(
            s,
            rseq.wrapping_add(1),
            &build_err(
                1045,
                "28000",
                &format!("Access denied for user '{}'", resp.username),
            ),
        )
        .await?;
        return Ok(());
    }

    // Latch the connection's startup identity: the authenticated `user` → the ACL actor
    // (only under an authenticating mode), and the connect `database` → the target graph
    // (the SAME rules pgwire's first-query latch uses).
    session.resolve_startup(Some(resp.username.clone()), resp.database.clone());
    write_packet(
        s,
        rseq.wrapping_add(1),
        &build_ok(0, 0, SERVER_STATUS_AUTOCOMMIT, ""),
    )
    .await?;

    // ── command phase ───────────────────────────────────────────────────────────
    loop {
        let (cmd_seq, payload) = match read_message(s).await {
            Ok(v) => v,
            Err(_) => break, // client closed the socket
        };
        if payload.is_empty() {
            break;
        }
        let cmd = payload[0];
        let arg = &payload[1..];
        let next = cmd_seq.wrapping_add(1);
        match cmd {
            COM_QUIT => break,
            COM_PING => {
                write_packet(s, next, &build_ok(0, 0, status_flags(&session), "")).await?;
            }
            COM_INIT_DB => {
                // Map the MySQL "database" to the engine graph via the shared session.
                let db = String::from_utf8_lossy(arg);
                let stmt = format!("SET graph = '{}'", db.replace('\'', "''"));
                match session.execute(&stmt).await {
                    Ok(_) => {
                        write_packet(s, next, &build_ok(0, 0, status_flags(&session), "")).await?;
                    }
                    Err(e) => {
                        write_packet(s, next, &wire_err_to_packet(&e)).await?;
                    }
                }
            }
            COM_QUERY => {
                let sql = String::from_utf8_lossy(arg).into_owned();
                match session.execute(&sql).await {
                    Ok(outcome) => {
                        let status = status_flags(&session);
                        let mut seq = next;
                        for p in encode_outcome(outcome, status) {
                            seq = write_packet(s, seq, &p).await?;
                        }
                    }
                    Err(e) => {
                        write_packet(s, next, &wire_err_to_packet(&e)).await?;
                    }
                }
            }
            COM_FIELD_LIST => {
                // Minimal: report an empty field list (a bare EOF). Enough for clients
                // that probe a table's columns before falling back to a SELECT.
                write_packet(s, next, &build_eof(status_flags(&session))).await?;
            }
            other => {
                write_packet(
                    s,
                    next,
                    &build_err(1047, "08S01", &format!("Unknown command {other}")),
                )
                .await?;
            }
        }
    }
    Ok(())
}

// ── the listener ────────────────────────────────────────────────────────────────

/// Bind `addr` and serve MySQL-wire connections until the process exits. Spawned by
/// `main.rs` only when built `--features mysql-wire` AND `EPISTEMIC_GRAPH_MYSQL_ADDR`
/// is set. The auth mode is resolved once from the engine secret + env, mirroring pgwire.
pub async fn serve(addr: &str, state: Arc<RwLock<ServerState>>) -> std::io::Result<()> {
    let auth_secret = state.read().await.auth_secret.clone();
    let mode = MysqlAuthMode::resolve(&auth_secret);
    serve_with_auth(addr, state, mode).await
}

/// `serve` with an EXPLICIT auth mode (CONCEPT:EG-076 / KG-2.202). `serve` resolves the
/// mode from the env + engine secret and delegates here; tests call this directly to pin
/// trust vs native deterministically without a process-global env toggle.
pub async fn serve_with_auth(
    addr: &str,
    state: Arc<RwLock<ServerState>>,
    mode: MysqlAuthMode,
) -> std::io::Result<()> {
    let default_graph =
        std::env::var(MYSQL_GRAPH_ENV).unwrap_or_else(|_| "__commons__".to_string());
    let auth_secret = state.read().await.auth_secret.clone();
    let listener = TcpListener::bind(addr).await?;
    tracing::info!(
        "mysql-wire: serving MySQL/MariaDB wire protocol on {} (default graph '{}', auth={})",
        addr,
        default_graph,
        mode.as_str()
    );
    loop {
        let (socket, peer) = listener.accept().await?;
        // A FRESH session per connection so each connection's `SET graph` / txn / actor
        // stays isolated (mirrors pgwire's per-connection backend).
        let session = Arc::new(WireSession::new(
            state.clone(),
            default_graph.clone(),
            mode.maps_actor(),
        ));
        let secret = auth_secret.clone();
        let conn_id = next_conn_id();
        tokio::spawn(async move {
            let mut socket = socket;
            if let Err(e) = handle_connection(&mut socket, session, mode, secret, conn_id).await {
                tracing::debug!("mysql-wire connection from {peer} ended: {e}");
            }
        });
    }
}

#[cfg(test)]
mod tests {
    //! Encoder unit tests + an in-process listener smoke test that drives the real
    //! listener through a full handshake and hand-built `COM_QUERY` packets over a real
    //! TCP socket (no mysql client crate) — proving SELECT / CREATE TABLE / INSERT
    //! end-to-end through the shared `WireSession` execution core (CONCEPT:EG-076).
    use super::*;
    use crate::channels::ChannelManager;
    use crate::isolation::IsolationLayer;
    use crate::registry::GraphRegistry;
    use crate::server::ServerState;
    use dashmap::DashMap;
    use eg_query::{TypedColumn, TypedQueryResult};
    use tokio::net::TcpStream;
    use tokio::sync::Semaphore;

    #[test]
    fn encode_rows_outcome_frames_columns_then_rows() {
        let result = TypedQueryResult {
            columns: vec![
                TypedColumn {
                    name: "id".into(),
                    ty: PgColType::Text,
                },
                TypedColumn {
                    name: "rank".into(),
                    ty: PgColType::Int8,
                },
            ],
            rows: vec![
                vec![serde_json::json!("n1"), serde_json::json!(1)],
                vec![serde_json::json!("n2"), serde_json::json!(2)],
            ],
        };
        let packets = encode_outcome(WireOutcome::Rows(result), SERVER_STATUS_AUTOCOMMIT);
        // column-count + 2 defs + EOF + 2 rows + EOF = 7 packets.
        assert_eq!(packets.len(), 7);
        assert_eq!(packets[0][0], 2, "column count = 2");
        assert_eq!(packets[3][0], 0xfe, "EOF after column defs");
        assert_eq!(packets[6][0], 0xfe, "trailing EOF after rows");
    }

    #[test]
    fn encode_command_outcome_is_ok_with_affected_rows() {
        let packets = encode_outcome(
            WireOutcome::command_rows("INSERT", 3),
            SERVER_STATUS_AUTOCOMMIT,
        );
        assert_eq!(packets.len(), 1);
        assert_eq!(packets[0][0], 0x00, "OK header");
        assert_eq!(packets[0][1], 3, "affected rows lenenc = 3");
    }

    /// A minimal `ServerState` seeded with three nodes so a wire SELECT over `nodes`
    /// returns rows. Mirrors the pgwire round-trip test's `state_with`.
    fn seeded_state() -> Arc<RwLock<ServerState>> {
        let registry = GraphRegistry::new();
        {
            let core = registry.get("__commons__").unwrap().core.clone();
            for (id, ty, rank) in [("n1", "Agent", 1i64), ("n2", "Agent", 2), ("n3", "Tool", 3)] {
                let blob =
                    rmp_serde::to_vec_named(&serde_json::json!({"type": ty, "rank": rank})).unwrap();
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
        }))
    }

    /// A hand-built MySQL client: complete the handshake (trust mode → empty auth) and
    /// return the connected stream ready for the command phase.
    async fn client_connect(addr: &str) -> TcpStream {
        let mut stream = TcpStream::connect(addr).await.unwrap();
        // Read the server Handshake (seq 0); we don't need to validate the scramble in
        // trust mode.
        let (seq, hs) = read_message(&mut stream).await.unwrap();
        assert_eq!(hs[0], 10, "server sends Handshake v10");
        let caps = CLIENT_PROTOCOL_41 | CLIENT_SECURE_CONNECTION | CLIENT_PLUGIN_AUTH;
        let resp = packets::build_handshake_response(caps, "tester", &[], None);
        write_packet(&mut stream, seq.wrapping_add(1), &resp)
            .await
            .unwrap();
        let (_seq, ok) = read_message(&mut stream).await.unwrap();
        assert_eq!(ok[0], 0x00, "auth OK");
        stream
    }

    /// Send a `COM_QUERY` and return the decoded outcome: either the affected-row count
    /// (OK), or the parsed text rows (a result set). Panics on an ERR frame.
    async fn client_query(stream: &mut TcpStream, sql: &str) -> QueryResult {
        let mut pkt = vec![COM_QUERY];
        pkt.extend_from_slice(sql.as_bytes());
        // A new command resets the sequence to 0.
        write_packet(stream, 0, &pkt).await.unwrap();
        let (_seq, first) = read_message(stream).await.unwrap();
        match first[0] {
            0xff => {
                let code = u16::from_le_bytes([first[1], first[2]]);
                let msg = String::from_utf8_lossy(&first[9..]).into_owned();
                panic!("query `{sql}` returned ERR {code}: {msg}");
            }
            0x00 => {
                // OK: affected_rows is the first lenenc int after the header.
                let mut pos = 1usize;
                let affected = packets::get_lenenc_int(&first, &mut pos).unwrap_or(0);
                QueryResult::Ok { affected }
            }
            _ => {
                // Result set: first packet is the column count.
                let mut pos = 0usize;
                let ncols = packets::get_lenenc_int(&first, &mut pos).unwrap() as usize;
                let mut col_names = Vec::with_capacity(ncols);
                for _ in 0..ncols {
                    let (_s, def) = read_message(stream).await.unwrap();
                    col_names.push(read_col_def_name(&def));
                }
                // EOF after column definitions.
                let (_s, eof) = read_message(stream).await.unwrap();
                assert_eq!(eof[0], 0xfe, "EOF after column defs");
                // Rows until the terminating EOF (0xfe header, short packet).
                let mut rows = Vec::new();
                loop {
                    let (_s, p) = read_message(stream).await.unwrap();
                    if p[0] == 0xfe && p.len() < 9 {
                        break; // EOF
                    }
                    rows.push(parse_text_row(&p, ncols));
                }
                QueryResult::Rows {
                    columns: col_names,
                    rows,
                }
            }
        }
    }

    #[derive(Debug)]
    enum QueryResult {
        Ok { affected: u64 },
        Rows {
            columns: Vec<String>,
            rows: Vec<Vec<Option<String>>>,
        },
    }

    /// Extract the `name` column (the 5th lenenc string) from a column-definition packet.
    fn read_col_def_name(def: &[u8]) -> String {
        let mut pos = 0usize;
        for _ in 0..4 {
            let len = packets::get_lenenc_int(def, &mut pos).unwrap() as usize;
            pos += len; // catalog, schema, table, org_table
        }
        let len = packets::get_lenenc_int(def, &mut pos).unwrap() as usize;
        String::from_utf8_lossy(&def[pos..pos + len]).into_owned()
    }

    /// Parse a text-protocol result row into `ncols` optional strings (NULL = `0xfb`).
    fn parse_text_row(p: &[u8], ncols: usize) -> Vec<Option<String>> {
        let mut pos = 0usize;
        let mut out = Vec::with_capacity(ncols);
        for _ in 0..ncols {
            if p.get(pos) == Some(&0xfb) {
                out.push(None);
                pos += 1;
                continue;
            }
            let len = packets::get_lenenc_int(p, &mut pos).unwrap() as usize;
            out.push(Some(String::from_utf8_lossy(&p[pos..pos + len]).into_owned()));
            pos += len;
        }
        out
    }

    async fn spawn_listener(state: Arc<RwLock<ServerState>>) -> String {
        let probe = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = probe.local_addr().unwrap().to_string();
        drop(probe);
        let serve_addr = addr.clone();
        tokio::spawn(async move {
            let _ = serve_with_auth(&serve_addr, state, MysqlAuthMode::Trust).await;
        });
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        addr
    }

    #[tokio::test]
    async fn mysql_wire_roundtrip_select_create_insert() {
        let addr = spawn_listener(seeded_state()).await;
        let mut c = client_connect(&addr).await;

        // 1) SELECT over the seeded graph `nodes` — proves the read path + result-set
        //    framing end-to-end through the shared WireSession.
        match client_query(&mut c, "SELECT id FROM nodes").await {
            QueryResult::Rows { columns, rows } => {
                assert_eq!(columns, vec!["id".to_string()]);
                assert_eq!(rows.len(), 3, "three seeded nodes");
            }
            other => panic!("SELECT returned {other:?}"),
        }

        // 2) CREATE TABLE — a DDL command tag (uniquely named to avoid the process-wide
        //    user-table store colliding with other tests).
        match client_query(
            &mut c,
            "CREATE TABLE eg076_items (id BIGINT, sku TEXT)",
        )
        .await
        {
            QueryResult::Ok { .. } => {}
            other => panic!("CREATE TABLE returned {other:?}"),
        }

        // 3) INSERT — an affected-row count.
        match client_query(
            &mut c,
            "INSERT INTO eg076_items (id, sku) VALUES (1, 'AAPL')",
        )
        .await
        {
            QueryResult::Ok { affected } => assert_eq!(affected, 1),
            other => panic!("INSERT returned {other:?}"),
        }

        // 4) SELECT the just-inserted row back — proves the write is visible + typed.
        match client_query(&mut c, "SELECT id, sku FROM eg076_items").await {
            QueryResult::Rows { columns, rows } => {
                assert_eq!(columns, vec!["id".to_string(), "sku".to_string()]);
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0][0].as_deref(), Some("1"));
                assert_eq!(rows[0][1].as_deref(), Some("AAPL"));
            }
            other => panic!("SELECT items returned {other:?}"),
        }

        // Clean up the shared user-table store so a re-run is idempotent.
        let _ = client_query(&mut c, "DROP TABLE eg076_items").await;
    }
}
