//! MySQL/MariaDB wire cross-modal transaction round-trip test (CONCEPT:EG-KG.query.eg-8).
//!
//! Per-surface parity proof (the North-Star "seamless" discipline, EG-373): the in-txn
//! cross-modal seam already routes through the SHARED `WireSession` (EG-074/EG-372), so
//! the MySQL wire INHERITS the pgwire seam. This test proves it end-to-end over the
//! MySQL text protocol by MIRRORING the pgwire cross-modal cases
//! (`tests/pgwire_roundtrip.rs`):
//!   * `wire_txn_update_then_cross_modal_read` — a staged in-txn graph UPDATE + vector
//!     `SET EMBEDDING` are BOTH read back by an in-txn cross-modal `UQL` read
//!     (read-your-own-writes across the graph + vector modalities before COMMIT), and
//!   * `wire_txn_crossmodal_ryow_isolated_until_commit` — a `BEGIN; SET EMBEDDING …;
//!     INSERT INTO series …; <UQL cross-modal read>; COMMIT` reads its OWN writes inside
//!     the txn while a SECOND connection sees NONE of them until COMMIT.
//!
//! There is NO server-side MySQL protocol crate and no MySQL client crate in the tree
//! (the Pi-contract idiom: the wire is hand-rolled). So — exactly like the sibling
//! `tests/mssql_roundtrip.rs` hand-rolls the TDS client — this test hand-rolls the
//! MySQL Handshake-v10 / Handshake-Response-41 + `COM_QUERY` text-protocol framing over
//! a raw `TcpStream`, driving the REAL `mysql_wire::serve_with_auth` listener. Every
//! statement is handed verbatim to `WireSession::execute`, the SAME core the pgwire
//! shim runs — so this proves the cross-modal verbs over the MySQL wire, not a mock.
//!
//! Only compiled with `--features mysql-wire`.

#![cfg(feature = "mysql-wire")]

use std::sync::Arc;

use dashmap::DashMap;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::{RwLock, Semaphore};

use epistemic_graph::channels::ChannelManager;
use epistemic_graph::isolation::IsolationLayer;
use epistemic_graph::registry::GraphRegistry;
use epistemic_graph::server::mysql_wire::{self, MysqlAuthMode};
use epistemic_graph::server::txn::TxnIdGen;
use epistemic_graph::server::ServerState;

fn sql_test_persist_dir() -> String {
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
    std::env::set_var("EPISTEMIC_GRAPH_AUDIENCE", "epistemic-graph-test");
    std::env::set_var("EPISTEMIC_GRAPH_TENANT", "tenant-test");
    std::env::set_var("EPISTEMIC_GRAPH_POLICY_VERSION", "policy-test");
    std::env::temp_dir()
        .join(format!(
            "epistemic-graph-mysql-test-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        ))
        .to_string_lossy()
        .into_owned()
}

// ── the seeded server state (mirrors the pgwire round-trip test's `seeded_state`) ──

/// A minimal authenticated `ServerState` seeded with three nodes so a wire SELECT/UQL has
/// rows to read. `__commons__` is pre-created by the registry. `persistence = None`
/// exercises the in-memory cross-modal commit path (EG-372: `commit_cross_modal_txn` is
/// persistence-None tolerant).
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
            epistemic_graph::server::persistence::cold_offload::ColdTenantTracker::new(),
        ),
        registry,
        isolation: IsolationLayer::new(),
        channels: ChannelManager::new(),
        auth_secret: "test".to_string(),
        #[cfg(feature = "kv")]
        kv: None,
        persist_dir: Some(sql_test_persist_dir()),
        persistence: None,
        max_in_flight: Arc::new(Semaphore::new(16)),
        read_admission: Arc::new(Semaphore::new(16)),
        per_graph_inflight: Arc::new(DashMap::new()),
        per_graph_inflight_limit: 8,
        write_coalescer: Arc::new(epistemic_graph::write_coalescer::WriteCoalescerRegistry::new()),
        open_txns: Arc::new(DashMap::new()),
        txn_id_gen: Arc::new(TxnIdGen),
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
        cdc: Some(Arc::new(epistemic_graph::server::cdc::CdcHub::new())),
        #[cfg(feature = "wasm-udf")]
        udf_registry: Arc::new(eg_wasm::UdfRegistry::new()),
        #[cfg(feature = "compute-dist")]
        matviews: Arc::new(parking_lot::Mutex::new(
            epistemic_graph::raft::pregel::MatViewStore::new(),
        )),
        #[cfg(feature = "federation")]
        foreign_sources: Arc::new(DashMap::new()),
        #[cfg(feature = "lake")]
        lake: std::sync::Arc::new(epistemic_graph::server::lake::LakeManager::new()),
    }))
}

/// Bind an ephemeral port and serve the MySQL wire with mandatory native auth.
async fn spawn_listener(state: Arc<RwLock<ServerState>>) -> String {
    let probe = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = probe.local_addr().unwrap().to_string();
    drop(probe);
    let serve_addr = addr.clone();
    tokio::spawn(async move {
        let _ = mysql_wire::serve_with_auth(&serve_addr, state, MysqlAuthMode::Native).await;
    });
    // Wait for the listener to accept.
    for _ in 0..50 {
        if TcpStream::connect(&addr).await.is_ok() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    addr
}

// ── a hand-rolled MySQL text-protocol client (no mysql client crate) ────────────────

// Capability flags the client negotiates (a subset — protocol 4.1 + secure-connection +
// plugin-auth is enough for the server's native-password path).
const CLIENT_PROTOCOL_41: u32 = 0x0000_0200;
const CLIENT_SECURE_CONNECTION: u32 = 0x0000_8000;
const CLIENT_PLUGIN_AUTH: u32 = 0x0008_0000;
const COLLATION_UTF8MB4: u8 = 45;
const COM_QUERY: u8 = 0x03;

/// Read one COMPLETE MySQL message, reassembling the 16 MiB multi-packet split. Returns
/// `(last_seq, payload)`.
async fn read_message(s: &mut TcpStream) -> (u8, Vec<u8>) {
    let mut out = Vec::new();
    let mut seq;
    loop {
        let mut hdr = [0u8; 4];
        s.read_exact(&mut hdr).await.unwrap();
        let len = u32::from_le_bytes([hdr[0], hdr[1], hdr[2], 0]) as usize;
        seq = hdr[3];
        let start = out.len();
        out.resize(start + len, 0);
        s.read_exact(&mut out[start..]).await.unwrap();
        if len < 0xff_ffff {
            break;
        }
    }
    (seq, out)
}

/// Write one payload as a single MySQL packet at sequence id `seq` (test payloads are far
/// below the 16 MiB frame limit, so no split is needed).
async fn write_packet(s: &mut TcpStream, seq: u8, payload: &[u8]) {
    let len = (payload.len() as u32).to_le_bytes();
    s.write_all(&[len[0], len[1], len[2], seq]).await.unwrap();
    s.write_all(payload).await.unwrap();
}

/// Read a length-encoded integer at `*pos`, advancing it.
fn get_lenenc_int(buf: &[u8], pos: &mut usize) -> u64 {
    let first = buf[*pos];
    *pos += 1;
    match first {
        0xfc => {
            let v = u16::from_le_bytes([buf[*pos], buf[*pos + 1]]) as u64;
            *pos += 2;
            v
        }
        0xfd => {
            let v = u32::from_le_bytes([buf[*pos], buf[*pos + 1], buf[*pos + 2], 0]) as u64;
            *pos += 3;
            v
        }
        0xfe => {
            let v = u64::from_le_bytes(buf[*pos..*pos + 8].try_into().unwrap());
            *pos += 8;
            v
        }
        n => n as u64,
    }
}

/// Build a Handshake-Response-41 with a native-password proof, no connect-DB
/// (the listener's default graph `__commons__` is used).
fn handshake_response(username: &str, auth_response: &[u8]) -> Vec<u8> {
    let caps = CLIENT_PROTOCOL_41 | CLIENT_SECURE_CONNECTION | CLIENT_PLUGIN_AUTH;
    let mut p = Vec::with_capacity(64);
    p.extend_from_slice(&caps.to_le_bytes());
    p.extend_from_slice(&0x0100_0000u32.to_le_bytes()); // max packet size (16 MiB)
    p.push(COLLATION_UTF8MB4);
    p.extend_from_slice(&[0u8; 23]); // reserved
    p.extend_from_slice(username.as_bytes());
    p.push(0);
    p.push(auth_response.len() as u8);
    p.extend_from_slice(auth_response);
    p.extend_from_slice(b"mysql_native_password"); // CLIENT_PLUGIN_AUTH plugin name
    p.push(0);
    p
}

/// A decoded `COM_QUERY` outcome.
#[derive(Debug)]
enum QueryResult {
    /// An OK packet (a command tag: BEGIN/COMMIT/UPDATE/SET/INSERT — no result set).
    Ok,
    /// A result set: the parsed text rows (as optional strings per cell).
    Rows { rows: Vec<Vec<Option<String>>> },
}

impl QueryResult {
    /// The first column of every returned row (mirrors pgwire's `simple_ids`).
    fn first_col(&self) -> Vec<String> {
        match self {
            QueryResult::Rows { rows } => rows
                .iter()
                .map(|r| r[0].clone().unwrap_or_default())
                .collect(),
            QueryResult::Ok => Vec::new(),
        }
    }
}

/// Complete the mandatory native-password handshake and return the connected stream.
async fn connect(addr: &str) -> TcpStream {
    let mut stream = TcpStream::connect(addr).await.unwrap();
    let (seq, hs) = read_message(&mut stream).await;
    assert_eq!(hs[0], 10, "server sends Handshake v10");
    let version_end = hs[1..].iter().position(|byte| *byte == 0).unwrap() + 1;
    let part1 = version_end + 1 + 4;
    let part2 = part1 + 8 + 1 + 2 + 1 + 2 + 2 + 1 + 10;
    let mut seed = [0u8; 20];
    seed[..8].copy_from_slice(&hs[part1..part1 + 8]);
    seed[8..].copy_from_slice(&hs[part2..part2 + 12]);
    let password = mysql_wire::derive_mysql_password("test", "tester");
    let proof = mysql_wire::native_password_scramble(&password, &seed);
    write_packet(
        &mut stream,
        seq.wrapping_add(1),
        &handshake_response("tester", &proof),
    )
    .await;
    let (_seq, ok) = read_message(&mut stream).await;
    assert_eq!(ok[0], 0x00, "auth OK");
    stream
}

/// Send a `COM_QUERY` and decode the response (OK / result set). Panics on an ERR frame,
/// surfacing the server's message (so a rejected cross-modal verb fails loud).
async fn query(stream: &mut TcpStream, sql: &str) -> QueryResult {
    let mut pkt = vec![COM_QUERY];
    pkt.extend_from_slice(sql.as_bytes());
    write_packet(stream, 0, &pkt).await; // a new command resets seq to 0
    let (_seq, first) = read_message(stream).await;
    match first[0] {
        0xff => {
            let code = u16::from_le_bytes([first[1], first[2]]);
            let msg = String::from_utf8_lossy(&first[9..]).into_owned();
            panic!("query `{sql}` returned ERR {code}: {msg}");
        }
        0x00 => QueryResult::Ok,
        _ => {
            // Result set: the first packet is the column count.
            let mut pos = 0usize;
            let ncols = get_lenenc_int(&first, &mut pos) as usize;
            for _ in 0..ncols {
                let _ = read_message(stream).await; // column definition (discarded)
            }
            let (_s, eof) = read_message(stream).await;
            assert_eq!(eof[0], 0xfe, "EOF after column defs");
            let mut rows = Vec::new();
            loop {
                let (_s, p) = read_message(stream).await;
                if p[0] == 0xfe && p.len() < 9 {
                    break; // terminating EOF
                }
                let mut cpos = 0usize;
                let mut row = Vec::with_capacity(ncols);
                for _ in 0..ncols {
                    if p[cpos] == 0xfb {
                        row.push(None);
                        cpos += 1;
                    } else {
                        let len = get_lenenc_int(&p, &mut cpos) as usize;
                        row.push(Some(
                            String::from_utf8_lossy(&p[cpos..cpos + len]).into_owned(),
                        ));
                        cpos += len;
                    }
                }
                rows.push(row);
            }
            QueryResult::Rows { rows }
        }
    }
}

// ── the cross-modal round-trip tests (mirror pgwire) ────────────────────────────────

/// Mirrors pgwire `wire_txn_update_then_cross_modal_read` (CONCEPT:EG-KG.query.eg-8 / EG-372):
/// inside an open txn, a staged graph UPDATE AND a staged vector `SET EMBEDDING` are BOTH
/// read back by an in-txn cross-modal `UQL` read over the MySQL wire (read-your-own-writes
/// across the graph + vector modalities before COMMIT).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mysql_wire_txn_update_then_cross_modal_read() {
    let addr = spawn_listener(seeded_state()).await;
    let mut c = connect(&addr).await;

    query(&mut c, "BEGIN").await;

    // Stage a graph write (UPDATE) AND a vector write (SET EMBEDDING) inside the txn —
    // neither committed yet. The cross-modal UQL below must read BOTH.
    query(&mut c, "UPDATE nodes SET rank = 99 WHERE id = 'n1'").await;
    query(&mut c, "SET EMBEDDING FOR 'n1' = '[1.0, 0.0, 0.0]'").await;

    // Plain-SQL RYOW sees the staged write (buffered write path).
    let sql_ryow = query(&mut c, "SELECT id FROM nodes WHERE rank = 99")
        .await
        .first_col();
    assert_eq!(sql_ryow, vec!["n1".to_string()], "plain SQL RYOW works");

    // THE SEAM: a CROSS-MODAL / UQL read issued over the MySQL wire ALSO reflects the
    // staged writes — a unified filter→rank over the staged row returns `n1` (RYOW across
    // the graph AND vector modalities before COMMIT).
    let unified = query(
        &mut c,
        "UQL MATCH (:Agent) WHERE rank = 99 |> RANK BY ~[1.0,0.0,0.0] |> LIMIT 5",
    )
    .await
    .first_col();
    assert_eq!(
        unified,
        vec!["n1".to_string()],
        "the cross-modal read must see the same-txn staged UPDATE + embedding (RYOW)"
    );

    query(&mut c, "COMMIT").await;
}

/// Mirrors pgwire `wire_txn_crossmodal_ryow_isolated_until_commit` (CONCEPT:EG-KG.query.eg-8 /
/// EG-372) — the headline per-surface parity proof. A `BEGIN; INSERT node; SET EMBEDDING;
/// INSERT INTO series; <UQL join over graph+vector>; COMMIT` reads its OWN writes inside
/// the txn, while a SECOND connection sees NONE of them until COMMIT. After COMMIT the
/// second connection reads the committed cross-modal state.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mysql_wire_txn_crossmodal_ryow_isolated_until_commit() {
    let addr = spawn_listener(seeded_state()).await;
    let mut writer = connect(&addr).await;
    let mut reader = connect(&addr).await;

    // The UQL cross-modal join used by both connections: a Widget node filtered on the
    // graph modality and ranked by the query embedding (graph + vector legs).
    let uql = "UQL MATCH (:Widget) WHERE rank = 5 |> RANK BY ~[1.0,0.0,0.0] |> LIMIT 5";

    query(&mut writer, "BEGIN").await;
    // Stage a graph node, its embedding, and a measurement — all inside the txn.
    query(
        &mut writer,
        "INSERT INTO nodes (id, type, rank) VALUES ('vv1', 'Widget', 5)",
    )
    .await;
    query(&mut writer, "SET EMBEDDING FOR 'vv1' = '[1.0, 0.0, 0.0]'").await;
    query(
        &mut writer,
        "INSERT INTO series (id, ts, value) VALUES ('vv1', 1000, 9.0)",
    )
    .await;

    // RYOW: the writer's OWN in-txn UQL join sees its staged node + embedding.
    let in_txn = query(&mut writer, uql).await.first_col();
    assert_eq!(
        in_txn,
        vec!["vv1".to_string()],
        "the writer reads its own staged cross-modal writes (RYOW)"
    );

    // Isolation: a SECOND connection (no open txn) sees NONE of the staged writes.
    let before = query(&mut reader, uql).await.first_col();
    assert!(
        before.is_empty(),
        "a second connection must see none of the uncommitted cross-modal writes, got {before:?}"
    );

    query(&mut writer, "COMMIT").await;

    // After COMMIT the committed cross-modal state is visible to the second connection.
    let after = query(&mut reader, uql).await.first_col();
    assert_eq!(
        after,
        vec!["vv1".to_string()],
        "after COMMIT the committed node + embedding are visible off-txn"
    );
}
