//! MSSQL TDS wire round-trip smoke test (CONCEPT:EG-KG.query.hand-rolled-tds-server).
//!
//! Starts the real hand-rolled TDS listener over an in-process `ServerState`, then
//! drives the raw protocol from a plain `TcpStream` (no `tiberius` — the server side
//! is hand-rolled, so the test hand-rolls the client too): PRELOGIN → authenticated LOGIN7
//! → a hand-built `SQLBatch` (`SELECT id FROM nodes …`), and asserts a correct
//! COLMETADATA + ROW* + DONE token stream comes back over the seeded graph — proving
//! the adapter reuses the SAME eg-query DataFusion path the shared `WireSession` runs.
//!
//! Only compiled with `--features mssql-wire`.

#![cfg(feature = "mssql-wire")]

mod common;
#[path = "common/test_support.rs"]
mod test_support;

use serde_json::Value;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use epistemic_graph::server::mssql_wire::{self, derive_mssql_password, protocol};

use protocol::{
    frame_message, parse_header, utf16le_bytes, utf16le_to_string, TdsType, HEADER_LEN, PKT_LOGIN7,
    PKT_PRELOGIN, PKT_SQLBATCH, TOKEN_COLMETADATA, TOKEN_DONE, TOKEN_ROW, TYPE_BITN, TYPE_FLTN,
    TYPE_INTN, TYPE_NVARCHAR,
};

const TEST_SECRET: &str = "test";
const TEST_USER: &str = "tester";

fn sql_test_persist_dir() -> String {
    test_support::sql_test_persist_dir("mssql")
}

/// A real tempdir-backed `RedbBackend` (mirrors `common::tempdir_persistence()` /
/// `server::mod::tests::test_state` / `bolt_wire::tests::durable_state` / the
/// identical helper in `tests/pgwire_roundtrip.rs`): the authoritative-commit flip
/// means every graph-node write now routes through the universal cross-modal
/// MutationBatch kernel, which fails closed ("cross-modal mutation requires durable
/// persistence") against `persistence: None`. Also provisions
/// `EPISTEMIC_GRAPH_ENCRYPTION_KEY` once, before the first backend opens, for the
/// multi-op commit's transaction-recovery-plan seal (`redb_backend::tests::cm_dir`'s
/// identical requirement).
fn default_persistence() -> Option<test_support::SharedPersistence> {
    test_support::durable_persistence("mssql-roundtrip-recovery-key")
}

/// Build a minimal authenticated `ServerState` seeded with three
/// nodes so a wire SELECT returns rows. `__commons__` is pre-created by the registry.
fn seeded_state() -> test_support::SharedState {
    test_support::seeded_wire_state(
        TEST_SECRET,
        TEST_USER,
        Some(sql_test_persist_dir()),
        default_persistence(),
    )
}

/// Bind an ephemeral port, serve the TDS listener there, and return the address.
async fn spawn_listener(state: test_support::SharedState) -> String {
    let addr = test_support::ephemeral_listener_addr().await;
    let serve_addr = addr.clone();
    tokio::spawn(async move {
        let _ = mssql_wire::serve(&serve_addr, state).await;
    });
    test_support::wait_for_listener_ready(&addr).await;
    addr
}

/// Build a minimal authenticated LOGIN7 record with user/password/database fields.
fn login7(user: &str, password: &str, database: &str) -> Vec<u8> {
    const FIXED_LEN: usize = 94;
    let user_b = utf16le_bytes(user);
    let pw_enc: Vec<u8> = utf16le_bytes(password)
        .into_iter()
        .map(|b| (b ^ 0xA5).rotate_left(4))
        .collect();
    let db_b = utf16le_bytes(database);

    let mut rec = vec![0u8; FIXED_LEN];
    let mut data = Vec::new();
    let put = |rec: &mut Vec<u8>,
               data: &mut Vec<u8>,
               offset_pos: usize,
               count_pos: usize,
               bytes: &[u8]| {
        let offset = (FIXED_LEN + data.len()) as u16;
        let units = (bytes.len() / 2) as u16;
        rec[offset_pos..offset_pos + 2].copy_from_slice(&offset.to_le_bytes());
        rec[count_pos..count_pos + 2].copy_from_slice(&units.to_le_bytes());
        data.extend_from_slice(bytes);
    };
    put(&mut rec, &mut data, 40, 42, &user_b);
    put(&mut rec, &mut data, 44, 46, &pw_enc);
    put(&mut rec, &mut data, 68, 70, &db_b);
    rec.extend_from_slice(&data);
    let len = rec.len() as u32;
    rec[0..4].copy_from_slice(&len.to_le_bytes());
    rec
}

/// Read one complete TDS message (reassembling until the EOM bit), returning the body.
async fn read_message(stream: &mut TcpStream) -> Vec<u8> {
    let mut payload = Vec::new();
    loop {
        let mut hdr = [0u8; HEADER_LEN];
        stream.read_exact(&mut hdr).await.unwrap();
        let h = parse_header(&hdr);
        let blen = h.body_len();
        if blen > 0 {
            let start = payload.len();
            payload.resize(start + blen, 0);
            stream.read_exact(&mut payload[start..]).await.unwrap();
        }
        if h.is_eom() {
            break;
        }
    }
    payload
}

/// Read one TYPE_INFO byte (already at `s[*i]`), advancing `*i` past it.
fn read_column_type(s: &[u8], i: &mut usize) -> TdsType {
    match s[*i] {
        TYPE_INTN => {
            *i += 2;
            TdsType::IntN
        }
        TYPE_FLTN => {
            *i += 2;
            TdsType::FloatN
        }
        TYPE_BITN => {
            *i += 2;
            TdsType::BitN
        }
        TYPE_NVARCHAR => {
            *i += 1 + 2 + 5; // token + max-len + collation
            TdsType::NVarchar
        }
        other => panic!("unexpected TYPE_INFO {other:#x}"),
    }
}

/// Read one column descriptor out of a COLMETADATA token body, advancing `*i`.
fn read_one_column(s: &[u8], i: &mut usize) -> (String, TdsType) {
    *i += 6; // UserType(4) + Flags(2)
    let ty = read_column_type(s, i);
    let units = s[*i] as usize;
    *i += 1;
    let name = utf16le_to_string(&s[*i..*i + units * 2]);
    *i += units * 2;
    (name, ty)
}

/// Read a COLMETADATA token body (already past the token byte), advancing `*i`.
fn read_colmetadata(s: &[u8], i: &mut usize) -> Vec<(String, TdsType)> {
    let count = u16::from_le_bytes([s[*i], s[*i + 1]]) as usize;
    *i += 2;
    (0..count).map(|_| read_one_column(s, i)).collect()
}

/// Read one row value for column type `ty` (already at `s[*i]`), advancing `*i`.
fn read_row_value(s: &[u8], i: &mut usize, ty: &TdsType) -> Value {
    match ty {
        TdsType::NVarchar => {
            let len = u16::from_le_bytes([s[*i], s[*i + 1]]) as usize;
            *i += 2;
            if len == 0xFFFF {
                Value::Null
            } else {
                let value = Value::String(utf16le_to_string(&s[*i..*i + len]));
                *i += len;
                value
            }
        }
        _ => {
            let len = s[*i] as usize;
            *i += 1 + len;
            Value::Null // value bytes not needed for this test
        }
    }
}

/// Read a ROW token body (already past the token byte), advancing `*i`.
fn read_row(s: &[u8], i: &mut usize, cols: &[(String, TdsType)]) -> Vec<Value> {
    cols.iter()
        .map(|(_, ty)| read_row_value(s, i, ty))
        .collect()
}

/// Read a DONE token body (already past the token byte), advancing `*i`.
fn read_done_status(s: &[u8], i: &mut usize) -> u16 {
    let status = u16::from_le_bytes([s[*i + 1], s[*i + 2]]);
    *i += 13;
    status
}

/// Walk a COLMETADATA + ROW* + DONE token stream into (columns, rows, done_status).
#[allow(clippy::type_complexity)]
fn walk_result(s: &[u8]) -> (Vec<(String, TdsType)>, Vec<Vec<Value>>, u16) {
    let mut i = 0usize;
    let mut cols: Vec<(String, TdsType)> = Vec::new();
    let mut rows: Vec<Vec<Value>> = Vec::new();
    let mut status = 0u16;
    while i < s.len() {
        match s[i] {
            TOKEN_COLMETADATA => {
                i += 1;
                cols = read_colmetadata(s, &mut i);
            }
            TOKEN_ROW => {
                i += 1;
                rows.push(read_row(s, &mut i, &cols));
            }
            TOKEN_DONE => {
                status = read_done_status(s, &mut i);
            }
            other => panic!("unexpected token {other:#x} at {i}"),
        }
    }
    (cols, rows, status)
}

#[tokio::test]
async fn tds_select_returns_colmetadata_rows_done() {
    let addr = spawn_listener(seeded_state()).await;
    let mut stream = TcpStream::connect(&addr).await.unwrap();

    // 1. PRELOGIN (a bare option terminator is enough for the server to reply).
    stream
        .write_all(&frame_message(PKT_PRELOGIN, &[0xFF]))
        .await
        .unwrap();
    let _prelogin_reply = read_message(&mut stream).await;

    // 2. Authenticated LOGIN7. Server replies LOGINACK + DONE.
    let password = derive_mssql_password(TEST_SECRET, TEST_USER);
    stream
        .write_all(&frame_message(
            PKT_LOGIN7,
            &login7(TEST_USER, &password, "__commons__"),
        ))
        .await
        .unwrap();
    let login_reply = read_message(&mut stream).await;
    assert_eq!(
        login_reply.first().copied(),
        Some(protocol::TOKEN_LOGINACK),
        "login succeeds with a LOGINACK token"
    );

    // 3. SQLBatch — the UCS-2 SQL text (no ALL_HEADERS block).
    let sql = utf16le_bytes("SELECT id FROM nodes ORDER BY id");
    stream
        .write_all(&frame_message(PKT_SQLBATCH, &sql))
        .await
        .unwrap();
    let result = read_message(&mut stream).await;

    assert_eq!(
        result.first().copied(),
        Some(TOKEN_COLMETADATA),
        "the result stream opens with COLMETADATA (not an ERROR token)"
    );
    let (cols, rows, status) = walk_result(&result);
    assert_eq!(cols.len(), 1, "one projected column");
    assert_eq!(cols[0].0, "id");
    assert_eq!(cols[0].1, TdsType::NVarchar, "text id → NVARCHAR");
    assert_eq!(rows.len(), 3, "three seeded nodes returned");
    let ids: Vec<String> = rows
        .iter()
        .map(|r| match &r[0] {
            Value::String(s) => s.clone(),
            other => panic!("expected string id, got {other:?}"),
        })
        .collect();
    assert_eq!(ids, vec!["n1", "n2", "n3"]);
    assert_eq!(status & protocol::DONE_ERROR, 0, "DONE has no error flag");
}

// ───────────────────────────────────────────────────────────────────────────
// Cross-modal transaction round-trip (CONCEPT:EG-KG.query.per-surface-parity) — per-surface parity.
//
// The in-txn cross-modal seam routes through the SHARED `WireSession`
// (CONCEPT:EG-KG.compute.subsystems-reference/EG-372), so the TDS wire INHERITS the pgwire seam. These tests
// MIRROR the pgwire cross-modal cases (`tests/pgwire_roundtrip.rs`) over the TDS
// SQLBatch protocol — every batch is handed verbatim to `WireSession::execute`, the
// SAME core the pgwire shim runs:
//   * `wire_txn_update_then_cross_modal_read` — a staged in-txn graph UPDATE + vector
//     `SET EMBEDDING` are BOTH read back by an in-txn cross-modal `UQL` read, and
//   * `wire_txn_crossmodal_ryow_isolated_until_commit` — `BEGIN; SET EMBEDDING …;
//     INSERT INTO series …; <UQL cross-modal read>; COMMIT` reads its own writes while
//     a SECOND connection sees NONE of them until COMMIT.
// ───────────────────────────────────────────────────────────────────────────

/// Complete PRELOGIN + authenticated LOGIN7 and return the connected stream ready for the
/// command phase. Panics if the login is not acknowledged.
async fn connect(addr: &str) -> TcpStream {
    let mut stream = TcpStream::connect(addr).await.unwrap();
    stream
        .write_all(&frame_message(PKT_PRELOGIN, &[0xFF]))
        .await
        .unwrap();
    let _ = read_message(&mut stream).await;
    let password = derive_mssql_password(TEST_SECRET, TEST_USER);
    stream
        .write_all(&frame_message(
            PKT_LOGIN7,
            &login7(TEST_USER, &password, "__commons__"),
        ))
        .await
        .unwrap();
    let login_reply = read_message(&mut stream).await;
    assert_eq!(
        login_reply.first().copied(),
        Some(protocol::TOKEN_LOGINACK),
        "login succeeds with a LOGINACK token"
    );
    stream
}

/// Decode a TDS ERROR token's message text (US_VARCHAR at a fixed offset) for a loud panic.
fn decode_error(s: &[u8]) -> String {
    // [0xAA][len u16][number i32][state u8][class u8][msglen u16 code-units][utf16 msg]…
    let msg_units = u16::from_le_bytes([s[9], s[10]]) as usize;
    utf16le_to_string(&s[11..11 + msg_units * 2])
}

/// Send a `SQLBatch` and walk the returned token stream into `(cols, rows, status)`.
/// Panics loudly on an ERROR token so a rejected cross-modal verb fails visibly.
#[allow(clippy::type_complexity)]
async fn batch(
    stream: &mut TcpStream,
    sql: &str,
) -> (Vec<(String, TdsType)>, Vec<Vec<Value>>, u16) {
    let bytes = utf16le_bytes(sql);
    stream
        .write_all(&frame_message(PKT_SQLBATCH, &bytes))
        .await
        .unwrap();
    let result = read_message(stream).await;
    if result.first().copied() == Some(protocol::TOKEN_ERROR) {
        panic!(
            "batch `{sql}` returned an ERROR token: {}",
            decode_error(&result)
        );
    }
    walk_result(&result)
}

/// The first column of every returned row (mirrors pgwire's `simple_ids`).
fn first_col(rows: &[Vec<Value>]) -> Vec<String> {
    rows.iter()
        .map(|r| match &r[0] {
            Value::String(s) => s.clone(),
            other => panic!("expected a string id, got {other:?}"),
        })
        .collect()
}

/// Mirrors pgwire `wire_txn_update_then_cross_modal_read` (CONCEPT:EG-KG.query.per-surface-parity / EG-372):
/// inside an open txn, a staged graph UPDATE AND a staged vector `SET EMBEDDING` are BOTH
/// read back by an in-txn cross-modal `UQL` read over the TDS wire (read-your-own-writes
/// across the graph + vector modalities before COMMIT).
#[tokio::test]
async fn tds_txn_update_then_cross_modal_read() {
    let addr = spawn_listener(seeded_state()).await;
    let mut c = connect(&addr).await;

    batch(&mut c, "BEGIN").await;

    // Stage a graph write (UPDATE) AND a vector write (SET EMBEDDING) inside the txn.
    batch(&mut c, "UPDATE nodes SET rank = 99 WHERE id = 'n1'").await;
    batch(&mut c, "SET EMBEDDING FOR 'n1' = '[1.0, 0.0, 0.0]'").await;

    // Plain-SQL RYOW sees the staged write (buffered write path).
    let (_c, rows, _s) = batch(&mut c, "SELECT id FROM nodes WHERE rank = 99").await;
    assert_eq!(
        first_col(&rows),
        vec!["n1".to_string()],
        "plain SQL RYOW works"
    );

    // THE SEAM: a CROSS-MODAL / UQL read over the TDS wire ALSO reflects the staged
    // writes — a unified filter→rank over the staged row returns `n1` (RYOW across the
    // graph AND vector modalities before COMMIT).
    let (_c, rows, _s) = batch(
        &mut c,
        "UQL MATCH (:Agent) WHERE rank = 99 |> RANK BY ~[1.0,0.0,0.0] |> LIMIT 5",
    )
    .await;
    assert_eq!(
        first_col(&rows),
        vec!["n1".to_string()],
        "the cross-modal read must see the same-txn staged UPDATE + embedding (RYOW)"
    );

    batch(&mut c, "COMMIT").await;
}

/// Mirrors pgwire `wire_txn_crossmodal_ryow_isolated_until_commit` (CONCEPT:EG-KG.query.per-surface-parity /
/// EG-372) — the headline per-surface parity proof over TDS. A `BEGIN; INSERT node;
/// SET EMBEDDING; INSERT INTO series; <UQL join over graph+vector>; COMMIT` reads its OWN
/// writes inside the txn, while a SECOND connection sees NONE of them until COMMIT. After
/// COMMIT the second connection reads the committed cross-modal state.
#[tokio::test]
async fn tds_txn_crossmodal_ryow_isolated_until_commit() {
    let addr = spawn_listener(seeded_state()).await;
    let mut writer = connect(&addr).await;
    let mut reader = connect(&addr).await;

    // The UQL cross-modal join used by both connections: a Widget node filtered on the
    // graph modality and ranked by the query embedding (graph + vector legs).
    let uql = "UQL MATCH (:Widget) WHERE rank = 5 |> RANK BY ~[1.0,0.0,0.0] |> LIMIT 5";

    batch(&mut writer, "BEGIN").await;
    // Stage a graph node, its embedding, and a measurement — all inside the txn.
    batch(
        &mut writer,
        "INSERT INTO nodes (id, type, rank, _visibility, _owner) VALUES ('vv1', 'Widget', 5, 'public', 'tester')",
    )
    .await;
    batch(&mut writer, "SET EMBEDDING FOR 'vv1' = '[1.0, 0.0, 0.0]'").await;
    batch(
        &mut writer,
        "INSERT INTO series (id, ts, value) VALUES ('vv1', 1000, 9.0)",
    )
    .await;

    // RYOW: the writer's OWN in-txn UQL join sees its staged node + embedding.
    let (_c, rows, _s) = batch(&mut writer, uql).await;
    assert_eq!(
        first_col(&rows),
        vec!["vv1".to_string()],
        "the writer reads its own staged cross-modal writes (RYOW)"
    );

    // Isolation: a SECOND connection (no open txn) sees NONE of the staged writes.
    let (_c, rows, _s) = batch(&mut reader, uql).await;
    assert!(
        rows.is_empty(),
        "a second connection must see none of the uncommitted cross-modal writes, got {rows:?}"
    );

    batch(&mut writer, "COMMIT").await;

    // After COMMIT the committed cross-modal state is visible to the second connection.
    let (_c, rows, _s) = batch(&mut reader, uql).await;
    assert_eq!(
        first_col(&rows),
        vec!["vv1".to_string()],
        "after COMMIT the committed node + embedding are visible off-txn"
    );
}
