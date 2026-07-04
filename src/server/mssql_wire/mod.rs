//! MSSQL **TDS** (Tabular Data Stream) wire-protocol listener (CONCEPT:EG-KG.query.hand-rolled-tds-server) — the
//! MSSQL adapter in the multi-wire family (CONCEPT:EG-KG.compute.subsystems-reference), a sibling of the Postgres
//! shim (`crate::server::pgwire`). It lets SQL Server clients / drivers connect over
//! the TDS protocol and run SQL against a graph, reusing the ONE shared
//! `classify → dispatch → exec` core ([`crate::server::wire::WireSession`]) — this
//! module adds ONLY the TDS-specific framing, handshake, auth, and token encoding.
//!
//! ## What this is (and is NOT)
//! A HAND-ROLLED TDS server over `tokio::net::TcpListener` (the Pi-contract idiom — no
//! `tiberius`/`tds` server crate). It does NOT re-implement any SQL semantics: a
//! decoded `SQLBatch` is handed verbatim to [`crate::server::wire::WireProtocol::execute`]
//! and the wire-neutral [`crate::server::wire::WireOutcome`] it returns is encoded into
//! a TDS token stream. All framing/tokens live in [`protocol`].
//!
//! ## Flow
//!   1. **PRELOGIN** → respond VERSION + ENCRYPTION = `ENCRYPT_NOT_SUP` (plaintext v1).
//!   2. **LOGIN7** → parse UserName/Password/Database; authenticate (secret-derived
//!      password when an engine secret is set, else trust); send LOGINACK + DONE.
//!   3. **Command phase** — `SQLBatch` → execute → COLMETADATA + ROW* + DONE (or an
//!      ERROR token + DONE(error)); ATTENTION → DONE(attn); transaction-manager → DONE;
//!      RPC → an ERROR token (RPC/prepared deferred). A closed socket ends the loop.
//!
//! ## Auth (CONCEPT:EG-KG.query.concept-13)
//! A TDS `user` maps to an engine `agent_id`, exactly like pgwire. When
//! `GRAPH_SERVICE_AUTH_SECRET` is set, the connection password must equal
//! `hex(HMAC-SHA256(secret, "mssql:" || user))` (an authorized operator computes it
//! offline) and the authenticated `user` becomes the ACL actor; with no secret the
//! listener is TRUST (anonymous) — the zero-infra/dev path.
//!
//! ## Deferred (documented)
//! TLS/encryption (answered `ENCRYPT_NOT_SUP`), RPC / prepared `sp_executesql`, MARS,
//! and NVARCHAR(MAX)/PLP chunking (long text truncated). See `protocol.rs`.

pub mod protocol;

use std::sync::Arc;

use hmac::{Hmac, Mac};
use sha2::Sha256;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::RwLock;

use eg_query::PgColType;

use crate::server::wire::{WireError, WireOutcome, WireProtocol, WireSession};
use crate::server::ServerState;

use protocol::{
    build_prelogin_response, decode_sqlbatch, encode_colmetadata, encode_done, encode_error,
    encode_loginack, encode_row, frame_message, parse_header, parse_login7, Header, TdsType,
    DONE_ATTN, DONE_COUNT, DONE_ERROR, DONE_FINAL, HEADER_LEN, PKT_ATTENTION, PKT_LOGIN7, PKT_RPC,
    PKT_SQLBATCH, PKT_TABULAR, PKT_TXMGR,
};

/// Env var: when set (and the binary is built `--features mssql-wire`), the TDS
/// listener binds this address (default `127.0.0.1:1433`). Unset → no listener.
pub const MSSQL_ADDR_ENV: &str = "EPISTEMIC_GRAPH_MSSQL_ADDR";
/// Env var: the default graph a fresh connection runs against when the LOGIN7
/// `Database` field is not supplied. Defaults to `__commons__`.
pub const MSSQL_GRAPH_ENV: &str = "EPISTEMIC_GRAPH_MSSQL_GRAPH";

/// The program name reported in the LOGINACK token + ERROR-token server name.
const SERVER_NAME: &str = "epistemic-graph";
/// The TDS error number used for engine (`WireError`) failures. SQLSTATE codes are
/// alphanumeric and do not map to the TDS integer error number, so we report a fixed
/// user-error-range number and carry the real SQLSTATE text in the message.
const ENGINE_ERROR_NUMBER: i32 = 50000;

type HmacSha256 = Hmac<Sha256>;

/// The per-user TDS password an authorized operator derives from the engine secret:
/// `hex(HMAC-SHA256(secret, "mssql:" || user))` (parallel to pgwire's derivation, with
/// its own domain-separation prefix so a pgwire and a TDS password never coincide).
pub fn derive_mssql_password(secret: &str, user: &str) -> String {
    let mut mac =
        HmacSha256::new_from_slice(secret.as_bytes()).expect("HMAC accepts any key length");
    mac.update(b"mssql:");
    mac.update(user.as_bytes());
    hex::encode(mac.finalize().into_bytes())
}

/// Map an engine result-column type to the TDS column type emitted in COLMETADATA.
/// Numbers/bools get their precise nullable TDS type; text (and the pgvector `vector`,
/// rendered as its text form by the shared core) goes as NVARCHAR — documented.
fn map_col_type(t: PgColType) -> TdsType {
    match t {
        PgColType::Int8 => TdsType::IntN,
        PgColType::Float8 => TdsType::FloatN,
        PgColType::Bool => TdsType::BitN,
        PgColType::Text | PgColType::Vector => TdsType::NVarchar,
    }
}

/// Read ONE complete TDS message (reassembling multi-packet messages until the EOM
/// status bit), returning `(message_type, payload)`. `Ok(None)` on a clean EOF at a
/// message boundary (the client closed the connection).
async fn read_message<R: AsyncRead + Unpin>(r: &mut R) -> std::io::Result<Option<(u8, Vec<u8>)>> {
    let mut payload = Vec::new();
    let mut msg_type: Option<u8> = None;
    loop {
        let mut hdr_buf = [0u8; HEADER_LEN];
        match r.read_exact(&mut hdr_buf).await {
            Ok(_) => {}
            // A clean close at a message boundary (nothing buffered yet) → end.
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof && payload.is_empty() => {
                return Ok(None);
            }
            Err(e) => return Err(e),
        }
        let hdr: Header = parse_header(&hdr_buf);
        msg_type.get_or_insert(hdr.ty);
        let body_len = hdr.body_len();
        if body_len > 0 {
            let start = payload.len();
            payload.resize(start + body_len, 0);
            r.read_exact(&mut payload[start..]).await?;
        }
        if hdr.is_eom() {
            break;
        }
    }
    Ok(Some((msg_type.unwrap_or(0), payload)))
}

/// Frame `payload` as a server → client Tabular-Result message and write it.
async fn write_tabular<W: AsyncWrite + Unpin>(w: &mut W, payload: &[u8]) -> std::io::Result<()> {
    let framed = frame_message(PKT_TABULAR, payload);
    w.write_all(&framed).await?;
    w.flush().await
}

/// Encode a `WireError` as an ERROR token followed by a DONE(error/final) token.
fn error_response(e: &WireError) -> Vec<u8> {
    let msg = format!("[{}] {}", e.code, e.message);
    let mut out = encode_error(ENGINE_ERROR_NUMBER, 1, 16, &msg, SERVER_NAME);
    out.extend(encode_done(DONE_FINAL | DONE_ERROR, 0, 0));
    out
}

/// Execute one decoded SQL batch through the shared session and encode the outcome as
/// a complete TDS token stream (COLMETADATA + ROW* + DONE, or ERROR + DONE).
async fn run_batch(session: &WireSession, sql: &str) -> Vec<u8> {
    match session.execute(sql).await {
        Ok(WireOutcome::Rows(result)) => {
            let cols: Vec<(String, TdsType)> = result
                .columns
                .iter()
                .map(|c| (c.name.clone(), map_col_type(c.ty)))
                .collect();
            let types: Vec<TdsType> = cols.iter().map(|(_, t)| *t).collect();
            let mut out = encode_colmetadata(&cols);
            for row in &result.rows {
                out.extend(encode_row(&types, row));
            }
            out.extend(encode_done(
                DONE_FINAL | DONE_COUNT,
                0,
                result.rows.len() as u64,
            ));
            out
        }
        Ok(WireOutcome::Command { rows, .. }) => {
            let status = if rows.is_some() {
                DONE_FINAL | DONE_COUNT
            } else {
                DONE_FINAL
            };
            encode_done(status, 0, rows.unwrap_or(0) as u64)
        }
        Ok(WireOutcome::TxnStart) | Ok(WireOutcome::TxnEnd { .. }) => encode_done(DONE_FINAL, 0, 0),
        Ok(WireOutcome::CopyIn { .. }) => {
            let e = WireError {
                code: "0A000".to_owned(),
                message: "COPY / bulk-load is not supported over the TDS wire".to_owned(),
            };
            error_response(&e)
        }
        Err(e) => error_response(&e),
    }
}

/// Drive one accepted TDS connection: PRELOGIN → LOGIN7/auth → command loop. `secret`
/// is the engine auth secret ("" ⇒ TRUST). Generic over the stream so an in-process
/// duplex can exercise the exact same path in tests.
pub async fn handle_connection<S>(
    mut stream: S,
    session: Arc<WireSession>,
    secret: &str,
) -> std::io::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    // ── 1. PRELOGIN ─────────────────────────────────────────────────────────────
    let Some((_ty, _pre)) = read_message(&mut stream).await? else {
        return Ok(());
    };
    write_tabular(&mut stream, &build_prelogin_response()).await?;

    // ── 2. LOGIN7 + auth ─────────────────────────────────────────────────────────
    let Some((ty, payload)) = read_message(&mut stream).await? else {
        return Ok(());
    };
    if ty != PKT_LOGIN7 {
        let e = WireError {
            code: "08P01".to_owned(),
            message: "expected a LOGIN7 record after PRELOGIN".to_owned(),
        };
        write_tabular(&mut stream, &error_response(&e)).await?;
        return Ok(());
    }
    let login = parse_login7(&payload);
    if !secret.is_empty() {
        let expected = derive_mssql_password(secret, &login.user);
        if login.password != expected {
            let e = WireError {
                code: "28000".to_owned(),
                message: format!("authentication failed for user '{}'", login.user),
            };
            write_tabular(&mut stream, &error_response(&e)).await?;
            return Ok(());
        }
    }
    // Latch the startup identity/graph onto the session (same rules as pgwire): the
    // authenticated user becomes the ACL actor only under an authenticating listener.
    let user = (!login.user.is_empty()).then(|| login.user.clone());
    let database = (!login.database.is_empty()).then(|| login.database.clone());
    session.resolve_startup(user, database);
    // LOGINACK + DONE(final) — the client is now ready to send batches.
    let mut ack = encode_loginack(SERVER_NAME);
    ack.extend(encode_done(DONE_FINAL, 0, 0));
    write_tabular(&mut stream, &ack).await?;

    // ── 3. command phase ──────────────────────────────────────────────────────────
    loop {
        let Some((ty, payload)) = read_message(&mut stream).await? else {
            break; // client closed the connection / logged out
        };
        let response = match ty {
            PKT_SQLBATCH => {
                let sql = decode_sqlbatch(&payload);
                run_batch(&session, &sql).await
            }
            // Attention (cancel): acknowledge with a DONE carrying the attention flag.
            PKT_ATTENTION => encode_done(DONE_FINAL | DONE_ATTN, 0, 0),
            // Transaction-manager envelope (driver BEGIN/COMMIT wrapper): ack no-op.
            PKT_TXMGR => encode_done(DONE_FINAL, 0, 0),
            // RPC / prepared statements are deferred (documented).
            PKT_RPC => {
                let e = WireError {
                    code: "0A000".to_owned(),
                    message: "RPC / prepared statements are not supported over the TDS wire \
                              (use a SQL batch)"
                        .to_owned(),
                };
                error_response(&e)
            }
            other => {
                let e = WireError {
                    code: "08P01".to_owned(),
                    message: format!("unsupported TDS message type {other:#x}"),
                };
                error_response(&e)
            }
        };
        write_tabular(&mut stream, &response).await?;
    }
    Ok(())
}

/// Bind `addr` and serve TDS connections until the process exits. Spawned by `main.rs`
/// only when built `--features mssql-wire` AND `EPISTEMIC_GRAPH_MSSQL_ADDR` is set. The
/// default graph is read once from `EPISTEMIC_GRAPH_MSSQL_GRAPH` (else `__commons__`);
/// TRUST vs authenticated is decided by whether an engine secret is configured.
pub async fn serve(addr: &str, state: Arc<RwLock<ServerState>>) -> std::io::Result<()> {
    let default_graph =
        std::env::var(MSSQL_GRAPH_ENV).unwrap_or_else(|_| "__commons__".to_string());
    let auth_secret = state.read().await.auth_secret.clone();
    let auth_mode = if auth_secret.is_empty() {
        "trust"
    } else {
        "secret-derived"
    };
    let listener = TcpListener::bind(addr).await?;
    tracing::info!(
        "mssql-wire: serving MSSQL TDS wire protocol on {} (default graph '{}', auth={}, \
         encryption=not-supported/plaintext)",
        addr,
        default_graph,
        auth_mode
    );
    loop {
        let (socket, peer) = listener.accept().await?;
        let state = state.clone();
        let default_graph = default_graph.clone();
        let secret = auth_secret.clone();
        tokio::spawn(async move {
            // The authenticated user becomes the ACL actor only under an authenticating
            // (secret-configured) listener; trust stays anonymous.
            let auth_maps_actor = !secret.is_empty();
            let session = Arc::new(WireSession::new(state, default_graph, auth_maps_actor));
            if let Err(e) = handle_connection(socket, session, &secret).await {
                tracing::warn!("mssql-wire connection from {peer} ended with error: {e}");
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn derived_password_is_stable_and_domain_separated() {
        let a = derive_mssql_password("s3cret", "agent:planner");
        assert_eq!(a, derive_mssql_password("s3cret", "agent:planner"));
        assert_ne!(a, derive_mssql_password("other", "agent:planner"));
        assert_ne!(a, derive_mssql_password("s3cret", "agent:worker"));
        assert_eq!(a.len(), 64, "hex of a 32-byte HMAC");
        // Domain separation from pgwire: the "mssql:" prefix differs from "pgwire:".
        assert_ne!(a, {
            let mut mac = HmacSha256::new_from_slice(b"s3cret").unwrap();
            mac.update(b"pgwire:");
            mac.update(b"agent:planner");
            hex::encode(mac.finalize().into_bytes())
        });
    }

    #[test]
    fn col_type_mapping() {
        assert_eq!(map_col_type(PgColType::Int8), TdsType::IntN);
        assert_eq!(map_col_type(PgColType::Float8), TdsType::FloatN);
        assert_eq!(map_col_type(PgColType::Bool), TdsType::BitN);
        assert_eq!(map_col_type(PgColType::Text), TdsType::NVarchar);
        assert_eq!(map_col_type(PgColType::Vector), TdsType::NVarchar);
    }
}
