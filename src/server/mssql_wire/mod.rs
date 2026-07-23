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
//!   2. **LOGIN7** → parse UserName/Password/Database; authenticate the mandatory
//!      secret-derived password; send LOGINACK + DONE.
//!   3. **Command phase** — `SQLBatch` → execute → COLMETADATA + ROW* + DONE (or an
//!      ERROR token + DONE(error)); ATTENTION → DONE(attn); transaction-manager → DONE;
//!      RPC → an ERROR token (RPC/prepared statements are unsupported). A closed
//!      socket ends the loop.
//!
//! ## Auth (CONCEPT:EG-KG.query.concept-13)
//! A TDS `user` maps to an engine `agent_id`, exactly like pgwire. When
//! `GRAPH_SERVICE_AUTH_SECRET` is required and the connection password must equal
//! `hex(HMAC-SHA256(secret, "mssql:" || user))` (an authorized operator computes it
//! offline). The authenticated `user` becomes the ACL actor. The direct plaintext TDS
//! listener is loopback-only; remote clients must traverse an
//! authenticated TLS/mTLS identity-binding gateway into that loopback listener.
//!
//! TLS/encryption is answered `ENCRYPT_NOT_SUP`; RPC/prepared `sp_executesql`, MARS,
//! and NVARCHAR(MAX)/PLP chunking are unsupported. See `protocol.rs`.

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

/// Verify a LOGIN7 principal/password without a timing-sensitive string comparison.
/// The credential is the hex encoding returned by [`derive_mssql_password`].
pub fn verify_mssql_login(secret: &str, user: &str, password: &str) -> bool {
    if secret.is_empty() || user.is_empty() || user.len() > 4 * 1024 || password.len() != 64 {
        return false;
    }
    let Ok(candidate) = hex::decode(password) else {
        return false;
    };
    let mut mac =
        HmacSha256::new_from_slice(secret.as_bytes()).expect("HMAC accepts any key length");
    mac.update(b"mssql:");
    mac.update(user.as_bytes());
    mac.verify_slice(&candidate).is_ok()
}

/// Fail closed before binding the direct TDS listener. TDS encryption
/// is not implemented here, so the only safe direct bind is authenticated loopback.
pub fn validate_startup_policy(addr: &str, secret: &str) -> std::io::Result<()> {
    crate::server::validate_direct_wire_security(addr, "mssql-wire", !secret.is_empty())
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
    const MAX_TDS_MESSAGE_BYTES: usize = 64 * 1024 * 1024;
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
            let end = start.checked_add(body_len).ok_or_else(|| {
                std::io::Error::new(std::io::ErrorKind::InvalidData, "TDS message size overflow")
            })?;
            if end > MAX_TDS_MESSAGE_BYTES {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "TDS message exceeds the resource limit",
                ));
            }
            payload.resize(end, 0);
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
/// is the required engine auth secret. Generic over the stream so an in-process
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
    if !verify_mssql_login(secret, &login.user, &login.password) {
        let e = WireError {
            code: "28000".to_owned(),
            message: "authentication failed".to_owned(),
        };
        write_tabular(&mut stream, &error_response(&e)).await?;
        return Ok(());
    }
    // Latch the startup identity/graph onto the session (same rules as pgwire): the
    // authenticated user becomes the ACL actor.
    let user = (!login.user.is_empty()).then(|| login.user.clone());
    let database = (!login.database.is_empty()).then(|| login.database.clone());
    session.resolve_startup(user, database);
    if let Err(error) = session
        .bind_authenticated_sql_actor("mssql-wire", &login.user)
        .await
    {
        write_tabular(&mut stream, &error_response(&error)).await?;
        return Ok(());
    }
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
            // RPC / prepared statements are unsupported and fail closed.
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
/// Authentication is mandatory and startup fails when the engine secret is absent.
pub async fn serve(addr: &str, state: Arc<RwLock<ServerState>>) -> std::io::Result<()> {
    let default_graph =
        std::env::var(MSSQL_GRAPH_ENV).unwrap_or_else(|_| "__commons__".to_string());
    let (auth_secret, persist_dir) = {
        let state = state.read().await;
        (state.auth_secret.clone(), state.persist_dir.clone())
    };
    validate_startup_policy(addr, &auth_secret)?;
    crate::server::sql_tables::validate_served_configuration(
        persist_dir.as_deref().map(std::path::Path::new),
    )?;
    let listener = TcpListener::bind(addr).await?;
    tracing::info!(
        "mssql-wire: serving authenticated MSSQL TDS protocol on loopback \
         (default graph '{}'; remote access requires a TLS identity-binding gateway)",
        default_graph
    );
    loop {
        let (socket, peer) = listener.accept().await?;
        let state = state.clone();
        let default_graph = default_graph.clone();
        let secret = auth_secret.clone();
        tokio::spawn(async move {
            let session = Arc::new(WireSession::new(state, default_graph));
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
    fn login_verification_is_fail_closed() {
        let password = derive_mssql_password("s3cret", "agent:planner");
        assert!(verify_mssql_login("s3cret", "agent:planner", &password));
        assert!(!verify_mssql_login("", "agent:planner", &password));
        assert!(!verify_mssql_login("s3cret", "", &password));
        assert!(!verify_mssql_login("s3cret", "agent:planner", "not-hex"));
        assert!(!verify_mssql_login("s3cret", "agent:worker", &password));
    }

    #[test]
    fn startup_policy_rejects_anonymous_or_remote_tds() {
        assert!(validate_startup_policy("127.0.0.1:1433", "").is_err());
        assert!(validate_startup_policy("0.0.0.0:1433", "s3cret").is_err());
        assert!(validate_startup_policy("127.0.0.1:1433", "s3cret").is_ok());
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
