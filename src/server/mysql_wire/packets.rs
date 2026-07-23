//! Hand-rolled MySQL client/server protocol framing + packet codecs (CONCEPT:EG-KG.query.kg-2).
//!
//! This is the wire-SPECIFIC half of the MySQL adapter: the length-prefixed packet
//! framing, the length-encoded integer/string primitives, the handshake v10 +
//! handshake-response codecs, and the OK / ERR / column-count / column-definition
//! / text-row encoders. It links NO mysql protocol crate — every byte layout is written
//! by hand against the documented MySQL protocol (matching the pgwire/sparql_http
//! Pi-contract idiom of framing directly over `tokio::net`).
//!
//! Framing: every packet is `[3-byte LE payload length][1-byte sequence id][payload]`.
//! The async read/write of a framed packet lives in `mod.rs` (over `tokio::io`); this
//! module builds/parses the PAYLOADS.

use eg_query::PgColType;

// ── capability flags (the subset this server negotiates) ──────────────────────
pub const CLIENT_LONG_PASSWORD: u32 = 0x0000_0001;
pub const CLIENT_CONNECT_WITH_DB: u32 = 0x0000_0008;
pub const CLIENT_PROTOCOL_41: u32 = 0x0000_0200;
pub const CLIENT_TRANSACTIONS: u32 = 0x0000_2000;
pub const CLIENT_SECURE_CONNECTION: u32 = 0x0000_8000;
pub const CLIENT_PLUGIN_AUTH: u32 = 0x0008_0000;
pub const CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA: u32 = 0x0020_0000;
pub const CLIENT_DEPRECATE_EOF: u32 = 0x0100_0000;

// ── server status flags ───────────────────────────────────────────────────────
pub const SERVER_STATUS_IN_TRANS: u16 = 0x0001;
pub const SERVER_STATUS_AUTOCOMMIT: u16 = 0x0002;

// ── MySQL column type codes (text protocol renders everything as strings, so these
// are cosmetic — but a sane mapping keeps type-introspecting clients happy). ──────
pub const MYSQL_TYPE_TINY: u8 = 0x01;
pub const MYSQL_TYPE_DOUBLE: u8 = 0x05;
pub const MYSQL_TYPE_LONGLONG: u8 = 0x08;
pub const MYSQL_TYPE_VAR_STRING: u8 = 0xfd;

/// utf8mb4 collation id (utf8mb4_general_ci) — advertised in the handshake + column defs.
pub const COLLATION_UTF8MB4: u8 = 45;

// ── length-encoded integer / string primitives ────────────────────────────────

/// Append a MySQL length-encoded integer.
pub fn put_lenenc_int(out: &mut Vec<u8>, v: u64) {
    if v < 0xfb {
        out.push(v as u8);
    } else if v <= 0xffff {
        out.push(0xfc);
        out.extend_from_slice(&(v as u16).to_le_bytes());
    } else if v <= 0xff_ffff {
        out.push(0xfd);
        out.extend_from_slice(&(v as u32).to_le_bytes()[..3]);
    } else {
        out.push(0xfe);
        out.extend_from_slice(&v.to_le_bytes());
    }
}

/// Append a MySQL length-encoded string (lenenc length + raw bytes).
pub fn put_lenenc_str(out: &mut Vec<u8>, s: &[u8]) {
    put_lenenc_int(out, s.len() as u64);
    out.extend_from_slice(s);
}

/// Read a length-encoded integer at `*pos`, advancing it. Returns `None` on truncation
/// or an `0xfb` (NULL) sentinel where an integer is required.
pub fn get_lenenc_int(buf: &[u8], pos: &mut usize) -> Option<u64> {
    let first = *buf.get(*pos)?;
    *pos += 1;
    match first {
        0xfb => None, // NULL where an int is required
        0xfc => {
            let b = buf.get(*pos..*pos + 2)?;
            *pos += 2;
            Some(u16::from_le_bytes([b[0], b[1]]) as u64)
        }
        0xfd => {
            let b = buf.get(*pos..*pos + 3)?;
            *pos += 3;
            Some(u32::from_le_bytes([b[0], b[1], b[2], 0]) as u64)
        }
        0xfe => {
            let b = buf.get(*pos..*pos + 8)?;
            *pos += 8;
            Some(u64::from_le_bytes(b.try_into().ok()?))
        }
        n => Some(n as u64),
    }
}

/// Read a NUL-terminated string at `*pos`, advancing past the NUL. `None` if no NUL.
fn get_nul_string(buf: &[u8], pos: &mut usize) -> Option<String> {
    let start = *pos;
    let rel = buf[start..].iter().position(|&b| b == 0)?;
    let s = String::from_utf8_lossy(&buf[start..start + rel]).into_owned();
    *pos = start + rel + 1;
    Some(s)
}

// ── PgColType → MySQL column type code ─────────────────────────────────────────

/// Map the engine's typed-column kind to a MySQL column-type code for the
/// column-definition packet. The text protocol serializes every value as a string, so
/// the exact code is cosmetic; a reasonable mapping keeps type-introspecting clients
/// (and ORMs' schema reflection) sane.
pub fn mysql_col_type(t: PgColType) -> u8 {
    match t {
        PgColType::Int8 => MYSQL_TYPE_LONGLONG,
        PgColType::Float8 => MYSQL_TYPE_DOUBLE,
        PgColType::Bool => MYSQL_TYPE_TINY,
        // Text and Vector both render as a variable-length string.
        PgColType::Text | PgColType::Vector => MYSQL_TYPE_VAR_STRING,
    }
}

/// Render one decoded JSON cell as its MySQL text-protocol string form (mirrors the
/// pgwire `encode_cell` shape but always produces a string, as the text protocol
/// requires). A NULL is signalled by the row encoder (see [`put_text_row`]), never here.
pub fn cell_to_text(ty: PgColType, cell: &serde_json::Value) -> String {
    use serde_json::Value;
    match ty {
        PgColType::Bool => match cell.as_bool() {
            // MySQL renders BOOL/TINYINT(1) as 1/0 in the text protocol.
            Some(true) => "1".to_string(),
            Some(false) => "0".to_string(),
            None => scalar_text(cell),
        },
        PgColType::Vector => match cell {
            Value::Array(items) => {
                let parts: Vec<String> = items
                    .iter()
                    .map(|v| match v {
                        Value::Number(n) => n.to_string(),
                        Value::Null => "0".to_string(),
                        other => scalar_text(other),
                    })
                    .collect();
                format!("[{}]", parts.join(","))
            }
            other => scalar_text(other),
        },
        _ => scalar_text(cell),
    }
}

/// A scalar JSON value's canonical text: strings pass through unquoted; numbers/bools
/// render canonically; structural values are JSON-stringified.
fn scalar_text(cell: &serde_json::Value) -> String {
    use serde_json::Value;
    match cell {
        Value::String(s) => s.clone(),
        Value::Bool(b) => {
            if *b {
                "1".to_string()
            } else {
                "0".to_string()
            }
        }
        Value::Null => String::new(),
        Value::Number(n) => n.to_string(),
        other => other.to_string(),
    }
}

// ── packet builders (server → client) ─────────────────────────────────────────

/// Build a Handshake v10 packet payload (CONCEPT:EG-KG.query.kg-2). Advertises the negotiated
/// server capabilities, the utf8mb4 collation, a 20-byte `seed` (split 8 + 12 + NUL as
/// the protocol requires), and the `mysql_native_password` auth plugin name.
pub fn build_handshake(
    conn_id: u32,
    seed: &[u8; 20],
    server_caps: u32,
    server_version: &str,
) -> Vec<u8> {
    let mut p = Vec::with_capacity(64);
    p.push(10); // protocol version
    p.extend_from_slice(server_version.as_bytes());
    p.push(0); // NUL-terminated server version
    p.extend_from_slice(&conn_id.to_le_bytes());
    // auth-plugin-data-part-1: first 8 bytes of the scramble.
    p.extend_from_slice(&seed[..8]);
    p.push(0); // filler
               // capability flags — lower 2 bytes.
    p.extend_from_slice(&(server_caps as u16).to_le_bytes());
    p.push(COLLATION_UTF8MB4);
    // status flags (autocommit at connect).
    p.extend_from_slice(&SERVER_STATUS_AUTOCOMMIT.to_le_bytes());
    // capability flags — upper 2 bytes.
    p.extend_from_slice(&((server_caps >> 16) as u16).to_le_bytes());
    // length of the whole auth-plugin-data (21 = 20 scramble bytes + a NUL).
    p.push(21);
    p.extend_from_slice(&[0u8; 10]); // reserved
                                     // auth-plugin-data-part-2: the remaining 12 scramble bytes + a NUL terminator.
    p.extend_from_slice(&seed[8..20]);
    p.push(0);
    // auth plugin name.
    p.extend_from_slice(super::auth::MYSQL_NATIVE_PASSWORD.as_bytes());
    p.push(0);
    p
}

/// Build an OK packet payload. `status` carries the current server status flags (e.g.
/// in-transaction) so a driver tracks the transaction state.
pub fn build_ok(affected_rows: u64, last_insert_id: u64, status: u16, info: &str) -> Vec<u8> {
    let mut p = Vec::with_capacity(16 + info.len());
    p.push(0x00); // OK header
    put_lenenc_int(&mut p, affected_rows);
    put_lenenc_int(&mut p, last_insert_id);
    // CLIENT_PROTOCOL_41 is always negotiated: status flags + warning count.
    p.extend_from_slice(&status.to_le_bytes());
    p.extend_from_slice(&0u16.to_le_bytes()); // warnings
    if !info.is_empty() {
        p.extend_from_slice(info.as_bytes());
    }
    p
}

/// Build an ERR packet payload with a MySQL error code + a 5-char SQLSTATE (prefixed by
/// the `#` marker per the CLIENT_PROTOCOL_41 layout) + the message.
pub fn build_err(code: u16, sqlstate: &str, message: &str) -> Vec<u8> {
    let mut p = Vec::with_capacity(16 + message.len());
    p.push(0xff); // ERR header
    p.extend_from_slice(&code.to_le_bytes());
    p.push(b'#'); // SQLSTATE marker
                  // Exactly 5 SQLSTATE chars (right-pad / truncate defensively).
    let mut ss = [b'0'; 5];
    for (i, b) in sqlstate.bytes().take(5).enumerate() {
        ss[i] = b;
    }
    p.extend_from_slice(&ss);
    p.extend_from_slice(message.as_bytes());
    p
}

/// Build the result-set terminator required when `CLIENT_DEPRECATE_EOF` is
/// negotiated. This is the protocol-41 OK shape with the `0xfe` result-set header,
/// two zero length-encoded counters, status flags, and warning count.
pub fn build_resultset_end(status: u16) -> Vec<u8> {
    let mut p = Vec::with_capacity(7);
    p.push(0xfe); // OK/result-set terminator header
    put_lenenc_int(&mut p, 0); // affected rows
    put_lenenc_int(&mut p, 0); // last insert id
    p.extend_from_slice(&status.to_le_bytes());
    p.extend_from_slice(&0u16.to_le_bytes()); // warnings
    p
}

/// Build the column-count packet payload (a bare length-encoded integer).
pub fn build_column_count(n: u64) -> Vec<u8> {
    let mut p = Vec::with_capacity(4);
    put_lenenc_int(&mut p, n);
    p
}

/// Build a Column-Definition (protocol 41) packet payload for one result column.
pub fn build_column_def(name: &str, ty: PgColType) -> Vec<u8> {
    let mut p = Vec::with_capacity(48);
    put_lenenc_str(&mut p, b"def"); // catalog
    put_lenenc_str(&mut p, b""); // schema
    put_lenenc_str(&mut p, b""); // table
    put_lenenc_str(&mut p, b""); // org_table
    put_lenenc_str(&mut p, name.as_bytes()); // name
    put_lenenc_str(&mut p, name.as_bytes()); // org_name
    put_lenenc_int(&mut p, 0x0c); // length of the fixed-length fields block (12)
    p.extend_from_slice(&(COLLATION_UTF8MB4 as u16).to_le_bytes());
    // Generous column length (cosmetic in the text protocol).
    p.extend_from_slice(&65535u32.to_le_bytes());
    p.push(mysql_col_type(ty));
    p.extend_from_slice(&0u16.to_le_bytes()); // flags (nullable)
    p.push(0); // decimals
    p.extend_from_slice(&0u16.to_le_bytes()); // filler
    p
}

/// Build a text-protocol result row payload. Each cell is a length-encoded string; a
/// JSON `null` cell is the single `0xfb` NULL byte.
pub fn put_text_row(types: &[PgColType], row: &[serde_json::Value]) -> Vec<u8> {
    let mut p = Vec::with_capacity(16 * row.len());
    for (i, cell) in row.iter().enumerate() {
        if cell.is_null() {
            p.push(0xfb); // NULL
            continue;
        }
        let ty = types.get(i).copied().unwrap_or(PgColType::Text);
        put_lenenc_str(&mut p, cell_to_text(ty, cell).as_bytes());
    }
    p
}

// ── handshake response (client → server) ───────────────────────────────────────

/// The parsed Handshake-Response-41 sent by the client.
#[derive(Debug, Clone)]
pub struct HandshakeResponse {
    /// The client's negotiated capability flags.
    pub capabilities: u32,
    /// The connecting user name.
    pub username: String,
    /// The `mysql_native_password` auth response bytes (the 20-byte scramble, or empty).
    pub auth_response: Vec<u8>,
    /// The database (graph) the client asked to connect to, if CLIENT_CONNECT_WITH_DB.
    pub database: Option<String>,
    /// The auth plugin name the client used, if CLIENT_PLUGIN_AUTH.
    pub auth_plugin: Option<String>,
}

/// Parse a Handshake-Response-41 payload (CONCEPT:EG-KG.query.kg-2). Returns `None` on a
/// truncated / pre-4.1 packet (the server then rejects the connection cleanly).
pub fn parse_handshake_response(buf: &[u8]) -> Option<HandshakeResponse> {
    let mut pos = 0usize;
    let caps = u32::from_le_bytes(buf.get(0..4)?.try_into().ok()?);
    pos += 4;
    // A 4.1+ response is required (protocol 41).
    if caps & CLIENT_PROTOCOL_41 == 0 {
        return None;
    }
    pos += 4; // max packet size
    pos += 1; // character set
    pos += 23; // reserved
    let username = get_nul_string(buf, &mut pos)?;

    // Auth response: three possible encodings depending on negotiated caps.
    let auth_response: Vec<u8> = if caps & CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA != 0 {
        let len = get_lenenc_int(buf, &mut pos)? as usize;
        let bytes = buf.get(pos..pos + len)?.to_vec();
        pos += len;
        bytes
    } else if caps & CLIENT_SECURE_CONNECTION != 0 {
        let len = *buf.get(pos)? as usize;
        pos += 1;
        let bytes = buf.get(pos..pos + len)?.to_vec();
        pos += len;
        bytes
    } else {
        // NUL-terminated (very old clients).
        let s = get_nul_string(buf, &mut pos)?;
        s.into_bytes()
    };

    let database = if caps & CLIENT_CONNECT_WITH_DB != 0 {
        get_nul_string(buf, &mut pos)
    } else {
        None
    };
    let auth_plugin = if caps & CLIENT_PLUGIN_AUTH != 0 {
        get_nul_string(buf, &mut pos)
    } else {
        None
    };

    Some(HandshakeResponse {
        capabilities: caps,
        username,
        auth_response,
        database,
        auth_plugin,
    })
}

/// Build a Handshake-Response-41 payload — used by the in-process smoke-test client to
/// drive the server through a real handshake without pulling a mysql client crate.
#[cfg(test)]
pub fn build_handshake_response(
    capabilities: u32,
    username: &str,
    auth_response: &[u8],
    database: Option<&str>,
) -> Vec<u8> {
    let mut p = Vec::with_capacity(64);
    p.extend_from_slice(&capabilities.to_le_bytes());
    p.extend_from_slice(&0x0100_0000u32.to_le_bytes()); // max packet size (16 MiB)
    p.push(COLLATION_UTF8MB4);
    p.extend_from_slice(&[0u8; 23]); // reserved
    p.extend_from_slice(username.as_bytes());
    p.push(0);
    if capabilities & CLIENT_SECURE_CONNECTION != 0 {
        p.push(auth_response.len() as u8);
        p.extend_from_slice(auth_response);
    } else {
        p.extend_from_slice(auth_response);
        p.push(0);
    }
    if let Some(db) = database {
        p.extend_from_slice(db.as_bytes());
        p.push(0);
    }
    if capabilities & CLIENT_PLUGIN_AUTH != 0 {
        p.extend_from_slice(super::auth::MYSQL_NATIVE_PASSWORD.as_bytes());
        p.push(0);
    }
    p
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lenenc_int_roundtrip_across_widths() {
        for v in [
            0u64,
            1,
            250,
            251,
            0xff,
            0x1234,
            0xffff,
            0x1_0000,
            0xff_ffff,
            0x100_0000,
            u32::MAX as u64,
            u64::MAX,
        ] {
            let mut buf = Vec::new();
            put_lenenc_int(&mut buf, v);
            let mut pos = 0;
            assert_eq!(get_lenenc_int(&buf, &mut pos), Some(v), "roundtrip {v}");
            assert_eq!(pos, buf.len(), "consumed all bytes for {v}");
        }
    }

    #[test]
    fn lenenc_int_byte_layout_is_canonical() {
        // Documented MySQL prefix bytes.
        let mut b = Vec::new();
        put_lenenc_int(&mut b, 10);
        assert_eq!(b, vec![10]);
        b.clear();
        put_lenenc_int(&mut b, 300);
        assert_eq!(b, vec![0xfc, 0x2c, 0x01]); // 300 = 0x012c LE
        b.clear();
        put_lenenc_int(&mut b, 0x123456);
        assert_eq!(b, vec![0xfd, 0x56, 0x34, 0x12]);
    }

    #[test]
    fn ok_packet_layout() {
        let p = build_ok(3, 0, SERVER_STATUS_AUTOCOMMIT, "");
        assert_eq!(p[0], 0x00, "OK header");
        // affected_rows=3 (1 byte), last_insert_id=0 (1 byte), then status LE.
        assert_eq!(p[1], 3);
        assert_eq!(p[2], 0);
        assert_eq!(&p[3..5], &SERVER_STATUS_AUTOCOMMIT.to_le_bytes());
        assert_eq!(&p[5..7], &[0, 0], "warnings");
    }

    #[test]
    fn resultset_end_uses_current_ok_shape() {
        let p = build_resultset_end(SERVER_STATUS_AUTOCOMMIT);
        assert_eq!(p[0], 0xfe);
        assert_eq!(&p[1..3], &[0, 0]);
        assert_eq!(&p[3..5], &SERVER_STATUS_AUTOCOMMIT.to_le_bytes());
        assert_eq!(&p[5..7], &[0, 0]);
    }

    #[test]
    fn err_packet_carries_code_and_sqlstate() {
        let p = build_err(1064, "42000", "boom");
        assert_eq!(p[0], 0xff);
        assert_eq!(u16::from_le_bytes([p[1], p[2]]), 1064);
        assert_eq!(p[3], b'#');
        assert_eq!(&p[4..9], b"42000");
        assert_eq!(&p[9..], b"boom");
    }

    #[test]
    fn column_def_and_text_row_encode() {
        let def = build_column_def("id", PgColType::Text);
        // catalog "def" is the first lenenc string.
        assert_eq!(def[0], 3);
        assert_eq!(&def[1..4], b"def");

        let types = [PgColType::Text, PgColType::Int8, PgColType::Bool];
        let row = vec![
            serde_json::json!("alice"),
            serde_json::json!(42),
            serde_json::Value::Null,
        ];
        let enc = put_text_row(&types, &row);
        let mut pos = 0;
        // "alice"
        let l = get_lenenc_int(&enc, &mut pos).unwrap() as usize;
        assert_eq!(&enc[pos..pos + l], b"alice");
        pos += l;
        // "42"
        let l = get_lenenc_int(&enc, &mut pos).unwrap() as usize;
        assert_eq!(&enc[pos..pos + l], b"42");
        pos += l;
        // NULL byte
        assert_eq!(enc[pos], 0xfb);
    }

    #[test]
    fn bool_and_vector_cells_render() {
        assert_eq!(cell_to_text(PgColType::Bool, &serde_json::json!(true)), "1");
        assert_eq!(
            cell_to_text(PgColType::Bool, &serde_json::json!(false)),
            "0"
        );
        assert_eq!(
            cell_to_text(PgColType::Vector, &serde_json::json!([1, 2, 3])),
            "[1,2,3]"
        );
        assert_eq!(mysql_col_type(PgColType::Int8), MYSQL_TYPE_LONGLONG);
        assert_eq!(mysql_col_type(PgColType::Float8), MYSQL_TYPE_DOUBLE);
    }

    #[test]
    fn handshake_and_response_roundtrip() {
        let seed: [u8; 20] = core::array::from_fn(|i| (i as u8).wrapping_add(1));
        let caps = CLIENT_PROTOCOL_41
            | CLIENT_SECURE_CONNECTION
            | CLIENT_PLUGIN_AUTH
            | CLIENT_CONNECT_WITH_DB
            | CLIENT_DEPRECATE_EOF;
        let hs = build_handshake(7, &seed, caps, "8.0.0-epistemic-graph");
        assert_eq!(hs[0], 10, "protocol version 10");

        let scramble: Vec<u8> = (0u8..20).collect();
        let resp = build_handshake_response(caps, "agent:planner", &scramble, Some("mygraph"));
        let parsed = parse_handshake_response(&resp).expect("parse");
        assert_eq!(parsed.username, "agent:planner");
        assert_eq!(parsed.auth_response, scramble);
        assert_eq!(parsed.database.as_deref(), Some("mygraph"));
        assert_eq!(
            parsed.auth_plugin.as_deref(),
            Some(super::super::auth::MYSQL_NATIVE_PASSWORD)
        );
    }
}
