//! Hand-rolled MSSQL **TDS** (Tabular Data Stream) framing + token codec
//! (CONCEPT:EG-077). PURE bytes — no sockets, no async, no engine coupling — so every
//! layout is unit-testable against the MS-TDS wire spec. `mod.rs` drives these over a
//! `tokio` `TcpListener` and bridges the decoded `SQLBatch` into the shared
//! [`crate::server::wire::WireSession`], then encodes the wire-neutral
//! [`crate::server::wire::WireOutcome`] back into the token stream built here.
//!
//! ## TDS subset implemented (v1)
//!   * **Packet framing** — the 8-byte header (type / status / length(BE) / SPID /
//!     PacketID / Window) + multi-packet reassembly + EOM chunking.
//!   * **PRELOGIN** — parse the option list; respond with VERSION + ENCRYPTION =
//!     `ENCRYPT_NOT_SUP` (0x02) so the session stays PLAINTEXT (no TLS in v1 —
//!     documented; TLS is deferred).
//!   * **LOGIN7** — parse the fixed header's offset/length table for UserName,
//!     Password (deobfuscated), and Database.
//!   * **SQLBatch** — skip the optional `ALL_HEADERS` block, decode the UCS-2
//!     (UTF-16LE) SQL text.
//!   * **Response tokens** — LOGINACK, COLMETADATA (INTN / FLTN / BITN / NVARCHAR
//!     TYPE_INFO), ROW, DONE (+ error flag), and ERROR.
//!
//! ## Deferred (documented, NOT implemented)
//!   * TLS / encryption (we answer `ENCRYPT_NOT_SUP`), RPC (0x03) / prepared
//!     `sp_executesql`, and MARS. The command phase rejects RPC with an ERROR token.

use serde_json::Value;

// ── packet (message) types (client → server, and 0x04 server → client) ─────────

/// SQL batch message (the SQL text, UCS-2). Client → server.
pub const PKT_SQLBATCH: u8 = 0x01;
/// Remote-procedure call. Client → server (v1: rejected).
pub const PKT_RPC: u8 = 0x03;
/// Tabular result. The type of EVERY server → client packet (prelogin reply, login
/// ack, and query token streams).
pub const PKT_TABULAR: u8 = 0x04;
/// Attention (cancel) signal. Client → server.
pub const PKT_ATTENTION: u8 = 0x06;
/// LOGIN7 login record. Client → server.
pub const PKT_LOGIN7: u8 = 0x10;
/// PRELOGIN handshake. Client → server.
pub const PKT_PRELOGIN: u8 = 0x12;
/// Transaction-manager request (e.g. driver-issued BEGIN/COMMIT envelope). Client →
/// server (v1: acked as a no-op DONE).
pub const PKT_TXMGR: u8 = 0x0E;

/// The status bit set on the FINAL packet of a message (End Of Message).
pub const STATUS_EOM: u8 = 0x01;

/// The 8-byte TDS packet header length.
pub const HEADER_LEN: usize = 8;
/// The default negotiated packet size (bytes) — response chunking uses this.
pub const DEFAULT_PACKET_SIZE: usize = 4096;

// ── response token type bytes ──────────────────────────────────────────────────

pub const TOKEN_COLMETADATA: u8 = 0x81;
pub const TOKEN_ROW: u8 = 0xD1;
pub const TOKEN_ERROR: u8 = 0xAA;
pub const TOKEN_LOGINACK: u8 = 0xAD;
pub const TOKEN_DONE: u8 = 0xFD;

// ── DONE status flags ──────────────────────────────────────────────────────────

pub const DONE_FINAL: u16 = 0x0000;
pub const DONE_ERROR: u16 = 0x0002;
pub const DONE_COUNT: u16 = 0x0010;
pub const DONE_ATTN: u16 = 0x0020;

// ── TDS data-type tokens used in COLMETADATA TYPE_INFO ─────────────────────────

pub const TYPE_INTN: u8 = 0x26;
pub const TYPE_FLTN: u8 = 0x6D;
pub const TYPE_BITN: u8 = 0x68;
pub const TYPE_NVARCHAR: u8 = 0xE7;

/// The TDS protocol version reported to the client (TDS 7.4 = `0x74000004`), in the
/// big-endian on-wire byte order the LOGINACK token carries.
pub const TDS_VERSION_BYTES: [u8; 4] = [0x74, 0x00, 0x00, 0x04];

/// The largest NVARCHAR value length we emit inline (bytes). `0xFFFF` is the NULL
/// sentinel, so a real value tops out at `0xFFFE` bytes; longer text is truncated on
/// an even (UTF-16 code-unit) boundary — a documented v1 limitation (NVARCHAR(MAX) /
/// PLP chunking is deferred).
const NVARCHAR_MAX_BYTES: usize = 0xFFFE;

/// The engine's mapped TDS column type — the sane subset of TYPE_INFO codes we emit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TdsType {
    /// `INTN` (nullable integer), emitted at max length 8 (i64).
    IntN,
    /// `FLTN` (nullable float), emitted at max length 8 (f64).
    FloatN,
    /// `BITN` (nullable bit / boolean), max length 1.
    BitN,
    /// `NVARCHAR` — the catch-all for text (and anything awkward to type precisely;
    /// the value is JSON-stringified). Documented in the type-mapping note.
    NVarchar,
}

// ── UTF-16LE (UCS-2) helpers ────────────────────────────────────────────────────

/// Encode a `str` to UTF-16LE bytes (the UCS-2 form TDS strings use on the wire).
pub fn utf16le_bytes(s: &str) -> Vec<u8> {
    s.encode_utf16().flat_map(|u| u.to_le_bytes()).collect()
}

/// Decode UTF-16LE bytes to a String (lossy on unpaired surrogates). A trailing odd
/// byte is ignored.
pub fn utf16le_to_string(bytes: &[u8]) -> String {
    let units: Vec<u16> = bytes
        .chunks_exact(2)
        .map(|c| u16::from_le_bytes([c[0], c[1]]))
        .collect();
    String::from_utf16_lossy(&units)
}

// ── packet framing ───────────────────────────────────────────────────────────────

/// A parsed TDS packet header.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Header {
    pub ty: u8,
    pub status: u8,
    /// Total packet length INCLUDING the 8-byte header (the on-wire big-endian field).
    pub length: u16,
}

impl Header {
    /// Whether this is the final packet of a message (EOM status bit set).
    pub fn is_eom(&self) -> bool {
        self.status & STATUS_EOM != 0
    }
    /// The body length (payload after the 8-byte header). Saturates if the field is
    /// malformed (< header length).
    pub fn body_len(&self) -> usize {
        (self.length as usize).saturating_sub(HEADER_LEN)
    }
}

/// Parse an 8-byte packet header (type, status, length; SPID/PacketID/Window ignored).
pub fn parse_header(buf: &[u8; HEADER_LEN]) -> Header {
    Header {
        ty: buf[0],
        status: buf[1],
        length: u16::from_be_bytes([buf[2], buf[3]]),
    }
}

/// Frame a full message payload into one or more TDS packets of type `ty`, splitting
/// at `DEFAULT_PACKET_SIZE` and setting the EOM bit on the last packet. An empty
/// payload yields a single empty EOM packet (used for a bare DONE-less ack path).
pub fn frame_message(ty: u8, payload: &[u8]) -> Vec<u8> {
    let max_data = DEFAULT_PACKET_SIZE - HEADER_LEN;
    let mut out = Vec::with_capacity(payload.len() + HEADER_LEN);
    let mut pkt_id: u8 = 1;
    if payload.is_empty() {
        push_header(&mut out, ty, STATUS_EOM, HEADER_LEN as u16, pkt_id);
        return out;
    }
    let mut chunks = payload.chunks(max_data).peekable();
    while let Some(chunk) = chunks.next() {
        let status = if chunks.peek().is_none() {
            STATUS_EOM
        } else {
            0x00
        };
        let length = (chunk.len() + HEADER_LEN) as u16;
        push_header(&mut out, ty, status, length, pkt_id);
        out.extend_from_slice(chunk);
        pkt_id = pkt_id.wrapping_add(1);
    }
    out
}

fn push_header(out: &mut Vec<u8>, ty: u8, status: u8, length: u16, pkt_id: u8) {
    out.push(ty);
    out.push(status);
    out.extend_from_slice(&length.to_be_bytes()); // length: BIG-endian
    out.extend_from_slice(&0u16.to_be_bytes()); // SPID (BE, unused → 0)
    out.push(pkt_id);
    out.push(0x00); // Window (unused)
}

// ── PRELOGIN ─────────────────────────────────────────────────────────────────────

const PRELOGIN_VERSION: u8 = 0x00;
const PRELOGIN_ENCRYPTION: u8 = 0x01;
const PRELOGIN_TERMINATOR: u8 = 0xFF;
/// ENCRYPT_NOT_SUP — the server does not support/require TLS (v1 plaintext).
pub const ENCRYPT_NOT_SUP: u8 = 0x02;

/// Build the PRELOGIN response: a VERSION option + an ENCRYPTION option answering
/// `ENCRYPT_NOT_SUP` (plaintext, v1). Layout: an option table (token + 2-byte BE
/// offset + 2-byte BE length per entry, `0xFF` terminator), then the option data.
pub fn build_prelogin_response() -> Vec<u8> {
    // Table = VERSION(5) + ENCRYPTION(5) + terminator(1) = 11 bytes; data follows.
    let ver_off: u16 = 11;
    let enc_off: u16 = ver_off + 6;
    let mut out = Vec::new();
    // VERSION option-table entry.
    out.push(PRELOGIN_VERSION);
    out.extend_from_slice(&ver_off.to_be_bytes());
    out.extend_from_slice(&6u16.to_be_bytes());
    // ENCRYPTION option-table entry.
    out.push(PRELOGIN_ENCRYPTION);
    out.extend_from_slice(&enc_off.to_be_bytes());
    out.extend_from_slice(&1u16.to_be_bytes());
    // Terminator.
    out.push(PRELOGIN_TERMINATOR);
    // VERSION data: UL_VERSION (major.minor.build) + US_SUBBUILD.
    out.extend_from_slice(&[16, 0, 0, 0]);
    out.extend_from_slice(&0u16.to_be_bytes());
    // ENCRYPTION data.
    out.push(ENCRYPT_NOT_SUP);
    out
}

/// Best-effort parse of the client's requested ENCRYPTION byte from a PRELOGIN option
/// list, if present. Used only for logging/diagnostics — v1 always answers NOT_SUP.
pub fn parse_prelogin_encryption(payload: &[u8]) -> Option<u8> {
    let mut i = 0usize;
    while i + 5 <= payload.len() {
        let token = payload[i];
        if token == PRELOGIN_TERMINATOR {
            break;
        }
        let off = u16::from_be_bytes([payload[i + 1], payload[i + 2]]) as usize;
        let len = u16::from_be_bytes([payload[i + 3], payload[i + 4]]) as usize;
        if token == PRELOGIN_ENCRYPTION && len >= 1 && off < payload.len() {
            return Some(payload[off]);
        }
        i += 5;
    }
    None
}

// ── LOGIN7 ─────────────────────────────────────────────────────────────────────

/// The identity fields parsed out of a LOGIN7 record.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Login7 {
    pub user: String,
    pub password: String,
    pub database: String,
}

/// The LOGIN7 fixed-header length: 32 bytes of scalars + the 62-byte offset/length
/// table = 94 bytes before the variable data region.
const LOGIN7_FIXED_LEN: usize = 94;

/// Parse a LOGIN7 record's UserName / Password / Database. Offsets in the table are
/// relative to the START of the record (byte 0 = the Length DWORD); `cch*` fields are
/// UTF-16 code-unit COUNTS (byte length = count * 2). The password is deobfuscated.
/// Returns an empty field for any offset/length that falls outside the record.
pub fn parse_login7(payload: &[u8]) -> Login7 {
    if payload.len() < LOGIN7_FIXED_LEN {
        return Login7::default();
    }
    let rd_u16 = |off: usize| -> u16 { u16::from_le_bytes([payload[off], payload[off + 1]]) };
    let slice = |ib_off: usize, cch_off: usize| -> &[u8] {
        let ib = rd_u16(ib_off) as usize;
        let cch = rd_u16(cch_off) as usize;
        let byte_len = cch * 2;
        if ib.checked_add(byte_len).map(|e| e <= payload.len()) == Some(true) {
            &payload[ib..ib + byte_len]
        } else {
            &[]
        }
    };
    // Offset/length table entries (see MS-TDS LOGIN7): UserName @40/42, Password
    // @44/46, Database @68/70.
    let user = utf16le_to_string(slice(40, 42));
    let password = decode_password(slice(44, 46));
    let database = utf16le_to_string(slice(68, 70));
    Login7 {
        user,
        password,
        database,
    }
}

/// Deobfuscate a LOGIN7 password field (MS-TDS): the client encodes each byte as
/// `swap_nibbles(plain) XOR 0xA5`, so decode is `swap_nibbles(byte) XOR 0xA5`, then
/// interpret the result as UTF-16LE.
fn decode_password(enc: &[u8]) -> String {
    let decoded: Vec<u8> = enc.iter().map(|&c| c.rotate_left(4) ^ 0xA5).collect();
    utf16le_to_string(&decoded)
}

// ── SQLBatch ─────────────────────────────────────────────────────────────────────

/// Decode a SQLBatch message body into its SQL text. A TDS 7.2+ batch prefixes the
/// UCS-2 SQL with an `ALL_HEADERS` block whose first DWORD is its total byte length
/// (INCLUDING that field); we skip it when the leading DWORD is a plausible header
/// length (>= 4, within the body, leaving an even-length UTF-16 remainder). A body
/// with no ALL_HEADERS (the leading bytes are UTF-16 text) falls through unmodified.
pub fn decode_sqlbatch(payload: &[u8]) -> String {
    let mut start = 0usize;
    if payload.len() >= 4 {
        let total = u32::from_le_bytes([payload[0], payload[1], payload[2], payload[3]]) as usize;
        if total >= 4 && total <= payload.len() && (payload.len() - total).is_multiple_of(2) {
            start = total;
        }
    }
    utf16le_to_string(&payload[start..])
}

// ── response token encoders ──────────────────────────────────────────────────────

/// Encode a `LOGINACK` token: interface = SQL_TSQL, the reported TDS version, the
/// server program name, and a program version.
pub fn encode_loginack(prog: &str) -> Vec<u8> {
    let mut inner = Vec::new();
    inner.push(1u8); // Interface = SQL_TSQL
    inner.extend_from_slice(&TDS_VERSION_BYTES);
    let name = utf16le_bytes(prog);
    inner.push((name.len() / 2) as u8); // B_VARCHAR: code-unit count
    inner.extend_from_slice(&name);
    inner.extend_from_slice(&[0u8, 1, 0, 0]); // ProgVersion: major.minor.build_hi.build_lo
    let mut out = vec![TOKEN_LOGINACK];
    out.extend_from_slice(&(inner.len() as u16).to_le_bytes());
    out.extend_from_slice(&inner);
    out
}

/// Encode a `COLMETADATA` token for the given `(name, type)` columns.
pub fn encode_colmetadata(cols: &[(String, TdsType)]) -> Vec<u8> {
    let mut out = vec![TOKEN_COLMETADATA];
    out.extend_from_slice(&(cols.len() as u16).to_le_bytes());
    for (name, ty) in cols {
        out.extend_from_slice(&0u32.to_le_bytes()); // UserType (4 bytes, TDS 7.2+)
        out.extend_from_slice(&0x0009u16.to_le_bytes()); // Flags: nullable + updateable
        match ty {
            TdsType::IntN => {
                out.push(TYPE_INTN);
                out.push(8);
            }
            TdsType::FloatN => {
                out.push(TYPE_FLTN);
                out.push(8);
            }
            TdsType::BitN => {
                out.push(TYPE_BITN);
                out.push(1);
            }
            TdsType::NVarchar => {
                out.push(TYPE_NVARCHAR);
                out.extend_from_slice(&(NVARCHAR_MAX_BYTES as u16).to_le_bytes());
                out.extend_from_slice(&[0u8; 5]); // COLLATION (LCID/flags/version/sortid)
            }
        }
        // ColName as a B_VARCHAR (1-byte code-unit count + UTF-16LE).
        let name_utf16 = utf16le_bytes(name);
        out.push((name_utf16.len() / 2) as u8);
        out.extend_from_slice(&name_utf16);
    }
    out
}

/// Encode one `ROW` token for `cells` typed by `types` (parallel slices). Each cell is
/// coerced to its column's type; a value that does not fit the type is emitted NULL
/// (INTN/FLTN/BITN) or JSON-stringified (NVARCHAR).
pub fn encode_row(types: &[TdsType], cells: &[Value]) -> Vec<u8> {
    let mut out = vec![TOKEN_ROW];
    for (ty, cell) in types.iter().zip(cells.iter()) {
        match ty {
            TdsType::IntN => match cell.as_i64() {
                Some(i) => {
                    out.push(8);
                    out.extend_from_slice(&i.to_le_bytes());
                }
                None => out.push(0), // NULL (length 0)
            },
            TdsType::FloatN => match cell.as_f64() {
                Some(f) => {
                    out.push(8);
                    out.extend_from_slice(&f.to_le_bytes());
                }
                None => out.push(0),
            },
            TdsType::BitN => match cell.as_bool() {
                Some(b) => {
                    out.push(1);
                    out.push(b as u8);
                }
                None => out.push(0),
            },
            TdsType::NVarchar => {
                if cell.is_null() {
                    out.extend_from_slice(&0xFFFFu16.to_le_bytes()); // CHARBIN_NULL
                } else {
                    let s = match cell {
                        Value::String(s) => s.clone(),
                        other => other.to_string(),
                    };
                    let mut bytes = utf16le_bytes(&s);
                    if bytes.len() > NVARCHAR_MAX_BYTES {
                        bytes.truncate(NVARCHAR_MAX_BYTES); // even boundary preserved
                    }
                    out.extend_from_slice(&(bytes.len() as u16).to_le_bytes());
                    out.extend_from_slice(&bytes);
                }
            }
        }
    }
    out
}

/// Encode a `DONE` token: status flags, the current-command code (0), and the row
/// count (an 8-byte ULONGLONG, TDS 7.2+).
pub fn encode_done(status: u16, cmd: u16, rowcount: u64) -> Vec<u8> {
    let mut out = vec![TOKEN_DONE];
    out.extend_from_slice(&status.to_le_bytes());
    out.extend_from_slice(&cmd.to_le_bytes());
    out.extend_from_slice(&rowcount.to_le_bytes());
    out
}

/// Encode an `ERROR` token: error number, state, class (severity), the message, and
/// the server name. ProcName is empty and LineNumber is 0 (4-byte LONG, TDS 7.2+).
pub fn encode_error(number: i32, state: u8, class: u8, msg: &str, server: &str) -> Vec<u8> {
    let mut inner = Vec::new();
    inner.extend_from_slice(&number.to_le_bytes());
    inner.push(state);
    inner.push(class);
    // MsgText: US_VARCHAR (2-byte code-unit count + UTF-16LE).
    let msg_utf16 = utf16le_bytes(msg);
    inner.extend_from_slice(&((msg_utf16.len() / 2) as u16).to_le_bytes());
    inner.extend_from_slice(&msg_utf16);
    // ServerName: B_VARCHAR.
    let srv_utf16 = utf16le_bytes(server);
    inner.push((srv_utf16.len() / 2) as u8);
    inner.extend_from_slice(&srv_utf16);
    inner.push(0); // ProcName: empty B_VARCHAR
    inner.extend_from_slice(&0u32.to_le_bytes()); // LineNumber
    let mut out = vec![TOKEN_ERROR];
    out.extend_from_slice(&(inner.len() as u16).to_le_bytes());
    out.extend_from_slice(&inner);
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── framing ────────────────────────────────────────────────────────────────

    #[test]
    fn header_roundtrip_and_fields() {
        let framed = frame_message(PKT_TABULAR, b"hello");
        assert_eq!(framed.len(), HEADER_LEN + 5);
        let hdr = parse_header(framed[..HEADER_LEN].try_into().unwrap());
        assert_eq!(hdr.ty, PKT_TABULAR);
        assert!(hdr.is_eom());
        assert_eq!(hdr.length as usize, HEADER_LEN + 5);
        assert_eq!(hdr.body_len(), 5);
        assert_eq!(&framed[HEADER_LEN..], b"hello");
    }

    #[test]
    fn framing_chunks_large_payload_with_eom_on_last() {
        let payload = vec![0x7Au8; DEFAULT_PACKET_SIZE * 2 + 100];
        let framed = frame_message(PKT_TABULAR, &payload);
        // Walk the packets, reassembling the body and checking the EOM placement.
        let mut i = 0usize;
        let mut body = Vec::new();
        let mut packets = 0;
        let mut saw_eom = false;
        while i < framed.len() {
            let hdr = parse_header(framed[i..i + HEADER_LEN].try_into().unwrap());
            let blen = hdr.body_len();
            body.extend_from_slice(&framed[i + HEADER_LEN..i + HEADER_LEN + blen]);
            packets += 1;
            if hdr.is_eom() {
                saw_eom = true;
                assert_eq!(
                    i + HEADER_LEN + blen,
                    framed.len(),
                    "EOM is the last packet"
                );
            }
            i += HEADER_LEN + blen;
        }
        assert_eq!(packets, 3, "two full packets + a remainder");
        assert!(saw_eom);
        assert_eq!(body, payload, "reassembled body matches the input");
    }

    // ── PRELOGIN ─────────────────────────────────────────────────────────────────

    #[test]
    fn prelogin_response_advertises_not_sup_encryption() {
        let pre = build_prelogin_response();
        // The ENCRYPTION option data byte must be ENCRYPT_NOT_SUP.
        assert_eq!(parse_prelogin_encryption(&pre), Some(ENCRYPT_NOT_SUP));
    }

    #[test]
    fn prelogin_parse_reads_client_encryption() {
        // Build a minimal client PRELOGIN with ENCRYPTION = ENCRYPT_ON (0x01).
        let mut p = Vec::new();
        let enc_off: u16 = 6; // table = ENCRYPTION(5) + terminator(1)
        p.push(PRELOGIN_ENCRYPTION);
        p.extend_from_slice(&enc_off.to_be_bytes());
        p.extend_from_slice(&1u16.to_be_bytes());
        p.push(PRELOGIN_TERMINATOR);
        p.push(0x01); // ENCRYPT_ON
        assert_eq!(parse_prelogin_encryption(&p), Some(0x01));
    }

    // ── LOGIN7 ─────────────────────────────────────────────────────────────────

    /// Build a LOGIN7 record with the given user/password/database in the variable
    /// region, mirroring how a real client lays out the offset/length table.
    fn build_login7(user: &str, password: &str, database: &str) -> Vec<u8> {
        let user_b = utf16le_bytes(user);
        // Obfuscate the password the way a client does: swap_nibbles(b) XOR 0xA5 then
        // reverse for decode. Encode = inverse of decode, and the transform is its own
        // structure — encode each plain UTF-16LE byte.
        let pw_plain = utf16le_bytes(password);
        let pw_enc: Vec<u8> = pw_plain
            .iter()
            .map(|&b| (b ^ 0xA5).rotate_left(4))
            .collect();
        let db_b = utf16le_bytes(database);

        let mut rec = vec![0u8; LOGIN7_FIXED_LEN];
        let mut data = Vec::new();
        let mut put =
            |rec: &mut Vec<u8>, data: &mut Vec<u8>, ib_off: usize, cch_off: usize, bytes: &[u8]| {
                let ib = (LOGIN7_FIXED_LEN + data.len()) as u16;
                let cch = (bytes.len() / 2) as u16;
                rec[ib_off..ib_off + 2].copy_from_slice(&ib.to_le_bytes());
                rec[cch_off..cch_off + 2].copy_from_slice(&cch.to_le_bytes());
                data.extend_from_slice(bytes);
            };
        put(&mut rec, &mut data, 40, 42, &user_b);
        put(&mut rec, &mut data, 44, 46, &pw_enc);
        put(&mut rec, &mut data, 68, 70, &db_b);
        rec.extend_from_slice(&data);
        // Length DWORD (byte 0) — full record length.
        let len = rec.len() as u32;
        rec[0..4].copy_from_slice(&len.to_le_bytes());
        rec
    }

    #[test]
    fn login7_parses_user_password_database() {
        let rec = build_login7("agent:planner", "s3cr3t", "mygraph");
        let parsed = parse_login7(&rec);
        assert_eq!(parsed.user, "agent:planner");
        assert_eq!(parsed.password, "s3cr3t");
        assert_eq!(parsed.database, "mygraph");
    }

    #[test]
    fn login7_short_record_is_empty() {
        assert_eq!(parse_login7(&[0u8; 10]), Login7::default());
    }

    // ── SQLBatch decode ──────────────────────────────────────────────────────────

    #[test]
    fn sqlbatch_decodes_utf16_without_headers() {
        let body = utf16le_bytes("SELECT 1");
        assert_eq!(decode_sqlbatch(&body), "SELECT 1");
    }

    #[test]
    fn sqlbatch_skips_all_headers_block() {
        // A 22-byte ALL_HEADERS transaction-descriptor block, then the SQL text.
        let mut body = Vec::new();
        let header_total: u32 = 22;
        body.extend_from_slice(&header_total.to_le_bytes());
        body.extend_from_slice(&[0u8; 18]); // rest of the 22-byte block
        body.extend_from_slice(&utf16le_bytes("SELECT id FROM nodes"));
        assert_eq!(decode_sqlbatch(&body), "SELECT id FROM nodes");
    }

    // ── token encoders (decode-back smoke) ─────────────────────────────────────────

    /// A minimal token-stream walker for tests: returns (columns, rows, done_status,
    /// done_rowcount) parsed back out of an encoded COLMETADATA + ROW* + DONE stream.
    #[allow(clippy::type_complexity)]
    fn walk_result(stream: &[u8]) -> (Vec<(String, TdsType)>, Vec<Vec<Value>>, u16, u64) {
        let mut i = 0usize;
        let mut cols: Vec<(String, TdsType)> = Vec::new();
        let mut rows: Vec<Vec<Value>> = Vec::new();
        let mut done = (0u16, 0u64);
        while i < stream.len() {
            match stream[i] {
                TOKEN_COLMETADATA => {
                    i += 1;
                    let count = u16::from_le_bytes([stream[i], stream[i + 1]]) as usize;
                    i += 2;
                    for _ in 0..count {
                        i += 4; // UserType
                        i += 2; // Flags
                        let tybyte = stream[i];
                        i += 1;
                        let ty = match tybyte {
                            TYPE_INTN => {
                                i += 1;
                                TdsType::IntN
                            }
                            TYPE_FLTN => {
                                i += 1;
                                TdsType::FloatN
                            }
                            TYPE_BITN => {
                                i += 1;
                                TdsType::BitN
                            }
                            TYPE_NVARCHAR => {
                                i += 2; // max byte len
                                i += 5; // collation
                                TdsType::NVarchar
                            }
                            other => panic!("unexpected TYPE_INFO byte {other:#x}"),
                        };
                        let name_units = stream[i] as usize;
                        i += 1;
                        let name = utf16le_to_string(&stream[i..i + name_units * 2]);
                        i += name_units * 2;
                        cols.push((name, ty));
                    }
                }
                TOKEN_ROW => {
                    i += 1;
                    let mut row = Vec::new();
                    for (_, ty) in &cols {
                        match ty {
                            TdsType::IntN => {
                                let len = stream[i] as usize;
                                i += 1;
                                if len == 0 {
                                    row.push(Value::Null);
                                } else {
                                    let v =
                                        i64::from_le_bytes(stream[i..i + 8].try_into().unwrap());
                                    i += len;
                                    row.push(Value::from(v));
                                }
                            }
                            TdsType::FloatN => {
                                let len = stream[i] as usize;
                                i += 1;
                                if len == 0 {
                                    row.push(Value::Null);
                                } else {
                                    let v =
                                        f64::from_le_bytes(stream[i..i + 8].try_into().unwrap());
                                    i += len;
                                    row.push(Value::from(v));
                                }
                            }
                            TdsType::BitN => {
                                let len = stream[i] as usize;
                                i += 1;
                                if len == 0 {
                                    row.push(Value::Null);
                                } else {
                                    let b = stream[i] != 0;
                                    i += len;
                                    row.push(Value::from(b));
                                }
                            }
                            TdsType::NVarchar => {
                                let len = u16::from_le_bytes([stream[i], stream[i + 1]]) as usize;
                                i += 2;
                                if len == 0xFFFF {
                                    row.push(Value::Null);
                                } else {
                                    let s = utf16le_to_string(&stream[i..i + len]);
                                    i += len;
                                    row.push(Value::String(s));
                                }
                            }
                        }
                    }
                    rows.push(row);
                }
                TOKEN_DONE => {
                    let status = u16::from_le_bytes([stream[i + 1], stream[i + 2]]);
                    let rowcount = u64::from_le_bytes(stream[i + 5..i + 13].try_into().unwrap());
                    done = (status, rowcount);
                    i += 13;
                }
                other => panic!("unexpected token {other:#x} at {i}"),
            }
        }
        (cols, rows, done.0, done.1)
    }

    #[test]
    fn colmetadata_row_done_roundtrip() {
        // A hand-built SQLBatch result: (id NVARCHAR, rank INTN, score FLTN, ok BITN).
        let cols = vec![
            ("id".to_string(), TdsType::NVarchar),
            ("rank".to_string(), TdsType::IntN),
            ("score".to_string(), TdsType::FloatN),
            ("ok".to_string(), TdsType::BitN),
        ];
        let types: Vec<TdsType> = cols.iter().map(|(_, t)| *t).collect();
        let mut stream = encode_colmetadata(&cols);
        let row1 = vec![
            Value::from("n1"),
            Value::from(7i64),
            Value::from(1.5f64),
            Value::from(true),
        ];
        let row2 = vec![
            Value::from("n2"),
            Value::Null,
            Value::Null,
            Value::from(false),
        ];
        stream.extend(encode_row(&types, &row1));
        stream.extend(encode_row(&types, &row2));
        stream.extend(encode_done(DONE_FINAL | DONE_COUNT, 0, 2));

        let (dcols, drows, status, rowcount) = walk_result(&stream);
        assert_eq!(dcols, cols, "column names + types survive the round-trip");
        assert_eq!(drows.len(), 2);
        assert_eq!(drows[0][0], Value::from("n1"));
        assert_eq!(drows[0][1], Value::from(7i64));
        assert_eq!(drows[0][2], Value::from(1.5f64));
        assert_eq!(drows[0][3], Value::from(true));
        assert_eq!(drows[1][1], Value::Null, "NULL INTN decodes back to null");
        assert_eq!(drows[1][3], Value::from(false));
        assert_eq!(status & DONE_COUNT, DONE_COUNT);
        assert_eq!(rowcount, 2);
    }

    #[test]
    fn error_token_layout() {
        let tok = encode_error(50000, 1, 16, "permission denied", "epistemic-graph");
        assert_eq!(tok[0], TOKEN_ERROR);
        let inner_len = u16::from_le_bytes([tok[1], tok[2]]) as usize;
        assert_eq!(tok.len(), 3 + inner_len, "declared length matches the body");
        let number = i32::from_le_bytes(tok[3..7].try_into().unwrap());
        assert_eq!(number, 50000);
        assert_eq!(tok[7], 1, "state");
        assert_eq!(tok[8], 16, "class/severity");
    }

    #[test]
    fn loginack_layout() {
        let tok = encode_loginack("epistemic-graph");
        assert_eq!(tok[0], TOKEN_LOGINACK);
        let inner_len = u16::from_le_bytes([tok[1], tok[2]]) as usize;
        assert_eq!(tok.len(), 3 + inner_len);
        assert_eq!(tok[3], 1, "interface = SQL_TSQL");
        assert_eq!(&tok[4..8], &TDS_VERSION_BYTES, "reported TDS version");
    }
}
