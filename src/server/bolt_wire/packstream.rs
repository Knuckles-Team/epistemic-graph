//! PackStream v2 codec + Bolt chunked framing (CONCEPT:EG-159).
//!
//! PackStream is the binary serialization Neo4j's Bolt protocol carries. This is a
//! HAND-ROLLED, pure-Rust encoder/decoder for the value space Bolt uses on the wire:
//! `Null`, `Bool`, `Int` (all width-optimized sizes), `Float`, `String`, `List`, `Map`,
//! and `Structure` (a tag byte + ordered fields — how every Bolt message and every
//! graph type is framed). It links NO third-party PackStream crate (the Pi-contract
//! idiom the pgwire/mysql-wire adapters follow): every marker byte is emitted/parsed
//! against the documented PackStream v2 spec.
//!
//! Bolt CHUNKED FRAMING wraps a serialized message: the message bytes are split into
//! one-or-more chunks, each prefixed with a 2-byte big-endian length, and the message
//! is terminated by a zero-length (`0x00 0x00`) chunk. [`chunk_message`] frames a
//! single serialized message; [`Dechunker`] reassembles inbound chunks into a message.

use std::collections::HashMap;

// ── PackStream marker bytes (PackStream v2) ──────────────────────────────────────
const M_NULL: u8 = 0xC0;
const M_FLOAT64: u8 = 0xC1;
const M_FALSE: u8 = 0xC2;
const M_TRUE: u8 = 0xC3;
const M_INT8: u8 = 0xC8;
const M_INT16: u8 = 0xC9;
const M_INT32: u8 = 0xCA;
const M_INT64: u8 = 0xCB;
const M_BYTES8: u8 = 0xCC;
const M_BYTES16: u8 = 0xCD;
const M_BYTES32: u8 = 0xCE;
const M_STRING8: u8 = 0xD0;
const M_STRING16: u8 = 0xD1;
const M_STRING32: u8 = 0xD2;
const M_LIST8: u8 = 0xD4;
const M_LIST16: u8 = 0xD5;
const M_LIST32: u8 = 0xD6;
const M_MAP8: u8 = 0xD8;
const M_MAP16: u8 = 0xD9;
const M_MAP32: u8 = 0xDA;
const M_STRUCT8: u8 = 0xDC;
const M_STRUCT16: u8 = 0xDD;

/// A decoded PackStream value (CONCEPT:EG-159). `Map` preserves key insertion order so
/// a round-trip is byte-stable for a fixed key order (Bolt metadata maps are small and
/// order-insensitive to clients, but stability keeps the tests deterministic).
#[derive(Debug, Clone, PartialEq)]
pub enum PackValue {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    String(String),
    Bytes(Vec<u8>),
    List(Vec<PackValue>),
    Map(Vec<(String, PackValue)>),
    /// A structure: a tag byte + up to 15 ordered fields (every Bolt message + graph type).
    Structure {
        tag: u8,
        fields: Vec<PackValue>,
    },
}

impl PackValue {
    /// Build a `Map` value from `(key, value)` pairs.
    pub fn map(pairs: Vec<(&str, PackValue)>) -> Self {
        PackValue::Map(pairs.into_iter().map(|(k, v)| (k.to_string(), v)).collect())
    }

    /// Look up a key in a `Map` value (else `None`).
    pub fn get(&self, key: &str) -> Option<&PackValue> {
        match self {
            PackValue::Map(pairs) => pairs.iter().find(|(k, _)| k == key).map(|(_, v)| v),
            _ => None,
        }
    }

    /// The `i64` if this is an `Int`.
    pub fn as_int(&self) -> Option<i64> {
        match self {
            PackValue::Int(i) => Some(*i),
            _ => None,
        }
    }

    /// The `&str` if this is a `String`.
    pub fn as_str(&self) -> Option<&str> {
        match self {
            PackValue::String(s) => Some(s),
            _ => None,
        }
    }

    /// Convert a `Map` value into a `HashMap<String, PackValue>` (Bolt `extra`/params).
    pub fn into_map(self) -> HashMap<String, PackValue> {
        match self {
            PackValue::Map(pairs) => pairs.into_iter().collect(),
            _ => HashMap::new(),
        }
    }
}

// ── encoding ─────────────────────────────────────────────────────────────────────

/// Append the PackStream v2 encoding of `v` to `out` (CONCEPT:EG-159). Integers pick
/// the smallest width; strings/lists/maps pick the tiny/8/16/32 marker by length.
pub fn encode(v: &PackValue, out: &mut Vec<u8>) {
    match v {
        PackValue::Null => out.push(M_NULL),
        PackValue::Bool(false) => out.push(M_FALSE),
        PackValue::Bool(true) => out.push(M_TRUE),
        PackValue::Int(i) => encode_int(*i, out),
        PackValue::Float(f) => {
            out.push(M_FLOAT64);
            out.extend_from_slice(&f.to_be_bytes());
        }
        PackValue::String(s) => {
            let b = s.as_bytes();
            encode_len_header(b.len(), 0x80, M_STRING8, M_STRING16, M_STRING32, out);
            out.extend_from_slice(b);
        }
        PackValue::Bytes(b) => {
            // Bytes has NO tiny form — always an 8/16/32 marker.
            let n = b.len();
            if n <= u8::MAX as usize {
                out.push(M_BYTES8);
                out.push(n as u8);
            } else if n <= u16::MAX as usize {
                out.push(M_BYTES16);
                out.extend_from_slice(&(n as u16).to_be_bytes());
            } else {
                out.push(M_BYTES32);
                out.extend_from_slice(&(n as u32).to_be_bytes());
            }
            out.extend_from_slice(b);
        }
        PackValue::List(items) => {
            encode_len_header(items.len(), 0x90, M_LIST8, M_LIST16, M_LIST32, out);
            for it in items {
                encode(it, out);
            }
        }
        PackValue::Map(pairs) => {
            encode_len_header(pairs.len(), 0xA0, M_MAP8, M_MAP16, M_MAP32, out);
            for (k, val) in pairs {
                encode(&PackValue::String(k.clone()), out);
                encode(val, out);
            }
        }
        PackValue::Structure { tag, fields } => {
            let n = fields.len();
            if n <= 0x0F {
                out.push(0xB0 | (n as u8));
            } else if n <= u8::MAX as usize {
                out.push(M_STRUCT8);
                out.push(n as u8);
            } else {
                out.push(M_STRUCT16);
                out.extend_from_slice(&(n as u16).to_be_bytes());
            }
            out.push(*tag);
            for f in fields {
                encode(f, out);
            }
        }
    }
}

/// Encode an integer with the smallest PackStream width (TINY_INT `-16..=127`, then
/// INT_8/16/32/64).
fn encode_int(i: i64, out: &mut Vec<u8>) {
    if (-16..=127).contains(&i) {
        out.push((i as i8) as u8);
    } else if (i8::MIN as i64..=i8::MAX as i64).contains(&i) {
        out.push(M_INT8);
        out.push((i as i8) as u8);
    } else if (i16::MIN as i64..=i16::MAX as i64).contains(&i) {
        out.push(M_INT16);
        out.extend_from_slice(&(i as i16).to_be_bytes());
    } else if (i32::MIN as i64..=i32::MAX as i64).contains(&i) {
        out.push(M_INT32);
        out.extend_from_slice(&(i as i32).to_be_bytes());
    } else {
        out.push(M_INT64);
        out.extend_from_slice(&i.to_be_bytes());
    }
}

/// Emit the length header for a String/List/Map: a tiny marker (`tiny_base | len`) when
/// `len <= 15`, else the 8/16/32-bit marker + big-endian length.
fn encode_len_header(len: usize, tiny_base: u8, m8: u8, m16: u8, m32: u8, out: &mut Vec<u8>) {
    if len <= 0x0F {
        out.push(tiny_base | (len as u8));
    } else if len <= u8::MAX as usize {
        out.push(m8);
        out.push(len as u8);
    } else if len <= u16::MAX as usize {
        out.push(m16);
        out.extend_from_slice(&(len as u16).to_be_bytes());
    } else {
        out.push(m32);
        out.extend_from_slice(&(len as u32).to_be_bytes());
    }
}

/// Encode `v` into a fresh byte vector (CONCEPT:EG-159).
pub fn encode_to_vec(v: &PackValue) -> Vec<u8> {
    let mut out = Vec::new();
    encode(v, &mut out);
    out
}

// ── decoding ─────────────────────────────────────────────────────────────────────

/// A PackStream decode error (a truncated buffer or an unknown marker).
#[derive(Debug)]
pub struct DecodeError(pub String);

impl std::fmt::Display for DecodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "PackStream decode error: {}", self.0)
    }
}
impl std::error::Error for DecodeError {}

/// A cursor over a PackStream byte buffer.
struct Cursor<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    fn u8(&mut self) -> Result<u8, DecodeError> {
        let b = *self
            .buf
            .get(self.pos)
            .ok_or_else(|| DecodeError("unexpected end of buffer".into()))?;
        self.pos += 1;
        Ok(b)
    }
    fn take(&mut self, n: usize) -> Result<&'a [u8], DecodeError> {
        if self.pos + n > self.buf.len() {
            return Err(DecodeError(format!("need {n} bytes, buffer exhausted")));
        }
        let s = &self.buf[self.pos..self.pos + n];
        self.pos += n;
        Ok(s)
    }
    fn be_u16(&mut self) -> Result<u16, DecodeError> {
        let s = self.take(2)?;
        Ok(u16::from_be_bytes([s[0], s[1]]))
    }
    fn be_u32(&mut self) -> Result<u32, DecodeError> {
        let s = self.take(4)?;
        Ok(u32::from_be_bytes([s[0], s[1], s[2], s[3]]))
    }
}

/// Decode ONE PackStream value from `buf`, returning it (CONCEPT:EG-159). Trailing bytes
/// are ignored (a message may pack several top-level values, though Bolt messages are
/// always a single top-level structure).
pub fn decode(buf: &[u8]) -> Result<PackValue, DecodeError> {
    let mut c = Cursor { buf, pos: 0 };
    decode_value(&mut c)
}

fn decode_value(c: &mut Cursor) -> Result<PackValue, DecodeError> {
    let marker = c.u8()?;
    match marker {
        M_NULL => Ok(PackValue::Null),
        M_TRUE => Ok(PackValue::Bool(true)),
        M_FALSE => Ok(PackValue::Bool(false)),
        M_FLOAT64 => {
            let s = c.take(8)?;
            let mut b = [0u8; 8];
            b.copy_from_slice(s);
            Ok(PackValue::Float(f64::from_be_bytes(b)))
        }
        M_INT8 => Ok(PackValue::Int(c.u8()? as i8 as i64)),
        M_INT16 => Ok(PackValue::Int(c.be_u16()? as i16 as i64)),
        M_INT32 => Ok(PackValue::Int(c.be_u32()? as i32 as i64)),
        M_INT64 => {
            let s = c.take(8)?;
            let mut b = [0u8; 8];
            b.copy_from_slice(s);
            Ok(PackValue::Int(i64::from_be_bytes(b)))
        }
        M_BYTES8 => {
            let n = c.u8()? as usize;
            Ok(PackValue::Bytes(c.take(n)?.to_vec()))
        }
        M_BYTES16 => {
            let n = c.be_u16()? as usize;
            Ok(PackValue::Bytes(c.take(n)?.to_vec()))
        }
        M_BYTES32 => {
            let n = c.be_u32()? as usize;
            Ok(PackValue::Bytes(c.take(n)?.to_vec()))
        }
        M_STRING8 => {
            let n = c.u8()? as usize;
            decode_string(c, n)
        }
        M_STRING16 => {
            let n = c.be_u16()? as usize;
            decode_string(c, n)
        }
        M_STRING32 => {
            let n = c.be_u32()? as usize;
            decode_string(c, n)
        }
        M_LIST8 => {
            let n = c.u8()? as usize;
            decode_list(c, n)
        }
        M_LIST16 => {
            let n = c.be_u16()? as usize;
            decode_list(c, n)
        }
        M_LIST32 => {
            let n = c.be_u32()? as usize;
            decode_list(c, n)
        }
        M_MAP8 => {
            let n = c.u8()? as usize;
            decode_map(c, n)
        }
        M_MAP16 => {
            let n = c.be_u16()? as usize;
            decode_map(c, n)
        }
        M_MAP32 => {
            let n = c.be_u32()? as usize;
            decode_map(c, n)
        }
        M_STRUCT8 => {
            let n = c.u8()? as usize;
            decode_struct(c, n)
        }
        M_STRUCT16 => {
            let n = c.be_u16()? as usize;
            decode_struct(c, n)
        }
        // Tiny / packed-into-marker forms.
        _ => {
            let hi = marker & 0xF0;
            let lo = (marker & 0x0F) as usize;
            match hi {
                0x80 => decode_string(c, lo),
                0x90 => decode_list(c, lo),
                0xA0 => decode_map(c, lo),
                0xB0 => decode_struct(c, lo),
                _ => {
                    // TINY_INT: markers 0x00..=0x7F (0..127) and 0xF0..=0xFF (-16..-1).
                    if marker <= 0x7F || marker >= 0xF0 {
                        Ok(PackValue::Int(marker as i8 as i64))
                    } else {
                        Err(DecodeError(format!(
                            "unknown PackStream marker {marker:#04x}"
                        )))
                    }
                }
            }
        }
    }
}

fn decode_string(c: &mut Cursor, n: usize) -> Result<PackValue, DecodeError> {
    let s = c.take(n)?;
    let s = std::str::from_utf8(s).map_err(|e| DecodeError(format!("invalid utf8: {e}")))?;
    Ok(PackValue::String(s.to_string()))
}

fn decode_list(c: &mut Cursor, n: usize) -> Result<PackValue, DecodeError> {
    let mut items = Vec::with_capacity(n);
    for _ in 0..n {
        items.push(decode_value(c)?);
    }
    Ok(PackValue::List(items))
}

fn decode_map(c: &mut Cursor, n: usize) -> Result<PackValue, DecodeError> {
    let mut pairs = Vec::with_capacity(n);
    for _ in 0..n {
        let key = match decode_value(c)? {
            PackValue::String(s) => s,
            other => return Err(DecodeError(format!("map key not a string: {other:?}"))),
        };
        let val = decode_value(c)?;
        pairs.push((key, val));
    }
    Ok(PackValue::Map(pairs))
}

fn decode_struct(c: &mut Cursor, n: usize) -> Result<PackValue, DecodeError> {
    let tag = c.u8()?;
    let mut fields = Vec::with_capacity(n);
    for _ in 0..n {
        fields.push(decode_value(c)?);
    }
    Ok(PackValue::Structure { tag, fields })
}

// ── Bolt chunked framing ───────────────────────────────────────────────────────

/// The max bytes a single Bolt chunk body may carry (2-byte length prefix).
const MAX_CHUNK: usize = u16::MAX as usize;

/// Frame one serialized message body into Bolt chunks + the terminating `0x00 0x00`
/// end-of-message marker (CONCEPT:EG-159). A body larger than 65535 bytes is split
/// across several length-prefixed chunks.
pub fn chunk_message(body: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(body.len() + 4);
    for chunk in body.chunks(MAX_CHUNK).filter(|c| !c.is_empty()) {
        out.extend_from_slice(&(chunk.len() as u16).to_be_bytes());
        out.extend_from_slice(chunk);
    }
    // Zero-length terminating chunk marks end-of-message.
    out.extend_from_slice(&[0x00, 0x00]);
    out
}

/// Encode a [`PackValue`] message and frame it into chunks in one step.
pub fn encode_chunked(v: &PackValue) -> Vec<u8> {
    chunk_message(&encode_to_vec(v))
}

/// Reassembles inbound Bolt chunks into complete message bodies (CONCEPT:EG-159). Feed
/// raw chunk bytes; when a zero-length chunk closes a message, the accumulated body is
/// returned. Used by tests + any buffered-byte transport; the live listener reads
/// chunks directly off the socket.
#[derive(Default)]
pub struct Dechunker {
    body: Vec<u8>,
}

impl Dechunker {
    /// Split `input` (a full chunk stream for one-or-more messages) into message bodies.
    /// Errors on a truncated chunk header/body.
    pub fn split_messages(input: &[u8]) -> Result<Vec<Vec<u8>>, DecodeError> {
        let mut c = Cursor { buf: input, pos: 0 };
        let mut msgs = Vec::new();
        let mut cur = Vec::new();
        loop {
            if c.pos >= input.len() {
                break;
            }
            let len = c.be_u16()? as usize;
            if len == 0 {
                msgs.push(std::mem::take(&mut cur));
            } else {
                cur.extend_from_slice(c.take(len)?);
            }
        }
        Ok(msgs)
    }
}

#[cfg(test)]
mod tests {
    //! PackStream v2 round-trip + chunk-framing unit tests (CONCEPT:EG-159).
    use super::*;

    fn roundtrip(v: PackValue) {
        let bytes = encode_to_vec(&v);
        let back = decode(&bytes).expect("decode");
        assert_eq!(v, back, "round-trip mismatch for {v:?}");
    }

    #[test]
    fn bolt_packstream_roundtrips_null_bool() {
        roundtrip(PackValue::Null);
        roundtrip(PackValue::Bool(true));
        roundtrip(PackValue::Bool(false));
    }

    #[test]
    fn bolt_packstream_roundtrips_int_all_widths() {
        // TINY range, INT8, INT16, INT32, INT64 boundaries.
        for i in [
            0i64,
            1,
            -1,
            -16,
            127,
            128,
            -17,
            -128,
            -129,
            200,
            32767,
            32768,
            -32769,
            2_000_000,
            2_147_483_647,
            2_147_483_648,
            -2_147_483_649,
            i64::MAX,
            i64::MIN,
        ] {
            roundtrip(PackValue::Int(i));
        }
    }

    #[test]
    fn bolt_packstream_tiny_int_marker_bytes() {
        // 1 encodes as a single byte 0x01; -1 as 0xFF.
        assert_eq!(encode_to_vec(&PackValue::Int(1)), vec![0x01]);
        assert_eq!(encode_to_vec(&PackValue::Int(-1)), vec![0xFF]);
        assert_eq!(encode_to_vec(&PackValue::Int(-16)), vec![0xF0]);
    }

    #[test]
    fn bolt_packstream_roundtrips_float() {
        roundtrip(PackValue::Float(0.0));
        roundtrip(PackValue::Float(3.141592653589793));
        roundtrip(PackValue::Float(-2.5e10));
    }

    #[test]
    fn bolt_packstream_roundtrips_string() {
        roundtrip(PackValue::String(String::new()));
        roundtrip(PackValue::String("hello".into()));
        // Force STRING8 (>15 bytes) and multi-byte UTF-8.
        roundtrip(PackValue::String("a".repeat(40)));
        roundtrip(PackValue::String("héllo — wörld ✅".into()));
        // Force STRING16 (>255 bytes).
        roundtrip(PackValue::String("x".repeat(300)));
    }

    #[test]
    fn bolt_packstream_roundtrips_list() {
        roundtrip(PackValue::List(vec![]));
        roundtrip(PackValue::List(vec![
            PackValue::Int(1),
            PackValue::String("two".into()),
            PackValue::Bool(true),
            PackValue::Null,
        ]));
        // Force LIST8 (>15 elements).
        roundtrip(PackValue::List((0..20).map(PackValue::Int).collect()));
    }

    #[test]
    fn bolt_packstream_roundtrips_map() {
        roundtrip(PackValue::map(vec![]));
        roundtrip(PackValue::map(vec![
            ("id", PackValue::Int(42)),
            ("name", PackValue::String("neo".into())),
            ("active", PackValue::Bool(true)),
        ]));
    }

    #[test]
    fn bolt_packstream_roundtrips_bytes() {
        roundtrip(PackValue::Bytes(vec![]));
        roundtrip(PackValue::Bytes(vec![1, 2, 3, 254, 255]));
        roundtrip(PackValue::Bytes(vec![7u8; 500]));
    }

    #[test]
    fn bolt_packstream_roundtrips_structure() {
        // A RUN message shape: tag 0x10, fields [query, params, extra].
        let run = PackValue::Structure {
            tag: 0x10,
            fields: vec![
                PackValue::String("RETURN 1".into()),
                PackValue::map(vec![("x", PackValue::Int(1))]),
                PackValue::map(vec![]),
            ],
        };
        roundtrip(run);
    }

    #[test]
    fn bolt_chunk_framing_wraps_and_terminates() {
        let body = encode_to_vec(&PackValue::String("hi".into()));
        let framed = chunk_message(&body);
        // [len_hi, len_lo, ...body..., 0x00, 0x00]
        assert_eq!(&framed[0..2], &(body.len() as u16).to_be_bytes());
        assert_eq!(&framed[framed.len() - 2..], &[0x00, 0x00]);
        // Dechunk it back to exactly the one body.
        let msgs = Dechunker::split_messages(&framed).expect("dechunk");
        assert_eq!(msgs.len(), 1);
        assert_eq!(msgs[0], body);
    }

    #[test]
    fn bolt_chunk_framing_splits_large_body() {
        // A >64KiB body must split across multiple chunks and still reassemble.
        let big = PackValue::String("z".repeat(70_000));
        let body = encode_to_vec(&big);
        let framed = chunk_message(&body);
        let msgs = Dechunker::split_messages(&framed).expect("dechunk");
        assert_eq!(msgs.len(), 1);
        assert_eq!(decode(&msgs[0]).unwrap(), big);
    }

    #[test]
    fn bolt_chunk_framing_multiple_messages() {
        let a = encode_to_vec(&PackValue::Int(1));
        let b = encode_to_vec(&PackValue::Int(2));
        let mut stream = chunk_message(&a);
        stream.extend_from_slice(&chunk_message(&b));
        let msgs = Dechunker::split_messages(&stream).expect("dechunk");
        assert_eq!(msgs.len(), 2);
        assert_eq!(decode(&msgs[0]).unwrap(), PackValue::Int(1));
        assert_eq!(decode(&msgs[1]).unwrap(), PackValue::Int(2));
    }
}
