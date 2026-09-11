//! AMQP 0.9.1 wire codec and content framing.
//!
//! This child owns byte-level protocol representation, cursor decoding, frame
//! boundaries, and content properties. Connection authentication and broker
//! session policy remain in the parent module.

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use crate::server::broker_wire::{self, invalid_data};

// ── Frame types + class/method ids (AMQP 0.9.1) ──────────────────────────
pub(super) const FRAME_METHOD: u8 = 1;
pub(super) const FRAME_HEADER: u8 = 2;
pub(super) const FRAME_BODY: u8 = 3;
pub(super) const FRAME_HEARTBEAT: u8 = 8;
pub(super) const FRAME_END: u8 = 0xCE;

pub(super) const C_CONNECTION: u16 = 10;
pub(super) const C_CHANNEL: u16 = 20;
pub(super) const C_EXCHANGE: u16 = 40;
pub(super) const C_QUEUE: u16 = 50;
pub(super) const C_BASIC: u16 = 60;
/// AMQP `confirm` class (CONCEPT:EG-KG.ingest.broker-reject-publish publisher confirms).
pub(super) const C_CONFIRM: u16 = 85;

/// Hard per-frame allocation ceiling for untrusted AMQP size prefixes.
pub(super) const MAX_AMQP_FRAME_BYTES: usize = 64 * 1024 * 1024;
/// A content body is assembled from multiple frames, so it needs an independent
/// aggregate cap rather than relying on the per-frame ceiling.
pub(super) const MAX_AMQP_CONTENT_BYTES: usize = 64 * 1024 * 1024;
pub(super) const MAX_AMQP_HEADER_FIELDS: usize = 4_096;

/// One raw AMQP frame off the wire.
pub(super) struct Frame {
    pub(super) kind: u8,
    pub(super) channel: u16,
    pub(super) payload: Vec<u8>,
}

/// A raised AMQP method: class/method ids plus argument bytes.
pub(super) struct MethodCall<'a> {
    pub(super) class: u16,
    pub(super) method: u16,
    pub(super) args: &'a [u8],
}

pub(super) fn parse_shortstr_args<const N: usize>(
    args: &[u8],
    error: &'static str,
) -> std::io::Result<[String; N]> {
    let mut cursor = Cursor::new(args);
    cursor.u16(); // reserved-1
    let values = std::array::from_fn(|_| cursor.shortstr());
    cursor
        .valid
        .then_some(values)
        .ok_or_else(|| invalid_data(error))
}
pub(super) async fn read_protocol_header(socket: &mut TcpStream) -> std::io::Result<bool> {
    let mut hdr = [0u8; 8];
    socket.read_exact(&mut hdr).await?;
    if &hdr != b"AMQP\x00\x00\x09\x01" {
        // Tell the client the version we speak, then close.
        socket.write_all(b"AMQP\x00\x00\x09\x01").await?;
        return Ok(false);
    }
    Ok(true)
}
/// Idempotency / priority fields lifted from a `basic.publish` content header
/// (CONCEPT:EG-KG.ingest.broker-reject-publish). All optional — an absent header leaves the default (no producer
/// stamp, priority 0), so a publish with no properties behaves exactly as before.
#[derive(Default)]
pub(super) struct ContentProps {
    /// `x-producer-id` application header (the idempotent-publish producer identity).
    pub(super) producer_id: Option<String>,
    /// `x-producer-seq` application header (the per-producer monotonic sequence).
    pub(super) producer_seq: Option<i64>,
    /// AMQP basic `priority` property → EG-278 priority band.
    pub(super) priority: i64,
}

/// Read the content-header frame (body size + basic-properties) then accumulate body
/// frames. Parses the idempotency application-headers + priority (CONCEPT:EG-KG.ingest.broker-reject-publish).
pub(super) async fn read_content(
    socket: &mut TcpStream,
    expected_channel: u16,
) -> std::io::Result<(ContentProps, Vec<u8>)> {
    let header = read_content_header(socket, expected_channel).await?;
    let body_size = content_body_size(&header)?;
    let props = parse_content_props(&header.payload)
        .ok_or_else(|| invalid_data("invalid AMQP content properties"))?;
    let body = read_content_body(socket, expected_channel, body_size).await?;
    Ok((props, body))
}

pub(super) async fn read_content_header(
    socket: &mut TcpStream,
    expected_channel: u16,
) -> std::io::Result<Frame> {
    let header = match read_frame(socket).await? {
        Some(f) if f.kind == FRAME_HEADER && f.channel == expected_channel => f,
        _ => return Err(invalid_data("invalid AMQP content header")),
    };
    // header payload: class(2) weight(2) body-size(8) property-flags(2) properties…
    if header.payload.len() < 12 {
        return Err(invalid_data("invalid AMQP content header"));
    }
    if u16::from_be_bytes([header.payload[0], header.payload[1]]) != C_BASIC
        || header.payload[2] != 0
        || header.payload[3] != 0
    {
        return Err(invalid_data("invalid AMQP content header"));
    }
    Ok(header)
}

pub(super) fn content_body_size(header: &Frame) -> std::io::Result<usize> {
    let declared_body_size = u64::from_be_bytes([
        header.payload[4],
        header.payload[5],
        header.payload[6],
        header.payload[7],
        header.payload[8],
        header.payload[9],
        header.payload[10],
        header.payload[11],
    ]);
    usize::try_from(declared_body_size)
        .ok()
        .filter(|size| *size <= MAX_AMQP_CONTENT_BYTES)
        .ok_or_else(|| invalid_data("AMQP content body exceeds the resource limit"))
}

pub(super) async fn read_content_body(
    socket: &mut TcpStream,
    expected_channel: u16,
    body_size: usize,
) -> std::io::Result<Vec<u8>> {
    // Do not reserve the entire attacker-declared size before any body bytes
    // arrive. Capacity grows only with frames that have actually been read.
    let mut body = Vec::new();
    while body.len() < body_size {
        match read_frame(socket).await? {
            Some(f) if f.kind == FRAME_BODY && f.channel == expected_channel => {
                if f.payload.is_empty() {
                    return Err(invalid_data("empty AMQP content body frame"));
                }
                if f.payload.len() > body_size - body.len() {
                    return Err(invalid_data("AMQP content body exceeds its declared size"));
                }
                if body.is_empty() {
                    body = f.payload;
                } else {
                    body.extend_from_slice(&f.payload);
                }
            }
            _ => return Err(invalid_data("incomplete AMQP content body")),
        }
    }
    Ok(body)
}

/// Parse a `basic.publish` content-header payload for the idempotency headers +
/// priority (CONCEPT:EG-KG.ingest.broker-reject-publish). Walks the AMQP basic-properties in flag order to reach
/// the application-`headers` table (bit `0x2000`) and the `priority` octet (`0x0800`).
/// A multi-word property-flags preamble (continuation bit `0x0001`, vanishingly rare
/// for a publish) is not decoded — extraction is skipped and the publish still lands.
pub(super) fn parse_content_props(payload: &[u8]) -> Option<ContentProps> {
    let mut props = ContentProps::default();
    if payload.len() < 14 {
        return None;
    }
    let flags = u16::from_be_bytes([payload[12], payload[13]]);
    if flags & 0x0001 != 0 {
        return None; // unsupported multi-word flags must not be partially accepted
    }
    let mut c = Cursor::new(&payload[14..]);
    parse_content_properties_prefix(&mut c, flags);
    if flags & 0x2000 != 0 {
        let table = c.longstr_slice();
        if !parse_headers_table(table, &mut props) {
            return None;
        }
    }
    parse_content_properties_suffix(&mut c, flags, &mut props);
    (c.valid && c.remaining() == 0).then_some(props)
}

pub(super) fn parse_content_properties_prefix(c: &mut Cursor<'_>, flags: u16) {
    if flags & 0x8000 != 0 {
        let _content_type = c.shortstr();
    }
    if flags & 0x4000 != 0 {
        let _content_encoding = c.shortstr();
    }
}

pub(super) fn parse_content_properties_suffix(
    c: &mut Cursor<'_>,
    flags: u16,
    props: &mut ContentProps,
) {
    parse_content_properties_delivery(c, flags, props);
    parse_content_properties_metadata(c, flags);
}

pub(super) fn parse_content_properties_delivery(
    c: &mut Cursor<'_>,
    flags: u16,
    props: &mut ContentProps,
) {
    if flags & 0x1000 != 0 {
        let _delivery_mode = c.u8();
    }
    if flags & 0x0800 != 0 {
        props.priority = c.u8() as i64;
    }
    if flags & 0x0400 != 0 {
        let _correlation_id = c.shortstr();
    }
}

pub(super) fn parse_content_properties_metadata(c: &mut Cursor<'_>, flags: u16) {
    if flags & 0x0200 != 0 {
        let _reply_to = c.shortstr();
    }
    if flags & 0x0100 != 0 {
        let _expiration = c.shortstr();
    }
    if flags & 0x0080 != 0 {
        let _message_id = c.shortstr();
    }
    if flags & 0x0040 != 0 {
        let _timestamp = c.u64();
    }
    if flags & 0x0020 != 0 {
        let _message_type = c.shortstr();
    }
    if flags & 0x0010 != 0 {
        let _user_id = c.shortstr();
    }
    if flags & 0x0008 != 0 {
        let _app_id = c.shortstr();
    }
    if flags & 0x0004 != 0 {
        let _cluster_id = c.shortstr();
    }
}

/// Scan an AMQP field-table for the idempotency headers (CONCEPT:EG-KG.ingest.broker-reject-publish): `x-producer-id`
/// (a string value) and `x-producer-seq` (an int, or a numeric string). Unknown value
/// types whose width can't be determined end the scan (the already-found keys stand).
pub(super) fn parse_headers_table(bytes: &[u8], props: &mut ContentProps) -> bool {
    let mut c = Cursor::new(bytes);
    let mut fields = 0usize;
    while c.remaining() > 0 {
        fields += 1;
        if fields > MAX_AMQP_HEADER_FIELDS {
            return false;
        }
        let name = c.shortstr();
        let Some(val) = c.field_value() else {
            return false;
        };
        match name.as_str() {
            "x-producer-id" => {
                if let FieldVal::Str(s) = val {
                    props.producer_id = Some(s);
                }
            }
            "x-producer-seq" => match val {
                FieldVal::Int(n) => props.producer_seq = Some(n),
                FieldVal::Str(s) => props.producer_seq = s.parse().ok(),
                FieldVal::Skip => {}
            },
            _ => {}
        }
    }
    c.valid
}

/// A decoded AMQP field-table value we care about (CONCEPT:EG-KG.ingest.broker-reject-publish) — a string, an
/// integer, or a correctly-sized value we skip over.
pub(super) enum FieldVal {
    Str(String),
    Int(i64),
    Skip,
}
/// Build a server `basic.ack` (class 60 / method 80): delivery-tag + `multiple` bit
/// (CONCEPT:EG-KG.ingest.broker-reject-publish publisher confirms).
pub(super) fn build_basic_ack(delivery_tag: u64, multiple: bool) -> Vec<u8> {
    let mut p = method_header(C_BASIC, 80);
    put_u64(&mut p, delivery_tag);
    p.push(u8::from(multiple));
    p
}

/// Build a server `basic.nack` (class 60 / method 120): delivery-tag + `multiple`/
/// `requeue` bits (CONCEPT:EG-KG.ingest.broker-reject-publish — a publish the broker could not accept).
pub(super) fn build_basic_nack(delivery_tag: u64, multiple: bool, requeue: bool) -> Vec<u8> {
    let mut p = method_header(C_BASIC, 120);
    put_u64(&mut p, delivery_tag);
    let mut bits = 0u8;
    if multiple {
        bits |= 0x01;
    }
    if requeue {
        bits |= 0x02;
    }
    p.push(bits);
    p
}

/// Emit a content header + single body frame for `body` on `channel`.
pub(super) async fn write_content(
    socket: &mut TcpStream,
    channel: u16,
    body: &[u8],
) -> std::io::Result<()> {
    let mut hp = Vec::new();
    put_u16(&mut hp, C_BASIC); // class-id
    put_u16(&mut hp, 0); // weight
    put_u64(&mut hp, body.len() as u64); // body-size
    put_u16(&mut hp, 0); // property-flags (no properties)
    write_frame(socket, FRAME_HEADER, channel, &hp).await?;
    if !body.is_empty() {
        write_frame(socket, FRAME_BODY, channel, body).await?;
    }
    Ok(())
}

// ── Frame codec ───────────────────────────────────────────────────────────

pub(super) async fn read_frame(socket: &mut TcpStream) -> std::io::Result<Option<Frame>> {
    let mut head = [0u8; 7];
    // A clean EOF at a frame boundary ⇒ None (connection closed).
    if let Err(e) = socket.read_exact(&mut head).await {
        if e.kind() == std::io::ErrorKind::UnexpectedEof {
            return Ok(None);
        }
        return Err(e);
    }
    let kind = head[0];
    let channel = u16::from_be_bytes([head[1], head[2]]);
    let size = u32::from_be_bytes([head[3], head[4], head[5], head[6]]) as usize;
    if !matches!(
        kind,
        FRAME_METHOD | FRAME_HEADER | FRAME_BODY | FRAME_HEARTBEAT
    ) {
        return Err(invalid_data("invalid AMQP frame type"));
    }
    if kind == FRAME_HEARTBEAT && (channel != 0 || size != 0) {
        return Err(invalid_data("invalid AMQP heartbeat frame"));
    }
    if size > MAX_AMQP_FRAME_BYTES {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "AMQP frame exceeds the resource limit",
        ));
    }
    let mut payload = vec![0u8; size];
    socket.read_exact(&mut payload).await?;
    let mut end = [0u8; 1];
    socket.read_exact(&mut end).await?;
    if end[0] != FRAME_END {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "bad AMQP frame-end octet",
        ));
    }
    Ok(Some(Frame {
        kind,
        channel,
        payload,
    }))
}

pub(super) async fn write_frame(
    socket: &mut TcpStream,
    kind: u8,
    channel: u16,
    payload: &[u8],
) -> std::io::Result<()> {
    if payload.len() > MAX_AMQP_FRAME_BYTES || u32::try_from(payload.len()).is_err() {
        return Err(invalid_data("AMQP output frame exceeds the resource limit"));
    }
    let capacity = payload
        .len()
        .checked_add(8)
        .ok_or_else(|| invalid_data("AMQP output frame length overflow"))?;
    let mut buf = Vec::with_capacity(capacity);
    buf.push(kind);
    put_u16(&mut buf, channel);
    put_u32(&mut buf, payload.len() as u32);
    buf.extend_from_slice(payload);
    buf.push(FRAME_END);
    socket.write_all(&buf).await
}

pub(super) fn parse_method(payload: &[u8]) -> Option<MethodCall<'_>> {
    if payload.len() < 4 {
        return None;
    }
    let class = u16::from_be_bytes([payload[0], payload[1]]);
    let method = u16::from_be_bytes([payload[2], payload[3]]);
    Some(MethodCall {
        class,
        method,
        args: &payload[4..],
    })
}

/// Method-frame payload header: class-id + method-id.
pub(super) fn method_header(class: u16, method: u16) -> Vec<u8> {
    let mut v = Vec::with_capacity(4);
    put_u16(&mut v, class);
    put_u16(&mut v, method);
    v
}
pub(super) fn build_connection_start() -> Vec<u8> {
    let mut p = method_header(C_CONNECTION, 10);
    p.push(0); // version-major
    p.push(9); // version-minor
    put_u32(&mut p, 0); // server-properties: empty field-table
    put_longstr(&mut p, b"PLAIN"); // mechanisms
    put_longstr(&mut p, b"en_US"); // locales
    p
}

pub(super) fn build_connection_tune() -> Vec<u8> {
    let mut p = method_header(C_CONNECTION, 30);
    put_u16(&mut p, 0); // channel-max (0 = no limit)
    put_u32(&mut p, 131_072); // frame-max
    put_u16(&mut p, 0); // heartbeat (0 = off)
    p
}

pub(super) fn build_connection_open_ok() -> Vec<u8> {
    let mut p = method_header(C_CONNECTION, 41);
    put_shortstr(&mut p, b""); // reserved-1
    p
}

// ── Primitive encoders ──────────────────────────────────────────────────

pub(super) fn put_u16(v: &mut Vec<u8>, x: u16) {
    v.extend_from_slice(&x.to_be_bytes());
}
pub(super) fn put_u32(v: &mut Vec<u8>, x: u32) {
    v.extend_from_slice(&x.to_be_bytes());
}
pub(super) fn put_u64(v: &mut Vec<u8>, x: u64) {
    v.extend_from_slice(&x.to_be_bytes());
}
pub(super) fn put_shortstr(v: &mut Vec<u8>, s: &[u8]) {
    v.push(s.len().min(255) as u8);
    v.extend_from_slice(&s[..s.len().min(255)]);
}
pub(super) fn put_longstr(v: &mut Vec<u8>, s: &[u8]) {
    put_u32(v, s.len() as u32);
    v.extend_from_slice(s);
}

/// A minimal read cursor over AMQP method argument bytes.
pub(super) type Cursor<'a> = broker_wire::ByteCursor<'a>;

pub(super) trait AmqpCursorExt<'a> {
    fn shortstr(&mut self) -> String;
    fn longstr_slice(&mut self) -> &'a [u8];
    fn field_value(&mut self) -> Option<FieldVal>;
    fn integer_field(&mut self, tag: u8) -> FieldVal;
    fn numeric_field(&mut self, tag: u8) -> FieldVal;
    fn string_field(&mut self) -> Option<FieldVal>;
}

impl<'a> AmqpCursorExt<'a> for Cursor<'a> {
    fn shortstr(&mut self) -> String {
        if self.i >= self.b.len() {
            self.valid = false;
            return String::new();
        }
        let len = self.b[self.i] as usize;
        self.i += 1;
        let Some(end) = self.i.checked_add(len).filter(|end| *end <= self.b.len()) else {
            self.valid = false;
            self.i = self.b.len();
            return String::new();
        };
        let s = match std::str::from_utf8(&self.b[self.i..end]) {
            Ok(value) => value.to_owned(),
            Err(_) => {
                self.valid = false;
                String::new()
            }
        };
        self.i = end;
        s
    }

    fn longstr_slice(&mut self) -> &'a [u8] {
        let len = self.u32() as usize;
        self.take(len)
    }

    fn field_value(&mut self) -> Option<FieldVal> {
        if self.remaining() == 0 {
            return None;
        }
        let tag = self.u8();
        let v = match tag {
            b't' => {
                self.u8();
                FieldVal::Skip // boolean
            }
            b'b' | b'B' | b'U' | b's' | b'u' | b'I' | b'i' | b'L' | b'l' | b'T' => {
                self.integer_field(tag)
            }
            b'f' | b'd' | b'D' => self.numeric_field(tag),
            b'S' => self.string_field()?,
            b'x' | b'F' | b'A' => {
                let _ = self.longstr_slice(); // byte-array / nested table / array
                FieldVal::Skip
            }
            b'V' => FieldVal::Skip, // void
            _ => return None,       // unknown type — width unknown, stop the scan
        };
        self.valid.then_some(v)
    }

    fn integer_field(&mut self, tag: u8) -> FieldVal {
        match tag {
            b'b' => FieldVal::Int(self.u8() as i8 as i64),
            b'B' => FieldVal::Int(self.u8() as i64),
            b'U' | b's' => FieldVal::Int(self.u16() as i16 as i64),
            b'u' => FieldVal::Int(self.u16() as i64),
            b'I' => FieldVal::Int(self.u32() as i32 as i64),
            b'i' => FieldVal::Int(self.u32() as i64),
            b'L' | b'l' | b'T' => FieldVal::Int(self.u64() as i64),
            _ => FieldVal::Skip,
        }
    }

    fn numeric_field(&mut self, tag: u8) -> FieldVal {
        match tag {
            b'f' => {
                self.u32();
            }
            b'd' => {
                self.u64();
            }
            b'D' => {
                self.u8(); // decimal scale
                self.u32(); // decimal value
            }
            _ => {}
        }
        FieldVal::Skip
    }

    fn string_field(&mut self) -> Option<FieldVal> {
        let bytes = self.longstr_slice();
        let value = match std::str::from_utf8(bytes) {
            Ok(value) if self.valid => value,
            _ => {
                self.valid = false;
                return None;
            }
        };
        Some(FieldVal::Str(value.to_owned()))
    }
}
