//! MQTT 3.1.1/5 packet codec and topic/filter wire parsing.
//!
//! This child owns packet bytes, property blocks, topic translation, and
//! bounded frame I/O. CONNECT authentication and per-session broker policy stay
//! in the parent module.

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use crate::server::broker_wire::{self, invalid_data};

pub(super) const MAX_MQTT_PACKET_BYTES: usize = 64 * 1024 * 1024;
pub(super) const MAX_MQTT_CONTROL_PACKET_BYTES: usize = 2 * 1024 * 1024;
pub(super) const MAX_MQTT_FILTERS_PER_PACKET: usize = 1_024;
pub(super) const MAX_MQTT_PROPERTIES: usize = 1_024;
pub(super) const MAX_MQTT_IDENTIFIER_BYTES: usize = 4 * 1024;
pub(super) const MAX_MQTT_TOPIC_BYTES: usize = u16::MAX as usize;
// ── MQTT control packet types (high nibble of byte 1) ─────────────────────
pub(super) const PKT_CONNECT: u8 = 1;
pub(super) const PKT_CONNACK: u8 = 2;
pub(super) const PKT_PUBLISH: u8 = 3;
pub(super) const PKT_PUBACK: u8 = 4;
pub(super) const PKT_SUBSCRIBE: u8 = 8;
pub(super) const PKT_SUBACK: u8 = 9;
pub(super) const PKT_UNSUBSCRIBE: u8 = 10;
pub(super) const PKT_UNSUBACK: u8 = 11;
pub(super) const PKT_PINGREQ: u8 = 12;
pub(super) const PKT_PINGRESP: u8 = 13;
pub(super) const PKT_DISCONNECT: u8 = 14;

// ── Topic ↔ routing-key translation (CONCEPT:EG-KG.query.mqtt-packet-codec) ──────────────────────

/// An MQTT PUBLISH topic (`sport/tennis`) → broker routing key (`sport.tennis`). MQTT
/// levels are `/`-delimited; the broker's topic matcher is `.`-delimited word lists.
pub(super) fn mqtt_topic_to_key(topic: &str) -> String {
    topic.replace('/', ".")
}

/// A broker routing key (`sport.tennis`) → MQTT topic (`sport/tennis`) for delivery.
pub(super) fn key_to_mqtt_topic(key: &str) -> String {
    key.replace('.', "/")
}

/// Scan an MQTT 5.0 PUBLISH property block for the idempotency User Properties
/// `producer-id` + `producer-seq` (CONCEPT:EG-KG.ingest.mqtt-publish-property-block), returning `(producer_id,
/// producer_seq)`. Steps over the other PUBLISH properties by their known wire widths;
/// an unrecognised property id ends the scan (its width is undeterminable) with whatever
/// was found so far. `producer-seq` accepts a decimal string value.
pub(super) fn parse_publish_properties(bytes: &[u8]) -> Option<(Option<String>, Option<i64>)> {
    let mut c = Cursor::new(bytes);
    let mut producer_id: Option<String> = None;
    let mut producer_seq: Option<i64> = None;
    let mut property_count = 0usize;
    while c.remaining() > 0 {
        property_count += 1;
        if property_count > MAX_MQTT_PROPERTIES {
            return None;
        }
        let id = c.u8();
        if !parse_publish_property(&mut c, id, &mut producer_id, &mut producer_seq) {
            return None;
        }
    }
    c.valid.then_some((producer_id, producer_seq))
}

pub(super) fn parse_publish_property(
    c: &mut Cursor<'_>,
    id: u8,
    producer_id: &mut Option<String>,
    producer_seq: &mut Option<i64>,
) -> bool {
    match id {
        0x01 => {
            c.u8();
        } // payload format indicator (byte)
        0x02 => {
            c.u32();
        } // message expiry interval (4 bytes)
        0x23 => {
            c.u16();
        } // topic alias (2 bytes)
        0x03 | 0x08 => {
            let _ = c.mqtt_str();
        } // content-type / response-topic (UTF-8)
        0x09 => {
            let n = c.u16() as usize;
            let _ = c.take(n);
        } // correlation data (binary)
        0x0B => {
            c.varint();
        } // subscription identifier (varint)
        0x26 => return parse_user_property(c, producer_id, producer_seq),
        _ => return false, // unknown property width: fail closed
    }
    true
}

pub(super) fn parse_user_property(
    c: &mut Cursor<'_>,
    producer_id: &mut Option<String>,
    producer_seq: &mut Option<i64>,
) -> bool {
    // User Property: a UTF-8 string pair.
    let key = c.mqtt_str();
    let val = c.mqtt_str();
    match key.as_str() {
        "producer-id" => *producer_id = Some(val),
        "producer-seq" => *producer_seq = val.parse().ok(),
        _ => {}
    }
    c.valid
}

/// An MQTT SUBSCRIBE topic FILTER (`sport/+/#`) → broker topic-binding pattern
/// (`sport.*.#`). MQTT `+` (one level) maps to the broker `*` (one word); MQTT `#`
/// (zero-or-more trailing levels) maps to the broker `#` (zero-or-more words) as-is.
pub(super) fn mqtt_filter_to_pattern(filter: &str) -> String {
    filter.replace('/', ".").replace('+', "*")
}

pub(super) fn valid_topic_name(topic: &str) -> bool {
    !topic.is_empty()
        && topic.len() <= MAX_MQTT_TOPIC_BYTES
        && !topic.contains(['#', '+'])
        && !topic.chars().any(char::is_control)
}

pub(super) fn valid_topic_filter(filter: &str) -> bool {
    if filter.is_empty()
        || filter.len() > MAX_MQTT_TOPIC_BYTES
        || filter.chars().any(char::is_control)
    {
        return false;
    }
    let mut levels = filter.split('/').peekable();
    while let Some(level) = levels.next() {
        if (level.contains('#') && (level != "#" || levels.peek().is_some()))
            || (level.contains('+') && level != "+")
        {
            return false;
        }
    }
    true
}

pub(super) type MqttPacket = (u8, u8, Vec<u8>);

pub(super) struct PublishPacket {
    pub(super) qos: u8,
    pub(super) packet_id: u16,
    pub(super) topic: String,
    pub(super) body: Vec<u8>,
    pub(super) producer_id: Option<String>,
    pub(super) producer_seq: Option<i64>,
}

#[derive(Clone, Copy)]
pub(super) enum FilterRequestKind {
    Subscribe,
    Unsubscribe,
}

impl FilterRequestKind {
    fn invalid_packet(self) -> &'static str {
        match self {
            Self::Subscribe => "invalid MQTT SUBSCRIBE packet",
            Self::Unsubscribe => "invalid MQTT UNSUBSCRIBE packet",
        }
    }

    fn limit_error(self) -> &'static str {
        match self {
            Self::Subscribe => "MQTT subscription packet exceeds resource limits",
            Self::Unsubscribe => "MQTT unsubscription packet exceeds resource limits",
        }
    }

    fn filter_error(self) -> &'static str {
        match self {
            Self::Subscribe => "invalid MQTT subscription filter",
            Self::Unsubscribe => "invalid MQTT unsubscription filter",
        }
    }
}
pub(super) fn parse_publish(
    payload: &mut Vec<u8>,
    version: u8,
    flags: u8,
) -> std::io::Result<PublishPacket> {
    let qos = (flags >> 1) & 0x03;
    let (topic, packet_id, producer_id, producer_seq, body_start) = {
        let mut c = Cursor::new(payload);
        let topic = c.mqtt_str();
        let packet_id = if qos > 0 { c.u16() } else { 0 };
        // MQTT 5.0 property block → extract the EG-314 idempotency user
        // properties (`producer-id` / `producer-seq`); MQTT 3.1.1 has none.
        let (producer_id, producer_seq) = if version >= 5 {
            let props = c.take_props();
            parse_publish_properties(props)
                .ok_or_else(|| invalid_data("invalid MQTT property block"))?
        } else {
            (None, None)
        };
        if !c.valid
            || !valid_topic_name(&topic)
            || qos > 1
            || flags & 0x01 != 0
            || (qos > 0 && packet_id == 0)
        {
            return Err(invalid_data("invalid MQTT PUBLISH packet"));
        }
        (
            topic,
            packet_id,
            producer_id,
            producer_seq,
            c.i.min(payload.len()),
        )
    };
    // Reuse the packet allocation for the body instead of cloning a
    // potentially-large payload into a second Vec.
    payload.drain(..body_start);
    Ok(PublishPacket {
        qos,
        packet_id,
        topic,
        body: std::mem::take(payload),
        producer_id,
        producer_seq,
    })
}

pub(super) fn valid_subscription_options(options: u8, version: u8) -> bool {
    options & 0x03 != 0x03
        && (version >= 5 || options & 0xfc == 0)
        && (version < 5 || options & 0xc0 == 0 && (options >> 4) & 0x03 != 0x03)
}

pub(super) fn parse_filter_options(
    cursor: &mut Cursor<'_>,
    kind: FilterRequestKind,
    version: u8,
) -> bool {
    match kind {
        FilterRequestKind::Subscribe => valid_subscription_options(cursor.u8(), version),
        FilterRequestKind::Unsubscribe => true,
    }
}

pub(super) fn parse_filter_request(
    payload: &[u8],
    version: u8,
    kind: FilterRequestKind,
) -> std::io::Result<(u16, Vec<String>)> {
    let mut c = Cursor::new(payload);
    let packet_id = c.u16();
    if version >= 5 {
        c.skip_props();
    }
    if !c.valid || packet_id == 0 {
        return Err(invalid_data(kind.invalid_packet()));
    }
    let mut patterns = Vec::new();
    while c.remaining() >= 2 {
        if patterns.len() >= MAX_MQTT_FILTERS_PER_PACKET {
            return Err(invalid_data(kind.limit_error()));
        }
        let filter = c.mqtt_str();
        let options_valid = parse_filter_options(&mut c, kind, version);
        if !c.valid {
            return Err(invalid_data(kind.invalid_packet()));
        }
        if !valid_topic_filter(&filter) || !options_valid {
            return Err(invalid_data(kind.filter_error()));
        }
        patterns.push(mqtt_filter_to_pattern(&filter));
    }
    Ok((packet_id, patterns))
}
// ── Packet codec ──────────────────────────────────────────────────────────

/// Read one MQTT control packet: `(type, flags, variable-header+payload bytes)`. A clean
/// EOF at a packet boundary ⇒ `None`.
pub(super) async fn read_packet(
    socket: &mut TcpStream,
) -> std::io::Result<Option<(u8, u8, Vec<u8>)>> {
    let Some((ptype, flags)) = read_fixed_header(socket).await? else {
        return Ok(None);
    };
    let len = read_remaining_length(socket).await?;
    let packet_limit = if ptype == PKT_PUBLISH {
        MAX_MQTT_PACKET_BYTES
    } else {
        MAX_MQTT_CONTROL_PACKET_BYTES
    };
    if len > packet_limit {
        return Err(invalid_data("MQTT packet exceeds the resource limit"));
    }
    let mut payload = vec![0u8; len];
    if len > 0 {
        socket.read_exact(&mut payload).await?;
    }
    Ok(Some((ptype, flags, payload)))
}

pub(super) async fn read_fixed_header(socket: &mut TcpStream) -> std::io::Result<Option<(u8, u8)>> {
    let mut b0 = [0u8; 1];
    if let Err(e) = socket.read_exact(&mut b0).await {
        if e.kind() == std::io::ErrorKind::UnexpectedEof {
            return Ok(None);
        }
        return Err(e);
    }
    let ptype = b0[0] >> 4;
    let flags = b0[0] & 0x0f;
    if !valid_packet_flags(ptype, flags) {
        return Err(invalid_data("invalid MQTT fixed header"));
    }
    Ok(Some((ptype, flags)))
}

pub(super) fn valid_packet_flags(ptype: u8, flags: u8) -> bool {
    match ptype {
        PKT_PUBLISH => ((flags >> 1) & 0x03) != 0x03,
        6 | PKT_SUBSCRIBE | PKT_UNSUBSCRIBE => flags == 0x02,
        1 | 2 | 4 | 5 | 7 | 9 | 11 | 12 | 13 | 14 | 15 => flags == 0,
        _ => false,
    }
}

pub(super) async fn read_remaining_length(socket: &mut TcpStream) -> std::io::Result<usize> {
    // Remaining-length: variable-byte integer (1..4 bytes).
    let mut mult: usize = 1;
    let mut len: usize = 0;
    let mut terminated = false;
    for index in 0..4 {
        let mut nb = [0u8; 1];
        socket.read_exact(&mut nb).await?;
        len += (nb[0] & 0x7f) as usize * mult;
        if nb[0] & 0x80 == 0 {
            if index > 0 && nb[0] == 0 {
                return Err(invalid_data("non-canonical MQTT remaining-length encoding"));
            }
            terminated = true;
            break;
        }
        mult *= 128;
    }
    if !terminated {
        return Err(invalid_data("invalid MQTT remaining-length encoding"));
    }
    Ok(len)
}

/// Write one MQTT control packet: fixed-header byte (`type<<4 | flags`) + variable-byte
/// remaining length + payload.
pub(super) async fn write_packet(
    socket: &mut TcpStream,
    header_byte: u8,
    payload: &[u8],
) -> std::io::Result<()> {
    if payload.len() > MAX_MQTT_PACKET_BYTES {
        return Err(invalid_data(
            "MQTT output packet exceeds the resource limit",
        ));
    }
    let capacity = payload
        .len()
        .checked_add(5)
        .ok_or_else(|| invalid_data("MQTT output packet length overflow"))?;
    let mut buf = Vec::with_capacity(capacity);
    buf.push(header_byte);
    encode_remaining_length(payload.len(), &mut buf);
    buf.extend_from_slice(payload);
    socket.write_all(&buf).await
}

/// Encode `len` as an MQTT variable-byte integer, appending to `out`.
pub(super) fn encode_remaining_length(mut len: usize, out: &mut Vec<u8>) {
    loop {
        let mut byte = (len % 128) as u8;
        len /= 128;
        if len > 0 {
            byte |= 0x80;
        }
        out.push(byte);
        if len == 0 {
            break;
        }
    }
}

/// Decode an MQTT variable-byte integer → `(value, bytes_consumed)`, or `None` if the
/// buffer is truncated / malformed (>4 continuation bytes).
pub(super) fn decode_remaining_length(bytes: &[u8]) -> Option<(usize, usize)> {
    let mut mult: usize = 1;
    let mut value: usize = 0;
    for (i, &b) in bytes.iter().enumerate().take(4) {
        value += (b & 0x7f) as usize * mult;
        if b & 0x80 == 0 {
            if i > 0 && b == 0 {
                return None;
            }
            return Some((value, i + 1));
        }
        mult *= 128;
    }
    None
}

/// Append a UTF-8 MQTT string (2-byte big-endian length prefix + bytes).
pub(super) fn put_mqtt_str(out: &mut Vec<u8>, s: &str) {
    let b = s.as_bytes();
    out.extend_from_slice(&(b.len() as u16).to_be_bytes());
    out.extend_from_slice(b);
}

/// CONNACK: session-present=0 + reason/return-code=0 (accepted). MQTT 5.0 appends a
/// zero-length property block.
pub(super) fn build_connack(version: u8) -> Vec<u8> {
    if version >= 5 {
        vec![0x00, 0x00, 0x00] // ack flags, reason code, property length = 0
    } else {
        vec![0x00, 0x00] // ack flags, return code
    }
}

/// CONNACK rejected due to bad credentials (v3.1.1 code 4 / v5 reason 0x86).
pub(super) fn build_auth_failure_connack(version: u8) -> Vec<u8> {
    if version >= 5 {
        vec![0x00, 0x86, 0x00]
    } else {
        vec![0x00, 0x04]
    }
}

/// SUBACK: packet id + one granted-QoS byte per filter (MQTT 5.0 inserts a zero-length
/// property block after the packet id).
pub(super) fn build_suback(packet_id: u16, granted: &[u8], version: u8) -> Vec<u8> {
    let mut p = Vec::with_capacity(granted.len() + 3);
    p.extend_from_slice(&packet_id.to_be_bytes());
    if version >= 5 {
        p.push(0); // property length = 0
    }
    p.extend_from_slice(granted);
    p
}

/// UNSUBACK: packet id (MQTT 5.0 adds a zero-length property block + one reason byte per
/// unsubscribed filter).
pub(super) fn build_unsuback(packet_id: u16, count: usize, version: u8) -> Vec<u8> {
    let mut p = Vec::with_capacity(count + 3);
    p.extend_from_slice(&packet_id.to_be_bytes());
    if version >= 5 {
        p.push(0); // property length = 0
        p.resize(p.len() + count, 0x00); // one success reason byte per filter
    }
    p
}

// ── Read cursor over packet bytes ─────────────────────────────────────────

/// A minimal read cursor over MQTT variable-header / payload bytes.
pub(super) type Cursor<'a> = broker_wire::ByteCursor<'a>;

pub(super) trait MqttCursorExt<'a> {
    /// Read an MQTT variable-byte-length-prefixed block.
    fn take_props(&mut self) -> &'a [u8];
    /// Read a length-prefixed UTF-8 MQTT string.
    fn mqtt_str(&mut self) -> String;
    /// Skip an MQTT 5.0 property block.
    fn skip_props(&mut self);
    /// Read an MQTT variable-byte integer.
    fn varint(&mut self) -> usize;
    /// Consume the remainder of the packet.
    fn rest(&mut self) -> Vec<u8>;
}

impl<'a> MqttCursorExt<'a> for Cursor<'a> {
    fn take_props(&mut self) -> &'a [u8] {
        let rem = &self.b[self.i.min(self.b.len())..];
        let Some((plen, consumed)) = decode_remaining_length(rem) else {
            self.valid = false;
            self.i = self.b.len();
            return &[];
        };
        let Some(start) = self
            .i
            .checked_add(consumed)
            .filter(|start| *start <= self.b.len())
        else {
            self.valid = false;
            self.i = self.b.len();
            return &[];
        };
        self.i = start;
        self.take(plen)
    }

    fn mqtt_str(&mut self) -> String {
        let len = self.u16() as usize;
        let Some(end) = self.i.checked_add(len).filter(|end| *end <= self.b.len()) else {
            self.valid = false;
            self.i = self.b.len();
            return String::new();
        };
        let s = match std::str::from_utf8(&self.b[self.i..end]) {
            Ok(value) => value.to_string(),
            Err(_) => {
                self.valid = false;
                String::new()
            }
        };
        self.i = end;
        s
    }

    fn skip_props(&mut self) {
        if let Some((plen, consumed)) = decode_remaining_length(&self.b[self.i.min(self.b.len())..])
        {
            if let Some(end) = self
                .i
                .checked_add(consumed)
                .and_then(|start| start.checked_add(plen))
                .filter(|end| *end <= self.b.len())
            {
                self.i = end;
            } else {
                self.valid = false;
                self.i = self.b.len();
            }
        } else {
            self.valid = false;
            self.i = self.b.len();
        }
    }

    fn varint(&mut self) -> usize {
        let rem = &self.b[self.i.min(self.b.len())..];
        if let Some((val, consumed)) = decode_remaining_length(rem) {
            if let Some(end) = self
                .i
                .checked_add(consumed)
                .filter(|end| *end <= self.b.len())
            {
                self.i = end;
                val
            } else {
                self.valid = false;
                self.i = self.b.len();
                0
            }
        } else {
            self.valid = false;
            self.i = self.b.len();
            0
        }
    }

    fn rest(&mut self) -> Vec<u8> {
        let out = self.b[self.i.min(self.b.len())..].to_vec();
        self.i = self.b.len();
        out
    }
}
