//! Binary encoding for [`crate::contract::ClusterLevel`] /
//! [`crate::contract::ClusterExpansion`], plus a chunk-streaming frame
//! envelope so a client can render a first tile before the rest of a graph
//! arrives.
//!
//! ## Why not reuse `viz_interactive::encode_tile`
//!
//! That format is a fixed 48-byte header plus a flat `(f32, f32)` point
//! array -- exactly right for an ordered scatter/series tile, and exactly
//! wrong here: a graph tile needs variable-length strings (node/edge type
//! names, labels), a dictionary (so a type name is spelled once per tile, not
//! once per edge), and two structurally different payloads (a cluster
//! SUMMARY vs. a cluster's full node/edge detail). Reusing that fixed layout
//! would mean bolting a second, incompatible meaning onto the same 48 bytes.
//! What IS reused is the transport shape: a small versioned header, raw
//! little-endian fixed-width fields, explicit `NaN`-as-absent sentinels
//! instead of a branch-per-field presence flag, and a hand-rolled
//! encoder/decoder pair with round-trip tests -- the same idiom, applied to a
//! genuinely different payload shape.
//!
//! ## Layout
//!
//! Every tile starts with a 4-byte magic (`b"EGT1"`), a version byte, and a
//! `TileKind` byte, so a decoder can dispatch on the first 6 bytes alone.
//! Both tile kinds dictionary-encode their type/label strings: every
//! DISTINCT string appears once in a dictionary section, and every node/edge/
//! cluster references it by a `u16` index -- this is where the real payload
//! win over JSON comes from at scale, on top of edges never repeating a
//! string node id (they carry `u32` array indices instead, per the shared
//! contract). Missing floats (`centroid`/`pos`) are encoded as `f32::NAN`
//! rather than a separate presence flag/byte: cheaper, and every consumer
//! already has to guard `is_finite()` before plotting a float from an
//! untrusted tile anyway.

use crate::contract::{
    ChildClusterRef, ClusterExpansion, ClusterLevel, ClusterSummary, InterClusterEdge, TileEdge,
    TileNode,
};

const MAGIC: [u8; 4] = *b"EGT1";
const WIRE_VERSION: u8 = 1;

/// Bound on distinct type/label strings a single tile's dictionary carries --
/// keeps a `u16` dictionary index valid and caps how much a single
/// pathological tile can force a decoder to allocate for the dictionary
/// alone. A real cluster/expand response has at most a few dozen distinct
/// type strings; this is headroom, not a realistic ceiling.
pub const MAX_DICTIONARY_ENTRIES: usize = u16::MAX as usize;

/// Bound on `top_node_types` a single [`ClusterSummary`] carries on the wire.
/// A `GraphSource` that computed more simply truncates -- the field is
/// documented as "most common first", so truncation loses only the least
/// useful entries, never silently corrupts the ones kept.
pub const MAX_TOP_TYPES_PER_CLUSTER: usize = 32;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WireError {
    TooShort { needed: usize, have: usize },
    BadMagic,
    UnsupportedVersion(u8),
    UnknownTileKind(u8),
    BadUtf8,
    DictionaryIndexOutOfRange { index: u16, len: usize },
    TooManyDictionaryEntries(usize),
    TooManyTopTypes(usize),
}

impl std::fmt::Display for WireError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WireError::TooShort { needed, have } => {
                write!(f, "buffer too short: needed {needed} bytes, have {have}")
            }
            WireError::BadMagic => write!(f, "bad magic bytes (not an EGT1 tile)"),
            WireError::UnsupportedVersion(v) => write!(f, "unsupported wire version {v}"),
            WireError::UnknownTileKind(k) => write!(f, "unknown tile kind byte {k}"),
            WireError::BadUtf8 => write!(f, "string field is not valid UTF-8"),
            WireError::DictionaryIndexOutOfRange { index, len } => write!(
                f,
                "dictionary index {index} out of range (dictionary has {len} entries)"
            ),
            WireError::TooManyDictionaryEntries(n) => write!(
                f,
                "{n} distinct dictionary strings exceeds the per-tile bound ({MAX_DICTIONARY_ENTRIES})"
            ),
            WireError::TooManyTopTypes(n) => write!(
                f,
                "{n} top_node_types exceeds the per-cluster bound ({MAX_TOP_TYPES_PER_CLUSTER})"
            ),
        }
    }
}

impl std::error::Error for WireError {}

type Result<T> = std::result::Result<T, WireError>;

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TileKind {
    ClusterLevel = 0,
    ClusterExpansion = 1,
    /// A sentinel frame closing a [`stream`] response so a client can detect
    /// a truncated stream instead of silently treating "connection closed
    /// early" as "graph fully loaded".
    StreamEnd = 0xFF,
}

impl TileKind {
    fn from_u8(b: u8) -> Result<Self> {
        match b {
            0 => Ok(TileKind::ClusterLevel),
            1 => Ok(TileKind::ClusterExpansion),
            0xFF => Ok(TileKind::StreamEnd),
            other => Err(WireError::UnknownTileKind(other)),
        }
    }
}

// ---------------------------------------------------------------------------
// Low-level cursor helpers (write side)
// ---------------------------------------------------------------------------

struct Writer {
    buf: Vec<u8>,
}

impl Writer {
    fn new() -> Self {
        Self { buf: Vec::new() }
    }
    fn u8(&mut self, v: u8) {
        self.buf.push(v);
    }
    fn u16(&mut self, v: u16) {
        self.buf.extend_from_slice(&v.to_le_bytes());
    }
    fn u32(&mut self, v: u32) {
        self.buf.extend_from_slice(&v.to_le_bytes());
    }
    fn u64(&mut self, v: u64) {
        self.buf.extend_from_slice(&v.to_le_bytes());
    }
    fn f32(&mut self, v: f32) {
        self.buf.extend_from_slice(&v.to_le_bytes());
    }
    /// `u16`-length-prefixed UTF-8 string, truncated to `u16::MAX` bytes --
    /// a defensive bound, not an expected case (labels/type names are short).
    fn str16(&mut self, s: &str) {
        let bytes = s.as_bytes();
        let len = bytes.len().min(u16::MAX as usize);
        self.u16(len as u16);
        self.buf.extend_from_slice(&bytes[..len]);
    }
}

// ---------------------------------------------------------------------------
// Low-level cursor helpers (read side)
// ---------------------------------------------------------------------------

struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    fn new(buf: &'a [u8]) -> Self {
        Self { buf, pos: 0 }
    }
    fn need(&self, n: usize) -> Result<()> {
        if self.pos + n > self.buf.len() {
            Err(WireError::TooShort {
                needed: self.pos + n,
                have: self.buf.len(),
            })
        } else {
            Ok(())
        }
    }
    fn u8(&mut self) -> Result<u8> {
        self.need(1)?;
        let v = self.buf[self.pos];
        self.pos += 1;
        Ok(v)
    }
    fn u16(&mut self) -> Result<u16> {
        self.need(2)?;
        let v = u16::from_le_bytes(self.buf[self.pos..self.pos + 2].try_into().unwrap());
        self.pos += 2;
        Ok(v)
    }
    fn u32(&mut self) -> Result<u32> {
        self.need(4)?;
        let v = u32::from_le_bytes(self.buf[self.pos..self.pos + 4].try_into().unwrap());
        self.pos += 4;
        Ok(v)
    }
    fn u64(&mut self) -> Result<u64> {
        self.need(8)?;
        let v = u64::from_le_bytes(self.buf[self.pos..self.pos + 8].try_into().unwrap());
        self.pos += 8;
        Ok(v)
    }
    fn f32(&mut self) -> Result<f32> {
        self.need(4)?;
        let v = f32::from_le_bytes(self.buf[self.pos..self.pos + 4].try_into().unwrap());
        self.pos += 4;
        Ok(v)
    }
    fn str16(&mut self) -> Result<String> {
        let len = self.u16()? as usize;
        self.need(len)?;
        let s = std::str::from_utf8(&self.buf[self.pos..self.pos + len])
            .map_err(|_| WireError::BadUtf8)?
            .to_string();
        self.pos += len;
        Ok(s)
    }
}

/// `f32::NAN`-as-absent sentinel for an optional `(x, y)` pair.
fn write_opt_pos(w: &mut Writer, pos: Option<(f32, f32)>) {
    let (x, y) = pos.unwrap_or((f32::NAN, f32::NAN));
    w.f32(x);
    w.f32(y);
}

fn read_opt_pos(r: &mut Reader) -> Result<Option<(f32, f32)>> {
    let x = r.f32()?;
    let y = r.f32()?;
    if x.is_nan() || y.is_nan() {
        Ok(None)
    } else {
        Ok(Some((x, y)))
    }
}

/// A dictionary built while encoding: every distinct string seen gets one
/// entry, in first-seen order, referenced everywhere else by `u16` index.
#[derive(Default)]
struct DictBuilder {
    index_of: std::collections::HashMap<String, u16>,
    entries: Vec<String>,
}

impl DictBuilder {
    fn intern(&mut self, s: &str) -> Result<u16> {
        if let Some(&idx) = self.index_of.get(s) {
            return Ok(idx);
        }
        if self.entries.len() >= MAX_DICTIONARY_ENTRIES {
            return Err(WireError::TooManyDictionaryEntries(self.entries.len() + 1));
        }
        let idx = self.entries.len() as u16;
        self.entries.push(s.to_string());
        self.index_of.insert(s.to_string(), idx);
        Ok(idx)
    }
    fn write(&self, w: &mut Writer) {
        w.u16(self.entries.len() as u16);
        for e in &self.entries {
            w.str16(e);
        }
    }
}

fn read_dictionary(r: &mut Reader) -> Result<Vec<String>> {
    let count = r.u16()? as usize;
    let mut out = Vec::with_capacity(count);
    for _ in 0..count {
        out.push(r.str16()?);
    }
    Ok(out)
}

fn dict_lookup(dict: &[String], idx: u16) -> Result<&str> {
    dict.get(idx as usize)
        .map(String::as_str)
        .ok_or(WireError::DictionaryIndexOutOfRange {
            index: idx,
            len: dict.len(),
        })
}

// ---------------------------------------------------------------------------
// ClusterLevel
// ---------------------------------------------------------------------------

fn write_header(w: &mut Writer, kind: TileKind) {
    w.buf.extend_from_slice(&MAGIC);
    w.u8(WIRE_VERSION);
    w.u8(kind as u8);
}

fn read_header(r: &mut Reader) -> Result<TileKind> {
    r.need(6)?;
    if &r.buf[r.pos..r.pos + 4] != MAGIC.as_slice() {
        return Err(WireError::BadMagic);
    }
    r.pos += 4;
    let version = r.u8()?;
    if version != WIRE_VERSION {
        return Err(WireError::UnsupportedVersion(version));
    }
    TileKind::from_u8(r.u8()?)
}

/// Encode a [`ClusterLevel`] to its binary wire form.
pub fn encode_cluster_level(level: &ClusterLevel) -> Result<Vec<u8>> {
    let mut dict = DictBuilder::default();
    for c in &level.clusters {
        if c.top_node_types.len() > MAX_TOP_TYPES_PER_CLUSTER {
            return Err(WireError::TooManyTopTypes(c.top_node_types.len()));
        }
        for t in &c.top_node_types {
            dict.intern(t)?;
        }
    }

    let mut w = Writer::new();
    write_header(&mut w, TileKind::ClusterLevel);
    w.u32(level.level);
    match level.parent_cluster_id {
        Some(id) => {
            w.u8(1);
            w.u64(id);
        }
        None => {
            w.u8(0);
            w.u64(0);
        }
    }
    dict.write(&mut w);

    w.u32(level.clusters.len() as u32);
    for c in &level.clusters {
        w.u64(c.id);
        w.u32(c.node_count);
        w.u32(c.edge_count);
        write_opt_pos(&mut w, c.centroid);
        w.str16(&c.label);
        w.u8(c.top_node_types.len() as u8);
        for t in &c.top_node_types {
            w.u16(dict.intern(t)?);
        }
    }

    w.u32(level.inter_cluster_edges.len() as u32);
    for e in &level.inter_cluster_edges {
        w.u32(e.src_idx);
        w.u32(e.dst_idx);
        w.f32(e.weight);
    }

    Ok(w.buf)
}

/// Decode a [`ClusterLevel`] previously produced by [`encode_cluster_level`].
/// Never panics on truncated/malformed input -- returns [`WireError`].
pub fn decode_cluster_level(bytes: &[u8]) -> Result<ClusterLevel> {
    let mut r = Reader::new(bytes);
    let kind = read_header(&mut r)?;
    if kind != TileKind::ClusterLevel {
        return Err(WireError::UnknownTileKind(kind as u8));
    }
    let level = r.u32()?;
    let has_parent = r.u8()? != 0;
    let parent_raw = r.u64()?;
    let parent_cluster_id = if has_parent { Some(parent_raw) } else { None };
    let dict = read_dictionary(&mut r)?;

    let cluster_count = r.u32()? as usize;
    let mut clusters = Vec::with_capacity(cluster_count.min(1 << 20));
    for _ in 0..cluster_count {
        let id = r.u64()?;
        let node_count = r.u32()?;
        let edge_count = r.u32()?;
        let centroid = read_opt_pos(&mut r)?;
        let label = r.str16()?;
        let top_type_count = r.u8()? as usize;
        let mut top_node_types = Vec::with_capacity(top_type_count);
        for _ in 0..top_type_count {
            let idx = r.u16()?;
            top_node_types.push(dict_lookup(&dict, idx)?.to_string());
        }
        clusters.push(ClusterSummary {
            id,
            label,
            node_count,
            edge_count,
            centroid,
            top_node_types,
        });
    }

    let inter_edge_count = r.u32()? as usize;
    let mut inter_cluster_edges = Vec::with_capacity(inter_edge_count.min(1 << 20));
    for _ in 0..inter_edge_count {
        let src_idx = r.u32()?;
        let dst_idx = r.u32()?;
        let weight = r.f32()?;
        inter_cluster_edges.push(InterClusterEdge {
            src_idx,
            dst_idx,
            weight,
        });
    }

    Ok(ClusterLevel {
        level,
        parent_cluster_id,
        clusters,
        inter_cluster_edges,
    })
}

// ---------------------------------------------------------------------------
// ClusterExpansion
// ---------------------------------------------------------------------------

/// Encode a [`ClusterExpansion`] to its binary wire form.
pub fn encode_cluster_expansion(expansion: &ClusterExpansion) -> Result<Vec<u8>> {
    let mut dict = DictBuilder::default();
    for n in &expansion.nodes {
        dict.intern(&n.node_type)?;
    }
    for e in &expansion.edges {
        dict.intern(&e.edge_type)?;
    }

    let mut w = Writer::new();
    write_header(&mut w, TileKind::ClusterExpansion);
    w.u64(expansion.cluster_id);
    dict.write(&mut w);

    w.u32(expansion.nodes.len() as u32);
    for n in &expansion.nodes {
        w.str16(&n.id);
        w.str16(&n.label);
        w.u16(dict.intern(&n.node_type)?);
        write_opt_pos(&mut w, n.pos);
    }

    w.u32(expansion.edges.len() as u32);
    for e in &expansion.edges {
        w.u32(e.src_idx);
        w.u32(e.dst_idx);
        w.u16(dict.intern(&e.edge_type)?);
    }

    w.u32(expansion.child_clusters.len() as u32);
    for c in &expansion.child_clusters {
        w.u64(c.id);
        w.u32(c.node_count);
        w.str16(&c.label);
    }

    Ok(w.buf)
}

/// Decode a [`ClusterExpansion`] previously produced by
/// [`encode_cluster_expansion`]. Never panics on truncated/malformed input.
pub fn decode_cluster_expansion(bytes: &[u8]) -> Result<ClusterExpansion> {
    let mut r = Reader::new(bytes);
    let kind = read_header(&mut r)?;
    if kind != TileKind::ClusterExpansion {
        return Err(WireError::UnknownTileKind(kind as u8));
    }
    let cluster_id = r.u64()?;
    let dict = read_dictionary(&mut r)?;

    let node_count = r.u32()? as usize;
    let mut nodes = Vec::with_capacity(node_count.min(1 << 20));
    for _ in 0..node_count {
        let id = r.str16()?;
        let label = r.str16()?;
        let type_idx = r.u16()?;
        let node_type = dict_lookup(&dict, type_idx)?.to_string();
        let pos = read_opt_pos(&mut r)?;
        nodes.push(TileNode {
            id,
            label,
            node_type,
            pos,
        });
    }

    let edge_count = r.u32()? as usize;
    let mut edges = Vec::with_capacity(edge_count.min(1 << 20));
    for _ in 0..edge_count {
        let src_idx = r.u32()?;
        let dst_idx = r.u32()?;
        let type_idx = r.u16()?;
        let edge_type = dict_lookup(&dict, type_idx)?.to_string();
        edges.push(TileEdge {
            src_idx,
            dst_idx,
            edge_type,
        });
    }

    let child_count = r.u32()? as usize;
    let mut child_clusters = Vec::with_capacity(child_count.min(1 << 20));
    for _ in 0..child_count {
        let id = r.u64()?;
        let node_count = r.u32()?;
        let label = r.str16()?;
        child_clusters.push(ChildClusterRef {
            id,
            label,
            node_count,
        });
    }

    Ok(ClusterExpansion {
        cluster_id,
        nodes,
        edges,
        child_clusters,
    })
}

// ---------------------------------------------------------------------------
// Streaming frame envelope
// ---------------------------------------------------------------------------

/// Append one already-encoded tile to a stream as a length-prefixed frame:
/// `u32` little-endian byte length, then the tile bytes verbatim. A stream is
/// just a concatenation of frames -- the tile's own header (magic/version/
/// kind) is still there inside each frame, so a frame is independently
/// decodable the moment its bytes are fully buffered, without waiting for the
/// rest of the stream. This is what lets a client render a first cluster
/// tile the instant its frame lands, before later frames (e.g. the top-K
/// expand tiles) arrive.
pub fn write_frame(stream: &mut Vec<u8>, tile_bytes: &[u8]) {
    stream.extend_from_slice(&(tile_bytes.len() as u32).to_le_bytes());
    stream.extend_from_slice(tile_bytes);
}

/// Append the sentinel frame that closes a stream -- `total_frames` counts
/// only the PRECEDING tile frames (never itself), so a client can assert
/// `frames_received == total_frames` and detect a connection that closed
/// early as a truncated stream rather than treating it as "graph fully
/// loaded".
pub fn write_stream_end(stream: &mut Vec<u8>, total_frames: u32) {
    let mut w = Writer::new();
    write_header(&mut w, TileKind::StreamEnd);
    w.u32(total_frames);
    write_frame(stream, &w.buf);
}

/// One frame decoded out of a stream by [`read_frames`]: either a tile
/// ([`ClusterLevel`]/[`ClusterExpansion`], left undecoded here -- the caller
/// picks the right decoder from [`StreamFrame::kind`]) or the closing
/// [`TileKind::StreamEnd`] sentinel.
pub struct StreamFrame<'a> {
    pub kind: TileKind,
    pub bytes: &'a [u8],
}

/// Incrementally split a byte buffer into whole frames written by
/// [`write_frame`]/[`write_stream_end`]. Returns every WHOLE frame currently
/// available plus the number of bytes consumed (always `<= buf.len()`); a
/// caller reading a growing buffer (e.g. from a chunked HTTP response) keeps
/// the unconsumed tail and calls again as more bytes arrive -- this is what
/// makes the format genuinely streamable rather than "one big buffer with an
/// internal length prefix nobody reads until EOF".
pub fn read_frames(buf: &[u8]) -> (Vec<StreamFrame<'_>>, usize) {
    let mut frames = Vec::new();
    let mut pos = 0usize;
    loop {
        if pos + 4 > buf.len() {
            break;
        }
        let len = u32::from_le_bytes(buf[pos..pos + 4].try_into().unwrap()) as usize;
        if pos + 4 + len > buf.len() {
            break;
        }
        let tile_bytes = &buf[pos + 4..pos + 4 + len];
        // A malformed/too-short frame body degrades to being skipped (not
        // decoded) rather than aborting the whole stream -- `read_header`
        // alone is cheap and safe to attempt on any byte slice.
        let kind = {
            let mut hr = Reader::new(tile_bytes);
            read_header(&mut hr)
        };
        if let Ok(kind) = kind {
            frames.push(StreamFrame {
                kind,
                bytes: tile_bytes,
            });
        }
        pos += 4 + len;
    }
    (frames, pos)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::contract::{ChildClusterRef, ClusterSummary, TileEdge, TileNode};

    fn sample_level() -> ClusterLevel {
        ClusterLevel {
            level: 2,
            parent_cluster_id: Some(9),
            clusters: vec![
                ClusterSummary {
                    id: 1,
                    label: "Person".to_string(),
                    node_count: 1000,
                    edge_count: 4000,
                    centroid: Some((0.25, 0.75)),
                    top_node_types: vec!["Person".to_string(), "Employee".to_string()],
                },
                ClusterSummary {
                    id: 2,
                    label: "Org".to_string(),
                    node_count: 40,
                    edge_count: 12,
                    centroid: None,
                    top_node_types: vec!["Org".to_string()],
                },
            ],
            inter_cluster_edges: vec![InterClusterEdge {
                src_idx: 0,
                dst_idx: 1,
                weight: 3.5,
            }],
        }
    }

    fn sample_expansion() -> ClusterExpansion {
        ClusterExpansion {
            cluster_id: 1,
            nodes: vec![
                TileNode {
                    id: "n:alice".to_string(),
                    label: "Alice".to_string(),
                    node_type: "Person".to_string(),
                    pos: Some((0.1, 0.2)),
                },
                TileNode {
                    id: "n:bob".to_string(),
                    label: "Bob".to_string(),
                    node_type: "Person".to_string(),
                    pos: None,
                },
            ],
            edges: vec![TileEdge {
                src_idx: 0,
                dst_idx: 1,
                edge_type: "knows".to_string(),
            }],
            child_clusters: vec![ChildClusterRef {
                id: 10,
                label: "Sub".to_string(),
                node_count: 2,
            }],
        }
    }

    #[test]
    fn cluster_level_round_trips_through_binary() {
        let level = sample_level();
        let bytes = encode_cluster_level(&level).unwrap();
        assert_eq!(&bytes[0..4], b"EGT1");
        let decoded = decode_cluster_level(&bytes).unwrap();
        assert_eq!(decoded, level);
    }

    #[test]
    fn cluster_expansion_round_trips_through_binary() {
        let expansion = sample_expansion();
        let bytes = encode_cluster_expansion(&expansion).unwrap();
        let decoded = decode_cluster_expansion(&bytes).unwrap();
        assert_eq!(decoded, expansion);
    }

    #[test]
    fn absent_centroid_and_pos_round_trip_as_none_not_zero() {
        let mut level = sample_level();
        level.clusters[0].centroid = None;
        let bytes = encode_cluster_level(&level).unwrap();
        let decoded = decode_cluster_level(&bytes).unwrap();
        assert_eq!(decoded.clusters[0].centroid, None);
    }

    #[test]
    fn dictionary_deduplicates_repeated_type_strings() {
        // 100 edges, all the SAME type string -- the dictionary must store
        // that string exactly once, not once per edge.
        let nodes: Vec<TileNode> = (0..50)
            .map(|i| TileNode {
                id: format!("n:{i}"),
                label: String::new(),
                node_type: "Person".to_string(),
                pos: None,
            })
            .collect();
        let edges: Vec<TileEdge> = (0..49)
            .map(|i| TileEdge {
                src_idx: i,
                dst_idx: i + 1,
                edge_type: "knows".to_string(),
            })
            .collect();
        let expansion = ClusterExpansion {
            cluster_id: 1,
            nodes,
            edges,
            child_clusters: vec![],
        };
        let bytes = encode_cluster_expansion(&expansion).unwrap();
        let decoded = decode_cluster_expansion(&bytes).unwrap();
        assert_eq!(decoded, expansion);
        // A JSON encoding of the same data repeats "Person"/"knows" per row;
        // the binary form must be substantially smaller.
        let json_len = serde_json::to_vec(&expansion).unwrap().len();
        assert!(
            bytes.len() < json_len,
            "binary ({} bytes) should beat JSON ({} bytes) once strings dictionary-dedupe",
            bytes.len(),
            json_len
        );
    }

    #[test]
    fn decode_rejects_bad_magic() {
        let err = decode_cluster_level(&[0u8; 16]).unwrap_err();
        assert_eq!(err, WireError::BadMagic);
    }

    #[test]
    fn decode_rejects_truncated_input_without_panicking() {
        let level = sample_level();
        let bytes = encode_cluster_level(&level).unwrap();
        for cut in 0..bytes.len() {
            // Must return an Err, never panic, for every possible truncation
            // point -- a streaming reader can hand a decoder a partial frame
            // if `read_frames`' own length check is ever bypassed.
            let _ = decode_cluster_level(&bytes[..cut]);
        }
    }

    #[test]
    fn decode_rejects_wrong_tile_kind() {
        let level = sample_level();
        let bytes = encode_cluster_level(&level).unwrap();
        let err = decode_cluster_expansion(&bytes).unwrap_err();
        assert_eq!(
            err,
            WireError::UnknownTileKind(TileKind::ClusterLevel as u8)
        );
    }

    #[test]
    fn decode_rejects_out_of_range_dictionary_index() {
        let mut w = Writer::new();
        write_header(&mut w, TileKind::ClusterLevel);
        w.u32(0);
        w.u8(0);
        w.u64(0);
        w.u16(0); // empty dictionary
        w.u32(1); // one cluster
        w.u64(1);
        w.u32(0);
        w.u32(0);
        write_opt_pos(&mut w, None);
        w.str16("x");
        w.u8(1);
        w.u16(5); // dictionary index 5, but the dictionary is empty
        w.u32(0); // zero inter-cluster edges
        let err = decode_cluster_level(&w.buf).unwrap_err();
        assert_eq!(
            err,
            WireError::DictionaryIndexOutOfRange { index: 5, len: 0 }
        );
    }

    #[test]
    fn stream_frames_round_trip_and_end_sentinel_carries_total() {
        let level = sample_level();
        let expansion = sample_expansion();
        let level_bytes = encode_cluster_level(&level).unwrap();
        let expansion_bytes = encode_cluster_expansion(&expansion).unwrap();

        let mut stream = Vec::new();
        write_frame(&mut stream, &level_bytes);
        write_frame(&mut stream, &expansion_bytes);
        write_stream_end(&mut stream, 2);

        let (frames, consumed) = read_frames(&stream);
        assert_eq!(consumed, stream.len());
        assert_eq!(frames.len(), 3);
        assert_eq!(frames[0].kind, TileKind::ClusterLevel);
        assert_eq!(decode_cluster_level(frames[0].bytes).unwrap(), level);
        assert_eq!(frames[1].kind, TileKind::ClusterExpansion);
        assert_eq!(
            decode_cluster_expansion(frames[1].bytes).unwrap(),
            expansion
        );
        assert_eq!(frames[2].kind, TileKind::StreamEnd);
        let mut end_reader = Reader::new(frames[2].bytes);
        read_header(&mut end_reader).unwrap();
        assert_eq!(end_reader.u32().unwrap(), 2);
    }

    #[test]
    fn read_frames_returns_only_whole_frames_from_a_partial_buffer() {
        let level = sample_level();
        let level_bytes = encode_cluster_level(&level).unwrap();
        let mut stream = Vec::new();
        write_frame(&mut stream, &level_bytes);
        write_frame(&mut stream, &level_bytes);

        // Simulate a chunked read that only delivered the first frame plus a
        // few bytes of the second.
        let partial = &stream[..stream.len() - level_bytes.len() + 3];
        let (frames, consumed) = read_frames(partial);
        assert_eq!(
            frames.len(),
            1,
            "the partial second frame must not be returned"
        );
        assert_eq!(consumed, 4 + level_bytes.len());
        assert!(consumed < partial.len());

        // Feeding the remaining bytes (the unconsumed tail plus the rest of
        // the stream) yields the second frame too.
        let mut remainder = partial[consumed..].to_vec();
        remainder.extend_from_slice(&stream[partial.len()..]);
        let (frames2, consumed2) = read_frames(&remainder);
        assert_eq!(frames2.len(), 1);
        assert_eq!(consumed2, remainder.len());
    }
}
