//! Versioned, bounded HNSW graph image. The caller owns durable storage and
//! activation; this module opens no files and never repairs a corrupt graph.

use super::compact::CompactHnswIndex;
use super::{HnswIndex, Node, MAX_LEVEL_CAP};
use crate::flat::Metric;
use std::collections::HashSet;
use std::io::{Error, ErrorKind, Result};

const MAGIC: &[u8; 8] = b"EGHNSW\0\x01";
const COMPACT_MAGIC: &[u8; 8] = b"EGHNSW\0\x02";
const VECTORLESS_COMPACT_MAGIC: &[u8; 8] = b"EGHNSW\0\x03";
pub(super) const COMPACT_HEADER_BYTES: usize = 58;
const COMPACT_DIGEST_OFFSET: usize = 50;
/// Maximum encoded graph size accepted for serving.
pub const MAX_ARTIFACT_BYTES: usize = 256 * 1024 * 1024;
pub const MAX_DIM: usize = 4096;
pub const MAX_NODES: usize = 250_000;
pub const MAX_M: usize = 128;
pub const MAX_EF_CONSTRUCTION: usize = 4096;
pub(super) const MAX_COORDINATE: f32 = 1.0e15;
const NO_ENTRY: u32 = u32::MAX;

fn invalid(message: &'static str) -> Error {
    Error::new(ErrorKind::InvalidData, message)
}

fn validate_parameters(index: &HnswIndex) -> Result<()> {
    if index.dim == 0 || index.dim > MAX_DIM {
        return Err(invalid("HNSW dimension exceeds artifact bounds"));
    }
    if !(2..=MAX_M).contains(&index.m)
        || index.m0 != index.m * 2
        || !(index.m..=MAX_EF_CONSTRUCTION).contains(&index.ef_construction)
        || !index.ml.is_finite()
        || index.ml != 1.0 / (index.m as f64).ln()
    {
        return Err(invalid("HNSW construction parameters are invalid"));
    }
    if index.nodes.len() > MAX_NODES || index.max_level > MAX_LEVEL_CAP {
        return Err(invalid("HNSW graph exceeds artifact bounds"));
    }
    Ok(())
}

fn validate_layer(
    index: &HnswIndex,
    node_idx: usize,
    layer: usize,
    neighbors: &[usize],
) -> Result<()> {
    let cap = if layer == 0 { index.m0 } else { index.m };
    if neighbors.len() > cap {
        return Err(invalid("HNSW adjacency exceeds degree bound"));
    }
    let mut seen = HashSet::with_capacity(neighbors.len());
    for &neighbor in neighbors {
        if neighbor == node_idx
            || neighbor >= index.nodes.len()
            || index.nodes[neighbor].neighbors.len() <= layer
            || !seen.insert(neighbor)
        {
            return Err(invalid("HNSW adjacency has an invalid edge"));
        }
    }
    Ok(())
}

fn validate_node(
    index: &HnswIndex,
    node_idx: usize,
    node: &Node,
    ids: &mut HashSet<u64>,
) -> Result<()> {
    if !ids.insert(node.id) {
        return Err(invalid("HNSW graph has duplicate external ids"));
    }
    if node.vector.len() != index.dim
        || node
            .vector
            .iter()
            .any(|v| !v.is_finite() || v.abs() > MAX_COORDINATE)
    {
        return Err(invalid("HNSW vector has invalid coordinates"));
    }
    if node.neighbors.is_empty() || node.neighbors.len() > MAX_LEVEL_CAP + 1 {
        return Err(invalid("HNSW node level is invalid"));
    }
    for (layer, neighbors) in node.neighbors.iter().enumerate() {
        validate_layer(index, node_idx, layer, neighbors)?;
    }
    Ok(())
}

pub(super) fn validate(index: &HnswIndex) -> Result<()> {
    validate_parameters(index)?;
    if index.nodes.is_empty() {
        if index.entry_point.is_some() || index.max_level != 0 {
            return Err(invalid("empty HNSW graph has an entry point"));
        }
        return Ok(());
    }
    let Some(entry) = index.entry_point else {
        return Err(invalid("nonempty HNSW graph has no entry point"));
    };
    if entry >= index.nodes.len() {
        return Err(invalid("HNSW entry point is out of range"));
    }
    let mut ids = HashSet::with_capacity(index.nodes.len());
    let mut highest = 0;
    for (node_idx, node) in index.nodes.iter().enumerate() {
        validate_node(index, node_idx, node, &mut ids)?;
        highest = highest.max(node.neighbors.len() - 1);
    }
    if highest != index.max_level || index.nodes[entry].neighbors.len() - 1 != highest {
        return Err(invalid("HNSW entry point does not own the top layer"));
    }
    Ok(())
}

fn append(out: &mut Vec<u8>, bytes: &[u8]) -> Result<()> {
    if out
        .len()
        .checked_add(bytes.len())
        .is_none_or(|len| len > MAX_ARTIFACT_BYTES)
    {
        return Err(invalid("HNSW artifact exceeds byte bound"));
    }
    out.extend_from_slice(bytes);
    Ok(())
}

/// Encode a complete validated graph for durable storage. No graph rebuild is
/// needed after [`decode`].
pub fn encode(index: &HnswIndex) -> Result<Vec<u8>> {
    validate(index)?;
    let mut out = Vec::new();
    append(&mut out, MAGIC)?;
    append(&mut out, &(index.dim as u32).to_le_bytes())?;
    let metric = match index.metric {
        Metric::L2 => 0,
        Metric::Cosine => 1,
        Metric::InnerProduct => 2,
    };
    append(&mut out, &[metric])?;
    append(&mut out, &(index.m as u16).to_le_bytes())?;
    append(&mut out, &(index.ef_construction as u16).to_le_bytes())?;
    append(&mut out, &index.seed.to_le_bytes())?;
    append(&mut out, &[index.max_level as u8])?;
    append(
        &mut out,
        &index
            .entry_point
            .map_or(NO_ENTRY, |n| n as u32)
            .to_le_bytes(),
    )?;
    append(&mut out, &(index.nodes.len() as u32).to_le_bytes())?;
    for node in &index.nodes {
        append(&mut out, &node.id.to_le_bytes())?;
        append(&mut out, &[node.neighbors.len() as u8])?;
        for value in &node.vector {
            append(&mut out, &value.to_le_bytes())?;
        }
        for neighbors in &node.neighbors {
            append(&mut out, &(neighbors.len() as u16).to_le_bytes())?;
            for &neighbor in neighbors {
                append(&mut out, &(neighbor as u32).to_le_bytes())?;
            }
        }
    }
    Ok(out)
}

struct Reader<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> Reader<'a> {
    fn take(&mut self, len: usize) -> Result<&'a [u8]> {
        let end = self
            .offset
            .checked_add(len)
            .ok_or_else(|| invalid("HNSW artifact length overflows"))?;
        let bytes = self
            .bytes
            .get(self.offset..end)
            .ok_or_else(|| invalid("HNSW artifact is truncated"))?;
        self.offset = end;
        Ok(bytes)
    }
    fn u8(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }
    fn u16(&mut self) -> Result<u16> {
        Ok(u16::from_le_bytes(self.take(2)?.try_into().unwrap()))
    }
    fn u32(&mut self) -> Result<u32> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }
    fn u64(&mut self) -> Result<u64> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }
    fn f32(&mut self) -> Result<f32> {
        Ok(f32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }
}

struct Header {
    dim: usize,
    metric: Metric,
    m: usize,
    ef: usize,
    seed: u64,
    max_level: usize,
    entry: u32,
    count: usize,
}

fn validate_header_bounds(header: &Header) -> Result<()> {
    if header.dim == 0
        || header.dim > MAX_DIM
        || !(2..=MAX_M).contains(&header.m)
        || !(header.m..=MAX_EF_CONSTRUCTION).contains(&header.ef)
        || header.max_level > MAX_LEVEL_CAP
        || header.count > MAX_NODES
    {
        return Err(invalid("HNSW artifact header exceeds bounds"));
    }
    Ok(())
}

fn read_header(reader: &mut Reader<'_>) -> Result<Header> {
    if reader.take(MAGIC.len())? != MAGIC {
        return Err(invalid("unsupported HNSW artifact version"));
    }
    let dim = reader.u32()? as usize;
    let metric = match reader.u8()? {
        0 => Metric::L2,
        1 => Metric::Cosine,
        2 => Metric::InnerProduct,
        _ => return Err(invalid("HNSW metric is invalid")),
    };
    let m = reader.u16()? as usize;
    let ef = reader.u16()? as usize;
    let seed = reader.u64()?;
    let max_level = reader.u8()? as usize;
    let entry = reader.u32()?;
    let count = reader.u32()? as usize;
    let header = Header {
        dim,
        metric,
        m,
        ef,
        seed,
        max_level,
        entry,
        count,
    };
    validate_header_bounds(&header)?;
    Ok(header)
}

fn read_vector(reader: &mut Reader<'_>, dim: usize) -> Result<Vec<f32>> {
    let mut vector = Vec::new();
    vector
        .try_reserve_exact(dim)
        .map_err(|_| invalid("HNSW vector allocation failed"))?;
    for _ in 0..dim {
        let value = reader.f32()?;
        if !value.is_finite() || value.abs() > MAX_COORDINATE {
            return Err(invalid("HNSW vector has invalid coordinates"));
        }
        vector.push(value);
    }
    Ok(vector)
}

fn read_neighbors(
    reader: &mut Reader<'_>,
    layer_count: usize,
    m: usize,
) -> Result<Vec<Vec<usize>>> {
    let mut neighbors = Vec::with_capacity(layer_count);
    for layer in 0..layer_count {
        let degree = reader.u16()? as usize;
        let cap = if layer == 0 { m * 2 } else { m };
        if degree > cap || degree > (reader.bytes.len() - reader.offset) / 4 {
            return Err(invalid("HNSW adjacency exceeds bounds"));
        }
        let mut edges = Vec::with_capacity(degree);
        for _ in 0..degree {
            edges.push(reader.u32()? as usize);
        }
        neighbors.push(edges);
    }
    Ok(neighbors)
}

fn read_node(reader: &mut Reader<'_>, header: &Header) -> Result<Node> {
    let id = reader.u64()?;
    let layer_count = reader.u8()? as usize;
    if layer_count == 0 || layer_count > MAX_LEVEL_CAP + 1 || layer_count > header.max_level + 1 {
        return Err(invalid("HNSW node level is invalid"));
    }
    let vector = read_vector(reader, header.dim)?;
    let neighbors = read_neighbors(reader, layer_count, header.m)?;
    Ok(Node {
        id,
        vector,
        neighbors,
    })
}

fn read_nodes(reader: &mut Reader<'_>, header: &Header) -> Result<Vec<Node>> {
    let min_node_bytes = 8usize + 1 + header.dim * 4 + 2;
    if header.count > (reader.bytes.len() - reader.offset) / min_node_bytes {
        return Err(invalid("HNSW node count exceeds remaining bytes"));
    }
    let mut nodes = Vec::new();
    nodes
        .try_reserve_exact(header.count)
        .map_err(|_| invalid("HNSW node allocation failed"))?;
    for _ in 0..header.count {
        nodes.push(read_node(reader, header)?);
    }
    Ok(nodes)
}

/// Decode a graph only after all byte, shape, edge, and numeric checks pass.
/// Unknown versions and partial artifacts fail closed.
pub fn decode(bytes: &[u8]) -> Result<HnswIndex> {
    if bytes.len() > MAX_ARTIFACT_BYTES {
        return Err(invalid("HNSW artifact exceeds byte bound"));
    }
    let mut reader = Reader { bytes, offset: 0 };
    let header = read_header(&mut reader)?;
    let nodes = read_nodes(&mut reader, &header)?;
    if reader.offset != bytes.len() {
        return Err(invalid("HNSW artifact has trailing bytes"));
    }
    let index = HnswIndex {
        dim: header.dim,
        metric: header.metric,
        m: header.m,
        m0: header.m * 2,
        ef_construction: header.ef,
        seed: header.seed,
        ml: 1.0 / (header.m as f64).ln(),
        entry_point: (header.entry != NO_ENTRY).then_some(header.entry as usize),
        max_level: header.max_level,
        nodes,
    };
    validate(&index)?;
    Ok(index)
}

/// Deterministic non-cryptographic integrity digest for the compact header and
/// payload (excluding the digest field itself).
///
/// The durable authority may bind a stronger content digest around this image;
/// this local digest catches truncation and accidental byte corruption before any
/// compact buffers are allocated.
fn compact_digest_parts(header: &[u8], body: &[u8]) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325u64;
    for &byte in header.iter().chain(body) {
        hash ^= byte as u64;
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}

fn compact_append(out: &mut Vec<u8>, bytes: &[u8]) -> Result<()> {
    if out
        .len()
        .checked_add(bytes.len())
        .is_none_or(|length| length > MAX_ARTIFACT_BYTES)
    {
        return Err(invalid("compact HNSW artifact exceeds byte bound"));
    }
    out.extend_from_slice(bytes);
    Ok(())
}

fn compact_append_u32(out: &mut Vec<u8>, value: u32) -> Result<()> {
    compact_append(out, &value.to_le_bytes())
}

fn compact_append_u64(out: &mut Vec<u8>, value: u64) -> Result<()> {
    compact_append(out, &value.to_le_bytes())
}

fn compact_encoded_len(index: &CompactHnswIndex) -> Option<usize> {
    index.encoded_size()
}

/// Encode an immutable HNSW graph with contiguous vectors and CSR-style
/// adjacency.  This is a new opt-in version; [`encode`] remains the v1 format.
pub fn encode_compact(index: &HnswIndex) -> Result<Vec<u8>> {
    let compact = CompactHnswIndex::from_hnsw(index)?;
    encode_compact_image(&compact)
}

/// Encode an opt-in vectorless graph image.  Version 3 has no embedding
/// section; callers must activate it with [`decode_compact_without_vectors`]
/// and query through `CompactHnswIndex::search_with`.
pub fn encode_compact_without_vectors(index: &HnswIndex) -> Result<Vec<u8>> {
    let compact = CompactHnswIndex::from_hnsw_without_vectors(index)?;
    encode_compact_image_with_magic(&compact, VECTORLESS_COMPACT_MAGIC, false)
}

fn encode_compact_image(compact: &CompactHnswIndex) -> Result<Vec<u8>> {
    encode_compact_image_with_magic(compact, COMPACT_MAGIC, true)
}

fn encode_compact_image_with_magic(
    compact: &CompactHnswIndex,
    magic: &[u8; 8],
    stores_vectors: bool,
) -> Result<Vec<u8>> {
    if compact.has_vectors() != stores_vectors {
        return Err(invalid(
            "compact HNSW vector section does not match artifact version",
        ));
    }
    let encoded_len = compact_encoded_len(compact)
        .ok_or_else(|| invalid("compact HNSW artifact length overflows"))?;
    if encoded_len > MAX_ARTIFACT_BYTES {
        return Err(invalid("compact HNSW artifact exceeds byte bound"));
    }
    let body_len = encoded_len
        .checked_sub(COMPACT_HEADER_BYTES)
        .ok_or_else(|| invalid("compact HNSW header exceeds artifact length"))?;
    let node_count =
        u32::try_from(compact.len()).map_err(|_| invalid("compact HNSW node count exceeds u32"))?;
    let total_layers = u32::try_from(compact.adjacency_offsets.len() - 1)
        .map_err(|_| invalid("compact HNSW layer count exceeds u32"))?;
    let edge_count = u32::try_from(compact.neighbors.len())
        .map_err(|_| invalid("compact HNSW edge count exceeds u32"))?;
    let max_level =
        u8::try_from(compact.max_level).map_err(|_| invalid("compact HNSW level exceeds u8"))?;
    let entry = compact.entry_point.map_or(NO_ENTRY, |entry| entry as u32);
    let metric = match compact.metric {
        Metric::L2 => 0,
        Metric::Cosine => 1,
        Metric::InnerProduct => 2,
    };

    let mut out = Vec::with_capacity(encoded_len);
    compact_append(&mut out, magic)?;
    compact_append_u32(
        &mut out,
        u32::try_from(compact.dim).map_err(|_| invalid("compact HNSW dimension exceeds u32"))?,
    )?;
    compact_append(&mut out, &[metric])?;
    compact_append(
        &mut out,
        &u16::try_from(compact.m)
            .map_err(|_| invalid("compact HNSW M exceeds u16"))?
            .to_le_bytes(),
    )?;
    compact_append(
        &mut out,
        &u16::try_from(compact.ef_construction)
            .map_err(|_| invalid("compact HNSW ef exceeds u16"))?
            .to_le_bytes(),
    )?;
    compact_append_u64(&mut out, compact.seed)?;
    compact_append(&mut out, &[max_level])?;
    compact_append_u32(&mut out, entry)?;
    compact_append_u32(&mut out, node_count)?;
    compact_append_u32(&mut out, total_layers)?;
    compact_append_u32(&mut out, edge_count)?;
    compact_append_u64(
        &mut out,
        u64::try_from(body_len).map_err(|_| invalid("compact HNSW body length exceeds u64"))?,
    )?;
    // The digest is filled after the payload is appended.
    compact_append_u64(&mut out, 0)?;

    for id in &compact.ids {
        compact_append_u64(&mut out, *id)?;
    }
    if let Some(vectors) = compact.vectors.as_ref() {
        for value in vectors {
            compact_append(&mut out, &value.to_le_bytes())?;
        }
    }
    for offset in &compact.node_layer_offsets {
        compact_append_u32(&mut out, *offset)?;
    }
    for offset in &compact.adjacency_offsets {
        compact_append_u32(&mut out, *offset)?;
    }
    for neighbor in &compact.neighbors {
        compact_append_u32(&mut out, *neighbor)?;
    }
    debug_assert_eq!(out.len(), encoded_len);

    let digest = compact_digest_parts(&out[..COMPACT_DIGEST_OFFSET], &out[COMPACT_HEADER_BYTES..]);
    out[COMPACT_DIGEST_OFFSET..COMPACT_HEADER_BYTES].copy_from_slice(&digest.to_le_bytes());
    Ok(out)
}

struct CompactHeader {
    dim: usize,
    metric: Metric,
    m: usize,
    ef: usize,
    seed: u64,
    max_level: usize,
    entry: u32,
    node_count: usize,
    total_layers: usize,
    edge_count: usize,
    body_len: usize,
    digest: u64,
}

fn compact_header_bounds(header: &CompactHeader, artifact_len: usize) -> Result<()> {
    if header.dim == 0
        || header.dim > MAX_DIM
        || !(2..=MAX_M).contains(&header.m)
        || !(header.m..=MAX_EF_CONSTRUCTION).contains(&header.ef)
        || header.max_level > MAX_LEVEL_CAP
        || header.node_count > MAX_NODES
    {
        return Err(invalid("compact HNSW header exceeds bounds"));
    }
    let max_layers = header
        .node_count
        .checked_mul(MAX_LEVEL_CAP + 1)
        .ok_or_else(|| invalid("compact HNSW layer bound overflows"))?;
    if header.total_layers > max_layers {
        return Err(invalid("compact HNSW layer count exceeds bounds"));
    }
    let max_edges = header
        .node_count
        .checked_mul(MAX_M * 2 + MAX_LEVEL_CAP * MAX_M)
        .ok_or_else(|| invalid("compact HNSW edge bound overflows"))?;
    if header.edge_count > max_edges {
        return Err(invalid("compact HNSW edge count exceeds bounds"));
    }
    if header.body_len != artifact_len.saturating_sub(COMPACT_HEADER_BYTES) {
        return Err(invalid("compact HNSW body length is invalid"));
    }
    Ok(())
}

fn compact_expected_body_len(header: &CompactHeader, stores_vectors: bool) -> Option<usize> {
    header
        .node_count
        .checked_mul(std::mem::size_of::<u64>())?
        .checked_add(if stores_vectors {
            header
                .node_count
                .checked_mul(header.dim)?
                .checked_mul(std::mem::size_of::<f32>())?
        } else {
            0
        })?
        .checked_add(
            header
                .node_count
                .checked_add(1)?
                .checked_mul(std::mem::size_of::<u32>())?,
        )?
        .checked_add(
            header
                .total_layers
                .checked_add(1)?
                .checked_mul(std::mem::size_of::<u32>())?,
        )?
        .checked_add(header.edge_count.checked_mul(std::mem::size_of::<u32>())?)
}

fn read_compact_header(
    reader: &mut Reader<'_>,
    artifact_len: usize,
    magic: &[u8; 8],
    stores_vectors: bool,
) -> Result<CompactHeader> {
    if reader.take(magic.len())? != magic {
        return Err(invalid("unsupported compact HNSW artifact version"));
    }
    let dim = reader.u32()? as usize;
    let metric = match reader.u8()? {
        0 => Metric::L2,
        1 => Metric::Cosine,
        2 => Metric::InnerProduct,
        _ => return Err(invalid("compact HNSW metric is invalid")),
    };
    let m = reader.u16()? as usize;
    let ef = reader.u16()? as usize;
    let seed = reader.u64()?;
    let max_level = reader.u8()? as usize;
    let entry = reader.u32()?;
    let node_count = reader.u32()? as usize;
    let total_layers = reader.u32()? as usize;
    let edge_count = reader.u32()? as usize;
    let body_len = usize::try_from(reader.u64()?)
        .map_err(|_| invalid("compact HNSW body length exceeds usize"))?;
    let digest = reader.u64()?;
    let header = CompactHeader {
        dim,
        metric,
        m,
        ef,
        seed,
        max_level,
        entry,
        node_count,
        total_layers,
        edge_count,
        body_len,
        digest,
    };
    compact_header_bounds(&header, artifact_len)?;
    let expected = compact_expected_body_len(&header, stores_vectors)
        .ok_or_else(|| invalid("compact HNSW body length overflows"))?;
    if expected != body_len || expected > MAX_ARTIFACT_BYTES - COMPACT_HEADER_BYTES {
        return Err(invalid("compact HNSW body length exceeds bounds"));
    }
    if header.node_count == 0
        && (header.total_layers != 0 || header.edge_count != 0 || header.entry != NO_ENTRY)
    {
        return Err(invalid("empty compact HNSW header has graph state"));
    }
    if header.node_count > 0
        && (header.entry == NO_ENTRY || header.entry as usize >= header.node_count)
    {
        return Err(invalid(
            "nonempty compact HNSW header has no valid entry point",
        ));
    }
    Ok(header)
}

fn read_compact_u64s(reader: &mut Reader<'_>, count: usize) -> Result<Vec<u64>> {
    let mut values = Vec::new();
    values
        .try_reserve_exact(count)
        .map_err(|_| invalid("compact HNSW id allocation failed"))?;
    for _ in 0..count {
        values.push(reader.u64()?);
    }
    Ok(values)
}

fn read_compact_vectors(reader: &mut Reader<'_>, count: usize, dim: usize) -> Result<Vec<f32>> {
    let length = count
        .checked_mul(dim)
        .ok_or_else(|| invalid("compact HNSW vector length overflows"))?;
    let mut values = Vec::new();
    values
        .try_reserve_exact(length)
        .map_err(|_| invalid("compact HNSW vector allocation failed"))?;
    for _ in 0..length {
        values.push(reader.f32()?);
    }
    Ok(values)
}

fn read_compact_u32s(reader: &mut Reader<'_>, count: usize) -> Result<Vec<u32>> {
    let mut values = Vec::new();
    values
        .try_reserve_exact(count)
        .map_err(|_| invalid("compact HNSW offset allocation failed"))?;
    for _ in 0..count {
        values.push(reader.u32()?);
    }
    Ok(values)
}

/// Decode the opt-in compact HNSW image.  The image digest and expected section
/// lengths are checked before any large buffer allocation; structural validation
/// then rejects bad offsets, duplicate/self edges, and invalid vectors.
pub fn decode_compact(bytes: &[u8]) -> Result<CompactHnswIndex> {
    decode_compact_image(bytes, COMPACT_MAGIC, true)
}

/// Decode the explicit vectorless compact image.  This reader never accepts the
/// stored-vector version and never silently falls back to an embedded matrix.
pub fn decode_compact_without_vectors(bytes: &[u8]) -> Result<CompactHnswIndex> {
    decode_compact_image(bytes, VECTORLESS_COMPACT_MAGIC, false)
}

fn decode_compact_image(
    bytes: &[u8],
    magic: &[u8; 8],
    stores_vectors: bool,
) -> Result<CompactHnswIndex> {
    if bytes.len() > MAX_ARTIFACT_BYTES {
        return Err(invalid("compact HNSW artifact exceeds byte bound"));
    }
    if bytes.len() < COMPACT_HEADER_BYTES {
        return Err(invalid("compact HNSW artifact is truncated"));
    }
    let mut reader = Reader { bytes, offset: 0 };
    let header = read_compact_header(&mut reader, bytes.len(), magic, stores_vectors)?;
    let actual_digest = compact_digest_parts(
        &bytes[..COMPACT_DIGEST_OFFSET],
        &bytes[COMPACT_HEADER_BYTES..],
    );
    if actual_digest != header.digest {
        return Err(invalid("compact HNSW payload digest mismatch"));
    }

    let ids = read_compact_u64s(&mut reader, header.node_count)?;
    let vectors = if stores_vectors {
        Some(read_compact_vectors(
            &mut reader,
            header.node_count,
            header.dim,
        )?)
    } else {
        None
    };
    let node_layer_offsets = read_compact_u32s(&mut reader, header.node_count + 1)?;
    let adjacency_offsets = read_compact_u32s(&mut reader, header.total_layers + 1)?;
    let neighbors = read_compact_u32s(&mut reader, header.edge_count)?;
    if reader.offset != bytes.len() {
        return Err(invalid("compact HNSW artifact has trailing bytes"));
    }

    let entry_point = (header.entry != NO_ENTRY).then_some(header.entry as usize);
    let index = CompactHnswIndex {
        dim: header.dim,
        metric: header.metric,
        m: header.m,
        m0: header.m * 2,
        ef_construction: header.ef,
        seed: header.seed,
        max_level: header.max_level,
        entry_point,
        ids,
        vectors,
        node_layer_offsets,
        adjacency_offsets,
        neighbors,
    };
    index.validate()?;
    Ok(index)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hnsw::compact::{CompactSearchError, SearchBudget, VectorProvider};

    struct FixtureProvider {
        vectors: Vec<Vec<f32>>,
        calls: usize,
        mode: ProviderMode,
    }

    #[derive(Clone, Copy)]
    enum ProviderMode {
        Good,
        Missing,
        WrongDimension,
        NonFinite,
    }

    impl VectorProvider for FixtureProvider {
        fn fetch(&mut self, node: usize, _external_id: u64) -> std::io::Result<Vec<f32>> {
            self.calls += 1;
            match self.mode {
                ProviderMode::Good => Ok(self.vectors[node].clone()),
                ProviderMode::Missing => Err(Error::new(ErrorKind::NotFound, "vector missing")),
                ProviderMode::WrongDimension => Ok(vec![0.0]),
                ProviderMode::NonFinite => Ok(vec![f32::NAN, 0.0]),
            }
        }
    }

    fn graph() -> HnswIndex {
        let mut index = HnswIndex::new(2, Metric::L2, 4, 16, 7);
        for id in 0..12 {
            index.insert(id, vec![id as f32, (id % 3) as f32]);
        }
        index
    }

    #[test]
    fn no_rebuild_round_trip_preserves_search() {
        let index = graph();
        let encoded = encode(&index).unwrap();
        let restored = decode(&encoded).unwrap();
        assert_eq!(restored.len(), index.len());
        assert_eq!(encode(&restored).unwrap(), encoded);
        assert_eq!(
            restored.search(&[4.0, 1.0], 5, 16),
            index.search(&[4.0, 1.0], 5, 16)
        );
    }

    #[test]
    fn rejects_version_truncation_trailing_bytes_and_bogus_counts() {
        let bytes = encode(&graph()).unwrap();
        let mut bad = bytes.clone();
        bad[7] = 2;
        assert!(decode(&bad).is_err());
        assert!(decode(&bytes[..bytes.len() - 1]).is_err());
        bad = bytes.clone();
        bad.push(0);
        assert!(decode(&bad).is_err());
        bad = bytes.clone();
        bad[30..34].copy_from_slice(&u32::MAX.to_le_bytes());
        assert!(decode(&bad).is_err());
    }

    #[test]
    fn rejects_invalid_edges_and_nonfinite_vectors() {
        let mut index = graph();
        let node_count = index.nodes.len();
        index.nodes[0].neighbors[0] = vec![node_count];
        assert!(encode(&index).is_err());
        let mut bytes = encode(&graph()).unwrap();
        // First node's first coordinate begins after the 34-byte header and id/level.
        bytes[43..47].copy_from_slice(&f32::NAN.to_le_bytes());
        assert!(decode(&bytes).is_err());

        let mut bytes = encode(&graph()).unwrap();
        // The first layer's degree and first edge follow the two coordinates.
        bytes[51..53].copy_from_slice(&u16::MAX.to_le_bytes());
        assert!(decode(&bytes).is_err());

        let mut bytes = encode(&graph()).unwrap();
        assert!(u16::from_le_bytes(bytes[51..53].try_into().unwrap()) > 0);
        bytes[53..57].copy_from_slice(&0_u32.to_le_bytes()); // self edge
        assert!(decode(&bytes).is_err());
    }

    // spec: MEDULLA-FEDERATED-QUERY-R040.5.2
    #[test]
    fn compact_round_trip_preserves_search_and_accounting() {
        let index = graph();
        let encoded = encode_compact(&index).unwrap();
        let restored = decode_compact(&encoded).unwrap();
        assert_eq!(restored.len(), index.len());
        assert!(restored.edge_count() > 0);
        assert_eq!(restored.encoded_size(), Some(encoded.len()));
        assert!(restored.byte_size() > 0);
        assert_eq!(encode_compact_image(&restored).unwrap(), encoded);
        for query in [[0.0, 0.0], [4.0, 1.0], [11.0, 2.0]] {
            assert_eq!(
                restored.search(&query, 5, 16),
                index.search(&query, 5, 16),
                "compact search must preserve HNSW traversal for query {query:?}"
            );
        }
        // Version 1 remains a distinct reader; callers must opt into the compact
        // path explicitly rather than changing the historical default.
        assert!(decode(&encoded).is_err());
    }

    // spec: MEDULLA-FEDERATED-QUERY-R040.5.2
    #[test]
    fn compact_rejects_version_digest_offsets_and_truncation() {
        let index = graph();
        let encoded = encode_compact(&index).unwrap();
        let mut bad = encoded.clone();
        bad[7] = 1;
        assert!(decode_compact(&bad).is_err());

        bad = encoded.clone();
        bad[COMPACT_DIGEST_OFFSET] ^= 1;
        assert!(decode_compact(&bad).is_err());

        bad = encoded.clone();
        bad[17] ^= 1; // seed is covered by the image digest, even though shape is unchanged.
        assert!(decode_compact(&bad).is_err());

        assert!(decode_compact(&encoded[..encoded.len() - 1]).is_err());

        // Keep the payload digest valid so this exercises structural offset
        // checks after the pre-allocation digest gate.
        bad = encoded.clone();
        let offset_start = COMPACT_HEADER_BYTES + index.len() * 8 + index.len() * index.dim * 4;
        bad[offset_start + 4..offset_start + 8].copy_from_slice(&u32::MAX.to_le_bytes());
        let digest =
            compact_digest_parts(&bad[..COMPACT_DIGEST_OFFSET], &bad[COMPACT_HEADER_BYTES..]);
        bad[COMPACT_DIGEST_OFFSET..COMPACT_HEADER_BYTES].copy_from_slice(&digest.to_le_bytes());
        assert!(decode_compact(&bad).is_err());

        // A duplicate canonical id is also rejected after the digest passes.
        bad = encoded;
        bad[COMPACT_HEADER_BYTES..COMPACT_HEADER_BYTES + 8].copy_from_slice(&1_u64.to_le_bytes());
        let digest =
            compact_digest_parts(&bad[..COMPACT_DIGEST_OFFSET], &bad[COMPACT_HEADER_BYTES..]);
        bad[COMPACT_DIGEST_OFFSET..COMPACT_HEADER_BYTES].copy_from_slice(&digest.to_le_bytes());
        assert!(decode_compact(&bad).is_err());
    }

    #[test]
    fn compact_empty_image_round_trips_without_entry_point() {
        let index = HnswIndex::new(3, Metric::Cosine, 4, 16, 11);
        let encoded = encode_compact(&index).unwrap();
        let restored = decode_compact(&encoded).unwrap();
        assert!(restored.is_empty());
        assert_eq!(restored.search(&[0.0, 0.0, 0.0], 3, 8), Vec::new());
    }

    // spec: MEDULLA-FEDERATED-QUERY-R040.5.3
    #[test]
    fn vectorless_round_trip_searches_through_bounded_provider() {
        let index = graph();
        let encoded = encode_compact_without_vectors(&index).unwrap();
        let restored = decode_compact_without_vectors(&encoded).unwrap();
        assert!(!restored.has_vectors());
        assert!(encoded.len() < encode_compact(&index).unwrap().len());
        assert!(decode_compact(&encoded).is_err());
        assert!(decode_compact_without_vectors(&encode_compact(&index).unwrap()).is_err());
        assert_eq!(
            encode_compact_image_with_magic(&restored, VECTORLESS_COMPACT_MAGIC, false).unwrap(),
            encoded
        );
        let mut corrupt = encoded.clone();
        let offset_start = COMPACT_HEADER_BYTES + index.len() * 8;
        corrupt[offset_start + 4..offset_start + 8].copy_from_slice(&u32::MAX.to_le_bytes());
        let digest = compact_digest_parts(
            &corrupt[..COMPACT_DIGEST_OFFSET],
            &corrupt[COMPACT_HEADER_BYTES..],
        );
        corrupt[COMPACT_DIGEST_OFFSET..COMPACT_HEADER_BYTES].copy_from_slice(&digest.to_le_bytes());
        assert!(decode_compact_without_vectors(&corrupt).is_err());
        let mut provider = FixtureProvider {
            vectors: (0..index.len())
                .map(|id| vec![id as f32, (id % 3) as f32])
                .collect(),
            calls: 0,
            mode: ProviderMode::Good,
        };
        let (actual, stats) = restored
            .search_with(
                &[4.0, 1.0],
                5,
                16,
                SearchBudget {
                    max_vector_fetches: 256,
                },
                &mut provider,
            )
            .unwrap();
        assert_eq!(actual, index.search(&[4.0, 1.0], 5, 16));
        assert_eq!(stats.fetched_vectors, provider.calls);
        assert!(stats.fetched_vectors <= 256);
    }

    // spec: MEDULLA-FEDERATED-QUERY-R040.5.3
    #[test]
    fn vectorless_search_rejects_budget_before_provider_call() {
        let index = graph();
        let encoded = encode_compact_without_vectors(&index).unwrap();
        let restored = decode_compact_without_vectors(&encoded).unwrap();
        let mut provider = FixtureProvider {
            vectors: Vec::new(),
            calls: 0,
            mode: ProviderMode::Good,
        };
        let error = restored
            .search_with(
                &[4.0, 1.0],
                5,
                16,
                SearchBudget {
                    max_vector_fetches: 0,
                },
                &mut provider,
            )
            .unwrap_err();
        assert!(matches!(
            error,
            CompactSearchError::VectorFetchBudgetExceeded {
                max_vector_fetches: 0
            }
        ));
        assert_eq!(provider.calls, 0);
    }

    // spec: MEDULLA-FEDERATED-QUERY-R040.5.3
    #[test]
    fn compact_search_bounds_huge_inputs_before_allocation_or_fetch() {
        let mut index = graph();
        for node in &mut index.nodes {
            node.neighbors.truncate(1);
        }
        index.max_level = 0;
        index.entry_point = Some(0);
        let compact = CompactHnswIndex::from_hnsw(&index).unwrap();
        assert_eq!(
            compact.search(&[4.0, 1.0], usize::MAX, usize::MAX),
            compact.search(&[4.0, 1.0], index.len(), index.len())
        );

        let vectorless = CompactHnswIndex::from_hnsw_without_vectors(&index).unwrap();
        for limit in [0, 1] {
            let mut provider = FixtureProvider {
                vectors: index.nodes.iter().map(|node| node.vector.clone()).collect(),
                calls: 0,
                mode: ProviderMode::Good,
            };
            assert!(matches!(
                vectorless.search_with(
                    &[4.0, 1.0],
                    usize::MAX,
                    usize::MAX,
                    SearchBudget {
                        max_vector_fetches: limit,
                    },
                    &mut provider,
                ),
                Err(CompactSearchError::VectorFetchBudgetExceeded { .. })
            ));
            assert_eq!(provider.calls, limit);
        }
    }

    // spec: MEDULLA-FEDERATED-QUERY-R040.5.3
    #[test]
    fn vectorless_search_validates_provider_vectors_and_queries() {
        let index = graph();
        let encoded = encode_compact_without_vectors(&index).unwrap();
        let restored = decode_compact_without_vectors(&encoded).unwrap();

        let mut wrong_dimension = FixtureProvider {
            vectors: Vec::new(),
            calls: 0,
            mode: ProviderMode::WrongDimension,
        };
        assert!(matches!(
            restored.search_with(
                &[4.0, 1.0],
                5,
                16,
                SearchBudget {
                    max_vector_fetches: 8,
                },
                &mut wrong_dimension,
            ),
            Err(CompactSearchError::VectorDimension { .. })
        ));

        let mut nonfinite = FixtureProvider {
            vectors: Vec::new(),
            calls: 0,
            mode: ProviderMode::NonFinite,
        };
        assert!(matches!(
            restored.search_with(
                &[4.0, 1.0],
                5,
                16,
                SearchBudget {
                    max_vector_fetches: 8,
                },
                &mut nonfinite,
            ),
            Err(CompactSearchError::VectorNonFinite { .. })
        ));

        let mut missing = FixtureProvider {
            vectors: Vec::new(),
            calls: 0,
            mode: ProviderMode::Missing,
        };
        assert!(matches!(
            restored.search_with(
                &[4.0, 1.0],
                5,
                16,
                SearchBudget {
                    max_vector_fetches: 8,
                },
                &mut missing,
            ),
            Err(CompactSearchError::MissingVector { .. })
        ));

        let mut good = FixtureProvider {
            vectors: Vec::new(),
            calls: 0,
            mode: ProviderMode::Good,
        };
        assert!(matches!(
            restored.search_with(
                &[4.0],
                5,
                16,
                SearchBudget {
                    max_vector_fetches: 8,
                },
                &mut good,
            ),
            Err(CompactSearchError::QueryDimension { .. })
        ));
        assert_eq!(good.calls, 0);

        let mut nonfinite_query = FixtureProvider {
            vectors: Vec::new(),
            calls: 0,
            mode: ProviderMode::Good,
        };
        assert!(matches!(
            restored.search_with(
                &[f32::NAN, 1.0],
                5,
                16,
                SearchBudget {
                    max_vector_fetches: 8,
                },
                &mut nonfinite_query,
            ),
            Err(CompactSearchError::QueryNonFinite { coordinate: 0 })
        ));
        assert_eq!(nonfinite_query.calls, 0);
    }
}
