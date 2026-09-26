//! Versioned, bounded HNSW graph image. The caller owns durable storage and
//! activation; this module opens no files and never repairs a corrupt graph.

use super::{HnswIndex, Node, MAX_LEVEL_CAP};
use crate::flat::Metric;
use std::collections::HashSet;
use std::io::{Error, ErrorKind, Result};

const MAGIC: &[u8; 8] = b"EGHNSW\0\x01";
/// Maximum encoded graph size accepted for serving.
pub const MAX_ARTIFACT_BYTES: usize = 256 * 1024 * 1024;
pub const MAX_DIM: usize = 4096;
pub const MAX_NODES: usize = 250_000;
pub const MAX_M: usize = 128;
pub const MAX_EF_CONSTRUCTION: usize = 4096;
const MAX_COORDINATE: f32 = 1.0e15;
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

fn validate(index: &HnswIndex) -> Result<()> {
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

#[cfg(test)]
mod tests {
    use super::*;

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
}
