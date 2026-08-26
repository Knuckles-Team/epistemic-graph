//! Payload-size and encode/decode-time comparison: the binary tile wire form
//! ([`eg_viz_graph_tiles::wire`]) vs. a plain JSON encoding of the SAME
//! [`eg_viz_graph_tiles::ClusterExpansion`] values, at 1k / 10k / 100k nodes.
//! This is the measurement the whole lane exists to produce -- run with:
//!
//! ```text
//! cargo run -p eg-viz-graph-tiles --example bench_tile_wire --target-dir ./target-isolated
//! ```
//!
//! Each scale builds a `ClusterExpansion` with `node_count` nodes and
//! `3 * node_count` edges (a representative sparse-graph edge:node ratio),
//! drawn from a small dictionary of node/edge type strings -- the case the
//! binary format's dictionary encoding is specifically for, and the case a
//! flat JSON array of `{src_idx, dst_idx, type: "knows"}` objects pays for
//! on every single edge.

use std::time::Instant;

use eg_viz_graph_tiles::{decode_cluster_expansion, encode_cluster_expansion};
use eg_viz_graph_tiles::{ClusterExpansion, TileEdge, TileNode};

const NODE_TYPES: [&str; 4] = ["Person", "Organization", "Document", "Event"];
const EDGE_TYPES: [&str; 3] = ["relatesTo", "knows", "mentions"];

fn build_expansion(node_count: usize) -> ClusterExpansion {
    let edge_count = node_count * 3;
    let nodes: Vec<TileNode> = (0..node_count)
        .map(|i| TileNode {
            id: format!("n:{i:08x}-{i:08x}-{i:08x}"), // realistic-length opaque id, not a bare int
            label: format!("Node label {i}"),
            node_type: NODE_TYPES[i % NODE_TYPES.len()].to_string(),
            pos: Some(((i % 997) as f32 / 997.0, (i % 991) as f32 / 991.0)),
        })
        .collect();
    let edges: Vec<TileEdge> = (0..edge_count)
        .map(|e| TileEdge {
            src_idx: (e % node_count) as u32,
            dst_idx: ((e * 7 + 3) % node_count) as u32,
            edge_type: EDGE_TYPES[e % EDGE_TYPES.len()].to_string(),
        })
        .collect();
    ClusterExpansion {
        cluster_id: 1,
        nodes,
        edges,
        child_clusters: Vec::new(),
    }
}

fn median_secs(mut samples: Vec<f64>) -> f64 {
    samples.sort_by(|a, b| a.partial_cmp(b).unwrap());
    samples[samples.len() / 2]
}

fn main() {
    println!(
        "{:>10} | {:>14} | {:>14} | {:>8} | {:>12} | {:>12} | {:>12} | {:>12}",
        "nodes",
        "binary bytes",
        "json bytes",
        "ratio",
        "bin encode",
        "json encode",
        "bin decode",
        "json decode"
    );
    println!("{}", "-".repeat(112));

    for &node_count in &[1_000usize, 10_000, 100_000] {
        let expansion = build_expansion(node_count);
        const REPS: usize = 5;

        let mut bin_encode_secs = Vec::with_capacity(REPS);
        let mut binary = Vec::new();
        for _ in 0..REPS {
            let start = Instant::now();
            binary = encode_cluster_expansion(&expansion).expect("encode");
            bin_encode_secs.push(start.elapsed().as_secs_f64());
        }

        let mut json_encode_secs = Vec::with_capacity(REPS);
        let mut json = Vec::new();
        for _ in 0..REPS {
            let start = Instant::now();
            json = serde_json::to_vec(&expansion).expect("json encode");
            json_encode_secs.push(start.elapsed().as_secs_f64());
        }

        let mut bin_decode_secs = Vec::with_capacity(REPS);
        for _ in 0..REPS {
            let start = Instant::now();
            let decoded = decode_cluster_expansion(&binary).expect("decode");
            bin_decode_secs.push(start.elapsed().as_secs_f64());
            assert_eq!(decoded.nodes.len(), expansion.nodes.len());
        }

        let mut json_decode_secs = Vec::with_capacity(REPS);
        for _ in 0..REPS {
            let start = Instant::now();
            let decoded: ClusterExpansion = serde_json::from_slice(&json).expect("json decode");
            json_decode_secs.push(start.elapsed().as_secs_f64());
            assert_eq!(decoded.nodes.len(), expansion.nodes.len());
        }

        let ratio = json.len() as f64 / binary.len() as f64;
        println!(
            "{:>10} | {:>14} | {:>14} | {:>7.2}x | {:>10.3}ms | {:>10.3}ms | {:>10.3}ms | {:>10.3}ms",
            node_count,
            binary.len(),
            json.len(),
            ratio,
            median_secs(bin_encode_secs) * 1e3,
            median_secs(json_encode_secs) * 1e3,
            median_secs(bin_decode_secs) * 1e3,
            median_secs(json_decode_secs) * 1e3,
        );
    }
}
