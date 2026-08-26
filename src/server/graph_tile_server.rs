//! VIZ-2: the binary tile/streaming protocol for GRAPH payloads, feature
//! `viz-graph-tiles`. Serves `clusters(graph, level, parent?)`/`expand(graph,
//! cluster_id)` (the shared VIZ-1/VIZ-2/VIZ-3 contract, see
//! `eg_viz_graph_tiles::contract`) over the SAME loopback-only, dependency-free
//! HTTP/1.1 listener `viz_interactive` already runs (implies `viz-interactive`
//! — see the root `Cargo.toml`'s `viz-graph-tiles` feature comment), as three
//! new routes:
//!
//! - `GET /graph_tile/clusters?level=&parent=` — one [`ClusterLevel`] tile,
//!   binary-encoded, as the whole response body (mirrors `/tile`'s "one tile,
//!   one response" shape).
//! - `GET /graph_tile/expand?cluster_id=` — one [`ClusterExpansion`] tile,
//!   same shape.
//! - `GET /graph_tile/stream?level=&top_k=` — the PROGRESSIVE path: writes the
//!   level's [`ClusterLevel`] summary tile first, flushes it to the socket,
//!   then computes and flushes an [`ClusterExpansion`] tile for each of the
//!   `top_k` largest clusters (by `node_count`) in turn, and finally a
//!   [`eg_viz_graph_tiles::TileKind::StreamEnd`] sentinel — using genuine HTTP
//!   chunked transfer encoding (`Transfer-Encoding: chunked`), so a client
//!   reading the response body as a stream (`fetch()` + `ReadableStream`, or
//!   any HTTP/1.1 client) can decode and render the first tile the moment its
//!   bytes land, before the server has even started computing the next one —
//!   this is the "render a first frame before the whole graph arrives"
//!   requirement, proven over the real wire, not just a format that could
//!   support it.
//!
//! ## Data source
//!
//! VIZ-1's real GraphCore-backed hierarchical clustering has not merged yet
//! (see `eg_viz_graph_tiles::demo`'s doc) — this module serves a deterministic
//! [`DemoGraph`], built fresh per request from query-param-controlled
//! [`DemoParams`] (`node_count`/`edge_count`/`seed`/`top_clusters`/
//! `sub_clusters_per_top`, all clamped to `eg_viz_graph_tiles::demo`'s bounds).
//! This is the SAME "engine-side generated, clearly labeled, capped" idiom
//! `VizDatasetSource::SyntheticGraph`/`SyntheticScatterClusters` already use in
//! production. Swapping in real data means constructing a real
//! `impl GraphSource` here instead of a `DemoGraph` — the routes, the wire
//! encoding, and the streaming behavior below do not change.

use std::collections::HashMap;

use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;

use eg_viz_graph_tiles::demo::{DemoGraph, DemoParams};
use eg_viz_graph_tiles::{
    encode_cluster_expansion, encode_cluster_level, write_frame, write_stream_end, GraphSource,
};

use super::viz_interactive::{parse_param, parse_query};

const GRAPH_TILE_CONTENT_TYPE: &str = "application/octet-stream";
/// Bound on `top_k` for `/graph_tile/stream` — a request-forgeable knob, so it
/// is capped the same way every other caller-controlled fan-out in this
/// codebase is (see `MAX_TILE_ROWS`/`MAX_TOP_TYPES_PER_CLUSTER`).
const MAX_STREAM_TOP_K: usize = 64;

/// The three routes this module owns. [`match_route`] is the single place
/// that decides whether a request path belongs here.
enum GraphTileRoute {
    Clusters,
    Expand,
    Stream,
}

fn match_route(path: &str) -> Option<GraphTileRoute> {
    match path {
        "/graph_tile/clusters" => Some(GraphTileRoute::Clusters),
        "/graph_tile/expand" => Some(GraphTileRoute::Expand),
        "/graph_tile/stream" => Some(GraphTileRoute::Stream),
        _ => None,
    }
}

/// Whether `request_target` (the raw HTTP request-line target, query string
/// included) names one of this module's routes — the entry point
/// `viz_interactive::serve` checks before falling back to its own synchronous
/// `route()` dispatch, since these handlers need `async` socket writes for
/// genuine chunked streaming that a plain `(status, content_type, Vec<u8>)`
/// return value cannot express.
pub fn handles(request_target: &str) -> bool {
    let (path, _) = parse_query(request_target);
    match_route(path).is_some()
}

fn demo_params_from_query(params: &HashMap<String, String>) -> DemoParams {
    let defaults = DemoParams::default();
    DemoParams {
        node_count: parse_param(params, "node_count").unwrap_or(defaults.node_count),
        edge_count: parse_param(params, "edge_count").unwrap_or(defaults.edge_count),
        seed: parse_param(params, "seed").unwrap_or(defaults.seed),
        top_clusters: parse_param(params, "top_clusters").unwrap_or(defaults.top_clusters),
        sub_clusters_per_top: parse_param(params, "sub_clusters_per_top")
            .unwrap_or(defaults.sub_clusters_per_top),
    }
    .clamped()
}

/// Write the response head for a chunked-transfer-encoded response (the
/// `/graph_tile/stream` route -- the two single-tile routes use
/// [`write_fixed_body`]'s plain `content-length` head instead).
async fn write_chunked_head(stream: &mut TcpStream, status_line: &str) -> std::io::Result<()> {
    let head = format!(
        "HTTP/1.1 {status_line}\r\ncontent-type: {GRAPH_TILE_CONTENT_TYPE}\r\ncache-control: no-store\r\nconnection: close\r\ntransfer-encoding: chunked\r\n\r\n"
    );
    stream.write_all(head.as_bytes()).await
}

async fn write_fixed_body(stream: &mut TcpStream, status_line: &str, body: &[u8]) {
    let head = format!(
        "HTTP/1.1 {status_line}\r\ncontent-type: {GRAPH_TILE_CONTENT_TYPE}\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
        body.len()
    );
    let _ = stream.write_all(head.as_bytes()).await;
    let _ = stream.write_all(body).await;
    let _ = stream.shutdown().await;
}

async fn write_json_error(stream: &mut TcpStream, status_line: &str, message: &str) {
    // Build the body FIRST and measure its real byte length -- `{:?}` on a
    // `&str` escapes quotes/backslashes, so a fixed "message.len() + N"
    // guess is wrong whenever `message` needs escaping, which would send a
    // `content-length` that does not match the bytes actually written.
    let body = format!("{{\"error\":{message:?}}}");
    let head = format!(
        "HTTP/1.1 {status_line}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
        body.len()
    );
    let _ = stream.write_all(head.as_bytes()).await;
    let _ = stream.write_all(body.as_bytes()).await;
    let _ = stream.shutdown().await;
}

/// Write one chunk of an HTTP/1.1 chunked-transfer-encoded body (RFC 7230
/// §4.1: hex length, CRLF, data, CRLF) and flush immediately -- the flush is
/// what actually pushes these bytes onto the wire NOW rather than letting the
/// OS buffer them behind whatever tile this handler computes next, which is
/// the entire point of chunking here.
async fn write_chunk(stream: &mut TcpStream, data: &[u8]) -> std::io::Result<()> {
    stream
        .write_all(format!("{:x}\r\n", data.len()).as_bytes())
        .await?;
    stream.write_all(data).await?;
    stream.write_all(b"\r\n").await?;
    stream.flush().await
}

async fn end_chunked(stream: &mut TcpStream) {
    let _ = stream.write_all(b"0\r\n\r\n").await;
    let _ = stream.shutdown().await;
}

/// Handle one request already routed here by [`handles`]. Never panics on a
/// malformed query -- every missing/invalid param is a `400`, matching
/// `viz_interactive::resolve_tile`'s own contract.
pub async fn serve(stream: &mut TcpStream, method: &str, target: &str) {
    if method != "GET" {
        write_fixed_body(stream, "405 Method Not Allowed", b"GET only").await;
        return;
    }
    let (path, params) = parse_query(target);
    let Some(route) = match_route(path) else {
        write_fixed_body(stream, "404 Not Found", b"not found").await;
        return;
    };

    match route {
        GraphTileRoute::Clusters => {
            let level: u32 = parse_param(&params, "level").unwrap_or(0);
            let parent: Option<u64> = parse_param(&params, "parent");
            let graph = DemoGraph::build(demo_params_from_query(&params));
            let response = graph.clusters(level, parent);
            match encode_cluster_level(&response) {
                Ok(bytes) => write_fixed_body(stream, "200 OK", &bytes).await,
                Err(err) => {
                    write_json_error(stream, "500 Internal Server Error", &err.to_string()).await
                }
            }
        }
        GraphTileRoute::Expand => {
            let Some(cluster_id) = parse_param::<u64>(&params, "cluster_id") else {
                write_json_error(
                    stream,
                    "400 Bad Request",
                    "missing required query param `cluster_id`",
                )
                .await;
                return;
            };
            let graph = DemoGraph::build(demo_params_from_query(&params));
            let response = graph.expand(cluster_id);
            match encode_cluster_expansion(&response) {
                Ok(bytes) => write_fixed_body(stream, "200 OK", &bytes).await,
                Err(err) => {
                    write_json_error(stream, "500 Internal Server Error", &err.to_string()).await
                }
            }
        }
        GraphTileRoute::Stream => {
            let level: u32 = parse_param(&params, "level").unwrap_or(0);
            let top_k: usize = parse_param(&params, "top_k")
                .unwrap_or(3usize)
                .min(MAX_STREAM_TOP_K);
            let graph = DemoGraph::build(demo_params_from_query(&params));

            if write_chunked_head(stream, "200 OK").await.is_err() {
                return;
            }
            let mut frames_sent = 0u32;

            let level_response = graph.clusters(level, None);
            let mut ranked: Vec<_> = level_response.clusters.iter().collect();
            ranked.sort_by_key(|c| std::cmp::Reverse(c.node_count));
            let top_ids: Vec<u64> = ranked.iter().take(top_k).map(|c| c.id).collect();

            if let Ok(bytes) = encode_cluster_level(&level_response) {
                let mut framed = Vec::with_capacity(bytes.len() + 4);
                write_frame(&mut framed, &bytes);
                if write_chunk(stream, &framed).await.is_err() {
                    return;
                }
                frames_sent += 1;
            }

            for cluster_id in top_ids {
                let expansion = graph.expand(cluster_id);
                if let Ok(bytes) = encode_cluster_expansion(&expansion) {
                    let mut framed = Vec::with_capacity(bytes.len() + 4);
                    write_frame(&mut framed, &bytes);
                    if write_chunk(stream, &framed).await.is_err() {
                        return;
                    }
                    frames_sent += 1;
                }
            }

            let mut end_frame = Vec::new();
            write_stream_end(&mut end_frame, frames_sent);
            if write_chunk(stream, &end_frame).await.is_ok() {
                end_chunked(stream).await;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn handles_recognizes_exactly_the_three_routes() {
        assert!(handles("/graph_tile/clusters?level=0"));
        assert!(handles("/graph_tile/expand?cluster_id=1"));
        assert!(handles("/graph_tile/stream?top_k=3"));
        assert!(!handles("/tile?dataset_ref=x"));
        assert!(!handles("/graph_tile/unknown"));
        assert!(!handles("/"));
    }

    #[test]
    fn demo_params_from_query_falls_back_to_defaults_and_clamps() {
        let mut params = HashMap::new();
        params.insert("node_count".to_string(), "50".to_string());
        let built = demo_params_from_query(&params);
        assert_eq!(built.node_count, 50);
        assert_eq!(built.seed, DemoParams::default().seed);

        let mut over = HashMap::new();
        over.insert(
            "node_count".to_string(),
            (eg_viz_graph_tiles::demo::MAX_DEMO_NODE_COUNT + 1).to_string(),
        );
        let clamped = demo_params_from_query(&over);
        assert_eq!(
            clamped.node_count,
            eg_viz_graph_tiles::demo::MAX_DEMO_NODE_COUNT
        );
    }
}
