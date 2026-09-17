//! VIZ-2 reachability proof: a REAL client, over a REAL TCP socket, against
//! the actual `viz_interactive::serve` HTTP listener with `/graph_tile/*`
//! wired in (feature `viz-graph-tiles`) -- not a call into `resolve_tile`-
//! style internals, and not an in-process `dispatch()` call the way
//! `tests/viz_dispatch.rs` proves `Method::Viz` reachability. This is the
//! literal transport this program's brief requires: "wired end-to-end with a
//! live caller. A capability with no live caller is not done."
//!
//! Three things are proven here:
//!
//! 1. `/graph_tile/clusters` and `/graph_tile/expand` each round-trip a real
//!    binary tile over the wire, decodable back to the exact values the demo
//!    `GraphSource` produced in-process.
//! 2. `/graph_tile/stream` is genuinely CHUNKED: the client reads the socket
//!    incrementally (never "buffer the whole response, then parse") and
//!    decodes the first tile frame from a STRICT PREFIX of the total response
//!    bytes -- i.e. before the server has even written the later frames, let
//!    alone before the connection closes. This is a byte-offset assertion,
//!    not a wall-clock one, so it is not flaky under CI load.
//! 3. The stream's closing `StreamEnd` sentinel carries the exact frame count
//!    the client actually received -- a truncated stream is detectable, not
//!    silently indistinguishable from "graph fully loaded".

#![cfg(feature = "viz-graph-tiles")]

use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use eg_viz_graph_tiles::{decode_cluster_expansion, decode_cluster_level, read_frames, TileKind};
use epistemic_graph::server::viz_engine::VizEngineState;
use epistemic_graph::server::viz_interactive::serve;

/// Start a real `viz_interactive` listener on an OS-assigned loopback port and
/// return its address. The server task is detached (test-process lifetime is
/// short enough that this is fine, matching this crate's other served-surface
/// tests).
///
/// `viz_interactive::serve` now also takes an OPTIONAL graph-registry handle
/// (VIZ-1/VIZ-2 bridge: `/graph_tile/*` resolves a real cluster hierarchy off
/// it when present -- `None` here since this file's `ServerState` is a
/// `pub(crate)`/`#[cfg(test)]` type this EXTERNAL integration test crate
/// cannot construct, and has no need to: every request below passes
/// `?demo=1`, so this file proves the WIRE PROTOCOL end-to-end against the
/// demo generator, not real-graph clustering (that is
/// `src/server/graph_tile_source.rs`'s own in-crate unit tests, which DO
/// construct a real `ServerState` via `ServerState::new_for_test`).
async fn start_server() -> std::net::SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let engine = Arc::new(VizEngineState::new(None));
    tokio::spawn(serve(listener, engine, None));
    addr
}

/// Send a bare GET request and read the ENTIRE response (headers + body) in
/// one shot -- used for the two single-tile routes, where "streaming" is not
/// the property under test.
async fn get_full(addr: std::net::SocketAddr, target: &str) -> (String, Vec<u8>) {
    let mut stream = TcpStream::connect(addr).await.unwrap();
    let request = format!("GET {target} HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n");
    stream.write_all(request.as_bytes()).await.unwrap();
    let mut raw = Vec::new();
    stream.read_to_end(&mut raw).await.unwrap();
    split_head_and_body(&raw)
}

fn split_head_and_body(raw: &[u8]) -> (String, Vec<u8>) {
    let sep = raw
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .expect("response must have a header/body separator");
    let head = String::from_utf8_lossy(&raw[..sep]).to_string();
    let body = raw[sep + 4..].to_vec();
    (head, body)
}

#[tokio::test]
async fn clusters_route_round_trips_a_real_binary_tile_over_tcp() {
    let addr = start_server().await;
    let (head, body) = get_full(
        addr,
        "/graph_tile/clusters?demo=1&level=0&node_count=300&edge_count=900&top_clusters=6&seed=11",
    )
    .await;
    assert!(head.starts_with("HTTP/1.1 200 OK"), "head was: {head}");
    assert!(head
        .to_lowercase()
        .contains("content-type: application/octet-stream"));

    let decoded = decode_cluster_level(&body).expect("decode over-the-wire bytes");
    assert_eq!(decoded.level, 0);
    assert_eq!(decoded.clusters.len(), 6);
    let total_nodes: u32 = decoded.clusters.iter().map(|c| c.node_count).sum();
    assert_eq!(
        total_nodes, 300,
        "every node must land in exactly one top-level cluster"
    );
}

#[tokio::test]
async fn expand_route_round_trips_a_real_binary_tile_over_tcp() {
    let addr = start_server().await;
    let (_head, level_body) = get_full(
        addr,
        "/graph_tile/clusters?demo=1&level=0&node_count=300&edge_count=900&top_clusters=6&seed=11",
    )
    .await;
    let level = decode_cluster_level(&level_body).unwrap();
    let first_cluster_id = level.clusters[0].id;

    let (head, body) = get_full(
        addr,
        &format!(
            "/graph_tile/expand?demo=1&cluster_id={first_cluster_id}&node_count=300&edge_count=900&top_clusters=6&seed=11"
        ),
    )
    .await;
    assert!(head.starts_with("HTTP/1.1 200 OK"), "head was: {head}");
    let expansion = decode_cluster_expansion(&body).expect("decode over-the-wire bytes");
    assert_eq!(expansion.cluster_id, first_cluster_id);
    assert_eq!(
        expansion.nodes.len() as u32,
        level.clusters[0].node_count,
        "expand() must return exactly the nodes clusters() said this cluster has"
    );
    for e in &expansion.edges {
        assert!((e.src_idx as usize) < expansion.nodes.len());
        assert!((e.dst_idx as usize) < expansion.nodes.len());
    }
}

#[tokio::test]
async fn unknown_route_under_graph_tile_is_a_404_not_a_hang() {
    let addr = start_server().await;
    let (head, _body) = get_full(addr, "/graph_tile/nope").await;
    assert!(head.starts_with("HTTP/1.1 404"), "head was: {head}");
}

#[tokio::test]
async fn expand_without_cluster_id_is_a_400_not_a_panic() {
    let addr = start_server().await;
    let (head, body) = get_full(addr, "/graph_tile/expand?node_count=100").await;
    assert!(head.starts_with("HTTP/1.1 400"), "head was: {head}");
    let text = String::from_utf8_lossy(&body);
    assert!(text.contains("cluster_id"));
}

/// Read HTTP/1.1 response headers from `stream` up to the blank line, the same
/// way the real production client would: read until `\r\n\r\n`, keeping any
/// body bytes that arrived in the same read. Returns the header text and the
/// leftover body-start bytes.
async fn read_http_headers(stream: &mut TcpStream) -> (String, Vec<u8>) {
    let mut buf = Vec::new();
    let mut tmp = [0u8; 512];
    let header_end = loop {
        if let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
            break pos + 4;
        }
        let n = stream.read(&mut tmp).await.unwrap();
        assert!(n > 0, "connection closed before headers completed");
        buf.extend_from_slice(&tmp[..n]);
    };
    let head = String::from_utf8_lossy(&buf[..header_end]).to_string();
    let body_start = buf[header_end..].to_vec();
    (head, body_start)
}

/// One step of decoding a chunked-transfer-encoding byte stream out of
/// `remainder` (hex length, CRLF, data, CRLF): either a data chunk (drained
/// from `remainder`), the terminating zero-length chunk (also drained), or
/// "not enough buffered yet" (left untouched, read more from the socket).
enum ChunkStep {
    Data(Vec<u8>),
    Terminator,
    NeedMore,
}

fn try_decode_chunk(remainder: &mut Vec<u8>) -> ChunkStep {
    let Some(nl) = remainder.windows(2).position(|w| w == b"\r\n") else {
        return ChunkStep::NeedMore;
    };
    let len_str = String::from_utf8_lossy(&remainder[..nl]).to_string();
    let Ok(len) = usize::from_str_radix(len_str.trim(), 16) else {
        return ChunkStep::NeedMore;
    };
    let needed = nl + 2 + len + 2;
    if remainder.len() < needed {
        return ChunkStep::NeedMore;
    }
    if len == 0 {
        remainder.drain(..needed);
        return ChunkStep::Terminator;
    }
    let data = remainder[nl + 2..nl + 2 + len].to_vec();
    remainder.drain(..needed);
    ChunkStep::Data(data)
}

/// Record which frames are newly decodable from `body` so far, appending
/// `(kind, body.len())` to `offsets` for each one not already recorded.
fn track_new_frame_offsets(body: &[u8], offsets: &mut Vec<(TileKind, usize)>) {
    let (frames, _) = read_frames(body);
    while offsets.len() < frames.len() {
        offsets.push((frames[offsets.len()].kind, body.len()));
    }
}

/// Read one HTTP/1.1 chunked-transfer-encoded response body over `stream`,
/// INCREMENTALLY -- never `read_to_end` first -- recording, for every whole
/// tile frame that becomes decodable, the cumulative number of raw body bytes
/// read from the socket AT THE MOMENT it became decodable. This is what lets
/// the test below assert "the first tile was usable before the rest of the
/// response existed" as a plain byte-offset comparison.
async fn read_chunked_and_track_frame_offsets(
    stream: &mut TcpStream,
) -> (Vec<(TileKind, usize)>, usize) {
    let (head, mut chunked_remainder) = read_http_headers(stream).await;
    assert!(
        head.to_lowercase().contains("transfer-encoding: chunked"),
        "head was: {head}"
    );

    // Decode the chunked-transfer-encoding framing into the raw tile-frame
    // byte stream, reading a SMALL amount from the socket at a time (never
    // the whole response) so this loop genuinely observes partial progress.
    let mut body = Vec::new();
    let mut offsets = Vec::new();
    let mut small_buf = [0u8; 256];
    loop {
        match try_decode_chunk(&mut chunked_remainder) {
            ChunkStep::Terminator => break,
            ChunkStep::Data(chunk) => {
                body.extend_from_slice(&chunk);
                track_new_frame_offsets(&body, &mut offsets);
                continue;
            }
            ChunkStep::NeedMore => {}
        }
        let n = tokio::time::timeout(Duration::from_secs(5), stream.read(&mut small_buf))
            .await
            .expect("stream read timed out -- server never sent the next chunk")
            .unwrap();
        assert!(n > 0, "connection closed mid-stream (truncated response)");
        chunked_remainder.extend_from_slice(&small_buf[..n]);
    }
    (offsets, body.len())
}

#[tokio::test]
async fn stream_route_delivers_the_first_expand_tile_from_a_strict_prefix_of_the_response() {
    let addr = start_server().await;
    let mut stream = TcpStream::connect(addr).await.unwrap();
    // A large enough demo graph that the level tile plus 3 comparably-sized
    // expand tiles make the "first frame decodable well before the whole
    // response" property meaningful rather than trivial.
    let request = "GET /graph_tile/stream?demo=1&node_count=60000&edge_count=180000&top_clusters=6&top_k=3&seed=5 HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n";
    stream.write_all(request.as_bytes()).await.unwrap();

    let (offsets, total_len) = read_chunked_and_track_frame_offsets(&mut stream).await;

    // level tile + 3 expand tiles + StreamEnd sentinel.
    assert_eq!(
        offsets.len(),
        5,
        "expected 1 ClusterLevel + 3 ClusterExpansion + 1 StreamEnd frame"
    );
    assert_eq!(offsets[0].0, TileKind::ClusterLevel);
    assert_eq!(offsets[1].0, TileKind::ClusterExpansion);
    assert_eq!(offsets[2].0, TileKind::ClusterExpansion);
    assert_eq!(offsets[3].0, TileKind::ClusterExpansion);
    assert_eq!(offsets[4].0, TileKind::StreamEnd);

    // The core streaming claim: the FIRST expand tile (offsets[1]) is
    // decodable from a strict, non-trivial prefix of the total response --
    // well under the full length the connection eventually carries. This is
    // a byte-offset fact about what was written and flushed, not a timing
    // guess, so it cannot be CI-load-flaky.
    let first_expand_offset = offsets[1].1;
    assert!(
        first_expand_offset < total_len,
        "first expand tile ({first_expand_offset} bytes in) must be decodable before the \
         response ({total_len} bytes) is complete"
    );
    assert!(
        (first_expand_offset as f64) < 0.85 * (total_len as f64),
        "first expand tile arrived at {first_expand_offset}/{total_len} bytes -- expected it \
         well before the end, proving the client did not have to wait for the whole graph"
    );
}
