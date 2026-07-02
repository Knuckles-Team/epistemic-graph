//! GraphQL **real subscriptions** over Server-Sent Events (CONCEPT:EG-064, feature
//! `graphql`).
//!
//! A minimal, dependency-free HTTP/1.1 listener — the SAME hand-rolled tokio idiom as
//! the SPARQL (`sparql_http::serve`) and Prometheus (`metrics::serve`) endpoints, so NO
//! axum/hyper/warp is pulled and the Pi contract holds. It carries a GraphQL
//! `subscription { … }` as a live query: the connection subscribes to the graph's
//! eg-core change stream (`GraphCore::changes()`), and on every committed write it
//! re-resolves the subscription over a FRESH snapshot and pushes the `{"data": …}` frame
//! down a chunked `text/event-stream`:
//!
//!   * `GET /graphql/subscribe?query=<url-encoded subscription>[&graph=<name>]`
//!       → `200 text/event-stream`, one `data: {json}\n\n` frame per change (plus an
//!         initial frame with the current matches), `: keepalive\n\n` comments in between.
//!   * `OPTIONS …` → `204` CORS preflight.
//!
//! ## SSE, not WebSocket (documented)
//! A full WebSocket carrier (the `graphql-ws` sub-protocol handshake: `Sec-WebSocket-Key`
//! accept, frame masking/opcodes, close) is a larger surface than a re-resolve-on-change
//! push needs, and a hand-rolled WS framer is easy to get subtly wrong under load. SSE is
//! a few lines over the SAME tokio listener, needs no framing, auto-reconnects in the
//! browser, and is exactly a one-way server→client push — which is what a live query is.
//! WebSocket + the `graphql-transport-ws` protocol is a documented FOLLOW-UP; the
//! re-resolve engine ([`eg_graphql::LiveQuery`]) is transport-agnostic, so adding WS later
//! is additive (no change to resolution).
//!
//! ## Scope (documented follow-ups)
//! Per-agent RLS filtering of the pushed frames and auth on the carrier mirror the SPARQL
//! endpoint's posture (convenience interop surface); wiring the same `IsolationLayer`
//! filter the RPC GraphQL path uses onto each pushed frame is a follow-up.

use std::sync::Arc;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::RwLock;

use crate::graph::{ChangeEvent, ChangeSink, GraphCore};
use crate::server::ServerState;
use eg_graphql::LiveQuery;

/// Env var carrying the bind address (`host:port`) when `--graphql-addr` is not passed.
pub const GRAPHQL_ADDR_ENV: &str = "EPISTEMIC_GRAPH_GRAPHQL_ADDR";
/// Keepalive comment cadence (seconds) so a dead connection is detected promptly and
/// intermediaries don't reap an idle stream.
const KEEPALIVE_SECS: u64 = 15;

/// A [`ChangeSink`] that forwards each eg-core change into the connection task over a
/// `watch` channel. `on_change` is called INLINE on the write path, so it does only a
/// non-blocking `send` (watch coalesces to the latest version — a missed tick just means
/// the next re-resolve already reflects it).
struct LiveSink {
    tx: tokio::sync::watch::Sender<u64>,
}
impl ChangeSink for LiveSink {
    fn on_change(&self, event: &ChangeEvent) {
        let _ = self.tx.send(event.version);
    }
}

/// Serve the GraphQL subscription SSE carrier on `listener`, backed by the engine `state`.
pub async fn serve(listener: TcpListener, state: Arc<RwLock<ServerState>>) {
    loop {
        let Ok((stream, _)) = listener.accept().await else {
            continue;
        };
        let state = state.clone();
        tokio::spawn(async move {
            if let Err(e) = handle_conn(stream, state).await {
                tracing::debug!("graphql-sub connection ended: {e}");
            }
        });
    }
}

/// Read the request line, route, and (for a valid subscribe) stream frames until the
/// client disconnects.
async fn handle_conn(mut stream: TcpStream, state: Arc<RwLock<ServerState>>) -> Result<(), String> {
    let (method, target) = read_request_line(&mut stream)
        .await
        .ok_or("malformed HTTP request")?;

    let (path, query_string) = match target.split_once('?') {
        Some((p, q)) => (p, q),
        None => (target.as_str(), ""),
    };

    if method == "OPTIONS" {
        return write_simple(&mut stream, "204 No Content", "text/plain", "").await;
    }
    if method != "GET" {
        return write_simple(
            &mut stream,
            "405 Method Not Allowed",
            "text/plain",
            "method",
        )
        .await;
    }
    if path != "/graphql/subscribe" {
        return write_simple(&mut stream, "404 Not Found", "text/plain", "not found").await;
    }

    let params = parse_form(query_string);
    let query = params.get("query").cloned().unwrap_or_default();
    if query.trim().is_empty() {
        return write_simple(&mut stream, "400 Bad Request", "text/plain", "empty query").await;
    }
    let graph = params
        .get("graph")
        .cloned()
        .or_else(|| std::env::var("EPISTEMIC_GRAPH_SPARQL_DEFAULT_GRAPH").ok())
        .unwrap_or_else(|| "__commons__".to_string());

    // Parse the subscription into a live query before we commit to streaming, so a bad
    // document is a clean 400 (not a half-open event-stream).
    let live = match LiveQuery::parse(&query) {
        Ok(l) => l,
        Err(e) => {
            return write_simple(
                &mut stream,
                "400 Bad Request",
                "text/plain",
                &format!("GraphQL subscription error: {e}"),
            )
            .await
        }
    };

    // Resolve the live core for the requested graph (a brief read lock, then off-lock).
    let core: Arc<GraphCore> = {
        let s = state.read().await;
        match s.registry.get(&graph) {
            Some(e) => e.core.clone(),
            None => {
                return write_simple(
                    &mut stream,
                    "404 Not Found",
                    "text/plain",
                    &format!("no graph `{graph}`"),
                )
                .await
            }
        }
    };

    // Subscribe to the change stream. The `Arc<dyn ChangeSink>` MUST outlive the loop —
    // dropping it (on return) unsubscribes (the notifier holds only a Weak).
    let (tx, mut rx) = tokio::sync::watch::channel(0u64);
    let sink: Arc<dyn ChangeSink> = Arc::new(LiveSink { tx });
    core.changes().subscribe(&sink);

    // SSE response head.
    stream
        .write_all(
            b"HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ncache-control: no-cache\r\nconnection: keep-alive\r\naccess-control-allow-origin: *\r\n\r\n",
        )
        .await
        .map_err(|e| e.to_string())?;

    // Initial frame: the current matches.
    push_frame(&mut stream, &live, &core).await?;

    // Push a fresh frame whenever the version advances; a keepalive comment otherwise so
    // a dropped client is detected (the write fails) and idle intermediaries don't reap us.
    let mut keepalive = tokio::time::interval(std::time::Duration::from_secs(KEEPALIVE_SECS));
    keepalive.tick().await; // consume the immediate first tick
    loop {
        tokio::select! {
            changed = rx.changed() => {
                if changed.is_err() {
                    break; // sender gone (shouldn't happen while we hold the sink)
                }
                push_frame(&mut stream, &live, &core).await?;
            }
            _ = keepalive.tick() => {
                if stream.write_all(b": keepalive\n\n").await.is_err() {
                    break; // client disconnected
                }
            }
        }
    }
    Ok(())
}

/// Re-resolve the live query over a fresh snapshot and write one SSE `data:` frame.
async fn push_frame(
    stream: &mut TcpStream,
    live: &LiveQuery,
    core: &GraphCore,
) -> Result<(), String> {
    let (data, _version) = live.resolve(core)?;
    let frame = format!(
        "data: {}\n\n",
        serde_json::to_string(&data).unwrap_or_default()
    );
    stream
        .write_all(frame.as_bytes())
        .await
        .map_err(|e| e.to_string())
}

/// Write a small one-shot HTTP response and close (errors / preflight).
async fn write_simple(
    stream: &mut TcpStream,
    status: &str,
    ctype: &str,
    body: &str,
) -> Result<(), String> {
    let resp = format!(
        "HTTP/1.1 {status}\r\ncontent-type: {ctype}\r\ncontent-length: {}\r\naccess-control-allow-origin: *\r\naccess-control-allow-headers: content-type, accept\r\nconnection: close\r\n\r\n{body}",
        body.len()
    );
    let _ = stream.write_all(resp.as_bytes()).await;
    let _ = stream.shutdown().await;
    Ok(())
}

/// Read just the request line (`METHOD TARGET HTTP/1.1`) — a subscribe is a GET with no
/// body, so we stop at the header terminator.
async fn read_request_line(stream: &mut TcpStream) -> Option<(String, String)> {
    let mut buf = Vec::new();
    let mut tmp = [0u8; 2048];
    loop {
        if find_subslice(&buf, b"\r\n\r\n").is_some() {
            break;
        }
        let n = stream.read(&mut tmp).await.ok()?;
        if n == 0 {
            break;
        }
        buf.extend_from_slice(&tmp[..n]);
        if buf.len() > 64 * 1024 {
            return None; // header flood guard
        }
    }
    let head = String::from_utf8_lossy(&buf);
    let first = head.lines().next()?;
    let mut parts = first.split_whitespace();
    let method = parts.next()?.to_string();
    let target = parts.next()?.to_string();
    Some((method, target))
}

fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack.windows(needle.len()).position(|w| w == needle)
}

/// Parse an `a=b&c=d` form/query string with percent-decoding (matches the SPARQL
/// endpoint's helper).
fn parse_form(s: &str) -> std::collections::HashMap<String, String> {
    let mut map = std::collections::HashMap::new();
    for pair in s.split('&') {
        if pair.is_empty() {
            continue;
        }
        let (k, v) = pair.split_once('=').unwrap_or((pair, ""));
        map.insert(percent_decode(k), percent_decode(v));
    }
    map
}

/// Minimal `application/x-www-form-urlencoded` percent-decode (`+` → space, `%XX` → byte).
fn percent_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'+' => {
                out.push(b' ');
                i += 1;
            }
            b'%' if i + 2 < bytes.len() => {
                let hi = (bytes[i + 1] as char).to_digit(16);
                let lo = (bytes[i + 2] as char).to_digit(16);
                if let (Some(hi), Some(lo)) = (hi, lo) {
                    out.push((hi * 16 + lo) as u8);
                    i += 3;
                } else {
                    out.push(bytes[i]);
                    i += 1;
                }
            }
            b => {
                out.push(b);
                i += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}
