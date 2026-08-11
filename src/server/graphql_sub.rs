//! Authenticated GraphQL subscriptions over Server-Sent Events.
//!
//! The carrier accepts exactly one current request shape:
//!
//! * `GET /graphql/subscribe?graph=<graph>&query=<subscription>`
//! * `Authorization: Bearer eg2.<verified-envelope>`
//! * `X-Epistemic-Request-Id: <u64>`
//!
//! The `eg2.` envelope is verified against a synthetic [`Method::GraphQl`] request
//! containing the exact request id, graph, and subscription document received over
//! HTTP. Consequently a token cannot be replayed for another graph or query. There is
//! no query-string token, unsigned development mode, default graph, authentication
//! alias, permissive CORS preflight, or legacy listener fallback.
//!
//! After cryptographic verification, every initial frame and re-resolution passes the
//! verified actor through graph ACL and the same default-deny row projection used by
//! native graph reads. ACL and RLS policy are re-read on graph changes and keepalive
//! ticks, so a long-lived connection cannot retain revoked access. Frames are emitted
//! only when the caller-visible result changes; a write confined to hidden rows cannot
//! become a subscription timing side channel.
//!
//! This listener is intentionally plain HTTP and therefore must bind to loopback. A
//! TLS reverse proxy on the same host may expose it remotely. `main.rs` enforces that
//! boundary before this module receives a listener.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use eg_graphql::LiveQuery;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{OwnedSemaphorePermit, RwLock, Semaphore};

use crate::graph::{ChangeEvent, ChangeSink, GraphCore, GraphView};
use crate::isolation::{AccessLevel, IsolationLayer};
use crate::protocol::{GraphType, Method, Request};
use crate::server::access::{check_graph_access, GraphReadAuthority};
use crate::server::auth::{verify_request_with_security_dir, VerifiedRequestContext};
use crate::server::ServerState;

/// Environment/CLI address used by `main.rs` to opt into this listener.
pub const GRAPHQL_ADDR_ENV: &str = "EPISTEMIC_GRAPH_GRAPHQL_ADDR";
/// Request header whose value participates in the `eg2.` signature.
pub const GRAPHQL_REQUEST_ID_HEADER: &str = "x-epistemic-request-id";

const KEEPALIVE_SECS: u64 = 15;
const HTTP_READ_TIMEOUT_SECS: u64 = 10;
const HTTP_WRITE_TIMEOUT_SECS: u64 = 10;
const MAX_HEADER_BYTES: usize = 64 * 1024;
const MAX_QUERY_BYTES: usize = 32 * 1024;
const MAX_GRAPH_BYTES: usize = 512;
const MAX_AUTHORIZATION_BYTES: usize = 48 * 1024;
const MAX_SYNTAX_NESTING: usize = 64;
const MAX_SSE_FRAME_BYTES: usize = 8 * 1024 * 1024;
const FRAME_LIMIT_ERROR: &str = "subscription result exceeds the SSE frame limit";
const MAX_CONNECTIONS_CEILING: usize = 10_000;
const MAX_SESSION_SECS_CEILING: u64 = 3_600;

/// Resource bounds for the long-lived subscription carrier.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct GraphQlSubscriptionConfig {
    max_connections: usize,
    max_session_secs: u64,
}

impl GraphQlSubscriptionConfig {
    pub fn new(max_connections: usize, max_session_secs: u64) -> Result<Self, String> {
        if !(1..=MAX_CONNECTIONS_CEILING).contains(&max_connections) {
            return Err(format!(
                "GraphQL subscription max connections must be between 1 and {MAX_CONNECTIONS_CEILING}"
            ));
        }
        if !(1..=MAX_SESSION_SECS_CEILING).contains(&max_session_secs) {
            return Err(format!(
                "GraphQL subscription max session seconds must be between 1 and {MAX_SESSION_SECS_CEILING}"
            ));
        }
        Ok(Self {
            max_connections,
            max_session_secs,
        })
    }
}

/// A [`ChangeSink`] that forwards graph-version changes without blocking the write
/// path. The watch channel intentionally coalesces bursts: the next authorized
/// snapshot already reflects every committed write in that burst.
struct LiveSink {
    tx: tokio::sync::watch::Sender<u64>,
}

impl ChangeSink for LiveSink {
    fn on_change(&self, event: &ChangeEvent) {
        let _ = self.tx.send(event.version);
    }
}

/// Serve the current authenticated GraphQL subscription carrier.
pub async fn serve(
    listener: TcpListener,
    state: Arc<RwLock<ServerState>>,
    config: GraphQlSubscriptionConfig,
) {
    let connections = Arc::new(Semaphore::new(config.max_connections));
    loop {
        let Ok((mut stream, _peer)) = listener.accept().await else {
            continue;
        };
        let permit = match connections.clone().try_acquire_owned() {
            Ok(permit) => permit,
            Err(_) => {
                tokio::spawn(async move {
                    let _ = write_simple(
                        &mut stream,
                        "503 Service Unavailable",
                        "subscription capacity exhausted",
                    )
                    .await;
                });
                continue;
            }
        };
        let state = state.clone();
        tokio::spawn(async move {
            if let Err(error) = handle_conn(stream, state, config, permit).await {
                tracing::debug!(reason = %error, "GraphQL subscription connection closed");
            }
        });
    }
}

#[derive(Debug)]
struct HttpRequest {
    method: String,
    target: String,
    headers: HashMap<String, String>,
}

struct CanonicalSubscription {
    graph: String,
    query: String,
    request: Request,
}

/// Parse, authenticate, authorize, and stream one connection.
async fn handle_conn(
    mut stream: TcpStream,
    state: Arc<RwLock<ServerState>>,
    config: GraphQlSubscriptionConfig,
    _permit: OwnedSemaphorePermit,
) -> Result<(), String> {
    let http = match tokio::time::timeout(
        Duration::from_secs(HTTP_READ_TIMEOUT_SECS),
        read_request(&mut stream),
    )
    .await
    {
        Err(_) => return write_simple(&mut stream, "408 Request Timeout", "request timeout").await,
        Ok(Err(_)) => return write_simple(&mut stream, "400 Bad Request", "invalid request").await,
        Ok(Ok(request)) => request,
    };

    if http.method != "GET" {
        return write_simple(
            &mut stream,
            "405 Method Not Allowed",
            "only GET is supported",
        )
        .await;
    }

    let canonical = match canonical_subscription(&http) {
        Ok(request) => request,
        Err(_) => return write_simple(&mut stream, "400 Bad Request", "invalid request").await,
    };
    let CanonicalSubscription {
        graph,
        query,
        request,
    } = canonical;

    // Copy only the immutable authentication inputs while holding state. The replay
    // ledger commit and signature verification run after releasing the state lock.
    let (secret, security_dir) = {
        let state = state.read().await;
        (state.auth_secret.clone(), state.persist_dir.clone())
    };
    let verified =
        match verify_request_with_security_dir(&secret, &request, security_dir.as_deref()) {
            Ok(context) => context,
            Err(_) => {
                crate::metrics::auth_failure();
                return write_simple(&mut stream, "401 Unauthorized", "authentication required")
                    .await;
            }
        };

    // `Method::GraphQl` is a query-or-mutation family in the capability ledger. A
    // successfully parsed LiveQuery below is provably read-only, so apply the ledger's
    // canonical action using read semantics. Exact/domain wildcard scopes, kg:read,
    // kg:write, and kg:admin follow the same VerifiedRequestContext rules as native reads.
    let method_policy = eg_capabilities::policy(&request.method);
    if !verified.allows_method(method_policy.authz_action, false) {
        crate::metrics::access_denied();
        return write_simple(&mut stream, "403 Forbidden", "access denied").await;
    }

    // Parsing happens only after the MAC and replay nonce verify, preventing an
    // unauthenticated client from spending GraphQL parser/validator CPU.
    // The signed bearer is no longer needed after verification and scope binding.
    // Do not retain it for the lifetime of the SSE connection.
    drop(request);

    if preflight_query_shape(&query).is_err() {
        return write_simple(
            &mut stream,
            "400 Bad Request",
            "invalid GraphQL subscription",
        )
        .await;
    }
    let live = match LiveQuery::parse_with_policy(&query, &subscription_policy()) {
        Ok(live) => live,
        Err(_) => {
            return write_simple(
                &mut stream,
                "400 Bad Request",
                "invalid GraphQL subscription",
            )
            .await
        }
    };

    // Authorize the current graph incarnation before subscribing to its notifier.
    let first = match authorized_snapshot(&state, &verified, &graph, None).await {
        Ok(snapshot) => snapshot,
        Err(GraphAccessError::NotFound) => {
            return write_simple(&mut stream, "404 Not Found", "graph not found").await
        }
        // Do not distinguish a missing graph from one outside the verified
        // tenant/actor ACL; graph-name probing receives the same response.
        Err(GraphAccessError::Denied) => {
            return write_simple(&mut stream, "404 Not Found", "graph not found").await
        }
        Err(GraphAccessError::Replaced) => {
            return write_simple(&mut stream, "404 Not Found", "graph not found").await
        }
    };

    // Subscribe before the second snapshot so a write racing the initial render is
    // observed by `rx` and re-resolved rather than missed.
    let (tx, mut rx) = tokio::sync::watch::channel(0u64);
    let sink: Arc<dyn ChangeSink> = Arc::new(LiveSink { tx });
    first.core.changes().subscribe(&sink);

    let initial_snapshot =
        match authorized_snapshot(&state, &verified, &graph, Some(&first.incarnation_id)).await {
            Ok(snapshot) => snapshot,
            Err(_) => return write_simple(&mut stream, "404 Not Found", "graph not found").await,
        };
    let initial = match render(&live, &initial_snapshot.view) {
        Ok(rendered) => rendered,
        Err(error) => {
            if error == FRAME_LIMIT_ERROR {
                return write_simple(
                    &mut stream,
                    "413 Content Too Large",
                    "subscription result exceeds the frame limit",
                )
                .await;
            }
            return write_simple(
                &mut stream,
                "400 Bad Request",
                "invalid GraphQL subscription",
            )
            .await;
        }
    };

    write_stream_head(&mut stream).await?;
    write_data_frame(&mut stream, &initial).await?;
    let mut last_visible = initial;

    let mut keepalive = tokio::time::interval(Duration::from_secs(KEEPALIVE_SECS));
    keepalive.tick().await;
    let session = tokio::time::sleep(Duration::from_secs(config.max_session_secs));
    tokio::pin!(session);

    loop {
        tokio::select! {
            changed = rx.changed() => {
                if changed.is_err() {
                    break;
                }
                let Some(visible) = refresh_visible(
                    &state,
                    &verified,
                    &graph,
                    &first.incarnation_id,
                    &live,
                ).await else {
                    break;
                };
                if visible != last_visible {
                    write_data_frame(&mut stream, &visible).await?;
                    last_visible = visible;
                }
            }
            _ = keepalive.tick() => {
                // Policy changes do not necessarily mutate this graph. Re-authorize and
                // re-project on every keepalive so revocation/default-deny takes effect
                // within a bounded interval even when the graph is idle.
                let Some(visible) = refresh_visible(
                    &state,
                    &verified,
                    &graph,
                    &first.incarnation_id,
                    &live,
                ).await else {
                    break;
                };
                if visible != last_visible {
                    write_data_frame(&mut stream, &visible).await?;
                    last_visible = visible;
                } else if write_bounded(&mut stream, b": keepalive\n\n").await.is_err() {
                    break;
                }
            }
            _ = &mut session => {
                // A fresh connection must present a newly signed request and nonce.
                break;
            }
        }
    }
    Ok(())
}

async fn refresh_visible(
    state: &Arc<RwLock<ServerState>>,
    verified: &VerifiedRequestContext,
    graph: &str,
    incarnation_id: &str,
    live: &LiveQuery,
) -> Option<String> {
    let snapshot = authorized_snapshot(state, verified, graph, Some(incarnation_id))
        .await
        .ok()?;
    render(live, &snapshot.view).ok()
}

struct AuthorizedSnapshot {
    core: Arc<GraphCore>,
    incarnation_id: String,
    view: GraphView,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum GraphAccessError {
    NotFound,
    Denied,
    Replaced,
}

/// Resolve one current graph incarnation under fresh ACL and RLS policy.
async fn authorized_snapshot(
    state: &Arc<RwLock<ServerState>>,
    verified: &VerifiedRequestContext,
    graph: &str,
    expected_incarnation: Option<&str>,
) -> Result<AuthorizedSnapshot, GraphAccessError> {
    let (core, incarnation_id) = {
        let state = state.read().await;
        let entry = state.registry.get(graph).ok_or_else(|| {
            if expected_incarnation.is_some() {
                GraphAccessError::Replaced
            } else {
                GraphAccessError::NotFound
            }
        })?;
        if expected_incarnation.is_some_and(|expected| expected != entry.incarnation_id.as_str()) {
            return Err(GraphAccessError::Replaced);
        }
        // Gate before taking a potentially large snapshot so an authenticated but
        // unauthorized caller cannot turn graph existence into snapshot work.
        graph_read_authority(
            &state.isolation,
            verified,
            graph,
            entry.graph_type,
            entry.owner.as_deref(),
        )?;
        (entry.core.clone(), entry.incarnation_id.clone())
    };
    let mut view = core.analysis_snapshot();
    // Fence delete/recreate and re-read ACL/RLS after the snapshot. This is the
    // policy decision immediately preceding publication, not a connection-time copy.
    let authority = {
        let state = state.read().await;
        let entry = state
            .registry
            .get(graph)
            .ok_or(GraphAccessError::Replaced)?;
        if entry.incarnation_id.as_str() != incarnation_id {
            return Err(GraphAccessError::Replaced);
        }
        graph_read_authority(
            &state.isolation,
            verified,
            graph,
            entry.graph_type,
            entry.owner.as_deref(),
        )?
    };
    authority.filter_view(&mut view);
    Ok(AuthorizedSnapshot {
        core,
        incarnation_id,
        view,
    })
}

fn graph_read_authority(
    isolation: &IsolationLayer,
    verified: &VerifiedRequestContext,
    graph: &str,
    graph_type: GraphType,
    owner: Option<&str>,
) -> Result<GraphReadAuthority, GraphAccessError> {
    check_graph_access(
        isolation,
        Some(verified.agent_id()),
        graph,
        graph_type,
        owner,
        AccessLevel::Read,
    )
    .map_err(|_| GraphAccessError::Denied)?;
    GraphReadAuthority::from_verified(verified, isolation).map_err(|_| GraphAccessError::Denied)
}

fn render(live: &LiveQuery, view: &GraphView) -> Result<String, String> {
    let data = live.resolve_view(view)?;
    let json =
        serde_json::to_string(&data).map_err(|_| "could not serialize subscription result")?;
    if json.len() > MAX_SSE_FRAME_BYTES {
        return Err(FRAME_LIMIT_ERROR.into());
    }
    Ok(json)
}

fn subscription_policy() -> eg_graphql::GraphQlPolicy {
    let mut policy = eg_graphql::GraphQlPolicy::locked_down();
    // The resolver's implicit list bound is intentionally large. Price an omitted
    // `first`/`limit` at that ceiling so live queries must declare bounded fan-out.
    policy.list_page_factor = 50_000;
    policy
}

/// Bound delimiter nesting before invoking the recursive GraphQL parser. Braces in
/// quoted strings and comments are ignored; mismatched syntax remains the parser's job.
fn preflight_query_shape(query: &str) -> Result<(), String> {
    let mut stack = Vec::new();
    let mut quoted = false;
    let mut escaped = false;
    let mut comment = false;
    for byte in query.bytes() {
        if comment {
            if byte == b'\n' || byte == b'\r' {
                comment = false;
            }
            continue;
        }
        if quoted {
            if escaped {
                escaped = false;
            } else if byte == b'\\' {
                escaped = true;
            } else if byte == b'"' {
                quoted = false;
            }
            continue;
        }
        match byte {
            b'#' => comment = true,
            b'"' => quoted = true,
            b'{' | b'(' | b'[' => {
                stack.push(byte);
                if stack.len() > MAX_SYNTAX_NESTING {
                    return Err("GraphQL syntax nesting exceeds the carrier limit".into());
                }
            }
            b'}' | b')' | b']' => {
                let expected = match byte {
                    b'}' => b'{',
                    b')' => b'(',
                    _ => b'[',
                };
                if stack.pop() != Some(expected) {
                    return Err("GraphQL delimiters are unbalanced".into());
                }
            }
            _ => {}
        }
    }
    if quoted || !stack.is_empty() {
        return Err("GraphQL syntax is incomplete".into());
    }
    Ok(())
}

fn canonical_subscription(http: &HttpRequest) -> Result<CanonicalSubscription, String> {
    let (path, query_string) = http
        .target
        .split_once('?')
        .ok_or_else(|| "missing subscription parameters".to_string())?;
    if path != "/graphql/subscribe" {
        return Err("unknown route".into());
    }
    let mut params = parse_form(query_string)?;
    let graph = params
        .remove("graph")
        .ok_or_else(|| "missing graph".to_string())?;
    let query = params
        .remove("query")
        .ok_or_else(|| "missing query".to_string())?;
    if !params.is_empty() {
        return Err("unknown subscription parameter".into());
    }
    if graph.is_empty()
        || graph.trim() != graph
        || graph.len() > MAX_GRAPH_BYTES
        || graph.chars().any(char::is_control)
    {
        return Err("invalid graph".into());
    }
    if query.trim().is_empty() || query.len() > MAX_QUERY_BYTES {
        return Err("invalid query".into());
    }

    let request_id = http
        .headers
        .get(GRAPHQL_REQUEST_ID_HEADER)
        .ok_or_else(|| "missing request id".to_string())?
        .parse::<u64>()
        .ok()
        .filter(|id| *id > 0)
        .ok_or_else(|| "invalid request id".to_string())?;
    let authorization = http
        .headers
        .get("authorization")
        .filter(|value| value.len() <= MAX_AUTHORIZATION_BYTES)
        .ok_or_else(|| "missing authorization".to_string())?;
    let auth_token = authorization
        .strip_prefix("Bearer ")
        .filter(|token| !token.is_empty() && !token.chars().any(char::is_whitespace))
        .ok_or_else(|| "invalid authorization".to_string())?
        .to_string();

    let request = Request {
        id: request_id,
        graph: graph.clone(),
        auth_token,
        agent_id: None,
        method: Method::GraphQl {
            query: query.clone(),
            variables: None,
        },
    };
    Ok(CanonicalSubscription {
        graph,
        query,
        request,
    })
}

/// Strict HTTP/1.1 header parser. Duplicate headers, folded headers, request bodies,
/// transfer encodings, invalid UTF-8, and oversized heads are rejected before auth.
async fn read_request(stream: &mut TcpStream) -> Result<HttpRequest, String> {
    let mut bytes = Vec::new();
    let mut chunk = [0u8; 2048];
    let head_end = loop {
        if let Some(end) = find_subslice(&bytes, b"\r\n\r\n") {
            break end;
        }
        let read = stream.read(&mut chunk).await.map_err(|_| "read failed")?;
        if read == 0 {
            return Err("incomplete request".into());
        }
        bytes.extend_from_slice(&chunk[..read]);
        if bytes.len() > MAX_HEADER_BYTES {
            return Err("request head too large".into());
        }
    };
    if head_end + 4 != bytes.len() {
        return Err("request body or pipelining is not supported".into());
    }
    let head = std::str::from_utf8(&bytes[..head_end]).map_err(|_| "invalid header encoding")?;
    let mut lines = head.split("\r\n");
    let request_line = lines.next().ok_or("missing request line")?;
    if !request_line.is_ascii()
        || request_line.bytes().filter(|byte| *byte == b' ').count() != 2
        || request_line.bytes().any(|byte| byte.is_ascii_control())
    {
        return Err("invalid request line".into());
    }
    let mut parts = request_line.split(' ');
    let method = parts.next().ok_or("missing method")?;
    let target = parts.next().ok_or("missing target")?;
    let version = parts.next().ok_or("missing HTTP version")?;
    if parts.next().is_some()
        || version != "HTTP/1.1"
        || !target.starts_with('/')
        || !target.is_ascii()
        || target.chars().any(char::is_control)
    {
        return Err("invalid request line".into());
    }

    let mut headers = HashMap::new();
    for line in lines {
        if line.is_empty() || line.starts_with(' ') || line.starts_with('\t') {
            return Err("invalid header line".into());
        }
        let (name, value) = line.split_once(':').ok_or("invalid header")?;
        if name.is_empty()
            || !name
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
        {
            return Err("invalid header name".into());
        }
        let value = value.trim();
        if value.is_empty() || value.chars().any(char::is_control) {
            return Err("invalid header value".into());
        }
        let name = name.to_ascii_lowercase();
        if headers.insert(name, value.to_string()).is_some() {
            return Err("duplicate header".into());
        }
    }
    if headers.get("host").is_none_or(|host| host.is_empty())
        || headers.contains_key("transfer-encoding")
        || headers
            .get("content-length")
            .is_some_and(|value| value != "0")
    {
        return Err("invalid request framing".into());
    }
    Ok(HttpRequest {
        method: method.to_string(),
        target: target.to_string(),
        headers,
    })
}

fn parse_form(input: &str) -> Result<HashMap<String, String>, String> {
    let mut params = HashMap::new();
    if input.is_empty() {
        return Err("empty query string".into());
    }
    for pair in input.split('&') {
        if pair.is_empty() {
            return Err("empty query parameter".into());
        }
        let (key, value) = pair.split_once('=').ok_or("parameter has no value")?;
        let key = percent_decode(key)?;
        let value = percent_decode(value)?;
        if key.is_empty() || params.insert(key, value).is_some() {
            return Err("duplicate or empty query parameter".into());
        }
    }
    Ok(params)
}

fn percent_decode(input: &str) -> Result<String, String> {
    let input = input.as_bytes();
    let mut output = Vec::with_capacity(input.len());
    let mut index = 0;
    while index < input.len() {
        match input[index] {
            b'+' => {
                output.push(b' ');
                index += 1;
            }
            b'%' => {
                if index + 2 >= input.len() {
                    return Err("incomplete percent escape".into());
                }
                let high = (input[index + 1] as char)
                    .to_digit(16)
                    .ok_or("invalid percent escape")?;
                let low = (input[index + 2] as char)
                    .to_digit(16)
                    .ok_or("invalid percent escape")?;
                output.push((high * 16 + low) as u8);
                index += 3;
            }
            byte => {
                output.push(byte);
                index += 1;
            }
        }
    }
    String::from_utf8(output).map_err(|_| "invalid UTF-8 in query parameter".into())
}

fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

async fn write_stream_head(stream: &mut TcpStream) -> Result<(), String> {
    write_bounded(
        stream,
        b"HTTP/1.1 200 OK\r\n\
content-type: text/event-stream\r\n\
cache-control: no-cache, no-store\r\n\
connection: keep-alive\r\n\
x-content-type-options: nosniff\r\n\
content-security-policy: default-src 'none'\r\n\
referrer-policy: no-referrer\r\n\
x-accel-buffering: no\r\n\r\n",
    )
    .await
}

async fn write_data_frame(stream: &mut TcpStream, json: &str) -> Result<(), String> {
    let frame = format!("data: {json}\n\n");
    write_bounded(stream, frame.as_bytes()).await
}

async fn write_bounded(stream: &mut TcpStream, bytes: &[u8]) -> Result<(), String> {
    tokio::time::timeout(
        Duration::from_secs(HTTP_WRITE_TIMEOUT_SECS),
        stream.write_all(bytes),
    )
    .await
    .map_err(|_| "HTTP write timed out".to_string())?
    .map_err(|_| "HTTP write failed".to_string())
}

async fn write_simple(stream: &mut TcpStream, status: &str, body: &str) -> Result<(), String> {
    let authenticate = if status.starts_with("401") {
        "www-authenticate: Bearer realm=\"epistemic-graph\"\r\n"
    } else {
        ""
    };
    let allow = if status.starts_with("405") {
        "allow: GET\r\n"
    } else {
        ""
    };
    let response = format!(
        "HTTP/1.1 {status}\r\n\
content-type: text/plain; charset=utf-8\r\n\
content-length: {}\r\n\
cache-control: no-store\r\n\
x-content-type-options: nosniff\r\n\
content-security-policy: default-src 'none'\r\n\
referrer-policy: no-referrer\r\n\
{authenticate}{allow}connection: close\r\n\r\n{body}",
        body.len()
    );
    write_bounded(stream, response.as_bytes()).await?;
    tokio::time::timeout(
        Duration::from_secs(HTTP_WRITE_TIMEOUT_SECS),
        stream.shutdown(),
    )
    .await
    .map_err(|_| "HTTP shutdown timed out".to_string())?
    .map_err(|_| "HTTP shutdown failed".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::acl::{AgentIdentity, AgentRole, RequestContextClaims};
    use crate::server::auth::{compute_verified_envelope_token, VerifiedEnvelopeParams};
    use serde_json::json;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn properties(value: serde_json::Value) -> Vec<u8> {
        rmp_serde::to_vec_named(&value).unwrap()
    }

    fn http(target: &str, authorization: Option<&str>) -> HttpRequest {
        let mut headers = HashMap::from([
            ("host".to_string(), "graph.invalid".to_string()),
            (GRAPHQL_REQUEST_ID_HEADER.to_string(), "41".to_string()),
        ]);
        if let Some(value) = authorization {
            headers.insert("authorization".to_string(), value.to_string());
        }
        HttpRequest {
            method: "GET".into(),
            target: target.into(),
            headers,
        }
    }

    fn isolation() -> IsolationLayer {
        let mut isolation = IsolationLayer::new();
        // RBAC (CONCEPT:EG-KG.compute.feature) is the mandatory current access
        // decision under `feature = "security"` (`isolation.rs::check_access`) —
        // there is no pre-RBAC "Commons is public" fall-through any more for a
        // non-`System` identity (see `server::mod::tests::multi_tenant_state`'s
        // doc comment for the same migration, and
        // `query::current_auth_test_support::current_isolation_with_agents`'s
        // identical fixture fix). Give both test agents the SAME "commons-user"
        // R/W grant that fixture already establishes as the replacement for the
        // retired open-bus ACL semantics.
        #[cfg(feature = "security")]
        {
            use crate::acl::{Grant, GrantEffect, RbacAction, ResourceSelector, Role};
            isolation.add_role(Role::new("commons-user"));
            let grant = |action: RbacAction| Grant {
                role: "commons-user".to_string(),
                resource: ResourceSelector::Graph("__commons__".to_string()),
                action,
                effect: GrantEffect::Allow,
            };
            isolation.add_grant(grant(RbacAction::Read));
            isolation.add_grant(grant(RbacAction::Write));
        }
        for agent_id in ["actor-a", "actor-b"] {
            isolation.register_agent(AgentIdentity {
                agent_id: agent_id.into(),
                role: AgentRole::Agent,
                teams: Vec::new(),
                #[cfg(feature = "security")]
                roles: vec!["commons-user".to_string()],
                #[cfg(not(feature = "security"))]
                roles: Vec::new(),
            });
        }
        isolation
    }

    #[test]
    fn canonical_request_binds_exact_graph_query_and_request_id() {
        let target = "/graphql/subscribe?graph=agent%3Aactor-a&query=subscription+%7B+Person+%7B+name+%7D+%7D";
        let canonical = canonical_subscription(&http(target, Some("Bearer eg2.00"))).unwrap();
        assert_eq!(canonical.request.id, 41);
        assert_eq!(canonical.request.graph, "agent:actor-a");
        assert_eq!(canonical.request.auth_token, "eg2.00");
        assert!(matches!(
            canonical.request.method,
            Method::GraphQl { ref query, variables: None }
                if query == "subscription { Person { name } }"
        ));
    }

    #[test]
    fn canonical_http_request_verifies_only_its_exact_current_eg2_envelope() {
        let target = "/graphql/subscribe?graph=agent%3Aactor-a&query=subscription+%7B+Person+%7B+name+%7D+%7D";
        let mut request = canonical_subscription(&http(target, Some("Bearer placeholder")))
            .unwrap()
            .request;
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let nonce = format!("graphql-sse-{}-{now}", std::process::id());
        let idempotency_key = format!("graphql-sse-request-{}-{now}", request.id);
        let context = RequestContextClaims {
            principal: "actor-a".into(),
            tenant: "tenant-shared".into(),
            audience: "epistemic-graph-test".into(),
            agent_id: "actor-a".into(),
            roles: vec!["test".into()],
            scopes: vec!["query:graphql".into()],
            policy_version: "policy-test".into(),
            delegation: Vec::new(),
            node: None,
            priority: None,
        };
        request.auth_token = compute_verified_envelope_token(
            "graphql-test-secret",
            &request,
            &VerifiedEnvelopeParams {
                context: &context,
                timestamp: now,
                nonce: &nonce,
                idempotency_key: &idempotency_key,
            },
        );

        let mut tampered = request.clone();
        tampered.graph = "agent:actor-b".into();
        assert!(verify_request_with_security_dir("graphql-test-secret", &tampered, None).is_err());
        let verified =
            verify_request_with_security_dir("graphql-test-secret", &request, None).unwrap();
        assert_eq!(verified.agent_id(), "actor-a");
        assert_eq!(verified.tenant(), "tenant-shared");
    }

    #[test]
    fn unsigned_and_query_string_token_shapes_are_rejected() {
        let target = "/graphql/subscribe?graph=__commons__&query=subscription%20%7B%20Person%20%7B%20name%20%7D%20%7D";
        assert!(canonical_subscription(&http(target, None)).is_err());
        let legacy = format!("{target}&token=eg2.00");
        assert!(canonical_subscription(&http(&legacy, None)).is_err());
    }

    #[test]
    fn duplicate_parameters_and_invalid_percent_escapes_are_rejected() {
        assert!(parse_form("graph=a&graph=b&query=x").is_err());
        assert!(parse_form("graph=a&query=%GG").is_err());
        assert!(parse_form("graph=a&query=%F0%28%8C%28").is_err());
    }

    #[test]
    fn parser_preflight_and_locked_policy_bound_subscription_work() {
        let deeply_nested = format!(
            "subscription {{ {} name {} }}",
            "edge { ".repeat(MAX_SYNTAX_NESTING + 1),
            " }".repeat(MAX_SYNTAX_NESTING + 1)
        );
        assert!(preflight_query_shape(&deeply_nested).is_err());
        assert!(LiveQuery::parse_with_policy(
            "subscription { Person { name } }",
            &subscription_policy(),
        )
        .is_err());
        assert!(LiveQuery::parse_with_policy(
            "subscription { Person(first: 5) { name } }",
            &subscription_policy(),
        )
        .is_ok());
        assert!(LiveQuery::parse_with_policy(
            "subscription { Person(first: 101) { name } }",
            &subscription_policy(),
        )
        .is_err());

        let mut fragment_chain = "subscription { ...F0 }\n".to_string();
        for depth in 0..=MAX_SYNTAX_NESTING {
            if depth == MAX_SYNTAX_NESTING {
                fragment_chain.push_str(&format!("fragment F{depth} on Person {{ name }}\n"));
            } else {
                fragment_chain.push_str(&format!(
                    "fragment F{depth} on Person {{ ...F{} }}\n",
                    depth + 1
                ));
            }
        }
        assert!(preflight_query_shape(&fragment_chain).is_ok());
        assert!(LiveQuery::parse_with_policy(&fragment_chain, &subscription_policy()).is_err());
    }

    #[test]
    fn current_subscription_uses_graphql_action_with_read_semantics() {
        let context = VerifiedRequestContext::verified_for_test("actor-a");
        let method = Method::GraphQl {
            query: "subscription { Person { name } }".into(),
            variables: None,
        };
        let policy = eg_capabilities::policy(&method);
        assert_eq!(policy.authz_action, "query:graphql");
        assert!(context.allows_method(policy.authz_action, false));
    }

    #[test]
    fn graph_acl_denies_a_peer_owned_graph() {
        let context = VerifiedRequestContext::verified_for_test("actor-a");
        assert!(graph_read_authority(
            &isolation(),
            &context,
            "agent:actor-b",
            GraphType::Agent,
            Some("actor-b"),
        )
        .is_err());
    }

    #[test]
    fn rls_projection_hides_peer_and_untagged_rows() {
        let isolation = isolation();
        let context = VerifiedRequestContext::verified_for_test("actor-a");
        let authority = graph_read_authority(
            &isolation,
            &context,
            "__commons__",
            GraphType::Commons,
            None,
        )
        .unwrap();
        let core = GraphCore::new();
        core.add_node(
            "owned".into(),
            properties(json!({
                "type": "Person", "name": "Owned",
                "_owner": "actor-a", "_visibility": "private"
            })),
        );
        core.add_node(
            "peer".into(),
            properties(json!({
                "type": "Person", "name": "Peer",
                "_owner": "actor-b", "_visibility": "private"
            })),
        );
        core.add_node(
            "public".into(),
            properties(json!({
                "type": "Person", "name": "Public", "_visibility": "public"
            })),
        );
        core.add_node(
            "untagged".into(),
            properties(json!({"type": "Person", "name": "Untagged"})),
        );
        let mut view = core.analysis_snapshot();
        authority.filter_view(&mut view);
        let live = LiveQuery::parse("subscription { Person { name } }").unwrap();
        let rendered = render(&live, &view).unwrap();
        assert!(rendered.contains("Owned"));
        assert!(rendered.contains("Public"));
        assert!(!rendered.contains("Peer"));
        assert!(!rendered.contains("Untagged"));
    }

    #[test]
    fn hidden_only_change_does_not_change_visible_payload() {
        let isolation = isolation();
        let context = VerifiedRequestContext::verified_for_test("actor-a");
        let authority = graph_read_authority(
            &isolation,
            &context,
            "__commons__",
            GraphType::Commons,
            None,
        )
        .unwrap();
        let core = GraphCore::new();
        core.add_node(
            "owned".into(),
            properties(json!({
                "type": "Person", "name": "Owned",
                "_owner": "actor-a", "_visibility": "private"
            })),
        );
        core.add_node(
            "peer".into(),
            properties(json!({
                "type": "Person", "name": "Before",
                "_owner": "actor-b", "_visibility": "private"
            })),
        );
        let live = LiveQuery::parse("subscription { Person { name } }").unwrap();
        let visible = |core: &GraphCore| {
            let mut view = core.analysis_snapshot();
            authority.filter_view(&mut view);
            render(&live, &view).unwrap()
        };
        let before = visible(&core);
        core.add_node(
            "peer".into(),
            properties(json!({
                "type": "Person", "name": "After",
                "_owner": "actor-b", "_visibility": "private"
            })),
        );
        assert_eq!(visible(&core), before);
    }

    #[test]
    fn config_is_fail_closed() {
        assert!(GraphQlSubscriptionConfig::new(0, 300).is_err());
        assert!(GraphQlSubscriptionConfig::new(4, 0).is_err());
        assert!(GraphQlSubscriptionConfig::new(MAX_CONNECTIONS_CEILING + 1, 300).is_err());
        assert!(GraphQlSubscriptionConfig::new(4, MAX_SESSION_SECS_CEILING + 1).is_err());
        assert_eq!(
            GraphQlSubscriptionConfig::new(4, 300).unwrap(),
            GraphQlSubscriptionConfig {
                max_connections: 4,
                max_session_secs: 300,
            }
        );
    }
}
