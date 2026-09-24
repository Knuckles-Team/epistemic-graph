//! The hand-rolled observability HTTP listener (no axum/hyper — the Pi
//! contract): framing, the fail-closed access gate, and route dispatch.

pub(super) mod search;

use std::sync::Arc;
use std::time::Duration;

use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;

use crate::server::http1::{self, HttpMessage, RequestLimits};

use super::parse::{doc_to_record, parse_es_bulk, parse_json_lines, parse_otlp_logs};
use super::{IngestOutcome, LogRecord, ObsState};
use search::handle_search;

/// Hard network bounds for the dependency-free observability HTTP listener.
pub(super) const MAX_HTTP_BODY_BYTES: usize = 16 * 1024 * 1024;
const HTTP_LIMITS: RequestLimits = RequestLimits {
    max_head_bytes: 64 * 1024,
    max_body_bytes: MAX_HTTP_BODY_BYTES,
};
const MAX_HTTP_CONNECTIONS: usize = 256;
const HTTP_READ_TIMEOUT: Duration = Duration::from_secs(30);

/// Read one framed HTTP/1.1 request and apply the observability surface's own
/// routing contract on top of [`http1`]'s framing: only the three methods this
/// listener serves, and a non-empty `Host` — which this surface has always
/// required on HTTP/1.1 and now also requires on HTTP/1.0, where it previously
/// accepted an empty one.
async fn read_request(stream: &mut tokio::net::TcpStream) -> Option<HttpMessage> {
    let request = http1::read_request(stream, HTTP_LIMITS).await?;
    if !matches!(request.method.as_str(), "GET" | "POST" | "OPTIONS")
        || request.header("host").is_empty()
    {
        return None;
    }
    Some(request)
}

/// Serve the observability log-ingestion HTTP surface on `listener`, backed by
/// `state`. One task per connection, one response per request, connection: close —
/// the SAME dependency-free idiom as the SPARQL / metrics listeners.
pub async fn serve(listener: TcpListener, state: Arc<ObsState>) {
    serve_inner(listener, state, None).await;
}

/// Production observability listener linked to the engine's live isolation
/// policy. The PromQL, trace and log-search routes have no verified request
/// envelope, so they can be failed closed as soon as secure/RLS policy activates.
pub async fn serve_with_security(
    listener: TcpListener,
    state: Arc<ObsState>,
    security_state: Arc<tokio::sync::RwLock<crate::server::ServerState>>,
) {
    // EH-408/EH-409: this store is also the one `TelemetryDerive` reads over
    // RPC, so the served engine registers it on its own state first.
    security_state.write().await.obs = Some(state.clone());
    serve_inner(listener, state, Some(security_state)).await;
}

pub(super) async fn serve_inner(
    listener: TcpListener,
    state: Arc<ObsState>,
    security_state: Option<Arc<tokio::sync::RwLock<crate::server::ServerState>>>,
) {
    let connections = Arc::new(tokio::sync::Semaphore::new(MAX_HTTP_CONNECTIONS));
    loop {
        let Ok((mut stream, _)) = listener.accept().await else {
            continue;
        };
        let Ok(connection_permit) = connections.clone().try_acquire_owned() else {
            // Drop excess sockets immediately. Spawning a rejection task here
            // would recreate the same unbounded-task resource exhaustion.
            drop(stream);
            continue;
        };
        let state = state.clone();
        let security_state = security_state.clone();
        tokio::spawn(async move {
            let _connection_permit = connection_permit;
            let (status, ctype, body) =
                match tokio::time::timeout(HTTP_READ_TIMEOUT, read_request(&mut stream)).await {
                    Ok(Some(req)) => handle(&state, security_state.as_ref(), req).await,
                    Ok(None) => (
                        "400 Bad Request",
                        "text/plain",
                        "malformed HTTP request".to_string(),
                    ),
                    Err(_) => (
                        "408 Request Timeout",
                        "text/plain",
                        "request read timeout".to_string(),
                    ),
                };
            let resp = format!(
                "HTTP/1.1 {status}\r\ncontent-type: {ctype}\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                body.len()
            );
            let _ = stream.write_all(resp.as_bytes()).await;
            let _ = stream.shutdown().await;
        });
    }
}

/// Every distinct operation the observability surface serves, classified by
/// **route**, never by HTTP-verb shape (GOC-62-keycloak-auth-standard.md §5:
/// "classify by declared operation semantics ... never by guessing from the
/// verb, the path shape, or which paths happened to be enumerated when the
/// gate was written").
///
/// BUG-037 (P0): the prior gate (`is_observability_read_carrier`) matched
/// `method == "GET"` plus a handful of named query-shaped paths. Every
/// ingest path this module routes below — `POST /v1/logs` (OTLP), `POST
/// /_bulk`/`POST */_bulk` (ES bulk), `POST /<stream>/_doc` (ES single-doc),
/// `POST /`/`/api/logs`/`/logs` (JSON-lines), `POST /v1/traces` (OTLP trace
/// ingest), and `POST /api/v1/write` (Prometheus `remote_write`, feature
/// `otel-export`) — is a `Method::POST` that names none of the read paths,
/// so the gate was never reached for any of them, in every deployment
/// configuration including `serve_with_security`. A mutation must be
/// authorized AT LEAST as strictly as a read, never less (same doc, same
/// section).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ObsOperation {
    /// A query/search/label-listing op: PromQL (`/api/v1/query*`,
    /// `/api/v1/labels`, `/api/v1/label/*`), the EG-162 `_search` surface
    /// (SQL or structured, any of `/api/_search`, `/_search`,
    /// `*/_search`), or trace search/assembly/dependency-graph reads
    /// (`/api/traces`, `/api/traces/*`, `/api/dependencies`,
    /// `/api/services/dependencies`).
    Read,
    /// An ingest/write op — every other path this listener accepts (see the
    /// enumeration above), and the fail-closed default for any path this
    /// classifier does not explicitly recognize as a `Read`: a surface that
    /// cannot mint a caller must refuse, never proceed
    /// (GOC-62-keycloak-auth-standard.md §4), so an as-yet-unenumerated path
    /// gets the HIGHER obligation rather than silently passing through the
    /// way the old GET-shaped allowlist did.
    Mutation,
}

/// Classify `path` (already stripped of query string) as a [`ObsOperation`].
/// Never called for the two no-data control requests `handle` answers
/// BEFORE reaching the gate — CORS preflight (`OPTIONS`) and the health
/// probe (`GET /healthz` / `GET /`) — so every path this function sees
/// carries observability data one way or the other.
fn classify_observability_operation(path: &str) -> ObsOperation {
    if is_read_only_obs_path(path) {
        ObsOperation::Read
    } else {
        ObsOperation::Mutation
    }
}

/// The read-only observability endpoints: PromQL/labels queries, search, traces, and
/// service-dependency lookups. A table-driven rewrite of
/// [`classify_observability_operation`]'s membership test (same predicate: exact path,
/// prefix, or `/_search` suffix).
fn is_read_only_obs_path(path: &str) -> bool {
    const EXACT: &[&str] = &[
        "/api/v1/labels",
        "/api/_search",
        "/_search",
        "/api/traces",
        "/api/dependencies",
        "/api/services/dependencies",
    ];
    const PREFIXES: &[&str] = &["/api/v1/query", "/api/v1/label/", "/api/traces/"];

    EXACT.contains(&path)
        || PREFIXES.iter().any(|prefix| path.starts_with(prefix))
        || path.ends_with("/_search")
}

async fn observability_access_denied(
    security_state: Option<&Arc<tokio::sync::RwLock<crate::server::ServerState>>>,
) -> bool {
    if security_state.is_none() {
        return false;
    }
    // A18: neither observability reads NOR ingest/mutations carry a
    // credential this surface can verify yet (no `eg2.` envelope, bearer
    // token, or other proof), so no `CarrierAuthority` can ever be minted
    // here today; this always denies under `serve_with_security`, honestly
    // (via the real check) rather than via the old unconditional stub —
    // and, per BUG-037, applies identically to both `ObsOperation` arms.
    crate::server::access::unauthenticated_carrier_denied(None)
}

/// Route + execute an ingest request → `(status, content_type, body)`.
pub(super) async fn handle(
    state: &Arc<ObsState>,
    security_state: Option<&Arc<tokio::sync::RwLock<crate::server::ServerState>>>,
    req: HttpMessage,
) -> (&'static str, &'static str, String) {
    let (path, query) = req.path_and_query();
    let body = req.text();
    if let Some(resp) = control_response(&req.method, path) {
        return resp;
    }
    // BUG-037: gate every remaining path (both `Read` and `Mutation`) — not
    // just the ones that happen to be GET/query-shaped. `handle`'s two
    // no-data control returns (OPTIONS above, the health probe just above)
    // already ran, so anything reaching this point genuinely serves
    // observability data one way or the other.
    if observability_access_denied(security_state).await {
        return access_denied_response(classify_observability_operation(path));
    }

    // CONCEPT:EG-KG.query.prometheus-http-query-api — the Prometheus HTTP query API (GET or POST), routed BEFORE the
    // POST-only ingest guard (instant queries are typically GET). Gated on `promql`,
    // which implies `obs`; absent that feature these paths fall through to 404.
    #[cfg(feature = "promql")]
    if let Some(resp) = try_promql_route(state, &req.method, path, query, &body).await {
        return resp;
    }

    // CONCEPT:EG-OS.observability.trace-assembly — distributed-trace surface (GET or POST), routed BEFORE the
    // POST-only ingest guard (trace SEARCH / assembly / dependency-graph are GET).
    // Gated on `traces`, which implies `obs`; absent that feature these paths fall
    // through to 404. Covers OTLP-JSON ingest (`POST /v1/traces`), trace search
    // (`/api/traces`), single-trace assembly (`/api/traces/<id>`) and the
    // service-dependency graph (`/api/dependencies`).
    #[cfg(feature = "traces")]
    if let Some(resp) = try_traces_route(state, &req.method, path, query, &body).await {
        return resp;
    }

    // CONCEPT:EG-OS.observability.prometheus-ingest — the Prometheus `remote_write` receiver (`POST /api/v1/write`):
    // decode the snappy-compressed protobuf WriteRequest from the RAW body bytes (the
    // lossy-UTF-8 `body` String would corrupt the binary) and land its samples in the
    // durable eg-tsdb SeriesStore. Gated on `otel-export`; absent the feature this path
    // falls through to the unknown-ingest 404.
    #[cfg(feature = "otel-export")]
    if let Some(resp) = try_otel_write_route(state, &req.method, path, &req.body).await {
        return resp;
    }

    handle_ingest(state, &req, path, query, &body).await
}

/// The POST-only ingest path: `_search` (EG-162), then route-by-shape log ingest.
/// Extracted from [`handle`]'s tail — everything after the GET-friendly gated routes.
async fn handle_ingest(
    state: &Arc<ObsState>,
    req: &HttpMessage,
    path: &str,
    query: &str,
    body: &str,
) -> (&'static str, &'static str, String) {
    if req.method != "POST" {
        return (
            "405 Method Not Allowed",
            "text/plain",
            "POST only".to_string(),
        );
    }

    // EG-162 search surface: O2/Elasticsearch `_search`-shaped query API. Routed
    // BEFORE ingest (no ingest path ends with `_search`).
    if path == "/api/_search" || path == "/_search" || path.ends_with("/_search") {
        return handle_search(state, path, query, body).await;
    }

    // The `stream` query param is the default stream for shapes that don't name one.
    let default_stream = query_param(query, "stream").unwrap_or_else(|| "default".to_string());

    // Route by path → parse into records + choose the response shape.
    let Some((records, shape)) = route_ingest_records(path, body, &default_stream) else {
        return (
            "404 Not Found",
            "text/plain",
            "unknown ingest path".to_string(),
        );
    };

    let records = match records {
        Ok(r) => r,
        Err(e) => return ("400 Bad Request", "text/plain", e),
    };

    // Ingest OFF the reactor (redb + Tantivy commit are blocking).
    let st = state.clone();
    let outcome = ::tokio::task::spawn_blocking(move || st.ingest(records)).await;
    match outcome {
        Ok(Ok(o)) => format_ingest_success(shape, &o),
        Ok(Err(error)) => {
            tracing::warn!(%error, "observability ingest failed");
            (
                "500 Internal Server Error",
                "text/plain",
                "observability ingest failed".to_string(),
            )
        }
        Err(_) => (
            "500 Internal Server Error",
            "text/plain",
            "observability ingest worker failed".to_string(),
        ),
    }
}

/// The two no-data control responses [`handle`] answers BEFORE the access-denied gate:
/// CORS preflight (`OPTIONS`) and the health probe (`GET /healthz` / `GET /`). `None`
/// for anything else (falls through to the gated routes). Extracted from [`handle`].
fn control_response(method: &str, path: &str) -> Option<(&'static str, &'static str, String)> {
    if method == "OPTIONS" {
        return Some(("204 No Content", "text/plain", String::new()));
    }
    if path == "/healthz" || path == "/" && method == "GET" {
        return Some(("200 OK", "text/plain", "ok".to_string()));
    }
    None
}

/// The literal 403 response body for a denied observability request — per-operation
/// text (not a `format!` interpolation) so the exact denial string stays greppable
/// verbatim in source, matching every other carrier's static denial string
/// (`scripts/check_universal_read_rls.py`). Extracted from [`handle`] (BUG-037: the
/// SAME check gates both `ObsOperation` arms).
fn access_denied_response(op: ObsOperation) -> (&'static str, &'static str, String) {
    let body = match op {
        ObsOperation::Read => {
            "ACCESS_DENIED: observability read carriers require verified tenant ownership"
        }
        ObsOperation::Mutation => {
            "ACCESS_DENIED: observability ingest carriers require verified tenant ownership"
        }
    };
    ("403 Forbidden", "text/plain", body.to_string())
}

/// The Prometheus HTTP query API route (`promql` feature). `None` when `path` doesn't
/// match, so [`handle`] falls through to its next route. Extracted from [`handle`].
#[cfg(feature = "promql")]
async fn try_promql_route(
    state: &Arc<ObsState>,
    method: &str,
    path: &str,
    query: &str,
    body: &str,
) -> Option<(&'static str, &'static str, String)> {
    if path.starts_with("/api/v1/query")
        || path == "/api/v1/labels"
        || path.starts_with("/api/v1/label/")
    {
        return Some(crate::server::promql::handle(state, method, path, query, body).await);
    }
    None
}

/// The distributed-trace surface route (`traces` feature): OTLP-JSON ingest, trace
/// search/assembly, and the service-dependency graph. `None` when `path` doesn't match.
/// Extracted from [`handle`].
#[cfg(feature = "traces")]
async fn try_traces_route(
    state: &Arc<ObsState>,
    method: &str,
    path: &str,
    query: &str,
    body: &str,
) -> Option<(&'static str, &'static str, String)> {
    if path == "/v1/traces"
        || path == "/api/traces"
        || path.starts_with("/api/traces/")
        || path == "/api/dependencies"
        || path == "/api/services/dependencies"
    {
        return Some(crate::server::traces::handle(state, method, path, query, body).await);
    }
    None
}

/// The Prometheus `remote_write` receiver route (`otel-export` feature). `None` when
/// `path` doesn't match. Extracted from [`handle`].
#[cfg(feature = "otel-export")]
async fn try_otel_write_route(
    state: &Arc<ObsState>,
    method: &str,
    path: &str,
    body_bytes: &[u8],
) -> Option<(&'static str, &'static str, String)> {
    if path == "/api/v1/write" {
        return Some(super::remote_write::handle(state, method, body_bytes).await);
    }
    None
}

/// The routed log-ingest shape [`handle`] parses `body` into, driving both which parser
/// runs and which success response format [`format_ingest_success`] uses.
enum IngestShape {
    Otlp,
    EsBulk,
    EsDoc,
    Lines,
}

/// Route `path` to its ingest parser, producing the parsed records (or the parse error)
/// plus which [`IngestShape`] to format the success response as. `None` when `path`
/// matches no known ingest shape (→ 404). Extracted from [`handle`].
fn route_ingest_records(
    path: &str,
    body: &str,
    default_stream: &str,
) -> Option<(Result<Vec<LogRecord>, String>, IngestShape)> {
    if path == "/v1/logs" {
        return Some((parse_otlp_logs(body, default_stream), IngestShape::Otlp));
    }
    if path == "/_bulk" || path.ends_with("/_bulk") {
        return Some((Ok(parse_es_bulk(body, default_stream)), IngestShape::EsBulk));
    }
    if let Some(stream) = es_doc_stream(path) {
        // `/<stream>/_doc` — a single ES document.
        let doc: Result<serde_json::Value, String> =
            serde_json::from_str(body).map_err(|e| format!("parse _doc JSON: {e}"));
        return Some((
            doc.map(|d| vec![doc_to_record(&d, &stream)]),
            IngestShape::EsDoc,
        ));
    }
    if path == "/" || path == "/api/logs" || path == "/logs" {
        return Some((
            Ok(parse_json_lines(body, default_stream)),
            IngestShape::Lines,
        ));
    }
    None
}

/// Format the ingest success response for one [`IngestShape`]. Extracted from
/// [`handle`]'s tail.
fn format_ingest_success(
    shape: IngestShape,
    outcome: &IngestOutcome,
) -> (&'static str, &'static str, String) {
    match shape {
        IngestShape::Otlp => (
            "200 OK",
            "application/json",
            "{\"partialSuccess\":{}}".to_string(),
        ),
        IngestShape::EsBulk => (
            "200 OK",
            "application/json",
            es_bulk_response(outcome.accepted),
        ),
        IngestShape::EsDoc => (
            "201 Created",
            "application/json",
            "{\"result\":\"created\",\"_shards\":{\"total\":1,\"successful\":1,\"failed\":0}}"
                .to_string(),
        ),
        IngestShape::Lines => (
            "200 OK",
            "application/json",
            format!(
                "{{\"successful\":{},\"failed\":0,\"segments\":{}}}",
                outcome.accepted, outcome.segments_flushed
            ),
        ),
    }
}

/// An ES `_bulk` response: `errors:false` with one `index`/`created` item per doc.
fn es_bulk_response(n: usize) -> String {
    let items: Vec<serde_json::Value> = (0..n)
        .map(|_| serde_json::json!({"index":{"status":201,"result":"created"}}))
        .collect();
    serde_json::json!({ "took": 0, "errors": false, "items": items }).to_string()
}

/// If `path` is `/<stream>/_doc` (optionally `/<stream>/_doc/<id>`), return `<stream>`.
pub(super) fn es_doc_stream(path: &str) -> Option<String> {
    let trimmed = path.trim_start_matches('/');
    let mut parts = trimmed.split('/');
    let stream = parts.next()?;
    let doc_marker = parts.next()?;
    if stream.is_empty() || doc_marker != "_doc" {
        return None;
    }
    Some(stream.to_string())
}

/// Extract a query-string parameter value (no percent-decoding needed for a bare
/// stream name; kept minimal).
fn query_param(query: &str, key: &str) -> Option<String> {
    for pair in query.split('&') {
        if let Some((k, v)) = pair.split_once('=') {
            if k == key && !v.is_empty() {
                return Some(v.to_string());
            }
        }
    }
    None
}
