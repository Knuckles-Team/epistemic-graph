//! W3C SPARQL 1.1 Protocol HTTP endpoint (CONCEPT:EG-017, feature `sparql-http`).
//!
//! A minimal, dependency-free HTTP/1.1 listener (the SAME hand-rolled idiom as the
//! Prometheus `--metrics-addr` exposition — no axum/hyper/warp, so the Pi contract
//! holds) that speaks the standard SPARQL protocol so an EXISTING Stardog/Jena/rdflib
//! SPARQL client can point at us UNCHANGED:
//!
//!   * `GET  /sparql?query=…`                          → SELECT/ASK/CONSTRUCT/DESCRIBE
//!   * `POST /sparql`  `application/sparql-query`      → query (body = the query)
//!   * `POST /sparql`  `application/sparql-update`     → UPDATE (body = the update)
//!   * `POST /sparql`  `application/x-www-form-urlencoded` with `query=` or `update=`
//!
//! Result media types are content-negotiated (CONCEPT:EG-050) from the `Accept` header
//! (with an `output=`/`format=` query-param override): SELECT/ASK serve SPARQL-results
//! JSON (default), XML, CSV or TSV; CONSTRUCT/DESCRIBE serve N-Triples (default) or
//! Turtle. With no `Accept` header the per-form default is used (byte-identical to the
//! prior fixed behavior). The default
//! graph is `?default-graph-uri=` (or `EPISTEMIC_GRAPH_SPARQL_DEFAULT_GRAPH`, else
//! `__commons__`); EVERY registry graph is exposed as a named graph so `GRAPH <name>{}`
//! and `GRAPH ?g{}` work across the engine's graphs.
//!
//! Durability note: queries run off an off-lock snapshot (read-only). UPDATEs apply
//! straight through the live graph cores (so they are visible immediately and persisted
//! by the engine's checkpoint of dirty graphs) — they do NOT ride the per-op WAL; for
//! crash-immediate durability use the wire methods (`AddTriples`/`RemoveTriples`/
//! `ApplyMutation`). The endpoint is the interop convenience surface.

use std::collections::HashMap;
use std::sync::Arc;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::RwLock;

use crate::graph::GraphCore;
use crate::protocol::GraphType;
use crate::server::ServerState;
use eg_rdf::sparql::{Binding, Dataset, Projection, QueryOutcome, SparqlResult};
use eg_rdf::update::GraphStore;

/// Env var naming the default-graph the endpoint resolves a bare query against.
pub const DEFAULT_GRAPH_ENV: &str = "EPISTEMIC_GRAPH_SPARQL_DEFAULT_GRAPH";
/// Env var carrying the bind address (`host:port`) when `--sparql-addr` is not passed.
pub const SPARQL_ADDR_ENV: &str = "EPISTEMIC_GRAPH_SPARQL_ADDR";
/// SSRF allowlist for outbound `SERVICE` federation (CONCEPT:EG-052, feature
/// `sparql-service`): a comma-separated set of allowed endpoint hosts / `scheme://host:port`
/// origins. **Empty / unset ⇒ SERVICE is DISABLED (fail-closed)** — no remote client is
/// bound, so a `SERVICE <ep> { … }` clause errors (or, under `SERVICE SILENT`, yields the
/// empty solution). A host resolving to a loopback/link-local/RFC-1918 address is refused
/// unless the allowlist names that exact host literally.
pub const SERVICE_ALLOW_ENV: &str = "EPISTEMIC_GRAPH_SPARQL_SERVICE_ALLOW";

/// Serve the SPARQL 1.1 HTTP protocol on `listener`, backed by the engine `state`.
pub async fn serve(listener: TcpListener, state: Arc<RwLock<ServerState>>) {
    loop {
        let Ok((mut stream, _)) = listener.accept().await else {
            continue;
        };
        let state = state.clone();
        tokio::spawn(async move {
            let (status, ctype, body) = match read_request(&mut stream).await {
                Some(req) => handle(&state, req).await,
                None => (
                    "400 Bad Request",
                    "text/plain",
                    "malformed HTTP request".to_string(),
                ),
            };
            let resp = format!(
                "HTTP/1.1 {status}\r\ncontent-type: {ctype}\r\ncontent-length: {}\r\naccess-control-allow-origin: *\r\naccess-control-allow-headers: content-type, accept\r\nconnection: close\r\n\r\n{body}",
                body.len()
            );
            let _ = stream.write_all(resp.as_bytes()).await;
            let _ = stream.shutdown().await;
        });
    }
}

/// A parsed HTTP request: method, raw target (`/sparql?…`), headers, body.
struct HttpRequest {
    method: String,
    target: String,
    content_type: String,
    accept: String,
    body: String,
}

/// Read one HTTP/1.1 request: headers up to the blank line, then `Content-Length` body.
async fn read_request(stream: &mut tokio::net::TcpStream) -> Option<HttpRequest> {
    let mut buf = Vec::new();
    let mut tmp = [0u8; 4096];
    // Read until the header terminator is seen (bounded).
    let header_end = loop {
        if let Some(pos) = find_subslice(&buf, b"\r\n\r\n") {
            break pos;
        }
        let n = stream.read(&mut tmp).await.ok()?;
        if n == 0 {
            return None;
        }
        buf.extend_from_slice(&tmp[..n]);
        if buf.len() > 16 * 1024 * 1024 {
            return None; // header flood guard
        }
    };
    let head = String::from_utf8_lossy(&buf[..header_end]).to_string();
    let mut lines = head.split("\r\n");
    let request_line = lines.next()?;
    let mut parts = request_line.split_whitespace();
    let method = parts.next()?.to_string();
    let target = parts.next()?.to_string();

    let mut content_length = 0usize;
    let mut content_type = String::new();
    let mut accept = String::new();
    for line in lines {
        if let Some((k, v)) = line.split_once(':') {
            let key = k.trim().to_ascii_lowercase();
            let val = v.trim();
            match key.as_str() {
                "content-length" => content_length = val.parse().unwrap_or(0),
                "content-type" => content_type = val.to_ascii_lowercase(),
                "accept" => accept = val.to_string(),
                _ => {}
            }
        }
    }

    // Body: whatever followed the header terminator, plus any remaining Content-Length.
    let mut body = buf[header_end + 4..].to_vec();
    while body.len() < content_length {
        let n = stream.read(&mut tmp).await.ok()?;
        if n == 0 {
            break;
        }
        body.extend_from_slice(&tmp[..n]);
    }
    if content_length > 0 && body.len() > content_length {
        body.truncate(content_length);
    }
    Some(HttpRequest {
        method,
        target,
        content_type,
        accept,
        body: String::from_utf8_lossy(&body).to_string(),
    })
}

/// Route + execute a request → `(status, content_type, body)`.
async fn handle(
    state: &Arc<RwLock<ServerState>>,
    req: HttpRequest,
) -> (&'static str, &'static str, String) {
    let (path, query_string) = match req.target.split_once('?') {
        Some((p, q)) => (p, q),
        None => (req.target.as_str(), ""),
    };
    if req.method == "OPTIONS"
        && (path.starts_with("/sparql") || path.starts_with("/rdf-graphs") || path == "/nl")
    {
        return ("204 No Content", "text/plain", String::new());
    }
    // Natural-language query facade route (CONCEPT:EG-080, feature `nl-query`): POST
    // `{text, graph}` → the NL planner → UQL → executed rows as JSON. Served on the SAME
    // hand-rolled HTTP facade listener as `/sparql` (no new HTTP dep). A build without
    // `nl-query` has no `/nl` route (it 404s below like any other unknown path).
    #[cfg(feature = "nl-query")]
    if path == "/nl" {
        return handle_nl(state, &req).await;
    }
    // W3C SPARQL 1.1 Graph Store HTTP Protocol (CONCEPT:EG-134) — direct graph management.
    if path.starts_with("/rdf-graphs") {
        return handle_graph_store(state, &req, path, query_string).await;
    }
    if !path.starts_with("/sparql") {
        return ("404 Not Found", "text/plain", "not found".to_string());
    }
    let params = parse_form(query_string);

    // Decide query vs update + extract the text and the default-graph hint.
    let (is_update, text) = if req.method == "GET" {
        (false, params.get("query").cloned().unwrap_or_default())
    } else if req.method == "POST" {
        if req.content_type.contains("application/sparql-update") {
            (true, req.body.clone())
        } else if req.content_type.contains("application/sparql-query") {
            (false, req.body.clone())
        } else if req
            .content_type
            .contains("application/x-www-form-urlencoded")
        {
            let form = parse_form(&req.body);
            if let Some(u) = form.get("update") {
                (true, u.clone())
            } else {
                (false, form.get("query").cloned().unwrap_or_default())
            }
        } else {
            return (
                "415 Unsupported Media Type",
                "text/plain",
                "use application/sparql-query, application/sparql-update or form-encoded"
                    .to_string(),
            );
        }
    } else {
        return ("405 Method Not Allowed", "text/plain", "method".to_string());
    };

    if text.trim().is_empty() {
        return (
            "400 Bad Request",
            "text/plain",
            "empty query/update".to_string(),
        );
    }

    let default_graph = params
        .get("default-graph-uri")
        .cloned()
        .or_else(|| std::env::var(DEFAULT_GRAPH_ENV).ok())
        .unwrap_or_else(|| "__commons__".to_string());

    if is_update {
        match run_update(state, &text, &default_graph).await {
            Ok(()) => ("204 No Content", "text/plain", String::new()),
            Err(e) => ("400 Bad Request", "text/plain", e),
        }
    } else {
        // `output=`/`format=` query-param override wins over the Accept header (EG-050).
        let fmt_override = params
            .get("output")
            .or_else(|| params.get("format"))
            .map(|s| s.as_str());
        run_query(state, &text, &default_graph, &req.accept, fmt_override).await
    }
}

/// Natural-language query facade route (CONCEPT:EG-080). Accepts a JSON body
/// `{"text": "...", "graph": "..."}` (graph optional — defaults to the SPARQL default
/// graph), builds an AUTHENTICATED in-process `Method::NlQuery` request, and runs it
/// through the FULL dispatch path (the planner + RLS + the deterministic
/// `UnifiedQueryText` pipeline) — so the HTTP route and the wire method share ONE code
/// path. The executed `[id, score]` rows are returned as JSON.
#[cfg(feature = "nl-query")]
async fn handle_nl(
    state: &Arc<RwLock<ServerState>>,
    req: &HttpRequest,
) -> (&'static str, &'static str, String) {
    if req.method != "POST" {
        return (
            "405 Method Not Allowed",
            "application/json",
            r#"{"error":"POST a JSON body {\"text\":\"…\",\"graph\":\"…\"} to /nl"}"#.to_string(),
        );
    }
    let body: serde_json::Value = match serde_json::from_str(&req.body) {
        Ok(v) => v,
        Err(e) => {
            return (
                "400 Bad Request",
                "application/json",
                serde_json::json!({ "error": format!("invalid JSON body: {e}") }).to_string(),
            )
        }
    };
    let text = body
        .get("text")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    if text.trim().is_empty() {
        return (
            "400 Bad Request",
            "application/json",
            r#"{"error":"missing non-empty 'text'"}"#.to_string(),
        );
    }
    let graph = body
        .get("graph")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .or_else(|| std::env::var(DEFAULT_GRAPH_ENV).ok())
        .unwrap_or_else(|| "__commons__".to_string());

    // Authenticated in-process dispatch — the `/nl` facade is a trusted local surface
    // (like `/sparql`), so it mints a valid token for the engine's own secret rather than
    // bypassing auth.
    let secret = { state.read().await.auth_secret.clone() };
    let id = 1u64;
    let request = crate::protocol::Request {
        id,
        graph: graph.clone(),
        auth_token: crate::server::compute_auth_token(&secret, id),
        agent_id: None,
        method: crate::protocol::Method::NlQuery { text, graph },
    };
    let resp = crate::server::dispatch(state, request).await;
    if let Some(err) = resp.error {
        return (
            "400 Bad Request",
            "application/json",
            serde_json::json!({ "error": err }).to_string(),
        );
    }
    let rows = match resp.result {
        Some(crate::protocol::ResultPayload::Raw(bytes)) => {
            rmp_serde::from_slice::<Vec<(String, Option<f32>)>>(&bytes).unwrap_or_default()
        }
        _ => Vec::new(),
    };
    let rows_json: Vec<serde_json::Value> = rows
        .into_iter()
        .map(|(id, score)| serde_json::json!({ "id": id, "score": score }))
        .collect();
    (
        "200 OK",
        "application/json",
        serde_json::json!({ "rows": rows_json }).to_string(),
    )
}

/// Execute a query over an off-lock dataset snapshot of every registry graph.
async fn run_query(
    state: &Arc<RwLock<ServerState>>,
    query: &str,
    default_graph: &str,
    accept: &str,
    fmt_override: Option<&str>,
) -> (&'static str, &'static str, String) {
    // Gather cores under a brief read lock, then snapshot off-lock.
    let (default_core, named_cores) = {
        let s = state.read().await;
        let default_core = s
            .registry
            .get(default_graph)
            .map(|e| e.core.clone())
            .unwrap_or_else(|| Arc::new(GraphCore::new()));
        let named: Vec<(String, Arc<GraphCore>)> = s
            .registry
            .list()
            .into_iter()
            .filter_map(|(name, _)| s.registry.get(&name).map(|e| (name, e.core.clone())))
            .collect();
        (default_core, named)
    };
    let query = query.to_string();
    let accept = accept.to_string();
    let outcome = tokio::task::spawn_blocking(move || {
        let default_view = default_core.analysis_snapshot();
        let named_views: Vec<(String, eg_core::graph::GraphView)> = named_cores
            .iter()
            .map(|(n, c)| (n.clone(), c.analysis_snapshot()))
            .collect();
        let named_refs: Vec<(String, &eg_core::graph::GraphView)> =
            named_views.iter().map(|(n, v)| (n.clone(), v)).collect();
        let ds = Dataset::new(&default_view, named_refs);
        run_dataset_query(&ds, &query)
    })
    .await;

    match outcome {
        Ok(Ok(QueryOutcome::Solutions(r))) => {
            let ct = choose_ct(&accept, fmt_override, SELECT_FORMS);
            let body = match ct {
                "application/sparql-results+xml" => results_xml(&r),
                "text/csv" => results_csv(&r),
                "text/tab-separated-values" => results_tsv(&r),
                _ => select_json(&r),
            };
            ("200 OK", ct, body)
        }
        Ok(Ok(QueryOutcome::Boolean(b))) => {
            let ct = choose_ct(&accept, fmt_override, SELECT_FORMS);
            ("200 OK", ct, boolean_body(ct, b))
        }
        Ok(Ok(QueryOutcome::Graph(triples))) => {
            let ct = choose_ct(&accept, fmt_override, GRAPH_FORMS);
            let ser = if ct == "text/turtle" {
                eg_rdf::mapping::to_turtle(&triples)
            } else {
                eg_rdf::mapping::to_ntriples(&triples)
            };
            match ser {
                Ok(body) => ("200 OK", ct, body),
                Err(e) => ("500 Internal Server Error", "text/plain", e),
            }
        }
        Ok(Err(e)) => (
            "400 Bad Request",
            "text/plain",
            format!("SPARQL error: {e}"),
        ),
        Err(e) => (
            "500 Internal Server Error",
            "text/plain",
            format!("compute task failed: {e}"),
        ),
    }
}

/// Evaluate a parsed dataset query, binding the outbound `SERVICE` client when the
/// `sparql-service` feature is on AND the SSRF allowlist is non-empty (CONCEPT:EG-052).
/// Otherwise (feature off, or allowlist empty) NO client is bound — SERVICE is fail-closed.
/// Runs inside the caller's `spawn_blocking` (the `ureq` client is blocking).
fn run_dataset_query(ds: &Dataset, query: &str) -> Result<QueryOutcome, String> {
    #[cfg(feature = "sparql-service")]
    {
        let client = ServiceClient::from_env();
        let svc: Option<&dyn eg_rdf::sparql::RemoteSparql> =
            client.as_ref().map(|c| c as &dyn eg_rdf::sparql::RemoteSparql);
        eg_rdf::sparql::run_outcome_dataset_service(ds, query, &Projection::raw(), svc)
    }
    #[cfg(not(feature = "sparql-service"))]
    {
        eg_rdf::sparql::run_outcome_dataset(ds, query, &Projection::raw())
    }
}

// ── SPARQL SERVICE federation client (CONCEPT:EG-052, feature `sparql-service`) ───

/// A `ureq`-backed [`eg_rdf::sparql::RemoteSparql`] with an SSRF allowlist. Reuses the SAME
/// pure-Rust rustls `ureq` stack `federation` already links (no new crate enters the tree).
#[cfg(feature = "sparql-service")]
struct ServiceClient {
    /// Allowed hosts / `scheme://host:port` origins (lower-cased), from `SERVICE_ALLOW_ENV`.
    allow: Vec<String>,
}

#[cfg(feature = "sparql-service")]
impl ServiceClient {
    /// Bounded HTTP timeouts + a response-size cap (a hostile/misbehaving endpoint must not
    /// hang or OOM the blocking pool).
    const CONNECT_TIMEOUT_SECS: u64 = 5;
    const READ_TIMEOUT_SECS: u64 = 30;
    const MAX_RESPONSE_BYTES: u64 = 64 * 1024 * 1024;

    /// Build from `SERVICE_ALLOW_ENV`. Empty / unset ⇒ `None` (SERVICE disabled, fail-closed).
    fn from_env() -> Option<Self> {
        let raw = std::env::var(SERVICE_ALLOW_ENV).ok()?;
        let allow: Vec<String> = raw
            .split(',')
            .map(|s| s.trim().to_ascii_lowercase())
            .filter(|s| !s.is_empty())
            .collect();
        if allow.is_empty() {
            None
        } else {
            Some(Self { allow })
        }
    }

    /// SSRF guard: the endpoint's scheme must be http/https, its bare host must be in the
    /// allowlist, and it must not resolve to a loopback/link-local/private/unspecified
    /// address UNLESS that exact host string is itself an allowlisted IP literal (an
    /// operator opt-in for an internal endpoint).
    fn check_endpoint(&self, endpoint: &str) -> Result<(), String> {
        let rest = endpoint
            .strip_prefix("https://")
            .or_else(|| endpoint.strip_prefix("http://"))
            .ok_or_else(|| format!("endpoint must be http(s): '{endpoint}'"))?;
        // Strip any path/query/fragment, then split an optional `:port`.
        let authority = rest
            .split(['/', '?', '#'])
            .next()
            .unwrap_or("")
            .rsplit('@') // drop any userinfo
            .next()
            .unwrap_or("");
        let (host, port): (&str, u16) = match authority.rsplit_once(':') {
            // Guard against IPv6 literals `[::1]:80` — only treat the tail as a port if numeric.
            Some((h, p)) if p.chars().all(|c| c.is_ascii_digit()) && !p.is_empty() => {
                (h, p.parse().unwrap_or(0))
            }
            _ => (authority, if endpoint.starts_with("https://") { 443 } else { 80 }),
        };
        let host = host.trim_start_matches('[').trim_end_matches(']');
        if host.is_empty() {
            return Err(format!("endpoint has no host: '{endpoint}'"));
        }
        let host_lc = host.to_ascii_lowercase();
        let allowed = self.allow.iter().any(|a| {
            *a == host_lc
                || *a == format!("{host_lc}:{port}")
                || *a == format!("http://{host_lc}")
                || *a == format!("https://{host_lc}")
                || *a == format!("http://{host_lc}:{port}")
                || *a == format!("https://{host_lc}:{port}")
        });
        if !allowed {
            return Err(format!("SSRF guard: host '{host}' not in allowlist"));
        }
        // Resolve + reject internal ranges (unless the host itself is an allowlisted IP).
        use std::net::ToSocketAddrs;
        let host_is_allowlisted_literal = host.parse::<std::net::IpAddr>().is_ok();
        let addrs = (host, port)
            .to_socket_addrs()
            .map_err(|e| format!("SSRF guard: cannot resolve '{host}': {e}"))?;
        for sa in addrs {
            if is_blocked_ip(&sa.ip()) && !host_is_allowlisted_literal {
                return Err(format!(
                    "SSRF guard: host '{host}' resolves to internal address {}",
                    sa.ip()
                ));
            }
        }
        Ok(())
    }
}

/// An internal (SSRF-sensitive) IP: loopback, unspecified, link-local, or RFC-1918 /
/// unique-local private space.
#[cfg(feature = "sparql-service")]
fn is_blocked_ip(ip: &std::net::IpAddr) -> bool {
    match ip {
        std::net::IpAddr::V4(v4) => {
            v4.is_loopback()
                || v4.is_unspecified()
                || v4.is_private()
                || v4.is_link_local()
                || v4.octets()[0] == 0
        }
        std::net::IpAddr::V6(v6) => {
            v6.is_loopback()
                || v6.is_unspecified()
                // link-local fe80::/10
                || (v6.segments()[0] & 0xffc0) == 0xfe80
                // unique-local fc00::/7
                || (v6.segments()[0] & 0xfe00) == 0xfc00
        }
    }
}

#[cfg(feature = "sparql-service")]
impl eg_rdf::sparql::RemoteSparql for ServiceClient {
    fn select(&self, endpoint: &str, query: &str) -> Result<SparqlResult, String> {
        use std::io::Read;
        self.check_endpoint(endpoint)?;
        let agent = ureq::AgentBuilder::new()
            .timeout_connect(std::time::Duration::from_secs(Self::CONNECT_TIMEOUT_SECS))
            .timeout_read(std::time::Duration::from_secs(Self::READ_TIMEOUT_SECS))
            .build();
        let resp = agent
            .post(endpoint)
            .set("Content-Type", "application/sparql-query")
            .set("Accept", "application/sparql-results+json")
            .send_string(query)
            .map_err(|e| format!("POST {endpoint} failed: {e}"))?;
        let mut body = String::new();
        resp.into_reader()
            .take(Self::MAX_RESPONSE_BYTES)
            .read_to_string(&mut body)
            .map_err(|e| format!("reading {endpoint} response: {e}"))?;
        parse_results_json(&body)
    }
}

/// Parse a SPARQL 1.1 Query Results JSON document into a [`SparqlResult`] — the INVERSE of
/// [`term_json`]: `{"type":"uri"}` → a `Node <iri>`, `bnode` → `Node _:label`, everything
/// else (`literal`/`typed-literal`) → a `Literal`. `head.vars` gives the column order.
#[cfg(feature = "sparql-service")]
fn parse_results_json(body: &str) -> Result<SparqlResult, String> {
    let j: serde_json::Value =
        serde_json::from_str(body).map_err(|e| format!("parse SPARQL-results JSON: {e}"))?;
    let vars: Vec<String> = j["head"]["vars"]
        .as_array()
        .map(|a| a.iter().filter_map(|v| v.as_str().map(String::from)).collect())
        .unwrap_or_default();
    let mut solutions = Vec::new();
    if let Some(bindings) = j["results"]["bindings"].as_array() {
        for b in bindings {
            let mut sol = eg_rdf::sparql::Solution::new();
            if let Some(obj) = b.as_object() {
                for (k, term) in obj {
                    if let Some(binding) = json_term_to_binding(term) {
                        sol.insert(k.clone(), binding);
                    }
                }
            }
            solutions.push(sol);
        }
    }
    Ok(SparqlResult { vars, solutions })
}

/// One SPARQL-results-JSON term object → a [`Binding`] (inverse of [`term_json`]).
#[cfg(feature = "sparql-service")]
fn json_term_to_binding(term: &serde_json::Value) -> Option<Binding> {
    let ty = term.get("type")?.as_str()?;
    let val = term.get("value")?.as_str()?;
    Some(match ty {
        "uri" => Binding::Node(format!("<{val}>")),
        "bnode" => Binding::Node(format!("_:{val}")),
        // "literal" / "typed-literal" (+ any unknown kind) → lexical literal.
        _ => Binding::Literal(val.to_string()),
    })
}

/// Execute a SPARQL UPDATE against the live registry graphs (creating any named graph
/// the update references). True named-graph routing: each graph term hits its own core.
async fn run_update(
    state: &Arc<RwLock<ServerState>>,
    update_text: &str,
    default_graph: &str,
) -> Result<(), String> {
    // Parse first (cheap) so we know which named graphs to ensure exist.
    let parsed = eg_rdf::update::parse_update(update_text)?;
    let referenced = eg_rdf::update::referenced_named_graphs(&parsed);

    // Under the write lock: ensure the referenced named graphs exist, then clone every
    // graph's `Arc<GraphCore>` (writing through the Arc mutates the live graph).
    let mut graphs: HashMap<String, Arc<GraphCore>> = HashMap::new();
    {
        let mut s = state.write().await;
        for name in &referenced {
            if !s.registry.exists(name) {
                let _ = s.registry.create_graph(name, GraphType::Global, None);
            }
        }
        // Default graph maps to the configured default; absent ⇒ create it.
        if !s.registry.exists(default_graph) {
            let _ = s
                .registry
                .create_graph(default_graph, GraphType::Global, None);
        }
        if let Some(e) = s.registry.get(default_graph) {
            graphs.insert(String::new(), e.core.clone());
        }
        for (name, _) in s.registry.list() {
            if let Some(e) = s.registry.get(&name) {
                graphs.insert(name, e.core.clone());
            }
        }
    }

    let store = EndpointStore { graphs };
    let text = update_text.to_string();
    let report = tokio::task::spawn_blocking(move || {
        eg_rdf::update::execute_str(&text, &store, &Projection::raw()).inspect(|_r| {
            // Mark touched graphs dirty so the checkpoint persists them.
            for core in store.graphs.values() {
                core.mark_dirty();
            }
        })
    })
    .await
    .map_err(|e| format!("compute task failed: {e}"))?;
    report.map(|_| ())
}

// ── W3C SPARQL 1.1 Graph Store HTTP Protocol (CONCEPT:EG-134) ─────────────────────
//
// Direct RDF-graph management over HTTP, DISTINCT from the query/update `/sparql`
// endpoint: the resource IS the graph, addressed by its name.
//
//   * `GET`  /rdf-graphs/service?graph=<iri>   → serialize the graph (EG-050 nego)
//   * `PUT`  …                                  → replace the graph with the posted RDF
//   * `POST` …                                  → merge the posted RDF into the graph
//   * `DELETE` …                                → empty the graph
//   * `HEAD` …                                  → as GET, headers only
//
// Naming follows the spec's two forms: INDIRECT `/rdf-graphs/service?graph=<iri>` (or
// `?default` for the default graph) and DIRECT `/rdf-graphs/<name>`. It reuses the SAME
// registry, RDF parsers (`parse_turtle`/`parse_ntriples`), the merge-aware
// `insert_triples` write op, and the `export_triples` + Turtle/N-Triples serializers the
// query endpoint already uses. Like the endpoint UPDATE path, writes apply straight
// through the live cores and are marked dirty for the next checkpoint.
async fn handle_graph_store(
    state: &Arc<RwLock<ServerState>>,
    req: &HttpRequest,
    path: &str,
    query_string: &str,
) -> (&'static str, &'static str, String) {
    let params = parse_form(query_string);
    let Some(graph) = gsp_target(path, &params) else {
        return (
            "400 Bad Request",
            "text/plain",
            "graph store protocol: name the graph via /rdf-graphs/service?graph=<iri> \
             (or ?default) or /rdf-graphs/<name>"
                .to_string(),
        );
    };

    match req.method.as_str() {
        "GET" | "HEAD" => {
            let core = {
                let s = state.read().await;
                s.registry.get(&graph).map(|e| e.core.clone())
            };
            let Some(core) = core else {
                return ("404 Not Found", "text/plain", format!("no such graph: {graph}"));
            };
            let ct = choose_ct(
                &req.accept,
                params
                    .get("output")
                    .or_else(|| params.get("format"))
                    .map(|s| s.as_str()),
                GRAPH_FORMS,
            );
            let head_only = req.method == "HEAD";
            let g = graph.clone();
            let out = tokio::task::spawn_blocking(move || {
                let triples = export_graph(&core, &g)?;
                if ct == "text/turtle" {
                    eg_rdf::mapping::to_turtle(&triples)
                } else {
                    eg_rdf::mapping::to_ntriples(&triples)
                }
            })
            .await;
            match out {
                Ok(Ok(body)) => ("200 OK", ct, if head_only { String::new() } else { body }),
                Ok(Err(e)) => ("500 Internal Server Error", "text/plain", e),
                Err(e) => (
                    "500 Internal Server Error",
                    "text/plain",
                    format!("compute task failed: {e}"),
                ),
            }
        }
        "PUT" | "POST" => {
            let triples = match parse_rdf_body(&req.content_type, &req.body) {
                Ok(t) => t,
                Err(e) => return ("400 Bad Request", "text/plain", format!("parse RDF body: {e}")),
            };
            let replace = req.method == "PUT";
            // Ensure the graph exists (remember whether we created it ⇒ 201 vs 204).
            let (core, created) = {
                let mut s = state.write().await;
                let created = !s.registry.exists(&graph);
                if created {
                    let _ = s.registry.create_graph(&graph, GraphType::Global, None);
                }
                match s.registry.get(&graph).map(|e| e.core.clone()) {
                    Some(c) => (c, created),
                    None => {
                        return (
                            "500 Internal Server Error",
                            "text/plain",
                            format!("could not open graph: {graph}"),
                        )
                    }
                }
            };
            let applied = tokio::task::spawn_blocking(move || -> Result<(), String> {
                if replace {
                    core.clear(); // PUT replaces; POST merges.
                }
                eg_rdf::update::insert_triples(&core, &triples)?;
                core.mark_dirty();
                Ok(())
            })
            .await;
            match applied {
                Ok(Ok(())) if created => ("201 Created", "text/plain", String::new()),
                Ok(Ok(())) => ("204 No Content", "text/plain", String::new()),
                Ok(Err(e)) => ("400 Bad Request", "text/plain", e),
                Err(e) => (
                    "500 Internal Server Error",
                    "text/plain",
                    format!("compute task failed: {e}"),
                ),
            }
        }
        "DELETE" => {
            let core = {
                let s = state.read().await;
                s.registry.get(&graph).map(|e| e.core.clone())
            };
            let Some(core) = core else {
                return ("404 Not Found", "text/plain", format!("no such graph: {graph}"));
            };
            // Empty the graph (keeps the registry entry addressable — the same semantics
            // the endpoint UPDATE `DROP`/`DropNamedGraph` op uses).
            core.clear();
            core.mark_dirty();
            ("204 No Content", "text/plain", String::new())
        }
        _ => (
            "405 Method Not Allowed",
            "text/plain",
            "graph store protocol: use GET/PUT/POST/DELETE/HEAD".to_string(),
        ),
    }
}

/// Resolve the Graph-Store-Protocol target graph name from the request path + params.
/// Indirect `/rdf-graphs/service?graph=<iri>` (or `?default` ⇒ the configured default
/// graph); direct `/rdf-graphs/<name>` (the trailing, percent-decoded path segment).
fn gsp_target(path: &str, params: &HashMap<String, String>) -> Option<String> {
    if path == "/rdf-graphs/service" || path == "/rdf-graphs/service/" {
        if params.contains_key("default") {
            return Some(gsp_default_graph());
        }
        return params.get("graph").filter(|g| !g.is_empty()).cloned();
    }
    let name = path.strip_prefix("/rdf-graphs/")?;
    if name.is_empty() {
        return None;
    }
    Some(percent_decode(name))
}

/// The configured default-graph name (shared with the query/update endpoint default).
fn gsp_default_graph() -> String {
    std::env::var(DEFAULT_GRAPH_ENV).unwrap_or_else(|_| "__commons__".to_string())
}

/// Parse an RDF request body per its `Content-Type` (N-Triples when so typed, else Turtle
/// — a superset that also parses N-Triples), reusing the endpoint's existing parsers.
fn parse_rdf_body(content_type: &str, body: &str) -> Result<Vec<eg_rdf::oxrdf::Triple>, String> {
    if content_type.contains("n-triples") || content_type.contains("ntriples") {
        eg_rdf::mapping::parse_ntriples(body)
    } else {
        eg_rdf::mapping::parse_turtle(body)
    }
}

/// Export a graph core to RDF triples for GSP `GET` — a cfg wrapper over the shared
/// [`eg_rdf::mapping::export_triples`] inverse mapping (the multi-valued-literal quad
/// store, present only under `rdf-redb`, is not unioned into this convenience read).
fn export_graph(core: &GraphCore, name: &str) -> Result<Vec<eg_rdf::oxrdf::Triple>, String> {
    #[cfg(feature = "rdf-redb")]
    {
        eg_rdf::mapping::export_triples(core, name, None)
    }
    #[cfg(not(feature = "rdf-redb"))]
    {
        let _ = name;
        eg_rdf::mapping::export_triples(core, name)
    }
}

/// The registry-backed store the endpoint UPDATE writes through (pre-seeded cores).
struct EndpointStore {
    /// `"" ⇒ default graph`, else the named-graph IRI → its live core.
    graphs: HashMap<String, Arc<GraphCore>>,
}

impl GraphStore for EndpointStore {
    fn core(&self, graph: Option<&str>) -> Option<Arc<GraphCore>> {
        self.graphs.get(graph.unwrap_or("")).cloned()
    }
    fn named(&self) -> Vec<(String, Arc<GraphCore>)> {
        self.graphs
            .iter()
            .filter(|(k, _)| !k.is_empty())
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }
    fn clear(&self, graph: Option<&str>) -> Result<(), String> {
        if let Some(c) = self.core(graph) {
            c.clear();
        }
        Ok(())
    }
    // DROP keeps the registry entry addressable (clears content) — matches the engine's
    // `DropNamedGraph` op rather than a registry eviction.
    fn drop_graph(&self, graph: Option<&str>) -> Result<(), String> {
        self.clear(graph)
    }
}

// ── result serialization ────────────────────────────────────────────────────────

/// SPARQL 1.1 Query Results JSON for a SELECT solution table.
fn select_json(r: &SparqlResult) -> String {
    let vars: Vec<serde_json::Value> = r
        .vars
        .iter()
        .map(|v| serde_json::Value::String(v.clone()))
        .collect();
    let bindings: Vec<serde_json::Value> = r
        .solutions
        .iter()
        .map(|sol| {
            let mut m = serde_json::Map::new();
            for v in &r.vars {
                if let Some(b) = sol.get(v) {
                    m.insert(v.clone(), term_json(b));
                }
            }
            serde_json::Value::Object(m)
        })
        .collect();
    serde_json::json!({
        "head": { "vars": vars },
        "results": { "bindings": bindings }
    })
    .to_string()
}

/// A solution binding → a SPARQL-JSON RDF term object (`uri` / `bnode` / `literal`).
fn term_json(b: &Binding) -> serde_json::Value {
    match b {
        Binding::Node(s) => {
            if let Some(iri) = s.strip_prefix('<').and_then(|x| x.strip_suffix('>')) {
                serde_json::json!({ "type": "uri", "value": iri })
            } else if let Some(bn) = s.strip_prefix("_:") {
                serde_json::json!({ "type": "bnode", "value": bn })
            } else {
                serde_json::json!({ "type": "literal", "value": s })
            }
        }
        Binding::Literal(v) => serde_json::json!({ "type": "literal", "value": v }),
    }
}

// ── content negotiation (CONCEPT:EG-050) ─────────────────────────────────────────

/// Candidate SELECT/ASK output media types, DEFAULT (SPARQL-results JSON) first.
const SELECT_FORMS: &[&str] = &[
    "application/sparql-results+json",
    "application/sparql-results+xml",
    "text/csv",
    "text/tab-separated-values",
];
/// Candidate CONSTRUCT/DESCRIBE output media types, DEFAULT (N-Triples) first.
const GRAPH_FORMS: &[&str] = &["application/n-triples", "text/turtle"];

/// Resolve the response media type (CONCEPT:EG-050): an `output=`/`format=` override
/// (constrained to this form's candidates) wins; otherwise negotiate the `Accept` header.
fn choose_ct(accept: &str, fmt_override: Option<&str>, forms: &[&'static str]) -> &'static str {
    if let Some(tok) = fmt_override {
        if let Some(ct) = override_ct(tok, forms) {
            return ct;
        }
    }
    negotiate(accept, forms)
}

/// Map a short `output=`/`format=` token (or a full media type) to one of `forms`, or
/// `None` if it names nothing valid for this query form (so the caller falls back).
fn override_ct(token: &str, forms: &[&'static str]) -> Option<&'static str> {
    let t = token.trim().to_ascii_lowercase();
    let want: &str = match t.as_str() {
        "json" | "srj" => "application/sparql-results+json",
        "xml" | "srx" => "application/sparql-results+xml",
        "csv" => "text/csv",
        "tsv" => "text/tab-separated-values",
        "nt" | "ntriples" | "n-triples" => "application/n-triples",
        "ttl" | "turtle" => "text/turtle",
        s => s,
    };
    forms.iter().copied().find(|&f| f == want)
}

/// Pick the best media type among `forms` for an `Accept` header (CONCEPT:EG-050).
/// Empty / `*/*` / no acceptable match → the per-form default (`forms[0]`). Honors
/// q-values and `type/*` wildcards; on a q-tie the client's listed order is respected.
fn negotiate(accept: &str, forms: &[&'static str]) -> &'static str {
    let accept = accept.trim();
    if accept.is_empty() {
        return forms[0];
    }
    let mut best: Option<(&'static str, f32)> = None;
    for part in accept.split(',') {
        let mut segs = part.split(';');
        let media = segs.next().unwrap_or("").trim().to_ascii_lowercase();
        let mut q = 1.0f32;
        for seg in segs {
            if let Some(v) = seg.trim().strip_prefix("q=") {
                q = v.parse().unwrap_or(1.0);
            }
        }
        if q <= 0.0 {
            continue;
        }
        for &f in forms {
            let matches = media == f
                || media == "*/*"
                || (media.ends_with("/*") && f.starts_with(&media[..media.len() - 1]));
            if matches {
                if best.map(|(_, bq)| q > bq).unwrap_or(true) {
                    best = Some((f, q));
                }
                break;
            }
        }
    }
    best.map(|(f, _)| f).unwrap_or(forms[0])
}

// ── hand-written SPARQL 1.1 Query Results serializers (CONCEPT:EG-050) ────────────

/// The ASK boolean rendered for the negotiated media type (JSON default, XML, or a bare
/// `true`/`false` for CSV/TSV). The JSON form is byte-identical to the prior fixed output.
fn boolean_body(ct: &str, b: bool) -> String {
    match ct {
        "application/sparql-results+xml" => format!(
            "<?xml version=\"1.0\"?>\n<sparql xmlns=\"http://www.w3.org/2005/sparql-results#\">\n <head/>\n <boolean>{b}</boolean>\n</sparql>\n"
        ),
        "text/csv" | "text/tab-separated-values" => format!("{b}"),
        _ => format!("{{\"head\":{{}},\"boolean\":{b}}}"),
    }
}

/// SPARQL 1.1 Query Results XML for a SELECT solution table.
fn results_xml(r: &SparqlResult) -> String {
    let mut out = String::from(
        "<?xml version=\"1.0\"?>\n<sparql xmlns=\"http://www.w3.org/2005/sparql-results#\">\n <head>\n",
    );
    for v in &r.vars {
        out.push_str(&format!("  <variable name=\"{}\"/>\n", xml_escape(v)));
    }
    out.push_str(" </head>\n <results>\n");
    for sol in &r.solutions {
        out.push_str("  <result>\n");
        for v in &r.vars {
            if let Some(b) = sol.get(v) {
                out.push_str(&format!(
                    "   <binding name=\"{}\">{}</binding>\n",
                    xml_escape(v),
                    term_xml(b)
                ));
            }
        }
        out.push_str("  </result>\n");
    }
    out.push_str(" </results>\n</sparql>\n");
    out
}

/// A binding → a SPARQL-XML term element (`<uri>`/`<bnode>`/`<literal>`), mirroring the
/// `term_json` classification.
fn term_xml(b: &Binding) -> String {
    match b {
        Binding::Node(s) => {
            if let Some(iri) = s.strip_prefix('<').and_then(|x| x.strip_suffix('>')) {
                format!("<uri>{}</uri>", xml_escape(iri))
            } else if let Some(bn) = s.strip_prefix("_:") {
                format!("<bnode>{}</bnode>", xml_escape(bn))
            } else {
                format!("<literal>{}</literal>", xml_escape(s))
            }
        }
        Binding::Literal(v) => format!("<literal>{}</literal>", xml_escape(v)),
    }
}

/// SPARQL 1.1 Query Results CSV: a header row of bare variable names, then one row per
/// solution. CSV is lossy (no term-type info): IRIs are the bare IRI, literals the
/// lexical value; a field with `,`/`"`/CR/LF is double-quoted with `"` doubled. CRLF
/// line endings per the spec; an unbound variable is an empty field.
fn results_csv(r: &SparqlResult) -> String {
    let mut out = String::new();
    out.push_str(&r.vars.join(","));
    out.push_str("\r\n");
    for sol in &r.solutions {
        let cells: Vec<String> = r
            .vars
            .iter()
            .map(|v| sol.get(v).map(csv_cell).unwrap_or_default())
            .collect();
        out.push_str(&cells.join(","));
        out.push_str("\r\n");
    }
    out
}

fn csv_cell(b: &Binding) -> String {
    let raw = match b {
        Binding::Node(s) => s
            .strip_prefix('<')
            .and_then(|x| x.strip_suffix('>'))
            .map(String::from)
            .unwrap_or_else(|| s.clone()),
        Binding::Literal(v) => v.clone(),
    };
    if raw.contains([',', '"', '\n', '\r']) {
        format!("\"{}\"", raw.replace('"', "\"\""))
    } else {
        raw
    }
}

/// SPARQL 1.1 Query Results TSV: a header row of `?var` names, then one tab-separated row
/// per solution. TSV keeps term types (Turtle syntax): IRIs as `<iri>`, blank nodes as
/// `_:label`, literals as escaped quoted strings; an unbound variable is an empty field.
fn results_tsv(r: &SparqlResult) -> String {
    let mut out = String::new();
    let header: Vec<String> = r.vars.iter().map(|v| format!("?{v}")).collect();
    out.push_str(&header.join("\t"));
    out.push('\n');
    for sol in &r.solutions {
        let cells: Vec<String> = r
            .vars
            .iter()
            .map(|v| sol.get(v).map(tsv_cell).unwrap_or_default())
            .collect();
        out.push_str(&cells.join("\t"));
        out.push('\n');
    }
    out
}

fn tsv_cell(b: &Binding) -> String {
    match b {
        Binding::Node(s) if s.starts_with('<') || s.starts_with("_:") => s.clone(),
        Binding::Node(s) => format!("\"{}\"", tsv_escape(s)),
        Binding::Literal(v) => format!("\"{}\"", tsv_escape(v)),
    }
}

fn tsv_escape(s: &str) -> String {
    s.replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\t', "\\t")
        .replace('\n', "\\n")
        .replace('\r', "\\r")
}

/// XML text/attribute escaping for the SPARQL-XML serializer.
fn xml_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}

// ── tiny HTTP helpers (no external dep) ──────────────────────────────────────────

fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack.windows(needle.len()).position(|w| w == needle)
}

/// Parse an `&`-separated `k=v` form (query string or urlencoded body), percent- and
/// `+`-decoding both sides.
fn parse_form(s: &str) -> HashMap<String, String> {
    let mut out = HashMap::new();
    for pair in s.split('&') {
        if pair.is_empty() {
            continue;
        }
        let (k, v) = pair.split_once('=').unwrap_or((pair, ""));
        out.insert(percent_decode(k), percent_decode(v));
    }
    out
}

/// Decode `%XX` escapes and `+` → space (application/x-www-form-urlencoded).
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
                let h = hex_val(bytes[i + 1]);
                let l = hex_val(bytes[i + 2]);
                if let (Some(h), Some(l)) = (h, l) {
                    out.push(h * 16 + l);
                    i += 3;
                } else {
                    out.push(bytes[i]);
                    i += 1;
                }
            }
            c => {
                out.push(c);
                i += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).to_string()
}

fn hex_val(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn percent_and_plus_decode() {
        assert_eq!(percent_decode("a+b%20c"), "a b c");
        assert_eq!(percent_decode("SELECT%20%3Fs%20WHERE"), "SELECT ?s WHERE");
    }

    #[test]
    fn form_parse_extracts_query() {
        let f = parse_form("query=SELECT%20%3Fs&default-graph-uri=g");
        assert_eq!(f.get("query").unwrap(), "SELECT ?s");
        assert_eq!(f.get("default-graph-uri").unwrap(), "g");
    }

    #[test]
    fn select_json_shape() {
        let mut sol = eg_rdf::sparql::Solution::new();
        sol.insert("s".to_string(), Binding::Node("<http://x>".to_string()));
        let r = SparqlResult {
            vars: vec!["s".to_string()],
            solutions: vec![sol],
        };
        let j: serde_json::Value = serde_json::from_str(&select_json(&r)).unwrap();
        assert_eq!(j["head"]["vars"][0], "s");
        assert_eq!(j["results"]["bindings"][0]["s"]["type"], "uri");
        assert_eq!(j["results"]["bindings"][0]["s"]["value"], "http://x");
    }

    // ── CONCEPT:EG-050 content negotiation + serializers ─────────────────────────

    fn sample_result() -> SparqlResult {
        let mut sol = eg_rdf::sparql::Solution::new();
        sol.insert("s".to_string(), Binding::Node("<http://x>".to_string()));
        sol.insert("n".to_string(), Binding::Literal("a,b".to_string()));
        SparqlResult {
            vars: vec!["s".to_string(), "n".to_string()],
            solutions: vec![sol],
        }
    }

    #[test]
    fn negotiate_defaults_and_accept() {
        // Empty / */* / unknown → the per-form default (forms[0]).
        assert_eq!(negotiate("", SELECT_FORMS), "application/sparql-results+json");
        assert_eq!(negotiate("*/*", SELECT_FORMS), "application/sparql-results+json");
        assert_eq!(
            negotiate("application/octet-stream", SELECT_FORMS),
            "application/sparql-results+json"
        );
        assert_eq!(negotiate("", GRAPH_FORMS), "application/n-triples");
        // Explicit acceptable types are honored.
        assert_eq!(negotiate("text/csv", SELECT_FORMS), "text/csv");
        assert_eq!(negotiate("text/turtle", GRAPH_FORMS), "text/turtle");
        // q-values choose the highest-weighted acceptable type.
        assert_eq!(
            negotiate("text/csv;q=0.5, application/sparql-results+xml;q=0.9", SELECT_FORMS),
            "application/sparql-results+xml"
        );
    }

    #[test]
    fn format_override_wins_and_falls_back() {
        assert_eq!(choose_ct("", Some("csv"), SELECT_FORMS), "text/csv");
        assert_eq!(choose_ct("text/csv", Some("xml"), SELECT_FORMS),
            "application/sparql-results+xml");
        // An override invalid for this form is ignored → negotiate the Accept header.
        assert_eq!(choose_ct("text/csv", Some("turtle"), SELECT_FORMS), "text/csv");
        assert_eq!(choose_ct("", Some("ttl"), GRAPH_FORMS), "text/turtle");
    }

    #[test]
    fn csv_quotes_special_fields() {
        let csv = results_csv(&sample_result());
        // header + one data row, CRLF-terminated; the comma field is quoted, IRI bare.
        assert_eq!(csv, "s,n\r\nhttp://x,\"a,b\"\r\n");
    }

    #[test]
    fn tsv_keeps_term_types() {
        let tsv = results_tsv(&sample_result());
        assert_eq!(tsv, "?s\t?n\n<http://x>\t\"a,b\"\n");
    }

    #[test]
    fn xml_shape_and_escaping() {
        let xml = results_xml(&sample_result());
        assert!(xml.contains("<variable name=\"s\"/>"));
        assert!(xml.contains("<uri>http://x</uri>"));
        assert!(xml.contains("<literal>a,b</literal>"));
        assert_eq!(xml_escape("a<b&c\">"), "a&lt;b&amp;c&quot;&gt;");
    }

    #[test]
    fn boolean_bodies_per_media_type() {
        // JSON default byte-identical to the prior fixed output.
        assert_eq!(
            boolean_body("application/sparql-results+json", true),
            "{\"head\":{},\"boolean\":true}"
        );
        assert!(boolean_body("application/sparql-results+xml", false).contains("<boolean>false</boolean>"));
        assert_eq!(boolean_body("text/csv", true), "true");
    }

    // ── CONCEPT:EG-052 — SPARQL SERVICE federation client ────────────────────────

    /// The results-JSON parse is the exact inverse of `term_json`'s uri/bnode/literal split.
    #[cfg(feature = "sparql-service")]
    #[test]
    fn results_json_round_trips_term_json() {
        let body = r#"{"head":{"vars":["s","b","l"]},"results":{"bindings":[
            {"s":{"type":"uri","value":"http://x"},
             "b":{"type":"bnode","value":"n1"},
             "l":{"type":"literal","value":"hi"}}]}}"#;
        let r = parse_results_json(body).unwrap();
        assert_eq!(r.vars, vec!["s", "b", "l"]);
        let sol = &r.solutions[0];
        assert_eq!(sol.get("s").unwrap(), &Binding::Node("<http://x>".to_string()));
        assert_eq!(sol.get("b").unwrap(), &Binding::Node("_:n1".to_string()));
        assert_eq!(sol.get("l").unwrap(), &Binding::Literal("hi".to_string()));
        // And `term_json` maps them straight back to the same JSON term kinds.
        assert_eq!(term_json(sol.get("s").unwrap())["type"], "uri");
        assert_eq!(term_json(sol.get("b").unwrap())["type"], "bnode");
        assert_eq!(term_json(sol.get("l").unwrap())["type"], "literal");
    }

    // ── CONCEPT:EG-134 — Graph Store HTTP Protocol ──────────────────────────────

    /// Indirect (`/rdf-graphs/service?graph=` / `?default`) and direct (`/rdf-graphs/<name>`)
    /// naming both resolve to the target graph; a bare `/rdf-graphs` names nothing.
    #[test]
    fn gsp_target_resolves_indirect_and_direct() {
        let g = parse_form("graph=http%3A%2F%2Fex%2Fg1");
        assert_eq!(
            gsp_target("/rdf-graphs/service", &g),
            Some("http://ex/g1".to_string())
        );
        let d = parse_form("default");
        assert_eq!(gsp_target("/rdf-graphs/service", &d), Some(gsp_default_graph()));
        // Direct naming percent-decodes the trailing segment.
        assert_eq!(
            gsp_target("/rdf-graphs/http%3A%2F%2Fex%2Fg2", &HashMap::new()),
            Some("http://ex/g2".to_string())
        );
        // No graph named.
        assert_eq!(gsp_target("/rdf-graphs/service", &HashMap::new()), None);
        assert_eq!(gsp_target("/rdf-graphs/", &HashMap::new()), None);
    }

    /// Content-type routing of the RDF body parser (Turtle default, N-Triples when typed).
    #[test]
    fn gsp_parse_rdf_body_by_content_type() {
        let ttl = "@prefix ex: <http://ex/> . ex:a ex:p ex:b .";
        assert_eq!(parse_rdf_body("text/turtle", ttl).unwrap().len(), 1);
        let nt = "<http://ex/a> <http://ex/p> <http://ex/b> .";
        assert_eq!(parse_rdf_body("application/n-triples", nt).unwrap().len(), 1);
        // Unknown content-type falls back to Turtle (a superset that parses N-Triples).
        assert_eq!(parse_rdf_body("", nt).unwrap().len(), 1);
    }

    /// PUT-then-GET round-trips a graph: parse the posted RDF, replace the core, then
    /// export + serialize + re-parse yields the SAME triple set. (Exercises the exact
    /// parse → `clear` + `insert_triples` → `export_graph` → serializer path GET/PUT use;
    /// a full `ServerState` has no public test constructor.)
    #[test]
    fn gsp_put_then_get_round_trips() {
        let core = GraphCore::new();
        let body = "@prefix ex: <http://ex/> .
                    ex:a ex:knows ex:b .
                    ex:a ex:name \"Alice\" .";
        let triples = parse_rdf_body("text/turtle", body).unwrap();
        // PUT = replace: clear then insert.
        core.clear();
        eg_rdf::update::insert_triples(&core, &triples).unwrap();
        // GET = export + serialize (default N-Triples), then re-parse to compare.
        let exported = export_graph(&core, "http://ex/g").unwrap();
        let nt = eg_rdf::mapping::to_ntriples(&exported).unwrap();
        let reparsed = eg_rdf::mapping::parse_ntriples(&nt).unwrap();
        assert_eq!(
            eg_rdf::mapping::triple_set_key(&reparsed),
            eg_rdf::mapping::triple_set_key(&triples),
            "PUT→GET preserves the graph's triple set"
        );
    }

    /// POST merges into an existing graph (no clear): the prior triple survives and the
    /// posted triple is added.
    #[test]
    fn gsp_post_merges() {
        let core = GraphCore::new();
        let seed = parse_rdf_body("text/turtle", "<http://ex/a> <http://ex/p> <http://ex/b> .")
            .unwrap();
        eg_rdf::update::insert_triples(&core, &seed).unwrap();
        // POST = merge: no clear.
        let add =
            parse_rdf_body("text/turtle", "<http://ex/c> <http://ex/q> <http://ex/d> .").unwrap();
        eg_rdf::update::insert_triples(&core, &add).unwrap();
        let exported = export_graph(&core, "http://ex/g").unwrap();
        assert_eq!(exported.len(), 2, "both the seeded and posted triples are present");
    }

    /// DELETE empties the graph: after a clear, the export is empty.
    #[test]
    fn gsp_delete_empties() {
        let core = GraphCore::new();
        let t = parse_rdf_body("text/turtle", "<http://ex/a> <http://ex/p> <http://ex/b> .")
            .unwrap();
        eg_rdf::update::insert_triples(&core, &t).unwrap();
        assert_eq!(export_graph(&core, "g").unwrap().len(), 1);
        core.clear();
        assert!(export_graph(&core, "g").unwrap().is_empty(), "DELETE empties the graph");
    }

    /// GET content-negotiation: Turtle when accepted, N-Triples by default, and each
    /// serializer emits its own syntax.
    #[test]
    fn gsp_get_content_negotiation() {
        assert_eq!(choose_ct("text/turtle", None, GRAPH_FORMS), "text/turtle");
        assert_eq!(choose_ct("", None, GRAPH_FORMS), "application/n-triples");
        let core = GraphCore::new();
        let t = parse_rdf_body("text/turtle", "<http://ex/a> <http://ex/p> <http://ex/b> .")
            .unwrap();
        eg_rdf::update::insert_triples(&core, &t).unwrap();
        let exported = export_graph(&core, "g").unwrap();
        let ttl = eg_rdf::mapping::to_turtle(&exported).unwrap();
        let nt = eg_rdf::mapping::to_ntriples(&exported).unwrap();
        assert!(nt.contains("<http://ex/a> <http://ex/p> <http://ex/b> ."));
        assert!(ttl.contains("http://ex/a"), "turtle serializer emits the subject");
    }

    /// The SSRF guard: allowlist required, internal-address resolutions refused, and an
    /// explicitly-listed public host permitted.
    #[cfg(feature = "sparql-service")]
    #[test]
    fn ssrf_guard_blocks_and_allows() {
        // Empty / unset allowlist ⇒ no client (fail-closed).
        std::env::remove_var(SERVICE_ALLOW_ENV);
        assert!(ServiceClient::from_env().is_none());
        // A non-http scheme and a non-allowlisted host are refused.
        let c = ServiceClient {
            allow: vec!["sparql.example.org".to_string()],
        };
        assert!(c.check_endpoint("ftp://sparql.example.org/x").is_err());
        assert!(c.check_endpoint("http://evil.example.com/sparql").is_err());
        // A loopback literal that is NOT allowlisted is refused.
        assert!(c.check_endpoint("http://127.0.0.1:8080/sparql").is_err());
        // Blocked-range classification.
        assert!(is_blocked_ip(&"10.1.2.3".parse().unwrap()));
        assert!(is_blocked_ip(&"192.168.0.1".parse().unwrap()));
        assert!(is_blocked_ip(&"169.254.1.1".parse().unwrap()));
        assert!(is_blocked_ip(&"::1".parse().unwrap()));
        assert!(!is_blocked_ip(&"8.8.8.8".parse().unwrap()));
    }
}
