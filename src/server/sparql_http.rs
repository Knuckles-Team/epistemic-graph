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
//! Result media types: `application/sparql-results+json` for SELECT (bindings) and ASK
//! (boolean); `application/n-triples` for CONSTRUCT/DESCRIBE (an RDF graph). The default
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
                "HTTP/1.1 {status}\r\ncontent-type: {ctype}\r\ncontent-length: {}\r\naccess-control-allow-origin: *\r\nconnection: close\r\n\r\n{body}",
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
    if !path.starts_with("/sparql") {
        return ("404 Not Found", "text/plain", "not found".to_string());
    }
    if req.method == "OPTIONS" {
        return ("204 No Content", "text/plain", String::new());
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
        } else if req.content_type.contains("application/x-www-form-urlencoded") {
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
        return ("400 Bad Request", "text/plain", "empty query/update".to_string());
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
        run_query(state, &text, &default_graph, &req.accept).await
    }
}

/// Execute a query over an off-lock dataset snapshot of every registry graph.
async fn run_query(
    state: &Arc<RwLock<ServerState>>,
    query: &str,
    default_graph: &str,
    accept: &str,
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
        eg_rdf::sparql::run_outcome_dataset(&ds, &query, &Projection::raw())
    })
    .await;

    match outcome {
        Ok(Ok(QueryOutcome::Solutions(r))) => (
            "200 OK",
            "application/sparql-results+json",
            select_json(&r),
        ),
        Ok(Ok(QueryOutcome::Boolean(b))) => (
            "200 OK",
            "application/sparql-results+json",
            format!("{{\"head\":{{}},\"boolean\":{b}}}"),
        ),
        Ok(Ok(QueryOutcome::Graph(triples))) => {
            match eg_rdf::mapping::to_ntriples(&triples) {
                Ok(nt) => {
                    let _ = &accept;
                    ("200 OK", "application/n-triples", nt)
                }
                Err(e) => ("500 Internal Server Error", "text/plain", e),
            }
        }
        Ok(Err(e)) => ("400 Bad Request", "text/plain", format!("SPARQL error: {e}")),
        Err(e) => (
            "500 Internal Server Error",
            "text/plain",
            format!("compute task failed: {e}"),
        ),
    }
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
            let _ = s.registry.create_graph(default_graph, GraphType::Global, None);
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
        eg_rdf::update::execute_str(&text, &store, &Projection::raw()).map(|r| {
            // Mark touched graphs dirty so the checkpoint persists them.
            for core in store.graphs.values() {
                core.mark_dirty();
            }
            r
        })
    })
    .await
    .map_err(|e| format!("compute task failed: {e}"))?;
    report.map(|_| ())
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

// ── tiny HTTP helpers (no external dep) ──────────────────────────────────────────

fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|w| w == needle)
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
        assert_eq!(
            percent_decode("SELECT%20%3Fs%20WHERE"),
            "SELECT ?s WHERE"
        );
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
}
