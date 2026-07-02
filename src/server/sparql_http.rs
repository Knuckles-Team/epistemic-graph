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
        eg_rdf::sparql::run_outcome_dataset(&ds, &query, &Projection::raw())
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
}
