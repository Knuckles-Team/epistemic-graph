//! W3C SPARQL 1.1 Protocol HTTP endpoint (CONCEPT:EG-KG.query.named-graph-support, feature `sparql-http`).
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
//! Result media types are content-negotiated (CONCEPT:EG-KG.ontology.content-negotiation-serializers) from the `Accept` header
//! (with an `output=`/`format=` query-param override): SELECT/ASK serve SPARQL-results
//! JSON (default), XML, CSV or TSV; CONSTRUCT/DESCRIBE serve N-Triples (default) or
//! Turtle. With no `Accept` header the per-form default is used (byte-identical to the
//! prior fixed behavior). The default
//! graph is `?default-graph-uri=` (or `EPISTEMIC_GRAPH_SPARQL_DEFAULT_GRAPH`, else
//! `__commons__`); EVERY registry graph is exposed as a named graph so `GRAPH <name>{}`
//! and `GRAPH ?g{}` work across the engine's graphs.
//!
//! Queries run off an off-lock snapshot. Every UPDATE and Graph Store write is
//! reconstructed as an exact signed `ApplyMutation` request and enters the native
//! multi-graph coordinator; the HTTP adapter never mutates a live graph core.

use std::collections::HashMap;
use std::sync::Arc;

use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;
use tokio::sync::RwLock;

use crate::graph::GraphCore;
use crate::server::http1::{self, HttpMessage, RequestLimits};
use crate::server::ServerState;
use eg_rdf::sparql::{Binding, Dataset, Projection, QueryOutcome, SparqlResult};

mod graph_store;
mod negotiation;
mod route;
mod update_plan;

use graph_store::handle_graph_store;
#[cfg(test)]
use graph_store::{export_graph, gsp_default_graph, gsp_target, parse_rdf_body};
use negotiation::{choose_ct, render_query_outcome, serialize_graph, GRAPH_FORMS};
#[cfg(test)]
use negotiation::{negotiate, SELECT_FORMS};
pub(crate) use update_plan::{
    plan_update, update_graphs, update_uses_variable_graph, PlannedGraphUpdate,
};

/// Env var naming the default-graph the endpoint resolves a bare query against.
pub const DEFAULT_GRAPH_ENV: &str = "EPISTEMIC_GRAPH_SPARQL_DEFAULT_GRAPH";
/// Env var carrying the bind address (`host:port`) when `--sparql-addr` is not passed.
pub const SPARQL_ADDR_ENV: &str = "EPISTEMIC_GRAPH_SPARQL_ADDR";
/// Current typed event carried by the signed `ApplyMutation` protocol method.
pub const SPARQL_HTTP_UPDATE_EVENT: &str = "sparql_http_update_v1";
/// SSRF allowlist for outbound `SERVICE` federation (CONCEPT:EG-KG.query.sparql-service-federation-client, feature
/// `sparql-service`): a comma-separated set of allowed endpoint hosts / `scheme://host:port`
/// origins. **Empty / unset ⇒ SERVICE is DISABLED (fail-closed)** — no remote client is
/// bound, so a `SERVICE <ep> { … }` clause errors (or, under `SERVICE SILENT`, yields the
/// empty solution). A host resolving to a loopback/link-local/RFC-1918 address is refused
/// unless the allowlist names that exact host literally.
pub const SERVICE_ALLOW_ENV: &str = "EPISTEMIC_GRAPH_SPARQL_SERVICE_ALLOW";
/// Env var: the static bearer-token secret for the `/sparql` SELECT/CONSTRUCT/
/// ASK read leg's carrier credential (A18) — the same credential SHAPE the
/// KV-cache HTTP surface accepts (`server::auth::BearerCredential`),
/// independently configured (own env, own JWT issuer/audience/JWKS below) so a
/// caller entitled to KV pages is not automatically entitled to run federated
/// SPARQL reads. Ignored once a JWT issuer is configured (see
/// `crate::server::oidc::JwtValidator::from_env_sparql`); unset with no JWT
/// issuer ⇒ the read leg has no credential to check and stays denied.
pub const SPARQL_BEARER_TOKEN_ENV: &str = "EPISTEMIC_GRAPH_SPARQL_BEARER_TOKEN";
const HTTP_LIMITS: RequestLimits = RequestLimits {
    max_head_bytes: 64 * 1024,
    max_body_bytes: 8 * 1024 * 1024,
};
const HTTP_READ_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(15);

/// Resolve the `/sparql` read leg's bearer/JWT credential from the
/// environment (A18). JWT first — paired with the platform's configured OIDC
/// provider, mirroring `kvcache_http::resolve_auth` exactly except for its OWN
/// (SPARQL-specific) issuer/audience/JWKS env vars — else the static secret
/// named by [`SPARQL_BEARER_TOKEN_ENV`]. `Ok(None)` when neither is
/// configured: NOT an error, this route simply has no credential shape to
/// check yet and every request keeps failing closed exactly as it did before
/// this credential existed. `Err` only for a genuinely broken JWT
/// configuration (an issuer selected without its mandatory audience/JWKS
/// URL) — logged, then also treated as unconfigured so a startup typo cannot
/// crash the listener that also serves SPARQL UPDATE / Graph Store traffic.
fn resolve_bearer_credential() -> Result<Option<crate::server::auth::BearerCredential>, String> {
    match crate::server::oidc::JwtValidator::from_env_sparql() {
        Ok(Some(validator)) => {
            return Ok(Some(crate::server::auth::BearerCredential::Jwt(
                std::sync::Arc::new(validator),
            )))
        }
        Ok(None) => {}
        Err(message) => return Err(message),
    }
    Ok(std::env::var(SPARQL_BEARER_TOKEN_ENV)
        .ok()
        .filter(|s| !s.is_empty())
        .map(crate::server::auth::BearerCredential::Static))
}

/// Serve the SPARQL 1.1 HTTP protocol on `listener`, backed by the engine `state`.
pub async fn serve(listener: TcpListener, state: Arc<RwLock<ServerState>>) {
    if let Err(error) = crate::server::require_loopback_listener(&listener) {
        tracing::error!("SPARQL listener refused: {error}");
        return;
    }
    // A18: resolved once at bind time (mirrors `kvcache_http`), not per
    // request — a JWT validator caches its JWKS keys and re-reading env vars
    // on every connection would be wasted work. `None` (unconfigured, or a
    // broken config logged below) means the SELECT/CONSTRUCT/ASK read leg
    // denies every request; SPARQL UPDATE and the Graph Store PUT/POST/DELETE
    // legs are unaffected (they authenticate via the `eg2.` envelope already).
    let bearer = match resolve_bearer_credential() {
        Ok(credential) => credential,
        Err(error) => {
            tracing::error!(
                "SPARQL bearer/JWT configuration is invalid ({error}); the \
                 SELECT/CONSTRUCT/ASK read leg stays denied until it is fixed"
            );
            None
        }
    };
    loop {
        let Ok((mut stream, _)) = listener.accept().await else {
            continue;
        };
        let state = state.clone();
        let bearer = bearer.clone();
        tokio::spawn(async move {
            let (status, ctype, body) =
                match tokio::time::timeout(HTTP_READ_TIMEOUT, read_request(&mut stream)).await {
                    Ok(Some(req)) => handle(&state, &bearer, req).await,
                    _ => (
                        "400 Bad Request",
                        "text/plain",
                        "malformed HTTP request".to_string(),
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

/// Read one framed HTTP/1.1 request for the SPARQL surface. Beyond [`http1`]'s
/// framing this rejects a present-but-unparseable `X-Epistemic-Request-Id`:
/// the `eg2.` signing leg requires it to be a request number, and a caller
/// that sent a malformed one is refused here rather than served a read and
/// denied an update.
async fn read_request(stream: &mut tokio::net::TcpStream) -> Option<HttpMessage> {
    let request = http1::read_request(stream, HTTP_LIMITS).await?;
    let id = request.header("x-epistemic-request-id");
    if !id.is_empty() && id.parse::<u64>().is_err() {
        return None;
    }
    Some(request)
}

fn signed_request(
    req: &HttpMessage,
    graph: String,
    method: crate::protocol::Method,
) -> Result<crate::protocol::Request, String> {
    let id = req
        .header("x-epistemic-request-id")
        .parse::<u64>()
        .map_err(|_| "missing X-Epistemic-Request-Id".to_string())?;
    let auth_token = req
        .header("authorization")
        .strip_prefix("Bearer ")
        .filter(|token| token.starts_with("eg2."))
        .ok_or_else(|| "Authorization must be Bearer eg2.<verified-envelope>".to_string())?
        .to_string();
    Ok(crate::protocol::Request {
        id,
        graph,
        auth_token,
        agent_id: None,
        method,
    })
}

/// Route + execute a request → `(status, content_type, body)`.
async fn handle(
    state: &Arc<RwLock<ServerState>>,
    bearer: &Option<crate::server::auth::BearerCredential>,
    req: HttpMessage,
) -> (&'static str, &'static str, String) {
    if !req.header("origin").is_empty() {
        return (
            "403 Forbidden",
            "text/plain",
            "browser origin denied".to_string(),
        );
    }
    let (path, query_string) = req.path_and_query();
    let content_type = req.header("content-type").to_ascii_lowercase();
    let body = req.text();
    if req.method == "OPTIONS"
        && (path.starts_with("/sparql") || path.starts_with("/rdf-graphs") || path == "/nl")
    {
        return ("204 No Content", "text/plain", String::new());
    }
    #[cfg(feature = "nl-query")]
    if path == "/nl" {
        return handle_nl(state, &req).await;
    }
    if path.starts_with("/rdf-graphs") {
        return handle_graph_store(state, &req, path, query_string).await;
    }
    if !path.starts_with("/sparql") {
        return ("404 Not Found", "text/plain", "not found".to_string());
    }
    route::handle_sparql(state, bearer, &req, query_string, &content_type, &body).await
}

/// Natural-language query facade route (CONCEPT:EG-KG.query.fence-stripper). Accepts a JSON body
/// `{"text": "...", "graph": "..."}` (graph optional — defaults to the SPARQL default
/// graph), builds an AUTHENTICATED in-process `Method::NlQuery` request, and runs it
/// through the FULL dispatch path (the planner + RLS + the deterministic
/// `UnifiedQueryText` pipeline) — so the HTTP route and the wire method share ONE code
/// path. The executed `[id, score]` rows are returned as JSON.
#[cfg(feature = "nl-query")]
async fn handle_nl(
    state: &Arc<RwLock<ServerState>>,
    req: &HttpMessage,
) -> (&'static str, &'static str, String) {
    // A18: `/nl` runs under a FIXED engine-owned service identity
    // (`dispatch_authenticated_local_query`), not a per-caller verified one —
    // there is no `eg2.` envelope (or any other credential) to check here, so no
    // `CarrierAuthority` can ever be minted for this route yet. Deny explicitly
    // (real, not the old blanket stub) rather than silently widen this fixed,
    // read-only adapter to every local caller now that the stub is gone
    // elsewhere in this file.
    if crate::server::access::unauthenticated_carrier_denied(None) {
        crate::metrics::access_denied();
        return (
            "403 Forbidden",
            "application/json",
            r#"{"error":"ACCESS_DENIED: /nl has no verified request-carrier mechanism yet"}"#
                .to_string(),
        );
    }
    if req.method != "POST" {
        return (
            "405 Method Not Allowed",
            "application/json",
            r#"{"error":"POST a JSON body {\"text\":\"…\",\"graph\":\"…\"} to /nl"}"#.to_string(),
        );
    }
    let body: serde_json::Value = match serde_json::from_str(&req.text()) {
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

    // The local adapter uses a fixed read-only service identity that must be
    // provisioned in durable RBAC policy.
    let id = 1u64;
    let request = crate::protocol::Request {
        id,
        graph: graph.clone(),
        auth_token: String::new(),
        agent_id: Some("service:local-query".to_string()),
        method: crate::protocol::Method::NlQuery { text, graph },
    };
    let resp = crate::server::dispatch::dispatch_authenticated_local_query(state, request).await;
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
    bearer: Option<&crate::server::auth::BearerCredential>,
    authorization_header: &str,
) -> (&'static str, &'static str, String) {
    // A18: a SPARQL SELECT/CONSTRUCT/ASK read can span multiple named graphs in
    // one query (FROM/FROM NAMED), so it does not fit the single-graph, envelope-
    // bound `Method::Sparql` RPC contract (the `eg2.` MAC binds the exact
    // request id/graph/method/body — there is no way to verify a caller's
    // envelope for a federated, HTTP-native query shape that has no matching
    // `Method`). This leg instead authenticates the SAME way the KV-cache HTTP
    // surface does — a configured bearer/JWT credential
    // (`server::auth::BearerCredential`, resolved once at `serve()` startup)
    // — and mints the SAME fixed-service-identity `CarrierAuthority` through
    // the ONE shared helper every auxiliary HTTP surface uses
    // (`server::auth::mint_fixed_service_carrier`, shared with the S3 SigV4
    // and KV-cache bearer/JWT routes: one implementation, three routes). No
    // configured credential, or an invalid one, still denies (fail closed) —
    // see AGENTS.md (A18) for the before/after across every auxiliary surface.
    let carrier = crate::server::auth::mint_fixed_service_carrier(
        bearer.is_some_and(|credential| credential.verify(authorization_header)),
        "sparql-http-client",
        &["kg:read"],
    );
    if crate::server::access::unauthenticated_carrier_denied(carrier.as_ref()) {
        crate::metrics::access_denied();
        return (
            "403 Forbidden",
            "text/plain",
            "ACCESS_DENIED: SPARQL SELECT/CONSTRUCT/ASK over HTTP requires a valid \
             Authorization: Bearer <token> (static secret or JWT) credential — see \
             EPISTEMIC_GRAPH_SPARQL_BEARER_TOKEN / EPISTEMIC_GRAPH_SPARQL_JWT_ISSUER"
                .to_string(),
        );
    }
    // Gather cores under a brief read lock, then snapshot off-lock.
    let (default_core, named_cores) = registry_cores(state, default_graph).await;
    let query = query.to_string();
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
        Ok(Ok(outcome)) => render_query_outcome(outcome, accept, fmt_override),
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

/// The default graph's core (an empty graph if absent) and every registry graph's
/// core by name, gathered under one brief read lock.
async fn registry_cores(
    state: &Arc<RwLock<ServerState>>,
    default_graph: &str,
) -> (Arc<GraphCore>, Vec<(String, Arc<GraphCore>)>) {
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
}

/// Evaluate a parsed dataset query, binding the outbound `SERVICE` client when the
/// `sparql-service` feature is on AND the SSRF allowlist is non-empty (CONCEPT:EG-KG.query.sparql-service-federation-client).
/// Otherwise (feature off, or allowlist empty) NO client is bound — SERVICE is fail-closed.
/// Runs inside the caller's `spawn_blocking` (the `ureq` client is blocking).
fn run_dataset_query(ds: &Dataset, query: &str) -> Result<QueryOutcome, String> {
    #[cfg(feature = "sparql-service")]
    {
        let client = ServiceClient::from_env();
        let svc: Option<&dyn eg_rdf::sparql::RemoteSparql> = client
            .as_ref()
            .map(|c| c as &dyn eg_rdf::sparql::RemoteSparql);
        eg_rdf::sparql::execute(ds, query, &Projection::raw(), svc)
    }
    #[cfg(not(feature = "sparql-service"))]
    {
        eg_rdf::sparql::execute(ds, query, &Projection::raw(), None)
    }
}

// ── SPARQL SERVICE federation client (CONCEPT:EG-KG.query.sparql-service-federation-client, feature `sparql-service`) ───

/// A `ureq`-backed [`eg_rdf::sparql::RemoteSparql`] with an SSRF allowlist. Reuses the SAME
/// pure-Rust rustls `ureq` stack `federation` already links (no new crate enters the tree).
///
/// `pub(crate)` (CA-12, feature `sparql-fuseki`): `server::sparql_service`'s Fuseki startup
/// health-check and qualification-test harness reuse this EXACT guarded client rather than
/// re-implementing the SSRF allowlist a second time — the type and `from_env` need
/// crate-visibility for that reuse. No behavior changed; `select`'s dispatch is unchanged
/// (trait-method visibility already follows `RemoteSparql`, which is `pub`).
#[cfg(feature = "sparql-service")]
pub(crate) struct ServiceClient {
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

    /// Build directly from an explicit allowlist, bypassing the process environment.
    /// `pub(crate)` (CA-12): lets `server::sparql_service`'s qualification tests exercise
    /// the exact same guarded client production code uses WITHOUT mutating the shared,
    /// process-global `SERVICE_ALLOW_ENV` var — several of those tests run concurrently in
    /// the same test binary, and env-var mutation is not per-test-thread isolated in Rust,
    /// so racing `std::env::set_var` calls would make the suite flaky. Not used by
    /// production code (which always goes through `from_env`, so a live deploy's
    /// fail-closed default is untouched by this constructor's existence).
    #[cfg(test)]
    pub(crate) fn with_allow(allow: Vec<String>) -> Self {
        Self { allow }
    }

    /// Build from `SERVICE_ALLOW_ENV`. Empty / unset ⇒ `None` (SERVICE disabled, fail-closed).
    /// `pub(crate)`: see the CA-12 note on the struct above.
    pub(crate) fn from_env() -> Option<Self> {
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
            _ => (
                authority,
                if endpoint.starts_with("https://") {
                    443
                } else {
                    80
                },
            ),
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
        .map(|a| {
            a.iter()
                .filter_map(|v| v.as_str().map(String::from))
                .collect()
        })
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

// ── hand-written SPARQL 1.1 Query Results serializers (CONCEPT:EG-KG.ontology.content-negotiation-serializers) ────────────

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
    fn signed_update_inventory_is_deterministic_and_detects_variable_graphs() {
        let graphs = update_graphs(
            "INSERT DATA { GRAPH <urn:g2> { <urn:s> <urn:p> <urn:o> } }",
            "urn:g1",
        )
        .unwrap();
        assert_eq!(graphs, vec!["urn:g1".to_string(), "urn:g2".to_string()]);
        assert!(update_uses_variable_graph(
            "DELETE { GRAPH ?g { ?s ?p ?o } } WHERE { GRAPH ?g { ?s ?p ?o } }"
        ));
        assert!(!update_uses_variable_graph(
            "INSERT DATA { GRAPH <urn:g> { <urn:s> <urn:p> <urn:o> } }"
        ));
    }

    /// `run_query` as a caller holding the configured static bearer token.
    async fn authorized_query(
        state: &Arc<RwLock<ServerState>>,
        query: &str,
        accept: &str,
        fmt_override: Option<&str>,
    ) -> (&'static str, &'static str, String) {
        let bearer = crate::server::auth::BearerCredential::Static("s3cret".to_string());
        let header = "Bearer s3cret";
        run_query(
            state,
            query,
            "no-such-graph",
            accept,
            fmt_override,
            Some(&bearer),
            header,
        )
        .await
    }

    /// Pins the read leg end to end: the bearer gate, content negotiation for each
    /// outcome shape, the result bodies, and the SPARQL-error mapping.
    #[tokio::test]
    async fn run_query_pins_bearer_gate_negotiation_and_errors() {
        let state = Arc::new(RwLock::new(ServerState::new_for_test(
            "test",
            crate::isolation::IsolationLayer::new(),
        )));
        let bearer = crate::server::auth::BearerCredential::Static("s3cret".to_string());
        let select = "SELECT ?s WHERE { ?s ?p ?o }";
        let ask = "ASK { ?s ?p ?o }";
        for (credential, header) in [(None, "Bearer s3cret"), (Some(&bearer), "Bearer nope")] {
            let (status, ct, body) =
                run_query(&state, select, "__commons__", "", None, credential, header).await;
            assert_eq!((status, ct), ("403 Forbidden", "text/plain"));
            assert!(body.starts_with("ACCESS_DENIED: SPARQL SELECT/CONSTRUCT/ASK over HTTP"));
        }
        const JSON: &str = "application/sparql-results+json";
        const XML: &str = "application/sparql-results+xml";
        let cases: [(&str, &str, Option<&str>, &str, &str); 8] = [
            (select, "", None, "200 OK", JSON),
            (select, XML, None, "200 OK", XML),
            (select, "text/csv", None, "200 OK", "text/csv"),
            (
                select,
                "",
                Some("tsv"),
                "200 OK",
                "text/tab-separated-values",
            ),
            (ask, "text/csv", None, "200 OK", "text/csv"),
            (ask, "", None, "200 OK", JSON),
            (
                "CONSTRUCT { ?s ?p ?o } WHERE { ?s ?p ?o }",
                "text/turtle",
                None,
                "200 OK",
                "text/turtle",
            ),
            ("SELECT WHERE {", "", None, "400 Bad Request", "text/plain"),
        ];
        for (query, accept, fmt, status, ct) in cases {
            let (got_status, got_ct, _) = authorized_query(&state, query, accept, fmt).await;
            assert_eq!(
                (got_status, got_ct),
                (status, ct),
                "{query} / {accept} / {fmt:?}"
            );
        }
        assert_eq!(
            authorized_query(&state, ask, "text/csv", None).await.2,
            "false"
        );
        assert_eq!(
            authorized_query(&state, ask, "", None).await.2,
            "{\"head\":{},\"boolean\":false}"
        );
        assert_eq!(
            authorized_query(&state, select, "", None).await.2,
            "{\"head\":{\"vars\":[\"s\"]},\"results\":{\"bindings\":[]}}"
        );
        let error = authorized_query(&state, "SELECT WHERE {", "", None).await.2;
        assert!(error.starts_with("SPARQL error: "), "{error}");
    }

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

    // ── CONCEPT:EG-KG.ontology.content-negotiation-serializers content negotiation + serializers ─────────────────────────

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
        assert_eq!(
            negotiate("", SELECT_FORMS),
            "application/sparql-results+json"
        );
        assert_eq!(
            negotiate("*/*", SELECT_FORMS),
            "application/sparql-results+json"
        );
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
            negotiate(
                "text/csv;q=0.5, application/sparql-results+xml;q=0.9",
                SELECT_FORMS
            ),
            "application/sparql-results+xml"
        );
    }

    /// CONCEPT:EG-KG.ontology.eg-concrete-syntax-matrix/EG-137 — the concrete-syntax matrix is content-negotiable on the
    /// graph forms: N-Quads/TriG/JSON-LD (always) and RDF/XML (under `rdf-xml`), by both
    /// Accept header and the `output=`/`format=` short token, and each serializes.
    #[test]
    fn eg136_137_graph_matrix_is_negotiable_and_serializes() {
        assert_eq!(
            negotiate("application/n-quads", GRAPH_FORMS),
            "application/n-quads"
        );
        assert_eq!(
            negotiate("application/trig", GRAPH_FORMS),
            "application/trig"
        );
        assert_eq!(
            negotiate("application/ld+json", GRAPH_FORMS),
            "application/ld+json"
        );
        assert_eq!(
            choose_ct("", Some("nq"), GRAPH_FORMS),
            "application/n-quads"
        );
        assert_eq!(choose_ct("", Some("trig"), GRAPH_FORMS), "application/trig");
        assert_eq!(
            choose_ct("", Some("jsonld"), GRAPH_FORMS),
            "application/ld+json"
        );
        // Default is still N-Triples (forms[0]) — matrix additions do not shift it.
        assert_eq!(negotiate("", GRAPH_FORMS), "application/n-triples");

        let triples =
            eg_rdf::mapping::parse_turtle("@prefix ex: <http://example.org/> . ex:a ex:p ex:b .")
                .unwrap();
        assert!(
            serialize_graph("application/n-quads", &triples)
                .unwrap()
                .contains("ex")
                || serialize_graph("application/n-quads", &triples)
                    .unwrap()
                    .contains("http://example.org/")
        );
        assert!(serialize_graph("application/trig", &triples).is_ok());
        assert!(serialize_graph("application/ld+json", &triples)
            .unwrap()
            .contains("@id"));
        #[cfg(feature = "rdf-xml")]
        {
            assert_eq!(
                negotiate("application/rdf+xml", GRAPH_FORMS),
                "application/rdf+xml"
            );
            assert_eq!(
                choose_ct("", Some("rdfxml"), GRAPH_FORMS),
                "application/rdf+xml"
            );
            assert!(serialize_graph("application/rdf+xml", &triples)
                .unwrap()
                .contains("RDF"));
        }
    }

    #[test]
    fn format_override_wins_and_falls_back() {
        assert_eq!(choose_ct("", Some("csv"), SELECT_FORMS), "text/csv");
        assert_eq!(
            choose_ct("text/csv", Some("xml"), SELECT_FORMS),
            "application/sparql-results+xml"
        );
        // An override invalid for this form is ignored → negotiate the Accept header.
        assert_eq!(
            choose_ct("text/csv", Some("turtle"), SELECT_FORMS),
            "text/csv"
        );
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
        assert!(boolean_body("application/sparql-results+xml", false)
            .contains("<boolean>false</boolean>"));
        assert_eq!(boolean_body("text/csv", true), "true");
    }

    // ── CONCEPT:EG-KG.query.sparql-service-federation-client — SPARQL SERVICE federation client ────────────────────────

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
        assert_eq!(
            sol.get("s").unwrap(),
            &Binding::Node("<http://x>".to_string())
        );
        assert_eq!(sol.get("b").unwrap(), &Binding::Node("_:n1".to_string()));
        assert_eq!(sol.get("l").unwrap(), &Binding::Literal("hi".to_string()));
        // And `term_json` maps them straight back to the same JSON term kinds.
        assert_eq!(term_json(sol.get("s").unwrap())["type"], "uri");
        assert_eq!(term_json(sol.get("b").unwrap())["type"], "bnode");
        assert_eq!(term_json(sol.get("l").unwrap())["type"], "literal");
    }

    // ── CONCEPT:EG-KG.query.graph-store-http-protocol — Graph Store HTTP Protocol ──────────────────────────────

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
        assert_eq!(
            gsp_target("/rdf-graphs/service", &d),
            Some(gsp_default_graph())
        );
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
        assert_eq!(
            parse_rdf_body("application/n-triples", nt).unwrap().len(),
            1
        );
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
        let seed =
            parse_rdf_body("text/turtle", "<http://ex/a> <http://ex/p> <http://ex/b> .").unwrap();
        eg_rdf::update::insert_triples(&core, &seed).unwrap();
        // POST = merge: no clear.
        let add =
            parse_rdf_body("text/turtle", "<http://ex/c> <http://ex/q> <http://ex/d> .").unwrap();
        eg_rdf::update::insert_triples(&core, &add).unwrap();
        let exported = export_graph(&core, "http://ex/g").unwrap();
        assert_eq!(
            exported.len(),
            2,
            "both the seeded and posted triples are present"
        );
    }

    /// DELETE empties the graph: after a clear, the export is empty.
    #[test]
    fn gsp_delete_empties() {
        let core = GraphCore::new();
        let t =
            parse_rdf_body("text/turtle", "<http://ex/a> <http://ex/p> <http://ex/b> .").unwrap();
        eg_rdf::update::insert_triples(&core, &t).unwrap();
        assert_eq!(export_graph(&core, "g").unwrap().len(), 1);
        core.clear();
        assert!(
            export_graph(&core, "g").unwrap().is_empty(),
            "DELETE empties the graph"
        );
    }

    /// GET content-negotiation: Turtle when accepted, N-Triples by default, and each
    /// serializer emits its own syntax.
    #[test]
    fn gsp_get_content_negotiation() {
        assert_eq!(choose_ct("text/turtle", None, GRAPH_FORMS), "text/turtle");
        assert_eq!(choose_ct("", None, GRAPH_FORMS), "application/n-triples");
        let core = GraphCore::new();
        let t =
            parse_rdf_body("text/turtle", "<http://ex/a> <http://ex/p> <http://ex/b> .").unwrap();
        eg_rdf::update::insert_triples(&core, &t).unwrap();
        let exported = export_graph(&core, "g").unwrap();
        let ttl = eg_rdf::mapping::to_turtle(&exported).unwrap();
        let nt = eg_rdf::mapping::to_ntriples(&exported).unwrap();
        assert!(nt.contains("<http://ex/a> <http://ex/p> <http://ex/b> ."));
        assert!(
            ttl.contains("http://ex/a"),
            "turtle serializer emits the subject"
        );
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
