//! Iceberg-REST catalog HTTP surface (INT-P2-3, `iceberg.apache.org/rest-catalog-spec`,
//! feature `lake-rest`).
//!
//! A hand-rolled, dependency-free HTTP/1.1 listener — the SAME idiom as the Prometheus
//! `--metrics-addr` exposition / `crate::server::sparql_http` / `crate::server::obs`
//! (NO axum/hyper/warp, so the Pi contract holds) — serving the Iceberg-REST endpoints a
//! standard client (PyIceberg / Spark / Trino) issues to discover, open, create, drop, and
//! rename a table (W03, GOC-75-W03):
//!
//! * `GET  /v1/config`                              → `{"defaults":{},"overrides":{}}`
//! * `POST /v1/oauth/tokens`                         → re-verifies an already Keycloak-
//!   issued bearer through the SAME `JwtValidator` every other endpoint uses (this engine
//!   is not an authorization server — it issues no NEW tokens)
//! * `GET  /v1/namespaces[?pageToken=&pageSize=]`    → `ListNamespaces` (paginated)
//! * `GET  /v1/namespaces/{ns}`                      → `GetNamespace` (exists check)
//! * `GET  /v1/namespaces/{ns}/tables[?pageToken=&pageSize=]` → `ListTables` (paginated)
//! * `POST /v1/namespaces/{ns}/tables`               → `CreateTable`
//! * `GET  /v1/namespaces/{ns}/tables/{table}`       → `LoadTable` (INLINE `metadata`, so
//!   a client needs no second fetch to open the table)
//! * `HEAD /v1/namespaces/{ns}/tables/{table}`       → `TableExists`
//! * `POST /v1/namespaces/{ns}/tables/{table}`       → `CommitTable` (see the honest scope
//!   note on [`super::LakeManager::commit_table`] — accepted per the spec's request/
//!   response envelope, but bridges to the engine's OWN compaction pass rather than
//!   ingesting an externally-authored manifest)
//! * `DELETE /v1/namespaces/{ns}/tables/{table}`     → `DropTable`
//! * `POST /v1/tables/rename`                        → `RenameTable`
//!
//! `{ns}` accepts the spec's multi-level `\x1f`-joined namespace identifier
//! (percent-encoded `%1F` on the wire); it round-trips through this tier's flat-string
//! namespace model losslessly (see [`super::namespace_levels`]) rather than requiring
//! `eg-lake`'s catalog itself to model nesting.
//!
//! **W04 (tenant/RLS projection):** every read/write above is projected through a
//! [`super::LakeVisibility`] derived from the request's verified `CarrierAuthority` — see
//! that type's docs for why this keys on per-agent ownership (`owner_scope`) rather than
//! `EPISTEMIC_GRAPH_TENANT` (already enforced, single-valued per deployment, at carrier-
//! minting time by BUG-222's W01/W02 fix).
//!
//! **W05 (audit + rate limits):** every denial and mutation is recorded through
//! [`super::LakeManager::record_audit`]; a per-source-IP token-bucket
//! ([`EPISTEMIC_GRAPH_ICEBERG_RATE_LIMIT_PER_MIN`]) sheds excess traffic with a typed
//! `429`, opt-in like every other rate/interval knob in this codebase (unset ⇒ disabled).

use std::collections::HashMap;
use std::sync::Arc;

use serde_json::{json, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::RwLock;

use crate::server::access::CarrierAuthority;
use crate::server::blob::store::ChunkStore;
use crate::server::oidc::{JwtValidator, VerifiedTokenClaims};
use crate::server::ServerState;
use eg_lake::schema::{LakeField, LakeSchema, LakeType};

use super::{namespace_levels, CreateTableError, LakeManager, LakeVisibility, RenameTableError};

/// Env var carrying the Iceberg-REST listener bind address (`host:port`). Unset ⇒ no
/// listener (matches `--metrics-addr`/`--sparql-addr`/`--obs-addr`'s opt-in idiom).
pub const ICEBERG_ADDR_ENV: &str = "EPISTEMIC_GRAPH_ICEBERG_ADDR";
/// Env var for the per-source-IP token-bucket rate limit, requests per minute (W05,
/// GOC-75-W05). Positive integer ⇒ enabled; unset/invalid/non-positive ⇒ disabled —
/// the same "0/unset = off" convention every interval knob in this codebase uses.
pub const ICEBERG_RATE_LIMIT_ENV: &str = "EPISTEMIC_GRAPH_ICEBERG_RATE_LIMIT_PER_MIN";
const MAX_HTTP_HEADER_BYTES: usize = 64 * 1024;
const MAX_HTTP_BODY_BYTES: usize = 1024 * 1024;
const HTTP_READ_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(15);

/// Serve the Iceberg-REST catalog surface on `listener`, backed by `lake` (the tables +
/// catalog) and `store` (the blob CAS a `CommitTable` compaction reads/writes through).
pub async fn serve(listener: TcpListener, lake: Arc<LakeManager>, store: Arc<dyn ChunkStore>) {
    serve_inner(listener, lake, store, None).await;
}

/// Production Iceberg listener linked to live engine isolation (BUG-222). A
/// `CarrierAuthority` is minted from the Iceberg REST spec's own OAuth2
/// bearer convention (`/v1/oauth/tokens`), verified against the deployment's
/// configured Keycloak/JWKS issuer (`EPISTEMIC_GRAPH_ICEBERG_JWT_*`) — no
/// bearer, an invalid bearer, or a bearer verified for a different tenant all
/// fail the carrier closed, same as before this fix; only a bearer that BOTH
/// verifies AND asserts this deployment's own tenant now succeeds.
pub async fn serve_with_security(
    listener: TcpListener,
    lake: Arc<LakeManager>,
    store: Arc<dyn ChunkStore>,
    state: Arc<RwLock<ServerState>>,
) {
    serve_inner(listener, lake, store, Some(state)).await;
}

/// The Iceberg-REST surface's own OAuth2 bearer credential (BUG-222):
/// resolved once at listener start from `EPISTEMIC_GRAPH_ICEBERG_JWT_*`
/// (falling back to the platform-wide `FASTMCP_SERVER_AUTH_JWT_*`/`OIDC_*`
/// vars, mirroring the KV-cache/`/sparql` bearer surfaces —
/// `oidc::JwtValidator::from_env_iceberg`). There is no static-secret
/// fallback here (unlike the KV-cache surface): the Iceberg REST spec's own
/// native mechanism is an OAuth2 bearer, a deliberate scheme decision, so an
/// unconfigured deployment stays fail-closed rather than accepting a shared
/// secret this protocol was never meant to carry.
struct IcebergCredential {
    validator: JwtValidator,
}

fn resolve_iceberg_credential() -> Result<Option<IcebergCredential>, String> {
    Ok(JwtValidator::from_env_iceberg()?.map(|validator| IcebergCredential { validator }))
}

/// Extract the raw bearer token from an `Authorization: Bearer <token>` header, if
/// present and well-formed. Shared by the per-request carrier guard
/// ([`verify_bearer`]) and the `/v1/oauth/tokens` endpoint (which additionally accepts
/// the token via form fields — see [`handle_oauth_token`]).
fn extract_bearer(headers: &HashMap<String, String>) -> Option<String> {
    headers
        .get("authorization")
        .and_then(|header| header.strip_prefix("Bearer "))
        .map(str::trim)
        .filter(|token| !token.is_empty())
        .map(str::to_string)
}

/// Verify the request's `Authorization: Bearer <token>` against `credential`
/// (RSA/JWKS signature + issuer + audience + expiry — `JwtValidator::
/// validate_claims`). `None` on a missing header, a malformed/non-Bearer
/// header, or a token that fails verification.
fn verify_bearer(
    credential: Option<&IcebergCredential>,
    headers: &HashMap<String, String>,
) -> Option<VerifiedTokenClaims> {
    let credential = credential?;
    let token = extract_bearer(headers)?;
    credential.validator.validate_claims(&token)
}

/// Resolve THIS request's [`CarrierAuthority`] and whether it is denied (BUG-222,
/// W04). `state.is_none()` ⇒ the non-security `serve()` path (used by this module's
/// own `handle()`-level tests and any caller with no live engine isolation to bind
/// to) — gating is a no-op there, AND no carrier is produced (so downstream
/// [`LakeVisibility`] is `Unfiltered`), mirroring the pre-existing behavior byte for
/// byte. Otherwise: no verified bearer, or a verified bearer whose tenant claim
/// disagrees with this deployment's own configured tenant, both deny through the
/// SAME shared `access::unauthenticated_carrier_denied` gate every other auxiliary
/// surface uses; a bearer that both verifies AND matches yields `Some(carrier)`.
fn resolve_carrier(
    state: Option<&Arc<RwLock<ServerState>>>,
    credential: Option<&IcebergCredential>,
    headers: &HashMap<String, String>,
) -> Result<Option<CarrierAuthority>, ()> {
    if state.is_none() {
        return Ok(None);
    }
    let verified = verify_bearer(credential, headers);
    let carrier = crate::server::auth::mint_iceberg_carrier(verified.as_ref());
    if crate::server::access::unauthenticated_carrier_denied(carrier.as_ref()) {
        Err(())
    } else {
        Ok(carrier)
    }
}

fn visibility_for(carrier: Option<&CarrierAuthority>) -> LakeVisibility {
    match carrier {
        None => LakeVisibility::Unfiltered,
        Some(c) if c.is_admin() => LakeVisibility::Unfiltered,
        Some(c) => LakeVisibility::Owner(c.owner_scope().to_string()),
    }
}

/// The minimum scope one Iceberg-REST operation needs (NE-048, P0). `None` for a
/// route that carries no per-table/-namespace authority of its own
/// (`GET /v1/config` is static capability advertisement; `/v1/oauth/tokens` is
/// handled before a carrier is even resolved, in `handle_open`/its callers — see
/// [`resolve_carrier`]'s doc comment) — such a route is never gated here.
///
/// This is the "map the REST surface's operations to the minimum scope each
/// needs" half of the fix: `POST`/`DELETE` (create/commit/drop/rename a table)
/// mutate the catalog and require `kg:write`; every `GET`/`HEAD` (list/exists/
/// load) requires only `kg:read`. An unmatched (method, path) pair returns `None`
/// too — `handle`'s own routing match falls through to `404 Not Found` for those,
/// so there is nothing to gate.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum IcebergScope {
    Read,
    Write,
}

fn operation_scope(method: &str, segs: &[&str]) -> Option<IcebergScope> {
    match (method, segs) {
        ("GET", ["v1", "config"]) => None,
        ("GET", ["v1", "namespaces"]) => Some(IcebergScope::Read),
        ("GET", ["v1", "namespaces", _ns]) => Some(IcebergScope::Read),
        ("GET", ["v1", "namespaces", _ns, "tables"]) => Some(IcebergScope::Read),
        ("POST", ["v1", "namespaces", _ns, "tables"]) => Some(IcebergScope::Write),
        ("GET", ["v1", "namespaces", _ns, "tables", _table]) => Some(IcebergScope::Read),
        ("HEAD", ["v1", "namespaces", _ns, "tables", _table]) => Some(IcebergScope::Read),
        ("POST", ["v1", "namespaces", _ns, "tables", _table]) => Some(IcebergScope::Write),
        ("DELETE", ["v1", "namespaces", _ns, "tables", _table]) => Some(IcebergScope::Write),
        ("POST", ["v1", "tables", "rename"]) => Some(IcebergScope::Write),
        _ => None,
    }
}

/// Is `carrier` entitled to perform an operation requiring `scope`?
///
/// `carrier: None` is the non-security `serve()` path (no live engine
/// isolation to bind to — see `handle`'s own doc comment); it stays
/// ungated here exactly as it was before this scope gate existed, matching
/// [`visibility_for`]'s identical `None => Unfiltered` treatment. `scope:
/// None` (a route [`operation_scope`] does not classify) is always allowed —
/// there is nothing to check.
///
/// A `Some(carrier)` is admin-unconditional (mirrors [`visibility_for`]);
/// otherwise it must carry the specific `kg:read`/`kg:write` grant the
/// operation needs. Before NE-048, `authenticated_iceberg_bearer` minted
/// EVERY verified, correctly-tenanted bearer with both scopes hardcoded, so
/// this check was structurally unreachable-false for any real bearer; it now
/// reflects the bearer's own verified scope claim
/// (`CarrierAuthority::can_read`/`can_write`).
fn scope_authorized(carrier: Option<&CarrierAuthority>, scope: Option<IcebergScope>) -> bool {
    let (Some(carrier), Some(scope)) = (carrier, scope) else {
        return true;
    };
    if carrier.is_admin() {
        return true;
    }
    match scope {
        IcebergScope::Read => carrier.can_read(),
        IcebergScope::Write => carrier.can_write(),
    }
}

/// A minimal per-source-IP token bucket (W05, GOC-75-W05). Deliberately
/// dependency-free (no new crate) — a bounded `HashMap` behind the SAME
/// `parking_lot::Mutex` idiom `LakeManager` already uses. Keyed by source IP rather
/// than by authenticated principal so an unauthenticated flood (including a flood of
/// invalid bearers) is governed too, not just successfully-authenticated traffic.
struct RateLimiter {
    capacity: f64,
    refill_per_sec: f64,
    buckets: parking_lot::Mutex<HashMap<String, (f64, std::time::Instant)>>,
}

impl RateLimiter {
    fn new(per_minute: f64) -> Self {
        RateLimiter {
            capacity: per_minute.max(1.0),
            refill_per_sec: per_minute.max(1.0) / 60.0,
            buckets: parking_lot::Mutex::new(HashMap::new()),
        }
    }

    /// `true` = admitted (a token was consumed), `false` = rate-limited.
    fn try_admit(&self, key: &str) -> bool {
        let mut buckets = self.buckets.lock();
        let now = std::time::Instant::now();
        let entry = buckets
            .entry(key.to_string())
            .or_insert((self.capacity, now));
        let elapsed = now.duration_since(entry.1).as_secs_f64();
        entry.0 = (entry.0 + elapsed * self.refill_per_sec).min(self.capacity);
        entry.1 = now;
        if entry.0 >= 1.0 {
            entry.0 -= 1.0;
            true
        } else {
            false
        }
    }
}

fn resolve_rate_limiter() -> Option<RateLimiter> {
    let raw = std::env::var(ICEBERG_RATE_LIMIT_ENV).ok()?;
    let per_minute: f64 = raw.trim().parse().ok()?;
    if per_minute > 0.0 {
        Some(RateLimiter::new(per_minute))
    } else {
        None
    }
}

async fn serve_inner(
    listener: TcpListener,
    lake: Arc<LakeManager>,
    store: Arc<dyn ChunkStore>,
    security_state: Option<Arc<RwLock<ServerState>>>,
) {
    if let Err(error) = crate::server::require_loopback_listener(&listener) {
        tracing::error!("Iceberg listener refused: {error}");
        return;
    }
    // Resolved ONCE for the listener's lifetime (mirrors the KV-cache
    // surface's `resolve_auth()`): a live JWKS refetch happens per-`kid`-miss
    // inside `JwtValidator`, not per request. A misconfigured issuer (one set
    // without its mandatory audience/JWKS URL) or no issuer at all both leave
    // `credential` `None`, so every request is denied — fail closed, never a
    // startup crash of the whole server process.
    let credential = if security_state.is_some() {
        match resolve_iceberg_credential() {
            Ok(credential) => credential,
            Err(error) => {
                tracing::error!("Iceberg-REST OAuth2 bearer verifier misconfigured: {error}");
                None
            }
        }
    } else {
        None
    };
    let credential = credential.map(Arc::new);
    let rate_limiter = resolve_rate_limiter().map(Arc::new);
    loop {
        let Ok((mut stream, peer)) = listener.accept().await else {
            continue;
        };
        let lake = lake.clone();
        let store = store.clone();
        let security_state = security_state.clone();
        let credential = credential.clone();
        let rate_limiter = rate_limiter.clone();
        tokio::spawn(async move {
            let (status, body) =
                match tokio::time::timeout(HTTP_READ_TIMEOUT, read_request(&mut stream)).await {
                    Ok(Some(req)) => {
                        if let Some(limiter) = rate_limiter.as_ref() {
                            if !limiter.try_admit(&peer.ip().to_string()) {
                                lake.record_audit(json!({
                                    "ts_ms": crate::server::lake::lineage::now_ms(),
                                    "op": "RateLimited",
                                    "method": req.method,
                                    "path": req.target,
                                    "source_ip": peer.ip().to_string(),
                                    "outcome": "deny",
                                }));
                                return respond(
                                    &mut stream,
                                    "429 Too Many Requests",
                                    err_body(
                                        "Iceberg-REST request rate exceeded",
                                        "RateLimitedException",
                                        429,
                                    ),
                                )
                                .await;
                            }
                        }
                        route(&lake, store.as_ref(), &req, security_state.as_ref(), credential.as_deref())
                    }
                    _ => (
                        "400 Bad Request",
                        err_body("malformed HTTP request", "BadRequestException", 400),
                    ),
                };
            respond(&mut stream, status, body).await;
        });
    }
}

async fn respond(stream: &mut TcpStream, status: &str, body: String) {
    // `handle`'s HEAD arm already returns an empty body, so the response
    // envelope needs no extra verb-tracking here (unlike a framework that
    // strips the body post-hoc) — content-length is 0 for HEAD/404-empty
    // responses and the body write below is simply empty.
    let resp = format!(
        "HTTP/1.1 {status}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
        body.len(),
        body
    );
    let _ = stream.write_all(resp.as_bytes()).await;
    let _ = stream.shutdown().await;
}

/// Top-level per-request dispatch: `/v1/oauth/tokens` is reachable WITHOUT a
/// pre-existing carrier (that is the whole point of a token endpoint — a client
/// presents a bearer to get "the identical authentication semantics" per the
/// lane's design note, it does not already have a gated carrier), so it is routed
/// BEFORE the carrier gate applies to every other endpoint.
fn route(
    lake: &LakeManager,
    store: &dyn ChunkStore,
    req: &HttpRequest,
    security_state: Option<&Arc<RwLock<ServerState>>>,
    credential: Option<&IcebergCredential>,
) -> (&'static str, String) {
    let path = req.target.split('?').next().unwrap_or(&req.target);
    let segs: Vec<String> = path
        .trim_matches('/')
        .split('/')
        .filter(|s| !s.is_empty())
        .map(percent_decode)
        .collect();
    let seg_refs: Vec<&str> = segs.iter().map(String::as_str).collect();

    if req.method == "POST" && seg_refs.as_slice() == ["v1", "oauth", "tokens"] {
        let (status, body) = handle_oauth_token(credential, req);
        if status != "200 OK" {
            lake.record_audit(json!({
                "ts_ms": crate::server::lake::lineage::now_ms(),
                "op": "OAuthTokenDenied",
                "outcome": "deny",
                "status": status,
            }));
        }
        return (status, body);
    }

    match resolve_carrier(security_state, credential, &req.headers) {
        Err(()) => {
            lake.record_audit(json!({
                "ts_ms": crate::server::lake::lineage::now_ms(),
                "op": "AccessDenied",
                "method": req.method,
                "path": req.target,
                "outcome": "deny",
            }));
            (
                "403 Forbidden",
                err_body(
                    "Iceberg carrier has no verified tenant/table ownership",
                    "ForbiddenException",
                    403,
                ),
            )
        }
        Ok(carrier) => handle(lake, store, req, carrier.as_ref()),
    }
}

/// A parsed HTTP/1.1 request: method, raw target (`/v1/…?…`), headers
/// (lowercased keys — BUG-222 adds this so the OAuth2 bearer guard can read
/// `authorization`), body.
struct HttpRequest {
    method: String,
    target: String,
    origin: String,
    headers: HashMap<String, String>,
    body: String,
}

/// Read one HTTP/1.1 request: headers up to the blank line, then the `Content-Length`
/// body. Mirrors `crate::server::sparql_http`'s reader (the SAME dependency-free idiom).
async fn read_request(stream: &mut TcpStream) -> Option<HttpRequest> {
    let mut buf = Vec::new();
    let mut tmp = [0u8; 4096];
    let header_end = loop {
        if let Some(pos) = find_subslice(&buf, b"\r\n\r\n") {
            break pos;
        }
        let n = stream.read(&mut tmp).await.ok()?;
        if n == 0 {
            return None;
        }
        buf.extend_from_slice(&tmp[..n]);
        if buf.len() > MAX_HTTP_HEADER_BYTES {
            return None;
        }
    };
    let head = String::from_utf8_lossy(&buf[..header_end]).to_string();
    let mut lines = head.split("\r\n");
    let request_line = lines.next()?;
    let mut parts = request_line.split_whitespace();
    let method = parts.next()?.to_string();
    let target = parts.next()?.to_string();
    let version = parts.next()?;
    if !version.starts_with("HTTP/1.") || parts.next().is_some() {
        return None;
    }

    let mut content_length: Option<usize> = None;
    let mut origin = String::new();
    let mut origin_seen = false;
    let mut headers: HashMap<String, String> = HashMap::new();
    for line in lines {
        let (k, v) = line.split_once(':')?;
        let key = k.trim().to_ascii_lowercase();
        let value = v.trim().to_string();
        if key.is_empty() {
            return None;
        }
        if key == "content-length" {
            if content_length.is_some() {
                return None;
            }
            content_length = Some(value.parse().ok()?);
        } else if key == "transfer-encoding" {
            return None;
        } else if key == "origin" {
            if origin_seen {
                return None;
            }
            origin_seen = true;
            origin = value.clone();
        }
        // BUG-222: capture every header (lowercased) so the OAuth2 bearer
        // guard can read `authorization` — mirrors `kvcache_http`'s reader.
        headers.insert(key, value);
    }
    let content_length = content_length.unwrap_or(0);
    if content_length > MAX_HTTP_BODY_BYTES {
        return None;
    }
    let mut body = buf[header_end + 4..].to_vec();
    if body.len() > content_length || body.len() > MAX_HTTP_BODY_BYTES {
        return None;
    }
    while body.len() < content_length {
        let n = stream.read(&mut tmp).await.ok()?;
        if n == 0 {
            break;
        }
        body.extend_from_slice(&tmp[..n]);
    }
    if body.len() != content_length {
        return None;
    }
    Some(HttpRequest {
        method,
        target,
        origin,
        headers,
        body: String::from_utf8_lossy(&body).to_string(),
    })
}

fn err_body(message: &str, kind: &str, code: u16) -> String {
    json!({ "error": { "message": message, "type": kind, "code": code } }).to_string()
}

fn not_found(kind: &str) -> String {
    err_body(&format!("{kind} not found"), "NoSuchTableException", 404)
}

/// The OAuth2 error envelope (RFC 6749 §5.2 / the spec's `OAuthErrorResponse`) —
/// deliberately DIFFERENT from [`err_body`]'s `{"error":{...}}` object shape: OAuth's
/// `error` field is a bare token string, not a nested object.
fn oauth_error_body(error: &str, description: &str) -> String {
    json!({ "error": error, "error_description": description }).to_string()
}

fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack.windows(needle.len()).position(|w| w == needle)
}

fn hex_val(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

/// Minimal percent-decoder (W03, GOC-75-W03): turns `%1F` (the spec's multi-level
/// namespace separator) and any other `%XX` escape back into its raw byte. An
/// incomplete/invalid escape is passed through literally rather than erroring — a
/// client sending a malformed path gets a 404 from the route not matching, not a
/// crash.
fn percent_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            if let (Some(h), Some(l)) = (hex_val(bytes[i + 1]), hex_val(bytes[i + 2])) {
                out.push(h * 16 + l);
                i += 3;
                continue;
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).to_string()
}

/// `application/x-www-form-urlencoded` body decoder — only what
/// [`handle_oauth_token`] needs (no nested/repeated keys).
fn parse_form(body: &str) -> HashMap<String, String> {
    body.split('&')
        .filter(|pair| !pair.is_empty())
        .filter_map(|pair| {
            let mut it = pair.splitn(2, '=');
            let k = it.next()?;
            let v = it.next().unwrap_or("");
            Some((
                percent_decode(&k.replace('+', " ")),
                percent_decode(&v.replace('+', " ")),
            ))
        })
        .collect()
}

/// Parse the Iceberg-REST pagination query params (W03, GOC-75-W03): the spec names
/// the `in: query` parameters `pageToken`/`pageSize` (camelCase — distinct from the
/// `next-page-token` JSON response field, which stays kebab-case to match this
/// codebase's existing `metadata-location`-style wire fields).
fn parse_pagination(target: &str) -> (Option<String>, Option<usize>) {
    let mut page_token = None;
    let mut page_size = None;
    if let Some(query) = target.split_once('?').map(|(_, q)| q) {
        for pair in query.split('&') {
            let mut it = pair.splitn(2, '=');
            let k = it.next().unwrap_or("");
            let v = percent_decode(it.next().unwrap_or(""));
            match k {
                "pageToken" => page_token = Some(v),
                "pageSize" => page_size = v.parse::<usize>().ok(),
                _ => {}
            }
        }
    }
    (page_token, page_size)
}

/// Join a `TableIdentifier.namespace` JSON array (`["a","b"]`) back into this
/// tier's internal `\x1f`-joined flat namespace string — the inverse of
/// [`namespace_levels`]. `None` if the field is missing or not an array of strings.
fn table_ident_namespace(v: &Value) -> Option<String> {
    let levels = v.get("namespace")?.as_array()?;
    let parts: Option<Vec<&str>> = levels.iter().map(|l| l.as_str()).collect();
    Some(parts?.join("\u{1f}"))
}

/// Map an Iceberg schema field's `type` name onto this tier's [`LakeType`]
/// intersection set (W03, GOC-75-W03). Unsupported types (nested struct/list/map,
/// decimal, binary, fixed, date, time) are a typed `400 BadRequestException` rather
/// than a silent, lossy coercion — the same "never lossy-drop a materialized column"
/// discipline [`eg_lake::schema`]'s own docs state.
fn lake_type_from_iceberg(name: &str) -> Option<LakeType> {
    match name {
        "long" | "int" => Some(LakeType::Long),
        "double" | "float" => Some(LakeType::Double),
        "boolean" => Some(LakeType::Bool),
        "string" | "uuid" => Some(LakeType::String),
        "timestamp" | "timestamptz" => Some(LakeType::Timestamp),
        _ => None,
    }
}

/// Decode a `CreateTableRequest` body's `schema.fields[]` into a [`LakeSchema`].
fn schema_from_create_request(body: &Value) -> Result<LakeSchema, String> {
    let fields = body
        .get("schema")
        .and_then(|s| s.get("fields"))
        .and_then(Value::as_array)
        .ok_or_else(|| "CreateTableRequest.schema.fields is required".to_string())?;
    if fields.is_empty() {
        return Err("CreateTableRequest.schema.fields must not be empty".to_string());
    }
    let mut out = Vec::with_capacity(fields.len());
    for f in fields {
        let name = f
            .get("name")
            .and_then(Value::as_str)
            .ok_or_else(|| "a schema field is missing its name".to_string())?;
        let ty_name = f
            .get("type")
            .and_then(Value::as_str)
            .ok_or_else(|| format!("field {name} is missing a scalar type"))?;
        let ty = lake_type_from_iceberg(ty_name)
            .ok_or_else(|| format!("field {name} has unsupported Iceberg type {ty_name:?}"))?;
        let required = f.get("required").and_then(Value::as_bool).unwrap_or(false);
        out.push(if required {
            LakeField::required(name, ty)
        } else {
            LakeField::new(name, ty)
        });
    }
    Ok(LakeSchema::new(out))
}

/// `POST /v1/oauth/tokens` (W03, GOC-75-W03). This engine is not an OAuth2
/// authorization server — it mints no NEW tokens. It re-verifies an already
/// Keycloak-issued bearer through the SAME [`JwtValidator`] + the SAME
/// `auth::mint_iceberg_carrier` tenant check every other endpoint uses (the lane's
/// own design note: "identical authentication semantics as one presenting a
/// pre-minted bearer"), then echoes it back as the `access_token` — giving a client
/// that only speaks the spec's token-endpoint convention (rather than sending its
/// bearer directly) the same pass/fail outcome. The bearer may arrive as the
/// request's own `Authorization` header, or — PyIceberg's `token` catalog property
/// convention — as the form's `client_secret`/`subject_token`/`assertion` field.
fn handle_oauth_token(
    credential: Option<&IcebergCredential>,
    req: &HttpRequest,
) -> (&'static str, String) {
    let Some(credential) = credential else {
        return (
            "401 Unauthorized",
            oauth_error_body(
                "invalid_client",
                "Iceberg-REST OAuth2 bearer is not configured for this deployment",
            ),
        );
    };
    let form = parse_form(&req.body);
    let grant_type = form.get("grant_type").cloned().unwrap_or_default();
    if grant_type.trim().is_empty() {
        return (
            "400 Bad Request",
            oauth_error_body("invalid_request", "grant_type is required"),
        );
    }
    let candidate = extract_bearer(&req.headers)
        .or_else(|| form.get("client_secret").cloned())
        .or_else(|| form.get("subject_token").cloned())
        .or_else(|| form.get("assertion").cloned())
        .filter(|t| !t.trim().is_empty());
    let Some(token) = candidate else {
        return (
            "400 Bad Request",
            oauth_error_body(
                "invalid_request",
                "no bearer/client_secret/subject_token/assertion presented",
            ),
        );
    };
    let verified = credential.validator.validate_claims(&token);
    match verified
        .as_ref()
        .and_then(|v| crate::server::auth::mint_iceberg_carrier(Some(v)))
    {
        Some(_carrier) => (
            "200 OK",
            json!({ "access_token": token, "token_type": "bearer" }).to_string(),
        ),
        None => (
            "401 Unauthorized",
            oauth_error_body(
                "invalid_client",
                "presented credential does not verify for this deployment's tenant",
            ),
        ),
    }
}

/// Route + execute one already-authenticated request → `(status, body)`. Pure (sync,
/// aside from the audit-log side effect) so it stays fully unit-testable without a
/// socket, mirroring `crate::server::s3::handle`'s precedent. `carrier` is `None` on
/// the non-security `serve()` path (unfiltered — today's pre-W04 behavior) or an
/// admin caller; `Some` projects every read/write through [`LakeVisibility::Owner`].
///
/// W04's tenant/ownership projection above is necessary but not sufficient: a
/// verified, correctly-tenanted `Some(carrier)` must ALSO carry the scope the
/// specific operation needs (NE-048, P0) — [`operation_scope`] maps each route to
/// its minimum `kg:read`/`kg:write` requirement and [`scope_authorized`] gates on
/// it before any of the routing below runs.
fn handle(
    lake: &LakeManager,
    store: &dyn ChunkStore,
    req: &HttpRequest,
    carrier: Option<&CarrierAuthority>,
) -> (&'static str, String) {
    if !req.origin.is_empty() {
        return (
            "403 Forbidden",
            err_body("browser origin denied", "ForbiddenException", 403),
        );
    }
    let path = req.target.split('?').next().unwrap_or(&req.target);
    let segs: Vec<String> = path
        .trim_matches('/')
        .split('/')
        .filter(|s| !s.is_empty())
        .map(percent_decode)
        .collect();
    let seg_refs: Vec<&str> = segs.iter().map(String::as_str).collect();

    // NE-048 (P0): a verified, correctly-tenanted carrier is still only
    // entitled to what its OWN scope claim actually grants — reject a
    // kg:read-only carrier attempting a mutating operation (and, for
    // completeness of the same mapping, a kg:write-only carrier attempting a
    // read) before touching `lake` at all. Checked ahead of body parsing and
    // existence lookups deliberately: an unauthorized caller learns nothing
    // about whether the target namespace/table even exists.
    if !scope_authorized(carrier, operation_scope(req.method.as_str(), &seg_refs)) {
        lake.record_audit(json!({
            "ts_ms": crate::server::lake::lineage::now_ms(),
            "op": "InsufficientScope",
            "method": req.method,
            "path": req.target,
            "owner": carrier
                .map(|c| c.owner_scope().to_string())
                .unwrap_or_else(|| "system".to_string()),
            "outcome": "deny",
        }));
        return (
            "403 Forbidden",
            err_body(
                "Iceberg carrier is not authorized for this operation",
                "ForbiddenException",
                403,
            ),
        );
    }

    let visibility = visibility_for(carrier);
    let owner = carrier.map(|c| c.owner_scope().to_string());

    match (req.method.as_str(), seg_refs.as_slice()) {
        ("GET", ["v1", "config"]) => (
            "200 OK",
            json!({ "defaults": {}, "overrides": {} }).to_string(),
        ),
        ("GET", ["v1", "namespaces"]) => {
            let (page_token, page_size) = parse_pagination(&req.target);
            (
                "200 OK",
                lake.list_namespaces_visible(&visibility, page_token.as_deref(), page_size)
                    .to_string(),
            )
        }
        ("GET", ["v1", "namespaces", ns]) => {
            if lake.namespace_exists_visible(ns, &visibility) {
                (
                    "200 OK",
                    json!({ "namespace": namespace_levels(ns), "properties": {} }).to_string(),
                )
            } else {
                ("404 Not Found", not_found("namespace"))
            }
        }
        ("GET", ["v1", "namespaces", ns, "tables"]) => {
            let (page_token, page_size) = parse_pagination(&req.target);
            (
                "200 OK",
                lake.list_tables_visible(ns, &visibility, page_token.as_deref(), page_size)
                    .to_string(),
            )
        }
        ("POST", ["v1", "namespaces", ns, "tables"]) => {
            let body: Value = match serde_json::from_str(&req.body) {
                Ok(v) => v,
                Err(_) => {
                    return (
                        "400 Bad Request",
                        err_body("malformed CreateTableRequest body", "BadRequestException", 400),
                    )
                }
            };
            let Some(table) = body.get("name").and_then(Value::as_str).map(str::to_string) else {
                return (
                    "400 Bad Request",
                    err_body("CreateTableRequest.name is required", "BadRequestException", 400),
                );
            };
            let schema = match schema_from_create_request(&body) {
                Ok(s) => s,
                Err(e) => return ("400 Bad Request", err_body(&e, "BadRequestException", 400)),
            };
            let outcome = lake.create_table(store, ns, &table, schema, owner.as_deref());
            let (status, resp) = match outcome {
                Ok(v) => ("200 OK", v.to_string()),
                Err(CreateTableError::AlreadyExists) => (
                    "409 Conflict",
                    err_body(
                        &format!("table {ns}.{table} already exists"),
                        "AlreadyExistsException",
                        409,
                    ),
                ),
                Err(CreateTableError::Other(e)) => {
                    ("400 Bad Request", err_body(&e, "BadRequestException", 400))
                }
            };
            lake.record_audit(json!({
                "ts_ms": crate::server::lake::lineage::now_ms(),
                "op": "CreateTable",
                "namespace": ns,
                "table": table,
                "owner": owner.clone().unwrap_or_else(|| "system".to_string()),
                "outcome": if status == "200 OK" { "allow" } else { "deny" },
                "status": status,
            }));
            (status, resp)
        }
        ("GET", ["v1", "namespaces", ns, "tables", table]) => {
            match lake.load_table_visible(ns, table, &visibility) {
                Some(v) => ("200 OK", v.to_string()),
                None => ("404 Not Found", not_found("table")),
            }
        }
        ("HEAD", ["v1", "namespaces", ns, "tables", table]) => {
            if lake.load_table_visible(ns, table, &visibility).is_some() {
                ("200 OK", String::new())
            } else {
                ("404 Not Found", String::new())
            }
        }
        ("POST", ["v1", "namespaces", ns, "tables", table]) => {
            if lake.load_table_visible(ns, table, &visibility).is_none() {
                return ("404 Not Found", not_found("table"));
            }
            let (status, resp) = match lake.commit_table(store, ns, table) {
                Ok(v) => ("200 OK", v.to_string()),
                Err(e) => (
                    "400 Bad Request",
                    err_body(&e, "CommitFailedException", 400),
                ),
            };
            lake.record_audit(json!({
                "ts_ms": crate::server::lake::lineage::now_ms(),
                "op": "CommitTable",
                "namespace": ns,
                "table": table,
                "owner": owner.clone().unwrap_or_else(|| "system".to_string()),
                "outcome": if status == "200 OK" { "allow" } else { "deny" },
                "status": status,
            }));
            (status, resp)
        }
        ("DELETE", ["v1", "namespaces", ns, "tables", table]) => {
            let dropped = lake.drop_table(ns, table, &visibility);
            let (status, resp) = if dropped {
                ("204 No Content", String::new())
            } else {
                ("404 Not Found", not_found("table"))
            };
            lake.record_audit(json!({
                "ts_ms": crate::server::lake::lineage::now_ms(),
                "op": "DropTable",
                "namespace": ns,
                "table": table,
                "owner": owner.clone().unwrap_or_else(|| "system".to_string()),
                "outcome": if dropped { "allow" } else { "deny" },
                "status": status,
            }));
            (status, resp)
        }
        ("POST", ["v1", "tables", "rename"]) => {
            let body: Value = match serde_json::from_str(&req.body) {
                Ok(v) => v,
                Err(_) => {
                    return (
                        "400 Bad Request",
                        err_body("malformed RenameTableRequest body", "BadRequestException", 400),
                    )
                }
            };
            let source = body.get("source").cloned().unwrap_or(Value::Null);
            let destination = body.get("destination").cloned().unwrap_or(Value::Null);
            let (Some(src_ns), Some(src_name)) = (
                table_ident_namespace(&source),
                source.get("name").and_then(Value::as_str),
            ) else {
                return (
                    "400 Bad Request",
                    err_body("source identifier is required", "BadRequestException", 400),
                );
            };
            let (Some(dst_ns), Some(dst_name)) = (
                table_ident_namespace(&destination),
                destination.get("name").and_then(Value::as_str),
            ) else {
                return (
                    "400 Bad Request",
                    err_body("destination identifier is required", "BadRequestException", 400),
                );
            };
            let outcome = lake.rename_table(&src_ns, src_name, &dst_ns, dst_name, &visibility);
            let (status, resp) = match outcome {
                Ok(()) => ("204 No Content", String::new()),
                Err(RenameTableError::SourceNotFound) => ("404 Not Found", not_found("table")),
                Err(RenameTableError::DestinationExists) => (
                    "409 Conflict",
                    err_body(
                        &format!("table {dst_ns}.{dst_name} already exists"),
                        "AlreadyExistsException",
                        409,
                    ),
                ),
            };
            lake.record_audit(json!({
                "ts_ms": crate::server::lake::lineage::now_ms(),
                "op": "RenameTable",
                "source": format!("{src_ns}.{src_name}"),
                "destination": format!("{dst_ns}.{dst_name}"),
                "owner": owner.clone().unwrap_or_else(|| "system".to_string()),
                "outcome": if status == "204 No Content" { "allow" } else { "deny" },
                "status": status,
            }));
            (status, resp)
        }
        _ => ("404 Not Found", not_found("route")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server::blob::store::RedbChunkStore;
    use eg_tsdb::point::Point;
    use eg_tsdb::store::SeriesStore;

    fn seed() -> (LakeManager, RedbChunkStore) {
        let store = RedbChunkStore::open_temp().unwrap();
        let tsdb =
            SeriesStore::open_in_dir(&crate::server::unique_temp_dir("eg-lake-rest-test")).unwrap();
        tsdb.append_batch(
            "rest.series1",
            1,
            3_600_000_000_000,
            &["v".to_string()],
            &[Point::single(0, 1.0), Point::single(1, 2.0)],
        )
        .unwrap();
        let mgr = LakeManager::new();
        mgr.drain_series(&store, &tsdb, "rest.series1").unwrap();
        (mgr, store)
    }

    fn req(method: &str, target: &str) -> HttpRequest {
        HttpRequest {
            method: method.to_string(),
            target: target.to_string(),
            origin: String::new(),
            headers: HashMap::new(),
            body: String::new(),
        }
    }

    fn req_body(method: &str, target: &str, body: &str) -> HttpRequest {
        HttpRequest {
            method: method.to_string(),
            target: target.to_string(),
            origin: String::new(),
            headers: HashMap::new(),
            body: body.to_string(),
        }
    }

    /// Every existing (pre-W03) unit test call site of `handle()` runs with no
    /// live carrier — the exact behavior `serve()` (non-security) had before this
    /// lane; kept as a helper so those tests read unchanged.
    fn handle_open(
        lake: &LakeManager,
        store: &dyn ChunkStore,
        req: &HttpRequest,
    ) -> (&'static str, String) {
        handle(lake, store, req, None)
    }

    #[test]
    fn config_and_namespace_listing_shapes() {
        let (mgr, store) = seed();
        let (status, body) = handle_open(&mgr, &store, &req("GET", "/v1/config"));
        assert_eq!(status, "200 OK");
        let v: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert!(v["defaults"].is_object());

        let (status, body) = handle_open(&mgr, &store, &req("GET", "/v1/namespaces"));
        assert_eq!(status, "200 OK");
        let v: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(v["namespaces"][0][0], "engine");

        let (status, _) = handle_open(&mgr, &store, &req("GET", "/v1/namespaces/engine"));
        assert_eq!(status, "200 OK");
        let (status, _) = handle_open(&mgr, &store, &req("GET", "/v1/namespaces/nope"));
        assert_eq!(status, "404 Not Found");
    }

    #[test]
    fn list_and_load_table_shapes_match_iceberg_rest_spec() {
        let (mgr, store) = seed();
        let (status, body) =
            handle_open(&mgr, &store, &req("GET", "/v1/namespaces/engine/tables"));
        assert_eq!(status, "200 OK");
        let v: serde_json::Value = serde_json::from_str(&body).unwrap();
        let ids = v["identifiers"].as_array().unwrap();
        assert_eq!(ids.len(), 1);
        assert_eq!(ids[0]["name"], "rest_series1");
        assert_eq!(ids[0]["namespace"][0], "engine");

        let (status, body) = handle_open(
            &mgr,
            &store,
            &req("GET", "/v1/namespaces/engine/tables/rest_series1"),
        );
        assert_eq!(status, "200 OK");
        let v: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert!(v["metadata-location"]
            .as_str()
            .unwrap()
            .ends_with(".metadata.json"));
        // Inline metadata: a client needs NO second fetch to open the table.
        assert_eq!(v["metadata"]["format-version"], 2);
        assert_eq!(
            v["metadata"]["schemas"][0]["fields"]
                .as_array()
                .unwrap()
                .len(),
            2
        );
        assert!(v["config"].is_object());

        let (status, _) = handle_open(
            &mgr,
            &store,
            &req("GET", "/v1/namespaces/engine/tables/nope"),
        );
        assert_eq!(status, "404 Not Found");

        let (status, body_head) = handle_open(
            &mgr,
            &store,
            &req("HEAD", "/v1/namespaces/engine/tables/rest_series1"),
        );
        assert_eq!(status, "200 OK");
        assert!(body_head.is_empty());
    }

    #[test]
    fn commit_table_post_triggers_compaction_and_returns_load_table_shape() {
        let (mgr, store) = seed();
        let (status, body) = handle_open(
            &mgr,
            &store,
            &HttpRequest {
                method: "POST".to_string(),
                target: "/v1/namespaces/engine/tables/rest_series1".to_string(),
                origin: String::new(),
                headers: HashMap::new(),
                body: json!({ "requirements": [], "updates": [] }).to_string(),
            },
        );
        assert_eq!(status, "200 OK");
        let v: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(
            v["metadata"]["snapshots"][0]["summary"]["total-data-files"],
            "1"
        );

        let (status, _) = handle_open(
            &mgr,
            &store,
            &HttpRequest {
                method: "POST".to_string(),
                target: "/v1/namespaces/engine/tables/nope".to_string(),
                origin: String::new(),
                headers: HashMap::new(),
                body: "{}".to_string(),
            },
        );
        assert_eq!(status, "404 Not Found");
    }

    // ── W03: CreateTable / DropTable / RenameTable / pagination / multi-level ──

    fn create_table_body(name: &str) -> String {
        json!({
            "name": name,
            "schema": {
                "type": "struct",
                "schema-id": 0,
                "fields": [
                    {"id": 1, "name": "ts", "required": true, "type": "timestamp"},
                    {"id": 2, "name": "value", "required": false, "type": "double"},
                ],
            },
        })
        .to_string()
    }

    #[test]
    fn create_table_then_load_round_trips_and_rejects_duplicate() {
        let (mgr, store) = seed();
        let (status, body) = handle_open(
            &mgr,
            &store,
            &req_body(
                "POST",
                "/v1/namespaces/engine/tables",
                &create_table_body("brand_new"),
            ),
        );
        assert_eq!(status, "200 OK", "got: {body}");
        let v: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(v["metadata"]["schemas"][0]["fields"].as_array().unwrap().len(), 2);

        let (status, _) = handle_open(
            &mgr,
            &store,
            &req("GET", "/v1/namespaces/engine/tables/brand_new"),
        );
        assert_eq!(status, "200 OK", "a freshly created table must be immediately loadable");

        // known-bad: creating the SAME table again is a typed 409, not a silent
        // overwrite.
        let (status, body) = handle_open(
            &mgr,
            &store,
            &req_body(
                "POST",
                "/v1/namespaces/engine/tables",
                &create_table_body("brand_new"),
            ),
        );
        assert_eq!(status, "409 Conflict", "got: {body}");
        let v: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(v["error"]["type"], "AlreadyExistsException");
    }

    #[test]
    fn create_table_rejects_unsupported_type_and_missing_name() {
        let (mgr, store) = seed();
        let bad_type = json!({
            "name": "t",
            "schema": {"fields": [{"name": "c", "type": "decimal(10,2)"}]},
        })
        .to_string();
        let (status, body) = handle_open(
            &mgr,
            &store,
            &req_body("POST", "/v1/namespaces/engine/tables", &bad_type),
        );
        assert_eq!(status, "400 Bad Request", "got: {body}");

        let no_name = json!({"schema": {"fields": [{"name": "c", "type": "long"}]}}).to_string();
        let (status, _) = handle_open(
            &mgr,
            &store,
            &req_body("POST", "/v1/namespaces/engine/tables", &no_name),
        );
        assert_eq!(status, "400 Bad Request");
    }

    #[test]
    fn drop_table_removes_it_and_is_idempotent_404_on_repeat() {
        let (mgr, store) = seed();
        let (status, _) = handle_open(
            &mgr,
            &store,
            &req("DELETE", "/v1/namespaces/engine/tables/rest_series1"),
        );
        assert_eq!(status, "204 No Content");

        let (status, _) = handle_open(
            &mgr,
            &store,
            &req("GET", "/v1/namespaces/engine/tables/rest_series1"),
        );
        assert_eq!(status, "404 Not Found", "a dropped table must no longer load");

        // known-bad: dropping an already-dropped (or never-existed) table is a
        // typed 404, not a 200/500.
        let (status, _) = handle_open(
            &mgr,
            &store,
            &req("DELETE", "/v1/namespaces/engine/tables/rest_series1"),
        );
        assert_eq!(status, "404 Not Found");
    }

    #[test]
    fn rename_table_moves_identity_and_rejects_existing_destination() {
        let (mgr, store) = seed();
        let body =
            json!({"source": {"namespace": ["engine"], "name": "rest_series1"},
                    "destination": {"namespace": ["engine"], "name": "renamed"}})
            .to_string();
        let (status, _) = handle_open(&mgr, &store, &req_body("POST", "/v1/tables/rename", &body));
        assert_eq!(status, "204 No Content");

        let (status, _) = handle_open(
            &mgr,
            &store,
            &req("GET", "/v1/namespaces/engine/tables/rest_series1"),
        );
        assert_eq!(status, "404 Not Found", "the OLD identifier must no longer resolve");
        let (status, _) = handle_open(
            &mgr,
            &store,
            &req("GET", "/v1/namespaces/engine/tables/renamed"),
        );
        assert_eq!(status, "200 OK", "the NEW identifier must resolve");

        // known-bad: renaming a nonexistent source is a typed 404.
        let bad_src = json!({"source": {"namespace": ["engine"], "name": "nope"},
                              "destination": {"namespace": ["engine"], "name": "x"}})
            .to_string();
        let (status, _) =
            handle_open(&mgr, &store, &req_body("POST", "/v1/tables/rename", &bad_src));
        assert_eq!(status, "404 Not Found");

        // known-bad: renaming onto an EXISTING destination is a typed 409, never a
        // silent overwrite.
        let (mgr2, store2) = seed();
        handle_open(
            &mgr2,
            &store2,
            &req_body("POST", "/v1/namespaces/engine/tables", &create_table_body("dest")),
        );
        let collide = json!({"source": {"namespace": ["engine"], "name": "rest_series1"},
                              "destination": {"namespace": ["engine"], "name": "dest"}})
            .to_string();
        let (status, _) =
            handle_open(&mgr2, &store2, &req_body("POST", "/v1/tables/rename", &collide));
        assert_eq!(status, "409 Conflict");
    }

    #[test]
    fn multi_level_namespace_round_trips_through_unit_separator() {
        let (mgr, store) = seed();
        // `accounting\x1ftax` percent-encoded as ONE path segment.
        let (status, body) = handle_open(
            &mgr,
            &store,
            &req_body(
                "POST",
                "/v1/namespaces/accounting%1Ftax/tables",
                &create_table_body("ledger"),
            ),
        );
        assert_eq!(status, "200 OK", "got: {body}");

        let (status, body) =
            handle_open(&mgr, &store, &req("GET", "/v1/namespaces/accounting%1Ftax"));
        assert_eq!(status, "200 OK");
        let v: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(v["namespace"], json!(["accounting", "tax"]));

        let (status, body) = handle_open(
            &mgr,
            &store,
            &req("GET", "/v1/namespaces/accounting%1Ftax/tables"),
        );
        assert_eq!(status, "200 OK");
        let v: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(v["identifiers"][0]["namespace"], json!(["accounting", "tax"]));
        assert_eq!(v["identifiers"][0]["name"], "ledger");
    }

    #[test]
    fn list_tables_pagination_pages_deterministically_and_terminates() {
        let (mgr, store) = seed();
        for i in 0..5 {
            let (status, _) = handle_open(
                &mgr,
                &store,
                &req_body(
                    "POST",
                    "/v1/namespaces/engine/tables",
                    &create_table_body(&format!("paged_{i}")),
                ),
            );
            assert_eq!(status, "200 OK");
        }
        // 6 total (5 new + the seeded rest_series1).
        let mut seen = std::collections::HashSet::new();
        let mut page_token: Option<String> = None;
        let mut pages = 0;
        loop {
            let target = match &page_token {
                Some(t) => format!("/v1/namespaces/engine/tables?pageSize=2&pageToken={t}"),
                None => "/v1/namespaces/engine/tables?pageSize=2".to_string(),
            };
            let (status, body) = handle_open(&mgr, &store, &req("GET", &target));
            assert_eq!(status, "200 OK");
            let v: serde_json::Value = serde_json::from_str(&body).unwrap();
            let ids = v["identifiers"].as_array().unwrap();
            assert!(ids.len() <= 2, "page-size=2 must never return more than 2");
            for id in ids {
                seen.insert(id["name"].as_str().unwrap().to_string());
            }
            pages += 1;
            assert!(pages <= 10, "pagination did not terminate");
            match v.get("next-page-token").and_then(Value::as_str) {
                Some(t) => page_token = Some(t.to_string()),
                None => break,
            }
        }
        assert_eq!(seen.len(), 6, "every table must be seen exactly once across pages");
    }

    // ── W04: tenant/RLS projection — the disjoint-catalog proof ─────────────────

    /// A carrier with BOTH `kg:read` and `kg:write` (NE-048 finding: before this
    /// lane's scope gate existed, EVERY caller of this helper implicitly
    /// exercised a read-only carrier — `verified_for_test_in_tenant`'s fixed
    /// `kg:read`-only default — because the surface under test never looked at
    /// scope at all, so it didn't matter. That was itself an instance of the bug
    /// this lane fixes: it happened to still pass CreateTable/DropTable/etc.
    /// only because those operations weren't scope-gated yet. These call sites
    /// test tenant/ownership isolation and audit behavior, not scope
    /// enforcement, so they now request a full grant explicitly rather than
    /// relying on that gap. Dedicated scope-enforcement coverage lives in the
    /// "NE-048: per-operation scope enforcement" section below, using
    /// [`carrier_for_scopes`].
    fn carrier_for(agent: &str, tenant: &str) -> CarrierAuthority {
        carrier_for_scopes(agent, tenant, &["kg:read", "kg:write"])
    }

    fn carrier_for_scopes(agent: &str, tenant: &str, scopes: &[&str]) -> CarrierAuthority {
        CarrierAuthority::from_verified(
            &crate::server::auth::VerifiedRequestContext::verified_for_test_with_scopes(
                agent, tenant, scopes,
            ),
        )
        .unwrap()
    }

    /// GOC-75-W04's own acceptance bar: two callers see DISJOINT catalogs from the
    /// SAME endpoint. Proven at owner-scope granularity (see [`super::super::
    /// LakeVisibility`]'s docs for why: `EPISTEMIC_GRAPH_TENANT` is single-valued
    /// per deployment and already enforced at carrier-minting time by W01/W02, so
    /// within one deployment "two tenants" is necessarily "two distinct verified
    /// principals" — this test uses two agents in the SAME `EPISTEMIC_GRAPH_TENANT`
    /// to isolate exactly the NEW behavior this lane adds).
    #[test]
    fn two_owners_see_disjoint_namespace_and_table_listings() {
        let (mgr, store) = seed();
        let alice = carrier_for("alice", "tenant-shared");
        let bob = carrier_for("bob", "tenant-shared");

        // Alice creates a table in a namespace only she should ever see.
        let (status, _) = handle(
            &mgr,
            &store,
            &req_body(
                "POST",
                "/v1/namespaces/alice_ns/tables",
                &create_table_body("alice_secret"),
            ),
            Some(&alice),
        );
        assert_eq!(status, "200 OK");
        // Bob creates his own, disjoint namespace/table.
        let (status, _) = handle(
            &mgr,
            &store,
            &req_body(
                "POST",
                "/v1/namespaces/bob_ns/tables",
                &create_table_body("bob_secret"),
            ),
            Some(&bob),
        );
        assert_eq!(status, "200 OK");

        // 1) Namespace listing: Alice sees her own namespace, NEVER Bob's.
        let (_, body) = handle(&mgr, &store, &req("GET", "/v1/namespaces"), Some(&alice));
        let v: serde_json::Value = serde_json::from_str(&body).unwrap();
        let ns_names: Vec<String> = v["namespaces"]
            .as_array()
            .unwrap()
            .iter()
            .map(|n| n[0].as_str().unwrap().to_string())
            .collect();
        assert!(ns_names.contains(&"alice_ns".to_string()));
        assert!(
            !ns_names.contains(&"bob_ns".to_string()),
            "cross-tenant leak: alice's namespace listing contains bob's namespace: {ns_names:?}"
        );

        // 2) Table listing: Bob's table listing of HIS OWN namespace shows his
        // table; Alice's namespace is invisible to him as a namespace at all.
        let (status, _) = handle(
            &mgr,
            &store,
            &req("GET", "/v1/namespaces/alice_ns"),
            Some(&bob),
        );
        assert_eq!(
            status, "404 Not Found",
            "cross-tenant leak: bob can see alice's namespace exists (via a non-404)"
        );

        // 3) LoadTable: Bob cannot load Alice's table by name even if he somehow
        // knew it existed — no distinguishing 403, the SAME 404 a nonexistent
        // table gets (no existence oracle via status code).
        let (status_bob, body_bob) = handle(
            &mgr,
            &store,
            &req("GET", "/v1/namespaces/alice_ns/tables/alice_secret"),
            Some(&bob),
        );
        let (status_nonexistent, body_nonexistent) = handle(
            &mgr,
            &store,
            &req("GET", "/v1/namespaces/alice_ns/tables/truly_does_not_exist"),
            Some(&bob),
        );
        assert_eq!(status_bob, "404 Not Found");
        assert_eq!(
            status_bob, status_nonexistent,
            "cross-tenant leak: a real-but-invisible table must be indistinguishable \
             from a nonexistent one"
        );
        assert_eq!(
            body_bob, body_nonexistent,
            "cross-tenant leak: response body must not differ (no error-message side channel)"
        );

        // 4) Counts don't leak either: Bob's OWN namespace's table listing has
        // exactly his 1 table, not 2.
        let (_, body) = handle(
            &mgr,
            &store,
            &req("GET", "/v1/namespaces/bob_ns/tables"),
            Some(&bob),
        );
        let v: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(v["identifiers"].as_array().unwrap().len(), 1);

        // 5) Mutation isolation: Bob cannot DROP Alice's table.
        let (status, _) = handle(
            &mgr,
            &store,
            &req("DELETE", "/v1/namespaces/alice_ns/tables/alice_secret"),
            Some(&bob),
        );
        assert_eq!(status, "404 Not Found", "cross-tenant leak: bob dropped alice's table");
        // Proven still alive for Alice.
        let (status, _) = handle(
            &mgr,
            &store,
            &req("GET", "/v1/namespaces/alice_ns/tables/alice_secret"),
            Some(&alice),
        );
        assert_eq!(status, "200 OK", "alice's table must be unaffected by bob's denied drop");

        // 6) Positive control: Alice CAN see/load/drop her own table throughout.
        let (status, _) = handle(
            &mgr,
            &store,
            &req("GET", "/v1/namespaces/alice_ns/tables/alice_secret"),
            Some(&alice),
        );
        assert_eq!(status, "200 OK");
    }

    /// Engine-internal (untagged) tables stay visible to every authenticated
    /// caller (the "system table" carve-out) — proven against the seeded
    /// `engine.rest_series1` table, which was drained (not REST-created), so it
    /// carries no owner tag.
    #[test]
    fn engine_internal_tables_remain_visible_to_every_authenticated_owner() {
        let (mgr, store) = seed();
        let alice = carrier_for("alice", "tenant-shared");
        let (status, _) = handle(
            &mgr,
            &store,
            &req("GET", "/v1/namespaces/engine/tables/rest_series1"),
            Some(&alice),
        );
        assert_eq!(status, "200 OK");
    }

    // ── W05: audit trail + rate limiting ────────────────────────────────────────

    #[test]
    fn create_and_drop_table_appear_in_the_audit_trail() {
        let (mgr, store) = seed();
        handle_open(
            &mgr,
            &store,
            &req_body(
                "POST",
                "/v1/namespaces/engine/tables",
                &create_table_body("audited"),
            ),
        );
        handle_open(
            &mgr,
            &store,
            &req("DELETE", "/v1/namespaces/engine/tables/audited"),
        );
        let events = mgr.recent_audit(10);
        assert!(
            events
                .iter()
                .any(|e| e["op"] == "CreateTable" && e["table"] == "audited" && e["outcome"] == "allow"),
            "CreateTable must appear in the audit trail: {events:?}"
        );
        assert!(
            events
                .iter()
                .any(|e| e["op"] == "DropTable" && e["table"] == "audited" && e["outcome"] == "allow"),
            "DropTable must appear in the audit trail: {events:?}"
        );
    }

    #[test]
    fn a_denied_cross_owner_drop_is_audited_as_a_denial() {
        let (mgr, store) = seed();
        let alice = carrier_for("alice", "tenant-shared");
        let bob = carrier_for("bob", "tenant-shared");
        handle(
            &mgr,
            &store,
            &req_body(
                "POST",
                "/v1/namespaces/alice_ns/tables",
                &create_table_body("alice_secret"),
            ),
            Some(&alice),
        );
        handle(
            &mgr,
            &store,
            &req("DELETE", "/v1/namespaces/alice_ns/tables/alice_secret"),
            Some(&bob),
        );
        let events = mgr.recent_audit(10);
        assert!(
            events
                .iter()
                .any(|e| e["op"] == "DropTable" && e["outcome"] == "deny"),
            "a denied cross-owner drop must be audited as a denial: {events:?}"
        );
    }

    // ── NE-048: per-operation scope enforcement (P0 privilege-escalation fix) ──
    //
    // Before this lane, `handle()` never looked at scope at all — a verified,
    // correctly-tenanted carrier could perform every operation regardless of
    // what its own bearer was actually issued (compounded by
    // `authenticated_iceberg_bearer` also hardcoding both scopes for every
    // bearer, fixed alongside this in `server::auth`). These tests exercise the
    // gate `handle()` now runs directly, independent of the scope-derivation
    // tests in `server::auth`'s `iceberg_bearer_carrier` module.

    #[test]
    fn read_only_carrier_is_denied_every_write_operation() {
        let (mgr, store) = seed();
        let writer = carrier_for_scopes("writer", "tenant-shared", &["kg:read", "kg:write"]);
        let reader = carrier_for_scopes("writer", "tenant-shared", &["kg:read"]);

        // Seed a table with a write-capable carrier so CommitTable/DropTable/
        // RenameTable below have something real to target.
        let (status, _) = handle(
            &mgr,
            &store,
            &req_body(
                "POST",
                "/v1/namespaces/scoped_ns/tables",
                &create_table_body("scoped_table"),
            ),
            Some(&writer),
        );
        assert_eq!(status, "200 OK", "setup: write-capable carrier must be able to create");

        // CreateTable
        let (status, _) = handle(
            &mgr,
            &store,
            &req_body(
                "POST",
                "/v1/namespaces/scoped_ns/tables",
                &create_table_body("denied_create"),
            ),
            Some(&reader),
        );
        assert_eq!(status, "403 Forbidden", "kg:read-only must be denied CreateTable");

        // CommitTable
        let (status, _) = handle(
            &mgr,
            &store,
            &req_body("POST", "/v1/namespaces/scoped_ns/tables/scoped_table", "{}"),
            Some(&reader),
        );
        assert_eq!(status, "403 Forbidden", "kg:read-only must be denied CommitTable");

        // DropTable
        let (status, _) = handle(
            &mgr,
            &store,
            &req("DELETE", "/v1/namespaces/scoped_ns/tables/scoped_table"),
            Some(&reader),
        );
        assert_eq!(status, "403 Forbidden", "kg:read-only must be denied DropTable");

        // RenameTable
        let rename_body = json!({
            "source": {"namespace": ["scoped_ns"], "name": "scoped_table"},
            "destination": {"namespace": ["scoped_ns"], "name": "renamed_table"},
        })
        .to_string();
        let (status, _) = handle(
            &mgr,
            &store,
            &req_body("POST", "/v1/tables/rename", &rename_body),
            Some(&reader),
        );
        assert_eq!(status, "403 Forbidden", "kg:read-only must be denied RenameTable");

        // The table must be completely unaffected by all four denied attempts.
        let (status, _) = handle(
            &mgr,
            &store,
            &req("GET", "/v1/namespaces/scoped_ns/tables/scoped_table"),
            Some(&reader),
        );
        assert_eq!(status, "200 OK", "the table must survive every denied write attempt");
    }

    #[test]
    fn read_only_carrier_is_allowed_every_read_operation() {
        let (mgr, store) = seed();
        let writer = carrier_for_scopes("owner", "tenant-shared", &["kg:read", "kg:write"]);
        let reader = carrier_for_scopes("owner", "tenant-shared", &["kg:read"]);
        let (status, _) = handle(
            &mgr,
            &store,
            &req_body(
                "POST",
                "/v1/namespaces/scoped_ns/tables",
                &create_table_body("scoped_table"),
            ),
            Some(&writer),
        );
        assert_eq!(status, "200 OK");

        let (status, _) = handle(&mgr, &store, &req("GET", "/v1/config"), Some(&reader));
        assert_eq!(status, "200 OK");
        let (status, _) = handle(&mgr, &store, &req("GET", "/v1/namespaces"), Some(&reader));
        assert_eq!(status, "200 OK");
        let (status, _) = handle(
            &mgr,
            &store,
            &req("GET", "/v1/namespaces/scoped_ns"),
            Some(&reader),
        );
        assert_eq!(status, "200 OK");
        let (status, _) = handle(
            &mgr,
            &store,
            &req("GET", "/v1/namespaces/scoped_ns/tables"),
            Some(&reader),
        );
        assert_eq!(status, "200 OK");
        let (status, _) = handle(
            &mgr,
            &store,
            &req("GET", "/v1/namespaces/scoped_ns/tables/scoped_table"),
            Some(&reader),
        );
        assert_eq!(status, "200 OK");
        let (status, _) = handle(
            &mgr,
            &store,
            &req("HEAD", "/v1/namespaces/scoped_ns/tables/scoped_table"),
            Some(&reader),
        );
        assert_eq!(status, "200 OK");
    }

    #[test]
    fn write_only_carrier_is_denied_reads_least_privilege_is_symmetric() {
        // Completes the mapping proof: a kg:write-only carrier must not
        // silently also receive kg:read. Not the headline escalation this
        // lane fixes, but proof `operation_scope`/`scope_authorized` really
        // project per-operation minimum scope rather than just special-casing
        // "deny writes for read-only".
        let (mgr, store) = seed();
        let writer_only = carrier_for_scopes("writer-only", "tenant-shared", &["kg:write"]);
        let (status, _) = handle(
            &mgr,
            &store,
            &req("GET", "/v1/namespaces"),
            Some(&writer_only),
        );
        assert_eq!(status, "403 Forbidden", "kg:write-only must be denied a read operation");
    }

    #[test]
    fn both_scopes_carrier_retains_full_read_write_behavior() {
        let (mgr, store) = seed();
        let full = carrier_for_scopes("full", "tenant-shared", &["kg:read", "kg:write"]);
        let (status, _) = handle(
            &mgr,
            &store,
            &req_body(
                "POST",
                "/v1/namespaces/scoped_ns/tables",
                &create_table_body("scoped_table"),
            ),
            Some(&full),
        );
        assert_eq!(status, "200 OK");
        let (status, _) = handle(
            &mgr,
            &store,
            &req("GET", "/v1/namespaces/scoped_ns/tables/scoped_table"),
            Some(&full),
        );
        assert_eq!(status, "200 OK");
        let (status, _) = handle(
            &mgr,
            &store,
            &req("DELETE", "/v1/namespaces/scoped_ns/tables/scoped_table"),
            Some(&full),
        );
        assert_eq!(status, "204 No Content");
    }

    #[test]
    fn insufficient_scope_denial_is_audited() {
        let (mgr, store) = seed();
        let reader = carrier_for_scopes("auditee", "tenant-shared", &["kg:read"]);
        let (status, _) = handle(
            &mgr,
            &store,
            &req_body(
                "POST",
                "/v1/namespaces/scoped_ns/tables",
                &create_table_body("denied"),
            ),
            Some(&reader),
        );
        assert_eq!(status, "403 Forbidden");
        let events = mgr.recent_audit(10);
        assert!(
            events
                .iter()
                .any(|e| e["op"] == "InsufficientScope" && e["outcome"] == "deny"),
            "an insufficient-scope denial must be audited: {events:?}"
        );
    }

    #[test]
    fn rate_limiter_admits_up_to_capacity_then_sheds() {
        let limiter = RateLimiter::new(3.0);
        assert!(limiter.try_admit("1.2.3.4"));
        assert!(limiter.try_admit("1.2.3.4"));
        assert!(limiter.try_admit("1.2.3.4"));
        // known-bad: a 4th request within the same instant, with no refill time
        // elapsed, must be shed rather than silently admitted.
        assert!(
            !limiter.try_admit("1.2.3.4"),
            "a caller over its bucket capacity must be rate-limited"
        );
        // A DIFFERENT key has its own, untouched bucket.
        assert!(limiter.try_admit("5.6.7.8"));
    }

    /// End-to-end over the REAL listener + a raw `TcpStream` — literal Iceberg-REST
    /// request shapes (method/path/media type) a standard client (PyIceberg/Spark/
    /// Trino) issues, exercised without adding a client SDK dependency.
    #[tokio::test]
    async fn http_listener_serves_config_namespaces_and_load_table() {
        let (mgr, store) = seed();
        let lake = Arc::new(mgr);
        let store: Arc<dyn ChunkStore> = Arc::new(store);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(serve(listener, lake, store));

        async fn get(addr: std::net::SocketAddr, path: &str) -> (String, String) {
            let mut sock = tokio::net::TcpStream::connect(addr).await.unwrap();
            let req = format!("GET {path} HTTP/1.1\r\nHost: x\r\naccept: application/json\r\n\r\n");
            sock.write_all(req.as_bytes()).await.unwrap();
            let mut resp = Vec::new();
            sock.read_to_end(&mut resp).await.unwrap();
            let text = String::from_utf8_lossy(&resp).to_string();
            let status = text.lines().next().unwrap_or("").to_string();
            let body = text.split("\r\n\r\n").nth(1).unwrap_or("").to_string();
            (status, body)
        }

        let (status, body) = get(addr, "/v1/config").await;
        assert!(status.contains("200"), "got: {status}");
        assert!(body.contains("defaults"));

        let (status, body) = get(addr, "/v1/namespaces").await;
        assert!(status.contains("200"));
        assert!(body.contains("engine"));

        let (status, body) = get(addr, "/v1/namespaces/engine/tables/rest_series1").await;
        assert!(status.contains("200"), "got: {status}");
        let v: serde_json::Value =
            serde_json::from_str(&body).expect("valid JSON LoadTableResponse");
        assert!(v["metadata"]["current-snapshot-id"].is_number());
    }

    // ── OAuth2 bearer verification (BUG-222) ────────────────────────────
    //
    // `verify_bearer` is the pure (header-parsing + RSA/JWKS) half of the
    // carrier gate; the tenant-match decision it feeds into
    // (`crate::server::auth::mint_iceberg_carrier`) has its own dedicated
    // test coverage in `server::auth`'s `iceberg_bearer_carrier` module. The
    // full wire-level 200/403/403 triple (authenticated / unauthenticated /
    // cross-tenant) is proven end-to-end against the real compiled server
    // binary by `tests/test_lake_iceberg_delta_parity.py`.
    mod bearer_verification {
        use super::*;
        use jsonwebtoken::{encode, Algorithm as JwtAlgorithm, DecodingKey, EncodingKey, Header};

        // Fixture RSA-2048 keypair generated solely for this test suite
        // (`openssl genrsa`), reused nowhere else — not a production key.
        // Mirrors `crate::server::oidc`'s own test fixture exactly.
        const TEST_RSA_PRIVATE_KEY_PKCS1_DER_HEX: &str = "308204a30201000282010100be4725fd791744d873c4c82cc04ba74db85707a72581e4773e3f9041531b15ea57dcccda092adecbfa818521f10de4f849de2f6b359a20ad4eeec7da6aa550baf49a8f471089348b5c677a4c3d9b7f027395d3a08fa87345e4f842d3f5e6d9846f139883cb9ed94e1a868f85a741a5cb1262beaa4b395c6f9bc82fc46e65267cd50d7d752d2194b69a03ca41f3c135a9862f48d7697f74e8da8dca840cdf4f2cda9addc48ea6445574ffbc79f23144a520ba9aaa3ea8b549c25a89188a869a8ee7f05a096a66bfa4f49d4b5900f49579e88da8c25da9baea53f93cb69e744e5d80b55a41e0de41449bb437b53b57f6ef179eae0b3815a20b1df65fbdf28fc3b7020301000102820100019495093241f2381b5b62ba3f17f71a1b2785e5bfd700af1e323da027f0e2a6b6a21bdacd16b1110aa746becdc21573c67bf4f2dead700b60761fecd2d3f0040d820c7744f8e419d58e4fcd65a443fd7638f95aad0c1e20fcd23463e44d4d8ddf0a4fa0509c4f7bbeebfd31d95374981232b06e0e5539f7a75895fa50b1c061bcb1816d44e1c9155192cc37707747c6abf0af131a3b7d94a774fdc8a491d949ca0049b5845aca493b71352800d31d6f8d4e6beb352571f1586e9c9184a7a691cc556e53953ac5fc7995fed28d0fd92918b2dac30a4892595f70083f18d42a8768bb76077625bc917b347a8c3ec245db23f0eaaebeff571a7141891df5aa380102818100f6cae082d13337d73a723d4672f5a8b7113dfc820251e05380a672055c27dbab82c044f73fdb5d1a3fce5894fda55e57372fcf5f2704ee0ae927fd73c0e80eead6832d5a5938c3c63e69cab78d53e15b535d8a724e93eadf2d9ad45ce6bd2ae3653d087583fd0c7c8e9dac3c33c1f5bc651a2f69f898c379cc3722a85a163c0102818100c5607fbcc1a5a3ae9fa1a3c2469c17dd6d402515ecc724957d7fec575517254acf1dfc70c915390d8f489fae188c17372548603d442b06ad8195c74f8ee8bf51cfa22a2b4740d9e43e35d1942e4e4be545baf43127910c1c7e983f0f5ff5852f85311a56dc8d27fb1b5f669b0f7e83971f99ada964c1f4c6233299a84666dfb702818100d4186938a417d37eca4111be30e044fe07f870c13ec324fa3e8f4d60a3e1b15d46027d82cc4377512ed2e4b82f00e702277094549f51124f18300117710b3e7ebe9a7fe8acd3271581e02392fa07c39e5c1800fad9e32fb05c1e3b32182f2ce3bec6e4353298d0195febcbf0f53e553572e23d2b62b5cf1126db9f9275d1b40102818001a2a60c4b527303bc60db797d9a477c572e63e045a0f4c5a44f8e06bf36bce15ccbf3ce7f6c0497ff2aebdfc6664abef339214b00a8969a936b49467879a734275341a43027f26638b9bb6dcde06a32911c566f9dd34ed5619b23529e49eb7b944feed6ef66e000ed9e21bc81295c2fc15c459b14b1a2b48d901ac3d129830b0281807b5d9e95bf0e2892ff7ee7251fa14bec34d00c031d216c0f06dfa698407ec750e3d357e800907812a61d90281ce93320ad4a50d33364429710f249b87bc925ba89c5f675ed99229d09399943934811b25f4bac5a6cba9303dcd82ccbd31216092e1b9fe5ab1921188bd3e96256c692602be876e09c919c04735638b19646a658";
        const TEST_RSA_MODULUS_HEX: &str = "BE4725FD791744D873C4C82CC04BA74DB85707A72581E4773E3F9041531B15EA57DCCCDA092ADECBFA818521F10DE4F849DE2F6B359A20AD4EEEC7DA6AA550BAF49A8F471089348B5C677A4C3D9B7F027395D3A08FA87345E4F842D3F5E6D9846F139883CB9ED94E1A868F85A741A5CB1262BEAA4B395C6F9BC82FC46E65267CD50D7D752D2194B69A03CA41F3C135A9862F48D7697F74E8DA8DCA840CDF4F2CDA9ADDC48EA6445574FFBC79F23144A520BA9AAA3EA8B549C25A89188A869A8EE7F05A096A66BFA4F49D4B5900F49579E88DA8C25DA9BAEA53F93CB69E744E5D80B55A41E0DE41449BB437B53B57F6EF179EAE0B3815A20B1DF65FBDF28FC3B7";
        const TEST_RSA_EXPONENT_HEX: &str = "010001";
        const ISSUER: &str = "https://identity.example.test/realms/eg";
        const AUDIENCE: &str = "epistemic-graph";
        const KID: &str = "iceberg-rest-test-kid";

        fn now_secs() -> u64 {
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs()
        }

        fn credential() -> IcebergCredential {
            let mut keys = HashMap::new();
            let n = hex::decode(TEST_RSA_MODULUS_HEX).unwrap();
            let e = hex::decode(TEST_RSA_EXPONENT_HEX).unwrap();
            keys.insert(
                KID.to_string(),
                DecodingKey::from_rsa_raw_components(&n, &e),
            );
            IcebergCredential {
                validator: JwtValidator::from_parts(ISSUER, AUDIENCE, keys),
            }
        }

        fn sign(sub: &str, tenant: &str, exp_offset_secs: i64) -> String {
            let mut header = Header::new(JwtAlgorithm::RS256);
            header.kid = Some(KID.to_string());
            let der = hex::decode(TEST_RSA_PRIVATE_KEY_PKCS1_DER_HEX).unwrap();
            let exp = (now_secs() as i64 + exp_offset_secs) as u64;
            let claims = serde_json::json!({
                "sub": sub,
                "iss": ISSUER,
                "aud": AUDIENCE,
                "exp": exp,
                "tenant_id": tenant,
            });
            encode(&header, &claims, &EncodingKey::from_rsa_der(&der)).unwrap()
        }

        fn headers_with_bearer(token: &str) -> HashMap<String, String> {
            let mut headers = HashMap::new();
            headers.insert("authorization".to_string(), format!("Bearer {token}"));
            headers
        }

        #[test]
        fn no_authorization_header_verifies_nothing() {
            let credential = credential();
            assert!(verify_bearer(Some(&credential), &HashMap::new()).is_none());
        }

        #[test]
        fn no_credential_configured_verifies_nothing_even_with_a_header() {
            let token = sign("agent:reader", "tenant-shared", 300);
            assert!(verify_bearer(None, &headers_with_bearer(&token)).is_none());
        }

        #[test]
        fn malformed_authorization_header_verifies_nothing() {
            let credential = credential();
            let mut headers = HashMap::new();
            headers.insert(
                "authorization".to_string(),
                "Basic dXNlcjpwYXNz".to_string(),
            );
            assert!(verify_bearer(Some(&credential), &headers).is_none());
        }

        #[test]
        fn valid_bearer_verifies_and_projects_subject_and_tenant() {
            let credential = credential();
            let token = sign("agent:reader", "tenant-shared", 300);
            let verified = verify_bearer(Some(&credential), &headers_with_bearer(&token))
                .expect("a validly-signed, unexpired, matching-issuer/audience bearer verifies");
            assert_eq!(verified.subject, "agent:reader");
            assert_eq!(verified.tenant.as_deref(), Some("tenant-shared"));
        }

        #[test]
        fn tampered_bearer_fails_verification() {
            let credential = credential();
            let mut token = sign("agent:reader", "tenant-shared", 300);
            token.push('x'); // corrupt the signature segment
            assert!(verify_bearer(Some(&credential), &headers_with_bearer(&token)).is_none());
        }

        #[test]
        fn expired_bearer_fails_verification() {
            let credential = credential();
            let token = sign("agent:reader", "tenant-shared", -3600);
            assert!(verify_bearer(Some(&credential), &headers_with_bearer(&token)).is_none());
        }

        /// End-to-end proof that a verified, correctly-scoped bearer mints an
        /// ALLOWED carrier while a verified bearer for a DIFFERENT tenant does
        /// not (BUG-222's own acceptance bar), chaining `verify_bearer`
        /// straight into `crate::server::auth::mint_iceberg_carrier` exactly
        /// as `resolve_carrier` does.
        #[test]
        fn verified_claims_feed_the_tenant_gate_correctly() {
            let credential = credential();
            let matching = sign("agent:reader", "tenant-shared", 300);
            let verified_matching =
                verify_bearer(Some(&credential), &headers_with_bearer(&matching)).unwrap();
            assert!(crate::server::auth::mint_iceberg_carrier(Some(&verified_matching)).is_some());

            let other_tenant = sign("agent:reader", "tenant-other", 300);
            let verified_other =
                verify_bearer(Some(&credential), &headers_with_bearer(&other_tenant)).unwrap();
            assert!(crate::server::auth::mint_iceberg_carrier(Some(&verified_other)).is_none());
        }

        /// W03: `/v1/oauth/tokens` gives an already-verifying bearer the SAME
        /// pass/fail outcome as presenting it directly — the lane's own design
        /// note ("identical authentication semantics").
        #[test]
        fn oauth_token_endpoint_echoes_a_verifying_bearer_and_rejects_a_bad_one() {
            let credential = credential();
            let token = sign("agent:reader", "tenant-shared", 300);
            let req = HttpRequest {
                method: "POST".to_string(),
                target: "/v1/oauth/tokens".to_string(),
                origin: String::new(),
                headers: HashMap::new(),
                body: format!(
                    "grant_type=client_credentials&client_id=x&client_secret={token}"
                ),
            };
            let (status, body) = handle_oauth_token(Some(&credential), &req);
            assert_eq!(status, "200 OK", "got: {body}");
            let v: serde_json::Value = serde_json::from_str(&body).unwrap();
            assert_eq!(v["access_token"], token);
            assert_eq!(v["token_type"], "bearer");

            // known-bad: a token for a mismatched tenant is denied, not echoed.
            let other_tenant = sign("agent:reader", "tenant-other", 300);
            let bad_req = HttpRequest {
                method: "POST".to_string(),
                target: "/v1/oauth/tokens".to_string(),
                origin: String::new(),
                headers: HashMap::new(),
                body: format!(
                    "grant_type=client_credentials&client_id=x&client_secret={other_tenant}"
                ),
            };
            let (status, body) = handle_oauth_token(Some(&credential), &bad_req);
            assert_eq!(status, "401 Unauthorized", "got: {body}");
            let v: serde_json::Value = serde_json::from_str(&body).unwrap();
            assert_eq!(v["error"], "invalid_client");

            // known-bad: no credential configured at all is fail-closed, never a
            // 200 with a fabricated token.
            let (status, _) = handle_oauth_token(None, &req);
            assert_eq!(status, "401 Unauthorized");

            // known-bad: missing grant_type is a typed 400.
            let no_grant = HttpRequest {
                method: "POST".to_string(),
                target: "/v1/oauth/tokens".to_string(),
                origin: String::new(),
                headers: HashMap::new(),
                body: format!("client_secret={token}"),
            };
            let (status, _) = handle_oauth_token(Some(&credential), &no_grant);
            assert_eq!(status, "400 Bad Request");
        }
    }
}
