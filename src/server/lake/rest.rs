//! Iceberg-REST catalog HTTP surface (INT-P2-3, `iceberg.apache.org/rest-catalog-spec`,
//! feature `lake-rest`).
//!
//! A hand-rolled, dependency-free HTTP/1.1 listener — the SAME idiom as the Prometheus
//! `--metrics-addr` exposition / `crate::server::sparql_http` / `crate::server::obs`
//! (NO axum/hyper/warp, so the Pi contract holds) — serving the handful of Iceberg-REST
//! endpoints a standard client (PyIceberg / Spark / Trino) issues to discover + open a
//! table:
//!
//!   * `GET  /v1/config`                              → `{"defaults":{},"overrides":{}}`
//!   * `GET  /v1/namespaces`                           → `ListNamespaces`
//!   * `GET  /v1/namespaces/{ns}`                      → `GetNamespace` (exists check)
//!   * `GET  /v1/namespaces/{ns}/tables`               → `ListTables`
//!   * `GET  /v1/namespaces/{ns}/tables/{table}`       → `LoadTable` (INLINE `metadata`,
//!     so a client needs no second fetch to open the table)
//!   * `HEAD /v1/namespaces/{ns}/tables/{table}`       → `TableExists`
//!   * `POST /v1/namespaces/{ns}/tables/{table}`       → `CommitTable` (see the honest
//!     scope note on [`super::LakeManager::commit_table`] — accepted per the spec's
//!     request/response envelope, but bridges to the engine's OWN compaction pass
//!     rather than ingesting an externally-authored manifest).
//!
//! Namespaces are single-level in this tier (eg-lake's own catalog models them that
//! way); the spec's multi-level `\x1f`-joined namespace identifiers are NOT decoded —
//! a documented scope note, not a silent gap.

use std::collections::HashMap;
use std::sync::Arc;

use serde_json::json;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::RwLock;

use crate::server::blob::store::ChunkStore;
use crate::server::oidc::{JwtValidator, VerifiedTokenClaims};
use crate::server::ServerState;

use super::LakeManager;

/// Env var carrying the Iceberg-REST listener bind address (`host:port`). Unset ⇒ no
/// listener (matches `--metrics-addr`/`--sparql-addr`/`--obs-addr`'s opt-in idiom).
pub const ICEBERG_ADDR_ENV: &str = "EPISTEMIC_GRAPH_ICEBERG_ADDR";
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

/// Verify the request's `Authorization: Bearer <token>` against `credential`
/// (RSA/JWKS signature + issuer + audience + expiry — `JwtValidator::
/// validate_claims`). `None` on a missing header, a malformed/non-Bearer
/// header, or a token that fails verification.
fn verify_bearer(
    credential: Option<&IcebergCredential>,
    headers: &HashMap<String, String>,
) -> Option<VerifiedTokenClaims> {
    let credential = credential?;
    let token = headers
        .get("authorization")
        .and_then(|header| header.strip_prefix("Bearer "))
        .map(str::trim)
        .filter(|token| !token.is_empty())?;
    credential.validator.validate_claims(token)
}

/// Mint (or fail to mint) this request's `CarrierAuthority` and report
/// whether it is denied (BUG-222). `state.is_none()` ⇒ the non-security
/// `serve()` path (used by this module's own `handle()`-level tests and any
/// caller with no live engine isolation to bind to) — gating is a no-op
/// there, mirroring the pre-existing behavior. Otherwise: no verified
/// bearer, OR a verified bearer whose tenant claim disagrees with this
/// deployment's own configured tenant, both deny through the SAME shared
/// `access::unauthenticated_carrier_denied` gate every other auxiliary
/// surface uses (`crate::server::auth::mint_iceberg_carrier` does the
/// tenant comparison).
fn carrier_denied(
    state: Option<&Arc<RwLock<ServerState>>>,
    credential: Option<&IcebergCredential>,
    headers: &HashMap<String, String>,
) -> bool {
    if state.is_none() {
        return false;
    }
    let verified = verify_bearer(credential, headers);
    let carrier = crate::server::auth::mint_iceberg_carrier(verified.as_ref());
    crate::server::access::unauthenticated_carrier_denied(carrier.as_ref())
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
    loop {
        let Ok((mut stream, _)) = listener.accept().await else {
            continue;
        };
        let lake = lake.clone();
        let store = store.clone();
        let security_state = security_state.clone();
        let credential = credential.clone();
        tokio::spawn(async move {
            let (status, body) =
                match tokio::time::timeout(HTTP_READ_TIMEOUT, read_request(&mut stream)).await {
                    Ok(Some(req)) => {
                        if carrier_denied(
                            security_state.as_ref(),
                            credential.as_deref(),
                            &req.headers,
                        ) {
                            (
                                "403 Forbidden",
                                err_body(
                                    "Iceberg carrier has no verified tenant/table ownership",
                                    "ForbiddenException",
                                    403,
                                ),
                            )
                        } else {
                            handle(&lake, store.as_ref(), &req)
                        }
                    }
                    _ => (
                        "400 Bad Request",
                        err_body("malformed HTTP request", "BadRequestException", 400),
                    ),
                };
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
        });
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

fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack.windows(needle.len()).position(|w| w == needle)
}

/// Route + execute one request → `(status, body)`. Pure (sync) so it is fully
/// unit-testable without a socket, mirroring `crate::server::s3::handle`'s precedent.
fn handle(lake: &LakeManager, store: &dyn ChunkStore, req: &HttpRequest) -> (&'static str, String) {
    if !req.origin.is_empty() {
        return (
            "403 Forbidden",
            err_body("browser origin denied", "ForbiddenException", 403),
        );
    }
    let path = req.target.split('?').next().unwrap_or(&req.target);
    let segs: Vec<&str> = path
        .trim_matches('/')
        .split('/')
        .filter(|s| !s.is_empty())
        .collect();

    match (req.method.as_str(), segs.as_slice()) {
        ("GET", ["v1", "config"]) => (
            "200 OK",
            json!({ "defaults": {}, "overrides": {} }).to_string(),
        ),
        ("GET", ["v1", "namespaces"]) => ("200 OK", lake.list_namespaces().to_string()),
        ("GET", ["v1", "namespaces", ns]) => {
            if lake.namespace_exists(ns) {
                (
                    "200 OK",
                    json!({ "namespace": [ns], "properties": {} }).to_string(),
                )
            } else {
                ("404 Not Found", not_found("namespace"))
            }
        }
        ("GET", ["v1", "namespaces", ns, "tables"]) => ("200 OK", lake.list_tables(ns).to_string()),
        ("GET", ["v1", "namespaces", ns, "tables", table]) => match lake.load_table(ns, table) {
            Some(v) => ("200 OK", v.to_string()),
            None => ("404 Not Found", not_found("table")),
        },
        ("HEAD", ["v1", "namespaces", ns, "tables", table]) => {
            if lake.load_table(ns, table).is_some() {
                ("200 OK", String::new())
            } else {
                ("404 Not Found", String::new())
            }
        }
        ("POST", ["v1", "namespaces", ns, "tables", table]) => {
            match lake.commit_table(store, ns, table) {
                Ok(v) => ("200 OK", v.to_string()),
                Err(e) => (
                    "400 Bad Request",
                    err_body(&e, "CommitFailedException", 400),
                ),
            }
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

    #[test]
    fn config_and_namespace_listing_shapes() {
        let (mgr, store) = seed();
        let (status, body) = handle(&mgr, &store, &req("GET", "/v1/config"));
        assert_eq!(status, "200 OK");
        let v: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert!(v["defaults"].is_object());

        let (status, body) = handle(&mgr, &store, &req("GET", "/v1/namespaces"));
        assert_eq!(status, "200 OK");
        let v: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(v["namespaces"][0][0], "engine");

        let (status, _) = handle(&mgr, &store, &req("GET", "/v1/namespaces/engine"));
        assert_eq!(status, "200 OK");
        let (status, _) = handle(&mgr, &store, &req("GET", "/v1/namespaces/nope"));
        assert_eq!(status, "404 Not Found");
    }

    #[test]
    fn list_and_load_table_shapes_match_iceberg_rest_spec() {
        let (mgr, store) = seed();
        let (status, body) = handle(&mgr, &store, &req("GET", "/v1/namespaces/engine/tables"));
        assert_eq!(status, "200 OK");
        let v: serde_json::Value = serde_json::from_str(&body).unwrap();
        let ids = v["identifiers"].as_array().unwrap();
        assert_eq!(ids.len(), 1);
        assert_eq!(ids[0]["name"], "rest_series1");
        assert_eq!(ids[0]["namespace"][0], "engine");

        let (status, body) = handle(
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

        let (status, _) = handle(
            &mgr,
            &store,
            &req("GET", "/v1/namespaces/engine/tables/nope"),
        );
        assert_eq!(status, "404 Not Found");

        let (status, body_head) = handle(
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
        let (status, body) = handle(
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

        let (status, _) = handle(
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
        assert_eq!(status, "400 Bad Request");
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
        /// as `carrier_denied` does.
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
    }
}
