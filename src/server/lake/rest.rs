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

use std::sync::Arc;

use serde_json::json;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::RwLock;

use crate::server::blob::store::ChunkStore;
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

/// Production Iceberg listener linked to live engine isolation. Catalog/table
/// handles have no verified tenant owner in this HTTP protocol, so secure/RLS
/// deployments fail the carrier closed.
pub async fn serve_with_security(
    listener: TcpListener,
    lake: Arc<LakeManager>,
    store: Arc<dyn ChunkStore>,
    state: Arc<RwLock<ServerState>>,
) {
    serve_inner(listener, lake, store, Some(state)).await;
}

async fn carrier_denied(state: Option<&Arc<RwLock<ServerState>>>) -> bool {
    if state.is_none() {
        return false;
    }
    // A18: the Iceberg-REST catalog protocol carries no credential this surface
    // can verify yet (no `eg2.` envelope, bearer/OAuth2 token, or other proof —
    // see reports/issue-register.md, A18), so no `CarrierAuthority` can ever be
    // minted here today; this always denies under `serve_with_security`,
    // honestly (via the real check) rather than via the old unconditional stub.
    crate::server::access::unauthenticated_carrier_denied(None)
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
    loop {
        let Ok((mut stream, _)) = listener.accept().await else {
            continue;
        };
        let lake = lake.clone();
        let store = store.clone();
        let security_state = security_state.clone();
        tokio::spawn(async move {
            let (status, body) =
                match tokio::time::timeout(HTTP_READ_TIMEOUT, read_request(&mut stream)).await {
                    Ok(Some(req)) => {
                        if carrier_denied(security_state.as_ref()).await {
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

/// A parsed HTTP/1.1 request: method, raw target (`/v1/…?…`), body.
struct HttpRequest {
    method: String,
    target: String,
    origin: String,
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
    for line in lines {
        let (k, v) = line.split_once(':')?;
        let key = k.trim().to_ascii_lowercase();
        if key.is_empty() {
            return None;
        }
        if key == "content-length" {
            if content_length.is_some() {
                return None;
            }
            content_length = Some(v.trim().parse().ok()?);
        } else if key == "transfer-encoding" {
            return None;
        } else if key == "origin" {
            if origin_seen {
                return None;
            }
            origin_seen = true;
            origin = v.trim().to_string();
        }
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
        let tsdb = SeriesStore::open_in_dir(
            &std::env::temp_dir().join(format!("eg-lake-rest-test-{}", std::process::id())),
        )
        .unwrap();
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
}
