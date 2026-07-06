//! Remote KV-cache HTTP surface (CONCEPT:EG-KG.backend.is-configured-so-co) — exposes the `eg-kvcache` shared,
//! content-addressed backend ([`eg_kvcache::SharedKvIndex`], CONCEPT:EG-KG.enrichment.content-address-separation) over
//! HTTP so parallel **vLLM / LMCache** instances SHARE KV-cache blocks by token-hash.
//!
//! ## What this is (and is NOT)
//!
//! A thin HTTP ADAPTER in front of the engine's in-process shared KV backend — NOT a
//! new store. Blocks land in a ref-counted, content-addressed
//! [`eg_kvcache::SharedKvIndex`]: two workers that PUT the same token-hash store the
//! bytes ONCE (the LMCache dedup / prefix-cache win) and a cold worker GETs a page a
//! warm worker already computed.
//!
//! It is a HAND-ROLLED HTTP/1.1 listener over `tokio::net` — the SAME dependency-free
//! idiom as [`crate::server::s3`] / [`crate::server::obs`] / [`crate::server::sparql_http`]
//! (NO axum/hyper/warp, so the Pi contract holds). Its parser keeps the body as raw
//! BYTES (KV pages are binary) and captures headers for the bearer-token guard.
//!
//! ## API (CONCEPT:EG-KG.backend.is-configured-so-co)
//!
//! * `GET  /kv/<hash>`          → the block bytes (`200`) or `404` if absent (or STALE,
//!   CONCEPT:EG-KG.storage.content-addressed-put — a stale entry reads as absent).
//! * `PUT  /kv/<hash>` (binary) → store the block under `<hash>` (`201 Created` on a
//!   new block, `200 OK` on a dedup hit against an existing block). An optional
//!   `X-EG-Data-Version: <n>` header (CONCEPT:EG-KG.storage.content-addressed-put) version-tags the entry so it goes
//!   stale on a later graph write; absent ⇒ a pure content-addressed page (never
//!   version-invalidated).
//! * `HEAD /kv/<hash>`          → `200` if present + fresh, `404` if absent/stale.
//! * `GET  /kv/<hash>/exists`   → `200` JSON `{"hash":…,"exists":bool}` (an
//!   HTTP-method-agnostic existence probe for clients that cannot issue `HEAD`).
//! * `GET  /kv/stats`           → `200` JSON occupancy + dedup + `stale_retired` stats.
//! * `GET  /kv/version`         → `200` JSON `{"tracking":bool,"version":n|null}` (the
//!   current data version, CONCEPT:EG-KG.storage.content-addressed-put).
//! * `PUT  /kv/version/<n>`     → advance the data version to `<n>`, retiring every
//!   now-stale versioned entry (the hook a graph write drives, CONCEPT:EG-KG.storage.content-addressed-put).
//!
//! ### Zero-copy snapshot-fork (CONCEPT:EG-KG.memory.zero-copy-snapshot-fork)
//!
//! The "LMCacheMPConnector snapshot → branch" primitive over HTTP — pin a candidate set of
//! pages, then fan out N branches that share ONE physical copy (zero-copy), copy-on-write
//! per branch:
//!
//! * `POST /kv/snapshot`               (JSON `{"keys":[…]}`) → `200 {"snapshot":id,"pages":n}`
//!   pins those pages into a snapshot.
//! * `POST /kv/snapshot/<id>/fork`     → `200 {"branch":id}` forks an O(1), zero-copy branch.
//! * `GET  /kv/branch/<bid>/<key>`     → the branch's view of `key` (overlay-then-shared),
//!   `404` if it resolves in neither.
//! * `PUT  /kv/branch/<bid>/<key>` (binary) → copy-on-write write into the branch overlay,
//!   isolated from siblings; `404` for an unknown branch.
//! * `GET  /kv/fork/stats`             → JSON occupancy proving `resident_fork_bytes` stays
//!   flat at the shared-page total regardless of branch count.
//!
//! ## Data-version invalidation (CONCEPT:EG-KG.storage.content-addressed-put)
//!
//! A cached *derived context* (an assembled prompt / retrieved subgraph a connector
//! stored under a token-hash) goes STALE when the underlying graph data changes. The
//! surface mirrors `eg-core`'s version-keyed result cache (KG-2.233): a PUT may carry the
//! `GraphCore::version()` it was derived at (`X-EG-Data-Version`), and a graph write
//! drives `PUT /kv/version/<n>` to advance the surface's data version — atomically
//! retiring every entry derived at an older version, so a stale entry is never served.
//! Pure KV pages (no version header) are `Agnostic` and untouched — the EG-186 dedup path
//! is unchanged.
//!
//! ## The address is the caller's token-hash (NOT a byte-hash)
//!
//! `<hash>` is the address the LMCache/vLLM connector computed over the *token ids* of
//! the block (a stable content key for that KV page), NOT a hash of the KV bytes. So
//! the server stores the body verbatim under `<hash>` via
//! [`eg_kvcache::SharedKvBackend::put_block`] and does NOT re-hash the body — dedup is
//! by the connector-supplied address, exactly as `SharedKvIndex` is designed for. See
//! the `docs/` note referenced in the crate report for the full connector contract.
//!
//! ## Auth
//!
//! A bearer-token guard mirroring the s3 SigV4-lite guard's posture: anonymous when
//! unconfigured; when `EPISTEMIC_GRAPH_KVCACHE_TOKEN` is set, every request must carry
//! `Authorization: Bearer <token>` or it is refused `401`.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

use eg_kvcache::{BranchId, DataVersion, SharedKvBackend, SharedKvIndex, SnapshotId};

/// Env var: when set (and built `--features kvcache-server`) the KV-cache HTTP listener
/// binds this address (documented loopback default `127.0.0.1:9130`). Unset ⇒ no
/// listener.
pub const KVCACHE_ADDR_ENV: &str = "EPISTEMIC_GRAPH_KVCACHE_ADDR";
/// Env var: the bearer token. When set, the guard is armed and anonymous access is
/// refused; every request must present `Authorization: Bearer <token>`.
pub const KVCACHE_TOKEN_ENV: &str = "EPISTEMIC_GRAPH_KVCACHE_TOKEN";
/// Request header (CONCEPT:EG-KG.storage.content-addressed-put): the `GraphCore::version()` (KG-2.180) the PUT body was
/// DERIVED at. When present, the stored entry is version-tagged and goes stale once the
/// surface's current data version advances past it (a graph write drives
/// `PUT /kv/version/<n>`). Absent ⇒ a pure content-addressed KV page
/// ([`DataVersion::Agnostic`], never version-invalidated) — so existing connectors are
/// unaffected.
pub const KVCACHE_DATA_VERSION_HEADER: &str = "x-eg-data-version";

/// The KV-cache backing store: the shared, content-addressed, ref-counted index behind
/// a `Mutex` (the index needs `&mut` to `put`/`release`; the guard is held only for the
/// duration of one op). A follow-up networked backend would swap this for an RPC /
/// object-store client implementing the SAME [`SharedKvBackend`] trait.
pub struct KvCacheStore {
    index: Mutex<SharedKvIndex>,
}

impl Default for KvCacheStore {
    fn default() -> Self {
        Self::new()
    }
}

impl KvCacheStore {
    /// A fresh, empty in-process shared KV-cache store.
    pub fn new() -> Self {
        Self {
            index: Mutex::new(SharedKvIndex::new()),
        }
    }

    fn get(&self, hash: &str) -> Option<Vec<u8>> {
        self.index.lock().unwrap().get_block(hash)
    }

    /// Store `block` under `hash`, stamped with the [`DataVersion`] it was `derived_at`
    /// (CONCEPT:EG-KG.storage.content-addressed-put). Returns `true` iff a NEW block was created (`false` on a dedup hit
    /// against an already-present block).
    fn put(&self, hash: &str, block: Vec<u8>, derived_at: DataVersion) -> bool {
        self.index
            .lock()
            .unwrap()
            .put_block(hash, block, derived_at)
    }

    fn contains(&self, hash: &str) -> bool {
        self.index.lock().unwrap().contains(hash)
    }

    /// Advance the surface's current data version (CONCEPT:EG-KG.storage.content-addressed-put) — driven by a graph
    /// write (`PUT /kv/version/<n>`). Retires every now-stale version-tagged entry so no
    /// stale agent/LLM context is served after the underlying data changed.
    fn set_data_version(&self, version: DataVersion) {
        self.index.lock().unwrap().set_data_version(version);
    }

    /// The surface's current data version as connector-friendly JSON (CONCEPT:EG-KG.storage.content-addressed-put).
    fn version_json(&self) -> String {
        let v = self.index.lock().unwrap().current_version();
        let (tracking, version) = match v {
            DataVersion::Agnostic => (false, serde_json::Value::Null),
            DataVersion::At(n) => (true, serde_json::json!(n)),
        };
        serde_json::json!({ "tracking": tracking, "version": version }).to_string()
    }

    // ── zero-copy snapshot-fork surface (CONCEPT:EG-KG.memory.zero-copy-snapshot-fork) ─────────────
    //
    // The "LMCacheMPConnector snapshot → branch" primitive over HTTP: a connector pins a
    // candidate set of pages into a SNAPSHOT, forks N BRANCHES that all read those SAME
    // physical pages (zero-copy), and writes copy-on-write per branch. This is the rung the
    // AU `crossmodal_fork` `max_concurrency>1` path targets to make fan-out O(1) in copies.

    /// Pin the current pages for `keys` into a snapshot; returns `(snapshot_id, page_count)`.
    fn snapshot(&self, keys: &[String]) -> (u64, usize) {
        let mut idx = self.index.lock().unwrap();
        let id = idx.snapshot(keys);
        let pages = idx.snapshot_page_count(id).unwrap_or(0);
        (id.0, pages)
    }

    /// Fork a branch off `snapshot`; `None` if the snapshot is unknown.
    fn fork(&self, snapshot: u64) -> Option<u64> {
        self.index
            .lock()
            .unwrap()
            .fork(SnapshotId(snapshot))
            .map(|b| b.0)
    }

    /// Zero-copy branch read (overlay-then-shared). The `Arc` is cloned to owned bytes only
    /// at the HTTP boundary (the wire needs an owned body); in-process readers stay zero-copy.
    fn branch_get(&self, branch: u64, key: &str) -> Option<Vec<u8>> {
        self.index
            .lock()
            .unwrap()
            .branch_get(BranchId(branch), key)
            .map(|a| (*a).clone())
    }

    /// Copy-on-write branch write; `false` if the branch is unknown.
    fn branch_put(&self, branch: u64, key: &str, val: Vec<u8>) -> bool {
        self.index
            .lock()
            .unwrap()
            .branch_put(BranchId(branch), key, val)
    }

    fn fork_stats_json(&self) -> String {
        let fs = self.index.lock().unwrap().fork_stats();
        serde_json::json!({
            "snapshots": fs.snapshots,
            "branches": fs.branches,
            "shared_pages": fs.shared_pages,
            "shared_bytes": fs.shared_bytes,
            "overlay_pages": fs.overlay_pages,
            "overlay_bytes": fs.overlay_bytes,
            "resident_fork_bytes": fs.resident_fork_bytes,
        })
        .to_string()
    }

    fn stats_json(&self) -> String {
        let s = self.index.lock().unwrap().stats();
        // Hand-built via serde_json (already workspace-wide) — flat, connector-friendly.
        serde_json::json!({
            "unique_blocks": s.unique_blocks,
            "total_refs": s.total_refs,
            "resident_bytes": s.resident_bytes,
            "logical_bytes": s.logical_bytes,
            "dedup_savings_bytes": s.dedup_savings(),
            "dedup_hits": s.dedup_hits,
            "get_hits": s.get_hits,
            "get_misses": s.get_misses,
            "stale_retired": s.stale_retired,
        })
        .to_string()
    }
}

// ── bearer-token auth guard (CONCEPT:EG-KG.backend.is-configured-so-co) ──────────────────────────────────────

mod jwt;

/// A configured auth mode. `None` ⇒ anonymous access. JWT is preferred:
/// [`KvAuth::Jwt`] validates a Keycloak client-credentials token (paired with the
/// platform's overall auth — the same tokens graph-os validates); [`KvAuth::Static`]
/// is a shared bearer secret (the documented OpenBao-sourced fallback).
#[derive(Clone, Debug)]
pub enum KvAuth {
    Static(String),
    Jwt(std::sync::Arc<jwt::JwtValidator>),
}

/// With auth configured, require a valid `Authorization: Bearer <token>` — a static
/// secret match, or a signature/issuer/audience/expiry-verified Keycloak JWT. Without
/// it, everything is anonymous-allowed (mirrors the s3 guard's posture).
fn authorized(auth: &Option<KvAuth>, headers: &HashMap<String, String>) -> bool {
    let cfg = match auth {
        None => return true,
        Some(c) => c,
    };
    let token = match headers
        .get("authorization")
        .and_then(|h| h.strip_prefix("Bearer "))
        .map(str::trim)
    {
        Some(t) => t,
        None => return false,
    };
    match cfg {
        KvAuth::Static(secret) => token == secret,
        KvAuth::Jwt(validator) => validator.validate(token),
    }
}

/// Parse the `X-EG-Data-Version` header (CONCEPT:EG-KG.storage.content-addressed-put) into a [`DataVersion`]. A valid
/// unsigned integer ⇒ [`DataVersion::At`]; absent or unparseable ⇒ [`DataVersion::Agnostic`]
/// (a pure content-addressed KV page — the backward-compatible default that keeps existing
/// connectors' entries out of version invalidation).
fn parse_data_version(headers: &HashMap<String, String>) -> DataVersion {
    headers
        .get(KVCACHE_DATA_VERSION_HEADER)
        .and_then(|v| v.trim().parse::<u64>().ok())
        .map(DataVersion::At)
        .unwrap_or(DataVersion::Agnostic)
}

// ── request / response + routing ──────────────────────────────────────────────────

/// A parsed request: method, decoded path, headers (lowercased keys), raw (binary) body.
struct KvRequest {
    method: String,
    path: String,
    headers: HashMap<String, String>,
    body: Vec<u8>,
}

/// A response ready to serialize: status line, content-type, body bytes, head-only flag.
struct KvResponse {
    status: &'static str,
    content_type: &'static str,
    body: Vec<u8>,
    head_only: bool,
}

impl KvResponse {
    fn bytes(status: &'static str, body: Vec<u8>) -> Self {
        KvResponse {
            status,
            content_type: "application/octet-stream",
            body,
            head_only: false,
        }
    }
    fn json(status: &'static str, body: String) -> Self {
        KvResponse {
            status,
            content_type: "application/json",
            body: body.into_bytes(),
            head_only: false,
        }
    }
    fn empty(status: &'static str) -> Self {
        KvResponse {
            status,
            content_type: "application/octet-stream",
            body: Vec::new(),
            head_only: false,
        }
    }
    fn error(status: &'static str, code: &str, message: &str) -> Self {
        let body = serde_json::json!({ "error": code, "message": message }).to_string();
        KvResponse::json(status, body)
    }
}

/// Route + execute one request → a [`KvResponse`]. Pure (sync) so it is fully
/// unit-testable without a socket (CONCEPT:EG-KG.backend.is-configured-so-co).
fn handle(store: &KvCacheStore, auth: &Option<KvAuth>, req: &KvRequest) -> KvResponse {
    if !authorized(auth, &req.headers) {
        return KvResponse::error(
            "401 Unauthorized",
            "Unauthorized",
            "missing or invalid bearer token",
        );
    }

    // Everything lives under `/kv/…`.
    let rest = match req.path.strip_prefix("/kv/") {
        Some(r) => r,
        None if req.path == "/kv" => "",
        None => return KvResponse::error("404 Not Found", "NotFound", "unknown path"),
    };

    // `GET /kv/stats` → JSON stats.
    if rest == "stats" {
        return match req.method.as_str() {
            "GET" | "HEAD" => KvResponse::json("200 OK", store.stats_json()),
            _ => KvResponse::error("405 Method Not Allowed", "MethodNotAllowed", "unsupported"),
        };
    }

    // `GET /kv/version` → the current data version; `PUT /kv/version/<n>` advances it
    // (CONCEPT:EG-KG.storage.content-addressed-put) — the hook a graph write drives to invalidate stale context.
    if rest == "version" {
        return match req.method.as_str() {
            "GET" | "HEAD" => KvResponse::json("200 OK", store.version_json()),
            _ => KvResponse::error("405 Method Not Allowed", "MethodNotAllowed", "unsupported"),
        };
    }
    if let Some(n) = rest.strip_prefix("version/") {
        return match req.method.as_str() {
            "PUT" | "POST" => match n.parse::<u64>() {
                Ok(v) => {
                    store.set_data_version(DataVersion::At(v));
                    KvResponse::json("200 OK", store.version_json())
                }
                Err(_) => {
                    KvResponse::error("400 Bad Request", "BadRequest", "version must be a u64")
                }
            },
            _ => KvResponse::error("405 Method Not Allowed", "MethodNotAllowed", "unsupported"),
        };
    }

    // Zero-copy snapshot-fork surface (CONCEPT:EG-KG.memory.zero-copy-snapshot-fork).
    // `POST /kv/snapshot` (JSON `{"keys":[...]}`) → pin those pages into a snapshot.
    if rest == "snapshot" {
        return match req.method.as_str() {
            "POST" | "PUT" => {
                let keys = serde_json::from_slice::<serde_json::Value>(&req.body)
                    .ok()
                    .and_then(|v| {
                        v.get("keys").and_then(|k| k.as_array()).map(|a| {
                            a.iter()
                                .filter_map(|x| x.as_str().map(String::from))
                                .collect::<Vec<_>>()
                        })
                    });
                match keys {
                    Some(keys) => {
                        let (id, pages) = store.snapshot(&keys);
                        KvResponse::json(
                            "200 OK",
                            serde_json::json!({ "snapshot": id, "pages": pages }).to_string(),
                        )
                    }
                    None => KvResponse::error(
                        "400 Bad Request",
                        "BadRequest",
                        "body must be JSON {\"keys\":[...]}",
                    ),
                }
            }
            _ => KvResponse::error("405 Method Not Allowed", "MethodNotAllowed", "unsupported"),
        };
    }
    // `POST /kv/snapshot/<id>/fork` → fork a branch off the snapshot (O(1), zero-copy).
    if let Some(sid) = rest
        .strip_prefix("snapshot/")
        .and_then(|r| r.strip_suffix("/fork"))
    {
        return match req.method.as_str() {
            "POST" | "PUT" => match sid.parse::<u64>() {
                Ok(s) => match store.fork(s) {
                    Some(b) => {
                        KvResponse::json("200 OK", serde_json::json!({ "branch": b }).to_string())
                    }
                    None => {
                        KvResponse::error("404 Not Found", "NoSuchSnapshot", "unknown snapshot")
                    }
                },
                Err(_) => {
                    KvResponse::error("400 Bad Request", "BadRequest", "snapshot id must be a u64")
                }
            },
            _ => KvResponse::error("405 Method Not Allowed", "MethodNotAllowed", "unsupported"),
        };
    }
    // `GET /kv/fork/stats` → the zero-copy occupancy proof (resident stays flat vs branch count).
    if rest == "fork/stats" {
        return match req.method.as_str() {
            "GET" | "HEAD" => KvResponse::json("200 OK", store.fork_stats_json()),
            _ => KvResponse::error("405 Method Not Allowed", "MethodNotAllowed", "unsupported"),
        };
    }
    // `GET /kv/branch/<bid>/<key>` (zero-copy read) + `PUT /kv/branch/<bid>/<key>` (CoW write).
    if let Some(r) = rest.strip_prefix("branch/") {
        let (bid, key) = match r.split_once('/') {
            Some((b, k)) if !k.is_empty() => (b, k),
            _ => {
                return KvResponse::error(
                    "400 Bad Request",
                    "BadRequest",
                    "expected branch/<id>/<key>",
                )
            }
        };
        let branch = match bid.parse::<u64>() {
            Ok(b) => b,
            Err(_) => {
                return KvResponse::error(
                    "400 Bad Request",
                    "BadRequest",
                    "branch id must be a u64",
                )
            }
        };
        return match req.method.as_str() {
            "GET" => match store.branch_get(branch, key) {
                Some(bytes) => KvResponse::bytes("200 OK", bytes),
                None => KvResponse::error(
                    "404 Not Found",
                    "NoSuchBranchKey",
                    "no page for that branch/key",
                ),
            },
            "PUT" | "POST" => {
                if store.branch_put(branch, key, req.body.clone()) {
                    KvResponse::empty("200 OK")
                } else {
                    KvResponse::error("404 Not Found", "NoSuchBranch", "unknown branch")
                }
            }
            _ => KvResponse::error("405 Method Not Allowed", "MethodNotAllowed", "unsupported"),
        };
    }

    // `GET /kv/<hash>/exists` → JSON existence probe.
    if let Some(hash) = rest.strip_suffix("/exists") {
        if hash.is_empty() {
            return KvResponse::error("400 Bad Request", "BadRequest", "empty hash");
        }
        let exists = store.contains(hash);
        return KvResponse::json(
            "200 OK",
            serde_json::json!({ "hash": hash, "exists": exists }).to_string(),
        );
    }

    // `/kv/<hash>` block ops.
    let hash = rest;
    if hash.is_empty() {
        return KvResponse::error("400 Bad Request", "BadRequest", "empty hash");
    }
    match req.method.as_str() {
        "GET" => match store.get(hash) {
            Some(bytes) => KvResponse::bytes("200 OK", bytes),
            None => KvResponse::error("404 Not Found", "NoSuchBlock", "no block under that hash"),
        },
        "HEAD" => {
            if store.contains(hash) {
                let mut r = KvResponse::empty("200 OK");
                r.head_only = true;
                r
            } else {
                let mut r =
                    KvResponse::error("404 Not Found", "NoSuchBlock", "no block under that hash");
                r.head_only = true;
                r
            }
        }
        "PUT" => {
            // Version-tag the entry with the `X-EG-Data-Version` header if the connector
            // supplied one (CONCEPT:EG-KG.storage.content-addressed-put); absent ⇒ a pure content-addressed page
            // (Agnostic, never version-invalidated).
            let derived_at = parse_data_version(&req.headers);
            let created = store.put(hash, req.body.clone(), derived_at);
            // 201 on a brand-new block; 200 on a dedup hit (the block was already present).
            KvResponse::empty(if created { "201 Created" } else { "200 OK" })
        }
        _ => KvResponse::error("405 Method Not Allowed", "MethodNotAllowed", "unsupported"),
    }
}

// ── the HTTP listener (hand-rolled, no axum/hyper — the Pi contract) ───────────────

fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack.windows(needle.len()).position(|w| w == needle)
}

/// Read one HTTP/1.1 request, keeping the body as RAW BYTES (KV pages are binary) and
/// capturing headers (lowercased keys) for the auth guard. Mirrors the s3 reader.
async fn read_request(stream: &mut tokio::net::TcpStream) -> Option<KvRequest> {
    let mut buf = Vec::new();
    let mut tmp = [0u8; 8192];
    let header_end = loop {
        if let Some(pos) = find_subslice(&buf, b"\r\n\r\n") {
            break pos;
        }
        let n = stream.read(&mut tmp).await.ok()?;
        if n == 0 {
            return None;
        }
        buf.extend_from_slice(&tmp[..n]);
        if buf.len() > 8 * 1024 * 1024 {
            return None; // header flood guard
        }
    };
    let head = String::from_utf8_lossy(&buf[..header_end]).to_string();
    let mut lines = head.split("\r\n");
    let request_line = lines.next()?;
    let mut parts = request_line.split_whitespace();
    let method = parts.next()?.to_string();
    let target = parts.next()?.to_string();
    // The address is an opaque hex/base token-hash — no query params are used here.
    let path = target.split('?').next().unwrap_or(&target).to_string();

    let mut headers = HashMap::new();
    let mut content_length = 0usize;
    for line in lines {
        if let Some((k, v)) = line.split_once(':') {
            let key = k.trim().to_ascii_lowercase();
            let val = v.trim().to_string();
            if key == "content-length" {
                content_length = val.parse().unwrap_or(0);
            }
            headers.insert(key, val);
        }
    }
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
    Some(KvRequest {
        method,
        path,
        headers,
        body,
    })
}

/// Serve the KV-cache HTTP surface on `addr` until the process exits. Spawned by
/// `main.rs` only when built `--features kvcache-server` AND
/// `EPISTEMIC_GRAPH_KVCACHE_ADDR` is set (CONCEPT:EG-KG.backend.is-configured-so-co). One task per connection,
/// one response per request, connection: close — the SAME idiom as the s3 listener.
pub async fn serve(addr: &str) -> std::io::Result<()> {
    let store = Arc::new(KvCacheStore::new());
    let auth = resolve_auth();
    serve_with_store(addr, store, auth).await
}

/// Resolve the bearer-token credential from the env (set ⇒ armed; else anonymous).
fn resolve_auth() -> Option<KvAuth> {
    // JWT FIRST — paired with the platform's Keycloak OIDC (same tokens graph-os uses).
    // Falls back to the static-token guard (the documented OpenBao-sourced option),
    // then anonymous. Backward-compatible: with no JWT/token env set, ⇒ None ⇒ anon.
    if let Some(v) = jwt::JwtValidator::from_env() {
        return Some(KvAuth::Jwt(std::sync::Arc::new(v)));
    }
    std::env::var(KVCACHE_TOKEN_ENV)
        .ok()
        .filter(|s| !s.is_empty())
        .map(KvAuth::Static)
}

/// `serve` with an EXPLICIT store + auth (CONCEPT:EG-KG.backend.is-configured-so-co) — tests bind an ephemeral
/// store on a random port without process env.
pub async fn serve_with_store(
    addr: &str,
    store: Arc<KvCacheStore>,
    auth: Option<KvAuth>,
) -> std::io::Result<()> {
    let listener = TcpListener::bind(addr).await?;
    tracing::info!(
        "kvcache-server: serving shared KV-cache HTTP surface on {} (auth={})",
        addr,
        auth.is_some()
    );
    loop {
        let (mut stream, _peer) = listener.accept().await?;
        let store = store.clone();
        let auth = auth.clone();
        tokio::spawn(async move {
            let resp = match read_request(&mut stream).await {
                Some(req) => handle(&store, &auth, &req),
                None => KvResponse::error("400 Bad Request", "InvalidRequest", "malformed"),
            };
            let head = format!(
                "HTTP/1.1 {}\r\ncontent-type: {}\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
                resp.status,
                resp.content_type,
                if resp.head_only { 0 } else { resp.body.len() },
            );
            let _ = stream.write_all(head.as_bytes()).await;
            if !resp.head_only {
                let _ = stream.write_all(&resp.body).await;
            }
            let _ = stream.shutdown().await;
        });
    }
}

#[cfg(test)]
mod tests {
    //! CONCEPT:EG-KG.backend.is-configured-so-co — PUT→GET round-trip of a binary block, HEAD/exists probes, 404
    //! on a missing block, stats JSON, dedup reflected in stats, the bearer-token
    //! guard accept/reject, and a live TCP listener round-trip.
    use super::*;

    fn store() -> KvCacheStore {
        KvCacheStore::new()
    }

    fn req(method: &str, path: &str, body: &[u8], headers: &[(&str, &str)]) -> KvRequest {
        KvRequest {
            method: method.to_string(),
            path: path.to_string(),
            headers: headers
                .iter()
                .map(|(k, v)| (k.to_ascii_lowercase(), v.to_string()))
                .collect(),
            body: body.to_vec(),
        }
    }

    #[test]
    fn eg187_put_then_get_round_trips_a_block() {
        let s = store();
        let none = None;
        let block = vec![7u8, 0, 255, 42, 13];
        // First PUT → 201 Created.
        let put = handle(&s, &none, &req("PUT", "/kv/abc123", &block, &[]));
        assert_eq!(put.status, "201 Created");
        // GET → the exact bytes back.
        let get = handle(&s, &none, &req("GET", "/kv/abc123", b"", &[]));
        assert_eq!(get.status, "200 OK");
        assert_eq!(get.body, block);
        assert_eq!(get.content_type, "application/octet-stream");
    }

    #[test]
    fn eg187_head_and_exists_probe() {
        let s = store();
        let none = None;
        handle(&s, &none, &req("PUT", "/kv/h1", b"x", &[]));
        // HEAD present → 200, no body.
        let head = handle(&s, &none, &req("HEAD", "/kv/h1", b"", &[]));
        assert_eq!(head.status, "200 OK");
        assert!(head.head_only);
        // HEAD absent → 404.
        assert_eq!(
            handle(&s, &none, &req("HEAD", "/kv/nope", b"", &[])).status,
            "404 Not Found"
        );
        // GET /kv/<hash>/exists → JSON true/false.
        let ex = handle(&s, &none, &req("GET", "/kv/h1/exists", b"", &[]));
        assert_eq!(ex.status, "200 OK");
        assert!(String::from_utf8_lossy(&ex.body).contains("\"exists\":true"));
        let no = handle(&s, &none, &req("GET", "/kv/ghost/exists", b"", &[]));
        assert!(String::from_utf8_lossy(&no.body).contains("\"exists\":false"));
    }

    #[test]
    fn eg187_get_missing_block_is_404() {
        let s = store();
        let none = None;
        assert_eq!(
            handle(&s, &none, &req("GET", "/kv/missing", b"", &[])).status,
            "404 Not Found"
        );
    }

    #[test]
    fn eg187_stats_json_reports_dedup() {
        let s = store();
        let none = None;
        // Two PUTs of the SAME hash ⇒ one resident block, a dedup hit on the second.
        assert_eq!(
            handle(&s, &none, &req("PUT", "/kv/dup", &[1u8; 100], &[])).status,
            "201 Created"
        );
        assert_eq!(
            handle(&s, &none, &req("PUT", "/kv/dup", &[1u8; 100], &[])).status,
            "200 OK"
        );
        let stats = handle(&s, &none, &req("GET", "/kv/stats", b"", &[]));
        assert_eq!(stats.status, "200 OK");
        assert_eq!(stats.content_type, "application/json");
        let v: serde_json::Value = serde_json::from_slice(&stats.body).unwrap();
        assert_eq!(v["unique_blocks"], 1, "deduped to one resident block");
        assert_eq!(v["total_refs"], 2, "both holders ref-counted");
        assert_eq!(v["resident_bytes"], 100);
        assert_eq!(v["dedup_hits"], 1);
        assert_eq!(
            v["dedup_savings_bytes"], 100,
            "one block's worth of bytes saved by dedup"
        );
    }

    #[test]
    fn eg187_bearer_token_guard_accept_reject() {
        let s = store();
        let auth = Some(KvAuth::Static("sekret".into()));
        // No Authorization → 401.
        assert_eq!(
            handle(&s, &auth, &req("GET", "/kv/stats", b"", &[])).status,
            "401 Unauthorized"
        );
        // Correct bearer → accepted.
        let ok = handle(
            &s,
            &auth,
            &req(
                "GET",
                "/kv/stats",
                b"",
                &[("authorization", "Bearer sekret")],
            ),
        );
        assert_eq!(ok.status, "200 OK");
        // Wrong token → 401.
        assert_eq!(
            handle(
                &s,
                &auth,
                &req("GET", "/kv/stats", b"", &[("authorization", "Bearer nope")])
            )
            .status,
            "401 Unauthorized"
        );
        // Unconfigured guard ⇒ anonymous allowed.
        assert!(authorized(&None, &HashMap::new()));
    }

    #[tokio::test]
    async fn eg187_tcp_listener_put_get_round_trip() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let store = Arc::new(KvCacheStore::new());
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        // Run one accept loop iteration per request in the background.
        let srv_store = store.clone();
        tokio::spawn(async move {
            loop {
                let (mut stream, _) = listener.accept().await.unwrap();
                let store = srv_store.clone();
                tokio::spawn(async move {
                    let resp = match read_request(&mut stream).await {
                        Some(r) => handle(&store, &None, &r),
                        None => KvResponse::error("400 Bad Request", "InvalidRequest", "malformed"),
                    };
                    let head = format!(
                        "HTTP/1.1 {}\r\ncontent-type: {}\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
                        resp.status,
                        resp.content_type,
                        if resp.head_only { 0 } else { resp.body.len() },
                    );
                    let _ = stream.write_all(head.as_bytes()).await;
                    if !resp.head_only {
                        let _ = stream.write_all(&resp.body).await;
                    }
                    let _ = stream.shutdown().await;
                });
            }
        });

        // PUT a block over a real socket.
        let block = b"\x00\x01\x02binary-kv-page\xff";
        let mut c = tokio::net::TcpStream::connect(addr).await.unwrap();
        let put = format!(
            "PUT /kv/tok987 HTTP/1.1\r\nhost: x\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
            block.len()
        );
        c.write_all(put.as_bytes()).await.unwrap();
        c.write_all(block).await.unwrap();
        let mut buf = Vec::new();
        c.read_to_end(&mut buf).await.unwrap();
        assert!(
            String::from_utf8_lossy(&buf).starts_with("HTTP/1.1 201 Created"),
            "{:?}",
            String::from_utf8_lossy(&buf)
        );

        // GET it back over a fresh socket; body bytes must match exactly.
        let mut c = tokio::net::TcpStream::connect(addr).await.unwrap();
        c.write_all(b"GET /kv/tok987 HTTP/1.1\r\nhost: x\r\nconnection: close\r\n\r\n")
            .await
            .unwrap();
        let mut buf = Vec::new();
        c.read_to_end(&mut buf).await.unwrap();
        let sep = find_subslice(&buf, b"\r\n\r\n").unwrap();
        assert!(String::from_utf8_lossy(&buf[..sep]).starts_with("HTTP/1.1 200 OK"));
        assert_eq!(
            &buf[sep + 4..],
            block,
            "binary block round-trips byte-for-byte over TCP"
        );
    }

    /// CONCEPT:EG-KG.storage.content-addressed-put — a version-tagged PUT is served while the data version holds, then
    /// a `PUT /kv/version/<n>` (a graph write) makes the stale entry a 404 MISS; a
    /// re-PUT at the new version serves fresh bytes. The stale context is never returned.
    #[test]
    fn eg359_version_bump_invalidates_derived_context_over_http() {
        let s = store();
        let none = None;
        // Advance to data version 5, then cache context derived at v5.
        assert_eq!(
            handle(&s, &none, &req("PUT", "/kv/version/5", b"", &[])).status,
            "200 OK"
        );
        let put = handle(
            &s,
            &none,
            &req("PUT", "/kv/ctx", b"ctx-v5", &[("x-eg-data-version", "5")]),
        );
        assert_eq!(put.status, "201 Created");
        let get = handle(&s, &none, &req("GET", "/kv/ctx", b"", &[]));
        assert_eq!(get.status, "200 OK");
        assert_eq!(get.body, b"ctx-v5");

        // A graph write bumps the surface's data version ⇒ v5 context is stale ⇒ 404.
        assert_eq!(
            handle(&s, &none, &req("PUT", "/kv/version/6", b"", &[])).status,
            "200 OK"
        );
        assert_eq!(
            handle(&s, &none, &req("GET", "/kv/ctx", b"", &[])).status,
            "404 Not Found",
            "stale context must not be served"
        );
        assert_eq!(
            handle(&s, &none, &req("HEAD", "/kv/ctx", b"", &[])).status,
            "404 Not Found"
        );
        let ex = handle(&s, &none, &req("GET", "/kv/ctx/exists", b"", &[]));
        assert!(String::from_utf8_lossy(&ex.body).contains("\"exists\":false"));

        // Re-cache the freshly-derived context at v6 ⇒ served again.
        let put = handle(
            &s,
            &none,
            &req("PUT", "/kv/ctx", b"ctx-v6", &[("x-eg-data-version", "6")]),
        );
        assert_eq!(put.status, "201 Created");
        let get = handle(&s, &none, &req("GET", "/kv/ctx", b"", &[]));
        assert_eq!(get.body, b"ctx-v6", "fresh context served");
    }

    /// CONCEPT:EG-KG.storage.content-addressed-put — a PUT WITHOUT the version header is a pure content-addressed page
    /// (Agnostic) that survives version bumps, so existing connectors are unaffected.
    #[test]
    fn eg359_unversioned_put_is_agnostic_and_survives_bumps() {
        let s = store();
        let none = None;
        assert_eq!(
            handle(&s, &none, &req("PUT", "/kv/page", b"kv-bytes", &[])).status,
            "201 Created"
        );
        handle(&s, &none, &req("PUT", "/kv/version/1", b"", &[]));
        handle(&s, &none, &req("PUT", "/kv/version/2", b"", &[]));
        let get = handle(&s, &none, &req("GET", "/kv/page", b"", &[]));
        assert_eq!(
            get.status, "200 OK",
            "agnostic page immune to version bumps"
        );
        assert_eq!(get.body, b"kv-bytes");
    }

    /// CONCEPT:EG-KG.storage.content-addressed-put — `GET /kv/version` reports tracking state, and a bad version path
    /// is a 400.
    #[test]
    fn eg359_version_endpoint_reports_and_validates() {
        let s = store();
        let none = None;
        // Initially agnostic (tracking inactive).
        let v = handle(&s, &none, &req("GET", "/kv/version", b"", &[]));
        assert!(String::from_utf8_lossy(&v.body).contains("\"tracking\":false"));
        // Set + read back.
        handle(&s, &none, &req("PUT", "/kv/version/9", b"", &[]));
        let v = handle(&s, &none, &req("GET", "/kv/version", b"", &[]));
        let j: serde_json::Value = serde_json::from_slice(&v.body).unwrap();
        assert_eq!(j["tracking"], true);
        assert_eq!(j["version"], 9);
        // Non-numeric version ⇒ 400.
        assert_eq!(
            handle(&s, &none, &req("PUT", "/kv/version/oops", b"", &[])).status,
            "400 Bad Request"
        );
    }

    /// CONCEPT:EG-KG.memory.zero-copy-snapshot-fork — snapshot a set of pages, fork two branches over HTTP,
    /// prove both read the shared page and a CoW write on one is isolated from the other,
    /// and that `/kv/fork/stats` reports resident bytes flat (ONE shared page + one CoW page).
    #[test]
    fn zerocopy_snapshot_fork_over_http() {
        let s = store();
        let none = None;
        // Seed two pages (content-addressed; the connector would PUT under its token-hash,
        // but here we PUT under explicit keys and snapshot those keys).
        handle(&s, &none, &req("PUT", "/kv/pageA", &[1u8; 256], &[]));
        handle(&s, &none, &req("PUT", "/kv/pageB", &[2u8; 256], &[]));

        // Snapshot both pages.
        let snap = handle(
            &s,
            &none,
            &req(
                "POST",
                "/kv/snapshot",
                br#"{"keys":["pageA","pageB"]}"#,
                &[],
            ),
        );
        assert_eq!(snap.status, "200 OK");
        let sj: serde_json::Value = serde_json::from_slice(&snap.body).unwrap();
        assert_eq!(sj["pages"], 2);
        let sid = sj["snapshot"].as_u64().unwrap();

        // Fork two branches.
        let branch = |sid: u64| -> u64 {
            let r = handle(
                &s,
                &none,
                &req("POST", &format!("/kv/snapshot/{sid}/fork"), b"", &[]),
            );
            assert_eq!(r.status, "200 OK");
            serde_json::from_slice::<serde_json::Value>(&r.body).unwrap()["branch"]
                .as_u64()
                .unwrap()
        };
        let b1 = branch(sid);
        let b2 = branch(sid);

        // Both branches read the shared pages byte-for-byte.
        let g = handle(
            &s,
            &none,
            &req("GET", &format!("/kv/branch/{b1}/pageA"), b"", &[]),
        );
        assert_eq!(g.status, "200 OK");
        assert_eq!(g.body, vec![1u8; 256]);
        assert_eq!(
            handle(
                &s,
                &none,
                &req("GET", &format!("/kv/branch/{b2}/pageA"), b"", &[])
            )
            .body,
            vec![1u8; 256]
        );

        // CoW write on b1 ⇒ isolated; b2 still sees the shared page.
        let p = handle(
            &s,
            &none,
            &req("PUT", &format!("/kv/branch/{b1}/pageA"), &[9u8; 256], &[]),
        );
        assert_eq!(p.status, "200 OK");
        assert_eq!(
            handle(
                &s,
                &none,
                &req("GET", &format!("/kv/branch/{b1}/pageA"), b"", &[])
            )
            .body,
            vec![9u8; 256],
            "writer sees its CoW value"
        );
        assert_eq!(
            handle(
                &s,
                &none,
                &req("GET", &format!("/kv/branch/{b2}/pageA"), b"", &[])
            )
            .body,
            vec![1u8; 256],
            "sibling isolated from the CoW write"
        );

        // fork/stats: 2 shared pages + 1 CoW overlay page = 3 * 256 resident, NOT 2 branches × 2 pages.
        let fs = handle(&s, &none, &req("GET", "/kv/fork/stats", b"", &[]));
        let fj: serde_json::Value = serde_json::from_slice(&fs.body).unwrap();
        assert_eq!(fj["branches"], 2);
        assert_eq!(fj["shared_pages"], 2);
        assert_eq!(fj["overlay_pages"], 1);
        assert_eq!(fj["resident_fork_bytes"], 3 * 256);

        // Unknown snapshot/branch paths are graceful.
        assert_eq!(
            handle(&s, &none, &req("POST", "/kv/snapshot/9999/fork", b"", &[])).status,
            "404 Not Found"
        );
        assert_eq!(
            handle(&s, &none, &req("GET", "/kv/branch/9999/pageA", b"", &[])).status,
            "404 Not Found"
        );
    }
}
