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
//! The listener starts only with configured JWT validation or a runtime-injected
//! bearer secret. Every request must authenticate or it is refused `401`.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;

use hmac::{Hmac, Mac};
use sha2::Sha256;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::RwLock;

use crate::server::ServerState;
use eg_kvcache::{BranchId, DataVersion, SharedKvBackend, SharedKvIndex, SnapshotId};

/// The networked (mutation-store-backed) shared backend — the durable, fleet-shared
/// KV-cache over the engine's live `kv.redb` (feature `kv`). See [`shared_store`].
#[cfg(feature = "kv")]
mod shared_store;
#[cfg(feature = "kv")]
pub use shared_store::{rank_keys_by_importance, SharedKvStoreBackend, SharedKvStoreStats};

/// Env var selecting the KV-cache backend (CONCEPT:EG-KG.backend.networked-shared-kv):
/// `durable` (aliases `shared`/`store`) ⇒ the mutation-store-backed, FLEET-SHARED,
/// restart-surviving [`SharedKvStoreBackend`] over the engine's live `kv.redb` — the SAME
/// store the `KvGet`/`KvPut` wire methods use, so a prefix block one serving instance PUTs
/// is a cache HIT for every other; requires the `kv` feature + a persist dir. Anything
/// else / unset ⇒ the in-process ephemeral [`SharedKvIndex`] (unchanged default).
pub const KVCACHE_BACKEND_ENV: &str = "EPISTEMIC_GRAPH_KVCACHE_BACKEND";
/// Env var labeling this serving instance in the KV-cache metrics/logs (so cross-instance
/// prefix reuse is attributable). Defaults to `EPISTEMIC_GRAPH_NODE_ID`, else `"kvcache"`.
pub const KVCACHE_INSTANCE_ID_ENV: &str = "EPISTEMIC_GRAPH_KVCACHE_INSTANCE_ID";
/// Upper bound on blocks paged in from the durable store for one `snapshot` (bounds the
/// read fan-in behind the LMCacheMPConnector snapshot→branch primitive).
#[cfg(feature = "kv")]
const DURABLE_SNAPSHOT_PAGE_IN_CAP: usize = 4096;

/// Env var: when set (and built `--features kvcache-server`) the KV-cache HTTP listener
/// binds this address (documented loopback default `127.0.0.1:9130`). Unset ⇒ no
/// listener.
pub const KVCACHE_ADDR_ENV: &str = "EPISTEMIC_GRAPH_KVCACHE_ADDR";
/// Env var: the runtime-injected bearer token used when JWT validation is not
/// configured. One of those two authentication modes is mandatory.
pub const KVCACHE_TOKEN_ENV: &str = "EPISTEMIC_GRAPH_KVCACHE_TOKEN";
/// Request header (CONCEPT:EG-KG.storage.content-addressed-put): the `GraphCore::version()` (KG-2.180) the PUT body was
/// DERIVED at. When present, the stored entry is version-tagged and goes stale once the
/// surface's current data version advances past it (a graph write drives
/// `PUT /kv/version/<n>`). Absent ⇒ a pure content-addressed KV page
/// ([`DataVersion::Agnostic`], never version-invalidated) — so existing connectors are
/// unaffected.
pub const KVCACHE_DATA_VERSION_HEADER: &str = "x-eg-data-version";
const MAX_HTTP_HEADER_BYTES: usize = 64 * 1024;
const MAX_KVCACHE_BODY_BYTES: usize = 64 * 1024 * 1024;
const HTTP_READ_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(15);

/// The KV-cache backing store: the shared, content-addressed, ref-counted index behind
/// a `Mutex` (the index needs `&mut` to `put`/`release`; the guard is held only for the
/// duration of one op). The networked, fleet-shared alternative — the durable,
/// mutation-store-backed `SharedKvStoreBackend` implementing the SAME [`SharedKvBackend`]
/// trait — is selectable via `KvCacheStore::with_durable` (env
/// `EPISTEMIC_GRAPH_KVCACHE_BACKEND=durable`).
pub struct KvCacheStore {
    /// The in-process shared index. When `durable` is `None` this IS the block store; when
    /// `durable` is `Some` it is only the zero-copy snapshot-fork working layer (pages are
    /// paged in from the durable store on `snapshot`).
    index: Mutex<SharedKvIndex>,
    /// When set, blocks live in the durable, mutation-store-backed, FLEET-SHARED backend
    /// over the engine's live `kv.redb` (CONCEPT:EG-KG.backend.networked-shared-kv) instead
    /// of the ephemeral in-process index — so the cache is shared across serving instances
    /// and survives an engine restart.
    #[cfg(feature = "kv")]
    durable: Option<SharedKvStoreBackend>,
}

impl Default for KvCacheStore {
    fn default() -> Self {
        Self::new()
    }
}

impl KvCacheStore {
    /// A fresh, empty in-process shared KV-cache store (ephemeral, single-process).
    pub fn new() -> Self {
        Self {
            index: Mutex::new(SharedKvIndex::new()),
            #[cfg(feature = "kv")]
            durable: None,
        }
    }

    /// A store whose blocks live in the durable, fleet-shared [`SharedKvStoreBackend`]
    /// (CONCEPT:EG-KG.backend.networked-shared-kv). The in-process index remains the
    /// zero-copy snapshot-fork working layer (pages are paged in from the durable store).
    #[cfg(feature = "kv")]
    pub fn with_durable(backend: SharedKvStoreBackend) -> Self {
        Self {
            index: Mutex::new(SharedKvIndex::new()),
            durable: Some(backend),
        }
    }

    fn get(&self, hash: &str) -> Option<Vec<u8>> {
        #[cfg(feature = "kv")]
        if let Some(durable) = &self.durable {
            return durable.get_block(hash);
        }
        self.index.lock().unwrap().get_block(hash)
    }

    /// Store `block` under `hash`, stamped with the [`DataVersion`] it was `derived_at`
    /// (CONCEPT:EG-KG.storage.content-addressed-put). `Ok(true)` iff a NEW block was created
    /// (`Ok(false)` on a dedup hit against an already-present block); `Err` on a durable
    /// store failure (so the surface can 500 rather than silently claim it cached).
    fn put(&self, hash: &str, block: Vec<u8>, derived_at: DataVersion) -> Result<bool, String> {
        #[cfg(feature = "kv")]
        if let Some(durable) = &self.durable {
            return durable.store_block(hash, block, derived_at);
        }
        Ok(self
            .index
            .lock()
            .unwrap()
            .put_block(hash, block, derived_at))
    }

    fn contains(&self, hash: &str) -> bool {
        #[cfg(feature = "kv")]
        if let Some(durable) = &self.durable {
            return durable.contains(hash);
        }
        self.index.lock().unwrap().contains(hash)
    }

    /// Advance the surface's current data version (CONCEPT:EG-KG.storage.content-addressed-put) — driven by a graph
    /// write (`PUT /kv/version/<n>`). The durable backend persists the epoch so the WHOLE
    /// fleet gates freshness against it (stale entries read as absent); the in-process
    /// index retires stale entries eagerly.
    fn set_data_version(&self, version: DataVersion) {
        #[cfg(feature = "kv")]
        if let Some(durable) = &self.durable {
            if let Err(error) = durable.set_version(version) {
                tracing::warn!(
                    target: "epistemic_graph::kvcache",
                    error = %error,
                    "kvcache set_data_version failed"
                );
            }
            return;
        }
        self.index.lock().unwrap().set_data_version(version);
    }

    /// The surface's current data version as connector-friendly JSON (CONCEPT:EG-KG.storage.content-addressed-put).
    fn version_json(&self) -> String {
        let current = self.current_version();
        let (tracking, version) = match current {
            DataVersion::Agnostic => (false, serde_json::Value::Null),
            DataVersion::At(n) => (true, serde_json::json!(n)),
        };
        serde_json::json!({ "tracking": tracking, "version": version }).to_string()
    }

    fn current_version(&self) -> DataVersion {
        #[cfg(feature = "kv")]
        if let Some(durable) = &self.durable {
            return durable.current_version();
        }
        self.index.lock().unwrap().current_version()
    }

    // ── zero-copy snapshot-fork surface (CONCEPT:EG-KG.memory.zero-copy-snapshot-fork) ─────────────
    //
    // The "LMCacheMPConnector snapshot → branch" primitive over HTTP: a connector pins a
    // candidate set of pages into a SNAPSHOT, forks N BRANCHES that all read those SAME
    // physical pages (zero-copy), and writes copy-on-write per branch. This is the rung the
    // AU `crossmodal_fork` `max_concurrency>1` path targets to make fan-out O(1) in copies.

    /// Pin the current pages for `keys` into a snapshot; returns `(snapshot_id, page_count)`.
    ///
    /// On the durable backend, the requested (present) blocks are first PAGED IN from the
    /// fleet-shared store into the in-process fork working-layer, so the LMCacheMPConnector
    /// snapshot→branch primitive works over durable, cross-instance blocks (bounded fan-in).
    /// The transient page-in ref is released after the snapshot pins the page — only the
    /// snapshot then holds it, so `release_snapshot` frees it (no leak).
    fn snapshot(&self, keys: &[String]) -> (u64, usize) {
        let mut idx = self.index.lock().unwrap();
        #[cfg(feature = "kv")]
        let paged_in: Vec<String> = if let Some(durable) = &self.durable {
            let mut newly = Vec::new();
            for key in keys.iter().take(DURABLE_SNAPSHOT_PAGE_IN_CAP) {
                if idx.refcount(key) == 0 {
                    if let Some(block) = durable.get_block(key) {
                        idx.put_block(key, block, DataVersion::Agnostic);
                        newly.push(key.clone());
                    }
                }
            }
            newly
        } else {
            Vec::new()
        };
        let id = idx.snapshot(keys);
        let pages = idx.snapshot_page_count(id).unwrap_or(0);
        #[cfg(feature = "kv")]
        for key in &paged_in {
            idx.release(key);
        }
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
        // Durable backend: the Seam-6 hit-rate export shape (hits/misses/dedup + hit_rate).
        // Occupancy (unique_blocks/resident_bytes) is deliberately omitted — a full scan on
        // every /kv/stats would be unbounded — so the stats probe stays O(1).
        #[cfg(feature = "kv")]
        if let Some(durable) = &self.durable {
            let s = durable.stats();
            return serde_json::json!({
                "backend": "durable",
                "instance": durable.instance_id(),
                "durable": durable.is_durable(),
                "get_hits": s.get_hits,
                "get_misses": s.get_misses,
                "dedup_hits": s.dedup_hits,
                "put_new": s.put_new,
                "stale_missed": s.stale_missed,
                "hit_rate": s.hit_rate(),
            })
            .to_string();
        }
        let s = self.index.lock().unwrap().stats();
        // Hand-built via serde_json (already workspace-wide) — flat, connector-friendly.
        serde_json::json!({
            "backend": "memory",
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
//
// The JWT verifier itself lives in `crate::server::oidc` (feature `oidc`) —
// shared with the primary `eg2.` protocol's identity binding, not vendored
// here.

/// A configured auth mode. JWT is preferred:
/// [`KvAuth::Jwt`] validates a Keycloak client-credentials token (paired with the
/// platform's overall auth — the same tokens graph-os validates); [`KvAuth::Static`]
/// is a shared bearer secret (the documented OpenBao-sourced fallback).
#[derive(Clone, Debug)]
pub enum KvAuth {
    Static(String),
    Jwt(std::sync::Arc<crate::server::oidc::JwtValidator>),
}

/// Require a valid `Authorization: Bearer <token>` — a static-secret match or a
/// signature/issuer/audience/expiry-verified Keycloak JWT.
fn authorized(auth: &KvAuth, headers: &HashMap<String, String>) -> bool {
    let token = match headers
        .get("authorization")
        .and_then(|h| h.strip_prefix("Bearer "))
        .map(str::trim)
    {
        Some(t) => t,
        None => return false,
    };
    match auth {
        KvAuth::Static(secret) => {
            // Compare fixed-size HMAC tags rather than secret-bearing strings.
            let Ok(mut candidate) = Hmac::<Sha256>::new_from_slice(token.as_bytes()) else {
                return false;
            };
            candidate.update(b"epistemic-graph:kvcache-static-token");
            let candidate_tag = candidate.finalize().into_bytes();
            let Ok(mut expected) = Hmac::<Sha256>::new_from_slice(secret.as_bytes()) else {
                return false;
            };
            expected.update(b"epistemic-graph:kvcache-static-token");
            expected.verify_slice(&candidate_tag).is_ok()
        }
        KvAuth::Jwt(validator) => validator.validate(token),
    }
}

/// Parse the `X-EG-Data-Version` header (CONCEPT:EG-KG.storage.content-addressed-put) into a [`DataVersion`]. A valid
/// unsigned integer ⇒ [`DataVersion::At`]; absent or unparseable ⇒
/// [`DataVersion::Agnostic`] (a pure content-addressed KV page outside version
/// invalidation).
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
fn handle(store: &KvCacheStore, auth: &KvAuth, req: &KvRequest) -> KvResponse {
    if !authorized(auth, &req.headers) {
        return KvResponse::error(
            "401 Unauthorized",
            "Unauthorized",
            "missing or invalid bearer token",
        );
    }
    // A18: the bearer/JWT guard above IS this surface's own carrier proof — mint
    // the engine-owned authority now that it succeeded, then gate through the
    // SAME shared check every other surface uses (real now: denies iff no
    // carrier was minted; here it can only be minted once `authorized` passed).
    let carrier = crate::server::auth::VerifiedRequestContext::authenticated_kvcache_actor()
        .ok()
        .and_then(|context| crate::server::access::CarrierAuthority::from_verified(&context).ok());
    if crate::server::access::unauthenticated_carrier_denied(carrier.as_ref()) {
        return KvResponse::error(
            "403 Forbidden",
            "AccessDenied",
            "KV-cache carrier has no verified tenant/page ownership",
        );
    }
    handle_authorized(store, req)
}

/// Route a request after the served boundary authenticated it. Kept separate so
/// unit tests can exercise storage/routing without manufacturing credentials;
/// only [`handle`] is called by the network listener.
fn handle_authorized(store: &KvCacheStore, req: &KvRequest) -> KvResponse {
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
            // 201 on a brand-new block; 200 on a dedup hit (the block was already present);
            // 500 on a durable-store write failure (never a silent success — no masking).
            match store.put(hash, req.body.clone(), derived_at) {
                Ok(true) => KvResponse::empty("201 Created"),
                Ok(false) => KvResponse::empty("200 OK"),
                Err(error) => {
                    tracing::warn!(
                        target: "epistemic_graph::kvcache",
                        hash = %hash,
                        error = %error,
                        "kvcache PUT failed"
                    );
                    KvResponse::error(
                        "500 Internal Server Error",
                        "StoreError",
                        "failed to store block",
                    )
                }
            }
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
        if buf.len() > MAX_HTTP_HEADER_BYTES {
            return None; // header flood guard
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
    // The address is an opaque hex/base token-hash — no query params are used here.
    let path = target.split('?').next().unwrap_or(&target).to_string();

    let mut headers = HashMap::new();
    let mut content_length: Option<usize> = None;
    for line in lines {
        let (k, v) = line.split_once(':')?;
        let key = k.trim().to_ascii_lowercase();
        let val = v.trim().to_string();
        if key.is_empty() || headers.contains_key(&key) {
            return None;
        }
        if key == "content-length" {
            if content_length.is_some() {
                return None;
            }
            content_length = Some(val.parse().ok()?);
        } else if key == "transfer-encoding" {
            return None;
        }
        headers.insert(key, val);
    }
    let content_length = content_length.unwrap_or(0);
    if content_length > MAX_KVCACHE_BODY_BYTES {
        return None;
    }
    let mut body = buf[header_end + 4..].to_vec();
    if body.len() > content_length || body.len() > MAX_KVCACHE_BODY_BYTES {
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
    Some(KvRequest {
        method,
        path,
        headers,
        body,
    })
}

/// Production KV-cache listener linked to the live engine isolation policy. The
/// HTTP bearer/JWT guard does not establish tenant+actor ownership of individual
/// cache pages, so secure/RLS mode fails the carrier closed.
pub async fn serve_with_security(
    addr: &str,
    state: Arc<RwLock<ServerState>>,
) -> std::io::Result<()> {
    let store = Arc::new(resolve_store(&state).await?);
    let auth = resolve_auth()?;
    serve_with_store_inner(addr, store, auth).await
}

/// Select the KV-cache backend from `EPISTEMIC_GRAPH_KVCACHE_BACKEND`
/// (CONCEPT:EG-KG.backend.networked-shared-kv): `durable` (aliases `shared`/`store`) ⇒ the
/// mutation-store-backed, FLEET-SHARED backend over the engine's live `kv.redb` (the SAME
/// store the `KvGet`/`KvPut` wire methods use, so blocks are shared across every serving
/// instance + survive a restart); anything else / unset ⇒ the in-process ephemeral index
/// (default, unchanged).
async fn resolve_store(state: &Arc<RwLock<ServerState>>) -> std::io::Result<KvCacheStore> {
    let backend = std::env::var(KVCACHE_BACKEND_ENV).unwrap_or_default();
    let want_durable = matches!(
        backend.trim().to_ascii_lowercase().as_str(),
        "durable" | "shared" | "store"
    );
    if !want_durable {
        return Ok(KvCacheStore::new());
    }
    #[cfg(feature = "kv")]
    {
        let kv = { state.read().await.kv.clone() };
        let Some(kv) = kv else {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "EPISTEMIC_GRAPH_KVCACHE_BACKEND=durable requires the kv store (set GRAPH_SERVICE_PERSIST_DIR)",
            ));
        };
        let instance_id = std::env::var(KVCACHE_INSTANCE_ID_ENV)
            .ok()
            .filter(|value| !value.is_empty())
            .or_else(|| {
                std::env::var("EPISTEMIC_GRAPH_NODE_ID")
                    .ok()
                    .filter(|value| !value.is_empty())
            })
            .unwrap_or_else(|| "kvcache".to_string());
        if !kv.is_durable() {
            tracing::warn!(
                target: "epistemic_graph::kvcache",
                "kvcache durable backend selected but the kv store is in-memory (no persist dir) — shared only across connections of THIS process, NOT restart-surviving"
            );
        }
        tracing::info!(
            target: "epistemic_graph::kvcache",
            instance = %instance_id,
            "kvcache: using the durable, fleet-shared mutation-store-backed backend"
        );
        Ok(KvCacheStore::with_durable(SharedKvStoreBackend::new(
            kv,
            instance_id,
        )))
    }
    #[cfg(not(feature = "kv"))]
    {
        let _ = state;
        Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "EPISTEMIC_GRAPH_KVCACHE_BACKEND=durable requires the `kv` feature",
        ))
    }
}

/// Resolve the mandatory bearer-token credential from the environment.
fn resolve_auth() -> std::io::Result<KvAuth> {
    // JWT first — paired with the platform's configured OIDC provider. An issuer
    // selects JWT mode and therefore makes its audience and JWKS URL mandatory.
    match crate::server::oidc::JwtValidator::from_env() {
        Ok(Some(validator)) => return Ok(KvAuth::Jwt(std::sync::Arc::new(validator))),
        Ok(None) => {}
        Err(message) => {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                message,
            ));
        }
    }
    std::env::var(KVCACHE_TOKEN_ENV)
        .ok()
        .filter(|s| !s.is_empty())
        .map(KvAuth::Static)
        .ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                "kvcache-server requires JWT configuration or a bearer-token secret",
            )
        })
}

async fn serve_with_store_inner(
    addr: &str,
    store: Arc<KvCacheStore>,
    auth: KvAuth,
) -> std::io::Result<()> {
    let listener = TcpListener::bind(addr).await?;
    crate::server::require_loopback_listener(&listener)?;
    tracing::info!(
        "kvcache-server: serving authenticated shared KV-cache HTTP surface on {}",
        addr
    );
    loop {
        let (mut stream, _peer) = listener.accept().await?;
        let store = store.clone();
        let auth = auth.clone();
        tokio::spawn(async move {
            let resp =
                match tokio::time::timeout(HTTP_READ_TIMEOUT, read_request(&mut stream)).await {
                    Ok(Some(req)) => handle(&store, &auth, &req),
                    _ => KvResponse::error("400 Bad Request", "InvalidRequest", "malformed"),
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
        let block = vec![7u8, 0, 255, 42, 13];
        // First PUT → 201 Created.
        let put = handle_authorized(&s, &req("PUT", "/kv/abc123", &block, &[]));
        assert_eq!(put.status, "201 Created");
        // GET → the exact bytes back.
        let get = handle_authorized(&s, &req("GET", "/kv/abc123", b"", &[]));
        assert_eq!(get.status, "200 OK");
        assert_eq!(get.body, block);
        assert_eq!(get.content_type, "application/octet-stream");
    }

    #[test]
    fn eg187_head_and_exists_probe() {
        let s = store();
        handle_authorized(&s, &req("PUT", "/kv/h1", b"x", &[]));
        // HEAD present → 200, no body.
        let head = handle_authorized(&s, &req("HEAD", "/kv/h1", b"", &[]));
        assert_eq!(head.status, "200 OK");
        assert!(head.head_only);
        // HEAD absent → 404.
        assert_eq!(
            handle_authorized(&s, &req("HEAD", "/kv/nope", b"", &[])).status,
            "404 Not Found"
        );
        // GET /kv/<hash>/exists → JSON true/false.
        let ex = handle_authorized(&s, &req("GET", "/kv/h1/exists", b"", &[]));
        assert_eq!(ex.status, "200 OK");
        assert!(String::from_utf8_lossy(&ex.body).contains("\"exists\":true"));
        let no = handle_authorized(&s, &req("GET", "/kv/ghost/exists", b"", &[]));
        assert!(String::from_utf8_lossy(&no.body).contains("\"exists\":false"));
    }

    #[test]
    fn eg187_get_missing_block_is_404() {
        let s = store();
        assert_eq!(
            handle_authorized(&s, &req("GET", "/kv/missing", b"", &[])).status,
            "404 Not Found"
        );
    }

    #[test]
    fn eg187_stats_json_reports_dedup() {
        let s = store();
        // Two PUTs of the SAME hash ⇒ one resident block, a dedup hit on the second.
        assert_eq!(
            handle_authorized(&s, &req("PUT", "/kv/dup", &[1u8; 100], &[])).status,
            "201 Created"
        );
        assert_eq!(
            handle_authorized(&s, &req("PUT", "/kv/dup", &[1u8; 100], &[])).status,
            "200 OK"
        );
        let stats = handle_authorized(&s, &req("GET", "/kv/stats", b"", &[]));
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
        let auth = KvAuth::Static("sekret".into());
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
    }

    #[test]
    fn eg18_authenticated_carrier_allowed_unauthenticated_denied() {
        // A18: the carrier-check stub used to deny EVERY request unconditionally,
        // even a correctly bearer-authenticated one. Prove both directions of the
        // real fix directly against the CarrierAuthority-gated response, not just
        // implicitly via the bearer-guard's own 401.
        let s = store();
        let auth = KvAuth::Static("sekret".into());

        // Unauthenticated: no bearer token at all → denied (401, from the bearer
        // guard itself — the carrier gate downstream is never even reached).
        let denied = handle(&s, &auth, &req("GET", "/kv/stats", b"", &[]));
        assert_eq!(denied.status, "401 Unauthorized");

        // Authenticated: a correct bearer token → the bearer guard AND the A18
        // carrier gate both pass, reaching the real stats response.
        let allowed = handle(
            &s,
            &auth,
            &req(
                "GET",
                "/kv/stats",
                b"",
                &[("authorization", "Bearer sekret")],
            ),
        );
        assert_eq!(
            allowed.status,
            "200 OK",
            "an authenticated carrier must be allowed through the A18 gate; got: {}",
            String::from_utf8_lossy(&allowed.body)
        );
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
                        Some(r) => handle_authorized(&store, &r),
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
        // Advance to data version 5, then cache context derived at v5.
        assert_eq!(
            handle_authorized(&s, &req("PUT", "/kv/version/5", b"", &[])).status,
            "200 OK"
        );
        let put = handle_authorized(
            &s,
            &req("PUT", "/kv/ctx", b"ctx-v5", &[("x-eg-data-version", "5")]),
        );
        assert_eq!(put.status, "201 Created");
        let get = handle_authorized(&s, &req("GET", "/kv/ctx", b"", &[]));
        assert_eq!(get.status, "200 OK");
        assert_eq!(get.body, b"ctx-v5");

        // A graph write bumps the surface's data version ⇒ v5 context is stale ⇒ 404.
        assert_eq!(
            handle_authorized(&s, &req("PUT", "/kv/version/6", b"", &[])).status,
            "200 OK"
        );
        assert_eq!(
            handle_authorized(&s, &req("GET", "/kv/ctx", b"", &[])).status,
            "404 Not Found",
            "stale context must not be served"
        );
        assert_eq!(
            handle_authorized(&s, &req("HEAD", "/kv/ctx", b"", &[])).status,
            "404 Not Found"
        );
        let ex = handle_authorized(&s, &req("GET", "/kv/ctx/exists", b"", &[]));
        assert!(String::from_utf8_lossy(&ex.body).contains("\"exists\":false"));

        // Re-cache the freshly-derived context at v6 ⇒ served again.
        let put = handle_authorized(
            &s,
            &req("PUT", "/kv/ctx", b"ctx-v6", &[("x-eg-data-version", "6")]),
        );
        assert_eq!(put.status, "201 Created");
        let get = handle_authorized(&s, &req("GET", "/kv/ctx", b"", &[]));
        assert_eq!(get.body, b"ctx-v6", "fresh context served");
    }

    /// CONCEPT:EG-KG.storage.content-addressed-put — a PUT WITHOUT the version header is a pure content-addressed page
    /// (Agnostic) that survives version bumps, so existing connectors are unaffected.
    #[test]
    fn eg359_unversioned_put_is_agnostic_and_survives_bumps() {
        let s = store();
        assert_eq!(
            handle_authorized(&s, &req("PUT", "/kv/page", b"kv-bytes", &[])).status,
            "201 Created"
        );
        handle_authorized(&s, &req("PUT", "/kv/version/1", b"", &[]));
        handle_authorized(&s, &req("PUT", "/kv/version/2", b"", &[]));
        let get = handle_authorized(&s, &req("GET", "/kv/page", b"", &[]));
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
        // Initially agnostic (tracking inactive).
        let v = handle_authorized(&s, &req("GET", "/kv/version", b"", &[]));
        assert!(String::from_utf8_lossy(&v.body).contains("\"tracking\":false"));
        // Set + read back.
        handle_authorized(&s, &req("PUT", "/kv/version/9", b"", &[]));
        let v = handle_authorized(&s, &req("GET", "/kv/version", b"", &[]));
        let j: serde_json::Value = serde_json::from_slice(&v.body).unwrap();
        assert_eq!(j["tracking"], true);
        assert_eq!(j["version"], 9);
        // Non-numeric version ⇒ 400.
        assert_eq!(
            handle_authorized(&s, &req("PUT", "/kv/version/oops", b"", &[])).status,
            "400 Bad Request"
        );
    }

    /// CONCEPT:EG-KG.memory.zero-copy-snapshot-fork — snapshot a set of pages, fork two branches over HTTP,
    /// prove both read the shared page and a CoW write on one is isolated from the other,
    /// and that `/kv/fork/stats` reports resident bytes flat (ONE shared page + one CoW page).
    #[test]
    fn zerocopy_snapshot_fork_over_http() {
        let s = store();
        // Seed two pages (content-addressed; the connector would PUT under its token-hash,
        // but here we PUT under explicit keys and snapshot those keys).
        handle_authorized(&s, &req("PUT", "/kv/pageA", &[1u8; 256], &[]));
        handle_authorized(&s, &req("PUT", "/kv/pageB", &[2u8; 256], &[]));

        // Snapshot both pages.
        let snap = handle_authorized(
            &s,
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
            let r = handle_authorized(
                &s,
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
        let g = handle_authorized(&s, &req("GET", &format!("/kv/branch/{b1}/pageA"), b"", &[]));
        assert_eq!(g.status, "200 OK");
        assert_eq!(g.body, vec![1u8; 256]);
        assert_eq!(
            handle_authorized(&s, &req("GET", &format!("/kv/branch/{b2}/pageA"), b"", &[])).body,
            vec![1u8; 256]
        );

        // CoW write on b1 ⇒ isolated; b2 still sees the shared page.
        let p = handle_authorized(
            &s,
            &req("PUT", &format!("/kv/branch/{b1}/pageA"), &[9u8; 256], &[]),
        );
        assert_eq!(p.status, "200 OK");
        assert_eq!(
            handle_authorized(&s, &req("GET", &format!("/kv/branch/{b1}/pageA"), b"", &[])).body,
            vec![9u8; 256],
            "writer sees its CoW value"
        );
        assert_eq!(
            handle_authorized(&s, &req("GET", &format!("/kv/branch/{b2}/pageA"), b"", &[])).body,
            vec![1u8; 256],
            "sibling isolated from the CoW write"
        );

        // fork/stats: 2 shared pages + 1 CoW overlay page = 3 * 256 resident, NOT 2 branches × 2 pages.
        let fs = handle_authorized(&s, &req("GET", "/kv/fork/stats", b"", &[]));
        let fj: serde_json::Value = serde_json::from_slice(&fs.body).unwrap();
        assert_eq!(fj["branches"], 2);
        assert_eq!(fj["shared_pages"], 2);
        assert_eq!(fj["overlay_pages"], 1);
        assert_eq!(fj["resident_fork_bytes"], 3 * 256);

        // Unknown snapshot/branch paths are graceful.
        assert_eq!(
            handle_authorized(&s, &req("POST", "/kv/snapshot/9999/fork", b"", &[])).status,
            "404 Not Found"
        );
        assert_eq!(
            handle_authorized(&s, &req("GET", "/kv/branch/9999/pageA", b"", &[])).status,
            "404 Not Found"
        );
    }
}
