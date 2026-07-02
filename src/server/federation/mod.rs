//! Super-cluster federated search (CONCEPT:EG-243).
//!
//! Fans a *read* query across a registry of peer engine instances, runs the SAME
//! query locally, and MERGES the partial result sets into one answer — the
//! multi-region / federated capability that lets a super-cluster of engines answer
//! as one (the "surpass OpenObserve federated search" goal).
//!
//! Design (all feature-gated behind `federation-search`, kept OUT of the Pi tier so
//! no `ureq`/rustls enters the slim build):
//!
//! * [`PeerRegistry`] — the set of peer base-URLs, seeded from the comma-separated
//!   env var [`FEDERATION_PEERS_ENV`] plus a programmatic [`PeerRegistry::register`].
//! * SSRF safety — every peer URL is vetted with the SAME allowlist + internal-range
//!   guard the SPARQL `SERVICE` federation uses (CONCEPT:EG-052): a peer that
//!   resolves to a loopback / link-local / RFC-1918 address is refused UNLESS the
//!   operator opts it in via [`FEDERATION_ALLOW_ENV`]. Per-peer connect + read
//!   timeouts bound a slow/hostile peer.
//! * Fan-out + merge — the query runs locally AND is POSTed concurrently to each
//!   peer's `/federated?local=1` endpoint (the `local=1` flag makes the peer run
//!   ONLY its own store, so there is no fan-out recursion). Partials are unioned and
//!   de-duplicated by key; ranked/search partials are re-ranked with Reciprocal Rank
//!   Fusion (RRF, the `Op::FuseRrf` idea). A slow / dead peer is SKIPPED and noted in
//!   `failed_peers` with `partial: true` — it never fails the whole query.
//! * HTTP surface — a small `/federated` POST listener (`{query, lang}` → merged
//!   rows + a `metadata` block). Reuses the same hand-rolled HTTP framing idiom as
//!   `sparql_http` (no new HTTP dep).

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::RwLock;

use crate::server::ServerState;

/// Comma-separated peer engine base-URLs, e.g.
/// `https://eg-eu.example:7900,https://eg-us.example:7900` (CONCEPT:EG-243).
pub const FEDERATION_PEERS_ENV: &str = "EPISTEMIC_GRAPH_FEDERATION_PEERS";

/// SSRF allowlist for outbound peer fan-out (CONCEPT:EG-243, mirrors EG-052). A
/// comma-separated list of bare hosts or `scheme://host:port` origins that may be
/// contacted even though they resolve to an internal (loopback / RFC-1918 / link-local)
/// address. Empty / unset ⇒ internal peers are refused (fail-closed default).
pub const FEDERATION_ALLOW_ENV: &str = "EPISTEMIC_GRAPH_FEDERATION_ALLOW";

/// Default graph a bare federated query resolves against (shared with the SPARQL surface).
pub const DEFAULT_GRAPH_ENV: &str = "EPISTEMIC_GRAPH_SPARQL_DEFAULT_GRAPH";

/// Per-peer connect timeout (seconds) — a peer that will not connect fast degrades.
const CONNECT_TIMEOUT_SECS: u64 = 5;
/// Per-peer read timeout (seconds) — a peer that answers slowly is dropped from the merge.
const READ_TIMEOUT_SECS: u64 = 20;
/// Response-size cap per peer (bytes) — a hostile/misbehaving peer must not OOM the pool.
const MAX_RESPONSE_BYTES: u64 = 64 * 1024 * 1024;
/// RRF rank constant `k` (the canonical 60) used when fusing ranked partials.
const RRF_K: f64 = 60.0;

// ── Normalized federated row ──────────────────────────────────────────────

/// One normalized result row shared by the local store and every peer (CONCEPT:EG-243).
/// `key` is the union/dedup identity; `score` (when present) drives re-ranking; `data`
/// carries the opaque payload the caller cares about.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct FedRow {
    /// De-duplication identity (a node id, an IRI, a row hash …).
    pub key: String,
    /// Relevance score for ranked/search queries; `None` for unordered rows.
    #[serde(default)]
    pub score: Option<f64>,
    /// Opaque row payload passed through to the caller.
    #[serde(default)]
    pub data: serde_json::Value,
}

impl FedRow {
    /// A ranked row with a score.
    pub fn scored(key: impl Into<String>, score: f64, data: serde_json::Value) -> Self {
        Self {
            key: key.into(),
            score: Some(score),
            data,
        }
    }
}

/// The outcome of contacting ONE source (local or a peer): either its rows, or the
/// error that made it degrade (CONCEPT:EG-243). A degraded peer keeps its label so the
/// merged metadata can report it in `failed_peers`.
#[derive(Debug, Clone)]
pub struct PeerOutcome {
    /// The source label — a peer base-URL, or `"<local>"` for the local store.
    pub source: String,
    /// The source's partial rows, or the degrade reason.
    pub rows: Result<Vec<FedRow>, String>,
}

/// The merged federated answer + provenance metadata (CONCEPT:EG-243).
#[derive(Debug, Clone, serde::Serialize)]
pub struct FederatedResponse {
    /// The merged, de-duplicated (and, for ranked queries, RRF-re-ranked) rows.
    pub rows: Vec<FedRow>,
    /// Provenance: which peers were queried / failed, and whether the answer is partial.
    pub metadata: FederatedMetadata,
}

/// Provenance block returned alongside the merged rows (CONCEPT:EG-243).
#[derive(Debug, Clone, serde::Serialize)]
pub struct FederatedMetadata {
    /// Peer base-URLs that were contacted (excludes the always-present local store).
    pub peers_queried: Vec<String>,
    /// Peer base-URLs that degraded (timeout / refused / SSRF-blocked / parse error).
    pub failed_peers: Vec<String>,
    /// `true` when at least one source degraded — the answer is a best-effort union.
    pub partial: bool,
    /// Total sources that contributed rows (local + healthy peers).
    pub contributing_sources: usize,
}

// ── Peer registry ─────────────────────────────────────────────────────────

/// The set of peer engine base-URLs to fan a federated query out to (CONCEPT:EG-243).
#[derive(Debug, Clone, Default)]
pub struct PeerRegistry {
    peers: Vec<String>,
}

impl PeerRegistry {
    /// An empty registry (local-only federation).
    pub fn new() -> Self {
        Self::default()
    }

    /// Parse a comma-separated peer list (CONCEPT:EG-243). Blanks are dropped, each
    /// entry is trimmed, a trailing `/` is normalized off, and duplicates are removed
    /// (order-preserving) so the same peer is never queried twice.
    pub fn parse(raw: &str) -> Self {
        let mut peers: Vec<String> = Vec::new();
        for tok in raw.split(',') {
            let t = tok.trim().trim_end_matches('/');
            if t.is_empty() {
                continue;
            }
            let owned = t.to_string();
            if !peers.contains(&owned) {
                peers.push(owned);
            }
        }
        Self { peers }
    }

    /// Seed from [`FEDERATION_PEERS_ENV`]; unset / empty ⇒ an empty registry.
    pub fn from_env() -> Self {
        std::env::var(FEDERATION_PEERS_ENV)
            .ok()
            .map(|raw| Self::parse(&raw))
            .unwrap_or_default()
    }

    /// Programmatically add a peer base-URL (CONCEPT:EG-243). No-ops on a blank or
    /// already-registered URL. Returns `true` when the peer was newly added.
    pub fn register(&mut self, url: impl Into<String>) -> bool {
        let url = url.into().trim().trim_end_matches('/').to_string();
        if url.is_empty() || self.peers.contains(&url) {
            return false;
        }
        self.peers.push(url);
        true
    }

    /// The registered peer base-URLs.
    pub fn peers(&self) -> &[String] {
        &self.peers
    }

    /// `true` when there are no peers (a purely-local "federation").
    pub fn is_empty(&self) -> bool {
        self.peers.is_empty()
    }
}

// ── SSRF guard (mirrors CONCEPT:EG-052) ──────────────────────────────────

/// The SSRF allowlist for peer fan-out (CONCEPT:EG-243). Built from [`FEDERATION_ALLOW_ENV`];
/// an internal-range peer is only contacted when its host is named here.
#[derive(Debug, Clone, Default)]
pub struct PeerAllowlist {
    allow: Vec<String>,
}

impl PeerAllowlist {
    /// Build from [`FEDERATION_ALLOW_ENV`] (empty / unset ⇒ no internal peers allowed).
    pub fn from_env() -> Self {
        let allow = std::env::var(FEDERATION_ALLOW_ENV)
            .ok()
            .map(|raw| {
                raw.split(',')
                    .map(|s| s.trim().to_ascii_lowercase())
                    .filter(|s| !s.is_empty())
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        Self { allow }
    }

    /// Build from an explicit list (test seam).
    pub fn from_list(list: &[&str]) -> Self {
        Self {
            allow: list.iter().map(|s| s.to_ascii_lowercase()).collect(),
        }
    }

    /// SSRF guard (CONCEPT:EG-243, mirrors EG-052): the peer URL must be http(s), and if
    /// its host resolves to a loopback / link-local / RFC-1918 / unspecified address it
    /// is REFUSED unless (a) the bare host string is in the allowlist, or (b) the host is
    /// itself an allowlisted IP literal. A public host passes without an allowlist entry.
    pub fn check(&self, peer_url: &str) -> Result<(), String> {
        let is_https = peer_url.starts_with("https://");
        let rest = peer_url
            .strip_prefix("https://")
            .or_else(|| peer_url.strip_prefix("http://"))
            .ok_or_else(|| format!("peer must be http(s): '{peer_url}'"))?;
        let authority = rest
            .split(['/', '?', '#'])
            .next()
            .unwrap_or("")
            .rsplit('@') // drop any userinfo
            .next()
            .unwrap_or("");
        let (host, port): (&str, u16) = match authority.rsplit_once(':') {
            Some((h, p)) if !p.is_empty() && p.chars().all(|c| c.is_ascii_digit()) => {
                (h, p.parse().unwrap_or(0))
            }
            _ => (authority, if is_https { 443 } else { 80 }),
        };
        let host = host.trim_start_matches('[').trim_end_matches(']');
        if host.is_empty() {
            return Err(format!("peer has no host: '{peer_url}'"));
        }
        let host_lc = host.to_ascii_lowercase();
        let host_allowlisted = self.allow.iter().any(|a| {
            *a == host_lc
                || *a == format!("{host_lc}:{port}")
                || *a == format!("http://{host_lc}")
                || *a == format!("https://{host_lc}")
                || *a == format!("http://{host_lc}:{port}")
                || *a == format!("https://{host_lc}:{port}")
        });
        // Resolve + reject internal ranges unless the host was explicitly allowlisted
        // (an allowlisted IP literal like `127.0.0.1` is thereby the operator's opt-in).
        use std::net::ToSocketAddrs;
        let addrs = (host, port)
            .to_socket_addrs()
            .map_err(|e| format!("SSRF guard: cannot resolve peer '{host}': {e}"))?;
        for sa in addrs {
            if is_blocked_ip(&sa.ip()) && !host_allowlisted {
                return Err(format!(
                    "SSRF guard: peer '{host}' resolves to internal address {} and is not allowlisted",
                    sa.ip()
                ));
            }
        }
        Ok(())
    }
}

/// An internal (SSRF-sensitive) IP: loopback, unspecified, link-local, or RFC-1918 /
/// unique-local private space (CONCEPT:EG-243, mirrors EG-052).
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
                || (v6.segments()[0] & 0xffc0) == 0xfe80 // link-local fe80::/10
                || (v6.segments()[0] & 0xfe00) == 0xfc00 // unique-local fc00::/7
        }
    }
}

// ── Merge / dedup / RRF re-rank ───────────────────────────────────────────

/// Merge the local + peer partial result sets into ONE answer (CONCEPT:EG-243).
///
/// * Rows are UNIONED and de-duplicated by [`FedRow::key`] (first occurrence wins for
///   the payload; the best score seen is kept).
/// * When ANY source contributed a score, the merged rows are re-ranked with Reciprocal
///   Rank Fusion (RRF) over each source's own ranking (the `Op::FuseRrf` idea). With no
///   scores anywhere, the union order (first-seen) is preserved.
/// * A degraded source (`rows: Err`) contributes nothing but is recorded so the metadata
///   can report `partial: true` + `failed_peers`.
pub fn merge_partials(outcomes: Vec<PeerOutcome>) -> FederatedResponse {
    let mut peers_queried: Vec<String> = Vec::new();
    let mut failed_peers: Vec<String> = Vec::new();
    let mut healthy_lists: Vec<Vec<FedRow>> = Vec::new();
    let mut any_score = false;
    let mut contributing = 0usize;

    for outcome in outcomes {
        let is_local = outcome.source == "<local>";
        if !is_local {
            peers_queried.push(outcome.source.clone());
        }
        match outcome.rows {
            Ok(rows) => {
                if rows.iter().any(|r| r.score.is_some()) {
                    any_score = true;
                }
                contributing += 1;
                healthy_lists.push(rows);
            }
            Err(_) => {
                // Local degradation is not a "peer" failure but still forces partial.
                if !is_local {
                    failed_peers.push(outcome.source);
                }
            }
        }
    }

    // Union + dedup by key (keep first payload, best score).
    let mut order: Vec<String> = Vec::new();
    let mut merged: HashMap<String, FedRow> = HashMap::new();
    for list in &healthy_lists {
        for row in list {
            match merged.get_mut(&row.key) {
                Some(existing) => {
                    if let Some(s) = row.score {
                        existing.score = Some(match existing.score {
                            Some(cur) => cur.max(s),
                            None => s,
                        });
                    }
                }
                None => {
                    order.push(row.key.clone());
                    merged.insert(row.key.clone(), row.clone());
                }
            }
        }
    }

    let mut rows: Vec<FedRow> = if any_score {
        // RRF: score each key by Σ 1/(k + rank) over every source list it appears in.
        let rrf = rrf_scores(&healthy_lists);
        let mut keyed: Vec<FedRow> = order
            .into_iter()
            .filter_map(|k| merged.remove(&k))
            .collect();
        keyed.sort_by(|a, b| {
            let ra = rrf.get(&a.key).copied().unwrap_or(0.0);
            let rb = rrf.get(&b.key).copied().unwrap_or(0.0);
            rb.partial_cmp(&ra)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| a.key.cmp(&b.key))
        });
        keyed
    } else {
        order
            .into_iter()
            .filter_map(|k| merged.remove(&k))
            .collect()
    };

    // Stamp the fused RRF score onto ranked rows so the caller sees the merged ranking.
    if any_score {
        let rrf = rrf_scores(&healthy_lists);
        for row in rows.iter_mut() {
            if let Some(s) = rrf.get(&row.key) {
                row.score = Some(*s);
            }
        }
    }

    let partial = !failed_peers.is_empty();
    FederatedResponse {
        rows,
        metadata: FederatedMetadata {
            peers_queried,
            failed_peers,
            partial,
            contributing_sources: contributing,
        },
    }
}

/// Reciprocal Rank Fusion score per key across the source ranked lists (CONCEPT:EG-243).
/// Each list is taken in its given order; a key at 0-based `rank` in a list adds
/// `1/(RRF_K + rank + 1)`; the per-key sum across lists is the fused score.
fn rrf_scores(lists: &[Vec<FedRow>]) -> HashMap<String, f64> {
    let mut scores: HashMap<String, f64> = HashMap::new();
    for list in lists {
        for (rank, row) in list.iter().enumerate() {
            let contrib = 1.0 / (RRF_K + (rank as f64) + 1.0);
            *scores.entry(row.key.clone()).or_insert(0.0) += contrib;
        }
    }
    scores
}

// ── Peer fan-out (ureq, blocking) ─────────────────────────────────────────

/// POST the query to ONE peer's `/federated?local=1` endpoint and parse its rows
/// (CONCEPT:EG-243). Runs inside a `spawn_blocking` task (the `ureq` client is blocking).
/// SSRF-vetted first; any failure (blocked / connect / read / parse) is returned as the
/// degrade reason so the caller records it as a failed peer.
fn fetch_one_peer(
    peer: &str,
    allow: &PeerAllowlist,
    query: &str,
    lang: &str,
) -> Result<Vec<FedRow>, String> {
    use std::io::Read;
    allow.check(peer)?;
    let endpoint = format!("{peer}/federated?local=1");
    let body = serde_json::json!({ "query": query, "lang": lang }).to_string();
    let agent = ureq::AgentBuilder::new()
        .timeout_connect(Duration::from_secs(CONNECT_TIMEOUT_SECS))
        .timeout_read(Duration::from_secs(READ_TIMEOUT_SECS))
        .build();
    let resp = agent
        .post(&endpoint)
        .set("Content-Type", "application/json")
        .set("Accept", "application/json")
        .send_string(&body)
        .map_err(|e| format!("POST {endpoint} failed: {e}"))?;
    let mut text = String::new();
    resp.into_reader()
        .take(MAX_RESPONSE_BYTES)
        .read_to_string(&mut text)
        .map_err(|e| format!("reading {endpoint} response: {e}"))?;
    parse_peer_rows(&text)
}

/// Parse a peer's `/federated` JSON response back into rows (CONCEPT:EG-243). Accepts the
/// full `{rows: [...], metadata: {...}}` envelope this module emits, or a bare `[...]`.
fn parse_peer_rows(text: &str) -> Result<Vec<FedRow>, String> {
    let val: serde_json::Value =
        serde_json::from_str(text).map_err(|e| format!("invalid peer JSON: {e}"))?;
    let rows_val = match val {
        serde_json::Value::Array(_) => val,
        serde_json::Value::Object(ref map) => map
            .get("rows")
            .cloned()
            .ok_or_else(|| "peer response missing `rows`".to_string())?,
        _ => return Err("peer response is neither array nor object".to_string()),
    };
    serde_json::from_value::<Vec<FedRow>>(rows_val)
        .map_err(|e| format!("peer rows not FedRow-shaped: {e}"))
}

/// Fan the query out to every peer concurrently and collect the per-peer outcomes
/// (CONCEPT:EG-243). Each peer runs in its own `spawn_blocking` task; a panic or join
/// error degrades that peer rather than the whole query.
pub async fn fetch_peers(
    registry: &PeerRegistry,
    allow: &PeerAllowlist,
    query: &str,
    lang: &str,
) -> Vec<PeerOutcome> {
    let mut handles = Vec::new();
    for peer in registry.peers() {
        let peer = peer.clone();
        let allow = allow.clone();
        let query = query.to_string();
        let lang = lang.to_string();
        handles.push(tokio::task::spawn_blocking(move || {
            let rows = fetch_one_peer(&peer, &allow, &query, &lang);
            PeerOutcome { source: peer, rows }
        }));
    }
    let mut outcomes = Vec::with_capacity(handles.len());
    for h in handles {
        match h.await {
            Ok(outcome) => outcomes.push(outcome),
            Err(e) => outcomes.push(PeerOutcome {
                source: "<unknown-peer>".to_string(),
                rows: Err(format!("peer task join error: {e}")),
            }),
        }
    }
    outcomes
}

// ── Local execution ───────────────────────────────────────────────────────

/// Run the query against the LOCAL store via in-process dispatch (CONCEPT:EG-243).
/// Routes by `lang`: `sparql`/`sql` use their native read method when that feature is
/// compiled; everything else (default `nl`/`search`) goes through the NL/search path.
/// The result is normalized to [`FedRow`]s so it merges uniformly with peer rows.
async fn local_execute(
    state: &Arc<RwLock<ServerState>>,
    query: &str,
    lang: &str,
    graph: &str,
) -> Result<Vec<FedRow>, String> {
    let method = build_local_method(query, lang, graph);
    let secret = { state.read().await.auth_secret.clone() };
    let id = 1u64;
    let request = crate::protocol::Request {
        id,
        graph: graph.to_string(),
        auth_token: crate::server::compute_auth_token(&secret, id),
        agent_id: None,
        method,
    };
    let resp = crate::server::dispatch(state, request).await;
    if let Some(err) = resp.error {
        return Err(err);
    }
    let bytes = match resp.result {
        Some(crate::protocol::ResultPayload::Raw(b)) => b,
        _ => return Ok(Vec::new()),
    };
    Ok(decode_local_rows(&bytes, lang))
}

/// Build the in-process read [`Method`] for a `lang` (CONCEPT:EG-243). SQL/SPARQL arms
/// only compile when their feature is present; otherwise every lang uses the NL/search
/// path so a slim build still federates over the search surface.
fn build_local_method(query: &str, lang: &str, graph: &str) -> crate::protocol::Method {
    match lang {
        #[cfg(feature = "sparql")]
        "sparql" => crate::protocol::Method::Sparql {
            query: query.to_string(),
            base_iri: String::new(),
            type_convention: String::new(),
        },
        #[cfg(feature = "query")]
        "sql" => crate::protocol::Method::Sql {
            query: query.to_string(),
            params_msgpack: Vec::new(),
        },
        _ => crate::protocol::Method::NlQuery {
            text: query.to_string(),
            graph: graph.to_string(),
        },
    }
}

/// Decode the local `ResultPayload::Raw` into normalized rows (CONCEPT:EG-243). The
/// ranked-search path is `[(id, score)]`; SPARQL/SQL rows are flattened to a stable key
/// + a JSON payload. Unrecognized shapes yield no local rows (peer rows still merge).
fn decode_local_rows(bytes: &[u8], lang: &str) -> Vec<FedRow> {
    #[cfg(feature = "sparql")]
    if lang == "sparql" {
        if let Ok(res) = rmp_serde::from_slice::<crate::protocol::SparqlResult>(bytes) {
            return res
                .rows
                .into_iter()
                .map(|cells| {
                    let key = cells
                        .iter()
                        .map(|c| c.clone().unwrap_or_default())
                        .collect::<Vec<_>>()
                        .join("\u{1}");
                    let data = serde_json::json!({
                        "vars": res.vars,
                        "cells": cells,
                    });
                    FedRow {
                        key,
                        score: None,
                        data,
                    }
                })
                .collect();
        }
        return Vec::new();
    }
    #[cfg(feature = "query")]
    if lang == "sql" {
        if let Ok(res) = rmp_serde::from_slice::<crate::protocol::QueryResult>(bytes) {
            return res
                .rows
                .into_iter()
                .map(|blob| {
                    let cells: Vec<serde_json::Value> =
                        rmp_serde::from_slice(&blob).unwrap_or_default();
                    let key = cells
                        .iter()
                        .map(|c| c.to_string())
                        .collect::<Vec<_>>()
                        .join("\u{1}");
                    let data = serde_json::json!({
                        "columns": res.columns,
                        "cells": cells,
                    });
                    FedRow {
                        key,
                        score: None,
                        data,
                    }
                })
                .collect();
        }
        return Vec::new();
    }
    let _ = lang;
    // Default ranked-search path: `[(id, score)]` rows.
    match rmp_serde::from_slice::<Vec<(String, Option<f32>)>>(bytes) {
        Ok(rows) => rows
            .into_iter()
            .map(|(id, score)| FedRow {
                key: id.clone(),
                score: score.map(|s| s as f64),
                data: serde_json::json!({ "id": id, "score": score }),
            })
            .collect(),
        Err(_) => Vec::new(),
    }
}

/// The full federated read (CONCEPT:EG-243): run locally AND fan out to peers, then merge.
/// `local_only` short-circuits the fan-out (used when a super-cluster peer calls us with
/// `?local=1`, preventing recursion). A local-store error degrades to a partial answer
/// rather than failing the request.
pub async fn run_federated(
    state: &Arc<RwLock<ServerState>>,
    query: &str,
    lang: &str,
    local_only: bool,
) -> FederatedResponse {
    let graph = std::env::var(DEFAULT_GRAPH_ENV).unwrap_or_else(|_| "__commons__".to_string());
    let local = local_execute(state, query, lang, &graph).await;
    let mut outcomes = vec![PeerOutcome {
        source: "<local>".to_string(),
        rows: local,
    }];
    if !local_only {
        let registry = PeerRegistry::from_env();
        if !registry.is_empty() {
            let allow = PeerAllowlist::from_env();
            let mut peer_outcomes = fetch_peers(&registry, &allow, query, lang).await;
            outcomes.append(&mut peer_outcomes);
        }
    }
    merge_partials(outcomes)
}

// ── HTTP surface (`/federated`) ───────────────────────────────────────────

/// Serve the `/federated` POST endpoint (CONCEPT:EG-243) using the same hand-rolled
/// HTTP framing idiom as `sparql_http` (no new HTTP dep). Body: `{query, lang}`; a
/// `?local=1` query param short-circuits the fan-out (peer-to-peer, no recursion).
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
                    "application/json",
                    r#"{"error":"malformed HTTP request"}"#.to_string(),
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

/// A parsed HTTP request for the federated surface.
struct HttpRequest {
    method: String,
    target: String,
    body: String,
}

/// Read one HTTP/1.1 request (headers to the blank line, then `Content-Length` body).
async fn read_request(stream: &mut tokio::net::TcpStream) -> Option<HttpRequest> {
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
    for line in lines {
        if let Some((k, v)) = line.split_once(':') {
            if k.trim().eq_ignore_ascii_case("content-length") {
                content_length = v.trim().parse().unwrap_or(0);
            }
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
    Some(HttpRequest {
        method,
        target,
        body: String::from_utf8_lossy(&body).to_string(),
    })
}

/// Route + execute a `/federated` request (CONCEPT:EG-243).
async fn handle(
    state: &Arc<RwLock<ServerState>>,
    req: HttpRequest,
) -> (&'static str, &'static str, String) {
    let (path, query_string) = match req.target.split_once('?') {
        Some((p, q)) => (p, q),
        None => (req.target.as_str(), ""),
    };
    if req.method == "OPTIONS" && path == "/federated" {
        return ("204 No Content", "application/json", String::new());
    }
    if path != "/federated" {
        return (
            "404 Not Found",
            "application/json",
            r#"{"error":"not found"}"#.to_string(),
        );
    }
    if req.method != "POST" {
        return (
            "405 Method Not Allowed",
            "application/json",
            r#"{"error":"POST a JSON body {\"query\":\"…\",\"lang\":\"…\"} to /federated"}"#
                .to_string(),
        );
    }
    let local_only = query_string
        .split('&')
        .any(|kv| matches!(kv, "local=1" | "local=true"));
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
    let query = body
        .get("query")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    if query.trim().is_empty() {
        return (
            "400 Bad Request",
            "application/json",
            r#"{"error":"missing non-empty 'query'"}"#.to_string(),
        );
    }
    let lang = body
        .get("lang")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .unwrap_or("nl")
        .to_string();

    let merged = run_federated(state, &query, &lang, local_only).await;
    let out = serde_json::json!({
        "rows": merged.rows,
        "metadata": {
            "peers_queried": merged.metadata.peers_queried,
            "failed_peers": merged.metadata.failed_peers,
            "partial": merged.metadata.partial,
            "contributing_sources": merged.metadata.contributing_sources,
        },
    });
    ("200 OK", "application/json", out.to_string())
}

/// Locate a byte subslice (header-terminator scan).
fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    if needle.is_empty() || haystack.len() < needle.len() {
        return None;
    }
    haystack.windows(needle.len()).position(|w| w == needle)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(key: &str, score: Option<f64>) -> FedRow {
        FedRow {
            key: key.to_string(),
            score,
            data: serde_json::json!({ "k": key }),
        }
    }

    #[test]
    fn eg243_peer_registry_parse_dedups_and_trims() {
        let reg = PeerRegistry::parse(
            " https://a.example:7900 , https://b.example/ ,, https://a.example:7900/ ",
        );
        assert_eq!(
            reg.peers(),
            &[
                "https://a.example:7900".to_string(),
                "https://b.example".to_string()
            ]
        );
        assert!(!reg.is_empty());
    }

    #[test]
    fn eg243_peer_registry_register_is_idempotent() {
        let mut reg = PeerRegistry::new();
        assert!(reg.register("https://x.example:7900/"));
        assert!(!reg.register("https://x.example:7900")); // already present (slash-normalized)
        assert!(!reg.register("   ")); // blank
        assert_eq!(reg.peers().len(), 1);
    }

    #[test]
    fn eg243_ssrf_allowlist_rejects_loopback_by_default() {
        let allow = PeerAllowlist::default();
        // Loopback host + literal must be refused with an empty allowlist.
        assert!(allow.check("http://127.0.0.1:7900").is_err());
        assert!(allow.check("http://localhost:7900").is_err());
        // A non-http scheme is refused outright.
        assert!(allow.check("ftp://example.com").is_err());
    }

    #[test]
    fn eg243_ssrf_allowlist_permits_explicitly_allowed_internal_peer() {
        let allow = PeerAllowlist::from_list(&["127.0.0.1", "localhost"]);
        assert!(allow.check("http://127.0.0.1:7900").is_ok());
        assert!(allow.check("http://localhost:7900/federated").is_ok());
    }

    #[test]
    fn eg243_merge_unions_and_dedups_by_key() {
        // No scores anywhere ⇒ union preserving first-seen order, deduped by key.
        let local = PeerOutcome {
            source: "<local>".to_string(),
            rows: Ok(vec![row("a", None), row("b", None)]),
        };
        let peer = PeerOutcome {
            source: "https://p.example".to_string(),
            rows: Ok(vec![row("b", None), row("c", None)]),
        };
        let merged = merge_partials(vec![local, peer]);
        let keys: Vec<&str> = merged.rows.iter().map(|r| r.key.as_str()).collect();
        assert_eq!(keys, vec!["a", "b", "c"]);
        assert!(!merged.metadata.partial);
        assert_eq!(merged.metadata.peers_queried, vec!["https://p.example"]);
        assert!(merged.metadata.failed_peers.is_empty());
        assert_eq!(merged.metadata.contributing_sources, 2);
    }

    #[test]
    fn eg243_merge_reranks_ranked_results_by_rrf() {
        // "b" is ranked highly by BOTH sources ⇒ RRF should lift it to the top.
        let local = PeerOutcome {
            source: "<local>".to_string(),
            rows: Ok(vec![row("a", Some(9.0)), row("b", Some(8.0))]),
        };
        let peer = PeerOutcome {
            source: "https://p.example".to_string(),
            rows: Ok(vec![row("b", Some(5.0)), row("c", Some(4.0))]),
        };
        let merged = merge_partials(vec![local, peer]);
        assert_eq!(
            merged.rows[0].key, "b",
            "b appears in both lists ⇒ top by RRF"
        );
        // Fused score is stamped onto the rows.
        assert!(merged.rows[0].score.unwrap() > merged.rows[1].score.unwrap());
        assert!(!merged.metadata.partial);
    }

    #[test]
    fn eg243_failed_peer_degrades_to_partial_not_error() {
        let local = PeerOutcome {
            source: "<local>".to_string(),
            rows: Ok(vec![row("a", None)]),
        };
        let dead = PeerOutcome {
            source: "https://dead.example".to_string(),
            rows: Err("connect timeout".to_string()),
        };
        let merged = merge_partials(vec![local, dead]);
        // The query still succeeds with the local rows...
        assert_eq!(merged.rows.len(), 1);
        assert_eq!(merged.rows[0].key, "a");
        // ...but is flagged partial with the dead peer named.
        assert!(merged.metadata.partial);
        assert_eq!(merged.metadata.failed_peers, vec!["https://dead.example"]);
        assert_eq!(merged.metadata.peers_queried, vec!["https://dead.example"]);
        assert_eq!(merged.metadata.contributing_sources, 1);
    }

    #[test]
    fn eg243_parse_peer_rows_accepts_envelope_and_bare_array() {
        let env = r#"{"rows":[{"key":"a","score":1.5,"data":{}}],"metadata":{}}"#;
        let bare = r#"[{"key":"a","score":1.5,"data":{}}]"#;
        assert_eq!(parse_peer_rows(env).unwrap().len(), 1);
        assert_eq!(parse_peer_rows(bare).unwrap().len(), 1);
        assert!(parse_peer_rows("not json").is_err());
        assert!(parse_peer_rows(r#"{"nope":1}"#).is_err());
    }

    #[test]
    fn eg243_decode_local_rows_search_shape() {
        let rows = vec![("n1".to_string(), Some(0.9f32)), ("n2".to_string(), None)];
        let bytes = rmp_serde::to_vec_named(&rows).unwrap();
        let decoded = decode_local_rows(&bytes, "nl");
        assert_eq!(decoded.len(), 2);
        assert_eq!(decoded[0].key, "n1");
        assert_eq!(decoded[0].score, Some(0.9f32 as f64));
        assert_eq!(decoded[1].score, None);
    }
}
