//! CA-14 — OpenSearch federation-search adapter (CONCEPT:EG-KG.query.federation-opensearch-adapter,
//! feature `federation-opensearch`, off by default; implies `federation-search`).
//!
//! Adds OpenSearch as an additional, non-peer source in the SAME `/federated` fan-out
//! [`super`] runs against peer eg engines (`DEC-CA-09`: "OpenSearch is the search tier").
//! Lexical/full-text only — vector/semantic search stays on eg-vector/vector-mcp
//! (`INTERCONNECT-GAPS.md` E2, explicit non-goal here). OpenSearch is READ-ONLY from
//! this adapter's perspective: it is a derived, rebuildable index (`DEC-CA-01`) fed by
//! CA-24's CDC indexer; this module never writes to it.
//!
//! ## Hook point (why `federation/mod.rs` has a two-function split)
//!
//! `run_federated` used to build its `Vec<PeerOutcome>` (local + peers) inline and merge
//! it in the same function, so there was no seam to append a THIRD kind of source
//! without either editing `run_federated` itself or forking a second merge path — both
//! explicitly out of scope for this lane. The only change made to `federation/mod.rs`
//! is extracting that outcome-collection step into [`super::collect_outcomes`]; the
//! merge call, `run_federated`'s signature, and its return value are byte-identical to
//! before the split (see that function's doc comment). [`run_federated_with_opensearch`]
//! below is this lane's actual hook: it calls `collect_outcomes` for the local/peer
//! legs exactly as `run_federated` does, appends one [`super::PeerOutcome`] per
//! configured OpenSearch index (source label `"opensearch:<index>"`), and then runs the
//! SAME [`super::merge_partials`]/[`super::merge_partials_typed`] pipeline — so RRF
//! fusion, dedup, and the `partial`/`failed_peers` degrade contract apply uniformly
//! across eg-native, peer, and OpenSearch rows.
//!
//! ## Query DSL subset (`DEC-CA-09`)
//!
//! Only `match` / `term` / `bool` (with `bool`'s `must`/`should`/`must_not`/`filter`
//! clause lists) are accepted — no aggregation, scripting, or other OpenSearch-specific
//! extension the eg fan-out can't express uniformly across its other federated sources.
//! [`validate_dsl_subset`] enforces this before a query ever leaves the process.
//!
//! ## Identity propagation
//!
//! The caller's bearer, when present, is forwarded UNMODIFIED as `Authorization: Bearer
//! <token>` so OpenSearch's own DLS (fed by CA-24/CA-26's Marking sync, `DEC-CA-04`)
//! enforces on the caller's REAL principal — never silently downgraded to a
//! service-account credential. This module never invents, caches, or substitutes a
//! credential of its own; a caller with no bearer gets whatever an anonymous request
//! yields under OpenSearch's own configured auth mode. Full end-to-end DLS proof (a
//! Marking-hidden document actually absent from OpenSearch's response) is CA-24/26/50/
//! 60's joint proof once a live target exists (`DEC-CA-09` P8 leg); this lane proves the
//! passthrough mechanism only — the header is on the wire, unmodified, every time.
//!
//! ## Failure semantics
//!
//! A slow/dead OpenSearch target degrades EXACTLY like a dead peer: its
//! [`super::PeerOutcome`] carries `rows: Err(reason)`, which flows into
//! `merge_partials`'s existing `failed_peers` + `partial: true` handling — never a hard
//! failure of the whole federated query, and never silently indistinguishable from a
//! genuinely empty index (an error is an `Err`, not an `Ok(vec![])`).
//!
//! ## Resource budget
//!
//! Mirrors [`super`]'s per-peer connect/read timeout + response-size-cap constants
//! (`CONNECT_TIMEOUT_SECS`=5s, `READ_TIMEOUT_SECS`=20s, `MAX_RESPONSE_BYTES`=64 MiB) — no
//! OpenSearch-specific values; RRF fusion cost is unchanged (`RRF_K`=60.0, `super`'s
//! constant, reused as-is). No new algorithmic complexity class.

use std::io::Read;
use std::sync::Arc;
use std::time::Duration;

use serde_json::Value;
use tokio::sync::RwLock;

use crate::server::ServerState;

use super::{FedRow, FederatedResponse, PeerOutcome};

/// OpenSearch base URL for the federation adapter (CA-14, `DEC-CA-09`), e.g.
/// `https://opensearch.svc:9200`. Unset (the default) ⇒ [`query_opensearch`] always
/// returns `Err` and [`run_federated_with_opensearch`] with an empty `indices` list
/// never contacts OpenSearch at all — existing peer-only federation behavior is
/// unchanged, which is this lane's rollback path.
pub const OPENSEARCH_URL_ENV: &str = "EPISTEMIC_GRAPH_FEDERATION_OPENSEARCH_URL";

/// The accepted top-level OpenSearch query-DSL clause kinds (`DEC-CA-09`).
const ALLOWED_QUERY_KEYS: &[&str] = &["match", "term", "bool"];
/// The accepted `bool` clause-list keys.
const ALLOWED_BOOL_CLAUSES: &[&str] = &["must", "should", "must_not", "filter"];

// ── Query DSL subset validation ───────────────────────────────────────────

/// Validate a query DSL value is within the `DEC-CA-09` accepted subset: exactly one
/// top-level `match` / `term` / `bool` clause, recursing into `bool`'s
/// `must`/`should`/`must_not`/`filter` clause lists (each entry validated the same way).
/// Anything else — aggregations, scripting, `function_score`, raw Query DSL escape
/// hatches — is rejected before a request ever leaves the process (lane budget: "DSL
/// subset kept small deliberately … no aggregation/scripting surface").
pub fn validate_dsl_subset(query: &Value) -> Result<(), String> {
    let obj = query
        .as_object()
        .ok_or_else(|| "query DSL must be a JSON object".to_string())?;
    if obj.len() != 1 {
        return Err(format!(
            "query DSL must have exactly one top-level clause, got {}",
            obj.len()
        ));
    }
    let (key, val) = obj.iter().next().expect("len checked == 1 above");
    if !ALLOWED_QUERY_KEYS.contains(&key.as_str()) {
        return Err(format!(
            "query clause '{key}' is outside the accepted subset (match/term/bool)"
        ));
    }
    if key == "bool" {
        let bool_obj = val
            .as_object()
            .ok_or_else(|| "'bool' clause must be a JSON object".to_string())?;
        for (clause_key, clause_val) in bool_obj {
            if !ALLOWED_BOOL_CLAUSES.contains(&clause_key.as_str()) {
                return Err(format!(
                    "bool clause '{clause_key}' is outside the accepted subset (must/should/must_not/filter)"
                ));
            }
            let items: Vec<&Value> = match clause_val {
                Value::Array(arr) => arr.iter().collect(),
                other => vec![other],
            };
            for item in items {
                validate_dsl_subset(item)?;
            }
        }
    }
    Ok(())
}

/// Build the `_search` request body — `{"query": <dsl>}` — after validating `dsl`
/// against [`validate_dsl_subset`]. Split out from [`query_opensearch`] so the request
/// SHAPE is independently unit-testable without a network round trip.
pub fn build_search_body(query_dsl: &Value) -> Result<String, String> {
    validate_dsl_subset(query_dsl)?;
    Ok(serde_json::json!({ "query": query_dsl }).to_string())
}

// ── OpenSearch client (ureq, blocking) ────────────────────────────────────

/// Query one OpenSearch index's `_search` endpoint and map its hits into [`FedRow`]s
/// (CONCEPT:EG-KG.query.federation-opensearch-adapter, `DEC-CA-09`). Blocking (`ureq`) — call from `spawn_blocking`, mirroring
/// [`super::fetch_one_peer`]'s pattern, never from an async context directly. Reuses
/// `super`'s connect/read timeout + response-size-cap constants; the caller's `bearer`,
/// when `Some` and non-blank, is forwarded unmodified as `Authorization: Bearer <token>`
/// (see module docs — identity propagation).
pub fn query_opensearch(
    index: &str,
    query_dsl: &Value,
    bearer: Option<&str>,
) -> Result<Vec<FedRow>, String> {
    let base = std::env::var(OPENSEARCH_URL_ENV)
        .map_err(|_| format!("{OPENSEARCH_URL_ENV} is not set"))?;
    let base = base.trim().trim_end_matches('/');
    if base.is_empty() {
        return Err(format!("{OPENSEARCH_URL_ENV} is empty"));
    }
    if !base.starts_with("http://") && !base.starts_with("https://") {
        return Err(format!("{OPENSEARCH_URL_ENV} must be http(s): '{base}'"));
    }
    if index.is_empty() || index.contains(['/', '?', '#']) {
        return Err(format!("invalid OpenSearch index name: '{index}'"));
    }
    let body = build_search_body(query_dsl)?;
    let endpoint = format!("{base}/{index}/_search");

    let agent = ureq::AgentBuilder::new()
        .timeout_connect(Duration::from_secs(super::CONNECT_TIMEOUT_SECS))
        .timeout_read(Duration::from_secs(super::READ_TIMEOUT_SECS))
        .build();
    let mut req = agent
        .post(&endpoint)
        .set("Content-Type", "application/json")
        .set("Accept", "application/json");
    if let Some(tok) = bearer {
        if !tok.trim().is_empty() {
            req = req.set("Authorization", &format!("Bearer {tok}"));
        }
    }
    let resp = req
        .send_string(&body)
        .map_err(|e| format!("POST {endpoint} failed: {e}"))?;
    let mut text = String::new();
    resp.into_reader()
        .take(super::MAX_RESPONSE_BYTES)
        .read_to_string(&mut text)
        .map_err(|e| format!("reading {endpoint} response: {e}"))?;
    parse_hits(&text, index)
}

/// Map an OpenSearch `_search` response body into [`FedRow`]s. Preserves index, doc id,
/// and score as provenance in `data` (this lane's contract: `FedRow{key: doc_id, score:
/// Some(_score), data: {"index", "doc_id", "source"}}`) so a fused row is always
/// traceable back to its exact OpenSearch origin.
fn parse_hits(text: &str, index: &str) -> Result<Vec<FedRow>, String> {
    let val: Value =
        serde_json::from_str(text).map_err(|e| format!("invalid OpenSearch response JSON: {e}"))?;
    if let Some(err) = val.get("error") {
        return Err(format!("OpenSearch returned an error: {err}"));
    }
    let hits = val
        .get("hits")
        .and_then(|h| h.get("hits"))
        .and_then(|h| h.as_array())
        .ok_or_else(|| "OpenSearch response missing hits.hits array".to_string())?;
    let mut rows = Vec::with_capacity(hits.len());
    for hit in hits {
        let doc_id = hit
            .get("_id")
            .and_then(|v| v.as_str())
            .ok_or_else(|| "OpenSearch hit missing '_id'".to_string())?
            .to_string();
        let hit_index = hit
            .get("_index")
            .and_then(|v| v.as_str())
            .unwrap_or(index)
            .to_string();
        let score = hit.get("_score").and_then(|v| v.as_f64());
        let source = hit.get("_source").cloned().unwrap_or(Value::Null);
        let data = serde_json::json!({
            "index": hit_index,
            "doc_id": doc_id,
            "source": source,
        });
        rows.push(FedRow {
            key: doc_id,
            score,
            data,
        });
    }
    Ok(rows)
}

// ── Hook into the federated-search fan-out ────────────────────────────────

/// The full federated read PLUS an OpenSearch leg (CONCEPT:EG-KG.query.federation-opensearch-adapter,
/// CA-14's actual hook point — see
/// module docs). Collects the SAME local + peer outcomes [`super::run_federated`] does
/// (via [`super::collect_outcomes`]), then queries each of `indices` on its own
/// `spawn_blocking` task (so one slow OpenSearch index cannot block the others or the
/// peer legs), appending a `PeerOutcome{source: "opensearch:<index>", rows}` per index.
/// All outcomes then flow through the SAME merge pipeline `run_federated` uses. An empty
/// `indices` list makes this byte-identical to `run_federated` (no OpenSearch leg is
/// even attempted) — the rollback path this lane's migration note describes.
pub async fn run_federated_with_opensearch(
    state: &Arc<RwLock<ServerState>>,
    query: &str,
    lang: &str,
    local_only: bool,
    query_dsl: &Value,
    indices: &[String],
    bearer: Option<&str>,
) -> FederatedResponse {
    let mut outcomes = super::collect_outcomes(state, query, lang, local_only).await;
    if !indices.is_empty() {
        let mut handles = Vec::with_capacity(indices.len());
        for index in indices {
            let index = index.clone();
            let dsl = query_dsl.clone();
            let bearer = bearer.map(|s| s.to_string());
            handles.push(tokio::task::spawn_blocking(move || {
                let rows = query_opensearch(&index, &dsl, bearer.as_deref());
                PeerOutcome {
                    source: format!("opensearch:{index}"),
                    rows,
                }
            }));
        }
        for h in handles {
            match h.await {
                Ok(outcome) => outcomes.push(outcome),
                Err(e) => outcomes.push(PeerOutcome {
                    source: "opensearch:<unknown-index>".to_string(),
                    rows: Err(format!("opensearch task join error: {e}")),
                }),
            }
        }
    }
    if super::is_typed_lang(lang) {
        super::merge_partials_typed(outcomes, lang)
    } else {
        super::merge_partials(outcomes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use std::net::TcpListener;
    use std::sync::Mutex;

    /// GOC-70 rule 2 (`AGENTS.md`): every test below that reads or mutates the
    /// process-global [`OPENSEARCH_URL_ENV`] holds this lock for its ENTIRE body, so
    /// `cargo test`'s multi-threaded-in-one-process execution can never interleave two
    /// tests' `set_var`/`remove_var` calls on the SAME var. No other module in this
    /// crate touches `OPENSEARCH_URL_ENV` (verified: it is declared and read only in
    /// this file), so a lock local to this test module — rather than the crate-wide
    /// `crypto::TEST_ENV_LOCK` — is the correct, minimal scope.
    static ENV_LOCK: Mutex<()> = Mutex::new(());

    /// Acquire [`ENV_LOCK`], recovering from a poison (a prior test panicking while
    /// holding it must not permanently wedge every later test).
    fn lock_env() -> std::sync::MutexGuard<'static, ()> {
        ENV_LOCK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    // ── DSL subset validation ───────────────────────────────────────────

    #[test]
    fn ca14_dsl_subset_accepts_match_term_bool() {
        assert!(validate_dsl_subset(&serde_json::json!({"match": {"content": "hello"}})).is_ok());
        assert!(validate_dsl_subset(&serde_json::json!({"term": {"node_type": "Agent"}})).is_ok());
        assert!(validate_dsl_subset(&serde_json::json!({
            "bool": {
                "must": [{"match": {"content": "hello"}}],
                "filter": [{"term": {"tenant": "acme"}}],
                "should": {"match": {"content": "world"}},
                "must_not": [{"term": {"marking": "restricted"}}]
            }
        }))
        .is_ok());
    }

    #[test]
    fn ca14_dsl_subset_rejects_scripting_and_aggregations() {
        assert!(validate_dsl_subset(&serde_json::json!({"script_score": {}})).is_err());
        assert!(validate_dsl_subset(&serde_json::json!({"function_score": {}})).is_err());
        assert!(validate_dsl_subset(&serde_json::json!({"match_all": {}})).is_err());
        // Two top-level clauses at once is rejected even if both are individually allowed.
        assert!(validate_dsl_subset(&serde_json::json!({
            "match": {"content": "x"}, "term": {"tenant": "y"}
        }))
        .is_err());
    }

    #[test]
    fn ca14_dsl_subset_rejects_disallowed_bool_clause_and_recurses() {
        // "boost" is not an accepted bool clause key in this lane's minimal subset.
        assert!(validate_dsl_subset(&serde_json::json!({
            "bool": {"must": [{"match": {"content": "x"}}], "boost": 2.0}
        }))
        .is_err());
        // A disallowed clause nested INSIDE an allowed bool clause list is also caught.
        assert!(validate_dsl_subset(&serde_json::json!({
            "bool": {"must": [{"script_score": {}}]}
        }))
        .is_err());
    }

    #[test]
    fn ca14_build_search_body_wraps_validated_dsl() {
        let dsl = serde_json::json!({"match": {"content": "hello"}});
        let body = build_search_body(&dsl).expect("valid dsl builds a body");
        let parsed: Value = serde_json::from_str(&body).unwrap();
        assert_eq!(parsed["query"], dsl);

        let bad = serde_json::json!({"script_score": {}});
        assert!(build_search_body(&bad).is_err());
    }

    // ── Hit mapping ──────────────────────────────────────────────────────

    #[test]
    fn ca14_parse_hits_preserves_index_score_doc_id() {
        let body = serde_json::json!({
            "hits": {
                "total": {"value": 2},
                "hits": [
                    {"_index": "kg-acme-agent", "_id": "n1", "_score": 1.5,
                     "_source": {"content": "hello"}},
                    {"_index": "kg-acme-agent", "_id": "n2", "_score": 0.7,
                     "_source": {"content": "world"}}
                ]
            }
        })
        .to_string();
        let rows = parse_hits(&body, "kg-acme-agent").expect("parses");
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].key, "n1");
        assert_eq!(rows[0].score, Some(1.5));
        assert_eq!(rows[0].data["index"], "kg-acme-agent");
        assert_eq!(rows[0].data["doc_id"], "n1");
        assert_eq!(rows[0].data["source"]["content"], "hello");
        assert_eq!(rows[1].key, "n2");
        assert_eq!(rows[1].score, Some(0.7));
    }

    #[test]
    fn ca14_parse_hits_rejects_error_response() {
        let body = serde_json::json!({"error": {"type": "index_not_found_exception"}}).to_string();
        let err = parse_hits(&body, "kg-missing").unwrap_err();
        assert!(err.contains("OpenSearch returned an error"));
    }

    #[test]
    fn ca14_parse_hits_rejects_malformed_response() {
        assert!(parse_hits("not json", "kg-x").is_err());
        assert!(parse_hits(r#"{"no_hits_field": true}"#, "kg-x").is_err());
    }

    // ── query_opensearch: env / URL / index validation ──────────────────

    #[test]
    fn ca14_query_opensearch_errs_when_url_unset() {
        let _guard = lock_env();
        // SAFETY: single-threaded env mutation guarded by not running in parallel with
        // another test that sets/reads the SAME var — this var is private to this test
        // module (no other test file touches OPENSEARCH_URL_ENV).
        std::env::remove_var(OPENSEARCH_URL_ENV);
        let err = query_opensearch(
            "kg-acme-agent",
            &serde_json::json!({"match": {"a": "b"}}),
            None,
        )
        .unwrap_err();
        assert!(err.contains(OPENSEARCH_URL_ENV));
    }

    #[test]
    fn ca14_query_opensearch_rejects_invalid_index_name() {
        let _guard = lock_env();
        std::env::set_var(OPENSEARCH_URL_ENV, "http://127.0.0.1:1");
        let err = query_opensearch("bad/index", &serde_json::json!({"match": {"a": "b"}}), None)
            .unwrap_err();
        assert!(err.contains("invalid OpenSearch index name"));
        std::env::remove_var(OPENSEARCH_URL_ENV);
    }

    /// Known-bad: OpenSearch unreachable (connection refused on a closed local port)
    /// degrades to `Err`, never a panic and never a silent `Ok(vec![])` indistinguishable
    /// from a genuinely empty index (acceptance gate 3's "down" case; the shared
    /// connect/read timeout constants cover the "slow" case identically to a dead peer,
    /// unexercised here to keep this test fast/deterministic).
    #[test]
    fn ca14_query_opensearch_degrades_on_unreachable_target() {
        let _guard = lock_env();
        // Bind and immediately drop a listener to obtain a port nothing is listening on.
        let probe = TcpListener::bind("127.0.0.1:0").expect("bind probe");
        let port = probe.local_addr().unwrap().port();
        drop(probe);
        std::env::set_var(OPENSEARCH_URL_ENV, format!("http://127.0.0.1:{port}"));
        let result = query_opensearch(
            "kg-acme-agent",
            &serde_json::json!({"match": {"a": "b"}}),
            None,
        );
        std::env::remove_var(OPENSEARCH_URL_ENV);
        assert!(
            result.is_err(),
            "unreachable target must degrade, not panic or succeed"
        );
    }

    /// Security: the caller's bearer reaches OpenSearch UNMODIFIED as `Authorization:
    /// Bearer <token>` (identity-propagation invariant) — proven against a real (mock)
    /// HTTP server capturing the raw request, not just by inspecting request-building
    /// code.
    #[test]
    fn ca14_query_opensearch_forwards_bearer_unmodified() {
        let _guard = lock_env();
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind mock opensearch");
        let addr = listener.local_addr().unwrap();
        let handle = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("accept");
            let mut buf = [0u8; 4096];
            let n = stream.read(&mut buf).expect("read request");
            let request_text = String::from_utf8_lossy(&buf[..n]).to_string();
            let body = serde_json::json!({"hits": {"hits": []}}).to_string();
            let resp = format!(
                "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
                body.len(),
                body
            );
            stream.write_all(resp.as_bytes()).expect("write response");
            request_text
        });
        std::env::set_var(OPENSEARCH_URL_ENV, format!("http://{addr}"));
        let result = query_opensearch(
            "kg-acme-agent",
            &serde_json::json!({"match": {"content": "hello"}}),
            Some("caller-real-token-123"),
        );
        std::env::remove_var(OPENSEARCH_URL_ENV);
        let request_text = handle.join().expect("mock server thread");
        assert!(
            result.is_ok(),
            "mock server response must parse: {result:?}"
        );
        assert!(
            request_text.contains("authorization: bearer caller-real-token-123")
                || request_text.contains("Authorization: Bearer caller-real-token-123"),
            "bearer must be forwarded unmodified, got headers:\n{request_text}"
        );
    }

    /// Negative: no bearer supplied ⇒ no `Authorization` header is invented.
    #[test]
    fn ca14_query_opensearch_omits_auth_header_when_no_bearer() {
        let _guard = lock_env();
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind mock opensearch");
        let addr = listener.local_addr().unwrap();
        let handle = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("accept");
            let mut buf = [0u8; 4096];
            let n = stream.read(&mut buf).expect("read request");
            let request_text = String::from_utf8_lossy(&buf[..n]).to_string();
            let body = serde_json::json!({"hits": {"hits": []}}).to_string();
            let resp = format!(
                "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
                body.len(),
                body
            );
            stream.write_all(resp.as_bytes()).expect("write response");
            request_text
        });
        std::env::set_var(OPENSEARCH_URL_ENV, format!("http://{addr}"));
        let result = query_opensearch(
            "kg-acme-agent",
            &serde_json::json!({"match": {"a": "b"}}),
            None,
        );
        std::env::remove_var(OPENSEARCH_URL_ENV);
        let request_text = handle.join().expect("mock server thread");
        assert!(result.is_ok());
        let lower = request_text.to_ascii_lowercase();
        assert!(
            !lower.contains("authorization:"),
            "no bearer supplied must not invent an Authorization header, got:\n{request_text}"
        );
    }
}
