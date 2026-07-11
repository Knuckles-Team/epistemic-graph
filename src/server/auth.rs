//! Request authentication: two envelope generations coexist.
//!
//!  * v0 (legacy) — `compute_auth_token`/`verify_auth`: HMAC-SHA256 over the
//!    request id ONLY. No timestamp, no nonce, no body binding. Kept
//!    byte-for-byte compatible — every existing client/test in the tree
//!    speaks this, and it remains the DEFAULT (the v1 secure mode is opt-in;
//!    see [`require_signed_envelope`]).
//!  * v1 (CONCEPT:EG-KG.security.signed-request-envelope, EG-P0-5) —
//!    `compute_envelope_token`/`verify_request`: a versioned signed envelope
//!    binding protocol version, audience, tenant, principal, graph, method
//!    name, a hash of the method's params (the "body"), a timestamp, a nonce,
//!    and an idempotency key, all under ONE HMAC-SHA256 — verified in
//!    CONSTANT TIME (`Mac::verify_slice`, never `==`), with a clock-skew
//!    window and a bounded replay-nonce cache.
//!
//!    The v1 envelope rides in the EXISTING `auth_token` wire field (prefixed
//!    `eg1.`) rather than as new `Request` struct fields:
//!    `eg_types::protocol::Request` is constructed via ~100 Rust
//!    struct-literal call sites across the server, raft, eg-graphql, eg-rdf,
//!    and integration-test crates. Adding required fields there would force
//!    edits far outside this workstream's audited scope (`eg-types` + this
//!    server crate). Packing the versioned envelope into the token string is
//!    additive at the TYPE level (an untouched `String` field, so all ~100
//!    call sites compile unchanged) while still explicit and versioned at the
//!    VALUE level (the fixed `eg1.` prefix unambiguously distinguishes it from
//!    a legacy plain hex digest — a v0 request is never silently mis-handled
//!    as v1 or vice versa). The canonical byte layout that gets HMAC'd lives
//!    in `eg_types::protocol::build_envelope_v1_bytes` (pure data, no crypto
//!    dep) so the signer (`eg-plan`'s `RemoteEngineSource::auth_token_v1`) and
//!    this verifier can never independently drift out of sync.
//!
//!    Transport TLS/mTLS and OIDC principal binding are a SEPARATE, later
//!    workstream — this module is the crypto core of the trust boundary only.

use crate::protocol::{build_envelope_v1_bytes, Request};
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};
use std::time::{SystemTime, UNIX_EPOCH};

type HmacSha256 = Hmac<Sha256>;

/// Fixed prefix that marks an `auth_token` as a v1 signed envelope rather than
/// a legacy v0 hex digest. Chosen so detection is an explicit, unambiguous
/// string match — never a heuristic on token length/shape.
const ENVELOPE_V1_PREFIX: &str = "eg1.";

// ── v0 (legacy) ─────────────────────────────────────────────────────────────

/// Compute the HMAC-SHA256 hex token for a request id. Shared by `verify_auth`
/// and trusted in-process callers (tests) that dispatch requests without going
/// through a socket client.
pub fn compute_auth_token(secret: &str, request_id: u64) -> String {
    let Ok(mut mac) = HmacSha256::new_from_slice(secret.as_bytes()) else {
        return String::new();
    };
    mac.update(request_id.to_string().as_bytes());
    hex::encode(mac.finalize().into_bytes())
}

/// Verify a v0 (legacy) HMAC-SHA256 authentication token.
///
/// An empty secret accepts everything — the binary only allows that
/// configuration behind the explicit insecure opt-out enforced at startup
/// (`--allow-insecure` / `EPISTEMIC_GRAPH_ALLOW_INSECURE=1`, see `main.rs`).
///
/// Uses the `hmac` crate's own constant-time [`Mac::verify_slice`] rather than
/// comparing the hex strings with `==`, so even the legacy path never leaks
/// timing information about how many leading bytes of a guessed token matched.
pub(crate) fn verify_auth(secret: &str, request_id: u64, token: &str) -> bool {
    if secret.is_empty() {
        return true; // Explicitly opted-out of authentication at startup.
    }
    let Ok(mut mac) = HmacSha256::new_from_slice(secret.as_bytes()) else {
        return false;
    };
    mac.update(request_id.to_string().as_bytes());
    match hex::decode(token) {
        Ok(bytes) => mac.verify_slice(&bytes).is_ok(),
        Err(_) => false,
    }
}

// ── v1 (signed envelope) — runtime config ──────────────────────────────────

/// Whether this deployment REQUIRES the v1 signed envelope, read ONCE from
/// `EPISTEMIC_GRAPH_REQUIRE_SIGNED` (mirrors the `max_response_nodes`/
/// `txn_limits_from_env` `OnceLock`-env-read convention in `server::state` — a
/// single flag rather than threading a new field through `ServerState`'s ~25
/// construction sites). Default `false`: a legacy v0 request is
/// accepted-with-a-warning, so local/dev and the full existing
/// test/production surface keeps working unchanged. Set to `1`/`true` to flip
/// the policy path to a hard rejection of any v0 request.
pub fn require_signed_envelope() -> bool {
    static FLAG: OnceLock<bool> = OnceLock::new();
    *FLAG.get_or_init(|| {
        std::env::var("EPISTEMIC_GRAPH_REQUIRE_SIGNED")
            .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
            .unwrap_or(false)
    })
}

/// Allowed clock-skew window (seconds) for a v1 envelope's `timestamp`, read
/// ONCE from `EPISTEMIC_GRAPH_ENVELOPE_SKEW_SECS`. Also doubles as the replay
/// cache's nonce-retention horizon (a nonce older than `2 * skew` can never
/// pass the timestamp check anyway, so it is safe to forget). Default 300s
/// (5 minutes).
pub fn envelope_skew_secs() -> u64 {
    static SKEW: OnceLock<u64> = OnceLock::new();
    *SKEW.get_or_init(|| {
        std::env::var("EPISTEMIC_GRAPH_ENVELOPE_SKEW_SECS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .filter(|&n| n > 0)
            .unwrap_or(300)
    })
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

// ── v1 (signed envelope) — wire shape ──────────────────────────────────────

/// The JSON payload packed (hex-encoded) after the [`ENVELOPE_V1_PREFIX`] in
/// `Request.auth_token`. `mac` is the hex HMAC-SHA256 over
/// [`build_envelope_v1_bytes`] (which ALSO folds in `request_id`/`graph`/
/// `method_name`/`body_hash` — those are read straight off the `Request`
/// being verified, not duplicated in this struct).
#[derive(Debug, Clone, Serialize, Deserialize)]
struct EnvelopeV1 {
    #[serde(default)]
    audience: String,
    #[serde(default)]
    tenant: String,
    #[serde(default)]
    principal: String,
    timestamp: u64,
    nonce: String,
    #[serde(default)]
    idempotency_key: String,
    /// Hex HMAC-SHA256 tag.
    mac: String,
}

/// Caller-supplied fields for a v1 envelope that don't already ride the
/// `Request` (`id`/`graph`/`method` are read directly off it).
#[derive(Debug, Clone, Default)]
pub struct EnvelopeParams<'a> {
    pub audience: &'a str,
    pub tenant: &'a str,
    pub principal: &'a str,
    pub timestamp: u64,
    pub nonce: &'a str,
    pub idempotency_key: &'a str,
}

/// Build the (unfinalized) HMAC over the canonical v1 envelope bytes for
/// `req` + the given fields. Shared by both the signer
/// ([`compute_envelope_token`]) and the verifier ([`verify_envelope_v1`]) so
/// signing and verifying can never diverge on the MAC construction — only the
/// final step (`finalize()` to sign vs. `verify_slice()` to check) differs.
fn envelope_mac(secret: &str, req: &Request, params: &EnvelopeParams) -> Option<HmacSha256> {
    let method_name = req.method.tag_name();
    let body_hash = hex::encode(Sha256::digest(req.method.canonical_body_bytes()));
    let bytes = build_envelope_v1_bytes(
        req.id,
        &req.graph,
        &method_name,
        &body_hash,
        params.audience,
        params.tenant,
        params.principal,
        params.timestamp,
        params.nonce,
        params.idempotency_key,
    );
    let mut mac = HmacSha256::new_from_slice(secret.as_bytes()).ok()?;
    mac.update(&bytes);
    Some(mac)
}

/// Sign a v1 envelope for `req`, returning the full `eg1.`-prefixed token to
/// place in `Request.auth_token`. The reference Rust signer implementation —
/// `eg-plan`'s `RemoteEngineSource::auth_token_v1` mirrors this exactly
/// (it cannot call this fn directly: `eg-plan` sits BELOW this facade crate in
/// the dependency DAG).
pub fn compute_envelope_token(secret: &str, req: &Request, params: &EnvelopeParams) -> String {
    let Some(mac) = envelope_mac(secret, req, params) else {
        return String::new();
    };
    let envelope = EnvelopeV1 {
        audience: params.audience.to_string(),
        tenant: params.tenant.to_string(),
        principal: params.principal.to_string(),
        timestamp: params.timestamp,
        nonce: params.nonce.to_string(),
        idempotency_key: params.idempotency_key.to_string(),
        mac: hex::encode(mac.finalize().into_bytes()),
    };
    let json = serde_json::to_vec(&envelope).unwrap_or_default();
    format!("{ENVELOPE_V1_PREFIX}{}", hex::encode(json))
}

/// Why a v1 verification failed (or succeeded), so the dispatch caller can
/// return an actionable error and the right metric/log line.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum EnvelopeVerdict {
    Ok,
    BadSignature,
    Expired,
    Replayed,
}

/// Verify a v1 signed envelope: constant-time MAC compare
/// (`Mac::verify_slice`, never `==`), a timestamp-skew window, and a
/// replay-nonce cache. The MAC is checked FIRST — before the timestamp/nonce
/// checks — so an unauthenticated attacker (who cannot produce a valid MAC)
/// can never pollute the replay cache with junk nonces to force legitimate
/// requests into false "replayed" rejections.
pub(crate) fn verify_envelope_v1(secret: &str, req: &Request) -> EnvelopeVerdict {
    let Some(hex_json) = req.auth_token.strip_prefix(ENVELOPE_V1_PREFIX) else {
        return EnvelopeVerdict::BadSignature;
    };
    let Ok(json_bytes) = hex::decode(hex_json) else {
        return EnvelopeVerdict::BadSignature;
    };
    let Ok(envelope) = serde_json::from_slice::<EnvelopeV1>(&json_bytes) else {
        return EnvelopeVerdict::BadSignature;
    };
    let Ok(got_mac) = hex::decode(&envelope.mac) else {
        return EnvelopeVerdict::BadSignature;
    };
    let Some(mac) = envelope_mac(
        secret,
        req,
        &EnvelopeParams {
            audience: &envelope.audience,
            tenant: &envelope.tenant,
            principal: &envelope.principal,
            timestamp: envelope.timestamp,
            nonce: &envelope.nonce,
            idempotency_key: &envelope.idempotency_key,
        },
    ) else {
        return EnvelopeVerdict::BadSignature;
    };
    // Constant-time tag comparison — CONCEPT: never `==` on secret-derived bytes.
    if mac.verify_slice(&got_mac).is_err() {
        return EnvelopeVerdict::BadSignature;
    }

    let now = now_secs();
    let skew = envelope_skew_secs();
    if now.abs_diff(envelope.timestamp) > skew {
        return EnvelopeVerdict::Expired;
    }

    if envelope.nonce.is_empty() || !replay_cache().check_and_record(&envelope.nonce, now, skew) {
        return EnvelopeVerdict::Replayed;
    }

    EnvelopeVerdict::Ok
}

// ── v1 (signed envelope) — replay-nonce cache ──────────────────────────────

/// Bounded TTL set of nonces seen within the current skew window. A nonce
/// presented twice inside the retention horizon is a replay and is rejected.
/// Pruned lazily on every check (no background task), and hard-capped so a
/// pathological configuration (a huge skew window, or a nonce flood) cannot
/// grow the process's memory unboundedly. Global (not per-graph/per-tenant):
/// a nonce only needs to be unique within its own timestamp window regardless
/// of which graph/tenant it targets, so one cache is both simpler and
/// strictly safer (a cross-tenant nonce reuse is still caught).
struct ReplayCache {
    seen: Mutex<HashMap<String, u64>>,
}

/// Hard cap on cached nonces — bounds memory under a misconfigured
/// (excessively large) skew window or a deliberate nonce-flood attempt.
const MAX_REPLAY_ENTRIES: usize = 200_000;

impl ReplayCache {
    /// Returns `true` if `nonce` is accepted (not seen before within the
    /// retention horizon); `false` if it is a replay. Always prunes entries
    /// older than `2 * window` first (anything older could never pass the
    /// timestamp-skew check anyway, so retaining it further gains nothing).
    fn check_and_record(&self, nonce: &str, now: u64, window: u64) -> bool {
        let mut seen = self.seen.lock().unwrap_or_else(|e| e.into_inner());
        let cutoff = now.saturating_sub(window.saturating_mul(2));
        seen.retain(|_, ts| *ts >= cutoff);
        if seen.contains_key(nonce) {
            return false;
        }
        if seen.len() >= MAX_REPLAY_ENTRIES {
            // Extremely defensive fallback for a pathological configuration
            // that outpaces normal pruning: drop the older half rather than
            // growing without bound.
            let mut entries: Vec<(String, u64)> = seen.drain().collect();
            entries.sort_by_key(|(_, ts)| *ts);
            let keep_from = entries.len() / 2;
            seen.extend(entries.into_iter().skip(keep_from));
        }
        seen.insert(nonce.to_string(), now);
        true
    }
}

fn replay_cache() -> &'static ReplayCache {
    static CACHE: OnceLock<ReplayCache> = OnceLock::new();
    CACHE.get_or_init(|| ReplayCache {
        seen: Mutex::new(HashMap::new()),
    })
}

// ── policy entry point ─────────────────────────────────────────────────────

/// Top-level auth gate `dispatch::dispatch_inner` calls. Routes v0 vs v1
/// requests (by the `eg1.` prefix — an explicit string match, never a
/// heuristic) and applies the [`require_signed_envelope`] policy to a v0
/// request: rejected when the deployment requires v1, accepted with a
/// degrade-path warning otherwise (the default, so local/dev and every
/// existing client/test keeps working unchanged).
pub(crate) fn verify_request(secret: &str, req: &Request) -> Result<(), &'static str> {
    if req.auth_token.starts_with(ENVELOPE_V1_PREFIX) {
        return match verify_envelope_v1(secret, req) {
            EnvelopeVerdict::Ok => Ok(()),
            EnvelopeVerdict::BadSignature => Err("Authentication failed"),
            EnvelopeVerdict::Expired => {
                Err("request timestamp is outside the allowed clock-skew window")
            }
            EnvelopeVerdict::Replayed => Err("nonce already used (replay rejected)"),
        };
    }
    verify_legacy_request(require_signed_envelope(), secret, req)
}

/// The v0 (legacy) policy branch of [`verify_request`], factored out so it
/// takes `require_signed` as an explicit argument — the process-wide
/// `OnceLock` behind [`require_signed_envelope`] can only be initialized once
/// per process, so tests exercise BOTH policy outcomes by calling this
/// directly with `true`/`false` rather than trying to flip the env-backed
/// global mid-suite.
fn verify_legacy_request(
    require_signed: bool,
    secret: &str,
    req: &Request,
) -> Result<(), &'static str> {
    // Explicit policy path — never a silent up/downgrade.
    if require_signed {
        return Err(
            "legacy (v0) request rejected: this deployment requires the v1 signed \
             envelope (EPISTEMIC_GRAPH_REQUIRE_SIGNED=1)",
        );
    }
    if !verify_auth(secret, req.id, &req.auth_token) {
        return Err("Authentication failed");
    }
    if !secret.is_empty() {
        tracing::warn!(
            request_id = req.id,
            "accepted a legacy v0 (unsigned-envelope) request — degrade path; set \
             EPISTEMIC_GRAPH_REQUIRE_SIGNED=1 to require the v1 signed envelope"
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::Method;

    const SECRET: &str = "request-security-test-secret";

    fn ping_request(id: u64, graph: &str, auth_token: String) -> Request {
        Request {
            id,
            graph: graph.to_string(),
            auth_token,
            agent_id: None,
            method: Method::Ping,
        }
    }

    fn signed(id: u64, graph: &str, params: &EnvelopeParams) -> Request {
        let mut req = ping_request(id, graph, String::new());
        req.auth_token = compute_envelope_token(SECRET, &req, params);
        req
    }

    // ── v0 legacy path — unaffected by this workstream ─────────────────

    #[test]
    fn v0_valid_token_accepted() {
        let req = ping_request(1, "g", compute_auth_token(SECRET, 1));
        assert!(verify_request(SECRET, &req).is_ok());
    }

    #[test]
    fn v0_bad_token_rejected() {
        let mut req = ping_request(1, "g", compute_auth_token(SECRET, 1));
        req.auth_token = "bogus".to_string();
        assert!(verify_request(SECRET, &req).is_err());
    }

    #[test]
    fn v0_accepted_with_warning_when_require_signed_is_off() {
        let req = ping_request(1, "g", compute_auth_token(SECRET, 1));
        assert!(verify_legacy_request(false, SECRET, &req).is_ok());
    }

    #[test]
    fn v0_rejected_when_require_signed_is_on() {
        // Same otherwise-valid v0 request; only the policy flag differs.
        let req = ping_request(1, "g", compute_auth_token(SECRET, 1));
        let err = verify_legacy_request(true, SECRET, &req).unwrap_err();
        assert!(err.contains("EPISTEMIC_GRAPH_REQUIRE_SIGNED"));
    }

    // ── v1 signed envelope ──────────────────────────────────────────────

    fn base_params(nonce: &'static str) -> EnvelopeParams<'static> {
        EnvelopeParams {
            audience: "engine",
            tenant: "tenant-a",
            principal: "agent:planner",
            timestamp: now_secs(),
            nonce,
            idempotency_key: "idem-1",
        }
    }

    #[test]
    fn v1_valid_envelope_verifies() {
        let params = base_params("nonce-a");
        let req = signed(1, "g", &params);
        assert!(req.auth_token.starts_with(ENVELOPE_V1_PREFIX));
        assert!(verify_request(SECRET, &req).is_ok());
    }

    #[test]
    fn v1_tampered_graph_rejected() {
        let params = base_params("nonce-b");
        let mut req = signed(1, "g", &params);
        req.graph = "other-graph".to_string();
        assert_eq!(verify_envelope_v1(SECRET, &req), EnvelopeVerdict::BadSignature);
    }

    #[test]
    fn v1_tampered_method_rejected() {
        let params = base_params("nonce-c");
        let mut req = signed(1, "g", &params);
        req.method = Method::Health;
        assert_eq!(verify_envelope_v1(SECRET, &req), EnvelopeVerdict::BadSignature);
    }

    #[test]
    fn v1_tampered_body_rejected() {
        let params = base_params("nonce-body");
        // Sign over `Method::Ping` (via `signed`'s default), then mutate the
        // body to a DIFFERENT method+params AFTER signing — simulating an
        // attacker mutating the request in flight. The body hash is baked
        // into the MAC, so this must be rejected.
        let mut req = signed(1, "g", &params);
        req.method = Method::TouchNodes {
            node_ids: vec!["a".to_string()],
        };
        assert_eq!(
            verify_envelope_v1(SECRET, &req),
            EnvelopeVerdict::BadSignature
        );
    }

    #[test]
    fn v1_tampered_tenant_rejected() {
        let params = base_params("nonce-d");
        let mut req = signed(1, "g", &params);
        // Forge a new envelope claiming a different tenant but reusing the
        // ORIGINAL mac — the mac was computed over "tenant-a", so it must not
        // verify against "tenant-b".
        let json = hex::decode(req.auth_token.strip_prefix(ENVELOPE_V1_PREFIX).unwrap()).unwrap();
        let mut envelope: EnvelopeV1 = serde_json::from_slice(&json).unwrap();
        envelope.tenant = "tenant-b".to_string();
        let forged_json = serde_json::to_vec(&envelope).unwrap();
        req.auth_token = format!("{ENVELOPE_V1_PREFIX}{}", hex::encode(forged_json));
        assert_eq!(verify_envelope_v1(SECRET, &req), EnvelopeVerdict::BadSignature);
    }

    #[test]
    fn v1_replayed_nonce_rejected() {
        let params = base_params("nonce-unique-replay-test");
        let req = signed(42, "replay-graph", &params);
        assert_eq!(verify_envelope_v1(SECRET, &req), EnvelopeVerdict::Ok);
        // Same request, same nonce, presented again — must be rejected.
        assert_eq!(verify_envelope_v1(SECRET, &req), EnvelopeVerdict::Replayed);
    }

    #[test]
    fn v1_expired_timestamp_rejected() {
        let mut params = base_params("nonce-expired");
        params.timestamp = now_secs().saturating_sub(envelope_skew_secs() + 3600);
        let req = signed(1, "g", &params);
        assert_eq!(verify_envelope_v1(SECRET, &req), EnvelopeVerdict::Expired);
    }

    #[test]
    fn v1_future_timestamp_beyond_skew_rejected() {
        let mut params = base_params("nonce-future");
        params.timestamp = now_secs() + envelope_skew_secs() + 3600;
        let req = signed(1, "g", &params);
        assert_eq!(verify_envelope_v1(SECRET, &req), EnvelopeVerdict::Expired);
    }

    /// (e) the constant-time verification path is genuinely exercised: a
    /// single-bit flip ANYWHERE in the MAC — including the very last byte —
    /// is rejected. A short-circuiting `==`-style comparison would still
    /// catch this (functionally), but this pins the CONTRACT that we go
    /// through `Mac::verify_slice` (never `==`) by asserting on the internal
    /// `envelope_mac` + `verify_slice` call directly, matching the
    /// implementation in `verify_envelope_v1` line for line.
    #[test]
    fn v1_constant_time_verify_slice_path_is_used() {
        let params = base_params("nonce-ct");
        let req = signed(7, "g", &params);
        let json = hex::decode(req.auth_token.strip_prefix(ENVELOPE_V1_PREFIX).unwrap()).unwrap();
        let envelope: EnvelopeV1 = serde_json::from_slice(&json).unwrap();
        let mut mac_bytes = hex::decode(&envelope.mac).unwrap();

        // Flip the LAST byte — the position a naive short-circuiting
        // byte-by-byte `==` is most likely to reach last, proving the whole
        // tag is checked, not a truncated prefix.
        *mac_bytes.last_mut().unwrap() ^= 0x01;
        let mac_params = EnvelopeParams {
            audience: &envelope.audience,
            tenant: &envelope.tenant,
            principal: &envelope.principal,
            timestamp: envelope.timestamp,
            nonce: &envelope.nonce,
            idempotency_key: &envelope.idempotency_key,
        };
        let mac = envelope_mac(SECRET, &req, &mac_params).unwrap();
        assert!(
            mac.verify_slice(&mac_bytes).is_err(),
            "Mac::verify_slice must reject a tampered tag"
        );

        // And the wrong-length case (verify_slice's other rejection path).
        let mac2 = envelope_mac(SECRET, &req, &mac_params).unwrap();
        assert!(mac2.verify_slice(&mac_bytes[..mac_bytes.len() - 1]).is_err());
    }

    #[test]
    fn v1_garbage_envelope_rejected_not_panicking() {
        let mut req = ping_request(1, "g", String::new());
        req.auth_token = format!("{ENVELOPE_V1_PREFIX}not-valid-hex-json");
        assert_eq!(verify_envelope_v1(SECRET, &req), EnvelopeVerdict::BadSignature);
        assert!(verify_request(SECRET, &req).is_err());
    }
}
