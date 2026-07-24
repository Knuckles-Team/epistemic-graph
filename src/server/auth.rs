//! Request authentication uses one `eg2.` verified request-context envelope.
//!
//! The envelope binds request/body/replay fields plus the effective ACL agent,
//! roles, scopes, active policy version, and delegation chain under HMAC-SHA256.
//! Audience, tenant, and policy version must match deployment configuration;
//! nonce acceptance is durable and committed before dispatch. Unknown envelope
//! formats fail closed. This workspace intentionally has no downgraded protocol,
//! environment alias, or development authentication escape.
//!
//!    Transport TLS/mTLS are a separate layer: this module is the
//!    request-envelope crypto core, while the served TCP boundary enforces its
//!    configured TLS policy before requests arrive.
//!
//! **Identity binding (feature `oidc`).** The HMAC envelope above proves only
//! that the caller holds `GRAPH_SERVICE_AUTH_SECRET` — it does not by itself
//! prove the envelope's self-asserted `principal`/`tenant`/`roles`/`scopes`
//! belong to whoever is actually calling. [`bind_verified_identity`] requires
//! the envelope to carry an `oidc_token` that independently RSA/JWKS-verifies
//! (reusing `crate::server::oidc`'s verifier, the same one the KV-cache HTTP
//! surface uses) and rejects any mismatch between the envelope's claims and
//! the token's verified subject/tenant/roles/scopes. The HMAC envelope
//! remains channel integrity (replay/tamper/downgrade); the OIDC token is the
//! independent proof of claimed identity.
//!
//! **SECURE BY DEFAULT since 2026-07-22** (`EPISTEMIC_GRAPH_REQUIRE_OIDC`,
//! see [`require_oidc`] — closes the Identity boundary seam, the highest-
//! priority finding of `reports/seam-closure-audit-2026-07-22.md`): a
//! deployment that has NOT configured `EPISTEMIC_GRAPH_OIDC_JWT_ISSUER` (plus
//! its audience/JWKS URL) now fails closed rather than silently accepting
//! HMAC-only identity. The one deliberate, explicit opt-out — for local/dev
//! use only — is `EPISTEMIC_GRAPH_REQUIRE_OIDC=false` (or `0`/`no`/`off`),
//! which restores the pre-2026-07-22 HMAC-only-permitted posture. Nothing
//! defaults into that downgrade; an operator must type it.

use crate::acl::{AgentRole, RequestContextClaims};
use crate::protocol::{
    build_context_operation_signature_bytes, build_envelope_v2_bytes, Method, Request,
};
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
#[cfg(test)]
use std::collections::HashMap;
use std::collections::{BTreeMap, HashSet};
#[cfg(any(test, feature = "security"))]
use std::sync::Mutex;
use std::sync::OnceLock;
use std::time::{SystemTime, UNIX_EPOCH};

type HmacSha256 = Hmac<Sha256>;

/// Prefix for the externally verified request-context envelope.
const ENVELOPE_V2_PREFIX: &str = "eg2.";

/// Authenticated request identity returned only after the corresponding
/// envelope has passed cryptographic and deployment-policy verification.
/// Fields are private so downstream code cannot construct a trusted context
/// from caller-supplied JSON.
#[derive(Debug, Clone)]
pub(crate) struct VerifiedRequestContext {
    claims: RequestContextClaims,
    idempotency_key: String,
    scope_index: HashSet<String>,
    scope_wildcard_domains: HashSet<String>,
}

impl VerifiedRequestContext {
    fn from_verified_claims(claims: RequestContextClaims, idempotency_key: String) -> Self {
        let scope_index = claims.scopes.iter().cloned().collect();
        let scope_wildcard_domains = claims
            .scopes
            .iter()
            .filter_map(|scope| scope.strip_suffix(":*").map(str::to_owned))
            .collect();
        Self {
            claims,
            idempotency_key,
            scope_index,
            scope_wildcard_domains,
        }
    }

    /// Reconstruct the privacy-safe authority carried by a committed Raft entry.
    /// This path is reachable only from the state-machine apply task: external
    /// requests must pass ordinary signature, scope, nonce, and RBAC verification
    /// before the leader can propose the entry. Raw tenant/principal identities are
    /// intentionally unavailable here; followers operate on their one-way scopes.
    #[cfg(feature = "raft")]
    pub(crate) fn replicated_mutation(
        authority: &crate::raft::RaftMutationContext,
    ) -> Result<Self, String> {
        let policy = request_context_policy()?;
        Ok(Self::from_verified_claims(
            RequestContextClaims {
                principal: authority.principal_fingerprint.clone(),
                tenant: authority.tenant_scope.clone(),
                audience: policy.expected_audience.clone(),
                agent_id: authority.principal_fingerprint.clone(),
                roles: vec!["replicated-state-machine".to_string()],
                scopes: vec!["*".to_string()],
                policy_version: policy.expected_policy_version.clone(),
                delegation: Vec::new(),
                node: None,
                priority: None,
            },
            authority.batch_id.clone(),
        ))
    }

    pub(crate) fn agent_id(&self) -> &str {
        self.claims.agent_id.as_str()
    }

    pub(crate) fn principal(&self) -> &str {
        &self.claims.principal
    }

    /// The request's MAC-covered advisory QoS priority claim, if any (W2.4 —
    /// engine-native QoS lanes). Read by the transport's QoS admission gate and
    /// mapped to an admission class by `server::qos::QosClass::from_priority_claim`.
    /// `None` for a client that predates the claim (treated as the orchestration
    /// default). Because it is bound into the verified envelope MAC, a principal
    /// cannot forge a higher class than it signed.
    pub(crate) fn priority(&self) -> Option<&str> {
        self.claims.priority.as_deref()
    }

    /// Tenant carried by the cryptographically verified context. Callers at
    /// subordinate (non-graph) stores must use this value rather than a tenant-like
    /// string supplied in the method body.
    pub(crate) fn tenant(&self) -> &str {
        self.claims.tenant.as_str()
    }

    /// Opaque stable subject identifier safe for durable mutation/audit rows.
    /// Raw identity-provider subjects remain inside the verified request context
    /// and are never copied into graph persistence.
    pub(crate) fn principal_persistence_id(&self) -> String {
        use sha2::{Digest, Sha256};
        format!(
            "principal:sha256:{}",
            hex::encode(Sha256::digest(self.claims.principal.as_bytes()))
        )
    }

    /// Scope gate for capability-ledger actions. A verified context must carry an exact scope,
    /// a domain wildcard (`graph:*`), or the global `*` scope.
    pub(crate) fn allows_action(&self, action: &str) -> bool {
        if self.scope_index.contains("*") || self.scope_index.contains(action) {
            return true;
        }
        action
            .match_indices(':')
            .any(|(offset, _)| self.scope_wildcard_domains.contains(&action[..offset]))
    }

    /// Dedicated authority for remote analytics worker leases. A general
    /// `kg:write` grant cannot read job payloads or publish results.
    pub(crate) fn allows_analytics_worker(&self) -> bool {
        self.allows_action("analytics:worker") || self.scope_index.contains("kg:admin")
    }

    pub(crate) fn allows_identity_bootstrap(&self) -> bool {
        self.claims.principal == self.claims.agent_id
            && self.claims.delegation.is_empty()
            && self.claims.scopes.len() == 1
            && self.claims.scopes[0] == "security:bootstrap"
    }

    /// Evaluate a primitive capability action with the coarse graph-os scopes.
    ///
    /// Exact capability scopes and domain wildcards remain the most precise
    /// grant. ``kg:read``/``kg:write`` are the served API's stable aggregate
    /// scopes and are interpreted using the capability ledger's ``mutates``
    /// bit. Administrative/control-plane actions are never implied by
    /// ``kg:write``; they require ``kg:admin`` or an exact primitive scope.
    pub(crate) fn allows_method(&self, action: &str, mutates: bool) -> bool {
        if self.allows_action(action) {
            return true;
        }
        let has = |scope: &str| self.scope_index.contains(scope);
        if has("kg:admin") {
            return true;
        }
        if coarse_kg_admin_only(action) {
            return false;
        }
        if mutates {
            has("kg:write")
        } else {
            // A write workflow may perform non-mutating precondition reads on
            // the same graph. This does not widen it into another write domain.
            has("kg:read") || has("kg:write")
        }
    }

    #[allow(dead_code)]
    pub(crate) fn claims(&self) -> &RequestContextClaims {
        &self.claims
    }

    #[allow(dead_code)]
    pub(crate) fn idempotency_key(&self) -> &str {
        &self.idempotency_key
    }

    #[cfg(test)]
    pub(crate) fn verified_for_test(agent_id: &str) -> Self {
        Self::verified_for_test_in_tenant(agent_id, "tenant-shared")
    }

    #[cfg(test)]
    pub(crate) fn verified_for_test_in_tenant(agent_id: &str, tenant: &str) -> Self {
        Self::from_verified_claims(
            RequestContextClaims {
                principal: format!("principal:{agent_id}"),
                tenant: tenant.to_string(),
                agent_id: agent_id.to_string(),
                audience: "epistemic-graph".to_string(),
                policy_version: "policy-test".to_string(),
                scopes: vec!["kg:read".to_string()],
                ..RequestContextClaims::default()
            },
            format!("test:{agent_id}"),
        )
    }

    /// Build the authenticated in-process context used only after an auxiliary broker
    /// protocol has verified its own credential and converted the principal to
    /// a secret-keyed opaque actor reference.
    pub(crate) fn authenticated_broker_actor(
        actor_ref: &str,
        request_id: u64,
    ) -> Result<Self, String> {
        let digest = actor_ref
            .strip_prefix("broker:actor:hmac-sha256:")
            .ok_or_else(|| "broker actor reference is invalid".to_string())?;
        if digest.len() != 64 || !digest.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            return Err("broker actor reference is invalid".to_string());
        }
        let policy = request_context_policy()?;
        Ok(Self::from_verified_claims(
            RequestContextClaims {
                principal: actor_ref.to_string(),
                tenant: policy.expected_tenant.clone(),
                audience: policy.expected_audience.clone(),
                agent_id: actor_ref.to_string(),
                roles: vec!["broker-client".to_string()],
                scopes: vec!["broker:*".to_string()],
                policy_version: policy.expected_policy_version.clone(),
                delegation: Vec::new(),
                node: None,
                priority: None,
            },
            format!("broker-request:{request_id}"),
        ))
    }

    /// Bind an engine-owned local query adapter to a fixed service identity.
    /// External traffic reaches this path only after its carrier has passed the
    /// adapter's authentication gate; the service identity must also be present
    /// in durable RBAC policy and receives read scope only.
    pub(crate) fn authenticated_local_query(request_id: u64) -> Result<Self, String> {
        let policy = request_context_policy()?;
        Ok(Self::from_verified_claims(
            RequestContextClaims {
                principal: "service:local-query".to_string(),
                tenant: policy.expected_tenant.clone(),
                audience: policy.expected_audience.clone(),
                agent_id: "service:local-query".to_string(),
                roles: vec!["local-query-adapter".to_string()],
                scopes: vec!["kg:read".to_string()],
                policy_version: policy.expected_policy_version.clone(),
                delegation: Vec::new(),
                node: None,
                priority: None,
            },
            format!("local-query:{request_id}"),
        ))
    }

    /// Build the engine-owned authority for a native SQL connection after that
    /// protocol has completed its mandatory cryptographic password proof.
    ///
    /// Native SQL protocols do not carry an `eg2.` envelope per statement.  Their
    /// loopback adapters therefore act as authenticated proxies: after SCRAM/HMAC
    /// verification they bind the authenticated ACL actor to the deployment's
    /// configured tenant/audience/policy.  The principal stored downstream is a
    /// secret-keyed opaque reference, never the login name.
    pub(crate) fn authenticated_sql_wire_actor(
        secret: &str,
        protocol: &str,
        agent_id: &str,
    ) -> Result<Self, String> {
        let protocol = match protocol {
            "pgwire" | "mysql-wire" | "mssql-wire" => protocol,
            _ => return Err("native SQL authority protocol is invalid".to_string()),
        };
        let agent_id = agent_id.trim();
        if secret.is_empty() || agent_id.is_empty() {
            return Err("native SQL authority requires a verified identity".to_string());
        }
        let mut mac = HmacSha256::new_from_slice(secret.as_bytes())
            .map_err(|_| "native SQL authority key is invalid".to_string())?;
        mac.update(b"native-sql-authority\0");
        mac.update(protocol.as_bytes());
        mac.update(&[0]);
        mac.update(agent_id.as_bytes());
        let principal = format!(
            "sql-wire:actor:hmac-sha256:{}",
            hex::encode(mac.finalize().into_bytes())
        );
        let idempotency_key = format!("native-sql-session:{protocol}:{principal}");
        let policy = request_context_policy()?;
        Ok(Self::from_verified_claims(
            RequestContextClaims {
                principal,
                tenant: policy.expected_tenant.clone(),
                audience: policy.expected_audience.clone(),
                agent_id: agent_id.to_string(),
                roles: vec!["native-sql-client".to_string()],
                scopes: vec!["kg:read".to_string(), "kg:write".to_string()],
                policy_version: policy.expected_policy_version.clone(),
                delegation: Vec::new(),
                node: None,
                priority: None,
            },
            idempotency_key,
        ))
    }
}

/// Actions that a coarse ``kg:write`` grant must never authorize.
///
/// Exact fine-grained scopes still work, and ``kg:admin`` remains the explicit
/// aggregate grant. The string classification is over MethodPolicy's canonical
/// action, not method names, so new methods inherit the correct posture when
/// they declare an ``*:admin``/``*:control``/``admin:*`` capability.
fn coarse_kg_admin_only(action: &str) -> bool {
    action.starts_with("admin:")
        || action.starts_with("security:")
        || action.ends_with(":admin")
        || action.ends_with(":control")
}

/// Deployment policy used to turn signed v2 claims into a trusted context.
/// The explicit constructor is intentionally test/adapter friendly; production
/// builds resolve the same fields from environment configuration.
#[derive(Debug, Clone)]
struct RequestContextPolicy {
    expected_audience: String,
    expected_tenant: String,
    expected_policy_version: String,
}

impl RequestContextPolicy {
    fn from_env() -> Result<Self, String> {
        fn configured(name: &str) -> Result<String, String> {
            std::env::var(name)
                .ok()
                .map(|v| v.trim().to_string())
                .filter(|v| !v.is_empty())
                .ok_or_else(|| format!("secure request context requires {name}"))
        }
        Ok(RequestContextPolicy {
            expected_audience: configured("EPISTEMIC_GRAPH_AUDIENCE")?,
            expected_tenant: configured("EPISTEMIC_GRAPH_TENANT")?,
            expected_policy_version: configured("EPISTEMIC_GRAPH_POLICY_VERSION")?,
        })
    }
}

#[cfg(not(test))]
fn request_context_policy() -> Result<&'static RequestContextPolicy, String> {
    static POLICY: OnceLock<Result<RequestContextPolicy, String>> = OnceLock::new();
    match POLICY.get_or_init(RequestContextPolicy::from_env) {
        Ok(policy) => Ok(policy),
        Err(message) => Err(message.clone()),
    }
}

#[cfg(test)]
fn request_context_policy() -> Result<&'static RequestContextPolicy, String> {
    static POLICY: OnceLock<RequestContextPolicy> = OnceLock::new();
    Ok(POLICY.get_or_init(|| RequestContextPolicy {
        expected_audience: "epistemic-graph-test".to_string(),
        expected_tenant: "tenant-shared".to_string(),
        expected_policy_version: "policy-test".to_string(),
    }))
}

/// Allowed clock-skew window (seconds) for an envelope's `timestamp`, read
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

// ── Node-bound envelopes (ADR-3 / W1.9, `reports/wave1/ADR-scale-trio.md`) ──
//
// Replay protection today is a per-node local ledger (see `RedbReplayLedger`
// below): in a cluster, a captured envelope can be replayed once per node
// within the clock-skew window, because each node's `seen`-nonce set is
// disjoint. Routing verification through Raft consensus would close that at
// an unacceptable per-request cost. Instead the envelope binds itself to the
// specific node the client minted it for: a replay presented to any OTHER
// node fails the exact-match check below before it ever reaches the nonce
// ledger, at zero consensus/replication cost. Same-node replay is unaffected
// and still caught by the existing ledger.

/// This node's own stable identity for envelope node-binding.
///
/// Clustered (built `--features raft` AND `EPISTEMIC_GRAPH_RAFT_NODE_ID`
/// set): the raft node id -- the SAME integer identifying this node in
/// `EPISTEMIC_GRAPH_RAFT_PEERS`, and (once ADR-1 lands) in
/// `ClusterMembers`/`PlacementRoute` endpoints, so a client that learns its
/// target node id from either of those mints a claim this function matches.
///
/// Otherwise (single-node, or a build without the `raft` feature): the
/// explicit `EPISTEMIC_GRAPH_NODE_ID` override, defaulting to the stable
/// literal `"single"` -- so a single-node deployment's node claim, if a
/// client ever bothers to mint one, always matches with zero operator
/// configuration (ADR-3: "claim optional forever... zero friction for the
/// published wheel's default profile").
#[cfg(not(test))]
fn node_identity() -> &'static str {
    static IDENTITY: OnceLock<String> = OnceLock::new();
    IDENTITY
        .get_or_init(|| {
            #[cfg(feature = "raft")]
            {
                if let Ok(raft_id) = std::env::var("EPISTEMIC_GRAPH_RAFT_NODE_ID") {
                    let trimmed = raft_id.trim();
                    if !trimmed.is_empty() {
                        return trimmed.to_string();
                    }
                }
            }
            std::env::var("EPISTEMIC_GRAPH_NODE_ID")
                .ok()
                .map(|v| v.trim().to_string())
                .filter(|v| !v.is_empty())
                .unwrap_or_else(|| "single".to_string())
        })
        .as_str()
}

/// Test-only fixed identity: real env/OnceLock resolution is intentionally
/// untested here (mirrors `envelope_skew_secs`/`RequestContextPolicy`'s own
/// `#[cfg(test)]` overrides elsewhere in this file) -- unit tests instead
/// vary the CLAIM's node value between a match (`"node-a"`) and a mismatch
/// (any other string).
#[cfg(test)]
fn node_identity() -> &'static str {
    "node-a"
}

/// Rollout posture for the node-binding claim
/// (`EPISTEMIC_GRAPH_REQUIRE_NODE_BINDING`). Tri-state, unlike the boolean
/// [`env_flag_explicit`] flags elsewhere in this module, because the safe
/// migration path needs a distinguishable "accept but log" middle state
/// between "ignore" and "enforce": a deployment can watch its `warn`-mode
/// logs for still-unmigrated clients before flipping to `on`.
///
/// A PRESENT claim is always exact-matched against [`node_identity`] in
/// EVERY mode -- only an ABSENT claim's handling varies by mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum NodeBindingMode {
    /// Absent claim accepted silently.
    Off,
    /// Absent claim accepted, but logged once per principal so an operator
    /// can find not-yet-migrated clients before flipping to `On`. The
    /// shipped default.
    Warn,
    /// Absent claim rejected (fail closed) -- the W5.2 cluster-cutover
    /// posture.
    On,
}

/// Parse `EPISTEMIC_GRAPH_REQUIRE_NODE_BINDING` (`off`/`warn`/`on`, case
/// insensitive, trimmed). Unset or unrecognized ⇒ `Warn` -- permissive
/// enough that an un-migrated client keeps working, but visible enough that
/// an operator preparing for the W5.2 cluster cutover can see it coming.
#[cfg(not(test))]
fn require_node_binding_mode() -> NodeBindingMode {
    static MODE: OnceLock<NodeBindingMode> = OnceLock::new();
    *MODE.get_or_init(|| {
        std::env::var("EPISTEMIC_GRAPH_REQUIRE_NODE_BINDING")
            .ok()
            .map(|v| v.trim().to_ascii_lowercase())
            .and_then(|v| match v.as_str() {
                "off" => Some(NodeBindingMode::Off),
                "warn" => Some(NodeBindingMode::Warn),
                "on" => Some(NodeBindingMode::On),
                _ => None,
            })
            .unwrap_or(NodeBindingMode::Warn)
    })
}

// Test-only override, exactly like `TEST_REQUIRE_OIDC` above it: env vars are
// process-global and would race across parallel `libtest` threads, so the
// posture is a per-thread cell each `#[test]` sets explicitly via the guard
// functions below. Default matches production's shipped default (`Warn`).
#[cfg(test)]
thread_local! {
    static TEST_NODE_BINDING_MODE: std::cell::Cell<NodeBindingMode> =
        const { std::cell::Cell::new(NodeBindingMode::Warn) };
}

#[cfg(test)]
fn require_node_binding_mode() -> NodeBindingMode {
    TEST_NODE_BINDING_MODE.with(std::cell::Cell::get)
}

/// Bounded best-effort de-duplication so `warn` mode logs an absent
/// node-binding claim once per principal rather than once per request.
/// Bounded like `ReplayCache` bounds nonces: an operator running many
/// distinct never-upgraded principals should not grow this unboundedly --
/// past the cap the set is cleared and warnings resume (a duplicate warning
/// is harmless, just noisier).
const MAX_WARNED_NODE_BINDING_PRINCIPALS: usize = 10_000;

fn warn_absent_node_claim_once(principal: &str) {
    static WARNED: OnceLock<std::sync::Mutex<HashSet<String>>> = OnceLock::new();
    let warned = WARNED.get_or_init(|| std::sync::Mutex::new(HashSet::new()));
    let mut warned = warned.lock().unwrap_or_else(|e| e.into_inner());
    if warned.len() >= MAX_WARNED_NODE_BINDING_PRINCIPALS {
        warned.clear();
    }
    if warned.insert(principal.to_string()) {
        tracing::warn!(
            principal,
            "eg2. request context is missing the node-binding claim \
             (EPISTEMIC_GRAPH_REQUIRE_NODE_BINDING=warn); accepted for \
             compatibility, but this principal's client should be upgraded \
             before the deployment flips to `on`"
        );
    }
}

// ── Verified request context envelope ──────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct EnvelopeV2 {
    context: RequestContextClaims,
    timestamp: u64,
    nonce: String,
    idempotency_key: String,
    /// Optional OIDC bearer/assertion binding `context`'s principal/tenant/
    /// roles/scopes to a verified external identity (feature `oidc`, see
    /// [`bind_verified_identity`]). Absent unless the issuer mints one.
    ///
    /// Deliberately NOT folded into the HMAC digest
    /// (`build_envelope_v2_bytes`): its own RSA signature is what anchors
    /// trust, not MAC coverage. A holder of `GRAPH_SERVICE_AUTH_SECRET` who
    /// swaps this field between two envelopes gains nothing — they still
    /// cannot forge a token's RSA signature, and [`bind_verified_identity`]
    /// rejects any mismatch between the swapped-in token's verified claims
    /// and `context`'s asserted ones. Channel integrity for this field is a
    /// transport concern (TLS), exactly as for the rest of the envelope.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    oidc_token: Option<String>,
    /// Hex HMAC-SHA256 tag over `build_envelope_v2_bytes`.
    mac: String,
}

/// Fields supplied by an external identity/context issuer when signing v2.
#[derive(Debug, Clone)]
pub struct VerifiedEnvelopeParams<'a> {
    pub context: &'a RequestContextClaims,
    pub timestamp: u64,
    pub nonce: &'a str,
    pub idempotency_key: &'a str,
}

fn envelope_v2_mac(
    secret: &str,
    req: &Request,
    params: &VerifiedEnvelopeParams,
) -> Option<HmacSha256> {
    let method_name = req.method.tag_name();
    let body_hash = hex::encode(Sha256::digest(req.method.canonical_body_bytes()));
    let bytes = build_envelope_v2_bytes(
        req.id,
        &req.graph,
        &method_name,
        &body_hash,
        params.context,
        params.timestamp,
        params.nonce,
        params.idempotency_key,
    );
    let mut mac = HmacSha256::new_from_slice(secret.as_bytes()).ok()?;
    mac.update(&bytes);
    Some(mac)
}

/// Reference v2 signer.  External gateways should produce the same canonical
/// bytes and place this `eg2.` token in `Request.auth_token`.
pub fn compute_verified_envelope_token(
    secret: &str,
    req: &Request,
    params: &VerifiedEnvelopeParams,
) -> String {
    let Some(mac) = envelope_v2_mac(secret, req, params) else {
        return String::new();
    };
    let envelope = EnvelopeV2 {
        context: params.context.clone(),
        timestamp: params.timestamp,
        nonce: params.nonce.to_string(),
        idempotency_key: params.idempotency_key.to_string(),
        // This reference signer does not carry an OIDC assertion; every one of
        // its ~20 call sites across the codebase mints envelopes for HMAC-only
        // deployments/tests. A caller that needs `oidc_token` bound in builds
        // its own `EnvelopeV2`-shaped envelope (see auth.rs's own OIDC binding
        // tests) rather than this general-purpose reference implementation.
        oidc_token: None,
        mac: hex::encode(mac.finalize().into_bytes()),
    };
    let json = serde_json::to_vec(&envelope).unwrap_or_default();
    format!("{ENVELOPE_V2_PREFIX}{}", hex::encode(json))
}

#[cfg(test)]
pub(crate) fn sign_current_test_request(secret: &str, mut request: Request) -> Request {
    const TEST_OPERATION_SIGNER_KEY: &str = "rust-unit-operation-signer-key";
    static NONCE: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
    let agent_id = request.agent_id.as_deref().unwrap_or("system").to_string();
    request.agent_id = Some(agent_id.clone());
    let identity_bootstrap = matches!(
        &request.method,
        Method::RegisterIdentity {
            agent_id: registered,
            role: AgentRole::System,
            teams,
            roles,
            ..
        } if registered == &agent_id && teams.is_empty() && roles.is_empty()
    );
    let context = RequestContextClaims {
        principal: agent_id.clone(),
        tenant: "tenant-shared".to_string(),
        audience: "epistemic-graph-test".to_string(),
        agent_id,
        roles: vec!["test".to_string()],
        scopes: vec![if identity_bootstrap {
            "security:bootstrap".to_string()
        } else {
            "*".to_string()
        }],
        policy_version: "policy-test".to_string(),
        delegation: Vec::new(),
        node: None,
    };
    let nonce = format!(
        "rust-unit-{}-{}",
        std::process::id(),
        NONCE.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    );
    let idempotency_key = format!("rust-unit-request-{}-{nonce}", request.id);
    if let Method::RegisterIdentity {
        agent_id,
        role,
        teams,
        signature,
        roles,
    } = &mut request.method
    {
        let verified_context =
            VerifiedRequestContext::from_verified_claims(context.clone(), idempotency_key.clone());
        let digest = register_identity_digest(
            &verified_context,
            &request.graph,
            agent_id,
            role,
            teams,
            roles,
        );
        let mut mac = HmacSha256::new_from_slice(TEST_OPERATION_SIGNER_KEY.as_bytes())
            .expect("test signer key");
        mac.update(&digest);
        *signature = format!(
            "{}:{}",
            context.principal,
            hex::encode(mac.finalize().into_bytes())
        );
    }
    request.auth_token = compute_verified_envelope_token(
        secret,
        &request,
        &VerifiedEnvelopeParams {
            context: &context,
            timestamp: now_secs(),
            nonce: &nonce,
            idempotency_key: &idempotency_key,
        },
    );
    request
}

fn decode_envelope_v2(req: &Request) -> Result<EnvelopeV2, String> {
    let hex_json = req
        .auth_token
        .strip_prefix(ENVELOPE_V2_PREFIX)
        .ok_or_else(|| "Authentication failed".to_string())?;
    let json = hex::decode(hex_json).map_err(|_| "Authentication failed".to_string())?;
    serde_json::from_slice(&json).map_err(|_| "Authentication failed".to_string())
}

fn validate_unique_claims(label: &str, values: &[String]) -> Result<(), String> {
    let mut seen = HashSet::new();
    for value in values {
        if value.trim().is_empty() {
            return Err(format!("request context contains an empty {label}"));
        }
        if !seen.insert(value.as_str()) {
            return Err(format!(
                "request context contains duplicate {label} '{value}'"
            ));
        }
    }
    Ok(())
}

fn validate_context_claims(
    req: &Request,
    claims: &RequestContextClaims,
    policy: &RequestContextPolicy,
) -> Result<(), String> {
    for (name, value) in [
        ("principal", claims.principal.as_str()),
        ("tenant", claims.tenant.as_str()),
        ("audience", claims.audience.as_str()),
        ("agent_id", claims.agent_id.as_str()),
        ("policy_version", claims.policy_version.as_str()),
    ] {
        if value.trim().is_empty() {
            return Err(format!("request context {name} must not be empty"));
        }
    }
    validate_unique_claims("role", &claims.roles)?;
    validate_unique_claims("scope", &claims.scopes)?;
    validate_unique_claims("delegation subject", &claims.delegation)?;

    if let Some(asserted_agent) = req.agent_id.as_deref() {
        if asserted_agent != claims.agent_id {
            return Err("request agent_id does not match verified context".to_string());
        }
    }
    if claims.principal == claims.agent_id {
        if !claims.delegation.is_empty() {
            return Err("non-delegated context must have an empty delegation chain".to_string());
        }
    } else if claims.delegation.first().map(String::as_str) != Some(claims.principal.as_str())
        || claims.delegation.last().map(String::as_str) != Some(claims.agent_id.as_str())
        || claims.delegation.len() < 2
    {
        return Err("delegation chain must run from principal to effective agent".to_string());
    }

    if claims.audience != policy.expected_audience {
        return Err("request context audience does not match deployment".to_string());
    }
    if claims.tenant != policy.expected_tenant {
        return Err("request context tenant does not match graph tenant".to_string());
    }
    if claims.policy_version != policy.expected_policy_version {
        return Err("request context policy version is not active".to_string());
    }

    // ── ADR-3 / W1.9: node-bound envelopes ──────────────────────────────
    // A present claim is ALWAYS exact-matched against this node's own
    // identity, in every posture -- only an ABSENT claim's handling varies
    // by `EPISTEMIC_GRAPH_REQUIRE_NODE_BINDING`. Checked here (inside the
    // same pre-dispatch claims check `verify_envelope_v2_with` runs BEFORE
    // its nonce/replay lookup) so a captured envelope replayed against a
    // DIFFERENT node fails fast, at zero consensus/replication cost, before
    // ever touching the replay ledger.
    match claims.node.as_deref() {
        Some(claimed) if claimed.trim().is_empty() => {
            return Err("request context node claim must not be empty when present".to_string());
        }
        Some(claimed) => {
            if claimed != node_identity() {
                return Err(format!(
                    "NODE_MISMATCH: request context is bound to node '{claimed}', \
                     which does not match this node's identity"
                ));
            }
        }
        None => match require_node_binding_mode() {
            NodeBindingMode::Off => {}
            NodeBindingMode::Warn => warn_absent_node_claim_once(&claims.principal),
            NodeBindingMode::On => {
                return Err("NODE_MISMATCH: request context is missing the required \
                     node-binding claim (EPISTEMIC_GRAPH_REQUIRE_NODE_BINDING=on)"
                    .to_string());
            }
        },
    }

    Ok(())
}

/// Replay ledger used after a request MAC, time window, and policy claims have
/// verified. The production adapter is durable and commits before dispatch.
trait ReplayLedger: Send + Sync {
    /// Atomically record a nonce. `Ok(false)` means it was already present.
    fn check_and_record(&self, nonce: &str, now: u64, window: u64) -> Result<bool, String>;
}

#[cfg(test)]
struct ReplayCache {
    seen: Mutex<HashMap<String, u64>>,
}

/// Hard cap on cached nonces — bounds memory under a misconfigured
/// (excessively large) skew window or a deliberate nonce-flood attempt.
#[cfg(test)]
const MAX_REPLAY_ENTRIES: usize = 200_000;

#[cfg(test)]
impl ReplayCache {
    /// Returns `true` if `nonce` is accepted (not seen before within the
    /// retention horizon); `false` if it is a replay. Always prunes entries
    /// older than `2 * window` first (anything older could never pass the
    /// timestamp-skew check anyway, so retaining it further gains nothing).
    fn check_and_record_memory(&self, nonce: &str, now: u64, window: u64) -> bool {
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

#[cfg(test)]
impl ReplayLedger for ReplayCache {
    fn check_and_record(&self, nonce: &str, now: u64, window: u64) -> Result<bool, String> {
        Ok(self.check_and_record_memory(nonce, now, window))
    }
}

#[cfg(feature = "security")]
const REPLAY_TABLE: redb::TableDefinition<&str, u64> =
    redb::TableDefinition::new("verified_request_replay_v2");

/// Durable replay adapter used by secure mode.  A successful insert is
/// committed with immediate durability before the request is dispatched, so a
/// process restart cannot make a previously accepted nonce usable again.
///
/// **KNOWN GAP — per-node only, NOT replicated across a `cluster`/`raft`
/// deployment** (tracked in `reports/seam-identity-closure.md`, "Raft
/// replay-ledger replication" section; called out by
/// `reports/seam-closure-audit-2026-07-22.md`'s Identity row). This ledger is
/// a local `redb` table scoped to ONE node's `EPISTEMIC_GRAPH_SECURITY_STATE_DIR`.
/// It is checked entirely BEFORE any Raft/consensus code runs (see
/// `dispatch_inner` in `server/dispatch.rs`, which calls
/// `verify_request_with_security_dir` — and therefore this ledger — before
/// any `#[cfg(feature = "raft")]` code executes). In a hypothetical
/// multi-node `cluster` deployment, a captured, still-signature-valid signed
/// envelope COULD be replayed once against every node independently within
/// the clock-skew window (`envelope_skew_secs()`, default 300s), because each
/// node's `seen`-nonce set is disjoint. Closing this requires routing the
/// nonce check-and-record through the SAME Raft-log consensus path ordinary
/// mutations use (`crate::raft::ReplicatedMutation` / `NativeMutationCommand`
/// in `src/raft/mod.rs`) rather than a purely local pre-check — a genuine new
/// integration point on the hot path of EVERY authenticated request
/// (including reads), not merely "replicate existing state." As of this
/// writing the homelab's production `epistemic-graph` deployment does not run
/// the `cluster`/`raft` feature at all (the default/`full` build links no
/// `openraft`; see this crate's `Cargo.toml` `cluster` feature and the seam
/// audit's Placement-seam finding), so this gap is not currently exploitable
/// in production — but MUST be closed before any multi-node `cluster` rollout.
#[cfg(feature = "security")]
struct RedbReplayLedger {
    db: redb::Database,
    last_prune: Mutex<u64>,
}

#[cfg(feature = "security")]
impl RedbReplayLedger {
    fn open(dir: &std::path::Path) -> Result<Self, String> {
        std::fs::create_dir_all(dir)
            .map_err(|e| format!("could not create security state directory: {e}"))?;
        let path = dir.join("request-replay.redb");
        let db = redb::Database::create(path)
            .map_err(|e| format!("could not open durable replay ledger: {e}"))?;
        let wtx = db
            .begin_write()
            .map_err(|e| format!("could not initialize durable replay ledger: {e}"))?;
        wtx.open_table(REPLAY_TABLE)
            .map_err(|e| format!("could not initialize durable replay table: {e}"))?;
        wtx.commit()
            .map_err(|e| format!("could not commit durable replay table: {e}"))?;
        Ok(RedbReplayLedger {
            db,
            last_prune: Mutex::new(0),
        })
    }
}

#[cfg(feature = "security")]
impl ReplayLedger for RedbReplayLedger {
    fn check_and_record(&self, nonce: &str, now: u64, window: u64) -> Result<bool, String> {
        use redb::ReadableTable;

        let should_prune = {
            let mut last = self.last_prune.lock().unwrap_or_else(|e| e.into_inner());
            if now.saturating_sub(*last) >= window {
                *last = now;
                true
            } else {
                false
            }
        };
        let mut wtx = self
            .db
            .begin_write()
            .map_err(|e| format!("durable replay transaction failed: {e}"))?;
        wtx.set_durability(redb::Durability::Immediate)
            .map_err(|e| format!("durable replay configuration failed: {e}"))?;
        {
            let mut table = wtx
                .open_table(REPLAY_TABLE)
                .map_err(|e| format!("durable replay table failed: {e}"))?;
            if should_prune {
                let cutoff = now.saturating_sub(window.saturating_mul(2));
                let mut expired = Vec::new();
                for row in table
                    .iter()
                    .map_err(|e| format!("durable replay scan failed: {e}"))?
                {
                    let (key, timestamp) =
                        row.map_err(|e| format!("durable replay row failed: {e}"))?;
                    if timestamp.value() < cutoff {
                        expired.push(key.value().to_string());
                    }
                }
                for key in expired {
                    table
                        .remove(key.as_str())
                        .map_err(|e| format!("durable replay prune failed: {e}"))?;
                }
            }
            if table
                .get(nonce)
                .map_err(|e| format!("durable replay lookup failed: {e}"))?
                .is_some()
            {
                return Ok(false);
            }
            table
                .insert(nonce, now)
                .map_err(|e| format!("durable replay insert failed: {e}"))?;
        }
        wtx.commit()
            .map_err(|e| format!("durable replay commit failed: {e}"))?;
        Ok(true)
    }
}

#[cfg(all(feature = "security", not(test)))]
fn durable_replay_ledger(state_dir: Option<&str>) -> Result<&'static RedbReplayLedger, String> {
    static LEDGER: OnceLock<Result<RedbReplayLedger, String>> = OnceLock::new();
    match LEDGER.get_or_init(|| {
        let dir = std::env::var("EPISTEMIC_GRAPH_SECURITY_STATE_DIR")
            .ok()
            .map(|v| v.trim().to_string())
            .filter(|v| !v.is_empty())
            .or_else(|| state_dir.map(str::to_string))
            .ok_or_else(|| {
                "secure request context requires EPISTEMIC_GRAPH_SECURITY_STATE_DIR or a persist directory".to_string()
            })?;
        RedbReplayLedger::open(std::path::Path::new(&dir))
    }) {
        Ok(ledger) => Ok(ledger),
        Err(message) => Err(message.clone()),
    }
}

#[cfg(all(not(feature = "security"), not(test)))]
fn durable_replay_ledger(_state_dir: Option<&str>) -> Result<&'static dyn ReplayLedger, String> {
    Err("secure request context requires the security feature".to_string())
}

#[cfg(test)]
fn durable_replay_ledger(_state_dir: Option<&str>) -> Result<&'static dyn ReplayLedger, String> {
    static LEDGER: OnceLock<ReplayCache> = OnceLock::new();
    Ok(LEDGER.get_or_init(|| ReplayCache {
        seen: Mutex::new(HashMap::new()),
    }))
}

/// Parse a boolean-ish deployment flag from the environment. Unset or any
/// unrecognized value ⇒ `false` (the safe default that preserves today's
/// behavior). Accepts the common truthy spellings so an operator is not
/// surprised by `TRUE`/`yes`/`on`. Read fresh each call: this is a coarse,
/// process-lifetime posture switch, not a hot path.
fn env_flag_enabled(name: &str) -> bool {
    std::env::var(name)
        .ok()
        .map(|v| v.trim().to_ascii_lowercase())
        .map(|v| matches!(v.as_str(), "1" | "true" | "yes" | "on"))
        .unwrap_or(false)
}

/// Parse an EXPLICIT env override for a boolean-ish deployment flag:
/// `Some(true)`/`Some(false)` for a recognized value, `None` if the variable
/// is unset, empty, OR carries an unrecognized value. Unlike
/// [`env_flag_enabled`] (whose safe default is "off"), this is the primitive
/// for flags whose safe default is "on" — [`require_oidc`] uses it so a
/// mistyped opt-out (e.g. `EPISTEMIC_GRAPH_REQUIRE_OIDC=fals`) can never
/// silently disable a security gate by falling through to `false`; only a
/// clearly-recognized falsy spelling opts out.
fn env_flag_explicit(name: &str) -> Option<bool> {
    let raw = std::env::var(name).ok()?;
    match raw.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Some(true),
        "0" | "false" | "no" | "off" => Some(false),
        _ => None,
    }
}

// ── OIDC identity binding (primary eg2. protocol, feature `oidc`) ─────────
//
// Extends `crate::server::oidc`'s existing RSA/JWKS verifier — the exact one
// the ancillary KV-cache HTTP surface already uses — to this primary
// protocol. No new signature/JWKS code: `bind_verified_identity` only calls
// `JwtValidator::validate_claims` and compares the result against the
// envelope's self-asserted claims.

// Test-only override for `primary_oidc_validator()`'s production
// (env/`OnceLock`-backed) resolution. Each `#[test]` fn runs on its own
// thread (standard `libtest` behavior), so this is cleanly isolated per
// test; `OidcTestGuard`'s `Drop` also resets it defensively.
#[cfg(all(feature = "oidc", test))]
thread_local! {
    static TEST_OIDC_VALIDATOR: std::cell::Cell<Option<&'static crate::server::oidc::JwtValidator>> =
        const { std::cell::Cell::new(None) };
}

/// Resolve the primary protocol's configured OIDC validator, if any.
///
/// `Ok(None)` ⇒ `EPISTEMIC_GRAPH_OIDC_JWT_ISSUER` is not set: identity
/// binding is disabled and today's HMAC-only behavior is preserved.
/// `Err` ⇒ an issuer is set but audience/JWKS URL are missing (fails closed,
/// also surfaced eagerly by `validate_verified_context_startup`).
#[cfg(all(feature = "oidc", test))]
fn primary_oidc_validator() -> Result<Option<&'static crate::server::oidc::JwtValidator>, String> {
    Ok(TEST_OIDC_VALIDATOR.with(|cell| cell.get()))
}

#[cfg(all(feature = "oidc", not(test)))]
fn primary_oidc_validator() -> Result<Option<&'static crate::server::oidc::JwtValidator>, String> {
    static VALIDATOR: OnceLock<Result<Option<crate::server::oidc::JwtValidator>, String>> =
        OnceLock::new();
    match VALIDATOR.get_or_init(crate::server::oidc::JwtValidator::from_env_primary) {
        Ok(Some(v)) => Ok(Some(v)),
        Ok(None) => Ok(None),
        Err(message) => Err(message.clone()),
    }
}

/// Config-gated MANDATORY-OIDC posture (`EPISTEMIC_GRAPH_REQUIRE_OIDC`).
/// Shared by both [`bind_verified_identity`] variants below (the `oidc`-
/// feature verifier path AND the no-`oidc`-feature fallback), so the default
/// posture is identical regardless of which server binary is running.
///
/// **SECURE BY DEFAULT since 2026-07-22** (unset, empty, or any unrecognized
/// value ⇒ `true`, via [`env_flag_explicit`]): the engine refuses any request
/// whose self-asserted identity cannot be bound to a valid, RSA/JWKS-verified
/// OIDC bearer token whose subject/tenant match the claimed principal/tenant.
/// Critically this also fails closed when NO OIDC issuer is configured (or
/// when this binary was built without the `oidc` feature at all) — a
/// deployment that demands OIDC but forgot to (or cannot) configure a
/// verifier must never silently downgrade to shared-secret HMAC alone.
///
/// Explicit, deliberate opt-out for local/dev use ONLY: a RECOGNIZED falsy
/// value (`false`/`0`/`no`/`off`) ⇒ `false`, restoring the pre-2026-07-22
/// HMAC-only-permitted posture. An unrecognized value does NOT opt out (see
/// [`env_flag_explicit`]) — only a clean, deliberate spelling downgrades
/// security.
#[cfg(not(test))]
fn require_oidc() -> bool {
    env_flag_explicit("EPISTEMIC_GRAPH_REQUIRE_OIDC").unwrap_or(true)
}

// Test-only override for `require_oidc()`. Env vars are process-global and
// would race across the parallel `libtest` threads, so — exactly like
// `TEST_OIDC_VALIDATOR` — the posture is a per-thread cell each `#[test]`
// sets explicitly.
//
// Default is `false`, i.e. the PRE-2026-07-22 posture, deliberately NOT
// matching the new production default. Rationale: hundreds of pre-existing
// unit/integration tests across this crate (dispatch, redb persistence, the
// bolt/mqtt/stomp/mysql/sqlite wire protocols, transactions, …) build a
// plain `eg2.` HMAC envelope with no `oidc_token` because they are
// deliberately testing something else entirely; if this thread-local
// defaulted to the new secure posture, every one of them would need an
// unrelated opt-out edit — exactly the kind of scope explosion a change like
// this must avoid. So this default stays scoped to the test harness rather
// than mirroring a live deployment: only tests that specifically exercise
// identity binding call
// `require_oidc_on()`. The REAL production default (secure-by-default,
// unset ⇒ required) lives in the `#[cfg(not(test))]` `require_oidc()` below
// and in `env_flag_explicit`'s `.unwrap_or(true)` fold, both exercised
// directly by `env_flag_explicit_defaults_secure_when_unset_or_unrecognized`
// and by the process-level integration tests in
// `tests/test_auth_enforcement.py` (which spawn the real binary with a truly
// unset environment).
#[cfg(test)]
thread_local! {
    static TEST_REQUIRE_OIDC: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

#[cfg(test)]
fn require_oidc() -> bool {
    TEST_REQUIRE_OIDC.with(std::cell::Cell::get)
}

/// Bind the envelope's self-asserted `principal`/`tenant`/`roles`/`scopes` to
/// a verified OIDC bearer token when the primary protocol is configured for
/// external identity verification.
///
/// Fail-closed and config-gated:
/// - No issuer configured (`primary_oidc_validator` returns `Ok(None)`) ⇒
///   `Ok(())` ONLY when the MANDATORY-OIDC posture (`EPISTEMIC_GRAPH_REQUIRE_OIDC`,
///   see [`require_oidc`]) has been explicitly opted out of — the pre-
///   2026-07-22 HMAC-only behavior for unauthenticated local/dev deployments.
///   Since 2026-07-22 the posture defaults ON, so the common case (no issuer,
///   no opt-out) is a hard, fail-closed rejection rather than a silent
///   downgrade to shared-secret HMAC alone.
/// - Issuer configured ⇒ the envelope MUST carry a non-empty `oidc_token`
///   that independently RSA/JWKS-verifies (signature, issuer, audience, not
///   expired — `JwtValidator::validate_claims`), AND whose claims agree with
///   the envelope's asserted principal/tenant/roles/scopes. Any absence or
///   mismatch is rejected.
///
/// `agent_id` is deliberately NOT compared here: it is the possibly-delegated
/// *effective* actor, while the OIDC token's `sub` anchors the raw calling
/// *principal*. `validate_context_claims` already enforces
/// `principal == agent_id` (no delegation) or a valid delegation chain from
/// `principal` to `agent_id`, so binding `principal` to the verified subject
/// transitively covers the common non-delegated case and leaves delegation
/// authority exactly where the rest of this module already places it.
/// **Flagged for human review**: whether a delegated request should
/// additionally require the token to assert an authority-to-delegate claim
/// is a policy question this pass did not have enough context to decide, so
/// today a verified principal may delegate exactly as before OIDC binding
/// was added.
#[cfg(feature = "oidc")]
fn bind_verified_identity(
    claims: &RequestContextClaims,
    oidc_token: Option<&str>,
) -> Result<(), String> {
    let Some(validator) = primary_oidc_validator()? else {
        // No OIDC verifier is configured. Default posture: preserve today's
        // HMAC-only behavior. MANDATORY-OIDC posture: fail closed — a
        // deployment that demanded OIDC must never fall back to shared-secret
        // HMAC just because a verifier was not (or could not be) configured.
        if require_oidc() {
            return Err(
                "EPISTEMIC_GRAPH_REQUIRE_OIDC is set but no OIDC issuer/JWKS is \
                 configured; refusing HMAC-only identity (fail-closed)"
                    .to_string(),
            );
        }
        return Ok(());
    };
    let token = oidc_token
        .map(str::trim)
        .filter(|t| !t.is_empty())
        .ok_or_else(|| "request context requires a verified OIDC bearer token".to_string())?;
    let verified = validator
        .validate_claims(token)
        .ok_or_else(|| "OIDC bearer token failed verification".to_string())?;
    if verified.subject != claims.principal {
        return Err(
            "request context principal does not match the verified OIDC subject".to_string(),
        );
    }
    // A tenant claim is required, not merely compared-if-present: an absent
    // tenant claim is proof of nothing, and this codebase's own equivalent
    // boundary (agent-utilities' `_mint_graph_session`) already treats a
    // missing verified tenant as a hard failure rather than a silent
    // pass-through.
    let tenant = verified
        .tenant
        .as_deref()
        .ok_or_else(|| "verified OIDC token is missing a tenant claim".to_string())?;
    if tenant != claims.tenant {
        return Err(
            "request context tenant does not match the verified OIDC tenant claim".to_string(),
        );
    }
    for role in &claims.roles {
        if !verified.roles.contains(role) {
            return Err(format!("request context asserts unverified role '{role}'"));
        }
    }
    for scope in &claims.scopes {
        if !verified.scopes.contains(scope) {
            return Err(format!(
                "request context asserts unverified scope '{scope}'"
            ));
        }
    }
    Ok(())
}

#[cfg(not(feature = "oidc"))]
fn bind_verified_identity(
    _claims: &RequestContextClaims,
    _oidc_token: Option<&str>,
) -> Result<(), String> {
    // This build has no `oidc` feature, so no verifier can ever exist. Same
    // shared `require_oidc()` posture as the `oidc`-feature variant above:
    // SECURE BY DEFAULT since 2026-07-22 — a build lacking the `oidc` feature
    // altogether must fail closed rather than silently accept HMAC-only
    // identity, unless an operator has explicitly opted out.
    if require_oidc() {
        return Err(
            "EPISTEMIC_GRAPH_REQUIRE_OIDC requires OIDC identity binding (the \
             secure-by-default posture) but this build lacks the `oidc` \
             feature; refusing HMAC-only identity (fail-closed). Either \
             deploy a build with the `oidc` feature (part of the default \
             `full` build) and configure EPISTEMIC_GRAPH_OIDC_JWT_ISSUER, or \
             set EPISTEMIC_GRAPH_REQUIRE_OIDC=false to explicitly opt out."
                .to_string(),
        );
    }
    Ok(())
}

fn verify_envelope_v2_with(
    secret: &str,
    req: &Request,
    policy: &RequestContextPolicy,
    replay: &dyn ReplayLedger,
) -> Result<VerifiedRequestContext, String> {
    let envelope = decode_envelope_v2(req)?;
    let got_mac = hex::decode(&envelope.mac).map_err(|_| "Authentication failed".to_string())?;
    let params = VerifiedEnvelopeParams {
        context: &envelope.context,
        timestamp: envelope.timestamp,
        nonce: &envelope.nonce,
        idempotency_key: &envelope.idempotency_key,
    };
    let mac =
        envelope_v2_mac(secret, req, &params).ok_or_else(|| "Authentication failed".to_string())?;
    if mac.verify_slice(&got_mac).is_err() {
        return Err("Authentication failed".to_string());
    }

    let now = now_secs();
    let skew = envelope_skew_secs();
    if now.abs_diff(envelope.timestamp) > skew {
        return Err("request timestamp is outside the allowed clock-skew window".to_string());
    }
    validate_context_claims(req, &envelope.context, policy)?;
    bind_verified_identity(&envelope.context, envelope.oidc_token.as_deref())?;
    if envelope.idempotency_key.trim().is_empty() {
        return Err("request idempotency key must not be empty".to_string());
    }
    if envelope.nonce.is_empty() || !replay.check_and_record(&envelope.nonce, now, skew)? {
        return Err("nonce already used (replay rejected)".to_string());
    }

    Ok(VerifiedRequestContext::from_verified_claims(
        envelope.context,
        envelope.idempotency_key,
    ))
}

// ── policy entry point ─────────────────────────────────────────────────────

/// Top-level authentication gate. Only the current verified context envelope
/// is accepted.
pub(crate) fn verify_request(
    secret: &str,
    req: &Request,
) -> Result<VerifiedRequestContext, String> {
    verify_request_with_security_dir(secret, req, None)
}

pub(super) fn validate_verified_context_startup(
    secret: &str,
    state_dir: Option<&str>,
) -> Result<(), String> {
    if secret.is_empty() {
        return Err(
            "secure request context requires a non-empty authentication secret".to_string(),
        );
    }
    request_context_policy()?;
    durable_replay_ledger(state_dir)?;
    signer_registry()?;
    // A partially configured OIDC identity binding (issuer set without
    // audience/JWKS URL) fails server startup here, same as every other
    // secure-context config error, rather than lazily on the first request.
    #[cfg(feature = "oidc")]
    let validator = primary_oidc_validator()?;
    #[cfg(not(feature = "oidc"))]
    let validator: Option<()> = None;
    // MANDATORY-OIDC posture (secure by default since 2026-07-22): if the
    // deployment has not explicitly opted out via
    // `EPISTEMIC_GRAPH_REQUIRE_OIDC=false`, an unconfigured (or unbuildable —
    // no `oidc` feature) OIDC verifier must fail server STARTUP, not merely
    // the first request. This turns a silently-inert production deployment
    // into an immediate, loud boot failure — the same operator experience as
    // the missing-auth-secret / missing-persist-dir gates above it.
    if require_oidc() && validator.is_none() {
        return Err(
            "EPISTEMIC_GRAPH_REQUIRE_OIDC requires OIDC identity binding (the \
             secure-by-default posture) but no usable OIDC verifier is \
             configured — set EPISTEMIC_GRAPH_OIDC_JWT_ISSUER (plus its \
             audience and JWKS URL, or the shared OIDC_ISSUER/OIDC_AUDIENCE) \
             pointing at your Keycloak realm, or set \
             EPISTEMIC_GRAPH_REQUIRE_OIDC=false to explicitly opt out for \
             local/dev use"
                .to_string(),
        );
    }
    Ok(())
}

pub(crate) fn verify_request_with_security_dir(
    secret: &str,
    req: &Request,
    state_dir: Option<&str>,
) -> Result<VerifiedRequestContext, String> {
    if secret.is_empty() {
        return Err(
            "secure request context requires a non-empty authentication secret".to_string(),
        );
    }
    if !req.auth_token.starts_with(ENVELOPE_V2_PREFIX) {
        return Err("Authentication failed".to_string());
    }
    let policy = request_context_policy()?;
    let replay = durable_replay_ledger(state_dir)?;
    verify_envelope_v2_with(secret, req, policy, replay)
}

// ── Detached identity / multisig verification ─────────────────────────────

#[derive(Debug, Default)]
struct SignerKeyRegistry {
    keys: BTreeMap<String, String>,
}

impl SignerKeyRegistry {
    fn from_json(raw: &str) -> Result<Self, String> {
        let keys: BTreeMap<String, String> = serde_json::from_str(raw)
            .map_err(|_| "EPISTEMIC_GRAPH_SIGNER_KEYS_JSON is not a string map".to_string())?;
        if keys.is_empty()
            || keys
                .iter()
                .any(|(signer, key)| signer.trim().is_empty() || key.is_empty())
        {
            return Err(
                "EPISTEMIC_GRAPH_SIGNER_KEYS_JSON must contain non-empty signer keys".to_string(),
            );
        }
        Ok(SignerKeyRegistry { keys })
    }

    fn verify(&self, signature: &str, digest: &[u8]) -> Result<String, String> {
        let (signer, tag) = signature
            .rsplit_once(':')
            .ok_or_else(|| "signature must use signer_id:hex_hmac format".to_string())?;
        if signer.is_empty() || tag.is_empty() {
            return Err("signature must use signer_id:hex_hmac format".to_string());
        }
        let key = self
            .keys
            .get(signer)
            .ok_or_else(|| format!("signature uses untrusted signer '{signer}'"))?;
        let tag = hex::decode(tag).map_err(|_| "signature HMAC is not valid hex".to_string())?;
        let mut mac = HmacSha256::new_from_slice(key.as_bytes())
            .map_err(|_| "signer key is invalid".to_string())?;
        mac.update(digest);
        mac.verify_slice(&tag)
            .map_err(|_| format!("signature from '{signer}' did not verify"))?;
        Ok(signer.to_string())
    }

    fn verified_unique_count(&self, signatures: &[String], digest: &[u8]) -> Result<usize, String> {
        let mut signers = HashSet::new();
        for signature in signatures {
            signers.insert(self.verify(signature, digest)?);
        }
        Ok(signers.len())
    }
}

#[cfg(not(test))]
fn signer_registry() -> Result<&'static SignerKeyRegistry, String> {
    static REGISTRY: OnceLock<Result<SignerKeyRegistry, String>> = OnceLock::new();
    match REGISTRY.get_or_init(|| {
        let raw = std::env::var("EPISTEMIC_GRAPH_SIGNER_KEYS_JSON").map_err(|_| {
            "verified identity operations require EPISTEMIC_GRAPH_SIGNER_KEYS_JSON".to_string()
        })?;
        SignerKeyRegistry::from_json(&raw)
    }) {
        Ok(registry) => Ok(registry),
        Err(message) => Err(message.clone()),
    }
}

#[cfg(test)]
fn signer_registry() -> Result<&'static SignerKeyRegistry, String> {
    static REGISTRY: OnceLock<SignerKeyRegistry> = OnceLock::new();
    Ok(REGISTRY.get_or_init(|| SignerKeyRegistry {
        keys: ["system", "root", "alice", "priv"]
            .into_iter()
            .map(|signer| {
                (
                    signer.to_string(),
                    "rust-unit-operation-signer-key".to_string(),
                )
            })
            .collect(),
    }))
}

fn digest_operation(
    domain: &str,
    context: &VerifiedRequestContext,
    graph: &str,
    body: &[u8],
) -> Vec<u8> {
    Sha256::digest(build_context_operation_signature_bytes(
        domain,
        &context.claims,
        &context.idempotency_key,
        graph,
        body,
    ))
    .to_vec()
}

fn register_identity_digest(
    context: &VerifiedRequestContext,
    graph: &str,
    agent_id: &str,
    role: &AgentRole,
    teams: &[String],
    roles: &[String],
) -> Vec<u8> {
    let method = Method::RegisterIdentity {
        agent_id: agent_id.to_string(),
        role: role.clone(),
        teams: teams.to_vec(),
        signature: String::new(),
        roles: roles.to_vec(),
    };
    digest_operation(
        "eg-register-identity-v2",
        context,
        graph,
        &method.canonical_body_bytes(),
    )
}

fn multisig_mutation_digest(
    context: &VerifiedRequestContext,
    graph: &str,
    threshold: usize,
    mutation_type: &str,
    query: &str,
) -> Vec<u8> {
    let method = Method::ApplyMultisigMutation {
        signatures: Vec::new(),
        threshold,
        mutation_type: mutation_type.to_string(),
        query: query.to_string(),
    };
    digest_operation(
        "eg-multisig-mutation-v2",
        context,
        graph,
        &method.canonical_body_bytes(),
    )
}

pub(crate) fn verify_register_identity_signature(
    context: &VerifiedRequestContext,
    graph: &str,
    agent_id: &str,
    role: &AgentRole,
    teams: &[String],
    roles: &[String],
    signature: &str,
) -> Result<(), String> {
    let digest = register_identity_digest(context, graph, agent_id, role, teams, roles);
    let signer = signer_registry()?.verify(signature, &digest)?;
    if signer != context.principal() {
        return Err("identity registration signer does not match verified principal".to_string());
    }
    Ok(())
}

pub(crate) fn verify_multisig_mutation_signatures(
    context: &VerifiedRequestContext,
    graph: &str,
    signatures: &[String],
    threshold: usize,
    mutation_type: &str,
    query: &str,
) -> Result<(), String> {
    if threshold == 0 {
        return Err("multisig threshold must be greater than zero".to_string());
    }
    let digest = multisig_mutation_digest(context, graph, threshold, mutation_type, query);
    let verified_signers = signer_registry()?.verified_unique_count(signatures, &digest)?;
    if verified_signers < threshold {
        return Err(format!(
            "insufficient unique verified signers: {} < {threshold}",
            verified_signers
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::Method;

    const SECRET: &str = "request-security-test-secret";

    /// Resets `TEST_REQUIRE_OIDC` when dropped so a posture override never
    /// leaks into a later test on the same thread. Resets to `false`,
    /// matching `TEST_REQUIRE_OIDC`'s own default (see its definition) — the
    /// test harness's baseline, NOT production's.
    struct RequireOidcGuard;

    impl Drop for RequireOidcGuard {
        fn drop(&mut self) {
            TEST_REQUIRE_OIDC.with(|cell| cell.set(false));
        }
    }

    /// Turn the `EPISTEMIC_GRAPH_REQUIRE_OIDC` posture ON for this test —
    /// simulates a deployment that either left it at its real (secure)
    /// default or explicitly set it truthy.
    fn require_oidc_on() -> RequireOidcGuard {
        TEST_REQUIRE_OIDC.with(|cell| cell.set(true));
        RequireOidcGuard
    }

    /// Turn the `EPISTEMIC_GRAPH_REQUIRE_OIDC` posture OFF for this test —
    /// the explicit, deliberate, documented opt-out
    /// (`EPISTEMIC_GRAPH_REQUIRE_OIDC=false`) a real local/dev deployment
    /// must type to restore pre-2026-07-22 HMAC-only-permitted behavior. A
    /// no-op against `TEST_REQUIRE_OIDC`'s own default, kept for tests that
    /// want to state the opt-out explicitly rather than rely on the harness
    /// default coinciding with it.
    fn require_oidc_off() -> RequireOidcGuard {
        TEST_REQUIRE_OIDC.with(|cell| cell.set(false));
        RequireOidcGuard
    }

    /// Resets `TEST_NODE_BINDING_MODE` to its default (`Warn`) when dropped,
    /// same rationale as `RequireOidcGuard` above it: a posture override must
    /// never leak into a later test that happens to reuse the same pooled
    /// libtest thread.
    struct NodeBindingModeGuard;

    impl Drop for NodeBindingModeGuard {
        fn drop(&mut self) {
            TEST_NODE_BINDING_MODE.with(|cell| cell.set(NodeBindingMode::Warn));
        }
    }

    fn node_binding_mode(mode: NodeBindingMode) -> NodeBindingModeGuard {
        TEST_NODE_BINDING_MODE.with(|cell| cell.set(mode));
        NodeBindingModeGuard
    }

    fn ping_request(id: u64, graph: &str, auth_token: String) -> Request {
        Request {
            id,
            graph: graph.to_string(),
            auth_token,
            agent_id: None,
            method: Method::Ping,
        }
    }

    fn verified_claims() -> RequestContextClaims {
        RequestContextClaims {
            principal: "agent:planner".into(),
            tenant: "tenant-a".into(),
            audience: "engine".into(),
            agent_id: "agent:planner".into(),
            roles: vec!["planner".into()],
            scopes: vec!["graph:read".into()],
            policy_version: "policy-7".into(),
            delegation: vec![],
            node: None,
            priority: None,
        }
    }

    fn verified_policy() -> RequestContextPolicy {
        RequestContextPolicy {
            expected_audience: "engine".into(),
            expected_tenant: "tenant-a".into(),
            expected_policy_version: "policy-7".into(),
        }
    }

    fn memory_replay() -> ReplayCache {
        ReplayCache {
            seen: Mutex::new(HashMap::new()),
        }
    }

    fn signed_v2_with_claims(id: u64, nonce: &str, claims: RequestContextClaims) -> Request {
        let mut req = ping_request(id, "tenant-a-graph", String::new());
        req.agent_id = Some(claims.agent_id.clone());
        req.auth_token = compute_verified_envelope_token(
            SECRET,
            &req,
            &VerifiedEnvelopeParams {
                context: &claims,
                timestamp: now_secs(),
                nonce,
                idempotency_key: "idem-v2",
            },
        );
        req
    }

    fn signed_v2(id: u64, nonce: &str) -> Request {
        signed_v2_with_claims(id, nonce, verified_claims())
    }

    #[test]
    fn v2_returns_effective_agent_only_after_context_policy_verifies() {
        // Not an identity-binding test: exercises HMAC-envelope-derived
        // policy/scope authority, so it deliberately opts out of the
        // mandatory-OIDC posture (no `oidc_token` is ever set on this
        // envelope). See `oidc_binding` below for the identity-binding
        // security tests.
        let _opt_out = require_oidc_off();
        let req = signed_v2(501, "v2-context-ok");
        let context =
            verify_envelope_v2_with(SECRET, &req, &verified_policy(), &memory_replay()).unwrap();
        assert_eq!(context.agent_id(), "agent:planner");
        assert_eq!(context.claims().tenant, "tenant-a");
        assert!(context.allows_action("graph:read"));
        assert!(!context.allows_action("graph:write"));
    }

    #[test]
    fn v2_coarse_kg_scopes_are_policy_aware_and_admin_safe() {
        // Not an identity-binding test: exercises scope-authority policy over
        // plain HMAC envelopes (no `oidc_token`), so it deliberately opts out
        // of the mandatory-OIDC posture.
        let _opt_out = require_oidc_off();
        let context_for = |scope: &str, id: u64, nonce: &str| {
            let mut claims = verified_claims();
            claims.scopes = vec![scope.into()];
            let req = signed_v2_with_claims(id, nonce, claims);
            verify_envelope_v2_with(SECRET, &req, &verified_policy(), &memory_replay()).unwrap()
        };

        let read = context_for("kg:read", 510, "kg-read");
        assert!(read.allows_method("node:read", false));
        assert!(!read.allows_method("node:write", true));
        assert!(!read.allows_method("graph:admin", false));

        let write = context_for("kg:write", 511, "kg-write");
        assert!(write.allows_method("node:read", false));
        assert!(write.allows_method("work:write", true));
        assert!(!write.allows_method("graph:admin", true));
        assert!(!write.allows_method("service:control", true));
        assert!(!write.allows_method("security:audit", false));
        assert!(!write.allows_method("admin:cluster-read", false));

        let admin = context_for("kg:admin", 512, "kg-admin");
        assert!(admin.allows_method("graph:admin", true));
        assert!(admin.allows_method("service:control", true));

        let exact = context_for("work:write", 513, "fine-work-write");
        assert!(exact.allows_method("work:write", true));
        assert!(!exact.allows_method("node:write", true));

        let wildcard = context_for("graph:*", 514, "graph-wildcard");
        assert!(wildcard.allows_action("graph:read"));
        assert!(wildcard.allows_action("graph:node:read"));
        assert!(!wildcard.allows_action("work:read"));
    }

    #[test]
    fn identity_bootstrap_requires_exact_self_scope_without_delegation() {
        let mut claims = verified_claims();
        claims.scopes = vec!["security:bootstrap".into()];
        let exact = VerifiedRequestContext::from_verified_claims(claims.clone(), "exact".into());
        assert!(exact.allows_identity_bootstrap());

        claims.scopes.push("kg:admin".into());
        assert!(
            !VerifiedRequestContext::from_verified_claims(claims.clone(), "extra".into())
                .allows_identity_bootstrap()
        );
        claims.scopes.pop();
        claims.delegation.push("delegate".into());
        assert!(
            !VerifiedRequestContext::from_verified_claims(claims.clone(), "delegated".into())
                .allows_identity_bootstrap()
        );
        claims.delegation.clear();
        claims.principal = "different-principal".into();
        assert!(
            !VerifiedRequestContext::from_verified_claims(claims, "not-self".into())
                .allows_identity_bootstrap()
        );
    }

    #[test]
    fn generated_negative_context_matrix_covers_every_protocol_method() {
        let denied = VerifiedRequestContext::from_verified_claims(
            RequestContextClaims {
                scopes: vec![],
                ..verified_claims()
            },
            "negative-matrix".into(),
        );
        let mut wrong_tenant = verified_claims();
        wrong_tenant.tenant = "tenant-b".into();
        let request = ping_request(599, "tenant-a-graph", String::new());

        for (method, policy, _) in eg_capabilities::ALL_METHODS {
            assert!(
                !denied.allows_method(policy.authz_action, policy.mutates),
                "{method}: a verified context with no scopes must be denied before row access"
            );
            let tenant_error = validate_context_claims(&request, &wrong_tenant, &verified_policy())
                .expect_err("cross-tenant context must fail before dispatch");
            assert!(
                tenant_error.contains("tenant"),
                "{method}: cross-tenant context was not rejected"
            );
        }
        assert!(
            eg_capabilities::ALL_METHODS.len() >= 350,
            "negative matrix must track the exhaustive protocol inventory"
        );
    }

    #[test]
    fn v2_rejects_request_agent_that_conflicts_with_signed_context() {
        let mut req = signed_v2(502, "v2-agent-mismatch");
        req.agent_id = Some("agent:attacker".into());
        let error = verify_envelope_v2_with(SECRET, &req, &verified_policy(), &memory_replay())
            .unwrap_err();
        assert!(error.contains("agent_id"));
    }

    #[test]
    fn v2_rejects_valid_mac_for_wrong_configured_audience() {
        let mut claims = verified_claims();
        claims.audience = "other-service".into();
        let req = signed_v2_with_claims(504, "v2-audience-mismatch", claims);
        let error = verify_envelope_v2_with(SECRET, &req, &verified_policy(), &memory_replay())
            .unwrap_err();
        assert!(error.contains("audience"));
    }

    #[test]
    fn v2_rejects_cross_tenant_context_before_dispatch() {
        let mut claims = verified_claims();
        claims.tenant = "tenant-b".into();
        let req = signed_v2_with_claims(505, "v2-tenant-mismatch", claims);
        let error = verify_envelope_v2_with(SECRET, &req, &verified_policy(), &memory_replay())
            .unwrap_err();
        assert!(error.contains("tenant"));
    }

    #[test]
    fn v2_rejects_stale_policy_version_before_dispatch() {
        let mut claims = verified_claims();
        claims.policy_version = "policy-6".into();
        let req = signed_v2_with_claims(506, "v2-stale-policy", claims);
        let error = verify_envelope_v2_with(SECRET, &req, &verified_policy(), &memory_replay())
            .unwrap_err();
        assert!(error.contains("policy version"));
    }

    #[test]
    fn current_envelope_replay_is_rejected() {
        // Not an identity-binding test: exercises nonce/replay rejection over
        // a plain HMAC envelope (no `oidc_token`), so it deliberately opts
        // out of the mandatory-OIDC posture.
        let _opt_out = require_oidc_off();
        let req = signed_v2(503, "v2-replay");
        let replay = memory_replay();
        verify_envelope_v2_with(SECRET, &req, &verified_policy(), &replay).unwrap();
        let error = verify_envelope_v2_with(SECRET, &req, &verified_policy(), &replay).unwrap_err();
        assert!(error.contains("replay"));
    }

    // ── ADR-3 / W1.9: node-bound envelopes ──────────────────────────────────

    #[test]
    fn v2_rejects_envelope_bound_to_a_different_node_before_nonce_check() {
        // `node_identity()` in test builds is the fixed constant "node-a".
        let _opt_out = require_oidc_off();
        let _mode = node_binding_mode(NodeBindingMode::Warn);
        let mut claims = verified_claims();
        claims.node = Some("node-b".into());
        let req = signed_v2_with_claims(520, "v2-node-mismatch", claims);
        let replay = memory_replay();
        let error = verify_envelope_v2_with(SECRET, &req, &verified_policy(), &replay).unwrap_err();
        assert!(error.starts_with("NODE_MISMATCH"), "got: {error}");
        // The nonce must NOT have been consumed by the rejected attempt --
        // replaying the EXACT same envelope produces the SAME node-mismatch
        // error again, not "replay". That is the proof this check runs
        // before the nonce/replay ledger, exactly as ADR-3 requires.
        let error_again =
            verify_envelope_v2_with(SECRET, &req, &verified_policy(), &replay).unwrap_err();
        assert!(
            error_again.starts_with("NODE_MISMATCH"),
            "got: {error_again}"
        );
    }

    #[test]
    fn v2_same_node_replay_is_still_rejected_by_the_ledger() {
        let _opt_out = require_oidc_off();
        let _mode = node_binding_mode(NodeBindingMode::Warn);
        let mut claims = verified_claims();
        claims.node = Some("node-a".into()); // matches node_identity() in tests
        let req = signed_v2_with_claims(521, "v2-node-match-replay", claims);
        let replay = memory_replay();
        verify_envelope_v2_with(SECRET, &req, &verified_policy(), &replay).unwrap();
        let error = verify_envelope_v2_with(SECRET, &req, &verified_policy(), &replay).unwrap_err();
        assert!(error.contains("replay"), "got: {error}");
    }

    // ── W2.4: engine-native QoS lanes — the priority claim round-trips + is MAC-covered ──

    #[test]
    fn v2_priority_claim_round_trips_through_verification() {
        let _opt_out = require_oidc_off();
        // Absent priority ⇒ None (an un-upgraded client is unaffected).
        let plain = signed_v2(540, "v2-prio-absent");
        let ctx_plain =
            verify_envelope_v2_with(SECRET, &plain, &verified_policy(), &memory_replay()).unwrap();
        assert_eq!(ctx_plain.priority(), None);

        // A present priority claim verifies and is readable post-verification, so the
        // QoS gate can classify off it.
        let mut claims = verified_claims();
        claims.priority = Some("background_ingestion".into());
        let req = signed_v2_with_claims(541, "v2-prio-present", claims);
        let ctx =
            verify_envelope_v2_with(SECRET, &req, &verified_policy(), &memory_replay()).unwrap();
        assert_eq!(ctx.priority(), Some("background_ingestion"));
    }

    #[test]
    fn v2_priority_claim_is_mac_covered_and_cannot_be_forged() {
        let _opt_out = require_oidc_off();
        // A principal signs itself as background_ingestion ...
        let mut claims = verified_claims();
        claims.priority = Some("background_ingestion".into());
        let mut req = signed_v2_with_claims(542, "v2-prio-forge", claims);

        // ... then tampers with ONLY the priority field of the decoded envelope,
        // trying to jump to the interactive lane WITHOUT re-signing (it holds no
        // signing secret). The MAC no longer covers the mutated claim, so
        // verification must fail — the noisy-neighbor defense cannot be forged.
        let mut envelope = decode_envelope_v2(&req).unwrap();
        assert_eq!(envelope.context.priority.as_deref(), Some("background_ingestion"));
        envelope.context.priority = Some("interactive".into());
        let json = serde_json::to_vec(&envelope).unwrap();
        req.auth_token = format!("{ENVELOPE_V2_PREFIX}{}", hex::encode(json));

        let error =
            verify_envelope_v2_with(SECRET, &req, &verified_policy(), &memory_replay()).unwrap_err();
        assert!(
            error.contains("Authentication failed") || error.to_lowercase().contains("mac"),
            "a forged priority class must fail MAC verification, got: {error}"
        );
    }

    #[test]
    fn v2_old_client_without_node_claim_is_accepted_under_warn_mode() {
        let _opt_out = require_oidc_off();
        let _mode = node_binding_mode(NodeBindingMode::Warn);
        let claims = verified_claims(); // node: None -- an old, node-unaware client
        let req = signed_v2_with_claims(522, "v2-old-client-warn", claims);
        let context =
            verify_envelope_v2_with(SECRET, &req, &verified_policy(), &memory_replay()).unwrap();
        assert_eq!(context.agent_id(), "agent:planner");
    }

    #[test]
    fn absent_node_claim_behavior_matches_require_node_binding_mode() {
        let request = ping_request(599, "tenant-a-graph", String::new());
        let policy = verified_policy();

        {
            let _mode = node_binding_mode(NodeBindingMode::Off);
            validate_context_claims(&request, &verified_claims(), &policy)
                .expect("off must accept an absent claim silently");
        }
        {
            let _mode = node_binding_mode(NodeBindingMode::Warn);
            validate_context_claims(&request, &verified_claims(), &policy)
                .expect("warn must accept an absent claim (logged once per principal)");
        }
        {
            let _mode = node_binding_mode(NodeBindingMode::On);
            let error = validate_context_claims(&request, &verified_claims(), &policy)
                .expect_err("on must reject an absent claim (fail closed)");
            assert!(error.starts_with("NODE_MISMATCH"), "got: {error}");
        }
    }

    #[test]
    fn present_node_claim_is_exact_matched_regardless_of_binding_mode() {
        let request = ping_request(600, "tenant-a-graph", String::new());
        let policy = verified_policy();
        let mut matching = verified_claims();
        matching.node = Some("node-a".into());
        let mut mismatched = verified_claims();
        mismatched.node = Some("node-b".into());

        for mode in [
            NodeBindingMode::Off,
            NodeBindingMode::Warn,
            NodeBindingMode::On,
        ] {
            let _mode = node_binding_mode(mode);
            validate_context_claims(&request, &matching, &policy)
                .unwrap_or_else(|e| panic!("{mode:?}: a matching node claim must pass: {e}"));
            let error = validate_context_claims(&request, &mismatched, &policy)
                .expect_err("a wrong node claim must be rejected in every mode");
            assert!(error.starts_with("NODE_MISMATCH"), "{mode:?}: got: {error}");
        }
    }

    #[test]
    fn empty_node_claim_is_rejected() {
        let request = ping_request(601, "tenant-a-graph", String::new());
        let mut claims = verified_claims();
        claims.node = Some(String::new());
        let error = validate_context_claims(&request, &claims, &verified_policy()).unwrap_err();
        assert!(error.contains("node claim must not be empty"));
    }

    #[test]
    fn env_flag_explicit_defaults_secure_when_unset_or_unrecognized() {
        // Direct unit coverage of the exact primitive `require_oidc()`'s real
        // (`#[cfg(not(test))]`) implementation folds via `.unwrap_or(true)` —
        // the thing that cannot be exercised through `TEST_REQUIRE_OIDC`
        // (see its definition) because that thread-local is a *simulation*
        // used by request-path tests, not the real env-reading code path.
        // Uses a private probe name (never read by production code) so this
        // can never race with another parallel test or a real deployment
        // env var of the same name.
        const PROBE: &str = "EPISTEMIC_GRAPH_REQUIRE_OIDC_TEST_PROBE_env_flag_explicit";

        std::env::remove_var(PROBE);
        assert_eq!(
            env_flag_explicit(PROBE),
            None,
            "unset must be None so callers can apply THEIR OWN default"
        );
        // The exact fold `require_oidc()` performs: unset ⇒ secure (true).
        assert!(
            env_flag_explicit(PROBE).unwrap_or(true),
            "production's require_oidc() must default to true (OIDC required) when unset"
        );

        std::env::set_var(PROBE, "not-a-recognized-value");
        assert_eq!(
            env_flag_explicit(PROBE),
            None,
            "an unrecognized value must not silently opt out of the secure default"
        );
        assert!(
            env_flag_explicit(PROBE).unwrap_or(true),
            "a typo'd opt-out must still resolve to the secure default"
        );

        for falsy in ["false", "0", "no", "off", "FALSE", " False "] {
            std::env::set_var(PROBE, falsy);
            assert_eq!(
                env_flag_explicit(PROBE),
                Some(false),
                "{falsy:?} must be a recognized, deliberate opt-out"
            );
        }
        for truthy in ["true", "1", "yes", "on", "TRUE"] {
            std::env::set_var(PROBE, truthy);
            assert_eq!(
                env_flag_explicit(PROBE),
                Some(true),
                "{truthy:?} must be truthy"
            );
        }

        std::env::remove_var(PROBE);
    }

    fn detached_signature(signer: &str, key: &str, digest: &[u8]) -> String {
        let mut mac = HmacSha256::new_from_slice(key.as_bytes()).unwrap();
        mac.update(digest);
        format!("{signer}:{}", hex::encode(mac.finalize().into_bytes()))
    }

    #[test]
    fn multisig_registry_counts_unique_signers_and_binds_exact_digest() {
        let registry = SignerKeyRegistry {
            keys: BTreeMap::from([
                ("signer:a".into(), "key-a".into()),
                ("signer:b".into(), "key-b".into()),
            ]),
        };
        let context =
            VerifiedRequestContext::from_verified_claims(verified_claims(), "idem".into());
        let digest = multisig_mutation_digest(&context, "g", 2, "apply", "MATCH (n) RETURN n");
        let a = detached_signature("signer:a", "key-a", &digest);
        let b = detached_signature("signer:b", "key-b", &digest);

        assert_eq!(
            registry
                .verified_unique_count(&[a.clone(), a.clone(), b.clone()], &digest)
                .unwrap(),
            2
        );
        let changed = multisig_mutation_digest(&context, "g", 2, "apply", "MATCH (m) RETURN m");
        assert!(registry.verify(&a, &changed).is_err());
    }

    #[test]
    fn identity_registration_signature_binds_full_identity_record() {
        let registry = SignerKeyRegistry {
            keys: BTreeMap::from([("agent:planner".into(), "planner-key".into())]),
        };
        let context =
            VerifiedRequestContext::from_verified_claims(verified_claims(), "register-1".into());
        let digest = register_identity_digest(
            &context,
            "g",
            "agent:new",
            &AgentRole::Agent,
            &["team-a".into()],
            &["reader".into()],
        );
        let signature = detached_signature("agent:planner", "planner-key", &digest);
        assert_eq!(
            registry.verify(&signature, &digest).unwrap(),
            "agent:planner"
        );

        let changed = register_identity_digest(
            &context,
            "g",
            "agent:new",
            &AgentRole::System,
            &["team-a".into()],
            &["reader".into()],
        );
        assert!(registry.verify(&signature, &changed).is_err());
    }

    #[test]
    fn native_sql_authority_is_keyed_opaque_and_protocol_bound() {
        let pg = VerifiedRequestContext::authenticated_sql_wire_actor(
            "secret-a",
            "pgwire",
            "local-login",
        )
        .unwrap();
        let mysql = VerifiedRequestContext::authenticated_sql_wire_actor(
            "secret-a",
            "mysql-wire",
            "local-login",
        )
        .unwrap();
        let other_key = VerifiedRequestContext::authenticated_sql_wire_actor(
            "secret-b",
            "pgwire",
            "local-login",
        )
        .unwrap();

        assert_eq!(pg.agent_id(), "local-login");
        assert!(!pg.principal().contains("local-login"));
        assert_ne!(pg.principal(), mysql.principal());
        assert_ne!(pg.principal(), other_key.principal());
        assert!(VerifiedRequestContext::authenticated_sql_wire_actor(
            "secret-a",
            "unknown-wire",
            "local-login"
        )
        .is_err());
        assert!(
            VerifiedRequestContext::authenticated_sql_wire_actor("secret-a", "pgwire", "").is_err()
        );
    }

    // ── OIDC identity binding (primary eg2. protocol, feature `oidc`) ─────
    //
    // Mirrors `crate::server::oidc`'s own test suite (same fixture keypair,
    // generated solely for tests via `openssl genrsa` — never used for
    // anything else): valid token binds; tampered/expired/wrong-audience
    // tokens fail closed; a token asserting a different principal/tenant/
    // role/scope than the envelope claims is rejected; a missing token when
    // configured is rejected; and an unconfigured deployment is unaffected.
    #[cfg(feature = "oidc")]
    mod oidc_binding {
        use super::*;
        use jsonwebtoken::{encode, Algorithm as JwtAlgorithm, EncodingKey, Header};

        const TEST_RSA_PRIVATE_KEY_PKCS1_DER_HEX: &str = "308204a30201000282010100be4725fd791744d873c4c82cc04ba74db85707a72581e4773e3f9041531b15ea57dcccda092adecbfa818521f10de4f849de2f6b359a20ad4eeec7da6aa550baf49a8f471089348b5c677a4c3d9b7f027395d3a08fa87345e4f842d3f5e6d9846f139883cb9ed94e1a868f85a741a5cb1262beaa4b395c6f9bc82fc46e65267cd50d7d752d2194b69a03ca41f3c135a9862f48d7697f74e8da8dca840cdf4f2cda9addc48ea6445574ffbc79f23144a520ba9aaa3ea8b549c25a89188a869a8ee7f05a096a66bfa4f49d4b5900f49579e88da8c25da9baea53f93cb69e744e5d80b55a41e0de41449bb437b53b57f6ef179eae0b3815a20b1df65fbdf28fc3b7020301000102820100019495093241f2381b5b62ba3f17f71a1b2785e5bfd700af1e323da027f0e2a6b6a21bdacd16b1110aa746becdc21573c67bf4f2dead700b60761fecd2d3f0040d820c7744f8e419d58e4fcd65a443fd7638f95aad0c1e20fcd23463e44d4d8ddf0a4fa0509c4f7bbeebfd31d95374981232b06e0e5539f7a75895fa50b1c061bcb1816d44e1c9155192cc37707747c6abf0af131a3b7d94a774fdc8a491d949ca0049b5845aca493b71352800d31d6f8d4e6beb352571f1586e9c9184a7a691cc556e53953ac5fc7995fed28d0fd92918b2dac30a4892595f70083f18d42a8768bb76077625bc917b347a8c3ec245db23f0eaaebeff571a7141891df5aa380102818100f6cae082d13337d73a723d4672f5a8b7113dfc820251e05380a672055c27dbab82c044f73fdb5d1a3fce5894fda55e57372fcf5f2704ee0ae927fd73c0e80eead6832d5a5938c3c63e69cab78d53e15b535d8a724e93eadf2d9ad45ce6bd2ae3653d087583fd0c7c8e9dac3c33c1f5bc651a2f69f898c379cc3722a85a163c0102818100c5607fbcc1a5a3ae9fa1a3c2469c17dd6d402515ecc724957d7fec575517254acf1dfc70c915390d8f489fae188c17372548603d442b06ad8195c74f8ee8bf51cfa22a2b4740d9e43e35d1942e4e4be545baf43127910c1c7e983f0f5ff5852f85311a56dc8d27fb1b5f669b0f7e83971f99ada964c1f4c6233299a84666dfb702818100d4186938a417d37eca4111be30e044fe07f870c13ec324fa3e8f4d60a3e1b15d46027d82cc4377512ed2e4b82f00e702277094549f51124f18300117710b3e7ebe9a7fe8acd3271581e02392fa07c39e5c1800fad9e32fb05c1e3b32182f2ce3bec6e4353298d0195febcbf0f53e553572e23d2b62b5cf1126db9f9275d1b40102818001a2a60c4b527303bc60db797d9a477c572e63e045a0f4c5a44f8e06bf36bce15ccbf3ce7f6c0497ff2aebdfc6664abef339214b00a8969a936b49467879a734275341a43027f26638b9bb6dcde06a32911c566f9dd34ed5619b23529e49eb7b944feed6ef66e000ed9e21bc81295c2fc15c459b14b1a2b48d901ac3d129830b0281807b5d9e95bf0e2892ff7ee7251fa14bec34d00c031d216c0f06dfa698407ec750e3d357e800907812a61d90281ce93320ad4a50d33364429710f249b87bc925ba89c5f675ed99229d09399943934811b25f4bac5a6cba9303dcd82ccbd31216092e1b9fe5ab1921188bd3e96256c692602be876e09c919c04735638b19646a658";
        const TEST_RSA_MODULUS_HEX: &str = "BE4725FD791744D873C4C82CC04BA74DB85707A72581E4773E3F9041531B15EA57DCCCDA092ADECBFA818521F10DE4F849DE2F6B359A20AD4EEEC7DA6AA550BAF49A8F471089348B5C677A4C3D9B7F027395D3A08FA87345E4F842D3F5E6D9846F139883CB9ED94E1A868F85A741A5CB1262BEAA4B395C6F9BC82FC46E65267CD50D7D752D2194B69A03CA41F3C135A9862F48D7697F74E8DA8DCA840CDF4F2CDA9ADDC48EA6445574FFBC79F23144A520BA9AAA3EA8B549C25A89188A869A8EE7F05A096A66BFA4F49D4B5900F49579E88DA8C25DA9BAEA53F93CB69E744E5D80B55A41E0DE41449BB437B53B57F6EF179EAE0B3815A20B1DF65FBDF28FC3B7";
        const TEST_RSA_EXPONENT_HEX: &str = "010001";

        const OIDC_ISSUER: &str = "https://identity.example.test/realms/eg";
        const OIDC_AUDIENCE: &str = "epistemic-graph";
        const OIDC_KID: &str = "auth-test-kid";

        /// Resets `TEST_OIDC_VALIDATOR` when dropped — including on an early
        /// `assert!`/`unwrap` panic — so one test can never leak a configured
        /// validator into a later test.
        struct OidcTestGuard;

        impl Drop for OidcTestGuard {
            fn drop(&mut self) {
                TEST_OIDC_VALIDATOR.with(|cell| cell.set(None));
            }
        }

        fn install_test_validator() -> OidcTestGuard {
            let mut keys = HashMap::new();
            let n = hex::decode(TEST_RSA_MODULUS_HEX).unwrap();
            let e = hex::decode(TEST_RSA_EXPONENT_HEX).unwrap();
            keys.insert(
                OIDC_KID.to_string(),
                jsonwebtoken::DecodingKey::from_rsa_raw_components(&n, &e),
            );
            let validator: &'static crate::server::oidc::JwtValidator = Box::leak(Box::new(
                crate::server::oidc::JwtValidator::from_parts(OIDC_ISSUER, OIDC_AUDIENCE, keys),
            ));
            TEST_OIDC_VALIDATOR.with(|cell| cell.set(Some(validator)));
            OidcTestGuard
        }

        fn sign(claims: &serde_json::Value) -> String {
            let mut header = Header::new(JwtAlgorithm::RS256);
            header.kid = Some(OIDC_KID.to_string());
            let der = hex::decode(TEST_RSA_PRIVATE_KEY_PKCS1_DER_HEX).unwrap();
            encode(&header, claims, &EncodingKey::from_rsa_der(&der)).unwrap()
        }

        fn oidc_claims(sub: &str, tenant: &str, roles: &[&str], scope: &str) -> serde_json::Value {
            serde_json::json!({
                "sub": sub,
                "iss": OIDC_ISSUER,
                "aud": OIDC_AUDIENCE,
                "exp": now_secs() + 300,
                "tenant_id": tenant,
                "roles": roles,
                "scope": scope,
            })
        }

        /// Build an `eg2.` envelope carrying `oidc_token` — the same private
        /// machinery `compute_verified_envelope_token` uses, just with the
        /// new field populated (that general-purpose reference signer
        /// deliberately never sets it; see its call site's comment).
        fn envelope_request(
            id: u64,
            nonce: &str,
            claims: RequestContextClaims,
            oidc_token: Option<&str>,
        ) -> Request {
            let mut req = ping_request(id, "tenant-a-graph", String::new());
            req.agent_id = Some(claims.agent_id.clone());
            let params = VerifiedEnvelopeParams {
                context: &claims,
                timestamp: now_secs(),
                nonce,
                idempotency_key: "idem-oidc",
            };
            let mac = envelope_v2_mac(SECRET, &req, &params).unwrap();
            let timestamp = params.timestamp;
            let idempotency_key = params.idempotency_key.to_string();
            let envelope = EnvelopeV2 {
                context: claims,
                timestamp,
                nonce: nonce.to_string(),
                idempotency_key,
                oidc_token: oidc_token.map(str::to_string),
                mac: hex::encode(mac.finalize().into_bytes()),
            };
            let json = serde_json::to_vec(&envelope).unwrap();
            req.auth_token = format!("{ENVELOPE_V2_PREFIX}{}", hex::encode(json));
            req
        }

        fn matching_claims() -> RequestContextClaims {
            RequestContextClaims {
                principal: "agent:planner".into(),
                tenant: "tenant-a".into(),
                audience: "engine".into(),
                agent_id: "agent:planner".into(),
                roles: vec!["kg:read".into()],
                scopes: vec!["kg:read".into()],
                policy_version: "policy-7".into(),
                delegation: vec![],
                node: None,
                priority: None,
            }
        }

        #[test]
        fn valid_oidc_token_binds_verified_identity() {
            let _guard = install_test_validator();
            let token = sign(&oidc_claims(
                "agent:planner",
                "tenant-a",
                &["kg:read"],
                "kg:read kg:write",
            ));
            let req = envelope_request(700, "oidc-valid", matching_claims(), Some(&token));
            let result =
                verify_envelope_v2_with(SECRET, &req, &verified_policy(), &memory_replay());
            assert!(result.is_ok(), "{:?}", result.err());
        }

        #[test]
        fn tampered_oidc_token_fails_closed() {
            let _guard = install_test_validator();
            let mut token = sign(&oidc_claims(
                "agent:planner",
                "tenant-a",
                &["kg:read"],
                "kg:read",
            ));
            token.push('x'); // corrupt the signature segment
            let req = envelope_request(701, "oidc-tampered", matching_claims(), Some(&token));
            let error = verify_envelope_v2_with(SECRET, &req, &verified_policy(), &memory_replay())
                .unwrap_err();
            assert!(error.contains("OIDC"), "{error}");
        }

        #[test]
        fn expired_oidc_token_fails_closed() {
            let _guard = install_test_validator();
            let mut claims = oidc_claims("agent:planner", "tenant-a", &["kg:read"], "kg:read");
            // Comfortably past jsonwebtoken's default 60s leeway (clock-skew
            // tolerance), so this is unambiguously expired rather than sitting
            // on the leeway boundary.
            claims["exp"] = serde_json::json!(now_secs() - 3600);
            let token = sign(&claims);
            let req = envelope_request(702, "oidc-expired", matching_claims(), Some(&token));
            let error = verify_envelope_v2_with(SECRET, &req, &verified_policy(), &memory_replay())
                .unwrap_err();
            assert!(error.contains("OIDC"), "{error}");
        }

        #[test]
        fn wrong_audience_oidc_token_fails_closed() {
            let _guard = install_test_validator();
            let mut claims = oidc_claims("agent:planner", "tenant-a", &["kg:read"], "kg:read");
            claims["aud"] = serde_json::json!("not-this-engine");
            let token = sign(&claims);
            let req = envelope_request(703, "oidc-wrong-aud", matching_claims(), Some(&token));
            let error = verify_envelope_v2_with(SECRET, &req, &verified_policy(), &memory_replay())
                .unwrap_err();
            assert!(error.contains("OIDC"), "{error}");
        }

        #[test]
        fn mismatched_principal_fails_closed() {
            let _guard = install_test_validator();
            // token asserts a DIFFERENT subject than the envelope's principal.
            let token = sign(&oidc_claims(
                "agent:someone-else",
                "tenant-a",
                &["kg:read"],
                "kg:read",
            ));
            let req = envelope_request(
                704,
                "oidc-mismatch-principal",
                matching_claims(),
                Some(&token),
            );
            let error = verify_envelope_v2_with(SECRET, &req, &verified_policy(), &memory_replay())
                .unwrap_err();
            assert!(error.contains("principal"), "{error}");
        }

        #[test]
        fn mismatched_tenant_fails_closed() {
            let _guard = install_test_validator();
            let token = sign(&oidc_claims(
                "agent:planner",
                "tenant-other",
                &["kg:read"],
                "kg:read",
            ));
            let req =
                envelope_request(705, "oidc-mismatch-tenant", matching_claims(), Some(&token));
            let error = verify_envelope_v2_with(SECRET, &req, &verified_policy(), &memory_replay())
                .unwrap_err();
            assert!(error.contains("tenant"), "{error}");
        }

        #[test]
        fn unverified_role_fails_closed() {
            let _guard = install_test_validator();
            // token grants no roles at all; envelope claims the "kg:read" role.
            let token = sign(&oidc_claims("agent:planner", "tenant-a", &[], "kg:read"));
            let req =
                envelope_request(706, "oidc-unverified-role", matching_claims(), Some(&token));
            let error = verify_envelope_v2_with(SECRET, &req, &verified_policy(), &memory_replay())
                .unwrap_err();
            assert!(error.contains("role"), "{error}");
        }

        #[test]
        fn unverified_scope_fails_closed() {
            let _guard = install_test_validator();
            let mut claims = matching_claims();
            claims.scopes = vec!["kg:admin".into()]; // envelope over-claims a scope
            let token = sign(&oidc_claims(
                "agent:planner",
                "tenant-a",
                &["kg:read"],
                "kg:read",
            ));
            let req = envelope_request(707, "oidc-unverified-scope", claims, Some(&token));
            let error = verify_envelope_v2_with(SECRET, &req, &verified_policy(), &memory_replay())
                .unwrap_err();
            assert!(error.contains("scope"), "{error}");
        }

        #[test]
        fn missing_oidc_token_fails_closed_when_configured() {
            let _guard = install_test_validator();
            let req = envelope_request(708, "oidc-missing-token", matching_claims(), None);
            let error = verify_envelope_v2_with(SECRET, &req, &verified_policy(), &memory_replay())
                .unwrap_err();
            assert!(
                error.contains("OIDC") && error.contains("bearer"),
                "{error}"
            );
        }

        #[test]
        fn explicit_opt_out_preserves_pre_2026_07_22_hmac_only_behavior() {
            // No validator installed AND the deliberate, documented opt-out
            // (`EPISTEMIC_GRAPH_REQUIRE_OIDC=false`) is set — mirrors a real
            // local/dev deployment that has consciously typed the escape
            // hatch. An envelope with NO oidc_token must verify exactly as it
            // did before the 2026-07-22 secure-by-default flip. This proves
            // the documented opt-out genuinely works; the complementary proof
            // that the SAME scenario is REJECTED when the posture is ON
            // (production's real default) is
            // `require_oidc_rejects_hmac_only_when_verifier_unconfigured`
            // below — together they show the gate is neither always-on
            // (worthless/unusable) nor always-off (worthless/no security).
            TEST_OIDC_VALIDATOR.with(|cell| cell.set(None));
            let _opt_out = require_oidc_off();
            let req = envelope_request(709, "oidc-unconfigured", matching_claims(), None);
            let result =
                verify_envelope_v2_with(SECRET, &req, &verified_policy(), &memory_replay());
            assert!(result.is_ok(), "{:?}", result.err());
        }

        // ── MANDATORY-OIDC posture (EPISTEMIC_GRAPH_REQUIRE_OIDC) ─────────
        //
        // With the posture ON, an unconfigured verifier must fail closed
        // rather than fall back to HMAC-only, and a configured verifier must
        // still accept a genuinely valid, matching token. Since 2026-07-22
        // "ON" is ALSO production's real, unconfigured default (see
        // `require_oidc()`'s `#[cfg(not(test))]` implementation, exercised
        // directly — not via this thread-local — by
        // `env_flag_explicit_defaults_secure_when_unset_or_unrecognized`
        // below and by `tests/test_auth_enforcement.py`'s process-level
        // spawn tests, both of which touch real environment variables rather
        // than this test-only override).

        #[test]
        fn require_oidc_rejects_hmac_only_when_verifier_unconfigured() {
            // Posture ON but NO validator installed — the exact hole this
            // package closes: today's default silently accepts HMAC-only here.
            TEST_OIDC_VALIDATOR.with(|cell| cell.set(None));
            let _require = require_oidc_on();
            let req = envelope_request(710, "oidc-require-unconfigured", matching_claims(), None);
            let error = verify_envelope_v2_with(SECRET, &req, &verified_policy(), &memory_replay())
                .unwrap_err();
            assert!(error.contains("EPISTEMIC_GRAPH_REQUIRE_OIDC"), "{error}");
        }

        #[test]
        fn require_oidc_rejects_even_with_token_when_verifier_unconfigured() {
            // A token cannot be RSA/JWKS-verified without a configured verifier,
            // so presenting one must not smuggle past the posture either.
            TEST_OIDC_VALIDATOR.with(|cell| cell.set(None));
            let _require = require_oidc_on();
            let token = sign(&oidc_claims(
                "agent:planner",
                "tenant-a",
                &["kg:read"],
                "kg:read",
            ));
            let req = envelope_request(
                711,
                "oidc-require-unconfigured-with-token",
                matching_claims(),
                Some(&token),
            );
            let error = verify_envelope_v2_with(SECRET, &req, &verified_policy(), &memory_replay())
                .unwrap_err();
            assert!(error.contains("EPISTEMIC_GRAPH_REQUIRE_OIDC"), "{error}");
        }

        #[test]
        fn require_oidc_still_accepts_a_valid_matching_token() {
            // Posture ON with a configured verifier and a genuinely valid,
            // subject/tenant/role/scope-matching token still binds and passes.
            let _guard = install_test_validator();
            let _require = require_oidc_on();
            let token = sign(&oidc_claims(
                "agent:planner",
                "tenant-a",
                &["kg:read"],
                "kg:read kg:write",
            ));
            let req = envelope_request(712, "oidc-require-valid", matching_claims(), Some(&token));
            let result =
                verify_envelope_v2_with(SECRET, &req, &verified_policy(), &memory_replay());
            assert!(result.is_ok(), "{:?}", result.err());
        }

        #[test]
        fn require_oidc_rejects_missing_token_with_configured_verifier() {
            // Posture ON, verifier configured, but the envelope carries no
            // token — rejected exactly as the configured-but-default case is.
            let _guard = install_test_validator();
            let _require = require_oidc_on();
            let req = envelope_request(713, "oidc-require-missing", matching_claims(), None);
            let error = verify_envelope_v2_with(SECRET, &req, &verified_policy(), &memory_replay())
                .unwrap_err();
            assert!(
                error.contains("OIDC") && error.contains("bearer"),
                "{error}"
            );
        }
    }
}
