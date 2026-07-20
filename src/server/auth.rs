//! Request authentication uses one `eg2.` verified request-context envelope.
//!
//! The envelope binds request/body/replay fields plus the effective ACL agent,
//! roles, scopes, active policy version, and delegation chain under HMAC-SHA256.
//! Audience, tenant, and policy version must match deployment configuration;
//! nonce acceptance is durable and committed before dispatch. Unknown envelope
//! formats fail closed. This workspace intentionally has no downgraded protocol,
//! environment alias, or development authentication escape.
//!
//!    Transport TLS/mTLS and upstream OIDC principal resolution are separate
//!    layers: this module is the request-envelope crypto core, while the served
//!    TCP boundary enforces its configured TLS policy before requests arrive.

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
use std::sync::{Mutex, OnceLock};
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

// ── Verified request context envelope ──────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct EnvelopeV2 {
    context: RequestContextClaims,
    timestamp: u64,
    nonce: String,
    idempotency_key: String,
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
        let req = signed_v2(503, "v2-replay");
        let replay = memory_replay();
        verify_envelope_v2_with(SECRET, &req, &verified_policy(), &replay).unwrap();
        let error = verify_envelope_v2_with(SECRET, &req, &verified_policy(), &replay).unwrap_err();
        assert!(error.contains("replay"));
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
}
