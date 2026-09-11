//! The trusted authority context produced after request authentication.
//!
//! This module owns the immutable, crate-private projection that dispatch and
//! auxiliary protocol adapters consume. `auth` remains responsible for envelope
//! cryptography and deployment claim validation; this type is deliberately
//! re-exported there so existing server call sites cannot construct a trusted
//! context through an alternate path.

use crate::acl::RequestContextClaims;
#[cfg(feature = "oidc")]
use crate::server::auth::iceberg_bearer_scopes;
use crate::server::auth::{coarse_kg_admin_only, request_context_policy};
use eg_types::contract::Nonce;
use hmac::{Hmac, Mac};
use sha2::Sha256;
use std::collections::HashSet;

type HmacSha256 = Hmac<Sha256>;

/// Authenticated request identity returned only after the corresponding
/// envelope has passed cryptographic and deployment-policy verification.
/// Fields are private so downstream code cannot construct a trusted context
/// from caller-supplied JSON.
#[derive(Debug, Clone)]
pub(crate) struct VerifiedRequestContext {
    claims: RequestContextClaims,
    idempotency_key: String,
    /// The authenticated transport nonce, converted to the fixed-width kernel
    /// value before a mutation compiler receives it. Read-only requests carry
    /// this for parity, but only a mutation envelope consumes it durably.
    wire_nonce: Option<Nonce>,
    scope_index: HashSet<String>,
    scope_wildcard_domains: HashSet<String>,
}

impl VerifiedRequestContext {
    pub(crate) fn from_verified_claims(
        claims: RequestContextClaims,
        idempotency_key: String,
    ) -> Self {
        Self::from_verified_claims_with_nonce(claims, idempotency_key, None)
    }

    pub(crate) fn from_verified_claims_with_nonce(
        claims: RequestContextClaims,
        idempotency_key: String,
        wire_nonce: Option<Nonce>,
    ) -> Self {
        let scope_index = claims.scopes.iter().cloned().collect();
        let scope_wildcard_domains = claims
            .scopes
            .iter()
            .filter_map(|scope| scope.strip_suffix(":*").map(str::to_owned))
            .collect();
        Self {
            claims,
            idempotency_key,
            wire_nonce,
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
        Ok(Self::from_verified_claims_with_nonce(
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
            authority.attempt_nonce,
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

    pub(crate) fn attempt_nonce(&self) -> Option<Nonce> {
        self.wire_nonce
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

    /// Same shape as [`Self::verified_for_test_in_tenant`] but with an explicit
    /// scope set, for surfaces (`lake::rest`'s Iceberg-REST tests, NE-048) that
    /// need to exercise a carrier with a specific `kg:read`/`kg:write` grant
    /// rather than the shared fixture's fixed `kg:read`-only default.
    #[cfg(test)]
    pub(crate) fn verified_for_test_with_scopes(
        agent_id: &str,
        tenant: &str,
        scopes: &[&str],
    ) -> Self {
        Self::from_verified_claims(
            RequestContextClaims {
                principal: format!("principal:{agent_id}"),
                tenant: tenant.to_string(),
                agent_id: agent_id.to_string(),
                audience: "epistemic-graph".to_string(),
                policy_version: "policy-test".to_string(),
                scopes: scopes.iter().map(|s| s.to_string()).collect(),
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

    /// Bind a fixed engine-owned service identity for an auxiliary HTTP surface
    /// whose OWN protocol-native guard authenticates the caller but cannot
    /// distinguish one caller from another (a single configured SigV4 credential
    /// pair, a shared bearer secret, or a JWT proving only platform membership).
    /// Mirrors [`Self::authenticated_local_query`]'s fixed-identity shape; callers
    /// pass ONLY after their own gate has already succeeded — this never
    /// authenticates anything itself.
    pub(crate) fn authenticated_fixed_service_actor(
        service: &str,
        scopes: &[&str],
    ) -> Result<Self, String> {
        let policy = request_context_policy()?;
        let principal = format!("service:{service}");
        Ok(Self::from_verified_claims(
            RequestContextClaims {
                principal: principal.clone(),
                tenant: policy.expected_tenant.clone(),
                audience: policy.expected_audience.clone(),
                agent_id: principal,
                roles: vec![format!("{service}-adapter")],
                scopes: scopes.iter().map(|s| s.to_string()).collect(),
                policy_version: policy.expected_policy_version.clone(),
                delegation: Vec::new(),
                node: None,
                priority: None,
            },
            format!("{service}-session"),
        ))
    }

    /// Bind an Iceberg-REST OAuth2 bearer's own verified subject + tenant
    /// claim to the engine-owned request context (BUG-222,
    /// `server::lake::rest`).
    ///
    /// Unlike [`Self::authenticated_fixed_service_actor`] (used by S3 SigV4
    /// and the KV-cache/`/sparql` bearer-JWT legs — protocols whose own
    /// credential carries no distinguishable per-caller tenant, so those mint
    /// ONE fixed service identity for every caller), a Keycloak-issued
    /// Iceberg bearer's verified `tenant`/`tenant_id`/`org`/`org_id`/`tid`
    /// claim (projected by `oidc::JwtValidator::validate_claims`) is itself
    /// compared against this deployment's own configured tenant
    /// (`EPISTEMIC_GRAPH_TENANT`) and REJECTED on any mismatch — a
    /// validly-signed bearer minted for a different tenant must never open a
    /// `CarrierAuthority` against this deployment's catalog. Namespace/table-
    /// level per-tenant projection of the catalog itself is a separate,
    /// later concern (GOC-75-W04); this is the identity-binding boundary,
    /// mirroring [`bind_verified_identity`]'s SAME tenant-claim requirement
    /// for the primary `eg2.` protocol.
    ///
    /// **Minted scopes are derived from the bearer's own verified `scope`/`scp`
    /// claim (NE-048, P0), never hardcoded.** Before this fix every
    /// tenant-matching Iceberg bearer was unconditionally minted BOTH
    /// `kg:read` AND `kg:write` regardless of what the token actually
    /// granted — a `kg:read`-only bearer silently received write authority.
    /// [`iceberg_bearer_scopes`] projects `verified.scopes` (already parsed
    /// by `oidc::JwtValidator::validate_claims` from the standard
    /// space-delimited `scope`/`scp` OAuth2 claim into a `HashSet<String>` —
    /// reused as-is, not re-parsed here) into the narrower Iceberg-REST
    /// vocabulary and fails closed on anything it does not recognize: an
    /// absent/empty claim, or a claim containing a scope this deployment does
    /// not project for this surface, both deny the bearer outright rather
    /// than falling back to the old always-both-scopes default or silently
    /// dropping the unrecognized token and granting whatever remainder is
    /// left. `server::lake::rest::handle` maps each REST operation to the
    /// minimum of these two scopes it actually needs (reads ⇒ `kg:read`,
    /// mutations ⇒ `kg:write`) via `CarrierAuthority::can_read`/`can_write`.
    ///
    /// `verified` has already passed RSA/JWKS signature + issuer + audience +
    /// expiry verification in the caller
    /// (`oidc::JwtValidator::validate_claims`) — this function only projects
    /// an already-verified claim set, exactly like
    /// [`Self::authenticated_fixed_service_actor`] never re-authenticates.
    #[cfg(feature = "oidc")]
    pub(crate) fn authenticated_iceberg_bearer(
        verified: &crate::server::oidc::VerifiedTokenClaims,
    ) -> Result<Self, String> {
        let policy = request_context_policy()?;
        let subject = verified.subject.trim();
        if subject.is_empty() {
            return Err("verified Iceberg-REST bearer is missing a subject".to_string());
        }
        // A tenant claim is required, not merely compared-if-present: an
        // absent tenant claim is proof of nothing (same reasoning as
        // `bind_verified_identity`'s identical requirement below).
        let tenant = verified
            .tenant
            .as_deref()
            .ok_or_else(|| "verified Iceberg-REST bearer is missing a tenant claim".to_string())?;
        if tenant != policy.expected_tenant {
            return Err(
                "verified Iceberg-REST bearer tenant does not match this deployment's \
                 configured tenant"
                    .to_string(),
            );
        }
        let scopes = iceberg_bearer_scopes(&verified.scopes).map_err(|detail| {
            // Logged with detail for operator diagnosis; the caller
            // (`mint_iceberg_carrier`) discards the `Err` and every denial
            // path collapses to the SAME generic 403 `resolve_carrier`
            // already returns for a missing/cross-tenant bearer, so nothing
            // here tells an unauthorized caller WHICH check failed.
            tracing::warn!(subject, "Iceberg-REST bearer denied: {detail}");
            "verified Iceberg-REST bearer does not carry an authorized scope claim".to_string()
        })?;
        let principal = format!("iceberg:{subject}");
        Ok(Self::from_verified_claims(
            RequestContextClaims {
                principal: principal.clone(),
                tenant: policy.expected_tenant.clone(),
                audience: policy.expected_audience.clone(),
                agent_id: principal,
                roles: vec!["iceberg-rest-client".to_string()],
                scopes,
                policy_version: policy.expected_policy_version.clone(),
                delegation: Vec::new(),
                node: None,
                priority: None,
            },
            format!("iceberg-rest-session:{subject}"),
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
