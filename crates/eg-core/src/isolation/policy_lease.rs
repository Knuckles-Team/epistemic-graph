#![cfg(feature = "security")]

use super::{row_visibility, AccessLevel, AgentIdentity, AgentRole, IsolationLayer, RowVisibility};
use std::collections::HashSet;

mod accessors;

// ── Policy decision lease (GRAPH-POLICY-LEASE-CONTRACT.md) ─────────────────
//
// Binds a served read to ONE durable, digest-verified image of the RBAC
// policy + registered identities, so a long-lived KnowledgeStream page can
// detect (and fail closed on) a policy mutation that races it, instead of
// silently trusting the caller's own `policy_version` claim (T1) or serving a
// stale visibility decision across a paginated stream (T2/T4). See the
// contract for the full threat model; this block implements §2-§4.

#[cfg(feature = "security")]
use sha2::Sha256;

/// Coarse authorization basis captured at lease-mint time (contract §2.2).
/// Consumed today purely as a cursor-binding differentiator — no call site
/// branches on it directly — but `filter_view` treats `System` as a full
/// row-visibility bypass, mirroring [`IsolationLayer::can_see_row`]'s own
/// unconditional System branch. This is not a new privilege: it is the
/// pre-existing `AgentRole::System` bypass every other engine surface has,
/// captured so it can be REVALIDATED (via the whole-image digest, §4)
/// instead of re-derived unsafely from a stale in-memory reference.
#[cfg(feature = "security")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PolicyDecisionBasis {
    System,
    RbacAllow,
}

/// Self-consistency error for a [`PolicySnapshot`] (contract §2.4). Does not
/// re-hash anything — it has no store to reload from — only checks that the
/// captured digest is well-formed.
#[cfg(feature = "security")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PolicySnapshotError {
    MalformedDigest,
}

#[cfg(feature = "security")]
impl std::fmt::Display for PolicySnapshotError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PolicySnapshotError::MalformedDigest => {
                write!(f, "graph policy snapshot digest is malformed")
            }
        }
    }
}

#[cfg(feature = "security")]
impl std::error::Error for PolicySnapshotError {}

/// Durable-policy epoch + canonical digest captured at lease-mint time
/// (contract §2.4). `version` is an audit/display-facing counter only —
/// re-read from the existing `eg_mutation_store` version this crate already
/// bumps atomically with every `RbacStore::save` (R5), never a second
/// persisted counter. `digest` is the actual staleness ground truth (§4):
/// `SHA-256("eg/rbac-policy-snapshot/v1\0" || canonical(RbacPolicy))` over a
/// `BTreeMap`-canonicalized view of the policy's roles (the policy's
/// `grants: Vec<Grant>` is already order-stable.
#[cfg(feature = "security")]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PolicySnapshot {
    pub version: u64,
    pub digest: String,
}

#[cfg(feature = "security")]
impl PolicySnapshot {
    /// Non-empty, correctly-shaped lowercase hex SHA-256 digest. This is a
    /// pure self-consistency check (mod.rs:142-144's binding-mismatch error
    /// path); the actual freshness check against live state is
    /// `validate_before`/`validate_after`/`filter_view` below, which alone
    /// have a store to reload from.
    pub fn validate(&self) -> Result<(), PolicySnapshotError> {
        let well_formed = self.digest.len() == 64
            && !self.digest.is_empty()
            && self.digest.bytes().all(|b| b.is_ascii_hexdigit());
        if well_formed {
            Ok(())
        } else {
            Err(PolicySnapshotError::MalformedDigest)
        }
    }
}

/// Mirrors [`IsolationLayer::is_manager_of`] exactly, parameterized over an
/// explicit identities map rather than `&self.agents`. Needed because a
/// [`PolicyDecisionLease`] captures a point-in-time identities snapshot (via
/// [`crate::rbac_persist::RbacPolicyStore::authority_snapshot`]) rather than borrowing an
/// `IsolationLayer`'s private `agents` field — the lease must outlive, and be
/// revalidated independently of, any particular `IsolationLayer` value.  This
/// is an intentional near-duplicate confined to this same module (never
/// crossing the `isolation`/`rbac_persist` boundary the contract's §3.1
/// forbids duplicating `check_access`'s decision across) — flagged so a
/// future edit to `is_manager_of` is checked against this copy too.
#[cfg(feature = "security")]
fn is_manager_of_for_identities(
    identities: &std::collections::BTreeMap<String, AgentIdentity>,
    agent_id: &str,
    subordinate_id: &str,
) -> bool {
    if let Some(identity) = identities.get(agent_id) {
        if let AgentRole::Manager { subordinates } = &identity.role {
            return subordinates.contains(&subordinate_id.to_string());
        }
    }
    false
}

/// Mirrors [`IsolationLayer::can_see_row`] exactly — see
/// [`is_manager_of_for_identities`]'s doc for why this near-duplicate exists.
#[cfg(feature = "security")]
fn can_see_row_for_identities(
    identities: &std::collections::BTreeMap<String, AgentIdentity>,
    agent_id: &str,
    vis: &RowVisibility,
) -> bool {
    let is_system = identities
        .get(agent_id)
        .is_some_and(|identity| identity.role == AgentRole::System);
    let Some(owner) = vis.owner.as_deref() else {
        return is_system || vis.schema || (vis.tagged && vis.public);
    };
    is_system
        || vis.schema
        || vis.public
        || owner == agent_id
        || vis.grants.iter().any(|grant| grant == agent_id)
        || is_manager_of_for_identities(identities, agent_id, owner)
}

/// Mirrors [`IsolationLayer::can_see_node`] exactly — see
/// [`is_manager_of_for_identities`]'s doc for why this near-duplicate exists.
#[cfg(feature = "security")]
fn can_see_node_for_identities(
    identities: &std::collections::BTreeMap<String, AgentIdentity>,
    agent_id: &str,
    view: &crate::graph::GraphView,
    id: &str,
) -> bool {
    let mut vis = match view.visibility_index.get(id) {
        Some(vis) => vis.clone(),
        None => view
            .node_properties
            .get(id)
            .map(|blob| row_visibility(blob))
            .unwrap_or_else(RowVisibility::default_public),
    };
    vis.schema = view.schema_node_ids.contains(id);
    can_see_row_for_identities(identities, agent_id, &vis)
}

/// Fail-closed mint-time error (contract §8, items 2/13/14).
#[cfg(feature = "security")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MintLeaseError {
    /// `actor` has no durable registered identity.
    UnknownActor,
    /// `check_access`-equivalent denied (System bypass or `RbacPolicy::evaluate`
    /// did not return `Some(Allow)`), or a non-`Read` access was requested.
    AccessDenied,
    /// `IsolationLayer::policy_store()` is `None` — nothing durable to mint a
    /// digest from (item 13).
    StoreUnavailable,
    /// The durable store is bound but its atomic authority snapshot could not
    /// be read or validated (I/O/serde/digest failure).
    StoreUnreadable,
}

#[cfg(feature = "security")]
impl std::fmt::Display for MintLeaseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MintLeaseError::UnknownActor => write!(f, "actor has no registered durable identity"),
            MintLeaseError::AccessDenied => write!(f, "policy decision lease denied"),
            MintLeaseError::StoreUnavailable => write!(f, "no durable policy store is bound"),
            MintLeaseError::StoreUnreadable => write!(f, "durable policy store is unreadable"),
        }
    }
}

#[cfg(feature = "security")]
impl std::error::Error for MintLeaseError {}

// ── Mint authorization (GRAPH-POLICY-LEASE-CONTRACT.md §3 hardening) ────────
//
// `RequestContextClaims` (`crate::acl`) is a `pub` struct with every field
// `pub` — it must be, since it is the wire type the server deserializes an
// envelope's claims into. That means any of the 17 crates depending on
// `eg-core` can freely build a `RequestContextClaims` naming an arbitrary
// registered actor and hand it straight to `mint_policy_decision_lease`, which
// would mint (and the resulting lease would then correctly validate/filter
// against) a lease FOR THE WRONG PRINCIPAL. `pub(crate)` on the mint method
// itself is not viable — see that method's own doc for why (the sole
// production caller is a different crate). `MintAuthorization` is the
// mitigation actually available here: a wrapper `mint_policy_decision_lease`
// accepts INSTEAD of a raw claims reference, buildable only by presenting the
// server authentication secret plus an HMAC over the claims that this module
// recomputes and compares in constant time.
//
// **Honest scope.** This does not, and cannot, cryptographically prove "the
// transport layer verified this envelope" — a malicious actor already running
// in-process with the server secret reachable could call `filter_view`/
// `validate_before`/`validate_after` directly regardless of what gates
// `mint_policy_decision_lease`, and no wrapper type changes that. What this DOES
// close: a plain `RequestContextClaims { agent_id: "someone-else", .. }`
// struct literal can no longer reach the mint call at all (the field is
// private, there is no public constructor), and a copy-pasted or
// hand-written future call site — an internal tool, an admin surface, a
// careless refactor — cannot mint for claims other than the ones it computed
// the presented MAC over, because `new` recomputes the MAC from `claims`
// itself and rejects a mismatch. Misdirection now requires a caller to
// deliberately compute a valid MAC for one claims value and then swap in a
// different one, not merely construct a struct and move on — exactly the
// bar §3's "impossible to do by accident or copy-paste" asks for, not a bar
// against a co-located attacker who already holds the secret.
#[cfg(feature = "security")]
use hmac::Mac as _;

#[cfg(feature = "security")]
type MintAuthorizationMac = hmac::Hmac<Sha256>;

/// Fail-closed construction error for [`MintAuthorization::new`].
#[cfg(feature = "security")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MintAuthorizationError {
    /// The presented server authentication secret is empty.
    EmptySecret,
    /// The secret could not key an HMAC (only possible for a
    /// pathologically-sized key; `hmac::Hmac<Sha256>` otherwise accepts any
    /// key length).
    InvalidSecret,
    /// The presented MAC does not match the one recomputed from `claims`
    /// under `auth_secret` — either a wrong secret, or `claims` was swapped
    /// after the MAC was computed.
    InvalidMac,
}

#[cfg(feature = "security")]
impl std::fmt::Display for MintAuthorizationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MintAuthorizationError::EmptySecret => {
                write!(f, "mint authorization requires a non-empty server secret")
            }
            MintAuthorizationError::InvalidSecret => {
                write!(f, "mint authorization secret could not key an HMAC")
            }
            MintAuthorizationError::InvalidMac => {
                write!(
                    f,
                    "mint authorization MAC does not match the presented claims"
                )
            }
        }
    }
}

#[cfg(feature = "security")]
impl std::error::Error for MintAuthorizationError {}

/// Canonical (deterministic) encoding of a [`crate::acl::RequestContextClaims`]
/// used ONLY to compute the [`MintAuthorization`] HMAC. Uses a purpose-built,
/// explicit-field-order structure fed through `serde_json::to_vec` rather than
/// hashing `RequestContextClaims` itself, whose derive/field order is a
/// source-code detail, not a contract this MAC should be brittle to.
#[cfg(feature = "security")]
fn mint_authorization_claims_bytes(claims: &crate::acl::RequestContextClaims) -> Vec<u8> {
    #[derive(serde::Serialize)]
    struct CanonicalClaims<'a> {
        principal: &'a str,
        tenant: &'a str,
        audience: &'a str,
        agent_id: &'a str,
        roles: &'a [String],
        scopes: &'a [String],
        policy_version: &'a str,
        delegation: &'a [String],
        node: &'a Option<String>,
        priority: &'a Option<String>,
    }
    let canonical = CanonicalClaims {
        principal: &claims.principal,
        tenant: &claims.tenant,
        audience: &claims.audience,
        agent_id: &claims.agent_id,
        roles: &claims.roles,
        scopes: &claims.scopes,
        policy_version: &claims.policy_version,
        delegation: &claims.delegation,
        node: &claims.node,
        priority: &claims.priority,
    };
    serde_json::to_vec(&canonical).unwrap_or_default()
}

#[cfg(feature = "security")]
fn mint_authorization_mac(
    auth_secret: &str,
    claims: &crate::acl::RequestContextClaims,
) -> Result<MintAuthorizationMac, MintAuthorizationError> {
    let mut mac = MintAuthorizationMac::new_from_slice(auth_secret.as_bytes())
        .map_err(|_| MintAuthorizationError::InvalidSecret)?;
    mac.update(b"eg/mint-policy-decision-lease-authorization/v1\0");
    mac.update(&mint_authorization_claims_bytes(claims));
    Ok(mac)
}

/// Capability token gating [`IsolationLayer::mint_policy_decision_lease`]
/// (GRAPH-POLICY-LEASE-CONTRACT.md §3 hardening). The `claims` field is
/// private — there is no public struct-literal construction — so the ONLY way
/// to obtain one is [`MintAuthorization::new`], which requires presenting the
/// server authentication secret together with an HMAC over the canonical
/// claims that this recomputes and compares in constant time
/// (`hmac::Mac::verify_slice`). See the module-level doc above this block for
/// exactly what this control does and does not prevent.
#[cfg(feature = "security")]
pub struct MintAuthorization {
    claims: crate::acl::RequestContextClaims,
}

#[cfg(feature = "security")]
impl MintAuthorization {
    /// Compute the HMAC a caller must present to [`MintAuthorization::new`]
    /// for `claims`, keyed by the server authentication secret. The ONE place
    /// that computes it — `new` independently recomputes the same value from
    /// `claims` and compares against what's presented, it never trusts the
    /// presented bytes as ground truth.
    pub fn compute_mac(
        auth_secret: &str,
        claims: &crate::acl::RequestContextClaims,
    ) -> Result<Vec<u8>, MintAuthorizationError> {
        if auth_secret.is_empty() {
            return Err(MintAuthorizationError::EmptySecret);
        }
        Ok(mint_authorization_mac(auth_secret, claims)?
            .finalize()
            .into_bytes()
            .to_vec())
    }

    /// The ONLY constructor. `mac` must equal
    /// [`MintAuthorization::compute_mac`]`(auth_secret, &claims)`, verified
    /// here via `hmac::Mac::verify_slice` (constant-time tag comparison, not
    /// a hand-rolled one) rather than trusted from the caller. A plain
    /// `MintAuthorization { claims }` struct literal can never compile
    /// outside this module — the field is private — so reaching a mint call
    /// always passes through this cryptographic check.
    pub fn new(
        auth_secret: &str,
        claims: &crate::acl::RequestContextClaims,
        mac: &[u8],
    ) -> Result<Self, MintAuthorizationError> {
        if auth_secret.is_empty() {
            return Err(MintAuthorizationError::EmptySecret);
        }
        mint_authorization_mac(auth_secret, claims)?
            .verify_slice(mac)
            .map_err(|_| MintAuthorizationError::InvalidMac)?;
        Ok(MintAuthorization {
            claims: claims.clone(),
        })
    }

    /// The verified claims this token carries.
    pub fn claims(&self) -> &crate::acl::RequestContextClaims {
        &self.claims
    }
}

/// Staleness error from [`PolicyDecisionLease::validate_before`]/`validate_after`/
/// `filter_view` (contract §4/§8). Deliberately carries no digest/version —
/// nothing derived from it may ever reach a caller-visible response (§8's
/// closing paragraph); every consumer maps it to one fixed generic string.
#[cfg(feature = "security")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LeaseStaleError {
    /// The durable store could not be reloaded.
    StoreUnavailable,
    /// A fresh reload's policy or identity digest no longer matches what was
    /// captured at mint time.
    Stale,
}

#[cfg(feature = "security")]
impl std::fmt::Display for LeaseStaleError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LeaseStaleError::StoreUnavailable => write!(f, "durable policy store is unavailable"),
            LeaseStaleError::Stale => write!(f, "policy decision lease is stale"),
        }
    }
}

#[cfg(feature = "security")]
impl std::error::Error for LeaseStaleError {}

/// Binds a served read to one durable, digest-verified RBAC/identity image
/// (contract §2.3). Deliberately **not** `Clone` and has no public
/// constructor: every field is private to this module, so the ONLY way to
/// obtain one is [`IsolationLayer::mint_policy_decision_lease`] — see that
/// method's doc for why non-constructibility is a hard security requirement
/// (T3), not a style choice.
#[cfg(feature = "security")]
pub struct PolicyDecisionLease {
    access: AccessLevel,
    originating_principal: String,
    effective_actor: String,
    tenant: String,
    resource: String,
    policy_snapshot: PolicySnapshot,
    decision_basis: PolicyDecisionBasis,
    row_policy_identity_digest: String,
}

#[cfg(feature = "security")]
impl PolicyDecisionLease {
    /// Reload the durable store ONCE and compare both digests captured at
    /// mint time against the fresh read (contract §4 `is_stale`). `version`
    /// is never compared — `digest` alone is the fail-closed ground truth.
    fn reload_and_verify_fresh(
        &self,
        store: &dyn crate::rbac_persist::RbacPolicyStore,
    ) -> Result<crate::rbac_persist::RbacAuthoritySnapshot, LeaseStaleError> {
        let authority = store
            .authority_snapshot()
            .map_err(|_| LeaseStaleError::StoreUnavailable)?;
        if authority.policy_digest() != self.policy_snapshot.digest
            || authority.identity_digest() != self.row_policy_identity_digest
        {
            return Err(LeaseStaleError::Stale);
        }
        Ok(authority)
    }

    /// Revalidate the durable policy image immediately before using a page
    /// (contract §4/§8 items 7/10).
    pub fn validate_before(
        &self,
        store: &dyn crate::rbac_persist::RbacPolicyStore,
    ) -> Result<(), LeaseStaleError> {
        self.reload_and_verify_fresh(store).map(|_| ())
    }

    /// Revalidate the durable policy image immediately after producing a page
    /// (contract §4/§8 items 8/9/10).
    pub fn validate_after(
        &self,
        store: &dyn crate::rbac_persist::RbacPolicyStore,
    ) -> Result<(), LeaseStaleError> {
        self.reload_and_verify_fresh(store).map(|_| ())
    }

    /// Filter a [`crate::graph::GraphView`] IN-PLACE down to only the rows
    /// this lease's effective actor may see, using a fresh reload of the durable store
    /// as the sole visibility authority for this call (contract §5/§6) — the
    /// one row-visibility decision every KnowledgeStream family uses. Fails
    /// closed (never filters against a stale image) exactly like
    /// `validate_before`/`validate_after`.
    pub fn filter_view(
        &self,
        store: &dyn crate::rbac_persist::RbacPolicyStore,
        view: &mut crate::graph::GraphView,
    ) -> Result<(), LeaseStaleError> {
        let authority = self.reload_and_verify_fresh(store)?;
        if matches!(self.decision_basis, PolicyDecisionBasis::System) {
            // Mirrors `can_see_row`'s own unconditional System bypass
            // (contract §2.2) — not a new privilege, see that section.
            return Ok(());
        }
        let hidden: HashSet<String> = view
            .node_map
            .keys()
            .filter_map(|id| {
                if can_see_node_for_identities(
                    authority.identities(),
                    self.effective_actor.as_str(),
                    view,
                    id,
                ) {
                    None
                } else {
                    Some(id.clone())
                }
            })
            .collect();
        if hidden.is_empty() {
            return Ok(());
        }
        for id in &hidden {
            if let Some(idx) = view.node_map.remove(id) {
                view.graph.remove_node(idx);
            }
            view.node_properties.remove(id);
        }
        view.edge_properties
            .retain(|(s, t), _| !hidden.contains(s) && !hidden.contains(t));
        Ok(())
    }
}

impl IsolationLayer {
    /// The ONLY function that may construct a [`PolicyDecisionLease`]
    /// (GRAPH-POLICY-LEASE-CONTRACT.md §3). Re-runs the exact `check_access`
    /// decision (System bypass, else `RbacPolicy::evaluate`) so minting can
    /// never become a second, laxer authorization path, then captures a
    /// durable, canonically-digested `PolicySnapshot` + identity digest from
    /// the SAME atomic authority snapshot (§2.4/§2.5) so a not-yet-persisted
    /// in-memory mutation can never mint a lease whose digest a concurrent
    /// `validate_before` reload wouldn't reproduce.
    ///
    /// **Identity source (§3, "Visibility and identity-source are part of
    /// the contract, not an implementation detail").** The contract specifies
    /// `&VerifiedRequestContext` (or its verified claims) so the caller
    /// identity can never be expressed as an arbitrary string. This crate
    /// (`eg-core`) sits BELOW the binary crate that defines
    /// `VerifiedRequestContext` (`src/server/auth.rs`, part of the
    /// `epistemic-graph` crate, not `eg-core`) in the dependency DAG, so the
    /// literal type is unreachable here — this takes `&MintAuthorization`
    /// instead, deriving actor/tenant from the claims it carries, never from
    /// a separately-passed claims argument. The security property is
    /// strengthened, not merely preserved: a bare `RequestContextClaims` is
    /// freely constructible by any of the 17 crates depending on `eg-core`
    /// (every field is `pub`, required by its role as a wire type), but
    /// `MintAuthorization` is not — obtaining one requires presenting the
    /// server authentication secret plus an HMAC over the exact claims,
    /// verified in constant time (see the `MintAuthorization` doc above). In
    /// production only `authorize_and_route_knowledge_stream` (dispatch.rs)
    /// holds the secret at the point it has verified claims in hand, so it
    /// remains the only real caller that can mint — now backed by a
    /// cryptographic check instead of a convention.
    ///
    /// **Visibility (§3, requirement 1 — "at most `pub(crate)`, narrower if
    /// possible").** The contract's own reasoning for narrow visibility is
    /// that 17 crates depend on `eg-core` and a wide-open constructor would
    /// be reachable from all of them. But the ONE traced production caller —
    /// `authorize_and_route_knowledge_stream` — lives in `src/server/
    /// dispatch.rs`, part of the `epistemic-graph` BINARY crate, a
    /// DIFFERENT crate than `eg-core`. `pub(crate)` in `eg-core` is invisible
    /// outside `eg-core` entirely, including to `dispatch.rs` — it would make
    /// the one call the contract's own §3 "wire the feature" requirement
    /// mandates impossible to compile. Stable Rust has no visibility
    /// modifier that reaches exactly "the `epistemic-graph` crate, not the
    /// other 16 `eg-core` dependents" across a crate boundary. This is a
    /// contract defect, not something to silently route around: the
    /// achievable bar is `pub`, kept narrow in every other way the contract
    /// requires AND hardened beyond it — non-constructible `PolicyDecisionLease`,
    /// now non-constructible `MintAuthorization`-gated identity, re-run
    /// `check_access`, digest captured from the durable store only. See
    /// `scripts/check_mint_lease_call_sites.py` for the CI gate that keeps
    /// this the one production call site.
    #[cfg(feature = "security")]
    pub fn mint_policy_decision_lease(
        &self,
        auth: &MintAuthorization,
        resource: &str,
        access: AccessLevel,
    ) -> Result<PolicyDecisionLease, MintLeaseError> {
        // §2.1: only Read is specified by any traced consumer; a Write lease
        // is legal to mint in principle but this feature never requests one.
        if access != AccessLevel::Read {
            return Err(MintLeaseError::AccessDenied);
        }
        let claims = auth.claims();
        let originating_principal = claims.principal.as_str();
        let effective_actor = claims.agent_id.as_str();
        let tenant = claims.tenant.as_str();
        let store = self
            .persist
            .clone()
            .ok_or(MintLeaseError::StoreUnavailable)?;
        let authority = store
            .authority_snapshot()
            .map_err(|_| MintLeaseError::StoreUnreadable)?;
        // Step 2: unregistered effective actor ⇒ no lease (matches
        // `check_access`'s `None ⇒ false`).
        let identity = authority
            .identities()
            .get(effective_actor)
            .ok_or(MintLeaseError::UnknownActor)?;
        // Step 3: the EXACT `check_access` decision — System bypass, else
        // `RbacPolicy::evaluate`. Never a second, laxer path.
        let decision_basis = if identity.role == AgentRole::System {
            PolicyDecisionBasis::System
        } else {
            let ctx = crate::acl::ResourceContext::graph(resource);
            let allowed = matches!(
                authority
                    .policy()
                    .evaluate(&identity.roles, &ctx, crate::acl::RbacAction::Read),
                Some(crate::acl::GrantEffect::Allow)
            );
            if !allowed {
                return Err(MintLeaseError::AccessDenied);
            }
            PolicyDecisionBasis::RbacAllow
        };
        // Step 4: revision, policy, identities, and both digests came from
        // the same atomic authority snapshot above. No separate store read
        // can race a policy or identity mutation into this decision.
        Ok(PolicyDecisionLease {
            access,
            originating_principal: originating_principal.to_string(),
            effective_actor: effective_actor.to_string(),
            tenant: tenant.to_string(),
            resource: resource.to_string(),
            policy_snapshot: PolicySnapshot {
                version: authority.revision(),
                digest: authority.policy_digest().to_string(),
            },
            decision_basis,
            row_policy_identity_digest: authority.identity_digest().to_string(),
        })
    }
}
