//! Identity / ACL wire types. They live here (not in `eg-core::isolation`)
//! because `protocol::Method::RegisterIdentity` carries an `AgentRole` over the
//! wire, and `protocol` is below `isolation` in the DAG. `eg-core::isolation`
//! re-exports both so its `IsolationLayer` and all call sites are unchanged.

use serde::{Deserialize, Serialize};

/// Identity and policy claims carried by a verified request-context envelope.
///
/// This is the wire representation only.  Callers must not treat a deserialized
/// value as trusted until the server has verified the envelope MAC, deployment
/// audience/tenant/policy version, request binding, and delegation chain.  The
/// server exposes a separate, non-constructible `VerifiedRequestContext` wrapper
/// after those checks succeed.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RequestContextClaims {
    /// Authenticated subject that originated the request.
    pub principal: String,
    /// Tenant security boundary selected by the deployment.
    pub tenant: String,
    /// Intended service audience.
    pub audience: String,
    /// Effective agent identity used for ACL/RBAC decisions.
    pub agent_id: String,
    /// Identity-provider roles asserted for policy evaluation/audit.
    pub roles: Vec<String>,
    /// Fine-grained capabilities asserted for policy evaluation/audit.
    pub scopes: Vec<String>,
    /// Exact policy bundle/version under which the context was issued.
    pub policy_version: String,
    /// Ordered delegation path from `principal` to `agent_id` (inclusive).
    /// Empty means no delegation and therefore requires principal == agent_id.
    pub delegation: Vec<String>,
    /// Target node id this envelope was minted for (ADR-3 / W1.9 node-bound
    /// envelopes — ``reports/wave1/ADR-scale-trio.md``). `None` for a client
    /// that predates node binding, or one that has not yet learned a target
    /// node id; a single-node deployment never needs to set this. When
    /// `Some(id)`, the server exact-matches it against the receiving node's
    /// own identity before dispatch (`server::auth::validate_context_claims`)
    /// — a mismatch means the envelope was captured and replayed against a
    /// different cluster member. `#[serde(default)]` so an envelope minted by
    /// a client that has never heard of this claim deserializes exactly as
    /// before (absent, not a hard error).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub node: Option<String>,
}

/// Role of an agent in the system hierarchy.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum AgentRole {
    /// System-level (can do anything).
    System,
    /// Manager agent with subordinates.
    Manager { subordinates: Vec<String> },
    /// Regular agent.
    Agent,
}

/// Agent identity for ACL checks.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentIdentity {
    pub agent_id: String,
    pub role: AgentRole,
    /// Teams this agent belongs to.
    pub teams: Vec<String>,
    /// RBAC role names this agent holds (CONCEPT:EG-KG.compute.feature). Expanded transitively
    /// through the role hierarchy by the policy evaluator. The field is mandatory in
    /// every current wire and durable identity image; an omitted authorization claim
    /// is rejected during deserialization instead of being synthesized as no roles.
    pub roles: Vec<String>,
}

// ── RBAC role model (CONCEPT:EG-KG.compute.feature) ─────────────────────────────────────────
// Durable, serde-serializable role/grant records layered ON TOP of the per-agent
// RLS/ACL. They persist exactly like `AgentIdentity` (in-memory in the
// `IsolationLayer`, replayable/serializable), and the pure-Rust evaluator lives in
// `eg-core::rbac` (behind the `security` feature). These TYPES are unconditional so
// the wire `Method::RbacAdmin` can carry them below the `isolation` layer in the DAG.

/// An action a grant permits or denies over a resource (CONCEPT:EG-KG.compute.feature).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum RbacAction {
    Read,
    Write,
    Admin,
}

/// Whether a [`Grant`] permits or forbids the action (CONCEPT:EG-KG.compute.feature).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum GrantEffect {
    Allow,
    Deny,
}

/// What a [`Grant`] applies to (CONCEPT:EG-KG.compute.feature). Ordered by [`specificity`] so the
/// evaluator can implement "most-specific-resource wins": a named graph beats a
/// label, which beats a glob pattern, which beats "all".
///
/// [`specificity`]: ResourceSelector::specificity
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ResourceSelector {
    /// Every resource (least specific).
    All,
    /// A glob over the graph name — a single optional leading and/or trailing `*`
    /// wildcard (e.g. `agent:*`, `*:private`, `*log*`).
    Pattern(String),
    /// A node label.
    Label(String),
    /// One named graph (most specific).
    Graph(String),
}

impl ResourceSelector {
    /// Higher = more specific. Used to break ties in the evaluator.
    pub fn specificity(&self) -> u8 {
        match self {
            ResourceSelector::All => 0,
            ResourceSelector::Pattern(_) => 1,
            ResourceSelector::Label(_) => 2,
            ResourceSelector::Graph(_) => 3,
        }
    }

    /// Does this selector apply to `ctx`?
    pub fn matches(&self, ctx: &ResourceContext) -> bool {
        match self {
            ResourceSelector::All => true,
            ResourceSelector::Graph(g) => *g == ctx.graph,
            ResourceSelector::Label(l) => ctx.label.as_deref() == Some(l.as_str()),
            ResourceSelector::Pattern(p) => glob_match(p, &ctx.graph),
        }
    }
}

/// A minimal single-`*` glob (leading and/or trailing wildcard) over `text`. A
/// pattern with no `*` is an exact match; `*` alone matches everything.
fn glob_match(pattern: &str, text: &str) -> bool {
    match (pattern.strip_prefix('*'), pattern.strip_suffix('*')) {
        // `*mid*` — contains.
        (Some(rest), Some(_)) => {
            let inner = rest.strip_suffix('*').unwrap_or(rest);
            inner.is_empty() || text.contains(inner)
        }
        // `*suffix` — ends-with.
        (Some(suffix), None) => text.ends_with(suffix),
        // `prefix*` — starts-with.
        (None, Some(prefix)) => text.starts_with(prefix),
        // no wildcard — exact.
        (None, None) => pattern == text,
    }
}

/// The concrete resource an access decision is about (CONCEPT:EG-KG.compute.feature). The
/// `IsolationLayer` builds this from the requested graph (and, where available, a
/// node label) before consulting the RBAC evaluator.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ResourceContext {
    pub graph: String,
    pub label: Option<String>,
}

impl ResourceContext {
    /// Convenience for the common graph-only context.
    pub fn graph(graph: impl Into<String>) -> Self {
        ResourceContext {
            graph: graph.into(),
            label: None,
        }
    }
}

/// A durable RBAC role with an optional set of parent roles forming a hierarchy
/// (CONCEPT:EG-KG.compute.feature). A role transitively inherits every grant of its parents.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Role {
    pub name: String,
    #[serde(default)]
    pub parents: Vec<String>,
}

impl Role {
    pub fn new(name: impl Into<String>) -> Self {
        Role {
            name: name.into(),
            parents: Vec::new(),
        }
    }

    pub fn with_parents(name: impl Into<String>, parents: Vec<String>) -> Self {
        Role {
            name: name.into(),
            parents,
        }
    }
}

/// A grant binding a role to an (`resource`, `action`, `effect`) triple
/// (CONCEPT:EG-KG.compute.feature).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Grant {
    pub role: String,
    pub resource: ResourceSelector,
    pub action: RbacAction,
    pub effect: GrantEffect,
}

/// A single administrative mutation of the RBAC policy (CONCEPT:EG-KG.compute.feature), carried by
/// `Method::RbacAdmin`. Unconditional so it sits below the `isolation` layer.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RbacAdminOp {
    AddRole(Role),
    RemoveRole(String),
    AddGrant(Grant),
    RemoveGrant(Grant),
    /// Read-only: list the current roles + grants.
    List,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn selector_specificity_ordering() {
        assert!(
            ResourceSelector::Graph("g".into()).specificity()
                > ResourceSelector::Label("L".into()).specificity()
        );
        assert!(
            ResourceSelector::Label("L".into()).specificity()
                > ResourceSelector::Pattern("a*".into()).specificity()
        );
        assert!(
            ResourceSelector::Pattern("a*".into()).specificity()
                > ResourceSelector::All.specificity()
        );
    }

    #[test]
    fn selector_matches() {
        let ctx = ResourceContext {
            graph: "agent:worker1".into(),
            label: Some("Doc".into()),
        };
        assert!(ResourceSelector::All.matches(&ctx));
        assert!(ResourceSelector::Graph("agent:worker1".into()).matches(&ctx));
        assert!(!ResourceSelector::Graph("agent:other".into()).matches(&ctx));
        assert!(ResourceSelector::Label("Doc".into()).matches(&ctx));
        assert!(!ResourceSelector::Label("Other".into()).matches(&ctx));
        assert!(ResourceSelector::Pattern("agent:*".into()).matches(&ctx));
        assert!(ResourceSelector::Pattern("*worker1".into()).matches(&ctx));
        assert!(ResourceSelector::Pattern("*worker*".into()).matches(&ctx));
        assert!(!ResourceSelector::Pattern("team:*".into()).matches(&ctx));
    }

    #[test]
    fn agent_identity_requires_roles_on_deserialize() {
        let json = r#"{"agent_id":"a","role":"Agent","teams":[]}"#;
        assert!(serde_json::from_str::<AgentIdentity>(json).is_err());
    }

    #[test]
    fn request_context_requires_roles_on_deserialize() {
        let json = r#"{
            "principal":"principal:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "tenant":"tenant:sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            "audience":"graph-os",
            "agent_id":"agent:sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
            "scopes":[],
            "policy_version":"current",
            "delegation":[]
        }"#;
        assert!(serde_json::from_str::<RequestContextClaims>(json).is_err());
    }

    #[test]
    fn absent_node_claim_deserializes_as_none_for_pre_binding_clients() {
        // ADR-3 / W1.9: a client that predates node binding (or one of the
        // non-Python `clients/{js,go}` bindings this change deliberately
        // leaves untouched) never sends a `node` key at all. `deny_unknown_fields`
        // must not reject that shape, and the field must come back `None`.
        let json = r#"{
            "principal":"principal:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "tenant":"tenant:sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            "audience":"graph-os",
            "agent_id":"agent:sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
            "roles":[],
            "scopes":[],
            "policy_version":"current",
            "delegation":[]
        }"#;
        let claims: RequestContextClaims = serde_json::from_str(json).unwrap();
        assert_eq!(claims.node, None);
    }

    #[test]
    fn present_node_claim_roundtrips() {
        let json = r#"{
            "principal":"p",
            "tenant":"t",
            "audience":"a",
            "agent_id":"p",
            "roles":[],
            "scopes":[],
            "policy_version":"v",
            "delegation":[],
            "node":"node-a"
        }"#;
        let claims: RequestContextClaims = serde_json::from_str(json).unwrap();
        assert_eq!(claims.node.as_deref(), Some("node-a"));
    }

    #[test]
    fn grant_roundtrips() {
        let g = Grant {
            role: "reader".into(),
            resource: ResourceSelector::Graph("g".into()),
            action: RbacAction::Read,
            effect: GrantEffect::Allow,
        };
        let s = serde_json::to_string(&g).unwrap();
        let back: Grant = serde_json::from_str(&s).unwrap();
        assert_eq!(g, back);
    }
}
