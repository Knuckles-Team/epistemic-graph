//! Identity / ACL wire types. They live here (not in `eg-core::isolation`)
//! because `protocol::Method::RegisterIdentity` carries an `AgentRole` over the
//! wire, and `protocol` is below `isolation` in the DAG. `eg-core::isolation`
//! re-exports both so its `IsolationLayer` and all call sites are unchanged.

use serde::{Deserialize, Serialize};

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
    /// RBAC role names this agent holds (CONCEPT:EG-092). Expanded transitively
    /// through the role hierarchy by the policy evaluator. `#[serde(default)]`
    /// keeps the wire/record layout backward-compatible — an identity persisted or
    /// sent before RBAC simply carries an empty set (⇒ no RBAC grants apply, so the
    /// existing RLS/ACL behavior is unchanged).
    #[serde(default)]
    pub roles: Vec<String>,
}

// ── RBAC role model (CONCEPT:EG-092) ─────────────────────────────────────────
// Durable, serde-serializable role/grant records layered ON TOP of the per-agent
// RLS/ACL. They persist exactly like `AgentIdentity` (in-memory in the
// `IsolationLayer`, replayable/serializable), and the pure-Rust evaluator lives in
// `eg-core::rbac` (behind the `security` feature). These TYPES are unconditional so
// the wire `Method::RbacAdmin` can carry them below the `isolation` layer in the DAG.

/// An action a grant permits or denies over a resource (CONCEPT:EG-092).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum RbacAction {
    Read,
    Write,
    Admin,
}

/// Whether a [`Grant`] permits or forbids the action (CONCEPT:EG-092).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum GrantEffect {
    Allow,
    Deny,
}

/// What a [`Grant`] applies to (CONCEPT:EG-092). Ordered by [`specificity`] so the
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

/// The concrete resource an access decision is about (CONCEPT:EG-092). The
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
/// (CONCEPT:EG-092). A role transitively inherits every grant of its parents.
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
/// (CONCEPT:EG-092).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Grant {
    pub role: String,
    pub resource: ResourceSelector,
    pub action: RbacAction,
    pub effect: GrantEffect,
}

/// A single administrative mutation of the RBAC policy (CONCEPT:EG-092), carried by
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
    fn agent_identity_roles_default_on_deserialize() {
        // A JSON payload written before RBAC (no `roles`) deserializes with an empty set.
        let json = r#"{"agent_id":"a","role":"Agent","teams":[]}"#;
        let id: AgentIdentity = serde_json::from_str(json).unwrap();
        assert!(id.roles.is_empty());
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
