//! RBAC-at-scale policy evaluator (CONCEPT:EG-092, feature `security`).
//!
//! Layered ON TOP of the per-agent Row-Level Security / ACL (CONCEPT:KG-2.231): an
//! agent carries a set of role NAMES; roles form a hierarchy (a role inherits every
//! grant of its transitively-reachable parents); and [`Grant`]s bind a role to a
//! (resource, action, effect) triple. Given an agent's roles + a requested
//! `(resource, action)`, [`RbacPolicy::evaluate`] returns the winning effect using:
//!
//! 1. **default deny** — no matching grant ⇒ `None` (the caller treats it as deny);
//! 2. **most-specific-resource wins** — a `Graph` grant beats a `Label`, beats a
//!    `Pattern`, beats `All` (see [`ResourceSelector::specificity`]);
//! 3. **deny overrides allow** — at the winning specificity, any `Deny` wins.
//!
//! Pure-Rust, dep-free, unit-testable. The [`RbacPolicy`] is serde-serializable so it
//! persists exactly like an `AgentIdentity` (in-memory in the `IsolationLayer`,
//! replayable/serializable). An EMPTY policy has no grants, so the evaluator never
//! fires and the existing RLS/ACL result is returned unchanged (backward-compatible).

use std::collections::{HashMap, HashSet};

use crate::acl::{Grant, GrantEffect, RbacAction, ResourceContext, Role};
use serde::{Deserialize, Serialize};

/// The durable RBAC policy: named roles (with parents) + a flat list of grants.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RbacPolicy {
    /// Role name → definition (carries its parents for hierarchy expansion).
    roles: HashMap<String, Role>,
    /// All grants; evaluation filters by role membership + resource match.
    grants: Vec<Grant>,
}

impl RbacPolicy {
    pub fn new() -> Self {
        RbacPolicy::default()
    }

    /// True while no grants are configured. The `IsolationLayer` uses this to keep
    /// the existing ACL/RLS path a NO-OP for single-tenant / pre-RBAC deployments.
    pub fn is_empty(&self) -> bool {
        self.grants.is_empty()
    }

    /// Add or replace a role definition.
    pub fn add_role(&mut self, role: Role) {
        self.roles.insert(role.name.clone(), role);
    }

    /// Remove a role definition (its grants, if any, remain until removed).
    pub fn remove_role(&mut self, name: &str) {
        self.roles.remove(name);
    }

    /// Add a grant (duplicates are collapsed).
    pub fn add_grant(&mut self, grant: Grant) {
        if !self.grants.contains(&grant) {
            self.grants.push(grant);
        }
    }

    /// Remove an exact grant. Returns true when one was removed.
    pub fn remove_grant(&mut self, grant: &Grant) -> bool {
        let before = self.grants.len();
        self.grants.retain(|g| g != grant);
        self.grants.len() != before
    }

    pub fn roles(&self) -> impl Iterator<Item = &Role> {
        self.roles.values()
    }

    pub fn grants(&self) -> &[Grant] {
        &self.grants
    }

    /// Expand a set of role names to include every transitively-reachable parent
    /// (cycle-safe). A parent named in `Role::parents` that has no definition is
    /// still included as a bare name — grants may reference it directly.
    pub fn expand_roles<S: AsRef<str>>(&self, names: &[S]) -> HashSet<String> {
        let mut out = HashSet::new();
        let mut stack: Vec<String> = names.iter().map(|s| s.as_ref().to_string()).collect();
        while let Some(name) = stack.pop() {
            if !out.insert(name.clone()) {
                continue; // already visited — breaks cycles.
            }
            if let Some(role) = self.roles.get(&name) {
                for parent in &role.parents {
                    if !out.contains(parent) {
                        stack.push(parent.clone());
                    }
                }
            }
        }
        out
    }

    /// Evaluate the policy for an agent's `role_names` against `(resource, action)`.
    /// Returns the winning [`GrantEffect`], or `None` when NO grant applies (the
    /// caller treats `None` as deny — rule 1, default-deny).
    pub fn evaluate<S: AsRef<str>>(
        &self,
        role_names: &[S],
        resource: &ResourceContext,
        action: RbacAction,
    ) -> Option<GrantEffect> {
        let expanded = self.expand_roles(role_names);
        // Track the best (most-specific) match; ties broken by deny-over-allow.
        let mut best: Option<(u8, GrantEffect)> = None;
        for g in &self.grants {
            if g.action != action
                || !expanded.contains(&g.role)
                || !g.resource.matches(resource)
            {
                continue;
            }
            let spec = g.resource.specificity();
            match best {
                None => best = Some((spec, g.effect)),
                Some((bspec, beffect)) => {
                    if spec > bspec {
                        best = Some((spec, g.effect)); // more specific wins (rule 2).
                    } else if spec == bspec
                        && g.effect == GrantEffect::Deny
                        && beffect == GrantEffect::Allow
                    {
                        best = Some((spec, GrantEffect::Deny)); // deny overrides (rule 3).
                    }
                }
            }
        }
        best.map(|(_, effect)| effect)
    }

    /// Convenience default-deny predicate: true only when [`evaluate`] returns
    /// `Some(Allow)`.
    ///
    /// [`evaluate`]: RbacPolicy::evaluate
    pub fn is_allowed<S: AsRef<str>>(
        &self,
        role_names: &[S],
        resource: &ResourceContext,
        action: RbacAction,
    ) -> bool {
        matches!(
            self.evaluate(role_names, resource, action),
            Some(GrantEffect::Allow)
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::acl::ResourceSelector;

    fn grant(role: &str, sel: ResourceSelector, action: RbacAction, effect: GrantEffect) -> Grant {
        Grant {
            role: role.into(),
            resource: sel,
            action,
            effect,
        }
    }

    #[test]
    fn child_inherits_parent_grants() {
        let mut p = RbacPolicy::new();
        // reader ⊂ editor ⊂ admin (child lists parent).
        p.add_role(Role::new("reader"));
        p.add_role(Role::with_parents("editor", vec!["reader".into()]));
        p.add_role(Role::with_parents("admin", vec!["editor".into()]));
        p.add_grant(grant(
            "reader",
            ResourceSelector::All,
            RbacAction::Read,
            GrantEffect::Allow,
        ));
        // An "admin" agent inherits reader's Read grant transitively.
        assert!(p.is_allowed(&["admin"], &ResourceContext::graph("g"), RbacAction::Read));
        // Expansion includes both parents.
        let exp = p.expand_roles(&["admin"]);
        assert!(exp.contains("admin") && exp.contains("editor") && exp.contains("reader"));
    }

    #[test]
    fn role_hierarchy_cycle_is_safe() {
        let mut p = RbacPolicy::new();
        p.add_role(Role::with_parents("a", vec!["b".into()]));
        p.add_role(Role::with_parents("b", vec!["a".into()]));
        let exp = p.expand_roles(&["a"]);
        assert!(exp.contains("a") && exp.contains("b"));
    }

    #[test]
    fn deny_overrides_allow_at_same_specificity() {
        let mut p = RbacPolicy::new();
        p.add_grant(grant(
            "r",
            ResourceSelector::Graph("g".into()),
            RbacAction::Write,
            GrantEffect::Allow,
        ));
        p.add_grant(grant(
            "r",
            ResourceSelector::Graph("g".into()),
            RbacAction::Write,
            GrantEffect::Deny,
        ));
        assert_eq!(
            p.evaluate(&["r"], &ResourceContext::graph("g"), RbacAction::Write),
            Some(GrantEffect::Deny)
        );
    }

    #[test]
    fn most_specific_resource_wins() {
        let mut p = RbacPolicy::new();
        // Broad Deny on All, specific Allow on the named graph ⇒ specific Allow wins.
        p.add_grant(grant(
            "r",
            ResourceSelector::All,
            RbacAction::Read,
            GrantEffect::Deny,
        ));
        p.add_grant(grant(
            "r",
            ResourceSelector::Graph("g".into()),
            RbacAction::Read,
            GrantEffect::Allow,
        ));
        assert_eq!(
            p.evaluate(&["r"], &ResourceContext::graph("g"), RbacAction::Read),
            Some(GrantEffect::Allow)
        );
        // For a DIFFERENT graph only the broad Deny applies.
        assert_eq!(
            p.evaluate(&["r"], &ResourceContext::graph("other"), RbacAction::Read),
            Some(GrantEffect::Deny)
        );
    }

    #[test]
    fn default_deny_for_unlisted_action() {
        let mut p = RbacPolicy::new();
        p.add_grant(grant(
            "r",
            ResourceSelector::All,
            RbacAction::Read,
            GrantEffect::Allow,
        ));
        // A Write is not granted anywhere ⇒ no match ⇒ default deny.
        assert_eq!(
            p.evaluate(&["r"], &ResourceContext::graph("g"), RbacAction::Write),
            None
        );
        assert!(!p.is_allowed(&["r"], &ResourceContext::graph("g"), RbacAction::Write));
    }

    #[test]
    fn no_grant_for_unheld_role_is_deny() {
        let mut p = RbacPolicy::new();
        p.add_grant(grant(
            "admin",
            ResourceSelector::All,
            RbacAction::Write,
            GrantEffect::Allow,
        ));
        // Agent only holds "reader" ⇒ the admin grant does not apply.
        assert_eq!(
            p.evaluate(&["reader"], &ResourceContext::graph("g"), RbacAction::Write),
            None
        );
    }

    #[test]
    fn empty_policy_evaluates_to_none() {
        let p = RbacPolicy::new();
        assert!(p.is_empty());
        assert_eq!(
            p.evaluate(&["anyone"], &ResourceContext::graph("g"), RbacAction::Read),
            None
        );
    }

    #[test]
    fn policy_roundtrips_through_serde() {
        let mut p = RbacPolicy::new();
        p.add_role(Role::with_parents("editor", vec!["reader".into()]));
        p.add_grant(grant(
            "editor",
            ResourceSelector::Label("Doc".into()),
            RbacAction::Write,
            GrantEffect::Allow,
        ));
        let s = serde_json::to_string(&p).unwrap();
        let back: RbacPolicy = serde_json::from_str(&s).unwrap();
        assert_eq!(back.grants().len(), 1);
        assert!(back.is_allowed(
            &["editor"],
            &ResourceContext {
                graph: "g".into(),
                label: Some("Doc".into())
            },
            RbacAction::Write
        ));
    }
}
