//! A per-principal allowlist of control-lease kinds (operator ruling
//! 2026-09-24, finance-ops).
//!
//! A deputy executor such as graph-os may hold `lease:write` so it can record
//! the leases its own features need, e.g. a `finance.order-proposal`. Holding
//! the scope must not let it write ANY kind -- in particular not one an
//! approver or the two-person elevation flow owns. This policy names, per
//! verified `agent_id`, the only kinds that principal may issue or
//! transition. A principal the policy does not name is unaffected; a named
//! principal with an empty list may write no control lease at all.
//!
//! The policy is deploy configuration (JSON object `agent_id -> [kind, ...]`),
//! not code: no principal name is built in.

use std::collections::{BTreeMap, BTreeSet};

use serde::Deserialize;

/// Bound on a principal id or a kind named by the policy.
const MAX_POLICY_REF_BYTES: usize = 512;

/// `agent_id -> the only control-lease kinds it may write`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
#[serde(transparent)]
pub struct ControlLeaseKindPolicy {
    restricted: BTreeMap<String, BTreeSet<String>>,
}

fn well_formed(value: &str) -> bool {
    !value.trim().is_empty() && value.len() <= MAX_POLICY_REF_BYTES
}

impl ControlLeaseKindPolicy {
    /// Parse the deploy policy. Any malformed entry fails the whole policy,
    /// so a typo never silently lifts a restriction.
    pub fn from_json(raw: &str) -> Result<Self, String> {
        let policy: Self = serde_json::from_str(raw)
            .map_err(|error| format!("control lease kind policy is not valid JSON: {error}"))?;
        let malformed = policy
            .restricted
            .iter()
            .any(|(agent, kinds)| !well_formed(agent) || !kinds.iter().all(|k| well_formed(k)));
        if malformed {
            return Err("control lease kind policy names an empty or oversized id".to_string());
        }
        Ok(policy)
    }

    /// Whether the policy restricts `agent_id` at all.
    pub fn restricts(&self, agent_id: &str) -> bool {
        self.restricted.contains_key(agent_id)
    }

    /// Whether `agent_id` may issue or transition a lease of `kind`.
    pub fn permits(&self, agent_id: &str, kind: &str) -> bool {
        match self.restricted.get(agent_id) {
            Some(kinds) => kinds.contains(kind),
            None => true,
        }
    }

    /// `Ok` when permitted, else the `ACCESS_DENIED` refusal.
    pub fn require(&self, agent_id: &str, kind: &str) -> Result<(), String> {
        if self.permits(agent_id, kind) {
            return Ok(());
        }
        Err(format!(
            "ACCESS_DENIED: this principal may not write control leases of kind '{kind}'"
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::ControlLeaseKindPolicy;

    fn policy() -> ControlLeaseKindPolicy {
        ControlLeaseKindPolicy::from_json(
            r#"{"service:deputy": ["finance.order-proposal"], "service:mute": []}"#,
        )
        .expect("well-formed policy")
    }

    #[test]
    fn a_restricted_principal_writes_only_its_listed_kinds() {
        let policy = policy();
        assert!(policy.restricts("service:deputy"));
        assert!(policy
            .require("service:deputy", "finance.order-proposal")
            .is_ok());
        for refused in ["browser.control", "rbac.elevation", "action.approval"] {
            let error = policy.require("service:deputy", refused).unwrap_err();
            assert!(error.starts_with("ACCESS_DENIED"), "{refused}: {error}");
        }
        assert!(!policy.permits("service:mute", "finance.order-proposal"));
    }

    #[test]
    fn an_unnamed_principal_and_an_empty_policy_are_unaffected() {
        let policy = policy();
        assert!(!policy.restricts("person:operator"));
        assert!(policy.permits("person:operator", "rbac.elevation"));
        assert!(ControlLeaseKindPolicy::default().permits("service:deputy", "anything"));
    }

    #[test]
    fn a_malformed_policy_is_refused_whole() {
        for raw in [
            "[]",
            r#"{"service:deputy": "finance.order-proposal"}"#,
            r#"{"": ["finance.order-proposal"]}"#,
            r#"{"service:deputy": [" "]}"#,
        ] {
            assert!(ControlLeaseKindPolicy::from_json(raw).is_err(), "{raw}");
        }
    }
}
