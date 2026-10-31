//! Durable run-scope bound (MEDULLA-IDENTITY-R008).
//!
//! A run's tool and data scope comes from a STORED policy node, never from plan
//! text: [`ScopeBound`] carries the policy IRI, the allowed tool IRIs and the
//! allowed graphs, and [`scope_within_bound`] checks a plan's tool list against
//! that bound in isolation from any scope field the plan body might carry.
//! Both halves are pure and fail-closed: a bound without a policy IRI is
//! refused at construction, and the subset check NAMES the first out-of-bound
//! tool instead of returning a bare boolean, so a refusal downstream can
//! audit which tool exactly crossed the line.

use std::collections::BTreeSet;

/// A refused [`ScopeBound`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScopeBoundError {
    /// The `policy_iri` was empty (or blank). A bound without a policy source
    /// has nothing to check against and is denied fail-closed.
    EmptyPolicyIri,
}

impl std::fmt::Display for ScopeBoundError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::EmptyPolicyIri => {
                f.write_str("ScopeBound refused: policy_iri is empty (fail-closed)")
            }
        }
    }
}

impl std::error::Error for ScopeBoundError {}

/// The durable scope a run may not exceed: the policy IRI it was read from,
/// the tool IRIs the policy allows, and the graphs the policy allows.
///
/// The `ScopeBound { policy_iri, allowed_tools, allowed_graphs }` contract of
/// MEDULLA-IDENTITY-R008.2: construction REFUSES an empty `policy_iri` with
/// [`ScopeBoundError::EmptyPolicyIri`]; an empty `allowed_tools` is legal and
/// simply means "no tools allowed".
///
/// External callers cannot bypass construction:
/// ```compile_fail,E0451
/// use medulla_core::isolation::ScopeBound;
/// let _ = ScopeBound {
///     policy_iri: String::new(),
///     allowed_tools: vec![],
///     allowed_graphs: vec![],
/// };
/// ```
/// Nor can they clear a validated policy IRI:
/// ```compile_fail,E0616
/// use medulla_core::isolation::ScopeBound;
/// let mut bound = ScopeBound::new("urn:policy:worker", vec![], vec![]).unwrap();
/// bound.policy_iri.clear();
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScopeBound {
    /// IRI of the stored policy node this bound was read from. Never empty.
    policy_iri: String,
    /// Tool IRIs the policy allows the run to name.
    pub allowed_tools: Vec<String>,
    /// Graph names the policy allows the run to touch.
    pub allowed_graphs: Vec<String>,
}

impl ScopeBound {
    /// Read the policy IRI without permitting mutation.
    pub fn policy_iri(&self) -> &str {
        &self.policy_iri
    }

    /// Build a bound, refusing an empty `policy_iri` fail-closed.
    pub fn new(
        policy_iri: &str,
        allowed_tools: Vec<String>,
        allowed_graphs: Vec<String>,
    ) -> Result<Self, ScopeBoundError> {
        if policy_iri.trim().is_empty() {
            return Err(ScopeBoundError::EmptyPolicyIri);
        }
        Ok(Self {
            policy_iri: policy_iri.to_string(),
            allowed_tools,
            allowed_graphs,
        })
    }
}

/// Pure subset check (MEDULLA-IDENTITY-R008.3): the FIRST plan tool IRI that is not
/// in `bound.allowed_tools`, or `None` when every planned tool is inside the
/// bound. Returning the named IRI (not a boolean) is what lets the admission
/// path refuse with `ScopeExceedsPolicy` naming the offending tool and the
/// bound's `policy_iri`.
pub fn scope_within_bound<'a>(plan_tools: &'a [String], bound: &ScopeBound) -> Option<&'a str> {
    let allowed: BTreeSet<&str> = bound.allowed_tools.iter().map(String::as_str).collect();
    plan_tools
        .iter()
        .find(|tool| !allowed.contains(tool.as_str()))
        .map(String::as_str)
}

#[cfg(test)]
mod tests {
    use super::*;

    // spec: MEDULLA-IDENTITY-R008.2
    #[test]
    fn empty_policy_iri_is_refused() {
        for policy_iri in ["", "   "] {
            assert_eq!(
                ScopeBound::new(policy_iri, vec![], vec![]),
                Err(ScopeBoundError::EmptyPolicyIri),
                "an empty policy_iri must be refused fail-closed"
            );
        }
        assert!(matches!(
            ScopeBound::new("", vec![], vec![]),
            Err(ScopeBoundError::EmptyPolicyIri)
        ));
    }

    // spec: MEDULLA-IDENTITY-R008.3
    #[test]
    fn first_excess_tool_is_named() {
        let bound = ScopeBound::new(
            "http://ex.org/policy/worker",
            vec!["http://ex.org/tools/query".to_string()],
            vec!["__commons__".to_string()],
        )
        .unwrap_or_else(|e| panic!("a non-empty policy_iri must construct: {e}"));
        let plan = vec![
            "http://ex.org/tools/query".to_string(),
            "http://ex.org/tools/write".to_string(),
            "http://ex.org/tools/erase".to_string(),
        ];
        assert_eq!(
            scope_within_bound(&plan, &bound),
            Some("http://ex.org/tools/write"),
            "the FIRST out-of-bound tool must be named, not just detected"
        );
    }

    // spec: MEDULLA-IDENTITY-R008.2
    #[test]
    fn valid_bound_round_trips_its_fields() {
        let bound = ScopeBound::new(
            "http://ex.org/policy/worker",
            vec!["http://ex.org/tools/query".to_string()],
            vec!["__commons__".to_string(), "agent:worker".to_string()],
        )
        .expect("a non-empty policy_iri must construct");
        assert_eq!(bound.policy_iri(), "http://ex.org/policy/worker");
        assert_eq!(
            bound.allowed_tools,
            vec!["http://ex.org/tools/query".to_string()]
        );
        assert_eq!(
            bound.allowed_graphs,
            vec!["__commons__".to_string(), "agent:worker".to_string()]
        );
    }

    // spec: MEDULLA-IDENTITY-R008.3
    #[test]
    fn in_bound_plan_returns_none() {
        let bound = ScopeBound::new(
            "http://ex.org/policy/worker",
            vec![
                "http://ex.org/tools/query".to_string(),
                "http://ex.org/tools/insert".to_string(),
            ],
            vec!["__commons__".to_string()],
        )
        .unwrap_or_else(|e| panic!("a non-empty policy_iri must construct: {e}"));
        let plan = vec![
            "http://ex.org/tools/query".to_string(),
            "http://ex.org/tools/insert".to_string(),
        ];
        assert_eq!(
            scope_within_bound(&plan, &bound),
            None,
            "a fully in-bound plan must pass quietly"
        );
        assert_eq!(scope_within_bound(&[], &bound), None);
    }
}
