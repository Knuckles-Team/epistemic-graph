//! ICV write-path enforcement — the per-graph policy that turns [`crate::check_write`]
//! into a **constraint-enforced transaction** guard (CONCEPT:EG-KG.ontology.rdf-update-guard).
//!
//! [`crate::icv::check_write`] is the pure decision function: given a base graph and a
//! proposed set of additions/removals, does the change INTRODUCE an integrity violation?
//! This module wires that decision into eg-rdf's commit path via the [`eg_rdf::guard::WriteGuard`]
//! hook (eg-rdf defines the hook; eg-shacl — which sits ABOVE eg-rdf — implements it, so
//! there is no dependency cycle).
//!
//! A graph carries an [`IcvPolicy`]: its registered shapes graph. Every registered
//! policy enforces. An absent policy is itself a hard rejection, so a caller cannot
//! turn integrity off or convert a violation into an advisory write.
//!
//! [`IcvPolicyRegistry`] maps graph names → policies and implements [`eg_rdf::guard::WriteGuard`],
//! so `eg_rdf::update::execute` enforces ICV over every write.

use std::collections::HashMap;

use crate::icv::{check_write, WriteCheck};
use crate::validate::graph_from_turtle;
use eg_rdf::guard::{GuardRejection, WriteGuard};
use eg_rdf::oxrdf::{Graph, Triple};

/// A per-graph ICV policy (CONCEPT:EG-KG.ontology.rdf-update-guard): the mandatory
/// registered integrity shapes.
#[derive(Debug, Clone)]
pub struct IcvPolicy {
    /// The SHACL shapes read as closed-world integrity constraints for this graph.
    pub shapes: Graph,
}

impl IcvPolicy {
    /// A policy over an already-parsed shapes graph.
    pub fn new(shapes: Graph) -> Self {
        IcvPolicy { shapes }
    }

    /// A policy whose shapes are parsed from a Turtle document. Returns a parse-error
    /// string if the shapes fail to parse.
    pub fn from_turtle(shapes_ttl: &str) -> Result<Self, String> {
        Ok(IcvPolicy {
            shapes: graph_from_turtle(shapes_ttl)?,
        })
    }

    /// The pure ICV write check for this policy's shapes (delegates to [`check_write`]).
    pub fn check(&self, base: &Graph, additions: &[Triple], removals: &[Triple]) -> WriteCheck {
        check_write(&self.shapes, base, additions, removals)
    }
}

/// A registry of per-graph ICV policies (CONCEPT:EG-KG.ontology.rdf-update-guard) that implements the eg-rdf
/// [`WriteGuard`] hook. Register a policy for the default graph and/or named graphs, then
/// pass it to `eg_rdf::update::execute` to enforce ICV on commit.
#[derive(Debug, Clone, Default)]
pub struct IcvPolicyRegistry {
    /// Policy for the default graph, if any.
    default: Option<IcvPolicy>,
    /// Policies for named graphs, keyed by bare IRI.
    named: HashMap<String, IcvPolicy>,
}

impl IcvPolicyRegistry {
    /// A new fail-closed registry. Graph writes remain unavailable until the graph or
    /// default policy is explicitly registered.
    pub fn new() -> Self {
        Self::default()
    }

    /// Register (or replace) the policy for a graph — `None` = the default graph.
    pub fn set(&mut self, graph: Option<&str>, policy: IcvPolicy) -> &mut Self {
        match graph {
            None => self.default = Some(policy),
            Some(g) => {
                self.named.insert(g.to_string(), policy);
            }
        }
        self
    }

    /// Builder-style convenience: register a policy and return `self`.
    pub fn with(mut self, graph: Option<&str>, policy: IcvPolicy) -> Self {
        self.set(graph, policy);
        self
    }

    /// The policy governing a graph, if one is registered.
    pub fn policy(&self, graph: Option<&str>) -> Option<&IcvPolicy> {
        match graph {
            None => self.default.as_ref(),
            Some(g) => self.named.get(g),
        }
    }
}

impl WriteGuard for IcvPolicyRegistry {
    fn check_graph(
        &self,
        graph: Option<&str>,
        base: &Graph,
        additions: &[Triple],
        removals: &[Triple],
    ) -> Result<(), GuardRejection> {
        let Some(policy) = self.policy(graph) else {
            return Err(GuardRejection {
                graph: graph.map(str::to_string),
                message: "EG-KG.ontology.rdf-update-guard: no integrity policy is registered"
                    .to_string(),
                details: serde_json::json!({"reason": "integrity_policy_required"}),
            });
        };
        let check = policy.check(base, additions, removals);
        if check.accepted {
            Ok(())
        } else {
            Err(GuardRejection {
                graph: graph.map(|g| g.to_string()),
                message: format!(
                    "EG-KG.ontology.rdf-update-guard: change introduces {} integrity constraint violation(s)",
                    check.introduced.len()
                ),
                details: serde_json::to_value(&check.introduced)
                    .unwrap_or(serde_json::Value::Null),
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const PREFIXES: &str = r#"
@prefix sh:   <http://www.w3.org/ns/shacl#> .
@prefix rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix ex:   <http://example.org/> .
"#;

    // A person may have at most one manager (a resource-valued maxCount — survives the
    // property-graph mapping, unlike a duplicate literal which the key-unique blob folds).
    const SHAPES: &str = r#"
ex:PersonShape a sh:NodeShape ;
    sh:targetClass ex:Person ;
    sh:property [ sh:path ex:manager ; sh:maxCount 1 ] .
"#;

    fn shapes_graph() -> Graph {
        graph_from_turtle(&format!("{PREFIXES}{SHAPES}")).unwrap()
    }

    fn triples(ttl: &str) -> Vec<Triple> {
        graph_from_turtle(&format!("{PREFIXES}{ttl}"))
            .unwrap()
            .iter()
            .map(|t| t.into_owned())
            .collect()
    }

    fn base_graph() -> Graph {
        graph_from_turtle(&format!(
            "{PREFIXES}{}",
            "ex:a a ex:Person ; ex:manager ex:m1 ."
        ))
        .unwrap()
    }

    #[test]
    fn enforce_rejects_violating_change() {
        let registry = IcvPolicyRegistry::new().with(None, IcvPolicy::new(shapes_graph()));
        let base = base_graph();
        // A SECOND manager breaks sh:maxCount 1.
        let add = triples("ex:a ex:manager ex:m2 .");
        let err = registry
            .check_graph(None, &base, &add, &[])
            .expect_err("enforce must reject a maxCount-breaking add");
        assert_eq!(err.graph, None);
        assert!(err.message.contains("EG-KG.ontology.rdf-update-guard"));
        // The structured detail carries the introduced violation(s) with their witness.
        let details = err.details.as_array().expect("details is an array");
        assert!(!details.is_empty());
        assert!(err.details.to_string().contains("witness"));
    }

    #[test]
    fn enforce_accepts_clean_change() {
        let registry = IcvPolicyRegistry::new().with(None, IcvPolicy::new(shapes_graph()));
        let base = base_graph();
        // An unrelated fact introduces no violation.
        let add = triples("ex:a ex:nickname \"Ay\" .");
        assert!(registry.check_graph(None, &base, &add, &[]).is_ok());
    }

    #[test]
    fn absent_policy_rejects_the_write() {
        let registry = IcvPolicyRegistry::new();
        let base = base_graph();
        let add = triples("ex:a ex:manager ex:m2 .");
        let error = registry.check_graph(None, &base, &add, &[]).unwrap_err();
        assert_eq!(error.details["reason"], "integrity_policy_required");
    }

    #[test]
    fn from_turtle_parses_shapes() {
        let policy = IcvPolicy::from_turtle(&format!("{PREFIXES}{SHAPES}")).unwrap();
        assert!(!policy.shapes.is_empty());
    }
}
