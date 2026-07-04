//! ICV write-path enforcement — the per-graph policy that turns [`crate::check_write`]
//! into a **constraint-enforced transaction** guard (CONCEPT:EG-KG.ontology.rdf-update-guard).
//!
//! [`crate::icv::check_write`] is the pure decision function: given a base graph and a
//! proposed set of additions/removals, does the change INTRODUCE an integrity violation?
//! This module wires that decision into eg-rdf's commit path via the [`eg_rdf::guard::WriteGuard`]
//! hook (eg-rdf defines the hook; eg-shacl — which sits ABOVE eg-rdf — implements it, so
//! there is no dependency cycle).
//!
//! A graph carries an [`IcvPolicy`]: the registered shapes graph plus a [`IcvMode`]:
//!
//! * [`IcvMode::Off`] (default) — the guard is inert; the write path is unchanged and
//!   backward-compatible.
//! * [`IcvMode::Warn`] — a change that would introduce a violation is LOGGED (to stderr)
//!   but still applied.
//! * [`IcvMode::Enforce`] — a change that would introduce a violation ABORTS the commit
//!   with a structured [`eg_rdf::guard::GuardRejection`] listing the introduced violations
//!   (each with its SPARQL witness); a clean change commits normally.
//!
//! [`IcvPolicyRegistry`] maps graph names → policies and implements [`eg_rdf::guard::WriteGuard`],
//! so `eg_rdf::update::execute_guarded` enforces ICV over any registered graph.

use std::collections::HashMap;

use eg_rdf::guard::{GuardRejection, WriteGuard};
use eg_rdf::oxrdf::{Graph, Triple};
use serde::{Deserialize, Serialize};

use crate::icv::{check_write, WriteCheck};
use crate::validate::graph_from_turtle;

/// How strictly a graph's ICV shapes are enforced on write (CONCEPT:EG-KG.ontology.rdf-update-guard).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum IcvMode {
    /// The guard is inert — the write path is unchanged (the default, backward-compatible).
    #[default]
    Off,
    /// A violating change is logged (stderr) but still applied.
    Warn,
    /// A violating change aborts the commit with a structured rejection.
    Enforce,
}

/// A per-graph ICV policy (CONCEPT:EG-KG.ontology.rdf-update-guard): the registered shapes + the enforcement mode.
#[derive(Debug, Clone)]
pub struct IcvPolicy {
    /// The enforcement mode.
    pub mode: IcvMode,
    /// The SHACL shapes read as closed-world integrity constraints for this graph.
    pub shapes: Graph,
}

impl IcvPolicy {
    /// A policy over an already-parsed shapes graph.
    pub fn new(mode: IcvMode, shapes: Graph) -> Self {
        IcvPolicy { mode, shapes }
    }

    /// A policy whose shapes are parsed from a Turtle document. Returns a parse-error
    /// string if the shapes fail to parse.
    pub fn from_turtle(mode: IcvMode, shapes_ttl: &str) -> Result<Self, String> {
        Ok(IcvPolicy {
            mode,
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
/// pass it to `eg_rdf::update::execute_guarded` to enforce ICV on commit.
#[derive(Debug, Clone, Default)]
pub struct IcvPolicyRegistry {
    /// Policy for the default graph, if any.
    default: Option<IcvPolicy>,
    /// Policies for named graphs, keyed by bare IRI.
    named: HashMap<String, IcvPolicy>,
}

impl IcvPolicyRegistry {
    /// An empty registry (no policies — an inactive guard).
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

    /// Whether any registered policy is `Warn` or `Enforce` (an active guard).
    pub fn is_active(&self) -> bool {
        self.default
            .iter()
            .chain(self.named.values())
            .any(|p| p.mode != IcvMode::Off)
    }
}

impl WriteGuard for IcvPolicyRegistry {
    fn active(&self) -> bool {
        self.is_active()
    }

    fn check_graph(
        &self,
        graph: Option<&str>,
        base: &Graph,
        additions: &[Triple],
        removals: &[Triple],
    ) -> Result<(), GuardRejection> {
        let Some(policy) = self.policy(graph) else {
            return Ok(()); // no policy for this graph — nothing to enforce.
        };
        match policy.mode {
            IcvMode::Off => Ok(()),
            IcvMode::Warn => {
                let check = policy.check(base, additions, removals);
                if !check.accepted {
                    let g = graph.unwrap_or("(default)");
                    eprintln!(
                        "[EG-300 ICV warn] graph {g}: change would introduce {} integrity \
                         violation(s) — applied anyway (mode=Warn)",
                        check.introduced.len(),
                    );
                }
                Ok(())
            }
            IcvMode::Enforce => {
                let check = policy.check(base, additions, removals);
                if check.accepted {
                    Ok(())
                } else {
                    Err(GuardRejection {
                        graph: graph.map(|g| g.to_string()),
                        message: format!(
                            "EG-300 ICV: change introduces {} integrity constraint violation(s)",
                            check.introduced.len()
                        ),
                        details: serde_json::to_value(&check.introduced)
                            .unwrap_or(serde_json::Value::Null),
                    })
                }
            }
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
        let registry =
            IcvPolicyRegistry::new().with(None, IcvPolicy::new(IcvMode::Enforce, shapes_graph()));
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
        let registry =
            IcvPolicyRegistry::new().with(None, IcvPolicy::new(IcvMode::Enforce, shapes_graph()));
        let base = base_graph();
        // An unrelated fact introduces no violation.
        let add = triples("ex:a ex:nickname \"Ay\" .");
        assert!(registry.check_graph(None, &base, &add, &[]).is_ok());
    }

    #[test]
    fn warn_applies_but_signals_via_ok() {
        let registry =
            IcvPolicyRegistry::new().with(None, IcvPolicy::new(IcvMode::Warn, shapes_graph()));
        let base = base_graph();
        let add = triples("ex:a ex:manager ex:m2 .");
        // Warn NEVER aborts — the guard returns Ok so the commit proceeds (it logs instead).
        assert!(registry.check_graph(None, &base, &add, &[]).is_ok());
    }

    #[test]
    fn off_is_inactive_and_permits_everything() {
        let registry =
            IcvPolicyRegistry::new().with(None, IcvPolicy::new(IcvMode::Off, shapes_graph()));
        assert!(!registry.is_active(), "an all-Off registry is inactive");
        let base = base_graph();
        let add = triples("ex:a ex:manager ex:m2 .");
        assert!(registry.check_graph(None, &base, &add, &[]).is_ok());
    }

    #[test]
    fn from_turtle_parses_shapes() {
        let policy =
            IcvPolicy::from_turtle(IcvMode::Enforce, &format!("{PREFIXES}{SHAPES}")).unwrap();
        assert_eq!(policy.mode, IcvMode::Enforce);
    }
}
