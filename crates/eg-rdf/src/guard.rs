//! EG-300 — a pluggable **write-time constraint guard** for the SPARQL UPDATE / commit
//! path.
//!
//! eg-rdf owns the transactional commit path ([`crate::update::execute`]) but sits BELOW
//! the SHACL/ICV engine in the DAG (eg-shacl depends on eg-rdf, not the reverse), so it
//! cannot call the ICV check directly without a dependency cycle. This module inverts the
//! dependency: eg-rdf defines the [`WriteGuard`] hook the commit path invokes, and the
//! upper layer (eg-shacl's ICV policy, CONCEPT:EG-KG.ontology.wired-into-commit-write/EG-300) *implements* it.
//!
//! The guard is handed, per graph, the pre-change base graph plus the NET additions /
//! removals a change set would apply, and decides whether the commit is allowed. A
//! rejection carries a structured [`GuardRejection`] (the violating detail as JSON, so
//! eg-rdf stays decoupled from the SHACL result types) which [`crate::update::execute_guarded`]
//! surfaces as a hard error WITHOUT touching the real store.

use crate::oxrdf::{Graph, Triple};

/// A write-time constraint guard (CONCEPT:EG-KG.ontology.rdf-update-guard). The commit path calls
/// [`WriteGuard::check_graph`] once per affected graph with the projected change; the
/// implementation (e.g. eg-shacl's ICV policy) returns `Ok(())` to allow the commit or a
/// [`GuardRejection`] to abort it.
pub trait WriteGuard {
    /// Whether this guard is doing anything. When `false`, [`crate::update::execute_guarded`]
    /// skips the (cost-bearing) simulate-and-diff transaction entirely and applies the
    /// update directly — so an all-`Off` policy is byte-for-byte the plain write path.
    fn active(&self) -> bool {
        true
    }

    /// Decide whether the net change to a single graph is allowed. `graph` is the bare
    /// named-graph IRI, or `None` for the default graph. `base` is the graph BEFORE the
    /// change; `additions` / `removals` are the net triples the change set would apply.
    fn check_graph(
        &self,
        graph: Option<&str>,
        base: &Graph,
        additions: &[Triple],
        removals: &[Triple],
    ) -> Result<(), GuardRejection>;
}

/// A structured commit rejection (CONCEPT:EG-KG.ontology.rdf-update-guard): which graph was refused, a
/// human-readable summary, and the guard's own violation payload as JSON (e.g. eg-shacl's
/// introduced `IcvViolation`s, each with its SPARQL witness). Kept as `serde_json::Value`
/// so eg-rdf never links the SHACL result types.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct GuardRejection {
    /// The graph whose commit was refused (`None` = the default graph).
    pub graph: Option<String>,
    /// A human-readable summary of why the commit was refused.
    pub message: String,
    /// The guard's structured detail — for the ICV guard, the introduced violations
    /// (each carrying its SPARQL witness), as JSON.
    pub details: serde_json::Value,
}

impl std::fmt::Display for GuardRejection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let g = self.graph.as_deref().unwrap_or("(default)");
        write!(f, "write rejected for graph {g}: {}", self.message)
    }
}

impl std::error::Error for GuardRejection {}
