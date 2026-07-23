//! Mandatory graph-scoped SHACL integrity enforcement.
//!
//! The validated Turtle source is authoritative graph control state in
//! [`GraphCore`]. It therefore participates in the same staged MutationBatch,
//! redb transaction, recovery image, and Raft snapshot as the rows it governs.
//! This module only compiles that source into eg-shacl's native guard at the
//! write boundary; it owns no process-global or pre-commit policy mirror.

use std::collections::HashMap;
use std::sync::Arc;

use eg_rdf::guard::{GuardRejection, WriteGuard};
use eg_rdf::oxrdf::{Graph, Triple};
use eg_shacl::policy::{IcvPolicy, IcvPolicyRegistry};

use crate::graph::{GraphCore, IntegrityPolicy};

/// Stage a current enforcing policy on the request graph. The optional wire
/// target is an assertion, not an alternate route: cross-graph configuration
/// must be dispatched and authorized against that graph explicitly.
pub(crate) fn configure(
    core: &GraphCore,
    request_graph: &str,
    graph: Option<&str>,
    mode: &str,
    shapes_ttl: &str,
) -> Result<(), String> {
    if request_graph.trim().is_empty() {
        return Err("IcvConfigure: request graph must not be empty".to_string());
    }
    if graph.is_some_and(|target| target != request_graph) {
        return Err(
            "IcvConfigure: target graph must match the authorized request graph".to_string(),
        );
    }
    if !mode.eq_ignore_ascii_case("enforce") {
        return Err("IcvConfigure: current policy mode must be 'enforce'".to_string());
    }
    if shapes_ttl.trim().is_empty() {
        return Err("IcvConfigure: a non-empty `shapes_ttl` is required".to_string());
    }
    IcvPolicy::from_turtle(shapes_ttl)
        .map_err(|error| format!("IcvConfigure: bad shapes graph: {error}"))?;
    core.set_integrity_policy(IntegrityPolicy {
        shapes_ttl: shapes_ttl.to_string(),
    });
    Ok(())
}

enum PolicyAuthority<'a> {
    Single(&'a GraphCore),
    Routed(&'a HashMap<String, Arc<GraphCore>>),
}

/// A derived write guard backed exclusively by authoritative graph images.
/// `Single` is used by graph-scoped RPC mutation staging; `Routed` follows the
/// SPARQL endpoint store's `"" = default graph` convention.
pub(crate) struct CoreIcvGuard<'a> {
    authority: PolicyAuthority<'a>,
}

impl<'a> CoreIcvGuard<'a> {
    pub(crate) fn single(core: &'a GraphCore) -> Self {
        Self {
            authority: PolicyAuthority::Single(core),
        }
    }

    pub(crate) fn routed(graphs: &'a HashMap<String, Arc<GraphCore>>) -> Self {
        Self {
            authority: PolicyAuthority::Routed(graphs),
        }
    }

    fn resolve(&self, graph: Option<&str>) -> Option<&GraphCore> {
        match &self.authority {
            PolicyAuthority::Single(core) => Some(*core),
            PolicyAuthority::Routed(graphs) => graphs.get(graph.unwrap_or("")).map(Arc::as_ref),
        }
    }
}

impl WriteGuard for CoreIcvGuard<'_> {
    fn check_graph(
        &self,
        graph: Option<&str>,
        base: &Graph,
        additions: &[Triple],
        removals: &[Triple],
    ) -> Result<(), GuardRejection> {
        let Some(core) = self.resolve(graph) else {
            return Err(GuardRejection {
                graph: graph.map(str::to_string),
                message: "EG-KG.ontology.rdf-update-guard: graph authority is not loaded"
                    .to_string(),
                details: serde_json::json!({"reason": "graph_authority_required"}),
            });
        };
        let Some(authority) = core.integrity_policy() else {
            return Err(GuardRejection {
                graph: graph.map(str::to_string),
                message: "EG-KG.ontology.rdf-update-guard: no integrity policy is registered"
                    .to_string(),
                details: serde_json::json!({"reason": "integrity_policy_required"}),
            });
        };
        let policy = IcvPolicy::from_turtle(&authority.shapes_ttl).map_err(|_| GuardRejection {
            graph: graph.map(str::to_string),
            message: "EG-KG.ontology.rdf-update-guard: authoritative integrity policy is invalid"
                .to_string(),
            details: serde_json::json!({"reason": "integrity_policy_invalid"}),
        })?;
        IcvPolicyRegistry::new()
            .with(graph, policy)
            .check_graph(graph, base, additions, removals)
    }
}

/// Enforce the policy on a direct property-graph RDF write. Export failure and
/// absent/invalid policy are hard rejections before any live mutation occurs.
pub(crate) fn check_before_write(
    core: &Arc<GraphCore>,
    graph_name: &str,
    additions: &[Triple],
    removals: &[Triple],
) -> Result<(), GuardRejection> {
    let exported =
        eg_rdf::mapping::export_triples(core, graph_name).map_err(|error| GuardRejection {
            graph: Some(graph_name.to_string()),
            message: format!(
                "EG-KG.ontology.rdf-update-guard: could not read the base graph to evaluate ICV: {error}"
            ),
            details: serde_json::Value::Null,
        })?;
    let mut base = Graph::new();
    for triple in &exported {
        base.insert(triple);
    }
    CoreIcvGuard::single(core.as_ref()).check_graph(Some(graph_name), &base, additions, removals)
}

#[cfg(test)]
mod tests {
    use super::*;

    const PREFIXES: &str = r#"
@prefix sh:   <http://www.w3.org/ns/shacl#> .
@prefix ex:   <http://example.org/> .
"#;
    const SHAPES: &str = r#"
ex:PersonShape a sh:NodeShape ;
    sh:targetClass ex:Person ;
    sh:property [ sh:path ex:manager ; sh:maxCount 1 ] .
"#;

    fn triple(s: &str, p: &str, o: &str) -> Triple {
        use eg_rdf::oxrdf::NamedNode;
        Triple::new(
            NamedNode::new_unchecked(s),
            NamedNode::new_unchecked(p),
            NamedNode::new_unchecked(o),
        )
    }

    fn check(
        core: &Arc<GraphCore>,
        graph_name: &str,
        additions: &[Triple],
    ) -> Result<(), GuardRejection> {
        check_before_write(core, graph_name, additions, &[])
    }

    #[test]
    fn unconfigured_graph_is_rejected() {
        let core = Arc::new(GraphCore::new());
        let add = vec![triple(
            "http://example.org/a",
            "http://example.org/manager",
            "http://example.org/m2",
        )];
        let error = check(&core, "graph", &add).unwrap_err();
        assert_eq!(error.details["reason"], "integrity_policy_required");
    }

    #[test]
    fn enforce_rejects_a_shape_violation() {
        let core = Arc::new(GraphCore::new());
        configure(
            core.as_ref(),
            "graph",
            Some("graph"),
            "enforce",
            &format!("{PREFIXES}{SHAPES}"),
        )
        .unwrap();
        let add = vec![
            triple(
                "http://example.org/a",
                "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
                "http://example.org/Person",
            ),
            triple(
                "http://example.org/a",
                "http://example.org/manager",
                "http://example.org/m1",
            ),
            triple(
                "http://example.org/a",
                "http://example.org/manager",
                "http://example.org/m2",
            ),
        ];
        let error = check(&core, "graph", &add).unwrap_err();
        assert!(error.details.to_string().contains("witness"));
    }

    #[test]
    fn enforce_accepts_a_clean_change() {
        let core = Arc::new(GraphCore::new());
        configure(
            core.as_ref(),
            "graph",
            None,
            "enforce",
            &format!("{PREFIXES}{SHAPES}"),
        )
        .unwrap();
        let add = vec![
            triple(
                "http://example.org/a",
                "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
                "http://example.org/Person",
            ),
            triple(
                "http://example.org/a",
                "http://example.org/manager",
                "http://example.org/m1",
            ),
        ];
        assert!(check(&core, "graph", &add).is_ok());
    }

    #[test]
    fn cross_graph_configuration_is_rejected_without_mutation() {
        let core = GraphCore::new();
        assert!(configure(
            &core,
            "authorized",
            Some("other"),
            "enforce",
            &format!("{PREFIXES}{SHAPES}"),
        )
        .is_err());
        assert!(core.integrity_policy().is_none());
    }

    #[test]
    fn retired_modes_and_empty_shapes_are_rejected() {
        let core = GraphCore::new();
        assert!(configure(&core, "graph", None, "warn", &format!("{PREFIXES}{SHAPES}")).is_err());
        assert!(configure(&core, "graph", None, "off", &format!("{PREFIXES}{SHAPES}")).is_err());
        assert!(configure(&core, "graph", None, "enforce", "").is_err());
    }
}
