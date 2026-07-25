//! Mandatory graph-scoped SHACL integrity enforcement.
//!
//! The validated Turtle source is authoritative graph control state in
//! [`GraphCore`]. It therefore participates in the same staged MutationBatch,
//! redb transaction, recovery image, and Raft snapshot as the rows it governs.
//! This module only compiles that source into eg-shacl's native guard at the
//! write boundary; it owns no process-global or pre-commit policy mirror.
//!
//! [`check_native_write`] (W4.13) extends the same registered policy to direct
//! property-graph writes (Cypher / native `GraphCore` mutations), gated by the
//! `EPISTEMIC_GRAPH_ICV_NATIVE_WRITES` env var. It is a TWO-level opt-in — the
//! process-wide gate, AND the graph's own registered [`IntegrityPolicy`] — and
//! deliberately NOT fail-closed like the RDF path: a graph that never
//! registered a policy is simply not opted in, so its native writes are
//! unaffected either way.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, OnceLock};

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

/// Process-wide native-write gate (CONCEPT:W4.13), cached for the life of the
/// process. `1`/`true`/`yes`/`on` (case-sensitive, trimmed) enable it; anything
/// else — including unset — leaves native writes unenforced.
fn native_write_gate_enabled() -> bool {
    static GATE: OnceLock<bool> = OnceLock::new();
    *GATE.get_or_init(|| {
        std::env::var("EPISTEMIC_GRAPH_ICV_NATIVE_WRITES")
            .map(|v| matches!(v.trim(), "1" | "true" | "yes" | "on"))
            .unwrap_or(false)
    })
}

/// Enforce a graph's registered integrity policy on a native (non-RDF) write —
/// a Cypher mutation or any other direct `GraphCore` change — when the
/// `EPISTEMIC_GRAPH_ICV_NATIVE_WRITES` gate is on. `before`/`after` are the
/// pre- and post-mutation graph images (the commit path's authoritative base
/// and its staged copy). A rejection carries the same witness-bearing
/// [`GuardRejection`] shape the RDF write path returns.
pub(crate) fn check_native_write(
    graph_name: &str,
    before: &GraphCore,
    after: &GraphCore,
) -> Result<(), GuardRejection> {
    check_native_write_gated(native_write_gate_enabled(), graph_name, before, after)
}

/// The gate-explicit core of [`check_native_write`], split out so tests can
/// exercise enforcement without mutating process-global env state (which
/// would race other tests in the same binary).
fn check_native_write_gated(
    gate_enabled: bool,
    graph_name: &str,
    before: &GraphCore,
    after: &GraphCore,
) -> Result<(), GuardRejection> {
    if !gate_enabled {
        return Ok(());
    }
    // Deliberately NOT fail-closed (unlike `check_before_write`): a graph that
    // never registered an integrity policy is simply not opted in.
    let Some(policy) = before.integrity_policy() else {
        return Ok(());
    };
    let export = |core: &GraphCore, label: &str| -> Result<Vec<Triple>, GuardRejection> {
        eg_rdf::mapping::export_triples(core, graph_name).map_err(|error| GuardRejection {
            graph: Some(graph_name.to_string()),
            message: format!(
                "EG-KG.ontology.rdf-update-guard: could not read the {label} graph image to evaluate ICV: {error}"
            ),
            details: serde_json::Value::Null,
        })
    };
    let before_set: HashSet<Triple> = export(before, "before")?.into_iter().collect();
    let after_set: HashSet<Triple> = export(after, "after")?.into_iter().collect();
    if before_set == after_set {
        // Fast no-op: no observable RDF-projected change, so no writer pays for
        // a shapes evaluation.
        return Ok(());
    }
    let additions: Vec<Triple> = after_set.difference(&before_set).cloned().collect();
    let removals: Vec<Triple> = before_set.difference(&after_set).cloned().collect();
    let mut base = Graph::new();
    for triple in &before_set {
        base.insert(triple);
    }
    let icv_policy = IcvPolicy::from_turtle(&policy.shapes_ttl).map_err(|_| GuardRejection {
        graph: Some(graph_name.to_string()),
        message: "EG-KG.ontology.rdf-update-guard: authoritative integrity policy is invalid"
            .to_string(),
        details: serde_json::json!({"reason": "integrity_policy_invalid"}),
    })?;
    IcvPolicyRegistry::new()
        .with(Some(graph_name), icv_policy)
        .check_graph(Some(graph_name), &base, &additions, &removals)
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

    // ── native-write ICV gate (W4.13) ────────────────────────────────────

    const RDF_TYPE: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";

    fn node_id(iri: &str) -> String {
        format!("<{iri}>")
    }

    /// A single property cell in the `eg_rdf::mapping` convention: an object with
    /// a `value` (and, unused here, optional `datatype`/`lang`) — NOT a bare JSON
    /// string, which `cell_to_literal` would silently skip on export.
    fn cell(value: &str) -> serde_json::Value {
        serde_json::json!({"value": value})
    }

    fn msgpack(props: serde_json::Value) -> Vec<u8> {
        rmp_serde::to_vec_named(&props).unwrap()
    }

    /// Add an `ex:Person` node with the given id and `ex:name`. Property-blob
    /// keys must be full predicate IRIs (bare, no angle brackets); `rdf:type`
    /// round-trips through `export_triples` only as an explicit edge, never the
    /// scalar `"type"` field, so the class node + typed edge are added too.
    fn add_person(core: &GraphCore, id: &str, name: &str) {
        core.add_node(
            node_id(id),
            msgpack(serde_json::json!({"http://example.org/name": cell(name)})),
        );
        core.add_node(node_id("http://example.org/Person"), Vec::new());
        core.add_edge(
            node_id(id),
            node_id("http://example.org/Person"),
            msgpack(serde_json::json!({"relationship": RDF_TYPE})),
        )
        .unwrap();
    }

    fn add_manager(core: &GraphCore, person_id: &str, manager_id: &str) {
        core.add_node(node_id(manager_id), Vec::new());
        core.add_edge(
            node_id(person_id),
            node_id(manager_id),
            msgpack(serde_json::json!({"relationship": "http://example.org/manager"})),
        )
        .unwrap();
    }

    /// A staged copy of `core` — a fresh `GraphCore` seeded from its current
    /// snapshot, mirroring `commit_mutation_inner`'s own
    /// `GraphCore::from_snapshot(base_snapshot, source_version)` staging step.
    fn staged_copy(core: &GraphCore) -> GraphCore {
        GraphCore::from_snapshot(core.snapshot(), 0).expect("snapshot round-trip")
    }

    #[test]
    fn native_write_rejects_a_maxcount_breaking_change_when_opted_in() {
        let before = GraphCore::new();
        configure(
            &before,
            "graph",
            None,
            "enforce",
            &format!("{PREFIXES}{SHAPES}"),
        )
        .unwrap();
        add_person(&before, "http://example.org/a", "Ay");
        add_manager(&before, "http://example.org/a", "http://example.org/m1");

        let after = staged_copy(&before);
        // A second manager breaks sh:maxCount 1 -- a pure native write, no Turtle.
        add_manager(&after, "http://example.org/a", "http://example.org/m2");

        let error = check_native_write_gated(true, "graph", &before, &after).unwrap_err();
        assert!(error.details.to_string().contains("witness"));
    }

    #[test]
    fn native_write_accepts_a_clean_native_change_when_opted_in() {
        let before = GraphCore::new();
        configure(
            &before,
            "graph",
            None,
            "enforce",
            &format!("{PREFIXES}{SHAPES}"),
        )
        .unwrap();
        add_person(&before, "http://example.org/a", "Ay");
        add_manager(&before, "http://example.org/a", "http://example.org/m1");

        let after = staged_copy(&before);
        // An unrelated node introduces no violation.
        add_person(&after, "http://example.org/b", "Bee");

        assert!(check_native_write_gated(true, "graph", &before, &after).is_ok());
    }

    #[test]
    fn native_write_unaffected_when_graph_never_opted_in() {
        // No `configure(...)` call: the graph never registered an integrity
        // policy, so it is not opted in -- even with the gate on, even with a
        // change that WOULD violate the shape above.
        let before = GraphCore::new();
        add_person(&before, "http://example.org/a", "Ay");
        add_manager(&before, "http://example.org/a", "http://example.org/m1");

        let after = staged_copy(&before);
        add_manager(&after, "http://example.org/a", "http://example.org/m2");

        assert!(check_native_write_gated(true, "graph", &before, &after).is_ok());
    }

    #[test]
    fn native_write_unaffected_when_gate_disabled() {
        // The public entry point reads the real (unset-by-default) env gate.
        // No test in this binary sets EPISTEMIC_GRAPH_ICV_NATIVE_WRITES, so this
        // proves the default-off posture even on an opted-in graph with a
        // violating change.
        let before = GraphCore::new();
        configure(
            &before,
            "graph",
            None,
            "enforce",
            &format!("{PREFIXES}{SHAPES}"),
        )
        .unwrap();
        add_person(&before, "http://example.org/a", "Ay");
        add_manager(&before, "http://example.org/a", "http://example.org/m1");

        let after = staged_copy(&before);
        add_manager(&after, "http://example.org/a", "http://example.org/m2");

        assert!(check_native_write("graph", &before, &after).is_ok());
    }

    #[test]
    fn native_write_no_observable_change_is_a_fast_no_op() {
        let before = GraphCore::new();
        configure(
            &before,
            "graph",
            None,
            "enforce",
            &format!("{PREFIXES}{SHAPES}"),
        )
        .unwrap();
        add_person(&before, "http://example.org/a", "Ay");
        add_manager(&before, "http://example.org/a", "http://example.org/m1");

        // A staged copy with no further mutation: identical RDF projection.
        let after = staged_copy(&before);

        assert!(check_native_write_gated(true, "graph", &before, &after).is_ok());
    }
}
