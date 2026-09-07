// CONCEPT:EG-KG.compute.compiled-semantic-reasoner - Compiled Semantic Reasoner
//
// Datalog forward-chaining reasoning engine with support for:
// - Subclass inheritance (rdfs:subClassOf)
// - Subproperty inheritance (rdfs:subPropertyOf)
// - Symmetric properties (owl:SymmetricProperty)
// - Transitive properties (owl:TransitiveProperty)
// - Inverse properties (owl:inverseOf)
//
// All reasoning operates on GraphCore and produces inferred triples.
//
// The five-rule fixpoint is evaluated by `reasoning_closure::infer_semi_naive`
// (CONCEPT:EG-KG.compute.reasoning-closure-gpu): the facts are interned to integer
// relations and derived SEMI-NAIVELY (each round works the per-round delta, not a full
// re-scan), and Rule 5 (transitive closure) — the one sparse-matrix-shaped rule — runs
// through a `ClosureBackend` seam with an always-compiled CPU hash-join and a
// feature-gated (`gpu-cuda`) CUDA kernel, mirroring `eg-ann::kmeans_gpu`. This module owns
// fact extraction from `GraphCore` and the write-back of derived facts; the inference is
// delegated. (Supersedes the earlier string-keyed naive fixpoint that lived here.)

use std::collections::HashMap;

use crate::graph::GraphCore;
use crate::reasoning_closure::{active_closure_backend, infer_semi_naive};

/// Extract the base type/property facts from `core` as flat `(node, type)` and
/// `(src, tgt, prop)` lists (the input to the semi-naive evaluator).
/// Every stored edge as `(src, tgt, relationship)`. The native graph keeps a `Vec` of
/// msgpack property blobs per ordered pair, so an edge is only a fact once its blob
/// decodes and names a `relationship`; a blob that does not is skipped, never guessed.
fn edge_relationship_facts(core: &GraphCore) -> Vec<(String, String, String)> {
    let mut facts = Vec::new();
    for entry in core.edge_properties.iter() {
        let ((src, tgt), props_msgpack_list) = (entry.key(), entry.value());
        for props_msgpack in props_msgpack_list {
            if let Ok(val) = eg_types::msgpack::decode_property_value(props_msgpack) {
                if let Some(t) = val.get("relationship").and_then(|v| v.as_str()) {
                    facts.push((src.clone(), tgt.clone(), t.to_string()));
                }
            }
        }
    }
    facts
}

/// Every stored node as `(node, type)`, on the same decode-or-skip contract.
fn node_type_facts(core: &GraphCore) -> Vec<(String, String)> {
    let mut facts = Vec::new();
    for entry in core.node_properties.iter() {
        let (node_id, props_msgpack) = (entry.key(), entry.value());
        if let Ok(val) = eg_types::msgpack::decode_property_value(props_msgpack) {
            if let Some(t) = val.get("type").and_then(|v| v.as_str()) {
                facts.push((node_id.clone(), t.to_string()));
            }
        }
    }
    facts
}

/// Group `(property, value)` rules by property, preserving declaration order.
fn group_by_property(rules: Vec<(String, String)>) -> HashMap<String, Vec<String>> {
    let mut grouped: HashMap<String, Vec<String>> = HashMap::new();
    for (property, value) in rules {
        grouped.entry(property).or_default().push(value);
    }
    grouped
}

/// One inference fact in the flat string-map shape every reasoning entrypoint returns.
fn inference_fact(
    subject: &str,
    predicate: &str,
    object: &str,
    inference_type: &str,
) -> HashMap<String, String> {
    HashMap::from([
        ("subject".to_string(), subject.to_string()),
        ("predicate".to_string(), predicate.to_string()),
        ("object".to_string(), object.to_string()),
        ("inference_type".to_string(), inference_type.to_string()),
    ])
}

/// Read/modify/write one node's decoded property object in place.
///
/// A node whose blob is absent, undecodable, not an object, or not re-encodable is left
/// untouched: inference never destroys a property blob it cannot round-trip.
fn update_node_properties(
    core: &GraphCore,
    node_id: &str,
    edit: impl FnOnce(&mut serde_json::Map<String, serde_json::Value>),
) {
    let Some(mut props_msgpack) = core.node_properties.get_mut(node_id) else {
        return;
    };
    let Ok(mut val) = eg_types::msgpack::decode_property_value(props_msgpack.as_slice()) else {
        return;
    };
    let Some(obj) = val.as_object_mut() else {
        return;
    };
    edit(obj);
    if let Ok(updated) = rmp_serde::to_vec_named(&val) {
        *props_msgpack = std::sync::Arc::new(updated);
    }
}

#[allow(clippy::type_complexity)]
fn extract_facts(core: &GraphCore) -> (Vec<(String, String)>, Vec<(String, String, String)>) {
    (node_type_facts(core), edge_relationship_facts(core))
}

/// SAFE-MODE invariant (CONCEPT:EG-KG.compute.reasoning-connect-only): materialisation of a
/// derived edge fact may only CONNECT a pair of nodes that currently has **no** edge between
/// them. It must never modify an edge between a pair that already has one — not its
/// relationship type, not its properties, not an `inferred` stamp.
///
/// This is the fix for a measured defect: the native graph stores edge properties as a `Vec`
/// per ordered `(source, target)` pair (`GraphCore::edge_properties`), and downstream readers
/// that expect a single "current" relationship per pair (temporal/latest-wins reads, exports,
/// simple pattern matches) observe whichever entry was written LAST. Before this guard,
/// reasoning would unconditionally `push` a new properties entry for a pair the moment the
/// closure derived a fact over it — e.g. an ontology's `PART_OF ⊑ DEPENDS_ON` subsumption
/// turned an asserted `a -PART_OF-> b` edge into what reads back as `a -DEPENDS_ON-> b`
/// (`inferred: true`), even though the topology graph never gained a second physical edge (its
/// own `find_edge` guard already skipped that). That silent relabeling of an ASSERTED fact by a
/// process the user turned on for its READ-ONLY inference value is exactly why the calling
/// wiring was shipped disabled.
///
/// A pair counts as "already connected" if EITHER the topology graph has an edge between the
/// two node indices OR `edge_properties` already holds a (possibly out-of-band) entry for the
/// pair — checking both sides defends against the very mismatch this bug can itself produce.
///
/// This is not a flag. The one-edge-per-ordered-pair *read* model (whatever a caller treats as
/// "the" relationship for a pair) is a hard invariant elsewhere in this codebase already (see
/// `GraphTxn::remove_edge`'s "pair removal replaces all" note, and `BatchOperation::AddEdge`'s
/// `upsert` semantics, which explicitly `remove_edge` before `add_edge` to replace a pair's
/// state); a reasoning pass has no basis to treat that invariant as optional just because its
/// own write is "only" an inference. Connect-only is therefore the only behaviour this function
/// has — there is no opt-out, in keeping with this codebase's native-by-default convention: a
/// safety property is not something a caller can forget to ask for.
fn pair_already_connected(
    txn: &crate::graph::GraphTxn<'_>,
    core: &GraphCore,
    src: &str,
    tgt: &str,
) -> bool {
    let topology_connected = match (txn.topo.node_map.get(src), txn.topo.node_map.get(tgt)) {
        (Some(&src_idx), Some(&tgt_idx)) => txn.topo.graph.find_edge(src_idx, tgt_idx).is_some(),
        _ => false,
    };
    topology_connected
        || core
            .edge_properties
            .get(&(src.to_string(), tgt.to_string()))
            .is_some_and(|props| !props.is_empty())
}

/// Run forward-chaining Datalog reasoning until fixpoint.
///
/// Returns a list of inferred triples as `HashMap<String, String>` with keys:
/// - `subject`, `predicate`, `object`, `inference_type`, and `materialized` (`"true"`/`"false"`)
///
/// Also mutates the graph in-place by adding inferred edges (SAFE-MODE: only between pairs with
/// no existing edge — see [`pair_already_connected`]) and type annotations. A derived edge fact
/// over a pair that already has an edge is still reported in the returned triples (it IS a true
/// logical consequence of the base facts and the ontology, and the caller may want to know that
/// — e.g. for audit/provenance), but with `materialized: "false"`: the graph itself is left
/// untouched for that pair.
pub fn run_datalog_reasoning(
    core: &GraphCore,
    subclass_relations: Vec<(String, String)>,
    subproperty_relations: Vec<(String, String)>,
    symmetric_properties: Vec<String>,
    transitive_properties: Vec<String>,
    inverse_properties: Vec<(String, String)>,
) -> Result<Vec<HashMap<String, String>>, String> {
    let mut inferred_triples = Vec::new();

    // 1. Extract base facts, then 2. derive the closure semi-naively (Rule 5 via the
    //    active CPU/CUDA `ClosureBackend`). `new_types_to_add`/`new_edges_to_add` are the
    //    DERIVED facts (accumulated minus base) — identical to the prior naive fixpoint.
    let (base_node_types, base_edge_types) = extract_facts(core);
    let (new_types_to_add, new_edges_to_add) = infer_semi_naive(
        &base_node_types,
        &base_edge_types,
        subclass_relations,
        subproperty_relations,
        symmetric_properties,
        transitive_properties,
        inverse_properties,
        active_closure_backend(),
    );

    // Apply all inferred facts back to internal structures
    for (node_id, new_type) in &new_types_to_add {
        inferred_triples.push(inference_fact(node_id, "type", new_type, "rust_datalog"));
        update_node_properties(core, node_id, |obj| {
            obj.insert(
                "inferred_type".to_string(),
                serde_json::Value::String(new_type.clone()),
            );
        });
    }

    // Topology edits run under one write txn (the inferred-edge additions are
    // atomic w.r.t. concurrent readers); the parallel edge_properties push goes
    // through the same DashMap (interior-mutable, ordered by the held topo guard).
    {
        let mut txn = core.txn();
        for (src, tgt, new_prop) in &new_edges_to_add {
            let mut fact = HashMap::new();
            fact.insert("subject".to_string(), src.clone());
            fact.insert("predicate".to_string(), new_prop.clone());
            fact.insert("object".to_string(), tgt.clone());
            fact.insert("inference_type".to_string(), "rust_datalog".to_string());

            // SAFE-MODE (see `pair_already_connected`): a pair that already has an edge is
            // reported as a logical consequence but left untouched -- never relabeled.
            if pair_already_connected(&txn, core, src, tgt) {
                fact.insert("materialized".to_string(), "false".to_string());
                inferred_triples.push(fact);
                continue;
            }
            fact.insert("materialized".to_string(), "true".to_string());
            inferred_triples.push(fact);

            // Safe access — only add edge if both nodes exist
            if let (Some(&src_idx), Some(&tgt_idx)) =
                (txn.topo.node_map.get(src), txn.topo.node_map.get(tgt))
            {
                txn.topo
                    .graph
                    .add_edge(src_idx, tgt_idx, format!("{}:{}", src, tgt));
            }

            let val = serde_json::json!({
                "relationship": new_prop.clone(),
                "inferred": true
            });
            if let Ok(props_msgpack) = rmp_serde::to_vec_named(&val) {
                core.edge_properties
                    .entry((src.clone(), tgt.clone()))
                    .or_default()
                    .push(std::sync::Arc::new(props_msgpack));
            }
        }
    }

    Ok(inferred_triples)
}

/// Domain/Range inference.
///
/// If property P has domain D, then for every edge (s, P, o), infer s rdf:type D.
/// If property P has range R, then for every edge (s, P, o), infer o rdf:type R.
///
/// Returns inferred type triples.
pub fn infer_domain_range(
    core: &GraphCore,
    domain_rules: Vec<(String, String)>, // (property, domain_type)
    range_rules: Vec<(String, String)>,  // (property, range_type)
) -> Vec<HashMap<String, String>> {
    let domain_map = group_by_property(domain_rules);
    let range_map = group_by_property(range_rules);

    let mut inferred = Vec::new();
    let mut new_types: Vec<(String, String)> = Vec::new();
    for (src, tgt, edge_type) in edge_relationship_facts(core) {
        // Domain inference gives the source the property's domain types; range
        // inference gives the target its range types.
        for (node, types, inference_type) in [
            (&src, domain_map.get(&edge_type), "domain_inference"),
            (&tgt, range_map.get(&edge_type), "range_inference"),
        ] {
            for inferred_type in types.into_iter().flatten() {
                new_types.push((node.clone(), inferred_type.clone()));
                inferred.push(inference_fact(
                    node,
                    "rdf:type",
                    inferred_type,
                    inference_type,
                ));
            }
        }
    }

    for (node_id, new_type) in &new_types {
        update_node_properties(core, node_id, |obj| {
            let arr = obj
                .entry("inferred_types".to_string())
                .or_insert_with(|| serde_json::Value::Array(vec![]));
            if let serde_json::Value::Array(ref mut a) = arr {
                let type_val = serde_json::Value::String(new_type.clone());
                if !a.contains(&type_val) {
                    a.push(type_val);
                }
            }
        });
    }

    inferred
}

/// Whether the ordered pair `(src, tgt)` already carries an edge named `relationship`.
fn edge_has_relationship(core: &GraphCore, src: &str, tgt: &str, relationship: &str) -> bool {
    core.edge_properties
        .get(&(src.to_string(), tgt.to_string()))
        .is_some_and(|props| {
            props.iter().any(|blob| {
                eg_types::msgpack::decode_property_value(blob)
                    .ok()
                    .and_then(|v| {
                        v.get("relationship")
                            .and_then(|t| t.as_str())
                            .map(str::to_string)
                    })
                    .as_deref()
                    == Some(relationship)
            })
        })
}

/// The `(a, c)` pairs the chain `prop1 ∘ prop2` entails and `inferred_prop` does not
/// already connect. A pair reachable through several middles is returned once per
/// middle, exactly as the chain rule fires.
fn chain_pairs(
    core: &GraphCore,
    edges_by_type: &HashMap<String, Vec<(String, String)>>,
    prop1: &str,
    prop2: &str,
    inferred_prop: &str,
) -> Vec<(String, String)> {
    // For prop2, map source -> targets, so each (a, prop1, b) can look up every
    // (b, prop2, c) in one step.
    let mut prop2_from: HashMap<&str, Vec<&str>> = HashMap::new();
    for (src, tgt) in edges_by_type.get(prop2).into_iter().flatten() {
        prop2_from
            .entry(src.as_str())
            .or_default()
            .push(tgt.as_str());
    }
    let mut pairs = Vec::new();
    for (a, b) in edges_by_type.get(prop1).into_iter().flatten() {
        for c in prop2_from.get(b.as_str()).into_iter().flatten() {
            if !edge_has_relationship(core, a, c, inferred_prop) {
                pairs.push((a.clone(), (*c).to_string()));
            }
        }
    }
    pairs
}

/// Property chain inference.
///
/// Given chains like [(hasPart, isPartOf) -> composedOf], infer new edges
/// when the chain pattern is found in the graph.
///
/// chain: (prop1, prop2, inferred_prop) — if (a, prop1, b) and (b, prop2, c), then (a, inferred_prop, c)
pub fn infer_property_chains(
    core: &GraphCore,
    chains: Vec<(String, String, String)>,
) -> Vec<HashMap<String, String>> {
    let mut inferred = Vec::new();
    // (src, tgt, inferred_prop, fact_index) — `fact_index` into `inferred` lets the
    // materialization loop below correct that fact's `materialized` flag once it knows
    // whether the pair was already connected (SAFE-MODE, see `pair_already_connected`).
    let mut new_edges: Vec<(String, String, String, usize)> = Vec::new();

    // Index edges by canonical relationship for fast lookup.
    let mut edges_by_type: HashMap<String, Vec<(String, String)>> = HashMap::new();
    for (src, tgt, edge_type) in edge_relationship_facts(core) {
        edges_by_type.entry(edge_type).or_default().push((src, tgt));
    }

    for (prop1, prop2, inferred_prop) in &chains {
        for (a, c) in chain_pairs(core, &edges_by_type, prop1, prop2, inferred_prop) {
            let mut fact = inference_fact(&a, inferred_prop, &c, "property_chain");
            // Corrected to "false" below if the pair turns out to already be connected
            // (SAFE-MODE: connect-only materialization).
            fact.insert("materialized".to_string(), "true".to_string());
            new_edges.push((a, c, inferred_prop.clone(), inferred.len()));
            inferred.push(fact);
        }
    }

    // Apply inferred edges to graph under one write txn (atomic topology edits;
    // edge_properties push via the interior-mutable DashMap). SAFE-MODE: a pair that
    // already has an edge (topology OR edge_properties) is left completely untouched —
    // never relabeled, never given a second properties entry — including a pair that a
    // PRIOR chain in this same call just connected (`pair_already_connected` re-checks
    // live state each iteration, so it also blocks a later chain in this batch from
    // stacking a second edge over a pair one earlier in the batch just created).
    let mut txn = core.txn();
    for (src, tgt, prop, fact_index) in &new_edges {
        if pair_already_connected(&txn, core, src, tgt) {
            inferred[*fact_index].insert("materialized".to_string(), "false".to_string());
            continue;
        }
        if let (Some(&src_idx), Some(&tgt_idx)) =
            (txn.topo.node_map.get(src), txn.topo.node_map.get(tgt))
        {
            txn.topo
                .graph
                .add_edge(src_idx, tgt_idx, format!("{}:{}", src, tgt));
        }
        let val = serde_json::json!({
            "relationship": prop.clone(),
            "inferred": true,
            "chain": true
        });
        if let Ok(props_msgpack) = rmp_serde::to_vec_named(&val) {
            core.edge_properties
                .entry((src.clone(), tgt.clone()))
                .or_default()
                .push(std::sync::Arc::new(props_msgpack));
        }
    }

    inferred
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph::GraphCore;

    fn props(json: serde_json::Value) -> Vec<u8> {
        rmp_serde::to_vec_named(&json).unwrap()
    }

    #[test]
    fn transitive_closure_infers_indirect_edge() {
        let core = GraphCore::new();
        core.add_node("a".into(), props(serde_json::json!({"type": "Person"})));
        core.add_node("b".into(), props(serde_json::json!({"type": "Person"})));
        core.add_node("c".into(), props(serde_json::json!({"type": "Person"})));
        core.add_edge(
            "a".into(),
            "b".into(),
            props(serde_json::json!({"relationship": "ancestor"})),
        )
        .unwrap();
        core.add_edge(
            "b".into(),
            "c".into(),
            props(serde_json::json!({"relationship": "ancestor"})),
        )
        .unwrap();

        let inferred = run_datalog_reasoning(
            &core,
            vec![],
            vec![],
            vec![],
            vec!["ancestor".into()],
            vec![],
        )
        .unwrap();

        assert!(!inferred.is_empty());
        assert!(core.has_edge("a", "c"));
    }

    #[test]
    fn subclass_inheritance_infers_supertype() {
        let core = GraphCore::new();
        core.add_node("rex".into(), props(serde_json::json!({"type": "Dog"})));

        let inferred = run_datalog_reasoning(
            &core,
            vec![("Dog".into(), "Animal".into())],
            vec![],
            vec![],
            vec![],
            vec![],
        )
        .unwrap();

        assert!(inferred.iter().any(|t| {
            t.get("subject").map(String::as_str) == Some("rex")
                && t.get("object").map(String::as_str) == Some("Animal")
        }));
    }

    /// SAFE-MODE regression test (CONCEPT:EG-KG.compute.reasoning-connect-only): this is the
    /// test that would have caught the relabel. `a -PART_OF-> b` is ASSERTED (a real, non-
    /// inferred edge). The ontology says `PART_OF` is a sub-property of `DEPENDS_ON`, so a
    /// reasoning pass over this graph legitimately entails `a -DEPENDS_ON-> b` — and, before
    /// the SAFE-MODE guard, materialisation pushed that entailed fact straight into
    /// `edge_properties` for the SAME pair, so a "current relationship" read of `(a, b)` (last
    /// entry wins) came back `DEPENDS_ON`/`inferred: true` instead of the asserted `PART_OF`.
    /// Assert the asserted edge is BYTE-FOR-BYTE unchanged after reasoning: still exactly one
    /// properties entry, still `PART_OF`, still no `inferred` stamp — and that the topology
    /// still has exactly the one edge it started with (no phantom second parallel edge either).
    #[test]
    fn subproperty_inference_never_relabels_an_asserted_edge() {
        let core = GraphCore::new();
        core.add_node("a".into(), props(serde_json::json!({"type": "Thing"})));
        core.add_node("b".into(), props(serde_json::json!({"type": "Thing"})));
        core.add_edge(
            "a".into(),
            "b".into(),
            props(serde_json::json!({"relationship": "PART_OF"})),
        )
        .unwrap();

        let before = core.get_edge_properties("a", "b");
        assert_eq!(
            before.len(),
            1,
            "exactly one asserted edge before reasoning"
        );

        let inferred = run_datalog_reasoning(
            &core,
            vec![],
            vec![("PART_OF".into(), "DEPENDS_ON".into())],
            vec![],
            vec![],
            vec![],
        )
        .unwrap();

        // The subsumption WAS a true logical consequence -- it must still be reported...
        let depends_on_fact = inferred
            .iter()
            .find(|t| {
                t.get("subject").map(String::as_str) == Some("a")
                    && t.get("predicate").map(String::as_str) == Some("DEPENDS_ON")
                    && t.get("object").map(String::as_str) == Some("b")
            })
            .expect("DEPENDS_ON over (a, b) is a true entailment and must be reported");
        // ...but explicitly marked as NOT materialized, because the pair already had an edge.
        assert_eq!(
            depends_on_fact.get("materialized").map(String::as_str),
            Some("false"),
            "an inferred fact over an already-connected pair must be reported unmaterialized"
        );

        // The graph itself: untouched. Still exactly one properties entry for (a, b), and it is
        // still the original asserted PART_OF -- not relabeled, not restamped `inferred: true`.
        let after = core.get_edge_properties("a", "b");
        assert_eq!(
            after.len(),
            1,
            "reasoning must not add a second properties entry over an already-connected pair"
        );
        let decoded =
            eg_types::msgpack::decode_property_value(&after[0]).expect("decode surviving edge");
        assert_eq!(
            decoded.get("relationship").and_then(|v| v.as_str()),
            Some("PART_OF"),
            "the asserted relationship type must survive the reasoning pass unchanged"
        );
        assert!(
            decoded.get("inferred").is_none(),
            "an asserted edge must never gain an `inferred` stamp from a reasoning pass"
        );

        // Topology: still exactly the one edge that was asserted -- no phantom parallel edge.
        assert!(core.has_edge("a", "b"));
    }

    /// Same SAFE-MODE guarantee, exercised through `infer_property_chains`: a chain rule that
    /// would derive a NEW relationship type over a pair that already has a DIFFERENT asserted
    /// edge must not touch that pair either.
    #[test]
    fn property_chain_inference_never_relabels_an_asserted_edge() {
        let core = GraphCore::new();
        core.add_node("a".into(), props(serde_json::json!({"type": "Thing"})));
        core.add_node("b".into(), props(serde_json::json!({"type": "Thing"})));
        core.add_node("c".into(), props(serde_json::json!({"type": "Thing"})));
        core.add_edge(
            "a".into(),
            "b".into(),
            props(serde_json::json!({"relationship": "hasPart"})),
        )
        .unwrap();
        core.add_edge(
            "b".into(),
            "c".into(),
            props(serde_json::json!({"relationship": "isPartOf"})),
        )
        .unwrap();
        // (a, c) is ALREADY asserted with an unrelated relationship type.
        core.add_edge(
            "a".into(),
            "c".into(),
            props(serde_json::json!({"relationship": "unrelated"})),
        )
        .unwrap();

        let inferred = infer_property_chains(
            &core,
            vec![("hasPart".into(), "isPartOf".into(), "composedOf".into())],
        );

        let fact = inferred
            .iter()
            .find(|t| {
                t.get("subject").map(String::as_str) == Some("a")
                    && t.get("predicate").map(String::as_str) == Some("composedOf")
                    && t.get("object").map(String::as_str) == Some("c")
            })
            .expect("composedOf over (a, c) is a true chain entailment and must be reported");
        assert_eq!(
            fact.get("materialized").map(String::as_str),
            Some("false"),
            "an inferred chain fact over an already-connected pair must be reported unmaterialized"
        );

        let after = core.get_edge_properties("a", "c");
        assert_eq!(
            after.len(),
            1,
            "property-chain inference must not add a second properties entry over an \
             already-connected pair"
        );
        let decoded =
            eg_types::msgpack::decode_property_value(&after[0]).expect("decode surviving edge");
        assert_eq!(
            decoded.get("relationship").and_then(|v| v.as_str()),
            Some("unrelated"),
            "the asserted relationship type must survive the chain-inference pass unchanged"
        );
    }
}
