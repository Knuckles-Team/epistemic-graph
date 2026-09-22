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

use std::collections::{HashMap, HashSet};

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

/// Bind every committed-authority inference to the exact GraphSchema snapshot
/// that produced it.  The receipt alone is not enough: rows survive restart and
/// must remain independently auditable after the active schema changes.
pub fn bind_inference_schema_digests(
    core: &GraphCore,
    facts: &mut [HashMap<String, String>],
    schema_digests: &[String],
) {
    if schema_digests.is_empty() {
        return;
    }
    let encoded = serde_json::to_string(schema_digests).expect("schema digests serialize");
    let property_value = serde_json::Value::Array(
        schema_digests
            .iter()
            .cloned()
            .map(serde_json::Value::String)
            .collect(),
    );
    for fact in facts {
        fact.insert("schema_digests".to_string(), encoded.clone());
        if fact
            .get("materialized")
            .is_some_and(|value| value == "false")
        {
            continue;
        }
        let (Some(subject), Some(predicate), Some(object)) = (
            fact.get("subject"),
            fact.get("predicate"),
            fact.get("object"),
        ) else {
            continue;
        };
        if matches!(predicate.as_str(), "type" | "rdf:type") {
            update_node_properties(core, subject, |properties| {
                properties.insert(
                    "inference_schema_digests".to_string(),
                    property_value.clone(),
                );
            });
            continue;
        }
        let Some(mut rows) = core
            .edge_properties
            .get_mut(&(subject.clone(), object.clone()))
        else {
            continue;
        };
        for row in rows.iter_mut() {
            let Ok(mut value) = eg_types::msgpack::decode_property_value(row.as_slice()) else {
                continue;
            };
            let Some(properties) = value.as_object_mut() else {
                continue;
            };
            if properties.get("inferred") != Some(&serde_json::Value::Bool(true))
                || properties
                    .get("relationship")
                    .and_then(serde_json::Value::as_str)
                    != Some(predicate.as_str())
            {
                continue;
            }
            properties.insert(
                "inference_schema_digests".to_string(),
                property_value.clone(),
            );
            if let Ok(bytes) = rmp_serde::to_vec_named(&value) {
                *row = std::sync::Arc::new(bytes);
            }
        }
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
            obj.insert("inferred".to_string(), serde_json::Value::Bool(true));
            obj.insert(
                "inferred_from".to_string(),
                serde_json::Value::String("owl_reasoner".to_string()),
            );
            obj.insert(
                "inference_type".to_string(),
                serde_json::Value::String("rust_datalog".to_string()),
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
                "inferred": true,
                "inferred_from": "owl_reasoner",
                "inference_type": "rust_datalog"
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
            obj.insert("inferred".to_string(), serde_json::Value::Bool(true));
            obj.insert(
                "inferred_from".to_string(),
                serde_json::Value::String("owl_reasoner".to_string()),
            );
            obj.insert(
                "inference_type".to_string(),
                serde_json::Value::String("domain_range".to_string()),
            );
        });
    }

    inferred
}

/// Follow one arbitrary-length property chain from every matching first edge.
/// Each returned path includes the ordered premise triples used by the proof.
fn chain_paths(
    edges_by_type: &HashMap<String, Vec<(String, String)>>,
    chain: &[String],
) -> Vec<(String, String, Vec<(String, String, String)>)> {
    fn follow(
        edges_by_type: &HashMap<String, Vec<(String, String)>>,
        chain: &[String],
        offset: usize,
        start: &str,
        current: &str,
        premises: &mut Vec<(String, String, String)>,
        output: &mut Vec<(String, String, Vec<(String, String, String)>)>,
    ) {
        if offset == chain.len() {
            output.push((start.to_string(), current.to_string(), premises.clone()));
            return;
        }
        for (source, target) in edges_by_type
            .get(&chain[offset])
            .into_iter()
            .flatten()
            .filter(|(source, _)| source == current)
        {
            premises.push((source.clone(), chain[offset].clone(), target.clone()));
            follow(
                edges_by_type,
                chain,
                offset + 1,
                start,
                target,
                premises,
                output,
            );
            premises.pop();
        }
    }

    let mut output = Vec::new();
    let Some(first) = chain.first() else {
        return output;
    };
    for (source, target) in edges_by_type.get(first).into_iter().flatten() {
        let mut premises = vec![(source.clone(), first.clone(), target.clone())];
        follow(
            edges_by_type,
            chain,
            1,
            source,
            target,
            &mut premises,
            &mut output,
        );
    }
    output
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
    infer_property_chain_axioms(
        core,
        chains
            .into_iter()
            .map(|(first, second, sup)| (vec![first, second], sup))
            .collect(),
    )
}

/// Materialize arbitrary-length OWL property-chain axioms to a deterministic
/// fixpoint. The legacy wire method still projects its two-role tuples through
/// [`infer_property_chains`], while committed GraphSchema may carry longer
/// `owl:propertyChainAxiom` lists without silently truncating them.
pub fn infer_property_chain_axioms(
    core: &GraphCore,
    chains: Vec<(Vec<String>, String)>,
) -> Vec<HashMap<String, String>> {
    let (mut edges_by_type, mut known) = indexed_property_edges(core);
    let (mut inferred, new_edges) =
        infer_property_chain_fixpoint(&chains, &mut edges_by_type, &mut known);
    materialize_property_chain_edges(core, &mut inferred, &new_edges);
    inferred
}

type PropertyChainEdge = (String, String, String, Vec<(String, String, String)>, usize);

fn indexed_property_edges(
    core: &GraphCore,
) -> (
    HashMap<String, Vec<(String, String)>>,
    HashSet<(String, String, String)>,
) {
    let mut edges_by_type = HashMap::new();
    let mut known = HashSet::new();
    for (src, tgt, edge_type) in edge_relationship_facts(core) {
        known.insert((src.clone(), edge_type.clone(), tgt.clone()));
        edges_by_type
            .entry(edge_type)
            .or_insert_with(Vec::new)
            .push((src, tgt));
    }
    (edges_by_type, known)
}

fn infer_property_chain_fixpoint(
    chains: &[(Vec<String>, String)],
    edges_by_type: &mut HashMap<String, Vec<(String, String)>>,
    known: &mut HashSet<(String, String, String)>,
) -> (Vec<HashMap<String, String>>, Vec<PropertyChainEdge>) {
    let mut inferred = Vec::new();
    let mut new_edges = Vec::new();
    let mut rounds = 0usize;
    let mut changed = true;
    while changed && rounds < 100_000 && known.len() < 1_000_000 {
        rounds += 1;
        changed =
            infer_property_chain_round(chains, edges_by_type, known, &mut inferred, &mut new_edges);
    }
    (inferred, new_edges)
}

fn infer_property_chain_round(
    chains: &[(Vec<String>, String)],
    edges_by_type: &mut HashMap<String, Vec<(String, String)>>,
    known: &mut HashSet<(String, String, String)>,
    inferred: &mut Vec<HashMap<String, String>>,
    new_edges: &mut Vec<PropertyChainEdge>,
) -> bool {
    let mut changed = false;
    for (chain, inferred_prop) in chains {
        if chain.is_empty() {
            continue;
        }
        for (source, target, premises) in chain_paths(edges_by_type, chain) {
            if !known.insert((source.clone(), inferred_prop.clone(), target.clone())) {
                continue;
            }
            changed = true;
            record_property_chain_fact(
                &source,
                &target,
                inferred_prop,
                premises,
                edges_by_type,
                inferred,
                new_edges,
            );
        }
    }
    changed
}

fn record_property_chain_fact(
    source: &str,
    target: &str,
    inferred_prop: &str,
    premises: Vec<(String, String, String)>,
    edges_by_type: &mut HashMap<String, Vec<(String, String)>>,
    inferred: &mut Vec<HashMap<String, String>>,
    new_edges: &mut Vec<PropertyChainEdge>,
) {
    edges_by_type
        .entry(inferred_prop.to_string())
        .or_default()
        .push((source.to_string(), target.to_string()));
    let mut fact = inference_fact(source, inferred_prop, target, "property_chain");
    fact.insert("rule".to_string(), "RL-propertyChain".to_string());
    fact.insert(
        "premises".to_string(),
        serde_json::to_string(&premises).unwrap_or_else(|_| "[]".to_string()),
    );
    // Corrected to "false" below if the pair turns out to already be connected
    // (SAFE-MODE: connect-only materialization).
    fact.insert("materialized".to_string(), "true".to_string());
    new_edges.push((
        source.to_string(),
        target.to_string(),
        inferred_prop.to_string(),
        premises,
        inferred.len(),
    ));
    inferred.push(fact);
}

fn materialize_property_chain_edges(
    core: &GraphCore,
    inferred: &mut [HashMap<String, String>],
    new_edges: &[PropertyChainEdge],
) {
    // Apply inferred edges under one write txn. Existing pairs remain untouched, including
    // edges another chain materialized earlier in this batch.
    let mut txn = core.txn();
    for edge in new_edges {
        materialize_property_chain_edge(core, &mut txn, inferred, edge);
    }
}

fn materialize_property_chain_edge(
    core: &GraphCore,
    txn: &mut eg_core::graph::GraphTxn<'_>,
    inferred: &mut [HashMap<String, String>],
    (src, tgt, prop, premises, fact_index): &PropertyChainEdge,
) {
    if pair_already_connected(txn, core, src, tgt) {
        inferred[*fact_index].insert("materialized".to_string(), "false".to_string());
        return;
    }
    if let (Some(&src_idx), Some(&tgt_idx)) =
        (txn.topo.node_map.get(src), txn.topo.node_map.get(tgt))
    {
        txn.topo
            .graph
            .add_edge(src_idx, tgt_idx, format!("{}:{}", src, tgt));
    }
    store_property_chain_provenance(core, src, tgt, prop, premises);
}

fn store_property_chain_provenance(
    core: &GraphCore,
    src: &str,
    tgt: &str,
    prop: &str,
    premises: &[(String, String, String)],
) {
    let value = serde_json::json!({
        "relationship": prop,
        "inferred": true,
        "inferred_from": "owl_reasoner",
        "inference_type": "property_chain",
        "inference_rule": "RL-propertyChain",
        "inference_premises": premises,
    });
    if let Ok(props_msgpack) = rmp_serde::to_vec_named(&value) {
        core.edge_properties
            .entry((src.to_string(), tgt.to_string()))
            .or_default()
            .push(std::sync::Arc::new(props_msgpack));
    }
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

    #[test]
    fn arbitrary_length_chains_reach_fixpoint_with_proof_metadata() {
        let core = GraphCore::new();
        for node in ["a", "b", "c", "d", "e"] {
            core.add_node(node.into(), props(serde_json::json!({"type": "Thing"})));
        }
        for (source, target, relationship) in [
            ("a", "b", "first"),
            ("b", "c", "second"),
            ("c", "d", "third"),
            ("d", "e", "tail"),
        ] {
            core.add_edge(
                source.into(),
                target.into(),
                props(serde_json::json!({"relationship": relationship})),
            )
            .unwrap();
        }

        let inferred = infer_property_chain_axioms(
            &core,
            vec![
                (
                    vec!["first".into(), "second".into(), "third".into()],
                    "long".into(),
                ),
                (vec!["long".into(), "tail".into()], "finished".into()),
            ],
        );
        let finished = inferred
            .iter()
            .find(|fact| {
                fact.get("subject").map(String::as_str) == Some("a")
                    && fact.get("predicate").map(String::as_str) == Some("finished")
                    && fact.get("object").map(String::as_str) == Some("e")
            })
            .expect("a later chain consumes the first chain's derived fact");
        assert_eq!(
            finished.get("rule").map(String::as_str),
            Some("RL-propertyChain")
        );
        assert!(finished
            .get("premises")
            .is_some_and(|premises| premises.contains("long")));
        let edge = core.get_edge_properties("a", "e");
        let materialized = eg_types::msgpack::decode_property_value(&edge[0]).unwrap();
        assert_eq!(materialized["inferred_from"], "owl_reasoner");
        assert_eq!(materialized["inference_rule"], "RL-propertyChain");
    }

    #[test]
    fn committed_inferences_persist_the_exact_schema_identity() {
        let core = GraphCore::new();
        for node in ["a", "b", "c"] {
            core.add_node(node.into(), props(serde_json::json!({"type": "Thing"})));
        }
        for (source, target) in [("a", "b"), ("b", "c")] {
            core.add_edge(
                source.into(),
                target.into(),
                props(serde_json::json!({"relationship": "PART_OF"})),
            )
            .unwrap();
        }
        let mut inferred = run_datalog_reasoning(
            &core,
            Vec::new(),
            Vec::new(),
            Vec::new(),
            vec!["PART_OF".into()],
            Vec::new(),
        )
        .unwrap();
        bind_inference_schema_digests(&core, &mut inferred, &["digest-a".into()]);
        let fact = inferred
            .iter()
            .find(|fact| {
                fact.get("subject").map(String::as_str) == Some("a")
                    && fact.get("object").map(String::as_str) == Some("c")
            })
            .unwrap();
        assert_eq!(
            fact.get("schema_digests").map(String::as_str),
            Some("[\"digest-a\"]")
        );
        let row = core.get_edge_properties("a", "c");
        let value = eg_types::msgpack::decode_property_value(&row[0]).unwrap();
        assert_eq!(value["inference_schema_digests"][0], "digest-a");
    }
}
