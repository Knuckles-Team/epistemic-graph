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
#[allow(clippy::type_complexity)]
fn extract_facts(core: &GraphCore) -> (Vec<(String, String)>, Vec<(String, String, String)>) {
    let mut node_types = Vec::new();
    for entry in core.node_properties.iter() {
        let (node_id, props_msgpack) = (entry.key(), entry.value());
        if let Ok(val) = eg_types::msgpack::decode_property_value(props_msgpack) {
            if let Some(t) = val.get("type").and_then(|v| v.as_str()) {
                node_types.push((node_id.clone(), t.to_string()));
            }
        }
    }

    let mut edge_types = Vec::new();
    for entry in core.edge_properties.iter() {
        let ((src, tgt), props_msgpack_list) = (entry.key(), entry.value());
        for props_msgpack in props_msgpack_list {
            if let Ok(val) = eg_types::msgpack::decode_property_value(props_msgpack) {
                if let Some(t) = val.get("relationship").and_then(|v| v.as_str()) {
                    edge_types.push((src.clone(), tgt.clone(), t.to_string()));
                }
            }
        }
    }
    (node_types, edge_types)
}

/// Run forward-chaining Datalog reasoning until fixpoint.
///
/// Returns a list of inferred triples as `HashMap<String, String>` with keys:
/// - `subject`, `predicate`, `object`, `inference_type`
///
/// Also mutates the graph in-place by adding inferred edges and type annotations.
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
        let mut fact = HashMap::new();
        fact.insert("subject".to_string(), node_id.clone());
        fact.insert("predicate".to_string(), "type".to_string());
        fact.insert("object".to_string(), new_type.clone());
        fact.insert("inference_type".to_string(), "rust_datalog".to_string());
        inferred_triples.push(fact);

        if let Some(mut props_msgpack) = core.node_properties.get_mut(node_id) {
            if let Ok(mut val) = eg_types::msgpack::decode_property_value(props_msgpack.as_slice())
            {
                if let Some(obj) = val.as_object_mut() {
                    obj.insert(
                        "inferred_type".to_string(),
                        serde_json::Value::String(new_type.clone()),
                    );
                    if let Ok(updated) = rmp_serde::to_vec_named(&val) {
                        *props_msgpack = std::sync::Arc::new(updated);
                    }
                }
            }
        }
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
            inferred_triples.push(fact);

            // Safe access — only add edge if both nodes exist
            if let (Some(&src_idx), Some(&tgt_idx)) =
                (txn.topo.node_map.get(src), txn.topo.node_map.get(tgt))
            {
                if txn.topo.graph.find_edge(src_idx, tgt_idx).is_none() {
                    txn.topo
                        .graph
                        .add_edge(src_idx, tgt_idx, format!("{}:{}", src, tgt));
                }
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
    let mut inferred = Vec::new();
    let mut new_types: Vec<(String, String)> = Vec::new();

    // Build lookup
    let domain_map: HashMap<String, Vec<String>> =
        domain_rules
            .into_iter()
            .fold(HashMap::new(), |mut acc, (prop, domain)| {
                acc.entry(prop).or_default().push(domain);
                acc
            });

    let range_map: HashMap<String, Vec<String>> =
        range_rules
            .into_iter()
            .fold(HashMap::new(), |mut acc, (prop, range)| {
                acc.entry(prop).or_default().push(range);
                acc
            });

    // Scan all edges
    for entry in core.edge_properties.iter() {
        let ((src, tgt), props_msgpack_list) = (entry.key(), entry.value());
        for props_msgpack in props_msgpack_list {
            if let Ok(val) = eg_types::msgpack::decode_property_value(props_msgpack) {
                if let Some(edge_type) = val.get("relationship").and_then(|v| v.as_str()) {
                    // Domain inference: src gets the domain type
                    if let Some(domains) = domain_map.get(edge_type) {
                        for domain in domains {
                            new_types.push((src.clone(), domain.clone()));
                            let mut fact = HashMap::new();
                            fact.insert("subject".to_string(), src.clone());
                            fact.insert("predicate".to_string(), "rdf:type".to_string());
                            fact.insert("object".to_string(), domain.clone());
                            fact.insert(
                                "inference_type".to_string(),
                                "domain_inference".to_string(),
                            );
                            inferred.push(fact);
                        }
                    }

                    // Range inference: tgt gets the range type
                    if let Some(ranges) = range_map.get(edge_type) {
                        for range in ranges {
                            new_types.push((tgt.clone(), range.clone()));
                            let mut fact = HashMap::new();
                            fact.insert("subject".to_string(), tgt.clone());
                            fact.insert("predicate".to_string(), "rdf:type".to_string());
                            fact.insert("object".to_string(), range.clone());
                            fact.insert(
                                "inference_type".to_string(),
                                "range_inference".to_string(),
                            );
                            inferred.push(fact);
                        }
                    }
                }
            }
        }
    }

    // Apply inferred types to graph
    for (node_id, new_type) in &new_types {
        if let Some(mut props_msgpack) = core.node_properties.get_mut(node_id) {
            if let Ok(mut val) = eg_types::msgpack::decode_property_value(props_msgpack.as_slice())
            {
                if let Some(obj) = val.as_object_mut() {
                    // Append to inferred_types array
                    let arr = obj
                        .entry("inferred_types".to_string())
                        .or_insert_with(|| serde_json::Value::Array(vec![]));
                    if let serde_json::Value::Array(ref mut a) = arr {
                        let type_val = serde_json::Value::String(new_type.clone());
                        if !a.contains(&type_val) {
                            a.push(type_val);
                        }
                    }
                    if let Ok(updated) = rmp_serde::to_vec_named(&val) {
                        *props_msgpack = std::sync::Arc::new(updated);
                    }
                }
            }
        }
    }

    inferred
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
    let mut new_edges: Vec<(String, String, String)> = Vec::new();

    // Index edges by canonical relationship for fast lookup.
    let mut edges_by_type: HashMap<String, Vec<(String, String)>> = HashMap::new();
    for entry in core.edge_properties.iter() {
        let ((src, tgt), props_msgpack_list) = (entry.key(), entry.value());
        for props_msgpack in props_msgpack_list {
            if let Ok(val) = eg_types::msgpack::decode_property_value(props_msgpack) {
                if let Some(edge_type) = val.get("relationship").and_then(|v| v.as_str()) {
                    edges_by_type
                        .entry(edge_type.to_string())
                        .or_default()
                        .push((src.clone(), tgt.clone()));
                }
            }
        }
    }

    for (prop1, prop2, inferred_prop) in &chains {
        let edges1 = edges_by_type.get(prop1).cloned().unwrap_or_default();
        let edges2 = edges_by_type.get(prop2).cloned().unwrap_or_default();

        // Build index: for prop2, map source -> targets
        let mut prop2_from: HashMap<String, Vec<String>> = HashMap::new();
        for (src, tgt) in &edges2 {
            prop2_from.entry(src.clone()).or_default().push(tgt.clone());
        }

        // For each (a, prop1, b), check if (b, prop2, c) exists
        for (a, b) in &edges1 {
            if let Some(targets) = prop2_from.get(b) {
                for c in targets {
                    // Check if (a, inferred_prop, c) already exists
                    let exists = core
                        .edge_properties
                        .get(&(a.clone(), c.clone()))
                        .map(|props| {
                            props.iter().any(|p| {
                                eg_types::msgpack::decode_property_value(p)
                                    .ok()
                                    .and_then(|v| {
                                        v.get("relationship")
                                            .and_then(|t| t.as_str())
                                            .map(|s| s.to_string())
                                    })
                                    == Some(inferred_prop.clone())
                            })
                        })
                        .unwrap_or(false);

                    if !exists {
                        new_edges.push((a.clone(), c.clone(), inferred_prop.clone()));
                        let mut fact = HashMap::new();
                        fact.insert("subject".to_string(), a.clone());
                        fact.insert("predicate".to_string(), inferred_prop.clone());
                        fact.insert("object".to_string(), c.clone());
                        fact.insert("inference_type".to_string(), "property_chain".to_string());
                        inferred.push(fact);
                    }
                }
            }
        }
    }

    // Apply inferred edges to graph under one write txn (atomic topology edits;
    // edge_properties push via the interior-mutable DashMap).
    let mut txn = core.txn();
    for (src, tgt, prop) in &new_edges {
        if let (Some(&src_idx), Some(&tgt_idx)) =
            (txn.topo.node_map.get(src), txn.topo.node_map.get(tgt))
        {
            if txn.topo.graph.find_edge(src_idx, tgt_idx).is_none() {
                txn.topo
                    .graph
                    .add_edge(src_idx, tgt_idx, format!("{}:{}", src, tgt));
            }
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
}
