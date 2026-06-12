// CONCEPT:KG-2.17 - Compiled Semantic Reasoner
//
// Datalog forward-chaining reasoning engine with support for:
// - Subclass inheritance (rdfs:subClassOf)
// - Subproperty inheritance (rdfs:subPropertyOf)
// - Symmetric properties (owl:SymmetricProperty)
// - Transitive properties (owl:TransitiveProperty)
// - Inverse properties (owl:inverseOf)
//
// All reasoning operates on GraphCore and produces inferred triples.

use std::collections::{HashMap, HashSet};

use crate::graph::GraphCore;

/// Run forward-chaining Datalog reasoning until fixpoint.
///
/// Returns a list of inferred triples as `HashMap<String, String>` with keys:
/// - `subject`, `predicate`, `object`, `inference_type`
///
/// Also mutates the graph in-place by adding inferred edges and type annotations.
pub fn run_datalog_reasoning(
    core: &mut GraphCore,
    subclass_relations: Vec<(String, String)>,
    subproperty_relations: Vec<(String, String)>,
    symmetric_properties: Vec<String>,
    transitive_properties: Vec<String>,
    inverse_properties: Vec<(String, String)>,
) -> Result<Vec<HashMap<String, String>>, String> {
    let mut inferred_triples = Vec::new();
    let mut new_edges_to_add = Vec::new();
    let mut new_types_to_add = Vec::new();

    // 1. Build rapid lookup structures
    let subclass_map: HashMap<String, Vec<String>> =
        subclass_relations
            .into_iter()
            .fold(HashMap::new(), |mut acc, (sub, sup)| {
                acc.entry(sub).or_default().push(sup);
                acc
            });

    let subprop_map: HashMap<String, Vec<String>> =
        subproperty_relations
            .into_iter()
            .fold(HashMap::new(), |mut acc, (sub, sup)| {
                acc.entry(sub).or_default().push(sup);
                acc
            });

    let symmetric_set: HashSet<String> = symmetric_properties.into_iter().collect();
    let transitive_set: HashSet<String> = transitive_properties.into_iter().collect();
    let inverse_map: HashMap<String, String> =
        inverse_properties
            .into_iter()
            .fold(HashMap::new(), |mut acc, (p1, p2)| {
                acc.insert(p1.clone(), p2.clone());
                acc.insert(p2, p1);
                acc
            });

    // 2. Extract current type and property facts
    let mut current_node_types: HashMap<String, HashSet<String>> = HashMap::new();
    for (node_id, props_msgpack) in &core.node_properties {
        if let Ok(val) = rmp_serde::from_slice::<serde_json::Value>(props_msgpack) {
            if let Some(t) = val.get("type").and_then(|v| v.as_str()) {
                current_node_types
                    .entry(node_id.clone())
                    .or_default()
                    .insert(t.to_string());
            }
        }
    }

    let mut current_edge_types: HashMap<(String, String), HashSet<String>> = HashMap::new();
    for ((src, tgt), props_msgpack_list) in &core.edge_properties {
        for props_msgpack in props_msgpack_list {
            if let Ok(val) = rmp_serde::from_slice::<serde_json::Value>(props_msgpack) {
                if let Some(t) = val.get("type").and_then(|v| v.as_str()) {
                    current_edge_types
                        .entry((src.clone(), tgt.clone()))
                        .or_default()
                        .insert(t.to_string());
                }
            }
        }
    }

    // 3. Forward chaining reasoning until fixpoint
    let mut changed = true;
    let mut iteration_count = 0;

    while changed && iteration_count < 100 {
        changed = false;
        let mut pending_node_types = Vec::new();
        let mut pending_edges = Vec::new();

        // Rule 1: Subclass inheritance
        for (node_id, types) in &current_node_types {
            for t in types {
                if let Some(supertypes) = subclass_map.get(t) {
                    for sup in supertypes {
                        if !types.contains(sup) {
                            pending_node_types.push((node_id.clone(), sup.clone()));
                        }
                    }
                }
            }
        }

        // Rule 2: Subproperty inheritance
        for ((src, tgt), types) in &current_edge_types {
            for t in types {
                if let Some(superprops) = subprop_map.get(t) {
                    for sup in superprops {
                        if !types.contains(sup) {
                            pending_edges.push((src.clone(), tgt.clone(), sup.clone()));
                        }
                    }
                }
            }
        }

        // Rule 3: Symmetric properties
        for ((src, tgt), types) in &current_edge_types {
            for t in types {
                if symmetric_set.contains(t) {
                    let rev_key = (tgt.clone(), src.clone());
                    let exists = current_edge_types
                        .get(&rev_key)
                        .is_some_and(|ts| ts.contains(t));
                    if !exists {
                        pending_edges.push((tgt.clone(), src.clone(), t.clone()));
                    }
                }
            }
        }

        // Rule 4: Inverse properties
        for ((src, tgt), types) in &current_edge_types {
            for t in types {
                if let Some(inv) = inverse_map.get(t) {
                    let rev_key = (tgt.clone(), src.clone());
                    let exists = current_edge_types
                        .get(&rev_key)
                        .is_some_and(|ts| ts.contains(inv));
                    if !exists {
                        pending_edges.push((tgt.clone(), src.clone(), inv.clone()));
                    }
                }
            }
        }

        // Rule 5: Transitive properties
        for p in &transitive_set {
            let mut p_edges = Vec::new();
            for ((src, tgt), ts) in &current_edge_types {
                if ts.contains(p) {
                    p_edges.push((src.clone(), tgt.clone()));
                }
            }

            for (x, y) in &p_edges {
                for (y2, z) in &p_edges {
                    if y == y2 {
                        let trans_key = (x.clone(), z.clone());
                        let exists = current_edge_types
                            .get(&trans_key)
                            .is_some_and(|ts| ts.contains(p));
                        if !exists {
                            pending_edges.push((x.clone(), z.clone(), p.clone()));
                        }
                    }
                }
            }
        }

        // Commit pending updates
        for (node_id, new_type) in pending_node_types {
            let entry = current_node_types.entry(node_id.clone()).or_default();
            if entry.insert(new_type.clone()) {
                new_types_to_add.push((node_id, new_type));
                changed = true;
            }
        }

        for (src, tgt, new_prop) in pending_edges {
            let entry = current_edge_types
                .entry((src.clone(), tgt.clone()))
                .or_default();
            if entry.insert(new_prop.clone()) {
                new_edges_to_add.push((src, tgt, new_prop));
                changed = true;
            }
        }

        iteration_count += 1;
    }

    // Apply all inferred facts back to internal structures
    for (node_id, new_type) in &new_types_to_add {
        let mut fact = HashMap::new();
        fact.insert("subject".to_string(), node_id.clone());
        fact.insert("predicate".to_string(), "type".to_string());
        fact.insert("object".to_string(), new_type.clone());
        fact.insert("inference_type".to_string(), "rust_datalog".to_string());
        inferred_triples.push(fact);

        if let Some(props_msgpack) = core.node_properties.get_mut(node_id) {
            if let Ok(mut val) =
                rmp_serde::from_slice::<serde_json::Value>(props_msgpack.as_slice())
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

    for (src, tgt, new_prop) in &new_edges_to_add {
        let mut fact = HashMap::new();
        fact.insert("subject".to_string(), src.clone());
        fact.insert("predicate".to_string(), new_prop.clone());
        fact.insert("object".to_string(), tgt.clone());
        fact.insert("inference_type".to_string(), "rust_datalog".to_string());
        inferred_triples.push(fact);

        // Safe access — only add edge if both nodes exist
        if let (Some(&src_idx), Some(&tgt_idx)) = (core.node_map.get(src), core.node_map.get(tgt)) {
            if core.graph.find_edge(src_idx, tgt_idx).is_none() {
                core.graph
                    .add_edge(src_idx, tgt_idx, format!("{}:{}", src, tgt));
            }
        }

        let val = serde_json::json!({
            "type": new_prop.clone(),
            "inferred": true
        });
        if let Ok(props_msgpack) = rmp_serde::to_vec_named(&val) {
            core.edge_properties
                .entry((src.clone(), tgt.clone()))
                .or_default()
                .push(std::sync::Arc::new(props_msgpack));
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
    core: &mut GraphCore,
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
    for ((src, tgt), props_msgpack_list) in &core.edge_properties {
        for props_msgpack in props_msgpack_list {
            if let Ok(val) = rmp_serde::from_slice::<serde_json::Value>(props_msgpack) {
                if let Some(edge_type) = val.get("type").and_then(|v| v.as_str()) {
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
        if let Some(props_msgpack) = core.node_properties.get_mut(node_id) {
            if let Ok(mut val) =
                rmp_serde::from_slice::<serde_json::Value>(props_msgpack.as_slice())
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
    core: &mut GraphCore,
    chains: Vec<(String, String, String)>,
) -> Vec<HashMap<String, String>> {
    let mut inferred = Vec::new();
    let mut new_edges: Vec<(String, String, String)> = Vec::new();

    // Index edges by type for fast lookup
    let mut edges_by_type: HashMap<String, Vec<(String, String)>> = HashMap::new();
    for ((src, tgt), props_msgpack_list) in &core.edge_properties {
        for props_msgpack in props_msgpack_list {
            if let Ok(val) = rmp_serde::from_slice::<serde_json::Value>(props_msgpack) {
                if let Some(edge_type) = val.get("type").and_then(|v| v.as_str()) {
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
                    let exists =
                        core.edge_properties
                            .get(&(a.clone(), c.clone()))
                            .map(|props| {
                                props.iter().any(|p| {
                                    rmp_serde::from_slice::<serde_json::Value>(p).ok().and_then(
                                        |v| {
                                            v.get("type")
                                                .and_then(|t| t.as_str())
                                                .map(|s| s.to_string())
                                        },
                                    ) == Some(inferred_prop.clone())
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

    // Apply inferred edges to graph
    for (src, tgt, prop) in &new_edges {
        if let (Some(&src_idx), Some(&tgt_idx)) = (core.node_map.get(src), core.node_map.get(tgt)) {
            if core.graph.find_edge(src_idx, tgt_idx).is_none() {
                core.graph
                    .add_edge(src_idx, tgt_idx, format!("{}:{}", src, tgt));
            }
        }
        let val = serde_json::json!({
            "type": prop.clone(),
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
        let mut core = GraphCore::new();
        core.add_node("a".into(), props(serde_json::json!({"type": "Person"})));
        core.add_node("b".into(), props(serde_json::json!({"type": "Person"})));
        core.add_node("c".into(), props(serde_json::json!({"type": "Person"})));
        core.add_edge(
            "a".into(),
            "b".into(),
            props(serde_json::json!({"type": "ancestor"})),
        )
        .unwrap();
        core.add_edge(
            "b".into(),
            "c".into(),
            props(serde_json::json!({"type": "ancestor"})),
        )
        .unwrap();

        let inferred = run_datalog_reasoning(
            &mut core,
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
        let mut core = GraphCore::new();
        core.add_node("rex".into(), props(serde_json::json!({"type": "Dog"})));

        let inferred = run_datalog_reasoning(
            &mut core,
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
