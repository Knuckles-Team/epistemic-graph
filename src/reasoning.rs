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
    for (node_id, props_json) in &core.node_properties {
        if let Ok(val) = serde_json::from_str::<serde_json::Value>(props_json) {
            if let Some(t) = val.get("type").and_then(|v| v.as_str()) {
                current_node_types
                    .entry(node_id.clone())
                    .or_default()
                    .insert(t.to_string());
            }
        }
    }

    let mut current_edge_types: HashMap<(String, String), HashSet<String>> = HashMap::new();
    for ((src, tgt), props_json_list) in &core.edge_properties {
        for props_json in props_json_list {
            if let Ok(val) = serde_json::from_str::<serde_json::Value>(props_json) {
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

        if let Some(props_json) = core.node_properties.get_mut(node_id) {
            if let Ok(mut val) = serde_json::from_str::<serde_json::Value>(props_json) {
                if let Some(obj) = val.as_object_mut() {
                    obj.insert(
                        "inferred_type".to_string(),
                        serde_json::Value::String(new_type.clone()),
                    );
                    if let Ok(updated) = serde_json::to_string(&val) {
                        *props_json = updated;
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
        if let (Some(&src_idx), Some(&tgt_idx)) =
            (core.node_map.get(src), core.node_map.get(tgt))
        {
            if core.graph.find_edge(src_idx, tgt_idx).is_none() {
                core.graph
                    .add_edge(src_idx, tgt_idx, format!("{}:{}", src, tgt));
            }
        }

        let props_json = format!("{{\"type\": \"{}\", \"inferred\": true}}", new_prop);
        core.edge_properties
            .entry((src.clone(), tgt.clone()))
            .or_default()
            .push(props_json);
    }

    Ok(inferred_triples)
}
