//! Data-mining ops (CONCEPT:EG-KG.mining.frequent-itemset-mining): association-rule
//! mining over EITHER explicit transactions OR a graph-derived transaction source
//! (compute-near-data), with optional KG write-back of the mined rules.
//!
//! Unlike the stateless finance/datascience handlers, mining is GRAPH-SCOPED: the
//! graph-derived source reads node neighborhoods off the live core, and write-back
//! materializes `:AssociationRule` nodes into the same core. So it routes in the
//! `dispatch_graph_op` chain with the graph core in hand (like the query/rdf
//! handlers) rather than the pre-graph pure-compute path.

// The Result router moves the large `Method` enum by value on the fall-through
// path; boxing the Err would allocate per non-mining request (see datascience.rs).
#![allow(clippy::result_large_err)]

use std::sync::Arc;

use eg_compute::mining::association::{self, Algorithm, LabeledRule};

use crate::graph::GraphCore;
use crate::protocol::{Method, MineAlgorithm, Response, ResultPayload, TransactionSource};

/// Handle a `Mine*` method. `Err(method)` hands a non-mining method back to the
/// dispatcher (routing fall-through). (CONCEPT:EG-KG.query.dispatch-convention.)
pub(crate) fn try_handle(
    req_id: u64,
    core: Arc<GraphCore>,
    method: Method,
) -> Result<Response, Method> {
    match method {
        Method::MineAssociate {
            transactions,
            source,
            min_support,
            min_confidence,
            algorithm,
            writeback,
        } => Ok(handle_associate(
            req_id,
            &core,
            transactions,
            source,
            min_support,
            min_confidence,
            algorithm,
            writeback,
        )),
        other => Err(other),
    }
}

/// Re-run a `MineAssociate` purely for its write-back side effect on WAL replay
/// (CONCEPT:EG-KG.mining.frequent-itemset-mining). Deterministic for explicit
/// transactions; a graph-derived source re-derives from the current graph state
/// (like the broker/memory replay ops). The response is discarded.
#[allow(dead_code)]
pub(crate) fn replay(core: &GraphCore, method: &Method) {
    if let Method::MineAssociate {
        transactions,
        source,
        min_support,
        min_confidence,
        algorithm,
        writeback: true,
    } = method
    {
        let txns = match build_transactions(core, transactions, source) {
            Ok(t) => t,
            Err(_) => return,
        };
        let rules =
            association::mine_labeled(&txns, *min_support, *min_confidence, to_algo(*algorithm));
        materialize_rules(core, &rules);
    }
}

#[allow(clippy::too_many_arguments)]
fn handle_associate(
    req_id: u64,
    core: &GraphCore,
    transactions: Vec<Vec<String>>,
    source: Option<TransactionSource>,
    min_support: f64,
    min_confidence: f64,
    algorithm: MineAlgorithm,
    writeback: bool,
) -> Response {
    let txns = match build_transactions(core, &transactions, &source) {
        Ok(t) => t,
        Err(e) => return Response::err(req_id, e),
    };
    let rules = association::mine_labeled(&txns, min_support, min_confidence, to_algo(algorithm));

    let written = if writeback {
        materialize_rules(core, &rules)
    } else {
        0
    };

    let rows: Vec<serde_json::Value> = rules
        .iter()
        .map(|r| {
            serde_json::json!({
                "antecedent": r.antecedent,
                "consequent": r.consequent,
                "support": r.support,
                "confidence": r.confidence,
                "lift": r.lift,
            })
        })
        .collect();

    Response::ok(
        req_id,
        ResultPayload::Json(serde_json::json!({
            "rules": rows,
            "n_transactions": txns.len(),
            "n_rules": rules.len(),
            "written_back": written,
        })),
    )
}

/// Resolve the transaction set: explicit `transactions` win; otherwise derive them
/// from the graph via `source`. An empty request (neither provided) yields no
/// transactions (⇒ no rules), which is a valid empty result, not an error.
fn build_transactions(
    core: &GraphCore,
    transactions: &[Vec<String>],
    source: &Option<TransactionSource>,
) -> Result<Vec<Vec<String>>, String> {
    if !transactions.is_empty() {
        return Ok(transactions.to_vec());
    }
    match source {
        Some(spec) => Ok(derive_from_graph(core, spec)),
        None => Ok(Vec::new()),
    }
}

/// Build one transaction per `node_label` instance from its neighbor set
/// (CONCEPT:EG-KG.mining.graph-derived-transactions). Each transaction is the deduped
/// set of `item_field` values over the owner's neighbors in `direction`, optionally
/// filtered to a `relation`.
fn derive_from_graph(core: &GraphCore, spec: &TransactionSource) -> Vec<Vec<String>> {
    let owners = core.get_nodes_by_label(&spec.node_label, spec.limit);
    let mut out: Vec<Vec<String>> = Vec::with_capacity(owners.len());
    for (owner_id, _blob) in owners {
        let neighbors = neighbors_in_direction(core, &owner_id, &spec.direction);
        let mut basket: Vec<String> = Vec::new();
        for nbr in neighbors {
            if let Some(rel) = &spec.relation {
                if !edge_matches_relation(core, &owner_id, &nbr, &spec.direction, rel) {
                    continue;
                }
            }
            if let Some(item) = extract_item(core, &nbr, &spec.item_field) {
                basket.push(item);
            }
        }
        basket.sort_unstable();
        basket.dedup();
        if !basket.is_empty() {
            out.push(basket);
        }
    }
    out
}

fn neighbors_in_direction(core: &GraphCore, node_id: &str, direction: &str) -> Vec<String> {
    match direction {
        "in" => core.get_predecessors(node_id).unwrap_or_default(),
        "any" => {
            let mut v = core.get_successors(node_id).unwrap_or_default();
            v.extend(core.get_predecessors(node_id).unwrap_or_default());
            v.sort_unstable();
            v.dedup();
            v
        }
        // "out" (default) and anything else.
        _ => core.get_successors(node_id).unwrap_or_default(),
    }
}

/// Whether an edge between owner and neighbor carries `relation` on its
/// `relation`/`type` property. Checks both directions when `direction == "any"`.
fn edge_matches_relation(
    core: &GraphCore,
    owner: &str,
    neighbor: &str,
    direction: &str,
    relation: &str,
) -> bool {
    let pairs: &[(&str, &str)] = match direction {
        "in" => &[(neighbor, owner)],
        "any" => &[(owner, neighbor), (neighbor, owner)],
        _ => &[(owner, neighbor)],
    };
    for &(s, t) in pairs {
        for blob in core.get_edge_properties(s, t) {
            if let Ok(val) = rmp_serde::from_slice::<serde_json::Value>(&blob) {
                for key in ["relation", "type", "rel"] {
                    if val.get(key).and_then(|v| v.as_str()) == Some(relation) {
                        return true;
                    }
                }
            }
        }
    }
    false
}

/// Extract the item value for `neighbor` per `item_field`:
///   * `None`         ⇒ the neighbor's node id.
///   * `"label"`      ⇒ the neighbor's type/label.
///   * `"prop:<key>"` ⇒ the neighbor's property `<key>`.
fn extract_item(core: &GraphCore, neighbor: &str, item_field: &Option<String>) -> Option<String> {
    let field = match item_field {
        None => return Some(neighbor.to_string()),
        Some(f) => f.as_str(),
    };
    let props = core.get_node_properties(neighbor)?;
    let val: serde_json::Value = rmp_serde::from_slice(&props).ok()?;
    if field == "label" {
        for key in ["type", "node_type", "label"] {
            if let Some(s) = val.get(key).and_then(|v| v.as_str()) {
                return Some(s.to_string());
            }
        }
        return None;
    }
    if let Some(key) = field.strip_prefix("prop:") {
        return val.get(key).and_then(json_scalar_string);
    }
    // Bare field name ⇒ treat as a property key.
    val.get(field).and_then(json_scalar_string)
}

fn json_scalar_string(v: &serde_json::Value) -> Option<String> {
    match v {
        serde_json::Value::String(s) => Some(s.clone()),
        serde_json::Value::Number(n) => Some(n.to_string()),
        serde_json::Value::Bool(b) => Some(b.to_string()),
        _ => None,
    }
}

/// Materialize each rule as a typed `:AssociationRule` node (the discovery
/// flywheel, CONCEPT:EG-KG.mining.rule-writeback). The node id is a deterministic
/// digest of `antecedent ⇒ consequent` so replay is idempotent. Each rule is linked
/// (best-effort) to any item that is itself a resident node id, via a `RULE_ITEM`
/// edge — so OWL reasoning + the next mining pass can traverse from the rule to its
/// sources. Returns the number of rule nodes written.
fn materialize_rules(core: &GraphCore, rules: &[LabeledRule]) -> usize {
    let mut written = 0usize;
    for r in rules {
        let node_id = rule_node_id(&r.antecedent, &r.consequent);
        let props = serde_json::json!({
            "type": "AssociationRule",
            "antecedent": r.antecedent,
            "consequent": r.consequent,
            "support": r.support,
            "confidence": r.confidence,
            "lift": r.lift,
        });
        let Ok(blob) = rmp_serde::to_vec_named(&props) else {
            continue;
        };
        core.add_node(node_id.clone(), blob);
        // Link the rule to any item that is a resident node (source objects).
        for item in r.antecedent.iter().chain(r.consequent.iter()) {
            if core.has_node(item) {
                let edge = serde_json::json!({ "relation": "RULE_ITEM" });
                if let Ok(eb) = rmp_serde::to_vec_named(&edge) {
                    let _ = core.add_edge(node_id.clone(), item.clone(), eb);
                }
            }
        }
        written += 1;
    }
    written
}

/// Deterministic, collision-resistant node id for a rule (order-stable — the items
/// are already sorted within each side by the rule generator).
fn rule_node_id(antecedent: &[String], consequent: &[String]) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(antecedent.join("\u{1}").as_bytes());
    hasher.update([0u8]);
    hasher.update(consequent.join("\u{1}").as_bytes());
    let digest = hasher.finalize();
    format!("assocrule:{}", hex::encode(&digest[..12]))
}

fn to_algo(a: MineAlgorithm) -> Algorithm {
    match a {
        MineAlgorithm::Apriori => Algorithm::Apriori,
        MineAlgorithm::Fpgrowth => Algorithm::FpGrowth,
        MineAlgorithm::Eclat => Algorithm::Eclat,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph::GraphCore;
    use crate::protocol::TransactionSource;

    fn node(props: serde_json::Value) -> Vec<u8> {
        rmp_serde::to_vec_named(&props).unwrap()
    }

    #[test]
    fn non_mining_method_falls_through() {
        let core = Arc::new(GraphCore::new());
        let m = Method::NodeCount;
        assert!(matches!(try_handle(1, core, m), Err(Method::NodeCount)));
    }

    #[test]
    fn explicit_transactions_produce_rules() {
        let core = Arc::new(GraphCore::new());
        let txns = vec![
            vec!["a".to_string(), "b".to_string(), "c".to_string()],
            vec!["a".to_string(), "b".to_string()],
            vec!["a".to_string(), "c".to_string()],
            vec!["b".to_string(), "c".to_string()],
            vec!["a".to_string(), "b".to_string(), "c".to_string()],
        ];
        let m = Method::MineAssociate {
            transactions: txns,
            source: None,
            min_support: 0.4,
            min_confidence: 0.5,
            algorithm: MineAlgorithm::Apriori,
            writeback: false,
        };
        let resp = try_handle(7, core, m).expect("handled");
        let Some(ResultPayload::Json(v)) = resp.result else {
            panic!("expected json payload");
        };
        assert_eq!(v["n_transactions"], 5);
        assert!(v["n_rules"].as_u64().unwrap() > 0);
        assert_eq!(v["written_back"], 0);
    }

    #[test]
    fn graph_derived_source_and_writeback() {
        let core = Arc::new(GraphCore::new());
        // Two "Cart" owners, each linked to its purchased "Item" nodes.
        core.add_node("cart1".into(), node(serde_json::json!({"type": "Cart"})));
        core.add_node("cart2".into(), node(serde_json::json!({"type": "Cart"})));
        for item in ["milk", "bread"] {
            core.add_node(item.into(), node(serde_json::json!({"type": "Item"})));
        }
        let _ = core.add_edge("cart1".into(), "milk".into(), node(serde_json::json!({})));
        let _ = core.add_edge("cart1".into(), "bread".into(), node(serde_json::json!({})));
        let _ = core.add_edge("cart2".into(), "milk".into(), node(serde_json::json!({})));
        let _ = core.add_edge("cart2".into(), "bread".into(), node(serde_json::json!({})));

        let m = Method::MineAssociate {
            transactions: Vec::new(),
            source: Some(TransactionSource {
                node_label: "Cart".into(),
                direction: "out".into(),
                item_field: None, // neighbor node id ⇒ "milk"/"bread"
                relation: None,
                limit: 0,
            }),
            min_support: 0.5,
            min_confidence: 0.5,
            algorithm: MineAlgorithm::Fpgrowth,
            writeback: true,
        };
        let resp = try_handle(9, Arc::clone(&core), m).expect("handled");
        let Some(ResultPayload::Json(v)) = resp.result else {
            panic!("expected json payload");
        };
        assert_eq!(v["n_transactions"], 2);
        let written = v["written_back"].as_u64().unwrap();
        assert!(written > 0);
        // The dispatch shell calls `mark_dirty()` after a write (invalidating the
        // lazy label index); mirror that here so the label query sees the new nodes.
        core.mark_dirty();
        // Write-back created queryable :AssociationRule nodes.
        let rule_nodes = core.get_nodes_by_label("AssociationRule", 0);
        assert_eq!(rule_nodes.len() as u64, written);
    }
}
