use super::writeback::*;
use super::*;
use eg_compute::mining::association::{self, Algorithm, LabeledRule};
use eg_types::compute_result::mining::{AssociationMiningResult, AssociationRuleRow};
use eg_types::result_contract::compute as results;

pub(crate) struct AssociationRequest {
    pub(crate) transactions: Vec<Vec<String>>,
    pub(crate) source: Option<TransactionSource>,
    pub(crate) min_support: f64,
    pub(crate) min_confidence: f64,
    pub(crate) algorithm: MineAlgorithm,
    pub(crate) writeback: WritebackOptions,
}

pub(crate) fn handle_associate(
    req_id: u64,
    core: &GraphCore,
    request: AssociationRequest,
) -> Response {
    let AssociationRequest {
        transactions,
        source,
        min_support,
        min_confidence,
        algorithm,
        writeback,
    } = request;
    let txns = build_transactions(core, &transactions, &source);
    let rules = association::mine_labeled(&txns, min_support, min_confidence, to_algo(algorithm));

    let written = if writeback.enabled {
        materialize_rules(core, &rules)
    } else {
        0
    };
    #[cfg(feature = "epistemic")]
    if writeback.enabled && writeback.as_claim {
        materialize_rule_claims(core, &rules, &source);
    }

    let rows: Vec<AssociationRuleRow> = rules
        .iter()
        .map(|r| AssociationRuleRow {
            antecedent: r.antecedent.clone(),
            consequent: r.consequent.clone(),
            support: r.support,
            confidence: r.confidence,
            lift: r.lift,
        })
        .collect();

    Response::ok(
        req_id,
        ResultPayload::of::<results::MineAssociate>(AssociationMiningResult {
            rules: rows,
            n_transactions: txns.len(),
            n_rules: rules.len(),
            written_back: written,
        }),
    )
}

/// Resolve the transaction set: explicit `transactions` win; otherwise derive them
/// from the graph via `source`. An empty request (neither provided) yields no
/// transactions (⇒ no rules), which is a valid empty result, not an error.
pub(super) fn build_transactions(
    core: &GraphCore,
    transactions: &[Vec<String>],
    source: &Option<TransactionSource>,
) -> Vec<Vec<String>> {
    if !transactions.is_empty() {
        return transactions.to_vec();
    }
    match source {
        Some(spec) => derive_from_graph(core, spec),
        None => Vec::new(),
    }
}

/// Build one transaction per `node_label` instance from its neighbor set
/// (CONCEPT:EG-KG.mining.graph-derived-transactions). Each transaction is the deduped
/// set of `item_field` values over the owner's neighbors in `direction`, optionally
/// filtered to a `relation`.
pub(super) fn derive_from_graph(core: &GraphCore, spec: &TransactionSource) -> Vec<Vec<String>> {
    let owners = core.get_nodes_by_label(&spec.node_label, spec.limit);
    let mut out: Vec<Vec<String>> = Vec::with_capacity(owners.len());
    for (owner_id, _blob) in owners {
        let mut basket = project_neighbor_items(
            core,
            &owner_id,
            &spec.direction,
            &spec.relation,
            &spec.item_field,
            false,
        );
        basket.sort_unstable();
        basket.dedup();
        if !basket.is_empty() {
            out.push(basket);
        }
    }
    out
}

pub(super) fn neighbors_in_direction(
    core: &GraphCore,
    node_id: &str,
    direction: &str,
) -> Vec<String> {
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

/// Project one owner's neighbors into mining items. Association transactions
/// consume a set, while sequential mining asks for insertion order; keeping
/// that one distinction explicit lets both graph-derived sources share the
/// same relation filtering and item extraction contract.
pub(super) fn project_neighbor_items(
    core: &GraphCore,
    owner_id: &str,
    direction: &str,
    relation: &Option<String>,
    item_field: &Option<String>,
    chronological: bool,
) -> Vec<String> {
    let mut neighbors = neighbors_in_direction(core, owner_id, direction);
    if chronological && direction != "any" {
        neighbors.reverse();
    }
    neighbors
        .into_iter()
        .filter(|neighbor| {
            relation.as_ref().is_none_or(|wanted| {
                edge_matches_relation(core, owner_id, neighbor, direction, wanted)
            })
        })
        .filter_map(|neighbor| extract_item(core, &neighbor, item_field))
        .collect()
}

/// Whether an edge between owner and neighbor carries the requested canonical
/// `relationship`. Checks both directions when `direction == "any"`.
pub(super) fn edge_matches_relation(
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
            if let Ok(val) = eg_types::msgpack::decode_property_value(&blob) {
                if val.get("relationship").and_then(|v| v.as_str()) == Some(relation) {
                    return true;
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
pub(super) fn extract_item(
    core: &GraphCore,
    neighbor: &str,
    item_field: &Option<String>,
) -> Option<String> {
    let field = match item_field {
        None => return Some(neighbor.to_string()),
        Some(f) => f.as_str(),
    };
    let props = core.get_node_properties(neighbor)?;
    let val = eg_types::msgpack::decode_property_value(&props).ok()?;
    if field == "label" {
        for key in ["type", "node_type", "label"] {
            if let Some(s) = val.get(key).and_then(|v| v.as_str()) {
                return Some(s.to_string());
            }
        }
        return None;
    }
    if let Some(key) = field.strip_prefix("prop:") {
        return val.get(key).and_then(GraphCore::property_value_key);
    }
    // Bare field name ⇒ treat as a property key. Canonicalized through
    // `GraphCore::property_value_key`, which is the property index's own
    // single source of truth for how a scalar becomes an equality key —
    // that function's doc comment exists precisely to stop a second
    // implementation here drifting out of sync with the index it must agree
    // with.
    val.get(field).and_then(GraphCore::property_value_key)
}

/// Materialize each rule as a typed `:AssociationRule` node (the discovery
/// flywheel, CONCEPT:EG-KG.mining.rule-writeback). The node id is a deterministic
/// digest of `antecedent ⇒ consequent` so replay is idempotent. Each rule is linked
/// (best-effort) to any item that is itself a resident node id, via a `RULE_ITEM`
/// edge — so OWL reasoning + the next mining pass can traverse from the rule to its
/// sources. Returns the number of rule nodes written.
pub(super) fn materialize_rules(core: &GraphCore, rules: &[LabeledRule]) -> usize {
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
        if !writeback_node(core, &node_id, &props) {
            continue;
        }
        // Link the rule to any item that is a resident node (source objects).
        for item in r.antecedent.iter().chain(r.consequent.iter()) {
            if core.has_node(item) {
                writeback_relationship(core, &node_id, item, "RULE_ITEM");
            }
        }
        written += 1;
    }
    written
}

/// Deterministic, collision-resistant node id for a rule (order-stable — the items
/// are already sorted within each side by the rule generator).
pub(super) fn rule_node_id(antecedent: &[String], consequent: &[String]) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(antecedent.join("\u{1}").as_bytes());
    hasher.update([0u8]);
    hasher.update(consequent.join("\u{1}").as_bytes());
    let digest = hasher.finalize();
    format!("assocrule:{}", hex::encode(&digest[..12]))
}

pub(super) fn to_algo(a: MineAlgorithm) -> Algorithm {
    match a {
        MineAlgorithm::Apriori => Algorithm::Apriori,
        MineAlgorithm::Fpgrowth => Algorithm::FpGrowth,
        MineAlgorithm::Eclat => Algorithm::Eclat,
    }
}
