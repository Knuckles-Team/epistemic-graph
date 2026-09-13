use super::*;

// ── Free Functions (non-method helpers) ──────────────────────────────────

/// Apply the Ebbinghaus retention curve to a single property map in place.
///
/// Reads `confidence` (default 1.0), `last_access` (→ `updated_at` → `created_at`
/// → `now`) and an optional per-item `half_life`. Writes the decayed
/// `confidence` and advances `last_access` to `now`. Returns `(new_confidence,
/// changed)`; `changed` is false when no time elapsed (fresh item) so callers can
/// skip a re-encode. `last_access` is always stamped so the next sweep has an
/// anchor.
pub(super) fn apply_decay(
    obj: &mut serde_json::Map<String, serde_json::Value>,
    now: u64,
    default_half_life: f64,
) -> (f64, bool) {
    let confidence = obj
        .get("confidence")
        .and_then(|v| v.as_f64())
        .unwrap_or(1.0);
    let last_access = obj
        .get("last_access")
        .and_then(|v| v.as_u64())
        .or_else(|| obj.get("updated_at").and_then(|v| v.as_u64()))
        .or_else(|| obj.get("created_at").and_then(|v| v.as_u64()))
        .unwrap_or(now);
    let half_life = obj
        .get("half_life")
        .and_then(|v| v.as_f64())
        .filter(|h| *h > 0.0)
        .unwrap_or(default_half_life);

    if now <= last_access || half_life <= 0.0 {
        // Nothing to decay yet; ensure there is an anchor for the next sweep.
        obj.insert("last_access".to_string(), serde_json::json!(now));
        return (confidence, false);
    }

    let dt = (now - last_access) as f64;
    let retention = 0.5_f64.powf(dt / half_life);
    let new_conf = (confidence * retention).clamp(0.0, 1.0);
    obj.insert("confidence".to_string(), serde_json::json!(new_conf));
    obj.insert("last_access".to_string(), serde_json::json!(now));
    (new_conf, true)
}

/// Conservative default cap on the number of matches [`vf2_match_views`] collects
/// before stopping the backtracking search early. VF2 subgraph isomorphism is
/// NP-hard with no bound otherwise; a caller wanting more must pass an explicit
/// `max_results` on the request.
pub const DEFAULT_VF2_MAX_RESULTS: usize = 1_000;

/// Conservative default cap on the number of candidate-pair attempts (one per
/// `backtrack_match` inner-loop iteration) [`vf2_match_views`] spends before
/// stopping early, regardless of how many matches it has already found. Bounds
/// worst-case CPU on a pathological pattern/host pair.
pub const DEFAULT_VF2_MAX_STEPS: usize = 2_000_000;

/// Collected matches plus search-budget accounting threaded through
/// [`backtrack_match`] (bundled into one struct rather than 2 more positional
/// params, keeping the function at 7 arguments). `truncated` is checked at the
/// top of every recursive call, so a tripped budget unwinds the whole call tree
/// promptly instead of finishing the in-flight branch.
struct Vf2Search {
    matches: Vec<HashMap<String, String>>,
    max_results: usize,
    max_steps: usize,
    steps: usize,
    truncated: bool,
}

/// VF2 subgraph match of `pattern` against an already-materialized `host`
/// `GraphView` (vs [`GraphCore::vf2_subgraph_match`], which snapshots its own
/// live graph first). Lets an off-lock caller — e.g. the Cypher exec
/// (CONCEPT:EG-KG.query.dep-free-behind), which already holds the `analysis_snapshot()` view — reuse
/// the exact same matcher without re-snapshotting. Each result maps a pattern
/// node id → the host node id it bound to.
///
/// `max_results`/`max_steps` bound the otherwise-unbounded backtracking search
/// (`0` ⇒ [`DEFAULT_VF2_MAX_RESULTS`]/[`DEFAULT_VF2_MAX_STEPS`]); the returned
/// `bool` is `true` when the search stopped early against either budget rather
/// than exhausting the search space (a PARTIAL result, not proof no further
/// match exists).
pub fn vf2_match_views(
    host: &GraphView,
    pattern: &GraphView,
    max_results: usize,
    max_steps: usize,
) -> (Vec<HashMap<String, String>>, bool) {
    let pattern_nodes: Vec<String> = pattern.node_map.keys().cloned().collect();
    if pattern_nodes.is_empty() {
        return (Vec::new(), false);
    }
    let mut current_mapping = HashMap::new();
    let mut mapped_targets = std::collections::HashSet::new();
    let mut search = Vf2Search {
        matches: Vec::new(),
        max_results: if max_results == 0 {
            DEFAULT_VF2_MAX_RESULTS
        } else {
            max_results
        },
        max_steps: if max_steps == 0 {
            DEFAULT_VF2_MAX_STEPS
        } else {
            max_steps
        },
        steps: 0,
        truncated: false,
    };
    backtrack_match(
        host,
        0,
        &pattern_nodes,
        &mut current_mapping,
        &mut mapped_targets,
        pattern,
        &mut search,
    );
    (search.matches, search.truncated)
}

fn backtrack_match(
    host: &GraphView,
    pattern_node_idx: usize,
    pattern_nodes: &[String],
    current_mapping: &mut HashMap<String, String>,
    mapped_targets: &mut std::collections::HashSet<String>,
    pattern: &GraphView,
    search: &mut Vf2Search,
) {
    if search.truncated {
        return;
    }
    if pattern_node_idx == pattern_nodes.len() {
        record_vf2_match(current_mapping, search);
        return;
    }

    let p_node = &pattern_nodes[pattern_node_idx];

    for t_node in host.node_map.keys() {
        if !vf2_step_allowed(search) {
            return;
        }
        if mapped_targets.contains(t_node) {
            continue;
        }
        if !check_match(host, p_node, t_node, current_mapping, pattern) {
            continue;
        }

        current_mapping.insert(p_node.clone(), t_node.clone());
        mapped_targets.insert(t_node.clone());

        backtrack_match(
            host,
            pattern_node_idx + 1,
            pattern_nodes,
            current_mapping,
            mapped_targets,
            pattern,
            search,
        );

        current_mapping.remove(p_node);
        mapped_targets.remove(t_node);

        if search.truncated {
            return;
        }
    }
}

/// Record one complete pattern→host mapping, truncating the search once
/// `max_results` mappings have been collected.
fn record_vf2_match(current_mapping: &HashMap<String, String>, search: &mut Vf2Search) {
    search.matches.push(current_mapping.clone());
    if search.matches.len() >= search.max_results {
        search.truncated = true;
    }
}

/// Charge one VF2 candidate step against the budget. Returns `false` when the
/// caller must stop: the search was ALREADY truncated, or this step exhausted
/// `max_steps` (which truncates it).
fn vf2_step_allowed(search: &mut Vf2Search) -> bool {
    if search.truncated {
        return false;
    }
    search.steps += 1;
    if search.steps > search.max_steps {
        search.truncated = true;
        return false;
    }
    true
}

fn check_match(
    host: &GraphView,
    p_node: &str,
    t_node: &str,
    current_mapping: &HashMap<String, String>,
    pattern: &GraphView,
) -> bool {
    let p_props = pattern
        .node_properties
        .get(p_node)
        .map(|s| s.as_slice())
        .unwrap_or(b"{}");
    let t_props = host
        .node_properties
        .get(t_node)
        .map(|s| s.as_slice())
        .unwrap_or(b"{}");

    if !match_props(p_props, t_props) {
        return false;
    }

    let Some(&p_idx) = pattern.node_map.get(p_node) else {
        return false;
    };

    check_directed_edges(
        host,
        pattern,
        p_idx,
        p_node,
        t_node,
        current_mapping,
        petgraph::Direction::Incoming,
    ) && check_directed_edges(
        host,
        pattern,
        p_idx,
        p_node,
        t_node,
        current_mapping,
        petgraph::Direction::Outgoing,
    )
}

/// Every already-mapped pattern edge at `p_idx` running in `direction` must exist in the
/// host between the mapped endpoints, with matching edge properties. Pattern edges whose
/// far endpoint is not mapped yet are skipped — a later step checks them.
fn check_directed_edges(
    host: &GraphView,
    pattern: &GraphView,
    p_idx: NodeIndex,
    p_node: &str,
    t_node: &str,
    current_mapping: &HashMap<String, String>,
    direction: petgraph::Direction,
) -> bool {
    let incoming = direction == petgraph::Direction::Incoming;
    for edge in pattern.graph.edges_directed(p_idx, direction) {
        let far = if incoming {
            edge.source()
        } else {
            edge.target()
        };
        let p_far = &pattern.graph[far];
        let Some(t_far) = current_mapping.get(p_far) else {
            continue;
        };
        // Orient the pair so the edge always reads source -> target.
        let (p_src, p_tgt) = if incoming {
            (p_far.as_str(), p_node)
        } else {
            (p_node, p_far.as_str())
        };
        let (t_src, t_tgt) = if incoming {
            (t_far.as_str(), t_node)
        } else {
            (t_node, t_far.as_str())
        };
        if !host.has_edge(t_src, t_tgt) {
            return false;
        }
        if !check_edge_props(host, pattern, p_src, p_tgt, t_src, t_tgt) {
            return false;
        }
    }
    true
}

fn check_edge_props(
    host: &GraphView,
    pattern: &GraphView,
    p_src: &str,
    p_tgt: &str,
    t_src: &str,
    t_tgt: &str,
) -> bool {
    let Some(p_props_list) = pattern
        .edge_properties
        .get(&(p_src.to_string(), p_tgt.to_string()))
    else {
        // No pattern edge between these endpoints ⇒ nothing to constrain.
        return true;
    };
    let Some(t_props_list) = host
        .edge_properties
        .get(&(t_src.to_string(), t_tgt.to_string()))
    else {
        return false;
    };
    p_props_list
        .iter()
        .all(|p_edge_props| any_edge_props_match(p_edge_props, t_props_list))
}

/// Does ANY of the host's parallel edge blobs satisfy this one pattern blob?
fn any_edge_props_match(p_edge_props: &[u8], t_props_list: &[Arc<Vec<u8>>]) -> bool {
    t_props_list
        .iter()
        .any(|t_edge_props| match_props(p_edge_props, t_edge_props))
}

pub fn match_props(p_msgpack: &[u8], t_msgpack: &[u8]) -> bool {
    let p_val: serde_json::Value = match decode_property_value(p_msgpack) {
        Ok(v) => v,
        Err(_) => return false,
    };
    let t_val: serde_json::Value = match decode_property_value(t_msgpack) {
        Ok(v) => v,
        Err(_) => return false,
    };

    if let (Some(p_obj), Some(t_obj)) = (p_val.as_object(), t_val.as_object()) {
        for (k, v) in p_obj {
            if let Some(t_v) = t_obj.get(k) {
                if v != t_v {
                    return false;
                }
            } else {
                return false;
            }
        }
        true
    } else {
        p_val == t_val
    }
}
