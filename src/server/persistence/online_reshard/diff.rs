//! [`compute_delta`]'s per-table diff halves, split out of `online_reshard.rs` itself
//! so the parent file's KISS `lines_per_file`/`functions_per_file`/`statements_per_file`
//! budget has room for the graft/reshard/migration machinery it still owns.

use super::*;

/// Node half of [`compute_delta`]'s diff, keyed by id: upsert new/changed rows, remove
/// ids gone from `latest`. Split out so `compute_delta` stays under the complexity cap.
pub(super) fn diff_nodes(bulk: &RawGraphRows, latest: &RawGraphRows, delta: &mut RawGraphDelta) {
    use std::collections::HashMap;
    let base_nodes: HashMap<&str, &Vec<u8>> =
        bulk.nodes.iter().map(|(k, v)| (k.as_str(), v)).collect();
    let latest_node_ids: std::collections::HashSet<&str> =
        latest.nodes.iter().map(|(k, _)| k.as_str()).collect();
    for (id, blob) in &latest.nodes {
        match base_nodes.get(id.as_str()) {
            Some(prev) if *prev == blob => {}
            _ => delta.upsert_nodes.push((id.clone(), blob.clone())),
        }
    }
    for (id, _) in &bulk.nodes {
        if !latest_node_ids.contains(id.as_str()) {
            delta.remove_nodes.push(id.clone());
        }
    }
}

/// Edge half of [`compute_delta`]'s diff, keyed by `(src, tgt, ordinal)`. Split out so
/// `compute_delta` stays under the complexity cap.
pub(super) fn diff_edges(bulk: &RawGraphRows, latest: &RawGraphRows, delta: &mut RawGraphDelta) {
    use std::collections::HashMap;
    type EdgeKey = (String, String, u32);
    let base_edges: HashMap<EdgeKey, &Vec<u8>> = bulk
        .edges
        .iter()
        .map(|(s, t, o, v)| ((s.clone(), t.clone(), *o), v))
        .collect();
    let latest_edge_keys: std::collections::HashSet<EdgeKey> = latest
        .edges
        .iter()
        .map(|(s, t, o, _)| (s.clone(), t.clone(), *o))
        .collect();
    for (s, t, o, blob) in &latest.edges {
        let key = (s.clone(), t.clone(), *o);
        match base_edges.get(&key) {
            Some(prev) if *prev == blob => {}
            _ => delta
                .upsert_edges
                .push((s.clone(), t.clone(), *o, blob.clone())),
        }
    }
    for (s, t, o, _) in &bulk.edges {
        let key = (s.clone(), t.clone(), *o);
        if !latest_edge_keys.contains(&key) {
            delta.remove_edges.push(key);
        }
    }
}

/// Ledger (append-only) and, under `security`, audit diff halves of [`compute_delta`]:
/// both simply copy entries with a seq beyond the bulk's tail. Split out so
/// `compute_delta` stays under the complexity cap.
pub(super) fn diff_ledger_and_audit(
    bulk: &RawGraphRows,
    latest: &RawGraphRows,
    delta: &mut RawGraphDelta,
) {
    let base_ledger_max = bulk.ledger.iter().map(|(seq, _)| *seq).max();
    for (seq, line) in &latest.ledger {
        if base_ledger_max.is_none_or(|m| *seq > m) {
            delta.upsert_ledger.push((*seq, line.clone()));
        }
    }

    #[cfg(feature = "security")]
    {
        let base_audit_max = bulk.audit.iter().map(|(seq, _)| *seq).max();
        for (seq, blob) in &latest.audit {
            if base_audit_max.is_none_or(|m| *seq > m) {
                delta.upsert_audit.push((*seq, blob.clone()));
            }
        }
    }
}
