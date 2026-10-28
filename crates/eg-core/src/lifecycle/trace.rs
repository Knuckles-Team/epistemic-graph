//! The lifecycle trace (EG-SDLC-R006.2): a breadth-first walk of the derived
//! edge set from one anchor, bounded by depth and node budget.

use std::collections::{BTreeSet, VecDeque};

use eg_types::lifecycle::{
    LifecycleTraceAnswer, LifecycleTraceRequest, TraceEdge, TraceRecord, TRACE_NODE_BUDGET,
};

use super::store::LifecycleStore;

/// Walk the edge set from `request.anchor` in both edge directions, to
/// `request.depth`. Records are filtered by `request.kinds` (empty keeps all);
/// the walk still passes through filtered records. `truncated` reports that
/// the node budget stopped the walk. An unknown anchor yields an empty answer.
pub fn lifecycle_trace(
    store: &LifecycleStore,
    request: &LifecycleTraceRequest,
) -> LifecycleTraceAnswer {
    let mut answer = LifecycleTraceAnswer::default();
    if store.node_kind(&request.anchor).is_none() {
        return answer;
    }
    let mut seen: BTreeSet<String> = BTreeSet::from([request.anchor.clone()]);
    let mut queue = VecDeque::from([(request.anchor.clone(), 0u8)]);
    let mut kept: BTreeSet<String> = BTreeSet::new();
    while let Some((id, depth)) = queue.pop_front() {
        let Some(kind) = store.node_kind(&id) else {
            continue;
        };
        if request.kinds.is_empty() || request.kinds.contains(&kind) {
            kept.insert(id.clone());
            answer.records.push(TraceRecord {
                id: id.clone(),
                kind,
            });
        }
        if depth >= request.depth {
            continue;
        }
        for edge in store.edges_of(&id) {
            let other = if edge.from == id {
                &edge.to
            } else {
                &edge.from
            };
            if seen.contains(other) {
                continue;
            }
            if seen.len() >= TRACE_NODE_BUDGET {
                answer.truncated = true;
                continue;
            }
            seen.insert(other.clone());
            queue.push_back((other.clone(), depth + 1));
        }
    }
    answer.edges = store
        .edges()
        .filter(|e| kept.contains(&e.from) && kept.contains(&e.to))
        .map(|e| TraceEdge {
            kind: e.kind,
            from: e.from.clone(),
            to: e.to.clone(),
            rule: e.rule.clone(),
            rule_version: e.rule_version,
        })
        .collect();
    answer
}
