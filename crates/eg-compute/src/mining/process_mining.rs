// CONCEPT:EG-KG.mining.process-mining — Directly-follows graph + alpha-miner-lite.
//
// Pure-Rust, dependency-light: given a set of ORDERED event traces (each a
// time-ordered list of interned activity ids — an activity may repeat within a
// trace), build the **directly-follows graph** (DFG): a directed edge `a -> b`
// weighted by how many times `a` is immediately followed by `b` across all
// traces. `alpha_lite` derives the classic alpha-algorithm LOG ABSTRACTION (the
// "footprint matrix") from the DFG:
//
//   * **causal** (`a > b`): `a` directly-follows `b` somewhere but `b` never
//     directly-follows `a` — a one-way dependency (sequential flow `a -> b`).
//   * **parallel** (`a || b`): BOTH `a > b` and `b > a` occur — the two
//     activities interleave (concurrent flow, no fixed order).
//   * **choice** (`a # b`): neither directly-follows the other anywhere — the
//     two activities never appear adjacent (an exclusive branch, or unrelated).
//
// plus the trace-level **start** / **end** activity sets. This is the standard
// alpha-algorithm footprint (Van der Aalst), stopping short of the full
// place/transition Petri-net construction — a documented "lite" scope: the
// footprint alone is already the queryable process MODEL this family writes
// back (a `:ProcessModel` claim), and the Petri-net synthesis step adds no
// further epistemic content over the relations it's built from.

use std::collections::HashMap;

pub type ActivityId = u32;

/// The directly-follows graph: dense activity ids `0..activities.len()` and the
/// `(from, to, count)` edges observed across all traces.
#[derive(Debug, Clone, PartialEq)]
pub struct DirectlyFollowsGraph {
    pub n_activities: usize,
    /// `(from, to, count)`, sorted by `(from, to)`.
    pub edges: Vec<(ActivityId, ActivityId, usize)>,
}

/// The alpha-miner-lite footprint + start/end sets over a [`DirectlyFollowsGraph`].
#[derive(Debug, Clone, PartialEq)]
pub struct ProcessModel {
    pub n_activities: usize,
    pub dfg_edges: Vec<(ActivityId, ActivityId, usize)>,
    /// One-way directly-follows pairs `(a, b)`: `a > b` and NOT `b > a`.
    pub causal: Vec<(ActivityId, ActivityId)>,
    /// Two-way directly-follows pairs `(a, b)` with `a < b` (each pair listed once).
    pub parallel: Vec<(ActivityId, ActivityId)>,
    /// Activities that open at least one trace.
    pub start_activities: Vec<ActivityId>,
    /// Activities that close at least one trace.
    pub end_activities: Vec<ActivityId>,
}

/// Build the directly-follows graph over interned traces (CONCEPT:EG-KG.mining.process-mining).
pub fn mine_dfg(traces: &[Vec<ActivityId>], n_activities: usize) -> DirectlyFollowsGraph {
    let mut counts: HashMap<(ActivityId, ActivityId), usize> = HashMap::new();
    for trace in traces {
        for w in trace.windows(2) {
            *counts.entry((w[0], w[1])).or_insert(0) += 1;
        }
    }
    let mut edges: Vec<(ActivityId, ActivityId, usize)> =
        counts.into_iter().map(|((a, b), c)| (a, b, c)).collect();
    edges.sort_unstable();
    DirectlyFollowsGraph {
        n_activities,
        edges,
    }
}

/// Derive the alpha-miner-lite footprint + start/end sets (CONCEPT:EG-KG.mining.process-mining).
pub fn alpha_lite(traces: &[Vec<ActivityId>], n_activities: usize) -> ProcessModel {
    let dfg = mine_dfg(traces, n_activities);
    let mut follows: std::collections::HashSet<(ActivityId, ActivityId)> =
        std::collections::HashSet::new();
    for &(a, b, _) in &dfg.edges {
        follows.insert((a, b));
    }

    let mut causal = Vec::new();
    let mut parallel = Vec::new();
    // Only compare pairs that appear in EITHER direction (a footprint over
    // OBSERVED activities, not the full n×n cross product of all possible ids).
    let mut seen_pairs: std::collections::BTreeSet<(ActivityId, ActivityId)> =
        std::collections::BTreeSet::new();
    for &(a, b) in &follows {
        seen_pairs.insert((a.min(b), a.max(b)));
    }
    for (lo, hi) in seen_pairs {
        let fwd = follows.contains(&(lo, hi));
        let bwd = follows.contains(&(hi, lo));
        match (fwd, bwd) {
            (true, true) => parallel.push((lo, hi)),
            (true, false) => causal.push((lo, hi)),
            (false, true) => causal.push((hi, lo)),
            (false, false) => {} // unreachable given seen_pairs construction
        }
    }
    causal.sort_unstable();
    parallel.sort_unstable();

    let mut starts: std::collections::BTreeSet<ActivityId> = std::collections::BTreeSet::new();
    let mut ends: std::collections::BTreeSet<ActivityId> = std::collections::BTreeSet::new();
    for trace in traces {
        if let Some(&first) = trace.first() {
            starts.insert(first);
        }
        if let Some(&last) = trace.last() {
            ends.insert(last);
        }
    }

    ProcessModel {
        n_activities,
        dfg_edges: dfg.edges,
        causal,
        parallel,
        start_activities: starts.into_iter().collect(),
        end_activities: ends.into_iter().collect(),
    }
}

/// String-labeled convenience: intern → mine → relabel, returning `(labels,
/// model)` so a caller can round-trip `ActivityId`s back to their original
/// strings. Reuses [`super::association::intern`] (same first-seen interning
/// scheme every other string-labeled mining family uses).
pub fn alpha_lite_labeled(traces: &[Vec<String>]) -> (Vec<String>, ProcessModel) {
    let (interned, labels) = super::association::intern(traces);
    let model = alpha_lite(&interned, labels.len());
    (labels, model)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Classic loan-process fixture: `register -> check -> decide -> {accept|reject}`.
    fn traces() -> Vec<Vec<ActivityId>> {
        vec![
            vec![0, 1, 2, 3], // register, check, decide, accept
            vec![0, 1, 2, 4], // register, check, decide, reject
            vec![0, 1, 2, 3],
        ]
    }

    #[test]
    fn dfg_counts_directly_follows_pairs() {
        let dfg = mine_dfg(&traces(), 5);
        let get = |a, b| {
            dfg.edges
                .iter()
                .find(|&&(x, y, _)| x == a && y == b)
                .map(|&(_, _, c)| c)
        };
        assert_eq!(get(0, 1), Some(3));
        assert_eq!(get(1, 2), Some(3));
        assert_eq!(get(2, 3), Some(2));
        assert_eq!(get(2, 4), Some(1));
        assert_eq!(get(3, 4), None); // never directly follow each other
    }

    #[test]
    fn alpha_lite_footprint_classifies_causal_and_choice() {
        let model = alpha_lite(&traces(), 5);
        assert!(model.causal.contains(&(0, 1)));
        assert!(model.causal.contains(&(1, 2)));
        assert!(model.causal.contains(&(2, 3)));
        assert!(model.causal.contains(&(2, 4)));
        // accept(3) and reject(4) never appear adjacent in any trace ⇒ choice
        // (neither direction in `follows`), so NOT in causal or parallel.
        assert!(!model
            .causal
            .iter()
            .any(|&(a, b)| (a, b) == (3, 4) || (a, b) == (4, 3)));
        assert!(!model
            .parallel
            .iter()
            .any(|&(a, b)| (a, b) == (3, 4) || (a, b) == (4, 3)));
        assert_eq!(model.start_activities, vec![0]);
        assert_eq!(model.end_activities, vec![3, 4]);
    }

    #[test]
    fn alpha_lite_detects_parallel_activities() {
        // b and c interleave in either order after a, both leading to d.
        let traces = vec![vec![0, 1, 2, 3], vec![0, 2, 1, 3]];
        let model = alpha_lite(&traces, 4);
        assert!(model.parallel.contains(&(1, 2)));
        assert!(!model
            .causal
            .iter()
            .any(|&(a, b)| (a, b) == (1, 2) || (a, b) == (2, 1)));
    }

    #[test]
    fn labeled_roundtrip_recovers_activity_names() {
        let traces = vec![
            vec![
                "register".to_string(),
                "check".to_string(),
                "accept".to_string(),
            ],
            vec![
                "register".to_string(),
                "check".to_string(),
                "accept".to_string(),
            ],
        ];
        let (labels, model) = alpha_lite_labeled(&traces);
        assert_eq!(labels.len(), 3);
        assert!(labels.contains(&"register".to_string()));
        assert_eq!(model.start_activities.len(), 1);
        assert_eq!(model.end_activities.len(), 1);
    }
}
