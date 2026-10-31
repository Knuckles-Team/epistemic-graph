// EH-283 — the single quality-generic local-moving kernel shared by Leiden's
// outer (unconstrained) pass and its own refinement pass.
//
// Before this module, Leiden's outer pass reused `louvain::local_moving`
// verbatim (modularity only) while its refinement pass was a SEPARATE,
// hand-duplicated near-copy of that same move/gain/tie-break shape,
// restricted to same-parent-community neighbours
// (`refinement_local_moving`/`move_refinement_node`/`best_refinement_community`
// in the pre-EH-283 `leiden.rs`). Adding a second quality function to only
// one of those two copies would not actually dodge modularity's resolution
// limit end to end (the coarse partition the outer pass builds would still be
// modularity-shaped before refinement ever ran), and adding it to both by
// hand would have meant three near-identical loops in this crate (Louvain's,
// Leiden-outer's, Leiden-refinement's) instead of the one BUILD-CONTRACT asks
// for. `generic_local_moving` is that one loop: it takes the quality-function
// dependent DATA (`node_weight`, `resolution`/`normalizer` — see
// `super::quality`) and the search-neighbourhood restriction (`restrict`,
// `None` for the outer pass, `Some(parent)` for refinement) as PARAMETERS,
// with a single gain formula that is bit-for-bit identical to
// `louvain::best_louvain_community`'s when specialised to
// `QualityFunction::Modularity` and an unrestricted search — see
// `super::quality::QualityFunction::normalizer`'s doc for exactly why.
//
// `louvain::local_moving` itself is untouched (still `pub(crate)` and still
// used by `louvain::louvain` directly) — this module does not change Louvain,
// only stops LEIDEN from re-deriving its own separate copy of it.

use std::collections::HashMap;
use std::time::Instant;

use super::super::louvain::{visit_order, DEADLINE_CHECK_STRIDE};

/// The per-call configuration one [`generic_local_moving`] invocation needs,
/// grouped — same idiom as the pre-existing `RefinementInputs` this module
/// replaces — to keep the function's own argument count inside this repo's
/// cap (`arguments = 8`, KISS).
pub(super) struct LocalMovingParams<'a> {
    /// Per-node scalar accumulated per community — weighted degree
    /// (modularity) or population size (CPM). See
    /// `super::quality::node_weight_vector`.
    pub(super) node_weight: &'a [f64],
    /// The configured resolution/gamma, as-is (never pre-divided — see
    /// [`Self::normalizer`] field's doc (below) on why the division stays a separate,
    /// LAST operation).
    pub(super) resolution: f64,
    /// The quality function's null-model scale factor: `2m` for modularity,
    /// `1.0` for CPM. Kept separate from `resolution` (rather than
    /// pre-computing `resolution / normalizer` once) so the arithmetic order
    /// in [`best_generic_community`] is `resolution * agg[c] * weight /
    /// normalizer` — the SAME left-to-right operation order as
    /// `louvain::best_louvain_community`'s `resolution * sigma_tot[c] *
    /// degree / m2` — so the default (modularity, unrestricted) case is
    /// bit-for-bit identical to the pre-EH-283 kernel, not merely
    /// numerically close.
    pub(super) normalizer: f64,
    /// `None`: any neighbouring community is a candidate (the outer,
    /// unconstrained pass). `Some(parent)`: candidates are confined to
    /// neighbours sharing `parent[node]` (Leiden's refinement pass — the
    /// restriction that makes connectivity a structural guarantee, see the
    /// `leiden` module doc).
    pub(super) restrict: Option<&'a [usize]>,
    /// `None` ⇒ ascending visit order (fully deterministic, no RNG) — always
    /// the case for refinement, matching its pre-existing no-RNG contract.
    /// `Some(seed)` ⇒ the outer pass's seeded shuffle, exactly
    /// `louvain::LouvainConfig::seed`'s contract.
    pub(super) seed: Option<u64>,
    /// Cap on sweeps for THIS call — `LeidenConfig::max_sweeps` for the outer
    /// pass, `config_sweep_cap(n)` for refinement (unchanged from before).
    pub(super) max_sweeps: usize,
}

/// One phase of quality-gain local moving: repeatedly moves each node to the
/// neighbouring community (or its own) that most increases the active
/// quality function's gain, until a full sweep makes no move or
/// `params.max_sweeps` is reached.
///
/// Returns `(community_of_node, improved, deadline_hit)`. Community ids are
/// NOT densified (callers that need dense ids densify them — e.g.
/// `super::refine`'s own `densify_refined_partition` — since only
/// set-membership, not label value, matters to every caller in this crate:
/// `refine`'s connectivity pass compares labels for equality only, and the
/// outer pass's `improved` flag is the only thing the multilevel driver reads
/// from it).
pub(super) fn generic_local_moving(
    adj: &[Vec<(usize, f64)>],
    params: &LocalMovingParams<'_>,
    deadline: Instant,
) -> (Vec<usize>, bool, bool) {
    let n = adj.len();
    let mut comm: Vec<usize> = (0..n).collect();
    let mut agg: Vec<f64> = params.node_weight.to_vec();
    let order = visit_order(n, params.seed);
    let mut improved = false;
    let mut deadline_hit = false;

    'sweeps: for _ in 0..params.max_sweeps {
        let mut moved = false;
        for (visited, &node) in order.iter().enumerate() {
            if visited % DEADLINE_CHECK_STRIDE == 0 && Instant::now() >= deadline {
                deadline_hit = true;
                break 'sweeps;
            }
            if move_generic_node(adj, params, &mut comm, &mut agg, node) {
                moved = true;
                improved = true;
            }
        }
        if !moved {
            break;
        }
    }
    (comm, improved, deadline_hit)
}

fn move_generic_node(
    adj: &[Vec<(usize, f64)>],
    params: &LocalMovingParams<'_>,
    comm: &mut [usize],
    agg: &mut [f64],
    node: usize,
) -> bool {
    let current = comm[node];
    let weights = generic_neighbor_weights(adj, params.restrict, comm, node);
    let weight_of_node = params.node_weight[node];
    agg[current] -= weight_of_node;
    let best = best_generic_community(&weights, current, agg, weight_of_node, params);
    agg[best] += weight_of_node;
    comm[node] = best;
    best != current
}

/// This node's neighbour-community edge-weight totals, restricted to
/// same-`restrict`-group neighbours when `restrict` is `Some` (refinement) or
/// every neighbour when `None` (the outer pass) — the one place the two
/// passes actually differ.
fn generic_neighbor_weights(
    adj: &[Vec<(usize, f64)>],
    restrict: Option<&[usize]>,
    comm: &[usize],
    node: usize,
) -> HashMap<usize, f64> {
    let mut weights = HashMap::new();
    for &(neighbor, weight) in &adj[node] {
        if neighbor == node || !same_group(restrict, node, neighbor) {
            continue;
        }
        *weights.entry(comm[neighbor]).or_insert(0.0) += weight;
    }
    weights
}

/// `true` when `node` and `neighbor` may move together under `restrict`:
/// always, when unrestricted (the outer pass); only within the same
/// `restrict`-group, otherwise (refinement — see the `leiden` module doc for
/// why this restriction is what makes connectivity a guarantee).
fn same_group(restrict: Option<&[usize]>, node: usize, neighbor: usize) -> bool {
    match restrict {
        Some(group) => group[neighbor] == group[node],
        None => true,
    }
}

/// The neighbouring community (or `current`) with the highest quality gain.
/// Candidates are sorted before comparison (unlike
/// `louvain::best_louvain_community`, which relies on an order-independence
/// argument instead) so the deterministic outcome is transparent by
/// construction rather than by a subtler proof — matching this crate's own
/// refinement-phase style, not a new convention.
fn best_generic_community(
    weights: &HashMap<usize, f64>,
    current: usize,
    agg: &[f64],
    weight_of_node: f64,
    params: &LocalMovingParams<'_>,
) -> usize {
    let own_weight = *weights.get(&current).unwrap_or(&0.0);
    let mut best = current;
    let mut best_gain = own_weight - null_model_penalty(agg[current], weight_of_node, params);
    let mut candidates: Vec<usize> = weights.keys().copied().collect();
    candidates.sort_unstable();
    for candidate in candidates {
        if candidate == current {
            continue;
        }
        let weight = weights[&candidate];
        let gain = weight - null_model_penalty(agg[candidate], weight_of_node, params);
        if gain > best_gain + 1e-12 || (gain > best_gain - 1e-12 && candidate < best) {
            best_gain = gain;
            best = candidate;
        }
    }
    best
}

/// `resolution * agg_of_candidate * weight_of_node / normalizer` — see
/// [`LocalMovingParams`]'s `normalizer` field doc for why this exact left-to-right
/// order (not `(resolution / normalizer) * agg * weight`) is load-bearing for
/// bit-for-bit parity with the pre-EH-283 kernel.
fn null_model_penalty(
    agg_of_candidate: f64,
    weight_of_node: f64,
    params: &LocalMovingParams<'_>,
) -> f64 {
    params.resolution * agg_of_candidate * weight_of_node / params.normalizer
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn far_future() -> Instant {
        Instant::now() + Duration::from_secs(60)
    }

    #[test]
    fn shared_visit_order_is_ascending_with_no_seed() {
        assert_eq!(visit_order(5, None), vec![0, 1, 2, 3, 4]);
    }

    #[test]
    fn shared_visit_order_is_reproducible_for_a_seed() {
        let a = visit_order(10, Some(7));
        let b = visit_order(10, Some(7));
        assert_eq!(a, b);
        let mut sorted = a.clone();
        sorted.sort_unstable();
        assert_eq!(sorted, (0..10).collect::<Vec<_>>(), "still a permutation");
    }

    /// Unrestricted, unweighted (weight = 1.0 per edge) fixture mirroring this
    /// crate's own PROVEN case (`leiden::tests::
    /// leiden_finds_two_communities_in_two_cliques_matching_louvain`): two
    /// 4-cliques joined by one bridge edge reliably split under modularity.
    /// (Plain triangles are deliberately avoided here — 3-node communities sit
    /// right at modularity's own resolution limit, `~sqrt(2m)`, so whether they
    /// split or merge is not something to assert from hand-derivation; the
    /// 4-clique shape is the one this repo already has a passing end-to-end
    /// test for.) Exercises `restrict = None` end to end.
    #[test]
    fn unrestricted_modularity_partitions_two_bridged_four_cliques() {
        let clique1 = [0usize, 1, 2, 3];
        let clique2 = [4usize, 5, 6, 7];
        let mut adj: Vec<Vec<(usize, f64)>> = vec![Vec::new(); 8];
        for clique in [clique1, clique2] {
            for i in 0..clique.len() {
                for j in 0..clique.len() {
                    if i != j {
                        adj[clique[i]].push((clique[j], 1.0));
                    }
                }
            }
        }
        adj[3].push((4, 1.0));
        adj[4].push((3, 1.0));

        let node_weight: Vec<f64> = adj
            .iter()
            .map(|row| row.iter().map(|(_, w)| *w).sum())
            .collect();
        let m2: f64 = node_weight.iter().sum();
        let params = LocalMovingParams {
            node_weight: &node_weight,
            resolution: 1.0,
            normalizer: m2,
            restrict: None,
            seed: None,
            max_sweeps: 100,
        };
        let (comm, improved, deadline_hit) = generic_local_moving(&adj, &params, far_future());
        assert!(improved);
        assert!(!deadline_hit);
        for &n in &clique1[1..] {
            assert_eq!(comm[n], comm[clique1[0]], "clique1 must stay together");
        }
        for &n in &clique2[1..] {
            assert_eq!(comm[n], comm[clique2[0]], "clique2 must stay together");
        }
        assert_ne!(
            comm[clique1[0]], comm[clique2[0]],
            "the two cliques must not merge"
        );
    }

    /// `restrict = Some(parent)` must never move a node into a community
    /// containing a neighbour OUTSIDE its own parent group, even when that
    /// neighbour would otherwise offer the best gain.
    #[test]
    fn restricted_local_moving_never_crosses_the_parent_boundary() {
        // A single node (0) with two neighbours in different parent groups.
        // Node 1 offers more weight, but is in a DIFFERENT parent group than 0.
        let adj: Vec<Vec<(usize, f64)>> =
            vec![vec![(1, 10.0), (2, 1.0)], vec![(0, 10.0)], vec![(0, 1.0)]];
        let node_weight = vec![1.0, 1.0, 1.0];
        let parent = [0usize, 1, 0]; // node 0 and node 2 share a parent; node 1 does not
        let params = LocalMovingParams {
            node_weight: &node_weight,
            resolution: 1.0,
            normalizer: 3.0,
            restrict: Some(&parent[..]),
            seed: None,
            max_sweeps: 100,
        };
        let (comm, _improved, _hit) = generic_local_moving(&adj, &params, far_future());
        assert_eq!(
            comm[0], comm[2],
            "node 0 may only ever join node 2's community"
        );
        assert_ne!(
            comm[0], comm[1],
            "node 0 must never cross into node 1's out-of-group community"
        );
    }
}
