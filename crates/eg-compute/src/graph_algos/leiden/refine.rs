// Leiden's refinement phase — see the `leiden` module doc for why this is
// what makes "every returned community induces a CONNECTED subgraph" a
// structural guarantee rather than a typical outcome.

use std::collections::HashMap;
use std::time::Instant;

use super::local_moving::{generic_local_moving, LocalMovingParams};

/// A bounded sweep cap for the refinement phase's own local-moving —
/// proportional to graph size like Louvain's `max_sweeps`, but derived
/// locally so `refine` does not need the full `LeidenConfig` threaded
/// through it.
fn config_sweep_cap(n: usize) -> usize {
    n.clamp(1, 100)
}

/// The refinement phase. Starting from singletons within each `p`-community,
/// runs the active quality function's local-moving restricted to same-`p`
/// -community neighbours (`super::local_moving::generic_local_moving` with
/// `restrict = Some(p)`), then — the correctness-critical step — recomputes
/// the connected components of every resulting group's induced subgraph and
/// returns THOSE as the final refined communities. See the `leiden` module
/// doc for why this makes connectivity a structural guarantee rather than a
/// typical outcome.
///
/// `node_weight`/`normalizer` are the SAME values the caller already computed
/// for the outer pass at this level (see `super::quality::node_weight_vector`
/// and `super::quality::QualityFunction::normalizer`) — both passes operate
/// over the same set of current-level nodes, so there is nothing level- or
/// pass-specific left to recompute here.
///
/// Returns `(refined_partition, deadline_hit)`.
pub(super) fn refine(
    adj: &[Vec<(usize, f64)>],
    p: &[usize],
    resolution: f64,
    node_weight: &[f64],
    normalizer: f64,
    deadline: Instant,
) -> (Vec<usize>, bool) {
    let params = LocalMovingParams {
        node_weight,
        resolution,
        normalizer,
        restrict: Some(p),
        // No RNG in the refinement phase itself (ascending index order,
        // deterministic tie-break) — see the `leiden` module doc.
        seed: None,
        max_sweeps: config_sweep_cap(adj.len()),
    };
    let (comm, _improved, deadline_hit) = generic_local_moving(adj, &params, deadline);
    // The component split and densify are single `O(V + E)` passes with no
    // convergence loop, so they need no deadline of their own — and they are
    // what makes the returned partition connectivity-guaranteed, so they must
    // run even on a truncated `comm`.
    let roots = refinement_components(adj, &comm);
    (densify_refined_partition(&comm, &roots), deadline_hit)
}

fn refinement_components(adj: &[Vec<(usize, f64)>], comm: &[usize]) -> Vec<usize> {
    let n = adj.len();
    let mut parent: Vec<usize> = (0..n).collect();
    for node in 0..n {
        for &(neighbor, _weight) in &adj[node] {
            if neighbor != node && comm[node] == comm[neighbor] {
                uf_union(&mut parent, node, neighbor);
            }
        }
    }
    (0..n).map(|node| uf_find(&mut parent, node)).collect()
}

fn densify_refined_partition(comm: &[usize], roots: &[usize]) -> Vec<usize> {
    let mut relabel = HashMap::new();
    let mut out = Vec::with_capacity(comm.len());
    for (&community, &root) in comm.iter().zip(roots) {
        let key = (community, root);
        let next = relabel.len();
        out.push(*relabel.entry(key).or_insert(next));
    }
    out
}

fn uf_find(parent: &mut [usize], mut x: usize) -> usize {
    while parent[x] != x {
        parent[x] = parent[parent[x]]; // path halving
        x = parent[x];
    }
    x
}

fn uf_union(parent: &mut [usize], a: usize, b: usize) {
    let (ra, rb) = (uf_find(parent, a), uf_find(parent, b));
    if ra != rb {
        // Attach the larger root under the smaller for a deterministic result
        // independent of call order (both a<b and a>b callers converge).
        if ra < rb {
            parent[rb] = ra;
        } else {
            parent[ra] = rb;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn far_future() -> Instant {
        Instant::now() + std::time::Duration::from_secs(60)
    }

    /// Two triangles sharing one node's `p`-community membership, but only
    /// connected through a NON-triangle path once the shared parent forces
    /// them together — refinement must split back into the genuinely
    /// connected pieces regardless of what `p` claims.
    #[test]
    fn refine_splits_a_parent_group_with_a_disconnected_induced_subgraph() {
        // adj: two disjoint edges (0-1) and (2-3), no path between the pairs.
        let adj: Vec<Vec<(usize, f64)>> = vec![
            vec![(1, 1.0)],
            vec![(0, 1.0)],
            vec![(3, 1.0)],
            vec![(2, 1.0)],
        ];
        let node_weight = vec![1.0, 1.0, 1.0, 1.0];
        // `p` claims all four nodes are ONE community, despite {0,1} and
        // {2,3} sharing no edge at all.
        let p = [0usize, 0, 0, 0];
        let (refined, deadline_hit) = refine(&adj, &p, 1.0, &node_weight, 4.0, far_future());
        assert!(!deadline_hit);
        assert_eq!(refined[0], refined[1], "0 and 1 are connected");
        assert_eq!(refined[2], refined[3], "2 and 3 are connected");
        assert_ne!(
            refined[0], refined[2],
            "refinement must split the disconnected halves of a bogus parent group"
        );
    }

    #[test]
    fn refinement_components_finds_two_components_in_a_disconnected_pair_of_edges() {
        let adj: Vec<Vec<(usize, f64)>> = vec![
            vec![(1, 1.0)],
            vec![(0, 1.0)],
            vec![(3, 1.0)],
            vec![(2, 1.0)],
        ];
        let comm = [0usize, 0, 0, 0];
        let roots = refinement_components(&adj, &comm);
        assert_eq!(roots[0], roots[1]);
        assert_eq!(roots[2], roots[3]);
        assert_ne!(roots[0], roots[2]);
    }

    #[test]
    fn uf_union_is_deterministic_regardless_of_argument_order() {
        let mut a = vec![0usize, 1, 2];
        uf_union(&mut a, 2, 0);
        let mut b = vec![0usize, 1, 2];
        uf_union(&mut b, 0, 2);
        assert_eq!(uf_find(&mut a, 2), uf_find(&mut b, 2));
        assert_eq!(uf_find(&mut a, 0), uf_find(&mut b, 0));
    }
}
