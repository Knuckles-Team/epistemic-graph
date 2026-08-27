//! Characterization tests for `label_propagation` (CX-EG-02,
//! `crates/eg-compute/src/graph_algos/label_propagation.rs`).
//!
//! Pins observed behaviour ahead of a pure extract-method refactor (CCN 12
//! -> target <=10): the `config.weighted` branch actually changing which
//! label wins a vote, the ascending-label-id tie-break under a genuine tie,
//! the no-neighbours `continue` for an isolated node mid-graph, and the
//! `max_iterations` sweep cap stopping propagation before full convergence.

use eg_compute::graph_algos::{label_propagation, AdjacencyGraph, LabelPropagationConfig};

/// Sorted community membership as plain strings, for order-independent
/// comparison (communities and their internal member order are already a
/// documented sorted convention, but we don't re-assert that convention
/// here -- only membership).
fn communities_as_sets<'a>(
    result: &eg_compute::graph_algos::LabelPropagationResult<&'a str>,
) -> Vec<Vec<&'a str>> {
    let mut v: Vec<Vec<&str>> = result.communities.clone();
    for c in v.iter_mut() {
        c.sort_unstable();
    }
    v.sort();
    v
}

#[test]
fn weighted_vote_picks_the_heavy_neighbor_unweighted_vote_ties_toward_smallest_label() {
    // a -- b (weight 1.0, b has NO other edges: b freely follows whatever a
    // decides). a -- e (weight 10.0), e -- e2 (weight 100.0, a strong
    // MUTUAL anchor pair protecting e from ever following a). Processing
    // order (ascending sorted-id index): a, b, e, e2.
    //
    // Weighted, one sweep: a's vote is dominated by e (10.0 > 1.0), so a
    // adopts e's original label. b (only neighbour: a) then copies a's
    // just-updated label. e is next: its vote is now dominated by e2
    // (100.0 > 10.0 from a), so e defects BACK to e2's original label --
    // leaving a and b holding an orphaned intermediate label that nobody
    // else carries. Net: {a, b} and {e, e2} as two SEPARATE communities.
    //
    // Unweighted, one sweep: every edge counts as a flat 1.0 vote, so a's
    // a-b vs a-e choice is a TIE (smallest original label wins: b's index
    // < e's index, so a adopts b's label -- trivially b's own value, no
    // visible change to b). e's turn: a's vote (1.0) now ties EQUALLY
    // against e2's vote (1.0) -- no more 100x anchor advantage -- so e
    // ALSO breaks the tie toward the smaller label (a/b's cluster, whose
    // label is numerically smaller than e2's original), joining a and b
    // instead of staying with e2. e2 then follows e. Net: {a, b, e, e2}
    // as ONE community.
    //
    // This is the actual mechanism the `config.weighted` branch is FOR:
    // an edge-weight-driven anchor beats a topology-only vote only when
    // weighting is honoured.
    let g = AdjacencyGraph::from_edges([
        ("a", "b", 1.0),
        ("a", "e", 10.0),
        ("e", "e2", 100.0),
        ("e2", "e", 100.0),
    ]);
    let weighted = label_propagation(
        &g,
        &LabelPropagationConfig {
            max_iterations: 1,
            weighted: true,
        },
    );
    let unweighted = label_propagation(
        &g,
        &LabelPropagationConfig {
            max_iterations: 1,
            weighted: false,
        },
    );
    assert_eq!(
        communities_as_sets(&weighted),
        vec![vec!["a", "b"], vec!["e", "e2"]],
        "weighted: e's strong anchor to e2 must beat a's weaker pull, got {:?}",
        communities_as_sets(&weighted)
    );
    assert_eq!(
        communities_as_sets(&unweighted),
        vec![vec!["a", "b", "e", "e2"]],
        "unweighted: stripping edge weight must let the smallest-label tie-break \
         pull everyone into one community, got {:?}",
        communities_as_sets(&unweighted)
    );
}

#[test]
fn isolated_node_mid_graph_keeps_its_own_singleton_community() {
    // {a,b} form a connected pair; "z" has NO edges at all, alongside them.
    // The no-neighbours `continue` branch must fire for z without
    // disturbing a/b's propagation.
    let g = AdjacencyGraph::from_adjacency([
        ("a", vec![("b", 1.0)]),
        ("b", vec![("a", 1.0)]),
        ("z", Vec::<(&str, f64)>::new()),
    ]);
    let result = label_propagation(&g, &LabelPropagationConfig::default());
    let z_comm = result
        .communities
        .iter()
        .find(|c| c.contains(&"z"))
        .expect("z's community");
    assert_eq!(z_comm, &vec!["z"], "isolated node must stay a singleton");
}

#[test]
fn max_iterations_one_stops_before_multi_hop_propagation_completes() {
    // Two dominant, self-stable cliques (hub1+f1..f3, hub2+g1..g3, weight
    // 5.0 followers so each hub never budges) joined by an UNPROTECTED
    // relay chain hub1 -- n4 -- n3 -- n2 -- n1 -- hub2 (weight 1.0 links).
    // The relay node NAMES are deliberately the REVERSE of their physical
    // chain position (n1 is physically adjacent to hub2, n4 to hub1), so
    // sorted-id sweep order (f1,f2,f3,g1,g2,g3,hub1,hub2,n1,n2,n3,n4)
    // processes the chain in the SAME direction information needs to
    // travel from hub2's end toward hub1's end -- letting hub2's
    // influence cascade through n1->n2->n3 WITHIN one sweep (each sees
    // its predecessor's already-updated value), while n4 (last in
    // processing order, adjacent to hub1) still ties against hub1's
    // original label at the OTHER end and keeps hub1's side. One sweep
    // therefore leaves the n3/n4 boundary un-settled; further sweeps keep
    // shifting it. OBSERVED (this IS the pin, not an assumption): the
    // exact community sets at max_iterations=1 vs a converged run.
    let g = AdjacencyGraph::from_edges([
        ("hub1", "f1", 5.0),
        ("f1", "hub1", 5.0),
        ("hub1", "f2", 5.0),
        ("f2", "hub1", 5.0),
        ("hub1", "f3", 5.0),
        ("f3", "hub1", 5.0),
        ("hub2", "g1", 5.0),
        ("g1", "hub2", 5.0),
        ("hub2", "g2", 5.0),
        ("g2", "hub2", 5.0),
        ("hub2", "g3", 5.0),
        ("g3", "hub2", 5.0),
        ("hub1", "n4", 1.0),
        ("n4", "hub1", 1.0),
        ("n4", "n3", 1.0),
        ("n3", "n4", 1.0),
        ("n3", "n2", 1.0),
        ("n2", "n3", 1.0),
        ("n2", "n1", 1.0),
        ("n1", "n2", 1.0),
        ("n1", "hub2", 1.0),
        ("hub2", "n1", 1.0),
    ]);
    let one_sweep = label_propagation(
        &g,
        &LabelPropagationConfig {
            max_iterations: 1,
            weighted: true,
        },
    );
    let converged = label_propagation(
        &g,
        &LabelPropagationConfig {
            max_iterations: 50,
            weighted: true,
        },
    );
    assert_eq!(
        communities_as_sets(&one_sweep),
        vec![
            vec!["f1", "f2", "f3", "hub1", "n4"],
            vec!["g1", "g2", "g3", "hub2", "n1", "n2", "n3"],
        ],
        "got {:?}",
        communities_as_sets(&one_sweep)
    );
    assert_ne!(
        communities_as_sets(&one_sweep),
        communities_as_sets(&converged),
        "max_iterations=1 must yield a LESS converged (different) partition \
         than a run given enough sweeps to stabilize; one_sweep={:?} converged={:?}",
        communities_as_sets(&one_sweep),
        communities_as_sets(&converged)
    );
}
