// CONCEPT:EG-144 — Node similarity: Jaccard + cosine over neighbour sets.
// Neo4j GDS `gds.nodeSimilarity` parity.

use super::graph::AdjacencyGraph;
use std::collections::HashMap;
use std::hash::Hash;

/// A similarity edge between two nodes. CONCEPT:EG-144
#[derive(Debug, Clone)]
pub struct SimilarityPair<N> {
    /// First node (the smaller-id endpoint).
    pub a: N,
    /// Second node.
    pub b: N,
    /// Similarity score in `[0, 1]`.
    pub score: f64,
}

/// Which relationship set forms each node's "neighbour" vector.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Direction {
    /// Outgoing neighbours (GDS default).
    Out,
    /// Incoming neighbours.
    In,
    /// Union of both directions (undirected view).
    Undirected,
}

/// Sorted `(neighbor, weight)` list for a node under the chosen direction; the
/// undirected view sums both directions' weights.
fn neighbor_vec<N>(graph: &AdjacencyGraph<N>, i: usize, dir: Direction) -> Vec<(usize, f64)>
where
    N: Clone + Eq + Hash + Ord,
{
    match dir {
        Direction::Out => graph.out_edges(i).to_vec(),
        Direction::In => graph.in_edges(i).to_vec(),
        Direction::Undirected => {
            let mut m: HashMap<usize, f64> = HashMap::new();
            for &(t, w) in graph.out_edges(i) {
                *m.entry(t).or_insert(0.0) += w;
            }
            for &(s, w) in graph.in_edges(i) {
                *m.entry(s).or_insert(0.0) += w;
            }
            let mut v: Vec<(usize, f64)> = m.into_iter().collect();
            v.sort_unstable_by_key(|(k, _)| *k);
            v
        }
    }
}

/// **Jaccard** similarity of two nodes' neighbour *sets* (weights ignored):
/// `|N(a) ∩ N(b)| / |N(a) ∪ N(b)|`. Two nodes with no neighbours score 0.
///
/// Complexity: `O(deg(a) + deg(b))`. CONCEPT:EG-144
pub fn jaccard_similarity<N>(graph: &AdjacencyGraph<N>, a: usize, b: usize, dir: Direction) -> f64
where
    N: Clone + Eq + Hash + Ord,
{
    let na: Vec<usize> = neighbor_vec(graph, a, dir)
        .into_iter()
        .map(|(k, _)| k)
        .collect();
    let nb: Vec<usize> = neighbor_vec(graph, b, dir)
        .into_iter()
        .map(|(k, _)| k)
        .collect();
    let (inter, union) = set_overlap(&na, &nb);
    if union == 0 {
        0.0
    } else {
        inter as f64 / union as f64
    }
}

/// **Cosine** similarity of two nodes' weighted neighbour *vectors*:
/// `(a · b) / (‖a‖ ‖b‖)` over the shared neighbour space. For unit weights this
/// reduces to `|N(a) ∩ N(b)| / √(|N(a)|·|N(b)|)`.
///
/// Complexity: `O(deg(a) + deg(b))`. CONCEPT:EG-144
pub fn cosine_similarity<N>(graph: &AdjacencyGraph<N>, a: usize, b: usize, dir: Direction) -> f64
where
    N: Clone + Eq + Hash + Ord,
{
    let va = neighbor_vec(graph, a, dir);
    let vb = neighbor_vec(graph, b, dir);
    let map_a: HashMap<usize, f64> = va.iter().copied().collect();
    let mut dot = 0.0;
    for &(k, w) in &vb {
        if let Some(&wa) = map_a.get(&k) {
            dot += wa * w;
        }
    }
    let norm_a: f64 = va.iter().map(|(_, w)| w * w).sum::<f64>().sqrt();
    let norm_b: f64 = vb.iter().map(|(_, w)| w * w).sum::<f64>().sqrt();
    if norm_a == 0.0 || norm_b == 0.0 {
        0.0
    } else {
        dot / (norm_a * norm_b)
    }
}

/// Which metric an all-pairs sweep uses.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Metric {
    /// Set-based Jaccard.
    Jaccard,
    /// Weighted cosine.
    Cosine,
}

/// All-pairs node similarity above a cutoff. Returns each unordered pair
/// `(a < b)` whose score `> cutoff`, sorted by descending score then ascending
/// node ids (deterministic).
///
/// Complexity: `O(V² · d̄)` naïvely. CONCEPT:EG-144
pub fn all_pairs_similarity<N>(
    graph: &AdjacencyGraph<N>,
    metric: Metric,
    dir: Direction,
    cutoff: f64,
) -> Vec<SimilarityPair<N>>
where
    N: Clone + Eq + Hash + Ord,
{
    let n = graph.node_count();
    let mut out: Vec<(usize, usize, f64)> = Vec::new();
    for a in 0..n {
        for b in (a + 1)..n {
            let s = match metric {
                Metric::Jaccard => jaccard_similarity(graph, a, b, dir),
                Metric::Cosine => cosine_similarity(graph, a, b, dir),
            };
            if s > cutoff {
                out.push((a, b, s));
            }
        }
    }
    // Descending score, then ascending (a, b) for stable ordering.
    out.sort_by(|x, y| {
        y.2.partial_cmp(&x.2)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| x.0.cmp(&y.0))
            .then_with(|| x.1.cmp(&y.1))
    });
    out.into_iter()
        .map(|(a, b, score)| SimilarityPair {
            a: graph.node_at(a).clone(),
            b: graph.node_at(b).clone(),
            score,
        })
        .collect()
}

/// Intersection + union sizes of two sorted, de-duplicated index lists.
fn set_overlap(a: &[usize], b: &[usize]) -> (usize, usize) {
    let (mut i, mut j) = (0, 0);
    let (mut inter, mut union) = (0usize, 0usize);
    while i < a.len() && j < b.len() {
        match a[i].cmp(&b[j]) {
            std::cmp::Ordering::Less => {
                union += 1;
                i += 1;
            }
            std::cmp::Ordering::Greater => {
                union += 1;
                j += 1;
            }
            std::cmp::Ordering::Equal => {
                inter += 1;
                union += 1;
                i += 1;
                j += 1;
            }
        }
    }
    union += (a.len() - i) + (b.len() - j);
    (inter, union)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn eg144_jaccard_overlapping_neighbors() {
        // a→{x,y,z}, b→{y,z,w}. Intersection {y,z}=2, union {w,x,y,z}=4 ⇒ 0.5.
        let g = AdjacencyGraph::from_edges([
            ("a", "x", 1.0),
            ("a", "y", 1.0),
            ("a", "z", 1.0),
            ("b", "y", 1.0),
            ("b", "z", 1.0),
            ("b", "w", 1.0),
        ]);
        let (a, b) = (g.index_of(&"a").unwrap(), g.index_of(&"b").unwrap());
        assert!((jaccard_similarity(&g, a, b, Direction::Out) - 0.5).abs() < 1e-9);
    }

    #[test]
    fn eg144_jaccard_identical_and_disjoint() {
        let g = AdjacencyGraph::from_edges([
            ("a", "x", 1.0),
            ("a", "y", 1.0),
            ("b", "x", 1.0),
            ("b", "y", 1.0),
            ("c", "p", 1.0),
            ("c", "q", 1.0),
        ]);
        let (a, b, c) = (
            g.index_of(&"a").unwrap(),
            g.index_of(&"b").unwrap(),
            g.index_of(&"c").unwrap(),
        );
        assert!((jaccard_similarity(&g, a, b, Direction::Out) - 1.0).abs() < 1e-9);
        assert!((jaccard_similarity(&g, a, c, Direction::Out) - 0.0).abs() < 1e-9);
    }

    #[test]
    fn eg144_cosine_unit_weights_matches_formula() {
        // a→{x,y,z}, b→{y,z,w}: inter=2, |a|=|b|=3 ⇒ 2/√(3·3)=2/3.
        let g = AdjacencyGraph::from_edges([
            ("a", "x", 1.0),
            ("a", "y", 1.0),
            ("a", "z", 1.0),
            ("b", "y", 1.0),
            ("b", "z", 1.0),
            ("b", "w", 1.0),
        ]);
        let (a, b) = (g.index_of(&"a").unwrap(), g.index_of(&"b").unwrap());
        assert!((cosine_similarity(&g, a, b, Direction::Out) - 2.0 / 3.0).abs() < 1e-9);
    }

    #[test]
    fn eg144_cosine_respects_weights() {
        // Same target, proportional weight vectors ⇒ cosine 1.0.
        let g = AdjacencyGraph::from_edges([
            ("a", "x", 1.0),
            ("a", "y", 2.0),
            ("b", "x", 2.0),
            ("b", "y", 4.0),
        ]);
        let (a, b) = (g.index_of(&"a").unwrap(), g.index_of(&"b").unwrap());
        assert!((cosine_similarity(&g, a, b, Direction::Out) - 1.0).abs() < 1e-9);
    }

    #[test]
    fn eg144_all_pairs_similarity_ranked() {
        let g = AdjacencyGraph::from_edges([
            ("a", "x", 1.0),
            ("a", "y", 1.0),
            ("b", "x", 1.0),
            ("b", "y", 1.0),
            ("c", "y", 1.0),
        ]);
        let pairs = all_pairs_similarity(&g, Metric::Jaccard, Direction::Out, 0.0);
        // a & b share {x,y} ⇒ top pair with score 1.0.
        assert_eq!(pairs[0].a, "a");
        assert_eq!(pairs[0].b, "b");
        assert!((pairs[0].score - 1.0).abs() < 1e-9);
    }
}
