// CONCEPT:EG-KG.mining.gspan-frequent-subgraph — Frequent subgraph mining + motif counting.
//
// Pure-Rust, dependency-light, graph-NATIVE (unlike the other mining families,
// this one mines the resident graph's own topology directly — no rows/vectors
// handed in): given a labeled, directed host graph (node labels + edge
// labels), find the frequent connected subgraph PATTERNS that recur in it, and
// separately census small topological motifs. This is the graph-native
// differentiator the plan calls for.
//
//   * **gSpan-style** (CONCEPT:EG-KG.mining.gspan-frequent-subgraph) — level-wise
//     growth (the graph analog of Apriori/GSP): start from every frequent
//     single labeled edge, then repeatedly extend each surviving pattern by
//     one edge (to a new node or closing a cycle onto an existing pattern
//     node), canonicalizing each candidate by brute-force permutation
//     (patterns stay tiny — `max_edges` bounds them — so this is exact and
//     tractable) and re-counting its embeddings EXACTLY via backtracking
//     subgraph isomorphism over the whole host graph. Support here is the
//     EMBEDDING COUNT (not minimum-node-image support) — a documented
//     simplification, like the brute k-NN scan or small-N UMAP/t-SNE cuts
//     elsewhere in this family.
//   * **Motif counting** (CONCEPT:EG-KG.mining.motif-counting) — a classical,
//     label-agnostic topological census (Milo-style): open wedges (2-paths),
//     triangles (closed triads), and directed 3-cycles.
//
// This module is graph-STORE-agnostic: it works over a plain [`HostGraph`]
// (dense node indices + labeled edges). The handler
// (`src/server/handlers/mining.rs`) builds the `HostGraph` from the resident
// `GraphCore` (every node's `type`/`label` property, every edge's canonical
// `relationship` property, optionally filtered to one node label) and does
// the KG write-back (`:FrequentSubgraph`).

use std::collections::{HashMap, HashSet};

/// A dense index into a [`HostGraph`]'s node array.
pub type HostNodeId = usize;

/// A labeled, directed host graph: `labels[i]` is node `i`'s type/label;
/// `out_adj`/`in_adj` are adjacency lists of `(neighbor, edge_label)`.
#[derive(Debug, Clone, Default)]
pub struct HostGraph {
    pub labels: Vec<String>,
    pub out_adj: Vec<Vec<(HostNodeId, String)>>,
    pub in_adj: Vec<Vec<(HostNodeId, String)>>,
}

impl HostGraph {
    /// Build a host graph from `labels` (per node index) and directed
    /// `(from, to, edge_label)` triples.
    pub fn build(labels: Vec<String>, edges: &[(HostNodeId, HostNodeId, String)]) -> Self {
        let n = labels.len();
        let mut out_adj = vec![Vec::new(); n];
        let mut in_adj = vec![Vec::new(); n];
        for (from, to, lbl) in edges {
            if *from < n && *to < n {
                out_adj[*from].push((*to, lbl.clone()));
                in_adj[*to].push((*from, lbl.clone()));
            }
        }
        HostGraph {
            labels,
            out_adj,
            in_adj,
        }
    }

    pub fn node_count(&self) -> usize {
        self.labels.len()
    }

    pub fn edge_count(&self) -> usize {
        self.out_adj.iter().map(|v| v.len()).sum()
    }
}

/// A small, directed, edge-labeled pattern graph over LOCAL node indices
/// `0..node_labels.len()`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Pattern {
    pub node_labels: Vec<String>,
    /// `(from, to, edge_label)`, local indices.
    pub edges: Vec<(usize, usize, String)>,
}

/// A mined frequent pattern: its shape, embedding count/support, and the
/// deduplicated set of host nodes appearing in ANY of its embeddings (for
/// write-back linking).
#[derive(Debug, Clone)]
pub struct FrequentSubgraph {
    pub pattern: Pattern,
    pub count: usize,
    pub support: f64,
    pub member_nodes: Vec<HostNodeId>,
}

/// Convert a fractional `min_support` (0.0–1.0) into an absolute minimum
/// embedding count over `n` host edges, clamped to at least 1.
fn min_count(min_support: f64, n: usize) -> usize {
    let raw = (min_support * n as f64).ceil() as usize;
    raw.max(1)
}

// ─────────────────────────── canonicalization ───────────────────────────

/// A pattern's canonical encoding: `(node_labels, sorted (src, dst, edge_label)
/// triples)` under a fixed node relabeling.
type CanonicalForm = (Vec<String>, Vec<(usize, usize, String)>);

/// Canonical form of a pattern: the lexicographically SMALLEST edge-list
/// encoding over every relabeling (permutation) of its node indices that
/// keeps node 0 first, etc. Patterns stay tiny (`max_edges + 1` nodes at
/// most), so brute-force permutation is exact and fast. Two patterns
/// represent the "same" shape iff their canonical forms are equal.
fn canonicalize(pattern: &Pattern) -> Pattern {
    let n = pattern.node_labels.len();
    let mut perm: Vec<usize> = (0..n).collect();
    let mut best: Option<CanonicalForm> = None;

    permute(&mut perm, 0, &mut |perm| {
        let node_labels: Vec<String> = perm
            .iter()
            .map(|&i| pattern.node_labels[i].clone())
            .collect();
        // `inv[old_idx] = new_idx`
        let mut inv = vec![0usize; n];
        for (new_idx, &old_idx) in perm.iter().enumerate() {
            inv[old_idx] = new_idx;
        }
        let mut edges: Vec<(usize, usize, String)> = pattern
            .edges
            .iter()
            .map(|(a, b, lbl)| (inv[*a], inv[*b], lbl.clone()))
            .collect();
        edges.sort();
        let candidate = (node_labels, edges);
        if best.is_none() || candidate < *best.as_ref().unwrap() {
            best = Some(candidate);
        }
    });

    let (node_labels, edges) =
        best.unwrap_or_else(|| (pattern.node_labels.clone(), pattern.edges.clone()));
    Pattern { node_labels, edges }
}

/// Heap's-algorithm-style recursive permutation enumeration (small `n`, so
/// simplicity over Heap's swap-in-place trick is fine).
fn permute(perm: &mut Vec<usize>, k: usize, visit: &mut impl FnMut(&[usize])) {
    if k == perm.len() {
        visit(perm);
        return;
    }
    for i in k..perm.len() {
        perm.swap(k, i);
        permute(perm, k + 1, visit);
        perm.swap(k, i);
    }
}

// ─────────────────────────── subgraph isomorphism (embeddings) ───────────────────────────

/// Every embedding (injective node mapping, pattern-local index → host node)
/// of `pattern` in `host`. Exact backtracking search — exponential in
/// pattern size, tractable because patterns are capped by `max_edges`.
pub fn find_embeddings(host: &HostGraph, pattern: &Pattern) -> Vec<Vec<HostNodeId>> {
    let k = pattern.node_labels.len();
    let mut results = Vec::new();
    if k == 0 {
        return results;
    }
    let mut mapping = vec![usize::MAX; k];
    let mut used = vec![false; host.node_count()];
    embed_backtrack(0, &mut mapping, &mut used, host, pattern, &mut results);
    results
}

fn embed_backtrack(
    pos: usize,
    mapping: &mut Vec<usize>,
    used: &mut Vec<bool>,
    host: &HostGraph,
    pattern: &Pattern,
    out: &mut Vec<Vec<HostNodeId>>,
) {
    if pos == pattern.node_labels.len() {
        out.push(mapping.clone());
        return;
    }
    for cand in 0..host.node_count() {
        if used[cand] || host.labels[cand] != pattern.node_labels[pos] {
            continue;
        }
        mapping[pos] = cand;
        if edges_consistent(pos, mapping, host, pattern) {
            used[cand] = true;
            embed_backtrack(pos + 1, mapping, used, host, pattern, out);
            used[cand] = false;
        }
        mapping[pos] = usize::MAX;
    }
}

/// Whether every pattern edge touching an already-assigned node (up to and
/// including `pos`) is realized by a matching host edge.
fn edges_consistent(pos: usize, mapping: &[usize], host: &HostGraph, pattern: &Pattern) -> bool {
    for (a, b, lbl) in &pattern.edges {
        if *a > pos || *b > pos {
            continue;
        }
        if *a != pos && *b != pos {
            continue; // already checked when the later endpoint was assigned
        }
        let (ha, hb) = (mapping[*a], mapping[*b]);
        if ha == usize::MAX || hb == usize::MAX {
            continue;
        }
        if !host.out_adj[ha]
            .iter()
            .any(|(nb, el)| *nb == hb && el == lbl)
        {
            return false;
        }
    }
    true
}

// ─────────────────────────── gSpan-style level-wise growth ───────────────────────────

/// Frequent subgraph mining (CONCEPT:EG-KG.mining.gspan-frequent-subgraph):
/// level-wise growth from every frequent single labeled edge up to
/// `max_edges` edges, canonicalizing + exactly re-counting each candidate.
/// `min_support` is a fraction of the host's total edge count.
pub fn mine_gspan(host: &HostGraph, min_support: f64, max_edges: usize) -> Vec<FrequentSubgraph> {
    let total_edges = host.edge_count().max(1);
    let mc = min_count(min_support, total_edges);
    let mut all: Vec<FrequentSubgraph> = Vec::new();
    if max_edges == 0 {
        return all;
    }

    // Level 1: every distinct (u_label, edge_label, v_label) signature.
    let mut seen_l1: HashSet<(String, String, String)> = HashSet::new();
    let mut frontier: Vec<Pattern> = Vec::new();
    for (u, nbrs) in host.out_adj.iter().enumerate() {
        for (v, lbl) in nbrs {
            let sig = (host.labels[u].clone(), lbl.clone(), host.labels[*v].clone());
            if seen_l1.insert(sig.clone()) {
                frontier.push(Pattern {
                    node_labels: vec![sig.0, sig.2],
                    edges: vec![(0, 1, sig.1)],
                });
            }
        }
    }

    let mut level = 1;
    let mut canon_seen: HashSet<Vec<(usize, usize, String)>> = HashSet::new();
    while !frontier.is_empty() && level <= max_edges {
        let mut next_frontier: Vec<Pattern> = Vec::new();
        for pattern in &frontier {
            let canon = canonicalize(pattern);
            let key = canon.edges.clone();
            if !canon_seen.insert(key) {
                continue; // already evaluated this shape at this or an earlier level
            }
            let embeddings = find_embeddings(host, &canon);
            if embeddings.len() < mc {
                continue;
            }
            let mut members: Vec<HostNodeId> = embeddings.iter().flatten().copied().collect();
            members.sort_unstable();
            members.dedup();
            all.push(FrequentSubgraph {
                pattern: canon.clone(),
                count: embeddings.len(),
                support: embeddings.len() as f64 / total_edges as f64,
                member_nodes: members,
            });

            if level == max_edges {
                continue; // no more growth needed
            }
            for candidate in extend_candidates(host, &canon, &embeddings) {
                next_frontier.push(candidate);
            }
        }
        // Dedup the next frontier by canonical form before recursing.
        let mut dedup: Vec<Pattern> = Vec::new();
        let mut dedup_keys: HashSet<Vec<(usize, usize, String)>> = HashSet::new();
        for cand in next_frontier {
            let canon = canonicalize(&cand);
            if dedup_keys.insert(canon.edges.clone()) {
                dedup.push(canon);
            }
        }
        frontier = dedup;
        level += 1;
    }
    all
}

/// Generate one-edge extensions of `pattern` by examining every embedding's
/// mapped host nodes' neighbors (both directions): either attach a NEW
/// pattern node (labeled by the host neighbor), or close a cycle onto an
/// EXISTING pattern node if that edge isn't already present. Candidates are
/// de-duplicated by the caller via canonicalization.
fn extend_candidates(
    host: &HostGraph,
    pattern: &Pattern,
    embeddings: &[Vec<HostNodeId>],
) -> Vec<Pattern> {
    let k = pattern.node_labels.len();
    let mut out = Vec::new();
    let pattern_edges: HashSet<(usize, usize, &str)> = pattern
        .edges
        .iter()
        .map(|(from, to, label)| (*from, *to, label.as_str()))
        .collect();
    // Only need a handful of embeddings to discover every extension shape
    // that actually occurs — cap the scan for tractability on dense graphs.
    for mapping in embeddings.iter().take(64) {
        // Reverse lookup turns every incident host-neighbor membership test into
        // expected O(1), instead of re-scanning all `k` mapped nodes. A malformed
        // embedding with a duplicate host node retains `position()`'s historical
        // first-local-index behavior.
        let mut local_by_host: HashMap<HostNodeId, usize> = HashMap::with_capacity(mapping.len());
        for (local, host_node) in mapping.iter().copied().enumerate() {
            local_by_host.entry(host_node).or_insert(local);
        }
        for (local_i, &host_i) in mapping.iter().enumerate() {
            // Outgoing extensions: host_i -> neighbor.
            for (nbr, lbl) in &host.out_adj[host_i] {
                if let Some(existing_local) = local_by_host.get(nbr).copied() {
                    // Closes a cycle onto an existing pattern node.
                    if !pattern_edges.contains(&(local_i, existing_local, lbl.as_str())) {
                        let mut edges = pattern.edges.clone();
                        edges.push((local_i, existing_local, lbl.clone()));
                        out.push(Pattern {
                            node_labels: pattern.node_labels.clone(),
                            edges,
                        });
                    }
                } else {
                    // Grows to a new node.
                    let mut node_labels = pattern.node_labels.clone();
                    node_labels.push(host.labels[*nbr].clone());
                    let mut edges = pattern.edges.clone();
                    edges.push((local_i, k, lbl.clone()));
                    out.push(Pattern { node_labels, edges });
                }
            }
            // Incoming extensions: neighbor -> host_i.
            for (nbr, lbl) in &host.in_adj[host_i] {
                if let Some(existing_local) = local_by_host.get(nbr).copied() {
                    if !pattern_edges.contains(&(existing_local, local_i, lbl.as_str())) {
                        let mut edges = pattern.edges.clone();
                        edges.push((existing_local, local_i, lbl.clone()));
                        out.push(Pattern {
                            node_labels: pattern.node_labels.clone(),
                            edges,
                        });
                    }
                } else {
                    let mut node_labels = pattern.node_labels.clone();
                    node_labels.push(host.labels[*nbr].clone());
                    let mut edges = pattern.edges.clone();
                    edges.push((k, local_i, lbl.clone()));
                    out.push(Pattern { node_labels, edges });
                }
            }
        }
    }
    out
}

// ─────────────────────────── motif counting ───────────────────────────

/// Small topological motif census (CONCEPT:EG-KG.mining.motif-counting),
/// label-agnostic (Milo-style): open wedges (2-paths whose endpoints are NOT
/// connected), triangles (closed triads, any edge directions), and directed
/// 3-cycles (`a→b→c→a`).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct MotifCounts {
    pub wedge: usize,
    pub triangle: usize,
    pub directed_cycle3: usize,
}

/// Census the motifs of `host`'s UNDERLYING undirected simple graph (directed
/// edges collapsed; multi-edges deduped), plus directed 3-cycles checked
/// against the original directed edge set.
pub fn count_motifs(host: &HostGraph) -> MotifCounts {
    let n = host.node_count();
    let mut undirected: Vec<HashSet<HostNodeId>> = vec![HashSet::new(); n];
    let mut directed: HashSet<(HostNodeId, HostNodeId)> = HashSet::new();
    for (u, nbrs) in host.out_adj.iter().enumerate() {
        for (v, _) in nbrs {
            if u != *v {
                undirected[u].insert(*v);
                undirected[*v].insert(u);
                directed.insert((u, *v));
            }
        }
    }

    let mut triangle = 0usize;
    let mut wedge = 0usize;
    for u in 0..n {
        let neighbors: Vec<HostNodeId> = undirected[u].iter().copied().collect();
        for i in 0..neighbors.len() {
            for j in (i + 1)..neighbors.len() {
                let (a, b) = (neighbors[i], neighbors[j]);
                if undirected[a].contains(&b) {
                    triangle += 1; // each triangle counted once per its 3 centers -> /3 below
                } else {
                    wedge += 1;
                }
            }
        }
    }
    triangle /= 3;

    let mut directed_cycle3 = 0usize;
    for &(a, b) in &directed {
        for &(b2, c) in &directed {
            if b2 != b {
                continue;
            }
            if directed.contains(&(c, a)) && a != b && b != c && a != c {
                directed_cycle3 += 1;
            }
        }
    }
    // Each 3-cycle a→b→c→a is found starting from each of its 3 edges.
    directed_cycle3 /= 3;

    MotifCounts {
        wedge,
        triangle,
        directed_cycle3,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A tiny deterministic splitmix64 PRNG — test-fixture-only (mirrors
    /// `cluster.rs`'s hand-rolled generator), used to seed background noise
    /// edges around a planted frequent pattern.
    struct SplitMix64 {
        state: u64,
    }
    impl SplitMix64 {
        fn new(seed: u64) -> Self {
            SplitMix64 {
                state: seed.wrapping_add(0x9E37_79B9_7F4A_7C15),
            }
        }
        fn next_u64(&mut self) -> u64 {
            self.state = self.state.wrapping_add(0x9E37_79B9_7F4A_7C15);
            let mut z = self.state;
            z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
            z ^ (z >> 31)
        }
    }

    /// Plants the pattern `Concept --touches--> Capability` five times (nodes
    /// 0..10) among random-label noise edges (fixed seed 11), so it recurs far
    /// more often than any noise combination.
    fn planted_fixture() -> HostGraph {
        let mut rng = SplitMix64::new(11);
        let mut labels: Vec<String> = Vec::new();
        let mut edges: Vec<(usize, usize, String)> = Vec::new();

        // 5 planted instances of Concept --touches--> Capability.
        for i in 0..5 {
            let c = labels.len();
            labels.push("Concept".to_string());
            let cap = labels.len();
            labels.push("Capability".to_string());
            edges.push((c, cap, "touches".to_string()));
            let _ = i;
        }
        // Noise: 15 extra nodes with varied labels and random edges among them
        // (never reproducing the exact Concept--touches-->Capability shape at
        // anywhere near the same frequency).
        let noise_labels = ["Noise", "Other", "Misc"];
        let noise_start = labels.len();
        for _ in 0..15 {
            let idx = (rng.next_u64() as usize) % noise_labels.len();
            labels.push(noise_labels[idx].to_string());
        }
        let noise_rels = ["rel_a", "rel_b"];
        for _ in 0..20 {
            let u = noise_start + (rng.next_u64() as usize) % 15;
            let v = noise_start + (rng.next_u64() as usize) % 15;
            if u == v {
                continue;
            }
            let rel = noise_rels[(rng.next_u64() as usize) % noise_rels.len()].to_string();
            edges.push((u, v, rel));
        }
        HostGraph::build(labels, &edges)
    }

    #[test]
    fn gspan_recovers_planted_frequent_pattern() {
        let host = planted_fixture();
        // 5 planted edges out of ~ (5 + up to 20) total edges — well above a
        // 0.1 support threshold; noise combinations should not reach it.
        let results = mine_gspan(&host, 0.1, 1);
        // Canonicalization may orient the 2-node pattern either way (whichever
        // node-label ordering sorts lexicographically smaller) — check for the
        // SHAPE (a single "touches" edge between a Concept and a Capability
        // node) rather than a fixed local-index orientation.
        let hit = results.iter().find(|r| {
            r.pattern.node_labels.len() == 2
                && r.pattern.node_labels.contains(&"Concept".to_string())
                && r.pattern.node_labels.contains(&"Capability".to_string())
                && r.pattern.edges.len() == 1
                && r.pattern.edges[0].2 == "touches"
        });
        assert!(
            hit.is_some(),
            "planted Concept--touches-->Capability pattern not recovered, got: {:?}",
            results.iter().map(|r| &r.pattern).collect::<Vec<_>>()
        );
        assert_eq!(hit.unwrap().count, 5);
    }

    #[test]
    fn gspan_grows_beyond_one_edge_when_possible() {
        // A simple deterministic chain A--r-->B--r-->C repeated 4 times should
        // yield a frequent 2-edge pattern when max_edges=2.
        let mut labels = Vec::new();
        let mut edges = Vec::new();
        for _ in 0..4 {
            let a = labels.len();
            labels.push("A".to_string());
            let b = labels.len();
            labels.push("B".to_string());
            let c = labels.len();
            labels.push("C".to_string());
            edges.push((a, b, "r".to_string()));
            edges.push((b, c, "r".to_string()));
        }
        let host = HostGraph::build(labels, &edges);
        let results = mine_gspan(&host, 0.3, 2);
        let two_edge = results.iter().find(|r| r.pattern.edges.len() == 2);
        assert!(
            two_edge.is_some(),
            "expected a frequent 2-edge A->B->C pattern, got: {:?}",
            results.iter().map(|r| &r.pattern).collect::<Vec<_>>()
        );
        assert_eq!(two_edge.unwrap().count, 4);
    }

    #[test]
    fn find_embeddings_matches_labels_and_edges_exactly() {
        let labels = vec!["X".to_string(), "Y".to_string(), "X".to_string()];
        let edges = vec![(0, 1, "e".to_string()), (2, 1, "e".to_string())];
        let host = HostGraph::build(labels, &edges);
        let pattern = Pattern {
            node_labels: vec!["X".to_string(), "Y".to_string()],
            edges: vec![(0, 1, "e".to_string())],
        };
        let embeddings = find_embeddings(&host, &pattern);
        // Two X nodes (0 and 2) both connect to Y (1) with label "e".
        assert_eq!(embeddings.len(), 2);
        for e in &embeddings {
            assert_eq!(e[1], 1);
        }
    }

    #[test]
    fn canonicalize_is_stable_under_relabeling() {
        let p1 = Pattern {
            node_labels: vec!["A".to_string(), "B".to_string()],
            edges: vec![(0, 1, "r".to_string())],
        };
        // The "same" pattern with node order swapped and edge direction kept
        // consistent by relabeling should canonicalize identically only if it
        // truly is an isomorphic relabeling; here we just check idempotence.
        let c1 = canonicalize(&p1);
        let c2 = canonicalize(&c1);
        assert_eq!(c1, c2);
    }

    #[test]
    fn motif_counting_finds_a_planted_triangle_and_wedge() {
        // Triangle: 0-1-2-0 (undirected). Wedge: 3-4, 3-5 (no 4-5 edge).
        let labels = vec!["N".to_string(); 6];
        let edges = vec![
            (0, 1, "e".to_string()),
            (1, 2, "e".to_string()),
            (2, 0, "e".to_string()),
            (3, 4, "e".to_string()),
            (3, 5, "e".to_string()),
        ];
        let host = HostGraph::build(labels, &edges);
        let motifs = count_motifs(&host);
        assert_eq!(motifs.triangle, 1);
        assert_eq!(motifs.wedge, 1);
    }

    #[test]
    fn motif_counting_finds_a_directed_3cycle() {
        let labels = vec!["N".to_string(); 3];
        let edges = vec![
            (0, 1, "e".to_string()),
            (1, 2, "e".to_string()),
            (2, 0, "e".to_string()),
        ];
        let host = HostGraph::build(labels, &edges);
        let motifs = count_motifs(&host);
        assert_eq!(motifs.directed_cycle3, 1);
        // The same 3 edges also form one undirected triangle.
        assert_eq!(motifs.triangle, 1);
    }

    #[test]
    fn min_support_filters_infrequent_patterns() {
        let host = planted_fixture();
        let loose = mine_gspan(&host, 0.05, 1);
        let strict = mine_gspan(&host, 0.9, 1);
        assert!(strict.len() < loose.len());
    }
}
