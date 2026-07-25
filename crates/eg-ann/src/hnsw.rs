//! HnswIndex — a Hierarchical Navigable Small World graph index (CONCEPT:EG-KG.retrieval.hnsw-vector-index).
//!
//! A pure-Rust, additive ANN index that complements the [`IvfPq`](crate::IvfPq)
//! (EG-069) and exact [`FlatIndex`](crate::FlatIndex) (EG-297) indices already in
//! this crate. HNSW builds a multi-layer proximity graph (Malkov & Yashunin, 2016):
//! upper layers are sparse "express lanes" for coarse navigation, layer 0 holds
//! every point. A search greedily descends the layers, then does a beam search
//! (parameterised by `ef`) at layer 0 to collect the top-k.
//!
//! Design choices that make this the crate's deterministic, persistable HNSW:
//!
//!   * **True-distance results** — every graph hop and the returned
//!     [`SearchResult::distance`] use the EXACT [`Metric`] distance on full f32
//!     vectors (L2 / cosine / inner-product), same "smaller = nearer" ascending
//!     convention as [`FlatIndex`], [`IvfPq`], and `merge_topk`. Only the *graph
//!     traversal* is approximate; the distances reported are real.
//!   * **Deterministic** — level assignment is a pure hash of `(id, seed)` (no clock
//!     RNG), and every heap / neighbour selection breaks ties by ascending id. Two
//!     builds with the same `seed` and the same insertion order are bit-identical,
//!     and results sort exactly like the rest of the crate.
//!   * **serde-serializable** — the whole graph (nodes, per-layer adjacency, entry
//!     point) round-trips via the crate's strict versioned postcard codec for a no-rebuild reload, mirroring
//!     [`FlatIndex`]'s persistence story.
//!
//! Recall is validated against [`FlatIndex`] ground truth with
//! [`recall_at_k`](crate::recall_at_k) in the crate tests (recall@10 ≥ 0.9 on a
//! random set at a reasonable `ef`).

use crate::flat::Metric;
use crate::ivfpq::SearchResult;
use serde::{Deserialize, Serialize};
use std::cmp::{Ordering, Reverse};
use std::collections::{BinaryHeap, HashSet};

/// One graph node: its external id, its full vector, and its per-layer adjacency
/// (`neighbors[l]` = internal node indices linked at layer `l`; `neighbors.len()`
/// is the node's top layer + 1).
#[derive(Clone, Debug, Serialize, Deserialize)]
struct Node {
    id: u64,
    vector: Vec<f32>,
    neighbors: Vec<Vec<usize>>,
}

/// A scored candidate during traversal: the EXACT distance to the query, the
/// internal node index, and the external id used purely for deterministic
/// tie-breaking. Ordered by `(distance, id, node)` under a TOTAL order (NaN sorts last),
/// so a [`BinaryHeap`] max-pops the farthest / largest-id element and `Reverse`
/// min-pops the nearest / smallest-id one.
#[derive(Clone, Copy, Debug)]
struct Cand {
    dist: f32,
    node: usize,
    id: u64,
}

impl PartialEq for Cand {
    fn eq(&self, other: &Self) -> bool {
        self.dist.to_bits() == other.dist.to_bits()
            && self.id == other.id
            && self.node == other.node
    }
}
impl Eq for Cand {}
impl Ord for Cand {
    fn cmp(&self, other: &Self) -> Ordering {
        // total order on the float (NaN last), then ascending id for determinism.
        match (self.dist.is_nan(), other.dist.is_nan()) {
            (true, false) => Ordering::Greater,
            (false, true) => Ordering::Less,
            (true, true) => self.dist.to_bits().cmp(&other.dist.to_bits()),
            (false, false) => self.dist.total_cmp(&other.dist),
        }
        .then_with(|| self.id.cmp(&other.id))
        .then_with(|| self.node.cmp(&other.node))
    }
}
impl PartialOrd for Cand {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// A persistable HNSW graph index (CONCEPT:EG-KG.retrieval.hnsw-vector-index).
///
/// Build it with [`HnswIndex::new`], stream points in with [`HnswIndex::insert`],
/// and query with [`HnswIndex::search`]. The index owns the full f32 vectors (graph
/// hops need exact distances), so its footprint is comparable to [`FlatIndex`] plus
/// the adjacency lists; it trades that memory for sub-linear search.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HnswIndex {
    /// Embedding dimension every inserted vector must match.
    pub dim: usize,
    /// Distance metric (ascending "smaller = nearer"); see [`Metric`].
    pub metric: Metric,
    /// `M` — target out-degree per node on layers > 0.
    pub m: usize,
    /// `M0` — target out-degree at the dense layer 0 (conventionally `2*M`).
    pub m0: usize,
    /// `efConstruction` — beam width used while inserting (bigger = better graph,
    /// slower build).
    pub ef_construction: usize,
    /// Seed for the deterministic, clock-free level assignment.
    pub seed: u64,
    /// Level multiplier `mL = 1/ln(M)` used by the exponential level law.
    ml: f64,
    /// Internal index of the current top-layer entry point, if any node exists.
    entry_point: Option<usize>,
    /// Highest layer currently present in the graph.
    max_level: usize,
    /// All nodes, indexed by internal id (insertion order).
    nodes: Vec<Node>,
}

/// Hard cap on assigned level — guards against a pathological hash producing an
/// absurd tower and keeps per-node adjacency vectors small.
const MAX_LEVEL_CAP: usize = 32;

impl HnswIndex {
    /// A fresh, empty index.
    ///
    /// * `dim` — embedding dimension (must be > 0).
    /// * `metric` — distance metric ([`Metric::L2`] / [`Metric::Cosine`] /
    ///   [`Metric::InnerProduct`]).
    /// * `m` — target out-degree on upper layers (must be ≥ 2; `m0` = `2*m`).
    /// * `ef_construction` — build-time beam width (clamped to ≥ `m`).
    /// * `seed` — determinism seed for level assignment.
    pub fn new(dim: usize, metric: Metric, m: usize, ef_construction: usize, seed: u64) -> Self {
        assert!(dim > 0, "dim must be positive");
        assert!(m >= 2, "M must be >= 2 (mL = 1/ln(M))");
        Self {
            dim,
            metric,
            m,
            m0: m * 2,
            ef_construction: ef_construction.max(m),
            seed,
            ml: 1.0 / (m as f64).ln(),
            entry_point: None,
            max_level: 0,
            nodes: Vec::new(),
        }
    }

    /// Number of nodes in the graph.
    pub fn len(&self) -> usize {
        self.nodes.len()
    }

    /// Whether the graph holds no nodes.
    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }

    /// Heap-byte footprint: full vectors + adjacency + node ids (approximate — the
    /// per-layer `Vec` headers are counted, their spilled capacity is estimated by
    /// length).
    pub fn byte_size(&self) -> usize {
        let mut n = std::mem::size_of::<Self>();
        for node in &self.nodes {
            n += std::mem::size_of::<Node>();
            n += node.vector.capacity() * std::mem::size_of::<f32>();
            for lvl in &node.neighbors {
                n += std::mem::size_of::<Vec<usize>>()
                    + lvl.capacity() * std::mem::size_of::<usize>();
            }
        }
        n
    }

    /// Deterministic exponential level for an id (CONCEPT:EG-KG.retrieval.hnsw-vector-index). A SplitMix64 hash
    /// of `(id, seed)` yields `u ∈ (0,1)`, and `level = floor(-ln(u) * mL)`, the
    /// standard HNSW law — but sourced from a pure hash so it needs no RNG state and
    /// is identical across runs and reloads.
    fn assign_level(&self, id: u64) -> usize {
        // SplitMix64 finaliser over id ^ seed.
        let mut x = id
            .wrapping_add(self.seed)
            .wrapping_add(0x9E37_79B9_7F4A_7C15);
        x = (x ^ (x >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        x = (x ^ (x >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        x ^= x >> 31;
        // Map the top 53 bits to (0, 1): +1 in numerator and denominator keeps the
        // open interval so -ln(u) is finite and non-negative.
        let mantissa = (x >> 11) as f64;
        let u = (mantissa + 1.0) / ((1u64 << 53) as f64 + 1.0);
        let lvl = (-u.ln() * self.ml).floor();
        (lvl.max(0.0) as usize).min(MAX_LEVEL_CAP)
    }

    /// EXACT metric distance from an external query vector to node `i`.
    #[inline]
    fn dist_to(&self, query: &[f32], i: usize) -> f32 {
        self.metric.distance(query, &self.nodes[i].vector)
    }

    /// Greedy descent within a single layer: from `entry`, repeatedly hop to the
    /// strictly-closer neighbour (ties broken by smaller id) until no neighbour
    /// improves, returning the local minimum node index. Used on the sparse upper
    /// layers where `ef = 1`.
    fn greedy_closest(&self, query: &[f32], entry: usize, layer: usize) -> usize {
        let mut best = entry;
        let mut best_d = self.dist_to(query, best);
        let mut best_id = self.nodes[best].id;
        loop {
            let mut improved = false;
            for &nb in &self.nodes[best].neighbors[layer] {
                let d = self.dist_to(query, nb);
                let nb_id = self.nodes[nb].id;
                if d < best_d || (d == best_d && nb_id < best_id) {
                    best = nb;
                    best_d = d;
                    best_id = nb_id;
                    improved = true;
                }
            }
            if !improved {
                break;
            }
        }
        best
    }

    /// Beam search at one layer (Malkov & Yashunin Algorithm 2). Explores from the
    /// entry points and keeps the `ef` nearest nodes found, using exact distances.
    /// Returns the result set as unordered [`Cand`]s (caller sorts / selects).
    fn search_layer(&self, query: &[f32], entries: &[usize], ef: usize, layer: usize) -> Vec<Cand> {
        let mut visited: HashSet<usize> = HashSet::with_capacity(ef * 4);
        // `candidates`: min-heap (nearest first) of the frontier to expand.
        let mut candidates: BinaryHeap<Reverse<Cand>> = BinaryHeap::new();
        // `w`: max-heap (farthest first) of the current best `ef` results.
        let mut w: BinaryHeap<Cand> = BinaryHeap::new();

        for &e in entries {
            let c = Cand {
                dist: self.dist_to(query, e),
                node: e,
                id: self.nodes[e].id,
            };
            visited.insert(e);
            candidates.push(Reverse(c));
            w.push(c);
        }
        while w.len() > ef {
            w.pop();
        }

        while let Some(Reverse(c)) = candidates.pop() {
            // If the nearest unexpanded candidate is farther than the worst kept
            // result (and w is full), the beam cannot improve — stop.
            if let Some(worst) = w.peek() {
                if w.len() >= ef && c.dist > worst.dist {
                    break;
                }
            }
            for &nb in &self.nodes[c.node].neighbors[layer] {
                if !visited.insert(nb) {
                    continue;
                }
                let d = self.dist_to(query, nb);
                let nc = Cand {
                    dist: d,
                    node: nb,
                    id: self.nodes[nb].id,
                };
                if w.len() < ef || w.peek().is_none_or(|worst| nc < *worst) {
                    candidates.push(Reverse(nc));
                    w.push(nc);
                    if w.len() > ef {
                        w.pop();
                    }
                }
            }
        }
        w.into_vec()
    }

    /// Select the `m` nearest candidates (simple heuristic), nearest-first with
    /// deterministic `(distance, id)` ordering, returning their internal indices.
    fn select_neighbors(cands: &[Cand], m: usize) -> Vec<usize> {
        let mut v: Vec<Cand> = cands.to_vec();
        truncate_nearest(&mut v, m);
        v.into_iter().map(|c| c.node).collect()
    }

    /// Re-prune node `node`'s adjacency at `layer` down to its `m` nearest current
    /// neighbours (exact distances from `node`'s own vector), keeping the graph
    /// degree bounded after a bidirectional link. Deterministic tie-break by id.
    fn prune(&mut self, node: usize, layer: usize, m: usize) {
        if self.nodes[node].neighbors[layer].len() <= m {
            return;
        }
        let base = self.nodes[node].vector.clone();
        let mut scored: Vec<Cand> = self.nodes[node].neighbors[layer]
            .iter()
            .map(|&nb| Cand {
                dist: self.metric.distance(&base, &self.nodes[nb].vector),
                node: nb,
                id: self.nodes[nb].id,
            })
            .collect();
        truncate_nearest(&mut scored, m);
        self.nodes[node].neighbors[layer] = scored.into_iter().map(|c| c.node).collect();
    }

    /// Insert `(id, vector)` into the graph (CONCEPT:EG-KG.retrieval.hnsw-vector-index). Assigns a level via the
    /// deterministic hash law, greedily descends the sparse upper layers, then runs
    /// an `efConstruction` beam search at each layer ≤ the node's level, wiring
    /// bidirectional edges and re-pruning over-full neighbours. `vector.len()` must
    /// equal `dim`.
    pub fn insert(&mut self, id: u64, vector: Vec<f32>) {
        assert_eq!(vector.len(), self.dim, "vector length must equal dim");
        let level = self.assign_level(id);
        let node_idx = self.nodes.len();
        self.nodes.push(Node {
            id,
            vector,
            neighbors: vec![Vec::new(); level + 1],
        });

        // First node becomes the entry point.
        let Some(mut ep) = self.entry_point else {
            self.entry_point = Some(node_idx);
            self.max_level = level;
            return;
        };

        let query = self.nodes[node_idx].vector.clone();

        // Phase 1: coarse greedy descent on layers ABOVE the new node's top layer.
        let mut lc = self.max_level;
        while lc > level {
            ep = self.greedy_closest(&query, ep, lc);
            lc -= 1;
        }

        // Phase 2: from the node's top layer (capped by the graph) down to 0, beam
        // search and connect.
        let start = level.min(self.max_level);
        for layer in (0..=start).rev() {
            let found = self.search_layer(&query, &[ep], self.ef_construction, layer);
            let m = if layer == 0 { self.m0 } else { self.m };
            let selected = Self::select_neighbors(&found, m);

            // Link the new node → selected, and selected → new node (bidirectional).
            self.nodes[node_idx].neighbors[layer] = selected.clone();
            for &nb in &selected {
                self.nodes[nb].neighbors[layer].push(node_idx);
                self.prune(nb, layer, m);
            }

            // Descend from the nearest node found at this layer.
            if let Some(nearest) = found.iter().min() {
                ep = nearest.node;
            }
        }

        // Grow the tower / move the entry point if this node reached a new ceiling.
        if level > self.max_level {
            self.max_level = level;
            self.entry_point = Some(node_idx);
        }
    }

    /// Convenience: bulk-insert `(id, vector)` pairs in order.
    pub fn insert_batch(&mut self, items: &[(u64, Vec<f32>)]) {
        for (id, v) in items {
            self.insert(*id, v.clone());
        }
    }

    /// Approximate top-`k` nearest neighbours to `query` (CONCEPT:EG-KG.retrieval.hnsw-vector-index). Descends
    /// the upper layers greedily, then beam-searches layer 0 with beam width `ef`
    /// (clamped to ≥ `k`). Results carry EXACT distances and are sorted nearest-first
    /// with deterministic `(distance, id)` tie-breaking — directly comparable and
    /// mergeable with [`FlatIndex`] / [`IvfPq`] / `merge_topk` output.
    ///
    /// A thin wrapper over [`Self::search_filtered`] with no candidate predicate,
    /// so the unfiltered walk is byte-identical to the original beam search.
    pub fn search(&self, query: &[f32], k: usize, ef: usize) -> Vec<SearchResult> {
        self.search_filtered(query, k, ef, None)
    }

    /// Approximate top-`k` with an optional metadata predicate PUSHED INTO the
    /// layer-0 neighbour expansion (CONCEPT:EG-KG.retrieval.hybrid-metadata-prefilter). `allow`, when present, is
    /// tested against each candidate's EXTERNAL id DURING the graph walk, so only
    /// permitted nodes ever enter the result beam and the returned top-`k` already
    /// satisfies the predicate — no over-fetch-then-post-filter. The traversal still
    /// routes THROUGH disallowed nodes (they remain navigability "bridges"), so recall
    /// over the allowed subset is preserved. `allow == None` is exactly the unfiltered
    /// [`Self::search`] (it dispatches to the original [`Self::search_layer`], so
    /// unfiltered results are unchanged). See [`Self::search_layer_filtered`] for why
    /// the explored count stays `O(ef / selectivity)` — independent of the graph size.
    pub fn search_filtered(
        &self,
        query: &[f32],
        k: usize,
        ef: usize,
        allow: Option<&dyn Fn(u64) -> bool>,
    ) -> Vec<SearchResult> {
        assert_eq!(query.len(), self.dim, "query length must equal dim");
        if k == 0 {
            return Vec::new();
        }
        let Some(mut ep) = self.entry_point else {
            return Vec::new();
        };
        // Upper-layer descent is PURE NAVIGATION (never filtered) — identical to the
        // unfiltered path. The predicate only governs which layer-0 nodes may be
        // RETURNED, not which nodes may be used to route toward the query.
        for layer in (1..=self.max_level).rev() {
            ep = self.greedy_closest(query, ep, layer);
        }
        let ef = ef.max(k);
        let mut found = match allow {
            None => self.search_layer(query, &[ep], ef, 0),
            Some(allow) => self.search_layer_filtered(query, &[ep], ef, 0, allow),
        };
        truncate_nearest(&mut found, k);
        found
            .into_iter()
            .map(|c| SearchResult {
                id: c.id,
                distance: c.dist,
            })
            .collect()
    }

    /// Beam search at one layer with a metadata predicate pushed into the neighbour
    /// expansion (CONCEPT:EG-KG.retrieval.hybrid-metadata-prefilter) — the filtered sibling of [`Self::search_layer`].
    ///
    /// The graph is walked through EVERY reachable node so navigability is preserved
    /// (a disallowed node is still expanded as a routing bridge), but only nodes whose
    /// external id passes `allow` may enter the result beam `results` (the max-heap of
    /// the `ef` nearest allowed candidates). The frontier admits a neighbour while the
    /// beam is not yet full of `ef` allowed nodes — so the walk floods toward the
    /// allowed subset — OR the neighbour is closer than the current worst allowed
    /// result — so once `ef` allowed nodes are found the frontier drains and the search
    /// terminates. Reaching `ef` allowed nodes at selectivity `s` costs `~ef/s` node
    /// visits regardless of the graph's size, which is the sub-linear win over a
    /// post-filter that must first over-fetch a size-proportional band. Deterministic:
    /// [`Cand`]'s total `(distance, id, node)` order drives every heap.
    fn search_layer_filtered(
        &self,
        query: &[f32],
        entries: &[usize],
        ef: usize,
        layer: usize,
        allow: &dyn Fn(u64) -> bool,
    ) -> Vec<Cand> {
        // A selective filter floods more of the graph than the unfiltered beam, so the
        // visited set is sized generously to cut rehashing.
        let mut visited: HashSet<usize> = HashSet::with_capacity(ef * 8);
        // `candidates`: min-heap (nearest first) of the frontier to expand — includes
        // disallowed bridge nodes.
        let mut candidates: BinaryHeap<Reverse<Cand>> = BinaryHeap::new();
        // `results`: max-heap (farthest first) of the current best `ef` ALLOWED nodes.
        let mut results: BinaryHeap<Cand> = BinaryHeap::new();

        for &e in entries {
            let c = Cand {
                dist: self.dist_to(query, e),
                node: e,
                id: self.nodes[e].id,
            };
            visited.insert(e);
            candidates.push(Reverse(c));
            if allow(c.id) {
                results.push(c);
            }
        }
        while results.len() > ef {
            results.pop();
        }

        while let Some(Reverse(c)) = candidates.pop() {
            // Stop only once the beam holds `ef` ALLOWED results AND the nearest
            // remaining frontier node is farther than the worst kept one — no closer
            // allowed node is reachable downhill.
            if results.len() >= ef {
                if let Some(worst) = results.peek() {
                    if c.dist > worst.dist {
                        break;
                    }
                }
            }
            for &nb in &self.nodes[c.node].neighbors[layer] {
                if !visited.insert(nb) {
                    continue;
                }
                let d = self.dist_to(query, nb);
                let worst_allowed = results.peek().map(|w| w.dist);
                // Frontier admission — the bound that keeps exploration sub-linear:
                // keep routing through this node while allowed results are still owed,
                // or while it could beat the current worst allowed result.
                if results.len() < ef || worst_allowed.is_none_or(|wd| d < wd) {
                    let nc = Cand {
                        dist: d,
                        node: nb,
                        id: self.nodes[nb].id,
                    };
                    candidates.push(Reverse(nc));
                    if allow(nc.id) {
                        results.push(nc);
                        if results.len() > ef {
                            results.pop();
                        }
                    }
                }
            }
        }
        // One targeted event per filtered search (a no-op without a subscriber): the
        // explored count + allowed-found let a caller derive the observed selectivity.
        tracing::trace!(
            target: "eg_ann::hnsw",
            mode = "filtered",
            ef,
            explored = visited.len(),
            allowed_found = results.len(),
            "hnsw filtered layer search",
        );
        results.into_vec()
    }
}

/// Retain the exact nearest `limit` candidates under [`Cand`]'s total
/// `(distance, id, node)` order. Selection is expected O(N); only the kept prefix is
/// ordered, reducing the final HNSW/neighbor-selection step to
/// O(N + limit log limit).
#[inline]
fn truncate_nearest(candidates: &mut Vec<Cand>, limit: usize) {
    if limit == 0 {
        candidates.clear();
        return;
    }
    if candidates.len() > limit {
        candidates.select_nth_unstable(limit);
        candidates.truncate(limit);
    }
    candidates.sort_unstable();
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::flat::FlatIndex;
    use crate::recall::recall_at_k;
    use rand::{Rng, SeedableRng};
    use rand_chacha::ChaCha8Rng;

    /// A tiny, hand-checkable 2-D graph — with only a handful of points every node
    /// is reachable, so HNSW must reproduce the exact brute-force order.
    fn tiny() -> HnswIndex {
        let mut h = HnswIndex::new(2, Metric::L2, 4, 32, 42);
        h.insert_batch(&[
            (10, vec![0.0, 0.0]),
            (20, vec![1.0, 0.0]),
            (30, vec![0.0, 2.0]),
            (40, vec![3.0, 4.0]),
            (50, vec![2.0, 1.0]),
        ]);
        h
    }

    #[test]
    fn partial_candidate_selection_matches_total_full_sort() {
        let input: Vec<Cand> = (0..257)
            .map(|node| Cand {
                // Deliberate ties exercise the external-id tiebreak at the boundary.
                dist: ((node * 37) % 23) as f32,
                node,
                id: (1_000 - node) as u64,
            })
            .collect();
        let mut expected = input.clone();
        expected.sort_unstable();
        expected.truncate(17);

        let mut selected = input;
        truncate_nearest(&mut selected, 17);

        assert_eq!(
            selected
                .iter()
                .map(|candidate| (candidate.dist.to_bits(), candidate.id, candidate.node))
                .collect::<Vec<_>>(),
            expected
                .iter()
                .map(|candidate| (candidate.dist.to_bits(), candidate.id, candidate.node))
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn eg301_tiny_graph_insert_and_search_matches_exact() {
        let h = tiny();
        // Squared-L2 from origin: 10→0, 20→1, 50→5, 30→4, 40→25. Nearest 3: 10,20,30.
        let res = h.search(&[0.0, 0.0], 3, 16);
        assert_eq!(
            res.iter().map(|r| r.id).collect::<Vec<_>>(),
            vec![10, 20, 30]
        );
        assert_eq!(res[0].distance, 0.0);
        assert_eq!(res[1].distance, 1.0);
        assert_eq!(res[2].distance, 4.0);
    }

    #[test]
    fn eg301_search_returns_true_distances_and_k_bound() {
        let h = tiny();
        let res = h.search(&[2.0, 1.0], 2, 16);
        // 50 is exactly the query point → distance 0, then 20 [1,0] → 2.
        assert_eq!(res.len(), 2);
        assert_eq!(res[0].id, 50);
        assert_eq!(res[0].distance, 0.0);
    }

    #[test]
    fn eg301_empty_and_k_zero() {
        let empty = HnswIndex::new(3, Metric::L2, 4, 16, 1);
        assert!(empty.is_empty());
        assert!(empty.search(&[0.0, 0.0, 0.0], 5, 16).is_empty());
        let h = tiny();
        assert!(h.search(&[0.0, 0.0], 0, 16).is_empty());
    }

    #[test]
    fn eg301_deterministic_same_seed_same_result() {
        let build = || {
            let mut h = HnswIndex::new(8, Metric::L2, 8, 64, 123);
            let mut rng = ChaCha8Rng::seed_from_u64(7);
            for id in 0..300u64 {
                let v: Vec<f32> = (0..8).map(|_| rng.gen::<f32>()).collect();
                h.insert(id, v);
            }
            h
        };
        let a = build();
        let b = build();
        let mut qr = ChaCha8Rng::seed_from_u64(99);
        for _ in 0..20 {
            let q: Vec<f32> = (0..8).map(|_| qr.gen::<f32>()).collect();
            let ra: Vec<u64> = a.search(&q, 10, 50).into_iter().map(|r| r.id).collect();
            let rb: Vec<u64> = b.search(&q, 10, 50).into_iter().map(|r| r.id).collect();
            assert_eq!(ra, rb, "same seed + order must be bit-identical");
        }
    }

    fn random_vecs(n: usize, dim: usize, seed: u64) -> Vec<Vec<f32>> {
        let mut rng = ChaCha8Rng::seed_from_u64(seed);
        (0..n)
            .map(|_| (0..dim).map(|_| rng.gen::<f32>() * 2.0 - 1.0).collect())
            .collect()
    }

    /// THE BAR (CONCEPT:EG-KG.retrieval.hnsw-vector-index): recall@10 ≥ 0.9 vs the exact [`FlatIndex`] ground
    /// truth on a random set at a reasonable `ef`.
    #[test]
    fn eg301_recall_at_10_meets_target_vs_flat() {
        let dim = 32;
        let n = 5_000;
        let data = random_vecs(n, dim, 11);

        let mut hnsw = HnswIndex::new(dim, Metric::L2, 16, 200, 7);
        let mut flat = FlatIndex::new(dim);
        for (i, v) in data.iter().enumerate() {
            hnsw.insert(i as u64, v.clone());
            flat.add(&[(i as u64, v.clone())]);
        }

        let mut qr = ChaCha8Rng::seed_from_u64(2024);
        let nq = 100;
        let mut sum = 0.0f64;
        for _ in 0..nq {
            let q: Vec<f32> = (0..dim).map(|_| qr.gen::<f32>() * 2.0 - 1.0).collect();
            let truth: Vec<u64> = flat
                .search(&q, 10, Metric::L2)
                .into_iter()
                .map(|r| r.id)
                .collect();
            let got: Vec<u64> = hnsw.search(&q, 10, 100).into_iter().map(|r| r.id).collect();
            sum += recall_at_k(&got, &truth, 10);
        }
        let recall = sum / nq as f64;
        assert!(
            recall >= 0.9,
            "HNSW recall@10 = {recall:.4} must be >= 0.9 (dim={dim}, n={n}, ef=100)"
        );
    }

    #[test]
    fn eg301_recall_holds_for_cosine_metric() {
        let dim = 24;
        let n = 3_000;
        let data = random_vecs(n, dim, 21);
        let mut hnsw = HnswIndex::new(dim, Metric::Cosine, 16, 200, 3);
        let mut flat = FlatIndex::new(dim);
        for (i, v) in data.iter().enumerate() {
            hnsw.insert(i as u64, v.clone());
            flat.add(&[(i as u64, v.clone())]);
        }
        let mut qr = ChaCha8Rng::seed_from_u64(55);
        let nq = 60;
        let mut sum = 0.0f64;
        for _ in 0..nq {
            let q: Vec<f32> = (0..dim).map(|_| qr.gen::<f32>() * 2.0 - 1.0).collect();
            let truth: Vec<u64> = flat
                .search(&q, 10, Metric::Cosine)
                .into_iter()
                .map(|r| r.id)
                .collect();
            let got: Vec<u64> = hnsw.search(&q, 10, 120).into_iter().map(|r| r.id).collect();
            sum += recall_at_k(&got, &truth, 10);
        }
        let recall = sum / nq as f64;
        assert!(
            recall >= 0.9,
            "cosine HNSW recall@10 = {recall:.4} must be >= 0.9"
        );
    }

    #[test]
    fn eg301_serde_round_trip_preserves_graph_and_search() {
        let dim = 16;
        let data = random_vecs(800, dim, 44);
        let mut hnsw = HnswIndex::new(dim, Metric::L2, 12, 100, 9);
        for (i, v) in data.iter().enumerate() {
            hnsw.insert(i as u64, v.clone());
        }
        let mut qr = ChaCha8Rng::seed_from_u64(1);
        let q: Vec<f32> = (0..dim).map(|_| qr.gen::<f32>() * 2.0 - 1.0).collect();
        let before = hnsw.search(&q, 10, 64);

        let bytes = crate::codec::serialize(&hnsw).expect("serialize HnswIndex");
        let back: HnswIndex = crate::codec::deserialize(&bytes).expect("deserialize HnswIndex");
        assert_eq!(back.len(), hnsw.len());
        assert_eq!(back.max_level, hnsw.max_level);
        assert_eq!(back.entry_point, hnsw.entry_point);
        let after = back.search(&q, 10, 64);
        assert_eq!(
            before, after,
            "search must be identical after a serde round-trip"
        );
    }

    #[test]
    fn eg301_self_retrieval_top1() {
        let dim = 20;
        let data = random_vecs(2_000, dim, 88);
        let mut hnsw = HnswIndex::new(dim, Metric::L2, 16, 200, 5);
        for (i, v) in data.iter().enumerate() {
            hnsw.insert(i as u64, v.clone());
        }
        // A stored vector should retrieve itself at rank 1 for a well-built graph.
        let mut hits = 0;
        for &probe in &[0usize, 500, 1000, 1500, 1999] {
            let res = hnsw.search(&data[probe], 1, 64);
            if res[0].id == probe as u64 {
                hits += 1;
            }
        }
        assert!(hits >= 4, "self-retrieval top-1 too weak: {hits}/5");
    }

    /// Exact filtered top-`k` ground truth: brute-force distances over ONLY the ids
    /// passing `allow`, nearest-first with the crate's `(distance, id)` tie-break.
    fn brute_filtered(
        data: &[Vec<f32>],
        q: &[f32],
        k: usize,
        metric: Metric,
        allow: &dyn Fn(u64) -> bool,
    ) -> Vec<u64> {
        let mut scored: Vec<(f32, u64)> = data
            .iter()
            .enumerate()
            .filter(|(i, _)| allow(*i as u64))
            .map(|(i, v)| (metric.distance(q, v), i as u64))
            .collect();
        scored.sort_by(|a, b| a.0.total_cmp(&b.0).then_with(|| a.1.cmp(&b.1)));
        scored.truncate(k);
        scored.into_iter().map(|(_, id)| id).collect()
    }

    #[test]
    fn filtered_none_equals_unfiltered() {
        let dim = 16;
        let data = random_vecs(1_000, dim, 71);
        let mut hnsw = HnswIndex::new(dim, Metric::L2, 12, 100, 4);
        for (i, v) in data.iter().enumerate() {
            hnsw.insert(i as u64, v.clone());
        }
        let mut qr = ChaCha8Rng::seed_from_u64(3);
        for _ in 0..20 {
            let q: Vec<f32> = (0..dim).map(|_| qr.gen::<f32>() * 2.0 - 1.0).collect();
            assert_eq!(
                hnsw.search(&q, 10, 64),
                hnsw.search_filtered(&q, 10, 64, None),
                "search_filtered(None) must equal the unfiltered search byte-for-byte"
            );
        }
    }

    #[test]
    fn filtered_search_returns_only_allowed_ids() {
        let dim = 24;
        let data = random_vecs(3_000, dim, 12);
        let mut hnsw = HnswIndex::new(dim, Metric::Cosine, 16, 200, 9);
        for (i, v) in data.iter().enumerate() {
            hnsw.insert(i as u64, v.clone());
        }
        // Allow only ids ≡ 0 (mod 7) — ~14% selectivity.
        let allow = |id: u64| id.is_multiple_of(7);
        let mut qr = ChaCha8Rng::seed_from_u64(101);
        for _ in 0..25 {
            let q: Vec<f32> = (0..dim).map(|_| qr.gen::<f32>() * 2.0 - 1.0).collect();
            let got = hnsw.search_filtered(&q, 10, 96, Some(&allow));
            assert!(
                got.iter().all(|r| allow(r.id)),
                "every returned id must satisfy the predicate"
            );
        }
    }

    /// THE FILTERED BAR (CONCEPT:EG-KG.retrieval.hybrid-metadata-prefilter): recall@10 ≥ 0.9 over the ALLOWED
    /// subset at ~1% selectivity, vs the exact filtered brute-force ground truth.
    #[test]
    fn filtered_recall_at_10_meets_target_1pct_selectivity() {
        let dim = 32;
        let n = 10_000;
        let data = random_vecs(n, dim, 33);
        let mut hnsw = HnswIndex::new(dim, Metric::L2, 16, 200, 7);
        for (i, v) in data.iter().enumerate() {
            hnsw.insert(i as u64, v.clone());
        }
        // ~1% selectivity: keep ids ≡ 0 (mod 100).
        let allow = |id: u64| id.is_multiple_of(100);
        let ef = 128;
        let mut qr = ChaCha8Rng::seed_from_u64(2025);
        let nq = 100;
        let mut sum = 0.0f64;
        for _ in 0..nq {
            let q: Vec<f32> = (0..dim).map(|_| qr.gen::<f32>() * 2.0 - 1.0).collect();
            let truth = brute_filtered(&data, &q, 10, Metric::L2, &allow);
            let got: Vec<u64> = hnsw
                .search_filtered(&q, 10, ef, Some(&allow))
                .into_iter()
                .map(|r| r.id)
                .collect();
            sum += recall_at_k(&got, &truth, 10);
        }
        let recall = sum / nq as f64;
        assert!(
            recall >= 0.9,
            "filtered HNSW recall@10 = {recall:.4} must be >= 0.9 (n={n}, sel=1%, ef={ef})"
        );
    }

    #[test]
    fn filtered_search_deterministic() {
        let dim = 20;
        let data = random_vecs(2_000, dim, 88);
        let build = || {
            let mut h = HnswIndex::new(dim, Metric::L2, 16, 200, 5);
            for (i, v) in data.iter().enumerate() {
                h.insert(i as u64, v.clone());
            }
            h
        };
        let a = build();
        let b = build();
        let allow = |id: u64| id.is_multiple_of(50);
        let mut qr = ChaCha8Rng::seed_from_u64(7);
        for _ in 0..20 {
            let q: Vec<f32> = (0..dim).map(|_| qr.gen::<f32>() * 2.0 - 1.0).collect();
            assert_eq!(
                a.search_filtered(&q, 10, 96, Some(&allow)),
                b.search_filtered(&q, 10, 96, Some(&allow)),
                "filtered search must be deterministic across identical builds"
            );
        }
    }
}
