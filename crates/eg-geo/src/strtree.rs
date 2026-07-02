//! A **durable, bulk-loaded STR (Sort-Tile-Recursive) R-tree** (CONCEPT:EG-263).
//!
//! [`crate::rtree`] gave the engine an in-memory packed *Hilbert* R-tree (CONCEPT:EG-083)
//! that answers `query_bbox` and is rebuilt per query. EG-263 adds a richer index that is:
//!
//! * **STR-packed** — the Guttman/Leutenegger *Sort-Tile-Recursive* bulk load: sort item
//!   boxes by centre-x, cut into `√P` vertical slices, sort each slice by centre-y, and pack
//!   `node_size` boxes per leaf; then recurse the same tiling over the node boxes. STR gives
//!   consistently low node overlap → fewer boxes touched per query.
//! * **serde-serialisable ⇒ durable** — the whole tree ([`StrTree`]) derives `Serialize`/
//!   `Deserialize`, so it persists **alongside** the redb per-graph store and is loaded back
//!   without a rebuild (the "durable spatial index" the wave asked for).
//! * **multi-query** — [`StrTree::query_bbox`] (range / bbox-intersect), [`StrTree::nearest`]
//!   (k-nearest-neighbour, best-first branch-and-bound on box `mindist`) and
//!   [`StrTree::query_contained_in`] (containment pruning — items whose box lies fully inside
//!   a window).
//!
//! `Op::SpatialScan` (whose executor lives in `eg-plan`, not here) can consult a persisted
//! [`StrTree`] for candidate pruning instead of rebuilding an index each scan; that exec
//! wiring is a documented follow-up (see the crate `lib.rs` note). Pure-Rust, only serde.

use std::cmp::Ordering;
use std::collections::BinaryHeap;

use serde::{Deserialize, Serialize};

use crate::geometry::{Bbox, Point};

/// One node of the [`StrTree`] arena.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct Node {
    /// The node's minimum bounding rectangle (covers all descendants).
    bbox: Bbox,
    kind: NodeKind,
}

/// A node is either a leaf holding `(item_id, item_bbox)` entries or an internal node
/// holding child-node arena indices.
#[derive(Clone, Debug, Serialize, Deserialize)]
enum NodeKind {
    Leaf(Vec<(usize, Bbox)>),
    Internal(Vec<usize>),
}

/// A durable STR-packed R-tree over item bounding boxes (CONCEPT:EG-263). Build once from
/// `(item_id, Bbox)` pairs, persist via serde, query many times.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StrTree {
    node_size: usize,
    num_items: usize,
    /// Arena of nodes; `root` indexes the covering node (`None` when empty).
    nodes: Vec<Node>,
    root: Option<usize>,
}

impl StrTree {
    /// Build the index from `(item_id, bbox)` pairs with the default `node_size` (16).
    pub fn build(items: &[(usize, Bbox)]) -> StrTree {
        Self::build_with_node_size(items, 16)
    }

    /// Build with an explicit `node_size` (branching factor / leaf capacity, min 2)
    /// (CONCEPT:EG-263). `item_id`s are returned verbatim by the query methods.
    pub fn build_with_node_size(items: &[(usize, Bbox)], node_size: usize) -> StrTree {
        let node_size = node_size.max(2);
        let num_items = items.len();
        if num_items == 0 {
            return StrTree {
                node_size,
                num_items: 0,
                nodes: Vec::new(),
                root: None,
            };
        }

        let mut nodes: Vec<Node> = Vec::new();

        // ── level 0: STR-pack the items into leaf nodes ──────────────────────────────
        let leaf_groups = str_tile(items.to_vec(), node_size, |(_, b)| *b);
        let mut level: Vec<usize> = Vec::with_capacity(leaf_groups.len());
        for group in leaf_groups {
            let bbox = cover(group.iter().map(|(_, b)| *b)).expect("non-empty leaf group");
            let idx = nodes.len();
            nodes.push(Node {
                bbox,
                kind: NodeKind::Leaf(group),
            });
            level.push(idx);
        }

        // ── internal levels: STR-pack node boxes bottom-up until a single root remains ─
        while level.len() > 1 {
            // Carry each child index tagged with its bbox for the tiling key.
            let entries: Vec<(usize, Bbox)> = level.iter().map(|&i| (i, nodes[i].bbox)).collect();
            let groups = str_tile(entries, node_size, |(_, b)| *b);
            let mut next: Vec<usize> = Vec::with_capacity(groups.len());
            for group in groups {
                let bbox = cover(group.iter().map(|(_, b)| *b)).expect("non-empty node group");
                let children: Vec<usize> = group.into_iter().map(|(i, _)| i).collect();
                let idx = nodes.len();
                nodes.push(Node {
                    bbox,
                    kind: NodeKind::Internal(children),
                });
                next.push(idx);
            }
            level = next;
        }

        let root = level.first().copied();
        StrTree {
            node_size,
            num_items,
            nodes,
            root,
        }
    }

    /// The number of indexed items.
    pub fn len(&self) -> usize {
        self.num_items
    }

    pub fn is_empty(&self) -> bool {
        self.num_items == 0
    }

    /// The root node's covering bounding box (`None` when empty).
    pub fn bounds(&self) -> Option<Bbox> {
        self.root.map(|r| self.nodes[r].bbox)
    }

    /// **Range query** (CONCEPT:EG-263): the ids of every indexed item whose box intersects
    /// `query`. Descends only into nodes whose box intersects the query. Order unspecified.
    pub fn query_bbox(&self, query: &Bbox) -> Vec<usize> {
        let mut out = Vec::new();
        let Some(root) = self.root else {
            return out;
        };
        let mut stack = vec![root];
        while let Some(n) = stack.pop() {
            let node = &self.nodes[n];
            if !node.bbox.intersects(query) {
                continue;
            }
            match &node.kind {
                NodeKind::Leaf(items) => {
                    for (id, b) in items {
                        if b.intersects(query) {
                            out.push(*id);
                        }
                    }
                }
                NodeKind::Internal(children) => stack.extend(children.iter().copied()),
            }
        }
        out
    }

    /// **Containment query** (CONCEPT:EG-263): the ids of every indexed item whose box lies
    /// **fully inside** `window`. Prunes any subtree whose covering box does not intersect the
    /// window; a subtree entirely inside the window is fully harvested. Order unspecified.
    pub fn query_contained_in(&self, window: &Bbox) -> Vec<usize> {
        let mut out = Vec::new();
        let Some(root) = self.root else {
            return out;
        };
        let mut stack = vec![root];
        while let Some(n) = stack.pop() {
            let node = &self.nodes[n];
            if !node.bbox.intersects(window) {
                continue; // disjoint ⇒ nothing inside
            }
            match &node.kind {
                NodeKind::Leaf(items) => {
                    for (id, b) in items {
                        if window.contains(b) {
                            out.push(*id);
                        }
                    }
                }
                NodeKind::Internal(children) => stack.extend(children.iter().copied()),
            }
        }
        out
    }

    /// **k-nearest-neighbour** (CONCEPT:EG-263): the up-to-`k` items whose box is closest to
    /// `point`, nearest first, each with its box `mindist` (0.0 when the point is inside the
    /// box). Best-first branch-and-bound over a min-heap keyed on box `mindist`, so whole
    /// subtrees prune once their nearest possible box is farther than the current k-th hit.
    pub fn nearest(&self, point: &Point, k: usize) -> Vec<(usize, f64)> {
        let mut out: Vec<(usize, f64)> = Vec::new();
        if k == 0 {
            return out;
        }
        let Some(root) = self.root else {
            return out;
        };

        // Min-heap of pending entries ordered by ascending mindist to `point`.
        let mut heap: BinaryHeap<Entry> = BinaryHeap::new();
        heap.push(Entry {
            dist: mindist(point, &self.nodes[root].bbox),
            node: root,
        });
        while let Some(Entry { dist, node }) = heap.pop() {
            // Everything remaining is at least `dist` away; if we already have k and the best
            // pending is not closer than the current worst kept, we are done.
            if out.len() == k && dist >= out[k - 1].1 {
                break;
            }
            match &self.nodes[node].kind {
                NodeKind::Leaf(items) => {
                    for (id, b) in items {
                        let d = mindist(point, b);
                        insert_knn(&mut out, k, (*id, d));
                    }
                }
                NodeKind::Internal(children) => {
                    for &c in children {
                        heap.push(Entry {
                            dist: mindist(point, &self.nodes[c].bbox),
                            node: c,
                        });
                    }
                }
            }
        }
        out
    }
}

/// A pending best-first entry, ordered so [`BinaryHeap`] (a max-heap) pops the **smallest**
/// `mindist` first.
struct Entry {
    dist: f64,
    node: usize,
}
impl PartialEq for Entry {
    fn eq(&self, o: &Self) -> bool {
        self.dist == o.dist
    }
}
impl Eq for Entry {}
impl PartialOrd for Entry {
    fn partial_cmp(&self, o: &Self) -> Option<Ordering> {
        Some(self.cmp(o))
    }
}
impl Ord for Entry {
    fn cmp(&self, o: &Self) -> Ordering {
        // Reverse: smaller dist == "greater" so it pops first. NaN sorts last.
        o.dist
            .partial_cmp(&self.dist)
            .unwrap_or(Ordering::Equal)
    }
}

/// Keep the running k-nearest list sorted ascending, inserting `cand` if it belongs.
fn insert_knn(out: &mut Vec<(usize, f64)>, k: usize, cand: (usize, f64)) {
    if out.len() == k && cand.1 >= out[k - 1].1 {
        return;
    }
    let pos = out
        .binary_search_by(|(_, d)| d.partial_cmp(&cand.1).unwrap_or(Ordering::Equal))
        .unwrap_or_else(|e| e);
    out.insert(pos, cand);
    if out.len() > k {
        out.truncate(k);
    }
}

/// Minimum Euclidean distance from `point` to bounding box `b` (0 when inside).
fn mindist(point: &Point, b: &Bbox) -> f64 {
    let dx = if point.x < b.minx {
        b.minx - point.x
    } else if point.x > b.maxx {
        point.x - b.maxx
    } else {
        0.0
    };
    let dy = if point.y < b.miny {
        b.miny - point.y
    } else if point.y > b.maxy {
        point.y - b.maxy
    } else {
        0.0
    };
    (dx * dx + dy * dy).sqrt()
}

/// The bounding box covering an iterator of boxes (`None` when empty).
fn cover(boxes: impl Iterator<Item = Bbox>) -> Option<Bbox> {
    let mut acc: Option<Bbox> = None;
    for b in boxes {
        match &mut acc {
            Some(a) => a.union(&b),
            None => acc = Some(b),
        }
    }
    acc
}

/// The Sort-Tile-Recursive tiling of `entries` into groups of at most `node_size`
/// (CONCEPT:EG-263): sort by box centre-x, cut into `√P` vertical slices, sort each slice by
/// centre-y, then chunk `node_size` per group. `key` extracts each entry's box.
fn str_tile<T: Clone>(
    mut entries: Vec<T>,
    node_size: usize,
    key: impl Fn(&T) -> Bbox,
) -> Vec<Vec<T>> {
    let n = entries.len();
    if n <= node_size {
        return vec![entries];
    }
    // Number of leaf groups P, and slice count S = ceil(√P); each slice spans S groups.
    let p = n.div_ceil(node_size);
    let s = (p as f64).sqrt().ceil() as usize;
    let s = s.max(1);
    let slice_items = s * node_size; // items per vertical slice

    // Sort by centre-x, then process slice by slice.
    entries.sort_by(|a, b| {
        let (ax, _) = key(a).center();
        let (bx, _) = key(b).center();
        ax.partial_cmp(&bx).unwrap_or(Ordering::Equal)
    });

    let mut groups: Vec<Vec<T>> = Vec::with_capacity(p);
    let mut idx = 0usize;
    while idx < n {
        let end = (idx + slice_items).min(n);
        let mut slice: Vec<T> = entries.drain(..end - idx).collect();
        idx = end;
        // Within the slice, sort by centre-y and chunk node_size per group.
        slice.sort_by(|a, b| {
            let (_, ay) = key(a).center();
            let (_, by) = key(b).center();
            ay.partial_cmp(&by).unwrap_or(Ordering::Equal)
        });
        let mut start = 0usize;
        while start < slice.len() {
            let take = node_size.min(slice.len() - start);
            groups.push(slice[start..start + take].to_vec());
            start += take;
        }
    }
    groups
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sorted(mut v: Vec<usize>) -> Vec<usize> {
        v.sort_unstable();
        v
    }

    fn brute_range(items: &[(usize, Bbox)], q: &Bbox) -> Vec<usize> {
        let mut v: Vec<usize> = items
            .iter()
            .filter(|(_, b)| b.intersects(q))
            .map(|(id, _)| *id)
            .collect();
        v.sort_unstable();
        v
    }

    fn brute_contained(items: &[(usize, Bbox)], q: &Bbox) -> Vec<usize> {
        let mut v: Vec<usize> = items
            .iter()
            .filter(|(_, b)| q.contains(b))
            .map(|(id, _)| *id)
            .collect();
        v.sort_unstable();
        v
    }

    /// Deterministic LCG (no rand dep) → pseudo-random f64 in [0, ~1).
    fn lcg() -> impl FnMut() -> f64 {
        let mut seed: u64 = 0x0bad_c0de_dead_beef;
        move || {
            seed = seed
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            ((seed >> 33) as f64) / (u32::MAX as f64)
        }
    }

    fn random_items(n: usize) -> Vec<(usize, Bbox)> {
        let mut next = lcg();
        (0..n)
            .map(|id| {
                let x = next() * 1000.0;
                let y = next() * 1000.0;
                let w = next() * 20.0;
                let h = next() * 20.0;
                (id, Bbox::new(x, y, x + w, y + h))
            })
            .collect()
    }

    #[test]
    fn eg263_empty_index() {
        let t = StrTree::build(&[]);
        assert!(t.is_empty());
        assert!(t.query_bbox(&Bbox::new(0.0, 0.0, 1.0, 1.0)).is_empty());
        assert!(t.nearest(&Point::new(0.0, 0.0), 5).is_empty());
        assert!(t.bounds().is_none());
    }

    #[test]
    fn eg263_str_range_matches_brute_force() {
        let items = random_items(1000);
        let t = StrTree::build_with_node_size(&items, 4); // small ⇒ several levels
        assert_eq!(t.len(), 1000);
        let mut next = lcg();
        for _ in 0..200 {
            let qx = next() * 1000.0;
            let qy = next() * 1000.0;
            let q = Bbox::new(qx, qy, qx + next() * 100.0, qy + next() * 100.0);
            assert_eq!(
                sorted(t.query_bbox(&q)),
                brute_range(&items, &q),
                "STR range mismatch for {q:?}"
            );
        }
    }

    #[test]
    fn eg263_containment_pruning_matches_brute_force() {
        let items = random_items(600);
        let t = StrTree::build_with_node_size(&items, 8);
        let mut next = lcg();
        for _ in 0..100 {
            let qx = next() * 900.0;
            let qy = next() * 900.0;
            // Large-ish windows so some boxes are fully contained.
            let q = Bbox::new(qx, qy, qx + next() * 200.0 + 30.0, qy + next() * 200.0 + 30.0);
            assert_eq!(
                sorted(t.query_contained_in(&q)),
                brute_contained(&items, &q),
                "STR containment mismatch for {q:?}"
            );
        }
    }

    #[test]
    fn eg263_knn_matches_brute_force() {
        let items = random_items(500);
        let t = StrTree::build_with_node_size(&items, 6);
        let mut next = lcg();
        for _ in 0..50 {
            let p = Point::new(next() * 1000.0, next() * 1000.0);
            let k = 7;
            let got = t.nearest(&p, k);
            // Brute-force: sort ALL items by box mindist, take k.
            let mut all: Vec<(usize, f64)> = items
                .iter()
                .map(|(id, b)| (*id, super::mindist(&p, b)))
                .collect();
            all.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
            all.truncate(k);
            // Distances must match element-wise (ids may differ only on exact ties).
            assert_eq!(got.len(), all.len(), "knn count for {p:?}");
            for (g, e) in got.iter().zip(&all) {
                assert!(
                    (g.1 - e.1).abs() < 1e-9,
                    "knn dist mismatch at {p:?}: {g:?} vs {e:?}"
                );
            }
        }
    }

    #[test]
    fn eg263_durable_serde_round_trip_preserves_queries() {
        // The tree persists via serde (durable index) and answers identically after reload.
        let items = random_items(300);
        let t = StrTree::build_with_node_size(&items, 5);
        let bytes = serde_json::to_vec(&t).expect("serialize StrTree");
        let restored: StrTree = serde_json::from_slice(&bytes).expect("deserialize StrTree");
        assert_eq!(restored.len(), t.len());

        let q = Bbox::new(100.0, 100.0, 400.0, 400.0);
        assert_eq!(sorted(t.query_bbox(&q)), sorted(restored.query_bbox(&q)));
        // kNN ids must match; distances match within the serde_json f64 text round-trip epsilon
        // (JSON is not guaranteed bit-exact for every f64 — the index STRUCTURE is preserved).
        let p = Point::new(500.0, 500.0);
        let before = t.nearest(&p, 10);
        let after = restored.nearest(&p, 10);
        assert_eq!(before.len(), after.len());
        for (a, b) in before.iter().zip(&after) {
            assert_eq!(a.0, b.0, "kNN id order changed after reload");
            assert!(
                (a.1 - b.1).abs() < 1e-9,
                "kNN dist drift after reload: {a:?} vs {b:?}"
            );
        }
    }
}
