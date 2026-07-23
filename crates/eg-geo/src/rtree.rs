//! An in-house **packed Hilbert R-tree** (CONCEPT:EG-KG.ontology.singles-concept) — a static, bottom-up-packed
//! spatial index over a set of bounding boxes, in the "flatbush" idiom: items are
//! sorted along a Hilbert space-filling curve (so spatially-near boxes are near in the
//! packed array), then grouped `node_size` at a time into a fixed tree stored in flat
//! `Vec`s. `query_bbox` descends only into nodes whose box intersects the query,
//! yielding the original item ids of every leaf box that intersects.
//!
//! Pure-Rust, no dependency. Build once from `(id, Bbox)` pairs; query many times.

use crate::geometry::Bbox;

/// A static packed Hilbert R-tree over item bounding boxes.
pub struct RTree {
    node_size: usize,
    num_items: usize,
    /// Bounding boxes for every node: `boxes[0..num_items]` are the leaves (in Hilbert
    /// order), followed by the internal levels bottom-up; the last entry is the root.
    boxes: Vec<Bbox>,
    /// For a leaf position `i < num_items`, `indices[i]` is the ORIGINAL item id. For an
    /// internal node, `indices[i]` is the position of its FIRST child in `boxes`.
    indices: Vec<usize>,
    /// Exclusive end offset (into `boxes`) of each level, leaves first.
    level_bounds: Vec<usize>,
}

impl RTree {
    /// Build the index from `(item_id, bbox)` pairs. `item_id`s are returned verbatim by
    /// [`RTree::query_bbox`] (they need not be contiguous or sorted). An empty input
    /// yields an empty index whose queries return nothing.
    pub fn build(items: &[(usize, Bbox)]) -> RTree {
        Self::build_with_node_size(items, 16)
    }

    /// As [`RTree::build`] with an explicit `node_size` (branching factor, min 2).
    pub fn build_with_node_size(items: &[(usize, Bbox)], node_size: usize) -> RTree {
        let node_size = node_size.max(2);
        let num_items = items.len();
        if num_items == 0 {
            return RTree {
                node_size,
                num_items: 0,
                boxes: Vec::new(),
                indices: Vec::new(),
                level_bounds: Vec::new(),
            };
        }

        // Total extent, to normalize Hilbert coordinates into a 16-bit grid.
        let mut total = items[0].1;
        for (_, b) in &items[1..] {
            total.union(b);
        }

        // Hilbert-sort the item order by each box center's Hilbert value.
        let width = (total.maxx - total.minx).max(f64::MIN_POSITIVE);
        let height = (total.maxy - total.miny).max(f64::MIN_POSITIVE);
        const HILBERT_MAX: f64 = ((1u32 << 16) - 1) as f64;
        let mut order: Vec<usize> = (0..num_items).collect();
        let hilbert_of = |b: &Bbox| -> u32 {
            let (cx, cy) = b.center();
            let hx = ((cx - total.minx) / width * HILBERT_MAX) as u32;
            let hy = ((cy - total.miny) / height * HILBERT_MAX) as u32;
            hilbert_xy_to_d(hx, hy)
        };
        order.sort_by_key(|&i| hilbert_of(&items[i].1));

        // Compute the number of nodes at each level and the total, bottom-up.
        let mut level_bounds = vec![num_items];
        let mut n = num_items;
        loop {
            n = n.div_ceil(node_size);
            let last = *level_bounds.last().unwrap();
            level_bounds.push(last + n);
            if n == 1 {
                break;
            }
        }
        let num_nodes = *level_bounds.last().unwrap();

        let mut boxes: Vec<Bbox> = Vec::with_capacity(num_nodes);
        let mut indices: Vec<usize> = Vec::with_capacity(num_nodes);

        // Leaf level, in Hilbert order.
        for &i in &order {
            boxes.push(items[i].1);
            indices.push(items[i].0);
        }

        // Build each internal level from the previous one.
        let mut read_start = 0usize; // start of the level we are grouping
        let last_level = level_bounds.len() - 1;
        for &read_end in &level_bounds[..last_level] {
            let mut pos = read_start;
            while pos < read_end {
                let child_start = pos;
                let mut node_box = boxes[pos];
                let mut k = 1;
                pos += 1;
                while k < node_size && pos < read_end {
                    node_box.union(&boxes[pos]);
                    pos += 1;
                    k += 1;
                }
                boxes.push(node_box);
                indices.push(child_start);
            }
            read_start = read_end;
        }

        RTree {
            node_size,
            num_items,
            boxes,
            indices,
            level_bounds,
        }
    }

    /// The number of indexed items.
    pub fn len(&self) -> usize {
        self.num_items
    }

    pub fn is_empty(&self) -> bool {
        self.num_items == 0
    }

    /// Return the original item ids of every indexed box that intersects `query`.
    /// Order is unspecified (packed/traversal order).
    pub fn query_bbox(&self, query: &Bbox) -> Vec<usize> {
        let mut out = Vec::new();
        if self.num_items == 0 {
            return out;
        }
        // Start at the root (last node) and its level.
        let root = self.boxes.len() - 1;
        let root_level = self.level_bounds.len() - 1;
        let mut stack: Vec<(usize, usize)> = vec![(root, root_level)];
        while let Some((node_pos, level)) = stack.pop() {
            if !self.boxes[node_pos].intersects(query) {
                continue;
            }
            if level == 0 {
                // Leaf: emit the original id.
                out.push(self.indices[node_pos]);
                continue;
            }
            // Internal: visit up to node_size children, bounded by this level's start.
            let child_start = self.indices[node_pos];
            let child_level = level - 1;
            let level_end = self.level_bounds[child_level];
            let end = (child_start + self.node_size).min(level_end);
            for child in child_start..end {
                if self.boxes[child].intersects(query) {
                    stack.push((child, child_level));
                }
            }
        }
        out
    }
}

/// Map an `(x, y)` pair (each in `0..=65535`) to its distance `d` along a 16-order
/// Hilbert curve. The standard xy→d transform.
fn hilbert_xy_to_d(mut x: u32, mut y: u32) -> u32 {
    let n: u32 = 1 << 16;
    let mut d: u32 = 0;
    let mut s: u32 = n / 2;
    while s > 0 {
        let rx = if (x & s) > 0 { 1 } else { 0 };
        let ry = if (y & s) > 0 { 1 } else { 0 };
        d = d.wrapping_add(s.wrapping_mul(s).wrapping_mul((3 * rx) ^ ry));
        // Rotate the quadrant.
        if ry == 0 {
            if rx == 1 {
                x = s.wrapping_sub(1).wrapping_sub(x);
                y = s.wrapping_sub(1).wrapping_sub(y);
            }
            std::mem::swap(&mut x, &mut y);
        }
        s /= 2;
    }
    d
}

#[cfg(test)]
mod tests {
    use super::*;

    fn brute_force(items: &[(usize, Bbox)], q: &Bbox) -> Vec<usize> {
        let mut v: Vec<usize> = items
            .iter()
            .filter(|(_, b)| b.intersects(q))
            .map(|(id, _)| *id)
            .collect();
        v.sort_unstable();
        v
    }

    fn sorted(mut v: Vec<usize>) -> Vec<usize> {
        v.sort_unstable();
        v
    }

    #[test]
    fn empty_index() {
        let rt = RTree::build(&[]);
        assert!(rt.is_empty());
        assert!(rt.query_bbox(&Bbox::new(0.0, 0.0, 1.0, 1.0)).is_empty());
    }

    #[test]
    fn small_hand_checked() {
        let items = vec![
            (10, Bbox::new(0.0, 0.0, 1.0, 1.0)),
            (20, Bbox::new(5.0, 5.0, 6.0, 6.0)),
            (30, Bbox::new(2.0, 2.0, 3.0, 3.0)),
            (40, Bbox::new(10.0, 10.0, 11.0, 11.0)),
        ];
        let rt = RTree::build(&items);
        assert_eq!(rt.len(), 4);
        // Query overlapping the first two small boxes.
        let q = Bbox::new(0.5, 0.5, 2.5, 2.5);
        assert_eq!(sorted(rt.query_bbox(&q)), vec![10, 30]);
        // Query hitting only #40.
        assert_eq!(
            sorted(rt.query_bbox(&Bbox::new(10.5, 10.5, 12.0, 12.0))),
            vec![40]
        );
        // Query hitting nothing.
        assert!(rt
            .query_bbox(&Bbox::new(100.0, 100.0, 101.0, 101.0))
            .is_empty());
    }

    #[test]
    fn matches_brute_force_random() {
        // Deterministic LCG (no rand dep) generates pseudo-random boxes.
        let mut seed: u64 = 0x1234_5678_9abc_def0;
        let mut next = || {
            seed = seed
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            ((seed >> 33) as f64) / (u32::MAX as f64) // in [0, ~1)
        };
        let mut items = Vec::new();
        for id in 0..1000usize {
            let x = next() * 1000.0;
            let y = next() * 1000.0;
            let w = next() * 20.0;
            let h = next() * 20.0;
            items.push((id, Bbox::new(x, y, x + w, y + h)));
        }
        // Use a small node_size to force several tree levels.
        let rt = RTree::build_with_node_size(&items, 4);
        assert_eq!(rt.len(), 1000);

        // Many random query windows must match the brute-force scan EXACTLY.
        for _ in 0..200 {
            let qx = next() * 1000.0;
            let qy = next() * 1000.0;
            let qw = next() * 100.0;
            let qh = next() * 100.0;
            let q = Bbox::new(qx, qy, qx + qw, qy + qh);
            assert_eq!(
                sorted(rt.query_bbox(&q)),
                brute_force(&items, &q),
                "mismatch for query {q:?}"
            );
        }
    }
}
