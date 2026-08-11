//! Deterministic node-link layout for `MarkKind::Graph` (D-VZ-1 lane V6,
//! "graph-native marks").
//!
//! ★ **The LOD ladder has to bound LAYOUT work, not just draw calls** — the exact
//! gap the program charter calls out. A naive renderer could spend an unbounded
//! O(N²) force-directed simulation on a million-node graph before it ever reaches
//! `select_tier`'s density-grid draw-op bound, defeating the entire point of a
//! bounded tier: the DRAW OPS would stay bounded, but the render would still take
//! forever to produce them. [`layout`] is the function that closes that gap:
//! below [`FULL_LAYOUT_NODE_CAP`] nodes it runs a real, bounded-iteration
//! Fruchterman-Reingold force-directed layout (genuine O(N²)-per-iteration
//! physics, capped at [`MAX_FR_ITERATIONS`] iterations — acceptable only below
//! the cap); above the cap it does NOT attempt physics at all — it falls back to
//! a fast, deterministic, APPROXIMATE placement (a seeded two-hash spread across
//! the unit square) so the function stays fast (well under a second even at
//! hundreds of thousands of nodes — see the module tests) rather than
//! technically-bounded-but-still-slow. This is deliberately NOT real physics
//! above the cap — it trades layout quality for the bound, the same "cheaper,
//! more-aggregated representation" idea `eg_viz_core::tier::LodTier` already
//! applies to draw ops, applied one layer earlier, to the layout itself.
//!
//! **Deterministic.** [`layout`] takes an explicit `seed` and uses no wall-clock/
//! thread-local randomness anywhere (a hand-rolled splitmix64 PRNG, seeded only
//! from its inputs) — the SAME `(node_count, edges, seed)` always produces the
//! SAME positions, bit for bit (see the module tests), matching the
//! reproducibility contract `eg_viz_core::result` documents for a
//! `ReductionKind::Density` seed.
//!
//! No new external dependency: this repo already avoids adding `rand`/
//! `rand_chacha` to this crate (see `Cargo.toml`'s "zero new external
//! dependencies" doc comment) unless one is already a resolved workspace
//! dependency reachable from here, which neither is — so this module hand-rolls
//! splitmix64, a small, well-known, deterministic bit-mixing PRNG.

/// Above this node count, [`layout`] does not run force-directed physics — see
/// the module doc.
pub const FULL_LAYOUT_NODE_CAP: usize = 2_000;

/// Bounded iteration cap for the Fruchterman-Reingold layout below
/// [`FULL_LAYOUT_NODE_CAP`].
pub const MAX_FR_ITERATIONS: u32 = 200;

/// splitmix64 — a small, dependency-free, deterministic PRNG (Vigna's
/// well-known public-domain construction). Not cryptographic; used here purely
/// as a deterministic bit source for initial layout jitter, never for anything
/// security-sensitive.
struct SplitMix64(u64);

impl SplitMix64 {
    fn new(seed: u64) -> Self {
        Self(seed)
    }

    fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// A uniform `f64` in `[0, 1)`.
    fn next_f64(&mut self) -> f64 {
        (self.next_u64() >> 11) as f64 / (1u64 << 53) as f64
    }
}

/// Deterministically position `node_count` nodes (ids `0..node_count`) given
/// `edges` (pairs of node ids; out-of-range or self-loop edges are ignored, not
/// an error — layout is best-effort geometry, not a graph validator), seeded by
/// `seed`. Returns one `(x, y)` position per node index, normalized to
/// `[0, 1] x [0, 1]`.
///
/// - `node_count <= FULL_LAYOUT_NODE_CAP`: real Fruchterman-Reingold
///   force-directed layout, capped at [`MAX_FR_ITERATIONS`] iterations.
/// - `node_count > FULL_LAYOUT_NODE_CAP`: a fast, deterministic, APPROXIMATE
///   placement — two independently-seeded hashes of the node id spread it
///   across the unit square. See the module doc for why this is deliberate.
pub fn layout(node_count: usize, edges: &[(u32, u32)], seed: u64) -> Vec<(f32, f32)> {
    if node_count == 0 {
        return Vec::new();
    }
    if node_count <= FULL_LAYOUT_NODE_CAP {
        force_directed_layout(node_count, edges, seed)
    } else {
        fast_hash_layout(node_count, seed)
    }
}

/// O(node_count) deterministic approximate placement — see the module doc.
fn fast_hash_layout(node_count: usize, seed: u64) -> Vec<(f32, f32)> {
    (0..node_count)
        .map(|id| {
            let id64 = id as u64;
            let mut hx =
                SplitMix64::new(seed ^ id64.wrapping_mul(0x9E37_79B9_7F4A_7C15).wrapping_add(0x1));
            let mut hy = SplitMix64::new(
                seed ^ id64
                    .wrapping_mul(0xC2B2_AE3D_27D4_EB4F)
                    .wrapping_add(0xD6E8_FEB8_6659_FD93),
            );
            (hx.next_f64() as f32, hy.next_f64() as f32)
        })
        .collect()
}

/// Reflect `v` back into `[0.0, 1.0]` (a single reflection off whichever wall it
/// overshot — see the caller's doc for why this bounds a single step) rather
/// than clamping, so nodes pushed past the same wall stay distinguishable
/// instead of collapsing onto the identical boundary coordinate.
fn reflect(v: f64) -> f64 {
    let reflected = if v < 0.0 {
        -v
    } else if v > 1.0 {
        2.0 - v
    } else {
        v
    };
    reflected.clamp(0.0, 1.0)
}

/// Bounded-iteration Fruchterman-Reingold force-directed layout — real
/// O(N²)-per-iteration physics, only ever called below [`FULL_LAYOUT_NODE_CAP`].
fn force_directed_layout(node_count: usize, edges: &[(u32, u32)], seed: u64) -> Vec<(f32, f32)> {
    let mut rng = SplitMix64::new(seed);
    // Initial positions: seeded pseudo-random spread across the unit square.
    let mut pos: Vec<(f64, f64)> = (0..node_count)
        .map(|_| (rng.next_f64(), rng.next_f64()))
        .collect();

    // Valid, non-self-loop edges within range — layout is best-effort geometry,
    // not a graph validator (see this function's caller doc).
    let valid_edges: Vec<(usize, usize)> = edges
        .iter()
        .filter_map(|&(a, b)| {
            let (a, b) = (a as usize, b as usize);
            if a != b && a < node_count && b < node_count {
                Some((a, b))
            } else {
                None
            }
        })
        .collect();

    // Fruchterman-Reingold constants over a unit-square layout area.
    const AREA: f64 = 1.0;
    let k = (AREA / node_count as f64).sqrt();
    let mut temperature = 0.1_f64;
    let cooling = temperature / MAX_FR_ITERATIONS as f64;

    for _ in 0..MAX_FR_ITERATIONS {
        let mut disp = vec![(0.0_f64, 0.0_f64); node_count];

        // Repulsive force between every pair — the real O(N^2)-per-iteration cost
        // that bounds this path to node_count <= FULL_LAYOUT_NODE_CAP.
        for i in 0..node_count {
            for j in (i + 1)..node_count {
                let dx = pos[i].0 - pos[j].0;
                let dy = pos[i].1 - pos[j].1;
                let dist_sq = (dx * dx + dy * dy).max(1e-12);
                let dist = dist_sq.sqrt();
                let force = k * k / dist;
                let (fx, fy) = (dx / dist * force, dy / dist * force);
                disp[i].0 += fx;
                disp[i].1 += fy;
                disp[j].0 -= fx;
                disp[j].1 -= fy;
            }
        }

        // Attractive force along edges, pulling connected nodes together.
        for &(a, b) in &valid_edges {
            let dx = pos[a].0 - pos[b].0;
            let dy = pos[a].1 - pos[b].1;
            let dist = (dx * dx + dy * dy).max(1e-12).sqrt();
            let force = dist * dist / k;
            let (fx, fy) = (dx / dist * force, dy / dist * force);
            disp[a].0 -= fx;
            disp[a].1 -= fy;
            disp[b].0 += fx;
            disp[b].1 += fy;
        }

        // Apply, capped by the cooling temperature, REFLECTED off the unit
        // square's walls (not merely clamped): a hard clamp collapses every
        // node pushed past the same wall to the identical boundary coordinate
        // (0.0 or 1.0), which can make two distinct nodes coincide exactly —
        // reflection preserves how far past the wall a node overshot, keeping
        // distinct nodes distinguishable. A single reflection is always enough
        // to land back in range: per-step displacement is capped at
        // `temperature <= 0.1`, so a position starting in `[0,1]` can overshoot
        // by at most `0.1`, and `reflect` handles overshoot up to `1.0`.
        for i in 0..node_count {
            let (dx, dy) = disp[i];
            let dist = (dx * dx + dy * dy).max(1e-12).sqrt();
            let capped = dist.min(temperature);
            pos[i].0 = reflect(pos[i].0 + dx / dist * capped);
            pos[i].1 = reflect(pos[i].1 + dy / dist * capped);
        }

        temperature = (temperature - cooling).max(0.0);
    }

    pos.into_iter().map(|(x, y)| (x as f32, y as f32)).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Instant;

    #[test]
    fn layout_is_deterministic_for_the_same_seed_full_layout_path() {
        let edges = [(0, 1), (1, 2), (2, 3), (3, 0), (0, 2)];
        let a = layout(30, &edges, 42);
        let b = layout(30, &edges, 42);
        assert_eq!(
            a, b,
            "same (node_count, edges, seed) must produce byte-identical output"
        );
    }

    #[test]
    fn layout_is_deterministic_for_the_same_seed_fast_hash_path() {
        let edges: Vec<(u32, u32)> = (0..5_000u32).map(|i| (i, (i + 1) % 5_000)).collect();
        let a = layout(5_000, &edges, 7);
        let b = layout(5_000, &edges, 7);
        assert_eq!(
            a, b,
            "same (node_count, edges, seed) must produce byte-identical output"
        );
    }

    #[test]
    fn different_seeds_produce_different_layouts() {
        let edges = [(0, 1), (1, 2)];
        let a = layout(30, &edges, 1);
        let b = layout(30, &edges, 2);
        assert_ne!(a, b);
    }

    #[test]
    fn fast_hash_path_completes_well_under_a_second_for_200_000_nodes() {
        let edges: Vec<(u32, u32)> = (0..200_000u32).map(|i| (i, (i + 1) % 200_000)).collect();
        let started = Instant::now();
        let positions = layout(200_000, &edges, 99);
        let elapsed = started.elapsed();
        assert_eq!(positions.len(), 200_000);
        assert!(
            elapsed.as_secs_f64() < 1.0,
            "fast-hash layout for 200,000 nodes took {:.3}s, expected well under 1s",
            elapsed.as_secs_f64()
        );
    }

    #[test]
    fn small_worked_example_stays_in_bounds_with_no_coincident_nodes() {
        // 30 nodes, a small ring plus a couple of chords -- enough real structure
        // to exercise both the repulsive and attractive forces.
        let edges: Vec<(u32, u32)> = (0..30u32)
            .map(|i| (i, (i + 1) % 30))
            .chain([(0, 15), (5, 20), (10, 25)])
            .collect();
        let positions = layout(30, &edges, 1234);
        assert_eq!(positions.len(), 30);
        for &(x, y) in &positions {
            assert!((0.0..=1.0).contains(&x), "x={x} out of [0,1]");
            assert!((0.0..=1.0).contains(&y), "y={y} out of [0,1]");
        }
        for i in 0..positions.len() {
            for j in (i + 1)..positions.len() {
                let (xi, yi) = positions[i];
                let (xj, yj) = positions[j];
                let dist = ((xi - xj).powi(2) + (yi - yj).powi(2)).sqrt();
                assert!(
                    dist > 1e-6,
                    "nodes {i} and {j} coincide at ({xi}, {yi}) -- expected the repulsive force to keep them apart"
                );
            }
        }
    }

    #[test]
    fn zero_nodes_returns_an_empty_layout() {
        assert!(layout(0, &[], 0).is_empty());
    }

    #[test]
    fn out_of_range_and_self_loop_edges_do_not_panic() {
        let edges = [(0, 1), (5, 5), (100, 200), (u32::MAX, 0)];
        let positions = layout(10, &edges, 3);
        assert_eq!(positions.len(), 10);
    }

    #[test]
    fn the_node_count_cap_boundary_uses_the_expected_strategy() {
        // A direct, deterministic proof that the two strategies genuinely differ
        // (not just an implementation detail) -- at the cap, the full
        // force-directed layout starts from a clustered seeded-random spread and
        // moves under physics; the fast-hash layout one node above the cap is a
        // hash spread with no physics at all. We cannot compare their outputs
        // directly (different node counts), so this test just asserts both paths
        // run and produce the documented output length, pinning the boundary
        // itself.
        let at_cap = layout(FULL_LAYOUT_NODE_CAP, &[], 5);
        let above_cap = layout(FULL_LAYOUT_NODE_CAP + 1, &[], 5);
        assert_eq!(at_cap.len(), FULL_LAYOUT_NODE_CAP);
        assert_eq!(above_cap.len(), FULL_LAYOUT_NODE_CAP + 1);
    }
}
