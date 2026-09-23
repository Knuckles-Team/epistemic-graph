//! Private HNSW traversal and graph wiring phases.

use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashSet};

use crate::ivfpq::SearchResult;

use super::{Cand, HnswIndex};

pub(super) fn assign_level(index: &HnswIndex, id: u64) -> usize {
    // SplitMix64 finaliser over id ^ seed.
    let mut x = id
        .wrapping_add(index.seed)
        .wrapping_add(0x9E37_79B9_7F4A_7C15);
    x = (x ^ (x >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    x ^= x >> 31;
    // Map the top 53 bits to (0, 1): +1 in numerator and denominator keeps the
    // open interval so -ln(u) is finite and non-negative.
    let mantissa = (x >> 11) as f64;
    let u = (mantissa + 1.0) / ((1u64 << 53) as f64 + 1.0);
    let lvl = (-u.ln() * index.ml).floor();
    (lvl.max(0.0) as usize).min(super::MAX_LEVEL_CAP)
}

#[inline]
fn dist_to(index: &HnswIndex, query: &[f32], node: usize) -> f32 {
    index.metric.distance(query, &index.nodes[node].vector)
}

pub(super) fn greedy_closest(
    index: &HnswIndex,
    query: &[f32],
    entry: usize,
    layer: usize,
) -> usize {
    let mut best = entry;
    let mut best_d = dist_to(index, query, best);
    let mut best_id = index.nodes[best].id;
    loop {
        let mut improved = false;
        for &nb in &index.nodes[best].neighbors[layer] {
            let d = dist_to(index, query, nb);
            let nb_id = index.nodes[nb].id;
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

/// Choose up to `m` neighbours of a base node from `cands` (each scored by its
/// distance to the base) with the neighbour-diversification heuristic (Malkov &
/// Yashunin Algorithm 4). Candidates are taken nearest-first and one is kept only
/// when it is nearer to the base than to every neighbour already kept, so a node
/// spends its edges on distinct directions — including the bridge out of its own
/// cluster — rather than on `m` near-duplicates. Without it, well-separated
/// clusters wire only to themselves, the graph falls apart into islands, and a
/// search that descends into the wrong island never leaves it. Pruned candidates
/// then backfill the free slots nearest-first (`keepPrunedConnections`), so the
/// out-degree stays `m` on dense data. Deterministic: the order is the total
/// `(distance, id, node)` order.
pub(super) fn select_neighbors(index: &HnswIndex, cands: &[Cand], m: usize) -> Vec<usize> {
    let mut ordered: Vec<Cand> = cands.to_vec();
    ordered.sort_unstable();
    let mut kept: Vec<Cand> = Vec::with_capacity(m);
    let mut pruned: Vec<Cand> = Vec::new();
    for candidate in ordered {
        if kept.len() == m {
            break;
        }
        if is_diverse(index, &candidate, &kept) {
            kept.push(candidate);
        } else {
            pruned.push(candidate);
        }
    }
    let free = m - kept.len();
    kept.extend(pruned.into_iter().take(free));
    kept.into_iter().map(|candidate| candidate.node).collect()
}

/// Whether `candidate` is nearer to the base than to every already-kept neighbour.
fn is_diverse(index: &HnswIndex, candidate: &Cand, kept: &[Cand]) -> bool {
    let vector = &index.nodes[candidate.node].vector;
    kept.iter().all(|neighbour| {
        candidate.dist
            < index
                .metric
                .distance(vector, &index.nodes[neighbour.node].vector)
    })
}

/// Re-select an over-full adjacency list with the same heuristic, scored from
/// the node that owns it.
pub(super) fn prune(index: &mut HnswIndex, node: usize, layer: usize, m: usize) {
    if index.nodes[node].neighbors[layer].len() <= m {
        return;
    }
    let base = &index.nodes[node].vector;
    let scored: Vec<Cand> = index.nodes[node].neighbors[layer]
        .iter()
        .map(|&nb| Cand {
            dist: index.metric.distance(base, &index.nodes[nb].vector),
            node: nb,
            id: index.nodes[nb].id,
        })
        .collect();
    let selected = select_neighbors(index, &scored, m);
    index.nodes[node].neighbors[layer] = selected;
}

pub(super) fn search(index: &HnswIndex, query: &[f32], k: usize, ef: usize) -> Vec<SearchResult> {
    search_with_allow(index, query, k, ef, None)
}

pub(super) fn search_filtered(
    index: &HnswIndex,
    query: &[f32],
    k: usize,
    ef: usize,
    allow: Option<&dyn Fn(u64) -> bool>,
) -> Vec<SearchResult> {
    search_with_allow(index, query, k, ef, allow)
}

fn search_with_allow(
    index: &HnswIndex,
    query: &[f32],
    k: usize,
    ef: usize,
    allow: Option<&dyn Fn(u64) -> bool>,
) -> Vec<SearchResult> {
    assert_eq!(query.len(), index.dim, "query length must equal dim");
    if k == 0 {
        return Vec::new();
    }
    let Some(mut ep) = index.entry_point else {
        return Vec::new();
    };
    for layer in (1..=index.max_level).rev() {
        ep = greedy_closest(index, query, ep, layer);
    }
    let ef = ef.max(k);
    let mut found = match allow {
        None => search_layer(index, query, &[ep], ef, 0),
        Some(allow) => search_layer_filtered(index, query, &[ep], ef, 0, allow),
    };
    super::truncate_nearest(&mut found, k);
    found
        .into_iter()
        .map(|candidate| SearchResult {
            id: candidate.id,
            distance: candidate.dist,
        })
        .collect()
}

/// Beam search at one layer (Malkov & Yashunin Algorithm 2). Explores from the
/// entry points and keeps the `ef` nearest nodes found, using exact distances.
pub(super) fn search_layer(
    index: &HnswIndex,
    query: &[f32],
    entries: &[usize],
    ef: usize,
    layer: usize,
) -> Vec<Cand> {
    let (mut visited, mut candidates, mut kept) =
        seed_beam(index, query, entries, ef * 4, ef, None);

    while let Some(Reverse(candidate)) = candidates.pop() {
        if beam_should_stop(&kept, &candidate, ef) {
            break;
        }
        expand_layer_node(
            index,
            query,
            candidate.node,
            layer,
            ef,
            (&mut visited, &mut candidates, &mut kept),
        );
    }
    kept.into_vec()
}

fn seed_beam(
    index: &HnswIndex,
    query: &[f32],
    entries: &[usize],
    capacity: usize,
    limit: usize,
    allow: Option<&dyn Fn(u64) -> bool>,
) -> (HashSet<usize>, BinaryHeap<Reverse<Cand>>, BinaryHeap<Cand>) {
    let mut visited: HashSet<usize> = HashSet::with_capacity(capacity);
    let mut candidates: BinaryHeap<Reverse<Cand>> = BinaryHeap::new();
    let mut kept: BinaryHeap<Cand> = BinaryHeap::new();
    for &entry in entries {
        let candidate = Cand {
            dist: dist_to(index, query, entry),
            node: entry,
            id: index.nodes[entry].id,
        };
        visited.insert(entry);
        candidates.push(Reverse(candidate));
        if allow.is_none_or(|predicate| predicate(candidate.id)) {
            kept.push(candidate);
        }
    }
    trim_kept(&mut kept, limit);
    (visited, candidates, kept)
}

fn trim_kept(kept: &mut BinaryHeap<Cand>, ef: usize) {
    while kept.len() > ef {
        kept.pop();
    }
}

#[inline]
fn beam_should_stop(kept: &BinaryHeap<Cand>, candidate: &Cand, ef: usize) -> bool {
    kept.len() >= ef && kept.peek().is_some_and(|worst| candidate.dist > worst.dist)
}

fn expand_layer_node(
    index: &HnswIndex,
    query: &[f32],
    node: usize,
    layer: usize,
    ef: usize,
    // The beam state threaded through every candidate admitted at this layer:
    // the visited set, the still-to-expand candidate frontier, and the kept
    // (best-so-far) results. The three are always read and mutated together.
    beam: (
        &mut HashSet<usize>,
        &mut BinaryHeap<Reverse<Cand>>,
        &mut BinaryHeap<Cand>,
    ),
) {
    let (visited, candidates, kept) = beam;
    for &neighbor in &index.nodes[node].neighbors[layer] {
        if !visited.insert(neighbor) {
            continue;
        }
        let next = Cand {
            dist: dist_to(index, query, neighbor),
            node: neighbor,
            id: index.nodes[neighbor].id,
        };
        admit_layer_candidate(next, ef, candidates, kept);
    }
}

struct FilteredBeam<'a> {
    ef: usize,
    allow: &'a dyn Fn(u64) -> bool,
    visited: &'a mut HashSet<usize>,
    candidates: &'a mut BinaryHeap<Reverse<Cand>>,
    results: &'a mut BinaryHeap<Cand>,
}

fn admit_layer_candidate(
    next: Cand,
    ef: usize,
    candidates: &mut BinaryHeap<Reverse<Cand>>,
    kept: &mut BinaryHeap<Cand>,
) {
    if kept.len() < ef || kept.peek().is_none_or(|worst| next < *worst) {
        candidates.push(Reverse(next));
        kept.push(next);
        if kept.len() > ef {
            kept.pop();
        }
    }
}

/// Filtered layer-0 traversal. Disallowed nodes remain routing bridges but never
/// enter the result beam.
pub(super) fn search_layer_filtered(
    index: &HnswIndex,
    query: &[f32],
    entries: &[usize],
    ef: usize,
    layer: usize,
    allow: &dyn Fn(u64) -> bool,
) -> Vec<Cand> {
    let (mut visited, mut candidates, mut results) =
        seed_beam(index, query, entries, ef * 8, ef, Some(allow));

    while let Some(Reverse(candidate)) = candidates.pop() {
        if beam_should_stop(&results, &candidate, ef) {
            break;
        }
        expand_filtered_node(
            index,
            query,
            candidate.node,
            layer,
            FilteredBeam {
                ef,
                allow,
                visited: &mut visited,
                candidates: &mut candidates,
                results: &mut results,
            },
        );
    }
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

fn expand_filtered_node(
    index: &HnswIndex,
    query: &[f32],
    node: usize,
    layer: usize,
    beam: FilteredBeam<'_>,
) {
    for &neighbor in &index.nodes[node].neighbors[layer] {
        if !beam.visited.insert(neighbor) {
            continue;
        }
        let distance = dist_to(index, query, neighbor);
        if filtered_candidate_is_promising(beam.results, beam.ef, distance) {
            let next = Cand {
                dist: distance,
                node: neighbor,
                id: index.nodes[neighbor].id,
            };
            beam.candidates.push(Reverse(next));
            admit_filtered_candidate(next, beam.ef, beam.allow, beam.results);
        }
    }
}

#[inline]
fn filtered_candidate_is_promising(results: &BinaryHeap<Cand>, ef: usize, distance: f32) -> bool {
    results.len() < ef || results.peek().is_none_or(|worst| distance < worst.dist)
}

fn admit_filtered_candidate(
    next: Cand,
    ef: usize,
    allow: &dyn Fn(u64) -> bool,
    results: &mut BinaryHeap<Cand>,
) {
    if allow(next.id) {
        results.push(next);
        if results.len() > ef {
            results.pop();
        }
    }
}
