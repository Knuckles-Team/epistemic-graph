// CONCEPT:EG-KG.mining.prefixspan — Sequential-pattern mining.
//
// Pure-Rust, dependency-light, batch (one round-trip): given a set of ORDERED
// item sequences (each a time/position-ordered list of item ids — an item may
// repeat within a sequence), find the frequent sequential patterns — ordered
// subsequences that appear (as a SUBSEQUENCE, not necessarily contiguously) in
// at least `min_support` (a fraction of sequences) of the input.
//
// Two interchangeable engines are provided — **PrefixSpan** (CONCEPT:EG-KG.mining.prefixspan,
// projection-based, no candidate generation) and **GSP** (CONCEPT:EG-KG.mining.gsp,
// Generalized Sequential Pattern — level-wise candidate generation + prune, the
// sequence analog of Apriori). Both are exact and, for the same `min_support`,
// produce the SAME frequent-pattern set (asserted by the parity test): the
// projected-database technique PrefixSpan uses computes the identical support
// count as GSP's direct subsequence-containment scan, just via a faster route.
//
// This module is graph-agnostic: it works over interned `ItemId`s. The handler
// (`src/server/handlers/mining.rs`) does the string↔id interning and the
// graph-derived sequence construction (compute-near-data — each node's ordered
// neighbor list, following the resident edge insertion order, becomes one
// sequence). `mine_labeled` is a convenience that interns `String` items, runs
// the chosen engine, and hands back string-labeled patterns — used by both the
// handler and the unit tests.

use std::collections::{HashMap, HashSet};

/// An interned item id (small, dense — assigned by [`intern`]).
pub type ItemId = u32;

/// Which sequential-pattern engine to run. Both are exact and agree on the output.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Algorithm {
    PrefixSpan,
    Gsp,
}

/// A frequent sequential pattern: its ORDERED items, absolute count (number of
/// input sequences containing it as a subsequence), and fractional support.
#[derive(Debug, Clone, PartialEq)]
pub struct SequentialPattern {
    pub items: Vec<ItemId>,
    pub count: usize,
    pub support: f64,
}

/// A string-labeled pattern (the wire/row shape the handler and client see).
#[derive(Debug, Clone, PartialEq)]
pub struct LabeledPattern {
    pub items: Vec<String>,
    pub count: usize,
    pub support: f64,
}

/// Convert a fractional `min_support` (0.0–1.0) into an absolute minimum count
/// over `n` sequences, clamped to at least 1.
fn min_count(min_support: f64, n: usize) -> usize {
    let raw = (min_support * n as f64).ceil() as usize;
    raw.max(1)
}

/// Whether `sub` occurs in `seq` as an ORDERED subsequence (items need not be
/// contiguous, but their relative order must be preserved).
fn is_subsequence(seq: &[ItemId], sub: &[ItemId]) -> bool {
    let mut si = 0usize;
    if sub.is_empty() {
        return true;
    }
    for &s in seq {
        if s == sub[si] {
            si += 1;
            if si == sub.len() {
                return true;
            }
        }
    }
    false
}

// ─────────────────────────── PrefixSpan ───────────────────────────

/// Frequent sequential patterns via PrefixSpan (CONCEPT:EG-KG.mining.prefixspan):
/// a projection-based, depth-first growth of a `prefix` pattern — no candidate
/// generation. At each step, count the frequent next-items across the current
/// projected database (the suffix of each sequence AFTER its first occurrence
/// of the prefix's last item), emit each frequent extension, and recurse into
/// its own projection. Deterministic (items processed in ascending id order).
pub fn prefixspan(sequences: &[Vec<ItemId>], min_count: usize) -> Vec<SequentialPattern> {
    let n = sequences.len();
    let projected: Vec<&[ItemId]> = sequences.iter().map(|s| s.as_slice()).collect();
    let mut out = Vec::new();
    prefixspan_rec(&projected, &[], min_count, n, &mut out);
    out.sort_by(|a, b| {
        a.items
            .len()
            .cmp(&b.items.len())
            .then(a.items.cmp(&b.items))
    });
    out
}

fn prefixspan_rec<'a>(
    projected: &[&'a [ItemId]],
    prefix: &[ItemId],
    min_count: usize,
    n: usize,
    out: &mut Vec<SequentialPattern>,
) {
    // Distinct items appearing in each projected sequence (once per sequence —
    // repeats within one sequence must not inflate the count).
    let mut counts: HashMap<ItemId, usize> = HashMap::new();
    for seq in projected {
        let mut seen: Vec<ItemId> = seq.to_vec();
        seen.sort_unstable();
        seen.dedup();
        for it in seen {
            *counts.entry(it).or_insert(0) += 1;
        }
    }
    let mut freq: Vec<(ItemId, usize)> = counts
        .into_iter()
        .filter(|&(_, c)| c >= min_count)
        .collect();
    freq.sort_unstable();

    for (item, count) in freq {
        let mut pattern = prefix.to_vec();
        pattern.push(item);
        out.push(SequentialPattern {
            items: pattern.clone(),
            count,
            support: count as f64 / n as f64,
        });

        // Project: for each sequence containing `item`, take the suffix strictly
        // after its FIRST occurrence.
        let mut new_projected: Vec<&'a [ItemId]> = Vec::new();
        for seq in projected {
            if let Some(pos) = seq.iter().position(|&x| x == item) {
                new_projected.push(&seq[pos + 1..]);
            }
        }
        if !new_projected.is_empty() {
            prefixspan_rec(&new_projected, &pattern, min_count, n, out);
        }
    }
}

// ─────────────────────────── GSP ───────────────────────────

/// Frequent sequential patterns via GSP (CONCEPT:EG-KG.mining.gsp — Generalized
/// Sequential Pattern): a level-wise breadth-first candidate-generation + prune
/// loop, the sequence analog of Apriori. Candidates of length k are formed by
/// joining two frequent (k-1)-patterns whose overlap matches (drop the first
/// item of one, the last of the other), then support is counted by a direct
/// subsequence-containment scan and every contiguous (k-1)-subsequence of a
/// candidate must itself be frequent (downward closure) before it is counted.
/// Deterministic.
pub fn gsp(sequences: &[Vec<ItemId>], min_count: usize) -> Vec<SequentialPattern> {
    let n = sequences.len();
    let mut all: Vec<SequentialPattern> = Vec::new();

    // L1: singleton counts (once per sequence, like PrefixSpan's projection).
    let mut counts: HashMap<ItemId, usize> = HashMap::new();
    for seq in sequences {
        let mut seen = seq.clone();
        seen.sort_unstable();
        seen.dedup();
        for it in seen {
            *counts.entry(it).or_insert(0) += 1;
        }
    }
    let mut current: Vec<Vec<ItemId>> = Vec::new();
    let mut singles: Vec<(ItemId, usize)> = counts
        .into_iter()
        .filter(|&(_, c)| c >= min_count)
        .collect();
    singles.sort_unstable();
    for (item, count) in singles {
        current.push(vec![item]);
        all.push(SequentialPattern {
            items: vec![item],
            count,
            support: count as f64 / n as f64,
        });
    }

    while !current.is_empty() {
        let freq_set: HashSet<Vec<ItemId>> = current.iter().cloned().collect();
        let mut seen_candidates: HashSet<Vec<ItemId>> = HashSet::new();
        let mut candidates: Vec<Vec<ItemId>> = Vec::new();
        for a in &current {
            for b in &current {
                let k = a.len();
                // Join: a's tail (dropping its first item) must equal b's head
                // (dropping its last item) — the standard GSP join.
                if a[1..] == b[..k - 1] {
                    let mut cand = a.clone();
                    cand.push(*b.last().unwrap());
                    if seen_candidates.insert(cand.clone()) {
                        candidates.push(cand);
                    }
                }
            }
        }
        candidates.sort();

        let mut next: Vec<Vec<ItemId>> = Vec::new();
        for cand in candidates {
            if !all_contiguous_subseqs_frequent(&cand, &freq_set) {
                continue;
            }
            let count = sequences
                .iter()
                .filter(|s| is_subsequence(s, &cand))
                .count();
            if count >= min_count {
                next.push(cand.clone());
                all.push(SequentialPattern {
                    items: cand,
                    count,
                    support: count as f64 / n as f64,
                });
            }
        }
        current = next;
    }
    all.sort_by(|a, b| {
        a.items
            .len()
            .cmp(&b.items.len())
            .then(a.items.cmp(&b.items))
    });
    all
}

/// Downward-closure prune: every (k-1)-length pattern obtained by dropping ONE
/// position from `cand` must be a member of the previous frequent set — mirrors
/// `association::all_subsets_frequent`, adapted to ordered sequences (position,
/// not subset).
fn all_contiguous_subseqs_frequent(cand: &[ItemId], freq_set: &HashSet<Vec<ItemId>>) -> bool {
    if cand.len() <= 1 {
        return true;
    }
    for drop in 0..cand.len() {
        let sub: Vec<ItemId> = cand
            .iter()
            .enumerate()
            .filter_map(|(i, &x)| (i != drop).then_some(x))
            .collect();
        if !freq_set.contains(&sub) {
            return false;
        }
    }
    true
}

/// Run the chosen sequential-pattern engine.
pub fn mine(
    sequences: &[Vec<ItemId>],
    min_support: f64,
    algorithm: Algorithm,
) -> Vec<SequentialPattern> {
    let mc = min_count(min_support, sequences.len().max(1));
    match algorithm {
        Algorithm::PrefixSpan => prefixspan(sequences, mc),
        Algorithm::Gsp => gsp(sequences, mc),
    }
}

// ─────────────────────────── String-labeled convenience ───────────────────────────

/// Intern string sequences into dense [`ItemId`]s, preserving a stable
/// id↔label mapping (first-seen order) and sequence order.
pub fn intern(sequences: &[Vec<String>]) -> (Vec<Vec<ItemId>>, Vec<String>) {
    let mut labels: Vec<String> = Vec::new();
    let mut index: HashMap<String, ItemId> = HashMap::new();
    let mut out: Vec<Vec<ItemId>> = Vec::with_capacity(sequences.len());
    for seq in sequences {
        let mut row: Vec<ItemId> = Vec::with_capacity(seq.len());
        for item in seq {
            let id = *index.entry(item.clone()).or_insert_with(|| {
                let id = labels.len() as ItemId;
                labels.push(item.clone());
                id
            });
            row.push(id);
        }
        out.push(row);
    }
    (out, labels)
}

/// Mine string-labeled sequences: intern → mine → relabel.
pub fn mine_labeled(
    sequences: &[Vec<String>],
    min_support: f64,
    algorithm: Algorithm,
) -> Vec<LabeledPattern> {
    let (interned, labels) = intern(sequences);
    let patterns = mine(&interned, min_support, algorithm);
    patterns
        .into_iter()
        .map(|p| LabeledPattern {
            items: p
                .items
                .iter()
                .map(|&i| labels[i as usize].clone())
                .collect(),
            count: p.count,
            support: p.support,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A planted pattern `[A, B, C]` (0,1,2) embedded (with noise interleaved) in
    /// 6 of 8 sequences; the rest are noise-only. Fixed, hand-built fixture (no
    /// randomness needed — these algorithms are deterministic).
    fn fixture() -> Vec<Vec<ItemId>> {
        vec![
            vec![0, 9, 1, 8, 2],    // A . B . C  (contains 0,1,2 in order)
            vec![0, 1, 2],          // A B C
            vec![9, 0, 8, 1, 7, 2], // noise A noise B noise C
            vec![0, 1, 9, 2, 8],    // A B . C .
            vec![0, 8, 1, 2, 9],    // A . B C .
            vec![0, 1, 2, 2],       // A B C C
            vec![9, 8, 7],          // pure noise — no planted pattern
            vec![2, 1, 0],          // reversed — does NOT contain 0,1,2 in order
        ]
    }

    #[test]
    fn prefixspan_recovers_planted_pattern() {
        // 6/8 = 0.75 sequences contain [0,1,2] as a subsequence.
        let patterns = prefixspan(&fixture(), min_count(0.5, 8));
        let hit = patterns.iter().find(|p| p.items == vec![0, 1, 2]);
        assert!(hit.is_some(), "planted pattern [0,1,2] not recovered");
        assert_eq!(hit.unwrap().count, 6);
        assert!((hit.unwrap().support - 0.75).abs() < 1e-9);
    }

    #[test]
    fn gsp_recovers_planted_pattern() {
        let patterns = gsp(&fixture(), min_count(0.5, 8));
        let hit = patterns.iter().find(|p| p.items == vec![0, 1, 2]);
        assert!(
            hit.is_some(),
            "planted pattern [0,1,2] not recovered by GSP"
        );
        assert_eq!(hit.unwrap().count, 6);
        assert!((hit.unwrap().support - 0.75).abs() < 1e-9);
    }

    /// PrefixSpan == GSP on the same threshold (the parity gate, mirroring
    /// association.rs's three-engine agreement test).
    #[test]
    fn prefixspan_and_gsp_agree() {
        let seqs = fixture();
        let canon = |mut v: Vec<SequentialPattern>| {
            v.sort_by(|a, b| a.items.cmp(&b.items));
            v.into_iter()
                .map(|p| (p.items, p.count))
                .collect::<Vec<_>>()
        };
        for mc in 1..=6 {
            let p = canon(prefixspan(&seqs, mc));
            let g = canon(gsp(&seqs, mc));
            assert_eq!(p, g, "prefixspan vs gsp diverged at min_count {mc}");
        }
    }

    #[test]
    fn is_subsequence_checks_order_not_contiguity() {
        assert!(is_subsequence(&[0, 9, 1, 8, 2], &[0, 1, 2]));
        assert!(!is_subsequence(&[2, 1, 0], &[0, 1, 2]));
        assert!(is_subsequence(&[0, 1, 2], &[]));
        assert!(!is_subsequence(&[0, 1], &[0, 1, 2]));
    }

    #[test]
    fn min_support_filters_infrequent_patterns() {
        let seqs = fixture();
        let loose = mine(&seqs, 0.1, Algorithm::PrefixSpan);
        let strict = mine(&seqs, 0.9, Algorithm::PrefixSpan);
        assert!(strict.len() < loose.len());
        assert!(strict.iter().all(|p| p.support >= 0.9 - 1e-12));
    }

    #[test]
    fn labeled_roundtrip_produces_string_patterns() {
        let seqs = vec![
            vec!["login".into(), "browse".into(), "purchase".into()],
            vec![
                "login".into(),
                "search".into(),
                "browse".into(),
                "purchase".into(),
            ],
            vec!["login".into(), "browse".into()],
            vec!["login".into(), "browse".into(), "purchase".into()],
        ];
        let patterns = mine_labeled(&seqs, 0.5, Algorithm::PrefixSpan);
        assert!(!patterns.is_empty());
        let hit = patterns
            .iter()
            .find(|p| p.items == vec!["login".to_string(), "browse".to_string()]);
        assert!(hit.is_some());
        assert_eq!(hit.unwrap().count, 4);
    }
}
