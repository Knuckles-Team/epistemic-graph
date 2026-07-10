// CONCEPT:EG-KG.mining.entity-resolution — Entity resolution + record linkage.
//
// Pure-Rust, dependency-light: given a set of records, find which PAIRS refer to
// the SAME real-world entity. Two input shapes are supported, sharing one
// blocking + pairwise-similarity pipeline:
//
//   * **Record linkage** (`link_records`) — each record is a set of string
//     attribute tokens (e.g. normalized name/address/email tokens); similarity is
//     **Jaccard** over the token sets.
//   * **Entity resolution over embeddings** (`resolve_entities`) — each record is
//     a float feature vector (e.g. a node embedding); similarity is **Cosine**.
//
// Both cut the naive O(n²) pairwise comparison down to same-block pairs only
// (the standard blocking speedup): `link_records` blocks by an explicit
// caller-supplied key per record (e.g. a normalized last name or postal code);
// `resolve_entities` blocks by a coarse grid bucket over each vector (rounding
// every dimension to `bucket_precision` decimals — a documented approximation:
// two near-duplicate vectors that straddle a bucket boundary can be missed, the
// classic grid-blocking trade-off traded for O(n) instead of O(n²) blocking).
// Pairs at or above `threshold` are emitted as matches, sorted by descending
// similarity for a stable, useful presentation order.

use std::collections::HashMap;

/// One matched pair: the two record indices (`left < right`), their similarity in
/// `[0,1]`, and the block key that brought them together (for provenance).
#[derive(Debug, Clone, PartialEq)]
pub struct EntityMatch {
    pub left: usize,
    pub right: usize,
    pub similarity: f64,
    pub block_key: String,
}

/// Jaccard similarity between two token sets: `|A∩B| / |A∪B|`, `0.0` when both
/// are empty (no signal either way).
fn jaccard(a: &[String], b: &[String]) -> f64 {
    use std::collections::HashSet;
    let sa: HashSet<&String> = a.iter().collect();
    let sb: HashSet<&String> = b.iter().collect();
    let inter = sa.intersection(&sb).count();
    let union = sa.union(&sb).count();
    if union == 0 {
        0.0
    } else {
        inter as f64 / union as f64
    }
}

/// Cosine similarity between two equal-length vectors; `0.0` when either norm is
/// zero (undefined direction).
fn cosine(a: &[f64], b: &[f64]) -> f64 {
    let dot: f64 = a.iter().zip(b).map(|(x, y)| x * y).sum();
    let na: f64 = a.iter().map(|x| x * x).sum::<f64>().sqrt();
    let nb: f64 = b.iter().map(|x| x * x).sum::<f64>().sqrt();
    if na == 0.0 || nb == 0.0 {
        0.0
    } else {
        dot / (na * nb)
    }
}

/// Run one blocked pairwise-similarity pass: group indices by `keys[i]`, compare
/// every same-block pair with `sim`, keep pairs `>= threshold`, and return them in
/// a stable descending-similarity order.
fn blocked_pairs<F: Fn(usize, usize) -> f64>(
    n: usize,
    keys: &[String],
    threshold: f64,
    sim: F,
) -> Vec<EntityMatch> {
    let mut blocks: HashMap<&str, Vec<usize>> = HashMap::new();
    for (i, key) in keys.iter().enumerate().take(n) {
        blocks.entry(key.as_str()).or_default().push(i);
    }
    let mut block_order: Vec<&str> = blocks.keys().copied().collect();
    block_order.sort_unstable();

    let mut out = Vec::new();
    for key in block_order {
        let members = &blocks[key];
        for i in 0..members.len() {
            for j in (i + 1)..members.len() {
                let (a, b) = (members[i], members[j]);
                let s = sim(a, b);
                if s + 1e-12 >= threshold {
                    out.push(EntityMatch {
                        left: a.min(b),
                        right: a.max(b),
                        similarity: s,
                        block_key: key.to_string(),
                    });
                }
            }
        }
    }
    out.sort_by(|x, y| {
        y.similarity
            .partial_cmp(&x.similarity)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then(x.left.cmp(&y.left))
            .then(x.right.cmp(&y.right))
    });
    out
}

/// **Record linkage** (CONCEPT:EG-KG.mining.entity-resolution): Jaccard-similarity
/// matching over token-attribute records, blocked by an explicit per-record key
/// (`block_keys[i]` — same length as `records`; an empty string for every record
/// degenerates to ONE global block, i.e. no blocking).
pub fn link_records(
    records: &[Vec<String>],
    block_keys: &[String],
    threshold: f64,
) -> Vec<EntityMatch> {
    blocked_pairs(records.len(), block_keys, threshold, |a, b| {
        jaccard(&records[a], &records[b])
    })
}

/// **Entity resolution over embeddings** (CONCEPT:EG-KG.mining.entity-resolution):
/// Cosine-similarity matching over float feature vectors, blocked by a grid bucket
/// over each vector rounded to `bucket_precision` decimals (e.g. `1` ⇒ buckets of
/// width `0.1` per dimension).
pub fn resolve_entities(
    vectors: &[Vec<f64>],
    bucket_precision: i32,
    threshold: f64,
) -> Vec<EntityMatch> {
    let scale = 10f64.powi(bucket_precision);
    let keys: Vec<String> = vectors
        .iter()
        .map(|v| {
            v.iter()
                .map(|x| format!("{:.*}", bucket_precision.max(0) as usize, (x * scale).round() / scale))
                .collect::<Vec<_>>()
                .join(",")
        })
        .collect();
    blocked_pairs(vectors.len(), &keys, threshold, |a, b| {
        cosine(&vectors[a], &vectors[b])
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn link_records_matches_overlapping_tokens_within_block() {
        let records = vec![
            vec!["john".into(), "smith".into(), "12345".into()],
            vec!["jon".into(), "smith".into(), "12345".into()], // near-dup, same block
            vec!["mary".into(), "jones".into(), "99999".into()], // unrelated, different block
        ];
        let blocks = vec!["smith".to_string(), "smith".to_string(), "jones".to_string()];
        let matches = link_records(&records, &blocks, 0.4);
        assert_eq!(matches.len(), 1);
        assert_eq!((matches[0].left, matches[0].right), (0, 1));
        assert!(matches[0].similarity > 0.4);
    }

    #[test]
    fn link_records_never_compares_across_blocks() {
        let records = vec![
            vec!["a".into(), "b".into()],
            vec!["a".into(), "b".into()], // identical tokens, DIFFERENT block
        ];
        let blocks = vec!["block1".to_string(), "block2".to_string()];
        let matches = link_records(&records, &blocks, 0.1);
        assert!(matches.is_empty(), "cross-block pairs must never be compared");
    }

    #[test]
    fn resolve_entities_matches_near_identical_vectors() {
        let vectors = vec![
            vec![1.0, 2.0],
            vec![1.01, 2.01],  // near-duplicate, same rounded bucket
            vec![10.0, -10.0], // unrelated
        ];
        let matches = resolve_entities(&vectors, 1, 0.9);
        assert_eq!(matches.len(), 1);
        assert_eq!((matches[0].left, matches[0].right), (0, 1));
        assert!(matches[0].similarity > 0.99);
    }

    #[test]
    fn threshold_filters_weak_matches() {
        let records = vec![
            vec!["a".into(), "b".into(), "c".into()],
            vec!["a".into()], // Jaccard = 1/3
        ];
        let blocks = vec!["x".to_string(), "x".to_string()];
        assert!(link_records(&records, &blocks, 0.5).is_empty());
        assert_eq!(link_records(&records, &blocks, 0.3).len(), 1);
    }
}
