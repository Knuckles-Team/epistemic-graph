//! Embedding similarity and entity-resolution proposals.

use crate::graph::GraphView;

pub fn compute_similarity_edges(core: &GraphView, threshold: f64) -> Vec<(String, String, f64)> {
    use rayon::prelude::*;

    // Extract nodes with embeddings
    let nodes_with_emb: Vec<(String, Vec<f64>)> = core
        .node_properties
        .iter()
        .filter_map(|(node_id, props_json)| {
            let val: serde_json::Value = serde_json::from_slice(props_json).ok()?;
            let emb = val.get("embedding")?;
            let vec: Vec<f64> = serde_json::from_value(emb.clone()).ok()?;
            if vec.is_empty() {
                return None;
            }
            Some((node_id.clone(), vec))
        })
        .collect();

    if nodes_with_emb.len() < 2 {
        return Vec::new();
    }

    // Parallel pairwise cosine similarity
    let results: Vec<(String, String, f64)> = nodes_with_emb
        .par_iter()
        .enumerate()
        .flat_map(|(i, (id_a, emb_a))| {
            let mut local_edges = Vec::new();
            for (id_b, emb_b) in nodes_with_emb.iter().skip(i + 1) {
                let sim = cosine_similarity(emb_a, emb_b);
                if sim >= threshold {
                    local_edges.push((id_a.clone(), id_b.clone(), sim));
                }
            }
            local_edges
        })
        .collect();

    results
}

/// One proposed entity-resolution action emitted by [`resolve_candidates`].
///
/// `kind` is `"same_as"` (a true-duplicate cluster — safe to merge onto
/// `canonical`) or `"extends"` (a subtype/version relationship between distinct
/// entities — link, don't merge). The op is **read/propose only**: it never
/// mutates the graph, so the client decides whether to apply via `BatchUpdate`.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct MergeProposal {
    pub canonical: String,
    pub members: Vec<String>,
    pub score: f64,
    pub kind: String,
}

/// Native entity-resolution candidate generator (CONCEPT:AU-KG.compute.when-exposes-native).
///
/// Composes the existing embedding + clustering primitives into ONE server-side
/// read op so the agent-utilities dedup ladder's residual escalates here instead
/// of an O(N²) client-side embedding pass:
///   1. collect entity nodes carrying an embedding (optionally filtered by type);
///   2. all-pairs cosine ≥ `sim_threshold` (rayon, off-lock by the caller);
///   3. union-find clusters over SAME-TYPE pairs ≥ `merge_threshold` → `same_as`
///      proposals (canonical = highest-degree member, sift-kg `resolver.py:190`);
///   4. high-sim pairs across DIFFERENT types → `extends` proposals (the
///      duplicates-vs-variants split; the OWL subclass refinement happens in the
///      ontology layer downstream).
///
/// Returns proposals only — applying them is the client's decision.
/// (id, embedding, node_type) for embedded nodes, optionally type-filtered.
/// Split out of `resolve_candidates` (extract-method, cx/wD8) — same terms,
/// same order as before.
fn collect_embedded_nodes(
    core: &GraphView,
    node_type: Option<&str>,
) -> Vec<(String, Vec<f64>, String)> {
    core.node_properties
        .iter()
        .filter_map(|(node_id, props_json)| {
            let val: serde_json::Value = serde_json::from_slice(props_json).ok()?;
            let vec: Vec<f64> = serde_json::from_value(val.get("embedding")?.clone()).ok()?;
            if vec.is_empty() {
                return None;
            }
            let nt = val
                .get("node_type")
                .or_else(|| val.get("type"))
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            if let Some(filter) = node_type {
                if nt != filter {
                    return None;
                }
            }
            Some((node_id.clone(), vec, nt))
        })
        .collect()
}

/// All-pairs cosine ≥ `sim_threshold` (the candidate floor). Split out of
/// `resolve_candidates` (extract-method, cx/wD8) — same terms, same order as
/// before.
fn compute_candidate_pairs(
    nodes: &[(String, Vec<f64>, String)],
    sim_threshold: f64,
) -> Vec<(usize, usize, f64)> {
    use rayon::prelude::*;
    (0..nodes.len())
        .into_par_iter()
        .flat_map(|i| {
            let mut local = Vec::new();
            for j in (i + 1)..nodes.len() {
                let s = cosine_similarity(&nodes[i].1, &nodes[j].1);
                if s >= sim_threshold {
                    local.push((i, j, s));
                }
            }
            local
        })
        .collect()
}

/// Union-find path-halving root lookup. Split out of `resolve_candidates`
/// (extract-method, cx/wD8) — was a nested `fn` there, unchanged.
fn find(parent: &mut [usize], mut x: usize) -> usize {
    while parent[x] != x {
        parent[x] = parent[parent[x]];
        x = parent[x];
    }
    x
}

/// (parent, degree, extends_pairs) — the union-find result of
/// [`build_same_as_clusters`]. The alias keeps this union-find result readable
/// at its call sites (cx/wD8).
type SameAsClusters = (Vec<usize>, Vec<usize>, Vec<(usize, usize, f64)>);

/// Union-find over SAME-TYPE pairs ≥ `merge_threshold` (the same_as bar).
/// Split out of `resolve_candidates` (extract-method, cx/wD8) — same terms,
/// same order as before. Returns (parent, degree, extends_pairs).
fn build_same_as_clusters(
    nodes: &[(String, Vec<f64>, String)],
    pairs: &[(usize, usize, f64)],
    merge_threshold: f64,
) -> SameAsClusters {
    let mut parent: Vec<usize> = (0..nodes.len()).collect();
    let mut degree = vec![0usize; nodes.len()];
    let mut extends_pairs: Vec<(usize, usize, f64)> = Vec::new();
    for &(i, j, s) in pairs {
        degree[i] += 1;
        degree[j] += 1;
        if s >= merge_threshold && nodes[i].2 == nodes[j].2 {
            let (ri, rj) = (find(&mut parent, i), find(&mut parent, j));
            if ri != rj {
                parent[ri] = rj;
            }
        } else {
            // weak (sim below merge bar) OR cross-type high-sim → variant link
            extends_pairs.push((i, j, s));
        }
    }
    (parent, degree, extends_pairs)
}

/// Build `same_as` merge proposals from the union-find clusters. Split out of
/// `resolve_candidates` (extract-method, cx/wD8) — same terms, same order as
/// before, including the write-only `clustered` set (kept as-is; see the
/// lane report).
fn build_same_as_proposals(
    nodes: &[(String, Vec<f64>, String)],
    pairs: &[(usize, usize, f64)],
    parent: &mut [usize],
    degree: &[usize],
) -> Vec<MergeProposal> {
    let mut clusters: std::collections::HashMap<usize, Vec<usize>> =
        std::collections::HashMap::new();
    for idx in 0..nodes.len() {
        let root = find(parent, idx);
        clusters.entry(root).or_default().push(idx);
    }
    let mut proposals: Vec<MergeProposal> = Vec::new();
    let mut clustered: std::collections::HashSet<usize> = std::collections::HashSet::new();
    for members in clusters.values() {
        if members.len() < 2 {
            continue;
        }
        for &m in members {
            clustered.insert(m);
        }
        // canonical = highest-degree member (most corroborating similar neighbours)
        let canonical_idx = *members
            .iter()
            .max_by_key(|&&m| degree[m])
            .unwrap_or(&members[0]);
        let score = pairs
            .iter()
            .filter(|(i, j, _)| members.contains(i) && members.contains(j))
            .map(|(_, _, s)| *s)
            .fold(0.0_f64, f64::max);
        proposals.push(MergeProposal {
            canonical: nodes[canonical_idx].0.clone(),
            members: members.iter().map(|&m| nodes[m].0.clone()).collect(),
            score,
            kind: "same_as".to_string(),
        });
    }
    proposals
}

/// Build `extends` merge proposals for cross-type / weak high-sim pairs not
/// already merged into a `same_as` cluster. Split out of `resolve_candidates`
/// (extract-method, cx/wD8) — same terms, same order as before.
fn build_extends_proposals(
    nodes: &[(String, Vec<f64>, String)],
    extends_pairs: &[(usize, usize, f64)],
    parent: &mut [usize],
) -> Vec<MergeProposal> {
    let mut proposals = Vec::new();
    for &(i, j, s) in extends_pairs {
        if find(parent, i) == find(parent, j) {
            continue; // already in one same_as cluster
        }
        proposals.push(MergeProposal {
            canonical: nodes[i].0.clone(),
            members: vec![nodes[i].0.clone(), nodes[j].0.clone()],
            score: s,
            kind: "extends".to_string(),
        });
    }
    proposals
}

pub fn resolve_candidates(
    core: &GraphView,
    sim_threshold: f64,
    merge_threshold: f64,
    node_type: Option<&str>,
) -> Vec<MergeProposal> {
    // (id, embedding, node_type) for embedded nodes, optionally type-filtered.
    let nodes = collect_embedded_nodes(core, node_type);
    if nodes.len() < 2 {
        return Vec::new();
    }

    // All-pairs cosine ≥ sim_threshold (the candidate floor).
    let pairs = compute_candidate_pairs(&nodes, sim_threshold);
    if pairs.is_empty() {
        return Vec::new();
    }

    let (mut parent, degree, extends_pairs) =
        build_same_as_clusters(&nodes, &pairs, merge_threshold);

    let mut proposals = build_same_as_proposals(&nodes, &pairs, &mut parent, &degree);
    proposals.extend(build_extends_proposals(&nodes, &extends_pairs, &mut parent));
    proposals
}

/// Cosine similarity between two vectors.
fn cosine_similarity(a: &[f64], b: &[f64]) -> f64 {
    if a.len() != b.len() || a.is_empty() {
        return 0.0;
    }
    let dot: f64 = a.iter().zip(b.iter()).map(|(x, y)| x * y).sum();
    let norm_a: f64 = a.iter().map(|x| x * x).sum::<f64>().sqrt();
    let norm_b: f64 = b.iter().map(|x| x * x).sum::<f64>().sqrt();
    if norm_a == 0.0 || norm_b == 0.0 {
        return 0.0;
    }
    dot / (norm_a * norm_b)
}
