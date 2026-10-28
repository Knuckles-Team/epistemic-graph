//! Optional, fail-closed HNSW adjacency pruning.
//!
//! Pruning is deliberately kept out of the default build path.  The parent HNSW
//! module owns activation and API re-export; this child only implements the
//! bounded transformation.  A caller supplies an explicit policy and receives a
//! cloned graph, so a policy, validation, budget, or reachability failure can
//! never partially mutate the source graph.

use super::HnswIndex;
use std::collections::VecDeque;
use std::fmt;

/// Bounds used by [`prune_hub_preserving`].
///
/// `hub_threshold` identifies the high-degree nodes that are allowed the larger
/// `hub_degree` cap.  Nodes below that threshold use `ordinary_degree`.  All
/// values are explicit: an empty/default policy is not an activation path.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PrunePolicy {
    /// Explicit opt-in. `new` creates a disabled identity policy.
    enabled: bool,
    ordinary_degree: usize,
    hub_degree: usize,
    hub_threshold: usize,
    max_work: usize,
    /// Caller-bound policy identity. The root manifest owns the revision and
    /// digest; this module only requires the binding before enabled execution.
    policy_revision: Option<u64>,
    policy_digest: Option<[u8; 32]>,
}

impl PrunePolicy {
    /// Build an explicit pruning policy.
    ///
    /// `max_work` bounds candidate scoring and reachability traversal.  The
    /// graph's own HNSW/artifact degree limits are checked when the policy is
    /// applied because they depend on the source index.
    pub const fn new(
        ordinary_degree: usize,
        hub_degree: usize,
        hub_threshold: usize,
        max_work: usize,
    ) -> Self {
        Self {
            enabled: false,
            ordinary_degree,
            hub_degree,
            hub_threshold,
            max_work,
            policy_revision: None,
            policy_digest: None,
        }
    }

    /// Opt into pruning explicitly. A disabled policy remains an identity
    /// operation even when its degree fields are otherwise invalid.
    pub const fn with_enabled(mut self, enabled: bool) -> Self {
        self.enabled = enabled;
        self
    }

    /// Bind the caller's versioned policy manifest identity. The root manifest
    /// computes these values; pruning does not invent or persist provenance.
    pub const fn with_provenance(mut self, policy_revision: u64, policy_digest: [u8; 32]) -> Self {
        self.policy_revision = Some(policy_revision);
        self.policy_digest = Some(policy_digest);
        self
    }

    pub const fn enabled(self) -> bool {
        self.enabled
    }

    pub const fn provenance(self) -> Option<(u64, [u8; 32])> {
        match (self.policy_revision, self.policy_digest) {
            (Some(revision), Some(digest)) => Some((revision, digest)),
            _ => None,
        }
    }

    pub const fn ordinary_degree(self) -> usize {
        self.ordinary_degree
    }

    pub const fn hub_degree(self) -> usize {
        self.hub_degree
    }

    pub const fn hub_threshold(self) -> usize {
        self.hub_threshold
    }

    pub const fn max_work(self) -> usize {
        self.max_work
    }
}

/// Refusal reasons for the bounded pruning transform.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PruneError {
    /// The caller supplied a policy that cannot describe a bounded graph.
    InvalidPolicy { reason: &'static str },
    /// The source or transformed graph failed the existing artifact contract.
    InvalidGraph { reason: String },
    /// The operation would exceed the policy's explicit work budget.
    BudgetExceeded { required: usize, budget: usize },
    /// A node that exists at `layer` is no longer reachable from the entry point.
    Unreachable { layer: usize, node: usize },
}

impl fmt::Display for PruneError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidPolicy { reason } => write!(f, "invalid HNSW prune policy: {reason}"),
            Self::InvalidGraph { reason } => write!(f, "invalid HNSW graph: {reason}"),
            Self::BudgetExceeded { required, budget } => write!(
                f,
                "HNSW prune work budget exceeded: required {required}, budget {budget}"
            ),
            Self::Unreachable { layer, node } => {
                write!(f, "HNSW node {node} is unreachable at layer {layer}")
            }
        }
    }
}

impl std::error::Error for PruneError {}

/// Apply an explicitly requested, deterministic, hub-preserving adjacency cap.
///
/// The source graph is validated through the existing artifact codec before any
/// clone is changed.  Candidates are ranked by the existing exact metric and
/// then by external id/internal index for stable ties.  The clone is validated
/// again and checked for entry-point reachability at every layer before it is
/// returned.  Any error drops the clone and leaves `index` untouched.
pub fn prune_hub_preserving(
    index: &HnswIndex,
    policy: PrunePolicy,
) -> Result<HnswIndex, PruneError> {
    if !policy.enabled() {
        return Ok(index.clone());
    }
    validate_policy(index, policy)?;
    validate_artifact(index)?;

    // Work is charged while scoring candidate edges and walking each layer.  A
    // low budget may fail after the private clone has changed; the source graph
    // remains immutable throughout this function.
    let mut work = WorkBudget::new(policy.max_work());
    let mut candidate = index.clone();
    let hub_flags = classify_hubs(&candidate, policy, &mut work)?;

    for (node_idx, node_hub_flags) in hub_flags.iter().enumerate().take(candidate.nodes.len()) {
        let layer_count = candidate.nodes[node_idx].neighbors.len();
        for (layer, &is_hub) in node_hub_flags.iter().enumerate().take(layer_count) {
            work.charge(1)?;
            let source = candidate.nodes[node_idx].neighbors[layer].clone();
            work.charge(source.len())?;

            let graph_cap = if layer == 0 {
                candidate.m0
            } else {
                candidate.m
            };
            let requested_cap = if is_hub {
                policy.hub_degree()
            } else {
                policy.ordinary_degree()
            };
            let cap = requested_cap.min(graph_cap);

            let base = &candidate.nodes[node_idx].vector;
            let metric = candidate.metric;
            let mut scored: Vec<(usize, f32, u64)> = source
                .into_iter()
                .map(|neighbor| {
                    let distance = metric.distance(base, &candidate.nodes[neighbor].vector);
                    (neighbor, distance, candidate.nodes[neighbor].id)
                })
                .collect();
            scored.sort_unstable_by(|left, right| {
                left.1
                    .total_cmp(&right.1)
                    .then_with(|| left.2.cmp(&right.2))
                    .then_with(|| left.0.cmp(&right.0))
            });
            scored.truncate(cap);
            candidate.nodes[node_idx].neighbors[layer] = scored
                .into_iter()
                .map(|(neighbor, _, _)| neighbor)
                .collect();
        }
    }

    check_reachability(&candidate, &mut work)?;
    validate_artifact(&candidate)?;
    Ok(candidate)
}

fn validate_policy(index: &HnswIndex, policy: PrunePolicy) -> Result<(), PruneError> {
    let Some((policy_revision, policy_digest)) = policy.provenance() else {
        return Err(PruneError::InvalidPolicy {
            reason: "enabled pruning requires caller-bound policy provenance",
        });
    };
    if policy_revision == 0 || policy_digest == [0; 32] {
        return Err(PruneError::InvalidPolicy {
            reason: "policy provenance must include a nonzero revision and digest",
        });
    }
    if policy.ordinary_degree() == 0 {
        return Err(PruneError::InvalidPolicy {
            reason: "ordinary_degree must be positive",
        });
    }
    if policy.hub_degree() < policy.ordinary_degree() {
        return Err(PruneError::InvalidPolicy {
            reason: "hub_degree must be at least ordinary_degree",
        });
    }
    if policy.hub_threshold() <= policy.ordinary_degree() {
        return Err(PruneError::InvalidPolicy {
            reason: "hub_threshold must exceed ordinary_degree",
        });
    }
    if policy.max_work() == 0 {
        return Err(PruneError::InvalidPolicy {
            reason: "max_work must be positive",
        });
    }
    // A policy that cannot fit the dense layer's artifact bound is rejected up
    // front. Upper layers may have a smaller source cap and are clamped to that
    // source cap below.
    if policy.ordinary_degree() > index.m0 || policy.hub_degree() > index.m0 {
        return Err(PruneError::InvalidPolicy {
            reason: "degree policy exceeds the source layer-0 bound",
        });
    }
    Ok(())
}

fn validate_artifact(index: &HnswIndex) -> Result<(), PruneError> {
    super::artifact::encode(index)
        .map(|_| ())
        .map_err(|error| PruneError::InvalidGraph {
            reason: error.to_string(),
        })
}

fn classify_hubs(
    index: &HnswIndex,
    policy: PrunePolicy,
    work: &mut WorkBudget,
) -> Result<Vec<Vec<bool>>, PruneError> {
    let mut hubs = Vec::with_capacity(index.nodes.len());
    for node in &index.nodes {
        let mut levels = Vec::with_capacity(node.neighbors.len());
        for neighbors in &node.neighbors {
            work.charge(1)?;
            levels.push(neighbors.len() >= policy.hub_threshold());
        }
        hubs.push(levels);
    }
    Ok(hubs)
}

fn check_reachability(index: &HnswIndex, work: &mut WorkBudget) -> Result<(), PruneError> {
    if index.nodes.is_empty() {
        return Ok(());
    }
    let entry = index.entry_point.ok_or_else(|| PruneError::InvalidGraph {
        reason: "nonempty graph has no entry point".to_owned(),
    })?;

    for layer in 0..=index.max_level {
        if index.nodes[entry].neighbors.len() <= layer {
            return Err(PruneError::InvalidGraph {
                reason: "entry point does not exist at every graph layer".to_owned(),
            });
        }
        let mut visited = vec![false; index.nodes.len()];
        let mut queue = VecDeque::new();
        visited[entry] = true;
        queue.push_back(entry);

        while let Some(node_idx) = queue.pop_front() {
            work.charge(1)?;
            for &neighbor in &index.nodes[node_idx].neighbors[layer] {
                work.charge(1)?;
                if neighbor >= index.nodes.len() || index.nodes[neighbor].neighbors.len() <= layer {
                    return Err(PruneError::InvalidGraph {
                        reason: "edge points outside its layer".to_owned(),
                    });
                }
                if !visited[neighbor] {
                    visited[neighbor] = true;
                    queue.push_back(neighbor);
                }
            }
        }

        for (node_idx, node) in index.nodes.iter().enumerate() {
            if node.neighbors.len() > layer && !visited[node_idx] {
                return Err(PruneError::Unreachable {
                    layer,
                    node: node_idx,
                });
            }
        }
    }
    Ok(())
}

struct WorkBudget {
    used: usize,
    budget: usize,
}

impl WorkBudget {
    const fn new(budget: usize) -> Self {
        Self { used: 0, budget }
    }

    fn charge(&mut self, units: usize) -> Result<(), PruneError> {
        let required = self.used.saturating_add(units);
        if required > self.budget {
            return Err(PruneError::BudgetExceeded {
                required,
                budget: self.budget,
            });
        }
        self.used = required;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::flat::{FlatIndex, Metric};
    use crate::hnsw::artifact;
    use crate::recall::recall_at_k;

    fn policy(max_work: usize) -> PrunePolicy {
        // Node 0 in the fixture is the only hub (degree 31); node 1 is an
        // ordinary degree-3 node and is reduced to two neighbors.
        PrunePolicy::new(2, 32, 16, max_work)
            .with_enabled(true)
            .with_provenance(1, [7; 32])
    }

    fn fixture() -> HnswIndex {
        let nodes = (0..32)
            .map(|id| super::super::Node {
                id,
                vector: vec![
                    id as f32,
                    (id * 3 % 11) as f32,
                    (id * 5 % 7) as f32,
                    (id * 7 % 13) as f32,
                ],
                neighbors: if id == 0 {
                    vec![(1..32).collect()]
                } else if id == 1 {
                    vec![vec![0, 2, 3]]
                } else {
                    vec![vec![0]]
                },
            })
            .collect();
        HnswIndex {
            dim: 4,
            metric: Metric::L2,
            m: 16,
            m0: 32,
            ef_construction: 32,
            seed: 7,
            ml: 1.0 / (16.0_f64).ln(),
            entry_point: Some(0),
            max_level: 0,
            nodes,
        }
    }

    fn graph_debug(index: &HnswIndex) -> String {
        format!("{index:?}")
    }

    #[test]
    fn pruning_preserves_hub_bounds_and_artifact_round_trip() {
        let before = fixture();
        let before_results = before.search(&[3.1, 4.0, 2.0, 7.0], 4, 64);
        let pruned = before.pruned_with_policy(policy(20_000)).unwrap();

        assert_eq!(pruned.nodes[0].neighbors[0].len(), 31);
        assert_eq!(pruned.nodes[1].neighbors[0].len(), 2);
        assert!(pruned
            .nodes
            .iter()
            .all(|node| node.neighbors[0].len() <= 32));

        let encoded = artifact::encode(&pruned).unwrap();
        let decoded = artifact::decode(&encoded).unwrap();
        assert_eq!(artifact::encode(&decoded).unwrap(), encoded);
        assert_eq!(
            decoded.search(&[3.1, 4.0, 2.0, 7.0], 4, 64),
            pruned.search(&[3.1, 4.0, 2.0, 7.0], 4, 64)
        );
        assert_eq!(before_results, pruned.search(&[3.1, 4.0, 2.0, 7.0], 4, 64));
    }

    #[test]
    fn pruning_retains_recall_against_flat_fixture() {
        let before = fixture();
        let items: Vec<(u64, Vec<f32>)> = before
            .nodes
            .iter()
            .map(|node| (node.id, node.vector.clone()))
            .collect();
        let mut flat = FlatIndex::new(4);
        flat.add(&items);
        let pruned = before.pruned_with_policy(policy(20_000)).unwrap();

        let mut baseline = 0.0;
        let mut after = 0.0;
        for query in [
            [0.2, 0.1, 0.0, 0.5],
            [3.1, 4.0, 2.0, 7.0],
            [6.2, 1.2, 5.0, 4.0],
            [17.0, 0.0, 3.0, 11.0],
            [31.0, 1.0, 6.0, 5.0],
        ] {
            let truth: Vec<u64> = flat
                .search(&query, 8, Metric::L2)
                .into_iter()
                .map(|result| result.id)
                .collect();
            let before_ids: Vec<u64> = before
                .search(&query, 8, 64)
                .into_iter()
                .map(|result| result.id)
                .collect();
            let after_ids: Vec<u64> = pruned
                .search(&query, 8, 64)
                .into_iter()
                .map(|result| result.id)
                .collect();
            baseline += recall_at_k(&before_ids, &truth, 8);
            after += recall_at_k(&after_ids, &truth, 8);
        }
        assert!(baseline >= 4.5, "baseline recall was {baseline}");
        assert!(
            after >= baseline - f64::EPSILON,
            "pruned recall {after} < baseline {baseline}"
        );
    }

    #[test]
    fn invalid_policy_and_budget_fail_without_source_mutation() {
        let source = fixture();
        let before = graph_debug(&source);

        assert!(matches!(
            prune_hub_preserving(
                &source,
                PrunePolicy::new(0, 32, 16, 20_000)
                    .with_enabled(true)
                    .with_provenance(1, [7; 32])
            ),
            Err(PruneError::InvalidPolicy { .. })
        ));
        assert_eq!(graph_debug(&source), before);

        assert!(matches!(
            prune_hub_preserving(&source, policy(1)),
            Err(PruneError::BudgetExceeded { .. })
        ));
        assert_eq!(graph_debug(&source), before);
    }

    #[test]
    fn disabled_policy_is_an_exact_identity() {
        let source = fixture();
        let before = artifact::encode(&source).unwrap();
        let disabled = PrunePolicy::new(0, 0, 0, 0);
        let result = prune_hub_preserving(&source, disabled).unwrap();
        assert_eq!(artifact::encode(&result).unwrap(), before);
        assert_eq!(graph_debug(&result), graph_debug(&source));
    }

    #[test]
    fn enabled_policy_requires_caller_bound_provenance() {
        let source = fixture();
        let enabled_without_provenance = PrunePolicy::new(2, 32, 16, 20_000).with_enabled(true);
        assert!(matches!(
            prune_hub_preserving(&source, enabled_without_provenance),
            Err(PruneError::InvalidPolicy { .. })
        ));
    }

    fn assert_invalid_graph_refuses(source: HnswIndex) {
        let before = graph_debug(&source);
        assert!(matches!(
            prune_hub_preserving(&source, policy(20_000)),
            Err(PruneError::InvalidGraph { .. })
        ));
        assert_eq!(graph_debug(&source), before);
    }

    #[test]
    fn malformed_self_duplicate_out_of_range_and_layer_edges_refuse() {
        let mut self_edge = fixture();
        self_edge.nodes[0].neighbors[0].push(0);
        assert_invalid_graph_refuses(self_edge);

        let mut duplicate = fixture();
        duplicate.nodes[0].neighbors[0].push(1);
        assert_invalid_graph_refuses(duplicate);

        let mut out_of_range = fixture();
        let out_of_range_neighbor = out_of_range.nodes.len();
        out_of_range.nodes[0].neighbors[0].push(out_of_range_neighbor);
        assert_invalid_graph_refuses(out_of_range);

        let mut invalid_layer = fixture();
        invalid_layer.nodes[0].neighbors.push(vec![1]);
        assert_invalid_graph_refuses(invalid_layer);
    }

    #[test]
    fn disconnected_entrypoint_refuses_without_source_mutation() {
        let mut source = fixture();
        source.nodes[0].neighbors[0].pop();
        let before = graph_debug(&source);
        assert!(matches!(
            prune_hub_preserving(&source, policy(20_000)),
            Err(PruneError::Unreachable { layer: 0, node: 31 })
        ));
        assert_eq!(graph_debug(&source), before);
    }
}
