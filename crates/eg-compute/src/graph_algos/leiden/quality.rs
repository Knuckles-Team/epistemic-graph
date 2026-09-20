// EH-283 — the quality (objective) function Leiden's local-moving and
// refinement optimize.
//
// `boolean_parameters` is capped at 1 in this repo's KISS thresholds, so this
// is a named enum rather than a `use_cpm: bool` threaded through the
// local-moving loop. The loop itself
// (`super::local_moving::generic_local_moving`) stays a SINGLE implementation:
// this module supplies the two quality-dependent DATA values that loop reads
// — a per-node scalar to accumulate per community, and a scale factor for the
// null-model penalty — rather than branching inside the loop body.

/// The objective [`super::leiden`]'s local-moving and refinement optimize.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum QualityFunction {
    /// Modularity (Newman 2006), the existing default. Its null model scales
    /// with each community's total EDGE WEIGHT against the whole graph's
    /// total (`resolution · Σ_c(tot_c)² / 2m`), which is exactly what gives
    /// modularity its well-known RESOLUTION LIMIT: on a large graph, two
    /// genuinely distinct small communities can score higher merged than
    /// separate, because the null-model term is tiny relative to `2m`
    /// regardless of the communities' own size.
    #[default]
    Modularity,
    /// Constant Potts Model (Traag, Van Dooren & Nesterov 2011): quality =
    /// `Σ_c [e_c − γ·n_c·(n_c−1)/2]`. Its null model scales with each
    /// community's own SIZE (`n_c`), not with the whole graph's total edge
    /// weight, which is precisely what removes the resolution limit — merging
    /// two small communities is penalised by their own combined size, not
    /// diluted by an unrelated `2m`. `resolution` under this variant is CPM's
    /// own γ (a link-density threshold), a different scale than modularity's
    /// γ — the two are not comparable number-for-number.
    Cpm,
}

impl QualityFunction {
    /// The scale factor `super::local_moving::best_generic_community` divides
    /// the null-model penalty by. Modularity's is the graph's total edge
    /// weight `2m` (Newman's own formula); CPM has no such term at all, so it
    /// divides by `1.0` — a literal no-op division, kept so both variants
    /// share the exact same `resolution * agg[c] * weight / normalizer`
    /// expression (same operation order as the pre-existing
    /// `louvain::best_louvain_community`, so the default `Modularity` variant
    /// is bit-for-bit identical to the pre-EH-283 kernel — see the
    /// [`super::local_moving`] module doc).
    pub(super) fn normalizer(self, m2: f64) -> f64 {
        match self {
            QualityFunction::Modularity => m2,
            QualityFunction::Cpm => 1.0,
        }
    }
}

/// Per-current-node scalar the active [`QualityFunction`] accumulates per
/// community (`agg` in [`super::local_moving::generic_local_moving`]) and
/// scales the null-model penalty by. Modularity uses each node's WEIGHTED
/// DEGREE, so the penalty scales with how much total edge weight the
/// candidate community already holds — Newman's own formula. CPM uses each
/// node's SIZE: the count of ORIGINAL leaf nodes a (possibly aggregated) node
/// represents, from `node_to_super` — the current level's population, not the
/// number of super-nodes — so the penalty scales with community POPULATION
/// instead, which is what removes the resolution limit (see
/// [`QualityFunction::Cpm`]'s doc). Computed once per level and shared by both
/// the outer local-moving pass and the refinement pass within that level
/// (both operate over the same `adj` nodes; only the search neighbourhood
/// differs).
pub(super) fn node_weight_vector(
    quality: QualityFunction,
    adj: &[Vec<(usize, f64)>],
    node_to_super: &[usize],
) -> Vec<f64> {
    match quality {
        QualityFunction::Modularity => adj
            .iter()
            .map(|row| row.iter().map(|(_, w)| *w).sum())
            .collect(),
        QualityFunction::Cpm => node_sizes(node_to_super, adj.len()),
    }
}

/// `sizes[j]` = the number of ORIGINAL base-graph nodes that currently map to
/// current-level node `j` (`node_to_super[o] == j`). At level 0 every node is
/// its own singleton (`sizes[j] == 1.0` for all `j`); aggregation only ever
/// grows a node's population, never splits it, so this is a plain O(original
/// node count) tally, not a running total threaded through levels.
fn node_sizes(node_to_super: &[usize], n_current: usize) -> Vec<f64> {
    let mut sizes = vec![0.0; n_current];
    for &j in node_to_super {
        sizes[j] += 1.0;
    }
    sizes
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn modularity_normalizer_is_m2() {
        assert_eq!(QualityFunction::Modularity.normalizer(42.0), 42.0);
    }

    #[test]
    fn cpm_normalizer_is_one_regardless_of_m2() {
        assert_eq!(QualityFunction::Cpm.normalizer(42.0), 1.0);
        assert_eq!(QualityFunction::Cpm.normalizer(0.0), 1.0);
    }

    #[test]
    fn modularity_node_weight_is_weighted_degree() {
        let adj = vec![vec![(1, 2.0), (2, 1.0)], vec![(0, 2.0)], vec![(0, 1.0)]];
        let node_to_super = [0, 1, 2];
        let weights = node_weight_vector(QualityFunction::Modularity, &adj, &node_to_super);
        assert_eq!(weights, vec![3.0, 2.0, 1.0]);
    }

    #[test]
    fn cpm_node_weight_is_population_size_at_a_coarsened_level() {
        // Original nodes 0,1,2 → current-level node 0; original node 3 →
        // current-level node 1: a level-1 adjacency of 2 (super-)nodes.
        let adj = vec![vec![(1, 5.0)], vec![(0, 5.0)]];
        let node_to_super = [0, 0, 0, 1];
        let weights = node_weight_vector(QualityFunction::Cpm, &adj, &node_to_super);
        assert_eq!(weights, vec![3.0, 1.0]);
    }

    #[test]
    fn default_quality_function_is_modularity() {
        assert_eq!(QualityFunction::default(), QualityFunction::Modularity);
    }
}
