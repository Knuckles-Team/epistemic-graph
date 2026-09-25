//! The probabilistic impact models of `MineRiskPropagation` (EH-526): noisy-OR
//! and independent cascade over `eg_compute::graph_algos::impact`, with hop
//! depth, the strongest paths, per-seed Shapley attribution, a provenance
//! digest, and `:ImpactAssessment` writeback that a WAL replay reproduces
//! (the request carries the scope, the as-of time and the Monte Carlo seed).

use super::*;
use super::{process::edge_indices, vector::link_writeback_source, writeback::*};
use eg_compute::graph_algos::impact::{
    independent_cascade, noisy_or, noisy_or_attribution, strongest_paths, CascadeSpec, ImpactGraph,
    ImpactPath, Seed, Semantics,
};
use eg_types::compute_result::mining::{
    ImpactOptions, ImpactPathRow, ImpactReport, ImpactSemantics, RiskPropagationMiningResult,
    RiskScoreRow, SeedAttributionRow,
};
use eg_types::decision::digest::digest_text;
use eg_types::result_contract::compute as results;

/// Domain of an impact run's provenance digest.
pub(crate) const IMPACT_DIGEST_DOMAIN: &str = "eg/impact-propagation/v1";
/// The kernel revision folded into every digest.
const KERNEL_REVISION: u32 = 1;
/// The scope of an assessment written without one.
const ADHOC_SCOPE: &str = "adhoc";

/// Which impact model runs.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ImpactKind {
    NoisyOr,
    IndependentCascade,
}

/// A model run: its rows, report and round count.
pub(crate) struct ImpactRun {
    pub(crate) rows: Vec<RiskScoreRow>,
    pub(crate) report: ImpactReport,
    pub(crate) rounds: usize,
}

/// The inputs an impact run is a function of.
pub(crate) struct ImpactInputs<'a> {
    pub(crate) nodes: &'a [String],
    pub(crate) seed: &'a [f64],
    pub(crate) edges: &'a [(String, String, f64)],
    pub(crate) kind: ImpactKind,
    pub(crate) options: &'a ImpactOptions,
}

impl ImpactInputs<'_> {
    fn seeds(&self) -> Vec<Seed> {
        self.seed
            .iter()
            .enumerate()
            .filter(|(node, p)| *node < self.nodes.len() && **p > 0.0)
            .map(|(node, p)| Seed {
                node,
                probability: *p,
            })
            .collect()
    }

    /// Provenance: the model, the options that change the answer, and every input.
    fn digest(&self) -> String {
        let o = self.options;
        digest_text(
            IMPACT_DIGEST_DOMAIN,
            &(
                KERNEL_REVISION,
                self.kind,
                (
                    o.hops,
                    o.samples,
                    o.rng_seed,
                    o.top_paths,
                    o.attribute_seeds,
                ),
                self.nodes,
                self.seed,
                self.edges,
            ),
        )
    }
}

/// Run one impact model.
pub(crate) fn run_impact(inputs: &ImpactInputs<'_>) -> ImpactRun {
    let graph = ImpactGraph::new(
        inputs.nodes.len(),
        &edge_indices(inputs.nodes, inputs.edges),
    );
    let seeds = inputs.seeds();
    let hops = inputs.options.hops;
    let paths = top_paths(&graph, &seeds, inputs);
    let targets: Vec<usize> = paths.iter().map(|(node, _)| *node).collect();
    let mut run = match inputs.kind {
        ImpactKind::NoisyOr => noisy_or_run(&graph, &seeds, &targets, inputs),
        ImpactKind::IndependentCascade => cascade_run(&graph, &seeds, &targets, inputs),
    };
    run.report.hops = hops;
    run.report.digest = inputs.digest();
    run.report.paths = paths
        .into_iter()
        .map(|(node, path)| path_row(inputs.nodes, node, path))
        .collect();
    run
}

/// The `top_paths` strongest paths into non-seed nodes, strongest first.
fn top_paths(
    graph: &ImpactGraph,
    seeds: &[Seed],
    inputs: &ImpactInputs<'_>,
) -> Vec<(usize, ImpactPath)> {
    let mut paths: Vec<(usize, ImpactPath)> = strongest_paths(graph, seeds, inputs.options.hops)
        .into_iter()
        .enumerate()
        .filter_map(|(node, path)| path.filter(|p| p.nodes.len() > 1).map(|p| (node, p)))
        .collect();
    paths.sort_by(|a, b| {
        b.1.probability
            .total_cmp(&a.1.probability)
            .then(a.0.cmp(&b.0))
    });
    paths.truncate(inputs.options.top_paths as usize);
    paths
}

fn path_row(nodes: &[String], node: usize, path: ImpactPath) -> ImpactPathRow {
    ImpactPathRow {
        node: nodes[node].clone(),
        path: path.nodes.iter().map(|&i| nodes[i].clone()).collect(),
        probability: path.probability,
    }
}

fn empty_report(semantics: ImpactSemantics, expected_spread: f64) -> ImpactReport {
    ImpactReport {
        semantics,
        digest: String::new(),
        hops: 0,
        expected_spread,
        spread_lower: None,
        spread_upper: None,
        paths: Vec::new(),
        attribution: Vec::new(),
        attribution_note: None,
    }
}

fn semantics_of(semantics: Semantics) -> ImpactSemantics {
    match semantics {
        Semantics::Exact => ImpactSemantics::Exact,
        Semantics::UpperBound => ImpactSemantics::UpperBound,
        Semantics::CyclicUnroll => ImpactSemantics::CyclicUnroll,
    }
}

fn score_rows(
    nodes: &[String],
    probability: &[f64],
    depth: &[Option<u32>],
    bounds: Option<(&[f64], &[f64])>,
) -> Vec<RiskScoreRow> {
    nodes
        .iter()
        .enumerate()
        .map(|(i, id)| RiskScoreRow {
            node: id.clone(),
            score: probability[i],
            hops: depth[i],
            lower: bounds.map(|(lower, _)| lower[i]),
            upper: bounds.map(|(_, upper)| upper[i]),
        })
        .collect()
}

fn noisy_or_run(
    graph: &ImpactGraph,
    seeds: &[Seed],
    targets: &[usize],
    inputs: &ImpactInputs<'_>,
) -> ImpactRun {
    let out = noisy_or(graph, seeds, inputs.options.hops);
    let rows = score_rows(inputs.nodes, &out.probability, &out.depth, None);
    let mut report = empty_report(semantics_of(out.semantics), out.probability.iter().sum());
    if inputs.options.attribute_seeds {
        match noisy_or_attribution(graph, seeds, targets, inputs.options.hops) {
            Some(table) => {
                report.attribution = attribution_rows(inputs.nodes, seeds, targets, &table)
            }
            None => {
                report.attribution_note = Some(format!(
                    "exact noisy-OR attribution is limited to {} seeds; use independent_cascade",
                    eg_compute::graph_algos::impact::MAX_EXACT_SEEDS
                ))
            }
        }
    }
    ImpactRun {
        rows,
        report,
        rounds: out.rounds as usize,
    }
}

fn cascade_run(
    graph: &ImpactGraph,
    seeds: &[Seed],
    targets: &[usize],
    inputs: &ImpactInputs<'_>,
) -> ImpactRun {
    let options = inputs.options;
    let attributed: &[usize] = if options.attribute_seeds {
        targets
    } else {
        &[]
    };
    let spec = CascadeSpec {
        hops: options.hops,
        samples: options.samples,
        seed: options.rng_seed,
    };
    let out = independent_cascade(graph, seeds, attributed, &spec);
    let depth = eg_compute::graph_algos::impact::hop_depths(graph, seeds, options.hops);
    let rows = score_rows(
        inputs.nodes,
        &out.probability,
        &depth,
        Some((&out.lower, &out.upper)),
    );
    let mut report = empty_report(ImpactSemantics::MonteCarlo, out.expected_spread);
    report.spread_lower = Some(out.spread_lower);
    report.spread_upper = Some(out.spread_upper);
    report.attribution = attribution_rows(inputs.nodes, seeds, attributed, &out.attribution);
    ImpactRun {
        rows,
        report,
        rounds: out.samples as usize,
    }
}

fn attribution_rows(
    nodes: &[String],
    seeds: &[Seed],
    targets: &[usize],
    table: &[Vec<f64>],
) -> Vec<SeedAttributionRow> {
    let mut rows = Vec::new();
    for (&target, shares) in targets.iter().zip(table) {
        for (seed, &shapley) in seeds.iter().zip(shares) {
            rows.push(SeedAttributionRow {
                node: nodes[target].clone(),
                seed: nodes[seed.node].clone(),
                shapley,
            });
        }
    }
    rows
}

/// Serve an impact model, writing `:ImpactAssessment` nodes on writeback.
pub(crate) fn handle_impact(
    req_id: u64,
    core: &GraphCore,
    inputs: &ImpactInputs<'_>,
    writeback: WritebackOptions,
) -> Response {
    let run = run_impact(inputs);
    let written = if writeback.enabled {
        materialize_assessments(core, inputs, &run)
    } else {
        0
    };
    #[cfg(feature = "epistemic")]
    if writeback.enabled && writeback.as_claim {
        materialize_assessment_claims(core, inputs, &run);
    }
    Response::ok(
        req_id,
        ResultPayload::of::<results::MineRiskPropagation>(RiskPropagationMiningResult {
            scores: run.rows,
            iterations: run.rounds,
            converged: true,
            written_back: written,
            impact: Some(run.report),
        }),
    )
}

/// The assessment node id of `node` under `options.scope`.
pub(crate) fn assessment_node_id(options: &ImpactOptions, node: &str) -> String {
    WritebackNodeId::new("impact_assessment", &[scope_of(options), node]).into_string()
}

fn scope_of(options: &ImpactOptions) -> &str {
    if options.scope.is_empty() {
        ADHOC_SCOPE
    } else {
        &options.scope
    }
}

/// The rows an assessment is written for: the `assess` list (zero included,
/// so a cleared impact is recorded), else every hit node.
fn assessed<'r>(options: &ImpactOptions, rows: &'r [RiskScoreRow]) -> Vec<&'r RiskScoreRow> {
    rows.iter()
        .filter(|row| {
            if options.assess.is_empty() {
                row.score > 0.0
            } else {
                options.assess.contains(&row.node)
            }
        })
        .collect()
}

/// Materialize one `:ImpactAssessment` per assessed node, linked `ASSESSES`.
pub(crate) fn materialize_assessments(
    core: &GraphCore,
    inputs: &ImpactInputs<'_>,
    run: &ImpactRun,
) -> usize {
    let options = inputs.options;
    let seeds: Vec<&str> = inputs
        .seeds()
        .iter()
        .map(|s| inputs.nodes[s.node].as_str())
        .collect();
    let mut written = 0usize;
    for row in assessed(options, &run.rows) {
        let node_id = assessment_node_id(options, &row.node);
        let props = serde_json::json!({
            "type": "ImpactAssessment",
            "of": row.node,
            "scope": scope_of(options),
            "model": inputs.kind,
            "semantics": run.report.semantics,
            "probability": row.score,
            "hops": row.hops,
            "lower": row.lower,
            "upper": row.upper,
            "seeds": seeds,
            "digest": run.report.digest,
            "as_of_ms": options.as_of_ms,
        });
        if writeback_node(core, &node_id, &props) {
            link_writeback_source(core, &node_id, &row.node, "ASSESSES");
            written += 1;
        }
    }
    written
}

/// A `:Claim` per assessment, its confidence the impact probability.
#[cfg(feature = "epistemic")]
pub(crate) fn materialize_assessment_claims(
    core: &GraphCore,
    inputs: &ImpactInputs<'_>,
    run: &ImpactRun,
) {
    for row in assessed(inputs.options, &run.rows) {
        materialize_claim(
            core,
            &assessment_node_id(inputs.options, &row.node),
            "impact_propagation",
            row.score.clamp(0.0, 1.0),
            &format!("impact:{}", scope_of(inputs.options)),
        );
    }
}

#[cfg(test)]
mod tests;
