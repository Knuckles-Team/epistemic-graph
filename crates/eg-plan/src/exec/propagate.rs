//! `Op::Propagate` (EH-526): the probability that each node is hit when the current
//! rows are hit.
//!
//! The downstream cone is collected breadth-first from the input rows along the
//! admitted edges (the same edge pattern and predicates as `TRAVERSE`), at most `hops`
//! hops deep and within the plan's traversal budget. Each collected edge transmits
//! with its `transmission` property (a number in `[0, 1]`), else the stage's default;
//! each seed is hit with its node's `p_seed` property (a number in `[0, 1]`), else 1.
//! The cone is then handed to the one impact kernel
//! (`eg_compute::graph_algos::impact`) that `MineRiskPropagation` also serves, so the
//! query surface and the mining surface cannot disagree.

use std::collections::HashMap;

use eg_compute::graph_algos::impact::{
    independent_cascade, noisy_or, CascadeSpec, ImpactGraph, Seed, MAX_HOPS,
};
use eg_core::graph::GraphView;
use eg_types::wire::{EdgeDir, Op, PropagateModel};
use petgraph::stable_graph::NodeIndex;
use serde_json::{Map, Value};

use super::expand::{admitted_edges, EdgeFilter};
use super::PlanCtx;
use crate::budget::Budget;
use crate::rowset::RowSet;

/// Edge property carrying a transmission probability.
pub const TRANSMISSION_PROPERTY: &str = "transmission";
/// Node property carrying a seed-hit probability.
pub const SEED_PROBABILITY_PROPERTY: &str = "p_seed";

/// One `PROPAGATE` stage, resolved.
pub(crate) struct PropagateSpec<'p> {
    pub(crate) model: PropagateModel,
    pub(crate) filter: EdgeFilter<'p>,
    pub(crate) dir: EdgeDir,
    pub(crate) hops: usize,
    pub(crate) default_transmission: f64,
}

/// The collected cone: local node order, their graph indices and the edges.
#[derive(Default)]
struct Cone {
    ids: Vec<NodeIndex>,
    local: HashMap<NodeIndex, usize>,
    edges: Vec<(usize, usize, f64)>,
    seeds: usize,
}

impl Cone {
    fn index(&mut self, node: NodeIndex) -> (usize, bool) {
        if let Some(&i) = self.local.get(&node) {
            return (i, false);
        }
        let i = self.ids.len();
        self.ids.push(node);
        self.local.insert(node, i);
        (i, true)
    }
}

/// A probability property of `props`, when it is a number in `[0, 1]`.
fn unit_property(props: &Map<String, Value>, key: &str) -> Option<f64> {
    props
        .get(key)
        .and_then(Value::as_f64)
        .filter(|p| (0.0..=1.0).contains(p))
}

fn node_props(view: &GraphView, id: &str) -> Map<String, Value> {
    view.node_properties
        .get(id)
        .and_then(|blob| eg_types::msgpack::decode_property_value(blob).ok())
        .and_then(|v| v.as_object().cloned())
        .unwrap_or_default()
}

fn collect_cone(
    view: &GraphView,
    input: &RowSet,
    spec: &PropagateSpec<'_>,
    budget: &Budget,
) -> Result<Cone, String> {
    let hops = spec.hops.min(MAX_HOPS as usize);
    let mut cone = Cone::default();
    let mut frontier: Vec<usize> = Vec::new();
    for node in input.ids().iter().filter_map(|id| view.node_map.get(id)) {
        let (i, fresh) = cone.index(*node);
        if fresh {
            frontier.push(i);
        }
    }
    cone.seeds = cone.ids.len();
    for _ in 0..hops {
        let mut next = Vec::new();
        for u in frontier {
            for (other, props) in admitted_edges(view, cone.ids[u], spec.dir, &spec.filter) {
                let (v, fresh) = cone.index(other);
                let p = unit_property(&props, TRANSMISSION_PROPERTY)
                    .unwrap_or(spec.default_transmission);
                cone.edges.push((u, v, p));
                if fresh {
                    next.push(v);
                }
            }
        }
        budget.check_traversal(cone.ids.len())?;
        frontier = next;
    }
    Ok(cone)
}

/// `Op::Propagate`: every hit node, most probable first (ties by id), scored by its
/// hit probability.
pub(crate) fn propagate_op(
    view: &GraphView,
    input: &RowSet,
    spec: &PropagateSpec<'_>,
    budget: &Budget,
) -> Result<RowSet, String> {
    let cone = collect_cone(view, input, spec, budget)?;
    let seeds: Vec<Seed> = (0..cone.seeds)
        .map(|i| Seed {
            node: i,
            probability: unit_property(
                &node_props(view, &view.graph[cone.ids[i]]),
                SEED_PROBABILITY_PROPERTY,
            )
            .unwrap_or(1.0),
        })
        .collect();
    let graph = ImpactGraph::new(cone.ids.len(), &cone.edges);
    let hops = spec.hops.min(MAX_HOPS as usize) as u32;
    let probability = match spec.model {
        PropagateModel::NoisyOr => noisy_or(&graph, &seeds, hops).probability,
        PropagateModel::Cascade { samples, seed } => {
            let sampling = CascadeSpec {
                hops,
                samples,
                seed,
            };
            independent_cascade(&graph, &seeds, &[], &sampling).probability
        }
    };
    let mut scored: Vec<(String, f64)> = cone
        .ids
        .iter()
        .zip(probability)
        .filter(|(_, p)| *p > 0.0)
        .map(|(node, p)| (view.graph[*node].clone(), p))
        .collect();
    scored.sort_by(|a, b| b.1.total_cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
    Ok(RowSet::from_scored(
        scored.into_iter().map(|(id, p)| (id, p as f32)),
    ))
}

/// Run a `PROPAGATE` op over `ctx` (any other op is a routing defect).
pub(crate) fn apply(op: &Op, input: &RowSet, ctx: &PlanCtx) -> Result<RowSet, String> {
    let Op::Propagate {
        model,
        rel,
        dir,
        edge_preds,
        hops,
        default_transmission,
    } = op
    else {
        return Err("propagate::apply was routed a non-PROPAGATE op".to_string());
    };
    let spec = PropagateSpec {
        model: *model,
        filter: EdgeFilter {
            rel: rel.as_deref(),
            preds: edge_preds,
        },
        dir: *dir,
        hops: *hops,
        default_transmission: *default_transmission,
    };
    propagate_op(ctx.view, input, &spec, &ctx.budget)
}
