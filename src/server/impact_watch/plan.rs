//! Planning one standing impact watch's recompute (pure over a graph snapshot).
//!
//! A watch is DATA: an `:ImpactWatch` node in the graph it watches, naming the seed
//! labels (for example `Incident`, `ConformanceViolation`, `Vulnerability`), the
//! dependency edges impact flows along and the model. When seed-labelled nodes
//! change, only their downstream cone (the *region*) is recomputed: every seed that
//! can reach the region within the hop bound is gathered (so a region node's
//! probability still accounts for every seed upstream of it, not only the changed
//! ones), their cone is handed to `MineRiskPropagation`, and exactly the region's
//! nodes are assessed — including nodes whose impact fell to zero because their
//! seed was resolved.

use std::collections::BTreeSet;

use eg_core::graph::GraphView;
use eg_plan::{impact_cone, ImpactEdges};
use eg_types::compute_result::mining::{ImpactOptions, RiskModel};
use eg_types::wire::EdgeDir;
use serde::Deserialize;
use serde_json::{Map, Value};

use crate::protocol::Method;

/// The label of a watch node.
pub const WATCH_LABEL: &str = "ImpactWatch";

/// Which way impact flows along the watch's relationship.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum WatchDirection {
    /// From a node to the nodes that point at it (`<-[:dependsOn]-`: dependents).
    #[default]
    In,
    Out,
    Both,
}

/// The impact model a watch runs.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WatchModel {
    #[default]
    NoisyOr,
    IndependentCascade,
}

/// One `:ImpactWatch` node's declaration.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(default)]
pub struct ImpactWatchSpec {
    #[serde(skip)]
    pub id: String,
    pub seed_labels: Vec<String>,
    pub relationship: Option<String>,
    pub direction: WatchDirection,
    pub model: WatchModel,
    pub hops: u32,
    pub default_transmission: f64,
    pub samples: u32,
    pub rng_seed: u64,
    /// A seed whose `status` is one of these is inactive (its impact is cleared).
    pub inactive_statuses: Vec<String>,
}

impl Default for ImpactWatchSpec {
    fn default() -> Self {
        Self {
            id: String::new(),
            seed_labels: Vec::new(),
            relationship: None,
            direction: WatchDirection::In,
            model: WatchModel::NoisyOr,
            hops: 4,
            default_transmission: 1.0,
            samples: 2_000,
            rng_seed: 0,
            inactive_statuses: vec!["resolved".to_string(), "closed".to_string()],
        }
    }
}

impl ImpactWatchSpec {
    fn edges(&self) -> ImpactEdges {
        let dir = match self.direction {
            WatchDirection::In => EdgeDir::In,
            WatchDirection::Out => EdgeDir::Out,
            WatchDirection::Both => EdgeDir::Both,
        };
        ImpactEdges {
            rel: self.relationship.clone(),
            dir,
            default_transmission: self.default_transmission.clamp(0.0, 1.0),
        }
    }

    /// Whether a node's label makes it a seed of this watch.
    pub fn watches_label(&self, label: &str) -> bool {
        self.seed_labels.iter().any(|l| l == label)
    }

    /// The node's seed probability under this watch: zero unless it carries a seed
    /// label and an active status; its `p_seed` in `[0, 1]`, else 1.
    fn seed_probability(&self, props: &Map<String, Value>) -> f64 {
        let status = props.get("status").and_then(Value::as_str);
        let inactive = status.is_some_and(|s| self.inactive_statuses.iter().any(|x| x == s));
        if inactive || !self.watches_label(label_of(props)) {
            return 0.0;
        }
        props
            .get("p_seed")
            .and_then(Value::as_f64)
            .filter(|p| (0.0..=1.0).contains(p))
            .unwrap_or(1.0)
    }

    fn model(&self, options: ImpactOptions) -> RiskModel {
        match self.model {
            WatchModel::NoisyOr => RiskModel::NoisyOr(options),
            WatchModel::IndependentCascade => RiskModel::IndependentCascade(options),
        }
    }
}

/// A node's label: its `type`, `node_type` or `label` property (the CDC feed's rule).
pub fn label_of(props: &Map<String, Value>) -> &str {
    ["type", "node_type", "label"]
        .into_iter()
        .find_map(|key| props.get(key).and_then(Value::as_str))
        .unwrap_or("")
}

/// A node's properties in `view` (empty when absent or undecodable).
pub fn node_props(view: &GraphView, id: &str) -> Map<String, Value> {
    view.node_properties
        .get(id)
        .and_then(|blob| eg_types::msgpack::decode_property_value(blob).ok())
        .and_then(|v| v.as_object().cloned())
        .unwrap_or_default()
}

/// Every well-formed watch declared in `view`, in id order.
pub fn load_watches(view: &GraphView) -> Vec<ImpactWatchSpec> {
    let mut ids: Vec<&String> = view.node_properties.keys().collect();
    ids.sort_unstable();
    ids.into_iter()
        .filter_map(|id| {
            let props = node_props(view, id);
            if label_of(&props) != WATCH_LABEL {
                return None;
            }
            let mut spec: ImpactWatchSpec = serde_json::from_value(Value::Object(props)).ok()?;
            spec.id = id.clone();
            Some(spec)
        })
        .collect()
}

/// Every node `watch` treats as a seed candidate (seed label, active or not).
pub fn seed_candidates(view: &GraphView, watch: &ImpactWatchSpec) -> Vec<String> {
    let mut ids: Vec<String> = view
        .node_properties
        .keys()
        .filter(|id| watch.watches_label(label_of(&node_props(view, id))))
        .cloned()
        .collect();
    ids.sort_unstable();
    ids
}

/// The `MineRiskPropagation` writeback that recomputes the region downstream of
/// `changed` (ids that no longer exist are skipped); `None` when nothing is left.
pub fn plan_watch(
    view: &GraphView,
    watch: &ImpactWatchSpec,
    changed: &[String],
    as_of_ms: u64,
) -> Result<Option<Method>, String> {
    let changed: Vec<String> = changed
        .iter()
        .filter(|id| view.node_map.contains_key(id.as_str()))
        .cloned()
        .collect();
    if changed.is_empty() {
        return Ok(None);
    }
    let edges = watch.edges();
    let hops = watch.hops as usize;
    let region = impact_cone(view, &changed, &edges, hops)?.nodes;
    let seeds: Vec<(String, f64)> = impact_cone(view, &region, &edges.reversed(), hops)?
        .nodes
        .into_iter()
        .map(|id| {
            let p = watch.seed_probability(&node_props(view, &id));
            (id, p)
        })
        .filter(|(_, p)| *p > 0.0)
        .collect();
    let seed_ids: Vec<String> = seeds.iter().map(|(id, _)| id.clone()).collect();
    let cone = impact_cone(view, &seed_ids, &edges, hops)?;
    let mut nodes = cone.nodes;
    let present: BTreeSet<String> = nodes.iter().cloned().collect();
    nodes.extend(region.iter().filter(|id| !present.contains(*id)).cloned());
    let seed = nodes
        .iter()
        .map(|id| seeds.iter().find(|(s, _)| s == id).map_or(0.0, |(_, p)| *p))
        .collect();
    let options = ImpactOptions {
        hops: watch.hops,
        samples: watch.samples,
        rng_seed: watch.rng_seed,
        top_paths: 0,
        attribute_seeds: false,
        assess: region,
        scope: watch.id.clone(),
        as_of_ms,
    };
    Ok(Some(Method::MineRiskPropagation {
        nodes,
        seed,
        edges: cone.edges,
        damping: 0.85,
        tolerance: 1e-9,
        max_iterations: 100,
        model: watch.model(options),
        writeback: true,
        #[cfg(feature = "epistemic")]
        as_claim: false,
    }))
}
