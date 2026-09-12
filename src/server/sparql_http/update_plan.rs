//! Detached SPARQL UPDATE planning shared by the HTTP adapter and dispatch.

use std::collections::HashMap;
use std::sync::Arc;

use eg_rdf::sparql::Projection;
use eg_rdf::update::GraphStore;
use tokio::sync::RwLock;

use crate::graph::GraphCore;
use crate::server::ServerState;

/// A snapshot of the caller-authorized graphs at plan time: each entry's
/// `(type, core)` if the graph already exists live in the registry, or `None`
/// for a graph the update may create.
#[cfg(feature = "shacl")]
type LiveGraphSnapshot = Vec<(String, Option<(crate::protocol::GraphType, Arc<GraphCore>)>)>;

/// Detached staging state threaded through one update's plan: pre-image bytes,
/// the staged (post-update) core, whether the graph existed before, and its
/// type -- each keyed by graph name.
#[cfg(feature = "shacl")]
type StagedGraphState = (
    HashMap<String, Vec<u8>>,
    HashMap<String, Arc<GraphCore>>,
    HashMap<String, bool>,
    HashMap<String, crate::protocol::GraphType>,
);

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PlannedGraphUpdate {
    pub graph: String,
    pub graph_type: crate::protocol::GraphType,
    pub existed_before: bool,
    pub before_msgpack: Vec<u8>,
    pub after_msgpack: Vec<u8>,
}

/// Return the complete graph set an update may address. Lifecycle creation stays
/// in dispatch so it uses the verified caller and durable graph coordinator.
pub(crate) fn update_graphs(update_text: &str, default_graph: &str) -> Result<Vec<String>, String> {
    let parsed = eg_rdf::update::parse_update(update_text)?;
    let mut graphs = eg_rdf::update::referenced_named_graphs(&parsed);
    graphs.push(default_graph.to_string());
    graphs.sort();
    graphs.dedup();
    Ok(graphs)
}

pub(crate) fn update_uses_variable_graph(update_text: &str) -> bool {
    let tokens = update_text.split_whitespace().collect::<Vec<_>>();
    tokens.windows(2).any(|pair| {
        pair[0].eq_ignore_ascii_case("graph")
            && (pair[1].starts_with('?') || pair[1].starts_with('$'))
    })
}

/// Plan a SPARQL UPDATE entirely on detached graph images. The returned before/
/// after images are consumed by dispatch's per-graph MutationBatch coordinator;
/// this function never exposes or mutates a live registry core.
pub(crate) async fn plan_update(
    state: &Arc<RwLock<ServerState>>,
    update_text: &str,
    default_graph: &str,
    authorized_graphs: &[String],
) -> Result<Vec<PlannedGraphUpdate>, String> {
    #[cfg(not(feature = "shacl"))]
    return Err("SPARQL UPDATE requires the shacl integrity-guard feature".to_string());

    #[cfg(feature = "shacl")]
    {
        let live = snapshot_authorized_graphs(state, authorized_graphs).await;
        let update_text = update_text.to_owned();
        let default_graph = default_graph.to_owned();
        tokio::task::spawn_blocking(move || {
            plan_detached_update(&update_text, live, &default_graph)
        })
        .await
        .map_err(|error| format!("compute task failed: {error}"))?
    }
}

#[cfg(feature = "shacl")]
async fn snapshot_authorized_graphs(
    state: &Arc<RwLock<ServerState>>,
    authorized_graphs: &[String],
) -> LiveGraphSnapshot {
    let s = state.read().await;
    authorized_graphs
        .iter()
        .map(|name| {
            (
                name.clone(),
                s.registry
                    .get(name)
                    .map(|entry| (entry.graph_type, entry.core.clone())),
            )
        })
        .collect()
}

#[cfg(feature = "shacl")]
fn plan_detached_update(
    update_text: &str,
    live: LiveGraphSnapshot,
    default_graph: &str,
) -> Result<Vec<PlannedGraphUpdate>, String> {
    let parsed = eg_rdf::update::parse_update(update_text)?;
    let (mut before, staged_by_name, mut existed, mut graph_types) = stage_graphs(live)?;
    let default = staged_by_name
        .get(default_graph)
        .cloned()
        .ok_or_else(|| format!("Graph '{default_graph}' not found"))?;
    let mut graphs = staged_by_name.clone();
    graphs.insert(String::new(), default);
    let store = EndpointStore { graphs };
    let guard = crate::server::icv_guard::CoreIcvGuard::routed(&store.graphs);
    eg_rdf::update::execute(&parsed, &store, &Projection::raw(), &guard)
        .map_err(|error| error.to_string())?;
    collect_planned_updates(staged_by_name, &mut before, &mut existed, &mut graph_types)
}

#[cfg(feature = "shacl")]
fn stage_graphs(live: LiveGraphSnapshot) -> Result<StagedGraphState, String> {
    let mut before = HashMap::new();
    let mut staged_by_name = HashMap::new();
    let mut existed = HashMap::new();
    let mut graph_types = HashMap::new();
    for (name, existing) in live {
        let (graph_type, core, existed_before) = match existing {
            Some((graph_type, core)) => (graph_type, core, true),
            None => (
                crate::protocol::GraphType::Global,
                Arc::new(GraphCore::new()),
                false,
            ),
        };
        let bytes = core.to_msgpack()?;
        let staged = Arc::new(GraphCore::from_snapshot(core.snapshot(), core.version())?);
        before.insert(name.clone(), bytes);
        existed.insert(name.clone(), existed_before);
        graph_types.insert(name.clone(), graph_type);
        staged_by_name.insert(name, staged);
    }
    Ok((before, staged_by_name, existed, graph_types))
}

#[cfg(feature = "shacl")]
fn collect_planned_updates(
    staged_by_name: HashMap<String, Arc<GraphCore>>,
    before: &mut HashMap<String, Vec<u8>>,
    existed: &mut HashMap<String, bool>,
    graph_types: &mut HashMap<String, crate::protocol::GraphType>,
) -> Result<Vec<PlannedGraphUpdate>, String> {
    let mut planned = Vec::new();
    for (graph, core) in staged_by_name {
        let after_msgpack = core.to_msgpack()?;
        let before_msgpack = before
            .remove(&graph)
            .ok_or_else(|| "SPARQL planner lost a graph pre-image".to_string())?;
        let existed_before = existed
            .remove(&graph)
            .ok_or_else(|| "SPARQL planner lost graph existence state".to_string())?;
        if !existed_before || before_msgpack != after_msgpack {
            planned.push(PlannedGraphUpdate {
                graph_type: graph_types
                    .remove(&graph)
                    .ok_or_else(|| "SPARQL planner lost graph type".to_string())?,
                graph,
                existed_before,
                before_msgpack,
                after_msgpack,
            });
        }
    }
    planned.sort_by(|left, right| left.graph.cmp(&right.graph));
    Ok(planned)
}

/// Registry-backed graph store used only against detached planner images.
struct EndpointStore {
    /// `"" ⇒ default graph`, else the named-graph IRI → its staged core.
    graphs: HashMap<String, Arc<GraphCore>>,
}

impl GraphStore for EndpointStore {
    fn core(&self, graph: Option<&str>) -> Option<Arc<GraphCore>> {
        self.graphs.get(graph.unwrap_or("")).cloned()
    }

    fn named(&self) -> Vec<(String, Arc<GraphCore>)> {
        self.graphs
            .iter()
            .filter(|(k, _)| !k.is_empty())
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }

    fn clear(&self, graph: Option<&str>) -> Result<(), String> {
        if let Some(c) = self.core(graph) {
            c.clear();
        }
        Ok(())
    }

    // DROP keeps the registry entry addressable (clears content) — matches the
    // engine's `DropNamedGraph` op rather than a registry eviction.
    fn drop_graph(&self, graph: Option<&str>) -> Result<(), String> {
        self.clear(graph)
    }
}
