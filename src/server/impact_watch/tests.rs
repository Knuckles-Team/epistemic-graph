//! Standing impact watches: cone-only recompute equals a full recompute, a resolved
//! seed clears its region, the CDC cost gate, and the end-to-end served writeback.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use eg_compute::graph_algos::impact::{noisy_or, ImpactGraph, Seed};
use eg_core::graph::GraphCore;
use serde_json::json;
use tokio::sync::RwLock;

use super::*;
use crate::protocol::{GraphType, Method};
use crate::server::state::ServerState;

fn blob(v: serde_json::Value) -> Vec<u8> {
    rmp_serde::to_vec_named(&v).unwrap()
}

type NodeRows = Vec<(String, Vec<u8>)>;
type EdgeRows = Vec<(String, String, Vec<u8>)>;

/// Two incidents on db and queue; api/worker depend on them; web on api and worker.
fn fixture(queue_status: &str) -> (NodeRows, EdgeRows) {
    let watch = json!({
        "type": "ImpactWatch",
        "seed_labels": ["Incident"],
        "relationship": "dependsOn",
        "hops": 4,
        "default_transmission": 0.5,
    });
    let mut nodes = vec![("watch-1".to_string(), blob(watch))];
    for id in ["db", "queue", "api", "worker", "web", "island"] {
        nodes.push((id.into(), blob(json!({ "type": "Service" }))));
    }
    for (id, status) in [("inc-db", "open"), ("inc-queue", queue_status)] {
        let props = json!({ "type": "Incident", "status": status });
        nodes.push((id.into(), blob(props)));
    }
    let edges = [
        ("db", "inc-db", 0.9),
        ("queue", "inc-queue", 0.7),
        ("api", "db", 0.8),
        ("worker", "queue", 0.6),
        ("web", "api", 0.5),
        ("web", "worker", 0.5),
    ]
    .into_iter()
    .map(|(from, to, p)| {
        let props = json!({ "relationship": "dependsOn", "transmission": p });
        (from.to_string(), to.to_string(), blob(props))
    })
    .collect();
    (nodes, edges)
}

fn populate(core: &GraphCore, queue_status: &str) {
    let (nodes, edges) = fixture(queue_status);
    for (id, props) in nodes {
        core.add_node(id, props);
    }
    for (from, to, props) in edges {
        core.add_edge(from, to, props).unwrap();
    }
}

fn view(queue_status: &str) -> eg_core::graph::GraphView {
    let core = GraphCore::new();
    populate(&core, queue_status);
    core.analysis_snapshot()
}

fn watch(view: &eg_core::graph::GraphView) -> ImpactWatchSpec {
    let watches = load_watches(view);
    assert_eq!(watches.len(), 1);
    watches[0].clone()
}

/// The kernel's probabilities for a planned run, and its assessed ids.
fn outcome(method: &Method) -> (BTreeMap<String, f64>, Vec<String>) {
    let Method::MineRiskPropagation {
        nodes,
        seed,
        edges,
        model: eg_types::compute_result::mining::RiskModel::NoisyOr(options),
        ..
    } = method
    else {
        panic!("a noisy-OR MineRiskPropagation writeback");
    };
    let index = |id: &str| nodes.iter().position(|n| n == id).unwrap();
    let triples: Vec<(usize, usize, f64)> = edges
        .iter()
        .map(|(u, v, p)| (index(u), index(v), *p))
        .collect();
    let seeds: Vec<Seed> = seed
        .iter()
        .enumerate()
        .filter(|(_, p)| **p > 0.0)
        .map(|(node, p)| Seed {
            node,
            probability: *p,
        })
        .collect();
    let graph = ImpactGraph::new(nodes.len(), &triples);
    let p = noisy_or(&graph, &seeds, options.hops).probability;
    let map = nodes.iter().cloned().zip(p).collect();
    (map, options.assess.clone())
}

// spec: EG-FEDERATED-QUERY-R032
#[test]
fn cone_only_recompute_equals_a_full_recompute() {
    let view = view("open");
    let watch = watch(&view);
    let all = plan::seed_candidates(&view, &watch);
    let (full, _) = outcome(&plan_watch(&view, &watch, &all, 1).unwrap().unwrap());
    let changed = ["inc-queue".to_string()];
    let (partial, region) = outcome(&plan_watch(&view, &watch, &changed, 1).unwrap().unwrap());
    assert!(region.contains(&"web".to_string()) && !region.contains(&"api".to_string()));
    for id in &region {
        assert!(
            (partial[id] - full[id]).abs() < 1e-12,
            "{id}: {} vs {}",
            partial[id],
            full[id]
        );
    }
    // web sees both incidents: 1 - (1 - .5*.72)(1 - .5*.42).
    let web = 1.0 - (1.0 - 0.5 * 0.9 * 0.8) * (1.0 - 0.5 * 0.7 * 0.6);
    assert!((partial["web"] - web).abs() < 1e-12);
}

// spec: EG-FEDERATED-QUERY-R032
#[test]
fn a_resolved_seed_clears_its_region() {
    let view = view("resolved");
    let watch = watch(&view);
    let changed = ["inc-queue".to_string()];
    let (p, region) = outcome(&plan_watch(&view, &watch, &changed, 1).unwrap().unwrap());
    assert_eq!(p["inc-queue"], 0.0);
    assert_eq!(p["worker"], 0.0);
    assert!(
        region.contains(&"worker".to_string()),
        "cleared nodes are assessed"
    );
    assert!(
        (p["web"] - 0.5 * 0.9 * 0.8).abs() < 1e-12,
        "db's incident still reaches web"
    );
    assert!(plan_watch(&view, &watch, &["gone".to_string()], 1)
        .unwrap()
        .is_none());
}

// spec: EG-FEDERATED-QUERY-R032
#[test]
fn an_unwatched_graph_leaves_no_state_and_labels_filter_once_loaded() {
    let hub = ImpactWatchHub::new(["g".to_string()].into(), Duration::ZERO);
    for _ in 0..100 {
        hub.note("other", "Incident", "i");
    }
    assert!(hub.has_no_state("other"));
    hub.seed_labels
        .insert("g".into(), ["Incident".to_string()].into());
    hub.note("g", "Service", "db");
    assert!(hub.has_no_state("g"));
    hub.note("g", "Incident", "inc");
    assert!(!hub.has_no_state("g"));
}

const SECRET: &str = "impact-watch-test-secret";

#[cfg(feature = "redb")]
/// A served state with a real persistence backend: the watch's writeback is an
/// authoritative MutationBatch commit, which a backendless state refuses.
fn persisted_state() -> Arc<RwLock<ServerState>> {
    use crate::server::persistence::redb_backend::RedbBackend;
    let dir = crate::server::sql_tables::test_persist_dir()
        .to_string_lossy()
        .into_owned();
    std::fs::create_dir_all(&dir).expect("create test persist dir");
    let backend = RedbBackend::open(dir.clone(), 4096).expect("open test redb backend");
    let mut state = ServerState::new_for_test(SECRET, ServerState::test_isolation("system"));
    state.persist_dir = Some(dir);
    state.persistence = Some(Arc::new(backend));
    Arc::new(RwLock::new(state))
}

/// Write the fixture through the served, durable write path: the watch's own
/// writeback links assessments to these nodes and needs durable endpoints.
#[cfg(feature = "redb")]
async fn populate_served(state: &Arc<RwLock<ServerState>>, queue_status: &str) {
    let (nodes, edges) = fixture(queue_status);
    let mut writes = Vec::new();
    for (node_id, properties_msgpack) in nodes {
        writes.push(Method::AddNode {
            node_id,
            properties_msgpack,
        });
    }
    for (source_id, target_id, properties_msgpack) in edges {
        writes.push(Method::AddEdge {
            source_id,
            target_id,
            properties_msgpack,
        });
    }
    for (id, method) in (1_u64..).zip(writes) {
        let request =
            crate::server::auth::build_shared_test_request(SECRET, id, "ops", "system", method);
        let response = crate::server::auth::dispatch_test_on_heap(state, request).await;
        assert!(
            response.error.is_none(),
            "fixture write: {:?}",
            response.error
        );
    }
}

#[cfg(feature = "redb")]
#[tokio::test]
async fn a_noticed_incident_writes_assessments_through_the_served_path() {
    let state = persisted_state();
    let core = {
        let mut guard = state.write().await;
        guard
            .registry
            .create_graph("ops", GraphType::Commons, None)
            .expect("create graph");
        guard.registry.get("ops").expect("graph").core.clone()
    };
    populate_served(&state, "open").await;
    let hub = ImpactWatchHub::new(["ops".to_string()].into(), Duration::ZERO);
    hub.note("ops", WATCH_LABEL, "watch-1");
    let report = hub.sweep_due(&state).await;
    assert_eq!(
        report.runs, 1,
        "one watch run committed: {:?}",
        report.refusals
    );
    core.mark_dirty();
    let assessed: BTreeMap<String, serde_json::Value> = core
        .get_nodes_by_label("ImpactAssessment", 0)
        .into_iter()
        .map(|(_, bytes)| {
            let props: serde_json::Value = rmp_serde::from_slice(&bytes).unwrap();
            (props["of"].as_str().unwrap().to_string(), props)
        })
        .collect();
    assert_eq!(assessed["web"]["scope"], "watch-1");
    assert_eq!(assessed["web"]["model"], "noisy_or");
    assert!(assessed["web"]["digest"]
        .as_str()
        .unwrap()
        .starts_with("sha256:"));
    assert!(!assessed.contains_key("island"));
}
