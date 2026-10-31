//! Served acceptance coverage for the Leiden objective-function selector.
//!
//! The persisted `CommunityDetection` method predates the selector and still
//! carries only `resolution`.  `CommunityDetectEphemeral` is the served,
//! stateless community-detection request that carries the `quality` parameter
//! and is the production query surface for this requirement.  Exercise that
//! wire path rather than calling `medulla_compute` directly.

#![cfg(all(feature = "server", feature = "security"))]

mod common;
#[path = "common/test_support.rs"]
mod test_support;

use medulla::protocol::{CommunityQualityFunction, GraphType, Method, ResultPayload};

const SECRET: &str = "medulla-fed-query-r060-served-secret";

fn ring_of_triangles(num_cliques: usize) -> (Vec<String>, Vec<(String, String, f64)>) {
    let mut nodes = Vec::with_capacity(num_cliques * 3);
    let mut edges = Vec::with_capacity(num_cliques * 4);
    for clique in 0..num_cliques {
        let n0 = format!("c{clique}_0");
        let n1 = format!("c{clique}_1");
        let n2 = format!("c{clique}_2");
        nodes.extend([n0.clone(), n1.clone(), n2.clone()]);
        edges.extend([
            (n0.clone(), n1.clone(), 1.0),
            (n1.clone(), n2.clone(), 1.0),
            (n0, n2, 1.0),
        ]);
    }
    for clique in 0..num_cliques {
        edges.push((
            format!("c{clique}_2"),
            format!("c{}_0", (clique + 1) % num_cliques),
            1.0,
        ));
    }
    (nodes, edges)
}

async fn call_quality(
    state: &test_support::SharedState,
    id: u64,
    graph: &str,
    node_ids: Vec<String>,
    edges: Vec<(String, String, f64)>,
    resolution: f64,
    quality: CommunityQualityFunction,
) -> Vec<Vec<String>> {
    let response = test_support::dispatch(
        state,
        test_support::request(
            SECRET,
            id,
            graph,
            Method::CommunityDetectEphemeral {
                node_ids,
                edges,
                resolution,
                quality,
            },
        ),
    )
    .await;
    assert!(
        response.error.is_none(),
        "served community detection failed: {:?} / {:?}",
        response.error,
        response.error_detail
    );
    match response.result {
        Some(ResultPayload::Raw(bytes)) => {
            rmp_serde::from_slice(&bytes).expect("decode served community result")
        }
        other => panic!("expected raw served community result, got {other:?}"),
    }
}

#[tokio::test]
// spec: MEDULLA-FEDERATED-QUERY-R060
async fn served_cpm_differs_from_modularity_on_the_same_graph() {
    common::configure_authority();
    let state = test_support::durable_state(SECRET, common::current_isolation());
    let graph = "medulla-fed-query-r060-served";
    let created = test_support::dispatch(
        &state,
        test_support::request(
            SECRET,
            1,
            graph,
            Method::CreateGraph {
                graph_name: graph.to_string(),
                graph_type: GraphType::Global,
            },
        ),
    )
    .await;
    assert!(
        created.error.is_none(),
        "test graph creation failed: {:?} / {:?}",
        created.error,
        created.error_detail
    );

    // This is the classic resolution-limit fixture used by the kernel oracle:
    // modularity merges adjacent triangles, while CPM at gamma=0.5 resolves
    // each triangle.  The graph and edge weights are byte-identical for both
    // served requests; only the objective selector (and its documented scale)
    // changes.
    let (node_ids, edges) = ring_of_triangles(20);
    let modularity = call_quality(
        &state,
        2,
        graph,
        node_ids.clone(),
        edges.clone(),
        1.0,
        CommunityQualityFunction::Modularity,
    )
    .await;
    let cpm = call_quality(
        &state,
        3,
        graph,
        node_ids,
        edges,
        0.5,
        CommunityQualityFunction::Cpm,
    )
    .await;

    assert_ne!(
        modularity, cpm,
        "the served quality selector must change the partition on the same graph"
    );
    assert!(
        modularity.len() < 20,
        "modularity should merge adjacent triangles on this fixture: {modularity:?}"
    );
    assert_eq!(cpm.len(), 20, "CPM should resolve every triangle: {cpm:?}");
    assert!(
        cpm.iter().all(|community| community.len() == 3),
        "each CPM community should be one triangle: {cpm:?}"
    );
}
