use super::*;
use crate::protocol::Method;

#[test]
fn native_command_excludes_plaintext_environment_values() {
    let marker = "private-environment-marker";
    let command = NativeMutationCommand::from_public_method(
        Method::CreateGraph {
            graph_name: marker.to_string(),
            graph_type: crate::protocol::GraphType::Global,
        },
        "cluster-test-key",
    )
    .unwrap();
    let encoded = rmp_serde::to_vec_named(&command).unwrap();
    assert!(!encoded
        .windows(marker.len())
        .any(|window| window == marker.as_bytes()));
    let opened = command
        .open_public_method("cluster-test-key")
        .unwrap()
        .unwrap();
    assert!(matches!(
        opened,
        Method::CreateGraph { graph_name, .. } if graph_name == marker
    ));
    assert!(command.open_public_method("different-key").is_err());

    let graph = ReplicatedMutation::graph(
        Method::RemoveNode {
            node_id: marker.to_string(),
        },
        "cluster-test-key",
    )
    .unwrap();
    let encoded = rmp_serde::to_vec_named(&graph).unwrap();
    assert!(!encoded
        .windows(marker.len())
        .any(|window| window == marker.as_bytes()));
    assert!(matches!(
        graph.open_graph("cluster-test-key").unwrap(),
        Some(Method::RemoveNode { node_id }) if node_id == marker
    ));
    assert!(graph.open_graph("different-key").is_err());
}
