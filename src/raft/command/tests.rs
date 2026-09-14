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

fn graph_request(graph_name: &str, group_id: crate::raft::GroupId) -> crate::raft::RaftRequest {
    crate::raft::RaftRequest {
        graph_fname: crate::persist::sanitize(graph_name),
        graph_name: graph_name.to_string(),
        graph_type: crate::protocol::GraphType::Global,
        command: ReplicatedMutation::caller_graph(
            Method::RemoveNode {
                node_id: "node".to_string(),
            },
            "cluster-test-key",
        )
        .unwrap(),
        committed_at_ms: 19,
        mutation: crate::raft::RaftMutationContext {
            batch_id: crate::server::mutation_batch::opaque_coordinator_key(
                "raft-graph-test",
                graph_name,
                "batch",
            ),
            request_id: 7,
            attempt_nonce: Some(eg_types::contract::Nonce::from_bytes([3; 32])),
            tenant_scope: crate::server::mutation_batch::opaque_coordinator_key(
                "carrier-tenant",
                "verified",
                "tenant-a",
            ),
            principal_fingerprint: crate::server::mutation_batch::opaque_coordinator_key(
                "principal:sha256",
                "principal-a",
                "authority",
            ),
            identity_bootstrap: false,
            placement_epoch: 11,
            fencing_token: Some(group_id),
            created_at_ms: 17,
        },
    }
}

#[test]
fn ordinary_graph_command_is_bound_to_request_group_and_authenticated_authority() {
    let mut request = graph_request("tenant-a:graph-a", 2);
    request
        .bind_graph_command("cluster-test-key", 2)
        .expect("selected route binds command");
    request
        .validate_graph_command("cluster-test-key", 2)
        .expect("same request and group validate");
    request
        .bind_graph_command("cluster-test-key", 2)
        .expect("exact same-request binding is idempotent");

    let mut cross_graph = request.clone();
    cross_graph.graph_name = "tenant-a:graph-b".to_string();
    assert!(cross_graph
        .validate_graph_command("cluster-test-key", 2)
        .is_err());

    let mut cross_physical_graph = request.clone();
    cross_physical_graph.graph_fname = crate::persist::sanitize("tenant-a:graph-b");
    assert!(cross_physical_graph
        .validate_graph_command("cluster-test-key", 2)
        .is_err());

    let mut fresh_identity = request.clone();
    fresh_identity.mutation.batch_id = crate::server::mutation_batch::opaque_coordinator_key(
        "raft-graph-test",
        "tenant-a:graph-a",
        "fresh-batch",
    );
    assert!(fresh_identity
        .validate_graph_command("cluster-test-key", 2)
        .is_err());
    assert!(cross_graph
        .bind_graph_command("cluster-test-key", 2)
        .is_err());

    assert!(request
        .validate_graph_command("cluster-test-key", 3)
        .is_err());

    let mut wrong_fence = request.clone();
    wrong_fence.mutation.fencing_token = Some(3);
    assert!(wrong_fence
        .validate_graph_command("cluster-test-key", 2)
        .is_err());

    let mut wrong_placement = request.clone();
    wrong_placement.mutation.placement_epoch += 1;
    assert!(wrong_placement
        .validate_graph_command("cluster-test-key", 2)
        .is_err());

    let mut wrong_tenant = request.clone();
    wrong_tenant.mutation.tenant_scope = crate::server::mutation_batch::opaque_coordinator_key(
        "carrier-tenant",
        "verified",
        "tenant-b",
    );
    assert!(wrong_tenant
        .validate_graph_command("cluster-test-key", 2)
        .is_err());

    let mut wrong_principal = request;
    wrong_principal.mutation.principal_fingerprint =
        crate::server::mutation_batch::opaque_coordinator_key(
            "principal:sha256",
            "principal-b",
            "authority",
        );
    assert!(wrong_principal
        .validate_graph_command("cluster-test-key", 2)
        .is_err());

    let mut wrong_request = graph_request("tenant-a:graph-a", 2);
    wrong_request
        .bind_graph_command("cluster-test-key", 2)
        .unwrap();
    wrong_request.mutation.request_id += 1;
    assert!(wrong_request
        .validate_graph_command("cluster-test-key", 2)
        .is_err());

    let mut wrong_nonce = graph_request("tenant-a:graph-a", 2);
    wrong_nonce
        .bind_graph_command("cluster-test-key", 2)
        .unwrap();
    wrong_nonce.mutation.attempt_nonce = Some(eg_types::contract::Nonce::from_bytes([4; 32]));
    assert!(wrong_nonce
        .validate_graph_command("cluster-test-key", 2)
        .is_err());

    let mut wrong_time = graph_request("tenant-a:graph-a", 2);
    wrong_time
        .bind_graph_command("cluster-test-key", 2)
        .unwrap();
    wrong_time.mutation.created_at_ms += 1;
    assert!(wrong_time
        .validate_graph_command("cluster-test-key", 2)
        .is_err());

    let mut wrong_commit_time = graph_request("tenant-a:graph-a", 2);
    wrong_commit_time
        .bind_graph_command("cluster-test-key", 2)
        .unwrap();
    wrong_commit_time.committed_at_ms += 1;
    assert!(wrong_commit_time
        .validate_graph_command("cluster-test-key", 2)
        .is_err());
}

#[test]
fn unbound_graph_method_is_rejected_by_the_apply_authority_gate() {
    let request = graph_request("tenant-a:graph-a", 2);
    assert!(request
        .validate_graph_command("cluster-test-key", 2)
        .is_err());
}

#[test]
fn internal_graph_command_requires_the_typed_internal_authority() {
    let mut request = graph_request("__placement_catalog__", 0);
    request.command = ReplicatedMutation::graph(
        Method::RemoveNode {
            node_id: "internal-node".to_string(),
        },
        "cluster-test-key",
    )
    .unwrap();
    assert!(request
        .validate_graph_command("cluster-test-key", 0)
        .is_err());

    request.mutation = crate::raft::RaftMutationContext::internal(
        "raft-placement",
        "__placement_catalog__",
        "internal-node",
        0,
        0,
    );
    request
        .validate_graph_command("cluster-test-key", 0)
        .expect("typed internal command accepts only engine-owned authority");

    let claimed = crate::raft::RaftMutationContext::from_verified_request(
        "caller-batch".to_string(),
        1,
        Some(eg_types::contract::Nonce::from_bytes([8; 32])),
        &request.mutation.tenant_scope,
        request.mutation.principal_fingerprint.clone(),
        false,
        0,
        crate::raft::RaftMutationTiming {
            fencing_token: None,
            created_at_ms: 1,
        },
    );
    assert!(
        claimed.is_err(),
        "verified callers cannot claim internal authority"
    );
}
