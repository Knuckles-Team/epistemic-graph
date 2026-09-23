use std::path::Path;
use std::sync::Arc;

use crate::protocol::{GraphType, Method, ResultPayload};
use crate::server::persistence::redb_backend::RedbBackend;
use crate::server::persistence::PersistenceBackend;
use eg_types::contract::Nonce;

use super::{
    commit_lifecycle, lifecycle_batch_id, lifecycle_was_committed, LifecycleCommitRequest,
};

/// The verified tenant scope every single-tenant lifecycle test commits under.
const TENANT: &str = "carrier-tenant:lifecycle-test";

fn open_backend(dir: &Path) -> Arc<dyn PersistenceBackend> {
    Arc::new(
        RedbBackend::open_with_shards(dir.to_string_lossy().into_owned(), 64, 1)
            .expect("open lifecycle test backend"),
    )
}

async fn assert_stable_lifecycle_replay(
    dir: &Path,
    action: &str,
    graph: &str,
    idempotency_key: &str,
    method: Method,
    changed_method: Method,
    result: ResultPayload,
) {
    let principal = Some("principal:lifecycle-test");
    let graph_fname = crate::persist::sanitize(graph);
    let first = {
        let persistence = open_backend(dir);
        let committed = commit_lifecycle(
            LifecycleCommitRequest::new(
                &persistence,
                action,
                crate::server::mutation_batch::CommitOrigin {
                    request_id: 101,
                    principal,
                },
                idempotency_key,
                graph,
                method.clone(),
                &result,
            )
            .with_attempt_nonce(Some(Nonce::from_bytes([1; 32])))
            .with_tenant_scope(TENANT),
        )
        .await
        .expect("first lifecycle commit");
        assert!(!committed.replayed);
        assert_eq!(
            committed.record.batch.batch_id,
            lifecycle_batch_id(action, TENANT, graph, principal, idempotency_key)
        );
        let stored = persistence
            .read_mutation_batch(&graph_fname, &committed.record.batch.batch_id)
            .await
            .expect("read first lifecycle receipt")
            .expect("first lifecycle receipt exists");
        assert_eq!(stored.committed_version, committed.record.committed_version);
        persistence.shutdown();
        drop(persistence);
        committed
    };

    // A restart and a fresh transport attempt with the same caller key must
    // replay the one durable receipt rather than append a second effect.
    let persistence = open_backend(dir);
    let replayed = commit_lifecycle(
        LifecycleCommitRequest::new(
            &persistence,
            action,
            crate::server::mutation_batch::CommitOrigin {
                request_id: 202,
                principal,
            },
            idempotency_key,
            graph,
            method.clone(),
            &result,
        )
        .with_attempt_nonce(Some(Nonce::from_bytes([2; 32])))
        .with_tenant_scope(TENANT),
    )
    .await
    .expect("stable lifecycle retry");
    assert!(replayed.replayed);
    assert_eq!(replayed.record.batch.batch_id, first.record.batch.batch_id);
    assert_eq!(
        replayed.record.committed_version,
        first.record.committed_version
    );
    assert_eq!(replayed.record.result_msgpack, first.record.result_msgpack);

    // Reusing the original consumed attempt nonce is a replay error even
    // though the caller-stable operation key is otherwise valid.
    let consumed = commit_lifecycle(
        LifecycleCommitRequest::new(
            &persistence,
            action,
            crate::server::mutation_batch::CommitOrigin {
                request_id: 303,
                principal,
            },
            idempotency_key,
            graph,
            method,
            &result,
        )
        .with_attempt_nonce(Some(Nonce::from_bytes([1; 32])))
        .with_tenant_scope(TENANT),
    )
    .await
    .expect_err("the original attempt nonce must be consumed");
    assert!(consumed.contains("REPLAY_NONCE_CONSUMED"), "{consumed}");

    // A changed method under the same stable key cannot masquerade as a retry.
    let conflict = commit_lifecycle(
        LifecycleCommitRequest::new(
            &persistence,
            action,
            crate::server::mutation_batch::CommitOrigin {
                request_id: 304,
                principal,
            },
            idempotency_key,
            graph,
            changed_method,
            &result,
        )
        .with_attempt_nonce(Some(Nonce::from_bytes([3; 32])))
        .with_tenant_scope(TENANT),
    )
    .await
    .expect_err("changed lifecycle payload must conflict");
    assert!(conflict.contains("IDEMPOTENCY_CONFLICT"), "{conflict}");

    let final_record = persistence
        .read_mutation_batch(&graph_fname, &first.record.batch.batch_id)
        .await
        .expect("read final lifecycle receipt")
        .expect("lifecycle receipt remains singular");
    assert_eq!(final_record.batch.batch_id, first.record.batch.batch_id);
    assert_eq!(
        final_record.committed_version,
        first.record.committed_version
    );
    persistence.shutdown();
    drop(persistence);
}

#[cfg(feature = "redb")]
#[tokio::test(flavor = "multi_thread")]
async fn create_lifecycle_replay_keeps_one_stable_receipt() {
    // Reads the ambient encryption env at its durable open, so the env must hold
    // still for this whole body. READ guard: it excludes only a key MUTATOR, never
    // another opener. See `crate::crypto::acquire_test_env_read_lock`'s doc.
    let _env_read_lock = crate::crypto::acquire_test_env_read_lock().await;
    let dir = crate::test_support::temp_dir("eg-lifecycle-replay", "create");
    assert_stable_lifecycle_replay(
        &dir,
        "create",
        "lifecycle-create",
        "caller-create-key",
        Method::CreateGraph {
            graph_name: "lifecycle-create".to_string(),
            graph_type: GraphType::Global,
        },
        Method::CreateGraph {
            graph_name: "lifecycle-create".to_string(),
            graph_type: GraphType::Agent,
        },
        ResultPayload::Json(serde_json::json!({"created": "lifecycle-create"})),
    )
    .await;
    let _ = std::fs::remove_dir_all(dir);
}

#[cfg(feature = "redb")]
#[tokio::test(flavor = "multi_thread")]
async fn delete_lifecycle_replay_keeps_one_stable_receipt() {
    // Reads the ambient encryption env at its durable open, so the env must hold
    // still for this whole body. READ guard: it excludes only a key MUTATOR, never
    // another opener. See `crate::crypto::acquire_test_env_read_lock`'s doc.
    let _env_read_lock = crate::crypto::acquire_test_env_read_lock().await;
    let dir = crate::test_support::temp_dir("eg-lifecycle-replay", "delete");
    let graph = "lifecycle-delete";
    {
        let persistence = open_backend(&dir);
        let result = ResultPayload::Json(serde_json::json!({"created": graph}));
        commit_lifecycle(
            LifecycleCommitRequest::new(
                &persistence,
                "create",
                crate::server::mutation_batch::CommitOrigin {
                    request_id: 1,
                    principal: Some("principal:lifecycle-test"),
                },
                "setup-create",
                graph,
                Method::CreateGraph {
                    graph_name: graph.to_string(),
                    graph_type: GraphType::Global,
                },
                &result,
            )
            .with_attempt_nonce(Some(Nonce::from_bytes([9; 32])))
            .with_tenant_scope(TENANT),
        )
        .await
        .expect("create delete-test graph");
        persistence.shutdown();
        drop(persistence);
    }

    assert_stable_lifecycle_replay(
        &dir,
        "delete",
        graph,
        "caller-delete-key",
        Method::DeleteGraph {
            graph_name: graph.to_string(),
        },
        Method::DeleteGraph {
            graph_name: "different-delete-target".to_string(),
        },
        ResultPayload::Json(serde_json::json!({"deleted": graph})),
    )
    .await;
    let _ = std::fs::remove_dir_all(dir);
}

/// EH-375: graph names are global and a verified agent id is not tenant-qualified, so
/// the lifecycle batch id must include the verified tenant. Same agent id, same
/// idempotency key, same graph name, different tenants: tenant B's retry probe must
/// NOT find (and so must not replay) tenant A's committed CreateGraph.
#[cfg(feature = "redb")]
#[tokio::test(flavor = "multi_thread")]
async fn lifecycle_receipt_is_not_replayed_across_tenants() {
    let _env_read_lock = crate::crypto::acquire_test_env_read_lock().await;
    let dir = crate::test_support::temp_dir("eg-lifecycle-replay", "cross-tenant");
    let persistence = open_backend(&dir);
    let graph = "lifecycle-shared-name";
    let method = || Method::CreateGraph {
        graph_name: graph.to_string(),
        graph_type: GraphType::Global,
    };
    let result = ResultPayload::Json(serde_json::json!({"created": graph}));
    let principal = Some("principal:same-agent");
    let committed = commit_lifecycle(
        LifecycleCommitRequest::new(
            &persistence,
            "create",
            crate::server::mutation_batch::CommitOrigin {
                request_id: 1,
                principal,
            },
            "same-key",
            graph,
            method(),
            &result,
        )
        .with_attempt_nonce(Some(Nonce::from_bytes([4; 32])))
        .with_tenant_scope("carrier-tenant:a"),
    )
    .await
    .expect("tenant A creates");
    assert!(!committed.replayed);
    assert_ne!(
        lifecycle_batch_id("create", "carrier-tenant:a", graph, principal, "same-key"),
        lifecycle_batch_id("create", "carrier-tenant:b", graph, principal, "same-key"),
    );
    let probe = |tenant_scope: &'static str, nonce: u8| {
        lifecycle_was_committed(
            &persistence,
            crate::server::mutation::LifecycleAttempt {
                action: "create",
                tenant_scope,
                graph,
                request_id: 2,
                attempt_nonce: Some(Nonce::from_bytes([nonce; 32])),
                principal,
                idempotency_key: "same-key",
            },
            method(),
            &result,
        )
    };
    assert!(
        !probe("carrier-tenant:b", 5).await.expect("tenant B probe"),
        "tenant B must not replay tenant A's CreateGraph receipt"
    );
    assert!(
        probe("carrier-tenant:a", 6).await.expect("tenant A probe"),
        "tenant A's own retry still replays its receipt"
    );
    persistence.shutdown();
    drop(persistence);
    let _ = std::fs::remove_dir_all(dir);
}
