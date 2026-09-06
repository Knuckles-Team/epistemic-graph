use std::sync::Arc;

use crate::graph::GraphCore;
use crate::mutation_batch::{MutationBatch, MutationSurface, VersionExpectation};
use crate::protocol::{Method, ResultPayload};
use crate::server::persistence::PersistenceBackend;

use super::commit::changed_work_item_ids;
use super::digest::work_item_batch_identity;
use super::{
    commit_work_item, compile_methods, lock_graph, publish_change_envelope_projection, CompileBatch,
};

struct LockProbePersistence {
    entered_commit: tokio::sync::Notify,
}

#[async_trait::async_trait]
impl PersistenceBackend for LockProbePersistence {
    async fn load_all(
        &self,
        _state: &Arc<tokio::sync::RwLock<crate::server::ServerState>>,
    ) -> Result<usize, String> {
        Ok(0)
    }

    async fn record_durable(&self, _graph_fname: &str, _method: &Method) -> Result<(), String> {
        Ok(())
    }

    async fn read_mutation_graph_version(&self, _graph_fname: &str) -> Result<Option<u64>, String> {
        Ok(Some(0))
    }

    async fn commit_mutation_batch(
        &self,
        _graph_fname: &str,
        _batch: &MutationBatch,
        _result_msgpack: Option<&[u8]>,
        _committed_at_ms: u64,
    ) -> Result<crate::mutation_batch::MutationBatchCommit, String> {
        self.entered_commit.notify_one();
        Err("lock-probe-stop".to_string())
    }

    fn shutdown(&self) {}
}

#[tokio::test]
async fn work_item_commit_waits_for_shared_graph_mutation_lane() {
    use std::time::Duration;

    let graph = "work-item-lock-probe";
    let held = lock_graph(graph).await;
    let probe = Arc::new(LockProbePersistence {
        entered_commit: tokio::sync::Notify::new(),
    });
    let persistence: Arc<dyn PersistenceBackend> = probe.clone();
    let core = Arc::new(GraphCore::new());
    let wait_for_commit = probe.entered_commit.notified();
    tokio::pin!(wait_for_commit);

    let task = tokio::spawn(async move {
        commit_work_item(
            Some(&persistence),
            &core,
            7,
            Some("principal:synthetic"),
            graph,
            0,
            None,
            Method::RenewWorkItemLease {
                tenant: "tenant:synthetic".into(),
                work_item_id: "work:synthetic".into(),
                worker_id: "worker:synthetic".into(),
                lease_epoch: 1,
                fencing_token: 1,
                now_ms: 1,
                lease_ms: 1_000,
            },
        )
        .await
    });

    assert!(
        tokio::time::timeout(Duration::from_millis(25), &mut wait_for_commit)
            .await
            .is_err(),
        "WorkItem persistence entered while the graph mutation lane was held"
    );
    drop(held);
    assert!(
        tokio::time::timeout(Duration::from_secs(1), &mut wait_for_commit)
            .await
            .is_ok(),
        "WorkItem persistence did not enter after the graph mutation lane released"
    );
    let outcome = task.await.expect("lock-probe task panicked");
    assert_eq!(outcome.unwrap_err(), "lock-probe-stop");
}

#[test]
fn work_item_result_bytes_preserve_projection_refresh_ids() {
    let value = serde_json::json!({
        "status": "leased",
        "changed_work_item_ids": ["work:one", "work:two"],
    });
    let bytes = rmp_serde::to_vec_named(&value).unwrap();
    let expected = vec!["work:one".to_string(), "work:two".to_string()];

    assert_eq!(
        changed_work_item_ids(&ResultPayload::Raw(bytes), true).unwrap(),
        expected
    );
    assert_eq!(
        changed_work_item_ids(&ResultPayload::Json(value), true).unwrap(),
        expected
    );
}

#[test]
fn resource_host_result_requires_no_work_item_projection_ids() {
    let value = serde_json::json!({"accepted": true, "host_ref": "host:one"});
    let bytes = rmp_serde::to_vec_named(&value).unwrap();

    assert_eq!(
        changed_work_item_ids(&ResultPayload::Raw(bytes.clone()), false).unwrap(),
        Vec::<String>::new()
    );
    assert_eq!(
        changed_work_item_ids(&ResultPayload::Json(value), true).unwrap_err(),
        "committed WorkItem result has no changed_work_item_ids"
    );
    assert_eq!(
        changed_work_item_ids(
            &ResultPayload::Json(serde_json::json!({"changed_work_item_ids": []})),
            false,
        )
        .unwrap_err(),
        "committed resource-host result unexpectedly has changed_work_item_ids"
    );
}

#[test]
fn terminal_work_item_retry_identity_is_transport_independent_and_scope_bound() {
    let method = Method::CommitWorkItemResult {
        tenant: "tenant-a".into(),
        work_item_id: "work-1".into(),
        worker_id: "worker-a".into(),
        lease_epoch: 2,
        fencing_token: 9,
        idempotency_key: "terminal-key".into(),
        outcome: "succeeded".into(),
        result_ref: Some("result:sha256:one".into()),
        error_ref: None,
        retryable: false,
        now_ms: 1_000,
    };

    let first = work_item_batch_identity("graph-a", "tenant-a", 10, &method).unwrap();
    let retry = work_item_batch_identity("graph-a", "tenant-a", 11, &method).unwrap();
    assert_eq!(first, retry);
    assert!(first.uses_native_row_cas);
    assert_ne!(first.durable_request_id, 0);
    assert_eq!(first.batch_id.len(), "work:".len() + 64);
    assert_eq!(first.idempotency_key.len(), "work-idem:".len() + 64);

    let other_tenant = work_item_batch_identity("graph-a", "tenant-b", 11, &method).unwrap();
    let other_graph = work_item_batch_identity("graph-b", "tenant-a", 11, &method).unwrap();
    assert_ne!(first.batch_id, other_tenant.batch_id);
    assert_ne!(first.batch_id, other_graph.batch_id);

    let renew = Method::RenewWorkItemLease {
        tenant: "tenant-a".into(),
        work_item_id: "work-1".into(),
        worker_id: "worker-a".into(),
        lease_epoch: 2,
        fencing_token: 9,
        now_ms: 1_000,
        lease_ms: 10_000,
    };
    let renew_first = work_item_batch_identity("graph-a", "tenant-a", 10, &renew).unwrap();
    let renew_retry = work_item_batch_identity("graph-a", "tenant-a", 11, &renew).unwrap();
    assert!(!renew_first.uses_native_row_cas);
    assert_ne!(renew_first.batch_id, renew_retry.batch_id);
}

#[test]
fn transaction_compiler_preserves_order_and_fences() {
    let batch = compile_methods(
        CompileBatch {
            batch_id: "txn-1",
            request_id: 1,
            principal: Some("agent:a"),
            tenant: "tenant-a",
            graph: "graph-a",
            placement_epoch: 8,
            idempotency_key: "idem-a",
            expected_graph_version: Some(4),
            fencing_token: Some(11),
            created_at_ms: 99,
            default_surface: MutationSurface::Transaction,
            authoritative_state: None,
        },
        vec![
            Method::RemoveNode {
                node_id: "a".into(),
            },
            Method::RemoveNode {
                node_id: "b".into(),
            },
        ],
    )
    .unwrap();
    assert_eq!(batch.operations[0].ordinal, 0);
    assert_eq!(batch.operations[1].ordinal, 1);
    assert_eq!(batch.version_expectation, VersionExpectation::Graph(4));
    assert_eq!(batch.fencing_token, Some(11));
    assert_eq!(batch.outbox.len(), 1);
}

#[test]
fn change_envelope_projection_failure_never_partially_publishes() {
    use crate::change_envelope::{
        ChangeEnvelope, ContentVersion, ContentVersionPosition, PrivacyAttestation,
        CHANGE_ENVELOPE_VERSION,
    };

    let core = Arc::new(GraphCore::new());
    core.add_node(
        "existing".into(),
        rmp_serde::to_vec_named(&serde_json::json!({"value": 0})).unwrap(),
    );
    core.mark_dirty();
    let source_version = core.version();
    let conditions =
        rmp_serde::to_vec_named(serde_json::json!({"value": 999}).as_object().unwrap()).unwrap();
    let updates =
        rmp_serde::to_vec_named(serde_json::json!({"value": 1}).as_object().unwrap()).unwrap();
    let mutation = compile_methods(
        CompileBatch {
            batch_id: "projection-atomic",
            request_id: 9,
            principal: Some("test-principal"),
            tenant: "tenant-a",
            graph: "graph-a",
            placement_epoch: 0,
            idempotency_key: "projection-atomic",
            expected_graph_version: Some(source_version),
            fencing_token: None,
            created_at_ms: 1,
            default_surface: MutationSurface::Graph,
            authoritative_state: None,
        },
        vec![
            Method::AddNode {
                node_id: "early".into(),
                properties_msgpack: rmp_serde::to_vec_named(
                    &serde_json::json!({"value": "must-not-publish"}),
                )
                .unwrap(),
            },
            Method::CompareAndSetNodeFields {
                node_id: "existing".into(),
                conditions_msgpack: conditions,
                updates_msgpack: updates,
            },
        ],
    )
    .unwrap();
    let envelope = ChangeEnvelope {
        schema_version: CHANGE_ENVELOPE_VERSION,
        envelope_id: "projection-atomic-envelope".into(),
        mutation,
        content_version: ContentVersion {
            object_id: "existing".into(),
            digest_algorithm: "sha256".into(),
            digest: "a".repeat(64),
            previous_digest: None,
            source_version: ContentVersionPosition::Sequence(1),
        },
        cursor: None,
        blobs: Vec::new(),
        features: Vec::new(),
        evidence: Vec::new(),
        policies: Vec::new(),
        lineage: Vec::new(),
        privacy: PrivacyAttestation {
            policy_version: "privacy-v1".into(),
            sanitizer_version: "sanitizer-v1".into(),
            sanitized_payload_digest: "b".repeat(64),
        },
        commit_seq: None,
        commit_descriptor_ref: None,
    };

    assert!(publish_change_envelope_projection(&core, &envelope).is_err());
    assert!(!core.has_node("early"));
    assert_eq!(core.version(), source_version);
    let existing: serde_json::Value = rmp_serde::from_slice(
        &core
            .get_node_properties("existing")
            .expect("existing node properties"),
    )
    .unwrap();
    assert_eq!(existing["value"], 0);
}

#[cfg(feature = "rdf")]
#[test]
fn rdf_adapter_marks_rdf_surface() {
    let batch = compile_methods(
        CompileBatch {
            batch_id: "rdf-1",
            request_id: 2,
            principal: Some("test-principal"),
            tenant: "tenant-a",
            graph: "graph-a",
            placement_epoch: 0,
            idempotency_key: "rdf-idem",
            expected_graph_version: Some(0),
            fencing_token: None,
            created_at_ms: 100,
            default_surface: MutationSurface::Other,
            authoritative_state: None,
        },
        vec![Method::DropNamedGraph],
    )
    .unwrap();
    assert_eq!(batch.operations[0].surface, MutationSurface::Rdf);
    assert!(matches!(batch.operations[0].method, Method::ClearGraph));
}
