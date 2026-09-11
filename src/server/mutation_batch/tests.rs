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
    seen_batch: std::sync::Mutex<Option<MutationBatch>>,
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
        batch: &MutationBatch,
        _result_msgpack: Option<&[u8]>,
        _committed_at_ms: u64,
    ) -> Result<crate::mutation_batch::MutationBatchCommit, String> {
        *self.seen_batch.lock().unwrap() = Some(batch.clone());
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
        seen_batch: std::sync::Mutex::new(None),
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
            None,
            None,
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

#[tokio::test]
async fn authenticated_work_item_commit_carries_nonce_and_stable_key() {
    let graph = "work-item-authenticated-key-probe";
    let probe = Arc::new(LockProbePersistence {
        entered_commit: tokio::sync::Notify::new(),
        seen_batch: std::sync::Mutex::new(None),
    });
    let persistence: Arc<dyn PersistenceBackend> = probe.clone();
    let core = Arc::new(GraphCore::new());
    let nonce = eg_types::contract::Nonce::from_bytes([0x42; 32]);
    let retry_nonce = eg_types::contract::Nonce::from_bytes([0x43; 32]);
    let stable_key = "signed-work-item-operation";
    let method = || Method::RenewWorkItemLease {
        tenant: "tenant:authenticated".into(),
        work_item_id: "work:authenticated".into(),
        worker_id: "worker:authenticated".into(),
        lease_epoch: 1,
        fencing_token: 1,
        now_ms: 1,
        lease_ms: 1_000,
    };
    assert_eq!(
        commit_work_item(
            Some(&persistence),
            &core,
            17,
            Some(nonce),
            Some(stable_key),
            Some("principal:authenticated"),
            graph,
            0,
            None,
            method(),
        )
        .await
        .unwrap_err(),
        "lock-probe-stop"
    );
    let first_batch = probe
        .seen_batch
        .lock()
        .unwrap()
        .clone()
        .expect("authenticated WorkItem compile reached persistence");
    assert_eq!(
        commit_work_item(
            Some(&persistence),
            &core,
            18,
            Some(retry_nonce),
            Some(stable_key),
            Some("principal:authenticated"),
            graph,
            0,
            None,
            method(),
        )
        .await
        .unwrap_err(),
        "lock-probe-stop"
    );
    let retry_batch = probe
        .seen_batch
        .lock()
        .unwrap()
        .clone()
        .expect("authenticated WorkItem retry compile reached persistence");
    let envelope = first_batch
        .envelope
        .operation()
        .expect("WorkItem compile emits an operation envelope");
    assert_eq!(envelope.authority.nonce, nonce);
    assert_eq!(envelope.idempotency_key(), stable_key);
    assert_eq!(first_batch.batch_id, retry_batch.batch_id);
    assert_eq!(first_batch.idempotency_key(), retry_batch.idempotency_key());
    assert_ne!(
        envelope.authority.nonce,
        retry_batch.envelope.operation().unwrap().authority.nonce
    );
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
        outcome_extension: None,
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
            attempt_nonce: None,
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
            attempt_nonce: None,
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
            attempt_nonce: None,
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

// ---------------------------------------------------------------------------
// Durability-domain classification golden (RF-RULING-007 / B13)
// ---------------------------------------------------------------------------

/// Every explicit `Method -> DurabilityDomain` arm of `canonical::domain_for`, in
/// source order, plus its two fall-through arms as `("_", ..)`.
///
/// This is the golden: a method's durability domain decides which authority owns
/// its state and its version counter, so a silent reclassification moves a write
/// to a different store. `DurabilityDomain` is not enumerable from a method NAME
/// (classification needs a `Method` VALUE, and the protocol has 400+ variants
/// with non-trivial payloads), so the map is read back out of the classifier's
/// own source and compared here. `classifier_source_map_agrees_with_domain_for`
/// keeps that reading honest by spot-checking constructed methods against the
/// compiled function.
const CLASSIFICATION_GOLDEN: &[(&str, &str)] = &[
    ("CreateGraph", "Lifecycle"),
    ("DeleteGraph", "Lifecycle"),
    ("MultiGraphBatchUpdate", "MultiGraph"),
    ("Commit", "CrossModal"),
    ("BlobBegin", "BlobStore"),
    ("BlobChunkPut", "BlobStore"),
    ("BlobCommit", "BlobStore"),
    ("BlobRef", "BlobStore"),
    ("BlobUnref", "BlobStore"),
    ("BlobGc", "BlobStore"),
    ("KvPut", "KvStore"),
    ("KvDelete", "KvStore"),
    ("KvCas", "KvStore"),
    ("TsAppend", "TimeSeries"),
    ("TsEvict", "TimeSeries"),
    ("TsDeleteSeries", "TimeSeries"),
    ("AnalyticsJob", "AnalyticsJob"),
    ("KgDelegate", "ControlPlane"),
    ("SubmitWorkItem", "ControlPlane"),
    ("SubmitWorkItems", "ControlPlane"),
    ("ClaimWorkItem", "ControlPlane"),
    ("RenewWorkItemLease", "ControlPlane"),
    ("CommitWorkItemResult", "ControlPlane"),
    ("CancelWorkItem", "ControlPlane"),
    ("DeferWorkItem", "ControlPlane"),
    ("CasWorkItemMetadata", "ControlPlane"),
    ("ReserveWorkItemResources", "ControlPlane"),
    ("ReleaseWorkItemResources", "ControlPlane"),
    ("ReclaimWorkItemResources", "ControlPlane"),
    ("UpdateResourceHost", "ControlPlane"),
    ("AcquireCapacity", "ControlPlane"),
    ("RenewCapacity", "ControlPlane"),
    ("ReleaseCapacity", "ControlPlane"),
    ("ReclaimExpiredCapacity", "ControlPlane"),
    ("UpdateCapacityCell", "ControlPlane"),
    ("Sql", "SqlCatalog"),
    ("AddTriples", "RdfDataset"),
    ("RemoveTriples", "RdfDataset"),
    ("DropNamedGraph", "RdfDataset"),
    ("DeclareExchange", "Broker"),
    ("DeleteExchange", "Broker"),
    ("BindQueue", "Broker"),
    ("UnbindQueue", "Broker"),
    ("Publish", "Broker"),
    ("DeclareQueue", "Broker"),
    ("PublishEx", "Broker"),
    ("BrokerConsume", "Broker"),
    ("BrokerAck", "Broker"),
    ("BrokerReject", "Broker"),
    ("SweepExpired", "Broker"),
    ("StreamDeclare", "Broker"),
    ("StreamPublish", "Broker"),
    ("StreamTrim", "Broker"),
    ("StreamCommitOffset", "Broker"),
    ("PublishConfirmed", "Broker"),
    ("PublishIdempotent", "Broker"),
    ("BrokerAckTag", "Broker"),
    ("BrokerNackTag", "Broker"),
    ("BrokerRenewTag", "Broker"),
    // The two tail arms: a `Transaction`-surface method with no explicit arm is
    // a graph-row write, anything else is a graph-snapshot write. `AddEmbedding`
    // reaches the second one.
    ("_", "GraphRows"),
    ("_", "GraphSnapshot"),
];

/// Read the `Method -> DurabilityDomain` arms back out of `canonical.rs`.
///
/// Deliberately a reader over the classifier's own text rather than a second
/// hand-maintained table: a second table would drift, and a hash over the file
/// would fail on a comment. Arms accumulate `Method::Name` tokens until the arm's
/// `DurabilityDomain::Name` is reached; a wildcard arm names no method and is
/// recorded as `_`.
fn classification_map_from_source() -> Vec<(String, String)> {
    let source = include_str!("canonical.rs");
    let start = source
        .find("pub(crate) fn domain_for")
        .expect("canonical.rs declares domain_for");
    let body = &source[start..];
    let end = body.find("\n}\n").expect("domain_for has a closing brace");
    let mut pending: Vec<String> = Vec::new();
    let mut map: Vec<(String, String)> = Vec::new();
    for line in body[..end].lines() {
        let line = line.trim();
        if line.starts_with("//") {
            continue;
        }
        for token in line.match_indices("Method::") {
            pending.push(identifier_after(line, token.0 + "Method::".len()));
        }
        if let Some(at) = line.find("DurabilityDomain::") {
            let domain = identifier_after(line, at + "DurabilityDomain::".len());
            if pending.is_empty() {
                map.push(("_".to_string(), domain));
            } else {
                for method in pending.drain(..) {
                    map.push((method, domain.clone()));
                }
            }
        }
    }
    map
}

fn identifier_after(line: &str, at: usize) -> String {
    line[at..]
        .chars()
        .take_while(|c| c.is_ascii_alphanumeric() || *c == '_')
        .collect()
}

#[test]
fn durability_domain_classification_matches_the_golden() {
    let actual = classification_map_from_source();
    let expected = CLASSIFICATION_GOLDEN
        .iter()
        .map(|(method, domain)| (method.to_string(), domain.to_string()))
        .collect::<Vec<_>>();
    assert_eq!(
        actual, expected,
        "canonical::domain_for reclassified a method. A durability domain decides \
         which store owns the write and its version counter, so this is a data-routing \
         change, never a cleanup: update CLASSIFICATION_GOLDEN in the same commit that \
         changes the arm, and say which authority the method moved to."
    );
}

#[test]
fn classifier_source_map_agrees_with_domain_for() {
    use crate::mutation_batch::DurabilityDomain;
    use crate::server::mutation_batch::domain_for;

    let map = classification_map_from_source();
    let lookup = |name: &str| {
        map.iter()
            .find(|(method, _)| method == name)
            .map(|(_, domain)| domain.clone())
            .unwrap_or_else(|| panic!("{name} is missing from the source classification map"))
    };
    // Constructed methods, classified by the COMPILED function, compared with the
    // map the golden is read from -- so a reader that silently stopped matching
    // cannot make the golden vacuous.
    let check = |method: Method, expected: DurabilityDomain, name: &str| {
        assert_eq!(domain_for(&method, MutationSurface::Graph), expected);
        assert_eq!(lookup(name), format!("{expected:?}"));
    };
    check(
        Method::CreateGraph {
            graph_name: "g".into(),
            graph_type: crate::protocol::GraphType::Global,
        },
        DurabilityDomain::Lifecycle,
        "CreateGraph",
    );
    #[cfg(feature = "kv")]
    check(
        Method::KvDelete {
            namespace: "ns".into(),
            key: "k".into(),
        },
        DurabilityDomain::KvStore,
        "KvDelete",
    );
    #[cfg(feature = "rdf")]
    check(
        Method::DropNamedGraph,
        DurabilityDomain::RdfDataset,
        "DropNamedGraph",
    );
    // The two tail arms, which no explicit name reaches.
    assert_eq!(
        domain_for(
            &Method::AddEmbedding {
                node_id: "n".into(),
                embedding: vec![0.0],
            },
            MutationSurface::Transaction,
        ),
        DurabilityDomain::GraphRows,
    );
    assert_eq!(
        domain_for(
            &Method::AddEmbedding {
                node_id: "n".into(),
                embedding: vec![0.0],
            },
            MutationSurface::Graph,
        ),
        DurabilityDomain::GraphSnapshot,
    );
}

#[cfg(feature = "kv")]
#[test]
fn graph_routed_compiler_refuses_a_store_authoritative_method() {
    // Planted known-bad input: a KV method handed to the graph-routed compiler.
    // Before the scope derivation this fell through to `MutationBatch::validate`,
    // which names neither the method nor the reason.
    let error = compile_methods(
        CompileBatch {
            batch_id: "store-authoritative",
            request_id: 1,
            attempt_nonce: None,
            principal: Some("agent:a"),
            tenant: "tenant-a",
            graph: "graph-a",
            placement_epoch: 0,
            idempotency_key: "store-authoritative",
            expected_graph_version: Some(0),
            fencing_token: None,
            created_at_ms: 1,
            default_surface: MutationSurface::Other,
            authoritative_state: None,
        },
        vec![Method::KvDelete {
            namespace: "ns".into(),
            key: "k".into(),
        }],
    )
    .expect_err("a store-authoritative method has no graph-committed route");
    assert!(
        error.contains("kv_store") && error.contains("store-authoritative"),
        "the refusal must name the domain that owns the write: {error}"
    );
}

// ---------------------------------------------------------------------------
// Owner-store batch principal (RF-RULING-004/005 / B12)
// ---------------------------------------------------------------------------

/// Uniform rule (RF-RULING-004 application note): a graph batch names the same
/// serving principal an owner-store batch does, and the caller is the header.
///
/// This test asserted the opposite until the shard became a kernel-owned store:
/// while `graph-N.redb` was a raw file its ledger keyed replay on the caller, so
/// the caller WAS the principal its ledger required. Now every shard row is an
/// owner row of `OwnerLayout::GraphShard`, admitted through `owner_rows`, which
/// accepts only the file's serving principal.
#[test]
fn a_graph_batch_names_the_serving_principal_and_carries_the_caller_actor() {
    let batch = compile_methods(
        CompileBatch {
            batch_id: "graph-principal",
            request_id: 1,
            attempt_nonce: None,
            principal: Some("agent:a"),
            tenant: "tenant-a",
            graph: "graph-a",
            placement_epoch: 0,
            idempotency_key: "graph-principal",
            expected_graph_version: Some(0),
            fencing_token: None,
            created_at_ms: 1,
            default_surface: MutationSurface::Graph,
            authoritative_state: None,
        },
        vec![Method::RemoveNode {
            node_id: "a".into(),
        }],
    )
    .unwrap();
    let actor = crate::server::mutation_batch::principal_fingerprint("agent:a").unwrap();
    assert_eq!(
        batch.serving_principal(),
        crate::server::mutation_batch::ENGINE_LEDGER_PRINCIPAL,
        "a graph batch is admitted through the same owner-row path as any other"
    );
    assert_ne!(
        batch.serving_principal(),
        actor,
        "the caller must not survive as the batch's serving principal on any domain"
    );
    assert_eq!(
        batch
            .envelope
            .operation()
            .expect("a compiled batch is a caller operation")
            .authority
            .actor
            .as_str(),
        actor,
        "the caller must be inside the stable operation replay identity"
    );
    assert_eq!(
        batch.outbox[0].headers.get("actor"),
        Some(&actor),
        "the verified caller is not lost: it is the outbox row's actor"
    );
}

/// The rule is uniform, so it holds across the domain axis rather than only on
/// the two arms that happened to be exercised above.
#[test]
fn no_compiled_batch_on_any_domain_carries_the_caller_as_its_context_principal() {
    let actor = crate::server::mutation_batch::principal_fingerprint("agent:a").unwrap();
    let compile = |batch_id: &str, methods: Vec<Method>| {
        compile_methods(
            CompileBatch {
                batch_id,
                request_id: 1,
                attempt_nonce: None,
                principal: Some("agent:a"),
                tenant: "tenant-a",
                graph: "graph-a",
                placement_epoch: 0,
                idempotency_key: batch_id,
                expected_graph_version: Some(0),
                fencing_token: None,
                created_at_ms: 1,
                default_surface: MutationSurface::Graph,
                authoritative_state: None,
            },
            methods,
        )
    };
    for (tag, methods) in [
        (
            "remove-node",
            vec![Method::RemoveNode {
                node_id: "a".into(),
            }],
        ),
        (
            "remove-edge",
            vec![Method::RemoveEdge {
                source_id: "a".into(),
                target_id: "b".into(),
            }],
        ),
    ] {
        let batch = compile(tag, methods).unwrap();
        assert_eq!(
            batch.serving_principal(),
            crate::server::mutation_batch::ENGINE_LEDGER_PRINCIPAL,
            "{tag}"
        );
        assert_eq!(batch.outbox[0].headers.get("actor"), Some(&actor), "{tag}");
    }
}

#[cfg(feature = "kv")]
#[test]
fn owner_store_batch_names_the_serving_principal_and_carries_the_caller_actor() {
    let batch = crate::server::mutation_batch::compile_opaque_method(
        CompileBatch {
            batch_id: "kv-principal",
            request_id: 1,
            attempt_nonce: None,
            principal: Some("agent:a"),
            tenant: "tenant-a",
            graph: "kv-scope-a",
            placement_epoch: 0,
            idempotency_key: "kv-principal",
            expected_graph_version: Some(0),
            fencing_token: None,
            created_at_ms: 1,
            default_surface: MutationSurface::Other,
            authoritative_state: None,
        },
        &Method::KvDelete {
            namespace: "ns".into(),
            key: "k".into(),
        },
        MutationSurface::Other,
        crate::mutation_batch::DurabilityDomain::KvStore,
        "kv_operation",
    )
    .unwrap();
    assert_eq!(
        batch.serving_principal(),
        crate::store_authority::ENGINE_PRINCIPAL,
        "an owner-store batch must name the principal the store's serving scope is \
         bound under, or `AdmittedMutation::owner_rows` refuses it"
    );
    assert_eq!(
        batch.outbox[0].headers.get("actor"),
        Some(&crate::server::mutation_batch::principal_fingerprint("agent:a").unwrap()),
        "the verified caller is not lost: it is the outbox row's actor"
    );
}

fn compile_sql_source_dirty_batch(
    request_id: u64,
    attempt_nonce: eg_types::contract::Nonce,
    expected_version: u64,
) -> (Method, MutationBatch) {
    let method = Method::Sql {
        query: "CREATE TABLE semantic_dirty_rows (id BIGINT PRIMARY KEY)".into(),
        params_msgpack: Vec::new(),
    };
    let batch = crate::server::mutation_batch::compile_opaque_method(
        CompileBatch {
            batch_id: "sql-source-dirty-stable-batch",
            request_id,
            attempt_nonce: Some(attempt_nonce),
            principal: Some("agent:semantic-source-writer"),
            tenant: "tenant-semantic-source",
            graph: "graph-semantic-source",
            placement_epoch: 0,
            idempotency_key: "sql-source-dirty-stable-key",
            expected_graph_version: Some(expected_version),
            fencing_token: None,
            created_at_ms: request_id,
            default_surface: MutationSurface::Query,
            authoritative_state: None,
        },
        &method,
        MutationSurface::Query,
        crate::mutation_batch::DurabilityDomain::SqlCatalog,
        "sql_catalog_operation",
    )
    .unwrap();
    (method, batch)
}

#[test]
fn sql_catalog_compile_declares_one_retry_stable_typed_source_dirty_intent() {
    use sha2::Digest as _;

    let (method, first) =
        compile_sql_source_dirty_batch(11, eg_types::contract::Nonce::from_bytes([11; 32]), 0);
    let (_, retry) =
        compile_sql_source_dirty_batch(12, eg_types::contract::Nonce::from_bytes([12; 32]), 1);
    let source_dirty = first
        .outbox
        .iter()
        .filter(|intent| intent.topic == eg_types::semantic_index::SEMANTIC_SOURCE_DIRTY_TOPIC)
        .collect::<Vec<_>>();
    assert_eq!(source_dirty.len(), 1);
    let decoded = eg_types::semantic_index::SemanticSourceDirtyIntent::from_canonical_cbor(
        &source_dirty[0].payload,
    )
    .unwrap();
    assert_eq!(
        decoded.source_scope_digest,
        eg_types::semantic_index::SemanticDigest::from_bytes(
            *first.identity.binding_digest().as_bytes()
        )
    );
    let encoded_method = rmp_serde::to_vec_named(&method).unwrap();
    assert_eq!(
        decoded.input_digest,
        eg_types::semantic_index::SemanticDigest::from_bytes(
            sha2::Sha256::digest(encoded_method).into()
        )
    );
    assert_eq!(source_dirty[0].key, first.batch_id);

    let retry_source_dirty = retry
        .outbox
        .iter()
        .find(|intent| intent.topic == eg_types::semantic_index::SEMANTIC_SOURCE_DIRTY_TOPIC)
        .unwrap();
    assert_eq!(
        retry_source_dirty.payload, source_dirty[0].payload,
        "attempt request, nonce, timestamp, and OCC version must not enter the source-dirty payload"
    );
    assert_eq!(retry_source_dirty.key, source_dirty[0].key);

    let non_sql = crate::server::mutation_batch::compile_opaque_method(
        CompileBatch {
            batch_id: "non-sql-opaque",
            request_id: 13,
            attempt_nonce: None,
            principal: Some("agent:semantic-source-writer"),
            tenant: "tenant-semantic-source",
            graph: "graph-semantic-source",
            placement_epoch: 0,
            idempotency_key: "non-sql-opaque",
            expected_graph_version: Some(0),
            fencing_token: None,
            created_at_ms: 13,
            default_surface: MutationSurface::Graph,
            authoritative_state: None,
        },
        &Method::RemoveNode {
            node_id: "node-a".into(),
        },
        MutationSurface::Graph,
        crate::mutation_batch::DurabilityDomain::GraphRows,
        "graph_operation",
    )
    .unwrap();
    assert!(non_sql
        .outbox
        .iter()
        .all(|intent| { intent.topic != eg_types::semantic_index::SEMANTIC_SOURCE_DIRTY_TOPIC }));
}

#[cfg(all(feature = "query", feature = "redb"))]
#[test]
fn sql_source_rows_receipt_version_and_dirty_intent_commit_and_replay_atomically() {
    let (store, _path) = eg_query::TableStore::open_temp().unwrap();
    let mut txn = eg_query::TableTxn::new();
    txn.push(eg_query::TxnOp::CreateTable {
        schema: eg_query::TableSchema::new(
            "semantic_dirty_rows",
            vec![eg_query::Column::new(
                "id",
                eg_query::ColumnType::BigInt,
                false,
                true,
            )],
        ),
        if_not_exists: false,
    });
    let nonce = eg_types::contract::Nonce::from_bytes([21; 32]);
    let (_, first) = compile_sql_source_dirty_batch(21, nonce, 0);
    let committed = store.commit_txn_batch(&txn, &first, 21).unwrap();
    assert!(!committed.replayed);
    assert_eq!(
        committed.record.committed_version,
        crate::mutation_batch::CommittedVersion::Native {
            source: 0,
            target: 1,
        }
    );
    assert_eq!(
        store.list_tables().unwrap(),
        vec!["semantic_dirty_rows".to_string()]
    );
    let durable_outbox = store
        .mutation_outbox(&first.identity, &first.batch_id)
        .unwrap();
    assert_eq!(durable_outbox.len(), first.outbox.len());
    let dirty = durable_outbox
        .iter()
        .find(|record| record.intent.topic == eg_types::semantic_index::SEMANTIC_SOURCE_DIRTY_TOPIC)
        .expect("the SQL owner transaction persists its typed source-dirty intent");
    assert_eq!(dirty.committed_version, committed.record.committed_version);

    let (_, consumed) = compile_sql_source_dirty_batch(22, nonce, 1);
    let error = store.commit_txn_batch(&txn, &consumed, 22).unwrap_err();
    assert!(error.contains("REPLAY_NONCE_CONSUMED"), "{error}");

    let (_, retry) =
        compile_sql_source_dirty_batch(23, eg_types::contract::Nonce::from_bytes([23; 32]), 1);
    let replayed = store.commit_txn_batch(&txn, &retry, 23).unwrap();
    assert!(replayed.replayed);
    assert_eq!(replayed.record.committed_version, dirty.committed_version);
    assert_eq!(
        store
            .mutation_outbox(&retry.identity, &retry.batch_id)
            .unwrap(),
        durable_outbox,
        "a fresh-nonce retry must not duplicate the source-dirty event"
    );
    assert_eq!(
        store.list_tables().unwrap(),
        vec!["semantic_dirty_rows".to_string()],
        "replay must not execute the SQL owner rows a second time"
    );
}

/// The whole B12 defect, end to end: compile a served owner-store batch, admit it
/// through the mutation kernel, take the owner-row capability, commit, and read
/// the caller back out of the DURABLE record.
///
/// Before the fix `owner_rows` rejected this batch with "owner write capability
/// does not match admitted batch" -- it compiled and every gate stayed green,
/// while every served KV/blob/time-series/analytics-job write failed at runtime.
#[cfg(all(feature = "kv", feature = "redb"))]
#[test]
fn served_owner_store_batch_commits_and_its_actor_survives_in_the_ledger() {
    use eg_storage::{KvOwner, PhysicalStoreIdentity, StorageKernel};
    use eg_transaction::{Begin, MutationKernel};

    let dir = crate::test_support::temp_dir("eg-mb", "owner-principal");
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("kv.redb");

    let authority = crate::store_authority::process_authority();
    let kernel = StorageKernel::create_owner::<KvOwner>(
        &path,
        PhysicalStoreIdentity::new("epistemic-graph:mutation-batch-owner-principal-test").unwrap(),
        None,
    )
    .unwrap();
    let (kernel, owner_authority) = kernel.into_read_and_mutation_authority().unwrap();
    let mutations = MutationKernel::new(owner_authority);

    let identity = eg_types::MutationScopeIdentity::native(
        eg_types::ScopeTenantId::new("tenant-a".to_string()).unwrap(),
        crate::mutation_batch::DurabilityDomain::KvStore,
        eg_types::LogicalName::new("kv-scope-a".to_string()).unwrap(),
        eg_types::IncarnationId::new(crate::server::mutation_batch::COMPILED_BATCH_INCARNATION)
            .unwrap(),
    )
    .unwrap();
    let grant = kernel
        .authenticate_scope::<KvOwner>(
            authority.as_ref(),
            identity.clone(),
            authority.principal().to_string(),
            &authority.proof(),
        )
        .unwrap();
    let owner = kernel.bind_serving_scope(grant, 0).unwrap();
    mutations.bootstrap_ledger(&owner).unwrap();

    let batch = crate::server::mutation_batch::compile_opaque_method(
        CompileBatch {
            batch_id: "kv-served",
            request_id: 7,
            attempt_nonce: None,
            principal: Some("agent:a"),
            tenant: "tenant-a",
            graph: "kv-scope-a",
            placement_epoch: 0,
            idempotency_key: "kv-served",
            expected_graph_version: Some(0),
            fencing_token: None,
            created_at_ms: 5,
            default_surface: MutationSurface::Other,
            authoritative_state: None,
        },
        &Method::KvDelete {
            namespace: "ns".into(),
            key: "k".into(),
        },
        MutationSurface::Other,
        crate::mutation_batch::DurabilityDomain::KvStore,
        "kv_operation",
    )
    .unwrap();
    assert_eq!(batch.identity, identity);

    let (write, begun) = mutations.admit(&owner, &batch).unwrap();
    let source_version = match begun {
        Begin::Apply { source_version } => source_version,
        Begin::Replay(_) => panic!("a fresh batch is not a replay"),
    };
    let rows = write
        .owner_rows(&owner, &batch)
        .expect("a served owner-store batch must be admissible for owner rows");
    rows.finish_owner().unwrap();
    mutations
        .finish(&write, &batch, None, 5, source_version)
        .unwrap();
    mutations.commit(write, &batch).unwrap();

    let read = kernel.read_scope(&owner).unwrap();
    let record = eg_transaction::read_ledger(&read, "kv-served")
        .unwrap()
        .expect("the committed batch has a durable receipt");
    assert_eq!(
        record.status,
        crate::mutation_batch::MutationBatchStatus::Committed
    );
    assert_eq!(
        record.batch.serving_principal(),
        crate::store_authority::ENGINE_PRINCIPAL
    );
    assert_eq!(
        record.batch.outbox[0].headers.get("actor"),
        Some(&crate::server::mutation_batch::principal_fingerprint("agent:a").unwrap()),
        "the verified caller must be recoverable from the durable record"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

/// The two reserved shard identifiers are refused to a request-boundary caller.
///
/// A graph-shard scope is `(GRAPH_SHARD_TENANT, graph, incarnation)` and
/// `GRAPH_SHARD_CONTROL_GRAPH` is the shard file's own control scope, so a
/// caller able to name either could compile a batch whose scope identity
/// collides with the shard's own -- taking its OCC counter and fence, or writing
/// under its control scope. This is the ONLY place the tenant half can be
/// caught: nothing below the compiler ever sees a tenant.
#[test]
fn a_caller_cannot_name_either_reserved_graph_shard_identifier() {
    let compile_with = |tenant: &str, graph: &str| {
        compile_methods(
            CompileBatch {
                batch_id: "reserved",
                request_id: 1,
                attempt_nonce: None,
                principal: Some("agent:a"),
                tenant,
                graph,
                placement_epoch: 0,
                idempotency_key: "reserved",
                expected_graph_version: Some(0),
                fencing_token: None,
                created_at_ms: 1,
                default_surface: MutationSurface::Graph,
                authoritative_state: None,
            },
            vec![Method::RemoveNode {
                node_id: "a".into(),
            }],
        )
    };

    #[cfg(feature = "redb")]
    {
        let tenant_error = compile_with("__shard__", "graph-a").unwrap_err();
        assert!(
            tenant_error.contains("reserved scope tenant"),
            "{tenant_error}"
        );
        let graph_error = compile_with("tenant-a", "__shard_control__").unwrap_err();
        assert!(
            graph_error.contains("reserved control scope"),
            "{graph_error}"
        );
    }
    #[cfg(not(feature = "redb"))]
    {
        let tenant_error = compile_with("__shard__", "graph-a").unwrap_err();
        assert_eq!(
            tenant_error,
            "reserved graph-shard identifier cannot be supplied by a caller"
        );
        let graph_error = compile_with("tenant-a", "__shard_control__").unwrap_err();
        assert_eq!(
            graph_error,
            "reserved graph-shard identifier cannot be supplied by a caller"
        );
    }

    // The guard is those two exact names, not a `__…__` shape: `__commons__` is
    // a real user-visible graph and must stay compilable.
    let ok = compile_with("tenant-a", "__commons__").unwrap();
    assert_eq!(
        ok.identity.scope().graph_name().map(|n| n.as_str()),
        Some("__commons__")
    );
}

/// A compiled batch's envelope covers its OWN body, so editing the body after
/// the mint is refused rather than committed.
///
/// This is the property that makes a post-compile outbox append unreachable
/// instead of merely discouraged. Two producers used to do exactly that --
/// `compile_crossmodal` pushed its manifest intent onto the finished batch, and
/// the reasoning wake-up rewrote the projection intent's payload in place -- and
/// both are now inputs to `finish_batch` rather than edits after it. The digest
/// they would have invalidated is checked here on both of the fields it covers,
/// so a third producer cannot reintroduce the shape.
///
/// It matters because the digest decides REPLAY: an envelope covering bytes the
/// batch no longer has could resolve a retry against a recorded result whose
/// outbox or operations differ from what this attempt proposed.
#[test]
fn editing_a_compiled_batch_after_the_mint_is_refused() {
    let compiled = || {
        compile_methods(
            CompileBatch {
                batch_id: "post-mint-edit",
                request_id: 1,
                attempt_nonce: None,
                principal: Some("agent:a"),
                tenant: "tenant-a",
                graph: "graph-a",
                placement_epoch: 0,
                idempotency_key: "post-mint-edit",
                expected_graph_version: Some(0),
                fencing_token: None,
                created_at_ms: 1,
                default_surface: MutationSurface::Graph,
                authoritative_state: None,
            },
            vec![Method::RemoveNode {
                node_id: "a".into(),
            }],
        )
        .unwrap()
    };
    compiled()
        .validate()
        .expect("a freshly compiled batch is valid");

    // An outbox intent appended after the mint.
    let mut appended = compiled();
    appended
        .outbox
        .push(crate::mutation_batch::MutationOutboxIntent {
            topic: "engine.crossmodal.committed".to_string(),
            key: "post-mint-edit".to_string(),
            payload: vec![7],
            headers: Default::default(),
        });
    let error = appended.validate().unwrap_err();
    assert!(
        error.contains("canonical payload digest"),
        "an appended intent must be refused, got: {error}"
    );

    // An operation rewritten after the mint.
    let mut rewritten = compiled();
    rewritten.operations[0].method = Method::RemoveNode {
        node_id: "some-other-node".into(),
    };
    let error = rewritten.validate().unwrap_err();
    assert!(
        error.contains("canonical payload digest"),
        "a rewritten operation must be refused, got: {error}"
    );

    // And what a PRODUCER does instead -- mint over the final body -- is
    // accepted: the check is about the envelope describing its batch, not about
    // the body being frozen.
    let mut resealed = appended;
    let schema_digest = resealed
        .envelope
        .operation()
        .expect("a compiled batch is a caller operation")
        .method_schema_digest;
    resealed
        .reseal_envelope(schema_digest)
        .expect("a re-minted envelope covers the batch it describes");
    resealed
        .validate()
        .expect("a batch whose envelope covers its own body is valid");
}
