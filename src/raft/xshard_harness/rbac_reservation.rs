//! Real coordinator/owner composition, including the crash between decision and apply.
use super::*;
use eg_core::rbac_persist::{RbacPersistError, RbacReservationRequest, RbacStore};
use eg_storage::{OwnerLayout, PhysicalStoreIdentity, ScopeGrantVerifier};

const PRINCIPAL: &str =
    "principal:sha256:d70d97fc35a6e2dfbef26a2bca76a96c6dd2c4142ae2a14850deaf61b478bba0";
struct OwnerVerifier;
impl ScopeGrantVerifier for OwnerVerifier {
    fn verify(
        &self,
        physical: &PhysicalStoreIdentity,
        layout: OwnerLayout,
        identity: &eg_types::MutationScopeIdentity,
        principal: &str,
        proof: &[u8],
    ) -> Result<(), String> {
        if *physical != PhysicalStoreIdentity::new("eg-core:rbac-security-control")?
            || layout != OwnerLayout::Rbac
            || identity.tenant().as_str() != "native"
            || principal != PRINCIPAL
            || proof != b"rbac-participant-test"
        {
            return Err("wrong test owner grant".into());
        }
        Ok(())
    }
}
fn open_owner(directory: &str) -> RbacStore {
    RbacStore::open(
        std::path::Path::new(directory).join("rbac-owner"),
        &OwnerVerifier,
        PRINCIPAL,
        b"rbac-participant-test",
    )
    .unwrap()
}
fn request(store: &RbacStore, txn: &CrossShardTxn) -> RbacReservationRequest {
    let digest = txn.rbac_read_request_digest().unwrap();
    RbacReservationRequest::new(
        &store.authority_snapshot().unwrap(),
        eg_transaction::CoordinatorDecisionBinding {
            transaction_id: txn.txn_id.clone(),
            tenant: "tenant-test".into(),
            coordinator_scope: "fixture-coordinator".into(),
            request_digest: hex::decode(digest).unwrap().try_into().unwrap(),
            authority_deadline_ms: u64::MAX,
        },
    )
    .unwrap()
}
fn save(store: &RbacStore) -> Result<u64, RbacPersistError> {
    let image = store.authority_snapshot()?;
    store.compare_and_save(
        image.revision(),
        image.policy(),
        image.identities(),
        image.bootstrap(),
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rbac_read_participant_commits_and_replays_after_parent_decision_collection() {
    let _environment = env_read_guard().await;
    let directory = fresh_dir("rbac-commit");
    let backend = fixture::open_backend(&directory).unwrap();
    let (multi, coordinator, state) = bring_up(&directory, backend.clone()).await;
    let store = open_owner(&directory);
    let txn = two_shard_txn("rbac-commit", "a-rbac", "b-rbac");
    let request = request(&store, &txn);
    assert_eq!(
        coordinator
            .commit_cross_shard_with_rbac_read(&txn, &store, request.clone())
            .await
            .unwrap(),
        TxnOutcome::Committed
    );
    assert!(store.pending_reservation().unwrap().is_none());
    save(&store).unwrap();
    coordinator
        .clear_recoverable_decision(&txn.txn_id)
        .await
        .unwrap();
    assert_eq!(
        coordinator
            .commit_cross_shard_with_rbac_read(&txn, &store, request)
            .await
            .unwrap(),
        TxnOutcome::Committed
    );
    assert_eq!(node_count(&state, GRAPH_A).await, 1);
    assert_eq!(node_count(&state, GRAPH_B).await, 1);
    drop(store);
    multi.shutdown().await;
    let _ = std::fs::remove_dir_all(directory);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rbac_read_recovery_retains_missing_decision_and_finishes_graphs_before_release() {
    let _environment = env_read_guard().await;
    let directory = fresh_dir("rbac-recover");
    let backend = fixture::open_backend(&directory).unwrap();
    let (multi, coordinator, state) = bring_up(&directory, backend.clone()).await;
    let txn = two_shard_txn("rbac-recover", "a-recover", "b-recover");
    {
        let store = open_owner(&directory);
        store.prepare_reservation(request(&store, &txn)).unwrap();
        assert!(coordinator.prepare_only(&txn).await.unwrap());
        assert!(!coordinator
            .recover_rbac_read_participant(&store)
            .await
            .unwrap());
        assert!(matches!(
            save(&store),
            Err(RbacPersistError::Reserved { .. })
        ));
        let binding = store
            .pending_reservation()
            .unwrap()
            .unwrap()
            .request()
            .binding()
            .clone();
        backend
            .as_redb()
            .unwrap()
            .xshard_certified_decision_put(binding, true)
            .await
            .unwrap();
    }
    {
        let store = open_owner(&directory);
        assert!(matches!(
            save(&store),
            Err(RbacPersistError::Reserved { .. })
        ));
        assert_eq!(node_count(&state, GRAPH_A).await, 0);
        assert!(coordinator
            .recover_rbac_read_participant(&store)
            .await
            .unwrap());
        assert_eq!(node_count(&state, GRAPH_A).await, 1);
        assert_eq!(node_count(&state, GRAPH_B).await, 1);
        save(&store).unwrap();
    }
    multi.shutdown().await;
    let _ = std::fs::remove_dir_all(directory);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mismatched_certificate_or_legacy_decision_does_not_acquire_owner_fence() {
    let _environment = env_read_guard().await;
    let directory = fresh_dir("rbac-decision-refusal");
    let backend = fixture::open_backend(&directory).unwrap();
    let (multi, coordinator, _) = bring_up(&directory, backend.clone()).await;
    let store = open_owner(&directory);
    let txn = two_shard_txn("rbac-wrong-context", "a-wrong", "b-wrong");
    let req = request(&store, &txn);
    let mut binding = req.binding().clone();
    binding.tenant = "other-tenant".into();
    backend
        .as_redb()
        .unwrap()
        .xshard_certified_decision_put(binding, false)
        .await
        .unwrap();
    assert!(coordinator
        .commit_cross_shard_with_rbac_read(&txn, &store, req)
        .await
        .is_err());
    assert!(store.pending_reservation().unwrap().is_none());
    let txn = two_shard_txn("rbac-legacy", "a-legacy", "b-legacy");
    coordinator.decide_only(&txn.txn_id, false).await.unwrap();
    assert!(coordinator
        .commit_cross_shard_with_rbac_read(&txn, &store, request(&store, &txn))
        .await
        .is_err());
    assert!(store.pending_reservation().unwrap().is_none());
    save(&store).unwrap();
    drop(store);
    multi.shutdown().await;
    let _ = std::fs::remove_dir_all(directory);
}
