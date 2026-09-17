use super::*;
use eg_query::Cell;
use eg_types::storage_wire::SqlSourceCell;

mod served;
mod support;
use support::*;

#[test]
fn write_scope_and_existing_insert_grant_are_both_required() {
    let fixture = Fixture::new();
    fixture.grant_insert("bob");
    let request = fixture.request("bob");
    let read_only = authority("bob", "tenant-a", "read-only", false);
    assert!(submit(&fixture, &read_only, request.clone(), 1)
        .unwrap_err()
        .contains("kg:write"));
    let ungranted = authority("carol", "tenant-a", "ungranted", true);
    assert!(submit(&fixture, &ungranted, request.clone(), 2)
        .unwrap_err()
        .contains("ACCESS_DENIED"));
    assert!(fixture.store().scan("issues").unwrap().is_empty());
    let granted = authority("bob", "tenant-a", "granted", true);
    let result = submit(&fixture, &granted, request, 3).unwrap();
    assert_eq!(result.affected_count, 1);
}

#[test]
fn typed_rls_requires_explicit_matching_stamp_and_never_rewrites_cells() {
    let fixture = Fixture::new();
    fixture.grant_insert("bob");
    fixture.enable_rls();
    let carrier = authority("bob", "tenant-a", "rls", true);
    let matching = fixture.request("bob");
    let omitted = change(&matching, |batch| {
        batch.columns = eg_types::contract::BoundedVec::new(vec![id("id"), id("payload")]).unwrap();
        batch.rows =
            eg_types::contract::BoundedVec::new(vec![eg_types::contract::BoundedVec::new(vec![
                batch.rows.as_slice()[0].as_slice()[0].clone(),
                batch.rows.as_slice()[0].as_slice()[2].clone(),
            ])
            .unwrap()])
            .unwrap();
    });
    assert!(submit(&fixture, &carrier, omitted, 1)
        .unwrap_err()
        .contains("ACCESS_DENIED"));
    assert!(submit(&fixture, &carrier, fixture.request("alice"), 2)
        .unwrap_err()
        .contains("ACCESS_DENIED"));
    let wrong_type = change(&matching, |batch| {
        let mut rows = batch.rows.as_slice().to_vec();
        let mut row = rows.remove(0).as_slice().to_vec();
        row[1] = SqlSourceCell::Json(
            eg_types::storage_wire::SqlSourceJson::new(serde_json::json!("bob")).unwrap(),
        );
        batch.rows = eg_types::contract::BoundedVec::new(vec![
            eg_types::contract::BoundedVec::new(row).unwrap(),
        ])
        .unwrap();
    });
    assert!(submit(&fixture, &carrier, wrong_type, 3)
        .unwrap_err()
        .contains("ACCESS_DENIED"));
    assert!(fixture.store().scan("issues").unwrap().is_empty());
    let before = matching.canonical_bytes().unwrap();
    let result = submit(&fixture, &carrier, matching.clone(), 4).unwrap();
    assert_eq!(matching.canonical_bytes().unwrap(), before);
    assert_eq!(
        result.canonical_digests,
        matching.canonical_digests().unwrap()
    );
    assert_eq!(
        fixture.store().scan("issues").unwrap()[0][2],
        Cell::Json(serde_json::Value::Null)
    );
}

#[test]
fn tenant_and_grant_isolation_ignore_descriptor_claims() {
    let fixture = Fixture::new();
    fixture.grant_insert("bob");
    let request = fixture.request("bob");
    let other_tenant = authority("bob", "tenant-b", "tenant-isolation", true);
    assert!(submit(&fixture, &other_tenant, request.clone(), 1)
        .unwrap_err()
        .contains("ACCESS_DENIED"));
    assert!(fixture.store().scan("issues").unwrap().is_empty());
    fixture.evict_tenant(other_tenant.tenant_scope());
    let ungranted = authority("carol", "tenant-a", "descriptor-isolation", true);
    let claims = change(&request, |batch| {
        batch.source_descriptor.metadata = eg_types::storage_wire::SqlSourceJson::new(
            serde_json::json!({"owner":"carol","permission":"insert","tenant":"tenant-a"}),
        )
        .unwrap();
    });
    assert!(submit(&fixture, &ungranted, claims, 2)
        .unwrap_err()
        .contains("ACCESS_DENIED"));
}

#[test]
fn fresh_nonce_replay_returns_original_typed_result_without_duplicate_publication() {
    let fixture = Fixture::new();
    fixture.grant_insert("bob");
    let carrier = authority("bob", "tenant-a", "replay", true);
    let request = fixture.request("bob");
    let first = submit(&fixture, &carrier, request.clone(), 1).unwrap();
    let replay = submit(&fixture, &carrier, request.clone(), 2).unwrap();
    assert_eq!(replay, first);
    assert_eq!(fixture.store().scan("issues").unwrap().len(), 1);
    assert!(submit(&fixture, &carrier, request.clone(), 2).is_err());
    let altered = change(&request, |batch| {
        batch.mapping_descriptor.content =
            eg_types::contract::RecordBytes::new(b"different mapping".to_vec()).unwrap();
    });
    assert!(submit(&fixture, &carrier, altered, 3)
        .unwrap_err()
        .contains("IDEMPOTENCY_CONFLICT"));
    assert_eq!(fixture.store().scan("issues").unwrap().len(), 1);
}

#[test]
fn append_preserves_sql_null_and_json_null_in_one_batch() {
    let fixture = Fixture::new();
    fixture.grant_insert("bob");
    let carrier = authority("bob", "tenant-a", "nulls", true);
    let request = change(&fixture.request("bob"), |batch| {
        let first = batch.rows.as_slice()[0].as_slice().to_vec();
        let mut second = first.clone();
        second[0] = SqlSourceCell::Int(2);
        second[2] = SqlSourceCell::Null;
        batch.rows = eg_types::contract::BoundedVec::new(vec![
            eg_types::contract::BoundedVec::new(first).unwrap(),
            eg_types::contract::BoundedVec::new(second).unwrap(),
        ])
        .unwrap();
    });
    let result = submit(&fixture, &carrier, request, 1).unwrap();
    assert_eq!(result.affected_count, 2);
    let rows = fixture.store().scan("issues").unwrap();
    assert_eq!(rows[0][2], Cell::Json(serde_json::Value::Null));
    assert_eq!(rows[1][2], Cell::Null);
}

#[tokio::test]
async fn terminal_response_uses_raw_typed_result_and_verified_nonce() {
    let fixture = Fixture::new();
    fixture.grant_insert("bob");
    let template = verified("bob", "tenant-a", "terminal", true);
    let verified = VerifiedRequestContext::from_verified_claims_with_nonce(
        template.claims().clone(),
        "terminal".into(),
        Some(Nonce::from_bytes([9; 32])),
    );
    let mut state = ServerState::new("test", crate::isolation::IsolationLayer::new());
    state.persist_dir = Some(fixture.directory.to_str().unwrap().into());
    let state = Arc::new(RwLock::new(state));
    let request = fixture.request("bob");
    let response = handle(&state, 99, &verified, request.clone()).await;
    assert_eq!(response.id, 99);
    assert!(response.error.is_none(), "{:?}", response.error);
    let Some(ResultPayload::Raw(bytes)) = response.result else {
        panic!("result must be raw MessagePack")
    };
    let result: SqlSourceBatchResult = eg_storage::decode_ledger_record(&bytes).unwrap();
    assert_eq!(
        result.canonical_digests,
        request.canonical_digests().unwrap()
    );
    let duplicate = handle(&state, 100, &verified, request).await;
    assert!(duplicate.error.is_some());
    assert_eq!(fixture.store().scan("issues").unwrap().len(), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelling_publication_waiter_does_not_cancel_owned_commit() {
    let fixture = Fixture::new();
    fixture.grant_insert("bob");
    let carrier = authority("bob", "tenant-a", "cancel", true);
    let request = fixture.request("bob");
    let directory = fixture.directory.clone();
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = std::sync::mpsc::sync_channel(1);
    let (done_tx, done_rx) = tokio::sync::oneshot::channel();
    let waiter = tokio::spawn(run_publication_job(move || {
        let _ = started_tx.send(());
        release_rx
            .recv_timeout(std::time::Duration::from_secs(10))
            .map_err(|_| "publication release timed out")?;
        let result = publish(
            10,
            &carrier,
            Some(Nonce::from_bytes([1; 32])),
            request,
            &directory,
            100,
        );
        let _ = done_tx.send(result.clone());
        result
    }));
    let started = tokio::time::timeout(std::time::Duration::from_secs(10), started_rx).await;
    waiter.abort();
    release_tx.send(()).unwrap();
    assert!(started.unwrap().is_ok());
    let result = tokio::time::timeout(std::time::Duration::from_secs(10), done_rx)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(result.affected_count, 1);
    assert_eq!(fixture.store().scan("issues").unwrap().len(), 1);
}

#[cfg(feature = "raft")]
#[test]
fn active_raft_refuses_local_sql_source_authority_before_publication() {
    let fixture = Fixture::new();
    fixture.grant_insert("bob");
    let carrier = authority("bob", "tenant-a", "raft", true);
    let request = fixture.request("bob");
    let _environment_lock = crate::crypto::acquire_test_env_lock_blocking();
    let _environment =
        crate::server::persistence::backup::EnvVarGuard::set("EPISTEMIC_GRAPH_RAFT_NODE_ID", "1");
    assert!(submit(&fixture, &carrier, request, 1)
        .unwrap_err()
        .contains("no replicated ordering"));
    assert!(fixture.store().scan("issues").unwrap().is_empty());
}

#[test]
fn audit_line_carries_only_the_canonical_batch_digest() {
    let fixture = Fixture::new();
    let request = fixture.request("bob");
    let expected = request.canonical_digests().unwrap().batch_digest.to_hex();
    let line =
        crate::audit::audit_line(&crate::protocol::Method::SqlSourceBatch { batch: request })
            .unwrap();
    assert_eq!(line, format!("SQL_SOURCE_BATCH_MUTATION|sha256:{expected}"));
    for leaked in ["jira", "issues", "project-a", "issue_id", "deployment"] {
        assert!(!line.contains(leaked), "audit line leaked {leaked}");
    }
}
