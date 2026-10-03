//! Operation audit append when the durable writer cannot take it: the caller
//! gets the declared refusal, never a receipt, and nothing is half-applied.

use super::*;
use crate::protocol::Method;
use crate::redb_store::operation_audit::tests::{event, TENANT};
use crate::server::persistence::PersistenceBackend;

const GRAPH: &str = "audit_append_graph";
const REFUSAL: &str = "AUDIT_WRITER_UNAVAILABLE: ";

/// A backend whose graph exists because one ordinary audited write created
/// it: that write is audit-chain entry 0.
async fn seeded(dir: &str) -> RedbBackend {
    let backend = RedbBackend::open_with_shards(dir.to_string(), 64, 1).expect("open");
    let seed = Method::AddNode {
        node_id: "seed".to_string(),
        properties_msgpack: rmp_serde::to_vec_named(&serde_json::json!({})).expect("encode"),
    };
    backend.record_durable(GRAPH, &seed).await.expect("seed");
    backend
}

/// `shutdown` stops the shard's writer thread and leaves the backend object in
/// place, which is exactly the state a request meets when the durable writer
/// is unavailable: the command cannot be handed over. The append is refused
/// with the declared code for a new reservation and for an outcome alike, and
/// after reopening the store holds only what was acknowledged before.
#[tokio::test(flavor = "multi_thread")]
async fn an_append_the_writer_cannot_take_is_refused_and_leaves_no_trace() {
    let _env_read_lock = crate::crypto::acquire_test_env_read_lock().await;
    let dir = tempfile::tempdir().expect("tempdir");
    let dir_s = dir.path().to_string_lossy().to_string();
    let backend = seeded(&dir_s).await;
    let kept = backend
        .audit_append(GRAPH, event("kept", "reserved"))
        .await
        .expect("the writer is up");

    backend.shutdown();
    for refused in [event("lost", "reserved"), event("kept", "ok")] {
        let error = backend.audit_append(GRAPH, refused).await.unwrap_err();
        assert!(error.starts_with(REFUSAL), "{error}");
    }
    drop(backend);

    let reopened = RedbBackend::open_with_shards(dir_s, 64, 1).expect("reopen");
    let proof = reopened
        .audit_read_event(GRAPH, TENANT, kept.seq)
        .await
        .expect("the acknowledged reservation is durable");
    assert!(proof.chain_verified);
    assert_eq!(
        proof.chain_entries, 2,
        "seed and the one acknowledged event"
    );
    let pending = reopened
        .audit_append(GRAPH, event("kept", "reserved"))
        .await
        .expect("replay");
    assert_eq!(
        (pending.replayed, pending.seq, pending.outcome_seq),
        (true, kept.seq, None),
        "the refused outcome was not linked"
    );
    let first = reopened
        .audit_append(GRAPH, event("lost", "reserved"))
        .await
        .expect("append");
    assert_eq!(
        (first.replayed, first.seq),
        (false, kept.seq + 1),
        "the refused reservation left no idempotency row"
    );
    reopened.shutdown();
}

/// A refusal of the event itself is the writer's answer and is passed through
/// unchanged: it is not reported as an unavailable writer.
#[tokio::test(flavor = "multi_thread")]
async fn an_event_refusal_is_not_reported_as_an_unavailable_writer() {
    let _env_read_lock = crate::crypto::acquire_test_env_read_lock().await;
    let dir = tempfile::tempdir().expect("tempdir");
    let backend = seeded(&dir.path().to_string_lossy()).await;
    let mut undeclared = event("request-a", "reserved");
    undeclared.audit_class = String::new();
    let error = backend.audit_append(GRAPH, undeclared).await.unwrap_err();
    assert_eq!(error, "AUDIT_CLASS_REQUIRED");
    let unreserved = backend.audit_append(GRAPH, event("request-a", "ok")).await;
    assert_eq!(unreserved.unwrap_err(), "AUDIT_RESERVATION_REQUIRED");
    backend.shutdown();
}
