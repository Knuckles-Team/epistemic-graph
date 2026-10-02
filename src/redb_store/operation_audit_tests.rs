//! Operation audit against a real shard file: class refusals, reconciliation
//! of a reservation whose outcome was interrupted, and what replicas hold.

use super::*;
use crate::protocol::{AuditAppendReceipt, Method};
use crate::redb_store::{commit_ops, DurableCrypto};

const GRAPH: &str = "g";
pub(crate) const TENANT: &str = "tenant-a";

fn open(dir: &std::path::Path) -> Shard {
    Shard::open(&dir.join("graph-0.redb")).unwrap()
}

/// A shard whose graph exists because one ordinary audited write created it:
/// that write is chain entry 0, so operation events start at sequence 1.
fn seeded(dir: &std::path::Path) -> (Shard, AuditTailCache) {
    let shard = open(dir);
    let mut tail = AuditTailCache::new();
    let seed = Method::AddNode {
        node_id: "seed".to_string(),
        properties_msgpack: rmp_serde::to_vec_named(&serde_json::json!({})).unwrap(),
    };
    commit_ops(
        &shard,
        &mut vec![(GRAPH.to_string(), seed)],
        &mut Vec::new(),
        "operation-audit-seed",
        0,
        DurableCrypto::none(),
        &mut tail,
    )
    .unwrap();
    (shard, tail)
}

/// A valid event of [`TENANT`] with the `event` audit class, shared by every
/// operation-audit test in the crate.
pub(crate) fn event(request_id: &str, status: &str) -> OperationAuditEvent {
    OperationAuditEvent {
        tenant: TENANT.into(),
        principal: "svc:caller".into(),
        op: "graph.nodes.write".into(),
        surface: "http".into(),
        params_sha256: "a".repeat(64),
        status: status.into(),
        request_id: request_id.into(),
        audit_class: "event".into(),
    }
}

/// One store and the audit tail its writer keeps, as the writer thread holds
/// them.
struct Store {
    shard: Shard,
    tail: AuditTailCache,
}

impl Store {
    fn seeded(dir: &std::path::Path) -> Self {
        let (shard, tail) = seeded(dir);
        Self { shard, tail }
    }

    /// Reopen after a stop: a new process starts with a cold tail.
    fn reopened(dir: &std::path::Path) -> Self {
        Self {
            shard: open(dir),
            tail: AuditTailCache::new(),
        }
    }

    fn append(&mut self, event: &OperationAuditEvent) -> Result<AuditAppendReceipt, String> {
        operation_audit_append(&self.shard, &mut self.tail, GRAPH, event)
    }

    /// Every durable audit-chain entry of the graph, proven unbroken.
    fn chain_entries(&self) -> u64 {
        let report = verify_audit(&self.shard, GRAPH).unwrap();
        assert!(report.ok, "{report:?}");
        report.entries
    }

    fn rows<K: redb::Key + 'static>(
        &self,
        table: redb::TableDefinition<'static, K, &'static [u8]>,
    ) -> Vec<Vec<u8>>
    where
        for<'k> K::SelfType<'k>: eg_storage::OwnerRowScope + eg_storage::OwnerRowScopeStart<'k>,
    {
        let handle = self.shard.graph(GRAPH).unwrap();
        let read = self.shard.read(&handle).unwrap();
        let table = read.scoped_owner_table(table).unwrap();
        let rows = table.scope_rows().unwrap();
        rows.map(|row| row.unwrap().1.value().to_vec()).collect()
    }

    fn line(&self, seq: u64) -> String {
        let proof = operation_audit_read(&self.shard, GRAPH, TENANT, seq).unwrap();
        assert!(proof.chain_verified);
        proof.event_line
    }
}

#[test]
fn operation_audit_accepts_only_bounded_privacy_safe_fields() {
    assert!(event("req-1", "ok").validate().is_ok());
    let mut smuggled = event("req-1", "ok");
    smuggled.op = "graph.nodes.write|params=secret".into();
    assert!(smuggled.validate().is_err());
    let mut raw = event("req-1", "ok");
    raw.params_sha256 = "raw params".into();
    assert!(raw.validate().is_err());
    assert!(event(&"x".repeat(129), "ok").validate().is_err());
}

/// An append refused for its audit class leaves the chain and the request
/// index exactly as they were, for a reservation and for an outcome alike.
fn assert_class_refused(class: &str, code: &str) {
    let dir = tempfile::tempdir().unwrap();
    let mut store = Store::seeded(dir.path());
    let reserved = store.append(&event("request-a", "reserved")).unwrap();
    for status in ["reserved", "ok"] {
        let mut refused = event("request-b", status);
        refused.audit_class = class.into();
        assert_eq!(store.append(&refused).unwrap_err(), code, "{class:?}");
        let mut outcome = event("request-a", "ok");
        outcome.audit_class = class.into();
        assert_eq!(store.append(&outcome).unwrap_err(), code, "{class:?}");
    }
    assert_eq!(store.chain_entries(), 2, "seed and the one reservation");
    assert_eq!(store.rows(AUDIT_REQUESTS).len(), 1);
    // The refused request was never recorded: it is appended now as a first
    // attempt, at the very next sequence.
    let first = store.append(&event("request-b", "reserved")).unwrap();
    assert!(!first.replayed);
    assert_eq!(first.seq, reserved.seq + 1);
}

#[test]
fn an_append_without_an_audit_class_is_refused_and_writes_nothing() {
    for missing in ["", "none"] {
        assert_class_refused(missing, AUDIT_CLASS_REQUIRED);
    }
}

#[test]
fn an_append_with_an_undefined_audit_class_is_refused_and_writes_nothing() {
    for undefined in ["bogus", "EVENT", "identity", "event "] {
        assert_class_refused(undefined, AUDIT_CLASS_UNKNOWN);
    }
}

/// A request that leaves the class off the wire decodes, so the refusal is
/// the engine's declared one and not a decoder error.
#[test]
fn a_wire_request_without_an_audit_class_reaches_the_declared_refusal() {
    let method: Method = serde_json::from_value(serde_json::json!({
        "method": "AuditAppend",
        "params": {
            "op": "graph.nodes.write",
            "surface": "http",
            "params_sha256": "a".repeat(64),
            "status": "reserved",
            "request_id": "request-a",
        },
    }))
    .unwrap();
    let Method::AuditAppend { audit_class, .. } = method else {
        panic!("decoded another method");
    };
    let mut undeclared = event("request-a", "reserved");
    undeclared.audit_class = audit_class;
    assert_eq!(undeclared.validate().unwrap_err(), AUDIT_CLASS_REQUIRED);
}

/// The store stops after the reservation is durable and before any outcome is
/// recorded. After reopening, the reservation is still there and still pending;
/// presenting the same request identity finds it, the outcome links to it once,
/// and every further retry is a replay.
#[test]
fn a_reservation_interrupted_before_its_outcome_is_reconciled_after_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = Store::seeded(dir.path());
    let reserved = store.append(&event("request-a", "reserved")).unwrap();
    assert_eq!(
        (
            reserved.replayed,
            reserved.reservation_seq,
            reserved.outcome_seq
        ),
        (false, reserved.seq, None)
    );
    drop(store);

    let mut store = Store::reopened(dir.path());
    assert_eq!(store.chain_entries(), 2, "seed and the pending reservation");
    assert_eq!(store.rows(AUDIT_REQUESTS).len(), 1);
    let found = store.append(&event("request-a", "reserved")).unwrap();
    assert_eq!(
        found,
        AuditAppendReceipt {
            replayed: true,
            ..reserved.clone()
        },
        "the pending reservation is found, with no outcome linked"
    );

    let outcome = store.append(&event("request-a", "ok")).unwrap();
    assert_eq!(
        (
            outcome.replayed,
            outcome.reservation_seq,
            outcome.outcome_seq
        ),
        (false, reserved.seq, Some(reserved.seq + 1))
    );
    assert_eq!(outcome.seq, reserved.seq + 1);

    let resolved = store.append(&event("request-a", "reserved")).unwrap();
    assert_eq!(
        (resolved.replayed, resolved.seq, resolved.outcome_seq),
        (true, reserved.seq, Some(outcome.seq))
    );
    let again = store.append(&event("request-a", "ok")).unwrap();
    assert_eq!(
        again,
        AuditAppendReceipt {
            replayed: true,
            ..outcome.clone()
        }
    );
    let other = store.append(&event("request-a", "error")).unwrap_err();
    assert_eq!(other, AUDIT_IDEMPOTENCY_CONFLICT);

    assert_eq!(store.chain_entries(), 3, "no duplicate audit record");
    assert_eq!(store.rows(AUDIT_REQUESTS).len(), 2);
    assert!(store.line(reserved.seq).contains("|status=reserved|"));
    let closing = store.line(outcome.seq);
    assert!(closing.contains("|status=ok|"), "{closing}");
    assert!(
        closing.ends_with(&format!("|reservation={}", reserved.seq)),
        "{closing}"
    );
    drop(store);

    let mut store = Store::reopened(dir.path());
    assert!(store.append(&event("request-a", "ok")).unwrap().replayed);
    assert_eq!(store.chain_entries(), 3);
}

/// A pending reservation can only be closed by the request it was made for.
#[test]
fn a_pending_reservation_is_not_closed_by_another_request_context() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = Store::seeded(dir.path());
    store.append(&event("request-a", "reserved")).unwrap();
    drop(store);

    let mut store = Store::reopened(dir.path());
    let mut altered = event("request-a", "ok");
    altered.params_sha256 = "b".repeat(64);
    assert_eq!(
        store.append(&altered).unwrap_err(),
        AUDIT_RESERVATION_MISMATCH
    );
    let unreserved = store.append(&event("request-z", "ok")).unwrap_err();
    assert_eq!(unreserved, AUDIT_RESERVATION_REQUIRED);
    assert_eq!(store.chain_entries(), 2);
    let pending = store.append(&event("request-a", "reserved")).unwrap();
    assert_eq!((pending.replayed, pending.outcome_seq), (true, None));
}

/// Two stores that apply the same events, as two replicas of one graph do, end
/// with byte-identical audit-chain and request-index rows and return the same
/// receipts. The per-attempt admission id differs between them on every call
/// (it is built from the local clock and a process counter), so this holds only
/// because that id is never written into either table.
#[test]
fn stores_applying_the_same_events_hold_identical_audit_rows() {
    let events = [
        event("request-a", "reserved"),
        event("request-b", "reserved"),
        event("request-a", "reserved"),
        event("request-a", "ok"),
        event("request-b", "denied"),
        event("request-a", "ok"),
    ];
    let replica = || {
        let dir = tempfile::tempdir().unwrap();
        let mut store = Store::seeded(dir.path());
        let receipts: Vec<_> = events
            .iter()
            .map(|event| store.append(event).unwrap())
            .collect();
        (receipts, store.rows(AUDIT), store.rows(AUDIT_REQUESTS))
    };
    let (first, second) = (replica(), replica());
    assert_eq!(first, second);
    assert_eq!((first.1.len(), first.2.len()), (5, 4));
}
