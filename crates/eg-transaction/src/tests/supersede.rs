//! Lease-independent outbox supersession during replicated source top-up.

use super::outbox::{bind_scope, budget};
use super::*;
use crate::admitted::AdmittedMutation;
use crate::outbox::{outbox_cursor, outbox_status, OutboxDelivery, OutboxRejectReason};
use eg_types::MutationOutboxIntent;
use std::collections::BTreeMap;

const CONSUMER: &str = "repository-enrichment-v1";
const TOPIC: &str = "repository.enrichment.pending";

fn emit(fixture: &Fixture, owner: &OwnedStoreHandle<LedgerOnlyOwner>, count: u64) {
    for version in 0..count {
        let mut value = batch(owner.identity().clone(), &format!("batch-{version}"));
        value.version_expectation = VersionExpectation::Native(version);
        value.created_at_ms = 1_000 + version;
        value.outbox = vec![MutationOutboxIntent {
            topic: TOPIC.into(),
            key: format!("batch-{version}"),
            payload: b"snapshot".to_vec(),
            headers: BTreeMap::new(),
        }];
        value
            .reseal_envelope(eg_types::contract::Digest256::from_bytes([1_u8; 32]))
            .unwrap();
        apply_batch(fixture, owner, &value);
    }
}

fn source_record(
    fixture: &Fixture,
    owner: &OwnedStoreHandle<LedgerOnlyOwner>,
    batch_id: &str,
) -> eg_types::MutationOutboxRecord {
    let read = fixture.kernel.read_scope(owner).unwrap();
    read_outbox(&read, batch_id).unwrap().remove(0)
}

fn admit_probe<'a>(
    fixture: &'a Fixture,
    owner: &OwnedStoreHandle<LedgerOnlyOwner>,
    batch_id: &str,
) -> (
    AdmittedMutation<'a, LedgerOnlyOwner>,
    MutationBatch,
    Option<u64>,
) {
    let (write, batch, begun) = fixture
        .mutations
        .admit_current(owner, |version| {
            let mut batch = batch(owner.identity().clone(), batch_id);
            batch.version_expectation = VersionExpectation::Native(version);
            batch.reseal_envelope(eg_types::contract::Digest256::from_bytes([1_u8; 32]))?;
            Ok(batch)
        })
        .unwrap();
    let Begin::Apply { source_version } = begun else {
        panic!("probe batch unexpectedly replayed")
    };
    (write, batch, source_version)
}

#[test]
fn supersede_installs_follower_subscription_and_replays_exact_receipt() {
    let dir = tempfile::tempdir().unwrap();
    let fixture = Fixture::create::<LedgerOnlyOwner>(
        &dir.path().join("native.redb"),
        "physical:test:supersede",
        None,
    );
    let owner = bind_scope(
        &fixture,
        "tenant-a",
        native_identity("tenant-a", "incarnation:supersede:follower"),
    );
    emit(&fixture, &owner, 1);
    let expected = source_record(&fixture, &owner, "batch-0");
    let (write, probe, source_version) = admit_probe(&fixture, &owner, "supersede-probe");
    let receipt = fixture
        .mutations
        .outbox_supersede_in(&write, &owner, CONSUMER, &expected, 20)
        .unwrap();
    fixture
        .mutations
        .finish(&write, &probe, None, 20, source_version)
        .unwrap();
    fixture.mutations.commit(write, &probe).unwrap();
    let read = fixture.kernel.read_scope(&owner).unwrap();
    assert_eq!(
        outbox_cursor(&read, CONSUMER).unwrap(),
        Some(receipt.clone())
    );
    assert_eq!(outbox_status(&read, CONSUMER, 20).unwrap().dead_lettered, 0);
    drop(read);

    let (write, _, _) = admit_probe(&fixture, &owner, "supersede-replay-probe");
    let replayed = fixture
        .mutations
        .outbox_supersede_in(&write, &owner, CONSUMER, &expected, 20)
        .unwrap();
    assert_eq!(replayed, receipt);
    assert_eq!(
        fixture
            .mutations
            .outbox_supersession_receipt_in(&write, &owner, CONSUMER, &expected, 20)
            .unwrap(),
        receipt
    );
    write.abort().unwrap();
    let (write, _, _) = admit_probe(&fixture, &owner, "supersede-wrong-time-probe");
    assert!(fixture
        .mutations
        .outbox_supersede_in(&write, &owner, CONSUMER, &expected, 21)
        .unwrap_err()
        .starts_with("OUTBOX_SUPERSEDE_MISMATCH:"));
    write.abort().unwrap();
}

#[test]
fn replacement_claim_and_prune_retain_old_supersession_receipt() {
    let dir = tempfile::tempdir().unwrap();
    let fixture = Fixture::create::<LedgerOnlyOwner>(
        &dir.path().join("native.redb"),
        "physical:test:supersede-replacement",
        None,
    );
    let owner = bind_scope(
        &fixture,
        "tenant-a",
        native_identity("tenant-a", "incarnation:supersede:replacement"),
    );
    emit(&fixture, &owner, 1);
    let old = source_record(&fixture, &owner, "batch-0");
    let (write, probe, source_version) = admit_probe(&fixture, &owner, "top-up-probe");
    let receipt = fixture
        .mutations
        .outbox_supersede_in(&write, &owner, CONSUMER, &old, 20)
        .unwrap();
    fixture
        .mutations
        .finish(&write, &probe, None, 20, source_version)
        .unwrap();
    fixture.mutations.commit(write, &probe).unwrap();

    // The replacement is later delivered by the same consumer. Its cursor
    // moves past the old source, allowing the bounded prune pass to visit the
    // source marker while the original Raft transition can still be retried.
    let mut replacement = batch(owner.identity().clone(), "batch-1");
    replacement.version_expectation = VersionExpectation::Native(2);
    replacement.created_at_ms = 1_002;
    replacement.outbox = vec![MutationOutboxIntent {
        topic: TOPIC.into(),
        key: "batch-1".into(),
        payload: b"funded-snapshot".to_vec(),
        headers: BTreeMap::new(),
    }];
    replacement
        .reseal_envelope(eg_types::contract::Digest256::from_bytes([1_u8; 32]))
        .unwrap();
    apply_batch(&fixture, &owner, &replacement);
    let mut sweep = budget(8, 30);
    let claimed = fixture
        .mutations
        .outbox_claim(&owner, CONSUMER, &mut sweep)
        .unwrap()
        .claims;
    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0].record.batch_id, "batch-1");
    fixture
        .mutations
        .outbox_ack(&owner, &claimed[0], 31)
        .unwrap();
    let mut prune = budget(8, 32);
    assert!(fixture
        .mutations
        .outbox_claim(&owner, CONSUMER, &mut prune)
        .unwrap()
        .claims
        .is_empty());

    let read = fixture.kernel.read_scope(&owner).unwrap();
    let scope = eg_storage::ledger_scope_key(owner.identity());
    let marker: OutboxDelivery = crate::outbox::decode_row(
        read.scoped_table(crate::tables::OUTBOX_DELIVERIES)
            .unwrap()
            .get((scope.as_str(), CONSUMER, old.batch_id.as_str(), old.ordinal))
            .unwrap()
            .expect("supersession marker must survive prune")
            .value(),
    )
    .unwrap();
    assert_eq!(marker.lease_epoch, 0);
    assert_eq!(marker.delivered_at_ms, Some(20));
    drop(read);

    let (write, _, _) = admit_probe(&fixture, &owner, "top-up-replay-probe");
    assert_eq!(
        fixture
            .mutations
            .outbox_supersession_receipt_in(&write, &owner, CONSUMER, &old, 20)
            .unwrap(),
        receipt
    );
    write.abort().unwrap();
}

#[test]
fn supersede_fences_old_lease_and_refuses_wrong_source() {
    let dir = tempfile::tempdir().unwrap();
    let fixture = Fixture::create::<LedgerOnlyOwner>(
        &dir.path().join("native.redb"),
        "physical:test:supersede-lease",
        None,
    );
    let owner = bind_scope(
        &fixture,
        "tenant-a",
        native_identity("tenant-a", "incarnation:supersede:lease"),
    );
    emit(&fixture, &owner, 1);
    fixture
        .mutations
        .outbox_subscribe(&owner, CONSUMER, TOPIC)
        .unwrap();
    let mut sweep = budget(8, 10);
    let lease = fixture
        .mutations
        .outbox_claim(&owner, CONSUMER, &mut sweep)
        .unwrap()
        .claims
        .remove(0);
    let mut wrong = lease.record.clone();
    wrong.intent.payload.push(99);
    let (write, _, _) = admit_probe(&fixture, &owner, "supersede-wrong-source-probe");
    assert!(fixture
        .mutations
        .outbox_supersede_in(&write, &owner, CONSUMER, &wrong, 20)
        .unwrap_err()
        .starts_with("OUTBOX_SUPERSEDE_MISMATCH:"));
    write.abort().unwrap();

    let (write, probe, source_version) = admit_probe(&fixture, &owner, "supersede-lease-probe");
    fixture
        .mutations
        .outbox_supersede_in(&write, &owner, CONSUMER, &lease.record, 20)
        .unwrap();
    fixture
        .mutations
        .finish(&write, &probe, None, 20, source_version)
        .unwrap();
    fixture.mutations.commit(write, &probe).unwrap();
    assert!(fixture.mutations.outbox_ack(&owner, &lease, 21).is_err());
    let read = fixture.kernel.read_scope(&owner).unwrap();
    assert_eq!(outbox_status(&read, CONSUMER, 21).unwrap().inflight, 0);
}

#[test]
fn supersede_refuses_order_gap_and_dead_letter() {
    let dir = tempfile::tempdir().unwrap();
    let fixture = Fixture::create::<LedgerOnlyOwner>(
        &dir.path().join("native.redb"),
        "physical:test:supersede-gap",
        None,
    );
    let owner = bind_scope(
        &fixture,
        "tenant-a",
        native_identity("tenant-a", "incarnation:supersede:gap"),
    );
    emit(&fixture, &owner, 2);
    let second = source_record(&fixture, &owner, "batch-1");
    let (write, _, _) = admit_probe(&fixture, &owner, "supersede-gap-probe");
    assert!(fixture
        .mutations
        .outbox_supersede_in(&write, &owner, CONSUMER, &second, 20)
        .unwrap_err()
        .starts_with("OUTBOX_ORDER_GAP:"));
    write.abort().unwrap();
    fixture
        .mutations
        .outbox_subscribe(&owner, CONSUMER, TOPIC)
        .unwrap();
    let mut sweep = budget(8, 10);
    let first = fixture
        .mutations
        .outbox_claim(&owner, CONSUMER, &mut sweep)
        .unwrap()
        .claims
        .remove(0);
    fixture
        .mutations
        .outbox_reject(&owner, &first, OutboxRejectReason::DomainRefused, 20)
        .unwrap();
    let (write, _, _) = admit_probe(&fixture, &owner, "supersede-dead-probe");
    assert!(fixture
        .mutations
        .outbox_supersede_in(&write, &owner, CONSUMER, &first.record, 21)
        .unwrap_err()
        .starts_with("OUTBOX_DEAD_LETTER:"));
    write.abort().unwrap();
}
