//! The outbox claim/deliver/acknowledge protocol (RF-RULING-007).
//!
//! Every case here is about a property the retired graph-shard implementation
//! had and the kernel had not: at-least-once delivery across a restart, two
//! workers of one consumer never holding the same row, a bounded queue that
//! defers rather than drops, a fairness cap, and a watermark that moves in the
//! same transaction as the delivery it records.

use super::*;
use crate::outbox::{
    outbox_cursor, outbox_status, OutboxClaimBudget, OutboxClaimCursor, OutboxConsumerState,
    OutboxDeferral, OutboxDelivery, OutboxPosition,
};
use eg_types::mutation_batch::{MutationCapability, RESERVED_SYSTEM_TENANT};
use eg_types::{
    MutationOutboxIntent, MutationOutboxLease, MutationProjectionCursor, MUTATION_BATCH_VERSION,
};
use std::collections::BTreeMap;

const TOPIC: &str = "engine.projection.rebuild";

fn bind_scope(
    fixture: &Fixture,
    tenant: &'static str,
    identity: MutationScopeIdentity,
) -> OwnedStoreHandle<LedgerOnlyOwner> {
    fixture.bind::<LedgerOnlyOwner>(&verifier(tenant, OwnerLayout::LedgerOnly), identity)
}

/// One batch that emits `events` outbox intents on `TOPIC` at `version`.
fn event_batch(
    identity: &MutationScopeIdentity,
    batch_id: &str,
    version: u64,
    events: u32,
) -> MutationBatch {
    let mut value = batch(identity.clone(), batch_id);
    value.version_expectation = VersionExpectation::Native(version);
    value.created_at_ms = 1_000 + version;
    value.outbox = (0..events)
        .map(|ordinal| MutationOutboxIntent {
            topic: TOPIC.to_string(),
            key: format!("{batch_id}:{ordinal}"),
            payload: vec![ordinal as u8],
            headers: BTreeMap::new(),
        })
        .collect();
    value
        .reseal_envelope(eg_types::contract::Digest256::from_bytes([1_u8; 32]))
        .expect("an outbox fixture batch reseals its final outbox");
    value.validate().unwrap();
    value
}

/// Commit `batches` batches of one event each, starting at version 0.
fn emit(fixture: &Fixture, owner: &OwnedStoreHandle<LedgerOnlyOwner>, batches: u64) {
    for version in 0..batches {
        let batch = event_batch(owner.identity(), &format!("batch-{version}"), version, 1);
        apply_batch(fixture, owner, &batch);
    }
}

fn budget(limit: u32, now_ms: u64) -> OutboxClaimBudget {
    OutboxClaimBudget::new(limit, 5_000, now_ms).unwrap()
}

/// Claim until a round makes no progress, retaining the caller-owned budget
/// across calls while the scope has work.
fn drain_claims(
    fixture: &Fixture,
    owner: &OwnedStoreHandle<LedgerOnlyOwner>,
    consumer: &str,
    budget: &mut OutboxClaimBudget,
) -> Vec<MutationOutboxLease> {
    let mut claimed = Vec::new();
    loop {
        let round = fixture
            .mutations
            .outbox_claim(owner, consumer, budget)
            .unwrap();
        if round.claims.is_empty() {
            return claimed;
        }
        claimed.extend(round.claims);
    }
}

/// One claim, for a test that wants the outcome rather than the leases.
fn claim_once(
    fixture: &Fixture,
    owner: &OwnedStoreHandle<LedgerOnlyOwner>,
    consumer: &str,
    budget: &mut OutboxClaimBudget,
) -> crate::outbox::OutboxClaimOutcome {
    fixture
        .mutations
        .outbox_claim(owner, consumer, budget)
        .unwrap()
}

#[test]
fn a_claim_needs_a_durable_subscription_and_returns_rows_in_commit_order() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("native.redb");
    let identity = native_identity("tenant-a", "incarnation:outbox:1");
    let (fixture, owner) = ledger_fixture(&path, identity);
    emit(&fixture, &owner, 3);

    let mut unsubscribed = budget(8, 10);
    assert!(fixture
        .mutations
        .outbox_claim(&owner, "projection", &mut unsubscribed)
        .is_err());

    fixture
        .mutations
        .outbox_subscribe(&owner, "projection", TOPIC)
        .unwrap();
    let mut sweep = budget(8, 10);
    let claimed = drain_claims(&fixture, &owner, "projection", &mut sweep);
    let ids: Vec<&str> = claimed
        .iter()
        .map(|lease| lease.record.batch_id.as_str())
        .collect();
    assert_eq!(ids, vec!["batch-0", "batch-1", "batch-2"]);
    assert!(claimed.iter().all(|lease| lease.lease_epoch == 1));
}

#[test]
fn two_workers_of_one_consumer_never_hold_the_same_row() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("native.redb");
    let identity = native_identity("tenant-a", "incarnation:outbox:2");
    let (fixture, owner) = ledger_fixture(&path, identity);
    emit(&fixture, &owner, 2);
    fixture
        .mutations
        .outbox_subscribe(&owner, "projection", TOPIC)
        .unwrap();

    let mut first = budget(8, 10);
    let held = drain_claims(&fixture, &owner, "projection", &mut first);
    assert_eq!(held.len(), 2);

    // A second worker for the SAME consumer, while the leases are live.
    let mut second = budget(8, 20);
    let outcome = claim_once(&fixture, &owner, "projection", &mut second);
    assert!(outcome.claims.is_empty());
    assert_eq!(outcome.deferred, None, "an idle round is not backpressure");
}

#[test]
fn two_consumers_of_one_topic_each_see_every_row() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("native.redb");
    let identity = native_identity("tenant-a", "incarnation:outbox:3");
    let (fixture, owner) = ledger_fixture(&path, identity);
    emit(&fixture, &owner, 2);
    for consumer in ["projection-a", "projection-b"] {
        fixture
            .mutations
            .outbox_subscribe(&owner, consumer, TOPIC)
            .unwrap();
    }

    for consumer in ["projection-a", "projection-b"] {
        let mut sweep = budget(8, 10);
        let claimed = drain_claims(&fixture, &owner, consumer, &mut sweep);
        assert_eq!(claimed.len(), 2, "{consumer} did not see every row");
    }
}

#[test]
fn a_restart_between_claim_and_ack_redelivers_the_row() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("native.redb");
    let identity = native_identity("tenant-a", "incarnation:outbox:4");
    {
        let (fixture, owner) = ledger_fixture(&path, identity.clone());
        emit(&fixture, &owner, 1);
        fixture
            .mutations
            .outbox_subscribe(&owner, "projection", TOPIC)
            .unwrap();
        let mut sweep = budget(4, 10);
        let claimed = drain_claims(&fixture, &owner, "projection", &mut sweep);
        assert_eq!(claimed.len(), 1);
        // The worker dies here: the claim is durable, the ack never happens.
    }

    let fixture = Fixture::open::<LedgerOnlyOwner>(&path, "physical:test:ledger-only", None);
    let owner = bind_scope(&fixture, "tenant-a", identity);
    let read = fixture.kernel.read_scope(&owner).unwrap();
    assert!(outbox_cursor(&read, "projection").unwrap().is_none());
    drop(read);

    // The lease has expired by now; the row is claimable again, at a strictly
    // greater epoch and a second attempt.
    let mut sweep = budget(4, 60_000);
    let claimed = drain_claims(&fixture, &owner, "projection", &mut sweep);
    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0].lease_epoch, 2);
    assert_eq!(claimed[0].attempt, 2);
}

#[test]
fn an_acknowledgement_advances_the_cursor_and_survives_a_restart() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("native.redb");
    let identity = native_identity("tenant-a", "incarnation:outbox:5");
    let expected = {
        let (fixture, owner) = ledger_fixture(&path, identity.clone());
        emit(&fixture, &owner, 1);
        fixture
            .mutations
            .outbox_subscribe(&owner, "projection", TOPIC)
            .unwrap();
        let mut sweep = budget(4, 10);
        let claimed = drain_claims(&fixture, &owner, "projection", &mut sweep);
        let cursor = fixture
            .mutations
            .outbox_ack(&owner, &claimed[0], 20)
            .unwrap();
        assert_eq!(cursor.projection, "projection");
        assert_eq!(cursor.batch_id, "batch-0");
        cursor
    };

    let fixture = Fixture::open::<LedgerOnlyOwner>(&path, "physical:test:ledger-only", None);
    let owner = bind_scope(&fixture, "tenant-a", identity);
    let read = fixture.kernel.read_scope(&owner).unwrap();
    assert_eq!(outbox_cursor(&read, "projection").unwrap(), Some(expected));
    drop(read);

    // Delivered rows are not re-claimed.
    let mut sweep = budget(4, 60_000);
    assert!(drain_claims(&fixture, &owner, "projection", &mut sweep).is_empty());
}

#[test]
fn a_failed_acknowledgement_leaves_the_delivery_and_the_cursor_unchanged() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("native.redb");
    let identity = native_identity("tenant-a", "incarnation:outbox:6");
    let (fixture, owner) = ledger_fixture(&path, identity);
    emit(&fixture, &owner, 1);
    fixture
        .mutations
        .outbox_subscribe(&owner, "projection", TOPIC)
        .unwrap();
    let mut sweep = budget(4, 10);
    let claimed = drain_claims(&fixture, &owner, "projection", &mut sweep);

    let mut superseded = claimed[0].clone();
    superseded.lease_epoch = 99;
    assert!(fixture
        .mutations
        .outbox_ack(&owner, &superseded, 20)
        .is_err());

    let read = fixture.kernel.read_scope(&owner).unwrap();
    assert!(outbox_cursor(&read, "projection").unwrap().is_none());
    let status = outbox_status(&read, "projection", 20).unwrap();
    assert_eq!(status.delivered, 0);
    assert_eq!(status.inflight, 1);
    drop(read);

    // The real lease still acknowledges, so the refusal fenced the impostor
    // rather than the row.
    fixture
        .mutations
        .outbox_ack(&owner, &claimed[0], 30)
        .unwrap();
}

#[test]
fn projection_cursor_must_match_the_claim_watermark() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("native.redb");
    let identity = native_identity("tenant-a", "incarnation:cursor:projection-link");
    let (fixture, owner) = ledger_fixture(&path, identity.clone());
    emit(&fixture, &owner, 2);
    fixture
        .mutations
        .outbox_subscribe(&owner, "projection", TOPIC)
        .unwrap();
    let mut sweep = budget(8, 10);
    let claimed = drain_claims(&fixture, &owner, "projection", &mut sweep);
    assert_eq!(claimed.len(), 2);
    fixture
        .mutations
        .outbox_ack(&owner, &claimed[0], 20)
        .unwrap();

    let forged = MutationProjectionCursor {
        schema_version: MUTATION_BATCH_VERSION,
        projection: "projection".to_string(),
        identity,
        batch_id: claimed[1].record.batch_id.clone(),
        outbox_ordinal: claimed[1].record.ordinal,
        committed_version: claimed[1].record.committed_version,
        advanced_at_ms: 21,
    };
    let write =
        crate::admitted::AdmittedMutation::open(fixture.mutations_authority(), &owner).unwrap();
    let scope = eg_storage::ledger_scope_key(owner.identity());
    let bytes = crate::outbox::encode_row(&forged, "planted projection cursor").unwrap();
    write
        .scoped_table(crate::tables::OUTBOX_CURSORS)
        .unwrap()
        .insert((scope.as_str(), "projection"), bytes.as_slice())
        .unwrap();
    write.commit().unwrap();

    let read = fixture.kernel.read_scope(&owner).unwrap();
    let error = outbox_cursor(&read, "projection").unwrap_err();
    assert!(error.contains("CORRUPT_OUTBOX_CURSOR"), "{error}");
    let error = outbox_status(&read, "projection", 20).unwrap_err();
    assert!(error.contains("CORRUPT_OUTBOX_CURSOR"), "{error}");
}

#[test]
fn an_out_of_order_acknowledgement_is_refused_as_an_order_gap() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("native.redb");
    let identity = native_identity("tenant-a", "incarnation:outbox:7");
    let (fixture, owner) = ledger_fixture(&path, identity);
    emit(&fixture, &owner, 2);
    fixture
        .mutations
        .outbox_subscribe(&owner, "projection", TOPIC)
        .unwrap();
    let mut sweep = budget(8, 10);
    let claimed = drain_claims(&fixture, &owner, "projection", &mut sweep);
    assert_eq!(claimed.len(), 2);

    let error = fixture
        .mutations
        .outbox_ack(&owner, &claimed[1], 20)
        .unwrap_err();
    assert!(error.starts_with("OUTBOX_ORDER_GAP"), "{error}");
    fixture
        .mutations
        .outbox_ack(&owner, &claimed[0], 20)
        .unwrap();
    fixture
        .mutations
        .outbox_ack(&owner, &claimed[1], 21)
        .unwrap();
}

#[test]
fn a_released_lease_is_reclaimable_and_can_never_be_acknowledged() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("native.redb");
    let identity = native_identity("tenant-a", "incarnation:outbox:8");
    let (fixture, owner) = ledger_fixture(&path, identity);
    emit(&fixture, &owner, 1);
    fixture
        .mutations
        .outbox_subscribe(&owner, "projection", TOPIC)
        .unwrap();
    let mut sweep = budget(4, 10);
    let claimed = drain_claims(&fixture, &owner, "projection", &mut sweep);
    fixture
        .mutations
        .outbox_release(&owner, &claimed[0])
        .unwrap();
    assert!(fixture
        .mutations
        .outbox_ack(&owner, &claimed[0], 20)
        .is_err());

    let mut again = budget(4, 11);
    let reclaimed = drain_claims(&fixture, &owner, "projection", &mut again);
    assert_eq!(reclaimed.len(), 1);
    assert_eq!(reclaimed[0].lease_epoch, 2);
}

#[test]
fn expiring_a_lease_reports_it_and_makes_the_row_claimable() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("native.redb");
    let identity = native_identity("tenant-a", "incarnation:outbox:9");
    let (fixture, owner) = ledger_fixture(&path, identity);
    emit(&fixture, &owner, 1);
    fixture
        .mutations
        .outbox_subscribe(&owner, "projection", TOPIC)
        .unwrap();
    let mut sweep = budget(4, 10);
    drain_claims(&fixture, &owner, "projection", &mut sweep);

    assert_eq!(
        fixture
            .mutations
            .outbox_expire(&owner, "projection", 20)
            .unwrap(),
        0
    );
    assert_eq!(
        fixture
            .mutations
            .outbox_expire(&owner, "projection", 60_000)
            .unwrap(),
        1
    );
    let read = fixture.kernel.read_scope(&owner).unwrap();
    assert_eq!(
        outbox_status(&read, "projection", 60_000).unwrap().inflight,
        0
    );
}

#[test]
fn a_contended_budget_caps_a_tenant_run_after_another_tenant_claims() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("native.redb");
    let alpha = native_identity("tenant-a", "incarnation:fair:a");
    let beta = native_identity("tenant-b", "incarnation:fair:b");
    let fixture = Fixture::create::<LedgerOnlyOwner>(&path, "physical:test:ledger-only", None);
    let owner_a = bind_scope(&fixture, "tenant-a", alpha);
    let owner_b = bind_scope(&fixture, "tenant-b", beta);
    emit(&fixture, &owner_a, 8);
    emit(&fixture, &owner_b, 8);
    for owner in [&owner_a, &owner_b] {
        fixture
            .mutations
            .outbox_subscribe(owner, "projection", TOPIC)
            .unwrap();
    }

    // Budget 16 -> a 4-row per-call cap. The shared budget only becomes a
    // contended-run cap after the scheduler visits another tenant.
    let mut sweep = budget(16, 10);
    assert_eq!(sweep.consecutive_cap(), 4);
    assert!(!sweep.contended());
    assert_eq!(
        claim_once(&fixture, &owner_a, "projection", &mut sweep)
            .claims
            .len(),
        4
    );
    // A alone is contending with nobody, so its run does not accumulate and it
    // is not starved of its own queue.
    assert_eq!(
        claim_once(&fixture, &owner_a, "projection", &mut sweep)
            .claims
            .len(),
        4
    );
    // B makes the sweep contended and takes its own turn.
    assert_eq!(
        claim_once(&fixture, &owner_b, "projection", &mut sweep)
            .claims
            .len(),
        4
    );
    assert!(sweep.contended());
    // B immediately again: its consecutive run is spent, and the deferral says
    // so rather than looking like an idle queue.
    let capped = claim_once(&fixture, &owner_b, "projection", &mut sweep);
    assert!(capped.claims.is_empty());
    assert_eq!(
        capped.deferred,
        Some(OutboxDeferral::FairnessCapped {
            consecutive: 4,
            cap: 4
        })
    );
}

#[test]
fn a_lone_tenant_can_continue_after_restart_without_fake_contention() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("native.redb");
    let alpha = native_identity("tenant-a", "incarnation:fair:restart");
    let identity = alpha.clone();
    {
        let (fixture, owner) = ledger_fixture(&path, alpha);
        emit(&fixture, &owner, 8);
        fixture
            .mutations
            .outbox_subscribe(&owner, "projection", TOPIC)
            .unwrap();
        let mut sweep = budget(16, 10);
        assert_eq!(
            claim_once(&fixture, &owner, "projection", &mut sweep)
                .claims
                .len(),
            4
        );
        let read = fixture.kernel.read_scope(&owner).unwrap();
        assert_eq!(
            outbox_status(&read, "projection", 10)
                .unwrap()
                .consecutive_claims,
            4
        );
    }
    // A brand-new process starts a new sweep. The durable row remains useful
    // accounting, but it cannot prove that another tenant was contending while
    // this process was down, so it must not seed a cross-tenant fairness gate.
    let fixture = Fixture::open::<LedgerOnlyOwner>(&path, "physical:test:ledger-only", None);
    let owner = bind_scope(&fixture, "tenant-a", identity);
    let read = fixture.kernel.read_scope(&owner).unwrap();
    assert_eq!(
        outbox_status(&read, "projection", 10)
            .unwrap()
            .consecutive_claims,
        4
    );
    drop(read);

    let mut resumed = budget(16, 10);
    let resumed_claim = claim_once(&fixture, &owner, "projection", &mut resumed);
    assert_eq!(resumed_claim.claims.len(), 4);
}

#[test]
fn commit_order_decides_delivery_order_even_when_batch_ids_sort_backwards() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("native.redb");
    let identity = native_identity("tenant-a", "incarnation:order:1");
    let (fixture, owner) = ledger_fixture(&path, identity);
    // Committed first, sorts last; committed second, sorts first.
    for (version, id) in [(0u64, "zzz"), (1, "aaa")] {
        let batch = event_batch(owner.identity(), id, version, 1);
        apply_batch(&fixture, &owner, &batch);
    }
    fixture
        .mutations
        .outbox_subscribe(&owner, "projection", TOPIC)
        .unwrap();

    let mut sweep = budget(8, 10);
    let claimed = drain_claims(&fixture, &owner, "projection", &mut sweep);
    let ids: Vec<&str> = claimed
        .iter()
        .map(|lease| lease.record.batch_id.as_str())
        .collect();
    assert_eq!(
        ids,
        vec!["zzz", "aaa"],
        "the index orders by commit, not by id"
    );
    fixture
        .mutations
        .outbox_ack(&owner, &claimed[0], 20)
        .unwrap();
    fixture
        .mutations
        .outbox_ack(&owner, &claimed[1], 21)
        .unwrap();
}

#[test]
fn a_poison_row_is_dead_lettered_and_the_stream_continues() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("native.redb");
    let identity = native_identity("tenant-a", "incarnation:poison:1");
    let (fixture, owner) = ledger_fixture(&path, identity.clone());
    emit(&fixture, &owner, 2);
    fixture
        .mutations
        .outbox_subscribe(&owner, "projection", TOPIC)
        .unwrap();

    // Claim and abandon the head row until it exhausts its retry class. Each
    // round leases it afresh because the previous lease has expired.
    let attempts = crate::outbox::max_delivery_attempts();
    for round in 0..attempts {
        let mut sweep = budget(1, u64::from(round) * 1_000 + 10);
        let claimed = claim_once(&fixture, &owner, "projection", &mut sweep);
        assert_eq!(claimed.claims.len(), 1);
        assert_eq!(claimed.claims[0].record.batch_id, "batch-0");
        fixture
            .mutations
            .outbox_expire(&owner, "projection", u64::from(round) * 1_000 + 999_999)
            .unwrap();
    }

    // The next claim dead-letters it and hands out the row behind it, which was
    // blocked by the ordering rule until now.
    let mut sweep = budget(4, u64::from(attempts) * 1_000_000);
    let after = claim_once(&fixture, &owner, "projection", &mut sweep);
    assert_eq!(after.claims.len(), 1);
    assert_eq!(after.claims[0].record.batch_id, "batch-1");

    let read = fixture.kernel.read_scope(&owner).unwrap();
    let status = outbox_status(&read, "projection", 0).unwrap();
    assert_eq!(status.dead_lettered, 1);
    drop(read);

    // And the successor acknowledges: a dead-lettered predecessor is resolved,
    // so it no longer wedges the watermark.
    fixture
        .mutations
        .outbox_ack(
            &owner,
            &after.claims[0],
            u64::from(attempts) * 1_000_000 + 1,
        )
        .unwrap();

    // A later empty claim advances the prune cursor past the retained poison
    // row. The dead-letter evidence must survive that sweep.
    let mut sweep = budget(4, u64::from(attempts) * 1_000_000 + 2);
    assert!(claim_once(&fixture, &owner, "projection", &mut sweep)
        .claims
        .is_empty());
    drop(owner);
    drop(fixture);

    // Reopening must preserve the exact retry evidence and must never make the
    // dead-lettered event claimable again.
    let reopened = Fixture::open::<LedgerOnlyOwner>(&path, "physical:test:ledger-only", None);
    let reopened_owner = bind_scope(&reopened, "tenant-a", identity);
    let mut sweep = budget(4, u64::from(attempts) * 1_000_000 + 3);
    assert!(
        claim_once(&reopened, &reopened_owner, "projection", &mut sweep)
            .claims
            .is_empty()
    );
    let read = reopened.kernel.read_scope(&reopened_owner).unwrap();
    let scope = eg_storage::ledger_scope_key(reopened_owner.identity());
    let dead_letter = read
        .scoped_table(crate::tables::OUTBOX_DELIVERIES)
        .unwrap()
        .get((scope.as_str(), "projection", "batch-0", 0))
        .unwrap()
        .map(|value| crate::outbox::decode_row::<OutboxDelivery>(value.value()))
        .transpose()
        .unwrap()
        .expect("dead-letter evidence was pruned");
    assert_eq!(dead_letter.position.batch_id, "batch-0");
    assert_eq!(dead_letter.attempt, attempts);
    assert!(dead_letter.dead_lettered_at_ms.is_some());
}

#[test]
fn backfilling_the_index_makes_pre_protocol_rows_deliverable() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("native.redb");
    let identity = native_identity("tenant-a", "incarnation:backfill:1");
    let (fixture, owner) = ledger_fixture(&path, identity);
    emit(&fixture, &owner, 2);
    fixture
        .mutations
        .outbox_subscribe(&owner, "projection", TOPIC)
        .unwrap();

    // Every row is already indexed at commit, so the repair is a no-op and says
    // so -- which is what makes it safe to run on a cutover without knowing.
    let repaired = fixture.mutations.outbox_backfill_index(&owner).unwrap();
    assert_eq!(repaired.indexed, 0);
    assert!(repaired.complete);
    let mut sweep = budget(8, 10);
    assert_eq!(
        drain_claims(&fixture, &owner, "projection", &mut sweep).len(),
        2
    );
}

#[test]
fn an_unversioned_control_plane_outbox_row_reopens_with_its_commit_sequence() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("control-plane.redb");
    let identity = MutationScopeIdentity::native(
        ScopeTenantId::system(),
        DurabilityDomain::ControlPlane,
        LogicalName::new("cluster-bootstrap").unwrap(),
        IncarnationId::new("incarnation:backfill:control-plane").unwrap(),
    )
    .unwrap();
    {
        let fixture = Fixture::create::<LedgerOnlyOwner>(
            &path,
            "physical:test:ledger-only-control-plane",
            None,
        );
        let owner = bind_scope(&fixture, RESERVED_SYSTEM_TENANT, identity.clone());
        let mut batch = batch(identity.clone(), "control-plane-event");
        batch.operations[0].domain = DurabilityDomain::ControlPlane;
        batch.version_expectation = VersionExpectation::Unversioned;
        let eg_types::mutation_batch::MutationEnvelope::Operation(envelope) = &mut batch.envelope
        else {
            panic!("a control-plane fixture requires an operation envelope");
        };
        envelope
            .verified_capabilities
            .insert(MutationCapability::UnversionedSystemMutation);
        batch.outbox = vec![MutationOutboxIntent {
            topic: TOPIC.to_string(),
            key: "control-plane-event:0".to_string(),
            payload: vec![0],
            headers: BTreeMap::new(),
        }];
        batch
            .reseal_envelope(eg_types::contract::Digest256::from_bytes([1_u8; 32]))
            .expect("a control-plane fixture batch reseals its final outbox");
        batch.validate().unwrap();
        apply_batch(&fixture, &owner, &batch);
        fixture
            .mutations
            .outbox_subscribe(&owner, "projection", TOPIC)
            .unwrap();
    }

    let reopened =
        Fixture::open::<LedgerOnlyOwner>(&path, "physical:test:ledger-only-control-plane", None);
    let owner = bind_scope(&reopened, RESERVED_SYSTEM_TENANT, identity);
    let read = reopened.kernel.read_scope(&owner).unwrap();
    assert_eq!(version(&read).unwrap(), 1);
    drop(read);
    let mut sweep = budget(4, 10);
    assert_eq!(
        claim_once(&reopened, &owner, "projection", &mut sweep)
            .claims
            .len(),
        1
    );
}

#[test]
fn backfill_repairs_a_row_whose_index_entry_is_missing() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("native.redb");
    let identity = native_identity("tenant-a", "incarnation:backfill:missing");
    let (fixture, owner) = ledger_fixture(&path, identity);
    emit(&fixture, &owner, 2);
    fixture
        .mutations
        .outbox_subscribe(&owner, "projection", TOPIC)
        .unwrap();

    // Simulate a pre-index row by removing only its derived index fact. The
    // producer row remains authoritative and the repair must recreate the
    // exact commit-order key from it.
    let write =
        crate::admitted::AdmittedMutation::open(fixture.mutations_authority(), &owner).unwrap();
    let scope = eg_storage::ledger_scope_key(owner.identity());
    write
        .scoped_table(crate::tables::OUTBOX_TOPIC_INDEX)
        .unwrap()
        .remove((scope.as_str(), TOPIC, 2, 1_001, "batch-1", 0))
        .unwrap();
    write.commit().unwrap();

    let repaired = fixture.mutations.outbox_backfill_index(&owner).unwrap();
    assert_eq!(repaired.indexed, 1);
    assert!(repaired.complete);
    let mut sweep = budget(8, 10);
    let claimed = drain_claims(&fixture, &owner, "projection", &mut sweep);
    assert_eq!(
        claimed
            .iter()
            .map(|lease| lease.record.batch_id.as_str())
            .collect::<Vec<_>>(),
        vec!["batch-0", "batch-1"]
    );
}

#[test]
fn an_indexed_backfill_page_reports_more_work_even_when_it_inserts_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("native.redb");
    let identity = native_identity("tenant-a", "incarnation:backfill:page");
    let (fixture, owner) = ledger_fixture(&path, identity);
    emit(&fixture, &owner, 3);
    fixture
        .mutations
        .outbox_subscribe(&owner, "projection", TOPIC)
        .unwrap();

    // The test page is two primary rows. They are already indexed, but the
    // third row must still keep the durable migration state incomplete.
    let first = fixture.mutations.outbox_backfill_index(&owner).unwrap();
    assert_eq!(first.indexed, 0);
    assert_eq!(first.examined, 2);
    assert!(!first.complete);
    let read = fixture.kernel.read_scope(&owner).unwrap();
    let status = outbox_status(&read, "projection", 10).unwrap();
    assert!(!status.index_complete);
    assert!(status.pending_is_lower_bound);
    drop(read);

    let mut sweep = budget(8, 10);
    let deferred = claim_once(&fixture, &owner, "projection", &mut sweep);
    assert!(deferred.claims.is_empty());
    assert_eq!(
        deferred.deferred,
        Some(OutboxDeferral::IndexBackfillPending)
    );

    let second = fixture.mutations.outbox_backfill_index(&owner).unwrap();
    assert_eq!(second.indexed, 0);
    assert_eq!(second.examined, 1);
    assert!(second.complete);
    let read = fixture.kernel.read_scope(&owner).unwrap();
    assert!(
        outbox_status(&read, "projection", 10)
            .unwrap()
            .index_complete
    );
}

#[test]
fn a_legacy_outbox_row_without_a_commit_sequence_fails_backfill_closed() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("native.redb");
    let identity = native_identity("tenant-a", "incarnation:backfill:legacy");
    let (fixture, owner) = ledger_fixture(&path, identity);
    emit(&fixture, &owner, 1);
    fixture
        .mutations
        .outbox_subscribe(&owner, "projection", TOPIC)
        .unwrap();

    let write =
        crate::admitted::AdmittedMutation::open(fixture.mutations_authority(), &owner).unwrap();
    let scope = eg_storage::ledger_scope_key(owner.identity());
    let mut record = write
        .scoped_table(crate::tables::OUTBOX)
        .unwrap()
        .get((scope.as_str(), "batch-0", 0))
        .unwrap()
        .map(|value| eg_storage::decode_outbox_record(value.value()))
        .transpose()
        .unwrap()
        .unwrap();
    record.commit_sequence = None;
    let bytes = eg_storage::encode_bounded(&record, "legacy outbox record").unwrap();
    write
        .scoped_table(crate::tables::OUTBOX)
        .unwrap()
        .insert((scope.as_str(), "batch-0", 0), bytes.as_slice())
        .unwrap();
    write
        .scoped_table(crate::tables::OUTBOX_TOPIC_INDEX)
        .unwrap()
        .remove((scope.as_str(), TOPIC, 1, 1_000, "batch-0", 0))
        .unwrap();
    write.commit().unwrap();

    let error = fixture.mutations.outbox_backfill_index(&owner).unwrap_err();
    assert!(error.contains("CORRUPT_OUTBOX_BACKFILL"), "{error}");
    let read = fixture.kernel.read_scope(&owner).unwrap();
    assert!(read
        .scoped_table(crate::tables::OUTBOX_TOPIC_INDEX)
        .unwrap()
        .get((scope.as_str(), TOPIC, 1, 1_000, "batch-0", 0))
        .unwrap()
        .is_none());
    let status = outbox_status(&read, "projection", 20).unwrap();
    assert!(!status.index_complete);
    assert!(status.pending_is_lower_bound);
    drop(read);
    let mut sweep = budget(4, 20);
    let deferred = claim_once(&fixture, &owner, "projection", &mut sweep);
    assert!(deferred.claims.is_empty());
    assert_eq!(
        deferred.deferred,
        Some(OutboxDeferral::IndexBackfillPending)
    );
}

fn assert_backfill_rejects_corrupt_primary(
    incarnation: &str,
    mutate: impl FnOnce(&mut eg_types::MutationOutboxRecord),
) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("native.redb");
    let identity = native_identity("tenant-a", incarnation);
    let (fixture, owner) = ledger_fixture(&path, identity);
    emit(&fixture, &owner, 1);
    fixture
        .mutations
        .outbox_subscribe(&owner, "projection", TOPIC)
        .unwrap();

    let write =
        crate::admitted::AdmittedMutation::open(fixture.mutations_authority(), &owner).unwrap();
    let scope = eg_storage::ledger_scope_key(owner.identity());
    let mut record = write
        .scoped_table(crate::tables::OUTBOX)
        .unwrap()
        .get((scope.as_str(), "batch-0", 0))
        .unwrap()
        .map(|value| eg_storage::decode_outbox_record(value.value()))
        .transpose()
        .unwrap()
        .unwrap();
    mutate(&mut record);
    let bytes = eg_storage::encode_bounded(&record, "corrupt outbox record").unwrap();
    write
        .scoped_table(crate::tables::OUTBOX)
        .unwrap()
        .insert((scope.as_str(), "batch-0", 0), bytes.as_slice())
        .unwrap();
    write
        .scoped_table(crate::tables::OUTBOX_TOPIC_INDEX)
        .unwrap()
        .remove((scope.as_str(), TOPIC, 1, 1_000, "batch-0", 0))
        .unwrap();
    write
        .scoped_table(crate::tables::OUTBOX_CLAIM_CURSORS)
        .unwrap()
        .remove((scope.as_str(), "\u{1}kernel-outbox-index-backfill"))
        .unwrap();
    write.commit().unwrap();

    let error = fixture.mutations.outbox_backfill_index(&owner).unwrap_err();
    assert!(error.contains("CORRUPT_OUTBOX_BACKFILL"), "{error}");
    let read = fixture.kernel.read_scope(&owner).unwrap();
    assert!(read
        .scoped_table(crate::tables::OUTBOX_TOPIC_INDEX)
        .unwrap()
        .get((scope.as_str(), TOPIC, 1, 1_000, "batch-0", 0))
        .unwrap()
        .is_none());
    assert!(
        !outbox_status(&read, "projection", 10)
            .unwrap()
            .index_complete
    );
}

#[test]
fn backfill_rejects_primary_key_body_and_identity_mismatch() {
    assert_backfill_rejects_corrupt_primary("incarnation:backfill:key-body", |record| {
        record.batch_id = "body-batch-mismatch".to_string();
    });
    assert_backfill_rejects_corrupt_primary("incarnation:backfill:identity", |record| {
        record.identity = native_identity("tenant-a", "incarnation:foreign");
    });
    assert_backfill_rejects_corrupt_primary("incarnation:backfill:schema", |record| {
        record.schema_version = 0;
    });
}

#[test]
fn an_unmarked_legacy_scope_stays_incomplete_until_backfill() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("native.redb");
    let identity = native_identity("tenant-a", "incarnation:backfill:marker");
    let (fixture, owner) = ledger_fixture(&path, identity);
    emit(&fixture, &owner, 1);
    fixture
        .mutations
        .outbox_subscribe(&owner, "projection", TOPIC)
        .unwrap();

    let write =
        crate::admitted::AdmittedMutation::open(fixture.mutations_authority(), &owner).unwrap();
    let scope = eg_storage::ledger_scope_key(owner.identity());
    write
        .scoped_table(crate::tables::OUTBOX_CLAIM_CURSORS)
        .unwrap()
        .remove((scope.as_str(), "\u{1}kernel-outbox-index-backfill"))
        .unwrap();
    write.commit().unwrap();

    let read = fixture.kernel.read_scope(&owner).unwrap();
    let status = outbox_status(&read, "projection", 10).unwrap();
    assert!(!status.index_complete);
    assert!(status.pending_is_lower_bound);
    drop(read);
    let mut sweep = budget(4, 10);
    let deferred = claim_once(&fixture, &owner, "projection", &mut sweep);
    assert_eq!(
        deferred.deferred,
        Some(OutboxDeferral::IndexBackfillPending)
    );
}

#[test]
fn a_partial_backfill_marker_blocks_status_and_new_producers_atomically() {
    #[derive(serde::Serialize)]
    struct PlantedBackfillState {
        schema_version: u16,
        identity: MutationScopeIdentity,
        batch_id: Option<String>,
        ordinal: Option<u32>,
        complete: bool,
    }

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("native.redb");
    let identity = native_identity("tenant-a", "incarnation:backfill:partial-marker");
    let (fixture, owner) = ledger_fixture(&path, identity.clone());
    emit(&fixture, &owner, 1);

    let write =
        crate::admitted::AdmittedMutation::open(fixture.mutations_authority(), &owner).unwrap();
    let scope = eg_storage::ledger_scope_key(owner.identity());
    let planted = PlantedBackfillState {
        schema_version: MUTATION_BATCH_VERSION,
        identity,
        batch_id: Some("batch-0".to_string()),
        ordinal: None,
        complete: true,
    };
    let bytes = crate::outbox::encode_row(&planted, "planted backfill state").unwrap();
    write
        .scoped_table(crate::tables::OUTBOX_CLAIM_CURSORS)
        .unwrap()
        .insert(
            (scope.as_str(), "\u{1}kernel-outbox-index-backfill"),
            bytes.as_slice(),
        )
        .unwrap();
    write
        .scoped_table(crate::tables::OUTBOX_TOPIC_INDEX)
        .unwrap()
        .remove((scope.as_str(), TOPIC, 1, 1_000, "batch-0", 0))
        .unwrap();
    write.commit().unwrap();

    let read = fixture.kernel.read_scope(&owner).unwrap();
    let error = outbox_status(&read, "projection", 20).unwrap_err();
    assert!(error.contains("CORRUPT_OUTBOX_BACKFILL"), "{error}");
    drop(read);

    // The readiness check runs before the producer writes version, receipt,
    // class, outbox or index rows. Aborting this admitted write must leave the
    // prior version and malformed marker untouched.
    let next = event_batch(owner.identity(), "batch-1", 1, 1);
    let (write, begun) = fixture.mutations.admit(&owner, &next).unwrap();
    let source_version = match begun {
        Begin::Apply { source_version } => source_version,
        Begin::Replay(_) => panic!("unexpected replay"),
    };
    let error = fixture
        .mutations
        .finish(&write, &next, None, 2, source_version)
        .unwrap_err();
    assert!(error.contains("CORRUPT_OUTBOX_BACKFILL"), "{error}");
    write.abort().unwrap();

    let read = fixture.kernel.read_scope(&owner).unwrap();
    assert_eq!(version(&read).unwrap(), 1);
    assert!(read_outbox(&read, "batch-1").unwrap().is_empty());
    let error = outbox_status(&read, "projection", 20).unwrap_err();
    assert!(error.contains("CORRUPT_OUTBOX_BACKFILL"), "{error}");
}

#[test]
fn a_claim_cursor_cannot_skip_a_resolved_boundary_without_delivery_evidence() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("native.redb");
    let identity = native_identity("tenant-a", "incarnation:cursor:boundary");
    let (fixture, owner) = ledger_fixture(&path, identity.clone());
    emit(&fixture, &owner, 2);
    fixture
        .mutations
        .outbox_subscribe(&owner, "projection", TOPIC)
        .unwrap();

    let write =
        crate::admitted::AdmittedMutation::open(fixture.mutations_authority(), &owner).unwrap();
    let scope = eg_storage::ledger_scope_key(owner.identity());
    let cursor = OutboxClaimCursor {
        schema_version: MUTATION_BATCH_VERSION,
        identity,
        consumer: "projection".to_string(),
        resolved_through: Some(OutboxPosition {
            sequence: 2,
            created_at_ms: 1_001,
            batch_id: "batch-1".to_string(),
            ordinal: 0,
        }),
        acked_through: None,
    };
    let bytes = crate::outbox::encode_row(&cursor, "planted claim cursor").unwrap();
    write
        .scoped_table(crate::tables::OUTBOX_CLAIM_CURSORS)
        .unwrap()
        .insert((scope.as_str(), "projection"), bytes.as_slice())
        .unwrap();
    write.commit().unwrap();

    let read = fixture.kernel.read_scope(&owner).unwrap();
    let error = outbox_status(&read, "projection", 20).unwrap_err();
    assert!(error.contains("CORRUPT_OUTBOX_CURSOR"), "{error}");
    drop(read);
    let mut sweep = budget(4, 20);
    let error = fixture
        .mutations
        .outbox_claim(&owner, "projection", &mut sweep)
        .unwrap_err();
    assert!(error.contains("CORRUPT_OUTBOX_CURSOR"), "{error}");
    let read = fixture.kernel.read_scope(&owner).unwrap();
    assert!(read
        .scoped_table(crate::tables::OUTBOX_DELIVERIES)
        .unwrap()
        .get((scope.as_str(), "projection", "batch-1", 0))
        .unwrap()
        .is_none());
}

#[test]
fn expiry_rejects_a_dangling_cursor_position_before_writing() {
    #[derive(serde::Serialize)]
    struct PlantedExpiryCursor {
        schema_version: u16,
        identity: MutationScopeIdentity,
        consumer: String,
        position: Option<OutboxPosition>,
    }

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("native.redb");
    let identity = native_identity("tenant-a", "incarnation:expiry:integrity");
    let (fixture, owner) = ledger_fixture(&path, identity.clone());
    emit(&fixture, &owner, 1);
    fixture
        .mutations
        .outbox_subscribe(&owner, "projection", TOPIC)
        .unwrap();

    let write =
        crate::admitted::AdmittedMutation::open(fixture.mutations_authority(), &owner).unwrap();
    let scope = eg_storage::ledger_scope_key(owner.identity());
    let key = "\u{1}kernel-outbox-expiry/projection";
    let row = PlantedExpiryCursor {
        schema_version: MUTATION_BATCH_VERSION,
        identity,
        consumer: "projection".to_string(),
        position: Some(OutboxPosition {
            sequence: 999,
            created_at_ms: 1_001,
            batch_id: "batch-0".to_string(),
            ordinal: 0,
        }),
    };
    let bytes = crate::outbox::encode_row(&row, "planted expiry cursor").unwrap();
    write
        .scoped_table(crate::tables::OUTBOX_CLAIM_CURSORS)
        .unwrap()
        .insert((scope.as_str(), key), bytes.as_slice())
        .unwrap();
    write.commit().unwrap();

    let error = fixture
        .mutations
        .outbox_expire(&owner, "projection", 20)
        .unwrap_err();
    assert!(error.contains("CORRUPT_OUTBOX_EXPIRY"), "{error}");
}

#[test]
fn acknowledgement_rejects_a_delivery_position_that_disagrees_with_the_index() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("native.redb");
    let identity = native_identity("tenant-a", "incarnation:delivery:integrity");
    let (fixture, owner) = ledger_fixture(&path, identity);
    emit(&fixture, &owner, 1);
    fixture
        .mutations
        .outbox_subscribe(&owner, "projection", TOPIC)
        .unwrap();
    let mut sweep = budget(4, 10);
    let claimed = claim_once(&fixture, &owner, "projection", &mut sweep);
    let lease = claimed.claims.into_iter().next().unwrap();

    let write =
        crate::admitted::AdmittedMutation::open(fixture.mutations_authority(), &owner).unwrap();
    let scope = eg_storage::ledger_scope_key(owner.identity());
    let mut delivery = write
        .scoped_table(crate::tables::OUTBOX_DELIVERIES)
        .unwrap()
        .get((
            scope.as_str(),
            "projection",
            lease.record.batch_id.as_str(),
            lease.record.ordinal,
        ))
        .unwrap()
        .map(|value| crate::outbox::decode_row::<OutboxDelivery>(value.value()))
        .transpose()
        .unwrap()
        .unwrap();
    delivery.position.sequence = delivery.position.sequence.saturating_add(100);
    let bytes = crate::outbox::encode_row(&delivery, "planted delivery row").unwrap();
    write
        .scoped_table(crate::tables::OUTBOX_DELIVERIES)
        .unwrap()
        .insert(
            (
                scope.as_str(),
                "projection",
                lease.record.batch_id.as_str(),
                lease.record.ordinal,
            ),
            bytes.as_slice(),
        )
        .unwrap();
    write.commit().unwrap();

    let error = fixture
        .mutations
        .outbox_ack(&owner, &lease, 20)
        .unwrap_err();
    assert!(error.contains("CORRUPT_OUTBOX_DELIVERY"), "{error}");
}

#[test]
fn a_delivered_flag_without_acknowledged_watermark_is_corrupt() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("native.redb");
    let identity = native_identity("tenant-a", "incarnation:delivery:terminal-proof");
    let (fixture, owner) = ledger_fixture(&path, identity);
    emit(&fixture, &owner, 1);
    fixture
        .mutations
        .outbox_subscribe(&owner, "projection", TOPIC)
        .unwrap();
    let mut sweep = budget(4, 10);
    let lease = claim_once(&fixture, &owner, "projection", &mut sweep)
        .claims
        .pop()
        .unwrap();

    let write =
        crate::admitted::AdmittedMutation::open(fixture.mutations_authority(), &owner).unwrap();
    let scope = eg_storage::ledger_scope_key(owner.identity());
    let mut delivery = write
        .scoped_table(crate::tables::OUTBOX_DELIVERIES)
        .unwrap()
        .get((
            scope.as_str(),
            "projection",
            lease.record.batch_id.as_str(),
            lease.record.ordinal,
        ))
        .unwrap()
        .map(|value| crate::outbox::decode_row::<OutboxDelivery>(value.value()))
        .transpose()
        .unwrap()
        .unwrap();
    delivery.delivered_at_ms = Some(20);
    let bytes = crate::outbox::encode_row(&delivery, "planted terminal delivery").unwrap();
    write
        .scoped_table(crate::tables::OUTBOX_DELIVERIES)
        .unwrap()
        .insert(
            (
                scope.as_str(),
                "projection",
                lease.record.batch_id.as_str(),
                lease.record.ordinal,
            ),
            bytes.as_slice(),
        )
        .unwrap();
    write.commit().unwrap();

    let read = fixture.kernel.read_scope(&owner).unwrap();
    let error = outbox_status(&read, "projection", 20).unwrap_err();
    assert!(error.contains("CORRUPT_OUTBOX_DELIVERY"), "{error}");
    drop(read);
    let mut retry = budget(4, 20);
    let error = fixture
        .mutations
        .outbox_claim(&owner, "projection", &mut retry)
        .unwrap_err();
    assert!(error.contains("CORRUPT_OUTBOX_DELIVERY"), "{error}");
}

#[test]
fn status_rejects_a_durable_inflight_counter_that_disagrees_with_rows() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("native.redb");
    let identity = native_identity("tenant-a", "incarnation:status:counter");
    let (fixture, owner) = ledger_fixture(&path, identity.clone());
    emit(&fixture, &owner, 1);
    fixture
        .mutations
        .outbox_subscribe(&owner, "projection", TOPIC)
        .unwrap();

    let write =
        crate::admitted::AdmittedMutation::open(fixture.mutations_authority(), &owner).unwrap();
    let scope = eg_storage::ledger_scope_key(owner.identity());
    let state = OutboxConsumerState {
        schema_version: MUTATION_BATCH_VERSION,
        identity,
        consumer: "projection".to_string(),
        consecutive_claims: 0,
        total_claims: 0,
        last_claim_at_ms: 0,
        inflight: crate::outbox::queue_capacity(),
        delivered: 0,
        dead_lettered: 0,
    };
    let bytes = crate::outbox::encode_row(&state, "planted fairness row").unwrap();
    write
        .scoped_table(crate::tables::OUTBOX_FAIRNESS)
        .unwrap()
        .insert((scope.as_str(), "projection"), bytes.as_slice())
        .unwrap();
    write.commit().unwrap();

    let read = fixture.kernel.read_scope(&owner).unwrap();
    let error = outbox_status(&read, "projection", 20).unwrap_err();
    assert!(error.contains("CORRUPT_OUTBOX_FAIRNESS"), "{error}");
}

#[test]
fn status_marks_inflight_unknown_when_index_repair_is_pending() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("native.redb");
    let identity = native_identity("tenant-a", "incarnation:status:incomplete");
    let (fixture, owner) = ledger_fixture(&path, identity);
    emit(&fixture, &owner, 1);
    fixture
        .mutations
        .outbox_subscribe(&owner, "projection", TOPIC)
        .unwrap();
    let mut sweep = budget(4, 10);
    let lease = claim_once(&fixture, &owner, "projection", &mut sweep)
        .claims
        .pop()
        .unwrap();

    let write =
        crate::admitted::AdmittedMutation::open(fixture.mutations_authority(), &owner).unwrap();
    let scope = eg_storage::ledger_scope_key(owner.identity());
    write
        .scoped_table(crate::tables::OUTBOX_TOPIC_INDEX)
        .unwrap()
        .remove((scope.as_str(), TOPIC, 1, 1_000, "batch-0", 0))
        .unwrap();
    write
        .scoped_table(crate::tables::OUTBOX_CLAIM_CURSORS)
        .unwrap()
        .remove((scope.as_str(), "\u{1}kernel-outbox-index-backfill"))
        .unwrap();
    write.commit().unwrap();

    let read = fixture.kernel.read_scope(&owner).unwrap();
    let status = outbox_status(&read, "projection", 100_000).unwrap();
    assert_eq!(status.inflight, 0);
    assert!(status.inflight_is_lower_bound);
    assert!(!status.saturated);
    drop(lease);
}

#[test]
fn a_full_queue_claims_nothing_and_leaves_every_intention_pending() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("native.redb");
    let identity = native_identity("tenant-a", "incarnation:outbox:full");
    let (fixture, owner) = ledger_fixture(&path, identity);
    let events = crate::outbox::queue_capacity() + 8;
    let batch = event_batch(owner.identity(), "wide", 0, events);
    apply_batch(&fixture, &owner, &batch);
    fixture
        .mutations
        .outbox_subscribe(&owner, "projection", TOPIC)
        .unwrap();

    let mut sweep = budget(u32::MAX, 10);
    let claimed = drain_claims(&fixture, &owner, "projection", &mut sweep);
    assert_eq!(claimed.len() as u32, crate::outbox::queue_capacity());

    // Queue full: the next claim takes nothing, says explicitly that it is
    // backpressure rather than an idle queue, and drops nothing.
    let mut again = budget(64, 20);
    let outcome = claim_once(&fixture, &owner, "projection", &mut again);
    assert!(outcome.claims.is_empty());
    assert_eq!(
        outcome.deferred,
        Some(OutboxDeferral::QueueFull {
            inflight: crate::outbox::queue_capacity(),
            capacity: crate::outbox::queue_capacity()
        })
    );
    let read = fixture.kernel.read_scope(&owner).unwrap();
    let status = outbox_status(&read, "projection", 20).unwrap();
    assert!(status.saturated);
    assert!(status.live);
    assert_eq!(status.pending, u64::from(events));
    assert_eq!(status.lag_rows, u64::from(events));
    assert_eq!(status.capacity, crate::outbox::queue_capacity());

    // Status is time-aware even before a caller runs the explicit expiry
    // maintenance operation: an expired full queue is no longer saturated.
    let expired = outbox_status(&read, "projection", 100_000).unwrap();
    assert_eq!(expired.inflight, 0);
    assert!(!expired.inflight_is_lower_bound);
    assert!(!expired.saturated);
}

#[test]
fn status_reports_liveness_lag_and_the_oldest_pending_age() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("native.redb");
    let identity = native_identity("tenant-a", "incarnation:outbox:10");
    let (fixture, owner) = ledger_fixture(&path, identity);
    emit(&fixture, &owner, 2);

    let read = fixture.kernel.read_scope(&owner).unwrap();
    let dead = outbox_status(&read, "projection", 5_000).unwrap();
    assert!(!dead.live);
    assert_eq!(dead.topic, None);
    assert_eq!(dead.lag_versions, 2);
    drop(read);

    fixture
        .mutations
        .outbox_subscribe(&owner, "projection", TOPIC)
        .unwrap();
    let read = fixture.kernel.read_scope(&owner).unwrap();
    let live = outbox_status(&read, "projection", 5_000).unwrap();
    assert!(live.live);
    assert_eq!(live.topic.as_deref(), Some(TOPIC));
    assert_eq!(live.pending, 2);
    // The oldest pending row was created at 1_000.
    assert_eq!(live.oldest_pending_age_ms, 4_000);
}

#[test]
fn a_consumer_may_not_change_the_topic_its_cursor_already_names() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("native.redb");
    let identity = native_identity("tenant-a", "incarnation:outbox:11");
    let (fixture, owner) = ledger_fixture(&path, identity);
    fixture
        .mutations
        .outbox_subscribe(&owner, "projection", TOPIC)
        .unwrap();
    fixture
        .mutations
        .outbox_subscribe(&owner, "projection", TOPIC)
        .unwrap();
    assert!(fixture
        .mutations
        .outbox_subscribe(&owner, "projection", "other.topic")
        .is_err());
}

#[test]
fn one_scopes_claims_never_reach_another_scopes_rows() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("native.redb");
    let alpha = native_identity("tenant-a", "incarnation:confine:a");
    let beta = native_identity("tenant-b", "incarnation:confine:b");
    let fixture = Fixture::create::<LedgerOnlyOwner>(&path, "physical:test:ledger-only", None);
    let owner_a = bind_scope(&fixture, "tenant-a", alpha);
    let owner_b = bind_scope(&fixture, "tenant-b", beta);
    emit(&fixture, &owner_a, 2);
    for owner in [&owner_a, &owner_b] {
        fixture
            .mutations
            .outbox_subscribe(owner, "projection", TOPIC)
            .unwrap();
    }

    let mut sweep = budget(8, 10);
    assert!(claim_once(&fixture, &owner_b, "projection", &mut sweep)
        .claims
        .is_empty());

    // Tenant A's own lease may not be acknowledged through tenant B's handle.
    let mut own = budget(8, 10);
    let claimed = drain_claims(&fixture, &owner_a, "projection", &mut own);
    assert!(fixture
        .mutations
        .outbox_ack(&owner_b, &claimed[0], 20)
        .is_err());
}

#[test]
fn an_invalid_consumer_name_can_never_key_a_durable_row() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("native.redb");
    let identity = native_identity("tenant-a", "incarnation:outbox:12");
    let (fixture, owner) = ledger_fixture(&path, identity);
    for name in ["", " padded", "with\nnewline"] {
        assert!(fixture
            .mutations
            .outbox_subscribe(&owner, name, TOPIC)
            .is_err());
    }
}

#[test]
fn a_claim_budget_needs_a_real_limit_and_a_real_lease() {
    assert!(OutboxClaimBudget::new(0, 1, 0).is_err());
    assert!(OutboxClaimBudget::new(1, 0, 0).is_err());
    assert_eq!(
        OutboxClaimBudget::new(1, 1, 0).unwrap().consecutive_cap(),
        1
    );
    assert_eq!(
        OutboxClaimBudget::new(100, 1, 0).unwrap().consecutive_cap(),
        25
    );
}
