//! Grafting one bound scope's ledger between two kernel-owned files
//! (RF-RULING-004 application note 4).
//!
//! The properties under test are the ones the fenced two-phase design exists
//! for: a source that cannot move once it is marked, a crash window that
//! converges on exactly one authority, rows that cross byte-identically, and a
//! destination that refuses anything it did not itself put there.

use super::*;
use crate::graft::{GraftDestination, GraftSource};
use crate::outbox::{outbox_cursor, outbox_status, OutboxClaimBudget};
use crate::read::{read_batches, read_class, read_fences};
use crate::AdmittedMutation;
use eg_storage::{encode_bounded, ledger_scope_key, ScopeFence};
use eg_types::{MutationBatchRecord, MutationOutboxIntent};
use std::collections::BTreeMap;

const TOPIC: &str = "engine.projection.rebuild";

fn graph_identity(incarnation: &str) -> MutationScopeIdentity {
    MutationScopeIdentity::graph(
        ScopeTenantId::new("tenant-a").unwrap(),
        LogicalName::new("graph-a").unwrap(),
        IncarnationId::new(incarnation).unwrap(),
    )
}

/// The exact bytes each receipt is stored as.
///
/// `MutationBatchRecord` has no `PartialEq`, and comparing the bytes is the
/// stronger claim anyway: a graft that re-encoded a row would pass a
/// field-by-field comparison and still have rewritten the ledger.
fn encoded(records: &[MutationBatchRecord]) -> Vec<Vec<u8>> {
    records
        .iter()
        .map(|record| encode_bounded(record, "grafted receipt").unwrap())
        .collect()
}

fn bind_scope(
    fixture: &Fixture,
    tenant: &'static str,
    identity: MutationScopeIdentity,
) -> OwnedStoreHandle<LedgerOnlyOwner> {
    fixture.bind::<LedgerOnlyOwner>(&verifier(tenant, OwnerLayout::LedgerOnly), identity)
}

fn event_batch(identity: &MutationScopeIdentity, batch_id: &str, version: u64) -> MutationBatch {
    let mut value = batch(identity.clone(), batch_id);
    value.version_expectation = VersionExpectation::Native(version);
    value.created_at_ms = 1_000 + version;
    value.outbox = vec![MutationOutboxIntent {
        topic: TOPIC.to_string(),
        key: batch_id.to_string(),
        payload: b"payload".to_vec(),
        headers: BTreeMap::new(),
    }];
    value
        .reseal_envelope(eg_types::contract::Digest256::from_bytes([1_u8; 32]))
        .expect("a graft fixture batch reseals its final outbox");
    value.validate().unwrap();
    value
}

/// One populated source file: three committed batches, a subscribed consumer,
/// one delivered row and one still leased, so every ledger table that can hold
/// a row for this scope does.
fn populated_source(
    path: &Path,
    identity: &MutationScopeIdentity,
) -> (Fixture, OwnedStoreHandle<LedgerOnlyOwner>) {
    let fixture = Fixture::create::<LedgerOnlyOwner>(path, "physical:test:source", None);
    let owner = bind_scope(&fixture, "tenant-a", identity.clone());
    for version in 0..3 {
        let batch = event_batch(identity, &format!("batch-{version}"), version);
        apply_batch(&fixture, &owner, &batch);
    }
    fixture
        .mutations
        .outbox_subscribe(&owner, "projection", TOPIC)
        .unwrap();
    let mut sweep = OutboxClaimBudget::new(8, 5_000, 10).unwrap();
    let claimed = fixture
        .mutations
        .outbox_claim(&owner, "projection", &mut sweep)
        .unwrap()
        .claims;
    fixture
        .mutations
        .outbox_ack(&owner, &claimed[0], 20)
        .unwrap();
    (fixture, owner)
}

fn empty_destination(
    path: &Path,
    identity: MutationScopeIdentity,
    tenant: &'static str,
) -> (Fixture, OwnedStoreHandle<LedgerOnlyOwner>) {
    let fixture = Fixture::create::<LedgerOnlyOwner>(path, "physical:test:destination", None);
    let owner = bind_scope(&fixture, tenant, identity);
    (fixture, owner)
}

fn graft_proof_record(
    fixture: &Fixture,
    owner: &OwnedStoreHandle<LedgerOnlyOwner>,
    reservation: bool,
) -> MutationBatchRecord {
    let read = fixture.kernel.read_scope(owner).unwrap();
    read_batches(&read)
        .unwrap()
        .into_iter()
        .find(|record| {
            let reserved = record
                .batch
                .batch_id
                .starts_with("kernel.graft/reservation/");
            record.batch.batch_id.starts_with("kernel.graft/") && reserved == reservation
        })
        .unwrap()
}

fn assert_destination_fenced(
    fixture: &Fixture,
    owner: &OwnedStoreHandle<LedgerOnlyOwner>,
    identity: &MutationScopeIdentity,
    case: &str,
) {
    let blocked = event_batch(identity, "destination-stays-fenced", 1);
    let error = match fixture.mutations.admit(owner, &blocked) {
        Err(error) => error,
        Ok((write, _)) => {
            write.abort().unwrap();
            panic!("{case}: destination became writable after refused graft")
        }
    };
    assert!(error.starts_with("STALE_FENCE"), "{case}: {error}");
}

#[test]
fn a_graft_moves_every_ledger_row_verbatim_and_retires_the_source() {
    let dir = tempfile::tempdir().unwrap();
    let identity = native_identity("tenant-a", "incarnation:graft:1");
    let (source, source_owner) = populated_source(&dir.path().join("source.redb"), &identity);
    let (destination, destination_owner) =
        empty_destination(&dir.path().join("dest.redb"), identity.clone(), "tenant-a");
    let target = GraftDestination::new(
        &destination.mutations,
        &destination.kernel,
        &destination_owner,
    );

    let intent = source
        .mutations
        .graft_begin(&source_owner, &target)
        .unwrap();
    assert_eq!(intent.identity, identity);

    let read = source.kernel.read_scope(&source_owner).unwrap();
    let expected_batches = encoded(&read_batches(&read).unwrap());
    let expected_outbox = read_outbox(&read, "batch-1").unwrap();
    let expected_class = read_class(&read, "batch-1").unwrap();
    let expected_cursor = outbox_cursor(&read, "projection").unwrap();
    let expected_version = version(&read).unwrap();
    assert_eq!(expected_version, intent.version);
    drop(read);

    let grafted = destination
        .mutations
        .graft_scope(
            GraftSource::new(&source.mutations, &source.kernel, &source_owner),
            &target,
        )
        .unwrap();
    assert_eq!(grafted.identity, identity);
    assert_eq!(grafted.version, expected_version);
    assert!(!grafted.resumed);
    assert!(grafted.rows > 0);

    let moved = destination.kernel.read_scope(&destination_owner).unwrap();
    assert_eq!(encoded(&read_batches(&moved).unwrap()), expected_batches);
    assert_eq!(read_outbox(&moved, "batch-1").unwrap(), expected_outbox);
    assert_eq!(read_class(&moved, "batch-1").unwrap(), expected_class);
    assert_eq!(
        outbox_cursor(&moved, "projection").unwrap(),
        expected_cursor
    );
    assert_eq!(version(&moved).unwrap(), expected_version);

    // The delivery side moved too: one row delivered, one lease still held.
    let status = outbox_status(&moved, "projection", 20).unwrap();
    assert!(status.live);
    assert_eq!(status.topic.as_deref(), Some(TOPIC));
    assert_eq!(status.delivered, 1);
    assert_eq!(status.inflight, 1);

    // The graft fence is gone at the destination, so it admits ordinary work.
    let fence = read_fences(&moved).unwrap().expect("a grafted fence");
    assert_eq!(fence.placement_epoch, 0);
    drop(moved);
    let next = event_batch(&identity, "after-graft", expected_version);
    apply_batch(&destination, &destination_owner, &next);

    // Exactly one file serves the scope now.
    assert!(source.kernel.read_scope(&source_owner).is_err());
}

#[test]
fn a_source_write_after_the_graft_marker_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let identity = native_identity("tenant-a", "incarnation:graft:fence");
    let (source, source_owner) = populated_source(&dir.path().join("source.redb"), &identity);
    let (destination, destination_owner) =
        empty_destination(&dir.path().join("dest.redb"), identity.clone(), "tenant-a");
    let target = GraftDestination::new(
        &destination.mutations,
        &destination.kernel,
        &destination_owner,
    );
    let intent = source
        .mutations
        .graft_begin(&source_owner, &target)
        .unwrap();

    let blocked = event_batch(&identity, "after-fence", intent.version);
    match source.mutations.admit(&source_owner, &blocked) {
        Ok(_) => panic!("a scope under graft admitted a write"),
        Err(error) => assert!(error.starts_with("STALE_FENCE"), "{error}"),
    }
    let mut equal_sentinel = event_batch(&identity, "equal-sentinel", intent.version);
    equal_sentinel.placement_epoch = crate::graft::GRAFT_FENCE;
    equal_sentinel.fencing_token = Some(crate::graft::GRAFT_FENCE);
    match source.mutations.admit(&source_owner, &equal_sentinel) {
        Ok(_) => panic!("a maximum-fence batch bypassed a graft barrier"),
        Err(error) => assert!(error.starts_with("STALE_FENCE"), "{error}"),
    }

    // Delivery-side writers do not pass through batch admission, so they must
    // honor the same source fence explicitly; otherwise a late lease or
    // subscription could be purged after the destination snapshot.
    let blocked_subscription = source
        .mutations
        .outbox_subscribe(&source_owner, "late-projection", TOPIC)
        .unwrap_err();
    assert!(
        blocked_subscription.starts_with("STALE_FENCE"),
        "{blocked_subscription}"
    );
    let blocked_retirement = source
        .mutations
        .purge_scope(&source_owner, &identity)
        .unwrap_err();
    assert!(
        blocked_retirement.starts_with("STALE_FENCE"),
        "{blocked_retirement}"
    );

    // Phase A is idempotent: a repeat replays its own marker.
    let again = source
        .mutations
        .graft_begin(&source_owner, &target)
        .unwrap();
    assert_eq!(again, intent);
}

#[test]
fn a_restart_after_destination_reservation_resumes_without_an_unfenced_window() {
    let dir = tempfile::tempdir().unwrap();
    let source_path = dir.path().join("source.redb");
    let destination_path = dir.path().join("dest.redb");
    let identity = native_identity("tenant-a", "incarnation:graft:reservation-restart");
    let (source, source_owner) = populated_source(&source_path, &identity);
    let (destination, destination_owner) =
        empty_destination(&destination_path, identity.clone(), "tenant-a");
    let target = GraftDestination::new(
        &destination.mutations,
        &destination.kernel,
        &destination_owner,
    );
    crate::graft::graft_reserve_for_test(&source.mutations, &source_owner, &target).unwrap();

    let blocked = event_batch(&identity, "during-reservation", 1);
    let error = match destination.mutations.admit(&destination_owner, &blocked) {
        Err(error) => error,
        Ok((write, _)) => {
            write.abort().unwrap();
            panic!("destination admitted a write while its reservation was durable")
        }
    };
    assert!(error.starts_with("STALE_FENCE"), "{error}");

    drop(source_owner);
    drop(destination_owner);
    drop(source);
    drop(destination);
    let source = Fixture::open::<LedgerOnlyOwner>(&source_path, "physical:test:source", None);
    let source_owner = bind_scope(&source, "tenant-a", identity.clone());
    let destination =
        Fixture::open::<LedgerOnlyOwner>(&destination_path, "physical:test:destination", None);
    let destination_owner = bind_scope(&destination, "tenant-a", identity.clone());
    let target = GraftDestination::new(
        &destination.mutations,
        &destination.kernel,
        &destination_owner,
    );
    let intent = source
        .mutations
        .graft_begin(&source_owner, &target)
        .unwrap();

    // Lose the process again after the source marker but before the copy.
    // Exact target-format re-entry must replay the marker and reservation.
    drop(source_owner);
    drop(destination_owner);
    drop(source);
    drop(destination);
    let source = Fixture::open::<LedgerOnlyOwner>(&source_path, "physical:test:source", None);
    let source_owner = bind_scope(&source, "tenant-a", identity.clone());
    let destination =
        Fixture::open::<LedgerOnlyOwner>(&destination_path, "physical:test:destination", None);
    let destination_owner = bind_scope(&destination, "tenant-a", identity.clone());
    let target = GraftDestination::new(
        &destination.mutations,
        &destination.kernel,
        &destination_owner,
    );
    assert_eq!(
        source
            .mutations
            .graft_begin(&source_owner, &target)
            .unwrap(),
        intent
    );
    destination
        .mutations
        .graft_scope(
            GraftSource::new(&source.mutations, &source.kernel, &source_owner),
            &target,
        )
        .unwrap();
    assert!(!source.kernel.scope_binding_exists(&identity).unwrap());
}

#[test]
fn a_crash_between_the_copy_and_the_retirement_recovers_to_one_authority() {
    let dir = tempfile::tempdir().unwrap();
    let source_path = dir.path().join("source.redb");
    let destination_path = dir.path().join("dest.redb");
    let identity = native_identity("tenant-a", "incarnation:graft:resume");
    let (source, source_owner) = populated_source(&source_path, &identity);
    let (destination, destination_owner) =
        empty_destination(&destination_path, identity.clone(), "tenant-a");
    let target = GraftDestination::new(
        &destination.mutations,
        &destination.kernel,
        &destination_owner,
    );
    source
        .mutations
        .graft_begin(&source_owner, &target)
        .unwrap();

    // Commit phase B without phase C, then admit legitimate destination work.
    // A retry must accept this monotonic progress rather than requiring the
    // destination to remain frozen at the copied marker version.
    let first = crate::graft::graft_copy_for_test(
        GraftSource::new(&source.mutations, &source.kernel, &source_owner),
        &target,
    )
    .unwrap();
    assert!(!first.resumed);
    apply_batch(
        &destination,
        &destination_owner,
        &event_batch(&identity, "after-phase-b", first.version),
    );
    let error = destination
        .mutations
        .graft_recover(&source.kernel, &identity, &target)
        .unwrap_err();
    assert!(error.contains("source binding to be retired"), "{error}");

    // Drop both kernels and all opaque handles. Reopening the two files is the
    // process-boundary state the recovery contract must actually survive.
    drop(source_owner);
    drop(destination_owner);
    drop(source);
    drop(destination);
    let source = Fixture::open::<LedgerOnlyOwner>(&source_path, "physical:test:source", None);
    let source_owner = bind_scope(&source, "tenant-a", identity.clone());
    let destination =
        Fixture::open::<LedgerOnlyOwner>(&destination_path, "physical:test:destination", None);
    let destination_owner = bind_scope(&destination, "tenant-a", identity.clone());
    let target = GraftDestination::new(
        &destination.mutations,
        &destination.kernel,
        &destination_owner,
    );

    // Re-running is the documented recovery, and it works: the destination
    // recognises its own completed copy and finishes the retirement.
    let resumed = destination
        .mutations
        .graft_scope(
            GraftSource::new(&source.mutations, &source.kernel, &source_owner),
            &target,
        )
        .unwrap();
    assert!(resumed.resumed);
    assert_eq!(resumed.rows, 0);
    assert_eq!(resumed.version, first.version + 1);
    assert!(source.kernel.read_scope(&source_owner).is_err());

    // And it is byte-identical to what the first run produced.
    let moved = destination.kernel.read_scope(&destination_owner).unwrap();
    assert_eq!(version(&moved).unwrap(), first.version + 1);
}

#[test]
fn an_occ_expectation_formed_at_the_fenced_source_is_valid_at_the_destination() {
    let dir = tempfile::tempdir().unwrap();
    let identity = native_identity("tenant-a", "incarnation:graft:2");
    let (source, source_owner) = populated_source(&dir.path().join("source.redb"), &identity);
    let (destination, destination_owner) =
        empty_destination(&dir.path().join("dest.redb"), identity.clone(), "tenant-a");
    let target = GraftDestination::new(
        &destination.mutations,
        &destination.kernel,
        &destination_owner,
    );
    let intent = source
        .mutations
        .graft_begin(&source_owner, &target)
        .unwrap();
    destination
        .mutations
        .graft_scope(
            GraftSource::new(&source.mutations, &source.kernel, &source_owner),
            &target,
        )
        .unwrap();

    // The version the scope carried at the source is the version it carries at
    // the destination: the move continues the counter, it does not reset it.
    let in_flight = event_batch(&identity, "in-flight", intent.version);
    apply_batch(&destination, &destination_owner, &in_flight);

    let stale = event_batch(&identity, "stale", 0);
    match destination.mutations.admit(&destination_owner, &stale) {
        Ok(_) => panic!("a stale version expectation was admitted"),
        Err(error) => assert!(error.starts_with("STALE_VERSION"), "{error}"),
    }
}

#[test]
fn a_graft_between_two_owner_layouts_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let identity = native_identity("tenant-a", "incarnation:graft:3");
    let (source, source_owner) = populated_source(&dir.path().join("source.redb"), &identity);
    let blob =
        Fixture::create::<BlobOwner>(&dir.path().join("blob.redb"), "physical:test:blob", None);
    let blob_owner =
        blob.bind::<BlobOwner>(&verifier("tenant-a", OwnerLayout::Blob), identity.clone());
    let target = GraftDestination::new(&blob.mutations, &blob.kernel, &blob_owner);

    let error = source
        .mutations
        .graft_begin(&source_owner, &target)
        .unwrap_err();
    assert!(error.contains("two owner layouts"), "{error}");
    // The source is untouched: a refused graft fences nothing.
    assert!(source.kernel.read_scope(&source_owner).is_ok());
}

#[test]
fn a_graft_between_two_scopes_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let identity = native_identity("tenant-a", "incarnation:graft:4");
    let other = native_identity("tenant-b", "incarnation:graft:4");
    let (source, source_owner) = populated_source(&dir.path().join("source.redb"), &identity);
    let (destination, destination_owner) =
        empty_destination(&dir.path().join("dest.redb"), other, "tenant-b");
    let target = GraftDestination::new(
        &destination.mutations,
        &destination.kernel,
        &destination_owner,
    );

    let error = source
        .mutations
        .graft_begin(&source_owner, &target)
        .unwrap_err();
    assert!(error.contains("same serving scope"), "{error}");
    assert!(source.kernel.read_scope(&source_owner).is_ok());
}

#[test]
fn a_destination_that_holds_anything_of_its_own_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let identity = native_identity("tenant-a", "incarnation:graft:5");
    let (source, source_owner) = populated_source(&dir.path().join("source.redb"), &identity);
    let (destination, destination_owner) =
        empty_destination(&dir.path().join("dest.redb"), identity.clone(), "tenant-a");
    let target = GraftDestination::new(
        &destination.mutations,
        &destination.kernel,
        &destination_owner,
    );

    // A consumer subscription writes no batch and bumps no version, so it used
    // to pass the emptiness check and then be silently overwritten.  Phase A1
    // now observes it before fencing the source.
    destination
        .mutations
        .outbox_subscribe(&destination_owner, "projection", "other.topic")
        .unwrap();
    let error = source
        .mutations
        .graft_begin(&source_owner, &target)
        .unwrap_err();
    assert!(error.contains("already holds ledger rows"), "{error}");

    // Refusal leaves the source live and writable.
    let next = event_batch(&identity, "after-refusal", 3);
    apply_batch(&source, &source_owner, &next);
    let error = destination
        .mutations
        .graft_scope(
            GraftSource::new(&source.mutations, &source.kernel, &source_owner),
            &target,
        )
        .unwrap_err();
    assert!(
        error.contains("no graft marker") || error.contains("no exact"),
        "{error}"
    );

    // Nothing landed, and the source still serves its scope.
    let moved = destination.kernel.read_scope(&destination_owner).unwrap();
    assert!(read_batches(&moved).unwrap().is_empty());
    assert_eq!(version(&moved).unwrap(), 0);
    drop(moved);
    assert!(source.kernel.read_scope(&source_owner).is_ok());
}

#[test]
fn a_graft_to_the_same_authority_is_refused_before_fencing() {
    let dir = tempfile::tempdir().unwrap();
    let identity = native_identity("tenant-a", "incarnation:graft:self");
    let (fixture, owner) = populated_source(&dir.path().join("same.redb"), &identity);
    let target = GraftDestination::new(&fixture.mutations, &fixture.kernel, &owner);
    let error = fixture.mutations.graft_begin(&owner, &target).unwrap_err();
    assert!(error.contains("same authority"), "{error}");
    let next = event_batch(&identity, "after-self-refusal", 3);
    apply_batch(&fixture, &owner, &next);
}

#[test]
fn mismatched_destination_kernels_are_refused_before_fencing() {
    let dir = tempfile::tempdir().unwrap();
    let identity = native_identity("tenant-a", "incarnation:graft:destination-authority");
    let (source, source_owner) = populated_source(&dir.path().join("source.redb"), &identity);
    let (destination, destination_owner) = empty_destination(
        &dir.path().join("destination.redb"),
        identity.clone(),
        "tenant-a",
    );
    let (other, _other_owner) =
        empty_destination(&dir.path().join("other.redb"), identity.clone(), "tenant-a");
    let mismatched =
        GraftDestination::new(&destination.mutations, &other.kernel, &destination_owner);
    let error = source
        .mutations
        .graft_begin(&source_owner, &mismatched)
        .unwrap_err();
    assert!(error.contains("different authorities"), "{error}");
    apply_batch(
        &source,
        &source_owner,
        &event_batch(&identity, "after-mismatched-destination", 3),
    );
}

#[test]
fn recovery_after_retirement_needs_no_source_handle_or_rebind() {
    let dir = tempfile::tempdir().unwrap();
    let source_path = dir.path().join("source.redb");
    let destination_path = dir.path().join("dest.redb");
    let identity = native_identity("tenant-a", "incarnation:graft:recover");
    let (source, source_owner) = populated_source(&source_path, &identity);
    let (destination, destination_owner) =
        empty_destination(&destination_path, identity.clone(), "tenant-a");
    let target = GraftDestination::new(
        &destination.mutations,
        &destination.kernel,
        &destination_owner,
    );
    source
        .mutations
        .graft_begin(&source_owner, &target)
        .unwrap();
    destination
        .mutations
        .graft_scope(
            GraftSource::new(&source.mutations, &source.kernel, &source_owner),
            &target,
        )
        .unwrap();

    drop(source_owner);
    drop(destination_owner);
    drop(source);
    drop(destination);

    let source = Fixture::open::<LedgerOnlyOwner>(&source_path, "physical:test:source", None);
    assert!(!source.kernel.scope_binding_exists(&identity).unwrap());
    let destination =
        Fixture::open::<LedgerOnlyOwner>(&destination_path, "physical:test:destination", None);
    let destination_owner = bind_scope(&destination, "tenant-a", identity.clone());
    let target = GraftDestination::new(
        &destination.mutations,
        &destination.kernel,
        &destination_owner,
    );
    let recovered = destination
        .mutations
        .graft_recover(&source.kernel, &identity, &target)
        .unwrap();
    assert!(recovered.resumed);
    assert_eq!(recovered.rows, 0);
    assert_eq!(
        version(&destination.kernel.read_scope(&destination_owner).unwrap()).unwrap(),
        recovered.version
    );
    assert!(!source.kernel.scope_binding_exists(&identity).unwrap());
}

#[test]
fn a_graph_scope_uses_graph_marker_domain_and_expectation() {
    let dir = tempfile::tempdir().unwrap();
    let identity = graph_identity("incarnation:graft:graph");
    let (source, source_owner) = ledger_fixture(&dir.path().join("source.redb"), identity.clone());
    let (destination, destination_owner) =
        ledger_fixture(&dir.path().join("dest.redb"), identity.clone());
    let target = GraftDestination::new(
        &destination.mutations,
        &destination.kernel,
        &destination_owner,
    );
    let intent = source
        .mutations
        .graft_begin(&source_owner, &target)
        .unwrap();
    assert_eq!(intent.identity, identity);
    destination
        .mutations
        .graft_scope(
            GraftSource::new(&source.mutations, &source.kernel, &source_owner),
            &target,
        )
        .unwrap();
}

#[test]
fn public_admission_cannot_forge_either_reserved_graft_key() {
    let dir = tempfile::tempdir().unwrap();
    let identity = native_identity("tenant-a", "incarnation:graft:namespace");
    let (fixture, owner) = ledger_fixture(&dir.path().join("scope.redb"), identity.clone());

    let reserved_batch_id = batch(
        identity.clone(),
        &format!("kernel.graft/{}", "a".repeat(64)),
    );
    let error = match fixture.mutations.admit(&owner, &reserved_batch_id) {
        Err(error) => error,
        Ok((write, _)) => {
            write.abort().unwrap();
            panic!("public operation admission accepted a reserved graft batch id")
        }
    };
    assert!(error.contains("kernel-owned"), "{error}");

    let mut reserved_maintenance = batch(identity, "ordinary-batch");
    reserved_maintenance.envelope = eg_types::mutation_batch::MutationEnvelope::maintenance(
        PRINCIPAL,
        "fixture_maintenance",
        "ordinary-batch",
        &format!(
            "kernel.graft/reservation/{}/{}",
            "b".repeat(64),
            "c".repeat(64)
        ),
    )
    .unwrap();
    let error = match fixture.mutations.admit(&owner, &reserved_maintenance) {
        Err(error) => error,
        Ok((write, _)) => {
            write.abort().unwrap();
            panic!("public maintenance admission accepted a reserved graft idempotency key")
        }
    };
    assert!(error.contains("kernel-owned"), "{error}");
    assert_eq!(
        version(&fixture.kernel.read_scope(&owner).unwrap()).unwrap(),
        0
    );
}

#[test]
fn resume_rejects_a_noncanonical_or_corrupt_marker_receipt() {
    let dir = tempfile::tempdir().unwrap();
    let identity = native_identity("tenant-a", "incarnation:graft:marker-proof");
    let (source, source_owner) = populated_source(&dir.path().join("source.redb"), &identity);
    let (destination, destination_owner) =
        empty_destination(&dir.path().join("dest.redb"), identity.clone(), "tenant-a");
    let target = GraftDestination::new(
        &destination.mutations,
        &destination.kernel,
        &destination_owner,
    );
    source
        .mutations
        .graft_begin(&source_owner, &target)
        .unwrap();

    let read = source.kernel.read_scope(&source_owner).unwrap();
    let mut marker = read_batches(&read)
        .unwrap()
        .into_iter()
        .find(|record| {
            record.batch.batch_id.starts_with("kernel.graft/")
                && !record
                    .batch
                    .batch_id
                    .starts_with("kernel.graft/reservation/")
        })
        .unwrap();
    drop(read);
    let Method::ApplyMutation { query, .. } = &mut marker.batch.operations[0].method else {
        panic!("graft marker lost its mutation operation")
    };
    query.push('/');
    let bytes = encode_bounded(&marker, "corrupt graft marker").unwrap();
    let scope = ledger_scope_key(&identity);
    let write = AdmittedMutation::open(source.mutations_authority(), &source_owner).unwrap();
    write
        .scoped_table(crate::tables::BATCHES)
        .unwrap()
        .insert(
            (scope.as_str(), marker.batch.batch_id.as_str()),
            bytes.as_slice(),
        )
        .unwrap();
    write.commit().unwrap();

    let error = source
        .mutations
        .graft_begin(&source_owner, &target)
        .unwrap_err();
    assert!(
        error.contains("not a graft intent") || error.contains("exact"),
        "{error}"
    );
}

#[test]
fn phase_b_requires_the_exact_source_fence_as_well_as_its_version() {
    for damage in ["missing", "lowered", "wrong-identity"] {
        let dir = tempfile::tempdir().unwrap();
        let identity = native_identity(
            "tenant-a",
            &format!("incarnation:graft:source-fence:{damage}"),
        );
        let (source, source_owner) = populated_source(&dir.path().join("source.redb"), &identity);
        let (destination, destination_owner) =
            empty_destination(&dir.path().join("dest.redb"), identity.clone(), "tenant-a");
        let target = GraftDestination::new(
            &destination.mutations,
            &destination.kernel,
            &destination_owner,
        );
        source
            .mutations
            .graft_begin(&source_owner, &target)
            .unwrap();

        let scope = ledger_scope_key(&identity);
        let write = AdmittedMutation::open(source.mutations_authority(), &source_owner).unwrap();
        let mut fences = write.scoped_table(crate::tables::FENCES).unwrap();
        if damage == "missing" {
            fences.remove(scope.as_str()).unwrap();
        } else {
            let stamped_identity = if damage == "wrong-identity" {
                native_identity("tenant-a", "incarnation:graft:foreign-fence")
            } else {
                identity.clone()
            };
            let value = ScopeFence {
                identity: stamped_identity,
                placement_epoch: if damage == "lowered" {
                    0
                } else {
                    crate::graft::GRAFT_FENCE
                },
                fencing_token: if damage == "lowered" {
                    0
                } else {
                    crate::graft::GRAFT_FENCE
                },
            };
            let bytes = encode_bounded(&value, "damaged source fence").unwrap();
            fences.insert(scope.as_str(), bytes.as_slice()).unwrap();
        }
        drop(fences);
        write.commit().unwrap();

        let error = destination
            .mutations
            .graft_scope(
                GraftSource::new(&source.mutations, &source.kernel, &source_owner),
                &target,
            )
            .unwrap_err();
        assert!(error.contains("durable maximum fence"), "{damage}: {error}");
        assert!(source.kernel.scope_binding_exists(&identity).unwrap());
        assert_destination_fenced(&destination, &destination_owner, &identity, damage);
    }
}

#[test]
fn marker_and_reservation_proofs_require_their_exact_maintenance_claim() {
    for proof in ["marker", "reservation"] {
        for damage in ["missing", "redirected"] {
            let dir = tempfile::tempdir().unwrap();
            let identity = native_identity(
                "tenant-a",
                &format!("incarnation:graft:maintenance:{proof}:{damage}"),
            );
            let (source, source_owner) =
                populated_source(&dir.path().join("source.redb"), &identity);
            let (destination, destination_owner) =
                empty_destination(&dir.path().join("dest.redb"), identity.clone(), "tenant-a");
            let target = GraftDestination::new(
                &destination.mutations,
                &destination.kernel,
                &destination_owner,
            );
            source
                .mutations
                .graft_begin(&source_owner, &target)
                .unwrap();

            let (fixture, owner) = if proof == "marker" {
                (&source, &source_owner)
            } else {
                (&destination, &destination_owner)
            };
            let record = graft_proof_record(fixture, owner, proof == "reservation");
            let scope = ledger_scope_key(&identity);
            let write = AdmittedMutation::open(fixture.mutations_authority(), owner).unwrap();
            let mut mappings = write.scoped_table(crate::tables::MAINTENANCE).unwrap();
            let key = (scope.as_str(), record.batch.idempotency_key());
            if damage == "missing" {
                mappings.remove(key).unwrap();
            } else {
                mappings.insert(key, "redirected-batch").unwrap();
            }
            drop(mappings);
            write.commit().unwrap();

            let error = destination
                .mutations
                .graft_scope(
                    GraftSource::new(&source.mutations, &source.kernel, &source_owner),
                    &target,
                )
                .unwrap_err();
            assert!(
                error.contains("exact durable maintenance claim"),
                "{proof}/{damage}: {error}"
            );
            assert!(source.kernel.scope_binding_exists(&identity).unwrap());
            assert_destination_fenced(
                &destination,
                &destination_owner,
                &identity,
                &format!("{proof}/{damage}"),
            );
        }
    }
}

#[test]
fn only_a_durable_different_source_marker_cancels_a_losing_reservation() {
    let dir = tempfile::tempdir().unwrap();
    let identity = native_identity("tenant-a", "incarnation:graft:race");
    let (source, source_owner) = populated_source(&dir.path().join("source.redb"), &identity);
    let (winner, winner_owner) = empty_destination(
        &dir.path().join("winner.redb"),
        identity.clone(),
        "tenant-a",
    );
    let (loser, loser_owner) =
        empty_destination(&dir.path().join("loser.redb"), identity.clone(), "tenant-a");
    let winner_target = GraftDestination::new(&winner.mutations, &winner.kernel, &winner_owner);
    let loser_target = GraftDestination::new(&loser.mutations, &loser.kernel, &loser_owner);

    source
        .mutations
        .graft_begin(&source_owner, &winner_target)
        .unwrap();
    let error = source
        .mutations
        .graft_begin(&source_owner, &loser_target)
        .unwrap_err();
    assert!(error.starts_with("STALE_FENCE"), "{error}");

    // The source marker is authenticated evidence for the winner and remains
    // unchanged by the losing attempt.
    let marker = source
        .mutations
        .graft_source_marker(&source_owner)
        .unwrap()
        .expect("the winning source marker");
    assert_eq!(
        marker.source,
        hex::encode(source.kernel.owner_authority_digest().unwrap())
    );
    assert_eq!(
        marker.destination,
        hex::encode(winner.kernel.owner_authority_digest().unwrap())
    );

    // Only that proof-gated path removes the exact losing reservation and
    // restores the destination's baseline. A normal batch can now proceed.
    let read = loser.kernel.read_scope(&loser_owner).unwrap();
    assert!(read_batches(&read).unwrap().iter().all(|record| !record
        .batch
        .batch_id
        .starts_with("kernel.graft/reservation/")));
    assert!(read_fences(&read).unwrap().is_none());
    drop(read);
    apply_batch(
        &loser,
        &loser_owner,
        &event_batch(&identity, "loser-live", 0),
    );
    let read = loser.kernel.read_scope(&loser_owner).unwrap();
    assert!(read_batches(&read).unwrap().iter().all(|record| !record
        .batch
        .batch_id
        .starts_with("kernel.graft/reservation/")));
    // The ordinary batch above re-asserts the scope's own baseline fence --
    // `write_fence` records `(placement_epoch, fencing_token)` on EVERY commit,
    // so absence is only ever true for a scope no batch has committed on. What
    // must hold here is that the graft reservation left nothing behind: the
    // fence is back to the plain `(0, 0)` baseline, not the reservation's.
    let fence = read_fences(&read)
        .unwrap()
        .expect("an ordinary admitted batch leaves its own baseline fence");
    assert_eq!(
        (fence.placement_epoch, fence.fencing_token),
        (0, 0),
        "a cancelled reservation must restore the plain baseline fence"
    );
}

#[test]
fn an_unproven_source_maximum_fence_keeps_a_losing_reservation() {
    let dir = tempfile::tempdir().unwrap();
    let identity = native_identity("tenant-a", "incarnation:graft:uncertain");
    let (source, source_owner) = populated_source(&dir.path().join("source.redb"), &identity);
    let (loser, loser_owner) =
        empty_destination(&dir.path().join("loser.redb"), identity.clone(), "tenant-a");
    let loser_target = GraftDestination::new(&loser.mutations, &loser.kernel, &loser_owner);
    crate::graft::graft_reserve_for_test(&source.mutations, &source_owner, &loser_target).unwrap();

    let scope = ledger_scope_key(&identity);
    let write = AdmittedMutation::open(source.mutations_authority(), &source_owner).unwrap();
    let fence = ScopeFence {
        identity: identity.clone(),
        placement_epoch: crate::graft::GRAFT_FENCE,
        fencing_token: crate::graft::GRAFT_FENCE,
    };
    let bytes = encode_bounded(&fence, "uncertain source fence").unwrap();
    write
        .scoped_table(crate::tables::FENCES)
        .unwrap()
        .insert(scope.as_str(), bytes.as_slice())
        .unwrap();
    write.commit().unwrap();

    let error = source
        .mutations
        .graft_begin(&source_owner, &loser_target)
        .unwrap_err();
    assert!(error.starts_with("STALE_FENCE"), "{error}");
    assert!(graft_proof_record(&loser, &loser_owner, true)
        .batch
        .batch_id
        .starts_with("kernel.graft/reservation/"));
    assert_destination_fenced(&loser, &loser_owner, &identity, "unproven source fence");
}

#[test]
fn the_graft_moves_every_table_the_ledger_census_declares() {
    let mut grafted = crate::graft::grafted_table_names();
    let mut declared = crate::tables::ledger_table_names();
    grafted.sort_unstable();
    declared.sort_unstable();
    assert_eq!(grafted, declared);
}
