//! Destination reservation and provenance checks for a two-file graft.

use super::*;

/// Reserve the destination before phase A2 fences the source.
///
/// The reservation is a normal kernel-owned maintenance receipt, but its
/// namespace is admitted only through `begin_graft`.  The destination write
/// lock makes the empty-baseline check and the maximum fence one atomic
/// decision: once this returns, no ordinary destination writer can enter.
pub(super) fn reserve_destination<D: OwnerDomain>(
    destination: &GraftDestination<'_, D>,
    identity: &MutationScopeIdentity,
    source_digest: &str,
    destination_digest: &str,
) -> Result<(), String> {
    let write = destination.mutation.open_write(destination.owner)?;
    let result = reserve_destination_in(
        destination,
        &write,
        identity,
        source_digest,
        destination_digest,
    );
    match result {
        Ok(Reservation::Existing) => write.abort(),
        Ok(Reservation::Fresh {
            batch,
            source_version,
        }) => {
            // A destination reservation is still a kernel-owned mutation on
            // owner-bearing layouts.  It writes no owner rows, but the
            // admission protocol must be closed explicitly before the
            // maintenance receipt can be sealed; otherwise the owner gate
            // poisons the write at `finish` and every GraphShard reservation
            // fails before its fence becomes durable.
            if !owner_table_names(D::LAYOUT).is_empty() {
                if let Err(error) = write
                    .owner_rows(destination.owner, &batch)
                    .and_then(|owner_write| owner_write.finish_owner())
                {
                    write.abort()?;
                    return Err(error);
                }
            }
            match destination.mutation.finish(
                &write,
                &batch,
                None,
                batch.created_at_ms,
                source_version,
            ) {
                Ok(_) => destination.mutation.commit(write, &batch),
                Err(error) => {
                    write.abort()?;
                    Err(error)
                }
            }
        }
        Err(error) => {
            write.abort()?;
            Err(error)
        }
    }
}

/// A reservation is never auto-cancelled from an uncertain source-side
/// failure. Before the source write exists, a separate destination transaction
/// can race a same-target marker commit. The source-transaction variant below
/// is the only cancellation path: its write lock first rechecks the target,
/// then authenticates a different durable marker and the maximum source fence.
pub(super) fn with_losing_reservation_cleanup<S: OwnerDomain, D: OwnerDomain>(
    primary: String,
    _kernel: &MutationKernel,
    _source: &OwnedStoreHandle<S>,
    _destination: &GraftDestination<'_, D>,
    _identity: &MutationScopeIdentity,
    _source_digest: &str,
    _target: &str,
) -> String {
    primary
}

/// Cancel exactly one losing reservation while `source_write` still owns the
/// source writer. A same-target marker, absent marker, mismatched source proof,
/// or non-maximum source fence leaves the reservation as the recovery handle.
/// The destination transaction verifies the complete reservation receipt,
/// maintenance mapping, class, baseline version, and fence before removing it.
pub(super) fn cleanup_losing_reservation_in<S: OwnerDomain, D: OwnerDomain>(
    primary: String,
    source_write: &AdmittedMutation<'_, S>,
    destination: &GraftDestination<'_, D>,
    identity: &MutationScopeIdentity,
    source_digest: &str,
    target: &str,
) -> String {
    // Recheck the requested target first while the source writer is held. If
    // it appeared, this attempt may have raced its own same-target marker and
    // must never remove that reservation.
    match super::marker_in(source_write, identity, target) {
        Ok(Some(_)) | Err(_) => return primary,
        Ok(None) => {}
    }
    let marker = match super::source_marker_in(source_write, identity) {
        Ok(Some(marker)) => marker,
        Ok(None) | Err(_) => return primary,
    };
    if marker.source != source_digest
        || marker.destination == target
        || super::current_fence(source_write, identity).ok()
            != Some((super::GRAFT_FENCE, super::GRAFT_FENCE))
    {
        return primary;
    }
    match cancel_reservation(destination, identity, source_digest, target) {
        Ok(()) => primary,
        Err(error) => format!("{primary}; losing reservation was retained: {error}"),
    }
}

fn cancel_reservation<D: OwnerDomain>(
    destination: &GraftDestination<'_, D>,
    identity: &MutationScopeIdentity,
    source_digest: &str,
    target: &str,
) -> Result<(), String> {
    let write = destination.mutation.open_write(destination.owner)?;
    let reservation = match reservation_in(&write, identity, source_digest, target) {
        Ok(Some(record)) => record,
        Ok(None) => {
            write.abort()?;
            return Ok(());
        }
        Err(error) => {
            write.abort()?;
            return Err(error);
        }
    };
    if let Err(error) = require_reservation_baseline(&write, identity, &reservation) {
        write.abort()?;
        return Err(error);
    }
    // Clearing a reservation also removes its maximum fence and restores the
    // destination at version zero. That is safe for LedgerOnly layouts, whose
    // owner census is empty, or when the real owner supplies its authenticated
    // retirement capability. For an owner-bearing layout without that proof,
    // retain the reservation as the recovery handle: otherwise staged payload
    // would become writable again at v0 and could be mistaken for a fresh scope.
    match destination.owner_payload() {
        Some(retirement) => {
            if let Err(error) = retirement.retire_owner_payload(write.capability(), identity) {
                write.abort()?;
                return Err(error);
            }
        }
        None if !owner_table_names(D::LAYOUT).is_empty() => {
            write.abort()?;
            return Err(
                "graft losing reservation has owner payload but no authenticated retirement"
                    .to_string(),
            );
        }
        None => {}
    }
    if let Err(error) = super::clear_reservation(&write, identity, &reservation) {
        write.abort()?;
        return Err(error);
    }
    write.commit()
}

/// A reserved destination may contain exactly its own reservation evidence and
/// the binding's zero version.  Any other row means an unrelated writer landed
/// after reservation (or a pre-planted lookalike was supplied), so phase B must
/// refuse instead of recognizing the reservation and overwriting that history.
pub(super) fn require_reservation_baseline<D: OwnerDomain>(
    write: &AdmittedMutation<'_, D>,
    identity: &MutationScopeIdentity,
    reservation: &eg_types::MutationBatchRecord,
) -> Result<(), String> {
    let scope = ledger_scope_key(identity);
    let reservation_id = reservation.batch.batch_id.as_str();

    macro_rules! only_text {
        ($table:expr, $allowed:expr) => {{
            let table = write.scoped_table($table)?;
            for row in table.range_inclusive(
                (scope.as_str(), ""),
                (scope.as_str(), MAX_BATCH_ID_SENTINEL),
            )? {
                let (key, _) = row.map_err(|error| error.to_string())?;
                if key.value().1 != $allowed {
                    return Err(
                        "graft destination reservation has unrelated ledger rows".to_string()
                    );
                }
            }
        }};
    }

    macro_rules! empty_text {
        ($table:expr) => {{
            let table = write.scoped_table($table)?;
            if table
                .range_inclusive(
                    (scope.as_str(), ""),
                    (scope.as_str(), MAX_BATCH_ID_SENTINEL),
                )?
                .next()
                .transpose()
                .map_err(|error| error.to_string())?
                .is_some()
            {
                return Err("graft destination reservation has unrelated ledger rows".to_string());
            }
        }};
    }

    // The reservation itself is expected in these three tables; all other
    // scope-text tables must be empty.
    only_text!(BATCHES, reservation_id);
    only_text!(MAINTENANCE, reservation_id);
    only_text!(CLASSES, reservation_id);
    empty_text!(PRIVATE_PAYLOADS);
    empty_text!(REPLAY_NONCES);
    empty_text!(REPLAY_OPERATIONS);
    empty_text!(OUTBOX_CONSUMERS);
    empty_text!(OUTBOX_CURSORS);
    empty_text!(OUTBOX_CLAIM_CURSORS);
    empty_text!(OUTBOX_FAIRNESS);

    macro_rules! empty_event {
        ($table:expr) => {{
            let table = write.scoped_table($table)?;
            if table
                .range_inclusive(
                    (scope.as_str(), "", 0),
                    (scope.as_str(), MAX_BATCH_ID_SENTINEL, u32::MAX),
                )?
                .next()
                .transpose()
                .map_err(|error| error.to_string())?
                .is_some()
            {
                return Err("graft destination reservation has unrelated ledger rows".to_string());
            }
        }};
    }
    empty_event!(OUTBOX);

    let deliveries = write.scoped_table(OUTBOX_DELIVERIES)?;
    if deliveries
        .range_inclusive(
            (scope.as_str(), "", "", 0),
            (
                scope.as_str(),
                MAX_BATCH_ID_SENTINEL,
                MAX_BATCH_ID_SENTINEL,
                u32::MAX,
            ),
        )?
        .next()
        .transpose()
        .map_err(|error| error.to_string())?
        .is_some()
    {
        return Err("graft destination reservation has unrelated ledger rows".to_string());
    }
    let index = write.scoped_table(OUTBOX_TOPIC_INDEX)?;
    if index
        .range_inclusive(
            (scope.as_str(), "", 0, 0, "", 0),
            (
                scope.as_str(),
                MAX_BATCH_ID_SENTINEL,
                u64::MAX,
                u64::MAX,
                MAX_BATCH_ID_SENTINEL,
                u32::MAX,
            ),
        )?
        .next()
        .transpose()
        .map_err(|error| error.to_string())?
        .is_some()
    {
        return Err("graft destination reservation has unrelated ledger rows".to_string());
    }
    let version = write
        .scoped_table(VERSIONS)?
        .get(scope.as_str())?
        .map(|value| value.value());
    if version != Some(1) || current_fence(write, identity)? != (GRAFT_FENCE, GRAFT_FENCE) {
        return Err(
            "graft destination reservation state is not fenced at its baseline".to_string(),
        );
    }
    Ok(())
}

enum Reservation {
    Existing,
    Fresh {
        /// Boxed purely to keep this call-local return type small next to
        /// the unit `Existing` arm; `Reservation` never crosses a durable
        /// boundary, so the box has no wire effect.
        batch: Box<MutationBatch>,
        source_version: Option<u64>,
    },
}

fn reserve_destination_in<D: OwnerDomain>(
    destination: &GraftDestination<'_, D>,
    write: &AdmittedMutation<'_, D>,
    identity: &MutationScopeIdentity,
    source_digest: &str,
    destination_digest: &str,
) -> Result<Reservation, String> {
    write.verify_scope(identity)?;
    if let Some(record) = reservation_in(write, identity, source_digest, destination_digest)? {
        validate_reservation_state(write, identity, &record)?;
        require_reservation_baseline(write, identity, &record)?;
        return Ok(Reservation::Existing);
    }
    require_empty_destination(write, &ledger_scope_key(identity))?;
    let version = crate::ledger::bound_scope_version(write, identity)?;
    let batch = reservation_batch(
        destination.owner,
        source_digest,
        destination_digest,
        version,
    )?;
    let begun = write.begin_graft(&batch)?;
    match begun {
        Begin::Apply { source_version } => Ok(Reservation::Fresh {
            batch: Box::new(batch),
            source_version,
        }),
        Begin::Replay(record) => {
            validate_reservation_record(identity, source_digest, destination_digest, &record)?;
            super::require_maintenance_mapping(write, identity, &record, "graft reservation")?;
            Ok(Reservation::Existing)
        }
    }
}

fn reservation_batch<D: OwnerDomain>(
    owner: &OwnedStoreHandle<D>,
    source_digest: &str,
    destination_digest: &str,
    version: u64,
) -> Result<MutationBatch, String> {
    let batch_id = reservation_batch_id(source_digest, destination_digest);
    let batch = MutationBatch {
        schema_version: MUTATION_BATCH_VERSION,
        batch_id: batch_id.clone(),
        envelope: MutationEnvelope::maintenance_for_scope(
            owner.identity(),
            owner.principal(),
            GRAFT_RESERVATION,
            &batch_id,
        )?,
        identity: owner.identity().clone(),
        placement_epoch: GRAFT_FENCE,
        version_expectation: scope_expectation(owner.identity(), version),
        fencing_token: Some(GRAFT_FENCE),
        authoritative_state: None,
        operations: vec![MutationOperation {
            ordinal: 0,
            surface: MutationSurface::Other,
            domain: scope_domain(owner.identity()),
            method: Method::ApplyMutation {
                event_type: GRAFT_RESERVATION.to_string(),
                query: reservation_query(source_digest, destination_digest),
            },
        }],
        outbox: Vec::new(),
        created_at_ms: 0,
    };
    batch.validate()?;
    Ok(batch)
}

pub(super) fn reservation_in<D: OwnerDomain>(
    write: &AdmittedMutation<'_, D>,
    identity: &MutationScopeIdentity,
    source_digest: &str,
    destination_digest: &str,
) -> Result<Option<eg_types::MutationBatchRecord>, String> {
    let batch_id = reservation_batch_id(source_digest, destination_digest);
    let scope = ledger_scope_key(identity);
    let table = write.scoped_table(BATCHES)?;
    let record = table
        .get((scope.as_str(), batch_id.as_str()))?
        .map(|value| decode_batch_record(value.value()))
        .transpose()?;
    let Some(record) = record else {
        return Ok(None);
    };
    validate_reservation_record(identity, source_digest, destination_digest, &record)?;
    super::require_maintenance_mapping(write, identity, &record, "graft reservation")?;
    let class = write
        .scoped_table(CLASSES)?
        .get((scope.as_str(), record.batch.batch_id.as_str()))?
        .map(|value| decode_ledger_record::<MutationClassRow>(value.value()))
        .transpose()?
        .ok_or_else(|| "graft destination reservation has no durable class".to_string())?;
    if class.identity != *identity
        || class.batch_id != record.batch.batch_id
        || class.class != MutationClass::Maintenance
    {
        return Err("graft destination reservation has an invalid class".to_string());
    }
    Ok(Some(record))
}

/// Prove that a destination marker represents a completed phase-B copy before
/// a retry is allowed to retire the source.  The reservation must be gone and
/// the copied scope must retain at least the marker's version and its restored
/// route fence. Later destination writes are valid before source retirement.
pub(super) fn validate_destination_phase_b<D: OwnerDomain>(
    write: &AdmittedMutation<'_, D>,
    identity: &MutationScopeIdentity,
    intent: &GraftIntent,
    destination: &str,
) -> Result<u64, String> {
    if reservation_in(write, identity, &intent.source, destination)?.is_some() {
        return Err("graft destination still carries its reservation".to_string());
    }
    let version = crate::ledger::bound_scope_version(write, identity)?;
    let fence = current_fence(write, identity)?;
    let restore = (intent.restore_epoch, intent.restore_token);
    if version < intent.version || fence == (GRAFT_FENCE, GRAFT_FENCE) || fence < restore {
        return Err("graft destination marker is not a completed phase-B copy".to_string());
    }
    Ok(version)
}

fn validate_reservation_record(
    identity: &MutationScopeIdentity,
    source_digest: &str,
    destination_digest: &str,
    record: &eg_types::MutationBatchRecord,
) -> Result<(), String> {
    let batch_id = reservation_batch_id(source_digest, destination_digest);
    if record.status != MutationBatchStatus::Committed
        || record.identity != *identity
        || record.batch.schema_version != MUTATION_BATCH_VERSION
        || record.batch.identity != *identity
        || record.batch.batch_id != batch_id
        || record.batch.idempotency_key() != batch_id.as_str()
        || record.batch.envelope
            != MutationEnvelope::maintenance_for_scope(
                identity,
                record.batch.serving_principal(),
                GRAFT_RESERVATION,
                record.batch.idempotency_key(),
            )?
        || record.committed_version.target() != Some(1)
        || record.batch.placement_epoch != GRAFT_FENCE
        || record.batch.fencing_token != Some(GRAFT_FENCE)
        || record.batch.version_expectation != scope_expectation(identity, 0)
        || record.batch.operations.len() != 1
        || !record.batch.outbox.is_empty()
        || record.batch.authoritative_state.is_some()
        || record.batch.created_at_ms != 0
    {
        return Err(
            "graft destination reservation is not an exact committed reservation".to_string(),
        );
    }
    let Some(operation) = record.batch.operations.first() else {
        return Err("graft destination reservation lost its operation".to_string());
    };
    let Method::ApplyMutation { event_type, query } = &operation.method else {
        return Err("graft destination reservation lost its operation".to_string());
    };
    if operation.ordinal != 0
        || operation.surface != MutationSurface::Other
        || operation.domain != scope_domain(identity)
        || event_type != GRAFT_RESERVATION
        || query != &reservation_query(source_digest, destination_digest)
    {
        return Err("graft destination reservation does not name this graft".to_string());
    }
    Ok(())
}

pub(super) fn validate_reservation_state<D: OwnerDomain>(
    write: &AdmittedMutation<'_, D>,
    identity: &MutationScopeIdentity,
    record: &eg_types::MutationBatchRecord,
) -> Result<(), String> {
    if crate::ledger::bound_scope_version(write, identity)? != 1
        || current_fence(write, identity)? != (GRAFT_FENCE, GRAFT_FENCE)
    {
        return Err(
            "graft destination reservation state is not fenced at its baseline".to_string(),
        );
    }
    if record.committed_version.target() != Some(1) {
        return Err("graft destination reservation has an unexpected version".to_string());
    }
    Ok(())
}

pub(super) fn scope_expectation(
    identity: &MutationScopeIdentity,
    version: u64,
) -> VersionExpectation {
    match identity.scope() {
        MutationScope::Graph { .. } => VersionExpectation::Graph(version),
        MutationScope::Native { .. } => VersionExpectation::Native(version),
    }
}

pub(super) fn scope_domain(identity: &MutationScopeIdentity) -> DurabilityDomain {
    match identity.scope() {
        MutationScope::Graph { .. } => DurabilityDomain::ControlPlane,
        MutationScope::Native { domain, .. } => *domain,
    }
}

/// A destination scope that holds anything of its own is not a graft target.
/// The binding's zero version is the sole allowed baseline; every ledger row,
/// including delivery-side rows that do not bump the version, is a refusal.
pub(super) fn require_empty_destination<D: OwnerDomain>(
    write: &AdmittedMutation<'_, D>,
    scope: &str,
) -> Result<(), String> {
    macro_rules! reject_if_any {
        ($table:expr, $low:expr, $high:expr) => {{
            let table = write.scoped_table($table)?;
            if table
                .range_inclusive($low, $high)?
                .next()
                .transpose()
                .map_err(|error| error.to_string())?
                .is_some()
            {
                return Err("graft destination scope already holds ledger rows".to_string());
            }
        }};
    }

    reject_if_any!(FENCES, scope, scope);
    reject_if_any!(BATCHES, (scope, ""), (scope, MAX_BATCH_ID_SENTINEL));
    reject_if_any!(MAINTENANCE, (scope, ""), (scope, MAX_BATCH_ID_SENTINEL));
    reject_if_any!(
        PRIVATE_PAYLOADS,
        (scope, ""),
        (scope, MAX_BATCH_ID_SENTINEL)
    );
    reject_if_any!(CLASSES, (scope, ""), (scope, MAX_BATCH_ID_SENTINEL));
    reject_if_any!(REPLAY_NONCES, (scope, ""), (scope, MAX_BATCH_ID_SENTINEL));
    reject_if_any!(
        REPLAY_OPERATIONS,
        (scope, ""),
        (scope, MAX_BATCH_ID_SENTINEL)
    );
    reject_if_any!(
        OUTBOX_CONSUMERS,
        (scope, ""),
        (scope, MAX_BATCH_ID_SENTINEL)
    );
    reject_if_any!(OUTBOX_CURSORS, (scope, ""), (scope, MAX_BATCH_ID_SENTINEL));
    reject_if_any!(
        OUTBOX_CLAIM_CURSORS,
        (scope, ""),
        (scope, MAX_BATCH_ID_SENTINEL)
    );
    reject_if_any!(OUTBOX_FAIRNESS, (scope, ""), (scope, MAX_BATCH_ID_SENTINEL));
    reject_if_any!(
        OUTBOX,
        (scope, "", 0),
        (scope, MAX_BATCH_ID_SENTINEL, u32::MAX)
    );
    reject_if_any!(
        OUTBOX_DELIVERIES,
        (scope, "", "", 0),
        (
            scope,
            MAX_BATCH_ID_SENTINEL,
            MAX_BATCH_ID_SENTINEL,
            u32::MAX
        )
    );
    reject_if_any!(
        OUTBOX_TOPIC_INDEX,
        (scope, "", 0, 0, "", 0),
        (
            scope,
            MAX_BATCH_ID_SENTINEL,
            u64::MAX,
            u64::MAX,
            MAX_BATCH_ID_SENTINEL,
            u32::MAX
        )
    );

    if crate::ledger::bound_scope_version(write, write.scope())? != 0 {
        return Err("graft destination scope has already been written".to_string());
    }
    Ok(())
}
