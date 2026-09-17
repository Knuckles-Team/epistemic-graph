//! Transplanting one bound scope's ledger between two kernel-owned files
//! (RF-RULING-004 application note 4).
//!
//! Shard migration and online reshard move ONE graph out of shard file A into
//! shard file B. Under one mutation authority a domain crate cannot write a
//! ledger row at all, and re-admitting each moved batch is not equivalent to
//! moving it: re-admission re-executes effects, re-emits outbox rows, stamps
//! new receipts, and resets the version an in-flight OCC expectation depends
//! on. So the move is the kernel's, and it is a **graft**.
//!
//! # Three phases, because two files are two transactions
//!
//! There is no atomic move across files. What replaces atomicity is a durable,
//! fail-closed marker and an exact version fence:
//!
//! * **Phase A, at the source** ([`MutationKernel::graft_begin`]) — one admitted
//!   maintenance batch whose version is resolved IN-LOCK, carrying a reserved
//!   batch id that names the destination. `commit::finish` writes its fence row
//!   at `(u64::MAX, u64::MAX)`, so from that commit onward **every ordinary
//!   admission at the source fails closed with `STALE_FENCE`** — the scope is
//!   under graft and cannot move. The batch id is the idempotency key, so
//!   re-running phase A on an already-marked scope replays rather than marking
//!   twice.
//! * **Phase B, at the destination** ([`MutationKernel::graft_scope`]) — opens
//!   its OWN snapshot of the source (never a caller's), refuses unless the
//!   source is marked for exactly this destination and has not moved since the
//!   marker, copies every ledger row verbatim, and restores the pre-graft fence
//!   the marker recorded so the destination is writable. One transaction.
//! * **Phase C, at the source** — retire the binding, inside a transaction that
//!   re-proves the source version still equals the marker's, so a write that
//!   somehow landed in the window is refused rather than destroyed.
//!
//! # Recovery
//!
//! A crash anywhere is resumable, because every phase is idempotent and the
//! evidence is in the data. [`MutationKernel::graft_scope`] IS the resume entry
//! point: a destination that already carries the copied marker receipt for this
//! destination is recognised as its own completed phase B and skips straight to
//! phase C. A destination that is empty runs phase B. Any other non-empty
//! destination — including one that merely has a consumer subscription — is
//! refused, because two ledgers for one scope cannot be merged.
//!
//! The earlier design took a caller-supplied snapshot and purged the source in
//! a fresh transaction with no version check, so any batch or acknowledgement
//! committed at the source after the snapshot was silently destroyed; and its
//! documented "re-running completes it" recovery was refused by its own
//! emptiness precondition. Neither is possible now.

use crate::admitted::{AdmittedMutation, AdmittedOwnerWrite};
use crate::commit::MAX_BATCH_ID_SENTINEL;
use crate::tables::{
    BATCHES, CLASSES, FENCES, MAINTENANCE, OUTBOX, OUTBOX_CLAIM_CURSORS, OUTBOX_CONSUMERS,
    OUTBOX_CURSORS, OUTBOX_DELIVERIES, OUTBOX_FAIRNESS, OUTBOX_TOPIC_INDEX, PRIVATE_PAYLOADS,
    REPLAY_NONCES, REPLAY_OPERATIONS, VERSIONS,
};
use crate::{Begin, MutationKernel};
use eg_storage::{
    decode_batch_record, decode_ledger_record, encode_bounded, ledger_scope_key, owner_table_names,
    LedgerRowScope, MutationClass, MutationClassRow, MutationOwnerAuthority, OwnedStoreHandle,
    OwnerDomain, OwnerPayloadRetirement, PhysicalWriteCapability, ScopeFence, ScopedOwnerTableMut,
    ScopedRead, StorageKernel,
};
use eg_types::mutation_batch::{DurabilityDomain, MutationEnvelope, MutationScope};
use eg_types::protocol::Method;
use eg_types::{
    MutationBatch, MutationBatchStatus, MutationOperation, MutationScopeIdentity, MutationSurface,
    VersionExpectation, MUTATION_BATCH_VERSION,
};
use redb::TableDefinition;

/// The owner-row write granted while a destination reservation is in force.
///
/// A graft payload transfer must not manufacture a normal maintenance batch:
/// doing so would leave a ledger receipt in the destination before Phase B and
/// make the empty-baseline proof impossible.  This narrow view exposes only
/// the destination layout's owner tables, and is constructed only by the
/// graft transaction after it has authenticated the exact reservation.
pub struct GraftOwnerWrite<'cap, 'store, D: OwnerDomain> {
    capability: &'cap PhysicalWriteCapability<'store, D>,
}

/// Owner-table access shared by a normal admitted owner window and the
/// reservation-authenticated graft window.  It deliberately has no ledger
/// accessor.
pub trait OwnerPayloadWrite {
    /// The logical scope authorized by this owner-row capability.
    ///
    /// This is read-only provenance used by callers that must reject a
    /// mismatched graph before opening any owner table.
    fn scope(&self) -> &MutationScope;

    fn open_table<K, V>(
        &self,
        definition: TableDefinition<'static, K, V>,
    ) -> Result<redb::Table<'_, K, V>, String>
    where
        K: redb::Key + 'static,
        V: redb::Value + 'static;

    fn open_scoped_table<K, V>(
        &self,
        definition: TableDefinition<'static, K, V>,
    ) -> Result<ScopedOwnerTableMut<'_, K, V>, String>
    where
        K: redb::Key + 'static,
        for<'k> K::SelfType<'k>: eg_storage::OwnerRowScope,
        V: redb::Value + 'static;
}

impl<'a, D: OwnerDomain> OwnerPayloadWrite for AdmittedOwnerWrite<'a, D> {
    fn scope(&self) -> &MutationScope {
        self.identity().scope()
    }

    fn open_table<K, V>(
        &self,
        definition: TableDefinition<'static, K, V>,
    ) -> Result<redb::Table<'_, K, V>, String>
    where
        K: redb::Key + 'static,
        V: redb::Value + 'static,
    {
        self.open_table(definition)
    }

    fn open_scoped_table<K, V>(
        &self,
        definition: TableDefinition<'static, K, V>,
    ) -> Result<ScopedOwnerTableMut<'_, K, V>, String>
    where
        K: redb::Key + 'static,
        for<'k> K::SelfType<'k>: eg_storage::OwnerRowScope,
        V: redb::Value + 'static,
    {
        self.open_scoped_table(definition)
    }
}

impl<'cap, 'store, D: OwnerDomain> OwnerPayloadWrite for GraftOwnerWrite<'cap, 'store, D> {
    fn scope(&self) -> &MutationScope {
        self.capability.scope().scope()
    }

    fn open_table<K, V>(
        &self,
        definition: TableDefinition<'static, K, V>,
    ) -> Result<redb::Table<'_, K, V>, String>
    where
        K: redb::Key + 'static,
        V: redb::Value + 'static,
    {
        self.open_table(definition)
    }

    fn open_scoped_table<K, V>(
        &self,
        definition: TableDefinition<'static, K, V>,
    ) -> Result<ScopedOwnerTableMut<'_, K, V>, String>
    where
        K: redb::Key + 'static,
        for<'k> K::SelfType<'k>: eg_storage::OwnerRowScope,
        V: redb::Value + 'static,
    {
        self.open_scoped_table(definition)
    }
}

impl<'cap, 'store, D: OwnerDomain> GraftOwnerWrite<'cap, 'store, D> {
    pub fn open_table<K, V>(
        &self,
        definition: TableDefinition<'static, K, V>,
    ) -> Result<redb::Table<'_, K, V>, String>
    where
        K: redb::Key + 'static,
        V: redb::Value + 'static,
    {
        self.capability.open_owner_write(definition)
    }

    pub fn open_scoped_table<K, V>(
        &self,
        definition: TableDefinition<'static, K, V>,
    ) -> Result<ScopedOwnerTableMut<'_, K, V>, String>
    where
        K: redb::Key + 'static,
        for<'k> K::SelfType<'k>: eg_storage::OwnerRowScope,
        V: redb::Value + 'static,
    {
        self.capability.scoped_owner_table_mut(definition)
    }
}

/// Domain-owned payload transfer performed under an authenticated graft
/// reservation.  The transfer shares the destination write transaction with
/// the ledger graft, so owner rows and the ledger copy cannot acknowledge in
/// different destination states.
pub trait OwnerPayloadTransfer<S: OwnerDomain, D: OwnerDomain> {
    fn transfer_owner_payload(
        &mut self,
        source: &ScopedRead<'_, S>,
        destination: &GraftOwnerWrite<'_, '_, D>,
        identity: &MutationScopeIdentity,
    ) -> Result<u64, String>;
}

mod reservation;
use reservation::{
    cleanup_losing_reservation_in, require_empty_destination, require_reservation_baseline,
    reservation_in, reserve_destination, scope_domain, scope_expectation,
    validate_destination_phase_b, validate_reservation_state, with_losing_reservation_cleanup,
};
mod copy;
#[cfg(test)]
pub(crate) use copy::grafted_table_names;
use copy::{copy_all, restore_fence, retire_source};
mod marker;

/// The reserved batch-id prefix a graft marker carries.
///
/// It is also the idempotency key, so phase A is replay-idempotent, and it is
/// how phase B recognises its own completed copy in a destination file.
const GRAFT_MARKER: &str = "kernel.graft";

/// The destination reservation is a separate kernel-owned maintenance receipt.
/// It fences the empty target before phase A2 can fence the source.
const GRAFT_RESERVATION: &str = "kernel.graft.reservation";

/// The fence a scope under graft carries.
///
/// `commit::reject_stale_fence` refuses any batch whose `(placement_epoch,
/// fencing_token)` is older than the current fence, so a scope fenced at the
/// maximum admits nothing further. This is the existing admission gate, used
/// for exactly what it is for: the scope's route has moved.
pub(crate) const GRAFT_FENCE: u64 = u64::MAX;

/// One scope marked for a graft to one destination.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraftIntent {
    /// The scope being moved. Identical at both ends: `ledger_scope_key` carries
    /// no store incarnation, so every ledger row is key-stable across files.
    pub identity: MutationScopeIdentity,
    /// The source file's owner-authority digest. The destination marker is
    /// keyed by destination, so this extra identity prevents a retry using a
    /// different source file with the same logical scope from accepting the
    /// wrong destination copy as its own.
    pub source: String,
    /// The destination file's owner-authority digest, in hex.
    pub destination: String,
    /// The scope's authoritative version at the moment it was fenced.
    pub version: u64,
    /// The route fence in force before the graft, restored at the destination.
    pub restore_epoch: u64,
    pub restore_token: u64,
}

impl GraftIntent {
    fn batch_id(destination: &str) -> String {
        format!("{GRAFT_MARKER}/{destination}")
    }

    fn encode(&self) -> String {
        format!(
            "{GRAFT_MARKER}/{}/{}/v{}/{}/{}",
            self.source, self.destination, self.version, self.restore_epoch, self.restore_token
        )
    }

    fn decode(identity: &MutationScopeIdentity, query: &str) -> Result<Self, String> {
        let mut parts = query.split('/');
        let marker = parts.next().unwrap_or_default();
        let source = parts.next().unwrap_or_default();
        let destination = parts.next().unwrap_or_default();
        let version = parts.next().unwrap_or_default();
        let restore_epoch = parts.next().unwrap_or_default();
        let restore_token = parts.next().unwrap_or_default();
        if marker != GRAFT_MARKER
            || !valid_digest_text(source)
            || !valid_digest_text(destination)
            || parts.next().is_some()
        {
            return Err("graft marker is not a graft intent".to_string());
        }
        let number = |text: &str, label: &str| -> Result<u64, String> {
            text.parse::<u64>()
                .map_err(|_| format!("graft marker has an unreadable {label}"))
        };
        Ok(Self {
            identity: identity.clone(),
            source: source.to_string(),
            destination: destination.to_string(),
            version: number(version.trim_start_matches('v'), "version")?,
            restore_epoch: number(restore_epoch, "fence epoch")?,
            restore_token: number(restore_token, "fence token")?,
        })
    }
}

fn reservation_batch_id(source: &str, destination: &str) -> String {
    format!("{GRAFT_MARKER}/reservation/{source}/{destination}")
}

fn reservation_query(source: &str, destination: &str) -> String {
    reservation_batch_id(source, destination)
}

fn valid_digest_text(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

/// The source half of a graft: one bound scope on another kernel-owned file of
/// the same layout, which the graft reads verbatim and then retires.
///
/// It carries the source's storage kernel rather than a snapshot, because the
/// graft must open its own read AFTER the source has been fenced. A
/// caller-supplied snapshot is a snapshot of an unfenced scope, and everything
/// committed after it would be destroyed by the retirement.
pub struct GraftSource<'a, S: OwnerDomain> {
    kernel: &'a MutationKernel,
    storage: &'a StorageKernel,
    owner: &'a OwnedStoreHandle<S>,
    payload: Option<&'a dyn OwnerPayloadRetirement<S>>,
}

impl<'a, S: OwnerDomain> GraftSource<'a, S> {
    /// Name the source scope: its storage kernel, its mutation kernel, and its
    /// bound handle.
    pub fn new(
        kernel: &'a MutationKernel,
        storage: &'a StorageKernel,
        owner: &'a OwnedStoreHandle<S>,
    ) -> Self {
        Self {
            kernel,
            storage,
            owner,
            payload: None,
        }
    }

    /// Retire the source layout's own owner rows with its ledger authority.
    ///
    /// Required for any layout that owns tables, for exactly the reason
    /// [`MutationKernel::purge_scope_with`] requires it: retiring the authority
    /// while leaving the payload behind hands the moved generation's rows to
    /// the next binding of the same logical name.
    pub fn with_owner_payload(mut self, retirement: &'a dyn OwnerPayloadRetirement<S>) -> Self {
        self.payload = Some(retirement);
        self
    }

    pub fn scope(&self) -> &MutationScopeIdentity {
        self.owner.identity()
    }
}

/// What one completed graft moved.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraftedScope {
    /// The exact scope identity that moved, unchanged by the move.
    pub identity: MutationScopeIdentity,
    /// Ledger rows copied verbatim. Zero when phase B was already durable and
    /// this call resumed straight to the source retirement.
    pub rows: u64,
    /// The authoritative marker-inclusive version the destination now serves --
    /// the source's version after Phase A, not a fresh re-admission.
    pub version: u64,
    /// Whether this call resumed a graft whose copy was already durable.
    pub resumed: bool,
}

/// The destination half of a graft: the file the scope is moving into.
///
/// It carries the storage kernel as well as the bound handle because the graft
/// marker names the destination by its owner-authority digest -- the file's own
/// identity, which only the storage kernel can answer for.
pub struct GraftDestination<'a, D: OwnerDomain> {
    mutation: &'a MutationKernel,
    storage: &'a StorageKernel,
    owner: &'a OwnedStoreHandle<D>,
    /// Authenticated owner-payload retirement for proof-gated loser cleanup.
    ///
    /// A destination layout with owner tables must supply this capability before
    /// a losing reservation can be cleared. Keeping it optional preserves the
    /// safe default for generic layouts: an owner-bearing destination remains
    /// fenced until its recovery handle can be consumed by the real owner.
    payload: Option<&'a dyn OwnerPayloadRetirement<D>>,
}

impl<'a, D: OwnerDomain> GraftDestination<'a, D> {
    /// Name a destination with the storage and its existing mutation kernel.
    ///
    /// Phase A1 must reserve the destination through that kernel before the
    /// source is fenced; creating a second authority for this purpose would
    /// violate the one-writer contract.
    pub fn new(
        mutation: &'a MutationKernel,
        storage: &'a StorageKernel,
        owner: &'a OwnedStoreHandle<D>,
    ) -> Self {
        Self {
            mutation,
            storage,
            owner,
            payload: None,
        }
    }

    /// Attach the existing owner-authority retirement capability used to purge
    /// staged owner rows when an authenticated different-target source marker
    /// proves this reservation lost the race. The purge and reservation
    /// cancellation remain one destination transaction.
    pub fn with_owner_payload(mut self, retirement: &'a dyn OwnerPayloadRetirement<D>) -> Self {
        self.payload = Some(retirement);
        self
    }

    pub(super) fn owner_payload(&self) -> Option<&'a dyn OwnerPayloadRetirement<D>> {
        self.payload
    }

    pub fn scope(&self) -> &MutationScopeIdentity {
        self.owner.identity()
    }

    /// The destination file's owner-authority digest, in hex.
    fn digest(&self) -> Result<String, String> {
        let storage = self.storage.owner_authority_digest()?;
        let mutation = self.mutation.owner_authority_digest()?;
        if mutation != storage {
            return Err(
                "graft destination mutation and storage kernels use different authorities"
                    .to_string(),
            );
        }
        Ok(hex::encode(storage))
    }
}

/// Phase A: fence the source scope and record where it is going.
///
/// One admitted maintenance batch, whose version is resolved inside the write
/// transaction that commits it, so nothing can move underneath it. Its fence is
/// `(u64::MAX, u64::MAX)`, so every ordinary admission afterwards fails closed
/// with `STALE_FENCE`. Its batch id is its idempotency key, so running it twice
/// replays the first marker rather than fencing again.
pub(crate) fn graft_begin<S: OwnerDomain, D: OwnerDomain>(
    kernel: &MutationKernel,
    source: &OwnedStoreHandle<S>,
    destination: &GraftDestination<'_, D>,
) -> Result<GraftIntent, String> {
    if S::LAYOUT != D::LAYOUT {
        return Err("graft may not move a scope between two owner layouts".to_string());
    }
    if source.identity() != destination.scope() {
        return Err("graft source and destination do not name the same serving scope".to_string());
    }
    let target = destination.digest()?;
    let source_digest = hex::encode(kernel.owner_authority_digest()?);
    if source_digest == target {
        return Err("graft source and destination use the same authority".to_string());
    }
    let identity = source.identity().clone();
    // A repeat after phase A2 (or after a destination copy committed) must not
    // try to reserve a target that is already carrying this graft's durable
    // marker.  Read the marker before phase A1 and require its source proof to
    // match the authority this caller supplied.
    if let Some(existing) = read_intent_optional(kernel, source, &identity, &target)? {
        if existing.source != source_digest || existing.destination != target {
            return Err("graft marker belongs to a different authority".to_string());
        }
        return Ok(existing);
    }
    if let Err(error) = reserve_destination(destination, &identity, &source_digest, &target) {
        return Err(with_losing_reservation_cleanup(
            error,
            kernel,
            source,
            destination,
            &identity,
            &source_digest,
            &target,
        ));
    }
    let write = match kernel.open_write(source) {
        Ok(write) => write,
        Err(error) => {
            return Err(with_losing_reservation_cleanup(
                error,
                kernel,
                source,
                destination,
                &identity,
                &source_digest,
                &target,
            ));
        }
    };
    match mark_source(kernel, &write, source, &identity, &source_digest, &target) {
        Ok(Marked::Fresh(batch, source_version)) => {
            if let Err(error) = kernel.commit(write, &batch) {
                return Err(with_losing_reservation_cleanup(
                    error,
                    kernel,
                    source,
                    destination,
                    &identity,
                    &source_digest,
                    &target,
                ));
            }
            let mut intent = intent_of(&identity, &batch)?;
            // The marker is itself an admitted mutation, so it advanced the
            // scope by one; that is the version the destination must serve.
            intent.version = source_version.saturating_add(1);
            Ok(intent)
        }
        Ok(Marked::Already(intent)) => {
            write.abort()?;
            Ok(intent)
        }
        Err(error) => {
            // Keep the source writer open while proving that a different,
            // already-committed marker won. This serializes the proof against
            // a same-target marker commit; cleanup after abort would race it.
            let error = cleanup_losing_reservation_in(
                error,
                &write,
                destination,
                &identity,
                &source_digest,
                &target,
            );
            write.abort()?;
            Err(error)
        }
    }
}

/// Stop after the destination reservation for restart tests.
#[cfg(test)]
pub(crate) fn graft_reserve_for_test<S: OwnerDomain, D: OwnerDomain>(
    kernel: &MutationKernel,
    source: &OwnedStoreHandle<S>,
    destination: &GraftDestination<'_, D>,
) -> Result<(), String> {
    if S::LAYOUT != D::LAYOUT || source.identity() != destination.scope() {
        return Err("graft test reservation requires one scope and owner layout".to_string());
    }
    let target = destination.digest()?;
    let source_digest = hex::encode(kernel.owner_authority_digest()?);
    if source_digest == target {
        return Err("graft source and destination use the same authority".to_string());
    }
    reserve_destination(destination, source.identity(), &source_digest, &target)
}

/// Phase A1 for a storage mover: reserve and fence the empty destination before
/// the source is fenced.  The reservation is intentionally separate from
/// [`graft_begin`]: a bulk owner-payload copy may take place while the source
/// continues serving reads and writes, but no unrelated destination writer may
/// enter after this point.
pub(crate) fn graft_reserve_destination<S: OwnerDomain, D: OwnerDomain>(
    kernel: &MutationKernel,
    source: &OwnedStoreHandle<S>,
    destination: &GraftDestination<'_, D>,
) -> Result<(), String> {
    if S::LAYOUT != D::LAYOUT {
        return Err("graft may not move a scope between two owner layouts".to_string());
    }
    if source.identity() != destination.scope() {
        return Err("graft source and destination do not name the same serving scope".to_string());
    }
    let target = destination.digest()?;
    let source_digest = hex::encode(kernel.owner_authority_digest()?);
    if source_digest == target {
        return Err("graft source and destination use the same authority".to_string());
    }
    reserve_destination(destination, source.identity(), &source_digest, &target)
}

/// Stage owner rows into a destination that carries an exact graft
/// reservation.  This is the owner-payload counterpart to a normal admitted
/// write: it can open only layout-owned tables, and the reservation proof and
/// payload are committed in one destination transaction.  No mutation ledger
/// row is created, so Phase B can still require the reservation baseline before
/// copying the source ledger verbatim.
pub(crate) fn graft_stage_destination_payload<D: OwnerDomain, F>(
    authority: &MutationOwnerAuthority,
    destination: &GraftDestination<'_, D>,
    identity: &MutationScopeIdentity,
    source_digest: &str,
    stage: F,
) -> Result<(), String>
where
    F: for<'cap, 'store> FnOnce(&GraftOwnerWrite<'cap, 'store, D>) -> Result<(), String>,
{
    let target = destination.digest()?;
    if !valid_digest_text(source_digest) {
        return Err("graft source authority digest is malformed".to_string());
    }
    let write = AdmittedMutation::open(authority, destination.owner)?;
    write.verify_scope(identity)?;
    let reservation = match reservation_in(&write, identity, source_digest, &target) {
        Ok(Some(record)) => record,
        Ok(None) => {
            write.abort()?;
            return Err("graft destination has no exact durable reservation".to_string());
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
    let owner_write = GraftOwnerWrite {
        capability: write.capability(),
    };
    if let Err(error) = stage(&owner_write) {
        write.abort()?;
        return Err(error);
    }
    write.commit()
}

/// Stage payload for the one reservation already present at `destination`.
/// Online import commands intentionally carry only the destination graph and
/// raw rows; the source binding is authenticated by the durable reservation
/// itself rather than by a caller-supplied digest that could be omitted.
pub(crate) fn graft_stage_reserved_payload<D: OwnerDomain, F>(
    authority: &MutationOwnerAuthority,
    destination: &GraftDestination<'_, D>,
    identity: &MutationScopeIdentity,
    stage: F,
) -> Result<(), String>
where
    F: for<'cap, 'store> FnOnce(&GraftOwnerWrite<'cap, 'store, D>) -> Result<(), String>,
{
    let target = destination.digest()?;
    let write = AdmittedMutation::open(authority, destination.owner)?;
    write.verify_scope(identity)?;
    let reservation = match reservation_for_target_in(&write, identity, &target) {
        Ok(Some(reservation)) => reservation,
        Ok(None) => {
            write.abort()?;
            return Err("graft destination has no exact durable reservation".to_string());
        }
        Err(error) => {
            write.abort()?;
            return Err(error);
        }
    };
    if let Err(error) = require_reservation_baseline(&write, identity, &reservation.1) {
        write.abort()?;
        return Err(error);
    }
    let owner_write = GraftOwnerWrite {
        capability: write.capability(),
    };
    if let Err(error) = stage(&owner_write) {
        write.abort()?;
        return Err(error);
    }
    write.commit()
}

/// Find and authenticate the unique destination reservation in a write.  The
/// source digest is part of the reservation key and is returned only after the
/// full receipt/class/mapping proof succeeds.
fn reservation_for_target_in<D: OwnerDomain>(
    write: &AdmittedMutation<'_, D>,
    identity: &MutationScopeIdentity,
    target: &str,
) -> Result<Option<(String, eg_types::MutationBatchRecord)>, String> {
    let scope = ledger_scope_key(identity);
    let ids = {
        let table = write.scoped_table(BATCHES)?;
        let mut ids = Vec::new();
        for row in table.range_inclusive(
            (scope.as_str(), "kernel.graft/reservation/"),
            (scope.as_str(), MAX_BATCH_ID_SENTINEL),
        )? {
            let (key, _) = row.map_err(|error| error.to_string())?;
            let batch_id = key.value().1;
            let Some(suffix) = batch_id.strip_prefix("kernel.graft/reservation/") else {
                continue;
            };
            let mut parts = suffix.split('/');
            let Some(source) = parts.next() else { continue };
            let Some(candidate_target) = parts.next() else {
                continue;
            };
            if parts.next().is_some() || candidate_target != target || !valid_digest_text(source) {
                continue;
            }
            ids.push((source.to_string(), batch_id.to_string()));
        }
        ids
    };
    let mut found = None;
    for (source, _batch_id) in ids {
        let Some(record) = reservation_in(write, identity, &source, target)? else {
            return Err("graft destination reservation key has no receipt".to_string());
        };
        if found.replace((source, record)).is_some() {
            return Err("graft destination has multiple reservations".to_string());
        }
    }
    Ok(found)
}

/// Whether phase A wrote a new marker or found one already in force.
enum Marked {
    /// Boxed only to keep this call-local return type small next to the
    /// `Already` arm's much smaller `GraftIntent`; `Marked` never crosses a
    /// durable boundary, so the box has no wire effect.
    Fresh(Box<MutationBatch>, u64),
    Already(GraftIntent),
}

/// Read the scope's version and fence in-lock, then admit the marker.
fn mark_source<S: OwnerDomain>(
    kernel: &MutationKernel,
    write: &AdmittedMutation<'_, S>,
    source: &OwnedStoreHandle<S>,
    identity: &MutationScopeIdentity,
    source_digest: &str,
    target: &str,
) -> Result<Marked, String> {
    write.verify_scope(identity)?;
    let version = crate::ledger::bound_scope_version(write, identity)?;
    let restore = current_fence(write, identity)?;
    let batch = marker_batch(source, version, source_digest, target, restore.0, restore.1)?;
    match write.begin_graft(&batch)? {
        Begin::Replay(record) => {
            let intent = Marker::from_record(identity, &record)?;
            require_maintenance_mapping(write, identity, &record, "graft marker")?;
            Ok(Marked::Already(intent))
        }
        Begin::Apply { source_version } => {
            if !owner_table_names(S::LAYOUT).is_empty() {
                write.owner_rows(source, &batch)?.finish_owner()?;
            }
            kernel.finish(write, &batch, None, batch.created_at_ms, source_version)?;
            Ok(Marked::Fresh(
                Box::new(batch),
                source_version.unwrap_or_default(),
            ))
        }
    }
}

/// Decoding a marker out of a receipt, in one place.
struct Marker;

impl Marker {
    fn from_record(
        identity: &MutationScopeIdentity,
        record: &eg_types::MutationBatchRecord,
    ) -> Result<GraftIntent, String> {
        let intent = intent_of(identity, &record.batch)?;
        let committed = record
            .committed_version
            .target()
            .ok_or_else(|| "graft marker has no committed version".to_string())?;
        let Some(operation) = record.batch.operations.first() else {
            return Err("graft marker batch lost its operation".to_string());
        };
        let Method::ApplyMutation { event_type, query } = &operation.method else {
            return Err("graft marker batch lost its operation".to_string());
        };
        if !marker::receipt_identity_matches(identity, record)
            || !marker::batch_keys_match(identity, record, &intent)?
            || !marker::operation_shape_matches(identity, operation, record)
            || !marker::marker_content_matches(&intent, committed, event_type, query, record)
        {
            return Err("graft marker receipt is not an exact kernel marker".to_string());
        }
        let mut intent = intent;
        intent.version = committed;
        Ok(intent)
    }
}

/// The graft intent one marker batch carries.
fn intent_of(
    identity: &MutationScopeIdentity,
    batch: &MutationBatch,
) -> Result<GraftIntent, String> {
    let operation = batch
        .operations
        .first()
        .ok_or_else(|| "graft marker batch lost its intent".to_string())?;
    let Method::ApplyMutation { query, .. } = &operation.method else {
        return Err("graft marker batch lost its intent".to_string());
    };
    GraftIntent::decode(identity, query)
}

/// The route fence currently in force on this scope, or `(0, 0)` if none.
fn current_fence<S: OwnerDomain>(
    write: &AdmittedMutation<'_, S>,
    identity: &MutationScopeIdentity,
) -> Result<(u64, u64), String> {
    let key = ledger_scope_key(identity);
    let table = write.scoped_table(FENCES)?;
    let fence = table
        .get(key.as_str())?
        .map(|value| decode_ledger_record::<ScopeFence>(value.value()))
        .transpose()?;
    Ok(fence.map_or((0, 0), |fence| (fence.placement_epoch, fence.fencing_token)))
}

/// The marker batch for one graft: a real, ledgered maintenance mutation that
/// fences the scope and records where it is going.
fn marker_batch<S: OwnerDomain>(
    owner: &OwnedStoreHandle<S>,
    version: u64,
    source: &str,
    destination: &str,
    restore_epoch: u64,
    restore_token: u64,
) -> Result<MutationBatch, String> {
    let batch_id = GraftIntent::batch_id(destination);
    let intent = GraftIntent {
        identity: owner.identity().clone(),
        source: source.to_string(),
        destination: destination.to_string(),
        version,
        restore_epoch,
        restore_token,
    };
    let expectation = scope_expectation(owner.identity(), version);
    // The marker is a real mutation, so its operation must be valid for the
    // identity it fences.  ControlPlane is valid for graph scopes, but native
    // scopes retain their own domain (for example BlobStore); the batch
    // validator deliberately rejects a cross-domain operation.
    let marker_domain = scope_domain(owner.identity());
    let batch = MutationBatch {
        schema_version: MUTATION_BATCH_VERSION,
        batch_id: batch_id.clone(),
        envelope: MutationEnvelope::maintenance_for_scope(
            owner.identity(),
            owner.principal(),
            GRAFT_MARKER,
            &batch_id,
        )?,
        identity: owner.identity().clone(),
        placement_epoch: GRAFT_FENCE,
        version_expectation: expectation,
        fencing_token: Some(GRAFT_FENCE),
        authoritative_state: None,
        operations: vec![MutationOperation {
            ordinal: 0,
            surface: MutationSurface::Other,
            domain: marker_domain,
            method: Method::ApplyMutation {
                event_type: GRAFT_MARKER.to_string(),
                query: intent.encode(),
            },
        }],
        outbox: Vec::new(),
        created_at_ms: 0,
    };
    batch.validate()?;
    Ok(batch)
}

/// Read the graft intent a scope is already marked with.
fn read_intent<S: OwnerDomain>(
    kernel: &MutationKernel,
    owner: &OwnedStoreHandle<S>,
    identity: &MutationScopeIdentity,
    destination: &str,
) -> Result<GraftIntent, String> {
    read_intent_optional(kernel, owner, identity, destination)?
        .ok_or_else(|| "scope carries no graft marker for this destination".to_string())
}

fn read_intent_optional<S: OwnerDomain>(
    kernel: &MutationKernel,
    owner: &OwnedStoreHandle<S>,
    identity: &MutationScopeIdentity,
    destination: &str,
) -> Result<Option<GraftIntent>, String> {
    let write = kernel.open_write(owner)?;
    let found = marker_in(&write, identity, destination);
    write.abort()?;
    found
}

/// Read the unique non-reservation graft marker already committed on a source
/// scope.  This is deliberately a kernel operation rather than a consumer
/// scan: every candidate is authenticated against its receipt, maintenance
/// class and exact idempotency claim before it is returned.  A storage mover
/// uses this proof before binding a destination, so a marker for a different
/// destination or a planted batch-shaped row cannot leave a destination
/// binding behind after refusal.
pub(crate) fn graft_source_marker<S: OwnerDomain>(
    kernel: &MutationKernel,
    owner: &OwnedStoreHandle<S>,
) -> Result<Option<GraftIntent>, String> {
    let identity = owner.identity().clone();
    let write = kernel.open_write(owner)?;
    let found = source_marker_in(&write, &identity);
    write.abort()?;
    found
}

/// Prove the exact destination reservation for one source and target without
/// changing the ledger.  The mutation authority still opens the owner-scoped
/// write transaction so the reservation receipt, maintenance mapping, class,
/// baseline version and graft fence are checked against one serialized view;
/// this helper always aborts that transaction before returning.  A caller must
/// use this exact proof before touching file-wide catalog rows: the existence
/// of any `kernel.graft/reservation/` row is insufficient because a destination
/// may be staging a different source concurrently.
pub(crate) fn graft_reservation_matches<D: OwnerDomain>(
    kernel: &MutationKernel,
    destination: &GraftDestination<'_, D>,
    identity: &MutationScopeIdentity,
    source_digest: &str,
) -> Result<bool, String> {
    let target = destination.digest()?;
    if kernel.owner_authority_digest()? != destination.mutation.owner_authority_digest()? {
        return Err("graft reservation proof uses a different mutation authority".to_string());
    }
    let write = kernel.open_write(destination.owner)?;
    let result = (|| {
        if !valid_digest_text(source_digest) {
            return Err("graft source authority digest is malformed".to_string());
        }
        write.verify_scope(identity)?;
        let Some(record) = reservation_in(&write, identity, source_digest, &target)? else {
            return Ok(false);
        };
        validate_reservation_state(&write, identity, &record)?;
        require_reservation_baseline(&write, identity, &record)?;
        Ok(true)
    })();
    match (result, write.abort()) {
        (Ok(value), Ok(())) => Ok(value),
        (Err(error), Ok(())) => Err(error),
        (Ok(_), Err(error)) => Err(error),
        (Err(error), Err(abort_error)) => Err(format!(
            "{error}; graft reservation proof abort failed: {abort_error}"
        )),
    }
}

fn source_marker_in<D: OwnerDomain>(
    write: &AdmittedMutation<'_, D>,
    identity: &MutationScopeIdentity,
) -> Result<Option<GraftIntent>, String> {
    let scope = ledger_scope_key(identity);
    let destinations = {
        let table = write.scoped_table(BATCHES)?;
        let mut destinations = Vec::new();
        for row in table.range_inclusive(
            (scope.as_str(), "kernel.graft/"),
            (scope.as_str(), MAX_BATCH_ID_SENTINEL),
        )? {
            let (key, _) = row.map_err(|error| error.to_string())?;
            let batch_id = key.value().1;
            let Some(destination) = batch_id.strip_prefix("kernel.graft/") else {
                continue;
            };
            if destination.is_empty() || destination.starts_with("reservation/") {
                continue;
            }
            destinations.push(destination.to_string());
        }
        destinations
    };

    let mut found = None;
    for destination in destinations {
        let Some(intent) = marker_in(write, identity, &destination)? else {
            return Err("graft marker key has no durable receipt".to_string());
        };
        if found.replace(intent).is_some() {
            return Err("graft source carries multiple durable markers".to_string());
        }
    }
    Ok(found)
}

/// The marker receipt for this destination, read inside `write`.
fn marker_in<D: OwnerDomain>(
    write: &AdmittedMutation<'_, D>,
    identity: &MutationScopeIdentity,
    destination: &str,
) -> Result<Option<GraftIntent>, String> {
    let Some(record) =
        crate::ledger::read_record_in_write(write, identity, &GraftIntent::batch_id(destination))?
    else {
        return Ok(None);
    };
    let intent = Marker::from_record(identity, &record)?;
    require_maintenance_mapping(write, identity, &record, "graft marker")?;
    let scope = ledger_scope_key(identity);
    let class = write
        .scoped_table(CLASSES)?
        .get((scope.as_str(), record.batch.batch_id.as_str()))?
        .map(|value| decode_ledger_record::<MutationClassRow>(value.value()))
        .transpose()?
        .ok_or_else(|| "graft marker has no durable maintenance class".to_string())?;
    if class.identity != *identity
        || class.batch_id != record.batch.batch_id
        || class.class != MutationClass::Maintenance
    {
        return Err("graft marker has an invalid durable class".to_string());
    }
    if intent.destination != destination {
        return Err("graft marker destination does not match its receipt key".to_string());
    }
    Ok(Some(intent))
}

/// Require the maintenance claim to resolve this proof receipt exactly. The
/// claim is the maintenance arm's sole replay authority; a batch-shaped row or
/// a redirected claim cannot authenticate a graft marker or reservation.
pub(super) fn require_maintenance_mapping<D: OwnerDomain>(
    write: &AdmittedMutation<'_, D>,
    identity: &MutationScopeIdentity,
    record: &eg_types::MutationBatchRecord,
    proof: &str,
) -> Result<(), String> {
    let scope = ledger_scope_key(identity);
    let mapping = {
        let table = write.scoped_table(MAINTENANCE)?;
        let found = table.get((scope.as_str(), record.batch.idempotency_key()))?;
        found.map(|value| value.value().to_string())
    };
    if mapping.as_deref() != Some(record.batch.batch_id.as_str()) {
        return Err(format!("{proof} has no exact durable maintenance claim"));
    }
    Ok(())
}

/// Phases B and C: copy the fenced scope into `destination`, then retire the
/// source. Also the resume entry point after a crash in either.
pub(crate) fn graft_scope<S: OwnerDomain, D: OwnerDomain>(
    authority: &MutationOwnerAuthority,
    source: GraftSource<'_, S>,
    destination: &GraftDestination<'_, D>,
) -> Result<GraftedScope, String> {
    graft_scope_with_payload(authority, source, destination, None)
}

/// The graft entry point used by layouts that own payload tables.  The
/// optional transfer runs only after the exact destination reservation and
/// source maximum fence are proved, and before the reservation is removed and
/// the ledger rows are copied.  A failed transfer aborts the one destination
/// transaction, preserving both the reservation and the source fence for
/// retry.
pub(crate) fn graft_scope_with_payload<S: OwnerDomain, D: OwnerDomain>(
    authority: &MutationOwnerAuthority,
    source: GraftSource<'_, S>,
    destination: &GraftDestination<'_, D>,
    payload: Option<&mut dyn OwnerPayloadTransfer<S, D>>,
) -> Result<GraftedScope, String> {
    if S::LAYOUT != D::LAYOUT {
        return Err("graft may not move a scope between two owner layouts".to_string());
    }
    let identity = destination.scope().clone();
    if source.owner.identity() != &identity {
        return Err("graft source and destination do not name the same serving scope".to_string());
    }
    let target = destination.digest()?;
    let intent = match read_intent(source.kernel, source.owner, &identity, &target) {
        Ok(intent) => intent,
        Err(source_error) => {
            // Once phase C has committed, the source binding is deliberately
            // gone.  A retry must still be able to finish the already durable
            // destination copy; treating every failed source open as that
            // state would hide I/O and recovery failures, so prove the binding
            // is absent before reading the destination marker.
            match source.storage.scope_is_bound(source.owner) {
                Ok(false) => {
                    let write = AdmittedMutation::open(authority, destination.owner)?;
                    let marker = marker_in(&write, &identity, &target)?;
                    write.abort()?;
                    marker.ok_or(source_error)?
                }
                Ok(true) => return Err(source_error),
                Err(probe_error) => {
                    return Err(format!(
                        "graft could not read source marker: {source_error}; binding probe failed: {probe_error}"
                    ));
                }
            }
        }
    };
    let source_digest = hex::encode(source.storage.owner_authority_digest()?);
    if intent.source != source_digest {
        return Err("graft marker belongs to a different source authority".to_string());
    }
    let grafted = transplant_phase(
        authority,
        &source,
        destination.owner,
        &identity,
        &intent,
        &target,
        payload,
    )?;
    if let Err(error) = retire_source(&source, &identity, &intent) {
        // Phase B is already durable when retirement fails. Name that state so
        // an operator can resume with the same marker instead of treating the
        // result as an ambiguous all-or-nothing failure.
        return Err(format!("GRAFT_RETIREMENT_PENDING: {error}"));
    }
    Ok(grafted)
}

/// Recover the final acknowledgement after source retirement when the caller
/// no longer has a serializable `OwnedStoreHandle` for that source.  The
/// destination marker is the durable phase-B proof; the source storage kernel
/// supplies only its physical authority digest and a direct binding-absence
/// check.  No source bind/probe is attempted, because rebinding would create a
/// new serving authority for a generation that has already been retired.
pub(crate) fn graft_recover<D: OwnerDomain>(
    authority: &MutationOwnerAuthority,
    source_storage: &StorageKernel,
    identity: &MutationScopeIdentity,
    destination: &GraftDestination<'_, D>,
) -> Result<GraftedScope, String> {
    if source_storage.layout() != D::LAYOUT {
        return Err("graft recovery requires matching owner layouts".to_string());
    }
    if source_storage.scope_binding_exists(identity)? {
        return Err("graft recovery requires the source binding to be retired".to_string());
    }
    let target = destination.digest()?;
    let source_digest = hex::encode(source_storage.owner_authority_digest()?);
    let write = AdmittedMutation::open(authority, destination.owner)?;
    let marker = match marker_in(&write, identity, &target) {
        Ok(Some(marker)) => marker,
        Ok(None) => {
            write.abort()?;
            return Err("graft destination has no durable marker for recovery".to_string());
        }
        Err(error) => {
            write.abort()?;
            return Err(error);
        }
    };
    if marker.source != source_digest || marker.destination != target {
        write.abort()?;
        return Err("graft recovery marker belongs to a different authority".to_string());
    }
    let version = match validate_destination_phase_b(&write, identity, &marker, &target) {
        Ok(version) => version,
        Err(error) => {
            write.abort()?;
            return Err(error);
        }
    };
    write.abort()?;
    Ok(GraftedScope {
        identity: identity.clone(),
        rows: 0,
        version,
        resumed: true,
    })
}

/// Phase B, skipped when the destination already carries this graft's copy.
fn transplant_phase<S: OwnerDomain, D: OwnerDomain>(
    authority: &MutationOwnerAuthority,
    source: &GraftSource<'_, S>,
    destination: &OwnedStoreHandle<D>,
    identity: &MutationScopeIdentity,
    intent: &GraftIntent,
    target: &str,
    payload: Option<&mut dyn OwnerPayloadTransfer<S, D>>,
) -> Result<GraftedScope, String> {
    let write = AdmittedMutation::open(authority, destination)?;
    if let Some(existing) = marker_in(&write, identity, target)? {
        if existing != *intent {
            write.abort()?;
            return Err("graft destination carries a different graft marker".to_string());
        }
        // The destination marker resumes a crash between phase B and retirement.
        let version = match validate_destination_phase_b(&write, identity, intent, target) {
            Ok(version) => version,
            Err(error) => {
                write.abort()?;
                return Err(error);
            }
        };
        write.abort()?;
        return Ok(GraftedScope {
            identity: identity.clone(),
            rows: 0,
            version,
            resumed: true,
        });
    }
    let reservation = reservation_in(&write, identity, &intent.source, target)?
        .ok_or_else(|| "graft destination has no exact durable reservation".to_string())?;
    require_reservation_baseline(&write, identity, &reservation)?;
    // Open the source snapshot after its fence, never from the caller.
    let read = source.storage.read_scope(source.owner)?;
    if let Some(payload) = payload {
        let owner_write = GraftOwnerWrite {
            capability: write.capability(),
        };
        payload.transfer_owner_payload(&read, &owner_write, identity)?;
    }
    match transplant(&write, &read, identity, intent, &reservation) {
        Ok(grafted) => {
            write.commit()?;
            Ok(grafted)
        }
        Err(error) => {
            // Abort so a partial graft never lands beside the live source.
            write.abort()?;
            Err(error)
        }
    }
}

/// Stop after phase B for restart tests. Production has no phase-boundary
/// switch: this seam exists only to exercise the durable state a process loss
/// can leave between the destination commit and source retirement.
#[cfg(test)]
pub(crate) fn graft_copy_for_test<S: OwnerDomain, D: OwnerDomain>(
    source: GraftSource<'_, S>,
    destination: &GraftDestination<'_, D>,
) -> Result<GraftedScope, String> {
    if S::LAYOUT != D::LAYOUT || source.owner.identity() != destination.scope() {
        return Err("graft test phase requires one scope and owner layout".to_string());
    }
    let identity = destination.scope().clone();
    let target = destination.digest()?;
    let intent = read_intent(source.kernel, source.owner, &identity, &target)?;
    let source_digest = hex::encode(source.storage.owner_authority_digest()?);
    if intent.source != source_digest {
        return Err("graft marker belongs to a different source authority".to_string());
    }
    transplant_phase(
        destination.mutation.authority(),
        &source,
        destination.owner,
        &identity,
        &intent,
        &target,
        None,
    )
}

/// Copy every ledger table's rows for this scope, inside the destination's one
/// admitted transaction, and restore the pre-graft route fence.
fn transplant<S: OwnerDomain, D: OwnerDomain>(
    write: &AdmittedMutation<'_, D>,
    read: &ScopedRead<'_, S>,
    identity: &MutationScopeIdentity,
    intent: &GraftIntent,
    reservation: &eg_types::MutationBatchRecord,
) -> Result<GraftedScope, String> {
    write.verify_scope(identity)?;
    let scope = ledger_scope_key(identity);
    require_source_unmoved(read, intent)?;
    clear_reservation(write, identity, reservation)?;
    require_empty_destination(write, &scope)?;
    let rows = copy_all(read, write, &scope)?;
    restore_fence(write, &scope, identity, intent)?;
    let version = crate::ledger::bound_scope_version(write, identity)?;
    Ok(GraftedScope {
        identity: identity.clone(),
        rows,
        version,
        resumed: false,
    })
}

/// Remove only the reservation receipt and restore the destination's binding
/// baseline.  This happens in the same destination transaction as the source
/// copy, so an empty target can never become writable between the reservation
/// and the verbatim ledger transplant.
fn clear_reservation<D: OwnerDomain>(
    write: &AdmittedMutation<'_, D>,
    identity: &MutationScopeIdentity,
    reservation: &eg_types::MutationBatchRecord,
) -> Result<(), String> {
    validate_reservation_state(write, identity, reservation)?;
    let scope = ledger_scope_key(identity);
    let batch_id = reservation.batch.batch_id.as_str();
    write
        .scoped_table(BATCHES)?
        .remove((scope.as_str(), batch_id))?;
    write
        .scoped_table(MAINTENANCE)?
        .remove((scope.as_str(), reservation.batch.idempotency_key()))?;
    write
        .scoped_table(CLASSES)?
        .remove((scope.as_str(), batch_id))?;
    write.scoped_table(FENCES)?.remove(scope.as_str())?;
    write.scoped_table(VERSIONS)?.insert(scope.as_str(), 0)?;
    Ok(())
}

/// The source must be exactly where the marker fenced it.
///
/// The fence makes an ordinary admission impossible, so this is the check that
/// catches anything that got in anyway -- a reserved-system write, a
/// concurrently admitted second marker, or a stale intent -- before copying.
fn require_source_unmoved<S: OwnerDomain>(
    read: &ScopedRead<'_, S>,
    intent: &GraftIntent,
) -> Result<(), String> {
    let version = crate::read::version(read)?;
    if version != intent.version {
        return Err(format!(
            "graft source moved from version {} to {} after it was fenced",
            intent.version, version
        ));
    }
    let scope = ledger_scope_key(&intent.identity);
    let fence = {
        let table = read.scoped_table(FENCES)?;
        let found = table.get(scope.as_str())?;
        found
            .map(|value| decode_ledger_record::<ScopeFence>(value.value()))
            .transpose()?
    }
    .ok_or_else(|| "graft source lost its durable maximum fence".to_string())?;
    if fence.identity != intent.identity
        || fence.placement_epoch != GRAFT_FENCE
        || fence.fencing_token != GRAFT_FENCE
    {
        return Err("graft source does not retain its exact durable maximum fence".to_string());
    }
    Ok(())
}
