//! `MutationKernel` -- the sole mutation authority.
//!
//! It depends inward on [`eg_storage::StorageKernel`] and holds the single
//! move-once [`MutationOwnerAuthority`] that kernel issued. It alone admits,
//! orders, fences, commits, replays and emits durable mutation and outbox rows,
//! and it does so only through storage-issued capabilities. `eg-storage` never
//! imports this crate, and neither kernel imports a domain consumer.

use crate::admitted::AdmittedMutation;
use crate::graft::{GraftDestination, GraftIntent, GraftSource, GraftedScope};
use crate::group::{expected_current_version, AdmittedGroup, CurrentIntent, ScopedIntent};
use crate::outbox::{OutboxBackfillOutcome, OutboxClaimBudget, OutboxClaimOutcome};
use crate::replay::{
    finalize_replay_receipt_in, record_replay_in, resolve_nonce_in, resolve_replay_in,
    ReplayResolution,
};
use crate::tables::ledger_table_names;
use crate::{commit, saga, Begin, SagaBegin};
use eg_storage::{
    declared_table_names, MutationOwnerAuthority, OwnedStoreHandle, OwnerDomain, OwnerLayout,
    OwnerPayloadRetirement, StorageKernel,
};
use eg_types::authority::{NonceReplayKey, OperationReplayIdentity};
use eg_types::mutation::MutationReceipt;
use eg_types::{
    MutationBatch, MutationBatchRecord, MutationOutboxLease, MutationProjectionCursor,
    MutationScopeIdentity,
};
use std::collections::BTreeSet;

/// The sole mutation owner over one physical owner file.
pub struct MutationKernel {
    // `pub(crate)` rather than a `pub(crate)` accessor method: every inherent
    // method this kernel exposes from another file in this crate's own
    // module tree (`outbox::operator`'s `impl MutationKernel` included) reads
    // this field directly, and kernel.rs is already near `kiss`'s
    // functions-per-file cap, so a same-crate field is one file-aggregate
    // finding cheaper than an equivalent accessor without changing what is
    // reachable from outside the crate (still nothing -- `pub(crate)` stops
    // at this crate's boundary either way).
    pub(crate) authority: MutationOwnerAuthority,
}

impl MutationKernel {
    /// Take ownership of the one physical write authority. The token is not
    /// `Clone` and has no public constructor, so exactly one mutation kernel
    /// can exist per storage kernel.
    pub fn new(authority: MutationOwnerAuthority) -> Self {
        Self { authority }
    }

    /// The move-once write authority itself, for test code that must write a
    /// deliberately malformed row.
    #[cfg(test)]
    pub(crate) fn authority(&self) -> &MutationOwnerAuthority {
        &self.authority
    }

    /// The physical authority digest used to bind a graft marker to its
    /// source file. It is read-only evidence; the move-once authority itself
    /// remains private to this kernel.
    pub fn owner_authority_digest(&self) -> Result<[u8; 32], String> {
        self.authority.owner_authority_digest()
    }

    /// Prove this owner file is ledger-ready before any batch is admitted.
    ///
    /// The storage kernel creates the whole declared table census atomically
    /// when the file is created, so this cannot partially create tables. What
    /// it does is fail closed when the file's declared ledger census differs
    /// from the tables this crate reads and writes -- the one seam where the
    /// storage/ledger table-ownership split could silently diverge -- and open
    /// every one of them once under a real write capability.
    pub fn bootstrap_ledger<D: OwnerDomain>(
        &self,
        owner: &OwnedStoreHandle<D>,
    ) -> Result<(), String> {
        let declared = declared_table_names(OwnerLayout::LedgerOnly)
            .into_iter()
            .collect::<BTreeSet<_>>();
        let mine = ledger_table_names().into_iter().collect::<BTreeSet<_>>();
        if !mine.is_subset(&declared) {
            return Err("mutation ledger census is not declared by the storage kernel".to_string());
        }
        let write = AdmittedMutation::open(&self.authority, owner)?;
        commit::open_ledger_tables(&write)?;
        write.commit()
    }

    /// Open the one write transaction for `owner` and admit `batch` into it.
    ///
    /// Returns [`Begin::Replay`] when the batch's idempotency key already names
    /// a terminally committed receipt; the caller must then abort the returned
    /// write rather than reapplying.
    pub fn admit<'a, D: OwnerDomain>(
        &'a self,
        owner: &OwnedStoreHandle<D>,
        batch: &MutationBatch,
    ) -> Result<(AdmittedMutation<'a, D>, Begin), String> {
        let write = AdmittedMutation::open(&self.authority, owner)?;
        match commit::begin(&write, batch) {
            Ok(begun) => Ok((write, begun)),
            Err(error) => {
                write.abort()?;
                Err(error)
            }
        }
    }

    /// Open the one write for `owner`, resolve the scope's authoritative version
    /// INSIDE that exclusive transaction, and admit the batch `build` returns for
    /// it.
    ///
    /// The two-step alternative -- read the version from a fresh snapshot, build
    /// a batch carrying it as `VersionExpectation::Native`, then admit -- has a
    /// window between the read and the write lock in which any other writer may
    /// commit, and [`commit::begin`] then fails the batch closed with
    /// `STALE_VERSION`. That is correct for a caller-supplied expectation, which
    /// is a real OCC claim about state the caller observed. It is wrong for a
    /// write whose expectation is not a claim at all but merely "whatever the
    /// scope is at". redb serializes writers, so a version read while the write
    /// transaction is held cannot move underneath the batch built from it: this
    /// entry point closes the window rather than retrying around it.
    ///
    /// `build` receives that authoritative version and must return a batch whose
    /// `version_expectation` is the one its scope owes -- `Native(version)` for
    /// a native scope, `Graph(version)` for a graph scope; anything else is
    /// refused, because it would reintroduce the claim this method exists to
    /// remove. [`Self::admit_group_current`] is the same rule over N scopes in
    /// one transaction.
    pub fn admit_current<'a, D: OwnerDomain, F>(
        &'a self,
        owner: &OwnedStoreHandle<D>,
        build: F,
    ) -> Result<(AdmittedMutation<'a, D>, MutationBatch, Begin), String>
    where
        F: FnOnce(u64) -> Result<MutationBatch, String>,
    {
        let write = AdmittedMutation::open(&self.authority, owner)?;
        let batch = match current_batch(&write, owner, build) {
            Ok(batch) => batch,
            Err(error) => {
                write.abort()?;
                return Err(error);
            }
        };
        match commit::begin(&write, &batch) {
            Ok(begun) => Ok((write, batch, begun)),
            Err(error) => {
                write.abort()?;
                Err(error)
            }
        }
    }

    /// Admit a **scope group** whose members' batches are BUILT INSIDE the one
    /// write transaction, from each scope's authoritative version resolved
    /// there (RF-RULING-008 + the `admit_current` rule).
    ///
    /// [`Self::admit_group`] takes already-built batches, so every member's
    /// `VersionExpectation` was necessarily read before the write lock: any
    /// other writer may commit in that window and [`commit::begin`] then fails
    /// the member closed with `STALE_VERSION`. A graph shard's drain builds N
    /// such batches at once and has no OCC claim to make -- its expectation is
    /// "whatever this scope is at" -- so it needs the group form of the fix
    /// `admit_current` already gives a sole writer. redb serializes writers, so
    /// a version read while this transaction is held cannot move underneath the
    /// batch built from it: this entry point closes the window rather than
    /// retrying around it.
    ///
    /// Returns the group plus the batches it built, control member first and
    /// members in admission order, because the caller needs them for
    /// [`Self::finish`] and [`Self::commit_group`] and did not build them
    /// itself.
    ///
    /// Everything after the build is exactly [`Self::admit_group`]: the same
    /// [`commit::begin`] per member, the same replayed-member marking, the same
    /// all-or-nothing discard, the same single commit. [`Self::admit_group`]
    /// remains the path for a caller that legitimately carries a real OCC claim.
    pub fn admit_group_current<'a, 'i, D: OwnerDomain>(
        &'a self,
        control: CurrentIntent<'i, D>,
        members: impl IntoIterator<Item = CurrentIntent<'i, D>>,
    ) -> Result<(AdmittedGroup<'a, D>, Vec<MutationBatch>), String> {
        let intents: Vec<CurrentIntent<'i, D>> = std::iter::once(control).chain(members).collect();
        let owners: Vec<&OwnedStoreHandle<D>> = intents.iter().map(|intent| intent.owner).collect();
        let admitted = self.group_members(&owners)?;
        let mut begins = Vec::with_capacity(intents.len());
        let mut batches: Vec<MutationBatch> = Vec::with_capacity(intents.len());
        for (index, intent) in intents.into_iter().enumerate() {
            let write = &admitted[index];
            let batch = match current_batch(write, intent.owner, intent.build) {
                Ok(batch) => batch,
                Err(error) => {
                    let _ = AdmittedGroup::new(admitted, begins).end(false);
                    return Err(error);
                }
            };
            match admit_group_member(write, &batch) {
                Ok(begun) => begins.push(begun),
                Err(error) => {
                    let _ = AdmittedGroup::new(admitted, begins).end(false);
                    return Err(error);
                }
            }
            batches.push(batch);
        }
        Ok((AdmittedGroup::new(admitted, begins), batches))
    }

    /// Admit a group whose control member is built from its in-lock version
    /// while its scoped members retain the caller's original batches.
    ///
    /// The mixed form is for request paths whose replay identity must be
    /// resolved before optimistic concurrency. A caller retry may carry the
    /// version it observed on its first attempt; rebuilding that batch at the
    /// current version would change its replay identity and make nonce/replay
    /// errors unreachable. The control member has no caller OCC claim, so it
    /// still uses [`CurrentIntent`]; every scoped member goes through the
    /// ordinary [`crate::commit::begin`] ordering inside this same write.
    pub fn admit_group_current_control<'a, 'i, D: OwnerDomain>(
        &'a self,
        control: CurrentIntent<'i, D>,
        members: impl IntoIterator<Item = ScopedIntent<'i, D>>,
    ) -> Result<(AdmittedGroup<'a, D>, Vec<MutationBatch>), String> {
        let member_intents: Vec<ScopedIntent<'i, D>> = members.into_iter().collect();
        let control_owner = control.owner;
        let control_build = control.build;
        let owners: Vec<&OwnedStoreHandle<D>> = std::iter::once(control_owner)
            .chain(member_intents.iter().map(|intent| intent.owner))
            .collect();
        let admitted = self.group_members(&owners)?;
        let control_batch = match current_batch(&admitted[0], control_owner, control_build) {
            Ok(batch) => batch,
            Err(error) => {
                let _ = AdmittedGroup::new(admitted, Vec::new()).end(false);
                return Err(error);
            }
        };
        let mut begins = Vec::with_capacity(admitted.len());
        let control_begin = match admit_group_member(&admitted[0], &control_batch) {
            Ok(begun) => begun,
            Err(error) => {
                let _ = AdmittedGroup::new(admitted, begins).end(false);
                return Err(error);
            }
        };
        begins.push(control_begin);
        let mut batches = vec![control_batch];
        for (write, intent) in admitted.iter().skip(1).zip(member_intents.iter()) {
            match admit_group_member(write, intent.batch) {
                Ok(begun) => {
                    begins.push(begun);
                    batches.push(intent.batch.clone());
                }
                Err(error) => {
                    let _ = AdmittedGroup::new(admitted, begins).end(false);
                    return Err(error);
                }
            }
        }
        Ok((AdmittedGroup::new(admitted, begins), batches))
    }

    /// Mint one write capability per member over the ONE shared transaction.
    fn group_members<'a, D: OwnerDomain>(
        &'a self,
        owners: &[&OwnedStoreHandle<D>],
    ) -> Result<Vec<AdmittedMutation<'a, D>>, String> {
        let (control, scoped) = owners
            .split_first()
            .ok_or_else(|| "a scope group needs its control member".to_string())?;
        Ok(self
            .authority
            .group_write_capabilities(control, scoped)?
            .into_iter()
            .map(AdmittedMutation::from_group_member)
            .collect())
    }

    /// Admit a **scope group**: N scoped batches plus the store's own control
    /// scope, in ONE physical write transaction (RF-RULING-008).
    ///
    /// `control` is the store's file-wide member — for a graph shard, the scope
    /// that owns the Raft log and meta rows, the cross-shard 2PC records and
    /// the series and matview key spaces, none of which carry a graph
    /// component. `members` are the scoped batches that ride the same fsync.
    ///
    /// Every member is admitted by the same [`commit::begin`] a sole writer
    /// runs — binding, exact idempotency, OCC against its own authoritative
    /// version, and route fencing — so a group weakens nothing; it repeats the
    /// per-scope bound N times over one transaction. If any member fails
    /// admission the whole group is discarded, because there is only one
    /// transaction and no partial outcome to choose.
    ///
    /// A member whose idempotency key names a terminal receipt returns
    /// [`Begin::Replay`] and is marked terminal here: it writes nothing and is
    /// not `finish`ed, and the group still commits, because the other members'
    /// rows are real. A sole writer aborts on `Begin::Replay`; a group cannot,
    /// since one retry among N is the coalescer's ordinary case.
    ///
    /// The single-scope [`Self::admit`] remains the path for every other layout.
    pub fn admit_group<'a, 'i, D: OwnerDomain>(
        &'a self,
        control: ScopedIntent<'i, D>,
        members: impl IntoIterator<Item = ScopedIntent<'i, D>>,
    ) -> Result<AdmittedGroup<'a, D>, String> {
        let intents: Vec<ScopedIntent<'i, D>> = std::iter::once(control).chain(members).collect();
        let owners: Vec<&OwnedStoreHandle<D>> = intents.iter().map(|intent| intent.owner).collect();
        let admitted = self.group_members(&owners)?;
        let mut begins = Vec::with_capacity(intents.len());
        for (write, intent) in admitted.iter().zip(intents.iter()) {
            let begun = match admit_group_member(write, intent.batch) {
                Ok(begun) => begun,
                Err(error) => {
                    // Discard the group, but report the ADMISSION failure: a
                    // teardown error must not stand in for the real cause.
                    let _ = AdmittedGroup::new(admitted, begins).end(false);
                    return Err(error);
                }
            };
            begins.push(begun);
        }
        Ok(AdmittedGroup::new(admitted, begins))
    }

    /// Commit a whole admitted scope group as one physical transaction.
    ///
    /// `batches[i]` is the batch member `i` was admitted with, control member
    /// first. Every member is sealed by the same check a sole commit runs, and
    /// a member that fails to seal aborts the group.
    pub fn commit_group<D: OwnerDomain>(
        &self,
        group: AdmittedGroup<'_, D>,
        batches: &[&MutationBatch],
    ) -> Result<(), String> {
        commit::commit_group(group, batches)
    }

    /// Discard a whole admitted scope group: no member's rows land.
    pub fn abort_group<D: OwnerDomain>(&self, group: AdmittedGroup<'_, D>) -> Result<(), String> {
        group.end(false)
    }

    /// Persist terminal metadata for one admitted batch, without committing.
    pub fn finish<D: OwnerDomain>(
        &self,
        write: &AdmittedMutation<'_, D>,
        batch: &MutationBatch,
        result_msgpack: Option<Vec<u8>>,
        committed_at_ms: u64,
        source_version: Option<u64>,
    ) -> Result<MutationBatchRecord, String> {
        commit::finish(
            write,
            batch,
            result_msgpack,
            committed_at_ms,
            source_version,
        )
    }

    /// Persist terminal metadata and a typed replay receipt atomically with
    /// the owner rows.
    pub fn finish_with_replay<D: OwnerDomain>(
        &self,
        write: &AdmittedMutation<'_, D>,
        batch: &MutationBatch,
        result_msgpack: Option<Vec<u8>>,
        committed_at_ms: u64,
        source_version: Option<u64>,
        // The typed authority receipt filed atomically as this batch's replay
        // row: the operation identity it authenticates, the nonce it consumes,
        // and the receipt itself. These three are never meaningful apart, so
        // they travel as one group rather than as three positional parameters.
        replay_receipt: (&OperationReplayIdentity, &NonceReplayKey, &MutationReceipt),
    ) -> Result<MutationBatchRecord, String> {
        commit::finish_with_replay(
            write,
            batch,
            result_msgpack,
            committed_at_ms,
            source_version,
            replay_receipt,
        )
    }

    /// Commit one admitted write, consuming it.
    pub fn commit<D: OwnerDomain>(
        &self,
        write: AdmittedMutation<'_, D>,
        batch: &MutationBatch,
    ) -> Result<(), String> {
        commit::commit(write, batch)
    }

    /// Atomically remove authority for one exact logical generation.
    ///
    /// Refused for a layout that owns tables: the kernel cannot sweep owner
    /// rows (their keys carry no scope component), and retiring the authority
    /// alone would leave the payload for the next binding of the same logical
    /// name. Such a layout uses [`Self::purge_scope_with`].
    pub fn purge_scope<D: OwnerDomain>(
        &self,
        owner: &OwnedStoreHandle<D>,
        identity: &MutationScopeIdentity,
    ) -> Result<(), String> {
        let write = AdmittedMutation::open(&self.authority, owner)?;
        commit::purge_scope(&write, identity, None)?;
        write.commit()
    }

    /// Retire one generation's ledger authority **and** its domain payload in
    /// one transaction, the domain supplying the sweep of its own tables.
    pub fn purge_scope_with<D: OwnerDomain>(
        &self,
        owner: &OwnedStoreHandle<D>,
        identity: &MutationScopeIdentity,
        owner_payload: &dyn OwnerPayloadRetirement<D>,
    ) -> Result<(), String> {
        let write = AdmittedMutation::open(&self.authority, owner)?;
        commit::purge_scope(&write, identity, Some(owner_payload))?;
        write.commit()
    }

    /// Prepare one saga: durable `Prepared` receipt, no owner rows yet.
    pub fn saga_begin<D: OwnerDomain>(
        &self,
        owner: &OwnedStoreHandle<D>,
        batch: &MutationBatch,
        prepared_at_ms: u64,
    ) -> Result<SagaBegin, String> {
        saga::prepare_saga(&self.authority, owner, batch, prepared_at_ms)
    }

    /// Prepare one saga together with its sealed private recovery payload.
    pub fn saga_step<D: OwnerDomain>(
        &self,
        owner: &OwnedStoreHandle<D>,
        batch: &MutationBatch,
        prepared_at_ms: u64,
        private_payload: Option<&[u8]>,
    ) -> Result<SagaBegin, String> {
        saga::prepare_saga_with_private_payload(
            &self.authority,
            owner,
            batch,
            prepared_at_ms,
            private_payload,
        )
    }

    /// Terminalize one prepared saga. The `bool` is `true` when the saga was
    /// already committed and this call only replayed its receipt.
    pub fn saga_end<D: OwnerDomain>(
        &self,
        owner: &OwnedStoreHandle<D>,
        batch: &MutationBatch,
        result_msgpack: Vec<u8>,
        committed_at_ms: u64,
    ) -> Result<(MutationBatchRecord, bool), String> {
        saga::commit_saga(
            &self.authority,
            owner,
            batch,
            result_msgpack,
            committed_at_ms,
        )
    }

    /// Decide one proposed attempt against the durable replay ledger, **inside**
    /// the caller's admitted write.
    ///
    /// Resolution, the effect and [`Self::record_replay`] therefore share one
    /// physical transaction: no window exists in which two attempts can both
    /// resolve `Fresh` and both commit. A resolution taken in a transaction
    /// that is then aborted decides nothing, which is why this does not open
    /// its own.
    pub fn resolve_replay<D: OwnerDomain>(
        &self,
        write: &AdmittedMutation<'_, D>,
        operation: &OperationReplayIdentity,
        nonce: &NonceReplayKey,
    ) -> Result<ReplayResolution, String> {
        resolve_replay_in(write, operation, nonce)
    }

    /// Resolve only an attempt nonce in the caller's already-open owner write.
    ///
    /// This is the nonce-first half of two-identity replay for domains whose
    /// stable operation body may require a retained-row read. It performs no
    /// operation lookup or mutation; the caller must follow a `None` result by
    /// building its complete operation and calling [`Self::resolve_replay`]
    /// before any effects.
    pub fn resolve_nonce<D: OwnerDomain>(
        &self,
        write: &AdmittedMutation<'_, D>,
        nonce: &NonceReplayKey,
    ) -> Result<Option<String>, String> {
        resolve_nonce_in(write, nonce)
    }

    pub fn read_replay_evidence<D: OwnerDomain>(
        &self,
        write: &AdmittedMutation<'_, D>,
        batch_id: &str,
    ) -> Result<
        Option<(
            MutationBatchRecord,
            eg_storage::MutationClass,
            Vec<eg_types::MutationOutboxRecord>,
        )>,
        String,
    > {
        commit::read_replay_evidence_in(write, batch_id)
    }

    pub fn finalize_replay_receipt<D: OwnerDomain>(
        &self,
        write: &AdmittedMutation<'_, D>,
        operation: &OperationReplayIdentity,
        nonce: &NonceReplayKey,
        receipt: &MutationReceipt,
    ) -> Result<(), String> {
        finalize_replay_receipt_in(write, operation, nonce, receipt)
    }

    /// Commit the already-open write that only finalized a typed replay nonce.
    /// The caller must invoke [`Self::finalize_replay_receipt`] first; this
    /// path has no admitted batch and therefore cannot write owner rows,
    /// versions, fences, batches or outbox records.
    pub fn commit_replay_receipt<D: OwnerDomain>(
        &self,
        write: AdmittedMutation<'_, D>,
    ) -> Result<(), String> {
        write.commit()
    }

    /// Open the one write for `owner` without admitting a batch, for a caller
    /// that must resolve replay before it knows whether a batch exists.
    pub fn open_write<'a, D: OwnerDomain>(
        &'a self,
        owner: &OwnedStoreHandle<D>,
    ) -> Result<AdmittedMutation<'a, D>, String> {
        AdmittedMutation::open(&self.authority, owner)
    }

    /// Read the bound scope version while the caller already holds the sole
    /// physical write transaction. This closes the read/build race for callers
    /// that must derive a complete owner-row batch from retained state.
    pub fn current_version<D: OwnerDomain>(
        &self,
        write: &AdmittedMutation<'_, D>,
        owner: &OwnedStoreHandle<D>,
    ) -> Result<u64, String> {
        if write.scope() != owner.identity() {
            return Err("mutation write scope does not match owner identity".to_string());
        }
        crate::ledger::bound_scope_version(write, owner.identity())
    }

    /// Durably subscribe one consumer of `owner`'s scope to one outbox topic.
    ///
    /// A subscription is the consumer's liveness and the boundary of its
    /// ordered stream. It is idempotent for the same topic and refused for a
    /// different one: changing it would move every position the consumer's
    /// cursor already names.
    pub fn outbox_subscribe<D: OwnerDomain>(
        &self,
        owner: &OwnedStoreHandle<D>,
        consumer: &str,
        topic: &str,
    ) -> Result<(), String> {
        crate::outbox::subscribe(&self.authority, owner, consumer, topic)
    }

    /// Claim pending outbox rows of `owner`'s scope for one durable consumer.
    ///
    /// Rows come in commit order from a durable scan position, each under a
    /// lease keyed `(scope, consumer, batch id, ordinal)` with a monotonic
    /// epoch. Selection and lease installation share one transaction, so two
    /// workers of one consumer can never both hold a row, and a crash between
    /// claim and ack leaves the lease to expire -- delivery is at-least-once.
    ///
    /// `budget` carries the caller-owned sweep-local 25% consecutive-claim cap
    /// across the scopes the caller visits. The composition scheduler chooses
    /// tenant order and weights and must reuse one budget for that sweep; this
    /// kernel does not persist cross-scope scheduler debt. A full in-flight
    /// queue or an exhausted sweep cap claims nothing and leaves the
    /// durable intention pending; it never drops work.
    pub fn outbox_claim<D: OwnerDomain>(
        &self,
        owner: &OwnedStoreHandle<D>,
        consumer: &str,
        budget: &mut OutboxClaimBudget,
    ) -> Result<OutboxClaimOutcome, String> {
        crate::outbox::claim(&self.authority, owner, consumer, budget)
    }

    /// Index every outbox row of `owner`'s scope that has no index row yet.
    ///
    /// The commit-ordered index exists only because `commit::write_outbox`
    /// writes it, so every row committed before this protocol landed is
    /// invisible to a claim -- never delivered and reported as no lag at all.
    /// The returned completion bit is durable state: an already-indexed page
    /// may report zero inserts while later primary rows still need repair.
    pub fn outbox_backfill_index<D: OwnerDomain>(
        &self,
        owner: &OwnedStoreHandle<D>,
    ) -> Result<OutboxBackfillOutcome, String> {
        crate::outbox::backfill(&self.authority, owner)
    }

    /// Acknowledge one held lease and advance the consumer's projection cursor
    /// in the SAME admitted transaction.
    ///
    /// A crash between marking the row delivered and moving the watermark is
    /// therefore not representable: both land or neither does.
    pub fn outbox_ack<D: OwnerDomain>(
        &self,
        owner: &OwnedStoreHandle<D>,
        lease: &MutationOutboxLease,
        now_ms: u64,
    ) -> Result<MutationProjectionCursor, String> {
        crate::outbox::ack(&self.authority, owner, lease, now_ms)
    }

    /// Validate one held lease inside an already-admitted owner transaction.
    ///
    /// This performs the exact checks [`Self::outbox_ack_in`] will repeat, but
    /// writes nothing. Domain consumers use it before opening their owner-row
    /// gate so a stale, expired, released or out-of-order lease cannot stage an
    /// effect that will only be rejected after the effect ran.
    pub fn outbox_validate_in<D: OwnerDomain>(
        &self,
        write: &AdmittedMutation<'_, D>,
        owner: &OwnedStoreHandle<D>,
        lease: &MutationOutboxLease,
        now_ms: u64,
    ) -> Result<(), String> {
        ensure_admitted_owner(write, owner)?;
        crate::outbox::validate_in(write, owner.identity(), lease, now_ms)
    }

    /// Acknowledge one held lease inside an already-admitted owner transaction.
    ///
    /// This method does not commit `write`. The caller first writes its owner
    /// rows and terminal receipt, then adds this delivery transition and commits
    /// the admitted batch once, making the effect, receipt and acknowledgement
    /// indivisible. Requiring an admitted, scope-matching write prevents this
    /// entry point from becoming a second standalone acknowledgement authority.
    pub fn outbox_ack_in<D: OwnerDomain>(
        &self,
        write: &AdmittedMutation<'_, D>,
        owner: &OwnedStoreHandle<D>,
        lease: &MutationOutboxLease,
        now_ms: u64,
    ) -> Result<MutationProjectionCursor, String> {
        ensure_admitted_owner(write, owner)?;
        crate::outbox::ack_in_transaction(write, owner.identity(), lease, now_ms)
    }

    /// Give one held lease back without delivering it, so the row is
    /// immediately re-claimable. The released lease can never be acknowledged.
    pub fn outbox_release<D: OwnerDomain>(
        &self,
        owner: &OwnedStoreHandle<D>,
        lease: &MutationOutboxLease,
    ) -> Result<(), String> {
        crate::outbox::release(&self.authority, owner, lease)
    }

    /// Retire every expired lease of one consumer, returning how many. An
    /// expired lease is already re-claimable; this makes the queue's reported
    /// in-flight count agree with that.
    pub fn outbox_expire<D: OwnerDomain>(
        &self,
        owner: &OwnedStoreHandle<D>,
        consumer: &str,
        now_ms: u64,
    ) -> Result<u32, String> {
        crate::outbox::expire(&self.authority, owner, consumer, now_ms)
    }

    // `outbox_reject`, `outbox_reject_in`, `outbox_dead_letters` and
    // `outbox_rewind` are inherent methods too, defined in
    // `outbox::operator` to keep this file's aggregates from growing further
    // (`Self::authority` is the seam that lets another file in this crate's
    // module tree write an `impl MutationKernel` block at all).

    /// Phases B and C of a graft: copy the fenced scope's whole ledger into
    /// `destination` verbatim, then retire the source binding
    /// (RF-RULING-004 application note 4).
    ///
    /// Also the resume entry point: a destination that already carries this
    /// graft's marker receipt is recognised as its own completed copy and the
    /// call goes straight to the source retirement, so a crash anywhere in the
    /// window converges on exactly one authority.
    ///
    /// Every ledger row of that scope -- receipts, idempotency, class, version,
    /// fence, outbox events, and the delivery-side claim, cursor and fairness
    /// rows -- crosses unchanged, because `ledger_scope_key` carries no store
    /// incarnation and the keys are therefore file-independent. An in-flight
    /// OCC expectation against the source is still exactly correct at the
    /// destination.
    ///
    /// Re-admitting the moved batches would not be equivalent: it would
    /// re-execute effects, re-emit outbox rows, stamp new receipts, and reset
    /// the version. Refused for two different layouts, for two different
    /// scopes, for a destination that already holds batches, and -- on
    /// completion -- for a source whose binding is still in place.
    pub fn graft_scope<S: OwnerDomain, D: OwnerDomain>(
        &self,
        source: GraftSource<'_, S>,
        destination: &GraftDestination<'_, D>,
    ) -> Result<GraftedScope, String> {
        crate::graft::graft_scope(&self.authority, source, destination)
    }

    /// Complete a graft whose owner layout has payload rows.  The transfer is
    /// invoked only after the destination reservation and source fence have
    /// been authenticated, and it shares the destination transaction with
    /// the verbatim ledger copy.
    pub fn graft_scope_with_payload<S: OwnerDomain, D: OwnerDomain>(
        &self,
        source: GraftSource<'_, S>,
        destination: &GraftDestination<'_, D>,
        payload: &mut dyn crate::OwnerPayloadTransfer<S, D>,
    ) -> Result<GraftedScope, String> {
        crate::graft::graft_scope_with_payload(&self.authority, source, destination, Some(payload))
    }

    /// Reserve the destination half of a graft before the source is fenced.
    /// Storage movers use this while their source remains live so an owner
    /// payload can be staged without manufacturing a maintenance ledger row.
    pub fn graft_reserve_destination<S: OwnerDomain, D: OwnerDomain>(
        &self,
        source: &OwnedStoreHandle<S>,
        destination: &GraftDestination<'_, D>,
    ) -> Result<(), String> {
        crate::graft::graft_reserve_destination(self, source, destination)
    }

    /// Stage owner payload through an exact destination reservation.  The
    /// callback receives only layout-owned table access and shares the
    /// reservation transaction; it cannot write mutation-ledger rows.
    pub fn graft_stage_destination_payload<D: OwnerDomain, F>(
        &self,
        destination: &GraftDestination<'_, D>,
        identity: &MutationScopeIdentity,
        source_digest: &str,
        stage: F,
    ) -> Result<(), String>
    where
        F: for<'cap, 'store> FnOnce(&crate::GraftOwnerWrite<'cap, 'store, D>) -> Result<(), String>,
    {
        crate::graft::graft_stage_destination_payload(
            &self.authority,
            destination,
            identity,
            source_digest,
            stage,
        )
    }

    /// Stage owner payload using the unique durable reservation already held
    /// by `destination`; the reservation supplies the authenticated source
    /// authority for command paths that carry only raw destination rows.
    pub fn graft_stage_reserved_payload<D: OwnerDomain, F>(
        &self,
        destination: &GraftDestination<'_, D>,
        identity: &MutationScopeIdentity,
        stage: F,
    ) -> Result<(), String>
    where
        F: for<'cap, 'store> FnOnce(&crate::GraftOwnerWrite<'cap, 'store, D>) -> Result<(), String>,
    {
        crate::graft::graft_stage_reserved_payload(&self.authority, destination, identity, stage)
    }

    /// Recover a graft after phase B and source retirement committed but the
    /// caller lost its source owner handle.  The source file is consulted only
    /// for its authority digest and direct binding-absence proof; it is never
    /// rebound or used to probe a retired scope.
    pub fn graft_recover<D: OwnerDomain>(
        &self,
        source_storage: &StorageKernel,
        identity: &MutationScopeIdentity,
        destination: &GraftDestination<'_, D>,
    ) -> Result<GraftedScope, String> {
        crate::graft::graft_recover(&self.authority, source_storage, identity, destination)
    }

    /// Phase A of a graft: fence this scope and record where it is going.
    ///
    /// Called on the SOURCE file's kernel. It admits one maintenance batch
    /// whose version is resolved in-lock and whose route fence is the maximum,
    /// so from its commit onward every ordinary admission on the scope fails
    /// closed with `STALE_FENCE` -- the scope is under graft and cannot move
    /// under the copy that follows. Idempotent: the marker's batch id is its
    /// idempotency key, so a repeat replays the receipt.
    pub fn graft_begin<S: OwnerDomain, D: OwnerDomain>(
        &self,
        source: &OwnedStoreHandle<S>,
        destination: &GraftDestination<'_, D>,
    ) -> Result<GraftIntent, String> {
        crate::graft::graft_begin(self, source, destination)
    }

    /// Read and authenticate the unique source-side graft marker before a
    /// storage mover binds a destination handle.  The scan validates the
    /// marker receipt, maintenance class and exact maintenance claim, so a
    /// malformed or differently targeted marker fails before the caller can
    /// create destination binding state as a side effect.
    pub fn graft_source_marker<S: OwnerDomain>(
        &self,
        source: &OwnedStoreHandle<S>,
    ) -> Result<Option<GraftIntent>, String> {
        crate::graft::graft_source_marker(self, source)
    }

    /// Prove that `destination` carries the exact authenticated reservation
    /// for `source_digest` and this destination authority.  This is a
    /// read-only proof: it opens the destination's serialized owner view and
    /// unconditionally aborts it, so no ledger or owner rows can change.  A
    /// false result means this specific source/target reservation is absent;
    /// malformed evidence is returned as an error rather than treated as a
    /// different reservation.
    pub fn graft_reservation_matches<D: OwnerDomain>(
        &self,
        destination: &GraftDestination<'_, D>,
        identity: &MutationScopeIdentity,
        source_digest: &str,
    ) -> Result<bool, String> {
        crate::graft::graft_reservation_matches(self, destination, identity, source_digest)
    }

    /// Consume one attempt nonce and record its receipt inside `write`, so the
    /// effect and its replay evidence commit atomically.
    pub fn record_replay<D: OwnerDomain>(
        &self,
        write: &AdmittedMutation<'_, D>,
        operation: &OperationReplayIdentity,
        nonce: &NonceReplayKey,
        receipt: &MutationReceipt,
    ) -> Result<(), String> {
        record_replay_in(write, operation, nonce, receipt)
    }
}

/// Bind an in-transaction helper to the exact admitted owner capability.
pub(crate) fn ensure_admitted_owner<D: OwnerDomain>(
    write: &AdmittedMutation<'_, D>,
    owner: &OwnedStoreHandle<D>,
) -> Result<(), String> {
    if write.scope() != owner.identity() {
        return Err("mutation write scope does not match owner identity".to_string());
    }
    write.admitted_batch_id().map(drop)
}

/// Resolve one scope's authoritative version INSIDE `write` and build the batch
/// for it, refusing any batch that does not expect exactly that version.
///
/// The one implementation shared by [`MutationKernel::admit_current`] and
/// [`MutationKernel::admit_group_current`]: a forked prologue is how the two
/// would drift about which expectation a graph scope owes.
fn current_batch<D: OwnerDomain, F>(
    write: &AdmittedMutation<'_, D>,
    owner: &OwnedStoreHandle<D>,
    build: F,
) -> Result<MutationBatch, String>
where
    F: FnOnce(u64) -> Result<MutationBatch, String>,
{
    let version = crate::ledger::bound_scope_version(write, owner.identity())?;
    let batch = build(version)?;
    let expected = expected_current_version(owner.identity(), version);
    if batch.version_expectation != expected {
        return Err(format!(
            "current-version admission requires {expected:?} but the batch expects {:?}",
            batch.version_expectation
        ));
    }
    Ok(batch)
}

/// Admit one group member through exactly the check a sole writer runs, and
/// mark a replayed member terminal so it writes nothing.
///
/// A replayed member's receipt is already durable, so it applies nothing here
/// and must not be able to. Marking it terminal is what lets the group commit
/// the other members' real rows.
fn admit_group_member<D: OwnerDomain>(
    write: &AdmittedMutation<'_, D>,
    batch: &MutationBatch,
) -> Result<Begin, String> {
    let begun = commit::begin(write, batch)?;
    if matches!(begun, Begin::Replay(_)) {
        write.admit_replayed_batch(batch)?;
    }
    Ok(begun)
}
