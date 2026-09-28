use super::store_prelude::*;
use super::*;

/// Deterministic failure injection points around the authoritative batch commit.
/// Production always calls with `None`; unit tests use these boundaries to prove
/// that restart observes either no batch or one complete committed batch.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum MutationBatchCrashpoint {
    BeforeRows,
    AfterRowsBeforeMetadata,
    BeforeCommit,
    AfterCommitBeforeAck,
}

/// Atomically apply one canonical mutation batch to graph rows, durable status,
/// idempotency index, and transactional outbox.
///
/// This is deliberately separate from [`commit_ops`]: a batch is already the
/// caller's all-or-nothing unit and must never be folded into a partially-acked
/// queue group.  One immediate redb `WriteTransaction` is its commit point.  An
/// exact retry returns the stored result; cross-modal retries may re-derive only
/// the OCC version from the current authoritative observation while the durable
/// record retains the original. Reusing an idempotency key for different work
/// fails closed.
pub(crate) fn commit_mutation_batch(
    shard: &Shard,
    graph_fname: &str,
    batch: &MutationBatch,
    result_msgpack: Option<&[u8]>,
    committed_at_ms: u64,
    crypto: DurableCrypto<'_>,
    #[cfg(feature = "security")] audit_tail: &mut AuditTailCache,
) -> Result<MutationBatchCommit, String> {
    commit_mutation_batch_inner(
        shard,
        BatchCommitInput::compact(graph_fname, batch, result_msgpack, committed_at_ms),
        crypto,
        #[cfg(feature = "security")]
        audit_tail,
    )
}

/// Commit authenticated staged graph material through the same batch kernel. The
/// digest/version descriptor lives in `batch`; complete snapshots or affected-row
/// deltas are supplied out-of-line so status/outbox do not duplicate them.
///
/// `audited` is the CALLER's already-resolved `MutationPlan::audited` (or
/// equivalent `eg_capabilities::policy(method).audited`) for the ORIGINAL,
/// pre-opaque-wrapped method -- see the doc comment on `commit_mutation_batch_inner`
/// for why this cannot be re-derived downstream once the operation is compiled.
/// Borrowed carrier for [`commit_mutation_batch_state`]'s graph-identifying and
/// coordinator-record inputs, bundled so the function stays under the clippy
/// argument-count ceiling.
pub(crate) struct StateCommitInput<'a> {
    pub(crate) graph_fname: &'a str,
    pub(crate) batch: &'a MutationBatch,
    pub(crate) authoritative_state_msgpack: &'a [u8],
    pub(crate) result_msgpack: Option<&'a [u8]>,
    pub(crate) committed_at_ms: u64,
    pub(crate) audited: bool,
}

pub(crate) fn commit_mutation_batch_state(
    shard: &Shard,
    input: StateCommitInput<'_>,
    crypto: DurableCrypto<'_>,
    #[cfg(feature = "security")] audit_tail: &mut AuditTailCache,
) -> Result<MutationBatchCommit, String> {
    commit_mutation_batch_inner(
        shard,
        BatchCommitInput {
            rows: BatchRowInput {
                graph_fname: input.graph_fname,
                batch: input.batch,
                change: None,
                source_budget: None,
                authoritative_state_msgpack: Some(input.authoritative_state_msgpack),
                committed_at_ms: input.committed_at_ms,
                audited: input.audited,
                crashpoint: None,
            },
            crossmodal: None,
            result_msgpack: input.result_msgpack,
        },
        crypto,
        #[cfg(feature = "security")]
        audit_tail,
    )
}

mod change_envelope;
#[cfg(test)]
pub(crate) use change_envelope::{commit_change_envelope, ChangeEnvelopesError};
pub(crate) use change_envelope::{commit_change_envelope_with_budget, commit_change_envelopes};

/// Non-graph rows that participate in an authoritative cross-modal
/// [`MutationBatch`] commit. The coordinator metadata, graph rows, semantic/blob
/// projections, time-series batches, result and outbox are written by the same redb
/// transaction; this borrowed carrier is never serialized as a second authority.
pub(crate) struct CrossModalBatchRows<'a> {
    pub(crate) methods: &'a [Method],
    pub(crate) vectors: &'a [VectorUpsert],
    pub(crate) blob_refs: &'a [BlobRefRow],
    pub(crate) measurements: &'a [crate::MeasurementBatch],
}

/// Authenticated graph material supplied by a complex MutationBatch. Callers may
/// provide a complete snapshot or persist only the affected rows.
pub(crate) enum AuthoritativeGraphState {
    Snapshot(Box<crate::graph::GraphSnapshot>),
    RowDelta(crate::graph_delta::GraphRowDelta),
}

/// Borrowed carrier for [`commit_mutation_batch_crossmodal`]'s graph-identifying
/// and coordinator-record inputs, bundled alongside the row material so the
/// function itself stays under the clippy argument-count ceiling.
pub(crate) struct CrossModalCommitInput<'a> {
    pub(crate) graph_fname: &'a str,
    pub(crate) batch: &'a MutationBatch,
    pub(crate) rows: CrossModalBatchRows<'a>,
    pub(crate) result_msgpack: Option<&'a [u8]>,
    pub(crate) committed_at_ms: u64,
}

/// Commit a canonical cross-modal batch through the universal status/fence/
/// idempotency/outbox kernel. Public mutation surfaces use this canonical path;
/// [`commit_crossmodal`] remains the low-level atomic projection primitive.
pub(crate) fn commit_mutation_batch_crossmodal(
    shard: &Shard,
    input: CrossModalCommitInput<'_>,
    crypto: DurableCrypto<'_>,
    #[cfg(feature = "security")] audit_tail: &mut AuditTailCache,
) -> Result<MutationBatchCommit, String> {
    commit_mutation_batch_inner(
        shard,
        BatchCommitInput {
            crossmodal: Some(input.rows),
            ..BatchCommitInput::compact(
                input.graph_fname,
                input.batch,
                input.result_msgpack,
                input.committed_at_ms,
            )
        },
        crypto,
        #[cfg(feature = "security")]
        audit_tail,
    )
}

/// Common owner-row inputs for both the commit and row-staging phases.
pub(crate) struct BatchRowInput<'a> {
    pub(crate) graph_fname: &'a str,
    pub(crate) batch: &'a MutationBatch,
    pub(crate) change: Option<&'a ChangeEnvelope>,
    pub(crate) source_budget:
        Option<&'a crate::redb_store::enrichment_budget::SourceBudgetAuthority>,
    pub(crate) authoritative_state_msgpack: Option<&'a [u8]>,
    pub(crate) committed_at_ms: u64,
    /// Whether THIS commit appends tamper-evident audit-chain entries. Only
    /// consulted on the authoritative-state branches; see the note below on why
    /// it cannot be re-derived from `batch.operations`.
    pub(crate) audited: bool,
    pub(crate) crashpoint: Option<MutationBatchCrashpoint>,
}

/// The inputs every batch-commit entry point shares, bundled so the phases
/// below stay inside the argument cap without positional strings.
pub(crate) struct BatchCommitInput<'a> {
    pub(crate) rows: BatchRowInput<'a>,
    pub(crate) crossmodal: Option<CrossModalBatchRows<'a>>,
    pub(crate) result_msgpack: Option<&'a [u8]>,
}

impl<'a> BatchCommitInput<'a> {
    /// Default fields for a batch without out-of-line authoritative state.
    /// Cross-modal callers replace the row effect; the audit bit is inert.
    pub(crate) fn compact(
        graph_fname: &'a str,
        batch: &'a MutationBatch,
        result_msgpack: Option<&'a [u8]>,
        committed_at_ms: u64,
    ) -> Self {
        Self {
            rows: BatchRowInput {
                graph_fname,
                batch,
                change: None,
                source_budget: None,
                authoritative_state_msgpack: None,
                committed_at_ms,
                audited: true,
                crashpoint: None,
            },
            crossmodal: None,
            result_msgpack,
        }
    }

    pub(crate) fn with_crashpoint(mut self, crashpoint: Option<MutationBatchCrashpoint>) -> Self {
        self.rows.crashpoint = crashpoint;
        self
    }
}

type ReactivationStage<'a> = dyn for<'g> FnMut(&ShardWrite<'g>) -> Result<(), String> + 'a;

/// Private opt-in for an enrichment reactivation. The callback stages the
/// budget/park CAS in the same owner-row admission as the canonical batch.
/// The batch must contain its replacement outbox intent. This entry point is
/// intentionally local to a graph shard: no Raft proposal is represented.
pub(crate) fn commit_mutation_batch_with_outbox_lease(
    shard: &Shard,
    input: BatchCommitInput<'_>,
    lease: &eg_types::MutationOutboxLease,
    stage_reactivation: &mut ReactivationStage<'_>,
    crypto: DurableCrypto<'_>,
    #[cfg(feature = "security")] audit_tail: &mut AuditTailCache,
) -> Result<MutationBatchCommit, String> {
    commit_mutation_batch_with_reactivation_effect(
        shard,
        input,
        Some(lease),
        stage_reactivation,
        crypto,
        #[cfg(feature = "security")]
        audit_tail,
    )
}

/// Stage deterministic reactivation owner rows and the replacement intent in
/// one graph batch, without depending on a node-local delivery lease. A Raft
/// apply can use this on every replica; the old delivery is resolved separately
/// by each node after the replicated supersession marker is durable. This is
/// deliberately a storage primitive, not a served reactivation route.
pub(crate) fn commit_mutation_batch_with_reactivation_rows(
    shard: &Shard,
    input: BatchCommitInput<'_>,
    stage_reactivation: &mut ReactivationStage<'_>,
    crypto: DurableCrypto<'_>,
    #[cfg(feature = "security")] audit_tail: &mut AuditTailCache,
) -> Result<MutationBatchCommit, String> {
    commit_mutation_batch_with_reactivation_effect(
        shard,
        input,
        None,
        stage_reactivation,
        crypto,
        #[cfg(feature = "security")]
        audit_tail,
    )
}

fn commit_mutation_batch_with_reactivation_effect(
    shard: &Shard,
    input: BatchCommitInput<'_>,
    lease: Option<&eg_types::MutationOutboxLease>,
    stage_reactivation: &mut ReactivationStage<'_>,
    crypto: DurableCrypto<'_>,
    #[cfg(feature = "security")] audit_tail: &mut AuditTailCache,
) -> Result<MutationBatchCommit, String> {
    commit_mutation_batch_inner_with_effect(
        shard,
        input,
        Some(OutboxBatchEffect {
            lease,
            #[cfg(feature = "raft")]
            top_up: None,
            stage_reactivation,
        }),
        crypto,
        #[cfg(feature = "security")]
        audit_tail,
    )
}

#[cfg(feature = "raft")]
#[derive(Clone, Copy)]
struct TopUpSourceEffect<'a> {
    transition: &'a crate::raft::EnrichmentTopUpTransition,
}

/// One Raft state-machine application of a verified, sealed top-up. The
/// replacement parent, old source receipt and policy/budget/park rows commit in
/// one admitted graph transaction on every replica.
#[cfg(feature = "raft")]
pub(crate) fn commit_repository_enrichment_top_up(
    shard: &Shard,
    graph: &str,
    transition: &crate::raft::EnrichmentTopUpTransition,
    committed_at_ms: u64,
    crypto: DurableCrypto<'_>,
    #[cfg(feature = "security")] audit_tail: &mut AuditTailCache,
) -> Result<MutationBatchCommit, String> {
    transition.validate(graph, committed_at_ms)?;
    // Retain the exact sealed transition in the private commit receipt so a
    // new leader can reconstruct a byte-identical retry after the park was
    // cleared and the replacement cursor has advanced.
    let receipt = rmp_serde::to_vec_named(transition)
        .map_err(|_| "CONFLICT: enrichment top-up receipt encode failed")?;
    let mut stage = |write: &ShardWrite<'_>| {
        crate::redb_store::enrichment_budget::stage_parked_reactivation(
            write,
            graph,
            &transition.expected_budget,
            &transition.expected_park,
            &transition.replacement_budget,
            &transition.revision,
            crypto,
        )
    };
    commit_mutation_batch_inner_with_effect(
        shard,
        BatchCommitInput::compact(
            graph,
            &transition.replacement_batch,
            Some(&receipt),
            committed_at_ms,
        ),
        Some(OutboxBatchEffect {
            lease: None,
            top_up: Some(TopUpSourceEffect { transition }),
            stage_reactivation: &mut stage,
        }),
        crypto,
        #[cfg(feature = "security")]
        audit_tail,
    )
}

struct OutboxBatchEffect<'a> {
    lease: Option<&'a eg_types::MutationOutboxLease>,
    #[cfg(feature = "raft")]
    top_up: Option<TopUpSourceEffect<'a>>,
    stage_reactivation: &'a mut ReactivationStage<'a>,
}

/// Commit ONE caller batch: its graph rows, its governance material, its
/// catalog entry, and the kernel's terminal metadata, in ONE transaction.
///
/// The batch is admitted through [`Shard::admit_batch`], which puts the caller's
/// batch verbatim on its graph member and the shard's own bookkeeping on the
/// control member. Everything that used to be checked here first is the
/// kernel's now and is checked INSIDE the transaction rather than before it:
/// binding, exact idempotency, OCC against the authoritative version, and route
/// fencing are `commit::begin`; the receipt, the idempotency row, the class row,
/// the version bump and the outbox rows are `commit::finish`. That is the whole
/// of the deleted `check_idempotency_replay` / `check_batch_id_uniqueness` /
/// `check_occ_version_and_fence` / `write_mutation_batch_*` family, and it
/// closes the window those checks could only narrow: they read before the write
/// lock was held.
///
/// A replay is therefore an ANSWER, not a pre-check. An exact retry resolves to
/// `Begin::Replay` at admission and its durable receipt is the result; nothing
/// is written for it, and there is no second idempotency authority to consult.
///
/// `audited`: whether THIS commit appends tamper-evident audit-chain entries for
/// its operations. Only consulted when `authoritative_state_msgpack` is `Some`.
/// It cannot be re-derived from `batch.operations`: `compile_methods`'s
/// `opaque_state_operation` rewrites EVERY state-backed operation into the same
/// opaque digest receipt (`Method::ApplyMutation{event_type:
/// "authoritative_state_operation", ..}`) by design, so sensitive row payloads
/// never enter the durable batch/audit/outbox record -- and by the time this
/// function sees the operation, the original method's own
/// `eg_capabilities::policy(..).audited` answer is unrecoverable. Callers pass
/// their already-resolved `MutationPlan::audited`.
pub(crate) fn commit_mutation_batch_inner(
    shard: &Shard,
    input: BatchCommitInput<'_>,
    crypto: DurableCrypto<'_>,
    #[cfg(feature = "security")] audit_tail: &mut AuditTailCache,
) -> Result<MutationBatchCommit, String> {
    commit_mutation_batch_inner_with_effect(
        shard,
        input,
        None,
        crypto,
        #[cfg(feature = "security")]
        audit_tail,
    )
}

fn commit_mutation_batch_inner_with_effect(
    shard: &Shard,
    input: BatchCommitInput<'_>,
    outbox_effect: Option<OutboxBatchEffect<'_>>,
    crypto: DurableCrypto<'_>,
    #[cfg(feature = "security")] audit_tail: &mut AuditTailCache,
) -> Result<MutationBatchCommit, String> {
    let graph_fname = input.rows.graph_fname;
    let batch = input.rows.batch;
    let committed_at_ms = input.rows.committed_at_ms;
    let result_msgpack = input.result_msgpack;

    // Bind before admission: a cold graph opens its own transaction. Raft
    // top-up already carries its sealed batch and must remain byte-identical.
    let handle = shard.graph(graph_fname)?;
    #[cfg(feature = "raft")]
    let top_up = outbox_effect.as_ref().and_then(|effect| effect.top_up);
    #[cfg(feature = "raft")]
    let bound = if top_up.is_some() {
        batch.clone()
    } else {
        shard::bind_caller_batch(handle.as_ref(), graph_fname, batch)?
    };
    #[cfg(not(feature = "raft"))]
    let bound = shard::bind_caller_batch(handle.as_ref(), graph_fname, batch)?;
    if outbox_effect.is_some() && bound.outbox.is_empty() {
        return Err("CONFLICT: reactivation batch requires a replacement outbox intent".into());
    }
    #[cfg(feature = "raft")]
    let (group, batches) = if top_up.is_some() {
        shard.admit_repository_enrichment_top_up_batch(
            graph_fname,
            &handle,
            &bound,
            &bound.batch_id,
        )?
    } else {
        shard.admit_batch(graph_fname, &handle, &bound, &bound.batch_id)?
    };
    #[cfg(not(feature = "raft"))]
    let (group, batches) = shard.admit_batch(graph_fname, &handle, &bound, &bound.batch_id)?;

    if matches!(group.begun(1)?, Begin::Replay(_)) {
        return finish_admitted_replay(
            shard,
            group,
            &batches,
            &handle,
            ReplayCommitInput {
                graph_fname,
                bound: &bound,
                result_msgpack,
                committed_at_ms,
                crypto,
                has_lease: outbox_effect
                    .as_ref()
                    .is_some_and(|effect| effect.lease.is_some()),
                #[cfg(feature = "raft")]
                top_up,
            },
        );
    }

    finish_fresh_batch(
        shard,
        AdmittedFreshBatch {
            group,
            batches,
            handle: &handle,
            bound: &bound,
            input,
            outbox_effect,
        },
        crypto,
        #[cfg(feature = "security")]
        audit_tail,
    )
}

struct AdmittedFreshBatch<'g, 'i, 'e> {
    group: AdmittedGroup<'g, GraphShardOwner>,
    batches: Vec<MutationBatch>,
    handle: &'g Arc<OwnedStoreHandle<GraphShardOwner>>,
    bound: &'g MutationBatch,
    input: BatchCommitInput<'i>,
    outbox_effect: Option<OutboxBatchEffect<'e>>,
}

fn finish_fresh_batch(
    shard: &Shard,
    admission: AdmittedFreshBatch<'_, '_, '_>,
    crypto: DurableCrypto<'_>,
    #[cfg(feature = "security")] audit_tail: &mut AuditTailCache,
) -> Result<MutationBatchCommit, String> {
    let AdmittedFreshBatch {
        group,
        batches,
        handle,
        bound,
        input,
        outbox_effect,
    } = admission;
    let BatchCommitInput {
        rows,
        crossmodal,
        result_msgpack,
    } = input;
    // The cache advances only after commit, never when a row or callback fails.
    #[cfg(feature = "security")]
    let mut staged_audit_tail = audit_tail.clone();
    #[cfg(feature = "raft")]
    let top_up = outbox_effect.as_ref().and_then(|effect| effect.top_up);
    let lease = outbox_effect.as_ref().and_then(|effect| effect.lease);
    let stage_reactivation = outbox_effect.map(|effect| effect.stage_reactivation);
    if let Some(effect) = lease {
        if let Err(error) = shard.mutations().outbox_validate_in(
            group.member(1)?,
            handle.as_ref(),
            effect,
            rows.committed_at_ms,
        ) {
            shard.mutations().abort_group(group)?;
            return Err(error);
        }
    }

    let staged_input = StagedRowInput {
        rows: BatchRowInput {
            graph_fname: rows.graph_fname,
            batch: bound,
            change: rows.change,
            source_budget: rows.source_budget,
            authoritative_state_msgpack: rows.authoritative_state_msgpack,
            committed_at_ms: rows.committed_at_ms,
            audited: rows.audited,
            crashpoint: rows.crashpoint,
        },
        crossmodal: crossmodal.as_ref(),
    };
    let staged_rows = stage_mutation_batch_rows_with_effect(
        shard,
        &group,
        &[(rows.graph_fname.to_string(), Arc::clone(handle))],
        &batches,
        staged_input,
        RowStagingOptions {
            effect: stage_reactivation,
            crypto,
        },
        #[cfg(feature = "security")]
        &mut staged_audit_tail,
    );
    let staged = match staged_rows {
        Ok(staged) => staged,
        Err(error) => {
            shard.mutations().abort_group(group)?;
            return Err(error);
        }
    };

    if let Some(effect) = lease {
        if let Err(error) = shard.mutations().outbox_ack_in(
            group.member(1)?,
            handle.as_ref(),
            effect,
            rows.committed_at_ms,
        ) {
            shard.mutations().abort_group(group)?;
            return Err(error);
        }
    }
    #[cfg(feature = "raft")]
    if let Some(top_up) = top_up {
        let transition = top_up.transition;
        if let Err(error) = shard.outbox_supersede_batch_record(
            &group,
            &handle,
            &transition.consumer,
            &transition.old_delivery,
            rows.committed_at_ms,
        ) {
            shard.mutations().abort_group(group)?;
            return Err(error);
        }
    }

    run_mutation_batch_crashpoint(
        bound,
        rows.crashpoint,
        MutationBatchCrashpoint::BeforeCommit,
    )?;
    crate::mutation_batch::apply_certification_fault(
        bound,
        crate::mutation_batch::MutationCommitPhase::BeforeCommit,
    )?;

    let result = staged
        .generated_result
        .or_else(|| result_msgpack.map(ToOwned::to_owned));
    let committed = shard.commit_batch(group, &batches, result, rows.committed_at_ms)?;

    #[cfg(feature = "security")]
    {
        *audit_tail = staged_audit_tail;
    }

    run_mutation_batch_crashpoint(
        bound,
        rows.crashpoint,
        MutationBatchCrashpoint::AfterCommitBeforeAck,
    )?;
    crate::mutation_batch::apply_certification_fault(
        bound,
        crate::mutation_batch::MutationCommitPhase::AfterCommitBeforeAck,
    )?;

    let commit = MutationBatchCommit {
        record: committed.record,
        identity: bound.identity.clone(),
        replayed: committed.replayed,
    };
    commit.validate()?;
    Ok(commit)
}

struct ReplayCommitInput<'a> {
    graph_fname: &'a str,
    bound: &'a MutationBatch,
    result_msgpack: Option<&'a [u8]>,
    committed_at_ms: u64,
    crypto: DurableCrypto<'a>,
    has_lease: bool,
    #[cfg(feature = "raft")]
    top_up: Option<TopUpSourceEffect<'a>>,
}

fn finish_admitted_replay(
    shard: &Shard,
    group: AdmittedGroup<'_, GraphShardOwner>,
    batches: &[MutationBatch],
    handle: &Arc<OwnedStoreHandle<GraphShardOwner>>,
    input: ReplayCommitInput<'_>,
) -> Result<MutationBatchCommit, String> {
    if input.has_lease {
        shard.mutations().abort_group(group)?;
        return Err(
            "CONFLICT: reactivation batch already committed; delivery state must be reconciled"
                .into(),
        );
    }
    // A byte-identical retry consumes the fresh attempt nonce in commit_batch
    // without reapplying any owner rows.
    let Begin::Replay(record) = group.begun(1)?.clone() else {
        return Err("admitted replay lost its receipt".to_string());
    };
    #[cfg(feature = "raft")]
    if let Some(top_up) = input.top_up {
        let transition = top_up.transition;
        let proposed_batch = rmp_serde::to_vec_named(input.bound)
            .map_err(|_| "CONFLICT: enrichment top-up retry encode failed")?;
        let committed_batch = rmp_serde::to_vec_named(&record.batch)
            .map_err(|_| "CONFLICT: enrichment top-up receipt encode failed")?;
        if proposed_batch != committed_batch
            || record.result_msgpack.as_deref() != input.result_msgpack
            || record.committed_at_ms != input.committed_at_ms
        {
            shard.mutations().abort_group(group)?;
            return Err("IDEMPOTENCY_CONFLICT: enrichment top-up retry changed".into());
        }
        if let Err(error) = shard.outbox_supersession_receipt_batch_record(
            &group,
            handle,
            &transition.consumer,
            &transition.old_delivery,
            input.committed_at_ms,
        ) {
            shard.mutations().abort_group(group)?;
            return Err(error);
        }
        if let Err(error) = crate::redb_store::enrichment_budget::verify_reactivation_replay(
            shard,
            input.graph_fname,
            &transition.expected_budget,
            &transition.expected_park,
            &transition.replacement_budget,
            &transition.revision,
            input.crypto,
        ) {
            shard.mutations().abort_group(group)?;
            return Err(error);
        }
    }
    #[cfg(not(feature = "raft"))]
    let _ = (handle, record, input.graph_fname, input.crypto);
    finish_replayed_batch(
        shard,
        group,
        batches,
        input.bound,
        input.result_msgpack,
        input.committed_at_ms,
    )
}

fn finish_replayed_batch(
    shard: &Shard,
    group: AdmittedGroup<'_, GraphShardOwner>,
    batches: &[MutationBatch],
    bound: &MutationBatch,
    result_msgpack: Option<&[u8]>,
    committed_at_ms: u64,
) -> Result<MutationBatchCommit, String> {
    let committed = shard.commit_batch(
        group,
        batches,
        result_msgpack.map(ToOwned::to_owned),
        committed_at_ms,
    )?;
    let commit = MutationBatchCommit {
        record: committed.record,
        identity: bound.identity.clone(),
        replayed: true,
    };
    commit.validate()?;
    Ok(commit)
}

#[cfg(test)]
mod outbox_reactivation_tests {
    use super::*;
    use eg_transaction::OutboxClaimBudget;
    use eg_types::mutation_batch::COMPILED_BATCH_INCARNATION;
    use eg_types::{
        contract::Digest256, MutationOperation, MutationOutboxIntent, MutationScopeIdentity,
        MutationSurface, VersionExpectation, MUTATION_BATCH_VERSION,
    };
    const GRAPH: &str = "graph-a";
    const CONSUMER: &str = "reactivation-worker";
    const TOPIC: &str = "repository.enrichment.pending";
    const CALLBACK_SOURCE: &str = "callback-source";

    fn batch(id: &str, key: &str, version: u64, node: &str) -> MutationBatch {
        let identity =
            MutationScopeIdentity::fixed_graph("tenant-a", GRAPH, COMPILED_BATCH_INCARNATION)
                .unwrap();
        let actor = format!("principal:sha256:{}", "a".repeat(64));
        let mut batch = MutationBatch {
            schema_version: MUTATION_BATCH_VERSION,
            batch_id: id.into(),
            envelope: crate::redb_store::fixture_operation_envelope(&identity, &actor, 42, key),
            identity,
            placement_epoch: 0,
            version_expectation: VersionExpectation::Graph(version),
            fencing_token: None,
            authoritative_state: None,
            operations: vec![MutationOperation {
                ordinal: 0,
                surface: MutationSurface::Graph,
                domain: DurabilityDomain::GraphRows,
                method: Method::AddNode {
                    node_id: node.into(),
                    properties_msgpack: rmp_serde::to_vec_named(&serde_json::json!({})).unwrap(),
                },
            }],
            outbox: vec![MutationOutboxIntent {
                topic: TOPIC.into(),
                key: id.into(),
                payload: vec![1],
                headers: Default::default(),
            }],
            created_at_ms: 1,
        };
        batch
            .reseal_envelope(Digest256::from_bytes([1_u8; 32]))
            .unwrap();
        batch
    }

    fn commit(
        shard: &Shard,
        batch: &MutationBatch,
        lease: Option<&eg_types::MutationOutboxLease>,
        stage: &mut ReactivationStage<'_>,
    ) -> Result<MutationBatchCommit, String> {
        #[cfg(feature = "security")]
        let mut audit_tail = AuditTailCache::new();
        let input =
            BatchCommitInput::compact(GRAPH, batch, None, if lease.is_some() { 12 } else { 10 });
        if let Some(lease) = lease {
            commit_mutation_batch_with_outbox_lease(
                shard,
                input,
                lease,
                stage,
                DurableCrypto::none(),
                #[cfg(feature = "security")]
                &mut audit_tail,
            )
        } else {
            commit_mutation_batch_inner(
                shard,
                input,
                DurableCrypto::none(),
                #[cfg(feature = "security")]
                &mut audit_tail,
            )
        }
    }

    fn commit_owner_rows(
        shard: &Shard,
        batch: &MutationBatch,
        stage: &mut ReactivationStage<'_>,
    ) -> Result<MutationBatchCommit, String> {
        #[cfg(feature = "security")]
        let mut audit_tail = AuditTailCache::new();
        commit_mutation_batch_with_reactivation_rows(
            shard,
            BatchCommitInput::compact(GRAPH, batch, None, 10),
            stage,
            DurableCrypto::none(),
            #[cfg(feature = "security")]
            &mut audit_tail,
        )
    }

    fn held_source(shard: &Shard) -> eg_types::MutationOutboxLease {
        commit(
            shard,
            &batch("source", "source-key", 0, "source-node"),
            None,
            &mut callback_budget,
        )
        .unwrap();
        shard.outbox_subscribe(GRAPH, CONSUMER, TOPIC).unwrap();
        let mut budget = OutboxClaimBudget::new(1, 1_000, 11).unwrap();
        let mut claims = shard
            .outbox_claim(GRAPH, CONSUMER, &mut budget)
            .unwrap()
            .claims;
        assert_eq!(claims.len(), 1);
        claims.remove(0)
    }

    fn has_node(shard: &Shard, node: &str) -> bool {
        let handle = shard.graph(GRAPH).unwrap();
        let read = shard.read(&handle).unwrap();
        let nodes = read.scoped_owner_table(crate::redb_store::NODES).unwrap();
        nodes.get((GRAPH, node)).unwrap().is_some()
    }

    fn callback_budget(write: &ShardWrite<'_>) -> Result<(), String> {
        crate::redb_store::enrichment_budget::seed_source_budget(
            write,
            GRAPH,
            &crate::redb_store::enrichment_budget::SourceBudgetAuthority {
                tenant_id: "tenant-a".into(),
                source_envelope: CALLBACK_SOURCE.into(),
                snapshot_digest: "a".repeat(64),
                repository_id: "repository".into(),
                policy_digest: "b".repeat(64),
                total_budget_units: 5,
                max_total_units: 5,
            },
            DurableCrypto::none(),
        )
    }

    fn assert_replacement_persisted(shard: &Shard) {
        assert!(has_node(shard, "replacement-node"));
        assert!(crate::redb_store::enrichment_budget::read(
            shard,
            GRAPH,
            CALLBACK_SOURCE,
            DurableCrypto::none(),
        )
        .unwrap()
        .is_some());
        assert_eq!(
            crate::redb_store::read_mutation_outbox(shard, GRAPH, "replacement")
                .unwrap()
                .len(),
            1
        );
    }

    #[test]
    fn held_delivery_replacement_and_callback_rows_commit_together() {
        let path = crate::redb_store::temp_path("outbox-reactivation", "atomic-success");
        let shard = Shard::open(&path).unwrap();
        let lease = held_source(&shard);
        let replacement = batch("replacement", "replacement-key", 1, "replacement-node");
        let result = commit(&shard, &replacement, Some(&lease), &mut callback_budget).unwrap();
        assert!(!result.replayed);
        drop(shard);

        let reopened = Shard::open(&path).unwrap();
        assert_replacement_persisted(&reopened);
        let cursor = reopened.outbox_cursor(GRAPH, CONSUMER).unwrap().unwrap();
        assert_eq!(cursor.batch_id, "source");
        drop(reopened);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn failed_callback_rolls_back_replacement_and_leaves_delivery_unacked() {
        let path = crate::redb_store::temp_path("outbox-reactivation", "atomic-rollback");
        let shard = Shard::open(&path).unwrap();
        let lease = held_source(&shard);
        let replacement = batch("replacement", "replacement-key", 1, "replacement-node");
        let error = commit(&shard, &replacement, Some(&lease), &mut |write| {
            callback_budget(write)?;
            Err("injected callback failure".into())
        })
        .unwrap_err();
        assert!(error.contains("injected callback failure"));
        drop(shard);

        let reopened = Shard::open(&path).unwrap();
        assert!(!has_node(&reopened, "replacement-node"));
        assert!(crate::redb_store::enrichment_budget::read(
            &reopened,
            GRAPH,
            CALLBACK_SOURCE,
            DurableCrypto::none(),
        )
        .unwrap()
        .is_none());
        assert!(
            crate::redb_store::read_mutation_outbox(&reopened, GRAPH, "replacement")
                .unwrap()
                .is_empty()
        );
        assert!(reopened.outbox_cursor(GRAPH, CONSUMER).unwrap().is_none());
        drop(reopened);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn lease_free_owner_rows_and_intent_replay_identically_on_two_shards() {
        let path_a = crate::redb_store::temp_path("outbox-reactivation", "replica-a");
        let path_b = crate::redb_store::temp_path("outbox-reactivation", "replica-b");
        let first = Shard::open(&path_a).unwrap();
        let second = Shard::open(&path_b).unwrap();
        let replacement = batch("replacement", "replacement-key", 0, "replacement-node");
        let a = commit_owner_rows(&first, &replacement, &mut callback_budget).unwrap();
        let b = commit_owner_rows(&second, &replacement, &mut callback_budget).unwrap();
        assert_eq!(
            rmp_serde::to_vec_named(&a.record).unwrap(),
            rmp_serde::to_vec_named(&b.record).unwrap()
        );
        assert!(!a.replayed);
        assert!(!b.replayed);
        // A retry carries a fresh attempt nonce: re-presenting the consumed
        // nonce is refused as REPLAY_NONCE_CONSUMED, not replayed.
        let retry = batch("replacement", "replacement-key", 0, "replacement-node");
        let replay = commit_owner_rows(&second, &retry, &mut |_| {
            Err("callback must not run on exact replay".into())
        })
        .unwrap();
        assert!(replay.replayed);
        drop(first);
        drop(second);

        for path in [&path_a, &path_b] {
            let reopened = Shard::open(path).unwrap();
            assert_replacement_persisted(&reopened);
            drop(reopened);
            let _ = std::fs::remove_file(path);
        }
    }
}
