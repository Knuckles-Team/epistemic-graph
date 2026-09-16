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
        BatchCommitInput {
            graph_fname,
            batch,
            change: None,
            authoritative_state_msgpack: None,
            crossmodal: None,
            result_msgpack,
            committed_at_ms,
            // Compact-row batches never carry `authoritative_state`, so this is
            // inert (see `commit_mutation_batch_inner`).
            audited: true,
            crashpoint: None,
        },
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
            graph_fname: input.graph_fname,
            batch: input.batch,
            change: None,
            authoritative_state_msgpack: Some(input.authoritative_state_msgpack),
            crossmodal: None,
            result_msgpack: input.result_msgpack,
            committed_at_ms: input.committed_at_ms,
            audited: input.audited,
            crashpoint: None,
        },
        crypto,
        #[cfg(feature = "security")]
        audit_tail,
    )
}

/// Engine-native ChangeEnvelope commit. Graph rows, every material/governance
/// projection, version/cursor fences, terminal batch/envelope records, and the
/// CDC outbox are written by one redb transaction and one durability barrier.
pub(crate) fn commit_change_envelope(
    shard: &Shard,
    graph_fname: &str,
    envelope: &ChangeEnvelope,
    committed_at_ms: u64,
    crypto: DurableCrypto<'_>,
    #[cfg(feature = "security")] audit_tail: &mut AuditTailCache,
) -> Result<ChangeEnvelopeCommit, String> {
    envelope.validate()?;
    let mutation = commit_mutation_batch_inner(
        shard,
        BatchCommitInput {
            graph_fname,
            batch: &envelope.mutation,
            change: Some(envelope),
            authoritative_state_msgpack: None,
            crossmodal: None,
            result_msgpack: None,
            committed_at_ms,
            // No `authoritative_state`, so `audited` is inert here.
            audited: true,
            crashpoint: None,
        },
        crypto,
        #[cfg(feature = "security")]
        audit_tail,
    )?;
    let outbox_count = envelope
        .mutation
        .operations
        .len()
        .checked_add(envelope.mutation.outbox.len())
        .and_then(|count| count.checked_add(1))
        .and_then(|count| u32::try_from(count).ok())
        .ok_or_else(|| "change envelope outbox count overflow".to_string())?;
    Ok(ChangeEnvelopeCommit {
        envelope_id: envelope.envelope_id.clone(),
        batch_id: envelope.mutation.batch_id.clone(),
        content_version: envelope.content_version.clone(),
        cursor: envelope.cursor.clone(),
        outbox_count,
        replayed: mutation.replayed,
    })
}

/// A ChangeEnvelope batch aborted at `index` (the first envelope that failed its
/// idempotency/version/cursor/fence check or row projection). Because every envelope
/// for one graph shares ONE atomic transaction, the abort rolls back the whole
/// group — no envelope in it commits — and the caller reports the batch outcome per
/// envelope honestly.
#[derive(Debug, Clone)]
pub(crate) struct ChangeEnvelopesError {
    pub(crate) index: usize,
    pub(crate) error: String,
}

/// Engine-native BATCH ChangeEnvelope commit: apply EVERY envelope in `envelopes`
/// (all of which must target `graph_fname`) into ONE redb transaction and one
/// durability barrier (CONCEPT:EG-KG.ingest.batched-change-envelopes). The envelopes
/// are applied in order; read-your-writes inside the shared transaction chains each
/// envelope's content-version, cursor, and +1 graph-version onto the previous one,
/// so a page of records built with sequential `expected_graph_version`s commits as a
/// single fsync instead of N.
///
/// Atomicity is per graph-batch: the first envelope that fails a check aborts the
/// whole transaction (nothing in this group commits) and returns [`ChangeEnvelopesError`]
/// naming the offending index. An idempotent replay with a fresh attempt nonce is
/// NOT a failure — it is reported per envelope via `ChangeEnvelopeCommit::replayed` and the
/// transaction still commits its non-replayed siblings. When every envelope is a
/// replay, nothing was written and the transaction is dropped without an fsync,
/// exactly like the single-envelope path.
pub(crate) fn commit_change_envelopes(
    shard: &Shard,
    graph_fname: &str,
    envelopes: &[ChangeEnvelope],
    committed_at_ms: u64,
    crypto: DurableCrypto<'_>,
    #[cfg(feature = "security")] audit_tail: &mut AuditTailCache,
) -> Result<Vec<ChangeEnvelopeCommit>, ChangeEnvelopesError> {
    validate_change_envelope_batch_size(envelopes)?;
    if envelopes.is_empty() {
        return Ok(Vec::new());
    }
    let prepared = prepare_change_envelopes(shard, graph_fname, envelopes)?;
    let start = start_change_envelope_group(
        shard,
        graph_fname,
        &prepared.handle,
        &prepared.bound,
        envelopes,
        &prepared.outbox_counts,
    )?;
    let session = match start {
        ChangeEnvelopeStart::AllReplayed(commits) => return Ok(commits),
        ChangeEnvelopeStart::Active(session) => *session,
    };
    #[cfg(feature = "security")]
    let mut staged_audit_tail = audit_tail.clone();
    let session = process_change_envelopes(
        shard,
        ChangeEnvelopeBatch {
            graph_fname,
            envelopes,
            bound: &prepared.bound,
            outbox_counts: &prepared.outbox_counts,
            members: &prepared.members,
        },
        session,
        committed_at_ms,
        crypto,
        #[cfg(feature = "security")]
        &mut staged_audit_tail,
    )?;
    finish_change_envelope_group(
        shard,
        session,
        #[cfg(feature = "security")]
        audit_tail,
        #[cfg(feature = "security")]
        staged_audit_tail,
    )
}

fn validate_change_envelope_batch_size(
    envelopes: &[ChangeEnvelope],
) -> Result<(), ChangeEnvelopesError> {
    let max_batch = crate::change_envelope::MAX_ENVELOPES_PER_BATCH;
    if envelopes.len() > max_batch {
        return Err(change_envelope_error(
            0,
            format!(
                "CHANGE_BATCH_TOO_LARGE: {} envelopes exceed the {max_batch} cap",
                envelopes.len()
            ),
        ));
    }
    Ok(())
}

fn change_envelope_error(index: usize, error: String) -> ChangeEnvelopesError {
    ChangeEnvelopesError { index, error }
}

struct PreparedChangeEnvelopes {
    handle: Arc<OwnedStoreHandle<GraphShardOwner>>,
    bound: Vec<MutationBatch>,
    members: Vec<(String, Arc<OwnedStoreHandle<GraphShardOwner>>)>,
    outbox_counts: Vec<u32>,
}

fn prepare_change_envelopes(
    shard: &Shard,
    graph_fname: &str,
    envelopes: &[ChangeEnvelope],
) -> Result<PreparedChangeEnvelopes, ChangeEnvelopesError> {
    let handle = shard
        .graph(graph_fname)
        .map_err(|error| change_envelope_error(0, error))?;
    let bound = envelopes
        .iter()
        .enumerate()
        .map(|(index, envelope)| {
            envelope
                .validate()
                .map_err(|error| change_envelope_error(index, error))?;
            shard::bind_caller_batch(handle.as_ref(), graph_fname, &envelope.mutation)
                .map_err(|error| change_envelope_error(index, error))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let outbox_counts = envelopes
        .iter()
        .enumerate()
        .map(|(index, envelope)| {
            envelope
                .mutation
                .operations
                .len()
                .checked_add(envelope.mutation.outbox.len())
                .and_then(|count| count.checked_add(1))
                .and_then(|count| u32::try_from(count).ok())
                .ok_or_else(|| {
                    change_envelope_error(
                        index,
                        "change envelope outbox count overflow".to_string(),
                    )
                })
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(PreparedChangeEnvelopes {
        members: vec![(graph_fname.to_string(), Arc::clone(&handle))],
        handle,
        bound,
        outbox_counts,
    })
}

struct ChangeEnvelopeSession<'a> {
    group: AdmittedGroup<'a, GraphShardOwner>,
    first_batches: Vec<MutationBatch>,
    first_control_begin: Begin,
    first_graph_begin: Begin,
    first_fresh: usize,
    commits: Vec<ChangeEnvelopeCommit>,
    control_version: u64,
    final_control_batch: MutationBatch,
    final_graph_batch: MutationBatch,
}

struct ChangeEnvelopeBatch<'a> {
    graph_fname: &'a str,
    envelopes: &'a [ChangeEnvelope],
    bound: &'a [MutationBatch],
    outbox_counts: &'a [u32],
    members: &'a [(String, Arc<OwnedStoreHandle<GraphShardOwner>>)],
}

struct ChangeEnvelopeRowInput<'a> {
    graph_fname: &'a str,
    members: &'a [(String, Arc<OwnedStoreHandle<GraphShardOwner>>)],
    current_batches: &'a [MutationBatch],
    envelope: &'a ChangeEnvelope,
    committed_at_ms: u64,
    control_source: Option<u64>,
    graph_source: Option<u64>,
}

enum ChangeEnvelopeStart<'a> {
    AllReplayed(Vec<ChangeEnvelopeCommit>),
    Active(Box<ChangeEnvelopeSession<'a>>),
}

fn start_change_envelope_group<'a>(
    shard: &'a Shard,
    graph_fname: &'a str,
    handle: &'a Arc<OwnedStoreHandle<GraphShardOwner>>,
    bound: &'a [MutationBatch],
    envelopes: &'a [ChangeEnvelope],
    outbox_counts: &'a [u32],
) -> Result<ChangeEnvelopeStart<'a>, ChangeEnvelopesError> {
    let mut first_fresh = 0usize;
    let mut commits = Vec::with_capacity(envelopes.len());
    loop {
        let (group, batches) = shard
            .admit_batch(
                graph_fname,
                handle,
                &bound[first_fresh],
                &bound[first_fresh].batch_id,
            )
            .map_err(|error| change_envelope_error(first_fresh, error))?;
        let control_begin = group
            .begun(0)
            .map_err(|error| change_envelope_error(first_fresh, error))?
            .clone();
        let graph_begin = group
            .begun(1)
            .map_err(|error| change_envelope_error(first_fresh, error))?
            .clone();
        match graph_begin {
            Begin::Replay(_) => {
                shard
                    .mutations()
                    .abort_group(group)
                    .map_err(|error| change_envelope_error(first_fresh, error))?;
                commits.push(change_envelope_commit(
                    &envelopes[first_fresh],
                    outbox_counts[first_fresh],
                    true,
                ));
                first_fresh += 1;
                if first_fresh == envelopes.len() {
                    return Ok(ChangeEnvelopeStart::AllReplayed(commits));
                }
            }
            Begin::Apply { .. } => {
                if !matches!(&control_begin, Begin::Apply { .. }) {
                    shard
                        .mutations()
                        .abort_group(group)
                        .map_err(|error| change_envelope_error(first_fresh, error))?;
                    return Err(change_envelope_error(
                        first_fresh,
                        "the shard control member unexpectedly replayed for a fresh envelope"
                            .to_string(),
                    ));
                }
                let control_version = match &control_begin {
                    Begin::Apply { source_version } => {
                        source_version.unwrap_or(0).saturating_add(1)
                    }
                    Begin::Replay(_) => unreachable!("fresh graph member requires fresh control"),
                };
                return Ok(ChangeEnvelopeStart::Active(Box::new(
                    ChangeEnvelopeSession {
                        group,
                        first_batches: batches.clone(),
                        first_control_begin: control_begin,
                        first_graph_begin: graph_begin,
                        first_fresh,
                        commits,
                        control_version,
                        final_control_batch: batches[0].clone(),
                        final_graph_batch: batches[1].clone(),
                    },
                )));
            }
        }
    }
}

fn change_envelope_commit(
    envelope: &ChangeEnvelope,
    outbox_count: u32,
    replayed: bool,
) -> ChangeEnvelopeCommit {
    ChangeEnvelopeCommit {
        envelope_id: envelope.envelope_id.clone(),
        batch_id: envelope.mutation.batch_id.clone(),
        content_version: envelope.content_version.clone(),
        cursor: envelope.cursor.clone(),
        outbox_count,
        replayed,
    }
}

fn process_change_envelopes<'a>(
    shard: &Shard,
    batch: ChangeEnvelopeBatch<'_>,
    mut session: ChangeEnvelopeSession<'a>,
    committed_at_ms: u64,
    crypto: DurableCrypto<'_>,
    #[cfg(feature = "security")] staged_audit_tail: &mut AuditTailCache,
) -> Result<ChangeEnvelopeSession<'a>, ChangeEnvelopesError> {
    for index in session.first_fresh..batch.envelopes.len() {
        if let Err(error) = process_change_envelope_at(
            shard,
            &batch,
            &mut session,
            index,
            committed_at_ms,
            crypto,
            #[cfg(feature = "security")]
            staged_audit_tail,
        ) {
            return abort_change_envelope_session(shard, session, error);
        }
    }
    Ok(session)
}

struct ChangeEnvelopeFailure {
    index: usize,
    error: String,
}

fn process_change_envelope_at(
    shard: &Shard,
    batch: &ChangeEnvelopeBatch<'_>,
    session: &mut ChangeEnvelopeSession<'_>,
    index: usize,
    committed_at_ms: u64,
    crypto: DurableCrypto<'_>,
    #[cfg(feature = "security")] staged_audit_tail: &mut AuditTailCache,
) -> Result<(), ChangeEnvelopeFailure> {
    let graph_begin = change_graph_begin(session, index, batch.bound)
        .map_err(|error| ChangeEnvelopeFailure { index, error })?;
    if matches!(graph_begin, Begin::Replay(_)) {
        session.commits.push(change_envelope_commit(
            &batch.envelopes[index],
            batch.outbox_counts[index],
            true,
        ));
        return Ok(());
    }
    let graph_source = match graph_begin {
        Begin::Apply { source_version } => source_version,
        Begin::Replay(_) => unreachable!("replay handled above"),
    };
    let (control_batch, control_source) =
        change_control_batch(shard, batch.graph_fname, session, index, batch.bound)
            .map_err(|error| ChangeEnvelopeFailure { index, error })?;
    let current_batches = vec![control_batch, batch.bound[index].clone()];
    finish_change_envelope_rows(
        shard,
        session,
        ChangeEnvelopeRowInput {
            graph_fname: batch.graph_fname,
            members: batch.members,
            current_batches: &current_batches,
            envelope: &batch.envelopes[index],
            committed_at_ms,
            control_source,
            graph_source,
        },
        crypto,
        #[cfg(feature = "security")]
        staged_audit_tail,
    )
    .map_err(|error| ChangeEnvelopeFailure { index, error })?;
    session.control_version = control_source.unwrap_or(0).saturating_add(1);
    session.final_control_batch = current_batches[0].clone();
    session.final_graph_batch = current_batches[1].clone();
    session.commits.push(change_envelope_commit(
        &batch.envelopes[index],
        batch.outbox_counts[index],
        false,
    ));
    Ok(())
}

fn change_graph_begin(
    session: &ChangeEnvelopeSession<'_>,
    index: usize,
    bound: &[MutationBatch],
) -> Result<Begin, String> {
    if index == session.first_fresh {
        return Ok(session.first_graph_begin.clone());
    }
    session
        .group
        .member(1)
        .and_then(|member| member.begin(&bound[index]))
}

fn change_control_batch(
    shard: &Shard,
    graph_fname: &str,
    session: &ChangeEnvelopeSession<'_>,
    index: usize,
    bound: &[MutationBatch],
) -> Result<(MutationBatch, Option<u64>), String> {
    if index == session.first_fresh {
        let source_version = match &session.first_control_begin {
            Begin::Apply { source_version } => *source_version,
            Begin::Replay(_) => unreachable!("fresh graph member requires fresh control"),
        };
        return Ok((session.first_batches[0].clone(), source_version));
    }
    let control_batch = shard.maintenance_batch_at(
        &format!("change-envelope/{graph_fname}/{}", bound[index].batch_id),
        session.control_version,
    )?;
    let control_begin = session.group.control().begin(&control_batch)?;
    let source_version = match control_begin {
        Begin::Apply { source_version } => source_version,
        Begin::Replay(_) => {
            return Err(
                "the shard control member unexpectedly replayed for a fresh envelope".to_string(),
            )
        }
    };
    Ok((control_batch, source_version))
}

fn finish_change_envelope_rows(
    shard: &Shard,
    session: &ChangeEnvelopeSession<'_>,
    input: ChangeEnvelopeRowInput<'_>,
    crypto: DurableCrypto<'_>,
    #[cfg(feature = "security")] staged_audit_tail: &mut AuditTailCache,
) -> Result<(), String> {
    let staged = stage_mutation_batch_rows(
        shard,
        &session.group,
        input.members,
        input.current_batches,
        StagedRowInput {
            graph_fname: input.graph_fname,
            batch: &input.current_batches[1],
            change: Some(input.envelope),
            authoritative_state_msgpack: None,
            crossmodal: None,
            committed_at_ms: input.committed_at_ms,
            audited: true,
            crashpoint: None,
        },
        crypto,
        #[cfg(feature = "security")]
        staged_audit_tail,
    )?;
    shard.mutations().finish(
        session.group.control(),
        &input.current_batches[0],
        None,
        input.committed_at_ms,
        input.control_source,
    )?;
    shard.mutations().finish(
        session.group.member(1)?,
        &input.current_batches[1],
        staged.generated_result,
        input.committed_at_ms,
        input.graph_source,
    )?;
    Ok(())
}

fn abort_change_envelope_session<'a>(
    shard: &Shard,
    session: ChangeEnvelopeSession<'a>,
    failure: ChangeEnvelopeFailure,
) -> Result<ChangeEnvelopeSession<'a>, ChangeEnvelopesError> {
    match shard.mutations().abort_group(session.group) {
        Ok(()) => Err(change_envelope_error(failure.index, failure.error)),
        Err(abort) => Err(change_envelope_error(
            failure.index,
            format!("{}; abort failed: {abort}", failure.error),
        )),
    }
}

fn finish_change_envelope_group<'a>(
    shard: &Shard,
    session: ChangeEnvelopeSession<'a>,
    #[cfg(feature = "security")] audit_tail: &mut AuditTailCache,
    #[cfg(feature = "security")] staged_audit_tail: AuditTailCache,
) -> Result<Vec<ChangeEnvelopeCommit>, ChangeEnvelopesError> {
    let ChangeEnvelopeSession {
        group,
        first_fresh,
        commits,
        final_control_batch,
        final_graph_batch,
        ..
    } = session;
    let final_batches = [final_control_batch, final_graph_batch];
    let commit_refs: Vec<&MutationBatch> = final_batches.iter().collect();
    if let Err(error) = shard.mutations().commit_group(group, &commit_refs) {
        return Err(change_envelope_error(first_fresh, error));
    }
    #[cfg(feature = "security")]
    {
        *audit_tail = staged_audit_tail;
    }
    Ok(commits)
}

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
            graph_fname: input.graph_fname,
            batch: input.batch,
            change: None,
            authoritative_state_msgpack: None,
            crossmodal: Some(input.rows),
            result_msgpack: input.result_msgpack,
            committed_at_ms: input.committed_at_ms,
            // No `authoritative_state`, so `audited` is inert here.
            audited: true,
            crashpoint: None,
        },
        crypto,
        #[cfg(feature = "security")]
        audit_tail,
    )
}

/// The inputs every batch-commit entry point shares, bundled so the phases
/// below stay inside the argument cap without positional strings.
pub(crate) struct BatchCommitInput<'a> {
    pub(crate) graph_fname: &'a str,
    pub(crate) batch: &'a MutationBatch,
    pub(crate) change: Option<&'a ChangeEnvelope>,
    pub(crate) authoritative_state_msgpack: Option<&'a [u8]>,
    pub(crate) crossmodal: Option<CrossModalBatchRows<'a>>,
    pub(crate) result_msgpack: Option<&'a [u8]>,
    pub(crate) committed_at_ms: u64,
    /// Whether THIS commit appends tamper-evident audit-chain entries. Only
    /// consulted on the authoritative-state branches; see the note below on why
    /// it cannot be re-derived from `batch.operations`.
    pub(crate) audited: bool,
    pub(crate) crashpoint: Option<MutationBatchCrashpoint>,
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
    let BatchCommitInput {
        graph_fname,
        batch,
        change,
        authoritative_state_msgpack,
        crossmodal,
        result_msgpack,
        committed_at_ms,
        audited,
        crashpoint,
    } = input;
    // Audit-tail updates are staged alongside the transaction. Advancing the
    // process cache before the commit would create a false tail when an
    // injected or real failure drops it.
    #[cfg(feature = "security")]
    let mut staged_audit_tail = audit_tail.clone();

    // The compiler preserves the verified caller scope on the batch because it
    // is part of the request authority. Bind exactly once at the physical
    // shard boundary: the ledger identity and serving principal belong to the
    // shard, while the caller authority and outbox attribution remain in the
    // rebound batch. Binding a cold graph opens its own transaction, so it must
    // happen before this group's admission.
    let handle = shard.graph(graph_fname)?;
    let bound = shard::bind_caller_batch(handle.as_ref(), graph_fname, batch)?;
    let members = vec![(graph_fname.to_string(), Arc::clone(&handle))];
    let (group, batches) = shard.admit_batch(graph_fname, &handle, &bound, &bound.batch_id)?;

    if matches!(group.begun(1)?, Begin::Replay(_)) {
        // A byte-identical retry still has to commit the admitted group: the
        // replay member's fresh attempt nonce is consumed only by the commit
        // finalizer. `commit_batch` finishes the control member and seals the
        // replay member without reapplying owner rows.
        let Begin::Replay(_record) = group.begun(1)?.clone() else {
            return Err("admitted replay lost its receipt".to_string());
        };
        return finish_replayed_batch(
            shard,
            group,
            &batches,
            &bound,
            result_msgpack,
            committed_at_ms,
        );
    }

    let staged = match stage_mutation_batch_rows(
        shard,
        &group,
        &members,
        &batches,
        StagedRowInput {
            graph_fname,
            batch: &bound,
            change,
            authoritative_state_msgpack,
            crossmodal: crossmodal.as_ref(),
            committed_at_ms,
            audited,
            crashpoint,
        },
        crypto,
        #[cfg(feature = "security")]
        &mut staged_audit_tail,
    ) {
        Ok(staged) => staged,
        Err(error) => {
            shard.mutations().abort_group(group)?;
            return Err(error);
        }
    };

    run_mutation_batch_crashpoint(&bound, crashpoint, MutationBatchCrashpoint::BeforeCommit)?;
    crate::mutation_batch::apply_certification_fault(
        &bound,
        crate::mutation_batch::MutationCommitPhase::BeforeCommit,
    )?;

    let result = staged
        .generated_result
        .or_else(|| result_msgpack.map(ToOwned::to_owned));
    let committed = shard.commit_batch(group, &batches, result, committed_at_ms)?;

    #[cfg(feature = "security")]
    {
        *audit_tail = staged_audit_tail;
    }

    run_mutation_batch_crashpoint(
        &bound,
        crashpoint,
        MutationBatchCrashpoint::AfterCommitBeforeAck,
    )?;
    crate::mutation_batch::apply_certification_fault(
        &bound,
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
