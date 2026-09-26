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
            source_budget: None,
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
            source_budget: None,
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
    commit_change_envelope_with_budget(
        shard,
        graph_fname,
        envelope,
        None,
        committed_at_ms,
        crypto,
        #[cfg(feature = "security")]
        audit_tail,
    )
}

pub(crate) fn commit_change_envelope_with_budget(
    shard: &Shard,
    graph_fname: &str,
    envelope: &ChangeEnvelope,
    source_budget: Option<&crate::redb_store::enrichment_budget::SourceBudgetAuthority>,
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
            source_budget,
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
            source_budget: None,
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
            source_budget: None,
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
    pub(crate) source_budget:
        Option<&'a crate::redb_store::enrichment_budget::SourceBudgetAuthority>,
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

/// Private opt-in for an enrichment reactivation. The callback stages the
/// budget/park CAS in the same owner-row admission as the canonical batch.
/// The batch must contain its replacement outbox intent. This entry point is
/// intentionally local to a graph shard: no Raft proposal is represented.
pub(crate) fn commit_mutation_batch_with_outbox_lease(
    shard: &Shard,
    input: BatchCommitInput<'_>,
    lease: &eg_types::MutationOutboxLease,
    stage_reactivation: &mut dyn for<'g> FnMut(&ShardWrite<'g>) -> Result<(), String>,
    crypto: DurableCrypto<'_>,
    #[cfg(feature = "security")] audit_tail: &mut AuditTailCache,
) -> Result<MutationBatchCommit, String> {
    commit_mutation_batch_inner_with_effect(
        shard,
        input,
        Some(OutboxBatchEffect {
            lease: Some(lease),
            #[cfg(feature = "raft")]
            top_up: None,
            stage_reactivation,
        }),
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
    stage_reactivation: &mut dyn for<'g> FnMut(&ShardWrite<'g>) -> Result<(), String>,
    crypto: DurableCrypto<'_>,
    #[cfg(feature = "security")] audit_tail: &mut AuditTailCache,
) -> Result<MutationBatchCommit, String> {
    commit_mutation_batch_inner_with_effect(
        shard,
        input,
        Some(OutboxBatchEffect {
            lease: None,
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
        BatchCommitInput {
            graph_fname: graph,
            batch: &transition.replacement_batch,
            change: None,
            source_budget: None,
            authoritative_state_msgpack: None,
            crossmodal: None,
            result_msgpack: Some(&receipt),
            committed_at_ms,
            audited: true,
            crashpoint: None,
        },
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
    stage_reactivation: &'a mut dyn for<'g> FnMut(&ShardWrite<'g>) -> Result<(), String>,
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
    mut outbox_effect: Option<OutboxBatchEffect<'_>>,
    crypto: DurableCrypto<'_>,
    #[cfg(feature = "security")] audit_tail: &mut AuditTailCache,
) -> Result<MutationBatchCommit, String> {
    let BatchCommitInput {
        graph_fname,
        batch,
        change,
        source_budget,
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
    let members = vec![(graph_fname.to_string(), Arc::clone(&handle))];
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
        if outbox_effect
            .as_ref()
            .is_some_and(|effect| effect.lease.is_some())
        {
            shard.mutations().abort_group(group)?;
            return Err(
                "CONFLICT: reactivation batch already committed; delivery state must be reconciled"
                    .into(),
            );
        }
        // A byte-identical retry still has to commit the admitted group: the
        // replay member's fresh attempt nonce is consumed only by the commit
        // finalizer. `commit_batch` finishes the control member and seals the
        // replay member without reapplying owner rows.
        let Begin::Replay(record) = group.begun(1)?.clone() else {
            return Err("admitted replay lost its receipt".to_string());
        };
        #[cfg(feature = "raft")]
        if let Some(top_up) = top_up {
            let transition = top_up.transition;
            let proposed_batch = rmp_serde::to_vec_named(&bound)
                .map_err(|_| "CONFLICT: enrichment top-up retry encode failed")?;
            let committed_batch = rmp_serde::to_vec_named(&record.batch)
                .map_err(|_| "CONFLICT: enrichment top-up receipt encode failed")?;
            if proposed_batch != committed_batch
                || record.result_msgpack.as_deref() != result_msgpack
                || record.committed_at_ms != committed_at_ms
            {
                shard.mutations().abort_group(group)?;
                return Err("IDEMPOTENCY_CONFLICT: enrichment top-up retry changed".into());
            }
            if let Err(error) = shard.outbox_supersession_receipt_batch_record(
                &group,
                &handle,
                &transition.consumer,
                &transition.old_delivery,
                committed_at_ms,
            ) {
                shard.mutations().abort_group(group)?;
                return Err(error);
            }
            if let Err(error) = crate::redb_store::enrichment_budget::verify_reactivation_replay(
                shard,
                graph_fname,
                &transition.expected_budget,
                &transition.expected_park,
                &transition.replacement_budget,
                &transition.revision,
                crypto,
            ) {
                shard.mutations().abort_group(group)?;
                return Err(error);
            }
        }
        return finish_replayed_batch(
            shard,
            group,
            &batches,
            &bound,
            result_msgpack,
            committed_at_ms,
        );
    }

    if let Some(effect) = outbox_effect.as_ref().and_then(|effect| effect.lease) {
        if let Err(error) =
            shard.outbox_validate_batch_lease(&group, &handle, effect, committed_at_ms)
        {
            shard.mutations().abort_group(group)?;
            return Err(error);
        }
    }

    let staged_input = StagedRowInput {
        graph_fname,
        batch: &bound,
        change,
        source_budget,
        authoritative_state_msgpack,
        crossmodal: crossmodal.as_ref(),
        committed_at_ms,
        audited,
        crashpoint,
    };
    let staged_rows = if let Some(effect) = outbox_effect.as_mut() {
        stage_mutation_batch_rows_with_reactivation(
            shard,
            &group,
            &members,
            &batches,
            staged_input,
            effect.stage_reactivation,
            crypto,
            #[cfg(feature = "security")]
            &mut staged_audit_tail,
        )
    } else {
        stage_mutation_batch_rows(
            shard,
            &group,
            &members,
            &batches,
            staged_input,
            crypto,
            #[cfg(feature = "security")]
            &mut staged_audit_tail,
        )
    };
    let staged = match staged_rows {
        Ok(staged) => staged,
        Err(error) => {
            shard.mutations().abort_group(group)?;
            return Err(error);
        }
    };

    if let Some(effect) = outbox_effect.as_ref().and_then(|effect| effect.lease) {
        if let Err(error) = shard.outbox_ack_batch_lease(&group, &handle, effect, committed_at_ms) {
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
            committed_at_ms,
        ) {
            shard.mutations().abort_group(group)?;
            return Err(error);
        }
    }

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

fn stage_mutation_batch_rows_with_reactivation(
    shard: &Shard,
    group: &AdmittedGroup<'_, GraphShardOwner>,
    members: &[(String, Arc<OwnedStoreHandle<GraphShardOwner>>)],
    batches: &[MutationBatch],
    input: StagedRowInput<'_>,
    stage_reactivation: &mut dyn for<'g> FnMut(&ShardWrite<'g>) -> Result<(), String>,
    crypto: DurableCrypto<'_>,
    #[cfg(feature = "security")] staged_audit_tail: &mut AuditTailCache,
) -> Result<StagedMutationRows, String> {
    let write = ShardWrite::open(shard, group, members, batches)?;
    let staged = stage_rows_in(
        &write,
        input,
        crypto,
        #[cfg(feature = "security")]
        staged_audit_tail,
    )
    .and_then(|staged| {
        stage_reactivation(&write)?;
        Ok(staged)
    });
    let finished = write.finish();
    match (staged, finished) {
        (Ok(staged), Ok(())) => Ok(staged),
        (Err(error), _) | (Ok(_), Err(error)) => Err(error),
    }
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
        stage: &mut dyn for<'g> FnMut(&ShardWrite<'g>) -> Result<(), String>,
    ) -> Result<MutationBatchCommit, String> {
        #[cfg(feature = "security")]
        let mut audit_tail = AuditTailCache::new();
        let input = BatchCommitInput {
            graph_fname: GRAPH,
            batch,
            change: None,
            source_budget: None,
            authoritative_state_msgpack: None,
            crossmodal: None,
            result_msgpack: None,
            committed_at_ms: if lease.is_some() { 12 } else { 10 },
            audited: true,
            crashpoint: None,
        };
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
        stage: &mut dyn for<'g> FnMut(&ShardWrite<'g>) -> Result<(), String>,
    ) -> Result<MutationBatchCommit, String> {
        #[cfg(feature = "security")]
        let mut audit_tail = AuditTailCache::new();
        commit_mutation_batch_with_reactivation_rows(
            shard,
            BatchCommitInput {
                graph_fname: GRAPH,
                batch,
                change: None,
                source_budget: None,
                authoritative_state_msgpack: None,
                crossmodal: None,
                result_msgpack: None,
                committed_at_ms: 10,
                audited: true,
                crashpoint: None,
            },
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
        assert!(has_node(&reopened, "replacement-node"));
        assert!(crate::redb_store::enrichment_budget::read(
            &reopened,
            GRAPH,
            CALLBACK_SOURCE,
            DurableCrypto::none(),
        )
        .unwrap()
        .is_some());
        assert_eq!(
            crate::redb_store::read_mutation_outbox(&reopened, GRAPH, "replacement")
                .unwrap()
                .len(),
            1
        );
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
        let replay = commit_owner_rows(&second, &replacement, &mut |_| {
            Err("callback must not run on exact replay".into())
        })
        .unwrap();
        assert!(replay.replayed);
        drop(first);
        drop(second);

        for path in [&path_a, &path_b] {
            let reopened = Shard::open(path).unwrap();
            assert!(has_node(&reopened, "replacement-node"));
            assert!(crate::redb_store::enrichment_budget::read(
                &reopened,
                GRAPH,
                CALLBACK_SOURCE,
                DurableCrypto::none(),
            )
            .unwrap()
            .is_some());
            assert_eq!(
                crate::redb_store::read_mutation_outbox(&reopened, GRAPH, "replacement")
                    .unwrap()
                    .len(),
                1
            );
            drop(reopened);
            let _ = std::fs::remove_file(path);
        }
    }
}
