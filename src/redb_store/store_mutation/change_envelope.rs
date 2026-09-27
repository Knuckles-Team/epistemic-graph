use super::*;

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
            rows: BatchRowInput {
                graph_fname,
                batch: &envelope.mutation,
                change: Some(envelope),
                source_budget,
                authoritative_state_msgpack: None,
                committed_at_ms,
                // No authoritative state, so audited is inert here.
                audited: true,
                crashpoint: None,
            },
            crossmodal: None,
            result_msgpack: None,
        },
        crypto,
        #[cfg(feature = "security")]
        audit_tail,
    )?;
    let outbox_count = change_envelope_outbox_count(envelope)?;
    Ok(change_envelope_commit(
        envelope,
        outbox_count,
        mutation.replayed,
    ))
}

fn change_envelope_outbox_count(envelope: &ChangeEnvelope) -> Result<u32, String> {
    envelope
        .mutation
        .operations
        .len()
        .checked_add(envelope.mutation.outbox.len())
        .and_then(|count| count.checked_add(1))
        .and_then(|count| u32::try_from(count).ok())
        .ok_or_else(|| "change envelope outbox count overflow".to_string())
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
            change_envelope_outbox_count(envelope)
                .map_err(|error| change_envelope_error(index, error))
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
            rows: BatchRowInput {
                graph_fname: input.graph_fname,
                batch: &input.current_batches[1],
                change: Some(input.envelope),
                source_budget: None,
                authoritative_state_msgpack: None,
                committed_at_ms: input.committed_at_ms,
                audited: true,
                crashpoint: None,
            },
            crossmodal: None,
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
