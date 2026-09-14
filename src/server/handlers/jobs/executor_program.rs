//! Private implementation module for the analytics job handler.

use super::prelude_jobs::*;
use super::prelude_server::*;
use super::prelude_std::*;
use super::*;

#[cfg(feature = "program-optimization")]
#[allow(clippy::too_many_arguments)]
pub(super) async fn execute_program_claim(
    state: &Arc<RwLock<ServerState>>,
    store: &Arc<JobStore>,
    job: &eg_jobs::AnalyticsJob,
    worker_ref: &str,
    epoch: u64,
    payload: Vec<u8>,
    request_msgpack: Vec<u8>,
) -> Result<(), String> {
    let request: OptimizationRequest = eg_types::msgpack::decode_bounded(
        &request_msgpack,
        eg_types::msgpack::MsgpackLimits::new(
            MAX_JOB_INPUT_BYTES,
            MAX_JOB_INPUT_ITEMS,
            eg_types::msgpack::DEFAULT_MAX_DEPTH,
        ),
    )
    .map_err(|_| "program optimization input decoding failed".to_string())?;
    store
        .checkpoint_fenced(
            &job.job_id,
            worker_ref,
            epoch,
            Checkpoint {
                progress: 0.1,
                stage: "optimizing".to_string(),
                state_blob: Some(payload),
                updated_at_ms: unix_ms(),
            },
            unix_ms(),
        )
        .map_err(|error| error.to_string())?;

    let promotion_program = request.program.clone();
    let promotion_corpus = request.corpus.clone();
    let promotion_seed = request.budget.seed;
    let Some(optimization) =
        run_program_kernel(store, &job.job_id, worker_ref, epoch, request).await?
    else {
        return Ok(());
    };

    let promotion_identity = program_promotion_identity(
        state,
        job,
        &optimization,
        &promotion_program,
        &promotion_corpus,
        promotion_seed,
    )
    .await?;

    let now = unix_ms();
    store
        .checkpoint_fenced(
            &job.job_id,
            worker_ref,
            epoch,
            Checkpoint {
                progress: 0.9,
                stage: "computed".to_string(),
                state_blob: None,
                updated_at_ms: now,
            },
            now,
        )
        .map_err(|error| error.to_string())?;
    let result = typed_program_result(
        job,
        &optimization,
        promotion_identity.as_ref(),
        &promotion_program.policy,
    )?;
    let staged = store
        .stage_result_fenced(&job.job_id, worker_ref, epoch, result, unix_ms())
        .map_err(|error| error.to_string())?;
    publish_staged_result(state, store, staged, worker_ref, epoch).await
}

#[cfg(feature = "program-optimization")]
async fn program_promotion_identity(
    state: &Arc<RwLock<ServerState>>,
    job: &eg_jobs::AnalyticsJob,
    optimization: &eg_program::OptimizationResult,
    program: &eg_program::ProgramRevision,
    corpus: &eg_program::TrainingCorpus,
    seed: u64,
) -> Result<Option<ProgramRevisionIdentity>, String> {
    if !optimization.promoted {
        return Ok(None);
    }
    let active_revision_ref =
        current_program_active_revision_ref(state, &job.input_snapshot.graph, &program.program_ref)
            .await?;
    let promotion_result_ref = OpaqueRef::new(job.result_ref())
        .map_err(|_| "program promotion result identity is invalid".to_string())?;
    optimization
        .selected_candidate()
        .map(|candidate| {
            ProgramRevisionIdentity::from_candidate_with_binding(
                program,
                candidate,
                active_revision_ref,
                eg_program::ProgramResultInput {
                    result_ref: promotion_result_ref,
                    dataset_ref: OpaqueRef::new(job.input_snapshot.dataset_ref.clone())
                        .map_err(|_| eg_program::ProgramError::InvalidCommit)?,
                    content_digest: job.input_snapshot.content_digest.clone(),
                    snapshot_version: job.input_snapshot.version,
                },
                eg_program::ProgramCorpusBinding {
                    corpus_ref: corpus.corpus_ref.clone(),
                    snapshot_version: corpus.snapshot_version,
                },
                seed,
            )
        })
        .transpose()
        .map_err(|error| format!("program promotion identity failed: {error}"))
}

#[cfg(feature = "program-optimization")]
async fn run_program_kernel(
    store: &JobStore,
    job_id: &str,
    worker_ref: &str,
    epoch: u64,
    request: OptimizationRequest,
) -> Result<Option<eg_program::OptimizationResult>, String> {
    let cancellation = Arc::new(AtomicBool::new(false));
    let kernel_token = cancellation.clone();
    let mut kernel = tokio::task::spawn_blocking(move || {
        NativeCompiler::compile_cancellable(&request, &kernel_token)
    });
    let began = Instant::now();
    let mut ticker = tokio::time::interval(Duration::from_millis(250));
    let mut stop_reason: Option<&'static str> = None;
    let optimization = loop {
        tokio::select! {
            joined = &mut kernel => {
                match joined {
                    Ok(Ok(result)) if stop_reason.is_none() => break result,
                    Ok(Ok(_)) | Ok(Err(eg_program::ProgramError::Cancelled)) => {
                        finish_cancelled_kernel(store, job_id, worker_ref, epoch, stop_reason);
                        return Ok(None);
                    }
                    Ok(Err(_)) | Err(_) => {
                        finish_failed_kernel(store, job_id, worker_ref, epoch);
                        return Ok(None);
                    }
                }
            }
            _ = ticker.tick() => {
                let now = unix_ms();
                let current = match store.get(job_id) {
                    Ok(current) => current,
                    Err(_) => {
                        cancellation.store(true, Ordering::Relaxed);
                        stop_reason = Some("lease_lost");
                        continue;
                    }
                };
                stop_reason = program_stop_reason(&current, &began, now);
                if stop_reason.is_some() {
                    cancellation.store(true, Ordering::Relaxed);
                } else if store
                    .renew_lease(job_id, worker_ref, epoch, now, 60_000)
                    .is_err()
                {
                    cancellation.store(true, Ordering::Relaxed);
                    stop_reason = Some("lease_lost");
                }
            }
        }
    };
    Ok(Some(optimization))
}

#[cfg(feature = "program-optimization")]
fn finish_cancelled_kernel(
    store: &JobStore,
    job_id: &str,
    worker_ref: &str,
    epoch: u64,
    stop_reason: Option<&str>,
) {
    match stop_reason {
        Some("cancelled") => {
            let _ = store.mark_cancelled_fenced(job_id, worker_ref, epoch, unix_ms());
        }
        Some(reason) => {
            let _ = store.fail_attempt_fenced(job_id, worker_ref, epoch, reason, unix_ms());
        }
        None => {
            let _ =
                store.fail_attempt_fenced(job_id, worker_ref, epoch, "kernel_cancelled", unix_ms());
        }
    }
}

#[cfg(feature = "program-optimization")]
fn finish_failed_kernel(store: &JobStore, job_id: &str, worker_ref: &str, epoch: u64) {
    let _ = store.fail_attempt_fenced(job_id, worker_ref, epoch, "kernel_failure", unix_ms());
}

#[cfg(feature = "program-optimization")]
fn program_stop_reason(
    current: &eg_jobs::AnalyticsJob,
    began: &Instant,
    now: i64,
) -> Option<&'static str> {
    let cpu_exceeded = current
        .policy
        .resources
        .cpu_ms
        .or(current.policy.quota_cpu_ms)
        .is_some_and(|limit| began.elapsed().as_millis() as u64 >= limit);
    if current.cancel_requested {
        Some("cancelled")
    } else if current.deadline_exceeded(now) {
        Some("deadline_exceeded")
    } else if cpu_exceeded {
        Some("cpu_budget_exceeded")
    } else {
        None
    }
}

#[cfg(feature = "program-optimization")]
pub(super) async fn current_program_active_revision_ref(
    state: &Arc<RwLock<ServerState>>,
    graph: &str,
    program_ref: &OpaqueRef,
) -> Result<Option<OpaqueRef>, String> {
    let (_, _, core) = resolve_core_ref(state, graph)
        .await
        .ok_or_else(|| "program promotion target graph is unavailable".to_string())?;
    let pointer = ProgramRevisionIdentity::active_pointer_ref_for(program_ref);
    let Some(properties) = core.get_node_properties(pointer.as_str()) else {
        return Ok(None);
    };
    let value = eg_types::msgpack::decode_property_value(&properties)
        .map_err(|_| "program active pointer properties are invalid".to_string())?;
    if value.get("program_ref").and_then(serde_json::Value::as_str) != Some(program_ref.as_str()) {
        return Err("program active pointer has a different program identity".to_string());
    }
    let revision_ref = value
        .get("revision_ref")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| "program active pointer has no revision identity".to_string())?;
    let revision_ref = OpaqueRef::new(revision_ref.to_string())
        .map_err(|_| "program active pointer revision identity is invalid".to_string())?;
    if revision_ref.namespace() != "program_revision" {
        return Err("program active pointer revision identity has the wrong namespace".to_string());
    }
    crate::server::mutation_batch::resolve_program_promotion_identity(&core, &revision_ref)?;
    Ok(Some(revision_ref))
}
