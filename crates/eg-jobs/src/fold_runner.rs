//! Fenced, per-fold execution for a durable walk-forward replay job.
//!
//! The caller owns the policy kernel and final result seal. This module owns
//! restart-safe progress: a fold is complete only after its outcome is in the
//! durable checkpoint under the same worker lease that claimed the job.

use serde::{de::DeserializeOwned, Deserialize, Serialize};

use crate::model::{Checkpoint, JobState};
use crate::store::{JobStore, WorkerClaim};

const FOLD_STATE_VERSION: u8 = 1;

#[derive(Debug, PartialEq, Eq)]
pub enum FoldRunError {
    Checkpoint(String),
    Work(String),
    Store(String),
}

impl std::fmt::Display for FoldRunError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Checkpoint(reason) => write!(f, "invalid fold checkpoint: {reason}"),
            Self::Work(reason) => write!(f, "fold execution failed: {reason}"),
            Self::Store(reason) => write!(f, "fold checkpoint store failed: {reason}"),
        }
    }
}

impl std::error::Error for FoldRunError {}

#[derive(Serialize, Deserialize)]
struct FoldState<T> {
    version: u8,
    input_digest: String,
    params_digest: String,
    total: usize,
    completed: Vec<T>,
}

#[derive(Serialize)]
struct FoldStateRef<'a, T> {
    version: u8,
    input_digest: &'a str,
    params_digest: &'a str,
    total: usize,
    completed: &'a [T],
}

fn restored<T: DeserializeOwned>(
    checkpoint: Option<&Checkpoint>,
    input_digest: &str,
    params_digest: &str,
    total: usize,
) -> Result<Vec<T>, FoldRunError> {
    let Some(checkpoint) = checkpoint else {
        return Ok(Vec::new());
    };
    let Some(blob) = &checkpoint.state_blob else {
        if checkpoint.progress == 0.0 {
            return Ok(Vec::new());
        }
        return Err(FoldRunError::Checkpoint(
            "progress has no fold state".into(),
        ));
    };
    let state: FoldState<T> = serde_json::from_slice(blob)
        .map_err(|_| FoldRunError::Checkpoint("fold state cannot be decoded".into()))?;
    if state.version != FOLD_STATE_VERSION
        || state.input_digest != input_digest
        || state.params_digest != params_digest
        || state.total != total
        || state.completed.len() > total
    {
        return Err(FoldRunError::Checkpoint(
            "fold state differs from the pinned job inputs".into(),
        ));
    }
    let progress = state.completed.len() as f64 / total as f64;
    if (checkpoint.progress - progress).abs() > f64::EPSILON {
        return Err(FoldRunError::Checkpoint(
            "fold count and progress disagree".into(),
        ));
    }
    Ok(state.completed)
}

fn execute<T, F, P>(
    checkpoint: Option<&Checkpoint>,
    input_digest: &str,
    params_digest: &str,
    total: usize,
    mut compute: F,
    mut persist: P,
    now_ms: fn() -> i64,
) -> Result<Vec<T>, FoldRunError>
where
    T: Serialize + DeserializeOwned,
    F: FnMut(usize) -> Result<T, String>,
    P: FnMut(Checkpoint) -> Result<(), String>,
{
    if total == 0 || input_digest.is_empty() || params_digest.is_empty() {
        return Err(FoldRunError::Checkpoint("empty fold job identity".into()));
    }
    let mut completed = restored(checkpoint, input_digest, params_digest, total)?;
    for index in completed.len()..total {
        let outcome = compute(index).map_err(FoldRunError::Work)?;
        completed.push(outcome);
        let state = FoldStateRef {
            version: FOLD_STATE_VERSION,
            input_digest,
            params_digest,
            total,
            completed: completed.as_slice(),
        };
        let blob = serde_json::to_vec(&state)
            .map_err(|_| FoldRunError::Checkpoint("fold state cannot be encoded".into()))?;
        persist(Checkpoint {
            progress: completed.len() as f64 / total as f64,
            stage: format!("replay-fold-{}/{total}", completed.len()),
            state_blob: Some(blob),
            updated_at_ms: now_ms(),
        })
        .map_err(FoldRunError::Store)?;
    }
    Ok(completed)
}

/// Run only the unfinished folds under an existing, live worker lease.
///
/// A crashed worker's stale epoch cannot checkpoint. A restarted worker passes
/// its newly claimed lease; the stored prefix is validated against the pinned
/// input and algorithm digests before any kernel work resumes.
pub fn run_folds_fenced<T, F>(
    store: &JobStore,
    claim: &WorkerClaim,
    total: usize,
    mut compute: F,
    now_ms: fn() -> i64,
) -> Result<Vec<T>, FoldRunError>
where
    T: Serialize + DeserializeOwned,
    F: FnMut(usize) -> Result<T, String>,
{
    let job = store
        .verify_lease(
            &claim.job.job_id,
            &claim.lease.worker_ref,
            claim.lease.epoch,
            now_ms(),
        )
        .map_err(|error| FoldRunError::Store(error.to_string()))?;
    let JobState::Running { checkpoint } = &job.state else {
        return Err(FoldRunError::Checkpoint("job is not running".into()));
    };
    execute(
        Some(checkpoint),
        &job.input_snapshot.content_digest,
        &job.algo.params_digest,
        total,
        |index| {
            let current = store.get(&job.job_id).map_err(|error| error.to_string())?;
            if current.cancel_requested {
                return Err("job cancellation requested".into());
            }
            compute(index)
        },
        |checkpoint| {
            store
                .checkpoint_fenced(
                    &job.job_id,
                    &claim.lease.worker_ref,
                    claim.lease.epoch,
                    checkpoint,
                    now_ms(),
                )
                .map(|_| ())
                .map_err(|error| error.to_string())
        },
        now_ms,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::RefCell;

    #[test]
    fn failed_fold_resumes_from_last_durable_prefix() {
        let saved = RefCell::new(None);
        let first = execute::<usize, _, _>(
            None,
            "input-a",
            "params-a",
            3,
            |fold| {
                if fold == 2 {
                    Err("worker stopped".into())
                } else {
                    Ok(fold * 10)
                }
            },
            |checkpoint| {
                *saved.borrow_mut() = Some(checkpoint);
                Ok(())
            },
            || 7,
        );
        assert_eq!(first, Err(FoldRunError::Work("worker stopped".into())));
        let mut called = Vec::new();
        let resumed = execute(
            saved.borrow().as_ref(),
            "input-a",
            "params-a",
            3,
            |fold| {
                called.push(fold);
                Ok(fold * 10)
            },
            |_| Ok(()),
            || 8,
        )
        .unwrap();
        assert_eq!(called, [2]);
        assert_eq!(resumed, [0, 10, 20]);
    }

    #[test]
    fn failed_persist_does_not_advance_the_durable_prefix() {
        let mut attempted = None;
        let failed = execute::<usize, _, _>(
            None,
            "input-a",
            "params-a",
            2,
            Ok::<_, String>,
            |checkpoint| {
                attempted = Some(checkpoint);
                Err("disk unavailable".into())
            },
            || 3,
        );
        assert_eq!(failed, Err(FoldRunError::Store("disk unavailable".into())));
        assert_eq!(attempted.unwrap().progress, 0.5);
        let mut recomputed = Vec::new();
        let resumed = execute(
            None,
            "input-a",
            "params-a",
            2,
            |fold| {
                recomputed.push(fold);
                Ok(fold)
            },
            |_| Ok(()),
            || 4,
        )
        .unwrap();
        assert_eq!(recomputed, [0, 1]);
        assert_eq!(resumed, [0, 1]);
    }

    #[test]
    fn changed_input_refuses_a_saved_prefix() {
        let mut saved = None;
        execute::<usize, _, _>(
            None,
            "input-a",
            "params-a",
            1,
            Ok::<_, String>,
            |checkpoint| {
                saved = Some(checkpoint);
                Ok(())
            },
            || 3,
        )
        .unwrap();
        assert!(matches!(
            restored::<usize>(saved.as_ref(), "input-b", "params-a", 1),
            Err(FoldRunError::Checkpoint(_))
        ));
    }
}
