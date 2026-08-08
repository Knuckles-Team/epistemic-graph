//! Staging-discipline step 2 (addendum §1.2): "a `FOREIGN` source returning
//! `(id, score)` via the existing federation seam (`crates/eg-plan/src/
//! federation.rs`). UQL is unchanged and the planner simply calls out."
//!
//! ★ This module deliberately does NOT submit-and-block inside `ForeignSource::fetch`
//! — doing so would silently reintroduce the "quantum as a synchronous call" shape
//! the whole point of lane Q5 is to avoid (real QPU queues are minutes to hours; even
//! the in-process simulator backends this crate ships are meant to be exercised
//! through the SAME async submit/claim/complete state machine a real backend would
//! use). Instead, [`quantum_job_foreign_source`] reads back an ALREADY-DURABLE job's
//! result — the honest "FOREIGN source over async job output" shape: a caller submits
//! (out of band, whenever), a worker completes it (out of band, whenever), and a
//! query's `FOREIGN "<name>"` clause composes whatever is durably there NOW. A job
//! that has not reached `Succeeded` yet surfaces as a clean fetch error (exactly like
//! [`crate::job::join_quantum_result_rows`]'s own contract), never a silent empty set
//! or a blocking wait inside the plan executor.

use std::sync::Arc;

use eg_jobs::JobStore;
use eg_plan::federation::ForeignSourceRegistry;
use eg_plan::rowset::RowSet;

use crate::job::join_quantum_result_rows;

/// Register a named [`eg_plan::federation::ForeignSource`] that resolves to the
/// `(id, score)` rows of `job_id`'s durable `TypedJobResult`, read fresh from `store`
/// on every `fetch()` (so a query issued after the worker completes the job sees the
/// result; one issued before sees the SAME clean "not yet Succeeded" error
/// [`join_quantum_result_rows`] already defines — never a stale/cached snapshot).
pub fn register_quantum_job_source(
    registry: &mut ForeignSourceRegistry,
    name: impl Into<String>,
    store: Arc<JobStore>,
    job_id: impl Into<String>,
) -> &mut ForeignSourceRegistry {
    let job_id = job_id.into();
    registry.register_closure(name, move || {
        let rows = join_quantum_result_rows(&store, &job_id).map_err(|e| e.to_string())?;
        Ok(RowSet::from_scored(rows.into_iter().map(|(id, s)| (id, s))))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use eg_jobs::{InputSnapshotHandle, JobPolicy, TenantJobQuota};
    use eg_quantum_core::backend::RunOptions;
    use eg_quantum_core::estimate::EstimateOptions;
    use eg_quantum_core::planner::PlannerOptions;
    use eg_quantum_sim::stabilizer::StabilizerSimulator;

    fn open_store() -> (Arc<JobStore>, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let store = JobStore::open(&dir.path().join("jobs.redb")).unwrap();
        (Arc::new(store), dir)
    }

    /// End-to-end: submit -> (a worker completes it) -> register as a named FOREIGN
    /// source -> `fetch()` returns the SAME rows `join_quantum_result_rows` would.
    #[test]
    fn quantum_job_composes_as_a_named_foreign_source() {
        let (store, _dir) = open_store();
        let candidates = vec!["n1".to_string(), "n2".to_string(), "n3".to_string()];
        let job = crate::job::submit_quantum_job(
            &store,
            InputSnapshotHandle::new("test-graph", 1),
            JobPolicy {
                tenant: "t1".into(),
                actor: "a1".into(),
                ..Default::default()
            },
            candidates.clone(),
            vec![(0, 1), (1, 2)],
            RunOptions {
                shots: Some(256),
                seed: Some(42),
                ..Default::default()
            },
            EstimateOptions::default(),
            PlannerOptions::default(),
            1,
            0,
        )
        .expect("submit succeeds");

        let stabilizer = StabilizerSimulator::new();
        let backends = crate::job::BackendSet::new(vec![&stabilizer]);
        let finished = crate::job::claim_and_run_quantum_job(
            &store,
            "worker-1",
            &backends,
            TenantJobQuota::default(),
            1_000,
            60_000,
        )
        .expect("worker run succeeds")
        .expect("a job was ready");

        let mut registry = ForeignSourceRegistry::new();
        register_quantum_job_source(&mut registry, "quantum_demo", store.clone(), finished.job_id);
        let rows = registry.resolve("quantum_demo").expect("resolves");
        let mut ids = rows.ids();
        ids.sort();
        assert_eq!(ids, vec!["n1", "n2", "n3"]);
        for row in rows.rows() {
            let score = row.score.expect("every row carries a confidence score");
            assert!((0.0..=1.0).contains(&score));
        }
    }

    #[test]
    fn unfinished_job_surfaces_a_clean_fetch_error() {
        let (store, _dir) = open_store();
        let job = crate::job::submit_quantum_job(
            &store,
            InputSnapshotHandle::new("test-graph", 1),
            JobPolicy {
                tenant: "t1".into(),
                actor: "a1".into(),
                ..Default::default()
            },
            vec!["n1".to_string()],
            vec![],
            RunOptions::default(),
            EstimateOptions::default(),
            PlannerOptions::default(),
            1,
            0,
        )
        .expect("submit succeeds");

        let mut registry = ForeignSourceRegistry::new();
        register_quantum_job_source(&mut registry, "quantum_demo", store, job.job_id);
        let err = registry.resolve("quantum_demo").unwrap_err();
        assert!(
            err.contains("join requires a Succeeded job"),
            "unexpected error: {err}"
        );
    }
}
