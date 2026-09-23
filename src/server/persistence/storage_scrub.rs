//! Background node-payload scrub scheduling (EH-384,
//! CONCEPT:EG-KG.storage.node-payload-scrub).
//!
//! The binary runs [`run_step`] on its periodic-sweep cadence. Each step is one
//! bounded, resumable, read-only pass per shard (`RedbBackend::scrub_step`), so
//! an unreadable node is found and named between restarts without any
//! per-commit scan and without ever holding the write lock.
//!
//! Configuration (read on every step, like the other sweep limits):
//! * `EPISTEMIC_GRAPH_STORAGE_SCRUB_SECS`: seconds between steps. Unset means
//!   [`DEFAULT_INTERVAL_SECS`]; `0` turns the scrub off.
//! * `EPISTEMIC_GRAPH_STORAGE_SCRUB_ROWS`: node payloads one pass may open per
//!   shard. Zero, absent or unparsable values mean [`DEFAULT_ROWS_PER_PASS`].

use super::PersistenceBackend;
use crate::redb_store::scrub::ScrubBudget;

/// Default seconds between scrub steps.
pub const DEFAULT_INTERVAL_SECS: u64 = 300;

/// Default node payloads one pass may open per shard.
pub const DEFAULT_ROWS_PER_PASS: usize = 4096;

const INTERVAL_ENV: &str = "EPISTEMIC_GRAPH_STORAGE_SCRUB_SECS";
const ROWS_ENV: &str = "EPISTEMIC_GRAPH_STORAGE_SCRUB_ROWS";

/// Seconds between scrub steps; `0` means the scrub is off.
pub fn interval_secs() -> u64 {
    match std::env::var(INTERVAL_ENV) {
        Ok(value) => value.trim().parse().unwrap_or(DEFAULT_INTERVAL_SECS),
        Err(_) => DEFAULT_INTERVAL_SECS,
    }
}

/// Node payloads one pass may open per shard (always at least one).
pub fn rows_per_pass() -> usize {
    crate::server::state::positive_runtime_limit_from_env(ROWS_ENV, DEFAULT_ROWS_PER_PASS)
}

/// Run one scrub step when the backend is redb; returns how many unreadable
/// node rows the step found. Each finding is already logged by name and
/// counted by cause on the storage-scrub metrics.
pub async fn run_step(persistence: &dyn PersistenceBackend) -> Result<usize, String> {
    let Some(redb) = persistence.as_redb() else {
        return Ok(0);
    };
    let budget = ScrubBudget {
        rows: rows_per_pass(),
    };
    let passes = redb.scrub_step(budget).await?;
    Ok(passes.iter().map(|pass| pass.findings.len()).sum())
}
