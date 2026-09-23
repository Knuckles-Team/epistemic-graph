//! Building and activating generations — the maintenance worker's side of the
//! authority (RF-019). Nothing here runs on a query path.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};

use super::generation::AnnGeneration;
use super::{lock, read, write, AnnIndexStatus, AnnSlot};
use crate::sql::{AnnIndexPlan, AnnMethod};
use crate::tables::TableStore;

/// Whether a refresh honours [`AnnLimits::rebuild_interval`](super::AnnLimits::rebuild_interval).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AnnRefreshPolicy {
    /// Skip an index rebuilt more recently than the interval (the periodic
    /// worker).
    Throttled,
    /// Rebuild every stale index now (an explicit refresh).
    Immediate,
}

/// What one refresh did for one registered index.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "outcome", rename_all = "snake_case")]
pub enum AnnRefreshOutcome {
    /// A new generation was built and is now live.
    Activated {
        index: String,
        generation: u64,
        built_epoch: u64,
        rows: usize,
    },
    /// The live generation already observed the current source epoch.
    Current { index: String, generation: u64 },
    /// Rebuilt too recently under [`AnnRefreshPolicy::Throttled`].
    Deferred { index: String },
    /// Another build of this index is running.
    InFlight { index: String },
    /// A build finished after a newer generation went live; it was discarded.
    Superseded { index: String, built_epoch: u64 },
    /// The build failed; the reason is also visible in the index status.
    Failed { index: String, reason: String },
}

/// Why a build could not start.
enum BuildRefusal {
    InFlight,
    Deferred,
}

/// The right to build one slot's next generation. Dropping it releases the
/// slot, whether the build succeeded, failed or panicked.
struct BuildTicket<'s> {
    slot: &'s AnnSlot,
    generation: u64,
}

impl Drop for BuildTicket<'_> {
    fn drop(&mut self) {
        lock(&self.slot.maintenance).building = false;
    }
}

impl TableStore {
    /// Bring every registered ANN index's live generation up to the current
    /// source epoch, and forget generations whose registration was dropped. The
    /// maintenance worker's entry point; a query never calls it.
    pub fn refresh_ann_generations(
        &self,
        policy: AnnRefreshPolicy,
    ) -> Result<Vec<AnnRefreshOutcome>, String> {
        let plans = self.list_ann_indexes()?;
        let registered: BTreeSet<String> = plans.iter().map(TableStore::ann_index_key).collect();
        self.ann_authority().retain(&registered);
        let epoch = self.ann_source_epoch()?;
        Ok(plans
            .iter()
            .map(|plan| self.refresh_ann_index(plan, epoch, policy))
            .collect())
    }

    /// The typed status of every registered ANN index.
    pub fn ann_index_status(&self) -> Result<Vec<AnnIndexStatus>, String> {
        let epoch = self.ann_source_epoch()?;
        Ok(self
            .list_ann_indexes()?
            .iter()
            .map(|plan| {
                self.ann_authority()
                    .status(plan, TableStore::ann_index_key(plan), epoch)
            })
            .collect())
    }

    fn refresh_ann_index(
        &self,
        plan: &AnnIndexPlan,
        epoch: u64,
        policy: AnnRefreshPolicy,
    ) -> AnnRefreshOutcome {
        let index = TableStore::ann_index_key(plan);
        let slot = self.ann_authority().slot(&index);
        if let Some(generation) = slot.current(plan.method, epoch) {
            return AnnRefreshOutcome::Current { index, generation };
        }
        let limits = self.ann_authority().limits();
        let ticket = match slot.begin_build(policy, limits.rebuild_interval) {
            Ok(ticket) => ticket,
            Err(BuildRefusal::InFlight) => return AnnRefreshOutcome::InFlight { index },
            Err(BuildRefusal::Deferred) => return AnnRefreshOutcome::Deferred { index },
        };
        let built = self
            .ann_source_rows(&plan.table, &plan.column, limits.build_rows)
            .map(|source| {
                AnnGeneration::build(ticket.generation, plan.method, plan.metric, source)
            });
        slot.finish_build(ticket, index, built)
    }
}

impl AnnSlot {
    /// The live generation's number when it matches `method` and has observed
    /// `epoch`.
    fn current(&self, method: AnnMethod, epoch: u64) -> Option<u64> {
        self.live_for(method)
            .filter(|generation| generation.built_epoch >= epoch)
            .map(|generation| generation.generation)
    }

    fn begin_build(
        &self,
        policy: AnnRefreshPolicy,
        interval: Duration,
    ) -> Result<BuildTicket<'_>, BuildRefusal> {
        let serving = read(&self.live).is_some();
        let mut state = lock(&self.maintenance);
        if state.building {
            return Err(BuildRefusal::InFlight);
        }
        let recent = state.last_build.is_some_and(|at| at.elapsed() < interval);
        if policy == AnnRefreshPolicy::Throttled && serving && recent {
            return Err(BuildRefusal::Deferred);
        }
        state.building = true;
        state.last_build = Some(Instant::now());
        state.generations_built += 1;
        Ok(BuildTicket {
            slot: self,
            generation: state.generations_built,
        })
    }

    fn finish_build(
        &self,
        ticket: BuildTicket<'_>,
        index: String,
        built: Result<AnnGeneration, String>,
    ) -> AnnRefreshOutcome {
        let outcome = match built {
            Ok(generation) => self.activate(generation, index),
            Err(reason) => {
                lock(&self.maintenance).last_failure = Some(reason.clone());
                AnnRefreshOutcome::Failed { index, reason }
            }
        };
        drop(ticket);
        outcome
    }

    /// Make `generation` live unless a generation built from a NEWER snapshot
    /// already is: activation never moves an index backwards in source time.
    pub(super) fn activate(&self, generation: AnnGeneration, index: String) -> AnnRefreshOutcome {
        let built_epoch = generation.built_epoch;
        let outcome = AnnRefreshOutcome::Activated {
            index: index.clone(),
            generation: generation.generation,
            built_epoch,
            rows: generation.rows,
        };
        {
            let mut live = write(&self.live);
            let newer = live.as_ref().is_some_and(|current| {
                current.method == generation.method && current.built_epoch > built_epoch
            });
            if newer {
                return AnnRefreshOutcome::Superseded { index, built_epoch };
            }
            *live = Some(Arc::new(generation));
        }
        lock(&self.maintenance).last_failure = None;
        outcome
    }
}
