//! Building, activating and persisting generations — the maintenance worker's
//! side of the authority (RF-019). Nothing here runs on a query path.
//!
//! A refresh compares each index's live generation with ITS table's last change
//! epoch (per-table staleness). A stale index is refreshed by EXTENDING the live
//! generation with the rows changed since its build — the changed-row log names
//! them — and rebuilt from a full snapshot only when there is no live
//! generation, the change set is incomplete, or the extensions have grown past
//! a quarter of the generation. Every activation is persisted under the same
//! build ticket, so a reopened store serves the last activated generation.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};

use super::durable::{encode, Encoding};
use super::generation::AnnGeneration;
use super::{lock, read, write, AnnIndexStatus, AnnLimits, AnnSlot, DurableMark};
use crate::sql::{AnnIndexPlan, AnnMethod};
use crate::tables::store::GenerationWrite;
use crate::tables::TableStore;

/// Extensions fold at most this fraction (as a divisor) of a generation's rows
/// before the next refresh rebuilds it from a full snapshot.
const EXTENSION_DIVISOR: usize = 4;
/// Small generations may always fold this many rows.
const EXTENSION_FLOOR: usize = 64;

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
    /// The live generation already observed the table's last change.
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

/// The right to build, activate and persist one slot's next generation.
/// Dropping it releases the slot, whether the build succeeded, failed or
/// panicked.
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
    /// Bring every registered ANN index's live generation up to its table's
    /// last change, and forget generations whose registration was dropped. The
    /// maintenance worker's entry point; a query never calls it.
    pub fn refresh_ann_generations(
        &self,
        policy: AnnRefreshPolicy,
    ) -> Result<Vec<AnnRefreshOutcome>, String> {
        let plans = self.list_ann_indexes()?;
        let registered: BTreeSet<String> = plans.iter().map(TableStore::ann_index_key).collect();
        self.ann_authority().retain(&registered);
        Ok(plans
            .iter()
            .map(|plan| self.refresh_ann_index(plan, &plans, policy))
            .collect())
    }

    /// The typed status of every registered ANN index.
    pub fn ann_index_status(&self) -> Result<Vec<AnnIndexStatus>, String> {
        self.list_ann_indexes()?
            .iter()
            .map(|plan| {
                let change_epoch = self.ann_table_change_epoch(&plan.table)?;
                Ok(self
                    .ann_authority()
                    .status(plan, TableStore::ann_index_key(plan), change_epoch))
            })
            .collect()
    }

    /// Serve every persisted generation again after the store is (re)opened.
    /// A generation that cannot be restored is reported in its index status and
    /// rebuilt by the worker; it never fails the open.
    pub(crate) fn restore_ann_generations(&self) -> Result<(), String> {
        for plan in self.list_ann_indexes()? {
            let index = TableStore::ann_index_key(&plan);
            let slot = self.ann_authority().slot(&index);
            match self.restore_ann_generation(&plan, &index) {
                Ok(Some(generation)) => slot.restore(generation),
                Ok(None) => {}
                Err(reason) => slot.fail(format!(
                    "the persisted generation could not be restored: {reason}"
                )),
            }
        }
        Ok(())
    }

    fn refresh_ann_index(
        &self,
        plan: &AnnIndexPlan,
        plans: &[AnnIndexPlan],
        policy: AnnRefreshPolicy,
    ) -> AnnRefreshOutcome {
        let index = TableStore::ann_index_key(plan);
        let change_epoch = match self.ann_table_change_epoch(&plan.table) {
            Ok(epoch) => epoch,
            Err(reason) => return AnnRefreshOutcome::Failed { index, reason },
        };
        let slot = self.ann_authority().slot(&index);
        if let Some(generation) = slot.current(plan.method, change_epoch) {
            return AnnRefreshOutcome::Current { index, generation };
        }
        let limits = self.ann_authority().limits();
        let ticket = match slot.begin_build(policy, limits.rebuild_interval) {
            Ok(ticket) => ticket,
            Err(BuildRefusal::InFlight) => return AnnRefreshOutcome::InFlight { index },
            Err(BuildRefusal::Deferred) => return AnnRefreshOutcome::Deferred { index },
        };
        let built = self.next_generation(plan, &slot, ticket.generation, limits);
        let outcome = slot.finish_build(index, built);
        if matches!(outcome, AnnRefreshOutcome::Activated { .. }) {
            self.persist_live(plan, plans, &slot);
        }
        drop(ticket);
        outcome
    }

    /// The live generation extended by the rows changed since its build when
    /// that is cheap, else a full build from one snapshot.
    fn next_generation(
        &self,
        plan: &AnnIndexPlan,
        slot: &AnnSlot,
        number: u64,
        limits: AnnLimits,
    ) -> Result<AnnGeneration, String> {
        if let Some(extended) = self.extended_generation(plan, slot, number, limits)? {
            return Ok(extended);
        }
        self.ann_source_rows(&plan.table, &plan.column, limits.build_rows)
            .map(|source| AnnGeneration::build(number, plan.method, plan.metric, source))
    }

    fn extended_generation(
        &self,
        plan: &AnnIndexPlan,
        slot: &AnnSlot,
        number: u64,
        limits: AnnLimits,
    ) -> Result<Option<AnnGeneration>, String> {
        let Some(live) = slot.live_for(plan.method) else {
            return Ok(None);
        };
        let changed = self.ann_changed_rows(
            &plan.table,
            &plan.column,
            live.built_epoch,
            limits.delta_rows,
        )?;
        let bound = (live.rows / EXTENSION_DIVISOR).max(EXTENSION_FLOOR);
        if !changed.complete || live.delta_len() + changed.rows.len() > bound {
            return Ok(None);
        }
        Ok(live.extend(number, &changed))
    }

    /// Persist the slot's live generation. A failure leaves the generation
    /// serving and is visible in the index status until the next activation.
    fn persist_live(&self, plan: &AnnIndexPlan, plans: &[AnnIndexPlan], slot: &AnnSlot) {
        let Some(live) = slot.live_for(plan.method) else {
            return;
        };
        match self.persist_generation(plan, plans, slot, &live) {
            Ok(mark) => slot.mark_durable(mark),
            Err(reason) => slot.fail(format!(
                "generation {} serves but was not persisted: {reason}",
                live.generation
            )),
        }
    }

    fn persist_generation(
        &self,
        plan: &AnnIndexPlan,
        plans: &[AnnIndexPlan],
        slot: &AnnSlot,
        live: &AnnGeneration,
    ) -> Result<DurableMark, String> {
        let durable_base = slot.durable().map(|mark| mark.full);
        let extension_base = live.base.filter(|base| durable_base == Some(*base));
        let encoding = match extension_base {
            Some(_) => Encoding::Extension,
            None => Encoding::Full,
        };
        let stored = encode(live, encoding)?;
        let index = TableStore::ann_index_key(plan);
        self.persist_ann_generation(&GenerationWrite {
            index: &index,
            table: &plan.table,
            stored: &stored,
            keeps: extension_base,
            prune_through: self.prune_floor(plan, plans, live.built_epoch),
        })?;
        Ok(DurableMark {
            full: extension_base.unwrap_or(live.generation),
            built_epoch: live.built_epoch,
        })
    }

    /// The epoch through which every index of `plan`'s table has a DURABLE
    /// generation (the one being persisted counts at `own_epoch`); `None` when
    /// any has none, so nothing a restored generation needs is ever pruned.
    fn prune_floor(
        &self,
        plan: &AnnIndexPlan,
        plans: &[AnnIndexPlan],
        own_epoch: u64,
    ) -> Option<u64> {
        let own = TableStore::ann_index_key(plan);
        plans
            .iter()
            .filter(|other| other.table.eq_ignore_ascii_case(&plan.table))
            .map(|other| {
                let key = TableStore::ann_index_key(other);
                if key == own {
                    return Some(own_epoch);
                }
                self.ann_authority()
                    .existing_slot(&key)?
                    .durable()
                    .map(|mark| mark.built_epoch)
            })
            .try_fold(u64::MAX, |floor, epoch| epoch.map(|epoch| floor.min(epoch)))
    }
}

impl AnnSlot {
    /// The live generation's number when it matches `method` and has observed
    /// `change_epoch`, its table's last change.
    fn current(&self, method: AnnMethod, change_epoch: u64) -> Option<u64> {
        self.live_for(method)
            .filter(|generation| generation.built_epoch >= change_epoch)
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
        index: String,
        built: Result<AnnGeneration, String>,
    ) -> AnnRefreshOutcome {
        match built {
            Ok(generation) => self.activate(generation, index),
            Err(reason) => {
                self.fail(reason.clone());
                AnnRefreshOutcome::Failed { index, reason }
            }
        }
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
