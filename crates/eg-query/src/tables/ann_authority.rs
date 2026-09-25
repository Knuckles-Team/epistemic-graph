//! The maintained user-table ANN authority (RF-019, CONCEPT:EG-KG.query.real-pgvector-ann-top).
//!
//! A `CREATE INDEX … USING hnsw|ivfflat (col opclass)` registration names an
//! index; this module OWNS it. One [`UserAnnAuthority`] belongs to one opened
//! [`TableStore`](crate::tables::TableStore) (every clone shares it) and holds, per
//! registration, at most one LIVE generation: an immutable eg-ann graph built
//! from one consistent snapshot of the source rows.
//!
//! * **Built off the query path.** Only [`TableStore::refresh_ann_generations`]
//!   builds, and only the maintenance worker calls it. A query never builds,
//!   rebuilds or trains an index.
//! * **Atomic activation.** A finished generation replaces the live one under one
//!   write lock, and only when it observed a source epoch at least as new; a
//!   probe clones the live `Arc` once, so it runs against exactly one generation.
//! * **Revalidated serving.** A generation is a candidate generator, never an
//!   answer. Every candidate is re-read from the row store in the probe's own
//!   snapshot, checked against the caller's visibility predicate INSIDE the
//!   graph walk, dropped when deleted (a tombstone), and re-scored on its current
//!   vector. Rows inserted after the build (row ids above the generation's high
//!   water) are scored exactly. Updates are eventually consistent: an updated row
//!   is re-scored whenever it is a candidate, and the lag is visible in the status.
//! * **Bounded fallback.** With no servable generation (building, failed,
//!   dimension mismatch, too many rows since the build, or a filter so selective
//!   the walk exceeds its budget) the query takes an exact scan bounded by
//!   [`AnnLimits::exact_rows`]; past that bound it fails with the typed reason
//!   instead of scanning unboundedly or answering wrong.
//!
//! Generations live in memory. After a restart every index reports
//! [`AnnGenerationState::Building`] until the worker rebuilds it from the
//! durable rows.
//!
//! [`TableStore::refresh_ann_generations`]: crate::tables::TableStore::refresh_ann_generations

mod generation;
mod maintain;
mod serve;
#[cfg(test)]
mod tests;

pub use maintain::{AnnRefreshOutcome, AnnRefreshPolicy};
pub use serve::{AnnTopK, AnnTopKRequest};

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::{Arc, Mutex, MutexGuard, RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};

use crate::sql::{AnnIndexPlan, AnnMethod, VectorMetric};
pub(crate) use generation::{AnnGeneration, GenerationMetadata};

/// Serve receipts kept for inspection; the oldest is dropped first.
const RECEIPT_CAPACITY: usize = 64;

/// Where one registered ANN index stands.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum AnnGenerationState {
    /// No servable generation yet; queries take the bounded exact path.
    Building,
    /// The live generation observed the current source epoch.
    Live,
    /// A live generation serves, but the source has moved past it.
    Stale,
    /// The last build failed and no generation serves.
    Failed { reason: String },
}

/// The typed, inspectable status of one registered ANN index.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AnnIndexStatus {
    /// The catalog key, `"<table>.<column>.<metric>"`.
    pub index: String,
    pub table: String,
    pub column: String,
    pub method: AnnMethod,
    pub metric: VectorMetric,
    pub state: AnnGenerationState,
    /// The live generation number, when one serves.
    pub generation: Option<u64>,
    /// The source epoch the live generation was built from.
    pub built_epoch: Option<u64>,
    /// The tenant SQL source epoch now.
    pub source_epoch: u64,
    /// `source_epoch - built_epoch`; the whole epoch when nothing serves.
    pub lag_epochs: u64,
    /// Rows the live generation indexes.
    pub indexed_rows: usize,
    /// The most recent build failure, kept until a build succeeds.
    pub last_failure: Option<String>,
}

/// Why a query took the bounded exact path instead of the maintained index.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AnnFallbackReason {
    /// No generation has been activated for this registration yet.
    GenerationBuilding,
    /// The last build failed and no generation serves.
    GenerationFailed,
    /// The query vector's width differs from the generation's.
    DimensionMismatch,
    /// More rows arrived since the build than one probe scores exactly.
    DeltaOverflow,
    /// More changed pre-generation rows remain than one probe may rescore.
    DirtyOverflow,
    /// The filtered graph walk read more candidate rows than its budget.
    ProbeBudget,
}

/// Which path answered one top-k request.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "path", rename_all = "snake_case")]
pub enum AnnServingPath {
    /// The live maintained generation produced the candidates.
    MaintainedIndex { generation: u64 },
    /// A bounded exact scan answered, for the stated reason.
    BoundedExact { reason: AnnFallbackReason },
}

/// The observed record of one top-k request — kept by the authority, never
/// returned to the SQL caller, so it cannot leak hidden-row counts.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AnnServeReceipt {
    pub index: String,
    pub path: AnnServingPath,
    pub source_epoch: u64,
    /// Rows read from the row store, hidden ones included.
    pub examined_rows: usize,
    pub returned_rows: usize,
}

/// Resource bounds of the maintained ANN authority.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AnnLimits {
    /// Rows the bounded exact fallback may examine.
    pub exact_rows: usize,
    /// Rows above a generation's high water one probe scores exactly.
    pub delta_rows: usize,
    /// Candidate rows one filtered graph walk may read.
    pub probe_rows: usize,
    /// Rows one generation may index.
    pub build_rows: usize,
    /// Minimum time between two throttled builds of one index.
    pub rebuild_interval: Duration,
}

impl Default for AnnLimits {
    fn default() -> Self {
        Self {
            exact_rows: 50_000,
            delta_rows: 8_192,
            probe_rows: 32_768,
            build_rows: 500_000,
            rebuild_interval: Duration::from_secs(5),
        }
    }
}

/// One registration's live generation and its maintenance state. The two locks
/// are never held together.
#[derive(Default)]
struct AnnSlot {
    live: RwLock<Option<Arc<AnnGeneration>>>,
    maintenance: Mutex<SlotMaintenance>,
}

#[derive(Default)]
struct SlotMaintenance {
    building: bool,
    generations_built: u64,
    last_failure: Option<String>,
    last_build: Option<Instant>,
}

impl AnnSlot {
    /// The live generation when it matches `method`.
    fn live_for(&self, method: AnnMethod) -> Option<Arc<AnnGeneration>> {
        read(&self.live)
            .as_ref()
            .filter(|generation| generation.method == method)
            .cloned()
    }

    fn last_failure(&self) -> Option<String> {
        lock(&self.maintenance).last_failure.clone()
    }
}

/// The maintained ANN authority of one opened table store.
#[derive(Default)]
pub struct UserAnnAuthority {
    slots: RwLock<BTreeMap<String, Arc<AnnSlot>>>,
    receipts: Mutex<VecDeque<AnnServeReceipt>>,
    limits: RwLock<AnnLimits>,
}

impl std::fmt::Debug for UserAnnAuthority {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UserAnnAuthority")
            .field("indexes", &self.slot_keys())
            .finish()
    }
}

impl UserAnnAuthority {
    /// The resource bounds in force.
    pub fn limits(&self) -> AnnLimits {
        *read(&self.limits)
    }

    /// Replace the resource bounds.
    pub fn set_limits(&self, limits: AnnLimits) {
        *write(&self.limits) = limits;
    }

    /// The most recent serve receipts, oldest first.
    pub fn recent_receipts(&self) -> Vec<AnnServeReceipt> {
        lock(&self.receipts).iter().cloned().collect()
    }

    /// Catalog keys that currently own a slot.
    pub(crate) fn slot_keys(&self) -> Vec<String> {
        read(&self.slots).keys().cloned().collect()
    }

    fn record(&self, receipt: AnnServeReceipt) {
        let mut receipts = lock(&self.receipts);
        if receipts.len() == RECEIPT_CAPACITY {
            receipts.pop_front();
        }
        receipts.push_back(receipt);
    }

    fn existing_slot(&self, index: &str) -> Option<Arc<AnnSlot>> {
        read(&self.slots).get(index).cloned()
    }

    fn slot(&self, index: &str) -> Arc<AnnSlot> {
        if let Some(slot) = self.existing_slot(index) {
            return slot;
        }
        write(&self.slots)
            .entry(index.to_string())
            .or_default()
            .clone()
    }

    /// Drop every slot whose registration no longer exists.
    fn retain(&self, registered: &BTreeSet<String>) {
        write(&self.slots).retain(|index, _| registered.contains(index));
    }

    pub(crate) fn forget(&self, index: &str) {
        write(&self.slots).remove(index);
    }

    /// The generation a probe of `index` may serve from, else why none may.
    fn servable(
        &self,
        index: &str,
        method: AnnMethod,
    ) -> Result<Arc<AnnGeneration>, AnnFallbackReason> {
        let Some(slot) = self.existing_slot(index) else {
            return Err(AnnFallbackReason::GenerationBuilding);
        };
        if let Some(generation) = slot.live_for(method) {
            return Ok(generation);
        }
        Err(match slot.last_failure() {
            Some(_) => AnnFallbackReason::GenerationFailed,
            None => AnnFallbackReason::GenerationBuilding,
        })
    }

    fn status(&self, plan: &AnnIndexPlan, index: String, source_epoch: u64) -> AnnIndexStatus {
        let slot = self.existing_slot(&index);
        let live = slot.as_ref().and_then(|slot| slot.live_for(plan.method));
        let last_failure = slot.as_ref().and_then(|slot| slot.last_failure());
        AnnIndexStatus {
            index,
            table: plan.table.clone(),
            column: plan.column.clone(),
            method: plan.method,
            metric: plan.metric,
            state: generation_state(live.as_deref(), last_failure.as_ref(), source_epoch),
            generation: live.as_ref().map(|generation| generation.generation),
            built_epoch: live.as_ref().map(|generation| generation.built_epoch),
            source_epoch,
            lag_epochs: live.as_ref().map_or(source_epoch, |generation| {
                source_epoch.saturating_sub(generation.built_epoch)
            }),
            indexed_rows: live.as_ref().map_or(0, |generation| generation.rows),
            last_failure,
        }
    }
}

fn generation_state(
    live: Option<&AnnGeneration>,
    last_failure: Option<&String>,
    source_epoch: u64,
) -> AnnGenerationState {
    match (live, last_failure) {
        (Some(generation), _) if generation.built_epoch >= source_epoch => AnnGenerationState::Live,
        (Some(_), _) => AnnGenerationState::Stale,
        (None, Some(reason)) => AnnGenerationState::Failed {
            reason: reason.clone(),
        },
        (None, None) => AnnGenerationState::Building,
    }
}

// A poisoned lock means a panic interrupted a generation swap or a counter
// update on another thread; the authority does not guess whether that left it
// coherent, it stops.
const POISONED: &str = "ANN authority lock poisoned by a panic on another thread";

fn read<T>(lock: &RwLock<T>) -> RwLockReadGuard<'_, T> {
    lock.read().expect(POISONED)
}

fn write<T>(lock: &RwLock<T>) -> RwLockWriteGuard<'_, T> {
    lock.write().expect(POISONED)
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().expect(POISONED)
}
