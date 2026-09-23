//! Edge-native vector and text search (EH-351) under the user-managed index
//! lifecycle (EH-352).
//!
//! An edge is indexed and returned AS AN EDGE — never reified as a node. Its
//! identity is [`EdgeKey`]: the endpoints plus the edge's position among the
//! parallel edges of that pair. Positions only grow (a pair's parallel edges are
//! appended, and a pair is only ever removed whole), so parallel edges are told
//! apart and an ordinal never moves under a live edge.
//!
//! The design is RF-019's maintained authority, keyed by edge:
//!
//! * **Generations built off the query path.** [`refresh_edge_index`] builds one
//!   immutable generation from one graph snapshot and activates it atomically; a
//!   search never builds. A generation is a vector graph (HNSW, in the index's
//!   metric) or BM25 postings over one edge property.
//! * **Maintained from committed batches.** The index is registered in the
//!   graph's `IndexManager` as a server index, so every committed batch's edge
//!   delta reaches it under the batch's own topology lock — the edge and its
//!   searchable projection commit atomically. It records which endpoint pairs
//!   changed since its generation; a search scores every edge of those pairs
//!   exactly.
//! * **Visibility inside the walk.** A search runs against the CALLER's graph
//!   view (on the served path, already row-level-security filtered) and an
//!   optional predicate over the edge's own properties (label, property, the
//!   edge's own owner). Every candidate is re-read from that view, admitted only
//!   when it still exists, both endpoints are visible and the predicate holds,
//!   and re-scored on its CURRENT property — so a hidden, deleted or replaced
//!   edge never occupies a result slot, and an unresolved identity (a missing
//!   discriminator) admits nothing (the BUG-CX-022 invariant).
//! * **Bounded exact fallback.** With no servable generation — none yet, a
//!   blocked build, a view the index has not accounted for — a search scores the
//!   view's edges exactly up to a bound, and refuses past it.
//! * **Tenant and purpose scope.** An index belongs to one graph (a tenant's)
//!   and declares its purpose; a search must present the same scope.

mod generation;
mod registered;
mod serve;
#[cfg(test)]
mod tests;

pub use serve::{EdgeFallbackReason, EdgeHit, EdgeQuery, EdgeSearchAnswer, EdgeSearchRequest};

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, MutexGuard, RwLock};

use eg_core::graph::GraphCore;
use eg_core::index::{
    IndexBlock, IndexBlockReason, IndexManifest, ManagedIndexFamily, ManagedIndexState,
    ManagedIndexStatus, ManagedIndexTarget,
};
use serde::{Deserialize, Serialize};

use crate::sql::VectorMetric;
use generation::EdgeGeneration;
use registered::RegisteredEdgeIndex;

/// The stable identity of one edge.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct EdgeKey {
    pub source: String,
    pub target: String,
    /// The edge's position among the parallel edges of `(source, target)`.
    pub ordinal: u32,
}

/// What an edge index searches.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "family", rename_all = "snake_case")]
pub enum EdgeIndexKind {
    /// Nearest neighbours of a vector property, in `metric`.
    Vector { metric: VectorMetric },
    /// BM25 over a text property.
    Text,
}

/// The tenant and purpose an index serves; a search must present the same.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EdgeScope {
    pub tenant: String,
    pub purpose: String,
}

/// A user's request for one edge index.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EdgeIndexSpec {
    pub name: String,
    /// The edge property indexed.
    pub property: String,
    pub kind: EdgeIndexKind,
    pub scope: EdgeScope,
}

/// Resource bounds of one edge index.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EdgeIndexLimits {
    /// Edges the bounded exact fallback may examine.
    pub exact_edges: usize,
    /// Changed endpoint pairs one search scores exactly.
    pub delta_pairs: usize,
    /// Candidate edges one filtered walk may read.
    pub probe_edges: usize,
    /// Edges one generation may index.
    pub build_edges: usize,
}

impl Default for EdgeIndexLimits {
    fn default() -> Self {
        Self {
            exact_edges: 50_000,
            delta_pairs: 4_096,
            probe_edges: 32_768,
            build_edges: 500_000,
        }
    }
}

/// What one refresh did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EdgeRefreshOutcome {
    /// A new generation is live.
    Activated { generation: u64, edges: usize },
    /// Another build of this index is running.
    InFlight,
    /// A generation from a newer snapshot already serves; this one was discarded.
    Superseded,
    /// The index was dropped; nothing was built.
    Retired,
    /// The build failed; the index status carries the diagnostic.
    Failed(IndexBlock),
}

#[derive(Default)]
struct EdgeMaintenance {
    building: bool,
    retired: bool,
    generations_built: u64,
    block: Option<IndexBlock>,
}

/// One user-managed edge index.
pub struct EdgeIndex {
    spec: EdgeIndexSpec,
    limits: RwLock<EdgeIndexLimits>,
    live: RwLock<Option<Arc<EdgeGeneration>>>,
    /// Endpoint pairs changed by committed batches, with the graph version the
    /// batch was applied over (its changes are visible at later versions).
    touched: Mutex<BTreeMap<(String, String), u64>>,
    manifest: Mutex<IndexManifest>,
    maintenance: Mutex<EdgeMaintenance>,
}

impl EdgeIndex {
    fn new(spec: EdgeIndexSpec) -> Self {
        Self {
            spec,
            limits: RwLock::new(EdgeIndexLimits::default()),
            live: RwLock::new(None),
            touched: Mutex::new(BTreeMap::new()),
            manifest: Mutex::new(IndexManifest::default()),
            maintenance: Mutex::new(EdgeMaintenance::default()),
        }
    }

    pub fn spec(&self) -> &EdgeIndexSpec {
        &self.spec
    }

    pub fn limits(&self) -> EdgeIndexLimits {
        *self.limits.read().expect(POISONED)
    }

    pub fn set_limits(&self, limits: EdgeIndexLimits) {
        *self.limits.write().expect(POISONED) = limits;
    }

    /// Build a generation from `core`'s current snapshot and activate it. The
    /// maintenance entry point; a search never builds.
    pub fn refresh(&self, core: &GraphCore) -> EdgeRefreshOutcome {
        let Some(number) = self.begin_build() else {
            return self.refused();
        };
        let (view, version) = core.analysis_snapshot_versioned();
        let built = EdgeGeneration::build(number, &self.spec, &view, version, self.limits());
        let outcome = match built {
            Ok(generation) => self.activate(generation),
            Err(block) => {
                self.lock_maintenance().block = Some(block.clone());
                EdgeRefreshOutcome::Failed(block)
            }
        };
        self.lock_maintenance().building = false;
        outcome
    }

    /// This index's lifecycle status.
    pub fn status(&self) -> ManagedIndexStatus {
        let live = self.live_generation();
        let (building, block) = {
            let state = self.lock_maintenance();
            (state.building, state.block.clone())
        };
        let change_version = self.lock_manifest().source_snapshot_version;
        let built_version = live.as_ref().map(|generation| generation.built_version);
        ManagedIndexStatus {
            name: self.spec.name.clone(),
            family: match self.spec.kind {
                EdgeIndexKind::Vector { .. } => ManagedIndexFamily::Vector,
                EdgeIndexKind::Text => ManagedIndexFamily::Text,
            },
            target: ManagedIndexTarget::GraphEdges {
                property: self.spec.property.clone(),
            },
            state: ManagedIndexState::of(live.is_some(), building, block.as_ref()),
            generation: live.as_ref().map(|generation| generation.generation),
            built_version,
            change_version,
            lag: change_version.saturating_sub(built_version.unwrap_or(0)),
            indexed: live.as_ref().map(|generation| generation.keys.len()),
            block,
        }
    }

    fn live_generation(&self) -> Option<Arc<EdgeGeneration>> {
        self.live.read().expect(POISONED).clone()
    }

    fn lock_maintenance(&self) -> MutexGuard<'_, EdgeMaintenance> {
        self.maintenance.lock().expect(POISONED)
    }

    fn lock_manifest(&self) -> MutexGuard<'_, IndexManifest> {
        self.manifest.lock().expect(POISONED)
    }

    /// The next generation number, unless a build runs or the index is retired.
    fn begin_build(&self) -> Option<u64> {
        let mut state = self.lock_maintenance();
        if state.building || state.retired {
            return None;
        }
        state.building = true;
        state.generations_built += 1;
        Some(state.generations_built)
    }

    fn refused(&self) -> EdgeRefreshOutcome {
        if self.lock_maintenance().retired {
            return EdgeRefreshOutcome::Retired;
        }
        EdgeRefreshOutcome::InFlight
    }

    /// Make `generation` live unless the index was retired meanwhile or a
    /// generation from a newer snapshot already serves; forget the changes it
    /// covers and publish its source coverage.
    fn activate(&self, generation: EdgeGeneration) -> EdgeRefreshOutcome {
        if self.lock_maintenance().retired {
            return EdgeRefreshOutcome::Retired;
        }
        let outcome = EdgeRefreshOutcome::Activated {
            generation: generation.generation,
            edges: generation.keys.len(),
        };
        let version = generation.built_version;
        let manifest = generation.manifest;
        {
            let mut live = self.live.write().expect(POISONED);
            if live
                .as_ref()
                .is_some_and(|current| current.built_version > version)
            {
                return EdgeRefreshOutcome::Superseded;
            }
            *live = Some(Arc::new(generation));
        }
        self.touched
            .lock()
            .expect(POISONED)
            .retain(|_, stamp| *stamp >= version);
        *self.lock_manifest() = manifest;
        self.lock_maintenance().block = None;
        outcome
    }
}

/// A poisoned lock means a panic interrupted a generation swap; the index does
/// not guess whether that left it coherent, it stops.
const POISONED: &str = "edge index lock poisoned by a panic on another thread";

/// Register `spec` on `core` — the typed create of the lifecycle — and return
/// its status (`requested` until [`refresh_edge_index`] builds it). A name is
/// unique per graph.
pub fn create_edge_index(
    core: &GraphCore,
    spec: EdgeIndexSpec,
) -> Result<ManagedIndexStatus, IndexBlock> {
    let not_indexable = |detail: &str| IndexBlock::new(IndexBlockReason::NotIndexable, detail);
    if spec.name.is_empty() || spec.property.is_empty() {
        return Err(not_indexable("an edge index needs a name and a property"));
    }
    if edge_index(core, &spec.name).is_some() {
        return Err(not_indexable("an index of that name already exists"));
    }
    let index = Arc::new(EdgeIndex::new(spec));
    let status = index.status();
    core.register_index(Box::new(RegisteredEdgeIndex(index)));
    Ok(status)
}

/// The edge index named `name` on `core`, released from the registry lock so a
/// build or search never holds it.
pub fn edge_index(core: &GraphCore, name: &str) -> Option<Arc<EdgeIndex>> {
    core.indexes()
        .with_managed_index(name, |index| {
            index
                .as_any()
                .downcast_ref::<RegisteredEdgeIndex>()
                .map(|registered| Arc::clone(&registered.0))
        })
        .flatten()
}

/// Build and activate the next generation of the edge index `name`.
pub fn refresh_edge_index(core: &GraphCore, name: &str) -> Option<EdgeRefreshOutcome> {
    edge_index(core, name).map(|index| index.refresh(core))
}

/// Drop the edge index `name` — fenced: it is retired first, so a build still
/// running for it never activates. Returns how many indexes were dropped.
pub fn drop_edge_index(core: &GraphCore, name: &str) -> usize {
    core.indexes().drop_managed(name)
}
