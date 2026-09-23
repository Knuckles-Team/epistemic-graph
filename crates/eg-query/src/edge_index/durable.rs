//! Durable edge indexes (EH-351 / EH-352): the registration and the activated
//! generation live in the tenant's SQL owner file, so an index survives a
//! restart without a rebuild.
//!
//! A restored generation is RECONCILED before it serves: the in-memory record
//! of pairs changed since the build does not survive a restart, so the restore
//! compares every current edge's content hash with the generation's and marks
//! every pair whose edges differ as changed — those pairs are scored exactly,
//! as any pair changed after a build is. That is one decode pass over the
//! edges, never an index build, and it is what makes installing an index into
//! a graph lazily (on the tenant's first use after a restart) exact.

use std::collections::BTreeMap;

use eg_core::graph::{GraphCore, GraphView};
use eg_core::index::{IndexBlock, IndexBlockReason, IndexManifest, ManagedIndexStatus};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::generation::{view_edges, EdgeGeneration, EdgeGraph};
use super::{
    create_edge_index, edge_index, snapshot, EdgeIndex, EdgeIndexSpec, EdgeKey, EdgeRefreshOutcome,
    POISONED,
};
use crate::tables::store::StoredGeneration;
use crate::tables::TableStore;

/// Endpoint pair -> `(ordinal, content hash)` of every edge carrying a value.
type PairContent = BTreeMap<(String, String), Vec<(u32, u64)>>;

#[derive(Serialize)]
struct StoredRef<'g> {
    keys: &'g [EdgeKey],
    hashes: &'g [u64],
    graph: &'g EdgeGraph,
}

#[derive(Deserialize)]
struct Stored {
    keys: Vec<EdgeKey>,
    hashes: Vec<u64>,
    graph: EdgeGraph,
}

fn encode(generation: &EdgeGeneration) -> Result<StoredGeneration, String> {
    let payload = rmp_serde::to_vec_named(&StoredRef {
        keys: &generation.keys,
        hashes: &generation.hashes,
        graph: &generation.graph,
    })
    .map_err(|error| format!("encode edge generation: {error}"))?;
    Ok(StoredGeneration {
        generation: generation.generation,
        manifest: Sha256::digest(&payload).to_vec(),
        payload,
    })
}

fn decode(stored: &StoredGeneration) -> Result<Stored, String> {
    if Sha256::digest(&stored.payload).as_slice() != stored.manifest.as_slice() {
        return Err("edge generation payload does not match its digest".to_string());
    }
    let decoded: Stored = rmp_serde::from_slice(&stored.payload)
        .map_err(|error| format!("edge generation is unreadable: {error}"))?;
    if decoded.keys.len() != decoded.hashes.len() {
        return Err("edge generation keys and hashes disagree".to_string());
    }
    Ok(decoded)
}

/// Every pair's current `(ordinal, hash)` list in `view`, and the view's edge
/// count.
fn current_content(view: &GraphView, spec: &EdgeIndexSpec) -> (PairContent, u64) {
    let mut content = PairContent::new();
    let mut edges = 0u64;
    for (key, value) in view_edges(view, &spec.property, spec.kind) {
        edges += 1;
        if let Some(value) = value {
            content
                .entry((key.source, key.target))
                .or_default()
                .push((key.ordinal, value.content_hash()));
        }
    }
    (content, edges)
}

fn stored_content(keys: &[EdgeKey], hashes: &[u64]) -> PairContent {
    let mut content = PairContent::new();
    for (key, hash) in keys.iter().zip(hashes) {
        content
            .entry((key.source.clone(), key.target.clone()))
            .or_default()
            .push((key.ordinal, *hash));
    }
    content
}

impl EdgeIndex {
    /// Persist the live generation for `graph` in `store`. A no-op without one.
    pub fn persist(&self, store: &TableStore, graph: &str) -> Result<(), String> {
        let Some(live) = self.live_generation() else {
            return Ok(());
        };
        store.persist_edge_generation(graph, &self.spec.name, &encode(&live)?)
    }

    /// Restore the persisted generation of this index on `graph`, reconciled
    /// against `core`'s current edges. `Ok(false)` when none was persisted.
    pub fn restore(
        &self,
        store: &TableStore,
        graph: &str,
        core: &GraphCore,
    ) -> Result<bool, String> {
        let Some(stored) = store.edge_generation(graph, &self.spec.name)? else {
            return Ok(false);
        };
        let decoded = decode(&stored)?;
        let (view, version) = snapshot(core);
        let (current, edges) = current_content(&view, &self.spec);
        let persisted = stored_content(&decoded.keys, &decoded.hashes);
        let changed = current
            .keys()
            .chain(persisted.keys())
            .filter(|pair| current.get(*pair) != persisted.get(*pair))
            .cloned()
            .collect::<Vec<_>>();
        {
            let mut touched = self.touched.lock().expect(POISONED);
            for pair in changed {
                touched.insert(pair, version);
            }
        }
        {
            let mut state = self.lock_maintenance();
            state.generations_built = state.generations_built.max(stored.generation);
        }
        let generation = EdgeGeneration {
            generation: stored.generation,
            built_version: version,
            keys: decoded.keys,
            hashes: decoded.hashes,
            graph: decoded.graph,
            manifest: IndexManifest::valid(version, view.node_map.len() as u64, edges),
        };
        Ok(matches!(
            self.activate(generation),
            EdgeRefreshOutcome::Activated { .. }
        ))
    }
}

/// Create edge index `spec` on `graph`: registered durably in `store` (the
/// tenant's catalog) and installed into `core`. Refused when the name exists.
pub fn create_durable_edge_index(
    store: &TableStore,
    graph: &str,
    core: &GraphCore,
    spec: EdgeIndexSpec,
) -> Result<ManagedIndexStatus, IndexBlock> {
    let failed = |detail: String| IndexBlock::new(IndexBlockReason::BuildFailed, &detail);
    let encoded = rmp_serde::to_vec_named(&spec).map_err(|error| failed(error.to_string()))?;
    let created = store
        .put_edge_index_record(graph, &spec.name, &encoded)
        .map_err(failed)?;
    if !created {
        return Err(IndexBlock::new(
            IndexBlockReason::NotIndexable,
            "an index of that name already exists",
        ));
    }
    create_edge_index(core, spec)
}

/// Install into `core` every edge index `store` registers for `graph` that
/// `core` does not hold yet, each restored from its persisted generation. The
/// lazy install a served edge operation runs first. Returns how many it
/// installed.
pub fn install_edge_indexes(
    store: &TableStore,
    graph: &str,
    core: &GraphCore,
) -> Result<usize, String> {
    let mut installed = 0;
    for (_, name, encoded) in store.edge_index_records(graph)? {
        if edge_index(core, &name).is_some() {
            continue;
        }
        let spec: EdgeIndexSpec = rmp_serde::from_slice(&encoded)
            .map_err(|error| format!("edge index `{name}` registration is unreadable: {error}"))?;
        create_edge_index(core, spec).map_err(|block| block.detail)?;
        if let Some(index) = edge_index(core, &name) {
            index.restore(store, graph, core)?;
        }
        installed += 1;
    }
    Ok(installed)
}

/// Build and activate the next generation of edge index `name` on `graph`, then
/// persist it.
pub fn refresh_durable_edge_index(
    store: &TableStore,
    graph: &str,
    core: &GraphCore,
    name: &str,
) -> Result<EdgeRefreshOutcome, String> {
    let index =
        edge_index(core, name).ok_or_else(|| format!("edge index `{name}` does not exist"))?;
    let outcome = index.refresh(core);
    if matches!(outcome, EdgeRefreshOutcome::Activated { .. }) {
        index.persist(store, graph)?;
    }
    Ok(outcome)
}

/// Drop edge index `name` of `graph`: fenced in `core` (retired, then
/// unregistered) and removed from `store` with its generations. `Ok(false)`
/// when it did not exist.
pub fn drop_durable_edge_index(
    store: &TableStore,
    graph: &str,
    core: &GraphCore,
    name: &str,
) -> Result<bool, String> {
    let dropped = super::drop_edge_index(core, name) > 0;
    Ok(store.drop_edge_index_record(graph, name)? || dropped)
}
