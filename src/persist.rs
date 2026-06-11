// Snapshot persistence for the multi-tenant graph registry.
//
// CONCEPT:KG-2.8 / OS-5.9 — fast, bounded, restart-surviving checkpoints.
//
// Each registered graph is serialized to `{persist_dir}/{sanitized_name}.mp`
// (compact MessagePack via `GraphCore::to_msgpack`), plus a `manifest.json`
// recording each file's logical graph name + type so the registry can be
// reconstructed on startup. Writes are atomic (temp file + rename). This is an
// RDB-style snapshot: bounded disk (~ one file per graph ≈ graph size, not an
// unbounded WAL), off the request hot path, and loaded once at boot for a fast
// warm restart. The durable system-of-record (pggraph) remains the disaster-
// recovery tier; this snapshot is the local fast-restart cache.

use std::path::Path;
use std::sync::Arc;

use tokio::sync::RwLock;
use tracing::info;

use crate::graph::GraphCore;
use crate::protocol::GraphType;
use crate::server::ServerState;

const MANIFEST: &str = "manifest.json";

/// Map a logical graph name (which may contain `:` / `/`) to a safe filename.
fn sanitize(name: &str) -> String {
    name.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.') {
                c
            } else {
                '_'
            }
        })
        .collect()
}

/// Atomically write `bytes` to `path` (temp file in the same dir + rename).
fn atomic_write(path: &Path, bytes: &[u8]) -> Result<(), String> {
    let tmp = path.with_extension("tmp");
    std::fs::write(&tmp, bytes).map_err(|e| e.to_string())?;
    std::fs::rename(&tmp, path).map_err(|e| e.to_string())
}

/// Serialize every registered graph to the configured persist dir.
///
/// Returns the number of graphs written. No-op (Ok(0)) when no persist dir is
/// configured. The global state lock is released before serializing — only the
/// per-graph read lock is held during each graph's snapshot — so checkpoints do
/// not block concurrent reads/writes to other graphs.
pub async fn checkpoint_all(state: &Arc<RwLock<ServerState>>) -> Result<usize, String> {
    let start = std::time::Instant::now();
    let (dir, entries) = {
        let s = state.read().await;
        let dir = match &s.persist_dir {
            Some(d) => d.clone(),
            None => return Ok(0),
        };
        let entries: Vec<(String, GraphType, Arc<RwLock<GraphCore>>)> = s
            .registry
            .all_entries()
            .iter()
            .map(|e| (e.name.clone(), e.graph_type, e.core.clone()))
            .collect();
        (dir, entries)
    };

    std::fs::create_dir_all(&dir).map_err(|e| e.to_string())?;
    let mut manifest = serde_json::Map::new();
    let mut count = 0usize;
    for (name, gtype, core) in entries {
        let bytes = {
            let g = core.read().await;
            g.to_msgpack()?
        };
        let fname = sanitize(&name);
        let path = Path::new(&dir).join(format!("{fname}.mp"));
        atomic_write(&path, &bytes)?;
        manifest.insert(
            fname,
            serde_json::json!({ "name": name, "graph_type": gtype }),
        );
        count += 1;
    }
    let manifest_bytes = serde_json::to_vec(&manifest).map_err(|e| e.to_string())?;
    atomic_write(&Path::new(&dir).join(MANIFEST), &manifest_bytes)?;
    crate::metrics::checkpoint_completed(start.elapsed().as_secs_f64());
    info!("Checkpoint wrote {} graphs to {}", count, dir);
    Ok(count)
}

/// Apply an Ebbinghaus decay sweep across every registered graph (CONCEPT:KG-2.16).
///
/// Mirrors `checkpoint_all`'s lock discipline: the global registry lock is
/// released before sweeping, and only one per-graph **write** lock is held at a
/// time, so the sweep never blocks the whole registry. Returns the aggregate
/// stats. Used by the optional periodic decay tick in `main.rs`.
pub async fn decay_all(
    state: &Arc<RwLock<ServerState>>,
    half_life_secs: f64,
    floor: f64,
    prune: bool,
) -> crate::types::DecayStats {
    let entries: Vec<Arc<RwLock<GraphCore>>> = {
        let s = state.read().await;
        s.registry
            .all_entries()
            .iter()
            .map(|e| e.core.clone())
            .collect()
    };
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let mut total = crate::types::DecayStats::default();
    for core in entries {
        let mut g = core.write().await;
        let s = g.decay_sweep(now, half_life_secs, floor, prune);
        total.nodes_decayed += s.nodes_decayed;
        total.edges_decayed += s.edges_decayed;
        total.nodes_pruned += s.nodes_pruned;
        total.edges_pruned += s.edges_pruned;
    }
    total
}

/// Reconstruct the registry from the persist dir on startup.
///
/// Returns the number of graphs loaded. No-op (Ok(0)) when no persist dir or no
/// manifest is present (fresh start). Graphs absent from the live registry are
/// created with their recorded type; existing ones (e.g. `__bus__`) are filled.
pub async fn load_all(state: &Arc<RwLock<ServerState>>) -> Result<usize, String> {
    let dir = {
        let s = state.read().await;
        match &s.persist_dir {
            Some(d) => d.clone(),
            None => return Ok(0),
        }
    };
    let manifest_path = Path::new(&dir).join(MANIFEST);
    if !manifest_path.exists() {
        return Ok(0);
    }
    let manifest: serde_json::Map<String, serde_json::Value> =
        serde_json::from_slice(&std::fs::read(&manifest_path).map_err(|e| e.to_string())?)
            .map_err(|e| e.to_string())?;

    let mut count = 0usize;
    for (fname, meta) in manifest {
        let name = meta
            .get("name")
            .and_then(|v| v.as_str())
            .unwrap_or(&fname)
            .to_string();
        let gtype: GraphType = meta
            .get("graph_type")
            .cloned()
            .and_then(|v| serde_json::from_value(v).ok())
            .unwrap_or(GraphType::Global);
        let path = Path::new(&dir).join(format!("{fname}.mp"));
        if !path.exists() {
            continue;
        }
        let bytes = std::fs::read(&path).map_err(|e| e.to_string())?;
        let core = {
            let mut s = state.write().await;
            if !s.registry.exists(&name) {
                let _ = s.registry.create_graph(&name, gtype, None);
            }
            s.registry.get_mut(&name).map(|e| e.core.clone())
        };
        if let Some(core) = core {
            let mut g = core.write().await;
            g.from_msgpack(&bytes)?;
            count += 1;
        }
    }
    info!("Loaded {} graphs from {}", count, dir);
    Ok(count)
}
