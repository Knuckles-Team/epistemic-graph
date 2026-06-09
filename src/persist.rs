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
        .map(|c| if c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.') { c } else { '_' })
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
    info!("Checkpoint wrote {} graphs to {}", count, dir);
    Ok(count)
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
