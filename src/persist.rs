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
pub(crate) fn sanitize(name: &str) -> String {
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
/// configured. The global state lock is released before serializing, and for each
/// graph the per-graph read lock is held ONLY long enough to clone an owned
/// `GraphSnapshot` (a memcpy) — the slow MessagePack encode then runs OFF the lock.
/// Previously the read lock was held through the whole encode, which on a 450MB
/// graph blocks every concurrent writer for ~10s each checkpoint (the periodic
/// ingest "freeze"). (CONCEPT:KG-2.8 — non-blocking checkpoint, A1)
pub async fn checkpoint_all(state: &Arc<RwLock<ServerState>>) -> Result<usize, String> {
    let start = std::time::Instant::now();
    let (dir, entries) = {
        let s = state.read().await;
        let dir = match &s.persist_dir {
            Some(d) => d.clone(),
            None => return Ok(0),
        };
        let entries: Vec<(
            String,
            GraphType,
            Arc<RwLock<GraphCore>>,
            crate::registry::WalHandle,
        )> = s
            .registry
            .all_entries()
            .iter()
            .map(|e| (e.name.clone(), e.graph_type, e.core.clone(), e.wal.clone()))
            .collect();
        (dir, entries)
    };

    std::fs::create_dir_all(&dir).map_err(|e| e.to_string())?;
    let mut manifest = serde_json::Map::new();
    let mut count = 0usize;
    for (name, gtype, core, wal) in entries {
        // WAL position the snapshot will cover. Captured BEFORE the snapshot so the
        // truncation is loss-free: any op appended during this checkpoint lands at
        // an offset ≥ this position and survives the prefix truncation. (Phase B2)
        let wal_pos = wal.lock().as_ref().map(|w| w.position());
        // Brief lock: clone the serializable state, then release immediately.
        let snapshot = {
            let g = core.read().await;
            g.snapshot()
        };
        // Slow path (MessagePack encode of the cloned snapshot) runs OFF the lock,
        // so concurrent writers to this graph are not blocked during the encode.
        let bytes = snapshot.to_msgpack()?;
        let fname = sanitize(&name);
        let path = Path::new(&dir).join(format!("{fname}.mp"));
        atomic_write(&path, &bytes)?;
        // Snapshot is durable on disk → drop the WAL prefix it superseded.
        if let Some(pos) = wal_pos {
            if let Some(w) = wal.lock().as_mut() {
                if let Err(e) = w.truncate_prefix(pos) {
                    tracing::warn!("WAL truncate failed for '{}': {}", name, e);
                }
            }
        }
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
/// Returns the number of graphs loaded. No-op (Ok(0)) when no persist dir is
/// configured. Loads each graph's snapshot (from the manifest) and replays its WAL
/// tail on top. ALSO replays any orphan WAL — a graph mutated but crashed before
/// its first checkpoint (so it has no manifest entry / snapshot), e.g. `__bus__`
/// after a hard kill — so the WAL is loss-free even with no prior checkpoint.
pub async fn load_all(state: &Arc<RwLock<ServerState>>) -> Result<usize, String> {
    let dir = {
        let s = state.read().await;
        match &s.persist_dir {
            Some(d) => d.clone(),
            None => return Ok(0),
        }
    };
    let manifest_path = Path::new(&dir).join(MANIFEST);
    // Missing manifest is NOT an early return: a graph may have a WAL but no
    // snapshot yet (crashed before the first checkpoint). Fall through with an
    // empty manifest so the orphan-WAL recovery below still runs.
    let manifest: serde_json::Map<String, serde_json::Value> = if manifest_path.exists() {
        serde_json::from_slice(&std::fs::read(&manifest_path).map_err(|e| e.to_string())?)
            .map_err(|e| e.to_string())?
    } else {
        serde_json::Map::new()
    };

    let mut count = 0usize;
    let mut recovered_fnames: std::collections::HashSet<String> = std::collections::HashSet::new();
    for (fname, meta) in manifest {
        recovered_fnames.insert(fname.clone());
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
            // Replay the WAL tail on top of the snapshot (Phase B2): recovers every
            // durable mutation since the last checkpoint, so warm restart loses
            // nothing the WAL captured rather than everything since the checkpoint.
            let wal_file = crate::wal::wal_path(&dir, &fname);
            if wal_file.exists() {
                let replayed = crate::wal::replay(&mut g, &wal_file);
                if replayed > 0 {
                    info!(
                        "WAL replay: {} op(s) recovered for graph '{}'",
                        replayed, name
                    );
                }
            }
            count += 1;
        }
    }

    // Orphan-WAL recovery (Phase B2): replay any `*.wal` whose graph had no snapshot
    // in the manifest — mutated but crashed before its first checkpoint. The graph
    // name is the `.wal` file stem (exact for `__bus__` and tenant names without
    // filename-sanitized characters); `__bus__` already exists in the registry, so
    // its WAL replays into the live core.
    if let Ok(rd) = std::fs::read_dir(&dir) {
        for ent in rd.flatten() {
            let p = ent.path();
            if p.extension().and_then(|e| e.to_str()) != Some("wal") {
                continue;
            }
            let fname = match p.file_stem().and_then(|s| s.to_str()) {
                Some(s) => s.to_string(),
                None => continue,
            };
            if recovered_fnames.contains(&fname) {
                continue; // already handled with its snapshot above
            }
            let core = {
                let mut s = state.write().await;
                if !s.registry.exists(&fname) {
                    let _ = s.registry.create_graph(&fname, GraphType::Global, None);
                }
                s.registry.get_mut(&fname).map(|e| e.core.clone())
            };
            if let Some(core) = core {
                let mut g = core.write().await;
                let n = crate::wal::replay(&mut g, &p);
                if n > 0 {
                    info!(
                        "WAL-only recovery: {} op(s) for graph '{}' (no prior snapshot)",
                        n, fname
                    );
                    count += 1;
                }
            }
        }
    }
    info!("Loaded {} graphs from {}", count, dir);
    Ok(count)
}
