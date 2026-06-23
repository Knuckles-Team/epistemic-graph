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

/// Emit a "loaded N/total graphs" progress line every this-many graphs during a
/// multi-graph snapshot load — and only when the registry is larger than this, so
/// the common small restart stays quiet (CONCEPT:KG-2.200).
const PROGRESS_EVERY: usize = 500;

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
///
/// The tmp file's contents are `fsync`'d BEFORE the rename: a rename is only
/// crash-safe if the data it will point at is already durable, otherwise a power
/// loss can land a rename onto a zero-length/partial snapshot (silent corruption
/// the atomicity was meant to prevent). The parent directory is `fsync`'d AFTER
/// the rename so the directory entry change itself survives power loss too.
fn atomic_write(path: &Path, bytes: &[u8]) -> Result<(), String> {
    use std::io::Write;
    let tmp = path.with_extension("tmp");
    {
        let mut f = std::fs::File::create(&tmp).map_err(|e| e.to_string())?;
        f.write_all(bytes).map_err(|e| e.to_string())?;
        f.sync_all().map_err(|e| e.to_string())?;
    }
    std::fs::rename(&tmp, path).map_err(|e| e.to_string())?;
    if let Some(parent) = path.parent() {
        // Best-effort: not all filesystems require/allow a dir fsync; failures
        // here don't undo the rename, so don't fail the checkpoint on them.
        if let Ok(dir) = std::fs::File::open(parent) {
            let _ = dir.sync_all();
        }
    }
    Ok(())
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
pub async fn checkpoint_all(
    state: &Arc<RwLock<ServerState>>,
    wal_service: Option<&crate::wal_service::WalService>,
) -> Result<usize, String> {
    let start = std::time::Instant::now();
    let (dir, entries) = {
        let s = state.read().await;
        let dir = match &s.persist_dir {
            Some(d) => d.clone(),
            None => return Ok(0),
        };
        let entries: Vec<(String, GraphType, Arc<GraphCore>)> = s
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
    let mut skipped = 0usize;
    for (name, gtype, core) in entries {
        let fname = sanitize(&name);
        let path = Path::new(&dir).join(format!("{fname}.mp"));
        // Incremental checkpointing (Phase C-C): atomically clear-and-test the dirty
        // flag BEFORE snapshotting, so a write that races this checkpoint re-marks
        // the graph dirty and is captured by the NEXT checkpoint (never lost). A
        // clean graph whose `.mp` already exists is skipped entirely — its on-disk
        // snapshot is still current — but it stays in the manifest so restore loads
        // it. An idle tenant therefore costs zero checkpoint encode/I/O.
        let was_dirty = core.take_dirty();
        if !was_dirty && path.exists() {
            manifest.insert(
                fname,
                serde_json::json!({ "name": name, "graph_type": gtype }),
            );
            skipped += 1;
            continue;
        }
        // WAL position the snapshot will cover. Captured BEFORE the snapshot so the
        // truncation is loss-free: any op appended during this checkpoint lands at
        // an offset ≥ this position and survives the prefix truncation. The service
        // processes Position in-order with appends, so it reflects exactly the ops
        // enqueued before now. 0 ⇒ nothing logged ⇒ no truncation. (Phase B3)
        let wal_pos = wal_service
            .map(|svc| svc.position(&fname))
            .filter(|&p| p > 0);
        // snapshot() takes the topology read lock internally for a consistent
        // point-in-time copy, then releases it; the encode below runs off-lock.
        let snapshot = core.snapshot();
        // Slow path (MessagePack encode of the cloned snapshot) runs OFF the lock,
        // so concurrent writers to this graph are not blocked during the encode.
        let bytes = snapshot.to_msgpack()?;
        atomic_write(&path, &bytes)?;
        // Snapshot is durable on disk → drop the WAL prefix it superseded.
        if let (Some(svc), Some(pos)) = (wal_service, wal_pos) {
            if let Err(e) = svc.truncate(&fname, pos) {
                tracing::warn!("WAL truncate failed for '{}': {}", name, e);
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
    info!(
        "Checkpoint wrote {} graph(s), skipped {} clean, to {}",
        count, skipped, dir
    );
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
    let entries: Vec<Arc<GraphCore>> = {
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
        let s = core.decay_sweep(now, half_life_secs, floor, prune);
        total.nodes_decayed += s.nodes_decayed;
        total.edges_decayed += s.edges_decayed;
        total.nodes_pruned += s.nodes_pruned;
        total.edges_pruned += s.edges_pruned;
    }
    total
}

/// Evict any graph above `max_nodes` back down to it (LRU), returning the number
/// of nodes evicted across all graphs. Mirrors `decay_all`'s lock discipline
/// (global registry lock released before the per-graph evictions).
///
/// Two modes:
///
/// * **Default (rebuildable cache, see AGENTS.md):** the engine is a cache over an
///   EXTERNAL durable backend, so a dropped LRU node re-hydrates on next access.
///   Drop the oldest nodes; this is the memory backstop that makes a shard DEGRADE
///   under pressure instead of OOM-killing every tenant on it. Unchanged.
///
/// * **redb-AUTHORITATIVE (CONCEPT:KG-2.187/2.191):** redb IS the source of truth
///   and there is no external tier — but with read-through-on-RAM-miss now wired,
///   an evicted node is still readable (its properties are served back from redb on
///   a miss). So the cap resumes EVICTING to bound memory, WITHOUT data loss, by a
///   strict durability gate: a candidate is dropped ONLY after a redb point-read
///   CONFIRMS its properties are durably on disk. Commit-before-ack makes that the
///   common case (every acked write is already committed); the explicit check is
///   the hard no-loss guarantee — a node whose durability can't be confirmed is
///   left resident, never evicted.
pub async fn evict_oversized_all(state: &Arc<RwLock<ServerState>>, max_nodes: usize) -> usize {
    if max_nodes == 0 {
        return 0;
    }
    let (authoritative, backend, entries) = {
        let s = state.read().await;
        let entries: Vec<(String, Arc<GraphCore>)> = s
            .registry
            .all_entries()
            .iter()
            .map(|e| (e.name.clone(), e.core.clone()))
            .collect();
        (s.redb_authoritative, s.persistence.clone(), entries)
    };

    if !authoritative {
        // Default rebuildable-cache eviction — drop the oldest, unchanged.
        let mut evicted = 0usize;
        for (_name, core) in entries {
            if core.node_count() > max_nodes {
                evicted += core.evict_lru(max_nodes);
            }
        }
        return evicted;
    }

    // redb-authoritative: durability-gated eviction (CONCEPT:KG-2.191). Without a
    // backend there is nothing durable to confirm against, so never drop.
    let backend = match backend {
        Some(b) => b,
        None => return 0,
    };
    let mut evicted = 0usize;
    let mut deferred = 0usize;
    for (name, core) in entries {
        let candidates = core.lru_eviction_candidates(max_nodes);
        if candidates.is_empty() {
            continue;
        }
        let fname = sanitize(&name);
        for node_id in candidates {
            // Confirm durable in redb BEFORE dropping from RAM. Only a confirmed
            // Some(_) (the node's properties are on disk) is safe to evict — with
            // read-through it stays readable. A None (not yet durable) or an Err
            // (read failure) means we CANNOT prove durability, so we keep the node
            // resident: no data loss, ever.
            match backend.read_node(&fname, &node_id).await {
                Ok(Some(_)) => {
                    core.remove_node(node_id);
                    evicted += 1;
                }
                Ok(None) | Err(_) => {
                    deferred += 1;
                }
            }
        }
    }
    if deferred > 0 {
        tracing::warn!(
            "redb authoritative eviction: kept {} node(s) resident — durability not yet \
             confirmed in redb (no data loss; they evict once durable)",
            deferred
        );
    }
    evicted
}

/// Reconstruct the registry from the persist dir on startup.
///
/// Returns the number of graphs loaded. No-op (Ok(0)) when no persist dir is
/// configured. Loads each graph's snapshot (from the manifest) and replays its WAL
/// tail on top. ALSO replays any orphan WAL — a graph mutated but crashed before
/// its first checkpoint (so it has no manifest entry / snapshot), e.g. `__commons__`
/// after a hard kill — so the WAL is loss-free even with no prior checkpoint.
/// One-time data migration: the shared commons graph was renamed `__bus__` →
/// `__commons__`. Rewrite any on-disk legacy artifacts (snapshot, WAL, manifest
/// entry incl. the old `"Bus"` graph_type) so an engine that persisted the old
/// name loads cleanly under the new one — after which the old files are gone. This
/// is the *persisted-state* exception to No-Legacy: a one-time read-old→write-new,
/// not a permanent dual-name reader.
fn migrate_legacy_commons(dir: &str) {
    let base = Path::new(dir);
    let mut migrated = false;
    for (old, new) in [
        ("__bus__.mp", "__commons__.mp"),
        ("__bus__.wal", "__commons__.wal"),
    ] {
        let (old_p, new_p) = (base.join(old), base.join(new));
        if old_p.exists() && !new_p.exists() && std::fs::rename(&old_p, &new_p).is_ok() {
            migrated = true;
        }
    }
    let manifest_path = base.join(MANIFEST);
    if let Ok(bytes) = std::fs::read(&manifest_path) {
        if let Ok(mut m) =
            serde_json::from_slice::<serde_json::Map<String, serde_json::Value>>(&bytes)
        {
            if let Some(mut entry) = m.remove("__bus__") {
                if let Some(obj) = entry.as_object_mut() {
                    obj.insert("name".into(), serde_json::json!("__commons__"));
                    if obj.get("graph_type").and_then(|v| v.as_str()) == Some("Bus") {
                        obj.insert("graph_type".into(), serde_json::json!("Commons"));
                    }
                }
                m.insert("__commons__".into(), entry);
                if let Ok(out) = serde_json::to_vec(&m) {
                    let _ = atomic_write(&manifest_path, &out);
                }
                migrated = true;
            }
        }
    }
    if migrated {
        info!("Migrated legacy '__bus__' commons graph → '__commons__'");
    }
}

pub async fn load_all(
    state: &Arc<RwLock<ServerState>>,
    // The snapshot+WAL recovery replays WAL tails directly from disk
    // (`crate::wal::replay`), so it does not consult the live writer handle; the
    // parameter keeps `load_all`/`checkpoint_all` symmetric for the backend trait.
    _wal_service: Option<&crate::wal_service::WalService>,
) -> Result<usize, String> {
    let dir = {
        let s = state.read().await;
        match &s.persist_dir {
            Some(d) => d.clone(),
            None => return Ok(0),
        }
    };
    // One-time rename of the legacy `__bus__` commons graph to `__commons__` before
    // anything is read, so the rest of load runs purely on the new name.
    migrate_legacy_commons(&dir);
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
    // Progress logging (CONCEPT:KG-2.200): a many-graph load (a homelab carries
    // thousands of tenant graphs; the one-time redb migration replays every one) is
    // otherwise a multi-second SILENT gap before the socket binds. Emit a periodic
    // "N/total graphs loaded" so an operator can SEE it advancing and distinguish
    // slow-but-working from hung. Cheap (a divisible-by check) and quiet for the
    // common small-registry restart.
    let total = manifest.len();
    if total > PROGRESS_EVERY {
        info!("Snapshot load: {} graph(s) to restore from {}", total, dir);
    }
    let mut processed = 0usize;
    let mut recovered_fnames: std::collections::HashSet<String> = std::collections::HashSet::new();
    for (fname, meta) in manifest {
        recovered_fnames.insert(fname.clone());
        processed += 1;
        if total > PROGRESS_EVERY && processed.is_multiple_of(PROGRESS_EVERY) {
            info!(
                "Snapshot load progress: {}/{} graph(s) processed ({} restored)",
                processed, total, count
            );
        }
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
            // A corrupt/unreadable snapshot for ONE graph must NOT abort the whole
            // load (CONCEPT:KG-2.200). Aborting here would, under the redb migration,
            // hand checkpoint_all a PARTIAL registry and seed redb with a TRUNCATED
            // store that the next boot considers "already migrated" — silent data
            // loss for every graph after the bad one. Skip the bad graph loudly and
            // keep going; the original `.mp` stays on disk as a backstop.
            if let Err(e) = core.from_msgpack(&bytes) {
                tracing::warn!("snapshot load skipped for graph '{}': {}", name, e);
                continue;
            }
            // Replay the WAL tail on top of the snapshot (Phase B2): recovers every
            // durable mutation since the last checkpoint, so warm restart loses
            // nothing the WAL captured rather than everything since the checkpoint.
            let wal_file = crate::wal::wal_path(&dir, &fname);
            if wal_file.exists() {
                let replayed = crate::wal::replay(&core, &wal_file);
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
    // name is the `.wal` file stem (exact for `__commons__` and tenant names without
    // filename-sanitized characters); `__commons__` already exists in the registry, so
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
                let n = crate::wal::replay(&core, &p);
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
