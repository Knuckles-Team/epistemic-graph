//! Single-writer guard for a persist directory, hoisted down from the facade's
//! `src/persist_lock.rs` (CONCEPT:EG-KG.storage.nonblocking-checkpoint / OS-5.9,
//! Phase B1) so a caller below the facade in the workspace DAG (`crates/eg-pyengine`)
//! can take the SAME exclusive lock — not a second, independently-drifting
//! mechanism. `acquire` below is the SAME `flock`-on-`engine.lock` scheme the facade
//! already ships and the live 13GB persist directory already contains a lock file
//! from; only its location moved.
//!
//! Exactly one engine may own a persist dir. A second engine started on the same
//! dir checkpoints the SAME per-graph files and clobbers the first's snapshots —
//! this closes that class at the engine level: the engine takes an EXCLUSIVE
//! advisory `flock` on `<persist_dir>/engine.lock` for its whole lifetime, so a
//! second engine on the same dir fails the lock and refuses to start. The lock
//! auto-releases when the holder dies (advisory locks are released by the kernel on
//! process exit), so a crashed engine never leaves a stale lock blocking restart.

use fs4::FileExt;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

/// Hold the exclusive persist-dir lock. Dropping it releases the lock, so the
/// caller must keep it alive for the whole process lifetime.
pub struct PersistDirLock {
    _file: File,
}

/// Acquire the exclusive persist-dir lock, or return a descriptive error if
/// another engine already owns the directory.
pub fn acquire(persist_dir: &str) -> Result<PersistDirLock, String> {
    std::fs::create_dir_all(persist_dir).map_err(|e| e.to_string())?;
    let path = Path::new(persist_dir).join("engine.lock");
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(&path)
        .map_err(|e| format!("cannot open {}: {e}", path.display()))?;
    match file.try_lock_exclusive() {
        Ok(()) => {
            // Best-effort: stamp our pid so an operator can see who holds it.
            let _ = file.set_len(0);
            let _ = (&file).write_all(format!("{}\n", std::process::id()).as_bytes());
            Ok(PersistDirLock { _file: file })
        }
        Err(_) => Err(format!(
            "persist dir {persist_dir} is already locked by another epistemic-graph \
             engine — refusing to start a second engine on the same persist dir \
             (they would clobber each other's snapshots). Stop the other engine first."
        )),
    }
}

/// Where a durable-mutation-capable caller stores its data. Deliberately has **no**
/// `Default` impl and no third "unset ⇒ pick a path for you" arm anywhere in this
/// crate: a caller MUST explicitly choose one of these two variants.
///
/// This is the structural fix for the deployed-system defect this hoist exists to
/// make impossible for a pyengine caller: `agent_utilities/knowledge_graph/core/
/// graph_compute.py:1914` reads `GRAPH_SERVICE_PERSIST_DIR`, and — today — an unset
/// value silently falls back to `agent_utilities.core.paths.data_dir()/
/// "graph_snapshots"`, which on the live pod resolves inside an `emptyDir`, not the
/// PVC, discarding the whole graph on every restart with no error and no warning
/// (`plans/pyengine/EG-PYENGINE-PLAN.md` §1.9, §9.2 Gate A; registered as
/// BUG-PE-003). The Python composition layer that will eventually read that env var
/// (`epistemic_graph/embedded.py`, a different lane) has no legal way to construct
/// an engine without deciding which variant applies — there is no path this type can
/// silently default to.
pub enum PersistMode {
    /// Durable: the caller commits through the redb-backed store at this directory,
    /// which is exclusively locked for the process's lifetime via [`acquire`].
    Durable(PathBuf),
    /// Explicit, logged choice: nothing this engine writes survives a restart.
    /// Never a default — see [`open_persist_mode`]'s log line.
    InMemoryOnly,
}

/// Open a [`PersistMode`]: for `Durable`, acquire the single-writer lock (refusing
/// to start — `Err` — if another engine already holds this directory); for
/// `InMemoryOnly`, emit one unambiguous, greppable `tracing::warn!` line stating
/// that no persistence is configured, and succeed with no lock held.
///
/// Returns the held lock (`None` for `InMemoryOnly`) — the caller must keep it alive
/// for the engine's whole lifetime, exactly like [`acquire`]'s own contract.
pub fn open_persist_mode(mode: PersistMode) -> Result<Option<PersistDirLock>, String> {
    match mode {
        PersistMode::Durable(dir) => acquire(&dir.to_string_lossy()).map(Some),
        PersistMode::InMemoryOnly => {
            tracing::warn!(
                "eg-core: engine constructed with PersistMode::InMemoryOnly — NO \
                 persistence is configured; every write will be lost on restart or \
                 crash. This must be an explicit caller choice, never a silent \
                 default (see PersistMode's doc comment / BUG-PE-003)."
            );
            Ok(None)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn second_acquire_on_same_dir_fails() {
        let dir = std::env::temp_dir().join(format!(
            "eg-core-locktest-{}-{}",
            std::process::id(),
            line!()
        ));
        let dir = dir.to_str().unwrap();
        let held = acquire(dir).expect("first acquire succeeds");
        // A second acquire of the SAME dir must fail while the first is held —
        // this is the concrete proof that BUG-PE-003's failure mode (two writers on
        // one persist dir) is structurally rejected.
        assert!(acquire(dir).is_err(), "second acquire should be refused");
        drop(held);
        // After releasing, a fresh acquire succeeds again.
        assert!(acquire(dir).is_ok(), "acquire after release should succeed");
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn distinct_dirs_do_not_contend() {
        let base = std::env::temp_dir().join(format!(
            "eg-core-locktest2-{}-{}",
            std::process::id(),
            line!()
        ));
        let a = base.join("a");
        let b = base.join("b");
        let la = acquire(a.to_str().unwrap()).expect("a");
        let lb = acquire(b.to_str().unwrap()).expect("b");
        drop((la, lb));
        let _ = std::fs::remove_dir_all(&base);
    }

    #[test]
    fn persist_mode_durable_holds_the_lock_and_rejects_a_second_opener() {
        let dir = std::env::temp_dir().join(format!(
            "eg-core-locktest-mode-{}-{}",
            std::process::id(),
            line!()
        ));
        let held = open_persist_mode(PersistMode::Durable(dir.clone()))
            .expect("first open succeeds")
            .expect("Durable mode returns a held lock");
        let second = open_persist_mode(PersistMode::Durable(dir.clone()));
        assert!(
            second.is_err(),
            "a second Durable open on the same dir must be refused"
        );
        drop(held);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn persist_mode_in_memory_only_never_takes_a_lock() {
        // No dir is touched at all — two InMemoryOnly opens never contend, and
        // neither creates or locks anything on disk.
        let a = open_persist_mode(PersistMode::InMemoryOnly).expect("in-memory always succeeds");
        let b = open_persist_mode(PersistMode::InMemoryOnly).expect("in-memory always succeeds");
        assert!(a.is_none());
        assert!(b.is_none());
    }
}
