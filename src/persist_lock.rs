//! Single-writer guard for a persist directory (CONCEPT:KG-2.8 / OS-5.9, Phase B1).
//!
//! Exactly one engine may own a persist dir. A second engine started on the same
//! dir checkpoints the SAME per-graph `.mp` files and clobbers the first's
//! snapshots — the 107GB-orphan / corrupted-snapshot incident that motivated the
//! Python-side spawn guard. This closes the same class at the ENGINE level: the
//! engine takes an EXCLUSIVE advisory `flock` on `<persist_dir>/engine.lock` for
//! its whole lifetime, so a second engine on the same dir fails the lock and
//! refuses to start. The lock auto-releases when the holder dies (advisory locks
//! are released by the kernel on process exit), so a crashed engine never leaves a
//! stale lock blocking restart.

use fs4::FileExt;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::Path;

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
             engine — refusing to start a second engine on the same --persist-dir \
             (they would clobber each other's snapshots). Stop the other engine first."
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn second_acquire_on_same_dir_fails() {
        let dir = std::env::temp_dir().join(format!("eg-locktest-{}", std::process::id()));
        let dir = dir.to_str().unwrap();
        let held = acquire(dir).expect("first acquire succeeds");
        // A second acquire of the SAME dir must fail while the first is held.
        assert!(acquire(dir).is_err(), "second acquire should be refused");
        drop(held);
        // After releasing, a fresh acquire succeeds again.
        assert!(acquire(dir).is_ok(), "acquire after release should succeed");
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn distinct_dirs_do_not_contend() {
        let base = std::env::temp_dir().join(format!("eg-locktest2-{}", std::process::id()));
        let a = base.join("a");
        let b = base.join("b");
        let la = acquire(a.to_str().unwrap()).expect("a");
        let lb = acquire(b.to_str().unwrap()).expect("b");
        drop((la, lb));
        let _ = std::fs::remove_dir_all(&base);
    }
}
