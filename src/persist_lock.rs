//! Single-writer guard for a persist directory (CONCEPT:EG-KG.storage.nonblocking-checkpoint / OS-5.9, Phase B1).
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

/// The lock file [`acquire`] holds, exclusively, for the engine's lifetime.
const LOCK_FILE: &str = "engine.lock";

/// Whether an engine currently holds the lock of `persist_dir`, asked without
/// creating or writing the lock file, so a read-only tool can refuse a running
/// engine and still leave the directory byte-for-byte as it found it.
pub fn is_held(persist_dir: &Path) -> Result<bool, String> {
    let path = persist_dir.join(LOCK_FILE);
    let file = match File::open(&path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(format!("cannot open {}: {error}", path.display())),
    };
    // A shared lock conflicts with the holder's exclusive one and is released
    // again when `file` is dropped.
    match File::try_lock_shared(&file) {
        Ok(()) => Ok(false),
        Err(std::fs::TryLockError::WouldBlock) => Ok(true),
        Err(std::fs::TryLockError::Error(error)) => {
            Err(format!("cannot probe {}: {error}", path.display()))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_held_lock_is_seen_without_creating_or_writing_the_lock_file() {
        let dir = tempfile::tempdir().unwrap();
        assert_eq!(is_held(dir.path()), Ok(false));
        assert!(!dir.path().join(LOCK_FILE).exists(), "the probe created it");
        let held = acquire(dir.path().to_str().unwrap()).expect("acquire");
        let stamped = std::fs::read(dir.path().join(LOCK_FILE)).unwrap();
        assert_eq!(is_held(dir.path()), Ok(true));
        drop(held);
        assert_eq!(is_held(dir.path()), Ok(false));
        assert_eq!(std::fs::read(dir.path().join(LOCK_FILE)).unwrap(), stamped);
    }

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
