//! Single-writer guard for a persist directory (CONCEPT:EG-KG.storage.nonblocking-checkpoint / OS-5.9, Phase B1).
//!
//! The facade's path to the ONE lock implementation, which lives in
//! `eg_core::persist_lock` so callers below the facade in the workspace DAG
//! (`crates/eg-pyengine`) take the SAME exclusive `flock` on
//! `<persist_dir>/engine.lock` rather than a second, independently-drifting copy.
//! A second engine started on the same dir would checkpoint the same per-graph
//! files and clobber the first's snapshots, so it fails the lock and refuses to
//! start. The lock auto-releases when the holder dies.

pub use eg_core::persist_lock::{acquire, is_held, PersistDirLock};
