//! Per-graph write-ahead log (CONCEPT:KG-2.8 / OS-5.9, Phase B2).
//!
//! The snapshot persistence (`persist.rs`) is an RDB-style point-in-time image
//! written every `--checkpoint-interval`. A crash between checkpoints loses every
//! mutation since the last one. The WAL closes that window: each durable mutation
//! is appended to `<persist_dir>/<graph>.wal` as it is applied, and on restart the
//! engine loads the snapshot and then REPLAYS the WAL tail — so warm restart
//! recovers right up to the last flushed op instead of up to the last checkpoint.
//! (pggraph remains the cross-restart system-of-record; this is the fast local
//! crash-consistency layer.) A checkpoint supersedes the logged ops, so it
//! truncates the WAL afterward.
//!
//! Durability model: `append` FLUSHES (a `write`, no `fsync`) so a PROCESS crash
//! (the common case — kill/segfault) keeps the data in the OS page cache and it
//! survives; `fsync` happens at checkpoint, bounding hard-power-loss exposure to
//! the checkpoint interval (same as before, but now with no between-checkpoint
//! loss on process crashes). Replay tolerates a torn trailing record (a partial
//! op from a crash mid-append) by stopping at the first short/garbage read.
//!
//! Only the persistent DATA mutations are logged. Derived/maintenance ops (decay,
//! evict, touch, parse-repository) are recomputable or best-effort and would bloat
//! the log; they are intentionally excluded.

use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use crate::graph::GraphCore;
use crate::protocol::Method;

/// True for the methods whose effect must survive a crash via the WAL.
pub fn is_durable_mutation(m: &Method) -> bool {
    matches!(
        m,
        Method::AddNode { .. }
            | Method::RemoveNode { .. }
            | Method::AddEdge { .. }
            | Method::RemoveEdge { .. }
            | Method::BatchUpdate { .. }
            | Method::ClearGraph
    )
}

/// Append-only writer for one graph's WAL.
///
/// A plain `File` (no `BufWriter`) so each `append` is a direct `write` that the
/// OS persists immediately — a PROCESS crash keeps it. `len` tracks the logical
/// write position so the checkpoint can capture a position and later truncate
/// exactly the prefix the snapshot superseded (position-based truncation), which
/// is what makes checkpoint + WAL loss-free without holding a lock across the slow
/// snapshot encode.
#[derive(Debug)]
pub struct WalWriter {
    file: File,
    len: u64,
}

impl WalWriter {
    /// Open (or create) the WAL file, positioned for append.
    pub fn open(path: &Path) -> std::io::Result<Self> {
        let mut file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(path)?;
        let len = file.metadata()?.len();
        file.seek(SeekFrom::End(0))?;
        Ok(Self { file, len })
    }

    /// Append a length-prefixed (u32 LE) MessagePack-encoded method.
    pub fn append(&mut self, method: &Method) -> std::io::Result<()> {
        let bytes = rmp_serde::to_vec_named(method)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        let prefix = (bytes.len() as u32).to_le_bytes();
        self.file.write_all(&prefix)?;
        self.file.write_all(&bytes)?;
        self.len += prefix.len() as u64 + bytes.len() as u64;
        Ok(())
    }

    /// Current logical end position — captured at checkpoint snapshot time and
    /// passed back to [`Self::truncate_prefix`] after the snapshot is durable.
    pub fn position(&self) -> u64 {
        self.len
    }

    /// Discard the first `prefix` bytes (the records the snapshot now supersedes),
    /// keeping everything appended after the checkpoint captured `prefix`. This is
    /// loss-free: ops appended DURING the checkpoint live at offsets ≥ `prefix` and
    /// are retained for replay. fsync'd so the truncation is durable.
    pub fn truncate_prefix(&mut self, prefix: u64) -> std::io::Result<()> {
        self.file.flush()?;
        if prefix >= self.len {
            self.file.set_len(0)?;
            self.file.seek(SeekFrom::Start(0))?;
            self.len = 0;
            return self.file.sync_all();
        }
        let mut tail = Vec::new();
        self.file.seek(SeekFrom::Start(prefix))?;
        self.file.read_to_end(&mut tail)?;
        self.file.set_len(0)?;
        self.file.seek(SeekFrom::Start(0))?;
        self.file.write_all(&tail)?;
        self.len = tail.len() as u64;
        self.file.sync_all()
    }
}

/// Apply one logged method to a graph (replay). Mirrors the dispatch mutation
/// handlers for exactly the `is_durable_mutation` set.
fn apply(core: &mut GraphCore, m: &Method) {
    match m {
        Method::AddNode {
            node_id,
            properties_msgpack,
        } => core.add_node(node_id.clone(), properties_msgpack.clone()),
        Method::RemoveNode { node_id } => core.remove_node(node_id.clone()),
        Method::AddEdge {
            source_id,
            target_id,
            properties_msgpack,
        } => {
            let _ = core.add_edge(
                source_id.clone(),
                target_id.clone(),
                properties_msgpack.clone(),
            );
        }
        Method::RemoveEdge {
            source_id,
            target_id,
        } => core.remove_edge(source_id.clone(), target_id.clone()),
        Method::BatchUpdate { operations_msgpack } => {
            let _ = crate::algorithms::batch_update(core, operations_msgpack);
        }
        Method::ClearGraph => core.clear(),
        _ => {}
    }
}

/// Replay a WAL file into `core` (after the snapshot is loaded). Returns the
/// number of ops applied. A torn trailing record (partial op from a crash mid
/// append) ends replay cleanly rather than erroring.
pub fn replay(core: &mut GraphCore, path: &Path) -> usize {
    let mut buf = Vec::new();
    if File::open(path)
        .and_then(|mut f| f.read_to_end(&mut buf))
        .is_err()
    {
        return 0;
    }
    let mut off = 0usize;
    let mut applied = 0usize;
    while off + 4 <= buf.len() {
        let len = u32::from_le_bytes([buf[off], buf[off + 1], buf[off + 2], buf[off + 3]]) as usize;
        off += 4;
        if off + len > buf.len() {
            break; // torn tail — stop cleanly
        }
        match rmp_serde::from_slice::<Method>(&buf[off..off + len]) {
            Ok(m) => {
                apply(core, &m);
                applied += 1;
            }
            Err(_) => break,
        }
        off += len;
    }
    applied
}

/// The on-disk WAL path for a sanitized graph file-name within a persist dir.
pub fn wal_path(dir: &str, fname: &str) -> PathBuf {
    Path::new(dir).join(format!("{fname}.wal"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn props(v: serde_json::Value) -> Vec<u8> {
        rmp_serde::to_vec_named(&v).unwrap()
    }

    #[test]
    fn wal_roundtrip_recovers_mutations() {
        let dir = std::env::temp_dir().join(format!("eg-wal-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = wal_path(dir.to_str().unwrap(), "g");
        let pa = props(serde_json::json!({"type": "Code", "n": 1}));
        let pe = props(serde_json::json!({"type": "CALLS"}));
        {
            let mut w = WalWriter::open(&path).unwrap();
            w.append(&Method::AddNode {
                node_id: "a".into(),
                properties_msgpack: pa.clone(),
            })
            .unwrap();
            w.append(&Method::AddNode {
                node_id: "b".into(),
                properties_msgpack: props(serde_json::json!({"type": "Code"})),
            })
            .unwrap();
            w.append(&Method::AddEdge {
                source_id: "a".into(),
                target_id: "b".into(),
                properties_msgpack: pe,
            })
            .unwrap();
            w.append(&Method::RemoveNode {
                node_id: "b".into(),
            })
            .unwrap();
        }
        // Fresh graph + replay == the mutation sequence applied in order.
        let mut g = GraphCore::new();
        let n = replay(&mut g, &path);
        assert_eq!(n, 4);
        assert_eq!(g.node_count(), 1); // b was removed
        assert_eq!(g.get_node_properties("a"), Some(pa));
        assert!(g.get_node_properties("b").is_none());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn replay_tolerates_torn_tail() {
        let dir = std::env::temp_dir().join(format!("eg-wal-torn-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = wal_path(dir.to_str().unwrap(), "g");
        {
            let mut w = WalWriter::open(&path).unwrap();
            w.append(&Method::AddNode {
                node_id: "a".into(),
                properties_msgpack: props(serde_json::json!({"t": 1})),
            })
            .unwrap();
        }
        // Simulate a crash mid-append: a length header claiming more bytes than exist.
        {
            let mut f = OpenOptions::new().append(true).open(&path).unwrap();
            f.write_all(&(9999u32).to_le_bytes()).unwrap();
            f.write_all(b"partial").unwrap();
        }
        let mut g = GraphCore::new();
        let n = replay(&mut g, &path); // must not panic; applies the one good record
        assert_eq!(n, 1);
        assert_eq!(g.node_count(), 1);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn truncate_full_prefix_empties_the_log() {
        let dir = std::env::temp_dir().join(format!("eg-wal-trunc-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = wal_path(dir.to_str().unwrap(), "g");
        let mut w = WalWriter::open(&path).unwrap();
        w.append(&Method::AddNode {
            node_id: "a".into(),
            properties_msgpack: props(serde_json::json!({"t": 1})),
        })
        .unwrap();
        w.truncate_prefix(w.position()).unwrap();
        let mut g = GraphCore::new();
        assert_eq!(replay(&mut g, &path), 0);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn truncate_prefix_keeps_ops_appended_after_checkpoint() {
        // The checkpoint/WAL race: an op appended AFTER the checkpoint captured the
        // position must survive truncation (loss-free). Snapshot covers op#1; op#2
        // arrives during the checkpoint; truncate_prefix(pos_after_1) must keep op#2.
        let dir = std::env::temp_dir().join(format!("eg-wal-race-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = wal_path(dir.to_str().unwrap(), "g");
        let mut w = WalWriter::open(&path).unwrap();
        w.append(&Method::AddNode {
            node_id: "in_snapshot".into(),
            properties_msgpack: props(serde_json::json!({"t": 1})),
        })
        .unwrap();
        let pos = w.position(); // checkpoint captures here
        w.append(&Method::AddNode {
            node_id: "after_checkpoint".into(),
            properties_msgpack: props(serde_json::json!({"t": 2})),
        })
        .unwrap();
        w.truncate_prefix(pos).unwrap(); // op#1 superseded by snapshot, op#2 kept
        drop(w);

        let mut g = GraphCore::new();
        let n = replay(&mut g, &path);
        assert_eq!(n, 1, "only the post-checkpoint op should remain");
        assert!(g.get_node_properties("after_checkpoint").is_some());
        assert!(g.get_node_properties("in_snapshot").is_none());
        let _ = std::fs::remove_dir_all(&dir);
    }
}
