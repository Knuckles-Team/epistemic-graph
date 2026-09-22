//! A throwaway graph shard for WorkItem-row store tests: rows are written in
//! one admitted maintenance transaction through the SAME scoped node table the
//! native transitions use.

use super::*;

pub(super) const GRAPH: &str = "graph-a";

pub(super) struct TempShard {
    path: std::path::PathBuf,
    pub(super) shard: Shard,
}

impl Drop for TempShard {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

pub(super) fn open(tag: &str) -> TempShard {
    let path = crate::redb_store::temp_path("eg-work-item-rows", tag);
    let shard = Shard::open(&path).expect("temp shard opens");
    TempShard { path, shard }
}

/// Run `write` against `GRAPH`'s node table in one committed transaction.
pub(super) fn with_nodes<R>(
    shard: &Shard,
    tag: &str,
    write: impl FnOnce(
        &mut ScopedOwnerTableMut<'_, (&'static str, &'static str), &'static [u8]>,
    ) -> Result<R, String>,
) -> R {
    let members = shard.graph_members(&[GRAPH]).unwrap();
    let op_id = format!("work-item-rows-test/{tag}");
    let (group, batches) = shard.admit_maintenance(&members, &op_id).unwrap();
    let admitted = ShardWrite::open(shard, &group, &members, &batches).unwrap();
    let outcome = {
        let member = admitted.graph(GRAPH).unwrap();
        let mut nodes = member.open_scoped_table(NODES).unwrap();
        write(&mut nodes).unwrap()
    };
    admitted.finish().unwrap();
    shard.commit_drain(group, &batches, 0).unwrap();
    outcome
}
