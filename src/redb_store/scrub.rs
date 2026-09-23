//! Bounded background node-payload scrub (EH-384,
//! CONCEPT:EG-KG.storage.node-payload-scrub).
//!
//! Until EH-290 every graph write commit unsealed every node of its graph as a
//! side effect of the lane-link check. That made each write O(graph size), and
//! it was also, by accident, the only thing that noticed an unreadable node
//! before something read it. EH-290 removed the scan. This module takes over
//! the detection as a deliberate, bounded job:
//!
//! * **Read-only.** A pass reads through kernel-issued MVCC scoped reads, so it
//!   never holds the write lock and never delays a commit.
//! * **Bounded.** One pass opens at most [`ScrubBudget::rows`] node payloads,
//!   and stops there.
//! * **Resumable.** A pass starts from a [`ScrubCursor`] `(graph, last node
//!   id)` and returns the next one. The cursor is persisted in the file-wide
//!   `storage_scrub_cursor` row, so a restart resumes where the scrub stopped
//!   rather than starting over. A full walk of every graph is one cycle.
//! * **Nothing skipped.** Every node row the pass reaches is opened. A row
//!   that cannot be opened becomes a typed [`NodeUnreadable`] finding. A
//!   storage error (as opposed to an unreadable payload) fails the pass,
//!   because it means the walk itself did not happen.

use super::dump::{read_graph_catalog, GraphDumpNodeRow};
use super::shard::{Shard, ShardWrite};
use super::{DurableCrypto, NodeUnreadable, NODES};
use redb::TableDefinition;
use serde::{Deserialize, Serialize};

/// The scrub cursor rows, one per scrub kind. File-wide (declared
/// `StorePrivate` in the graph-shard census).
pub(crate) const STORAGE_SCRUB_CURSOR: TableDefinition<&str, &[u8]> =
    TableDefinition::new("storage_scrub_cursor");

/// The cursor row of the node-payload scrub.
const NODE_PAYLOAD_CURSOR_KEY: &str = "node_payload";

/// Where the next pass starts. `graph == None` is the start of a cycle.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ScrubCursor {
    /// The graph the walk is inside, or `None` at a cycle boundary.
    pub(crate) graph: Option<String>,
    /// The last node id already opened in `graph`; the pass resumes after it.
    pub(crate) after: Option<String>,
    /// Completed full walks of this file.
    pub(crate) cycles: u64,
}

/// How much one pass may do.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ScrubBudget {
    /// Node payloads one pass may open.
    pub(crate) rows: usize,
}

/// What one pass did.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct ScrubPass {
    /// Node payloads opened by this pass.
    pub(crate) scanned: u64,
    /// Every unreadable row the pass reached, in walk order.
    pub(crate) findings: Vec<NodeUnreadable>,
    /// Where the next pass starts.
    pub(crate) next: ScrubCursor,
}

/// One graph's share of a pass.
struct GraphWalk {
    scanned: usize,
    last: Option<String>,
    exhausted: bool,
}

/// Run one bounded pass from `start`.
///
/// Graphs are walked in catalog (key) order. A graph that `start` names and
/// that no longer exists is simply passed over: the walk resumes at the next
/// graph in order. When the last graph is exhausted the cursor wraps to the
/// start of the next cycle.
pub(crate) fn scrub_pass(
    shard: &Shard,
    crypto: DurableCrypto<'_>,
    start: &ScrubCursor,
    budget: ScrubBudget,
) -> Result<ScrubPass, String> {
    let mut pass = ScrubPass {
        next: start.clone(),
        ..ScrubPass::default()
    };
    let resume_graph = start.graph.as_deref();
    for (graph, _) in read_graph_catalog(shard)? {
        if resume_graph.is_some_and(|resume| graph.as_str() < resume) {
            continue;
        }
        let after = start.after.as_deref().filter(|_| resume_graph == Some(graph.as_str()));
        let remaining = budget.rows.saturating_sub(pass.scanned as usize);
        let walk = scrub_graph(shard, crypto, &graph, after, remaining, &mut pass.findings)?;
        pass.scanned += walk.scanned as u64;
        if !walk.exhausted {
            pass.next = ScrubCursor {
                graph: Some(graph),
                after: walk.last,
                cycles: start.cycles,
            };
            return Ok(pass);
        }
    }
    pass.next = ScrubCursor {
        graph: None,
        after: None,
        cycles: start.cycles + 1,
    };
    Ok(pass)
}

/// Open up to `limit` node payloads of one graph after `after`.
fn scrub_graph(
    shard: &Shard,
    crypto: DurableCrypto<'_>,
    graph: &str,
    after: Option<&str>,
    limit: usize,
    findings: &mut Vec<NodeUnreadable>,
) -> Result<GraphWalk, String> {
    let handle = shard.graph(graph)?;
    let read = shard.read(&handle)?;
    let nodes = read.scoped_owner_table(NODES)?;
    let rows: Box<dyn Iterator<Item = GraphDumpNodeRow>> = match after {
        Some(last) => Box::new(nodes.scope_rows_from((graph, last))?),
        None => Box::new(nodes.scope_rows()?),
    };
    let mut walk = GraphWalk {
        scanned: 0,
        last: after.map(str::to_string),
        exhausted: true,
    };
    for row in rows {
        let (key, value) = row?;
        let (_, id) = key.value();
        if after == Some(id) {
            continue;
        }
        if walk.scanned == limit {
            walk.exhausted = false;
            break;
        }
        if let Err(cause) = crypto.open(value.value()) {
            findings.push(NodeUnreadable::new(graph, id, cause));
        }
        walk.scanned += 1;
        walk.last = Some(id.to_string());
    }
    Ok(walk)
}

/// The persisted cursor, or the start of the first cycle when none exists.
pub(crate) fn load_scrub_cursor(shard: &Shard) -> Result<ScrubCursor, String> {
    let control = shard.control_read()?;
    let table = control.open_owner_table(STORAGE_SCRUB_CURSOR)?;
    let row = table
        .get(NODE_PAYLOAD_CURSOR_KEY)
        .map_err(|error| error.to_string())?;
    match row {
        Some(bytes) => rmp_serde::from_slice(bytes.value())
            .map_err(|error| format!("storage scrub cursor is undecodable: {error}")),
        None => Ok(ScrubCursor::default()),
    }
}

/// Write the cursor inside the caller's control write. The writer thread
/// commits it as one control-only maintenance group, so it is durable at the
/// same `Durability::Immediate` as every other row.
pub(crate) fn put_scrub_cursor_in(
    write: &ShardWrite<'_>,
    cursor: &ScrubCursor,
) -> Result<(), String> {
    let encoded = rmp_serde::to_vec_named(cursor).map_err(|error| error.to_string())?;
    let mut table = write.control().open_table(STORAGE_SCRUB_CURSOR)?;
    table
        .insert(NODE_PAYLOAD_CURSOR_KEY, encoded.as_slice())
        .map_err(|error| error.to_string())?;
    Ok(())
}

#[cfg(test)]
#[path = "scrub_tests.rs"]
mod tests;
