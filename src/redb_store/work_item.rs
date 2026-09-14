//! Native WorkItem transitions are split by operation so each module has one
//! bounded responsibility while the parent module preserves the historical paths.

//! Native WorkItem row transitions: submit, claim, renew, compare-and-set,
//! commit, cancel and defer.
//!
//! These are the `Method` arms that write a WorkItem's node, its dependency
//! edges and its downstream index inside an already-admitted mutation. All of
//! their tables are scope-prefixed, so each transition works through one graph
//! member's owner-row write.

use eg_storage::ScopedOwnerTableMut;

use super::*;

/// Apply one native WorkItem transition while the MutationBatch write
/// transaction is held. The returned payload is persisted as the batch result in
/// that same transaction, so a retry observes the exact original claim/commit
/// outcome rather than running selection twice.
/// The commit-scoped inputs both WorkItem row appliers need alongside their
/// scoped tables: the durable crypto handle, the authoritative commit timestamp,
/// and the outbox id. Grouped so each applier keeps a readable arity
/// (clippy::too_many_arguments) without disturbing the borrowed table params,
/// whose scoped-table lifetimes are load-bearing.
pub(crate) struct WorkItemCommitScope<'a> {
    pub(crate) crypto: DurableCrypto<'a>,
    pub(crate) authoritative_now_ms: u64,
    pub(crate) outbox_id: &'a str,
}

/// Refuse a per-graph scan asked about a graph other than the one its table is
/// bound to.
///
/// `scope_rows` takes its bound from the capability rather than from a key, so
/// the graph argument and the table's own scope can no longer disagree
/// silently: before the cutover the scan's `range((graph, "")..)` start made
/// the argument the bound, and a mismatch would now count -- or select from --
/// another graph's WorkItems while reporting them as this graph's.
fn permit_scoped_scan(scope_key: &str, graph: &str) -> Result<(), String> {
    if scope_key != graph {
        return Err(format!(
            "WorkItem scan for '{graph}' was given a table bound to '{scope_key}'"
        ));
    }
    Ok(())
}

mod cancel;
mod claim;
mod commit;
mod dispatch;
mod lease;
mod submit;

pub(crate) use cancel::*;
pub(crate) use claim::*;
pub(crate) use commit::*;
pub(crate) use dispatch::*;
pub(crate) use lease::*;
pub(crate) use submit::*;
