//! The user-managed index lifecycle (EH-352): one vocabulary every managed
//! index family reports in, whichever store maintains it.
//!
//! A managed index is one a user created and can drop — a maintained ANN index
//! over a SQL table column (`eg-query`'s maintained ANN authority) or an
//! edge-native vector/text index over a graph's edges (`eg-query`'s edge index,
//! registered here as a server index). Its lifecycle is the state machine
//! `requested -> backfilling -> active | blocked`:
//!
//! * `requested`: registered, no build has started yet;
//! * `backfilling`: the first generation is being built; queries take the
//!   family's bounded exact path meanwhile and never see a partial index;
//! * `active`: a generation is live (it may lag the source; `lag` says by how
//!   much, and the family scores every change since its build exactly);
//! * `blocked`: no generation serves and the last attempt failed, with a
//!   bounded, typed diagnostic.
//!
//! [`IndexManager`] answers the status of every managed index of a graph and
//! drops one by name, fenced, so a build in flight never activates after its
//! drop.

use serde::{Deserialize, Serialize};

use super::IndexManager;

/// Longest diagnostic detail a status row carries, in bytes.
pub const MAX_BLOCK_DETAIL_BYTES: usize = 256;

/// Where a managed index is in its lifecycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ManagedIndexState {
    Requested,
    Backfilling,
    Active,
    Blocked,
}

impl ManagedIndexState {
    /// The one lifecycle rule every family applies: a live generation is
    /// active; without one, a recorded failure blocks, a running build
    /// backfills, and otherwise the index is only requested.
    pub fn of(serving: bool, building: bool, block: Option<&IndexBlock>) -> Self {
        match (serving, block.is_some(), building) {
            (true, _, _) => Self::Active,
            (false, true, _) => Self::Blocked,
            (false, false, true) => Self::Backfilling,
            (false, false, false) => Self::Requested,
        }
    }

    pub fn as_str(self) -> &'static str {
        const NAMES: [&str; 4] = ["requested", "backfilling", "active", "blocked"];
        NAMES[self as usize]
    }
}

/// What a managed index answers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ManagedIndexFamily {
    /// Nearest-neighbour search over a vector property.
    Vector,
    /// BM25 search over a text property.
    Text,
}

impl ManagedIndexFamily {
    pub fn as_str(self) -> &'static str {
        const NAMES: [&str; 2] = ["vector", "text"];
        NAMES[self as usize]
    }
}

/// The typed target of a managed index. Graph edges are a native target: an
/// edge is indexed and returned as an edge, never reified as a node (EH-351).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ManagedIndexTarget {
    /// One column of one SQL table.
    TableColumn { table: String, column: String },
    /// One property of every edge of the graph.
    GraphEdges { property: String },
}

impl ManagedIndexTarget {
    /// The target kind's stable name.
    pub fn kind(&self) -> &'static str {
        match self {
            Self::TableColumn { .. } => "table_column",
            Self::GraphEdges { .. } => "graph_edges",
        }
    }

    /// The relation the index covers: the table, or `edges`.
    pub fn relation(&self) -> &str {
        match self {
            Self::TableColumn { table, .. } => table,
            Self::GraphEdges { .. } => "edges",
        }
    }

    /// The column or property the index covers.
    pub fn attribute(&self) -> &str {
        match self {
            Self::TableColumn { column, .. } => column,
            Self::GraphEdges { property } => property,
        }
    }
}

/// Why a managed index is blocked.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IndexBlockReason {
    /// The target cannot be indexed (missing, or not of the family's type).
    NotIndexable,
    /// The source exceeds the build's resource bound.
    BuildBound,
    /// A build failed for another reason.
    BuildFailed,
    /// The persisted generation could not be restored.
    RestoreFailed,
    /// A live generation could not be persisted.
    PersistFailed,
}

impl IndexBlockReason {
    pub fn as_str(self) -> &'static str {
        const NAMES: [&str; 5] = [
            "not_indexable",
            "build_bound",
            "build_failed",
            "restore_failed",
            "persist_failed",
        ];
        NAMES[self as usize]
    }
}

/// A typed, bounded diagnostic.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IndexBlock {
    pub reason: IndexBlockReason,
    /// At most [`MAX_BLOCK_DETAIL_BYTES`], cut on a character boundary.
    pub detail: String,
}

impl IndexBlock {
    pub fn new(reason: IndexBlockReason, detail: &str) -> Self {
        let mut end = detail.len().min(MAX_BLOCK_DETAIL_BYTES);
        while !detail.is_char_boundary(end) {
            end -= 1;
        }
        Self {
            reason,
            detail: detail[..end].to_string(),
        }
    }
}

/// One managed index's status: one row of the SQL status relation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManagedIndexStatus {
    pub name: String,
    pub family: ManagedIndexFamily,
    pub target: ManagedIndexTarget,
    pub state: ManagedIndexState,
    /// The live generation.
    pub generation: Option<u64>,
    /// The source version (SQL epoch / graph version) the live generation was
    /// built from.
    pub built_version: Option<u64>,
    /// The source version of the target's last change.
    pub change_version: u64,
    /// `change_version - built_version`; the whole change version when nothing
    /// serves.
    pub lag: u64,
    /// Entries the live generation indexes; `None` when nothing serves or the
    /// reader may not learn the count (row-level security hides rows from it).
    pub indexed: Option<usize>,
    pub block: Option<IndexBlock>,
}

impl IndexManager {
    /// The status of every managed index registered on this graph, by name.
    pub fn managed_statuses(&self) -> Vec<ManagedIndexStatus> {
        let mut statuses: Vec<ManagedIndexStatus> = self
            .server_indexes
            .read()
            .iter()
            .filter_map(|index| index.managed_status())
            .collect();
        statuses.sort_by(|a, b| a.name.cmp(&b.name));
        statuses
    }

    /// Drop every managed index named `name`: each is retired first (a build in
    /// flight never activates), then unregistered. Returns how many were
    /// dropped.
    pub fn drop_managed(&self, name: &str) -> usize {
        let mut indexes = self.server_indexes.write();
        let before = indexes.len();
        indexes.retain(|index| {
            let named = index
                .managed_status()
                .is_some_and(|status| status.name == name);
            if named {
                index.retire();
            }
            !named
        });
        before - indexes.len()
    }

    /// Run `f` against the managed server index named `name`, holding the
    /// registry's read lock; `None` when no such index is registered.
    pub fn with_managed_index<F, R>(&self, name: &str, f: F) -> Option<R>
    where
        F: FnOnce(&dyn super::SecondaryIndex) -> R,
    {
        self.server_indexes
            .read()
            .iter()
            .find(|index| {
                index
                    .managed_status()
                    .is_some_and(|status| status.name == name)
            })
            .map(|index| f(index.as_ref()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_lifecycle_rule_is_requested_backfilling_active_or_blocked() {
        let block = IndexBlock::new(IndexBlockReason::BuildFailed, "boom");
        assert_eq!(
            ManagedIndexState::of(false, false, None),
            ManagedIndexState::Requested
        );
        assert_eq!(
            ManagedIndexState::of(false, true, None),
            ManagedIndexState::Backfilling
        );
        assert_eq!(
            ManagedIndexState::of(false, true, Some(&block)),
            ManagedIndexState::Blocked
        );
        assert_eq!(
            ManagedIndexState::of(true, true, Some(&block)),
            ManagedIndexState::Active,
            "a live generation serves whatever the last attempt did"
        );
    }

    #[test]
    fn a_diagnostic_is_bounded_on_a_character_boundary() {
        let long = "é".repeat(MAX_BLOCK_DETAIL_BYTES);
        let block = IndexBlock::new(IndexBlockReason::BuildBound, &long);
        assert!(block.detail.len() <= MAX_BLOCK_DETAIL_BYTES);
        assert!(long.starts_with(&block.detail));
    }
}
