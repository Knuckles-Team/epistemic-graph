//! Scope-group admission (RF-RULING-008).
//!
//! One physical write transaction, N scoped members plus the file's control
//! member. These cases prove the group repeats the per-scope bound rather than
//! relaxing it: no member reaches another's ledger rows, the control member and
//! the graph members reach disjoint owner tables, a failing member leaves no
//! row of the group behind, each member's version and fence advance on its own
//! scope only, and a crash before the one commit loses graph rows and Raft rows
//! together.

use super::*;
use crate::read::{read_batches, read_fences};
use crate::tables::BATCHES;
use crate::{AdmittedGroup, ScopedIntent};
use eg_storage::{ledger_scope_key, GraphShardOwner};
use eg_types::mutation_batch::VersionExpectation;
use redb::TableDefinition;

/// A graph-scoped row of the shard: the key leads with the graph name.
const NODES: TableDefinition<(&str, &str), &[u8]> = TableDefinition::new("nodes");
/// A file-wide control row: keyed by Raft group and index, no graph component.
const RAFT_LOG: TableDefinition<(u64, u64), &[u8]> = TableDefinition::new("raft_log");

/// The reserved graph name the shard's own file-wide rows are admitted under.
///
/// `OwnerLayout::GraphShard` declares `MutationDomain::GraphRows`, which may
/// never own a native scope, so every scope bound to a shard file is a graph
/// scope — the control member included. Its row class is its position in the
/// group, not its identity.
const CONTROL_GRAPH: &str = "__shard_control__";

fn shard_verifier() -> TestScopeVerifier {
    verifier("tenant-a", OwnerLayout::GraphShard)
}

fn graph_identity(graph: &str) -> MutationScopeIdentity {
    MutationScopeIdentity::graph(
        TenantId::new("tenant-a").unwrap(),
        LogicalName::new(graph).unwrap(),
        IncarnationId::new("incarnation:shard:0").unwrap(),
    )
}

/// A batch on a graph scope: `VersionExpectation::Graph`, not `Native`.
fn graph_batch(identity: MutationScopeIdentity, batch_id: &str) -> MutationBatch {
    let mut batch = batch(identity, batch_id);
    batch.version_expectation = VersionExpectation::Graph(0);
    batch.operations[0].domain = MutationDomain::GraphRows;
    batch
}

struct Shard {
    fixture: Fixture,
    control: OwnedStoreHandle<GraphShardOwner>,
    graphs: Vec<OwnedStoreHandle<GraphShardOwner>>,
}

/// One shard file with a control scope and `graphs` graph scopes bound to it.
fn shard(path: &Path, graphs: &[&str]) -> Shard {
    let fixture = Fixture::create::<GraphShardOwner>(path, "physical:test:graph-0", None);
    open_shard(fixture, graphs)
}

fn open_shard(fixture: Fixture, graphs: &[&str]) -> Shard {
    let control =
        fixture.bind::<GraphShardOwner>(&shard_verifier(), graph_identity(CONTROL_GRAPH));
    let graphs = graphs
        .iter()
        .map(|graph| fixture.bind::<GraphShardOwner>(&shard_verifier(), graph_identity(graph)))
        .collect();
    Shard {
        fixture,
        control,
        graphs,
    }
}

impl Shard {
    fn admit(&self, batches: &[&MutationBatch]) -> AdmittedGroup<'_, GraphShardOwner> {
        self.fixture
            .mutations
            .admit_group(
                ScopedIntent::maintenance(&self.control, batches[0]),
                self.graphs
                    .iter()
                    .zip(batches.iter().skip(1))
                    .map(|(owner, batch)| ScopedIntent::operation(owner, batch)),
            )
            .unwrap()
    }

    /// Write one graph row through member `index`'s own admitted owner window.
    fn write_graph_row(
        &self,
        group: &AdmittedGroup<'_, GraphShardOwner>,
        index: usize,
        batch: &MutationBatch,
        key: &str,
    ) {
        let member = group.member(index).unwrap();
        let rows = member.owner_rows(&self.graphs[index - 1], batch).unwrap();
        rows.open_table(NODES)
            .unwrap()
            .insert((self.graphs[index - 1].identity().scope().graph_name().unwrap().as_str(), key), b"row".as_slice())
            .unwrap();
        rows.finish_owner().unwrap();
    }

    fn write_raft_row(
        &self,
        group: &AdmittedGroup<'_, GraphShardOwner>,
        batch: &MutationBatch,
        index: u64,
    ) {
        let rows = group.control().owner_rows(&self.control, batch).unwrap();
        rows.open_table(RAFT_LOG)
            .unwrap()
            .insert((1u64, index), b"entry".as_slice())
            .unwrap();
        rows.finish_owner().unwrap();
    }

    fn finish(&self, group: &AdmittedGroup<'_, GraphShardOwner>, batches: &[&MutationBatch]) {
        for (index, batch) in batches.iter().enumerate() {
            let source_version = match group.begun(index).unwrap() {
                Begin::Apply { source_version } => *source_version,
                Begin::Replay(_) => panic!("unexpected replay"),
            };
            self.fixture
                .mutations
                .finish(group.member(index).unwrap(), batch, None, 2, source_version)
                .unwrap();
        }
    }
}

/// A group is N repetitions of the per-scope bound over one transaction: each
/// member writes only its own ledger rows and only the owner tables of its own
/// row class, and one commit makes all of them durable.
#[test]
fn a_scope_group_commits_every_members_rows_in_one_transaction() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("graph-0.redb");
    let shard = shard(&path, &["graph-a", "graph-b"]);
    let control_batch = graph_batch(graph_identity(CONTROL_GRAPH), "raft-1");
    let a = graph_batch(graph_identity("graph-a"), "graph-a-1");
    let b = graph_batch(graph_identity("graph-b"), "graph-b-1");
    let batches = [&control_batch, &a, &b];

    let group = shard.admit(&batches);
    assert_eq!(group.len(), 3);
    assert_eq!(group.scope(0).unwrap(), &graph_identity(CONTROL_GRAPH));
    assert_eq!(group.scope(2).unwrap(), &graph_identity("graph-b"));
    assert!(group.member(3).is_err());

    shard.write_raft_row(&group, &control_batch, 7);
    shard.write_graph_row(&group, 1, &a, "node-1");
    shard.write_graph_row(&group, 2, &b, "node-1");
    shard.finish(&group, &batches);
    shard.fixture.mutations.commit_group(group, &batches).unwrap();

    // Every member's receipt, version and fence landed, on its own scope.
    for (owner, batch_id) in [
        (&shard.control, "raft-1"),
        (&shard.graphs[0], "graph-a-1"),
        (&shard.graphs[1], "graph-b-1"),
    ] {
        let read = shard.fixture.kernel.read_scope(owner).unwrap();
        assert!(read_ledger(&read, batch_id).unwrap().is_some());
        assert_eq!(version(&read).unwrap(), 1);
        assert!(read_fences(&read).unwrap().is_some());
        // and nobody else's receipt is visible on it.
        for other in ["raft-1", "graph-a-1", "graph-b-1"] {
            assert_eq!(
                read_ledger(&read, other).unwrap().is_some(),
                other == batch_id
            );
        }
    }

    // Both row classes are durable after the one commit.
    let read = shard.fixture.kernel.read_scope(&shard.control).unwrap();
    assert!(read
        .open_owner_table(RAFT_LOG)
        .unwrap()
        .get((1u64, 7u64))
        .unwrap()
        .is_some());
    assert!(read
        .open_owner_table(NODES)
        .unwrap()
        .get(("graph-a", "node-1"))
        .unwrap()
        .is_some());
}

/// One member cannot reach another's ledger rows even though both are in the
/// same transaction, and it cannot reach the other's owner-table class either.
#[test]
fn a_group_member_cannot_reach_another_members_rows() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("graph-0.redb");
    let shard = shard(&path, &["graph-a", "graph-b"]);
    let control_batch = graph_batch(graph_identity(CONTROL_GRAPH), "raft-1");
    let a = graph_batch(graph_identity("graph-a"), "graph-a-1");
    let b = graph_batch(graph_identity("graph-b"), "graph-b-1");
    let batches = [&control_batch, &a, &b];
    let group = shard.admit(&batches);

    // Ledger: A's member is bound to A's scope key, so B's receipt key is
    // refused on read, insert and remove alike.
    let member_a = group.member(1).unwrap();
    let key_b = ledger_scope_key(&graph_identity("graph-b"));
    let mut ledger = member_a.scoped_table(BATCHES).unwrap();
    assert!(ledger.get((key_b.as_str(), "graph-b-1")).is_err());
    assert!(ledger
        .insert((key_b.as_str(), "graph-b-1"), b"forged".as_slice())
        .is_err());
    assert!(ledger.remove((key_b.as_str(), "graph-b-1")).is_err());
    drop(ledger);

    // Owner rows: a scoped member may not open a control table, and the
    // control member may not open a serving one.
    assert!(member_a
        .owner_rows(&shard.graphs[0], &a)
        .unwrap()
        .open_table(RAFT_LOG)
        .unwrap_err()
        .contains("outside its row class"));
    assert!(group
        .control()
        .owner_rows(&shard.control, &control_batch)
        .unwrap()
        .open_table(NODES)
        .unwrap_err()
        .contains("outside its row class"));

    // And a member may not admit a batch for a scope it does not serve.
    assert!(member_a.owner_rows(&shard.graphs[1], &b).is_err());

    // No member can end the shared transaction on its own.
    shard.fixture.mutations.abort_group(group).unwrap();
}

/// One transaction means one outcome: a member that fails admission discards
/// the whole group, and no other member's rows survive.
#[test]
fn a_failing_member_leaves_no_row_of_the_group() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("graph-0.redb");
    let control_batch = graph_batch(graph_identity(CONTROL_GRAPH), "raft-1");
    let a = graph_batch(graph_identity("graph-a"), "graph-a-1");
    // `graph-b`'s batch claims a version its scope does not have.
    let mut stale = graph_batch(graph_identity("graph-b"), "graph-b-1");
    stale.version_expectation = VersionExpectation::Graph(9);
    {
        let shard = shard(&path, &["graph-a", "graph-b"]);
        let error = shard
            .fixture
            .mutations
            .admit_group(
                ScopedIntent::maintenance(&shard.control, &control_batch),
                [
                    ScopedIntent::operation(&shard.graphs[0], &a),
                    ScopedIntent::operation(&shard.graphs[1], &stale),
                ],
            )
            .err()
            .unwrap();
        assert!(error.contains("STALE_VERSION"));
    }
    // Reopen: neither the failing member's rows nor the two that admitted
    // cleanly before it are there.
    let shard = open_shard(
        Fixture::open::<GraphShardOwner>(&path, "physical:test:graph-0", None),
        &["graph-a", "graph-b"],
    );
    for owner in [&shard.control, &shard.graphs[0], &shard.graphs[1]] {
        let read = shard.fixture.kernel.read_scope(owner).unwrap();
        assert!(read_batches(&read).unwrap().is_empty());
        assert_eq!(version(&read).unwrap(), 0);
        assert!(read_fences(&read).unwrap().is_none());
    }
}

/// A crash before the group's one commit loses the Raft entry and the graph row
/// together — which is the whole reason they share a transaction. Committing
/// the same work afterwards makes both durable, also together.
#[test]
fn a_crash_before_the_group_commit_loses_the_raft_row_and_the_graph_row_together() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("graph-0.redb");
    let control_batch = graph_batch(graph_identity(CONTROL_GRAPH), "raft-1");
    let a = graph_batch(graph_identity("graph-a"), "graph-a-1");
    let batches = [&control_batch, &a];
    {
        let shard = shard(&path, &["graph-a"]);
        let group = shard.admit(&batches);
        shard.write_raft_row(&group, &control_batch, 7);
        shard.write_graph_row(&group, 1, &a, "node-1");
        shard.finish(&group, &batches);
        // The physical equivalent of dying before `commit` returns.
        drop(group);
    }
    {
        let shard = open_shard(
            Fixture::open::<GraphShardOwner>(&path, "physical:test:graph-0", None),
            &["graph-a"],
        );
        let read = shard.fixture.kernel.read_scope(&shard.control).unwrap();
        assert!(read
            .open_owner_table(RAFT_LOG)
            .unwrap()
            .get((1u64, 7u64))
            .unwrap()
            .is_none());
        assert!(read
            .open_owner_table(NODES)
            .unwrap()
            .get(("graph-a", "node-1"))
            .unwrap()
            .is_none());
        assert_eq!(version(&read).unwrap(), 0);
        drop(read);

        let group = shard.admit(&batches);
        shard.write_raft_row(&group, &control_batch, 7);
        shard.write_graph_row(&group, 1, &a, "node-1");
        shard.finish(&group, &batches);
        shard.fixture.mutations.commit_group(group, &batches).unwrap();
    }
    let shard = open_shard(
        Fixture::open::<GraphShardOwner>(&path, "physical:test:graph-0", None),
        &["graph-a"],
    );
    let read = shard.fixture.kernel.read_scope(&shard.control).unwrap();
    assert!(read
        .open_owner_table(RAFT_LOG)
        .unwrap()
        .get((1u64, 7u64))
        .unwrap()
        .is_some());
    assert!(read
        .open_owner_table(NODES)
        .unwrap()
        .get(("graph-a", "node-1"))
        .unwrap()
        .is_some());
}

/// The group is minted by the kernel and by nothing else: a repeated scope, an
/// unnamed member at commit, and a lone member trying to end the transaction
/// are all refused.
#[test]
fn only_the_kernel_mints_a_group_and_only_the_group_ends_it() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("graph-0.redb");
    let shard = shard(&path, &["graph-a"]);
    let control_batch = graph_batch(graph_identity(CONTROL_GRAPH), "raft-1");
    let a = graph_batch(graph_identity("graph-a"), "graph-a-1");

    // The same scope twice would own two version, fence and receipt row sets.
    assert!(shard
        .fixture
        .mutations
        .admit_group(
            ScopedIntent::maintenance(&shard.control, &control_batch),
            [ScopedIntent::operation(&shard.control, &control_batch)],
        )
        .err()
        .unwrap()
        .contains("may not repeat a scope"));

    // A sole capability is not a group member and cannot be ended as one.
    let sole = shard
        .fixture
        .mutations_authority()
        .write_capability(&shard.control)
        .unwrap();
    assert!(!sole.is_group_member());
    assert!(sole
        .end_group_transaction(true)
        .unwrap_err()
        .contains("not a group member"));

    // Committing a group without naming every member is refused, and the group
    // is discarded rather than half-committed.
    let batches = [&control_batch, &a];
    let group = shard.admit(&batches);
    shard.write_raft_row(&group, &control_batch, 7);
    shard.write_graph_row(&group, 1, &a, "node-1");
    shard.finish(&group, &batches);
    assert!(shard
        .fixture
        .mutations
        .commit_group(group, &[&control_batch])
        .unwrap_err()
        .contains("does not name every admitted member"));
    let read = shard.fixture.kernel.read_scope(&shard.control).unwrap();
    assert!(read_batches(&read).unwrap().is_empty());
}
