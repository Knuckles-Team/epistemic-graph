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
use crate::read::{read_batches, read_fences, read_outbox};
use crate::tables::BATCHES;
use crate::{AdmittedGroup, CurrentIntent, MaintenanceBatch, ScopedIntent};
use eg_storage::{ledger_scope_key, GraphShardOwner};
use eg_types::mutation_batch::VersionExpectation;
use redb::TableDefinition;

/// A graph-scoped row of the shard: the key leads with the graph name.
const NODES: TableDefinition<(&str, &str), &[u8]> = TableDefinition::new("nodes");
/// A file-wide control row: keyed by Raft group and index, no graph component.
const RAFT_LOG: TableDefinition<(u64, u64), &[u8]> = TableDefinition::new("raft_log");

/// The reserved graph name the shard's own file-wide rows are admitted under.
///
/// `OwnerLayout::GraphShard` declares `DurabilityDomain::GraphRows`, which may
/// never own a native scope, so every scope bound to a shard file is a graph
/// scope — the control member included. The kernel reserves this ONE name and
/// derives the row class from it, so the control/serving split is a property
/// of the identity rather than of a caller's argument order.
const CONTROL_GRAPH: &str = eg_storage::GRAPH_SHARD_CONTROL_GRAPH;

fn shard_verifier() -> TestScopeVerifier {
    verifier("tenant-a", OwnerLayout::GraphShard)
}

fn graph_of(owner: &OwnedStoreHandle<GraphShardOwner>) -> &str {
    owner.identity().scope().graph_name().unwrap().as_str()
}

fn graph_identity(graph: &str) -> MutationScopeIdentity {
    MutationScopeIdentity::graph(
        ScopeTenantId::new("tenant-a").unwrap(),
        LogicalName::new(graph).unwrap(),
        IncarnationId::new("incarnation:shard:0").unwrap(),
    )
}

/// A caller-operation batch on a graph scope: `VersionExpectation::Graph`, not
/// `Native`.
fn graph_batch(identity: MutationScopeIdentity, batch_id: &str) -> MutationBatch {
    let mut batch = batch(identity, batch_id);
    batch.version_expectation = VersionExpectation::Graph(0);
    batch.operations[0].domain = DurabilityDomain::GraphRows;
    batch
        .reseal_envelope(eg_types::contract::Digest256::from_bytes([1_u8; 32]))
        .expect("a fixture batch reseals its envelope over its final body");
    batch
}

/// The shard's CONTROL member: an owner-maintenance batch on a graph scope.
///
/// The control scope owns the file's own rows -- the Raft log, meta and 2PC
/// records -- which have no caller, so the member is admitted as maintenance and
/// its envelope must say so. `commit::begin` refuses the mismatch, which is what
/// makes the class structural rather than an admission argument.
fn shard_control_batch(identity: MutationScopeIdentity, batch_id: &str) -> MutationBatch {
    let mut batch = maintenance_batch(identity, batch_id);
    batch.version_expectation = VersionExpectation::Graph(0);
    batch.operations[0].domain = DurabilityDomain::GraphRows;
    batch
}

/// The central maintenance constructor follows the bound scope's version
/// namespace. A graph scope must carry `Graph(version)` while a native scope
/// must carry `Native(version)`; both versions are resolved while their write
/// lock is held.
#[test]
fn maintenance_batch_preserves_graph_and_native_expectations() {
    let dir = tempfile::tempdir().unwrap();

    let graph_path = dir.path().join("graph-current.redb");
    let graph_shard = shard(&graph_path, &["graph-a"]);
    let graph_maintenance =
        MaintenanceBatch::new(DurabilityDomain::GraphRows, "graph-maintenance", "graph-a");
    let (graph_write, graph_batch, graph_begun) = graph_shard
        .fixture
        .mutations
        .admit_current(&graph_shard.graphs[0], |version| {
            graph_maintenance.for_scope_version(&graph_shard.graphs[0], version)
        })
        .unwrap();
    assert!(matches!(
        graph_begun,
        Begin::Apply {
            source_version: Some(0)
        }
    ));
    assert_eq!(
        graph_batch.version_expectation,
        VersionExpectation::Graph(0)
    );
    graph_write.abort().unwrap();

    let native_path = dir.path().join("native-current.redb");
    let (native_fixture, native_owner) = ledger_fixture(
        &native_path,
        native_identity("tenant-a", "incarnation:native-current"),
    );
    let native_maintenance = MaintenanceBatch::new(
        DurabilityDomain::BlobStore,
        "native-maintenance",
        "blob-catalog",
    );
    let (native_write, native_batch, native_begun) = native_fixture
        .mutations
        .admit_current(&native_owner, |version| {
            native_maintenance.for_scope_version(&native_owner, version)
        })
        .unwrap();
    assert!(matches!(
        native_begun,
        Begin::Apply {
            source_version: Some(0)
        }
    ));
    assert_eq!(
        native_batch.version_expectation,
        VersionExpectation::Native(0)
    );
    native_write.abort().unwrap();
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
    let control = fixture.bind::<GraphShardOwner>(&shard_verifier(), graph_identity(CONTROL_GRAPH));
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
                ScopedIntent::new(&self.control, batches[0]),
                self.graphs
                    .iter()
                    .zip(batches.iter().skip(1))
                    .map(|(owner, batch)| ScopedIntent::new(owner, batch)),
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
        rows.open_scoped_table(NODES)
            .unwrap()
            .insert((graph_of(&self.graphs[index - 1]), key), b"row".as_slice())
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

    /// Finish every member that applied. A replayed member is already terminal
    /// and must not be finished — its receipt was durable before this
    /// transaction opened.
    fn finish(&self, group: &AdmittedGroup<'_, GraphShardOwner>, batches: &[&MutationBatch]) {
        for (index, batch) in batches.iter().enumerate() {
            let source_version = match group.begun(index).unwrap() {
                Begin::Apply { source_version } => *source_version,
                Begin::Replay(_) => continue,
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
    let control_batch = shard_control_batch(graph_identity(CONTROL_GRAPH), "raft-1");
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
    shard
        .fixture
        .mutations
        .commit_group(group, &batches)
        .unwrap();

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

    // Both row classes are durable after the one commit, each reachable only
    // from the scope that owns it.
    let control_read = shard.fixture.kernel.read_scope(&shard.control).unwrap();
    assert!(control_read
        .open_owner_table(RAFT_LOG)
        .unwrap()
        .get((1u64, 7u64))
        .unwrap()
        .is_some());
    // The control scope cannot read a member's prefixed rows at all.
    assert!(control_read
        .open_owner_table(NODES)
        .unwrap_err()
        .contains("scoped accessor"));
    assert!(control_read
        .scoped_owner_table(NODES)
        .err()
        .unwrap()
        .contains("owns no scope-prefixed rows"));
    drop(control_read);

    for (index, graph) in ["graph-a", "graph-b"].into_iter().enumerate() {
        let read = shard
            .fixture
            .kernel
            .read_scope(&shard.graphs[index])
            .unwrap();
        let nodes = read.scoped_owner_table(NODES).unwrap();
        assert!(nodes.get((graph, "node-1")).unwrap().is_some());
        // and it cannot address the other graph's row in the same table.
        let other = ["graph-a", "graph-b"][1 - index];
        assert!(nodes.get((other, "node-1")).is_err());
        // nor the file's own rows.
        assert!(read.open_owner_table(RAFT_LOG).is_err());
    }
}

/// One member cannot reach another's ledger rows even though both are in the
/// same transaction, and it cannot reach the other's owner-table class either.
#[test]
fn a_group_member_cannot_reach_another_members_rows() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("graph-0.redb");
    let shard = shard(&path, &["graph-a", "graph-b"]);
    let control_batch = shard_control_batch(graph_identity(CONTROL_GRAPH), "raft-1");
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

    // Owner rows, class bound: a scoped member may not open a file-wide table,
    // and the control member may not open a scope-prefixed one.
    {
        let rows = member_a.owner_rows(&shard.graphs[0], &a).unwrap();
        assert!(rows
            .open_table(RAFT_LOG)
            .unwrap_err()
            .contains("only the file's control scope"));
        // Owner rows, ROW bound: A's own scoped accessor refuses B's key on
        // get, insert and remove, in the table they share.
        let mut nodes = rows.open_scoped_table(NODES).unwrap();
        assert_eq!(nodes.scope_key(), "graph-a");
        assert!(nodes.get(("graph-b", "node-1")).is_err());
        assert!(nodes.remove(("graph-b", "node-1")).is_err());
        assert!(nodes
            .insert(("graph-b", "node-1"), b"forged".as_slice())
            .unwrap_err()
            .contains("another scope's rows"));
        assert!(nodes
            .range_inclusive(("graph-a", ""), ("graph-b", "~"))
            .is_err());
        // its own key is fine.
        nodes
            .insert(("graph-a", "node-1"), b"row".as_slice())
            .unwrap();
        drop(nodes);
        rows.finish_owner().unwrap();
    }
    {
        let rows = group
            .control()
            .owner_rows(&shard.control, &control_batch)
            .unwrap();
        assert!(rows
            .open_table(NODES)
            .unwrap_err()
            .contains("scoped accessor"));
        assert!(rows
            .open_scoped_table(NODES)
            .err()
            .unwrap()
            .contains("owns no scope-prefixed rows"));
        rows.open_table(RAFT_LOG)
            .unwrap()
            .insert((1u64, 1u64), b"entry".as_slice())
            .unwrap();
        rows.finish_owner().unwrap();
    }

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
    let control_batch = shard_control_batch(graph_identity(CONTROL_GRAPH), "raft-1");
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
                ScopedIntent::new(&shard.control, &control_batch),
                [
                    ScopedIntent::new(&shard.graphs[0], &a),
                    ScopedIntent::new(&shard.graphs[1], &stale),
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
    let control_batch = shard_control_batch(graph_identity(CONTROL_GRAPH), "raft-1");
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
        assert_eq!(version(&read).unwrap(), 0);
        drop(read);
        let graph_read = shard.fixture.kernel.read_scope(&shard.graphs[0]).unwrap();
        assert!(graph_read
            .scoped_owner_table(NODES)
            .unwrap()
            .get(("graph-a", "node-1"))
            .unwrap()
            .is_none());
        drop(graph_read);

        let group = shard.admit(&batches);
        shard.write_raft_row(&group, &control_batch, 7);
        shard.write_graph_row(&group, 1, &a, "node-1");
        shard.finish(&group, &batches);
        shard
            .fixture
            .mutations
            .commit_group(group, &batches)
            .unwrap();
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
    let graph_read = shard.fixture.kernel.read_scope(&shard.graphs[0]).unwrap();
    assert!(graph_read
        .scoped_owner_table(NODES)
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
    let control_batch = shard_control_batch(graph_identity(CONTROL_GRAPH), "raft-1");
    let a = graph_batch(graph_identity("graph-a"), "graph-a-1");

    // The same scope twice would own two version, fence and receipt row sets.
    assert!(shard
        .fixture
        .mutations
        .admit_group(
            ScopedIntent::new(&shard.control, &control_batch),
            [
                ScopedIntent::new(&shard.graphs[0], &a),
                ScopedIntent::new(&shard.graphs[0], &a),
            ],
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

/// One member retrying a batch whose receipt is already durable does not cost
/// the other members their work: the replayed member is terminal, writes
/// nothing, and the group still commits. This is the coalescer's ordinary case
/// — one retry inside a drained burst of N.
#[test]
fn a_replayed_member_is_terminal_and_the_group_still_commits() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("graph-0.redb");
    let shard = shard(&path, &["graph-a", "graph-b"]);
    let control_batch = shard_control_batch(graph_identity(CONTROL_GRAPH), "raft-1");
    let a = graph_batch(graph_identity("graph-a"), "graph-a-1");
    let b = graph_batch(graph_identity("graph-b"), "graph-b-1");

    // First group: A and the control member commit; B stays untouched.
    let first = [&control_batch, &a];
    let group = shard
        .fixture
        .mutations
        .admit_group(
            ScopedIntent::new(&shard.control, &control_batch),
            [ScopedIntent::new(&shard.graphs[0], &a)],
        )
        .unwrap();
    shard.write_raft_row(&group, &control_batch, 7);
    shard.write_graph_row(&group, 1, &a, "node-1");
    shard.finish(&group, &first);
    shard.fixture.mutations.commit_group(group, &first).unwrap();

    // Second group: A's batch is a retry, B's is new, the control member moves
    // on. Admission resolves A to its durable receipt.
    let mut control_2 = shard_control_batch(graph_identity(CONTROL_GRAPH), "raft-2");
    // The control scope advanced once in the first group.
    control_2.version_expectation = VersionExpectation::Graph(1);
    // A's retry is a FRESH attempt over A's unchanged stable identity, which is
    // what a producer compiles on a retry; re-submitting the byte-identical
    // batch value would be a duplicated ATTEMPT and is refused by name.
    let a_retry = retry_of(&a);
    let batches = [&control_2, &a_retry, &b];
    let group = shard.admit(&batches);
    assert!(matches!(group.begun(1).unwrap(), Begin::Replay(record)
        if record.batch.batch_id == "graph-a-1"));
    assert!(matches!(group.begun(2).unwrap(), Begin::Apply { .. }));

    // The replayed member can write nothing, by construction.
    assert!(group
        .member(1)
        .unwrap()
        .owner_rows(&shard.graphs[0], &a_retry)
        .is_err());
    assert!(shard
        .fixture
        .mutations
        .finish(group.member(1).unwrap(), &a_retry, None, 2, Some(0))
        .is_err());

    shard.write_raft_row(&group, &control_2, 8);
    shard.write_graph_row(&group, 2, &b, "node-1");
    shard.finish(&group, &batches);
    shard
        .fixture
        .mutations
        .commit_group(group, &batches)
        .unwrap();

    // The two applying members advanced; the replayed one did not.
    let control_read = shard.fixture.kernel.read_scope(&shard.control).unwrap();
    assert_eq!(version(&control_read).unwrap(), 2);
    assert!(read_ledger(&control_read, "raft-2").unwrap().is_some());
    drop(control_read);
    let read_a = shard.fixture.kernel.read_scope(&shard.graphs[0]).unwrap();
    assert_eq!(version(&read_a).unwrap(), 1);
    assert_eq!(read_batches(&read_a).unwrap().len(), 1);
    drop(read_a);
    let read_b = shard.fixture.kernel.read_scope(&shard.graphs[1]).unwrap();
    assert_eq!(version(&read_b).unwrap(), 1);
    assert!(read_b
        .scoped_owner_table(NODES)
        .unwrap()
        .get(("graph-b", "node-1"))
        .unwrap()
        .is_some());

    let consumed = match shard.fixture.mutations.admit(&shard.graphs[0], &a_retry) {
        Err(error) => error,
        Ok((write, _)) => {
            write
                .abort()
                .expect("unexpected successful replay admission aborts");
            panic!("a committed mixed-group replay nonce cannot be reused");
        }
    };
    assert!(consumed.contains("REPLAY_NONCE_CONSUMED"), "{consumed}");
}

/// A caller retry keeps its original OCC observation so the kernel can resolve
/// replay/nonce identity before checking freshness. The control member still
/// builds at the current in-lock version, and both decisions share one group.
#[test]
fn current_control_group_resolves_stale_replay_before_occ() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("graph-0.redb");
    let shard = shard(&path, &["graph-a"]);
    let control = shard_control_batch(graph_identity(CONTROL_GRAPH), "raft-1");
    let original = graph_batch(graph_identity("graph-a"), "graph-a-1");
    let first = [&control, &original];
    let group = shard.admit(&first);
    shard.write_raft_row(&group, &control, 1);
    shard.write_graph_row(&group, 1, &original, "node-1");
    shard.finish(&group, &first);
    shard.fixture.mutations.commit_group(group, &first).unwrap();

    // The retry intentionally retains Graph(0), the observation from its
    // original attempt. A current-control group must reach Begin::Replay
    // before treating that stale expectation as an error.
    let retry = retry_of(&original);
    let (group, batches) = shard
        .fixture
        .mutations
        .admit_group_current_control(
            CurrentIntent::maintenance(&shard.control, |version| {
                MaintenanceBatch::new(DurabilityDomain::GraphRows, "replay-control", "control")
                    .for_scope_version(&shard.control, version)
            }),
            [ScopedIntent::new(&shard.graphs[0], &retry)],
        )
        .unwrap();
    assert!(matches!(group.begun(1).unwrap(), Begin::Replay(record)
        if record.batch.batch_id == original.batch_id));
    shard.write_raft_row(&group, &batches[0], 2);
    let refs: Vec<&MutationBatch> = batches.iter().collect();
    shard.finish(&group, &refs);
    shard.fixture.mutations.commit_group(group, &refs).unwrap();

    let consumed = match shard.fixture.mutations.admit(&shard.graphs[0], &retry) {
        Err(error) => error,
        Ok((write, _)) => {
            write
                .abort()
                .expect("unexpected successful replay admission aborts");
            panic!("a committed current-control replay nonce cannot be reused");
        }
    };
    assert!(consumed.contains("REPLAY_NONCE_CONSUMED"), "{consumed}");
}

/// A group containing only already-terminal members still commits the fresh
/// operation retry nonce. No owner rows or versions change, but replay
/// finalization must be durable rather than an abort-only probe.
#[test]
fn an_all_replayed_group_consumes_the_fresh_operation_nonce() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("graph-0.redb");
    let shard = shard(&path, &["graph-a"]);
    let control = shard_control_batch(graph_identity(CONTROL_GRAPH), "raft-1");
    let original = graph_batch(graph_identity("graph-a"), "graph-a-1");
    let first = [&control, &original];
    let group = shard.admit(&first);
    shard.write_raft_row(&group, &control, 1);
    shard.write_graph_row(&group, 1, &original, "node-1");
    shard.finish(&group, &first);
    shard.fixture.mutations.commit_group(group, &first).unwrap();

    let retry = retry_of(&original);
    let replay = [&control, &retry];
    let group = shard.admit(&replay);
    assert!(matches!(group.begun(0).unwrap(), Begin::Replay(_)));
    assert!(matches!(group.begun(1).unwrap(), Begin::Replay(_)));
    shard.finish(&group, &replay);
    shard
        .fixture
        .mutations
        .commit_group(group, &replay)
        .unwrap();

    let consumed = match shard.fixture.mutations.admit(&shard.graphs[0], &retry) {
        Err(error) => error,
        Ok((write, _)) => {
            write
                .abort()
                .expect("unexpected successful replay admission aborts");
            panic!("an all-replayed group's retry nonce cannot be reused");
        }
    };
    assert!(consumed.contains("REPLAY_NONCE_CONSUMED"), "{consumed}");
    let read = shard.fixture.kernel.read_scope(&shard.graphs[0]).unwrap();
    assert_eq!(version(&read).unwrap(), 1);
    assert_eq!(read_batches(&read).unwrap().len(), 1);
}

/// The control scope is the kernel's, not a caller's argument position: a
/// tenant graph passed first is refused, the reserved name is refused as a
/// scoped member, and no other layout may bind it at all.
#[test]
fn the_control_member_must_be_the_reserved_control_scope() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("graph-0.redb");
    let shard = shard(&path, &["graph-a"]);
    let a = graph_batch(graph_identity("graph-a"), "graph-a-1");
    let control_batch = shard_control_batch(graph_identity(CONTROL_GRAPH), "raft-1");

    // A tenant graph cannot be the control member.
    assert!(shard
        .fixture
        .mutations
        .admit_group(
            ScopedIntent::new(&shard.graphs[0], &a),
            [ScopedIntent::new(&shard.control, &control_batch)],
        )
        .err()
        .unwrap()
        .contains("reserved control scope"));

    // And the control scope cannot also ride as a scoped member.
    assert!(shard
        .fixture
        .mutations
        .admit_group(
            ScopedIntent::new(&shard.control, &control_batch),
            [
                ScopedIntent::new(&shard.graphs[0], &a),
                ScopedIntent::new(&shard.control, &control_batch),
            ],
        )
        .err()
        .unwrap()
        .contains("may not also be a scoped member"));

    // Outside a group the split still holds: a plain admit on a tenant graph
    // cannot reach the file-wide rows, and cannot open a prefixed table raw.
    let (write, _) = shard.fixture.mutations.admit(&shard.graphs[0], &a).unwrap();
    let rows = write.owner_rows(&shard.graphs[0], &a).unwrap();
    assert!(rows
        .open_table(RAFT_LOG)
        .unwrap_err()
        .contains("only the file's control scope"));
    assert!(rows
        .open_table(NODES)
        .unwrap_err()
        .contains("scoped accessor"));
    rows.open_scoped_table(NODES)
        .unwrap()
        .insert(("graph-a", "node-1"), b"row".as_slice())
        .unwrap();
    rows.finish_owner().unwrap();
    write.abort().unwrap();

    // And no other layout may bind the reserved name.
    let ledger_path = dir.path().join("ledger.redb");
    let ledger = Fixture::create::<LedgerOnlyOwner>(&ledger_path, "physical:test:ledger", None);
    assert!(ledger
        .kernel
        .authenticate_scope::<LedgerOnlyOwner>(
            &verifier("tenant-a", OwnerLayout::LedgerOnly),
            graph_identity(CONTROL_GRAPH),
            PRINCIPAL.to_string(),
            b"verified",
        )
        .err()
        .unwrap()
        .contains("does not reserve a file-wide control scope"));
}

// ── admit_group_current: the in-lock version, over N scopes ─────────────────

/// Build one graph-scoped batch that expects exactly the version it is handed.
fn current_graph_batch(graph: &str, batch_id: &str, version: u64) -> MutationBatch {
    let mut batch = batch(graph_identity(graph), batch_id);
    batch.version_expectation = VersionExpectation::Graph(version);
    batch.operations[0].domain = DurabilityDomain::GraphRows;
    batch
        .reseal_envelope(eg_types::contract::Digest256::from_bytes([1_u8; 32]))
        .expect("a current graph fixture batch reseals its final body");
    batch
}

/// A current graph batch with one stable notification so replay preservation
/// proves that no duplicate outbox intent is emitted.
fn current_graph_batch_with_stable_outbox(
    graph: &str,
    batch_id: &str,
    version: u64,
) -> MutationBatch {
    let mut batch = current_graph_batch(graph, batch_id, version);
    batch.outbox.push(eg_types::MutationOutboxIntent {
        topic: "scope-group.replay".to_string(),
        key: "graph-a-stable".to_string(),
        payload: b"stable-intent".to_vec(),
        headers: Default::default(),
    });
    batch
        .reseal_envelope(eg_types::contract::Digest256::from_bytes([1_u8; 32]))
        .expect("a current graph fixture batch reseals its outbox content");
    batch
}

impl Shard {
    /// Admit a drain: control member plus every graph, each batch built from
    /// the version resolved inside the group's own write transaction.
    fn admit_current(
        &self,
        suffix: &str,
    ) -> Result<(AdmittedGroup<'_, GraphShardOwner>, Vec<MutationBatch>), String> {
        let control_id = format!("raft-{suffix}");
        self.fixture.mutations.admit_group_current(
            CurrentIntent::maintenance(&self.control, move |version| {
                Ok(current_graph_batch(CONTROL_GRAPH, &control_id, version))
            }),
            self.graphs.iter().map(|owner| {
                let graph = graph_of(owner).to_string();
                let batch_id = format!("{graph}-{suffix}");
                CurrentIntent::operation(owner, move |version| {
                    Ok(current_graph_batch(&graph, &batch_id, version))
                })
            }),
        )
    }

    fn finish_and_commit_current(
        &self,
        group: AdmittedGroup<'_, GraphShardOwner>,
        batches: &[MutationBatch],
    ) {
        let refs: Vec<&MutationBatch> = batches.iter().collect();
        self.write_raft_row(&group, &batches[0], 1);
        for index in 1..batches.len() {
            self.write_graph_row(&group, index, &batches[index], "node-1");
        }
        self.finish(&group, &refs);
        self.fixture.mutations.commit_group(group, &refs).unwrap();
    }
}

/// The version every member's batch is built from is resolved INSIDE the one
/// write transaction, so a drain never carries an OCC claim it did not make.
#[test]
fn admit_group_current_builds_every_member_from_its_in_lock_version() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("graph-0.redb");
    let shard = shard(&path, &["graph-a", "graph-b"]);

    let (group, batches) = shard.admit_current("1").unwrap();
    assert_eq!(group.len(), 3);
    assert_eq!(batches.len(), 3);
    // Each member's expectation is its OWN scope's version, at version 0.
    for batch in &batches {
        assert_eq!(batch.version_expectation, VersionExpectation::Graph(0));
    }
    shard.finish_and_commit_current(group, &batches);

    for (owner, batch_id) in [
        (&shard.control, "raft-1"),
        (&shard.graphs[0], "graph-a-1"),
        (&shard.graphs[1], "graph-b-1"),
    ] {
        let read = shard.fixture.kernel.read_scope(owner).unwrap();
        assert!(read_ledger(&read, batch_id).unwrap().is_some());
        assert_eq!(version(&read).unwrap(), 1);
    }
}

/// A second drain immediately after the first observes the version the first
/// produced, rather than failing `STALE_VERSION` on a version read before the
/// write lock — which is the whole defect `admit_group` left open for a group.
#[test]
fn a_second_drain_sees_the_version_the_first_committed() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("graph-0.redb");
    let shard = shard(&path, &["graph-a", "graph-b"]);

    let (group, first) = shard.admit_current("1").unwrap();
    shard.finish_and_commit_current(group, &first);

    let (group, second) = shard.admit_current("2").unwrap();
    for batch in &second {
        assert_eq!(batch.version_expectation, VersionExpectation::Graph(1));
    }
    shard.finish_and_commit_current(group, &second);

    for owner in [&shard.control, &shard.graphs[0], &shard.graphs[1]] {
        let read = shard.fixture.kernel.read_scope(owner).unwrap();
        assert_eq!(version(&read).unwrap(), 2);
    }
}

/// The SAME two drains through `admit_group` — which takes already-built
/// batches — fail closed, because the second drain's expectation was read
/// before the write lock. This is the planted known-bad input that proves the
/// new entry point removes a real failure rather than a hypothetical one.
#[test]
fn admit_group_still_fails_closed_on_a_stale_prebuilt_expectation() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("graph-0.redb");
    let shard = shard(&path, &["graph-a"]);

    let (group, first) = shard.admit_current("1").unwrap();
    shard.finish_and_commit_current(group, &first);

    // A caller that observed version 0 before the first drain committed.
    let stale_control = current_graph_batch(CONTROL_GRAPH, "raft-stale", 0);
    let stale_graph = current_graph_batch("graph-a", "graph-a-stale", 0);
    let error = shard
        .fixture
        .mutations
        .admit_group(
            ScopedIntent::new(&shard.control, &stale_control),
            [ScopedIntent::new(&shard.graphs[0], &stale_graph)],
        )
        .err()
        .unwrap();
    assert!(error.contains("STALE_VERSION"), "{error}");
}

/// A builder that returns an expectation other than the one its scope owes is
/// refused, so the claim `admit_group_current` exists to remove cannot be
/// smuggled back in through the closure.
#[test]
fn admit_group_current_refuses_a_builder_that_expects_another_version() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("graph-0.redb");
    let shard = shard(&path, &["graph-a"]);

    let error = shard
        .fixture
        .mutations
        .admit_group_current(
            CurrentIntent::maintenance(&shard.control, |version| {
                Ok(current_graph_batch(CONTROL_GRAPH, "raft-1", version))
            }),
            [CurrentIntent::operation(&shard.graphs[0], |_version| {
                Ok(current_graph_batch("graph-a", "graph-a-1", 41))
            })],
        )
        .err()
        .unwrap();
    assert!(
        error.contains("current-version admission requires Graph(0)"),
        "{error}"
    );
    // Nothing of the discarded group is durable, control member included.
    let read = shard.fixture.kernel.read_scope(&shard.control).unwrap();
    assert!(read_ledger(&read, "raft-1").unwrap().is_none());
    assert_eq!(version(&read).unwrap(), 0);
}

/// A builder that fails discards the whole group and reports ITS error, not a
/// teardown error.
#[test]
fn a_failing_builder_discards_the_whole_group() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("graph-0.redb");
    let shard = shard(&path, &["graph-a"]);

    let error = shard
        .fixture
        .mutations
        .admit_group_current(
            CurrentIntent::maintenance(&shard.control, |version| {
                Ok(current_graph_batch(CONTROL_GRAPH, "raft-1", version))
            }),
            [CurrentIntent::operation(&shard.graphs[0], |_version| {
                Err("the drain had nothing to build".to_string())
            })],
        )
        .err()
        .unwrap();
    assert_eq!(error, "the drain had nothing to build");
    let read = shard.fixture.kernel.read_scope(&shard.control).unwrap();
    assert!(read_ledger(&read, "raft-1").unwrap().is_none());
}

/// A current group can mix a stable replay with fresh members. The replayed
/// member is terminal and leaves its receipt, outbox, and version untouched;
/// the other members apply and commit together.
#[test]
fn a_retried_member_of_a_current_group_replays_while_fresh_members_commit() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("graph-0.redb");
    let shard = shard(&path, &["graph-a", "graph-b"]);

    // First group: control and graph-a commit, while graph-b remains empty.
    let (group, first) = shard
        .fixture
        .mutations
        .admit_group_current(
            CurrentIntent::maintenance(&shard.control, |version| {
                Ok(current_graph_batch(CONTROL_GRAPH, "raft-1", version))
            }),
            [CurrentIntent::operation(&shard.graphs[0], |version| {
                Ok(current_graph_batch_with_stable_outbox("graph-a", "graph-a-1", version))
            })],
        )
        .unwrap();
    assert!(matches!(group.begun(0).unwrap(), Begin::Apply { .. }));
    assert!(matches!(group.begun(1).unwrap(), Begin::Apply { .. }));
    let first_refs: Vec<&MutationBatch> = first.iter().collect();
    shard.write_raft_row(&group, &first[0], 7);
    shard.write_graph_row(&group, 1, &first[1], "node-1");
    shard.finish(&group, &first_refs);
    shard
        .fixture
        .mutations
        .commit_group(group, &first_refs)
        .unwrap();

    // Capture graph-a's durable state before the retry-containing group.
    let before_a = {
        let read = shard.fixture.kernel.read_scope(&shard.graphs[0]).unwrap();
        let receipt = read_ledger(&read, "graph-a-1").unwrap().unwrap();
        let outbox = read_outbox(&read, "graph-a-1").unwrap();
        assert_eq!(outbox.len(), 1);
        assert_eq!(outbox[0].intent.topic, "scope-group.replay");
        assert_eq!(outbox[0].intent.key, "graph-a-stable");
        (
            eg_storage::encode_bounded(&receipt, "replay receipt").unwrap(),
            outbox,
            version(&read).unwrap(),
        )
    };

    // The second group rebuilds graph-a's stable operation at its current
    // version. Its identity is replayable; graph-b and the control member are
    // fresh and still use the current-group builder.
    let (group, batches) = shard
        .fixture
        .mutations
        .admit_group_current(
            CurrentIntent::maintenance(&shard.control, |version| {
                Ok(current_graph_batch(CONTROL_GRAPH, "raft-2", version))
            }),
            [
                CurrentIntent::operation(&shard.graphs[0], |version| {
                    Ok(current_graph_batch_with_stable_outbox("graph-a", "graph-a-1", version))
                }),
                CurrentIntent::operation(&shard.graphs[1], |version| {
                    Ok(current_graph_batch("graph-b", "graph-b-2", version))
                }),
            ],
        )
        .unwrap();
    assert!(matches!(group.begun(0).unwrap(), Begin::Apply { .. }));
    assert!(matches!(
        group.begun(1).unwrap(),
        Begin::Replay(record) if record.batch.batch_id == "graph-a-1"
    ));
    assert!(matches!(group.begun(2).unwrap(), Begin::Apply { .. }));
    assert!(group
        .member(1)
        .unwrap()
        .owner_rows(&shard.graphs[0], &batches[1])
        .is_err());

    let refs: Vec<&MutationBatch> = batches.iter().collect();
    shard.write_raft_row(&group, &batches[0], 8);
    shard.write_graph_row(&group, 2, &batches[2], "node-1");
    shard.finish(&group, &refs);
    shard.fixture.mutations.commit_group(group, &refs).unwrap();

    let control_read = shard.fixture.kernel.read_scope(&shard.control).unwrap();
    assert_eq!(version(&control_read).unwrap(), 2);
    assert!(read_ledger(&control_read, "raft-2").unwrap().is_some());
    drop(control_read);

    let read_a = shard.fixture.kernel.read_scope(&shard.graphs[0]).unwrap();
    let receipt_a = read_ledger(&read_a, "graph-a-1").unwrap().unwrap();
    assert_eq!(
        eg_storage::encode_bounded(&receipt_a, "replay receipt").unwrap(),
        before_a.0
    );
    assert_eq!(read_outbox(&read_a, "graph-a-1").unwrap(), before_a.1);
    assert_eq!(version(&read_a).unwrap(), before_a.2);
    assert!(read_ledger(&read_a, "graph-a-2").unwrap().is_none());
    drop(read_a);

    let read_b = shard.fixture.kernel.read_scope(&shard.graphs[1]).unwrap();
    assert_eq!(version(&read_b).unwrap(), 1);
    assert!(read_ledger(&read_b, "graph-b-2").unwrap().is_some());
    assert!(read_b
        .scoped_owner_table(NODES)
        .unwrap()
        .get(("graph-b", "node-1"))
        .unwrap()
        .is_some());
}
