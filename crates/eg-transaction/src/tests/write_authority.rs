//! EH-390: store authority is proved once per write transaction and each
//! member's scope binding once per binding epoch, through the real group
//! admission path, and the check still fails closed at the boundary.

use super::scope_group::{graph_identity, shard};
use eg_storage::WriteValidationCounts;

fn delta(before: WriteValidationCounts, after: WriteValidationCounts) -> (u64, u64) {
    (
        after.store_authority - before.store_authority,
        after.scope_bindings - before.scope_bindings,
    )
}

/// A whole group drain (admit, owner rows, finish, seal, commit) runs the
/// store-authority census exactly once and reads each member's binding
/// exactly once, although `verify_scope` is called at every one of those
/// stages. Before EH-390 each call re-ran the full census.
// spec: EG-DURABLE-KERNEL-R021
#[test]
fn a_group_commit_validates_store_authority_exactly_once() {
    let dir = tempfile::tempdir().unwrap();
    let shard = shard(&dir.path().join("graph-0.redb"), &["graph-a", "graph-b"]);
    let before = shard.fixture.kernel.write_validation_counts();

    let (group, batches) = shard.admit_current("1").unwrap();
    shard.finish_and_commit_current(group, &batches);

    let after = shard.fixture.kernel.write_validation_counts();
    assert_eq!(
        delta(before, after),
        (1, 3),
        "one store-authority check, one binding read per member (control + 2)"
    );
}

/// A binding retired inside the transaction invalidates every cached proof:
/// the retired member is refused on its next use, and the others re-read.
#[test]
fn a_binding_retired_mid_transaction_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let shard = shard(&dir.path().join("graph-0.redb"), &["graph-a"]);
    let (group, _batches) = shard.admit_current("1").unwrap();
    let member = group.member(1).unwrap();
    member.verify_scope(&graph_identity("graph-a")).unwrap();
    let before = shard.fixture.kernel.write_validation_counts();

    member.retire_scope_binding().unwrap();
    let refused = member
        .verify_scope(&graph_identity("graph-a"))
        .expect_err("a retired binding must not verify from a stale proof");
    assert!(refused.contains("not bound"), "{refused}");
    group
        .control()
        .verify_scope(&graph_identity(eg_storage::GRAPH_SHARD_CONTROL_GRAPH))
        .expect("an untouched member re-proves its own binding");
    assert_eq!(
        delta(before, shard.fixture.kernel.write_validation_counts()).1,
        2
    );
    shard.fixture.mutations.abort_group(group).unwrap();
}

/// A revoked authority fails the NEXT transaction at its boundary: a binding
/// retired and committed in one transaction cannot be admitted in the next.
#[test]
fn a_revoked_binding_fails_the_next_transaction() {
    let dir = tempfile::tempdir().unwrap();
    let shard = shard(&dir.path().join("graph-0.redb"), &["graph-a"]);
    let revoke = shard
        .fixture
        .mutations_authority()
        .write_capability(&shard.graphs[0])
        .unwrap();
    revoke.retire_scope_binding().unwrap();
    revoke.commit().unwrap();

    let refused = shard
        .admit_current("after-revoke")
        .err()
        .expect("the next transaction must not admit a revoked scope");
    assert!(refused.contains("not bound"), "{refused}");
}
