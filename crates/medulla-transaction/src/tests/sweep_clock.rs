//! A multi-scope claim sweep leases each scope from the clock it reached it at.
//!
//! One `OutboxClaimBudget` spans a whole sweep so its limit and fairness
//! accounting bound the sweep, but a lease must run its full term from the
//! moment its own scope is claimed. A sweep that stamped every scope with its
//! creation time handed later scopes leases that were already partly spent,
//! and on a busy node already expired, so their acknowledgements failed as
//! stale. The clock here is injected: every figure is exact.

use super::outbox::{bind_scope, budget, emit, TOPIC};
use super::*;

const LEASE_MS: u64 = 5_000;

#[test]
fn a_restamped_sweep_leases_each_scope_for_its_full_term() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("native.redb");
    let fixture = Fixture::create::<LedgerOnlyOwner>(&path, "physical:test:ledger-only", None);
    let first = bind_scope(
        &fixture,
        "tenant-a",
        native_identity("tenant-a", "incarnation:sweep-clock:a"),
    );
    let second = bind_scope(
        &fixture,
        "tenant-b",
        native_identity("tenant-b", "incarnation:sweep-clock:b"),
    );
    for owner in [&first, &second] {
        emit(&fixture, owner, 1);
        fixture
            .mutations
            .outbox_subscribe(owner, "projection", TOPIC)
            .unwrap();
    }

    let mut sweep = budget(8, 10);
    let leased = fixture
        .mutations
        .outbox_claim(&first, "projection", &mut sweep)
        .unwrap()
        .claims;
    assert_eq!(leased[0].lease_until_ms, 10 + LEASE_MS);
    fixture
        .mutations
        .outbox_ack(&first, &leased[0], 4_000)
        .unwrap();

    // The first scope's work took the sweep to t=6_000. Re-stamped, the
    // second scope's lease runs its full term from there, and an ack at
    // t=9_000 -- after the creation-time lease would have lapsed -- lands.
    sweep.restamp(6_000);
    sweep.restamp(20);
    assert_eq!(
        sweep.now_ms(),
        6_000,
        "the sweep clock never runs backwards"
    );
    let leased = fixture
        .mutations
        .outbox_claim(&second, "projection", &mut sweep)
        .unwrap()
        .claims;
    assert_eq!(leased[0].lease_until_ms, 6_000 + LEASE_MS);
    fixture
        .mutations
        .outbox_ack(&second, &leased[0], 9_000)
        .unwrap();
    assert_eq!(
        sweep.remaining(),
        6,
        "the limit is the sweep's, not the scope's"
    );
}
