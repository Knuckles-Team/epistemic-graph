//! Regression test for `impl/raft-catchup-apply`: a follower that falls behind and
//! must REPLAY committed log entries where a `CreateGraph` is immediately followed by
//! a write into that same graph used to fail the replay with
//! `graph '<name>' missing after create`, which is FATAL to `RaftStateMachine::apply`
//! (it aborts the whole `apply()` call for the stream) — so the rejoining node's Raft
//! instance never advances past that point and the node can never finish catching up.
//!
//! `cargo test --features "raft harness" raft::harness::catchup_test` runs this.

use std::time::Duration;

use super::cluster::Cluster;
use crate::protocol::GraphType;

/// KILL a follower, commit `CreateGraph(g)` then `AddNode(g, n0)` on the surviving
/// quorum (so BOTH land as adjacent entries the killed node has never seen), RESTART
/// it, and assert it converges — proving the CreateGraph→write replay sequence
/// applies cleanly on the catch-up path.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn follower_catchup_replays_create_graph_then_write() {
    const NEW_GRAPH: &str = "raft-catchup-newgraph";

    let mut cluster = Cluster::start(3, "raft-catchup-newgraph")
        .await
        .expect("cluster starts");

    let leader = cluster
        .wait_for_leader(Duration::from_secs(15))
        .await
        .expect("initial leader elected");

    // Pick a NON-leader to kill so the remaining two (a quorum of 3) can still commit.
    let victim = cluster
        .all_ids()
        .into_iter()
        .find(|id| *id != leader)
        .expect("a non-leader member exists");

    cluster.kill(victim).await.expect("kill follower");

    // Commit CreateGraph(NEW_GRAPH) immediately followed by a write into it, on the
    // surviving quorum. `victim` has NEVER seen either entry — it must catch BOTH up
    // via replay after it restarts, back to back, exactly the failing sequence.
    cluster
        .write_req(
            leader,
            Cluster::create_graph_req(NEW_GRAPH, GraphType::Global, 1),
        )
        .await
        .expect("CreateGraph commits on the surviving quorum");
    cluster
        .write_req(
            leader,
            Cluster::add_node_req_for(NEW_GRAPH, GraphType::Global, 2),
        )
        .await
        .expect("the immediately-following write commits on the surviving quorum");

    // A couple more ordinary writes to the pre-existing default graph so the restarted
    // node has a realistic multi-entry backlog to replay, not just the two under test.
    for seq in 3..=5 {
        cluster
            .write_via(leader, seq)
            .await
            .expect("ordinary commons writes keep committing");
    }

    cluster.restart(victim).await.expect("restart the follower");

    // Poll for convergence: the restarted node must replay its backlog — including
    // the CreateGraph→write sequence — and end up with the graph AND the node.
    // Before the fix: replay hits `graph '<name>' missing after create`, which is
    // fatal to `apply()`, so the node's applied index stalls and this deadline trips.
    let deadline = Duration::from_secs(20);
    let start = std::time::Instant::now();
    let mut converged = false;
    while start.elapsed() < deadline {
        if cluster.has_node_in(victim, NEW_GRAPH, "n2").await {
            converged = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(150)).await;
    }

    assert!(
        converged,
        "follower {victim} never caught up on the CreateGraph({NEW_GRAPH})→AddNode \
         replay sequence within {deadline:?} — graph_exists={}, node_count={}, \
         applied_index={:?} (leader applied_index={:?})",
        cluster.graph_exists(victim, NEW_GRAPH).await,
        cluster.node_count_in(victim, NEW_GRAPH).await,
        cluster.applied_index(victim).await,
        cluster.applied_index(leader).await,
    );

    // The rest of the replayed backlog must ALSO be intact (no partial/poisoned
    // replay of the entries before or after the failing pair).
    assert!(
        cluster.graph_exists(victim, NEW_GRAPH).await,
        "graph must be registered on the caught-up follower"
    );
    assert_eq!(
        cluster.node_count_in(victim, NEW_GRAPH).await,
        1,
        "exactly the one replayed node must be present"
    );

    cluster.teardown().await;
}
