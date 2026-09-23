//! A projection sweep leases each graph scope for a full term (PX10b).
//!
//! One claim budget spans the whole sweep so its limit and fairness run bound
//! the sweep. Its clock used to be the sweep's START: every graph after the
//! first was leased from a moment that had already passed, and on a busy node
//! the later graphs' leases lapsed before their acknowledgements -- the
//! projection then stalled one row into the graph and retried on expiry.
//! The clock here is injected and advances a fixed step per reading, so the
//! failure is exact rather than load-dependent.

use std::sync::atomic::{AtomicU64, Ordering};

use super::*;

/// Each reading of the injected clock is this much later than the last:
/// two readings fit in one lease term, three do not.
const STEP_MS: u64 = CLAIM_LEASE_MS * 2 / 3;

static NOW_MS: AtomicU64 = AtomicU64::new(0);

fn stepping_clock() -> u64 {
    NOW_MS.fetch_add(STEP_MS, Ordering::SeqCst)
}

#[tokio::test(flavor = "current_thread")]
async fn every_graph_of_a_sweep_is_leased_from_the_moment_it_is_claimed() {
    let _env_read_lock = crate::crypto::acquire_test_env_read_lock().await;

    let (root, root_str, backend) = shard_backend("open sweep clock backend");
    let graph_names: Vec<String> = (0..3)
        .map(|index| format!("sweep-clock-{}-{index}", std::process::id()))
        .collect();
    let persistence: Arc<dyn crate::server::persistence::PersistenceBackend> = backend.clone();
    for graph in &graph_names {
        persistence
            .commit_mutation_batch(graph, &projection_budget_batch(graph, 0), None, 1)
            .await
            .unwrap();
    }
    NOW_MS.store(current_time_ms(), Ordering::SeqCst);
    let context = ProjectionContext {
        persistence: persistence.clone(),
        persist_dir: Some(root_str.clone()),
        graphs: graph_names
            .iter()
            .map(|graph| (graph.clone(), Arc::new(eg_core::graph::GraphCore::new())))
            .collect(),
        clock: stepping_clock,
    };
    // Readings: sweep start, then per graph one claim stamp and one ack. On
    // a start-stamped clock (no per-graph stamp) the second graph's ack is two
    // steps -- more than a lease term -- past the lease's start and is refused
    // as stale; re-stamped per graph, every ack is one step into a fresh lease.
    assert!(process_graphs(&context).await);
    for graph in &graph_names {
        let cursor = persistence
            .read_mutation_projection_cursor(graph, CONSUMER)
            .await
            .unwrap()
            .unwrap_or_else(|| panic!("{graph} was acknowledged inside its own lease"));
        assert_eq!(cursor.batch_id, format!("projection-budget-{graph}-0"));
    }

    backend.shutdown();
    drop(backend);
    let _ = std::fs::remove_dir_all(root);
}
