//! Shared redb fan-in harness for the durable-write benches
//! (`redb_group_commit_bench`, `provenance_anchor_bench`).
//!
//! Included by `#[path]` from each bench target, so both measure the identical
//! write pattern: `n` authoritative (commit-before-ack) `record_durable` writes
//! from concurrent tasks into ONE redb backend.

use std::sync::Arc;

use epistemic_graph::protocol::Method;
use epistemic_graph::server::persistence::redb_backend::RedbBackend;
use epistemic_graph::server::persistence::PersistenceBackend;

pub const GRAPH: &str = "__commons__";

pub fn node_props(k: usize) -> Vec<u8> {
    rmp_serde::to_vec_named(&serde_json::json!({ "k": k })).unwrap()
}

/// Fan `n` authoritative (commit-before-ack) writes from concurrent tasks into
/// ONE redb backend. Producers PIPELINE — each fires its own `record_durable`
/// task and they all await together — so the writer sees concurrent in-flight
/// barrier writes within the micro-linger window (the case the linger targets).
/// Writes start at offset `base` so successive criterion iterations write
/// distinct node ids (no upsert short-circuit skewing the I/O).
pub async fn fan_in(backend: &Arc<RedbBackend>, n: usize, base: usize) {
    let mut tasks = Vec::with_capacity(n);
    for i in 0..n {
        let b = backend.clone();
        let id = base + i;
        tasks.push(tokio::spawn(async move {
            b.record_durable(
                GRAPH,
                &Method::AddNode {
                    node_id: format!("n{id}"),
                    properties_msgpack: node_props(id),
                },
            )
            .await
        }));
    }
    for t in tasks {
        t.await.unwrap().expect("durable commit ok");
    }
}
