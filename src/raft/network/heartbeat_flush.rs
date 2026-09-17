//! The [`HeartbeatCoalescer`] flush worker: drain queued heartbeats, send one
//! bounded batch per peer, and complete (or fail) every queued caller.
//!
//! Split out of `network.rs` (CCCC burn-down lane L-raft-b). The per-peer flush
//! was one async closure whose three reply arms each repeated the completion
//! loop; as named functions it is flat. `network.rs` was already over the KISS
//! whole-file thresholds and `HeartbeatCoalescer` over `methods_per_class`, so
//! the worker half of the type's impl lives here and the parent's counts only
//! go down.

use std::io;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use super::{
    GroupRpc, GroupRpcReply, HeartbeatCoalescer, PeerPool, PendingHeartbeat,
    HEARTBEAT_COALESCE_WINDOW,
};

impl HeartbeatCoalescer {
    /// Run the bounded coalescing worker used by a live [`super::super::multi::MultiRaft`].
    /// Each wake gets one short window, then every peer is drained into at most one
    /// bounded batch and sent through the shared [`PeerPool`].
    pub(crate) async fn run(self: Arc<Self>, pool: Arc<PeerPool>) {
        loop {
            self.wake.notified().await;
            if self.stopping.load(Ordering::Acquire) {
                break;
            }
            tokio::select! {
                _ = tokio::time::sleep(HEARTBEAT_COALESCE_WINDOW) => {}
                _ = self.wake.notified() => {}
            }
            if self.stopping.load(Ordering::Acquire) {
                break;
            }
            self.flush_pending(&pool).await;
        }
        self.fail_pending("raft heartbeat coalescer stopped");
    }

    /// Stop the worker and release every caller waiting on a queued heartbeat.
    pub(crate) fn stop(&self) {
        self.stopping.store(true, Ordering::Release);
        self.wake.notify_waiters();
        self.fail_pending("raft heartbeat coalescer stopped");
    }

    fn take_pending(&self) -> Vec<(String, Vec<PendingHeartbeat>)> {
        let mut pending = self.pending.lock().unwrap();
        pending.drain().collect()
    }

    pub(super) fn drain_pending(&self) -> Vec<(String, Vec<PendingHeartbeat>)> {
        let drained = self.take_pending();
        let folded: u64 = drained.iter().map(|(_, v)| v.len() as u64).sum();
        if folded > 0 {
            self.coalesced.fetch_add(folded, Ordering::Relaxed);
            self.flushes.fetch_add(1, Ordering::Relaxed);
        }
        drained
    }

    async fn flush_pending(&self, pool: &PeerPool) {
        // Flush peers concurrently: one unavailable destination must not hold the
        // heartbeat cadence of every other peer behind the transport timeout.
        let jobs = self
            .drain_pending()
            .into_iter()
            .map(|(addr, pending)| flush_peer(pool, addr, pending));
        futures::future::join_all(jobs).await;
    }

    fn fail_pending(&self, error: &str) {
        // These requests never reached a peer, so shutdown/failure cleanup must
        // not report them as emitted/coalesced frames in the live metrics.
        for (_, pending) in self.take_pending() {
            fail_heartbeats(pending, error);
        }
    }
}

/// Send one peer's queued heartbeats as a single batch and complete each caller
/// with its ordered reply.
async fn flush_peer(pool: &PeerPool, addr: String, pending: Vec<PendingHeartbeat>) {
    let batch: Vec<GroupRpc> = pending.iter().map(|item| item.rpc.clone()).collect();
    let result = HeartbeatCoalescer::send_batch(pool, &addr, batch).await;
    complete_heartbeats(pending, result);
}

/// Pair every queued heartbeat with its reply, or fail them all when the batch
/// failed or answered with a different number of replies.
fn complete_heartbeats(
    pending: Vec<PendingHeartbeat>,
    result: Result<Vec<GroupRpcReply>, io::Error>,
) {
    match result {
        Ok(replies) if replies.len() == pending.len() => {
            for (item, reply) in pending.into_iter().zip(replies) {
                if let Some(done) = item.completion {
                    let _ = done.send(Ok(reply));
                }
            }
        }
        Ok(replies) => {
            let error = format!(
                "raft heartbeat batch reply count mismatch: expected {}, got {}",
                pending.len(),
                replies.len()
            );
            fail_heartbeats(pending, &error);
        }
        Err(error) => fail_heartbeats(pending, &format!("raft heartbeat batch failed: {error}")),
    }
}

fn fail_heartbeats(pending: Vec<PendingHeartbeat>, error: &str) {
    for item in pending {
        if let Some(done) = item.completion {
            let _ = done.send(Err(error.to_string()));
        }
    }
}
