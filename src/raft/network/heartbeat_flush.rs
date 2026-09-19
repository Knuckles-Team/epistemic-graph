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
    /// Each wake gets one short window, then every READY peer (EH-288: one not already
    /// mid-flush) is spawned as its OWN independent flush over the shared [`PeerPool`],
    /// and this loop returns to waiting at once rather than waiting for them.
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
            Arc::clone(&self).spawn_ready_flushes(Arc::clone(&pool));
        }
        self.fail_pending("raft heartbeat coalescer stopped");
    }

    /// Stop the worker and release every caller still waiting on a QUEUED heartbeat.
    /// A heartbeat already handed to an in-flight per-peer flush (EH-288) completes or
    /// fails on its own round trip, same as before this peer-independent flush split.
    pub(crate) fn stop(&self) {
        self.stopping.store(true, Ordering::Release);
        self.wake.notify_waiters();
        self.fail_pending("raft heartbeat coalescer stopped");
    }

    /// Fold one taken batch set into the coalescing counters. Shared by both take
    /// paths so the two never drift: a batch set that folded nothing is not a flush.
    fn record_fold(&self, batches: &[(String, Vec<PendingHeartbeat>)]) {
        let folded: u64 = batches.iter().map(|(_, v)| v.len() as u64).sum();
        if folded > 0 {
            self.coalesced.fetch_add(folded, Ordering::Relaxed);
            self.flushes.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn take_pending(&self) -> Vec<(String, Vec<PendingHeartbeat>)> {
        let mut pending = self.pending.lock().unwrap();
        pending.drain().collect()
    }

    pub(super) fn drain_pending(&self) -> Vec<(String, Vec<PendingHeartbeat>)> {
        let drained = self.take_pending();
        self.record_fold(&drained);
        drained
    }

    /// Take every peer's queued batch EXCEPT one already being flushed (EH-288): that
    /// peer's own accumulating heartbeats are left queued for the window AFTER its
    /// current round trip finishes, so at most one batch per peer is ever outstanding
    /// -- preserving that peer's own AppendEntries order -- while a peer with no flush
    /// in flight is never held up by one that does.
    fn take_ready_pending(&self) -> Vec<(String, Vec<PendingHeartbeat>)> {
        let mut pending = self.pending.lock().unwrap();
        let in_flight = self.in_flight.lock().unwrap();
        let ready_addrs: Vec<String> = pending
            .keys()
            .filter(|addr| !in_flight.contains(*addr))
            .cloned()
            .collect();
        drop(in_flight);
        let ready: Vec<(String, Vec<PendingHeartbeat>)> = ready_addrs
            .into_iter()
            .filter_map(|addr| pending.remove(&addr).map(|items| (addr, items)))
            .collect();
        self.record_fold(&ready);
        ready
    }

    /// Spawn one independent flush task per ready peer (EH-288): peers flush
    /// concurrently with EACH OTHER, and none of them -- however slow -- blocks this
    /// coalescing loop from returning to `wake.notified()` for the next window. This
    /// is the fix for the confirmed head-of-line defect: previously one shared
    /// `flush_pending` call awaited every peer's round trip before the worker could
    /// pick up newly queued heartbeats for ANY peer, so one slow peer withheld
    /// delivery to every other peer behind it.
    fn spawn_ready_flushes(self: Arc<Self>, pool: Arc<PeerPool>) {
        for (addr, pending) in self.take_ready_pending() {
            self.in_flight.lock().unwrap().insert(addr.clone());
            let this = Arc::clone(&self);
            let pool = Arc::clone(&pool);
            let flush_addr = addr.clone();
            tokio::spawn(async move {
                flush_peer(&pool, flush_addr, pending).await;
                this.in_flight.lock().unwrap().remove(&addr);
                // A heartbeat queued for this peer WHILE it was in flight has no
                // waiter guaranteed to still be blocked on `wake.notified()` for it
                // (the run loop may already be mid-window on a LATER wake) -- nudge
                // it once more now that this peer is flushable again.
                this.wake.notify_one();
            });
        }
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
