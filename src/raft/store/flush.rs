//! Log-flush notification off the Raft core (EH-288).
//!
//! openraft's `RaftLogStorage::append` must return once the entries are
//! READABLE and report durability later through its `IOFlushed` callback. The
//! store used to await the shard writer's group-commit fsync inside `append`,
//! which parked the group's Raft core -- and with it every heartbeat and vote
//! reply -- for as long as the shared shard writer took to reach that fsync;
//! under a concurrent write workload that was seconds, long enough for followers
//! to elect. `append` now only enqueues the write (the writer's per-shard FIFO
//! makes it readable to every later command) and hands the completion here: one
//! task per store delivers the callbacks in append order.

use super::*;

/// One enqueued append awaiting its fsync.
struct PendingFlush {
    completion: tokio::sync::oneshot::Receiver<Result<(), String>>,
    callback: IOFlushed<TypeConfig>,
}

/// Appends whose durability callback may be pending at once. Far above the
/// Raft core's in-flight append window; reaching it means the redb writer has
/// stalled, and the append fails with a named error instead of queueing
/// without bound.
const MAX_PENDING_FLUSHES: usize = 65_536;

/// Delivers each append's durability callback, in order, from its own task.
#[derive(Default)]
pub(super) struct FlushNotifier {
    queue: std::sync::OnceLock<tokio::sync::mpsc::Sender<PendingFlush>>,
}

impl FlushNotifier {
    /// Report `callback` once `completion` resolves. Started lazily on the
    /// runtime of the first append; it ends when the store is dropped.
    pub(super) fn notify(
        &self,
        completion: tokio::sync::oneshot::Receiver<Result<(), String>>,
        callback: IOFlushed<TypeConfig>,
    ) {
        let queue = self.queue.get_or_init(|| {
            let (queue, pending) = tokio::sync::mpsc::channel(MAX_PENDING_FLUSHES);
            tokio::spawn(deliver(pending));
            queue
        });
        if let Err(unsent) = queue.try_send(PendingFlush {
            completion,
            callback,
        }) {
            let reason = match &unsent {
                tokio::sync::mpsc::error::TrySendError::Full(_) => {
                    "raft log flush queue is full: the redb writer has stalled"
                }
                tokio::sync::mpsc::error::TrySendError::Closed(_) => {
                    "raft log flush notifier stopped"
                }
            };
            unsent
                .into_inner()
                .callback
                .io_completed(Err(ioerr(reason)));
        }
    }
}

async fn deliver(mut pending: tokio::sync::mpsc::Receiver<PendingFlush>) {
    while let Some(flush) = pending.recv().await {
        let flushed = match flush.completion.await {
            Ok(result) => result.map_err(ioerr),
            Err(_) => Err(ioerr("redb writer dropped raft_log_append completion")),
        };
        flush.callback.io_completed(flushed);
    }
}
