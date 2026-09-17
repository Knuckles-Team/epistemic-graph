//! Pipelined producer fan-in over one [`GraphWriter`]
//! (CONCEPT:EG-KG.txn.write-path-benchmarks).

use std::sync::Arc;

use tokio::sync::oneshot;

use super::{GraphWriter, WriteOp, WriteOutcome};

/// Fan `n` pipelined `AddNode` writes (`node_id = "n{i}"`, properties
/// `properties(i)`) from `producers` concurrent tasks into one writer.
///
/// This is the ingestion-firehose load shape the coalescer benchmarks and tests
/// drive: each producer fires ops without blocking on their replies, so the
/// worker sees a deep queue and batches it. When the bounded queue is full the
/// producer drains one pending reply and retries the SAME op; nothing is ever
/// applied inline, because that could overtake an accepted ticket. Replies are
/// awaited after each producer's burst. Returns how many replies were lost
/// (their sender dropped without an outcome).
pub async fn fan_in_add_nodes(
    writer: &Arc<GraphWriter>,
    n: usize,
    producers: usize,
    properties: fn(i64) -> Vec<u8>,
) -> usize {
    let tasks: Vec<_> = (0..producers)
        .map(|producer| {
            let writer = writer.clone();
            tokio::spawn(async move {
                let mut pending = Vec::new();
                for i in (producer..n).step_by(producers) {
                    let (reply, rx) = oneshot::channel();
                    let op = WriteOp::AddNode {
                        node_id: format!("n{i}"),
                        properties_msgpack: properties(i as i64),
                        reply,
                    };
                    enqueue_draining(&writer, op, &mut pending).await;
                    pending.push(rx);
                }
                let mut lost = 0;
                for rx in pending {
                    lost += usize::from(rx.await.is_err());
                }
                lost
            })
        })
        .collect();
    let mut lost = 0;
    for task in tasks {
        lost += task.await.expect("fan-in producer task panicked");
    }
    lost
}

/// Enqueue `op`, draining the most recent pending reply (or yielding) while the
/// bounded queue refuses it.
async fn enqueue_draining(
    writer: &GraphWriter,
    mut op: WriteOp,
    pending: &mut Vec<oneshot::Receiver<WriteOutcome>>,
) {
    while let Err(returned) = writer.try_enqueue(op) {
        op = returned;
        match pending.pop() {
            Some(front) => {
                let _ = front.await;
            }
            None => tokio::task::yield_now().await,
        }
    }
}
