//! Change-notification, broker, append-log, and Redis collection probes.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use super::{allocation_bytes, timed, Observation, ProbeError};

/// How long a wire probe waits at a rendezvous before reporting a stall.
///
/// These probes deliberately park one thread inside a change callback to prove
/// the subscriber list stays available. If the defect they test for is present
/// the parties never meet, so an unbounded wait would hang the probe on exactly
/// the fault it exists to detect -- and a probe that hangs reports nothing,
/// which is strictly worse than one that fails by name.
const PROBE_RENDEZVOUS_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(120);
use epistemic_graph::broker::{self, Binding, ExchangeKind, ReadFrom, StreamRetention};
use epistemic_graph::graph::{ChangeEvent, ChangeNotifier, ChangeSink, GraphCore};

/// Join a probe worker within [`PROBE_RENDEZVOUS_TIMEOUT`], or say which probe
/// stalled.
///
/// `performance_probe` is a module of the SERVER BIN and reaches the engine as
/// an ordinary dependency, so the library's `pub(crate)` `bounded_join` is not
/// in scope here. Same mechanism, kept local: the unbounded join runs on a
/// throwaway thread and the deadline is this `recv_timeout`.
fn join_probe_worker<T: Send + 'static>(
    worker: std::thread::JoinHandle<T>,
    what: &str,
) -> Result<T, ProbeError> {
    let (finished, waiting) = std::sync::mpsc::sync_channel(1);
    #[allow(clippy::disallowed_methods)]
    let joiner = std::thread::spawn(move || {
        let value = worker.join();
        let _ = finished.send(());
        value
    });
    if waiting.recv_timeout(PROBE_RENDEZVOUS_TIMEOUT).is_err() {
        return Err(format!("{what}: the worker thread did not finish").into());
    }
    // The helper already signalled, so neither join can block.
    #[allow(clippy::disallowed_methods)]
    match joiner.join() {
        Ok(Ok(value)) => Ok(value),
        _ => Err(format!("{what} thread panicked").into()),
    }
}

struct CountingSink(AtomicU64);

impl ChangeSink for CountingSink {
    fn on_change(&self, _event: &ChangeEvent) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

struct ReentrantSink {
    notifier: Arc<ChangeNotifier>,
    next: Arc<dyn ChangeSink>,
}

impl ChangeSink for ReentrantSink {
    fn on_change(&self, _event: &ChangeEvent) {
        self.notifier.subscribe(&self.next);
    }
}

struct BlockingSink {
    entered: std::sync::mpsc::SyncSender<()>,
    release: std::sync::Mutex<std::sync::mpsc::Receiver<()>>,
}

impl ChangeSink for BlockingSink {
    fn on_change(&self, _event: &ChangeEvent) {
        let _ = self.entered.send(());
        // Bounded: if the probe driver never releases this sink, the callback
        // returns instead of parking this thread for the life of the process.
        let _ = self
            .release
            .lock()
            .expect("probe release lock")
            .recv_timeout(PROBE_RENDEZVOUS_TIMEOUT);
    }
}

pub(super) fn probe_notifications(scale: usize) -> Result<Observation, ProbeError> {
    let notifier = Arc::new(ChangeNotifier::default());
    notifier.set_graph("g37");
    let (counters, mut retained) = retain_notification_counters(&notifier, scale);
    let latency = exercise_notification_reentrancy(&notifier, &mut retained)?;
    let fanout_exact = counters
        .iter()
        .all(|counter| counter.0.load(Ordering::SeqCst) == 1);
    Ok(Observation {
        work_units: scale.saturating_add(3).max(1) as u64,
        memory_bytes: allocation_bytes::<Arc<dyn ChangeSink>>(retained.capacity()),
        latency_ns: latency,
        equivalent: fanout_exact && notifier.has_subscribers(),
    })
}

fn retain_notification_counters(
    notifier: &Arc<ChangeNotifier>,
    scale: usize,
) -> (Vec<Arc<CountingSink>>, Vec<Arc<dyn ChangeSink>>) {
    let counters: Vec<Arc<CountingSink>> = (0..scale)
        .map(|_| Arc::new(CountingSink(AtomicU64::new(0))))
        .collect();
    let mut retained: Vec<Arc<dyn ChangeSink>> = Vec::with_capacity(scale + 3);
    for counter in &counters {
        let sink: Arc<dyn ChangeSink> = counter.clone();
        notifier.subscribe(&sink);
        retained.push(sink);
    }
    (counters, retained)
}

fn exercise_notification_reentrancy(
    notifier: &Arc<ChangeNotifier>,
    retained: &mut Vec<Arc<dyn ChangeSink>>,
) -> Result<u64, ProbeError> {
    let next = Arc::new(CountingSink(AtomicU64::new(0)));
    let next_sink: Arc<dyn ChangeSink> = next.clone();
    let reentrant: Arc<dyn ChangeSink> = Arc::new(ReentrantSink {
        notifier: notifier.clone(),
        next: next_sink.clone(),
    });
    notifier.subscribe(&reentrant);
    retained.push(reentrant);
    retained.push(next_sink.clone());

    let (entered_tx, entered_rx) = std::sync::mpsc::sync_channel(1);
    let (release_tx, release_rx) = std::sync::mpsc::sync_channel(1);
    let blocking: Arc<dyn ChangeSink> = Arc::new(BlockingSink {
        entered: entered_tx,
        release: std::sync::Mutex::new(release_rx),
    });
    notifier.subscribe(&blocking);
    retained.push(blocking);

    let worker_notifier = notifier.clone();
    let started = Instant::now();
    let worker = std::thread::spawn(move || worker_notifier.emit(1));
    entered_rx
        .recv_timeout(PROBE_RENDEZVOUS_TIMEOUT)
        .map_err(|_| "notification probe: the blocking sink never entered its change callback")?;
    // This must complete while the slow callback is blocked. It deadlocks here
    // if callbacks still run under the subscriber-list mutex.
    notifier.subscribe(&next_sink);
    release_tx.send(())?;
    join_probe_worker(worker, "notification probe")?;
    let latency = u64::try_from(started.elapsed().as_nanos())
        .unwrap_or(u64::MAX)
        .max(1);
    // Do not emit again: the deliberately blocking sink is retained. Reentrancy
    // was exercised during the first emit and the concurrently-added sink proves
    // subscriber-list maintenance remained available.
    Ok(latency)
}

pub(super) fn probe_broker(row_id: &str, scale: usize) -> Result<Observation, ProbeError> {
    match row_id {
        "G37-HP-043" => {
            let pattern = std::iter::repeat_n("#", scale)
                .chain(std::iter::once("tail"))
                .collect::<Vec<_>>()
                .join(".");
            let key = std::iter::repeat_n("word", scale)
                .chain(std::iter::once("tail"))
                .collect::<Vec<_>>()
                .join(".");
            let (matched, latency) = timed(|| broker::topic_matches(&pattern, &key));
            let miss = broker::topic_matches(&pattern, &format!("{key}.extra"));
            Ok(Observation {
                work_units: scale.saturating_mul(scale).max(1) as u64,
                memory_bytes: (pattern.capacity() + key.capacity()).max(1) as u64,
                latency_ns: latency,
                equivalent: matched && !miss,
            })
        }
        "G37-HP-044" => {
            let bindings: Vec<_> = (0..scale)
                .map(|index| Binding {
                    exchange: "g37".to_string(),
                    queue: format!("queue-{:04}", index % 17),
                    routing_key: "a.*".to_string(),
                })
                .collect();
            let (routed, latency) = timed(|| broker::route(ExchangeKind::Topic, &bindings, "a.b"));
            let mut seen = HashSet::new();
            let reference: Vec<_> = bindings
                .iter()
                .filter(|binding| {
                    broker::topic_matches(&binding.routing_key, "a.b")
                        && seen.insert(binding.queue.clone())
                })
                .map(|binding| binding.queue.clone())
                .collect();
            Ok(Observation {
                work_units: scale.max(1) as u64,
                memory_bytes: allocation_bytes::<Binding>(bindings.capacity()),
                latency_ns: latency,
                equivalent: routed == reference,
            })
        }
        _ => Err("invalid broker probe row".into()),
    }
}

pub(super) fn probe_appendlog(row_id: &str, scale: usize) -> Result<Observation, ProbeError> {
    let graph = GraphCore::new();
    let stream = "g37";
    broker::declare_stream(
        &graph,
        stream,
        &StreamRetention {
            max_messages: Some((scale / 2).max(1) as u64),
            max_age_ms: Some(scale.max(1) as u64),
        },
    );
    for index in 0..scale {
        broker::stream_publish(&graph, stream, &index.to_le_bytes(), index as u64);
    }
    match row_id {
        "G37-HP-045" => {
            let from = (scale / 2) as i64;
            let (rows, latency) =
                timed(|| broker::stream_read(&graph, stream, ReadFrom::Offset(from), 16));
            Ok(Observation {
                work_units: scale
                    .saturating_add(rows.len().saturating_mul(scale.ilog2() as usize + 1))
                    .max(1) as u64,
                memory_bytes: graph.memory_estimate().max(1),
                latency_ns: latency,
                equivalent: rows.len() == scale.saturating_sub(scale / 2).min(16)
                    && rows.windows(2).all(|pair| pair[0].0 < pair[1].0)
                    && rows.first().is_none_or(|row| row.0 == from),
            })
        }
        "G37-HP-046" => {
            let now = scale.saturating_mul(2) as u64;
            let (removed, latency) = timed(|| broker::stream_trim(&graph, stream, now));
            let remaining = broker::stream_read(&graph, stream, ReadFrom::Earliest, 0);
            Ok(Observation {
                work_units: scale.saturating_mul(2).max(1) as u64,
                memory_bytes: graph.memory_estimate().max(1),
                latency_ns: latency,
                equivalent: removed + remaining.len() == scale
                    && remaining.len() <= (scale / 2).max(1),
            })
        }
        _ => Err("invalid append-log probe row".into()),
    }
}

pub(super) fn probe_redis_kernel(row_id: &str, scale: usize) -> Result<Observation, ProbeError> {
    match row_id {
        "G37-HP-047" => probe_redis_hash_set(scale),
        "G37-HP-048" => probe_redis_list_push(scale),
        _ => Err("invalid Redis probe row".into()),
    }
}

fn probe_redis_hash_set(scale: usize) -> Result<Observation, ProbeError> {
    let mut ordered: Vec<(Vec<u8>, Vec<u8>)> = (0..scale)
        .map(|index| (index.to_le_bytes().to_vec(), vec![0]))
        .collect();
    let updates: Vec<_> = (0..scale)
        .map(|index| {
            (
                (index / 2).to_le_bytes().to_vec(),
                index.to_le_bytes().to_vec(),
            )
        })
        .collect();
    let (added, latency) = timed(|| {
        let mut positions: HashMap<Vec<u8>, usize> = ordered
            .iter()
            .enumerate()
            .map(|(index, (field, _))| (field.clone(), index))
            .collect();
        let mut added = 0usize;
        for (field, value) in &updates {
            if let Some(index) = positions.get(field).copied() {
                ordered[index].1 = value.clone();
            } else {
                positions.insert(field.clone(), ordered.len());
                ordered.push((field.clone(), value.clone()));
                added += 1;
            }
        }
        added
    });
    let fields: HashSet<_> = ordered.iter().map(|(field, _)| field).collect();
    Ok(Observation {
        work_units: scale.saturating_mul(2).max(1) as u64,
        memory_bytes: allocation_bytes::<(Vec<u8>, Vec<u8>)>(
            ordered.capacity() + updates.capacity(),
        ),
        latency_ns: latency,
        equivalent: fields.len() == ordered.len() && added == 0,
    })
}

fn probe_redis_list_push(scale: usize) -> Result<Observation, ProbeError> {
    let mut list: Vec<Vec<u8>> = (0..scale)
        .map(|index| index.to_le_bytes().to_vec())
        .collect();
    let values: Vec<Vec<u8>> = (scale..scale.saturating_mul(2))
        .map(|index| index.to_le_bytes().to_vec())
        .collect();
    let original = list.clone();
    let (_, latency) = timed(|| {
        let mut prefixed = Vec::with_capacity(list.len() + values.len());
        prefixed.extend(values.iter().rev().cloned());
        prefixed.append(&mut list);
        list = prefixed;
    });
    let expected: Vec<_> = values.iter().rev().cloned().chain(original).collect();
    Ok(Observation {
        work_units: scale.saturating_mul(2).max(1) as u64,
        memory_bytes: allocation_bytes::<Vec<u8>>(list.capacity() + values.capacity()),
        latency_ns: latency,
        equivalent: list == expected,
    })
}
