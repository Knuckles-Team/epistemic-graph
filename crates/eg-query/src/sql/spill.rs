//! Bounded, cancellable SQL result collection with one off-executor Arrow IPC
//! spill owner (CONCEPT:EG-KG.query.streaming-spillable-collect, EG-P1-4).

use std::sync::{Arc, Mutex};

use arrow::datatypes::SchemaRef;

/// Implicit max rows guarded into the result. Transport is one Response per
/// Request (no streaming), so an unbounded SELECT would buffer the whole graph in
/// one message; we cap and truncate.
pub(super) const MAX_ROWS: usize = 50_000;

// ── streaming / spillable / cancellable SQL collect (CONCEPT:EG-KG.query.streaming-spillable-collect, EG-P1-4) ──
//
// `DataFrame::collect()` materializes the ENTIRE result as one `Vec<RecordBatch>`
// before the caller sees anything — no batch-at-a-time consumption, no way to stop
// a running query early, and no bound on peak memory short of `MAX_ROWS` (which is
// only applied AFTER everything is already resident). `collect_streaming` below is
// the drop-in replacement `run`/`run_typed` use instead: it drives DataFusion's own
// `SendableRecordBatchStream` (the SAME physical plan, just pulled batch-by-batch
// instead of awaited-to-completion), checks a [`CancellationToken`] BETWEEN
// batches, and spills already-buffered batches to a temp Arrow-IPC file once the
// running row count crosses a threshold — bounding resident memory to that
// threshold regardless of total result size.
//
// This module element compiles under `sql` only (arrow-ipc + futures-util are both
// already-resolved transitive deps at that feature, so this adds no new crate to a
// default/non-sql build's tree).

#[derive(Debug, Default)]
struct CancellationState {
    cancelled: std::sync::atomic::AtomicBool,
    wakers: Mutex<Vec<std::task::Waker>>,
}

/// Cooperative cancellation signal for a running SQL execution
/// (CONCEPT:EG-KG.query.streaming-spillable-collect). It wakes a collector even
/// while its source is stalled and stops before accepting the next completed
/// batch. `Clone` + `Send + Sync`, so a caller can hold one end while the query
/// runs with another.
#[derive(Clone, Debug, Default)]
pub struct CancellationToken(Arc<CancellationState>);

impl CancellationToken {
    /// A fresh, not-yet-cancelled token.
    pub fn new() -> Self {
        Self::default()
    }

    /// Raise the flag — the next batch boundary a [`collect_streaming`] loop checks
    /// will observe it and stop.
    pub fn cancel(&self) {
        self.0
            .cancelled
            .store(true, std::sync::atomic::Ordering::SeqCst);
        let wakers =
            std::mem::take(&mut *self.0.wakers.lock().expect("cancellation lock poisoned"));
        for waker in wakers {
            waker.wake();
        }
    }

    /// Has [`Self::cancel`] been called (on this token or any clone of it)?
    pub fn is_cancelled(&self) -> bool {
        self.0.cancelled.load(std::sync::atomic::Ordering::SeqCst)
    }

    async fn next_or_cancelled<S>(&self, stream: &mut S) -> Result<Option<S::Item>, ()>
    where
        S: futures_util::Stream + Unpin,
    {
        std::future::poll_fn(|context| {
            if self.is_cancelled() {
                return std::task::Poll::Ready(Err(()));
            }
            let mut wakers = self.0.wakers.lock().expect("cancellation lock poisoned");
            if !wakers.iter().any(|waker| waker.will_wake(context.waker())) {
                wakers.push(context.waker().clone());
            }
            drop(wakers);
            if self.is_cancelled() {
                return std::task::Poll::Ready(Err(()));
            }
            futures_util::Stream::poll_next(std::pin::Pin::new(&mut *stream), context).map(Ok)
        })
        .await
    }
}

/// Row-count threshold past which [`collect_streaming`] spills already-buffered
/// batches to a temp Arrow-IPC file instead of holding them resident, bounding the
/// SQL collect path's peak memory regardless of total result size. Overridable via
/// `EPISTEMIC_GRAPH_SQL_SPILL_ROWS`; the default sits comfortably above [`MAX_ROWS`]
/// so an ORDINARY served query (already capped there) essentially never spills in
/// practice — the budget exists for the streaming path's internal accumulation
/// ahead of that cap, not to change the served row cap itself. A caller exercising
/// the spill path directly (e.g. a bulk export or an admin query with a raised row
/// cap) passes a smaller threshold to [`collect_streaming`] explicitly.
pub fn default_spill_rows() -> usize {
    let configured = std::env::var("EPISTEMIC_GRAPH_SQL_SPILL_ROWS")
        .ok()
        .and_then(|s| s.trim().parse::<usize>().ok())
        .unwrap_or(0);
    if configured == 0 {
        200_000
    } else {
        configured
    }
}

/// Summary of how a [`collect_streaming`] run ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct StreamOutcome {
    /// Stopped because the [`CancellationToken`] fired mid-stream (before the source
    /// was drained or the row cap reached).
    pub cancelled: bool,
    /// At least one spill-to-disk round-trip happened (the running row count
    /// crossed the spill threshold at least once).
    pub spilled: bool,
    /// Total rows actually pulled off the stream (across every batch seen, whether
    /// resident or spilled) before stopping.
    pub rows: usize,
}

/// Commands sent to the one blocking worker that owns a SQL spill file for its
/// complete lifetime. The async collector never owns an open file and never runs
/// create/write/finish/read/remove work on its executor thread.
enum SpillCommand {
    Append {
        batches: Vec<arrow::record_batch::RecordBatch>,
        completed: SpillAppendCompletion,
    },
    Finish,
    Discard,
}

struct SpillAppendCompletion {
    acknowledgement: Arc<SpillAppendAck>,
    completed: bool,
}

impl SpillAppendCompletion {
    fn new(acknowledgement: Arc<SpillAppendAck>) -> Self {
        Self {
            acknowledgement,
            completed: false,
        }
    }

    fn complete(mut self, result: Result<(), String>) {
        self.completed = true;
        self.acknowledgement.complete(result);
    }
}

impl Drop for SpillAppendCompletion {
    fn drop(&mut self) {
        if !self.completed {
            self.acknowledgement.complete(Err(
                "spill worker stopped before append completion".to_string()
            ));
        }
    }
}

/// The acknowledgement's two-field state: whichever of the outcome and the
/// waiting task's waker exists yet. Named fields rather than a tuple so the
/// `complete`/`wait` pair below cannot transpose them.
#[derive(Default)]
struct SpillAppendState {
    outcome: Option<Result<(), String>>,
    waker: Option<std::task::Waker>,
}

#[derive(Default)]
struct SpillAppendAck {
    state: Mutex<SpillAppendState>,
}

impl SpillAppendAck {
    fn complete(&self, result: Result<(), String>) {
        let waker = {
            let mut state = self
                .state
                .lock()
                .expect("spill acknowledgement lock poisoned");
            state.outcome = Some(result);
            state.waker.take()
        };
        if let Some(waker) = waker {
            waker.wake();
        }
    }

    async fn wait(&self) -> Result<(), String> {
        std::future::poll_fn(|context| {
            let mut state = self
                .state
                .lock()
                .expect("spill acknowledgement lock poisoned");
            if let Some(result) = state.outcome.take() {
                return std::task::Poll::Ready(result);
            }
            state.waker = Some(context.waker().clone());
            std::task::Poll::Pending
        })
        .await
    }
}

/// Test-only observation points for deterministic executor and cleanup proofs.
/// Production callers pass `None`, so the spill hot path carries no hook state.
#[derive(Default)]
struct SpillIoHooks {
    #[cfg(test)]
    worker_entries: std::sync::atomic::AtomicUsize,
    #[cfg(test)]
    completed_appends: std::sync::atomic::AtomicUsize,
    #[cfg(test)]
    start_barrier: Option<Arc<std::sync::Barrier>>,
    #[cfg(test)]
    corrupt_before_read: std::sync::atomic::AtomicBool,
    #[cfg(test)]
    force_cleanup_error: std::sync::atomic::AtomicBool,
    #[cfg(test)]
    cleanup_results: Mutex<Vec<(std::path::PathBuf, bool)>>,
    #[cfg(test)]
    worker_entered: Option<std::sync::mpsc::SyncSender<()>>,
    #[cfg(test)]
    cleanup_completed: Option<std::sync::mpsc::SyncSender<()>>,
}

/// Async-side handle for a single owned blocking spill job. An unbounded channel
/// is safe here because [`Self::append`] awaits each write acknowledgement before
/// the collector can enqueue another resident batch. Dropping the handle closes
/// the channel; the worker then closes and removes its file before exiting.
struct SpillWorker {
    commands: Option<std::sync::mpsc::Sender<SpillCommand>>,
    worker: Option<tokio::task::JoinHandle<Result<Vec<arrow::record_batch::RecordBatch>, String>>>,
}

impl SpillWorker {
    fn start(schema: SchemaRef) -> Self {
        Self::start_with_hooks(schema, None)
    }

    fn start_with_hooks(schema: SchemaRef, hooks: Option<Arc<SpillIoHooks>>) -> Self {
        let (commands, receiver) = std::sync::mpsc::channel();
        let worker = tokio::task::spawn_blocking(move || run_spill_worker(schema, receiver, hooks));
        Self {
            commands: Some(commands),
            worker: Some(worker),
        }
    }

    async fn append(&self, batches: Vec<arrow::record_batch::RecordBatch>) -> Result<(), String> {
        let completed = Arc::new(SpillAppendAck::default());
        self.commands
            .as_ref()
            .ok_or("spill worker already finished")?
            .send(SpillCommand::Append {
                batches,
                completed: SpillAppendCompletion::new(Arc::clone(&completed)),
            })
            .map_err(|_| "spill worker stopped before append".to_string())?;
        completed.wait().await
    }

    async fn finish(mut self) -> Result<Vec<arrow::record_batch::RecordBatch>, String> {
        self.send_terminal(SpillCommand::Finish)?;
        self.join().await
    }

    async fn discard(mut self) -> Result<(), String> {
        self.send_terminal(SpillCommand::Discard)?;
        self.join().await.map(|_| ())
    }

    fn send_terminal(&mut self, command: SpillCommand) -> Result<(), String> {
        let sender = self
            .commands
            .take()
            .ok_or("spill worker already finished")?;
        sender
            .send(command)
            .map_err(|_| "spill worker stopped before terminal command".to_string())
    }

    async fn join(&mut self) -> Result<Vec<arrow::record_batch::RecordBatch>, String> {
        self.worker
            .take()
            .ok_or("spill worker already joined")?
            .await
            .map_err(|e| format!("spill worker join: {e}"))?
    }
}

fn run_spill_worker(
    schema: SchemaRef,
    receiver: std::sync::mpsc::Receiver<SpillCommand>,
    hooks: Option<Arc<SpillIoHooks>>,
) -> Result<Vec<arrow::record_batch::RecordBatch>, String> {
    observe_worker_entry(hooks.as_ref());

    let mut path = None;
    let mut writer: Option<arrow::ipc::writer::FileWriter<std::fs::File>> = None;
    while let Ok(command) = receiver.recv() {
        match command {
            SpillCommand::Append { batches, completed } => {
                match append_spill_batches(&mut path, &schema, &mut writer, &batches) {
                    Ok(()) => {
                        record_completed_append(hooks.as_ref());
                        completed.complete(Ok(()));
                    }
                    Err(error) => {
                        writer.take();
                        let error = preserve_primary_error(
                            error,
                            cleanup_optional_spill_file(path.as_deref(), hooks.as_ref()),
                        );
                        completed.complete(Err(error.clone()));
                        return Err(error);
                    }
                }
            }
            SpillCommand::Finish => {
                let result = finish_and_read_spill(
                    path.as_deref().ok_or("spill file was never initialized")?,
                    writer.take(),
                    hooks.as_ref(),
                );
                let cleanup = cleanup_optional_spill_file(path.as_deref(), hooks.as_ref());
                return match result {
                    Ok(batches) => cleanup.map(|()| batches),
                    Err(error) => Err(preserve_primary_error(error, cleanup)),
                };
            }
            SpillCommand::Discard => {
                writer.take();
                cleanup_optional_spill_file(path.as_deref(), hooks.as_ref())?;
                return Ok(Vec::new());
            }
        }
    }

    // The async future was dropped without a terminal command. Channel closure is
    // the cancellation signal; all file ownership stays here on the blocking pool.
    writer.take();
    cleanup_optional_spill_file(path.as_deref(), hooks.as_ref())?;
    Ok(Vec::new())
}

fn observe_worker_entry(_hooks: Option<&Arc<SpillIoHooks>>) {
    #[cfg(test)]
    if let Some(hooks) = _hooks {
        hooks
            .worker_entries
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        if let Some(entered) = hooks.worker_entered.as_ref() {
            entered.send(()).expect("worker entry receiver was dropped");
        }
        if let Some(barrier) = hooks.start_barrier.as_ref() {
            barrier.wait();
        }
    }
}

fn preserve_primary_error(error: String, cleanup: Result<(), String>) -> String {
    match cleanup {
        Ok(()) => error,
        Err(cleanup_error) => format!("{error}; {cleanup_error}"),
    }
}

fn record_completed_append(_hooks: Option<&Arc<SpillIoHooks>>) {
    #[cfg(test)]
    if let Some(hooks) = _hooks {
        hooks
            .completed_appends
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }
}

fn append_spill_batches(
    path: &mut Option<std::path::PathBuf>,
    schema: &arrow::datatypes::Schema,
    writer: &mut Option<arrow::ipc::writer::FileWriter<std::fs::File>>,
    batches: &[arrow::record_batch::RecordBatch],
) -> Result<(), String> {
    if writer.is_none() {
        let (created_path, created_writer) = create_spill_writer(schema)?;
        *path = Some(created_path);
        *writer = Some(created_writer);
    }
    let writer = writer.as_mut().ok_or("spill writer was not initialized")?;
    for batch in batches {
        writer
            .write(batch)
            .map_err(|e| format!("spill write: {e}"))?;
    }
    Ok(())
}

fn create_spill_writer(
    schema: &arrow::datatypes::Schema,
) -> Result<
    (
        std::path::PathBuf,
        arrow::ipc::writer::FileWriter<std::fs::File>,
    ),
    String,
> {
    for _ in 0..32 {
        let path = spill_path();
        match open_new_spill_file(&path) {
            Ok(file) => {
                let writer = arrow::ipc::writer::FileWriter::try_new(file, schema)
                    .map_err(|error| preserve_created_file_error(error.to_string(), &path))?;
                return Ok((path, writer));
            }
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(format!("spill create {}: {error}", path.display())),
        }
    }
    Err("spill create exhausted 32 collision retries".to_string())
}

fn open_new_spill_file(path: &std::path::Path) -> std::io::Result<std::fs::File> {
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    options.open(path)
}

fn preserve_created_file_error(error: String, path: &std::path::Path) -> String {
    preserve_primary_error(
        format!("spill writer: {error}"),
        cleanup_spill_file(path, None),
    )
}

fn finish_and_read_spill(
    path: &std::path::Path,
    writer: Option<arrow::ipc::writer::FileWriter<std::fs::File>>,
    _hooks: Option<&Arc<SpillIoHooks>>,
) -> Result<Vec<arrow::record_batch::RecordBatch>, String> {
    let mut writer = writer.ok_or("spill file was never initialized")?;
    writer.finish().map_err(|e| format!("spill finish: {e}"))?;
    drop(writer);

    #[cfg(test)]
    if _hooks.is_some_and(|hooks| {
        hooks
            .corrupt_before_read
            .load(std::sync::atomic::Ordering::SeqCst)
    }) {
        std::fs::OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(path)
            .map_err(|e| format!("spill test corruption {}: {e}", path.display()))?;
    }

    let file =
        std::fs::File::open(path).map_err(|e| format!("spill reopen {}: {e}", path.display()))?;
    let reader = arrow::ipc::reader::FileReader::try_new(file, None)
        .map_err(|e| format!("spill reader: {e}"))?;
    reader
        .map(|batch| batch.map_err(|e| format!("spill read: {e}")))
        .collect()
}

fn cleanup_spill_file(
    path: &std::path::Path,
    _hooks: Option<&Arc<SpillIoHooks>>,
) -> Result<(), String> {
    #[cfg(test)]
    let force_error = _hooks.is_some_and(|hooks| {
        hooks
            .force_cleanup_error
            .load(std::sync::atomic::Ordering::SeqCst)
    });
    #[cfg(not(test))]
    let force_error = false;
    let result = if force_error {
        Err(format!(
            "spill cleanup {}: injected failure",
            path.display()
        ))
    } else {
        match std::fs::remove_file(path) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(format!("spill cleanup {}: {error}", path.display())),
        }
    };
    #[cfg(test)]
    if let Some(hooks) = _hooks {
        hooks
            .cleanup_results
            .lock()
            .expect("spill cleanup test lock poisoned")
            .push((path.to_path_buf(), result.is_ok()));
        if let Some(completed) = hooks.cleanup_completed.as_ref() {
            completed
                .send(())
                .expect("cleanup completion receiver was dropped");
        }
    }
    result
}

fn cleanup_optional_spill_file(
    path: Option<&std::path::Path>,
    hooks: Option<&Arc<SpillIoHooks>>,
) -> Result<(), String> {
    match path {
        Some(path) => cleanup_spill_file(path, hooks),
        None => Ok(()),
    }
}

/// A unique temp-file path for one spill (`<tmp>/eg-query-sql-spill-<pid>-<seq>.arrow`):
/// the process id + a monotonic counter avoid ordinary collisions across threads
/// and concurrent queries. Creation remains atomic and retries collisions, so an
/// existing path or symlink is never followed or truncated.
fn spill_path() -> std::path::PathBuf {
    use std::sync::atomic::{AtomicU64, Ordering};
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let seq = SEQ.fetch_add(1, Ordering::Relaxed);
    let pid = std::process::id();
    std::env::temp_dir().join(format!("eg-query-sql-spill-{pid}-{seq}.arrow"))
}

/// Batch-at-a-time, spillable, cancellable materialization of a batch stream
/// (CONCEPT:EG-KG.query.streaming-spillable-collect, EG-P1-4) — the streaming replacement for
/// `DataFrame::collect()`'s eager whole-result buffering. Pulls ONE batch at a time
/// (never blocking on the full result before the caller can act), checks `cancel`
/// between batches, and spills already-buffered batches to a temp Arrow-IPC file
/// once the running row count crosses `spill_rows` — freeing them from RAM so peak
/// resident memory is bounded by `spill_rows`, not the total result size. Stops
/// early — never over-reading the source — once either `cancel` fires or
/// [`MAX_ROWS`] is reached (the same cap the eager path applies, just enforced
/// during accumulation instead of after). A batch that crosses the cap is sliced to
/// the remaining allowance before it is retained or spilled, so every output mode
/// receives the same bounded, schema-preserving Arrow prefix. Returns every batch
/// actually produced (spilled-then-recovered ++ resident, in original order) plus
/// an outcome summary. Generic over the stream's item type (`Result<RecordBatch,
/// String>`) so this core loop is directly unit-testable against a synthetic
/// `futures_util::stream::iter` fixture, independent of a running DataFusion
/// physical plan.
async fn collect_streaming<S>(
    stream: S,
    cancel: &CancellationToken,
    spill_rows: usize,
) -> Result<(Vec<arrow::record_batch::RecordBatch>, StreamOutcome), String>
where
    S: futures_util::Stream<Item = Result<arrow::record_batch::RecordBatch, String>> + Unpin,
{
    collect_streaming_inner(stream, cancel, spill_rows, None).await
}

async fn collect_streaming_inner<S>(
    mut stream: S,
    cancel: &CancellationToken,
    spill_rows: usize,
    spill_hooks: Option<Arc<SpillIoHooks>>,
) -> Result<(Vec<arrow::record_batch::RecordBatch>, StreamOutcome), String>
where
    S: futures_util::Stream<Item = Result<arrow::record_batch::RecordBatch, String>> + Unpin,
{
    let mut resident: Vec<arrow::record_batch::RecordBatch> = Vec::new();
    let mut resident_rows = 0usize;
    let mut total_rows = 0usize;
    let mut spill: Option<SpillWorker> = None;
    let mut cancelled = false;

    loop {
        let batch = match next_accepted_batch(&mut stream, cancel, &mut cancelled).await {
            Ok(Some(batch)) => batch,
            Ok(None) => break,
            Err(error) => {
                return Err(discard_after_source_error(spill.take(), error).await);
            }
        };
        let reached_limit =
            retain_bounded_batch(batch, &mut resident, &mut resident_rows, &mut total_rows);
        flush_resident_batches(
            &mut spill,
            &mut resident,
            &mut resident_rows,
            spill_rows,
            spill_hooks.as_ref(),
        )
        .await?;
        if reached_limit {
            break; // never over-read a source already past the served cap
        }
    }

    finish_collection(spill, resident, cancelled, total_rows).await
}

async fn next_accepted_batch<S>(
    stream: &mut S,
    cancel: &CancellationToken,
    cancelled: &mut bool,
) -> Result<Option<arrow::record_batch::RecordBatch>, String>
where
    S: futures_util::Stream<Item = Result<arrow::record_batch::RecordBatch, String>> + Unpin,
{
    match cancel.next_or_cancelled(stream).await {
        Ok(Some(_)) if cancel.is_cancelled() => {
            *cancelled = true;
            Ok(None)
        }
        Ok(Some(batch)) => batch.map(Some),
        Ok(None) => Ok(None),
        Err(()) => {
            *cancelled = true;
            Ok(None)
        }
    }
}

async fn discard_after_source_error(spill: Option<SpillWorker>, error: String) -> String {
    match spill {
        Some(spill) => match spill.discard().await {
            Ok(()) => error,
            Err(cleanup_error) => preserve_primary_error(error, Err(cleanup_error)),
        },
        None => error,
    }
}

async fn finish_collection(
    spill: Option<SpillWorker>,
    resident: Vec<arrow::record_batch::RecordBatch>,
    cancelled: bool,
    total_rows: usize,
) -> Result<(Vec<arrow::record_batch::RecordBatch>, StreamOutcome), String> {
    let spilled = spill.is_some();
    let mut out = match spill {
        Some(worker) => worker.finish().await?,
        None => Vec::new(),
    };
    out.extend(resident);

    Ok((
        out,
        StreamOutcome {
            cancelled,
            spilled,
            rows: total_rows,
        },
    ))
}

fn retain_bounded_batch(
    batch: arrow::record_batch::RecordBatch,
    resident: &mut Vec<arrow::record_batch::RecordBatch>,
    resident_rows: &mut usize,
    total_rows: &mut usize,
) -> bool {
    let remaining = MAX_ROWS.saturating_sub(*total_rows);
    if remaining == 0 {
        return true;
    }
    // RecordBatch::slice is zero-copy over the existing Arrow buffers. Apply the
    // cap before spill bookkeeping so every output observes the same prefix.
    let batch = if batch.num_rows() > remaining {
        batch.slice(0, remaining)
    } else {
        batch
    };
    let rows = batch.num_rows();
    *resident_rows += rows;
    *total_rows += rows;
    resident.push(batch);
    *total_rows >= MAX_ROWS
}

async fn flush_resident_batches(
    spill: &mut Option<SpillWorker>,
    resident: &mut Vec<arrow::record_batch::RecordBatch>,
    resident_rows: &mut usize,
    spill_rows: usize,
    hooks: Option<&Arc<SpillIoHooks>>,
) -> Result<(), String> {
    if *resident_rows < spill_rows.max(1) {
        return Ok(());
    }
    if spill.is_none() {
        *spill = Some(match hooks {
            Some(hooks) => {
                SpillWorker::start_with_hooks(resident[0].schema(), Some(Arc::clone(hooks)))
            }
            None => SpillWorker::start(resident[0].schema()),
        });
    }
    spill
        .as_ref()
        .expect("spill worker was initialized above")
        .append(std::mem::take(resident))
        .await?;
    *resident_rows = 0;
    Ok(())
}

/// Execute `df` and collect its result via the streaming/spillable path
/// ([`collect_streaming`]), threading `cancel` through so a request-scoped
/// cancellation (CONCEPT:EG-KG.query.streaming-spillable-collect, L36) actually stops the
/// stream at its next batch boundary — the drop-in replacement for `df.collect()` every
/// internal call site below now uses. For any query under the default spill threshold
/// (the overwhelming common case) an uncancelled run is behaviorally identical to the
/// eager path: same ordered rows and the same `MAX_ROWS` cap — only the ACCUMULATION
/// becomes batch-at-a-time and boundedly resident instead of buffering the whole
/// result up front.
pub(super) async fn collect_default(
    df: datafusion::dataframe::DataFrame,
    cancel: &CancellationToken,
) -> Result<Vec<arrow::record_batch::RecordBatch>, String> {
    use futures_util::StreamExt;
    let stream = df
        .execute_stream()
        .await
        .map_err(|e| format!("execute_stream: {e}"))?;
    let stream = stream.map(|r| r.map_err(|e| format!("stream: {e}")));
    let (batches, _outcome) = collect_streaming(stream, cancel, default_spill_rows()).await?;
    Ok(batches)
}

// ── streaming / spillable / cancellable collect tests (CONCEPT:EG-KG.query.streaming-spillable-collect, EG-P1-4) ──

#[cfg(test)]
mod streaming_tests {
    use super::*;
    use crate::sql::exec::{batches_to_result, batches_to_typed};
    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use futures_util::StreamExt;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::Barrier;

    fn batch(vals: &[i32]) -> arrow::record_batch::RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]));
        let arr = Int32Array::from(vals.to_vec());
        arrow::record_batch::RecordBatch::try_new(schema, vec![Arc::new(arr)]).unwrap()
    }

    fn total_rows(batches: &[arrow::record_batch::RecordBatch]) -> usize {
        batches.iter().map(|b| b.num_rows()).sum()
    }

    fn flatten_i32(batches: &[arrow::record_batch::RecordBatch]) -> Vec<i32> {
        batches
            .iter()
            .flat_map(|b| {
                b.column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .values()
                    .iter()
                    .copied()
            })
            .collect()
    }

    fn assert_one_successful_cleanup(hooks: &SpillIoHooks) {
        let cleanups = hooks.cleanup_results.lock().unwrap();
        assert_eq!(
            cleanups.len(),
            1,
            "one spill file must be cleaned exactly once"
        );
        assert!(
            cleanups[0].1,
            "spill cleanup must remove {}",
            cleanups[0].0.display()
        );
    }

    async fn await_signal(receiver: std::sync::mpsc::Receiver<()>, description: &'static str) {
        let received = tokio::task::spawn_blocking(move || {
            receiver.recv_timeout(std::time::Duration::from_secs(5))
        })
        .await
        .expect("bounded signal waiter must join");
        assert!(received.is_ok(), "timed out waiting for {description}");
    }

    /// The worker blocks on a real barrier while this single-thread executor keeps
    /// scheduling async work. This is deterministic proof that spill file I/O is
    /// owned by the blocking pool rather than the current-thread query executor.
    #[tokio::test(flavor = "current_thread")]
    async fn spill_io_never_blocks_the_current_thread_executor() {
        let release = Arc::new(Barrier::new(2));
        let (entered, entered_receiver) = std::sync::mpsc::sync_channel(1);
        let hooks = Arc::new(SpillIoHooks {
            start_barrier: Some(Arc::clone(&release)),
            worker_entered: Some(entered),
            ..SpillIoHooks::default()
        });
        let worker = SpillWorker::start_with_hooks(batch(&[1]).schema(), Some(Arc::clone(&hooks)));
        let spill_task = tokio::spawn(async move {
            worker.append(vec![batch(&[1, 2, 3])]).await?;
            worker.finish().await
        });

        await_signal(entered_receiver, "spill worker entry").await;
        assert_eq!(hooks.worker_entries.load(Ordering::SeqCst), 1);
        let heartbeat = Arc::new(AtomicUsize::new(0));
        let heartbeat_task = {
            let heartbeat = Arc::clone(&heartbeat);
            tokio::spawn(async move {
                heartbeat.fetch_add(1, Ordering::SeqCst);
            })
        };
        heartbeat_task.await.unwrap();
        assert_eq!(heartbeat.load(Ordering::SeqCst), 1);

        tokio::task::spawn_blocking(move || release.wait())
            .await
            .unwrap();
        let recovered = spill_task.await.unwrap().unwrap();
        assert_eq!(flatten_i32(&recovered), vec![1, 2, 3]);
        assert_eq!(hooks.completed_appends.load(Ordering::SeqCst), 1);
        assert_one_successful_cleanup(&hooks);
    }

    /// A damaged Arrow IPC file remains a typed query error, and the worker still
    /// removes the file after the reader rejects it.
    #[tokio::test(flavor = "current_thread")]
    async fn corrupt_spill_fails_closed_and_is_cleaned() {
        let hooks = Arc::new(SpillIoHooks {
            corrupt_before_read: AtomicBool::new(true),
            ..SpillIoHooks::default()
        });
        let worker = SpillWorker::start_with_hooks(batch(&[1]).schema(), Some(Arc::clone(&hooks)));
        worker.append(vec![batch(&[1, 2])]).await.unwrap();
        let error = worker.finish().await.unwrap_err();
        assert!(
            error.starts_with("spill reader:") || error.starts_with("spill read:"),
            "unexpected corruption error: {error}"
        );
        assert_one_successful_cleanup(&hooks);
    }

    /// Token cancellation after a real spill returns only the accepted prefix and
    /// synchronously joins the worker's successful cleanup before completing.
    #[tokio::test(flavor = "current_thread")]
    async fn cancelled_spill_recovers_prefix_and_cleans_file() {
        let hooks = Arc::new(SpillIoHooks::default());
        let cancel = CancellationToken::new();
        let cancel_trigger = cancel.clone();
        let mut seen = 0usize;
        let stream = futures_util::stream::iter(vec![Ok(batch(&[1, 2])), Ok(batch(&[3, 4]))]).map(
            move |item| {
                seen += 1;
                if seen == 2 {
                    cancel_trigger.cancel();
                }
                item
            },
        );

        let (out, outcome) = collect_streaming_inner(stream, &cancel, 1, Some(Arc::clone(&hooks)))
            .await
            .unwrap();
        assert_eq!(flatten_i32(&out), vec![1, 2]);
        assert_eq!(outcome.rows, 2);
        assert!(outcome.cancelled);
        assert!(outcome.spilled);
        assert_one_successful_cleanup(&hooks);
    }

    /// Dropping an in-flight collector handle closes its command channel. The
    /// detached blocking owner observes that cancellation and removes its file.
    #[tokio::test(flavor = "current_thread")]
    async fn dropped_spill_handle_cleans_file_off_executor() {
        let (cleaned, cleaned_receiver) = std::sync::mpsc::sync_channel(1);
        let hooks = Arc::new(SpillIoHooks {
            cleanup_completed: Some(cleaned),
            ..SpillIoHooks::default()
        });
        let worker = SpillWorker::start_with_hooks(batch(&[1]).schema(), Some(Arc::clone(&hooks)));
        worker.append(vec![batch(&[1, 2])]).await.unwrap();
        drop(worker);

        await_signal(cleaned_receiver, "dropped spill cleanup").await;
        assert_one_successful_cleanup(&hooks);
    }

    /// Cancellation registers the collector's task waker before polling the
    /// source. A permanently pending source therefore cannot trap a cancelled
    /// query waiting for a batch boundary that will never arrive.
    #[tokio::test(flavor = "current_thread")]
    async fn cancellation_wakes_a_stalled_source() {
        let (polled, polled_receiver) = std::sync::mpsc::sync_channel(1);
        let mut polled = Some(polled);
        let stream = futures_util::stream::poll_fn(move |_| {
            if let Some(polled) = polled.take() {
                polled.send(()).unwrap();
            }
            std::task::Poll::Pending
        });
        let cancel = CancellationToken::new();
        let collector_cancel = cancel.clone();
        let collector =
            tokio::spawn(
                async move { collect_streaming(stream, &collector_cancel, MAX_ROWS).await },
            );

        await_signal(polled_receiver, "stalled source poll").await;
        cancel.cancel();
        let (batches, outcome) = collector.await.unwrap().unwrap();
        assert!(batches.is_empty());
        assert!(outcome.cancelled);
        assert_eq!(outcome.rows, 0);
    }

    /// Atomic exclusive creation rejects an occupied path without truncating it;
    /// on Unix, newly created spill files are private regardless of ambient umask.
    #[test]
    fn spill_file_creation_is_exclusive_and_private() {
        let occupied = spill_path();
        let mut occupied_file = open_new_spill_file(&occupied).unwrap();
        std::io::Write::write_all(&mut occupied_file, b"sentinel").unwrap();
        drop(occupied_file);
        let error = open_new_spill_file(&occupied).unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::AlreadyExists);
        assert_eq!(std::fs::read(&occupied).unwrap(), b"sentinel");
        std::fs::remove_file(&occupied).unwrap();

        let private = spill_path();
        let file = open_new_spill_file(&private).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(file.metadata().unwrap().permissions().mode() & 0o777, 0o600);
        }
        drop(file);
        std::fs::remove_file(private).unwrap();
    }

    /// Explicit completion does not hide a failed removal. The query returns the
    /// cleanup error, and the retained path remains observable for recovery.
    #[tokio::test(flavor = "current_thread")]
    async fn explicit_finish_propagates_cleanup_failure() {
        let hooks = Arc::new(SpillIoHooks {
            force_cleanup_error: AtomicBool::new(true),
            ..SpillIoHooks::default()
        });
        let worker = SpillWorker::start_with_hooks(batch(&[1]).schema(), Some(Arc::clone(&hooks)));
        worker.append(vec![batch(&[1, 2])]).await.unwrap();
        let error = worker.finish().await.unwrap_err();
        assert!(error.contains("spill cleanup"), "unexpected error: {error}");

        let cleanups = hooks.cleanup_results.lock().unwrap();
        assert_eq!(cleanups.len(), 1);
        assert!(!cleanups[0].1);
        std::fs::remove_file(&cleanups[0].0).unwrap();
    }

    /// With a spill threshold far above the total row count, `collect_streaming`
    /// pulls every batch, in order, with no spill — the un-cancelled, un-spilled
    /// common case.
    #[tokio::test]
    async fn collect_streaming_pulls_every_batch_in_order_no_spill() {
        let batches = vec![Ok(batch(&[1, 2, 3])), Ok(batch(&[4, 5])), Ok(batch(&[6]))];
        let stream = futures_util::stream::iter(batches);
        let cancel = CancellationToken::new();
        let (out, outcome) = collect_streaming(stream, &cancel, 1_000_000).await.unwrap();
        assert_eq!(flatten_i32(&out), vec![1, 2, 3, 4, 5, 6]);
        assert!(!outcome.cancelled);
        assert!(!outcome.spilled);
        assert_eq!(outcome.rows, 6);
    }

    /// Crossing the spill threshold mid-stream triggers a real spill-to-disk
    /// round-trip, and the recovered result is LOSSLESS and ORDER-PRESERVING —
    /// identical to the no-spill case, just materialized through a temp file.
    #[tokio::test]
    async fn collect_streaming_spills_past_threshold_and_recovers_losslessly() {
        let batches = vec![
            Ok(batch(&[1, 2, 3])),
            Ok(batch(&[4, 5, 6])),
            Ok(batch(&[7, 8, 9])),
        ];
        let stream = futures_util::stream::iter(batches);
        let cancel = CancellationToken::new();
        // Threshold 4: batch 1 (3 rows) stays resident; batch 2 pushes resident to 6
        // rows (>= 4) ⇒ spills batches 1+2; batch 3 (3 more) never re-crosses 4 in
        // this run, so it stays resident and is appended after the recovered spill.
        let (out, outcome) = collect_streaming(stream, &cancel, 4).await.unwrap();
        assert!(
            outcome.spilled,
            "crossing the threshold must trigger a spill"
        );
        assert_eq!(outcome.rows, 9);
        assert_eq!(flatten_i32(&out), vec![1, 2, 3, 4, 5, 6, 7, 8, 9]);
    }

    /// A token cancelled AS A SIDE EFFECT of consuming the stream's 3rd batch is
    /// observed at the next batch boundary — `collect_streaming` stops SHORT of the
    /// full 5-batch source rather than running to completion, and the outcome
    /// reports the cancellation.
    #[tokio::test]
    async fn collect_streaming_stops_early_when_cancelled() {
        let batches = vec![
            Ok(batch(&[1])),
            Ok(batch(&[2])),
            Ok(batch(&[3])),
            Ok(batch(&[4])),
            Ok(batch(&[5])),
        ];
        let cancel = CancellationToken::new();
        let trigger = cancel.clone();
        let mut seen = 0usize;
        let stream = futures_util::stream::iter(batches).map(move |b| {
            seen += 1;
            if seen == 3 {
                trigger.cancel();
            }
            b
        });
        let (out, outcome) = collect_streaming(stream, &cancel, 1_000_000).await.unwrap();
        assert!(outcome.cancelled, "outcome must report the cancellation");
        let rows = total_rows(&out);
        assert!(
            rows < 5,
            "a cancelled collect must stop before draining the whole 5-row stream: got {rows}"
        );
        assert!(rows >= 2, "batches consumed before cancellation still land");
    }

    /// A multi-batch source whose second batch crosses [`MAX_ROWS`] is sliced before
    /// any output adapter sees it. Raw Arrow batches, JSON cells, and MessagePack
    /// rows must expose the same capped, ordered prefix and never an oversized batch.
    #[tokio::test]
    async fn collect_streaming_caps_crossing_batch_for_all_result_modes() {
        let first: Vec<i32> = (0..(MAX_ROWS as i32 - 1)).collect();
        let last = MAX_ROWS as i32 - 1;
        let stream =
            futures_util::stream::iter(vec![Ok(batch(&first)), Ok(batch(&[last, last + 1]))]);
        let cancel = CancellationToken::new();
        let (capped, outcome) = collect_streaming(stream, &cancel, MAX_ROWS).await.unwrap();

        assert_eq!(outcome.rows, MAX_ROWS);
        assert!(!outcome.cancelled);
        assert!(
            outcome.spilled,
            "the exact-cap boundary also exercises spill"
        );
        assert_eq!(total_rows(&capped), MAX_ROWS);
        assert_eq!(capped.len(), 2, "the crossing source remains multi-batch");
        assert_eq!(capped[0].num_rows(), MAX_ROWS - 1);
        assert_eq!(capped[1].num_rows(), 1);
        assert!(capped.iter().all(|b| b.num_rows() <= MAX_ROWS));
        let flattened = flatten_i32(&capped);
        assert_eq!(flattened.first(), Some(&0));
        assert_eq!(flattened.last(), Some(&last));

        let typed = batches_to_typed(&capped).unwrap();
        let msgpack = batches_to_result(&capped).unwrap();
        assert_eq!(typed.rows.len(), MAX_ROWS);
        assert_eq!(msgpack.rows.len(), MAX_ROWS);
        assert_eq!(typed.rows[MAX_ROWS - 1][0], serde_json::json!(last));
        let msgpack_last: Vec<serde_json::Value> =
            rmp_serde::from_slice(&msgpack.rows[MAX_ROWS - 1]).unwrap();
        assert_eq!(msgpack_last, vec![serde_json::json!(last)]);
    }

    /// The cap is terminal: a source error queued after the crossing batch must
    /// not be pulled, and no oversized batch may reach any result adapter. The
    /// same collector also stops on cancellation before processing a batch, while
    /// an error before the cap remains an execution error rather than being
    /// swallowed by the bounded collector.
    #[tokio::test]
    async fn collect_streaming_cap_cancellation_and_error_boundaries() {
        let first: Vec<i32> = (0..(MAX_ROWS as i32 - 1)).collect();
        let last = MAX_ROWS as i32 - 1;
        let seen = Arc::new(AtomicUsize::new(0));
        let seen_source = Arc::clone(&seen);
        let cancel = CancellationToken::new();
        let stream = futures_util::stream::iter(vec![
            Ok(batch(&first)),
            Ok(batch(&[last, last + 1])),
            Err("source error after MAX_ROWS".to_string()),
        ])
        .map(move |item| {
            seen_source.fetch_add(1, Ordering::SeqCst);
            item
        });

        let (capped, outcome) = collect_streaming(stream, &cancel, MAX_ROWS)
            .await
            .expect("the post-cap source error must not be over-read");
        assert_eq!(
            seen.load(Ordering::SeqCst),
            2,
            "the error item was not pulled"
        );
        assert_eq!(outcome.rows, MAX_ROWS);
        assert!(!outcome.cancelled);
        assert_eq!(total_rows(&capped), MAX_ROWS);
        assert!(capped.iter().all(|batch| batch.num_rows() <= MAX_ROWS));
        assert_eq!(flatten_i32(&capped).last(), Some(&last));

        // Cancellation is checked on the same batch-boundary loop. It must stop
        // before processing the first item and before pulling the queued error.
        let cancelled_seen = Arc::new(AtomicUsize::new(0));
        let cancelled_seen_source = Arc::clone(&cancelled_seen);
        let cancelled = CancellationToken::new();
        let cancel_trigger = cancelled.clone();
        let cancelled_stream = futures_util::stream::iter(vec![
            Ok(batch(&[1])),
            Err("source error after cancellation".to_string()),
        ])
        .map(move |item| {
            cancelled_seen_source.fetch_add(1, Ordering::SeqCst);
            cancel_trigger.cancel();
            item
        });
        let (stopped, cancelled_outcome) =
            collect_streaming(cancelled_stream, &cancelled, MAX_ROWS)
                .await
                .expect("cancellation is a successful early stop");
        assert!(cancelled.is_cancelled());
        assert!(stopped.is_empty());
        assert_eq!(cancelled_outcome.rows, 0);
        assert!(cancelled_outcome.cancelled);
        assert_eq!(
            cancelled_seen.load(Ordering::SeqCst),
            1,
            "cancellation must prevent the queued error from being pulled"
        );

        let error_stream = futures_util::stream::iter(vec![
            Ok(batch(&[1])),
            Err("source error before MAX_ROWS".to_string()),
        ]);
        let error = collect_streaming(error_stream, &CancellationToken::new(), MAX_ROWS).await;
        assert!(matches!(error, Err(message) if message == "source error before MAX_ROWS"));
    }

    /// Empty streams retain the existing zero-row contract across all three result
    /// representations; no fabricated schema or batch is introduced by the cap.
    #[tokio::test]
    async fn collect_streaming_empty_result_stays_empty_across_result_modes() {
        let stream = futures_util::stream::iter(Vec::<
            Result<arrow::record_batch::RecordBatch, String>,
        >::new());
        let cancel = CancellationToken::new();
        let (batches, outcome) = collect_streaming(stream, &cancel, 1_000_000).await.unwrap();

        assert!(batches.is_empty());
        assert_eq!(outcome.rows, 0);
        assert!(!outcome.cancelled);
        assert!(batches_to_typed(&batches).unwrap().rows.is_empty());
        assert!(batches_to_result(&batches).unwrap().rows.is_empty());
    }

    #[test]
    fn default_spill_rows_sits_above_max_rows_so_ordinary_queries_never_spill() {
        assert!(default_spill_rows() > MAX_ROWS);
    }

    #[test]
    fn cancellation_token_clone_shares_the_flag() {
        let tok = CancellationToken::new();
        assert!(!tok.is_cancelled());
        let clone = tok.clone();
        clone.cancel();
        assert!(
            tok.is_cancelled(),
            "cancel on a clone must be observed on every other clone (shared flag)"
        );
    }
}
