//! Observability log ingestion front door (CONCEPT:AU-KG.ingest.self-ingest) — the first slice of
//! Phase T ("surpass OpenObserve").
//!
//! ## Why this exists
//!
//! The engine already IS OpenObserve's stack: Rust + redb + Arrow/DataFusion + a
//! Tantivy full-text index (`eg-text`) + a content-addressed blob store on S3
//! (`blob-s3`). OpenObserve (O2) is: ingest logs/metrics/traces over standard
//! wire protocols → land them as time-series + full-text → roll them into
//! Parquet-on-object-store → search with SQL. This module lands the **log
//! ingestion front door**; [`segment`] lands the **Parquet segment substrate**
//! (CONCEPT:EG-KG.retrieval.observability-search). Search / PromQL / dashboards are later Phase T items.
//!
//! ## The listener
//!
//! A HAND-ROLLED HTTP/1.1 listener over `tokio::net::TcpListener` — the SAME
//! dependency-free idiom as [`crate::metrics::serve`] and
//! [`crate::server::sparql_http`] (NO axum/hyper/warp, so the Pi contract holds).
//! Bound via `EPISTEMIC_GRAPH_OBS_ADDR`. It accepts log records over three
//! interchange shapes so EXISTING agents/collectors point at it unchanged:
//!
//!  * `POST /v1/logs`            — OTLP/HTTP JSON (`ExportLogsServiceRequest`)
//!  * `POST /_bulk`              — Elasticsearch `_bulk` NDJSON (action/doc pairs)
//!  * `POST /<stream>/_doc`      — Elasticsearch single-doc index
//!  * `POST /` or `/api/logs`    — plain JSON-lines (one JSON object per line)
//!
//! Every shape is normalized into a common [`LogRecord`] and stored into (a) an
//! `eg-tsdb` series keyed by stream (time-range + retention) and (b) a per-stream
//! `eg-text` Tantivy index (full-text search) — schema-on-read: attributes are a
//! dynamic map, never a fixed column. Multi-tenant by **stream**: an org/stream
//! name gets its own tsdb series id + its own text-index namespace.

pub mod search;
pub mod segment;

/// CONCEPT:EG-OS.observability.prometheus-ingest — the observability EGRESS half: OTLP/HTTP-JSON export of the engine's
/// OWN Prometheus metrics + its stored distributed-trace spans to an external
/// OpenTelemetry collector (closing the loop the engine already ingests). Behind the
/// `otel-export` feature.
#[cfg(feature = "otel-export")]
pub mod otel_export;

/// CONCEPT:EG-OS.observability.prometheus-ingest — the Prometheus `remote_write` receiver: land snappy-compressed
/// protobuf `WriteRequest` POSTs (`/api/v1/write`) into the durable eg-tsdb SeriesStore
/// so an external Prometheus can PUSH to the engine. Behind the `otel-export` feature.
#[cfg(feature = "otel-export")]
pub mod remote_write;

mod http;
mod manifests;
mod parse;
mod snapshot;
mod state;
pub(crate) mod writer;

use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use parking_lot::Mutex;

use crate::server::blob::store::ChunkStore;
use eg_text::TextIndex;
use eg_tsdb::store::SeriesStore;

pub use http::{serve, serve_with_security};
pub use parse::{parse_es_bulk, parse_json_lines, parse_otlp_logs};
pub use search::{LogQuery, DEFAULT_SEARCH_SIZE};
pub use segment::SegmentManifest;

use manifests::SegmentManifestUsage;

/// Env var carrying the observability-ingest listener bind address (`host:port`),
/// e.g. `127.0.0.1:5080` (O2's default log-ingest port). Unset ⇒ no listener.
pub const OBS_ADDR_ENV: &str = "EPISTEMIC_GRAPH_OBS_ADDR";
/// Env var overriding the per-stream buffered-record count that triggers a Parquet
/// segment flush (CONCEPT:EG-KG.retrieval.observability-search). Default [`DEFAULT_FLUSH_RECORDS`].
pub const OBS_FLUSH_RECORDS_ENV: &str = "EPISTEMIC_GRAPH_OBS_FLUSH_RECORDS";

/// Default flush window: buffer this many records per stream, then roll a segment.
pub const DEFAULT_FLUSH_RECORDS: usize = 1024;

/// One normalized log record — the common shape every wire format is parsed into.
/// `attrs` is a dynamic string map (schema-on-read); the fixed fields are the ones
/// every observability format carries.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LogRecord {
    /// Timestamp, epoch nanoseconds (the tsdb series key).
    pub ts: i64,
    /// Org/stream namespace (the tenancy key → its own series + text namespace).
    pub stream: String,
    /// Severity text (e.g. `INFO`/`WARN`/`ERROR`); empty if the source omitted it.
    pub severity: String,
    /// The log message body.
    pub body: String,
    /// Dynamic attributes (schema-on-read), sorted for a stable serialization.
    pub attrs: BTreeMap<String, String>,
}

/// The ingest counts an ingest call landed, shaped into the wire response.
#[derive(Clone, Copy, Debug, Default)]
pub struct IngestOutcome {
    pub accepted: usize,
    pub segments_flushed: usize,
}

/// Wall-clock now in epoch nanoseconds.
fn now_ns() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as i64)
        .unwrap_or(0)
}

/// Map a severity text onto a numeric level (OTLP-ish) for the tsdb series value, so
/// the series is a meaningful "severity over time" signal, not just a count.
fn severity_number(sev: &str) -> f64 {
    match sev.trim().to_ascii_uppercase().as_str() {
        "TRACE" => 1.0,
        "DEBUG" => 5.0,
        "INFO" | "INFORMATION" => 9.0,
        "WARN" | "WARNING" => 13.0,
        "ERROR" | "ERR" => 17.0,
        "FATAL" | "CRITICAL" | "CRIT" => 21.0,
        _ => 0.0,
    }
}

/// Collision-resistant, opaque filesystem component for one exact stream name.
/// Domain separation prevents an index directory and manifest file from sharing
/// an identity scheme with unrelated persistent data.
fn stream_storage_key(stream: &str) -> String {
    use sha2::{Digest as _, Sha256};

    let mut digest = Sha256::new();
    digest.update(b"epistemic-graph/observability-stream\0");
    digest.update(stream.as_bytes());
    hex::encode(digest.finalize())
}

/// The self-contained ingest state: a tsdb series store + per-stream text indices +
/// the selected blob CAS for Parquet segments + per-stream flush buffers + the
/// recorded segment manifests. Production startup injects the graph server's
/// selected CAS; the private Redb fallback remains for direct test/embedded
/// construction.
pub struct ObsState {
    /// Time-series store (series id = `obs:logs:<stream>`).
    series: Arc<SeriesStore>,
    /// Blob CAS the Parquet segments land in, injected from server composition
    /// (Redb or an explicitly built `blob-s3` backend).
    blob: Arc<dyn ChunkStore>,
    /// Per-stream Tantivy full-text indices, created lazily.
    indices: Mutex<HashMap<String, TextIndex>>,
    /// Per-stream buffers of records awaiting a Parquet flush.
    buffers: Mutex<HashMap<String, Vec<LogRecord>>>,
    /// Recorded segment manifests (the prune index), newest last.
    segments: Mutex<Vec<SegmentManifest>>,
    /// Positions into `segments`, grouped by stream, so one persistence flush
    /// visits only its tenant's manifests while search keeps its vector contract.
    manifest_positions: Mutex<HashMap<String, Vec<usize>>>,
    /// Per-stream and aggregate bounds for the manifest files that have completed
    /// durable publication. Updated before the global publication lock is released.
    manifest_usage: Mutex<SegmentManifestUsage>,
    /// Base dir for persistent text indices; `None` ⇒ in-memory indices (tests).
    text_dir: Option<std::path::PathBuf>,
    /// `{persist_dir}/obs` -- the base this module's own durable side-files
    /// (`traces.msgpack` for BUG-016, `segments/<stream>.msgpack` for BUG-210)
    /// live under. `None` ⇒ ephemeral/test instance (mirrors `text_dir`'s "None ⇒
    /// in-memory" contract): nothing beyond the tsdb series + blob CAS the
    /// `None`-branch of `open()` already gives a temp dir is durable in that mode.
    obs_base: Option<PathBuf>,
    /// Roll a segment once a stream buffers this many records.
    flush_threshold: usize,
    /// Monotonic doc-id sequence for text-index document ids.
    next_doc: AtomicU64,
    /// CONCEPT:EG-OS.observability.trace-assembly — the distributed-trace span store (in-memory; a durable tier
    /// mirroring logs is a follow-up). Present only under the `traces` sub-feature.
    #[cfg(feature = "traces")]
    traces: Arc<eg_tsdb::traces::SpanStore>,
}

#[cfg(test)]
mod tests {
    use super::http::{es_doc_stream, handle, MAX_HTTP_BODY_BYTES};
    use super::manifests::{
        include_manifest_budget, load_segment_manifests, segment_manifest_path,
        MANIFEST_BOUNDS_ERROR, MANIFEST_DIRECTORY_ERROR, MANIFEST_READ_ERROR, MAX_MANIFEST_FILES,
        MAX_MANIFEST_TOTAL_BYTES, MAX_MANIFEST_TOTAL_ITEMS,
    };
    #[cfg(feature = "traces")]
    use super::snapshot::{load_traces_snapshot, traces_snapshot_path};
    use super::snapshot::{
        read_snapshot_blocking, snapshot_write_lock, write_snapshot_atomically_blocking,
        write_snapshot_atomically_blocking_with, MAX_SNAPSHOT_BYTES, SNAPSHOT_READ_ERROR,
        SNAPSHOT_WRITE_ERROR,
    };
    use super::*;
    use crate::lock_recovery::LockRecovery;
    use crate::server::blob::store::RedbChunkStore;
    use crate::server::http1::HttpMessage;
    use crate::test_rendezvous::{join_bounded, recv_within};
    use std::path::Path;
    use std::sync::atomic::Ordering;
    use tokio::io::AsyncReadExt as _;
    use tokio::io::AsyncWriteExt as _;

    fn snapshot_temporaries(parent: &Path) -> Vec<PathBuf> {
        std::fs::read_dir(parent)
            .expect("read snapshot parent")
            .map(|entry| entry.expect("read snapshot entry").path())
            .filter(|path| {
                path.file_name()
                    .and_then(|name| name.to_str())
                    .is_some_and(|name| name.ends_with(".msgpack.tmp"))
            })
            .collect()
    }

    /// A temporary root that satisfies the snapshot authority's private-directory
    /// contract regardless of the process umask.
    fn private_tempdir() -> tempfile::TempDir {
        let directory = tempfile::tempdir().expect("temp dir");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            std::fs::set_permissions(directory.path(), std::fs::Permissions::from_mode(0o700))
                .expect("make temp directory private");
        }
        directory
    }

    /// Create every missing fixture-directory component with private permissions.
    fn create_private_test_directory(path: &Path) {
        #[cfg(unix)]
        {
            use std::os::unix::fs::DirBuilderExt as _;
            let mut builder = std::fs::DirBuilder::new();
            builder.recursive(true).mode(0o700);
            builder.create(path).expect("create private test directory");
        }
        #[cfg(not(unix))]
        std::fs::create_dir_all(path).expect("create private test directory");
    }

    #[cfg(feature = "traces")]
    #[tokio::test(flavor = "current_thread")]
    async fn public_trace_persistence_yields_the_current_thread_reactor() {
        let dir = private_tempdir();
        let persist_dir = dir.path().to_str().expect("utf8 temp path");
        let obs = ObsState::open(Some(persist_dir), 1024).await.expect("open");
        let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = std::sync::mpsc::sync_channel(1);
        let counter = Arc::new(AtomicU64::new(0));
        let observed = counter.clone();

        let lock_holder = std::thread::spawn(move || {
            let _guard = snapshot_write_lock().lock_recovering("obs snapshot write lock");
            entered_tx.send(()).expect("announce held lock");
            recv_within(&release_rx, "the test releasing the held snapshot lock");
        });
        entered_rx
            .await
            .expect("lock holder reached deterministic barrier");
        let progress = async move {
            tokio::task::yield_now().await;
            observed.fetch_add(1, Ordering::SeqCst);
            release_tx.send(()).expect("release persistence lock");
        };
        let (persisted, ()) = tokio::join!(obs.persist_traces(), progress);
        persisted.expect("public trace persistence");
        join_bounded(lock_holder, "the snapshot-lock holder thread");
        assert_eq!(
            counter.load(Ordering::SeqCst),
            1,
            "the current-thread reactor must progress while persistence is blocked"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn cancelling_waiter_does_not_cancel_started_atomic_publication() {
        let dir = private_tempdir();
        let path = dir.path().join("snapshot.msgpack");
        let written_path = path.clone();
        let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = std::sync::mpsc::sync_channel(1);
        let (finished_tx, finished_rx) = tokio::sync::oneshot::channel();

        let waiter = tokio::spawn(async move {
            ::tokio::task::spawn_blocking(move || {
                entered_tx
                    .send(())
                    .map_err(|_| SNAPSHOT_WRITE_ERROR.to_string())?;
                release_rx
                    .recv_timeout(crate::test_rendezvous::RENDEZVOUS_TIMEOUT)
                    .map_err(|_| SNAPSHOT_WRITE_ERROR.to_string())?;
                let result = write_snapshot_atomically_blocking(written_path, || {
                    Ok(b"committed-after-cancellation".to_vec())
                });
                if finished_tx.send(result.clone()).is_err() {
                    return Err(SNAPSHOT_WRITE_ERROR.to_string());
                }
                result
            })
            .await
            .map_err(|_| "observability persistence worker failed".to_string())?
        });
        entered_rx
            .await
            .expect("worker reached deterministic barrier");
        waiter.abort();
        release_tx.send(()).expect("release persistence worker");
        finished_rx
            .await
            .expect("started worker must finish")
            .expect("atomic publication");

        assert_eq!(
            std::fs::read(&path).expect("published snapshot"),
            b"committed-after-cancellation"
        );
        assert!(snapshot_temporaries(dir.path()).is_empty());
    }

    #[test]
    fn failed_atomic_publication_preserves_authority_and_cleans_temporary() {
        let dir = private_tempdir();
        let path = dir.path().join("snapshot.msgpack");
        write_snapshot_atomically_blocking(path.clone(), || Ok(b"prior-authority".to_vec()))
            .expect("seed private authority");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o755))
                .expect("make the boundary shared read-only");
        }

        let error = write_snapshot_atomically_blocking_with(
            path.clone(),
            || Ok(b"unpublished".to_vec()),
            || Err("injected failure detail".to_string()),
            || {},
        )
        .expect_err("injected failure must abort publication");

        assert_eq!(error, SNAPSHOT_WRITE_ERROR);
        assert_eq!(
            std::fs::read(&path).expect("read prior authority"),
            b"prior-authority"
        );
        assert!(snapshot_temporaries(dir.path()).is_empty());
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            let mode = std::fs::metadata(dir.path())
                .expect("directory metadata")
                .permissions()
                .mode();
            assert_eq!(
                mode & 0o022,
                0,
                "publication must require its trust boundary"
            );
        }
    }

    #[test]
    fn panicked_snapshot_attempt_does_not_poison_later_publication() {
        let dir = private_tempdir();
        let path = dir.path().join("snapshot.msgpack");
        let failed_path = path.clone();

        let panic = std::panic::catch_unwind(|| {
            let _result = write_snapshot_atomically_blocking_with(
                failed_path,
                || Ok(b"abandoned".to_vec()),
                || panic!("injected publication panic"),
                || {},
            );
        });
        assert!(panic.is_err(), "the injected panic must escape its attempt");

        write_snapshot_atomically_blocking(path.clone(), || Ok(b"recovered".to_vec()))
            .expect("later publication recovers the poisoned lock");
        assert_eq!(std::fs::read(path).expect("read authority"), b"recovered");
        assert!(snapshot_temporaries(dir.path()).is_empty());
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn parent_replacement_cannot_redirect_an_opened_publication() {
        use std::os::unix::fs::symlink;

        let dir = private_tempdir();
        let parent = dir.path().join("authority");
        let moved_parent = dir.path().join("moved-authority");
        let outside = dir.path().join("outside");
        create_private_test_directory(&parent);
        create_private_test_directory(&outside);
        let path = parent.join("snapshot.msgpack");

        let error = write_snapshot_atomically_blocking_with(
            path,
            || Ok(b"opened-authority".to_vec()),
            || {
                std::fs::rename(&parent, &moved_parent)
                    .map_err(|_| SNAPSHOT_WRITE_ERROR.to_string())?;
                symlink(&outside, &parent).map_err(|_| SNAPSHOT_WRITE_ERROR.to_string())
            },
            || {},
        )
        .expect_err("renamed authority must not be acknowledged");

        assert_eq!(error, SNAPSHOT_WRITE_ERROR);
        assert!(!outside.join("snapshot.msgpack").exists());
        assert_eq!(
            std::fs::read(moved_parent.join("snapshot.msgpack")).expect("fd-bound publication"),
            b"opened-authority"
        );
    }

    #[test]
    fn oversized_and_nonregular_snapshots_fail_closed() {
        let dir = private_tempdir();
        let oversized = dir.path().join("oversized.msgpack");
        std::fs::File::create(&oversized)
            .and_then(|file| file.set_len(MAX_SNAPSHOT_BYTES as u64 + 1))
            .expect("create sparse oversized snapshot");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            std::fs::set_permissions(&oversized, std::fs::Permissions::from_mode(0o600))
                .expect("make oversized authority private");
        }
        assert_eq!(
            read_snapshot_blocking(&oversized).expect_err("oversized snapshot must fail"),
            SNAPSHOT_READ_ERROR
        );

        let nonregular = dir.path().join("directory.msgpack");
        std::fs::create_dir(&nonregular).expect("create nonregular snapshot authority");
        assert_eq!(
            read_snapshot_blocking(&nonregular).expect_err("directory read must fail"),
            SNAPSHOT_READ_ERROR
        );
        assert_eq!(
            write_snapshot_atomically_blocking(nonregular, || Ok(Vec::new()))
                .expect_err("directory publication must fail"),
            SNAPSHOT_WRITE_ERROR
        );

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;

            let writable = dir.path().join("writable.msgpack");
            std::fs::write(&writable, b"untrusted").expect("write broad authority");
            std::fs::set_permissions(&writable, std::fs::Permissions::from_mode(0o666))
                .expect("make authority broadly writable");
            assert_eq!(
                read_snapshot_blocking(&writable).expect_err("broad read authority must fail"),
                SNAPSHOT_READ_ERROR
            );
            assert_eq!(
                write_snapshot_atomically_blocking(writable, || Ok(b"replacement".to_vec()))
                    .expect_err("broad destination authority must fail"),
                SNAPSHOT_WRITE_ERROR
            );

            let private = dir.path().join("private.msgpack");
            write_snapshot_atomically_blocking(private.clone(), || Ok(b"private".to_vec()))
                .expect("publish private snapshot");
            let mode = std::fs::metadata(private)
                .expect("private snapshot metadata")
                .permissions()
                .mode();
            assert_eq!(mode & 0o077, 0, "new snapshots must be mode 0600");
        }
    }

    #[test]
    fn aggregate_manifest_recovery_budget_is_bounded() {
        let mut total = MAX_MANIFEST_FILES;
        assert_eq!(
            include_manifest_budget(&mut total, 1, MAX_MANIFEST_FILES).expect_err("file overflow"),
            MANIFEST_BOUNDS_ERROR
        );

        let mut total = 0usize;
        include_manifest_budget(
            &mut total,
            MAX_MANIFEST_TOTAL_BYTES,
            MAX_MANIFEST_TOTAL_BYTES,
        )
        .expect("exact byte bound");
        assert_eq!(
            include_manifest_budget(&mut total, 1, MAX_MANIFEST_TOTAL_BYTES)
                .expect_err("byte overflow"),
            MANIFEST_BOUNDS_ERROR
        );

        let mut total = 0usize;
        include_manifest_budget(
            &mut total,
            MAX_MANIFEST_TOTAL_ITEMS,
            MAX_MANIFEST_TOTAL_ITEMS,
        )
        .expect("exact item bound");
        assert_eq!(
            include_manifest_budget(&mut total, 1, MAX_MANIFEST_TOTAL_ITEMS)
                .expect_err("item overflow"),
            MANIFEST_BOUNDS_ERROR
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn public_open_rejects_a_nonregular_manifest_authority() {
        let dir = private_tempdir();
        let manifests = dir.path().join("obs/segments");
        create_private_test_directory(&manifests);
        std::fs::create_dir(manifests.join("nonregular.msgpack"))
            .expect("create nonregular manifest");
        let persist_dir = dir.path().to_str().expect("utf8 temp path");

        let error = match ObsState::open(Some(persist_dir), 1).await {
            Err(error) => error,
            Ok(_) => panic!("nonregular manifest authority must fail"),
        };
        assert_eq!(error, MANIFEST_READ_ERROR);
    }

    #[cfg(feature = "traces")]
    #[tokio::test(flavor = "current_thread")]
    async fn public_open_rejects_an_oversized_trace_snapshot() {
        let dir = private_tempdir();
        let obs_base = dir.path().join("obs");
        create_private_test_directory(&obs_base);
        std::fs::File::create(traces_snapshot_path(&obs_base))
            .and_then(|file| file.set_len(MAX_SNAPSHOT_BYTES as u64 + 1))
            .expect("create sparse oversized trace snapshot");
        let persist_dir = dir.path().to_str().expect("utf8 temp path");

        let error = match ObsState::open(Some(persist_dir), 1).await {
            Err(error) => error,
            Ok(_) => panic!("oversized trace snapshot must fail"),
        };
        assert_eq!(error, SNAPSHOT_READ_ERROR);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn colliding_stream_names_publish_concurrently_without_aliasing() {
        fn record(stream: &str, ts: i64) -> LogRecord {
            LogRecord {
                ts,
                stream: stream.to_string(),
                severity: "INFO".to_string(),
                body: format!("body-{stream}"),
                attrs: BTreeMap::new(),
            }
        }

        let dir = private_tempdir();
        let persist_dir = dir.path().to_str().expect("utf8 temp path");
        let obs = Arc::new(ObsState::open(Some(persist_dir), 1).await.expect("open"));
        let first = obs.clone();
        let second = obs.clone();
        let third = obs.clone();
        let fourth = obs.clone();
        let (first_result, second_result, third_result, fourth_result) = tokio::join!(
            ::tokio::task::spawn_blocking(move || first.ingest(vec![record("tenant/a", 1)])),
            ::tokio::task::spawn_blocking(move || second.ingest(vec![record("tenant_a", 2)])),
            ::tokio::task::spawn_blocking(move || third.ingest(vec![record("same", 3)])),
            ::tokio::task::spawn_blocking(move || fourth.ingest(vec![record("same", 4)])),
        );
        first_result
            .expect("join first ingest")
            .expect("first ingest");
        second_result
            .expect("join second ingest")
            .expect("second ingest");
        third_result
            .expect("join third ingest")
            .expect("third ingest");
        fourth_result
            .expect("join fourth ingest")
            .expect("fourth ingest");
        assert_ne!(
            segment_manifest_path(&dir.path().join("obs"), "tenant/a"),
            segment_manifest_path(&dir.path().join("obs"), "tenant_a")
        );
        drop(obs);

        let reopened = ObsState::open(Some(persist_dir), 1).await.expect("reopen");
        assert_eq!(reopened.segments_for("tenant/a").len(), 1);
        assert_eq!(reopened.segments_for("tenant_a").len(), 1);
        assert_eq!(
            reopened.segments_for("same").len(),
            2,
            "the last same-stream publication must include both concurrent segments"
        );
    }

    #[test]
    fn missing_manifest_directory_is_an_empty_fresh_authority() {
        let dir = private_tempdir();
        assert!(load_segment_manifests(dir.path())
            .expect("missing manifest directory")
            .manifests
            .is_empty());
    }

    #[test]
    fn corrupt_manifest_fails_closed_without_path_disclosure() {
        let dir = private_tempdir();
        let manifests = dir.path().join("segments");
        create_private_test_directory(&manifests);
        let path = manifests.join("private-stream.msgpack");
        write_snapshot_atomically_blocking(path.clone(), || Ok(b"not-messagepack".to_vec()))
            .expect("write private corrupt manifest");

        let error = load_segment_manifests(dir.path()).expect_err("corruption must fail");
        assert_eq!(error, "segment manifest is invalid or exceeds its bounds");
        assert!(!error.contains("private-stream"));
        assert!(!error.contains(&dir.path().display().to_string()));
    }

    #[cfg(feature = "traces")]
    #[test]
    fn corrupt_trace_snapshot_fails_closed_without_path_disclosure() {
        let dir = private_tempdir();
        let path = traces_snapshot_path(dir.path());
        write_snapshot_atomically_blocking(path.clone(), || Ok(b"not-messagepack".to_vec()))
            .expect("write private corrupt trace snapshot");

        let error = match load_traces_snapshot(dir.path()) {
            Err(error) => error,
            Ok(_) => panic!("corruption must fail"),
        };
        assert_eq!(error, "trace snapshot is invalid or exceeds its bounds");
        assert!(!error.contains(&path.display().to_string()));
        assert!(!error.contains(&dir.path().display().to_string()));
    }

    #[cfg(unix)]
    #[test]
    fn symlinked_snapshot_and_manifest_authorities_fail_closed() {
        use std::os::unix::fs::symlink;

        let dir = private_tempdir();
        let outside = dir.path().join("outside");
        create_private_test_directory(&outside);
        let outside_file = outside.join("authority.msgpack");
        std::fs::write(&outside_file, b"outside-authority").expect("seed outside");

        let destination = dir.path().join("snapshot.msgpack");
        symlink(&outside_file, &destination).expect("symlink destination");
        let read_error =
            read_snapshot_blocking(&destination).expect_err("symlinked read authority must fail");
        assert_eq!(read_error, SNAPSHOT_READ_ERROR);
        assert!(!read_error.contains(&destination.display().to_string()));
        let write_error = write_snapshot_atomically_blocking(destination, || {
            Ok(b"must-not-follow-symlink".to_vec())
        })
        .expect_err("symlinked write authority must fail");
        assert_eq!(write_error, SNAPSHOT_WRITE_ERROR);
        assert_eq!(
            std::fs::read(&outside_file).expect("outside authority unchanged"),
            b"outside-authority"
        );

        let obs_base = dir.path().join("obs");
        create_private_test_directory(&obs_base);
        symlink(&outside, obs_base.join("segments")).expect("symlink manifest directory");
        assert_eq!(
            load_segment_manifests(&obs_base).expect_err("symlinked directory must fail"),
            "segment manifest directory is unavailable"
        );

        let linked_base = dir.path().join("linked-obs");
        symlink(&outside, &linked_base).expect("symlink manifest ancestor");
        create_private_test_directory(&outside.join("segments"));
        assert_eq!(
            load_segment_manifests(&linked_base).expect_err("symlinked ancestor must fail"),
            "segment manifest directory is unavailable"
        );

        let dangling_base = dir.path().join("dangling-obs");
        symlink(dir.path().join("missing-target"), &dangling_base)
            .expect("create dangling ancestor");
        assert_eq!(
            load_segment_manifests(&dangling_base).expect_err("dangling ancestor must fail"),
            MANIFEST_DIRECTORY_ERROR
        );
        assert_eq!(
            read_snapshot_blocking(&dangling_base.join("traces.msgpack"))
                .expect_err("dangling read ancestor must fail"),
            SNAPSHOT_READ_ERROR
        );
    }

    #[test]
    fn otlp_parses_and_derives_stream_from_service_name() {
        let body = r#"{
          "resourceLogs":[{
            "resource":{"attributes":[{"key":"service.name","value":{"stringValue":"checkout"}}]},
            "scopeLogs":[{"logRecords":[
              {"timeUnixNano":"1700000000000000000","severityText":"ERROR",
               "body":{"stringValue":"payment declined"},
               "attributes":[{"key":"order","value":{"stringValue":"o-42"}}]},
              {"timeUnixNano":"1700000000000000001","severityText":"INFO",
               "body":{"stringValue":"cart viewed"}}
            ]}]
          }]
        }"#;
        let recs = parse_otlp_logs(body, "default").unwrap();
        assert_eq!(recs.len(), 2);
        assert_eq!(recs[0].stream, "checkout");
        assert_eq!(recs[0].severity, "ERROR");
        assert_eq!(recs[0].body, "payment declined");
        assert_eq!(recs[0].ts, 1_700_000_000_000_000_000);
        assert_eq!(recs[0].attrs.get("order").unwrap(), "o-42");
    }

    #[test]
    fn es_bulk_parses_action_doc_pairs() {
        let body = "{\"index\":{\"_index\":\"weblogs\"}}\n\
                    {\"@timestamp\":1700,\"level\":\"WARN\",\"message\":\"slow\"}\n\
                    {\"create\":{\"_index\":\"weblogs\"}}\n\
                    {\"message\":\"ok\",\"level\":\"INFO\"}\n\
                    {\"delete\":{\"_index\":\"weblogs\",\"_id\":\"x\"}}\n";
        let recs = parse_es_bulk(body, "default");
        assert_eq!(recs.len(), 2, "delete carries no source line");
        assert_eq!(recs[0].stream, "weblogs");
        assert_eq!(recs[0].severity, "WARN");
        assert_eq!(recs[0].body, "slow");
        assert_eq!(recs[0].ts, 1700 * 1_000_000, "epoch-ms → ns");
        assert_eq!(recs[1].body, "ok");
    }

    #[test]
    fn json_lines_parse_with_default_and_explicit_stream() {
        let body = "{\"message\":\"a\",\"level\":\"INFO\"}\n\
                    {\"message\":\"b\",\"stream\":\"other\",\"level\":\"ERROR\"}\n";
        let recs = parse_json_lines(body, "svc");
        assert_eq!(recs.len(), 2);
        assert_eq!(recs[0].stream, "svc");
        assert_eq!(recs[1].stream, "other");
    }

    #[test]
    fn es_doc_path_extracts_stream() {
        assert_eq!(es_doc_stream("/logs/_doc").as_deref(), Some("logs"));
        assert_eq!(es_doc_stream("/logs/_doc/abc").as_deref(), Some("logs"));
        assert_eq!(es_doc_stream("/_bulk"), None);
        assert_eq!(es_doc_stream("/v1/logs"), None);
    }

    #[test]
    fn ingest_lands_in_tsdb_series_and_text_index() {
        let obs = ObsState::in_memory(1024).unwrap();
        let recs = vec![
            LogRecord {
                ts: 1000,
                stream: "app".into(),
                severity: "ERROR".into(),
                body: "database connection refused".into(),
                attrs: BTreeMap::new(),
            },
            LogRecord {
                ts: 2000,
                stream: "app".into(),
                severity: "INFO".into(),
                body: "server listening on port".into(),
                attrs: BTreeMap::new(),
            },
        ];
        let out = obs.ingest(recs).unwrap();
        assert_eq!(out.accepted, 2);

        // (a) tsdb: both points land in the series, ts-ordered.
        let points = obs.series_range("app", 0, 10_000).unwrap();
        assert_eq!(points.len(), 2);
        assert_eq!(points[0].ts, 1000);
        assert_eq!(points[0].values[0], severity_number("ERROR"));

        // (b) text: a BM25 query retrieves the matching record.
        let hits = obs.search("app", "database connection", 10);
        assert_eq!(hits.len(), 1, "one doc matches 'database connection'");
        assert!(hits[0].id.starts_with("app:"));
        // A term only in the OTHER doc matches that one, not the first.
        let listening = obs.search("app", "listening port", 10);
        assert_eq!(listening.len(), 1);
    }

    #[test]
    fn threshold_flush_rolls_a_parquet_segment_that_round_trips() {
        // flush_threshold=2 → the 2-record ingest rolls one segment immediately.
        let obs = ObsState::in_memory(2).unwrap();
        let recs = vec![
            LogRecord {
                ts: 10,
                stream: "s".into(),
                severity: "INFO".into(),
                body: "one".into(),
                attrs: BTreeMap::new(),
            },
            LogRecord {
                ts: 20,
                stream: "s".into(),
                severity: "WARN".into(),
                body: "two".into(),
                attrs: BTreeMap::new(),
            },
        ];
        let out = obs.ingest(recs).unwrap();
        assert_eq!(out.segments_flushed, 1);

        let segs = obs.segments_for("s");
        assert_eq!(segs.len(), 1);
        assert_eq!(segs[0].row_count, 2);
        assert_eq!(segs[0].min_ts, 10);
        assert_eq!(segs[0].max_ts, 20);

        // The Parquet segment round-trips out of the CAS.
        let back = obs.read_segment(&segs[0]).unwrap();
        assert_eq!(back.len(), 2);
        let bodies: Vec<&str> = back.iter().map(|r| r.body.as_str()).collect();
        assert!(bodies.contains(&"one") && bodies.contains(&"two"));
    }

    #[test]
    fn small_stream_force_flush_via_flush_stream() {
        // Below the threshold: nothing auto-flushes, but flush_stream forces it.
        let obs = ObsState::in_memory(1000).unwrap();
        obs.ingest(vec![LogRecord {
            ts: 5,
            stream: "tiny".into(),
            severity: "INFO".into(),
            body: "hello".into(),
            attrs: BTreeMap::new(),
        }])
        .unwrap();
        assert!(obs.segments_for("tiny").is_empty());
        let m = obs.flush_stream("tiny").unwrap().expect("forced segment");
        assert_eq!(m.row_count, 1);
        assert!(
            obs.flush_stream("tiny").unwrap().is_none(),
            "buffer drained"
        );
    }

    /// BUG-016 -- end-to-end restart proof against a REAL `ObsState` bound to a
    /// real persist dir. Proves BOTH halves: (1) a restart with NO explicit
    /// `persist_traces()` call correctly starts empty -- persistence is never
    /// implicit on the hot ingest path (that would reintroduce BUG-017's
    /// per-request write-amplification defect class); (2) a restart AFTER
    /// `persist_traces()` recovers the exact trace, byte-identically re-derivable.
    #[cfg(feature = "traces")]
    #[tokio::test(flavor = "current_thread")]
    async fn bug_016_traces_survive_an_obsstate_restart_only_after_persist_traces() {
        let dir = private_tempdir();
        let persist_dir = dir.path().to_str().expect("utf8 temp path");

        let span = eg_tsdb::traces::Span {
            trace_id: "t1".into(),
            span_id: "root".into(),
            parent_span_id: String::new(),
            service: "checkout".into(),
            operation: "POST /order".into(),
            start_time: 100,
            duration: 50,
            status: "OK".into(),
            attributes: BTreeMap::new(),
            events: Vec::new(),
        };

        // (1) Ingest a span but never call `persist_traces` -- a restart at this
        // point must see NOTHING durable yet (the bounded-staleness window).
        {
            let obs = ObsState::open(Some(persist_dir), 1024).await.expect("open");
            obs.trace_store().add_span(span.clone());
            assert_eq!(obs.trace_store().trace_count(), 1, "ingested in RAM");
        }
        {
            let reopened = ObsState::open(Some(persist_dir), 1024)
                .await
                .expect("reopen");
            assert_eq!(
                reopened.trace_store().trace_count(),
                0,
                "no persist_traces() call ever ran, so restart must start empty"
            );
        }

        // (2) Ingest again and THIS time call `persist_traces` before "restarting".
        {
            let obs = ObsState::open(Some(persist_dir), 1024).await.expect("open");
            obs.trace_store().add_span(span.clone());
            obs.persist_traces().await.expect("persist_traces");
        }
        {
            let reopened = ObsState::open(Some(persist_dir), 1024)
                .await
                .expect("reopen");
            assert_eq!(
                reopened.trace_store().trace_count(),
                1,
                "persist_traces() ran, so the restart must recover the trace"
            );
            let recovered = reopened.trace_store().assemble("t1").expect("trace t1");
            assert_eq!(recovered.span_count, 1);
            assert_eq!(recovered.services, vec!["checkout".to_string()]);
        }
    }

    /// BUG-210 -- end-to-end restart proof for the segment-manifest prune index.
    /// Ingest past `flush_threshold` (forcing a real segment flush) against a real
    /// persist dir, confirm `segments_for` is non-empty, "restart" (drop + reopen
    /// on the SAME persist dir), and assert the manifest -- and the segment's rows
    /// via `read_segment` -- are STILL reachable. Pre-fix this assertion fails (0
    /// segments after reopen, per the ledger's root-cause finding); post-fix it
    /// must pass.
    #[tokio::test(flavor = "current_thread")]
    async fn bug_210_segment_manifests_survive_an_obsstate_restart() {
        let dir = private_tempdir();
        let persist_dir = dir.path().to_str().expect("utf8 temp path");

        {
            let obs = ObsState::open(Some(persist_dir), 2).await.expect("open");
            let recs = vec![
                LogRecord {
                    ts: 10,
                    stream: "s".into(),
                    severity: "INFO".into(),
                    body: "one".into(),
                    attrs: BTreeMap::new(),
                },
                LogRecord {
                    ts: 20,
                    stream: "s".into(),
                    severity: "WARN".into(),
                    body: "two".into(),
                    attrs: BTreeMap::new(),
                },
            ];
            let out = obs.ingest(recs).expect("ingest");
            assert_eq!(out.segments_flushed, 1, "threshold=2 rolls one segment");
            assert_eq!(
                obs.segments_for("s").len(),
                1,
                "durably indexed before restart"
            );
        }

        let reopened = ObsState::open(Some(persist_dir), 2).await.expect("reopen");
        let segs = reopened.segments_for("s");
        assert_eq!(
            segs.len(),
            1,
            "the segment manifest must survive a restart, not silently vanish"
        );
        assert_eq!(segs[0].row_count, 2);
        assert_eq!(segs[0].min_ts, 10);
        assert_eq!(segs[0].max_ts, 20);

        // The bytes themselves were always durable in the blob CAS; the fix is
        // that the manifest recovered above makes them REACHABLE again.
        let rows = reopened.read_segment(&segs[0]).expect("read_segment");
        assert_eq!(rows.len(), 2);
        let bodies: Vec<&str> = rows.iter().map(|r| r.body.as_str()).collect();
        assert!(bodies.contains(&"one") && bodies.contains(&"two"));
    }

    /// The served observability state must use the server-selected CAS rather
    /// than opening a private `{persist_dir}/obs/blob.redb`.  The same selected
    /// store is supplied after restart so both the manifest index and segment
    /// bytes remain visible through one authority.
    #[tokio::test(flavor = "current_thread")]
    async fn injected_blob_store_is_shared_across_obs_restart() {
        let dir = private_tempdir();
        let persist_dir = dir.path().to_str().expect("utf8 temp path");
        let blob_dir = dir.path().join("selected-blob");
        let blob_dir_string = blob_dir.to_string_lossy().into_owned();
        let first_manifest = {
            let selected: Arc<dyn ChunkStore> =
                Arc::new(RedbChunkStore::open(&blob_dir_string).expect("open selected blob"));
            let obs = ObsState::open_with_blob_store(Some(persist_dir), 1, Some(selected.clone()))
                .await
                .expect("open injected observability state");
            let out = obs
                .ingest(vec![LogRecord {
                    ts: 10,
                    stream: "selected".into(),
                    severity: "INFO".into(),
                    body: "shared CAS".into(),
                    attrs: BTreeMap::new(),
                }])
                .expect("ingest");
            assert_eq!(out.segments_flushed, 1);
            let manifest = obs.segments_for("selected").pop().expect("segment");
            assert!(selected
                .get_manifest(&manifest.blob_digest)
                .expect("selected manifest")
                .is_some());
            manifest
        };

        let selected: Arc<dyn ChunkStore> =
            Arc::new(RedbChunkStore::open(&blob_dir_string).expect("reopen selected blob"));
        let reopened = ObsState::open_with_blob_store(Some(persist_dir), 1, Some(selected.clone()))
            .await
            .expect("reopen injected observability state");
        let manifests = reopened.segments_for("selected");
        assert_eq!(manifests.len(), 1);
        assert_eq!(manifests[0].blob_digest, first_manifest.blob_digest);
        let rows = reopened
            .read_segment(&manifests[0])
            .expect("read shared segment");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].body, "shared CAS");
        assert!(selected
            .get_manifest(&first_manifest.blob_digest)
            .expect("reopened selected manifest")
            .is_some());
        assert!(
            !dir.path().join("obs/blob/blob.redb").exists(),
            "injected composition must not create a private observability CAS"
        );
    }

    #[tokio::test]
    async fn http_otlp_ingest_end_to_end() {
        let obs = Arc::new(ObsState::in_memory(1024).unwrap());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let served = obs.clone();
        tokio::spawn(async move { serve(listener, served).await });

        let body = r#"{"resourceLogs":[{"resource":{"attributes":[{"key":"service.name","value":{"stringValue":"api"}}]},"scopeLogs":[{"logRecords":[{"timeUnixNano":"4200","severityText":"INFO","body":{"stringValue":"unique_marker_token served"}}]}]}]}"#;
        let mut sock = tokio::net::TcpStream::connect(addr).await.unwrap();
        let req = format!(
            "POST /v1/logs HTTP/1.1\r\nHost: x\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{}",
            body.len(),
            body
        );
        sock.write_all(req.as_bytes()).await.unwrap();
        let mut resp = Vec::new();
        sock.read_to_end(&mut resp).await.unwrap();
        let text = String::from_utf8_lossy(&resp);
        assert!(text.starts_with("HTTP/1.1 200 OK"), "got: {text}");
        assert!(text.contains("partialSuccess"));

        // The record landed: tsdb series + searchable text.
        let points = obs.series_range("api", 0, 10_000).unwrap();
        assert_eq!(points.len(), 1);
        assert_eq!(points[0].ts, 4200);
        let hits = obs.search("api", "unique_marker_token", 10);
        assert_eq!(hits.len(), 1);
    }

    #[tokio::test]
    async fn http_es_bulk_ingest_end_to_end() {
        let obs = Arc::new(ObsState::in_memory(1024).unwrap());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let served = obs.clone();
        tokio::spawn(async move { serve(listener, served).await });

        let body = "{\"index\":{\"_index\":\"esstream\"}}\n\
                    {\"@timestamp\":7,\"level\":\"ERROR\",\"message\":\"es_bulk_marker boom\"}\n";
        let mut sock = tokio::net::TcpStream::connect(addr).await.unwrap();
        let req = format!(
            "POST /_bulk HTTP/1.1\r\nHost: x\r\ncontent-type: application/x-ndjson\r\ncontent-length: {}\r\n\r\n{}",
            body.len(),
            body
        );
        sock.write_all(req.as_bytes()).await.unwrap();
        let mut resp = Vec::new();
        sock.read_to_end(&mut resp).await.unwrap();
        let text = String::from_utf8_lossy(&resp);
        assert!(text.starts_with("HTTP/1.1 200 OK"), "got: {text}");
        assert!(text.contains("\"errors\":false"));

        let hits = obs.search("esstream", "es_bulk_marker", 10);
        assert_eq!(hits.len(), 1);
        let points = obs.series_range("esstream", 0, 100_000_000).unwrap();
        assert_eq!(points.len(), 1);
    }

    /// Send a POST to `addr` and return the response text.
    #[cfg(test)]
    async fn post(addr: std::net::SocketAddr, path: &str, body: &str) -> String {
        let mut sock = tokio::net::TcpStream::connect(addr).await.unwrap();
        let req = format!(
            "POST {path} HTTP/1.1\r\nHost: x\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{}",
            body.len(),
            body
        );
        sock.write_all(req.as_bytes()).await.unwrap();
        let mut resp = Vec::new();
        sock.read_to_end(&mut resp).await.unwrap();
        String::from_utf8_lossy(&resp).to_string()
    }

    #[tokio::test]
    async fn http_preflight_rejects_ambiguous_or_oversized_framing() {
        let obs = Arc::new(ObsState::in_memory(1024).unwrap());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { serve(listener, obs).await });

        async fn raw(addr: std::net::SocketAddr, request: &str) -> String {
            let mut sock = tokio::net::TcpStream::connect(addr).await.unwrap();
            sock.write_all(request.as_bytes()).await.unwrap();
            let mut response = Vec::new();
            sock.read_to_end(&mut response).await.unwrap();
            String::from_utf8_lossy(&response).to_string()
        }

        let valid_body = r#"{"resourceLogs":[{"scopeLogs":[{"logRecords":[{"body":{"stringValue":"framing-control"}}]}]}]}"#;
        let framed = |head: &str| {
            format!(
                "{head}Content-Length: {}\r\n\r\n{valid_body}",
                valid_body.len()
            )
        };
        let assert_framing_rejection = |case: &str, response: &str| {
            assert!(
                response.starts_with("HTTP/1.1 400 Bad Request\r\n"),
                "{case} must reject with 400; got {response:?}"
            );
            assert_eq!(
                response.split("\r\n\r\n").nth(1),
                Some("malformed HTTP request"),
                "{case} must be rejected by the framing reader; got {response:?}"
            );
        };

        let control = raw(
            addr,
            &framed("POST /v1/logs HTTP/1.1\r\nHost: x\r\ncontent-type: application/json\r\n"),
        )
        .await;
        assert!(control.starts_with("HTTP/1.1 200 OK\r\n"), "got: {control}");
        assert_eq!(
            control.split("\r\n\r\n").nth(1),
            Some("{\"partialSuccess\":{}}")
        );

        let duplicate = raw(
            addr,
            "POST /v1/logs HTTP/1.1\r\nHost: x\r\nContent-Length: 0\r\nContent-Length: 0\r\n\r\n",
        )
        .await;
        assert_framing_rejection("duplicate content-length", &duplicate);

        let chunked = raw(
            addr,
            "POST /v1/logs HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\n\r\n0\r\n\r\n",
        )
        .await;
        assert_framing_rejection("chunked transfer-encoding", &chunked);

        let oversized = raw(
            addr,
            &format!(
                "POST /v1/logs HTTP/1.1\r\nHost: x\r\nContent-Length: {}\r\n\r\n",
                MAX_HTTP_BODY_BYTES + 1
            ),
        )
        .await;
        assert_framing_rejection("oversized body", &oversized);

        // Every case below is a MUST-reject that the shared `http1` reader owns
        // for all ten listeners. Each is a known-bad input: none of them may
        // ever frame, because each is a way for a fronting proxy and this
        // server to disagree about where one request ends.
        for (case, request) in [
            // RFC 7230 3.2.4: whitespace between a field name and its colon.
            // `name.trim()` would make this `host`, i.e. a second Host header a
            // proxy never saw.
            (
                "space before the colon",
                framed("POST /v1/logs HTTP/1.1\r\nHost : evil\r\n"),
            ),
            // RFC 7230 3.2.4: obs-fold. Trimming the name would re-materialize
            // a folded continuation as an independent `authorization` header.
            (
                "obs-fold continuation",
                framed(
                    "POST /v1/logs HTTP/1.1\r\nHost: x\r\nAccept: a\r\n Authorization: Bearer smuggled\r\n",
                ),
            ),
            // A repeated arbitrary name (not just Content-Length) is ambiguous.
            (
                "duplicate arbitrary header",
                framed("POST /v1/logs HTTP/1.1\r\nHost: x\r\nAccept: a\r\nAccept: b\r\n"),
            ),
            // RFC 7230 3.1.1: exactly two single spaces, no control bytes.
            (
                "tab in the request line",
                framed("POST /v1/logs\tHTTP/1.1\r\nHost: x\r\n"),
            ),
            (
                "doubled space in the request line",
                framed("POST  /v1/logs HTTP/1.1\r\nHost: x\r\n"),
            ),
            (
                "leading space in the request line",
                framed(" POST /v1/logs HTTP/1.1\r\nHost: x\r\n"),
            ),
            // A method must be an RFC 7230 token: `{` is not a tchar.
            (
                "non-token method",
                framed("PO{ST /v1/logs HTTP/1.1\r\nHost: x\r\n"),
            ),
            // Only HTTP/1.0 and HTTP/1.1 frame.
            (
                "unsupported version",
                framed("POST /v1/logs HTTP/2.0\r\nHost: x\r\n"),
            ),
            // Origin-form only.
            (
                "absolute-form target",
                framed("POST http://x/v1/logs HTTP/1.1\r\nHost: x\r\n"),
            ),
            // This surface requires a non-empty Host on 1.0 as well as 1.1.
            (
                "empty Host on HTTP/1.0",
                framed("POST /v1/logs HTTP/1.0\r\nHost: \r\n"),
            ),
            // The header-count bound.
            (
                "too many headers",
                framed(&format!(
                    "POST /v1/logs HTTP/1.1\r\nHost: x\r\n{}",
                    (0..200)
                        .map(|index| format!("x-pad-{index}: v\r\n"))
                        .collect::<String>()
                )),
            ),
        ] {
            let response = raw(addr, &request).await;
            assert_framing_rejection(case, &response);
        }
    }

    /// The `_search` HTTP surface: a structured search returns ES-shaped hits, and a
    /// `{"sql":…}` body aggregates over the log segments (CONCEPT:EG-KG.query.concept-4).
    #[tokio::test]
    async fn http_search_end_to_end() {
        // flush_threshold=2 → two records roll a segment, one stays hot.
        let obs = Arc::new(ObsState::in_memory(2).unwrap());
        obs.ingest(vec![
            LogRecord {
                ts: 100,
                stream: "web".into(),
                severity: "ERROR".into(),
                body: "search_marker cold".into(),
                attrs: BTreeMap::new(),
            },
            LogRecord {
                ts: 200,
                stream: "web".into(),
                severity: "INFO".into(),
                body: "quiet cold".into(),
                attrs: BTreeMap::new(),
            },
        ])
        .unwrap();
        obs.ingest(vec![LogRecord {
            ts: 300,
            stream: "web".into(),
            severity: "WARN".into(),
            body: "search_marker hot".into(),
            attrs: BTreeMap::new(),
        }])
        .unwrap();
        assert_eq!(obs.segments_for("web").len(), 1);

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let served = obs.clone();
        tokio::spawn(async move { serve(listener, served).await });

        // Structured search: time window spanning both tiers, path-derived stream.
        let text = post(
            addr,
            "/api/default/web/_search",
            r#"{"start_time":0,"end_time":1000}"#,
        )
        .await;
        assert!(text.starts_with("HTTP/1.1 200 OK"), "got: {text}");
        let json_body = text.split("\r\n\r\n").nth(1).unwrap();
        let v: serde_json::Value = serde_json::from_str(json_body).unwrap();
        assert_eq!(v["hits"]["total"]["value"], 3, "both tiers: {json_body}");

        // Full-text term via the `query` string → only the two "search_marker" records.
        let text = post(
            addr,
            "/api/_search",
            r#"{"stream":"web","query":"search_marker"}"#,
        )
        .await;
        let v: serde_json::Value =
            serde_json::from_str(text.split("\r\n\r\n").nth(1).unwrap()).unwrap();
        assert_eq!(v["hits"]["total"]["value"], 2);

        // SQL mode: count by severity over the `logs` table (segments + hot).
        let text = post(
            addr,
            "/api/_search",
            r#"{"sql":"SELECT severity, count(*) AS n FROM logs GROUP BY severity ORDER BY severity"}"#,
        )
        .await;
        assert!(text.starts_with("HTTP/1.1 200 OK"), "got: {text}");
        let v: serde_json::Value =
            serde_json::from_str(text.split("\r\n\r\n").nth(1).unwrap()).unwrap();
        assert_eq!(v["total"], 3, "ERROR/INFO/WARN buckets: {text}");
        assert_eq!(v["hits"][0]["severity"], "ERROR");
        assert_eq!(v["hits"][0]["n"], 1);
    }

    // ── GOC-62 D3(b): failing-first proof of BUG-037 ────────────────────────
    //
    // BUG-037 (P0, DEC-015 §1 finding #2 / GOC-62-keycloak-auth-standard.md §4):
    // `is_observability_read_carrier` (above) matches ONLY GET/query-shaped
    // paths, so `observability_read_denied`'s check in `handle` is never even
    // REACHED for a POST/ingest path -- every ingest path (`/v1/logs`, `/_bulk`,
    // `/<stream>/_doc`, `/`, and (feature `otel-export`) `/api/v1/write`) skips
    // authorization entirely, in EVERY deployment configuration including
    // `serve_with_security` (the production listener). This is materially worse
    // than every other EG auxiliary surface (Iceberg REST, SPARQL reads,
    // federation, `/nl`), which at minimum deny unconditionally.
    //
    // This test proves it by calling the SAME `handle()` this module's own
    // `serve_inner`/`serve_with_security` route every real request through,
    // with a non-`None` `security_state` (i.e. "a secured deployment") and NO
    // credential at all on a POST /v1/logs ingest request. A read-shaped GET is
    // exercised alongside as the contrast case (correctly denied), so this test
    // fails for the RIGHT reason -- an ingest bypass -- not because the whole
    // security posture is broken.
    //
    // Red today, by design (GOC-62 D3(b)). Turns green only once ingest paths
    // are folded into the SAME carrier check `is_observability_read_carrier`
    // gates reads with -- see GOC-62-keycloak-auth-standard.md §4/§5's method-
    // sensitivity rule: "read-vs-mutation classification, never a route/path-
    // shape heuristic".

    /// A minimal, real `ServerState` -- `unauthenticated_carrier_denied`/
    /// `observability_read_denied` only branch on `security_state.is_some()`
    /// (verified by direct reading, `access.rs::unauthenticated_carrier_denied`:
    /// `carrier.is_none()` unconditionally, since no carrier mechanism exists for
    /// this surface today), so ANY validly-constructed `ServerState` proves the
    /// "secured deployment" precondition. Use the public canonical constructor
    /// so feature-gated fields stay in sync with the rest of the engine fixtures.
    fn goc62_bug037_security_state(
    ) -> std::sync::Arc<tokio::sync::RwLock<crate::server::ServerState>> {
        let mut state = crate::server::ServerState::new_for_test(
            "goc62-bug037-test-secret", // nosec B105 - test only  // sanitizer:ignore — synthetic in-process test fixture, never a live credential
            crate::isolation::IsolationLayer::new(),
        );
        // Preserve this fixture's prior omission of the optional CDC hub.
        #[cfg(feature = "streaming")]
        {
            state.cdc = None;
        }
        std::sync::Arc::new(tokio::sync::RwLock::new(state))
    }

    #[tokio::test]
    async fn bug_037_obs_ingest_post_bypasses_the_deny_gate() {
        let obs = std::sync::Arc::new(ObsState::in_memory(1024).unwrap());
        let security_state = goc62_bug037_security_state();

        // Contrast case: a read-shaped GET IS correctly denied under a secured
        // deployment -- proves the harness is exercising the real gate, not a
        // vacuous stub.
        let read_req = HttpMessage {
            method: "GET".to_string(),
            target: "/api/v1/query?query=up".to_string(),
            version: "HTTP/1.1".to_string(),
            headers: HashMap::new(),
            body: Vec::new(),
        };
        let (read_status, _, _) = handle(&obs, Some(&security_state), read_req).await;
        assert_eq!(
            read_status, "403 Forbidden",
            "precondition: reads under a secured deployment must still deny with \
             no carrier -- if this fails, the harness itself is broken, not just \
             the ingest gate"
        );

        // The actual BUG-037 proof: an ingest POST, same secured deployment, same
        // zero credential -- must ALSO deny, but does not.
        let ingest_body = r#"{"resourceLogs":[]}"#;
        let ingest_req = HttpMessage {
            method: "POST".to_string(),
            target: "/v1/logs".to_string(),
            version: "HTTP/1.1".to_string(),
            headers: HashMap::new(),
            body: ingest_body.as_bytes().to_vec(),
        };
        let (ingest_status, _, ingest_body_out) =
            handle(&obs, Some(&security_state), ingest_req).await;

        assert_eq!(
            ingest_status, "403 Forbidden",
            "BUG-037 FAIL-OPEN: POST /v1/logs (an ingest/mutation path) was \
             accepted with a secured deployment and ZERO credential -- \
             is_observability_read_carrier only matches GET/query-shaped paths, \
             so observability_read_denied's check in `handle` is never reached \
             for this request. Got status {ingest_status:?}, body: \
             {ingest_body_out:?}"
        );
    }
}
