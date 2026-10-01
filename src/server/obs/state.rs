//! Construction, recovery and the ingest/read methods of [`ObsState`].

use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use parking_lot::Mutex;

use crate::server::blob::store::{ChunkStore, RedbChunkStore};
use eg_text::TextIndex;
use eg_tsdb::point::Point;
use eg_tsdb::store::SeriesStore;

use super::manifests::{
    load_segment_manifests, record_segment_manifest, LoadedSegmentManifests, SegmentManifestUsage,
};
use super::parse::text_body;
use super::segment::{self, SegmentManifest};
#[cfg(feature = "traces")]
use super::snapshot::load_traces_snapshot;
use super::snapshot::{SnapshotDirectory, OBS_PERSISTENCE_DIRECTORY_ERROR};
use super::{severity_number, stream_storage_key, IngestOutcome, LogRecord, ObsState};

/// TSDB time-partition width for a log series: 1 hour of wall-clock per chunk.
const SERIES_BUCKET_NS: u64 = 3_600_000_000_000;

fn open_persistent_obs_stores(
    base: &Path,
    blob_store: Option<Arc<dyn ChunkStore>>,
) -> Result<(SeriesStore, Arc<dyn ChunkStore>), String> {
    let authority = SnapshotDirectory::open(base, true, OBS_PERSISTENCE_DIRECTORY_ERROR)?;
    let series = SeriesStore::open_in_dir(
        &authority.io_path,
        crate::store_authority::process_verifier(),
        crate::store_authority::process_authority().principal(),
        &crate::store_authority::process_authority().proof(),
    )
    .map_err(|_| "observability series store is unavailable".to_string())?;
    let blob: Arc<dyn ChunkStore> = match blob_store {
        Some(blob) => blob,
        None => Arc::new(
            RedbChunkStore::open(
                &authority
                    .child(std::ffi::OsStr::new("blob"))
                    .to_string_lossy(),
            )
            .map_err(|_| "observability blob store is unavailable".to_string())?,
        ),
    };
    authority.require_still_named(OBS_PERSISTENCE_DIRECTORY_ERROR)?;
    Ok((series, blob))
}

/// The complete synchronous construction/recovery boundary used by both the
/// public async constructor and test-only ephemeral construction.
fn open_obs_state_blocking(
    persist_dir: Option<&str>,
    flush_threshold: usize,
    blob_store: Option<Arc<dyn ChunkStore>>,
) -> Result<ObsState, String> {
    let (series, blob, text_dir, obs_base) = match persist_dir {
        Some(dir) => {
            let base = Path::new(dir).join("obs");
            let (series, blob) = open_persistent_obs_stores(&base, blob_store)?;
            (series, blob, Some(base.join("text")), Some(base))
        }
        None => {
            // A repeated clock tick must not alias another live store.
            let base = crate::server::unique_temp_dir("eg-obs");
            let series = SeriesStore::open_in_dir(
                &base,
                crate::store_authority::process_verifier(),
                crate::store_authority::process_authority().principal(),
                &crate::store_authority::process_authority().proof(),
            )
            .map_err(|_| "observability series store is unavailable".to_string())?;
            let blob: Arc<dyn ChunkStore> = match blob_store {
                Some(blob) => blob,
                None => Arc::new(
                    RedbChunkStore::open(&base.join("blob").to_string_lossy())
                        .map_err(|_| "observability blob store is unavailable".to_string())?,
                ),
            };
            (series, blob, None, None)
        }
    };
    let loaded = match obs_base.as_deref() {
        Some(base) => load_segment_manifests(base)?,
        None => LoadedSegmentManifests {
            manifests: Vec::new(),
            positions: HashMap::new(),
            usage: SegmentManifestUsage::default(),
        },
    };
    #[cfg(feature = "traces")]
    let traces = match obs_base.as_deref() {
        Some(base) => match load_traces_snapshot(base)? {
            Some(traces) => traces,
            None => eg_tsdb::traces::SpanStore::new(),
        },
        None => eg_tsdb::traces::SpanStore::new(),
    };
    Ok(ObsState {
        series: Arc::new(series),
        blob,
        indices: Mutex::new(HashMap::new()),
        buffers: Mutex::new(HashMap::new()),
        segments: Mutex::new(loaded.manifests),
        manifest_positions: Mutex::new(loaded.positions),
        manifest_usage: Mutex::new(loaded.usage),
        text_dir,
        obs_base,
        flush_threshold: flush_threshold.max(1),
        next_doc: AtomicU64::new(1),
        #[cfg(feature = "traces")]
        traces: Arc::new(traces),
    })
}

impl ObsState {
    /// Open the ingest substrate with a selected blob CAS under a persist dir
    /// (durable series + injected blob CAS + on-disk text indices under
    /// `{persist_dir}/obs/…`). With `persist_dir = None` everything is in a temp
    /// dir / in-memory (tests / ephemeral). The complete initialization and
    /// recovery boundary runs on Tokio's blocking pool.
    pub async fn open_with_blob_store(
        persist_dir: Option<&str>,
        flush_threshold: usize,
        blob_store: Option<Arc<dyn ChunkStore>>,
    ) -> Result<Self, String> {
        let persist_dir = persist_dir.map(str::to_owned);
        ::tokio::task::spawn_blocking(move || {
            open_obs_state_blocking(persist_dir.as_deref(), flush_threshold, blob_store)
        })
        .await
        .map_err(|_| "observability persistence worker failed".to_string())?
    }

    /// Open the observability substrate with its legacy private Redb fallback.
    /// Served startup uses [`Self::open_with_blob_store`] so all blob consumers
    /// share the server-selected CAS authority.
    pub async fn open(persist_dir: Option<&str>, flush_threshold: usize) -> Result<Self, String> {
        Self::open_with_blob_store(persist_dir, flush_threshold, None).await
    }

    /// CONCEPT:EG-OS.observability.trace-assembly — the distributed-trace span store handle, used by the trace
    /// facade (`src/server/traces`) to ingest spans and serve trace search/assembly.
    #[cfg(feature = "traces")]
    pub fn trace_store(&self) -> Arc<eg_tsdb::traces::SpanStore> {
        self.traces.clone()
    }

    /// In-memory ingest state (temp series/blob, RAM text indices) — for tests.
    pub fn in_memory(flush_threshold: usize) -> Result<Self, String> {
        open_obs_state_blocking(None, flush_threshold, None)
    }

    /// Ingest a batch of normalized records: append each stream's points to its tsdb
    /// series, upsert each record into its stream's text index, buffer it for a
    /// Parquet flush, and roll a segment for any stream that crossed the threshold.
    pub fn ingest(&self, records: Vec<LogRecord>) -> Result<IngestOutcome, String> {
        if records.is_empty() {
            return Ok(IngestOutcome::default());
        }
        // Group by stream so each series append + index commit is one batch.
        let mut by_stream: HashMap<String, Vec<LogRecord>> = HashMap::new();
        for r in records {
            by_stream.entry(r.stream.clone()).or_default().push(r);
        }

        let mut accepted = 0usize;
        let mut segments_flushed = 0usize;
        for (stream, recs) in by_stream {
            accepted += recs.len();

            // (a) tsdb series — time-range + retention.
            let series_id = format!("obs:logs:{stream}");
            let points: Vec<Point> = recs
                .iter()
                .map(|r| Point {
                    ts: r.ts,
                    values: vec![severity_number(&r.severity)],
                })
                .collect();
            self.series
                .append_batch(
                    &series_id,
                    1,
                    SERIES_BUCKET_NS,
                    &["severity".to_string()],
                    &points,
                )
                .map_err(|e| e.to_string())?;

            // (b) full-text index — schema-on-read (attrs flattened into the text).
            self.with_index(&stream, |ix| {
                for r in &recs {
                    let seq = self.next_doc.fetch_add(1, Ordering::Relaxed);
                    let doc_id = format!("{stream}:{seq}");
                    ix.upsert(&doc_id, &text_body(r));
                }
                ix.commit().map_err(|e| e.to_string())
            })?;

            // (c) buffer for Parquet; flush a segment past the threshold.
            let drained = {
                let mut buffers = self.buffers.lock();
                let buf = buffers.entry(stream.clone()).or_default();
                buf.extend(recs);
                if buf.len() >= self.flush_threshold {
                    Some(std::mem::take(buf))
                } else {
                    None
                }
            };
            if let Some(batch) = drained {
                if let Some(manifest) =
                    segment::flush_records_to_segment(self.blob.as_ref(), &stream, &batch)?
                {
                    record_segment_manifest(&self.segments, &self.manifest_positions, manifest);
                    // BUG-210: durably index this stream's manifest list at the SAME
                    // cadence as the flush itself (bounded: one small rewrite per
                    // `flush_threshold`-many records, not per request).
                    self.persist_stream_segments(&stream)?;
                    segments_flushed += 1;
                }
            }
        }
        Ok(IngestOutcome {
            accepted,
            segments_flushed,
        })
    }

    /// Force-flush a stream's buffered records into a Parquet segment NOW (used at
    /// shutdown / tests / a small stream that never crosses the threshold). Returns
    /// the manifest, or `None` if the buffer was empty.
    pub fn flush_stream(&self, stream: &str) -> Result<Option<SegmentManifest>, String> {
        let batch = {
            let mut buffers = self.buffers.lock();
            match buffers.get_mut(stream) {
                Some(buf) if !buf.is_empty() => std::mem::take(buf),
                _ => return Ok(None),
            }
        };
        let manifest = segment::flush_records_to_segment(self.blob.as_ref(), stream, &batch)?;
        if let Some(m) = &manifest {
            record_segment_manifest(&self.segments, &self.manifest_positions, m.clone());
            // BUG-210: see the matching comment in `ingest`.
            self.persist_stream_segments(stream)?;
        }
        Ok(manifest)
    }

    /// BM25 search a stream's text index — returns `(doc_id, score)` hits. The
    /// query-back surface tests + a future EG-162 search endpoint use.
    pub fn search(&self, stream: &str, query: &str, k: usize) -> Vec<eg_text::TextHit> {
        let mut indices = self.indices.lock();
        match indices.get_mut(stream) {
            Some(ix) => ix.search(query, k),
            None => Vec::new(),
        }
    }

    /// The durable time-series store handle — used by the PromQL facade
    /// (CONCEPT:EG-KG.query.prometheus-http-query-api) to resolve selectors / enumerate labels over the same series
    /// the log ingest lands in.
    pub fn series_store(&self) -> Arc<SeriesStore> {
        self.series.clone()
    }

    /// Read a stream's tsdb series points over `[from, to)` (epoch-ns) — proves the
    /// records landed in the time-series tier.
    pub fn series_range(&self, stream: &str, from: i64, to: i64) -> Result<Vec<Point>, String> {
        self.series
            .range(&format!("obs:logs:{stream}"), from, to)
            .map_err(|e| e.to_string())
    }

    /// Segment manifests recorded for a stream (the prune index).
    pub fn segments_for(&self, stream: &str) -> Vec<SegmentManifest> {
        self.segments
            .lock()
            .iter()
            .filter(|manifest| manifest.stream == stream)
            .cloned()
            .collect()
    }

    /// Read a flushed Parquet segment's records back out of the blob CAS.
    pub fn read_segment(&self, manifest: &SegmentManifest) -> Result<Vec<LogRecord>, String> {
        let bytes = segment::read_segment_bytes(self.blob.as_ref(), &manifest.blob_digest)?;
        segment::parquet_to_records(&bytes)
    }

    /// Run `f` with a mutable handle to `stream`'s text index, creating it lazily
    /// (on-disk under `text_dir` when persistent, else in-RAM).
    fn with_index<T>(
        &self,
        stream: &str,
        f: impl FnOnce(&mut TextIndex) -> Result<T, String>,
    ) -> Result<T, String> {
        let mut indices = self.indices.lock();
        if !indices.contains_key(stream) {
            let ix = match &self.text_dir {
                Some(base) => {
                    let dir = base.join(stream_storage_key(stream));
                    std::fs::create_dir_all(&dir).map_err(|_| {
                        "observability text index directory is unavailable".to_string()
                    })?;
                    TextIndex::open(&dir)
                        .map_err(|_| "observability text index is unavailable".to_string())?
                }
                None => TextIndex::in_memory()
                    .map_err(|_| "observability text index is unavailable".to_string())?,
            };
            indices.insert(stream.to_string(), ix);
        }
        let ix = indices.get_mut(stream).expect("just inserted");
        f(ix)
    }
}
