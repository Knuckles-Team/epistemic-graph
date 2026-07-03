//! Time-partitioned series store over redb (CONCEPT:KG-2.210) — the lean / Pi path.
//!
//! ## Layout (the storage thesis)
//!
//! The engine already keys its redb tables by composite tuples that redb
//! range-scans natively:
//! ```text
//! NODES: TableDefinition<(&str, &str), &[u8]>            // (graph, node_id)
//! EDGES: TableDefinition<(&str, &str, &str, u32), &[u8]> // (graph, src, dst, ord)
//! LEDGER: TableDefinition<(&str, u64), &str>             // (graph, seq)
//! ```
//! A time series is the SAME idea with `(series_id, bucket_start)` as the key, so
//! it lives in the SAME redb `Database` family next to nodes/edges with NO new
//! storage engine. The two `TableDefinition`s below are the canonical schema; the
//! facade's `src/server/persistence/redb_backend.rs` references them so they "live
//! with the other tables" while the store/query logic stays here in `eg-tsdb`.
//!
//! ```text
//! SERIES_CHUNKS: TableDefinition<(&str, u64), &[u8]>  // (series_id, bucket_start_ns) -> packed chunk
//! SERIES_META:   TableDefinition<&str, &[u8]>          // series_id -> SeriesMeta (msgpack)
//! ```
//!
//! ### Why chunks (the append-amortization decision)
//! One redb entry per point would mean one B-tree insert per 8-byte value —
//! per-key overhead dominates and append throughput/footprint both suffer. Instead
//! each key is a **time bucket** (e.g. 1h of wall-clock); its value is a packed
//! columnar chunk of every point whose ts falls in that bucket:
//! `[u32 n | u8 n_fields | i64 ts[n] | f64 vals[n*n_fields]]`. Append
//! read-modify-writes the touched bucket(s) in ONE write txn per batch; a
//! time-range scan is a redb RANGE over the covering bucket keys then a linear
//! walk of each decoded chunk. This is the LSM-of-chunks shape a TSDB wants,
//! expressed in redb's existing range contract. Multi-field (OHLCV) is just
//! `n_fields > 1` f64 columns per point.
//!
//! ### Durability
//! The store opens `{persist_dir}/series.redb` — a SEPARATE redb file from the
//! engine's `graph.redb`, because redb holds an EXCLUSIVE per-process file lock
//! (two handles on one file error "Database already open"). A separate file in the
//! same persist dir gets the same group-commit/fsync durability while staying
//! independent of the graph writer's lock. In-memory deployments (no persist dir)
//! open a temp file.

#![cfg(feature = "redb-store")]

use std::collections::BTreeMap;
use std::path::Path;

use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition, WriteTransaction};

pub use crate::point::{Point, Ts, TsError};

/// `(series_id, bucket_start_ns)` -> packed chunk blob. Same composite-key shape as
/// the engine's `NODES`/`EDGES` tables. CANONICAL schema for the series store; the
/// facade's `redb_backend.rs` references this so the durable tier owns the name.
pub const SERIES_CHUNKS: TableDefinition<'static, (&str, u64), &[u8]> =
    TableDefinition::new("series_chunks");
/// `series_id` -> msgpack [`SeriesMeta`]. CANONICAL schema (see `SERIES_CHUNKS`).
pub const SERIES_META: TableDefinition<'static, &str, &[u8]> = TableDefinition::new("series_meta");

type Result<T> = std::result::Result<T, TsError>;

fn redb_err<E: std::fmt::Display>(e: E) -> TsError {
    TsError::Redb(e.to_string())
}
fn codec_err<E: std::fmt::Display>(e: E) -> TsError {
    TsError::Codec(e.to_string())
}

/// Per-series metadata: field schema, bucket width, point count, ts span.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct SeriesMeta {
    pub n_fields: usize,
    /// Bucket width in nanoseconds (the time-partition size).
    pub bucket_ns: u64,
    pub field_names: Vec<String>,
    pub count: u64,
    pub min_ts: Ts,
    pub max_ts: Ts,
}

/// A decoded chunk: a bucket's worth of points, kept ts-sorted.
/// On disk: `[u32 n | u8 n_fields | i64 ts[n] | f64 vals[n*n_fields]]` (LE).
#[derive(Default)]
struct Chunk {
    n_fields: usize,
    ts: Vec<Ts>,
    vals: Vec<f64>, // row-major: point i fields at [i*n_fields .. (i+1)*n_fields]
}

impl Chunk {
    fn encode(&self) -> Vec<u8> {
        let n = self.ts.len();
        let mut out = Vec::with_capacity(5 + n * (8 + self.n_fields * 8));
        out.extend_from_slice(&(n as u32).to_le_bytes());
        out.push(self.n_fields as u8);
        for &t in &self.ts {
            out.extend_from_slice(&t.to_le_bytes());
        }
        for &v in &self.vals {
            out.extend_from_slice(&v.to_le_bytes());
        }
        out
    }

    /// Decode a stored chunk. Returns `Codec` rather than panicking on a truncated
    /// blob (durable bytes are trusted but a typed error beats an index panic).
    fn decode(buf: &[u8]) -> Result<Chunk> {
        if buf.len() < 5 {
            return Err(codec_err("chunk blob shorter than header"));
        }
        let n = u32::from_le_bytes(buf[0..4].try_into().unwrap()) as usize;
        let n_fields = buf[4] as usize;
        let need = 5 + n * 8 + n * n_fields * 8;
        if buf.len() < need {
            return Err(codec_err(format!(
                "chunk blob truncated: have {}, need {need}",
                buf.len()
            )));
        }
        let mut off = 5;
        let mut ts = Vec::with_capacity(n);
        for _ in 0..n {
            ts.push(i64::from_le_bytes(buf[off..off + 8].try_into().unwrap()));
            off += 8;
        }
        let mut vals = Vec::with_capacity(n * n_fields);
        for _ in 0..n * n_fields {
            vals.push(f64::from_le_bytes(buf[off..off + 8].try_into().unwrap()));
            off += 8;
        }
        Ok(Chunk { n_fields, ts, vals })
    }

    /// Insert keeping ts sorted (handles out-of-order / late points). Stable on ties
    /// (a later-arriving point with an equal ts lands AFTER the existing one), so a
    /// re-append of the same ts appends a sibling sample rather than reordering.
    fn insert(&mut self, ts: Ts, values: &[f64]) {
        let pos = self.ts.partition_point(|&t| t <= ts);
        self.ts.insert(pos, ts);
        let at = pos * self.n_fields;
        for (k, &v) in values.iter().enumerate() {
            self.vals.insert(at + k, v);
        }
    }

    /// CONCEPT:EG-068 — drop the leading points older than `cutoff`, keeping only
    /// `ts >= cutoff` (the per-point retention trim of a STRADDLING bucket). Because
    /// the chunk stays ts-sorted ascending the old points are a prefix, so the cut is
    /// a single `partition_point`; the kept suffix preserves order. Returns the number
    /// of points dropped (0 ⇒ nothing older than `cutoff`, leave the chunk untouched).
    fn trim_before(&mut self, cutoff: Ts) -> usize {
        let cut = self.ts.partition_point(|&t| t < cutoff);
        if cut == 0 {
            return 0;
        }
        self.ts.drain(0..cut);
        self.vals.drain(0..cut * self.n_fields);
        cut
    }
}

/// A time-partitioned series store over a redb database.
pub struct SeriesStore {
    db: Database,
}

impl SeriesStore {
    /// Open (or create) the series store at `path`, materializing the schema so an
    /// empty DB is queryable.
    pub fn open(path: &Path) -> Result<Self> {
        let db = Database::create(path).map_err(redb_err)?;
        let wtx = db.begin_write().map_err(redb_err)?;
        {
            wtx.open_table(SERIES_CHUNKS).map_err(redb_err)?;
            wtx.open_table(SERIES_META).map_err(redb_err)?;
        }
        wtx.commit().map_err(redb_err)?;
        Ok(Self { db })
    }

    /// Open `{persist_dir}/series.redb` — the durable location beside `graph.redb`.
    pub fn open_in_dir(persist_dir: &Path) -> Result<Self> {
        std::fs::create_dir_all(persist_dir)
            .map_err(|e| TsError::Redb(format!("create persist dir: {e}")))?;
        Self::open(&persist_dir.join("series.redb"))
    }

    #[inline]
    fn bucket_of(ts: Ts, bucket_ns: u64) -> u64 {
        (ts as u64 / bucket_ns) * bucket_ns
    }

    /// Append a batch of points to a series in ONE redb write transaction — the
    /// throughput primitive. Groups points by bucket, read-modify-writes each
    /// touched chunk once, updates meta. Creates the series (and its meta) on first
    /// append. Out-of-order / late points are handled by the sorted chunk insert.
    ///
    /// `bucket_ns`/`field_names` are used only when the series is NEW; for an
    /// existing series the stored schema wins, and a width mismatch is a hard error.
    pub fn append_batch(
        &self,
        series_id: &str,
        n_fields: usize,
        bucket_ns: u64,
        field_names: &[String],
        points: &[Point],
    ) -> Result<()> {
        if points.is_empty() {
            return Ok(());
        }
        let wtx = self.db.begin_write().map_err(redb_err)?;
        append_batch_in_wtx(&wtx, series_id, n_fields, bucket_ns, field_names, points)?;
        wtx.commit().map_err(redb_err)?;
        Ok(())
    }

    /// Fetch a series' metadata (`None` if the series doesn't exist).
    pub fn meta(&self, series_id: &str) -> Result<Option<SeriesMeta>> {
        let rtx = self.db.begin_read().map_err(redb_err)?;
        let tab = rtx.open_table(SERIES_META).map_err(redb_err)?;
        match tab.get(series_id).map_err(redb_err)? {
            Some(g) => Ok(Some(rmp_serde::from_slice(g.value()).map_err(codec_err)?)),
            None => Ok(None),
        }
    }

    /// Scan `[from, to)` of a series in ts order — the time-range read primitive.
    /// Implemented as a redb RANGE over the covering bucket keys, decoding each
    /// chunk and trimming to the exact window. Empty for an unknown series.
    pub fn range(&self, series_id: &str, from: Ts, to: Ts) -> Result<Vec<Point>> {
        let meta = match self.meta(series_id)? {
            Some(m) => m,
            None => return Ok(vec![]),
        };
        if meta.count == 0 {
            return Ok(vec![]);
        }
        // Clamp to the series' real bucket span. Bucket keys are u64; a caller's open
        // bound (Ts::MIN / Ts::MAX) must NOT be cast through u64 (two's-complement
        // wrap), so bound by the stored min/max instead. Real epoch-ns timestamps are
        // non-negative; this is what makes an unbounded `scan_all` correct.
        let scan_from = from.max(meta.min_ts);
        if scan_from > meta.max_ts {
            return Ok(vec![]);
        }
        let from_bucket = Self::bucket_of(scan_from, meta.bucket_ns);
        let to_bucket = Self::bucket_of(meta.max_ts, meta.bucket_ns);
        let rtx = self.db.begin_read().map_err(redb_err)?;
        let chunks = rtx.open_table(SERIES_CHUNKS).map_err(redb_err)?;
        let mut out = Vec::new();
        let lo = (series_id, from_bucket);
        let hi = (series_id, to_bucket); // covering buckets; exact ts filtered below
        for item in chunks.range(lo..=hi).map_err(redb_err)? {
            let (_k, v) = item.map_err(redb_err)?;
            let chunk = Chunk::decode(v.value())?;
            let nf = chunk.n_fields;
            for (i, &t) in chunk.ts.iter().enumerate() {
                if t >= from && t < to {
                    out.push(Point {
                        ts: t,
                        values: chunk.vals[i * nf..(i + 1) * nf].to_vec(),
                    });
                }
            }
        }
        Ok(out)
    }

    /// Full series scan (every point, ts order).
    pub fn scan_all(&self, series_id: &str) -> Result<Vec<Point>> {
        self.range(series_id, Ts::MIN, Ts::MAX)
    }

    /// List every series id present (scans the meta table's keys). Additive read used
    /// by the PromQL facade to resolve label matchers / enumerate labels (CONCEPT:EG-172)
    /// — the store keys series by opaque id, so the PromQL layer encodes a metric's
    /// labels INTO the id and enumerates them here.
    pub fn list_series(&self) -> Result<Vec<String>> {
        let rtx = self.db.begin_read().map_err(redb_err)?;
        let tab = rtx.open_table(SERIES_META).map_err(redb_err)?;
        let mut out = Vec::new();
        for item in tab.iter().map_err(redb_err)? {
            let (k, _v) = item.map_err(redb_err)?;
            out.push(k.value().to_string());
        }
        Ok(out)
    }

    /// Retention: drop every point of `series_id` older than `cutoff`. Returns the
    /// number of WHOLE buckets removed. Two cases (CONCEPT:EG-068):
    ///  * a bucket whose entire span ends at-or-before `cutoff` is range-deleted whole
    ///    (the cheap fast path — one B-tree remove, no decode);
    ///  * a bucket STRADDLING `cutoff` (starts before, ends after) is rewritten in
    ///    place keeping only its points `>= cutoff` (per-point trim), so retention is
    ///    exact to the point rather than rounded up to the bucket. A straddler whose
    ///    points are all older than `cutoff` collapses to a whole-bucket drop.
    ///
    /// Meta's `count`/`min_ts` are recomputed from the surviving (possibly trimmed)
    /// buckets; `max_ts` is unchanged unless the series is fully emptied.
    pub fn evict_before(&self, series_id: &str, cutoff: Ts) -> Result<usize> {
        let meta = match self.meta(series_id)? {
            Some(m) => m,
            None => return Ok(0),
        };
        let wtx = self.db.begin_write().map_err(redb_err)?;
        let mut dropped = 0usize;
        {
            let mut chunks = wtx.open_table(SERIES_CHUNKS).map_err(redb_err)?;
            let mut meta_tab = wtx.open_table(SERIES_META).map_err(redb_err)?;

            // Pass 1 (read-only over the range — can't mutate while the iterator
            // borrows `chunks`): classify each bucket into a whole-bucket victim or a
            // straddler rewrite. A straddler trimmed to empty becomes a victim.
            let mut victims: Vec<u64> = Vec::new();
            let mut rewrites: Vec<(u64, Vec<u8>)> = Vec::new();
            let lo = (series_id, 0u64);
            let hi = (series_id, u64::MAX);
            for item in chunks.range(lo..=hi).map_err(redb_err)? {
                let (k, v) = item.map_err(redb_err)?;
                let bucket = k.value().1;
                let bucket_end = bucket.saturating_add(meta.bucket_ns);
                if (bucket_end as i64) <= cutoff {
                    victims.push(bucket);
                } else if (bucket as i64) < cutoff {
                    // Straddles `cutoff`: trim the older prefix in place (CONCEPT:EG-068).
                    let mut chunk = Chunk::decode(v.value())?;
                    if chunk.trim_before(cutoff) > 0 {
                        if chunk.ts.is_empty() {
                            victims.push(bucket);
                        } else {
                            rewrites.push((bucket, chunk.encode()));
                        }
                    }
                }
            }

            let mut new_count = 0u64;
            let mut new_min = Ts::MAX;
            for bucket in &victims {
                if let Some(g) = chunks.remove((series_id, *bucket)).map_err(redb_err)? {
                    drop(g);
                    dropped += 1;
                }
            }
            for (bucket, blob) in &rewrites {
                chunks
                    .insert((series_id, *bucket), blob.as_slice())
                    .map_err(redb_err)?;
            }
            // Recompute count/min over survivors (max is unchanged — we only drop old).
            for item in chunks.range(lo..=hi).map_err(redb_err)? {
                let (_k, v) = item.map_err(redb_err)?;
                let chunk = Chunk::decode(v.value())?;
                new_count += chunk.ts.len() as u64;
                if let Some(&first) = chunk.ts.first() {
                    new_min = new_min.min(first);
                }
            }
            let mut m = meta.clone();
            m.count = new_count;
            m.min_ts = if new_count == 0 { Ts::MAX } else { new_min };
            if new_count == 0 {
                m.max_ts = Ts::MIN;
            }
            let mblob = rmp_serde::to_vec(&m).map_err(codec_err)?;
            meta_tab
                .insert(series_id, mblob.as_slice())
                .map_err(redb_err)?;
        }
        wtx.commit().map_err(redb_err)?;
        Ok(dropped)
    }
}

/// Append `points` to `series_id` INTO an already-open redb [`WriteTransaction`] the
/// CALLER owns (CONCEPT:EG-360 — cross-modal atomic commit). Byte-for-byte the SAME
/// chunk encoding + read-modify-write + meta bookkeeping as [`SeriesStore::append_batch`]
/// (that method now delegates here), but it opens `SERIES_CHUNKS`/`SERIES_META` on the
/// passed transaction instead of the store's own `series.redb`.
///
/// This is what lets a time-series measurement batch land in the SAME `graph.redb`
/// `WriteTransaction` as a cross-modal txn's graph/vector/blob writes: redb holds an
/// EXCLUSIVE per-process file lock, so the only way measurements can be atomic WITH the
/// graph modalities is to write them through the graph transaction the redb writer thread
/// already owns — not a second `series.redb` handle. The caller is responsible for
/// `begin_write`, `set_durability`, and `commit`; on any error it drops the `wtx` and NONE
/// of the modalities (measurements included) land — a true all-or-nothing rollback.
///
/// `bucket_ns`/`field_names` are used only when the series is NEW; for an existing series
/// the stored schema is authoritative and a width mismatch is a hard error (identical to
/// [`SeriesStore::append_batch`]).
pub fn append_batch_in_wtx(
    wtx: &WriteTransaction,
    series_id: &str,
    n_fields: usize,
    bucket_ns: u64,
    field_names: &[String],
    points: &[Point],
) -> Result<()> {
    if points.is_empty() {
        return Ok(());
    }
    // Reject a mixed-width batch up front (each point must match n_fields).
    for p in points {
        if p.values.len() != n_fields {
            return Err(TsError::FieldMismatch {
                expected: n_fields,
                got: p.values.len(),
            });
        }
    }
    let mut chunks = wtx.open_table(SERIES_CHUNKS).map_err(redb_err)?;
    let mut meta_tab = wtx.open_table(SERIES_META).map_err(redb_err)?;

    // Load-or-init meta. An existing series' stored schema is authoritative.
    let mut meta: SeriesMeta = match meta_tab.get(series_id).map_err(redb_err)? {
        Some(g) => rmp_serde::from_slice(g.value()).map_err(codec_err)?,
        None => SeriesMeta {
            n_fields,
            bucket_ns,
            field_names: field_names.to_vec(),
            count: 0,
            min_ts: Ts::MAX,
            max_ts: Ts::MIN,
        },
    };
    if meta.n_fields != n_fields {
        return Err(TsError::FieldMismatch {
            expected: meta.n_fields,
            got: n_fields,
        });
    }

    // Group incoming points by bucket so each chunk is touched once.
    let mut by_bucket: BTreeMap<u64, Vec<&Point>> = BTreeMap::new();
    for p in points {
        by_bucket
            .entry(SeriesStore::bucket_of(p.ts, meta.bucket_ns))
            .or_default()
            .push(p);
    }

    for (bucket, pts) in by_bucket {
        let mut chunk = match chunks.get((series_id, bucket)).map_err(redb_err)? {
            Some(g) => Chunk::decode(g.value())?,
            None => Chunk {
                n_fields: meta.n_fields,
                ts: vec![],
                vals: vec![],
            },
        };
        for p in pts {
            chunk.insert(p.ts, &p.values);
            meta.count += 1;
            meta.min_ts = meta.min_ts.min(p.ts);
            meta.max_ts = meta.max_ts.max(p.ts);
        }
        let blob = chunk.encode();
        chunks
            .insert((series_id, bucket), blob.as_slice())
            .map_err(redb_err)?;
    }

    let mblob = rmp_serde::to_vec(&meta).map_err(codec_err)?;
    meta_tab
        .insert(series_id, mblob.as_slice())
        .map_err(redb_err)?;
    Ok(())
}
