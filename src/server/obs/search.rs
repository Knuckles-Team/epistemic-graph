//! Observability log SEARCH + query API (CONCEPT:EG-162) — the third slice of
//! Phase T ("surpass OpenObserve"), layered over the log-ingestion front door
//! (CONCEPT:EG-160) and the Parquet-on-blob-CAS segments (CONCEPT:EG-161).
//!
//! ## The search thesis
//!
//! A log search names a `stream`, a time window and (optionally) full-text terms +
//! structured attribute filters. The matching records are UNIONed across the two
//! physical tiers the ingest front door lands them in:
//!
//!  * **hot** — the per-stream buffer of records NOT YET rolled into a Parquet
//!    segment (their bodies still live in RAM; the tsdb series is the time index
//!    over them), and
//!  * **cold** — the Parquet columnar segments in the blob CAS.
//!
//! The manifest is the prune index: BEFORE reading any segment bytes we drop every
//! segment whose `[min_ts, max_ts]` does NOT overlap the query window (the
//! "reduce the search space 99%" idea — a bounded search touches only the handful of
//! segments that could hold a matching row). Full-text terms consult the per-stream
//! `eg-text` BM25 index (the authority on "does any doc match these terms"); the
//! matching records are then selected out of the tier union.
//!
//! ## SQL over logs
//!
//! [`ObsState::search_sql`] registers the scanned segments + hot buffers as a single
//! DataFusion `logs` table (Arrow batches via [`super::segment::records_to_batch`])
//! and runs arbitrary read-only SQL over it through the SAME DataFusion stack the
//! graph SQL surface uses ([`eg_query::exec_sql_over_tables`]) — so
//! `SELECT severity, count(*) FROM logs GROUP BY severity` aggregates over the
//! columnar log segments.

use super::segment::{self, SegmentManifest};
use super::{text_body, LogRecord, ObsState};

/// Default hit cap when a query does not name a `size`.
pub const DEFAULT_SEARCH_SIZE: usize = 100;
/// Upper bound on BM25 hits pulled to gate a full-text search.
const MAX_TEXT_HITS: usize = 10_000;

/// A parsed log-search query: a stream (per-stream tsdb/text/segments), a half-open
/// time window `[from, to)` in epoch-ns, optional full-text terms, optional structured
/// filters (attribute equality + severity), and a hit cap.
#[derive(Clone, Debug)]
pub struct LogQuery {
    /// Stream namespace to search (required — its own series + text index + segments).
    pub stream: String,
    /// Inclusive lower time bound (epoch-ns). `i64::MIN` ⇒ unbounded below.
    pub from: i64,
    /// Exclusive upper time bound (epoch-ns). `i64::MAX` ⇒ unbounded above.
    pub to: i64,
    /// Full-text terms (whitespace-separated); `None`/empty ⇒ no full-text filter.
    pub terms: Option<String>,
    /// Structured attribute equality filters (`attrs[key] == value`).
    pub filters: Vec<(String, String)>,
    /// Optional severity equality filter (case-insensitive).
    pub severity: Option<String>,
    /// Max records to return (0 ⇒ [`DEFAULT_SEARCH_SIZE`]).
    pub size: usize,
}

impl LogQuery {
    /// A whole-stream query (unbounded time window, no filters).
    pub fn all(stream: impl Into<String>) -> Self {
        Self {
            stream: stream.into(),
            from: i64::MIN,
            to: i64::MAX,
            terms: None,
            filters: Vec::new(),
            severity: None,
            size: DEFAULT_SEARCH_SIZE,
        }
    }
}

/// Does the closed segment range `[min, max]` overlap the half-open query `[from, to)`?
fn overlaps(min: i64, max: i64, from: i64, to: i64) -> bool {
    min < to && max >= from
}

/// Does a record's searchable text contain every whitespace term (case-insensitive)?
/// The stemming-aware BM25 index is the authority on match existence; this predicate
/// selects the concrete records out of the scanned tier union.
fn text_matches(rec: &LogRecord, terms: &str) -> bool {
    let hay = text_body(rec).to_ascii_lowercase();
    terms
        .split_whitespace()
        .map(|t| t.trim_matches(|c| c == '"' || c == '\'').to_ascii_lowercase())
        .filter(|t| !t.is_empty())
        .all(|t| hay.contains(&t))
}

impl ObsState {
    /// Execute a log search: UNION the hot buffer + the manifest-pruned cold Parquet
    /// segments for the stream, filter by the time window, then apply the full-text
    /// (BM25-gated) + structured filters, sort by timestamp ascending, and cap at
    /// `size` (CONCEPT:EG-162).
    pub fn search_logs(&self, q: &LogQuery) -> Result<Vec<LogRecord>, String> {
        let mut candidates: Vec<LogRecord> = Vec::new();

        // (cold) Prune FIRST via the manifest: read only segments whose time range
        // overlaps the query window — the bytes of a non-overlapping segment are never
        // fetched from the CAS.
        let overlapping: Vec<SegmentManifest> = {
            let segs = self.segments.lock();
            segs.iter()
                .filter(|m| m.stream == q.stream && overlaps(m.min_ts, m.max_ts, q.from, q.to))
                .cloned()
                .collect()
        };
        for m in &overlapping {
            candidates.extend(self.read_segment(m)?);
        }

        // (hot) The per-stream buffer of records not yet rolled into a segment.
        {
            let buffers = self.buffers.lock();
            if let Some(buf) = buffers.get(&q.stream) {
                candidates.extend(buf.iter().cloned());
            }
        }

        // Time window.
        candidates.retain(|r| r.ts >= q.from && r.ts < q.to);

        // Full-text: the BM25 index is the authority on whether the terms match at all
        // (stemming/analysis); an empty hit set short-circuits to no records.
        if let Some(terms) = q.terms.as_deref().filter(|t| !t.trim().is_empty()) {
            let hits = self.search(&q.stream, terms, MAX_TEXT_HITS);
            if hits.is_empty() {
                return Ok(Vec::new());
            }
            candidates.retain(|r| text_matches(r, terms));
        }

        // Structured filters.
        if let Some(sev) = &q.severity {
            candidates.retain(|r| r.severity.eq_ignore_ascii_case(sev));
        }
        for (k, v) in &q.filters {
            candidates.retain(|r| r.attrs.get(k).map(|x| x == v).unwrap_or(false));
        }

        candidates.sort_by_key(|r| r.ts);
        let size = if q.size == 0 { DEFAULT_SEARCH_SIZE } else { q.size };
        candidates.truncate(size);
        Ok(candidates)
    }

    /// Run read-only `sql` over ALL log records — the cold Parquet segments (scanned
    /// out of the blob CAS) plus the hot buffers — registered as one DataFusion `logs`
    /// table (CONCEPT:EG-162). Columns are the fixed segment schema (`ts`, `stream`,
    /// `severity`, `body`, `attrs`), so `SELECT severity, count(*) FROM logs GROUP BY
    /// severity` aggregates over the columnar segments; `json_get('<attrs>', 'key')`
    /// reaches a schema-on-read attribute. Synchronous — safe under `spawn_blocking`.
    pub fn search_sql(&self, sql: &str) -> Result<eg_query::TypedQueryResult, String> {
        let mut batches = Vec::new();

        // (cold) every Parquet segment → Arrow batch (read bytes from the CAS, decode).
        let segs: Vec<SegmentManifest> = self.segments.lock().clone();
        for m in &segs {
            let recs = self.read_segment(m)?;
            if !recs.is_empty() {
                batches.push(segment::records_to_batch(&recs)?);
            }
        }

        // (hot) all not-yet-flushed buffered records across every stream.
        let hot: Vec<LogRecord> = {
            let buffers = self.buffers.lock();
            buffers.values().flatten().cloned().collect()
        };
        if !hot.is_empty() {
            batches.push(segment::records_to_batch(&hot)?);
        }

        let schema = segment::log_arrow_schema();
        eg_query::exec_sql_over_tables(vec![("logs".to_string(), schema, batches)], sql)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    fn rec(ts: i64, stream: &str, sev: &str, body: &str) -> LogRecord {
        LogRecord {
            ts,
            stream: stream.to_string(),
            severity: sev.to_string(),
            body: body.to_string(),
            attrs: BTreeMap::new(),
        }
    }

    /// A batch that straddles both tiers: with flush_threshold=2, the first two
    /// records roll a Parquet segment; a third stays in the hot buffer. A time-range
    /// search returns records from BOTH tiers and prunes the non-overlapping segment.
    #[test]
    fn search_time_range_unions_hot_and_cold_tiers() {
        let obs = ObsState::in_memory(2).unwrap();
        // ts 100 & 200 → flushed to a segment; ts 300 stays hot.
        obs.ingest(vec![
            rec(100, "app", "INFO", "cold one"),
            rec(200, "app", "WARN", "cold two"),
        ])
        .unwrap();
        obs.ingest(vec![rec(300, "app", "ERROR", "hot three")])
            .unwrap();

        // One cold segment recorded; one record still buffered hot.
        assert_eq!(obs.segments_for("app").len(), 1);

        // A window spanning both tiers returns all three, ts-ordered.
        let mut q = LogQuery::all("app");
        q.from = 0;
        q.to = 1_000;
        let hits = obs.search_logs(&q).unwrap();
        let bodies: Vec<&str> = hits.iter().map(|r| r.body.as_str()).collect();
        assert_eq!(bodies, vec!["cold one", "cold two", "hot three"]);

        // A narrow window over only the hot record prunes the cold segment entirely.
        let mut q2 = LogQuery::all("app");
        q2.from = 250;
        q2.to = 400;
        let hot_only = obs.search_logs(&q2).unwrap();
        assert_eq!(hot_only.len(), 1);
        assert_eq!(hot_only[0].body, "hot three");

        // A window below everything returns nothing (both tiers pruned/filtered).
        let mut q3 = LogQuery::all("app");
        q3.from = 0;
        q3.to = 50;
        assert!(obs.search_logs(&q3).unwrap().is_empty());
    }

    /// A full-text term search consults the BM25 index and returns the matching record.
    #[test]
    fn search_full_text_hits_bm25_index() {
        let obs = ObsState::in_memory(2).unwrap();
        obs.ingest(vec![
            rec(10, "svc", "ERROR", "database connection refused"),
            rec(20, "svc", "INFO", "server listening on port"),
        ])
        .unwrap();
        // Both records flushed to a segment (threshold 2). Full-text still resolves
        // against the BM25 index + the scanned cold records.
        assert_eq!(obs.segments_for("svc").len(), 1);

        let mut q = LogQuery::all("svc");
        q.terms = Some("database connection".to_string());
        let hits = obs.search_logs(&q).unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].body, "database connection refused");

        // A term that matches nothing (BM25 gate) returns no records.
        let mut none = LogQuery::all("svc");
        none.terms = Some("kubernetes".to_string());
        assert!(obs.search_logs(&none).unwrap().is_empty());
    }

    /// A SQL query aggregates over the flushed segment (count by severity).
    #[test]
    fn search_sql_aggregates_over_segments() {
        let obs = ObsState::in_memory(4).unwrap();
        obs.ingest(vec![
            rec(1, "q", "ERROR", "a"),
            rec(2, "q", "ERROR", "b"),
            rec(3, "q", "INFO", "c"),
            rec(4, "q", "WARN", "d"),
        ])
        .unwrap();
        // All four flushed into one Parquet segment.
        assert_eq!(obs.segments_for("q").len(), 1);

        let res = obs
            .search_sql("SELECT severity, count(*) AS n FROM logs GROUP BY severity ORDER BY severity")
            .unwrap();
        assert_eq!(res.columns.len(), 2);
        assert_eq!(res.columns[0].name, "severity");
        // Rows: ERROR=2, INFO=1, WARN=1 (severity-ordered).
        let rows: Vec<(String, i64)> = res
            .rows
            .iter()
            .map(|r| {
                (
                    r[0].as_str().unwrap().to_string(),
                    r[1].as_i64().unwrap(),
                )
            })
            .collect();
        assert_eq!(
            rows,
            vec![
                ("ERROR".to_string(), 2),
                ("INFO".to_string(), 1),
                ("WARN".to_string(), 1),
            ]
        );
    }

    /// SQL also sees the hot (un-flushed) buffer, not just cold segments.
    #[test]
    fn search_sql_includes_hot_buffer() {
        let obs = ObsState::in_memory(1000).unwrap(); // nothing auto-flushes
        obs.ingest(vec![rec(1, "h", "INFO", "hot"), rec(2, "h", "ERROR", "hot2")])
            .unwrap();
        assert!(obs.segments_for("h").is_empty(), "still buffered");
        let res = obs.search_sql("SELECT count(*) AS n FROM logs").unwrap();
        assert_eq!(res.rows[0][0].as_i64().unwrap(), 2);
    }
}
