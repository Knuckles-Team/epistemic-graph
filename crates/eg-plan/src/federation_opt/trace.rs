//! What crossed the network, per remote fragment (design §7) — the EXPLAIN/PROFILE record.

use serde::Serialize;

/// How a fragment was fetched.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub enum FetchStrategy {
    /// The whole source, then local evaluation (no capability, or cheaper by estimate).
    FullFetch,
    /// A `Limit k` pushed into the source's request.
    LimitPushdown,
    /// A pushed limit returned fewer distinct ids than `k`; refilled from a full fetch.
    LimitRefill,
    /// Read page by page.
    Paged,
    /// Local ids shipped as batched key lookups.
    BindJoin,
    /// A pushed request failed; the fragment fell back to the naive full fetch.
    FallbackFullFetch,
}

/// Where the cardinality estimate behind a strategy choice came from.
#[derive(Clone, Copy, Debug, PartialEq, Serialize)]
pub enum EstimateProvenance {
    /// No estimate was needed (a source op, or no capability to choose between).
    NotNeeded,
    /// No execution had observed the source yet.
    Default,
    /// Learned from `samples` past full fetches averaging `rows` rows.
    Learned { samples: u64, rows: f64 },
}

/// One remote fragment of one query. Never carries a URL, DSN, credential or key value —
/// the source is named by kind, registered name and an 8-hex fingerprint.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct FragmentTrace {
    /// `<kind>[:<registered name>]#<fingerprint>`.
    pub source: String,
    pub strategy: FetchStrategy,
    /// Distinct local ids shipped as keys (bind join).
    pub keys_pushed: usize,
    /// The limit pushed into the source, if any.
    pub limit_pushed: Option<usize>,
    /// Round trips.
    pub requests: u32,
    /// Rows received from the source.
    pub rows_fetched: usize,
    /// Rows the fragment handed to the rest of the plan.
    pub rows_kept: usize,
    /// Wall time spent in the fragment.
    pub elapsed_ms: u64,
    pub estimate: EstimateProvenance,
}

impl FragmentTrace {
    pub(crate) fn new(source: String, strategy: FetchStrategy) -> Self {
        Self {
            source,
            strategy,
            keys_pushed: 0,
            limit_pushed: None,
            requests: 0,
            rows_fetched: 0,
            rows_kept: 0,
            elapsed_ms: 0,
            estimate: EstimateProvenance::NotNeeded,
        }
    }
}

/// One line per fragment for a log/EXPLAIN view.
pub fn render_trace(trace: &[FragmentTrace]) -> String {
    trace
        .iter()
        .map(|t| {
            format!(
                "{} {:?} keys={} limit={:?} requests={} fetched={} kept={} ms={} estimate={:?}",
                t.source,
                t.strategy,
                t.keys_pushed,
                t.limit_pushed,
                t.requests,
                t.rows_fetched,
                t.rows_kept,
                t.elapsed_ms,
                t.estimate
            )
        })
        .collect::<Vec<_>>()
        .join("\n")
}
