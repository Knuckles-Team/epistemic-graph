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

/// Drop a registry name from the optimizer's trusted `<kind>[:name]#<8 hex>` label.
/// Fail closed for malformed labels rather than echoing arbitrary caller text.
pub(crate) fn redacted_label(label: &str) -> String {
    let Some((prefix, digest)) = label.rsplit_once('#') else {
        return "source#unknown".into();
    };
    if digest.len() != 8 || !digest.bytes().all(|b| b.is_ascii_hexdigit()) {
        return "source#unknown".into();
    }
    let kind = prefix.split(':').next().unwrap_or_default();
    if !matches!(
        kind,
        "named"
            | "registered"
            | "remote-engine"
            | "http-json"
            | "sql"
            | "trino"
            | "neo4j"
            | "age"
            | "falkordb"
            | "spark-batch"
    ) {
        return "source#unknown".into();
    }
    format!("{kind}#{digest}")
}

/// One line per fragment for a log/EXPLAIN view, without registered source names.
pub fn render_trace(trace: &[FragmentTrace]) -> String {
    trace
        .iter()
        .map(|t| {
            format!(
                "{} {:?} keys={} limit={:?} requests={} fetched={} kept={} ms={} estimate={:?}",
                redacted_label(&t.source),
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

#[cfg(test)]
mod tests {
    use super::{render_trace, FetchStrategy, FragmentTrace};

    // spec: EG-FEDERATED-QUERY-R044
    #[test]
    fn telemetry_trace_redacts_registered_names_and_keeps_counts() {
        let mut fragment = FragmentTrace::new(
            "registered:https://user:redact-me@example.invalid/db#deadbeef".into(),
            FetchStrategy::BindJoin,
        );
        fragment.keys_pushed = 3;
        fragment.requests = 2;
        fragment.rows_fetched = 5;
        fragment.rows_kept = 4;
        let output = render_trace(&[fragment]);
        assert!(
            output.starts_with("registered#deadbeef BindJoin"),
            "{output}"
        );
        assert!(output.contains("keys=3 limit=None requests=2 fetched=5 kept=4"));
        assert!(!output.contains("redact-me") && !output.contains("example.invalid"));
    }

    // spec: EG-FEDERATED-QUERY-R044
    #[test]
    fn telemetry_trace_fails_closed_for_malformed_source_labels() {
        let fragment = FragmentTrace::new("untrusted:redact-me".into(), FetchStrategy::FullFetch);
        let output = render_trace(&[fragment]);
        assert!(output.starts_with("source#unknown FullFetch"), "{output}");
        assert!(!output.contains("redact-me"));
    }
}
