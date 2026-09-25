//! Learned per-source statistics (design §4.4): what full fetches of a source returned and
//! how long its requests took, keyed by the source's fingerprint — the SHA-256 of its full
//! spec, so identical specs (same credential) share statistics and a different credential is
//! a different source. The in-memory cache is bounded. A credential-free
//! `SourceStatisticsFact` can cross the durable graph boundary explicitly.

use std::collections::HashMap;
use std::sync::{OnceLock, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

use eg_types::wire::ForeignSourceSpec;
use serde::{Deserialize, Serialize};

/// A source's identity for statistics and traces.
pub(crate) type Fingerprint = [u8; 32];

/// EWMA smoothing: recent observations dominate.
const ALPHA: f64 = 0.3;
/// Bound on distinct sources remembered; the least recently observed is evicted.
const MAX_SOURCES: usize = 4096;

/// What past executions observed about one source.
#[derive(Clone, Copy, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct SourceStats {
    /// EWMA of rows a full fetch returned (`0.0` until the first full fetch).
    pub ewma_full_rows: f64,
    /// Full fetches observed.
    pub full_samples: u64,
    /// EWMA of milliseconds per remote request.
    pub ewma_request_ms: f64,
    /// Requests observed.
    pub request_samples: u64,
    /// Key-lookup fragments that failed and fell back to a full fetch.
    pub key_lookup_failures: u32,
    /// Unix milliseconds of the latest observation.
    pub last_observed_unix_ms: u64,
    /// Validated registration-time catalog estimate, if one was available.
    /// It is weaker than a real full-fetch observation and never changes rows.
    pub catalog_rows: Option<f64>,
}

/// A credential-free fact payload for a durable `eg:SourceStatistics` node.
/// `run_id` identifies the registration probe or execution that produced it.
/// The server owns placement in an owner-scoped graph; this type carries no
/// endpoint, DSN, source name, or credential.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SourceStatisticsFact {
    pub source_fingerprint: String,
    pub run_id: String,
    pub stats: SourceStats,
}

impl SourceStatisticsFact {
    /// Construct a portable fact from a full source fingerprint and run id.
    pub fn new(
        source_fingerprint: [u8; 32],
        run_id: String,
        stats: SourceStats,
    ) -> Result<Self, String> {
        if run_id.is_empty() || run_id.len() > 256 || !run_id.is_ascii() {
            return Err("federation: statistics fact needs a bounded ASCII run id".into());
        }
        if !valid_stats(&stats) {
            return Err("federation: statistics fact contains an invalid estimate".into());
        }
        Ok(Self {
            source_fingerprint: hex::encode(source_fingerprint),
            run_id,
            stats,
        })
    }

    /// Validate a decoded graph fact before it can influence a plan.
    pub fn validated_fingerprint(&self) -> Result<[u8; 32], String> {
        if self.run_id.is_empty()
            || self.run_id.len() > 256
            || !self.run_id.is_ascii()
            || !valid_stats(&self.stats)
        {
            return Err("federation: invalid statistics fact".into());
        }
        let bytes = hex::decode(&self.source_fingerprint)
            .map_err(|_| "federation: invalid statistics fingerprint")?;
        bytes
            .try_into()
            .map_err(|_| "federation: invalid statistics fingerprint".into())
    }
}

fn valid_stats(stats: &SourceStats) -> bool {
    stats.ewma_full_rows.is_finite()
        && stats.ewma_full_rows >= 0.0
        && stats.ewma_request_ms.is_finite()
        && stats.ewma_request_ms >= 0.0
        && stats
            .catalog_rows
            .is_none_or(|rows| rows.is_finite() && rows >= 0.0)
}

impl SourceStats {
    fn ewma(current: f64, samples: u64, value: f64) -> f64 {
        if samples == 0 {
            value
        } else {
            ALPHA * value + (1.0 - ALPHA) * current
        }
    }
}

/// The SHA-256 fingerprint of a label (a spec's canonical bytes, or `named:<name>`).
pub(crate) fn fingerprint(bytes: &[u8]) -> Fingerprint {
    use sha2::{Digest, Sha256};
    Sha256::digest(bytes).into()
}

/// Full-spec identity, including credential, used by the runtime optimizer.
pub fn source_fingerprint(spec: &ForeignSourceSpec) -> [u8; 32] {
    fingerprint(&rmp_serde::to_vec_named(spec).unwrap_or_default())
}

/// Bounded registration-time PostgreSQL plan estimate. Failure is advisory:
/// the optimizer will use a learned full-fetch observation, or its default.
/// `EXPLAIN` does not execute the query and lets arbitrary validated read-only
/// SELECT statements produce a cardinality estimate without a full table scan.
#[cfg(feature = "federation-sql")]
pub async fn probe_postgres_rows(spec: &ForeignSourceSpec) -> Option<f64> {
    use sqlx::Connection;
    use std::time::Duration;

    let ForeignSourceSpec::Sql { dsn, query, .. } = spec else {
        return None;
    };
    if !dsn.starts_with("postgres://") && !dsn.starts_with("postgresql://") {
        return None;
    }
    crate::federation::validate_federated_sql(query, crate::sql_text::SqlDialect::Postgres).ok()?;
    crate::federation_ssrf::check_sql_dsn(dsn).ok()?;
    let statement = format!(
        "EXPLAIN (FORMAT TEXT) {}",
        query.trim().trim_end_matches(';')
    );
    let plan = tokio::time::timeout(Duration::from_secs(2), async {
        let mut conn = sqlx::postgres::PgConnection::connect(dsn).await.ok()?;
        let mut tx = conn.begin().await.ok()?;
        sqlx::query("SET TRANSACTION READ ONLY")
            .execute(&mut *tx)
            .await
            .ok()?;
        let first_line: Option<String> = sqlx::query_scalar(sqlx::AssertSqlSafe(statement))
            .fetch_optional(&mut *tx)
            .await
            .ok()?;
        tx.rollback().await.ok()?;
        first_line.and_then(|line| parse_explain_rows(&line))
    })
    .await
    .ok()
    .flatten()?;
    Some(plan)
}

#[cfg(feature = "federation-sql")]
fn parse_explain_rows(line: &str) -> Option<f64> {
    let (_, after) = line.split_once(" rows=")?;
    let number = after.split(|ch: char| !ch.is_ascii_digit()).next()?;
    number.parse::<f64>().ok().filter(|rows| rows.is_finite())
}

/// The first 8 hex digits of a fingerprint — enough to tell sources apart in a trace.
pub(crate) fn short(fp: &Fingerprint) -> String {
    hex::encode(&fp[..4])
}

fn store() -> &'static RwLock<HashMap<Fingerprint, SourceStats>> {
    static STORE: OnceLock<RwLock<HashMap<Fingerprint, SourceStats>>> = OnceLock::new();
    STORE.get_or_init(|| RwLock::new(HashMap::new()))
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
}

/// The learned statistics of `fp`, if any execution observed it.
/// Statistics are advisory: a poisoned store (a panic mid-update) is simply not consulted
/// or updated again, which degrades to the default estimates — never a wrong answer.
pub(crate) fn lookup(fp: &Fingerprint) -> Option<SourceStats> {
    store().read().ok()?.get(fp).copied()
}

/// Seed a source from a bounded, read-only registration probe. A failed or
/// unavailable probe must call nothing, leaving strategy provenance `Default`.
pub fn observe_catalog_estimate(source_fingerprint: [u8; 32], rows: f64) -> bool {
    if !rows.is_finite() || rows < 0.0 {
        return false;
    }
    observe(&source_fingerprint, |stats| stats.catalog_rows = Some(rows));
    true
}

/// Restore a server-verified graph fact after an engine restart. Execution
/// observations from this process retain precedence over the restored estimate.
pub fn restore_statistics_fact(fact: &SourceStatisticsFact) -> Result<(), String> {
    let fingerprint = fact.validated_fingerprint()?;
    observe(&fingerprint, |current| {
        if current.full_samples == 0 {
            *current = fact.stats;
        } else if current.catalog_rows.is_none() {
            current.catalog_rows = fact.stats.catalog_rows;
        }
    });
    Ok(())
}

/// Snapshot one source's observations for an owner-scoped graph write. The
/// caller supplies the run identity; missing statistics produce no fact.
pub fn statistics_fact(
    source_fingerprint: [u8; 32],
    run_id: String,
) -> Result<Option<SourceStatisticsFact>, String> {
    lookup(&source_fingerprint)
        .map(|stats| SourceStatisticsFact::new(source_fingerprint, run_id, stats))
        .transpose()
}

/// Apply `update` to `fp`'s statistics (creating them, evicting the stalest when full).
fn observe(fp: &Fingerprint, update: impl FnOnce(&mut SourceStats)) {
    let Ok(mut map) = store().write() else {
        return;
    };
    if !map.contains_key(fp) && map.len() >= MAX_SOURCES {
        let stalest = map
            .iter()
            .min_by_key(|(_, s)| s.last_observed_unix_ms)
            .map(|(k, _)| *k);
        if let Some(k) = stalest {
            map.remove(&k);
        }
    }
    let stats = map.entry(*fp).or_default();
    update(stats);
    stats.last_observed_unix_ms = now_ms();
}

/// A full fetch of `fp` returned `rows` rows.
pub(crate) fn observe_full_fetch(fp: &Fingerprint, rows: usize) {
    observe(fp, |s| {
        s.ewma_full_rows = SourceStats::ewma(s.ewma_full_rows, s.full_samples, rows as f64);
        s.full_samples += 1;
    });
}

/// One request to `fp` took `ms` milliseconds.
pub(crate) fn observe_request(fp: &Fingerprint, ms: u64) {
    observe(fp, |s| {
        s.ewma_request_ms = SourceStats::ewma(s.ewma_request_ms, s.request_samples, ms as f64);
        s.request_samples += 1;
    });
}

/// A key-lookup fragment against `fp` failed and fell back.
pub(crate) fn observe_key_failure(fp: &Fingerprint) {
    observe(fp, |s| {
        s.key_lookup_failures = s.key_lookup_failures.saturating_add(1)
    });
}

/// Every source's statistics, labelled by its short fingerprint, sorted — the queryable
/// artifact behind EXPLAIN's "estimate provenance".
pub fn stats_snapshot() -> Vec<(String, SourceStats)> {
    let Ok(map) = store().read() else {
        return Vec::new();
    };
    let mut out: Vec<(String, SourceStats)> = map.iter().map(|(k, v)| (short(k), *v)).collect();
    out.sort_by(|a, b| a.0.cmp(&b.0));
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn statistics_fact_round_trip_has_full_fingerprint_and_run_provenance() {
        let fp = fingerprint(b"eh575-test-source");
        let stats = SourceStats {
            catalog_rows: Some(25.0),
            ..SourceStats::default()
        };
        let fact = SourceStatisticsFact::new(fp, "registration:run-42".into(), stats).unwrap();
        let bytes = rmp_serde::to_vec_named(&fact).unwrap();
        let decoded: SourceStatisticsFact = rmp_serde::from_slice(&bytes).unwrap();
        assert_eq!(decoded.validated_fingerprint().unwrap(), fp);
        assert_eq!(decoded.run_id, "registration:run-42");
        restore_statistics_fact(&decoded).unwrap();
        assert_eq!(lookup(&fp).unwrap().catalog_rows, Some(25.0));
        assert_eq!(
            statistics_fact(fp, "run:next".into())
                .unwrap()
                .unwrap()
                .run_id,
            "run:next"
        );
    }

    #[test]
    fn poisoned_or_invalid_estimates_are_refused() {
        let fp = fingerprint(b"eh575-invalid-source");
        assert!(!observe_catalog_estimate(fp, f64::NAN));
        assert!(lookup(&fp).is_none());
        let mut fact = SourceStatisticsFact::new(fp, "run".into(), SourceStats::default()).unwrap();
        fact.stats.catalog_rows = Some(f64::INFINITY);
        assert!(restore_statistics_fact(&fact).is_err());
        fact.stats.catalog_rows = None;
        fact.source_fingerprint = "bad".into();
        assert!(restore_statistics_fact(&fact).is_err());
    }

    #[cfg(feature = "federation-sql")]
    #[test]
    fn postgres_plan_estimate_parser_ignores_non_cardinality_text() {
        assert_eq!(
            parse_explain_rows("Seq Scan on records  (cost=0.00..35.50 rows=2048 width=8)"),
            Some(2048.0)
        );
        assert_eq!(parse_explain_rows("Planning Time: 0.1 ms"), None);
        assert_eq!(parse_explain_rows("rows=999"), None);
    }
}
