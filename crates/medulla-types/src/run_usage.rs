//! Bounded aggregation of native immutable run-completion measurements (R801.7).
//! No prices, inferred counts or client-side authority live in this contract.
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

pub const MAX_USAGE_COUNT: u64 = 9_007_199_254_740_991;
pub const MAX_SESSION_USAGE_RUNS: usize = 10_000;
const COUNT_NAMES: [&str; 5] = [
    "requests",
    "tool_calls",
    "input_tokens",
    "output_tokens",
    "total_tokens",
];

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SessionUsageRecord {
    pub run_id: String,
    pub context_id: String,
    pub tenant: String,
    /// Fingerprint from the native immutable run-owner binding.
    pub principal: String,
    pub counts: BTreeMap<String, u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct SessionUsage {
    pub context_id: String,
    pub inventory_complete: bool,
    pub counts: BTreeMap<String, u64>,
    pub expected_runs: usize,
    pub recorded_runs: usize,
    pub missing_runs: usize,
    /// Counts are partial when not every expected run measured that counter.
    pub count_coverage: BTreeMap<String, usize>,
}

impl SessionUsageRecord {
    pub fn validate(&self) -> Result<(), String> {
        for value in [
            &self.run_id,
            &self.context_id,
            &self.tenant,
            &self.principal,
        ] {
            if value.trim().is_empty() || value.len() > 512 {
                return Err("invalid committed usage identity".into());
            }
        }
        if self.counts.is_empty() || self.counts.len() > COUNT_NAMES.len() {
            return Err("committed usage requires observed counts".into());
        }
        if self
            .counts
            .iter()
            .any(|(key, value)| !COUNT_NAMES.contains(&key.as_str()) || *value > MAX_USAGE_COUNT)
        {
            return Err("invalid observed usage count".into());
        }
        Ok(())
    }
}

/// Aggregate an explicitly bounded inventory of authorized committed runs.
/// A missing record or counter is missing coverage, never an observed zero.
pub fn aggregate_session_usage(
    tenant: &str,
    principal: &str,
    context_id: &str,
    expected_run_ids: &[String],
    records: &[SessionUsageRecord],
) -> Result<SessionUsage, String> {
    if tenant.is_empty()
        || principal.is_empty()
        || context_id.is_empty()
        || expected_run_ids.len() > MAX_SESSION_USAGE_RUNS
        || records.len() > MAX_SESSION_USAGE_RUNS
    {
        return Err("invalid session usage scope or bound".into());
    }
    let expected: BTreeSet<&str> = expected_run_ids.iter().map(String::as_str).collect();
    if expected.iter().any(|id| id.is_empty() || id.len() > 512) {
        return Err("invalid expected run identity".into());
    }
    let unique = unique_session_records(tenant, principal, context_id, &expected, records)?;
    let mut counts = BTreeMap::<String, u64>::new();
    let mut count_coverage = BTreeMap::new();
    for record in unique.values() {
        for (key, value) in &record.counts {
            let sum = counts
                .get(key)
                .copied()
                .unwrap_or(0)
                .checked_add(*value)
                .filter(|sum| *sum <= MAX_USAGE_COUNT)
                .ok_or("session usage overflow")?;
            counts.insert(key.clone(), sum);
            *count_coverage.entry(key.clone()).or_insert(0) += 1;
        }
    }
    Ok(SessionUsage {
        context_id: context_id.into(),
        inventory_complete: true,
        counts,
        expected_runs: expected.len(),
        recorded_runs: unique.len(),
        missing_runs: expected.len() - unique.len(),
        count_coverage,
    })
}

fn unique_session_records<'a>(
    tenant: &str,
    principal: &str,
    context_id: &str,
    expected: &BTreeSet<&str>,
    records: &'a [SessionUsageRecord],
) -> Result<BTreeMap<&'a str, &'a SessionUsageRecord>, String> {
    let mut unique: BTreeMap<&str, &SessionUsageRecord> = BTreeMap::new();
    for record in records {
        record.validate()?;
        if record.tenant != tenant
            || record.principal != principal
            || record.context_id != context_id
            || !expected.contains(record.run_id.as_str())
        {
            return Err("committed usage attribution mismatch".into());
        }
        if let Some(previous) = unique.insert(&record.run_id, record) {
            if previous != record {
                return Err("conflicting committed usage for run".into());
            }
        }
    }
    Ok(unique)
}

// spec: MEDULLA-DECISION-ENGINE-R801.8
#[cfg(test)]
mod tests {
    use super::*;
    fn record(run: &str) -> SessionUsageRecord {
        SessionUsageRecord {
            run_id: run.into(),
            context_id: "context".into(),
            tenant: "tenant".into(),
            principal: "owner".into(),
            counts: BTreeMap::from([("input_tokens".into(), 7)]),
        }
    }
    #[test]
    fn deduplicates_and_preserves_partial_coverage() {
        let one = record("one");
        let result = aggregate_session_usage(
            "tenant",
            "owner",
            "context",
            &["one".into(), "two".into()],
            &[one.clone(), one],
        )
        .unwrap();
        assert_eq!(result.recorded_runs, 1);
        assert_eq!(result.missing_runs, 1);
        assert_eq!(result.counts["input_tokens"], 7);
        assert_eq!(result.count_coverage["input_tokens"], 1);
        assert!(!result.counts.contains_key("output_tokens"));
    }
    #[test]
    fn explicit_zero_is_measured_but_absent_counter_is_unknown() {
        let mut zero = record("one");
        zero.counts = BTreeMap::from([("requests".into(), 0)]);
        let result = aggregate_session_usage(
            "tenant",
            "owner",
            "context",
            &["one".into(), "missing".into()],
            &[zero],
        )
        .unwrap();
        assert_eq!(result.counts.get("requests"), Some(&0));
        assert!(!result.counts.contains_key("input_tokens"));
        assert_eq!(result.count_coverage["requests"], 1);
        assert_eq!(result.recorded_runs, 1);
        assert_eq!(result.missing_runs, 1);
    }

    #[test]
    fn rejects_conflicts_foreign_attribution_and_overflow() {
        let original = record("one");
        for field in ["tenant", "principal", "context_id"] {
            let mut value = serde_json::to_value(&original).unwrap();
            value[field] = "foreign".into();
            let other = serde_json::from_value(value).unwrap();
            assert!(aggregate_session_usage(
                "tenant",
                "owner",
                "context",
                &["one".into()],
                &[other]
            )
            .is_err());
        }
        let mut other = original.clone();
        other.counts.insert("input_tokens".into(), 8);
        assert!(aggregate_session_usage(
            "tenant",
            "owner",
            "context",
            &["one".into()],
            &[original, other]
        )
        .is_err());
        let mut high = record("one");
        high.counts.insert("input_tokens".into(), MAX_USAGE_COUNT);
        assert!(aggregate_session_usage(
            "tenant",
            "owner",
            "context",
            &["one".into(), "two".into()],
            &[high, record("two")]
        )
        .is_err());
    }
    #[test]
    fn bounded_inventory_and_count_validation() {
        let records: Vec<_> = (0..MAX_SESSION_USAGE_RUNS)
            .map(|n| record(&n.to_string()))
            .collect();
        let expected: Vec<_> = records.iter().map(|r| r.run_id.clone()).collect();
        assert_eq!(
            aggregate_session_usage("tenant", "owner", "context", &expected, &records)
                .unwrap()
                .recorded_runs,
            MAX_SESSION_USAGE_RUNS
        );
        let mut bad = record("bad");
        bad.counts.insert("cost".into(), 1);
        assert!(bad.validate().is_err());
        bad.counts = BTreeMap::from([("input_tokens".into(), MAX_USAGE_COUNT + 1)]);
        assert!(bad.validate().is_err());
        assert!(serde_json::from_str::<SessionUsageRecord>(r#"{"run_id":"x","context_id":"c","tenant":"t","principal":"p","counts":{"input_tokens":-1}}"#).is_err());
    }
}
