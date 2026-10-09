//! A published performance-superiority claim (EG-DURABLE-KERNEL-R042): a
//! benchmark report is accepted as a claim that this engine outperforms a
//! named comparison system only when it publishes its configuration AND
//! carries a passing measured result for that exact comparison. This is the
//! typed-model slice (`.1`): the named comparisons, the claim record, and
//! the refusal for an unpublished-configuration or non-passing claim.
//! Running the benchmark suites themselves is a later child.

use std::fmt;

/// One of the named comparisons EG-DURABLE-KERNEL-R042 requires a published
/// benchmark suite for.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum BenchmarkComparison {
    /// YCSB workloads A-F against the async durability class.
    YcsbAsyncDurability,
    /// redis-benchmark p99 against the ephemeral durability class.
    RedisBenchmarkEphemeral,
    /// pgbench and HammerDB TPC-C.
    PgbenchHammerDbTpcc,
    /// LDBC SNB Interactive.
    LdbcSnbInteractive,
    /// A converged graph, vector, SQL and time-series benchmark against
    /// PostgreSQL with AGE, pgvector and TimescaleDB.
    ConvergedGraphVectorSqlTimeseries,
}

/// A benchmark report for one named comparison. Never itself runs a
/// benchmark -- it is the record a benchmark run produces.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BenchmarkClaim {
    pub comparison: BenchmarkComparison,
    /// The exact benchmark configuration (workload parameters, hardware,
    /// versions of the compared systems). Must be non-empty: a claim with no
    /// recorded configuration cannot be reproduced or checked.
    pub configuration: String,
    /// The measured result text (e.g. throughput/latency numbers) for this
    /// exact comparison. Must be non-empty.
    pub measured_result: String,
    /// Whether the measured result actually outperformed the compared
    /// system under the published configuration.
    pub passed: bool,
}

impl BenchmarkClaim {
    /// Accept this report as a superiority claim, or refuse it with the
    /// reason. Never accepts a claim missing its configuration or measured
    /// result, and never accepts a non-passing result as a superiority
    /// claim regardless of how complete its record is.
    pub fn accept_as_superiority_claim(&self) -> Result<(), RefusedBenchmarkClaim> {
        if self.configuration.trim().is_empty() {
            return Err(RefusedBenchmarkClaim::MissingConfiguration);
        }
        if self.measured_result.trim().is_empty() {
            return Err(RefusedBenchmarkClaim::MissingMeasuredResult);
        }
        if !self.passed {
            return Err(RefusedBenchmarkClaim::DidNotPass);
        }
        Ok(())
    }
}

/// Why a benchmark report was refused as a superiority claim.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RefusedBenchmarkClaim {
    MissingConfiguration,
    MissingMeasuredResult,
    DidNotPass,
}

impl fmt::Display for RefusedBenchmarkClaim {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let reason = match self {
            Self::MissingConfiguration => "no published configuration",
            Self::MissingMeasuredResult => "no recorded measured result",
            Self::DidNotPass => "measured result did not pass the comparison",
        };
        write!(
            f,
            "benchmark report refused as a superiority claim: {reason}"
        )
    }
}

impl std::error::Error for RefusedBenchmarkClaim {}

#[cfg(test)]
mod tests {
    use super::*;

    fn claim(configuration: &str, measured_result: &str, passed: bool) -> BenchmarkClaim {
        BenchmarkClaim {
            comparison: BenchmarkComparison::YcsbAsyncDurability,
            configuration: configuration.to_string(),
            measured_result: measured_result.to_string(),
            passed,
        }
    }

    #[test]
    fn a_published_passing_report_is_accepted() {
        claim("workload A, 8 shards", "p99 3.2ms vs 5.1ms", true)
            .accept_as_superiority_claim()
            .unwrap();
    }

    #[test]
    fn missing_configuration_is_refused() {
        let err = claim("", "p99 3.2ms", true)
            .accept_as_superiority_claim()
            .unwrap_err();
        assert_eq!(err, RefusedBenchmarkClaim::MissingConfiguration);
    }

    #[test]
    fn missing_measured_result_is_refused() {
        let err = claim("workload A", "   ", true)
            .accept_as_superiority_claim()
            .unwrap_err();
        assert_eq!(err, RefusedBenchmarkClaim::MissingMeasuredResult);
    }

    #[test]
    fn a_non_passing_report_is_never_a_superiority_claim() {
        let err = claim("workload A", "p99 9.9ms vs 5.1ms", false)
            .accept_as_superiority_claim()
            .unwrap_err();
        assert_eq!(err, RefusedBenchmarkClaim::DidNotPass);
        assert!(err.to_string().contains("did not pass"));
    }
}
