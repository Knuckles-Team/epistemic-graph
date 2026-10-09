//! Hot-store engine evaluation (EG-DURABLE-KERNEL-R033): comparing redb,
//! fjall, and RocksDB behind the existing storage interface using captured
//! workload traffic. This is the typed model slice (`.1`): the closed engine
//! and workload sets, a validated benchmark result, and a winner-selection
//! decision that refuses rather than silently defaulting when no captured
//! result matches the requested workload. Running the real benchmark and
//! publishing the architecture decision record are later children.

use std::fmt;

use serde::{Deserialize, Serialize};

/// A hot-store storage engine candidate.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum HotStoreEngine {
    Redb,
    Fjall,
    #[serde(rename = "rocksdb")]
    RocksDb,
}

/// A captured-traffic workload the evaluation replays.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkloadProfile {
    RepresentativeApplication,
    Finance,
}

/// One engine's measured result on one workload. Only constructible via
/// [`BenchmarkResult::new`], which refuses a non-finite or non-positive
/// measurement.
#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
pub struct BenchmarkResult {
    pub engine: HotStoreEngine,
    pub workload: WorkloadProfile,
    pub ops_per_second: f64,
    pub p99_latency_ms: f64,
}

/// A benchmark measurement was refused: a non-finite or non-positive value
/// is never a valid captured measurement.
#[derive(Clone, Debug, PartialEq)]
pub struct InvalidBenchmarkResult {
    pub engine: HotStoreEngine,
    pub reason: &'static str,
}

impl fmt::Display for InvalidBenchmarkResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "benchmark result for {:?} refused: {}",
            self.engine, self.reason
        )
    }
}

impl std::error::Error for InvalidBenchmarkResult {}

impl BenchmarkResult {
    pub fn new(
        engine: HotStoreEngine,
        workload: WorkloadProfile,
        ops_per_second: f64,
        p99_latency_ms: f64,
    ) -> Result<Self, InvalidBenchmarkResult> {
        if !ops_per_second.is_finite() || ops_per_second <= 0.0 {
            return Err(InvalidBenchmarkResult {
                engine,
                reason: "ops_per_second must be finite and positive",
            });
        }
        if !p99_latency_ms.is_finite() || p99_latency_ms <= 0.0 {
            return Err(InvalidBenchmarkResult {
                engine,
                reason: "p99_latency_ms must be finite and positive",
            });
        }
        Ok(Self {
            engine,
            workload,
            ops_per_second,
            p99_latency_ms,
        })
    }
}

/// No captured benchmark result matched the requested workload: selection
/// never silently falls back to a default engine.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EngineSelectionRefused(pub WorkloadProfile);

impl fmt::Display for EngineSelectionRefused {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "no captured benchmark result matched workload {:?}",
            self.0
        )
    }
}

impl std::error::Error for EngineSelectionRefused {}

/// Pick the engine with the highest `ops_per_second` among `results` that
/// match `workload`. Refuses when no result matches.
pub fn select_winner(
    workload: WorkloadProfile,
    results: &[BenchmarkResult],
) -> Result<HotStoreEngine, EngineSelectionRefused> {
    results
        .iter()
        .filter(|r| r.workload == workload)
        .max_by(|a, b| a.ops_per_second.total_cmp(&b.ops_per_second))
        .map(|r| r.engine)
        .ok_or(EngineSelectionRefused(workload))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn engines_round_trip_through_their_wire_name() {
        for (engine, name) in [
            (HotStoreEngine::Redb, "redb"),
            (HotStoreEngine::Fjall, "fjall"),
            (HotStoreEngine::RocksDb, "rocksdb"),
        ] {
            let wire = serde_json::to_string(&engine).unwrap();
            assert_eq!(wire, format!("\"{name}\""));
            let back: HotStoreEngine = serde_json::from_str(&wire).unwrap();
            assert_eq!(back, engine);
        }
    }

    #[test]
    fn workloads_round_trip_through_their_wire_name() {
        for (workload, name) in [
            (
                WorkloadProfile::RepresentativeApplication,
                "representative_application",
            ),
            (WorkloadProfile::Finance, "finance"),
        ] {
            let wire = serde_json::to_string(&workload).unwrap();
            assert_eq!(wire, format!("\"{name}\""));
            let back: WorkloadProfile = serde_json::from_str(&wire).unwrap();
            assert_eq!(back, workload);
        }
    }

    #[test]
    fn benchmark_result_refuses_non_positive_throughput() {
        for bad in [0.0, -1.0, f64::NAN, f64::INFINITY] {
            let err =
                BenchmarkResult::new(HotStoreEngine::Redb, WorkloadProfile::Finance, bad, 1.0)
                    .unwrap_err();
            assert_eq!(err.engine, HotStoreEngine::Redb);
        }
    }

    #[test]
    fn select_winner_picks_highest_throughput_for_the_workload() {
        let results = [
            BenchmarkResult::new(HotStoreEngine::Redb, WorkloadProfile::Finance, 1000.0, 2.0)
                .unwrap(),
            BenchmarkResult::new(HotStoreEngine::Fjall, WorkloadProfile::Finance, 5000.0, 1.0)
                .unwrap(),
            BenchmarkResult::new(
                HotStoreEngine::RocksDb,
                WorkloadProfile::Finance,
                3000.0,
                1.5,
            )
            .unwrap(),
        ];
        assert_eq!(
            select_winner(WorkloadProfile::Finance, &results).unwrap(),
            HotStoreEngine::Fjall
        );
    }

    #[test]
    fn select_winner_refuses_when_no_result_matches_the_workload() {
        let results = [BenchmarkResult::new(
            HotStoreEngine::Redb,
            WorkloadProfile::RepresentativeApplication,
            1000.0,
            2.0,
        )
        .unwrap()];
        let err = select_winner(WorkloadProfile::Finance, &results).unwrap_err();
        assert_eq!(err.0, WorkloadProfile::Finance);
    }
}
