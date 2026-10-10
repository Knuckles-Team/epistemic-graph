//! Typed model for the Postgres-equivalence differential harness's deviation
//! baseline (EG-UNIFIED-DATA-PLANE-R028): a run's deviations compared
//! against the prior run's recorded baseline, so a deviation can be marked
//! resolved only by naming who resolved it, never by silently disappearing
//! from the report. This is the typed-model slice (`.1`): the deviation
//! source/record shape and `reconcile`'s refusal for a baseline update that
//! drops a deviation unaccounted for. Running the regression suite,
//! SQLancer-style generation, and the captured-traffic corpus are later
//! children.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// Where a differential case came from. Closed — the harness's three
/// declared corpora.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DifferentialSource {
    PostgresRegressionSuite,
    SqlancerGenerated,
    CapturedApplicationTraffic,
}

/// One named, owned, reproducible difference between EG and real Postgres.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PostgresDeviation {
    pub name: String,
    pub source: DifferentialSource,
    pub owner: String,
    pub reproducing_fixture: String,
}

/// A run's full set of deviations, keyed by name (the stable identity a
/// later run's baseline is reconciled against).
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeviationBaseline {
    pub deviations: BTreeMap<String, PostgresDeviation>,
}

/// A baseline update dropped a deviation the prior baseline carried without
/// naming who resolved it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UnaccountedDeviationDrop {
    pub names: Vec<String>,
}

impl std::fmt::Display for UnaccountedDeviationDrop {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "baseline update dropped deviation(s) with no resolution record: {:?}",
            self.names
        )
    }
}

impl std::error::Error for UnaccountedDeviationDrop {}

impl DeviationBaseline {
    /// Reconcile this (prior) baseline against `current`, the new run's
    /// observed deviations, given `resolved`: the names of deviations a
    /// human has explicitly recorded as fixed since the prior run. Refuses
    /// when `current` is missing a prior deviation that is NOT in
    /// `resolved` -- a deviation can disappear from the report only by
    /// explicit resolution, never by the next run simply not reproducing it
    /// (a flaky harness, a scope narrowing, or a silent regression-hiding
    /// bug all look identical to "it vanished").
    pub fn reconcile(
        &self,
        current: &DeviationBaseline,
        resolved: &[String],
    ) -> Result<(), UnaccountedDeviationDrop> {
        let dropped: Vec<String> = self
            .deviations
            .keys()
            .filter(|name| !current.deviations.contains_key(*name) && !resolved.contains(name))
            .cloned()
            .collect();
        if dropped.is_empty() {
            Ok(())
        } else {
            Err(UnaccountedDeviationDrop { names: dropped })
        }
    }
}

/// One corpus source's evaluator, pluggable so the differential harness
/// runner (`EG-UNIFIED-DATA-PLANE-R028.2.1`) needs no live Postgres
/// instance to run against. Needs no live service: driven against fake
/// evaluators in tests.
pub trait DifferentialCaseEvaluator {
    /// This evaluator's corpus source.
    fn source(&self) -> DifferentialSource;
    /// Every deviation this source's corpus found against EG, this run.
    fn evaluate(&self) -> Vec<PostgresDeviation>;
}

/// Run the differential harness across one evaluator per declared corpus
/// source, assembling this run's `DeviationBaseline` and reconciling it
/// against `prior` (`EG-UNIFIED-DATA-PLANE-R028.2.1`). Bubbles
/// `reconcile`'s refusal when this run drops a prior deviation that
/// `resolved` does not name.
pub fn run_differential_harness(
    evaluators: &[&dyn DifferentialCaseEvaluator],
    prior: &DeviationBaseline,
    resolved: &[String],
) -> Result<DeviationBaseline, UnaccountedDeviationDrop> {
    let mut current = DeviationBaseline::default();
    for evaluator in evaluators {
        for deviation in evaluator.evaluate() {
            current.deviations.insert(deviation.name.clone(), deviation);
        }
    }
    prior.reconcile(&current, resolved)?;
    Ok(current)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn deviation(name: &str) -> PostgresDeviation {
        PostgresDeviation {
            name: name.to_string(),
            source: DifferentialSource::PostgresRegressionSuite,
            owner: "alice".to_string(),
            reproducing_fixture: format!("tests/fixtures/{name}.sql"),
        }
    }

    fn baseline(names: &[&str]) -> DeviationBaseline {
        DeviationBaseline {
            deviations: names
                .iter()
                .map(|name| (name.to_string(), deviation(name)))
                .collect(),
        }
    }

    #[test]
    fn identical_baseline_reconciles() {
        let prior = baseline(&["timestamp_precision"]);
        let current = baseline(&["timestamp_precision"]);
        assert_eq!(prior.reconcile(&current, &[]), Ok(()));
    }

    #[test]
    fn new_deviation_in_current_is_not_a_problem() {
        let prior = baseline(&["timestamp_precision"]);
        let current = baseline(&["timestamp_precision", "collation_order"]);
        assert_eq!(prior.reconcile(&current, &[]), Ok(()));
    }

    #[test]
    fn dropped_deviation_without_resolution_is_refused() {
        let prior = baseline(&["timestamp_precision", "collation_order"]);
        let current = baseline(&["timestamp_precision"]);
        let err = prior.reconcile(&current, &[]).unwrap_err();
        assert_eq!(err.names, vec!["collation_order".to_string()]);
    }

    #[test]
    fn dropped_deviation_with_explicit_resolution_reconciles() {
        let prior = baseline(&["timestamp_precision", "collation_order"]);
        let current = baseline(&["timestamp_precision"]);
        assert_eq!(
            prior.reconcile(&current, &["collation_order".to_string()]),
            Ok(())
        );
    }

    #[test]
    fn empty_current_against_nonempty_prior_is_refused() {
        let prior = baseline(&["a", "b"]);
        let current = DeviationBaseline::default();
        let err = prior.reconcile(&current, &[]).unwrap_err();
        assert_eq!(err.names.len(), 2);
    }

    #[test]
    fn deviation_serializes_round_trip() {
        let dev = deviation("regex_posix_class");
        let encoded = serde_json::to_string(&dev).unwrap();
        let decoded: PostgresDeviation = serde_json::from_str(&encoded).unwrap();
        assert_eq!(decoded, dev);
    }

    struct FakeEvaluator {
        source: DifferentialSource,
        found: Vec<&'static str>,
    }

    impl DifferentialCaseEvaluator for FakeEvaluator {
        fn source(&self) -> DifferentialSource {
            self.source
        }

        fn evaluate(&self) -> Vec<PostgresDeviation> {
            self.found.iter().map(|name| deviation(name)).collect()
        }
    }

    // spec: EG-UNIFIED-DATA-PLANE-R028.2.1
    #[test]
    fn harness_merges_deviations_from_every_declared_source() {
        let regression = FakeEvaluator {
            source: DifferentialSource::PostgresRegressionSuite,
            found: vec!["timestamp_precision"],
        };
        let sqlancer = FakeEvaluator {
            source: DifferentialSource::SqlancerGenerated,
            found: vec!["collation_order"],
        };
        let traffic = FakeEvaluator {
            source: DifferentialSource::CapturedApplicationTraffic,
            found: vec![],
        };
        let prior = DeviationBaseline::default();
        let current =
            run_differential_harness(&[&regression, &sqlancer, &traffic], &prior, &[]).unwrap();
        assert_eq!(current.deviations.len(), 2);
        assert!(current.deviations.contains_key("timestamp_precision"));
        assert!(current.deviations.contains_key("collation_order"));
    }

    // spec: EG-UNIFIED-DATA-PLANE-R028.2.1
    #[test]
    fn harness_refuses_when_an_evaluator_drops_a_prior_deviation_unaccounted() {
        let regression = FakeEvaluator {
            source: DifferentialSource::PostgresRegressionSuite,
            found: vec![],
        };
        let prior = baseline(&["timestamp_precision"]);
        let err = run_differential_harness(&[&regression], &prior, &[]).unwrap_err();
        assert_eq!(err.names, vec!["timestamp_precision".to_string()]);
    }

    // spec: EG-UNIFIED-DATA-PLANE-R028.2.1
    #[test]
    fn harness_accepts_a_dropped_deviation_with_explicit_resolution() {
        let regression = FakeEvaluator {
            source: DifferentialSource::PostgresRegressionSuite,
            found: vec![],
        };
        let prior = baseline(&["timestamp_precision"]);
        let current =
            run_differential_harness(&[&regression], &prior, &["timestamp_precision".to_string()])
                .unwrap();
        assert!(current.deviations.is_empty());
    }
}
