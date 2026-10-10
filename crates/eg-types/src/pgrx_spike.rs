//! Typed model for the pgrx companion-extension spike's go/no-go decision
//! (EG-UNIFIED-DATA-PLANE-R025): the spike's declared evaluation scope and
//! its recorded decision. This is the typed-model slice (`.1`): the closed
//! scope-area vocabulary and the refusal for a decision recorded without
//! evidence for every declared area. `.2.1` adds the spike runner that
//! assembles a decision from one evidence source per declared area, driven
//! against fake evaluators so collecting the runner's coverage needs no
//! live pgrx extension or Postgres instance. Running the real spike inside
//! pgrx/Postgres and the architecture-decision-record review remain later
//! children.

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

/// One area the pgrx spike must evaluate before a decision is recorded.
/// Closed per EG-UNIFIED-DATA-PLANE-R025's defined scope.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PgrxSpikeArea {
    SparqlOverTables,
    VectorSearch,
    TimeSeriesKernel,
    WaitForPositionHelper,
}

impl PgrxSpikeArea {
    /// Every area the spike's defined scope requires evidence for.
    pub const ALL: [Self; 4] = [
        Self::SparqlOverTables,
        Self::VectorSearch,
        Self::TimeSeriesKernel,
        Self::WaitForPositionHelper,
    ];
}

/// The spike's recorded outcome.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PgrxSpikeOutcome {
    Go,
    NoGo,
}

/// One area's evaluation result: a short note on what the spike found.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PgrxSpikeEvidence {
    pub area: PgrxSpikeArea,
    pub finding: String,
}

/// The spike's full decision record: every area's evidence plus the
/// recorded outcome.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PgrxSpikeDecision {
    pub evidence: Vec<PgrxSpikeEvidence>,
    pub outcome: Option<PgrxSpikeOutcome>,
}

/// A decision was recorded without evidence for every declared scope area,
/// or with no outcome at all.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum InvalidPgrxDecision {
    MissingAreas(Vec<PgrxSpikeArea>),
    NoOutcome,
}

impl std::fmt::Display for InvalidPgrxDecision {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MissingAreas(areas) => write!(f, "decision missing evidence for: {areas:?}"),
            Self::NoOutcome => write!(f, "decision records no outcome"),
        }
    }
}

impl std::error::Error for InvalidPgrxDecision {}

impl PgrxSpikeDecision {
    /// Confirm every declared scope area has evidence and an outcome is
    /// recorded. Refuses a decision scoped narrower than R025 defines rather
    /// than approving it on partial evidence.
    pub fn validate(&self) -> Result<PgrxSpikeOutcome, InvalidPgrxDecision> {
        let covered: BTreeSet<PgrxSpikeArea> = self.evidence.iter().map(|item| item.area).collect();
        let missing: Vec<PgrxSpikeArea> = PgrxSpikeArea::ALL
            .into_iter()
            .filter(|area| !covered.contains(area))
            .collect();
        if !missing.is_empty() {
            return Err(InvalidPgrxDecision::MissingAreas(missing));
        }
        self.outcome.ok_or(InvalidPgrxDecision::NoOutcome)
    }
}

/// Evaluates one declared pgrx spike scope area and reports its evidence.
/// Abstracted as a trait so the spike runner is unit-tested against fake
/// evaluators, with no live pgrx extension or Postgres instance required.
pub trait PgrxSpikeAreaEvaluator {
    /// The scope area this evaluator covers.
    fn area(&self) -> PgrxSpikeArea;
    /// The finding this evaluator produced for its area.
    fn evaluate(&self) -> String;
}

/// Run the spike across every evaluator supplied, then record the given
/// outcome. This is the runner slice (`EG-UNIFIED-DATA-PLANE-R025.2.1`):
/// it assembles a `PgrxSpikeDecision` from one evidence source per area and
/// defers to `PgrxSpikeDecision::validate` for the full-scope refusal, so a
/// caller supplying evaluators for fewer than all declared areas gets the
/// same named-missing-area error as a hand-built decision would.
pub fn run_spike(
    evaluators: &[&dyn PgrxSpikeAreaEvaluator],
    outcome: Option<PgrxSpikeOutcome>,
) -> Result<PgrxSpikeDecision, InvalidPgrxDecision> {
    let decision = PgrxSpikeDecision {
        evidence: evaluators
            .iter()
            .map(|evaluator| PgrxSpikeEvidence {
                area: evaluator.area(),
                finding: evaluator.evaluate(),
            })
            .collect(),
        outcome,
    };
    decision.validate().map(|_| decision)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A fake area evaluator: returns a fixed finding for a fixed area, so
    /// spike-runner tests need no live pgrx extension or Postgres instance.
    struct FakeEvaluator {
        area: PgrxSpikeArea,
        finding: &'static str,
    }

    impl PgrxSpikeAreaEvaluator for FakeEvaluator {
        fn area(&self) -> PgrxSpikeArea {
            self.area
        }

        fn evaluate(&self) -> String {
            self.finding.to_string()
        }
    }

    fn fake_evaluators() -> Vec<FakeEvaluator> {
        PgrxSpikeArea::ALL
            .into_iter()
            .map(|area| FakeEvaluator {
                area,
                finding: "evaluated by fake evaluator",
            })
            .collect()
    }

    fn evidence(area: PgrxSpikeArea) -> PgrxSpikeEvidence {
        PgrxSpikeEvidence {
            area,
            finding: "evaluated".to_string(),
        }
    }

    fn full_evidence() -> Vec<PgrxSpikeEvidence> {
        PgrxSpikeArea::ALL.into_iter().map(evidence).collect()
    }

    #[test]
    fn complete_decision_validates_with_its_outcome() {
        let decision = PgrxSpikeDecision {
            evidence: full_evidence(),
            outcome: Some(PgrxSpikeOutcome::NoGo),
        };
        assert_eq!(decision.validate(), Ok(PgrxSpikeOutcome::NoGo));
    }

    #[test]
    fn missing_area_is_refused_and_named() {
        let mut evidence = full_evidence();
        evidence.pop();
        let decision = PgrxSpikeDecision {
            evidence,
            outcome: Some(PgrxSpikeOutcome::Go),
        };
        let err = decision.validate().unwrap_err();
        assert_eq!(
            err,
            InvalidPgrxDecision::MissingAreas(vec![PgrxSpikeArea::WaitForPositionHelper])
        );
    }

    #[test]
    fn decision_without_outcome_is_refused() {
        let decision = PgrxSpikeDecision {
            evidence: full_evidence(),
            outcome: None,
        };
        assert_eq!(decision.validate(), Err(InvalidPgrxDecision::NoOutcome));
    }

    #[test]
    fn empty_decision_names_every_missing_area() {
        let decision = PgrxSpikeDecision::default();
        match decision.validate() {
            Err(InvalidPgrxDecision::MissingAreas(areas)) => assert_eq!(areas.len(), 4),
            other => panic!("expected MissingAreas, got {other:?}"),
        }
    }

    #[test]
    fn decision_serializes_round_trip() {
        let decision = PgrxSpikeDecision {
            evidence: full_evidence(),
            outcome: Some(PgrxSpikeOutcome::Go),
        };
        let encoded = serde_json::to_string(&decision).unwrap();
        let decoded: PgrxSpikeDecision = serde_json::from_str(&encoded).unwrap();
        assert_eq!(decoded, decision);
    }

    // spec: EG-UNIFIED-DATA-PLANE-R025.2.1
    #[test]
    fn run_spike_assembles_a_valid_decision_from_every_declared_area() {
        let evaluators = fake_evaluators();
        let refs: Vec<&dyn PgrxSpikeAreaEvaluator> = evaluators
            .iter()
            .map(|e| e as &dyn PgrxSpikeAreaEvaluator)
            .collect();
        let decision = run_spike(&refs, Some(PgrxSpikeOutcome::Go)).unwrap();
        assert_eq!(decision.evidence.len(), PgrxSpikeArea::ALL.len());
        assert_eq!(decision.validate(), Ok(PgrxSpikeOutcome::Go));
    }

    // spec: EG-UNIFIED-DATA-PLANE-R025.2.1
    #[test]
    fn run_spike_refuses_a_decision_missing_an_area() {
        let evaluators = fake_evaluators();
        let refs: Vec<&dyn PgrxSpikeAreaEvaluator> = evaluators[..evaluators.len() - 1]
            .iter()
            .map(|e| e as &dyn PgrxSpikeAreaEvaluator)
            .collect();
        let err = run_spike(&refs, Some(PgrxSpikeOutcome::Go)).unwrap_err();
        assert_eq!(
            err,
            InvalidPgrxDecision::MissingAreas(vec![PgrxSpikeArea::WaitForPositionHelper])
        );
    }

    // spec: EG-UNIFIED-DATA-PLANE-R025.2.1
    #[test]
    fn run_spike_refuses_a_decision_with_no_outcome() {
        let evaluators = fake_evaluators();
        let refs: Vec<&dyn PgrxSpikeAreaEvaluator> = evaluators
            .iter()
            .map(|e| e as &dyn PgrxSpikeAreaEvaluator)
            .collect();
        assert_eq!(
            run_spike(&refs, None).unwrap_err(),
            InvalidPgrxDecision::NoOutcome
        );
    }
}
