//! Typed model for the per-dialect conformance harness
//! (EG-UNIFIED-DATA-PLANE-R022): one report entry per adapter/engine-version
//! pair, and a reviewed, named deviation rather than a self-updating
//! baseline. This is the typed-model slice (`.1`): the entry/deviation shape
//! and the refusal for an auto-suppressed (unreviewed) deviation. Running the
//! containerized matrix and the change-capture replay are later children.

use serde::{Deserialize, Serialize};

/// Whether a conformance entry's pushed-down query result (or change-capture
/// replay) matched the native engine.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConformanceOutcome {
    Green,
    Deviated,
}

/// A known, reviewed difference between EG's result and the native engine's.
/// Never auto-generated: a deviation always names the human who reviewed it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConformanceDeviation {
    pub description: String,
    pub reviewed_by: String,
    /// A fixture path or test name that reproduces the difference.
    pub reproducing_fixture: String,
}

/// One adapter/engine-version pair's conformance result.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DialectConformanceEntry {
    pub adapter: String,
    pub engine_version: String,
    pub outcome: ConformanceOutcome,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deviation: Option<ConformanceDeviation>,
}

/// A deviated entry carried no reviewed deviation record, or a green entry
/// carried one — either is a malformed report, never silently accepted.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum InvalidConformanceEntry {
    DeviatedWithoutReview,
    GreenWithDeviation,
    UnreviewedDeviation,
}

impl std::fmt::Display for InvalidConformanceEntry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let message = match self {
            Self::DeviatedWithoutReview => "deviated entry carries no deviation record",
            Self::GreenWithDeviation => "green entry carries a deviation record",
            Self::UnreviewedDeviation => "deviation record names no reviewer",
        };
        write!(f, "{message}")
    }
}

impl std::error::Error for InvalidConformanceEntry {}

impl DialectConformanceEntry {
    /// Confirm this entry's outcome and deviation record agree, and that any
    /// deviation names its reviewer. A deviation can never be auto-suppressed
    /// by leaving `reviewed_by` blank — that refuses, it does not pass
    /// silently.
    pub fn validate(&self) -> Result<(), InvalidConformanceEntry> {
        match (&self.outcome, &self.deviation) {
            (ConformanceOutcome::Deviated, None) => {
                Err(InvalidConformanceEntry::DeviatedWithoutReview)
            }
            (ConformanceOutcome::Green, Some(_)) => Err(InvalidConformanceEntry::GreenWithDeviation),
            (ConformanceOutcome::Deviated, Some(deviation)) if deviation.reviewed_by.trim().is_empty() => {
                Err(InvalidConformanceEntry::UnreviewedDeviation)
            }
            _ => Ok(()),
        }
    }
}

/// A full conformance report: every adapter/engine-version entry attempted
/// in one harness run. `validate` refuses a report containing any malformed
/// entry rather than reporting a partial, silently-filtered green count.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConformanceReport {
    pub entries: Vec<DialectConformanceEntry>,
}

impl ConformanceReport {
    pub fn validate(&self) -> Result<(), InvalidConformanceEntry> {
        for entry in &self.entries {
            entry.validate()?;
        }
        Ok(())
    }

    /// Count of entries whose outcome is `Green`.
    pub fn green_count(&self) -> usize {
        self.entries
            .iter()
            .filter(|entry| entry.outcome == ConformanceOutcome::Green)
            .count()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn deviation(reviewer: &str) -> ConformanceDeviation {
        ConformanceDeviation {
            description: "timestamp precision differs".to_string(),
            reviewed_by: reviewer.to_string(),
            reproducing_fixture: "tests/fixtures/ts_precision.sql".to_string(),
        }
    }

    #[test]
    fn green_entry_without_deviation_validates() {
        let entry = DialectConformanceEntry {
            adapter: "mongodb".to_string(),
            engine_version: "7.0".to_string(),
            outcome: ConformanceOutcome::Green,
            deviation: None,
        };
        assert_eq!(entry.validate(), Ok(()));
    }

    #[test]
    fn deviated_entry_with_reviewed_deviation_validates() {
        let entry = DialectConformanceEntry {
            adapter: "mongodb".to_string(),
            engine_version: "7.0".to_string(),
            outcome: ConformanceOutcome::Deviated,
            deviation: Some(deviation("alice")),
        };
        assert_eq!(entry.validate(), Ok(()));
    }

    #[test]
    fn deviated_entry_without_deviation_is_refused() {
        let entry = DialectConformanceEntry {
            adapter: "mongodb".to_string(),
            engine_version: "7.0".to_string(),
            outcome: ConformanceOutcome::Deviated,
            deviation: None,
        };
        assert_eq!(
            entry.validate(),
            Err(InvalidConformanceEntry::DeviatedWithoutReview)
        );
    }

    #[test]
    fn green_entry_with_deviation_is_refused() {
        let entry = DialectConformanceEntry {
            adapter: "mongodb".to_string(),
            engine_version: "7.0".to_string(),
            outcome: ConformanceOutcome::Green,
            deviation: Some(deviation("alice")),
        };
        assert_eq!(
            entry.validate(),
            Err(InvalidConformanceEntry::GreenWithDeviation)
        );
    }

    #[test]
    fn deviation_with_blank_reviewer_is_refused_not_auto_suppressed() {
        let entry = DialectConformanceEntry {
            adapter: "mongodb".to_string(),
            engine_version: "7.0".to_string(),
            outcome: ConformanceOutcome::Deviated,
            deviation: Some(deviation("  ")),
        };
        assert_eq!(
            entry.validate(),
            Err(InvalidConformanceEntry::UnreviewedDeviation)
        );
    }

    #[test]
    fn report_validate_refuses_if_any_entry_is_malformed() {
        let report = ConformanceReport {
            entries: vec![
                DialectConformanceEntry {
                    adapter: "mongodb".to_string(),
                    engine_version: "7.0".to_string(),
                    outcome: ConformanceOutcome::Green,
                    deviation: None,
                },
                DialectConformanceEntry {
                    adapter: "mongodb".to_string(),
                    engine_version: "6.0".to_string(),
                    outcome: ConformanceOutcome::Deviated,
                    deviation: None,
                },
            ],
        };
        assert!(report.validate().is_err());
        assert_eq!(report.green_count(), 1);
    }
}
