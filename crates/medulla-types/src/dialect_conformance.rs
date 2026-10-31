//! Typed model for the per-dialect conformance harness
//! (MEDULLA-UNIFIED-DATA-PLANE-R022): one report entry per adapter/engine-version
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
            (ConformanceOutcome::Green, Some(_)) => {
                Err(InvalidConformanceEntry::GreenWithDeviation)
            }
            (ConformanceOutcome::Deviated, Some(deviation))
                if deviation.reviewed_by.trim().is_empty() =>
            {
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

/// Where a source category's rows live by default (MEDULLA-UNIFIED-DATA-PLANE-R022.4.1):
/// read at query time from the source (`virtual`) or stored by the engine
/// (`ingested`). A fetched web page has no virtual category; it enters as a
/// Document through `SourceIngest`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SourceResidency {
    Virtual,
    Ingested,
}

/// Default cache time-to-live, in seconds, for virtual web metasearch rows.
pub const WEB_METASEARCH_TTL_S: u64 = 900;

/// The default residency of one source category and, for a cached virtual
/// category, its time-to-live in seconds.
pub fn default_residency(
    category: crate::metadata_acceleration::SourceCategory,
) -> (SourceResidency, Option<u64>) {
    use crate::metadata_acceleration::SourceCategory;
    match category {
        SourceCategory::WebMetasearch => (SourceResidency::Virtual, Some(WEB_METASEARCH_TTL_S)),
        _ => (SourceResidency::Virtual, None),
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

    // spec: MEDULLA-UNIFIED-DATA-PLANE-R022.1
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

    // spec: MEDULLA-UNIFIED-DATA-PLANE-R022.1
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

    // spec: MEDULLA-UNIFIED-DATA-PLANE-R022.1
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

    // spec: MEDULLA-UNIFIED-DATA-PLANE-R022.1
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

    // spec: MEDULLA-UNIFIED-DATA-PLANE-R022.1
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

    // spec: MEDULLA-UNIFIED-DATA-PLANE-R022.1
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

    /// Parse the residency cell of each row of the coverage matrix in
    /// `plan.md`, keyed by the category label.
    fn plan_matrix_residency() -> Vec<(String, String)> {
        let plan = include_str!("../../../specs/unified-data-plane/plan.md");
        let section = plan
            .split("### Source-category coverage matrix (R022.4)")
            .nth(1)
            .expect("plan.md holds the coverage matrix");
        section
            .lines()
            .skip_while(|line| !line.starts_with("| Category"))
            .skip(2)
            .take_while(|line| line.starts_with('|'))
            .map(|line| {
                let cells: Vec<&str> = line.trim_matches('|').split('|').map(str::trim).collect();
                (cells[0].to_string(), cells.last().unwrap().to_string())
            })
            .collect()
    }

    // spec: MEDULLA-UNIFIED-DATA-PLANE-R022.4.1
    #[test]
    fn matrix_states_residency() {
        use crate::metadata_acceleration::SourceCategory;
        let rows = plan_matrix_residency();
        for category in SourceCategory::ALL {
            let label = category.matrix_label();
            let (_, cell) = rows
                .iter()
                .find(|(name, _)| name == label)
                .unwrap_or_else(|| panic!("coverage matrix has no row for {label}"));
            let (residency, ttl) = default_residency(category);
            let word = cell.split(',').next().unwrap().trim();
            let stated: SourceResidency =
                serde_json::from_value(serde_json::Value::String(word.to_string()))
                    .unwrap_or_else(|_| panic!("{label} has no closed residency value: {cell:?}"));
            assert_eq!(stated, residency, "{label}");
            match ttl {
                Some(seconds) => assert!(
                    cell.contains(&format!("{seconds} s time-to-live")),
                    "{label}"
                ),
                None => assert_eq!(cell, word, "{label}"),
            }
        }
        assert_eq!(
            default_residency(SourceCategory::WebMetasearch),
            (SourceResidency::Virtual, Some(900))
        );
    }
}
