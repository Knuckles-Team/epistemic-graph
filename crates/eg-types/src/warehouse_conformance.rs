//! Typed conformance-entry constructor for the Snowflake/BigQuery/DuckDB/Iceberg
//! warehouse federation adapters (EG-UNIFIED-DATA-PLANE-R020.3): a builder over the
//! generic [`crate::dialect_conformance::DialectConformanceEntry`]
//! (EG-UNIFIED-DATA-PLANE-R022.1) that derives this row's `adapter` label directly
//! from the closed [`crate::warehouse_federation::WarehouseSourceKind`]
//! (EG-UNIFIED-DATA-PLANE-R020.1) rather than a caller-supplied string, and refuses a
//! malformed entry or a blank engine version before it is admitted into a report.
//! This is the typed-model slice (`.1`): no live warehouse/lake connection is needed
//! to build or refuse an entry. Actually running the comparison against each native
//! engine and recording its real outcome is a later child
//! (EG-UNIFIED-DATA-PLANE-R020.3.2).

use crate::dialect_conformance::{
    ConformanceDeviation, ConformanceOutcome, DialectConformanceEntry, InvalidConformanceEntry,
};
use crate::warehouse_federation::WarehouseSourceKind;

/// Why a warehouse conformance entry was refused before being admitted into a
/// report.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum InvalidWarehouseConformanceEntry {
    /// The caller supplied no engine version to compare against (e.g. a
    /// DuckDB build or an Iceberg catalog revision) — never silently left
    /// blank.
    EmptyEngineVersion,
    Entry(InvalidConformanceEntry),
}

impl std::fmt::Display for InvalidWarehouseConformanceEntry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::EmptyEngineVersion => {
                write!(f, "warehouse conformance entry: empty engine version")
            }
            Self::Entry(inner) => write!(f, "{inner}"),
        }
    }
}

impl std::error::Error for InvalidWarehouseConformanceEntry {}

impl From<InvalidConformanceEntry> for InvalidWarehouseConformanceEntry {
    fn from(inner: InvalidConformanceEntry) -> Self {
        Self::Entry(inner)
    }
}

/// This row's `adapter` label for a given warehouse source kind — derived
/// from the closed [`WarehouseSourceKind`] vocabulary, never from a
/// caller-supplied string, so an entry can never be mislabeled as a kind it
/// did not actually compare against.
pub const fn warehouse_adapter_label(kind: WarehouseSourceKind) -> &'static str {
    match kind {
        WarehouseSourceKind::Snowflake => "warehouse_snowflake",
        WarehouseSourceKind::BigQuery => "warehouse_bigquery",
        WarehouseSourceKind::DuckDb => "warehouse_duckdb",
        WarehouseSourceKind::Iceberg => "warehouse_iceberg",
    }
}

/// Build one [`DialectConformanceEntry`] for a warehouse/lake federation
/// adapter, refusing a blank engine version or a malformed outcome/deviation
/// shape rather than admitting it into a report. Performs no I/O: the caller
/// supplies the already-observed `outcome`/`deviation`.
pub fn build_warehouse_conformance_entry(
    kind: WarehouseSourceKind,
    engine_version: &str,
    outcome: ConformanceOutcome,
    deviation: Option<ConformanceDeviation>,
) -> Result<DialectConformanceEntry, InvalidWarehouseConformanceEntry> {
    if engine_version.trim().is_empty() {
        return Err(InvalidWarehouseConformanceEntry::EmptyEngineVersion);
    }
    let entry = DialectConformanceEntry {
        adapter: warehouse_adapter_label(kind).to_string(),
        engine_version: engine_version.to_string(),
        outcome,
        deviation,
    };
    entry.validate()?;
    Ok(entry)
}

#[cfg(test)]
mod tests {
    use super::*;

    // spec: EG-UNIFIED-DATA-PLANE-R020.3.1
    #[test]
    fn green_entry_builds_with_kind_derived_label() {
        let entry = build_warehouse_conformance_entry(
            WarehouseSourceKind::Snowflake,
            "8.17",
            ConformanceOutcome::Green,
            None,
        )
        .unwrap();
        assert_eq!(entry.adapter, "warehouse_snowflake");
        assert_eq!(entry.engine_version, "8.17");
    }

    // spec: EG-UNIFIED-DATA-PLANE-R020.3.1
    #[test]
    fn each_kind_maps_to_a_distinct_label() {
        assert_eq!(
            warehouse_adapter_label(WarehouseSourceKind::BigQuery),
            "warehouse_bigquery"
        );
        assert_eq!(
            warehouse_adapter_label(WarehouseSourceKind::DuckDb),
            "warehouse_duckdb"
        );
        assert_eq!(
            warehouse_adapter_label(WarehouseSourceKind::Iceberg),
            "warehouse_iceberg"
        );
    }

    // spec: EG-UNIFIED-DATA-PLANE-R020.3.1
    #[test]
    fn empty_engine_version_is_refused() {
        let result = build_warehouse_conformance_entry(
            WarehouseSourceKind::DuckDb,
            "",
            ConformanceOutcome::Green,
            None,
        );
        assert_eq!(
            result,
            Err(InvalidWarehouseConformanceEntry::EmptyEngineVersion)
        );
    }

    // spec: EG-UNIFIED-DATA-PLANE-R020.3.1
    #[test]
    fn deviated_entry_without_review_is_refused() {
        let result = build_warehouse_conformance_entry(
            WarehouseSourceKind::Iceberg,
            "1.6.0",
            ConformanceOutcome::Deviated,
            None,
        );
        assert_eq!(
            result,
            Err(InvalidWarehouseConformanceEntry::Entry(
                InvalidConformanceEntry::DeviatedWithoutReview
            ))
        );
    }

    // spec: EG-UNIFIED-DATA-PLANE-R020.3.1
    #[test]
    fn deviated_entry_with_reviewed_deviation_builds() {
        let deviation = ConformanceDeviation {
            description: "BigQuery NUMERIC rounding differs at scale 38".to_string(),
            reviewed_by: "bob".to_string(),
            reproducing_fixture: "tests/fixtures/bq_numeric_scale38.sql".to_string(),
        };
        let entry = build_warehouse_conformance_entry(
            WarehouseSourceKind::BigQuery,
            "2026-10",
            ConformanceOutcome::Deviated,
            Some(deviation),
        )
        .unwrap();
        assert_eq!(entry.outcome, ConformanceOutcome::Deviated);
    }
}
