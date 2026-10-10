//! Typed conformance-entry constructor for the MongoDB/DocumentDB adapter
//! (EG-UNIFIED-DATA-PLANE-R019.3): a closed per-engine vocabulary plus a
//! builder over the generic [`crate::dialect_conformance::DialectConformanceEntry`]
//! (EG-UNIFIED-DATA-PLANE-R022.1) that always fixes this row's `adapter` label and
//! refuses a malformed entry before it is admitted into a report. This is the
//! typed-model slice (`.1`): no live MongoDB/DocumentDB connection or capture is
//! needed to build or refuse an entry. Actually running the comparison against a
//! native engine and recording its real outcome is a later child
//! (EG-UNIFIED-DATA-PLANE-R019.3.2).

use crate::dialect_conformance::{
    ConformanceDeviation, ConformanceOutcome, DialectConformanceEntry, InvalidConformanceEntry,
};

/// The closed set of native engines this row's conformance entry may compare
/// against. An entry can only be built from one of these — never from an
/// arbitrary caller-supplied string.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DocumentConformanceEngine {
    MongoDb6,
    MongoDb7,
    DocumentDb,
}

impl DocumentConformanceEngine {
    /// The `engine_version` string recorded on the built entry.
    pub const fn version_label(self) -> &'static str {
        match self {
            Self::MongoDb6 => "mongodb-6.0",
            Self::MongoDb7 => "mongodb-7.0",
            Self::DocumentDb => "documentdb",
        }
    }
}

/// The fixed adapter label every entry this module builds carries. Never
/// settable by a caller, so an entry for this row can never be mislabeled as
/// another adapter's result.
pub const DOCUMENT_ADAPTER_LABEL: &str = "mongodb_documentdb";

/// Build one [`DialectConformanceEntry`] for the MongoDB/DocumentDB adapter,
/// refusing a malformed shape (a deviated outcome with no reviewed deviation,
/// a green outcome carrying one, or an unreviewed deviation) rather than
/// admitting it into a report. Performs no I/O: the caller supplies the
/// already-observed `outcome`/`deviation`.
pub fn build_document_conformance_entry(
    engine: DocumentConformanceEngine,
    outcome: ConformanceOutcome,
    deviation: Option<ConformanceDeviation>,
) -> Result<DialectConformanceEntry, InvalidConformanceEntry> {
    let entry = DialectConformanceEntry {
        adapter: DOCUMENT_ADAPTER_LABEL.to_string(),
        engine_version: engine.version_label().to_string(),
        outcome,
        deviation,
    };
    entry.validate()?;
    Ok(entry)
}

#[cfg(test)]
mod tests {
    use super::*;

    // spec: EG-UNIFIED-DATA-PLANE-R019.3.1
    #[test]
    fn green_entry_builds_with_fixed_adapter_label() {
        let entry = build_document_conformance_entry(
            DocumentConformanceEngine::MongoDb7,
            ConformanceOutcome::Green,
            None,
        )
        .unwrap();
        assert_eq!(entry.adapter, DOCUMENT_ADAPTER_LABEL);
        assert_eq!(entry.engine_version, "mongodb-7.0");
    }

    // spec: EG-UNIFIED-DATA-PLANE-R019.3.1
    #[test]
    fn documentdb_engine_maps_to_its_own_label() {
        let entry = build_document_conformance_entry(
            DocumentConformanceEngine::DocumentDb,
            ConformanceOutcome::Green,
            None,
        )
        .unwrap();
        assert_eq!(entry.engine_version, "documentdb");
    }

    // spec: EG-UNIFIED-DATA-PLANE-R019.3.1
    #[test]
    fn deviated_entry_without_review_is_refused() {
        let result = build_document_conformance_entry(
            DocumentConformanceEngine::MongoDb6,
            ConformanceOutcome::Deviated,
            None,
        );
        assert_eq!(result, Err(InvalidConformanceEntry::DeviatedWithoutReview));
    }

    // spec: EG-UNIFIED-DATA-PLANE-R019.3.1
    #[test]
    fn deviated_entry_with_reviewed_deviation_builds() {
        let deviation = ConformanceDeviation {
            description: "ObjectId ordering differs from insertion order".to_string(),
            reviewed_by: "alice".to_string(),
            reproducing_fixture: "tests/fixtures/mongo_objectid_order.json".to_string(),
        };
        let entry = build_document_conformance_entry(
            DocumentConformanceEngine::MongoDb7,
            ConformanceOutcome::Deviated,
            Some(deviation),
        )
        .unwrap();
        assert_eq!(entry.outcome, ConformanceOutcome::Deviated);
    }

    // spec: EG-UNIFIED-DATA-PLANE-R019.3.1
    #[test]
    fn green_entry_with_deviation_is_refused() {
        let deviation = ConformanceDeviation {
            description: "unused".to_string(),
            reviewed_by: "alice".to_string(),
            reproducing_fixture: "unused".to_string(),
        };
        let result = build_document_conformance_entry(
            DocumentConformanceEngine::MongoDb7,
            ConformanceOutcome::Green,
            Some(deviation),
        );
        assert_eq!(result, Err(InvalidConformanceEntry::GreenWithDeviation));
    }
}
