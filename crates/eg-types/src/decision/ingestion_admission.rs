//! Per-unit embedding-admission classification for ingestion
//! (`EG-DECISION-ENGINE-R079.1`): before any derived unit of text is sent to
//! an embedding call, a deterministic classifier decides whether its
//! content class is even eligible.
//!
//! This module fixes the taxonomy and the pure admission rule only. It
//! wires no real ingestion call site, no embedding client, no
//! cheaper-index lookup (exact/BM25/graph), no near-duplicate entropy or
//! content-hash dedup, no size bound, no SQL-column cardinality rule, and
//! no retrieval-telemetry feedback -- those are later slices of
//! `EG-DECISION-ENGINE-R079` (`.2`/`.3`+), built on top of the typed gate
//! this one establishes.
//!
//! Three classes are admitted because they carry information a human wrote
//! for humans to read: prose, document, and comment content. Four are
//! excluded because they are mechanically produced or not meaningfully
//! natural-language text: generated, lockfile, vendored, and minified
//! content. Embedding an excluded class would spend budget on text that
//! cannot answer a retrieval question any better than its exact, BM25, or
//! graph index already does.

use serde::{Deserialize, Serialize};

/// The fixed per-unit content-class taxonomy an ingestion admission
/// decision classifies text into. Adding a class is a registry change
/// here, not a new ad hoc check at each ingestion call site.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum ContentClass {
    /// Free natural-language prose: README bodies, docstrings, commit
    /// messages, design notes.
    Prose,
    /// Structured documentation: spec, task, and status prose-bearing
    /// documents.
    Document,
    /// Inline source comments.
    Comment,
    /// Mechanically produced output: codegen, compiled artifacts, snapshot
    /// fixtures.
    Generated,
    /// Dependency lockfiles (`Cargo.lock`, `package-lock.json`, and the
    /// like).
    Lockfile,
    /// Vendored third-party source copied verbatim into the tree.
    Vendored,
    /// Minified or otherwise machine-compacted text.
    Minified,
}

impl ContentClass {
    /// Every registered content class, in a stable order.
    pub const ALL: [ContentClass; 7] = [
        Self::Prose,
        Self::Document,
        Self::Comment,
        Self::Generated,
        Self::Lockfile,
        Self::Vendored,
        Self::Minified,
    ];

    /// Whether a unit of this content class may ever be embedded.
    ///
    /// This is the single, deterministic admission gate an ingestion call
    /// site must consult before it embeds any derived text for a unit of
    /// this class: `false` means no embedding call is made for that unit,
    /// full stop -- independent of any cheaper-index lookup, dedup, size
    /// bound, or telemetry demotion a later slice layers on top for the
    /// classes that do admit.
    pub const fn admits(self) -> bool {
        matches!(self, Self::Prose | Self::Document | Self::Comment)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ingestion_admission_classifier_excludes_every_excluded_class() {
        for class in [
            ContentClass::Generated,
            ContentClass::Lockfile,
            ContentClass::Vendored,
            ContentClass::Minified,
        ] {
            assert!(
                !class.admits(),
                "{class:?} must not be admitted: no embedding call may be made for it"
            );
        }
    }

    #[test]
    fn ingestion_admission_classifier_admits_every_eligible_class() {
        for class in [
            ContentClass::Prose,
            ContentClass::Document,
            ContentClass::Comment,
        ] {
            assert!(class.admits(), "{class:?} must be admitted");
        }
    }

    #[test]
    fn ingestion_admission_classifier_registry_partitions_with_no_overlap() {
        let admitted = ContentClass::ALL
            .iter()
            .filter(|class| class.admits())
            .count();
        let excluded = ContentClass::ALL
            .iter()
            .filter(|class| !class.admits())
            .count();
        assert_eq!(admitted, 3);
        assert_eq!(excluded, 4);
        assert_eq!(admitted + excluded, ContentClass::ALL.len());
    }
}
