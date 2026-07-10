//! `Provenance` — a generalized derivation record for a `ModalityContract` value.
//!
//! Drafted against `eg-rdf::owl::Justification` (its provenance shape, per the E4
//! brief): `Justification { rule, axioms, premises }` records WHICH rule derived an
//! inferred subsumption and from WHAT prior axioms/subsumptions, plus
//! `Classification::confidence` tracks a `[0,1]` confidence per derivation. Most
//! modalities (tensor, geo) have no derivation history at all — a `Tensor` or
//! `Geometry` is either asserted (written directly) or produced by a plan operator
//! that doesn't (yet) record lineage — so `ModalityContract::provenance` defaults to
//! `None`. `eg-rdf`'s pilot retrofit (documented in the crate README's retrofit order)
//! is expected to be the reference non-trivial implementation, mapping
//! `Justification` -> `Provenance` losslessly (`rule` -> `source`, `axioms`+`premises`
//! rendered -> `detail`, `Classification::confidence` -> `confidence`).

use serde::{Deserialize, Serialize};

/// A generalized provenance/derivation record. Fields are deliberately loose
/// (`String`/`Vec<String>`) rather than typed per-modality, so ANY modality can map
/// its native provenance type onto this shape without eg-modality needing to know
/// about it.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Provenance {
    /// The rule/operation that produced this value — e.g. eg-rdf's `Justification::rule`
    /// ("EL+ completion"), or `"asserted"` for a directly-written value.
    pub source: String,
    /// Free-form justification detail — e.g. eg-rdf's `Justification::axioms` +
    /// rendered `premises`, or a compute lineage trail for a derived tensor/geometry.
    #[serde(default)]
    pub detail: Vec<String>,
    /// Confidence in `[0, 1]`. `1.0` for an asserted/hard value (the default a
    /// modality should use if it has no notion of partial confidence); mirrors
    /// eg-rdf's `Classification::confidence` for a derived subsumption.
    #[serde(default = "Provenance::default_confidence")]
    pub confidence: f64,
}

impl Provenance {
    fn default_confidence() -> f64 {
        1.0
    }

    /// A fully-confident, directly-asserted provenance record — the common case for
    /// a modality value written by a caller rather than derived by reasoning.
    pub fn asserted() -> Self {
        Self {
            source: "asserted".to_string(),
            detail: Vec::new(),
            confidence: 1.0,
        }
    }
}
