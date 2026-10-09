//! Ingestion cost ladder with provenance rungs (EG-DECISION-ENGINE-R080.1).
//!
//! Ingestion derives each fact through the cheapest sufficient rung of a
//! six-level cost ladder: AST extraction, symbol or type resolution,
//! statistical or community derivation, classical machine-learning or
//! named-entity modelling, embeddings, and finally a language model. A
//! higher rung runs only when every lower rung abstains, and no rung may
//! overwrite a fact a lower, more deterministic rung already established.
//!
//! This slice is the typed model and the pure resolution rule only: which
//! rung a candidate wins against an existing fact. The real per-rung
//! extractors (AST walking, symbol resolution, embeddings, an LLM call) are
//! out of scope here and land in a later slice of this requirement.

use serde::{Deserialize, Serialize};

/// The six cost rungs, cheapest first. Ordinal order is the cost order:
/// a lower variant is always cheaper and more deterministic than a higher
/// one, and [`Ord`] follows declaration order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum ProvenanceRung {
    AstExtraction,
    SymbolResolution,
    StatisticalOrCommunity,
    ClassicalMlOrNer,
    Embeddings,
    LanguageModel,
}

/// A derived fact tagged with the rung that produced it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct RungFact<T> {
    pub value: T,
    pub rung: ProvenanceRung,
}

/// Resolve a candidate fact against whatever fact (if any) a prior rung
/// already established for the same slot (EG-DECISION-ENGINE-R080.1): a
/// higher (more expensive, less deterministic) rung never overwrites a fact
/// a lower rung already set, and between two candidates offered together the
/// lowest-rung one wins. Pure: the same pair of inputs always resolves the
/// same way.
pub fn resolve_rung<T>(existing: Option<RungFact<T>>, candidate: RungFact<T>) -> RungFact<T> {
    match existing {
        Some(current) if current.rung <= candidate.rung => current,
        _ => candidate,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fact(value: &str, rung: ProvenanceRung) -> RungFact<String> {
        RungFact {
            value: value.to_string(),
            rung,
        }
    }

    // spec: EG-DECISION-ENGINE-R080.1
    #[test]
    fn the_lowest_sufficient_rung_wins_with_the_matching_provenance_tag() {
        let resolved = resolve_rung(None, fact("fn foo()", ProvenanceRung::AstExtraction));
        assert_eq!(resolved.rung, ProvenanceRung::AstExtraction);
        assert_eq!(resolved.value, "fn foo()");
    }

    // spec: EG-DECISION-ENGINE-R080.1
    #[test]
    fn a_higher_rung_never_overwrites_an_existing_deterministic_fact() {
        let deterministic = fact(
            "alice@example.com owns billing",
            ProvenanceRung::SymbolResolution,
        );
        let guess = fact(
            "alice@example.com maybe owns billing",
            ProvenanceRung::LanguageModel,
        );
        let resolved = resolve_rung(Some(deterministic.clone()), guess);
        assert_eq!(
            resolved, deterministic,
            "the deterministic rung's fact must survive"
        );
    }

    // spec: EG-DECISION-ENGINE-R080.1
    #[test]
    fn a_lower_rung_offered_later_replaces_a_higher_rungs_guess() {
        let guess = fact(
            "alice@example.com maybe owns billing",
            ProvenanceRung::LanguageModel,
        );
        let deterministic = fact(
            "alice@example.com owns billing",
            ProvenanceRung::SymbolResolution,
        );
        let resolved = resolve_rung(Some(guess), deterministic.clone());
        assert_eq!(
            resolved, deterministic,
            "a later, cheaper, more deterministic rung may replace an earlier guess"
        );
    }

    #[test]
    fn rungs_order_cheapest_first() {
        assert!(ProvenanceRung::AstExtraction < ProvenanceRung::SymbolResolution);
        assert!(ProvenanceRung::SymbolResolution < ProvenanceRung::StatisticalOrCommunity);
        assert!(ProvenanceRung::StatisticalOrCommunity < ProvenanceRung::ClassicalMlOrNer);
        assert!(ProvenanceRung::ClassicalMlOrNer < ProvenanceRung::Embeddings);
        assert!(ProvenanceRung::Embeddings < ProvenanceRung::LanguageModel);
    }
}
