//! EG-DECISION-ENGINE-R015 (`.1` slice): a closed, non-generative head-kind
//! vocabulary.
//!
//! The requirement is that the decision engine's statistical heads are small
//! fitted models used only to rank legal options or abstain -- never a
//! generative model's weights or free-text generation. This module gives
//! that boundary a typed, closed name: [`StatisticalHeadKind::parse`] accepts
//! only the two heads the decision ladder actually fits, and refuses any
//! other name, including a plausible generative-model label, by construction
//! rather than by code review alone.
//!
//! This is the `.1` typed-model slice: it names and validates the closed set.
//! Wiring it into the ladder's own head registry is a later entry-point slice.

use std::fmt;

/// The two statistical heads the decision ladder may fit. There is
/// deliberately no third arm: adding one for a generative model would be a
/// visible, reviewable diff to this enum rather than a silent capability.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StatisticalHeadKind {
    /// Advisory weighted-feature scoring. Never calibrated.
    WeightedFeatures,
    /// The deterministic, bit-for-bit reproducible listwise optimizer.
    ListwiseLogistic,
}

/// Why a head-kind name was refused.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HeadKindRefusal {
    /// The name is not one of the closed set's two members.
    NotAStatisticalHead { name: String },
}

impl fmt::Display for HeadKindRefusal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotAStatisticalHead { name } => write!(
                f,
                "HEAD_KIND_NOT_STATISTICAL: {name:?} is not a fitted statistical head"
            ),
        }
    }
}

impl StatisticalHeadKind {
    /// Parse a wire head-kind name, refusing anything outside the closed set
    /// -- in particular any name suggesting a generative or LLM-backed head.
    pub fn parse(name: &str) -> Result<Self, HeadKindRefusal> {
        match name {
            "weighted_features" => Ok(Self::WeightedFeatures),
            "listwise_logistic" => Ok(Self::ListwiseLogistic),
            other => Err(HeadKindRefusal::NotAStatisticalHead {
                name: other.to_string(),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_the_two_closed_heads() {
        assert_eq!(
            StatisticalHeadKind::parse("weighted_features"),
            Ok(StatisticalHeadKind::WeightedFeatures)
        );
        assert_eq!(
            StatisticalHeadKind::parse("listwise_logistic"),
            Ok(StatisticalHeadKind::ListwiseLogistic)
        );
    }

    #[test]
    fn refuses_a_generative_model_name() {
        let refusal = StatisticalHeadKind::parse("generative_llm").unwrap_err();
        assert_eq!(
            refusal,
            HeadKindRefusal::NotAStatisticalHead {
                name: "generative_llm".to_string()
            }
        );
    }

    #[test]
    fn refuses_an_unknown_name() {
        assert!(StatisticalHeadKind::parse("kolmogorov_arnold").is_err());
    }
}
