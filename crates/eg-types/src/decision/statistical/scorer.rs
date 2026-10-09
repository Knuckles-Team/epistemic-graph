//! The parameters of the resident option-attention scorer (EH-291, EH-300).
//!
//! An `OptionAttention` head reads each option's STRUCTURED feature row (no
//! text, no tokeniser: EH-292), embeds it, lets every option attend over the
//! embedded option set at its own marker, and adds that to the head's linear
//! logit. Every parameter is a raw `Q32` integer (`value / 2^32`), because the
//! engine scores in fixed-point arithmetic only: the same body produces the
//! same decision bits on every host and at every later replay.
//!
//! Layouts are row-major with the INPUT index outer: `embed[f * width + j]`
//! maps feature `f` to embedding unit `j`; `query/key/value[a * width + b]`
//! map unit `a` to unit `b`.

use serde::{Deserialize, Serialize};

use crate::contract::BoundedVec;

/// Largest embedding width a scorer may declare.
pub const MAX_SCORER_WIDTH: usize = 16;
/// Largest scorer shortlist: the candidate-matrix bound.
pub const MAX_SCORER_SHORTLIST: usize = 64;
/// Largest feature count a head reads (the head's weight bound).
pub const MAX_SCORER_FEATURES: usize = 32;

/// The fitted parameters of an `OptionAttention` head, all raw `Q32`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct OptionAttentionParams {
    /// Embedding width `d`, `1..=16`.
    pub width: u8,
    /// How many legal options are scored, `1..=64`: the rest of the legal
    /// set is ranked out by the linear pre-score first (EH-296).
    pub shortlist: u8,
    /// `features x width`.
    pub embed: BoundedVec<i64, 512>,
    /// `width`.
    pub embed_bias: BoundedVec<i64, 16>,
    /// `width x width` each.
    pub query: BoundedVec<i64, 256>,
    pub key: BoundedVec<i64, 256>,
    pub value: BoundedVec<i64, 256>,
    /// `width`: weight of an option's own embedding in its logit.
    pub self_weight: BoundedVec<i64, 16>,
    /// `width`: weight of the attended context in its logit.
    pub context_weight: BoundedVec<i64, 16>,
}

fn within(name: &str, value: usize, maximum: usize) -> Result<(), String> {
    if (1..=maximum).contains(&value) {
        return Ok(());
    }
    Err(format!("scorer {name} {value} is outside 1..={maximum}"))
}

/// EG-DECISION-ENGINE-R090.1: the declared resident budget a compiled scorer
/// must stay inside -- "on the order of 700,000 parameters and a few
/// megabytes", serving a decision in single-digit milliseconds on CPU, with
/// no Python runtime, sidecar process, inter-process hop or GPU. Split out
/// of R090 per the rapid-delivery contract's sizing rule (the full row also
/// names the compiled benchmark harness, a second code root); this slice is
/// the typed budget and its refusal test.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResidentScorerBudget {
    pub max_parameters: u32,
    pub max_bytes: u32,
    pub max_p99_micros: u32,
}

/// The hard ceiling no declared budget may exceed.
pub const RESIDENT_SCORER_CEILING: ResidentScorerBudget = ResidentScorerBudget {
    max_parameters: 700_000,
    max_bytes: 8 * 1024 * 1024,
    max_p99_micros: 10_000,
};

impl ResidentScorerBudget {
    /// Refuses a declared budget that would exceed the resident ceiling.
    pub fn declare(
        max_parameters: u32,
        max_bytes: u32,
        max_p99_micros: u32,
    ) -> Result<Self, String> {
        let candidate = Self {
            max_parameters,
            max_bytes,
            max_p99_micros,
        };
        if candidate.max_parameters > RESIDENT_SCORER_CEILING.max_parameters
            || candidate.max_bytes > RESIDENT_SCORER_CEILING.max_bytes
            || candidate.max_p99_micros > RESIDENT_SCORER_CEILING.max_p99_micros
        {
            return Err(format!(
                "declared scorer budget {candidate:?} exceeds the resident ceiling {RESIDENT_SCORER_CEILING:?}"
            ));
        }
        Ok(candidate)
    }

    /// The largest `OptionAttentionParams` shape this crate allows: every
    /// dimension at its declared maximum. Proves the compiled scorer's own
    /// worst case fits the ceiling.
    pub fn largest_compiled_scorer_parameter_count() -> u32 {
        let width = MAX_SCORER_WIDTH;
        let features = MAX_SCORER_FEATURES;
        (features * width + width + 3 * width * width + 2 * width) as u32
    }
}

impl OptionAttentionParams {
    /// Every dimension agrees with `width` and the head's `features`.
    pub fn check(&self, features: usize) -> Result<(), String> {
        let width = usize::from(self.width);
        within("width", width, MAX_SCORER_WIDTH)?;
        within(
            "shortlist",
            usize::from(self.shortlist),
            MAX_SCORER_SHORTLIST,
        )?;
        within("feature count", features, MAX_SCORER_FEATURES)?;
        let shapes = [
            ("embed", self.embed.len(), features * width),
            ("embed_bias", self.embed_bias.len(), width),
            ("query", self.query.len(), width * width),
            ("key", self.key.len(), width * width),
            ("value", self.value.len(), width * width),
            ("self_weight", self.self_weight.len(), width),
            ("context_weight", self.context_weight.len(), width),
        ];
        match shapes
            .iter()
            .find(|(_, actual, expected)| actual != expected)
        {
            Some((name, actual, expected)) => Err(format!(
                "scorer {name} holds {actual} values where its shape needs {expected}"
            )),
            None => Ok(()),
        }
    }
}

#[cfg(test)]
mod budget_tests {
    use super::*;

    #[test]
    fn the_largest_compiled_scorer_fits_the_resident_ceiling() {
        let params = ResidentScorerBudget::largest_compiled_scorer_parameter_count();
        assert!(
            params <= RESIDENT_SCORER_CEILING.max_parameters,
            "worst-case compiled scorer has {params} parameters, over the {} ceiling",
            RESIDENT_SCORER_CEILING.max_parameters
        );
    }

    #[test]
    fn a_budget_at_the_ceiling_is_declared() {
        let budget = ResidentScorerBudget::declare(700_000, 8 * 1024 * 1024, 10_000)
            .expect("the ceiling itself is a legal declaration");
        assert_eq!(budget, RESIDENT_SCORER_CEILING);
    }

    #[test]
    fn a_budget_over_the_parameter_ceiling_is_refused() {
        let err = ResidentScorerBudget::declare(700_001, 1024, 1_000)
            .expect_err("over the parameter ceiling");
        assert!(err.contains("700001"));
    }

    #[test]
    fn a_budget_over_the_latency_ceiling_is_refused() {
        let err = ResidentScorerBudget::declare(1, 1024, 10_001)
            .expect_err("over the single-digit-millisecond ceiling");
        assert!(err.contains("10001"));
    }
}
