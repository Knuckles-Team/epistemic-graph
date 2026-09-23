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
