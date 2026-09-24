//! `ATTRIBUTE` (EH-523): the wire form of the contribution-attribution stage and its
//! canonical UQL spelling.
//!
//! `ATTRIBUTE <agg> OF ( SCORE | <property> ) ( LINEAR | SHAPLEY [ SAMPLES n SEED s ]
//! | OWEN BY <property> )` — the incoming rows are the players, each player's value is
//! its row score or a numeric node property, and a coalition's value is the aggregate of
//! its members' values (`v(∅) = 0`). The stage re-scores every row with its
//! contribution (channel `attribution`; a sampled estimate's CI half-width is channel
//! `attribution_ci`) and orders the rows by it, descending.

use super::*;

/// Where each player's value comes from.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum AttributionInput {
    /// The incoming row's score (a prior RANK, WINDOW, TSSCAN, … stage).
    Score,
    /// A numeric property of the row's node.
    Property { name: String },
}

/// The aggregate a coalition's value is.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum AttributionValue {
    /// Additive: the only aggregate `LINEAR` accepts.
    Sum,
    Mean,
    Max,
    Min,
    /// The `p`-th percentile, `1..=99` (UQL `P<p>`, e.g. `P95`).
    Percentile {
        p: u8,
    },
}

/// How contributions are computed.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum AttributionMethod {
    /// The exact split of an additive value (`SUM`); anything else is refused.
    Linear,
    /// Exact Shapley over a memoised coalition table (at most 16 rows).
    Shapley,
    /// Sampled Shapley: `samples` antithetic permutation pairs drawn from `seed`.
    ShapleySampled { samples: u32, seed: u64 },
    /// Owen values with the rows grouped into unions by the node property `by`.
    Owen { by: String },
}

/// `P<p>`, `SUM`, … — the canonical aggregate spelling.
pub fn uql_attribution_value(value: AttributionValue) -> String {
    match value {
        AttributionValue::Sum => "SUM".into(),
        AttributionValue::Mean => "MEAN".into(),
        AttributionValue::Max => "MAX".into(),
        AttributionValue::Min => "MIN".into(),
        AttributionValue::Percentile { p } => format!("P{p}"),
    }
}

fn input_text(input: &AttributionInput) -> String {
    match input {
        AttributionInput::Score => "SCORE".into(),
        AttributionInput::Property { name } => uql_ident(name),
    }
}

fn method_text(method: &AttributionMethod) -> String {
    match method {
        AttributionMethod::Linear => "LINEAR".into(),
        AttributionMethod::Shapley => "SHAPLEY".into(),
        AttributionMethod::ShapleySampled { samples, seed } => {
            format!("SHAPLEY SAMPLES {samples} SEED {seed}")
        }
        AttributionMethod::Owen { by } => format!("OWEN BY {}", uql_ident(by)),
    }
}

/// `ATTRIBUTE <agg> OF <input> <method>`. A percentile outside `1..=99` has no
/// spelling (it would re-parse to a different stage or not at all).
pub(super) fn attribute(
    input: &AttributionInput,
    value: AttributionValue,
    method: &AttributionMethod,
) -> Result<String, UqlPrintError> {
    if let AttributionValue::Percentile { p } = value {
        if !(1..=99).contains(&p) {
            return Err(refuse(
                UqlPrintCode::UnknownAggregate,
                format!("percentile P{p} is outside 1..=99"),
            ));
        }
    }
    Ok(format!(
        "ATTRIBUTE {} OF {} {}",
        uql_attribution_value(value),
        input_text(input),
        method_text(method)
    ))
}
