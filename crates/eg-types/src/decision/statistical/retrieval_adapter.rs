//! The query-side embedding adapter (EH-396): a small fitted statistical head,
//! never a mutation of stored vectors.
//!
//! An adapter is `r <= 8` orthonormal directions `u_i` with bounded gains
//! `g_i` (`|g_i| <= 1/2`) in ONE embedding space. It maps a query vector
//! `q -> q + sum_i g_i (u_i . q) u_i` -- the identity plus a symmetric low-rank
//! term -- and is applied to the QUERY only, inside the ANN probe, after row-level
//! security has fixed the candidate set. Corpus vectors are never touched, so
//! activating or rolling back an adapter is a pointer move, every prior state
//! replays, and an adapter can only re-order rows the caller could already see.
//!
//! Every number is fixed-point: unit components on `Q30`, gains on `Q16`, so a
//! body and its digest are identical on every release target.

use serde::{Deserialize, Serialize};

use super::super::jobs::RecordWindow;
use super::retrieval::MAX_QUERY_DIMENSIONS;
use crate::contract::BoundedVec;

/// Format identity of an adapter body.
pub const QUERY_ADAPTER_SCHEMA_VERSION: u16 = 1;
/// Most directions an adapter may carry.
pub const MAX_ADAPTER_RANK: usize = 8;
/// Largest admissible `|g_i|`, on `Q16` (one half).
pub const MAX_ADAPTER_GAIN_Q16: i32 = 1 << 15;
/// The `Q30` scale of a unit component.
pub const UNIT_SCALE_BITS: u32 = 30;
/// Tolerance, on `Q30`, of the orthonormality check (about 1e-3).
const ORTHONORMAL_TOLERANCE_Q30: i128 = 1 << 20;

/// One direction and its gain.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct AdapterDirection {
    pub gain_q16: i32,
    pub unit_q30: BoundedVec<i32, MAX_QUERY_DIMENSIONS>,
}

/// The body of a fitted query adapter.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct QueryAdapterBody {
    pub schema_version: u16,
    /// The `EmbeddingSpaceRef::digest` it applies in; any other space ignores it.
    pub space_digest: String,
    pub dimensions: u32,
    pub directions: BoundedVec<AdapterDirection, MAX_ADAPTER_RANK>,
    /// Digest of the admitted training items, in order.
    pub training_digest: String,
    pub n_training: u64,
}

fn dot_q60(left: &[i32], right: &[i32]) -> i128 {
    left.iter()
        .zip(right)
        .map(|(a, b)| i128::from(*a) * i128::from(*b))
        .sum()
}

fn near(value_q60: i128, target_q30: i128) -> bool {
    let value_q30 = value_q60 >> UNIT_SCALE_BITS;
    (value_q30 - target_q30).abs() <= ORTHONORMAL_TOLERANCE_Q30
}

fn orthonormal(directions: &[AdapterDirection]) -> bool {
    let one = 1_i128 << UNIT_SCALE_BITS;
    directions.iter().enumerate().all(|(i, a)| {
        directions.iter().skip(i).enumerate().all(|(offset, b)| {
            let target = if offset == 0 { one } else { 0 };
            near(
                dot_q60(a.unit_q30.as_slice(), b.unit_q30.as_slice()),
                target,
            )
        })
    })
}

impl QueryAdapterBody {
    /// The validating check: a served version, `1..=8` directions of the
    /// declared width, bounded gains, orthonormal units.
    pub fn check(&self) -> Result<(), String> {
        if self.schema_version != QUERY_ADAPTER_SCHEMA_VERSION {
            return Err(format!(
                "adapter version {} is not served",
                self.schema_version
            ));
        }
        if self.space_digest.is_empty() || self.directions.is_empty() {
            return Err("an adapter names its space and carries a direction".to_string());
        }
        let width = self.dimensions as usize;
        let shaped = self.directions.iter().all(|d| {
            d.unit_q30.len() == width
                && d.gain_q16.unsigned_abs() <= MAX_ADAPTER_GAIN_Q16.unsigned_abs()
        });
        if !shaped {
            return Err("every direction has the declared width and |gain| <= 1/2".to_string());
        }
        if !orthonormal(self.directions.as_slice()) {
            return Err("adapter directions must be orthonormal".to_string());
        }
        Ok(())
    }
}

/// Fit an adapter from the judged retrieval outcomes of one space.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct AdapterFitRequest {
    /// The graph whose stored vectors score the judged units. The caller's
    /// graph ACL and row-level security apply: an invisible unit is never read.
    pub graph: String,
    pub space_digest: String,
    /// `None`: every retrieval question.
    #[serde(default)]
    pub question_id: Option<String>,
    pub window: RecordWindow,
    /// Directions to fit, `1..=8`.
    pub rank: u8,
    /// Largest `|g_i|` on `Q16`, at most one half.
    pub max_gain_q16: i32,
    /// Share of judged items held out for evaluation, per mille (`100..=500`).
    pub holdout_per_mille: u16,
    /// Fewest held-out items a passing receipt needs.
    pub min_eval_items: u32,
}

/// The promotion receipt of one fitted adapter: a paired sign test of the
/// held-out reciprocal rank of the best cited unit, adapted vs base.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct AdapterEvalReceipt {
    pub schema_version: u16,
    pub adapter_digest: String,
    pub space_digest: String,
    pub n_training: u64,
    pub n_eval: u64,
    pub wins: u64,
    pub losses: u64,
    pub ties: u64,
    /// Mean reciprocal rank on the held-out items, on `Q16`.
    pub base_mrr_q16: i64,
    pub adapted_mrr_q16: i64,
    /// Wilson 95% lower bound of `wins / (wins + losses)`, on `Q16`.
    pub win_rate_lower_q16: i64,
    /// Passed: `n_eval >= min_eval_items` and the lower bound exceeds one half.
    pub passed: bool,
}

/// A fitted adapter, its receipt and the digests they are pinned by.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct AdapterFitted {
    pub adapter_digest: String,
    pub body: QueryAdapterBody,
    pub receipt_digest: String,
    pub receipt: AdapterEvalReceipt,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn direction(unit: &[i32], gain_q16: i32) -> AdapterDirection {
        AdapterDirection {
            gain_q16,
            unit_q30: BoundedVec::new(unit.to_vec()).unwrap(),
        }
    }

    fn body(directions: Vec<AdapterDirection>) -> QueryAdapterBody {
        QueryAdapterBody {
            schema_version: QUERY_ADAPTER_SCHEMA_VERSION,
            space_digest: "sha256:space".to_string(),
            dimensions: 2,
            directions: BoundedVec::new(directions).unwrap(),
            training_digest: "sha256:t".to_string(),
            n_training: 1,
        }
    }

    #[test]
    fn an_adapter_is_orthonormal_and_bounded() {
        let one = 1 << 30;
        assert!(
            body(vec![direction(&[one, 0], 100), direction(&[0, one], -100)])
                .check()
                .is_ok()
        );
        assert!(body(vec![direction(&[one, 0], 1 << 16)]).check().is_err());
        assert!(body(vec![direction(&[one, 0], 1), direction(&[one, 0], 1)])
            .check()
            .is_err());
        assert!(body(vec![direction(&[one / 2, 0], 1)]).check().is_err());
        assert!(body(vec![direction(&[one, 0, 0], 1)]).check().is_err());
    }
}
