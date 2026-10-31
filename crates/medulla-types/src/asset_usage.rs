//! Asset usage and catalog search hits (MEDULLA-UNIFIED-DATA-PLANE-R764). Usage
//! counts come from the query audit. Search fuses text and vector scores
//! with a usage prior that never exceeds `USAGE_PRIOR_CAP` of the score.

use serde::{Deserialize, Serialize};

/// Reads and queries of one asset by one principal in one period.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AssetUsage {
    pub asset: String,
    pub principal: String,
    pub period: String,
    pub reads: u64,
    pub queries: u64,
}

/// The score components of one hit.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct HitComponents {
    pub text: f64,
    pub vector: f64,
    pub usage: f64,
}

/// One catalog search hit.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CatalogSearchHit {
    pub asset: String,
    pub kind: String,
    pub score: f64,
    pub usage_rank: u32,
    pub components: HitComponents,
}

/// The usage prior's maximum share of a fused score.
pub const USAGE_PRIOR_CAP: f64 = 0.20;

/// Fuse a relevance score (text plus vector, each in `[0, 1]`, averaged)
/// with a usage prior. `usage_norm` is the asset's usage normalized to
/// `[0, 1]` over the candidate set. The usage component is at most
/// `USAGE_PRIOR_CAP` of the fused score.
pub fn fuse_with_usage_prior(text: f64, vector: f64, usage_norm: f64) -> HitComponents {
    let clamp = |value: f64| {
        if value.is_finite() {
            value.clamp(0.0, 1.0)
        } else {
            0.0
        }
    };
    let relevance = (clamp(text) + clamp(vector)) / 2.0;
    // usage <= cap * (relevance + usage)  <=>  usage <= cap / (1 - cap) * relevance
    let ceiling = USAGE_PRIOR_CAP / (1.0 - USAGE_PRIOR_CAP) * relevance;
    let usage = (clamp(usage_norm) * USAGE_PRIOR_CAP).min(ceiling);
    HitComponents {
        text: clamp(text) / 2.0,
        vector: clamp(vector) / 2.0,
        usage,
    }
}

impl HitComponents {
    pub fn total(&self) -> f64 {
        self.text + self.vector + self.usage
    }
}
