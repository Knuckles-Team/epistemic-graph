//! Implicit-reasoning item recommendation over temporal semantic ids
//! (MEDULLA-DECISION-ENGINE-R175.1).

use serde::{Deserialize, Deserializer, Serialize};

pub const MAX_PAUSE_STEPS: u8 = 8;
pub const MAX_RECOMMEND_TOP_K: u16 = 100;

/// A time bucket followed by four residual-quantization codes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct SemanticId {
    pub time_bucket: u16,
    pub codes: [u8; 4],
}

fn pause_steps<'de, D: Deserializer<'de>>(deserializer: D) -> Result<u8, D::Error> {
    let value = u8::deserialize(deserializer)?;
    if value > MAX_PAUSE_STEPS {
        return Err(serde::de::Error::custom("pause_steps must be 0 to 8"));
    }
    Ok(value)
}

fn top_k<'de, D: Deserializer<'de>>(deserializer: D) -> Result<u16, D::Error> {
    let value = u16::deserialize(deserializer)?;
    if value == 0 || value > MAX_RECOMMEND_TOP_K {
        return Err(serde::de::Error::custom("top_k must be 1 to 100"));
    }
    Ok(value)
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct RecommendRequestV1 {
    pub catalog_ref: String,
    pub query_embedding: Vec<f32>,
    pub history_item_ids: Vec<String>,
    #[serde(deserialize_with = "pause_steps")]
    pub pause_steps: u8,
    #[serde(deserialize_with = "top_k")]
    pub top_k: u16,
    pub as_of: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct RecommendedItem {
    pub item_id: String,
    pub semantic_id: SemanticId,
    /// Cosine similarity on the Q32 scale.
    pub score_q32: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct RecommendAnswerV1 {
    pub catalog_ref: String,
    pub as_of: i64,
    pub items: Vec<RecommendedItem>,
}

#[cfg(test)]
mod tests;
