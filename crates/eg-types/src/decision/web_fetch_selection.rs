//! Web page selection contract (MEDULLA-DECISION-ENGINE-R802).
//!
//! `SelectWebPages` takes the candidate rows of one web search and returns
//! the pages worth fetching, in order, each with its reasons. It is a pure,
//! deterministic decision: no model runs, and the same request against the
//! same held Documents always yields the same answer. Scores are integers in
//! millionths so the answer is exact on every platform.

use serde::{Deserialize, Serialize};

/// The most candidates one request may carry.
pub const MAX_WEB_CANDIDATES: usize = 256;
/// The most pages one request may select.
pub const MAX_WEB_FETCHES: u32 = 32;
/// The default freshness window: a held copy younger than this is not refetched.
pub const DEFAULT_FRESHNESS_WINDOW_S: u64 = 86_400;

/// One search result row.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct WebCandidate {
    pub url: String,
    #[serde(default)]
    pub title: String,
    #[serde(default)]
    pub snippet: String,
    /// The search engine that returned the row.
    #[serde(default)]
    pub engine: String,
    /// The engine's 1-based rank for the row.
    pub rank: u32,
    /// `sha256:<hex>` of the page content, when already known.
    #[serde(default)]
    pub content_digest: Option<String>,
    /// Publication time in Unix milliseconds, when the engine reports one.
    #[serde(default)]
    pub published_at_ms: Option<u64>,
}

/// Request body of `SelectWebPages`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct SelectWebPagesRequest {
    pub query: String,
    pub candidates: Vec<WebCandidate>,
    /// The fetch budget, 1 to 32.
    pub max_fetches: u32,
    /// Absent is 86 400 s.
    #[serde(default)]
    pub freshness_window_s: Option<u64>,
    /// The decision clock in Unix milliseconds. Absent reads the server
    /// clock; pass it to make a replay exact.
    #[serde(default)]
    pub now_ms: Option<u64>,
}

impl SelectWebPagesRequest {
    /// Refuse a request outside its bounds.
    pub fn validate(&self) -> Result<(), String> {
        if self.query.trim().is_empty() {
            return Err("WEB_SELECTION_QUERY_MISSING: query must not be empty".into());
        }
        if self.candidates.is_empty() {
            return Err("WEB_SELECTION_NO_CANDIDATES: no candidates to select from".into());
        }
        if self.candidates.len() > MAX_WEB_CANDIDATES {
            return Err(format!(
                "WEB_SELECTION_TOO_MANY_CANDIDATES: at most {MAX_WEB_CANDIDATES} candidates"
            ));
        }
        if !(1..=MAX_WEB_FETCHES).contains(&self.max_fetches) {
            return Err(format!(
                "WEB_SELECTION_BUDGET_INVALID: max_fetches must be in 1..={MAX_WEB_FETCHES}"
            ));
        }
        Ok(())
    }
}

/// Why a candidate was not selected.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum SkipReason {
    /// Another row has the same canonical URL; its rank was fused into it.
    DuplicateUrl,
    /// Another row, or a held Document, has the same content digest.
    DuplicateDigest,
    /// A held Document for the URL is younger than the freshness window.
    HeldFresh,
    /// The URL is not an absolute http or https URL.
    InvalidUrl,
    /// The candidate ranked below the fetch budget.
    OverBudget,
}

/// One selected page.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct SelectedPage {
    /// 0-based fetch order.
    pub order: u32,
    pub url: String,
    pub canonical_url: String,
    pub title: String,
    /// Weighted total in millionths.
    pub score_micros: u64,
    pub relevance_micros: u64,
    pub rank_micros: u64,
    pub freshness_micros: u64,
    /// The engines that returned the URL, sorted.
    pub engines: Vec<String>,
    /// True when a held copy exists but is older than the window.
    pub refresh: bool,
    /// Human-readable reasons, in a fixed order.
    pub reasons: Vec<String>,
}

/// One candidate that was not selected.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct SkippedPage {
    pub url: String,
    pub canonical_url: String,
    pub reason: SkipReason,
    pub detail: String,
}

/// Result body of `SelectWebPages`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct SelectWebPagesAnswer {
    /// `sha256:<hex>` of the trimmed query text.
    pub query_digest: String,
    pub selected: Vec<SelectedPage>,
    pub skipped: Vec<SkippedPage>,
    pub max_fetches: u32,
    pub considered: u32,
    pub now_ms: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request(max_fetches: u32) -> SelectWebPagesRequest {
        SelectWebPagesRequest {
            query: "rust chunking".into(),
            candidates: vec![WebCandidate {
                url: "https://example.org/a".into(),
                title: "A".into(),
                snippet: String::new(),
                engine: "searxng".into(),
                rank: 1,
                content_digest: None,
                published_at_ms: None,
            }],
            max_fetches,
            freshness_window_s: None,
            now_ms: Some(1),
        }
    }

    // spec: MEDULLA-DECISION-ENGINE-R802.1
    #[test]
    fn question_round_trips() {
        let original = request(4);
        let bytes = rmp_serde::to_vec_named(&original).unwrap();
        assert_eq!(
            rmp_serde::from_slice::<SelectWebPagesRequest>(&bytes).unwrap(),
            original
        );
        assert!(original.validate().is_ok());
        assert!(request(33)
            .validate()
            .unwrap_err()
            .contains("BUDGET_INVALID"));
        assert!(request(0).validate().is_err());
        let mut empty = request(1);
        empty.candidates.clear();
        assert!(empty.validate().unwrap_err().contains("NO_CANDIDATES"));
        assert!(serde_json::from_value::<SkipReason>(serde_json::json!("too_short")).is_err());
        assert_eq!(
            serde_json::to_value(SkipReason::HeldFresh).unwrap(),
            "held_fresh"
        );
    }
}
