//! Per-tenant usage ledger model (EG-DECISION-ENGINE-R801).
//!
//! A [`UsageRecord`] is one observed model call, recorded under the tenant of
//! the verified caller. Prompt and completion content never cross the wire or
//! reach the store: only their [`UsageRecord::content_digest`] does.

use serde::{Deserialize, Serialize};

/// The closed outcome of one model call.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UsageOutcome {
    /// The call completed and produced a completion.
    Ok,
    /// The call failed before producing a usable completion.
    Error,
}

/// One observed model call, owned by the tenant of the verified caller.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UsageRecord {
    /// The tenant that owns the record (the verified caller's tenant).
    pub tenant: String,
    /// The principal that made the call.
    pub principal: String,
    /// The run the call belongs to.
    pub run_id: String,
    /// The model key as reported by the caller, unnormalized.
    pub model_key: String,
    /// Input tokens billed on the call.
    pub tokens_in: u64,
    /// Output tokens produced by the call.
    pub tokens_out: u64,
    /// Cached input tokens reused from the prefix cache.
    pub tokens_cached: u64,
    /// Wall-clock latency of the call, in milliseconds.
    pub latency_ms: u64,
    /// Whether the call succeeded.
    pub outcome: UsageOutcome,
    /// Digest of the prompt and completion content; the content itself is
    /// never stored.
    pub content_digest: String,
}
