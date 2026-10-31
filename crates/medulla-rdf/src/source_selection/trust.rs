//! Untrusted taint mark on content that enters a run (MEDULLA-SRC-AUTH-R011).
//!
//! Content from a tool result, a retrieved document or an upload is
//! `untrusted`; content from a `live_truth` source with an approved mapping is
//! `trusted`; derived content is `untrusted` when any input is. Each rule is a
//! named, replaceable default ([`TrustMarkPolicy`]). A stored mark carries the
//! rule that set it and the generation it was set at, and it is sticky: lowering
//! `untrusted` to `trusted` needs an operator approval identifier.

use serde::{Deserialize, Serialize};

use super::SourceRole;

/// The stable code of a refused `untrusted` to `trusted` change.
pub const TRUST_MARK_DOWNGRADE_REFUSED: &str = "TRUST_MARK_DOWNGRADE_REFUSED";

/// The closed trust mark of one content node (`vg:trustMark`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TrustMark {
    Trusted,
    Untrusted,
}

/// Where a piece of content came from.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ContentOrigin {
    ToolResult,
    RetrievedDocument,
    Upload,
    /// Read from a connected source through a mapping.
    Source {
        approved_mapping: bool,
    },
    /// Computed from other content, carrying each input's mark.
    Derived {
        inputs: Vec<TrustMark>,
    },
}

/// The replaceable marking rules. The returned `&'static str` names the rule
/// that decided, stored as `markedBy`.
pub trait TrustMarkPolicy {
    fn mark(&self, origin: &ContentOrigin, role: Option<SourceRole>) -> (TrustMark, &'static str);
}

/// The default rules of MEDULLA-SRC-AUTH-R011.2.
#[derive(Clone, Copy, Debug, Default)]
pub struct DefaultTrustMarkPolicy;

impl TrustMarkPolicy for DefaultTrustMarkPolicy {
    fn mark(&self, origin: &ContentOrigin, role: Option<SourceRole>) -> (TrustMark, &'static str) {
        match origin {
            ContentOrigin::ToolResult => (TrustMark::Untrusted, "tool_result_untrusted"),
            ContentOrigin::RetrievedDocument => {
                (TrustMark::Untrusted, "retrieved_document_untrusted")
            }
            ContentOrigin::Upload => (TrustMark::Untrusted, "upload_untrusted"),
            ContentOrigin::Source {
                approved_mapping: true,
            } if role == Some(SourceRole::LiveTruth) => {
                (TrustMark::Trusted, "live_truth_approved_trusted")
            }
            ContentOrigin::Source { .. } => (TrustMark::Untrusted, "source_unapproved_untrusted"),
            ContentOrigin::Derived { inputs } if inputs.contains(&TrustMark::Untrusted) => {
                (TrustMark::Untrusted, "derived_inherits_untrusted")
            }
            ContentOrigin::Derived { inputs } if inputs.is_empty() => {
                (TrustMark::Untrusted, "derived_without_inputs_untrusted")
            }
            ContentOrigin::Derived { .. } => (TrustMark::Trusted, "derived_all_trusted"),
        }
    }
}

/// The default mark for content of `origin` read from a source of `role`.
pub fn mark_for_origin(origin: &ContentOrigin, role: Option<SourceRole>) -> TrustMark {
    DefaultTrustMarkPolicy.mark(origin, role).0
}

/// A stored mark with its evidence (MEDULLA-SRC-AUTH-R011.5).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoredTrustMark {
    pub mark: TrustMark,
    #[serde(rename = "markedBy")]
    pub marked_by: String,
    #[serde(rename = "markedAtGeneration")]
    pub marked_at_generation: u64,
    /// The operator approval that lowered the mark, when one did.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub approval_id: Option<String>,
}

impl StoredTrustMark {
    /// Mark content under `policy` at `generation`.
    pub fn stamp(
        policy: &impl TrustMarkPolicy,
        origin: &ContentOrigin,
        role: Option<SourceRole>,
        generation: u64,
    ) -> Self {
        let (mark, rule) = policy.mark(origin, role);
        Self {
            mark,
            marked_by: rule.to_string(),
            marked_at_generation: generation,
            approval_id: None,
        }
    }
}

/// A refused mark change: names the node and the missing approval.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TrustMarkRefusal {
    pub code: &'static str,
    pub node: String,
    pub missing: &'static str,
}

impl std::fmt::Display for TrustMarkRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}: node {} needs {} to become trusted",
            self.code, self.node, self.missing
        )
    }
}

impl std::error::Error for TrustMarkRefusal {}

/// Apply a requested mark to `node`. Raising to `untrusted` is always allowed;
/// lowering `untrusted` to `trusted` needs a non-blank operator approval id
/// (MEDULLA-SRC-AUTH-R011.4).
pub fn change_trust_mark(
    node: &str,
    current: &StoredTrustMark,
    requested: TrustMark,
    approval_id: Option<&str>,
    generation: u64,
) -> Result<StoredTrustMark, TrustMarkRefusal> {
    if current.mark == requested {
        return Ok(current.clone());
    }
    let approval = approval_id.map(str::trim).filter(|id| !id.is_empty());
    match (requested, approval) {
        (TrustMark::Trusted, None) => Err(TrustMarkRefusal {
            code: TRUST_MARK_DOWNGRADE_REFUSED,
            node: node.to_string(),
            missing: "operator_approval_id",
        }),
        (TrustMark::Trusted, Some(id)) => Ok(StoredTrustMark {
            mark: TrustMark::Trusted,
            marked_by: "operator_approval".to_string(),
            marked_at_generation: generation,
            approval_id: Some(id.to_string()),
        }),
        (TrustMark::Untrusted, _) => Ok(StoredTrustMark {
            mark: TrustMark::Untrusted,
            marked_by: "explicit_untrusted".to_string(),
            marked_at_generation: generation,
            approval_id: None,
        }),
    }
}
