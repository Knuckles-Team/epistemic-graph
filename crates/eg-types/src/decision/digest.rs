//! The one digest scheme the decision surface uses.
//!
//! `sha256(domain ‖ 0x00 ‖ compact-JSON(value))`, rendered `sha256:<hex>`. It
//! is the scheme the solver certificate already uses, and Python reproduces it
//! with `json.dumps(..., separators=(",", ":"), ensure_ascii=False)` over the
//! same model in field order. Every value it is applied to holds no float and
//! no non-string map key, so its JSON encoding is a pure function of the value.

use serde::Serialize;

use super::policy::DecisionPolicy;
use super::record::{DecisionInputs, DecisionRecord};
use super::statistical::StatisticalDecisionRecord;
use crate::agent_library::AgentLibraryLifecycle;
use crate::solve::Sha256Digest;

/// Domain of a v1 (assembly) decision record.
pub const DECISION_RECORD_DIGEST_DOMAIN: &str = "eg/decision-record/v1";
/// Domain of a v2 (statistical) decision record.
pub const STATISTICAL_RECORD_DIGEST_DOMAIN: &str = "eg/decision-record/v2";
/// Domain of a decision's input set.
pub const DECISION_INPUTS_DIGEST_DOMAIN: &str = "eg/decision-inputs/v1";
/// Domain of the candidate catalog a decision was taken against.
pub const DECISION_CATALOG_DIGEST_DOMAIN: &str = "eg/decision-catalog/v1";
/// Domain of a decision policy body.
pub const DECISION_POLICY_DIGEST_DOMAIN: &str = "eg/decision-policy/v1";

/// The textual prefix every decision digest carries.
pub const DIGEST_TEXT_PREFIX: &str = "sha256:";

/// One catalog member, as the catalog digest sees it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
struct CatalogMember<'a> {
    component_id: &'a str,
    definition_digest: &'a str,
    lifecycle: AgentLibraryLifecycle,
}

/// Digest `value` under `domain`, in the wave's one text form.
pub fn digest_text<T: Serialize>(domain: &str, value: &T) -> String {
    let digest = String::from(Sha256Digest::of_json(domain, value));
    format!("{DIGEST_TEXT_PREFIX}{digest}")
}

/// The digest of one assembly record.
///
/// Computed over the record with its own `record_digest` field cleared, which
/// is the only way a field can carry the digest of the value that contains it.
pub fn record_digest(record: &DecisionRecord) -> String {
    let mut subject = record.clone();
    subject.record_digest = String::new();
    digest_text(DECISION_RECORD_DIGEST_DOMAIN, &subject)
}

/// The digest of one statistical record, cleared the same way.
pub fn statistical_record_digest(record: &StatisticalDecisionRecord) -> String {
    let mut subject = record.clone();
    subject.record_digest = String::new();
    digest_text(STATISTICAL_RECORD_DIGEST_DOMAIN, &subject)
}

/// The digest of a decision's input set.
pub fn inputs_digest(inputs: &DecisionInputs) -> String {
    digest_text(DECISION_INPUTS_DIGEST_DOMAIN, inputs)
}

/// The digest of a decision policy body.
pub fn policy_digest(policy: &DecisionPolicy) -> String {
    digest_text(DECISION_POLICY_DIGEST_DOMAIN, policy)
}

/// The digest of the candidate catalog a decision was taken against.
///
/// Sorted here rather than trusted from the caller: the catalog is read from a
/// store whose iteration order is an implementation detail, and a digest that
/// depended on it would be a compare-and-set that fails at random.
pub fn catalog_digest(members: &[(&str, &str, AgentLibraryLifecycle)]) -> String {
    let mut sorted: Vec<CatalogMember<'_>> = members
        .iter()
        .map(
            |(component_id, definition_digest, lifecycle)| CatalogMember {
                component_id,
                definition_digest,
                lifecycle: *lifecycle,
            },
        )
        .collect();
    sorted.sort_unstable_by_key(|member| (member.component_id, member.definition_digest));
    digest_text(DECISION_CATALOG_DIGEST_DOMAIN, &sorted)
}
