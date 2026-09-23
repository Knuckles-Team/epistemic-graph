//! What the statistical handlers share: reading a pinned catalog component's
//! typed body, resolving the decision policy, and the engine's default
//! statistical levels.
//!
//! A pin is checked in full before a byte of its body is trusted: the
//! component must exist in the caller's tenant, be of the pinned kind, and one
//! of its retained revisions must carry the pinned definition digest. The body
//! is then re-hashed against the revision's content digest.

use serde::de::DeserializeOwned;

use eg_types::agent_component::{AgentComponentEntry, AgentComponentKind, ComponentDependency};
use eg_types::decision::digest::policy_digest;
use eg_types::decision::statistical::body::decode_body;
use eg_types::decision::statistical::StatisticalErrorCode;
use eg_types::decision::{
    ColdStart, DecisionPolicy, DecisionPolicyRef, ObjectiveLevelKind, ObjectiveOrder,
    QuantScaleTag, QuantisedValue, StatisticalPolicy, TraceFidelityLevel, UnitRationalWire,
    UnknownCostRule, DECISION_POLICY_SCHEMA_VERSION,
};

use crate::server::persistence::agent_library::AgentLibraryStore;

/// The refusal text of a statistical-surface failure.
pub(super) fn refusal(code: StatisticalErrorCode, detail: impl std::fmt::Display) -> String {
    code.refusal(detail)
}

/// The revision a pin names, checked for tenant, kind and digest.
pub(super) fn pinned_entry(
    store: &AgentLibraryStore,
    tenant_id: &str,
    pin: &ComponentDependency,
    kind: AgentComponentKind,
) -> Result<AgentComponentEntry, String> {
    let mismatch = |detail: &str| {
        refusal(
            StatisticalErrorCode::ComponentPinMismatch,
            format!("{} {detail}", pin.component_id),
        )
    };
    if pin.kind != kind {
        return Err(mismatch("is pinned as the wrong kind"));
    }
    store
        .component_revisions(tenant_id, &pin.component_id)?
        .into_iter()
        .rev()
        .find(|entry| entry.definition_digest == pin.definition_digest)
        .filter(|entry| entry.kind == kind)
        .ok_or_else(|| mismatch("has no revision at the pinned definition digest"))
}

/// Decode the typed body of a pinned component.
pub(super) fn pinned_body<T: DeserializeOwned>(
    store: &AgentLibraryStore,
    tenant_id: &str,
    pin: &ComponentDependency,
    kind: AgentComponentKind,
    code: StatisticalErrorCode,
) -> Result<(T, String), String> {
    let entry = pinned_entry(store, tenant_id, pin, kind)?;
    let body = decode_body(&entry.content_digest, &entry.attributes)
        .map_err(|detail| refusal(code, detail))?;
    Ok((body, entry.content_digest))
}

fn rational(numerator: u64, denominator: u64) -> UnitRationalWire {
    UnitRationalWire::new(numerator, denominator).expect("default levels are unit rationals")
}

/// The engine's default statistical levels: alpha 1/10, epsilon 1/20, delta
/// 1/20, `n_min` 100 per class, k-anonymity 10, minimum ESS 50, tool-call
/// fidelity, and the ruled 5% audit sample of acted decisions.
pub(super) fn default_statistical_policy() -> StatisticalPolicy {
    StatisticalPolicy {
        alpha: rational(1, 10),
        epsilon: rational(1, 20),
        delta: rational(1, 20),
        n_min: 100,
        min_support: 10,
        min_ess: QuantisedValue {
            scale: QuantScaleTag::Q32,
            value: 50 << 32,
        },
        min_outcome_fidelity: TraceFidelityLevel::ToolCalls,
        tenant_public_features: false,
        audit_sample: rational(1, 20),
    }
}

/// The engine default policy. A shim with the same values as the assembly
/// lane's `DecisionPolicy::engine_default()`, deleted in its favour at landing.
fn engine_default_policy() -> DecisionPolicy {
    DecisionPolicy {
        schema_version: DECISION_POLICY_SCHEMA_VERSION,
        objective: ObjectiveOrder::Lexicographic {
            levels: eg_types::contract::BoundedVec::new(vec![
                ObjectiveLevelKind::Uncovered,
                ObjectiveLevelKind::Components,
                ObjectiveLevelKind::DeclaredCost,
                ObjectiveLevelKind::DeclaredP95Latency,
            ])
            .expect("four levels fit the eight-level bound"),
        },
        unknown_cost: UnknownCostRule::ExcludeWhenStrict,
        accepted_gap: eg_types::solve::Scalar::new(0),
        node_budget: eg_types::decision::policy::DEFAULT_DECISION_NODE_BUDGET,
        max_templates: 8,
        max_slots: 6,
        max_nogood_rounds: 8,
        max_why_not_per_slot: 3,
        a2a_requires_observation: true,
        cold_start: ColdStart::DeterministicOnly,
        statistical: None,
    }
}

/// The attribute a published `DecisionPolicy` carries its canonical JSON in.
/// The assembly lane's encoding (`eg_types::decision::policy::
/// {DECISION_POLICY_ATTRIBUTE, policy_from_attributes}`), mirrored until that
/// lands and then deleted in its favour, so there is one policy codec.
pub(super) const DECISION_POLICY_ATTRIBUTE: &str = "decision.policy";

fn policy_from_attributes(
    attributes: &std::collections::BTreeMap<String, String>,
    content_digest: &str,
) -> Result<DecisionPolicy, eg_types::decision::DecisionErrorCode> {
    let unreadable = eg_types::decision::DecisionErrorCode::DecisionReplayMismatch;
    let body = attributes
        .get(DECISION_POLICY_ATTRIBUTE)
        .ok_or(unreadable)?;
    let policy: DecisionPolicy = serde_json::from_str(body).map_err(|_| unreadable)?;
    if policy_digest(&policy) != content_digest {
        return Err(unreadable);
    }
    policy.checked()
}

/// A resolved policy: the body, its digest and its statistical half.
pub(super) struct ResolvedPolicy {
    pub(super) policy: DecisionPolicy,
    pub(super) digest: String,
    pub(super) statistical: StatisticalPolicy,
}

/// Resolve `reference` for `tenant_id`.
pub(super) fn resolve_policy(
    store: &AgentLibraryStore,
    tenant_id: &str,
    reference: &DecisionPolicyRef,
) -> Result<ResolvedPolicy, String> {
    let policy = match reference {
        DecisionPolicyRef::Default => engine_default_policy(),
        DecisionPolicyRef::Pinned { component } => {
            let entry = pinned_entry(
                store,
                tenant_id,
                component,
                AgentComponentKind::DecisionPolicy,
            )?;
            policy_from_attributes(&entry.attributes, &entry.content_digest)
                .map_err(|code| format!("{code}: pinned decision policy"))?
        }
    };
    let statistical = policy
        .statistical
        .clone()
        .unwrap_or_else(default_statistical_policy);
    Ok(ResolvedPolicy {
        digest: policy_digest(&policy),
        policy,
        statistical,
    })
}

/// The engine default policy, for tests that pin a variant of it.
#[cfg(test)]
pub(super) fn default_policy_for_tests() -> DecisionPolicy {
    engine_default_policy()
}
