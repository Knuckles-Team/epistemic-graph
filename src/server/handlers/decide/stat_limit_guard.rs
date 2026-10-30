//! The limit guard on the served statistical `Decide`
//! (EG-DECISION-ENGINE-R131.3 and R131.4).
//!
//! A request that names the action its answer would advise is checked against
//! the resolved policy's limit rules before any option becomes advisory. A
//! breach replaces the answer with an abstention naming the first breached
//! rule; the engine performs nothing either way.

use eg_types::contract::BoundedVec;
use serde::{Deserialize, Serialize};

use eg_types::decision::limit_guard::{check_limits, limit_refusal, GuardedAction};
use eg_types::decision::statistical::{StatisticalDecisionRecord, StatisticalOutcome};

use super::stat_executor::Executed;
use super::stat_support::ResolvedPolicy;

/// Replace the executed outcome with a `limit_breach` abstention when the
/// request's guarded action breaches a policy limit. Without a guarded action
/// or limit rules the executed answer is returned unchanged.
pub(super) fn guard(
    mut executed: Executed,
    guarded: Option<&GuardedAction>,
    policy: &ResolvedPolicy,
) -> Executed {
    let Some(action) = guarded else {
        return executed;
    };
    let breaches = check_limits(action, policy.statistical.limit_rules.as_slice());
    if let Some(reason) = limit_refusal(&breaches) {
        executed.ladder.outcome = StatisticalOutcome::Abstained {
            reasons: BoundedVec::new(vec![reason]).expect("one reason is inside the bound"),
        };
        executed.ladder.calibration = None;
        executed.ladder.explained = None;
        executed.ladder.logging = Vec::new();
    }
    executed
}

/// One stored limit breach, an observation linked to the signal it affected
/// (EG-DECISION-ENGINE-R131.5).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct BreachObservation {
    pub record_id: String,
    pub signal_iri: String,
    pub rule_id: String,
    pub observed: i64,
    pub bound: i64,
}

/// Key of one observation, grouped by the signal it affects.
pub(super) fn observation_key(signal_iri: &str, record_id: &str, rule_id: &str) -> String {
    format!("limit_breach:{signal_iri}:{record_id}:{rule_id}")
}

/// The observation rows a committed record writes: one per breach, re-derived
/// from the sealed action and the pinned policy. Empty without a breach.
pub(super) fn observation_rows(
    record: &StatisticalDecisionRecord,
    policy: &ResolvedPolicy,
) -> Result<Vec<(String, Vec<u8>)>, String> {
    let Some(action) = &record.inputs.guarded_action else {
        return Ok(Vec::new());
    };
    check_limits(action, policy.statistical.limit_rules.as_slice())
        .into_iter()
        .map(|breach| {
            let key = observation_key(&breach.signal_iri, &record.record_id, &breach.rule_id);
            let row = BreachObservation {
                record_id: record.record_id.clone(),
                signal_iri: breach.signal_iri,
                rule_id: breach.rule_id,
                observed: breach.observed,
                bound: breach.bound,
            };
            Ok((
                key,
                crate::server::persistence::decision_jobs::encode_artifact(&row)?,
            ))
        })
        .collect()
}

/// Every stored breach observation of one signal.
pub(super) fn breaches_of_signal(
    store: &crate::server::persistence::agent_library::AgentLibraryStore,
    tenant_id: &str,
    signal_iri: &str,
) -> Result<Vec<BreachObservation>, String> {
    store
        .decision_artifacts_with_prefix(tenant_id, &format!("limit_breach:{signal_iri}:"), 256)?
        .into_iter()
        .map(|(_, bytes)| {
            crate::server::persistence::decision_jobs::decode_artifact(&bytes, "limit breach")
        })
        .collect()
}
