//! The limit guard on the served statistical `Decide`
//! (EG-DECISION-ENGINE-R131.3 and R131.4).
//!
//! A request that names the action its answer would advise is checked against
//! the resolved policy's limit rules before any option becomes advisory. A
//! breach replaces the answer with an abstention naming the first breached
//! rule; the engine performs nothing either way.

use eg_types::contract::BoundedVec;
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

/// The breaches a committed record's sealed action makes against its pinned
/// policy, re-derived on the commit path (EG-DECISION-ENGINE-R131.5). Empty
/// without a guarded action.
pub(crate) async fn breaches_of_commit(
    state: &super::SharedState,
    verified: &crate::server::auth::VerifiedRequestContext,
    record: &StatisticalDecisionRecord,
) -> Result<Vec<eg_types::decision::limit_guard::LimitBreach>, String> {
    let Some(action) = &record.inputs.guarded_action else {
        return Ok(Vec::new());
    };
    let store = state.write().await.ensure_agent_library()?;
    let policy = super::stat_support::resolve_policy(
        store.as_ref(),
        verified.tenant(),
        &record.inputs.policy,
    )?;
    Ok(check_limits(action, policy.statistical.limit_rules.as_slice()))
}
