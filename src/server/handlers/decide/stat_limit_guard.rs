//! The limit guard on the served statistical `Decide`
//! (EG-DECISION-ENGINE-R131.3 and R131.4).
//!
//! A request that names the action its answer would advise is checked against
//! the resolved policy's limit rules before any option becomes advisory. A
//! breach replaces the answer with an abstention naming the first breached
//! rule; the engine performs nothing either way.

use eg_types::contract::BoundedVec;
use eg_types::decision::limit_guard::{check_limits, limit_refusal};
use eg_types::decision::statistical::{DecideRequest, StatisticalOutcome};

use super::stat_executor::Executed;
use super::stat_support::ResolvedPolicy;

/// Replace the executed outcome with a `limit_breach` abstention when the
/// request's guarded action breaches a policy limit. Without a guarded action
/// or limit rules the executed answer is returned unchanged.
pub(super) fn guard(
    mut executed: Executed,
    request: &DecideRequest,
    policy: &ResolvedPolicy,
) -> Executed {
    let Some(action) = &request.guarded_action else {
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
