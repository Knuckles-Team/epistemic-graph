//! EG-UNIFIED-DATA-PLANE-R006 — pushing down virtual-graph queries to source SQL. This is the
//! `.1` typed-model slice: the typed pushdown decision and its explicit fallback reason, and the
//! refusal of a self-contradictory decision (claiming both success and a fallback reason).
//! Rendering actual joins/aggregates/ORDER BY/LIMIT into source SQL is a later child.

use std::fmt;

/// Why EG could not push a fragment of a query plan down to the source, and instead ran it
/// in-memory. EXPLAIN reports this reason whenever pushdown did not happen.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PushdownFallbackReason {
    CrossSourceJoin,
    UnsupportedAggregate,
    TypedCastUnavailable,
    UnsupportedOrderOrLimit,
}

impl fmt::Display for PushdownFallbackReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let msg = match self {
            Self::CrossSourceJoin => "join spans more than one source",
            Self::UnsupportedAggregate => "aggregate has no source-native equivalent",
            Self::TypedCastUnavailable => "a typed value cast is unavailable on the source",
            Self::UnsupportedOrderOrLimit => "ORDER BY/LIMIT form is not source-renderable",
        };
        write!(f, "{msg}")
    }
}

/// Whether one query-plan fragment was pushed down to source SQL, and if not, why.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PushdownDecision {
    pub pushed_down: bool,
    pub fallback_reason: Option<PushdownFallbackReason>,
}

/// A decision claims both that pushdown succeeded and that a fallback reason applies: those are
/// mutually exclusive, so the decision is refused rather than silently favoring one half.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct InconsistentPushdownDecision(pub PushdownFallbackReason);

impl fmt::Display for InconsistentPushdownDecision {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "pushdown decision claims success AND fallback reason ({}); these are mutually exclusive",
            self.0
        )
    }
}

impl std::error::Error for InconsistentPushdownDecision {}

impl PushdownDecision {
    /// Refuses the self-contradictory state `pushed_down == true` with a fallback reason set.
    pub fn validate(&self) -> Result<(), InconsistentPushdownDecision> {
        if self.pushed_down {
            if let Some(reason) = self.fallback_reason {
                return Err(InconsistentPushdownDecision(reason));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // spec: EG-UNIFIED-DATA-PLANE-R006.1
    #[test]
    fn a_pushed_down_decision_with_no_fallback_validates() {
        let decision = PushdownDecision {
            pushed_down: true,
            fallback_reason: None,
        };
        decision.validate().unwrap();
    }

    // spec: EG-UNIFIED-DATA-PLANE-R006.1
    #[test]
    fn a_fallback_decision_validates() {
        let decision = PushdownDecision {
            pushed_down: false,
            fallback_reason: Some(PushdownFallbackReason::CrossSourceJoin),
        };
        decision.validate().unwrap();
    }

    // spec: EG-UNIFIED-DATA-PLANE-R006.1
    #[test]
    fn a_contradictory_decision_is_refused() {
        let decision = PushdownDecision {
            pushed_down: true,
            fallback_reason: Some(PushdownFallbackReason::UnsupportedAggregate),
        };
        let err = decision.validate().unwrap_err();
        assert_eq!(err.0, PushdownFallbackReason::UnsupportedAggregate);
        assert!(err.to_string().contains("mutually exclusive"));
    }
}
