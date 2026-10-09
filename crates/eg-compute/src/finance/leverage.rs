// CONCEPT:EG-KG.domains.finance-compute.leverage — Leverage Order Governance
//
// Live-order eligibility boundary (EG-FINANCE-PRIMITIVES-R010.1): a live
// leveraged order is eligible only with a matching per-instrument policy and
// a designated approver. Analysis defaults to paper mode, so a paper-mode
// order is never eligible for live authorization. CFDs are flagged
// unavailable for US retail accounts regardless of any policy. Margin-call,
// liquidation and position-sizing scenario simulation are later slices
// (R010.2+).

use serde::{Deserialize, Serialize};

/// Whether an order is analyzed only (paper; never placed or authorized) or
/// live. Paper is the default for every new analysis.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum OrderMode {
    Paper,
    Live,
}

/// A per-instrument policy authorizing live leveraged orders, naming its
/// designated approvers.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LeveragePolicy {
    pub instrument_id: String,
    pub approvers: Vec<String>,
    /// CFDs stay unavailable for US retail accounts even under this policy.
    pub cfd_unavailable_us_retail: bool,
}

/// Why a live leveraged order was refused.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "reason", rename_all = "snake_case")]
pub enum LiveOrderRefusal {
    /// The default: an order is paper-mode unless explicitly made live.
    NotLive,
    NoPolicyForInstrument {
        instrument_id: String,
    },
    ApproverNotDesignated {
        approver: String,
    },
    CfdUnavailableForUsRetail {
        instrument_id: String,
    },
}

/// Decide whether a live leveraged order is eligible.
///
/// A paper-mode order is never eligible: there is nothing to authorize. A
/// live order is eligible only when a policy names both the instrument and
/// the acting approver, and a CFD for a US retail account is refused even
/// under a matching policy.
pub fn live_order_eligibility(
    mode: OrderMode,
    instrument_id: &str,
    approver: &str,
    is_cfd: bool,
    is_us_retail: bool,
    policy: Option<&LeveragePolicy>,
) -> Result<(), LiveOrderRefusal> {
    if mode == OrderMode::Paper {
        return Err(LiveOrderRefusal::NotLive);
    }
    let policy = match policy {
        Some(policy) if policy.instrument_id == instrument_id => policy,
        _ => {
            return Err(LiveOrderRefusal::NoPolicyForInstrument {
                instrument_id: instrument_id.to_string(),
            })
        }
    };
    if is_cfd && is_us_retail && policy.cfd_unavailable_us_retail {
        return Err(LiveOrderRefusal::CfdUnavailableForUsRetail {
            instrument_id: instrument_id.to_string(),
        });
    }
    if !policy.approvers.iter().any(|a| a == approver) {
        return Err(LiveOrderRefusal::ApproverNotDesignated {
            approver: approver.to_string(),
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn policy() -> LeveragePolicy {
        LeveragePolicy {
            instrument_id: "futures:ES".to_string(),
            approvers: vec!["user:alice".to_string()],
            cfd_unavailable_us_retail: true,
        }
    }

    #[test]
    fn paper_mode_is_never_eligible_for_a_live_order() {
        let err = live_order_eligibility(
            OrderMode::Paper,
            "futures:ES",
            "user:alice",
            false,
            false,
            Some(&policy()),
        )
        .unwrap_err();
        assert_eq!(err, LiveOrderRefusal::NotLive);
    }

    #[test]
    fn a_live_order_without_a_matching_policy_is_refused() {
        let err = live_order_eligibility(
            OrderMode::Live,
            "futures:ES",
            "user:alice",
            false,
            false,
            None,
        )
        .unwrap_err();
        assert!(matches!(
            err,
            LiveOrderRefusal::NoPolicyForInstrument { .. }
        ));
    }

    #[test]
    fn a_live_order_with_an_undesignated_approver_is_refused() {
        let err = live_order_eligibility(
            OrderMode::Live,
            "futures:ES",
            "user:mallory",
            false,
            false,
            Some(&policy()),
        )
        .unwrap_err();
        assert_eq!(
            err,
            LiveOrderRefusal::ApproverNotDesignated {
                approver: "user:mallory".to_string()
            }
        );
    }

    #[test]
    fn a_cfd_is_refused_for_us_retail_regardless_of_policy() {
        let err = live_order_eligibility(
            OrderMode::Live,
            "futures:ES",
            "user:alice",
            true,
            true,
            Some(&policy()),
        )
        .unwrap_err();
        assert_eq!(
            err,
            LiveOrderRefusal::CfdUnavailableForUsRetail {
                instrument_id: "futures:ES".to_string()
            }
        );
    }

    #[test]
    fn a_live_order_with_a_matching_policy_and_approver_is_eligible() {
        assert!(live_order_eligibility(
            OrderMode::Live,
            "futures:ES",
            "user:alice",
            false,
            false,
            Some(&policy()),
        )
        .is_ok());
    }
}
