//! `Method::DecisionFit` and `Method::DecisionEval`: the two admin jobs behind
//! a calibrated decision head.
//!
//! Owned after S1 by the statistical package, which replaces both stub bodies
//! with the durable job rows, the fitting optimiser and the off-policy
//! estimators.

use crate::server::contract_wave::contract_wave_stub;

contract_wave_stub! {
    /// Submit or read one head-fitting job.
    handle_decision_fit(eg_types::decision::DecisionFitOp)
        refuses "DecisionFit", tested by fit_stub_tests
}

contract_wave_stub! {
    /// Submit or read one head-evaluation job.
    handle_decision_eval(eg_types::decision::DecisionEvalOp)
        refuses "DecisionEval", tested by eval_stub_tests
}
