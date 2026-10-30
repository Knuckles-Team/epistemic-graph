//! The refusal codes of native program optimization and promotion
//! (EG-DECISION-ENGINE-R810.4, R812.4). Registered in
//! [`super::declared_error_code`] so the wire reports each code verbatim.

use super::closed_error_codes;

closed_error_codes! {
    /// Every refusal a program optimization job or a champion change can report.
    pub enum ProgramErrorCode {
        /// A corpus without a validation or test example.
        HeldoutMissing => "PROGRAM_HELDOUT_MISSING",
        /// An optimizer name outside the closed set.
        OptimizerUnknown => "PROGRAM_OPTIMIZER_UNKNOWN",
        /// A budget field over the job ceiling.
        BudgetExceeded => "PROGRAM_BUDGET_EXCEEDED",
        /// An evaluation scored on fewer cases than the promotion policy needs.
        EvalTooSmall => "PROGRAM_EVAL_TOO_SMALL",
        /// A promotion or rollback that names a champion that is no longer active.
        ChampionStale => "PROGRAM_CHAMPION_STALE",
        /// A rollback of a revision with no parent.
        NoParent => "PROGRAM_NO_PARENT",
        /// An unsupported contract version.
        UnsupportedVersion => "PROGRAM_UNSUPPORTED_VERSION",
        /// A malformed program, signature, example, artifact, plan or promotion.
        Invalid => "PROGRAM_INVALID",
        /// A policy, privacy or modality mismatch.
        PolicyMismatch => "PROGRAM_POLICY_MISMATCH",
        /// A request over a resource limit.
        ResourceLimit => "PROGRAM_RESOURCE_LIMIT",
        /// No governed example satisfies the optimizer.
        NoEligibleExamples => "PROGRAM_NO_ELIGIBLE_EXAMPLES",
        /// The run was cancelled.
        Cancelled => "PROGRAM_CANCELLED",
    }
}
