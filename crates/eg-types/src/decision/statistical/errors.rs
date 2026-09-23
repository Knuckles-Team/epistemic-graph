//! The closed refusal vocabulary of the statistical decision surface.
//!
//! Kept beside the statistical types rather than folded into the assembly
//! surface's `DecisionErrorCode`: these refusals are about feature schemas,
//! heads, datasets and jobs, none of which the assembly layer has.

use crate::contract::closed_error_codes;

closed_error_codes! {
    /// Every typed refusal `Decide`, `DecisionFit` and `DecisionEval` answer with.
    pub enum StatisticalErrorCode {
        /// A pinned component is missing, of the wrong kind, or at another revision.
        ComponentPinMismatch => "COMPONENT_PIN_MISMATCH",
        /// A feature schema body is absent, malformed or of an unserved version.
        FeatureSchemaInvalid => "FEATURE_SCHEMA_INVALID",
        /// A head body is absent, malformed, or reads another feature schema.
        HeadInvalid => "HEAD_INVALID",
        /// The visible candidate set exceeds the decision bound.
        CandidateSetTooLarge => "CANDIDATE_SET_TOO_LARGE",
        /// A graph candidate plan ranks or truncates before visibility.
        CandidatePlanRefused => "CANDIDATE_PLAN_REFUSED",
        /// A typed parameter a feature names is absent or of the wrong type.
        ParameterInvalid => "PARAMETER_INVALID",
        /// A labelled dataset is malformed or does not match its pin.
        DatasetInvalid => "DATASET_INVALID",
        /// No admissible labelled item remains after the training filter.
        NoAdmissibleLabels => "NO_ADMISSIBLE_LABELS",
        /// The same idempotency key was reused for a different request.
        IdempotencyConflict => "IDEMPOTENCY_CONFLICT",
        /// No job with that id exists for this tenant.
        JobNotFound => "JOB_NOT_FOUND",
        /// An evaluation names a draft or head this tenant does not hold.
        EvalCandidateNotFound => "EVAL_CANDIDATE_NOT_FOUND",
        /// A decision-head publish names a receipt that is absent, failed, or
        /// qualified a different head.
        EvaluationReceiptMismatch => "EVALUATION_RECEIPT_MISMATCH",
        /// A numeric kernel refused its input.
        NumericRefused => "NUMERIC_REFUSED",
    }
}

impl StatisticalErrorCode {
    /// `"CODE: detail"`, the one refusal text form.
    pub fn refusal(self, detail: impl std::fmt::Display) -> String {
        format!("{}: {detail}", self.as_str())
    }
}
