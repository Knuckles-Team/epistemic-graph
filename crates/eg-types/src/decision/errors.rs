//! The closed refusal vocabulary of the decision surface.
//!
//! Each code is the `"CODE: detail"` prefix of an error response, so a caller
//! branches on the code and reads the detail, rather than matching prose.

use crate::contract::closed_error_codes;

closed_error_codes! {
    /// Every typed refusal the assembly, commit and statistical decision
    /// surfaces can answer with.
    pub enum DecisionErrorCode {
        /// The catalog moved between the assembly read and the commit.
        StaleCatalog => "STALE_CATALOG",
        /// Re-deriving the record on commit produced a different digest.
        DecisionReplayMismatch => "DECISION_REPLAY_MISMATCH",
        /// The certificate in the record does not verify against its model.
        CertificateRejected => "CERTIFICATE_REJECTED",
        /// A pinned candidate's definition digest no longer matches.
        CandidateFactsChanged => "CANDIDATE_FACTS_CHANGED",
        /// The record declares a schema version this engine does not serve.
        DecisionRecordVersionUnsupported => "DECISION_RECORD_VERSION_UNSUPPORTED",
        /// The encoded record is larger than the durable record bound.
        RecordTooLarge => "RECORD_TOO_LARGE",
        /// A request tried to widen, not tighten, the pinned policy.
        PolicyLoosening => "POLICY_LOOSENING",
        /// Exploration was requested for a question that forbids it.
        ExplorationForbidden => "EXPLORATION_FORBIDDEN",
        /// Publishing a decision head needs its evaluation receipt digest.
        EvaluationReceiptRequired => "EVALUATION_RECEIPT_REQUIRED",
    }
}
