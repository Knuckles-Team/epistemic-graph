//! The served refusal codes of the harness vocabulary (MEDULLA-ORCH-VOCAB-R801
//! versioned components, R802 improvement library).
//!
//! The closed set is registered in
//! [`crate::contract::declared_error_code`] so the wire reports each code
//! verbatim instead of classifying the refusal as `INTERNAL`
//! (MEDULLA-DURABLE-KERNEL-R043.8). The engine-internal typed refusals
//! ([`super::HarnessComponentRefusal`], [`super::improvement::
//! ImprovementLibraryRefusal`]) map onto this set; the first four R801 codes
//! and the three R802 codes are the ones the spec rows name, the rest are the
//! vocabulary's own additional refusals.

use crate::contract::closed_error_codes;

closed_error_codes! {
    /// Every refusal a harness-component write, an improvement-library write
    /// or read, or a supersession can report.
    pub enum HarnessVocabularyErrorCode {
        /// A component version without an `evidence_run_id`.
        VersionWithoutEvidence => "HarnessVersionWithoutEvidence",
        /// A `kind` outside the closed set of eight components.
        KindUnknown => "HarnessKindUnknown",
        /// A confidence outside 0 to 1 (or not a number at all).
        ConfidenceInvalid => "HarnessConfidenceInvalid",
        /// A write that would mutate a published version.
        VersionImmutable => "HarnessVersionImmutable",
        /// A harness record whose identity names nothing.
        IdentityMissing => "HarnessIdentityMissing",
        /// A content digest that does not recompute from the content reference.
        ContentDigestMismatch => "HarnessContentDigestMismatch",
        /// A valid interval that runs backwards or is zero.
        ValidIntervalInverted => "HarnessValidIntervalInverted",
        /// A supersession that names a version the store does not hold.
        SupersessionUnknown => "HarnessSupersessionUnknown",
        /// A supersession that names an older version instead of the current one.
        SupersessionStale => "HarnessSupersessionStale",
        /// A version written without its transaction-time stamp.
        WithoutStamp => "HarnessVersionWithoutStamp",
        /// A version record that does not decode to the vocabulary's shape.
        RecordMalformed => "HarnessRecordMalformed",
        /// A lesson without evidence runs, or one that does not cite its runs.
        LessonWithoutEvidence => "LessonWithoutEvidence",
        /// A lesson whose contrast shows no measurable improvement.
        LessonWithoutImprovement => "LessonWithoutImprovement",
        /// A lesson confidence outside 0 to 1 (or not a number at all).
        LessonConfidenceInvalid => "LessonConfidenceInvalid",
        /// A contrast whose conditions digest differs from its runs.
        ContrastConditionsMismatch => "ContrastConditionsMismatch",
        /// An adopted harness without a decision reference.
        AdoptedHarnessNoDecision => "AdoptedHarnessNoDecision",
        /// The library query matched nothing; the read abstains, it does not fail.
        NoLessons => "NoLessons",
        /// A lesson, adoption, contrast or probe run written without its
        /// transaction-time stamp.
        RecordWithoutStamp => "HarnessRecordWithoutStamp",
        /// A write that would mutate a published lesson, adoption, contrast
        /// or probe run.
        RecordImmutable => "HarnessRecordImmutable",
    }
}
