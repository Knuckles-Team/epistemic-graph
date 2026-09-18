//! The Decide layer's wire contract (RF-ADR-010).
//!
//! Four methods share this vocabulary: `AgentAssemble` proves an agent graph
//! out of a tenant's own component library, `DecisionCommit` makes one of its
//! records durable, `Decide` answers a statistical question evaluate-only, and
//! `DecisionFit`/`DecisionEval` are the admin jobs that produce and qualify a
//! calibrated head.
//!
//! Three properties hold across all of them:
//!
//! * **A conclusion carries its premises.** Every record names the facts it
//!   used, the class of evidence each one is, and where it came from, so it is
//!   classified by its WEAKEST premise instead of by its own confidence.
//! * **Abstention is a first-class answer.** There is no arm that returns a
//!   guess shaped like a proof. An engine that cannot decide says what it could
//!   not resolve.
//! * **Nothing here is a float, and nothing reads a clock.** Times are recorded
//!   and never read by the math, so replaying a record anywhere reproduces its
//!   digest exactly.
//!
//! Every algorithm -- derivation, solving, verification, execution, fitting --
//! lives above this module. These are data types and their bounds.

pub mod digest;
pub mod errors;
pub mod jobs;
pub mod numeric;
pub mod policy;
pub mod record;
pub mod request;
pub mod statistical;

/// Format identity (RF-ADR-006) of an assembly decision record.
pub const DECISION_RECORD_SCHEMA_VERSION: u16 = 1;
/// Format identity of a statistical decision record. It starts at 2 because a
/// statistical record is the SECOND shape of the same durable record family,
/// not a second family.
pub const STATISTICAL_DECISION_RECORD_SCHEMA_VERSION: u16 = 2;
/// Format identity of a decision policy body.
pub const DECISION_POLICY_SCHEMA_VERSION: u16 = 1;
/// Format identity of a durable decision-job row.
pub const DECISION_JOB_SCHEMA_VERSION: u16 = 1;

/// Largest encoded decision record that may be committed.
pub const MAX_DECISION_RECORD_BYTES: usize = 64 * 1024;
/// Largest candidate set one assembly may consider.
pub const MAX_ASSEMBLY_CANDIDATES: usize = 64;
/// Largest required-capability set one assembly request may carry.
pub const MAX_ASSEMBLY_REQUIRED_CAPABILITIES: usize = 32;
/// Largest number of graph templates one assembly may enumerate.
pub const MAX_ASSEMBLY_TEMPLATES: usize = 8;
/// Largest number of slots one template may declare.
pub const MAX_TEMPLATE_SLOTS: usize = 6;

pub use errors::DecisionErrorCode;
pub use jobs::{
    DecisionEvalOp, DecisionEvalReceipt, DecisionEvalRequest, DecisionFitOp, DecisionFitRequest,
    DecisionJobKind, DecisionJobOutput, DecisionJobRecord, DecisionJobState,
    DecisionJobStatusRequest, EvalCandidate, HeadKind, LabelRegime, OpeEstimateView,
    OpeEstimatorKind, OptimiserSpec, RecordWindow,
};
pub use numeric::{
    QuantScaleTag, QuantisedValue, UnitRationalFields, UnitRationalWire,
    MAX_UNIT_RATIONAL_DENOMINATOR,
};
pub use policy::{
    ColdStart, DecisionPolicy, ExplorationBudget, ObjectiveLevelKind, ObjectiveOrder,
    StatisticalPolicy, TraceFidelityLevel, UnknownCostRule, WeightedLevel,
};
pub use record::{
    AbstainReason, CandidateFacts, CandidateSourceRecord, CoverageDerivation, DecisionInputs,
    DecisionOutcome, DecisionQuestion, DecisionRecord, DerivationClass, DerivationEdge, EdgeSource,
    Elimination, EvidenceClass, PremiseClass, PremiseProvenance, PremiseRef, ResolutionKind,
    SlotAssignment, SolverIdentity, TraceFidelity, Violation, WhyNot,
};
pub use request::{
    AssemblyConstraints, AssemblyRequest, AssemblyRequirements, AssemblyResult, ClaimProvenance,
    ClaimedTaskMapping, CostBudget, DecisionCommitRequest, DecisionCommitResult, DecisionPolicyRef,
    LibraryCandidateScope, SolverBudget, DECISION_COMPONENT_ID_PREFIX,
};
pub use statistical::{
    CalibrationMethod, CalibrationStatement, CandidateSource, DecideRequest, DecisionBatch,
    ExplorationRecord, FeatureMatrixRef, QuestionKind, QuestionSafety, RiskMethod, RiskStatement,
    ScoredOption, ShortlistProvenance, StatisticalDecisionRecord, StatisticalInputs,
    StatisticalOutcome, StatisticalQuestion, TypedParam, TypedValue,
};
