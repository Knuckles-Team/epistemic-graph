//! `Method::DecisionLog`: the engine's own statistical decision log (EH-060,
//! EH-061, EH-012).
//!
//! `Decide` is evaluate-only. A caller that ACTS on a statistical record makes
//! it durable here: the commit re-derives the outcome from the record's stored
//! inputs (verify-replay) and reveals the exploration seed the record committed
//! to. Independent outcome evaluations then join the record by id, and the
//! outcome aggregate -- per option and question, and across questions -- is
//! read from the joined log with k-anonymous support gating. Fitting and
//! evaluation can read their labels from this log instead of a submitted
//! dataset.
//!
//! Visibility: a library-sourced record is visible tenant-wide (its inputs
//! are); a graph-sourced record is visible only to the principal that
//! committed it, the conservative end of "caller scope ∩ referenced-row
//! visibility" (§4.3), and every read filters by it.

use serde::{Deserialize, Serialize};

use super::super::jobs::RecordWindow;
use super::super::numeric::QuantisedValue;
use super::super::record::EvidenceClass;
use super::dataset::OutcomeFidelity;
use super::StatisticalDecisionRecord;
use crate::contract::BoundedVec;
use crate::decision::request::DecisionPolicyRef;

/// Format identity of a decision-log body.
pub const DECISION_LOG_SCHEMA_VERSION: u16 = 1;
/// Most joined rows one aggregate answers.
pub const MAX_AGGREGATE_ROWS: usize = 1_024;

/// Who may read a committed record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "visibility", rename_all = "snake_case", deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum RecordVisibility {
    /// Library-sourced: every principal of the tenant.
    Tenant,
    /// Graph-sourced: only the committing principal.
    Principal { principal: String },
}

/// One independent evaluation of a committed record's executed option.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct DecisionOutcomeEvaluation {
    pub record_id: String,
    pub evaluation_id: String,
    pub class: EvidenceClass,
    /// The agent the executed option ran as; the evaluator must not be it.
    pub selected_agent: String,
    pub lease_holder: String,
    pub fidelity: OutcomeFidelity,
    /// `None`: censored, neither success nor failure.
    #[serde(default)]
    pub success: Option<bool>,
}

/// Where a compacted record's feature matrix lives: one engine-owned Blob
/// CAS body, pinned by its content digest.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct InputsBlob {
    /// `sha256:<hex>` of the canonical JSON of the original inline matrix.
    pub sha256: String,
    pub manifest_digest: String,
    pub holder_id: String,
    pub length: u64,
}

/// Where a logged record's bulky inputs are (EH-060). The record digest is
/// never recomputed: a compacted record's matrix is replaced by a blob pin,
/// and verification restores it from CAS before checking the digest.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(tag = "inputs", rename_all = "snake_case", deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum EntryInputs {
    /// The record carries its inputs inline.
    #[default]
    Inline,
    /// The matrix moved to Blob CAS; the entry pins it.
    Compacted {
        blob: InputsBlob,
        compacted_at_ms: u64,
    },
    /// The blob was released under the policy's `drop_blob_after`; the record
    /// digest stays attested, its inputs are gone.
    Retired {
        blob: InputsBlob,
        compacted_at_ms: u64,
        retired_at_ms: u64,
    },
}

/// A committed record with its log metadata.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct DecisionLogEntry {
    pub schema_version: u16,
    pub record: Box<StatisticalDecisionRecord>,
    pub committed_by: String,
    pub committed_at_ms: u64,
    pub visibility: RecordVisibility,
    #[serde(default)]
    pub inputs: EntryInputs,
}

/// Most entries one compaction call touches.
pub const MAX_COMPACT_PER_CALL: u32 = 256;

/// What one compaction call did.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct DecisionLogCompacted {
    pub compacted: u32,
    pub retired: u32,
    /// True when the per-call bound stopped the pass; call again.
    pub more: bool,
}

/// The outcome of verifying one logged record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "verification", rename_all = "snake_case", deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum DecisionLogVerification {
    /// The inputs (restored from CAS when compacted) reproduce the record
    /// digest and the decision replays.
    Verified { record_digest: String },
    /// `INPUTS_RETIRED`: the inputs were dropped under retention. The digest is
    /// still the attested one; nothing can be re-derived.
    InputsRetired {
        record_digest: String,
        blob_sha256: String,
    },
}

/// A stored evaluation and who recorded it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct StoredEvaluation {
    pub evaluation: DecisionOutcomeEvaluation,
    /// The verified principal that recorded it: the evaluator.
    pub producer: String,
    pub recorded_at_ms: u64,
}

/// Who resolved an abstention (EH-037, §6.4 "abstentions as labels").
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "resolver", rename_all = "snake_case", deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum AbstentionResolver {
    /// The verified caller, a human, resolved it: a gold-label candidate
    /// (observation class).
    Human,
    /// A model proposed the option: a CLAIM with unknown propensity, never a
    /// calibration or off-policy label; at most a candidate for human review.
    Model {
        producer: String,
        #[serde(default)]
        prompt_digest: Option<String>,
    },
}

/// One resolution of a logged abstention: the option the escalation chose.
/// The option must be one the abstained record already held, so a
/// resolution can never introduce an option the decision did not have.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct AbstentionResolution {
    pub record_id: String,
    pub resolution_id: String,
    pub option_id: String,
    pub resolver: AbstentionResolver,
}

/// A stored resolution, its evidence class and who recorded it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct StoredResolution {
    pub resolution: AbstentionResolution,
    /// `Observation` for a human resolver, `Claim` for a model.
    pub class: EvidenceClass,
    /// The verified principal that recorded it.
    pub producer: String,
    pub recorded_at_ms: u64,
}

/// Ask for the outcome aggregate.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct OutcomeAggregateRequest {
    pub tenant_id: String,
    /// `None` aggregates every question; rows then also carry the
    /// cross-question total per option.
    #[serde(default)]
    pub question_id: Option<String>,
    pub window: RecordWindow,
}

/// Outcome counts per trace fidelity.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct FidelityCounts {
    pub full_step: u64,
    pub tool_calls: u64,
    pub final_output: u64,
    pub censored: u64,
}

/// One aggregate row: an option (or a whole assembly slate) under one
/// question and policy, or across every question when `question_id` is `None`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct OptionAggregate {
    pub option_id: String,
    #[serde(default)]
    pub question_id: Option<String>,
    #[serde(default)]
    pub policy_digest: Option<String>,
    /// Independently evaluated, uncensored, at or above the fidelity floor.
    pub trials: u64,
    pub successes: u64,
    /// Evaluations refused as labels (self-reported, claims, below floor).
    pub refused: u64,
    pub by_fidelity: FidelityCounts,
    /// Pooled (Beta-Binomial, question -> option) success rate; present only
    /// at or above `min_support`.
    #[serde(default)]
    pub pooled_rate: Option<QuantisedValue>,
}

/// The joined outcome aggregate. Rows below `min_support` report no rate;
/// counts are k-anonymised by the same floor (reported as zero below it).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct OutcomeAggregate {
    pub schema_version: u16,
    pub min_support: u64,
    pub rows: BoundedVec<OptionAggregate, 1024>,
}

/// The durable identity of a committed record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct DecisionLogCommitted {
    pub schema_version: u16,
    pub record_id: String,
    /// The committed record's digest, after the seed reveal.
    pub record_digest: String,
    pub replayed: bool,
}

/// One decision-log operation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum DecisionLogOp {
    /// Make one statistical record durable, after verify-replay.
    Commit {
        record: Box<StatisticalDecisionRecord>,
    },
    /// Join one independent evaluation to a committed record.
    Evaluate {
        tenant_id: String,
        evaluation: DecisionOutcomeEvaluation,
    },
    /// Read one committed record.
    Get {
        tenant_id: String,
        record_id: String,
    },
    /// Read the outcome aggregate.
    Aggregate { request: OutcomeAggregateRequest },
    /// Apply the policy's retention to at most `limit` entries: compact the
    /// ones older than `compact_after_ms`, retire blobs older than
    /// `drop_blob_after_ms`. Idempotent.
    Compact {
        tenant_id: String,
        policy: DecisionPolicyRef,
        limit: u32,
    },
    /// Re-verify one logged record against its (possibly compacted) inputs.
    Verify {
        tenant_id: String,
        record_id: String,
    },
    /// Record how an escalation resolved one logged abstention (EH-037).
    Resolve {
        tenant_id: String,
        resolution: AbstentionResolution,
    },
}

impl DecisionLogOp {
    /// Whether this operation commits durable state; the one classifier.
    pub fn is_mutation(&self) -> bool {
        matches!(
            self,
            Self::Commit { .. }
                | Self::Evaluate { .. }
                | Self::Compact { .. }
                | Self::Resolve { .. }
        )
    }

    /// The authorization action this operation needs.
    pub fn authz_action(&self) -> &'static str {
        match self {
            Self::Commit { .. } => "agent:decision-write",
            Self::Evaluate { .. } | Self::Resolve { .. } => "agent:decision-evaluate",
            Self::Compact { .. } => "admin:decision-log",
            Self::Get { .. } | Self::Aggregate { .. } | Self::Verify { .. } => {
                "agent:decision-read"
            }
        }
    }

    /// The tenant this operation names.
    pub fn tenant_id(&self) -> &str {
        match self {
            Self::Commit { record } => &record.tenant_id,
            Self::Evaluate { tenant_id, .. }
            | Self::Get { tenant_id, .. }
            | Self::Compact { tenant_id, .. }
            | Self::Verify { tenant_id, .. }
            | Self::Resolve { tenant_id, .. } => tenant_id,
            Self::Aggregate { request } => &request.tenant_id,
        }
    }
}
