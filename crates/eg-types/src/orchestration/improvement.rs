//! The improvement library: adopted harnesses, behavioral contrasts and
//! lessons (EG-ORCH-VOCAB-R802).
//!
//! The engine stores the adopted harness, the behavioral contrast and the
//! lesson of one task. A lesson retains only when its evidence shows
//! measurable improvement: fewer retries, better tool selection, a higher
//! evaluation score or lower cost. The library queries by deficiency, task
//! class and trajectory features, ranks hits by confidence and abstains with
//! `NoLessons` when nothing matches. A record without evidence refuses.
//!
//! A contrast compares two runs of the same task under identical conditions;
//! no label or verifier participates — the evidence is the measured delta. A
//! lesson is a fact with applicability, not a rule: it cites its evidence
//! runs by trace id and keeps the confidence the runs earned it.

use serde::{Deserialize, Serialize};

use super::HarnessComponentKind;
use crate::contract::BoundedVec;

/// The engine-owned keys of an improvement-library record's property object:
/// its write marker, the transaction-time stamp (stamped by the engine on the
/// direct AddNode path, by the generated client on the ChangeEnvelope path),
/// the window-close transaction stamp the bitemporal kernel adds when the
/// record stops being current, and the owner-id stamp the gateway applies to
/// every native write. A record decoded from the object ignores exactly
/// these.
const ENGINE_OWNED_IMPROVEMENT_KEYS: &[&str] = &[
    "_harness_lesson",
    "_adopted_harness",
    "_harness_contrast",
    "_harness_probe_run",
    "tx_from",
    "tx_to",
    "_owner_id",
];

/// Whether `props` is a harness-lesson write (its engine-owned marker).
pub fn is_harness_lesson_write(props: &serde_json::Map<String, serde_json::Value>) -> bool {
    props
        .get("_harness_lesson")
        .and_then(|value| value.as_bool())
        == Some(true)
}

/// Whether `props` is an adopted-harness write (its engine-owned marker).
pub fn is_adopted_harness_write(props: &serde_json::Map<String, serde_json::Value>) -> bool {
    props
        .get("_adopted_harness")
        .and_then(|value| value.as_bool())
        == Some(true)
}

/// Whether `props` is a harness-contrast write (its engine-owned marker).
pub fn is_harness_contrast_write(props: &serde_json::Map<String, serde_json::Value>) -> bool {
    props
        .get("_harness_contrast")
        .and_then(|value| value.as_bool())
        == Some(true)
}

/// Whether `props` is a harness-probe-run write (its engine-owned marker).
pub fn is_harness_probe_run_write(props: &serde_json::Map<String, serde_json::Value>) -> bool {
    props
        .get("_harness_probe_run")
        .and_then(|value| value.as_bool())
        == Some(true)
}

/// Strictly decode an improvement-library record from its property object
/// (R802.5): the engine-owned keys are stripped, everything else is the
/// record's own shape — an unknown or mistyped field refuses
/// [`ImprovementLibraryRefusal::RecordMalformed`] instead of being dropped.
fn decode_improvement_record<T: for<'de> Deserialize<'de>>(
    props: &serde_json::Map<String, serde_json::Value>,
) -> Result<T, ImprovementLibraryRefusal> {
    let mut record = props.clone();
    for key in ENGINE_OWNED_IMPROVEMENT_KEYS {
        record.remove(*key);
    }
    serde_json::from_value(serde_json::Value::Object(record))
        .map_err(|_| ImprovementLibraryRefusal::RecordMalformed)
}

/// Decode a harness-lesson write (R802.1). The marker is required; a record
/// with the marker but a different shape is malformed, not a lesson.
pub fn decode_lesson_write(
    props: &serde_json::Map<String, serde_json::Value>,
) -> Result<HarnessLesson, ImprovementLibraryRefusal> {
    if !is_harness_lesson_write(props) {
        return Err(ImprovementLibraryRefusal::RecordMalformed);
    }
    decode_improvement_record(props)
}

/// Decode an adopted-harness write (R802.1).
pub fn decode_adopted_harness_write(
    props: &serde_json::Map<String, serde_json::Value>,
) -> Result<AdoptedHarness, ImprovementLibraryRefusal> {
    if !is_adopted_harness_write(props) {
        return Err(ImprovementLibraryRefusal::RecordMalformed);
    }
    decode_improvement_record(props)
}

/// Decode a harness-contrast write (R802.1).
pub fn decode_harness_contrast_write(
    props: &serde_json::Map<String, serde_json::Value>,
) -> Result<HarnessContrast, ImprovementLibraryRefusal> {
    if !is_harness_contrast_write(props) {
        return Err(ImprovementLibraryRefusal::RecordMalformed);
    }
    decode_improvement_record(props)
}

/// Decode a harness-probe-run write (R802.5).
pub fn decode_harness_probe_run_write(
    props: &serde_json::Map<String, serde_json::Value>,
) -> Result<HarnessProbeRun, ImprovementLibraryRefusal> {
    if !is_harness_probe_run_write(props) {
        return Err(ImprovementLibraryRefusal::RecordMalformed);
    }
    decode_improvement_record(props)
}

/// The typed refusal of an improvement-library write or read; each variant
/// carries its served code verbatim (R802.5 names the first, third and last).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ImprovementLibraryRefusal {
    /// A lesson without evidence runs, or one that does not cite its runs.
    LessonWithoutEvidence,
    /// A lesson whose contrast shows no measurable improvement.
    LessonWithoutImprovement,
    /// A lesson confidence outside 0 to 1 (or not a number at all).
    LessonConfidenceInvalid,
    /// A lesson, adoption or contrast whose identity names nothing.
    IdentityMissing,
    /// A record stored at a node id that is not its own content address
    /// (the contrast, lesson or adoption re-digests to a different id).
    ContentDigestMismatch,
    /// A contrast whose conditions digest differs from its runs.
    ContrastConditionsMismatch,
    /// An adopted harness without a decision reference.
    AdoptedHarnessNoDecision,
    /// A supersession that names an older record instead of the current one.
    SupersessionStale,
    /// A supersession that names a record the store does not hold.
    SupersessionUnknown,
    /// The library query matched nothing; the read abstains, it does not fail.
    NoLessons,
    /// A record that does not decode to the vocabulary's shape.
    RecordMalformed,
    /// A record written without its transaction-time stamp.
    WithoutStamp,
    /// A write that would mutate a published record.
    RecordImmutable,
}

impl ImprovementLibraryRefusal {
    /// The served error code, verbatim (EG-DURABLE-KERNEL-R043.8). Identity
    /// refusals share the vocabulary's one `HarnessIdentityMissing` code.
    pub fn code(self) -> &'static str {
        use super::HarnessVocabularyErrorCode as Code;
        match self {
            Self::LessonWithoutEvidence => Code::LessonWithoutEvidence,
            Self::LessonWithoutImprovement => Code::LessonWithoutImprovement,
            Self::LessonConfidenceInvalid => Code::LessonConfidenceInvalid,
            Self::IdentityMissing => Code::IdentityMissing,
            Self::ContentDigestMismatch => Code::ContentDigestMismatch,
            Self::ContrastConditionsMismatch => Code::ContrastConditionsMismatch,
            Self::AdoptedHarnessNoDecision => Code::AdoptedHarnessNoDecision,
            Self::SupersessionStale => Code::SupersessionStale,
            Self::SupersessionUnknown => Code::SupersessionUnknown,
            Self::NoLessons => Code::NoLessons,
            Self::RecordMalformed => Code::RecordMalformed,
            Self::WithoutStamp => Code::RecordWithoutStamp,
            Self::RecordImmutable => Code::RecordImmutable,
        }
        .as_str()
    }
}

/// One probe run of one task under the contrast's conditions: its trace id
/// and the digest of the conditions it ran under (R802.5).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct HarnessProbeRun {
    pub trace_id: String,
    pub conditions_digest: String,
}

impl HarnessProbeRun {
    /// The store identity of one probe run: its trace id, on the run's own
    /// prefix.
    pub fn node_id(&self) -> String {
        format!("harness-probe-run:{}", self.trace_id)
    }

    /// The admission check (R802.5): the trace id names the run.
    pub fn validate(&self) -> Result<(), ImprovementLibraryRefusal> {
        if self.trace_id.trim().is_empty() {
            return Err(ImprovementLibraryRefusal::IdentityMissing);
        }
        Ok(())
    }
}

/// The measured behavioral contrast of two probe runs of one task
/// (EG-ORCH-VOCAB-R802.1/EG-ORCH-VOCAB-R802.2). Every delta is B minus A on the Q32 fixed-point scale:
/// signed counts as `count << 32`, fractions as their Q32 numerator.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct HarnessContrast {
    /// The task both runs probed.
    pub task_ref: String,
    /// The digest of the identical probe conditions (task, seed, model,
    /// budget); both runs and the engine agree on it or the contrast refuses.
    pub conditions_digest: String,
    /// The baseline run, by trace id.
    pub run_a_trace_id: String,
    /// The candidate run, by trace id.
    pub run_b_trace_id: String,
    /// B minus A of the retry count; negative is the improvement (fewer).
    pub retry_delta: i64,
    /// B minus A of the tool-selection quality fraction; positive is the
    /// improvement (better selection).
    pub tool_selection_delta: i64,
    /// B minus A of the evaluation score; positive is the improvement
    /// (higher score).
    pub eval_score_delta: i64,
    /// B minus A of the run cost; negative is the improvement (lower cost).
    pub cost_delta: i64,
}

impl HarnessContrast {
    /// The store identity of one contrast: content-addressed over the
    /// identical conditions and the two runs it compares — a contrast is a
    /// single measurement, one per run pair, never re-published.
    pub fn node_id(&self) -> String {
        format!(
            "harness-contrast:{}:{}:{}",
            self.conditions_digest, self.run_a_trace_id, self.run_b_trace_id
        )
    }

    /// Measurable improvement (R802.2): fewer retries, better tool selection,
    /// a higher evaluation score or lower cost — any one of the four deltas
    /// moving in the improving direction.
    pub fn measurable_improvement(&self) -> bool {
        self.retry_delta < 0
            || self.tool_selection_delta > 0
            || self.eval_score_delta > 0
            || self.cost_delta < 0
    }

    /// The probe runs must agree with the contrast (R802.5): the cited trace
    /// ids, and the same conditions digest on all three sides. A mismatch
    /// means the two runs did not probe identical conditions, so no delta of
    /// the contrast is evidence at all.
    pub fn validate_against(
        &self,
        run_a: &HarnessProbeRun,
        run_b: &HarnessProbeRun,
    ) -> Result<(), ImprovementLibraryRefusal> {
        if self.task_ref.trim().is_empty()
            || self.run_a_trace_id.trim().is_empty()
            || self.run_b_trace_id.trim().is_empty()
        {
            return Err(ImprovementLibraryRefusal::IdentityMissing);
        }
        if self.run_a_trace_id != run_a.trace_id || self.run_b_trace_id != run_b.trace_id {
            return Err(ImprovementLibraryRefusal::ContrastConditionsMismatch);
        }
        if run_a.conditions_digest != run_b.conditions_digest
            || self.conditions_digest != run_a.conditions_digest
        {
            return Err(ImprovementLibraryRefusal::ContrastConditionsMismatch);
        }
        Ok(())
    }
}

/// The harness a task adopted: which component versions, under which decision
/// and contrast (R802.1).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct AdoptedHarness {
    /// The agent the harness belongs to.
    pub agent_ref: String,
    /// The task class the harness is adopted for.
    pub task_class: String,
    /// The adopted component version node ids, one per component slot.
    pub component_version_refs: BoundedVec<String, 8>,
    /// The decision record that adopted the harness; never empty (R802.5).
    pub decision_ref: String,
    /// The stored contrast the decision read.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub contrast_ref: Option<String>,
}

impl AdoptedHarness {
    /// The store identity of one adoption, content-addressed over its exact
    /// refs: the same adoption is the same node, a different one supersedes.
    pub fn node_id(&self) -> String {
        format!(
            "adopted-harness:{}:{}:{}",
            self.agent_ref,
            self.task_class,
            crate::decision::digest::digest_text(
                "eg/adopted-harness-refs/v1",
                &self.component_version_refs
            )
        )
    }

    /// The admission check (R802.5): the decision reference is the adoption's
    /// evidence, and the identity names its agent and task class.
    pub fn validate(&self) -> Result<(), ImprovementLibraryRefusal> {
        if self.agent_ref.trim().is_empty() || self.task_class.trim().is_empty() {
            return Err(ImprovementLibraryRefusal::IdentityMissing);
        }
        if self.decision_ref.trim().is_empty() {
            return Err(ImprovementLibraryRefusal::AdoptedHarnessNoDecision);
        }
        Ok(())
    }
}

/// The lesson one task learned from one contrast (R802.1): a fact with
/// applicability, cited to its evidence runs by trace id.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct HarnessLesson {
    /// The task class the lesson applies to.
    pub task_class: String,
    /// The deficiency the contrast measured.
    pub deficiency: String,
    /// The situation the lesson was learned in.
    pub situation: String,
    /// Which of the eight components changed.
    pub changed_component_kind: HarnessComponentKind,
    /// What changed, in the writer's words.
    pub what_changed: String,
    /// The trajectory features under which the lesson applies.
    #[serde(default)]
    pub applicability: BoundedVec<String, 64>,
    /// How much the writer trusts the lesson; 0 to 1.
    pub confidence: f64,
    /// The evidence runs, cited by trace id; never empty (R802.5).
    pub evidence_run_ids: BoundedVec<String, 32>,
    /// The lesson this one supersedes, by node id; the engine sets it when
    /// the writer leaves it absent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub supersedes_id: Option<String>,
}

impl HarnessLesson {
    /// The store identity of one lesson, content-addressed over its content.
    pub fn node_id(&self) -> String {
        format!(
            "harness-lesson:{}:{}:{}",
            self.task_class,
            self.deficiency,
            crate::decision::digest::digest_text(
                "eg/harness-lesson/v1",
                &(
                    self.situation.clone(),
                    self.changed_component_kind,
                    self.what_changed.clone(),
                    self.applicability.as_slice(),
                    self.confidence,
                    self.evidence_run_ids.as_slice(),
                )
            )
        )
    }

    /// Whether two lessons carry the same writer-supplied fields. The
    /// comparison is over the writer's fields only: `supersedes_id` is
    /// engine state (filled by the write path when the writer leaves it
    /// absent), so it never turns a replay into a "mutation" (R802.4).
    pub fn same_writer_fields(&self, other: &Self) -> bool {
        self.task_class == other.task_class
            && self.deficiency == other.deficiency
            && self.situation == other.situation
            && self.changed_component_kind == other.changed_component_kind
            && self.what_changed == other.what_changed
            && self.applicability == other.applicability
            && self.confidence == other.confidence
            && self.evidence_run_ids == other.evidence_run_ids
    }

    /// The admission check (R802.5): evidence runs are the lesson's
    /// existence, and the confidence stays inside 0 to 1.
    pub fn validate(&self) -> Result<(), ImprovementLibraryRefusal> {
        if self.task_class.trim().is_empty() || self.deficiency.trim().is_empty() {
            return Err(ImprovementLibraryRefusal::IdentityMissing);
        }
        if self.evidence_run_ids.is_empty() {
            return Err(ImprovementLibraryRefusal::LessonWithoutEvidence);
        }
        if !self.confidence.is_finite() || !(0.0..=1.0).contains(&self.confidence) {
            return Err(ImprovementLibraryRefusal::LessonConfidenceInvalid);
        }
        Ok(())
    }
}

/// One `:supersedes` edge between two lessons, stamped bitemporally (R802.4).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct LessonSupersession {
    pub new_version: String,
    pub superseded_version: String,
    /// Valid time: from when the newer lesson is current.
    pub valid_at: u64,
    /// Transaction time: when the write that made it was stamped.
    pub tx_now: u64,
}

/// One `:supersedes` edge between two adoptions of the same agent and task
/// class, stamped bitemporally (R802.4).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct AdoptionSupersession {
    pub new_version: String,
    pub superseded_version: String,
    /// Valid time: from when the newer adoption is current.
    pub valid_at: u64,
    /// Transaction time: when the write that made it was stamped.
    pub tx_now: u64,
}

/// The measured evidence behind one lesson (R802.5): its contrast and the
/// two probe runs it compares.
pub struct LessonEvidence<'a> {
    /// The measured contrast of the two runs.
    pub contrast: &'a HarnessContrast,
    /// The baseline run (run A).
    pub run_a: &'a HarnessProbeRun,
    /// The candidate run (run B).
    pub run_b: &'a HarnessProbeRun,
}

/// Admit one lesson into the library (R802.2/R802.4).
///
/// The engine retains the lesson only when its contrast shows measurable
/// improvement and the runs behind it agree with the contrast; the lesson
/// must cite both evidence runs by trace id. A newer lesson supersedes the
/// older one of the same task class and deficiency by edge, and the older
/// one stays readable. The byte-identical write is an idempotent no-op.
pub fn register_lesson(
    lessons: &mut Vec<HarnessLesson>,
    supersessions: &mut Vec<LessonSupersession>,
    mut new: HarnessLesson,
    evidence: &LessonEvidence<'_>,
    valid_at: u64,
    tx_now: u64,
) -> Result<(), ImprovementLibraryRefusal> {
    let contrast = evidence.contrast;
    let run_a = evidence.run_a;
    let run_b = evidence.run_b;
    new.validate()?;
    contrast.validate_against(run_a, run_b)?;
    if !contrast.measurable_improvement() {
        return Err(ImprovementLibraryRefusal::LessonWithoutImprovement);
    }
    let cited = |trace: &str| new.evidence_run_ids.iter().any(|id| id == trace);
    if !cited(&run_a.trace_id) || !cited(&run_b.trace_id) {
        return Err(ImprovementLibraryRefusal::LessonWithoutEvidence);
    }
    if replayed_or_immutable(lessons.iter().find(|l| l.node_id() == new.node_id()), &new)? {
        return Ok(());
    }
    let head = lessons
        .iter()
        .rev()
        .find(|l| l.task_class == new.task_class && l.deficiency == new.deficiency);
    match new.supersedes_id.as_deref() {
        Some(superseded) => match head {
            Some(h) if h.node_id() == superseded => {}
            Some(_) => return Err(ImprovementLibraryRefusal::SupersessionStale),
            None => {
                if !lessons
                    .iter()
                    .any(|l| l.node_id() == superseded && l.task_class == new.task_class)
                {
                    return Err(ImprovementLibraryRefusal::IdentityMissing);
                }
            }
        },
        None => new.supersedes_id = head.map(|h| h.node_id()),
    }
    if let Some(head) = &new.supersedes_id {
        supersessions.push(LessonSupersession {
            new_version: new.node_id(),
            superseded_version: head.clone(),
            valid_at,
            tx_now,
        });
    } else {
        // The first lesson of this task class and deficiency is current from
        // its own write time; the head-edge (an empty superseded version)
        // records that, so the bitemporal read abstains before the write.
        supersessions.push(LessonSupersession {
            new_version: new.node_id(),
            superseded_version: String::new(),
            valid_at,
            tx_now,
        });
    }
    lessons.push(new);
    Ok(())
}

/// Admit one adopted harness (R802.4). An adopted harness supersedes the
/// previous adoption of the same agent and task class by edge; the previous
/// adoption stays readable. The byte-identical write is an idempotent no-op.
pub fn register_adoption(
    adoptions: &mut Vec<AdoptedHarness>,
    supersessions: &mut Vec<AdoptionSupersession>,
    new: AdoptedHarness,
    valid_at: u64,
    tx_now: u64,
) -> Result<(), ImprovementLibraryRefusal> {
    new.validate()?;
    if replayed_or_immutable(
        adoptions.iter().find(|a| a.node_id() == new.node_id()),
        &new,
    )? {
        return Ok(());
    }
    let head = adoptions
        .iter()
        .rev()
        .find(|a| a.agent_ref == new.agent_ref && a.task_class == new.task_class);
    if let Some(head) = head {
        supersessions.push(AdoptionSupersession {
            new_version: new.node_id(),
            superseded_version: head.node_id(),
            valid_at,
            tx_now,
        });
    } else {
        // The first adoption of this agent and task class is current from its
        // own write time; the head-edge (an empty superseded version) records
        // that, so the bitemporal read returns nothing before the write.
        supersessions.push(AdoptionSupersession {
            new_version: new.node_id(),
            superseded_version: String::new(),
            valid_at,
            tx_now,
        });
    }
    adoptions.push(new);
    Ok(())
}

/// Whether `record` is current at `as_of`: not superseded at or before it,
/// and (when it supersedes an older one) stamped at or before it.
/// Owned `(new, superseded, valid_at)` supersession edges.
fn supersession_edges<'e>(
    edges: impl Iterator<Item = (&'e String, &'e String, u64)>,
) -> Vec<(String, String, u64)> {
    edges
        .map(|(new, superseded, valid_at)| (new.clone(), superseded.clone(), valid_at))
        .collect()
}

/// A published record never mutates: the same record replays as an
/// idempotent no-op (`Ok(true)`); anything else with the same identity
/// refuses.
fn replayed_or_immutable<T: PartialEq>(
    existing: Option<&T>,
    new: &T,
) -> Result<bool, ImprovementLibraryRefusal> {
    match existing {
        Some(existing) if existing == new => Ok(true),
        Some(_) => Err(ImprovementLibraryRefusal::RecordImmutable),
        None => Ok(false),
    }
}

fn edge_current_at(record_id: &str, supersessions: &[(String, String, u64)], as_of: u64) -> bool {
    !supersessions
        .iter()
        .any(|(_, superseded, valid_at)| superseded == record_id && *valid_at <= as_of)
        && supersessions
            .iter()
            .filter(|(new, _, _)| new == record_id)
            .all(|(_, _, valid_at)| *valid_at <= as_of)
}

/// Query the library by task class, deficiency and trajectory features
/// (R802.3). Hits are ranked by confidence, highest first (ties by node id,
/// so the order is deterministic), and read bitemporally with `AS OF`: a
/// lesson is current at `as_of` only while its supersession, when one
/// exists, has not closed it. The query abstains with `NoLessons` when
/// nothing matches.
pub fn retrieve_lessons<'a>(
    lessons: &'a [HarnessLesson],
    supersessions: &[LessonSupersession],
    task_class: &str,
    deficiency: &str,
    trajectory_features: &[String],
    as_of: u64,
) -> Result<Vec<&'a HarnessLesson>, ImprovementLibraryRefusal> {
    let edges = supersession_edges(
        supersessions
            .iter()
            .map(|e| (&e.new_version, &e.superseded_version, e.valid_at)),
    );
    let hits: Vec<&HarnessLesson> = lessons
        .iter()
        .filter(|l| {
            l.task_class == task_class
                && l.deficiency == deficiency
                && trajectory_features
                    .iter()
                    .all(|f| l.applicability.iter().any(|a| a == f))
                && edge_current_at(&l.node_id(), &edges, as_of)
        })
        .collect();
    if hits.is_empty() {
        return Err(ImprovementLibraryRefusal::NoLessons);
    }
    let mut ranked = hits;
    ranked.sort_by(|a, b| {
        b.confidence
            .total_cmp(&a.confidence)
            .then_with(|| a.node_id().cmp(&b.node_id()))
    });
    Ok(ranked)
}

/// The current adoption of one agent and task class at `as_of` (R802.4), or
/// `None` when no adoption is current then. Every older adoption stays
/// readable in `adoptions`.
pub fn current_adopted_harness<'a>(
    adoptions: &'a [AdoptedHarness],
    supersessions: &[AdoptionSupersession],
    agent_ref: &str,
    task_class: &str,
    as_of: u64,
) -> Option<&'a AdoptedHarness> {
    let edges = supersession_edges(
        supersessions
            .iter()
            .map(|e| (&e.new_version, &e.superseded_version, e.valid_at)),
    );
    adoptions.iter().rev().find(|a| {
        a.agent_ref == agent_ref
            && a.task_class == task_class
            && edge_current_at(&a.node_id(), &edges, as_of)
    })
}
