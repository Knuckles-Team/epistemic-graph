//! Versioned harness components as graph facts (EG-ORCH-VOCAB-R801), and the
//! improvement library that stores what a task learned from them (R802, in
//! [`improvement`]).
//!
//! A harness component is one of the eight editable pieces of an agent's
//! harness: system_prompt, guardrails, memory, tool_guidance, subagent,
//! skill, script or plugin. Each version carries a content digest, a valid
//! time, a confidence and an evidence link. A new version supersedes the
//! older one by edge; a published version never mutates.
//!
//! This module is the typed vocabulary and the pure rules over it. The
//! durable write (the ChangeEnvelope/AddNode path, R801.3) stamps the
//! versions bitemporally and links the `:supersedes` edges; the served reads
//! (R078/R081) walk them. The records themselves are plain data: one
//! vocabulary, one writer.

pub mod errors;
pub mod improvement;
pub mod workflows;

use serde::{Deserialize, Serialize};

pub use errors::HarnessVocabularyErrorCode;

/// The closed set of editable harness components (R801).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum HarnessComponentKind {
    /// The agent's standing instructions.
    SystemPrompt,
    /// The guardrails a run must respect (EG-ORCH-VOCAB-R004.700 scope).
    Guardrails,
    /// The memory a run reads and writes.
    Memory,
    /// The guidance over which tools to reach for.
    ToolGuidance,
    /// The subagent delegation surface.
    Subagent,
    /// A skill procedure the harness can invoke.
    Skill,
    /// A script the harness can run.
    Script,
    /// A plugin extending the harness.
    Plugin,
}

impl HarnessComponentKind {
    /// The one wire name of each kind.
    pub fn wire_name(self) -> &'static str {
        match self {
            Self::SystemPrompt => "system_prompt",
            Self::Guardrails => "guardrails",
            Self::Memory => "memory",
            Self::ToolGuidance => "tool_guidance",
            Self::Subagent => "subagent",
            Self::Skill => "skill",
            Self::Script => "script",
            Self::Plugin => "plugin",
        }
    }

    /// Parse one of the eight wire names. Anything else is an unknown kind:
    /// the closed set is the model itself, so the write path refuses it with
    /// [`HarnessComponentRefusal::KindUnknown`] (R801.4) instead of guessing.
    pub fn from_wire(name: &str) -> Result<Self, HarnessComponentRefusal> {
        Ok(match name {
            "system_prompt" => Self::SystemPrompt,
            "guardrails" => Self::Guardrails,
            "memory" => Self::Memory,
            "tool_guidance" => Self::ToolGuidance,
            "subagent" => Self::Subagent,
            "skill" => Self::Skill,
            "script" => Self::Script,
            "plugin" => Self::Plugin,
            _ => return Err(HarnessComponentRefusal::KindUnknown),
        })
    }
}

/// The typed refusal of a harness-component write; each variant carries its
/// served code verbatim (R801.4 names the first four).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HarnessComponentRefusal {
    /// A version without an `evidence_run_id`.
    VersionWithoutEvidence,
    /// A kind outside the closed set of eight.
    KindUnknown,
    /// A confidence outside 0 to 1 (or not a number at all).
    ConfidenceInvalid,
    /// A write that would mutate a published version.
    VersionImmutable,
    /// A component id or content reference that names nothing.
    IdentityMissing,
    /// A content digest that does not recompute from the content reference.
    ContentDigestMismatch,
    /// A valid interval that runs backwards or is zero.
    ValidIntervalInverted,
    /// A supersession that names a version the store does not hold.
    SupersessionUnknown,
    /// A supersession that names an older version instead of the current one.
    SupersessionStale,
    /// A version written without its transaction-time stamp.
    WithoutStamp,
    /// A version record that does not decode to the vocabulary's shape.
    RecordMalformed,
}

impl HarnessComponentRefusal {
    /// The served error code, verbatim (EG-DURABLE-KERNEL-R043.8).
    pub fn code(self) -> &'static str {
        match self {
            Self::VersionWithoutEvidence => HarnessVocabularyErrorCode::VersionWithoutEvidence,
            Self::KindUnknown => HarnessVocabularyErrorCode::KindUnknown,
            Self::ConfidenceInvalid => HarnessVocabularyErrorCode::ConfidenceInvalid,
            Self::VersionImmutable => HarnessVocabularyErrorCode::VersionImmutable,
            Self::IdentityMissing => HarnessVocabularyErrorCode::IdentityMissing,
            Self::ContentDigestMismatch => HarnessVocabularyErrorCode::ContentDigestMismatch,
            Self::ValidIntervalInverted => HarnessVocabularyErrorCode::ValidIntervalInverted,
            Self::SupersessionUnknown => HarnessVocabularyErrorCode::SupersessionUnknown,
            Self::SupersessionStale => HarnessVocabularyErrorCode::SupersessionStale,
            Self::WithoutStamp => HarnessVocabularyErrorCode::WithoutStamp,
            Self::RecordMalformed => HarnessVocabularyErrorCode::RecordMalformed,
        }
        .as_str()
    }
}

/// One published version of one harness component (R801.1).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct HarnessComponentVersion {
    /// The component this is a version of; the same across its versions.
    pub component_id: String,
    /// Which of the eight components; the closed set.
    pub kind: HarnessComponentKind,
    /// Where the content lives; the digest recomputes from it.
    pub content_ref: String,
    /// `sha256:` of the content reference under the vocabulary's digest
    /// domain; a published version's identity.
    pub content_digest: String,
    /// Valid time: from when this version is current (bitemporal, R078/R081).
    pub valid_from: u64,
    /// Valid time: until when; absent while the version is still current.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub valid_to: Option<u64>,
    /// How much the writer trusts this version; 0 to 1.
    pub confidence: f64,
    /// The evaluation run that produced it; never empty (R801.4).
    pub evidence_run_id: String,
    /// The version this one supersedes, by node id; the engine sets it when
    /// the writer leaves it absent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub supersedes_id: Option<String>,
    /// The agent or task scope the version is applied to.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub applied_to_id: Option<String>,
}

impl HarnessComponentVersion {
    /// The one digest scheme the vocabulary uses for content: the engine's
    /// canonical text digest over the content reference.
    pub fn content_digest(content_ref: &str) -> String {
        crate::decision::digest::digest_text(
            "eg/harness-component-content/v1",
            &content_ref.to_string(),
        )
    }

    /// The store identity of one version. It is content-addressed on purpose:
    /// a rewrite of the same content is the same node (an idempotent replay),
    /// and any change of content is a different version, so a published
    /// version's node can never carry different content (R801.4).
    pub fn node_id(&self) -> String {
        format!(
            "harness-component:{}:{}",
            self.component_id, self.content_digest
        )
    }

    /// The admission check over the fields the writer can still get wrong:
    /// identity, evidence, confidence, digest integrity and the valid
    /// interval (R801.4). The kind is the closed set itself; an unknown kind
    /// refuses at decode, and the write path reports it as
    /// [`HarnessComponentRefusal::KindUnknown`].
    pub fn validate(&self) -> Result<(), HarnessComponentRefusal> {
        if self.component_id.trim().is_empty() || self.content_ref.trim().is_empty() {
            return Err(HarnessComponentRefusal::IdentityMissing);
        }
        if self.evidence_run_id.trim().is_empty() {
            return Err(HarnessComponentRefusal::VersionWithoutEvidence);
        }
        if !self.confidence.is_finite() || !(0.0..=1.0).contains(&self.confidence) {
            return Err(HarnessComponentRefusal::ConfidenceInvalid);
        }
        if self.content_digest != Self::content_digest(&self.content_ref) {
            return Err(HarnessComponentRefusal::ContentDigestMismatch);
        }
        if let Some(valid_to) = self.valid_to {
            if valid_to <= self.valid_from {
                return Err(HarnessComponentRefusal::ValidIntervalInverted);
            }
        }
        Ok(())
    }

    /// Whether two records of the same content carry the same writer-supplied
    /// metadata. The comparison is over the writer's fields only:
    /// `supersedes_id` and `valid_to` are engine state (filled and closed by
    /// the write path), so they never turn a replay into a "mutation"
    /// (R801.4).
    pub fn same_writer_fields(&self, other: &Self) -> bool {
        self.component_id == other.component_id
            && self.kind == other.kind
            && self.content_ref == other.content_ref
            && self.content_digest == other.content_digest
            && self.valid_from == other.valid_from
            && self.confidence == other.confidence
            && self.evidence_run_id == other.evidence_run_id
            && self.applied_to_id == other.applied_to_id
    }
}

/// Whether `props` is a harness-component version write: the engine-owned
/// marker on the node's property object (R801.3).
pub fn is_harness_component_write(props: &serde_json::Map<String, serde_json::Value>) -> bool {
    props
        .get("_harness_component")
        .and_then(|value| value.as_bool())
        == Some(true)
}

/// The engine-owned keys of a harness-component node's property object: the
/// write marker, the transaction-time stamp (the engine stamps it on the
/// direct AddNode path; the generated client stamps it on the
/// ChangeEnvelope path), the window-close transaction stamp the
/// bitemporal kernel adds when the version stops being current, and the
/// owner-id stamp the gateway applies to every native write. A record
/// decoded from the object ignores exactly these.
const ENGINE_OWNED_HARNESS_KEYS: &[&str] = &["_harness_component", "tx_from", "tx_to", "_owner_id"];

/// Decode a harness-component version write from its stored property object
/// (R801.1/R801.3). The shape is strict: an unknown `kind` refuses
/// [`HarnessComponentRefusal::KindUnknown`] before anything else is looked
/// at, and any other decode failure (a wrong field type, a missing field, an
/// unknown field) refuses [`HarnessComponentRefusal::RecordMalformed`]
/// instead of silently dropping the field. The engine-owned keys are
/// ignored; the caller runs [`HarnessComponentVersion::validate`] for the
/// typed evidence/refusals.
pub fn decode_component_write(
    props: &serde_json::Map<String, serde_json::Value>,
) -> Result<HarnessComponentVersion, HarnessComponentRefusal> {
    if !is_harness_component_write(props) {
        return Err(HarnessComponentRefusal::RecordMalformed);
    }
    match props.get("kind").and_then(|value| value.as_str()) {
        Some(name) => HarnessComponentKind::from_wire(name)?,
        None => return Err(HarnessComponentRefusal::KindUnknown),
    };
    let mut record = props.clone();
    for key in ENGINE_OWNED_HARNESS_KEYS {
        record.remove(*key);
    }
    serde_json::from_value(serde_json::Value::Object(record))
        .map_err(|_| HarnessComponentRefusal::RecordMalformed)
}

/// The transaction-time stamp a harness-component node must carry
/// (R801.3): `tx_from` set, and a positive number.
pub fn component_tx_stamp(props: &serde_json::Map<String, serde_json::Value>) -> Option<u64> {
    props
        .get("tx_from")
        .and_then(|value| value.as_u64())
        .filter(|stamp| *stamp > 0)
}

/// One `:supersedes` edge between two versions, stamped bitemporally (R078/
/// R081): the edge is the validity window by which the older version stops
/// being current, and it is what an `AS OF` read walks.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct HarnessSupersession {
    /// The newer version, by node id.
    pub new_version: String,
    /// The version it supersedes, by node id.
    pub superseded_version: String,
    /// Valid time: from when the newer version is current.
    pub valid_at: u64,
    /// Transaction time: when the write that made it was stamped.
    pub tx_now: u64,
}

/// Resolve the `supersedes_id` of one new version before it is admitted
/// (R801.2): a writer-named pointer must name the component's current
/// version, or an existing version of the component when no current window
/// is open; an absent pointer is filled with the current version's node id —
/// never the version's own id, since a version never supersedes itself.
fn resolve_supersedes_pointer(
    new: &mut HarnessComponentVersion,
    component: Option<&HarnessComponentVersion>,
    versions: &[HarnessComponentVersion],
) -> Result<(), HarnessComponentRefusal> {
    match new.supersedes_id.as_deref() {
        Some(superseded) => match component {
            Some(head) if head.node_id() == superseded => {}
            Some(_) => return Err(HarnessComponentRefusal::SupersessionStale),
            None => {
                if !versions
                    .iter()
                    .any(|v| v.node_id() == superseded && v.component_id == new.component_id)
                {
                    return Err(HarnessComponentRefusal::SupersessionUnknown);
                }
            }
        },
        None => {
            // A version never supersedes itself: when the writer is naming
            // the only existing version of the component, there is nothing
            // to supersede.
            new.supersedes_id = component
                .filter(|head| head.node_id() != new.node_id())
                .map(|head| head.node_id());
        }
    }
    Ok(())
}

/// Close the superseded version's validity window and record the
/// `:supersedes` edge (R801.2): the window closes at the newer version's
/// `valid_from`, and the edge is stamped with the write's transaction time
/// (R078/R081). A version never closes its own window.
fn record_supersession(
    versions: &mut [HarnessComponentVersion],
    supersessions: &mut Vec<HarnessSupersession>,
    new: &HarnessComponentVersion,
    tx_now: u64,
) {
    if let Some(head) = &new.supersedes_id {
        if head != &new.node_id() {
            if versions
                .iter()
                .find(|v| v.node_id() == *head)
                .and_then(|v| v.valid_to)
                .is_none()
            {
                for v in versions.iter_mut().filter(|v| v.node_id() == *head) {
                    if v.valid_to.is_none() {
                        v.valid_to = Some(new.valid_from);
                    }
                }
            }
            supersessions.push(HarnessSupersession {
                new_version: new.node_id(),
                superseded_version: head.clone(),
                valid_at: new.valid_from,
                tx_now,
            });
        }
    }
}

/// Admit one new version into the store's vocabulary (R801.2).
///
/// A new version supersedes the current one by edge: the older version's
/// validity window closes at the newer one's `valid_from`, and the newer one
/// names it in `supersedes_id` (the engine fills the pointer when the writer
/// leaves it absent). The older version stays readable in `versions` —
/// nothing about it is deleted, only its window closes.
///
/// A published version never mutates: the same `(component_id,
/// content_digest)` with any other writer-supplied metadata is refused
/// [`HarnessComponentRefusal::VersionImmutable`]; a replay of the same write
/// is admitted as an idempotent no-op. The comparison is over the writer's
/// fields only — `supersedes_id` and `valid_to` are engine state (filled and
/// closed here), so they never turn a replay into a "mutation".
pub fn register_version(
    versions: &mut Vec<HarnessComponentVersion>,
    supersessions: &mut Vec<HarnessSupersession>,
    mut new: HarnessComponentVersion,
    tx_now: u64,
) -> Result<(), HarnessComponentRefusal> {
    new.validate()?;
    // Replay and mutation are judged before the engine normalizes anything:
    // a stored version already carries the engine-filled supersedes pointer
    // and (once superseded) a closed window, so the comparison must run
    // against the writer's own fields only.
    match versions.iter().find(|v| v.node_id() == new.node_id()) {
        Some(published) if published.same_writer_fields(&new) => return Ok(()),
        Some(_) => return Err(HarnessComponentRefusal::VersionImmutable),
        None => {}
    }
    let component = versions
        .iter()
        .filter(|v| v.component_id == new.component_id)
        .max_by_key(|v| (v.valid_from, v.node_id().clone()));
    resolve_supersedes_pointer(&mut new, component, versions)?;
    record_supersession(versions, supersessions, &new, tx_now);
    versions.push(new);
    Ok(())
}

/// The newest version of `component_id` valid at `as_of` (R801.2), or `None`
/// when no version is valid then: the component was not yet current, or its
/// intervals carry a gap at that time. Every older version stays readable in
/// `versions`.
pub fn current_harness_component<'a>(
    component_id: &str,
    versions: &'a [HarnessComponentVersion],
    as_of: u64,
) -> Option<&'a HarnessComponentVersion> {
    versions
        .iter()
        .filter(|v| v.component_id == component_id)
        .filter(|v| v.valid_from <= as_of && v.valid_to.is_none_or(|to| to > as_of))
        .max_by_key(|v| (v.valid_from, v.node_id().clone()))
}

/// One node of the supersession chain, with the evidence it carries and the
/// content digest recomputed from its content reference (R801.5).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct HarnessVersionEntry {
    /// The version itself: its evidence run id, confidence and valid
    /// interval ride here.
    pub version: HarnessComponentVersion,
    /// The version this one supersedes, by node id (the chain's next hop).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub supersedes_id: Option<String>,
    /// The content digest recomputed from `version.content_ref`; equals
    /// `version.content_digest` for every admitted version.
    pub content_digest_recomputed: String,
}

/// The full supersession chain of one component, newest first (R801.5). The
/// walk follows each version's `supersedes_id` pointer, falling back to the
/// edge table when a record does not carry one; a pointer that names a
/// version the store does not hold stops the walk there rather than
/// inventing a hop.
pub fn version_history(
    component_id: &str,
    versions: &[HarnessComponentVersion],
    supersessions: &[HarnessSupersession],
) -> Vec<HarnessVersionEntry> {
    let mut out = Vec::new();
    let mut cursor = versions
        .iter()
        .filter(|v| v.component_id == component_id)
        .max_by_key(|v| (v.valid_from, v.node_id().clone()));
    while let Some(version) = cursor {
        if out
            .iter()
            .any(|e: &HarnessVersionEntry| e.version.node_id() == version.node_id())
        {
            break;
        }
        out.push(HarnessVersionEntry {
            version: version.clone(),
            supersedes_id: version.supersedes_id.clone(),
            content_digest_recomputed: HarnessComponentVersion::content_digest(
                &version.content_ref,
            ),
        });
        cursor = version
            .supersedes_id
            .as_deref()
            .and_then(|target| versions.iter().find(|v| v.node_id() == target))
            .or_else(|| {
                supersessions
                    .iter()
                    .find(|e| e.new_version == version.node_id())
                    .and_then(|e| {
                        versions
                            .iter()
                            .find(|v| v.node_id() == e.superseded_version)
                    })
            });
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    // spec: EG-ORCH-VOCAB-R801.1
    #[test]
    fn the_eight_components_are_the_closed_set() {
        for kind in [
            HarnessComponentKind::SystemPrompt,
            HarnessComponentKind::Guardrails,
            HarnessComponentKind::Memory,
            HarnessComponentKind::ToolGuidance,
            HarnessComponentKind::Subagent,
            HarnessComponentKind::Skill,
            HarnessComponentKind::Script,
            HarnessComponentKind::Plugin,
        ] {
            assert_eq!(
                HarnessComponentKind::from_wire(kind.wire_name()).unwrap(),
                kind
            );
        }
        assert_eq!(
            HarnessComponentKind::from_wire("widget"),
            Err(HarnessComponentRefusal::KindUnknown)
        );
    }
}
