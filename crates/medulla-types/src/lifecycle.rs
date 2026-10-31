//! Software-lifecycle records (MEDULLA-SDLC): releases, deployments, defects, the
//! derived requirement-state answer and the lifecycle trace.
//!
//! Pure data and bounds. The derivation rules, the defect intake and the status
//! derivation live in `medulla-core`'s `lifecycle` module; these types are the
//! vocabulary they and the served methods share. Every closed set is a serde
//! enum, so an unknown value is refused at decode.

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// Lowercase hex SHA-256, the digest form every lifecycle record uses.
pub fn sha256_hex(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

/// The closed edge vocabulary (MEDULLA-SDLC-R001).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum LifecycleEdgeKind {
    Changes,
    CitesRequirement,
    Implements,
    VerifiedBy,
    Exercises,
    Judges,
    ProducedBy,
    Affects,
    IntroducedBy,
    FixedBy,
    Contains,
    Deploys,
    ObservedIn,
    Raises,
}

impl LifecycleEdgeKind {
    /// Every edge kind, in declaration order.
    pub const ALL: [Self; 14] = [
        Self::Changes,
        Self::CitesRequirement,
        Self::Implements,
        Self::VerifiedBy,
        Self::Exercises,
        Self::Judges,
        Self::ProducedBy,
        Self::Affects,
        Self::IntroducedBy,
        Self::FixedBy,
        Self::Contains,
        Self::Deploys,
        Self::ObservedIn,
        Self::Raises,
    ];

    /// The wire and ontology name of the edge.
    pub fn name(self) -> &'static str {
        match self {
            Self::Changes => "changes",
            Self::CitesRequirement => "citesRequirement",
            Self::Implements => "implements",
            Self::VerifiedBy => "verifiedBy",
            Self::Exercises => "exercises",
            Self::Judges => "judges",
            Self::ProducedBy => "producedBy",
            Self::Affects => "affects",
            Self::IntroducedBy => "introducedBy",
            Self::FixedBy => "fixedBy",
            Self::Contains => "contains",
            Self::Deploys => "deploys",
            Self::ObservedIn => "observedIn",
            Self::Raises => "raises",
        }
    }

    /// The kinds a named derivation rule emits. A direct write of one of these
    /// refuses: the changeset log and the code graph are their only source.
    pub fn is_derived(self) -> bool {
        matches!(
            self,
            Self::Changes
                | Self::CitesRequirement
                | Self::Implements
                | Self::VerifiedBy
                | Self::Exercises
                | Self::Contains
                | Self::IntroducedBy
                | Self::FixedBy
        )
    }
}

/// The record kinds a lifecycle trace can reach.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LifecycleKind {
    Requirement,
    CodeSymbol,
    TestCase,
    Changeset,
    GateVerdict,
    RunTrace,
    Release,
    Deployment,
    Defect,
    Incident,
}

impl LifecycleKind {
    /// Parse a wire name; an unknown name carries the stable refusal code.
    pub fn parse(name: &str) -> Result<Self, LifecycleRefusal> {
        serde_json::from_value(serde_json::Value::String(name.to_owned()))
            .map_err(|_| LifecycleRefusal::KindUnknown(name.to_owned()))
    }
}

/// A typed refusal from the lifecycle records.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum LifecycleRefusal {
    #[error("LIFECYCLE_KIND_UNKNOWN: {0}")]
    KindUnknown(String),
    #[error("LIFECYCLE_TRACE_DEPTH: depth {0} is outside 1..=8")]
    TraceDepth(u8),
    #[error("LIFECYCLE_EDGE_DERIVED_ONLY: {0}")]
    EdgeDerivedOnly(&'static str),
    #[error("LIFECYCLE_ENDPOINT_UNKNOWN: {0}")]
    EndpointUnknown(String),
    #[error("RELEASE_HEAD_UNKNOWN: {0}")]
    ReleaseHeadUnknown(String),
}

impl LifecycleRefusal {
    /// The stable refusal code.
    pub fn code(&self) -> &'static str {
        match self {
            Self::KindUnknown(_) => "LIFECYCLE_KIND_UNKNOWN",
            Self::TraceDepth(_) => "LIFECYCLE_TRACE_DEPTH",
            Self::EdgeDerivedOnly(_) => "LIFECYCLE_EDGE_DERIVED_ONLY",
            Self::EndpointUnknown(_) => "LIFECYCLE_ENDPOINT_UNKNOWN",
            Self::ReleaseHeadUnknown(_) => "RELEASE_HEAD_UNKNOWN",
        }
    }
}

/// A published release of one repository.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Release {
    pub repo: String,
    pub version: String,
    pub git_ref: String,
    pub changeset_head: String,
    pub artifact_digests: Vec<String>,
    pub published_at: u64,
}

impl Release {
    /// The stable record id a deployment links to.
    pub fn id(&self) -> String {
        format!("{}@{}", self.repo, self.version)
    }

    fn names_digest(&self, digest: &str) -> bool {
        self.artifact_digests.iter().any(|d| d == digest)
    }
}

/// How a deployment ended. `Unattributed` means no release names its image.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeploymentOutcome {
    Succeeded,
    Failed,
    RolledBack,
    Unattributed,
}

/// One rollout of an image to an environment.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Deployment {
    pub environment: String,
    pub workload: String,
    pub image_digest: String,
    pub release_ref: Option<String>,
    pub started_at: u64,
    pub finished_at: u64,
    pub outcome: DeploymentOutcome,
}

/// The caller's view of a deployment, before the engine resolves its release.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeploymentInput {
    pub environment: String,
    pub workload: String,
    pub image_digest: String,
    pub started_at: u64,
    pub finished_at: u64,
    pub outcome: DeploymentOutcome,
}

/// Resolve a deployment to its release by image digest. A digest no release
/// names stays `unattributed` with no release; the engine never guesses one.
pub fn record_deployment(releases: &[Release], input: DeploymentInput) -> Deployment {
    let release = releases
        .iter()
        .find(|release| release.names_digest(&input.image_digest));
    let (release_ref, outcome) = match release {
        Some(release) => (Some(release.id()), input.outcome),
        None => (None, DeploymentOutcome::Unattributed),
    };
    Deployment {
        environment: input.environment,
        workload: input.workload,
        image_digest: input.image_digest,
        release_ref,
        started_at: input.started_at,
        finished_at: input.finished_at,
        outcome,
    }
}

/// The changesets a release contains: those after the previous release head up
/// to and including its own head, on the repository chain (oldest first).
pub fn release_range(
    chain: &[String],
    previous_head: Option<&str>,
    head: &str,
) -> Result<Vec<String>, LifecycleRefusal> {
    let position = |id: &str| chain.iter().position(|c| c == id);
    let end =
        position(head).ok_or_else(|| LifecycleRefusal::ReleaseHeadUnknown(head.to_owned()))?;
    let start = match previous_head {
        Some(previous) => position(previous)
            .map(|index| index + 1)
            .ok_or_else(|| LifecycleRefusal::ReleaseHeadUnknown(previous.to_owned()))?,
        None => 0,
    };
    Ok(chain[start.min(end + 1)..=end].to_vec())
}

/// How a defect reached the engine.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DefectIntake {
    GateFailure,
    RunFailure,
    Incident,
    ConnectorIssue,
    Direct,
}

/// A defect's place in its life cycle.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DefectStatus {
    New,
    Triaged,
    Fixing,
    Fixed,
    Verified,
    Closed,
    Rejected,
}

/// How serious a defect is judged to be.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Severity {
    Critical,
    High,
    Medium,
    Low,
}

/// One observation of a defect, with its intake path and source record.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Symptom {
    pub intake: DefectIntake,
    pub source_record_id: String,
    pub observed_at: u64,
    pub text: String,
}

/// A code fault, deduplicated by the digest of its normalized symptom.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Defect {
    pub id: String,
    pub repo: String,
    pub title: String,
    pub intake: DefectIntake,
    pub signature_digest: String,
    pub severity: Severity,
    pub status: DefectStatus,
    pub symptoms: Vec<Symptom>,
    pub created_at: u64,
}

/// The derived state of a requirement row.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum SpecState {
    Specified,
    Landed,
    Verified,
}

/// One requirement row with the evidence behind its state.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SpecRowState {
    pub id: String,
    pub state: SpecState,
    pub landed_by: Vec<String>,
    pub verified_by: Vec<String>,
    pub withdrawn: bool,
}

/// The served answer of `SpecStatus`: every row of one repository at one revision.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SpecStatusAnswer {
    pub repo: String,
    pub rev: String,
    pub rows: Vec<SpecRowState>,
}

/// Deepest walk a lifecycle trace accepts.
pub const TRACE_MAX_DEPTH: u8 = 8;
/// Records one trace may visit before it reports `truncated`.
pub const TRACE_NODE_BUDGET: usize = 10_000;

/// A request for the records reachable from one anchor.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "LifecycleTraceRequestWire")]
pub struct LifecycleTraceRequest {
    pub anchor: String,
    pub depth: u8,
    pub kinds: Vec<LifecycleKind>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct LifecycleTraceRequestWire {
    anchor: String,
    depth: u8,
    #[serde(default)]
    kinds: Vec<LifecycleKind>,
}

impl TryFrom<LifecycleTraceRequestWire> for LifecycleTraceRequest {
    type Error = LifecycleRefusal;

    fn try_from(wire: LifecycleTraceRequestWire) -> Result<Self, Self::Error> {
        Self::new(wire.anchor, wire.depth, wire.kinds)
    }
}

impl LifecycleTraceRequest {
    /// Build a request; a depth outside `1..=8` refuses.
    pub fn new(
        anchor: String,
        depth: u8,
        kinds: Vec<LifecycleKind>,
    ) -> Result<Self, LifecycleRefusal> {
        if !(1..=TRACE_MAX_DEPTH).contains(&depth) {
            return Err(LifecycleRefusal::TraceDepth(depth));
        }
        Ok(Self {
            anchor,
            depth,
            kinds,
        })
    }
}

/// A record a trace reached.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TraceRecord {
    pub id: String,
    pub kind: LifecycleKind,
}

/// An edge a trace reached, with the rule that derived it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TraceEdge {
    pub kind: LifecycleEdgeKind,
    pub from: String,
    pub to: String,
    pub rule: String,
    pub rule_version: u32,
}

/// The records and edges reachable from the anchor.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct LifecycleTraceAnswer {
    pub records: Vec<TraceRecord>,
    pub edges: Vec<TraceEdge>,
    pub truncated: bool,
}
