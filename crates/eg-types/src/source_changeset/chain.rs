//! The served changeset chain of one repository branch (EG-SCM-R002/R003,
//! R008, R009, R012): the wire operations, the durable record and the pure
//! landing and revert judgments. Persistence lives in the server crate; this
//! module owns every rule so the rules are testable without storage.

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

use super::{ChangedPath, Changeset, PathOp, Trailers};
use crate::lifecycle::sha256_hex;

/// Most paths one changeset may carry.
pub const MAX_CHANGESET_PATHS: usize = 4096;
/// Most landed changesets one landing judgment walks back over.
pub const MAX_LANDING_WALK: usize = 4096;

/// Where a changeset is in its life.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum ChangesetState {
    /// Submitted, not on the branch chain yet.
    Proposed,
    /// On the branch chain at `landed_sequence`.
    Landed,
}

/// A gate outcome.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum GateOutcome {
    Succeeded,
    Failed,
}

/// One sealed gate verdict against one changeset.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct GateVerdict {
    pub gate_id: String,
    pub input_digest: String,
    pub verdict: GateOutcome,
    pub evidence_digest: String,
    pub duration_ms: u64,
    pub recorded_by: String,
    pub recorded_at_ms: u64,
}

/// The durable record of one changeset on one branch.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct SourceChangesetRecord {
    pub tenant_id: String,
    pub branch: String,
    pub changeset: Changeset,
    /// The verified agent identity that submitted the changeset.
    pub agent_id: String,
    /// The RunTrace that produced the changeset. Required for an agent.
    pub run_trace_id: Option<String>,
    pub state: ChangesetState,
    /// Position on the branch chain, 1-based, once landed.
    pub landed_sequence: Option<u64>,
    pub landed_at_ms: Option<u64>,
    /// Sealed verdicts, keyed by gate id in id order.
    pub verdicts: Vec<GateVerdict>,
    /// The landed changeset this one reverts.
    pub revert_of: Option<String>,
}

/// The head of one branch chain.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct SourceBranchHead {
    pub repo: String,
    pub branch: String,
    /// Number of landed changesets; 0 for an empty branch.
    pub sequence: u64,
    /// Id of the newest landed changeset.
    pub head: Option<String>,
}

/// The changeset a caller submits. The engine sets the author, the time,
/// the id and the trailers.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct ChangesetSubmission {
    pub tenant_id: String,
    pub repo: String,
    pub branch: String,
    /// The branch head the changeset was written against; empty for the first
    /// changeset of a branch.
    #[serde(default)]
    pub parents: Vec<String>,
    pub message: String,
    pub changed_paths: Vec<ChangedPath>,
    #[serde(default)]
    pub run_trace_id: Option<String>,
}

/// The served source changeset operations.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
pub enum SourceChangesetOp {
    /// Record a proposed changeset (arbora `source.commit`).
    Submit {
        submission: Box<ChangesetSubmission>,
    },
    /// Seal one gate verdict against a proposed changeset.
    RecordVerdict {
        tenant_id: String,
        changeset_id: String,
        gate_id: String,
        input_digest: String,
        verdict: GateOutcome,
        evidence_digest: String,
        duration_ms: u64,
    },
    /// Land a proposed changeset on its branch (arbora `source.land`).
    Land {
        tenant_id: String,
        changeset_id: String,
    },
    /// Land the inverse of a landed changeset (arbora `source.revert`).
    Revert {
        tenant_id: String,
        changeset_id: String,
        message: String,
    },
    /// Read one changeset record.
    Get {
        tenant_id: String,
        changeset_id: String,
    },
    /// Read one branch head.
    Head {
        tenant_id: String,
        repo: String,
        branch: String,
    },
}

impl SourceChangesetOp {
    pub fn tenant_id(&self) -> &str {
        match self {
            Self::Submit { submission } => &submission.tenant_id,
            Self::RecordVerdict { tenant_id, .. }
            | Self::Land { tenant_id, .. }
            | Self::Revert { tenant_id, .. }
            | Self::Get { tenant_id, .. }
            | Self::Head { tenant_id, .. } => tenant_id,
        }
    }

    pub fn is_mutation(&self) -> bool {
        !matches!(self, Self::Get { .. } | Self::Head { .. })
    }

    pub fn authz_action(&self) -> &'static str {
        if self.is_mutation() {
            "source:write"
        } else {
            "source:read"
        }
    }

    /// Stable operation name for audit lines and result contracts.
    pub fn name(&self) -> &'static str {
        match self {
            Self::Submit { .. } => "submit",
            Self::RecordVerdict { .. } => "record_verdict",
            Self::Land { .. } => "land",
            Self::Revert { .. } => "revert",
            Self::Get { .. } => "get",
            Self::Head { .. } => "head",
        }
    }
}

/// Why the engine refused a changeset operation. Every refusal leaves the
/// branch chain unchanged.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SourceRefusal {
    EmptyChangeset,
    InvalidPath(String),
    InvalidName(&'static str),
    StaleBase(String),
    Conflict(Vec<String>),
    GateFailed(String),
    VerdictSealed(String),
    NotProposed(String),
    NotLanded(String),
    RevertDependent(Vec<String>),
    RunTraceRequired,
    UnknownChangeset(String),
    BlobUnreadable(String),
    TooLarge(&'static str),
}

impl SourceRefusal {
    /// The stable wire code.
    pub fn code(&self) -> &'static str {
        match self {
            Self::EmptyChangeset => "SOURCE_EMPTY_CHANGESET",
            Self::InvalidPath(_) => "SOURCE_INVALID_PATH",
            Self::InvalidName(_) => "SOURCE_INVALID_NAME",
            Self::StaleBase(_) => "SOURCE_STALE_BASE",
            Self::Conflict(_) => "SOURCE_CONFLICT",
            Self::GateFailed(_) => "SOURCE_GATE_FAILED",
            Self::VerdictSealed(_) => "SOURCE_VERDICT_SEALED",
            Self::NotProposed(_) => "SOURCE_NOT_PROPOSED",
            Self::NotLanded(_) => "SOURCE_NOT_LANDED",
            Self::RevertDependent(_) => "SOURCE_REVERT_DEPENDENT",
            Self::RunTraceRequired => "SOURCE_RUN_TRACE_REQUIRED",
            Self::UnknownChangeset(_) => "SOURCE_UNKNOWN_CHANGESET",
            Self::BlobUnreadable(_) => "SOURCE_BLOB_UNREADABLE",
            Self::TooLarge(_) => "SOURCE_TOO_LARGE",
        }
    }
}

impl std::fmt::Display for SourceRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let code = self.code();
        match self {
            Self::EmptyChangeset | Self::RunTraceRequired => f.write_str(code),
            Self::InvalidName(what) | Self::TooLarge(what) => write!(f, "{code}: {what}"),
            Self::Conflict(items) | Self::RevertDependent(items) => {
                write!(f, "{code}: {}", items.join(","))
            }
            Self::InvalidPath(item)
            | Self::StaleBase(item)
            | Self::GateFailed(item)
            | Self::VerdictSealed(item)
            | Self::NotProposed(item)
            | Self::NotLanded(item)
            | Self::UnknownChangeset(item)
            | Self::BlobUnreadable(item) => write!(f, "{code}: {item}"),
        }
    }
}

impl std::error::Error for SourceRefusal {}

fn check_name(value: &str, what: &'static str) -> Result<(), SourceRefusal> {
    if value.is_empty() || value.len() > 512 || value.contains('\u{1f}') {
        return Err(SourceRefusal::InvalidName(what));
    }
    Ok(())
}

fn check_path(path: &ChangedPath) -> Result<(), SourceRefusal> {
    let bad_shape = path.path.is_empty()
        || path.path.starts_with('/')
        || path.path.split('/').any(|part| part.is_empty() || part == "..");
    let blobs_match = match path.op {
        PathOp::Add => path.old_blob.is_none() && path.new_blob.is_some(),
        PathOp::Delete => path.old_blob.is_some() && path.new_blob.is_none(),
        PathOp::Modify | PathOp::Rename => path.old_blob.is_some() && path.new_blob.is_some(),
    };
    if bad_shape || !blobs_match {
        return Err(SourceRefusal::InvalidPath(path.path.clone()));
    }
    Ok(())
}

/// The storage key of one branch chain.
pub fn branch_key(repo: &str, branch: &str) -> String {
    format!("{repo}\u{1f}{branch}")
}

/// Every blob digest a changeset points at.
pub fn referenced_blobs(changeset: &Changeset) -> BTreeSet<&str> {
    changeset
        .changed_paths
        .iter()
        .flat_map(|path| [path.old_blob.as_deref(), path.new_blob.as_deref()])
        .flatten()
        .collect()
}

/// The paths a changeset touches.
pub fn path_set(changeset: &Changeset) -> BTreeSet<&str> {
    changeset
        .changed_paths
        .iter()
        .map(|path| path.path.as_str())
        .collect()
}

fn changeset_id(tenant_id: &str, branch: &str, changeset: &Changeset) -> String {
    let identity = (
        tenant_id,
        &changeset.repo,
        branch,
        &changeset.parents,
        &changeset.author_principal,
        &changeset.message,
        &changeset.changed_paths,
    );
    sha256_hex(&serde_json::to_vec(&identity).unwrap_or_default())
}

/// Build the proposed record for a submission. `is_agent` says whether the
/// verified caller acts as an agent, which makes the run trace mandatory.
pub fn propose(
    submission: ChangesetSubmission,
    author_principal: &str,
    agent_id: &str,
    is_agent: bool,
    created_at: u64,
) -> Result<SourceChangesetRecord, SourceRefusal> {
    check_name(&submission.repo, "repo")?;
    check_name(&submission.branch, "branch")?;
    if submission.changed_paths.is_empty() {
        return Err(SourceRefusal::EmptyChangeset);
    }
    if submission.changed_paths.len() > MAX_CHANGESET_PATHS {
        return Err(SourceRefusal::TooLarge("changed_paths"));
    }
    if submission.parents.len() > 1 {
        return Err(SourceRefusal::TooLarge("parents"));
    }
    let mut seen = BTreeSet::new();
    for path in &submission.changed_paths {
        check_path(path)?;
        if !seen.insert(path.path.as_str()) {
            return Err(SourceRefusal::InvalidPath(path.path.clone()));
        }
    }
    let run_trace_id = submission.run_trace_id.filter(|id| !id.is_empty());
    if is_agent && run_trace_id.is_none() {
        return Err(SourceRefusal::RunTraceRequired);
    }
    let trailers = Trailers::parse(&submission.message);
    let mut changeset = Changeset {
        id: String::new(),
        repo: submission.repo,
        parents: submission.parents,
        author_principal: author_principal.to_owned(),
        message: submission.message,
        changed_paths: submission.changed_paths,
        created_at,
        trailers,
    };
    changeset.id = changeset_id(&submission.tenant_id, &submission.branch, &changeset);
    Ok(SourceChangesetRecord {
        tenant_id: submission.tenant_id,
        branch: submission.branch,
        changeset,
        agent_id: agent_id.to_owned(),
        run_trace_id,
        state: ChangesetState::Proposed,
        landed_sequence: None,
        landed_at_ms: None,
        verdicts: Vec::new(),
        revert_of: None,
    })
}

/// Seal one verdict on a proposed record. A second verdict for the same gate
/// refuses, whatever its value.
pub fn seal_verdict(
    record: &mut SourceChangesetRecord,
    verdict: GateVerdict,
) -> Result<(), SourceRefusal> {
    if record.state != ChangesetState::Proposed {
        return Err(SourceRefusal::NotProposed(record.changeset.id.clone()));
    }
    check_name(&verdict.gate_id, "gate_id")?;
    if verdict.input_digest != record.changeset.id {
        return Err(SourceRefusal::InvalidName("input_digest"));
    }
    match record
        .verdicts
        .binary_search_by(|v| v.gate_id.cmp(&verdict.gate_id))
    {
        Ok(_) => Err(SourceRefusal::VerdictSealed(verdict.gate_id)),
        Err(at) => {
            record.verdicts.insert(at, verdict);
            Ok(())
        }
    }
}

/// One landed changeset as the landing judgment sees it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LandedEntry {
    pub id: String,
    pub paths: BTreeSet<String>,
}

/// Judge whether `record` may land on a branch whose landed changesets after
/// the record's base are `since_base`, newest first. `base_on_chain` says
/// whether the record's parent is on the chain (an empty parent list is the
/// branch root).
///
/// Disjoint path sets land with no lock on the rest of the branch. An overlap
/// refuses with every conflicting path. A failed gate verdict refuses first.
pub fn judge_landing(
    record: &SourceChangesetRecord,
    base_on_chain: bool,
    since_base: &[LandedEntry],
) -> Result<(), SourceRefusal> {
    if record.state != ChangesetState::Proposed {
        return Err(SourceRefusal::NotProposed(record.changeset.id.clone()));
    }
    if let Some(failed) = record
        .verdicts
        .iter()
        .find(|v| v.verdict == GateOutcome::Failed)
    {
        return Err(SourceRefusal::GateFailed(failed.gate_id.clone()));
    }
    if !base_on_chain {
        let base = record.changeset.parents.first().cloned().unwrap_or_default();
        return Err(SourceRefusal::StaleBase(base));
    }
    let mine = path_set(&record.changeset);
    let conflicts: BTreeSet<&str> = since_base
        .iter()
        .flat_map(|entry| entry.paths.iter())
        .map(String::as_str)
        .filter(|path| mine.contains(path))
        .collect();
    if conflicts.is_empty() {
        Ok(())
    } else {
        Err(SourceRefusal::Conflict(
            conflicts.into_iter().map(str::to_owned).collect(),
        ))
    }
}

fn inverse(path: &ChangedPath) -> ChangedPath {
    let op = match path.op {
        PathOp::Add => PathOp::Delete,
        PathOp::Delete => PathOp::Add,
        other => other,
    };
    ChangedPath {
        path: path.path.clone(),
        op,
        old_blob: path.new_blob.clone(),
        new_blob: path.old_blob.clone(),
        mode: path.mode,
    }
}

/// Build the revert of a landed record on top of `head`. `later` are the
/// changesets landed after the target, newest first; any of them that touched
/// a reverted path depends on it and refuses the revert.
pub fn build_revert(
    target: &SourceChangesetRecord,
    head: &str,
    later: &[LandedEntry],
    message: String,
    author_principal: &str,
    agent_id: &str,
    created_at: u64,
) -> Result<SourceChangesetRecord, SourceRefusal> {
    if target.state != ChangesetState::Landed {
        return Err(SourceRefusal::NotLanded(target.changeset.id.clone()));
    }
    let reverted = path_set(&target.changeset);
    let dependents: Vec<String> = later
        .iter()
        .filter(|entry| entry.paths.iter().any(|p| reverted.contains(p.as_str())))
        .map(|entry| entry.id.clone())
        .collect();
    if !dependents.is_empty() {
        return Err(SourceRefusal::RevertDependent(dependents));
    }
    let mut changeset = Changeset {
        id: String::new(),
        repo: target.changeset.repo.clone(),
        parents: vec![head.to_owned()],
        author_principal: author_principal.to_owned(),
        trailers: Trailers::parse(&message),
        message,
        changed_paths: target.changeset.changed_paths.iter().map(inverse).collect(),
        created_at,
    };
    changeset.id = changeset_id(&target.tenant_id, &target.branch, &changeset);
    Ok(SourceChangesetRecord {
        tenant_id: target.tenant_id.clone(),
        branch: target.branch.clone(),
        changeset,
        agent_id: agent_id.to_owned(),
        run_trace_id: target.run_trace_id.clone(),
        state: ChangesetState::Proposed,
        landed_sequence: None,
        landed_at_ms: None,
        verdicts: Vec::new(),
        revert_of: Some(target.changeset.id.clone()),
    })
}

/// Group landed entries by id, for callers that index by path.
pub fn entry_of(record: &SourceChangesetRecord) -> LandedEntry {
    LandedEntry {
        id: record.changeset.id.clone(),
        paths: path_set(&record.changeset)
            .into_iter()
            .map(str::to_owned)
            .collect(),
    }
}

#[cfg(test)]
#[path = "chain_tests.rs"]
mod tests;
