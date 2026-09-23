//! GOC-20 — the atomic WorkItem outcome/provenance bundle and durable run-event
//! wire contract (BUG-015).
//!
//! A WorkItem may report a terminal success only when its immutable result/
//! artifact references and its RunTrace/ToolCall/OutcomeEvaluation provenance
//! are committed together, under one fence — never a result visible without
//! its provenance, or provenance visible without its result. This module
//! freezes the `CommitOutcomeBundle`/`RunEvent` wire shape
//! (`plans/graph-os-completion-program/lanes/GOC-20-atomic-outcome-provenance-streaming.md`
//! and `decisions/GOC-20-atomic-outcome-provenance.md`) as a NEW, additive
//! module, exactly as GOC-03's `commit_descriptor` module was added rather
//! than folded into `mutation_batch.rs`.
//!
//! The server mutation compiler lowers the optional terminal extension into
//! the existing `CommitWorkItemResult` shape and one conditional mutation
//! outbox intent. The native WorkItem applier remains the single status/CAS
//! authority and writes the bound receipt rows only after a real transition.
//!
//! `CommitOutcomeBundle` deliberately does not embed a full
//! [`crate::commit_descriptor::CommitDescriptor`]: GOC-03's descriptor is
//! the cross-domain commit IDENTITY a participant registers a digest against
//! once native wiring lands, not a payload this module should duplicate.
//! `commit_participant_domains` instead names which [`CommitParticipantDomain`]
//! entries an applied bundle would register — `Evidence` (RunTrace/ToolCall),
//! `AnalyticsOutcome` (OutcomeEvaluation), and `Outbox` (the one durable
//! `RunEvent`) — so the eventual native applier has an unambiguous mapping
//! from this bundle to GOC-03's participant registry.

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::commit_descriptor::CommitParticipantDomain;

mod engine_fields;
mod validation;

/// Current wire/on-disk schema version for [`CommitOutcomeBundle`] and
/// [`RunEvent`]. Mirrors [`crate::commit_descriptor::COMMIT_DESCRIPTOR_VERSION`]'s
/// fail-closed-on-mismatch convention (see this crate's "No Legacy" edict —
/// no compatibility shim for an unsupported version).
pub const OUTCOME_BUNDLE_VERSION: u16 = 1;
pub const MAX_OUTCOME_ARTIFACTS: usize = 128;
pub const MAX_OUTCOME_ARTIFACT_BYTES: u64 = 64 * 1024 * 1024;
pub const MAX_OUTCOME_REF_BYTES: usize = 4 * 1024;
pub const MAX_RECEIPT_NODES: usize = 512;
pub const MAX_RECEIPT_PROPERTIES_BYTES: usize = 512 * 1024;
pub const MAX_LANGFUSE_OBSERVATION_REFS: usize = 512;
/// Topic for the one durable RunEvent intent emitted with a terminal receipt.
pub const RUN_EVENT_OUTBOX_TOPIC: &str = "engine.run-event.v1";

/// The GOC-03 participant domains a [`CommitOutcomeBundle`] registers once its
/// native commit path exists. Fixed at exactly these three for every bundle
/// (never a caller-supplied set): a WorkItem outcome bundle always carries
/// RunTrace/ToolCall provenance (`Evidence`), an OutcomeEvaluation
/// (`AnalyticsOutcome`), and the one durable event it publishes (`Outbox`).
pub const OUTCOME_BUNDLE_PARTICIPANT_DOMAINS: [CommitParticipantDomain; 3] = [
    CommitParticipantDomain::Evidence,
    CommitParticipantDomain::AnalyticsOutcome,
    CommitParticipantDomain::Outbox,
];

/// Terminal completeness state of a committed [`CommitOutcomeBundle`]
/// (lane doc "Authority and invariants" + the decision record's "Completeness
/// semantics"). Distinct from [`crate::commit_descriptor::CommitStatus`]:
/// that enum describes the CROSS-DOMAIN COMMIT's own prepare/publish
/// lifecycle; this one describes whether the WorkItem's REQUIRED provenance
/// specifically landed once the commit itself succeeded.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum OutcomeCompleteness {
    /// The authoritative outcome AND every required provenance node/edge
    /// (RunTrace, OutcomeEvaluation, and the full ToolCall set when the run
    /// made tool calls) are durably committed. Never claimed on the strength
    /// of the outcome commit alone.
    Complete,
    /// The authoritative outcome committed, but one or more required
    /// provenance components did not. Always carries a non-empty
    /// `missing_refs` naming which ones.
    Degraded,
    /// A `Degraded` outcome a reconciler has claimed and is actively
    /// repairing. Repair is idempotent and MUST NOT alter `result_digest`.
    Reconciling,
}

/// One immutable, content-addressed artifact reference a
/// [`CommitOutcomeBundle`] carries. The bundle never embeds artifact bytes —
/// only a bounded reference/digest pair, mirroring
/// [`crate::commit_descriptor::CommitDescriptor`]'s "digests cover canonical
/// encoded participant bytes, never mutable pointers or raw content" rule.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct OutcomeArtifactRef {
    /// Opaque CAS/artifact reference (never a raw filesystem path or URL).
    pub artifact_ref: String,
    /// sha256 digest of the artifact's canonical encoded bytes.
    pub digest: String,
    /// Caller-defined artifact kind (e.g. `"document"`, `"table"`, `"log"`).
    pub kind: String,
    pub size_bytes: u64,
    /// GOC-16 classification label carried alongside the reference so a
    /// projection can enforce policy without a second lookup.
    pub classification: String,
}

impl OutcomeArtifactRef {
    fn validate(&self) -> Result<(), String> {
        validate_opaque_ref("artifact_ref", &self.artifact_ref)?;
        validate_sha256("artifact digest", &self.digest)?;
        require_bounded_text("artifact kind", &self.kind, MAX_OUTCOME_REF_BYTES)?;
        require_bounded_text(
            "artifact classification",
            &self.classification,
            MAX_OUTCOME_REF_BYTES,
        )?;
        if self.size_bytes > MAX_OUTCOME_ARTIFACT_BYTES {
            return Err("artifact size exceeds the native bound".to_string());
        }
        Ok(())
    }
}

/// Receipt node kinds that may ride alongside a terminal WorkItem result.
/// Every node uses the existing graph node payload shape; this type only binds
/// its identity and bounded property bytes to the terminal commit.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum ReceiptNodeKind {
    RunTrace,
    ToolCall,
    OutcomeEvaluation,
}

/// One Langfuse observation identity.  EG records the opaque receipt and its
/// digest; the Langfuse client/export path remains outside the graph authority.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct LangfuseObservationRef {
    pub observation_id: String,
    pub digest: String,
}

impl LangfuseObservationRef {
    fn validate(&self) -> Result<(), String> {
        require_text("observation_id", &self.observation_id)?;
        validate_sha256("observation digest", &self.digest)
    }
}

/// A graph node that is co-committed with one terminal result.
///
/// The `properties_msgpack` bytes are the same bounded `AddNode` payload the
/// existing native B9 applier accepts.  The digest is checked against those
/// bytes, while every identity/fence/outbox field is checked against the
/// enclosing [`CommitOutcomeBundle`].  Consequently a schema-valid node from
/// another WorkItem or lease cannot be smuggled into the batch.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ReceiptNode {
    pub node_id: String,
    pub kind: ReceiptNodeKind,
    pub delegation_id: String,
    pub work_item_id: String,
    pub run_id: String,
    pub fence_token: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result_ref: Option<String>,
    pub outbox_id: String,
    /// Opaque CAS/ref-store identity for the redacted node payload.
    pub payload_ref: String,
    /// SHA-256 of the canonical `properties_msgpack` bytes.
    pub payload_digest: String,
    #[cfg_attr(feature = "contract-schema", schemars(with = "Vec<u8>"))]
    #[serde(with = "serde_bytes")]
    pub properties_msgpack: Vec<u8>,
}

impl ReceiptNode {
    fn validate(&self) -> Result<(), String> {
        for (field, value) in [
            ("node_id", &self.node_id),
            ("delegation_id", &self.delegation_id),
            ("work_item_id", &self.work_item_id),
            ("run_id", &self.run_id),
            ("outbox_id", &self.outbox_id),
            ("payload_ref", &self.payload_ref),
        ] {
            require_text(field, value)?;
        }
        if self.fence_token == 0 {
            return Err("receipt node fence_token must be non-zero".to_string());
        }
        if let Some(result_ref) = &self.result_ref {
            validate_opaque_ref("receipt node result_ref", result_ref)?;
        }
        if self.properties_msgpack.is_empty() {
            return Err("receipt node properties_msgpack must not be empty".to_string());
        }
        if self.properties_msgpack.len() > MAX_RECEIPT_PROPERTIES_BYTES {
            return Err("receipt node properties_msgpack exceeds the native bound".to_string());
        }
        validate_opaque_ref("receipt node payload_ref", &self.payload_ref)?;
        validate_sha256("receipt node payload_digest", &self.payload_digest)?;
        let computed = hex::encode(Sha256::digest(&self.properties_msgpack));
        if self.payload_digest != computed {
            return Err(
                "receipt node payload_digest does not match properties_msgpack".to_string(),
            );
        }
        Ok(())
    }
}

/// Explicit optional extension for the current terminal WorkItem contract.
///
/// The protocol owner may add this as one `#[serde(default)]` field on the
/// existing terminal method.  Keeping the extension grouped and optional
/// makes the generated contract change visible without changing the native
/// terminal method's identity fields or inventing a second commit operation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct TerminalOutcomeExtension {
    pub outcome_bundle: CommitOutcomeBundle,
    #[serde(default)]
    pub receipt_nodes: Vec<ReceiptNode>,
    /// The one durable RunEvent carried by the native mutation outbox.
    pub run_event: RunEvent,
}

impl TerminalOutcomeExtension {
    pub fn validate(&self) -> Result<(), String> {
        self.outcome_bundle
            .validate_receipt_nodes(&self.receipt_nodes)?;
        self.run_event.validate_for_bundle(&self.outcome_bundle)
    }
}

/// The one atomic terminal result/artifact/provenance/outbox unit a WorkItem
/// commits under (lane doc "Schemas/APIs"). `work_item_id`/`fence_token` bind
/// this bundle to the SAME lease epoch/fencing token
/// `Method::CommitWorkItemResult` already CAS's on
/// (`src/server/mutation_batch.rs`'s `WorkItemBatchIdentity`) — a stale or
/// duplicate writer is rejected there today, and this bundle's eventual
/// native applier must reuse that identical fencing rather than inventing a
/// second one.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct CommitOutcomeBundle {
    pub schema_version: u16,
    /// Delegation identity shared by the admission WorkItem and all terminal
    /// receipt nodes.  This is not a second commit identity; the native
    /// WorkItem batch/outbox remains authoritative.
    pub delegation_id: String,
    /// Authenticated delegator (A), distinct from the selected Agent Library
    /// entry (B) and from the worker holding the lease.
    pub delegator_id: String,
    /// Retained Agent Library identity selected at admission (B).
    pub selected_agent_id: String,
    /// The worker identity that held the lease for this terminal transition.
    pub executor_lease_actor: String,
    /// The exact terminal outcome supplied to `CommitWorkItemResult`.
    pub outcome: String,
    pub work_item_id: String,
    /// The lease fencing token this bundle commits under — MUST match the
    /// durable WorkItem row's current `fencing_token`; a stale value is
    /// rejected exactly like `CommitWorkItemResult`'s existing fencing CAS.
    pub fence_token: u64,
    /// Opaque run identity this bundle's RunTrace/ToolCall/OutcomeEvaluation
    /// are keyed under (`observability/trace_ontology.py`'s trace id family).
    pub run_id: String,
    /// Opaque, immutable reference to the terminal result body. Never the
    /// body itself — mirrors `Method::CommitWorkItemResult`'s existing
    /// `result_ref: Option<String>`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result_ref: Option<String>,
    /// sha256 digest of the canonical encoded result body, present whenever
    /// `result_ref` is. A repair/reconcile pass may never change this once
    /// committed (lane doc: "Repair is idempotent and cannot alter result
    /// digest").
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result_digest: Option<String>,
    #[serde(default)]
    pub artifacts: Vec<OutcomeArtifactRef>,
    /// Opaque reference to the durable RunTrace node this bundle's provenance
    /// batch wrote (`observability/trace_ontology.py`'s trace node id).
    pub trace_ref: String,
    /// Opaque references to the durable ToolCall nodes this bundle's
    /// provenance batch wrote, in call order. Never synthesized from a
    /// progress receipt or free-text log — every entry names a real node this
    /// SAME bundle committed.
    #[serde(default)]
    pub tool_call_refs: Vec<String>,
    /// Opaque reference to the durable OutcomeEvaluation node.
    pub outcome_ref: String,
    /// Digest of the capability/tool catalog snapshot the run executed
    /// against (GOC-19/GOC-21 currency binding).
    pub capability_digest: String,
    pub catalog_digest: String,
    pub policy_digest: String,
    pub model_digest: String,
    /// Monotonic, process-anchored sequence for this run
    /// (`observability/trace_ontology.next_event_sequence()`'s existing
    /// authority — this bundle does not mint a second one).
    pub event_sequence: u64,
    pub completeness: OutcomeCompleteness,
    /// Names of the specific required components missing when
    /// `completeness != Complete` (e.g. `"tool_call:3"`,
    /// `"outcome_evaluation"`). Empty only when `completeness == Complete`.
    #[serde(default)]
    pub missing_refs: Vec<String>,
    /// Identity of the one durable outbox row this bundle's commit publishes.
    pub outbox_id: String,
    /// Opaque Langfuse observation receipts attached to this same run.  The
    /// observation store is a projection and never a second authority.
    #[serde(default)]
    pub langfuse_observation_refs: Vec<LangfuseObservationRef>,
}

impl CommitOutcomeBundle {
    /// The [`CommitParticipantDomain`]s this bundle registers once applied
    /// natively. Fixed, not derived from the bundle's own content — see this
    /// module's doc for why.
    pub fn participant_domains(&self) -> &'static [CommitParticipantDomain] {
        &OUTCOME_BUNDLE_PARTICIPANT_DOMAINS
    }

    pub fn validate(&self) -> Result<(), String> {
        if self.schema_version != OUTCOME_BUNDLE_VERSION {
            return Err(format!(
                "unsupported outcome bundle version {} (expected {})",
                self.schema_version, OUTCOME_BUNDLE_VERSION
            ));
        }
        self.required_refs_nonempty()?;
        validate_terminal_outcome("outcome", &self.outcome)?;
        for (field, digest) in [
            ("capability_digest", &self.capability_digest),
            ("catalog_digest", &self.catalog_digest),
            ("policy_digest", &self.policy_digest),
            ("model_digest", &self.model_digest),
        ] {
            validate_sha256(field, digest)?;
        }
        if self.artifacts.len() > MAX_OUTCOME_ARTIFACTS {
            return Err("artifacts exceed the native bound".to_string());
        }
        for artifact in &self.artifacts {
            artifact.validate()?;
        }
        self.validate_langfuse_refs()?;
        if self.fence_token == 0 {
            return Err("fence_token must be non-zero".to_string());
        }
        if self.result_ref.is_some() != self.result_digest.is_some() {
            return Err(
                "result_ref and result_digest must both be present or both absent".to_string(),
            );
        }
        if let Some(result_digest) = &self.result_digest {
            validate_sha256("result_digest", result_digest)?;
        }
        self.validate_completion_refs()?;
        validate_completeness_currency(self.completeness, &self.missing_refs)?;
        self.validate_completeness_invariant()
    }

    /// Every identity/digest/ref field named below must be non-empty (after trimming).
    fn required_refs_nonempty(&self) -> Result<(), String> {
        for (field, value) in [
            ("delegation_id", &self.delegation_id),
            ("delegator_id", &self.delegator_id),
            ("selected_agent_id", &self.selected_agent_id),
            ("executor_lease_actor", &self.executor_lease_actor),
            ("work_item_id", &self.work_item_id),
            ("run_id", &self.run_id),
            ("trace_ref", &self.trace_ref),
            ("outcome_ref", &self.outcome_ref),
            ("capability_digest", &self.capability_digest),
            ("catalog_digest", &self.catalog_digest),
            ("policy_digest", &self.policy_digest),
            ("model_digest", &self.model_digest),
            ("outbox_id", &self.outbox_id),
        ] {
            require_bounded_text(field, value, MAX_OUTCOME_REF_BYTES)?;
        }
        Ok(())
    }

    /// `Complete` must carry no `missing_refs` and must have a `result_ref`;
    /// `Degraded`/`Reconciling` must name what is missing.
    fn validate_completeness_invariant(&self) -> Result<(), String> {
        match self.completeness {
            OutcomeCompleteness::Complete => {
                if !self.missing_refs.is_empty() {
                    return Err(
                        "Complete must not carry missing_refs -- see the lane's invariant \
                         'never claim complete with a known-missing component'"
                            .to_string(),
                    );
                }
                if self.result_ref.is_none() {
                    return Err("Complete requires an immutable result_ref".to_string());
                }
            }
            OutcomeCompleteness::Degraded | OutcomeCompleteness::Reconciling => {
                if self.missing_refs.is_empty() {
                    return Err(format!(
                        "{:?} requires a non-empty missing_refs naming what is absent",
                        self.completeness
                    ));
                }
            }
        }
        Ok(())
    }

    fn validate_langfuse_refs(&self) -> Result<(), String> {
        if self.langfuse_observation_refs.len() > MAX_LANGFUSE_OBSERVATION_REFS {
            return Err("langfuse observation refs exceed the native bound".to_string());
        }
        let mut ids = BTreeSet::new();
        for observation in &self.langfuse_observation_refs {
            observation.validate()?;
            if !ids.insert(&observation.observation_id) {
                return Err("duplicate langfuse observation_id".to_string());
            }
        }
        Ok(())
    }

    fn validate_completion_refs(&self) -> Result<(), String> {
        let mut refs = BTreeSet::new();
        for (field, value) in [
            ("trace_ref", self.trace_ref.as_str()),
            ("outcome_ref", self.outcome_ref.as_str()),
        ] {
            if !refs.insert(value) {
                return Err(format!("{field} duplicates another completion reference"));
            }
        }
        for value in &self.tool_call_refs {
            require_text("tool_call_ref", value)?;
            if !refs.insert(value.as_str()) {
                return Err("duplicate or overlapping tool_call_ref".to_string());
            }
        }
        Ok(())
    }

    /// Validate the exact node identities that the terminal commit will
    /// co-commit through the existing `CommitWorkItemResult` native operation.
    ///
    /// This is intentionally separate from [`Self::validate`]: a caller may
    /// validate the bundle while constructing a request, but the native commit
    /// owner must call this method immediately before applying the batch, after
    /// it has the actual node payloads.  It therefore checks content digests,
    /// WorkItem/fence/result/outbox equality, duplicate IDs, and completeness.
    pub fn validate_receipt_nodes(&self, nodes: &[ReceiptNode]) -> Result<(), String> {
        self.validate()?;
        if nodes.len() > MAX_RECEIPT_NODES {
            return Err("receipt nodes exceed the native bound".to_string());
        }
        let mut seen = BTreeSet::new();
        let mut actual = BTreeSet::new();
        for node in nodes {
            validate_receipt_node(self, node, &mut seen)?;
            actual.insert(node.node_id.as_str());
        }

        let mut expected = BTreeSet::new();
        expected.insert(self.trace_ref.as_str());
        expected.insert(self.outcome_ref.as_str());
        expected.extend(self.tool_call_refs.iter().map(String::as_str));
        let missing: Vec<&str> = expected.difference(&actual).copied().collect();
        validate_missing_receipt_nodes(self, &missing)
    }
}

fn validate_receipt_node(
    bundle: &CommitOutcomeBundle,
    node: &ReceiptNode,
    seen: &mut BTreeSet<String>,
) -> Result<(), String> {
    node.validate()?;
    if !seen.insert(node.node_id.clone()) {
        return Err(format!("duplicate receipt node id '{}'", node.node_id));
    }
    if !receipt_node_matches_bundle(bundle, node) {
        return Err(format!(
            "receipt node '{}' is not bound to the outcome bundle",
            node.node_id
        ));
    }
    if !receipt_node_kind_matches_bundle(bundle, node) {
        return Err(format!(
            "receipt node '{}' has an unexpected kind or id",
            node.node_id
        ));
    }
    validation::validate_receipt_properties(bundle, node)?;
    Ok(())
}

fn receipt_kind_name(kind: ReceiptNodeKind) -> &'static str {
    match kind {
        ReceiptNodeKind::RunTrace => "run_trace",
        ReceiptNodeKind::ToolCall => "tool_call",
        ReceiptNodeKind::OutcomeEvaluation => "outcome_evaluation",
    }
}

fn receipt_node_matches_bundle(bundle: &CommitOutcomeBundle, node: &ReceiptNode) -> bool {
    node.delegation_id == bundle.delegation_id
        && node.work_item_id == bundle.work_item_id
        && node.run_id == bundle.run_id
        && node.fence_token == bundle.fence_token
        && node.outbox_id == bundle.outbox_id
        && node.result_ref == bundle.result_ref
}

fn receipt_node_kind_matches_bundle(bundle: &CommitOutcomeBundle, node: &ReceiptNode) -> bool {
    match node.kind {
        ReceiptNodeKind::RunTrace => node.node_id == bundle.trace_ref,
        ReceiptNodeKind::ToolCall => bundle.tool_call_refs.contains(&node.node_id),
        ReceiptNodeKind::OutcomeEvaluation => node.node_id == bundle.outcome_ref,
    }
}

fn validate_missing_receipt_nodes(
    bundle: &CommitOutcomeBundle,
    missing: &[&str],
) -> Result<(), String> {
    if matches!(bundle.completeness, OutcomeCompleteness::Complete) && !missing.is_empty() {
        return Err(format!(
            "Complete outcome is missing co-committed receipt nodes: {}",
            missing.join(", ")
        ));
    }
    if !matches!(
        bundle.completeness,
        OutcomeCompleteness::Degraded | OutcomeCompleteness::Reconciling
    ) {
        return Ok(());
    }
    for node_id in missing {
        let named = missing_receipt_name(bundle, node_id);
        if !bundle
            .missing_refs
            .iter()
            .any(|reference| reference == node_id || reference == named)
        {
            return Err(format!(
                "{:?} outcome does not name missing receipt node '{}'",
                bundle.completeness, node_id
            ));
        }
    }
    Ok(())
}

fn missing_receipt_name(bundle: &CommitOutcomeBundle, node_id: &str) -> &'static str {
    if node_id == bundle.trace_ref.as_str() {
        "run_trace"
    } else if node_id == bundle.outcome_ref.as_str() {
        "outcome_evaluation"
    } else {
        "tool_call"
    }
}

fn require_text(field: &str, value: &str) -> Result<(), String> {
    if value.trim().is_empty() {
        return Err(format!("{field} must not be empty"));
    }
    if value.chars().any(|character| character.is_control()) {
        return Err(format!("{field} contains a control character"));
    }
    Ok(())
}

fn validate_terminal_outcome(field: &str, value: &str) -> Result<(), String> {
    if !matches!(value, "succeeded" | "failed" | "cancelled") {
        return Err(format!("{field} must be succeeded, failed, or cancelled"));
    }
    Ok(())
}

fn validate_completeness_currency(
    completeness: OutcomeCompleteness,
    missing_refs: &[String],
) -> Result<(), String> {
    if missing_refs.len() > MAX_RECEIPT_NODES {
        return Err("missing_refs exceed the native bound".to_string());
    }
    let mut seen = BTreeSet::new();
    for reference in missing_refs {
        require_bounded_text("missing_ref", reference, MAX_OUTCOME_REF_BYTES)?;
        if !seen.insert(reference) {
            return Err("missing_refs contain a duplicate reference".to_string());
        }
    }
    match completeness {
        OutcomeCompleteness::Complete if !missing_refs.is_empty() => Err(
            "Complete must not carry missing_refs -- see the lane's invariant \
             'never claim complete with a known-missing component'"
                .to_string(),
        ),
        OutcomeCompleteness::Degraded | OutcomeCompleteness::Reconciling
            if missing_refs.is_empty() =>
        {
            Err(format!(
                "{completeness:?} requires a non-empty missing_refs naming what is absent"
            ))
        }
        _ => Ok(()),
    }
}

fn completeness_name(completeness: OutcomeCompleteness) -> &'static str {
    match completeness {
        OutcomeCompleteness::Complete => "complete",
        OutcomeCompleteness::Degraded => "degraded",
        OutcomeCompleteness::Reconciling => "reconciling",
    }
}

fn require_bounded_text(field: &str, value: &str, max_bytes: usize) -> Result<(), String> {
    require_text(field, value)?;
    if value.len() > max_bytes {
        return Err(format!("{field} exceeds {max_bytes} bytes"));
    }
    Ok(())
}

fn validate_opaque_ref(field: &str, value: &str) -> Result<(), String> {
    require_bounded_text(field, value, MAX_OUTCOME_REF_BYTES)?;
    if value.contains('/') || value.contains('\\') || value.contains("://") {
        return Err(format!("{field} must be an opaque reference"));
    }
    Ok(())
}

fn validate_sha256(field: &str, digest: &str) -> Result<(), String> {
    if digest.len() != 64 || !digest.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(format!("{field} must be a 64-character sha256 hex digest"));
    }
    if digest.bytes().any(|byte| byte.is_ascii_uppercase()) {
        return Err(format!("{field} must use lowercase hexadecimal"));
    }
    Ok(())
}

/// One durable projection event a [`CommitOutcomeBundle`]'s commit (or a
/// repair pass) publishes. Streaming/AG-UI/SSE/WebSocket/terminal/MCP
/// surfaces are projections of a sequence of these, never a second event
/// authority (lane doc "Cursor/streaming semantics").
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct RunEvent {
    pub schema_version: u16,
    pub delegation_id: String,
    pub delegator_id: String,
    pub selected_agent_id: String,
    pub executor_lease_actor: String,
    pub outcome: String,
    pub work_item_id: String,
    pub run_id: String,
    pub fence_token: u64,
    pub outbox_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result_ref: Option<String>,
    pub capability_digest: String,
    pub catalog_digest: String,
    pub policy_digest: String,
    pub model_digest: String,
    /// Same numeric authority as [`CommitOutcomeBundle::event_sequence`] —
    /// never a timestamp or opaque string (the decision record's explicit
    /// rule for every GOC-21/22/23/25/29/31 consumer).
    pub event_sequence: u64,
    pub completeness: OutcomeCompleteness,
    #[serde(default)]
    pub missing_refs: Vec<String>,
    /// Caller-defined event kind (e.g. `"tool_call"`, `"outcome"`,
    /// `"degraded"`, `"reconciled"`).
    pub kind: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool_call_ref: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub outcome_ref: Option<String>,
    /// sha256 digest of this event's canonical encoded payload — the event
    /// carries a reference/digest, never raw prompt/tool-argument content
    /// (GOC-16 classification applies upstream, at bundle-commit time).
    pub payload_digest: String,
    pub timestamp_ms: u64,
    /// Opaque, resume-safe cursor token a stream consumer persists and
    /// replays from — tenant/actor scoped, never crossing runs or audiences.
    pub cursor_token: String,
    /// Digest of the GOC-15 carrier/session this event was emitted under.
    pub carrier_digest: String,
}

impl RunEvent {
    pub fn validate(&self) -> Result<(), String> {
        if self.schema_version != OUTCOME_BUNDLE_VERSION {
            return Err(format!(
                "unsupported run event version {} (expected {})",
                self.schema_version, OUTCOME_BUNDLE_VERSION
            ));
        }
        for (field, value) in [
            ("delegation_id", &self.delegation_id),
            ("delegator_id", &self.delegator_id),
            ("selected_agent_id", &self.selected_agent_id),
            ("executor_lease_actor", &self.executor_lease_actor),
            ("outcome", &self.outcome),
            ("work_item_id", &self.work_item_id),
            ("run_id", &self.run_id),
            ("outbox_id", &self.outbox_id),
            ("kind", &self.kind),
            ("payload_digest", &self.payload_digest),
            ("cursor_token", &self.cursor_token),
            ("carrier_digest", &self.carrier_digest),
        ] {
            require_bounded_text(field, value, MAX_OUTCOME_REF_BYTES)?;
        }
        if self.fence_token == 0 {
            return Err("fence_token must be non-zero".to_string());
        }
        validate_terminal_outcome("outcome", &self.outcome)?;
        validate_completeness_currency(self.completeness, &self.missing_refs)?;
        for (field, digest) in [
            ("payload_digest", &self.payload_digest),
            ("carrier_digest", &self.carrier_digest),
            ("capability_digest", &self.capability_digest),
            ("catalog_digest", &self.catalog_digest),
            ("policy_digest", &self.policy_digest),
            ("model_digest", &self.model_digest),
        ] {
            validate_sha256(field, digest)?;
        }
        Ok(())
    }

    /// Bind the one outbox event to the same terminal bundle that produced it.
    pub fn validate_for_bundle(&self, bundle: &CommitOutcomeBundle) -> Result<(), String> {
        self.validate()?;
        if !validation::event_identity_matches_bundle(self, bundle) {
            return Err("run event is not bound to the outcome bundle".to_string());
        }
        match self.kind.as_str() {
            "outcome" if self.outcome_ref.as_deref() == Some(bundle.outcome_ref.as_str()) => {}
            "tool_call"
                if self
                    .tool_call_ref
                    .as_ref()
                    .is_some_and(|reference| bundle.tool_call_refs.contains(reference)) => {}
            "degraded" | "reconciled"
                if self.outcome_ref.as_deref() == Some(bundle.outcome_ref.as_str()) => {}
            _ => {
                return Err("run event completion reference is not bound to the bundle".to_string())
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn complete_bundle() -> CommitOutcomeBundle {
        CommitOutcomeBundle {
            schema_version: OUTCOME_BUNDLE_VERSION,
            delegation_id: "delegation:fixture-1".into(),
            delegator_id: "agent:delegator-a".into(),
            selected_agent_id: "agent:selected-b".into(),
            executor_lease_actor: "worker:executor".into(),
            outcome: "succeeded".into(),
            work_item_id: "workitem:fixture-1".into(),
            fence_token: 42,
            run_id: "run:fixture-1".into(),
            result_ref: Some("cas:result-1".into()),
            result_digest: Some("d".repeat(64)),
            artifacts: vec![],
            trace_ref: "trace:fixture-1".into(),
            tool_call_refs: vec!["toolcall:fixture-1:0".into()],
            outcome_ref: "outcome:fixture-1".into(),
            capability_digest: "c".repeat(64),
            catalog_digest: "e".repeat(64),
            policy_digest: "f".repeat(64),
            model_digest: "a".repeat(64),
            event_sequence: 7,
            completeness: OutcomeCompleteness::Complete,
            missing_refs: vec![],
            outbox_id: "outbox:fixture-1".into(),
            langfuse_observation_refs: vec![],
        }
    }

    #[test]
    fn valid_complete_bundle_passes() {
        assert!(complete_bundle().validate().is_ok());
    }

    #[test]
    fn complete_with_missing_refs_is_rejected() {
        let mut bundle = complete_bundle();
        bundle.missing_refs.push("tool_call:1".into());
        let err = bundle.validate().unwrap_err();
        assert!(err.contains("never claim complete"));
    }

    #[test]
    fn complete_without_result_ref_is_rejected() {
        let mut bundle = complete_bundle();
        bundle.result_ref = None;
        bundle.result_digest = None;
        let err = bundle.validate().unwrap_err();
        assert!(err.contains("immutable result_ref"));
    }

    #[test]
    fn degraded_requires_missing_refs() {
        let mut bundle = complete_bundle();
        bundle.completeness = OutcomeCompleteness::Degraded;
        let err = bundle.validate().unwrap_err();
        assert!(err.contains("Degraded requires"));

        bundle.missing_refs.push("outcome_evaluation".into());
        assert!(bundle.validate().is_ok());
    }

    #[test]
    fn zero_fence_token_is_rejected() {
        let mut bundle = complete_bundle();
        bundle.fence_token = 0;
        assert!(bundle.validate().unwrap_err().contains("fence_token"));
    }

    #[test]
    fn mismatched_result_ref_and_digest_is_rejected() {
        let mut bundle = complete_bundle();
        bundle.result_digest = None;
        assert!(bundle.validate().unwrap_err().contains("both be present"));
    }

    #[test]
    fn result_and_artifact_digests_are_validated() {
        let mut bundle = complete_bundle();
        bundle.result_digest = Some("Z".repeat(64));
        assert!(bundle.validate().unwrap_err().contains("result_digest"));

        let mut bundle = complete_bundle();
        bundle.artifacts.push(OutcomeArtifactRef {
            artifact_ref: "cas:artifact:fixture".into(),
            digest: "not-a-digest".into(),
            kind: "document".into(),
            size_bytes: 1,
            classification: "public".into(),
        });
        assert!(bundle.validate().unwrap_err().contains("artifact digest"));
    }

    #[test]
    fn participant_domains_are_evidence_analytics_outbox() {
        let bundle = complete_bundle();
        assert_eq!(
            bundle.participant_domains(),
            &[
                CommitParticipantDomain::Evidence,
                CommitParticipantDomain::AnalyticsOutcome,
                CommitParticipantDomain::Outbox,
            ]
        );
    }

    fn receipt_node(
        bundle: &CommitOutcomeBundle,
        kind: ReceiptNodeKind,
        node_id: &str,
    ) -> ReceiptNode {
        let properties = serde_json::json!({
            "node_id": node_id,
            "kind": receipt_kind_name(kind),
            "delegation_id": bundle.delegation_id,
            "delegator_id": bundle.delegator_id,
            "selected_agent_id": bundle.selected_agent_id,
            "executor_lease_actor": bundle.executor_lease_actor,
            "outcome": bundle.outcome,
            "work_item_id": bundle.work_item_id,
            "run_id": bundle.run_id,
            "fence_token": bundle.fence_token,
            "result_ref": bundle.result_ref,
            "result_digest": bundle.result_digest,
            "event_sequence": bundle.event_sequence,
            "completeness": bundle.completeness,
            "missing_refs": bundle.missing_refs,
            "outbox_id": bundle.outbox_id,
            "payload_ref": format!("cas:receipt:{node_id}"),
            "capability_digest": bundle.capability_digest,
            "catalog_digest": bundle.catalog_digest,
            "policy_digest": bundle.policy_digest,
            "model_digest": bundle.model_digest,
            "payload": {"fixture": true},
        });
        let properties_msgpack = rmp_serde::to_vec_named(&properties).unwrap();
        ReceiptNode {
            node_id: node_id.into(),
            kind,
            delegation_id: bundle.delegation_id.clone(),
            work_item_id: bundle.work_item_id.clone(),
            run_id: bundle.run_id.clone(),
            fence_token: bundle.fence_token,
            result_ref: bundle.result_ref.clone(),
            outbox_id: bundle.outbox_id.clone(),
            payload_ref: format!("cas:receipt:{node_id}"),
            payload_digest: hex::encode(Sha256::digest(&properties_msgpack)),
            properties_msgpack,
        }
    }

    #[test]
    fn receipt_payload_must_carry_authority_bindings() {
        let bundle = complete_bundle();
        let mut node = receipt_node(&bundle, ReceiptNodeKind::RunTrace, &bundle.trace_ref);
        node.properties_msgpack = rmp_serde::to_vec_named(&serde_json::json!({})).unwrap();
        node.payload_digest = hex::encode(Sha256::digest(&node.properties_msgpack));
        let error = bundle
            .validate_receipt_nodes(&[
                node,
                receipt_node(
                    &bundle,
                    ReceiptNodeKind::ToolCall,
                    &bundle.tool_call_refs[0],
                ),
                receipt_node(
                    &bundle,
                    ReceiptNodeKind::OutcomeEvaluation,
                    &bundle.outcome_ref,
                ),
            ])
            .unwrap_err();
        assert!(error.contains("missing 'node_id'"), "{error}");
    }

    #[test]
    fn empty_caller_binding_is_rejected() {
        let bundle = complete_bundle();
        let mut node = receipt_node(&bundle, ReceiptNodeKind::RunTrace, &bundle.trace_ref);
        let mut properties: serde_json::Map<String, serde_json::Value> =
            rmp_serde::from_slice(&node.properties_msgpack).unwrap();
        properties.insert("caller".into(), serde_json::json!({}));
        node.properties_msgpack = rmp_serde::to_vec_named(&properties).unwrap();
        node.payload_digest = hex::encode(Sha256::digest(&node.properties_msgpack));
        let mut nodes = vec![
            node,
            receipt_node(
                &bundle,
                ReceiptNodeKind::ToolCall,
                &bundle.tool_call_refs[0],
            ),
            receipt_node(
                &bundle,
                ReceiptNodeKind::OutcomeEvaluation,
                &bundle.outcome_ref,
            ),
        ];
        let error = bundle.validate_receipt_nodes(&nodes).unwrap_err();
        assert!(error.contains("empty caller"), "{error}");
        nodes.clear();
    }

    #[test]
    fn complete_bundle_requires_bound_receipt_nodes() {
        let bundle = complete_bundle();
        assert!(bundle.validate_receipt_nodes(&[]).is_err());
        let nodes = vec![
            receipt_node(&bundle, ReceiptNodeKind::RunTrace, &bundle.trace_ref),
            receipt_node(
                &bundle,
                ReceiptNodeKind::ToolCall,
                &bundle.tool_call_refs[0],
            ),
            receipt_node(
                &bundle,
                ReceiptNodeKind::OutcomeEvaluation,
                &bundle.outcome_ref,
            ),
        ];
        assert!(bundle.validate_receipt_nodes(&nodes).is_ok());
    }

    #[test]
    fn receipt_node_fence_and_outbox_are_bound_to_bundle() {
        let bundle = complete_bundle();
        let mut node = receipt_node(&bundle, ReceiptNodeKind::RunTrace, &bundle.trace_ref);
        node.fence_token += 1;
        assert!(bundle
            .validate_receipt_nodes(&[node])
            .unwrap_err()
            .contains("not bound"));
    }

    #[test]
    fn duplicate_tool_completion_refs_are_rejected() {
        let mut bundle = complete_bundle();
        bundle.tool_call_refs.push(bundle.tool_call_refs[0].clone());
        assert!(bundle.validate().unwrap_err().contains("duplicate"));
    }

    fn valid_event() -> RunEvent {
        RunEvent {
            schema_version: OUTCOME_BUNDLE_VERSION,
            delegation_id: "delegation:fixture-1".into(),
            delegator_id: "agent:delegator-a".into(),
            selected_agent_id: "agent:selected-b".into(),
            executor_lease_actor: "worker:executor".into(),
            outcome: "succeeded".into(),
            work_item_id: "workitem:fixture-1".into(),
            run_id: "run:fixture-1".into(),
            fence_token: 42,
            outbox_id: "outbox:fixture-1".into(),
            result_ref: Some("cas:result-1".into()),
            capability_digest: "c".repeat(64),
            catalog_digest: "e".repeat(64),
            policy_digest: "f".repeat(64),
            model_digest: "a".repeat(64),
            event_sequence: 7,
            completeness: OutcomeCompleteness::Complete,
            missing_refs: vec![],
            kind: "outcome".into(),
            tool_call_ref: None,
            outcome_ref: Some("outcome:fixture-1".into()),
            payload_digest: "b".repeat(64),
            timestamp_ms: 1_000,
            cursor_token: "cursor:fixture-1:7".into(),
            carrier_digest: "1".repeat(64),
        }
    }

    #[test]
    fn valid_run_event_passes() {
        let event = valid_event();
        assert!(event.validate().is_ok());
        assert!(event.validate_for_bundle(&complete_bundle()).is_ok());
    }

    #[test]
    fn terminal_outcome_and_completeness_currency_are_visible_and_bound() {
        for outcome in ["failed", "cancelled"] {
            let mut bundle = complete_bundle();
            bundle.outcome = outcome.into();
            let mut event = valid_event();
            event.outcome = outcome.into();
            assert!(event.validate_for_bundle(&bundle).is_ok());
            let receipt = receipt_node(&bundle, ReceiptNodeKind::RunTrace, &bundle.trace_ref);
            let properties: std::collections::BTreeMap<String, serde_json::Value> =
                rmp_serde::from_slice(&receipt.properties_msgpack).unwrap();
            assert_eq!(properties["outcome"], outcome);

            let mut mismatched = event.clone();
            mismatched.outcome = if outcome == "failed" {
                "cancelled".into()
            } else {
                "failed".into()
            };
            assert!(mismatched
                .validate_for_bundle(&bundle)
                .unwrap_err()
                .contains("not bound"));
        }

        let mut bundle = complete_bundle();
        bundle.completeness = OutcomeCompleteness::Degraded;
        bundle.missing_refs = vec!["tool_call:fixture-1:1".into()];
        let mut event = valid_event();
        event.completeness = OutcomeCompleteness::Degraded;
        event.missing_refs = bundle.missing_refs.clone();
        event.kind = "degraded".into();
        assert!(event.validate_for_bundle(&bundle).is_ok());
        let receipt = receipt_node(&bundle, ReceiptNodeKind::RunTrace, &bundle.trace_ref);
        let properties: std::collections::BTreeMap<String, serde_json::Value> =
            rmp_serde::from_slice(&receipt.properties_msgpack).unwrap();
        assert_eq!(properties["completeness"], "degraded");
        assert_eq!(
            properties["missing_refs"],
            serde_json::json!(bundle.missing_refs)
        );
        event.missing_refs.clear();
        assert!(event.validate().unwrap_err().contains("missing_refs"));
    }

    #[test]
    fn terminal_extension_binds_one_run_event_to_receipts() {
        let bundle = complete_bundle();
        let receipt_nodes = vec![
            receipt_node(&bundle, ReceiptNodeKind::RunTrace, &bundle.trace_ref),
            receipt_node(
                &bundle,
                ReceiptNodeKind::ToolCall,
                &bundle.tool_call_refs[0],
            ),
            receipt_node(
                &bundle,
                ReceiptNodeKind::OutcomeEvaluation,
                &bundle.outcome_ref,
            ),
        ];
        let mut extension = TerminalOutcomeExtension {
            outcome_bundle: bundle,
            receipt_nodes,
            run_event: valid_event(),
        };
        assert!(extension.validate().is_ok());
        extension.run_event.outbox_id = "outbox:other".into();
        assert!(extension.validate().unwrap_err().contains("not bound"));
    }

    #[test]
    fn run_event_cannot_cross_work_item_or_outbox() {
        let mut event = valid_event();
        event.outbox_id = "outbox:other".into();
        assert!(event
            .validate_for_bundle(&complete_bundle())
            .unwrap_err()
            .contains("not bound"));
    }

    #[test]
    fn run_event_cannot_cross_completion_reference() {
        let mut event = valid_event();
        event.outcome_ref = Some("outcome:other".into());
        assert!(event
            .validate_for_bundle(&complete_bundle())
            .unwrap_err()
            .contains("completion reference"));
    }

    #[test]
    fn run_event_requires_cursor_token() {
        let mut event = valid_event();
        event.cursor_token = String::new();
        assert!(event.validate().unwrap_err().contains("cursor_token"));
    }
}
