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
//! **Status: type contract only, not yet wired into dispatch/commit.** Folding
//! a WorkItem's status CAS and its RunTrace/ToolCall/OutcomeEvaluation node
//! writes into ONE redb transaction is a `src/server/mutation_batch.rs`
//! change, and that file is a currently-unordered five-lane collision
//! (GOC-03/04/19/20/35 — `plans/graph-os-completion-program/file-ownership.yaml`
//! `FO-001`) with no established edit sequencing. Landing this type here
//! (never touching `mutation_batch.rs`) lets the AU-side interim contract
//! (`agent_dispatch_worker.py`'s `"written"`/`"degraded"`/`"failed"`/
//! `"unavailable"` signals) target a stable shape once that native fusion is
//! unblocked, without inventing a second, incompatible contract later.
//!
//! `CommitOutcomeBundle` deliberately does not embed a full
//! [`crate::commit_descriptor::CommitDescriptorV1`]: GOC-03's descriptor is
//! the cross-domain commit IDENTITY a participant registers a digest against
//! once native wiring lands, not a payload this module should duplicate.
//! `commit_participant_domains` instead names which [`CommitParticipantDomain`]
//! entries an applied bundle would register — `Evidence` (RunTrace/ToolCall),
//! `AnalyticsOutcome` (OutcomeEvaluation), and `Outbox` (the one durable
//! `RunEvent`) — so the eventual native applier has an unambiguous mapping
//! from this bundle to GOC-03's participant registry.

use serde::{Deserialize, Serialize};

use crate::commit_descriptor::CommitParticipantDomain;

/// Current wire/on-disk schema version for [`CommitOutcomeBundle`] and
/// [`RunEvent`]. Mirrors [`crate::commit_descriptor::COMMIT_DESCRIPTOR_VERSION`]'s
/// fail-closed-on-mismatch convention (see this crate's "No Legacy" edict —
/// no compatibility shim for an unsupported version).
pub const OUTCOME_BUNDLE_VERSION: u16 = 1;

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
/// [`crate::commit_descriptor::CommitDescriptorV1`]'s "digests cover canonical
/// encoded participant bytes, never mutable pointers or raw content" rule.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
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
pub struct CommitOutcomeBundle {
    pub schema_version: u16,
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
        for (field, value) in [
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
            if value.trim().is_empty() {
                return Err(format!("{field} must not be empty"));
            }
        }
        if self.fence_token == 0 {
            return Err("fence_token must be non-zero".to_string());
        }
        if self.result_ref.is_some() != self.result_digest.is_some() {
            return Err(
                "result_ref and result_digest must both be present or both absent".to_string(),
            );
        }
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
}

/// One durable projection event a [`CommitOutcomeBundle`]'s commit (or a
/// repair pass) publishes. Streaming/AG-UI/SSE/WebSocket/terminal/MCP
/// surfaces are projections of a sequence of these, never a second event
/// authority (lane doc "Cursor/streaming semantics").
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunEvent {
    pub schema_version: u16,
    pub run_id: String,
    /// Same numeric authority as [`CommitOutcomeBundle::event_sequence`] —
    /// never a timestamp or opaque string (the decision record's explicit
    /// rule for every GOC-21/22/23/25/29/31 consumer).
    pub event_sequence: u64,
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
            ("run_id", &self.run_id),
            ("kind", &self.kind),
            ("payload_digest", &self.payload_digest),
            ("cursor_token", &self.cursor_token),
            ("carrier_digest", &self.carrier_digest),
        ] {
            if value.trim().is_empty() {
                return Err(format!("{field} must not be empty"));
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

    fn valid_event() -> RunEvent {
        RunEvent {
            schema_version: OUTCOME_BUNDLE_VERSION,
            run_id: "run:fixture-1".into(),
            event_sequence: 7,
            kind: "outcome".into(),
            tool_call_ref: None,
            outcome_ref: Some("outcome:fixture-1".into()),
            payload_digest: "b".repeat(64),
            timestamp_ms: 1_000,
            cursor_token: "cursor:fixture-1:7".into(),
            carrier_digest: "carrier:sha256:fixture".into(),
        }
    }

    #[test]
    fn valid_run_event_passes() {
        assert!(valid_event().validate().is_ok());
    }

    #[test]
    fn run_event_requires_cursor_token() {
        let mut event = valid_event();
        event.cursor_token = String::new();
        assert!(event.validate().unwrap_err().contains("cursor_token"));
    }
}
