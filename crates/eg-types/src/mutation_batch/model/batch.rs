use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use super::{
    MutationCapability, MutationEnvelope, MutationOperation, MutationOutboxRecord, MutationScope,
    MutationScopeIdentity,
};
use crate::contract::Digest256;

/// Reserved maintenance kind for one EH-557 replacement intent. Admission of
/// this shape still requires the private, verified Raft top-up transition.
pub const REPOSITORY_ENRICHMENT_TOP_UP_KIND: &str = "repository_enrichment_top_up";
pub const REPOSITORY_ENRICHMENT_PENDING_TOPIC: &str = "repository.enrichment.pending";

/// Inputs already checked by the private Raft top-up transition. The old row
/// and policy sequence are bound into the maintenance subject, so a retry of a
/// different revision cannot masquerade as the same replacement parent.
pub struct RepositoryEnrichmentTopUp<'a> {
    pub old_delivery: &'a MutationOutboxRecord,
    pub policy_sequence: u64,
    pub revision_idempotency_key: &'a str,
    pub replacement_intent: MutationOutboxIntent,
    pub replacement_batch_id: &'a str,
    pub serving_principal: &'a str,
    pub graph_version: u64,
    pub placement_epoch: u64,
    pub fencing_token: u64,
    pub created_at_ms: u64,
}

/// Digest/version descriptor for authenticated authoritative graph material.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct MutationStateDescriptor {
    pub algorithm: String,
    pub digest: String,
    pub source_graph_version: u64,
    pub target_graph_version: u64,
}

/// A projection/index/CDC/audit/lineage notification to publish after commit.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct MutationOutboxIntent {
    pub topic: String,
    pub key: String,
    #[cfg_attr(feature = "contract-schema", schemars(with = "Vec<u8>"))]
    #[serde(with = "serde_bytes")]
    pub payload: Vec<u8>,
    pub headers: BTreeMap<String, String>,
}

/// Explicit OCC semantics captured before validation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum VersionExpectation {
    Graph(u64),
    Native(u64),
    /// Valid only for a capability-bearing reserved-system operation.
    Unversioned,
}

/// Exact authoritative version transition committed with a durable record.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum CommittedVersion {
    Graph { source: u64, target: u64 },
    Native { source: u64, target: u64 },
    None,
}

impl CommittedVersion {
    pub fn checked_graph(source: u64) -> Result<Self, String> {
        Ok(Self::Graph {
            source,
            target: source
                .checked_add(1)
                .ok_or_else(|| "graph mutation version overflow".to_string())?,
        })
    }

    pub fn checked_native(source: u64) -> Result<Self, String> {
        Ok(Self::Native {
            source,
            target: source
                .checked_add(1)
                .ok_or_else(|| "native mutation version overflow".to_string())?,
        })
    }

    pub fn target(self) -> Option<u64> {
        match self {
            Self::Graph { target, .. } | Self::Native { target, .. } => Some(target),
            Self::None => None,
        }
    }
}

/// Universal, deterministic durable mutation unit.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct MutationBatch {
    pub schema_version: u16,
    /// Stable identity for status lookup and outbox correlation.
    pub batch_id: String,
    /// The admission header this batch is admitted under: verified caller
    /// authority for an operation, the store's own declaration for maintenance.
    pub envelope: MutationEnvelope,
    /// Exact tenant, typed logical owner, lifecycle generation, and digest.
    pub identity: MutationScopeIdentity,
    /// Catalog epoch used to resolve a placed owner.
    pub placement_epoch: u64,
    pub version_expectation: VersionExpectation,
    /// Lease/worker fencing epoch for work-driven writes.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fencing_token: Option<u64>,
    /// Present when operations are represented by a complete staged graph image.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub authoritative_state: Option<MutationStateDescriptor>,
    pub operations: Vec<MutationOperation>,
    pub outbox: Vec<MutationOutboxIntent>,
    /// Creation time supplied by the request boundary.
    pub created_at_ms: u64,
}

impl MutationBatch {
    /// Build the real ledger parent for a replacement intent. This constructor
    /// does not confer admission authority; the kernel's dedicated top-up path
    /// must verify the old delivery and revision in the same admitted write.
    pub fn repository_enrichment_top_up(
        input: RepositoryEnrichmentTopUp<'_>,
    ) -> Result<Self, String> {
        input.old_delivery.validate()?;
        if input.old_delivery.intent.topic != REPOSITORY_ENRICHMENT_PENDING_TOPIC
            || input.old_delivery.intent.key == input.replacement_intent.key
        {
            return Err("repository enrichment top-up requires a distinct pending source".into());
        }
        if input.replacement_intent.key != input.replacement_batch_id {
            return Err("repository enrichment replacement batch must name its new source".into());
        }
        let identity = input.old_delivery.identity.clone();
        if !matches!(identity.scope(), MutationScope::Graph { .. }) {
            return Err("repository enrichment replacement requires graph scope".into());
        }
        let ordinal = input.old_delivery.ordinal.to_be_bytes();
        let sequence = input.policy_sequence.to_be_bytes();
        let subject = Digest256::framed(
            b"eg/repository-enrichment-top-up-parent/v1",
            &[
                input.old_delivery.batch_id.as_bytes(),
                &ordinal,
                input.old_delivery.intent.key.as_bytes(),
                &input.old_delivery.intent.payload,
                &sequence,
            ],
        )?;
        let envelope = MutationEnvelope::maintenance(
            input.serving_principal,
            REPOSITORY_ENRICHMENT_TOP_UP_KIND,
            &format!("outbox-replacement:{subject}"),
            input.revision_idempotency_key,
        )?;
        let batch = Self {
            schema_version: crate::mutation_batch::MUTATION_BATCH_VERSION,
            batch_id: input.replacement_batch_id.to_string(),
            envelope,
            identity,
            placement_epoch: input.placement_epoch,
            version_expectation: VersionExpectation::Graph(input.graph_version),
            fencing_token: Some(input.fencing_token),
            authoritative_state: None,
            operations: Vec::new(),
            outbox: vec![input.replacement_intent],
            created_at_ms: input.created_at_ms,
        };
        batch.validate()?;
        Ok(batch)
    }

    /// Exact reserved zero-operation parent shape. This is a description, not
    /// an authority grant: ordinary commit admission must refuse it.
    pub fn is_repository_enrichment_top_up(&self) -> bool {
        let MutationEnvelope::Maintenance(envelope) = &self.envelope else {
            return false;
        };
        self.operations.is_empty()
            && self.authoritative_state.is_none()
            && matches!(self.identity.scope(), MutationScope::Graph { .. })
            && matches!(self.version_expectation, VersionExpectation::Graph(_))
            && self.placement_epoch > 0
            && self.fencing_token.is_some()
            && envelope.kind.as_str() == REPOSITORY_ENRICHMENT_TOP_UP_KIND
            && envelope
                .subject
                .as_str()
                .strip_prefix("outbox-replacement:")
                .is_some_and(|hex| {
                    hex.len() == 64
                        && hex
                            .bytes()
                            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
                })
            && self.outbox.len() == 1
            && self.outbox[0].topic == REPOSITORY_ENRICHMENT_PENDING_TOPIC
            && self.outbox[0].key == self.batch_id
            && self.outbox[0].headers.is_empty()
    }
    /// A batch that applies over its owner's native version with no placement
    /// epoch, fencing token or staged state -- the shape every native owner
    /// write shares. `content` is the batch's final operations and outbox.
    pub fn native(
        batch_id: &str,
        envelope: MutationEnvelope,
        identity: MutationScopeIdentity,
        version: u64,
        content: (Vec<MutationOperation>, Vec<MutationOutboxIntent>),
        created_at_ms: u64,
    ) -> Self {
        let (operations, outbox) = content;
        Self {
            schema_version: crate::mutation_batch::MUTATION_BATCH_VERSION,
            batch_id: batch_id.to_string(),
            envelope,
            identity,
            placement_epoch: 0,
            version_expectation: VersionExpectation::Native(version),
            fencing_token: None,
            authoritative_state: None,
            operations,
            outbox,
            created_at_ms,
        }
    }

    pub fn validate_identity(&self) -> Result<(), String> {
        self.identity.validate_digest()
    }

    /// Re-mint this batch's envelope over its CURRENT content.
    ///
    /// The compile path mints from final content by construction -- the outbox
    /// and operations are inputs to the mint, not things appended after it -- so
    /// production never needs this. A caller that assembles a batch in steps
    /// (every test fixture, and any future producer that legitimately builds its
    /// body incrementally) uses this to do exactly what the compile path does at
    /// the end: recompute the method identity and canonical payload digest FROM
    /// the batch's own operations, outbox and authoritative state.
    ///
    /// It cannot forge anything and it is not a bypass: it reads the body it is
    /// about to describe. What it prevents is the opposite defect --
    /// an envelope left covering bytes the batch no longer has, which
    /// `validate` refuses.
    ///
    /// A maintenance envelope covers no content and is left untouched.
    pub fn reseal_envelope(
        &mut self,
        method_schema_digest: crate::contract::Digest256,
    ) -> Result<(), String> {
        let content = super::BatchContent {
            operations: &self.operations,
            outbox: &self.outbox,
            authoritative_state: self.authoritative_state.as_ref(),
        };
        let resealed =
            super::CompiledOperation::for_content(&self.identity, content, method_schema_digest)?;
        let MutationEnvelope::Operation(envelope) = &mut self.envelope else {
            return Ok(());
        };
        envelope.method = resealed.method;
        envelope.method_schema_id = resealed.method_schema_id;
        envelope.method_schema_digest = resealed.method_schema_digest;
        envelope.canonical_payload_digest = resealed.canonical_payload_digest;
        Ok(())
    }

    /// The caller-stable retry key, read through the envelope.
    ///
    /// It is deliberately NOT a field: RF-ADR-001 forbids two copies of one
    /// value, and the key lives inside the authority the replay identity is
    /// derived from. A batch and its identity therefore cannot disagree about
    /// which key the ledger row is under.
    pub fn idempotency_key(&self) -> &str {
        self.envelope.idempotency_key()
    }

    /// The principal the committing ledger requires -- for every domain, the
    /// store's serving principal. The verified CALLER is the outbox `actor`
    /// header (see `MutationBatchRecord::committing_actor`), never this.
    pub fn serving_principal(&self) -> &str {
        self.envelope.serving_principal()
    }

    /// True when this batch is an owner-maintenance write (RF-RULING-005).
    pub fn is_maintenance(&self) -> bool {
        self.envelope.is_maintenance()
    }

    /// Capabilities verified at admission; `None` for a maintenance write, which
    /// has no caller to have verified anything about.
    pub fn verified_capabilities(&self) -> Option<&std::collections::BTreeSet<MutationCapability>> {
        self.envelope.verified_capabilities()
    }
}
