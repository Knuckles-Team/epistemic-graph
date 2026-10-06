//! The client-authored half of a [`ChangeEnvelope`].
//!
//! A [`MutationBatch`] carries server authority: the scope identity, the
//! version expectation and the admission envelope are minted by the request
//! boundary from the verified request context, never supplied by a caller. A
//! client therefore submits a [`ChangeEnvelopeDraft`] -- every envelope row plus
//! the operations, outbox and fences it intends -- and the engine compiles the
//! draft into the full envelope before it reaches the governed commit path.

use serde::{Deserialize, Serialize};

use super::{
    BlobReference, ChangeCursor, ChangeEnvelope, ContentVersion, EvidenceRecord, FeatureRecord,
    LineageRecord, MaterialClass, PolicyRecord, PrivacyAttestation,
};
use crate::mutation_batch::{MutationBatch, MutationOperation, MutationOutboxIntent};

/// What a caller may author of an envelope's mutation.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ChangeMutationDraft {
    /// Stable batch identity for status lookup and outbox correlation.
    pub batch_id: String,
    #[serde(default)]
    pub placement_epoch: u64,
    /// The graph version this change was prepared against (optimistic
    /// compare-and-set). `None` binds the authoritative version the engine
    /// reads when it compiles the draft, so a concurrent write still refuses.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_graph_version: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fencing_token: Option<u64>,
    /// Ordered operations; `ordinal` must equal the position. The engine
    /// re-derives each operation's surface and domain and refuses a mismatch.
    pub operations: Vec<MutationOperation>,
    #[serde(default)]
    pub outbox: Vec<MutationOutboxIntent>,
}

/// A [`ChangeEnvelope`] whose mutation is still a caller draft.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ChangeEnvelopeDraft {
    pub schema_version: u16,
    pub envelope_id: String,
    pub mutation: ChangeMutationDraft,
    pub content_version: ContentVersion,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor: Option<ChangeCursor>,
    #[serde(default)]
    pub blobs: Vec<BlobReference>,
    #[serde(default)]
    pub features: Vec<FeatureRecord>,
    #[serde(default)]
    pub evidence: Vec<EvidenceRecord>,
    #[serde(default)]
    pub policies: Vec<PolicyRecord>,
    #[serde(default)]
    pub lineage: Vec<LineageRecord>,
    pub privacy: PrivacyAttestation,
    #[serde(default, skip_serializing_if = "MaterialClass::is_attested")]
    pub material_class: MaterialClass,
}

impl ChangeMutationDraft {
    /// The draft's operations in order, after checking every ordinal is its
    /// position: a reordered or sparse list is a malformed draft, not a hint.
    pub fn ordered_operations(&self) -> Result<&[MutationOperation], String> {
        if self.batch_id.trim().is_empty() {
            return Err("change mutation draft batch_id must not be empty".into());
        }
        if self.operations.is_empty() {
            return Err("change mutation draft has no operations".into());
        }
        let misplaced = self
            .operations
            .iter()
            .enumerate()
            .any(|(index, operation)| operation.ordinal as usize != index);
        if misplaced {
            return Err("change mutation draft ordinals must equal their positions".into());
        }
        Ok(&self.operations)
    }

    /// Require the compiled batch to classify every operation exactly as the
    /// draft declared it.
    pub fn check_compiled(&self, compiled: &MutationBatch) -> Result<(), String> {
        let same = compiled.operations.len() == self.operations.len()
            && compiled
                .operations
                .iter()
                .zip(&self.operations)
                .all(|(compiled, drafted)| {
                    compiled.ordinal == drafted.ordinal
                        && compiled.surface == drafted.surface
                        && compiled.domain == drafted.domain
                });
        if !same {
            return Err("change mutation draft classification differs from the engine's".into());
        }
        Ok(())
    }
}

impl ChangeEnvelopeDraft {
    /// The governed envelope this draft becomes under `mutation`, the batch the
    /// request boundary compiled from this draft and the verified context.
    pub fn into_envelope(self, mutation: MutationBatch) -> ChangeEnvelope {
        ChangeEnvelope {
            schema_version: self.schema_version,
            envelope_id: self.envelope_id,
            mutation,
            content_version: self.content_version,
            cursor: self.cursor,
            blobs: self.blobs,
            features: self.features,
            evidence: self.evidence,
            policies: self.policies,
            lineage: self.lineage,
            privacy: self.privacy,
            material_class: self.material_class,
            commit_seq: None,
            commit_descriptor_ref: None,
        }
    }
}
