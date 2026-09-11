//! Promotion commit seam.
//!
//! `eg-program` can decide which candidate satisfies policy, but it cannot mutate
//! graph state. The caller must lower the selected candidate to the engine's
//! existing `ChangeEnvelope`/`MutationBatch` and submit it through the one governed
//! persistence authority.

use std::future::Future;
use std::pin::Pin;

use eg_modality::{Classification, OpaqueRef, PolicyEnvelope};
use eg_types::{ChangeEnvelope, MutationBatchCommit};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{
    AdapterKind, CandidateRole, ModuleKind, OptimizationResult, OptimizerKind, ProgramCandidate,
    ProgramError, ProgramModality, ProgramRevision, PROGRAM_SCHEMA_VERSION,
};

/// Immutable compiler inputs retained beside a promoted revision.
///
/// The graph row is keyed by `candidate_ref` and points to the durable result
/// claim row that carried the selected candidate. Keeping the full candidate
/// digest inputs here makes a restart resolver independent of the ephemeral job
/// record while retaining the exact compiler identity and opaque bindings.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProgramCandidateRecord {
    pub result_ref: OpaqueRef,
    pub result_input_dataset_ref: OpaqueRef,
    pub result_input_content_digest: String,
    pub result_input_snapshot_version: u64,
    pub candidate_claim_ref: String,
    pub program_ref: OpaqueRef,
    pub base_revision: u64,
    pub signature_ref: OpaqueRef,
    pub instruction_ref: OpaqueRef,
    pub module: ModuleKind,
    pub adapter: AdapterKind,
    pub tenant_ref: OpaqueRef,
    pub access_policy_ref: OpaqueRef,
    pub corpus_ref: OpaqueRef,
    pub corpus_snapshot_version: u64,
    pub optimizer: OptimizerKind,
    pub seed: u64,
    pub role: CandidateRole,
    pub demonstration_refs: Vec<OpaqueRef>,
    pub artifact_refs: Vec<OpaqueRef>,
    pub composition_refs: Vec<OpaqueRef>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub candidate_instruction_ref: Option<OpaqueRef>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool_policy_ref: Option<OpaqueRef>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model_profile_ref: Option<OpaqueRef>,
    pub modalities: std::collections::BTreeSet<ProgramModality>,
    pub candidate_ref: OpaqueRef,
    pub content_digest: String,
    /// Digest of the complete immutable authority record.  `content_digest`
    /// remains the compiler candidate digest addressed by `candidate_ref`; this
    /// second digest binds the result/claim lineage without a self-referential
    /// candidate-ref hash cycle.
    pub authority_digest: String,
}

impl ProgramCandidateRecord {
    pub fn from_candidate(
        program: &ProgramRevision,
        candidate: &ProgramCandidate,
        // `result_input` groups the job result's own ref, its source dataset
        // ref, the dataset's content digest, and the dataset snapshot version.
        result_input: (OpaqueRef, OpaqueRef, String, u64),
        // `corpus` groups the training corpus ref and its snapshot version.
        corpus: (OpaqueRef, u64),
        seed: u64,
    ) -> Result<Self, ProgramError> {
        let (
            result_ref,
            result_input_dataset_ref,
            result_input_content_digest,
            result_input_snapshot_version,
        ) = result_input;
        let (corpus_ref, corpus_snapshot_version) = corpus;
        let candidate_claim_ref = format!(
            "jobclaim:{}:{}",
            result_ref.as_str(),
            candidate.candidate_ref.as_str()
        );
        let mut record = Self {
            result_ref,
            result_input_dataset_ref,
            result_input_content_digest,
            result_input_snapshot_version,
            candidate_claim_ref,
            program_ref: program.program_ref.clone(),
            base_revision: program.revision,
            signature_ref: program.signature.signature_ref.clone(),
            instruction_ref: program.signature.instruction_ref.clone(),
            module: program.module,
            adapter: program.adapter,
            tenant_ref: program.policy.tenant_ref.clone(),
            access_policy_ref: program.policy.access_policy_ref.clone(),
            corpus_ref,
            corpus_snapshot_version,
            optimizer: candidate.optimizer,
            seed,
            role: candidate.role,
            demonstration_refs: candidate.demonstration_refs.clone(),
            artifact_refs: candidate.artifact_refs.clone(),
            composition_refs: candidate.composition_refs.clone(),
            candidate_instruction_ref: candidate.instruction_ref.clone(),
            tool_policy_ref: candidate.tool_policy_ref.clone(),
            model_profile_ref: candidate.model_profile_ref.clone(),
            modalities: candidate.modalities.clone(),
            candidate_ref: candidate.candidate_ref.clone(),
            content_digest: candidate.content_digest.clone(),
            authority_digest: String::new(),
        };
        record.authority_digest = record.recompute_content_digest();
        record.validate()?;
        Ok(record)
    }

    pub fn validate(&self) -> Result<(), ProgramError> {
        if self.result_ref.namespace() != "job_result"
            || self.result_input_dataset_ref.namespace() != "job_input"
            || self.result_input_content_digest.len() != 64
            || !self
                .result_input_content_digest
                .bytes()
                .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
            || !scoped_ref_matches(
                &self.result_input_dataset_ref,
                "job_input",
                &self.result_input_content_digest,
            )
            || self.result_input_snapshot_version == 0
            || self.program_ref.namespace() != "program"
            || self.signature_ref.namespace() != "signature"
            || self.instruction_ref.namespace() != "instruction"
            || self.tenant_ref.namespace() != "tenant"
            || self.access_policy_ref.namespace() != "policy"
            || self.corpus_ref.namespace() != "corpus"
            || self.candidate_ref.namespace() != "program_candidate"
            || !scoped_ref_matches(
                &self.candidate_ref,
                "program_candidate",
                &self.content_digest,
            )
            || self.demonstration_refs.is_empty()
            || self.modalities.is_empty()
            || self.content_digest.is_empty()
            || self.content_digest.len() != 64
            || !self
                .content_digest
                .bytes()
                .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
            || self.authority_digest.len() != 64
            || !self
                .authority_digest
                .bytes()
                .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
            || self
                .candidate_instruction_ref
                .as_ref()
                .is_some_and(|reference| reference.namespace() != "instruction")
            || self
                .tool_policy_ref
                .as_ref()
                .is_some_and(|reference| reference.namespace() != "tool_policy")
            || self
                .model_profile_ref
                .as_ref()
                .is_some_and(|reference| reference.namespace() != "model_profile")
            || self.candidate_claim_ref
                != format!(
                    "jobclaim:{}:{}",
                    self.result_ref.as_str(),
                    self.candidate_ref.as_str()
                )
        {
            return Err(ProgramError::InvalidCommit);
        }
        let expected = self.recompute_candidate_digest();
        if expected != self.content_digest {
            return Err(ProgramError::InvalidCommit);
        }
        if self.recompute_content_digest() != self.authority_digest {
            return Err(ProgramError::InvalidCommit);
        }
        Ok(())
    }

    fn recompute_candidate_digest(&self) -> String {
        let mut digest = Sha256::new();
        digest.update(b"eg-program.candidate.v3\0");
        digest.update(self.program_ref.as_str().as_bytes());
        digest.update(self.base_revision.to_le_bytes());
        digest.update(self.signature_ref.as_str().as_bytes());
        digest.update(self.instruction_ref.as_str().as_bytes());
        digest.update(self.module.as_str().as_bytes());
        digest.update(self.adapter.as_str().as_bytes());
        digest.update(self.tenant_ref.as_str().as_bytes());
        digest.update(self.access_policy_ref.as_str().as_bytes());
        digest.update(self.corpus_ref.as_str().as_bytes());
        digest.update(self.corpus_snapshot_version.to_le_bytes());
        digest.update(self.optimizer.as_str().as_bytes());
        digest.update(self.seed.to_le_bytes());
        digest.update(self.role.as_str().as_bytes());
        digest_ref_list(&mut digest, b"demonstrations", &self.demonstration_refs);
        digest_ref_list(&mut digest, b"artifacts", &self.artifact_refs);
        digest_ref_list(&mut digest, b"composition", &self.composition_refs);
        digest_optional_ref(
            &mut digest,
            b"instruction",
            self.candidate_instruction_ref.as_ref(),
        );
        digest_optional_ref(&mut digest, b"tool_policy", self.tool_policy_ref.as_ref());
        digest_optional_ref(
            &mut digest,
            b"model_profile",
            self.model_profile_ref.as_ref(),
        );
        digest_modalities(&mut digest, &self.modalities);
        hex::encode(digest.finalize())
    }

    /// Hash the complete durable authority record, including the result and
    /// selected-claim lineage.  `authority_digest` is kept separate from the
    /// compiler `content_digest` because `candidate_ref` is already addressed
    /// by that compiler digest.
    fn recompute_content_digest(&self) -> String {
        let mut digest = Sha256::new();
        digest.update(b"eg-program.candidate-authority.v1\0");
        frame_digest(&mut digest, self.result_ref.as_str());
        frame_digest(&mut digest, self.result_input_dataset_ref.as_str());
        frame_digest(&mut digest, self.result_input_content_digest.as_str());
        frame_digest(
            &mut digest,
            self.result_input_snapshot_version.to_be_bytes(),
        );
        frame_digest(&mut digest, self.candidate_claim_ref.as_str());
        frame_digest(&mut digest, self.program_ref.as_str());
        frame_digest(&mut digest, self.base_revision.to_be_bytes());
        frame_digest(&mut digest, self.signature_ref.as_str());
        frame_digest(&mut digest, self.instruction_ref.as_str());
        frame_digest(&mut digest, self.module.as_str());
        frame_digest(&mut digest, self.adapter.as_str());
        frame_digest(&mut digest, self.tenant_ref.as_str());
        frame_digest(&mut digest, self.access_policy_ref.as_str());
        frame_digest(&mut digest, self.corpus_ref.as_str());
        frame_digest(&mut digest, self.corpus_snapshot_version.to_be_bytes());
        frame_digest(&mut digest, self.optimizer.as_str());
        frame_digest(&mut digest, self.seed.to_be_bytes());
        frame_digest(&mut digest, self.role.as_str());
        digest_ref_list(&mut digest, b"demonstrations", &self.demonstration_refs);
        digest_ref_list(&mut digest, b"artifacts", &self.artifact_refs);
        digest_ref_list(&mut digest, b"composition", &self.composition_refs);
        digest_optional_ref(
            &mut digest,
            b"instruction",
            self.candidate_instruction_ref.as_ref(),
        );
        digest_optional_ref(&mut digest, b"tool_policy", self.tool_policy_ref.as_ref());
        digest_optional_ref(
            &mut digest,
            b"model_profile",
            self.model_profile_ref.as_ref(),
        );
        digest_modalities(&mut digest, &self.modalities);
        frame_digest(&mut digest, self.candidate_ref.as_str());
        frame_digest(&mut digest, self.content_digest.as_str());
        hex::encode(digest.finalize())
    }
}

fn scoped_ref_matches(reference: &OpaqueRef, namespace: &str, token: &str) -> bool {
    OpaqueRef::scoped(namespace, token).is_ok_and(|expected| expected == *reference)
}

pub(crate) fn digest_modalities(
    digest: &mut Sha256,
    modalities: &std::collections::BTreeSet<ProgramModality>,
) {
    digest.update((b"modalities".len() as u64).to_le_bytes());
    digest.update(b"modalities");
    digest.update((modalities.len() as u64).to_le_bytes());
    for modality in modalities {
        digest.update((modality.as_str().len() as u64).to_le_bytes());
        digest.update(modality.as_str().as_bytes());
    }
}

/// Folds a labeled, ordered list of [`OpaqueRef`]s into a running digest.
///
/// Both the label and the list length are framed ahead of the elements so
/// that an empty list under one label can never collide with an absent
/// label, and so a shorter list can never be extended into a longer one by
/// concatenation. Shared by the commit-record digest (recorded refs such as
/// demonstrations/artifacts/composition) and the optimizer's request digest,
/// which must derive the identical content-addressed value from the same
/// request shape.
pub(crate) fn digest_ref_list(digest: &mut Sha256, label: &[u8], references: &[OpaqueRef]) {
    digest.update((label.len() as u64).to_le_bytes());
    digest.update(label);
    digest.update((references.len() as u64).to_le_bytes());
    for reference in references {
        digest.update((reference.as_str().len() as u64).to_le_bytes());
        digest.update(reference.as_str().as_bytes());
    }
}

/// Folds a labeled, optional [`OpaqueRef`] into a running digest.
///
/// The presence flag is framed before the value so that "field absent" and
/// "field present with an empty string" are distinguishable, and so the
/// presence byte itself cannot be forged by any encoding of the reference.
/// Shared by the commit-record digest and the optimizer's request digest;
/// see [`digest_ref_list`] for why the two must agree byte-for-byte.
pub(crate) fn digest_optional_ref(
    digest: &mut Sha256,
    label: &[u8],
    reference: Option<&OpaqueRef>,
) {
    digest.update((label.len() as u64).to_le_bytes());
    digest.update(label);
    digest.update([u8::from(reference.is_some())]);
    if let Some(reference) = reference {
        digest.update((reference.as_str().len() as u64).to_le_bytes());
        digest.update(reference.as_str().as_bytes());
    }
}

/// Immutable identity of a native program revision produced by promotion.
///
/// The revision reference is content addressed over the logical program, base
/// revision, observed active parent, selected candidate, content digest, and
/// retained policy/model bindings. The active pointer is a separate, mutable
/// graph row; the engine's promotion coordinator changes it with a strict
/// compare-and-set after classifying replay.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProgramRevisionIdentity {
    pub schema_version: u16,
    pub program_ref: OpaqueRef,
    pub revision_ref: OpaqueRef,
    pub base_revision: u64,
    pub revision: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_ref: Option<OpaqueRef>,
    pub candidate_ref: OpaqueRef,
    pub content_digest: String,
    pub policy: PolicyEnvelope,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool_policy_ref: Option<OpaqueRef>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model_profile_ref: Option<OpaqueRef>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub candidate_record: Option<ProgramCandidateRecord>,
}

impl ProgramRevisionIdentity {
    /// Derive the next immutable revision from one validated base program and
    /// one compiler-selected candidate. `active_revision_ref` is the observed
    /// current pointer, not [`ProgramRevision::parent_ref`], which describes the
    /// input program's own lineage. No candidate body or prompt is copied.
    pub fn from_candidate(
        program: &ProgramRevision,
        candidate: &ProgramCandidate,
        active_revision_ref: Option<OpaqueRef>,
    ) -> Result<Self, ProgramError> {
        Self::from_candidate_parts(program, candidate, active_revision_ref, None)
    }

    /// Derive a promotion identity with the immutable result/candidate chain
    /// needed by the engine resolver after the analytics job is purged.
    pub fn from_candidate_with_binding(
        program: &ProgramRevision,
        candidate: &ProgramCandidate,
        active_revision_ref: Option<OpaqueRef>,
        result_ref: OpaqueRef,
        result_input_dataset_ref: OpaqueRef,
        result_input_content_digest: String,
        result_input_snapshot_version: u64,
        corpus_ref: OpaqueRef,
        corpus_snapshot_version: u64,
        seed: u64,
    ) -> Result<Self, ProgramError> {
        let record = ProgramCandidateRecord::from_candidate(
            program,
            candidate,
            (
                result_ref,
                result_input_dataset_ref,
                result_input_content_digest,
                result_input_snapshot_version,
            ),
            (corpus_ref, corpus_snapshot_version),
            seed,
        )?;
        Self::from_candidate_parts(program, candidate, active_revision_ref, Some(record))
    }

    fn from_candidate_parts(
        program: &ProgramRevision,
        candidate: &ProgramCandidate,
        active_revision_ref: Option<OpaqueRef>,
        candidate_record: Option<ProgramCandidateRecord>,
    ) -> Result<Self, ProgramError> {
        program.validate()?;
        if candidate.program_ref != program.program_ref
            || candidate.policy != program.policy
            || (program.revision > 1 && active_revision_ref.is_none())
            || candidate.content_digest.is_empty()
            || candidate.content_digest.len() > 128
            || !candidate
                .content_digest
                .bytes()
                .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
        {
            return Err(ProgramError::InvalidCommit);
        }
        let revision = program
            .revision
            .checked_add(1)
            .ok_or(ProgramError::InvalidCommit)?;
        let token = revision_token(
            (
                program.program_ref.as_str(),
                program.revision,
                active_revision_ref.as_ref(),
            ),
            (candidate.candidate_ref.as_str(), &candidate.content_digest),
            &candidate.policy,
            (
                candidate.tool_policy_ref.as_ref(),
                candidate.model_profile_ref.as_ref(),
            ),
            candidate_record.as_ref(),
        );
        let revision_ref = OpaqueRef::scoped("program_revision", &token)
            .map_err(|_| ProgramError::InvalidCommit)?;
        let identity = Self {
            schema_version: PROGRAM_SCHEMA_VERSION,
            program_ref: program.program_ref.clone(),
            revision_ref,
            base_revision: program.revision,
            revision,
            parent_ref: active_revision_ref,
            candidate_ref: candidate.candidate_ref.clone(),
            content_digest: candidate.content_digest.clone(),
            policy: candidate.policy.clone(),
            tool_policy_ref: candidate.tool_policy_ref.clone(),
            model_profile_ref: candidate.model_profile_ref.clone(),
            candidate_record,
        };
        identity.validate()?;
        Ok(identity)
    }

    pub fn validate(&self) -> Result<(), ProgramError> {
        if self.schema_version != PROGRAM_SCHEMA_VERSION
            || self.base_revision == 0
            || self.revision != self.base_revision.checked_add(1).unwrap_or(0)
            || self.program_ref.namespace() != "program"
            || self.revision_ref.namespace() != "program_revision"
            || self.content_digest.is_empty()
            || self.content_digest.len() > 128
            || self.policy.tenant_ref.namespace() != "tenant"
            || self.policy.access_policy_ref.namespace() != "policy"
            || self.policy.retention_policy_ref.namespace() != "retention"
            || self.policy.deletion_policy_ref.namespace() != "deletion"
            || self.policy.purpose_refs.len() > crate::MAX_PURPOSE_REFS
            || self
                .policy
                .purpose_refs
                .iter()
                .any(|reference| reference.namespace() != "purpose")
            || self
                .tool_policy_ref
                .as_ref()
                .is_some_and(|reference| reference.namespace() != "tool_policy")
            || self
                .model_profile_ref
                .as_ref()
                .is_some_and(|reference| reference.namespace() != "model_profile")
            || !self
                .content_digest
                .bytes()
                .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
        {
            return Err(ProgramError::InvalidCommit);
        }
        if self.parent_ref.as_ref() == Some(&self.program_ref)
            || self
                .parent_ref
                .as_ref()
                .is_some_and(|parent| parent.namespace() != "program_revision")
            || self.candidate_ref == self.program_ref
            || self.revision_ref == self.program_ref
            || self.candidate_ref.namespace() != "program_candidate"
            || !scoped_ref_matches(
                &self.candidate_ref,
                "program_candidate",
                &self.content_digest,
            )
        {
            return Err(ProgramError::InvalidCommit);
        }
        let expected_revision_ref = OpaqueRef::scoped(
            "program_revision",
            &revision_token(
                (
                    self.program_ref.as_str(),
                    self.base_revision,
                    self.parent_ref.as_ref(),
                ),
                (self.candidate_ref.as_str(), &self.content_digest),
                &self.policy,
                (
                    self.tool_policy_ref.as_ref(),
                    self.model_profile_ref.as_ref(),
                ),
                self.candidate_record.as_ref(),
            ),
        )
        .map_err(|_| ProgramError::InvalidCommit)?;
        if self.revision_ref != expected_revision_ref {
            return Err(ProgramError::InvalidCommit);
        }
        if let Some(record) = &self.candidate_record {
            record.validate()?;
            if record.program_ref != self.program_ref
                || record.base_revision != self.base_revision
                || record.candidate_ref != self.candidate_ref
                || record.content_digest != self.content_digest
                || record.tenant_ref != self.policy.tenant_ref
                || record.access_policy_ref != self.policy.access_policy_ref
                || record.tool_policy_ref != self.tool_policy_ref
                || record.model_profile_ref != self.model_profile_ref
            {
                return Err(ProgramError::InvalidCommit);
            }
        }
        Ok(())
    }

    /// Decode and validate one durable `ProgramRevision` row. The row may carry
    /// its graph-only `type` discriminator; all identity fields remain required
    /// and the content-addressed revision reference is checked before use.
    pub fn from_durable_properties(properties: &serde_json::Value) -> Result<Self, ProgramError> {
        let mut object = properties
            .as_object()
            .cloned()
            .ok_or(ProgramError::InvalidCommit)?;
        match object.remove("type") {
            Some(serde_json::Value::String(value)) if value == "ProgramRevision" => {}
            _ => return Err(ProgramError::InvalidCommit),
        }
        let identity: Self = serde_json::from_value(serde_json::Value::Object(object))
            .map_err(|_| ProgramError::InvalidCommit)?;
        identity.validate()?;
        Ok(identity)
    }

    /// Stable row id for the one mutable pointer associated with a program.
    pub fn active_pointer_ref(&self) -> OpaqueRef {
        Self::active_pointer_ref_for(&self.program_ref)
    }

    /// Stable row id for the one mutable pointer associated with a program.
    pub fn active_pointer_ref_for(program_ref: &OpaqueRef) -> OpaqueRef {
        let mut digest = Sha256::new();
        digest.update(b"eg-program.active-pointer.v1\0");
        frame_digest(&mut digest, program_ref.as_str());
        OpaqueRef::scoped("program_active", &hex::encode(digest.finalize()))
            .expect("program active pointer digest is a valid opaque reference")
    }

    /// Validate the selected candidate claim row linked by the durable record.
    pub fn validate_candidate_claim(
        &self,
        claim_properties: &serde_json::Value,
    ) -> Result<(), ProgramError> {
        self.validate()?;
        let record = self
            .candidate_record
            .as_ref()
            .ok_or(ProgramError::InvalidCommit)?;
        let object = claim_properties
            .as_object()
            .ok_or(ProgramError::InvalidCommit)?;
        if object.get("type").and_then(serde_json::Value::as_str) != Some("Claim")
            || object.get("result_ref").and_then(serde_json::Value::as_str)
                != Some(record.result_ref.as_str())
            || object.get("about").and_then(serde_json::Value::as_str)
                != Some(record.candidate_ref.as_str())
        {
            return Err(ProgramError::InvalidCommit);
        }
        let knowledge = object
            .get("knowledge")
            .and_then(serde_json::Value::as_object)
            .ok_or(ProgramError::InvalidCommit)?;
        let expected_policy =
            serde_json::to_value(&self.policy).map_err(|_| ProgramError::InvalidCommit)?;
        let expected_modalities =
            serde_json::to_value(&record.modalities).map_err(|_| ProgramError::InvalidCommit)?;
        let expected_identity =
            serde_json::to_value(self).map_err(|_| ProgramError::InvalidCommit)?;
        if knowledge.get("id").and_then(serde_json::Value::as_str)
            != Some(record.candidate_ref.as_str())
            || knowledge.get("kind").and_then(serde_json::Value::as_str)
                != Some("program_candidate")
            || object.get("family").and_then(serde_json::Value::as_str)
                != Some("program.optimization")
            || knowledge
                .get("program_ref")
                .and_then(serde_json::Value::as_str)
                != Some(record.program_ref.as_str())
            || knowledge
                .get("optimizer")
                .and_then(serde_json::Value::as_str)
                != Some(record.optimizer.as_str())
            || knowledge
                .get("execution")
                .and_then(serde_json::Value::as_str)
                != Some(record.optimizer.execution().as_str())
            || knowledge
                .get("candidate_role")
                .and_then(serde_json::Value::as_str)
                != Some(record.role.as_str())
            || knowledge.get("policy") != Some(&expected_policy)
            || !json_ref_list_matches(
                knowledge.get("demonstration_refs"),
                &record.demonstration_refs,
            )
            || !json_ref_list_matches(knowledge.get("artifact_refs"), &record.artifact_refs)
            || !json_ref_list_matches(knowledge.get("composition_refs"), &record.composition_refs)
            || !json_optional_ref_matches(
                knowledge.get("instruction_ref"),
                record.candidate_instruction_ref.as_ref(),
            )
            || !json_optional_ref_matches(
                knowledge.get("tool_policy_ref"),
                record.tool_policy_ref.as_ref(),
            )
            || !json_optional_ref_matches(
                knowledge.get("model_profile_ref"),
                record.model_profile_ref.as_ref(),
            )
            || knowledge.get("modalities") != Some(&expected_modalities)
            || knowledge.get("selected") != Some(&serde_json::Value::Bool(true))
            || knowledge.get("promotion_identity") != Some(&expected_identity)
        {
            return Err(ProgramError::InvalidCommit);
        }
        Ok(())
    }
}

fn json_ref_list_matches(value: Option<&serde_json::Value>, expected: &[OpaqueRef]) -> bool {
    value
        .and_then(serde_json::Value::as_array)
        .is_some_and(|values| {
            values.len() == expected.len()
                && values
                    .iter()
                    .zip(expected)
                    .all(|(value, reference)| value.as_str() == Some(reference.as_str()))
        })
}

fn json_optional_ref_matches(
    value: Option<&serde_json::Value>,
    expected: Option<&OpaqueRef>,
) -> bool {
    match (value, expected) {
        (Some(value), None) => value.is_null(),
        (Some(value), Some(reference)) => value.as_str() == Some(reference.as_str()),
        _ => false,
    }
}

fn frame_digest(digest: &mut Sha256, value: impl AsRef<[u8]>) {
    let value = value.as_ref();
    digest.update((value.len() as u64).to_be_bytes());
    digest.update(value);
}

fn revision_token(
    // `program` groups the program ref, its base revision, and the observed
    // active parent; `candidate` groups the selected candidate ref and its
    // content digest; `bindings` groups the tool-policy and model-profile refs.
    program: (&str, u64, Option<&OpaqueRef>),
    candidate: (&str, &str),
    policy: &PolicyEnvelope,
    bindings: (Option<&OpaqueRef>, Option<&OpaqueRef>),
    candidate_record: Option<&ProgramCandidateRecord>,
) -> String {
    let (program_ref, base_revision, parent_ref) = program;
    let (candidate_ref, content_digest) = candidate;
    let (tool_policy_ref, model_profile_ref) = bindings;
    let mut digest = Sha256::new();
    digest.update(b"eg-program.revision.v3\0");
    frame_digest(&mut digest, program_ref);
    frame_digest(&mut digest, base_revision.to_be_bytes());
    digest.update([u8::from(parent_ref.is_some())]);
    if let Some(parent) = parent_ref {
        frame_digest(&mut digest, parent.as_str());
    }
    frame_digest(&mut digest, candidate_ref);
    frame_digest(&mut digest, content_digest);
    frame_digest(&mut digest, policy.tenant_ref.as_str());
    frame_digest(&mut digest, policy.access_policy_ref.as_str());
    frame_digest(&mut digest, classification_token(policy.classification));
    frame_digest(&mut digest, policy.retention_policy_ref.as_str());
    frame_digest(&mut digest, policy.deletion_policy_ref.as_str());
    digest.update([u8::from(policy.legal_hold_ref.is_some())]);
    if let Some(legal_hold_ref) = &policy.legal_hold_ref {
        frame_digest(&mut digest, legal_hold_ref.as_str());
    }
    frame_digest(
        &mut digest,
        (policy.purpose_refs.len() as u64).to_be_bytes(),
    );
    for purpose_ref in &policy.purpose_refs {
        frame_digest(&mut digest, purpose_ref.as_str());
    }
    digest.update([u8::from(tool_policy_ref.is_some())]);
    if let Some(tool_policy_ref) = tool_policy_ref {
        frame_digest(&mut digest, tool_policy_ref.as_str());
    }
    digest.update([u8::from(model_profile_ref.is_some())]);
    if let Some(model_profile_ref) = model_profile_ref {
        frame_digest(&mut digest, model_profile_ref.as_str());
    }
    digest.update([u8::from(candidate_record.is_some())]);
    if let Some(candidate_record) = candidate_record {
        frame_digest(
            &mut digest,
            candidate_record.result_input_dataset_ref.as_str(),
        );
        frame_digest(&mut digest, candidate_record.result_ref.as_str());
        frame_digest(&mut digest, candidate_record.candidate_claim_ref.as_str());
        frame_digest(
            &mut digest,
            candidate_record.result_input_content_digest.as_str(),
        );
        frame_digest(
            &mut digest,
            candidate_record.result_input_snapshot_version.to_be_bytes(),
        );
        frame_digest(&mut digest, candidate_record.content_digest.as_str());
        frame_digest(&mut digest, candidate_record.authority_digest.as_str());
    }
    hex::encode(digest.finalize())
}

fn classification_token(classification: Classification) -> &'static [u8] {
    match classification {
        Classification::Public => b"public",
        Classification::Internal => b"internal",
        Classification::Confidential => b"confidential",
        Classification::Restricted => b"restricted",
    }
}

#[derive(Clone, Debug)]
pub struct PromotionCommit {
    pub result: OptimizationResult,
    pub envelope: ChangeEnvelope,
    pub revision: ProgramRevisionIdentity,
}

impl PromotionCommit {
    pub fn validate(&self) -> Result<(), ProgramError> {
        let candidate = self
            .result
            .selected_candidate()
            .filter(|_| self.result.promoted)
            .ok_or(ProgramError::InvalidCommit)?;
        self.envelope
            .validate()
            .map_err(|_| ProgramError::InvalidCommit)?;
        if self.envelope.content_version.object_id != candidate.candidate_ref.as_str()
            || self.envelope.content_version.digest != candidate.content_digest
        {
            return Err(ProgramError::InvalidCommit);
        }
        self.revision.validate()?;
        if self.revision.program_ref != candidate.program_ref
            || self.revision.candidate_ref != candidate.candidate_ref
            || self.revision.content_digest != candidate.content_digest
            || self.revision.policy != candidate.policy
            || self.revision.tool_policy_ref != candidate.tool_policy_ref
            || self.revision.model_profile_ref != candidate.model_profile_ref
        {
            return Err(ProgramError::InvalidCommit);
        }
        Ok(())
    }
}

/// Adapter implemented by the engine's existing mutation coordinator. There is no
/// direct storage or graph-write API in this crate.
pub trait GovernedPromotionSink: Send + Sync {
    fn commit<'a>(
        &'a self,
        promotion: PromotionCommit,
    ) -> Pin<Box<dyn Future<Output = Result<MutationBatchCommit, ProgramError>> + Send + 'a>>;
}
