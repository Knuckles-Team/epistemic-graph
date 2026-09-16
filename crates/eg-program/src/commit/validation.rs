//! Fail-closed validation of retained compiler authority and selected claims.
//!
//! Each domain is checked in the same order as the durable contract. Hash
//! recomputation remains in the parent commit seam, before any mutation authority.

use super::{revision_token, scoped_ref_matches, ProgramCandidateRecord, ProgramRevisionIdentity};
use crate::{ProgramError, PROGRAM_SCHEMA_VERSION};
use eg_modality::OpaqueRef;
use serde_json::{Map, Value};

type ClaimObject = Map<String, Value>;

impl ProgramCandidateRecord {
    pub fn validate(&self) -> Result<(), ProgramError> {
        if !self.has_valid_result_input()
            || !self.has_valid_compiler_bindings()
            || !self.has_valid_payload_shape()
            || !self.has_valid_optional_bindings()
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

    fn has_valid_result_input(&self) -> bool {
        self.result_ref.namespace() == "job_result"
            && self.result_input_dataset_ref.namespace() == "job_input"
            && is_candidate_digest(&self.result_input_content_digest)
            && scoped_ref_matches(
                &self.result_input_dataset_ref,
                "job_input",
                &self.result_input_content_digest,
            )
            && self.result_input_snapshot_version != 0
    }

    fn has_valid_compiler_bindings(&self) -> bool {
        self.program_ref.namespace() == "program"
            && self.signature_ref.namespace() == "signature"
            && self.instruction_ref.namespace() == "instruction"
            && self.tenant_ref.namespace() == "tenant"
            && self.access_policy_ref.namespace() == "policy"
            && self.corpus_ref.namespace() == "corpus"
            && self.candidate_ref.namespace() == "program_candidate"
            && scoped_ref_matches(
                &self.candidate_ref,
                "program_candidate",
                &self.content_digest,
            )
    }

    fn has_valid_payload_shape(&self) -> bool {
        !self.demonstration_refs.is_empty()
            && !self.modalities.is_empty()
            && !self.content_digest.is_empty()
            && is_candidate_digest(&self.content_digest)
            && is_candidate_digest(&self.authority_digest)
    }

    fn has_valid_optional_bindings(&self) -> bool {
        self.candidate_instruction_ref
            .as_ref()
            .is_none_or(|r| r.namespace() == "instruction")
            && self
                .tool_policy_ref
                .as_ref()
                .is_none_or(|r| r.namespace() == "tool_policy")
            && self
                .model_profile_ref
                .as_ref()
                .is_none_or(|r| r.namespace() == "model_profile")
    }
}

fn has_lowercase_hex_digits(value: &str) -> bool {
    value
        .bytes()
        .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
}

fn is_candidate_digest(value: &str) -> bool {
    value.len() == 64 && has_lowercase_hex_digits(value)
}

impl ProgramRevisionIdentity {
    pub fn validate(&self) -> Result<(), ProgramError> {
        validate_revision_fields(self)?;
        validate_revision_lineage(self)?;
        validate_revision_address(self)?;
        validate_candidate_record(self)?;
        Ok(())
    }

    /// Validate the selected candidate claim row linked by the durable record.
    pub fn validate_candidate_claim(&self, claim_properties: &Value) -> Result<(), ProgramError> {
        self.validate()?;
        let record = self
            .candidate_record
            .as_ref()
            .ok_or(ProgramError::InvalidCommit)?;
        let object = claim_properties
            .as_object()
            .ok_or(ProgramError::InvalidCommit)?;
        validate_claim_header(record, object)?;
        let knowledge = object
            .get("knowledge")
            .and_then(Value::as_object)
            .ok_or(ProgramError::InvalidCommit)?;
        validate_claim_knowledge(self, record, object, knowledge)
    }
}

fn validate_revision_fields(identity: &ProgramRevisionIdentity) -> Result<(), ProgramError> {
    if !has_valid_revision_coordinates(identity)
        || !has_valid_revision_policy(identity)
        || !has_valid_revision_model_bindings(identity)
        || !has_lowercase_hex_digits(&identity.content_digest)
    {
        return Err(ProgramError::InvalidCommit);
    }
    Ok(())
}

fn has_valid_revision_coordinates(identity: &ProgramRevisionIdentity) -> bool {
    identity.schema_version == PROGRAM_SCHEMA_VERSION
        && identity.base_revision != 0
        && identity.revision == identity.base_revision.checked_add(1).unwrap_or(0)
        && identity.program_ref.namespace() == "program"
        && identity.revision_ref.namespace() == "program_revision"
        && !identity.content_digest.is_empty()
        && identity.content_digest.len() <= 128
}

fn has_valid_revision_policy(identity: &ProgramRevisionIdentity) -> bool {
    identity.policy.tenant_ref.namespace() == "tenant"
        && identity.policy.access_policy_ref.namespace() == "policy"
        && identity.policy.retention_policy_ref.namespace() == "retention"
        && identity.policy.deletion_policy_ref.namespace() == "deletion"
        && identity.policy.purpose_refs.len() <= crate::MAX_PURPOSE_REFS
        && !identity
            .policy
            .purpose_refs
            .iter()
            .any(|r| r.namespace() != "purpose")
}

fn has_valid_revision_model_bindings(identity: &ProgramRevisionIdentity) -> bool {
    identity
        .tool_policy_ref
        .as_ref()
        .is_none_or(|r| r.namespace() == "tool_policy")
        && identity
            .model_profile_ref
            .as_ref()
            .is_none_or(|r| r.namespace() == "model_profile")
}

fn validate_revision_lineage(identity: &ProgramRevisionIdentity) -> Result<(), ProgramError> {
    if identity.parent_ref.as_ref() == Some(&identity.program_ref)
        || identity
            .parent_ref
            .as_ref()
            .is_some_and(|parent| parent.namespace() != "program_revision")
        || identity.candidate_ref == identity.program_ref
        || identity.revision_ref == identity.program_ref
        || identity.candidate_ref.namespace() != "program_candidate"
        || !scoped_ref_matches(
            &identity.candidate_ref,
            "program_candidate",
            &identity.content_digest,
        )
    {
        return Err(ProgramError::InvalidCommit);
    }
    Ok(())
}

fn validate_revision_address(identity: &ProgramRevisionIdentity) -> Result<(), ProgramError> {
    let expected_revision_ref = OpaqueRef::scoped(
        "program_revision",
        &revision_token(
            (
                identity.program_ref.as_str(),
                identity.base_revision,
                identity.parent_ref.as_ref(),
            ),
            (identity.candidate_ref.as_str(), &identity.content_digest),
            &identity.policy,
            (
                identity.tool_policy_ref.as_ref(),
                identity.model_profile_ref.as_ref(),
            ),
            identity.candidate_record.as_ref(),
        ),
    )
    .map_err(|_| ProgramError::InvalidCommit)?;
    if identity.revision_ref != expected_revision_ref {
        return Err(ProgramError::InvalidCommit);
    }
    Ok(())
}

fn validate_candidate_record(identity: &ProgramRevisionIdentity) -> Result<(), ProgramError> {
    if let Some(record) = &identity.candidate_record {
        record.validate()?;
        if !candidate_record_matches_revision(record, identity) {
            return Err(ProgramError::InvalidCommit);
        }
    }
    Ok(())
}

fn candidate_record_matches_revision(
    record: &ProgramCandidateRecord,
    identity: &ProgramRevisionIdentity,
) -> bool {
    record.program_ref == identity.program_ref
        && record.base_revision == identity.base_revision
        && record.candidate_ref == identity.candidate_ref
        && record.content_digest == identity.content_digest
        && record.tenant_ref == identity.policy.tenant_ref
        && record.access_policy_ref == identity.policy.access_policy_ref
        && record.tool_policy_ref == identity.tool_policy_ref
        && record.model_profile_ref == identity.model_profile_ref
}

fn validate_claim_header(
    record: &ProgramCandidateRecord,
    object: &ClaimObject,
) -> Result<(), ProgramError> {
    if object.get("type").and_then(Value::as_str) != Some("Claim")
        || object.get("result_ref").and_then(Value::as_str) != Some(record.result_ref.as_str())
        || object.get("about").and_then(Value::as_str) != Some(record.candidate_ref.as_str())
    {
        return Err(ProgramError::InvalidCommit);
    }
    Ok(())
}

fn validate_claim_knowledge(
    identity: &ProgramRevisionIdentity,
    record: &ProgramCandidateRecord,
    object: &ClaimObject,
    knowledge: &ClaimObject,
) -> Result<(), ProgramError> {
    let expected_policy =
        serde_json::to_value(&identity.policy).map_err(|_| ProgramError::InvalidCommit)?;
    let expected_modalities =
        serde_json::to_value(&record.modalities).map_err(|_| ProgramError::InvalidCommit)?;
    let expected_identity =
        serde_json::to_value(identity).map_err(|_| ProgramError::InvalidCommit)?;
    if !claim_identity_matches(record, object, knowledge) {
        return Err(ProgramError::InvalidCommit);
    }
    validate_claim_materials(
        record,
        knowledge,
        &expected_policy,
        &expected_modalities,
        &expected_identity,
    )
}

fn claim_identity_matches(
    record: &ProgramCandidateRecord,
    object: &ClaimObject,
    knowledge: &ClaimObject,
) -> bool {
    knowledge.get("id").and_then(Value::as_str) == Some(record.candidate_ref.as_str())
        && knowledge.get("kind").and_then(Value::as_str) == Some("program_candidate")
        && object.get("family").and_then(Value::as_str) == Some("program.optimization")
        && knowledge.get("program_ref").and_then(Value::as_str) == Some(record.program_ref.as_str())
        && knowledge.get("optimizer").and_then(Value::as_str) == Some(record.optimizer.as_str())
        && knowledge.get("execution").and_then(Value::as_str)
            == Some(record.optimizer.execution().as_str())
        && knowledge.get("candidate_role").and_then(Value::as_str) == Some(record.role.as_str())
}

fn validate_claim_materials(
    record: &ProgramCandidateRecord,
    knowledge: &ClaimObject,
    expected_policy: &Value,
    expected_modalities: &Value,
    expected_identity: &Value,
) -> Result<(), ProgramError> {
    if knowledge.get("policy") != Some(expected_policy)
        || !claim_reference_bindings_match(record, knowledge)
        || knowledge.get("modalities") != Some(expected_modalities)
        || knowledge.get("selected") != Some(&Value::Bool(true))
        || knowledge.get("promotion_identity") != Some(expected_identity)
    {
        return Err(ProgramError::InvalidCommit);
    }
    Ok(())
}

fn claim_reference_bindings_match(
    record: &ProgramCandidateRecord,
    knowledge: &ClaimObject,
) -> bool {
    json_ref_list_matches(
        knowledge.get("demonstration_refs"),
        &record.demonstration_refs,
    ) && json_ref_list_matches(knowledge.get("artifact_refs"), &record.artifact_refs)
        && json_ref_list_matches(knowledge.get("composition_refs"), &record.composition_refs)
        && json_optional_ref_matches(
            knowledge.get("instruction_ref"),
            record.candidate_instruction_ref.as_ref(),
        )
        && json_optional_ref_matches(
            knowledge.get("tool_policy_ref"),
            record.tool_policy_ref.as_ref(),
        )
        && json_optional_ref_matches(
            knowledge.get("model_profile_ref"),
            record.model_profile_ref.as_ref(),
        )
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
