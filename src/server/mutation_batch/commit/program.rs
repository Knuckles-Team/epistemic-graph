use crate::graph::GraphCore;
use crate::protocol::{Method, ResultPayload};
use crate::server::persistence::PersistenceBackend;
use eg_types::contract::Nonce;

#[cfg(feature = "program-optimization")]
use eg_modality::OpaqueRef;

pub(crate) struct ProgramPromotionRequest<'a> {
    pub(crate) persistence: Option<&'a std::sync::Arc<dyn PersistenceBackend>>,
    pub(crate) core: &'a std::sync::Arc<GraphCore>,
    pub(crate) request_id: u64,
    pub(crate) principal: Option<&'a str>,
    pub(crate) graph: &'a str,
    pub(crate) batch_id: &'a str,
    pub(crate) claim_methods: Vec<Method>,
    pub(crate) result: &'a ResultPayload,
    identity: Option<&'a eg_program::ProgramRevisionIdentity>,
    attempt_nonce: Option<Nonce>,
}

impl<'a> ProgramPromotionRequest<'a> {
    pub(crate) fn new(
        persistence: Option<&'a std::sync::Arc<dyn PersistenceBackend>>,
        core: &'a std::sync::Arc<GraphCore>,
        request_id: u64,
        principal: Option<&'a str>,
        graph: &'a str,
        batch_id: &'a str,
        claim_methods: Vec<Method>,
        result: &'a ResultPayload,
    ) -> Self {
        Self {
            persistence,
            core,
            request_id,
            principal,
            graph,
            batch_id,
            claim_methods,
            result,
            identity: None,
            attempt_nonce: None,
        }
    }

    pub(crate) fn with_identity(
        mut self,
        identity: &'a eg_program::ProgramRevisionIdentity,
    ) -> Self {
        self.identity = Some(identity);
        self
    }

    pub(crate) fn with_attempt_nonce(mut self, attempt_nonce: Option<Nonce>) -> Self {
        self.attempt_nonce = attempt_nonce;
        self
    }
}

/// Commit the selected native program revision and its result claim through the
/// same graph authority. The stable batch identity is checked before this
/// function performs any pointer/CAS validation, so a retry with a new nonce
/// returns its stored receipt even when the active pointer has since moved.
pub(crate) async fn commit_program_promotion(
    request: ProgramPromotionRequest<'_>,
) -> Result<crate::mutation_batch::MutationBatchCommit, String> {
    let ProgramPromotionRequest {
        persistence,
        core,
        request_id,
        principal,
        graph,
        batch_id,
        claim_methods,
        result,
        identity,
        attempt_nonce,
    } = request;
    let identity =
        identity.ok_or_else(|| "program promotion request has no identity".to_string())?;
    identity
        .validate()
        .map_err(|error| format!("invalid program promotion identity: {error}"))?;
    let record = identity
        .candidate_record
        .as_ref()
        .ok_or_else(|| "program promotion identity has no durable candidate record".to_string())?;
    let claim_method = claim_methods.iter().find_map(|method| match method {
        Method::AddNode {
            node_id,
            properties_msgpack,
        } if node_id == &record.candidate_claim_ref => Some(properties_msgpack),
        _ => None,
    });
    let claim_properties = claim_method
        .ok_or_else(|| "program promotion claim has no selected candidate row".to_string())
        .and_then(|properties| {
            eg_types::msgpack::decode_property_value(properties)
                .map_err(|_| "program promotion candidate claim is not decodable".to_string())
        })?;
    let result_claim_ref = format!("jobclaim:{}", record.result_ref.as_str());
    let result_claim_properties = claim_methods.iter().find_map(|method| match method {
        Method::AddNode {
            node_id,
            properties_msgpack,
        } if node_id == &result_claim_ref => Some(properties_msgpack),
        _ => None,
    });
    let result_claim_properties = result_claim_properties
        .ok_or_else(|| "program promotion claim has no result lineage row".to_string())
        .and_then(|properties| {
            eg_types::msgpack::decode_property_value(properties)
                .map_err(|_| "program promotion result claim is not decodable".to_string())
        })?;
    validate_program_result_claim(identity, &result_claim_properties)?;
    identity
        .validate_candidate_claim(&claim_properties)
        .map_err(|error| format!("program promotion candidate claim is invalid: {error}"))?;
    if claim_methods
        .iter()
        .any(|method| !matches!(method, Method::AddNode { .. } | Method::AddEdge { .. }))
    {
        return Err("program promotion claim contains a non-claim graph method".to_string());
    }
    let mut methods = program_promotion_methods(identity)?;
    methods.extend(claim_methods);
    super::internal::commit_internal_graph_methods_with_nonce_mode(
        super::internal::InternalGraphCommitRequest::new(
            persistence,
            core,
            request_id,
            principal,
            graph,
            batch_id,
            methods,
            result,
        )
        .with_attempt_nonce(attempt_nonce)
        .with_strict_promotion(),
    )
    .await
}

fn program_promotion_methods(
    identity: &eg_program::ProgramRevisionIdentity,
) -> Result<Vec<Method>, String> {
    let record = identity
        .candidate_record
        .as_ref()
        .ok_or_else(|| "program promotion identity has no durable candidate record".to_string())?;
    let mut candidate_properties =
        serde_json::to_value(record).map_err(|error| error.to_string())?;
    candidate_properties
        .as_object_mut()
        .ok_or_else(|| "program candidate record is not an object".to_string())?
        .insert(
            "type".to_string(),
            serde_json::Value::String("ProgramCandidate".to_string()),
        );
    let revision_properties = serde_json::json!({
        "type": "ProgramRevision",
        "schema_version": identity.schema_version,
        "program_ref": identity.program_ref.as_str(),
        "revision_ref": identity.revision_ref.as_str(),
        "base_revision": identity.base_revision,
        "revision": identity.revision,
        "parent_ref": identity.parent_ref.as_ref().map(|value| value.as_str()),
        "candidate_ref": identity.candidate_ref.as_str(),
        "content_digest": identity.content_digest,
        "policy": &identity.policy,
        "tool_policy_ref": identity.tool_policy_ref.as_ref().map(|value| value.as_str()),
        "model_profile_ref": identity.model_profile_ref.as_ref().map(|value| value.as_str()),
        "candidate_record": &identity.candidate_record,
    });
    let active_properties = serde_json::json!({
        "type": "ProgramActiveRevision",
        "schema_version": identity.schema_version,
        "program_ref": identity.program_ref.as_str(),
        "revision_ref": identity.revision_ref.as_str(),
        "base_revision": identity.base_revision,
        "revision": identity.revision,
        "parent_ref": identity.parent_ref.as_ref().map(|value| value.as_str()),
        "candidate_ref": identity.candidate_ref.as_str(),
        "content_digest": identity.content_digest,
        "policy": &identity.policy,
        "tool_policy_ref": identity.tool_policy_ref.as_ref().map(|value| value.as_str()),
        "model_profile_ref": identity.model_profile_ref.as_ref().map(|value| value.as_str()),
        "candidate_record": &identity.candidate_record,
    });
    let candidate_bytes =
        rmp_serde::to_vec_named(&candidate_properties).map_err(|error| error.to_string())?;
    let revision_bytes =
        rmp_serde::to_vec_named(&revision_properties).map_err(|error| error.to_string())?;
    let active_bytes =
        rmp_serde::to_vec_named(&active_properties).map_err(|error| error.to_string())?;
    let active_node = identity.active_pointer_ref();
    let mut methods = vec![
        Method::AddNode {
            node_id: identity.candidate_ref.as_str().to_string(),
            properties_msgpack: candidate_bytes,
        },
        Method::AddNode {
            node_id: identity.revision_ref.as_str().to_string(),
            properties_msgpack: revision_bytes,
        },
    ];
    for (reference, node_type) in [
        (identity.tool_policy_ref.as_ref(), "ToolPolicy"),
        (identity.model_profile_ref.as_ref(), "ModelProfile"),
    ] {
        if let Some(reference) = reference {
            let conditions = serde_json::json!({
                "type": node_type,
                "ref": reference.as_str(),
            });
            methods.push(Method::CompareAndSetNodeFields {
                node_id: reference.as_str().to_string(),
                conditions_msgpack: rmp_serde::to_vec_named(&conditions)
                    .map_err(|error| error.to_string())?,
                updates_msgpack: rmp_serde::to_vec_named(&serde_json::json!({}))
                    .map_err(|error| error.to_string())?,
            });
        }
    }
    if let Some(parent_ref) = &identity.parent_ref {
        let conditions = serde_json::json!({
            "program_ref": identity.program_ref.as_str(),
            "revision_ref": parent_ref.as_str(),
            "revision": identity.base_revision,
        });
        let conditions_msgpack =
            rmp_serde::to_vec_named(&conditions).map_err(|error| error.to_string())?;
        methods.push(Method::CompareAndSetNodeFields {
            node_id: active_node.as_str().to_string(),
            conditions_msgpack,
            updates_msgpack: active_bytes,
        });
    } else {
        methods.push(Method::AddNode {
            node_id: active_node.as_str().to_string(),
            properties_msgpack: active_bytes,
        });
    }
    Ok(methods)
}

fn resolve_program_binding_node(
    core: &GraphCore,
    reference: &OpaqueRef,
    expected_type: &str,
) -> Result<(), String> {
    let properties = core
        .get_node_properties(reference.as_str())
        .ok_or_else(|| {
            format!(
                "durable program binding '{}' is missing",
                reference.as_str()
            )
        })?;
    let value = eg_types::msgpack::decode_property_value(&properties)
        .map_err(|_| format!("durable program binding '{}' is not decodable", reference))?;
    let object = value
        .as_object()
        .ok_or_else(|| format!("durable program binding '{}' is not an object", reference))?;
    if object.get("type").and_then(serde_json::Value::as_str) != Some(expected_type)
        || object.get("ref").and_then(serde_json::Value::as_str) != Some(reference.as_str())
    {
        return Err(format!(
            "durable program binding '{}' has the wrong canonical type or ref",
            reference.as_str()
        ));
    }
    Ok(())
}

fn validate_program_result_claim(
    identity: &eg_program::ProgramRevisionIdentity,
    claim_properties: &serde_json::Value,
) -> Result<(), String> {
    let record = identity
        .candidate_record
        .as_ref()
        .ok_or_else(|| "program promotion identity has no candidate record".to_string())?;
    let object = claim_properties
        .as_object()
        .ok_or_else(|| "program result claim is not an object".to_string())?;
    validate_program_claim_identity(record, object)?;
    validate_program_input_snapshot(record, object)?;
    validate_program_claim_lineage(object)
}

fn validate_program_claim_identity(
    record: &eg_program::ProgramCandidateRecord,
    object: &serde_json::Map<String, serde_json::Value>,
) -> Result<(), String> {
    if object.get("type").and_then(serde_json::Value::as_str) != Some("Claim")
        || object.get("family").and_then(serde_json::Value::as_str) != Some("program.optimization")
        || object.get("about").and_then(serde_json::Value::as_str)
            != Some(record.result_ref.as_str())
        || object.get("result_ref").and_then(serde_json::Value::as_str)
            != Some(record.result_ref.as_str())
    {
        return Err("program result claim is not bound to the promotion result".to_string());
    }
    Ok(())
}

fn validate_program_input_snapshot(
    record: &eg_program::ProgramCandidateRecord,
    object: &serde_json::Map<String, serde_json::Value>,
) -> Result<(), String> {
    let input_dataset_ref = object
        .get("input_dataset_ref")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| "program result claim has no input dataset binding".to_string())?;
    let input_dataset_ref = OpaqueRef::new(input_dataset_ref.to_string())
        .map_err(|_| "program result claim input dataset binding is invalid".to_string())?;
    if input_dataset_ref.namespace() != "job_input" {
        return Err(
            "program result claim input dataset binding has the wrong namespace".to_string(),
        );
    }
    let input_content_digest = object
        .get("input_content_digest")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| "program result claim has no input content digest".to_string())?;
    let input_snapshot_version = object
        .get("input_snapshot_version")
        .and_then(serde_json::Value::as_u64)
        .ok_or_else(|| "program result claim has no input snapshot binding".to_string())?;
    if input_content_digest.len() != 64
        || !input_content_digest
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
        || input_snapshot_version == 0
        || input_dataset_ref != record.result_input_dataset_ref
        || input_content_digest != record.result_input_content_digest
        || input_snapshot_version != record.result_input_snapshot_version
    {
        return Err("program result claim input snapshot binding is invalid".to_string());
    }
    Ok(())
}

fn validate_program_claim_lineage(
    object: &serde_json::Map<String, serde_json::Value>,
) -> Result<(), String> {
    let job_id = object
        .get("job_id")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| "program result claim has no job binding".to_string())?;
    let algo_family = object
        .get("algo_family")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| "program result claim has no algorithm family binding".to_string())?;
    let algo_algorithm = object
        .get("algo_algorithm")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| "program result claim has no algorithm binding".to_string())?;
    let has_complete_algo_lineage = [
        "algo_params_digest",
        "algo_code_version",
        "algo_env_version",
    ]
    .into_iter()
    .all(|field| {
        object
            .get(field)
            .and_then(serde_json::Value::as_str)
            .is_some_and(|value| !value.is_empty())
    });
    let confidence = object
        .get("confidence")
        .and_then(serde_json::Value::as_f64)
        .ok_or_else(|| "program result claim has no confidence binding".to_string())?;
    if job_id.is_empty()
        || algo_family != "program.optimization"
        || algo_algorithm.is_empty()
        || !has_complete_algo_lineage
        || !confidence.is_finite()
        || !(0.0..=1.0).contains(&confidence)
        || object
            .get("validation_state")
            .and_then(serde_json::Value::as_str)
            != Some("unvalidated")
    {
        return Err("program result claim lineage binding is invalid".to_string());
    }
    Ok(())
}
/// Resolve a promoted revision only through its durable revision, candidate,
/// and selected-result-claim chain. The analytics job row may be purged after
/// publication; these graph rows remain the immutable execution identity.
pub(crate) fn resolve_program_promotion_identity(
    core: &GraphCore,
    revision_ref: &OpaqueRef,
) -> Result<eg_program::ProgramRevisionIdentity, String> {
    let identity = load_program_revision_identity(core, revision_ref)?;
    let record = identity
        .candidate_record
        .as_ref()
        .ok_or_else(|| "durable program revision has no candidate record".to_string())?;
    validate_program_candidate_chain(core, &identity, record)?;
    validate_program_claim_chain(core, &identity, record)?;
    Ok(identity)
}

fn load_program_revision_identity(
    core: &GraphCore,
    revision_ref: &OpaqueRef,
) -> Result<eg_program::ProgramRevisionIdentity, String> {
    let revision_properties = core
        .get_node_properties(revision_ref.as_str())
        .ok_or_else(|| "durable program revision is missing".to_string())?;
    let revision_value = eg_types::msgpack::decode_property_value(&revision_properties)
        .map_err(|_| "durable program revision is not decodable".to_string())?;
    let identity = eg_program::ProgramRevisionIdentity::from_durable_properties(&revision_value)
        .map_err(|error| format!("durable program revision is invalid: {error}"))?;
    if identity.revision_ref != *revision_ref {
        return Err("durable program revision id does not match its row".to_string());
    }
    Ok(identity)
}

fn validate_program_candidate_chain(
    core: &GraphCore,
    identity: &eg_program::ProgramRevisionIdentity,
    record: &eg_program::ProgramCandidateRecord,
) -> Result<(), String> {
    let candidate_properties = core
        .get_node_properties(identity.candidate_ref.as_str())
        .ok_or_else(|| "durable program candidate record is missing".to_string())?;
    let candidate_value = eg_types::msgpack::decode_property_value(&candidate_properties)
        .map_err(|_| "durable program candidate record is not decodable".to_string())?;
    let mut candidate_object = candidate_value
        .as_object()
        .cloned()
        .ok_or_else(|| "durable program candidate record is not an object".to_string())?;
    if candidate_object.remove("type")
        != Some(serde_json::Value::String("ProgramCandidate".to_string()))
    {
        return Err("durable program candidate record has the wrong type".to_string());
    }
    let stored_record: eg_program::ProgramCandidateRecord =
        serde_json::from_value(serde_json::Value::Object(candidate_object))
            .map_err(|_| "durable program candidate record is invalid".to_string())?;
    stored_record
        .validate()
        .map_err(|error| format!("durable program candidate digest is invalid: {error}"))?;
    if stored_record != *record {
        return Err("durable program candidate record differs from the revision".to_string());
    }
    if let Some(reference) = record.tool_policy_ref.as_ref() {
        resolve_program_binding_node(core, reference, "ToolPolicy")?;
    }
    if let Some(reference) = record.model_profile_ref.as_ref() {
        resolve_program_binding_node(core, reference, "ModelProfile")?;
    }
    Ok(())
}

fn validate_program_claim_chain(
    core: &GraphCore,
    identity: &eg_program::ProgramRevisionIdentity,
    record: &eg_program::ProgramCandidateRecord,
) -> Result<(), String> {
    let claim_properties = core
        .get_node_properties(&record.candidate_claim_ref)
        .ok_or_else(|| "durable selected candidate claim is missing".to_string())?;
    let claim_value = eg_types::msgpack::decode_property_value(&claim_properties)
        .map_err(|_| "durable selected candidate claim is not decodable".to_string())?;
    identity
        .validate_candidate_claim(&claim_value)
        .map_err(|error| format!("durable selected candidate claim is invalid: {error}"))?;
    let result_claim_ref = format!("jobclaim:{}", record.result_ref.as_str());
    let result_claim_properties = core
        .get_node_properties(&result_claim_ref)
        .ok_or_else(|| "durable program result claim is missing".to_string())?;
    let result_claim_value = eg_types::msgpack::decode_property_value(&result_claim_properties)
        .map_err(|_| "durable program result claim is not decodable".to_string())?;
    validate_program_result_claim(identity, &result_claim_value)
}
/// Apply a promotion write-set to the isolated staging graph with fail-closed
/// AddNode and AddEdge checks. Generic internal graph writes intentionally keep
/// their historical upsert/no-op behavior; only the promotion coordinator uses
/// this strict path.
pub(super) fn apply_promotion_projectable_method(
    core: &GraphCore,
    method: &Method,
) -> Result<(), String> {
    match method {
        Method::AddNode {
            node_id,
            properties_msgpack,
        } => {
            if core.has_node(node_id) {
                return if core.get_node_properties(node_id).as_deref()
                    == Some(properties_msgpack.as_slice())
                {
                    Ok(())
                } else {
                    Err(format!(
                        "promotion node '{}' already has different properties",
                        node_id
                    ))
                };
            }
            if core.create_node_if_absent(node_id.clone(), properties_msgpack.clone()) {
                Ok(())
            } else {
                Err(format!("promotion node '{}' could not be created", node_id))
            }
        }
        Method::AddEdge {
            source_id,
            target_id,
            properties_msgpack,
        } => {
            let snapshot = core.snapshot();
            let existing = snapshot.edges.iter().find_map(|(source, target, bytes)| {
                (source == source_id && target == target_id).then_some(bytes.as_ref())
            });
            match existing {
                Some(bytes) if bytes == properties_msgpack => Ok(()),
                Some(_) => Err(format!(
                    "promotion edge '{} -> {}' already has different properties",
                    source_id, target_id
                )),
                None => core.add_edge(
                    source_id.clone(),
                    target_id.clone(),
                    properties_msgpack.clone(),
                ),
            }
        }
        Method::CompareAndSetNodeFields {
            node_id,
            conditions_msgpack,
            updates_msgpack,
        } => {
            let conditions = eg_types::msgpack::decode_property_object(conditions_msgpack)
                .map_err(|_| "invalid promotion CAS conditions".to_string())?;
            let updates = eg_types::msgpack::decode_property_object(updates_msgpack)
                .map_err(|_| "invalid promotion CAS updates".to_string())?;
            if core.compare_and_set_fields(node_id, &conditions, &updates) {
                Ok(())
            } else {
                Err(format!(
                    "promotion active pointer CAS failed for '{}'",
                    node_id
                ))
            }
        }
        _ => Err("promotion write-set contains a non-projectable method".to_string()),
    }
}
