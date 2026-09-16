use super::*;

pub(crate) fn validate_resource_extension_authority(
    extension: &serde_json::Map<String, serde_json::Value>,
) -> Result<(), String> {
    if extension
        .get("schema_version")
        .and_then(serde_json::Value::as_str)
        != Some("1")
        || extension
            .get("resolved_profile_authority")
            .and_then(serde_json::Value::as_str)
            != Some("repository_manager:resource_profile_registry:v1")
    {
        return Err(
            "WorkItem resource extension is legacy or lacks resolved-profile authority".into(),
        );
    }
    Ok(())
}

pub(crate) fn resolve_resource_extension_branch(
    extension: &serde_json::Map<String, serde_json::Value>,
    request: &ResourceReservationRequest,
) -> Result<String, String> {
    let extension_branch = resource_metadata_string(extension, "branch", "resource branch")?;
    if request.branch_exclusive
        && extension
            .get("branch_explicit")
            .and_then(serde_json::Value::as_bool)
            != Some(true)
    {
        return Err("branch-exclusive WorkItem has no explicit branch".into());
    }
    Ok(extension_branch)
}

pub(crate) fn resource_extension_validate_and_extract_branch(
    extension: &serde_json::Map<String, serde_json::Value>,
    request: &ResourceReservationRequest,
) -> Result<String, String> {
    validate_resource_extension_authority(extension)?;
    resolve_resource_extension_branch(extension, request)
}

/// The four sorted label sets `resource_extension_resolve_labels` returns.
pub(crate) struct ResourceLabelSets {
    /// The host's advertised labels.
    pub(crate) labels: Vec<String>,
    /// The request's required labels.
    pub(crate) request_labels: Vec<String>,
    /// The host's anti-affinity keys.
    pub(crate) anti_affinity: Vec<String>,
    /// The request's anti-affinity keys.
    pub(crate) request_anti_affinity: Vec<String>,
}

pub(crate) fn resource_extension_resolve_labels(
    extension: &serde_json::Map<String, serde_json::Value>,
    request: &ResourceReservationRequest,
) -> Result<ResourceLabelSets, String> {
    let mut labels =
        resource_opaque_sequence(extension.get("host_labels"), "resource host_labels")?;
    labels.sort();
    let mut request_labels = request.required_labels.clone();
    request_labels.sort();
    let mut anti_affinity =
        resource_opaque_sequence(extension.get("anti_affinity"), "resource anti_affinity")?;
    anti_affinity.sort();
    let mut request_anti_affinity = request.anti_affinity.clone();
    request_anti_affinity.sort();
    Ok(ResourceLabelSets {
        labels,
        request_labels,
        anti_affinity,
        request_anti_affinity,
    })
}

// This is the immutable outer WorkItem digest, not an opaque user field.
// Keep its frozen `v1:<lowercase-hex>` spelling separate from the nested
// opaque:v1 values so a valid resolved WorkItem is not rejected at the
// trust boundary.
pub(crate) fn verify_resource_extension_work_item_digest(
    repository: &serde_json::Map<String, serde_json::Value>,
    extension: &serde_json::Map<String, serde_json::Value>,
) -> Result<bool, String> {
    let work_item_digest = extension
        .get("work_item_input_fingerprint")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| "work_item_input_fingerprint is missing".to_string())?
        .to_string();
    resource_fingerprint(&work_item_digest, "work_item_input_fingerprint")?;
    let stored_work_item_digest = repository
        .get("immutable_input_digest")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| "repository immutable_input_digest is missing".to_string())?;
    if stored_work_item_digest.len() != 64
        || !stored_work_item_digest
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
        || work_item_digest != format!("v1:{stored_work_item_digest}")
    {
        // The nested WorkItem admission digest is distinct from the later
        // fenced reservation fingerprint, but it must still be bound to the
        // immutable outer WorkItem digest.  A validly-shaped forged digest
        // cannot otherwise be detected by field-by-field policy comparison.
        return Ok(false);
    }
    Ok(true)
}

pub(crate) fn resource_extension_resolve_alias_if_digest_matches(
    repository: &serde_json::Map<String, serde_json::Value>,
    extension: &serde_json::Map<String, serde_json::Value>,
) -> Result<Option<Option<String>>, String> {
    let alias = resource_opaque_string(extension.get("target_alias"), "resource target_alias")?;
    if !verify_resource_extension_work_item_digest(repository, extension)? {
        return Ok(None);
    }
    Ok(Some(alias))
}

/// The resolved-profile fields carried by a WorkItem's nested resource extension.
pub(crate) struct ResourceExtensionProfileFields {
    pub(crate) profile_version: String,
    pub(crate) profile_name: String,
    pub(crate) repository_id: String,
    pub(crate) concurrency_key: String,
    pub(crate) fairness_group: String,
    pub(crate) disk_policy_key: String,
}

/// The outer WorkItem repository projection the nested extension must agree with.
pub(crate) struct ResourceExtensionRepositoryFields {
    pub(crate) repository_id_outer: String,
    pub(crate) owner_id_outer: String,
    pub(crate) outer_target_kind: String,
    pub(crate) outer_target_alias: Option<String>,
    pub(crate) tenant_id: String,
}

pub(crate) fn resolve_resource_extension_profile_fields(
    extension: &serde_json::Map<String, serde_json::Value>,
) -> Result<ResourceExtensionProfileFields, String> {
    let profile_version =
        resource_metadata_string(extension, "profile_version", "resource profile_version")?;
    let profile_version_number = profile_version
        .parse::<u64>()
        .map_err(|_| "resource profile_version must be a canonical integer".to_string())?;
    if profile_version_number.to_string() != profile_version {
        return Err("resource profile_version must use canonical integer spelling".to_string());
    }
    let profile_name =
        resource_metadata_string(extension, "profile_name", "resource profile_name")?;
    let repository_id =
        resource_metadata_string(extension, "repository_id", "resource repository_id")?;
    let concurrency_key =
        resource_metadata_string(extension, "concurrency_key", "resource concurrency_key")?;
    let fairness_group =
        resource_metadata_string(extension, "fairness_group", "resource fairness_group")?;
    let disk_policy_key =
        resource_metadata_string(extension, "disk_policy_key", "resource disk_policy_key")?;
    Ok(ResourceExtensionProfileFields {
        profile_version,
        profile_name,
        repository_id,
        concurrency_key,
        fairness_group,
        disk_policy_key,
    })
}

pub(crate) fn resolve_resource_extension_repository_fields(
    repository: &serde_json::Map<String, serde_json::Value>,
) -> Result<ResourceExtensionRepositoryFields, String> {
    let repository_id_outer =
        resource_metadata_string(repository, "repository_id", "repository repository_id")?;
    let owner_id_outer = resource_metadata_string(repository, "owner_id", "repository owner_id")?;
    let outer_target_kind = repository
        .get("target_kind")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| "repository target_kind is missing".to_string())?
        .to_string();
    let outer_target_alias =
        resource_opaque_string(repository.get("target_alias"), "repository target_alias")?;
    let tenant_id = repository
        .get("tenant_id")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| "repository tenant_id is missing".to_string())?;
    let tenant_id = resource_b64_value(tenant_id, "repository tenant_id")?;
    Ok(ResourceExtensionRepositoryFields {
        repository_id_outer,
        owner_id_outer,
        outer_target_kind,
        outer_target_alias,
        tenant_id,
    })
}

pub(crate) fn resource_extension_resolve_extracted_fields(
    repository: &serde_json::Map<String, serde_json::Value>,
    extension: &serde_json::Map<String, serde_json::Value>,
) -> Result<
    (
        ResourceExtensionProfileFields,
        ResourceExtensionRepositoryFields,
    ),
    String,
> {
    let profile_fields = resolve_resource_extension_profile_fields(extension)?;
    let repository_fields = resolve_resource_extension_repository_fields(repository)?;
    Ok((profile_fields, repository_fields))
}

pub(crate) fn resolve_resource_extension_target_kind(
    extension: &serde_json::Map<String, serde_json::Value>,
    alias: &Option<String>,
) -> Result<String, String> {
    let extension_target_kind = extension
        .get("target_kind")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| "resource target_kind is missing".to_string())?;
    if extension_target_kind != "local" && extension_target_kind != "inventory_alias" {
        return Err("resource target_kind is invalid".to_string());
    }
    if (extension_target_kind == "local") != alias.is_none() {
        return Err("resource target_alias does not match target_kind".to_string());
    }
    Ok(extension_target_kind.to_string())
}

pub(crate) struct ResourceExtensionIdentity<'a> {
    pub(crate) request: &'a ResourceReservationRequest,
    pub(crate) profile_name: &'a str,
    pub(crate) profile_version: &'a str,
    pub(crate) repository_id: &'a str,
    pub(crate) repository_id_outer: &'a str,
    pub(crate) owner_id_outer: &'a str,
    pub(crate) tenant_id: &'a str,
    pub(crate) extension_branch: &'a str,
    pub(crate) extension_target_kind: &'a str,
    pub(crate) outer_target_kind: &'a str,
    pub(crate) alias: &'a Option<String>,
    pub(crate) outer_target_alias: &'a Option<String>,
    pub(crate) concurrency_key: &'a str,
}

pub(crate) fn resource_extension_identity_matches(context: ResourceExtensionIdentity<'_>) -> bool {
    context.profile_name == context.request.profile_name
        && context.profile_version == context.request.profile_version
        && context.repository_id == context.request.repository_id
        && context.repository_id_outer == context.request.repository_id
        && context.owner_id_outer == context.request.owner_id
        && context.tenant_id == context.request.tenant_ref
        && context.extension_branch == context.request.branch
        // The nested extension and the outer WorkItem projection must agree on
        // the original execution-target declaration.  The reservation request
        // carries the scheduler's *selected* host target, which may be remote
        // even when this top-level declaration is local with a remote
        // preferred/required policy; that selected pair is checked separately
        // against the host row below.
        && context.extension_target_kind == context.outer_target_kind
        && context.alias.as_deref() == context.outer_target_alias.as_deref()
        && context.concurrency_key == context.request.concurrency_key
}

pub(crate) fn resource_extension_requirements_match(
    extension: &serde_json::Map<String, serde_json::Value>,
    request: &ResourceReservationRequest,
) -> Result<bool, String> {
    Ok(resource_u64_matches(
        extension,
        "cpu_weight",
        request.requirement.cpu_weight,
        "resource cpu_weight",
    )? && resource_u64_matches(
        extension,
        "memory_mib",
        request.requirement.memory_mib,
        "resource memory_mib",
    )? && resource_u64_matches(
        extension,
        "disk_mib",
        request.requirement.disk_mib,
        "resource disk_mib",
    )? && resource_u64_matches(
        extension,
        "process_slots",
        request.requirement.process_slots,
        "resource process_slots",
    )?)
}

pub(crate) fn resource_extension_affinity_and_exclusivity_matches(
    extension: &serde_json::Map<String, serde_json::Value>,
    request: &ResourceReservationRequest,
    label_sets: &ResourceLabelSets,
    fairness_group: &str,
) -> Result<bool, String> {
    Ok(label_sets.labels == label_sets.request_labels
        && label_sets.anti_affinity == label_sets.request_anti_affinity
        && fairness_group == request.fairness_group
        && resource_optional_u64_matches(
            extension,
            "concurrency_limit",
            request.concurrency_limit,
            "resource concurrency_limit",
        )?
        && resource_bool_matches(
            extension,
            "repository_exclusive",
            request.repository_exclusive,
            "resource repository_exclusive",
        )?
        && resource_bool_matches(
            extension,
            "branch_exclusive",
            request.branch_exclusive,
            "resource branch_exclusive",
        )?)
}

pub(crate) fn resource_extension_disk_policy_matches(
    extension: &serde_json::Map<String, serde_json::Value>,
    request: &ResourceReservationRequest,
    disk_policy_key: &str,
) -> Result<bool, String> {
    Ok(resource_optional_u64_matches(
        extension,
        "disk_low_watermark_mib",
        request.disk_low_watermark_mib,
        "resource disk_low_watermark_mib",
    )? && resource_optional_u64_matches(
        extension,
        "disk_high_watermark_mib",
        request.disk_high_watermark_mib,
        "resource disk_high_watermark_mib",
    )? && disk_policy_key == request.disk_policy_key
        && resource_optional_u64_matches(
            extension,
            "fairness_cost",
            Some(request.fairness_cost),
            "resource fairness_cost",
        )?)
}

pub(crate) fn resource_extension_policy_matches(
    extension: &serde_json::Map<String, serde_json::Value>,
    request: &ResourceReservationRequest,
    label_sets: &ResourceLabelSets,
    fairness_group: &str,
    disk_policy_key: &str,
) -> Result<bool, String> {
    Ok(resource_extension_affinity_and_exclusivity_matches(
        extension,
        request,
        label_sets,
        fairness_group,
    )? && resource_extension_disk_policy_matches(extension, request, disk_policy_key)?)
}

pub(crate) struct ResourceExtensionFinalMatch<'a> {
    pub(crate) extension: &'a serde_json::Map<String, serde_json::Value>,
    pub(crate) request: &'a ResourceReservationRequest,
    pub(crate) extension_branch: &'a str,
    pub(crate) label_sets: &'a ResourceLabelSets,
    pub(crate) alias: &'a Option<String>,
    pub(crate) profile_fields: &'a ResourceExtensionProfileFields,
    pub(crate) repository_fields: &'a ResourceExtensionRepositoryFields,
    pub(crate) extension_target_kind: &'a str,
}

pub(crate) fn resource_extension_final_match(
    context: ResourceExtensionFinalMatch<'_>,
) -> Result<bool, String> {
    let profile = context.profile_fields;
    let repository = context.repository_fields;
    let identity_ok = resource_extension_identity_matches(ResourceExtensionIdentity {
        request: context.request,
        profile_name: &profile.profile_name,
        profile_version: &profile.profile_version,
        repository_id: &profile.repository_id,
        repository_id_outer: &repository.repository_id_outer,
        owner_id_outer: &repository.owner_id_outer,
        tenant_id: &repository.tenant_id,
        extension_branch: context.extension_branch,
        extension_target_kind: context.extension_target_kind,
        outer_target_kind: &repository.outer_target_kind,
        alias: context.alias,
        outer_target_alias: &repository.outer_target_alias,
        concurrency_key: &profile.concurrency_key,
    });
    let requirements_ok =
        resource_extension_requirements_match(context.extension, context.request)?;
    let policy_ok = resource_extension_policy_matches(
        context.extension,
        context.request,
        context.label_sets,
        &profile.fairness_group,
        &profile.disk_policy_key,
    )?;
    Ok(identity_ok && requirements_ok && policy_ok)
}

pub(crate) fn resource_extension_matches(
    repository: &serde_json::Map<String, serde_json::Value>,
    extension: &serde_json::Map<String, serde_json::Value>,
    request: &ResourceReservationRequest,
) -> Result<bool, String> {
    let extension_branch = resource_extension_validate_and_extract_branch(extension, request)?;
    let label_sets = resource_extension_resolve_labels(extension, request)?;
    let alias = match resource_extension_resolve_alias_if_digest_matches(repository, extension)? {
        Some(alias) => alias,
        None => return Ok(false),
    };
    let (profile_fields, repository_fields) =
        resource_extension_resolve_extracted_fields(repository, extension)?;
    let extension_target_kind = resolve_resource_extension_target_kind(extension, &alias)?;
    resource_extension_final_match(ResourceExtensionFinalMatch {
        extension,
        request,
        extension_branch: &extension_branch,
        label_sets: &label_sets,
        alias: &alias,
        profile_fields: &profile_fields,
        repository_fields: &repository_fields,
        extension_target_kind: &extension_target_kind,
    })
}

/// Tail of `resource_validate_work_item`, run after the fence checks and in the
/// same order: the lease-owner match (skipped for a superseded row, exactly as
/// before) followed by the repository/extension projection comparison.
pub(crate) fn resource_validate_work_item_owner_and_extension(
    props: &serde_json::Map<String, serde_json::Value>,
    request: &ResourceReservationRequest,
    superseded: bool,
) -> Result<(), ResourceReservationResultDecision> {
    let status = property_string(props, "status");
    let owner = if matches!(status, "leased" | "running") {
        property_string(props, "lease_owner")
    } else {
        property_string(props, "last_lease_owner")
    };
    if !superseded && owner != request.owner_id {
        return Err(ResourceReservationResultDecision::Stale);
    }
    let (repository, extension) =
        resource_metadata_maps(props).map_err(|_| ResourceReservationResultDecision::Policy)?;
    let matches = resource_extension_matches(repository, extension, request)
        .map_err(|_| ResourceReservationResultDecision::Policy)?;
    if !matches {
        return Err(ResourceReservationResultDecision::InputConflict);
    }
    Ok(())
}

pub(crate) fn resource_validate_work_item(
    props: &serde_json::Map<String, serde_json::Value>,
    request: &ResourceReservationRequest,
    allow_superseded: bool,
) -> Result<ResourceWorkItemFence, ResourceReservationResultDecision> {
    if property_string(props, "node_type") != "WorkItem"
        || property_string(props, "tenant") != request.tenant_ref
    {
        return Err(ResourceReservationResultDecision::NotFound);
    }
    let current_attempt = property_u64(props, "attempt");
    let lease_epoch = property_u64(props, "lease_epoch");
    let fencing_token = property_u64(props, "fencing_token");
    let superseded = current_attempt > request.attempt
        || lease_epoch != request.lease_epoch
        || fencing_token != request.fencing_token;
    if superseded && !(allow_superseded && current_attempt > request.attempt) {
        return Err(ResourceReservationResultDecision::Stale);
    }
    if request.fence != resource_expected_fence(request.fencing_token) {
        return Err(ResourceReservationResultDecision::Stale);
    }
    resource_validate_work_item_owner_and_extension(props, request, superseded)?;
    Ok(ResourceWorkItemFence::new(
        current_attempt,
        lease_epoch,
        fencing_token,
        superseded,
    ))
}

pub(crate) fn resource_target_policy_value(
    value: Option<&serde_json::Value>,
    name: &str,
) -> Result<serde_json::Value, String> {
    let map = value
        .and_then(serde_json::Value::as_object)
        .ok_or_else(|| format!("{name} is missing"))?;
    let kind = map
        .get("kind")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| format!("{name}.kind is missing"))?;
    if kind != "local" && kind != "inventory_alias" {
        return Err(format!("{name}.kind is invalid"));
    }
    let alias = resource_opaque_string(map.get("alias"), &format!("{name}.alias"))?;
    if (kind == "local") != alias.is_none() {
        return Err(format!("{name}.alias does not match kind"));
    }
    let labels = resource_opaque_sequence(
        map.get("capability_labels"),
        &format!("{name}.capability_labels"),
    )?;
    let mut value = serde_json::Map::new();
    value.insert(
        "alias".into(),
        alias.map_or(serde_json::Value::Null, serde_json::Value::String),
    );
    value.insert("capability_labels".into(), serde_json::json!(labels));
    // ResourceProfileRegistry's TargetPolicy.model_dump(mode="json") includes
    // its contract marker.  This is part of RMDD-08's canonical fingerprint,
    // not merely a wire-validation detail.
    value.insert("contract_version".into(), serde_json::json!("1"));
    value.insert("kind".into(), serde_json::Value::String(kind.to_string()));
    Ok(serde_json::Value::Object(value))
}

/// Validate the selected host against the immutable WorkItem target policy.
/// A preferred target is a placement hint and is therefore intentionally not
/// required to equal the selected host; a required target is an admission
/// constraint and must match exactly.
pub(crate) fn resource_target_selection_matches(
    extension: &serde_json::Map<String, serde_json::Value>,
    host: &DurableResourceHost,
) -> Result<bool, String> {
    if let Some(required) = extension.get("required_target") {
        if !required.is_null() {
            let required =
                resource_target_policy_value(Some(required), "resource required_target")?;
            let required_kind = required
                .get("kind")
                .and_then(serde_json::Value::as_str)
                .ok_or_else(|| "resource required_target.kind is missing".to_string())?;
            let required_alias = required.get("alias").and_then(serde_json::Value::as_str);
            return Ok(
                required_kind == host.target_kind && required_alias == host.target_alias.as_deref()
            );
        }
    }
    let preferred = resource_target_policy_value(
        extension.get("preferred_target"),
        "resource preferred_target",
    )?;
    let preferred_kind = preferred
        .get("kind")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| "resource preferred_target.kind is missing".to_string())?;
    // A remote/inventory host is eligible only when the immutable policy
    // explicitly names an inventory preference.  The preferred alias orders
    // eligible remote hosts; it is not an equality constraint here.
    Ok(host.target_kind == "local" || preferred_kind == "inventory_alias")
}

pub(crate) fn resource_selected_target_matches_request(
    request: &ResourceReservationRequest,
    host: &DurableResourceHost,
) -> bool {
    resource_request_target_kind(request.target_kind) == host.target_kind
        && request.target_alias.as_deref() == host.target_alias.as_deref()
}
