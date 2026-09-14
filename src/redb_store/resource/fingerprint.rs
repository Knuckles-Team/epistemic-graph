use std::collections::BTreeMap;

struct ResourceFingerprintInputs {
    job_id: String,
    host_labels: Vec<String>,
    anti_affinity: Vec<String>,
    required_target: serde_json::Value,
    preferred_target: serde_json::Value,
    profile_version: u64,
    priority: u64,
    queue_deadline: serde_json::Value,
    ttl_seconds: u64,
}

fn resource_fingerprint_inputs(
    props: &serde_json::Map<String, serde_json::Value>,
    request: &ResourceReservationRequest,
) -> Result<ResourceFingerprintInputs, String> {
    let (repository, extension) = resource_metadata_maps(props)?;
    let job_id = resource_metadata_string(repository, "job_id", "repository job_id")?;
    let host_labels =
        resource_opaque_sequence(extension.get("host_labels"), "resource host_labels")?;
    let anti_affinity =
        resource_opaque_sequence(extension.get("anti_affinity"), "resource anti_affinity")?;
    let required_target = match extension.get("required_target") {
        Some(value) if !value.is_null() => {
            resource_target_policy_value(Some(value), "resource required_target")?
        }
        _ => serde_json::Value::Null,
    };
    let preferred_target = resource_target_policy_value(
        extension.get("preferred_target"),
        "resource preferred_target",
    )?;
    let profile_version =
        resource_metadata_string(extension, "profile_version", "resource profile_version")?;
    let profile_version_number = profile_version
        .parse::<u64>()
        .map_err(|_| "resource profile_version must be a canonical integer".to_string())?;
    if profile_version_number.to_string() != profile_version {
        return Err("resource profile_version must use canonical integer spelling".to_string());
    }
    let priority = resource_metadata_u64(repository, "priority", "repository priority")?;
    let queue_deadline = repository
        .get("queue_deadline")
        .cloned()
        .unwrap_or(serde_json::Value::Null);
    let queue_deadline = match queue_deadline {
        serde_json::Value::String(value) if value.ends_with("+00:00") => {
            serde_json::Value::String(format!("{}Z", &value[..value.len() - 6]))
        }
        value => value,
    };
    let ttl_ms = request
        .expires_at_ms
        .checked_sub(request.reserved_at_ms)
        .ok_or_else(|| "resource TTL underflow".to_string())?;
    if ttl_ms == 0 || ttl_ms % 1_000 != 0 {
        return Err("resource TTL must be an integral number of seconds".to_string());
    }
    Ok(ResourceFingerprintInputs {
        job_id,
        host_labels,
        anti_affinity,
        required_target,
        preferred_target,
        profile_version: profile_version_number,
        priority,
        queue_deadline,
        ttl_seconds: ttl_ms / 1_000,
    })
}

fn resource_fingerprint_resources(
    request: &ResourceReservationRequest,
    inputs: &ResourceFingerprintInputs,
) -> BTreeMap<&'static str, serde_json::Value> {
    let mut resources = BTreeMap::new();
    resources.insert("anti_affinity", serde_json::json!(inputs.anti_affinity));
    resources.insert(
        "concurrency_key",
        serde_json::json!(request.concurrency_key),
    );
    resources.insert("contract_version", serde_json::json!("1"));
    resources.insert(
        "cpu_weight",
        serde_json::json!(request.requirement.cpu_weight),
    );
    resources.insert(
        "disk_high_watermark_mib",
        request
            .disk_high_watermark_mib
            .map_or(serde_json::Value::Null, serde_json::Value::from),
    );
    resources.insert(
        "disk_low_watermark_mib",
        request
            .disk_low_watermark_mib
            .map_or(serde_json::Value::Null, serde_json::Value::from),
    );
    resources.insert("disk_mib", serde_json::json!(request.requirement.disk_mib));
    resources.insert("fairness_group", serde_json::json!(request.fairness_group));
    resources.insert("host_labels", serde_json::json!(inputs.host_labels));
    resources.insert(
        "memory_mib",
        serde_json::json!(request.requirement.memory_mib),
    );
    resources.insert("preferred_target", inputs.preferred_target.clone());
    resources.insert("priority", serde_json::json!(inputs.priority));
    resources.insert(
        "process_slots",
        serde_json::json!(request.requirement.process_slots),
    );
    resources.insert("queue_deadline", inputs.queue_deadline.clone());
    resources.insert("required_target", inputs.required_target.clone());
    resources.insert("resource_class", serde_json::json!(request.profile_name));
    resources
}

fn resource_fingerprint_payload(
    request: &ResourceReservationRequest,
    inputs: &ResourceFingerprintInputs,
    resources: BTreeMap<&'static str, serde_json::Value>,
) -> Result<BTreeMap<&'static str, serde_json::Value>, String> {
    let mut payload = BTreeMap::new();
    payload.insert("attempt", serde_json::json!(request.attempt));
    payload.insert("branch", serde_json::json!(request.branch));
    payload.insert("fence", serde_json::json!(request.fence));
    payload.insert("job_id", serde_json::json!(inputs.job_id));
    payload.insert("owner_id", serde_json::json!(request.owner_id));
    payload.insert("profile", serde_json::json!(request.profile_name));
    payload.insert("profile_version", serde_json::json!(inputs.profile_version));
    payload.insert("repository_id", serde_json::json!(request.repository_id));
    payload.insert("reservation_id", serde_json::json!(request.reservation_id));
    payload.insert(
        "resources",
        serde_json::to_value(resources).map_err(|e| e.to_string())?,
    );
    payload.insert("tenant_id", serde_json::json!(request.tenant_ref));
    payload.insert("ttl_seconds", serde_json::json!(inputs.ttl_seconds));
    payload.insert("version", serde_json::json!("v1"));
    payload.insert("work_item_id", serde_json::json!(request.work_item_id));
    Ok(payload)
}

pub(crate) fn resource_recomputed_fingerprint(
    props: &serde_json::Map<String, serde_json::Value>,
    request: &ResourceReservationRequest,
) -> Result<String, String> {
    let inputs = resource_fingerprint_inputs(props, request)?;
    let resources = resource_fingerprint_resources(request, &inputs);
    let payload = resource_fingerprint_payload(request, &inputs, resources)?;
    let bytes = serde_json::to_vec(&payload).map_err(|e| e.to_string())?;
    use sha2::{Digest, Sha256};
    Ok(format!("v1:{}", hex::encode(Sha256::digest(bytes))))
}

/// Identity/fencing half of the replay comparison: the keys that name the
/// reservation and the attempt that produced it.
pub(crate) fn resource_request_matches_identity(
    request: &ResourceReservationRequest,
    record: &ResourceReservationRecord,
) -> bool {
    request.reservation_id == record.reservation_id
        && request.tenant_ref == record.tenant_ref
        && request.owner_id == record.owner_id
        && request.work_item_id == record.work_item_id
        && request.fence == record.fence
        && request.attempt == record.attempt
        && request.lease_epoch == record.lease_epoch
        && request.fencing_token == record.fencing_token
}

/// Placement half: the fingerprint, host binding, resolved profile, requirement
/// vector and target selector.
pub(crate) fn resource_request_matches_placement(
    request: &ResourceReservationRequest,
    record: &ResourceReservationRecord,
) -> bool {
    request.input_fingerprint == record.input_fingerprint
        && request.host_ref == record.host_ref
        && request.profile_name == record.profile_name
        && request.profile_version == record.profile_version
        && request.requirement == record.requirement
        && resource_request_target_kind(request.target_kind)
            == resource_record_target_kind(record.target_kind)
        && request.target_alias == record.target_alias
        && request.repository_id == record.repository_id
}

/// Admission half: branch, concurrency key/limit, the exclusivity flags and the
/// label/anti-affinity selectors.
pub(crate) fn resource_request_matches_admission(
    request: &ResourceReservationRequest,
    record: &ResourceReservationRecord,
) -> bool {
    request.branch == record.branch
        && request.concurrency_key == record.concurrency_key
        && request.concurrency_limit == record.concurrency_limit
        && request.repository_exclusive == record.repository_exclusive
        && request.branch_exclusive == record.branch_exclusive
        && request.required_labels == record.required_labels
        && request.anti_affinity == record.anti_affinity
        && request.fairness_group == record.fairness_group
}

/// Accounting half: fairness cost, the disk watermarks/policy key and the
/// reservation window.
pub(crate) fn resource_request_matches_accounting(
    request: &ResourceReservationRequest,
    record: &ResourceReservationRecord,
) -> bool {
    request.fairness_cost == record.fairness_cost
        && request.disk_low_watermark_mib == record.disk_low_watermark_mib
        && request.disk_high_watermark_mib == record.disk_high_watermark_mib
        && request.disk_policy_key == record.disk_policy_key
        && request.reserved_at_ms == record.reserved_at_ms
        && request.expires_at_ms == record.expires_at_ms
        && request.expected_host_revision == record.expected_host_revision
}

/// A stored reservation replays a request only when every projected field is
/// identical.  The four halves are evaluated in the original field order, so a
/// mismatch short-circuits at exactly the same field it did before the split.
pub(crate) fn resource_request_matches_record(
    request: &ResourceReservationRequest,
    record: &ResourceReservationRecord,
) -> bool {
    resource_request_matches_identity(request, record)
        && resource_request_matches_placement(request, record)
        && resource_request_matches_admission(request, record)
        && resource_request_matches_accounting(request, record)
}
