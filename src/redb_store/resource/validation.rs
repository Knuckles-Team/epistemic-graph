pub(crate) fn resource_text(value: &str, name: &str) -> Result<(), String> {
    if value.is_empty() || value.len() > MAX_RESOURCE_TEXT {
        return Err(format!(
            "{name} is empty or exceeds {MAX_RESOURCE_TEXT} bytes"
        ));
    }
    if value.chars().any(char::is_control) {
        return Err(format!("{name} contains a control character"));
    }
    Ok(())
}

pub(crate) fn resource_labels(values: &[String], name: &str) -> Result<(), String> {
    if values.len() > MAX_RESOURCE_LABELS {
        return Err(format!("{name} exceeds {MAX_RESOURCE_LABELS} entries"));
    }
    let mut seen = std::collections::HashSet::with_capacity(values.len());
    for value in values {
        resource_text(value, name)?;
        if !seen.insert(value) {
            return Err(format!("{name} contains a duplicate value"));
        }
    }
    Ok(())
}

pub(crate) fn resource_disk_policy_blocked(
    previously_blocked: bool,
    predicted_used_mib: u64,
    low_watermark_mib: Option<u64>,
    high_watermark_mib: Option<u64>,
) -> bool {
    if previously_blocked {
        // RMDD-08 watermarks are USED MiB.  A blocked policy reopens only at
        // or below low; with low==high this branch must not immediately fall
        // through to the open-state high-watermark check.
        low_watermark_mib.is_none_or(|low| predicted_used_mib > low)
    } else {
        high_watermark_mib.is_some_and(|high| predicted_used_mib >= high)
    }
}

pub(crate) fn resource_fingerprint(value: &str, name: &str) -> Result<(), String> {
    resource_text(value, name)?;
    let bytes = value.as_bytes();
    if bytes.len() != 67 || &bytes[..3] != b"v1:" || !bytes[3..].iter().all(u8::is_ascii_hexdigit) {
        return Err(format!("{name} must be v1:<64 lowercase hex characters>"));
    }
    if bytes[3..].iter().any(u8::is_ascii_uppercase) {
        return Err(format!("{name} must use lowercase hex"));
    }
    Ok(())
}

/// Phase 1 of `resource_validate_request`: the identity text fields and the
/// canonical-integer `profile_version`.
pub(crate) fn resource_validate_request_identity(
    request: &ResourceReservationRequest,
) -> Result<(), String> {
    resource_text(&request.tenant_ref, "resource tenant_ref")?;
    resource_text(&request.work_item_id, "resource work_item_id")?;
    resource_text(&request.owner_id, "resource owner_id")?;
    resource_text(&request.fence, "resource fence")?;
    resource_text(&request.reservation_id, "resource reservation_id")?;
    resource_text(&request.profile_name, "resource profile_name")?;
    resource_text(&request.profile_version, "resource profile_version")?;
    let parsed_profile_version = request
        .profile_version
        .parse::<u64>()
        .map_err(|_| "resource profile_version must be a canonical integer".to_string())?;
    if parsed_profile_version.to_string() != request.profile_version {
        return Err("resource profile_version must be a canonical integer".to_string());
    }
    Ok(())
}

/// Phase 2 of `resource_validate_request`: host/target selector, the remaining
/// bounded text fields, the input fingerprint and the label sets.
pub(crate) fn resource_validate_request_selectors(
    request: &ResourceReservationRequest,
) -> Result<(), String> {
    resource_text(&request.host_ref, "resource host_ref")?;
    let target_kind = resource_request_target_kind(request.target_kind);
    resource_text(target_kind, "resource target_kind")?;
    if (target_kind == "local") != request.target_alias.is_none() {
        return Err("resource target_alias does not match target_kind".to_string());
    }
    if let Some(alias) = request.target_alias.as_deref() {
        resource_text(alias, "resource target_alias")?;
    }
    resource_text(&request.repository_id, "resource repository_id")?;
    resource_text(&request.branch, "resource branch")?;
    resource_text(&request.concurrency_key, "resource concurrency_key")?;
    resource_text(&request.fairness_group, "resource fairness_group")?;
    resource_text(&request.disk_policy_key, "resource disk_policy_key")?;
    resource_text(&request.idempotency_key, "resource idempotency_key")?;
    resource_fingerprint(&request.input_fingerprint, "resource input_fingerprint")?;
    resource_labels(&request.required_labels, "resource required_labels")?;
    resource_labels(&request.anti_affinity, "resource anti_affinity")?;
    Ok(())
}

/// The requirement vector is rejected when any dimension is zero or above
/// `MAX_RESOURCE_DIMENSION`.  Verbatim lift of the original disjunction.
pub(crate) fn resource_requirement_dimensions_invalid(requirement: &ResourceRequirement) -> bool {
    requirement.cpu_weight == 0
        || requirement.memory_mib == 0
        || requirement.disk_mib == 0
        || requirement.process_slots == 0
        || requirement.cpu_weight > MAX_RESOURCE_DIMENSION
        || requirement.memory_mib > MAX_RESOURCE_DIMENSION
        || requirement.disk_mib > MAX_RESOURCE_DIMENSION
        || requirement.process_slots > MAX_RESOURCE_DIMENSION
}

/// Phase 3 of `resource_validate_request`: attempt, requirement dimensions,
/// concurrency limit and fairness cost.
pub(crate) fn resource_validate_request_requirement(
    request: &ResourceReservationRequest,
) -> Result<(), String> {
    if request.attempt == 0 {
        return Err("resource attempt must be positive".to_string());
    }
    if resource_requirement_dimensions_invalid(&request.requirement) {
        return Err("resource requirement dimensions must be positive".to_string());
    }
    if request.concurrency_limit.is_some_and(|limit| limit == 0) {
        return Err("resource concurrency_limit must be positive".to_string());
    }
    if request.fairness_cost.checked_add(0).is_none() || request.fairness_cost == 0 {
        return Err("resource fairness_cost must be positive".to_string());
    }
    if request.fairness_cost > MAX_RESOURCE_DIMENSION {
        return Err("resource fairness_cost exceeds the native bound".to_string());
    }
    Ok(())
}

/// Phase 4 of `resource_validate_request`: disk watermark ordering and the
/// reservation window/TTL bound.
pub(crate) fn resource_validate_request_window(
    request: &ResourceReservationRequest,
) -> Result<(), String> {
    if request
        .disk_low_watermark_mib
        .zip(request.disk_high_watermark_mib)
        .is_some_and(|(low, high)| low > high)
    {
        return Err("resource disk low watermark exceeds high watermark".to_string());
    }
    if request.expires_at_ms <= request.reserved_at_ms {
        return Err("resource expiry must be after reservation time".to_string());
    }
    if request.expires_at_ms.saturating_sub(request.reserved_at_ms) > MAX_RESOURCE_TTL_MS {
        return Err("resource TTL exceeds the native bound".to_string());
    }
    Ok(())
}

/// The four phases run in the original statement order, so a doubly-invalid
/// request still reports the first field that was wrong before the split.
pub(crate) fn resource_validate_request(
    request: &ResourceReservationRequest,
) -> Result<(), String> {
    resource_validate_request_identity(request)?;
    resource_validate_request_selectors(request)?;
    resource_validate_request_requirement(request)?;
    resource_validate_request_window(request)?;
    Ok(())
}

pub(crate) fn resource_capacity_sum(
    host: &DurableResourceHost,
    requirement: &ResourceRequirement,
) -> bool {
    host.observed
        .cpu_weight
        .checked_add(host.held_cpu_weight)
        .and_then(|value| value.checked_add(requirement.cpu_weight))
        .is_some_and(|value| value <= host.capacity.cpu_weight)
        && host
            .observed
            .memory_mib
            .checked_add(host.held_memory_mib)
            .and_then(|value| value.checked_add(requirement.memory_mib))
            .is_some_and(|value| value <= host.capacity.memory_mib)
        && host
            .observed
            .disk_mib
            .checked_add(host.held_disk_mib)
            .and_then(|value| value.checked_add(requirement.disk_mib))
            .is_some_and(|value| value <= host.capacity.disk_mib)
        && host
            .observed
            .process_slots
            .checked_add(host.held_process_slots)
            .and_then(|value| value.checked_add(requirement.process_slots))
            .is_some_and(|value| value <= host.capacity.process_slots)
}

struct ResourceStateProjection {
    result: ResourceReservationResultState,
    summary: ResourceReservationSummaryState,
}

fn resource_state_projection(state: ResourceReservationRecordState) -> ResourceStateProjection {
    match state {
        ResourceReservationRecordState::Reserved => ResourceStateProjection {
            result: ResourceReservationResultState::Reserved,
            summary: ResourceReservationSummaryState::Reserved,
        },
        ResourceReservationRecordState::Released => ResourceStateProjection {
            result: ResourceReservationResultState::Released,
            summary: ResourceReservationSummaryState::Released,
        },
        ResourceReservationRecordState::Reclaimed => ResourceStateProjection {
            result: ResourceReservationResultState::Reclaimed,
            summary: ResourceReservationSummaryState::Reclaimed,
        },
        ResourceReservationRecordState::Expired => ResourceStateProjection {
            result: ResourceReservationResultState::Expired,
            summary: ResourceReservationSummaryState::Expired,
        },
        ResourceReservationRecordState::Superseded => ResourceStateProjection {
            result: ResourceReservationResultState::Superseded,
            summary: ResourceReservationSummaryState::Superseded,
        },
        ResourceReservationRecordState::Absent => ResourceStateProjection {
            result: ResourceReservationResultState::Absent,
            summary: ResourceReservationSummaryState::Absent,
        },
    }
}

pub(crate) fn resource_result_state(
    state: ResourceReservationRecordState,
) -> ResourceReservationResultState {
    resource_state_projection(state).result
}

pub(crate) fn resource_summary_state(
    state: ResourceReservationRecordState,
) -> ResourceReservationSummaryState {
    resource_state_projection(state).summary
}

pub(crate) fn resource_result_payload(
    decision: ResourceReservationResultDecision,
    request: &ResourceReservationRequest,
    record: Option<ResourceReservationRecord>,
    host: Option<&DurableResourceHost>,
    fairness_debt: u64,
    changed: Vec<String>,
) -> Result<ResourceReservationResult, String> {
    let (state, lifecycle_revision, tombstone, held) = match record.as_ref() {
        Some(record) => {
            let held = if record.state == ResourceReservationRecordState::Reserved {
                (
                    record.requirement.cpu_weight,
                    record.requirement.memory_mib,
                    record.requirement.disk_mib,
                    record.requirement.process_slots,
                )
            } else {
                (0, 0, 0, 0)
            };
            (
                resource_result_state(record.state),
                record.lifecycle_revision,
                record.tombstone,
                held,
            )
        }
        None => (
            ResourceReservationResultState::Absent,
            0,
            false,
            (0, 0, 0, 0),
        ),
    };
    let host_ref = record
        .as_ref()
        .map(|record| record.host_ref.clone())
        .or_else(|| Some(request.host_ref.clone()));
    let host_revision = host.map_or(0, |host| host.revision);
    Ok(ResourceReservationResult {
        schema_version: ResourceReservationResultSchemaVersion::V1,
        decision,
        reservation_id: Some(record.as_ref().map_or_else(
            || request.reservation_id.clone(),
            |record| record.reservation_id.clone(),
        )),
        work_item_id: request.work_item_id.clone(),
        attempt: record
            .as_ref()
            .map_or(request.attempt, |record| record.attempt),
        lease_epoch: record
            .as_ref()
            .map_or(request.lease_epoch, |record| record.lease_epoch),
        fencing_token: record
            .as_ref()
            .map_or(request.fencing_token, |record| record.fencing_token),
        lifecycle_revision,
        host_ref,
        host_revision,
        record,
        state,
        held_cpu_weight: held.0,
        held_memory_mib: held.1,
        held_disk_mib: held.2,
        held_process_slots: held.3,
        fairness_debt,
        tombstone,
        changed_work_item_ids: changed,
    })
}

pub(crate) fn resource_host_result(
    request: &ResourceHostUpdateRequest,
    host: Option<&DurableResourceHost>,
    policies: &[(String, DurableResourceDiskPolicy)],
    accepted: bool,
    reason: ResourceHostUpdateResultReason,
) -> Result<ResourceHostUpdateResult, String> {
    let host_snapshot = host
        .map(|host| resource_host_update_snapshot(host, policies))
        .transpose()?;
    Ok(ResourceHostUpdateResult {
        schema_version: ResourceHostUpdateResultSchemaVersion::V1,
        accepted,
        reason,
        host_ref: request.host_ref.clone(),
        host_snapshot,
        revision: host.map_or(request.revision, |host| host.revision),
        held_cpu_weight: host.map_or(0, |host| host.held_cpu_weight),
        held_memory_mib: host.map_or(0, |host| host.held_memory_mib),
        held_disk_mib: host.map_or(0, |host| host.held_disk_mib),
        held_process_slots: host.map_or(0, |host| host.held_process_slots),
        draining: host.is_some_and(|host| host.draining),
        quarantined: host.is_some_and(|host| host.quarantined),
    })
}

pub(crate) fn resource_b64_urlsafe(value: &str) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
    let bytes = value.as_bytes();
    let mut encoded = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let first = chunk[0];
        encoded.push(ALPHABET[(first >> 2) as usize] as char);
        if chunk.len() == 1 {
            encoded.push(ALPHABET[((first & 0x03) << 4) as usize] as char);
            encoded.push('=');
            encoded.push('=');
            continue;
        }
        let second = chunk[1];
        encoded.push(ALPHABET[(((first & 0x03) << 4) | (second >> 4)) as usize] as char);
        if chunk.len() == 2 {
            encoded.push(ALPHABET[((second & 0x0f) << 2) as usize] as char);
            encoded.push('=');
            continue;
        }
        let third = chunk[2];
        encoded.push(ALPHABET[(((second & 0x0f) << 2) | (third >> 6)) as usize] as char);
        encoded.push(ALPHABET[(third & 0x3f) as usize] as char);
    }
    let chunks: Vec<String> = encoded
        .as_bytes()
        .chunks(3)
        .map(|chunk| String::from_utf8_lossy(chunk).into_owned())
        .collect();
    format!("opaque:v1:{}", chunks.join("."))
}

pub(crate) fn resource_b64_value(value: &str, name: &str) -> Result<String, String> {
    let original = value.to_string();
    let encoded = value
        .strip_prefix("opaque:v1:")
        .ok_or_else(|| format!("{name} is not an opaque:v1 value"))?
        .replace('.', "");
    if encoded.is_empty() || encoded.len() % 4 != 0 || encoded.len() > 512 {
        return Err(format!("{name} has invalid opaque:v1 length"));
    }
    fn digit(byte: u8) -> Option<u8> {
        match byte {
            b'A'..=b'Z' => Some(byte - b'A'),
            b'a'..=b'z' => Some(byte - b'a' + 26),
            b'0'..=b'9' => Some(byte - b'0' + 52),
            b'-' => Some(62),
            b'_' => Some(63),
            _ => None,
        }
    }
    let bytes = encoded.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len() / 4 * 3);
    for chunk in bytes.chunks_exact(4) {
        let a = digit(chunk[0]).ok_or_else(|| format!("{name} has invalid base64"))?;
        let b = digit(chunk[1]).ok_or_else(|| format!("{name} has invalid base64"))?;
        decoded.push((a << 2) | (b >> 4));
        if chunk[2] != b'=' {
            let c = digit(chunk[2]).ok_or_else(|| format!("{name} has invalid base64"))?;
            decoded.push((b << 4) | (c >> 2));
            if chunk[3] != b'=' {
                let d = digit(chunk[3]).ok_or_else(|| format!("{name} has invalid base64"))?;
                decoded.push((c << 6) | d);
            }
        } else if chunk[3] != b'=' {
            return Err(format!("{name} has invalid base64 padding"));
        }
    }
    let value = String::from_utf8(decoded).map_err(|_| format!("{name} is not UTF-8"))?;
    resource_text(&value, name)?;
    if resource_b64_urlsafe(&value) != original {
        return Err(format!("{name} is not canonical opaque:v1 encoding"));
    }
    Ok(value)
}

pub(crate) fn resource_opaque_string(
    value: Option<&serde_json::Value>,
    name: &str,
) -> Result<Option<String>, String> {
    let Some(value) = value else {
        return Ok(None);
    };
    if value.is_null() {
        return Ok(None);
    }
    let value = value
        .as_str()
        .ok_or_else(|| format!("{name} must be an opaque string or null"))?;
    Ok(Some(resource_b64_value(value, name)?))
}

pub(crate) fn resource_opaque_sequence(
    value: Option<&serde_json::Value>,
    name: &str,
) -> Result<Vec<String>, String> {
    let values = value
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| format!("{name} must be an opaque string array"))?;
    if values.len() > MAX_RESOURCE_LABELS {
        return Err(format!("{name} exceeds {MAX_RESOURCE_LABELS} entries"));
    }
    let mut decoded = Vec::with_capacity(values.len());
    for value in values {
        decoded.push(
            resource_opaque_string(Some(value), name)?
                .ok_or_else(|| format!("{name} contains a null value"))?,
        );
    }
    resource_labels(&decoded, name)?;
    decoded.sort();
    Ok(decoded)
}

pub(crate) type ResourceMetadataMaps<'a> = (
    &'a serde_json::Map<String, serde_json::Value>,
    &'a serde_json::Map<String, serde_json::Value>,
);

pub(crate) fn resource_metadata_maps(
    props: &serde_json::Map<String, serde_json::Value>,
) -> Result<ResourceMetadataMaps<'_>, String> {
    let metadata = props
        .get("metadata")
        .and_then(serde_json::Value::as_object)
        .ok_or_else(|| "WorkItem resource admission metadata is missing".to_string())?;
    let repository = metadata
        .get("repository_work_item")
        .and_then(serde_json::Value::as_object)
        .ok_or_else(|| "WorkItem resource admission extension is missing".to_string())?;
    let resource = repository
        .get("resource_reservation")
        .and_then(serde_json::Value::as_object)
        .ok_or_else(|| "WorkItem resource reservation extension is missing".to_string())?;
    Ok((repository, resource))
}

pub(crate) fn resource_metadata_string(
    map: &serde_json::Map<String, serde_json::Value>,
    key: &str,
    name: &str,
) -> Result<String, String> {
    resource_opaque_string(map.get(key), name)?.ok_or_else(|| format!("{name} is missing"))
}

pub(crate) fn resource_metadata_u64(
    map: &serde_json::Map<String, serde_json::Value>,
    key: &str,
    name: &str,
) -> Result<u64, String> {
    map.get(key)
        .and_then(serde_json::Value::as_u64)
        .ok_or_else(|| format!("{name} is missing or invalid"))
}

#[derive(Debug, Clone)]
pub(crate) struct ResourceWorkItemFence {
    attempt: u64,
    lease_epoch: u64,
    fencing_token: u64,
    superseded: bool,
}

impl ResourceWorkItemFence {
    pub(crate) fn is_superseded(&self) -> bool {
        self.superseded
    }
}

pub(crate) fn resource_expected_fence(fencing_token: u64) -> String {
    // The Repository Manager bridge exposes the engine fencing token as the
    // stable opaque fence string.  Do not accept a caller-invented composite
    // spelling merely because the numeric epoch/token pair happens to match.
    fencing_token.to_string()
}

pub(crate) fn resource_request_target_kind(
    kind: ResourceReservationRequestTargetKind,
) -> &'static str {
    match kind {
        ResourceReservationRequestTargetKind::Local => "local",
        ResourceReservationRequestTargetKind::InventoryAlias => "inventory_alias",
    }
}

pub(crate) fn resource_record_target_kind(
    kind: ResourceReservationRecordTargetKind,
) -> &'static str {
    match kind {
        ResourceReservationRecordTargetKind::Local => "local",
        ResourceReservationRecordTargetKind::InventoryAlias => "inventory_alias",
    }
}

pub(crate) fn resource_host_target_kind(kind: ResourceHostUpdateRequestTargetKind) -> &'static str {
    match kind {
        ResourceHostUpdateRequestTargetKind::Local => "local",
        ResourceHostUpdateRequestTargetKind::InventoryAlias => "inventory_alias",
    }
}

pub(crate) fn resource_snapshot_kind(kind: &str) -> Result<ResourceTargetSnapshotKind, String> {
    match kind {
        "local" => Ok(ResourceTargetSnapshotKind::Local),
        "inventory_alias" => Ok(ResourceTargetSnapshotKind::InventoryAlias),
        _ => Err("resource target kind is invalid".to_string()),
    }
}

pub(crate) fn resource_reservation_snapshot_kind(
    kind: &str,
) -> Result<ResourceReservationHostSnapshotTargetKind, String> {
    match kind {
        "local" => Ok(ResourceReservationHostSnapshotTargetKind::Local),
        "inventory_alias" => Ok(ResourceReservationHostSnapshotTargetKind::InventoryAlias),
        _ => Err("resource host target kind is invalid".to_string()),
    }
}

pub(crate) fn resource_host_update_snapshot_kind(
    kind: &str,
) -> Result<ResourceHostUpdateSnapshotTargetKind, String> {
    match kind {
        "local" => Ok(ResourceHostUpdateSnapshotTargetKind::Local),
        "inventory_alias" => Ok(ResourceHostUpdateSnapshotTargetKind::InventoryAlias),
        _ => Err("resource host target kind is invalid".to_string()),
    }
}

pub(crate) fn resource_opaque_matches(
    map: &serde_json::Map<String, serde_json::Value>,
    key: &str,
    expected: Option<&str>,
    name: &str,
) -> Result<bool, String> {
    let actual = resource_opaque_string(map.get(key), name)?;
    Ok(actual.as_deref() == expected)
}

pub(crate) fn resource_u64_matches(
    map: &serde_json::Map<String, serde_json::Value>,
    key: &str,
    expected: u64,
    name: &str,
) -> Result<bool, String> {
    Ok(resource_metadata_u64(map, key, name)? == expected)
}

pub(crate) fn resource_optional_u64_matches(
    map: &serde_json::Map<String, serde_json::Value>,
    key: &str,
    expected: Option<u64>,
    name: &str,
) -> Result<bool, String> {
    let actual = match map.get(key) {
        Some(value) if !value.is_null() => {
            Some(value.as_u64().ok_or_else(|| format!("{name} is invalid"))?)
        }
        _ => None,
    };
    Ok(actual == expected)
}

pub(crate) fn resource_bool_matches(
    map: &serde_json::Map<String, serde_json::Value>,
    key: &str,
    expected: bool,
    name: &str,
) -> Result<bool, String> {
    Ok(map
        .get(key)
        .and_then(serde_json::Value::as_bool)
        .ok_or_else(|| format!("{name} is missing or invalid"))?
        == expected)
}
