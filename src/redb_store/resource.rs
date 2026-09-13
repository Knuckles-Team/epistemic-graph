//! Native resource reservation: admission, lifecycle, query and status.
//!
//! Every table this module touches is scope-prefixed -- a reservation, a host,
//! an exclusivity key and a fairness debt all lead their key with the graph --
//! so every row access here goes through the graph member's own
//! `open_scoped_table` / `scoped_owner_table` and is confined to that graph by
//! the capability rather than by an argument. There is no file-wide table here,
//! and therefore no control-member access: the nine `resource_*` tables plus
//! `nodes` are all `Serving`.
//!
//! Two consequences of that confinement are visible in the read paths below.
//! A scoped table has no `range(start..)`, so the two prefix scans this module
//! runs -- the tenant index and one host's disk policies -- are
//! `ScopedOwnerTable::scope_rows`, which starts at the graph's least key rather
//! than at the caller's prefix. Both therefore SKIP the rows sorting before the
//! prefix instead of taking the first non-matching row as the end of the scan;
//! their `MAX_RESOURCE_*_SCAN` budgets are unchanged and still bound the work.

use super::*;

use eg_storage::{GraphShardOwner, ScopedRead};

use crate::redb_store::shard::Shard;

// RMDD-27 native reservation bounds.  These are deliberately independent of
// the much larger durable MessagePack budget: reservation strings and status
// scans are public control-plane inputs and must remain cheap to validate.
pub(crate) const MAX_RESOURCE_TEXT: usize = 256;
pub(crate) const MAX_RESOURCE_LABELS: usize = 128;
pub(crate) const MAX_RESOURCE_STATUS_LIMIT: usize = 1_000;
pub(crate) const MAX_RESOURCE_STATUS_SCAN: usize = 100_000;
// ResourceHostUpdate/Status schemas expose at most 128 versioned disk-policy
// rows. Admission uses the same bound before creating a new host+policy key;
// otherwise a native peer could persist a snapshot that generated clients
// cannot decode or force an unbounded policy scan during reconciliation.
pub(crate) const MAX_RESOURCE_HOST_DISK_POLICIES: usize = 128;
// Graph clear/delete is an administrative operation, but its drain check must
// remain bounded in allocation even if a hostile or corrupted graph accumulated
// a large terminal history.  Deletion proceeds in bounded key chunks from an
// in-transaction cursor; the cap is not a lifetime limit on tombstone history.
pub(crate) const MAX_RESOURCE_CLEAR_SCAN: usize = 100_000;
pub(crate) const MAX_RESOURCE_TTL_MS: u64 = 7 * 24 * 60 * 60 * 1_000;
pub(crate) const RESOURCE_HEARTBEAT_GRACE_MS: u64 = 120_000;
pub(crate) const MAX_RESOURCE_DIMENSION: u64 = 1_000_000_000_000;

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

pub(super) fn resource_capacity_sum(
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

pub(crate) fn resource_result_state(
    state: ResourceReservationRecordState,
) -> ResourceReservationResultState {
    match state {
        ResourceReservationRecordState::Reserved => ResourceReservationResultState::Reserved,
        ResourceReservationRecordState::Released => ResourceReservationResultState::Released,
        ResourceReservationRecordState::Reclaimed => ResourceReservationResultState::Reclaimed,
        ResourceReservationRecordState::Expired => ResourceReservationResultState::Expired,
        ResourceReservationRecordState::Superseded => ResourceReservationResultState::Superseded,
        ResourceReservationRecordState::Absent => ResourceReservationResultState::Absent,
    }
}

pub(crate) fn resource_summary_state(
    state: ResourceReservationRecordState,
) -> ResourceReservationSummaryState {
    match state {
        ResourceReservationRecordState::Reserved => ResourceReservationSummaryState::Reserved,
        ResourceReservationRecordState::Released => ResourceReservationSummaryState::Released,
        ResourceReservationRecordState::Reclaimed => ResourceReservationSummaryState::Reclaimed,
        ResourceReservationRecordState::Expired => ResourceReservationSummaryState::Expired,
        ResourceReservationRecordState::Superseded => ResourceReservationSummaryState::Superseded,
        ResourceReservationRecordState::Absent => ResourceReservationSummaryState::Absent,
    }
}

pub(super) fn resource_result_payload(
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

pub(super) fn resource_host_result(
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

/// The four sorted label sets `resource_extension_resolve_labels` returns, in
/// order: the host's advertised labels, the request's required labels, the host's
/// anti-affinity keys, and the request's anti-affinity keys.
pub(crate) type ResourceLabelSets = (Vec<String>, Vec<String>, Vec<String>, Vec<String>);

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
    Ok((labels, request_labels, anti_affinity, request_anti_affinity))
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

#[allow(clippy::type_complexity)]
pub(crate) fn resolve_resource_extension_profile_fields(
    extension: &serde_json::Map<String, serde_json::Value>,
) -> Result<(String, String, String, String, String, String), String> {
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
    Ok((
        profile_version,
        profile_name,
        repository_id,
        concurrency_key,
        fairness_group,
        disk_policy_key,
    ))
}

#[allow(clippy::type_complexity)]
pub(crate) fn resolve_resource_extension_repository_fields(
    repository: &serde_json::Map<String, serde_json::Value>,
) -> Result<(String, String, String, Option<String>, String), String> {
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
    Ok((
        repository_id_outer,
        owner_id_outer,
        outer_target_kind,
        outer_target_alias,
        tenant_id,
    ))
}

#[allow(clippy::type_complexity)]
pub(crate) fn resource_extension_resolve_extracted_fields(
    repository: &serde_json::Map<String, serde_json::Value>,
    extension: &serde_json::Map<String, serde_json::Value>,
) -> Result<
    (
        (String, String, String, String, String, String),
        (String, String, String, Option<String>, String),
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

#[allow(clippy::too_many_arguments)]
pub(crate) fn resource_extension_identity_matches(
    request: &ResourceReservationRequest,
    profile_name: &str,
    profile_version: &str,
    repository_id: &str,
    repository_id_outer: &str,
    owner_id_outer: &str,
    tenant_id: &str,
    extension_branch: &str,
    extension_target_kind: &str,
    outer_target_kind: &str,
    alias: &Option<String>,
    outer_target_alias: &Option<String>,
    concurrency_key: &str,
) -> bool {
    profile_name == request.profile_name
        && profile_version == request.profile_version
        && repository_id == request.repository_id
        && repository_id_outer == request.repository_id
        && owner_id_outer == request.owner_id
        && tenant_id == request.tenant_ref
        && extension_branch == request.branch
        // The nested extension and the outer WorkItem projection must agree on
        // the original execution-target declaration.  The reservation request
        // carries the scheduler's *selected* host target, which may be remote
        // even when this top-level declaration is local with a remote
        // preferred/required policy; that selected pair is checked separately
        // against the host row below.
        && extension_target_kind == outer_target_kind
        && alias.as_deref() == outer_target_alias.as_deref()
        && concurrency_key == request.concurrency_key
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

#[allow(clippy::too_many_arguments)]
pub(crate) fn resource_extension_affinity_and_exclusivity_matches(
    extension: &serde_json::Map<String, serde_json::Value>,
    request: &ResourceReservationRequest,
    labels: &[String],
    request_labels: &[String],
    anti_affinity: &[String],
    request_anti_affinity: &[String],
    fairness_group: &str,
) -> Result<bool, String> {
    Ok(labels == request_labels
        && anti_affinity == request_anti_affinity
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

#[allow(clippy::too_many_arguments)]
pub(crate) fn resource_extension_policy_matches(
    extension: &serde_json::Map<String, serde_json::Value>,
    request: &ResourceReservationRequest,
    labels: &[String],
    request_labels: &[String],
    anti_affinity: &[String],
    request_anti_affinity: &[String],
    fairness_group: &str,
    disk_policy_key: &str,
) -> Result<bool, String> {
    Ok(resource_extension_affinity_and_exclusivity_matches(
        extension,
        request,
        labels,
        request_labels,
        anti_affinity,
        request_anti_affinity,
        fairness_group,
    )? && resource_extension_disk_policy_matches(extension, request, disk_policy_key)?)
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn resource_extension_final_match(
    extension: &serde_json::Map<String, serde_json::Value>,
    request: &ResourceReservationRequest,
    extension_branch: &str,
    labels: &[String],
    request_labels: &[String],
    anti_affinity: &[String],
    request_anti_affinity: &[String],
    alias: &Option<String>,
    profile_fields: &(String, String, String, String, String, String),
    repository_fields: &(String, String, String, Option<String>, String),
    extension_target_kind: &str,
) -> Result<bool, String> {
    let (
        profile_version,
        profile_name,
        repository_id,
        concurrency_key,
        fairness_group,
        disk_policy_key,
    ) = profile_fields;
    let (repository_id_outer, owner_id_outer, outer_target_kind, outer_target_alias, tenant_id) =
        repository_fields;
    let identity_ok = resource_extension_identity_matches(
        request,
        profile_name,
        profile_version,
        repository_id,
        repository_id_outer,
        owner_id_outer,
        tenant_id,
        extension_branch,
        extension_target_kind,
        outer_target_kind,
        alias,
        outer_target_alias,
        concurrency_key,
    );
    let requirements_ok = resource_extension_requirements_match(extension, request)?;
    let policy_ok = resource_extension_policy_matches(
        extension,
        request,
        labels,
        request_labels,
        anti_affinity,
        request_anti_affinity,
        fairness_group,
        disk_policy_key,
    )?;
    Ok(identity_ok && requirements_ok && policy_ok)
}

pub(crate) fn resource_extension_matches(
    repository: &serde_json::Map<String, serde_json::Value>,
    extension: &serde_json::Map<String, serde_json::Value>,
    request: &ResourceReservationRequest,
) -> Result<bool, String> {
    let extension_branch = resource_extension_validate_and_extract_branch(extension, request)?;
    let (labels, request_labels, anti_affinity, request_anti_affinity) =
        resource_extension_resolve_labels(extension, request)?;
    let alias = match resource_extension_resolve_alias_if_digest_matches(repository, extension)? {
        Some(alias) => alias,
        None => return Ok(false),
    };
    let (profile_fields, repository_fields) =
        resource_extension_resolve_extracted_fields(repository, extension)?;
    let extension_target_kind = resolve_resource_extension_target_kind(extension, &alias)?;
    resource_extension_final_match(
        extension,
        request,
        &extension_branch,
        &labels,
        &request_labels,
        &anti_affinity,
        &request_anti_affinity,
        &alias,
        &profile_fields,
        &repository_fields,
        &extension_target_kind,
    )
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
    Ok(ResourceWorkItemFence {
        attempt: current_attempt,
        lease_epoch,
        fencing_token,
        superseded,
    })
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
pub(super) fn resource_target_selection_matches(
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

pub(super) fn resource_selected_target_matches_request(
    request: &ResourceReservationRequest,
    host: &DurableResourceHost,
) -> bool {
    resource_request_target_kind(request.target_kind) == host.target_kind
        && request.target_alias.as_deref() == host.target_alias.as_deref()
}

pub(crate) fn resource_recomputed_fingerprint(
    props: &serde_json::Map<String, serde_json::Value>,
    request: &ResourceReservationRequest,
) -> Result<String, String> {
    use std::collections::BTreeMap;
    let (repository, extension) = resource_metadata_maps(props)?;
    let job_id = resource_metadata_string(repository, "job_id", "repository job_id")?;
    let host_labels =
        resource_opaque_sequence(extension.get("host_labels"), "resource host_labels")?;
    let anti_affinity =
        resource_opaque_sequence(extension.get("anti_affinity"), "resource anti_affinity")?;
    let required_target = match extension.get("required_target") {
        Some(value) if !value.is_null() => Some(resource_target_policy_value(
            Some(value),
            "resource required_target",
        )?),
        _ => None,
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
    let mut resources = BTreeMap::new();
    resources.insert("anti_affinity", serde_json::json!(anti_affinity));
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
    resources.insert("host_labels", serde_json::json!(host_labels));
    resources.insert(
        "memory_mib",
        serde_json::json!(request.requirement.memory_mib),
    );
    resources.insert("preferred_target", preferred_target);
    resources.insert("priority", serde_json::json!(priority));
    resources.insert(
        "process_slots",
        serde_json::json!(request.requirement.process_slots),
    );
    resources.insert("queue_deadline", queue_deadline);
    resources.insert(
        "required_target",
        required_target.map_or(serde_json::Value::Null, |value| value),
    );
    resources.insert("resource_class", serde_json::json!(request.profile_name));
    let mut payload = BTreeMap::new();
    payload.insert("attempt", serde_json::json!(request.attempt));
    payload.insert("branch", serde_json::json!(request.branch));
    payload.insert("fence", serde_json::json!(request.fence));
    payload.insert("job_id", serde_json::json!(job_id));
    payload.insert("owner_id", serde_json::json!(request.owner_id));
    payload.insert("profile", serde_json::json!(request.profile_name));
    // RMDD-08 hashes the resolved registry profile version as an integer.  The
    // wire/record projection retains its bounded string spelling, but the
    // canonical digest must use the frozen numeric JSON form.
    payload.insert("profile_version", serde_json::json!(profile_version_number));
    payload.insert("repository_id", serde_json::json!(request.repository_id));
    payload.insert("reservation_id", serde_json::json!(request.reservation_id));
    payload.insert(
        "resources",
        serde_json::to_value(resources).map_err(|e| e.to_string())?,
    );
    payload.insert("tenant_id", serde_json::json!(request.tenant_ref));
    payload.insert("ttl_seconds", serde_json::json!(ttl_ms / 1_000));
    payload.insert("version", serde_json::json!("v1"));
    payload.insert("work_item_id", serde_json::json!(request.work_item_id));
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

pub(crate) fn resource_encode<T: serde::Serialize>(
    value: &T,
    crypto: DurableCrypto<'_>,
) -> Result<Vec<u8>, String> {
    let bytes = rmp_serde::to_vec_named(value).map_err(|e| e.to_string())?;
    Ok(crypto.seal(&bytes).into_owned())
}

pub(crate) fn resource_decode<T: serde::de::DeserializeOwned>(
    value: &[u8],
    crypto: DurableCrypto<'_>,
) -> Result<T, String> {
    let bytes = crypto.unseal(value)?;
    decode_durable(&bytes)
}

pub(super) fn resource_put_host(
    hosts: &mut eg_storage::ScopedOwnerTableMut<'_, (&str, &str), &[u8]>,
    graph: &str,
    host: &DurableResourceHost,
    crypto: DurableCrypto<'_>,
) -> Result<(), String> {
    let bytes = resource_encode(host, crypto)?;
    hosts.insert((graph, host.host_ref.as_str()), bytes.as_slice())?;
    Ok(())
}

pub(super) fn resource_put_reservation(
    reservations: &mut eg_storage::ScopedOwnerTableMut<'_, (&str, &str), &[u8]>,
    graph: &str,
    reservation: &DurableResourceReservation,
    crypto: DurableCrypto<'_>,
) -> Result<(), String> {
    let bytes = resource_encode(reservation, crypto)?;
    reservations.insert(
        (graph, reservation.record.reservation_id.as_str()),
        bytes.as_slice(),
    )?;
    Ok(())
}

pub(super) fn resource_load_fairness(
    fairness: &mut eg_storage::ScopedOwnerTableMut<'_, (&str, &str), &[u8]>,
    graph: &str,
    tenant: &str,
    group: &str,
    crypto: DurableCrypto<'_>,
) -> Result<DurableResourceFairness, String> {
    let key = resource_fairness_scope_key(tenant, group);
    fairness
        .get((graph, key.as_str()))?
        .map(|row| resource_decode(row.value(), crypto))
        .transpose()
        .map(|value| value.unwrap_or_default())
}

pub(super) fn resource_put_fairness(
    fairness: &mut eg_storage::ScopedOwnerTableMut<'_, (&str, &str), &[u8]>,
    graph: &str,
    tenant: &str,
    group: &str,
    value: &DurableResourceFairness,
    crypto: DurableCrypto<'_>,
) -> Result<(), String> {
    let key = resource_fairness_scope_key(tenant, group);
    let bytes = resource_encode(value, crypto)?;
    fairness.insert((graph, key.as_str()), bytes.as_slice())?;
    Ok(())
}

pub(super) fn resource_load_host(
    hosts: &mut eg_storage::ScopedOwnerTableMut<'_, (&str, &str), &[u8]>,
    graph: &str,
    host_ref: &str,
    crypto: DurableCrypto<'_>,
) -> Result<Option<DurableResourceHost>, String> {
    hosts
        .get((graph, host_ref))?
        .map(|row| resource_decode(row.value(), crypto))
        .transpose()
}

pub(super) fn resource_load_reservation(
    reservations: &mut eg_storage::ScopedOwnerTableMut<'_, (&str, &str), &[u8]>,
    graph: &str,
    reservation_id: &str,
    crypto: DurableCrypto<'_>,
) -> Result<Option<DurableResourceReservation>, String> {
    reservations
        .get((graph, reservation_id))?
        .map(|row| resource_decode(row.value(), crypto))
        .transpose()
}

pub(crate) fn resource_adjust_concurrency(
    concurrency: &mut eg_storage::ScopedOwnerTableMut<'_, (&str, &str), u64>,
    graph: &str,
    key: &str,
    delta: i64,
) -> Result<u64, String> {
    let current = concurrency
        .get((graph, key))?
        .map(|value| value.value())
        .unwrap_or(0);
    let next = if delta >= 0 {
        current
            .checked_add(delta as u64)
            .ok_or_else(|| "resource concurrency counter overflow".to_string())?
    } else {
        current
            .checked_sub(delta.unsigned_abs())
            .ok_or_else(|| "resource concurrency counter underflow".to_string())?
    };
    if next == 0 {
        concurrency.remove((graph, key))?;
    } else {
        concurrency.insert((graph, key), next)?;
    }
    Ok(next)
}

pub(crate) fn resource_adjust_anti_affinity(
    anti_affinity: &mut eg_storage::ScopedOwnerTableMut<'_, (&str, &str, &str), u64>,
    graph: &str,
    host_ref: &str,
    tag: &str,
    delta: i64,
) -> Result<u64, String> {
    let current = anti_affinity
        .get((graph, host_ref, tag))?
        .map(|value| value.value())
        .unwrap_or(0);
    let next = if delta >= 0 {
        current
            .checked_add(delta as u64)
            .ok_or_else(|| "resource anti-affinity counter overflow".to_string())?
    } else {
        current
            .checked_sub(delta.unsigned_abs())
            .ok_or_else(|| "resource anti-affinity counter underflow".to_string())?
    };
    if next == 0 {
        anti_affinity.remove((graph, host_ref, tag))?;
    } else {
        anti_affinity.insert((graph, host_ref, tag), next)?;
    }
    Ok(next)
}

pub(crate) fn resource_exclusivity_keys(request: &ResourceReservationRequest) -> Vec<String> {
    let mut keys = Vec::with_capacity(2);
    if request.repository_exclusive {
        keys.push(resource_scope_key(&[
            "tenant",
            &request.tenant_ref,
            "repository",
            &request.repository_id,
        ]));
    }
    if request.branch_exclusive {
        keys.push(resource_scope_key(&[
            "tenant",
            &request.tenant_ref,
            "branch",
            &request.repository_id,
            &request.branch,
        ]));
    }
    keys
}

/// Composite native index keys use a reserved NUL separator. Every component
/// has already passed `resource_text`, which rejects NUL/control characters,
/// making tenant/scope boundaries unambiguous instead of relying on a caller's
/// arbitrary spelling.
pub(crate) fn resource_scope_key(parts: &[&str]) -> String {
    parts.join("\0")
}

/// Concurrency keys are explicit global scheduler scope.  The prefix prevents
/// an untrusted caller from colliding with future tenant-scoped namespaces while
/// preserving one exact counter across tenants for a shared host profile.
pub(crate) fn resource_concurrency_scope_key(key: &str) -> String {
    resource_scope_key(&["global", key])
}

pub(crate) fn resource_fairness_scope_key(tenant: &str, group: &str) -> String {
    resource_scope_key(&[tenant, group])
}

pub(crate) fn resource_record_target_kind_from_request(
    kind: ResourceReservationRequestTargetKind,
) -> ResourceReservationRecordTargetKind {
    match kind {
        ResourceReservationRequestTargetKind::Local => ResourceReservationRecordTargetKind::Local,
        ResourceReservationRequestTargetKind::InventoryAlias => {
            ResourceReservationRecordTargetKind::InventoryAlias
        }
    }
}

pub(super) fn resource_build_record(
    request: &ResourceReservationRequest,
    host: &DurableResourceHost,
    revision: u64,
    lifecycle_revision: u64,
) -> Result<ResourceReservationRecord, String> {
    let mut labels = host.labels.clone();
    labels.sort();
    Ok(ResourceReservationRecord {
        reservation_id: request.reservation_id.clone(),
        tenant_ref: request.tenant_ref.clone(),
        owner_id: request.owner_id.clone(),
        work_item_id: request.work_item_id.clone(),
        fence: request.fence.clone(),
        attempt: request.attempt,
        lease_epoch: request.lease_epoch,
        fencing_token: request.fencing_token,
        input_fingerprint: request.input_fingerprint.clone(),
        host_ref: request.host_ref.clone(),
        profile_name: request.profile_name.clone(),
        profile_version: request.profile_version.clone(),
        requirement: request.requirement.clone(),
        capacity_snapshot: ResourceCapacitySnapshot {
            cpu_weight: host.capacity.cpu_weight,
            memory_mib: host.capacity.memory_mib,
            disk_mib: host.capacity.disk_mib,
            process_slots: host.capacity.process_slots,
            host_revision: host.revision,
        },
        selected_target: ResourceTargetSnapshot {
            kind: resource_snapshot_kind(&host.target_kind)?,
            alias: host.target_alias.clone(),
            capability_labels: labels,
        },
        target_kind: resource_record_target_kind_from_request(request.target_kind),
        target_alias: request.target_alias.clone(),
        repository_id: request.repository_id.clone(),
        branch: request.branch.clone(),
        concurrency_key: request.concurrency_key.clone(),
        concurrency_limit: request.concurrency_limit,
        repository_exclusive: request.repository_exclusive,
        branch_exclusive: request.branch_exclusive,
        required_labels: request.required_labels.clone(),
        anti_affinity: request.anti_affinity.clone(),
        fairness_group: request.fairness_group.clone(),
        fairness_cost: request.fairness_cost,
        disk_low_watermark_mib: request.disk_low_watermark_mib,
        disk_high_watermark_mib: request.disk_high_watermark_mib,
        disk_policy_key: request.disk_policy_key.clone(),
        reserved_at_ms: request.reserved_at_ms,
        expires_at_ms: request.expires_at_ms,
        expected_host_revision: request.expected_host_revision,
        // Lifecycle CAS is an operation input, not immutable admission
        // identity. Reserve creation accepts only absent/zero; release and
        // reclaim record the successful precondition on their tombstone below
        // so an exact lifecycle retry can be distinguished from a changed one.
        expected_lifecycle_revision: None,
        state: ResourceReservationRecordState::Reserved,
        revision,
        lifecycle_revision,
        tombstone: false,
    })
}

pub(super) fn resource_validate_host_freshness(
    host: &DurableResourceHost,
    now_ms: u64,
) -> ResourceReservationResultDecision {
    if host.heartbeat_at_ms > now_ms {
        return ResourceReservationResultDecision::StaleHost;
    }
    if now_ms.saturating_sub(host.heartbeat_at_ms) > host.heartbeat_ttl_ms {
        return ResourceReservationResultDecision::StaleHost;
    }
    if host.draining {
        return ResourceReservationResultDecision::Drained;
    }
    if host.quarantined {
        return ResourceReservationResultDecision::Quarantined;
    }
    ResourceReservationResultDecision::Accepted
}

/// Apply reserve/release/reclaim/host-update through the graph member's already-open
/// owner-row admission. Every read and index update below is part of that one
/// admitted group's single transaction; no scheduler mirror or second CAS
/// participates, and every table handle is bounded to `graph` by the capability
/// that opened it.
#[allow(clippy::too_many_arguments)]
pub(crate) fn apply_resource_reservation_rows(
    graph: &str,
    method: &Method,
    nodes: &mut eg_storage::ScopedOwnerTableMut<'_, (&str, &str), &[u8]>,
    reservations: &mut eg_storage::ScopedOwnerTableMut<'_, (&str, &str), &[u8]>,
    tenant_index: &mut eg_storage::ScopedOwnerTableMut<'_, (&str, &str, &str), &str>,
    attempts: &mut eg_storage::ScopedOwnerTableMut<'_, (&str, &str, u64), &str>,
    hosts: &mut eg_storage::ScopedOwnerTableMut<'_, (&str, &str), &[u8]>,
    exclusivity: &mut eg_storage::ScopedOwnerTableMut<'_, (&str, &str), &str>,
    fairness: &mut eg_storage::ScopedOwnerTableMut<'_, (&str, &str), &[u8]>,
    concurrency: &mut eg_storage::ScopedOwnerTableMut<'_, (&str, &str), u64>,
    anti_affinity: &mut eg_storage::ScopedOwnerTableMut<'_, (&str, &str, &str), u64>,
    disk_policies: &mut eg_storage::ScopedOwnerTableMut<'_, (&str, &str), &[u8]>,
    crypto: DurableCrypto<'_>,
) -> Result<Option<crate::protocol::ResultPayload>, String> {
    // CX-EG-05 (CCN 186 -> decomposed): the two match arms below are each a
    // literal, behaviour-preserving relocation of the original arm body into
    // its own named function (see immediately after this function).
    match method {
        Method::UpdateResourceHost { request } => {
            apply_update_resource_host_rows(request, graph, hosts, disk_policies, crypto)?
                .map(
                    crate::protocol::ResultPayload::of::<
                        eg_types::result_contract::coordination::UpdateResourceHost,
                    >,
                )
                .transpose()
        }
        Method::ReserveWorkItemResources { request }
        | Method::ReleaseWorkItemResources { request }
        | Method::ReclaimWorkItemResources { request } => {
            apply_resource_reservation_lifecycle_rows(
                graph,
                method,
                request,
                nodes,
                reservations,
                tenant_index,
                attempts,
                hosts,
                exclusivity,
                fairness,
                concurrency,
                anti_affinity,
                disk_policies,
                crypto,
            )?
            .map(|result| resource_reservation_payload(method, result))
            .transpose()
        }
        _ => Ok(None),
    }
}

/// Encode a reservation lifecycle result as the declared result of the method that
/// produced it.
fn resource_reservation_payload(
    method: &Method,
    result: ResourceReservationResult,
) -> Result<crate::protocol::ResultPayload, String> {
    match method {
        Method::ReserveWorkItemResources { .. } => crate::protocol::ResultPayload::of::<
            eg_types::result_contract::coordination::ReserveWorkItemResources,
        >(result),
        Method::ReleaseWorkItemResources { .. } => crate::protocol::ResultPayload::of::<
            eg_types::result_contract::coordination::ReleaseWorkItemResources,
        >(result),
        Method::ReclaimWorkItemResources { .. } => crate::protocol::ResultPayload::of::<
            eg_types::result_contract::coordination::ReclaimWorkItemResources,
        >(result),
        _ => Err("resource reservation result for a non-reservation method".to_string()),
    }
}

pub(crate) fn validate_resource_host_update_identity(
    request: &ResourceHostUpdateRequest,
) -> Result<(), String> {
    resource_text(&request.tenant_ref, "resource host tenant_ref")?;
    resource_text(&request.host_ref, "resource host_ref")?;
    resource_labels(&request.labels, "resource host labels")?;
    resource_text(
        request.target_alias.as_deref().unwrap_or("local"),
        "resource host target_alias",
    )?;
    Ok(())
}

pub(crate) fn resolve_resource_host_update_target_kind(
    request: &ResourceHostUpdateRequest,
) -> Result<&'static str, String> {
    let target_kind = resource_host_target_kind(request.target_kind);
    if (target_kind == "local") != request.target_alias.is_none() {
        return Err("resource host target_alias does not match target_kind".into());
    }
    Ok(target_kind)
}

pub(crate) fn resource_host_heartbeat_bounds_violated(request: &ResourceHostUpdateRequest) -> bool {
    request.heartbeat_ttl_ms < 1_000
        || request.heartbeat_ttl_ms > 86_400_000
        || request.heartbeat_at_ms > request.now_ms
        || request.now_ms.saturating_sub(request.heartbeat_at_ms) > request.heartbeat_ttl_ms
}

pub(crate) fn resource_host_capacity_bounds_violated(request: &ResourceHostUpdateRequest) -> bool {
    request.capacity.cpu_weight == 0
        || request.capacity.memory_mib == 0
        || request.capacity.disk_mib == 0
        || request.capacity.process_slots == 0
        || request.capacity.cpu_weight > MAX_RESOURCE_DIMENSION
        || request.capacity.memory_mib > MAX_RESOURCE_DIMENSION
        || request.capacity.disk_mib > MAX_RESOURCE_DIMENSION
        || request.capacity.process_slots > MAX_RESOURCE_DIMENSION
}

pub(crate) fn resource_host_disk_bounds_violated(request: &ResourceHostUpdateRequest) -> bool {
    request.disk_used_mib > request.disk_capacity_mib
        || request.disk_capacity_mib == 0
        || request.disk_capacity_mib > MAX_RESOURCE_DIMENSION
        || request.disk_used_mib > MAX_RESOURCE_DIMENSION
}

pub(crate) fn resource_host_observed_bounds_violated(request: &ResourceHostUpdateRequest) -> bool {
    request.observed.cpu_weight > request.capacity.cpu_weight
        || request.observed.memory_mib > request.capacity.memory_mib
        || request.observed.disk_mib > request.capacity.disk_mib
        || request.observed.process_slots > request.capacity.process_slots
        || request.observed.cpu_weight > MAX_RESOURCE_DIMENSION
        || request.observed.memory_mib > MAX_RESOURCE_DIMENSION
        || request.observed.disk_mib > MAX_RESOURCE_DIMENSION
        || request.observed.process_slots > MAX_RESOURCE_DIMENSION
}

pub(crate) fn validate_resource_host_update_telemetry_bounds(
    request: &ResourceHostUpdateRequest,
) -> Result<(), String> {
    if resource_host_heartbeat_bounds_violated(request)
        || resource_host_capacity_bounds_violated(request)
        || resource_host_disk_bounds_violated(request)
        || resource_host_observed_bounds_violated(request)
        || request.revision == 0
    {
        return Err("resource host update violates telemetry bounds".into());
    }
    Ok(())
}

pub(crate) fn validate_resource_host_update_request(
    request: &ResourceHostUpdateRequest,
) -> Result<&'static str, String> {
    validate_resource_host_update_identity(request)?;
    let target_kind = resolve_resource_host_update_target_kind(request)?;
    validate_resource_host_update_telemetry_bounds(request)?;
    Ok(target_kind)
}

pub(super) fn resource_host_update_exceeds_capacity(
    request: &ResourceHostUpdateRequest,
    host: &DurableResourceHost,
) -> bool {
    request
        .observed
        .cpu_weight
        .checked_add(host.held_cpu_weight)
        .is_none_or(|value| value > request.capacity.cpu_weight)
        || request
            .observed
            .memory_mib
            .checked_add(host.held_memory_mib)
            .is_none_or(|value| value > request.capacity.memory_mib)
        || request
            .observed
            .disk_mib
            .checked_add(host.held_disk_mib)
            .is_none_or(|value| value > request.capacity.disk_mib)
        || request
            .disk_used_mib
            .checked_add(host.held_disk_mib)
            .is_none_or(|value| value > request.disk_capacity_mib)
        || request
            .observed
            .process_slots
            .checked_add(host.held_process_slots)
            .is_none_or(|value| value > request.capacity.process_slots)
}

pub(super) fn check_resource_host_update_conflicts(
    request: &ResourceHostUpdateRequest,
    host: &DurableResourceHost,
    target_kind: &str,
    policy_rows: &[(String, DurableResourceDiskPolicy)],
) -> Result<Option<ResourceHostUpdateResult>, String> {
    if host.target_kind != target_kind || host.target_alias != request.target_alias {
        return Ok(Some(resource_host_result(
            request,
            Some(host),
            policy_rows,
            false,
            ResourceHostUpdateResultReason::Conflict,
        )?));
    }
    if request.revision <= host.revision {
        return Ok(Some(resource_host_result(
            request,
            Some(host),
            policy_rows,
            false,
            ResourceHostUpdateResultReason::StaleHost,
        )?));
    }
    if resource_host_update_exceeds_capacity(request, host) {
        return Ok(Some(resource_host_result(
            request,
            Some(host),
            policy_rows,
            false,
            ResourceHostUpdateResultReason::Conflict,
        )?));
    }
    Ok(None)
}

pub(super) fn build_resource_host_from_update(
    request: &ResourceHostUpdateRequest,
    current: Option<&DurableResourceHost>,
    target_kind: &str,
) -> DurableResourceHost {
    DurableResourceHost {
        // Physical host accounting is graph-scoped and shared across
        // tenants.  Preserve the first controller's provenance label;
        // authz on UpdateResourceHost, not this label, controls who may
        // publish telemetry.
        tenant_ref: current.map_or_else(
            || request.tenant_ref.clone(),
            |host| host.tenant_ref.clone(),
        ),
        host_ref: request.host_ref.clone(),
        revision: request.revision,
        capacity: request.capacity.clone(),
        observed: request.observed.clone(),
        heartbeat_at_ms: request.heartbeat_at_ms,
        heartbeat_ttl_ms: request.heartbeat_ttl_ms,
        now_ms: request.now_ms,
        draining: request.draining,
        quarantined: request.quarantined,
        labels: request.labels.clone(),
        target_kind: target_kind.to_string(),
        target_alias: request.target_alias.clone(),
        disk_used_mib: request.disk_used_mib,
        disk_capacity_mib: request.disk_capacity_mib,
        held_cpu_weight: current.map_or(0, |h| h.held_cpu_weight),
        held_memory_mib: current.map_or(0, |h| h.held_memory_mib),
        held_disk_mib: current.map_or(0, |h| h.held_disk_mib),
        held_process_slots: current.map_or(0, |h| h.held_process_slots),
    }
}

pub(crate) fn apply_update_resource_host_rows(
    request: &ResourceHostUpdateRequest,
    graph: &str,
    hosts: &mut eg_storage::ScopedOwnerTableMut<'_, (&str, &str), &[u8]>,
    disk_policies: &mut eg_storage::ScopedOwnerTableMut<'_, (&str, &str), &[u8]>,
    crypto: DurableCrypto<'_>,
) -> Result<Option<ResourceHostUpdateResult>, String> {
    let target_kind = validate_resource_host_update_request(request)?;
    let current = resource_load_host(hosts, graph, &request.host_ref, crypto)?;
    let policy_rows =
        resource_collect_disk_policy_rows(disk_policies, graph, &request.host_ref, crypto)?;
    if let Some(host) = current.as_ref() {
        if let Some(payload) =
            check_resource_host_update_conflicts(request, host, target_kind, &policy_rows)?
        {
            return Ok(Some(payload));
        }
    }
    let host = build_resource_host_from_update(request, current.as_ref(), target_kind);
    resource_put_host(hosts, graph, &host, crypto)?;
    Ok(Some(resource_host_result(
        request,
        Some(&host),
        &policy_rows,
        true,
        ResourceHostUpdateResultReason::Accepted,
    )?))
}

/// Early-return signal used while decomposing `apply_resource_reservation_lifecycle_rows`
/// into phase functions: `Continue(v)` carries the phase's output forward to the next
/// phase; `Return(payload)` means the phase already produced the function's final result
/// and every caller must stop and return it unchanged.
pub(crate) enum ReservationLifecycleStep<T> {
    Continue(T),
    Return(Box<ResourceReservationResult>),
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn apply_resource_reservation_lifecycle_rows(
    graph: &str,
    method: &Method,
    request: &ResourceReservationRequest,
    nodes: &mut eg_storage::ScopedOwnerTableMut<'_, (&str, &str), &[u8]>,
    reservations: &mut eg_storage::ScopedOwnerTableMut<'_, (&str, &str), &[u8]>,
    tenant_index: &mut eg_storage::ScopedOwnerTableMut<'_, (&str, &str, &str), &str>,
    attempts: &mut eg_storage::ScopedOwnerTableMut<'_, (&str, &str, u64), &str>,
    hosts: &mut eg_storage::ScopedOwnerTableMut<'_, (&str, &str), &[u8]>,
    exclusivity: &mut eg_storage::ScopedOwnerTableMut<'_, (&str, &str), &str>,
    fairness: &mut eg_storage::ScopedOwnerTableMut<'_, (&str, &str), &[u8]>,
    concurrency: &mut eg_storage::ScopedOwnerTableMut<'_, (&str, &str), u64>,
    anti_affinity: &mut eg_storage::ScopedOwnerTableMut<'_, (&str, &str, &str), u64>,
    disk_policies: &mut eg_storage::ScopedOwnerTableMut<'_, (&str, &str), &[u8]>,
    crypto: DurableCrypto<'_>,
) -> Result<Option<ResourceReservationResult>, String> {
    let (is_reserve, is_reclaim, existing, props, work_item_fence) =
        match resource_lifecycle_precheck_and_load_work_item(
            method,
            request,
            reservations,
            hosts,
            nodes,
            graph,
            crypto,
        )? {
            ReservationLifecycleStep::Return(payload) => return Ok(Some(*payload)),
            ReservationLifecycleStep::Continue(value) => value,
        };
    let extension = match resource_validate_work_item_status_and_extension(
        request,
        is_reserve,
        is_reclaim,
        &work_item_fence,
        &props,
    )? {
        ReservationLifecycleStep::Return(payload) => return Ok(Some(*payload)),
        ReservationLifecycleStep::Continue(value) => value,
    };
    if let Some(payload) = resource_commit_release_or_reclaim_or_reserve_gate(
        graph,
        request,
        existing.as_ref(),
        is_reserve,
        is_reclaim,
        &work_item_fence,
        &props,
        hosts,
        reservations,
        fairness,
        concurrency,
        anti_affinity,
        exclusivity,
        disk_policies,
        crypto,
    )? {
        return Ok(Some(payload));
    }
    let host = match resource_admit_reserve_host_with_winner_check(
        attempts,
        reservations,
        hosts,
        disk_policies,
        anti_affinity,
        concurrency,
        exclusivity,
        graph,
        request,
        extension,
        crypto,
    )? {
        ReservationLifecycleStep::Return(payload) => return Ok(Some(*payload)),
        ReservationLifecycleStep::Continue(value) => value,
    };
    resource_commit_reserve_admission(
        graph,
        request,
        host,
        hosts,
        reservations,
        tenant_index,
        attempts,
        exclusivity,
        concurrency,
        anti_affinity,
        fairness,
        crypto,
    )
}

/// Thin sequencing wrapper: runs Phase 1 (`resource_lifecycle_precheck`) then Phase 2
/// (`resource_load_and_validate_work_item`) back to back, so the orchestrator has a
/// single call/match site for "validate the request and load both the existing
/// reservation (if any) and the WorkItem row." No behaviour is added; this is pure
/// call-site consolidation (see CX-EG-05's finding that a `?` after a call counts as
/// a branch under this repo's complexity gate the same as an `if`, so flattening N
/// sequential fallible calls into fewer named steps is what brings the caller's own
/// CCN down, not simplifying any individual step).
/// What Phases 1+2 hand the reservation orchestrator, in order: is-reserve,
/// is-reclaim, the existing reservation (if any), the WorkItem's properties, and
/// its lease fence.
pub(super) type ResourceLifecyclePrelude = (
    bool,
    bool,
    Option<DurableResourceReservation>,
    serde_json::Map<String, serde_json::Value>,
    ResourceWorkItemFence,
);

#[allow(clippy::too_many_arguments)]
pub(super) fn resource_lifecycle_precheck_and_load_work_item(
    method: &Method,
    request: &ResourceReservationRequest,
    reservations: &mut eg_storage::ScopedOwnerTableMut<'_, (&str, &str), &[u8]>,
    hosts: &mut eg_storage::ScopedOwnerTableMut<'_, (&str, &str), &[u8]>,
    nodes: &mut eg_storage::ScopedOwnerTableMut<'_, (&str, &str), &[u8]>,
    graph: &str,
    crypto: DurableCrypto<'_>,
) -> Result<ReservationLifecycleStep<ResourceLifecyclePrelude>, String> {
    let (is_reserve, is_reclaim, existing) =
        match resource_lifecycle_precheck(method, request, reservations, hosts, graph, crypto)? {
            ReservationLifecycleStep::Return(payload) => {
                return Ok(ReservationLifecycleStep::Return(payload));
            }
            ReservationLifecycleStep::Continue(value) => value,
        };
    let (props, work_item_fence) =
        match resource_load_and_validate_work_item(nodes, graph, request, is_reclaim, crypto)? {
            ReservationLifecycleStep::Return(payload) => {
                return Ok(ReservationLifecycleStep::Return(payload));
            }
            ReservationLifecycleStep::Continue(value) => value,
        };
    Ok(ReservationLifecycleStep::Continue((
        is_reserve,
        is_reclaim,
        existing,
        props,
        work_item_fence,
    )))
}

/// Thin sequencing wrapper: runs Phase 4 (`resource_commit_release_or_reclaim`) and,
/// only when it declined to decide (no existing reservation row), applies the
/// `!is_reserve -> NotFound` fallback that immediately followed it in the original
/// function. Pure call-site consolidation, no behaviour change.
#[allow(clippy::too_many_arguments)]
pub(super) fn resource_commit_release_or_reclaim_or_reserve_gate(
    graph: &str,
    request: &ResourceReservationRequest,
    existing: Option<&DurableResourceReservation>,
    is_reserve: bool,
    is_reclaim: bool,
    work_item_fence: &ResourceWorkItemFence,
    props: &serde_json::Map<String, serde_json::Value>,
    hosts: &mut eg_storage::ScopedOwnerTableMut<'_, (&str, &str), &[u8]>,
    reservations: &mut eg_storage::ScopedOwnerTableMut<'_, (&str, &str), &[u8]>,
    fairness: &mut eg_storage::ScopedOwnerTableMut<'_, (&str, &str), &[u8]>,
    concurrency: &mut eg_storage::ScopedOwnerTableMut<'_, (&str, &str), u64>,
    anti_affinity: &mut eg_storage::ScopedOwnerTableMut<'_, (&str, &str, &str), u64>,
    exclusivity: &mut eg_storage::ScopedOwnerTableMut<'_, (&str, &str), &str>,
    disk_policies: &mut eg_storage::ScopedOwnerTableMut<'_, (&str, &str), &[u8]>,
    crypto: DurableCrypto<'_>,
) -> Result<Option<ResourceReservationResult>, String> {
    if let Some(payload) = resource_commit_release_or_reclaim(
        graph,
        request,
        existing,
        is_reserve,
        is_reclaim,
        work_item_fence,
        props,
        hosts,
        reservations,
        fairness,
        concurrency,
        anti_affinity,
        exclusivity,
        disk_policies,
        crypto,
    )? {
        return Ok(Some(payload));
    }
    if !is_reserve {
        return Ok(Some(resource_result_payload(
            ResourceReservationResultDecision::NotFound,
            request,
            None,
            None,
            0,
            vec![],
        )?));
    }
    Ok(None)
}

/// Thin sequencing wrapper: runs Phase 5 (`resource_check_attempt_winner_conflict`)
/// then, only if it did not already decide the request, Phase 6
/// (`resource_admit_reserve_host`). Pure call-site consolidation, no behaviour change.
#[allow(clippy::too_many_arguments)]
pub(super) fn resource_admit_reserve_host_with_winner_check(
    attempts: &mut eg_storage::ScopedOwnerTableMut<'_, (&str, &str, u64), &str>,
    reservations: &mut eg_storage::ScopedOwnerTableMut<'_, (&str, &str), &[u8]>,
    hosts: &mut eg_storage::ScopedOwnerTableMut<'_, (&str, &str), &[u8]>,
    disk_policies: &mut eg_storage::ScopedOwnerTableMut<'_, (&str, &str), &[u8]>,
    anti_affinity: &mut eg_storage::ScopedOwnerTableMut<'_, (&str, &str, &str), u64>,
    concurrency: &mut eg_storage::ScopedOwnerTableMut<'_, (&str, &str), u64>,
    exclusivity: &mut eg_storage::ScopedOwnerTableMut<'_, (&str, &str), &str>,
    graph: &str,
    request: &ResourceReservationRequest,
    extension: &serde_json::Map<String, serde_json::Value>,
    crypto: DurableCrypto<'_>,
) -> Result<ReservationLifecycleStep<DurableResourceHost>, String> {
    if let Some(payload) =
        resource_check_attempt_winner_conflict(attempts, reservations, graph, request, crypto)?
    {
        return Ok(ReservationLifecycleStep::Return(payload.into()));
    }
    resource_admit_reserve_host(
        hosts,
        disk_policies,
        anti_affinity,
        concurrency,
        exclusivity,
        graph,
        request,
        extension,
        crypto,
    )
}

/// The two reserve-only window preconditions.
///
/// A creation has no prior lifecycle revision to satisfy: accept only the
/// explicit zero/absent form, since a positive caller precondition must not be
/// silently persisted or bypassed by a reserve replay.  The reservation window
/// must also contain `now`.
pub(crate) fn resource_reserve_window_precheck(
    request: &ResourceReservationRequest,
) -> Result<Option<ResourceReservationResult>, String> {
    if request
        .expected_lifecycle_revision
        .is_some_and(|revision| revision != 0)
    {
        return Ok(Some(resource_result_payload(
            ResourceReservationResultDecision::InputConflict,
            request,
            None,
            None,
            0,
            vec![],
        )?));
    }
    if request.now_ms < request.reserved_at_ms || request.now_ms >= request.expires_at_ms {
        return Ok(Some(resource_result_payload(
            ResourceReservationResultDecision::Policy,
            request,
            None,
            None,
            0,
            vec![],
        )?));
    }
    Ok(None)
}

/// The `expected_lifecycle_revision` precondition of a release/reclaim.
///
/// A live row is matched against its current revision; a terminal replay carries
/// the precondition captured by the successful lifecycle mutation, which keeps an
/// exact retry idempotent while refusing a changed precondition after the row is
/// tombstoned.  The refusal decision differs accordingly (`Stale` vs
/// `InputConflict`).
pub(super) fn resource_lifecycle_revision_precheck(
    request: &ResourceReservationRequest,
    stored: &DurableResourceReservation,
) -> Result<Option<ResourceReservationResult>, String> {
    let reserved = stored.record.state == ResourceReservationRecordState::Reserved;
    let lifecycle_matches = if reserved {
        request.expected_lifecycle_revision == Some(stored.record.lifecycle_revision)
    } else {
        request.expected_lifecycle_revision == stored.record.expected_lifecycle_revision
    };
    if lifecycle_matches {
        return Ok(None);
    }
    Ok(Some(resource_result_payload(
        if reserved {
            ResourceReservationResultDecision::Stale
        } else {
            ResourceReservationResultDecision::InputConflict
        },
        request,
        Some(stored.record.clone()),
        None,
        stored.fairness_debt,
        vec![],
    )?))
}

/// The idempotency-precondition pass over an existing reservation row: tenant
/// match, full immutable record match, the release/reclaim lifecycle-revision
/// precondition, and the terminal-replay short-circuit.  `Ok(Some(..))` decides
/// the request; `Ok(None)` lets the lifecycle continue.
pub(super) fn resource_existing_reservation_precheck(
    request: &ResourceReservationRequest,
    stored: &DurableResourceReservation,
    is_reserve: bool,
    graph: &str,
    hosts: &mut eg_storage::ScopedOwnerTableMut<'_, (&str, &str), &[u8]>,
    crypto: DurableCrypto<'_>,
) -> Result<Option<ResourceReservationResult>, String> {
    if stored.record.tenant_ref != request.tenant_ref {
        return Ok(Some(resource_result_payload(
            ResourceReservationResultDecision::Conflict,
            request,
            None,
            None,
            0,
            vec![],
        )?));
    }
    if !resource_request_matches_record(request, &stored.record) {
        return Ok(Some(resource_result_payload(
            ResourceReservationResultDecision::InputConflict,
            request,
            None,
            None,
            stored.fairness_debt,
            vec![],
        )?));
    }
    if !is_reserve {
        if let Some(payload) = resource_lifecycle_revision_precheck(request, stored)? {
            return Ok(Some(payload));
        }
    }
    if stored.record.state != ResourceReservationRecordState::Reserved {
        let host = resource_load_host(hosts, graph, &stored.record.host_ref, crypto)?;
        return Ok(Some(resource_result_payload(
            ResourceReservationResultDecision::Idempotent,
            request,
            Some(stored.record.clone()),
            host.as_ref(),
            stored.fairness_debt,
            vec![],
        )?));
    }
    Ok(None)
}

/// Phase 1: TTL/precondition/window validation, plus the FIRST idempotency-precondition
/// pass over any existing reservation row (tenant match, request match, the
/// release/reclaim `expected_lifecycle_revision` precondition, and the terminal-replay
/// short-circuit). Literal relocation of the original function's first ~110 lines;
/// no branch was added, removed, or reordered.
pub(super) fn resource_lifecycle_precheck(
    method: &Method,
    request: &ResourceReservationRequest,
    reservations: &mut eg_storage::ScopedOwnerTableMut<'_, (&str, &str), &[u8]>,
    hosts: &mut eg_storage::ScopedOwnerTableMut<'_, (&str, &str), &[u8]>,
    graph: &str,
    crypto: DurableCrypto<'_>,
) -> Result<ReservationLifecycleStep<(bool, bool, Option<DurableResourceReservation>)>, String> {
    resource_validate_request(request)?;
    if request.expires_at_ms <= request.reserved_at_ms
        || request.expires_at_ms.saturating_sub(request.reserved_at_ms) > MAX_RESOURCE_TTL_MS
    {
        return Err("resource TTL violates the native bound".into());
    }
    let is_reserve = matches!(method, Method::ReserveWorkItemResources { .. });
    let is_reclaim = matches!(method, Method::ReclaimWorkItemResources { .. });
    if is_reserve {
        if let Some(payload) = resource_reserve_window_precheck(request)? {
            return Ok(ReservationLifecycleStep::Return(payload.into()));
        }
    }
    // A terminal row is the durable idempotency tombstone.  Replay of
    // the exact release/reclaim (or an accepted reserve) must remain
    // answerable after the WorkItem has rotated to a newer attempt or
    // even been removed from the graph; requiring the old live lease
    // first would turn a safe replay into a misleading stale refusal.
    // The full immutable record comparison prevents a caller from
    // using a tombstone's reservation id as a substitute for current
    // WorkItem/fence authorization.
    let existing = resource_load_reservation(reservations, graph, &request.reservation_id, crypto)?;
    if let Some(stored) = existing.as_ref() {
        if let Some(payload) = resource_existing_reservation_precheck(
            request, stored, is_reserve, graph, hosts, crypto,
        )? {
            return Ok(ReservationLifecycleStep::Return(payload.into()));
        }
    }
    Ok(ReservationLifecycleStep::Continue((
        is_reserve, is_reclaim, existing,
    )))
}

/// Phase 2: load the WorkItem row and validate its fence (attempt/lease_epoch/
/// fencing_token vs. the request), exactly as the original function's next block.
/// The validated WorkItem row: its properties map plus the lease fence read from
/// the same MVCC snapshot.
pub(crate) type ResourceWorkItemAdmission = (
    serde_json::Map<String, serde_json::Value>,
    ResourceWorkItemFence,
);

pub(crate) fn resource_load_and_validate_work_item(
    nodes: &mut eg_storage::ScopedOwnerTableMut<'_, (&str, &str), &[u8]>,
    graph: &str,
    request: &ResourceReservationRequest,
    is_reclaim: bool,
    crypto: DurableCrypto<'_>,
) -> Result<ReservationLifecycleStep<ResourceWorkItemAdmission>, String> {
    let item_bytes = nodes
        .get((graph, request.work_item_id.as_str()))?
        .map(|value| crypto.unseal(value.value()))
        .transpose()?;
    let Some(item_bytes) = item_bytes else {
        return Ok(ReservationLifecycleStep::Return(
            resource_result_payload(
                ResourceReservationResultDecision::NotFound,
                request,
                None,
                None,
                0,
                vec![],
            )?
            .into(),
        ));
    };
    let props: serde_json::Map<String, serde_json::Value> = decode_durable(&item_bytes)?;
    let work_item_fence = match resource_validate_work_item(&props, request, is_reclaim) {
        Ok(fence) => fence,
        Err(decision) => {
            return Ok(ReservationLifecycleStep::Return(
                resource_result_payload(decision, request, None, None, 0, vec![])?.into(),
            ));
        }
    };
    Ok(ReservationLifecycleStep::Continue((props, work_item_fence)))
}

/// Phase 3: validate the WorkItem's live status is consistent with the requested
/// lifecycle transition, then parse (and return) its resource admission extension,
/// checking the reserve-only input fingerprint. Literal relocation of the original
/// function's third block.
pub(crate) fn resource_validate_work_item_status_and_extension<'p>(
    request: &ResourceReservationRequest,
    is_reserve: bool,
    is_reclaim: bool,
    work_item_fence: &ResourceWorkItemFence,
    props: &'p serde_json::Map<String, serde_json::Value>,
) -> Result<ReservationLifecycleStep<&'p serde_json::Map<String, serde_json::Value>>, String> {
    if is_reserve {
        let status = property_string(props, "status");
        let lease_expires_at_ms =
            (property_f64(props, "lease_expires_at") * 1000.0).max(0.0) as u64;
        if !matches!(status, "leased" | "running") || lease_expires_at_ms <= request.now_ms {
            return Ok(ReservationLifecycleStep::Return(
                resource_result_payload(
                    ResourceReservationResultDecision::Stale,
                    request,
                    None,
                    None,
                    0,
                    vec![],
                )?
                .into(),
            ));
        }
    } else if (!is_reclaim || !work_item_fence.superseded)
        && !matches!(
            property_string(props, "status"),
            "leased" | "running" | "succeeded" | "failed" | "cancelled" | "dead_letter"
        )
    {
        // Release: any non-terminal, non-live status is stale.
        // Reclaim: same, but a reclaim of an already-superseded
        // reservation is legitimate (that is precisely what reclaim
        // is for), so it is exempted -- `!is_reclaim ||
        // !superseded` is `true` for release and `!superseded` for
        // reclaim, matching the two branches this replaces.
        return Ok(ReservationLifecycleStep::Return(
            resource_result_payload(
                ResourceReservationResultDecision::Stale,
                request,
                None,
                None,
                0,
                vec![],
            )?
            .into(),
        ));
    }
    let (_repository, extension) = resource_metadata_maps(props)
        .map_err(|_| "WorkItem resource admission extension is invalid".to_string())?;
    if is_reserve {
        let expected = resource_recomputed_fingerprint(props, request)?;
        if expected != request.input_fingerprint {
            return Ok(ReservationLifecycleStep::Return(
                resource_result_payload(
                    ResourceReservationResultDecision::InputConflict,
                    request,
                    None,
                    None,
                    0,
                    vec![],
                )?
                .into(),
            ));
        }
    }
    Ok(ReservationLifecycleStep::Continue(extension))
}

/// Tenant / record / terminal-state prechecks of a release-or-reclaim commit.
/// A reserve that finds a live row is itself idempotent.  `Ok(Some(..))` decides
/// the request.
pub(super) fn resource_release_row_precheck(
    graph: &str,
    request: &ResourceReservationRequest,
    stored: &DurableResourceReservation,
    is_reserve: bool,
    hosts: &mut eg_storage::ScopedOwnerTableMut<'_, (&str, &str), &[u8]>,
    crypto: DurableCrypto<'_>,
) -> Result<Option<ResourceReservationResult>, String> {
    if stored.record.tenant_ref != request.tenant_ref {
        return Ok(Some(resource_result_payload(
            ResourceReservationResultDecision::Conflict,
            request,
            None,
            None,
            0,
            vec![],
        )?));
    }
    if !resource_request_matches_record(request, &stored.record) {
        return Ok(Some(resource_result_payload(
            ResourceReservationResultDecision::InputConflict,
            request,
            None,
            None,
            stored.fairness_debt,
            vec![],
        )?));
    }
    if stored.record.state != ResourceReservationRecordState::Reserved || is_reserve {
        let host = resource_load_host(hosts, graph, &stored.record.host_ref, crypto)?;
        return Ok(Some(resource_result_payload(
            ResourceReservationResultDecision::Idempotent,
            request,
            Some(stored.record.clone()),
            host.as_ref(),
            stored.fairness_debt,
            vec![],
        )?));
    }
    Ok(None)
}

/// Reclaim-only policy gates: the reservation must have expired, and the linked
/// WorkItem must not still hold a live lease.
pub(super) fn resource_reclaim_policy_precheck(
    request: &ResourceReservationRequest,
    stored: &DurableResourceReservation,
    props: &serde_json::Map<String, serde_json::Value>,
) -> Result<Option<ResourceReservationResult>, String> {
    let refuse = || {
        resource_result_payload(
            ResourceReservationResultDecision::Policy,
            request,
            Some(stored.record.clone()),
            None,
            stored.fairness_debt,
            vec![],
        )
        .map(Some)
    };
    if request.now_ms < stored.record.expires_at_ms {
        return refuse();
    }
    let status = property_string(props, "status");
    let lease_expires_at_ms = (property_f64(props, "lease_expires_at") * 1000.0).max(0.0) as u64;
    if matches!(status, "leased" | "running") && lease_expires_at_ms > request.now_ms {
        return refuse();
    }
    Ok(None)
}

/// Give the reservation's held capacity back to its host.  Any underflow is a
/// corrupt-accounting error, never a silent saturation.
pub(super) fn resource_release_host_capacity(
    host: &mut DurableResourceHost,
    stored: &DurableResourceReservation,
) -> Result<(), String> {
    host.held_cpu_weight = host
        .held_cpu_weight
        .checked_sub(stored.held_cpu_weight)
        .ok_or_else(|| "resource host cpu accounting underflow".to_string())?;
    host.held_memory_mib = host
        .held_memory_mib
        .checked_sub(stored.held_memory_mib)
        .ok_or_else(|| "resource host memory accounting underflow".to_string())?;
    host.held_disk_mib = host
        .held_disk_mib
        .checked_sub(stored.held_disk_mib)
        .ok_or_else(|| "resource host disk accounting underflow".to_string())?;
    host.held_process_slots = host
        .held_process_slots
        .checked_sub(stored.held_process_slots)
        .ok_or_else(|| "resource host process accounting underflow".to_string())?;
    Ok(())
}

/// The tombstoned successor of a released/reclaimed reservation.
///
/// Fairness debt is historical service debt, not held capacity; releasing a
/// reservation must not erase the cost already charged to this tenant/group, so
/// the current `debt` is carried onto the tombstone.
pub(super) fn resource_build_released_record(
    stored: &DurableResourceReservation,
    request: &ResourceReservationRequest,
    is_reclaim: bool,
    work_item_fence: &ResourceWorkItemFence,
    debt: u64,
) -> DurableResourceReservation {
    let mut next = stored.clone();
    next.record.state = if is_reclaim {
        if work_item_fence.superseded {
            ResourceReservationRecordState::Superseded
        } else {
            ResourceReservationRecordState::Reclaimed
        }
    } else {
        ResourceReservationRecordState::Released
    };
    next.record.revision = next.record.revision.saturating_add(1);
    next.record.lifecycle_revision = next.record.lifecycle_revision.saturating_add(1);
    next.record.expected_lifecycle_revision = request.expected_lifecycle_revision;
    next.record.tombstone = true;
    next.held_cpu_weight = 0;
    next.held_memory_mib = 0;
    next.held_disk_mib = 0;
    next.held_process_slots = 0;
    next.fairness_debt = debt;
    next
}

/// Drop the exclusivity keys this reservation owned and clear its disk-policy
/// block once the freed capacity is back under the low watermark.
pub(super) fn resource_release_exclusivity_and_disk(
    graph: &str,
    request: &ResourceReservationRequest,
    host: &DurableResourceHost,
    exclusivity: &mut eg_storage::ScopedOwnerTableMut<'_, (&str, &str), &str>,
    disk_policies: &mut eg_storage::ScopedOwnerTableMut<'_, (&str, &str), &[u8]>,
    crypto: DurableCrypto<'_>,
) -> Result<(), String> {
    for key in resource_exclusivity_keys(request) {
        let owner = exclusivity
            .get((graph, key.as_str()))?
            .map(|value| value.value().to_string());
        if owner.as_deref() == Some(request.reservation_id.as_str()) {
            exclusivity.remove((graph, key.as_str()))?;
        }
    }
    let disk_key = format!("{}\0{}", request.host_ref, request.disk_policy_key);
    let existing_policy = disk_policies
        .get((graph, disk_key.as_str()))?
        .map(|value| value.value().to_vec());
    let Some(policy_bytes) = existing_policy else {
        return Ok(());
    };
    let mut policy: DurableResourceDiskPolicy = resource_decode(&policy_bytes, crypto)?;
    if policy.low_watermark_mib != request.disk_low_watermark_mib
        || policy.high_watermark_mib != request.disk_high_watermark_mib
    {
        return Ok(());
    }
    let used = host.disk_used_mib.saturating_add(host.held_disk_mib);
    if policy
        .low_watermark_mib
        .is_some_and(|watermark| used <= watermark)
    {
        policy.blocked = false;
        policy.revision = policy.revision.saturating_add(1);
        let bytes = resource_encode(&policy, crypto)?;
        disk_policies.insert((graph, disk_key.as_str()), bytes.as_slice())?;
    }
    Ok(())
}

/// Phase 4: the SECOND existing-reservation branch -- when a reservation row is
/// already on file, this is guaranteed to fully decide the request (idempotent
/// replay, reserve short-circuit, reclaim-not-yet-expired refusal, or the actual
/// release/reclaim commit that decrements the host and tombstones the record).
/// Returns `Ok(None)` only when `existing` is `None`, meaning: no decision made,
/// continue into the reserve-admission path. Literal relocation of the original
/// function's fourth block (`if let Some(stored) = existing.as_ref() { .. }`).
#[allow(clippy::too_many_arguments)]
pub(super) fn resource_commit_release_or_reclaim(
    graph: &str,
    request: &ResourceReservationRequest,
    existing: Option<&DurableResourceReservation>,
    is_reserve: bool,
    is_reclaim: bool,
    work_item_fence: &ResourceWorkItemFence,
    props: &serde_json::Map<String, serde_json::Value>,
    hosts: &mut eg_storage::ScopedOwnerTableMut<'_, (&str, &str), &[u8]>,
    reservations: &mut eg_storage::ScopedOwnerTableMut<'_, (&str, &str), &[u8]>,
    fairness: &mut eg_storage::ScopedOwnerTableMut<'_, (&str, &str), &[u8]>,
    concurrency: &mut eg_storage::ScopedOwnerTableMut<'_, (&str, &str), u64>,
    anti_affinity: &mut eg_storage::ScopedOwnerTableMut<'_, (&str, &str, &str), u64>,
    exclusivity: &mut eg_storage::ScopedOwnerTableMut<'_, (&str, &str), &str>,
    disk_policies: &mut eg_storage::ScopedOwnerTableMut<'_, (&str, &str), &[u8]>,
    crypto: DurableCrypto<'_>,
) -> Result<Option<ResourceReservationResult>, String> {
    let Some(stored) = existing else {
        return Ok(None);
    };
    if let Some(payload) =
        resource_release_row_precheck(graph, request, stored, is_reserve, hosts, crypto)?
    {
        return Ok(Some(payload));
    }
    if is_reclaim {
        if let Some(payload) = resource_reclaim_policy_precheck(request, stored, props)? {
            return Ok(Some(payload));
        }
    }
    let Some(mut host) = resource_load_host(hosts, graph, &stored.record.host_ref, crypto)? else {
        return Ok(Some(resource_result_payload(
            ResourceReservationResultDecision::Policy,
            request,
            Some(stored.record.clone()),
            None,
            stored.fairness_debt,
            vec![],
        )?));
    };
    resource_release_host_capacity(&mut host, stored)?;
    resource_put_host(hosts, graph, &host, crypto)?;
    let debt_row = resource_load_fairness(
        fairness,
        graph,
        &request.tenant_ref,
        &request.fairness_group,
        crypto,
    )?;
    let debt = debt_row.debt;
    let next = resource_build_released_record(stored, request, is_reclaim, work_item_fence, debt);
    resource_put_reservation(reservations, graph, &next, crypto)?;
    let concurrency_key = resource_concurrency_scope_key(&request.concurrency_key);
    resource_adjust_concurrency(concurrency, graph, &concurrency_key, -1)?;
    for tag in &request.anti_affinity {
        resource_adjust_anti_affinity(anti_affinity, graph, &request.host_ref, tag, -1)?;
    }
    resource_release_exclusivity_and_disk(
        graph,
        request,
        &host,
        exclusivity,
        disk_policies,
        crypto,
    )?;
    Ok(Some(resource_result_payload(
        ResourceReservationResultDecision::Accepted,
        request,
        Some(next.record),
        Some(&host),
        debt,
        vec![request.work_item_id.clone()],
    )?))
}

/// Phase 5 (reserve-only path): the attempt-index winner check. Literal relocation.
pub(crate) fn resource_check_attempt_winner_conflict(
    attempts: &mut eg_storage::ScopedOwnerTableMut<'_, (&str, &str, u64), &str>,
    reservations: &mut eg_storage::ScopedOwnerTableMut<'_, (&str, &str), &[u8]>,
    graph: &str,
    request: &ResourceReservationRequest,
    crypto: DurableCrypto<'_>,
) -> Result<Option<ResourceReservationResult>, String> {
    let winner = attempts
        .get((graph, request.work_item_id.as_str(), request.attempt))?
        .map(|value| value.value().to_string());
    if let Some(winner) = winner {
        // The attempt index is a derived invariant, not an alternate
        // source of reservation truth.  Recharging an existing host
        // when the index points at a missing authoritative row would
        // turn partial/corrupt state into a second accepted hold.
        if resource_load_reservation(reservations, graph, &winner, crypto)?.is_none() {
            return Err("resource reservation attempt index references missing reservation".into());
        }
        if winner != request.reservation_id {
            return Ok(Some(resource_result_payload(
                ResourceReservationResultDecision::Conflict,
                request,
                None,
                None,
                0,
                vec![],
            )?));
        }
    }
    Ok(None)
}

pub(super) fn resource_admission_refusal(
    decision: ResourceReservationResultDecision,
    request: &ResourceReservationRequest,
    host: &DurableResourceHost,
) -> Result<ResourceReservationResult, String> {
    resource_result_payload(decision, request, None, Some(host), 0, vec![])
}

/// Host-eligibility gates: the caller's expected host revision, freshness,
/// required labels, and the two target-identity checks.
///
/// The request target is the scheduler's selected placement, while
/// preferred/required targets in the WorkItem extension describe eligibility and
/// ordering.  Once selected, the host's immutable target identity must still
/// equal the asserted local/alias pair; otherwise a local record could carry an
/// inventory host snapshot (or vice versa) and RM could reconstruct a
/// contradictory target.
pub(super) fn resource_admit_check_host_eligibility(
    request: &ResourceReservationRequest,
    host: &DurableResourceHost,
    extension: &serde_json::Map<String, serde_json::Value>,
) -> Result<Option<ResourceReservationResult>, String> {
    if let Some(expected) = request.expected_host_revision {
        if expected != host.revision {
            return Ok(Some(resource_admission_refusal(
                ResourceReservationResultDecision::StaleHost,
                request,
                host,
            )?));
        }
    }
    let host_state = resource_validate_host_freshness(host, request.now_ms);
    if host_state != ResourceReservationResultDecision::Accepted {
        return Ok(Some(resource_admission_refusal(host_state, request, host)?));
    }
    if !request
        .required_labels
        .iter()
        .all(|label| host.labels.iter().any(|value| value == label))
    {
        return Ok(Some(resource_admission_refusal(
            ResourceReservationResultDecision::Labels,
            request,
            host,
        )?));
    }
    if !resource_target_selection_matches(extension, host)? {
        return Ok(Some(resource_admission_refusal(
            ResourceReservationResultDecision::Policy,
            request,
            host,
        )?));
    }
    if !resource_selected_target_matches_request(request, host) {
        return Ok(Some(resource_admission_refusal(
            ResourceReservationResultDecision::Policy,
            request,
            host,
        )?));
    }
    Ok(None)
}

/// Index-backed admission gates, in the original order: anti-affinity tags,
/// the concurrency scope limit, exclusivity keys, and the capacity vector.
pub(super) fn resource_admit_check_index_gates(
    graph: &str,
    request: &ResourceReservationRequest,
    host: &DurableResourceHost,
    anti_affinity: &eg_storage::ScopedOwnerTableMut<'_, (&str, &str, &str), u64>,
    concurrency: &eg_storage::ScopedOwnerTableMut<'_, (&str, &str), u64>,
    exclusivity: &eg_storage::ScopedOwnerTableMut<'_, (&str, &str), &str>,
) -> Result<Option<ResourceReservationResult>, String> {
    for tag in &request.anti_affinity {
        let count = anti_affinity
            .get((graph, request.host_ref.as_str(), tag.as_str()))?
            .map(|value| value.value())
            .unwrap_or(0);
        if count != 0 {
            return Ok(Some(resource_admission_refusal(
                ResourceReservationResultDecision::AntiAffinity,
                request,
                host,
            )?));
        }
    }
    let concurrency_key = resource_concurrency_scope_key(&request.concurrency_key);
    let concurrency_count = concurrency
        .get((graph, concurrency_key.as_str()))?
        .map(|value| value.value())
        .unwrap_or(0);
    if request
        .concurrency_limit
        .is_some_and(|limit| concurrency_count >= limit)
    {
        return Ok(Some(resource_admission_refusal(
            ResourceReservationResultDecision::Concurrency,
            request,
            host,
        )?));
    }
    for key in resource_exclusivity_keys(request) {
        if exclusivity.get((graph, key.as_str()))?.is_some() {
            return Ok(Some(resource_admission_refusal(
                ResourceReservationResultDecision::Exclusivity,
                request,
                host,
            )?));
        }
    }
    if !resource_capacity_sum(host, &request.requirement) {
        return Ok(Some(resource_admission_refusal(
            ResourceReservationResultDecision::Capacity,
            request,
            host,
        )?));
    }
    Ok(None)
}

/// The disk gates that can refuse before anything is written: the per-host
/// policy-count bound, watermark agreement with the existing policy row, and
/// free space on the host.
pub(super) fn resource_admit_check_disk(
    request: &ResourceReservationRequest,
    host: &DurableResourceHost,
    existing_policy: Option<&DurableResourceDiskPolicy>,
    policy_row_count: usize,
) -> Result<Option<ResourceReservationResult>, String> {
    if existing_policy.is_none() && policy_row_count >= MAX_RESOURCE_HOST_DISK_POLICIES {
        return Ok(Some(resource_admission_refusal(
            ResourceReservationResultDecision::Policy,
            request,
            host,
        )?));
    }
    if let Some(policy) = existing_policy {
        if policy.low_watermark_mib != request.disk_low_watermark_mib
            || policy.high_watermark_mib != request.disk_high_watermark_mib
        {
            return Ok(Some(resource_admission_refusal(
                ResourceReservationResultDecision::Policy,
                request,
                host,
            )?));
        }
    }
    let available_disk = host
        .disk_capacity_mib
        .checked_sub(host.disk_used_mib)
        .and_then(|value| value.checked_sub(host.held_disk_mib))
        .unwrap_or(0);
    if request.requirement.disk_mib > available_disk {
        return Ok(Some(resource_admission_refusal(
            ResourceReservationResultDecision::Disk,
            request,
            host,
        )?));
    }
    Ok(None)
}

/// Persist one disk-policy row for `disk_key`.
pub(super) fn resource_put_disk_policy(
    disk_policies: &mut eg_storage::ScopedOwnerTableMut<'_, (&str, &str), &[u8]>,
    graph: &str,
    disk_key: &str,
    policy: &DurableResourceDiskPolicy,
    crypto: DurableCrypto<'_>,
) -> Result<(), String> {
    let bytes = resource_encode(policy, crypto)?;
    disk_policies.insert((graph, disk_key), bytes.as_slice())?;
    Ok(())
}

/// The disk-policy hysteresis step: compute whether this reservation would cross
/// the watermark, persist the resulting policy row, and refuse when blocked.
#[allow(clippy::too_many_arguments)]
pub(super) fn resource_admit_apply_disk_policy(
    graph: &str,
    request: &ResourceReservationRequest,
    host: &DurableResourceHost,
    disk_key: &str,
    existing_policy: Option<&DurableResourceDiskPolicy>,
    disk_policies: &mut eg_storage::ScopedOwnerTableMut<'_, (&str, &str), &[u8]>,
    crypto: DurableCrypto<'_>,
) -> Result<Option<ResourceReservationResult>, String> {
    let predicted_used = host
        .disk_used_mib
        .checked_add(host.held_disk_mib)
        .and_then(|value| value.checked_add(request.requirement.disk_mib))
        .ok_or_else(|| "resource disk accounting overflow".to_string())?;
    let blocked = resource_disk_policy_blocked(
        existing_policy.is_some_and(|policy| policy.blocked),
        predicted_used,
        request.disk_low_watermark_mib,
        request.disk_high_watermark_mib,
    );
    let bumped_policy = |blocked: bool| DurableResourceDiskPolicy {
        blocked,
        low_watermark_mib: request.disk_low_watermark_mib,
        high_watermark_mib: request.disk_high_watermark_mib,
        revision: existing_policy.map_or(1, |value| value.revision.saturating_add(1)),
    };
    if blocked {
        resource_put_disk_policy(
            disk_policies,
            graph,
            disk_key,
            &bumped_policy(blocked),
            crypto,
        )?;
        return Ok(Some(resource_admission_refusal(
            ResourceReservationResultDecision::Disk,
            request,
            host,
        )?));
    }
    if existing_policy.is_some_and(|policy| policy.blocked != blocked) {
        resource_put_disk_policy(
            disk_policies,
            graph,
            disk_key,
            &bumped_policy(blocked),
            crypto,
        )?;
    }
    if existing_policy.is_none() {
        let policy = DurableResourceDiskPolicy {
            blocked: false,
            low_watermark_mib: request.disk_low_watermark_mib,
            high_watermark_mib: request.disk_high_watermark_mib,
            revision: 1,
        };
        resource_put_disk_policy(disk_policies, graph, disk_key, &policy, crypto)?;
    }
    Ok(None)
}

/// Phase 6 (reserve-only path): every host-admission gate (freshness, labels, target
/// selection, anti-affinity, concurrency, exclusivity, capacity, disk-policy bound and
/// hysteresis), including the disk-policy table normalization writes that were part of
/// the same guard chain in the original function. Literal relocation; the ONLY
/// difference from the original is that the final "insert a fresh default policy when
/// none existed" step (previously the last few lines before the fairness/commit phase)
/// is included here rather than split across the phase boundary, because nothing after
/// it in the original function ever read `existing_policy`, `policy_rows`, or
/// `disk_key` again.
/// Every reserve-admission refusal reports the same shape: the decision, the
/// request, and the host snapshot it was evaluated against.
#[allow(clippy::too_many_arguments)]
pub(super) fn resource_admit_reserve_host(
    hosts: &mut eg_storage::ScopedOwnerTableMut<'_, (&str, &str), &[u8]>,
    disk_policies: &mut eg_storage::ScopedOwnerTableMut<'_, (&str, &str), &[u8]>,
    anti_affinity: &mut eg_storage::ScopedOwnerTableMut<'_, (&str, &str, &str), u64>,
    concurrency: &mut eg_storage::ScopedOwnerTableMut<'_, (&str, &str), u64>,
    exclusivity: &mut eg_storage::ScopedOwnerTableMut<'_, (&str, &str), &str>,
    graph: &str,
    request: &ResourceReservationRequest,
    extension: &serde_json::Map<String, serde_json::Value>,
    crypto: DurableCrypto<'_>,
) -> Result<ReservationLifecycleStep<DurableResourceHost>, String> {
    let host = resource_load_host(hosts, graph, &request.host_ref, crypto)?;
    let Some(host) = host else {
        return Ok(ReservationLifecycleStep::Return(
            resource_result_payload(
                ResourceReservationResultDecision::NotFound,
                request,
                None,
                None,
                0,
                vec![],
            )?
            .into(),
        ));
    };
    // Admission and host snapshots share the schema's 128-policy
    // bound.  Enumerating this exact host prefix is part of the same
    // transaction, so a new policy key cannot race a concurrent
    // reservation into an undecodable/unbounded host projection.
    let policy_rows =
        resource_collect_disk_policy_rows(disk_policies, graph, &request.host_ref, crypto)?;
    if let Some(payload) = resource_admit_check_host_eligibility(request, &host, extension)? {
        return Ok(ReservationLifecycleStep::Return(payload.into()));
    }
    if let Some(payload) = resource_admit_check_index_gates(
        graph,
        request,
        &host,
        anti_affinity,
        concurrency,
        exclusivity,
    )? {
        return Ok(ReservationLifecycleStep::Return(payload.into()));
    }
    let disk_key = format!("{}\0{}", request.host_ref, request.disk_policy_key);
    let existing_policy = disk_policies
        .get((graph, disk_key.as_str()))?
        .map(|value| resource_decode::<DurableResourceDiskPolicy>(value.value(), crypto))
        .transpose()?;
    if let Some(payload) =
        resource_admit_check_disk(request, &host, existing_policy.as_ref(), policy_rows.len())?
    {
        return Ok(ReservationLifecycleStep::Return(payload.into()));
    }
    if let Some(payload) = resource_admit_apply_disk_policy(
        graph,
        request,
        &host,
        &disk_key,
        existing_policy.as_ref(),
        disk_policies,
        crypto,
    )? {
        return Ok(ReservationLifecycleStep::Return(payload.into()));
    }
    Ok(ReservationLifecycleStep::Continue(host))
}

/// Phase 7 (reserve-only path): fairness-debt update, host held-capacity increments,
/// and the final durable persist (reservation, tenant index, attempt index,
/// exclusivity, concurrency, anti-affinity) that produces the Accepted result.
/// Literal relocation of the original function's final block.
#[allow(clippy::too_many_arguments)]
pub(super) fn resource_commit_reserve_admission(
    graph: &str,
    request: &ResourceReservationRequest,
    mut host: DurableResourceHost,
    hosts: &mut eg_storage::ScopedOwnerTableMut<'_, (&str, &str), &[u8]>,
    reservations: &mut eg_storage::ScopedOwnerTableMut<'_, (&str, &str), &[u8]>,
    tenant_index: &mut eg_storage::ScopedOwnerTableMut<'_, (&str, &str, &str), &str>,
    attempts: &mut eg_storage::ScopedOwnerTableMut<'_, (&str, &str, u64), &str>,
    exclusivity: &mut eg_storage::ScopedOwnerTableMut<'_, (&str, &str), &str>,
    concurrency: &mut eg_storage::ScopedOwnerTableMut<'_, (&str, &str), u64>,
    anti_affinity: &mut eg_storage::ScopedOwnerTableMut<'_, (&str, &str, &str), u64>,
    fairness: &mut eg_storage::ScopedOwnerTableMut<'_, (&str, &str), &[u8]>,
    crypto: DurableCrypto<'_>,
) -> Result<Option<ResourceReservationResult>, String> {
    let mut debt = resource_load_fairness(
        fairness,
        graph,
        &request.tenant_ref,
        &request.fairness_group,
        crypto,
    )?
    .debt;
    debt = debt
        .checked_add(request.fairness_cost)
        .ok_or_else(|| "resource fairness debt overflow".to_string())?;
    let fairness_row = DurableResourceFairness { debt };
    resource_put_fairness(
        fairness,
        graph,
        &request.tenant_ref,
        &request.fairness_group,
        &fairness_row,
        crypto,
    )?;
    host.held_cpu_weight = host
        .held_cpu_weight
        .checked_add(request.requirement.cpu_weight)
        .ok_or_else(|| "resource host cpu accounting overflow".to_string())?;
    host.held_memory_mib = host
        .held_memory_mib
        .checked_add(request.requirement.memory_mib)
        .ok_or_else(|| "resource host memory accounting overflow".to_string())?;
    host.held_disk_mib = host
        .held_disk_mib
        .checked_add(request.requirement.disk_mib)
        .ok_or_else(|| "resource host disk accounting overflow".to_string())?;
    host.held_process_slots = host
        .held_process_slots
        .checked_add(request.requirement.process_slots)
        .ok_or_else(|| "resource host process accounting overflow".to_string())?;
    let record = resource_build_record(request, &host, 1, 1)?;
    let stored = DurableResourceReservation {
        record: record.clone(),
        held_cpu_weight: request.requirement.cpu_weight,
        held_memory_mib: request.requirement.memory_mib,
        held_disk_mib: request.requirement.disk_mib,
        held_process_slots: request.requirement.process_slots,
        fairness_debt: debt,
    };
    resource_put_host(hosts, graph, &host, crypto)?;
    resource_put_reservation(reservations, graph, &stored, crypto)?;
    tenant_index.insert(
        (
            graph,
            request.tenant_ref.as_str(),
            request.reservation_id.as_str(),
        ),
        request.reservation_id.as_str(),
    )?;
    attempts.insert(
        (graph, request.work_item_id.as_str(), request.attempt),
        request.reservation_id.as_str(),
    )?;
    for key in resource_exclusivity_keys(request) {
        exclusivity.insert((graph, key.as_str()), request.reservation_id.as_str())?;
    }
    let concurrency_key = resource_concurrency_scope_key(&request.concurrency_key);
    resource_adjust_concurrency(concurrency, graph, &concurrency_key, 1)?;
    for tag in &request.anti_affinity {
        resource_adjust_anti_affinity(anti_affinity, graph, &request.host_ref, tag, 1)?;
    }
    Ok(Some(resource_result_payload(
        ResourceReservationResultDecision::Accepted,
        request,
        Some(record),
        Some(&host),
        debt,
        vec![request.work_item_id.clone()],
    )?))
}

pub(crate) fn resource_request_from_record(
    record: &ResourceReservationRecord,
    now_ms: u64,
) -> ResourceReservationRequest {
    ResourceReservationRequest {
        schema_version: crate::epistemic_operations::ResourceReservationRequestSchemaVersion::V1,
        tenant_ref: record.tenant_ref.clone(),
        work_item_id: record.work_item_id.clone(),
        owner_id: record.owner_id.clone(),
        fence: record.fence.clone(),
        lease_epoch: record.lease_epoch,
        fencing_token: record.fencing_token,
        attempt: record.attempt,
        reservation_id: record.reservation_id.clone(),
        input_fingerprint: record.input_fingerprint.clone(),
        profile_name: record.profile_name.clone(),
        profile_version: record.profile_version.clone(),
        host_ref: record.host_ref.clone(),
        requirement: record.requirement.clone(),
        target_kind: match record.target_kind {
            ResourceReservationRecordTargetKind::Local => {
                ResourceReservationRequestTargetKind::Local
            }
            ResourceReservationRecordTargetKind::InventoryAlias => {
                ResourceReservationRequestTargetKind::InventoryAlias
            }
        },
        target_alias: record.target_alias.clone(),
        repository_id: record.repository_id.clone(),
        branch: record.branch.clone(),
        concurrency_key: record.concurrency_key.clone(),
        concurrency_limit: record.concurrency_limit,
        repository_exclusive: record.repository_exclusive,
        branch_exclusive: record.branch_exclusive,
        required_labels: record.required_labels.clone(),
        anti_affinity: record.anti_affinity.clone(),
        fairness_group: record.fairness_group.clone(),
        fairness_cost: record.fairness_cost,
        disk_low_watermark_mib: record.disk_low_watermark_mib,
        disk_high_watermark_mib: record.disk_high_watermark_mib,
        disk_policy_key: record.disk_policy_key.clone(),
        reserved_at_ms: record.reserved_at_ms,
        expires_at_ms: record.expires_at_ms,
        idempotency_key: format!("query:{}", record.reservation_id),
        now_ms,
        expected_host_revision: record.expected_host_revision,
        expected_lifecycle_revision: record.expected_lifecycle_revision,
    }
}

pub(crate) fn resource_no_reservation_query_result(
    request: &ResourceReservationStatusRequest,
    decision: ResourceReservationResultDecision,
) -> Result<ResourceReservationResult, String> {
    let work_item_id = request.work_item_id.clone().unwrap_or_default();
    Ok(ResourceReservationResult {
        schema_version: ResourceReservationResultSchemaVersion::V1,
        decision,
        reservation_id: None,
        work_item_id,
        attempt: request.attempt.unwrap_or(1),
        lease_epoch: request.lease_epoch.unwrap_or(0),
        fencing_token: request.fencing_token.unwrap_or(0),
        lifecycle_revision: 0,
        host_ref: None,
        host_revision: 0,
        record: None,
        state: ResourceReservationResultState::Absent,
        held_cpu_weight: 0,
        held_memory_mib: 0,
        held_disk_mib: 0,
        held_process_slots: 0,
        fairness_debt: 0,
        tombstone: false,
        changed_work_item_ids: Vec::new(),
    })
}

/// Bounded-text validation of a status query's optional selectors, in the
/// original field order so the first offending field is still the one reported.
pub(crate) fn resource_validate_query_selectors(
    request: &ResourceReservationStatusRequest,
) -> Result<(), String> {
    if let Some(value) = request.work_item_id.as_deref() {
        resource_text(value, "resource query work_item_id")?;
    }
    if let Some(value) = request.reservation_id.as_deref() {
        resource_text(value, "resource query reservation_id")?;
    }
    if let Some(value) = request.host_ref.as_deref() {
        resource_text(value, "resource query host_ref")?;
    }
    if let Some(value) = request.owner_id.as_deref() {
        resource_text(value, "resource query owner_id")?;
    }
    Ok(())
}

/// Continuation of `resource_validate_query_selectors`: fence, fingerprint,
/// fairness group and cursor.
pub(crate) fn resource_validate_query_correlations(
    request: &ResourceReservationStatusRequest,
) -> Result<(), String> {
    if let Some(value) = request.fence.as_deref() {
        resource_text(value, "resource query fence")?;
    }
    if let Some(value) = request.input_fingerprint.as_deref() {
        resource_fingerprint(value, "resource query input_fingerprint")?;
    }
    if let Some(value) = request.fairness_group.as_deref() {
        resource_text(value, "resource query fairness_group")?;
    }
    if let Some(value) = request.cursor.as_deref() {
        resource_text(value, "resource query cursor")?;
    }
    Ok(())
}

pub(crate) fn resource_validate_query_request(
    request: &ResourceReservationStatusRequest,
    require_limit: bool,
) -> Result<(), String> {
    resource_text(&request.tenant_ref, "resource query tenant_ref")?;
    resource_validate_query_selectors(request)?;
    resource_validate_query_correlations(request)?;
    if request.attempt.is_some_and(|attempt| attempt == 0) {
        return Err("resource query attempt must be positive".into());
    }
    if require_limit {
        if request.limit == 0 || request.limit > MAX_RESOURCE_STATUS_LIMIT as u64 {
            return Err("resource status request violates bounds".into());
        }
    } else if request.limit > MAX_RESOURCE_STATUS_LIMIT as u64 {
        return Err("resource query violates bounds".into());
    }
    Ok(())
}

pub(crate) fn resource_record_work_item_live(
    props: &serde_json::Map<String, serde_json::Value>,
    record: &ResourceReservationRecord,
    now_ms: u64,
) -> bool {
    let status = property_string(props, "status");
    let owner = if matches!(status, "leased" | "running") {
        property_string(props, "lease_owner")
    } else {
        property_string(props, "last_lease_owner")
    };
    let lease_until = (property_f64(props, "lease_expires_at") * 1000.0).max(0.0) as u64;
    property_string(props, "node_type") == "WorkItem"
        && property_string(props, "tenant") == record.tenant_ref
        && property_u64(props, "attempt") == record.attempt
        && property_u64(props, "lease_epoch") == record.lease_epoch
        && property_u64(props, "fencing_token") == record.fencing_token
        && owner == record.owner_id
        && matches!(status, "leased" | "running")
        && lease_until > now_ms
        && resource_expected_fence(record.fencing_token) == record.fence
}

#[cfg(test)]
pub(crate) fn resource_decode_result_payload(
    payload: crate::protocol::ResultPayload,
) -> Result<ResourceReservationResult, String> {
    let bytes = match payload {
        crate::protocol::ResultPayload::Raw(bytes) => bytes,
        _ => return Err("resource query result encoding failed".into()),
    };
    eg_types::msgpack::decode_bounded(
        &bytes,
        eg_types::msgpack::MsgpackLimits::new(64 * 1024, 10_000, 32),
    )
    .map_err(|_| "resource query result encoding failed".into())
}

/// Exact query reads the native reservation row and all caller correlations
/// from one MVCC snapshot.  A null reservation id is the intentionally narrow
/// current-WorkItem precheck used for scheduler ranking; it never returns a
/// reservation ledger and Reserve revalidates the same fence transactionally.
#[allow(clippy::type_complexity)]
pub(crate) fn resolve_current_work_item_query_identity_fields(
    request: &ResourceReservationStatusRequest,
) -> Result<(&str, &str, &str), String> {
    let work_item_id = request
        .work_item_id
        .as_deref()
        .ok_or_else(|| "current WorkItem query requires work_item_id".to_string())?;
    let owner = request
        .owner_id
        .as_deref()
        .ok_or_else(|| "current WorkItem query requires owner_id".to_string())?;
    let fence = request
        .fence
        .as_deref()
        .ok_or_else(|| "current WorkItem query requires fence".to_string())?;
    Ok((work_item_id, owner, fence))
}

pub(crate) fn resolve_current_work_item_query_fence_fields(
    request: &ResourceReservationStatusRequest,
) -> Result<(u64, u64, u64), String> {
    let attempt = request
        .attempt
        .ok_or_else(|| "current WorkItem query requires attempt".to_string())?;
    let lease_epoch = request
        .lease_epoch
        .ok_or_else(|| "current WorkItem query requires lease_epoch".to_string())?;
    let fencing_token = request
        .fencing_token
        .ok_or_else(|| "current WorkItem query requires fencing_token".to_string())?;
    Ok((attempt, lease_epoch, fencing_token))
}

#[allow(clippy::type_complexity)]
pub(crate) fn resolve_current_work_item_query_fields(
    request: &ResourceReservationStatusRequest,
) -> Result<(&str, &str, &str, u64, u64, u64), String> {
    let (work_item_id, owner, fence) = resolve_current_work_item_query_identity_fields(request)?;
    let (attempt, lease_epoch, fencing_token) =
        resolve_current_work_item_query_fence_fields(request)?;
    Ok((
        work_item_id,
        owner,
        fence,
        attempt,
        lease_epoch,
        fencing_token,
    ))
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn current_work_item_query_matches(
    props: &serde_json::Map<String, serde_json::Value>,
    request: &ResourceReservationStatusRequest,
    owner: &str,
    fence: &str,
    attempt: u64,
    lease_epoch: u64,
    fencing_token: u64,
) -> bool {
    let current_attempt = property_u64(props, "attempt");
    let current_epoch = property_u64(props, "lease_epoch");
    let current_token = property_u64(props, "fencing_token");
    let status = property_string(props, "status");
    let current_owner = if matches!(status, "leased" | "running") {
        property_string(props, "lease_owner")
    } else {
        property_string(props, "last_lease_owner")
    };
    let live_until = (property_f64(props, "lease_expires_at") * 1000.0).max(0.0) as u64;
    property_string(props, "node_type") == "WorkItem"
        && property_string(props, "tenant") == request.tenant_ref
        && current_attempt == attempt
        && current_epoch == lease_epoch
        && current_token == fencing_token
        && fence == resource_expected_fence(fencing_token)
        && current_owner == owner
        && matches!(status, "leased" | "running")
        && live_until > request.now_ms
}

pub(crate) fn read_resource_reservation_current_work_item_query(
    nodes: &eg_storage::ScopedOwnerTable<(&str, &str), &[u8]>,
    graph: &str,
    request: &ResourceReservationStatusRequest,
    crypto: DurableCrypto<'_>,
) -> Result<ResourceReservationResult, String> {
    let (work_item_id, owner, fence, attempt, lease_epoch, fencing_token) =
        resolve_current_work_item_query_fields(request)?;
    let bytes = nodes
        .get((graph, work_item_id))?
        .map(|value| crypto.unseal(value.value()))
        .transpose()?;
    let Some(bytes) = bytes else {
        return resource_no_reservation_query_result(
            request,
            ResourceReservationResultDecision::NotFound,
        );
    };
    let props: serde_json::Map<String, serde_json::Value> = decode_durable(&bytes)?;
    let current = current_work_item_query_matches(
        &props,
        request,
        owner,
        fence,
        attempt,
        lease_epoch,
        fencing_token,
    );
    let decision = if current {
        ResourceReservationResultDecision::Accepted
    } else {
        ResourceReservationResultDecision::Stale
    };
    resource_no_reservation_query_result(request, decision)
}

// RM's mirrorless retry query intentionally omits the fingerprint: the
// native record is the source of truth and the adapter compares it
// after decoding.  If a mirror supplies one, it remains an exact
// correlation and a mismatch fails closed.
pub(crate) fn resource_reservation_query_correlates(
    request: &ResourceReservationStatusRequest,
    record: &ResourceReservationRecord,
) -> bool {
    request.work_item_id.as_deref() == Some(record.work_item_id.as_str())
        && request
            .host_ref
            .as_deref()
            .is_none_or(|host_ref| host_ref == record.host_ref)
        && request.owner_id.as_deref() == Some(record.owner_id.as_str())
        && request.fence.as_deref() == Some(record.fence.as_str())
        && request.attempt == Some(record.attempt)
        && request.lease_epoch == Some(record.lease_epoch)
        && request.fencing_token == Some(record.fencing_token)
        && request
            .input_fingerprint
            .as_deref()
            .is_none_or(|fingerprint| fingerprint == record.input_fingerprint.as_str())
}

pub(super) fn resource_reservation_query_host(
    read: &ScopedRead<'_, GraphShardOwner>,
    graph: &str,
    record: &ResourceReservationRecord,
    crypto: DurableCrypto<'_>,
) -> Result<Option<DurableResourceHost>, String> {
    let hosts = read.scoped_owner_table(RESOURCE_HOSTS)?;
    hosts
        .get((graph, record.host_ref.as_str()))?
        .map(|row| resource_decode::<DurableResourceHost>(row.value(), crypto))
        .transpose()
}

pub(crate) fn resource_reservation_query_current_work_item(
    nodes: &eg_storage::ScopedOwnerTable<(&str, &str), &[u8]>,
    graph: &str,
    record: &ResourceReservationRecord,
    crypto: DurableCrypto<'_>,
) -> Result<Option<serde_json::Map<String, serde_json::Value>>, String> {
    let current_item = nodes
        .get((graph, record.work_item_id.as_str()))?
        .map(|value| crypto.unseal(value.value()))
        .transpose()?;
    current_item
        .as_deref()
        .map(decode_durable::<serde_json::Map<String, serde_json::Value>>)
        .transpose()
}

pub(super) fn build_resource_reservation_query_result(
    read: &ScopedRead<'_, GraphShardOwner>,
    nodes: &eg_storage::ScopedOwnerTable<(&str, &str), &[u8]>,
    graph: &str,
    request: &ResourceReservationStatusRequest,
    stored: &DurableResourceReservation,
    crypto: DurableCrypto<'_>,
) -> Result<ResourceReservationResult, String> {
    let record = &stored.record;
    let host = resource_reservation_query_host(read, graph, record, crypto)?;
    let request_for_payload = resource_request_from_record(record, request.now_ms);
    let current = resource_reservation_query_current_work_item(nodes, graph, record, crypto)?;
    let current_valid = current
        .as_ref()
        .is_some_and(|props| resource_record_work_item_live(props, record, request.now_ms));
    let tombstone_replay = record.tombstone;
    let decision = if current_valid || tombstone_replay {
        ResourceReservationResultDecision::Idempotent
    } else {
        ResourceReservationResultDecision::Stale
    };
    resource_result_payload(
        decision,
        &request_for_payload,
        (current_valid || tombstone_replay).then(|| record.clone()),
        host.as_ref(),
        stored.fairness_debt,
        Vec::new(),
    )
}

pub(crate) fn read_resource_reservation_by_id(
    read: &ScopedRead<'_, GraphShardOwner>,
    nodes: &eg_storage::ScopedOwnerTable<(&str, &str), &[u8]>,
    graph: &str,
    request: &ResourceReservationStatusRequest,
    crypto: DurableCrypto<'_>,
) -> Result<ResourceReservationResult, String> {
    let reservation_id = request.reservation_id.as_deref().unwrap_or_default();
    resource_text(reservation_id, "resource reservation_id")?;
    let reservations = read.scoped_owner_table(RESOURCE_RESERVATIONS)?;
    // A mirrorless RM admission query is an expected pre-reserve read.  A
    // missing native row is a typed absence, not a transport failure; the
    // scheduler then submits Reserve and lets that transaction revalidate
    // the WorkItem/fence atomically.
    let Some(row) = reservations.get((graph, reservation_id))? else {
        return resource_no_reservation_query_result(
            request,
            ResourceReservationResultDecision::NotFound,
        );
    };
    let stored: DurableResourceReservation = resource_decode(row.value(), crypto)?;
    if stored.record.tenant_ref != request.tenant_ref {
        // Preserve tenant isolation while keeping the public query vocabulary
        // typed and bounded.  Do not reveal whether another tenant owns this
        // reservation id through a transport error.
        return resource_no_reservation_query_result(
            request,
            ResourceReservationResultDecision::NotFound,
        );
    }
    if !resource_reservation_query_correlates(request, &stored.record) {
        return Err("resource reservation correlation does not match".into());
    }
    build_resource_reservation_query_result(read, nodes, graph, request, &stored, crypto)
}

pub(crate) fn read_resource_reservation(
    shard: &Shard,
    graph: &str,
    request: &ResourceReservationStatusRequest,
    crypto: DurableCrypto<'_>,
) -> Result<ResourceReservationResult, String> {
    resource_validate_query_request(request, false)?;
    let handle = shard.graph(graph)?;
    let read = shard.read(&handle)?;
    let nodes = read.scoped_owner_table(NODES)?;
    if request.reservation_id.is_none() {
        return read_resource_reservation_current_work_item_query(&nodes, graph, request, crypto);
    }
    read_resource_reservation_by_id(&read, &nodes, graph, request, crypto)
}

#[allow(clippy::type_complexity)]
pub(crate) fn open_resource_reservation_status_tables(
    read: &ScopedRead<'_, GraphShardOwner>,
) -> Result<
    (
        eg_storage::ScopedOwnerTable<(&'static str, &'static str), &'static [u8]>,
        eg_storage::ScopedOwnerTable<(&'static str, &'static str, &'static str), &'static str>,
        eg_storage::ScopedOwnerTable<(&'static str, &'static str), &'static [u8]>,
        eg_storage::ScopedOwnerTable<(&'static str, &'static str), &'static [u8]>,
    ),
    String,
> {
    let reservations = read.scoped_owner_table(RESOURCE_RESERVATIONS)?;
    let tenant_index = read.scoped_owner_table(RESOURCE_RESERVATION_TENANT_INDEX)?;
    let hosts = read.scoped_owner_table(RESOURCE_HOSTS)?;
    let disk_policies = read.scoped_owner_table(RESOURCE_DISK_POLICIES)?;
    Ok((reservations, tenant_index, hosts, disk_policies))
}

pub(crate) enum ResourceReservationStatusRowOutcome {
    StopScan,
    SkipCursor,
    Orphan,
    Processed {
        superseded: bool,
        summary: Option<ResourceReservationSummary>,
    },
}

pub(crate) fn resource_reservation_status_row_is_filtered_out(
    request: &ResourceReservationStatusRequest,
    record: &ResourceReservationRecord,
) -> bool {
    request
        .host_ref
        .as_deref()
        .is_some_and(|host| host != record.host_ref)
        || request
            .work_item_id
            .as_deref()
            .is_some_and(|id| id != record.work_item_id)
        || request
            .fairness_group
            .as_deref()
            .is_some_and(|group| group != record.fairness_group)
        || request
            .owner_id
            .as_deref()
            .is_some_and(|owner| owner != record.owner_id)
        || request
            .fence
            .as_deref()
            .is_some_and(|fence| fence != record.fence)
        || request
            .input_fingerprint
            .as_deref()
            .is_some_and(|fingerprint| fingerprint != record.input_fingerprint)
}

pub(super) fn build_resource_reservation_summary(
    record: &ResourceReservationRecord,
    stored: &DurableResourceReservation,
) -> ResourceReservationSummary {
    let is_reserved = record.state == ResourceReservationRecordState::Reserved;
    ResourceReservationSummary {
        reservation_id: record.reservation_id.clone(),
        work_item_id: record.work_item_id.clone(),
        attempt: record.attempt,
        host_ref: record.host_ref.clone(),
        profile_name: record.profile_name.clone(),
        fairness_group: record.fairness_group.clone(),
        state: resource_summary_state(record.state),
        revision: record.revision,
        expires_at_ms: record.expires_at_ms,
        held_cpu_weight: if is_reserved {
            stored.held_cpu_weight
        } else {
            0
        },
        held_memory_mib: if is_reserved {
            stored.held_memory_mib
        } else {
            0
        },
        held_disk_mib: if is_reserved { stored.held_disk_mib } else { 0 },
        held_process_slots: if is_reserved {
            stored.held_process_slots
        } else {
            0
        },
        tombstone: record.tombstone,
    }
}

pub(crate) fn resolve_resource_reservation_status_row(
    reservation_id: &str,
    graph: &str,
    request: &ResourceReservationStatusRequest,
    reservations: &eg_storage::ScopedOwnerTable<(&str, &str), &[u8]>,
    crypto: DurableCrypto<'_>,
) -> Result<ResourceReservationStatusRowOutcome, String> {
    let Some(row) = reservations.get((graph, reservation_id))? else {
        return Ok(ResourceReservationStatusRowOutcome::Orphan);
    };
    let stored: DurableResourceReservation = resource_decode(row.value(), crypto)?;
    let record = &stored.record;
    if record.tenant_ref != request.tenant_ref {
        return Ok(ResourceReservationStatusRowOutcome::Orphan);
    }
    let superseded = record.state == ResourceReservationRecordState::Superseded;
    if resource_reservation_status_row_is_filtered_out(request, record) {
        return Ok(ResourceReservationStatusRowOutcome::Processed {
            superseded,
            summary: None,
        });
    }
    let summary = build_resource_reservation_summary(record, &stored);
    Ok(ResourceReservationStatusRowOutcome::Processed {
        superseded,
        summary: Some(summary),
    })
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn resource_reservation_status_row_outcome(
    row_graph: &str,
    tenant: &str,
    reservation_id: &str,
    index_value: &str,
    graph: &str,
    cursor: &str,
    request: &ResourceReservationStatusRequest,
    reservations: &eg_storage::ScopedOwnerTable<(&str, &str), &[u8]>,
    crypto: DurableCrypto<'_>,
) -> Result<ResourceReservationStatusRowOutcome, String> {
    // The scan is `scope_rows()`, which starts at the FIRST row this graph
    // owns rather than at the `(graph, tenant, cursor)` position a raw range
    // could start from -- a key whose non-leading components are `&str` has no
    // inclusive upper bound, so no bounded range can express this prefix. The
    // rows before that position are therefore skipped here instead of never
    // being read; the decision for every row at or after it is unchanged, and
    // the scan still stops as soon as it leaves the requested tenant.
    if row_graph != graph || tenant > request.tenant_ref.as_str() {
        return Ok(ResourceReservationStatusRowOutcome::StopScan);
    }
    if tenant < request.tenant_ref.as_str() || reservation_id <= cursor {
        return Ok(ResourceReservationStatusRowOutcome::SkipCursor);
    }
    if index_value != reservation_id {
        return Ok(ResourceReservationStatusRowOutcome::Orphan);
    }
    resolve_resource_reservation_status_row(reservation_id, graph, request, reservations, crypto)
}

pub(crate) struct ResourceReservationStatusScan {
    values: Vec<ResourceReservationSummary>,
    has_more: bool,
    last_returned_cursor: Option<String>,
    orphan_count: u64,
    superseded_count: u64,
}

pub(crate) fn apply_resource_reservation_status_row_outcome(
    outcome: ResourceReservationStatusRowOutcome,
    scan: &mut ResourceReservationStatusScan,
    limit: usize,
) -> std::ops::ControlFlow<()> {
    match outcome {
        ResourceReservationStatusRowOutcome::StopScan => std::ops::ControlFlow::Break(()),
        ResourceReservationStatusRowOutcome::SkipCursor => std::ops::ControlFlow::Continue(()),
        ResourceReservationStatusRowOutcome::Orphan => {
            scan.orphan_count = scan.orphan_count.saturating_add(1);
            std::ops::ControlFlow::Continue(())
        }
        ResourceReservationStatusRowOutcome::Processed {
            superseded,
            summary,
        } => {
            if superseded {
                scan.superseded_count = scan.superseded_count.saturating_add(1);
            }
            let Some(summary) = summary else {
                return std::ops::ControlFlow::Continue(());
            };
            let reservation_cursor = summary.reservation_id.clone();
            scan.values.push(summary);
            if scan.values.len() > limit {
                scan.values.pop();
                scan.has_more = true;
                return std::ops::ControlFlow::Break(());
            }
            scan.last_returned_cursor = Some(reservation_cursor);
            std::ops::ControlFlow::Continue(())
        }
    }
}

pub(crate) fn scan_resource_reservation_status_rows(
    tenant_index: &eg_storage::ScopedOwnerTable<(&str, &str, &str), &str>,
    reservations: &eg_storage::ScopedOwnerTable<(&str, &str), &[u8]>,
    graph: &str,
    cursor: &str,
    request: &ResourceReservationStatusRequest,
    crypto: DurableCrypto<'_>,
) -> Result<ResourceReservationStatusScan, String> {
    let mut scan = ResourceReservationStatusScan {
        values: Vec::new(),
        has_more: false,
        last_returned_cursor: None,
        orphan_count: 0,
        superseded_count: 0,
    };
    let mut scanned = 0usize;
    for row in tenant_index.scope_rows()? {
        scanned = scanned.saturating_add(1);
        if scanned > MAX_RESOURCE_STATUS_SCAN {
            return Err("resource status scan exceeds native bound".into());
        }
        let (key, value) = row?;
        let (row_graph, tenant, reservation_id) = key.value();
        let outcome = resource_reservation_status_row_outcome(
            row_graph,
            tenant,
            reservation_id,
            value.value(),
            graph,
            cursor,
            request,
            reservations,
            crypto,
        )?;
        if apply_resource_reservation_status_row_outcome(outcome, &mut scan, request.limit as usize)
            .is_break()
        {
            break;
        }
    }
    Ok(scan)
}

pub(super) fn resource_reservation_status_host(
    hosts: &eg_storage::ScopedOwnerTable<(&str, &str), &[u8]>,
    graph: &str,
    request: &ResourceReservationStatusRequest,
    crypto: DurableCrypto<'_>,
) -> Result<Option<DurableResourceHost>, String> {
    let Some(host_ref) = request.host_ref.as_deref() else {
        return Ok(None);
    };
    hosts
        .get((graph, host_ref))?
        .map(|row| resource_decode::<DurableResourceHost>(row.value(), crypto))
        .transpose()
}

pub(super) fn decode_resource_disk_policy_row(
    policy_key: &str,
    prefix: &str,
    value_bytes: &[u8],
    crypto: DurableCrypto<'_>,
) -> Result<(String, DurableResourceDiskPolicy), String> {
    let policy_key = policy_key
        .strip_prefix(prefix)
        .ok_or_else(|| "resource disk-policy key escaped host scope".to_string())?;
    resource_text(policy_key, "resource disk_policy_key")?;
    Ok((
        policy_key.to_string(),
        resource_decode::<DurableResourceDiskPolicy>(value_bytes, crypto)?,
    ))
}

pub(super) fn resource_reservation_status_host_policies(
    disk_policies: &eg_storage::ScopedOwnerTable<(&str, &str), &[u8]>,
    graph: &str,
    host: Option<&DurableResourceHost>,
    crypto: DurableCrypto<'_>,
) -> Result<Vec<(String, DurableResourceDiskPolicy)>, String> {
    let Some(host) = host else {
        return Ok(Vec::new());
    };
    let prefix = format!("{}\0", host.host_ref);
    let mut rows = Vec::new();
    // `scope_rows()` is the whole of this graph's disk-policy rows: a key whose
    // second component is a `&str` has no maximum, so the host prefix cannot be
    // expressed as a bounded range. The rows sorting BEFORE the prefix are
    // skipped rather than treated as the end of the scan; the first row at or
    // after it that does not carry the prefix still ends the scan, exactly as
    // the prefix `break` did.
    //
    // Starting at the prefix used to bound the whole scan for free; starting at
    // the scope does not, so the rows this skips are counted against the same
    // native status-scan budget the tenant-index scan already uses. Without it
    // one host's policy read would be linear in every OTHER host's policy rows,
    // unbounded.
    let mut scanned = 0usize;
    for row in disk_policies.scope_rows()? {
        scanned = scanned.saturating_add(1);
        if scanned > MAX_RESOURCE_STATUS_SCAN || rows.len() >= MAX_RESOURCE_HOST_DISK_POLICIES {
            return Err("resource disk-policy scan exceeds native bound".to_string());
        }
        let (key, value) = row?;
        let (row_graph, policy_key) = key.value();
        if row_graph != graph {
            break;
        }
        if !policy_key.starts_with(&prefix) {
            if policy_key < prefix.as_str() {
                continue;
            }
            break;
        }
        rows.push(decode_resource_disk_policy_row(
            policy_key,
            &prefix,
            value.value(),
            crypto,
        )?);
    }
    Ok(rows)
}

pub(crate) fn resource_reservation_status_fairness_debt(
    read: &ScopedRead<'_, GraphShardOwner>,
    graph: &str,
    request: &ResourceReservationStatusRequest,
    crypto: DurableCrypto<'_>,
) -> Result<u64, String> {
    let Some(group) = request.fairness_group.as_deref() else {
        return Ok(0);
    };
    let fairness = read.scoped_owner_table(RESOURCE_FAIRNESS)?;
    let row = fairness
        .get((
            graph,
            resource_fairness_scope_key(&request.tenant_ref, group).as_str(),
        ))?
        .map(|row| resource_decode::<DurableResourceFairness>(row.value(), crypto))
        .transpose()?;
    Ok(row.map_or(0, |row| row.debt))
}

pub(super) fn build_resource_reservation_status_result(
    scan: ResourceReservationStatusScan,
    host: Option<DurableResourceHost>,
    host_snapshot: Option<ResourceReservationHostSnapshot>,
    fairness_debt: u64,
) -> ResourceReservationStatusResult {
    let next_cursor = scan.has_more.then_some(scan.last_returned_cursor).flatten();
    ResourceReservationStatusResult {
        schema_version: ResourceReservationStatusResultSchemaVersion::V1,
        complete: !scan.has_more,
        next_cursor,
        host_snapshot,
        host_ref: host.as_ref().map(|value| value.host_ref.clone()),
        host_revision: host.as_ref().map_or(0, |value| value.revision),
        held_cpu_weight: host.as_ref().map_or(0, |value| value.held_cpu_weight),
        held_memory_mib: host.as_ref().map_or(0, |value| value.held_memory_mib),
        held_disk_mib: host.as_ref().map_or(0, |value| value.held_disk_mib),
        held_process_slots: host.as_ref().map_or(0, |value| value.held_process_slots),
        fairness_debt,
        reservations: scan.values,
        orphan_count: scan.orphan_count,
        superseded_count: scan.superseded_count,
    }
}

pub(crate) fn read_resource_reservation_status(
    shard: &Shard,
    graph: &str,
    request: &ResourceReservationStatusRequest,
    crypto: DurableCrypto<'_>,
) -> Result<ResourceReservationStatusResult, String> {
    resource_validate_query_request(request, true)?;
    let cursor = request.cursor.as_deref().unwrap_or("");
    let handle = shard.graph(graph)?;
    let read = shard.read(&handle)?;
    let (reservations, tenant_index, hosts, disk_policies) =
        open_resource_reservation_status_tables(&read)?;

    let scan = scan_resource_reservation_status_rows(
        &tenant_index,
        &reservations,
        graph,
        cursor,
        request,
        crypto,
    )?;

    let host = resource_reservation_status_host(&hosts, graph, request, crypto)?;
    let host_policies =
        resource_reservation_status_host_policies(&disk_policies, graph, host.as_ref(), crypto)?;
    let host_snapshot = host
        .as_ref()
        .map(|value| resource_reservation_host_snapshot(value, &host_policies))
        .transpose()?;
    let fairness_debt = resource_reservation_status_fairness_debt(&read, graph, request, crypto)?;

    Ok(build_resource_reservation_status_result(
        scan,
        host,
        host_snapshot,
        fairness_debt,
    ))
}
