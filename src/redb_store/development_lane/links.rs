use super::super::{decode_durable, resource_decode, DurableCrypto, NODES};
use super::identity::lane_intent_matches_hold;
use super::reserve_support::work_item_kind_name;
use super::rows::LaneRows;
use super::types::LaneDecision;
use super::validation::{bounded_texts, decision_name};
use super::{
    DurableLaneHold, HOLDS, LANE_CLEANUP_EXTENSION_KEY, LANE_INTENT_EXTENSION_KEY, MAX_COUNT,
    MAX_DISK_BYTES, MAX_FINGERPRINT, MAX_TEXT, MAX_TTL_MS, REPOSITORY_WORK_ITEM_EXTENSION_KEY,
    WORK_ITEM_METADATA_KEY,
};
use crate::epistemic_operations::{
    DevelopmentLaneCleanupIntent, DevelopmentLaneCleanupIntentSchemaVersion, DevelopmentLaneHold,
    DevelopmentLaneHoldHostTargetKind, DevelopmentLaneHoldState, DevelopmentLaneIntent,
    DevelopmentLaneIntentHostTargetKind,
};
use crate::protocol::DevelopmentLaneWorkItemKind;
use crate::redb_store::shard::ShardWrite;

/// A lifecycle WorkItem's owner is authoritative while it is live and is
/// retained in `last_lease_owner` after terminalization.  Keep the terminal
/// shape strict as well: a terminal row must not retain a live lease owner that
/// could be mistaken for a fresh claim.
pub(super) fn lifecycle_work_item_owner_matches(
    props: &serde_json::Map<String, serde_json::Value>,
    status: &str,
    expected_owner: &str,
) -> bool {
    match status {
        "leased" | "running" => {
            super::super::property_string(props, "lease_owner") == expected_owner
        }
        "succeeded" | "failed" | "cancelled" | "dead_letter" => {
            super::super::property_string(props, "lease_owner").is_empty()
                && super::super::property_string(props, "last_lease_owner") == expected_owner
        }
        _ => false,
    }
}

/// The checkpoint's node image, indexed by node id.
pub(super) type IncomingNodes<'a> = std::collections::HashMap<&'a str, &'a [u8]>;

/// Index the checkpoint's node set by id, rejecting a duplicate node.
pub(super) fn checkpoint_incoming_nodes(
    incoming_nodes: &[(String, Vec<u8>)],
) -> Result<IncomingNodes<'_>, String> {
    let mut incoming = std::collections::HashMap::with_capacity(incoming_nodes.len());
    for (id, bytes) in incoming_nodes {
        if incoming.insert(id.as_str(), bytes.as_slice()).is_some() {
            return Err("checkpoint contains duplicate WorkItem node".to_string());
        }
    }
    Ok(incoming)
}

/// Does the incoming lifecycle WorkItem carry a different identity/fence tuple
/// than the retained hold?
pub(super) fn checkpoint_lifecycle_fence_mismatch(
    props: &serde_json::Map<String, serde_json::Value>,
    status: &str,
    hold: &DevelopmentLaneHold,
) -> bool {
    super::super::property_string(props, "node_type") != "WorkItem"
        || super::super::property_string(props, "tenant") != hold.tenant_ref
        || super::super::property_string(props, "kind")
            != work_item_kind_name(DevelopmentLaneWorkItemKind::Lifecycle)
        || super::super::property_u64(props, "attempt") != hold.attempt
        || super::super::property_u64(props, "lease_epoch") != hold.lease_epoch
        || super::super::property_u64(props, "fencing_token") != hold.fencing_token
        || super::super::property_string(props, "work_item_fence") != hold.work_item_fence
        || !lifecycle_work_item_owner_matches(props, status, &hold.owner_id)
}

/// The same check for the distinct cleanup WorkItem of a cleaned hold, whose
/// fence tuple is recorded on the hold rather than shared with the lifecycle
/// attempt.
pub(super) fn checkpoint_cleanup_fence_mismatch(
    props: &serde_json::Map<String, serde_json::Value>,
    hold: &DevelopmentLaneHold,
    attempt: u64,
    lease_epoch: u64,
    fencing_token: u64,
    fence: &str,
) -> bool {
    let status = super::super::property_string(props, "status");
    let terminal = matches!(status, "succeeded" | "failed" | "cancelled" | "dead_letter");
    super::super::property_string(props, "node_type") != "WorkItem"
        || super::super::property_string(props, "tenant") != hold.tenant_ref
        || super::super::property_string(props, "kind")
            != work_item_kind_name(DevelopmentLaneWorkItemKind::Cleanup)
        || super::super::property_u64(props, "attempt") != attempt
        || super::super::property_u64(props, "lease_epoch") != lease_epoch
        || super::super::property_u64(props, "fencing_token") != fencing_token
        || super::super::property_string(props, "work_item_fence") != fence
        || (!terminal && !matches!(status, "leased" | "running"))
}

/// A cleaned hold must still carry its distinct cleanup WorkItem, with the
/// exact recorded fence tuple and the exact cleanup intent.
pub(super) fn validate_checkpoint_cleanup_link(
    row: &DurableLaneHold,
    incoming: &IncomingNodes<'_>,
) -> Result<(), String> {
    let cleanup_id = row
        .hold
        .cleanup_work_item_id
        .as_deref()
        .ok_or_else(|| "checkpoint cleaned lane cleanup WorkItem missing".to_string())?;
    let cleanup_fence = row
        .hold
        .cleanup_work_item_fence
        .as_deref()
        .ok_or_else(|| "checkpoint cleaned lane cleanup fence missing".to_string())?;
    let cleanup_attempt = row
        .hold
        .cleanup_attempt
        .ok_or_else(|| "checkpoint cleaned lane cleanup attempt missing".to_string())?;
    let cleanup_lease_epoch = row
        .hold
        .cleanup_lease_epoch
        .ok_or_else(|| "checkpoint cleaned lane cleanup lease epoch missing".to_string())?;
    let cleanup_fencing_token = row
        .hold
        .cleanup_fencing_token
        .ok_or_else(|| "checkpoint cleaned lane cleanup fencing token missing".to_string())?;
    let expected_revision = row
        .cleanup_expected_hold_revision
        .ok_or_else(|| "checkpoint cleaned lane cleanup revision missing".to_string())?;
    let cleanup_bytes = incoming
        .get(cleanup_id)
        .ok_or_else(|| "checkpoint would orphan a cleaned lane cleanup WorkItem".to_string())?;
    let cleanup_props: serde_json::Map<String, serde_json::Value> =
        decode_durable(cleanup_bytes)
            .map_err(|_| "checkpoint cleanup WorkItem decode failed".to_string())?;
    if checkpoint_cleanup_fence_mismatch(
        &cleanup_props,
        &row.hold,
        cleanup_attempt,
        cleanup_lease_epoch,
        cleanup_fencing_token,
        cleanup_fence,
    ) {
        return Err("checkpoint cleaned lane cleanup WorkItem fence mismatch".to_string());
    }
    let cleanup = lane_cleanup_value(&cleanup_props)
        .map_err(|_| "checkpoint cleaned lane cleanup intent missing".to_string())?;
    if cleanup.hold_id != row.hold.hold_id
        || cleanup.lane_id != row.hold.lane_id
        || cleanup.expected_hold_revision != expected_revision
    {
        return Err("checkpoint cleaned lane cleanup intent mismatch".to_string());
    }
    Ok(())
}

/// One retained hold's link into the incoming checkpoint image: its exact
/// lifecycle WorkItem, that WorkItem's status/state agreement and intent, and
/// -- once cleaned -- its distinct cleanup WorkItem.
pub(super) fn validate_checkpoint_hold_link(
    row: &DurableLaneHold,
    incoming: &IncomingNodes<'_>,
) -> Result<(), String> {
    let bytes = incoming
        .get(row.hold.work_item_id.as_str())
        .ok_or_else(|| "checkpoint would orphan a development lane hold".to_string())?;
    let props: serde_json::Map<String, serde_json::Value> =
        decode_durable(bytes).map_err(|_| "checkpoint WorkItem decode failed".to_string())?;
    let status = super::super::property_string(&props, "status");
    if checkpoint_lifecycle_fence_mismatch(&props, status, &row.hold) {
        return Err("checkpoint development lane WorkItem fence mismatch".to_string());
    }
    if !checkpoint_lifecycle_status_matches(row, status) {
        return Err("checkpoint development lane WorkItem status/state mismatch".to_string());
    }
    let intent = lane_intent_value(&props)
        .map_err(|_| "checkpoint development lane intent missing".to_string())?;
    if !lane_intent_matches_hold(
        Some(&intent),
        &row.hold,
        row.ttl_ms,
        &row.resource_reservation_id,
    ) {
        return Err("checkpoint development lane intent mismatch".to_string());
    }
    if row.hold.state != DevelopmentLaneHoldState::Cleaned {
        return Ok(());
    }
    validate_checkpoint_cleanup_link(row, incoming)
}

/// Ordinary in-place checkpoints originate in `GraphCore` and carry no lane
/// tables. Durable read-only `GraphDump` materializations are not accepted as
/// checkpoints or transfer images. Native rows therefore remain in place and
/// the replacement image must prove every
/// retained hold still has its exact immutable/fenced lifecycle WorkItem (and,
/// after cleanup, its distinct cleanup WorkItem correlation).
/// This is the lane equivalent of RMDD-27's resource-link validation and
/// prevents restore from either discarding a live authority or preserving one
/// whose WorkItem vanished from the incoming image.
pub(crate) fn validate_checkpoint_lane_links<T>(
    graph: &str,
    incoming_nodes: &[(String, Vec<u8>)],
    holds: &T,
    crypto: DurableCrypto<'_>,
) -> Result<(), String>
where
    T: LaneRows<(&'static str, &'static str), &'static [u8]>,
{
    text(graph, "lane graph").map_err(|_| "development lane graph key is invalid".to_string())?;
    // `incoming_nodes` is the checkpoint's FULL node set for `graph` — every node
    // the graph carries, not just development-lane WorkItems (e.g. `__commons__`
    // also carries broker exchange/binding/message nodes whose ids intentionally
    // use a `\u{1}` control-byte delimiter — see `broker::binding_node_id` — which
    // is a legal graph node id but not `text()`-bounded "WorkItem id" text).
    // Bound-checking every incoming id against the WorkItem id format here would
    // reject an entire checkpoint over an unrelated node's id shape. The actual
    // WorkItem ids this function cares about (`row.hold.work_item_id` /
    // `cleanup_work_item_id`) are already bound-checked at the point they matter —
    // `durable_hold_bounds` below validates the STORED hold's own `work_item_id`
    // field before it is ever used as a lookup key into `incoming`. Mirrors
    // `work_item_capability::validate_snapshot_nodes`'s same content-shape-scoped
    // (not id-format-universal) convention for this same checkpoint path.
    let incoming = checkpoint_incoming_nodes(incoming_nodes)?;
    holds.visit_scope_rows(&mut |_: (&str, &str), value: &[u8]| {
        let row: DurableLaneHold = resource_decode(value, crypto)?;
        durable_hold_bounds(&row)?;
        if matches!(
            row.hold.state,
            DevelopmentLaneHoldState::Absent | DevelopmentLaneHoldState::Aborted
        ) {
            return Ok(true);
        }
        validate_checkpoint_hold_link(&row, &incoming)?;
        Ok(true)
    })
}

/// Validate a replacement image against the lane rows already staged in the
/// caller's admitted write.  Snapshot/row-delta commits use this seam before
/// their transaction can commit, so a WorkItem replacement cannot orphan a live
/// or retained lane authority.
pub(crate) fn validate_lane_links_in_wtx(
    write: &ShardWrite<'_>,
    graph: &str,
    incoming_nodes: &[(String, Vec<u8>)],
    crypto: DurableCrypto<'_>,
) -> Result<(), String> {
    let holds = write.graph(graph)?.open_scoped_table(HOLDS)?;
    validate_checkpoint_lane_links(graph, incoming_nodes, &holds, crypto)
}

/// Validate the current post-delta WorkItem image from the same admitted write.
/// Node values are unsealed before they are passed to the checkpoint validator,
/// preserving the exact WorkItem extension checks while keeping the lane and
/// graph replacement atomic.
pub(crate) fn validate_current_lane_links_in_wtx(
    write: &ShardWrite<'_>,
    graph: &str,
    crypto: DurableCrypto<'_>,
) -> Result<(), String> {
    let incoming_nodes = {
        let nodes = write.graph(graph)?.open_scoped_table(NODES)?;
        let mut incoming_nodes = Vec::new();
        nodes.visit_scope_rows(&mut |key: (&str, &str), value: &[u8]| {
            incoming_nodes.push((key.1.to_string(), crypto.unseal(value)?));
            Ok(true)
        })?;
        incoming_nodes
    };
    validate_lane_links_in_wtx(write, graph, &incoming_nodes, crypto)
}

/// The WorkItem statuses that mean the lifecycle attempt has finished.
pub(super) fn checkpoint_terminal_status(status: &str) -> bool {
    matches!(status, "succeeded" | "failed" | "cancelled" | "dead_letter")
}

/// A live hold must still have the current lease claim and no terminal replay
/// tuple.  Ready/pending rows are not authoritative claims.
pub(super) fn checkpoint_live_status_matches(row: &DurableLaneHold, status: &str) -> bool {
    matches!(status, "leased" | "running")
        && !checkpoint_terminal_status(status)
        && row.terminal_state.is_none()
        && row.terminal_expected_hold_revision.is_none()
}

/// Expiry can race the WorkItem terminal transition.  Both a still-live
/// leased/running claim and an exact terminal outcome remain cleanable, but
/// neither may carry a finish terminal replay tuple.
pub(super) fn checkpoint_expired_status_matches(row: &DurableLaneHold, status: &str) -> bool {
    (matches!(status, "leased" | "running") || checkpoint_terminal_status(status))
        && row.terminal_state.is_none()
        && row.terminal_expected_hold_revision.is_none()
        && row.hold.tombstone
        && !row.hold.active_count_charged
}

/// Finish records the exact terminal outcome and the pre-finish hold revision.
/// Every post-finish retained state shares these invariants; they differ only
/// in whether the retained charge has been released yet.
pub(super) fn checkpoint_terminal_outcome_matches(row: &DurableLaneHold, status: &str) -> bool {
    checkpoint_terminal_status(status)
        && row
            .terminal_state
            .as_deref()
            .is_some_and(|expected| expected == status)
        && row.terminal_expected_hold_revision.is_some()
        && row.hold.tombstone
        && !row.hold.active_count_charged
}

pub(super) fn checkpoint_lifecycle_status_matches(row: &DurableLaneHold, status: &str) -> bool {
    match row.hold.state {
        DevelopmentLaneHoldState::Allocating
        | DevelopmentLaneHoldState::Active
        | DevelopmentLaneHoldState::Submitted => checkpoint_live_status_matches(row, status),
        // Released is a terminal retained state in the durable vocabulary; it
        // carries the same exact outcome mapping as CleanupPending, and both
        // still hold their retained charge.
        DevelopmentLaneHoldState::CleanupPending | DevelopmentLaneHoldState::Released => {
            checkpoint_terminal_outcome_matches(row, status) && row.hold.retained_disk_bytes != 0
        }
        DevelopmentLaneHoldState::Expired => checkpoint_expired_status_matches(row, status),
        // Cleanup has released the retained charge, but the lifecycle terminal
        // outcome and replay revision remain bound to the tombstone.
        DevelopmentLaneHoldState::Cleaned => {
            checkpoint_terminal_outcome_matches(row, status) && row.hold.retained_disk_bytes == 0
        }
        DevelopmentLaneHoldState::Aborted | DevelopmentLaneHoldState::Absent => true,
    }
}

/// Validate persisted hold identifiers before they can be used as a table
/// lookup key or scope component.  Native rows are encrypted, but encryption
/// authenticates bytes; it does not make a corrupt/old row safe to feed into
/// redb or a pressure index.  Reconciliation and graph lifecycle therefore
/// fail closed on the same bounded vocabulary as fresh requests.
pub(super) fn durable_hold_bounds(row: &DurableLaneHold) -> Result<(), String> {
    durable_hold_text_bounds(row)?;
    durable_hold_quota_bounds(row)?;
    durable_hold_fence_bounds(row)?;
    durable_hold_terminal_bounds(row)
}

/// Bounded text identity, fingerprints, and host placement of a stored hold.
pub(super) fn durable_hold_text_bounds(row: &DurableLaneHold) -> Result<(), String> {
    bounded_texts(&[
        (&row.hold.hold_id, "stored hold"),
        (&row.hold.lane_id, "stored lane"),
        (&row.hold.tenant_ref, "stored tenant"),
        (&row.hold.request_id, "stored request"),
        (&row.hold.work_item_id, "stored WorkItem"),
        (&row.hold.owner_id, "stored owner"),
        (&row.hold.session_id, "stored session"),
        (&row.hold.fairness_group, "stored fairness group"),
        (&row.hold.workspace_ref, "stored workspace"),
        (&row.hold.repository_id, "stored repository"),
        (&row.hold.base_ref, "stored base ref"),
        (&row.hold.branch, "stored branch"),
        (&row.hold.host_ref, "stored host"),
        (&row.resource_reservation_id, "stored resource reservation"),
        (&row.hold.work_item_fence, "stored WorkItem fence"),
        (&row.hold.quota_policy_name, "stored policy name"),
        (&row.hold.quota_policy_version, "stored policy version"),
    ])
    .map_err(|decision| format!("stored development lane hold: {}", decision_name(decision)))?;
    fingerprint(&row.hold.hold_id)
        .map_err(|decision| format!("stored development lane hold: {}", decision_name(decision)))?;
    base_sha(&row.hold.base_sha)
        .map_err(|decision| format!("stored development lane hold: {}", decision_name(decision)))?;
    relative_locator(&row.hold.worktree_locator)
        .map_err(|decision| format!("stored development lane hold: {}", decision_name(decision)))?;
    fingerprint(&row.hold.input_fingerprint)
        .map_err(|decision| format!("stored development lane hold: {}", decision_name(decision)))?;
    if let Some(alias) = row.hold.host_target_alias.as_deref() {
        text(alias, "stored host alias").map_err(|decision| {
            format!("stored development lane hold: {}", decision_name(decision))
        })?;
    }
    if matches!(
        (
            row.hold.host_target_kind,
            row.hold.host_target_alias.is_some()
        ),
        (DevelopmentLaneHoldHostTargetKind::Local, true)
            | (DevelopmentLaneHoldHostTargetKind::InventoryAlias, false)
    ) {
        return Err("stored lane host target does not match its alias".to_string());
    }
    Ok(())
}

/// Quota charge magnitudes recorded on a stored hold.
pub(super) fn durable_hold_quota_bounds(row: &DurableLaneHold) -> Result<(), String> {
    for (value, name) in [
        (row.hold.predicted_disk_bytes, "stored predicted disk"),
        (row.hold.observed_disk_bytes, "stored observed disk"),
        (row.hold.retained_disk_bytes, "stored retained disk"),
    ] {
        if value > MAX_DISK_BYTES {
            return Err(format!("{name} exceeds native bound"));
        }
    }
    if [
        row.hold.quota_charge.tenant_count,
        row.hold.quota_charge.owner_count,
        row.hold.quota_charge.session_count,
        row.hold.quota_charge.workspace_count,
        row.hold.quota_charge.repository_count,
        row.hold.quota_charge.host_count,
        row.hold.quota_charge.global_count,
    ]
    .into_iter()
    .any(|value| value > MAX_COUNT)
    {
        return Err("stored lane quota count exceeds native bound".to_string());
    }
    if [
        row.hold.quota_charge.tenant_predicted_disk_bytes,
        row.hold.quota_charge.owner_predicted_disk_bytes,
        row.hold.quota_charge.session_predicted_disk_bytes,
        row.hold.quota_charge.workspace_predicted_disk_bytes,
        row.hold.quota_charge.repository_predicted_disk_bytes,
        row.hold.quota_charge.host_predicted_disk_bytes,
        row.hold.quota_charge.global_predicted_disk_bytes,
        row.hold.quota_charge.tenant_observed_disk_bytes,
        row.hold.quota_charge.owner_observed_disk_bytes,
        row.hold.quota_charge.session_observed_disk_bytes,
        row.hold.quota_charge.workspace_observed_disk_bytes,
        row.hold.quota_charge.repository_observed_disk_bytes,
        row.hold.quota_charge.host_observed_disk_bytes,
        row.hold.quota_charge.global_observed_disk_bytes,
        row.hold.quota_charge.tenant_retained_disk_bytes,
        row.hold.quota_charge.owner_retained_disk_bytes,
        row.hold.quota_charge.session_retained_disk_bytes,
        row.hold.quota_charge.workspace_retained_disk_bytes,
        row.hold.quota_charge.repository_retained_disk_bytes,
        row.hold.quota_charge.host_retained_disk_bytes,
        row.hold.quota_charge.global_retained_disk_bytes,
    ]
    .into_iter()
    .any(|value| value > MAX_DISK_BYTES)
    {
        return Err("stored lane quota disk charge exceeds native bound".to_string());
    }
    if row.hold.quota_charge.revision > MAX_COUNT
        || row.hold.quota_charge.policy_revision > MAX_COUNT
    {
        return Err("stored lane quota revision exceeds native bound".to_string());
    }
    Ok(())
}

/// Every stored fence/revision counter, checked as two bounded tables rather
/// than one eleven-term disjunction: the lease tuple must be a non-zero
/// counter, the revisions merely bounded.
pub(super) fn durable_hold_fence_out_of_bounds(row: &DurableLaneHold) -> bool {
    let lease_counters = [
        row.hold.attempt,
        row.hold.lease_epoch,
        row.hold.fencing_token,
    ];
    let revisions = [
        row.observation_revision,
        row.hold.hold_revision,
        row.hold.lifecycle_revision,
        row.hold.allocation_revision,
        row.hold.cleanup_revision,
    ];
    lease_counters
        .into_iter()
        .any(|value| !(1..=MAX_COUNT).contains(&value))
        || revisions.into_iter().any(|value| value > MAX_COUNT)
}

/// TTL, fence and observation-timestamp bounds of a stored hold.
pub(super) fn durable_hold_fence_bounds(row: &DurableLaneHold) -> Result<(), String> {
    if row.ttl_ms == 0 || row.ttl_ms > MAX_TTL_MS {
        return Err("stored lane TTL exceeds native bound".to_string());
    }
    if durable_hold_fence_out_of_bounds(row) {
        return Err("stored lane fence is invalid".to_string());
    }
    if row.hold.last_renewed_at_ms > row.hold.expires_at_ms
        || row
            .last_observed_at_ms
            .is_some_and(|observed_at| observed_at > row.hold.expires_at_ms)
    {
        return Err("stored lane observation timestamp is invalid".to_string());
    }
    Ok(())
}

/// Optional cleanup fence counters and terminal replay revisions.
pub(super) fn durable_hold_optional_counter_bounds(row: &DurableLaneHold) -> Result<(), String> {
    for (value, name) in [
        (row.hold.cleanup_attempt, "stored cleanup attempt"),
        (row.hold.cleanup_lease_epoch, "stored cleanup lease epoch"),
        (
            row.hold.cleanup_fencing_token,
            "stored cleanup fencing token",
        ),
        (
            row.terminal_expected_hold_revision,
            "stored terminal hold revision",
        ),
        (
            row.cleanup_expected_hold_revision,
            "stored cleanup hold revision",
        ),
        (
            row.terminal_source_attempt,
            "stored terminal source attempt",
        ),
        (
            row.terminal_source_lease_epoch,
            "stored terminal source lease epoch",
        ),
        (
            row.terminal_source_fencing_token,
            "stored terminal source fencing token",
        ),
    ] {
        if let Some(value) = value {
            if value == 0 || value > MAX_COUNT {
                return Err(format!("{name} exceeds native bound"));
            }
        }
    }
    Ok(())
}

/// The optional terminal outcome, cleanup correlation, and terminal-source
/// replay tuple of a stored hold.
pub(super) fn durable_hold_terminal_bounds(row: &DurableLaneHold) -> Result<(), String> {
    if let Some(value) = row.terminal_state.as_deref() {
        text(value, "stored terminal state").map_err(|decision| {
            format!("stored development lane hold: {}", decision_name(decision))
        })?;
        if !matches!(value, "succeeded" | "failed" | "cancelled" | "dead_letter") {
            return Err("stored development lane terminal state is invalid".to_string());
        }
    }
    for (value, name) in [
        (
            row.hold.cleanup_work_item_id.as_deref(),
            "stored cleanup WorkItem",
        ),
        (
            row.hold.cleanup_work_item_fence.as_deref(),
            "stored cleanup fence",
        ),
    ] {
        if let Some(value) = value {
            text(value, name).map_err(|decision| {
                format!("stored development lane hold: {}", decision_name(decision))
            })?;
        }
    }
    durable_hold_optional_counter_bounds(row)?;
    if let Some(value) = row.cleanup_removal_proof_ref.as_deref() {
        text(value, "stored cleanup removal proof").map_err(|decision| {
            format!("stored development lane hold: {}", decision_name(decision))
        })?;
    }
    if let Some(value) = row.terminal_source_work_item_fence.as_deref() {
        text(value, "stored terminal source WorkItem fence").map_err(|decision| {
            format!("stored development lane hold: {}", decision_name(decision))
        })?;
    }
    let terminal_source_fields = [
        row.terminal_source_attempt.is_some(),
        row.terminal_source_lease_epoch.is_some(),
        row.terminal_source_fencing_token.is_some(),
        row.terminal_source_work_item_fence.is_some(),
    ];
    if terminal_source_fields.iter().any(|present| *present)
        && terminal_source_fields.iter().any(|present| !*present)
    {
        return Err("stored terminal source tuple is incomplete".to_string());
    }
    Ok(())
}

pub(super) fn text(value: &str, name: &str) -> Result<(), LaneDecision> {
    if value.is_empty()
        || value.len() > MAX_TEXT
        || value
            .as_bytes()
            .iter()
            .any(|byte| *byte == 0 || *byte < 0x20)
    {
        return Err(LaneDecision::Invalid);
    }
    let _ = name;
    Ok(())
}

pub(super) fn fingerprint(value: &str) -> Result<(), LaneDecision> {
    if value.len() != MAX_FINGERPRINT
        || !value.starts_with("v1:")
        || !value.as_bytes()[3..].iter().all(u8::is_ascii_hexdigit)
        || value.as_bytes()[3..].iter().any(u8::is_ascii_uppercase)
    {
        return Err(LaneDecision::Invalid);
    }
    Ok(())
}

pub(super) fn base_sha(value: &str) -> Result<(), LaneDecision> {
    if !matches!(value.len(), 40 | 64)
        || !value.as_bytes().iter().all(u8::is_ascii_hexdigit)
        || value.as_bytes().iter().any(u8::is_ascii_uppercase)
    {
        return Err(LaneDecision::Invalid);
    }
    Ok(())
}

pub(super) fn relative_locator(value: &str) -> Result<(), LaneDecision> {
    text(value, "worktree_locator")?;
    if value.starts_with('/')
        || value.starts_with('\\')
        || value.contains('\\')
        || value
            .split('/')
            .any(|part| part.is_empty() || part == "." || part == "..")
    {
        return Err(LaneDecision::Invalid);
    }
    Ok(())
}

pub(super) fn intent_validate(intent: &DevelopmentLaneIntent) -> Result<(), LaneDecision> {
    text(&intent.tenant_ref, "intent tenant")?;
    text(&intent.request_id, "intent request")?;
    text(&intent.lane_id, "intent lane")?;
    text(&intent.repository_id, "intent repository")?;
    text(&intent.base_ref, "intent base_ref")?;
    base_sha(&intent.base_sha)?;
    text(&intent.branch, "intent branch")?;
    text(&intent.workspace_ref, "intent workspace")?;
    relative_locator(&intent.worktree_locator)?;
    text(&intent.owner_id, "intent owner")?;
    text(&intent.session_id, "intent session")?;
    text(&intent.fairness_group, "intent fairness")?;
    text(&intent.quota_policy_name, "intent policy")?;
    text(&intent.quota_policy_version, "intent policy version")?;
    fingerprint(&intent.input_fingerprint)?;
    if intent.predicted_disk_bytes == 0
        || intent.predicted_disk_bytes > MAX_DISK_BYTES
        || intent.ttl_ms == 0
        || intent.ttl_ms > MAX_TTL_MS
    {
        return Err(LaneDecision::Invalid);
    }
    match intent.host_target_kind {
        DevelopmentLaneIntentHostTargetKind::Local if intent.host_target_alias.is_some() => {
            return Err(LaneDecision::Invalid);
        }
        DevelopmentLaneIntentHostTargetKind::InventoryAlias
            if intent.host_target_alias.is_none() =>
        {
            return Err(LaneDecision::Invalid)
        }
        _ => {}
    }
    if let Some(alias) = intent.host_target_alias.as_deref() {
        text(alias, "intent host alias")?;
    }
    text(&intent.host_ref, "intent host ref")?;
    text(
        &intent.resource_reservation_id,
        "intent resource reservation id",
    )?;
    Ok(())
}

pub(super) fn repository_work_item_extension(
    props: &serde_json::Map<String, serde_json::Value>,
) -> Result<&serde_json::Map<String, serde_json::Value>, LaneDecision> {
    props
        .get(WORK_ITEM_METADATA_KEY)
        .and_then(serde_json::Value::as_object)
        .and_then(|metadata| metadata.get(REPOSITORY_WORK_ITEM_EXTENSION_KEY))
        .and_then(serde_json::Value::as_object)
        .ok_or(LaneDecision::InputConflict)
}

pub(super) fn lane_intent_value(
    props: &serde_json::Map<String, serde_json::Value>,
) -> Result<DevelopmentLaneIntent, LaneDecision> {
    let extension = repository_work_item_extension(props)?;
    let value = extension
        .get(LANE_INTENT_EXTENSION_KEY)
        .cloned()
        .ok_or(LaneDecision::InputConflict)?;
    serde_json::from_value(value).map_err(|_| LaneDecision::InputConflict)
}

pub(super) fn lane_cleanup_value(
    props: &serde_json::Map<String, serde_json::Value>,
) -> Result<DevelopmentLaneCleanupIntent, LaneDecision> {
    let extension = repository_work_item_extension(props)?;
    let value = extension
        .get(LANE_CLEANUP_EXTENSION_KEY)
        .cloned()
        .ok_or(LaneDecision::InputConflict)?;
    let correlation: DevelopmentLaneCleanupIntent =
        serde_json::from_value(value).map_err(|_| LaneDecision::InputConflict)?;
    if !matches!(
        correlation.schema_version,
        DevelopmentLaneCleanupIntentSchemaVersion::V1
    ) || correlation.hold_id.is_empty()
        || correlation.lane_id.is_empty()
        || correlation.expected_hold_revision == 0
    {
        return Err(LaneDecision::InputConflict);
    }
    fingerprint(&correlation.hold_id)?;
    text(&correlation.lane_id, "cleanup lane id")?;
    Ok(correlation)
}
