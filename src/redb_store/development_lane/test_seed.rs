use super::*;

/// One admitted maintenance write over `TEST_GRAPH`, committed when
/// `write` answers `Ok` and aborted when it does not.
///
/// Tests seed rows through the kernel for the same reason production does:
/// a raw `redb::Database` on a shard file is row-level authority over every
/// graph it hosts.  The one exception below plants a deliberately corrupt
/// row, which no capability will accept.
pub(super) fn shard_write<R>(
    shard: &Shard,
    tag: &str,
    write: impl FnOnce(&ShardWrite<'_>) -> Result<R, String>,
) -> Result<R, String> {
    let members = shard.graph_members(&[TEST_GRAPH])?;
    let op_id = lane_op_id(TEST_GRAPH, tag, TEST_NOW);
    let (group, batches) = shard.admit_maintenance(&members, &op_id)?;
    let admitted = ShardWrite::open(shard, &group, &members, &batches)?;
    let outcome = write(&admitted);
    admitted.finish()?;
    match outcome {
        Ok(value) => {
            shard.commit_drain(group, &batches, TEST_NOW)?;
            Ok(value)
        }
        Err(error) => {
            shard.mutations().abort_group(group)?;
            Err(error)
        }
    }
}

pub(super) fn seed_lane_work_item(
    shard: &Shard,
    request: &DevelopmentLaneReserveRequest,
) -> Result<(), String> {
    let intent = &request.intent;
    let props = serde_json::json!({
        "node_type": "WorkItem",
        "kind": "lane.lifecycle",
        "tenant": request.tenant_ref,
        "status": "running",
        "lease_owner": request.owner_id,
        "last_lease_owner": request.owner_id,
        "attempt": request.attempt,
        // Keep the native retry regression below on the pre-DLQ path.
        "max_attempts": 2,
        "lease_epoch": request.lease_epoch,
        "fencing_token": request.fencing_token,
        "work_item_fence": request.work_item_fence,
        "lease_expires_at": 1000.0,
        "metadata": {
            "repository_work_item": {
                "development_lane_intent": serde_json::to_value(intent)
                    .map_err(|error| error.to_string())?
            }
        }
    })
    .as_object()
    .ok_or_else(|| "lane test WorkItem properties are not an object".to_string())?
    .clone();
    let node_bytes = rmp_serde::to_vec_named(&props).map_err(|error| error.to_string())?;
    let requirement = ResourceRequirement {
        cpu_weight: 1,
        memory_mib: 1,
        disk_mib: intent.predicted_disk_bytes,
        process_slots: 1,
    };
    let record = ResourceReservationRecord {
        reservation_id: intent.resource_reservation_id.clone(),
        tenant_ref: request.tenant_ref.clone(),
        owner_id: request.owner_id.clone(),
        work_item_id: request.work_item_id.clone(),
        fence: request.work_item_fence.clone(),
        attempt: request.attempt,
        lease_epoch: request.lease_epoch,
        fencing_token: request.fencing_token,
        input_fingerprint: intent.input_fingerprint.clone(),
        host_ref: intent.host_ref.clone(),
        profile_name: "lane-profile".into(),
        profile_version: "1".into(),
        requirement: requirement.clone(),
        capacity_snapshot: ResourceCapacitySnapshot {
            cpu_weight: 10,
            memory_mib: 10,
            disk_mib: 10_000,
            process_slots: 10,
            host_revision: 1,
        },
        selected_target: ResourceTargetSnapshot {
            kind: ResourceTargetSnapshotKind::Local,
            alias: None,
            capability_labels: Vec::new(),
        },
        target_kind: ResourceReservationRecordTargetKind::Local,
        target_alias: None,
        repository_id: intent.repository_id.clone(),
        branch: intent.branch.clone(),
        concurrency_key: "lane".into(),
        concurrency_limit: None,
        repository_exclusive: false,
        branch_exclusive: false,
        required_labels: Vec::new(),
        anti_affinity: Vec::new(),
        fairness_group: intent.fairness_group.clone(),
        fairness_cost: 1,
        disk_low_watermark_mib: None,
        disk_high_watermark_mib: None,
        disk_policy_key: "lane".into(),
        reserved_at_ms: 1,
        expires_at_ms: 100_000,
        expected_host_revision: Some(1),
        expected_lifecycle_revision: None,
        state: ResourceReservationRecordState::Reserved,
        revision: 1,
        lifecycle_revision: 1,
        tombstone: false,
    };
    let resource = super::super::super::DurableResourceReservation {
        record,
        held_cpu_weight: requirement.cpu_weight,
        held_memory_mib: requirement.memory_mib,
        held_disk_mib: requirement.disk_mib,
        held_process_slots: requirement.process_slots,
        fairness_debt: 0,
    };
    let resource_bytes = resource_encode(&resource, DurableCrypto::none())?;
    shard_write(shard, "seed-work-item", |write| {
        let member = write.graph(TEST_GRAPH)?;
        member.open_scoped_table(NODES)?.insert(
            (TEST_GRAPH, request.work_item_id.as_str()),
            node_bytes.as_slice(),
        )?;
        member
            .open_scoped_table(super::super::super::RESOURCE_RESERVATIONS)?
            .insert(
                (TEST_GRAPH, intent.resource_reservation_id.as_str()),
                resource_bytes.as_slice(),
            )
    })
}
