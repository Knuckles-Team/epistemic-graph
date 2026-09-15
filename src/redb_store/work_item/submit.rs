//! WorkItem submit.rs transitions.

use super::*;

pub(crate) fn validate_submit_work_item_identity(
    graph: &str,
    request: &eg_types::native_control::SubmitWorkItemRequest,
) -> Result<(), String> {
    use eg_types::native_control::{NativeControlSchemaVersion, MAX_SUBMIT_REF_BYTES};
    if request.schema_version != NativeControlSchemaVersion::V1 {
        return Err("SubmitWorkItem schema_version must be 1".to_string());
    }
    let context_tenant = request.context.tenant_id.as_str();
    let context_graph = request.context.graph.as_str();
    if context_tenant.trim().is_empty() || sanitize(context_graph) != graph {
        return Err("SubmitWorkItem context graph/tenant is invalid".to_string());
    }
    for (field, value) in [
        ("idempotency_key", request.idempotency_key.as_str()),
        ("command_digest", request.command_digest.as_str()),
        ("kind", request.kind.as_str()),
        ("policy_digest", request.policy_digest.as_str()),
        ("catalog_digest", request.catalog_digest.as_str()),
        ("model_digest", request.model_digest.as_str()),
    ] {
        if value.trim().is_empty() || value.len() > MAX_SUBMIT_REF_BYTES {
            return Err(format!("SubmitWorkItem {field} is outside native bounds"));
        }
    }
    Ok(())
}

pub(crate) fn validate_submit_work_item_refs(
    request: &eg_types::native_control::SubmitWorkItemRequest,
) -> Result<(), String> {
    use eg_types::native_control::{MAX_SUBMIT_DEPENDENCIES, MAX_SUBMIT_REF_BYTES};
    if request.command_digest.len() != 64
        || !request
            .command_digest
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit())
    {
        return Err("SubmitWorkItem command_digest must be a SHA-256 hex value".to_string());
    }
    if request.input_ref.len() > MAX_SUBMIT_REF_BYTES {
        return Err("SubmitWorkItem input_ref exceeds native payload bound".to_string());
    }
    if request.depends_on.len() > MAX_SUBMIT_DEPENDENCIES {
        return Err("SubmitWorkItem dependency count exceeds native bound".to_string());
    }
    Ok(())
}

pub(crate) fn validate_submit_work_item_identity_and_refs(
    graph: &str,
    request: &eg_types::native_control::SubmitWorkItemRequest,
) -> Result<(), String> {
    validate_submit_work_item_identity(graph, request)?;
    validate_submit_work_item_refs(request)?;
    Ok(())
}

pub(crate) fn resolve_submit_work_item_max_inflight(
    request: &eg_types::native_control::SubmitWorkItemRequest,
) -> Result<u64, String> {
    let max_inflight = if request.max_tenant_in_flight == 0 {
        4096
    } else {
        request.max_tenant_in_flight
    };
    if !(1..=4096).contains(&max_inflight)
        || request.max_attempts == 0
        || request.max_attempts > 4096
        || !(-1024..=1024).contains(&request.priority)
    {
        return Err(
            "SubmitWorkItem admission/max_attempts/priority is outside native bounds".to_string(),
        );
    }
    if request
        .deadline_unix
        .is_some_and(|deadline| !deadline.is_finite() || deadline < 0.0)
    {
        return Err("SubmitWorkItem deadline_unix is invalid".to_string());
    }
    Ok(max_inflight)
}

pub(crate) fn validate_submit_work_item_provenance_and_metadata(
    request: &eg_types::native_control::SubmitWorkItemRequest,
) -> Result<(), String> {
    use eg_types::native_control::{
        MAX_SUBMIT_METADATA_BYTES, MAX_SUBMIT_PROVENANCE_REFS, MAX_SUBMIT_REF_BYTES,
    };
    if request.provenance_refs.len() > MAX_SUBMIT_PROVENANCE_REFS
        || request
            .provenance_refs
            .iter()
            .any(|reference| reference.trim().is_empty() || reference.len() > MAX_SUBMIT_REF_BYTES)
    {
        return Err("SubmitWorkItem provenance_refs exceed native bounds".to_string());
    }
    let metadata_bytes = rmp_serde::to_vec_named(&request.metadata).map_err(|e| e.to_string())?;
    if metadata_bytes.len() > MAX_SUBMIT_METADATA_BYTES {
        return Err("SubmitWorkItem metadata exceeds native bound".to_string());
    }
    Ok(())
}

pub(crate) fn resolve_submit_work_item_admission_limits(
    request: &eg_types::native_control::SubmitWorkItemRequest,
) -> Result<u64, String> {
    let max_inflight = resolve_submit_work_item_max_inflight(request)?;
    validate_submit_work_item_provenance_and_metadata(request)?;
    Ok(max_inflight)
}

pub(crate) fn check_submit_work_item_dependencies_unique(
    request: &eg_types::native_control::SubmitWorkItemRequest,
) -> Result<Vec<String>, String> {
    let mut dependencies = request.depends_on.clone();
    dependencies.sort();
    dependencies.dedup();
    if dependencies.len() != request.depends_on.len() {
        return Err("SubmitWorkItem dependencies must be unique".to_string());
    }
    Ok(dependencies)
}

// Resolve the dependency state in this same write snapshot.  A successful
// parent is already satisfied; every other existing WorkItem remains a
// counted dependency and receives a downstream index entry below.  The
// index is what the existing terminal WorkItem transition uses for atomic
// push-release, so native submit must populate it rather than relying on a
// later graph scan.
pub(crate) fn resolve_submit_work_item_dependency_row(
    graph: &str,
    dependency: &str,
    context_tenant: &str,
    nodes: &mut ScopedOwnerTableMut<'_, (&str, &str), &[u8]>,
    crypto: DurableCrypto<'_>,
) -> Result<bool, String> {
    if dependency.trim().is_empty() || dependency.len() > 512 {
        return Err("SubmitWorkItem dependency id is outside native bounds".to_string());
    }
    let value = nodes
        .get((graph, dependency))?
        .ok_or_else(|| format!("SubmitWorkItem dependency '{dependency}' was not found"))?;
    let props: serde_json::Map<String, serde_json::Value> =
        decode_durable(&crypto.unseal(value.value())?)?;
    if property_string(&props, "node_type") != "WorkItem" {
        return Err(format!(
            "SubmitWorkItem dependency '{dependency}' is not a WorkItem"
        ));
    }
    if property_string(&props, "tenant") != context_tenant {
        return Err("ACCESS_DENIED: WorkItem dependency tenant mismatch".to_string());
    }
    Ok(property_string(&props, "status") != "succeeded")
}

pub(crate) fn resolve_submit_work_item_dependency_rows(
    graph: &str,
    dependencies: &[String],
    context_tenant: &str,
    nodes: &mut ScopedOwnerTableMut<'_, (&str, &str), &[u8]>,
    crypto: DurableCrypto<'_>,
) -> Result<Vec<(String, bool)>, String> {
    let mut dependency_rows = Vec::with_capacity(dependencies.len());
    for dependency in dependencies {
        let pending = resolve_submit_work_item_dependency_row(
            graph,
            dependency,
            context_tenant,
            nodes,
            crypto,
        )?;
        dependency_rows.push((dependency.clone(), pending));
    }
    Ok(dependency_rows)
}

/// A submit request's resolved admission inputs, in order: the max-inflight
/// bound, the declared dependency ids, and each dependency's `(id, satisfied)` row.
pub(crate) type SubmitWorkItemDependencies = (u64, Vec<String>, Vec<(String, bool)>);

pub(crate) fn validate_and_resolve_submit_work_item_dependencies(
    graph: &str,
    request: &eg_types::native_control::SubmitWorkItemRequest,
    nodes: &mut ScopedOwnerTableMut<'_, (&str, &str), &[u8]>,
    crypto: DurableCrypto<'_>,
) -> Result<SubmitWorkItemDependencies, String> {
    validate_submit_work_item_identity_and_refs(graph, request)?;
    let max_inflight = resolve_submit_work_item_admission_limits(request)?;
    let dependencies = check_submit_work_item_dependencies_unique(request)?;
    let dependency_rows = resolve_submit_work_item_dependency_rows(
        graph,
        &dependencies,
        request.context.tenant_id.as_str(),
        nodes,
        crypto,
    )?;
    Ok((max_inflight, dependencies, dependency_rows))
}

pub(crate) fn is_submit_work_item_inflight_row(
    props: &serde_json::Map<String, serde_json::Value>,
    context_tenant: &str,
) -> bool {
    property_string(props, "node_type") == "WorkItem"
        && property_string(props, "tenant") == context_tenant
        && !matches!(
            property_string(props, "status"),
            "succeeded" | "failed" | "cancelled" | "dead_letter" | "completed"
        )
}

pub(crate) fn count_submit_work_item_tenant_inflight(
    graph: &str,
    context_tenant: &str,
    nodes: &mut ScopedOwnerTableMut<'_, (&str, &str), &[u8]>,
    crypto: DurableCrypto<'_>,
) -> Result<u64, String> {
    permit_scoped_scan(nodes.scope_key(), graph)?;
    let mut inflight = 0u64;
    let mut scanned = 0usize;
    for row in nodes.scope_rows()? {
        let (_, value) = row?;
        scanned += 1;
        if scanned > 50_000 {
            return Err(
                "ADMISSION_BACKPRESSURE: WorkItem quota scan exceeds native bound".to_string(),
            );
        }
        let props: serde_json::Map<String, serde_json::Value> =
            decode_durable(&crypto.unseal(value.value())?)?;
        if is_submit_work_item_inflight_row(&props, context_tenant) {
            inflight = inflight.saturating_add(1);
        }
    }
    Ok(inflight)
}

pub(crate) fn resolve_submit_work_item_id(
    graph: &str,
    context_tenant: &str,
    request: &eg_types::native_control::SubmitWorkItemRequest,
    nodes: &mut ScopedOwnerTableMut<'_, (&str, &str), &[u8]>,
) -> Result<String, String> {
    use sha2::{Digest, Sha256};
    let work_item_id = request.work_item_id.clone().unwrap_or_else(|| {
        let mut digest = Sha256::new();
        digest.update(graph.as_bytes());
        digest.update([0]);
        digest.update(context_tenant.as_bytes());
        digest.update([0]);
        digest.update(request.idempotency_key.as_bytes());
        format!("work-item:{}", hex::encode(digest.finalize()))
    });
    if work_item_id.trim().is_empty() || work_item_id.len() > 512 {
        return Err("SubmitWorkItem work_item_id is outside native bounds".to_string());
    }
    if nodes.get((graph, work_item_id.as_str()))?.is_some() {
        return Err("IDEMPOTENCY_CONFLICT: work_item_id is already present".to_string());
    }
    Ok(work_item_id)
}

pub(crate) fn resolve_submit_work_item_command_sequence(
    graph: &str,
    command_sequences: &mut ScopedOwnerTableMut<'_, &str, u64>,
) -> Result<u64, String> {
    let found_command_sequence = command_sequences.get(graph)?;
    let command_sequence = found_command_sequence
        .map(|v| v.value())
        .unwrap_or(0)
        .checked_add(1)
        .ok_or_else(|| "WorkItem command sequence exhausted".to_string())?;
    command_sequences.insert(graph, command_sequence)?;
    Ok(command_sequence)
}

struct SubmitWorkItemAdmission<'args, 'table, 'crypto> {
    graph: &'args str,
    context_tenant: &'args str,
    request: &'args eg_types::native_control::SubmitWorkItemRequest,
    nodes: &'args mut ScopedOwnerTableMut<'table, (&'static str, &'static str), &'static [u8]>,
    command_sequences: &'args mut ScopedOwnerTableMut<'table, &'static str, u64>,
    max_inflight: u64,
    crypto: DurableCrypto<'crypto>,
}

fn admit_and_identify_submit_work_item(
    input: SubmitWorkItemAdmission<'_, '_, '_>,
) -> Result<(u64, String, u64), String> {
    let SubmitWorkItemAdmission {
        graph,
        context_tenant,
        request,
        nodes,
        command_sequences,
        max_inflight,
        crypto,
    } = input;
    let inflight = count_submit_work_item_tenant_inflight(graph, context_tenant, nodes, crypto)?;
    if inflight >= max_inflight {
        return Err(format!(
            "TENANT_QUOTA: tenant has {inflight} in-flight WorkItems (limit {max_inflight})"
        ));
    }
    let work_item_id = resolve_submit_work_item_id(graph, context_tenant, request, nodes)?;
    let command_sequence = resolve_submit_work_item_command_sequence(graph, command_sequences)?;
    Ok((inflight, work_item_id, command_sequence))
}

pub(crate) fn submit_work_item_status(pending_dependencies: usize) -> &'static str {
    if pending_dependencies == 0 {
        "ready"
    } else {
        "submitted"
    }
}

fn insert_submit_work_item_identity_props(
    props: &mut serde_json::Map<String, serde_json::Value>,
    context_tenant: &str,
    request: &eg_types::native_control::SubmitWorkItemRequest,
    status: &str,
) {
    props.insert(
        "node_type".into(),
        serde_json::Value::String("WorkItem".into()),
    );
    props.insert(
        "name".into(),
        serde_json::Value::String(format!("WorkItem: {}", request.kind)),
    );
    props.insert(
        "tenant".into(),
        serde_json::Value::String(context_tenant.to_string()),
    );
    props.insert(
        "kind".into(),
        serde_json::Value::String(request.kind.clone()),
    );
    props.insert(
        "queue".into(),
        serde_json::Value::String(request.kind.clone()),
    );
    props.insert("status".into(), serde_json::Value::String(status.into()));
    props.insert("state".into(), serde_json::Value::String(status.into()));
    props.insert("priority".into(), serde_json::Value::from(request.priority));
    props.insert(
        "prio_bucket".into(),
        serde_json::Value::from(request.priority),
    );
}

fn insert_submit_work_item_queue_props(
    props: &mut serde_json::Map<String, serde_json::Value>,
    dependencies: &[String],
    pending_dependencies: usize,
) {
    props.insert(
        "depends_on".into(),
        serde_json::Value::Array(
            dependencies
                .iter()
                .cloned()
                .map(serde_json::Value::String)
                .collect(),
        ),
    );
    props.insert(
        "dep_count".into(),
        serde_json::Value::from(pending_dependencies as u64),
    );
    props.insert(
        "downstream_ids".into(),
        serde_json::Value::Array(Vec::new()),
    );
    props.insert("next_retry_at".into(), serde_json::Value::from(0.0));
    props.insert("backoff_base_s".into(), serde_json::Value::from(1.0));
    props.insert(
        "resource_class".into(),
        serde_json::Value::String(String::new()),
    );
    props.insert(
        "fairness_group".into(),
        serde_json::Value::String(String::new()),
    );
}

fn insert_submit_work_item_lease_props(props: &mut serde_json::Map<String, serde_json::Value>) {
    props.insert("lease_owner".into(), serde_json::Value::Null);
    props.insert("last_lease_owner".into(), serde_json::Value::Null);
    props.insert("lease_epoch".into(), serde_json::Value::from(0u64));
    props.insert("fencing_token".into(), serde_json::Value::from(0u64));
    props.insert("lease_expires_at".into(), serde_json::Value::Null);
    props.insert(
        "work_item_fence".into(),
        serde_json::Value::String(String::new()),
    );
    props.insert("defer_count".into(), serde_json::Value::from(0u64));
}

fn insert_submit_work_item_artifact_props(
    props: &mut serde_json::Map<String, serde_json::Value>,
    request: &eg_types::native_control::SubmitWorkItemRequest,
) {
    props.insert(
        "input_artifact_refs".into(),
        serde_json::json!([request.input_ref]),
    );
    props.insert(
        "output_artifact_refs".into(),
        serde_json::Value::Array(Vec::new()),
    );
    props.insert(
        "payload_ref".into(),
        serde_json::Value::String(request.input_ref.clone()),
    );
    props.insert("attempt".into(), serde_json::Value::from(0u64));
    props.insert(
        "max_attempts".into(),
        serde_json::Value::from(request.max_attempts),
    );
}

fn insert_submit_work_item_identity_metadata_props(
    props: &mut serde_json::Map<String, serde_json::Value>,
    request: &eg_types::native_control::SubmitWorkItemRequest,
    now_s: f64,
) {
    props.insert("created_at".into(), serde_json::Value::from(now_s));
    props.insert("updated_at".into(), serde_json::Value::from(now_s));
    props.insert(
        "idempotency_key".into(),
        serde_json::Value::String(request.idempotency_key.clone()),
    );
    props.insert(
        "command_digest".into(),
        serde_json::Value::String(request.command_digest.to_ascii_lowercase()),
    );
    props.insert(
        "policy_digest".into(),
        serde_json::Value::String(request.policy_digest.clone()),
    );
    props.insert(
        "catalog_digest".into(),
        serde_json::Value::String(request.catalog_digest.clone()),
    );
    props.insert(
        "model_digest".into(),
        serde_json::Value::String(request.model_digest.clone()),
    );
}

fn insert_submit_work_item_provenance_props(
    props: &mut serde_json::Map<String, serde_json::Value>,
    request: &eg_types::native_control::SubmitWorkItemRequest,
    command_sequence: u64,
) -> Result<(), String> {
    props.insert(
        "metadata".into(),
        serde_json::Value::Object(request.metadata.clone().into_iter().collect()),
    );
    props.insert(
        "provenance_refs".into(),
        serde_json::Value::Array(
            request
                .provenance_refs
                .iter()
                .cloned()
                .map(serde_json::Value::String)
                .collect(),
        ),
    );
    props.insert(
        "context".into(),
        serde_json::to_value(&request.context).map_err(|e| e.to_string())?,
    );
    props.insert(
        "command_sequence".into(),
        serde_json::Value::from(command_sequence),
    );
    if let Some(deadline) = request.deadline_unix {
        props.insert("deadline_unix".into(), serde_json::Value::from(deadline));
    }
    Ok(())
}

pub(crate) fn build_submit_work_item_props(
    context_tenant: &str,
    request: &eg_types::native_control::SubmitWorkItemRequest,
    dependencies: &[String],
    pending_dependencies: usize,
    command_sequence: u64,
    now_s: f64,
    status: &str,
) -> Result<serde_json::Map<String, serde_json::Value>, String> {
    let mut props = serde_json::Map::new();
    insert_submit_work_item_identity_props(&mut props, context_tenant, request, status);
    insert_submit_work_item_queue_props(&mut props, dependencies, pending_dependencies);
    insert_submit_work_item_lease_props(&mut props);
    insert_submit_work_item_artifact_props(&mut props, request);
    insert_submit_work_item_identity_metadata_props(&mut props, request, now_s);
    insert_submit_work_item_provenance_props(&mut props, request, command_sequence)?;
    Ok(props)
}

pub(crate) fn write_submit_work_item_dependency_edges(
    graph: &str,
    work_item_id: &str,
    context_tenant: &str,
    dependencies: &[String],
    edges: &mut ScopedOwnerTableMut<'_, (&str, &str, &str, u32), &[u8]>,
    crypto: DurableCrypto<'_>,
) -> Result<(), String> {
    for dependency in dependencies {
        let edge_props = rmp_serde::to_vec_named(&serde_json::json!({
            "relationship": "DEPENDS_ON",
            "tenant": context_tenant,
        }))
        .map_err(|e| e.to_string())?;
        let sealed = crypto.seal(&edge_props);
        let ordinal = next_edge_ordinal(edges, graph, work_item_id, dependency)?;
        edges.insert(
            (graph, work_item_id, String::as_str(dependency), ordinal),
            sealed.as_ref(),
        )?;
    }
    Ok(())
}

pub(crate) fn insert_submit_work_item_downstream_id(
    parent: &mut serde_json::Map<String, serde_json::Value>,
    dependency: &str,
    work_item_id: &str,
) -> Result<bool, String> {
    let downstream = parent
        .entry("downstream_ids".to_string())
        .or_insert_with(|| serde_json::Value::Array(Vec::new()));
    let ids = downstream.as_array_mut().ok_or_else(|| {
        format!("SubmitWorkItem dependency '{dependency}' has invalid downstream index")
    })?;
    if ids.iter().any(|id| id.as_str() == Some(work_item_id)) {
        Ok(false)
    } else {
        ids.push(serde_json::Value::String(work_item_id.to_string()));
        Ok(true)
    }
}

pub(crate) fn update_submit_work_item_downstream_row(
    graph: &str,
    work_item_id: &str,
    dependency: &str,
    nodes: &mut ScopedOwnerTableMut<'_, (&str, &str), &[u8]>,
    crypto: DurableCrypto<'_>,
) -> Result<(), String> {
    let mut parent: serde_json::Map<String, serde_json::Value> = {
        let value = nodes
            .get((graph, dependency))?
            .ok_or_else(|| format!("SubmitWorkItem dependency '{dependency}' disappeared"))?;
        decode_durable(&crypto.unseal(value.value())?)?
    };
    let insert_downstream =
        insert_submit_work_item_downstream_id(&mut parent, dependency, work_item_id)?;
    if insert_downstream {
        write_work_item_props(nodes, graph, dependency, &parent, crypto)?;
    }
    Ok(())
}

pub(crate) fn update_submit_work_item_downstream_index(
    graph: &str,
    work_item_id: &str,
    dependency_rows: &[(String, bool)],
    nodes: &mut ScopedOwnerTableMut<'_, (&str, &str), &[u8]>,
    crypto: DurableCrypto<'_>,
) -> Result<(), String> {
    for (dependency, pending) in dependency_rows {
        if !pending {
            continue;
        }
        update_submit_work_item_downstream_row(graph, work_item_id, dependency, nodes, crypto)?;
    }
    Ok(())
}

pub(crate) struct SubmitWorkItemWrite<'args, 'table, 'crypto> {
    graph: &'args str,
    work_item_id: &'args str,
    context_tenant: &'args str,
    request: &'args eg_types::native_control::SubmitWorkItemRequest,
    dependencies: &'args [String],
    dependency_rows: &'args [(String, bool)],
    command_sequence: u64,
    now_s: f64,
    status: &'args str,
    pending_dependencies: usize,
    nodes: &'args mut ScopedOwnerTableMut<'table, (&'static str, &'static str), &'static [u8]>,
    edges: &'args mut ScopedOwnerTableMut<
        'table,
        (&'static str, &'static str, &'static str, u32),
        &'static [u8],
    >,
    crypto: DurableCrypto<'crypto>,
}

pub(crate) fn write_submit_work_item_node_and_edges(
    input: SubmitWorkItemWrite<'_, '_, '_>,
) -> Result<(), String> {
    let SubmitWorkItemWrite {
        graph,
        work_item_id,
        context_tenant,
        request,
        dependencies,
        dependency_rows,
        command_sequence,
        now_s,
        status,
        pending_dependencies,
        nodes,
        edges,
        crypto,
    } = input;
    let props = build_submit_work_item_props(
        context_tenant,
        request,
        dependencies,
        pending_dependencies,
        command_sequence,
        now_s,
        status,
    )?;
    write_work_item_props(nodes, graph, work_item_id, &props, crypto)?;
    write_submit_work_item_dependency_edges(
        graph,
        work_item_id,
        context_tenant,
        dependencies,
        edges,
        crypto,
    )?;
    update_submit_work_item_downstream_index(graph, work_item_id, dependency_rows, nodes, crypto)?;
    Ok(())
}

pub(crate) struct SubmitWorkItemResultInput<'a> {
    work_item_id: String,
    status: &'a str,
    command_sequence: u64,
    request: &'a eg_types::native_control::SubmitWorkItemRequest,
    dependencies: &'a [String],
    dependency_rows: &'a [(String, bool)],
    inflight: u64,
    max_inflight: u64,
    outbox_id: &'a str,
}

pub(crate) fn build_submit_work_item_result(
    input: SubmitWorkItemResultInput<'_>,
) -> eg_types::native_control::SubmitWorkItemResult {
    let SubmitWorkItemResultInput {
        work_item_id,
        status,
        command_sequence,
        request,
        dependencies,
        dependency_rows,
        inflight,
        max_inflight,
        outbox_id,
    } = input;
    let mut changed_work_item_ids = Vec::with_capacity(1 + dependency_rows.len());
    changed_work_item_ids.push(work_item_id.clone());
    changed_work_item_ids.extend(
        dependency_rows
            .iter()
            .filter(|(_, pending)| *pending)
            .map(|(dependency, _)| dependency.clone()),
    );
    eg_types::native_control::SubmitWorkItemResult {
        schema_version: eg_types::native_control::NativeControlSchemaVersion::V1,
        work_item_id,
        status: status.to_string(),
        created: true,
        replayed: false,
        command_sequence,
        idempotency_key: request.idempotency_key.clone(),
        dependency_count: dependencies.len() as u32,
        admitted_count: inflight.saturating_add(1),
        max_tenant_in_flight: max_inflight,
        outbox_id: outbox_id.to_string(),
        command_digest: request.command_digest.to_ascii_lowercase(),
        provenance_refs: request.provenance_refs.clone(),
        changed_work_item_ids,
    }
}

pub(crate) fn apply_submit_work_item_rows<'txn, 'crypto>(
    graph: &str,
    request: &eg_types::native_control::SubmitWorkItemRequest,
    nodes: &mut ScopedOwnerTableMut<'txn, (&'static str, &'static str), &'static [u8]>,
    edges: &mut ScopedOwnerTableMut<
        'txn,
        (&'static str, &'static str, &'static str, u32),
        &'static [u8],
    >,
    command_sequences: &mut ScopedOwnerTableMut<'txn, &'static str, u64>,
    scope: WorkItemCommitScope<'crypto, '_>,
) -> Result<eg_types::native_control::SubmitWorkItemResult, String> {
    let WorkItemCommitScope {
        crypto,
        authoritative_now_ms,
        outbox_id,
    } = scope;
    let context_tenant = request.context.tenant_id.as_str();

    let (max_inflight, dependencies, dependency_rows) =
        validate_and_resolve_submit_work_item_dependencies(graph, request, nodes, crypto)?;

    let (inflight, work_item_id, command_sequence) =
        admit_and_identify_submit_work_item(SubmitWorkItemAdmission {
            graph,
            context_tenant,
            request,
            nodes: &mut *nodes,
            command_sequences: &mut *command_sequences,
            max_inflight,
            crypto,
        })?;

    let now_s = authoritative_now_ms as f64 / 1000.0;
    let pending_dependencies = dependency_rows
        .iter()
        .filter(|(_, pending)| *pending)
        .count();
    let status = submit_work_item_status(pending_dependencies);

    write_submit_work_item_node_and_edges(SubmitWorkItemWrite {
        graph,
        work_item_id: &work_item_id,
        context_tenant,
        request,
        dependencies: &dependencies,
        dependency_rows: &dependency_rows,
        command_sequence,
        now_s,
        status,
        pending_dependencies,
        nodes: &mut *nodes,
        edges: &mut *edges,
        crypto,
    })?;

    Ok(build_submit_work_item_result(SubmitWorkItemResultInput {
        work_item_id,
        status,
        command_sequence,
        request,
        dependencies: &dependencies,
        dependency_rows: &dependency_rows,
        inflight,
        max_inflight,
        outbox_id,
    }))
}

pub(crate) fn apply_submit_work_items_rows<'txn, 'crypto>(
    graph: &str,
    request: &eg_types::native_control::SubmitWorkItemsRequest,
    nodes: &mut ScopedOwnerTableMut<'txn, (&'static str, &'static str), &'static [u8]>,
    edges: &mut ScopedOwnerTableMut<
        'txn,
        (&'static str, &'static str, &'static str, u32),
        &'static [u8],
    >,
    command_sequences: &mut ScopedOwnerTableMut<'txn, &'static str, u64>,
    scope: WorkItemCommitScope<'crypto, '_>,
) -> Result<eg_types::native_control::SubmitWorkItemsResult, String> {
    let WorkItemCommitScope {
        crypto,
        authoritative_now_ms,
        outbox_id,
    } = scope;
    use eg_types::native_control::{
        NativeControlSchemaVersion, MAX_SUBMIT_BATCH, MAX_SUBMIT_BATCH_CHANGED_IDS,
    };
    if request.schema_version != NativeControlSchemaVersion::V1
        || request.requests.is_empty()
        || request.requests.len() > MAX_SUBMIT_BATCH
    {
        return Err("SubmitWorkItems batch is outside native bounds".to_string());
    }
    if request.idempotency_key.trim().is_empty() || sanitize(&request.context.graph) != graph {
        return Err("SubmitWorkItems context/idempotency is invalid".to_string());
    }
    let changed_bound = request
        .requests
        .iter()
        .try_fold(0usize, |total, child| {
            total
                .checked_add(child.depends_on.len().saturating_add(1))
                .ok_or(())
        })
        .map_err(|_| "SubmitWorkItems result cardinality overflow".to_string())?;
    if changed_bound > MAX_SUBMIT_BATCH_CHANGED_IDS {
        return Err("SubmitWorkItems changed-row result exceeds native bound".to_string());
    }
    let mut results = Vec::with_capacity(request.requests.len());
    let mut changed = Vec::with_capacity(request.requests.len());
    for child in &request.requests {
        if child.context.tenant_id != request.context.tenant_id
            || sanitize(&child.context.graph) != graph
        {
            return Err("SubmitWorkItems child context scope mismatch".to_string());
        }
        let result = apply_submit_work_item_rows(
            graph,
            child,
            nodes,
            edges,
            command_sequences,
            WorkItemCommitScope {
                crypto,
                authoritative_now_ms,
                outbox_id,
            },
        )?;
        changed.extend(result.changed_work_item_ids.clone());
        results.push(result);
    }
    Ok(eg_types::native_control::SubmitWorkItemsResult {
        schema_version: NativeControlSchemaVersion::V1,
        results,
        replayed: false,
        outbox_id: outbox_id.to_string(),
        changed_work_item_ids: changed,
    })
}
