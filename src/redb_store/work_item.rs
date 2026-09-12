//! Native WorkItem row transitions: submit, claim, renew, compare-and-set,
//! commit, cancel and defer.
//!
//! These are the `Method` arms that write a WorkItem's node, its dependency
//! edges and its downstream index inside an already-admitted mutation. All of
//! their tables are scope-prefixed, so each transition works through one graph
//! member's owner-row write.

use eg_storage::ScopedOwnerTableMut;

use super::*;

/// Apply one native WorkItem transition while the MutationBatch write
/// transaction is held. The returned payload is persisted as the batch result in
/// that same transaction, so a retry observes the exact original claim/commit
/// outcome rather than running selection twice.
/// The commit-scoped inputs both WorkItem row appliers need alongside their
/// scoped tables: the durable crypto handle, the authoritative commit timestamp,
/// and the outbox id. Grouped so each applier keeps a readable arity
/// (clippy::too_many_arguments) without disturbing the borrowed table params,
/// whose scoped-table lifetimes are load-bearing.
pub(crate) struct WorkItemCommitScope<'a> {
    pub(crate) crypto: DurableCrypto<'a>,
    pub(crate) authoritative_now_ms: u64,
    pub(crate) outbox_id: &'a str,
}

/// Refuse a per-graph scan asked about a graph other than the one its table is
/// bound to.
///
/// `scope_rows` takes its bound from the capability rather than from a key, so
/// the graph argument and the table's own scope can no longer disagree
/// silently: before the cutover the scan's `range((graph, "")..)` start made
/// the argument the bound, and a mismatch would now count -- or select from --
/// another graph's WorkItems while reporting them as this graph's.
fn permit_scoped_scan(scope_key: &str, graph: &str) -> Result<(), String> {
    if scope_key != graph {
        return Err(format!(
            "WorkItem scan for '{graph}' was given a table bound to '{scope_key}'"
        ));
    }
    Ok(())
}

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

#[allow(clippy::too_many_arguments)]
pub(crate) fn admit_and_identify_submit_work_item(
    graph: &str,
    context_tenant: &str,
    request: &eg_types::native_control::SubmitWorkItemRequest,
    nodes: &mut ScopedOwnerTableMut<'_, (&str, &str), &[u8]>,
    command_sequences: &mut ScopedOwnerTableMut<'_, &str, u64>,
    max_inflight: u64,
    crypto: DurableCrypto<'_>,
) -> Result<(u64, String, u64), String> {
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

#[allow(clippy::too_many_arguments)]
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

#[allow(clippy::too_many_arguments)]
pub(crate) fn write_submit_work_item_node_and_edges(
    graph: &str,
    work_item_id: &str,
    context_tenant: &str,
    request: &eg_types::native_control::SubmitWorkItemRequest,
    dependencies: &[String],
    dependency_rows: &[(String, bool)],
    command_sequence: u64,
    now_s: f64,
    status: &str,
    pending_dependencies: usize,
    nodes: &mut ScopedOwnerTableMut<'_, (&str, &str), &[u8]>,
    edges: &mut ScopedOwnerTableMut<'_, (&str, &str, &str, u32), &[u8]>,
    crypto: DurableCrypto<'_>,
) -> Result<(), String> {
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

#[allow(clippy::too_many_arguments)]
pub(crate) fn build_submit_work_item_result(
    work_item_id: String,
    status: &str,
    command_sequence: u64,
    request: &eg_types::native_control::SubmitWorkItemRequest,
    dependencies: &[String],
    dependency_rows: &[(String, bool)],
    inflight: u64,
    max_inflight: u64,
    outbox_id: &str,
) -> eg_types::native_control::SubmitWorkItemResult {
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

pub(crate) fn apply_submit_work_item_rows(
    graph: &str,
    request: &eg_types::native_control::SubmitWorkItemRequest,
    nodes: &mut ScopedOwnerTableMut<'_, (&str, &str), &[u8]>,
    edges: &mut ScopedOwnerTableMut<'_, (&str, &str, &str, u32), &[u8]>,
    command_sequences: &mut ScopedOwnerTableMut<'_, &str, u64>,
    scope: WorkItemCommitScope<'_>,
) -> Result<eg_types::native_control::SubmitWorkItemResult, String> {
    let WorkItemCommitScope {
        crypto,
        authoritative_now_ms,
        outbox_id,
    } = scope;
    let context_tenant = request.context.tenant_id.as_str();

    let (max_inflight, dependencies, dependency_rows) =
        validate_and_resolve_submit_work_item_dependencies(graph, request, nodes, crypto)?;

    let (inflight, work_item_id, command_sequence) = admit_and_identify_submit_work_item(
        graph,
        context_tenant,
        request,
        nodes,
        command_sequences,
        max_inflight,
        crypto,
    )?;

    let now_s = authoritative_now_ms as f64 / 1000.0;
    let pending_dependencies = dependency_rows
        .iter()
        .filter(|(_, pending)| *pending)
        .count();
    let status = submit_work_item_status(pending_dependencies);

    write_submit_work_item_node_and_edges(
        graph,
        &work_item_id,
        context_tenant,
        request,
        &dependencies,
        &dependency_rows,
        command_sequence,
        now_s,
        status,
        pending_dependencies,
        nodes,
        edges,
        crypto,
    )?;

    Ok(build_submit_work_item_result(
        work_item_id,
        status,
        command_sequence,
        request,
        &dependencies,
        &dependency_rows,
        inflight,
        max_inflight,
        outbox_id,
    ))
}

pub(crate) fn apply_submit_work_items_rows(
    graph: &str,
    request: &eg_types::native_control::SubmitWorkItemsRequest,
    nodes: &mut ScopedOwnerTableMut<'_, (&str, &str), &[u8]>,
    edges: &mut ScopedOwnerTableMut<'_, (&str, &str, &str, u32), &[u8]>,
    command_sequences: &mut ScopedOwnerTableMut<'_, &str, u64>,
    scope: WorkItemCommitScope<'_>,
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

#[allow(clippy::too_many_arguments)]
pub(crate) fn apply_work_item_rows(
    graph: &str,
    batch_id: &str,
    method: &Method,
    nodes: &mut ScopedOwnerTableMut<'_, (&str, &str), &[u8]>,
    holds: &mut ScopedOwnerTableMut<'_, (&str, &str), &[u8]>,
    work_item_index: &ScopedOwnerTableMut<'_, (&str, &str, u64), &str>,
    counters: &mut ScopedOwnerTableMut<'_, (&str, &str), &[u8]>,
    pressure_index: &mut ScopedOwnerTableMut<'_, (&str, &str, &str, &str, u64, &str), u8>,
    policies: &ScopedOwnerTableMut<'_, (&str, &str), &[u8]>,
    native_work_items: &mut ScopedOwnerTableMut<'_, (&str, &str), &[u8]>,
    crypto: DurableCrypto<'_>,
) -> Result<Option<crate::protocol::ResultPayload>, String> {
    match method {
        Method::ClaimWorkItem { request } => {
            apply_claim_work_item_row(graph, request, nodes, native_work_items, crypto)
        }
        Method::RenewWorkItemLease {
            tenant,
            work_item_id,
            worker_id,
            lease_epoch,
            fencing_token,
            now_ms,
            lease_ms,
        } => apply_renew_work_item_lease_row(
            graph,
            tenant,
            work_item_id,
            worker_id,
            *lease_epoch,
            *fencing_token,
            *now_ms,
            *lease_ms,
            nodes,
            crypto,
        ),
        Method::CasWorkItemMetadata { request } => {
            apply_cas_work_item_metadata_row(graph, request, nodes, crypto)
        }
        Method::CommitWorkItemResult {
            tenant,
            work_item_id,
            worker_id,
            lease_epoch,
            fencing_token,
            outcome,
            result_ref,
            error_ref,
            retryable,
            now_ms,
            outcome_extension,
            idempotency_key: _,
        } => apply_commit_work_item_result_row(
            graph,
            tenant,
            work_item_id,
            worker_id,
            *lease_epoch,
            *fencing_token,
            outcome,
            result_ref,
            error_ref,
            *retryable,
            *now_ms,
            outcome_extension.as_deref(),
            batch_id,
            nodes,
            holds,
            work_item_index,
            counters,
            pressure_index,
            policies,
            crypto,
        ),
        Method::CancelWorkItem {
            tenant,
            work_item_id,
            reason_ref,
            now_ms,
            ..
        } => apply_cancel_work_item_row(
            graph,
            tenant,
            work_item_id,
            reason_ref,
            *now_ms,
            nodes,
            holds,
            work_item_index,
            counters,
            pressure_index,
            policies,
            crypto,
        ),
        Method::DeferWorkItem {
            tenant,
            work_item_id,
            worker_id,
            lease_epoch,
            fencing_token,
            next_retry_at_ms,
            reason_ref,
            now_ms,
            ..
        } => apply_defer_work_item_row(
            graph,
            tenant,
            work_item_id,
            worker_id,
            *lease_epoch,
            *fencing_token,
            *next_retry_at_ms,
            reason_ref,
            *now_ms,
            nodes,
            crypto,
        ),
        _ => Ok(None),
    }
}

/// One selectable row of a claim scan: `(prio_bucket, deadline, created_at_ms,
/// node_id, props)`.  The first four components are the sort key.
pub(crate) type ClaimCandidateRow = (
    u64,
    u64,
    u64,
    String,
    serde_json::Map<String, serde_json::Value>,
);

/// What the claim scan decided about one scanned node.
pub(crate) enum ClaimRowOutcome {
    /// Not a claimable row for this request; the scan moves on.
    Skip,
    /// A live lease held by someone else — counts against the tenant quota.
    InFlight,
    /// An expired lease past its attempt ceiling, retired to `dead_letter`.
    Exhausted(serde_json::Map<String, serde_json::Value>),
    /// A selectable candidate.
    Candidate(ClaimCandidateRow),
}

/// Everything one pass over the graph's nodes produced for a claim.
pub(crate) struct ClaimWorkItemScan {
    inflight: u32,
    candidates: Vec<ClaimCandidateRow>,
    // The redb range cursor immutably borrows the table, so expired
    // exhausted rows are collected here and written only after the
    // scan. They still commit in this same MutationBatch transaction.
    exhausted: Vec<(String, serde_json::Map<String, serde_json::Value>)>,
    changed_work_item_ids: Vec<String>,
}

/// Fence out an expired lease owner before the item participates in selection.
/// Returns `true` when the attempt ceiling was reached and the item was retired
/// to `dead_letter`; `false` when it was reclaimed back to `ready`.  Either way
/// the update is still private to the held transaction.
// `node_id`/`status` feed the statechart mirror only; a slim build without that
// feature still needs them in the signature.
#[cfg_attr(not(feature = "statechart"), allow(unused_variables))]
pub(crate) fn claim_reclaim_expired_lease(
    props: &mut serde_json::Map<String, serde_json::Value>,
    node_id: &str,
    status: &str,
    now_s: f64,
) -> bool {
    let attempts = property_u64(props, "attempt");
    let max_attempts = property_u64(props, "max_attempts").max(1);
    let next_epoch = property_u64(props, "lease_epoch").saturating_add(1);
    if attempts >= max_attempts {
        props.insert(
            "status".into(),
            serde_json::Value::String("dead_letter".into()),
        );
        props.insert("lease_epoch".into(), serde_json::Value::from(next_epoch));
        props.insert("fencing_token".into(), serde_json::Value::from(next_epoch));
        props.insert("lease_owner".into(), serde_json::Value::Null);
        props.insert("lease_expires_at".into(), serde_json::Value::Null);
        props.insert("completed_at".into(), serde_json::Value::from(now_s));
        props.insert("updated_at".into(), serde_json::Value::from(now_s));
        props.insert(
            "error_ref".into(),
            serde_json::Value::String("lease_exhausted".into()),
        );
        #[cfg(feature = "statechart")]
        apply_work_item_mirror(
            props,
            node_id,
            status,
            crate::work_item_statechart::EV_LEASE_EXHAUSTED,
            serde_json::json!({}),
            Some("dead_letter"),
        );
        return true;
    }
    props.insert("status".into(), serde_json::Value::String("ready".into()));
    props.insert("lease_epoch".into(), serde_json::Value::from(next_epoch));
    props.insert("fencing_token".into(), serde_json::Value::from(next_epoch));
    props.insert("lease_owner".into(), serde_json::Value::Null);
    props.insert("lease_expires_at".into(), serde_json::Value::Null);
    #[cfg(feature = "statechart")]
    apply_work_item_mirror(
        props,
        node_id,
        status,
        crate::work_item_statechart::EV_LEASE_RECLAIM,
        serde_json::json!({}),
        Some("ready"),
    );
    false
}

/// The selection filter, in its original order: the item must be `ready`, match
/// every supplied queue/class/fairness selector, be past its retry backoff, and
/// not have blown its deadline.
pub(crate) fn claim_candidate_is_excluded(
    props: &serde_json::Map<String, serde_json::Value>,
    request: &crate::epistemic_operations::ClaimWorkItemRequest,
    now_s: f64,
) -> bool {
    property_string(props, "status") != "ready"
        || request
            .queue_ref
            .as_deref()
            .is_some_and(|queue| property_string(props, "queue") != queue)
        || request
            .resource_class
            .as_deref()
            .is_some_and(|resource_class| {
                property_string(props, "resource_class") != resource_class
            })
        || request
            .fairness_group
            .as_deref()
            .is_some_and(|fairness_group| {
                property_string(props, "fairness_group") != fairness_group
            })
        || property_f64(props, "next_retry_at") > now_s
        || props
            .get("deadline_unix")
            .and_then(serde_json::Value::as_f64)
            .is_some_and(|deadline| deadline < now_s)
}

/// The sort-key deadline of a selectable candidate: an absent (or already
/// filtered-out) deadline sorts last.
pub(crate) fn claim_candidate_deadline(
    props: &serde_json::Map<String, serde_json::Value>,
    now_s: f64,
) -> u64 {
    props
        .get("deadline_unix")
        .and_then(serde_json::Value::as_f64)
        .filter(|deadline| *deadline >= now_s)
        .map(|deadline| (deadline * 1000.0) as u64)
        .unwrap_or(u64::MAX)
}

/// Classify one scanned node for a claim request.  The checks run in the
/// original order, which matters: admission is tenant-wide even for an exact-id
/// delivery, so live leases contribute to the quota before the exact-id filter,
/// and that filter runs before an expired unrelated row could be reclaimed.
pub(crate) fn classify_claim_row(
    request: &crate::epistemic_operations::ClaimWorkItemRequest,
    node_id: &str,
    mut props: serde_json::Map<String, serde_json::Value>,
    now_s: f64,
) -> ClaimRowOutcome {
    if property_string(&props, "node_type") != "WorkItem" {
        return ClaimRowOutcome::Skip;
    }
    if property_string(&props, "tenant") != request.tenant_ref.as_str() {
        return ClaimRowOutcome::Skip;
    }
    let status = property_string(&props, "status").to_string();
    if matches!(status.as_str(), "leased" | "running")
        && property_f64(&props, "lease_expires_at") > now_s
    {
        return ClaimRowOutcome::InFlight;
    }
    if request
        .work_item_id
        .as_deref()
        .is_some_and(|selected| selected != node_id)
    {
        return ClaimRowOutcome::Skip;
    }
    if matches!(status.as_str(), "leased" | "running")
        && claim_reclaim_expired_lease(&mut props, node_id, &status, now_s)
    {
        return ClaimRowOutcome::Exhausted(props);
    }
    if claim_candidate_is_excluded(&props, request, now_s) {
        return ClaimRowOutcome::Skip;
    }
    let deadline = claim_candidate_deadline(&props, now_s);
    ClaimRowOutcome::Candidate((
        property_u64(&props, "prio_bucket"),
        deadline,
        (property_f64(&props, "created_at") * 1000.0) as u64,
        node_id.to_string(),
        props,
    ))
}

/// One bounded pass over this graph's node rows, tallying the tenant's in-flight
/// leases, the expired rows to retire, and the claimable candidates.
pub(crate) fn scan_claim_work_item_candidates(
    graph: &str,
    request: &crate::epistemic_operations::ClaimWorkItemRequest,
    now_s: f64,
    nodes: &ScopedOwnerTableMut<'_, (&str, &str), &[u8]>,
    crypto: DurableCrypto<'_>,
) -> Result<ClaimWorkItemScan, String> {
    permit_scoped_scan(nodes.scope_key(), graph)?;
    let mut scan = ClaimWorkItemScan {
        inflight: 0,
        candidates: Vec::new(),
        exhausted: Vec::new(),
        changed_work_item_ids: Vec::new(),
    };
    for row in nodes.scope_rows()? {
        let (key, value) = row?;
        let (_, node_id) = key.value();
        let bytes = crypto.unseal(value.value())?;
        let Ok(props) = decode_durable::<serde_json::Map<String, serde_json::Value>>(&bytes) else {
            continue;
        };
        match classify_claim_row(request, node_id, props, now_s) {
            ClaimRowOutcome::Skip => {}
            ClaimRowOutcome::InFlight => scan.inflight = scan.inflight.saturating_add(1),
            ClaimRowOutcome::Exhausted(props) => {
                let node_id = node_id.to_string();
                scan.changed_work_item_ids.push(node_id.clone());
                scan.exhausted.push((node_id, props));
            }
            ClaimRowOutcome::Candidate(candidate) => scan.candidates.push(candidate),
        }
    }
    Ok(scan)
}

/// The `claimed: false` result shape, shared by the tenant-quota and empty-queue
/// refusals.
pub(crate) fn claim_not_claimed_payload(
    reason: ClaimWorkItemResultReason,
    inflight: u32,
    changed_work_item_ids: Vec<String>,
) -> Result<crate::protocol::ResultPayload, String> {
    crate::protocol::ResultPayload::raw(&ClaimWorkItemResult {
        schema_version: ClaimWorkItemResultSchemaVersion::V1,
        claimed: false,
        reason,
        work_item_id: None,
        kind: None,
        payload_ref: None,
        lease_holder_ref: None,
        lease_epoch: None,
        fencing_token: None,
        lease_expires_at_ms: None,
        attempt: None,
        max_attempts: None,
        tenant_in_flight: Some(u64::from(inflight)),
        changed_work_item_ids,
    })
}

/// Stamp the granted lease onto the selected candidate and report its
/// `(lease_epoch, attempt)`.
///
/// The native claim authority owns the per-attempt WorkItem fence.  Ready
/// submissions cannot supply one through generic graph writes; deriving it from
/// the authoritative lease epoch keeps the Raft transition deterministic while
/// ensuring every capability-bound live lease has a non-empty fence that changes
/// on reclaim.
pub(crate) fn claim_grant_lease(
    props: &mut serde_json::Map<String, serde_json::Value>,
    worker_id: &str,
    now_s: f64,
    lease_until_s: f64,
) -> (u64, u64) {
    let epoch = property_u64(props, "lease_epoch").saturating_add(1);
    let attempt = property_u64(props, "attempt").saturating_add(1);
    props.insert("status".into(), serde_json::Value::String("leased".into()));
    props.insert(
        "lease_owner".into(),
        serde_json::Value::String(worker_id.to_string()),
    );
    props.insert(
        "last_lease_owner".into(),
        serde_json::Value::String(worker_id.to_string()),
    );
    props.insert("lease_epoch".into(), serde_json::Value::from(epoch));
    props.insert("fencing_token".into(), serde_json::Value::from(epoch));
    props.insert(
        "lease_expires_at".into(),
        serde_json::Value::from(lease_until_s),
    );
    props.insert(
        "work_item_fence".into(),
        serde_json::Value::String(format!("lease-fence-v1:{epoch}")),
    );
    props.insert("heartbeat_at".into(), serde_json::Value::from(now_s));
    props.insert("updated_at".into(), serde_json::Value::from(now_s));
    props.insert("attempt".into(), serde_json::Value::from(attempt));
    (epoch, attempt)
}

pub(crate) fn apply_claim_work_item_row(
    graph: &str,
    request: &crate::epistemic_operations::ClaimWorkItemRequest,
    nodes: &mut ScopedOwnerTableMut<'_, (&str, &str), &[u8]>,
    native_work_items: &mut ScopedOwnerTableMut<'_, (&str, &str), &[u8]>,
    crypto: DurableCrypto<'_>,
) -> Result<Option<crate::protocol::ResultPayload>, String> {
    let tenant = &request.tenant_ref;
    let worker_id = &request.worker_ref;
    let now_ms = request.now_ms;
    let lease_ms = request.lease_ms;
    let max_tenant_in_flight = request.max_tenant_in_flight;
    if tenant.trim().is_empty()
        || worker_id.trim().is_empty()
        || lease_ms == 0
        || !(1..=4096).contains(&max_tenant_in_flight)
    {
        return Err("ClaimWorkItem request violates the current protocol contract".into());
    }
    let now_s = now_ms as f64 / 1000.0;
    let lease_until_s = now_s + (lease_ms as f64 / 1000.0);
    let tenant_in_flight_limit = max_tenant_in_flight as u32;
    let ClaimWorkItemScan {
        inflight,
        mut candidates,
        exhausted,
        mut changed_work_item_ids,
    } = scan_claim_work_item_candidates(graph, request, now_s, nodes, crypto)?;
    for (node_id, props) in exhausted {
        write_work_item_props(nodes, graph, &node_id, &props, crypto)?;
    }
    if inflight >= tenant_in_flight_limit {
        return Ok(Some(claim_not_claimed_payload(
            ClaimWorkItemResultReason::TenantQuota,
            inflight,
            changed_work_item_ids,
        )?));
    }
    candidates.sort_by(|left, right| {
        (&left.0, &left.1, &left.2, &left.3).cmp(&(&right.0, &right.1, &right.2, &right.3))
    });
    let Some((_, _, _, node_id, mut props)) = candidates.into_iter().next() else {
        return Ok(Some(claim_not_claimed_payload(
            ClaimWorkItemResultReason::Empty,
            inflight,
            changed_work_item_ids,
        )?));
    };
    let (epoch, attempt) = claim_grant_lease(&mut props, worker_id, now_s, lease_until_s);
    let kind = property_string(&props, "kind").to_string();
    let payload_ref = property_string(&props, "payload_ref").to_string();
    let max_attempts = property_u64(&props, "max_attempts").max(1);
    // Phase-1 statechart mirror: the picked candidate was `ready` (it passed the
    // `status != "ready"` filter above); selection already happened outside the
    // chart, so its `ready --claim--> leased` edge is unconditional.
    #[cfg(feature = "statechart")]
    apply_work_item_mirror(
        &mut props,
        &node_id,
        "ready",
        crate::work_item_statechart::EV_CLAIM,
        serde_json::json!({}),
        Some("leased"),
    );
    write_work_item_props(nodes, graph, &node_id, &props, crypto)?;
    work_item_capability::record_native_claim_in_wtx(
        native_work_items,
        graph,
        &node_id,
        &props,
        now_ms,
        crypto,
    )?;
    Ok(Some(crate::protocol::ResultPayload::raw(
        &ClaimWorkItemResult {
            schema_version: ClaimWorkItemResultSchemaVersion::V1,
            claimed: true,
            reason: ClaimWorkItemResultReason::Claimed,
            work_item_id: Some(node_id.clone()),
            kind: (!kind.is_empty()).then_some(kind),
            payload_ref: (!payload_ref.is_empty()).then_some(payload_ref),
            lease_holder_ref: Some(worker_id.clone()),
            lease_epoch: Some(epoch),
            fencing_token: Some(epoch),
            lease_expires_at_ms: Some(now_ms.saturating_add(lease_ms)),
            attempt: Some(attempt),
            max_attempts: Some(max_attempts),
            tenant_in_flight: Some(u64::from(inflight.saturating_add(1))),
            changed_work_item_ids: {
                changed_work_item_ids.push(node_id);
                changed_work_item_ids
            },
        },
    )?))
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn apply_renew_work_item_lease_row(
    graph: &str,
    tenant: &String,
    work_item_id: &String,
    worker_id: &String,
    lease_epoch: u64,
    fencing_token: u64,
    now_ms: u64,
    lease_ms: u64,
    nodes: &mut ScopedOwnerTableMut<'_, (&str, &str), &[u8]>,
    crypto: DurableCrypto<'_>,
) -> Result<Option<crate::protocol::ResultPayload>, String> {
    let decode = |bytes: &[u8]| -> Result<serde_json::Map<String, serde_json::Value>, String> {
        decode_durable(bytes)
    };
    if worker_id.trim().is_empty() || lease_ms == 0 {
        return Err("RenewWorkItemLease requires worker_id and non-zero lease_ms".into());
    }
    let current = nodes
        .get((graph, work_item_id.as_str()))?
        .map(|value| crypto.unseal(value.value()))
        .transpose()?;
    // Every WorkItem result — including one that changed no row — MUST carry
    // `changed_work_item_ids`. The commit has already advanced the authoritative
    // graph version by the time `commit_work_item` reads this field, so a shape
    // missing it strands the serving projection one version behind and makes the
    // graph permanently read-only (INCIDENT-kg-readonly-2026-07-31).
    let Some(bytes) = current else {
        return Ok(Some(crate::protocol::ResultPayload::Json(
            serde_json::json!({
                "renewed": false,
                "reason": "missing",
                "changed_work_item_ids": [],
            }),
        )));
    };
    let mut props = decode(&bytes)?;
    let valid = property_string(&props, "tenant") == tenant
        && property_string(&props, "lease_owner") == worker_id
        && matches!(property_string(&props, "status"), "leased" | "running")
        && property_u64(&props, "lease_epoch") == lease_epoch
        && property_u64(&props, "fencing_token") == fencing_token
        && property_f64(&props, "lease_expires_at") >= now_ms as f64 / 1000.0;
    if !valid {
        return Ok(Some(crate::protocol::ResultPayload::Json(
            serde_json::json!({
                "renewed": false,
                "reason": "fenced",
                "changed_work_item_ids": [],
            }),
        )));
    }
    // Phase-1 mirror: the lease was validated (fence_valid), so leased|running →
    // running. Capture the pre-status before the authority overwrites it.
    #[cfg(feature = "statechart")]
    let pre_status = property_string(&props, "status").to_string();
    let now_s = now_ms as f64 / 1000.0;
    props.insert("status".into(), serde_json::Value::String("running".into()));
    props.insert("heartbeat_at".into(), serde_json::Value::from(now_s));
    props.insert("updated_at".into(), serde_json::Value::from(now_s));
    props.insert(
        "lease_expires_at".into(),
        serde_json::Value::from(now_s + lease_ms as f64 / 1000.0),
    );
    #[cfg(feature = "statechart")]
    apply_work_item_mirror(
        &mut props,
        work_item_id,
        &pre_status,
        crate::work_item_statechart::EV_RENEW,
        serde_json::json!({ "fence_valid": true }),
        Some("running"),
    );
    write_work_item_props(nodes, graph, work_item_id, &props, crypto)?;
    Ok(Some(crate::protocol::ResultPayload::Json(
        serde_json::json!({
            "renewed": true,
            "work_item_id": work_item_id,
            "lease_epoch": lease_epoch,
            "fencing_token": fencing_token,
            "lease_expires_at_ms": (now_ms).saturating_add(lease_ms),
            "changed_work_item_ids": [work_item_id],
        }),
    )))
}

/// Shape validation for a `CasWorkItemMetadata` request, in the original order:
/// exactly one settable field, a non-empty expected status set, and non-blank
/// tenant/work-item identifiers.
pub(crate) fn validate_cas_work_item_metadata_request(
    request: &crate::epistemic_operations_ext::CasWorkItemMetadataRequest,
) -> Result<(), String> {
    let field_pairs_set = [
        request.set_checkpoint_id.is_some(),
        request.set_metadata_msgpack.is_some(),
        request.set_prio_bucket.is_some(),
    ]
    .into_iter()
    .filter(|set| *set)
    .count();
    if field_pairs_set != 1 {
        return Err(
            "CasWorkItemMetadata requires exactly one of set_checkpoint_id / \
             set_metadata_msgpack / set_prio_bucket"
                .to_string(),
        );
    }
    if request.expected_status.is_empty() {
        return Err("CasWorkItemMetadata requires a non-empty expected_status".into());
    }
    if request.tenant_ref.trim().is_empty() || request.work_item_id.trim().is_empty() {
        return Err("CasWorkItemMetadata requires tenant_ref and work_item_id".into());
    }
    Ok(())
}

/// The status / tenant / lease-fence preconditions of a metadata CAS.  All three
/// are evaluated (as before) and the conjunction decides; a `false` here is a
/// `Conflict` outcome, not an error.
pub(crate) fn cas_work_item_metadata_preconditions_ok(
    request: &crate::epistemic_operations_ext::CasWorkItemMetadataRequest,
    props: &serde_json::Map<String, serde_json::Value>,
) -> bool {
    let tenant = &request.tenant_ref;
    let status_ok = request
        .expected_status
        .iter()
        .any(|status| status == property_string(props, "status"));
    let tenant_ok = property_string(props, "tenant") == tenant;
    let lease_ok = match &request.expected_lease {
        Some(fence) => {
            property_string(props, "lease_owner") == fence.worker_ref
                && property_u64(props, "lease_epoch") == fence.lease_epoch
                && property_u64(props, "fencing_token") == fence.fencing_token
        }
        None => true,
    };
    status_ok && tenant_ok && lease_ok
}

/// Apply the one settable field of a metadata CAS to `props`, after checking its
/// own expected pre-image.  Returns `Ok(false)` when that pre-image does not
/// match -- the caller turns that into a `Conflict` outcome, exactly as the
/// inline branches did.  `props` is only mutated on the matching path.
pub(crate) fn apply_cas_work_item_metadata_field(
    request: &crate::epistemic_operations_ext::CasWorkItemMetadataRequest,
    props: &mut serde_json::Map<String, serde_json::Value>,
) -> Result<bool, String> {
    if let Some(set_checkpoint_id) = &request.set_checkpoint_id {
        let current_checkpoint_id = props
            .get("checkpoint_id")
            .and_then(serde_json::Value::as_str)
            .map(str::to_string);
        if current_checkpoint_id != request.expected_checkpoint_id {
            return Ok(false);
        }
        props.insert(
            "checkpoint_id".into(),
            serde_json::Value::String(set_checkpoint_id.clone()),
        );
    } else if let Some(set_metadata_bytes) = &request.set_metadata_msgpack {
        let current_metadata = props
            .get("metadata")
            .cloned()
            .unwrap_or(serde_json::Value::Object(Default::default()));
        let expected_metadata = match &request.expected_metadata_msgpack {
            Some(bytes) => decode_durable::<serde_json::Value>(bytes)
                .map_err(|_| "invalid expected_metadata_msgpack".to_string())?,
            None => serde_json::Value::Object(Default::default()),
        };
        if current_metadata != expected_metadata {
            return Ok(false);
        }
        let set_metadata = decode_durable::<serde_json::Value>(set_metadata_bytes)
            .map_err(|_| "invalid set_metadata_msgpack".to_string())?;
        props.insert("metadata".into(), set_metadata);
    } else if let Some(set_prio_bucket) = request.set_prio_bucket {
        let expected_prio_bucket = request.expected_prio_bucket.unwrap_or(0);
        let current_prio_bucket = props
            .get("prio_bucket")
            .and_then(serde_json::Value::as_i64)
            .unwrap_or(0);
        if current_prio_bucket != expected_prio_bucket {
            return Ok(false);
        }
        props.insert(
            "prio_bucket".into(),
            serde_json::Value::from(set_prio_bucket),
        );
    }
    Ok(true)
}

pub(crate) fn apply_cas_work_item_metadata_row(
    graph: &str,
    request: &crate::epistemic_operations_ext::CasWorkItemMetadataRequest,
    nodes: &mut ScopedOwnerTableMut<'_, (&str, &str), &[u8]>,
    crypto: DurableCrypto<'_>,
) -> Result<Option<crate::protocol::ResultPayload>, String> {
    use crate::epistemic_operations_ext::{
        CasWorkItemMetadataOutcome, CasWorkItemMetadataResult,
        CasWorkItemMetadataResultSchemaVersion,
    };

    let work_item_id = &request.work_item_id;
    let now_ms = request.now_ms;

    validate_cas_work_item_metadata_request(request)?;

    let respond = |outcome: CasWorkItemMetadataOutcome, changed: Vec<String>| {
        Ok(Some(crate::protocol::ResultPayload::raw(
            &CasWorkItemMetadataResult {
                schema_version: CasWorkItemMetadataResultSchemaVersion::V1,
                outcome,
                work_item_id: work_item_id.clone(),
                changed_work_item_ids: changed,
            },
        )?))
    };

    let current = nodes
        .get((graph, work_item_id.as_str()))?
        .map(|value| crypto.unseal(value.value()))
        .transpose()?;
    let Some(bytes) = current else {
        return respond(CasWorkItemMetadataOutcome::NotFound, vec![]);
    };
    let mut props: serde_json::Map<String, serde_json::Value> = decode_durable(&bytes)?;

    if !cas_work_item_metadata_preconditions_ok(request, &props) {
        return respond(CasWorkItemMetadataOutcome::Conflict, vec![]);
    }

    if !apply_cas_work_item_metadata_field(request, &mut props)? {
        return respond(CasWorkItemMetadataOutcome::Conflict, vec![]);
    }

    let now_s = now_ms as f64 / 1000.0;
    props.insert("updated_at".into(), serde_json::Value::from(now_s));
    write_work_item_props(nodes, graph, work_item_id, &props, crypto)?;
    respond(
        CasWorkItemMetadataOutcome::Applied,
        vec![work_item_id.clone()],
    )
}

/// The lease fence a `CommitWorkItemResult` must satisfy: the caller owns the
/// lease, the item is live, and the lease has not expired.
pub(crate) fn commit_work_item_lease_is_valid(
    props: &serde_json::Map<String, serde_json::Value>,
    worker_id: &str,
    lease_epoch: u64,
    fencing_token: u64,
    now_ms: u64,
) -> bool {
    property_string(props, "lease_owner") == worker_id
        && matches!(property_string(props, "status"), "leased" | "running")
        && property_u64(props, "lease_epoch") == lease_epoch
        && property_u64(props, "fencing_token") == fencing_token
        && property_f64(props, "lease_expires_at") >= now_ms as f64 / 1000.0
}

/// The three short-circuit responses of a commit, in their original order:
/// a tenant mismatch reads as `missing`, an already-terminal item as `noop`,
/// and a failed lease fence as `fenced`.  `Ok(None)` means the commit proceeds.
pub(crate) fn commit_work_item_result_precheck(
    props: &serde_json::Map<String, serde_json::Value>,
    work_item_id: &str,
    tenant: &str,
    worker_id: &str,
    lease_epoch: u64,
    fencing_token: u64,
    now_ms: u64,
) -> Option<crate::protocol::ResultPayload> {
    if property_string(props, "tenant") != tenant {
        return Some(crate::protocol::ResultPayload::Json(
            serde_json::json!({"status": "missing", "changed_work_item_ids": []}),
        ));
    }
    if matches!(
        property_string(props, "status"),
        "succeeded" | "failed" | "cancelled" | "dead_letter"
    ) {
        return Some(crate::protocol::ResultPayload::Json(serde_json::json!({
            "status": "noop",
            "work_item_id": work_item_id,
            "changed_work_item_ids": [],
        })));
    }
    if !commit_work_item_lease_is_valid(props, worker_id, lease_epoch, fencing_token, now_ms) {
        return Some(crate::protocol::ResultPayload::Json(serde_json::json!({
            "status": "fenced",
            "work_item_id": work_item_id,
            "changed_work_item_ids": [],
        })));
    }
    None
}

/// Write the committed status into `props` and report it.  A retryable failure
/// below the attempt ceiling reschedules (`ready` + backoff + bumped fence) and
/// reports `retry_scheduled`; otherwise the item goes terminal (`dead_letter`
/// for an exhausted retryable failure, else the outcome verb itself).
pub(crate) fn commit_work_item_apply_status<'o>(
    props: &mut serde_json::Map<String, serde_json::Value>,
    outcome: &'o str,
    retryable: bool,
    lease_epoch: u64,
    fencing_token: u64,
    now_s: f64,
) -> &'o str {
    let attempts = property_u64(props, "attempt");
    let max_attempts = property_u64(props, "max_attempts").max(1);
    if outcome == "failed" && retryable && attempts < max_attempts {
        let backoff = property_f64(props, "backoff_base_s").max(1.0)
            * 2f64.powi(attempts.saturating_sub(1).min(31) as i32);
        props.insert("status".into(), serde_json::Value::String("ready".into()));
        props.insert(
            "next_retry_at".into(),
            serde_json::Value::from(now_s + backoff),
        );
        props.insert(
            "lease_epoch".into(),
            serde_json::Value::from((lease_epoch).saturating_add(1)),
        );
        props.insert(
            "fencing_token".into(),
            serde_json::Value::from((fencing_token).saturating_add(1)),
        );
        return "retry_scheduled";
    }
    let terminal = if outcome == "failed" && retryable {
        "dead_letter"
    } else {
        outcome
    };
    props.insert("status".into(), serde_json::Value::String(terminal.into()));
    props.insert("completed_at".into(), serde_json::Value::from(now_s));
    terminal
}

/// Record the commit's lease/result bookkeeping on the item.
pub(crate) fn commit_work_item_record_result_refs(
    props: &mut serde_json::Map<String, serde_json::Value>,
    worker_id: &str,
    result_ref: &Option<String>,
    error_ref: &Option<String>,
    now_s: f64,
) {
    props.insert(
        "result_ref".into(),
        result_ref
            .clone()
            .map(serde_json::Value::String)
            .unwrap_or(serde_json::Value::Null),
    );
    props.insert(
        "error_ref".into(),
        error_ref
            .clone()
            .map(serde_json::Value::String)
            .unwrap_or(serde_json::Value::Null),
    );
    props.insert("lease_owner".into(), serde_json::Value::Null);
    props.insert(
        "last_lease_owner".into(),
        serde_json::Value::String(worker_id.to_string()),
    );
    props.insert("lease_expires_at".into(), serde_json::Value::Null);
    props.insert("updated_at".into(), serde_json::Value::from(now_s));
}

/// Decrement each downstream child's dependency count after a successful
/// commit, releasing a child to `ready` once its last dependency clears.
/// Children that no longer exist are skipped, as before.
pub(crate) fn commit_work_item_release_downstream(
    graph: &str,
    props: &serde_json::Map<String, serde_json::Value>,
    now_s: f64,
    nodes: &mut ScopedOwnerTableMut<'_, (&str, &str), &[u8]>,
    crypto: DurableCrypto<'_>,
    changed: &mut Vec<String>,
) -> Result<(), String> {
    let downstream = props
        .get("downstream_ids")
        .and_then(serde_json::Value::as_array)
        .cloned()
        .unwrap_or_default();
    for child in downstream.iter().filter_map(serde_json::Value::as_str) {
        let child_bytes = nodes
            .get((graph, child))?
            .map(|value| crypto.unseal(value.value()))
            .transpose()?;
        let Some(child_bytes) = child_bytes else {
            continue;
        };
        let mut child_props: serde_json::Map<String, serde_json::Value> =
            decode_durable(&child_bytes)?;
        let count = property_u64(&child_props, "dep_count").saturating_sub(1);
        child_props.insert("dep_count".into(), serde_json::Value::from(count));
        if count == 0 && property_string(&child_props, "status") == "submitted" {
            child_props.insert("status".into(), serde_json::Value::String("ready".into()));
        }
        child_props.insert("updated_at".into(), serde_json::Value::from(now_s));
        write_work_item_props(nodes, graph, child, &child_props, crypto)?;
        changed.push(child.to_string());
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn apply_commit_work_item_result_row(
    graph: &str,
    tenant: &str,
    work_item_id: &str,
    worker_id: &str,
    lease_epoch: u64,
    fencing_token: u64,
    outcome: &str,
    result_ref: &Option<String>,
    error_ref: &Option<String>,
    retryable: bool,
    now_ms: u64,
    outcome_extension: Option<&eg_types::outcome_bundle::TerminalOutcomeExtension>,
    batch_id: &str,
    nodes: &mut ScopedOwnerTableMut<'_, (&str, &str), &[u8]>,
    holds: &mut ScopedOwnerTableMut<'_, (&str, &str), &[u8]>,
    work_item_index: &ScopedOwnerTableMut<'_, (&str, &str, u64), &str>,
    counters: &mut ScopedOwnerTableMut<'_, (&str, &str), &[u8]>,
    pressure_index: &mut ScopedOwnerTableMut<'_, (&str, &str, &str, &str, u64, &str), u8>,
    policies: &ScopedOwnerTableMut<'_, (&str, &str), &[u8]>,
    crypto: DurableCrypto<'_>,
) -> Result<Option<crate::protocol::ResultPayload>, String> {
    let current = nodes
        .get((graph, work_item_id))?
        .map(|value| crypto.unseal(value.value()))
        .transpose()?;
    let Some(bytes) = current else {
        return Ok(Some(crate::protocol::ResultPayload::Json(
            serde_json::json!({"status": "missing", "changed_work_item_ids": []}),
        )));
    };
    let mut props: serde_json::Map<String, serde_json::Value> = decode_durable(&bytes)?;
    let pre_props = props.clone();
    if let Some(payload) = commit_work_item_result_precheck(
        &props,
        work_item_id,
        tenant,
        worker_id,
        lease_epoch,
        fencing_token,
        now_ms,
    ) {
        return Ok(Some(payload));
    }
    if !matches!(outcome, "succeeded" | "failed" | "cancelled") {
        return Err("CommitWorkItemResult outcome must be succeeded, failed, or cancelled".into());
    }
    if let Some(extension) = outcome_extension {
        validate_terminal_extension_binding(
            &props,
            extension,
            TerminalCommitClaim {
                worker_id,
                work_item_id,
                fencing_token,
                outcome,
                result_ref,
                batch_id,
            },
        )?;
        ensure_receipt_rows_absent(graph, &extension.receipt_nodes, nodes)?;
    }
    let now_s = now_ms as f64 / 1000.0;
    // Phase-1 mirror inputs: pre-status (leased|running, validated above) + the
    // DLQ-threshold POLICY boolean (`retryable && attempt < max_attempts`) the
    // chart reads as a pre-computed guard input — see `work_item_statechart`.
    #[cfg(feature = "statechart")]
    let pre_status = property_string(&props, "status").to_string();
    #[cfg(feature = "statechart")]
    let commit_retry_eligible =
        property_u64(&props, "attempt") < property_u64(&props, "max_attempts").max(1);
    let committed_status = commit_work_item_apply_status(
        &mut props,
        outcome,
        retryable,
        lease_epoch,
        fencing_token,
        now_s,
    );
    commit_work_item_record_result_refs(&mut props, worker_id, result_ref, error_ref, now_s);
    development_lane::transition_work_item_terminal_hold(
        graph,
        &pre_props,
        work_item_id,
        committed_status,
        false,
        property_u64(&props, "attempt"),
        property_u64(&props, "lease_epoch"),
        property_u64(&props, "fencing_token"),
        property_string(&props, "work_item_fence"),
        holds,
        work_item_index,
        counters,
        pressure_index,
        policies,
        crypto,
    )?;
    // Phase-1 mirror: the commit outcome maps to the chart's commit_* event; the
    // authoritative next state is whatever the handler persisted (ready on a
    // scheduled retry, else the terminal). The chart must independently agree.
    #[cfg(feature = "statechart")]
    {
        let event = match outcome {
            "succeeded" => crate::work_item_statechart::EV_COMMIT_SUCCEEDED,
            "cancelled" => crate::work_item_statechart::EV_COMMIT_CANCELLED,
            _ => crate::work_item_statechart::EV_COMMIT_FAILED,
        };
        let mirror_payload = serde_json::json!({
            "fence_valid": true,
            "retryable": retryable,
            "retry_eligible": commit_retry_eligible,
        });
        let authoritative_next = property_string(&props, "status").to_string();
        apply_work_item_mirror(
            &mut props,
            work_item_id,
            &pre_status,
            event,
            mirror_payload,
            Some(&authoritative_next),
        );
    }
    write_work_item_props(nodes, graph, work_item_id, &props, crypto)?;

    let mut changed = vec![work_item_id.to_string()];
    if committed_status == "succeeded" {
        commit_work_item_release_downstream(graph, &props, now_s, nodes, crypto, &mut changed)?;
    }
    if committed_status != "retry_scheduled" {
        if let Some(extension) = outcome_extension {
            apply_receipt_rows(graph, &extension.receipt_nodes, nodes, crypto)?;
        }
    }
    Ok(Some(crate::protocol::ResultPayload::Json(
        serde_json::json!({
            "status": committed_status,
            "work_item_id": work_item_id,
            "lease_epoch": lease_epoch,
            "fencing_token": fencing_token,
            "changed_work_item_ids": changed,
        }),
    )))
}

/// The terminal commit an outcome bundle has to be bound to.
///
/// These are the admitted `Method::CommitWorkItemResult` facts plus the batch
/// the commit rides in. A bundle is only trustworthy if it agrees with ALL of
/// them at once -- a bundle that names the right work item but the wrong worker,
/// fence or batch is exactly the forgery this check exists to refuse -- so they
/// are carried as one claim rather than six positional facts a caller can
/// transpose.
struct TerminalCommitClaim<'a> {
    /// The worker the lease is held by, and which the bundle's
    /// `executor_lease_actor` must name.
    worker_id: &'a str,
    work_item_id: &'a str,
    fencing_token: u64,
    /// `succeeded` / `failed` / `cancelled`, already validated by the caller.
    outcome: &'a str,
    result_ref: &'a Option<String>,
    /// The mutation batch this commit rides in; the bundle's outbox id must
    /// match it, so a bundle cannot be replayed under a different batch.
    batch_id: &'a str,
}

fn validate_terminal_extension_binding(
    props: &serde_json::Map<String, serde_json::Value>,
    extension: &eg_types::outcome_bundle::TerminalOutcomeExtension,
    claim: TerminalCommitClaim<'_>,
) -> Result<(), String> {
    let TerminalCommitClaim {
        worker_id,
        work_item_id,
        fencing_token,
        outcome,
        result_ref,
        batch_id,
    } = claim;
    extension.validate()?;
    let bundle = &extension.outcome_bundle;
    if bundle.outcome != outcome {
        return Err(
            "terminal outcome bundle outcome does not match the terminal method".to_string(),
        );
    }
    if bundle.work_item_id != work_item_id
        || bundle.fence_token != fencing_token
        || bundle.result_ref != *result_ref
    {
        return Err("terminal outcome bundle does not match the WorkItem CAS".to_string());
    }
    if bundle.executor_lease_actor != worker_id {
        return Err("terminal outcome bundle executor does not own the lease".to_string());
    }
    if bundle.outbox_id != batch_id {
        return Err("terminal outcome bundle outbox does not match the mutation batch".to_string());
    }
    let metadata = props
        .get("metadata")
        .and_then(serde_json::Value::as_object)
        .ok_or_else(|| "WorkItem metadata is missing delegation bindings".to_string())?;
    let context = props
        .get("context")
        .and_then(serde_json::Value::as_object)
        .ok_or_else(|| "WorkItem context is missing delegation bindings".to_string())?;
    require_binding(
        "delegation_id",
        metadata.get("delegation_id"),
        &bundle.delegation_id,
    )?;
    require_binding("run_id", metadata.get("run_id"), &bundle.run_id)?;
    require_binding(
        "delegator_id",
        context.get("agent_id"),
        &bundle.delegator_id,
    )?;
    require_binding(
        "selected_agent_id",
        metadata.get("agent_id"),
        &bundle.selected_agent_id,
    )?;
    require_digest_binding(
        "capability_digest",
        metadata.get("capability_digest"),
        &bundle.capability_digest,
    )?;
    for (field, expected) in [
        ("catalog_digest", &bundle.catalog_digest),
        ("model_digest", &bundle.model_digest),
    ] {
        require_digest_binding(field, props.get(field), expected)?;
    }
    require_digest_binding(
        "policy_digest",
        props.get("policy_digest"),
        &bundle.policy_digest,
    )
}

fn require_binding(
    field: &str,
    actual: Option<&serde_json::Value>,
    expected: &str,
) -> Result<(), String> {
    let actual = actual
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| format!("WorkItem binding '{field}' is missing"))?;
    if actual != expected {
        return Err(format!(
            "WorkItem binding '{field}' does not match terminal outcome"
        ));
    }
    Ok(())
}

fn require_digest_binding(
    field: &str,
    actual: Option<&serde_json::Value>,
    expected: &str,
) -> Result<(), String> {
    let actual = actual
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| format!("WorkItem binding '{field}' is missing"))?;
    let normalized = actual.strip_prefix("sha256:").unwrap_or(actual);
    if normalized != expected {
        return Err(format!(
            "WorkItem binding '{field}' does not match terminal outcome"
        ));
    }
    Ok(())
}

fn ensure_receipt_rows_absent(
    graph: &str,
    receipt_nodes: &[eg_types::outcome_bundle::ReceiptNode],
    nodes: &mut ScopedOwnerTableMut<'_, (&str, &str), &[u8]>,
) -> Result<(), String> {
    for receipt in receipt_nodes {
        if nodes.get((graph, receipt.node_id.as_str()))?.is_some() {
            return Err(format!(
                "terminal receipt node '{}' already exists",
                receipt.node_id
            ));
        }
    }
    Ok(())
}

fn apply_receipt_rows(
    graph: &str,
    receipt_nodes: &[eg_types::outcome_bundle::ReceiptNode],
    nodes: &mut ScopedOwnerTableMut<'_, (&str, &str), &[u8]>,
    crypto: DurableCrypto<'_>,
) -> Result<(), String> {
    for receipt in receipt_nodes {
        let sealed = crypto.seal(&receipt.properties_msgpack);
        nodes
            .insert((graph, receipt.node_id.as_str()), sealed.as_ref())
            .map_err(|error| error.to_string())?;
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn apply_cancel_work_item_row(
    graph: &str,
    tenant: &String,
    work_item_id: &String,
    reason_ref: &Option<String>,
    now_ms: u64,
    nodes: &mut ScopedOwnerTableMut<'_, (&str, &str), &[u8]>,
    holds: &mut ScopedOwnerTableMut<'_, (&str, &str), &[u8]>,
    work_item_index: &ScopedOwnerTableMut<'_, (&str, &str, u64), &str>,
    counters: &mut ScopedOwnerTableMut<'_, (&str, &str), &[u8]>,
    pressure_index: &mut ScopedOwnerTableMut<'_, (&str, &str, &str, &str, u64, &str), u8>,
    policies: &ScopedOwnerTableMut<'_, (&str, &str), &[u8]>,
    crypto: DurableCrypto<'_>,
) -> Result<Option<crate::protocol::ResultPayload>, String> {
    let decode = |bytes: &[u8]| -> Result<serde_json::Map<String, serde_json::Value>, String> {
        decode_durable(bytes)
    };
    let current = nodes
        .get((graph, work_item_id.as_str()))?
        .map(|value| crypto.unseal(value.value()))
        .transpose()?;
    let Some(bytes) = current else {
        return Ok(Some(crate::protocol::ResultPayload::Json(
            serde_json::json!({"status": "missing", "changed_work_item_ids": []}),
        )));
    };
    let mut props = decode(&bytes)?;
    let pre_props = props.clone();
    if property_string(&props, "tenant") != tenant {
        return Ok(Some(crate::protocol::ResultPayload::Json(
            serde_json::json!({"status": "missing", "changed_work_item_ids": []}),
        )));
    }
    if matches!(
        property_string(&props, "status"),
        "succeeded" | "failed" | "cancelled" | "dead_letter"
    ) {
        return Ok(Some(crate::protocol::ResultPayload::Json(
            serde_json::json!({
                "status": "noop",
                "work_item_id": work_item_id,
                "changed_work_item_ids": [],
            }),
        )));
    }
    let now_s = now_ms as f64 / 1000.0;
    if matches!(property_string(&props, "status"), "leased" | "running")
        && property_f64(&props, "lease_expires_at") >= now_s
    {
        return Ok(Some(crate::protocol::ResultPayload::Json(
            serde_json::json!({
                "status": "in_flight",
                "work_item_id": work_item_id,
                "changed_work_item_ids": [],
            }),
        )));
    }
    if !matches!(
        property_string(&props, "status"),
        "submitted" | "ready" | "leased" | "running"
    ) {
        return Ok(Some(crate::protocol::ResultPayload::Json(
            serde_json::json!({
                "status": "not_cancellable",
                "work_item_id": work_item_id,
                "changed_work_item_ids": [],
            }),
        )));
    }
    // Phase-1 mirror: capture the pre-status (a cancellable non-terminal state)
    // before the authority marks it cancelled.
    #[cfg(feature = "statechart")]
    let pre_status = property_string(&props, "status").to_string();
    let lease_owner = property_string(&props, "lease_owner");
    let last_lease_owner = if lease_owner.is_empty() {
        property_string(&props, "last_lease_owner")
    } else {
        lease_owner
    }
    .to_string();
    let next_epoch = property_u64(&props, "lease_epoch")
        .checked_add(1)
        .ok_or_else(|| "CancelWorkItem lease epoch overflow".to_string())?;
    let next_fencing_token = property_u64(&props, "fencing_token")
        .checked_add(1)
        .ok_or_else(|| "CancelWorkItem fencing token overflow".to_string())?;
    props.insert(
        "status".into(),
        serde_json::Value::String("cancelled".into()),
    );
    props.insert("completed_at".into(), serde_json::Value::from(now_s));
    props.insert("updated_at".into(), serde_json::Value::from(now_s));
    props.insert("lease_owner".into(), serde_json::Value::Null);
    props.insert(
        "last_lease_owner".into(),
        serde_json::Value::String(last_lease_owner),
    );
    props.insert("lease_expires_at".into(), serde_json::Value::Null);
    props.insert("lease_epoch".into(), serde_json::Value::from(next_epoch));
    props.insert(
        "fencing_token".into(),
        serde_json::Value::from(next_fencing_token),
    );
    props.insert(
        "cancel_reason_ref".into(),
        reason_ref
            .clone()
            .map(serde_json::Value::String)
            .unwrap_or(serde_json::Value::Null),
    );
    development_lane::transition_work_item_terminal_hold(
        graph,
        &pre_props,
        work_item_id,
        "cancelled",
        true,
        property_u64(&props, "attempt"),
        property_u64(&props, "lease_epoch"),
        property_u64(&props, "fencing_token"),
        property_string(&props, "work_item_fence"),
        holds,
        work_item_index,
        counters,
        pressure_index,
        policies,
        crypto,
    )?;
    #[cfg(feature = "statechart")]
    apply_work_item_mirror(
        &mut props,
        work_item_id,
        &pre_status,
        crate::work_item_statechart::EV_CANCEL,
        serde_json::json!({ "cancellable": true }),
        Some("cancelled"),
    );
    write_work_item_props(nodes, graph, work_item_id, &props, crypto)?;
    Ok(Some(crate::protocol::ResultPayload::Json(
        serde_json::json!({
            "status": "cancelled",
            "work_item_id": work_item_id,
            "lease_epoch": next_epoch,
            "fencing_token": next_fencing_token,
            "changed_work_item_ids": [work_item_id],
        }),
    )))
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn apply_defer_work_item_row(
    graph: &str,
    tenant: &String,
    work_item_id: &String,
    worker_id: &String,
    lease_epoch: u64,
    fencing_token: u64,
    next_retry_at_ms: u64,
    reason_ref: &Option<String>,
    now_ms: u64,
    nodes: &mut ScopedOwnerTableMut<'_, (&str, &str), &[u8]>,
    crypto: DurableCrypto<'_>,
) -> Result<Option<crate::protocol::ResultPayload>, String> {
    let decode = |bytes: &[u8]| -> Result<serde_json::Map<String, serde_json::Value>, String> {
        decode_durable(bytes)
    };
    if next_retry_at_ms < now_ms {
        return Err("DeferWorkItem next_retry_at_ms must not precede now_ms".into());
    }
    let current = nodes
        .get((graph, work_item_id.as_str()))?
        .map(|value| crypto.unseal(value.value()))
        .transpose()?;
    let Some(bytes) = current else {
        return Ok(Some(crate::protocol::ResultPayload::Json(
            serde_json::json!({"status": "missing", "changed_work_item_ids": []}),
        )));
    };
    let mut props = decode(&bytes)?;
    let now_s = now_ms as f64 / 1000.0;
    let valid = property_string(&props, "tenant") == tenant
        && property_string(&props, "lease_owner") == worker_id
        && matches!(property_string(&props, "status"), "leased" | "running")
        && property_u64(&props, "lease_epoch") == lease_epoch
        && property_u64(&props, "fencing_token") == fencing_token
        && property_f64(&props, "lease_expires_at") >= now_s;
    if !valid {
        return Ok(Some(crate::protocol::ResultPayload::Json(
            serde_json::json!({
                "status": "fenced",
                "work_item_id": work_item_id,
                "changed_work_item_ids": [],
            }),
        )));
    }
    // Phase-1 mirror: capture the leased|running pre-status before the fenced
    // lease is released back to `ready`.
    #[cfg(feature = "statechart")]
    let pre_status = property_string(&props, "status").to_string();
    let next_epoch = (lease_epoch).saturating_add(1);
    let attempts = property_u64(&props, "attempt").saturating_sub(1);
    let defer_count = property_u64(&props, "defer_count").saturating_add(1);
    props.insert("status".into(), serde_json::Value::String("ready".into()));
    props.insert(
        "next_retry_at".into(),
        serde_json::Value::from(next_retry_at_ms as f64 / 1000.0),
    );
    props.insert("attempt".into(), serde_json::Value::from(attempts));
    props.insert("defer_count".into(), serde_json::Value::from(defer_count));
    props.insert("lease_owner".into(), serde_json::Value::Null);
    props.insert("lease_expires_at".into(), serde_json::Value::Null);
    props.insert("lease_epoch".into(), serde_json::Value::from(next_epoch));
    props.insert("fencing_token".into(), serde_json::Value::from(next_epoch));
    props.insert("updated_at".into(), serde_json::Value::from(now_s));
    props.insert(
        "defer_reason_ref".into(),
        reason_ref
            .clone()
            .map(serde_json::Value::String)
            .unwrap_or(serde_json::Value::Null),
    );
    #[cfg(feature = "statechart")]
    apply_work_item_mirror(
        &mut props,
        work_item_id,
        &pre_status,
        crate::work_item_statechart::EV_DEFER,
        serde_json::json!({ "fence_valid": true }),
        Some("ready"),
    );
    write_work_item_props(nodes, graph, work_item_id, &props, crypto)?;
    Ok(Some(crate::protocol::ResultPayload::Json(
        serde_json::json!({
            "status": "deferred",
            "work_item_id": work_item_id,
            "lease_epoch": next_epoch,
            "fencing_token": next_epoch,
            "next_retry_at_ms": next_retry_at_ms,
            "attempt": attempts,
            "defer_count": defer_count,
            "changed_work_item_ids": [work_item_id],
        }),
    )))
}
