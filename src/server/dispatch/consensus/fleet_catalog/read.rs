//! Fleet catalog reads: the caller's tenant-scoped records from the Agent
//! Library owner, the registry's desired state from `__commons__`, and the
//! connector-pack components those records make visible.

use std::collections::BTreeMap;

use eg_types::contract::{BoundedVec, Digest256};
use eg_types::fleet_catalog::{
    FleetCatalogKind, FleetCatalogListRequest, FleetCatalogLookup, FleetCatalogLookupRequest,
    FleetCatalogPage, FleetCatalogRow, SkillType, FLEET_CATALOG_SCHEMA_VERSION,
    MAX_FLEET_SNAPSHOT_ROWS,
};
use eg_types::result_contract::cluster::{
    FleetCatalogList, FleetCatalogLookupRows, ServerDesiredState,
};

use super::super::registry::{live_desired_states, REGISTRY_GRAPH};
use super::project::{
    component_row, connector_bindings, discovery_row, filtered_snapshot, member_prefix, page,
    parse_member_id, ConnectorBinding, ProjectionInputs, VisibleDiscovery,
};
use super::records::{
    decode_record, discovery_record_id, skill_type_override, DiscoveryBody, OverrideBody, Viewer,
};
use super::*;
use crate::server::persistence::agent_library::AgentLibraryStore;
use crate::server::persistence::fleet_records::FleetRecordFamily;

/// Everything one read joins, taken before any row is built.
struct FleetView {
    commons_revision: u64,
    observed_at_ms: u64,
    discoveries: Vec<VisibleDiscovery>,
    skill_overrides: BTreeMap<String, (SkillType, u64)>,
    desired: BTreeMap<String, ServerDesiredState>,
}

impl FleetView {
    fn inputs(&self) -> ProjectionInputs<'_> {
        ProjectionInputs {
            desired: &self.desired,
            skill_overrides: &self.skill_overrides,
        }
    }
}

/// Every observation of the caller's tenant it may see, in record-id order.
/// The store answers only the tenant's own rows; the viewer adds the
/// principal/grant half the store cannot decide.
fn visible_discoveries(
    store: &AgentLibraryStore,
    viewer: &Viewer<'_>,
) -> Result<Vec<VisibleDiscovery>, String> {
    let rows = store.fleet_records(
        viewer.tenant_id,
        FleetRecordFamily::Discovery,
        MAX_FLEET_SNAPSHOT_ROWS,
    )?;
    Ok(rows
        .iter()
        .filter_map(|(_, row)| decode_record::<DiscoveryBody>(viewer.tenant_id, row))
        .filter_map(|record| {
            viewer
                .sees(&record)
                .map(|visibility| VisibleDiscovery { record, visibility })
        })
        .collect())
}

/// The tenant's live skill-type overrides, by component id.
fn live_skill_overrides(
    store: &AgentLibraryStore,
    tenant_id: &str,
) -> Result<BTreeMap<String, (SkillType, u64)>, String> {
    let rows = store.fleet_records(
        tenant_id,
        FleetRecordFamily::Override,
        MAX_FLEET_SNAPSHOT_ROWS,
    )?;
    Ok(rows
        .iter()
        .filter_map(|(_, row)| decode_record::<OverrideBody>(tenant_id, row))
        .filter_map(|record| {
            skill_type_override(&record).map(|value| (record.body.component_id.clone(), value))
        })
        .collect())
}

/// The registry half: each live, caller-visible server's desired state, read
/// through the same graph ACL and row-level projection as `ListRegisteredServers`.
async fn registry_desired(
    state: &Arc<RwLock<ServerState>>,
    verified: &VerifiedRequestContext,
) -> Result<(u64, u64, BTreeMap<String, ServerDesiredState>), String> {
    let _registry_guard = crate::server::mutation_batch::lock_graph(REGISTRY_GRAPH).await;
    let (core, authority) = {
        let current = timed_read(state).await;
        let entry = current
            .registry
            .get(REGISTRY_GRAPH)
            .ok_or("fleet catalog registry authority is unavailable")?;
        check_graph_access(
            &current.isolation,
            Some(verified.agent_id()),
            REGISTRY_GRAPH,
            entry.graph_type,
            entry.owner.as_deref(),
            AccessLevel::Read,
        )?;
        let authority = GraphReadAuthority::from_verified(verified, &current.isolation)?;
        (entry.core.clone(), authority)
    };
    let observed_at_ms = authoritative_now_ms();
    let desired = live_desired_states(&core, observed_at_ms, |node_id, properties| {
        authority.can_see_node(properties, core.is_schema_node(node_id))
    });
    Ok((core.version(), observed_at_ms, desired))
}

async fn load_view(
    state: &Arc<RwLock<ServerState>>,
    store: &AgentLibraryStore,
    verified: &VerifiedRequestContext,
    grants: &[Digest256],
) -> Result<FleetView, String> {
    let (commons_revision, observed_at_ms, desired) = registry_desired(state, verified).await?;
    let principal = verified.principal_persistence_id();
    let viewer = Viewer {
        tenant_id: verified.tenant(),
        principal: &principal,
        grants,
    };
    Ok(FleetView {
        commons_revision,
        observed_at_ms,
        discoveries: visible_discoveries(store, &viewer)?,
        skill_overrides: live_skill_overrides(store, verified.tenant())?,
        desired,
    })
}

/// Every visible member of `kind`, through each connector's widest binding.
fn content_rows(
    store: &AgentLibraryStore,
    tenant_id: &str,
    view: &FleetView,
    kind: FleetCatalogKind,
) -> Result<Vec<FleetCatalogRow>, String> {
    let inputs = view.inputs();
    let mut rows = Vec::new();
    for binding in connector_bindings(&view.discoveries) {
        let Some(prefix) = member_prefix(&binding.connector, kind) else {
            continue;
        };
        let heads =
            store.component_heads_with_prefix(tenant_id, &prefix, MAX_FLEET_SNAPSHOT_ROWS)?;
        rows.extend(
            heads
                .iter()
                .filter_map(|entry| component_row(kind, entry, &binding, &inputs)),
        );
        if rows.len() > MAX_FLEET_SNAPSHOT_ROWS {
            return Err(format!(
                "FLEET_SNAPSHOT_TOO_LARGE: more than {MAX_FLEET_SNAPSHOT_ROWS} visible {} rows",
                kind.as_str()
            ));
        }
    }
    Ok(rows)
}

async fn list_page(
    state: &Arc<RwLock<ServerState>>,
    store: &AgentLibraryStore,
    verified: &VerifiedRequestContext,
    request: &FleetCatalogListRequest,
) -> Result<FleetCatalogPage, String> {
    let view = load_view(state, store, verified, request.grant_digests.as_slice()).await?;
    let rows = if request.kind == FleetCatalogKind::Discoveries {
        view.discoveries.iter().map(discovery_row).collect()
    } else {
        content_rows(store, verified.tenant(), &view, request.kind)?
    };
    let rows = filtered_snapshot(rows, request.query.as_deref())?;
    page(rows, request, view.commons_revision, view.observed_at_ms)
}

/// The observation a lookup id names, if the caller may see it.
fn lookup_discovery(view: &FleetView, id: &str) -> Option<FleetCatalogRow> {
    view.discoveries
        .iter()
        .find(|discovery| {
            discovery_record_id(
                &discovery.record.body.server_name,
                &discovery.record.body.scope,
            ) == id
        })
        .map(discovery_row)
}

/// Resolve one lookup id, or `None` when it names nothing the caller may see.
fn lookup_one(
    store: &AgentLibraryStore,
    tenant_id: &str,
    view: &FleetView,
    bindings: &[ConnectorBinding],
    id: &str,
) -> Result<Option<FleetCatalogRow>, String> {
    if let Some(row) = lookup_discovery(view, id) {
        return Ok(Some(row));
    }
    let Some((connector, kind)) = parse_member_id(id) else {
        return Ok(None);
    };
    let Some(binding) = bindings
        .iter()
        .find(|binding| binding.connector == connector)
    else {
        return Ok(None);
    };
    Ok(store
        .current_component(tenant_id, id)?
        .and_then(|entry| component_row(kind, &entry, binding, &view.inputs())))
}

async fn lookup_rows(
    state: &Arc<RwLock<ServerState>>,
    store: &AgentLibraryStore,
    verified: &VerifiedRequestContext,
    request: &FleetCatalogLookupRequest,
) -> Result<FleetCatalogLookup, String> {
    let view = load_view(state, store, verified, request.grant_digests.as_slice()).await?;
    let bindings = connector_bindings(&view.discoveries);
    let mut rows: Vec<FleetCatalogRow> = Vec::new();
    for id in request.ids.iter() {
        if rows.iter().any(|row| row.key().1 == id) {
            continue;
        }
        rows.extend(lookup_one(store, verified.tenant(), &view, &bindings, id)?);
    }
    Ok(FleetCatalogLookup {
        schema_version: FLEET_CATALOG_SCHEMA_VERSION,
        rows: BoundedVec::new(rows)?,
        commons_revision: view.commons_revision,
        observed_at_ms: view.observed_at_ms,
    })
}

pub(super) async fn list(
    state: &Arc<RwLock<ServerState>>,
    store: &AgentLibraryStore,
    req_id: u64,
    verified: &VerifiedRequestContext,
    request: FleetCatalogListRequest,
) -> Response {
    respond::<FleetCatalogList>(req_id, list_page(state, store, verified, &request).await)
}

pub(super) async fn lookup(
    state: &Arc<RwLock<ServerState>>,
    store: &AgentLibraryStore,
    req_id: u64,
    verified: &VerifiedRequestContext,
    request: FleetCatalogLookupRequest,
) -> Response {
    respond::<FleetCatalogLookupRows>(req_id, lookup_rows(state, store, verified, &request).await)
}
