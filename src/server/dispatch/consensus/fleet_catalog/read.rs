//! Fleet catalog reads: one consistent view of the `__commons__` records, then
//! the connector-pack components those records make visible.

use std::collections::BTreeMap;

use eg_types::agent_component::AgentComponentEntry;
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
    DISCOVERY_NODE_TYPE, OVERRIDE_NODE_TYPE,
};
use super::*;

/// The `__commons__` half of one read, taken under the registry graph's lock so
/// observations, overrides and registrations are one committed image.
struct CommonsView {
    commons_revision: u64,
    observed_at_ms: u64,
    discoveries: Vec<VisibleDiscovery>,
    skill_overrides: BTreeMap<String, (SkillType, u64)>,
    desired: BTreeMap<String, ServerDesiredState>,
}

impl CommonsView {
    fn inputs(&self) -> ProjectionInputs<'_> {
        ProjectionInputs {
            desired: &self.desired,
            skill_overrides: &self.skill_overrides,
        }
    }
}

/// Every observation the viewer may see, in node-id order.
fn visible_discoveries(
    core: &crate::graph::GraphCore,
    viewer: &Viewer<'_>,
) -> Vec<VisibleDiscovery> {
    core.get_nodes_by_label(DISCOVERY_NODE_TYPE, 0)
        .into_iter()
        .filter_map(|(_, properties)| {
            decode_record::<DiscoveryBody>(DISCOVERY_NODE_TYPE, &properties)
        })
        .filter_map(|record| {
            viewer
                .sees(&record)
                .map(|visibility| VisibleDiscovery { record, visibility })
        })
        .collect()
}

/// The tenant's live skill-type overrides, by component id.
fn live_skill_overrides(
    core: &crate::graph::GraphCore,
    tenant_id: &str,
) -> BTreeMap<String, (SkillType, u64)> {
    core.get_nodes_by_label(OVERRIDE_NODE_TYPE, 0)
        .into_iter()
        .filter_map(|(_, properties)| {
            decode_record::<OverrideBody>(OVERRIDE_NODE_TYPE, &properties)
        })
        .filter(|record| record.meta.tenant_id == tenant_id)
        .filter_map(|record| {
            skill_type_override(&record).map(|value| (record.body.component_id.clone(), value))
        })
        .collect()
}

async fn load_commons(
    state: &Arc<RwLock<ServerState>>,
    verified: &VerifiedRequestContext,
    grants: &[Digest256],
) -> Result<CommonsView, String> {
    let _registry_guard = crate::server::mutation_batch::lock_graph(REGISTRY_GRAPH).await;
    let (core, authority) = {
        let current = timed_read(state).await;
        let entry = current
            .registry
            .get(REGISTRY_GRAPH)
            .ok_or("fleet catalog authority is unavailable")?;
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
    let principal = verified.principal_persistence_id();
    let viewer = Viewer {
        tenant_id: verified.tenant(),
        principal: &principal,
        grants,
    };
    Ok(CommonsView {
        commons_revision: core.version(),
        observed_at_ms,
        discoveries: visible_discoveries(&core, &viewer),
        skill_overrides: live_skill_overrides(&core, verified.tenant()),
        desired: live_desired_states(&core, observed_at_ms, |node_id, properties| {
            authority.can_see_node(properties, core.is_schema_node(node_id))
        }),
    })
}

/// Where connector-pack components are read from. The owner is `redb`-backed;
/// a build without it serves observations only and says so by name.
#[cfg(feature = "redb")]
type ComponentStore = Arc<crate::server::persistence::agent_library::AgentLibraryStore>;
/// Uninhabited without `redb`: `component_store` refuses first, so the member
/// reads below can never be reached with one.
#[cfg(not(feature = "redb"))]
type ComponentStore = std::convert::Infallible;

#[cfg(not(feature = "redb"))]
const COMPONENTS_UNAVAILABLE: &str =
    "fleet catalog content is not available in this build (requires the `redb` feature)";

#[cfg(feature = "redb")]
async fn component_store(state: &Arc<RwLock<ServerState>>) -> Result<ComponentStore, String> {
    state.write().await.ensure_agent_library()
}

#[cfg(not(feature = "redb"))]
async fn component_store(_state: &Arc<RwLock<ServerState>>) -> Result<ComponentStore, String> {
    Err(COMPONENTS_UNAVAILABLE.to_string())
}

#[cfg(feature = "redb")]
fn member_heads(
    store: &ComponentStore,
    tenant_id: &str,
    prefix: &str,
) -> Result<Vec<AgentComponentEntry>, String> {
    store.component_heads_with_prefix(tenant_id, prefix, MAX_FLEET_SNAPSHOT_ROWS)
}

#[cfg(not(feature = "redb"))]
fn member_heads(
    store: &ComponentStore,
    _tenant_id: &str,
    _prefix: &str,
) -> Result<Vec<AgentComponentEntry>, String> {
    match *store {}
}

#[cfg(feature = "redb")]
fn member_head(
    store: &ComponentStore,
    tenant_id: &str,
    component_id: &str,
) -> Result<Option<AgentComponentEntry>, String> {
    store.current_component(tenant_id, component_id)
}

#[cfg(not(feature = "redb"))]
fn member_head(
    store: &ComponentStore,
    _tenant_id: &str,
    _component_id: &str,
) -> Result<Option<AgentComponentEntry>, String> {
    match *store {}
}

/// Every visible member of `kind`, through each connector's widest binding.
async fn content_rows(
    state: &Arc<RwLock<ServerState>>,
    tenant_id: &str,
    view: &CommonsView,
    kind: FleetCatalogKind,
) -> Result<Vec<FleetCatalogRow>, String> {
    let bindings = connector_bindings(&view.discoveries);
    if bindings.is_empty() {
        return Ok(Vec::new());
    }
    let store = component_store(state).await?;
    let inputs = view.inputs();
    let mut rows = Vec::new();
    for binding in &bindings {
        let Some(prefix) = member_prefix(&binding.connector, kind) else {
            continue;
        };
        let heads = member_heads(&store, tenant_id, &prefix)?;
        rows.extend(
            heads
                .iter()
                .filter_map(|entry| component_row(kind, entry, binding, &inputs)),
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
    verified: &VerifiedRequestContext,
    request: &FleetCatalogListRequest,
) -> Result<FleetCatalogPage, String> {
    let view = load_commons(state, verified, request.grant_digests.as_slice()).await?;
    let rows = if request.kind == FleetCatalogKind::Discoveries {
        view.discoveries.iter().map(discovery_row).collect()
    } else {
        content_rows(state, verified.tenant(), &view, request.kind).await?
    };
    let rows = filtered_snapshot(rows, request.query.as_deref())?;
    page(rows, request, view.commons_revision, view.observed_at_ms)
}

/// Resolve one lookup id against the view, or `None` when it names nothing the
/// caller may see.
fn lookup_one(
    store: &ComponentStore,
    tenant_id: &str,
    view: &CommonsView,
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
    Ok(member_head(store, tenant_id, id)?
        .and_then(|entry| component_row(kind, &entry, binding, &view.inputs())))
}

/// Whether `id` names a pack member of a connector some visible observation
/// binds -- the only case a lookup has to open the component owner for.
fn names_bound_member(bindings: &[ConnectorBinding], id: &str) -> bool {
    let Some((connector, _)) = parse_member_id(id) else {
        return false;
    };
    bindings
        .iter()
        .any(|binding| binding.connector == connector)
}

async fn lookup_rows(
    state: &Arc<RwLock<ServerState>>,
    verified: &VerifiedRequestContext,
    request: &FleetCatalogLookupRequest,
) -> Result<FleetCatalogLookup, String> {
    let view = load_commons(state, verified, request.grant_digests.as_slice()).await?;
    let bindings = connector_bindings(&view.discoveries);
    let needs_components = request
        .ids
        .iter()
        .any(|id| names_bound_member(&bindings, id));
    let store = if needs_components {
        Some(component_store(state).await?)
    } else {
        None
    };
    let mut rows: Vec<FleetCatalogRow> = Vec::new();
    for id in request.ids.iter() {
        if rows.iter().any(|row| row.key().1 == id) {
            continue;
        }
        let found = match &store {
            Some(store) => lookup_one(store, verified.tenant(), &view, &bindings, id)?,
            None => lookup_discovery(&view, id),
        };
        rows.extend(found);
    }
    Ok(FleetCatalogLookup {
        schema_version: FLEET_CATALOG_SCHEMA_VERSION,
        rows: BoundedVec::new(rows)?,
        commons_revision: view.commons_revision,
        observed_at_ms: view.observed_at_ms,
    })
}

/// The observation half of a lookup, for a request that names no visible
/// component and so never opens the component owner.
fn lookup_discovery(view: &CommonsView, id: &str) -> Option<FleetCatalogRow> {
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

pub(super) async fn list(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    verified: &VerifiedRequestContext,
    request: FleetCatalogListRequest,
) -> Response {
    respond::<FleetCatalogList>(req_id, list_page(state, verified, &request).await)
}

pub(super) async fn lookup(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    verified: &VerifiedRequestContext,
    request: FleetCatalogLookupRequest,
) -> Response {
    respond::<FleetCatalogLookupRows>(req_id, lookup_rows(state, verified, &request).await)
}
