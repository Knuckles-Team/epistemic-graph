//! The fleet catalog read projection: observations + overrides + published
//! connector-pack components -> typed rows, filtered, ordered, counted,
//! digested and paged.
//!
//! Pure. Every input is already loaded and already visibility-checked; this
//! module only decides what each row SAYS and which page it lands on.

use std::collections::BTreeMap;

use eg_types::agent_component::{
    AgentComponentEntry, AgentComponentFacts, AgentComponentKind, ComponentProvenance,
};
use eg_types::agent_library::AgentLibraryLifecycle;
use eg_types::connector_pack::digest::entry_kind_token;
use eg_types::connector_pack::ids::PACK_COMPONENT_ID_PREFIX;
use eg_types::connector_pack::index::PackEntryKind;
use eg_types::contract::{BoundedVec, Digest256, ResourceId};
use eg_types::fleet_catalog::{
    FleetCatalogCursor, FleetCatalogKind, FleetCatalogListRequest, FleetCatalogPage,
    FleetCatalogRow, FleetComponentRef, FleetDiscoveryRow, FleetPromptRow, FleetResourceRow,
    FleetRowAcl, FleetRowSubject, FleetSkillRow, FleetToolRow, FleetVisibility, ResourceKind,
    SkillType, SkillTypeSource, ToolMode, FLEET_CATALOG_SCHEMA_VERSION, MAX_FLEET_SNAPSHOT_ROWS,
};
use eg_types::result_contract::cluster::ServerDesiredState;

use super::records::{discovery_record_id, DiscoveryBody, StoredRecord};

const SNAPSHOT_DOMAIN: &[u8] = b"eg/fleet-catalog-snapshot/v1";
const CURSOR_STALE: &str =
    "FLEET_CURSOR_STALE: the fleet catalog snapshot changed; restart from the first page";
/// The attribute keys a connector pack records for facts that have no typed
/// field yet. The ONE place they are read.
const ATTR_URI: &str = "mcp.uri";
const ATTR_MEDIA_TYPE: &str = "mcp.media_type";
const ATTR_SKILL_TYPE: &str = "skill.type";
const ATTR_TOOL_MODE: &str = "sdk.tool_mode";

/// One observation the viewer may see, and how.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct VisibleDiscovery {
    pub(super) record: StoredRecord<DiscoveryBody>,
    pub(super) visibility: FleetVisibility,
}

/// The observation a connector's content is visible through.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct ConnectorBinding {
    pub(super) server_name: String,
    pub(super) connector: ResourceId,
    pub(super) visibility: FleetVisibility,
}

/// Registry and override state every content row consults.
pub(super) struct ProjectionInputs<'a> {
    pub(super) desired: &'a BTreeMap<String, ServerDesiredState>,
    pub(super) skill_overrides: &'a BTreeMap<String, (SkillType, u64)>,
}

/// One binding per connector, through its WIDEST visible observation.
///
/// Tenant-wide observations sort before principal-scoped ones, so a connector
/// named by both is shown tenant-wide once, never twice.
pub(super) fn connector_bindings(discoveries: &[VisibleDiscovery]) -> Vec<ConnectorBinding> {
    let rank = |discovery: &VisibleDiscovery| {
        (
            matches!(discovery.visibility, FleetVisibility::Principal { .. }),
            discovery.record.body.server_name.clone(),
            discovery.record.body.scope,
        )
    };
    let mut ordered: Vec<&VisibleDiscovery> = discoveries.iter().collect();
    ordered.sort_by_key(|left| rank(left));
    let mut bound: BTreeMap<&str, ConnectorBinding> = BTreeMap::new();
    for discovery in ordered {
        bound
            .entry(discovery.record.body.connector.as_str())
            .or_insert_with(|| ConnectorBinding {
                server_name: discovery.record.body.server_name.clone(),
                connector: discovery.record.body.connector.clone(),
                visibility: discovery.visibility.clone(),
            });
    }
    bound.into_values().collect()
}

/// The pack entry kind a content kind lists; `None` for discoveries.
fn pack_kind(kind: FleetCatalogKind) -> Option<PackEntryKind> {
    match kind {
        FleetCatalogKind::Discoveries => None,
        FleetCatalogKind::Tools => Some(PackEntryKind::Tool),
        FleetCatalogKind::Prompts => Some(PackEntryKind::Prompt),
        FleetCatalogKind::Resources => Some(PackEntryKind::Resource),
        FleetCatalogKind::Skills => Some(PackEntryKind::Skill),
    }
}

/// The component kind a content kind lists.
fn component_kind(kind: FleetCatalogKind) -> Option<AgentComponentKind> {
    match kind {
        FleetCatalogKind::Discoveries => None,
        FleetCatalogKind::Tools => Some(AgentComponentKind::Tool),
        FleetCatalogKind::Prompts => Some(AgentComponentKind::McpPrompt),
        FleetCatalogKind::Resources => Some(AgentComponentKind::McpResource),
        FleetCatalogKind::Skills => Some(AgentComponentKind::Skill),
    }
}

/// `mcp:<connector>/<kind token>/` -- the exact key prefix of one connector's
/// members of one kind. Exact because a pack-escaped name never contains `/`
/// and a bound connector never does either.
pub(super) fn member_prefix(connector: &ResourceId, kind: FleetCatalogKind) -> Option<String> {
    pack_kind(kind).map(|pack_kind| {
        format!(
            "{PACK_COMPONENT_ID_PREFIX}{}/{}/",
            connector.as_str(),
            entry_kind_token(pack_kind)
        )
    })
}

/// Split a member id into `(connector, kind)`, or `None` for anything that is
/// not a fleet-listable pack member.
pub(super) fn parse_member_id(component_id: &str) -> Option<(ResourceId, FleetCatalogKind)> {
    let rest = component_id.strip_prefix(PACK_COMPONENT_ID_PREFIX)?;
    let (connector, rest) = rest.split_once('/')?;
    let (token, name) = rest.split_once('/')?;
    if name.is_empty() || name.contains('/') {
        return None;
    }
    let kind = [
        FleetCatalogKind::Tools,
        FleetCatalogKind::Prompts,
        FleetCatalogKind::Resources,
        FleetCatalogKind::Skills,
    ]
    .into_iter()
    .find(|kind| pack_kind(*kind).map(entry_kind_token) == Some(token))?;
    Some((ResourceId::new(connector).ok()?, kind))
}

/// Reverse the pack's percent-escaping of an entry name.
fn unescape_pack_name(escaped: &str) -> Option<String> {
    let bytes = escaped.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'%' {
            let hex = escaped.get(index + 1..index + 3)?;
            out.push(u8::from_str_radix(hex, 16).ok()?);
            index += 3;
        } else {
            out.push(bytes[index]);
            index += 1;
        }
    }
    String::from_utf8(out).ok()
}

/// The name the server itself uses: the provenance's upstream name when the
/// record carries one, otherwise the unescaped last segment of its id.
fn component_name(entry: &AgentComponentEntry) -> String {
    if let ComponentProvenance::McpServer { upstream_name, .. } = &entry.provenance {
        return upstream_name.clone();
    }
    let escaped = entry.component_id.rsplit('/').next().unwrap_or_default();
    unescape_pack_name(escaped).unwrap_or_else(|| escaped.to_string())
}

fn attribute<'a>(entry: &'a AgentComponentEntry, key: &str) -> Option<&'a str> {
    entry.attributes.get(key).map(String::as_str)
}

pub(super) fn discovery_row(discovery: &VisibleDiscovery) -> FleetCatalogRow {
    let record = &discovery.record;
    FleetCatalogRow::Discovery {
        row: FleetDiscoveryRow {
            id: discovery_record_id(&record.body.server_name, &record.body.scope),
            server_name: record.body.server_name.clone(),
            scope: record.body.scope,
            connector: record.body.connector.clone(),
            outcome: record.body.outcome.clone(),
            counts: record.body.counts,
            observed_at_ms: record.meta.written_at_ms,
            revision: record.meta.revision,
            acl: FleetRowAcl {
                tenant_id: record.meta.tenant_id.clone(),
                visibility: discovery.visibility.clone(),
                publisher: record.body.observer.clone(),
            },
        },
    }
}

fn component_ref(
    entry: &AgentComponentEntry,
    binding: &ConnectorBinding,
    inputs: &ProjectionInputs<'_>,
) -> FleetComponentRef {
    FleetComponentRef {
        id: entry.component_id.clone(),
        name: component_name(entry),
        description: entry.summary.clone(),
        server_name: binding.server_name.clone(),
        connector: binding.connector.clone(),
        enabled: inputs.desired.get(&binding.server_name) != Some(&ServerDesiredState::Disabled),
        entry_revision: entry.entry_revision,
        definition_digest: entry.definition_digest.clone(),
        acl: FleetRowAcl {
            tenant_id: entry.tenant_id.clone(),
            visibility: binding.visibility.clone(),
            publisher: entry.actor_scope.clone(),
        },
    }
}

/// The row one published member shows, or `None` when the record is not a
/// currently-served member of `kind` (retired, withdrawn, another kind, or a
/// tool without tool facts).
pub(super) fn component_row(
    kind: FleetCatalogKind,
    entry: &AgentComponentEntry,
    binding: &ConnectorBinding,
    inputs: &ProjectionInputs<'_>,
) -> Option<FleetCatalogRow> {
    if entry.lifecycle != AgentLibraryLifecycle::Published || component_kind(kind)? != entry.kind {
        return None;
    }
    let component = component_ref(entry, binding, inputs);
    let uri = attribute(entry, ATTR_URI).unwrap_or_default().to_string();
    match kind {
        FleetCatalogKind::Discoveries => None,
        FleetCatalogKind::Tools => tool_row(entry, component),
        FleetCatalogKind::Prompts => Some(FleetCatalogRow::Prompt {
            row: FleetPromptRow { component, uri },
        }),
        FleetCatalogKind::Resources => Some(FleetCatalogRow::Resource {
            row: FleetResourceRow {
                component,
                resource_kind: ResourceKind::from_uri(&uri),
                uri,
                media_type: attribute(entry, ATTR_MEDIA_TYPE).map(str::to_string),
            },
        }),
        FleetCatalogKind::Skills => Some(skill_row(entry, component, uri, inputs)),
    }
}

fn tool_row(entry: &AgentComponentEntry, component: FleetComponentRef) -> Option<FleetCatalogRow> {
    let AgentComponentFacts::Tool {
        effect,
        input_schema_digest,
        ..
    } = &entry.facts
    else {
        return None;
    };
    Some(FleetCatalogRow::Tool {
        row: FleetToolRow {
            component,
            input_schema_digest: input_schema_digest.clone(),
            effect: *effect,
            tool_mode: ToolMode::from_declared(attribute(entry, ATTR_TOOL_MODE)),
        },
    })
}

/// A skill's type: an operator override wins, then the skill's own
/// declaration, then the atomic default -- and the row says which decided it.
fn skill_row(
    entry: &AgentComponentEntry,
    component: FleetComponentRef,
    uri: String,
    inputs: &ProjectionInputs<'_>,
) -> FleetCatalogRow {
    let declared = attribute(entry, ATTR_SKILL_TYPE).and_then(SkillType::from_declared);
    let (skill_type, skill_type_source, override_revision) =
        match (inputs.skill_overrides.get(&entry.component_id), declared) {
            (Some((overridden, revision)), _) => {
                (*overridden, SkillTypeSource::Override, Some(*revision))
            }
            (None, Some(declared)) => (declared, SkillTypeSource::Declared, None),
            (None, None) => (SkillType::Skill, SkillTypeSource::Default, None),
        };
    FleetCatalogRow::Skill {
        row: FleetSkillRow {
            component,
            uri,
            skill_type,
            classification: skill_type.label().to_string(),
            skill_type_source,
            override_revision,
        },
    }
}

/// `(lowercased name, id)`: the order rows are listed and resumed in.
fn sort_key(row: &FleetCatalogRow) -> (String, &str) {
    let (name, id) = row.key();
    (name.to_ascii_lowercase(), id)
}

fn matches_query(row: &FleetCatalogRow, needle: &str) -> bool {
    let haystacks: [&str; 3] = match row.subject() {
        FleetRowSubject::Component(component) => [
            &component.name,
            &component.server_name,
            &component.description,
        ],
        FleetRowSubject::Observation(discovery) => [&discovery.server_name, "", ""],
    };
    haystacks
        .iter()
        .any(|haystack| haystack.to_ascii_lowercase().contains(needle))
}

/// Filter, order and bound one kind's visible snapshot.
pub(super) fn filtered_snapshot(
    mut rows: Vec<FleetCatalogRow>,
    query: Option<&str>,
) -> Result<Vec<FleetCatalogRow>, String> {
    if let Some(needle) = query.map(str::to_ascii_lowercase).filter(|q| !q.is_empty()) {
        rows.retain(|row| matches_query(row, &needle));
    }
    if rows.len() > MAX_FLEET_SNAPSHOT_ROWS {
        return Err(format!(
            "FLEET_SNAPSHOT_TOO_LARGE: {} visible rows exceed the {MAX_FLEET_SNAPSHOT_ROWS}-row snapshot bound; narrow the query",
            rows.len()
        ));
    }
    rows.sort_by(|left, right| sort_key(left).cmp(&sort_key(right)));
    Ok(rows)
}

fn snapshot_digest(kind: FleetCatalogKind, rows: &[FleetCatalogRow]) -> Result<Digest256, String> {
    let encoded = rmp_serde::to_vec_named(rows)
        .map_err(|error| format!("fleet catalog snapshot encoding failed: {error}"))?;
    Digest256::framed(
        SNAPSHOT_DOMAIN,
        &[
            &FLEET_CATALOG_SCHEMA_VERSION.to_be_bytes(),
            kind.as_str().as_bytes(),
            encoded.as_slice(),
        ],
    )
}

/// Where the requested page starts, after checking its cursor against THIS
/// snapshot. Fenced by content only: a heartbeat elsewhere in `__commons__` that
/// changes nothing this kind shows does not invalidate a caller's cursor.
fn page_start(
    rows: &[FleetCatalogRow],
    cursor: Option<&FleetCatalogCursor>,
    digest: Digest256,
) -> Result<usize, String> {
    let Some(cursor) = cursor else {
        return Ok(0);
    };
    if cursor.snapshot_digest != digest {
        return Err(CURSOR_STALE.to_string());
    }
    let after = (
        cursor.after_name.to_ascii_lowercase(),
        cursor.after_id.as_str(),
    );
    Ok(rows.partition_point(|row| {
        let (name, id) = sort_key(row);
        (name.as_str(), id) <= (after.0.as_str(), after.1)
    }))
}

/// Cut one page from an already filtered and ordered snapshot.
pub(super) fn page(
    rows: Vec<FleetCatalogRow>,
    request: &FleetCatalogListRequest,
    commons_revision: u64,
    observed_at_ms: u64,
) -> Result<FleetCatalogPage, String> {
    let digest = snapshot_digest(request.kind, &rows)?;
    let start = page_start(&rows, request.cursor.as_ref(), digest)?;
    let end = start.saturating_add(request.page_limit()).min(rows.len());
    let next_cursor = (end < rows.len()).then(|| {
        let (name, id) = rows[end - 1].key();
        FleetCatalogCursor {
            after_name: name.to_string(),
            after_id: id.to_string(),
            snapshot_digest: digest,
        }
    });
    let total = u32::try_from(rows.len())
        .map_err(|_| "FLEET_SNAPSHOT_TOO_LARGE: row count exceeds u32".to_string())?;
    let page_rows = rows.into_iter().skip(start).take(end - start).collect();
    Ok(FleetCatalogPage {
        schema_version: FLEET_CATALOG_SCHEMA_VERSION,
        kind: request.kind,
        rows: BoundedVec::new(page_rows)?,
        total,
        next_cursor,
        commons_revision,
        snapshot_digest: digest,
        observed_at_ms,
    })
}

#[cfg(test)]
#[path = "project_tests.rs"]
mod tests;
