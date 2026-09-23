//! The fleet catalog's two record kinds, stored as `__commons__` graph nodes
//! beside the `:Server` rows they describe.
//!
//! Pure: node identity, encoding, decoding, the compare-and-set decision and
//! the visibility predicate. Nothing here touches a lock, a store or a clock.

use eg_types::contract::{Digest256, ResourceId};
use eg_types::fleet_catalog::{
    DiscoveryCounts, DiscoveryOutcome, DiscoveryScope, FleetOverride, FleetOverrideField,
    FleetVisibility, SkillType,
};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

/// `node_type` of one discovery observation.
pub(super) const DISCOVERY_NODE_TYPE: &str = "ServerDiscovery";
/// `node_type` of one operator override.
pub(super) const OVERRIDE_NODE_TYPE: &str = "FleetOverride";
const RECORD_CONTENT_DOMAIN: &[u8] = b"eg/fleet-catalog-record/v1";
const REVISION_CONFLICT: &str = "FLEET_REVISION_CONFLICT";

/// What a probe observed, as stored. The observer is the VERIFIED principal's
/// opaque persistence id, stamped by the server, never a request field.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct DiscoveryBody {
    pub(super) server_name: String,
    pub(super) scope: DiscoveryScope,
    pub(super) connector: ResourceId,
    pub(super) outcome: DiscoveryOutcome,
    pub(super) counts: DiscoveryCounts,
    pub(super) observer: String,
}

/// One override, as stored. `value: None` is a cleared override: a tombstone
/// revision, so the compare-and-set chain never restarts from zero.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct OverrideBody {
    pub(super) component_id: String,
    pub(super) field: FleetOverrideField,
    pub(super) value: Option<FleetOverride>,
    pub(super) set_by: String,
}

/// The bookkeeping every record carries.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct RecordMeta {
    pub(super) tenant_id: String,
    pub(super) revision: u64,
    pub(super) content_digest: String,
    pub(super) written_at_ms: u64,
}

/// One decoded record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct StoredRecord<B> {
    pub(super) meta: RecordMeta,
    pub(super) body: B,
}

/// The on-node shape: bookkeeping at the top level (so a compare-and-set can
/// condition on `revision` and `tenant_id`), the typed body under `body`.
#[derive(Deserialize)]
struct StoredNode<B> {
    node_type: String,
    #[serde(flatten)]
    meta: RecordMeta,
    body: B,
}

fn tenant_key(tenant_id: &str) -> String {
    Digest256::sha256(tenant_id.as_bytes()).to_hex()
}

/// The graph node holding `(tenant, server, scope)`'s observation. The tenant
/// is digested into the id, so two tenants' records can never collide.
pub(super) fn discovery_node_id(
    tenant_id: &str,
    server_name: &str,
    scope: &DiscoveryScope,
) -> String {
    format!(
        "fleetobs:{}:{server_name}:{}",
        tenant_key(tenant_id),
        scope.key()
    )
}

/// The caller-facing id of an observation: stable across revisions, and
/// carrying nothing about the tenant.
pub(super) fn discovery_record_id(server_name: &str, scope: &DiscoveryScope) -> String {
    format!("srvobs:{server_name}:{}", scope.key())
}

pub(super) fn override_node_id(
    tenant_id: &str,
    field: FleetOverrideField,
    component_id: &str,
) -> String {
    format!(
        "fleetovr:{}:{}:{}",
        tenant_key(tenant_id),
        field.as_str(),
        Digest256::sha256(component_id.as_bytes()).to_hex()
    )
}

pub(super) fn override_record_id(field: FleetOverrideField, component_id: &str) -> String {
    format!("ovr:{}:{component_id}", field.as_str())
}

/// The digest a replay is recognized by: the tenant and the typed body,
/// never the revision or the clock.
pub(super) fn content_digest<B: Serialize>(tenant_id: &str, body: &B) -> Result<String, String> {
    let encoded = rmp_serde::to_vec_named(body)
        .map_err(|error| format!("fleet catalog record encoding failed: {error}"))?;
    Ok(Digest256::framed(
        RECORD_CONTENT_DOMAIN,
        &[tenant_id.as_bytes(), encoded.as_slice()],
    )?
    .to_hex())
}

/// Decode one node as a record of `node_type`, or `None` for anything else.
pub(super) fn decode_record<B: DeserializeOwned>(
    node_type: &str,
    properties_msgpack: &[u8],
) -> Option<StoredRecord<B>> {
    let value = eg_types::msgpack::decode_property_value(properties_msgpack).ok()?;
    let node = serde_json::from_value::<StoredNode<B>>(value).ok()?;
    (node.node_type == node_type).then_some(StoredRecord {
        meta: node.meta,
        body: node.body,
    })
}

/// Who wrote a record, for the graph's own row-level projection.
///
/// Fleet records live in the shared `__commons__` graph, so a generic graph read
/// there must not hand one tenant's observations to another. They are stamped
/// private to the writing agent; the fleet projection applies its own typed
/// tenant/principal predicate instead of the per-agent one.
pub(super) struct RecordWriter<'a> {
    pub(super) agent_id: &'a str,
    pub(super) written_at_ms: u64,
}

/// The complete property blob of one record revision.
pub(super) fn record_properties<B: Serialize>(
    node_type: &str,
    meta: &RecordMeta,
    body: &B,
    writer: &RecordWriter<'_>,
) -> Result<Vec<u8>, String> {
    let properties = serde_json::json!({
        "node_type": node_type,
        "tenant_id": meta.tenant_id,
        "revision": meta.revision,
        "content_digest": meta.content_digest,
        "written_at_ms": writer.written_at_ms,
        "body": body,
        "_owner": writer.agent_id,
        "_visibility": "private",
    });
    rmp_serde::to_vec_named(&properties)
        .map_err(|error| format!("fleet catalog record encoding failed: {error}"))
}

/// The compare-and-set precondition for replacing revision `from`.
pub(super) fn revision_condition(tenant_id: &str, from: u64) -> Result<Vec<u8>, String> {
    rmp_serde::to_vec_named(&serde_json::json!({
        "tenant_id": tenant_id,
        "revision": from,
    }))
    .map_err(|error| format!("fleet catalog condition encoding failed: {error}"))
}

/// What a write must do, decided from the stored record before it is sent.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum WritePlan {
    /// The stored record already says exactly this.
    Replay { revision: u64 },
    /// No record exists: create revision 1 only if it still does not.
    Create,
    /// Replace revision `from` only if it is still current.
    Update { from: u64 },
}

/// Decide a write. A byte-identical repeat is a replay whatever the caller
/// expected -- that is what makes a retried write provably a no-op -- and only
/// then is `expected_revision` (`0` meaning "absent") compared.
pub(super) fn plan_write(
    current: Option<&RecordMeta>,
    expected_revision: Option<u64>,
    content_digest: &str,
) -> Result<WritePlan, String> {
    if let Some(meta) = current.filter(|meta| meta.content_digest == content_digest) {
        return Ok(WritePlan::Replay {
            revision: meta.revision,
        });
    }
    let found = current.map_or(0, |meta| meta.revision);
    if let Some(expected) = expected_revision.filter(|expected| *expected != found) {
        return Err(format!(
            "{REVISION_CONFLICT}: expected revision {expected}, found {found}"
        ));
    }
    Ok(match current {
        None => WritePlan::Create,
        Some(meta) => WritePlan::Update {
            from: meta.revision,
        },
    })
}

/// The refusal a lost compare-and-set race answers with.
pub(super) fn lost_race(record_id: &str) -> String {
    format!("{REVISION_CONFLICT}: '{record_id}' changed concurrently; re-read and retry")
}

/// Who is reading, as the verified context says.
pub(super) struct Viewer<'a> {
    pub(super) tenant_id: &'a str,
    /// The caller's opaque persistence id.
    pub(super) principal: &'a str,
    /// The caller's CURRENT grants, as it presented them. Only narrows.
    pub(super) grants: &'a [Digest256],
}

impl Viewer<'_> {
    /// How an observation is visible to this viewer, or `None`.
    pub(super) fn sees(&self, record: &StoredRecord<DiscoveryBody>) -> Option<FleetVisibility> {
        if record.meta.tenant_id != self.tenant_id {
            return None;
        }
        match &record.body.scope {
            DiscoveryScope::TenantLocal => Some(FleetVisibility::Tenant),
            DiscoveryScope::OauthGrant { grant_digest } => (record.body.observer == self.principal
                && self.grants.contains(grant_digest))
            .then(|| FleetVisibility::Principal {
                principal: record.body.observer.clone(),
            }),
        }
    }
}

/// A live skill-type override: the value and the revision that set it.
pub(super) fn skill_type_override(record: &StoredRecord<OverrideBody>) -> Option<(SkillType, u64)> {
    record
        .body
        .value
        .map(|FleetOverride::SkillType { skill_type }| (skill_type, record.meta.revision))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn meta(revision: u64, digest: &str) -> RecordMeta {
        RecordMeta {
            tenant_id: "tenant-a".to_string(),
            revision,
            content_digest: digest.to_string(),
            written_at_ms: 1,
        }
    }

    fn observation(scope: DiscoveryScope, observer: &str) -> StoredRecord<DiscoveryBody> {
        StoredRecord {
            meta: meta(1, "d"),
            body: DiscoveryBody {
                server_name: "github".to_string(),
                scope,
                connector: ResourceId::new("github").unwrap(),
                outcome: DiscoveryOutcome::Reachable,
                counts: DiscoveryCounts::default(),
                observer: observer.to_string(),
            },
        }
    }

    #[test]
    fn a_repeat_is_a_replay_even_against_a_stale_expectation() {
        let stored = meta(4, "same");
        assert_eq!(
            plan_write(Some(&stored), Some(1), "same"),
            Ok(WritePlan::Replay { revision: 4 })
        );
    }

    #[test]
    fn expected_revision_fences_create_and_update() {
        assert_eq!(plan_write(None, None, "x"), Ok(WritePlan::Create));
        assert_eq!(plan_write(None, Some(0), "x"), Ok(WritePlan::Create));
        assert!(plan_write(None, Some(3), "x")
            .unwrap_err()
            .starts_with(REVISION_CONFLICT));
        let stored = meta(3, "old");
        assert_eq!(
            plan_write(Some(&stored), Some(3), "new"),
            Ok(WritePlan::Update { from: 3 })
        );
        assert_eq!(
            plan_write(Some(&stored), None, "new"),
            Ok(WritePlan::Update { from: 3 })
        );
        assert!(plan_write(Some(&stored), Some(0), "new").is_err());
    }

    #[test]
    fn records_round_trip_through_node_properties_and_refuse_lookalikes() {
        let record = observation(DiscoveryScope::TenantLocal, "principal:sha256:ab");
        let writer = RecordWriter {
            agent_id: "agent-a",
            written_at_ms: 9,
        };
        let blob =
            record_properties(DISCOVERY_NODE_TYPE, &record.meta, &record.body, &writer).unwrap();
        let decoded = decode_record::<DiscoveryBody>(DISCOVERY_NODE_TYPE, &blob).unwrap();
        assert_eq!(decoded.body, record.body);
        assert_eq!(decoded.meta.revision, 1);
        assert_eq!(decoded.meta.written_at_ms, 9);
        assert!(decode_record::<DiscoveryBody>(OVERRIDE_NODE_TYPE, &blob).is_none());
        assert!(decode_record::<OverrideBody>(OVERRIDE_NODE_TYPE, &blob).is_none());
    }

    #[test]
    fn content_digest_ignores_revision_and_binds_tenant() {
        let body = observation(DiscoveryScope::TenantLocal, "p").body;
        let a = content_digest("tenant-a", &body).unwrap();
        assert_eq!(a, content_digest("tenant-a", &body.clone()).unwrap());
        assert_ne!(a, content_digest("tenant-b", &body).unwrap());
    }

    #[test]
    fn node_ids_are_tenant_disjoint_and_record_ids_are_not_tenant_bearing() {
        let scope = DiscoveryScope::TenantLocal;
        assert_ne!(
            discovery_node_id("tenant-a", "github", &scope),
            discovery_node_id("tenant-b", "github", &scope)
        );
        assert_eq!(
            discovery_record_id("github", &scope),
            "srvobs:github:tenant_local"
        );
        assert_eq!(
            override_record_id(FleetOverrideField::SkillType, "mcp:s/skill/x"),
            "ovr:skill_type:mcp:s/skill/x"
        );
        assert_ne!(
            override_node_id("tenant-a", FleetOverrideField::SkillType, "mcp:s/skill/x"),
            override_node_id("tenant-b", FleetOverrideField::SkillType, "mcp:s/skill/x")
        );
    }

    fn viewer<'a>(tenant_id: &'a str, principal: &'a str, grants: &'a [Digest256]) -> Viewer<'a> {
        Viewer {
            tenant_id,
            principal,
            grants,
        }
    }

    #[test]
    fn grant_scoped_observations_are_visible_only_to_their_principal_and_current_grant() {
        let grant = Digest256::from_bytes([3; 32]);
        let record = observation(
            DiscoveryScope::OauthGrant {
                grant_digest: grant,
            },
            "alice",
        );
        let current = [grant];
        assert_eq!(
            viewer("tenant-a", "alice", &current).sees(&record),
            Some(FleetVisibility::Principal {
                principal: "alice".to_string()
            })
        );
        assert_eq!(viewer("tenant-a", "alice", &[]).sees(&record), None);
        assert_eq!(viewer("tenant-a", "bob", &current).sees(&record), None);
        assert_eq!(viewer("tenant-b", "alice", &current).sees(&record), None);
        let local = observation(DiscoveryScope::TenantLocal, "alice");
        assert_eq!(
            viewer("tenant-a", "bob", &[]).sees(&local),
            Some(FleetVisibility::Tenant)
        );
        assert_eq!(viewer("tenant-b", "bob", &[]).sees(&local), None);
    }
}
