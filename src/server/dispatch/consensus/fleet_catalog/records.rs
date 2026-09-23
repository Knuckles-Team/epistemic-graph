//! The fleet catalog's two typed record bodies and how they map onto rows of
//! the tenant-scoped Agent Library owner (`persistence::fleet_records`).
//!
//! Pure: record identity, body encoding/decoding, the content digest a replay
//! is recognized by, and the one visibility predicate. The store owns
//! revisions, the compare-and-set and tenant scoping.

use eg_types::contract::{Digest256, ResourceId};
use eg_types::fleet_catalog::{
    DiscoveryCounts, DiscoveryOutcome, DiscoveryScope, FleetOverride, FleetOverrideField,
    FleetVisibility, SkillType,
};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use crate::server::persistence::fleet_records::FleetRecordRow;

const RECORD_CONTENT_DOMAIN: &[u8] = b"eg/fleet-catalog-record/v1";

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

/// The bookkeeping a decoded record carries.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct RecordMeta {
    pub(super) tenant_id: String,
    pub(super) revision: u64,
    pub(super) written_at_ms: u64,
}

/// One decoded record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct StoredRecord<B> {
    pub(super) meta: RecordMeta,
    pub(super) body: B,
}

/// The caller-facing id of an observation: stable across revisions, and the
/// row id within its tenant.
pub(super) fn discovery_record_id(server_name: &str, scope: &DiscoveryScope) -> String {
    format!("srvobs:{server_name}:{}", scope.key())
}

pub(super) fn override_record_id(field: FleetOverrideField, component_id: &str) -> String {
    format!("ovr:{}:{component_id}", field.as_str())
}

/// The digest a replay is recognized by: the tenant and the typed body,
/// never the revision or the clock.
pub(super) fn content_digest<B: Serialize>(tenant_id: &str, body: &B) -> Result<String, String> {
    let encoded = encode_body(body)?;
    Ok(Digest256::framed(
        RECORD_CONTENT_DOMAIN,
        &[tenant_id.as_bytes(), encoded.as_slice()],
    )?
    .to_hex())
}

pub(super) fn encode_body<B: Serialize>(body: &B) -> Result<Vec<u8>, String> {
    rmp_serde::to_vec_named(body)
        .map_err(|error| format!("fleet catalog record encoding failed: {error}"))
}

/// Decode one stored row as a record of `B`, or `None` for a row that is not.
pub(super) fn decode_record<B: DeserializeOwned>(
    tenant_id: &str,
    row: &FleetRecordRow,
) -> Option<StoredRecord<B>> {
    let body = rmp_serde::from_slice::<B>(&row.body).ok()?;
    Some(StoredRecord {
        meta: RecordMeta {
            tenant_id: tenant_id.to_string(),
            revision: row.revision,
            written_at_ms: row.written_at_ms,
        },
        body,
    })
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
    ///
    /// The tenant half is the store's own scoping (rows are read under the
    /// viewer's tenant key); it is re-checked here so a decoded record can
    /// never be shown to another tenant whatever reached this function. The
    /// principal half is the one visibility decision the store cannot make: a
    /// grant-scoped observation is private to the principal who made it.
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
    match record.body.value {
        Some(FleetOverride::SkillType { skill_type }) => Some((skill_type, record.meta.revision)),
        None => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn observation(scope: DiscoveryScope, observer: &str) -> StoredRecord<DiscoveryBody> {
        StoredRecord {
            meta: RecordMeta {
                tenant_id: "tenant-a".to_string(),
                revision: 1,
                written_at_ms: 1,
            },
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

    fn viewer<'a>(tenant_id: &'a str, principal: &'a str, grants: &'a [Digest256]) -> Viewer<'a> {
        Viewer {
            tenant_id,
            principal,
            grants,
        }
    }

    #[test]
    fn records_round_trip_through_store_rows_and_refuse_other_shapes() {
        let record = observation(DiscoveryScope::TenantLocal, "principal:sha256:ab");
        let row = FleetRecordRow {
            revision: 4,
            content_digest: content_digest("tenant-a", &record.body).unwrap(),
            written_at_ms: 9,
            writer: "principal:sha256:ab".to_string(),
            body: encode_body(&record.body).unwrap(),
        };
        let decoded = decode_record::<DiscoveryBody>("tenant-a", &row).unwrap();
        assert_eq!(decoded.body, record.body);
        assert_eq!((decoded.meta.revision, decoded.meta.written_at_ms), (4, 9));
        assert!(decode_record::<OverrideBody>("tenant-a", &row).is_none());
    }

    #[test]
    fn content_digest_binds_tenant_and_body_only() {
        let body = observation(DiscoveryScope::TenantLocal, "p").body;
        let a = content_digest("tenant-a", &body).unwrap();
        assert_eq!(a, content_digest("tenant-a", &body.clone()).unwrap());
        assert_ne!(a, content_digest("tenant-b", &body).unwrap());
    }

    #[test]
    fn record_ids_are_stable_and_carry_no_tenant() {
        assert_eq!(
            discovery_record_id("github", &DiscoveryScope::TenantLocal),
            "srvobs:github:tenant_local"
        );
        assert_eq!(
            override_record_id(FleetOverrideField::SkillType, "mcp:s/skill/x"),
            "ovr:skill_type:mcp:s/skill/x"
        );
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
