//! Fleet catalog records (EH-345) in the tenant-scoped Agent Library owner.
//!
//! Discovery observations and operator overrides are tenant-private facts, so
//! they live in the owner that already serves each tenant through its own
//! scope -- beside the connector-pack components they describe -- not in the
//! shared `__commons__` graph. Every write is one admitted maintenance batch in
//! the writing tenant's scope; the compare-and-set on the record's revision is
//! decided INSIDE that write transaction, so it is linearizable by construction.
//!
//! Rows are keyed `(tenant, family, record id)`. The body is the fleet handler's
//! typed record, stored as opaque MessagePack: this module owns revisions,
//! replay and scoping, never the record's meaning.

use eg_storage::{AgentLibraryOwner, FLEET_CATALOG_RECORDS};
use eg_transaction::AdmittedOwnerWrite;
use redb::ReadableTable;
use serde::{Deserialize, Serialize};

use super::agent_library::AgentLibraryStore;

const REVISION_CONFLICT: &str = "FLEET_REVISION_CONFLICT";

/// Which kind of record a row is: the key's middle component.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FleetRecordFamily {
    Discovery,
    Override,
}

impl FleetRecordFamily {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Discovery => "discovery",
            Self::Override => "override",
        }
    }
}

/// One stored record revision.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FleetRecordRow {
    pub revision: u64,
    /// Digest of the tenant and typed body: what a replay is recognized by.
    pub content_digest: String,
    pub written_at_ms: u64,
    /// The verified writer's opaque persistence id.
    pub writer: String,
    #[serde(with = "serde_bytes")]
    pub body: Vec<u8>,
}

/// One record write, fully decided except for the stored state it races.
pub struct FleetRecordWrite {
    pub tenant_id: String,
    pub family: FleetRecordFamily,
    pub record_id: String,
    pub content_digest: String,
    pub body: Vec<u8>,
    pub writer: String,
    pub written_at_ms: u64,
    /// `None`: against whatever is stored. `Some(0)`: only if absent.
    /// `Some(n)`: only if the stored revision is exactly `n`.
    pub expected_revision: Option<u64>,
}

/// What a write did, and the record's revision afterwards.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FleetRecordOutcome {
    Written { revision: u64 },
    Replayed { revision: u64 },
}

/// Decide a write against the stored row. A byte-identical repeat is a replay
/// whatever the caller expected -- that is what makes a retried write provably
/// a no-op -- and only then is `expected_revision` (`0` = absent) compared.
pub fn decide_fleet_write(
    current: Option<&FleetRecordRow>,
    expected_revision: Option<u64>,
    content_digest: &str,
) -> Result<FleetRecordOutcome, String> {
    let found = current.map_or(0, |row| row.revision);
    if current.is_some_and(|row| row.content_digest == content_digest) {
        return Ok(FleetRecordOutcome::Replayed { revision: found });
    }
    if let Some(expected) = expected_revision.filter(|expected| *expected != found) {
        return Err(format!(
            "{REVISION_CONFLICT}: expected revision {expected}, found {found}"
        ));
    }
    let revision = found
        .checked_add(1)
        .ok_or_else(|| format!("{REVISION_CONFLICT}: revision overflow"))?;
    Ok(FleetRecordOutcome::Written { revision })
}

fn decode_row(bytes: &[u8]) -> Result<FleetRecordRow, String> {
    rmp_serde::from_slice(bytes)
        .map_err(|error| format!("fleet catalog record is corrupt: {error}"))
}

/// Read, decide and (when it is not a replay) write one record inside the
/// tenant's admitted owner write.
fn apply_fleet_write(
    owner: &AdmittedOwnerWrite<'_, AgentLibraryOwner>,
    write: &FleetRecordWrite,
) -> Result<FleetRecordOutcome, String> {
    let key = (
        write.tenant_id.as_str(),
        write.family.as_str(),
        write.record_id.as_str(),
    );
    let mut table = owner.open_table(FLEET_CATALOG_RECORDS)?;
    let current = table
        .get(key)
        .map_err(|error| error.to_string())?
        .map(|row| decode_row(row.value()))
        .transpose()?;
    let outcome = decide_fleet_write(
        current.as_ref(),
        write.expected_revision,
        &write.content_digest,
    )?;
    if let FleetRecordOutcome::Written { revision } = outcome {
        let row = FleetRecordRow {
            revision,
            content_digest: write.content_digest.clone(),
            written_at_ms: write.written_at_ms,
            writer: write.writer.clone(),
            body: write.body.clone(),
        };
        let bytes = rmp_serde::to_vec_named(&row).map_err(|error| error.to_string())?;
        table
            .insert(key, bytes.as_slice())
            .map_err(|error| error.to_string())?;
    }
    Ok(outcome)
}

impl AgentLibraryStore {
    /// Commit one fleet record revision in its tenant's own scope.
    pub fn put_fleet_record(&self, write: &FleetRecordWrite) -> Result<FleetRecordOutcome, String> {
        // A replay needs no write at all: answer it from a read snapshot. The
        // write below re-decides under the owner's writer, so a race between
        // this read and that write can only turn into a conflict, never a
        // silent overwrite.
        let stored = self.fleet_record(&write.tenant_id, write.family, &write.record_id)?;
        if let Ok(replayed @ FleetRecordOutcome::Replayed { .. }) = decide_fleet_write(
            stored.as_ref(),
            write.expected_revision,
            &write.content_digest,
        ) {
            return Ok(replayed);
        }
        let mut outcome = None;
        self.maintain_write_back(&write.tenant_id, "fleet_catalog_record", |owner| {
            outcome = Some(apply_fleet_write(owner, write)?);
            Ok(())
        })?;
        outcome.ok_or_else(|| "fleet catalog write committed no outcome".to_string())
    }

    /// One record, or `None`.
    pub fn fleet_record(
        &self,
        tenant_id: &str,
        family: FleetRecordFamily,
        record_id: &str,
    ) -> Result<Option<FleetRecordRow>, String> {
        let read = self.read()?;
        let table = read.open_owner_table(FLEET_CATALOG_RECORDS)?;
        table
            .get((tenant_id, family.as_str(), record_id))
            .map_err(|error| error.to_string())?
            .map(|row| decode_row(row.value()))
            .transpose()
    }

    /// Every record of one family in one tenant, in id order. Refuses past
    /// `max_rows` by name rather than answering a truncated set.
    pub fn fleet_records(
        &self,
        tenant_id: &str,
        family: FleetRecordFamily,
        max_rows: usize,
    ) -> Result<Vec<(String, FleetRecordRow)>, String> {
        let read = self.read()?;
        let table = read.open_owner_table(FLEET_CATALOG_RECORDS)?;
        let mut rows = Vec::new();
        for row in table
            .range((tenant_id, family.as_str(), "")..)
            .map_err(|error| error.to_string())?
        {
            let (key, value) = row.map_err(|error| error.to_string())?;
            let (row_tenant, row_family, record_id) = key.value();
            if row_tenant != tenant_id || row_family != family.as_str() {
                break;
            }
            if rows.len() == max_rows {
                return Err(format!(
                    "FLEET_SNAPSHOT_TOO_LARGE: more than {max_rows} {} records",
                    family.as_str()
                ));
            }
            rows.push((record_id.to_string(), decode_row(value.value())?));
        }
        Ok(rows)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn store() -> (tempfile::TempDir, AgentLibraryStore) {
        let dir = tempfile::tempdir().unwrap();
        let store = AgentLibraryStore::open(dir.path().to_str().unwrap()).unwrap();
        (dir, store)
    }

    fn write(tenant: &str, digest: &str, expected_revision: Option<u64>) -> FleetRecordWrite {
        FleetRecordWrite {
            tenant_id: tenant.to_string(),
            family: FleetRecordFamily::Discovery,
            record_id: "srvobs:github:tenant_local".to_string(),
            content_digest: digest.to_string(),
            body: vec![1, 2, 3],
            writer: "principal:sha256:ab".to_string(),
            written_at_ms: 7,
            expected_revision,
        }
    }

    #[test]
    fn writes_are_revisioned_replayed_and_fenced_in_the_owner() {
        let (_dir, store) = store();
        assert_eq!(
            store.put_fleet_record(&write("tenant-a", "one", Some(0))),
            Ok(FleetRecordOutcome::Written { revision: 1 })
        );
        assert_eq!(
            store.put_fleet_record(&write("tenant-a", "one", Some(0))),
            Ok(FleetRecordOutcome::Replayed { revision: 1 }),
            "a byte-identical repeat is a replay even against a stale expectation"
        );
        assert!(store
            .put_fleet_record(&write("tenant-a", "two", Some(0)))
            .unwrap_err()
            .starts_with(REVISION_CONFLICT));
        assert_eq!(
            store.put_fleet_record(&write("tenant-a", "two", Some(1))),
            Ok(FleetRecordOutcome::Written { revision: 2 })
        );
        assert_eq!(
            store.put_fleet_record(&write("tenant-a", "three", None)),
            Ok(FleetRecordOutcome::Written { revision: 3 })
        );
        let stored = store
            .fleet_record(
                "tenant-a",
                FleetRecordFamily::Discovery,
                "srvobs:github:tenant_local",
            )
            .unwrap()
            .unwrap();
        assert_eq!(
            (stored.revision, stored.content_digest.as_str()),
            (3, "three")
        );
    }

    #[test]
    fn tenants_and_families_never_see_each_others_rows() {
        let (_dir, store) = store();
        store
            .put_fleet_record(&write("tenant-a", "a", None))
            .unwrap();
        store
            .put_fleet_record(&write("tenant-b", "b", None))
            .unwrap();
        let a = store
            .fleet_records("tenant-a", FleetRecordFamily::Discovery, 16)
            .unwrap();
        assert_eq!(a.len(), 1);
        assert_eq!(a[0].1.content_digest, "a");
        assert!(store
            .fleet_records("tenant-a", FleetRecordFamily::Override, 16)
            .unwrap()
            .is_empty());
        assert!(store
            .fleet_records("tenant-c", FleetRecordFamily::Discovery, 16)
            .unwrap()
            .is_empty());
    }

    #[test]
    fn the_listing_bound_refuses_rather_than_truncates() {
        let (_dir, store) = store();
        for server in ["alpha", "bravo"] {
            let mut next = write("tenant-a", server, None);
            next.record_id = format!("srvobs:{server}:tenant_local");
            store.put_fleet_record(&next).unwrap();
        }
        assert!(store
            .fleet_records("tenant-a", FleetRecordFamily::Discovery, 1)
            .unwrap_err()
            .starts_with("FLEET_SNAPSHOT_TOO_LARGE"));
    }
}
