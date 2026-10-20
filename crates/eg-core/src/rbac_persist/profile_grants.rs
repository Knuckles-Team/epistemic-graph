//! RbacOwner record reads and permanent revocation; no issuance authority.
use super::{RbacPersistError, RbacStore};
use crate::rbac::profile_grants::{ProfileGrantKey, ProfileUseGrant};
use redb::{ReadableTable, TableDefinition};
use sha2::{Digest, Sha256};

pub(super) const PROFILE_USE_GRANTS: TableDefinition<&str, &[u8]> =
    TableDefinition::new("profile_use_grants");

/// One MVCC record/revision pair, not admission proof.
pub struct ProfileGrantSnapshot {
    pub owner_revision: u64,
    pub record: Option<ProfileUseGrant>,
}

impl RbacStore {
    pub fn load_profile_use_grant(
        &self,
        key: &ProfileGrantKey,
    ) -> Result<ProfileGrantSnapshot, RbacPersistError> {
        let encoded_key = key.encode().map_err(RbacPersistError::Redb)?;
        let read = self.scoped_read()?;
        let owner_revision = eg_transaction::version(&read).map_err(RbacPersistError::Redb)?;
        let table = read
            .open_owner_table(PROFILE_USE_GRANTS)
            .map_err(RbacPersistError::Redb)?;
        let record = table
            .get(encoded_key.as_str())
            .map_err(|e| RbacPersistError::Redb(e.to_string()))?
            .map(|value| {
                ProfileUseGrant::decode(key, value.value()).map_err(RbacPersistError::Redb)
            })
            .transpose()?;
        Ok(ProfileGrantSnapshot {
            owner_revision,
            record,
        })
    }

    /// No create/update endpoint: the authorized owner caller can only revoke the
    /// exact preimage. The stored time is owner observation before admission,
    /// not the separate coordinator's durable decision timestamp.
    pub(crate) fn revoke_profile_use_grant(
        &self,
        expected_owner_revision: u64,
        expected: &ProfileUseGrant,
    ) -> Result<u64, RbacPersistError> {
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|_| RbacPersistError::IncompleteState("profile grant clock unavailable"))?
            .as_millis()
            .try_into()
            .map_err(|_| RbacPersistError::IncompleteState("profile grant clock overflow"))?;
        let next = expected
            .revoke(expected.revision, now_ms)
            .map_err(RbacPersistError::Redb)?;
        let key = expected.key.encode().map_err(RbacPersistError::Redb)?;
        let bytes = next.encode().map_err(RbacPersistError::Redb)?;
        let mut digest = Sha256::new();
        digest.update(b"eg/rbac-profile-grant-revoke/v1\0");
        digest.update(serde_json::to_vec(&(
            expected_owner_revision,
            &key,
            expected,
            &next,
        ))?);
        let digest = hex::encode(digest.finalize());
        super::durable_write::commit_authority_mutation(
            self,
            expected_owner_revision,
            &digest,
            None,
            |write| {
                let mut table = write
                    .open_table(PROFILE_USE_GRANTS)
                    .map_err(RbacPersistError::Redb)?;
                {
                    let row = table
                        .get(key.as_str())
                        .map_err(|e| RbacPersistError::Redb(e.to_string()))?
                        .ok_or(RbacPersistError::IncompleteState("profile grant missing"))?;
                    let actual = ProfileUseGrant::decode(&expected.key, row.value())
                        .map_err(RbacPersistError::Redb)?;
                    if actual != *expected {
                        return Err(RbacPersistError::IncompleteState(
                            "profile grant preimage mismatch",
                        ));
                    }
                }
                table
                    .insert(key.as_str(), bytes.as_slice())
                    .map_err(|e| RbacPersistError::Redb(e.to_string()))?;
                Ok(())
            },
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rbac::profile_grants::tests::record;

    fn directory() -> std::path::PathBuf {
        static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        std::env::temp_dir().join(format!(
            "eg-profile-grants-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        ))
    }

    // Fixture setup uses the ordinary owner transaction, never a production issuer.
    fn seed(store: &RbacStore, grant: &ProfileUseGrant) {
        let key = grant.key.encode().unwrap();
        let bytes = grant.encode().unwrap();
        let digest = hex::encode(Sha256::digest(&bytes));
        super::super::durable_write::commit_authority_mutation(
            store,
            store.current_version().unwrap(),
            &digest,
            None,
            |write| {
                let mut table = write
                    .open_table(PROFILE_USE_GRANTS)
                    .map_err(RbacPersistError::Redb)?;
                table
                    .insert(key.as_str(), bytes.as_slice())
                    .map_err(|e| RbacPersistError::Redb(e.to_string()))?;
                Ok(())
            },
        )
        .unwrap();
    }

    #[test]
    fn revoke_survives_reopen_and_preserves_original_provenance() {
        let directory = directory();
        let original = record();
        let revision;
        {
            let store = super::super::test_support::open_test_store(&directory).unwrap();
            assert!(store
                .load_profile_use_grant(&original.key)
                .unwrap()
                .record
                .is_none());
            seed(&store, &original);
            let snapshot = store.load_profile_use_grant(&original.key).unwrap();
            revision = store
                .revoke_profile_use_grant(snapshot.owner_revision, &original)
                .unwrap();
            assert!(store
                .revoke_profile_use_grant(snapshot.owner_revision, &original)
                .is_err());
        }
        let store = super::super::test_support::open_test_store(&directory).unwrap();
        let snapshot = store.load_profile_use_grant(&original.key).unwrap();
        assert_eq!(snapshot.owner_revision, revision);
        let revoked = snapshot.record.unwrap();
        assert!(revoked.revocation.is_some());
        assert_eq!(revoked.approval, original.approval);
        assert_eq!(revoked.binding, original.binding);
        assert_eq!(revoked.expires_at_ms, original.expires_at_ms);
        assert!(store.revoke_profile_use_grant(revision, &revoked).is_err());
        drop(store);
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn changed_preimage_refuses_without_advancing_owner_revision() {
        let directory = directory();
        let store = super::super::test_support::open_test_store(&directory).unwrap();
        let original = record();
        seed(&store, &original);
        let snapshot = store.load_profile_use_grant(&original.key).unwrap();
        let mut changed = original.clone();
        changed.binding.graph = "foreign".into();
        assert!(store
            .revoke_profile_use_grant(snapshot.owner_revision, &changed)
            .is_err());
        assert_eq!(store.current_version().unwrap(), snapshot.owner_revision);
        assert_eq!(
            store.load_profile_use_grant(&original.key).unwrap().record,
            Some(original)
        );
        drop(store);
        std::fs::remove_dir_all(directory).unwrap();
    }
}
