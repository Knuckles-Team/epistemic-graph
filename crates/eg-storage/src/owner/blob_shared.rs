use super::*;
use redb::ReadTransaction;

/// Independent authority for the two physical shared-service CAS tables.
pub trait BlobSharedServiceVerifier: Send + Sync {
    fn verify(
        &self,
        physical: &PhysicalStoreIdentity,
        principal: &str,
        proof: &[u8],
    ) -> Result<(), String>;
}

pub struct BlobSharedServiceHandle {
    principal: String,
    authority_digest: [u8; 32],
}

pub trait BlobSharedTable: sealed::Sealed {
    const TABLE_ID: &'static str;

    /// Shared physical tables are only reachable through independently
    /// authenticated blob-service operations, never an owner row adapter.
    fn access_class() -> OwnerTableAccess {
        OwnerTableAccess::SharedService
    }
}

mod sealed {
    pub trait Sealed {}
}

pub struct CasChunkRows;
pub struct CasRefcountRows;
impl sealed::Sealed for CasChunkRows {}
impl sealed::Sealed for CasRefcountRows {}
impl BlobSharedTable for CasChunkRows {
    const TABLE_ID: &'static str = "cas_chunks";
}
impl BlobSharedTable for CasRefcountRows {
    const TABLE_ID: &'static str = "cas_refcount";
}

pub struct BlobSharedRead {
    transaction: ReadTransaction,
}

impl MutationStore {
    pub fn authenticate_blob_shared_service(
        &self,
        verifier: &dyn BlobSharedServiceVerifier,
        principal: String,
        proof: &[u8],
    ) -> Result<BlobSharedServiceHandle, String> {
        let manifest = self.current_owner_manifest()?;
        if manifest.layout != OwnerLayout::Blob {
            return Err("shared blob authority requires the blob owner layout".to_string());
        }
        verifier.verify(&manifest.physical_identity, &principal, proof)?;
        Ok(BlobSharedServiceHandle {
            principal,
            authority_digest: manifest.authority_digest(self.incarnation()),
        })
    }

    pub fn read_blob_shared(
        &self,
        owner: &BlobSharedServiceHandle,
        principal: &str,
    ) -> Result<BlobSharedRead, String> {
        let transaction = self
            .database
            .begin_read()
            .map_err(|error| error.to_string())?;
        self.validate_physical_root()?;
        crate::identity::validate_incarnation_read(&transaction, self.incarnation())?;
        let cached = self.strict_manifest()?;
        let manifest =
            validate_manifest_read(&transaction, &cached.physical_identity, cached.layout)?;
        validate_declared_owner_tables(&transaction, manifest.layout)?;
        if manifest != *cached
            || manifest.layout != OwnerLayout::Blob
            || manifest.authority_digest(self.incarnation()) != owner.authority_digest
            || owner.principal != principal
        {
            return Err(
                "shared blob read authority does not match this store or actor".to_string(),
            );
        }
        Ok(BlobSharedRead { transaction })
    }
}

impl BlobSharedRead {
    pub fn table_rows<T: BlobSharedTable>(&self) -> Result<u64, String> {
        strict_snapshot_read(&self.transaction, OwnerLayout::Blob)?
            .tables
            .into_iter()
            .find(|table| table.table_id == T::TABLE_ID)
            .map(|table| table.rows)
            .ok_or_else(|| "shared blob table is outside the closed manifest".to_string())
    }
}
