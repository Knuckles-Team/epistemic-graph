//! `StorageKernelV1` — the sole physical-state authority.

use crate::capability::{PhysicalWriteCapability, ScopedRead, ScopedSnapshot};
use crate::owner::blob_shared::{
    BlobSharedRead, BlobSharedServiceHandle, BlobSharedServiceVerifier,
};
use crate::owner::domain::OwnerDomain;
use crate::owner::grant::{AuthenticatedScopeGrant, ScopeGrantVerifier};
use crate::owner::handle::OwnedStoreHandle;
use crate::owner::identity::PhysicalStoreIdentity;
use crate::owner::layout::OwnerLayout;
use crate::owner::{
    open_declared_owner_tables, validate_declared_owner_tables, validate_manifest_read,
    write_new_manifest,
};
use crate::physical::binding::bind_scope_in;
use crate::physical::incarnation::StoreIncarnation;
use crate::physical::integrity::{authenticate_with, PrivatePayloadIntegrity};
use crate::physical::manifest::{OwnerManifest, OwnerManifestDigest};
use crate::physical::root::{initialize_strict_in, store_handle, validate_incarnation_read, PhysicalStore};
use crate::recovery::validate::validate_recovery_content;
use eg_types::MutationScopeIdentity;
use redb::{Database, ReadableDatabase};
use std::path::Path;
use std::sync::Arc;

/// The sole physical owner of one durable store file.
///
/// It alone opens and identifies the file, owns its complete table/domain
/// registry, and issues scoped read, snapshot and write capabilities. Domain
/// crates hold only the capabilities it issues.
pub struct StorageKernelV1 {
    store: Arc<PhysicalStore>,
    mutation_authority: Option<MutationOwnerAuthority>,
}

/// Move-once physical write authority.
///
/// It is not `Clone`, not `Default`, not serializable, and has no public
/// constructor. [`StorageKernelV1::into_read_and_mutation_authority`] yields
/// the single instance for one kernel, so a domain crate can never name one and
/// therefore can never obtain a write capability.
///
/// ```compile_fail
/// # use eg_storage::MutationOwnerAuthority;
/// fn forge() -> MutationOwnerAuthority {
///     MutationOwnerAuthority {}
/// }
/// ```
pub struct MutationOwnerAuthority {
    store: Arc<PhysicalStore>,
}

impl StorageKernelV1 {
    /// Create a current-format owner file. No serving scope is created here.
    pub fn create_owner<D: OwnerDomain>(
        path: &Path,
        physical_identity: PhysicalStoreIdentity,
        private_integrity: Option<Arc<dyn PrivatePayloadIntegrity>>,
    ) -> Result<Self, String> {
        create_physical(path, physical_identity, private_integrity, D::LAYOUT).map(Self::from_store)
    }

    /// Open only an exact current-format owner file; absent or invalid
    /// declarations fail closed.
    pub fn open_owner<D: OwnerDomain>(
        path: &Path,
        physical_identity: PhysicalStoreIdentity,
        private_integrity: Option<Arc<dyn PrivatePayloadIntegrity>>,
    ) -> Result<Self, String> {
        open_physical(path, physical_identity, private_integrity, D::LAYOUT).map(Self::from_store)
    }

    pub(crate) fn from_store(store: PhysicalStore) -> Self {
        let store = Arc::new(store);
        Self {
            mutation_authority: Some(MutationOwnerAuthority {
                store: Arc::clone(&store),
            }),
            store,
        }
    }

    /// Split this kernel into its read/snapshot half and the one mutation
    /// authority token. Callable exactly once: the token is moved out.
    pub fn into_read_and_mutation_authority(
        mut self,
    ) -> Result<(Self, MutationOwnerAuthority), String> {
        let authority = self
            .mutation_authority
            .take()
            .ok_or_else(|| "mutation owner authority was already issued".to_string())?;
        Ok((self, authority))
    }

    pub fn incarnation(&self) -> &StoreIncarnation {
        self.store.incarnation()
    }

    pub fn layout(&self) -> OwnerLayout {
        self.store.manifest().layout
    }

    /// Authenticate one exact logical serving identity against this store's
    /// declared owner authority.
    pub fn authenticate_scope<D: OwnerDomain>(
        &self,
        verifier: &dyn ScopeGrantVerifier,
        identity: MutationScopeIdentity,
        principal: String,
        proof: &[u8],
    ) -> Result<AuthenticatedScopeGrant<D>, String> {
        authenticate_scope_in(&self.store, verifier, identity, principal, proof)
    }

    /// Bind an authenticated grant once, yielding the owner handle every scoped
    /// capability is issued against.
    pub fn bind_serving_scope<D: OwnerDomain>(
        &self,
        grant: AuthenticatedScopeGrant<D>,
        initial_version: u64,
    ) -> Result<OwnedStoreHandle<D>, String> {
        bind_serving_scope_in(&self.store, grant, initial_version)
    }

    /// Issue one scoped read over this owner file.
    pub fn read_scope<D: OwnerDomain>(
        &self,
        owner: &OwnedStoreHandle<D>,
    ) -> Result<ScopedRead<'_, D>, String> {
        ScopedRead::open(&self.store, owner)
    }

    /// Capture one complete owner-scoped physical snapshot.
    pub fn snapshot<D: OwnerDomain>(
        &self,
        owner: &OwnedStoreHandle<D>,
    ) -> Result<ScopedSnapshot<D>, String> {
        ScopedSnapshot::capture(&self.store, owner)
    }

    pub fn owner_manifest_digest(&self) -> Result<OwnerManifestDigest, String> {
        self.current_owner_manifest()?.digest()
    }

    /// Digest of the current persisted owner manifest anchored to this exact
    /// store incarnation. Unlike the manifest digest, this changes when an
    /// otherwise identical store image is adopted at a new inode.
    pub fn owner_authority_digest(&self) -> Result<[u8; 32], String> {
        Ok(self
            .current_owner_manifest()?
            .authority_digest(self.incarnation()))
    }

    pub(crate) fn store(&self) -> &PhysicalStore {
        &self.store
    }

    /// Authenticate the independent shared-service authority over the two
    /// physical CAS tables.
    pub fn authenticate_blob_shared_service(
        &self,
        verifier: &dyn BlobSharedServiceVerifier,
        principal: String,
        proof: &[u8],
    ) -> Result<BlobSharedServiceHandle, String> {
        crate::owner::blob_shared::authenticate_blob_shared_service(
            &self.store,
            verifier,
            principal,
            proof,
        )
    }

    pub fn read_blob_shared(
        &self,
        owner: &BlobSharedServiceHandle,
        principal: &str,
    ) -> Result<BlobSharedRead, String> {
        crate::owner::blob_shared::read_blob_shared(&self.store, owner, principal)
    }

    pub(crate) fn current_owner_manifest(&self) -> Result<OwnerManifest, String> {
        current_owner_manifest(&self.store)
    }
}

impl MutationOwnerAuthority {
    /// Mint the one physical write capability for one bound serving scope.
    pub fn write_capability<D: OwnerDomain>(
        &self,
        owner: &OwnedStoreHandle<D>,
    ) -> Result<PhysicalWriteCapability<'_, D>, String> {
        PhysicalWriteCapability::open(&self.store, owner)
    }

    pub fn incarnation(&self) -> &StoreIncarnation {
        self.store.incarnation()
    }

    /// Digest of the persisted owner manifest anchored to this incarnation.
    pub fn owner_authority_digest(&self) -> Result<[u8; 32], String> {
        Ok(current_owner_manifest(&self.store)?.authority_digest(self.store.incarnation()))
    }
}

/// Authenticate one exact logical serving identity against a store's declared
/// owner authority.
pub(crate) fn authenticate_scope_in<D: OwnerDomain>(
    store: &PhysicalStore,
    verifier: &dyn ScopeGrantVerifier,
    identity: MutationScopeIdentity,
    principal: String,
    proof: &[u8],
) -> Result<AuthenticatedScopeGrant<D>, String> {
    identity.validate_digest()?;
    let manifest = current_owner_manifest(store)?;
    if manifest.layout != D::LAYOUT || !manifest.layout.accepts(&identity) {
        return Err("serving scope is outside the declared owner layout".to_string());
    }
    verifier.verify(
        &manifest.physical_identity,
        manifest.layout,
        &identity,
        &principal,
        proof,
    )?;
    let authority_digest = manifest.authority_digest(store.incarnation());
    Ok(AuthenticatedScopeGrant::new(
        identity,
        principal,
        authority_digest,
    ))
}

/// Bind one authenticated grant once, yielding its owner handle.
pub(crate) fn bind_serving_scope_in<D: OwnerDomain>(
    store: &PhysicalStore,
    grant: AuthenticatedScopeGrant<D>,
    initial_version: u64,
) -> Result<OwnedStoreHandle<D>, String> {
    let manifest = current_owner_manifest(store)?;
    if manifest.layout != D::LAYOUT
        || manifest.authority_digest(store.incarnation()) != *grant.authority_digest()
        || !manifest.layout.accepts(grant.identity())
    {
        return Err("authenticated serving grant does not match this store".to_string());
    }
    let transaction = store.begin_write()?;
    bind_scope_in(
        &store.handle,
        &transaction,
        grant.identity(),
        initial_version,
    )?;
    transaction.commit().map_err(|error| error.to_string())?;
    let (identity, principal, authority_digest) = grant.into_parts();
    Ok(OwnedStoreHandle::new(identity, principal, authority_digest))
}

pub(crate) fn current_owner_manifest(store: &PhysicalStore) -> Result<OwnerManifest, String> {
    store.validate_physical_root()?;
    let cached = store.manifest();
    let transaction = store.begin_read()?;
    let persisted = validate_manifest_read(&transaction, &cached.physical_identity, cached.layout)?;
    validate_declared_owner_tables(&transaction, persisted.layout)?;
    if persisted != *cached {
        return Err("cached owner manifest differs from persisted authority".to_string());
    }
    Ok(persisted)
}

/// Create one current-format physical owner file at `path`.
pub(crate) fn create_physical(
    path: &Path,
    physical_identity: PhysicalStoreIdentity,
    private_integrity: Option<Arc<dyn PrivatePayloadIntegrity>>,
    layout: OwnerLayout,
) -> Result<PhysicalStore, String> {
    if path.exists() {
        return Err("mutation store create target already exists".to_string());
    }
    let database = Database::create(path).map_err(|error| error.to_string())?;
    let (incarnation, physical_path) = StoreIncarnation::derive(path)?;
    let manifest = OwnerManifest::new(physical_identity, layout)?;
    let mut wtx = database.begin_write().map_err(|error| error.to_string())?;
    wtx.set_durability(redb::Durability::Immediate)
        .map_err(|error| error.to_string())?;
    let handle = initialize_strict_in(&wtx, &incarnation, &manifest)?;
    open_declared_owner_tables(&wtx, layout)?;
    write_new_manifest(&wtx, &manifest)?;
    wtx.commit().map_err(|error| error.to_string())?;
    Ok(PhysicalStore::from_parts(
        database,
        handle,
        physical_path,
        private_integrity,
        manifest,
    ))
}

/// Open one exact current-format physical owner file.
pub(crate) fn open_physical(
    path: &Path,
    physical_identity: PhysicalStoreIdentity,
    private_integrity: Option<Arc<dyn PrivatePayloadIntegrity>>,
    layout: OwnerLayout,
) -> Result<PhysicalStore, String> {
    physical_identity.validate()?;
    let database = Database::open(path).map_err(|error| error.to_string())?;
    let (incarnation, physical_path) = StoreIncarnation::derive(path)?;
    let rtx = database.begin_read().map_err(|error| error.to_string())?;
    validate_incarnation_read(&rtx, &incarnation)?;
    let manifest = validate_manifest_read(&rtx, &physical_identity, layout)?;
    validate_declared_owner_tables(&rtx, layout)?;
    let authenticate = |sealed: &[u8], digest: &str| {
        authenticate_with(private_integrity.as_deref(), sealed, digest)
    };
    validate_recovery_content(&incarnation, &rtx, &authenticate)?;
    drop(rtx);
    Ok(PhysicalStore::from_parts(
        database,
        store_handle(incarnation),
        physical_path,
        private_integrity,
        manifest,
    ))
}
