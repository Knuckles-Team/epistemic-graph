//! The one shape every root-binary sidecar owner file takes (RF-RULING-004).
//!
//! Six small durable stores in this binary — the signed-request replay ledger,
//! the render-provenance side store, the cold-tier cache, the tenant catalog,
//! the node-info directory and the cluster-hierarchy cache — are each ONE
//! physical file serving ONE fixed native `ControlPlane` scope. Before the
//! kernels each opened its own `redb::Database`, which made each a second
//! physical authority.
//!
//! This is not a wrapper that hands a raw database back out: it composes the
//! two kernels and exposes only what they issue — a scoped read, and an
//! admitted owner write. Every write is a MAINTENANCE mutation
//! (RF-RULING-005): these stores carry no caller identity (a nonce record, a
//! recomputable cache row, a catalog projection), so they are outside
//! operation-replay conflict semantics, but they are still ledgered, fenced and
//! version-bumping, because an un-ledgered owner write would be a second
//! authority.

use std::path::Path;

use eg_storage::{OwnedStoreHandle, OwnerDomain, PhysicalStoreIdentity, ScopedRead, StorageKernel};
use eg_transaction::{AdmittedOwnerWrite, Begin, MaintenanceBatch, MutationKernel};
use eg_types::MutationScopeIdentity;

use crate::store_authority::EngineScopeAuthority;

/// The tenant every store-private sidecar scope belongs to. These files hold no
/// tenant data; they are the node's own state.
const SIDECAR_TENANT: &str = "native";

/// One kernel-owned sidecar store: a storage kernel, the one mutation kernel it
/// issued, and the single bound serving scope.
pub struct SidecarStore<D: OwnerDomain> {
    kernel: StorageKernel,
    mutations: MutationKernel,
    owner: OwnedStoreHandle<D>,
}

impl<D: OwnerDomain> std::fmt::Debug for SidecarStore<D> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SidecarStore").finish_non_exhaustive()
    }
}

impl<D: OwnerDomain> SidecarStore<D> {
    /// Open (creating if absent) the one owner file at `path`.
    ///
    /// `physical_name` names the physical authority boundary and `resource`
    /// the logical scope; both are fixed per store, so a file opened under one
    /// store's identity can never be served under another's.
    pub fn open(
        path: &Path,
        physical_name: &str,
        resource: &str,
        incarnation: &str,
        authority: &EngineScopeAuthority,
    ) -> Result<Self, String> {
        let identity = MutationScopeIdentity::fixed_native(
            SIDECAR_TENANT,
            eg_types::mutation_batch::DurabilityDomain::ControlPlane,
            resource,
            incarnation,
        )?;
        Self::open_with(path, physical_name, identity, None, authority)
    }

    /// Open one owner file whose serving scope is not the fixed-native shape
    /// [`Self::open`] builds, or which needs a private-payload integrity
    /// authority.
    ///
    /// The admin-mutations coordinator store is both: its scope is
    /// GRAPH-shaped (`native`/`cluster-admin`, which only `OwnerLayout::LedgerOnly`
    /// accepts) and its sealed transaction-recovery plans are authenticated by
    /// the transaction-recovery cipher.
    pub fn open_with(
        path: &Path,
        physical_name: &str,
        identity: MutationScopeIdentity,
        private_integrity: Option<std::sync::Arc<dyn eg_storage::PrivatePayloadIntegrity>>,
        authority: &EngineScopeAuthority,
    ) -> Result<Self, String> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(|error| error.to_string())?;
        }
        let physical = PhysicalStoreIdentity::new(physical_name)?;
        let proof = authority.proof();
        let kernel = if path.exists() {
            StorageKernel::open_owner::<D>(path, physical, private_integrity)
        } else {
            StorageKernel::create_owner::<D>(path, physical, private_integrity)
        }?;
        let (kernel, write_authority) = kernel.into_read_and_mutation_authority()?;
        let mutations = MutationKernel::new(write_authority);
        let grant = kernel.authenticate_scope::<D>(
            authority,
            identity,
            authority.principal().to_string(),
            &proof,
        )?;
        let owner = kernel.bind_serving_scope(grant, 0)?;
        mutations.bootstrap_ledger(&owner)?;
        Ok(Self {
            kernel,
            mutations,
            owner,
        })
    }

    /// One scoped read over this owner file.
    pub fn read(&self) -> Result<ScopedRead<'_, D>, String> {
        self.kernel.read_scope(&self.owner)
    }

    /// The one mutation kernel this store issued, for a domain whose writes are
    /// not owner-row writes — a saga, say, whose whole effect is ledger rows.
    pub fn mutations(&self) -> &MutationKernel {
        &self.mutations
    }

    /// The one bound serving scope, the capability every kernel call takes.
    pub fn owner(&self) -> &OwnedStoreHandle<D> {
        &self.owner
    }

    /// The storage kernel that owns this file, for the recovery, backup and
    /// fingerprint operations `eg_storage` defines over a whole store rather
    /// than over one scope.
    pub fn kernel(&self) -> &StorageKernel {
        &self.kernel
    }

    /// Apply one owner-row maintenance write in a single admitted, ledgered,
    /// version-bumping transaction.
    ///
    /// `event` names the operation in the durable batch record, so a store's
    /// ledger says what each of its versions did.
    ///
    /// The scope version the batch fences on is resolved by
    /// [`MutationKernel::admit_current`] INSIDE the write transaction. Reading
    /// it from a snapshot first would let two concurrent callers observe the
    /// same version, build byte-identical batches, and have the second one
    /// silently replay the first's record instead of applying its own write.
    pub fn maintain<F>(&self, event: &str, apply: F) -> Result<(), String>
    where
        F: FnOnce(&AdmittedOwnerWrite<'_, D>) -> Result<(), String>,
    {
        let subject = eg_storage::ledger_scope_key(self.owner.identity());
        let write = MaintenanceBatch::new(
            eg_types::mutation_batch::DurabilityDomain::ControlPlane,
            event,
            &subject,
        );
        let (txn, batch, begun) = self.mutations.admit_current(
            &self.owner,
            eg_storage::MutationClass::Maintenance,
            |version| write.for_scope_version(&self.owner, version),
        )?;
        let source_version = match begun {
            // The same version can only be written once, so a replay means this
            // exact attempt already committed; re-applying it would double the
            // effect.
            Begin::Replay(_) => return txn.abort(),
            Begin::Apply { source_version } => source_version,
        };
        let owner_write = txn.owner_rows(&self.owner, &batch)?;
        match apply(&owner_write) {
            Ok(()) => owner_write.finish_owner()?,
            Err(error) => {
                drop(owner_write);
                txn.abort()?;
                return Err(error);
            }
        }
        self.mutations
            .finish(&txn, &batch, None, 0, source_version)?;
        self.mutations.commit(txn, &batch)
    }
}
