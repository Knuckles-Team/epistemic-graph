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

use eg_storage::{
    OwnedStoreHandle, OwnerDomain, PhysicalStoreIdentity, ScopedRead, StorageKernelV1,
};
use eg_transaction::{AdmittedOwnerWrite, Begin, MutationKernelV1};
use eg_types::{MutationBatch, MutationScopeIdentity};

use crate::store_authority::EngineScopeAuthority;

/// The tenant every store-private sidecar scope belongs to. These files hold no
/// tenant data; they are the node's own state.
const SIDECAR_TENANT: &str = "native";

/// One kernel-owned sidecar store: a storage kernel, the one mutation kernel it
/// issued, and the single bound serving scope.
pub struct SidecarStore<D: OwnerDomain> {
    kernel: StorageKernelV1,
    mutations: MutationKernelV1,
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
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(|error| error.to_string())?;
        }
        let physical = PhysicalStoreIdentity::new(physical_name)?;
        let identity = MutationScopeIdentity::fixed_native(
            SIDECAR_TENANT,
            eg_types::mutation_batch::MutationDomain::ControlPlane,
            resource,
            incarnation,
        )?;
        let proof = authority.proof(&physical, D::LAYOUT, &identity);
        let kernel = if path.exists() {
            StorageKernelV1::open_owner::<D>(path, physical, None)
        } else {
            StorageKernelV1::create_owner::<D>(path, physical, None)
        }?;
        let (kernel, write_authority) = kernel.into_read_and_mutation_authority()?;
        let mutations = MutationKernelV1::new(write_authority);
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

    /// The scope's authoritative mutation version.
    pub fn version(&self) -> Result<u64, String> {
        eg_transaction::version(&self.read()?)
    }

    /// Apply one owner-row maintenance write in a single admitted, ledgered,
    /// version-bumping transaction.
    ///
    /// `event` names the operation in the durable batch record, so a store's
    /// ledger says what each of its versions did.
    pub fn maintain<F>(&self, event: &str, apply: F) -> Result<(), String>
    where
        F: FnOnce(&AdmittedOwnerWrite<'_, D>) -> Result<(), String>,
    {
        let batch = maintenance_batch(
            self.owner.identity(),
            self.owner.principal(),
            event,
            self.version()?,
        )?;
        let (write, begun) = self.mutations.admit_maintenance(&self.owner, &batch)?;
        let source_version = match begun {
            // The same version can only be written once, so a replay means this
            // exact attempt already committed; re-applying it would double the
            // effect.
            Begin::Replay(_) => return write.abort(),
            Begin::Apply { source_version } => source_version,
        };
        let owner_write = write.owner_rows(&self.owner, &batch)?;
        match apply(&owner_write) {
            Ok(()) => owner_write.finish_owner()?,
            Err(error) => {
                drop(owner_write);
                write.abort()?;
                return Err(error);
            }
        }
        self.mutations.finish(&write, &batch, None, 0, source_version)?;
        self.mutations.commit(write, &batch)
    }
}

/// The batch for one sidecar maintenance write.
///
/// `batch_id` is `(event, scope version)`: exactly one batch commits per
/// version, so it is unique per attempt and stable across a crash-retry of that
/// attempt, which makes a retry a replay rather than an `IDEMPOTENCY_CONFLICT`.
fn maintenance_batch(
    identity: &MutationScopeIdentity,
    principal: &str,
    event: &str,
    expected_version: u64,
) -> Result<MutationBatch, String> {
    let batch_id = format!("{event}:v{expected_version}");
    let operation = eg_types::MutationOperation {
        ordinal: 0,
        surface: eg_types::MutationSurface::Other,
        domain: eg_types::mutation_batch::MutationDomain::ControlPlane,
        method: eg_types::protocol::Method::ApplyMutation {
            event_type: event.to_string(),
            query: batch_id.clone(),
        },
    };
    let batch = MutationBatch {
        schema_version: eg_types::MUTATION_BATCH_VERSION,
        batch_id: batch_id.clone(),
        context: eg_types::MutationRequestContext {
            request_id: 0,
            principal: principal.to_string(),
            purpose: None,
            policy_fingerprint: None,
            trace_id: None,
            // A maintenance mutation claims no capability: a plain
            // `Native`-versioned write, not the reserved-system `Unversioned`
            // path. Empty is the true fact here, not a placeholder.
            verified_capabilities: std::collections::BTreeSet::new(),
        },
        identity: identity.clone(),
        placement_epoch: 0,
        idempotency_key: batch_id,
        version_expectation: eg_types::VersionExpectation::Native(expected_version),
        fencing_token: None,
        authoritative_state: None,
        operations: vec![operation],
        outbox: Vec::new(),
        created_at_ms: 0,
    };
    batch.validate()?;
    Ok(batch)
}
