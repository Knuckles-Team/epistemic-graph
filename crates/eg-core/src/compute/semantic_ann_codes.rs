//! Durable ANN code tier for the semantic index (feature `ann-redb`).
//!
//! RF-RULING-007. One index generation becomes durable as **one admitted
//! `Native(SemanticIndex)` mutation**: the generation's buffers go into the
//! `eg_ann` owner table of [`eg_storage::OwnerLayout::SemanticIndex`], keyed
//! `(tenant, binding, generation, part)`, and the binding's live-generation
//! pointer is flipped in the SAME transaction, so a generation never becomes
//! live without its codes and never carries codes it cannot serve from.
//! Retiring a generation is `purge_scope_with`, so its ledger authority and its
//! payload retire together or not at all.
//!
//! **Reads write nothing.** The serving scope is bound once at `open`; every
//! read is a [`eg_storage::ScopedRead`] on it. Binding a generation's own scope
//! — two committed write transactions — happens only on the `activate` and
//! `retire` paths, so probing an unactivated generation creates no authority
//! and a read after `retire` cannot resurrect one.
//!
//! Two properties this replaces, both defects rather than plumbing:
//!   * `eg_ann::redb_store` opened its own `redb::Database` — a leaf crate as a
//!     second physical authority, which RF-RULING-004 forbids.
//!   * it wrote the fixed keys `meta`/`codes`/`refine`, so building generation
//!     `N+1` overwrote the generation `N` that was still serving. The
//!     generation key component is what makes the two coexist.
//!
//! This module holds no physical authority of its own: the [`StorageKernelV1`]
//! it owns is the sole opener of the file, and every write is admitted, ordered
//! and committed by [`MutationKernelV1`].

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;

use eg_storage::{
    OwnedStoreHandle, PhysicalStoreIdentity, ScopeGrantVerifier, ScopedRead, SemanticIndexOwner,
    StorageKernelV1, ANN_CODES, SEMANTIC_POINTERS, SEMANTIC_STATES,
};
use eg_transaction::{AdmittedMutation, Begin, MutationKernelV1};
use parking_lot::RwLock;
use sha2::{Digest, Sha256};

use crate::compute::semantic::SemanticGenerationImage;

#[path = "semantic_ann_codes/rows.rs"]
mod rows;

use rows::{
    decode_authority_row, decode_live_pointer, encode, read_part, BindingAuthority,
    BoundBindingRows, BoundCodeRows, GenerationRetirement, LivePointer, DIGEST_PART, PARTS,
};

/// Operator-facing identity of the one physical semantic-index owner file.
const SEMANTIC_PHYSICAL_STORE: &str = "eg-core:semantic-index";

/// Errors from the durable ANN code tier.
#[derive(Debug)]
pub enum SemanticCodeError {
    /// Creating the persist dir failed.
    Io(std::io::Error),
    /// A kernel admission, capability, table or commit operation failed.
    Kernel(String),
    /// A stored generation is absent, truncated, or does not describe itself.
    Corrupt(String),
    /// A write was refused by this owner's own row-key or identity ACL.
    Refused(String),
}

impl std::fmt::Display for SemanticCodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(error) => write!(f, "semantic code store io error: {error}"),
            Self::Kernel(error) => write!(f, "semantic code store kernel error: {error}"),
            Self::Corrupt(error) => write!(f, "semantic code store content error: {error}"),
            Self::Refused(error) => write!(f, "semantic code store refused: {error}"),
        }
    }
}

impl std::error::Error for SemanticCodeError {}

impl From<std::io::Error> for SemanticCodeError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error)
    }
}

pub(crate) fn kernel_error(error: impl std::fmt::Display) -> SemanticCodeError {
    SemanticCodeError::Kernel(error.to_string())
}

/// The mutation scope of ONE generation of one binding.
///
/// The generation is part of the scope's **logical name**, and that is forced
/// rather than chosen. A scope binding is keyed by `binding_digest`, computed
/// from the tenant and the scope only -- deliberately NOT from the incarnation,
/// because that is what rejects silent same-name rebinding
/// (`physical::binding::bind_scope_in`). So two generations distinguished only
/// by their incarnation are not two scopes; they are one scope being rebound,
/// and the kernel refuses it. Making the generation part of the name is what
/// makes generation `N` an authority `purge_scope_with` can retire while `N+1`
/// keeps serving.
///
/// The owner rows keep `binding` and `generation` as SEPARATE key components
/// regardless, so retiring one generation is a bounded prefix range and a sweep
/// across a binding's generations stays one too.
pub(crate) fn generation_identity(
    tenant: &str,
    binding: &str,
    generation: u64,
) -> Result<eg_types::MutationScopeIdentity, SemanticCodeError> {
    scope_identity(
        tenant,
        &format!("{binding}:generation:{generation}"),
        &format!("semantic-ann-generation:{generation}"),
    )
}

/// The read-only serving scope of one binding. Bound once at `open` so that
/// every later read is a pure snapshot; it owns no generation and is never
/// purged with one.
fn serving_identity(
    tenant: &str,
    binding: &str,
) -> Result<eg_types::MutationScopeIdentity, SemanticCodeError> {
    scope_identity(
        tenant,
        &format!("{binding}:serving"),
        "semantic-ann-serving:v1",
    )
}

fn scope_identity(
    tenant: &str,
    resource: &str,
    incarnation: &str,
) -> Result<eg_types::MutationScopeIdentity, SemanticCodeError> {
    eg_types::MutationScopeIdentity::fixed_native(
        tenant,
        eg_types::mutation_batch::MutationDomain::SemanticIndex,
        resource,
        incarnation,
    )
    .map_err(SemanticCodeError::Kernel)
}

/// Durable, kernel-backed ANN code tier for one `(tenant, binding)` semantic
/// index, holding any number of generations of which exactly one is live.
pub struct SemanticCodeStore {
    kernel: StorageKernelV1,
    mutations: MutationKernelV1,
    verifier: Arc<dyn ScopeGrantVerifier>,
    principal: String,
    proof: Vec<u8>,
    tenant: String,
    binding: String,
    /// The read-only serving scope, bound once at `open`.
    serving: OwnedStoreHandle<SemanticIndexOwner>,
    /// Generation scopes bound by a WRITER. `OwnedStoreHandle` is a capability
    /// and is not `Clone`, so the cache owns the one handle per generation and
    /// hands out `Arc` clones of it.
    bound: RwLock<BTreeMap<u64, Arc<OwnedStoreHandle<SemanticIndexOwner>>>>,
}

impl std::fmt::Debug for SemanticCodeStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SemanticCodeStore")
            .field("tenant", &self.tenant)
            .field("binding", &self.binding)
            .finish_non_exhaustive()
    }
}

impl SemanticCodeStore {
    /// Open (or create) this binding's owner file through the storage kernel
    /// under [`eg_storage::OwnerLayout::SemanticIndex`].
    ///
    /// The file name is derived from `(tenant, binding)`, because one physical
    /// file serves one binding: two bindings opened against the same directory
    /// would otherwise collide on redb's file lock.
    ///
    /// `verifier` is the composition root's proof authority: only it may decide
    /// that `principal` is entitled to serve this binding's scopes. It is held
    /// rather than borrowed because a new generation's scope is authenticated
    /// lazily, long after `open` returned.
    pub fn open(
        dir: &Path,
        verifier: Arc<dyn ScopeGrantVerifier>,
        principal: &str,
        proof: &[u8],
        tenant: &str,
        binding: &str,
    ) -> Result<Self, SemanticCodeError> {
        std::fs::create_dir_all(dir)?;
        let path = dir.join(store_file_name(tenant, binding));
        let physical = PhysicalStoreIdentity::new(SEMANTIC_PHYSICAL_STORE).map_err(kernel_error)?;
        let kernel = if path.exists() {
            StorageKernelV1::open_owner::<SemanticIndexOwner>(&path, physical, None)
        } else {
            StorageKernelV1::create_owner::<SemanticIndexOwner>(&path, physical, None)
        }
        .map_err(kernel_error)?;
        let (kernel, authority) = kernel
            .into_read_and_mutation_authority()
            .map_err(kernel_error)?;
        let mutations = MutationKernelV1::new(authority);
        let serving = bind_scope(
            &kernel,
            &mutations,
            verifier.as_ref(),
            principal,
            proof,
            serving_identity(tenant, binding)?,
        )?;
        Ok(Self {
            kernel,
            mutations,
            verifier,
            principal: principal.to_string(),
            proof: proof.to_vec(),
            tenant: tenant.to_string(),
            binding: binding.to_string(),
            serving,
            bound: RwLock::new(BTreeMap::new()),
        })
    }

    /// Persist one generation's image and make it live, as ONE admitted
    /// maintenance mutation.
    ///
    /// `Maintenance` and not `Operation`: an index build carries no caller
    /// identity, and RF-RULING-004 requires an owner write with none to be
    /// labelled as such rather than to look like an unattributed operation.
    ///
    /// Everything that decides the outcome is read INSIDE the admitted write
    /// through `open_read_table` -- the "is this already durable?" check and
    /// the binding-authority comparison -- so there is no window between the
    /// decision and the write. The version expectation is still read outside;
    /// `begin` re-verifies it and a loser fails closed with `STALE_VERSION`,
    /// which is the same shape eg-jobs uses.
    pub fn activate(
        &self,
        generation: u64,
        image: &SemanticGenerationImage,
    ) -> Result<(), SemanticCodeError> {
        let (dimensions, model_digest) = image.identity()?;
        let digest = image_digest(image);
        let owner = self.bind_for_write(generation)?;
        let expected =
            eg_transaction::version(&self.kernel.read_scope(&owner).map_err(kernel_error)?)
                .map_err(kernel_error)?;
        let batch = self.generation_batch(&owner, generation, &digest, expected);
        let write = self.mutations.open_write(&owner).map_err(kernel_error)?;
        match self.decide(&write, generation, &digest, dimensions, &model_digest) {
            Ok(true) => return write.abort().map_err(kernel_error),
            Ok(false) => {}
            Err(error) => {
                write.abort().map_err(kernel_error)?;
                return Err(error);
            }
        }
        let source_version = match write.begin_maintenance(&batch).map_err(kernel_error)? {
            // The idempotency key names a terminally committed receipt at this
            // exact version: an earlier attempt already applied it.
            Begin::Replay(_) => return write.abort().map_err(kernel_error),
            Begin::Apply { source_version } => source_version,
        };
        let rows = write.owner_rows(&owner, &batch).map_err(kernel_error)?;
        let staged = self.stage(&rows, generation, image, &digest, dimensions, &model_digest);
        rows.finish_owner().map_err(kernel_error)?;
        staged?;
        self.mutations
            .finish(&write, &batch, None, 0, source_version)
            .map_err(kernel_error)?;
        self.mutations.commit(write, &batch).map_err(kernel_error)
    }

    /// The live generation's image, or `None` when this binding has none.
    ///
    /// This is the serving read, and it serves ONLY the live generation: a
    /// generation that has been superseded or retired is not reachable here.
    pub fn read_live(
        &self,
    ) -> Result<Option<(u64, SemanticGenerationImage)>, SemanticCodeError> {
        let read = self.serving_read()?;
        let Some(generation) = self.live_generation_in(&read)? else {
            return Ok(None);
        };
        Ok(self
            .read_generation_in(&read, generation)?
            .map(|image| (generation, image)))
    }

    /// One generation's image whether or not it is live -- the MAINTENANCE
    /// read, for building, verifying and retiring a generation. Writes nothing:
    /// an unactivated or retired generation is simply `None`.
    pub fn read_generation(
        &self,
        generation: u64,
    ) -> Result<Option<SemanticGenerationImage>, SemanticCodeError> {
        let read = self.serving_read()?;
        self.read_generation_in(&read, generation)
    }

    /// The live generation number, if any.
    pub fn live_generation(&self) -> Result<Option<u64>, SemanticCodeError> {
        let read = self.serving_read()?;
        self.live_generation_in(&read)
    }

    /// Retire one generation: its ledger authority, its owner rows, and its
    /// live pointer if it held one, all in the same transaction. A later
    /// generation of the same binding is a different scope and is untouched.
    pub fn retire(&self, generation: u64) -> Result<(), SemanticCodeError> {
        let owner = self.bind_for_write(generation)?;
        let identity = generation_identity(&self.tenant, &self.binding, generation)?;
        let retirement = GenerationRetirement {
            tenant: self.tenant.clone(),
            binding: self.binding.clone(),
            generation,
        };
        self.mutations
            .purge_scope_with(&owner, &identity, &retirement)
            .map_err(kernel_error)?;
        self.bound.write().remove(&generation);
        Ok(())
    }

    /// One kernel-issued scoped read over the binding's serving scope. Owner
    /// tables are layout-bounded, so this one snapshot serves every generation.
    fn serving_read(&self) -> Result<ScopedRead<'_, SemanticIndexOwner>, SemanticCodeError> {
        self.kernel.read_scope(&self.serving).map_err(kernel_error)
    }

    fn live_generation_in(
        &self,
        read: &ScopedRead<'_, SemanticIndexOwner>,
    ) -> Result<Option<u64>, SemanticCodeError> {
        let pointers = read
            .open_owner_table(SEMANTIC_POINTERS)
            .map_err(kernel_error)?;
        decode_live_pointer(
            pointers
                .get((self.tenant.as_str(), self.binding.as_str()))
                .map_err(kernel_error)?
                .map(|value| value.value().to_vec()),
        )
    }

    fn read_generation_in(
        &self,
        read: &ScopedRead<'_, SemanticIndexOwner>,
        generation: u64,
    ) -> Result<Option<SemanticGenerationImage>, SemanticCodeError> {
        let codes = read.open_owner_table(ANN_CODES).map_err(kernel_error)?;
        let mut parts = BTreeMap::new();
        for part in PARTS {
            let Some(bytes) = read_part(&codes, &self.tenant, &self.binding, generation, part)?
            else {
                return Ok(None);
            };
            parts.insert(part, bytes);
        }
        let mut take = |name: &str| parts.remove(name).unwrap_or_default();
        Ok(Some(SemanticGenerationImage {
            index: crate::compute::semantic_ann::AnnIndexImage {
                codes: eg_ann::durable_codes::AnnCodeArtifact {
                    meta: take("meta"),
                    codes: take("codes"),
                    refine: take("refine"),
                },
                ids: take("ids"),
            },
            manifest: take("manifest"),
        }))
    }

    /// The bound serving handle for one generation, authenticated and bound on
    /// first WRITE. This commits two transactions (the scope binding and the
    /// ledger bootstrap) and is therefore never on a read path.
    fn bind_for_write(
        &self,
        generation: u64,
    ) -> Result<Arc<OwnedStoreHandle<SemanticIndexOwner>>, SemanticCodeError> {
        if let Some(handle) = self.bound.read().get(&generation) {
            return Ok(Arc::clone(handle));
        }
        let identity = generation_identity(&self.tenant, &self.binding, generation)?;
        let owner = Arc::new(bind_scope(
            &self.kernel,
            &self.mutations,
            self.verifier.as_ref(),
            &self.principal,
            &self.proof,
            identity,
        )?);
        self.bound.write().insert(generation, Arc::clone(&owner));
        Ok(owner)
    }

    /// Decide, inside the admitted write, whether this activation is a no-op
    /// and whether it is admissible at all. `true` means "already durable".
    fn decide(
        &self,
        write: &AdmittedMutation<'_, SemanticIndexOwner>,
        generation: u64,
        digest: &str,
        dimensions: usize,
        model_digest: &Option<String>,
    ) -> Result<bool, SemanticCodeError> {
        let codes = write.open_read_table(ANN_CODES).map_err(kernel_error)?;
        let current = codes
            .get((
                self.tenant.as_str(),
                self.binding.as_str(),
                generation,
                DIGEST_PART,
            ))
            .map_err(kernel_error)?
            .map(|value| value.value().to_vec());
        drop(codes);
        if current.as_deref() == Some(digest.as_bytes()) {
            return Ok(true);
        }
        let states = write
            .open_read_table(SEMANTIC_STATES)
            .map_err(kernel_error)?;
        let raw = states
            .get((self.tenant.as_str(), self.binding.as_str()))
            .map_err(kernel_error)?
            .map(|value| value.value().to_vec());
        drop(states);
        let stored = decode_authority_row(raw)?;
        if let Some(authority) = stored {
            if authority.dimensions != dimensions || &authority.model_digest != model_digest {
                return Err(SemanticCodeError::Refused(format!(
                    "generation {generation} declares {dimensions} dimensions / model {model_digest:?}; \
                     binding `{}` is bound to {} dimensions / model {:?}",
                    self.binding, authority.dimensions, authority.model_digest
                )));
            }
        }
        Ok(false)
    }

    /// Write the generation's parts, its binding authority (first activation
    /// only) and the live pointer, all through the bound accessors so no key
    /// outside this binding's prefix is reachable.
    fn stage(
        &self,
        rows: &eg_transaction::AdmittedOwnerWrite<'_, SemanticIndexOwner>,
        generation: u64,
        image: &SemanticGenerationImage,
        digest: &str,
        dimensions: usize,
        model_digest: &Option<String>,
    ) -> Result<(), SemanticCodeError> {
        let mut codes = BoundCodeRows::new(
            rows.open_table(ANN_CODES).map_err(kernel_error)?,
            &self.tenant,
            &self.binding,
            generation,
        );
        for (part, bytes) in [
            ("meta", image.index.codes.meta.as_slice()),
            ("codes", image.index.codes.codes.as_slice()),
            ("refine", image.index.codes.refine.as_slice()),
            ("ids", image.index.ids.as_slice()),
            ("manifest", image.manifest.as_slice()),
        ] {
            codes.put_part(part, bytes)?;
        }
        codes.insert(
            (&self.tenant, &self.binding, generation, DIGEST_PART),
            digest.as_bytes(),
        )?;
        drop(codes);
        let authority = encode(&BindingAuthority {
            dimensions,
            model_digest: model_digest.clone(),
        })?;
        BoundBindingRows::new(
            rows.open_table(SEMANTIC_STATES).map_err(kernel_error)?,
            &self.tenant,
            &self.binding,
        )
        .put(&authority)?;
        let pointer = encode(&LivePointer { generation })?;
        BoundBindingRows::new(
            rows.open_table(SEMANTIC_POINTERS).map_err(kernel_error)?,
            &self.tenant,
            &self.binding,
        )
        .put(&pointer)
    }

    /// The batch is content-addressed from the image, so a byte-identical
    /// re-activation at the same version replays instead of applying twice.
    fn generation_batch(
        &self,
        owner: &OwnedStoreHandle<SemanticIndexOwner>,
        generation: u64,
        digest: &str,
        expected: u64,
    ) -> eg_types::MutationBatch {
        let batch_id = format!("semantic-ann:{}:{generation}:{digest}", self.binding);
        eg_types::MutationBatch {
            schema_version: eg_types::MUTATION_BATCH_VERSION,
            batch_id: batch_id.clone(),
            context: eg_types::MutationRequestContext {
                request_id: 0,
                principal: owner.principal().to_string(),
                purpose: None,
                policy_fingerprint: None,
                trace_id: None,
                verified_capabilities: std::collections::BTreeSet::new(),
            },
            identity: owner.identity().clone(),
            placement_epoch: 0,
            idempotency_key: batch_id,
            version_expectation: eg_types::VersionExpectation::Native(expected),
            fencing_token: None,
            authoritative_state: None,
            operations: vec![eg_types::MutationOperation {
                ordinal: 0,
                surface: eg_types::MutationSurface::Other,
                domain: eg_types::mutation_batch::MutationDomain::SemanticIndex,
                method: eg_types::protocol::Method::ApplyMutation {
                    event_type: "semantic_index_generation_activated".to_string(),
                    query: format!("sha256:{digest}"),
                },
            }],
            outbox: Vec::new(),
            created_at_ms: 0,
        }
    }
}

/// Authenticate one scope and bind it, bootstrapping the ledger. TWO committed
/// write transactions -- which is exactly why no read path calls this.
fn bind_scope(
    kernel: &StorageKernelV1,
    mutations: &MutationKernelV1,
    verifier: &dyn ScopeGrantVerifier,
    principal: &str,
    proof: &[u8],
    identity: eg_types::MutationScopeIdentity,
) -> Result<OwnedStoreHandle<SemanticIndexOwner>, SemanticCodeError> {
    let grant = kernel
        .authenticate_scope::<SemanticIndexOwner>(verifier, identity, principal.to_string(), proof)
        .map_err(kernel_error)?;
    let owner = kernel.bind_serving_scope(grant, 0).map_err(kernel_error)?;
    mutations.bootstrap_ledger(&owner).map_err(kernel_error)?;
    Ok(owner)
}

/// One physical file per `(tenant, binding)`, named by a digest so a binding
/// name never becomes a path component.
fn store_file_name(tenant: &str, binding: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"eg/semantic-index-file/v1\0");
    hasher.update(tenant.as_bytes());
    hasher.update([0]);
    hasher.update(binding.as_bytes());
    format!("semantic_index-{}.redb", hex::encode(hasher.finalize()))
}

/// `sha256` over the image in a fixed part order, so the batch identity and the
/// "already durable" decision are both the content.
fn image_digest(image: &SemanticGenerationImage) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"eg/semantic-ann-generation/v1\0");
    for bytes in [
        image.index.codes.meta.as_slice(),
        image.index.codes.codes.as_slice(),
        image.index.codes.refine.as_slice(),
        image.index.ids.as_slice(),
        image.manifest.as_slice(),
    ] {
        hasher.update((bytes.len() as u64).to_le_bytes());
        hasher.update(bytes);
    }
    hex::encode(hasher.finalize())
}

#[cfg(test)]
#[path = "semantic_ann_codes/tests.rs"]
mod tests;
