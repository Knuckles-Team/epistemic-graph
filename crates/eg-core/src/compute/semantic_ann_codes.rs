//! Durable ANN code tier for the semantic index (feature `ann-redb`).
//!
//! RF-RULING-007. One index generation becomes durable as **one admitted
//! `Native(SemanticIndex)` mutation**: the generation's buffers are written to
//! the `eg_ann` owner table of [`eg_storage::OwnerLayout::SemanticIndex`],
//! keyed `(tenant, binding, generation, part)`, through the mutation kernel's
//! layout-bounded owner-write handle. Retiring a generation is
//! `purge_scope_with`, so the generation's ledger authority and its payload
//! retire in one transaction or not at all.
//!
//! Two properties this replaces, both defects rather than plumbing:
//!   * `eg_ann::redb_store` opened its own `redb::Database` — a leaf crate as a
//!     second physical authority, which RF-RULING-004 forbids.
//!   * it wrote the fixed keys `meta`/`codes`/`refine`, so building generation
//!     `N+1` overwrote the generation `N` that was still serving. The
//!     generation key component is what makes the two coexist.
//!
//! This module holds no physical authority of its own: the [`StorageKernelV1`]
//! it owns is the sole opener of the file, every read is a kernel-issued
//! [`ScopedRead`], and every write is admitted, ordered and committed by
//! [`MutationKernelV1`].

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;

use eg_storage::{
    OwnedStoreHandle, OwnerPayloadRetirement, PhysicalStoreIdentity, PhysicalWriteCapability,
    ScopeGrantVerifier, SemanticIndexOwner, StorageKernelV1, ANN_CODES,
};
use eg_transaction::{AdmittedOwnerWrite, Begin, MutationKernelV1};
use parking_lot::RwLock;

use crate::compute::semantic::SemanticGenerationImage;

/// Operator-facing identity of the one physical semantic-index owner file.
const SEMANTIC_PHYSICAL_STORE: &str = "eg-core:semantic-index";
/// File name under the caller's persist dir.
const SEMANTIC_STORE_FILE: &str = "semantic_index.redb";

/// Largest value written into one `eg_ann` row. The `part` key component
/// exists so a code buffer is chunked instead of stored as one
/// multi-hundred-megabyte value: 100k rows at dim 768 is ~0.8 MB of PQ codes
/// but ~77 MB of SQ8 refine codes.
const MAX_PART_BYTES: usize = 4 * 1024 * 1024;

/// The five parts of one generation, in read order. Each is stored as a
/// header row naming its exact byte length plus `ceil(len / MAX_PART_BYTES)`
/// chunk rows, so a missing or truncated chunk fails closed instead of
/// silently restoring a short buffer.
const PARTS: [&str; 5] = ["meta", "codes", "refine", "ids", "manifest"];

/// The `eg_ann` key as the storage kernel declares it: `(tenant, binding,
/// generation, part)`. Aliased so the part codec below names one type instead
/// of repeating the tuple at every signature.
type CodeKey = (&'static str, &'static str, u64, &'static str);
type CodeTable<'t> = redb::Table<'t, CodeKey, &'static [u8]>;
type CodeReadTable = redb::ReadOnlyTable<CodeKey, &'static [u8]>;

/// Errors from the durable ANN code tier.
#[derive(Debug)]
pub enum SemanticCodeError {
    /// Creating the persist dir failed.
    Io(std::io::Error),
    /// A kernel admission, capability, table or commit operation failed.
    Kernel(String),
    /// A stored generation is absent, truncated, or does not describe itself.
    Corrupt(String),
}

impl std::fmt::Display for SemanticCodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(error) => write!(f, "semantic code store io error: {error}"),
            Self::Kernel(error) => write!(f, "semantic code store kernel error: {error}"),
            Self::Corrupt(error) => write!(f, "semantic code store content error: {error}"),
        }
    }
}

impl std::error::Error for SemanticCodeError {}

impl From<std::io::Error> for SemanticCodeError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error)
    }
}

fn kernel_error(error: impl std::fmt::Display) -> SemanticCodeError {
    SemanticCodeError::Kernel(error.to_string())
}

/// The mutation scope of ONE generation of one binding.
///
/// The generation is part of the scope's **logical name**, and that is forced
/// rather than chosen. A scope binding is keyed by `binding_digest`, which is
/// computed from the tenant and the scope only -- deliberately NOT from the
/// incarnation, because that is what rejects silent same-name rebinding
/// (`physical::binding::bind_scope_in`). So two generations distinguished only
/// by their incarnation are not two scopes at all; they are one scope being
/// rebound, and the kernel refuses it with "mutation scope rebinding
/// mismatch". Making the generation part of the name is what makes generation
/// `N` an authority that `purge_scope_with` can retire while `N+1` keeps
/// serving, which is the whole point of the two-generation layout.
///
/// The owner rows keep `binding` and `generation` as SEPARATE key components
/// regardless, so a sweep across every generation of one binding stays a
/// prefix range. The two namings answer different questions: the owner key
/// groups a binding's generations, the scope names one generation's authority.
fn generation_identity(
    tenant: &str,
    binding: &str,
    generation: u64,
) -> Result<eg_types::MutationScopeIdentity, SemanticCodeError> {
    eg_types::MutationScopeIdentity::fixed_native(
        tenant,
        eg_types::mutation_batch::MutationDomain::SemanticIndex,
        &format!("{binding}:generation:{generation}"),
        &format!("semantic-ann-generation:{generation}"),
    )
    .map_err(SemanticCodeError::Kernel)
}

/// Sweep of one generation's owner payload, invoked by the mutation kernel
/// inside the same write transaction as the ledger retirement.
///
/// The generation is carried explicitly rather than parsed back out of the
/// scope's incarnation string, and the scope it is paired with is checked: a
/// retirement built for one generation cannot be handed to another's purge.
struct GenerationRetirement {
    tenant: String,
    binding: String,
    generation: u64,
}

impl OwnerPayloadRetirement<SemanticIndexOwner> for GenerationRetirement {
    fn retire_owner_payload(
        &self,
        write: &PhysicalWriteCapability<'_, SemanticIndexOwner>,
        scope: &eg_types::MutationScopeIdentity,
    ) -> Result<(), String> {
        let expected = generation_identity(&self.tenant, &self.binding, self.generation)
            .map_err(|error| error.to_string())?;
        if scope != &expected {
            return Err(
                "owner-payload retirement does not describe the scope being purged".to_string(),
            );
        }
        let mut codes = write.open_owner_write(ANN_CODES)?;
        codes
            .retain(|key, _| {
                !(key.0 == self.tenant && key.1 == self.binding && key.2 == self.generation)
            })
            .map_err(|error| error.to_string())
    }
}

/// Durable, kernel-backed ANN code tier for one `(tenant, binding)` semantic
/// index, holding any number of generations.
pub struct SemanticCodeStore {
    kernel: StorageKernelV1,
    mutations: MutationKernelV1,
    verifier: Arc<dyn ScopeGrantVerifier>,
    principal: String,
    proof: Vec<u8>,
    tenant: String,
    binding: String,
    /// Generations bound so far. `OwnedStoreHandle` is a capability and is not
    /// `Clone`, so the cache owns the one handle per generation and hands out
    /// `Arc` clones of it.
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
    /// Open (or create) `{dir}/semantic_index.redb` through the storage kernel
    /// under [`eg_storage::OwnerLayout::SemanticIndex`].
    ///
    /// `verifier` is the composition root's proof authority: only it may decide
    /// that `principal` is entitled to serve a generation of this binding. It
    /// is held rather than borrowed because a new generation's scope is
    /// authenticated lazily, long after `open` returned.
    pub fn open(
        dir: &Path,
        verifier: Arc<dyn ScopeGrantVerifier>,
        principal: &str,
        proof: &[u8],
        tenant: &str,
        binding: &str,
    ) -> Result<Self, SemanticCodeError> {
        std::fs::create_dir_all(dir)?;
        let path = dir.join(SEMANTIC_STORE_FILE);
        let physical =
            PhysicalStoreIdentity::new(SEMANTIC_PHYSICAL_STORE).map_err(kernel_error)?;
        let kernel = if path.exists() {
            StorageKernelV1::open_owner::<SemanticIndexOwner>(&path, physical, None)
        } else {
            StorageKernelV1::create_owner::<SemanticIndexOwner>(&path, physical, None)
        }
        .map_err(kernel_error)?;
        let (kernel, authority) = kernel
            .into_read_and_mutation_authority()
            .map_err(kernel_error)?;
        Ok(Self {
            kernel,
            mutations: MutationKernelV1::new(authority),
            verifier,
            principal: principal.to_string(),
            proof: proof.to_vec(),
            tenant: tenant.to_string(),
            binding: binding.to_string(),
            bound: RwLock::new(BTreeMap::new()),
        })
    }

    /// Persist one generation's image as ONE admitted maintenance mutation.
    ///
    /// `Maintenance` and not `Operation`: an index build carries no caller
    /// identity, and RF-RULING-004 requires an owner write with none to be
    /// labelled as such rather than to look like an unattributed operation.
    pub fn activate(
        &self,
        generation: u64,
        image: &SemanticGenerationImage,
    ) -> Result<(), SemanticCodeError> {
        // Read before writing. A batch's identity includes its
        // `version_expectation`, so re-admitting the same idempotency key at a
        // later scope version is an `IDEMPOTENCY_CONFLICT` rather than a
        // replay -- the kernel is right, and re-activating an image that is
        // already durable is not a mutation at all.
        if self.read(generation)?.as_ref() == Some(image) {
            return Ok(());
        }
        let owner = self.handle(generation)?;
        let batch = self.generation_batch(&owner, generation, image)?;
        let (write, begun) = self
            .mutations
            .admit_maintenance(&owner, &batch)
            .map_err(kernel_error)?;
        let source_version = match begun {
            // The idempotency key names a terminally committed receipt at this
            // exact version: an earlier attempt already applied it, so the
            // write is discarded rather than reapplied.
            Begin::Replay(_) => return write.abort().map_err(kernel_error),
            Begin::Apply { source_version } => source_version,
        };
        let rows = write.owner_rows(&owner, &batch).map_err(kernel_error)?;
        self.write_generation(&rows, generation, image)?;
        rows.finish_owner().map_err(kernel_error)?;
        self.mutations
            .finish(&write, &batch, None, 0, source_version)
            .map_err(kernel_error)?;
        self.mutations.commit(write, &batch).map_err(kernel_error)
    }

    /// Read one generation back, or `None` if it was never activated.
    ///
    /// Exactly ONE kernel-issued scoped read serves the whole generation: every
    /// part comes out of the same snapshot, so a concurrent activation of a
    /// later generation cannot tear the image being restored.
    pub fn read(
        &self,
        generation: u64,
    ) -> Result<Option<SemanticGenerationImage>, SemanticCodeError> {
        let owner = self.handle(generation)?;
        let read = self.kernel.read_scope(&owner).map_err(kernel_error)?;
        let codes = read.open_owner_table(ANN_CODES).map_err(kernel_error)?;
        let mut parts = BTreeMap::new();
        for part in PARTS {
            let Some(bytes) = read_part(&codes, &self.tenant, &self.binding, generation, part)?
            else {
                return Ok(None);
            };
            parts.insert(part, bytes);
        }
        let take = |name: &str| parts.get(name).cloned().unwrap_or_default();
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

    /// Retire one generation: its ledger authority and its owner rows go in the
    /// same transaction. A later generation of the same binding is a different
    /// scope and is untouched.
    pub fn retire(&self, generation: u64) -> Result<(), SemanticCodeError> {
        let owner = self.handle(generation)?;
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

    /// The bound serving handle for one generation, authenticated on first use.
    fn handle(
        &self,
        generation: u64,
    ) -> Result<Arc<OwnedStoreHandle<SemanticIndexOwner>>, SemanticCodeError> {
        if let Some(handle) = self.bound.read().get(&generation) {
            return Ok(Arc::clone(handle));
        }
        let identity = generation_identity(&self.tenant, &self.binding, generation)?;
        let grant = self
            .kernel
            .authenticate_scope::<SemanticIndexOwner>(
                self.verifier.as_ref(),
                identity,
                self.principal.clone(),
                &self.proof,
            )
            .map_err(kernel_error)?;
        let owner = Arc::new(self.kernel.bind_serving_scope(grant, 0).map_err(kernel_error)?);
        self.mutations
            .bootstrap_ledger(&owner)
            .map_err(kernel_error)?;
        self.bound
            .write()
            .insert(generation, Arc::clone(&owner));
        Ok(owner)
    }

    /// The batch is content-addressed from the image, so re-activating the same
    /// generation with the same bytes is an idempotent replay.
    fn generation_batch(
        &self,
        owner: &OwnedStoreHandle<SemanticIndexOwner>,
        generation: u64,
        image: &SemanticGenerationImage,
    ) -> Result<eg_types::MutationBatch, SemanticCodeError> {
        let digest = image_digest(image);
        let batch_id = format!("semantic-ann:{}:{generation}:{digest}", self.binding);
        let read = self
            .kernel
            .read_scope(owner)
            .map_err(kernel_error)?;
        let expected = eg_transaction::version(&read).map_err(kernel_error)?;
        Ok(eg_types::MutationBatch {
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
        })
    }

    fn write_generation(
        &self,
        rows: &AdmittedOwnerWrite<'_, SemanticIndexOwner>,
        generation: u64,
        image: &SemanticGenerationImage,
    ) -> Result<(), SemanticCodeError> {
        let mut codes = rows.open_table(ANN_CODES).map_err(kernel_error)?;
        for (part, bytes) in [
            ("meta", image.index.codes.meta.as_slice()),
            ("codes", image.index.codes.codes.as_slice()),
            ("refine", image.index.codes.refine.as_slice()),
            ("ids", image.index.ids.as_slice()),
            ("manifest", image.manifest.as_slice()),
        ] {
            write_part(
                &mut codes,
                &self.tenant,
                &self.binding,
                generation,
                part,
                bytes,
            )?;
        }
        Ok(())
    }
}

/// `sha256` over the image in a fixed part order, so the batch identity is the
/// content and a byte-identical re-activation replays.
fn image_digest(image: &SemanticGenerationImage) -> String {
    use sha2::{Digest, Sha256};
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

fn part_chunks(length: usize) -> usize {
    length.div_ceil(MAX_PART_BYTES)
}

fn write_part(
    codes: &mut CodeTable<'_>,
    tenant: &str,
    binding: &str,
    generation: u64,
    part: &str,
    bytes: &[u8],
) -> Result<(), SemanticCodeError> {
    codes
        .insert(
            (tenant, binding, generation, part),
            (bytes.len() as u64).to_le_bytes().as_slice(),
        )
        .map_err(kernel_error)?;
    for (ordinal, chunk) in bytes.chunks(MAX_PART_BYTES).enumerate() {
        codes
            .insert(
                (tenant, binding, generation, chunk_part(part, ordinal).as_str()),
                chunk,
            )
            .map_err(kernel_error)?;
    }
    Ok(())
}

fn read_part(
    codes: &CodeReadTable,
    tenant: &str,
    binding: &str,
    generation: u64,
    part: &str,
) -> Result<Option<Vec<u8>>, SemanticCodeError> {
    let Some(header) = codes
        .get((tenant, binding, generation, part))
        .map_err(kernel_error)?
    else {
        return Ok(None);
    };
    let length = u64::from_le_bytes(
        header
            .value()
            .try_into()
            .map_err(|_| SemanticCodeError::Corrupt(format!("part `{part}` has no length")))?,
    );
    let length = usize::try_from(length)
        .map_err(|_| SemanticCodeError::Corrupt(format!("part `{part}` length is unreadable")))?;
    let mut out = Vec::with_capacity(length.min(MAX_PART_BYTES));
    for ordinal in 0..part_chunks(length) {
        let chunk = codes
            .get((
                tenant,
                binding,
                generation,
                chunk_part(part, ordinal).as_str(),
            ))
            .map_err(kernel_error)?
            .ok_or_else(|| {
                SemanticCodeError::Corrupt(format!("part `{part}` chunk {ordinal} is missing"))
            })?;
        out.extend_from_slice(chunk.value());
    }
    if out.len() != length {
        return Err(SemanticCodeError::Corrupt(format!(
            "part `{part}` restored {} of {length} bytes",
            out.len()
        )));
    }
    Ok(Some(out))
}

fn chunk_part(part: &str, ordinal: usize) -> String {
    format!("{part}:{ordinal:08}")
}

#[cfg(test)]
#[path = "semantic_ann_codes/tests.rs"]
mod tests;
