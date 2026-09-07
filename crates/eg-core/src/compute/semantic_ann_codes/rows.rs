//! Bound row accessors, the chunked part codec, and generation retirement.
//!
//! `AdmittedOwnerWrite::open_table` hands back a raw `redb::Table` — the mutation
//! kernel bounds an owner write to its layout, not to its scope's row keys,
//! because owner tables in general carry no scope component (the jobs scheduler
//! indexes carry none at all). For the semantic tables the key DOES carry the
//! scope, so the row-key ACL is this owner's obligation and it lives here: every
//! write of `eg_ann` or a binding row in this crate goes through one of the two
//! bound accessors below, each of which refuses any key outside the prefix it
//! was minted for.

use eg_storage::{ANN_CODES, SEMANTIC_POINTERS};
use serde::{Deserialize, Serialize};

use super::{kernel_error, SemanticCodeError};

/// Largest value written into one `eg_ann` row. The `part` key component
/// exists so a code buffer is chunked instead of stored as one
/// multi-hundred-megabyte value: 100k rows at dim 768 is ~0.8 MB of PQ codes
/// but ~77 MB of SQ8 refine codes.
pub(super) const MAX_PART_BYTES: usize = 4 * 1024 * 1024;

/// The five parts that make one generation's image, in read order. Each is
/// stored as a header row naming its exact byte length plus
/// `ceil(len / MAX_PART_BYTES)` chunk rows, so a missing or truncated chunk
/// fails closed instead of silently restoring a short buffer.
pub(super) const PARTS: [&str; 5] = ["meta", "codes", "refine", "ids", "manifest"];

/// The generation's content digest, stored unchunked beside its parts so the
/// "is this exactly what is already durable?" decision is one lookup inside the
/// admitted write rather than a re-read of every buffer.
pub(super) const DIGEST_PART: &str = "digest";

/// The `eg_ann` key as the storage kernel declares it.
pub(super) type CodeKey = (&'static str, &'static str, u64, &'static str);
pub(super) type CodeTable<'t> = redb::Table<'t, CodeKey, &'static [u8]>;
pub(super) type CodeReadTable = redb::ReadOnlyTable<CodeKey, &'static [u8]>;
pub(super) type BindingKey = (&'static str, &'static str);
pub(super) type BindingTable<'t> = redb::Table<'t, BindingKey, &'static [u8]>;

/// The binding's durable authority record: the model identity and width every
/// generation of this binding must agree with. Written at first activation and
/// compared, never replaced, thereafter.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct BindingAuthority {
    pub(super) dimensions: usize,
    pub(super) model_digest: Option<String>,
}

/// The binding's live-generation pointer. ONE row per `(tenant, binding)`, so
/// two live generations are structurally impossible rather than merely unlikely.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct LivePointer {
    pub(super) generation: u64,
}

/// `eg_ann` rows bound to exactly one `(tenant, binding, generation)`.
pub(super) struct BoundCodeRows<'t> {
    table: CodeTable<'t>,
    tenant: String,
    binding: String,
    generation: u64,
}

impl<'t> BoundCodeRows<'t> {
    pub(super) fn new(table: CodeTable<'t>, tenant: &str, binding: &str, generation: u64) -> Self {
        Self {
            table,
            tenant: tenant.to_string(),
            binding: binding.to_string(),
            generation,
        }
    }

    /// The row-key ACL. A key naming another tenant, binding or generation is
    /// refused even though the kernel's layout bound would allow it.
    fn permit(&self, key: (&str, &str, u64, &str)) -> Result<(), SemanticCodeError> {
        if key.0 != self.tenant || key.1 != self.binding || key.2 != self.generation {
            return Err(SemanticCodeError::Refused(format!(
                "semantic code write may not address another generation's rows \
                 (bound to {}/{}/{}, attempted {}/{}/{})",
                self.tenant, self.binding, self.generation, key.0, key.1, key.2
            )));
        }
        Ok(())
    }

    pub(super) fn insert(
        &mut self,
        key: (&str, &str, u64, &str),
        value: &[u8],
    ) -> Result<(), SemanticCodeError> {
        self.permit(key)?;
        self.table.insert(key, value).map_err(kernel_error)?;
        Ok(())
    }

    /// Write one part as a length header plus size-bounded chunks, then drop any
    /// chunk left behind by a longer previous image of the same part.
    pub(super) fn put_part(&mut self, part: &str, bytes: &[u8]) -> Result<(), SemanticCodeError> {
        let (tenant, binding, generation) =
            (self.tenant.clone(), self.binding.clone(), self.generation);
        self.insert(
            (&tenant, &binding, generation, part),
            (bytes.len() as u64).to_le_bytes().as_slice(),
        )?;
        let chunks = part_chunks(bytes.len());
        for (ordinal, chunk) in bytes.chunks(MAX_PART_BYTES).enumerate() {
            let key = chunk_part(part, ordinal);
            self.insert((&tenant, &binding, generation, key.as_str()), chunk)?;
        }
        self.table
            .retain_in(
                (
                    tenant.as_str(),
                    binding.as_str(),
                    generation,
                    chunk_part(part, 0).as_str(),
                )
                    ..=(
                        tenant.as_str(),
                        binding.as_str(),
                        generation,
                        chunk_upper_bound(part).as_str(),
                    ),
                |key, _| chunk_ordinal(key.3).is_some_and(|ordinal| ordinal < chunks),
            )
            .map_err(kernel_error)?;
        Ok(())
    }
}

/// The binding-level rows (`(tenant, binding)` keys) bound to one binding.
pub(super) struct BoundBindingRows<'t> {
    table: BindingTable<'t>,
    tenant: String,
    binding: String,
}

impl<'t> BoundBindingRows<'t> {
    pub(super) fn new(table: BindingTable<'t>, tenant: &str, binding: &str) -> Self {
        Self {
            table,
            tenant: tenant.to_string(),
            binding: binding.to_string(),
        }
    }

    fn permit(&self, key: (&str, &str)) -> Result<(), SemanticCodeError> {
        if key.0 != self.tenant || key.1 != self.binding {
            return Err(SemanticCodeError::Refused(format!(
                "semantic binding write may not address another binding's rows \
                 (bound to {}/{}, attempted {}/{})",
                self.tenant, self.binding, key.0, key.1
            )));
        }
        Ok(())
    }

    pub(super) fn insert(
        &mut self,
        key: (&str, &str),
        value: &[u8],
    ) -> Result<(), SemanticCodeError> {
        self.permit(key)?;
        self.table.insert(key, value).map_err(kernel_error)?;
        Ok(())
    }

    pub(super) fn put(&mut self, value: &[u8]) -> Result<(), SemanticCodeError> {
        let (tenant, binding) = (self.tenant.clone(), self.binding.clone());
        self.insert((tenant.as_str(), binding.as_str()), value)
    }
}

/// Sweep of one generation's owner payload, invoked by the mutation kernel
/// inside the same write transaction as the ledger retirement.
///
/// The generation is carried explicitly rather than parsed back out of the
/// scope's incarnation string, and the scope it is paired with is checked: a
/// retirement built for one generation cannot be handed to another's purge.
pub(super) struct GenerationRetirement {
    pub(super) tenant: String,
    pub(super) binding: String,
    pub(super) generation: u64,
}

impl eg_storage::OwnerPayloadRetirement<eg_storage::SemanticIndexOwner> for GenerationRetirement {
    fn retire_owner_payload(
        &self,
        write: &eg_storage::PhysicalWriteCapability<'_, eg_storage::SemanticIndexOwner>,
        scope: &eg_types::MutationScopeIdentity,
    ) -> Result<(), String> {
        let expected = super::generation_identity(&self.tenant, &self.binding, self.generation)
            .map_err(|error| error.to_string())?;
        if scope != &expected {
            return Err(
                "owner-payload retirement does not describe the scope being purged".to_string(),
            );
        }
        // A bounded range over exactly this generation's prefix, not a scan of
        // every tenant's rows: retirement of one generation must not cost the
        // whole table (plan R3's complexity family).
        let mut codes = write.open_owner_write(ANN_CODES)?;
        codes
            .retain_in(
                (
                    self.tenant.as_str(),
                    self.binding.as_str(),
                    self.generation,
                    "",
                )
                    ..=(
                        self.tenant.as_str(),
                        self.binding.as_str(),
                        self.generation,
                        PART_UPPER_BOUND,
                    ),
                |_, _| false,
            )
            .map_err(|error| error.to_string())?;
        drop(codes);
        // The live pointer may not outlive the generation it names.
        let live = read_live_pointer_in(write, &self.tenant, &self.binding)
            .map_err(|error| error.to_string())?;
        if live == Some(self.generation) {
            write
                .open_owner_write(SEMANTIC_POINTERS)?
                .remove((self.tenant.as_str(), self.binding.as_str()))
                .map_err(|error| error.to_string())?;
        }
        Ok(())
    }
}

fn read_live_pointer_in(
    write: &eg_storage::PhysicalWriteCapability<'_, eg_storage::SemanticIndexOwner>,
    tenant: &str,
    binding: &str,
) -> Result<Option<u64>, SemanticCodeError> {
    let pointers = write
        .open_owner_read(SEMANTIC_POINTERS)
        .map_err(kernel_error)?;
    let raw = pointers
        .get((tenant, binding))
        .map_err(kernel_error)?
        .map(|value| value.value().to_vec());
    decode_live_pointer(raw)
}

pub(super) fn decode_live_pointer(raw: Option<Vec<u8>>) -> Result<Option<u64>, SemanticCodeError> {
    let Some(bytes) = raw else {
        return Ok(None);
    };
    let pointer: LivePointer = rmp_serde::from_slice(&bytes)
        .map_err(|error| SemanticCodeError::Corrupt(error.to_string()))?;
    Ok(Some(pointer.generation))
}

pub(super) fn decode_authority(bytes: &[u8]) -> Result<BindingAuthority, SemanticCodeError> {
    rmp_serde::from_slice(bytes).map_err(|error| SemanticCodeError::Corrupt(error.to_string()))
}

pub(super) fn encode<T: Serialize>(value: &T) -> Result<Vec<u8>, SemanticCodeError> {
    rmp_serde::to_vec_named(value).map_err(|error| SemanticCodeError::Corrupt(error.to_string()))
}

/// Sorts above every `{part}:{ordinal:08}` and every bare part name, so a
/// `..=` range over it covers one generation's whole key space.
const PART_UPPER_BOUND: &str = "\u{10FFFF}";

pub(super) fn part_chunks(length: usize) -> usize {
    length.div_ceil(MAX_PART_BYTES)
}

pub(super) fn chunk_part(part: &str, ordinal: usize) -> String {
    format!("{part}:{ordinal:08}")
}

fn chunk_upper_bound(part: &str) -> String {
    format!("{part}:{PART_UPPER_BOUND}")
}

fn chunk_ordinal(key: &str) -> Option<usize> {
    key.rsplit_once(':')
        .and_then(|(_, ordinal)| ordinal.parse().ok())
}

/// Read one part back out of a snapshot, verifying the header's exact length.
pub(super) fn read_part(
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
    let length: [u8; 8] = header
        .value()
        .try_into()
        .map_err(|_| SemanticCodeError::Corrupt(format!("part `{part}` has no length")))?;
    let length = usize::try_from(u64::from_le_bytes(length))
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

/// The binding authority row, decoded from whatever snapshot read it. Takes the
/// raw bytes rather than a table because the kernel's two read surfaces are
/// distinct types (`ReadOnlyTable` outside a write, `OwnerReadTable` inside
/// one) and this decode is the same either way.
pub(super) fn decode_authority_row(
    raw: Option<Vec<u8>>,
) -> Result<Option<BindingAuthority>, SemanticCodeError> {
    let Some(bytes) = raw else {
        return Ok(None);
    };
    decode_authority(&bytes).map(Some)
}
