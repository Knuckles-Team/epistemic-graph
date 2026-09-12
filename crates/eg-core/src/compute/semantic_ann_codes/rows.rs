//! The bound `eg_ann` row accessor and the chunked part codec.
//!
//! `AdmittedOwnerWrite::open_table` hands back a raw `redb::Table` — the mutation
//! kernel bounds an owner write to its layout, not to its scope's row keys,
//! because owner tables in general carry no scope component (the jobs scheduler
//! indexes carry none at all). For the semantic tables the key DOES carry the
//! scope, so the row-key ACL is this owner's obligation and it lives here:
//! every write of `eg_ann` in this crate goes through [`BoundCodeRows`], which
//! refuses any key outside the `(tenant, binding, generation)` prefix it was
//! minted for.
//!
//! The BINDING-level tables (`semantic_bindings`, `semantic_active_pointers`,
//! `semantic_tombstones`, …) need no such accessor, and deliberately do not
//! have one. One `SemanticCodeStore` serves exactly one `(tenant, binding)`
//! physical file, so every key those writes form is
//! `(self.tenant, self.binding[, generation])` taken from the store's own
//! identity — a foreign tenant or binding component is not constructible on
//! that path. The generation component is the ONE part that comes from request
//! data, and bounding it is exactly what `BoundCodeRows` is for.

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
