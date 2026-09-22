// CONCEPT:EH-280 — validation and identities of a branch-aware IndexRepository scope.
//
// A scope declares refs, the `(ref, path) -> blob digest` memberships of this
// batch, and the tombstones for paths that left a ref. Everything here is pure:
// the catalog is a validated, ORDERED lookup view (every map is a BTreeMap so
// projection output never depends on hash iteration order) and the identity
// functions are deterministic, length-prefixed SHA-256 keys.

use std::collections::{BTreeMap, BTreeSet};

use eg_types::ingestion_wire::{
    IndexFileVersion, IndexRefStatus, IndexRepositoryScope, IndexTombstone,
};
use sha2::{Digest, Sha256};

const INVALID: &str = "AST_INPUT_INVALID";
const MAX_SCOPE_NAME_BYTES: usize = 1_024;
const SHA256_PREFIX: &str = "sha256:";

type RefTable<'a> = BTreeMap<&'a str, IndexRefStatus>;
/// `(ref_name, path) -> blob digest`.
pub(super) type Memberships<'a> = BTreeMap<(&'a str, &'a str), &'a str>;

/// Validated lookup view over one [`IndexRepositoryScope`].
pub(super) struct ScopeCatalog<'a> {
    pub(super) scope: &'a IndexRepositoryScope,
    pub(super) memberships: Memberships<'a>,
    /// Every `(path, blob digest)` bound by at least one live ref.
    bindings: BTreeSet<(&'a str, &'a str)>,
}

impl<'a> ScopeCatalog<'a> {
    /// Validate `scope` structurally. Logical-path syntax is checked by the
    /// request boundary, which owns the portable-path rule.
    pub(super) fn build(scope: &'a IndexRepositoryScope) -> Result<Self, String> {
        check_name("repository_id", &scope.repository_id)?;
        let refs = declared_refs(scope)?;
        let memberships = collect_memberships(scope.file_versions.as_slice(), &refs)?;
        check_tombstones(scope.tombstones.as_slice(), &refs, &memberships)?;
        let bindings = memberships
            .iter()
            .map(|(&(_, path), &digest)| (path, digest))
            .collect();
        Ok(Self {
            scope,
            memberships,
            bindings,
        })
    }

    /// Whether some live ref of this batch binds `path` to `digest`.
    pub(super) fn binds(&self, path: &str, digest: &str) -> bool {
        self.bindings.contains(&(path, digest))
    }

    /// The batch's memberships grouped by ref, in ref-name then path order.
    pub(super) fn paths_by_ref(&self) -> BTreeMap<&'a str, Vec<(&'a str, &'a str)>> {
        let mut grouped: BTreeMap<&'a str, Vec<(&'a str, &'a str)>> = BTreeMap::new();
        for (&(ref_name, path), &digest) in &self.memberships {
            grouped.entry(ref_name).or_default().push((path, digest));
        }
        grouped
    }
}

fn declared_refs(scope: &IndexRepositoryScope) -> Result<RefTable<'_>, String> {
    let mut refs = RefTable::new();
    for item in scope.refs.iter() {
        check_name("ref_name", &item.ref_name)?;
        check_git_object_id(&item.revision_id)?;
        if refs.insert(item.ref_name.as_str(), item.status).is_some() {
            return Err(format!(
                "{INVALID}: ref {} is declared twice",
                item.ref_name
            ));
        }
    }
    Ok(refs)
}

fn collect_memberships<'a>(
    versions: &'a [IndexFileVersion],
    refs: &RefTable<'_>,
) -> Result<Memberships<'a>, String> {
    let mut memberships = Memberships::new();
    for version in versions {
        require_live_ref(refs, &version.ref_name)?;
        check_sha256("blob_digest", &version.blob_digest)?;
        let key = (version.ref_name.as_str(), version.path.as_str());
        if memberships
            .insert(key, version.blob_digest.as_str())
            .is_some()
        {
            return Err(format!(
                "{INVALID}: path {} appears twice in ref {}",
                version.path, version.ref_name
            ));
        }
    }
    Ok(memberships)
}

fn require_live_ref(refs: &RefTable<'_>, ref_name: &str) -> Result<(), String> {
    match refs.get(ref_name) {
        Some(IndexRefStatus::Live) => Ok(()),
        Some(IndexRefStatus::Deleted) => Err(format!(
            "{INVALID}: deleted ref {ref_name} cannot contain file versions"
        )),
        None => Err(format!(
            "{INVALID}: file version names undeclared ref {ref_name}"
        )),
    }
}

fn check_tombstones(
    tombstones: &[IndexTombstone],
    refs: &RefTable<'_>,
    memberships: &Memberships<'_>,
) -> Result<(), String> {
    let mut seen = BTreeSet::new();
    for tombstone in tombstones {
        if !refs.contains_key(tombstone.ref_name.as_str()) {
            return Err(format!(
                "{INVALID}: tombstone names undeclared ref {}",
                tombstone.ref_name
            ));
        }
        check_sha256("prior_blob_digest", &tombstone.prior_blob_digest)?;
        let key = (tombstone.ref_name.as_str(), tombstone.path.as_str());
        if !seen.insert(key) {
            return Err(format!(
                "{INVALID}: path {} is tombstoned twice in ref {}",
                tombstone.path, tombstone.ref_name
            ));
        }
        if memberships.get(&key) == Some(&tombstone.prior_blob_digest.as_str()) {
            return Err(format!(
                "{INVALID}: tombstone for {} contradicts its live membership",
                tombstone.path
            ));
        }
    }
    Ok(())
}

fn check_name(field: &str, value: &str) -> Result<(), String> {
    let bounded = !value.is_empty() && value.len() <= MAX_SCOPE_NAME_BYTES;
    if bounded && !value.chars().any(char::is_control) {
        return Ok(());
    }
    Err(format!(
        "{INVALID}: {field} must be a bounded printable name"
    ))
}

fn is_lower_hex(value: &str) -> bool {
    value
        .bytes()
        .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
}

fn check_git_object_id(value: &str) -> Result<(), String> {
    if matches!(value.len(), 40 | 64) && is_lower_hex(value) {
        return Ok(());
    }
    Err(format!(
        "{INVALID}: revision_id must be an immutable lowercase Git object id"
    ))
}

fn check_sha256(field: &str, value: &str) -> Result<(), String> {
    match value.strip_prefix(SHA256_PREFIX) {
        Some(hex) if hex.len() == 64 && is_lower_hex(hex) => Ok(()),
        _ => Err(format!(
            "{INVALID}: {field} must be sha256:<64 lowercase hex>"
        )),
    }
}

/// `sha256:<hex>` over the length-prefixed parts, so the key is injective.
fn scoped_key(parts: &[&str]) -> String {
    let mut digest = Sha256::new();
    for part in parts {
        digest.update((part.len() as u64).to_be_bytes());
        digest.update(part.as_bytes());
    }
    format!("{SHA256_PREFIX}{}", hex::encode(digest.finalize()))
}

/// `sha256:<hex>` of raw blob content — the `:Blob` key.
pub(super) fn content_digest(content: &[u8]) -> String {
    format!("{SHA256_PREFIX}{}", hex::encode(Sha256::digest(content)))
}

/// `:Blob` identity: content only. A symbol is a property of content, not of a
/// branch or a path, so every ref and path sharing the bytes shares this node.
pub(super) fn blob_node_id(digest: &str) -> String {
    format!("blob:{digest}")
}

/// `:FileVersion` identity: one repository path bound to one blob.
pub(super) fn file_version_id(repository_id: &str, path: &str, digest: &str) -> String {
    let key = scoped_key(&["eg:file-version", repository_id, path, digest]);
    format!("fileversion:{key}")
}

/// `:Branch` identity: one ref name in one repository (not its revision, which
/// moves while the branch stays the same entity).
pub(super) fn branch_node_id(repository_id: &str, ref_name: &str) -> String {
    let key = scoped_key(&["eg:branch", repository_id, ref_name]);
    format!("branch:{key}")
}
