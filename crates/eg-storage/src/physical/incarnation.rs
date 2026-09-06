use crate::codec::decode_ledger_record;
use eg_types::IncarnationId;
use serde::{Deserialize, Deserializer, Serialize};
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};

#[cfg(unix)]
use std::os::unix::fs::MetadataExt;

/// Schema version of the physical storage-kernel root declaration, which is
/// also the durable ledger format identity: it is folded into
/// [`StoreIncarnation`]'s identity digest and stamped on every scope binding,
/// so a file written under an earlier format fails to open rather than being
/// reinterpreted.
///
/// * v1 -- ledger rows were keyed by the scope *identity* digest while scope
///   bindings and versions were keyed by the *binding* digest, so no committed
///   row could ever resolve its own binding.
/// * v2 -- one ledger scope key ([`crate::ledger_scope_key`], the binding
///   digest) keys every ledger table, and every ledger row carries the exact
///   [`eg_types::MutationScopeIdentity`] it was written under. Greenfield
///   format, no migration.
pub const STORAGE_KERNEL_SCHEMA_VERSION: u16 = 2;
pub(crate) const STORE_ROOT_KEY: &str = "root";
const STORE_DIGEST_DOMAIN: &[u8] = b"eg/mutation-store-root/v1\0";
const PHYSICAL_ROOT_DOMAIN: &[u8] = b"eg/mutation-store-physical-root/v1\0";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct StoreIdentityDigest([u8; 32]);

impl StoreIdentityDigest {
    pub(crate) fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

/// Immutable physical identity of one common mutation-store database.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct StoreIncarnation {
    schema_version: u16,
    root_id: IncarnationId,
    identity_digest: StoreIdentityDigest,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct StoreIncarnationWire {
    schema_version: u16,
    root_id: IncarnationId,
    identity_digest: StoreIdentityDigest,
}

impl StoreIncarnation {
    pub(crate) fn derive(path: &Path) -> Result<(Self, PathBuf), String> {
        let canonical = std::fs::canonicalize(path).map_err(|error| error.to_string())?;
        let root_id = physical_root_id(&canonical)?;
        let identity_digest = store_digest(STORAGE_KERNEL_SCHEMA_VERSION, &root_id);
        Ok((
            Self {
                schema_version: STORAGE_KERNEL_SCHEMA_VERSION,
                root_id,
                identity_digest,
            },
            canonical,
        ))
    }

    pub fn identity_digest(&self) -> StoreIdentityDigest {
        self.identity_digest
    }

    pub fn validate_digest(&self) -> Result<(), String> {
        if self.schema_version != STORAGE_KERNEL_SCHEMA_VERSION {
            return Err(format!(
                "unsupported mutation store schema {} (expected {})",
                self.schema_version, STORAGE_KERNEL_SCHEMA_VERSION
            ));
        }
        if self.identity_digest != store_digest(self.schema_version, &self.root_id) {
            return Err("mutation store root digest mismatch".to_string());
        }
        Ok(())
    }

    fn from_wire(wire: StoreIncarnationWire) -> Result<Self, String> {
        let incarnation = Self {
            schema_version: wire.schema_version,
            root_id: wire.root_id,
            identity_digest: wire.identity_digest,
        };
        incarnation.validate_digest()?;
        Ok(incarnation)
    }
}

impl<'de> Deserialize<'de> for StoreIncarnation {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = StoreIncarnationWire::deserialize(deserializer)?;
        Self::from_wire(wire).map_err(serde::de::Error::custom)
    }
}

fn decode_incarnation(bytes: &[u8]) -> Result<StoreIncarnation, String> {
    let wire: StoreIncarnationWire = decode_ledger_record(bytes)?;
    StoreIncarnation::from_wire(wire)
}

pub(crate) fn persisted_root<T>(table: &T) -> Result<Option<StoreIncarnation>, String>
where
    T: redb::ReadableTable<&'static str, &'static [u8]>,
{
    let mut rows = table.iter().map_err(|error| error.to_string())?;
    let Some(first) = rows.next() else {
        return Ok(None);
    };
    let (key, value) = first.map_err(|error| error.to_string())?;
    let has_extra_row = rows
        .next()
        .transpose()
        .map_err(|error| error.to_string())?
        .is_some();
    if key.value() != STORE_ROOT_KEY || has_extra_row {
        return Err("mutation store must contain exactly one canonical root".to_string());
    }
    decode_incarnation(value.value()).map(Some)
}

pub(crate) fn require_persisted_root<T>(table: &T) -> Result<StoreIncarnation, String>
where
    T: redb::ReadableTable<&'static str, &'static [u8]>,
{
    persisted_root(table)?.ok_or_else(|| "mutation store root is missing".to_string())
}

fn store_digest(schema_version: u16, root_id: &IncarnationId) -> StoreIdentityDigest {
    let mut hasher = Sha256::new();
    hasher.update(STORE_DIGEST_DOMAIN);
    hasher.update(schema_version.to_be_bytes());
    let bytes = root_id.as_str().as_bytes();
    let length = u32::try_from(bytes.len()).expect("validated store root identity fits LP32");
    hasher.update(length.to_be_bytes());
    hasher.update(bytes);
    StoreIdentityDigest(hasher.finalize().into())
}

// NOTE: deliberately does NOT hash `path`. A store's physical identity is the
// device+inode it lives on, not the path string used to reach it: a store
// legitimately moves between a private staging path and its published
// location (a backup bundle) or gets copied to a fresh path (a restore),
// without becoming a different store. Binding identity to a location made
// "the same bytes at a different path" indistinguishable from "a substituted
// file" -- and only the second is the attack this control exists to stop
// (SEC-FINDING-V1-INCARNATION-BREAKS-RESTORE-20260903). `(dev, ino)` alone
// still detects a live store file being swapped out from under an open
// handle at its OWN unchanged path, which is the control's actual job.
#[cfg(unix)]
fn physical_root_id(path: &Path) -> Result<IncarnationId, String> {
    let metadata = std::fs::metadata(path).map_err(|error| error.to_string())?;
    if !metadata.is_file() {
        return Err("mutation store physical root is not a regular file".to_string());
    }
    let mut hasher = Sha256::new();
    hasher.update(PHYSICAL_ROOT_DOMAIN);
    hasher.update(metadata.dev().to_be_bytes());
    hasher.update(metadata.ino().to_be_bytes());
    IncarnationId::new(format!("physical:sha256:{}", lower_hex(hasher.finalize())))
}

#[cfg(not(unix))]
fn physical_root_id(_path: &Path) -> Result<IncarnationId, String> {
    Err("physical mutation-store identity is unsupported on this platform".to_string())
}

fn lower_hex(bytes: impl AsRef<[u8]>) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let bytes = bytes.as_ref();
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        encoded.push(DIGITS[(byte >> 4) as usize] as char);
        encoded.push(DIGITS[(byte & 0x0f) as usize] as char);
    }
    encoded
}
