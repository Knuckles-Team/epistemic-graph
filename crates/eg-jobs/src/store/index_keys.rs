//! Secondary-index key derivation for the jobs store (CONCEPT:INT-P2-1).
//!
//! Every secondary index in `jobs.redb` is keyed by a hash rather than by the raw tenant
//! or capability string, so an index key is fixed-width and carries no user text. The
//! DOMAIN TAG is what makes those key spaces disjoint: the same input string hashes to a
//! different key in the tenant index than in the capability index, so a tenant named after
//! a capability can never collide with it.

/// The tenant index key for `tenant`.
pub(super) fn tenant_index_key(tenant: &str) -> String {
    index_key(b"eg-jobs.tenant-index.v1\0", tenant)
}

/// The capability index key for `token` — also used for the synthetic `pool:` / `region:`
/// placement tokens, which share the capability key space by construction.
pub(super) fn capability_index_key(token: &str) -> String {
    index_key(b"eg-jobs.capability-index.v1\0", token)
}

/// A stable secondary-index key: SHA-256 over the NUL-terminated `domain` tag followed by
/// `value`, hex-encoded. The domain tag is what keeps the index spaces above from
/// colliding on the same input string.
fn index_key(domain: &[u8], value: &str) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(domain);
    hasher.update(value.as_bytes());
    hex::encode(hasher.finalize())
}
