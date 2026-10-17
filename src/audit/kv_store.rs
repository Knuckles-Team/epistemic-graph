//! Audit lines for the served namespaced key-value surface (EG-DURABLE-KERNEL-R102).

use crate::protocol::Method;

/// KV mutations. A key is arbitrary caller bytes, so the line carries only a digest
/// of the caller's `(namespace, key)`, never the key or the value.
#[cfg(feature = "kv")]
pub(super) fn audit_line(method: &Method) -> Option<String> {
    use sha2::{Digest, Sha256};
    let (event, namespace, key) = match method {
        Method::KvPut { namespace, key, .. } => ("KV_PUT", namespace, key),
        Method::KvDelete { namespace, key } => ("KV_DELETE", namespace, key),
        Method::KvCas { namespace, key, .. } => ("KV_CAS", namespace, key),
        _ => return None,
    };
    let mut digest = Sha256::new();
    digest.update(namespace.as_bytes());
    digest.update([0]);
    digest.update(key.as_bytes());
    Some(format!("{event}|sha256:{}", hex::encode(digest.finalize())))
}

/// A build without the `kv` feature has no KV methods to audit.
#[cfg(not(feature = "kv"))]
pub(super) fn audit_line(_method: &Method) -> Option<String> {
    None
}
