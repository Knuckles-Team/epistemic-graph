//! The engine method catalog, computed from the descriptor rows in code.
//!
//! `OperationReplayIdentity` binds a method's `SchemaId` and a digest of that
//! method's contract row, and the whole identity is bound to the catalog's own
//! digest. Both are pure functions of [`crate::method_descriptors`]: the Rust
//! descriptor rows are the one source of truth, there is no generated file and
//! nothing on disk to drift. A change to any descriptor moves its row digest and
//! the catalog digest, and therefore every replay identity minted under it.

use std::collections::BTreeMap;
use std::sync::OnceLock;

use eg_types::contract::Digest256;

struct Catalog {
    rows: BTreeMap<&'static str, (&'static str, [u8; 32])>,
    digest: String,
}

fn catalog() -> &'static Catalog {
    static CATALOG: OnceLock<Catalog> = OnceLock::new();
    CATALOG.get_or_init(|| {
        let mut rows = BTreeMap::new();
        for descriptor in crate::method_descriptors() {
            let id = descriptor.id.as_str();
            let schema_id: &'static str =
                Box::leak(format!("medulla:method/{id}").into_boxed_str());
            let row = Digest256::sha256(format!("{descriptor:?}").as_bytes());
            rows.insert(id, (schema_id, *row.as_bytes()));
        }
        let mut all = Vec::new();
        for (id, (schema_id, digest)) in &rows {
            all.extend_from_slice(id.as_bytes());
            all.push(0);
            all.extend_from_slice(schema_id.as_bytes());
            all.push(0);
            all.extend_from_slice(digest);
        }
        let digest = Digest256::sha256(&all).to_hex();
        Catalog { rows, digest }
    })
}

/// The one digest a consumer pins: every method row, in method-id order.
pub fn contract_catalog_digest() -> &'static str {
    catalog().digest.as_str()
}

/// The `SchemaId` and row digest of one method.
///
/// `None` means the id names no contract row at all, which the caller must fail
/// closed on: an identity cannot claim a method the contract does not declare.
pub fn method_schema(method_id: &str) -> Option<(&'static str, [u8; 32])> {
    catalog().rows.get(method_id).copied()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_descriptor_has_a_catalog_row() {
        for descriptor in crate::method_descriptors() {
            let (schema_id, _) = method_schema(descriptor.id.as_str())
                .unwrap_or_else(|| panic!("{} has no catalog row", descriptor.id.as_str()));
            assert_eq!(
                schema_id,
                format!("medulla:method/{}", descriptor.id.as_str())
            );
        }
    }

    #[test]
    fn an_undeclared_method_has_no_catalog_row() {
        assert!(method_schema("NoSuchMethod").is_none());
    }

    // spec: EG-TYPED-PACKS-R068
    #[test]
    fn the_catalog_digest_is_a_stable_sha256() {
        assert_eq!(contract_catalog_digest().len(), 64);
        assert_eq!(contract_catalog_digest(), contract_catalog_digest());
        assert!(Digest256::parse(contract_catalog_digest()).is_ok());
    }
}
