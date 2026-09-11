//! The engine contract, as a compile-time table.
//!
//! `OperationReplayIdentity` binds a method's `SchemaId` and the digest of that
//! method's request schema, and the whole identity is bound to the contract
//! catalog's own digest. Both facts exist today only as JSON on disk under
//! `contract/`, and the admission path may not do file I/O to read them: an
//! identity minted from a file read would be a second source of truth for the
//! contract, and a batch whose identity depends on a file that a deployment can
//! edit is not an identity at all.
//!
//! So `gen_contract` emits them here, and this module is the only reader. The
//! generated files are committed and included unconditionally, because a default
//! build (which has neither `schemars` nor the generator) still admits mutations
//! and therefore still needs the table.
//!
//! # Staleness
//!
//! `gen_contract --check` byte-diffs both generated files against what the
//! descriptor rows and the `Method` schema would produce right now, exactly as
//! it does for `contract/methods.json`. A stale table is therefore a red gate,
//! not a silent divergence -- and `catalog_is_current` pins the table against the
//! committed receipt so the two can never drift apart in the same commit.

// `METHOD_CATALOG`: `(MethodId, SchemaId, request-schema digest)` for every
// contract row, sorted by method id so a binary search is valid and the file is
// stable. Its own doc comment is generated with it.
include!("../generated/method_catalog.rs");

// `CONTRACT_CATALOG_DIGEST`: the one digest a consumer pins,
// `contract/receipt.json`'s `contract_digest`. Deliberately NOT inside
// `METHOD_CATALOG`'s file: the receipt digests every generated artifact, so a
// digest that lives inside a digested artifact would have no fixpoint. It is
// written the way the receipt itself is -- last, and outside
// `artifact_digests`.
include!("../generated/catalog_digest.rs");

/// The `SchemaId` and request-schema digest of one method.
///
/// `None` means the id names no contract row at all, which the caller must fail
/// closed on: an identity cannot claim a method the contract does not declare.
pub fn method_schema(method_id: &str) -> Option<(&'static str, [u8; 32])> {
    METHOD_CATALOG
        .binary_search_by(|(id, _, _)| (*id).cmp(method_id))
        .ok()
        .map(|index| {
            let (_, schema_id, digest) = METHOD_CATALOG[index];
            (schema_id, digest)
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_catalog_is_sorted_unique_and_complete() {
        assert_eq!(METHOD_CATALOG.len(), crate::method_descriptors().count());
        for pair in METHOD_CATALOG.windows(2) {
            assert!(pair[0].0 < pair[1].0, "{} !< {}", pair[0].0, pair[1].0);
        }
        for descriptor in crate::method_descriptors() {
            let (schema_id, _) = method_schema(descriptor.id.as_str())
                .unwrap_or_else(|| panic!("{} has no catalog row", descriptor.id.as_str()));
            assert_eq!(
                schema_id,
                format!(
                    "contract/schemas/method.request.json#/methods/{}",
                    descriptor.id.as_str()
                )
            );
        }
    }

    #[test]
    fn an_undeclared_method_has_no_catalog_row() {
        assert!(method_schema("NoSuchMethod").is_none());
    }

    /// The catalog digest is the receipt's, byte for byte. A table generated
    /// under a different contract than the receipt records is exactly the
    /// staleness this constant exists to make impossible.
    #[test]
    fn the_catalog_digest_is_the_committed_receipt_digest() {
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .and_then(|path| path.parent())
            .expect("crates/eg-capabilities is two levels below the repo root")
            .to_path_buf();
        let receipt = std::fs::read_to_string(root.join("contract/receipt.json"))
            .expect("the committed receipt is readable");
        let recorded = receipt
            .split_once("\"contract_digest\": \"")
            .expect("the receipt declares a contract digest")
            .1
            .split_once('"')
            .expect("the contract digest is a JSON string")
            .0;
        assert_eq!(recorded, CONTRACT_CATALOG_DIGEST);
    }
}
