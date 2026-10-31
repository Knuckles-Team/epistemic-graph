use crate::codec::encode_bounded;
use crate::owner::validate_manifest_write;
use crate::physical::binding::decode_binding;
use crate::physical::incarnation::{require_persisted_root, StoreIncarnation, STORE_ROOT_KEY};
use crate::physical::manifest::OwnerManifest;
use crate::tables::{OWNER_MANIFEST, SCOPE_BINDINGS, STORE_ROOT};
use redb::{ReadableTable, WriteTransaction};
use std::ops::Bound;
use std::path::Path;

pub(crate) fn rewrite_store_authority(
    wtx: &WriteTransaction,
    expected_root: &StoreIncarnation,
    expected_manifest: &OwnerManifest,
    adopted: &StoreIncarnation,
    manifest: &OwnerManifest,
) -> Result<(), String> {
    validate_source_authority(wtx, expected_root, expected_manifest)?;
    write_adopted_declarations(wtx, adopted, manifest)?;
    rebind_serving_scopes(wtx, adopted)
}

/// Reanchor a complete private staging image without changing its logical
/// owner declaration. The caller has already authenticated that exact
/// manifest and every source row under `expected_root`.
pub(crate) fn reanchor_staged_store_authority(
    wtx: &WriteTransaction,
    expected_root: &StoreIncarnation,
    expected_manifest: &OwnerManifest,
    adopted: &StoreIncarnation,
) -> Result<(), String> {
    validate_source_authority(wtx, expected_root, expected_manifest)?;
    write_adopted_root(wtx, adopted)?;
    rebind_serving_scopes(wtx, adopted)
}

fn validate_source_authority(
    wtx: &WriteTransaction,
    expected_root: &StoreIncarnation,
    expected_manifest: &OwnerManifest,
) -> Result<(), String> {
    let root = wtx
        .open_table(STORE_ROOT)
        .map_err(|error| error.to_string())?;
    if require_persisted_root(&root)? != *expected_root {
        return Err("recovery root changed before atomic adoption".to_string());
    }
    drop(root);
    let current = validate_manifest_write(
        wtx,
        &expected_manifest.physical_identity,
        expected_manifest.layout,
    )?;
    if current != *expected_manifest {
        return Err("recovery authority changed before atomic adoption".to_string());
    }
    Ok(())
}

fn write_adopted_declarations(
    wtx: &WriteTransaction,
    adopted: &StoreIncarnation,
    manifest: &OwnerManifest,
) -> Result<(), String> {
    write_adopted_root(wtx, adopted)?;
    let bytes = encode_bounded(manifest, "mutation owner manifest")?;
    wtx.open_table(OWNER_MANIFEST)
        .map_err(|error| error.to_string())?
        .insert("manifest", bytes.as_slice())
        .map_err(|error| error.to_string())?;
    Ok(())
}

fn write_adopted_root(wtx: &WriteTransaction, adopted: &StoreIncarnation) -> Result<(), String> {
    let bytes = encode_bounded(adopted, "mutation store root")?;
    wtx.open_table(STORE_ROOT)
        .map_err(|error| error.to_string())?
        .insert(STORE_ROOT_KEY, bytes.as_slice())
        .map_err(|error| error.to_string())?;
    Ok(())
}

/// Pin the range's key type. A bare `(Bound<&str>, Bound<&str>)` tuple satisfies both
/// `RangeBounds<str>` and `RangeBounds<&str>`, so redb's `range` cannot infer its key
/// parameter from it; naming the bound here resolves that without changing the range.
fn scope_key_range(lower: Bound<&str>) -> impl std::ops::RangeBounds<&str> + '_ {
    (lower, Bound::Unbounded)
}

fn rebind_serving_scopes(wtx: &WriteTransaction, adopted: &StoreIncarnation) -> Result<(), String> {
    let mut after: Option<String> = None;
    loop {
        let next = {
            let bindings = wtx
                .open_table(SCOPE_BINDINGS)
                .map_err(|error| error.to_string())?;
            // Bound to a local so the `Range` temporary is dropped before `bindings`.
            let row = bindings
                .range(scope_key_range(
                    after.as_deref().map_or(Bound::Unbounded, Bound::Excluded),
                ))
                .map_err(|error| error.to_string())?
                .next()
                .transpose()
                .map_err(|error| error.to_string())?
                .map(|(key, value)| (key.value().to_string(), value.value().to_vec()));
            row
        };
        let Some((key, bytes)) = next else {
            break;
        };
        let mut bindings = wtx
            .open_table(SCOPE_BINDINGS)
            .map_err(|error| error.to_string())?;
        let mut binding = decode_binding(&bytes)?;
        binding.store_identity_digest = adopted.identity_digest();
        let bytes = encode_bounded(&binding, "mutation scope binding")?;
        bindings
            .insert(key.as_str(), bytes.as_slice())
            .map_err(|error| error.to_string())?;
        after = Some(key);
    }
    Ok(())
}

/// Rebind a store that has been COPIED to a new path so it can be opened there.
///
/// A store's physical root is derived from its canonical path
/// (`StoreIncarnation::derive`), and the derived value is compared against the
/// one persisted inside the file on every open. That binding is deliberate: it
/// is what stops a byte copy of a store from being served as if it were the
/// original. The consequence is that a copy is UNOPENABLE at its new path until
/// its root is rewritten — which is not a limitation to work around but the
/// invariant working.
///
/// So a caller that legitimately made a copy has to say so, and this is how it
/// says it. Deliberately narrow and deliberately loud:
///
/// * it refuses unless the file currently carries the root of `copied_from`, so
///   it cannot be pointed at a store that is not the copy the caller believes;
/// * it rewrites the root to the one derived from the file's OWN path, so the
///   result is exactly what a natively-created store there would carry, never a
///   caller-supplied value;
/// * it rebinds the serving scopes in the same transaction, so no window exists
///   in which the root and the scopes disagree.
///
/// It is NOT a way to adopt an untrusted image — that is
/// [`crate::adopt_staged_mutation_store`], which additionally validates the
/// image against recorded evidence. Use this only for a copy this process just
/// made from a store it already trusted.
pub fn rebind_copied_store(path: &Path, copied_from: &Path) -> Result<(), String> {
    let (copied_from_root, _) = StoreIncarnation::derive(copied_from)?;
    let (adopted, _) = StoreIncarnation::derive(path)?;
    if adopted == copied_from_root {
        // Same canonical path: there is nothing to rebind, and silently
        // succeeding would hide a caller that copied a file onto itself.
        return Err(
            "rebind_copied_store was given one path twice: a copy at the same canonical path \
             is not a copy"
                .to_string(),
        );
    }
    let database = redb::Database::open(path).map_err(|error| error.to_string())?;
    let mut wtx = database.begin_write().map_err(|error| error.to_string())?;
    wtx.set_durability(redb::Durability::Immediate)
        .map_err(|error| error.to_string())?;
    {
        let root = wtx
            .open_table(STORE_ROOT)
            .map_err(|error| error.to_string())?;
        if require_persisted_root(&root)? != copied_from_root {
            return Err(
                "rebind_copied_store target does not carry the root of the store it was \
                 copied from"
                    .to_string(),
            );
        }
    }
    write_adopted_root(&wtx, &adopted)?;
    rebind_serving_scopes(&wtx, &adopted)?;
    wtx.commit().map_err(|error| error.to_string())
}
