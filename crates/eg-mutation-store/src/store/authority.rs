use super::*;
use std::ops::Bound;

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
