//! Regenerate `docs/operations/owner-store-formats.md` from the owner-store
//! layout lineage registry. The lineage test fails until the checked-in file
//! equals this output.

fn main() -> std::io::Result<()> {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../docs/operations/owner-store-formats.md");
    std::fs::write(&path, eg_storage::render_owner_store_formats())?;
    println!("wrote {}", path.display());
    Ok(())
}
