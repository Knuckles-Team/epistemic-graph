//! Regenerate `docs/capabilities.generated.md` from the [`eg_capabilities::policy`] table.
//!
//! Run with: `cargo run -p eg-capabilities --features jobs,knowledge-batch,modality-serving --bin gen_ledger`
//!
//! `tests/consistency.rs`'s `generated_ledger_is_not_stale` test fails CI-style if this
//! file's checked-in content doesn't match what this binary would (re)generate, so the
//! ledger can never silently drift from the policy table it's rendered from.

use std::path::PathBuf;

fn main() {
    let ledger = eg_capabilities::gen_ledger();
    // `crates/eg-capabilities/` -> repo root is two levels up.
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|p| p.parent())
        .expect("crates/eg-capabilities should be two levels below the repo root")
        .to_path_buf();
    let out_path = root.join("docs").join("capabilities.generated.md");
    std::fs::write(&out_path, &ledger)
        .unwrap_or_else(|e| panic!("failed to write {}: {e}", out_path.display()));
    eprintln!("wrote {}", out_path.display());
}
