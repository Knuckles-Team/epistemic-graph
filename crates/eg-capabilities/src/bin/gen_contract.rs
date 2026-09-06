//! Regenerate — or verify — the canonical EG engine contract (RF-RULING-003).
//!
//! Run with:
//!   `cargo run -p eg-capabilities --features contract --bin gen_contract`
//!   `cargo run -p eg-capabilities --features contract --bin gen_contract -- --check`
//!
//! This binary replaces `gen_ledger`: `docs/capabilities.generated.md` is now one of the
//! artifacts it renders, alongside `contract/methods.json`, `contract/schemas/*.json` and
//! `contract/receipt.json`. `--check` byte-diffs the committed tree and exits non-zero on
//! any drift, which is the whole of what the deleted Python parity gate used to assert.

use std::path::PathBuf;

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|p| p.parent())
        .expect("crates/eg-capabilities is two levels below the repo root")
        .to_path_buf()
}

fn main() {
    let root = repo_root();
    if std::env::args().any(|a| a == "--check") {
        match eg_capabilities::contract::check(&root) {
            Ok(count) => eprintln!("contract is current: {count} artifacts match"),
            Err(drift) => {
                eprintln!(
                    "engine contract is STALE -- regenerate with `cargo run -p eg-capabilities --features contract --bin gen_contract`:"
                );
                for line in drift {
                    eprintln!("  {line}");
                }
                std::process::exit(1);
            }
        }
        return;
    }
    match eg_capabilities::contract::write_all(&root) {
        Ok(count) => eprintln!("wrote {count} contract artifacts under {}", root.display()),
        Err(e) => {
            eprintln!("failed to write the engine contract: {e}");
            std::process::exit(1);
        }
    }
}
