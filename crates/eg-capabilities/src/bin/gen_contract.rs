//! Regenerate — or verify — the canonical EG engine contract (RF-RULING-003).
//!
//! Run with:
//!   `cargo run -p eg-capabilities --features contract --bin gen_contract`
//!   `cargo run -p eg-capabilities --features contract --bin gen_contract -- --check`
//!
//! from the repository root, or with `-- --root <repository>`.
//!
//! This binary replaces `gen_ledger`: `contract/capabilities.generated.md` is now one of the
//! artifacts it renders, alongside `contract/methods.json`, `contract/schemas/*.json` and
//! `contract/receipt.json`. `--check` byte-diffs the committed tree and exits non-zero on
//! any drift, which is the whole of what the deleted Python parity gate used to assert.

use std::path::PathBuf;

/// The repository whose contract is written or checked: `--root <dir>`, else the
/// current directory (every caller -- hooks, CI, lanes -- runs from the repo root).
///
/// NOT `env!("CARGO_MANIFEST_DIR")`: that is baked in at COMPILE time, and cargo
/// does not rebuild a binary whose source merely moved. A reused binary therefore
/// wrote and `--check`ed a previous checkout's tree (a build-host run wrote its 49
/// artifacts into the previous run's directory, and `--check` passed against it).
fn repo_root(args: &[String]) -> Result<PathBuf, String> {
    let explicit = args
        .iter()
        .position(|arg| arg == "--root")
        .map(|at| args.get(at + 1).cloned().ok_or("--root needs a directory"))
        .transpose()?
        .map(PathBuf::from);
    let root = match explicit {
        Some(root) => root,
        None => std::env::current_dir().map_err(|e| format!("no current directory: {e}"))?,
    };
    if !root.join("crates/eg-capabilities/Cargo.toml").is_file() {
        return Err(format!(
            "{} is not the epistemic-graph repository root; run from it or pass --root",
            root.display()
        ));
    }
    Ok(root)
}

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let root = repo_root(&args).unwrap_or_else(|message| {
        eprintln!("gen_contract: {message}");
        std::process::exit(2);
    });
    if args.iter().any(|a| a == "--check") {
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
