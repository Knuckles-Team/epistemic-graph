//! EG-DURABLE-KERNEL-R007 — the host binary's `storage inspect`/`storage
//! upgrade` command surface, exercised through the real compiled binary.
//!
//! `src/operator_command.rs` and `src/server/persistence/store_upgrade.rs`
//! already carry library-level unit tests for the `inspect`/`apply`
//! functions. Nothing before this file spawned the actual
//! `epistemic-graph-server` executable and parsed real argv into the
//! `store-upgrade inspect`/`store-upgrade apply` subcommands, so the clap
//! wiring in `src/main.rs`/`src/operator_command.rs` (subcommand names, exit
//! status propagation) was untested at the CLI boundary R007 names.
#![cfg(all(feature = "server", feature = "security"))]

use std::process::{Command, Output};

fn server_bin() -> std::path::PathBuf {
    // `cargo test` defines this at compile time; `cargo check`/`clippy` may not, so
    // fall back to the binary cargo places next to this test's own executable.
    if let Some(path) = option_env!("CARGO_BIN_EXE_epistemic-graph-server") {
        return path.into();
    }
    let exe = std::env::current_exe().unwrap_or_default();
    exe.parent()
        .and_then(std::path::Path::parent)
        .unwrap_or_else(|| std::path::Path::new("."))
        .join("epistemic-graph-server")
}

fn run(args: &[&str]) -> Output {
    Command::new(server_bin())
        .args(args)
        .output()
        .unwrap_or_else(|error| panic!("spawn {} {args:?}: {error}", server_bin().display()))
}

#[test]
fn storage_inspect_reports_nothing_to_do_on_an_empty_data_directory() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let data_dir = data_dir.path().to_str().expect("utf8 tempdir path");

    let output = run(&["store-upgrade", "inspect", data_dir]);

    assert_eq!(
        output.status.code(),
        Some(0),
        "inspect on an empty data directory must report nothing to do; stderr: {}",
        String::from_utf8_lossy(&output.stderr),
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("\"command\":\"store-upgrade\""),
        "expected the single-line JSON summary in stdout, got: {stdout}"
    );
    assert!(
        stdout.contains("\"verb\":\"inspect\""),
        "expected the inspect verb recorded in the report: {stdout}"
    );
}

#[test]
fn storage_apply_without_confirm_is_refused_and_opens_nothing() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let data_dir = data_dir.path().to_str().expect("utf8 tempdir path");

    let output = run(&["store-upgrade", "apply", data_dir]);

    assert_ne!(
        output.status.code(),
        Some(0),
        "apply without --confirm must be refused, not silently accepted; stdout: {}",
        String::from_utf8_lossy(&output.stdout),
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("--confirm"),
        "the refusal must name the missing --confirm flag: {stderr}"
    );
}

#[test]
fn storage_apply_with_confirm_reports_nothing_to_do_on_an_empty_data_directory() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let data_dir = data_dir.path().to_str().expect("utf8 tempdir path");

    let output = run(&["store-upgrade", "apply", data_dir, "--confirm"]);

    assert_eq!(
        output.status.code(),
        Some(0),
        "apply --confirm on an empty data directory must report nothing to do; stderr: {}",
        String::from_utf8_lossy(&output.stderr),
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("\"verb\":\"apply\""),
        "expected the apply verb recorded in the report: {stdout}"
    );
}
