//! Proves the crate-local `clippy.toml` actually is what its own header
//! comment claims: a superset of the workspace `clippy.toml` (clippy reads
//! only the nearest file and does not merge), plus a ban on every `f64`/`f32`
//! transcendental the deterministic statistics modules must never call
//! directly.

use std::collections::BTreeSet;
use std::fs;

/// Every `path = "..."` value inside a `disallowed-methods` table, extracted
/// without a TOML parser: the format is fixed (one quoted path per entry) and
/// this test only ever reads files inside this repository.
fn disallowed_method_paths(toml_text: &str) -> BTreeSet<String> {
    let marker = "path = \"";
    let mut paths = BTreeSet::new();
    let mut rest = toml_text;
    while let Some(start) = rest.find(marker) {
        let after = &rest[start + marker.len()..];
        let end = after.find('"').expect("clippy.toml: unterminated path string");
        paths.insert(after[..end].to_string());
        rest = &after[end + 1..];
    }
    paths
}

fn read(relative: &str) -> String {
    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    fs::read_to_string(format!("{manifest_dir}/{relative}"))
        .unwrap_or_else(|error| panic!("reading {relative} from {manifest_dir}: {error}"))
}

#[test]
fn crate_clippy_policy_is_a_superset_of_the_workspace_policy() {
    let workspace_paths = disallowed_method_paths(&read("../../clippy.toml"));
    let crate_paths = disallowed_method_paths(&read("clippy.toml"));
    assert!(!workspace_paths.is_empty(), "sanity: the workspace clippy.toml lists disallowed methods");
    let missing: Vec<&String> = workspace_paths.difference(&crate_paths).collect();
    assert!(
        missing.is_empty(),
        "crate clippy.toml is missing workspace disallowed-methods entries: {missing:?}"
    );
}

#[test]
fn crate_clippy_policy_bans_every_float_transcendental() {
    let crate_paths = disallowed_method_paths(&read("clippy.toml"));
    let methods = [
        "acos", "acosh", "asin", "asinh", "atan", "atan2", "atanh", "cbrt", "cos", "cosh", "exp", "exp2", "exp_m1",
        "hypot", "ln", "ln_1p", "log", "log10", "log2", "mul_add", "powf", "powi", "sin", "sin_cos", "sinh", "tan",
        "tanh",
    ];
    for base in ["f64", "f32"] {
        for method in methods {
            let full = format!("{base}::{method}");
            assert!(crate_paths.contains(&full), "crate clippy.toml is missing {full}");
        }
    }
    // Every entry must carry a reason pointing at the pinned replacement, so a
    // future entry cannot ban a method silently.
    let toml = read("clippy.toml");
    let reasons = toml.matches("reason = ").count();
    let paths = toml.matches("path = \"").count();
    assert_eq!(reasons, paths, "every disallowed-methods entry needs a reason");
}
