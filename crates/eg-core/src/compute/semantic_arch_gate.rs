//! RF-RULING-007 layer (c): the call-graph gate.
//!
//! Layers (a) and (b) — no storage dependency edge from `eg-ann`, and
//! fail-closed admission — stop the *current* bypasses. This layer is what
//! stops them coming back: it scans the two regions the slice cleared and fails
//! on any physical-storage or filesystem construct, against an allowlist that
//! is deliberately **empty**. A marker string can be moved by extraction; a
//! source-level construct in a named region cannot hide from a scan of that
//! region.
//!
//! Stdlib only: no engine, no cargo metadata, no generated fixture. The gate
//! proves itself on a planted known-bad input in the same test, so a scanner
//! that silently matched nothing would fail rather than pass.

#![cfg(test)]

use std::path::{Path, PathBuf};

/// Constructs that would make either region a physical-storage or filesystem
/// authority again.
///
/// The redb entries name the types that ACQUIRE authority — a database, a
/// builder, a transaction — and the one call that mints a table declaration.
/// `redb::Table` and `redb::ReadOnlyTable` are deliberately absent: they are
/// the kernel's own return types (`AdmittedOwnerWrite::open_table` hands one
/// back), so forbidding the whole `redb::` namespace would forbid holding what
/// the kernel gives you and would force an allowlist — which is the thing this
/// gate must not have. `write_atomic` is named because it is what the deleted
/// `save_index` / `AnnIndex::save` / `persist::save` legs were called.
const FORBIDDEN: [&str; 12] = [
    "redb::Database",
    "redb::Builder",
    "redb::WriteTransaction",
    "redb::ReadTransaction",
    "Database::open",
    "Database::create",
    "TableDefinition::new",
    "begin_write",
    "begin_read",
    "File::create",
    "fs::write",
    "write_atomic",
];

/// Regions this gate owns, relative to the workspace root. Both were cleared by
/// this slice; nothing in either may reacquire a store.
const REGIONS: [&str; 2] = ["crates/eg-ann/src", "crates/eg-core/src/compute"];

/// Files exempted from the scan. **Empty, and it must stay empty** — an entry
/// here is a bypass with a comment on it. The one module that legitimately
/// names storage types, `compute/semantic_ann_codes*`, does so only through the
/// mutation kernel's typed handles and therefore contains none of the
/// constructs above.
///
/// This gate's own source is skipped by identity (`file!()`), not by an entry
/// here: it necessarily contains every forbidden literal, and a scanner that
/// scanned itself would be measuring its own vocabulary rather than the code.
const ALLOWLIST: [&str; 0] = [];

/// This module's own path, relative to the workspace root.
const SELF_PATH: &str = concat!("crates/eg-core/", file!());

/// `file!()` is relative to the workspace root under cargo, but relative to the
/// package under some invocations; normalise to the workspace-relative form the
/// scan produces.
fn self_path() -> String {
    let raw = file!().replace('\\', "/");
    if raw.starts_with("crates/") {
        raw
    } else {
        SELF_PATH.replace('\\', "/")
    }
}

fn workspace_root() -> PathBuf {
    // `crates/eg-core` -> the workspace root.
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("eg-core must live two levels below the workspace root")
        .to_path_buf()
}

fn rust_sources(dir: &Path, out: &mut Vec<PathBuf>) {
    let entries = std::fs::read_dir(dir)
        .unwrap_or_else(|error| panic!("arch gate cannot read {}: {error}", dir.display()));
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            rust_sources(&path, out);
        } else if path.extension().is_some_and(|ext| ext == "rs") {
            out.push(path);
        }
    }
}

/// Every forbidden construct in `source`, as `(line number, construct)`.
fn violations(source: &str) -> Vec<(usize, &'static str)> {
    let mut found = Vec::new();
    for (number, line) in source.lines().enumerate() {
        let code = line.trim_start();
        // A doc comment naming a retired construct is prose, not a call. The
        // scan is of code, and the retirement notes are the reason it exists.
        if code.starts_with("//") {
            continue;
        }
        for construct in FORBIDDEN {
            if code.contains(construct) {
                found.push((number + 1, construct));
            }
        }
    }
    found
}

#[test]
fn no_semantic_region_reacquires_a_store_or_a_file() {
    let root = workspace_root();
    let mut files = Vec::new();
    for region in REGIONS {
        rust_sources(&root.join(region), &mut files);
    }
    assert!(
        files.len() > 10,
        "the arch gate scanned only {} files -- it is not looking where it thinks it is",
        files.len()
    );

    let mut hits = Vec::new();
    for file in &files {
        let relative = file
            .strip_prefix(&root)
            .unwrap_or(file)
            .to_string_lossy()
            .replace('\\', "/");
        if ALLOWLIST.contains(&relative.as_str()) || relative == self_path() {
            continue;
        }
        let source = std::fs::read_to_string(file)
            .unwrap_or_else(|error| panic!("arch gate cannot read {relative}: {error}"));
        for (line, construct) in violations(&source) {
            hits.push(format!("{relative}:{line} uses `{construct}`"));
        }
    }
    assert!(
        hits.is_empty(),
        "semantic activation and purge must reach durable state only through the \
         mutation kernel; found:\n{}",
        hits.join("\n")
    );
    assert!(
        ALLOWLIST.is_empty(),
        "the arch-gate allowlist must stay empty"
    );
}

/// The gate proves itself: a planted known-bad source must be rejected, and a
/// clean one accepted. Without this, a scanner that matched nothing would pass
/// silently and look identical to a clean tree.
#[test]
fn the_arch_gate_catches_a_planted_bypass() {
    let planted = "\
use redb::Database;
fn reopen(path: &std::path::Path) -> Database {
    let db = Database::open(path).unwrap();
    let _ = db.begin_read();
    db
}
fn spill(path: &std::path::Path, bytes: &[u8]) {
    std::fs::write(path, bytes).unwrap();
}
";
    let found = violations(planted);
    for construct in ["redb::Database", "Database::open", "begin_read", "fs::write"] {
        assert!(
            found.iter().any(|(_, hit)| *hit == construct),
            "the gate missed `{construct}` in a planted bypass: {found:?}"
        );
    }

    let clean = "\
//! A retirement note may name `Database::open` in prose.
use eg_storage::ANN_CODES;
type Handle<'t> = redb::Table<'t, (&'static str, u64), &'static [u8]>;
fn put(rows: &mut Handle<'_>) {
    rows.insert((\"t\", 1), b\"x\".as_slice()).unwrap();
}
";
    assert!(
        violations(clean).is_empty(),
        "the gate flagged a clean source -- prose naming a retired construct and a \
         kernel-issued `redb::Table` handle are both permitted: {:?}",
        violations(clean)
    );
}
