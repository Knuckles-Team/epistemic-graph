//! RF-RULING-007 layer (c), the root binary's half of the call-graph gate.
//!
//! `crates/eg-core/src/compute/semantic_arch_gate.rs` owns the same scan over
//! the two CRATE regions the semantic slice cleared. It cannot own this one:
//! its regions are workspace-relative paths under `crates/`, and the root
//! binary's semantic surface lives in `src/`. This is that sibling, with the
//! identical forbidden set and the same deliberately EMPTY allowlist.
//!
//! The regions here are exactly the files the semantic slice cleared in the
//! root binary. The graph shard's own semantic legs (`src/redb_store.rs`,
//! `src/embedded/store.rs`, `src/server/persistence/redb_backend.rs`) are NOT
//! in scope: they are the graph shard, which still opens `redb` directly
//! because no table-owning `OwnerLayout` accepts a `MutationScope::Graph`, and
//! putting them in a region whose whole point is "nothing here holds a store"
//! would make this gate a lie with an allowlist attached.
//!
//! Stdlib only: no engine, no cargo metadata, no fixture. The gate proves
//! itself on a planted known-bad input in the same test, so a scanner that
//! silently matched nothing fails rather than passes.

use std::path::{Path, PathBuf};

/// Constructs that would make a region a physical-storage or filesystem
/// authority again. Identical to the crate-side gate's set.
///
/// `redb::Table`/`redb::ReadOnlyTable` are deliberately absent: they are what
/// the kernel HANDS BACK (`AdmittedOwnerWrite::open_table`), so forbidding the
/// whole `redb::` namespace would forbid holding what the kernel gives you and
/// would force an allowlist — the thing this gate must not have.
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

/// The root-binary regions the semantic slice cleared, relative to the package
/// root. Every durable leg in these is the mutation kernel's.
const REGIONS: [&str; 2] = [
    "src/server/semantic_activation.rs",
    "src/server/handlers/graph_ops/semantic.rs",
];

/// **Empty, and it must stay empty** — an entry here is a bypass with a comment
/// on it.
const ALLOWLIST: [&str; 0] = [];

fn package_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

fn rust_sources(path: &Path, out: &mut Vec<PathBuf>) {
    if path.is_dir() {
        let entries = std::fs::read_dir(path)
            .unwrap_or_else(|error| panic!("arch gate cannot read {}: {error}", path.display()));
        for entry in entries.flatten() {
            rust_sources(&entry.path(), out);
        }
    } else if path.extension().is_some_and(|ext| ext == "rs") {
        out.push(path.to_path_buf());
    }
}

/// Every forbidden construct in `source`, as `(line number, construct)`. A doc
/// comment naming a retired construct is prose, not a call: the scan is of
/// code, and the retirement notes are the reason it exists.
fn violations(source: &str, forbidden: &[&'static str]) -> Vec<(usize, &'static str)> {
    let mut found = Vec::new();
    for (number, line) in source.lines().enumerate() {
        let code = line.trim_start();
        if code.starts_with("//") {
            continue;
        }
        for construct in forbidden {
            if code.contains(construct) {
                found.push((number + 1, *construct));
            }
        }
    }
    found
}

fn region_sources() -> Vec<(PathBuf, String)> {
    let root = package_root();
    let mut files = Vec::new();
    for region in REGIONS {
        let path = root.join(region);
        assert!(
            path.exists(),
            "arch gate region {region} does not exist; regions must be corrected, not dropped"
        );
        rust_sources(&path, &mut files);
    }
    assert!(!files.is_empty(), "arch gate scanned no sources");
    files
        .into_iter()
        .map(|path| {
            let source = std::fs::read_to_string(&path).unwrap_or_else(|error| {
                panic!("arch gate cannot read {}: {error}", path.display())
            });
            (path, source)
        })
        .collect()
}

#[test]
fn no_root_semantic_region_reacquires_a_store_or_a_file() {
    assert!(
        ALLOWLIST.is_empty(),
        "the arch-gate allowlist must stay empty"
    );
    for (path, source) in region_sources() {
        let found = violations(&source, &FORBIDDEN);
        assert!(
            found.is_empty(),
            "{} reacquires physical storage: {found:?}",
            path.display()
        );
    }
    // The scanner must catch a known-bad input, or "no violations" is
    // indistinguishable from "matched nothing".
    let planted = "fn bad() { let db = redb::Database::create(path).unwrap(); }";
    assert_eq!(
        violations(planted, &FORBIDDEN)
            .into_iter()
            .map(|(_, construct)| construct)
            .collect::<Vec<_>>(),
        vec!["redb::Database", "Database::create"]
    );
}

/// The resident serving image is mutated by exactly ONE call —
/// `SemanticStore::adopt_generation`, which takes `&self` and sets
/// `STATE_READY` inside eg-core. An activation path that took
/// `core.semantic_store.write()` directly would install an index without the
/// dimension, member-set and embedding-space validation `adopt_generation`
/// performs, and nothing outside eg-core would notice.
///
/// This is scoped to the activation regions on purpose: the embedding arena's
/// seven in-memory serving mutations (`graph_delta.rs`, `write_coalescer.rs`,
/// `access.rs`, `mutation.rs`, `embedded.rs`) legitimately take that lock and
/// are not activation.
#[test]
fn root_semantic_activation_never_takes_the_resident_write_lock() {
    const FORBIDDEN_WRITE: [&str; 2] = ["semantic_store.write()", "semantic_store().write()"];
    for (path, source) in region_sources() {
        let found = violations(&source, &FORBIDDEN_WRITE);
        assert!(
            found.is_empty(),
            "{} mutates the resident semantic image outside adopt_generation: {found:?}",
            path.display()
        );
    }
    let planted = "fn bad(core: &GraphCore) { *core.semantic_store.write() = other; }";
    assert_eq!(
        violations(planted, &FORBIDDEN_WRITE)
            .into_iter()
            .map(|(_, construct)| construct)
            .collect::<Vec<_>>(),
        vec!["semantic_store.write()"]
    );
}
