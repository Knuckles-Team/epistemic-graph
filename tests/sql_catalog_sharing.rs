//! External-crate structural guards for NE-003 (tenant-scoped SQL catalog
//! sharing + grants + row-level security).
//!
//! The actual access-control behavior this track implements — ownership, grants,
//! revoke, row-level security, migration, cross-tenant isolation, restart
//! durability — is exercised by `#[cfg(test)] mod tests` INSIDE
//! `src/server/sql_catalog_acl.rs`, not here. That is not a style choice: every
//! type this feature touches (`CarrierAuthority`, `sql_tables::user_table_store`,
//! `sql_catalog_acl::*`) is `pub(crate)` inside `pub(crate) mod access` /
//! `pub(crate) mod sql_catalog_acl`, so an EXTERNAL integration test in `tests/`
//! (which only sees this crate's public API) cannot name them at all — the exact
//! same reason `sql_tables.rs`'s own pre-existing catalog-isolation tests
//! (`tenant_and_actor_receive_distinct_opaque_catalogs`, etc.) already live inside
//! the module rather than under `tests/`.
//!
//! What DOES belong here, and IS genuinely external: a source-level regression
//! guard on the one hard invariant this track was told never to violate — that
//! `check_graph_access` runs strictly before every dispatch-reachable handler
//! that can now reach `sql_catalog_acl`, inside `dispatch_graph_op_inner`, so
//! nothing this track adds can reach table access without passing that gate.
//!
//! **Correction (this track's gap-closure pass):** NE-003 is no longer unwired.
//! `sql_catalog_acl` now has THREE live callers — `src/server/wire/mod.rs`
//! (pgwire/mysql/mssql/sqlite-wire and every other `WireSession` adapter),
//! `src/server/handlers/sqlite_file.rs`, and `src/server/handlers/rdf.rs`
//! (`Method::SparqlVirtual` → `handle_sparql_virtual` →
//! `sql_catalog_acl::open_authorized_table`, per that file's own CONCEPT:NE-046
//! comment). Of those, `Method::SparqlVirtual` is reached through
//! `handlers::rdf::try_handle`, which — like `handlers::query::try_handle` — is
//! reached from `dispatch_graph_op_inner`, so the guard below checks ordering
//! against BOTH. `handlers::sqlite_file::try_handle` is instead called from the
//! sibling function `dispatch_inner`, so it falls outside this guard's bound;
//! that remains a known, disclosed gap rather than coverage silently claimed.
//!
//! **Correction (eg-f3 burndown, 2026-09-11):** the guard used to read a single
//! hard-coded path (`src/server/dispatch.rs`) and to bound itself to the literal
//! body of `dispatch_graph_op_inner`, asserting a byte-offset ordering between
//! `check_graph_access(` and the two `try_handle(` calls INSIDE that one
//! function. The dispatch pipeline has since been decomposed: the function moved
//! to `src/server/dispatch/graph_pipeline.rs`, the access check moved down into
//! `gate_graph_op_under_lock` → `check_graph_op_access` → `check_graph_access`,
//! and the two handler calls moved out into `route_query_gateway` /
//! `route_rdf_gateway` reached through `route_graph_op_method`. The old guard did
//! not merely weaken under that move — its very first `.expect` blew up
//! ("dispatch_graph_op_inner must exist in dispatch.rs"), because the property it
//! keyed on was a byte offset in a named file rather than the call graph it meant
//! to describe. It is rewritten below to LOCATE the defining file and to follow
//! the chain, so the same decomposition cannot blind it again: every link it
//! asserts is a call that must exist, in an order that must hold, and it fails if
//! any one of them is removed or reordered.

use std::fs;
use std::path::{Path, PathBuf};

/// Every file the graph-op dispatch pipeline may define its entry point in.
///
/// A list, not a constant path: the previous version of this guard named
/// `src/server/dispatch.rs` and silently stopped describing anything the day the
/// function moved into the `dispatch/` submodule tree.
fn dispatch_sources() -> Vec<PathBuf> {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src/server");
    let mut files = vec![root.join("dispatch.rs")];
    let mut stack = vec![root.join("dispatch")];
    while let Some(dir) = stack.pop() {
        let Ok(entries) = fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
            } else if path.extension().is_some_and(|ext| ext == "rs") {
                files.push(path);
            }
        }
    }
    files.sort();
    files
}

/// The one dispatch source that defines `signature`, and its text.
///
/// Fails loudly when the definition is missing or duplicated, so a move is a
/// RED test rather than a guard that quietly measures nothing.
fn defining_source(signature: &str) -> (PathBuf, String) {
    let mut found: Vec<(PathBuf, String)> = Vec::new();
    for path in dispatch_sources() {
        let Ok(text) = fs::read_to_string(&path) else {
            continue;
        };
        if definition_offset(&text, signature).is_some() {
            found.push((path, text));
        }
    }
    assert_eq!(
        found.len(),
        1,
        "exactly one dispatch source must define `{signature}`, found {:?}",
        found.iter().map(|(p, _)| p.clone()).collect::<Vec<_>>()
    );
    found.pop().expect("checked above")
}

/// Offset of the DEFINITION `signature` introduces — it must start at column 0,
/// so a doc comment or a call site that merely names the function is not
/// mistaken for it.
fn definition_offset(source: &str, signature: &str) -> Option<usize> {
    if source.starts_with(signature) {
        return Some(0);
    }
    source
        .match_indices(&format!("\n{signature}"))
        .map(|(offset, _)| offset + 1)
        .next()
}

/// The body text of the item introduced by `signature`, bounded by the next
/// column-0 item in the same file.
fn item_body<'a>(source: &'a str, signature: &str, path: &Path) -> &'a str {
    let start = definition_offset(source, signature)
        .unwrap_or_else(|| panic!("`{signature}` must be defined in {}", path.display()));
    let after = &source[start + signature.len()..];
    let end = [
        "\nfn ",
        "\nasync fn ",
        "\npub fn ",
        "\npub async fn ",
        "\npub(crate) fn ",
        "\npub(crate) async fn ",
    ]
    .iter()
    .filter_map(|marker| after.find(marker))
    .min()
    .unwrap_or(after.len());
    &after[..end]
}

/// `check_graph_access` must run strictly BEFORE every dispatch-reachable
/// `handlers::query::try_handle` / `handlers::rdf::try_handle` — the ordering
/// this track was explicitly told to preserve, now covering NE-003's actual live
/// dispatch-reachable entry point (`handlers::rdf::try_handle` →
/// `Method::SparqlVirtual` → `sql_catalog_acl::open_authorized_table`) as well as
/// the pre-existing `handlers::query::try_handle` path.
///
/// The pipeline is decomposed, so the guard follows the chain instead of a byte
/// offset inside one function body:
///
/// 1. `dispatch_graph_op_inner` calls `gate_graph_op_under_lock` BEFORE it calls
///    `route_graph_op_method`, and calls neither handler itself;
/// 2. `gate_graph_op_under_lock` calls `check_graph_op_access`;
/// 3. `check_graph_op_access` calls `check_graph_access`;
/// 4. the ONLY call sites of the two handlers in the whole dispatch tree are
///    inside `route_query_gateway` / `route_rdf_gateway`, which are reachable
///    only through `route_graph_op_method` — i.e. only after link 1's gate.
///
/// Remove any one of those calls, or route before gating, and this test fails.
#[test]
fn check_graph_access_precedes_query_and_rdf_try_handle_in_dispatch() {
    let (path, source) = defining_source("async fn dispatch_graph_op_inner");
    let entry = item_body(&source, "async fn dispatch_graph_op_inner", &path);

    // (1) gate before route, inside the entry point itself.
    let gate_offset = entry.find("gate_graph_op_under_lock(").unwrap_or_else(|| {
        panic!(
            "dispatch_graph_op_inner must take the access gate via \
             gate_graph_op_under_lock in {}",
            path.display()
        )
    });
    let route_offset = entry.find("route_graph_op_method(").unwrap_or_else(|| {
        panic!(
            "dispatch_graph_op_inner must route methods via route_graph_op_method in {}",
            path.display()
        )
    });
    assert!(
        gate_offset < route_offset,
        "the access gate must run BEFORE method routing inside \
         dispatch_graph_op_inner — this ordering is the gate every table access \
         (SQL included, via NE-003's sql_catalog_acl) must pass through"
    );
    for callee in ["handlers::query::try_handle(", "handlers::rdf::try_handle("] {
        assert!(
            !entry.contains(callee),
            "dispatch_graph_op_inner must not call {callee} directly — it is \
             reachable only through the routed gateways, after the access gate"
        );
    }

    // (2) + (3) the gate really reaches check_graph_access.
    let (gate_path, gate_source) = defining_source("fn gate_graph_op_under_lock");
    assert!(
        item_body(&gate_source, "fn gate_graph_op_under_lock", &gate_path)
            .contains("check_graph_op_access("),
        "gate_graph_op_under_lock must call check_graph_op_access — without it the \
         gate this test names does not gate anything"
    );
    let (access_path, access_source) = defining_source("fn check_graph_op_access");
    assert!(
        item_body(&access_source, "fn check_graph_op_access", &access_path)
            .contains("check_graph_access("),
        "check_graph_op_access must call check_graph_access — without it the whole \
         chain this test follows terminates in nothing"
    );

    // (4) the two handlers have no dispatch call site outside the routed gateways.
    for (callee, gateway) in [
        (
            "handlers::query::try_handle(",
            "async fn route_query_gateway",
        ),
        ("handlers::rdf::try_handle(", "async fn route_rdf_gateway"),
    ] {
        let (gateway_path, gateway_source) = defining_source(gateway);
        let inside = item_body(&gateway_source, gateway, &gateway_path)
            .matches(callee)
            .count();
        assert!(
            inside > 0,
            "{gateway} must call {callee} at least once (this guard is \
             meaningless if that call moved elsewhere)"
        );
        let total: usize = dispatch_sources()
            .iter()
            .filter_map(|path| fs::read_to_string(path).ok())
            .map(|text| text.matches(callee).count())
            .sum();
        assert_eq!(
            total, inside,
            "every {callee} call site in the dispatch tree must live inside \
             {gateway}; a call from anywhere else would reach a handler without \
             passing the access gate"
        );
    }

    // (5) and the gateways themselves are never invoked ahead of the gate: any
    // call to one from inside the entry point must sit after `gate_offset`, so
    // (1)-(5) compose into the ordering this guard claims.
    for gateway_call in ["route_query_gateway(", "route_rdf_gateway("] {
        for (offset, _) in entry.match_indices(gateway_call) {
            assert!(
                gate_offset < offset,
                "{gateway_call} must not be reached before the access gate inside \
                 dispatch_graph_op_inner"
            );
        }
    }
}

/// NE-003's new capability module exists, is crate-private (never a `pub` item a
/// stray caller outside `src/server/` could reach without going through the
/// intended entry points), and is gated the same as `sql_tables` — a lightweight
/// guard against an accidental visibility widening or feature-gate drift.
#[test]
fn sql_catalog_acl_module_is_declared_crate_private_and_query_gated() {
    let mod_rs = fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/src/server/mod.rs"))
        .expect("src/server/mod.rs must exist and be readable");
    assert!(
        mod_rs.contains("pub(crate) mod sql_catalog_acl;"),
        "sql_catalog_acl must be declared pub(crate), not pub — its access-control \
         primitives must only be reachable from inside src/server/, not the crate's \
         public API"
    );
    let acl_src = fs::read_to_string(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/src/server/sql_catalog_acl.rs"
    ))
    .expect("src/server/sql_catalog_acl.rs must exist and be readable");
    assert!(
        !acl_src.contains("\npub fn ") && !acl_src.contains("\npub struct "),
        "sql_catalog_acl must expose only pub(crate) items — a bare `pub` item here \
         would leak past the module boundary this test just checked"
    );
}
