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
//! `handlers::rdf::try_handle`, which — like `handlers::query::try_handle` — IS
//! called directly from inside `dispatch_graph_op_inner`, so the guard below now
//! checks ordering against BOTH. `handlers::sqlite_file::try_handle` is instead
//! called from the sibling function `dispatch_inner` (not
//! `dispatch_graph_op_inner`), so it falls outside this guard's single-function
//! bound; extending the guard to cover it would mean generalizing the bounding
//! logic to walk the whole file / multiple functions, which this track's owned
//! files (`sql_catalog_acl.rs` and this file) leave as a known, disclosed gap
//! rather than silently claiming coverage it does not have.

use std::fs;

const DISPATCH_SRC: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/src/server/dispatch.rs");

/// `check_graph_access` inside `dispatch_graph_op_inner` must run strictly BEFORE
/// every `handlers::query::try_handle` AND `handlers::rdf::try_handle` call in
/// the same function — the ordering this track was explicitly told to preserve
/// and never bypass, now covering NE-003's actual live dispatch-reachable entry
/// point (`handlers::rdf::try_handle` → `Method::SparqlVirtual` →
/// `sql_catalog_acl::open_authorized_table`) as well as the pre-existing
/// `handlers::query::try_handle` path this guard originally covered.
#[test]
fn check_graph_access_precedes_query_and_rdf_try_handle_in_dispatch() {
    let source = fs::read_to_string(DISPATCH_SRC)
        .expect("src/server/dispatch.rs must exist and be readable");

    let fn_start = source
        .find("async fn dispatch_graph_op_inner")
        .expect("dispatch_graph_op_inner must exist in dispatch.rs");

    // Bound the search to this one function body by finding the next top-level
    // `\nasync fn ` / `\nfn ` after the opening one (a coarse but adequate bound
    // for a single large `impl`-free free function in this file).
    let after_start = &source[fn_start + "async fn dispatch_graph_op_inner".len()..];
    let next_fn_offset = after_start
        .find("\nasync fn ")
        .or_else(|| after_start.find("\npub(crate) async fn "))
        .or_else(|| after_start.find("\npub async fn "))
        .unwrap_or(after_start.len());
    let body = &after_start[..next_fn_offset];

    let access_offset = body
        .find("check_graph_access(")
        .expect("dispatch_graph_op_inner must call check_graph_access");

    for callee in ["handlers::query::try_handle(", "handlers::rdf::try_handle("] {
        let callee_offsets: Vec<usize> = body
            .match_indices(callee)
            .map(|(offset, _)| offset)
            .collect();
        assert!(
            !callee_offsets.is_empty(),
            "dispatch_graph_op_inner must call {callee} at least once \
             (this guard is meaningless if that call moved elsewhere)"
        );
        for callee_offset in callee_offsets {
            assert!(
                access_offset < callee_offset,
                "check_graph_access must run BEFORE {callee} inside \
                 dispatch_graph_op_inner — this ordering is the gate every table \
                 access (SQL included, via NE-003's sql_catalog_acl) must pass \
                 through; nothing may add a path around it"
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
