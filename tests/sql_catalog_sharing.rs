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
/// Fails loudly, and DISTINCTLY, on the two failure modes this guard can hit —
/// they mean completely different things and must never share a message:
///   * **zero candidates**: the guard is BLIND — it could not locate its
///     subject at all (e.g. the signature text drifted, as it did for EH-331:
///     the search string omitted `pub(super)`, which every one of these
///     functions actually carries). This is a defect in the GUARD, not
///     necessarily in the code it guards, and must never be confused with an
///     ordering violation.
///   * **more than one candidate**: the guard is AMBIGUOUS — the subject is
///     defined in more than one dispatch source, so "the" definition this
///     guard reasons about does not exist.
/// Only when exactly one candidate is found does this function return, and
/// only then can a caller meaningfully ask about ordering within its body.
fn defining_source(signature: &str) -> (PathBuf, String) {
    let searched = dispatch_sources();
    let mut found: Vec<(PathBuf, String)> = Vec::new();
    for path in &searched {
        let Ok(text) = fs::read_to_string(path) else {
            continue;
        };
        if definition_offset(&text, signature).is_some() {
            found.push((path.clone(), text));
        }
    }
    resolve_unique_definition(signature, &searched, found)
}

/// Turns what `defining_source` found into either the unique answer or one
/// of its two DISTINCT loud failures. Split out so the two failure messages
/// — and the match that picks between them — don't add branching to
/// `defining_source` itself; this function is new, so it carries its own
/// complexity budget rather than regressing an existing one.
fn resolve_unique_definition(
    signature: &str,
    searched: &[PathBuf],
    mut found: Vec<(PathBuf, String)>,
) -> (PathBuf, String) {
    match found.len() {
        1 => found.pop().expect("checked above"),
        0 => panic!(
            "GUARD IS BLIND (not an ordering failure): searched {} dispatch \
             source(s) for a column-0 definition of `{signature}` (optionally \
             preceded by visibility/qualifier keywords such as `pub(super)`, \
             `pub(crate)`, `unsafe`, `const`, `extern \"C\"`) and found 0 \
             candidates. Sources searched: {:?}. Either the signature text \
             this guard looks for has drifted from the real declaration, or \
             the item was removed/renamed/moved out of the dispatch tree — \
             fix the guard's search text or `dispatch_sources()`, do not \
             assume the guarded invariant itself is violated.",
            searched.len(),
            searched
        ),
        _ => panic!(
            "GUARD IS AMBIGUOUS (not an ordering failure): `{signature}` is \
             defined in {} dispatch sources, expected exactly 1: {:?}. The \
             guard cannot reason about \"the\" definition's body while more \
             than one candidate exists.",
            found.len(),
            found.iter().map(|(p, _)| p.clone()).collect::<Vec<_>>()
        ),
    }
}

/// Strips a leading `pub`, `pub(crate)`, `pub(super)`, or `pub(in path)` from
/// `line`. `None` when `line` does not start with the `pub` keyword at a real
/// word boundary (so an identifier like `public_key` is never mistaken for
/// it).
fn try_strip_pub(line: &str) -> Option<&str> {
    let after = line.strip_prefix("pub")?;
    let boundary = after.chars().next()?;
    if !boundary.is_whitespace() && boundary != '(' {
        return None;
    }
    let after = after.trim_start();
    let Some(inner) = after.strip_prefix('(') else {
        return Some(after);
    };
    let close = inner.find(')')?;
    Some(inner[close + 1..].trim_start())
}

/// Strips a leading `unsafe` or `const` qualifier (word-boundary checked).
fn try_strip_unsafe_or_const(line: &str) -> Option<&str> {
    for keyword in ["unsafe", "const"] {
        let Some(after) = line.strip_prefix(keyword) else {
            continue;
        };
        if after.chars().next().is_some_and(char::is_whitespace) {
            return Some(after.trim_start());
        }
    }
    None
}

/// Strips a leading `extern` qualifier and its optional `"ABI"` string.
fn try_strip_extern(line: &str) -> Option<&str> {
    let after = line.strip_prefix("extern")?;
    if !after.chars().next().is_some_and(char::is_whitespace) {
        return None;
    }
    let after = after.trim_start();
    let Some(after_quote) = after.strip_prefix('"') else {
        return Some(after);
    };
    let close = after_quote.find('"')?;
    Some(after_quote[close + 1..].trim_start())
}

/// Strips any leading visibility (`pub`, `pub(crate)`, `pub(super)`,
/// `pub(in path::to::mod)`) and item qualifiers (`unsafe`, `const`,
/// `extern "ABI"`) from the front of `line`, in any order/repetition Rust's
/// grammar allows before a `fn`/`async fn` item. Only whitespace and these
/// recognized keywords are ever consumed — anything else (a comment marker,
/// a call expression, arbitrary indentation) is left untouched, so this
/// cannot turn a mid-line mention or an indented item into a false match.
/// Delegates each keyword to its own `try_strip_*` helper so this function
/// stays a thin dispatch loop instead of one large nested conditional.
fn strip_item_modifiers(line: &str) -> &str {
    let mut rest = line.trim_start();
    loop {
        let stripped = try_strip_pub(rest)
            .or_else(|| try_strip_unsafe_or_const(rest))
            .or_else(|| try_strip_extern(rest));
        rest = match stripped {
            Some(next) => next,
            None => return rest,
        };
    }
}

/// Byte offset and text (including its trailing `\n`, if present) of every
/// line in `source`. Shared by `definition_offset` and `next_item_offset` so
/// both scan column-0 items the same way instead of each re-implementing the
/// same line walk.
fn lines_with_start(source: &str) -> impl Iterator<Item = (usize, &str)> {
    let mut offset = 0usize;
    source.split_inclusive('\n').map(move |line| {
        let start = offset;
        offset += line.len();
        (start, line)
    })
}

/// If `line`, after stripping leading visibility/qualifier keywords, starts
/// with `signature`, the byte offset (from the start of `line`) where
/// `signature` itself begins.
fn signature_offset_in_line(line: &str, signature: &str) -> Option<usize> {
    let after_modifiers = strip_item_modifiers(line);
    after_modifiers
        .starts_with(signature)
        .then(|| line.len() - after_modifiers.len())
}

/// Offset of the DEFINITION `signature` introduces — it must start at column
/// 0 (optionally after stripping leading visibility/qualifier keywords), so
/// a doc comment or a call site that merely names the function is not
/// mistaken for it, and so `pub(super) async fn foo` matches a search for
/// `async fn foo` just as a bare `async fn foo` would.
fn definition_offset(source: &str, signature: &str) -> Option<usize> {
    lines_with_start(source)
        .find_map(|(start, line)| signature_offset_in_line(line, signature).map(|o| start + o))
}

/// Counts CALL sites of `callee` (a string like `"foo("` or
/// `"path::to::foo("`) inside `text`, excluding its own definition line.
/// A bare local name's definition (`fn callee(` / `async fn callee(` /
/// `pub(super) fn callee(`, etc.) always ends in `"fn {callee}"`, so any match
/// immediately preceded by `"fn "` is the declaration, not a call, and is
/// excluded. A qualified callee like `"handlers::query::try_handle("` can
/// never be a definition's own name (Rust items aren't named with `::`), so
/// this is a no-op for those and safe to use everywhere uniformly.
fn count_call_sites(text: &str, callee: &str) -> usize {
    text.match_indices(callee)
        .filter(|(offset, _)| !text[..*offset].ends_with("fn "))
        .count()
}

/// Whether `line`, after stripping leading visibility/qualifier keywords,
/// is the start of a `fn`/`async fn` item.
fn is_fn_item_start(line: &str) -> bool {
    let after_modifiers = strip_item_modifiers(line);
    after_modifiers.starts_with("fn ") || after_modifiers.starts_with("async fn ")
}

/// Offset of the next column-0 `fn`/`async fn` item inside `text` — i.e. the
/// start of whatever item follows the one `item_body` is bounding. The first
/// line is always skipped: it is the remainder of the signature's own line,
/// never preceded by a real newline, so it can never itself be "the next
/// item" (mirrors the original marker-based search, which only ever matched
/// after a literal `"\n"`).
///
/// Like `definition_offset`, this accepts any leading visibility/qualifier
/// keywords via `strip_item_modifiers`. Before this fix it was a fixed list
/// of six literal markers (`"\nfn "`, `"\npub fn "`, ...) that did not
/// include `pub(super)` — the modifier every function in this dispatch tree
/// actually carries — so it could never find the NEXT item either, and a
/// body would silently run past its true end to the next boundary the old
/// list DID recognize (or to EOF). That never flipped an assertion in this
/// file from pass to fail, because every consumer only calls `.contains(..)`
/// on the (possibly over-long) body, which an over-capture can only make
/// MORE likely to match — so the bug could have hidden a real removal of a
/// call (a false PASS) without ever producing a false FAIL. Fixed for the
/// same reason `definition_offset` was: a text-scanning guard must bound
/// itself by the real grammar it is describing, not a snapshot of the
/// modifier combinations that happened to exist when it was written.
fn next_item_offset(text: &str) -> Option<usize> {
    lines_with_start(text)
        .skip(1)
        .find(|(_, line)| is_fn_item_start(line))
        .map(|(start, _)| start)
}

/// The body text of the item introduced by `signature`, bounded by the next
/// column-0 item in the same file.
fn item_body<'a>(source: &'a str, signature: &str, path: &Path) -> &'a str {
    let start = definition_offset(source, signature)
        .unwrap_or_else(|| panic!("`{signature}` must be defined in {}", path.display()));
    let after = &source[start + signature.len()..];
    let end = next_item_offset(after).unwrap_or(after.len());
    &after[..end]
}

/// Proves `helper_signature` (identified by its `fn`/`async fn` signature and
/// its bare call text `helper_call`, e.g. `"commit_query_gateway("`) has
/// EXACTLY ONE caller in the whole dispatch tree — `gateway` — and returns
/// how many times `callee` is called inside that helper's body. Split out of
/// `assert_handler_reachable_only_through_gateway` so the exclusivity proof's
/// own branching doesn't add to that function's complexity; it is new code,
/// so it carries its own budget rather than regressing an existing one.
fn assert_helper_exclusively_owned_and_count_calls(
    gateway: &str,
    gateway_body: &str,
    helper_signature: &str,
    helper_call: &str,
    callee: &str,
) -> usize {
    let helper_call_total: usize = dispatch_sources()
        .iter()
        .filter_map(|path| fs::read_to_string(path).ok())
        .map(|text| count_call_sites(&text, helper_call))
        .sum();
    let helper_call_inside_gateway = count_call_sites(gateway_body, helper_call);
    assert!(
        helper_call_inside_gateway > 0,
        "{gateway} must call {helper_signature} (this guard's exemption for \
         the helper is meaningless if the gateway never calls it)"
    );
    assert_eq!(
        helper_call_total, helper_call_inside_gateway,
        "{helper_signature} must be called ONLY from {gateway} — a call \
         inside it is only as trustworthy as {gateway} itself because \
         {gateway} is its one caller; a call from anywhere else would let \
         something outside the access gate reach {callee} through this \
         helper"
    );
    let (helper_path, helper_source) = defining_source(helper_signature);
    let helper_body = item_body(&helper_source, helper_signature, &helper_path);
    count_call_sites(helper_body, callee)
}

/// One iteration of guard link 5: every dispatch call site of `callee` lives
/// inside `gateway`, or inside a helper `gateway` exclusively owns
/// (`owned_helper`, see `assert_helper_exclusively_owned_and_count_calls`).
fn assert_handler_reachable_only_through_gateway(
    callee: &str,
    gateway: &str,
    owned_helper: Option<(&str, &str)>,
) {
    let (gateway_path, gateway_source) = defining_source(gateway);
    let gateway_body = item_body(&gateway_source, gateway, &gateway_path);
    let mut inside = count_call_sites(gateway_body, callee);
    if let Some((helper_signature, helper_call)) = owned_helper {
        inside += assert_helper_exclusively_owned_and_count_calls(
            gateway,
            gateway_body,
            helper_signature,
            helper_call,
            callee,
        );
    }
    assert!(
        inside > 0,
        "{gateway} must call {callee} at least once (this guard is \
         meaningless if that call moved elsewhere)"
    );
    let total: usize = dispatch_sources()
        .iter()
        .filter_map(|path| fs::read_to_string(path).ok())
        .map(|text| count_call_sites(&text, callee))
        .sum();
    assert_eq!(
        total, inside,
        "every {callee} call site in the dispatch tree must live inside \
         {gateway} or its exclusively-owned commit helper; a call from \
         anywhere else would reach a handler without passing the access gate"
    );
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
/// 1. `dispatch_graph_op_inner` calls `capture_graph_dispatch` BEFORE it calls
///    `route_graph_op_method`, and calls neither handler itself;
/// 2. `capture_graph_dispatch` — the entry point's own gate step, run and
///    awaited before routing can happen — calls `gate_graph_op_under_lock`;
/// 3. `gate_graph_op_under_lock` calls `check_graph_op_access`;
/// 4. `check_graph_op_access` calls `check_graph_access`;
/// 5. the ONLY call sites of the two handlers in the whole dispatch tree are
///    inside `route_query_gateway` / `route_rdf_gateway` — or inside a private
///    `commit_query_gateway` / `commit_rdf_gateway` helper each gateway
///    exclusively owns (proven by checking that helper has no other caller) —
///    and the gateways are reachable only through `route_graph_op_method`,
///    i.e. only after link 1's gate.
///
/// (Corrected again, EH-331: the pipeline decomposed a second time since the
/// note above was written — `dispatch_graph_op_inner` no longer calls
/// `gate_graph_op_under_lock` itself, it calls `capture_graph_dispatch`, which
/// calls the gate (link 2). Link 5 also grew the commit-helper exemption:
/// `route_query_gateway`/`route_rdf_gateway` now delegate their write-commit
/// path to a private helper that itself calls the handler.)
///
/// Remove any one of those calls, or route before gating, and this test fails.
#[test]
fn check_graph_access_precedes_query_and_rdf_try_handle_in_dispatch() {
    let (path, source) = defining_source("async fn dispatch_graph_op_inner");
    let entry = item_body(&source, "async fn dispatch_graph_op_inner", &path);

    // (1) capture (which takes the gate) before route, inside the entry point.
    let capture_offset = entry.find("capture_graph_dispatch(").unwrap_or_else(|| {
        panic!(
            "dispatch_graph_op_inner must take the access gate via \
             capture_graph_dispatch (which itself calls gate_graph_op_under_lock) \
             in {}",
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
        capture_offset < route_offset,
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

    // (2) capture_graph_dispatch really takes the gate.
    let (capture_path, capture_source) = defining_source("async fn capture_graph_dispatch");
    assert!(
        item_body(&capture_source, "async fn capture_graph_dispatch", &capture_path)
            .contains("gate_graph_op_under_lock("),
        "capture_graph_dispatch must call gate_graph_op_under_lock — \
         dispatch_graph_op_inner's ordering guarantee (link 1) is worthless if \
         the function it awaits before routing does not actually gate"
    );

    // (3) + (4) the gate really reaches check_graph_access.
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

    // (5) the two handlers have no dispatch call site outside the routed
    // gateways OR a helper a gateway exclusively owns. `route_query_gateway`
    // (and `route_rdf_gateway`) delegate their write-commit path to a private
    // `commit_query_gateway` (`commit_rdf_gateway`) helper that ALSO calls the
    // handler, from inside a `commit_conditional_mutation_async` apply
    // closure, to actually run the query/RDF op once the write is staged. A
    // call inside that helper is exactly as gated as one inside the gateway
    // body itself — but only because the helper has exactly one caller in the
    // whole dispatch tree, namely its own gateway; this guard proves that
    // exclusivity rather than assuming it, so a stray second caller of the
    // helper (which WOULD bypass the gate) still fails it.
    for (callee, gateway, owned_helper) in [
        (
            "handlers::query::try_handle(",
            "async fn route_query_gateway",
            Some(("async fn commit_query_gateway", "commit_query_gateway(")),
        ),
        (
            "handlers::rdf::try_handle(",
            "async fn route_rdf_gateway",
            Some(("async fn commit_rdf_gateway", "commit_rdf_gateway(")),
        ),
    ] {
        assert_handler_reachable_only_through_gateway(callee, gateway, owned_helper);
    }

    // (6) and the gateways themselves are never invoked ahead of the gate: any
    // call to one from inside the entry point must sit after `capture_offset`,
    // so (1)-(6) compose into the ordering this guard claims.
    for gateway_call in ["route_query_gateway(", "route_rdf_gateway("] {
        for (offset, _) in entry.match_indices(gateway_call) {
            assert!(
                capture_offset < offset,
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
