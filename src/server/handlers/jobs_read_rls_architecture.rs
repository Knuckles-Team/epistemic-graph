//! Architecture test (W1b / L-RLS-2 follow-up): job-**KIND** read-path RLS
//! coverage, plus a regression guard on the O(1) opaque-ref index.
//!
//! `server::access::read_rls_coverage_tests` (see that module) exhaustively
//! partitions every non-mutating **protocol `Method`** into
//! `RLS_ROUTED`/`NON_ROW_SCOPED`/`NOT_YET_AUDITED`. `Method::AnalyticsJob`
//! itself never appears on that read side at all: `eg_capabilities`'s static
//! policy table classifies the WHOLE method `mutates: true` (a conservative
//! upper bound, annotated "Status is a read; Submit/Cancel/Resume commit
//! through the native jobs.redb MutationBatch gateway") because one wire
//! method carries multiple `op`s of different shapes. So the method-level
//! ratchet is structurally blind to the one read axis that actually matters
//! here: which **`JobKind`** (the compute a `Submit` describes) is allowed to
//! read graph node/edge rows server-side. That is exactly what
//! `super::reads_graph_rows_server_side` decides, and what this file ratchets.
//!
//! `reads_graph_rows_server_side` is already an exhaustive, no-wildcard
//! `match` — a new `JobKind` variant without an arm is a compile error
//! (`E0004`), not a silent default. This file adds the two halves a compiler
//! error alone can't give a human reviewer:
//!
//!  1. A live cross-check against an INDEPENDENTLY hand-maintained
//!     classification table (mirrors `RLS_ROUTED`/`NON_ROW_SCOPED` in
//!     `server::access`, and `RUNTIME_CONDITIONAL`/`ACCESS_RS_COVERAGE_GAP` in
//!     `crates/eg-capabilities/tests/consistency.rs`) — so a variant added to
//!     `eg_types::jobs::JobKind` without a matching classification entry HERE
//!     fails a `cargo test`, independent of whatever the exhaustive match
//!     itself decided.
//!  2. A textual self-scan (mirrors `scripts/check_universal_read_rls.py`'s
//!     substring-presence style, but as a real `cargo test` so it runs with
//!     this package's own targeted suite) proving the SHAPE of the guard
//!     hasn't regressed: no wildcard arm crept into
//!     `reads_graph_rows_server_side`, `handle_submit` still consults it
//!     BEFORE using `kind` and fails closed, and `resolve_core_ref` still
//!     resolves through the O(1)-amortized index instead of a raw per-call
//!     linear scan over every resident graph.

use super::*;

/// Independent, hand-maintained classification of every `JobKind` variant
/// this build compiles in. Cross-checked live against
/// `reads_graph_rows_server_side` in
/// `every_job_kind_is_classified_and_matches_the_live_guard` below — this is
/// NOT a second copy of that function's logic, it is a human-legible
/// assertion of what TODAY's answer for each variant is and why, so a future
/// change to the real guard shows up as a failing assertion here with a
/// named reason attached, not just a green compile.
///
/// There is deliberately no `RLS_ROUTED_JOB_KINDS` bucket yet: no shipped
/// `JobKind` reads a graph row server-side today. The day one does, it must
/// be wired through `GraphReadAuthority::project_core`/`filter_view` FIRST,
/// then added to a new routed bucket here in the SAME change — never the
/// reverse order (see `super::handle_submit`'s own fail-closed guard, which
/// enforces exactly this ordering at runtime too).
const NON_ROW_SCOPED_JOB_KINDS: &[(&str, &str)] = &[
    (
        "MineAssociate",
        "mines only the `transactions` field the CALLER supplied inline in \
         the request; `handle_submit`'s JobKind::MineAssociate arm hashes \
         each caller-supplied item under the caller's own owner_scope before \
         it ever reaches durable jobs.redb storage -- no node/edge property \
         is read from `core`",
    ),
    #[cfg(feature = "program-optimization")]
    (
        "ProgramOptimize",
        "submits an opaque OptimizationRequest; the actual optimization work \
         is claimed and executed later by a REMOTE worker under its OWN \
         independently authenticated session (see this file's module doc, \
         \"Distributed execution contract\") -- this handler never reads a \
         graph row on that worker's behalf",
    ),
];

#[test]
fn every_job_kind_is_classified_and_matches_the_live_guard() {
    let classified: std::collections::HashMap<&str, &str> =
        NON_ROW_SCOPED_JOB_KINDS.iter().copied().collect();
    assert_eq!(
        classified.len(),
        NON_ROW_SCOPED_JOB_KINDS.len(),
        "duplicate JobKind entry in NON_ROW_SCOPED_JOB_KINDS"
    );

    // Every kind this build actually ships must be classified -- an
    // undocumented variant is the hard failure, mirroring
    // `server::access::read_rls_coverage_tests`'s UNDOCUMENTED gate.
    let shipped: &[&str] = &[
        "MineAssociate",
        #[cfg(feature = "program-optimization")]
        "ProgramOptimize",
    ];
    let undocumented: Vec<&str> = shipped
        .iter()
        .copied()
        .filter(|name| !classified.contains_key(name))
        .collect();
    assert!(
        undocumented.is_empty(),
        "UNDOCUMENTED JobKind(s): {undocumented:?} -- classify each in \
         NON_ROW_SCOPED_JOB_KINDS (or a new RLS_ROUTED_JOB_KINDS bucket, once \
         one exists) in jobs_read_rls_architecture.rs"
    );

    // Live cross-check: today every shipped kind must be non-row-scoped
    // (`reads_graph_rows_server_side` returns `false`), because there is no
    // RLS_ROUTED_JOB_KINDS bucket yet. The moment `reads_graph_rows_server_side`
    // flips ANY of these to `true` without this test also being updated, this
    // assertion is what catches it.
    assert!(
        !reads_graph_rows_server_side(&JobKind::MineAssociate {
            transactions: vec![],
            min_support: 0.1,
            min_confidence: 0.5,
            algorithm: "fpgrowth".to_string(),
        }),
        "MineAssociate flipped to graph-row-reading -- update its \
         NON_ROW_SCOPED_JOB_KINDS entry (or move it to a routed bucket) and \
         verify handle_submit actually projects the row through \
         GraphReadAuthority before this assertion changes"
    );
    #[cfg(feature = "program-optimization")]
    assert!(
        !reads_graph_rows_server_side(&JobKind::ProgramOptimize {
            request_msgpack: Vec::new(),
        }),
        "ProgramOptimize flipped to graph-row-reading -- see the MineAssociate \
         assertion above for what must happen before this test may change"
    );
}

/// Extract a function's `{ ... }` body by brace-depth matching, starting from
/// the first `{` after `signature_needle`. Used instead of a fixed-offset
/// substring so reformatting (e.g. `cargo fmt`) inside the body doesn't
/// spuriously break these tests — only a structural change (an added/removed
/// brace, a renamed function) does.
fn function_body<'a>(source: &'a str, signature_needle: &str) -> &'a str {
    let sig_at = source
        .find(signature_needle)
        .unwrap_or_else(|| panic!("expected to find {signature_needle:?} in jobs.rs"));
    let body_start = source[sig_at..]
        .find('{')
        .map(|i| sig_at + i)
        .unwrap_or_else(|| panic!("{signature_needle:?} has no body"));
    let mut depth = 0i32;
    for (offset, ch) in source[body_start..].char_indices() {
        match ch {
            '{' => depth += 1,
            '}' => {
                depth -= 1;
                if depth == 0 {
                    return &source[body_start..=body_start + offset];
                }
            }
            _ => {}
        }
    }
    panic!("{signature_needle:?}'s body is never closed");
}

/// Textual self-scan half of the ratchet — mirrors
/// `scripts/check_universal_read_rls.py`'s substring-presence style, but runs
/// as a real `cargo test` alongside this package's other targeted tests
/// instead of a separate CI step.
const JOBS_SOURCE: &str = include_str!("jobs.rs");

/// `reads_graph_rows_server_side` must stay a `match` with NO wildcard arm.
/// That is what turns an unclassified future `JobKind` variant into an
/// `E0004` compile error instead of a guard that silently defaults to
/// `false` (unprotected) for anything it doesn't recognize. A `_ =>`/`_ if`
/// anywhere in its body would defeat the entire forcing function this
/// package's task exists to add.
#[test]
fn reads_graph_rows_server_side_has_no_wildcard_arm() {
    let body = function_body(
        JOBS_SOURCE,
        "fn reads_graph_rows_server_side(kind: &JobKind) -> bool {",
    );
    assert!(
        !body.contains("_ =>") && !body.contains("_ if"),
        "reads_graph_rows_server_side gained a wildcard arm -- this defeats \
         the compile-time exhaustiveness a new JobKind variant relies on to \
         force an explicit RLS classification"
    );
}

/// `handle_submit` must consult the guard, and fail closed on it, BEFORE it
/// does anything else with `kind` (in particular before the
/// `governed_kind`/`algorithm_family` destructure that starts committing
/// per-kind durable state) — never after.
///
/// Deliberately does NOT run `function_body` over the whole (large)
/// `handle_submit` — its later `JobKind` arms build durable payloads with
/// `format!`/interpolated strings, whose literal `{`/`}` characters would
/// corrupt a naive brace-depth walk. Ordering is checked with plain anchor
/// offsets instead, and only the guard's OWN small `if` block (verified
/// brace-safe: a single `return Response::err(...)` with no interpolation)
/// is brace-matched.
#[test]
fn handle_submit_fails_closed_before_using_kind() {
    let submit_at = JOBS_SOURCE
        .find("async fn handle_submit(")
        .expect("handle_submit must exist in jobs.rs");
    let after_submit = &JOBS_SOURCE[submit_at..];

    let guard_if_at = after_submit
        .find("if reads_graph_rows_server_side(&kind) {")
        .expect("handle_submit must call reads_graph_rows_server_side(&kind) before using it");
    let match_kind_at = after_submit
        .find("match kind {")
        .expect("handle_submit must destructure kind through a match somewhere");
    assert!(
        guard_if_at < match_kind_at,
        "the reads_graph_rows_server_side fail-closed guard must run BEFORE \
         handle_submit's `match kind {{ ... }}` destructure, not after"
    );

    // Brace-match JUST the guard's own if-block (small and known brace-safe)
    // to confirm ITS body -- not merely some unrelated later code -- fails
    // closed.
    let guard_block = function_body(after_submit, "if reads_graph_rows_server_side(&kind) {");
    assert!(
        guard_block.contains("return Response::err("),
        "a tripped reads_graph_rows_server_side guard must return an error \
         Response (fail closed), not silently continue"
    );
}

/// `resolve_core_ref` must resolve through the O(1)-amortized
/// `resolve_opaque_graph_ref` index, not a direct per-call
/// `all_entries().into_iter().find(...)` scan — the exact O(resident-graphs)
/// pattern this package's task replaced. Guards against a future edit
/// silently reintroducing the linear scan on the job-publication hot path.
#[test]
fn resolve_core_ref_uses_the_indexed_resolver_not_a_raw_linear_scan() {
    let body = function_body(
        JOBS_SOURCE,
        "async fn resolve_core_ref(\n    state: &Arc<RwLock<ServerState>>,",
    );
    assert!(
        body.contains("resolve_opaque_graph_ref("),
        "resolve_core_ref no longer delegates to the O(1)-amortized \
         resolve_opaque_graph_ref -- did it regress to a direct scan?"
    );
    assert!(
        !body.contains(".all_entries()"),
        "resolve_core_ref calls .all_entries() directly again -- the O(1) \
         fast path (and the fallback scan) must live in \
         resolve_opaque_graph_ref, not be reintroduced here"
    );
    assert!(
        JOBS_SOURCE.contains("fn opaque_graph_ref_index("),
        "the process-wide opaque_ref -> graph_name reverse index accessor is \
         missing from jobs.rs"
    );
    assert!(
        JOBS_SOURCE.contains("fn resolve_opaque_graph_ref("),
        "the O(1)-amortized resolver resolve_opaque_graph_ref is missing \
         from jobs.rs"
    );
}
