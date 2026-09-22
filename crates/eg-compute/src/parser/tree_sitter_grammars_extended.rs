// CONCEPT:EH-281 grammar expansion — the extended-language grammar table.
//
// Split out of `tree_sitter.rs` so the core dispatcher file doesn't keep
// growing as this tier does: adding a language here is a table ROW, never a
// new function in the parent file. Compiled only under `ast-extended` (see
// the `#[cfg]` on the `mod grammars_extended` declaration in `tree_sitter.rs`),
// mirroring exactly how `CORE_LANGUAGES` is shaped and looked up
// (`super::language_table_entry`) — same data shape, same linear scan, no
// second dispatch mechanism invented for this tier.
//
// Every grammar below is pinned in `Cargo.toml` to a tree-sitter ABI-14
// release (this crate's tree-sitter core accepts ABI 13..=14 only — see the
// `tree-sitter-c-sharp` precedent in `CORE_LANGUAGES`'s file and the ABI note
// above these dependencies in `Cargo.toml`). Two ledger-requested (EH-281)
// languages are deliberately NOT here — no maintained, ABI-14-compatible
// crates.io binding exists for either (re-verified 2026-09-22; see the
// Cargo.toml comment above the dependency block for the full evidence):
// Terraform/HCL (its one-ever release is ABI 15) and DreamMaker
// (`tree-sitter-dm` exists and is actively maintained, but every one of its
// 4 releases is also ABI 15).
//
// Julia, Fortran, Pascal, and PowerShell needed real walker vocabulary work
// (`tree_sitter_ast.rs`'s `class_like_kind`/`function_like_kind` plus a
// language-scoped `symbol_name` fallback for each — their identifiers sit in
// shapes the shared `identifier_child_name` positional-child fallback
// doesn't cover: a nested `*_statement`/`type_head`/`signature` wrapper, not
// a bare positional identifier). Elixir needed something structurally
// different again: its grammar has NO dedicated declaration node kinds at
// all (Elixir is homoiconic — `defmodule`/`def`/`defp` all parse as a plain
// `call` node; the only way to tell one from an arbitrary function call is
// the call's `target` TEXT, which `class_like_kind`/`function_like_kind`
// cannot see since they take only a bare node-kind string). Elixir is
// handled by a dedicated `elixir_call_scope` in `tree_sitter_walk.rs`
// instead of the shared kind-string tables. (Verilog and Objective-C are the
// precedent for "vocabulary looked missing but wasn't": both are registered
// below with zero `class_like_kind`/`function_like_kind` additions — their
// declaration KIND strings already matched the existing table; the only gap
// was `symbol_name()` not knowing how to read an identifier that's a
// positional child rather than a field, fixed by `identifier_child_name`.)
//
// HTML, CSS, and JSON are registered too, but contribute NO new vocabulary:
// none of the three has a function/class concept, so the generic walker
// extracts zero symbols from any of them (proven by
// `html_css_json_parse_with_zero_symbols` in `tests/grammar_expansion.rs`).
// Registering them still buys real capability — content_digest/
// parser_capability_digest and a clean base for a future DEDICATED
// structural extractor (element/attribute for HTML, selector/property for
// CSS, key path for JSON — the same shape `tree_sitter_sql.rs` already has
// for SQL DDL) — which remains architecture work for a follow-up lane, not
// a grammar-table row.

use super::{language_table_entry, LangCtor};
use tree_sitter::Language;

/// `(extensions, grammar constructor, stable language label)` for the
/// extended-language tier — same table shape as `CORE_LANGUAGES`.
const EXTENDED_LANGUAGES: &[(&[&str], LangCtor, &str)] = &[
    (&["rb"], || tree_sitter_ruby::LANGUAGE.into(), "ruby"),
    (&["php"], || tree_sitter_php::LANGUAGE_PHP.into(), "php"),
    (
        &["sh", "bash"],
        || tree_sitter_bash::LANGUAGE.into(),
        "bash",
    ),
    (
        &["scala", "sc"],
        || tree_sitter_scala::LANGUAGE.into(),
        "scala",
    ),
    (&["lua"], || tree_sitter_lua::LANGUAGE.into(), "lua"),
    // CONCEPT:EH-281 grammar expansion additions below.
    (
        &["kt", "kts"],
        || tree_sitter_kotlin_ng::LANGUAGE.into(),
        "kotlin",
    ),
    (&["m", "mm"], || tree_sitter_objc::LANGUAGE.into(), "objc"),
    (&["zig"], || tree_sitter_zig::LANGUAGE.into(), "zig"),
    // Gradle's Groovy DSL (`build.gradle`) needs no grammar of its own — it
    // IS Groovy — so `gradle` is just another extension on this row. The
    // Kotlin DSL (`build.gradle.kts`) needs no row of its own either: its
    // extension is plain `kts`, already routed to Kotlin above.
    (
        &["groovy", "gradle"],
        || tree_sitter_groovy::LANGUAGE.into(),
        "groovy",
    ),
    (&["swift"], || tree_sitter_swift::LANGUAGE.into(), "swift"),
    // Verilog/SystemVerilog (CONCEPT:EH-281). `.v`/`.vh` are the classic
    // Verilog extensions this grammar's own test suite targets; `.sv`/`.svh`
    // (SystemVerilog) are NOT included — the grammar parses a meaningful
    // subset of SystemVerilog but isn't validated against it here.
    (
        &["v", "vh"],
        || tree_sitter_verilog::LANGUAGE.into(),
        "verilog",
    ),
    (&["jl"], || tree_sitter_julia::LANGUAGE.into(), "julia"),
    (
        &["ex", "exs"],
        || tree_sitter_elixir::LANGUAGE.into(),
        "elixir",
    ),
    (
        &["ps1", "psm1", "psd1"],
        || tree_sitter_powershell::LANGUAGE.into(),
        "powershell",
    ),
    // The classic fixed-form extensions (`.f`, `.for`) are deliberately not
    // included: this grammar targets modern free-form Fortran (90+), and
    // fixed-form column rules would need a separate validated pass.
    (
        &["f90", "f95", "f03", "f08"],
        || tree_sitter_fortran::LANGUAGE.into(),
        "fortran",
    ),
    (
        &["pas", "pp", "dpr"],
        || tree_sitter_pascal::LANGUAGE.into(),
        "pascal",
    ),
    // HTML/CSS/JSON (CONCEPT:EH-281 follow-up) — see this file's module doc:
    // registered for real parsing, zero symbols extracted today.
    (
        &["html", "htm"],
        || tree_sitter_html::LANGUAGE.into(),
        "html",
    ),
    (&["css"], || tree_sitter_css::LANGUAGE.into(), "css"),
    (&["json"], || tree_sitter_json::LANGUAGE.into(), "json"),
];

/// Resolve `ext` against the extended-language tier, or `None` if it isn't
/// one of these grammars.
pub(super) fn lookup(ext: &str) -> Option<(Language, &'static str)> {
    language_table_entry(EXTENDED_LANGUAGES, ext).map(|(ctor, label)| (ctor(), label))
}
