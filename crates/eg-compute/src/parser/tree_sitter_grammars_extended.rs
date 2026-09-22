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
// above these dependencies in `Cargo.toml`). Three groups of ledger-requested
// (EH-281) languages are deliberately NOT here — see the lane's WRAPUP for
// the full evidence per language:
//   * No maintained, ABI-compatible crates.io binding at all: Terraform/HCL
//     (only release is ABI 15) and DreamMaker (no maintained grammar).
//   * An ABI-14-compatible crate exists, but none of the language's
//     declaration node kinds are covered by the existing generic AST
//     walker's node-kind vocabulary (`tree_sitter_ast.rs`'s
//     `class_like_kind`/`function_like_kind`) — registering them would
//     silently extract zero symbols: Julia, Elixir, PowerShell, Fortran,
//     Pascal. (Verilog and Objective-C used to be in this bucket too, but
//     both are now registered below: their declaration KIND strings already
//     matched the existing vocabulary — `class_declaration`/
//     `interface_declaration`/`function_declaration` for Verilog,
//     `class_interface`/`class_implementation` newly added for
//     Objective-C — the only gap was `symbol_name()` not knowing how to read
//     an identifier that's a positional child rather than a field, fixed by
//     `identifier_child_name` in `tree_sitter_ast.rs`.)
//   * An ABI-14-compatible crate exists and the walker gap is moot — these
//     languages have no function/class concept at all, so no vocabulary
//     addition to `class_like_kind`/`function_like_kind` could ever extract
//     anything from them. They need a wholly separate DEDICATED extractor
//     (element/attribute for HTML, selector/property for CSS, key path for
//     JSON — the same shape `tree_sitter_sql.rs` already has for SQL DDL),
//     which is architecture work for a follow-up lane, not a grammar-table
//     row: HTML, CSS, JSON.

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
];

/// Resolve `ext` against the extended-language tier, or `None` if it isn't
/// one of these grammars.
pub(super) fn lookup(ext: &str) -> Option<(Language, &'static str)> {
    language_table_entry(EXTENDED_LANGUAGES, ext).map(|(ctor, label)| (ctor(), label))
}
