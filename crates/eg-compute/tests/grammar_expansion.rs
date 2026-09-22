//! CONCEPT:EH-281 grammar expansion — extended-language grammar registration.
//!
//! Proves each newly registered extended-tier grammar (`tree_sitter.rs`'s
//! `grammars_extended` table) actually parses a small representative source
//! and yields the expected symbols through the crate's PUBLIC `parse_file`
//! API — the same contract `sql_ddl_extraction.rs` exercises for the SQL DDL
//! path. Kept as its own integration-test file (rather than growing the
//! crate's inline `#[cfg(test)] mod tests`) per this lane's file ownership.
//!
//! Objective-C's `@interface`/`@implementation` now extract as `Class`
//! symbols too (CONCEPT:EH-281 follow-up): `class_interface`/
//! `class_implementation` were added to the walker's `class_like_kind`
//! vocabulary, and `symbol_name()`'s new `identifier_child_name` fallback
//! reads their name off the positional `identifier` child the grammar
//! carries it on (not a `name`/`declarator`/`type` field). The plain-C
//! constructs a `.m` file may also contain (a `struct`, a C-style function)
//! still extract exactly as before, since Objective-C shares C's
//! `struct_specifier`/`function_definition` node kinds.
//!
//! Verilog (CONCEPT:EH-281) needed no walker vocabulary addition at all —
//! its `class_declaration`/`interface_declaration`/`function_declaration`
//! node kinds already match the existing table. The only gap was the same
//! one Objective-C had: the identifier lives on a positional
//! `class_identifier`/`interface_identifier`/`function_identifier` child,
//! not a field, which the same `identifier_child_name` fallback resolves.
//!
//! HTML/CSS/JSON are deliberately NOT registered by this lane, even though
//! ABI-14-compatible crates exist for all three: they have no function/class
//! concept, so no vocabulary addition to the generic walker could ever
//! extract anything from them, and registering the grammar alone (with
//! nothing to consume elements/selectors/keys) would only inflate the
//! supported-extension count for zero capability. `html_css_json_not_yet_registered`
//! below is a regression guard for that decision, not a symbol test.
#![cfg(feature = "ast-extended")]

use eg_compute::parser::tree_sitter::parse_file;

/// Find a symbol by name in a parsed file, returning its properties.
fn sym(path: &str, src: &str, name: &str) -> std::collections::HashMap<String, String> {
    let r = parse_file(path, src.as_bytes()).unwrap_or_else(|e| panic!("parse {path} failed: {e}"));
    r.nodes
        .into_iter()
        .find(|n| n.properties.get("name").map(String::as_str) == Some(name))
        .unwrap_or_else(|| panic!("no symbol {name} in {path}"))
        .properties
}

#[test]
fn kotlin_class_and_function() {
    // Top-level class and function kept separate (rather than a method
    // nested in the class) so the test doesn't depend on exactly how deep
    // Kotlin's grammar nests a class member declaration — the walker
    // recurses through wrapper nodes regardless, but a top-level construct
    // is the smallest possible claim about grammar shape.
    let class_src = "class Widget {\n    val value: Int = 0\n}\n";
    let c = sym("Widget.kt", class_src, "Widget");
    assert_eq!(c["symbol_type"], "Class");
    assert_eq!(c["kind_detail"], "class");
    assert_eq!(c["language"], "kotlin");

    let fn_src = "fun run(): Int {\n    return 1\n}\n";
    let f = sym("run.kt", fn_src, "run");
    assert_eq!(f["symbol_type"], "Function");
    assert_eq!(f["language"], "kotlin");
}

#[test]
fn objc_extracts_its_embedded_c_constructs() {
    // A real Objective-C file: a plain C struct and function, which extract
    // because Objective-C shares C's `struct_specifier`/`function_definition`
    // node kinds (unaffected by the `@interface`/`@implementation` work
    // below).
    let src = r#"
struct Point {
    int x;
    int y;
};

@interface Greeter : NSObject
- (void)greet;
@end

@implementation Greeter
- (void)greet {
}
@end

int add(int a, int b) {
    return a + b;
}
"#;
    let point = sym("Greeter.m", src, "Point");
    assert_eq!(point["kind_detail"], "struct");
    assert_eq!(point["language"], "objc");
    let add = sym("Greeter.m", src, "add");
    assert_eq!(add["symbol_type"], "Function");
    assert_eq!(add["language"], "objc");
}

#[test]
fn objc_extracts_interface_and_implementation_as_classes() {
    // `@interface`/`@implementation` (CONCEPT:EH-281 follow-up): the class
    // name is an unnamed positional child in both node kinds, not a
    // `name`/`declarator`/`type` field, resolved by `identifier_child_name`.
    let src = "@interface Greeter : NSObject\n@end\n\n@implementation Greeter\n@end\n";
    let r = parse_file("Greeter.m", src.as_bytes()).unwrap_or_else(|e| panic!("parse failed: {e}"));
    let greeters: Vec<_> = r
        .nodes
        .iter()
        .filter(|n| n.properties.get("name").map(String::as_str) == Some("Greeter"))
        .collect();
    // One Class symbol from `class_interface`, one from `class_implementation`.
    assert_eq!(greeters.len(), 2, "{greeters:?}");
    for g in &greeters {
        assert_eq!(g.properties["symbol_type"], "Class");
        assert_eq!(g.properties["kind_detail"], "class");
        assert_eq!(g.properties["language"], "objc");
    }
}

#[test]
fn verilog_module_class_interface_and_function() {
    // Verilog (CONCEPT:EH-281): no walker vocabulary addition needed — its
    // `class_declaration`/`interface_declaration`/`function_declaration`
    // kinds already matched the existing table. Only the identifier-child
    // name fallback was new (`class_identifier`/`interface_identifier`/
    // `function_identifier` are positional children, not fields).
    let src = r#"
class Widget;
  int value;
endclass

interface Bus;
  logic clk;
endinterface

function int add(int a, int b);
  add = a + b;
endfunction
"#;
    let c = sym("widget.v", src, "Widget");
    assert_eq!(c["symbol_type"], "Class");
    assert_eq!(c["kind_detail"], "class");
    assert_eq!(c["language"], "verilog");

    let i = sym("widget.v", src, "Bus");
    assert_eq!(i["kind_detail"], "interface");
    assert_eq!(i["language"], "verilog");

    let f = sym("widget.v", src, "add");
    assert_eq!(f["symbol_type"], "Function");
    assert_eq!(f["language"], "verilog");
}

#[test]
fn zig_function() {
    let src = "fn add(a: i32, b: i32) i32 {\n    return a + b;\n}\n";
    let f = sym("add.zig", src, "add");
    assert_eq!(f["symbol_type"], "Function");
    assert_eq!(f["language"], "zig");
}

#[test]
fn groovy_class_interface_and_method() {
    // Groovy's grammar shares Java's node-kind vocabulary, so it gets the
    // same full coverage the existing `java_class_and_method` unit test
    // proves for Java.
    let src = r#"
class Widget {
    int compute(int n) {
        return n * 2
    }
}
interface Drawable {
    void draw()
}
"#;
    let c = sym("Widget.groovy", src, "Widget");
    assert_eq!(c["symbol_type"], "Class");
    assert_eq!(c["kind_detail"], "class");
    assert_eq!(c["language"], "groovy");
    let m = sym("Widget.groovy", src, "compute");
    assert_eq!(m["symbol_type"], "Function");
    assert_eq!(m["kind_detail"], "method");
    let i = sym("Widget.groovy", src, "Drawable");
    assert_eq!(i["kind_detail"], "interface");
}

#[test]
fn swift_class_and_function() {
    let class_src = "class Widget {\n    var value: Int = 0\n}\n";
    let c = sym("Widget.swift", class_src, "Widget");
    assert_eq!(c["symbol_type"], "Class");
    assert_eq!(c["language"], "swift");

    let fn_src = "func run() -> Int {\n    return 1\n}\n";
    let f = sym("run.swift", fn_src, "run");
    assert_eq!(f["symbol_type"], "Function");
    assert_eq!(f["language"], "swift");
}

#[test]
fn html_css_json_not_yet_registered() {
    // Deliberate exclusion, not an oversight (CONCEPT:EH-281 — see this
    // file's module doc and the exclusion note in `grammars_extended`):
    // registering a grammar with no extractor that can consume its node
    // kinds would inflate the supported-extension count for zero capability.
    // This is a regression guard on that decision — if a future lane adds a
    // dedicated structural extractor and registers these grammars for real,
    // this test should be replaced with one that asserts genuine extraction,
    // not updated to keep expecting "Unsupported file extension".
    for path in ["index.html", "styles.css", "data.json"] {
        let err = parse_file(path, b"").expect_err("not registered yet");
        assert_eq!(err, "Unsupported file extension");
    }
}

#[test]
fn gradle_dsl_files_route_to_groovy_and_kotlin() {
    // `build.gradle` is Groovy DSL; `build.gradle.kts` is Kotlin DSL. Neither
    // needs a grammar of its own (CONCEPT:EH-281) — both extensions just
    // route to the grammar already registered for their host language
    // (`.gradle` -> groovy row, `.gradle.kts` -> the bare `kts` extension on
    // the kotlin row).
    let groovy_src = "class Config {\n    int value = 1\n}\n";
    let g = sym("build.gradle", groovy_src, "Config");
    assert_eq!(g["language"], "groovy");

    let kotlin_src = "class Config {\n    var value: Int = 1\n}\n";
    let k = sym("build.gradle.kts", kotlin_src, "Config");
    assert_eq!(k["language"], "kotlin");
}
