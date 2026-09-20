//! CONCEPT:EH-281 grammar expansion — extended-language grammar registration.
//!
//! Proves each newly registered extended-tier grammar (`tree_sitter.rs`'s
//! `grammars_extended` table) actually parses a small representative source
//! and yields the expected symbols through the crate's PUBLIC `parse_file`
//! API — the same contract `sql_ddl_extraction.rs` exercises for the SQL DDL
//! path. Kept as its own integration-test file (rather than growing the
//! crate's inline `#[cfg(test)] mod tests`) per this lane's file ownership.
//!
//! Two languages register a grammar but are deliberately tested for what the
//! EXISTING generic AST walker (`tree_sitter_ast.rs`, owned by a different
//! lane) actually extracts, not for a symbol shape this lane cannot add:
//! Objective-C's own `@interface`/`@implementation`/method nodes carry no
//! `name`/`declarator`/`type` field the walker's `symbol_name()` can read, so
//! only the plain-C constructs a `.m` file may also contain (a `struct`, a
//! C-style function) extract; HTML/CSS/JSON have no function/class concept at
//! all, so zero SYMBOL nodes is the CORRECT result, not a defect.
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
    // A real Objective-C file: an `@interface`/`@implementation` pair (whose
    // `class_interface`/`class_implementation`/`method_definition` nodes are
    // NOT in the generic walker's node-kind vocabulary and so contribute no
    // symbol — a known, documented gap, not asserted here) plus a plain C
    // struct and function, which DO extract because Objective-C shares C's
    // `struct_specifier`/`function_definition` node kinds.
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
fn html_css_json_parse_with_no_code_symbols() {
    // HTML/CSS/JSON have no function/class concept, so the generic AST
    // symbol walker correctly extracts zero SYMBOL nodes for them.
    // Registering these grammars closes the "Unsupported file extension"
    // gap for repository-wide parsing sweeps; a dedicated structural
    // extractor (as SQL DDL already has for database entities) is future
    // work, not this lane's grammar-table scope.
    let html =
        parse_file("index.html", b"<html><body><p>hi</p></body></html>").expect("parse html");
    assert_eq!(html.nodes.len(), 0);
    assert_eq!(html.symbols_extracted, 0);

    let css = parse_file("styles.css", b".foo { color: red; }").expect("parse css");
    assert_eq!(css.nodes.len(), 0);
    assert_eq!(css.symbols_extracted, 0);

    let json = parse_file("data.json", br#"{"a": 1, "b": [1, 2, 3]}"#).expect("parse json");
    assert_eq!(json.nodes.len(), 0);
    assert_eq!(json.symbols_extracted, 0);
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
