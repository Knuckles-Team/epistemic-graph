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
//! Julia, Fortran, Pascal, and PowerShell (CONCEPT:EH-281 follow-up) needed
//! real walker vocabulary work — their identifiers sit one or two levels
//! deeper than any existing fallback reaches, in language-specific wrapper
//! shapes (`signature`/`type_head` for Julia, a `*_statement` child for
//! Fortran, a `header`/`genericDot` chain for Pascal, a leaf
//! `function_name`/`simple_name` child for PowerShell) resolved by the new
//! `extended_symbol_name` dispatch in `tree_sitter_ast.rs`, one small
//! per-language function each.
//!
//! Elixir (CONCEPT:EH-281 follow-up) is structurally different again: its
//! grammar has NO dedicated declaration node kinds — `defmodule`/`def`/
//! `defp` all parse as a plain `call`, distinguishable only by the call's
//! `target` TEXT. `class_like_kind`/`function_like_kind` take a bare
//! node-kind string and can't see that, so Elixir is handled by a dedicated
//! `elixir_call_scope` in `tree_sitter_walk.rs` instead.
//!
//! HTML/CSS/JSON ARE now registered (CONCEPT:EH-281 follow-up), but
//! contribute no new vocabulary: none of the three has a function/class
//! concept, so the generic walker extracts zero symbols from any of them —
//! `html_css_json_parse_with_zero_symbols` below proves the grammar is
//! genuinely registered (parses real markup/CSS/JSON without error) while
//! confirming that non-extraction, rather than replacing the old
//! "unregistered" test with a fake claim of capability. A future lane can
//! still add a dedicated structural extractor (element/attribute, selector/
//! property, key path) without needing to touch grammar registration again.
//!
//! Terraform/HCL and DreamMaker (CONCEPT:EH-281 ABI-15 follow-up) needed the
//! `tree-sitter` core bumped 0.23 -> 0.25 (both are ABI 15; every earlier
//! grammar in this file is ABI 14 and needed no change). HCL is the same
//! shape as HTML/CSS/JSON: `hcl_parses_with_zero_symbols` proves real
//! parsing with zero extraction — `resource`/`variable`/`module` are all
//! the SAME `block` node kind, distinguished only by a label string.
//! DreamMaker is the opposite: a real OOP scripting language, so it got real
//! vocabulary work — `dreammaker_type_proc_and_include` proves class (a
//! `type_definition`'s dotted path, e.g. `/obj/item/weapon` -> `weapon`),
//! method (`type_proc_definition`), function (`proc_definition`, a
//! DIFFERENT node kind for a top-level proc), and import
//! (`preproc_include`, sharing C/C++'s node kind but a different field name
//! for the included path) extraction; `dreammaker_single_segment_type_path`
//! covers the one-segment path shape (`/obj`) that has no `type_identifier`
//! at all, only a `primitive_type` wrapping its own `identifier`.
#![cfg(feature = "ast-extended")]

use eg_compute::parser::tree_sitter::{parse_file, ParseResult};

/// The unresolved dependency targets (`depends_on_raw` edges) a parse emitted.
fn raw_deps(r: &ParseResult) -> Vec<&str> {
    r.edges
        .iter()
        .filter(|e| e.edge_type == "depends_on_raw")
        .map(|e| e.target.as_str())
        .collect()
}

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
fn html_css_json_parse_with_zero_symbols() {
    // CONCEPT:EH-281 follow-up — registered for real (see this file's
    // module doc): each parses successfully (no "Unsupported file
    // extension", no parse error) but extracts zero symbols, since none of
    // the three has a function/class concept the generic walker's vocabulary
    // covers. If a future lane adds a dedicated structural extractor, this
    // test should be replaced with one asserting genuine extraction.
    let html = parse_file("index.html", b"<div class=\"a\"><p>hi</p></div>")
        .unwrap_or_else(|e| panic!("html parse failed: {e}"));
    assert_eq!(html.symbols_extracted, 0);

    let css = parse_file("styles.css", b".widget { width: 10px; color: red; }")
        .unwrap_or_else(|e| panic!("css parse failed: {e}"));
    assert_eq!(css.symbols_extracted, 0);

    let json = parse_file("data.json", b"{\"a\": 1, \"b\": [1, 2, 3]}")
        .unwrap_or_else(|e| panic!("json parse failed: {e}"));
    assert_eq!(json.symbols_extracted, 0);
}

#[test]
fn julia_module_struct_abstract_function_and_macro() {
    let src = r#"
module MyMod

using Base.Threads
import Statistics: mean

struct Point
    x::Float64
    y::Float64
end

abstract type Shape end

function area(p::Point)
    return p.x * p.y
end

macro mymacro(x)
    return x
end

end
"#;
    let m = sym("MyMod.jl", src, "MyMod");
    assert_eq!(m["symbol_type"], "Class");
    assert_eq!(m["kind_detail"], "module");
    assert_eq!(m["language"], "julia");

    let p = sym("MyMod.jl", src, "Point");
    assert_eq!(p["symbol_type"], "Class");
    assert_eq!(p["kind_detail"], "struct");

    let s = sym("MyMod.jl", src, "Shape");
    assert_eq!(s["kind_detail"], "abstract_type");

    let a = sym("MyMod.jl", src, "area");
    assert_eq!(a["symbol_type"], "Function");
    assert_eq!(a["kind_detail"], "function");

    let mac = sym("MyMod.jl", src, "mymacro");
    assert_eq!(mac["symbol_type"], "Function");
    assert_eq!(mac["kind_detail"], "macro");
}

#[test]
fn elixir_module_functions_and_imports() {
    let src = r#"
defmodule MyApp.Widget do
  import Enum, only: [map: 2]
  alias MyApp.Helper
  require Logger

  def area(w, h) do
    w * h
  end

  defp zero_arg do
    :ok
  end
end
"#;
    let r = parse_file("widget.ex", src.as_bytes()).unwrap_or_else(|e| panic!("parse failed: {e}"));

    let module = r
        .nodes
        .iter()
        .find(|n| n.properties.get("name").map(String::as_str) == Some("MyApp.Widget"))
        .unwrap_or_else(|| panic!("no MyApp.Widget symbol: {r:?}"));
    assert_eq!(module.properties["symbol_type"], "Class");
    assert_eq!(module.properties["kind_detail"], "module");
    assert_eq!(module.properties["language"], "elixir");

    let area = r
        .nodes
        .iter()
        .find(|n| n.properties.get("name").map(String::as_str) == Some("area"))
        .unwrap_or_else(|| panic!("no area symbol: {r:?}"));
    assert_eq!(area.properties["symbol_type"], "Function");

    let zero_arg = r
        .nodes
        .iter()
        .find(|n| n.properties.get("name").map(String::as_str) == Some("zero_arg"))
        .unwrap_or_else(|| panic!("no zero_arg symbol: {r:?}"));
    assert_eq!(zero_arg.properties["symbol_type"], "Function");

    let raw_deps = raw_deps(&r);
    assert!(raw_deps.contains(&"Enum"), "{raw_deps:?}");
    assert!(raw_deps.contains(&"MyApp.Helper"), "{raw_deps:?}");
    assert!(raw_deps.contains(&"Logger"), "{raw_deps:?}");
}

#[test]
fn powershell_function_and_class() {
    let src = r#"
function Get-Area {
    param($Width, $Height)
    return $Width * $Height
}

class Widget {
    [int]$Width
    [int]$Height

    [int] Area() {
        return $this.Width * $this.Height
    }
}
"#;
    let f = sym("widget.ps1", src, "Get-Area");
    assert_eq!(f["symbol_type"], "Function");
    assert_eq!(f["language"], "powershell");

    let c = sym("widget.ps1", src, "Widget");
    assert_eq!(c["symbol_type"], "Class");
    assert_eq!(c["kind_detail"], "class");

    let m = sym("widget.ps1", src, "Area");
    assert_eq!(m["symbol_type"], "Function");
    assert_eq!(m["kind_detail"], "method");
}

#[test]
fn fortran_module_function_subroutine_and_use() {
    let src = r#"
module mymod
  use iso_fortran_env
  implicit none

contains

  function area(w, h) result(a)
    real :: w, h, a
    a = w * h
  end function area

  subroutine greet(name)
    character(len=*) :: name
    print *, name
  end subroutine greet

end module mymod
"#;
    let r = parse_file("mymod.f90", src.as_bytes()).unwrap_or_else(|e| panic!("parse failed: {e}"));

    let m = sym("mymod.f90", src, "mymod");
    assert_eq!(m["symbol_type"], "Class");
    assert_eq!(m["kind_detail"], "module");
    assert_eq!(m["language"], "fortran");

    let a = sym("mymod.f90", src, "area");
    assert_eq!(a["symbol_type"], "Function");
    assert_eq!(a["kind_detail"], "function");

    let g = sym("mymod.f90", src, "greet");
    assert_eq!(g["symbol_type"], "Function");
    assert_eq!(g["kind_detail"], "subroutine");

    let raw_deps = raw_deps(&r);
    assert!(raw_deps.contains(&"iso_fortran_env"), "{raw_deps:?}");
}

#[test]
fn pascal_function_method_and_uses() {
    let src = r#"
unit MyUnit;

interface

uses SysUtils, Classes;

function Add(a, b: Integer): Integer;

type
  TPoint = class
  public
    function Area: Integer;
  end;

implementation

function Add(a, b: Integer): Integer;
begin
  Result := a + b;
end;

function TPoint.Area: Integer;
begin
  Result := 1;
end;

end.
"#;
    let r =
        parse_file("mounit.pas", src.as_bytes()).unwrap_or_else(|e| panic!("parse failed: {e}"));

    let add = sym("mounit.pas", src, "Add");
    assert_eq!(add["symbol_type"], "Function");
    assert_eq!(add["language"], "pascal");

    let area = sym("mounit.pas", src, "Area");
    assert_eq!(area["symbol_type"], "Function");

    let raw_deps = raw_deps(&r);
    assert!(raw_deps.contains(&"SysUtils"), "{raw_deps:?}");
    assert!(raw_deps.contains(&"Classes"), "{raw_deps:?}");
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

#[test]
fn dreammaker_type_proc_and_include() {
    // CONCEPT:EH-281 ABI-15 follow-up. `/obj/item/weapon` is DM's class
    // shape (a type path); `proc/attack` inside it is a method
    // (type_proc_definition); a top-level `/proc/GlobalHelper` is a
    // function (proc_definition, distinct node kind, also name-fielded).
    let src = r#"
#include "code\other.dm"

/obj/item/weapon
	name = "weapon"
	var/damage = 10

	proc/attack(mob/target)
		target.health -= damage

/proc/GlobalHelper(a, b)
	return a + b
"#;
    let r = parse_file("weapon.dm", src.as_bytes()).unwrap_or_else(|e| panic!("parse failed: {e}"));

    let weapon = sym("weapon.dm", src, "weapon");
    assert_eq!(weapon["symbol_type"], "Class");
    assert_eq!(weapon["kind_detail"], "class");
    assert_eq!(weapon["language"], "dreammaker");

    let attack = sym("weapon.dm", src, "attack");
    assert_eq!(attack["symbol_type"], "Function");
    assert_eq!(attack["kind_detail"], "method");

    let helper = sym("weapon.dm", src, "GlobalHelper");
    assert_eq!(helper["symbol_type"], "Function");
    assert_eq!(helper["kind_detail"], "function");

    let raw_deps = raw_deps(&r);
    assert!(
        raw_deps.iter().any(|d| d.contains("other.dm")),
        "{raw_deps:?}"
    );
}

#[test]
fn dreammaker_single_segment_type_path() {
    // A single-segment path (`/obj`) has no `type_identifier` at all — only
    // a `primitive_type` wrapping its own `identifier` child — the fallback
    // branch `dm_symbol_name` needs.
    let src = "/obj\n\tname = \"thing\"\n";
    let obj = sym("thing.dm", src, "obj");
    assert_eq!(obj["symbol_type"], "Class");
}

#[test]
fn hcl_parses_with_zero_symbols() {
    // Terraform/HCL (CONCEPT:EH-281 ABI-15 follow-up): registered for real
    // parsing but, like HTML/CSS/JSON, contributes no new vocabulary —
    // `resource`/`variable`/`module` are all the same `block` node kind,
    // distinguished only by a label string, not a function/class-shaped
    // declaration.
    let src = r#"
resource "aws_instance" "web" {
  ami           = "ami-123456"
  instance_type = "t2.micro"
}

variable "region" {
  type    = string
  default = "us-east-1"
}
"#;
    let r = parse_file("main.tf", src.as_bytes()).unwrap_or_else(|e| panic!("parse failed: {e}"));
    assert_eq!(r.symbols_extracted, 0);
}
