use super::{first_child_of_kind, get_node_text};
use std::collections::BTreeSet;
use tree_sitter::Node;

// CONCEPT:EG-KG.storage.nonblocking-checkpoint — Native test-quality metrics. Computed in the Rust compute
// layer (not Python) so "which pytests need work" is a graph fact, not a script.
const MOCK_CALLS: &[&str] = &[
    "Mock",
    "MagicMock",
    "AsyncMock",
    "NonCallableMock",
    "PropertyMock",
    "patch",
    "create_autospec",
];
const RAISES_CALLS: &[&str] = &["raises", "warns", "fail"];

#[derive(Default)]
pub(super) struct TestMetrics {
    pub(super) assert_count: usize,
    pub(super) raises_count: usize,
    pub(super) mock_count: usize,
    pub(super) calls: Vec<String>,
}

/// Last identifier of a Python `call` node's function (e.g. `pytest.raises` → `raises`).
fn py_callee_name(call_node: Node, source: &[u8]) -> Option<String> {
    let f = call_node.child_by_field_name("function")?;
    let text = get_node_text(f, source);
    text.rsplit('.').next().map(|s| s.trim().to_string())
}

/// Recursively accumulate assert/raises/mock counts and callee names over a
/// function body subtree.
pub(super) fn collect_test_metrics(node: Node, source: &[u8], m: &mut TestMetrics) {
    let kind = node.kind();
    if kind == "assert_statement" {
        m.assert_count += 1;
    } else if kind == "call" {
        if let Some(callee) = py_callee_name(node, source) {
            if RAISES_CALLS.contains(&callee.as_str()) {
                m.raises_count += 1;
            }
            if MOCK_CALLS.contains(&callee.as_str()) {
                m.mock_count += 1;
            }
            m.calls.push(callee);
        }
    }
    let mut cursor = node.walk();
    for child in node.children(&mut cursor) {
        collect_test_metrics(child, source, m);
    }
}

// CONCEPT:EG-KG.compute.type-scope-resolved-call — Type/scope-resolved call graph. A call site carries the
// receiver (`self`/`this`, a variable, or a Type for a static call — empty for a
// bare call), the callee name (last dotted/`::` segment), and the argument count.
// The cross-file resolver (`super::resolve`) binds these to the right *method on
// the receiver's class* (scope) and disambiguates same-name overloads by arity,
// instead of the old name-only match. Computed in Rust, shipped already-resolved.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct CallSite {
    pub receiver: String,
    pub callee: String,
    pub argc: Option<usize>,
}

pub(super) const MAX_SYMBOL_CALL_SITES: usize = 64;

fn retain_call_site(out: &mut BTreeSet<CallSite>, site: CallSite) {
    let _ = out.insert(site);
    if out.len() > MAX_SYMBOL_CALL_SITES {
        let _ = out.pop_last();
    }
}

/// Split a callee expression's text into (receiver, callee) by its last dotted/
/// `::` segment: `obj.method` → (`obj`,`method`), `a::b::run` → (`b`,`run`),
/// `foo` → (``,`foo`).
fn split_receiver_callee(text: &str) -> (String, String) {
    let norm = text.replace("::", ".");
    let mut segs: Vec<&str> = norm
        .split('.')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .collect();
    let callee = segs.pop().unwrap_or("").to_string();
    let receiver = segs.pop().unwrap_or("").to_string();
    (receiver, callee)
}

/// Count the arguments at a call node (named children of its `arguments` /
/// `argument_list`), or `None` when no argument list is present.
fn count_args(node: Node) -> Option<usize> {
    let args = node.child_by_field_name("arguments").or_else(|| {
        let mut c = node.walk();
        let found = node
            .children(&mut c)
            .find(|ch| matches!(ch.kind(), "argument_list" | "arguments"));
        found
    })?;
    let mut c = args.walk();
    Some(args.children(&mut c).filter(|ch| ch.is_named()).count())
}

/// Collect structured call sites within a function/method body across languages:
/// Python `call`, JS/TS/Go/Rust/C/C++ `call_expression`, Java `method_invocation`.
/// (CONCEPT:EG-KG.compute.type-scope-resolved-call; supersedes the old name-only `collect_calls`.)
pub(super) fn collect_call_sites(node: Node, source: &[u8], out: &mut BTreeSet<CallSite>) {
    match node.kind() {
        "call" | "call_expression" => {
            if let Some(f) = node.child_by_field_name("function") {
                let (receiver, callee) = split_receiver_callee(&get_node_text(f, source));
                if !callee.is_empty() {
                    retain_call_site(
                        out,
                        CallSite {
                            receiver,
                            callee,
                            argc: count_args(node),
                        },
                    );
                }
            }
        }
        "method_invocation" => {
            // Java `recv.name(args)`: `object` is the receiver, `name` the callee.
            if let Some(n) = node.child_by_field_name("name") {
                let callee = get_node_text(n, source).trim().to_string();
                if !callee.is_empty() {
                    let receiver = node
                        .child_by_field_name("object")
                        .map(|o| split_receiver_callee(&get_node_text(o, source)).1)
                        .unwrap_or_default();
                    retain_call_site(
                        out,
                        CallSite {
                            receiver,
                            callee,
                            argc: count_args(node),
                        },
                    );
                }
            }
        }
        _ => {}
    }
    let mut cursor = node.walk();
    for child in node.children(&mut cursor) {
        collect_call_sites(child, source, out);
    }
}

/// Serialize call sites onto a SYMBOL property: `recv:callee:argc` per site,
/// joined by `;` (identifiers/digits only, so the delimiters never clash). The
/// resolver decodes this with [`decode_call_sites`]; it is stripped from the
/// graph nodes after resolution (purely a resolution input).
pub(crate) fn encode_call_sites(sites: &[CallSite]) -> String {
    sites
        .iter()
        .map(|s| {
            let a = s.argc.map(|n| n.to_string()).unwrap_or_default();
            format!("{}:{}:{}", s.receiver, s.callee, a)
        })
        .collect::<Vec<_>>()
        .join(";")
}

/// A decoded call site (the resolver's view of [`encode_call_sites`]).
pub(crate) struct DecodedSite {
    pub receiver: String,
    pub callee: String,
    pub argc: Option<usize>,
}

/// Inverse of [`encode_call_sites`].
pub(crate) fn decode_call_sites(s: &str) -> Vec<DecodedSite> {
    s.split(';')
        .filter(|x| !x.is_empty())
        .filter_map(|site| {
            let mut it = site.splitn(3, ':');
            let receiver = it.next()?.to_string();
            let callee = it.next()?.to_string();
            let argc = it
                .next()
                .and_then(|a| if a.is_empty() { None } else { a.parse().ok() });
            if callee.is_empty() {
                None
            } else {
                Some(DecodedSite {
                    receiver,
                    callee,
                    argc,
                })
            }
        })
        .collect()
}

/// Formal-parameter count for a callable across grammars (the def-side `arity`
/// the resolver matches against a call's `argc`). Python excludes a leading
/// `self`/`cls`; other grammars carry the receiver outside the parameter list
/// (Go `receiver` field, Rust `self_parameter`), so a plain named-child count of
/// the parameter list is the callee-visible arity.
pub(super) fn param_count(node: Node, source: &[u8], language: &str) -> usize {
    if language == "python" {
        return py_param_count(node, source);
    }
    let params = node.child_by_field_name("parameters").or_else(|| {
        let mut c = node.walk();
        let found = node.children(&mut c).find(|ch| {
            matches!(
                ch.kind(),
                "formal_parameters" | "parameter_list" | "parameters"
            )
        });
        found
    });
    let params = match params {
        Some(p) => p,
        None => return 0,
    };
    let mut c = params.walk();
    params
        .children(&mut c)
        .filter(|ch| {
            ch.is_named()
                && !matches!(
                    ch.kind(),
                    "self_parameter" | "comment" | "line_comment" | "block_comment"
                )
        })
        .count()
}

/// Collect the last segment of every type identifier under a node (e.g. a
/// heritage clause), skipping generic type-argument lists so only the base
/// type/interface names are returned. Best-effort and tolerant of grammar shape.
fn collect_type_names(node: Node, source: &[u8], out: &mut Vec<String>) {
    let k = node.kind();
    if k.ends_with("type_identifier") || k == "identifier" {
        let t = get_node_text(node, source);
        let name = t.rsplit(['.', ':']).next().unwrap_or(&t).trim().to_string();
        if !name.is_empty() && !out.contains(&name) {
            out.push(name);
        }
        return;
    }
    if matches!(k, "type_arguments" | "type_parameters") {
        return;
    }
    let mut c = node.walk();
    for child in node.children(&mut c) {
        collect_type_names(child, source, out);
    }
}

/// Best-effort (inherits, realizes) base/interface names for a class-like node.
/// Conservative and language-scoped: Python bases, Java `extends`/`implements`,
/// TS/JS `extends`/`implements`. Rust trait impls and Go embedding need
/// impl-block / embedding analysis and are deferred (return empty) — the
/// resolver never emits a dangling edge, so an empty result is safe.
pub(super) fn class_relations(
    node: Node,
    source: &[u8],
    language: &str,
) -> (Vec<String>, Vec<String>) {
    match language {
        "python" => (py_class_bases(node, source), Vec::new()),
        "java" => {
            let mut inh = Vec::new();
            let mut real = Vec::new();
            if let Some(sc) = node.child_by_field_name("superclass") {
                collect_type_names(sc, source, &mut inh);
            }
            if let Some(ifs) = node.child_by_field_name("interfaces") {
                collect_type_names(ifs, source, &mut real);
            }
            (inh, real)
        }
        "typescript" | "javascript" => {
            let mut inh = Vec::new();
            let mut real = Vec::new();
            let mut c = node.walk();
            for ch in node.children(&mut c) {
                if ch.kind() == "class_heritage" {
                    let mut c2 = ch.walk();
                    for clause in ch.children(&mut c2) {
                        append_heritage_relation(clause, source, &mut inh, &mut real);
                    }
                }
            }
            (inh, real)
        }
        _ => (Vec::new(), Vec::new()),
    }
}

fn append_heritage_relation(
    clause: Node,
    source: &[u8],
    inherits: &mut Vec<String>,
    realizes: &mut Vec<String>,
) {
    match clause.kind() {
        "extends_clause" => collect_type_names(clause, source, inherits),
        "implements_clause" => collect_type_names(clause, source, realizes),
        _ => {}
    }
}

/// Count a Python function's parameters, excluding a leading `self`/`cls`.
pub(super) fn py_param_count(node: Node, source: &[u8]) -> usize {
    let params = match node.child_by_field_name("parameters") {
        Some(p) => p,
        None => return 0,
    };
    let mut n = 0usize;
    let mut cursor = params.walk();
    for child in params.children(&mut cursor) {
        match child.kind() {
            "identifier"
            | "typed_parameter"
            | "default_parameter"
            | "typed_default_parameter"
            | "list_splat_pattern"
            | "dictionary_splat_pattern" => {
                let txt = get_node_text(child, source);
                let base = txt.trim_start_matches('*');
                let first = base.split([':', '=']).next().unwrap_or("").trim();
                if first != "self" && first != "cls" && !first.is_empty() {
                    n += 1;
                }
            }
            _ => {}
        }
    }
    n
}

/// Collect decorator source strings for a function (handles `decorated_definition`).
pub(super) fn py_decorators(node: Node, source: &[u8]) -> Vec<String> {
    let mut out = Vec::new();
    if let Some(parent) = node.parent() {
        if parent.kind() == "decorated_definition" {
            let mut cursor = parent.walk();
            for child in parent.children(&mut cursor) {
                if child.kind() == "decorator" {
                    out.push(get_node_text(child, source).trim().to_string());
                }
            }
        }
    }
    out
}

/// Base-class names of a Python `class_definition` (from the `superclasses` field).
pub(super) fn py_class_bases(node: Node, source: &[u8]) -> Vec<String> {
    let mut bases = Vec::new();
    if let Some(supers) = node.child_by_field_name("superclasses") {
        let mut cursor = supers.walk();
        for child in supers.children(&mut cursor) {
            match child.kind() {
                "identifier" | "attribute" => {
                    bases.push(get_node_text(child, source));
                }
                "keyword_argument" => {
                    // e.g. metaclass=ABCMeta — record the value side.
                    if let Some(v) = child.child_by_field_name("value") {
                        bases.push(get_node_text(v, source));
                    }
                }
                _ => {}
            }
        }
    }
    bases
}

/// Member method names of a Python class + whether any is `@abstractmethod`.
pub(super) fn py_class_methods(node: Node, source: &[u8]) -> (Vec<String>, bool) {
    let mut methods = Vec::new();
    let mut has_abstract = false;
    let Some(body) = node.child_by_field_name("body") else {
        return (methods, has_abstract);
    };
    let mut cursor = body.walk();
    for child in body.children(&mut cursor) {
        let fdef = match child.kind() {
            "decorated_definition" => {
                let mut c2 = child.walk();
                let function = child
                    .children(&mut c2)
                    .find(|n| n.kind() == "function_definition");
                function
            }
            "function_definition" => Some(child),
            _ => None,
        };
        let Some(f) = fdef else {
            continue;
        };
        if let Some(n) = f.child_by_field_name("name") {
            methods.push(get_node_text(n, source));
        }
        has_abstract |= py_decorators(f, source)
            .iter()
            .any(|decorator| decorator.contains("abstractmethod"));
    }
    (methods, has_abstract)
}

/// Extract pytest mark names from decorator strings (`@pytest.mark.skip` → `skip`).
pub(super) fn py_marks(decorators: &[String]) -> Vec<String> {
    let mut marks = Vec::new();
    for d in decorators {
        if let Some(idx) = d.find(".mark.") {
            let rest = &d[idx + ".mark.".len()..];
            let name: String = rest
                .chars()
                .take_while(|c| c.is_alphanumeric() || *c == '_')
                .collect();
            if !name.is_empty() {
                marks.push(name);
            }
        }
    }
    marks
}

/// Semantic kind for a type-like declaration node across grammars, or ``None``.
/// Covers Python/JS/TS/Java/C#/C/C++/Rust/Go type containers so every language's
/// classes, structs, interfaces, enums, traits, etc. surface as ``Class`` symbols
/// (the detail string preserves the precise kind for classification).
pub(super) fn class_like_kind(kind: &str) -> Option<&'static str> {
    class_declaration_kind(kind).or_else(|| extended_class_kind(kind))
}

fn class_declaration_kind(kind: &str) -> Option<&'static str> {
    match kind {
        "class_definition"
        | "class_declaration"
        | "class_specifier"
        | "abstract_class_declaration" => "class",
        "interface_declaration" => "interface",
        // `struct_definition` Julia (CONCEPT:EH-281).
        "struct_specifier" | "struct_item" | "struct_declaration" | "struct_definition" => "struct",
        "enum_declaration" | "enum_item" | "enum_specifier" => "enum",
        // `trait_item` Rust, `trait_definition` Scala, `trait_declaration` PHP.
        "trait_item" | "trait_definition" | "trait_declaration" => "trait",
        "union_item" | "union_specifier" => "union",
        "record_declaration" | "record_struct_declaration" => "record",
        // Julia `abstract type Shape end` (CONCEPT:EH-281) — a type with no
        // fields/methods of its own, closest existing concept is `interface`.
        "abstract_definition" => "abstract_type",
        _ => return None,
    }
    .into()
}

fn extended_class_kind(kind: &str) -> Option<&'static str> {
    match kind {
        "namespace_definition" => "namespace",
        // Ruby `class`/`module`; Scala `object_definition` (CONCEPT:AU-KG.compute.built-ast-extended).
        // Fortran's own outer `module` container (CONCEPT:EH-281) reuses this
        // same node-kind string; its name resolution differs (see
        // `fortran_symbol_name`), so it needs no separate table row.
        "class" => "class",
        "module" => "module",
        "object_definition" => "object",
        // Go puts struct/interface names on the type_spec under a type_declaration.
        "type_spec" | "type_alias_declaration" => "type",
        // Objective-C (CONCEPT:EH-281) `@interface`/`@implementation` — the
        // class name is an unnamed positional child, not a `name` field (see
        // `identifier_child_name`'s doc for why `symbol_name` still resolves it).
        "class_interface" | "class_implementation" => "class",
        // Julia `module_definition` (CONCEPT:EH-281) — has a `name` field,
        // resolved by `symbol_name`'s existing first check.
        "module_definition" => "module",
        // Pascal/Delphi's `unit` (whole-file container) is deliberately NOT
        // mapped here (CONCEPT:EH-281): its own kind string, `"unit"`, is
        // ALSO CSS's node kind for a length-unit token (`px` in `10px`) —
        // confirmed via `tree-sitter-css`'s `node-types.json`, a real
        // cross-grammar kind-string collision (every other shared kind
        // string in this table, like `"class"`/`"module"`, means the same
        // thing in both grammars that use it; this one doesn't). Harmless in
        // practice (a CSS `unit` leaf has no name-bearing field or child, so
        // `symbol_name` already resolves to `None` for it and no symbol is
        // emitted either way) but not worth carrying: the Pascal `unit`'s
        // own name is redundant with the file's path, so this lane extracts
        // only its functions and imports (see `pascal_symbol_name`), not a
        // Module symbol for the file container itself.
        // PowerShell `class Widget { ... }` (CONCEPT:EH-281); name resolved
        // by `powershell_symbol_name`.
        "class_statement" => "class",
        // DreamMaker `/obj/item/weapon` (CONCEPT:EH-281 ABI-15 follow-up) —
        // name resolved by `dm_symbol_name`.
        "type_definition" => "class",
        _ => return None,
    }
    .into()
}

/// Callable declaration node kinds across grammars, and the semantic kind
/// each one is. A closed table: a kind not listed is not a callable.
const FUNCTION_LIKE_KINDS: &[(&str, &str)] = &[
    ("function_definition", "function"),
    ("function_declaration", "function"),
    ("function_item", "function"),
    ("generator_function_declaration", "function"),
    // Ruby `method`/`singleton_method` (CONCEPT:AU-KG.compute.built-ast-extended).
    ("method_definition", "method"),
    ("method_declaration", "method"),
    ("method", "method"),
    ("singleton_method", "method"),
    ("constructor_declaration", "constructor"),
    // Julia `macro mymacro(x) ... end` (CONCEPT:EH-281) — a compile-time
    // callable, closest existing concept is a function.
    ("macro_definition", "macro"),
    // Fortran's outer `function`/`subroutine` containers (CONCEPT:EH-281);
    // name resolved by `fortran_symbol_name` (the field lives on the
    // nested `function_statement`/`subroutine_statement`, not here).
    ("function", "function"),
    ("subroutine", "subroutine"),
    // Pascal/Delphi `defProc` (CONCEPT:EH-281) — a DEFINING occurrence
    // (header + body), the Pascal analogue of a C `function_definition`.
    // The bare forward declaration (`declProc` alone, in an `interface`
    // section) is deliberately NOT listed, mirroring how a C
    // prototype-only `declaration` never matches this table either —
    // only the defining occurrence is extracted.
    ("defProc", "function"),
    // PowerShell `function Get-Area { ... }` / a class method
    // (CONCEPT:EH-281); name resolved by `powershell_symbol_name`.
    ("function_statement", "function"),
    ("class_method_definition", "method"),
    // DreamMaker (CONCEPT:EH-281 ABI-15 follow-up). Both already carry a
    // `name` field, so `symbol_name`'s first check resolves them — no
    // `dm_symbol_name` involvement, unlike `type_definition`.
    ("proc_definition", "function"),
    ("type_proc_definition", "method"),
];

/// Semantic kind for a callable declaration node across grammars, or ``None``.
pub(super) fn function_like_kind(kind: &str) -> Option<&'static str> {
    FUNCTION_LIKE_KINDS
        .iter()
        .find(|(node_kind, _)| *node_kind == kind)
        .map(|(_, semantic)| *semantic)
}

/// Best-effort symbol name across grammars. Most declarations expose a ``name``
/// field; C/C++ functions nest the identifier under ``declarator`` and Rust
/// ``impl`` blocks use ``type``, so fall back to those. Some grammars
/// (Verilog, Objective-C — CONCEPT:EH-281) carry the identifier as an
/// unnamed POSITIONAL child instead of any field at all, so
/// [`identifier_child_name`] is tried next. A further tier of grammars
/// (Julia, Fortran, Pascal, PowerShell — CONCEPT:EH-281) nest the identifier
/// even deeper, inside a language-specific wrapper shape that doesn't fit
/// [`identifier_child`]'s generic bounded descent, so [`extended_symbol_name`]
/// is the last resort, dispatched by `language` since these wrapper shapes
/// are not distinguishable by node kind alone across grammars.
pub(super) fn symbol_name(node: Node, source: &[u8], language: &str) -> Option<String> {
    if let Some(n) = node.child_by_field_name("name") {
        return Some(get_node_text(n, source));
    }
    if let Some(decl) = node.child_by_field_name("declarator") {
        if let Some(name) = innermost_declarator_name(decl, source) {
            return Some(name);
        }
    }
    if let Some(t) = node.child_by_field_name("type") {
        return Some(get_node_text(t, source));
    }
    identifier_child_name(node, source).or_else(|| extended_symbol_name(node, source, language))
}

/// Language-dispatched name resolution for declaration shapes
/// [`identifier_child`]'s generic positional-child descent doesn't cover
/// (CONCEPT:EH-281). A `match` (not a chain of `if let`s tried against every
/// language) so this stays O(1) cyclomatic as the tier grows, and so a
/// grammar with no special case here costs nothing (falls straight to the
/// `_ => None` arm).
fn extended_symbol_name(node: Node, source: &[u8], language: &str) -> Option<String> {
    match language {
        "julia" => julia_symbol_name(node, source),
        "fortran" => fortran_symbol_name(node, source),
        "pascal" => pascal_symbol_name(node, source),
        "powershell" => powershell_symbol_name(node, source),
        "dreammaker" => dm_symbol_name(node, source),
        _ => None,
    }
}

/// Julia (CONCEPT:EH-281): `function_definition`/`macro_definition` nest
/// their name inside a `signature` child that is itself a `call_expression`
/// (`function foo(x) ... end` parses exactly like a call to `foo`) — neither
/// `signature` nor `call_expression` is a field, so this reaches two levels
/// deep by node KIND. `struct_definition`/`abstract_definition` nest their
/// name inside a `type_head` child instead. Does not resolve an
/// operator-overload definition (`function Base.:+(a, b)`) or a `where`-
/// clause generic signature — both leave the callee's `identifier` one
/// level deeper than this reaches; an honest, undetected gap rather than a
/// silently wrong name.
fn julia_symbol_name(node: Node, source: &[u8]) -> Option<String> {
    let mut cursor = node.walk();
    for child in node.children(&mut cursor) {
        match child.kind() {
            "type_head" => {
                if let Some(id) = first_child_of_kind(child, "identifier") {
                    return Some(get_node_text(id, source));
                }
            }
            "signature" => {
                let call = first_child_of_kind(child, "call_expression")?;
                if let Some(id) = first_child_of_kind(call, "identifier") {
                    return Some(get_node_text(id, source));
                }
            }
            _ => {}
        }
    }
    None
}

/// Fortran (CONCEPT:EH-281): the outer `module`/`function`/`subroutine`
/// container (the node this is called on, spanning the whole declaration
/// including its body) has no fields at all — the identifier lives one
/// level down, on its first child, a `module_statement`/`function_statement`/
/// `subroutine_statement` (the grammar's own naming convention: the
/// container's FIRST child is always that container's own `*_statement`), as
/// a child of kind `name`. That `name` child is field-tagged on
/// `function_statement`/`subroutine_statement` but — a real grammar
/// inconsistency, confirmed empirically, not by `node-types.json`'s field
/// list, which claims otherwise — NOT on `module_statement`, so this matches
/// by KIND rather than by field to cover both.
fn fortran_symbol_name(node: Node, source: &[u8]) -> Option<String> {
    let mut cursor = node.walk();
    let stmt = node
        .children(&mut cursor)
        .find(|c| c.kind().ends_with("_statement"))?;
    first_child_of_kind(stmt, "name").map(|c| get_node_text(c, source))
}

/// Pascal/Delphi `defProc` (CONCEPT:EH-281) — a defining function/method
/// occurrence. The name lives on its `header` field's own `name` field
/// (`header` is a nested `declProc`). A free function's `name` field is a
/// plain `identifier`; a method implementation's
/// (`function TPoint.Area: Integer;`) is a `genericDot` (`TPoint.Area`)
/// whose `rhs` field is the bare method name — `symbol_name` always returns
/// the BARE name (qualification is a separate concern), so this reads `rhs`
/// rather than the whole dotted text. (Pascal's own whole-file `unit`
/// container is deliberately not resolved here — see the `extended_class_kind`
/// comment on why it isn't registered as class-like at all.)
fn pascal_symbol_name(node: Node, source: &[u8]) -> Option<String> {
    let header = node.child_by_field_name("header")?;
    let name = header.child_by_field_name("name")?;
    match name.kind() {
        "genericDot" => name
            .child_by_field_name("rhs")
            .map(|n| get_node_text(n, source)),
        _ => Some(get_node_text(name, source)),
    }
}

/// PowerShell (CONCEPT:EH-281): `function_statement`/`class_statement`/
/// `class_method_definition` all carry their identifier as an unnamed
/// positional child, but a LEAF one (`function_name`/`simple_name`) whose
/// own text IS the name — no further descent needed, unlike
/// [`identifier_child`]'s wrapper-then-identifier shape.
fn powershell_symbol_name(node: Node, source: &[u8]) -> Option<String> {
    let mut cursor = node.walk();
    let found = node
        .children(&mut cursor)
        .find(|c| matches!(c.kind(), "function_name" | "simple_name"));
    found.map(|c| get_node_text(c, source))
}

/// DreamMaker `type_definition` (CONCEPT:EH-281 ABI-15 follow-up) — a class
/// path like `/obj/item/weapon`. The name is the LAST identifier-shaped
/// segment of its `type_path` child (`weapon`), not a field: most segments
/// are `type_identifier` leaves, but a single-segment path (`/obj`) has only
/// a `primitive_type` wrapping its own `identifier` child, so that is the
/// fallback last segment. (`proc_definition`/`type_proc_definition` — DM's
/// functions/methods — both already carry a `name` field and never reach
/// this function; see `function_like_kind`.)
fn dm_symbol_name(node: Node, source: &[u8]) -> Option<String> {
    let type_path = first_child_of_kind(node, "type_path")?;
    let mut inner = type_path.walk();
    let mut last = None;
    for child in type_path.children(&mut inner) {
        if matches!(child.kind(), "type_identifier" | "primitive_type") {
            last = Some(child);
        }
    }
    let last = last?;
    if last.kind() == "type_identifier" {
        return Some(get_node_text(last, source));
    }
    first_child_of_kind(last, "identifier").map(|c| get_node_text(c, source))
}

/// Fall back to an unnamed identifier-SHAPED child when no field
/// (`name`/`declarator`/`type`) resolves a declaration's name.
///
/// Some grammars carry the identifier as a plain positional child rather
/// than a field: Verilog's `class_declaration` nests a `class_identifier`
/// child with no field name at all, and Objective-C's
/// `class_interface`/`class_implementation` carry their name as a bare
/// `identifier` child ahead of the `category`/`superclass` FIELDS (which are
/// also `identifier`-kinded, but — unlike the name — carry a field name).
/// Others nest the identifier one level DEEPER, inside a single
/// signature-shaped wrapper production: Verilog's `function_declaration`
/// wraps everything in `function_body_declaration`, and
/// `interface_declaration` wraps its header in
/// `interface_ansi_header`/`interface_nonansi_header`. [`identifier_child`]
/// bounds that descent to 2 levels and only follows a child whose kind ends
/// in `_header` or `_body_declaration` — never a statement/body child — so
/// it can't wander into an unrelated identifier deep in a function's body.
fn identifier_child_name(node: Node, source: &[u8]) -> Option<String> {
    identifier_child(node, 2).map(|n| get_node_text(n, source))
}

fn identifier_child(node: Node, depth: u8) -> Option<Node> {
    if depth == 0 {
        return None;
    }
    let children = unfielded_children(node);
    if let Some(identifier) = children
        .iter()
        .find(|child| is_identifier_kind(child.kind()))
    {
        return Some(*identifier);
    }
    children
        .into_iter()
        .find(|child| is_wrapper_kind(child.kind()))
        .and_then(|wrapper| identifier_child(wrapper, depth - 1))
}

/// The direct children that carry no field name, in order.
fn unfielded_children(node: Node) -> Vec<Node> {
    let mut children = Vec::new();
    let mut cursor = node.walk();
    if !cursor.goto_first_child() {
        return children;
    }
    loop {
        if cursor.field_name().is_none() {
            children.push(cursor.node());
        }
        if !cursor.goto_next_sibling() {
            return children;
        }
    }
}

fn is_identifier_kind(kind: &str) -> bool {
    kind == "identifier" || kind.ends_with("_identifier")
}

/// A `*_header` / `*_body_declaration` wrapper one level above the name.
fn is_wrapper_kind(kind: &str) -> bool {
    kind.ends_with("_header") || kind.ends_with("_body_declaration")
}

/// Descend a C/C++ declarator chain (pointer/function/array declarators) to the
/// innermost identifier — that's the function/variable name.
fn innermost_declarator_name(node: Node, source: &[u8]) -> Option<String> {
    match node.kind() {
        "identifier"
        | "field_identifier"
        | "type_identifier"
        | "qualified_identifier"
        | "destructor_name"
        | "operator_name" => return Some(get_node_text(node, source)),
        _ => {}
    }
    if let Some(inner) = node.child_by_field_name("declarator") {
        if let Some(name) = innermost_declarator_name(inner, source) {
            return Some(name);
        }
    }
    let mut cursor = node.walk();
    for child in node.children(&mut cursor) {
        if let Some(name) = innermost_declarator_name(child, source) {
            return Some(name);
        }
    }
    None
}

// ── CONCEPT:EG-KG.compute.qualified-symbol — lexical qualification of symbols ──
//
// `name` is BARE, so two `run` methods on different types are indistinguishable
// downstream. `qualified_symbol` stamps the symbol's LEXICAL ancestor chain
// (containers actually present in the AST) joined by the language's scope
// separator: `Outer.Inner.method` (Python), `inner::Thing::method` (Rust).
//
// Deterministic rules — chosen for stability, not elegance, because these strings
// key an external inventory:
//   * Containers that contribute a segment: every class-like and function-like
//     declaration (so nested functions/methods qualify), plus Rust `mod_item` and
//     `impl_item`, which are NOT class-like and would otherwise be invisible.
//   * `impl Type` contributes `Type`; `impl Trait for Type` contributes
//     `<Type as Trait>` — Rust's own unambiguous disambiguation syntax, so an
//     inherent `new` and a trait `new` on the same type do not collide.
//   * Generic arguments and references are stripped from an impl's type
//     (`Foo<T>` / `&Foo` → `Foo`), so a symbol's qualified name does not move
//     when a type parameter is renamed.
//   * A container with no readable name (anonymous impls of an unnamed type,
//     closures, expression-position lambdas) contributes NO segment and is
//     skipped — its children qualify against the nearest named ancestor.
//   * The FILE/module/crate prefix is deliberately NOT included: it is not in the
//     AST and its derivation needs repository layout (Python package roots) and
//     Cargo metadata (Rust crate + file-module path). The inventory driver
//     composes `<module prefix><sep><qualified_symbol>`.

/// Scope separator for a language's qualified names.
fn scope_sep(language: &str) -> &'static str {
    match language {
        "rust" | "c" | "cpp" => "::",
        _ => ".",
    }
}

/// Collapse whitespace runs so a segment read from multi-line source stays a
/// single stable token.
fn squash_ws(s: &str) -> String {
    s.split_whitespace().collect::<Vec<_>>().join(" ")
}

/// Base of a Rust type expression: strip generic arguments and references so
/// `Foo<T>`, `&Foo` and `&mut Foo<'a, T>` all qualify as `Foo`.
fn rust_type_base(node: Node, source: &[u8]) -> String {
    match node.kind() {
        "generic_type" | "reference_type" => node
            .child_by_field_name("type")
            .map(|n| rust_type_base(n, source))
            .unwrap_or_else(|| squash_ws(&get_node_text(node, source))),
        _ => squash_ws(&get_node_text(node, source)),
    }
}

/// Qualification segment for a Rust `impl_item`: `Type`, or `<Type as Trait>`
/// for a trait impl.
fn rust_impl_segment(node: Node, source: &[u8]) -> Option<String> {
    let ty = rust_type_base(node.child_by_field_name("type")?, source);
    if ty.is_empty() {
        return None;
    }
    Some(match node.child_by_field_name("trait") {
        Some(tr) => {
            let t = rust_type_base(tr, source);
            if t.is_empty() {
                ty
            } else {
                format!("<{ty} as {t}>")
            }
        }
        None => ty,
    })
}

/// The segment this node contributes to its DESCENDANTS' qualified names, or
/// `None` when it contributes nothing.
pub(super) fn qual_segment(node: Node, source: &[u8], language: &str) -> Option<String> {
    let kind = node.kind();
    if language == "rust" {
        match kind {
            // Rust inline modules are containers but not `class_like_kind`.
            "mod_item" => {
                return node
                    .child_by_field_name("name")
                    .map(|n| squash_ws(&get_node_text(n, source)))
                    .filter(|n| !n.is_empty())
            }
            "impl_item" => return rust_impl_segment(node, source),
            _ => {}
        }
    }
    if class_like_kind(kind).is_some() || function_like_kind(kind).is_some() {
        return symbol_name(node, source, language)
            .map(|n| squash_ws(&n))
            .filter(|n| !n.is_empty());
    }
    None
}

/// Join an ancestor chain and a bare name into a qualified symbol name.
pub(super) fn join_qualified(qual: &[String], name: &str, language: &str) -> String {
    if qual.is_empty() {
        return name.to_string();
    }
    let sep = scope_sep(language);
    format!("{}{}{}", qual.join(sep), sep, name)
}
