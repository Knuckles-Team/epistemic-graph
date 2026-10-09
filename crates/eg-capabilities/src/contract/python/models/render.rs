//! Render hoisted definitions as a generated module body (`models.py`, a DTO surface
//! module or `_shared.py`), formatter-stable by construction.
//!
//! Every emitted line fits the 88-column limit without the formatter's help: a field
//! whose annotation is too long first tries the parenthesized form the formatter
//! itself would produce for a flat union, and otherwise gets a named alias; an alias
//! whose right-hand side is too long is split the way the formatter splits it
//! (one union member per line, an exploded `Annotated[...]` with magic trailing
//! commas, a one-item `list[...]`). Classes come first in name order; aliases follow
//! in dependency order, because an alias is evaluated when the module is imported.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;

use serde_json::{Map, Value};

use super::super::dto::{
    canonical_digest_spec, dto_python_type, pascal_case, push_dto_definition, push_string_enum,
    push_type_alias, push_union_alias, ref_name, string_literal_variants, tagged_variants,
    variant_tag, JSON_VALUE_TYPES, SCOPED_PATCH_DIGEST_SPECS,
};
use super::super::names::field_identifier;
use super::super::FieldPresence;
use super::hoist::fresh_name;

const WIDTH: usize = 88;
const MODEL_CONFIG: &str =
    "    model_config = ConfigDict(extra=\"forbid\", frozen=True, defer_build=True)\n";

/// One rendered top-level statement.
struct Block {
    alias: bool,
    text: String,
}

/// The renderer's state: the definitions, and aliases minted to keep lines short.
struct Renderer<'a> {
    definitions: &'a Map<String, Value>,
    minted: BTreeMap<String, Value>,
}

/// The body of one generated module: every definition `owned` accepts, rendered
/// once. Minted alias names stay unique against every definition, so a module
/// never mints a name another module exports.
pub(in super::super) fn render_owned(
    definitions: &Map<String, Value>,
    owned: &dyn Fn(&str) -> bool,
) -> String {
    let mut renderer = Renderer {
        definitions,
        minted: BTreeMap::new(),
    };
    let mut blocks: BTreeMap<String, Vec<Block>> = BTreeMap::new();
    for (name, node) in definitions.iter().filter(|(name, _)| owned(name)) {
        blocks.insert(name.clone(), renderer.definition(name, node));
    }
    renderer.render_minted(&mut blocks);
    let body = ordered_body(&blocks);
    format!("{}{body}", validation_helpers(&body))
}

/// Emit shared field validators only in modules that use their annotations.
fn validation_helpers(body: &str) -> String {
    let mut helpers = String::new();
    if body.contains("_eg_utf8_text(") {
        helpers.push_str(
            r#"

def _eg_utf8_text(max_bytes):
    def validate(value):
        if value is not None:
            if not value or len(value.encode("utf-8")) > max_bytes:
                raise ValueError("engine text violates UTF-8 byte bounds")
            if any(ord(char) < 0x20 for char in value):
                raise ValueError("engine text contains a control byte")
        return value

    return validate
"#,
        );
    }
    if body.contains("_eg_nonblank") {
        // Rust char::is_whitespace uses Unicode White_Space; Python strip also
        // treats U+001C..U+001F as whitespace, so spell out the Rust set.
        helpers.push_str(
            r#"

def _eg_nonblank(value):
    whitespace = "\t\n\v\f\r \x85\xa0\u1680\u2000\u2001\u2002\u2003\u2004"
    whitespace += "\u2005\u2006\u2007\u2008\u2009\u200a\u2028\u2029\u202f\u205f\u3000"
    if value is not None and not value.strip(whitespace):
        raise ValueError("engine identifier must not be blank")
    return value
"#,
        );
    }
    if body.contains("_eg_unique_items") {
        helpers.push_str(
            r#"

def _eg_unique_items(value):
    if value is not None and len(set(value)) != len(value):
        raise ValueError("engine list items must be unique")
    return value
"#,
        );
    }
    helpers
}

/// The variant classes the renderer emits for definition `name` (a tagged union's
/// member classes), in declaration order; empty for every other definition. A module
/// that re-exports the union alias re-exports these too, so callers can construct and
/// `isinstance`-check a variant from the module they import the union from.
pub(in super::super) fn member_classes(name: &str, node: &Value) -> Vec<String> {
    if JSON_VALUE_TYPES.contains(&name) || has_digest_methods(name) || enum_values(node).is_some() {
        return Vec::new();
    }
    tagged_variants(node)
        .map(|(tag, variants)| emitted_names(name, tag, &variants))
        .unwrap_or_default()
}

/// Every name a tagged union's variants are emitted under: each variant's own
/// name, preceded -- when the variant is itself a tagged union -- by the names
/// of its members.
fn emitted_names(name: &str, tag: &str, variants: &[&Value]) -> Vec<String> {
    let mut names = Vec::new();
    for (class, variant) in variant_classes(name, tag, variants)
        .into_iter()
        .zip(variants)
    {
        if let Some((inner_tag, inner)) = tagged_variants(variant) {
            names.extend(emitted_names(&class, inner_tag, &inner));
        }
        names.push(class);
    }
    names
}

/// One member of a variant that is itself a tagged union, carrying the outer
/// variant's own fields (its tag) as well as its own: an internally tagged
/// enum that wraps another puts both tags on one wire object.
fn member_with_outer_fields(outer: &Value, member: &Value) -> Value {
    let mut merged = member.clone();
    let outer_properties = outer.get("properties").and_then(Value::as_object);
    for (field, schema) in outer_properties.into_iter().flatten() {
        merged["properties"][field] = schema.clone();
    }
    let mut required: BTreeSet<&str> = BTreeSet::new();
    for node in [outer, member] {
        let listed = node.get("required").and_then(Value::as_array);
        required.extend(listed.into_iter().flatten().filter_map(Value::as_str));
    }
    merged["required"] = required.into_iter().map(Value::from).collect();
    merged
}

/// `{Union}{PascalTag}` for each variant of a tagged union.
fn variant_classes(name: &str, tag: &str, variants: &[&Value]) -> Vec<String> {
    variants
        .iter()
        .map(|variant| {
            let value = variant_tag(variant, tag).expect("tagged variant has a tag");
            format!("{name}{}", pascal_case(value))
        })
        .collect()
}

impl Renderer<'_> {
    fn definition(&mut self, name: &str, node: &Value) -> Vec<Block> {
        if JSON_VALUE_TYPES.contains(&name) {
            return vec![alias_block(format!("\n\n{name} = Any\n"))];
        }
        if has_digest_methods(name) {
            let mut text = String::new();
            push_dto_definition(&mut text, name, node);
            // A digest subclass of a referenced base must follow every class.
            let alias = ref_name(node).is_some();
            return vec![Block { alias, text }];
        }
        if let Some(values) = enum_values(node) {
            let mut text = String::new();
            push_string_enum(&mut text, name, values);
            return vec![class_block(text)];
        }
        if let Some((tag, variants)) = tagged_variants(node) {
            return vec![class_block(self.tagged_union(name, tag, &variants))];
        }
        if is_object(node) {
            return vec![class_block(self.object_class(name, node))];
        }
        vec![alias_block(self.alias(name, node))]
    }

    fn render_minted(&mut self, blocks: &mut BTreeMap<String, Vec<Block>>) {
        let mut done = BTreeSet::new();
        while let Some((name, node)) = self
            .minted
            .iter()
            .find(|(name, _)| !done.contains(*name))
            .map(|(name, node)| (name.clone(), node.clone()))
        {
            done.insert(name.clone());
            let text = self.alias(&name, &node);
            blocks.insert(name, vec![alias_block(text)]);
        }
    }

    fn tagged_union(&mut self, name: &str, tag: &str, variants: &[&Value]) -> String {
        let mut text = String::new();
        let names = variant_classes(name, tag, variants);
        for (class, variant) in names.iter().zip(variants) {
            text.push_str(&self.variant(class, variant));
        }
        let (discriminator, _) = field_identifier(tag);
        push_union_alias(&mut text, name, &discriminator, &names);
        text
    }

    /// One variant of a tagged union: a class -- or, when the variant is itself
    /// a tagged union, a nested union under the variant's name whose every
    /// member class carries both tags, so the outer discriminator still selects
    /// it and the inner one selects within it.
    fn variant(&mut self, class: &str, variant: &Value) -> String {
        let Some((inner_tag, inner)) = tagged_variants(variant) else {
            return self.object_class(class, variant);
        };
        let members: Vec<Value> = inner
            .iter()
            .map(|member| member_with_outer_fields(variant, member))
            .collect();
        let members: Vec<&Value> = members.iter().collect();
        self.tagged_union(class, inner_tag, &members)
    }

    fn object_class(&mut self, name: &str, node: &Value) -> String {
        let mut text = format!("\n\nclass {name}(BaseModel):\n{MODEL_CONFIG}\n");
        let required: Vec<&str> = node
            .get("required")
            .and_then(Value::as_array)
            .map(|values| values.iter().filter_map(Value::as_str).collect())
            .unwrap_or_default();
        let properties = node.get("properties").and_then(Value::as_object);
        match properties {
            Some(properties) if !properties.is_empty() => {
                for (field, schema) in properties {
                    let required = required.contains(&field.as_str());
                    text.push_str(&self.field(name, field, schema, required));
                }
            }
            _ => text.push_str("    pass\n"),
        }
        text
    }

    /// One field, in the first form that fits: flat, a parenthesized flat union, or
    /// an alias minted for the annotation (flat, or with its `Field(...)` default split).
    fn field(&mut self, owner: &str, wire: &str, schema: &Value, required: bool) -> String {
        let (identifier, aliased) = field_identifier(wire);
        let presence = FieldPresence::of(required, schema);
        let mut annotation = dto_python_type(schema);
        for minted in [false, true] {
            let (declared, default) = presence.declaration(&annotation, aliased.then_some(wire));
            let default = default
                .map(|value| format!(" = {value}"))
                .unwrap_or_default();
            let line = format!("    {identifier}: {declared}{default}");
            if width(&line) <= WIDTH {
                return format!("{line}\n");
            }
            if is_flat_union(&declared) && width(&declared) + 8 <= WIDTH {
                return format!("    {identifier}: (\n        {declared}\n    ){default}\n");
            }
            // A minted alias is a bare name, so the formatter splits the default's call.
            if let Some(line) = minted
                .then(|| split_default_call(&identifier, &declared, &default))
                .flatten()
            {
                return line;
            }
            annotation = self.mint(&format!("{owner}{}", pascal_case(wire)), schema);
        }
        panic!("{owner}.{wire}: no field form fits the formatter's line limit");
    }

    /// A named alias for `schema`, rendered after the definitions.
    fn mint(&mut self, base: &str, schema: &Value) -> String {
        let name = fresh_name(base, |candidate| {
            self.definitions.contains_key(candidate) || self.minted.contains_key(candidate)
        });
        self.minted.insert(name.clone(), schema.clone());
        name
    }

    /// `Name = annotation`, split the way the formatter splits it when too long.
    fn alias(&mut self, name: &str, node: &Value) -> String {
        // `dto_python_type` has no tuple form; tuples are always laid out here.
        if let Some(items) = tuple_items(node) {
            return self.tuple_alias(name, node, items);
        }
        let annotation = dto_python_type(node);
        if width(name) + width(&annotation) + 3 <= WIDTH {
            return format!("\n\n{name} = {annotation}\n");
        }
        if let Some(text) = self.validated_alias(name, node) {
            return text;
        }
        if let Some(members) = union_members(node) {
            return self.union_alias(name, &members);
        }
        if let Some(item) = single_item(node) {
            let item = self.mint(&format!("{name}Item"), item);
            return bracket_alias(name, "list[", &item);
        }
        if let Some(value) = map_value(node) {
            let value = self.mint(&format!("{name}Value"), value);
            return bracket_alias(name, "dict[str, ", &value);
        }
        let mut text = String::new();
        push_type_alias(&mut text, name, &annotation);
        text
    }

    /// Keep outer field validators when a long nullable annotation is hoisted.
    fn validated_alias(&mut self, name: &str, node: &Value) -> Option<String> {
        let mut validators = Vec::new();
        if let Some(limit) = node.get("x-eg-utf8-max-bytes").and_then(Value::as_u64) {
            validators.push(format!("AfterValidator(_eg_utf8_text({limit}))"));
        }
        if node.get("x-eg-nonblank").and_then(Value::as_bool) == Some(true) {
            validators.push("AfterValidator(_eg_nonblank)".to_string());
        }
        if validators.is_empty() {
            return None;
        }
        let mut inner = node.clone();
        let object = inner
            .as_object_mut()
            .expect("validated field schema is an object");
        for key in [
            "x-eg-utf8-max-bytes",
            "x-eg-no-control-bytes",
            "x-eg-nonblank",
        ] {
            object.remove(key);
        }
        let inner_name = self.mint(&format!("{name}Value"), &inner);
        let mut text = format!("\n\n{name} = Annotated[\n    {inner_name},\n");
        for validator in validators {
            let _ = writeln!(text, "    {validator},");
        }
        text.push_str("]\n");
        Some(text)
    }

    /// A fixed-width tuple: every item that is not a bare name gets its own alias
    /// (`{Name}Item{i}`), and the length bounds explode with magic trailing commas.
    fn tuple_alias(&mut self, name: &str, node: &Value, items: &[Value]) -> String {
        let mut rendered = Vec::new();
        for (index, item) in items.iter().enumerate() {
            let annotation = dto_python_type(item);
            let bare = annotation
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '_');
            rendered.push(if bare {
                annotation
            } else {
                self.mint(&format!("{name}Item{index}"), item)
            });
        }
        tuple_alias_text(name, &rendered, &length_constraints(node))
    }

    fn union_alias(&mut self, name: &str, members: &[Value]) -> String {
        let mut rendered: Vec<String> = Vec::new();
        for (index, member) in members.iter().enumerate() {
            let mut annotation = dto_python_type(member);
            if width(&annotation) + 6 > WIDTH {
                annotation = self.mint(&format!("{name}Member{index}"), member);
            }
            if !rendered.contains(&annotation) {
                rendered.push(annotation);
            }
        }
        let flat = rendered.join(" | ");
        if width(&flat) + 4 <= WIDTH {
            return format!("\n\n{name} = (\n    {flat}\n)\n");
        }
        let mut text = format!("\n\n{name} = (\n");
        for (index, member) in rendered.iter().enumerate() {
            let prefix = if index == 0 { "" } else { "| " };
            let _ = writeln!(text, "    {prefix}{member}");
        }
        text.push_str(")\n");
        text
    }
}

/// `Name = list[Item]` / `Name = dict[str, Value]`; when too long, the formatter
/// breaks inside the brackets (a single `list` item stays on one indented line; a
/// `dict` gets one argument per line and a magic trailing comma).
fn bracket_alias(name: &str, open: &str, inner: &str) -> String {
    let flat = format!("{name} = {open}{inner}]");
    if width(&flat) <= WIDTH {
        return format!("\n\n{flat}\n");
    }
    match open.split_once(", ") {
        None => format!("\n\n{name} = {open}\n    {inner}\n]\n"),
        Some((head, key)) => format!("\n\n{name} = {head}\n    {key},\n    {inner},\n]\n"),
    }
}

/// Line width as the generator's own line-limit test counts it (bytes, which is
/// never less than the formatter's character count).
fn width(text: &str) -> usize {
    text.len()
}

fn is_object(node: &Value) -> bool {
    node.get("type").and_then(Value::as_str) == Some("object") && node.get("properties").is_some()
}

/// The value schema of a `dict[str, V]` map.
fn map_value(node: &Value) -> Option<&Value> {
    let is_map = node.get("type").and_then(Value::as_str) == Some("object");
    let value = node
        .get("additionalProperties")
        .filter(|value| value.is_object())?;
    (is_map && node.get("properties").is_none()).then_some(value)
}

/// A `Field(...)` default the formatter moves to its own line inside the call's
/// parentheses (`name: T = Field(\n        default_factory=list\n    )`), when the
/// head fits and the arguments fit one indented line.
fn split_default_call(identifier: &str, declared: &str, default: &str) -> Option<String> {
    let arguments = default.strip_prefix(" = Field(")?.strip_suffix(')')?;
    let head = format!("    {identifier}: {declared} = Field(");
    (width(&head) <= WIDTH && width(arguments) + 8 <= WIDTH)
        .then(|| format!("{head}\n        {arguments}\n    )\n"))
}

/// A top-level union of bare names: the formatter parenthesizes it as a whole.
fn is_flat_union(annotation: &str) -> bool {
    annotation.contains(" | ") && !annotation.contains('[')
}

/// The member schemas of an unnamed union: `anyOf`/`oneOf`, or a nullable
/// `"type": [T, "null"]` split the way `dto_python_type` splits it.
fn union_members(node: &Value) -> Option<Vec<Value>> {
    if ref_name(node).is_some() {
        return None;
    }
    if let Some(members) = node
        .get("anyOf")
        .or_else(|| node.get("oneOf"))
        .and_then(Value::as_array)
    {
        return Some(members.clone());
    }
    let types = node.get("type").and_then(Value::as_array)?;
    Some(
        types
            .iter()
            .map(|wire_type| {
                if wire_type.as_str() == Some("null") {
                    return serde_json::json!({ "type": "null" });
                }
                let mut member = node.clone();
                member["type"] = wire_type.clone();
                member
            })
            .collect(),
    )
}

/// `Name = [Annotated[]tuple[...][, Field(...)]]`, flat when it fits; otherwise
/// exploded with magic trailing commas (which the formatter keeps), the tuple items
/// one per line when the tuple alone does not fit its indented line.
fn tuple_alias_text(name: &str, items: &[String], constraints: &[String]) -> String {
    let base = format!("tuple[{}]", items.join(", "));
    let annotated = !constraints.is_empty();
    let flat = if annotated {
        format!("Annotated[{base}, Field({})]", constraints.join(", "))
    } else {
        base.clone()
    };
    if width(name) + width(&flat) + 3 <= WIDTH {
        return format!("\n\n{name} = {flat}\n");
    }
    let indent = if annotated { "    " } else { "" };
    let mut text = format!("\n\n{name} = ");
    if annotated {
        text.push_str("Annotated[\n    ");
    }
    if annotated && width(&base) + 5 <= WIDTH {
        text.push_str(&base);
        text.push_str(",\n");
    } else {
        text.push_str("tuple[\n");
        for item in items {
            let _ = writeln!(text, "{indent}    {item},");
        }
        let _ = writeln!(text, "{indent}]{}", if annotated { "," } else { "" });
    }
    if annotated {
        text.push_str("    Field(\n");
        for constraint in constraints {
            let _ = writeln!(text, "        {constraint},");
        }
        text.push_str("    ),\n]\n");
    }
    text
}

/// The item schemas of a fixed-width array (a Rust tuple).
fn tuple_items(node: &Value) -> Option<&Vec<Value>> {
    let is_array = node.get("type").and_then(Value::as_str) == Some("array");
    let items = node.get("prefixItems").and_then(Value::as_array)?;
    (is_array && !items.is_empty()).then_some(items)
}

/// `min_length`/`max_length` constraints of an array, in `dto_python_type`'s order.
fn length_constraints(node: &Value) -> Vec<String> {
    [("minItems", "min_length"), ("maxItems", "max_length")]
        .into_iter()
        .filter_map(|(schema, python)| {
            let bound = node.get(schema).and_then(Value::as_u64)?;
            Some(format!("{python}={bound}"))
        })
        .collect()
}

/// The item schema of an unconstrained array.
fn single_item(node: &Value) -> Option<&Value> {
    let is_array = node.get("type").and_then(Value::as_str) == Some("array");
    let item = node.get("items").filter(|item| item.is_object())?;
    (is_array && node.get("minItems").is_none() && node.get("maxItems").is_none()).then_some(item)
}

fn enum_values(node: &Value) -> Option<Vec<&str>> {
    if let Some(values) = node.get("enum").and_then(Value::as_array) {
        return Some(values.iter().filter_map(Value::as_str).collect());
    }
    string_literal_variants(node)
}

fn has_digest_methods(name: &str) -> bool {
    canonical_digest_spec(name).is_some()
        || SCOPED_PATCH_DIGEST_SPECS
            .iter()
            .any(|spec| spec.model == name)
}

fn class_block(text: String) -> Block {
    Block { alias: false, text }
}

fn alias_block(text: String) -> Block {
    Block { alias: true, text }
}

/// Classes (and tagged unions) in name order, then aliases so that every alias an
/// alias names is already defined.
fn ordered_body(blocks: &BTreeMap<String, Vec<Block>>) -> String {
    let mut body = String::new();
    let mut aliases: BTreeMap<&str, &str> = BTreeMap::new();
    for (name, rendered) in blocks {
        for block in rendered {
            if block.alias {
                aliases.insert(name.as_str(), block.text.as_str());
            } else {
                body.push_str(&block.text);
            }
        }
    }
    let mut emitted = BTreeSet::new();
    for name in aliases.keys() {
        emit_alias(name, &aliases, &mut emitted, &mut body);
    }
    body
}

fn emit_alias<'a>(
    name: &'a str,
    aliases: &BTreeMap<&'a str, &'a str>,
    emitted: &mut BTreeSet<&'a str>,
    body: &mut String,
) {
    if !emitted.insert(name) {
        return;
    }
    let text = aliases[name];
    let right_hand_side = text.split_once(" = ").map_or("", |(_, rhs)| rhs);
    for dependency in identifiers(right_hand_side) {
        if let Some((key, _)) = aliases.get_key_value(dependency) {
            emit_alias(key, aliases, emitted, body);
        }
    }
    body.push_str(text);
}

/// Whether a character of a Python expression sits inside a double-quoted
/// string literal (with backslash escapes).
#[derive(Default)]
struct QuoteState {
    quoted: bool,
    escaped: bool,
}

impl QuoteState {
    /// Advance over `character`; true when it lies outside every literal.
    fn outside(&mut self, character: char) -> bool {
        if self.quoted && character == '\\' && !self.escaped {
            self.escaped = true;
            return false;
        }
        if character == '"' && !self.escaped {
            self.quoted = !self.quoted;
        }
        self.escaped = false;
        !self.quoted
    }
}

/// The identifiers of a Python expression, skipping string literals.
fn identifiers(expression: &str) -> Vec<&str> {
    let mut out = Vec::new();
    let mut start = None;
    let mut quotes = QuoteState::default();
    for (index, character) in expression.char_indices() {
        let part =
            quotes.outside(character) && (character.is_ascii_alphanumeric() || character == '_');
        match (part, start) {
            (true, None) => start = Some(index),
            (false, Some(begin)) => {
                out.push(&expression[begin..index]);
                start = None;
            }
            (true, Some(_)) | (false, None) => {}
        }
    }
    if let Some(begin) = start {
        out.push(&expression[begin..]);
    }
    out
}
