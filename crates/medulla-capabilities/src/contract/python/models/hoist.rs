//! Name every inline object and variant so each becomes a Python model (EH-192).
//!
//! JSON Schema lets a definition carry anonymous structure: an inline object as a
//! field, an externally tagged enum variant (`{"Scan": {...}}`), a tagged variant's
//! nested struct. A Python model needs a class for each. This pass rewrites every such
//! node into a `$ref` to a new definition named after where it sits
//! (`{Owner}{Field}`), until no anonymous structure is left. It is deterministic: the
//! definitions are processed in name order and new ones are appended in the order
//! they are found.

use std::collections::VecDeque;

use serde_json::{json, Map, Value};

use super::super::dto::{ref_name, string_literal_variants, tagged_variants, variant_tag};
use super::super::names::pascal;

/// The union keyword a node uses, if any.
fn union_key(node: &Value) -> Option<&'static str> {
    ["anyOf", "oneOf"]
        .into_iter()
        .find(|key| node.get(*key).and_then(Value::as_array).is_some())
}

fn is_inline_object(node: &Value) -> bool {
    node.get("properties").is_some_and(Value::is_object) && ref_name(node).is_none()
}

/// The single key of an externally tagged variant (`{"Key": body}`), if `node` is one.
fn external_variant_key(node: &Value) -> Option<&str> {
    if !is_inline_object(node) {
        return None;
    }
    let properties = node.get("properties")?.as_object()?;
    let required = node.get("required")?.as_array()?;
    let (key, _) = properties.iter().next()?;
    let single = properties.len() == 1 && required.len() == 1;
    (single && required[0].as_str() == Some(key)).then_some(key.as_str())
}

fn is_external_union(node: &Value) -> bool {
    let Some(key) = union_key(node) else {
        return false;
    };
    tagged_variants(node).is_none()
        && node[key]
            .as_array()
            .is_some_and(|variants| variants.iter().any(|v| external_variant_key(v).is_some()))
}

/// A fixed-width array (a Rust tuple). Tuples are always named: `dto_python_type`
/// has no tuple form, and a named alias is where the renderer can lay one out.
pub(super) fn is_tuple(node: &Value) -> bool {
    node.get("type").and_then(Value::as_str) == Some("array")
        && node
            .get("prefixItems")
            .and_then(Value::as_array)
            .is_some_and(|items| !items.is_empty())
}

/// `"type": ["array", "null"]` over a tuple: an optional tuple.
fn is_nullable_tuple(node: &Value) -> bool {
    let types = node.get("type").and_then(Value::as_array);
    let has = |wire: &str| types.is_some_and(|types| types.iter().any(|t| t == wire));
    has("array")
        && has("null")
        && types.is_some_and(|t| t.len() == 2)
        && node.get("prefixItems").is_some()
}

/// A node that must become its own definition when it appears nested.
fn needs_definition(node: &Value) -> bool {
    is_tuple(node)
        || is_inline_object(node)
        || (union_key(node).is_some()
            && (tagged_variants(node).is_some() || is_external_union(node)))
}

/// The worklist state of one hoisting run.
pub(super) struct Hoister {
    pub(super) definitions: Map<String, Value>,
    work: VecDeque<String>,
}

impl Hoister {
    pub(super) fn new(definitions: Map<String, Value>) -> Self {
        let work = definitions.keys().cloned().collect();
        Self { definitions, work }
    }

    /// Hoist every queued definition, including the ones hoisting creates.
    pub(super) fn run(mut self) -> Map<String, Value> {
        while let Some(name) = self.work.pop_front() {
            self.hoist_definition(&name);
        }
        self.definitions
    }

    /// Register `node` as a new definition named from `base` and return its `$ref`.
    pub(super) fn register(&mut self, base: &str, node: Value) -> Value {
        let name = fresh_name(base, |candidate| self.definitions.contains_key(candidate));
        self.definitions.insert(name.clone(), node);
        self.work.push_back(name.clone());
        json!({ "$ref": format!("#/$defs/{name}") })
    }

    fn hoist_definition(&mut self, name: &str) {
        let Some(node) = self.definitions.get(name).cloned() else {
            return;
        };
        let rewritten = if let Some((tag, _)) = tagged_variants(&node) {
            let tag = tag.to_string();
            self.hoist_tagged(name, &node, &tag)
        } else if is_external_union(&node) {
            self.hoist_external(name, &node)
        } else if is_inline_object(&node) {
            self.hoist_fields(&node, name)
        } else if is_tuple(&node) {
            let items = self.hoist_list(&node["prefixItems"], name, "Item");
            with_key(&node, "prefixItems", items)
        } else if string_literal_variants(&node).is_some() {
            node
        } else {
            self.hoist_nested(&node, &format!("{name}Value"))
        };
        self.definitions.insert(name.to_string(), rewritten);
    }

    /// An internally tagged union: each variant stays inline (the renderer names it
    /// `{Name}{Tag}`), but its fields are hoisted under that name.
    fn hoist_tagged(&mut self, name: &str, node: &Value, tag: &str) -> Value {
        let key = union_key(node).expect("a tagged union has variants");
        let variants: Vec<Value> = node[key]
            .as_array()
            .expect("variant list")
            .iter()
            .map(|variant| {
                let owner = format!("{name}{}", pascal(variant_tag(variant, tag).unwrap_or("")));
                if is_inline_object(variant) {
                    self.hoist_fields(variant, &owner)
                } else {
                    variant.clone()
                }
            })
            .collect();
        with_key(node, key, Value::Array(variants))
    }

    /// An externally tagged union: every `{"Key": body}` variant becomes a named
    /// single-field model `{Name}{Key}`; unit variants stay string literals.
    fn hoist_external(&mut self, name: &str, node: &Value) -> Value {
        let key = union_key(node).expect("an external union has variants");
        let variants: Vec<Value> = node[key]
            .as_array()
            .expect("variant list")
            .iter()
            .map(|variant| self.hoist_external_variant(name, variant))
            .collect();
        with_key(node, key, Value::Array(variants))
    }

    fn hoist_external_variant(&mut self, name: &str, variant: &Value) -> Value {
        let Some(key) = external_variant_key(variant).map(str::to_string) else {
            return self.hoist_nested(variant, &format!("{name}Value"));
        };
        let owner = format!("{name}{}", pascal(&key));
        let body = self.hoist_nested(&variant["properties"][&key], &format!("{owner}Body"));
        let wrapped = json!({
            "type": "object",
            "additionalProperties": false,
            "required": [key.clone()],
            "properties": { key: body },
        });
        self.register(&owner, wrapped)
    }

    /// An object's fields, each hoisted under `{Owner}{Field}`.
    pub(super) fn hoist_fields(&mut self, object: &Value, owner: &str) -> Value {
        let properties = object["properties"]
            .as_object()
            .cloned()
            .unwrap_or_default();
        let hoisted: Map<String, Value> = properties
            .into_iter()
            .map(|(field, node)| {
                let base = format!("{owner}{}", pascal(&field));
                let value = self.hoist_nested(&node, &base);
                (field, value)
            })
            .collect();
        with_key(object, "properties", Value::Object(hoisted))
    }

    /// Replace every anonymous structure inside `node` with a named definition.
    fn hoist_nested(&mut self, node: &Value, owner: &str) -> Value {
        if !node.is_object() || ref_name(node).is_some() {
            return node.clone();
        }
        if needs_definition(node) {
            return self.register(owner, node.clone());
        }
        if is_nullable_tuple(node) {
            let mut tuple = node.clone();
            tuple["type"] = json!("array");
            let named = self.register(owner, tuple);
            return json!({ "anyOf": [named, { "type": "null" }] });
        }
        let mut out = node.clone();
        if let Some(key) = union_key(node) {
            let variants = self.hoist_list(&node[key], owner, "");
            out[key] = variants;
        }
        if node.get("items").is_some_and(Value::is_object) {
            out["items"] = self.hoist_nested(&node["items"], &format!("{owner}Item"));
        }
        if node
            .get("additionalProperties")
            .is_some_and(Value::is_object)
        {
            out["additionalProperties"] =
                self.hoist_nested(&node["additionalProperties"], &format!("{owner}Value"));
        }
        if node.get("prefixItems").is_some() {
            out["prefixItems"] = self.hoist_list(&node["prefixItems"], owner, "Item");
        }
        out
    }

    /// Hoist a list of member schemas; member `i` (after the first) is named
    /// `{owner}{infix}{i}`.
    fn hoist_list(&mut self, list: &Value, owner: &str, infix: &str) -> Value {
        let members = list.as_array().cloned().unwrap_or_default();
        Value::Array(
            members
                .iter()
                .enumerate()
                .map(|(index, member)| {
                    let base = match (index, infix) {
                        (0, "") => owner.to_string(),
                        _ => format!("{owner}{infix}{index}"),
                    };
                    self.hoist_nested(member, &base)
                })
                .collect(),
        )
    }
}

/// `base`, or `base` with the first free `Inline`/`InlineN` suffix.
pub(super) fn fresh_name(base: &str, taken: impl Fn(&str) -> bool) -> String {
    if !taken(base) {
        return base.to_string();
    }
    (1..)
        .map(|index| match index {
            1 => format!("{base}Inline"),
            _ => format!("{base}Inline{index}"),
        })
        .find(|candidate| !taken(candidate))
        .expect("an unbounded suffix sequence has a free name")
}

fn with_key(node: &Value, key: &str, value: Value) -> Value {
    let mut out = node.clone();
    out[key] = value;
    out
}
