//! Schema-minimal request samples, synthesized from the published request
//! schema itself so a new method is covered without a hand-written sample.
//!
//! The rule is "what a pydantic sender that omits defaults would send":
//! required properties get a value, nullable optional properties an explicit
//! null (a `deserialize_required_option` field must be present), every other
//! optional property is omitted so serde materializes its default.

use serde_json::{json, Map, Value};

/// Deeper than any schema path a required-only sample follows.
const MAX_DEPTH: usize = 48;
const SEMANTIC_DIGEST_PATTERN: &str = "^sha256:[0-9a-f]{64}$";
const HEX_DIGEST_PATTERN: &str = "^[0-9a-f]{64}$";
/// Lowercase hex digits: a valid digest, nonce or opaque text alike.
const SAMPLE_TEXT: &str = "0000000000000000000000000000000000000000000000000000000000000000";

pub(super) struct Synth<'a> {
    definitions: &'a Map<String, Value>,
}

impl<'a> Synth<'a> {
    pub(super) fn new(document: &'a Value) -> Self {
        Self {
            definitions: document
                .get("$defs")
                .and_then(Value::as_object)
                .expect("the method request document carries $defs"),
        }
    }

    /// `{"method": id, "params": <sample>}`, or no params for a unit method.
    pub(super) fn request(&self, id: &str, subschema: &'a Value) -> Value {
        let mut request = Map::new();
        request.insert("method".to_string(), Value::from(id));
        if let Some(params) = subschema.pointer("/properties/params") {
            request.insert("params".to_string(), self.sample(params, 0));
        }
        Value::Object(request)
    }

    fn resolve(&self, mut node: &'a Value) -> &'a Value {
        while let Some(name) = reference_name(node) {
            node = self
                .definitions
                .get(name)
                .unwrap_or_else(|| panic!("unresolved schema reference {name}"));
        }
        node
    }

    fn sample(&self, node: &'a Value, depth: usize) -> Value {
        assert!(
            depth < MAX_DEPTH,
            "request schema recursion exceeds {MAX_DEPTH}"
        );
        let node = self.resolve(node);
        literal(node)
            .or_else(|| self.alternative(node, depth))
            .unwrap_or_else(|| self.typed(node, depth))
    }

    /// The first non-null branch of a `oneOf`/`anyOf`/`allOf`.
    fn alternative(&self, node: &'a Value, depth: usize) -> Option<Value> {
        let branches = ["oneOf", "anyOf", "allOf"]
            .iter()
            .find_map(|key| node.get(*key).and_then(Value::as_array))?;
        let chosen = branches
            .iter()
            .find(|branch| !self.is_null(branch))
            .or_else(|| branches.first())?;
        Some(self.sample(chosen, depth + 1))
    }

    fn typed(&self, node: &'a Value, depth: usize) -> Value {
        match primary_type(node) {
            "object" => self.object(node, depth),
            "array" => self.array(node, depth),
            "string" => string_sample(node),
            "integer" => integer_sample(node),
            "number" => json!(0.5),
            "boolean" => Value::Bool(false),
            _ => Value::Null,
        }
    }

    fn object(&self, node: &'a Value, depth: usize) -> Value {
        let required: Vec<&str> = node
            .get("required")
            .and_then(Value::as_array)
            .map(|names| names.iter().filter_map(Value::as_str).collect())
            .unwrap_or_default();
        let mut sample = Map::new();
        let properties = node.get("properties").and_then(Value::as_object);
        for (name, property) in properties.into_iter().flatten() {
            if required.contains(&name.as_str()) {
                sample.insert(name.clone(), self.sample(property, depth + 1));
            } else if self.nullable(property) {
                sample.insert(name.clone(), Value::Null);
            }
        }
        Value::Object(sample)
    }

    fn array(&self, node: &'a Value, depth: usize) -> Value {
        if let Some(items) = node.get("prefixItems").and_then(Value::as_array) {
            return items
                .iter()
                .map(|item| self.sample(item, depth + 1))
                .collect();
        }
        let count = node.get("minItems").and_then(Value::as_u64).unwrap_or(0);
        let items = node.get("items").unwrap_or(&Value::Null);
        (0..count).map(|_| self.sample(items, depth + 1)).collect()
    }

    fn is_null(&self, node: &'a Value) -> bool {
        self.resolve(node).get("type").and_then(Value::as_str) == Some("null")
    }

    /// Whether the property admits an explicit null.
    fn nullable(&self, node: &'a Value) -> bool {
        let node = self.resolve(node);
        let typed_null = node
            .get("type")
            .and_then(Value::as_array)
            .is_some_and(|types| types.iter().any(|kind| kind == "null"));
        let branch_null = ["oneOf", "anyOf"]
            .iter()
            .filter_map(|key| node.get(*key).and_then(Value::as_array))
            .flatten()
            .any(|branch| self.is_null(branch));
        typed_null || branch_null || unconstrained(node)
    }
}

/// A schema that constrains nothing (`Option<serde_json::Value>` renders as
/// one) admits an explicit null like any other value.
fn unconstrained(node: &Value) -> bool {
    const CONSTRAINTS: [&str; 8] = [
        "type",
        "$ref",
        "oneOf",
        "anyOf",
        "allOf",
        "const",
        "enum",
        "properties",
    ];
    CONSTRAINTS.iter().all(|key| node.get(*key).is_none())
}

fn reference_name(node: &Value) -> Option<&str> {
    let reference = node.get("$ref")?.as_str()?;
    Some(reference.rsplit('/').next().unwrap_or(reference))
}

/// A `const` or the first `enum` value.
fn literal(node: &Value) -> Option<Value> {
    node.get("const").cloned().or_else(|| {
        node.get("enum")
            .and_then(Value::as_array)
            .and_then(|values| values.first())
            .cloned()
    })
}

/// The first non-null declared type; a bare `properties` node is an object.
fn primary_type(node: &Value) -> &str {
    match node.get("type") {
        Some(Value::String(kind)) => kind.as_str(),
        Some(Value::Array(kinds)) => kinds
            .iter()
            .filter_map(Value::as_str)
            .find(|kind| *kind != "null")
            .unwrap_or("null"),
        _ if node.get("properties").is_some() => "object",
        _ => "",
    }
}

fn string_sample(node: &Value) -> Value {
    match node.get("pattern").and_then(Value::as_str) {
        Some(SEMANTIC_DIGEST_PATTERN) => Value::from(format!("sha256:{SAMPLE_TEXT}")),
        None | Some(HEX_DIGEST_PATTERN) => {
            let limit = node
                .get("maxLength")
                .and_then(Value::as_u64)
                .map_or(SAMPLE_TEXT.len(), |max| {
                    usize::try_from(max).unwrap_or(usize::MAX)
                });
            Value::from(&SAMPLE_TEXT[..limit.clamp(1, SAMPLE_TEXT.len())])
        }
        Some(other) => panic!("no sample rule for string pattern {other}"),
    }
}

fn integer_sample(node: &Value) -> Value {
    let minimum = node
        .get("minimum")
        .and_then(Value::as_i64)
        .unwrap_or(1)
        .max(1);
    let maximum = node
        .get("maximum")
        .and_then(Value::as_i64)
        .unwrap_or(i64::MAX);
    Value::from(minimum.min(maximum))
}
