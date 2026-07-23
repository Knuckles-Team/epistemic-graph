//! Schema-FROM-graph (CONCEPT:EG-KG.query.sparql-completeness): derive a GraphQL schema by INTROSPECTING a
//! live `GraphView`, so the GraphQL surface tracks the actual graph with no hand-
//! maintained SDL. The mapping is the natural one a property-graph→GraphQL bridge uses:
//!
//!   * each distinct node `type`/`node_type`/`label` value (the same labels the
//!     eg-core label index keys on) → a GraphQL OBJECT TYPE + a root query field of
//!     that name returning `[Type]`;
//!   * each scalar property key seen on a node of that type → a scalar FIELD;
//!   * each distinct outgoing-edge relationship type from a node of that type → an
//!     OBJECT FIELD whose target is the connected node type (a `[Type]` traversal).
//!
//! The schema is descriptive (used for `to_sdl` introspection + validating a query's
//! root type exists); the RESOLVER (`crate::resolver`) does the actual scan/traversal.

use std::collections::{BTreeMap, BTreeSet};

use eg_core::graph::GraphView;
use serde_json::Value;

/// One GraphQL object type derived from a node label.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ObjectType {
    /// Scalar property fields seen on nodes of this type.
    pub scalar_fields: BTreeSet<String>,
    /// Outgoing relationship types (object fields) from nodes of this type.
    pub edge_fields: BTreeSet<String>,
}

/// A GraphQL schema derived from a graph: a map of type-name → its fields.
#[derive(Clone, Debug, Default)]
pub struct Schema {
    pub types: BTreeMap<String, ObjectType>,
}

impl Schema {
    /// Introspect `view` into a schema. O(nodes + edges) over the off-lock snapshot.
    pub fn from_view(view: &GraphView) -> Result<Self, String> {
        let mut types: BTreeMap<String, ObjectType> = BTreeMap::new();

        // node types + their scalar fields.
        for (id, blob) in &view.node_properties {
            let val =
                decode(blob).map_err(|error| format!("GraphQL schema: node `{id}` {error}"))?;
            let obj = val.as_object().ok_or_else(|| {
                format!("GraphQL schema: node `{id}` properties must be an object")
            })?;
            for label in node_labels(&val)? {
                validate_type_name(&label)?;
                let t = types.entry(label).or_default();
                for (k, _) in obj {
                    if !is_label_key(k) {
                        validate_field_name(k)?;
                        t.scalar_fields.insert(k.clone());
                    }
                }
            }
        }

        // edge relationship types → object fields on the SOURCE node's type(s).
        for ((source, target), blobs) in &view.edge_properties {
            let src_blob = view.node_properties.get(source).ok_or_else(|| {
                format!(
                    "GraphQL schema: edge `{source}` -> `{target}` has no source node properties"
                )
            })?;
            let src_val = decode(src_blob)
                .map_err(|error| format!("GraphQL schema: node `{source}` {error}"))?;
            let src_labels = node_labels(&src_val)?;
            for blob in blobs {
                let ev = decode(blob).map_err(|error| {
                    format!("GraphQL schema: edge `{source}` -> `{target}` {error}")
                })?;
                let edge = ev.as_object().ok_or_else(|| {
                    format!(
                        "GraphQL schema: edge `{source}` -> `{target}` properties must be an object"
                    )
                })?;
                // Relationship identity has one structural field. An edge's ordinary
                // `type` property remains payload and is never reinterpreted here.
                let rel = relationship_name(edge)?.ok_or_else(|| {
                    format!(
                        "GraphQL schema: edge `{source}` -> `{target}` has no relationship name"
                    )
                })?;
                validate_field_name(rel)?;
                for label in &src_labels {
                    let t = types.get_mut(label).ok_or_else(|| {
                        format!("GraphQL schema: source type `{label}` was not registered")
                    })?;
                    if t.scalar_fields.contains(rel) {
                        return Err(format!(
                            "GraphQL schema: type `{label}` defines `{rel}` as both a scalar and an edge field"
                        ));
                    }
                    t.edge_fields.insert(rel.to_string());
                }
            }
        }

        Ok(Schema { types })
    }

    /// Is `name` a root query type (a node label) the resolver can serve?
    pub fn has_type(&self, name: &str) -> bool {
        self.types.contains_key(name)
    }

    /// Render the GraphQL MUTATION SDL (CONCEPT:EG-KG.query.mutation). The write surface is a small
    /// fixed CRUD vocabulary over the property graph, mirroring the query schema's view
    /// of it: `createNode` returns any of the graph's object types (`Node`), the edge
    /// ops return an `Edge` descriptor. Input objects (`props`) are the untyped
    /// `JSON`-shaped maps the property graph stores. The actual `type Query` / per-label
    /// types come from [`Self::to_sdl`]; this renders only the `Mutation` root so the two
    /// can be concatenated into one schema document.
    pub fn to_mutation_sdl(&self) -> String {
        // A `# types: …` comment ties the generic ops back to the graph's real labels,
        // so an introspector sees which node types `createNode(label: …)` can produce.
        let labels = self.types.keys().cloned().collect::<Vec<_>>().join(", ");
        let mut out = String::new();
        if !labels.is_empty() {
            out.push_str(&format!("# node types in this graph: {labels}\n"));
        }
        out.push_str("scalar JSON\n\n");
        out.push_str("type Mutation {\n");
        out.push_str("  createNode(label: String!, props: JSON, id: ID): Node\n");
        out.push_str("  updateNode(id: ID!, props: JSON): Node\n");
        out.push_str("  deleteNode(id: ID!): DeleteResult\n");
        out.push_str("  addEdge(from: ID!, to: ID!, type: String!, props: JSON): Edge\n");
        out.push_str("  removeEdge(from: ID!, to: ID!): Edge\n");
        out.push_str("}\n\n");
        out.push_str("type DeleteResult {\n  id: ID!\n  deleted: Boolean!\n}\n\n");
        out.push_str("type Edge {\n  from: ID!\n  to: ID!\n  type: String\n}\n");
        out
    }

    /// Render a minimal GraphQL SDL for introspection / debugging. Scalar fields are
    /// `String` (the property-graph stores untyped JSON cells); edge fields are
    /// `[<rel>]` lists. The `Query` root exposes one `[Type]` field per type.
    pub fn to_sdl(&self) -> String {
        let mut out = String::new();
        out.push_str("type Query {\n");
        for name in self.types.keys() {
            out.push_str(&format!("  {name}: [{name}]\n"));
        }
        out.push_str("}\n\n");
        for (name, t) in &self.types {
            out.push_str(&format!("type {name} {{\n"));
            for f in &t.scalar_fields {
                out.push_str(&format!("  {f}: String\n"));
            }
            for e in &t.edge_fields {
                // The target type is data-dependent; expose the relationship as a list
                // of the generic node type. (The resolver returns the real targets.)
                out.push_str(&format!("  {e}: [Node]\n"));
            }
            out.push_str("}\n\n");
        }
        out
    }
}

/// Decode a MessagePack property blob to JSON.
pub(crate) fn decode(blob: &[u8]) -> Result<Value, String> {
    eg_types::msgpack::decode_property_value(blob)
        .map_err(|_| "contains malformed MessagePack properties".to_string())
}

/// The label keys a node blob may carry its type under — mirrors `node_has_label` in
/// eg-query/cypher and `GraphCore::build_label_index`.
fn is_label_key(k: &str) -> bool {
    matches!(k, "type" | "node_type" | "label" | "labels")
}

/// All labels a node carries (from `type`/`node_type`/`label` + the `labels` array).
pub(crate) fn node_labels(val: &Value) -> Result<Vec<String>, String> {
    let obj = val
        .as_object()
        .ok_or_else(|| "GraphQL: node properties must be an object".to_string())?;
    let mut out = Vec::new();
    let mut singular: Option<&str> = None;
    for key in ["type", "node_type", "label"] {
        let Some(value) = obj.get(key) else { continue };
        let s = value
            .as_str()
            .filter(|name| !name.is_empty())
            .ok_or_else(|| format!("GraphQL: `{key}` must be a non-empty string"))?;
        if let Some(existing) = singular {
            if existing != s {
                return Err(format!(
                    "GraphQL: singular label fields conflict (`{existing}` versus `{s}`)"
                ));
            }
        } else {
            singular = Some(s);
            out.push(s.to_string());
        }
    }
    if let Some(labels) = obj.get("labels") {
        let arr = labels
            .as_array()
            .ok_or_else(|| "GraphQL: `labels` must be an array of strings".to_string())?;
        let mut array_labels = BTreeSet::new();
        for x in arr {
            let s = x.as_str().filter(|name| !name.is_empty()).ok_or_else(|| {
                "GraphQL: every `labels` entry must be a non-empty string".to_string()
            })?;
            if !array_labels.insert(s) {
                return Err(format!("GraphQL: duplicate `labels` entry `{s}`"));
            }
            if !out.iter().any(|existing| existing == s) {
                out.push(s.to_string());
            }
        }
    }
    Ok(out)
}

pub(crate) fn relationship_name(
    edge: &serde_json::Map<String, Value>,
) -> Result<Option<&str>, String> {
    let relationship = edge.get("relationship");
    if relationship.is_some_and(|candidate| !candidate.is_string()) {
        return Err("GraphQL schema: edge `relationship` must be a non-empty string".to_string());
    }
    let relationship = relationship.and_then(Value::as_str);
    if relationship.is_some_and(str::is_empty) {
        return Err("GraphQL schema: edge relationship name must not be empty".to_string());
    }
    Ok(relationship)
}

fn valid_graphql_name(name: &str) -> bool {
    let mut chars = name.chars();
    chars
        .next()
        .is_some_and(|first| first == '_' || first.is_ascii_alphabetic())
        && chars.all(|ch| ch == '_' || ch.is_ascii_alphanumeric())
        && !name.starts_with("__")
}

pub(crate) fn validate_type_name(name: &str) -> Result<(), String> {
    const GENERATED_TYPES: &[&str] = &["Query", "Mutation", "Node", "Edge", "DeleteResult", "JSON"];
    if !valid_graphql_name(name) || GENERATED_TYPES.contains(&name) {
        return Err(format!(
            "GraphQL schema: `{name}` is not an available object type name"
        ));
    }
    Ok(())
}

pub(crate) fn validate_field_name(name: &str) -> Result<(), String> {
    if !valid_graphql_name(name) {
        return Err(format!(
            "GraphQL schema: `{name}` is not a valid field name"
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use eg_core::graph::GraphCore;
    use serde_json::json;

    fn encoded(value: Value) -> Vec<u8> {
        rmp_serde::to_vec_named(&value).unwrap()
    }

    #[test]
    fn schema_rejects_duplicate_property_keys_and_malformed_blobs() {
        let duplicate = [
            0x83, 0xa4, b't', b'y', b'p', b'e', 0xa6, b'P', b'e', b'r', b's', b'o', b'n', 0xa4,
            b'n', b'a', b'm', b'e', 0xa1, b'A', 0xa4, b'n', b'a', b'm', b'e', 0xa1, b'B',
        ];
        let duplicate_core = GraphCore::new();
        duplicate_core.add_node("duplicate".into(), duplicate.to_vec());
        assert!(Schema::from_view(&duplicate_core.analysis_snapshot()).is_err());

        let malformed_core = GraphCore::new();
        malformed_core.add_node("malformed".into(), vec![0xc1]);
        assert!(Schema::from_view(&malformed_core.analysis_snapshot()).is_err());
    }

    #[test]
    fn schema_rejects_reserved_and_invalid_names() {
        let reserved = GraphCore::new();
        reserved.add_node("reserved".into(), encoded(json!({"type": "Query"})));
        assert!(Schema::from_view(&reserved.analysis_snapshot()).is_err());

        let invalid = GraphCore::new();
        invalid.add_node(
            "invalid".into(),
            encoded(json!({"type": "Person", "bad-name": "value"})),
        );
        assert!(Schema::from_view(&invalid.analysis_snapshot()).is_err());

        let contradictory_labels = GraphCore::new();
        contradictory_labels.add_node(
            "contradictory".into(),
            encoded(json!({"type": "Person", "node_type": "Document"})),
        );
        assert!(Schema::from_view(&contradictory_labels.analysis_snapshot()).is_err());

        let duplicate_labels = GraphCore::new();
        duplicate_labels.add_node(
            "duplicate-label".into(),
            encoded(json!({"labels": ["Person", "Person"]})),
        );
        assert!(Schema::from_view(&duplicate_labels.analysis_snapshot()).is_err());
    }

    #[test]
    fn schema_rejects_field_kind_and_relationship_name_collisions() {
        let collision = GraphCore::new();
        collision.add_node(
            "source".into(),
            encoded(json!({"type": "Person", "KNOWS": "scalar"})),
        );
        collision.add_node("target".into(), encoded(json!({"type": "Person"})));
        collision
            .add_edge(
                "source".into(),
                "target".into(),
                encoded(json!({"relationship": "KNOWS"})),
            )
            .unwrap();
        assert!(Schema::from_view(&collision.analysis_snapshot()).is_err());

        let payload_type_only = GraphCore::new();
        payload_type_only.add_node("source".into(), encoded(json!({"type": "Person"})));
        payload_type_only.add_node("target".into(), encoded(json!({"type": "Person"})));
        payload_type_only
            .add_edge(
                "source".into(),
                "target".into(),
                encoded(json!({"type": "transport-metadata"})),
            )
            .unwrap();
        assert!(Schema::from_view(&payload_type_only.analysis_snapshot()).is_err());

        let untyped = GraphCore::new();
        untyped.add_node("source".into(), encoded(json!({"type": "Person"})));
        untyped.add_node("target".into(), encoded(json!({"type": "Person"})));
        untyped
            .add_edge("source".into(), "target".into(), encoded(json!({})))
            .unwrap();
        assert!(Schema::from_view(&untyped.analysis_snapshot()).is_err());
    }
}
