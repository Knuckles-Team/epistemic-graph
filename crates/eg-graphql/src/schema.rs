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
    pub fn from_view(view: &GraphView) -> Self {
        let mut types: BTreeMap<String, ObjectType> = BTreeMap::new();

        // node types + their scalar fields.
        for (id, blob) in &view.node_properties {
            let Some(val) = decode(blob) else { continue };
            let Some(obj) = val.as_object() else { continue };
            for label in node_labels(&val) {
                let t = types.entry(label).or_default();
                for (k, _) in obj {
                    if !is_label_key(k) {
                        t.scalar_fields.insert(k.clone());
                    }
                }
            }
            let _ = id;
        }

        // edge relationship types → object fields on the SOURCE node's type(s).
        for ((s, _o), blobs) in &view.edge_properties {
            let Some(src_val) = view.node_properties.get(s).and_then(|b| decode(b)) else {
                continue;
            };
            let src_labels = node_labels(&src_val);
            for blob in blobs {
                let Some(ev) = decode(blob) else { continue };
                // The relationship type lives under `type` or `relationship` (the same
                // two keys eg-query/cypher's `rel_matches` reads).
                let Some(rel) = ev
                    .get("relationship")
                    .or_else(|| ev.get("type"))
                    .and_then(|x| x.as_str())
                else {
                    continue;
                };
                for label in &src_labels {
                    if let Some(t) = types.get_mut(label) {
                        t.edge_fields.insert(rel.to_string());
                    }
                }
            }
        }

        Schema { types }
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
pub(crate) fn decode(blob: &[u8]) -> Option<Value> {
    rmp_serde::from_slice(blob).ok()
}

/// The label keys a node blob may carry its type under — mirrors `node_has_label` in
/// eg-query/cypher and `GraphCore::build_label_index`.
fn is_label_key(k: &str) -> bool {
    matches!(k, "type" | "node_type" | "label" | "labels")
}

/// All labels a node carries (from `type`/`node_type`/`label` + the `labels` array).
pub(crate) fn node_labels(val: &Value) -> Vec<String> {
    let mut out = Vec::new();
    for key in ["type", "node_type", "label"] {
        if let Some(s) = val.get(key).and_then(|v| v.as_str()) {
            if !out.contains(&s.to_string()) {
                out.push(s.to_string());
            }
        }
    }
    if let Some(arr) = val.get("labels").and_then(|v| v.as_array()) {
        for x in arr {
            if let Some(s) = x.as_str() {
                if !out.contains(&s.to_string()) {
                    out.push(s.to_string());
                }
            }
        }
    }
    out
}
