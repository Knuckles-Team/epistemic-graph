//! The `nodes` table provider (CONCEPT:EG-KG.query.read-only-sql-query). Schema-on-read: scan every
//! node's property MessagePack blob once, decode to `serde_json::Value` (the same
//! path `get_nodes_by_label` uses), infer an Arrow schema as the union of observed
//! keys, and materialize a single RecordBatch wrapped in a DataFusion `MemTable`.
//!
//! Type inference per key (over all nodes that carry it):
//!   bool                    -> Boolean
//!   integer                 -> Int64
//!   float (or int+float mix)-> Float64
//!   anything else / nested / heterogeneous -> Utf8 (JSON-stringified)
//!   missing on a row        -> null
//! An `id: Utf8` column (the node id) and a raw `props: Binary` escape-hatch column
//! (the original msgpack blob, for the `json_get*` UDFs) are ALWAYS emitted.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::array::{
    ArrayRef, BinaryBuilder, BooleanBuilder, Float64Builder, Int64Builder, StringBuilder,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use eg_core::graph::GraphView;
use serde_json::Value;

/// Widening lattice for an inferred column type. `Null` means "seen only null /
/// not yet seen"; anything wider wins on conflict, collapsing to `Utf8` for
/// heterogeneous or nested values.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Inferred {
    Null,
    Bool,
    Int,
    Float,
    Str,
}

impl Inferred {
    fn widen(self, other: Inferred) -> Inferred {
        use Inferred::*;
        match (self, other) {
            (Null, x) | (x, Null) => x,
            (a, b) if a == b => a,
            // int + float collapse to float; everything else heterogeneous -> str.
            (Int, Float) | (Float, Int) => Float,
            _ => Str,
        }
    }

    fn from_value(v: &Value) -> Inferred {
        match v {
            Value::Null => Inferred::Null,
            Value::Bool(_) => Inferred::Bool,
            Value::Number(n) if n.is_i64() || n.is_u64() => Inferred::Int,
            Value::Number(_) => Inferred::Float,
            Value::String(_) => Inferred::Str,
            // arrays / objects -> JSON-stringified Utf8.
            _ => Inferred::Str,
        }
    }

    fn arrow_type(self) -> DataType {
        match self {
            // A column that was only ever null still needs a concrete type for
            // Arrow; Utf8 (all-null) is the least surprising.
            Inferred::Null | Inferred::Str => DataType::Utf8,
            Inferred::Bool => DataType::Boolean,
            Inferred::Int => DataType::Int64,
            Inferred::Float => DataType::Float64,
        }
    }
}

/// The fixed column names `infer_nodes` always emits (`id` at the front, `props` at
/// the back) — reserved so a same-named node PROPERTY never produces a second Arrow
/// `Field` with the same name (DataFusion rejects a schema with a duplicate
/// qualified field name, e.g. `nodes.id`, on every query over the table, not just
/// ones that reference the column).
fn is_reserved_column(name: &str) -> bool {
    name == "id" || name == "props"
}

/// A decoded node: its id plus the raw blob and the decoded JSON object (or `None`
/// if the blob didn't decode to an object — it still appears as an id+props row).
struct DecodedNode<'a> {
    id: &'a str,
    raw: &'a [u8],
    obj: Option<serde_json::Map<String, Value>>,
}

/// Inferred (schema, single batch) for the `nodes` table over `view` — the
/// schema-on-read scan. Split out from MemTable construction so the result can be
/// cached and re-wrapped per query (CONCEPT:EG-KG.query.version-keyed-cache version-keyed cache).
pub(crate) fn infer_nodes(view: &GraphView) -> Result<(SchemaRef, RecordBatch), String> {
    // Pass 1: decode blobs and infer the per-key type union.
    let mut decoded: Vec<DecodedNode> = Vec::with_capacity(view.node_properties.len());
    // BTreeMap keeps a stable, deterministic column order.
    let mut inferred: BTreeMap<String, Inferred> = BTreeMap::new();

    for (id, blob) in view.node_properties.iter() {
        let obj = eg_types::msgpack::decode_property_object(blob.as_slice()).ok();
        if let Some(ref m) = obj {
            for (k, v) in m.iter() {
                let kind = Inferred::from_value(v);
                inferred
                    .entry(k.clone())
                    .and_modify(|cur| *cur = cur.widen(kind))
                    .or_insert(kind);
            }
        }
        decoded.push(DecodedNode {
            id,
            raw: blob.as_slice(),
            obj,
        });
    }

    // Schema: id (Utf8, non-null), inferred columns (all nullable), props (Binary).
    // `id` and `props` are RESERVED column names owned by the fixed columns above/
    // below this loop — if a node's own JSON properties happen to carry a key
    // literally named "id" or "props" (common: many ingested nodes stash their own
    // id as a property), skip it here rather than emitting a second `Field` with the
    // same name. DataFusion's schema validation rejects a duplicate qualified field
    // name (`nodes.id`) on ANY query over the table, even `SELECT COUNT(*)` — so an
    // unfiltered duplicate silently broke every NL/SQL query once a single node
    // anywhere in the graph carried an `id`/`props` property. The reserved fixed
    // column always wins; the duplicate property value is still reachable via the
    // `props` escape-hatch blob.
    let mut fields: Vec<Field> = Vec::with_capacity(inferred.len() + 2);
    fields.push(Field::new("id", DataType::Utf8, false));
    for (name, kind) in inferred.iter() {
        if is_reserved_column(name) {
            continue;
        }
        fields.push(Field::new(name, kind.arrow_type(), true));
    }
    fields.push(Field::new("props", DataType::Binary, false));
    let schema: SchemaRef = Arc::new(Schema::new(fields));

    let batch = build_batch(&schema, &inferred, &decoded)?;
    Ok((schema, batch))
}

/// Build one inferred property column. Looking the property up on every decoded node is
/// the same for each inferred kind; only how the JSON value is appended differs.
fn build_property_column<B: arrow::array::builder::ArrayBuilder>(
    decoded: &[DecodedNode],
    name: &str,
    mut values: B,
    mut append: impl FnMut(&mut B, Option<&Value>),
) -> ArrayRef {
    for n in decoded {
        append(&mut values, n.obj.as_ref().and_then(|o| o.get(name)));
    }
    arrow::array::builder::ArrayBuilder::finish(&mut values)
}

/// Materialize one RecordBatch column-by-column following `inferred`.
fn build_batch(
    schema: &SchemaRef,
    inferred: &BTreeMap<String, Inferred>,
    decoded: &[DecodedNode],
) -> Result<RecordBatch, String> {
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());

    // id column.
    let mut id_b = StringBuilder::new();
    for n in decoded {
        id_b.append_value(n.id);
    }
    columns.push(Arc::new(id_b.finish()));

    // inferred property columns. Mirrors the field-list skip above: a property
    // literally named `id`/`props` does not get its own column (the reserved fixed
    // column already occupies that name), keeping the batch's column count aligned
    // 1:1 with the schema built above.
    for (name, kind) in inferred.iter() {
        if is_reserved_column(name) {
            continue;
        }
        let col: ArrayRef = match kind {
            Inferred::Bool => {
                build_property_column(decoded, name, BooleanBuilder::new(), |b, v| {
                    b.append_option(match v {
                        Some(Value::Bool(x)) => Some(*x),
                        _ => None,
                    })
                })
            }
            Inferred::Int => build_property_column(decoded, name, Int64Builder::new(), |b, v| {
                b.append_option(v.and_then(Value::as_i64))
            }),
            Inferred::Float => {
                build_property_column(decoded, name, Float64Builder::new(), |b, v| {
                    b.append_option(v.and_then(Value::as_f64))
                })
            }
            // Str / Null columns: JSON-stringify non-string scalars, pass strings
            // through, null for missing/json-null.
            Inferred::Str | Inferred::Null => {
                build_property_column(decoded, name, StringBuilder::new(), |b, v| match v {
                    None | Some(Value::Null) => b.append_null(),
                    Some(Value::String(s)) => b.append_value(s),
                    Some(other) => b.append_value(other.to_string()),
                })
            }
        };
        columns.push(col);
    }

    // raw props escape-hatch column.
    let mut props_b = BinaryBuilder::new();
    for n in decoded {
        props_b.append_value(n.raw);
    }
    columns.push(Arc::new(props_b.finish()));

    RecordBatch::try_new(schema.clone(), columns).map_err(|e| format!("record batch: {e}"))
}
