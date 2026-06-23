//! The `nodes` table provider (CONCEPT:KG-2.178). Schema-on-read: scan every
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
use datafusion::datasource::MemTable;
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

/// A decoded node: its id plus the raw blob and the decoded JSON object (or `None`
/// if the blob didn't decode to an object — it still appears as an id+props row).
struct DecodedNode<'a> {
    id: &'a str,
    raw: &'a [u8],
    obj: Option<serde_json::Map<String, Value>>,
}

/// Build the in-memory `nodes` MemTable for `view`. Returns the table plus its
/// schema (the caller registers it on the SessionContext).
pub(crate) fn build_nodes_table(view: &GraphView) -> Result<MemTable, String> {
    // Pass 1: decode blobs and infer the per-key type union.
    let mut decoded: Vec<DecodedNode> = Vec::with_capacity(view.node_properties.len());
    // BTreeMap keeps a stable, deterministic column order.
    let mut inferred: BTreeMap<String, Inferred> = BTreeMap::new();

    for (id, blob) in view.node_properties.iter() {
        let obj = rmp_serde::from_slice::<Value>(blob.as_slice())
            .ok()
            .and_then(|v| match v {
                Value::Object(m) => Some(m),
                _ => None,
            });
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
    let mut fields: Vec<Field> = Vec::with_capacity(inferred.len() + 2);
    fields.push(Field::new("id", DataType::Utf8, false));
    for (name, kind) in inferred.iter() {
        fields.push(Field::new(name, kind.arrow_type(), true));
    }
    fields.push(Field::new("props", DataType::Binary, false));
    let schema: SchemaRef = Arc::new(Schema::new(fields));

    let batch = build_batch(&schema, &inferred, &decoded)?;
    MemTable::try_new(schema, vec![vec![batch]]).map_err(|e| format!("mem table: {e}"))
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

    // inferred property columns.
    for (name, kind) in inferred.iter() {
        let col: ArrayRef = match kind {
            Inferred::Bool => {
                let mut b = BooleanBuilder::new();
                for n in decoded {
                    match n.obj.as_ref().and_then(|o| o.get(name)) {
                        Some(Value::Bool(x)) => b.append_value(*x),
                        _ => b.append_null(),
                    }
                }
                Arc::new(b.finish())
            }
            Inferred::Int => {
                let mut b = Int64Builder::new();
                for n in decoded {
                    match n
                        .obj
                        .as_ref()
                        .and_then(|o| o.get(name))
                        .and_then(Value::as_i64)
                    {
                        Some(x) => b.append_value(x),
                        None => b.append_null(),
                    }
                }
                Arc::new(b.finish())
            }
            Inferred::Float => {
                let mut b = Float64Builder::new();
                for n in decoded {
                    match n
                        .obj
                        .as_ref()
                        .and_then(|o| o.get(name))
                        .and_then(Value::as_f64)
                    {
                        Some(x) => b.append_value(x),
                        None => b.append_null(),
                    }
                }
                Arc::new(b.finish())
            }
            // Str / Null columns: JSON-stringify non-string scalars, pass strings
            // through, null for missing/json-null.
            Inferred::Str | Inferred::Null => {
                let mut b = StringBuilder::new();
                for n in decoded {
                    match n.obj.as_ref().and_then(|o| o.get(name)) {
                        None | Some(Value::Null) => b.append_null(),
                        Some(Value::String(s)) => b.append_value(s),
                        Some(other) => b.append_value(other.to_string()),
                    }
                }
                Arc::new(b.finish())
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
