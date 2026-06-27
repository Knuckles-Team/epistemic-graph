//! GraphQL **mutation** execution (CONCEPT:EG-019): map a parsed `mutation { … }`
//! operation onto eg-core's native write ops over the SAME live `GraphCore` the read
//! resolver snapshots — NO async-graphql, NO second graph. The five CRUD root fields
//! mirror the engine's primitives one-for-one:
//!
//!   * `createNode(label, props)` → `GraphCore::add_node` (the `props` object becomes
//!     the node blob, stamped with `type = label` so the read surface sees the label);
//!   * `updateNode(id, props)`    → read-merge-write the node blob via `add_node`
//!     (overwrites properties under the topology guard);
//!   * `deleteNode(id)`           → `GraphCore::remove_node`;
//!   * `addEdge(from, to, type, props)` → `GraphCore::add_edge` (`type` stored under
//!     `relationship`, the same key the read resolver's `rel_matches` reads);
//!   * `removeEdge(from, to)`     → `GraphCore::remove_edge`.
//!
//! Each field's selection set shapes the returned object using the SAME
//! `resolver::resolve_selection` the query path uses, so a `createNode { id name }`
//! returns exactly what an equivalent `query` would for that node. The whole batch
//! bumps the engine's OCC version (`mark_dirty`) once it lands.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use eg_core::graph::GraphCore;
use serde_json::{Map, Value};

use crate::parser::{gql_to_json, parse_operation, Field, GqlValue, Operation};
use crate::resolver::resolve_selection;
use crate::schema::decode;

/// Per-process counter that, combined with a nanosecond timestamp, makes a generated
/// node id unique even for many `createNode`s within the same instant.
static ID_SEQ: AtomicU64 = AtomicU64::new(0);

/// Parse + execute a GraphQL mutation string over `core`, returning the GraphQL-shaped
/// `{"data": { … }}` JSON. A parse error, a non-mutation operation, or an invalid write
/// argument is an `Err`. Writes go through the engine's native single-op write methods.
pub fn execute(core: &GraphCore, src: &str) -> Result<Value, String> {
    let op = parse_operation(src).map_err(|e| e.to_string())?;
    let m = match op {
        Operation::Mutation(m) => m,
        Operation::Query(_) => {
            return Err("GraphQL: expected a mutation, got a query (use the query path)".into())
        }
        Operation::Subscription(_) => {
            return Err("GraphQL: expected a mutation, got a subscription".into())
        }
    };

    let mut data = Map::new();
    for field in &m.roots {
        let result = execute_field(core, field)?;
        data.insert(field.alias.clone(), result);
    }
    // One OCC version bump + lazy-index invalidation after the batch lands.
    core.mark_dirty();

    Ok(Value::Object(
        [("data".to_string(), Value::Object(data))]
            .into_iter()
            .collect(),
    ))
}

/// Dispatch one mutation root field to its native write op.
fn execute_field(core: &GraphCore, field: &Field) -> Result<Value, String> {
    match field.name.as_str() {
        "createNode" => create_node(core, field),
        "updateNode" => update_node(core, field),
        "deleteNode" => delete_node(core, field),
        "addEdge" => add_edge(core, field),
        "removeEdge" => remove_edge(core, field),
        other => Err(format!(
            "GraphQL: unknown mutation `{other}` (expected one of \
             createNode/updateNode/deleteNode/addEdge/removeEdge)"
        )),
    }
}

/// `createNode(label, props)` → `add_node`. The `props` object is the node blob,
/// stamped with `type = label`. The node id is an explicit `id` arg, else `props.id`,
/// else a generated `<label>:<ts>-<seq>`.
fn create_node(core: &GraphCore, field: &Field) -> Result<Value, String> {
    let label = arg_str(field, "label")?;
    let mut obj = props_map(field)?;

    let id = match arg(field, "id") {
        Some(GqlValue::Str(s)) => s.clone(),
        Some(_) => return Err("`id` must be a string".into()),
        None => match obj.get("id").and_then(|v| v.as_str()) {
            Some(s) => s.to_string(),
            None => generate_id(&label),
        },
    };

    // Stamp the label so the read surface's label index / schema sees the type.
    obj.entry("type".to_string())
        .or_insert_with(|| Value::String(label.clone()));

    let blob = rmp_serde::to_vec_named(&Value::Object(obj)).map_err(|e| e.to_string())?;
    core.add_node(id.clone(), blob);
    Ok(shape_node(core, &id, &field.selection))
}

/// `updateNode(id, props)` → read-merge-write the node blob. Each `props` key is merged
/// into the existing object. Errors if the node does not exist or fails to decode.
fn update_node(core: &GraphCore, field: &Field) -> Result<Value, String> {
    let id = arg_str(field, "id")?;
    let existing = core
        .get_node_properties(&id)
        .ok_or_else(|| format!("GraphQL: updateNode: node `{id}` not found"))?;
    let mut obj = decode(&existing)
        .and_then(|v| v.as_object().cloned())
        .ok_or_else(|| format!("GraphQL: updateNode: node `{id}` blob is not a JSON object"))?;

    for (k, v) in props_map(field)? {
        obj.insert(k, v);
    }

    let blob = rmp_serde::to_vec_named(&Value::Object(obj)).map_err(|e| e.to_string())?;
    // `add_node` on an existing id keeps the topology index and overwrites properties.
    core.add_node(id.clone(), blob);
    Ok(shape_node(core, &id, &field.selection))
}

/// `deleteNode(id)` → `remove_node`. Returns `{ id, deleted }` (the node is gone, so its
/// selection can no longer be resolved).
fn delete_node(core: &GraphCore, field: &Field) -> Result<Value, String> {
    let id = arg_str(field, "id")?;
    let existed = core.get_node_properties(&id).is_some();
    core.remove_node(id.clone());
    let mut obj = Map::new();
    obj.insert("id".to_string(), Value::String(id));
    obj.insert("deleted".to_string(), Value::Bool(existed));
    Ok(Value::Object(obj))
}

/// `addEdge(from, to, type, props)` → `add_edge`. `type` is stored under `relationship`
/// (the key the read resolver's `rel_matches` reads). Endpoints must exist.
fn add_edge(core: &GraphCore, field: &Field) -> Result<Value, String> {
    let from = arg_str(field, "from")?;
    let to = arg_str(field, "to")?;
    let rel = arg_str(field, "type")?;
    let mut obj = props_map(field)?;
    obj.insert("relationship".to_string(), Value::String(rel.clone()));

    let blob = rmp_serde::to_vec_named(&Value::Object(obj)).map_err(|e| e.to_string())?;
    core.add_edge(from.clone(), to.clone(), blob)?;

    Ok(edge_result(&from, &to, Some(&rel), "added"))
}

/// `removeEdge(from, to)` → `remove_edge` (removes the edge(s) between `from` and `to`).
fn remove_edge(core: &GraphCore, field: &Field) -> Result<Value, String> {
    let from = arg_str(field, "from")?;
    let to = arg_str(field, "to")?;
    core.remove_edge(from.clone(), to.clone());
    Ok(edge_result(&from, &to, None, "removed"))
}

// ── helpers ──────────────────────────────────────────────────────────────────────

/// Materialize a written node's selection using the SAME selection-resolution as the
/// query path, over a fresh post-write snapshot. `null` if the node is absent.
fn shape_node(core: &GraphCore, id: &str, selection: &[Field]) -> Value {
    let view = core.analysis_snapshot();
    match view.node_properties.get(id).and_then(|b| decode(b)) {
        Some(val) => resolve_selection(&view, id, &val, selection).unwrap_or(Value::Null),
        None => Value::Null,
    }
}

/// Build the `{from, to, type?, <status>: true}` descriptor an edge mutation returns.
fn edge_result(from: &str, to: &str, rel: Option<&str>, status: &str) -> Value {
    let mut obj = Map::new();
    obj.insert("from".to_string(), Value::String(from.to_string()));
    obj.insert("to".to_string(), Value::String(to.to_string()));
    if let Some(rel) = rel {
        obj.insert("type".to_string(), Value::String(rel.to_string()));
    }
    obj.insert(status.to_string(), Value::Bool(true));
    Value::Object(obj)
}

/// Look up a named argument on a field.
fn arg<'a>(field: &'a Field, name: &str) -> Option<&'a GqlValue> {
    field.args.iter().find(|(k, _)| k == name).map(|(_, v)| v)
}

/// Require a string-valued argument.
fn arg_str(field: &Field, name: &str) -> Result<String, String> {
    match arg(field, name) {
        Some(GqlValue::Str(s)) => Ok(s.clone()),
        Some(_) => Err(format!("`{name}` must be a string")),
        None => Err(format!(
            "mutation `{}` requires a `{name}` argument",
            field.name
        )),
    }
}

/// The `props` argument as a JSON object map (empty if absent). Errors if `props` is
/// present but not an input object.
fn props_map(field: &Field) -> Result<Map<String, Value>, String> {
    match arg(field, "props") {
        None => Ok(Map::new()),
        Some(v @ GqlValue::Object(_)) => match gql_to_json(v) {
            Value::Object(m) => Ok(m),
            _ => unreachable!("an Object GqlValue converts to a JSON object"),
        },
        Some(_) => Err("`props` must be an input object, e.g. props: {name: \"Alice\"}".into()),
    }
}

/// Generate a unique node id from a nanosecond timestamp + a per-process sequence.
fn generate_id(label: &str) -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    let seq = ID_SEQ.fetch_add(1, Ordering::Relaxed);
    format!("{label}:{nanos}-{seq}")
}
