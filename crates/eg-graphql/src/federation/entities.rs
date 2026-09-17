//! Apollo Federation v2 `_service`/`_entities` resolution (CONCEPT:EG-KG.query.apollo-federation-subgraph).
//!
//! Split out of `federation.rs` (a "shared module instead of growing a file
//! past the file-size caps" split, not a rename: `federation.rs` keeps its own
//! path and its schema-derivation/SDL-emission/directive-scanning content; this
//! module holds only the `_service { sdl }` and `_entities(representations:)`
//! resolvers the root-field dispatcher in `federation.rs` calls into).

use std::collections::HashMap;

use serde_json::{Map, Value};

use eg_core::graph::GraphView;

use super::FederatedSchema;
use crate::parser::{gql_to_json, Directive, Field, Fragment, RawField, RawSelection};
use crate::resolver::Variables;
use crate::schema::{decode, node_labels};

/// Resolve `_service { sdl }` (CONCEPT:EG-KG.query.apollo-federation-subgraph): the `sdl` field returns the subgraph SDL.
pub(super) fn resolve_service(fed: &FederatedSchema, rf: &RawField) -> Value {
    let sdl = fed.to_federation_sdl();
    let mut obj = Map::new();
    if rf.selections.is_empty() {
        obj.insert("sdl".to_string(), Value::String(sdl));
        return Value::Object(obj);
    }
    for sub in &rf.selections {
        if let RawSelection::Field(f) = sub {
            let cell = match f.name.as_str() {
                "sdl" => Value::String(sdl.clone()),
                "__typename" => Value::String("_Service".to_string()),
                _ => Value::Null,
            };
            obj.insert(f.alias.clone(), cell);
        }
    }
    Value::Object(obj)
}

/// Resolve `_entities(representations: [_Any!]!): [_Entity]!` (CONCEPT:EG-KG.query.apollo-federation-subgraph). Each
/// representation is a `{__typename, <key fields>}` JSON object; look the entity up by its
/// key in the graph and materialize the selection (matching the representation's
/// `__typename` against inline fragments). An unresolvable representation yields `null`,
/// keeping the result list aligned with the input list.
pub(super) fn resolve_entities(
    view: &GraphView,
    fed: &FederatedSchema,
    rf: &RawField,
    frags: &HashMap<&str, &Fragment>,
    vars: &Variables,
) -> Result<Value, String> {
    let reps_arg = rf
        .args
        .iter()
        .find(|(k, _)| k == "representations")
        .map(|(_, v)| gql_to_json(&crate::resolver::subst(v, vars)))
        .ok_or_else(|| {
            "GraphQL federation: `_entities` requires a `representations` argument".to_string()
        })?;
    let reps = reps_arg.as_array().ok_or_else(|| {
        "GraphQL federation: `representations` must be a list of entity references".to_string()
    })?;

    let mut out = Vec::with_capacity(reps.len());
    for rep in reps {
        let Some(obj) = rep.as_object() else {
            out.push(Value::Null);
            continue;
        };
        let Some(typename) = obj.get("__typename").and_then(|v| v.as_str()) else {
            out.push(Value::Null);
            continue;
        };
        match lookup_entity(view, fed, typename, obj)? {
            Some((id, val)) => {
                let fields = flatten_typed(&rf.selections, frags, vars, typename)?;
                let mut resolved = crate::resolver::resolve_selection(view, &id, &val, &fields)?;
                // Force `__typename` to the representation's declared type (a node may
                // carry several labels; the federation answer is the requested one).
                if let Value::Object(m) = &mut resolved {
                    for f in &fields {
                        if f.name == "__typename" {
                            m.insert(f.alias.clone(), Value::String(typename.to_string()));
                        }
                    }
                }
                out.push(resolved);
            }
            None => out.push(Value::Null),
        }
    }
    Ok(Value::Array(out))
}

/// Look an entity up by its `@key` (CONCEPT:EG-KG.query.apollo-federation-subgraph). Picks the first key whose field-set is
/// fully supplied by the representation; the `id` key hits the node index directly, other
/// keys scan nodes of the type by property equality. Returns the node id + decoded props.
pub(super) fn lookup_entity(
    view: &GraphView,
    fed: &FederatedSchema,
    typename: &str,
    repr: &Map<String, Value>,
) -> Result<Option<(String, Value)>, String> {
    let Some(meta) = fed.entities.get(typename) else {
        return Ok(None);
    };
    for key in &meta.keys {
        if !key.resolvable {
            continue;
        }
        let fields: Vec<&str> = key.fields.split_whitespace().collect();
        if fields.is_empty() || !fields.iter().all(|f| repr.contains_key(*f)) {
            continue;
        }
        if fields == ["id"] {
            let Some(id) = repr.get("id").and_then(json_id) else {
                return Ok(None);
            };
            let Some(blob) = view.node_properties.get(&id) else {
                return Ok(None);
            };
            let val = decode(blob)?;
            return Ok(node_labels(&val)?
                .iter()
                .any(|l| l == typename)
                .then_some((id, val)));
        }
        // General (property-keyed) entity: match all key fields as property filters.
        let filters: Vec<(String, Value)> = fields
            .iter()
            .map(|field| {
                repr.get(*field)
                    .cloned()
                    .map(|value| ((*field).to_string(), value))
                    .ok_or_else(|| {
                        format!(
                            "GraphQL federation: representation lost required key field `{field}`"
                        )
                    })
            })
            .collect::<Result<_, _>>()?;
        return Ok(crate::resolver::ordered_matches(view, typename, &filters)?
            .into_iter()
            .next());
    }
    Ok(None)
}

/// Coerce a representation's `id` value to the node-id string (ids are string keys; a
/// numeric id representation is stringified).
fn json_id(v: &Value) -> Option<String> {
    match v {
        Value::String(s) => Some(s.clone()),
        Value::Number(n) => Some(n.to_string()),
        _ => None,
    }
}

/// Flatten a raw selection set for a CONCRETE entity type (CONCEPT:EG-KG.query.apollo-federation-subgraph): like the
/// resolver's `flatten_selections` but type-condition aware — an inline fragment
/// `... on T { … }` (or a named fragment `on T`) is included only when `T` matches
/// `typename` (or is unconditional). Keeps `__typename` as a selectable field.
fn flatten_typed(
    items: &[RawSelection],
    frags: &HashMap<&str, &Fragment>,
    vars: &Variables,
    typename: &str,
) -> Result<Vec<Field>, String> {
    let mut out = Vec::new();
    for item in items {
        flatten_typed_item(item, frags, vars, typename, &mut out)?;
    }
    Ok(out)
}

/// One selection item's contribution to [`flatten_typed`]'s output. Split out
/// of `flatten_typed` (extract-method) so the recursive walk's per-variant
/// logic isn't nested inside a `for` + `match` in the same function — same
/// behaviour, same order as before.
fn flatten_typed_item(
    item: &RawSelection,
    frags: &HashMap<&str, &Fragment>,
    vars: &Variables,
    typename: &str,
    out: &mut Vec<Field>,
) -> Result<(), String> {
    match item {
        RawSelection::Field(rf) => flatten_typed_field(rf, frags, vars, typename, out),
        RawSelection::Spread { name, directives } => {
            flatten_typed_spread(name, directives, frags, vars, typename, out)
        }
        RawSelection::Inline {
            type_cond,
            directives,
            selections,
        } => flatten_typed_inline(
            type_cond.as_deref(),
            directives,
            selections,
            frags,
            vars,
            typename,
            out,
        ),
    }
}

/// [`flatten_typed_item`]'s `RawSelection::Field` arm.
fn flatten_typed_field(
    rf: &RawField,
    frags: &HashMap<&str, &Fragment>,
    vars: &Variables,
    typename: &str,
    out: &mut Vec<Field>,
) -> Result<(), String> {
    if !crate::resolver::should_include(&rf.directives, vars)? {
        return Ok(());
    }
    out.push(Field {
        alias: rf.alias.clone(),
        name: rf.name.clone(),
        args: crate::resolver::subst_args(&rf.args, vars),
        selection: flatten_typed(&rf.selections, frags, vars, typename)?,
    });
    Ok(())
}

/// [`flatten_typed_item`]'s `RawSelection::Spread` arm: expand a named fragment
/// when its type condition matches (or is unconditional).
fn flatten_typed_spread(
    name: &str,
    directives: &[Directive],
    frags: &HashMap<&str, &Fragment>,
    vars: &Variables,
    typename: &str,
    out: &mut Vec<Field>,
) -> Result<(), String> {
    if !crate::resolver::should_include(directives, vars)? {
        return Ok(());
    }
    let frag = frags
        .get(name)
        .ok_or_else(|| format!("GraphQL: unknown fragment `...{name}`"))?;
    if frag.type_cond.is_empty() || frag.type_cond == typename {
        out.extend(flatten_typed(&frag.selections, frags, vars, typename)?);
    }
    Ok(())
}

/// [`flatten_typed_item`]'s `RawSelection::Inline` arm: expand an inline
/// fragment when its type condition matches (or is unconditional).
fn flatten_typed_inline(
    type_cond: Option<&str>,
    directives: &[Directive],
    selections: &[RawSelection],
    frags: &HashMap<&str, &Fragment>,
    vars: &Variables,
    typename: &str,
    out: &mut Vec<Field>,
) -> Result<(), String> {
    if !crate::resolver::should_include(directives, vars)? {
        return Ok(());
    }
    if type_cond.is_none_or(|tc| tc == typename) {
        out.extend(flatten_typed(selections, frags, vars, typename)?);
    }
    Ok(())
}
