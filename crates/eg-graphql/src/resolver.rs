//! The GraphQL resolver (CONCEPT:EG-KG.query.sparql-completeness): compile a parsed [`Query`] to scans +
//! BFS over the SAME `GraphView` the Cypher / unified executor reads, and materialize
//! the result as JSON. NO second graph copy, NO async-graphql — the same "more scans
//! over the one substrate" approach as eg-rdf's SPARQL compile.
//!
//! Resolution per field shape (the "which maps to what" contract — kept byte-identical
//! to eg-query/cypher's primitives so a GraphQL query returns the SAME nodes/fields as
//! the equivalent Cypher query):
//!   * a ROOT field `Type(args) { … }` → the eg-core label index (the
//!     `type`/`node_type`/`label`/`labels` keys), filtered by the property-equality
//!     `args`, capped by `first`/`limit`, then each node's selection resolved;
//!   * a SCALAR field `prop` → that property from the node's blob (`null` if absent);
//!   * an OBJECT field `rel { … }` → follow outgoing edges whose canonical
//!     `relationship` equals `rel` to the target nodes, each
//!     recursively resolved → a LIST.

use std::collections::{HashMap, HashSet};

use eg_core::graph::GraphView;
use petgraph::visit::EdgeRef;
use serde_json::{Map, Value};

use crate::parser::{
    gql_to_json, parse_raw, Directive, Field, Fragment, GqlValue, Query, RawDocument, RawSelection,
};
use crate::schema::{decode, node_labels, relationship_name, Schema};

/// Execution variables (CONCEPT:EG-KG.query.fragments-variables-directives): a map of `$name` → bound value, used to
/// substitute variable references in arguments and evaluate `@skip`/`@include`.
pub(crate) type Variables = HashMap<String, GqlValue>;

/// Default cap on root-field rows when no `first`/`limit` arg is given (mirrors the
/// Cypher surface's implicit bound — one Response per Request).
const MAX_ROOT_ROWS: usize = 50_000;
/// Hard ceiling applied while expanding fragments/directives into the executable
/// field tree. This is deliberately enforced during expansion (not after it) so a
/// compact, exponentially-reused fragment document cannot allocate an unbounded AST.
const MAX_DESUGARED_FIELDS: usize = 10_000;
/// Fragment dependencies are not represented by selection-delimiter nesting. Bound
/// that independent recursion axis before following the next spread.
const MAX_FRAGMENT_EXPANSION_DEPTH: usize = 64;

/// The split of a field's args: an optional `first`/`limit` cap + the property-equality
/// filters (`(key, expected-value)` pairs).
type ArgSplit = (Option<usize>, Vec<(String, Value)>);

/// Parse + execute a GraphQL query string over `view`, returning the GraphQL-shaped
/// `{"data": { … }}` JSON. A parse error or an unknown root type is an `Err`. This is the
/// no-variables entry point (the server's existing caller); it delegates to
/// [`execute_with_variables`] with an empty variable set.
pub fn execute(view: &GraphView, query: &str) -> Result<Value, String> {
    execute_with_variables(view, query, &Value::Null)
}

/// Parse + execute a GraphQL query, binding the supplied `variables` (CONCEPT:EG-KG.query.fragments-variables-directives).
///
/// `variables` is a JSON object of `name → value`; a `query Q($x: Int) { … }` op's
/// declared defaults are applied first, then overridden by any provided value. Fragment
/// spreads / inline fragments are inlined and `@skip`/`@include` directives applied
/// against these variables BEFORE resolution. Pass `Value::Null` (or a non-object) for
/// no variables. A non-query operation is an `Err`.
pub fn execute_with_variables(
    view: &GraphView,
    query: &str,
    variables: &Value,
) -> Result<Value, String> {
    let doc = parse_raw(query).map_err(|e| e.to_string())?;
    if doc.op_kind != "query" {
        return Err(format!(
            "GraphQL: expected a query operation, found a {} (use the {} execution path)",
            doc.op_kind, doc.op_kind
        ));
    }
    let vars = bind_variables(&doc.var_defs, variables);
    // CONCEPT:EG-KG.query.apollo-federation-subgraph — an Apollo Federation subgraph query (`_service`/`_entities`) is
    // dispatched to the federation resolver before the normal node-label root path (those
    // meta-fields are not node labels, so `execute_query` would reject them).
    #[cfg(feature = "federation")]
    if crate::federation::is_federation_query(&doc) {
        return crate::federation::resolve(view, &doc, &vars);
    }
    let roots = flatten_document(&doc, &vars)?;
    execute_query(view, &Query { roots })
}

/// Execute an already-parsed [`Query`], validating each root field against the schema
/// derived from `view`.
pub fn execute_query(view: &GraphView, q: &Query) -> Result<Value, String> {
    let schema = Schema::from_view(view)?;
    let mut data = Map::new();
    for root in &q.roots {
        if !schema.has_type(&root.name) {
            return Err(format!(
                "GraphQL: no node type `{}` in the graph (root fields must be node labels)",
                root.name
            ));
        }
        // A plain root resolves to a `[Type]` array; a relay-connection root
        // (CONCEPT:EG-KG.query.graphql-cursors) resolves to a connection envelope object — so insert the
        // resolved Value directly rather than always wrapping in an array.
        data.insert(root.alias.clone(), resolve_root(view, root)?);
    }
    Ok(Value::Object(
        [("data".to_string(), Value::Object(data))]
            .into_iter()
            .collect(),
    ))
}

// ── fragments / variables / directives desugar (CONCEPT:EG-KG.query.fragments-variables-directives) ──────────────────

/// Bind execution variables: declared defaults first, then the JSON-provided overrides.
pub(crate) fn bind_variables(defs: &[crate::parser::VarDef], provided: &Value) -> Variables {
    let mut m = Variables::new();
    for d in defs {
        if let Some(def) = &d.default {
            m.insert(d.name.clone(), def.clone());
        }
    }
    if let Some(obj) = provided.as_object() {
        for (k, v) in obj {
            m.insert(k.clone(), json_to_gql(v));
        }
    }
    m
}

/// Lower a [`RawDocument`] to the plain [`Field`] tree the resolver consumes: inline
/// fragment spreads / inline fragments, apply `@skip`/`@include`, and substitute `$var`
/// references in arguments using `vars` (CONCEPT:EG-KG.query.fragments-variables-directives).
pub(crate) fn flatten_document(doc: &RawDocument, vars: &Variables) -> Result<Vec<Field>, String> {
    let frags: HashMap<&str, &Fragment> =
        doc.fragments.iter().map(|f| (f.name.as_str(), f)).collect();
    let mut active = HashSet::new();
    flatten_selections(&doc.selections, &frags, vars, &mut active)
}

/// Recursively inline a raw selection set into resolved [`Field`]s. `active` guards
/// against fragment-spread cycles.
pub(crate) fn flatten_selections(
    items: &[RawSelection],
    frags: &HashMap<&str, &Fragment>,
    vars: &Variables,
    active: &mut HashSet<String>,
) -> Result<Vec<Field>, String> {
    let mut expanded_fields = 0usize;
    flatten_selections_bounded(items, frags, vars, active, &mut expanded_fields)
}

fn flatten_selections_bounded(
    items: &[RawSelection],
    frags: &HashMap<&str, &Fragment>,
    vars: &Variables,
    active: &mut HashSet<String>,
    expanded_fields: &mut usize,
) -> Result<Vec<Field>, String> {
    let mut out = Vec::new();
    for item in items {
        match item {
            RawSelection::Field(rf) => {
                if !should_include(&rf.directives, vars)? {
                    continue;
                }
                *expanded_fields = expanded_fields.saturating_add(1);
                if *expanded_fields > MAX_DESUGARED_FIELDS {
                    return Err(format!(
                        "GraphQL: expanded field count exceeds {MAX_DESUGARED_FIELDS}"
                    ));
                }
                out.push(Field {
                    alias: rf.alias.clone(),
                    name: rf.name.clone(),
                    args: subst_args(&rf.args, vars),
                    selection: flatten_selections_bounded(
                        &rf.selections,
                        frags,
                        vars,
                        active,
                        expanded_fields,
                    )?,
                });
            }
            RawSelection::Spread { name, directives } => {
                if !should_include(directives, vars)? {
                    continue;
                }
                let frag = frags
                    .get(name.as_str())
                    .ok_or_else(|| format!("GraphQL: unknown fragment `...{name}`"))?;
                if !active.insert(name.clone()) {
                    return Err(format!("GraphQL: fragment cycle through `{name}`"));
                }
                if active.len() > MAX_FRAGMENT_EXPANSION_DEPTH {
                    active.remove(name);
                    return Err(format!(
                        "GraphQL: fragment expansion depth exceeds {MAX_FRAGMENT_EXPANSION_DEPTH}"
                    ));
                }
                let inner = flatten_selections_bounded(
                    &frag.selections,
                    frags,
                    vars,
                    active,
                    expanded_fields,
                )?;
                active.remove(name);
                out.extend(inner);
            }
            RawSelection::Inline {
                type_cond: _,
                directives,
                selections,
            } => {
                // Type conditions are accepted but not enforced (the resolver inlines
                // unconditionally — a node's type is known only at scan time).
                if !should_include(directives, vars)? {
                    continue;
                }
                out.extend(flatten_selections_bounded(
                    selections,
                    frags,
                    vars,
                    active,
                    expanded_fields,
                )?);
            }
        }
    }
    Ok(out)
}

/// Apply `@skip(if:)` / `@include(if:)` directives (CONCEPT:EG-KG.query.fragments-variables-directives): returns whether the
/// element survives. Unknown directives are ignored.
pub(crate) fn should_include(directives: &[Directive], vars: &Variables) -> Result<bool, String> {
    let mut include = true;
    for d in directives {
        match d.name.as_str() {
            "skip" if directive_if(d, vars)? => include = false,
            "include" if !directive_if(d, vars)? => include = false,
            _ => {}
        }
    }
    Ok(include)
}

/// Evaluate a directive's boolean `if:` argument (a literal `true`/`false` or a `$var`).
fn directive_if(d: &Directive, vars: &Variables) -> Result<bool, String> {
    let raw = d
        .args
        .iter()
        .find(|(k, _)| k == "if")
        .map(|(_, v)| v)
        .ok_or_else(|| format!("@{} requires an `if:` argument", d.name))?;
    match subst(raw, vars) {
        GqlValue::Bool(b) => Ok(b),
        GqlValue::Null => Ok(false), // unprovided variable → treat as false
        _ => Err(format!("@{} `if:` must resolve to a boolean", d.name)),
    }
}

/// Substitute `$var` references inside an argument value (recursively into lists /
/// objects). An unbound variable resolves to `null`.
pub(crate) fn subst(v: &GqlValue, vars: &Variables) -> GqlValue {
    match v {
        GqlValue::Var(name) => vars.get(name).cloned().unwrap_or(GqlValue::Null),
        GqlValue::List(items) => GqlValue::List(items.iter().map(|x| subst(x, vars)).collect()),
        GqlValue::Object(fs) => GqlValue::Object(
            fs.iter()
                .map(|(k, x)| (k.clone(), subst(x, vars)))
                .collect(),
        ),
        other => other.clone(),
    }
}

pub(crate) fn subst_args(args: &[(String, GqlValue)], vars: &Variables) -> Vec<(String, GqlValue)> {
    args.iter()
        .map(|(k, v)| (k.clone(), subst(v, vars)))
        .collect()
}

/// Convert a JSON variable value into a [`GqlValue`] (inverse of `gql_to_json`), so a
/// substituted variable flows through the same argument-coercion path as a literal.
fn json_to_gql(v: &Value) -> GqlValue {
    match v {
        Value::Null => GqlValue::Null,
        Value::Bool(b) => GqlValue::Bool(*b),
        Value::Number(n) => n
            .as_i64()
            .map(GqlValue::Int)
            .unwrap_or_else(|| GqlValue::Float(n.as_f64().unwrap_or(0.0))),
        Value::String(s) => GqlValue::Str(s.clone()),
        Value::Array(a) => GqlValue::List(a.iter().map(json_to_gql).collect()),
        Value::Object(o) => {
            GqlValue::Object(o.iter().map(|(k, v)| (k.clone(), json_to_gql(v))).collect())
        }
    }
}

// ── root resolution (plain `[Type]` array OR relay connection, CONCEPT:EG-KG.query.graphql-cursors) ────

/// Resolve a root field. If its selection has a relay shape (an `edges` / `pageInfo`
/// child), return a connection envelope (CONCEPT:EG-KG.query.graphql-cursors); otherwise return the plain
/// `[Type]` array (unchanged behavior).
pub(crate) fn resolve_root(view: &GraphView, field: &Field) -> Result<Value, String> {
    if is_relay_selection(&field.selection) {
        resolve_connection(view, field)
    } else {
        Ok(Value::Array(resolve_plain_root(view, field)?))
    }
}

/// A selection is a relay connection when it selects `edges` or `pageInfo` (the envelope
/// shape), rather than node properties directly.
fn is_relay_selection(selection: &[Field]) -> bool {
    selection
        .iter()
        .any(|f| f.name == "edges" || f.name == "pageInfo")
}

/// Resolve a plain root field: label scan + arg filter + `first`/`limit`, then per-node
/// selection. Returns the list of result objects.
fn resolve_plain_root(view: &GraphView, field: &Field) -> Result<Vec<Value>, String> {
    let (limit, filters) = split_args(&field.args)?;
    let cap = limit.unwrap_or(MAX_ROOT_ROWS).min(MAX_ROOT_ROWS);

    // Candidate node ids carrying this label, in a stable (sorted) order so the result
    // is deterministic across runs (matching a DB's stable scan order).
    let mut candidates = Vec::new();
    for (id, blob) in &view.node_properties {
        let val = decode(blob)?;
        if node_labels(&val)?.iter().any(|label| label == &field.name) {
            candidates.push(id.clone());
        }
    }
    candidates.sort();

    let mut out = Vec::new();
    for id in candidates {
        if out.len() >= cap {
            break;
        }
        let blob = view
            .node_properties
            .get(&id)
            .ok_or_else(|| format!("GraphQL: selected node `{id}` has no properties"))?;
        let val = decode(blob)?;
        // property-equality filters from the args.
        if !filters.iter().all(|(k, v)| prop_eq(&val, k, v)) {
            continue;
        }
        out.push(resolve_selection(view, &id, &val, &field.selection)?);
    }
    Ok(out)
}

/// Resolve a root field as a relay-style connection (CONCEPT:EG-KG.query.graphql-cursors): apply the
/// `first`/`after`/`before`/`last` cursor args over the SAME deterministic (id-sorted)
/// match order the plain path uses, and materialize the `edges { node cursor } pageInfo`
/// envelope the selection asks for. Cursors are the base64 of the node id (the sort
/// key), so they are stable across runs.
fn resolve_connection(view: &GraphView, field: &Field) -> Result<Value, String> {
    let (relay, filters) = split_relay_args(&field.args)?;
    let ordered = ordered_matches(view, &field.name, &filters)?;
    let total = ordered.len();

    // `after` / `before` carve a window out of the full ordered match set.
    let mut start = 0usize;
    if let Some(after) = &relay.after {
        let aid =
            cursor_decode(after).ok_or_else(|| "GraphQL: invalid `after` cursor".to_string())?;
        if let Some(pos) = ordered.iter().position(|(id, _)| *id == aid) {
            start = pos + 1;
        }
    }
    let mut end = total;
    if let Some(before) = &relay.before {
        let bid =
            cursor_decode(before).ok_or_else(|| "GraphQL: invalid `before` cursor".to_string())?;
        if let Some(pos) = ordered.iter().position(|(id, _)| *id == bid) {
            end = pos;
        }
    }
    if end < start {
        end = start;
    }

    // `first` takes from the front of the window; `last` from the back.
    let mut sel_start = start;
    let mut sel_end = end;
    if let Some(first) = relay.first {
        sel_end = sel_end.min(sel_start + first);
    }
    if let Some(last) = relay.last {
        sel_start = sel_start.max(sel_end.saturating_sub(last));
    }
    let page = &ordered[sel_start..sel_end];
    let has_next = sel_end < total;
    let has_prev = sel_start > 0;

    // Build the envelope honoring exactly the fields the selection asked for.
    let mut conn = Map::new();
    for sub in &field.selection {
        match sub.name.as_str() {
            "edges" => {
                let mut edges = Vec::new();
                for (id, val) in page {
                    let mut edge = Map::new();
                    for ef in &sub.selection {
                        let cell = match ef.name.as_str() {
                            "node" => resolve_selection(view, id, val, &ef.selection)?,
                            "cursor" => Value::String(cursor_encode(id)),
                            _ => Value::Null,
                        };
                        edge.insert(ef.alias.clone(), cell);
                    }
                    edges.push(Value::Object(edge));
                }
                conn.insert(sub.alias.clone(), Value::Array(edges));
            }
            "nodes" => {
                let mut nodes = Vec::new();
                for (id, val) in page {
                    nodes.push(resolve_selection(view, id, val, &sub.selection)?);
                }
                conn.insert(sub.alias.clone(), Value::Array(nodes));
            }
            "pageInfo" => {
                let start_cursor = page
                    .first()
                    .map(|(id, _)| Value::String(cursor_encode(id)))
                    .unwrap_or(Value::Null);
                let end_cursor = page
                    .last()
                    .map(|(id, _)| Value::String(cursor_encode(id)))
                    .unwrap_or(Value::Null);
                let mut pi = Map::new();
                for pf in &sub.selection {
                    let cell = match pf.name.as_str() {
                        "startCursor" => start_cursor.clone(),
                        "endCursor" => end_cursor.clone(),
                        "hasNextPage" => Value::Bool(has_next),
                        "hasPreviousPage" => Value::Bool(has_prev),
                        _ => Value::Null,
                    };
                    pi.insert(pf.alias.clone(), cell);
                }
                conn.insert(sub.alias.clone(), Value::Object(pi));
            }
            "totalCount" => {
                conn.insert(sub.alias.clone(), Value::Number(total.into()));
            }
            _ => {
                conn.insert(sub.alias.clone(), Value::Null);
            }
        }
    }
    Ok(Value::Object(conn))
}

/// The full, deterministically (id-)sorted set of nodes carrying `label` that pass the
/// property-equality `filters` — the ordered basis relay cursors page over.
pub(crate) fn ordered_matches(
    view: &GraphView,
    label: &str,
    filters: &[(String, Value)],
) -> Result<Vec<(String, Value)>, String> {
    let mut ids = Vec::new();
    for (id, blob) in &view.node_properties {
        let val = decode(blob)?;
        if node_labels(&val)?
            .iter()
            .any(|candidate| candidate == label)
        {
            ids.push(id.clone());
        }
    }
    ids.sort();
    let mut out = Vec::new();
    for id in ids {
        let blob = view
            .node_properties
            .get(&id)
            .ok_or_else(|| format!("GraphQL: selected node `{id}` has no properties"))?;
        let val = decode(blob)?;
        if filters.iter().all(|(k, v)| prop_eq(&val, k, v)) {
            out.push((id, val));
        }
    }
    Ok(out)
}

/// The relay cursor args split off a field's arguments (CONCEPT:EG-KG.query.graphql-cursors).
#[derive(Default)]
struct RelayArgs {
    first: Option<usize>,
    last: Option<usize>,
    after: Option<String>,
    before: Option<String>,
}

/// Split a relay field's args into the cursor args + the property-equality filters.
fn split_relay_args(
    args: &[(String, GqlValue)],
) -> Result<(RelayArgs, Vec<(String, Value)>), String> {
    let mut r = RelayArgs::default();
    let mut filters = Vec::new();
    for (k, v) in args {
        match k.as_str() {
            "first" => r.first = Some(as_count(k, v)?),
            "last" => r.last = Some(as_count(k, v)?),
            "after" => r.after = Some(as_cursor(k, v)?),
            "before" => r.before = Some(as_cursor(k, v)?),
            _ => filters.push((k.clone(), gql_to_json(v))),
        }
    }
    Ok((r, filters))
}

fn as_count(k: &str, v: &GqlValue) -> Result<usize, String> {
    match v {
        GqlValue::Int(n) if *n >= 0 => Ok(*n as usize),
        _ => Err(format!("`{k}` must be a non-negative integer")),
    }
}

fn as_cursor(k: &str, v: &GqlValue) -> Result<String, String> {
    match v {
        GqlValue::Str(s) => Ok(s.clone()),
        _ => Err(format!("`{k}` must be a cursor string")),
    }
}

/// Encode a node id (the deterministic sort key) as an opaque relay cursor.
fn cursor_encode(id: &str) -> String {
    b64_encode(id.as_bytes())
}

/// Decode an opaque relay cursor back to the node id it points at.
fn cursor_decode(cursor: &str) -> Option<String> {
    String::from_utf8(b64_decode(cursor)?).ok()
}

/// A tiny hand-written standard base64 encoder (keeps the crate dependency-free — no
/// base64 crate pulled in for CONCEPT:EG-KG.query.graphql-cursors cursors).
fn b64_encode(input: &[u8]) -> String {
    const T: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::new();
    for chunk in input.chunks(3) {
        let b0 = chunk[0];
        let b1 = *chunk.get(1).unwrap_or(&0);
        let b2 = *chunk.get(2).unwrap_or(&0);
        out.push(T[(b0 >> 2) as usize] as char);
        out.push(T[(((b0 & 0x03) << 4) | (b1 >> 4)) as usize] as char);
        if chunk.len() > 1 {
            out.push(T[(((b1 & 0x0f) << 2) | (b2 >> 6)) as usize] as char);
        } else {
            out.push('=');
        }
        if chunk.len() > 2 {
            out.push(T[(b2 & 0x3f) as usize] as char);
        } else {
            out.push('=');
        }
    }
    out
}

/// The matching standard base64 decoder (ignores `=` padding).
fn b64_decode(s: &str) -> Option<Vec<u8>> {
    fn val(c: u8) -> Option<u8> {
        match c {
            b'A'..=b'Z' => Some(c - b'A'),
            b'a'..=b'z' => Some(c - b'a' + 26),
            b'0'..=b'9' => Some(c - b'0' + 52),
            b'+' => Some(62),
            b'/' => Some(63),
            _ => None,
        }
    }
    let bytes: Vec<u8> = s.bytes().filter(|&c| c != b'=').collect();
    let mut out = Vec::new();
    for chunk in bytes.chunks(4) {
        let mut buf = [0u8; 4];
        let mut n = 0;
        for (i, &c) in chunk.iter().enumerate() {
            buf[i] = val(c)?;
            n += 1;
        }
        if n >= 2 {
            out.push((buf[0] << 2) | (buf[1] >> 4));
        }
        if n >= 3 {
            out.push((buf[1] << 4) | (buf[2] >> 2));
        }
        if n >= 4 {
            out.push((buf[2] << 6) | buf[3]);
        }
    }
    Some(out)
}

/// Resolve a node's selection set into a JSON object. `pub(crate)` so the mutation
/// executor (CONCEPT:EG-KG.query.mutation) can shape the object it returns for a written node using
/// the SAME selection-resolution the query path uses.
pub(crate) fn resolve_selection(
    view: &GraphView,
    node_id: &str,
    val: &Value,
    selection: &[Field],
) -> Result<Value, String> {
    let mut obj = Map::new();
    // A node with no selection set still resolves to its id (so `{ Person }` is legal).
    if selection.is_empty() {
        obj.insert("id".to_string(), Value::String(node_id.to_string()));
        return Ok(Value::Object(obj));
    }
    for f in selection {
        // The `__typename` meta-field resolves to the node's primary label — needed by
        // GraphQL introspection and by Apollo Federation `_entities` selections
        // (CONCEPT:EG-KG.query.apollo-federation-subgraph), which typically select `__typename` alongside an inline
        // fragment per entity type.
        if f.name == "__typename" && f.selection.is_empty() {
            let tn = node_labels(val)?
                .into_iter()
                .next()
                .ok_or_else(|| format!("GraphQL: node `{node_id}` has no object type"))?;
            obj.insert(f.alias.clone(), Value::String(tn));
            continue;
        }
        if f.selection.is_empty() && f.args.is_empty() {
            // scalar field: the node property (or `id`), `null` if absent.
            let cell = if f.name == "id" {
                Value::String(node_id.to_string())
            } else {
                prop_value(val, &f.name).unwrap_or(Value::Null)
            };
            obj.insert(f.alias.clone(), cell);
        } else {
            // object field: an edge relationship — traverse to targets, recurse.
            let targets = resolve_edge(view, node_id, f)?;
            obj.insert(f.alias.clone(), Value::Array(targets));
        }
    }
    Ok(Value::Object(obj))
}

/// Resolve an edge field `rel { … }`: outgoing edges typed `rel` from `node_id` →
/// the target nodes, each resolved against `field.selection`. Honors an optional
/// `first`/`limit` arg on the edge field.
fn resolve_edge(view: &GraphView, node_id: &str, field: &Field) -> Result<Vec<Value>, String> {
    let (limit, filters) = split_args(&field.args)?;
    let cap = limit.unwrap_or(MAX_ROOT_ROWS).min(MAX_ROOT_ROWS);

    let mut targets = outgoing_targets(view, node_id, &field.name)?;
    targets.sort();
    targets.dedup();

    let mut out = Vec::new();
    for tid in targets {
        if out.len() >= cap {
            break;
        }
        let blob = view
            .node_properties
            .get(&tid)
            .ok_or_else(|| format!("GraphQL: edge target `{tid}` has no properties"))?;
        let val = decode(blob)?;
        if !filters.iter().all(|(k, v)| prop_eq(&val, k, v)) {
            continue;
        }
        out.push(resolve_selection(view, &tid, &val, &field.selection)?);
    }
    Ok(out)
}

/// Target node ids of outgoing edges whose canonical `relationship` is `rel`.
fn outgoing_targets(view: &GraphView, src: &str, rel: &str) -> Result<Vec<String>, String> {
    let Some(&src_idx) = view.node_map.get(src) else {
        return Ok(Vec::new());
    };
    let mut out = Vec::new();
    let mut seen: HashSet<String> = HashSet::new();
    for edge in view
        .graph
        .edges_directed(src_idx, petgraph::Direction::Outgoing)
    {
        let tgt = edge.target();
        let tid = view
            .graph
            .node_weight(tgt)
            .ok_or_else(|| "GraphQL: edge target is missing from the graph".to_string())?;
        if rel_matches(view, src, tid, rel)? && seen.insert(tid.clone()) {
            out.push(tid.clone());
        }
    }
    Ok(out)
}

/// Does the stored edge `(from→to)` carry relationship `rel`? Reads the edge blob's
/// canonical `relationship` field.
fn rel_matches(view: &GraphView, from: &str, to: &str, rel: &str) -> Result<bool, String> {
    let Some(blobs) = view
        .edge_properties
        .get(&(from.to_string(), to.to_string()))
    else {
        return Ok(false);
    };
    for blob in blobs {
        let value = decode(blob)?;
        let properties = value
            .as_object()
            .ok_or_else(|| "GraphQL: edge properties must be an object".to_string())?;
        if relationship_name(properties)? == Some(rel) {
            return Ok(true);
        }
    }
    Ok(false)
}

/// Split a field's args into (the `first`/`limit` cap, the property-equality filters).
fn split_args(args: &[(String, GqlValue)]) -> Result<ArgSplit, String> {
    let mut limit = None;
    let mut filters = Vec::new();
    for (k, v) in args {
        if k == "first" || k == "limit" {
            match v {
                GqlValue::Int(n) if *n >= 0 => limit = Some(*n as usize),
                _ => return Err(format!("`{k}` must be a non-negative integer")),
            }
        } else {
            filters.push((k.clone(), gql_to_json(v)));
        }
    }
    Ok((limit, filters))
}

/// Read one property from a node's blob, unwrapping the property-graph `{value: …}`
/// cell shape if present (so a typed literal returns its value, matching SPARQL).
fn prop_value(val: &Value, key: &str) -> Option<Value> {
    let cell = val.get(key)?;
    // A property-graph cell may be `{"value": …, "datatype": …}` or a bare scalar.
    if let Some(obj) = cell.as_object() {
        if let Some(inner) = obj.get("value") {
            return Some(inner.clone());
        }
    }
    Some(cell.clone())
}

/// Property-equality filter: does `key`'s (unwrapped) value equal `expected`?
fn prop_eq(val: &Value, key: &str, expected: &Value) -> bool {
    match prop_value(val, key) {
        Some(actual) => values_eq(&actual, expected),
        None => false,
    }
}

/// Equality across JSON types, tolerant of string-vs-number (a GraphQL `name: "30"`
/// matches a stored numeric `30` and vice-versa, since property cells are untyped).
fn values_eq(a: &Value, b: &Value) -> bool {
    if a == b {
        return true;
    }
    match (a.as_str(), b.as_str()) {
        (Some(sa), Some(sb)) => sa == sb,
        _ => {
            let sa = scalar_string(a);
            let sb = scalar_string(b);
            sa.is_some() && sa == sb
        }
    }
}

fn scalar_string(v: &Value) -> Option<String> {
    match v {
        Value::String(s) => Some(s.clone()),
        Value::Number(n) => Some(n.to_string()),
        Value::Bool(b) => Some(b.to_string()),
        _ => None,
    }
}
