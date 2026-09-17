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
    gql_to_json, parse_raw, Directive, Field, Fragment, GqlValue, Query, RawDocument, RawField,
    RawSelection,
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
        flatten_bounded_item(item, frags, vars, active, expanded_fields, &mut out)?;
    }
    Ok(out)
}

/// One selection item's contribution to [`flatten_selections_bounded`]'s
/// output. Split out (extract-method) so the recursive walk's per-variant
/// logic isn't nested inside a `for` + `match` in the same function — same
/// behaviour, same order, same bounds as before.
fn flatten_bounded_item(
    item: &RawSelection,
    frags: &HashMap<&str, &Fragment>,
    vars: &Variables,
    active: &mut HashSet<String>,
    expanded_fields: &mut usize,
    out: &mut Vec<Field>,
) -> Result<(), String> {
    match item {
        RawSelection::Field(rf) => {
            flatten_bounded_field(rf, frags, vars, active, expanded_fields, out)
        }
        RawSelection::Spread { name, directives } => {
            flatten_bounded_spread(name, directives, frags, vars, active, expanded_fields, out)
        }
        RawSelection::Inline {
            type_cond: _,
            directives,
            selections,
        } => {
            // Type conditions are accepted but not enforced (the resolver inlines
            // unconditionally — a node's type is known only at scan time).
            if !should_include(directives, vars)? {
                return Ok(());
            }
            out.extend(flatten_selections_bounded(
                selections,
                frags,
                vars,
                active,
                expanded_fields,
            )?);
            Ok(())
        }
    }
}

/// [`flatten_bounded_item`]'s `RawSelection::Field` arm: enforce the expanded-
/// field-count bound, then recurse into the field's own selection set.
fn flatten_bounded_field(
    rf: &RawField,
    frags: &HashMap<&str, &Fragment>,
    vars: &Variables,
    active: &mut HashSet<String>,
    expanded_fields: &mut usize,
    out: &mut Vec<Field>,
) -> Result<(), String> {
    if !should_include(&rf.directives, vars)? {
        return Ok(());
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
    Ok(())
}

/// [`flatten_bounded_item`]'s `RawSelection::Spread` arm: enforce the fragment-
/// cycle and expansion-depth bounds, then recurse into the fragment's body.
fn flatten_bounded_spread(
    name: &str,
    directives: &[Directive],
    frags: &HashMap<&str, &Fragment>,
    vars: &Variables,
    active: &mut HashSet<String>,
    expanded_fields: &mut usize,
    out: &mut Vec<Field>,
) -> Result<(), String> {
    if !should_include(directives, vars)? {
        return Ok(());
    }
    let frag = frags
        .get(name)
        .ok_or_else(|| format!("GraphQL: unknown fragment `...{name}`"))?;
    if !active.insert(name.to_string()) {
        return Err(format!("GraphQL: fragment cycle through `{name}`"));
    }
    if active.len() > MAX_FRAGMENT_EXPANSION_DEPTH {
        active.remove(name);
        return Err(format!(
            "GraphQL: fragment expansion depth exceeds {MAX_FRAGMENT_EXPANSION_DEPTH}"
        ));
    }
    let inner = flatten_selections_bounded(&frag.selections, frags, vars, active, expanded_fields)?;
    active.remove(name);
    out.extend(inner);
    Ok(())
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

// Root field resolution (plain `[Type]` array OR relay connection, CONCEPT:EG-KG.query.graphql-cursors) — split
// into its own module (kiss file-size split, not a rename: this file keeps every
// other resolution concern). Re-exported so `crate::resolver::{resolve_root,
// ordered_matches}` keeps working for `federation.rs`/`mutation.rs`.
mod connections;
pub(crate) use connections::{ordered_matches, resolve_root};

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
