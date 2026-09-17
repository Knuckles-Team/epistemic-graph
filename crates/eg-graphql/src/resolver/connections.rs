//! Root field resolution: a plain `[Type]` array OR a relay-style cursor
//! connection (CONCEPT:EG-KG.query.graphql-cursors).
//!
//! Split out of `resolver.rs` (a "shared module instead of growing a file past
//! the file-size caps" split, not a rename: `resolver.rs` keeps its own path
//! and every other resolution concern; this module holds only root-field /
//! relay-connection resolution). `resolve_root` and `ordered_matches` are
//! re-exported from `resolver.rs` so `crate::resolver::{resolve_root,
//! ordered_matches}` keeps working for `federation.rs`/`mutation.rs`.

use eg_core::graph::GraphView;
use serde_json::{Map, Value};

use crate::parser::{gql_to_json, Field, GqlValue};
use crate::schema::{decode, node_labels};

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
    let (limit, filters) = super::split_args(&field.args)?;
    let cap = limit
        .unwrap_or(super::MAX_ROOT_ROWS)
        .min(super::MAX_ROOT_ROWS);

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
        if !filters.iter().all(|(k, v)| super::prop_eq(&val, k, v)) {
            continue;
        }
        out.push(super::resolve_selection(view, &id, &val, &field.selection)?);
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
    let (sel_start, sel_end) = relay_window(&relay, &ordered)?;
    let page = &ordered[sel_start..sel_end];
    let has_next = sel_end < total;
    let has_prev = sel_start > 0;

    // Build the envelope honoring exactly the fields the selection asked for.
    let mut conn = Map::new();
    for sub in &field.selection {
        let cell = match sub.name.as_str() {
            "edges" => connection_edges(view, page, &sub.selection)?,
            "nodes" => {
                let mut nodes = Vec::new();
                for (id, val) in page {
                    nodes.push(super::resolve_selection(view, id, val, &sub.selection)?);
                }
                Value::Array(nodes)
            }
            "pageInfo" => {
                connection_page_info(page, &sub.selection, PageAdjacency { has_next, has_prev })
            }
            "totalCount" => Value::Number(total.into()),
            _ => Value::Null,
        };
        conn.insert(sub.alias.clone(), cell);
    }
    Ok(Value::Object(conn))
}

/// The half-open `[start, end)` slice of the ordered match set that the relay cursor
/// arguments select: `after` and `before` carve a window (a cursor naming an id that is
/// not in the set leaves its edge of the window where it was), then `first` takes from the
/// window's front and `last` from its back.
fn relay_window(relay: &RelayArgs, ordered: &[(String, Value)]) -> Result<(usize, usize), String> {
    let mut start = 0usize;
    if let Some(after) = &relay.after {
        let aid =
            cursor_decode(after).ok_or_else(|| "GraphQL: invalid `after` cursor".to_string())?;
        if let Some(pos) = ordered.iter().position(|(id, _)| *id == aid) {
            start = pos + 1;
        }
    }
    let mut end = ordered.len();
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
    if let Some(first) = relay.first {
        end = end.min(start + first);
    }
    if let Some(last) = relay.last {
        start = start.max(end.saturating_sub(last));
    }
    Ok((start, end))
}

/// The `edges { node cursor }` array for one page, honouring exactly the sub-fields the
/// selection asked for.
fn connection_edges(
    view: &GraphView,
    page: &[(String, Value)],
    selection: &[Field],
) -> Result<Value, String> {
    let mut edges = Vec::new();
    for (id, val) in page {
        let mut edge = Map::new();
        for ef in selection {
            let cell = match ef.name.as_str() {
                "node" => super::resolve_selection(view, id, val, &ef.selection)?,
                "cursor" => Value::String(cursor_encode(id)),
                _ => Value::Null,
            };
            edge.insert(ef.alias.clone(), cell);
        }
        edges.push(Value::Object(edge));
    }
    Ok(Value::Array(edges))
}

/// Whether a page's window has neighbours on either side. A named struct
/// instead of two positional `bool`s (kiss `boolean_parameters`) so
/// `connection_page_info`'s call site can't transpose "next" and "previous".
struct PageAdjacency {
    has_next: bool,
    has_prev: bool,
}

/// The `pageInfo` object for one page: its bounding cursors and whether the window has
/// neighbours on either side.
fn connection_page_info(
    page: &[(String, Value)],
    selection: &[Field],
    adjacency: PageAdjacency,
) -> Value {
    let cursor_at = |edge: Option<&(String, Value)>| {
        edge.map(|(id, _)| Value::String(cursor_encode(id)))
            .unwrap_or(Value::Null)
    };
    let mut pi = Map::new();
    for pf in selection {
        let cell = match pf.name.as_str() {
            "startCursor" => cursor_at(page.first()),
            "endCursor" => cursor_at(page.last()),
            "hasNextPage" => Value::Bool(adjacency.has_next),
            "hasPreviousPage" => Value::Bool(adjacency.has_prev),
            _ => Value::Null,
        };
        pi.insert(pf.alias.clone(), cell);
    }
    Value::Object(pi)
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
        if filters.iter().all(|(k, v)| super::prop_eq(&val, k, v)) {
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
