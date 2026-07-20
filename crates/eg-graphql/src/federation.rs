//! CONCEPT:EG-KG.query.apollo-federation-subgraph — Apollo Federation v2 SUBGRAPH support over the native GraphQL
//! surface, so the epistemic-graph engine composes into an Apollo supergraph as one
//! federated subgraph (alongside the existing schema/query/mutation/subscription surface,
//! CONCEPT:EG-KG.compute.cdc-event-emit..066). This is ADDITIVE — it reuses the existing resolver primitives
//! (label scans, edge traversal, selection resolution) and does NOT rewrite them.
//!
//! What a subgraph must provide (federation v2 spec):
//!   * a schema `@link`ed to `https://specs.apollo.dev/federation/v2.x`, exposing the
//!     `@key` / `@shareable` / `@external` / `@provides` / `@requires` / `@override`
//!     directives. A type with a `@key` is a federated ENTITY.
//!   * `_service: _Service!` whose `sdl: String!` field returns the subgraph SDL the
//!     Apollo router fetches during composition.
//!   * `_entities(representations: [_Any!]!): [_Entity]!` — the router hands back
//!     `{__typename, <key fields>}` JSON "representations"; the subgraph looks each entity
//!     up by its key and returns it, so the router can stitch fields across subgraphs.
//!
//! Entity mapping (the natural property-graph → federation one): every node object type
//! derived from the graph (CONCEPT:EG-KG.query.sparql-completeness) is a federated entity keyed by its node id
//! (`@key(fields: "id")`). Flat scalar and compound `@key` field sets are validated,
//! emitted, and resolved for node-backed types. Unsupported nested field sets and
//! non-node entities are rejected rather than accepted as a partial schema.

use std::collections::{BTreeMap, HashMap, HashSet};

use eg_core::graph::GraphView;
use serde_json::{Map, Value};

use crate::parser::{gql_to_json, Field, Fragment, RawDocument, RawField, RawSelection};
use crate::resolver::Variables;
use crate::schema::{decode, node_labels, validate_field_name, validate_type_name, Schema};

/// The Apollo Federation v2 spec URL this subgraph links against (CONCEPT:EG-KG.query.apollo-federation-subgraph).
pub const FEDERATION_LINK_URL: &str = "https://specs.apollo.dev/federation/v2.3";

/// A `@key(fields: "…")` directive making a type a federated entity (CONCEPT:EG-KG.query.apollo-federation-subgraph).
/// `fields` is the raw field-set string (e.g. `"id"` or `"sku pkg"`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct KeyDirective {
    pub fields: String,
    /// `@key(… resolvable: false)` marks a reference-only key; defaults to `true`.
    pub resolvable: bool,
}

/// Type-level federation metadata (CONCEPT:EG-KG.query.apollo-federation-subgraph). A non-empty `keys` makes the type a
/// federated ENTITY.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct EntityMeta {
    pub keys: Vec<KeyDirective>,
    pub shareable: bool,
}

/// Field-level federation directives (CONCEPT:EG-KG.query.apollo-federation-subgraph): `@shareable` / `@external` /
/// `@provides(fields:)` / `@requires(fields:)` / `@override(from:)`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct FieldFedMeta {
    pub shareable: bool,
    pub external: bool,
    pub provides: Option<String>,
    pub requires: Option<String>,
    pub override_from: Option<String>,
}

/// A federated view of the derived schema (CONCEPT:EG-KG.query.apollo-federation-subgraph): the base schema-from-graph
/// plus federation directive metadata per type / per field.
#[derive(Clone, Debug, Default)]
pub struct FederatedSchema {
    pub base: Schema,
    /// Federation metadata keyed by type name (only types with a `@key` are entities).
    pub entities: BTreeMap<String, EntityMeta>,
    /// Field-level federation directives, keyed by `(type, field)`.
    pub field_meta: BTreeMap<(String, String), FieldFedMeta>,
    declared_types: std::collections::BTreeSet<String>,
    declared_fields: std::collections::BTreeSet<(String, String)>,
}

impl FederatedSchema {
    /// Derive a federated schema from a live `GraphView` (CONCEPT:EG-KG.query.apollo-federation-subgraph). Every node
    /// object type becomes a federated entity keyed by its node id (`@key(fields:"id")`)
    /// — the natural property-graph → federation mapping. Overlay explicit federation
    /// directives afterward with [`Self::parse_directives`].
    pub fn from_view(view: &GraphView) -> Result<Self, String> {
        let base = Schema::from_view(view)?;
        let mut entities = BTreeMap::new();
        for name in base.types.keys() {
            entities.insert(
                name.clone(),
                EntityMeta {
                    keys: vec![KeyDirective {
                        fields: "id".to_string(),
                        resolvable: true,
                    }],
                    shareable: false,
                },
            );
        }
        Ok(FederatedSchema {
            base,
            entities,
            field_meta: BTreeMap::new(),
            declared_types: std::collections::BTreeSet::new(),
            declared_fields: std::collections::BTreeSet::new(),
        })
    }

    /// Parse Apollo federation directives out of an SDL fragment and OVERLAY them onto
    /// this schema (CONCEPT:EG-KG.query.apollo-federation-subgraph). Line-oriented: a `type Name @dir … {` line sets
    /// type-level directives (`@key`/`@shareable`); a field line `field: T @dir …` inside
    /// the block sets field-level directives (`@external`/`@shareable`/`@provides`/
    /// `@requires`/`@override`). This is the "parse" half of parse+emit; combined with
    /// [`Self::to_federation_sdl`] it round-trips the directive vocabulary.
    pub fn parse_directives(&mut self, sdl: &str) -> Result<(), String> {
        let mut entities = self.entities.clone();
        let mut field_meta = self.field_meta.clone();
        let mut declared_types = self.declared_types.clone();
        let mut declared_fields = self.declared_fields.clone();
        let mut current: Option<String> = None;
        for raw in sdl.lines() {
            let line = raw.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            if let Some(rest) = line.strip_prefix("type ") {
                if current.is_some() || line.contains('}') {
                    return Err("GraphQL federation SDL: malformed type definition".to_string());
                }
                let open = rest.rfind('{').ok_or_else(|| {
                    "GraphQL federation SDL: type definition is missing `{`".to_string()
                })?;
                if !rest[open + 1..].trim().is_empty() {
                    return Err(
                        "GraphQL federation SDL: type header has content after `{`".to_string()
                    );
                }
                let header = rest[..open].trim_end();
                let name: String = header
                    .split(|c: char| c.is_whitespace() || c == '@')
                    .next()
                    .unwrap_or("")
                    .to_string();
                validate_type_name(&name)?;
                if !self.base.types.contains_key(&name) {
                    return Err(format!(
                        "GraphQL federation SDL: type `{name}` is not present in the derived schema"
                    ));
                }
                if !declared_types.insert(name.clone()) {
                    return Err(format!(
                        "GraphQL federation SDL: duplicate type definition `{name}`"
                    ));
                }
                let meta = entities.get_mut(&name).ok_or_else(|| {
                    format!("GraphQL federation SDL: entity type `{name}` is unavailable")
                })?;
                let mut seen_keys = HashSet::new();
                let mut seen_type_directives = HashSet::new();
                let directive_text = header
                    .strip_prefix(name.as_str())
                    .ok_or_else(|| "GraphQL federation SDL: malformed type header".to_string())?;
                for d in scan_directives(directive_text)? {
                    match d.name.as_str() {
                        "key" => {
                            if d.from.is_some() {
                                return Err("GraphQL federation SDL: @key does not accept `from`"
                                    .to_string());
                            }
                            let fields = d.fields.ok_or_else(|| {
                                "GraphQL federation SDL: @key requires `fields`".to_string()
                            })?;
                            validate_key_fields(&self.base, &name, &fields)?;
                            let resolvable = d.resolvable.unwrap_or(true);
                            if !seen_keys.insert(fields.clone()) {
                                return Err(format!(
                                    "GraphQL federation SDL: duplicate @key `{fields}` on `{name}`"
                                ));
                            }
                            if let Some(existing) =
                                meta.keys.iter().find(|key| key.fields == fields)
                            {
                                if existing.resolvable != resolvable {
                                    return Err(format!(
                                        "GraphQL federation SDL: conflicting @key `{fields}` on `{name}`"
                                    ));
                                }
                            } else {
                                meta.keys.push(KeyDirective { fields, resolvable });
                            }
                        }
                        "shareable" => {
                            reject_directive_arguments(&d)?;
                            if !seen_type_directives.insert(d.name) {
                                return Err(format!(
                                    "GraphQL federation SDL: duplicate @shareable on `{name}`"
                                ));
                            }
                            meta.shareable = true;
                        }
                        other => {
                            return Err(format!(
                                "GraphQL federation SDL: @{other} is not valid on a type"
                            ));
                        }
                    }
                }
                current = Some(name);
                continue;
            }
            if line.starts_with('}') {
                if current.is_none() || line != "}" {
                    return Err("GraphQL federation SDL: unmatched type terminator".to_string());
                }
                current = None;
                continue;
            }
            if let Some(tname) = &current {
                let (field_header, field_body) = line.split_once(':').ok_or_else(|| {
                    format!("GraphQL federation SDL: malformed field in `{tname}`")
                })?;
                let fname: String = field_header
                    .split(|c: char| c == '(' || c.is_whitespace())
                    .next()
                    .unwrap_or("")
                    .to_string();
                validate_field_name(&fname)?;
                if field_header.trim() != fname {
                    return Err(format!(
                        "GraphQL federation SDL: field arguments are not supported on `{tname}.{fname}`"
                    ));
                }
                let known = fname == "id"
                    || self.base.types[tname].scalar_fields.contains(&fname)
                    || self.base.types[tname].edge_fields.contains(&fname);
                if !known {
                    return Err(format!(
                        "GraphQL federation SDL: field `{tname}.{fname}` is not present in the derived schema"
                    ));
                }
                let directive_offset = field_body.find('@').unwrap_or(field_body.len());
                let declared_type = field_body[..directive_offset].trim();
                let directive_text = &field_body[directive_offset..];
                let expected_type = if fname == "id" {
                    "ID!"
                } else if self.base.types[tname].scalar_fields.contains(&fname) {
                    "String"
                } else {
                    "[Node]"
                };
                if declared_type != expected_type {
                    return Err(format!(
                        "GraphQL federation SDL: `{tname}.{fname}` declares `{declared_type}`, expected `{expected_type}`"
                    ));
                }
                let field_key = (tname.clone(), fname.clone());
                if !declared_fields.insert(field_key.clone()) {
                    return Err(format!(
                        "GraphQL federation SDL: duplicate field definition `{tname}.{fname}`"
                    ));
                }
                let dirs = scan_directives(directive_text)?;
                let fm = field_meta.entry(field_key).or_default();
                let mut seen = HashSet::new();
                for d in dirs {
                    if !seen.insert(d.name.clone()) {
                        return Err(format!(
                            "GraphQL federation SDL: duplicate @{} on `{tname}.{fname}`",
                            d.name
                        ));
                    }
                    match d.name.as_str() {
                        "shareable" => {
                            reject_directive_arguments(&d)?;
                            fm.shareable = true;
                        }
                        "external" => {
                            reject_directive_arguments(&d)?;
                            fm.external = true;
                        }
                        "provides" => {
                            if d.from.is_some() || d.resolvable.is_some() {
                                return Err(
                                    "GraphQL federation SDL: @provides only accepts `fields`"
                                        .to_string(),
                                );
                            }
                            let fields = d.fields.ok_or_else(|| {
                                "GraphQL federation SDL: @provides requires `fields`".to_string()
                            })?;
                            validate_flat_field_set("@provides", &fields)?;
                            fm.provides = Some(fields);
                        }
                        "requires" => {
                            if d.from.is_some() || d.resolvable.is_some() {
                                return Err(
                                    "GraphQL federation SDL: @requires only accepts `fields`"
                                        .to_string(),
                                );
                            }
                            let fields = d.fields.ok_or_else(|| {
                                "GraphQL federation SDL: @requires requires `fields`".to_string()
                            })?;
                            validate_flat_field_set("@requires", &fields)?;
                            fm.requires = Some(fields);
                        }
                        "override" => {
                            if d.fields.is_some() || d.resolvable.is_some() {
                                return Err(
                                    "GraphQL federation SDL: @override only accepts `from`"
                                        .to_string(),
                                );
                            }
                            fm.override_from = Some(d.from.ok_or_else(|| {
                                "GraphQL federation SDL: @override requires `from`".to_string()
                            })?)
                        }
                        other => {
                            return Err(format!(
                                "GraphQL federation SDL: @{other} is not valid on a field"
                            ));
                        }
                    }
                }
            } else {
                return Err(format!(
                    "GraphQL federation SDL: unsupported top-level definition `{line}`"
                ));
            }
        }
        if let Some(name) = current {
            return Err(format!(
                "GraphQL federation SDL: type `{name}` is missing its closing brace"
            ));
        }
        self.entities = entities;
        self.field_meta = field_meta;
        self.declared_types = declared_types;
        self.declared_fields = declared_fields;
        Ok(())
    }

    /// Render the Apollo Federation v2 SUBGRAPH SDL (CONCEPT:EG-KG.query.apollo-federation-subgraph) — the exact string an
    /// Apollo router fetches via `_service.sdl` during composition. It carries the
    /// `@link(url:"…/federation/v2.x")` schema directive, the `Query` root (the derived
    /// `[Type]` fields), and each node object type as an entity with its `@key` /
    /// `@shareable` and per-field federation directives. Per spec, the composition-layer
    /// internals (`_Service`, `_Entity`, `_Any`, `Query._service`, `Query._entities`) are
    /// NOT emitted here — the router adds them.
    pub fn to_federation_sdl(&self) -> String {
        let mut out = String::new();
        out.push_str("extend schema\n  @link(url: \"");
        out.push_str(FEDERATION_LINK_URL);
        out.push_str(
            "\", import: [\"@key\", \"@shareable\", \"@external\", \"@provides\", \"@requires\", \"@override\"])\n\n",
        );

        out.push_str("type Query {\n");
        for name in self.base.types.keys() {
            out.push_str(&format!("  {name}: [{name}]\n"));
        }
        out.push_str("}\n\n");

        for (name, t) in &self.base.types {
            out.push_str(&format!("type {name}"));
            if let Some(meta) = self.entities.get(name) {
                for k in &meta.keys {
                    if k.resolvable {
                        out.push_str(&format!(" @key(fields: \"{}\")", k.fields));
                    } else {
                        out.push_str(&format!(
                            " @key(fields: \"{}\", resolvable: false)",
                            k.fields
                        ));
                    }
                }
                if meta.shareable {
                    out.push_str(" @shareable");
                }
            }
            out.push_str(" {\n");
            // A node-backed entity is always keyed on its id.
            out.push_str("  id: ID!\n");
            for f in &t.scalar_fields {
                if f == "id" {
                    continue; // already emitted as the ID! key field
                }
                out.push_str(&format!("  {f}: String{}\n", self.field_suffix(name, f)));
            }
            for e in &t.edge_fields {
                out.push_str(&format!("  {e}: [Node]{}\n", self.field_suffix(name, e)));
            }
            out.push_str("}\n\n");
        }
        out
    }

    /// The trailing federation-directive suffix for a field (e.g. ` @external @shareable`).
    fn field_suffix(&self, type_name: &str, field: &str) -> String {
        let Some(m) = self
            .field_meta
            .get(&(type_name.to_string(), field.to_string()))
        else {
            return String::new();
        };
        let mut s = String::new();
        if m.external {
            s.push_str(" @external");
        }
        if m.shareable {
            s.push_str(" @shareable");
        }
        if let Some(f) = &m.provides {
            s.push_str(&format!(" @provides(fields: \"{f}\")"));
        }
        if let Some(f) = &m.requires {
            s.push_str(&format!(" @requires(fields: \"{f}\")"));
        }
        if let Some(from) = &m.override_from {
            s.push_str(&format!(" @override(from: \"{from}\")"));
        }
        s
    }
}

// ── query dispatch (CONCEPT:EG-KG.query.apollo-federation-subgraph) ──────────────────────────────────────────────

/// Does this document select a federation meta-field (`_service` / `_entities`)? The
/// resolver dispatches such queries here instead of the node-label root path.
pub(crate) fn is_federation_query(doc: &RawDocument) -> bool {
    doc.selections.iter().any(
        |s| matches!(s, RawSelection::Field(rf) if rf.name == "_service" || rf.name == "_entities"),
    )
}

/// Resolve a federation query (CONCEPT:EG-KG.query.apollo-federation-subgraph) over `view`, returning the GraphQL-shaped
/// `{"data": …}` JSON. Handles `_service { sdl }`, `_entities(representations:) { … }`, the
/// `__typename` root meta-field, and passes any ordinary node-label root field through the
/// standard resolver — so a router's composition (`_service`) and entity-fetch
/// (`_entities`) queries both return correct results.
pub(crate) fn resolve(
    view: &GraphView,
    doc: &RawDocument,
    vars: &Variables,
) -> Result<Value, String> {
    let fed = FederatedSchema::from_view(view)?;
    let frags: HashMap<&str, &Fragment> =
        doc.fragments.iter().map(|f| (f.name.as_str(), f)).collect();
    let mut data = Map::new();
    for sel in &doc.selections {
        let RawSelection::Field(rf) = sel else {
            return Err(
                "GraphQL federation: a top-level fragment spread is not supported in a \
                 federation query (select `_service`/`_entities`/a root type directly)"
                    .to_string(),
            );
        };
        match rf.name.as_str() {
            "_service" => {
                data.insert(rf.alias.clone(), resolve_service(&fed, rf));
            }
            "_entities" => {
                data.insert(
                    rf.alias.clone(),
                    resolve_entities(view, &fed, rf, &frags, vars)?,
                );
            }
            "__typename" => {
                data.insert(rf.alias.clone(), Value::String("Query".to_string()));
            }
            other => {
                if !fed.base.has_type(other) {
                    return Err(format!(
                        "GraphQL: no node type `{other}` in the graph (root fields must be \
                         node labels, `_service`, or `_entities`)"
                    ));
                }
                let mut active = HashSet::new();
                let fields = crate::resolver::flatten_selections(
                    std::slice::from_ref(sel),
                    &frags,
                    vars,
                    &mut active,
                )?;
                if let Some(f) = fields.first() {
                    data.insert(rf.alias.clone(), crate::resolver::resolve_root(view, f)?);
                }
            }
        }
    }
    Ok(Value::Object(
        [("data".to_string(), Value::Object(data))]
            .into_iter()
            .collect(),
    ))
}

/// Resolve `_service { sdl }` (CONCEPT:EG-KG.query.apollo-federation-subgraph): the `sdl` field returns the subgraph SDL.
fn resolve_service(fed: &FederatedSchema, rf: &RawField) -> Value {
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
fn resolve_entities(
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
fn lookup_entity(
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
        match item {
            RawSelection::Field(rf) => {
                if !crate::resolver::should_include(&rf.directives, vars)? {
                    continue;
                }
                out.push(Field {
                    alias: rf.alias.clone(),
                    name: rf.name.clone(),
                    args: crate::resolver::subst_args(&rf.args, vars),
                    selection: flatten_typed(&rf.selections, frags, vars, typename)?,
                });
            }
            RawSelection::Spread { name, directives } => {
                if !crate::resolver::should_include(directives, vars)? {
                    continue;
                }
                let frag = frags
                    .get(name.as_str())
                    .ok_or_else(|| format!("GraphQL: unknown fragment `...{name}`"))?;
                if frag.type_cond.is_empty() || frag.type_cond == typename {
                    out.extend(flatten_typed(&frag.selections, frags, vars, typename)?);
                }
            }
            RawSelection::Inline {
                type_cond,
                directives,
                selections,
            } => {
                if !crate::resolver::should_include(directives, vars)? {
                    continue;
                }
                if type_cond.as_deref().is_none_or(|tc| tc == typename) {
                    out.extend(flatten_typed(selections, frags, vars, typename)?);
                }
            }
        }
    }
    Ok(out)
}

// ── directive scanning (CONCEPT:EG-KG.query.apollo-federation-subgraph) ──────────────────────────────────────────

/// A federation directive scanned out of an SDL segment.
struct ScannedDir {
    name: String,
    /// The `fields:` argument (for `@key`/`@provides`/`@requires`).
    fields: Option<String>,
    /// The `from:` argument (for `@override`).
    from: Option<String>,
    /// The `resolvable:` argument (for `@key`).
    resolvable: Option<bool>,
}

fn reject_directive_arguments(directive: &ScannedDir) -> Result<(), String> {
    if directive.fields.is_some() || directive.from.is_some() || directive.resolvable.is_some() {
        return Err(format!(
            "GraphQL federation SDL: @{} does not accept arguments",
            directive.name
        ));
    }
    Ok(())
}

fn flat_field_names<'a>(directive: &str, fields: &'a str) -> Result<Vec<&'a str>, String> {
    let names: Vec<_> = fields.split_ascii_whitespace().collect();
    if names.is_empty() {
        return Err(format!(
            "GraphQL federation SDL: {directive} field set must not be empty"
        ));
    }
    let mut unique = HashSet::new();
    for name in &names {
        validate_field_name(name).map_err(|_| {
            format!("GraphQL federation SDL: {directive} only supports flat GraphQL field names")
        })?;
        if !unique.insert(*name) {
            return Err(format!(
                "GraphQL federation SDL: {directive} repeats field `{name}`"
            ));
        }
    }
    Ok(names)
}

fn validate_flat_field_set(directive: &str, fields: &str) -> Result<(), String> {
    flat_field_names(directive, fields).map(|_| ())
}

fn validate_key_fields(base: &Schema, type_name: &str, fields: &str) -> Result<(), String> {
    let names = flat_field_names("@key", fields)?;
    let object = base
        .types
        .get(type_name)
        .ok_or_else(|| format!("GraphQL federation SDL: unknown key type `{type_name}`"))?;
    for name in names {
        if name != "id" && !object.scalar_fields.contains(name) {
            return Err(format!(
                "GraphQL federation SDL: @key field `{type_name}.{name}` is not a discovered scalar field"
            ));
        }
    }
    Ok(())
}

/// Scan `@name(args)` federation directives out of one SDL text segment (a type header or
/// a field line). String args are captured for `fields:`/`from:`; the `@key`
/// `resolvable:` flag is read as a bare `true`/`false`.
fn scan_directives(seg: &str) -> Result<Vec<ScannedDir>, String> {
    let bytes = seg.as_bytes();
    let mut out = Vec::new();
    let mut i = 0;
    while i < bytes.len() {
        while i < bytes.len() && bytes[i].is_ascii_whitespace() {
            i += 1;
        }
        if i == bytes.len() {
            break;
        }
        if bytes[i] != b'@' {
            return Err(format!(
                "GraphQL federation SDL: unexpected text in directive list `{}`",
                &seg[i..]
            ));
        }
        let start = i + 1;
        let mut j = start;
        while j < bytes.len() && (bytes[j].is_ascii_alphanumeric() || bytes[j] == b'_') {
            j += 1;
        }
        let name = seg[start..j].to_string();
        if name.is_empty()
            || !bytes[start].is_ascii_alphabetic() && bytes[start] != b'_'
            || name.starts_with("__")
        {
            return Err("GraphQL federation SDL: malformed directive name".to_string());
        }
        let mut fields = None;
        let mut from = None;
        let mut resolvable = None;
        let mut k = j;
        while k < bytes.len() && bytes[k].is_ascii_whitespace() {
            k += 1;
        }
        if k < bytes.len() && bytes[k] == b'(' {
            let mut cursor = k + 1;
            let mut quoted = false;
            let mut escaped = false;
            while cursor < bytes.len() {
                match bytes[cursor] {
                    b'\\' if quoted => escaped = !escaped,
                    b'"' if !escaped => quoted = !quoted,
                    b')' if !quoted => break,
                    _ => escaped = false,
                }
                cursor += 1;
            }
            if cursor == bytes.len() || quoted {
                return Err(format!(
                    "GraphQL federation SDL: unterminated @{name} arguments"
                ));
            }
            let raw_args = &seg[k + 1..cursor];
            if raw_args.trim().is_empty() {
                return Err(format!(
                    "GraphQL federation SDL: @{name} has an empty argument list"
                ));
            }
            let mut args = parse_directive_args(raw_args)?;
            fields = args
                .remove("fields")
                .map(|value| parse_quoted_arg("fields", &value))
                .transpose()?;
            from = args
                .remove("from")
                .map(|value| parse_quoted_arg("from", &value))
                .transpose()?;
            resolvable =
                args.remove("resolvable")
                    .map(|value| match value.as_str() {
                        "true" => Ok(true),
                        "false" => Ok(false),
                        _ => Err("GraphQL federation SDL: `resolvable` must be true or false"
                            .to_string()),
                    })
                    .transpose()?;
            if let Some(unexpected) = args.keys().next() {
                return Err(format!(
                    "GraphQL federation SDL: unsupported directive argument `{unexpected}`"
                ));
            }
            j = cursor + 1;
        }
        out.push(ScannedDir {
            name,
            fields,
            from,
            resolvable,
        });
        i = j;
    }
    Ok(out)
}

fn parse_directive_args(args: &str) -> Result<BTreeMap<String, String>, String> {
    let mut parsed = BTreeMap::new();
    let mut start = 0usize;
    let mut quoted = false;
    let mut escaped = false;
    for (index, byte) in args.bytes().enumerate() {
        match byte {
            b'\\' if quoted => escaped = !escaped,
            b'"' if !escaped => quoted = !quoted,
            b',' if !quoted => {
                insert_directive_arg(&mut parsed, &args[start..index])?;
                start = index + 1;
            }
            _ => escaped = false,
        }
    }
    if quoted {
        return Err("GraphQL federation SDL: unterminated quoted argument".to_string());
    }
    if !args.trim().is_empty() {
        insert_directive_arg(&mut parsed, &args[start..])?;
    }
    Ok(parsed)
}

fn insert_directive_arg(parsed: &mut BTreeMap<String, String>, raw: &str) -> Result<(), String> {
    let (key, value) = raw
        .split_once(':')
        .ok_or_else(|| "GraphQL federation SDL: malformed directive argument".to_string())?;
    let key = key.trim();
    let value = value.trim();
    if key.is_empty() || value.is_empty() {
        return Err("GraphQL federation SDL: empty directive argument".to_string());
    }
    if parsed.insert(key.to_string(), value.to_string()).is_some() {
        return Err(format!(
            "GraphQL federation SDL: duplicate directive argument `{key}`"
        ));
    }
    Ok(())
}

fn parse_quoted_arg(key: &str, value: &str) -> Result<String, String> {
    if value.len() < 2
        || !value.starts_with('"')
        || !value.ends_with('"')
        || value[1..value.len() - 1]
            .chars()
            .any(|ch| matches!(ch, '"' | '\\'))
    {
        return Err(format!(
            "GraphQL federation SDL: `{key}` must be one unescaped quoted string"
        ));
    }
    let value = &value[1..value.len() - 1];
    if value.trim().is_empty() {
        return Err(format!("GraphQL federation SDL: `{key}` must not be empty"));
    }
    Ok(value.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::parse_raw;
    use crate::resolver::execute;
    use eg_core::graph::GraphCore;
    use serde_json::json;

    fn pbytes(v: Value) -> Vec<u8> {
        rmp_serde::to_vec_named(&v).unwrap()
    }

    /// alice/bob/carol People (+ a Doc); alice-KNOWS->bob — same shape as the crate's
    /// query fixtures, so federation resolves over the SAME substrate.
    fn view() -> eg_core::graph::GraphView {
        let core = GraphCore::new();
        core.add_node(
            "alice".into(),
            pbytes(json!({"type":"Person","name":"Alice","age":30,"email":"a@x.io"})),
        );
        core.add_node(
            "bob".into(),
            pbytes(json!({"type":"Person","name":"Bob","age":25,"email":"b@x.io"})),
        );
        core.add_node("d1".into(), pbytes(json!({"type":"Doc","title":"Graphs"})));
        core.add_edge(
            "alice".into(),
            "bob".into(),
            pbytes(json!({"relationship":"KNOWS"})),
        )
        .unwrap();
        core.analysis_snapshot()
    }

    #[allow(dead_code)]
    fn empty_vars() -> Variables {
        Variables::new()
    }

    /// CONCEPT:EG-KG.query.apollo-federation-subgraph — `_service.sdl` is a valid federation v2 subgraph SDL: it carries
    /// the `@link` to the federation spec and marks the derived node types as `@key`'d
    /// entities.
    #[test]
    fn federation_service_sdl_has_link_and_key_eg295() {
        let v = view();
        let res = execute(&v, "{ _service { sdl } }").unwrap();
        let sdl = res["data"]["_service"]["sdl"].as_str().unwrap();
        assert!(
            sdl.contains("@link(url: \"https://specs.apollo.dev/federation/v2"),
            "SDL must @link the federation spec, got:\n{sdl}"
        );
        assert!(
            sdl.contains("type Person @key(fields: \"id\")"),
            "Person must be a @key'd entity, got:\n{sdl}"
        );
        assert!(
            sdl.contains("type Doc @key(fields: \"id\")"),
            "Doc must be a @key'd entity, got:\n{sdl}"
        );
        // The composition-layer internals are NOT part of the returned subgraph SDL.
        assert!(!sdl.contains("_Service"), "sdl must not emit _Service");
        assert!(!sdl.contains("_entities"), "sdl must not emit _entities");
    }

    /// CONCEPT:EG-KG.query.apollo-federation-subgraph — `_entities` resolves a `{__typename,id}` representation to the
    /// right node, materializing the requested (typed) selection.
    #[test]
    fn federation_entities_resolves_representation_eg295() {
        let v = view();
        let q = r#"query($reps: [_Any!]!) {
            _entities(representations: $reps) {
                __typename
                ... on Person { name age KNOWS { name } }
            }
        }"#;
        let vars = json!({ "reps": [
            { "__typename": "Person", "id": "alice" },
            { "__typename": "Doc", "id": "d1" }
        ]});
        let res = crate::resolver::execute_with_variables(&v, q, &vars).unwrap();
        let ents = res["data"]["_entities"].as_array().unwrap();
        assert_eq!(ents.len(), 2, "one entity per representation");
        // alice → Person with the typed fields + the KNOWS traversal.
        assert_eq!(ents[0]["__typename"], json!("Person"));
        assert_eq!(ents[0]["name"], json!("Alice"));
        assert_eq!(ents[0]["age"], json!(30));
        assert_eq!(ents[0]["KNOWS"][0]["name"], json!("Bob"));
        // d1 → the Doc entity; the Person inline fragment did NOT apply.
        assert_eq!(ents[1]["__typename"], json!("Doc"));
        assert!(ents[1].get("name").is_none());
    }

    /// CONCEPT:EG-KG.query.apollo-federation-subgraph — a representation whose id has no matching node resolves to `null`,
    /// keeping the `_entities` result aligned with the input list.
    #[test]
    fn federation_entities_unknown_ref_is_null_eg295() {
        let v = view();
        let q = r#"query($reps: [_Any!]!) {
            _entities(representations: $reps) { __typename ... on Person { name } }
        }"#;
        let vars = json!({ "reps": [ { "__typename": "Person", "id": "ghost" } ] });
        let res = crate::resolver::execute_with_variables(&v, q, &vars).unwrap();
        assert_eq!(res["data"]["_entities"][0], Value::Null);
    }

    /// CONCEPT:EG-KG.query.apollo-federation-subgraph — an entity keyed by a PROPERTY (`@key(fields:"email")`, not id) is
    /// resolved by scanning nodes of the type; proves the general (non-id) key path.
    #[test]
    fn federation_entities_by_property_key_eg295() {
        let v = view();
        let mut fed = FederatedSchema::from_view(&v).unwrap();
        // Override Person's key to the `email` property.
        fed.entities.insert(
            "Person".to_string(),
            EntityMeta {
                keys: vec![KeyDirective {
                    fields: "email".to_string(),
                    resolvable: true,
                }],
                shareable: false,
            },
        );
        let repr: Map<String, Value> = json!({ "__typename": "Person", "email": "b@x.io" })
            .as_object()
            .unwrap()
            .clone();
        let (id, val) = lookup_entity(&v, &fed, "Person", &repr).unwrap().unwrap();
        assert_eq!(id, "bob");
        assert_eq!(val["name"], json!("Bob"));
    }

    /// CONCEPT:EG-KG.query.apollo-federation-subgraph — `@shareable` (type + field) and `@external` PARSE from an SDL
    /// fragment and re-EMIT in the federation SDL (the parse+emit round-trip).
    #[test]
    fn federation_shareable_and_external_parse_and_emit_eg295() {
        let v = view();
        let mut fed = FederatedSchema::from_view(&v).unwrap();
        fed.parse_directives(
            r#"
            type Person @key(fields: "id") @shareable {
              name: String @shareable
              email: String @external
            }
            "#,
        )
        .unwrap();
        // parsed:
        assert!(fed.entities["Person"].shareable, "type @shareable parsed");
        assert!(
            fed.field_meta[&("Person".to_string(), "name".to_string())].shareable,
            "field @shareable parsed"
        );
        assert!(
            fed.field_meta[&("Person".to_string(), "email".to_string())].external,
            "field @external parsed"
        );
        // emitted:
        let sdl = fed.to_federation_sdl();
        assert!(sdl.contains("type Person @key(fields: \"id\") @shareable"));
        assert!(sdl.contains("name: String @shareable"));
        assert!(sdl.contains("email: String @external"));
    }

    /// CONCEPT:EG-KG.query.apollo-federation-subgraph — `@provides`/`@requires`/`@override` parse their args and re-emit.
    #[test]
    fn federation_provides_requires_override_round_trip_eg295() {
        let v = view();
        let mut fed = FederatedSchema::from_view(&v).unwrap();
        fed.parse_directives(
            r#"
            type Person @key(fields: "id") {
              name: String @provides(fields: "age") @requires(fields: "email")
              age: String @override(from: "catalog")
            }
            "#,
        )
        .unwrap();
        let name_meta = &fed.field_meta[&("Person".to_string(), "name".to_string())];
        assert_eq!(name_meta.provides.as_deref(), Some("age"));
        assert_eq!(name_meta.requires.as_deref(), Some("email"));
        let age_meta = &fed.field_meta[&("Person".to_string(), "age".to_string())];
        assert_eq!(age_meta.override_from.as_deref(), Some("catalog"));
        let sdl = fed.to_federation_sdl();
        assert!(sdl.contains("@provides(fields: \"age\")"));
        assert!(sdl.contains("@requires(fields: \"email\")"));
        assert!(sdl.contains("@override(from: \"catalog\")"));
    }

    #[test]
    fn federation_schema_overlay_rejects_duplicate_definitions_atomically() {
        let v = view();
        let mut fed = FederatedSchema::from_view(&v).unwrap();
        let before = fed.to_federation_sdl();
        let duplicate_type = r#"
            type Person {
              name: String
            }
            type Person {
              email: String
            }
        "#;
        assert!(fed.parse_directives(duplicate_type).is_err());
        assert_eq!(fed.to_federation_sdl(), before);

        let duplicate_field = r#"
            type Person {
              name: String
              name: String @shareable
            }
        "#;
        assert!(fed.parse_directives(duplicate_field).is_err());
        assert_eq!(fed.to_federation_sdl(), before);
    }

    #[test]
    fn federation_schema_overlay_rejects_ambiguous_or_malformed_directives() {
        let v = view();
        let mut fed = FederatedSchema::from_view(&v).unwrap();
        assert!(fed
            .parse_directives(
                r#"type Person @key(fields: "id", fields: "email") {
                    name: String
                }"#,
            )
            .is_err());
        assert!(fed
            .parse_directives(
                r#"type Person {
                    name: String @override(from: "one") @override(from: "two")
                }"#,
            )
            .is_err());
        assert!(fed
            .parse_directives(
                r#"type Person @key(fields: "id") {
                    name: String
                "#,
            )
            .is_err());
        assert!(fed
            .parse_directives(
                r#"type Person nonsense {
                    name: String
                }"#,
            )
            .is_err());
        assert!(fed
            .parse_directives(
                r#"type Person {
                    name: Int
                }"#,
            )
            .is_err());
        assert!(fed
            .parse_directives(
                r#"type Person @key(fields: "email { name }") {
                    name: String
                }"#,
            )
            .is_err());
        assert!(fed.parse_directives("scalar Unsupported").is_err());
    }

    #[test]
    fn federation_does_not_resolve_reference_only_keys() {
        let v = view();
        let mut fed = FederatedSchema::from_view(&v).unwrap();
        fed.entities.insert(
            "Person".to_string(),
            EntityMeta {
                keys: vec![KeyDirective {
                    fields: "email".to_string(),
                    resolvable: false,
                }],
                shareable: false,
            },
        );
        let repr = json!({ "__typename": "Person", "email": "b@x.io" })
            .as_object()
            .unwrap()
            .clone();
        assert!(lookup_entity(&v, &fed, "Person", &repr).unwrap().is_none());
    }

    #[test]
    fn federation_schema_overlay_cannot_be_redefined_across_calls() {
        let v = view();
        let mut fed = FederatedSchema::from_view(&v).unwrap();
        fed.parse_directives(
            r#"type Person @key(fields: "id") {
                name: String @shareable
            }"#,
        )
        .unwrap();
        assert!(fed
            .parse_directives(
                r#"type Person @key(fields: "email") {
                    email: String @external
                }"#,
            )
            .is_err());
    }

    /// CONCEPT:EG-KG.query.apollo-federation-subgraph — the federation dispatcher only fires for `_service`/`_entities`;
    /// an ordinary query is untouched (regression guard).
    #[test]
    fn federation_query_detection_eg295() {
        assert!(is_federation_query(
            &parse_raw("{ _service { sdl } }").unwrap()
        ));
        assert!(is_federation_query(
            &parse_raw(r#"{ _entities(representations: []) { __typename } }"#).unwrap()
        ));
        assert!(!is_federation_query(
            &parse_raw("{ Person { name } }").unwrap()
        ));
    }

    /// CONCEPT:EG-KG.query.apollo-federation-subgraph — an ordinary node query still resolves correctly even when routed
    /// through the federation-aware entry point (a normal root field mixed alongside is
    /// passed to the standard resolver).
    #[test]
    fn federation_passes_through_normal_root_eg295() {
        let v = view();
        // A doc that selects BOTH _service and a normal root type.
        let res = execute(&v, "{ _service { sdl } Person { name } }").unwrap();
        assert!(res["data"]["_service"]["sdl"].is_string());
        let names: Vec<&str> = res["data"]["Person"]
            .as_array()
            .unwrap()
            .iter()
            .map(|p| p["name"].as_str().unwrap())
            .collect();
        assert!(names.contains(&"Alice") && names.contains(&"Bob"));
    }
}
