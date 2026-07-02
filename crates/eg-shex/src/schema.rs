//! The ShEx schema model + a ShExJ (JSON abstract-syntax) parser (CONCEPT:EG-133).
//!
//! ShEx has two concrete syntaxes: **ShExC** (a compact, Turtle-like grammar) and
//! **ShExJ** (the JSON abstract syntax). ShExJ is the canonical, unambiguous form and is
//! far more robust to parse than the compact grammar, so this crate parses **ShExJ**.
//! ShExC is a documented follow-up (see the crate docs). The parser walks a
//! `serde_json::Value` by hand — this keeps the reference-vs-inline (`shapeExpr` is either
//! an IRI string label OR an inline object) polymorphism simple and explicit.
//!
//! The model captures ShEx Core: node constraints (`nodeKind`, `datatype`, string facets
//! `length`/`minlength`/`maxlength`/`pattern`, numeric facets `mininclusive`/… , value
//! sets), triple constraints (`predicate` + `valueExpr` + cardinality `min`/`max`), the
//! `EachOf`/`OneOf` triple-expression combinators and the `ShapeAnd`/`ShapeOr`/`ShapeNot`
//! shape-expression combinators, shape references, and `closed` shapes with `extra`.

use std::collections::HashMap;

use serde_json::Value;

/// A shape label — an IRI (or the sentinel [`START`] for the schema's start shape).
pub type ShapeLabel = String;

/// The sentinel label for a schema's `start` shape expression in a shape map.
pub const START: &str = "START";

/// ShEx node kinds (`nodeKind`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeKind {
    Iri,
    BNode,
    Literal,
    /// `nonliteral` — an IRI or a blank node.
    NonLiteral,
}

/// A member of a `values` value set. Stems / language-stem ranges are an EG-133 follow-up.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ValueSetValue {
    /// A plain IRI value.
    Iri(String),
    /// A literal value, optionally typed and/or language-tagged.
    Literal {
        value: String,
        datatype: Option<String>,
        language: Option<String>,
    },
}

/// String facets of a node constraint.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StringFacets {
    pub length: Option<usize>,
    pub minlength: Option<usize>,
    pub maxlength: Option<usize>,
    pub pattern: Option<String>,
    pub flags: Option<String>,
}

impl StringFacets {
    fn is_empty(&self) -> bool {
        self.length.is_none()
            && self.minlength.is_none()
            && self.maxlength.is_none()
            && self.pattern.is_none()
    }
}

/// Numeric facets of a node constraint (compared over the value node's lexical form
/// parsed as `f64`).
#[derive(Debug, Clone, Default, PartialEq)]
pub struct NumericFacets {
    pub mininclusive: Option<f64>,
    pub maxinclusive: Option<f64>,
    pub minexclusive: Option<f64>,
    pub maxexclusive: Option<f64>,
}

impl NumericFacets {
    fn is_empty(&self) -> bool {
        self.mininclusive.is_none()
            && self.maxinclusive.is_none()
            && self.minexclusive.is_none()
            && self.maxexclusive.is_none()
    }
}

/// A ShEx `NodeConstraint` — constraints on the value node itself (not its arcs).
#[derive(Debug, Clone, Default, PartialEq)]
pub struct NodeConstraint {
    pub node_kind: Option<NodeKind>,
    pub datatype: Option<String>,
    pub values: Option<Vec<ValueSetValue>>,
    pub string_facets: StringFacets,
    pub numeric_facets: NumericFacets,
}

/// A ShEx triple expression — the arc pattern a `Shape` matches against a node's
/// neighborhood.
#[derive(Debug, Clone)]
pub enum TripleExpr {
    /// A single `predicate` with a value shape and cardinality `[min, max]`
    /// (`max == -1` ⇒ unbounded).
    TripleConstraint {
        predicate: String,
        value_expr: Option<Box<ShapeExpr>>,
        min: i64,
        max: i64,
        /// `inverse` (`^predicate`) — an EG-133 follow-up (see crate docs); parsed but the
        /// validator evaluates outgoing arcs only.
        inverse: bool,
    },
    /// `EachOf` (`;`) — every sub-expression must match (a conjunction over arcs).
    EachOf(Vec<TripleExpr>),
    /// `OneOf` (`|`) — exactly one branch matches the partition (alternation).
    OneOf(Vec<TripleExpr>),
}

/// A ShEx `Shape` — a triple-expression pattern over a node's neighborhood, optionally
/// `closed` (rejecting arcs on predicates not mentioned / not in `extra`).
#[derive(Debug, Clone)]
pub struct Shape {
    pub expression: Option<TripleExpr>,
    pub closed: bool,
    /// `extra` predicates — allowed to carry additional arcs even on a closed shape.
    pub extra: Vec<String>,
}

/// A ShEx shape expression — the thing a node is tested against.
#[derive(Debug, Clone)]
pub enum ShapeExpr {
    NodeConstraint(NodeConstraint),
    Shape(Shape),
    /// `ShapeAnd` — the node must satisfy every sub-expression.
    And(Vec<ShapeExpr>),
    /// `ShapeOr` — the node must satisfy at least one sub-expression.
    Or(Vec<ShapeExpr>),
    /// `ShapeNot` — the node must NOT satisfy the sub-expression.
    Not(Box<ShapeExpr>),
    /// A reference to another shape by its label (IRI).
    Ref(ShapeLabel),
}

/// A parsed ShEx schema — labelled shape expressions plus an optional `start`.
#[derive(Debug, Clone, Default)]
pub struct Schema {
    pub shapes: HashMap<ShapeLabel, ShapeExpr>,
    pub start: Option<ShapeExpr>,
}

impl Schema {
    /// Resolve a shape label to its expression (`START` ⇒ the start shape).
    pub fn resolve(&self, label: &str) -> Option<&ShapeExpr> {
        if label == START {
            self.start.as_ref()
        } else {
            self.shapes.get(label)
        }
    }

    /// Parse a ShExJ (JSON abstract-syntax) schema document (CONCEPT:EG-133).
    pub fn from_shexj(json: &str) -> Result<Schema, String> {
        let root: Value =
            serde_json::from_str(json).map_err(|e| format!("ShExJ: invalid JSON: {e}"))?;
        let mut schema = Schema::default();

        if let Some(arr) = root.get("shapes").and_then(Value::as_array) {
            for item in arr {
                // A `shapes` member is a shape expression carrying an `id` (its label).
                let id = item
                    .get("id")
                    .and_then(Value::as_str)
                    .ok_or_else(|| "ShExJ: a `shapes` entry is missing its `id`".to_string())?;
                let expr = parse_shape_expr(item)?;
                schema.shapes.insert(id.to_string(), expr);
            }
        }
        if let Some(start) = root.get("start") {
            schema.start = Some(parse_shape_expr(start)?);
        }
        Ok(schema)
    }
}

// ── ShExJ Value → model ──────────────────────────────────────────────────────

fn type_of(v: &Value) -> Option<&str> {
    v.get("type").and_then(Value::as_str)
}

/// Parse a `shapeExpr` — either an inline object (tagged by `type`) or a bare IRI-string
/// reference to another shape.
fn parse_shape_expr(v: &Value) -> Result<ShapeExpr, String> {
    if let Some(s) = v.as_str() {
        return Ok(ShapeExpr::Ref(s.to_string()));
    }
    match type_of(v) {
        Some("NodeConstraint") => Ok(ShapeExpr::NodeConstraint(parse_node_constraint(v)?)),
        Some("Shape") => Ok(ShapeExpr::Shape(parse_shape(v)?)),
        Some("ShapeAnd") => Ok(ShapeExpr::And(parse_shape_expr_list(v, "shapeExprs")?)),
        Some("ShapeOr") => Ok(ShapeExpr::Or(parse_shape_expr_list(v, "shapeExprs")?)),
        Some("ShapeNot") => {
            let inner = v
                .get("shapeExpr")
                .ok_or_else(|| "ShExJ: ShapeNot missing `shapeExpr`".to_string())?;
            Ok(ShapeExpr::Not(Box::new(parse_shape_expr(inner)?)))
        }
        // `ShapeExternal` and named-only stubs are an EG-133 follow-up.
        Some(other) => Err(format!("ShExJ: unsupported shapeExpr type `{other}`")),
        None => Err("ShExJ: shapeExpr is neither a string ref nor a typed object".into()),
    }
}

fn parse_shape_expr_list(v: &Value, key: &str) -> Result<Vec<ShapeExpr>, String> {
    let arr = v
        .get(key)
        .and_then(Value::as_array)
        .ok_or_else(|| format!("ShExJ: expected array `{key}`"))?;
    arr.iter().map(parse_shape_expr).collect()
}

fn parse_shape(v: &Value) -> Result<Shape, String> {
    let closed = v.get("closed").and_then(Value::as_bool).unwrap_or(false);
    let extra = v
        .get("extra")
        .and_then(Value::as_array)
        .map(|a| {
            a.iter()
                .filter_map(Value::as_str)
                .map(str::to_string)
                .collect()
        })
        .unwrap_or_default();
    let expression = match v.get("expression") {
        Some(e) => Some(parse_triple_expr(e)?),
        None => None,
    };
    Ok(Shape {
        expression,
        closed,
        extra,
    })
}

fn parse_triple_expr(v: &Value) -> Result<TripleExpr, String> {
    // Triple-expression *references* (a bare label string) are an EG-133 follow-up.
    if v.is_string() {
        return Err("ShExJ: triple-expression references are not supported (EG-133 follow-up)".into());
    }
    match type_of(v) {
        Some("TripleConstraint") => {
            let predicate = v
                .get("predicate")
                .and_then(Value::as_str)
                .ok_or_else(|| "ShExJ: TripleConstraint missing `predicate`".to_string())?
                .to_string();
            let value_expr = match v.get("valueExpr") {
                Some(ve) => Some(Box::new(parse_shape_expr(ve)?)),
                None => None,
            };
            // ShEx cardinality defaults are {1,1}; max == -1 means unbounded (`*`/`+`).
            let min = v.get("min").and_then(Value::as_i64).unwrap_or(1);
            let max = v.get("max").and_then(Value::as_i64).unwrap_or(1);
            let inverse = v.get("inverse").and_then(Value::as_bool).unwrap_or(false);
            Ok(TripleExpr::TripleConstraint {
                predicate,
                value_expr,
                min,
                max,
                inverse,
            })
        }
        Some("EachOf") => Ok(TripleExpr::EachOf(parse_triple_expr_list(v)?)),
        Some("OneOf") => Ok(TripleExpr::OneOf(parse_triple_expr_list(v)?)),
        Some(other) => Err(format!("ShExJ: unsupported tripleExpr type `{other}`")),
        None => Err("ShExJ: tripleExpr missing `type`".into()),
    }
}

fn parse_triple_expr_list(v: &Value) -> Result<Vec<TripleExpr>, String> {
    let arr = v
        .get("expressions")
        .and_then(Value::as_array)
        .ok_or_else(|| "ShExJ: EachOf/OneOf missing `expressions`".to_string())?;
    arr.iter().map(parse_triple_expr).collect()
}

fn parse_node_constraint(v: &Value) -> Result<NodeConstraint, String> {
    let node_kind = match v.get("nodeKind").and_then(Value::as_str) {
        Some("iri") => Some(NodeKind::Iri),
        Some("bnode") => Some(NodeKind::BNode),
        Some("literal") => Some(NodeKind::Literal),
        Some("nonliteral") => Some(NodeKind::NonLiteral),
        Some(other) => return Err(format!("ShExJ: unknown nodeKind `{other}`")),
        None => None,
    };
    let datatype = v
        .get("datatype")
        .and_then(Value::as_str)
        .map(str::to_string);

    let values = match v.get("values").and_then(Value::as_array) {
        Some(arr) => Some(arr.iter().map(parse_value_set_value).collect::<Result<_, _>>()?),
        None => None,
    };

    let string_facets = StringFacets {
        length: usize_field(v, "length"),
        minlength: usize_field(v, "minlength"),
        maxlength: usize_field(v, "maxlength"),
        pattern: v.get("pattern").and_then(Value::as_str).map(str::to_string),
        flags: v.get("flags").and_then(Value::as_str).map(str::to_string),
    };
    let numeric_facets = NumericFacets {
        mininclusive: f64_field(v, "mininclusive"),
        maxinclusive: f64_field(v, "maxinclusive"),
        minexclusive: f64_field(v, "minexclusive"),
        maxexclusive: f64_field(v, "maxexclusive"),
    };
    let _ = (string_facets.is_empty(), numeric_facets.is_empty()); // helpers used in tests

    Ok(NodeConstraint {
        node_kind,
        datatype,
        values,
        string_facets,
        numeric_facets,
    })
}

fn parse_value_set_value(v: &Value) -> Result<ValueSetValue, String> {
    // A value-set member is either a bare IRI string or an object literal
    // ({ "value": ..., "type"?: <dt>, "language"?: <tag> }). Stem ranges are deferred.
    if let Some(s) = v.as_str() {
        return Ok(ValueSetValue::Iri(s.to_string()));
    }
    if let Some(obj) = v.as_object() {
        let value = obj
            .get("value")
            .and_then(Value::as_str)
            .ok_or_else(|| "ShExJ: value-set object literal missing `value`".to_string())?
            .to_string();
        let datatype = obj.get("type").and_then(Value::as_str).map(str::to_string);
        let language = obj.get("language").and_then(Value::as_str).map(str::to_string);
        return Ok(ValueSetValue::Literal {
            value,
            datatype,
            language,
        });
    }
    Err("ShExJ: unsupported value-set member (stem ranges are an EG-133 follow-up)".into())
}

fn usize_field(v: &Value, key: &str) -> Option<usize> {
    v.get(key).and_then(Value::as_u64).map(|n| n as usize)
}

/// A numeric facet field — a JSON number, or a numeric string (ShExJ sometimes carries a
/// typed literal's lexical form as a string).
fn f64_field(v: &Value, key: &str) -> Option<f64> {
    match v.get(key)? {
        Value::Number(n) => n.as_f64(),
        Value::String(s) => s.trim().parse::<f64>().ok(),
        _ => None,
    }
}
