//! VRL-style ingest transform pipelines (CONCEPT:EG-165).
//!
//! OpenObserve / Vector apply a *Vector Remap Language* (VRL) program to every log
//! or event record at ingest, BEFORE it lands in a stream: parse a JSON blob into
//! fields, drop noisy records, set/rename/remove fields, coerce types, and route the
//! record to a destination stream. This module is the epistemic-graph equivalent —
//! a compact, deterministic, pure-Rust transform engine that runs over a record just
//! before it lands in an eg-tsdb series (the log-ingest front door of CONCEPT:EG-160/
//! 161, `obs` feature).
//!
//! It is *not* a full VRL parser (that is a deferred follow-up, see the module tail).
//! Instead it is a small, closed transform-op enum ([`Stage`]) plus a builder
//! ([`Pipeline`]) and a minimal one-stage-per-line textual form ([`Pipeline::parse`]).
//!
//! ## Record model
//! A record is a [`Record`] = `BTreeMap<String, PipeValue>`, mirroring the
//! field-map shape the OTLP/Elasticsearch/JSON-lines log front door hands to the
//! series landing path. [`PipeValue`] is a small JSON-ish value enum (scalars +
//! `Array`/`Object`) so `parse_json` can explode a nested blob into sub-fields. It is
//! pure-Rust + `serde` only (no `serde_json`, no Arrow/redb) — the same zero-new-dep
//! contract the `promql`/`traces` modules hold; the JSON reader is a tiny hand-rolled
//! recursive-descent parser ([`parse_json_value`]).
//!
//! ## Cross-modal enrichment — the "surpass OpenObserve" differentiator
//! OpenObserve/Vector can only enrich from static in-memory enrichment *tables*.
//! Here, the [`Stage::Enrich`] stage takes a caller-supplied [`Lookup`] closure
//! (`Fn(&PipeValue) -> Option<PipeValue>`). Because the closure is injected by the
//! caller, eg-tsdb stays fully decoupled from eg-core / the graph engine, yet a log
//! record can be enriched *live from the knowledge graph* at ingest — e.g. resolve a
//! `user_id` field to the graph's `:Person` display name, or a `service` field to its
//! `:Service` team owner. The graph read lives on the caller's side of the seam; the
//! pipeline just calls the closure. That cross-modal (log ⨯ graph) enrichment is the
//! thing OpenObserve structurally cannot do.
//!
//! ## Ingest-path seam (documented, NOT wired here)
//! The live wiring — parse an OTLP/`_bulk`/JSON-lines batch → run a per-stream
//! [`Pipeline`] → land the kept, routed records into the eg-tsdb series + Tantivy
//! index — lives in the facade's `src/server` obs listener (the `obs` feature) and is
//! a deferred follow-up (it would touch `server/*`, owned by other agents). This
//! module provides the pure engine plus [`stream_to_columnar`], the seam that turns a
//! routed record batch into the existing [`ColumnarSegment`](crate::columnar) landing
//! shape, so the follow-up is a thin call site.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::columnar::{CellValue, ColumnarSegment};

/// A small JSON-ish value carried through the pipeline (CONCEPT:EG-165). Scalars
/// mirror the columnar [`CellValue`] kinds; `Array`/`Object` let `parse_json` hold a
/// nested blob before it is exploded into sub-fields. `Object` is a `BTreeMap` so
/// field order (and therefore every transform result) is deterministic.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum PipeValue {
    Null,
    Bool(bool),
    I64(i64),
    F64(f64),
    Str(String),
    Array(Vec<PipeValue>),
    Object(BTreeMap<String, PipeValue>),
}

impl PipeValue {
    /// A string convenience constructor.
    pub fn str(s: impl Into<String>) -> Self {
        PipeValue::Str(s.into())
    }

    /// Numeric view (`I64`/`F64`/`Bool`) for `gt`/`lt` compares; `None` for
    /// non-numeric kinds (CONCEPT:EG-165).
    fn as_f64(&self) -> Option<f64> {
        match self {
            PipeValue::I64(n) => Some(*n as f64),
            PipeValue::F64(x) => Some(*x),
            PipeValue::Bool(b) => Some(if *b { 1.0 } else { 0.0 }),
            _ => None,
        }
    }

    /// String view for `contains` / string ordering, when the value is a `Str`.
    fn as_str(&self) -> Option<&str> {
        match self {
            PipeValue::Str(s) => Some(s.as_str()),
            _ => None,
        }
    }

    /// Flatten to a columnar [`CellValue`] for the series landing seam
    /// (CONCEPT:EG-165). Scalars map 1:1; `Array`/`Object`/`Null` collapse to their
    /// JSON text as a `Str` cell (so a nested blob still lands in a string column
    /// rather than being dropped). See [`stream_to_columnar`].
    pub fn to_cell(&self) -> CellValue {
        match self {
            PipeValue::Null => CellValue::Null,
            PipeValue::Bool(b) => CellValue::Bool(*b),
            PipeValue::I64(n) => CellValue::I64(*n),
            PipeValue::F64(x) => CellValue::F64(*x),
            PipeValue::Str(s) => CellValue::Str(s.clone()),
            PipeValue::Array(_) | PipeValue::Object(_) => CellValue::Str(self.to_json()),
        }
    }

    /// Render back to compact JSON text (used by `to_cell` for nested values and by
    /// `coerce`) (CONCEPT:EG-165).
    pub fn to_json(&self) -> String {
        let mut s = String::new();
        self.write_json(&mut s);
        s
    }

    fn write_json(&self, out: &mut String) {
        match self {
            PipeValue::Null => out.push_str("null"),
            PipeValue::Bool(true) => out.push_str("true"),
            PipeValue::Bool(false) => out.push_str("false"),
            PipeValue::I64(n) => out.push_str(&n.to_string()),
            PipeValue::F64(x) => out.push_str(&x.to_string()),
            PipeValue::Str(s) => write_json_str(s, out),
            PipeValue::Array(items) => {
                out.push('[');
                for (i, it) in items.iter().enumerate() {
                    if i > 0 {
                        out.push(',');
                    }
                    it.write_json(out);
                }
                out.push(']');
            }
            PipeValue::Object(map) => {
                out.push('{');
                for (i, (k, v)) in map.iter().enumerate() {
                    if i > 0 {
                        out.push(',');
                    }
                    write_json_str(k, out);
                    out.push(':');
                    v.write_json(out);
                }
                out.push('}');
            }
        }
    }
}

impl From<CellValue> for PipeValue {
    fn from(c: CellValue) -> Self {
        match c {
            CellValue::Null => PipeValue::Null,
            CellValue::Bool(b) => PipeValue::Bool(b),
            CellValue::I64(n) => PipeValue::I64(n),
            CellValue::F64(x) => PipeValue::F64(x),
            CellValue::Str(s) => PipeValue::Str(s),
        }
    }
}

/// A record flowing through the pipeline: an ordered field map (CONCEPT:EG-165).
pub type Record = BTreeMap<String, PipeValue>;

/// The scalar type a [`Stage::Coerce`] converts a field to (CONCEPT:EG-165).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum CoerceType {
    Str,
    I64,
    F64,
    Bool,
}

/// The comparison a [`Predicate`] applies to a field (CONCEPT:EG-165).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum CmpOp {
    /// Field equals the operand.
    Eq,
    /// Field is absent or differs from the operand.
    Ne,
    /// Numeric (or string) field is strictly greater than the operand.
    Gt,
    /// Numeric (or string) field is strictly less than the operand.
    Lt,
    /// String field contains the operand substring, or array field contains the value.
    Contains,
    /// Field is present (operand ignored).
    Exists,
}

/// A single field comparison used by `filter` / `drop_if` / `route`
/// (CONCEPT:EG-165). A missing field is `false` for every op except `Ne` (absent ≠
/// operand ⇒ `true`) and, of course, `Exists` (absent ⇒ `false`).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Predicate {
    pub field: String,
    pub op: CmpOp,
    /// The right-hand operand; ignored for [`CmpOp::Exists`].
    pub value: PipeValue,
}

impl Predicate {
    pub fn new(field: impl Into<String>, op: CmpOp, value: PipeValue) -> Self {
        Self {
            field: field.into(),
            op,
            value,
        }
    }

    /// Evaluate against a record (CONCEPT:EG-165). Deterministic and side-effect free.
    pub fn eval(&self, rec: &Record) -> bool {
        let present = rec.get(&self.field);
        match self.op {
            CmpOp::Exists => present.is_some(),
            CmpOp::Ne => match present {
                None => true,
                Some(v) => v != &self.value,
            },
            CmpOp::Eq => present == Some(&self.value),
            CmpOp::Gt => cmp_num_or_str(present, &self.value).map(|o| o.is_gt()).unwrap_or(false),
            CmpOp::Lt => cmp_num_or_str(present, &self.value).map(|o| o.is_lt()).unwrap_or(false),
            CmpOp::Contains => match present {
                Some(PipeValue::Str(s)) => self
                    .value
                    .as_str()
                    .map(|needle| s.contains(needle))
                    .unwrap_or(false),
                Some(PipeValue::Array(items)) => items.contains(&self.value),
                _ => false,
            },
        }
    }
}

/// Compare a present field to an operand numerically when both are numeric, else
/// lexicographically when both are strings (CONCEPT:EG-165).
fn cmp_num_or_str(present: Option<&PipeValue>, operand: &PipeValue) -> Option<std::cmp::Ordering> {
    let v = present?;
    if let (Some(a), Some(b)) = (v.as_f64(), operand.as_f64()) {
        return a.partial_cmp(&b);
    }
    if let (Some(a), Some(b)) = (v.as_str(), operand.as_str()) {
        return Some(a.cmp(b));
    }
    None
}

/// A caller-supplied enrichment lookup (CONCEPT:EG-165) — the cross-modal seam. The
/// pipeline calls this with a field's value and folds any returned value into a
/// target field, without knowing (or depending on) where the value came from. A graph
/// read, a static table, an HTTP call — all live on the caller's side.
///
/// A blanket impl covers plain closures, so a caller can write
/// `Stage::enrich("user_id", "user_name", |v| graph.name_of(v))`.
pub trait Lookup: Send + Sync {
    fn lookup(&self, value: &PipeValue) -> Option<PipeValue>;
}

impl<F> Lookup for F
where
    F: Fn(&PipeValue) -> Option<PipeValue> + Send + Sync,
{
    fn lookup(&self, value: &PipeValue) -> Option<PipeValue> {
        (self)(value)
    }
}

/// One transform op (CONCEPT:EG-165). A closed, compact set — the VRL-subset the
/// ingest path needs. Not `serde`-derivable because [`Stage::Enrich`] carries a
/// closure; the textual form ([`Pipeline::parse`]) is the serialized representation
/// for the non-enrich stages.
#[derive(Clone)]
pub enum Stage {
    /// Parse a string field as JSON. When it parses to an object, its entries are
    /// merged in as top-level sub-fields and the original field is removed; otherwise
    /// the field is replaced by the parsed value. A parse error leaves the record
    /// unchanged (deterministic no-op).
    ParseJson { field: String },
    /// Keep only records for which the predicate holds; drop the rest (standard VRL
    /// `filter` semantics — keep matching).
    Filter(Predicate),
    /// Drop records for which the predicate holds (VRL `drop_if` — remove matching).
    DropIf(Predicate),
    /// Set (insert or overwrite) a field to a constant value.
    Set { field: String, value: PipeValue },
    /// Move a field's value from `from` to `to` (no-op if `from` is absent).
    Rename { from: String, to: String },
    /// Remove a field.
    Remove { field: String },
    /// Coerce a field's value to a scalar type (best-effort; an unconvertible value is
    /// left unchanged).
    Coerce { field: String, ty: CoerceType },
    /// Tag the record's destination stream when `field` equals `value`. A later match
    /// overrides an earlier one.
    Route {
        field: String,
        value: PipeValue,
        stream: String,
    },
    /// Enrich `target` from the caller-supplied [`Lookup`] applied to `source`'s value
    /// — the cross-modal (log ⨯ graph) differentiator (CONCEPT:EG-165). If `source` is
    /// present and the lookup returns `Some`, `target` is set to it.
    Enrich {
        source: String,
        target: String,
        lookup: Arc<dyn Lookup>,
    },
}

impl fmt::Debug for Stage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Stage::ParseJson { field } => write!(f, "ParseJson({field})"),
            Stage::Filter(p) => write!(f, "Filter({p:?})"),
            Stage::DropIf(p) => write!(f, "DropIf({p:?})"),
            Stage::Set { field, value } => write!(f, "Set({field} = {value:?})"),
            Stage::Rename { from, to } => write!(f, "Rename({from} -> {to})"),
            Stage::Remove { field } => write!(f, "Remove({field})"),
            Stage::Coerce { field, ty } => write!(f, "Coerce({field} as {ty:?})"),
            Stage::Route {
                field,
                value,
                stream,
            } => write!(f, "Route({field} == {value:?} -> {stream})"),
            Stage::Enrich { source, target, .. } => write!(f, "Enrich({source} -> {target})"),
        }
    }
}

impl Stage {
    /// A convenience constructor for [`Stage::Enrich`] that boxes a closure
    /// (CONCEPT:EG-165).
    pub fn enrich<F>(source: impl Into<String>, target: impl Into<String>, lookup: F) -> Self
    where
        F: Fn(&PipeValue) -> Option<PipeValue> + Send + Sync + 'static,
    {
        Stage::Enrich {
            source: source.into(),
            target: target.into(),
            lookup: Arc::new(lookup),
        }
    }
}

/// The result of running a [`Pipeline`] over one record (CONCEPT:EG-165): the
/// transformed fields plus the routed destination stream (`None` when no `route`
/// stage matched — the caller falls back to a default stream).
#[derive(Clone, Debug, PartialEq)]
pub struct RoutedRecord {
    pub stream: Option<String>,
    pub fields: Record,
}

/// An ordered list of [`Stage`]s applied to a record in sequence (CONCEPT:EG-165).
/// Build fluently (`Pipeline::new().parse_json(..).drop_if(..)`) or from the textual
/// form ([`Pipeline::parse`]).
#[derive(Clone, Debug, Default)]
pub struct Pipeline {
    pub stages: Vec<Stage>,
}

impl Pipeline {
    /// An empty pipeline (identity transform).
    pub fn new() -> Self {
        Self { stages: Vec::new() }
    }

    /// Append a raw stage.
    pub fn push(mut self, stage: Stage) -> Self {
        self.stages.push(stage);
        self
    }

    // ---- fluent builder sugar (CONCEPT:EG-165) ----

    pub fn parse_json(self, field: impl Into<String>) -> Self {
        self.push(Stage::ParseJson {
            field: field.into(),
        })
    }
    pub fn filter(self, pred: Predicate) -> Self {
        self.push(Stage::Filter(pred))
    }
    pub fn drop_if(self, pred: Predicate) -> Self {
        self.push(Stage::DropIf(pred))
    }
    pub fn set(self, field: impl Into<String>, value: PipeValue) -> Self {
        self.push(Stage::Set {
            field: field.into(),
            value,
        })
    }
    pub fn rename(self, from: impl Into<String>, to: impl Into<String>) -> Self {
        self.push(Stage::Rename {
            from: from.into(),
            to: to.into(),
        })
    }
    pub fn remove(self, field: impl Into<String>) -> Self {
        self.push(Stage::Remove {
            field: field.into(),
        })
    }
    pub fn coerce(self, field: impl Into<String>, ty: CoerceType) -> Self {
        self.push(Stage::Coerce {
            field: field.into(),
            ty,
        })
    }
    pub fn route(
        self,
        field: impl Into<String>,
        value: PipeValue,
        stream: impl Into<String>,
    ) -> Self {
        self.push(Stage::Route {
            field: field.into(),
            value,
            stream: stream.into(),
        })
    }
    pub fn enrich<F>(self, source: impl Into<String>, target: impl Into<String>, lookup: F) -> Self
    where
        F: Fn(&PipeValue) -> Option<PipeValue> + Send + Sync + 'static,
    {
        self.push(Stage::enrich(source, target, lookup))
    }

    /// Run the pipeline over ONE record, returning the transformed + routed record, or
    /// `None` if a `filter`/`drop_if` stage dropped it (CONCEPT:EG-165). Deterministic.
    pub fn run(&self, record: Record) -> Option<RoutedRecord> {
        let mut rec = record;
        let mut stream: Option<String> = None;
        for stage in &self.stages {
            match stage {
                Stage::Filter(p) => {
                    if !p.eval(&rec) {
                        return None;
                    }
                }
                Stage::DropIf(p) => {
                    if p.eval(&rec) {
                        return None;
                    }
                }
                Stage::ParseJson { field } => apply_parse_json(&mut rec, field),
                Stage::Set { field, value } => {
                    rec.insert(field.clone(), value.clone());
                }
                Stage::Rename { from, to } => {
                    if let Some(v) = rec.remove(from) {
                        rec.insert(to.clone(), v);
                    }
                }
                Stage::Remove { field } => {
                    rec.remove(field);
                }
                Stage::Coerce { field, ty } => apply_coerce(&mut rec, field, *ty),
                Stage::Route {
                    field,
                    value,
                    stream: s,
                } => {
                    if rec.get(field) == Some(value) {
                        stream = Some(s.clone());
                    }
                }
                Stage::Enrich {
                    source,
                    target,
                    lookup,
                } => {
                    if let Some(v) = rec.get(source) {
                        if let Some(out) = lookup.lookup(v) {
                            rec.insert(target.clone(), out);
                        }
                    }
                }
            }
        }
        Some(RoutedRecord { stream, fields: rec })
    }

    /// Run the pipeline over a batch, returning the kept + transformed records GROUPED
    /// by their routed stream (CONCEPT:EG-165). Records that no `route` stage tagged
    /// land under `default_stream`. Input order is preserved within each group, and
    /// the outer map is a `BTreeMap`, so the result is fully deterministic.
    pub fn run_batch(
        &self,
        records: impl IntoIterator<Item = Record>,
        default_stream: &str,
    ) -> BTreeMap<String, Vec<Record>> {
        let mut out: BTreeMap<String, Vec<Record>> = BTreeMap::new();
        for rec in records {
            if let Some(routed) = self.run(rec) {
                let key = routed.stream.unwrap_or_else(|| default_stream.to_string());
                out.entry(key).or_default().push(routed.fields);
            }
        }
        out
    }

    /// Parse the minimal one-stage-per-line textual form (CONCEPT:EG-165). Blank lines
    /// and `#` comments are ignored. Supported lines:
    ///
    /// ```text
    /// parse_json <field>
    /// filter  <field> <op> <value>
    /// drop_if <field> <op> <value>
    /// set <field> <value>
    /// rename <from> <to>
    /// remove <field>
    /// coerce <field> <i64|f64|str|bool>
    /// route <field> <value> -> <stream>
    /// ```
    ///
    /// where `<op>` ∈ `eq | ne | gt | lt | contains | exists` and a `<value>` token is
    /// parsed as an int, then a float, then `true`/`false`/`null`, else a bare string.
    /// `enrich` is deliberately NOT expressible in text (it needs a closure) — build it
    /// with [`Pipeline::enrich`].
    pub fn parse(text: &str) -> Result<Self, String> {
        let mut pipe = Pipeline::new();
        for (lineno, raw) in text.lines().enumerate() {
            let line = raw.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let stage = parse_line(line).map_err(|e| format!("line {}: {e}", lineno + 1))?;
            pipe.stages.push(stage);
        }
        Ok(pipe)
    }
}

/// Turn a routed batch of records for ONE stream into a [`ColumnarSegment`] — the
/// documented landing seam into the existing eg-tsdb columnar path (CONCEPT:EG-165).
/// The column set is the sorted union of every record's keys (so a sparse field is
/// NULL where absent), giving a deterministic schema. Nested `Array`/`Object` values
/// land as their JSON text (see [`PipeValue::to_cell`]).
pub fn stream_to_columnar(records: &[Record]) -> Result<ColumnarSegment, String> {
    let mut names: Vec<String> = Vec::new();
    {
        let mut seen = std::collections::BTreeSet::new();
        for rec in records {
            for k in rec.keys() {
                if seen.insert(k.clone()) {
                    names.push(k.clone());
                }
            }
        }
        names.sort();
    }
    let rows: Vec<Vec<CellValue>> = records
        .iter()
        .map(|rec| {
            names
                .iter()
                .map(|n| rec.get(n).map(|v| v.to_cell()).unwrap_or(CellValue::Null))
                .collect()
        })
        .collect();
    ColumnarSegment::from_rows_inferred(&names, &rows)
}

// ---------------------------------------------------------------------------
// stage implementations
// ---------------------------------------------------------------------------

fn apply_parse_json(rec: &mut Record, field: &str) {
    let Some(PipeValue::Str(s)) = rec.get(field) else {
        return;
    };
    let Ok(parsed) = parse_json_value(s) else {
        return; // leave the record unchanged on a parse error (deterministic no-op)
    };
    match parsed {
        PipeValue::Object(map) => {
            rec.remove(field);
            for (k, v) in map {
                rec.insert(k, v);
            }
        }
        other => {
            rec.insert(field.to_string(), other);
        }
    }
}

fn apply_coerce(rec: &mut Record, field: &str, ty: CoerceType) {
    let Some(v) = rec.get(field) else {
        return;
    };
    let coerced = match ty {
        CoerceType::Str => Some(PipeValue::Str(match v {
            PipeValue::Str(s) => s.clone(),
            other => other.to_json(),
        })),
        CoerceType::I64 => match v {
            PipeValue::I64(_) => Some(v.clone()),
            PipeValue::F64(x) => Some(PipeValue::I64(*x as i64)),
            PipeValue::Bool(b) => Some(PipeValue::I64(if *b { 1 } else { 0 })),
            PipeValue::Str(s) => s.trim().parse::<i64>().ok().map(PipeValue::I64),
            _ => None,
        },
        CoerceType::F64 => match v {
            PipeValue::F64(_) => Some(v.clone()),
            PipeValue::I64(n) => Some(PipeValue::F64(*n as f64)),
            PipeValue::Bool(b) => Some(PipeValue::F64(if *b { 1.0 } else { 0.0 })),
            PipeValue::Str(s) => s.trim().parse::<f64>().ok().map(PipeValue::F64),
            _ => None,
        },
        CoerceType::Bool => match v {
            PipeValue::Bool(_) => Some(v.clone()),
            PipeValue::I64(n) => Some(PipeValue::Bool(*n != 0)),
            PipeValue::Str(s) => match s.trim().to_ascii_lowercase().as_str() {
                "true" | "1" | "yes" => Some(PipeValue::Bool(true)),
                "false" | "0" | "no" => Some(PipeValue::Bool(false)),
                _ => None,
            },
            _ => None,
        },
    };
    if let Some(c) = coerced {
        rec.insert(field.to_string(), c);
    }
}

// ---------------------------------------------------------------------------
// textual form parsing
// ---------------------------------------------------------------------------

fn parse_line(line: &str) -> Result<Stage, String> {
    let toks: Vec<&str> = line.split_whitespace().collect();
    let op = toks[0];
    match op {
        "parse_json" => {
            let field = toks.get(1).ok_or("parse_json needs a <field>")?;
            Ok(Stage::ParseJson {
                field: (*field).to_string(),
            })
        }
        "filter" | "drop_if" => {
            let pred = parse_predicate(&toks[1..])?;
            Ok(if op == "filter" {
                Stage::Filter(pred)
            } else {
                Stage::DropIf(pred)
            })
        }
        "set" => {
            let field = toks.get(1).ok_or("set needs <field> <value>")?;
            let value = toks.get(2).ok_or("set needs <field> <value>")?;
            Ok(Stage::Set {
                field: (*field).to_string(),
                value: parse_value_token(value),
            })
        }
        "rename" => {
            let from = toks.get(1).ok_or("rename needs <from> <to>")?;
            let to = toks.get(2).ok_or("rename needs <from> <to>")?;
            Ok(Stage::Rename {
                from: (*from).to_string(),
                to: (*to).to_string(),
            })
        }
        "remove" => {
            let field = toks.get(1).ok_or("remove needs a <field>")?;
            Ok(Stage::Remove {
                field: (*field).to_string(),
            })
        }
        "coerce" => {
            let field = toks.get(1).ok_or("coerce needs <field> <type>")?;
            let ty = match *toks.get(2).ok_or("coerce needs <field> <type>")? {
                "i64" | "int" => CoerceType::I64,
                "f64" | "float" => CoerceType::F64,
                "str" | "string" => CoerceType::Str,
                "bool" => CoerceType::Bool,
                other => return Err(format!("unknown coerce type '{other}'")),
            };
            Ok(Stage::Coerce {
                field: (*field).to_string(),
                ty,
            })
        }
        "route" => {
            // route <field> <value> -> <stream>
            let arrow = toks
                .iter()
                .position(|t| *t == "->")
                .ok_or("route needs '-> <stream>'")?;
            if arrow < 3 {
                return Err("route needs <field> <value> -> <stream>".into());
            }
            let field = toks[1];
            let value = toks[2];
            let stream = toks.get(arrow + 1).ok_or("route needs a <stream> after '->'")?;
            Ok(Stage::Route {
                field: field.to_string(),
                value: parse_value_token(value),
                stream: (*stream).to_string(),
            })
        }
        "enrich" => Err("enrich is not expressible in the textual form; use Pipeline::enrich".into()),
        other => Err(format!("unknown stage '{other}'")),
    }
}

fn parse_predicate(toks: &[&str]) -> Result<Predicate, String> {
    let field = toks.first().ok_or("predicate needs a <field>")?;
    let op = match *toks.get(1).ok_or("predicate needs an <op>")? {
        "eq" | "==" => CmpOp::Eq,
        "ne" | "!=" => CmpOp::Ne,
        "gt" | ">" => CmpOp::Gt,
        "lt" | "<" => CmpOp::Lt,
        "contains" => CmpOp::Contains,
        "exists" => CmpOp::Exists,
        other => return Err(format!("unknown predicate op '{other}'")),
    };
    let value = if op == CmpOp::Exists {
        PipeValue::Null
    } else {
        parse_value_token(toks.get(2).ok_or("predicate needs a <value>")?)
    };
    Ok(Predicate {
        field: (*field).to_string(),
        op,
        value,
    })
}

/// Parse a bare token into a [`PipeValue`]: int, then float, then `true`/`false`/
/// `null`, else a string (CONCEPT:EG-165).
fn parse_value_token(tok: &str) -> PipeValue {
    if let Ok(n) = tok.parse::<i64>() {
        return PipeValue::I64(n);
    }
    if let Ok(x) = tok.parse::<f64>() {
        return PipeValue::F64(x);
    }
    match tok {
        "true" => PipeValue::Bool(true),
        "false" => PipeValue::Bool(false),
        "null" => PipeValue::Null,
        other => PipeValue::Str(other.to_string()),
    }
}

// ---------------------------------------------------------------------------
// minimal hand-rolled JSON reader (no serde_json — the zero-new-dep contract)
// ---------------------------------------------------------------------------

/// Parse a JSON document into a [`PipeValue`] (CONCEPT:EG-165). A tiny
/// recursive-descent reader over `&[u8]` covering objects, arrays, strings (with the
/// standard escapes), numbers (int vs float), and `true`/`false`/`null`. Deliberately
/// dependency-free to hold the Pi contract the rest of eg-tsdb keeps.
pub fn parse_json_value(s: &str) -> Result<PipeValue, String> {
    let bytes = s.as_bytes();
    let mut p = JsonParser { b: bytes, i: 0 };
    p.skip_ws();
    let v = p.parse_value()?;
    p.skip_ws();
    if p.i != bytes.len() {
        return Err(format!("trailing bytes at offset {}", p.i));
    }
    Ok(v)
}

struct JsonParser<'a> {
    b: &'a [u8],
    i: usize,
}

impl JsonParser<'_> {
    fn skip_ws(&mut self) {
        while self.i < self.b.len() && matches!(self.b[self.i], b' ' | b'\t' | b'\n' | b'\r') {
            self.i += 1;
        }
    }

    fn peek(&self) -> Option<u8> {
        self.b.get(self.i).copied()
    }

    fn parse_value(&mut self) -> Result<PipeValue, String> {
        self.skip_ws();
        match self.peek().ok_or("unexpected end of JSON")? {
            b'{' => self.parse_object(),
            b'[' => self.parse_array(),
            b'"' => Ok(PipeValue::Str(self.parse_string()?)),
            b't' | b'f' => self.parse_bool(),
            b'n' => self.parse_null(),
            _ => self.parse_number(),
        }
    }

    fn parse_object(&mut self) -> Result<PipeValue, String> {
        self.i += 1; // '{'
        let mut map = BTreeMap::new();
        self.skip_ws();
        if self.peek() == Some(b'}') {
            self.i += 1;
            return Ok(PipeValue::Object(map));
        }
        loop {
            self.skip_ws();
            if self.peek() != Some(b'"') {
                return Err("expected string key in object".into());
            }
            let key = self.parse_string()?;
            self.skip_ws();
            if self.peek() != Some(b':') {
                return Err("expected ':' after object key".into());
            }
            self.i += 1;
            let val = self.parse_value()?;
            map.insert(key, val);
            self.skip_ws();
            match self.peek() {
                Some(b',') => {
                    self.i += 1;
                }
                Some(b'}') => {
                    self.i += 1;
                    break;
                }
                _ => return Err("expected ',' or '}' in object".into()),
            }
        }
        Ok(PipeValue::Object(map))
    }

    fn parse_array(&mut self) -> Result<PipeValue, String> {
        self.i += 1; // '['
        let mut items = Vec::new();
        self.skip_ws();
        if self.peek() == Some(b']') {
            self.i += 1;
            return Ok(PipeValue::Array(items));
        }
        loop {
            let val = self.parse_value()?;
            items.push(val);
            self.skip_ws();
            match self.peek() {
                Some(b',') => {
                    self.i += 1;
                }
                Some(b']') => {
                    self.i += 1;
                    break;
                }
                _ => return Err("expected ',' or ']' in array".into()),
            }
        }
        Ok(PipeValue::Array(items))
    }

    fn parse_string(&mut self) -> Result<String, String> {
        self.i += 1; // opening quote
        let mut out = String::new();
        while let Some(c) = self.peek() {
            self.i += 1;
            match c {
                b'"' => return Ok(out),
                b'\\' => {
                    let esc = self.peek().ok_or("unterminated escape")?;
                    self.i += 1;
                    match esc {
                        b'"' => out.push('"'),
                        b'\\' => out.push('\\'),
                        b'/' => out.push('/'),
                        b'n' => out.push('\n'),
                        b't' => out.push('\t'),
                        b'r' => out.push('\r'),
                        b'b' => out.push('\u{0008}'),
                        b'f' => out.push('\u{000C}'),
                        b'u' => {
                            let hex = self
                                .b
                                .get(self.i..self.i + 4)
                                .ok_or("truncated \\u escape")?;
                            let code = u32::from_str_radix(
                                std::str::from_utf8(hex).map_err(|_| "bad \\u hex")?,
                                16,
                            )
                            .map_err(|_| "bad \\u hex")?;
                            self.i += 4;
                            out.push(char::from_u32(code).unwrap_or('\u{FFFD}'));
                        }
                        other => return Err(format!("bad escape '\\{}'", other as char)),
                    }
                }
                _ => {
                    // Copy the raw UTF-8 byte(s). `c` is one byte of a (possibly multi-
                    // byte) char; push it through a 1-byte buffer-safe path.
                    out.push(c as char);
                    // Fix up multi-byte UTF-8: if the byte was a lead byte, the naive
                    // `c as char` above is wrong, so handle >=0x80 via the source slice.
                    if c >= 0x80 {
                        out.pop();
                        let start = self.i - 1;
                        let width = utf8_width(c);
                        let end = (start + width).min(self.b.len());
                        let chunk = std::str::from_utf8(&self.b[start..end])
                            .map_err(|_| "invalid UTF-8 in string")?;
                        out.push_str(chunk);
                        self.i = end;
                    }
                }
            }
        }
        Err("unterminated string".into())
    }

    fn parse_bool(&mut self) -> Result<PipeValue, String> {
        if self.b[self.i..].starts_with(b"true") {
            self.i += 4;
            Ok(PipeValue::Bool(true))
        } else if self.b[self.i..].starts_with(b"false") {
            self.i += 5;
            Ok(PipeValue::Bool(false))
        } else {
            Err("invalid literal (expected true/false)".into())
        }
    }

    fn parse_null(&mut self) -> Result<PipeValue, String> {
        if self.b[self.i..].starts_with(b"null") {
            self.i += 4;
            Ok(PipeValue::Null)
        } else {
            Err("invalid literal (expected null)".into())
        }
    }

    fn parse_number(&mut self) -> Result<PipeValue, String> {
        let start = self.i;
        let mut is_float = false;
        while let Some(c) = self.peek() {
            match c {
                b'0'..=b'9' | b'-' | b'+' => self.i += 1,
                b'.' | b'e' | b'E' => {
                    is_float = true;
                    self.i += 1;
                }
                _ => break,
            }
        }
        let tok = std::str::from_utf8(&self.b[start..self.i]).map_err(|_| "bad number")?;
        if tok.is_empty() {
            return Err(format!("unexpected byte at offset {start}"));
        }
        if is_float {
            tok.parse::<f64>()
                .map(PipeValue::F64)
                .map_err(|_| format!("bad float '{tok}'"))
        } else {
            match tok.parse::<i64>() {
                Ok(n) => Ok(PipeValue::I64(n)),
                Err(_) => tok
                    .parse::<f64>()
                    .map(PipeValue::F64)
                    .map_err(|_| format!("bad number '{tok}'")),
            }
        }
    }
}

/// UTF-8 byte-width from a lead byte (CONCEPT:EG-165 JSON reader helper).
fn utf8_width(lead: u8) -> usize {
    if lead < 0x80 {
        1
    } else if lead >> 5 == 0b110 {
        2
    } else if lead >> 4 == 0b1110 {
        3
    } else if lead >> 3 == 0b11110 {
        4
    } else {
        1
    }
}

/// Write a JSON string literal (quoted + escaped) into `out` (CONCEPT:EG-165).
fn write_json_str(s: &str, out: &mut String) {
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\t' => out.push_str("\\t"),
            '\r' => out.push_str("\\r"),
            '\u{0008}' => out.push_str("\\b"),
            '\u{000C}' => out.push_str("\\f"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out.push('"');
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rec(pairs: &[(&str, PipeValue)]) -> Record {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.clone()))
            .collect()
    }

    /// CONCEPT:EG-165 — `parse_json` explodes a JSON string field into top-level
    /// sub-fields and removes the original blob field.
    #[test]
    fn eg_165_parse_json_expands_fields() {
        let r = rec(&[(
            "body",
            PipeValue::str(r#"{"level":"error","code":500,"ok":false}"#),
        )]);
        let out = Pipeline::new().parse_json("body").run(r).unwrap();
        assert_eq!(out.fields.get("level"), Some(&PipeValue::str("error")));
        assert_eq!(out.fields.get("code"), Some(&PipeValue::I64(500)));
        assert_eq!(out.fields.get("ok"), Some(&PipeValue::Bool(false)));
        assert!(!out.fields.contains_key("body")); // original blob removed
    }

    /// CONCEPT:EG-165 — `parse_json` of a nested object yields a nested `Object`
    /// sub-field, and the hand-rolled JSON reader handles arrays + floats + escapes.
    #[test]
    fn eg_165_parse_json_nested_and_types() {
        let v = parse_json_value(r#"{"a":[1,2.5,"x"],"b":{"c":true},"s":"hi\n\"q\""}"#).unwrap();
        let PipeValue::Object(map) = v else {
            panic!("expected object")
        };
        assert_eq!(
            map.get("a"),
            Some(&PipeValue::Array(vec![
                PipeValue::I64(1),
                PipeValue::F64(2.5),
                PipeValue::str("x"),
            ]))
        );
        assert_eq!(map.get("s"), Some(&PipeValue::str("hi\n\"q\"")));
    }

    /// CONCEPT:EG-165 — `filter` KEEPS records matching the predicate (drops the rest).
    #[test]
    fn eg_165_filter_keeps_matching() {
        let pipe = Pipeline::new().filter(Predicate::new("level", CmpOp::Eq, PipeValue::str("error")));
        let kept = pipe.run(rec(&[("level", PipeValue::str("error"))]));
        let dropped = pipe.run(rec(&[("level", PipeValue::str("info"))]));
        assert!(kept.is_some());
        assert!(dropped.is_none());
    }

    /// CONCEPT:EG-165 — `drop_if` REMOVES records matching the predicate.
    #[test]
    fn eg_165_drop_if_removes_matching() {
        let pipe = Pipeline::new().drop_if(Predicate::new("level", CmpOp::Eq, PipeValue::str("debug")));
        assert!(pipe.run(rec(&[("level", PipeValue::str("debug"))])).is_none());
        assert!(pipe.run(rec(&[("level", PipeValue::str("warn"))])).is_some());
    }

    /// CONCEPT:EG-165 — every comparison op (eq/ne/gt/lt/contains/exists), incl.
    /// missing-field handling for `ne`.
    #[test]
    fn eg_165_predicate_ops() {
        let r = rec(&[
            ("n", PipeValue::I64(10)),
            ("msg", PipeValue::str("connection refused")),
        ]);
        assert!(Predicate::new("n", CmpOp::Gt, PipeValue::I64(5)).eval(&r));
        assert!(!Predicate::new("n", CmpOp::Gt, PipeValue::I64(50)).eval(&r));
        assert!(Predicate::new("n", CmpOp::Lt, PipeValue::I64(50)).eval(&r));
        assert!(Predicate::new("n", CmpOp::Eq, PipeValue::I64(10)).eval(&r));
        assert!(Predicate::new("n", CmpOp::Ne, PipeValue::I64(11)).eval(&r));
        assert!(Predicate::new("msg", CmpOp::Contains, PipeValue::str("refused")).eval(&r));
        assert!(Predicate::new("msg", CmpOp::Exists, PipeValue::Null).eval(&r));
        assert!(!Predicate::new("absent", CmpOp::Exists, PipeValue::Null).eval(&r));
        // A missing field is `!=` any operand.
        assert!(Predicate::new("absent", CmpOp::Ne, PipeValue::I64(1)).eval(&r));
    }

    /// CONCEPT:EG-165 — `set` / `rename` / `remove` / `coerce` transform the record.
    #[test]
    fn eg_165_set_rename_remove_coerce() {
        let out = Pipeline::new()
            .set("env", PipeValue::str("prod"))
            .rename("msg", "message")
            .remove("password")
            .coerce("status", CoerceType::I64)
            .run(rec(&[
                ("msg", PipeValue::str("hello")),
                ("password", PipeValue::str("secret")),
                ("status", PipeValue::str("404")),
            ]))
            .unwrap();
        assert_eq!(out.fields.get("env"), Some(&PipeValue::str("prod")));
        assert_eq!(out.fields.get("message"), Some(&PipeValue::str("hello")));
        assert!(!out.fields.contains_key("msg"));
        assert!(!out.fields.contains_key("password"));
        assert_eq!(out.fields.get("status"), Some(&PipeValue::I64(404)));
    }

    /// CONCEPT:EG-165 — `coerce` is best-effort: an unconvertible value is left
    /// unchanged, and each target type round-trips.
    #[test]
    fn eg_165_coerce_best_effort() {
        let out = Pipeline::new()
            .coerce("bad", CoerceType::I64)
            .coerce("f", CoerceType::F64)
            .coerce("b", CoerceType::Bool)
            .coerce("n", CoerceType::Str)
            .run(rec(&[
                ("bad", PipeValue::str("not-a-number")),
                ("f", PipeValue::I64(3)),
                ("b", PipeValue::str("yes")),
                ("n", PipeValue::I64(7)),
            ]))
            .unwrap();
        assert_eq!(out.fields.get("bad"), Some(&PipeValue::str("not-a-number"))); // unchanged
        assert_eq!(out.fields.get("f"), Some(&PipeValue::F64(3.0)));
        assert_eq!(out.fields.get("b"), Some(&PipeValue::Bool(true)));
        assert_eq!(out.fields.get("n"), Some(&PipeValue::str("7")));
    }

    /// CONCEPT:EG-165 — `route` tags the destination stream when a field matches.
    #[test]
    fn eg_165_route_tags_stream() {
        let pipe = Pipeline::new().route("level", PipeValue::str("error"), "errors");
        let hit = pipe.run(rec(&[("level", PipeValue::str("error"))])).unwrap();
        let miss = pipe.run(rec(&[("level", PipeValue::str("info"))])).unwrap();
        assert_eq!(hit.stream.as_deref(), Some("errors"));
        assert_eq!(miss.stream, None);
    }

    /// CONCEPT:EG-165 — the cross-modal enrichment hook: a caller-supplied lookup
    /// (here a mock "graph" map) folds a resolved value into a new field. This is the
    /// "surpass OpenObserve" differentiator (log ⨯ graph).
    #[test]
    fn eg_165_enrich_via_mock_lookup() {
        // Mock graph lookup: user_id -> display name.
        let graph: BTreeMap<i64, &str> =
            [(1, "alice"), (2, "bob")].into_iter().collect();
        let pipe = Pipeline::new().enrich("user_id", "user_name", move |v| match v {
            PipeValue::I64(id) => graph.get(id).map(|n| PipeValue::str(*n)),
            _ => None,
        });
        let out = pipe.run(rec(&[("user_id", PipeValue::I64(2))])).unwrap();
        assert_eq!(out.fields.get("user_name"), Some(&PipeValue::str("bob")));

        // Unknown id -> no enrichment field added.
        let miss = pipe.run(rec(&[("user_id", PipeValue::I64(99))])).unwrap();
        assert!(!miss.fields.contains_key("user_name"));
    }

    /// CONCEPT:EG-165 — a multi-stage pipeline over a BATCH: parse, drop noise, set,
    /// route, and the executor groups the kept records by routed stream.
    #[test]
    fn eg_165_multi_stage_batch_groups_by_stream() {
        let pipe = Pipeline::new()
            .parse_json("body")
            .drop_if(Predicate::new("level", CmpOp::Eq, PipeValue::str("debug")))
            .set("ingested", PipeValue::Bool(true))
            .route("level", PipeValue::str("error"), "errors")
            .route("level", PipeValue::str("warn"), "warnings");

        let batch = vec![
            rec(&[("body", PipeValue::str(r#"{"level":"error","m":"boom"}"#))]),
            rec(&[("body", PipeValue::str(r#"{"level":"debug","m":"noise"}"#))]),
            rec(&[("body", PipeValue::str(r#"{"level":"warn","m":"slow"}"#))]),
            rec(&[("body", PipeValue::str(r#"{"level":"info","m":"ok"}"#))]),
        ];
        let grouped = pipe.run_batch(batch, "_default");

        assert_eq!(grouped.len(), 3); // errors, warnings, _default (debug dropped)
        assert_eq!(grouped["errors"].len(), 1);
        assert_eq!(grouped["warnings"].len(), 1);
        assert_eq!(grouped["_default"].len(), 1); // the info record
        assert!(!grouped.contains_key("debug"));
        // set stage applied to a routed record
        assert_eq!(
            grouped["errors"][0].get("ingested"),
            Some(&PipeValue::Bool(true))
        );
    }

    /// CONCEPT:EG-165 — determinism: the same batch through the same pipeline yields
    /// byte-identical grouped output across runs (BTreeMap ordering + preserved order).
    #[test]
    fn eg_165_determinism() {
        let pipe = Pipeline::new()
            .parse_json("body")
            .route("level", PipeValue::str("error"), "errors");
        let batch: Vec<Record> = (0..50)
            .map(|i| {
                let lvl = if i % 2 == 0 { "error" } else { "info" };
                rec(&[("body", PipeValue::str(format!(r#"{{"level":"{lvl}","i":{i}}}"#)))])
            })
            .collect();
        let a = pipe.run_batch(batch.clone(), "_default");
        let b = pipe.run_batch(batch, "_default");
        assert_eq!(a, b);
        // And each group's field order is stable (BTreeMap keys sorted).
        let keys: Vec<&String> = a["errors"][0].keys().collect();
        let mut sorted = keys.clone();
        sorted.sort();
        assert_eq!(keys, sorted);
    }

    /// CONCEPT:EG-165 — the textual DSL form parses into an equivalent pipeline.
    #[test]
    fn eg_165_textual_form_parse() {
        let text = r#"
            # a small ingest program
            parse_json body
            drop_if level eq debug
            set env prod
            rename msg message
            remove password
            coerce status i64
            route level error -> errors
        "#;
        let pipe = Pipeline::parse(text).unwrap();
        assert_eq!(pipe.stages.len(), 7);
        let out = pipe
            .run(rec(&[(
                "body",
                PipeValue::str(r#"{"level":"error","status":"500","password":"x","msg":"hi"}"#),
            )]))
            .unwrap();
        assert_eq!(out.stream.as_deref(), Some("errors"));
        assert_eq!(out.fields.get("env"), Some(&PipeValue::str("prod")));
        assert_eq!(out.fields.get("message"), Some(&PipeValue::str("hi")));
        assert_eq!(out.fields.get("status"), Some(&PipeValue::I64(500)));
        assert!(!out.fields.contains_key("password"));
    }

    /// CONCEPT:EG-165 — a malformed textual line reports its line number; `enrich` is
    /// rejected in text (needs a closure).
    #[test]
    fn eg_165_textual_form_errors() {
        assert!(Pipeline::parse("bogus_op x").unwrap_err().contains("unknown stage"));
        assert!(Pipeline::parse("enrich a b").unwrap_err().contains("enrich"));
        let e = Pipeline::parse("parse_json a\ncoerce f nope").unwrap_err();
        assert!(e.contains("line 2"), "got: {e}");
    }

    /// CONCEPT:EG-165 — the documented landing seam: a routed record batch lowers into
    /// an eg-tsdb ColumnarSegment (sorted-union schema, nested values as JSON text).
    #[test]
    fn eg_165_stream_to_columnar_seam() {
        let pipe = Pipeline::new().parse_json("body");
        let grouped = pipe.run_batch(
            vec![
                rec(&[("body", PipeValue::str(r#"{"level":"error","code":500}"#))]),
                rec(&[("body", PipeValue::str(r#"{"level":"warn","note":"x"}"#))]),
            ],
            "logs",
        );
        let seg = stream_to_columnar(&grouped["logs"]).unwrap();
        assert_eq!(seg.len(), 2);
        // union of keys: code, level, note (sorted) -> a `level` string column exists
        let level: Vec<Option<&str>> = seg.column("level").unwrap().iter_str().collect();
        assert_eq!(level, vec![Some("error"), Some("warn")]);
        // `code` present in row 0, NULL in row 1 (sparse union)
        let code = seg.column("code").unwrap();
        assert!(code.is_valid(0));
        assert!(!code.is_valid(1));
    }
}
