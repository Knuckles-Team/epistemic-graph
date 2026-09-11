//! VRL-style ingest transform pipelines (CONCEPT:EG-KG.enrichment.cross-modal-enrichment-hook).
//!
//! OpenObserve / Vector apply a *Vector Remap Language* (VRL) program to every log
//! or event record at ingest, BEFORE it lands in a stream: parse a JSON blob into
//! fields, drop noisy records, set/rename/remove fields, coerce types, and route the
//! record to a destination stream. This module is the epistemic-graph equivalent —
//! a compact, deterministic, pure-Rust transform engine that runs over a record just
//! before it lands in an eg-tsdb series (the log-ingest front door of CONCEPT:AU-KG.ingest.self-ingest/
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

mod columnar;
mod json;
mod stages;

use crate::columnar::{CellValue, ColumnarSegment};

/// A small JSON-ish value carried through the pipeline (CONCEPT:EG-KG.enrichment.cross-modal-enrichment-hook). Scalars
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
    /// non-numeric kinds (CONCEPT:EG-KG.enrichment.cross-modal-enrichment-hook).
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
    /// (CONCEPT:EG-KG.enrichment.cross-modal-enrichment-hook). Scalars map 1:1; `Array`/`Object`/`Null` collapse to their
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
    /// `coerce`) (CONCEPT:EG-KG.enrichment.cross-modal-enrichment-hook).
    pub fn to_json(&self) -> String {
        json::to_json(self)
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

/// A record flowing through the pipeline: an ordered field map (CONCEPT:EG-KG.enrichment.cross-modal-enrichment-hook).
pub type Record = BTreeMap<String, PipeValue>;

/// The scalar type a [`Stage::Coerce`] converts a field to (CONCEPT:EG-KG.enrichment.cross-modal-enrichment-hook).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum CoerceType {
    Str,
    I64,
    F64,
    Bool,
}

/// The comparison a [`Predicate`] applies to a field (CONCEPT:EG-KG.enrichment.cross-modal-enrichment-hook).
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
/// (CONCEPT:EG-KG.enrichment.cross-modal-enrichment-hook). A missing field is `false` for every op except `Ne` (absent ≠
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

    /// Evaluate against a record (CONCEPT:EG-KG.enrichment.cross-modal-enrichment-hook). Deterministic and side-effect free.
    pub fn eval(&self, rec: &Record) -> bool {
        let present = rec.get(&self.field);
        match self.op {
            CmpOp::Exists => present.is_some(),
            CmpOp::Ne => match present {
                None => true,
                Some(v) => v != &self.value,
            },
            CmpOp::Eq => present == Some(&self.value),
            CmpOp::Gt => cmp_num_or_str(present, &self.value)
                .map(|o| o.is_gt())
                .unwrap_or(false),
            CmpOp::Lt => cmp_num_or_str(present, &self.value)
                .map(|o| o.is_lt())
                .unwrap_or(false),
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
/// lexicographically when both are strings (CONCEPT:EG-KG.enrichment.cross-modal-enrichment-hook).
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

/// A caller-supplied enrichment lookup (CONCEPT:EG-KG.enrichment.cross-modal-enrichment-hook) — the cross-modal seam. The
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

/// One transform op (CONCEPT:EG-KG.enrichment.cross-modal-enrichment-hook). A closed, compact set — the VRL-subset the
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
    /// — the cross-modal (log ⨯ graph) differentiator (CONCEPT:EG-KG.enrichment.cross-modal-enrichment-hook). If `source` is
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
    /// (CONCEPT:EG-KG.enrichment.cross-modal-enrichment-hook).
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

/// The result of running a [`Pipeline`] over one record (CONCEPT:EG-KG.enrichment.cross-modal-enrichment-hook): the
/// transformed fields plus the routed destination stream (`None` when no `route`
/// stage matched — the caller falls back to a default stream).
#[derive(Clone, Debug, PartialEq)]
pub struct RoutedRecord {
    pub stream: Option<String>,
    pub fields: Record,
}

/// An ordered list of [`Stage`]s applied to a record in sequence (CONCEPT:EG-KG.enrichment.cross-modal-enrichment-hook).
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

    /// Run the pipeline over ONE record, returning the transformed + routed record, or
    /// `None` if a `filter`/`drop_if` stage dropped it (CONCEPT:EG-KG.enrichment.cross-modal-enrichment-hook). Deterministic.
    pub fn run(&self, record: Record) -> Option<RoutedRecord> {
        let mut rec = record;
        let mut stream: Option<String> = None;
        for stage in &self.stages {
            if !stages::apply_stage(stage, &mut rec, &mut stream) {
                return None;
            }
        }
        Some(RoutedRecord {
            stream,
            fields: rec,
        })
    }

    /// Run the pipeline over a batch, returning the kept + transformed records GROUPED
    /// by their routed stream (CONCEPT:EG-KG.enrichment.cross-modal-enrichment-hook). Records that no `route` stage tagged
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

    /// Parse the minimal one-stage-per-line textual form (CONCEPT:EG-KG.enrichment.cross-modal-enrichment-hook). Blank lines
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
        stages::parse_text(text)
    }
}

/// Turn a routed batch of records for ONE stream into a [`ColumnarSegment`] — the
/// documented landing seam into the existing eg-tsdb columnar path (CONCEPT:EG-KG.enrichment.cross-modal-enrichment-hook).
/// The column set is the sorted union of every record's keys (so a sparse field is
/// NULL where absent), giving a deterministic schema. Nested `Array`/`Object` values
/// land as their JSON text (see [`PipeValue::to_cell`]).
pub fn stream_to_columnar(records: &[Record]) -> Result<ColumnarSegment, String> {
    columnar::stream_to_columnar(records)
}

// ---------------------------------------------------------------------------
// child phase owners
// ---------------------------------------------------------------------------

pub fn parse_json_value(s: &str) -> Result<PipeValue, String> {
    json::parse_json_value(s)
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

    /// CONCEPT:EG-KG.enrichment.cross-modal-enrichment-hook — `parse_json` explodes a JSON string field into top-level
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

    /// CONCEPT:EG-KG.enrichment.cross-modal-enrichment-hook — `parse_json` of a nested object yields a nested `Object`
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

    /// CONCEPT:EG-KG.enrichment.cross-modal-enrichment-hook — JSON emission keeps the
    /// BTreeMap order, scalar spellings, nested structure, nulls, and string escapes stable.
    #[test]
    fn eg_165_json_emission_is_sorted_and_lossless() {
        let mut fields = BTreeMap::new();
        fields.insert(
            "z".to_string(),
            PipeValue::Array(vec![
                PipeValue::Bool(true),
                PipeValue::I64(-2),
                PipeValue::F64(1.5),
                PipeValue::str("line\n\"quote\""),
            ]),
        );
        fields.insert("a".to_string(), PipeValue::Null);

        assert_eq!(
            PipeValue::Object(fields).to_json(),
            r#"{"a":null,"z":[true,-2,1.5,"line\n\"quote\""]}"#
        );
    }

    /// CONCEPT:EG-KG.enrichment.cross-modal-enrichment-hook — JSON string scanning preserves
    /// UTF-8 and every supported escape while rejecting malformed escape boundaries.
    #[test]
    fn eg_165_json_string_escape_boundaries() {
        assert_eq!(
            parse_json_value(r#""café\/\b\f\r\t\u0041""#),
            Ok(PipeValue::str("café/\u{0008}\u{000C}\r\tA"))
        );
        assert!(parse_json_value(r#""\q""#).is_err());
        assert!(parse_json_value(r#""\u12""#).is_err());
        assert!(parse_json_value(r#""unterminated"#).is_err());
    }

    /// CONCEPT:EG-KG.enrichment.cross-modal-enrichment-hook — `filter` KEEPS records matching the predicate (drops the rest).
    #[test]
    fn eg_165_filter_keeps_matching() {
        let pipe =
            Pipeline::new().filter(Predicate::new("level", CmpOp::Eq, PipeValue::str("error")));
        let kept = pipe.run(rec(&[("level", PipeValue::str("error"))]));
        let dropped = pipe.run(rec(&[("level", PipeValue::str("info"))]));
        assert!(kept.is_some());
        assert!(dropped.is_none());
    }

    /// CONCEPT:EG-KG.enrichment.cross-modal-enrichment-hook — `drop_if` REMOVES records matching the predicate.
    #[test]
    fn eg_165_drop_if_removes_matching() {
        let pipe =
            Pipeline::new().drop_if(Predicate::new("level", CmpOp::Eq, PipeValue::str("debug")));
        assert!(pipe
            .run(rec(&[("level", PipeValue::str("debug"))]))
            .is_none());
        assert!(pipe
            .run(rec(&[("level", PipeValue::str("warn"))]))
            .is_some());
    }

    /// CONCEPT:EG-KG.enrichment.cross-modal-enrichment-hook — every comparison op (eq/ne/gt/lt/contains/exists), incl.
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

    /// CONCEPT:EG-KG.enrichment.cross-modal-enrichment-hook — `set` / `rename` / `remove` / `coerce` transform the record.
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

    /// CONCEPT:EG-KG.enrichment.cross-modal-enrichment-hook — `coerce` is best-effort: an unconvertible value is left
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

    /// CONCEPT:EG-KG.enrichment.cross-modal-enrichment-hook — each primitive coercion keeps
    /// its existing trim, numeric, boolean, null, and unsupported-value boundaries.
    #[test]
    fn eg_165_coerce_primitive_boundaries() {
        let mut object = BTreeMap::new();
        object.insert("x".to_string(), PipeValue::I64(1));
        let out = Pipeline::new()
            .coerce("str_null", CoerceType::Str)
            .coerce("str_array", CoerceType::Str)
            .coerce("i_float", CoerceType::I64)
            .coerce("i_bool", CoerceType::I64)
            .coerce("i_text", CoerceType::I64)
            .coerce("i_null", CoerceType::I64)
            .coerce("f_int", CoerceType::F64)
            .coerce("f_bool", CoerceType::F64)
            .coerce("f_text", CoerceType::F64)
            .coerce("f_null", CoerceType::F64)
            .coerce("b_int", CoerceType::Bool)
            .coerce("b_text", CoerceType::Bool)
            .coerce("b_bad", CoerceType::Bool)
            .coerce("b_float", CoerceType::Bool)
            .run(rec(&[
                ("str_null", PipeValue::Null),
                (
                    "str_array",
                    PipeValue::Array(vec![PipeValue::I64(1), PipeValue::Bool(false)]),
                ),
                ("i_float", PipeValue::F64(3.75)),
                ("i_bool", PipeValue::Bool(true)),
                ("i_text", PipeValue::str(" -12 ")),
                ("i_null", PipeValue::Null),
                ("f_int", PipeValue::I64(-2)),
                ("f_bool", PipeValue::Bool(false)),
                ("f_text", PipeValue::str(" 4.5 ")),
                ("f_null", PipeValue::Null),
                ("b_int", PipeValue::I64(-1)),
                ("b_text", PipeValue::str("YeS")),
                ("b_bad", PipeValue::str("maybe")),
                ("b_float", PipeValue::F64(1.0)),
                ("obj", PipeValue::Object(object)),
            ]))
            .unwrap();

        assert_eq!(out.fields.get("str_null"), Some(&PipeValue::str("null")));
        assert_eq!(
            out.fields.get("str_array"),
            Some(&PipeValue::str("[1,false]"))
        );
        assert_eq!(out.fields.get("i_float"), Some(&PipeValue::I64(3)));
        assert_eq!(out.fields.get("i_bool"), Some(&PipeValue::I64(1)));
        assert_eq!(out.fields.get("i_text"), Some(&PipeValue::I64(-12)));
        assert_eq!(out.fields.get("i_null"), Some(&PipeValue::Null));
        assert_eq!(out.fields.get("f_int"), Some(&PipeValue::F64(-2.0)));
        assert_eq!(out.fields.get("f_bool"), Some(&PipeValue::F64(0.0)));
        assert_eq!(out.fields.get("f_text"), Some(&PipeValue::F64(4.5)));
        assert_eq!(out.fields.get("f_null"), Some(&PipeValue::Null));
        assert_eq!(out.fields.get("b_int"), Some(&PipeValue::Bool(true)));
        assert_eq!(out.fields.get("b_text"), Some(&PipeValue::Bool(true)));
        assert_eq!(out.fields.get("b_bad"), Some(&PipeValue::str("maybe")));
        assert_eq!(out.fields.get("b_float"), Some(&PipeValue::F64(1.0)));
        assert_eq!(
            out.fields.get("obj"),
            Some(&PipeValue::Object(BTreeMap::from([(
                "x".to_string(),
                PipeValue::I64(1),
            )])))
        );
    }

    /// CONCEPT:EG-KG.enrichment.cross-modal-enrichment-hook — `route` tags the destination stream when a field matches.
    #[test]
    fn eg_165_route_tags_stream() {
        let pipe = Pipeline::new().route("level", PipeValue::str("error"), "errors");
        let hit = pipe
            .run(rec(&[("level", PipeValue::str("error"))]))
            .unwrap();
        let miss = pipe.run(rec(&[("level", PipeValue::str("info"))])).unwrap();
        assert_eq!(hit.stream.as_deref(), Some("errors"));
        assert_eq!(miss.stream, None);
    }

    /// CONCEPT:EG-KG.enrichment.cross-modal-enrichment-hook — the cross-modal enrichment hook: a caller-supplied lookup
    /// (here a mock "graph" map) folds a resolved value into a new field. This is the
    /// "surpass OpenObserve" differentiator (log ⨯ graph).
    #[test]
    fn eg_165_enrich_via_mock_lookup() {
        // Mock graph lookup: user_id -> display name.
        let graph: BTreeMap<i64, &str> = [(1, "alice"), (2, "bob")].into_iter().collect();
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

    /// CONCEPT:EG-KG.enrichment.cross-modal-enrichment-hook — a multi-stage pipeline over a BATCH: parse, drop noise, set,
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

    /// CONCEPT:EG-KG.enrichment.cross-modal-enrichment-hook — determinism: the same batch through the same pipeline yields
    /// byte-identical grouped output across runs (BTreeMap ordering + preserved order).
    #[test]
    fn eg_165_determinism() {
        let pipe =
            Pipeline::new()
                .parse_json("body")
                .route("level", PipeValue::str("error"), "errors");
        let batch: Vec<Record> = (0..50)
            .map(|i| {
                let lvl = if i % 2 == 0 { "error" } else { "info" };
                rec(&[(
                    "body",
                    PipeValue::str(format!(r#"{{"level":"{lvl}","i":{i}}}"#)),
                )])
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

    /// CONCEPT:EG-KG.enrichment.cross-modal-enrichment-hook — the textual DSL form parses into an equivalent pipeline.
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

    /// CONCEPT:EG-KG.enrichment.cross-modal-enrichment-hook — a malformed textual line reports its line number; `enrich` is
    /// rejected in text (needs a closure).
    #[test]
    fn eg_165_textual_form_errors() {
        assert!(Pipeline::parse("bogus_op x")
            .unwrap_err()
            .contains("unknown stage"));
        assert!(Pipeline::parse("enrich a b")
            .unwrap_err()
            .contains("enrich"));
        let e = Pipeline::parse("parse_json a\ncoerce f nope").unwrap_err();
        assert!(e.contains("line 2"), "got: {e}");
    }

    /// CONCEPT:EG-KG.enrichment.cross-modal-enrichment-hook — the documented landing seam: a routed record batch lowers into
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
