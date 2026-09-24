//! UQL PRINTER (UQL-06) — `Plan` → canonical UQL text.
//!
//! The inverse of `eg_plan::uql::parse`: for every plan the printer accepts,
//! `parse(print(plan)) == canonicalize(plan)` (proved by eg-plan's round-trip property
//! test and its every-variant walk). It lives HERE, beside the wire types, rather than in
//! eg-plan, because only this crate sees the exact feature set each `Op`/`Pred` variant is
//! compiled under: every match below is exhaustive with no catch-all, so adding a wire
//! variant is a compile error until it has a UQL spelling.
//!
//! The printer refuses — with a named [`UqlPrintCode`], never a lossy spelling — the few
//! values UQL deliberately cannot carry: non-finite numbers, credential-bearing foreign
//! specs (a secret or DSN must never be written into query text, which is logged, cached
//! and model-generated), an unknown window aggregate, and empty connectives.

use super::*;

/// Why a plan has no canonical UQL spelling.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum UqlPrintCode {
    /// NaN or ±∞ — UQL numbers are finite.
    NonFiniteNumber,
    /// `And`/`Or` with fewer than two members, an empty `IN` list, or a `Filter` with
    /// no predicates — shapes whose only spelling would re-parse to something else.
    DegenerateShape,
    /// A `ForeignScan` spec carrying a secret, signed context or DSN. Register the
    /// source (`RegisterForeignSource`) and use `FOREIGN SCAN '<name>'`.
    CredentialBearingSpec,
    /// A `WindowAgg` aggregate outside the closed set the executor recognizes.
    UnknownAggregate,
}

impl UqlPrintCode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::NonFiniteNumber => "UQL_PRINT_NON_FINITE_NUMBER",
            Self::DegenerateShape => "UQL_PRINT_DEGENERATE_SHAPE",
            Self::CredentialBearingSpec => "UQL_PRINT_CREDENTIAL_BEARING_SPEC",
            Self::UnknownAggregate => "UQL_PRINT_UNKNOWN_AGGREGATE",
        }
    }
}

/// A printer refusal: the code plus what was refused.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UqlPrintError {
    pub code: UqlPrintCode,
    pub detail: String,
}

impl std::fmt::Display for UqlPrintError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.code.as_str(), self.detail)
    }
}

pub(super) type Printed = Result<String, UqlPrintError>;

pub(super) fn refuse(code: UqlPrintCode, detail: impl Into<String>) -> UqlPrintError {
    UqlPrintError {
        code,
        detail: detail.into(),
    }
}

/// Every word the UQL grammar gives meaning to. An identifier that equals one of these
/// (case-insensitively) is printed back-quoted so it can never be read as the keyword.
/// eg-plan's grammar test asserts its keyword set is a subset of this list.
pub const UQL_RESERVED_WORDS: &[&str] = &[
    "ABSENCE",
    "ADD",
    "AGENT",
    "ALIGN",
    "AND",
    "AS",
    "ASOF_HOLD",
    "ASSEMBLE",
    "AT",
    "ATTRIBUTE",
    "AXIS",
    "BBOX",
    "BELIEF",
    "BERNOULLI",
    "BETWEEN",
    "BUFFER",
    "BY",
    "CENTROID",
    "CEP",
    "CLOCK",
    "CONDITIONAL",
    "CONFIDENCE",
    "CONFORMING",
    "CONTAINS",
    "CONTRADICTS",
    "CONVEX_HULL",
    "COVERS",
    "CROSSES",
    "DECIDE",
    "DECISIONS",
    "DIFFERENCE",
    "DISJOINT",
    "DIV",
    "DWITHIN",
    "ENGINE",
    "EQUALS",
    "EVIDENCE",
    "EXISTS",
    "EXPECTATION",
    "EXPLAIN",
    "FALSE",
    "FOR",
    "FOREIGN",
    "FROM",
    "FUSE",
    "GAUSSIAN",
    "HTTP",
    "ID",
    "IN",
    "INTERSECTION",
    "IS",
    "JOIN",
    "JSON",
    "JSONPATH",
    "K",
    "KEEP",
    "KEY",
    "KNOWLEDGE",
    "LABEL",
    "LET",
    "LIMIT",
    "LINEAR",
    "MARGINAL",
    "MATCH",
    "MAX",
    "MEAN",
    "MENTIONS",
    "MIN",
    "MMR",
    "MUL",
    "NEAREST",
    "NODE_DISTANCE",
    "NOT",
    "NULL",
    "OF",
    "ONTOLOGY",
    "OR",
    "OVERLAPS",
    "OWEN",
    "PATH",
    "PROB",
    "PROFILE",
    "PROOF",
    "RANK",
    "REASON",
    "REDUCE",
    "RELIABILITY",
    "REPROJECT",
    "RERANK",
    "RETURN",
    "SAMPLE",
    "SAMPLES",
    "SCAN",
    "SCORE",
    "SEED",
    "SENSOR",
    "SEQ",
    "SHAPE",
    "SHAPLEY",
    "SIMPLIFY",
    "SLICE",
    "SLIDING",
    "SOURCE",
    "SPARQL",
    "SPATIAL",
    "SQL",
    "STEP",
    "SUB",
    "SUM",
    "SUPPORTED",
    "TENSOR",
    "TEXT",
    "THEN",
    "TO",
    "TOLERANCE",
    "TOUCHES",
    "TRAVERSE",
    "TRUE",
    "TSSCAN",
    "TUMBLING",
    "TX",
    "UDF",
    "UNIFORM",
    "UNION",
    "UQL",
    "USING",
    "VALID",
    "VALIDATE",
    "VAR",
    "VARIANCE",
    "VIOLATING",
    "WHERE",
    "WIDTH",
    "WINDOW",
    "WITH",
    "WITHIN",
];

/// The aggregates `WindowAgg` accepts (the executor's closed set, canonical names).
pub const UQL_WINDOW_AGGREGATES: &[&str] = &["mean", "sum", "min", "max", "count", "first", "last"];

// ── lexical helpers ─────────────────────────────────────────────────────────────

/// A single-quoted UQL string literal; `'` doubles. Total over every `&str`.
pub fn uql_quote(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

/// An identifier: bare when it is a plain ASCII word that is not reserved, back-quoted
/// (`` ` `` doubles) otherwise. Total over every `&str`.
pub fn uql_ident(s: &str) -> String {
    let plain = s
        .chars()
        .next()
        .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
        && s.chars().all(|c| c.is_ascii_alphanumeric() || c == '_');
    let reserved = UQL_RESERVED_WORDS.iter().any(|w| w.eq_ignore_ascii_case(s));
    if plain && !reserved {
        s.to_string()
    } else {
        format!("`{}`", s.replace('`', "``"))
    }
}

/// A finite `f64` in the shortest text that parses back to the identical value.
pub fn uql_num(n: f64) -> Printed {
    if n.is_finite() {
        Ok(format!("{n}"))
    } else {
        Err(refuse(UqlPrintCode::NonFiniteNumber, format!("{n}")))
    }
}

/// A finite `f32`, printed in `f32`'s own shortest round-trip form (never widened).
pub fn uql_num32(n: f32) -> Printed {
    if n.is_finite() {
        Ok(format!("{n}"))
    } else {
        Err(refuse(UqlPrintCode::NonFiniteNumber, format!("{n}")))
    }
}

/// `KEYWORD 'quoted'` — the shape of every single-string clause.
pub(super) fn keyword_quoted(keyword: &str, value: &str) -> Printed {
    Ok(format!("{keyword} {}", uql_quote(value)))
}

pub(super) fn list<T>(items: &[T], each: impl Fn(&T) -> Printed) -> Printed {
    let parts = items.iter().map(each).collect::<Result<Vec<_>, _>>()?;
    Ok(format!("[{}]", parts.join(", ")))
}

/// A JSON literal: scalars in UQL scalar syntax, containers as `JSON '<text>'`.
pub fn uql_json(value: &serde_json::Value) -> Printed {
    Ok(match value {
        serde_json::Value::Null => "NULL".into(),
        serde_json::Value::Bool(b) => uql_bool(*b).into(),
        serde_json::Value::Number(n) => n.to_string(),
        serde_json::Value::String(s) => uql_quote(s),
        serde_json::Value::Array(_) | serde_json::Value::Object(_) => {
            format!("JSON {}", uql_quote(&value.to_string()))
        }
    })
}

pub(super) fn uql_bool(b: bool) -> &'static str {
    if b {
        "TRUE"
    } else {
        "FALSE"
    }
}

// ── plans ───────────────────────────────────────────────────────────────────────

impl Plan {
    /// Canonical UQL for this plan (one `|>`-separated pipeline). See the module docs
    /// for the round-trip contract and the refusals.
    pub fn to_uql(&self) -> Printed {
        let stages = self.ops.iter().map(uql_op).collect::<Result<Vec<_>, _>>()?;
        Ok(stages.join("\n  |> "))
    }
}

/// One op's canonical UQL clause.
pub fn uql_op(op: &Op) -> Printed {
    match op {
        Op::Scan { label } => scan(label),
        Op::ScanAll {} => fixed("MATCH ()"),
        Op::Filter { preds } => filter(preds),
        Op::Traverse { rel, min, max } => traverse(rel, *min, *max),
        Op::Expand {
            rel,
            dir,
            min,
            max,
            edge_preds,
        } => expand(rel.as_deref(), *dir, (*min, *max), edge_preds),
        Op::Rank { query } => rank(query),
        Op::RankEmbed { text } => keyword_quoted("RANK BY ~", text),
        Op::RankNodeDistance { center } => keyword_quoted("RERANK NODE_DISTANCE FROM", center),
        Op::RankMentions {} => fixed("RERANK MENTIONS"),
        Op::RankMmr { lambda, k } => rank_mmr(*lambda, *k),
        #[cfg(feature = "text")]
        Op::RankText { query } => keyword_quoted("TEXT", query),
        #[cfg(feature = "text")]
        Op::FuseRrf { branches, k } => fuse(branches, *k),
        #[cfg(feature = "owl-plan")]
        Op::Reason {
            target_class,
            ontology,
        } => super::wire_query_uql_modal::reason(target_class, ontology),
        #[cfg(feature = "owl-plan")]
        Op::SparqlBgp { query, var } => super::wire_query_uql_modal::sparql(query, var),
        #[cfg(feature = "owl-plan")]
        Op::ValidateShape {
            shape,
            shapes,
            keep,
        } => super::wire_query_uql_modal::validate_shape(shape, shapes, *keep),
        #[cfg(feature = "wasm-udf")]
        Op::Udf { id } => keyword_quoted("UDF", id),
        #[cfg(feature = "federation")]
        Op::ForeignScan { source, join } => {
            super::wire_query_uql_modal::foreign_scan(source, *join)
        }
        Op::AsOf { ts, axis } => as_of(*ts, *axis),
        Op::Window { secs } => window(*secs),
        Op::WindowAgg { secs, agg } => window_agg(*secs, agg),
        Op::Foreign { name } => keyword_quoted("FOREIGN", name),
        #[cfg(feature = "geo")]
        Op::SpatialScan { layer, bbox } => super::wire_query_uql_modal::spatial_scan(layer, bbox),
        #[cfg(feature = "geo")]
        Op::Reproject { to_epsg, from_epsg } => {
            super::wire_query_uql_modal::reproject(*to_epsg, *from_epsg)
        }
        #[cfg(feature = "geo")]
        Op::SpatialOp { kind } => super::wire_query_uql_modal::spatial_op(kind),
        #[cfg(feature = "tensor")]
        Op::TensorScan { layer } => keyword_quoted("TENSOR SCAN", layer),
        #[cfg(feature = "tensor")]
        Op::TensorOp { kind } => super::wire_query_uql_modal::tensor_op(kind),
        #[cfg(feature = "stream")]
        Op::Cep { pattern } => super::wire_query_uql_modal::cep(pattern),
        #[cfg(feature = "timeseries")]
        Op::SensorFuse {
            streams,
            tolerance_ns,
        } => super::wire_query_uql_modal::sensor_fuse(streams, *tolerance_ns),
        #[cfg(feature = "timeseries")]
        Op::SensorAlign {
            streams,
            clock,
            tolerance_ns,
        } => super::wire_query_uql_modal::sensor_align(streams, clock, *tolerance_ns),
        #[cfg(feature = "timeseries")]
        Op::TsScan { series, from, to } => super::wire_query_uql_modal::ts_scan(series, *from, *to),
        #[cfg(feature = "probabilistic")]
        Op::Probabilistic { query } => super::wire_query_uql_modal::probabilistic(query),
        #[cfg(feature = "epistemic")]
        Op::EvidenceFor { claim_id } => keyword_quoted("EVIDENCE FOR", claim_id),
        #[cfg(feature = "epistemic")]
        Op::Contradicts { node_id } => keyword_quoted("CONTRADICTS", node_id),
        #[cfg(feature = "epistemic")]
        Op::SupportedBy { node_id } => keyword_quoted("SUPPORTED BY", node_id),
        #[cfg(feature = "epistemic")]
        Op::BeliefAsOf { ts } => prefixed_num("BELIEF AS OF @", *ts),
        #[cfg(feature = "epistemic")]
        Op::SourceReliability { source_id } => keyword_quoted("SOURCE RELIABILITY", source_id),
        #[cfg(feature = "epistemic")]
        Op::ConfidenceOp {} => fixed("CONFIDENCE"),
        #[cfg(feature = "epistemic")]
        Op::ExplainBelief { node_id } => keyword_quoted("EXPLAIN BELIEF", node_id),
        Op::Attribute {
            input,
            value,
            method,
        } => super::wire_query_attribution::attribute(input, *value, method),
        Op::DecisionScan { preds } => decision_scan(preds),
        Op::Limit { k } => Ok(format!("LIMIT {k}")),
        Op::Project { channels } => project(channels),
    }
}

/// `DECISIONS [WHERE pred]` (EH-066).
fn decision_scan(preds: &[Pred]) -> Printed {
    if preds.is_empty() {
        return fixed("DECISIONS");
    }
    Ok(format!("DECISIONS WHERE {}", conjunction(preds)?))
}

/// A clause with no arguments.
fn fixed(text: &str) -> Printed {
    Ok(text.to_string())
}

/// `PREFIX<number>`.
fn prefixed_num(prefix: &str, n: f64) -> Printed {
    Ok(format!("{prefix}{}", uql_num(n)?))
}

fn scan(label: &str) -> Printed {
    Ok(format!("MATCH (:{})", uql_ident(label)))
}

fn traverse(rel: &str, min: usize, max: usize) -> Printed {
    Ok(format!("TRAVERSE -[:{}]->{{{min},{max}}}", uql_ident(rel)))
}

fn rank(query: &[f32]) -> Printed {
    Ok(format!("RANK BY ~{}", list(query, |v| uql_num32(*v))?))
}

fn rank_mmr(lambda: f32, k: usize) -> Printed {
    Ok(format!("RERANK MMR {} {k}", uql_num32(lambda)?))
}

fn window(secs: f64) -> Printed {
    Ok(format!("WINDOW {} s", uql_num(secs)?))
}

fn filter(preds: &[Pred]) -> Printed {
    if preds.is_empty() {
        return Err(refuse(
            UqlPrintCode::DegenerateShape,
            "Filter with no predicates",
        ));
    }
    Ok(format!("WHERE {}", conjunction(preds)?))
}

fn project(channels: &[String]) -> Printed {
    if channels.is_empty() {
        return Err(refuse(
            UqlPrintCode::DegenerateShape,
            "RETURN with no channels",
        ));
    }
    let names: Vec<String> = channels.iter().map(|c| uql_ident(c)).collect();
    Ok(format!("RETURN {}", names.join(", ")))
}

fn expand(rel: Option<&str>, dir: EdgeDir, hops: (usize, usize), edge_preds: &[Pred]) -> Printed {
    let rel = rel.map_or_else(|| "*".to_string(), |r| format!(":{}", uql_ident(r)));
    let cond = if edge_preds.is_empty() {
        String::new()
    } else {
        format!(" WHERE {}", conjunction(edge_preds)?)
    };
    let (open, close) = match dir {
        EdgeDir::Out => ("-[", "]->"),
        EdgeDir::In => ("<-[", "]-"),
        EdgeDir::Both => ("-[", "]-"),
    };
    Ok(format!(
        "TRAVERSE {open}{rel}{cond}{close}{{{},{}}}",
        hops.0, hops.1
    ))
}

fn as_of(ts: f64, axis: TimeAxis) -> Printed {
    let ts = uql_num(ts)?;
    Ok(match axis {
        TimeAxis::Valid => format!("AS OF @{ts}"),
        TimeAxis::Transaction => format!("AS OF TX @{ts}"),
    })
}

fn window_agg(secs: f64, agg: &str) -> Printed {
    if !UQL_WINDOW_AGGREGATES.contains(&agg) {
        return Err(refuse(UqlPrintCode::UnknownAggregate, agg));
    }
    Ok(format!(
        "WINDOW {} s {}",
        uql_num(secs)?,
        agg.to_ascii_uppercase()
    ))
}

#[cfg(feature = "text")]
fn fuse(branches: &[Vec<Op>], k: f32) -> Printed {
    if branches.is_empty() {
        return Err(refuse(UqlPrintCode::DegenerateShape, "FUSE with no branch"));
    }
    let mut out = String::from("FUSE");
    if k != 0.0 {
        out.push_str(&format!(" K {}", uql_num32(k)?));
    }
    for branch in branches {
        let stages = branch.iter().map(uql_op).collect::<Result<Vec<_>, _>>()?;
        out.push_str(&format!(" [{}]", stages.join(" |> ")));
    }
    Ok(out)
}
