//! UQL PRINTER (EH-370) — `Plan` → canonical UQL text.
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

type Printed = Result<String, UqlPrintError>;

fn refuse(code: UqlPrintCode, detail: impl Into<String>) -> UqlPrintError {
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
    "SCAN",
    "SCORE",
    "SEED",
    "SENSOR",
    "SEQ",
    "SHAPE",
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

fn list<T>(items: &[T], each: impl Fn(&T) -> Printed) -> Printed {
    let parts = items.iter().map(each).collect::<Result<Vec<_>, _>>()?;
    Ok(format!("[{}]", parts.join(", ")))
}

fn strings(items: &[String]) -> String {
    let parts: Vec<String> = items.iter().map(|s| uql_quote(s)).collect();
    format!("[{}]", parts.join(", "))
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

fn uql_bool(b: bool) -> &'static str {
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
        Op::Scan { label } => Ok(format!("MATCH (:{})", uql_ident(label))),
        Op::ScanAll {} => Ok("MATCH ()".into()),
        Op::Filter { preds } => filter(preds),
        Op::Traverse { rel, min, max } => {
            Ok(format!("TRAVERSE -[:{}]->{{{min},{max}}}", uql_ident(rel)))
        }
        Op::Expand {
            rel,
            dir,
            min,
            max,
            edge_preds,
        } => expand(rel.as_deref(), *dir, (*min, *max), edge_preds),
        Op::Rank { query } => Ok(format!("RANK BY ~{}", list(query, |v| uql_num32(*v))?)),
        Op::RankEmbed { text } => Ok(format!("RANK BY ~{}", uql_quote(text))),
        Op::RankNodeDistance { center } => {
            Ok(format!("RERANK NODE_DISTANCE FROM {}", uql_quote(center)))
        }
        Op::RankMentions {} => Ok("RERANK MENTIONS".into()),
        Op::RankMmr { lambda, k } => Ok(format!("RERANK MMR {} {k}", uql_num32(*lambda)?)),
        #[cfg(feature = "text")]
        Op::RankText { query } => Ok(format!("TEXT {}", uql_quote(query))),
        #[cfg(feature = "text")]
        Op::FuseRrf { branches, k } => fuse(branches, *k),
        #[cfg(feature = "owl-plan")]
        Op::Reason {
            target_class,
            ontology,
        } => reason(target_class, ontology),
        #[cfg(feature = "owl-plan")]
        Op::SparqlBgp { query, var } => Ok(format!(
            "SPARQL {} VAR {}",
            uql_quote(query),
            uql_quote(var)
        )),
        #[cfg(feature = "owl-plan")]
        Op::ValidateShape {
            shape,
            shapes,
            keep,
        } => Ok(validate_shape(shape, shapes, *keep)),
        #[cfg(feature = "wasm-udf")]
        Op::Udf { id } => Ok(format!("UDF {}", uql_quote(id))),
        #[cfg(feature = "federation")]
        Op::ForeignScan { source, join } => foreign_scan(source, *join),
        Op::AsOf { ts, axis } => as_of(*ts, *axis),
        Op::Window { secs } => Ok(format!("WINDOW {} s", uql_num(*secs)?)),
        Op::WindowAgg { secs, agg } => window_agg(*secs, agg),
        Op::Foreign { name } => Ok(format!("FOREIGN {}", uql_quote(name))),
        #[cfg(feature = "geo")]
        Op::SpatialScan { layer, bbox } => Ok(format!(
            "SPATIAL SCAN {} BBOX {}",
            uql_quote(layer),
            list(bbox, |v| uql_num(*v))?
        )),
        #[cfg(feature = "geo")]
        Op::Reproject { to_epsg, from_epsg } => Ok(match from_epsg {
            Some(from) => format!("REPROJECT TO {to_epsg} FROM {from}"),
            None => format!("REPROJECT TO {to_epsg}"),
        }),
        #[cfg(feature = "geo")]
        Op::SpatialOp { kind } => spatial_op(kind),
        #[cfg(feature = "tensor")]
        Op::TensorScan { layer } => Ok(format!("TENSOR SCAN {}", uql_quote(layer))),
        #[cfg(feature = "tensor")]
        Op::TensorOp { kind } => tensor_op(kind),
        #[cfg(feature = "stream")]
        Op::Cep { pattern } => cep(pattern),
        #[cfg(feature = "timeseries")]
        Op::SensorFuse {
            streams,
            tolerance_ns,
        } => Ok(format!(
            "SENSOR FUSE {} TOLERANCE {tolerance_ns}",
            strings(streams)
        )),
        #[cfg(feature = "timeseries")]
        Op::SensorAlign {
            streams,
            clock,
            tolerance_ns,
        } => Ok(sensor_align(streams, clock, *tolerance_ns)),
        #[cfg(feature = "timeseries")]
        Op::TsScan { series, from, to } => Ok(format!(
            "TSSCAN {} FROM {} TO {}",
            strings(series),
            uql_num(*from)?,
            uql_num(*to)?
        )),
        #[cfg(feature = "probabilistic")]
        Op::Probabilistic { query } => probabilistic(query),
        #[cfg(feature = "epistemic")]
        Op::EvidenceFor { claim_id } => Ok(format!("EVIDENCE FOR {}", uql_quote(claim_id))),
        #[cfg(feature = "epistemic")]
        Op::Contradicts { node_id } => Ok(format!("CONTRADICTS {}", uql_quote(node_id))),
        #[cfg(feature = "epistemic")]
        Op::SupportedBy { node_id } => Ok(format!("SUPPORTED BY {}", uql_quote(node_id))),
        #[cfg(feature = "epistemic")]
        Op::BeliefAsOf { ts } => Ok(format!("BELIEF AS OF @{}", uql_num(*ts)?)),
        #[cfg(feature = "epistemic")]
        Op::SourceReliability { source_id } => {
            Ok(format!("SOURCE RELIABILITY {}", uql_quote(source_id)))
        }
        #[cfg(feature = "epistemic")]
        Op::ConfidenceOp {} => Ok("CONFIDENCE".into()),
        #[cfg(feature = "epistemic")]
        Op::ExplainBelief { node_id } => Ok(format!("EXPLAIN BELIEF {}", uql_quote(node_id))),
        Op::Limit { k } => Ok(format!("LIMIT {k}")),
        Op::Project { channels } => project(channels),
    }
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

#[cfg(feature = "owl-plan")]
fn reason(target_class: &str, ontology: &str) -> Printed {
    let class = iri_or_string(target_class);
    Ok(if ontology.is_empty() {
        format!("REASON {class}")
    } else {
        format!("REASON {class} ONTOLOGY {}", uql_quote(ontology))
    })
}

#[cfg(feature = "owl-plan")]
fn validate_shape(shape: &str, shapes: &str, keep: ShapeKeep) -> String {
    let mut out = format!("VALIDATE SHAPE {}", iri_or_string(shape));
    if !shapes.is_empty() {
        out.push_str(&format!(" USING {}", uql_quote(shapes)));
    }
    if keep == ShapeKeep::Violating {
        out.push_str(" KEEP VIOLATING");
    }
    out
}

/// An IRI token when `s` lexes as one, else a quoted string (both parse back to `s`).
#[cfg(feature = "owl-plan")]
fn iri_or_string(s: &str) -> String {
    if is_iri_token(s) {
        s.to_string()
    } else {
        uql_quote(s)
    }
}

/// Would the UQL lexer read `s` back as ONE angle-bracketed IRI token?
#[cfg(feature = "owl-plan")]
fn is_iri_token(s: &str) -> bool {
    let Some(body) = s.strip_prefix('<').and_then(|r| r.strip_suffix('>')) else {
        return false;
    };
    !body.is_empty()
        && body.contains(':')
        && !body
            .chars()
            .any(|c| c.is_whitespace() || c == '>' || c == '<')
}

#[cfg(feature = "federation")]
fn foreign_scan(source: &ForeignSourceSpec, join: bool) -> Printed {
    let join = if join { " JOIN" } else { "" };
    match source {
        ForeignSourceSpec::Named { name } => Ok(format!("FOREIGN SCAN {}{join}", uql_quote(name))),
        ForeignSourceSpec::HttpJson {
            url,
            json_path,
            field_map,
        } => {
            let path = if json_path.is_empty() {
                String::new()
            } else {
                format!(" PATH {}", uql_quote(json_path))
            };
            let score = field_map
                .score
                .as_deref()
                .map_or_else(String::new, |s| format!(" SCORE {}", uql_quote(s)));
            Ok(format!(
                "FOREIGN HTTP {}{path} ID {}{score}{join}",
                uql_quote(url),
                uql_quote(&field_map.id)
            ))
        }
        ForeignSourceSpec::RemoteEngine { .. } | ForeignSourceSpec::Sql { .. } => Err(refuse(
            UqlPrintCode::CredentialBearingSpec,
            "a remote-engine or SQL foreign spec carries credentials; register it and use \
             FOREIGN SCAN '<name>'",
        )),
    }
}

#[cfg(feature = "geo")]
fn spatial_op(kind: &SpatialOpKind) -> Printed {
    Ok(match kind {
        SpatialOpKind::Buffer { distance } => format!("SPATIAL BUFFER {}", uql_num(*distance)?),
        SpatialOpKind::ConvexHull => "SPATIAL CONVEX_HULL".into(),
        SpatialOpKind::Simplify { tolerance } => {
            format!("SPATIAL SIMPLIFY {}", uql_num(*tolerance)?)
        }
        SpatialOpKind::Centroid => "SPATIAL CENTROID".into(),
        SpatialOpKind::Union { wkt } => format!("SPATIAL UNION {}", uql_quote(wkt)),
        SpatialOpKind::Intersection { wkt } => format!("SPATIAL INTERSECTION {}", uql_quote(wkt)),
        SpatialOpKind::Difference { wkt } => format!("SPATIAL DIFFERENCE {}", uql_quote(wkt)),
    })
}

#[cfg(feature = "tensor")]
fn tensor_op(kind: &TensorOpKind) -> Printed {
    Ok(match kind {
        TensorOpKind::Slice { ranges } => {
            let parts: Vec<String> = ranges.iter().map(|(a, b)| format!("{a}:{b}")).collect();
            format!("TENSOR SLICE [{}]", parts.join(", "))
        }
        TensorOpKind::Reduce { axis, kind } => {
            let name = match kind {
                TensorReduceKind::Sum => "SUM",
                TensorReduceKind::Mean => "MEAN",
                TensorReduceKind::Max => "MAX",
                TensorReduceKind::Min => "MIN",
            };
            format!("TENSOR REDUCE {name} AXIS {axis}")
        }
        TensorOpKind::Elementwise { op, scalar } => {
            let name = match op {
                TensorElementwiseOp::Add => "ADD",
                TensorElementwiseOp::Sub => "SUB",
                TensorElementwiseOp::Mul => "MUL",
                TensorElementwiseOp::Div => "DIV",
            };
            format!("TENSOR {name} {}", uql_num(*scalar)?)
        }
    })
}

#[cfg(feature = "stream")]
fn cep(spec: &CepPatternSpec) -> Printed {
    let window = match spec.window {
        CepWindowSpec::Sliding { size } => format!("SLIDING {size}"),
        CepWindowSpec::Tumbling { size } => format!("TUMBLING {size}"),
    };
    Ok(format!("CEP {} WINDOW {window}", cep_node(&spec.pattern)?))
}

#[cfg(feature = "stream")]
fn cep_node(node: &CepNodeSpec) -> Printed {
    match node {
        CepNodeSpec::Sequence(matchers) => {
            let parts = matchers
                .iter()
                .map(cep_matcher)
                .collect::<Result<Vec<_>, _>>()?;
            Ok(format!("SEQ ({})", parts.join(", ")))
        }
        CepNodeSpec::Within { within, pattern } => {
            Ok(format!("WITHIN {within} ({})", cep_node(pattern)?))
        }
        CepNodeSpec::Absence { a, b, within } => Ok(format!(
            "ABSENCE {} THEN NOT {} WITHIN {within}",
            cep_matcher(a)?,
            cep_matcher(b)?
        )),
    }
}

#[cfg(feature = "stream")]
fn cep_matcher(m: &CepMatcherSpec) -> Printed {
    let mut parts = Vec::new();
    if let Some(key) = &m.key {
        parts.push(format!("KEY {}", uql_quote(key)));
    }
    if !m.preds.is_empty() {
        let preds = m
            .preds
            .iter()
            .map(cep_pred)
            .collect::<Result<Vec<_>, _>>()?;
        parts.push(format!("WHERE {}", preds.join(" AND ")));
    }
    Ok(format!("{{{}}}", parts.join(" ")))
}

#[cfg(feature = "stream")]
fn cep_pred(p: &CepAttrPredSpec) -> Printed {
    Ok(match p {
        CepAttrPredSpec::Eq { field, value } => {
            format!("{} = {}", uql_ident(field), uql_json(value)?)
        }
        CepAttrPredSpec::Gt { field, value } => {
            format!("{} > {}", uql_ident(field), uql_num(*value)?)
        }
        CepAttrPredSpec::Lt { field, value } => {
            format!("{} < {}", uql_ident(field), uql_num(*value)?)
        }
        CepAttrPredSpec::Exists { field } => format!("{} EXISTS", uql_ident(field)),
    })
}

#[cfg(feature = "timeseries")]
fn sensor_align(streams: &[FuseStream], clock: &FuseClock, tolerance_ns: Option<u64>) -> String {
    let parts: Vec<String> = streams
        .iter()
        .map(|s| {
            let interp = match s.interp {
                FuseInterp::Nearest => "NEAREST",
                FuseInterp::Linear => "LINEAR",
                FuseInterp::AsofHold => "ASOF_HOLD",
            };
            format!("{} {interp}", uql_quote(&s.layer))
        })
        .collect();
    let clock = match clock {
        FuseClock::Uniform {
            from_ns,
            to_ns,
            step_ns,
        } => format!("UNIFORM FROM {from_ns} TO {to_ns} STEP {step_ns}"),
        FuseClock::Tumbling { width_ns, step_ns } => {
            format!("TUMBLING WIDTH {width_ns} STEP {step_ns}")
        }
    };
    let tolerance = tolerance_ns.map_or_else(String::new, |t| format!(" TOLERANCE {t}"));
    format!(
        "SENSOR ALIGN [{}] CLOCK {clock}{tolerance}",
        parts.join(", ")
    )
}

#[cfg(feature = "probabilistic")]
fn probabilistic(query: &ProbQuery) -> Printed {
    Ok(match query {
        ProbQuery::Expectation => "PROB EXPECTATION".into(),
        ProbQuery::Marginal { at, label } => {
            let label = label
                .as_deref()
                .map_or_else(String::new, |l| format!(" LABEL {}", uql_quote(l)));
            format!("PROB MARGINAL AT {}{label}", uql_num(*at)?)
        }
        ProbQuery::Conditional { evidence } => match evidence {
            ProbEvidenceSpec::Bernoulli {
                successes,
                failures,
            } => format!(
                "PROB CONDITIONAL BERNOULLI {} {}",
                uql_num(*successes)?,
                uql_num(*failures)?
            ),
            ProbEvidenceSpec::Gaussian {
                observations,
                known_variance,
            } => format!(
                "PROB CONDITIONAL GAUSSIAN {} VARIANCE {}",
                list(observations, |v| uql_num(*v))?,
                uql_num(*known_variance)?
            ),
        },
        ProbQuery::Sample { seed } => format!("PROB SAMPLE SEED {seed}"),
    })
}

// ── predicates ──────────────────────────────────────────────────────────────────

/// A top-level `AND` list (a `Filter`'s `preds`): each member is an operand, so a
/// nested connective is parenthesised and re-parses as the same single node.
pub fn conjunction(preds: &[Pred]) -> Printed {
    let parts = preds.iter().map(operand).collect::<Result<Vec<_>, _>>()?;
    Ok(parts.join(" AND "))
}

/// A predicate in operand position: connectives are parenthesised, atoms bare.
fn operand(pred: &Pred) -> Printed {
    let text = uql_pred(pred)?;
    Ok(if matches!(pred, Pred::And { .. } | Pred::Or { .. }) {
        format!("({text})")
    } else {
        text
    })
}

fn connective(preds: &[Pred], word: &str) -> Printed {
    if preds.len() < 2 {
        return Err(refuse(
            UqlPrintCode::DegenerateShape,
            format!("{word} with fewer than two members"),
        ));
    }
    let parts = preds.iter().map(operand).collect::<Result<Vec<_>, _>>()?;
    Ok(parts.join(&format!(" {word} ")))
}

/// One predicate's canonical UQL.
pub fn uql_pred(pred: &Pred) -> Printed {
    match pred {
        Pred::Eq { prop, value } => Ok(format!("{} = {}", uql_ident(prop), uql_quote(value))),
        Pred::GtNum { prop, n } => Ok(format!("{} > {}", uql_ident(prop), uql_num(*n)?)),
        Pred::LtNum { prop, n } => Ok(format!("{} < {}", uql_ident(prop), uql_num(*n)?)),
        Pred::Cmp { prop, op, value } => Ok(format!(
            "{} {} {}",
            uql_ident(prop),
            cmp_op(*op),
            uql_scalar(value)?
        )),
        Pred::In { prop, values } => in_list(prop, values),
        Pred::Between { prop, lo, hi } => Ok(format!(
            "{} BETWEEN {} AND {}",
            uql_ident(prop),
            uql_scalar(lo)?,
            uql_scalar(hi)?
        )),
        Pred::IsNull { prop } => Ok(format!("{} IS NULL", uql_ident(prop))),
        Pred::And { preds } => connective(preds, "AND"),
        Pred::Or { preds } => connective(preds, "OR"),
        Pred::Not { pred } => Ok(format!("NOT {}", operand(pred)?)),
        Pred::JsonPath { path, op } => json_path(path, op),
        #[cfg(feature = "geo")]
        Pred::SpatialWithin { column, wkt } => spatial_pred("WITHIN", column, wkt, None),
        #[cfg(feature = "geo")]
        Pred::SpatialDWithin {
            column,
            wkt,
            distance,
        } => spatial_pred("DWITHIN", column, wkt, Some(*distance)),
        #[cfg(feature = "geo")]
        Pred::SpatialContains { column, wkt } => spatial_pred("CONTAINS", column, wkt, None),
        #[cfg(feature = "geo")]
        Pred::SpatialCovers { column, wkt } => spatial_pred("COVERS", column, wkt, None),
        #[cfg(feature = "geo")]
        Pred::SpatialTouches { column, wkt } => spatial_pred("TOUCHES", column, wkt, None),
        #[cfg(feature = "geo")]
        Pred::SpatialCrosses { column, wkt } => spatial_pred("CROSSES", column, wkt, None),
        #[cfg(feature = "geo")]
        Pred::SpatialOverlaps { column, wkt } => spatial_pred("OVERLAPS", column, wkt, None),
        #[cfg(feature = "geo")]
        Pred::SpatialEquals { column, wkt } => spatial_pred("EQUALS", column, wkt, None),
        #[cfg(feature = "geo")]
        Pred::SpatialDisjoint { column, wkt } => spatial_pred("DISJOINT", column, wkt, None),
    }
}

/// The UQL spelling of a comparison operator.
pub fn cmp_op(op: CmpOp) -> &'static str {
    match op {
        CmpOp::Eq => "=",
        CmpOp::Ne => "!=",
        CmpOp::Gt => ">",
        CmpOp::Ge => ">=",
        CmpOp::Lt => "<",
        CmpOp::Le => "<=",
    }
}

/// A typed scalar literal.
pub fn uql_scalar(value: &Scalar) -> Printed {
    match value {
        Scalar::Str(s) => Ok(uql_quote(s)),
        Scalar::Num(n) => uql_num(*n),
        Scalar::Bool(b) => Ok(uql_bool(*b).into()),
    }
}

fn in_list(prop: &str, values: &[Scalar]) -> Printed {
    if values.is_empty() {
        return Err(refuse(
            UqlPrintCode::DegenerateShape,
            "IN with an empty list",
        ));
    }
    let parts = values
        .iter()
        .map(uql_scalar)
        .collect::<Result<Vec<_>, _>>()?;
    Ok(format!("{} IN ({})", uql_ident(prop), parts.join(", ")))
}

fn json_path(path: &str, op: &JsonPathOp) -> Printed {
    let path = if is_path_token(path) {
        path.to_string()
    } else {
        format!("JSONPATH {}", uql_quote(path))
    };
    Ok(match op {
        JsonPathOp::Exists => format!("{path} EXISTS"),
        JsonPathOp::Eq { value } => format!("{path} = {}", uql_json(value)?),
        JsonPathOp::Contains { value } => format!("{path} @> {}", uql_json(value)?),
    })
}

/// Would the UQL lexer read `s` back as ONE bare JSONPath token (`$.a.b[0]`)?
pub fn is_path_token(s: &str) -> bool {
    let mut chars = s.chars();
    chars.next() == Some('$')
        && matches!(chars.next(), Some('.') | Some('['))
        && s.chars()
            .all(|c| c.is_alphanumeric() || "$._[]*".contains(c))
}

#[cfg(feature = "geo")]
fn spatial_pred(rel: &str, column: &str, wkt: &str, distance: Option<f64>) -> Printed {
    let tail = match distance {
        Some(d) => format!(", {}", uql_num(d)?),
        None => String::new(),
    };
    Ok(format!(
        "SPATIAL {rel}({}, {}{tail})",
        uql_ident(column),
        uql_quote(wkt)
    ))
}
