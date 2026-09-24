//! UQL printer, modality and semantic clauses (UQL-04/UQL-06): OWL/SPARQL/SHACL,
//! federation, spatial, tensor, CEP, sensor fusion, time-series and probabilistic
//! spellings. Split from `wire_query_uql.rs` by concern (KISS); the contract is that
//! module's — each function is the exact inverse of its eg-plan parser production.

use super::*;

#[cfg(feature = "timeseries")]
pub(super) fn strings(items: &[String]) -> String {
    let parts: Vec<String> = items.iter().map(|s| uql_quote(s)).collect();
    format!("[{}]", parts.join(", "))
}

#[cfg(feature = "owl-plan")]
pub(super) fn sparql(query: &str, var: &str) -> Printed {
    Ok(format!(
        "SPARQL {} VAR {}",
        uql_quote(query),
        uql_quote(var)
    ))
}

#[cfg(feature = "geo")]
pub(super) fn spatial_scan(layer: &str, bbox: &[f64; 4]) -> Printed {
    Ok(format!(
        "SPATIAL SCAN {} BBOX {}",
        uql_quote(layer),
        list(bbox, |v| uql_num(*v))?
    ))
}

#[cfg(feature = "geo")]
pub(super) fn reproject(to_epsg: u32, from_epsg: Option<u32>) -> Printed {
    Ok(match from_epsg {
        Some(from) => format!("REPROJECT TO {to_epsg} FROM {from}"),
        None => format!("REPROJECT TO {to_epsg}"),
    })
}

#[cfg(feature = "timeseries")]
pub(super) fn sensor_fuse(streams: &[String], tolerance_ns: u64) -> Printed {
    Ok(format!(
        "SENSOR FUSE {} TOLERANCE {tolerance_ns}",
        strings(streams)
    ))
}

#[cfg(feature = "timeseries")]
pub(super) fn ts_scan(series: &[String], from: f64, to: f64) -> Printed {
    Ok(format!(
        "TSSCAN {} FROM {} TO {}",
        strings(series),
        uql_num(from)?,
        uql_num(to)?
    ))
}

/// `DERIVE expr AS name {, expr AS name}` (EH-522).
#[cfg(feature = "timeseries")]
pub(super) fn derive(columns: &[crate::series_expr::DeriveColumn]) -> Printed {
    let parts = columns
        .iter()
        .map(|c| Ok(format!("{} AS {}", uql_series_expr(&c.expr)?, uql_ident(&c.name))))
        .collect::<Result<Vec<_>, UqlPrintError>>()?;
    Ok(format!("DERIVE {}", parts.join(", ")))
}

/// The canonical spelling of a series expression — `func(args…, params…)`, channels as
/// identifiers, numbers in shortest round-trip form. The digest input of a derived column.
#[cfg(feature = "timeseries")]
pub fn uql_series_expr(expr: &crate::series_expr::SeriesExpr) -> Printed {
    use crate::series_expr::SeriesExpr;
    match expr {
        SeriesExpr::Channel { name } => Ok(uql_ident(name)),
        SeriesExpr::Const { value } => uql_num(*value),
        SeriesExpr::Call { func, args, params } => {
            let mut parts = args.iter().map(uql_series_expr).collect::<Result<Vec<_>, _>>()?;
            for p in params {
                parts.push(uql_num(*p)?);
            }
            Ok(format!("{}({})", func.signature().name, parts.join(", ")))
        }
    }
}

#[cfg(feature = "owl-plan")]
pub(super) fn reason(target_class: &str, ontology: &str) -> Printed {
    let class = iri_or_string(target_class);
    Ok(if ontology.is_empty() {
        format!("REASON {class}")
    } else {
        format!("REASON {class} ONTOLOGY {}", uql_quote(ontology))
    })
}

#[cfg(feature = "owl-plan")]
pub(super) fn validate_shape(shape: &str, shapes: &str, keep: ShapeKeep) -> Printed {
    let mut out = format!("VALIDATE SHAPE {}", iri_or_string(shape));
    if !shapes.is_empty() {
        out.push_str(&format!(" USING {}", uql_quote(shapes)));
    }
    if keep == ShapeKeep::Violating {
        out.push_str(" KEEP VIOLATING");
    }
    Ok(out)
}

/// An IRI token when `s` lexes as one, else a quoted string (both parse back to `s`).
#[cfg(feature = "owl-plan")]
pub(super) fn iri_or_string(s: &str) -> String {
    if is_iri_token(s) {
        s.to_string()
    } else {
        uql_quote(s)
    }
}

/// Would the UQL lexer read `s` back as ONE angle-bracketed IRI token?
#[cfg(feature = "owl-plan")]
pub(super) fn is_iri_token(s: &str) -> bool {
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
pub(super) fn foreign_scan(source: &ForeignSourceSpec, join: bool) -> Printed {
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
pub(super) fn spatial_op(kind: &SpatialOpKind) -> Printed {
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
pub(super) fn tensor_op(kind: &TensorOpKind) -> Printed {
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
pub(super) fn cep(spec: &CepPatternSpec) -> Printed {
    let window = match spec.window {
        CepWindowSpec::Sliding { size } => format!("SLIDING {size}"),
        CepWindowSpec::Tumbling { size } => format!("TUMBLING {size}"),
    };
    Ok(format!("CEP {} WINDOW {window}", cep_node(&spec.pattern)?))
}

#[cfg(feature = "stream")]
pub(super) fn cep_node(node: &CepNodeSpec) -> Printed {
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
pub(super) fn cep_matcher(m: &CepMatcherSpec) -> Printed {
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
pub(super) fn cep_pred(p: &CepAttrPredSpec) -> Printed {
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
pub(super) fn sensor_align(
    streams: &[FuseStream],
    clock: &FuseClock,
    tolerance_ns: Option<u64>,
) -> Printed {
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
    Ok(format!(
        "SENSOR ALIGN [{}] CLOCK {clock}{tolerance}",
        parts.join(", ")
    ))
}

#[cfg(feature = "probabilistic")]
pub(super) fn probabilistic(query: &ProbQuery) -> Printed {
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
