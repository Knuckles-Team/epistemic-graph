//! Pipeline stage execution, builder construction, and textual DSL parsing.

use super::{CmpOp, CoerceType, PipeValue, Pipeline, Predicate, Record, Stage};

// ---- fluent builder sugar (CONCEPT:EG-KG.enrichment.cross-modal-enrichment-hook) ----

impl Pipeline {
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
}

// ---------------------------------------------------------------------------
// stage implementations
// ---------------------------------------------------------------------------

pub(super) fn apply_stage(stage: &Stage, rec: &mut Record, stream: &mut Option<String>) -> bool {
    match stage {
        Stage::Filter(predicate) => predicate.eval(rec),
        Stage::DropIf(predicate) => !predicate.eval(rec),
        stage @ (Stage::ParseJson { .. }
        | Stage::Set { .. }
        | Stage::Rename { .. }
        | Stage::Remove { .. }
        | Stage::Coerce { .. }) => {
            apply_mutation(stage, rec);
            true
        }
        Stage::Route {
            field,
            value,
            stream: target,
        } => {
            if rec.get(field) == Some(value) {
                *stream = Some(target.clone());
            }
            true
        }
        Stage::Enrich {
            source,
            target,
            lookup,
        } => {
            if let Some(value) = rec.get(source) {
                if let Some(enriched) = lookup.lookup(value) {
                    rec.insert(target.clone(), enriched);
                }
            }
            true
        }
    }
}

fn apply_mutation(stage: &Stage, rec: &mut Record) {
    match stage {
        Stage::ParseJson { field } => apply_parse_json(rec, field),
        Stage::Set { field, value } => {
            rec.insert(field.clone(), value.clone());
        }
        Stage::Rename { from, to } => {
            if let Some(value) = rec.remove(from) {
                rec.insert(to.clone(), value);
            }
        }
        Stage::Remove { field } => {
            rec.remove(field);
        }
        Stage::Coerce { field, ty } => apply_coerce(rec, field, *ty),
        _ => unreachable!("non-mutation stage routed to apply_mutation"),
    }
}

fn apply_parse_json(rec: &mut Record, field: &str) {
    let Some(PipeValue::Str(s)) = rec.get(field) else {
        return;
    };
    let Ok(parsed) = super::parse_json_value(s) else {
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
    let Some(value) = rec.get(field) else {
        return;
    };
    let Some(coerced) = coerce_value(value, ty) else {
        return;
    };
    rec.insert(field.to_string(), coerced);
}

fn coerce_value(value: &PipeValue, ty: CoerceType) -> Option<PipeValue> {
    match ty {
        CoerceType::Str => Some(match value {
            PipeValue::Str(text) => PipeValue::Str(text.clone()),
            other => PipeValue::Str(other.to_json()),
        }),
        CoerceType::I64 | CoerceType::F64 => coerce_number(value, ty),
        CoerceType::Bool => coerce_bool(value),
    }
}

fn coerce_number(value: &PipeValue, ty: CoerceType) -> Option<PipeValue> {
    match value {
        PipeValue::I64(number) => Some(ty.number_from_i64(*number)),
        PipeValue::F64(number) => Some(ty.number_from_f64(*number)),
        PipeValue::Bool(boolean) => Some(ty.number_from_bool(*boolean)),
        PipeValue::Str(text) => ty.number_from_text(text),
        _ => None,
    }
}

impl CoerceType {
    fn number_from_i64(self, number: i64) -> PipeValue {
        match self {
            CoerceType::I64 => PipeValue::I64(number),
            CoerceType::F64 => PipeValue::F64(number as f64),
            _ => unreachable!("non-numeric coercion requested from i64"),
        }
    }

    fn number_from_f64(self, number: f64) -> PipeValue {
        match self {
            CoerceType::I64 => PipeValue::I64(number as i64),
            CoerceType::F64 => PipeValue::F64(number),
            _ => unreachable!("non-numeric coercion requested from f64"),
        }
    }

    fn number_from_bool(self, boolean: bool) -> PipeValue {
        match self {
            CoerceType::I64 => PipeValue::I64(if boolean { 1 } else { 0 }),
            CoerceType::F64 => PipeValue::F64(if boolean { 1.0 } else { 0.0 }),
            _ => unreachable!("non-numeric coercion requested from bool"),
        }
    }

    fn number_from_text(self, text: &str) -> Option<PipeValue> {
        match self {
            CoerceType::I64 => text.trim().parse::<i64>().ok().map(PipeValue::I64),
            CoerceType::F64 => text.trim().parse::<f64>().ok().map(PipeValue::F64),
            _ => None,
        }
    }
}

fn coerce_bool(value: &PipeValue) -> Option<PipeValue> {
    match value {
        PipeValue::Bool(_) => Some(value.clone()),
        PipeValue::I64(number) => Some(PipeValue::Bool(*number != 0)),
        PipeValue::Str(text) => match text.trim().to_ascii_lowercase().as_str() {
            "true" | "1" | "yes" => Some(PipeValue::Bool(true)),
            "false" | "0" | "no" => Some(PipeValue::Bool(false)),
            _ => None,
        },
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// textual form parsing
// ---------------------------------------------------------------------------

pub(super) fn parse_text(text: &str) -> Result<Pipeline, String> {
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

fn parse_line(line: &str) -> Result<Stage, String> {
    let toks: Vec<&str> = line.split_whitespace().collect();
    let op = toks[0];
    match op {
        "parse_json" | "set" | "rename" | "remove" | "coerce" => {
            parse_mutation_stage(op, &toks[1..])
        }
        "filter" | "drop_if" | "route" => parse_conditional_stage(op, &toks[1..]),
        "enrich" => {
            Err("enrich is not expressible in the textual form; use Pipeline::enrich".into())
        }
        other => Err(format!("unknown stage '{other}'")),
    }
}

fn parse_mutation_stage(op: &str, toks: &[&str]) -> Result<Stage, String> {
    match op {
        "parse_json" => {
            let field = toks.first().ok_or("parse_json needs a <field>")?;
            Ok(Stage::ParseJson {
                field: (*field).to_string(),
            })
        }
        "set" => {
            let field = toks.first().ok_or("set needs <field> <value>")?;
            let value = toks.get(1).ok_or("set needs <field> <value>")?;
            Ok(Stage::Set {
                field: (*field).to_string(),
                value: parse_value_token(value),
            })
        }
        "rename" => {
            let from = toks.first().ok_or("rename needs <from> <to>")?;
            let to = toks.get(1).ok_or("rename needs <from> <to>")?;
            Ok(Stage::Rename {
                from: (*from).to_string(),
                to: (*to).to_string(),
            })
        }
        "remove" => {
            let field = toks.first().ok_or("remove needs a <field>")?;
            Ok(Stage::Remove {
                field: (*field).to_string(),
            })
        }
        "coerce" => {
            let field = toks.first().ok_or("coerce needs <field> <type>")?;
            let ty = match *toks.get(1).ok_or("coerce needs <field> <type>")? {
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
        _ => unreachable!("non-mutation stage routed to parse_mutation_stage"),
    }
}

fn parse_conditional_stage(op: &str, toks: &[&str]) -> Result<Stage, String> {
    match op {
        "filter" | "drop_if" => {
            let pred = parse_predicate(toks)?;
            Ok(if op == "filter" {
                Stage::Filter(pred)
            } else {
                Stage::DropIf(pred)
            })
        }
        "route" => {
            // route <field> <value> -> <stream>
            let arrow = toks
                .iter()
                .position(|t| *t == "->")
                .ok_or("route needs '-> <stream>'")?;
            if arrow < 2 {
                return Err("route needs <field> <value> -> <stream>".into());
            }
            let field = toks[0];
            let value = toks[1];
            let stream = toks
                .get(arrow + 1)
                .ok_or("route needs a <stream> after '->'")?;
            Ok(Stage::Route {
                field: field.to_string(),
                value: parse_value_token(value),
                stream: (*stream).to_string(),
            })
        }
        _ => unreachable!("non-conditional stage routed to parse_conditional_stage"),
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
/// `null`, else a string (CONCEPT:EG-KG.enrichment.cross-modal-enrichment-hook).
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
