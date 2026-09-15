//! SWRL built-in evaluation for rule body filters and producers.

use std::collections::HashMap;

use super::RTerm;

// ── SWRL built-in library (CONCEPT:EG-KG.ontology.concept-3) ───────────────────────────────────

/// The SWRL built-ins namespace.
const SWRLB_NS: &str = "http://www.w3.org/2003/11/swrlb#";

/// If `pred` names a SWRL built-in (`swrlb:name` short form OR the full
/// `<http://www.w3.org/2003/11/swrlb#name>` IRI), return its bare `name`; else `None`.
/// Gating on the `swrlb` prefix/IRI keeps recognition disjoint from ordinary fact
/// predicates, so non-built-in atoms are matched against facts exactly as before.
pub(super) fn swrl_builtin_name(pred: &str) -> Option<&str> {
    if let Some(n) = pred.strip_prefix("swrlb:") {
        return Some(n);
    }
    let bare = pred.trim_start_matches('<').trim_end_matches('>');
    bare.strip_prefix(SWRLB_NS)
}

/// Evaluate a SWRL built-in atom against the current `binding` (CONCEPT:EG-KG.ontology.concept-3).
///
/// Returns `None` when the built-in does NOT hold for this binding (the join path dies);
/// `Some(extra)` when it holds, where `extra` carries any output variable the built-in
/// freshly bound (the SWRL convention: math / string producers write their first
/// argument). Comparison built-ins return `Some(vec![])`. An unknown built-in name or a
/// missing/unbound required argument yields `None` (sound — it simply never fires).
pub(super) fn eval_builtin(
    name: &str,
    args: &[RTerm],
    binding: &HashMap<String, String>,
) -> Option<Vec<(String, String)>> {
    // Resolve the i-th argument to its current string value (None ⇒ unbound variable).
    let val = |i: usize| -> Option<String> {
        args.get(i).and_then(|a| match a {
            RTerm::Const(c) => Some(c.clone()),
            RTerm::Var(v) => binding.get(v).cloned(),
        })
    };
    let num = |i: usize| -> Option<f64> { val(i).and_then(|s| s.parse::<f64>().ok()) };

    match name {
        // ── comparisons (filters; every argument must be bound) ──────────────
        "equal" | "notEqual" => eval_builtin_equality(name, val(0)?, val(1)?),
        "lessThan" | "lessThanOrEqual" | "greaterThan" | "greaterThanOrEqual" => {
            eval_builtin_ordering(name, num(0)?, num(1)?)
        }
        "contains" | "startsWith" | "endsWith" => eval_builtin_string_test(name, val(0)?, val(1)?),
        // ── math (first argument = result) ───────────────────────────────────
        "add" | "subtract" | "multiply" | "divide" => eval_builtin_math(name, args, binding),
        // ── string producers (first argument = result) ──────────────────────
        "stringConcat" => {
            let parts: Vec<String> = (1..args.len()).map(val).collect::<Option<_>>()?;
            bind_or_check_str(args.first()?, parts.concat(), binding)
        }
        "stringLength" => {
            let s = val(1)?;
            bind_or_check_num(args.first()?, s.chars().count() as f64, binding)
        }
        "upperCase" => bind_or_check_str(args.first()?, val(1)?.to_uppercase(), binding),
        "lowerCase" => bind_or_check_str(args.first()?, val(1)?.to_lowercase(), binding),
        _ => None,
    }
}

/// `equal`/`notEqual`: numeric equality when BOTH arguments parse as numbers (so
/// `18 == 18.0`), else string equality. Extracted from [`eval_builtin`]'s match.
fn eval_builtin_equality(name: &str, a: String, b: String) -> Option<Vec<(String, String)>> {
    let eq = match (a.parse::<f64>(), b.parse::<f64>()) {
        (Ok(x), Ok(y)) => (x - y).abs() < 1e-9,
        _ => a == b,
    };
    ((name == "equal") == eq).then(Vec::new)
}

/// `lessThan`/`lessThanOrEqual`/`greaterThan`/`greaterThanOrEqual`. Extracted from
/// [`eval_builtin`]'s match.
fn eval_builtin_ordering(name: &str, a: f64, b: f64) -> Option<Vec<(String, String)>> {
    let ok = match name {
        "lessThan" => a < b,
        "lessThanOrEqual" => a <= b,
        "greaterThan" => a > b,
        _ => a >= b,
    };
    ok.then(Vec::new)
}

/// `contains`/`startsWith`/`endsWith`. Extracted from [`eval_builtin`]'s match.
fn eval_builtin_string_test(name: &str, a: String, b: String) -> Option<Vec<(String, String)>> {
    let ok = match name {
        "contains" => a.contains(&b),
        "startsWith" => a.starts_with(&b),
        _ => a.ends_with(&b),
    };
    ok.then(Vec::new)
}

/// `add`/`subtract`/`multiply`/`divide` (first argument = result; inputs are
/// `args[1..]`, all must be bound + numeric). Extracted from [`eval_builtin`]'s match.
fn eval_builtin_math(
    name: &str,
    args: &[RTerm],
    binding: &HashMap<String, String>,
) -> Option<Vec<(String, String)>> {
    let mut inputs = Vec::new();
    for index in 1..args.len() {
        inputs.push(numeric_arg(args, binding, index)?);
    }
    if inputs.is_empty() {
        return None;
    }
    let result = match name {
        "add" => inputs.iter().sum(),
        "multiply" => inputs.iter().product(),
        "subtract" => {
            if inputs.len() != 2 {
                return None;
            }
            inputs[0] - inputs[1]
        }
        _ => {
            if inputs.len() != 2 || inputs[1] == 0.0 {
                return None;
            }
            inputs[0] / inputs[1]
        }
    };
    bind_or_check_num(args.first()?, result, binding)
}

fn numeric_arg(args: &[RTerm], binding: &HashMap<String, String>, index: usize) -> Option<f64> {
    args.get(index).and_then(|arg| match arg {
        RTerm::Const(value) => value.parse::<f64>().ok(),
        RTerm::Var(name) => binding
            .get(name)
            .and_then(|value| value.parse::<f64>().ok()),
    })
}

/// Bind a producer built-in's result `out` to a numeric `value` (when an unbound
/// variable), or verify equality (numeric) when it is already bound / a constant.
fn bind_or_check_num(
    out: &RTerm,
    value: f64,
    binding: &HashMap<String, String>,
) -> Option<Vec<(String, String)>> {
    let check = |s: &str| matches!(s.parse::<f64>(), Ok(b) if (b - value).abs() < 1e-9);
    match out {
        RTerm::Var(v) => match binding.get(v) {
            None => Some(vec![(v.clone(), fmt_num(value))]),
            Some(bound) => check(bound).then(Vec::new),
        },
        RTerm::Const(c) => check(c).then(Vec::new),
    }
}

/// Bind a producer built-in's result `out` to a string `value`, or verify equality.
fn bind_or_check_str(
    out: &RTerm,
    value: String,
    binding: &HashMap<String, String>,
) -> Option<Vec<(String, String)>> {
    match out {
        RTerm::Var(v) => match binding.get(v) {
            None => Some(vec![(v.clone(), value)]),
            Some(bound) => (bound == &value).then(Vec::new),
        },
        RTerm::Const(c) => (c == &value).then(Vec::new),
    }
}

/// Render a built-in numeric result: an integral value as a plain integer (`19`, not
/// `19.0`) so it round-trips with literal facts; otherwise the default float form.
fn fmt_num(f: f64) -> String {
    if f.is_finite() && f.fract() == 0.0 && f.abs() < 1e15 {
        format!("{}", f as i64)
    } else {
        format!("{f}")
    }
}
