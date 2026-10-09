use super::*;

pub(super) fn eval_filter(ctx: &Ctx, expr: &Expression, sol: &Solution) -> bool {
    eval_expr_bool(ctx, expr, sol).unwrap_or(false)
}

// Rich FILTER expression evaluation (CONCEPT:EG-KG.ontology.rich-filter). The evaluator has three layers:
//   * `eval_term`     — evaluates ANY expression to a typed term `Binding` (Node vs
//                       Literal), preserving the type info `isIRI`/`STR`/`DATATYPE` need.
//   * `eval_expr_bool`— the boolean (FILTER) layer: logical ops, comparisons, `IN`,
//                       `IF`, `COALESCE`, and the boolean built-in `FunctionCall`s.
//   * `expr_str`/`num`— scalar projections over `eval_term`, used by BIND/aggregates.
// Datatype-aware where feasible (numeric comparison/equality); unsupported forms still
// fail SAFE (the FILTER yields `false` / the bind yields no value).

pub(super) fn eval_expr_bool(ctx: &Ctx, expr: &Expression, sol: &Solution) -> Option<bool> {
    match expr {
        Expression::Bound(v) => Some(sol.contains_key(v.as_str())),
        Expression::Equal(..)
        | Expression::SameTerm(..)
        | Expression::Greater(..)
        | Expression::GreaterOrEqual(..)
        | Expression::Less(..)
        | Expression::LessOrEqual(..) => eval_expr_bool_compare(ctx, expr, sol),
        Expression::And(..) | Expression::Or(..) | Expression::Not(..) => {
            eval_expr_bool_logic(ctx, expr, sol)
        }
        // `IN` (and `NOT IN`, which spargebra parses to `Not(In(…))`) — numeric-aware
        // membership over the candidate list.
        Expression::In(a, list) => eval_expr_bool_in(ctx, a, list, sol),
        Expression::If(c, t, e) => eval_expr_bool_if(ctx, c, t, e, sol),
        Expression::Coalesce(args) => args.iter().find_map(|e| eval_expr_bool(ctx, e, sol)),
        Expression::FunctionCall(f, args) => eval_bool_function(ctx, f, args, sol),
        // FILTER EXISTS / NOT EXISTS (CONCEPT:EG-KG.ontology.order-by-values-exists). `NOT EXISTS` parses to
        // `Not(Exists(…))`, so the negation is handled by the `Not` arm above. Evaluate
        // the sub-pattern under the active context and report whether ANY of its solutions
        // is COMPATIBLE with the current solution (agrees on the shared variables) — the
        // substitution-and-nonempty semantics of EXISTS.
        Expression::Exists(pattern) => eval_expr_bool_exists(ctx, pattern, sol),
        // Effective boolean value of any other value-producing expression.
        _ => eval_term(ctx, expr, sol).map(|b| ebv(&b)),
    }
}

/// The comparison arms of [`eval_expr_bool`]: `=`, `sameTerm`, and the ordering
/// operators.
pub(super) fn eval_expr_bool_compare(ctx: &Ctx, expr: &Expression, sol: &Solution) -> Option<bool> {
    match expr {
        Expression::Equal(a, b) => Some(terms_equal(ctx, a, b, sol)),
        Expression::SameTerm(a, b) => Some(expr_str(ctx, a, sol)? == expr_str(ctx, b, sol)?),
        Expression::Greater(a, b) => Some(num(ctx, a, sol)? > num(ctx, b, sol)?),
        Expression::GreaterOrEqual(a, b) => Some(num(ctx, a, sol)? >= num(ctx, b, sol)?),
        Expression::Less(a, b) => Some(num(ctx, a, sol)? < num(ctx, b, sol)?),
        Expression::LessOrEqual(a, b) => Some(num(ctx, a, sol)? <= num(ctx, b, sol)?),
        _ => unreachable!("eval_expr_bool_compare called with a non-comparison Expression variant"),
    }
}

/// The boolean-connective arms of [`eval_expr_bool`]: `&&`, `||`, `!`.
pub(super) fn eval_expr_bool_logic(ctx: &Ctx, expr: &Expression, sol: &Solution) -> Option<bool> {
    match expr {
        Expression::And(a, b) => Some(eval_expr_bool(ctx, a, sol)? && eval_expr_bool(ctx, b, sol)?),
        Expression::Or(a, b) => Some(eval_expr_bool(ctx, a, sol)? || eval_expr_bool(ctx, b, sol)?),
        Expression::Not(a) => Some(!eval_expr_bool(ctx, a, sol)?),
        _ => unreachable!("eval_expr_bool_logic called with a non-logic Expression variant"),
    }
}

/// The `IN` arm of [`eval_expr_bool`]: numeric-aware membership over the candidate list.
pub(super) fn eval_expr_bool_in(
    ctx: &Ctx,
    a: &Expression,
    list: &[Expression],
    sol: &Solution,
) -> Option<bool> {
    let lhs = eval_term(ctx, a, sol)?;
    Some(list.iter().any(|e| {
        eval_term(ctx, e, sol)
            .map(|rhs| binding_terms_equal(&lhs, &rhs))
            .unwrap_or(false)
    }))
}

/// The `IF` arm of [`eval_expr_bool`].
pub(super) fn eval_expr_bool_if(
    ctx: &Ctx,
    c: &Expression,
    t: &Expression,
    e: &Expression,
    sol: &Solution,
) -> Option<bool> {
    if eval_expr_bool(ctx, c, sol).unwrap_or(false) {
        eval_expr_bool(ctx, t, sol)
    } else {
        eval_expr_bool(ctx, e, sol)
    }
}

/// The `EXISTS` arm of [`eval_expr_bool`]: whether any solution of the sub-pattern is
/// COMPATIBLE with the current solution (agrees on the shared variables) — the
/// substitution-and-nonempty semantics of EXISTS.
pub(super) fn eval_expr_bool_exists(
    ctx: &Ctx,
    pattern: &GraphPattern,
    sol: &Solution,
) -> Option<bool> {
    let sols = eval_pattern(ctx, pattern).ok()?;
    Some(sols.iter().any(|s| merge(sol, s).is_some()))
}

/// Evaluate any expression to a typed term `Binding` (CONCEPT:EG-KG.ontology.rich-filter). Preserves the
/// Node/Literal distinction so `isIRI`/`isLiteral`/`STR`/`DATATYPE` resolve correctly.
pub(super) fn eval_term(ctx: &Ctx, e: &Expression, sol: &Solution) -> Option<Binding> {
    match e {
        Expression::Variable(v) => sol.get(v.as_str()).cloned(),
        Expression::Literal(l) => Some(Binding::Literal(l.value().to_string())),
        Expression::NamedNode(n) => Some(Binding::Node(format!("<{}>", n.as_str()))),
        Expression::Add(..)
        | Expression::Subtract(..)
        | Expression::Multiply(..)
        | Expression::Divide(..)
        | Expression::UnaryPlus(..)
        | Expression::UnaryMinus(..) => eval_term_arith(ctx, e, sol),
        Expression::If(c, t, f) => eval_term_if(ctx, c, t, f, sol),
        Expression::Coalesce(args) => args.iter().find_map(|a| eval_term(ctx, a, sol)),
        Expression::FunctionCall(f, args) => eval_str_function(ctx, f, args, sol),
        // Boolean-valued expressions render as an xsd:boolean lexical — including
        // `EXISTS` used in a value context, e.g. `BIND(EXISTS { … } AS ?x)` (CONCEPT:EG-KG.ontology.order-by-values-exists).
        Expression::Bound(_)
        | Expression::Equal(..)
        | Expression::SameTerm(..)
        | Expression::Greater(..)
        | Expression::GreaterOrEqual(..)
        | Expression::Less(..)
        | Expression::LessOrEqual(..)
        | Expression::And(..)
        | Expression::Or(..)
        | Expression::Not(..)
        | Expression::In(..)
        | Expression::Exists(_) => eval_term_bool_literal(ctx, e, sol),
    }
}

/// The arithmetic arms of [`eval_term`]: `+`, `-`, `*`, `/`, unary `+`/`-`. `/` by
/// zero is `None` (undefined), not a panic or an infinity literal.
pub(super) fn eval_term_arith(ctx: &Ctx, e: &Expression, sol: &Solution) -> Option<Binding> {
    match e {
        Expression::Add(a, b) => Some(Binding::Literal(fmt_num(
            num(ctx, a, sol)? + num(ctx, b, sol)?,
        ))),
        Expression::Subtract(a, b) => Some(Binding::Literal(fmt_num(
            num(ctx, a, sol)? - num(ctx, b, sol)?,
        ))),
        Expression::Multiply(a, b) => Some(Binding::Literal(fmt_num(
            num(ctx, a, sol)? * num(ctx, b, sol)?,
        ))),
        Expression::Divide(a, b) => {
            let d = num(ctx, b, sol)?;
            if d == 0.0 {
                return None;
            }
            Some(Binding::Literal(fmt_num(num(ctx, a, sol)? / d)))
        }
        Expression::UnaryPlus(a) => Some(Binding::Literal(fmt_num(num(ctx, a, sol)?))),
        Expression::UnaryMinus(a) => Some(Binding::Literal(fmt_num(-num(ctx, a, sol)?))),
        _ => unreachable!("eval_term_arith called with a non-arithmetic Expression variant"),
    }
}

/// The `IF` arm of [`eval_term`].
pub(super) fn eval_term_if(
    ctx: &Ctx,
    c: &Expression,
    t: &Expression,
    f: &Expression,
    sol: &Solution,
) -> Option<Binding> {
    if eval_expr_bool(ctx, c, sol).unwrap_or(false) {
        eval_term(ctx, t, sol)
    } else {
        eval_term(ctx, f, sol)
    }
}

/// The boolean-valued-expression arm of [`eval_term`]: render as an xsd:boolean lexical.
pub(super) fn eval_term_bool_literal(ctx: &Ctx, e: &Expression, sol: &Solution) -> Option<Binding> {
    Some(Binding::Literal(
        if eval_expr_bool(ctx, e, sol)? {
            "true"
        } else {
            "false"
        }
        .to_string(),
    ))
}

/// Boolean SPARQL built-ins (CONCEPT:EG-KG.ontology.rich-filter): `REGEX`, `CONTAINS`/`STRSTARTS`/`STRENDS`,
/// `LANGMATCHES`, and the `isIRI`/`isBlank`/`isLiteral`/`isNumeric` type tests.
///
/// `Function` has far more variants than the boolean built-ins, so this dispatch
/// ends in a catch-all by nature. The four unary term-type tests are looked up with
/// `bool_builtins::term_type_test` from that catch-all; every other variant yields `None`.
pub(super) fn eval_bool_function(
    ctx: &Ctx,
    f: &Function,
    args: &[Expression],
    sol: &Solution,
) -> Option<bool> {
    use spargebra::algebra::Function as F;
    match f {
        F::Contains => eval_bool_str_relation(ctx, args, sol, |text, part| text.contains(part)),
        F::StrStarts => eval_bool_str_relation(ctx, args, sol, |text, part| text.starts_with(part)),
        F::StrEnds => eval_bool_str_relation(ctx, args, sol, |text, part| text.ends_with(part)),
        F::LangMatches => eval_bool_langmatches(ctx, args, sol),
        F::Regex => eval_bool_regex(ctx, args, sol),
        // RDF-star (CONCEPT:EG-KG.ontology.concept-5): isTRIPLE tests whether the term is a quoted triple.
        #[cfg(feature = "sparql-star")]
        F::IsTriple => Some(term_test(ctx, args.first()?, sol, is_quoted)),
        // GeoSPARQL boolean spatial relations (CONCEPT:EG-KG.ontology.concept-10): a `geof:sf*` call parses
        // to `Function::Custom(<geof-ns>…)`; we evaluate the two operands to their WKT
        // lexical forms and lower the relation onto eg-geo's DE-9IM predicates.
        #[cfg(feature = "geosparql")]
        F::Custom(iri) if iri.as_str().starts_with(crate::geosparql::GEOF_NS) => {
            eval_bool_geof(ctx, iri.as_str(), args, sol)
        }
        _ => {
            let test = term_type_test(f)?;
            Some(term_test(ctx, args.first()?, sol, test))
        }
    }
}

/// `LANGMATCHES(?lang, range)`: case-insensitive BCP-47 range match (`*`, exact, or a
/// `range-` prefix).
pub(super) fn eval_bool_langmatches(
    ctx: &Ctx,
    args: &[Expression],
    sol: &Solution,
) -> Option<bool> {
    let tag = expr_str(ctx, args.first()?, sol)?.to_lowercase();
    let range = expr_str(ctx, args.get(1)?, sol)?.to_lowercase();
    Some((range == "*" && !tag.is_empty()) || tag == range || tag.starts_with(&format!("{range}-")))
}

/// `REGEX(text, pattern, flags?)`: `i` in `flags` enables case-insensitive matching.
pub(super) fn eval_bool_regex(ctx: &Ctx, args: &[Expression], sol: &Solution) -> Option<bool> {
    let text = expr_str(ctx, args.first()?, sol)?;
    let pat = expr_str(ctx, args.get(1)?, sol)?;
    let flags = args
        .get(2)
        .and_then(|f| expr_str(ctx, f, sol))
        .unwrap_or_default();
    let pattern = if flags.contains('i') {
        format!("(?i){pat}")
    } else {
        pat
    };
    regex::Regex::new(&pattern)
        .ok()
        .map(|re| re.is_match(&text))
}

/// Evaluate `arg` to a term and apply `test`; an unevaluable term is `false` (the
/// shared shape of the `isIRI`/`isBlank`/`isLiteral`/`isNumeric`/`isTRIPLE` tests).
pub(super) fn term_test(
    ctx: &Ctx,
    arg: &Expression,
    sol: &Solution,
    test: impl FnOnce(&Binding) -> bool,
) -> bool {
    eval_term(ctx, arg, sol).map(|b| test(&b)).unwrap_or(false)
}

/// A `geof:sf*` boolean spatial relation call: evaluate the two operands to their WKT
/// lexical forms and lower the relation onto eg-geo's DE-9IM predicates.
#[cfg(feature = "geosparql")]
pub(super) fn eval_bool_geof(
    ctx: &Ctx,
    iri: &str,
    args: &[Expression],
    sol: &Solution,
) -> Option<bool> {
    let local = &iri[crate::geosparql::GEOF_NS.len()..];
    let a = expr_str(ctx, args.first()?, sol)?;
    let b = expr_str(ctx, args.get(1)?, sol)?;
    crate::geosparql::eval_relation(local, &a, &b)
}

/// String/term-valued SPARQL built-ins (CONCEPT:EG-KG.ontology.rich-filter): `STR`/`IRI`/`LANG`/`DATATYPE`,
/// `UCASE`/`LCASE`/`STRLEN`/`CONCAT`/`SUBSTR`, plus the boolean built-ins rendered as an
/// xsd:boolean lexical so they compose inside other string expressions.
pub(super) fn eval_str_function(
    ctx: &Ctx,
    f: &Function,
    args: &[Expression],
    sol: &Solution,
) -> Option<Binding> {
    use spargebra::algebra::Function as F;
    match f {
        F::Str
        | F::Iri
        | F::Lang
        | F::Datatype
        | F::UCase
        | F::LCase
        | F::StrLen
        | F::Concat
        | F::SubStr => eval_str_core(ctx, f, args, sol),
        F::BNode | F::StrDt | F::StrLang | F::Uuid | F::StrUuid => {
            eval_str_term_ctors(ctx, f, args, sol)
        }
        // Pure-Rust RustCrypto, gated behind `sparql-hash` (OUT of pi). When the
        // feature is off they fall through to `_ => None` (unsupported, fails SAFE).
        #[cfg(feature = "sparql-hash")]
        F::Md5 | F::Sha1 | F::Sha256 | F::Sha384 | F::Sha512 => eval_str_hash(ctx, f, args, sol),
        F::Abs | F::Ceil | F::Floor | F::Round | F::Rand => eval_str_numeric(ctx, f, args, sol),
        F::Now
        | F::Year
        | F::Month
        | F::Day
        | F::Hours
        | F::Minutes
        | F::Seconds
        | F::Tz
        | F::Timezone => eval_str_datetime(ctx, f, args, sol),
        F::StrBefore | F::StrAfter | F::Replace | F::EncodeForUri => {
            eval_str_extras(ctx, f, args, sol)
        }
        // RDF-star / SPARQL-star term accessors (CONCEPT:EG-KG.ontology.concept-5): a quoted triple is a
        // first-class term encoded as the canonical `<< s p o >>` string in a
        // `Binding::Node`; TRIPLE constructs it and SUBJECT/PREDICATE/OBJECT project its
        // components.
        #[cfg(feature = "sparql-star")]
        F::Triple | F::Subject | F::Predicate | F::Object | F::IsTriple => {
            eval_str_rdfstar(ctx, f, args, sol)
        }
        // Boolean built-ins composed in a string context → "true"/"false".
        F::Contains
        | F::StrStarts
        | F::StrEnds
        | F::Regex
        | F::LangMatches
        | F::IsIri
        | F::IsBlank
        | F::IsLiteral
        | F::IsNumeric => eval_str_bool_literal(ctx, f, args, sol),
        // GeoSPARQL value functions (CONCEPT:EG-KG.ontology.concept-10): `geof:distance(a,b,units)` → a
        // numeric literal; `geof:buffer(g,radius,units)` → a WKT lexical (a wktLiteral).
        // A boolean `geof:sf*` used in a value context renders as "true"/"false".
        #[cfg(feature = "geosparql")]
        F::Custom(iri) if iri.as_str().starts_with(crate::geosparql::GEOF_NS) => {
            eval_str_geof(ctx, f, iri.as_str(), args, sol)
        }
        _ => None,
    }
}

/// `STR`/`IRI`/`LANG`/`DATATYPE`/`UCASE`/`LCASE`/`STRLEN`/`CONCAT`/`SUBSTR`.
pub(super) fn eval_str_core(
    ctx: &Ctx,
    f: &Function,
    args: &[Expression],
    sol: &Solution,
) -> Option<Binding> {
    use spargebra::algebra::Function as F;
    match f {
        F::Str => Some(Binding::Literal(term_lexical(&eval_term(
            ctx,
            args.first()?,
            sol,
        )?))),
        F::Iri => {
            let iri = expr_str(ctx, args.first()?, sol)?
                .trim_start_matches('<')
                .trim_end_matches('>')
                .to_string();
            Some(Binding::Node(format!("<{iri}>")))
        }
        // No language tag is retained in a `Binding`, so LANG is the empty string (the
        // correct value for a plain/typed literal).
        F::Lang => Some(Binding::Literal(String::new())),
        F::Datatype => Some(Binding::Node(format!(
            "<{}>",
            best_effort_datatype(&eval_term(ctx, args.first()?, sol)?)
        ))),
        F::UCase => Some(Binding::Literal(
            expr_str(ctx, args.first()?, sol)?.to_uppercase(),
        )),
        F::LCase => Some(Binding::Literal(
            expr_str(ctx, args.first()?, sol)?.to_lowercase(),
        )),
        F::StrLen => Some(Binding::Literal(fmt_num(
            expr_str(ctx, args.first()?, sol)?.chars().count() as f64,
        ))),
        F::Concat => eval_str_concat(ctx, args, sol),
        F::SubStr => eval_str_substr(ctx, args, sol),
        _ => unreachable!("eval_str_core called with an out-of-group Function variant"),
    }
}

/// `CONCAT(...)`: string-concatenate every argument.
pub(super) fn eval_str_concat(ctx: &Ctx, args: &[Expression], sol: &Solution) -> Option<Binding> {
    let mut s = String::new();
    for a in args {
        s.push_str(&expr_str(ctx, a, sol)?);
    }
    Some(Binding::Literal(s))
}

/// `SUBSTR(str, start, len?)`: SPARQL SUBSTR is 1-based; an optional length truncates.
pub(super) fn eval_str_substr(ctx: &Ctx, args: &[Expression], sol: &Solution) -> Option<Binding> {
    let chars: Vec<char> = expr_str(ctx, args.first()?, sol)?.chars().collect();
    let begin = (num(ctx, args.get(1)?, sol)?.max(1.0) as usize).saturating_sub(1);
    let slice: String = match args.get(2) {
        Some(lenexpr) => {
            let len = num(ctx, lenexpr, sol)?.max(0.0) as usize;
            chars.iter().skip(begin).take(len).collect()
        }
        None => chars.iter().skip(begin).collect(),
    };
    Some(Binding::Literal(slice))
}

// ── Term constructors (CONCEPT:EG-KG.ontology.concept-4) ────────────────────────────────
/// BNODE() → a fresh blank node; BNODE(str) → a blank node labelled from the arg. The
/// arg-less form is NON-DETERMINISTIC (see `next_rand_u64`) and must stay out of any
/// cached/deterministic evaluation path.
///
/// STRDT(lexical, datatype) / STRLANG(lexical, lang): a `Binding` carries only the
/// lexical form (as DATATYPE/LANG already infer best-effort), so the value round-trips
/// while the datatype/lang ride along implicitly.
///
/// UUID() → a fresh urn:uuid: IRI; STRUUID() → its lexical form. NON-DETERMINISTIC.
pub(super) fn eval_str_term_ctors(
    ctx: &Ctx,
    f: &Function,
    args: &[Expression],
    sol: &Solution,
) -> Option<Binding> {
    use spargebra::algebra::Function as F;
    match f {
        F::BNode => {
            let label = match args.first() {
                Some(a) => sanitize_bnode_label(&expr_str(ctx, a, sol)?),
                None => format!("b{:x}", fresh_id()),
            };
            Some(Binding::Node(format!("_:{label}")))
        }
        F::StrDt | F::StrLang => Some(Binding::Literal(expr_str(ctx, args.first()?, sol)?)),
        F::Uuid => Some(Binding::Node(format!("<urn:uuid:{}>", fresh_uuid()))),
        F::StrUuid => Some(Binding::Literal(fresh_uuid())),
        _ => unreachable!("eval_str_term_ctors called with an out-of-group Function variant"),
    }
}

// ── Hash built-ins (CONCEPT:EG-KG.ontology.concept-4) ──────────────────────────────────
#[cfg(feature = "sparql-hash")]
pub(super) fn eval_str_hash(
    ctx: &Ctx,
    f: &Function,
    args: &[Expression],
    sol: &Solution,
) -> Option<Binding> {
    use spargebra::algebra::Function as F;
    match f {
        F::Md5 => Some(Binding::Literal(hash_hex::<md5::Md5>(&expr_str(
            ctx,
            args.first()?,
            sol,
        )?))),
        F::Sha1 => Some(Binding::Literal(hash_hex::<sha1::Sha1>(&expr_str(
            ctx,
            args.first()?,
            sol,
        )?))),
        F::Sha256 => Some(Binding::Literal(hash_hex::<sha2::Sha256>(&expr_str(
            ctx,
            args.first()?,
            sol,
        )?))),
        F::Sha384 => Some(Binding::Literal(hash_hex::<sha2::Sha384>(&expr_str(
            ctx,
            args.first()?,
            sol,
        )?))),
        F::Sha512 => Some(Binding::Literal(hash_hex::<sha2::Sha512>(&expr_str(
            ctx,
            args.first()?,
            sol,
        )?))),
        _ => unreachable!("eval_str_hash called with an out-of-group Function variant"),
    }
}

// ── Numeric built-ins (CONCEPT:EG-KG.ontology.concept-4) ───────────────────────────────
/// SPARQL ROUND is half-towards-positive-infinity (ROUND(-2.5)=-2, ROUND(2.5)=3).
/// RAND() → xsd:double in [0,1). NON-DETERMINISTIC.
pub(super) fn eval_str_numeric(
    ctx: &Ctx,
    f: &Function,
    args: &[Expression],
    sol: &Solution,
) -> Option<Binding> {
    use spargebra::algebra::Function as F;
    match f {
        F::Abs => Some(Binding::Literal(fmt_num(
            num(ctx, args.first()?, sol)?.abs(),
        ))),
        F::Ceil => Some(Binding::Literal(fmt_num(
            num(ctx, args.first()?, sol)?.ceil(),
        ))),
        F::Floor => Some(Binding::Literal(fmt_num(
            num(ctx, args.first()?, sol)?.floor(),
        ))),
        F::Round => Some(Binding::Literal(fmt_num(
            (num(ctx, args.first()?, sol)? + 0.5).floor(),
        ))),
        F::Rand => Some(Binding::Literal(fmt_num(rand_f64()))),
        _ => unreachable!("eval_str_numeric called with an out-of-group Function variant"),
    }
}

// ── Date-time built-ins (CONCEPT:EG-KG.ontology.concept-4) ─────────────────────────────
/// NOW() → the current xsd:dateTime (UTC). NON-DETERMINISTIC.
pub(super) fn eval_str_datetime(
    ctx: &Ctx,
    f: &Function,
    args: &[Expression],
    sol: &Solution,
) -> Option<Binding> {
    use spargebra::algebra::Function as F;
    match f {
        F::Now => Some(Binding::Literal(now_xsd_datetime())),
        F::Year => Some(Binding::Literal(fmt_num(
            parse_datetime(&expr_str(ctx, args.first()?, sol)?)?.year as f64,
        ))),
        F::Month => Some(Binding::Literal(fmt_num(
            parse_datetime(&expr_str(ctx, args.first()?, sol)?)?.month as f64,
        ))),
        F::Day => Some(Binding::Literal(fmt_num(
            parse_datetime(&expr_str(ctx, args.first()?, sol)?)?.day as f64,
        ))),
        F::Hours => Some(Binding::Literal(fmt_num(
            parse_datetime(&expr_str(ctx, args.first()?, sol)?)?.hour as f64,
        ))),
        F::Minutes => Some(Binding::Literal(fmt_num(
            parse_datetime(&expr_str(ctx, args.first()?, sol)?)?.minute as f64,
        ))),
        F::Seconds => Some(Binding::Literal(fmt_num(
            parse_datetime(&expr_str(ctx, args.first()?, sol)?)?.second,
        ))),
        F::Tz => Some(Binding::Literal(
            parse_datetime(&expr_str(ctx, args.first()?, sol)?)?.tz,
        )),
        F::Timezone => Some(Binding::Literal(tz_to_duration(
            &parse_datetime(&expr_str(ctx, args.first()?, sol)?)?.tz,
        ))),
        _ => unreachable!("eval_str_datetime called with an out-of-group Function variant"),
    }
}

// ── String extras (CONCEPT:EG-KG.ontology.concept-4) ───────────────────────────────────
pub(super) fn eval_str_extras(
    ctx: &Ctx,
    f: &Function,
    args: &[Expression],
    sol: &Solution,
) -> Option<Binding> {
    use spargebra::algebra::Function as F;
    match f {
        F::StrBefore => {
            let s = expr_str(ctx, args.first()?, sol)?;
            let sep = expr_str(ctx, args.get(1)?, sol)?;
            Some(Binding::Literal(match s.find(&sep) {
                Some(i) => s[..i].to_string(),
                None => String::new(),
            }))
        }
        F::StrAfter => {
            let s = expr_str(ctx, args.first()?, sol)?;
            let sep = expr_str(ctx, args.get(1)?, sol)?;
            Some(Binding::Literal(match s.find(&sep) {
                Some(i) => s[i + sep.len()..].to_string(),
                None => String::new(),
            }))
        }
        // REPLACE(str, pattern, replacement [, flags]) — regex-backed (reuses the
        // `regex` dep already in `sparql`); `$1` back-references pass through.
        F::Replace => {
            let s = expr_str(ctx, args.first()?, sol)?;
            let pat = expr_str(ctx, args.get(1)?, sol)?;
            let rep = expr_str(ctx, args.get(2)?, sol)?;
            let flags = args
                .get(3)
                .and_then(|fl| expr_str(ctx, fl, sol))
                .unwrap_or_default();
            let pattern = if flags.contains('i') {
                format!("(?i){pat}")
            } else {
                pat
            };
            let re = regex::Regex::new(&pattern).ok()?;
            Some(Binding::Literal(
                re.replace_all(&s, rep.as_str()).into_owned(),
            ))
        }
        F::EncodeForUri => Some(Binding::Literal(encode_for_uri(&expr_str(
            ctx,
            args.first()?,
            sol,
        )?))),
        _ => unreachable!("eval_str_extras called with an out-of-group Function variant"),
    }
}

// ── RDF-star / SPARQL-star term accessors (CONCEPT:EG-KG.ontology.concept-5) ────────────
#[cfg(feature = "sparql-star")]
pub(super) fn eval_str_rdfstar(
    ctx: &Ctx,
    f: &Function,
    args: &[Expression],
    sol: &Solution,
) -> Option<Binding> {
    use spargebra::algebra::Function as F;
    match f {
        F::Triple => {
            let s = eval_term(ctx, args.first()?, sol)?;
            let p = eval_term(ctx, args.get(1)?, sol)?;
            let o = eval_term(ctx, args.get(2)?, sol)?;
            Some(Binding::Node(encode_quoted(&s, &p, &o)))
        }
        F::Subject => quoted_component(&eval_term(ctx, args.first()?, sol)?, 0),
        F::Predicate => quoted_component(&eval_term(ctx, args.first()?, sol)?, 1),
        F::Object => quoted_component(&eval_term(ctx, args.first()?, sol)?, 2),
        F::IsTriple => Some(Binding::Literal(bool_str(eval_bool_function(
            ctx, f, args, sol,
        )?))),
        _ => unreachable!("eval_str_rdfstar called with an out-of-group Function variant"),
    }
}

/// Boolean built-ins composed in a string context → "true"/"false".
pub(super) fn eval_str_bool_literal(
    ctx: &Ctx,
    f: &Function,
    args: &[Expression],
    sol: &Solution,
) -> Option<Binding> {
    Some(Binding::Literal(bool_str(eval_bool_function(
        ctx, f, args, sol,
    )?)))
}

/// `"true"`/`"false"` lexical rendering of a bool — the shared tail of every boolean
/// built-in composed in a string (value) context.
pub(super) fn bool_str(b: bool) -> String {
    if b { "true" } else { "false" }.to_string()
}

/// GeoSPARQL value functions (CONCEPT:EG-KG.ontology.concept-10): `geof:distance(a,b,units)` → a numeric
/// literal; `geof:buffer(g,radius,units)` → a WKT lexical (a wktLiteral). A boolean
/// `geof:sf*` used in a value context renders as "true"/"false".
#[cfg(feature = "geosparql")]
pub(super) fn eval_str_geof(
    ctx: &Ctx,
    f: &Function,
    iri: &str,
    args: &[Expression],
    sol: &Solution,
) -> Option<Binding> {
    let local = &iri[crate::geosparql::GEOF_NS.len()..];
    match local {
        "distance" => eval_str_geof_distance(ctx, args, sol),
        "buffer" => eval_str_geof_buffer(ctx, args, sol),
        _ => Some(Binding::Literal(bool_str(eval_bool_function(
            ctx, f, args, sol,
        )?))),
    }
}

#[cfg(feature = "geosparql")]
pub(super) fn eval_str_geof_distance(
    ctx: &Ctx,
    args: &[Expression],
    sol: &Solution,
) -> Option<Binding> {
    let a = expr_str(ctx, args.first()?, sol)?;
    let b = expr_str(ctx, args.get(1)?, sol)?;
    let units = args
        .get(2)
        .and_then(|u| expr_str(ctx, u, sol))
        .unwrap_or_default();
    Some(Binding::Literal(fmt_num(crate::geosparql::eval_distance(
        &a, &b, &units,
    )?)))
}

#[cfg(feature = "geosparql")]
pub(super) fn eval_str_geof_buffer(
    ctx: &Ctx,
    args: &[Expression],
    sol: &Solution,
) -> Option<Binding> {
    let g = expr_str(ctx, args.first()?, sol)?;
    let radius = num(ctx, args.get(1)?, sol)?;
    let units = args
        .get(2)
        .and_then(|u| expr_str(ctx, u, sol))
        .unwrap_or_default();
    Some(Binding::Literal(crate::geosparql::eval_buffer(
        &g, radius, &units,
    )?))
}

/// Lexical value of a term: an IRI loses its angle brackets (`STR()` / comparison).
pub(super) fn term_lexical(b: &Binding) -> String {
    match b {
        Binding::Node(s) => s.trim_start_matches('<').trim_end_matches('>').to_string(),
        Binding::Literal(s) => s.clone(),
    }
}

/// Best-effort xsd datatype IRI for `DATATYPE()` — bindings drop the original datatype,
/// so we infer numeric vs string from the lexical form (datatype-aware where feasible).
pub(super) fn best_effort_datatype(b: &Binding) -> &'static str {
    match b {
        Binding::Node(_) => "http://www.w3.org/2001/XMLSchema#anyURI",
        Binding::Literal(s) => {
            if s.parse::<i64>().is_ok() {
                "http://www.w3.org/2001/XMLSchema#integer"
            } else if s.parse::<f64>().is_ok() {
                "http://www.w3.org/2001/XMLSchema#decimal"
            } else {
                "http://www.w3.org/2001/XMLSchema#string"
            }
        }
    }
}

/// Effective boolean value (EBV) of a term for a bare expression in FILTER position.
pub(super) fn ebv(b: &Binding) -> bool {
    match b {
        Binding::Literal(s) => match s.parse::<f64>() {
            Ok(n) => n != 0.0,
            Err(_) => !s.is_empty() && !s.eq_ignore_ascii_case("false"),
        },
        Binding::Node(s) => !s.is_empty(),
    }
}

/// Datatype-aware `=` (CONCEPT:EG-KG.ontology.rich-filter): numeric comparison when both sides parse as
/// numbers, else lexical-term equality.
pub(super) fn terms_equal(ctx: &Ctx, a: &Expression, b: &Expression, sol: &Solution) -> bool {
    match (eval_term(ctx, a, sol), eval_term(ctx, b, sol)) {
        (Some(x), Some(y)) => binding_terms_equal(&x, &y),
        _ => false,
    }
}

pub(super) fn binding_terms_equal(x: &Binding, y: &Binding) -> bool {
    let (xs, ys) = (term_lexical(x), term_lexical(y));
    match (xs.parse::<f64>(), ys.parse::<f64>()) {
        (Ok(nx), Ok(ny)) => nx == ny,
        _ => xs == ys,
    }
}

pub(super) fn expr_str(ctx: &Ctx, e: &Expression, sol: &Solution) -> Option<String> {
    eval_term(ctx, e, sol).map(|b| b.as_str().to_string())
}

pub(super) fn num(ctx: &Ctx, e: &Expression, sol: &Solution) -> Option<f64> {
    expr_str(ctx, e, sol)?.parse::<f64>().ok()
}
