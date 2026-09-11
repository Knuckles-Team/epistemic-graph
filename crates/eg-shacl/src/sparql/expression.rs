use std::cmp::Ordering;

use eg_rdf::oxrdf::{Literal, Term};
use spargebra::algebra::{Expression, Function};

use super::terms::{
    bool_term, ebv, int_term, lang_range_matches, lexical_of, literal_lexical, numeric_of,
    pattern_ok, term_eq,
};
use super::{Ctx, Solution};

pub(super) fn eval_filter(
    ctx: &Ctx,
    expression: &Expression,
    solution: &Solution,
) -> Result<bool, String> {
    Ok(ebv(eval_term(ctx, expression, solution)?.as_ref()))
}

/// Evaluate an expression to a bound term. `Ok(None)` is an unbound/type-error
/// operand; `Err` is reserved for unsupported constructs.
pub(super) fn eval_term(
    ctx: &Ctx,
    expression: &Expression,
    solution: &Solution,
) -> Result<Option<Term>, String> {
    match expression {
        Expression::NamedNode(_) | Expression::Literal(_) | Expression::Variable(_) => {
            eval_term_atomic(expression, solution)
        }
        Expression::Bound(variable) => {
            Ok(Some(bool_term(solution.contains_key(variable.as_str()))))
        }
        Expression::Not(_) | Expression::And(_, _) | Expression::Or(_, _) => {
            eval_term_logic(ctx, expression, solution)
        }
        Expression::Equal(_, _) | Expression::SameTerm(_, _) => {
            eval_term_equality(ctx, expression, solution)
        }
        Expression::Greater(_, _)
        | Expression::GreaterOrEqual(_, _)
        | Expression::Less(_, _)
        | Expression::LessOrEqual(_, _) => eval_term_compare(ctx, expression, solution),
        Expression::In(_, _) => eval_term_in(ctx, expression, solution),
        Expression::FunctionCall(function, args) => eval_function(ctx, function, args, solution),
        Expression::Exists(_)
        | Expression::If(_, _, _)
        | Expression::Coalesce(_)
        | Expression::Add(_, _)
        | Expression::Subtract(_, _)
        | Expression::Multiply(_, _)
        | Expression::Divide(_, _)
        | Expression::UnaryPlus(_)
        | Expression::UnaryMinus(_) => eval_term_unsupported(expression),
    }
}

fn eval_term_atomic(expression: &Expression, solution: &Solution) -> Result<Option<Term>, String> {
    match expression {
        Expression::NamedNode(node) => Ok(Some(Term::NamedNode(node.clone()))),
        Expression::Literal(literal) => Ok(Some(Term::Literal(literal.clone()))),
        Expression::Variable(variable) => Ok(solution.get(variable.as_str()).cloned()),
        _ => Err("sh:sparql: invalid atomic expression dispatch".to_string()),
    }
}

fn eval_term_logic(
    ctx: &Ctx,
    expression: &Expression,
    solution: &Solution,
) -> Result<Option<Term>, String> {
    match expression {
        Expression::Not(inner) => Ok(Some(bool_term(!ebv(
            eval_term(ctx, inner, solution)?.as_ref()
        )))),
        Expression::And(left, right) => Ok(Some(bool_term(
            ebv(eval_term(ctx, left, solution)?.as_ref())
                && ebv(eval_term(ctx, right, solution)?.as_ref()),
        ))),
        Expression::Or(left, right) => Ok(Some(bool_term(
            ebv(eval_term(ctx, left, solution)?.as_ref())
                || ebv(eval_term(ctx, right, solution)?.as_ref()),
        ))),
        _ => Err("sh:sparql: invalid logical expression dispatch".to_string()),
    }
}

fn eval_term_equality(
    ctx: &Ctx,
    expression: &Expression,
    solution: &Solution,
) -> Result<Option<Term>, String> {
    let (left, right) = match expression {
        Expression::Equal(left, right) | Expression::SameTerm(left, right) => (left, right),
        _ => return Err("sh:sparql: invalid equality expression dispatch".to_string()),
    };
    let (left_value, right_value) = (
        eval_term(ctx, left, solution)?,
        eval_term(ctx, right, solution)?,
    );
    let equal = match expression {
        Expression::Equal(_, _) => term_eq(&left_value, &right_value),
        Expression::SameTerm(_, _) => left_value == right_value,
        _ => false,
    };
    Ok(Some(bool_term(equal)))
}

fn eval_term_compare(
    ctx: &Ctx,
    expression: &Expression,
    solution: &Solution,
) -> Result<Option<Term>, String> {
    match expression {
        Expression::Greater(left, right) => cmp_expr(ctx, left, right, solution, |order| {
            order == Ordering::Greater
        }),
        Expression::GreaterOrEqual(left, right) => {
            cmp_expr(ctx, left, right, solution, |order| order != Ordering::Less)
        }
        Expression::Less(left, right) => {
            cmp_expr(ctx, left, right, solution, |order| order == Ordering::Less)
        }
        Expression::LessOrEqual(left, right) => cmp_expr(ctx, left, right, solution, |order| {
            order != Ordering::Greater
        }),
        _ => Err("sh:sparql: invalid comparison expression dispatch".to_string()),
    }
}

fn eval_term_in(
    ctx: &Ctx,
    expression: &Expression,
    solution: &Solution,
) -> Result<Option<Term>, String> {
    let (value, list) = match expression {
        Expression::In(value, list) => (value, list),
        _ => return Err("sh:sparql: invalid IN expression dispatch".to_string()),
    };
    let value = eval_term(ctx, value, solution)?;
    let mut found = false;
    for item in list {
        if term_eq(&value, &eval_term(ctx, item, solution)?) {
            found = true;
            break;
        }
    }
    Ok(Some(bool_term(found)))
}

fn eval_term_unsupported(expression: &Expression) -> Result<Option<Term>, String> {
    match expression {
        Expression::Exists(_) => Err("sh:sparql: EXISTS / NOT EXISTS is not supported".to_string()),
        Expression::If(_, _, _) => Err("sh:sparql: IF is not supported".to_string()),
        Expression::Coalesce(_) => Err("sh:sparql: COALESCE is not supported".to_string()),
        Expression::Add(_, _)
        | Expression::Subtract(_, _)
        | Expression::Multiply(_, _)
        | Expression::Divide(_, _)
        | Expression::UnaryPlus(_)
        | Expression::UnaryMinus(_) => Err("sh:sparql: arithmetic is not supported".to_string()),
        _ => Err("sh:sparql: invalid unsupported expression dispatch".to_string()),
    }
}

fn cmp_expr(
    ctx: &Ctx,
    left: &Expression,
    right: &Expression,
    solution: &Solution,
    is_valid: impl Fn(Ordering) -> bool,
) -> Result<Option<Term>, String> {
    let (left_value, right_value) = (
        eval_term(ctx, left, solution)?,
        eval_term(ctx, right, solution)?,
    );
    let order = match (
        left_value.as_ref().and_then(literal_lexical),
        right_value.as_ref().and_then(literal_lexical),
    ) {
        (Some(left_lexical), Some(right_lexical)) => match (
            left_lexical.trim().parse::<f64>(),
            right_lexical.trim().parse::<f64>(),
        ) {
            (Ok(left_number), Ok(right_number)) => left_number.partial_cmp(&right_number),
            _ => Some(left_lexical.cmp(&right_lexical)),
        },
        _ => None,
    };
    Ok(Some(bool_term(order.is_some_and(is_valid))))
}

fn eval_function(
    ctx: &Ctx,
    function: &Function,
    args: &[Expression],
    solution: &Solution,
) -> Result<Option<Term>, String> {
    match function {
        Function::IsIri | Function::IsBlank | Function::IsLiteral | Function::IsNumeric => {
            eval_type_function(ctx, function, args, solution)
        }
        Function::Str | Function::Lang | Function::Datatype => {
            eval_string_function(ctx, function, args, solution)
        }
        Function::LangMatches | Function::Regex => {
            eval_pattern_function(ctx, function, args, solution)
        }
        Function::Contains | Function::StrStarts | Function::StrEnds => {
            eval_text_function(ctx, function, args, solution)
        }
        Function::UCase | Function::LCase | Function::StrLen => {
            eval_case_function(ctx, function, args, solution)
        }
        other => Err(format!("sh:sparql: unsupported SPARQL function {other:?}")),
    }
}

fn function_arg(
    ctx: &Ctx,
    args: &[Expression],
    solution: &Solution,
    index: usize,
) -> Result<Option<Term>, String> {
    match args.get(index) {
        Some(expression) => eval_term(ctx, expression, solution),
        None => Ok(None),
    }
}

fn eval_type_function(
    ctx: &Ctx,
    function: &Function,
    args: &[Expression],
    solution: &Solution,
) -> Result<Option<Term>, String> {
    let value = function_arg(ctx, args, solution, 0)?;
    let result = match function {
        Function::IsIri => matches!(value, Some(Term::NamedNode(_))),
        Function::IsBlank => matches!(value, Some(Term::BlankNode(_))),
        Function::IsLiteral => matches!(value, Some(Term::Literal(_))),
        Function::IsNumeric => value.as_ref().and_then(numeric_of).is_some(),
        _ => return Err("sh:sparql: invalid type-function dispatch".to_string()),
    };
    Ok(Some(bool_term(result)))
}

fn eval_string_function(
    ctx: &Ctx,
    function: &Function,
    args: &[Expression],
    solution: &Solution,
) -> Result<Option<Term>, String> {
    match function {
        Function::Str => {
            let value = function_arg(ctx, args, solution, 0)?;
            Ok(Some(Term::Literal(Literal::new_simple_literal(
                lexical_of(value.as_ref()),
            ))))
        }
        Function::Lang => {
            let value = function_arg(ctx, args, solution, 0)?;
            let language = match value {
                Some(Term::Literal(literal)) => literal.language().unwrap_or("").to_string(),
                _ => String::new(),
            };
            Ok(Some(Term::Literal(Literal::new_simple_literal(language))))
        }
        Function::Datatype => Ok(match function_arg(ctx, args, solution, 0)? {
            Some(Term::Literal(literal)) => Some(Term::NamedNode(literal.datatype().into_owned())),
            _ => None,
        }),
        _ => Err("sh:sparql: invalid string-function dispatch".to_string()),
    }
}

fn eval_pattern_function(
    ctx: &Ctx,
    function: &Function,
    args: &[Expression],
    solution: &Solution,
) -> Result<Option<Term>, String> {
    let first = lexical_of(function_arg(ctx, args, solution, 0)?.as_ref());
    let second = lexical_of(function_arg(ctx, args, solution, 1)?.as_ref());
    let result = match function {
        Function::LangMatches => lang_range_matches(&second, &first),
        Function::Regex => {
            let flags = match args.get(2) {
                Some(_) => Some(lexical_of(function_arg(ctx, args, solution, 2)?.as_ref())),
                None => None,
            };
            pattern_ok(&first, &second, flags.as_deref())
        }
        _ => return Err("sh:sparql: invalid pattern-function dispatch".to_string()),
    };
    Ok(Some(bool_term(result)))
}

fn eval_text_function(
    ctx: &Ctx,
    function: &Function,
    args: &[Expression],
    solution: &Solution,
) -> Result<Option<Term>, String> {
    let first = function_arg(ctx, args, solution, 0)?;
    let second = function_arg(ctx, args, solution, 1)?;
    match function {
        Function::Contains => str_bool(first, second, |value, needle| value.contains(needle)),
        Function::StrStarts => str_bool(first, second, |value, needle| value.starts_with(needle)),
        Function::StrEnds => str_bool(first, second, |value, needle| value.ends_with(needle)),
        _ => Err("sh:sparql: invalid text-function dispatch".to_string()),
    }
}

fn eval_case_function(
    ctx: &Ctx,
    function: &Function,
    args: &[Expression],
    solution: &Solution,
) -> Result<Option<Term>, String> {
    let value = lexical_of(function_arg(ctx, args, solution, 0)?.as_ref());
    match function {
        Function::UCase => Ok(Some(Term::Literal(Literal::new_simple_literal(
            value.to_uppercase(),
        )))),
        Function::LCase => Ok(Some(Term::Literal(Literal::new_simple_literal(
            value.to_lowercase(),
        )))),
        Function::StrLen => Ok(Some(int_term(value.chars().count()))),
        _ => Err("sh:sparql: invalid case-function dispatch".to_string()),
    }
}

fn str_bool(
    left: Option<Term>,
    right: Option<Term>,
    predicate: impl Fn(&str, &str) -> bool,
) -> Result<Option<Term>, String> {
    Ok(Some(bool_term(predicate(
        &lexical_of(left.as_ref()),
        &lexical_of(right.as_ref()),
    ))))
}
