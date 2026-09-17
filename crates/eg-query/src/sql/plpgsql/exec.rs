//! [`super::Interp::exec`]'s simple-statement arms (CONCEPT:EG-KG.query.concept-7):
//! assignment, `EXIT`/`CONTINUE`, `RAISE`, and a bare `PERFORM`/`SELECT`.
//!
//! Split out of `plpgsql.rs` as FREE FUNCTIONS taking `&Interp`/`&mut Interp`
//! explicitly, not `Interp` methods (a "shared module instead of growing a
//! file/type past the kiss caps" split, not a rename): a `methods_per_class`
//! violation from new helpers is fixed by moving them off the type, per this
//! program's standing lesson (see `WRAPUP.md` precedent from
//! `refactor/eg-bd-raft-a-20260916`), not by nesting them back inside `exec`.

use super::{Flow, Interp};

/// `var := expr`.
pub(super) fn exec_assign(interp: &mut Interp<'_>, var: &str, expr: &str) -> Result<Flow, String> {
    let v = interp.eval(expr)?;
    interp.env.insert(var.to_ascii_lowercase(), v);
    Ok(Flow::Normal)
}

/// `EXIT [WHEN cond]`. Exits unconditionally with no `WHEN`, or when `cond`
/// evaluates true. A genuinely exhaustive match over `when`'s two `Option`
/// variants (no catch-all), not the guarded-catch-all shape `exec`'s inline
/// arm used before the split.
pub(super) fn exec_exit(interp: &mut Interp<'_>, when: &Option<String>) -> Result<Flow, String> {
    let should_exit = match when {
        Some(cond) => interp.eval_bool(cond)?,
        None => true,
    };
    Ok(if should_exit {
        Flow::Exit
    } else {
        Flow::Normal
    })
}

/// `CONTINUE [WHEN cond]`. Same shape as [`exec_exit`].
pub(super) fn exec_continue(
    interp: &mut Interp<'_>,
    when: &Option<String>,
) -> Result<Flow, String> {
    let should_continue = match when {
        Some(cond) => interp.eval_bool(cond)?,
        None => true,
    };
    Ok(if should_continue {
        Flow::Continue
    } else {
        Flow::Normal
    })
}

/// `RAISE [EXCEPTION] [message]`.
pub(super) fn exec_raise(fatal: bool, message: &Option<String>) -> Result<Flow, String> {
    let msg = message.clone().unwrap_or_else(|| "raised".to_string());
    if fatal {
        Err(format!("plpgsql RAISE EXCEPTION: {msg}"))
    } else {
        Ok(Flow::Normal)
    }
}

/// A bare `PERFORM`/`SELECT` statement whose result is discarded.
pub(super) fn exec_perform(interp: &mut Interp<'_>, sql: &str) -> Result<Flow, String> {
    let sql = super::substitute_vars(sql, &interp.env);
    interp.query(&sql)?;
    Ok(Flow::Normal)
}
