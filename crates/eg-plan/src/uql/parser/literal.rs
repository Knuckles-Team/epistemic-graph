//! Literals: names, strings, numbers read from their exact source text, typed `$name`
//! parameters, lists, vectors and JSON values (UQL-02/UQL-07).
//!
//! Parameters are resolved HERE, at the literal position that consumes them, and only
//! when the bound value's type fits that position — a `$name` is a value, never text,
//! so it can never change the query's structure.

use std::str::FromStr;

use eg_types::wire::{Scalar, UqlParam};

use super::Parser;
use crate::uql::diag::{UqlCode, UqlError};
use crate::uql::lexer::Tok;

/// A literal-position expectation, for parameter type errors.
#[derive(Clone, Copy)]
pub(super) enum Want {
    Str,
    Num,
    Scalar,
    Vector,
    List,
}

impl Want {
    fn name(self) -> &'static str {
        match self {
            Want::Str => "a string",
            Want::Num => "a number",
            Want::Scalar => "a string, number or boolean",
            Want::Vector => "a vector",
            Want::List => "a list",
        }
    }
}

impl<'a> Parser<'a> {
    /// Consume a `$name`, returning its bound value (marked used); `None` when the next
    /// token is not a parameter.
    pub(super) fn param(&mut self, want: Want) -> Result<Option<UqlParam>, UqlError> {
        let Some(Tok::Param(name)) = self.peek_kind() else {
            return Ok(None);
        };
        let name = name.clone();
        let span = self.cur_span();
        let Some(value) = self.params.get(&name) else {
            return Err(UqlError::new(
                UqlCode::UnboundParameter,
                format!("parameter `${name}` is not bound"),
                span,
            )
            .with_help(format!(
                "bind `{name}` in the request's params (expected {})",
                want.name()
            )));
        };
        self.used_params.insert(name);
        self.bump();
        Ok(Some(value.clone()))
    }

    pub(super) fn param_type_error(&self, want: Want) -> UqlError {
        UqlError::new(
            UqlCode::ParameterType,
            format!(
                "parameter bound to the wrong type here (expected {})",
                want.name()
            ),
            self.prev_span(),
        )
    }

    /// An identifier (bare or back-quoted).
    pub(super) fn name(&mut self, what: &str) -> Result<String, UqlError> {
        match self.peek_kind() {
            Some(Tok::Ident(s)) | Some(Tok::QIdent(s)) => {
                let s = s.clone();
                self.bump();
                Ok(s)
            }
            _ => Err(self.err_here(&format!("expected {what}"))),
        }
    }

    /// A string literal or a string parameter.
    pub(super) fn string(&mut self, what: &str) -> Result<String, UqlError> {
        if let Some(Tok::Str(s)) = self.peek_kind() {
            let s = s.clone();
            self.bump();
            return Ok(s);
        }
        match self.param(Want::Str)? {
            Some(UqlParam::Str(s)) => Ok(s),
            Some(_) => Err(self.param_type_error(Want::Str)),
            None => Err(self.err_here(&format!("expected {what} (a quoted string)"))),
        }
    }

    /// An id: a name, a string, or a string parameter.
    pub(super) fn id(&mut self, what: &str) -> Result<String, UqlError> {
        match self.peek_kind() {
            Some(Tok::Ident(_)) | Some(Tok::QIdent(_)) => self.name(what),
            _ => self.string(what),
        }
    }

    /// The source text of a (possibly `-`-signed) number at the cursor, consumed.
    pub(super) fn signed_text(&mut self) -> Option<String> {
        let negative = self.peek(&Tok::Dash)
            && matches!(
                self.toks.get(self.pos + 1).map(|t| &t.kind),
                Some(Tok::Num(_))
            );
        let at = self.pos + usize::from(negative);
        if !matches!(self.toks.get(at).map(|t| &t.kind), Some(Tok::Num(_))) {
            return None;
        }
        let text = self.text_of(at);
        self.pos = at + 1;
        Some(if negative {
            format!("-{text}")
        } else {
            text.to_string()
        })
    }

    /// A signed number (exact `f64` of its text) or a number parameter.
    pub(super) fn number(&mut self, what: &str) -> Result<f64, UqlError> {
        self.parse_number(what)
    }

    /// A signed number read directly as `T` from its text (so an `f32` is never
    /// double-rounded through `f64`).
    pub(super) fn parse_number<T: FromStr + TryFromParam>(
        &mut self,
        what: &str,
    ) -> Result<T, UqlError> {
        let span = self.cur_span();
        if let Some(text) = self.signed_text() {
            return text.parse::<T>().map_err(|_| {
                UqlError::new(
                    UqlCode::ExpectedInteger,
                    format!("{what} must be {}, found `{text}`", T::KIND),
                    span,
                )
            });
        }
        match self.param(Want::Num)? {
            Some(UqlParam::Num(n)) => {
                T::from_param(n).ok_or_else(|| self.param_type_error(Want::Num))
            }
            Some(_) => Err(self.param_type_error(Want::Num)),
            None => Err(self.err_here(&format!("expected {what} ({})", T::KIND))),
        }
    }

    /// A typed scalar: string, signed number, `TRUE`/`FALSE`, a bare name (a string),
    /// or a scalar parameter. `NULL` is refused with a hint (use `IS NULL`).
    pub(super) fn scalar(&mut self) -> Result<Scalar, UqlError> {
        if self.peek_kw("NULL") {
            return Err(self
                .error(UqlCode::NullLiteral, "NULL is not a comparable value")
                .with_help("use `<prop> IS NULL` / `<prop> IS NOT NULL`"));
        }
        if let Some(b) = self.boolean() {
            return Ok(Scalar::Bool(b));
        }
        match self.peek_kind() {
            Some(Tok::Str(_)) => Ok(Scalar::Str(self.string("a value")?)),
            Some(Tok::Ident(_)) | Some(Tok::QIdent(_)) => Ok(Scalar::Str(self.name("a value")?)),
            Some(Tok::Param(_)) => self.scalar_param(),
            _ => Ok(Scalar::Num(self.number("a value")?)),
        }
    }

    fn scalar_param(&mut self) -> Result<Scalar, UqlError> {
        match self.param(Want::Scalar)? {
            Some(UqlParam::Str(s)) => Ok(Scalar::Str(s)),
            Some(UqlParam::Num(n)) => Ok(Scalar::Num(n)),
            Some(UqlParam::Bool(b)) => Ok(Scalar::Bool(b)),
            _ => Err(self.param_type_error(Want::Scalar)),
        }
    }

    /// `TRUE` / `FALSE`, consumed.
    pub(super) fn boolean(&mut self) -> Option<bool> {
        let value = if self.peek_kw("TRUE") {
            true
        } else if self.peek_kw("FALSE") {
            false
        } else {
            return None;
        };
        self.bump();
        Some(value)
    }
}

/// A numeric type a literal position can hold, with its parameter conversion.
pub(super) trait TryFromParam: Sized {
    const KIND: &'static str;
    fn from_param(n: f64) -> Option<Self>;
}

impl TryFromParam for f64 {
    const KIND: &'static str = "a number";
    fn from_param(n: f64) -> Option<Self> {
        Some(n)
    }
}

impl TryFromParam for f32 {
    const KIND: &'static str = "a number";
    fn from_param(n: f64) -> Option<Self> {
        Some(n as f32)
    }
}

macro_rules! integer_param {
    ($($t:ty),*) => {$(
        impl TryFromParam for $t {
            const KIND: &'static str = "an integer";
            fn from_param(n: f64) -> Option<Self> {
                (n.fract() == 0.0 && n >= <$t>::MIN as f64 && n <= <$t>::MAX as f64)
                    .then(|| n as $t)
            }
        }
    )*};
}
integer_param!(usize, u32, u64, i64);
