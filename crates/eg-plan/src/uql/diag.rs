//! Structured UQL diagnostics (UQL-02): a stable CODE an agent can branch on, the
//! byte SPAN of the offending text, what the parser EXPECTED there, and a fix HINT —
//! plus a human rendering with `line:column` and a caret under the exact span.

use super::lexer::{LexError, LexErrorKind};

/// The closed diagnostic vocabulary is owned by the wire contract so the
/// planner, response boundary, and generated API cannot drift.
pub use eg_types::contract::UqlCode;

/// Every UQL diagnostic code, in canonical declaration order.
pub const ALL_CODES: &[UqlCode] = UqlCode::ALL;

impl From<LexErrorKind> for UqlCode {
    fn from(kind: LexErrorKind) -> Self {
        match kind {
            LexErrorKind::UnexpectedCharacter => Self::UnexpectedCharacter,
            LexErrorKind::UnterminatedString => Self::UnterminatedString,
            LexErrorKind::UnterminatedIdentifier => Self::UnterminatedIdentifier,
            LexErrorKind::InvalidNumber => Self::InvalidNumber,
            LexErrorKind::EmptyParameterName => Self::EmptyParameterName,
        }
    }
}

/// A UQL error: code, message, span, the expected set and an optional fix hint.
/// [`UqlError::render`] turns it into a caret diagnostic.
#[derive(Clone, Debug, PartialEq)]
pub struct UqlError {
    pub code: UqlCode,
    pub msg: String,
    /// Start byte offset of the offending text.
    pub at: usize,
    /// End byte offset (exclusive; `== at` at end of input).
    pub end: usize,
    /// What would have been accepted here (grammar spellings), when known.
    pub expected: Vec<String>,
    /// A concrete fix, when one is known.
    pub help: Option<String>,
}

impl UqlError {
    pub fn new(code: UqlCode, msg: impl Into<String>, span: (usize, usize)) -> Self {
        Self {
            code,
            msg: msg.into(),
            at: span.0,
            end: span.1,
            expected: Vec::new(),
            help: None,
        }
    }

    /// Attach the expected set.
    pub fn expecting(mut self, expected: Vec<String>) -> Self {
        self.expected = expected;
        self
    }

    /// Attach a fix hint.
    pub fn with_help(mut self, help: impl Into<String>) -> Self {
        self.help = Some(help.into());
        self
    }

    /// Render a `line:column` caret diagnostic against `src`:
    /// ```text
    /// UQL_UNEXPECTED_TOKEN at 2:13: expected a `LIMIT` count, found `)`
    ///   2 |   |> LIMIT )
    ///     |            ^
    ///   = expected: <integer>
    ///   = help: …
    /// ```
    pub fn render(&self, src: &str) -> String {
        render_caret(
            self.code.as_str(),
            &self.msg,
            (self.at, self.end),
            &self.expected,
            self.help.as_deref(),
            src,
        )
    }
}

/// The `line:column` caret diagnostic of an error `code` at byte `span` of `src` — the
/// one renderer UQL and DecideText errors share.
pub(crate) fn render_caret(
    code: &str,
    msg: &str,
    span: (usize, usize),
    expected: &[String],
    help: Option<&str>,
    src: &str,
) -> String {
    let at = floor_char_boundary(src, span.0);
    let line_start = src[..at].rfind('\n').map_or(0, |n| n + 1);
    let line_end = src[at..].find('\n').map_or(src.len(), |n| at + n);
    let line_no = src[..at].matches('\n').count() + 1;
    let col = src[line_start..at].chars().count() + 1;
    let end = floor_char_boundary(src, span.1.clamp(at, line_end));
    let width = src[at..end].chars().count().max(1);
    let gutter = line_no.to_string().len();
    let mut out = format!(
        "{code} at {line_no}:{col}: {msg}\n  {line_no} | {}\n  {:gutter$} | {}{}",
        &src[line_start..line_end],
        "",
        " ".repeat(col - 1),
        "^".repeat(width),
    );
    if !expected.is_empty() {
        out.push_str(&format!("\n  = expected: {}", expected.join(", ")));
    }
    if let Some(help) = help {
        out.push_str(&format!("\n  = help: {help}"));
    }
    out
}

fn floor_char_boundary(src: &str, at: usize) -> usize {
    let mut at = at.min(src.len());
    while !src.is_char_boundary(at) {
        at -= 1;
    }
    at
}

impl std::fmt::Display for UqlError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} at byte {}: {}",
            self.code.as_str(),
            self.at,
            self.msg
        )
    }
}

impl std::error::Error for UqlError {}

impl From<LexError> for UqlError {
    fn from(e: LexError) -> Self {
        let code = UqlCode::from(e.kind);
        UqlError::new(code, e.msg, (e.at, e.at + 1))
    }
}

/// A non-fatal finding: the query parses, but probably does not mean what it says.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum UqlWarnCode {
    /// A transform (not a source) heads a pipeline: it runs over an empty RowSet.
    HeadTransformYieldsEmpty,
}

impl UqlWarnCode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::HeadTransformYieldsEmpty => "UQL_W_HEAD_TRANSFORM_YIELDS_EMPTY",
        }
    }
}

/// A warning with its span.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UqlWarning {
    pub code: UqlWarnCode,
    pub msg: String,
    pub at: usize,
    pub end: usize,
}
