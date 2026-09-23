//! Structured UQL diagnostics (UQL-02): a stable CODE an agent can branch on, the
//! byte SPAN of the offending text, what the parser EXPECTED there, and a fix HINT —
//! plus a human rendering with `line:column` and a caret under the exact span.

use super::lexer::{LexError, LexErrorKind};

/// The closed set of UQL error kinds. [`UqlCode::as_str`] is the stable wire name.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum UqlCode {
    // lexical
    UnexpectedCharacter,
    UnterminatedString,
    UnterminatedIdentifier,
    InvalidNumber,
    EmptyParameterName,
    // syntax
    UnexpectedToken,
    UnknownStage,
    ExpectedInteger,
    TrailingTokens,
    NestingTooDeep,
    InvalidRange,
    DecisionClauseInUql,
    UnsupportedVersion,
    DuplicateBinding,
    UnknownBinding,
    UnusedBinding,
    NullLiteral,
    StatementNotPipeline,
    CredentialBearingSpec,
    // parameters
    UnboundParameter,
    ParameterType,
    UnusedParameter,
    // build
    FeatureNotInBuild,
}

/// Every code (for docs and exhaustiveness tests).
pub const ALL_CODES: &[UqlCode] = &[
    UqlCode::UnexpectedCharacter,
    UqlCode::UnterminatedString,
    UqlCode::UnterminatedIdentifier,
    UqlCode::InvalidNumber,
    UqlCode::EmptyParameterName,
    UqlCode::UnexpectedToken,
    UqlCode::UnknownStage,
    UqlCode::ExpectedInteger,
    UqlCode::TrailingTokens,
    UqlCode::NestingTooDeep,
    UqlCode::InvalidRange,
    UqlCode::DecisionClauseInUql,
    UqlCode::UnsupportedVersion,
    UqlCode::DuplicateBinding,
    UqlCode::UnknownBinding,
    UqlCode::UnusedBinding,
    UqlCode::NullLiteral,
    UqlCode::StatementNotPipeline,
    UqlCode::CredentialBearingSpec,
    UqlCode::UnboundParameter,
    UqlCode::ParameterType,
    UqlCode::UnusedParameter,
    UqlCode::FeatureNotInBuild,
];

impl UqlCode {
    /// The stable name (`UQL_…`), what callers and agents match on.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::UnexpectedCharacter => "UQL_UNEXPECTED_CHARACTER",
            Self::UnterminatedString => "UQL_UNTERMINATED_STRING",
            Self::UnterminatedIdentifier => "UQL_UNTERMINATED_IDENTIFIER",
            Self::InvalidNumber => "UQL_INVALID_NUMBER",
            Self::EmptyParameterName => "UQL_EMPTY_PARAMETER_NAME",
            Self::UnexpectedToken => "UQL_UNEXPECTED_TOKEN",
            Self::UnknownStage => "UQL_UNKNOWN_STAGE",
            Self::ExpectedInteger => "UQL_EXPECTED_INTEGER",
            Self::TrailingTokens => "UQL_TRAILING_TOKENS",
            Self::NestingTooDeep => "UQL_NESTING_TOO_DEEP",
            Self::InvalidRange => "UQL_INVALID_RANGE",
            Self::DecisionClauseInUql => "DECISION_CLAUSE_IN_UQL",
            Self::UnsupportedVersion => "UQL_UNSUPPORTED_VERSION",
            Self::DuplicateBinding => "UQL_DUPLICATE_BINDING",
            Self::UnknownBinding => "UQL_UNKNOWN_BINDING",
            Self::UnusedBinding => "UQL_UNUSED_BINDING",
            Self::NullLiteral => "UQL_NULL_LITERAL",
            Self::StatementNotPipeline => "UQL_STATEMENT_NOT_PIPELINE",
            Self::CredentialBearingSpec => "UQL_CREDENTIAL_BEARING_SPEC",
            Self::UnboundParameter => "UQL_UNBOUND_PARAMETER",
            Self::ParameterType => "UQL_PARAMETER_TYPE",
            Self::UnusedParameter => "UQL_UNUSED_PARAMETER",
            Self::FeatureNotInBuild => "UQL_FEATURE_NOT_IN_BUILD",
        }
    }
}

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
        let at = floor_char_boundary(src, self.at);
        let line_start = src[..at].rfind('\n').map_or(0, |n| n + 1);
        let line_end = src[at..].find('\n').map_or(src.len(), |n| at + n);
        let line_no = src[..at].matches('\n').count() + 1;
        let col = src[line_start..at].chars().count() + 1;
        let end = floor_char_boundary(src, self.end.clamp(at, line_end));
        let width = src[at..end].chars().count().max(1);
        let gutter = line_no.to_string().len();
        let mut out = format!(
            "{} at {line_no}:{col}: {}\n  {line_no} | {}\n  {:gutter$} | {}{}",
            self.code.as_str(),
            self.msg,
            &src[line_start..line_end],
            "",
            " ".repeat(col - 1),
            "^".repeat(width),
        );
        if !self.expected.is_empty() {
            out.push_str(&format!("\n  = expected: {}", self.expected.join(", ")));
        }
        if let Some(help) = &self.help {
            out.push_str(&format!("\n  = help: {help}"));
        }
        out
    }
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
