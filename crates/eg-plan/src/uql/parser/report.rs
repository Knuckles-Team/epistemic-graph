//! Error positioning and warnings: spans, source text, typed diagnostics.

use super::Parser;
use crate::uql::diag::{UqlCode, UqlError, UqlWarning};

impl<'a> Parser<'a> {
    /// Span of the current token (or end of input).
    pub(super) fn cur_span(&self) -> (usize, usize) {
        self.toks
            .get(self.pos)
            .map_or((self.end, self.end), |t| (t.start, t.end))
    }

    /// Byte offset of the previously consumed token.
    pub(super) fn prev_start(&self) -> usize {
        self.prev_span().0
    }

    pub(super) fn prev_span(&self) -> (usize, usize) {
        self.pos
            .checked_sub(1)
            .and_then(|i| self.toks.get(i))
            .map_or((self.end, self.end), |t| (t.start, t.end))
    }

    /// The source text of the token at `index`.
    pub(super) fn text_of(&self, index: usize) -> &'a str {
        self.toks
            .get(index)
            .map_or("", |t| &self.src[t.start..t.end])
    }

    /// An error of `code` anchored at the current token, naming what was found.
    pub(super) fn error(&self, code: UqlCode, msg: &str) -> UqlError {
        let found = match self.peek_kind() {
            Some(t) => format!(", found {t}"),
            None => ", found end of input".to_string(),
        };
        UqlError::new(code, format!("{msg}{found}"), self.cur_span())
    }

    /// An `UQL_UNEXPECTED_TOKEN` at the current token.
    pub(super) fn err_here(&self, msg: &str) -> UqlError {
        self.error(UqlCode::UnexpectedToken, msg)
    }

    /// An `UQL_UNEXPECTED_TOKEN` at an explicit offset.
    pub(super) fn err_at(&self, at: usize, msg: &str) -> UqlError {
        UqlError::new(UqlCode::UnexpectedToken, msg, (at, at + 1))
    }

    /// The refusal of a clause whose executor this build lacks (only a build missing
    /// one of the gated features has such a clause).
    #[cfg(not(all(
        feature = "text",
        feature = "owl",
        feature = "wasm-udf",
        feature = "federation",
        feature = "geo",
        feature = "tensor",
        feature = "stream",
        feature = "timeseries",
        feature = "probabilistic",
        feature = "epistemic"
    )))]
    pub(super) fn not_built(&self, feature: &str) -> UqlError {
        let clause = self
            .text_of(self.pos.saturating_sub(1))
            .to_ascii_uppercase();
        UqlError::new(
            UqlCode::FeatureNotInBuild,
            format!("`{clause}` requires build feature `{feature}`; not available in this build"),
            self.prev_span(),
        )
        .with_help(format!("build epistemic-graph with `--features {feature}`"))
    }

    pub(super) fn warn(&mut self, warning: UqlWarning) {
        self.warnings.push(warning);
    }

    pub(super) fn take_warnings(&mut self) -> Vec<UqlWarning> {
        std::mem::take(&mut self.warnings)
    }
}
