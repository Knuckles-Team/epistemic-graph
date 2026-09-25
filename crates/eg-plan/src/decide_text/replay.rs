//! EH-528: bounded DecideText spelling for a typed walk-forward evaluation job.
//!
//! Names in the text select already-bound typed values. No parameter text is
//! substituted into a query or a policy. The resulting request is submitted
//! through the existing `DecisionEval` job, never executed on the request path.

use std::collections::BTreeMap;

use eg_types::contract::BoundedVec;
use eg_types::decision::jobs::DatasetSource;
use eg_types::decision::numeric::{QuantScaleTag, QuantisedValue};
use eg_types::decision::replay::{
    AllocationRule, EvalMode, ReplayEnvironment, ReplaySpec, SharedCap, TrialLog, WalkForward,
};
use eg_types::decision::statistical::TypedValue;
use eg_types::decision::{DecisionEvalRequest, DecisionPolicyRef, EvalCandidate, RecordWindow};

use super::lexer::{self, Spanned, Tok};
use super::{DecideTextError, DecideTextErrorKind};

/// Trusted typed bindings for a replay request. Text contains references only.
pub struct ReplayBindings<'a> {
    pub candidates: &'a BTreeMap<String, EvalCandidate>,
    pub policies: &'a BTreeMap<String, DecisionPolicyRef>,
    pub values: &'a BTreeMap<String, TypedValue>,
    pub source: DatasetSource,
    pub gold_set_digest: Option<String>,
    pub trials: TrialLog,
    pub idempotency_key: String,
}

struct ReplayParser<'a> {
    tokens: &'a [Spanned],
    pos: usize,
    end: usize,
}

impl ReplayParser<'_> {
    fn span(&self) -> (usize, usize) {
        self.tokens
            .get(self.pos)
            .map_or((self.end, self.end), |(_, span)| *span)
    }

    fn error(&self, kind: DecideTextErrorKind, message: impl Into<String>) -> DecideTextError {
        DecideTextError::new(kind, message, self.span())
    }

    fn keyword(&mut self, expected: &str) -> Result<(), DecideTextError> {
        match self.tokens.get(self.pos) {
            Some((Tok::Word(value), _)) if value.eq_ignore_ascii_case(expected) => {
                self.pos += 1;
                Ok(())
            }
            _ => Err(self
                .error(
                    DecideTextErrorKind::Syntax,
                    format!("expected `{expected}`"),
                )
                .expecting(vec![format!("`{expected}`")])),
        }
    }

    fn pipe(&mut self) -> Result<(), DecideTextError> {
        if matches!(self.tokens.get(self.pos), Some((Tok::Pipe, _))) {
            self.pos += 1;
            Ok(())
        } else {
            Err(self.error(DecideTextErrorKind::Syntax, "expected `|>`"))
        }
    }

    fn param(&mut self) -> Result<String, DecideTextError> {
        match self.tokens.get(self.pos) {
            Some((Tok::Param(value), _)) => {
                self.pos += 1;
                Ok(value.clone())
            }
            _ => Err(self.error(DecideTextErrorKind::Syntax, "expected a `$parameter`")),
        }
    }

    fn number(&mut self) -> Result<(String, (usize, usize)), DecideTextError> {
        let span = self.span();
        match self.tokens.get(self.pos) {
            Some((Tok::Word(value), _)) => {
                self.pos += 1;
                Ok((value.clone(), span))
            }
            _ => Err(self.error(DecideTextErrorKind::Syntax, "expected a number")),
        }
    }

    fn u32(&mut self, label: &str) -> Result<u32, DecideTextError> {
        let (text, span) = self.number()?;
        text.parse::<u32>().map_err(|_| {
            DecideTextError::new(
                DecideTextErrorKind::Syntax,
                format!("{label} must be an unsigned integer"),
                span,
            )
        })
    }

    fn budget(&mut self) -> Result<QuantisedValue, DecideTextError> {
        let (text, span) = self.number()?;
        let mut parts = text.split('.');
        let whole = parts.next().unwrap_or("");
        let fraction = parts.next().unwrap_or("");
        if parts.next().is_some()
            || whole.is_empty()
            || !whole.bytes().all(|b| b.is_ascii_digit())
            || !fraction.bytes().all(|b| b.is_ascii_digit())
            || fraction.len() > 12
        {
            return Err(DecideTextError::new(
                DecideTextErrorKind::Syntax,
                "BUDGET must be a nonnegative decimal with at most 12 fractional digits",
                span,
            ));
        }
        let unit = 1_000_000_000_000u64;
        let whole = whole.parse::<u64>().ok().and_then(|n| n.checked_mul(unit));
        let fractional = if fraction.is_empty() {
            Some(0)
        } else {
            fraction
                .parse::<u64>()
                .ok()
                .and_then(|n| n.checked_mul(10u64.pow((12 - fraction.len()) as u32)))
        };
        let value = whole
            .zip(fractional)
            .and_then(|(a, b)| a.checked_add(b))
            .and_then(|n| i64::try_from(n).ok())
            .filter(|n| *n > 0)
            .ok_or_else(|| {
                DecideTextError::new(DecideTextErrorKind::Syntax, "BUDGET is out of range", span)
            })?;
        Ok(QuantisedValue {
            scale: QuantScaleTag::Pico,
            value,
        })
    }

    fn timestamp(&mut self, values: &BTreeMap<String, TypedValue>) -> Result<u64, DecideTextError> {
        let span = self.span();
        let name = self.param()?;
        let value = values.get(&name).ok_or_else(|| {
            DecideTextError::new(
                DecideTextErrorKind::UnboundParameter,
                format!("${name} is not bound"),
                span,
            )
        })?;
        match value {
            TypedValue::Int(ms) => u64::try_from(*ms).map_err(|_| {
                DecideTextError::new(
                    DecideTextErrorKind::ParameterType,
                    "timestamp must be a nonnegative integer in milliseconds",
                    span,
                )
            }),
            _ => Err(DecideTextError::new(
                DecideTextErrorKind::ParameterType,
                "timestamp must be an integer in milliseconds",
                span,
            )),
        }
    }

    fn parse(
        &mut self,
        tenant_id: &str,
        bindings: ReplayBindings<'_>,
    ) -> Result<DecisionEvalRequest, DecideTextError> {
        self.keyword("CANDIDATES")?;
        let candidate_name = self.param()?;
        self.pipe()?;
        self.keyword("DECIDE")?;
        self.keyword("USING")?;
        let policy_name = self.param()?;
        self.pipe()?;
        self.keyword("REPLAY")?;
        self.keyword("WALK")?;
        self.keyword("FORWARD")?;
        self.keyword("TRAIN")?;
        let train = self.u32("TRAIN")?;
        self.keyword("TEST")?;
        let test = self.u32("TEST")?;
        self.keyword("STEP")?;
        let step = self.u32("STEP")?;
        self.keyword("PURGE")?;
        let purge = self.u32("PURGE")?;
        self.keyword("EMBARGO")?;
        let embargo = self.u32("EMBARGO")?;
        self.keyword("FROM")?;
        let from_ms = self.timestamp(bindings.values)?;
        self.keyword("TO")?;
        let to_ms = self.timestamp(bindings.values)?;
        self.keyword("BUDGET")?;
        let cap = self.budget()?;
        if self.pos != self.tokens.len() {
            return Err(self.error(DecideTextErrorKind::Syntax, "unexpected trailing clause"));
        }
        if train == 0 || test == 0 || step < test || from_ms >= to_ms {
            return Err(self.error(
                DecideTextErrorKind::Syntax,
                "walk-forward windows must be positive, non-overlapping and time-ordered",
            ));
        }
        let candidate = bindings.candidates.get(&candidate_name).ok_or_else(|| {
            self.error(
                DecideTextErrorKind::UnboundParameter,
                format!("${candidate_name} is not a bound evaluation candidate"),
            )
        })?;
        let policy = bindings.policies.get(&policy_name).ok_or_else(|| {
            self.error(
                DecideTextErrorKind::UnboundParameter,
                format!("${policy_name} is not a bound decision policy"),
            )
        })?;
        if bindings.idempotency_key.is_empty() || tenant_id.is_empty() {
            return Err(self.error(
                DecideTextErrorKind::ParameterType,
                "tenant and idempotency key are required",
            ));
        }
        Ok(DecisionEvalRequest {
            tenant_id: tenant_id.to_string(),
            idempotency_key: bindings.idempotency_key,
            candidate: candidate.clone(),
            policy: policy.clone(),
            estimators: BoundedVec::default(),
            gold_set_digest: bindings.gold_set_digest,
            window: RecordWindow { from_ms, to_ms },
            source: bindings.source,
            mode: EvalMode::Replay {
                spec: Box::new(ReplaySpec {
                    folds: WalkForward {
                        train,
                        test,
                        step,
                        purge,
                        embargo,
                    },
                    budget: SharedCap {
                        cap,
                        rule: AllocationRule::Proportional,
                    },
                    env: ReplayEnvironment::PolicyIndependent,
                    refit: None,
                    trials: bindings.trials,
                    incumbent: None,
                    supersedes: None,
                }),
            },
        })
    }
}

/// Parse the approved `REPLAY WALK FORWARD` spelling into a DecisionEval job.
pub fn parse_replay(
    src: &str,
    tenant_id: &str,
    bindings: ReplayBindings<'_>,
) -> Result<DecisionEvalRequest, DecideTextError> {
    let tokens = lexer::lex(src)?;
    ReplayParser {
        tokens: &tokens,
        pos: 0,
        end: src.len(),
    }
    .parse(tenant_id, bindings)
}
