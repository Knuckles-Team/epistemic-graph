//! The scorer's forward pass, in fixed point (EH-291, EH-292, EH-300).
//!
//! Tokens are options: each legal option's structured feature row,
//! standardised on Q32. The state is encoded ONCE ([`EncodedState::encode`]:
//! embedding plus query/key/value projections per token); each question then
//! scores its options at their own markers ([`EncodedState::score`]): option
//! `i` attends over the question's options, and its logit is the head's
//! linear logit plus its own embedding and its attended context, each read
//! through a weight vector. Several questions over one state share one encode.
//!
//! Every option outside the scored set -- eliminated by the deterministic
//! rungs, or ranked out by the shortlist -- reports [`EXCLUDED_LOGIT`] and
//! probability zero. It was never a token.

use eg_types::decision::statistical::head::{DecisionHeadBody, FeatureStandardisation};
use eg_types::decision::statistical::scorer::OptionAttentionParams;
use eg_types::decision::statistical::StatisticalErrorCode;

use super::fixed::{add, dot, mul, saturate, softmax, to_q32, FRAC_BITS, ONE};
use super::legal::{shortlist, LegalSet};
use crate::decision::refusal::{Refusal, RefusalResult};

/// The logit reported for an option that was not scored.
pub const EXCLUDED_LOGIT: i64 = -(1 << 62);

/// A head's scorer, resolved against its body.
#[derive(Debug, Clone)]
pub struct Scorer<'a> {
    params: &'a OptionAttentionParams,
    standardisation: &'a [FeatureStandardisation],
    weights: Vec<i64>,
    inverse_temperature: i64,
    width: usize,
}

/// One encoded option.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Token {
    option: usize,
    prescore: i64,
    hidden: Vec<i64>,
    query: Vec<i64>,
    key: Vec<i64>,
    value: Vec<i64>,
}

/// The state, encoded once over the shortlisted legal options.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EncodedState {
    universe: usize,
    standardised: Vec<Vec<i64>>,
    tokens: Vec<Token>,
}

/// Why a state could not be encoded: an option's feature out of the range
/// the head was fitted on (drift: the decision abstains).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OutOfDistribution {
    pub option: usize,
    pub feature: usize,
}

/// One question's scores over the whole option universe, all Q32.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Scoring {
    /// Standardised rows; empty for an option that was never read.
    pub standardised: Vec<Vec<i64>>,
    pub logits: Vec<i64>,
    pub probabilities: Vec<i64>,
}

/// Standardise one row on Q32 with integer division; `Err(feature)` when a
/// value lies outside the fitted range.
pub fn standardise(spec: &[FeatureStandardisation], row: &[i64]) -> Result<Vec<i64>, usize> {
    let mut out = Vec::with_capacity(row.len());
    for (index, (&raw, s)) in row.iter().zip(spec).enumerate() {
        if raw < to_q32(s.lower) || raw > to_q32(s.upper) {
            return Err(index);
        }
        let centred = i128::from(raw) - i128::from(to_q32(s.center));
        out.push(saturate(
            (centred << FRAC_BITS) / i128::from(to_q32(s.scale).max(1)),
        ));
    }
    Ok(out)
}

/// `W^T h` for a `width x width` matrix stored input-major.
fn project(matrix: &[i64], hidden: &[i64], width: usize) -> Vec<i64> {
    (0..width)
        .map(|b| {
            let column: Vec<i64> = (0..width).map(|a| matrix[a * width + b]).collect();
            dot(hidden, &column)
        })
        .collect()
}

/// Softmax of `inverse_temperature x logits` over the scored options; an
/// [`EXCLUDED_LOGIT`] option gets probability zero.
pub fn served_probabilities(logits: &[i64], inverse_temperature: i64) -> Vec<i64> {
    let scored: Vec<i64> = logits
        .iter()
        .filter(|&&z| z != EXCLUDED_LOGIT)
        .map(|&z| mul(z, inverse_temperature))
        .collect();
    let mut distribution = softmax(&scored).into_iter();
    logits
        .iter()
        .map(|&z| {
            if z == EXCLUDED_LOGIT {
                0
            } else {
                distribution.next().unwrap_or(0)
            }
        })
        .collect()
}

/// Multiply-accumulates one decision costs: pre-scores over the legal set,
/// the encode over the scored set, and attention within it.
pub fn macs(features: usize, width: usize, legal: usize, scored: usize) -> u64 {
    let (f, d, l, s) = (features as u64, width as u64, legal as u64, scored as u64);
    l * f + s * (f * d + 3 * d * d + 2 * d) + 2 * s * s * d
}

impl<'a> Scorer<'a> {
    /// The scorer of an `OptionAttention` head.
    pub fn of(head: &'a DecisionHeadBody) -> RefusalResult<Self> {
        let params = head.scorer.as_deref().ok_or_else(|| {
            Refusal::new(
                StatisticalErrorCode::HeadInvalid,
                "the head carries no scorer parameters",
            )
        })?;
        params
            .check(head.weights.len())
            .map_err(|detail| Refusal::new(StatisticalErrorCode::HeadInvalid, detail))?;
        Ok(Self {
            params,
            standardisation: head.standardisation.as_slice(),
            weights: head.weights.iter().map(|w| to_q32(*w)).collect(),
            inverse_temperature: head
                .calibration
                .as_ref()
                .map_or(ONE, |c| to_q32(c.inverse_temperature)),
            width: usize::from(params.width),
        })
    }

    /// Scored options at most.
    pub fn shortlist_limit(&self) -> usize {
        usize::from(self.params.shortlist)
    }

    /// The embedding width.
    pub fn width(&self) -> usize {
        self.width
    }

    /// The linear logit `w . x`: the pre-score the shortlist ranks by.
    pub fn prescore(&self, standardised: &[i64]) -> i64 {
        dot(&self.weights, standardised)
    }

    fn token(&self, option: usize, x: &[i64]) -> Token {
        let d = self.width;
        let p = self.params;
        let hidden: Vec<i64> = (0..d)
            .map(|j| {
                let column: Vec<i64> = (0..x.len())
                    .map(|f| p.embed.as_slice()[f * d + j])
                    .collect();
                add(dot(x, &column), p.embed_bias.as_slice()[j]).max(0)
            })
            .collect();
        Token {
            option,
            prescore: self.prescore(x),
            query: project(p.query.as_slice(), &hidden, d),
            key: project(p.key.as_slice(), &hidden, d),
            value: project(p.value.as_slice(), &hidden, d),
            hidden,
        }
    }
}

impl EncodedState {
    /// Encode the legal options of `rows` (one row per option in the
    /// universe): standardise every legal row, shortlist by pre-score, and
    /// embed and project the shortlisted ones. Rows of eliminated options are
    /// never read.
    pub fn encode(
        scorer: &Scorer,
        rows: &[&[i64]],
        legal: &LegalSet,
    ) -> Result<Self, OutOfDistribution> {
        let mut standardised = vec![Vec::new(); legal.universe()];
        let mut prescored = Vec::with_capacity(legal.members().len());
        for &option in legal.members() {
            let x = standardise(scorer.standardisation, rows[option])
                .map_err(|feature| OutOfDistribution { option, feature })?;
            prescored.push((option, scorer.prescore(&x)));
            standardised[option] = x;
        }
        let tokens = shortlist(&prescored, scorer.shortlist_limit())
            .into_iter()
            .map(|option| scorer.token(option, &standardised[option]))
            .collect();
        Ok(Self {
            universe: legal.universe(),
            standardised,
            tokens,
        })
    }

    /// The options that became tokens, ascending.
    pub fn scored_options(&self) -> Vec<usize> {
        self.tokens.iter().map(|t| t.option).collect()
    }

    /// Score one question over `options` (a subset of the encoded options;
    /// any other option is excluded from it).
    pub fn score(&self, scorer: &Scorer, options: &[usize]) -> Scoring {
        let members: Vec<&Token> = self
            .tokens
            .iter()
            .filter(|t| options.contains(&t.option))
            .collect();
        let mut logits = vec![EXCLUDED_LOGIT; self.universe];
        for token in &members {
            logits[token.option] = marker_logit(scorer, token, &members);
        }
        Scoring {
            standardised: self.standardised.clone(),
            probabilities: served_probabilities(&logits, scorer.inverse_temperature),
            logits,
        }
    }
}

/// Option `token`'s logit: it attends over `members` at its own marker.
fn marker_logit(scorer: &Scorer, token: &Token, members: &[&Token]) -> i64 {
    let attention = softmax(
        &members
            .iter()
            .map(|other| dot(&token.query, &other.key))
            .collect::<Vec<_>>(),
    );
    let context: Vec<i64> = (0..scorer.width)
        .map(|b| {
            let column: Vec<i64> = members.iter().map(|other| other.value[b]).collect();
            dot(&attention, &column)
        })
        .collect();
    let own = dot(scorer.params.self_weight.as_slice(), &token.hidden);
    let read = dot(scorer.params.context_weight.as_slice(), &context);
    add(add(token.prescore, own), read)
}

/// Read an `OptionAttention` head over one question: every legal option of
/// `rows`, scored at most `shortlist` at a time.
pub fn read(
    head: &DecisionHeadBody,
    rows: &[&[i64]],
    legal: &LegalSet,
) -> RefusalResult<Result<Scoring, OutOfDistribution>> {
    let scorer = Scorer::of(head)?;
    Ok(EncodedState::encode(&scorer, rows, legal)
        .map(|state| state.score(&scorer, &state.scored_options())))
}
