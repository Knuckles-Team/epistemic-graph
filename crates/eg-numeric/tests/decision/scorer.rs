//! EH-291 / EH-294 / EH-296 / EH-297 / EH-300: the resident scorer's served
//! path -- fixed-point kernels, the cross-host golden vector, legality, the
//! shortlist, encode-once scoring and trajectory-prefix belief.

use std::time::Instant;

use eg_numeric::decision::scorer::fixed::{exp_non_positive, softmax, to_f64, ONE};
use eg_numeric::decision::scorer::forward::{read, EncodedState, Scorer, EXCLUDED_LOGIT};
use eg_numeric::decision::scorer::legal::{shortlist, LegalSet};
use eg_numeric::decision::trajectory::{belief, Prefix};
use eg_types::decision::digest::digest_text;
use eg_types::decision::statistical::head::{
    DecisionHeadBody, FeatureStandardisation, FittedRegime, HeadKind, DECISION_HEAD_SCHEMA_VERSION,
};
use eg_types::decision::statistical::scorer::{
    OptionAttentionParams, MAX_SCORER_FEATURES, MAX_SCORER_SHORTLIST, MAX_SCORER_WIDTH,
};
use eg_types::decision::{QuantScaleTag, QuantisedValue};

use super::common::{bounded, SCHEMA_DIGEST};

/// Digest of the golden fixture's integer outputs. Reproduced independently
/// by an integer-only Python port of the kernels; it must be the same on
/// every host (checked independently on different hardware).
const GOLDEN_DIGEST: &str =
    "sha256:918ab491393590240314694cae472deb42b0ebf9b7546ba019c1c29672ee1beb";
const GOLDEN_DOMAIN: &str = "eg/decision-scorer-golden/v1";
const WIDTH: usize = 4;
const FEATURES: usize = 2;
const OPTIONS: usize = 4;

fn q(value: i64) -> QuantisedValue {
    QuantisedValue {
        scale: QuantScaleTag::Q32,
        value,
    }
}

/// A deterministic parameter pattern in `[-0.5, 0.5]`.
fn pattern(n: usize, salt: usize) -> Vec<i64> {
    (0..n)
        .map(|k| (((k * 7 + salt * 13) % 17) as i64 - 8) << 28)
        .collect()
}

fn params(shortlist: u8) -> OptionAttentionParams {
    OptionAttentionParams {
        width: WIDTH as u8,
        shortlist,
        embed: bounded(pattern(FEATURES * WIDTH, 1)),
        embed_bias: bounded(pattern(WIDTH, 2)),
        query: bounded(pattern(WIDTH * WIDTH, 3)),
        key: bounded(pattern(WIDTH * WIDTH, 4)),
        value: bounded(pattern(WIDTH * WIDTH, 5)),
        self_weight: bounded(pattern(WIDTH, 6)),
        context_weight: bounded(pattern(WIDTH, 7)),
    }
}

fn head(shortlist: u8) -> DecisionHeadBody {
    let spec = FeatureStandardisation {
        center: q(ONE / 4),
        scale: q(2 * ONE),
        lower: q(-8 * ONE),
        upper: q(8 * ONE),
    };
    DecisionHeadBody {
        schema_version: DECISION_HEAD_SCHEMA_VERSION,
        kind: HeadKind::OptionAttention,
        regime: FittedRegime::FullLabel,
        feature_schema_digest: SCHEMA_DIGEST.to_string(),
        standardisation: bounded(vec![spec; FEATURES]),
        weights: bounded(vec![q(ONE), q(-ONE / 2)]),
        calibration: None,
        training_records_digest: "sha256:golden".to_string(),
        n_training: 0,
        synthetic: true,
        scorer: Some(Box::new(params(shortlist))),
    }
}

/// Raw `Q32` rows in `[-1.5, 1.5]`.
fn rows() -> Vec<Vec<i64>> {
    (0..OPTIONS)
        .map(|r| {
            (0..FEATURES)
                .map(|f| (((r * 5 + f * 3) % 7) as i64 - 3) << 31)
                .collect()
        })
        .collect()
}

fn views(rows: &[Vec<i64>]) -> Vec<&[i64]> {
    rows.iter().map(Vec::as_slice).collect()
}

#[test]
fn the_fixed_point_kernels_are_accurate_where_it_matters() {
    assert_eq!(exp_non_positive(0), ONE);
    let half = exp_non_positive(-2_977_044_472); // -ln 2
    assert!((half - ONE / 2).abs() < 16, "e^-ln2 = {half}");
    assert_eq!(exp_non_positive(-1000 * ONE), 0);
    let p = softmax(&[ONE, 0, -ONE, 3 * ONE]);
    let total: i64 = p.iter().sum();
    assert!((total - ONE).abs() < 8, "softmax mass {total}");
    assert!(p[3] > p[0] && p[0] > p[1] && p[1] > p[2]);
}

// spec: EG-DECISION-ENGINE-R091
#[test]
fn the_golden_vector_is_bit_identical_across_hosts() {
    let head = head(3);
    let rows = rows();
    let legal = LegalSet::derive(OPTIONS, &[]);
    let scoring = read(&head, &views(&rows), &legal)
        .expect("reads")
        .expect("in distribution");
    let exps: Vec<i64> = (0..40).map(|k| exp_non_positive(-k * (ONE / 3))).collect();
    let spread = softmax(&[ONE, 0, -ONE, 3 * ONE]);
    let digest = digest_text(
        GOLDEN_DOMAIN,
        &(&exps, &spread, &scoring.logits, &scoring.probabilities),
    );
    assert_eq!(
        scoring
            .logits
            .iter()
            .filter(|&&z| z == EXCLUDED_LOGIT)
            .count(),
        1,
        "shortlist 3 of 4: one option is never scored"
    );
    assert_eq!(
        digest, GOLDEN_DIGEST,
        "golden outputs: logits {:?} probabilities {:?}",
        scoring.logits, scoring.probabilities
    );
}

// spec: EG-DECISION-ENGINE-R009
#[test]
fn an_eliminated_option_is_never_scored_and_moves_nothing() {
    let head = head(8);
    let mut rows = rows();
    // Option 1 is eliminated by the deterministic rungs; give it a row that
    // would dominate attention and is out of range besides.
    rows[1] = vec![1_000 * ONE; FEATURES];
    let with_illegal = read(&head, &views(&rows), &LegalSet::derive(OPTIONS, &[1]))
        .expect("reads")
        .expect("the illegal row is never standardised, so never out of range");
    assert_eq!(with_illegal.probabilities[1], 0);
    assert_eq!(with_illegal.logits[1], EXCLUDED_LOGIT);
    assert!(
        with_illegal.standardised[1].is_empty(),
        "its row was never read"
    );

    let mut benign = rows.clone();
    benign[1] = vec![0; FEATURES];
    let reference = read(&head, &views(&benign), &LegalSet::derive(OPTIONS, &[1]))
        .expect("reads")
        .expect("in distribution");
    assert_eq!(
        with_illegal, reference,
        "an eliminated option's content cannot move any legal option's score"
    );
    let legal_mass: i64 = with_illegal.probabilities.iter().sum();
    assert!((legal_mass - ONE).abs() < 8);
}

// spec: EG-DECISION-ENGINE-R087
#[test]
fn the_shortlist_bounds_what_is_scored_and_breaks_ties_by_option_order() {
    // Top two: option 1 (9), then the 5-5 tie between options 0 and 2 goes to
    // the lower option index. The kept set is returned in option order.
    assert_eq!(shortlist(&[(0, 5), (1, 9), (2, 5), (3, 1)], 2), vec![0, 1]);
    // The tie-break is by option index, not by input position.
    assert_eq!(shortlist(&[(2, 5), (1, 9), (0, 5), (3, 1)], 2), vec![0, 1]);
    assert_eq!(
        shortlist(&[(0, 5), (1, 9), (2, 5), (3, 1)], 3),
        vec![0, 1, 2]
    );
    let head = head(2);
    let rows = rows();
    let scoring = read(&head, &views(&rows), &LegalSet::derive(OPTIONS, &[]))
        .expect("reads")
        .expect("in distribution");
    let scored = scoring.probabilities.iter().filter(|&&p| p > 0).count();
    assert_eq!(scored, 2, "a shortlist of two scores two options");
}

#[test]
fn one_encode_serves_several_questions_exactly() {
    let head = head(4);
    let scorer = Scorer::of(&head).expect("scorer");
    let rows = rows();
    let state =
        EncodedState::encode(&scorer, &views(&rows), &LegalSet::derive(OPTIONS, &[])).unwrap();
    for question in [vec![0, 2], vec![1, 2, 3]] {
        let eliminated: Vec<usize> = (0..OPTIONS).filter(|i| !question.contains(i)).collect();
        let alone = read(
            &head,
            &views(&rows),
            &LegalSet::derive(OPTIONS, &eliminated),
        )
        .unwrap()
        .unwrap();
        let shared = state.score(&scorer, &question);
        assert_eq!(shared.logits, alone.logits, "question {question:?}");
        assert_eq!(shared.probabilities, alone.probabilities);
    }
}

#[test]
fn trajectory_belief_reads_one_state_over_time() {
    let head = head(4);
    let early = rows();
    let mut late = rows();
    late[2] = vec![3 * ONE / 2; FEATURES];
    let (early_views, late_views) = (views(&early), views(&late));
    let prefixes = [
        Prefix {
            as_of_ms: 10,
            rows: &early_views,
        },
        Prefix {
            as_of_ms: 15,
            rows: &[],
        },
        Prefix {
            as_of_ms: 20,
            rows: &late_views,
        },
    ];
    let trajectory = belief(&head, &prefixes, OPTIONS).expect("believes");
    assert_eq!(trajectory.len(), 3);
    let first = trajectory[0].probabilities.as_ref().expect("in range");
    assert_eq!(
        trajectory[1].probabilities, None,
        "an empty slice has no belief"
    );
    let last = trajectory[2].probabilities.as_ref().expect("in range");
    assert_ne!(first, last, "the belief moves with the state");
    let single = read(&head, &early_views, &LegalSet::derive(OPTIONS, &[]))
        .unwrap()
        .unwrap();
    let exact: Vec<f64> = single.probabilities.iter().map(|&p| to_f64(p)).collect();
    assert_eq!(first, &exact, "each slice is one served reading");

    let reversed = [prefixes[2], prefixes[0]];
    let refusal = belief(&head, &reversed, OPTIONS).expect_err("time must increase");
    assert_eq!(refusal.code, "PARAMETER_INVALID");
}

/// A head shaped at the largest dimensions `OptionAttentionParams::check`
/// ever admits: `MAX_SCORER_WIDTH`, `MAX_SCORER_FEATURES`, `MAX_SCORER_SHORTLIST`.
fn largest_legal_head() -> DecisionHeadBody {
    let width = MAX_SCORER_WIDTH;
    let features = MAX_SCORER_FEATURES;
    let spec = FeatureStandardisation {
        center: q(ONE / 4),
        scale: q(2 * ONE),
        lower: q(-8 * ONE),
        upper: q(8 * ONE),
    };
    DecisionHeadBody {
        schema_version: DECISION_HEAD_SCHEMA_VERSION,
        kind: HeadKind::OptionAttention,
        regime: FittedRegime::FullLabel,
        feature_schema_digest: SCHEMA_DIGEST.to_string(),
        standardisation: bounded(vec![spec; features]),
        weights: bounded(pattern(features, 0).into_iter().map(q).collect()),
        calibration: None,
        training_records_digest: "sha256:golden".to_string(),
        n_training: 0,
        synthetic: true,
        scorer: Some(Box::new(OptionAttentionParams {
            width: width as u8,
            shortlist: MAX_SCORER_SHORTLIST as u8,
            embed: bounded(pattern(features * width, 1)),
            embed_bias: bounded(pattern(width, 2)),
            query: bounded(pattern(width * width, 3)),
            key: bounded(pattern(width * width, 4)),
            value: bounded(pattern(width * width, 5)),
            self_weight: bounded(pattern(width, 6)),
            context_weight: bounded(pattern(width, 7)),
        })),
    }
}

/// The total scalar parameter count of a head's scorer, the way its wire
/// bytes actually lay it out (EH-291/EH-300). `weights` is the linear head's
/// own parameter vector, read by every head kind; `scorer` is additional.
fn parameter_count(head: &DecisionHeadBody) -> usize {
    head.weights.len()
        + head.scorer.as_ref().map_or(0, |p| {
            p.embed.len()
                + p.embed_bias.len()
                + p.query.len()
                + p.key.len()
                + p.value.len()
                + p.self_weight.len()
                + p.context_weight.len()
        })
}

/// EG-DECISION-ENGINE-R090: the resident scorer is a small compiled-in Rust
/// struct, not a GPU or sidecar model. The largest shape
/// `OptionAttentionParams::check` ever admits still holds far under the
/// declared "a few megabytes" footprint, and a served reading over the
/// largest `Decide` batch (`MAX_DECIDE_RECORDS`) stays at a small fraction of
/// the declared single-digit-millisecond CPU budget.
#[test]
fn the_largest_legal_scorer_stays_inside_its_resident_cpu_and_memory_budget() {
    const UNIVERSE: usize = 256; // eg_types::decision::statistical::MAX_DECIDE_RECORDS
    let head = largest_legal_head();

    let params = parameter_count(&head);
    let footprint_bytes = params * std::mem::size_of::<i64>();
    assert!(
        params < 700_000,
        "the largest legal scorer holds {params} parameters, past the declared budget"
    );
    assert!(
        footprint_bytes < 2 * 1024 * 1024,
        "the largest legal scorer's resident footprint is {footprint_bytes} bytes, past a few megabytes"
    );

    let rows: Vec<Vec<i64>> = (0..UNIVERSE)
        .map(|r| {
            (0..MAX_SCORER_FEATURES)
                .map(|f| (((r * 5 + f * 3) % 7) as i64 - 3) << 28)
                .collect()
        })
        .collect();
    let views: Vec<&[i64]> = rows.iter().map(Vec::as_slice).collect();
    let legal = LegalSet::derive(UNIVERSE, &[]);

    // Warm the allocator once, then measure the compiled scorer's own
    // per-decision CPU cost: encode plus every question's attention pass.
    read(&head, &views, &legal)
        .expect("reads")
        .expect("in distribution");
    const ITERATIONS: u32 = 50;
    let started = Instant::now();
    for _ in 0..ITERATIONS {
        read(&head, &views, &legal)
            .expect("reads")
            .expect("in distribution");
    }
    let per_decision = started.elapsed() / ITERATIONS;
    assert!(
        per_decision.as_millis() < 10,
        "the largest legal scorer took {per_decision:?} per decision, past single-digit milliseconds"
    );
}

/// EG-DECISION-ENGINE-R083: the scorer reads each option's STRUCTURED
/// feature row only (EH-292) -- never a serialized text label -- so its
/// per-option width is the feature schema's fixed cardinality. An option
/// count this large would overrun any plausible text-token budget if an
/// option were instead described to a model as text; scored from structured
/// facts, width never depends on option count or label length, and scoring
/// still succeeds.
#[test]
fn a_large_option_set_scores_because_width_is_bounded_by_cardinality_not_text() {
    const LARGE_OPTION_COUNT: usize = 4_000;
    // A generous per-option label estimate (a realistic tool/agent summary)
    // and a common chars-per-token ratio, so the contrast is not an
    // arbitrarily chosen number.
    const NOTIONAL_LABEL_CHARS: usize = 120;
    const CHARS_PER_TOKEN: usize = 4;
    const PLAUSIBLE_TOKEN_BUDGET: usize = 128_000;
    let notional_text_tokens = (LARGE_OPTION_COUNT * NOTIONAL_LABEL_CHARS) / CHARS_PER_TOKEN;
    assert!(
        notional_text_tokens > PLAUSIBLE_TOKEN_BUDGET,
        "fixture is not large enough to make the point: {notional_text_tokens} notional tokens"
    );

    let head = head(8);
    let rows: Vec<Vec<i64>> = (0..LARGE_OPTION_COUNT)
        .map(|r| {
            (0..FEATURES)
                .map(|f| (((r * 11 + f * 5) % 13) as i64 - 6) << 29)
                .collect()
        })
        .collect();
    let views: Vec<&[i64]> = rows.iter().map(Vec::as_slice).collect();
    let legal = LegalSet::derive(LARGE_OPTION_COUNT, &[]);

    let scoring = read(&head, &views, &legal)
        .expect("reads")
        .expect("in distribution");

    // Every legal option's standardised row -- all `LARGE_OPTION_COUNT` of
    // them -- is exactly `FEATURES` wide: the schema's declared cardinality,
    // never a function of option count or any notional label length.
    let legal_rows: Vec<&Vec<i64>> = scoring
        .standardised
        .iter()
        .filter(|row| !row.is_empty())
        .collect();
    assert_eq!(legal_rows.len(), LARGE_OPTION_COUNT);
    for row in legal_rows {
        assert_eq!(
            row.len(),
            FEATURES,
            "feature width must equal the schema's fixed cardinality"
        );
    }

    // The shortlist still bounds how many of those legal options are scored,
    // independent of how many were legal. A scored option's logit is never
    // `EXCLUDED_LOGIT`; this count is exact (unlike a probability, it is
    // never at risk of rounding to zero).
    assert_eq!(scoring.probabilities.len(), LARGE_OPTION_COUNT);
    assert_eq!(scoring.logits.len(), LARGE_OPTION_COUNT);
    let scored_count = scoring
        .logits
        .iter()
        .filter(|&&logit| logit != EXCLUDED_LOGIT)
        .count();
    assert_eq!(
        scored_count,
        usize::from(head.scorer.as_ref().unwrap().shortlist)
    );
}
