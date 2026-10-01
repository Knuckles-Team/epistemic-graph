//! Every record against replay: flip records under revisions and point-in-time
//! views, the latest-state scanner, flip confidence and backtest-run records.

use super::backtest_run::{seal, verify};
use super::confidence::flip_confidence;
use super::golden_tests::{daily, request, series};
use super::resolve::resolve;
use super::scan::SignalIndex;
use super::signal::{advance, initial_state, replay, signal_key};
use super::{
    BacktestRunDraft, BarRecord, CandleBasis, CostModel, DataRevisionRef, DataStatus, Direction,
    FillRule, FinalityFilter, FlipAbstainReason, FlipConfidence, FlipConfidenceRequest,
    FlipFeatures, FlipOutcomeSample, FlipRecordStatus, IndicatorKind, IndicatorSpec, ScanFilter,
    SignalState, Timeframe, TradeFill, UniverseMember, ValidationInputs, INVALID_REQUEST,
    LOOK_AHEAD,
};

const DAY: i64 = super::calendar::NS_PER_DAY;

fn spec() -> IndicatorSpec {
    IndicatorSpec {
        version: 1,
        kind: IndicatorKind::SuperTrend {
            atr_period: 10,
            multiplier_milli: 3_000,
            basis: CandleBasis::Raw,
        },
    }
}

/// The original series plus a correction of the first flip's bar, known 30
/// days after the bar closed, that moves its close to its low.
fn revised() -> (Vec<BarRecord>, i64) {
    let bars = daily();
    let first_flip = replay(&request(bars.clone(), None)).unwrap().current[0].clone();
    let original = bars
        .iter()
        .find(|b| b.open_time == first_flip.bar_open)
        .unwrap();
    let mut correction = original.clone();
    correction.revision = 1;
    correction.close = correction.low;
    correction.known_at = original.close_time + 30 * DAY;
    let known = correction.known_at;
    let mut all = bars;
    all.push(correction);
    (all, known)
}

#[test]
fn a_revision_appends_revision_records_and_never_rewrites() {
    let (records, revised_at) = revised();
    let before = replay(&request(daily(), None)).unwrap();
    let after = replay(&request(records.clone(), None)).unwrap();
    // The prefix known before the correction is exactly the original record log.
    let prefix: Vec<_> = after
        .records
        .iter()
        .filter(|r| r.recorded_at < revised_at)
        .cloned()
        .collect();
    let original_prefix: Vec<_> = before
        .records
        .iter()
        .filter(|r| r.recorded_at < revised_at)
        .cloned()
        .collect();
    assert_eq!(prefix, original_prefix);
    // The correction changed the flip history, and said so with linked records.
    assert_ne!(after.current, before.current);
    let revisions: Vec<_> = after
        .records
        .iter()
        .filter(|r| r.revises.is_some())
        .collect();
    assert!(!revisions.is_empty());
    for revision in revisions {
        let prior = revision.revises.as_deref().unwrap();
        let position = after
            .records
            .iter()
            .position(|r| r.record_id == prior)
            .unwrap();
        assert!(after.records[position].recorded_at <= revision.recorded_at);
    }
    assert!(
        after
            .records
            .iter()
            .any(|r| r.status == FlipRecordStatus::Retracted)
            || after
                .records
                .iter()
                .any(|r| r.revises.is_some() && r.status == FlipRecordStatus::Emitted)
    );
    // The replayed state equals a fresh advance over the latest resolved bars.
    let latest = resolve(&records, None, FinalityFilter::FinalOnly).unwrap();
    let key = signal_key(&series(), &spec()).unwrap();
    let (state, flips) = advance(&initial_state(key, spec()), &latest).unwrap();
    assert_eq!(after.state, state);
    assert_eq!(after.current, flips);
}

#[test]
fn a_point_in_time_replay_sees_only_what_was_known() {
    let (records, revised_at) = revised();
    let then = replay(&request(records.clone(), Some(revised_at - 1))).unwrap();
    let original = replay(&request(daily(), Some(revised_at - 1))).unwrap();
    assert_eq!(then, original);
    let as_of_bars = resolve(&records, Some(revised_at - 1), FinalityFilter::FinalOnly).unwrap();
    let key = signal_key(&series(), &spec()).unwrap();
    let (state, _) = advance(&initial_state(key, spec()), &as_of_bars).unwrap();
    assert_eq!(then.state, state);
}

#[test]
fn replay_is_order_free_and_idempotent_on_duplicate_appends() {
    let (mut records, _) = revised();
    let forward = replay(&request(records.clone(), None)).unwrap();
    records.reverse();
    let duplicated: Vec<BarRecord> = records
        .iter()
        .chain(records.iter().take(40))
        .cloned()
        .collect();
    assert_eq!(replay(&request(records, None)).unwrap(), forward);
    assert_eq!(replay(&request(duplicated, None)).unwrap(), forward);
}

#[test]
fn a_stale_or_empty_series_reports_its_data_status() {
    let bars = daily();
    let last = bars.last().unwrap().close_time;
    let mut stale = request(bars, Some(last + 10 * DAY));
    stale.stale_after = Some(3 * DAY);
    assert_eq!(replay(&stale).unwrap().state.data_status, DataStatus::Stale);
    let empty = replay(&request(Vec::new(), None)).unwrap();
    assert_eq!(empty.state.data_status, DataStatus::Unavailable);
    let warming = replay(&request(daily()[..5].to_vec(), None)).unwrap();
    assert_eq!(warming.state.data_status, DataStatus::Warming);
    assert_eq!(warming.state.direction, None);
    assert_eq!(warming.state.last_flip_at, None);
}

fn state_for(listing: &str, bars: &[BarRecord]) -> SignalState {
    let mut identity = series();
    identity.listing_id = listing.to_string();
    let mut replay_request = request(bars.to_vec(), None);
    replay_request.series = identity;
    replay(&replay_request).unwrap().state
}

#[test]
fn the_scanner_keeps_one_latest_state_per_key_and_counts_against_it() {
    let bars = daily();
    let mut index = SignalIndex::default();
    let newest = state_for("a", &bars);
    let older = state_for("a", &bars[..900]);
    assert!(index.upsert(newest.clone()));
    assert!(!index.upsert(older));
    assert!(index.upsert(state_for("b", &bars[..900])));
    assert!(index.upsert(state_for("c", &bars[..4])));
    assert_eq!(index.len(), 3);
    assert_eq!(index.get(&newest.key.digest), Some(&newest));
    let page = index.scan(&ScanFilter::default(), 2);
    assert_eq!(page.counts.total, 3);
    assert_eq!(page.counts.warming, 1);
    assert_eq!(page.counts.bullish + page.counts.bearish, 2);
    assert_eq!(page.superseded, 1);
    assert_eq!(page.rows.len(), 2);
    assert!(page.rows[0].last_flip_at >= page.rows[1].last_flip_at);
    let bullish = ScanFilter {
        direction: Some(Direction::Bullish),
        ..ScanFilter::default()
    };
    let only = index.scan(&bullish, 10);
    assert!(only
        .rows
        .iter()
        .all(|row| row.direction == Some(Direction::Bullish)));
    let weekly = ScanFilter {
        timeframe: Some(Timeframe::Week),
        ..ScanFilter::default()
    };
    assert_eq!(index.scan(&weekly, 10).counts.total, 0);
}

fn sample(at: i64, agree: bool, followed_through: bool, regime: u32) -> FlipOutcomeSample {
    FlipOutcomeSample {
        effective_at: at,
        direction: Direction::Bullish,
        features: FlipFeatures {
            timeframe_agreement: agree,
            above_200w_sma: false,
            regime,
        },
        followed_through,
    }
}

fn confidence(history: Vec<FlipOutcomeSample>, agree: bool) -> FlipConfidenceRequest {
    FlipConfidenceRequest {
        indicator_version: "super_trend@1".to_string(),
        timeframe: Timeframe::Day,
        asset_class: "crypto".to_string(),
        horizon_bars: 10,
        direction: Direction::Bullish,
        features: FlipFeatures {
            timeframe_agreement: agree,
            above_200w_sma: false,
            regime: 0,
        },
        data_status: DataStatus::Valid,
        history,
        alpha_permille: 100,
        n_min: 10,
    }
}

/// 60 flips alternating between an always-follows cell and a never-follows cell.
fn separated() -> Vec<FlipOutcomeSample> {
    (0..60)
        .map(|i| sample(i, i % 2 == 0, i % 2 == 0, 0))
        .collect()
}

fn reason(outcome: FlipConfidence) -> FlipAbstainReason {
    match outcome {
        FlipConfidence::Abstained { reason, .. } => reason,
        FlipConfidence::Calibrated { .. } => panic!("expected an abstention, got {outcome:?}"),
    }
}

#[test]
fn a_separated_history_gives_a_singleton_claim_either_way() {
    let yes = flip_confidence(&confidence(separated(), true)).unwrap();
    assert!(matches!(
        yes,
        FlipConfidence::Calibrated {
            follows_through: true,
            n_calibration: 60,
            ..
        }
    ));
    let no = flip_confidence(&confidence(separated(), false)).unwrap();
    assert!(matches!(
        no,
        FlipConfidence::Calibrated {
            follows_through: false,
            ..
        }
    ));
    assert_eq!(
        flip_confidence(&confidence(separated(), true)).unwrap(),
        yes
    );
}

#[test]
fn warmup_thin_history_regime_drift_and_ambiguity_abstain() {
    let mut warming = confidence(separated(), true);
    warming.data_status = DataStatus::Warming;
    let warm = reason(flip_confidence(&warming).unwrap());
    assert_eq!(
        warm,
        FlipAbstainReason::DataNotValid {
            data_status: DataStatus::Warming
        }
    );
    let thin = reason(flip_confidence(&confidence(separated()[..5].to_vec(), true)).unwrap());
    assert_eq!(
        thin,
        FlipAbstainReason::InsufficientHistory { n: 5, n_min: 10 }
    );
    let mut other_regime = confidence(separated(), true);
    other_regime.features.regime = 7;
    let regime = reason(flip_confidence(&other_regime).unwrap());
    assert_eq!(
        regime,
        FlipAbstainReason::RegimeUnsupported {
            regime: 7,
            n: 0,
            n_min: 10
        }
    );
    let drifting: Vec<_> = (0..60).map(|i| sample(i, true, i < 30, 0)).collect();
    let drift = reason(flip_confidence(&confidence(drifting, true)).unwrap());
    assert!(matches!(drift, FlipAbstainReason::Drift { .. }));
    let mixed: Vec<_> = (0..60).map(|i| sample(i, true, i % 2 == 0, 0)).collect();
    let ambiguous = reason(flip_confidence(&confidence(mixed, true)).unwrap());
    assert_eq!(ambiguous, FlipAbstainReason::AmbiguousSet);
    let mut bad = confidence(separated(), true);
    bad.alpha_permille = 0;
    assert_eq!(flip_confidence(&bad).unwrap_err().code, INVALID_REQUEST);
}

fn draft() -> BacktestRunDraft {
    let returns: Vec<f64> = (0..48)
        .map(|i| f64::from((i * 37 % 11) - 4) / 1_000.0)
        .collect();
    BacktestRunDraft {
        strategy: "super_trend@1 long-only".to_string(),
        signal_keys: vec![signal_key(&series(), &spec()).unwrap().digest],
        data_revisions: vec![DataRevisionRef {
            series_id: "bars/binance/BTC-USDT/1D".to_string(),
            source_revision: replay(&request(daily(), None))
                .unwrap()
                .state
                .source_revision,
            known_as_of: 1_000 * DAY,
        }],
        universe: vec![UniverseMember {
            listing_id: "binance:BTC/USDT:spot".to_string(),
            from: 0,
            until: Some(900 * DAY),
        }],
        costs: CostModel {
            fee_bps: 10,
            slippage_bps: 5,
        },
        fill_rule: FillRule::NextBarOpen,
        fills: vec![TradeFill {
            listing_id: "binance:BTC/USDT:spot".to_string(),
            known_at: 100 * DAY,
            fill_at: 100 * DAY,
            fill_price: 101_000,
            direction: Direction::Bullish,
        }],
        returns,
        validation: ValidationInputs {
            n_groups: 6,
            n_test_groups: 2,
            purge_window: 2,
            embargo: 1,
            n_trials: 5,
            // One per-period row per return. Variant 0 wins in every train
            // and test set, regardless of the 15 CPCV combinations.
            performance: vec![vec![1.0, 0.2, 0.1]; 48],
        },
        supersedes: None,
    }
}

#[test]
fn a_backtest_run_seals_with_mandatory_outputs_and_verifies_by_replay() {
    let run = seal(&draft()).unwrap();
    assert!(run.informational_only);
    assert_eq!(run.validation.cpcv_splits, 15);
    assert!(run.validation.cpcv_min_train > 0);
    assert!((0.0..=1.0).contains(&run.validation.deflated_sharpe));
    assert!((0.0..=1.0).contains(&run.validation.probability_backtest_overfit));
    assert!(verify(&run).unwrap());
    assert_eq!(seal(&draft()).unwrap(), run);
    eprintln!("backtest run digest {}", run.digest);
    let mut tampered = run.clone();
    tampered.validation.deflated_sharpe = 0.99;
    assert!(!verify(&tampered).unwrap());
    let mut revision = draft();
    revision.supersedes = Some(run.digest.clone());
    assert_ne!(seal(&revision).unwrap().digest, run.digest);
}

/// EH-517: the sealed record of the fixed draft, pinned. The validation outputs
/// run on the pinned soft-float kernel, so this digest is the same on every build
/// host; the lane's runs pass this test unchanged on two hosts.
const PINNED_RUN_DIGEST: &str =
    "sha256:991112d2c9fe33d30edab5e67a0a522a0d30a3521c5d0e91138a0c8992b814bc";

#[test]
fn a_sealed_backtest_run_digest_is_pinned_across_hosts() {
    let run = seal(&draft()).unwrap();
    let validation = &run.validation;
    eprintln!(
        "pin {} observed_sharpe={:#018x} deflated_sharpe={:#018x} pbo={:#018x}",
        run.digest,
        validation.observed_sharpe.to_bits(),
        validation.deflated_sharpe.to_bits(),
        validation.probability_backtest_overfit.to_bits(),
    );
    assert_eq!(run.digest, PINNED_RUN_DIGEST);
}

#[test]
fn look_ahead_fills_and_missing_outputs_are_refused() {
    let mut early = draft();
    early.fills[0].fill_at = early.fills[0].known_at - 1;
    assert_eq!(seal(&early).unwrap_err().code, LOOK_AHEAD);
    let mut outside = draft();
    outside.fills[0].fill_at = 950 * DAY;
    outside.fills[0].known_at = 950 * DAY;
    assert_eq!(seal(&outside).unwrap_err().code, LOOK_AHEAD);
    let mut no_pbo = draft();
    no_pbo.validation.performance.clear();
    assert_eq!(seal(&no_pbo).unwrap_err().code, INVALID_REQUEST);
    let mut misaligned = draft();
    misaligned.validation.performance.pop();
    assert_eq!(seal(&misaligned).unwrap_err().code, INVALID_REQUEST);
    let mut no_keys = draft();
    no_keys.signal_keys.clear();
    assert_eq!(seal(&no_keys).unwrap_err().code, INVALID_REQUEST);
}

/// EG-FINANCE-PRIMITIVES-R003: PBO comes from the record's own CPCV splits
/// over its per-period performance, not from caller-supplied summary rows.
/// This fails on the pre-fix code, which passed the raw (and here,
/// deliberately too-short) in-sample/out-of-sample arrays straight into the
/// PBO kernel instead of deriving them from `purged_cpcv_splits`.
#[test]
fn pbo_uses_all_cpcv_splits_from_the_records_performance() {
    let skilled = seal(&draft()).unwrap();
    assert_eq!(skilled.validation.cpcv_splits, 15);
    assert_eq!(skilled.validation.probability_backtest_overfit, 0.0);

    // Three variants specialise in disjoint pairs of the six groups. When
    // a variant wins in-sample, the held-out groups favour another variant.
    let mut overfit = draft();
    let peaks = [[1.0, 0.0, 0.0], [0.0, 1.0, 0.0], [0.0, 0.0, 1.0]];
    overfit.validation.performance = (0..48).map(|period| peaks[period / 16].to_vec()).collect();
    let result = seal(&overfit).unwrap();
    assert_eq!(result.validation.cpcv_splits, 15);
    assert_eq!(result.validation.probability_backtest_overfit, 1.0);
    assert_ne!(result.digest, skilled.digest);
}
