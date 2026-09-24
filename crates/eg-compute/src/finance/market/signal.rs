//! Per-bar signal advance and bitemporal replay with flip revisions (EH-415).
//!
//! A signal state is a projection of a series' final bars. [`advance`] consumes
//! new final bars after the state's last bar, one O(1) step each, and emits a
//! [`TrendFlip`] whenever the trailing line changes direction. [`replay`]
//! processes every bar version in the order it became knowable: an arrival that
//! only appends advances incrementally; a revised (or late, back-filled) bar
//! recomputes the projection and appends revision records — a changed flip is a
//! new `emitted` record that `revises` the old one, a vanished flip a `retracted`
//! record. No record is ever edited, so the output is replayable as of any time.

use std::collections::BTreeMap;

use super::digest;
use super::indicators::{indicator_version, param_hash};
use super::resolve::apply_version;
use super::supertrend::{step as trail_step, TrailParams};
use super::{
    BarRecord, BarStatus, DataStatus, FlipRecord, FlipRecordStatus, IndicatorSpec, MarketError,
    MarketResult, SeriesIdentity, SignalKey, SignalReplay, SignalReplayRequest, SignalState,
    SuperTrendCheckpoint, TrendFlip, INVALID_BAR, INVALID_REQUEST, REVISION_NEEDS_REPLAY,
};

const KEY_DOMAIN: &str = "eg/finance/signal-key/v1";
const EVENT_DOMAIN: &str = "eg/finance/flip-event/v1";
const RECORD_DOMAIN: &str = "eg/finance/flip-record/v1";
const SOURCE_DOMAIN: &str = "eg/finance/source-revision/v1";
/// Bar steps one replay may spend on recomputations after revisions: each
/// revision re-folds the whole history, so this bounds a revision storm.
pub const MAX_REPLAY_WORK: usize = 20_000_000;

/// The signal key of a trailing-trend spec over a series.
pub fn signal_key(series: &SeriesIdentity, spec: &IndicatorSpec) -> MarketResult<SignalKey> {
    TrailParams::of(spec.kind)?;
    let version = indicator_version(spec);
    let params = param_hash(spec);
    let timeframe = serde_json::to_vec(&series.timeframe).unwrap_or_default();
    let digest = digest::framed(
        KEY_DOMAIN,
        &[
            series.listing_id.as_bytes(),
            series.price_basis.as_bytes(),
            &timeframe,
            series.calendar_id.as_bytes(),
            version.as_bytes(),
            params.as_bytes(),
        ],
    );
    Ok(SignalKey {
        series: series.clone(),
        indicator_version: version,
        param_hash: params,
        digest,
    })
}

/// The flip idempotency key: the signal key and the bar close.
pub fn event_id(key_digest: &str, bar_close: i64) -> String {
    digest::framed(
        EVENT_DOMAIN,
        &[key_digest.as_bytes(), &bar_close.to_be_bytes()],
    )
}

/// The state before any bar.
pub fn initial_state(key: SignalKey, spec: IndicatorSpec) -> SignalState {
    SignalState {
        key,
        spec,
        direction: None,
        data_status: DataStatus::Unavailable,
        line: None,
        atr: None,
        last_bar_open: None,
        last_bar_close: None,
        last_close: None,
        last_flip_at: None,
        flip_reference_price: None,
        source_revision: digest::framed(SOURCE_DOMAIN, &[]),
        kernel: SuperTrendCheckpoint::default(),
    }
}

fn check_next(state: &SignalState, bar: &BarRecord) -> MarketResult<()> {
    if bar.status != BarStatus::Final {
        return Err(MarketError::new(
            INVALID_BAR,
            format!(
                "bar at {} is provisional; signals confirm on final bars",
                bar.open_time
            ),
        ));
    }
    match state.last_bar_close {
        Some(close) if bar.open_time < close => Err(MarketError::new(
            REVISION_NEEDS_REPLAY,
            format!(
                "bar at {} is not after the state's last bar; replay the series",
                bar.open_time
            ),
        )),
        _ => Ok(()),
    }
}

/// One final bar, strictly after the state's last bar.
fn advance_one(
    state: &mut SignalState,
    params: TrailParams,
    bar: &BarRecord,
) -> MarketResult<Option<TrendFlip>> {
    check_next(state, bar)?;
    let step = trail_step(&mut state.kernel, params, bar)?;
    state.last_bar_open = Some(bar.open_time);
    state.last_bar_close = Some(bar.close_time);
    state.last_close = Some(bar.close);
    state.source_revision = digest::framed(
        SOURCE_DOMAIN,
        &[
            state.source_revision.as_bytes(),
            &bar.open_time.to_be_bytes(),
            &bar.revision.to_be_bytes(),
        ],
    );
    let Some(step) = step else {
        state.data_status = DataStatus::Warming;
        return Ok(None);
    };
    state.data_status = DataStatus::Valid;
    state.direction = Some(step.direction);
    state.line = Some(step.line);
    state.atr = Some(step.atr);
    let Some(from) = step.flipped_from else {
        return Ok(None);
    };
    state.last_flip_at = Some(bar.close_time);
    state.flip_reference_price = Some(bar.close);
    Ok(Some(TrendFlip {
        event_id: event_id(&state.key.digest, bar.close_time),
        key_digest: state.key.digest.clone(),
        from,
        to: step.direction,
        bar_open: bar.open_time,
        effective_at: bar.close_time,
        observed_at: bar.known_at,
        price: bar.close,
        line: step.line,
        bar_revision: bar.revision,
    }))
}

/// Advance `state` by final bars after its last one, in order.
pub fn advance(
    state: &SignalState,
    bars: &[BarRecord],
) -> MarketResult<(SignalState, Vec<TrendFlip>)> {
    let params = TrailParams::of(state.spec.kind)?;
    let mut next = state.clone();
    let mut flips = Vec::new();
    for bar in bars {
        flips.extend(advance_one(&mut next, params, bar)?);
    }
    Ok((next, flips))
}

fn record(
    status: FlipRecordStatus,
    flip: TrendFlip,
    revises: Option<String>,
    recorded_at: i64,
) -> FlipRecord {
    let revised = revises.clone().unwrap_or_default();
    let body = serde_json::to_vec(&(status, &flip, recorded_at)).unwrap_or_default();
    FlipRecord {
        record_id: digest::framed(RECORD_DOMAIN, &[&body, revised.as_bytes()]),
        status,
        flip,
        revises,
        recorded_at,
    }
}

/// The replay's running projection.
struct Projection {
    key: SignalKey,
    spec: IndicatorSpec,
    params: TrailParams,
    versions: BTreeMap<i64, BarRecord>,
    state: SignalState,
    /// Standing (emitted, not retracted) records by event id.
    standing: BTreeMap<String, FlipRecord>,
    records: Vec<FlipRecord>,
    /// Bar steps spent on recomputations so far.
    work: usize,
}

impl Projection {
    fn new(request: &SignalReplayRequest) -> MarketResult<Self> {
        let key = signal_key(&request.series, &request.spec)?;
        Ok(Self {
            state: initial_state(key.clone(), request.spec),
            key,
            spec: request.spec,
            params: TrailParams::of(request.spec.kind)?,
            versions: BTreeMap::new(),
            standing: BTreeMap::new(),
            records: Vec::new(),
            work: 0,
        })
    }

    fn final_bars(&self) -> impl Iterator<Item = &BarRecord> {
        self.versions
            .values()
            .filter(|bar| bar.status == BarStatus::Final)
    }

    /// Apply one arrival group (one `known_at`); returns whether the final view
    /// changed at or before the state's last bar.
    fn absorb(&mut self, group: &[BarRecord]) -> MarketResult<bool> {
        let last = self.state.last_bar_open;
        let mut rewinds = false;
        for version in group {
            let changed = apply_version(&mut self.versions, version)?;
            let is_final = self.versions[&version.open_time].status == BarStatus::Final;
            rewinds |= changed && is_final && last.is_some_and(|open| version.open_time <= open);
        }
        Ok(rewinds)
    }

    fn append_new(&mut self, recorded_at: i64) -> MarketResult<()> {
        let last = self.state.last_bar_open;
        let fresh: Vec<BarRecord> = self
            .final_bars()
            .filter(|bar| last.is_none_or(|open| bar.open_time > open))
            .cloned()
            .collect();
        for bar in &fresh {
            if let Some(flip) = advance_one(&mut self.state, self.params, bar)? {
                let emitted = record(FlipRecordStatus::Emitted, flip, None, recorded_at);
                self.standing
                    .insert(emitted.flip.event_id.clone(), emitted.clone());
                self.records.push(emitted);
            }
        }
        Ok(())
    }

    fn recompute(&mut self, recorded_at: i64) -> MarketResult<()> {
        let bars: Vec<BarRecord> = self.final_bars().cloned().collect();
        self.work = self.work.saturating_add(bars.len());
        if self.work > MAX_REPLAY_WORK {
            return Err(MarketError::new(
                INVALID_REQUEST,
                format!("replay recomputation exceeds {MAX_REPLAY_WORK} bar steps; replay a shorter window"),
            ));
        }
        let (state, flips) = advance(&initial_state(self.key.clone(), self.spec), &bars)?;
        self.state = state;
        let mut fresh: BTreeMap<String, TrendFlip> = flips
            .into_iter()
            .map(|flip| (flip.event_id.clone(), flip))
            .collect();
        let prior = std::mem::take(&mut self.standing);
        for (id, old) in prior {
            match fresh.remove(&id) {
                None => self.push(
                    FlipRecordStatus::Retracted,
                    old.flip.clone(),
                    Some(old.record_id),
                    recorded_at,
                    false,
                ),
                Some(flip) if flip == old.flip => {
                    self.standing.insert(id, old);
                }
                Some(flip) => self.push(
                    FlipRecordStatus::Emitted,
                    flip,
                    Some(old.record_id),
                    recorded_at,
                    true,
                ),
            }
        }
        for flip in fresh.into_values() {
            self.push(FlipRecordStatus::Emitted, flip, None, recorded_at, true);
        }
        Ok(())
    }

    fn push(
        &mut self,
        status: FlipRecordStatus,
        flip: TrendFlip,
        revises: Option<String>,
        at: i64,
        stands: bool,
    ) {
        let entry = record(status, flip, revises, at);
        if stands {
            self.standing
                .insert(entry.flip.event_id.clone(), entry.clone());
        }
        self.records.push(entry);
    }
}

fn arrivals(request: &SignalReplayRequest) -> Vec<BarRecord> {
    let mut known: Vec<BarRecord> = request
        .records
        .iter()
        .filter(|record| request.as_of.is_none_or(|cutoff| record.known_at <= cutoff))
        .cloned()
        .collect();
    known.sort_by_key(|record| (record.known_at, record.open_time, record.revision));
    known
}

fn mark_stale(state: &mut SignalState, as_of: Option<i64>, stale_after: Option<i64>) {
    let (Some(now), Some(bound), Some(close)) = (as_of, stale_after, state.last_bar_close) else {
        return;
    };
    if state.data_status == DataStatus::Valid && now - close > bound {
        state.data_status = DataStatus::Stale;
    }
}

/// Replay every known version of one series' bars through the signal.
pub fn replay(request: &SignalReplayRequest) -> MarketResult<SignalReplay> {
    let mut projection = Projection::new(request)?;
    let known = arrivals(request);
    for group in known.chunk_by(|a, b| a.known_at == b.known_at) {
        let recorded_at = group[0].known_at;
        if projection.absorb(group)? {
            projection.recompute(recorded_at)?;
        } else {
            projection.append_new(recorded_at)?;
        }
    }
    let mut state = projection.state;
    mark_stale(&mut state, request.as_of, request.stale_after);
    let mut current: Vec<TrendFlip> = projection
        .standing
        .into_values()
        .map(|entry| entry.flip)
        .collect();
    current.sort_by_key(|flip| flip.effective_at);
    Ok(SignalReplay {
        state,
        records: projection.records,
        current,
    })
}
