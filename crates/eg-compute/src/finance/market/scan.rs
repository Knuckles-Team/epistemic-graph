//! The latest-state view a scanner reads (EH-415).
//!
//! [`SignalIndex`] keeps one state per signal key and never regresses: a state
//! replaces the held one only when it has consumed a later bar (then more
//! bars, then a greater source revision, so the choice is total and
//! deterministic). A scan filters it, counts every state it kept against a
//! stated denominator (one state per key, so nothing is double counted) and
//! returns the most recently flipped rows first.

use std::cmp::Reverse;
use std::collections::BTreeMap;

use super::fixed::basis_points;
use super::{
    DataStatus, Direction, MarketResult, ScanCounts, ScanFilter, ScanPage, ScanRequest, ScanRow,
    SignalState,
};

/// Rows returned by one scan at most.
pub const MAX_SCAN_ROWS: u32 = 10_000;

fn recency(state: &SignalState) -> (Option<i64>, u64, &str) {
    (
        state.last_bar_close,
        state.kernel.bars,
        &state.source_revision,
    )
}

/// One latest state per signal key.
#[derive(Debug, Clone, Default)]
pub struct SignalIndex {
    states: BTreeMap<String, SignalState>,
    superseded: u64,
}

impl SignalIndex {
    /// Hold `state` unless a state at least as recent is already held.
    pub fn upsert(&mut self, state: SignalState) -> bool {
        let digest = state.key.digest.clone();
        let newer = self
            .states
            .get(&digest)
            .is_none_or(|held| recency(&state) > recency(held));
        if newer {
            self.superseded += u64::from(self.states.contains_key(&digest));
            self.states.insert(digest, state);
        } else {
            self.superseded += 1;
        }
        newer
    }

    pub fn len(&self) -> usize {
        self.states.len()
    }

    pub fn is_empty(&self) -> bool {
        self.states.is_empty()
    }

    pub fn get(&self, key_digest: &str) -> Option<&SignalState> {
        self.states.get(key_digest)
    }

    /// Filter, count and page the held states.
    pub fn scan(&self, filter: &ScanFilter, limit: u32) -> ScanPage {
        let kept: Vec<&SignalState> = self
            .states
            .values()
            .filter(|state| keeps(filter, state))
            .collect();
        let mut counts = ScanCounts::default();
        for state in &kept {
            count(&mut counts, state);
        }
        let mut rows: Vec<ScanRow> = kept.into_iter().map(row).collect();
        rows.sort_by(|a, b| {
            (Reverse(a.last_flip_at), &a.key_digest).cmp(&(Reverse(b.last_flip_at), &b.key_digest))
        });
        rows.truncate(limit.min(MAX_SCAN_ROWS) as usize);
        ScanPage {
            rows,
            counts,
            superseded: self.superseded,
        }
    }
}

fn keeps(filter: &ScanFilter, state: &SignalState) -> bool {
    let direction_ok = filter
        .direction
        .is_none_or(|wanted| state.direction == Some(wanted));
    let status_ok = filter.statuses.is_empty() || filter.statuses.contains(&state.data_status);
    let flip_ok = filter
        .flipped_since
        .is_none_or(|since| state.last_flip_at.is_some_and(|at| at >= since));
    let timeframe_ok = filter
        .timeframe
        .is_none_or(|timeframe| state.key.series.timeframe == timeframe);
    direction_ok && status_ok && flip_ok && timeframe_ok
}

fn count(counts: &mut ScanCounts, state: &SignalState) {
    counts.total += 1;
    match state.data_status {
        DataStatus::Warming => counts.warming += 1,
        DataStatus::Stale => counts.stale += 1,
        DataStatus::Unavailable => counts.unavailable += 1,
        DataStatus::Valid => match state.direction {
            Some(Direction::Bullish) => counts.bullish += 1,
            Some(Direction::Bearish) => counts.bearish += 1,
            None => counts.warming += 1,
        },
    }
}

fn row(state: &SignalState) -> ScanRow {
    let change = match (state.last_close, state.flip_reference_price) {
        (Some(close), Some(reference)) if reference != 0 => {
            Some(basis_points(close - reference, reference))
        }
        _ => None,
    };
    ScanRow {
        key_digest: state.key.digest.clone(),
        listing_id: state.key.series.listing_id.clone(),
        timeframe: state.key.series.timeframe,
        direction: state.direction,
        data_status: state.data_status,
        last_flip_at: state.last_flip_at,
        flip_reference_price: state.flip_reference_price,
        last_close: state.last_close,
        change_since_flip_bps: change,
    }
}

/// The scan a `SignalScan` request asks for.
pub fn scan(request: &ScanRequest) -> MarketResult<ScanPage> {
    let mut index = SignalIndex::default();
    for state in &request.states {
        index.upsert(state.clone());
    }
    Ok(index.scan(&request.filter, request.limit))
}
