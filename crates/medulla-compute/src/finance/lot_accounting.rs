// CONCEPT:MEDULLA-KG.domains.finance-compute.lot-accounting — deterministic
// fixed-point lot/cost-basis matching (MEDULLA-FINANCE-PRIMITIVES-R005.1).
//
// Money and quantity are fixed-point integers scaled by `SCALE`, never
// `f64`: replaying the same ordered activities must reproduce byte-identical
// results, which floating-point rounding cannot guarantee across hosts.
//
// Scope: the standalone FIFO/LIFO cost-basis matching engine only, over its
// own minimal `LotActivity`/`Lot` types -- not finance-v1's wire
// `Account`/`Activity`/`Lot`/`Position` records (MEDULLA-FINANCE-PRIMITIVES-R004.2,
// still landing). Wiring to those wire types, plus specific-lot/average-cost
// elections and P&L/TWR/XIRR, are later MEDULLA-FINANCE-PRIMITIVES-R005 children.

use std::fmt;

/// Fixed-point scale: one whole unit of quantity or price is `SCALE` ticks.
pub const SCALE: i64 = 100_000_000;

/// One signed, ordered ledger event against a position: a buy (positive
/// `quantity_ticks`) or a sell (negative).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LotActivity {
    /// The canonical order; must strictly increase across a replayed slice.
    pub sequence: u64,
    /// Signed quantity in fixed-point ticks: positive buy, negative sell.
    pub quantity_ticks: i64,
    /// Unit price in fixed-point ticks (always positive).
    pub price_ticks: i64,
}

/// One open or partially-closed cost-basis parcel.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Lot {
    /// `sequence` of the activity that opened this lot.
    pub opened_by: u64,
    /// Remaining open quantity in ticks (always positive while retained).
    pub remaining_ticks: i64,
    /// Unit cost basis in ticks, fixed at open.
    pub unit_cost_ticks: i64,
}

/// How open lots are selected to close against a sell.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LotMethod {
    Fifo,
    Lifo,
    /// Specific-lot election (MEDULLA-FINANCE-PRIMITIVES-R005.2.1): close exactly
    /// the open lot whose `opened_by` equals this sequence, never a
    /// FIFO/LIFO-chosen one.
    Specific(u64),
}

/// One closing of all or part of one lot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClosedLot {
    pub opened_by: u64,
    pub closing_sequence: u64,
    pub closed_ticks: i64,
    pub unit_cost_ticks: i64,
    pub unit_proceeds_ticks: i64,
    pub realized_gain_ticks: i64,
}

/// The result of replaying an ordered activity slice: every open lot
/// remaining, and every closing it produced along the way.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LotLedger {
    pub open: Vec<Lot>,
    pub closed: Vec<ClosedLot>,
}

/// A refused activity. `activities` up to and including the refusing one are
/// never reflected in any returned ledger, because none is returned at all --
/// `apply_activities` is a pure function with no observable partial state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LotError {
    NonPositiveQuantity {
        sequence: u64,
    },
    NonPositivePrice {
        sequence: u64,
    },
    OutOfOrderSequence {
        sequence: u64,
        last_sequence: u64,
    },
    Oversold {
        sequence: u64,
        requested_ticks: i64,
        available_ticks: i64,
    },
    /// A specific-lot election (MEDULLA-FINANCE-PRIMITIVES-R005.2.1) named a
    /// `requested_lot` sequence with no corresponding open lot.
    SpecificLotNotFound {
        sequence: u64,
        requested_lot: u64,
    },
    /// A specific-lot election named an open lot that does not hold enough
    /// remaining quantity to cover the sell, even though other open lots do.
    SpecificLotInsufficient {
        sequence: u64,
        requested_lot: u64,
        requested_ticks: i64,
        available_ticks: i64,
    },
    /// Checked fixed-point arithmetic overflowed at `sequence`.
    ArithmeticOverflow {
        sequence: u64,
    },
    /// A transfer's cash sent does not equal cash received plus its fee.
    UnbalancedTransfer {
        sequence: u64,
        cash_sent_ticks: i64,
        cash_received_ticks: i64,
        fee_ticks: i64,
    },
    /// A split ratio had a zero numerator or denominator.
    InvalidSplitRatio {
        numerator: u64,
        denominator: u64,
    },
    /// A split rounded a lot's quantity down to zero ticks.
    SplitRoundsToZero {
        opened_by: u64,
    },
}

impl fmt::Display for LotError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LotError::NonPositiveQuantity { sequence } => {
                write!(f, "activity {sequence} has a zero quantity")
            }
            LotError::NonPositivePrice { sequence } => {
                write!(f, "activity {sequence} has a non-positive price")
            }
            LotError::OutOfOrderSequence { sequence, last_sequence } => write!(
                f,
                "activity {sequence} is not after the last applied sequence {last_sequence}"
            ),
            LotError::Oversold { sequence, requested_ticks, available_ticks } => write!(
                f,
                "activity {sequence} sells {requested_ticks} ticks but only {available_ticks} are open"
            ),
            LotError::SpecificLotNotFound { sequence, requested_lot } => write!(
                f,
                "activity {sequence} names lot {requested_lot} but no open lot was opened by that sequence"
            ),
            LotError::SpecificLotInsufficient {
                sequence,
                requested_lot,
                requested_ticks,
                available_ticks,
            } => write!(
                f,
                "activity {sequence} sells {requested_ticks} ticks from lot {requested_lot} but only {available_ticks} remain in it"
            ),
            LotError::ArithmeticOverflow { sequence } => {
                write!(f, "activity {sequence} overflowed fixed-point arithmetic")
            }
            LotError::UnbalancedTransfer {
                sequence,
                cash_sent_ticks,
                cash_received_ticks,
                fee_ticks,
            } => write!(
                f,
                "transfer {sequence} sends {cash_sent_ticks} cash ticks but receives {cash_received_ticks} plus a {fee_ticks} fee"
            ),
            LotError::InvalidSplitRatio { numerator, denominator } => {
                write!(f, "split ratio {numerator}:{denominator} is invalid")
            }
            LotError::SplitRoundsToZero { opened_by } => {
                write!(f, "split rounds lot {opened_by} to zero quantity")
            }
        }
    }
}

impl std::error::Error for LotError {}

/// Apply an ordered sequence of buy/sell activities and return the resulting
/// open lots and every lot-closing this produced, under one election rule.
/// The same input slice always produces the same output (Sec MEDULLA-FINANCE-
/// PRIMITIVES-R005's determinism requirement): nothing here reads a clock,
/// randomness, or any float.
pub fn apply_activities(
    activities: &[LotActivity],
    method: LotMethod,
) -> Result<LotLedger, LotError> {
    let mut open: Vec<Lot> = Vec::new();
    let mut closed: Vec<ClosedLot> = Vec::new();
    let mut last_sequence: Option<u64> = None;

    for activity in activities {
        validate_activity(activity, last_sequence)?;
        last_sequence = Some(activity.sequence);

        if activity.quantity_ticks > 0 {
            open.push(Lot {
                opened_by: activity.sequence,
                remaining_ticks: activity.quantity_ticks,
                unit_cost_ticks: activity.price_ticks,
            });
        } else {
            close_lots(activity, method, &mut open, &mut closed)?;
        }
    }

    Ok(LotLedger { open, closed })
}

/// Reject out-of-order, non-positive-price and zero-quantity activities.
fn validate_activity(activity: &LotActivity, last_sequence: Option<u64>) -> Result<(), LotError> {
    if let Some(last) = last_sequence {
        if activity.sequence <= last {
            return Err(LotError::OutOfOrderSequence {
                sequence: activity.sequence,
                last_sequence: last,
            });
        }
    }
    if activity.price_ticks <= 0 {
        return Err(LotError::NonPositivePrice {
            sequence: activity.sequence,
        });
    }
    if activity.quantity_ticks == 0 {
        return Err(LotError::NonPositiveQuantity {
            sequence: activity.sequence,
        });
    }
    Ok(())
}

/// The order in which open lots are consumed by a sale under `method`.
fn closing_order(
    method: LotMethod,
    open: &[Lot],
    activity: &LotActivity,
    to_close: i64,
) -> Result<Vec<usize>, LotError> {
    match method {
        LotMethod::Fifo => Ok((0..open.len()).collect()),
        LotMethod::Lifo => Ok((0..open.len()).rev().collect()),
        LotMethod::Specific(requested_lot) => {
            let idx = open
                .iter()
                .position(|lot| lot.opened_by == requested_lot)
                .ok_or(LotError::SpecificLotNotFound {
                    sequence: activity.sequence,
                    requested_lot,
                })?;
            if open[idx].remaining_ticks < to_close {
                return Err(LotError::SpecificLotInsufficient {
                    sequence: activity.sequence,
                    requested_lot,
                    requested_ticks: to_close,
                    available_ticks: open[idx].remaining_ticks,
                });
            }
            Ok(vec![idx])
        }
    }
}

/// Apply one sale to the open lots, recording each closing.
fn close_lots(
    activity: &LotActivity,
    method: LotMethod,
    open: &mut Vec<Lot>,
    closed: &mut Vec<ClosedLot>,
) -> Result<(), LotError> {
    let mut to_close = -activity.quantity_ticks;
    let available: i64 = open.iter().map(|lot| lot.remaining_ticks).sum();
    if to_close > available {
        return Err(LotError::Oversold {
            sequence: activity.sequence,
            requested_ticks: to_close,
            available_ticks: available,
        });
    }

    for idx in closing_order(method, open, activity, to_close)? {
        if to_close == 0 {
            break;
        }
        let take = to_close.min(open[idx].remaining_ticks);
        if take == 0 {
            continue;
        }
        open[idx].remaining_ticks -= take;
        to_close -= take;
        let gain = (take as i128 * (activity.price_ticks - open[idx].unit_cost_ticks) as i128
            / SCALE as i128) as i64;
        closed.push(ClosedLot {
            opened_by: open[idx].opened_by,
            closing_sequence: activity.sequence,
            closed_ticks: take,
            unit_cost_ticks: open[idx].unit_cost_ticks,
            unit_proceeds_ticks: activity.price_ticks,
            realized_gain_ticks: gain,
        });
    }
    open.retain(|lot| lot.remaining_ticks > 0);
    Ok(())
}

/// Declared rounding for every fixed-point division in the elections below:
/// round half up (ties away from zero on the positive magnitudes divided).
/// No floating point is involved.
fn div_round_half_up(numerator: i128, denominator: i128) -> Option<i128> {
    if denominator <= 0 || numerator < 0 {
        return None;
    }
    numerator
        .checked_mul(2)?
        .checked_add(denominator)?
        .checked_div(denominator.checked_mul(2)?)
}

fn to_ticks(value: i128, sequence: u64) -> Result<i64, LotError> {
    i64::try_from(value).map_err(|_| LotError::ArithmeticOverflow { sequence })
}

/// `quantity_ticks * unit_ticks / SCALE`, rounded half up, in money ticks.
fn extended_ticks(quantity_ticks: i64, unit_ticks: i64, sequence: u64) -> Result<i64, LotError> {
    let product = (quantity_ticks as i128)
        .checked_mul(unit_ticks as i128)
        .ok_or(LotError::ArithmeticOverflow { sequence })?;
    let rounded = div_round_half_up(product, SCALE as i128)
        .ok_or(LotError::ArithmeticOverflow { sequence })?;
    to_ticks(rounded, sequence)
}

/// One closing under the average-cost election.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AverageCostClosing {
    pub closing_sequence: u64,
    pub closed_ticks: i64,
    pub basis_removed_ticks: i64,
    pub proceeds_ticks: i64,
    pub realized_gain_ticks: i64,
}

/// The pooled position after replaying activities under the average-cost
/// election (MEDULLA-FINANCE-PRIMITIVES-R005.2.2).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AverageCostLedger {
    pub quantity_ticks: i64,
    /// Total remaining cost basis in money ticks.
    pub basis_ticks: i64,
    pub closed: Vec<AverageCostClosing>,
}

/// Replay buys and sells under the explicit average-cost election
/// (MEDULLA-FINANCE-PRIMITIVES-R005.2.2). Basis is pooled; a sell removes
/// `basis * sold / quantity` rounded half up, and a sell of the whole pool
/// removes the exact remaining basis so no residue is left behind. Checked
/// arithmetic only; oversell and overflow refuse with no partial ledger.
pub fn apply_activities_average_cost(
    activities: &[LotActivity],
) -> Result<AverageCostLedger, LotError> {
    let mut ledger = AverageCostLedger::default();
    let mut last_sequence: Option<u64> = None;
    for activity in activities {
        validate_activity(activity, last_sequence)?;
        last_sequence = Some(activity.sequence);
        let sequence = activity.sequence;
        if activity.quantity_ticks > 0 {
            let cost = extended_ticks(activity.quantity_ticks, activity.price_ticks, sequence)?;
            ledger.quantity_ticks = ledger
                .quantity_ticks
                .checked_add(activity.quantity_ticks)
                .ok_or(LotError::ArithmeticOverflow { sequence })?;
            ledger.basis_ticks = ledger
                .basis_ticks
                .checked_add(cost)
                .ok_or(LotError::ArithmeticOverflow { sequence })?;
            continue;
        }
        let sold = activity
            .quantity_ticks
            .checked_neg()
            .ok_or(LotError::ArithmeticOverflow { sequence })?;
        if sold > ledger.quantity_ticks {
            return Err(LotError::Oversold {
                sequence,
                requested_ticks: sold,
                available_ticks: ledger.quantity_ticks,
            });
        }
        let removed = if sold == ledger.quantity_ticks {
            ledger.basis_ticks
        } else {
            let scaled = (ledger.basis_ticks as i128)
                .checked_mul(sold as i128)
                .ok_or(LotError::ArithmeticOverflow { sequence })?;
            let rounded = div_round_half_up(scaled, ledger.quantity_ticks as i128)
                .ok_or(LotError::ArithmeticOverflow { sequence })?;
            to_ticks(rounded, sequence)?
        };
        let proceeds = extended_ticks(sold, activity.price_ticks, sequence)?;
        let gain = proceeds
            .checked_sub(removed)
            .ok_or(LotError::ArithmeticOverflow { sequence })?;
        ledger.quantity_ticks -= sold;
        ledger.basis_ticks -= removed;
        ledger.closed.push(AverageCostClosing {
            closing_sequence: sequence,
            closed_ticks: sold,
            basis_removed_ticks: removed,
            proceeds_ticks: proceeds,
            realized_gain_ticks: gain,
        });
    }
    Ok(ledger)
}

/// Exact aggregate cost basis of `lots` as `sum(remaining * unit_cost)` in
/// tick-squared units. Lot movement and splitting never change it, so it is
/// the quantity the conservation checks compare.
pub fn basis_numerator(lots: &[Lot]) -> i128 {
    lots.iter()
        .map(|lot| lot.remaining_ticks as i128 * lot.unit_cost_ticks as i128)
        .sum()
}

/// Total open quantity of `lots` in ticks.
pub fn total_quantity_ticks(lots: &[Lot]) -> i128 {
    lots.iter().map(|lot| lot.remaining_ticks as i128).sum()
}

/// One ordered, paired transfer of `quantity_ticks` between two accounts.
/// Cash sent must equal cash received plus the explicit fee.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LotTransfer {
    pub sequence: u64,
    pub quantity_ticks: i64,
    pub cash_sent_ticks: i64,
    pub cash_received_ticks: i64,
    pub fee_ticks: i64,
}

/// Both accounts' open lots after a transfer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransferOutcome {
    pub source: Vec<Lot>,
    pub destination: Vec<Lot>,
}

/// Move `transfer.quantity_ticks` of open lots from `source` to `destination`
/// (MEDULLA-FINANCE-PRIMITIVES-R005.2.3), choosing source lots by `method`. Moved
/// lots keep their `opened_by` identity and unit cost basis; a partial lot is
/// split into a retained and a moved part. Quantity, basis and cash are
/// conserved (cash net of the fee). Inputs are borrowed and untouched, so a
/// refusal leaves no partial ledger update.
pub fn transfer_lots(
    source: &[Lot],
    destination: &[Lot],
    transfer: &LotTransfer,
    method: LotMethod,
) -> Result<TransferOutcome, LotError> {
    let sequence = transfer.sequence;
    if transfer.quantity_ticks <= 0 {
        return Err(LotError::NonPositiveQuantity { sequence });
    }
    let received_plus_fee = transfer
        .cash_received_ticks
        .checked_add(transfer.fee_ticks)
        .ok_or(LotError::ArithmeticOverflow { sequence })?;
    if transfer.fee_ticks < 0
        || transfer.cash_received_ticks < 0
        || received_plus_fee != transfer.cash_sent_ticks
    {
        return Err(LotError::UnbalancedTransfer {
            sequence,
            cash_sent_ticks: transfer.cash_sent_ticks,
            cash_received_ticks: transfer.cash_received_ticks,
            fee_ticks: transfer.fee_ticks,
        });
    }
    let available: i64 = source.iter().map(|lot| lot.remaining_ticks).sum();
    if transfer.quantity_ticks > available {
        return Err(LotError::Oversold {
            sequence,
            requested_ticks: transfer.quantity_ticks,
            available_ticks: available,
        });
    }
    let probe = LotActivity {
        sequence,
        quantity_ticks: -transfer.quantity_ticks,
        price_ticks: SCALE,
    };
    let order = closing_order(method, source, &probe, transfer.quantity_ticks)?;

    let mut remaining_source = source.to_vec();
    let mut moved: Vec<Lot> = Vec::new();
    let mut to_move = transfer.quantity_ticks;
    for idx in order {
        if to_move == 0 {
            break;
        }
        let take = to_move.min(remaining_source[idx].remaining_ticks);
        if take == 0 {
            continue;
        }
        remaining_source[idx].remaining_ticks -= take;
        to_move -= take;
        moved.push(Lot {
            opened_by: remaining_source[idx].opened_by,
            remaining_ticks: take,
            unit_cost_ticks: remaining_source[idx].unit_cost_ticks,
        });
    }
    remaining_source.retain(|lot| lot.remaining_ticks > 0);

    let mut merged = destination.to_vec();
    for lot in moved {
        match merged.iter_mut().find(|held| {
            held.opened_by == lot.opened_by && held.unit_cost_ticks == lot.unit_cost_ticks
        }) {
            Some(held) => {
                held.remaining_ticks = held
                    .remaining_ticks
                    .checked_add(lot.remaining_ticks)
                    .ok_or(LotError::ArithmeticOverflow { sequence })?;
            }
            None => merged.push(lot),
        }
    }
    Ok(TransferOutcome {
        source: remaining_source,
        destination: merged,
    })
}

/// Open lots after a split, plus the exact aggregate-basis rounding residual.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SplitOutcome {
    pub lots: Vec<Lot>,
    /// `basis_numerator(after) - basis_numerator(before)` in tick-squared
    /// units. Zero when the ratio divides every lot exactly; otherwise bounded
    /// by half a tick of unit cost times each lot's new quantity.
    pub basis_residual: i128,
}

/// Adjust every open lot for a `numerator:denominator` split
/// (MEDULLA-FINANCE-PRIMITIVES-R005.2.4): a 2:1 forward split is `(2, 1)`, a 1:2
/// reverse split is `(1, 2)`. New quantity is `remaining * num / den` rounded
/// half up; the new unit cost is recomputed from the lot's pre-split basis,
/// rounded half up, so aggregate basis is preserved to within the reported
/// residual. A zero ratio, overflow, or a lot rounding to zero refuses.
pub fn apply_split(
    lots: &[Lot],
    numerator: u64,
    denominator: u64,
) -> Result<SplitOutcome, LotError> {
    if numerator == 0 || denominator == 0 {
        return Err(LotError::InvalidSplitRatio {
            numerator,
            denominator,
        });
    }
    let mut adjusted = Vec::with_capacity(lots.len());
    for lot in lots {
        let sequence = lot.opened_by;
        let scaled = (lot.remaining_ticks as i128)
            .checked_mul(numerator as i128)
            .ok_or(LotError::ArithmeticOverflow { sequence })?;
        let new_ticks = to_ticks(
            div_round_half_up(scaled, denominator as i128)
                .ok_or(LotError::ArithmeticOverflow { sequence })?,
            sequence,
        )?;
        if new_ticks == 0 {
            return Err(LotError::SplitRoundsToZero {
                opened_by: lot.opened_by,
            });
        }
        let basis = (lot.remaining_ticks as i128)
            .checked_mul(lot.unit_cost_ticks as i128)
            .ok_or(LotError::ArithmeticOverflow { sequence })?;
        let new_cost = to_ticks(
            div_round_half_up(basis, new_ticks as i128)
                .ok_or(LotError::ArithmeticOverflow { sequence })?,
            sequence,
        )?;
        adjusted.push(Lot {
            opened_by: lot.opened_by,
            remaining_ticks: new_ticks,
            unit_cost_ticks: new_cost,
        });
    }
    let basis_residual = basis_numerator(&adjusted) - basis_numerator(lots);
    Ok(SplitOutcome {
        lots: adjusted,
        basis_residual,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn act(sequence: u64, quantity_units: i64, price_units: i64) -> LotActivity {
        LotActivity {
            sequence,
            quantity_ticks: quantity_units * SCALE,
            price_ticks: price_units * SCALE,
        }
    }

    // spec: MEDULLA-FINANCE-PRIMITIVES-R005.1
    // spec: MEDULLA-FINANCE-PRIMITIVES-R005.1.1
    #[test]
    fn fifo_closes_the_oldest_lot_first() {
        // act(3, ...) is the sell that FIFO must close against the two buys
        // above it; `quantity_ticks` is signed (positive buy, negative sell,
        // per the struct doc at the top of this file), so the sell's units
        // must be negative or `apply_activities` treats it as a third buy
        // and never closes anything.
        let activities = [act(1, 10, 100), act(2, 5, 110), act(3, -12, 120)];
        let ledger = apply_activities(&activities, LotMethod::Fifo).unwrap();
        assert_eq!(ledger.closed.len(), 2);
        assert_eq!(ledger.closed[0].opened_by, 1);
        assert_eq!(ledger.closed[0].closed_ticks, 10 * SCALE);
        assert_eq!(ledger.closed[1].opened_by, 2);
        assert_eq!(ledger.closed[1].closed_ticks, 2 * SCALE);
        assert_eq!(ledger.open.len(), 1);
        assert_eq!(ledger.open[0].opened_by, 2);
        assert_eq!(ledger.open[0].remaining_ticks, 3 * SCALE);
    }

    // spec: MEDULLA-FINANCE-PRIMITIVES-R005.1
    // spec: MEDULLA-FINANCE-PRIMITIVES-R005.1.1
    #[test]
    fn lifo_closes_the_newest_lot_first() {
        // Same sign fix as fifo_closes_the_oldest_lot_first above: act(3,
        // ...) must be a sell (negative quantity_ticks) for LIFO to have
        // anything to close.
        let activities = [act(1, 10, 100), act(2, 5, 110), act(3, -12, 120)];
        let ledger = apply_activities(&activities, LotMethod::Lifo).unwrap();
        assert_eq!(ledger.closed.len(), 2);
        assert_eq!(ledger.closed[0].opened_by, 2);
        assert_eq!(ledger.closed[0].closed_ticks, 5 * SCALE);
        assert_eq!(ledger.closed[1].opened_by, 1);
        assert_eq!(ledger.closed[1].closed_ticks, 7 * SCALE);
        assert_eq!(ledger.open.len(), 1);
        assert_eq!(ledger.open[0].opened_by, 1);
        assert_eq!(ledger.open[0].remaining_ticks, 3 * SCALE);
    }

    // spec: MEDULLA-FINANCE-PRIMITIVES-R005.1
    #[test]
    fn realized_gain_is_proceeds_minus_cost_times_quantity() {
        // act(2, ...) must be the sell that realizes the gain against the
        // lot opened by act(1, ...); same sign fix as above.
        let activities = [act(1, 10, 100), act(2, -10, 150)];
        let ledger = apply_activities(&activities, LotMethod::Fifo).unwrap();
        assert_eq!(ledger.closed[0].realized_gain_ticks, 10 * 50 * SCALE);
    }

    #[test]
    fn oversold_quantity_is_refused() {
        // act(2, ...) must be a sell (negative quantity_ticks) to be
        // refused as oversold; as a positive quantity it is just a second
        // buy, which `apply_activities` accepts, so the ledger ends with 2
        // open lots and an empty `closed` instead of an `Oversold` error.
        let activities = [act(1, 10, 100), act(2, -11, 100)];
        let error = apply_activities(&activities, LotMethod::Fifo).unwrap_err();
        assert_eq!(
            error,
            LotError::Oversold {
                sequence: 2,
                requested_ticks: 11 * SCALE,
                available_ticks: 10 * SCALE,
            }
        );
    }

    #[test]
    fn out_of_order_sequence_is_refused() {
        let activities = [act(2, 10, 100), act(1, 5, 100)];
        let error = apply_activities(&activities, LotMethod::Fifo).unwrap_err();
        assert_eq!(
            error,
            LotError::OutOfOrderSequence {
                sequence: 1,
                last_sequence: 2
            }
        );
    }

    #[test]
    fn non_positive_price_and_quantity_are_refused() {
        let zero_qty = [LotActivity {
            sequence: 1,
            quantity_ticks: 0,
            price_ticks: SCALE,
        }];
        assert_eq!(
            apply_activities(&zero_qty, LotMethod::Fifo).unwrap_err(),
            LotError::NonPositiveQuantity { sequence: 1 }
        );
        let zero_price = [act(1, 10, 0)];
        assert_eq!(
            apply_activities(&zero_price, LotMethod::Fifo).unwrap_err(),
            LotError::NonPositivePrice { sequence: 1 }
        );
    }

    // spec: MEDULLA-FINANCE-PRIMITIVES-R005.2.1
    #[test]
    fn specific_lot_closes_the_named_lot_not_the_fifo_choice() {
        let activities = [
            act(1, 10, 100),
            act(2, 5, 110),
            LotActivity {
                sequence: 3,
                quantity_ticks: -3 * SCALE,
                price_ticks: 120 * SCALE,
            },
        ];
        // FIFO would close lot 1 first; naming lot 2 must close lot 2 instead.
        let ledger = apply_activities(&activities, LotMethod::Specific(2)).unwrap();
        assert_eq!(ledger.closed.len(), 1);
        assert_eq!(ledger.closed[0].opened_by, 2);
        assert_eq!(ledger.closed[0].closed_ticks, 3 * SCALE);
        assert_eq!(ledger.open.len(), 2);
        assert!(ledger
            .open
            .iter()
            .any(|lot| lot.opened_by == 1 && lot.remaining_ticks == 10 * SCALE));
        assert!(ledger
            .open
            .iter()
            .any(|lot| lot.opened_by == 2 && lot.remaining_ticks == 2 * SCALE));
    }

    // spec: MEDULLA-FINANCE-PRIMITIVES-R005.2.1
    #[test]
    fn specific_lot_naming_an_absent_lot_is_refused() {
        let activities = [
            act(1, 10, 100),
            LotActivity {
                sequence: 2,
                quantity_ticks: -SCALE,
                price_ticks: 120 * SCALE,
            },
        ];
        let error = apply_activities(&activities, LotMethod::Specific(99)).unwrap_err();
        assert_eq!(
            error,
            LotError::SpecificLotNotFound {
                sequence: 2,
                requested_lot: 99
            }
        );
    }

    // spec: MEDULLA-FINANCE-PRIMITIVES-R005.2.1
    #[test]
    fn specific_lot_without_enough_quantity_is_refused_even_if_other_lots_have_it() {
        let activities = [
            act(1, 10, 100),
            act(2, 5, 110),
            LotActivity {
                sequence: 3,
                quantity_ticks: -6 * SCALE,
                price_ticks: 120 * SCALE,
            },
        ];
        let error = apply_activities(&activities, LotMethod::Specific(2)).unwrap_err();
        assert_eq!(
            error,
            LotError::SpecificLotInsufficient {
                sequence: 3,
                requested_lot: 2,
                requested_ticks: 6 * SCALE,
                available_ticks: 5 * SCALE,
            }
        );
    }

    #[test]
    fn replaying_the_same_ordered_activities_is_byte_identical() {
        let activities = [act(1, 10, 100), act(2, 5, 110), act(3, 12, 120)];
        let first = apply_activities(&activities, LotMethod::Fifo).unwrap();
        let second = apply_activities(&activities, LotMethod::Fifo).unwrap();
        assert_eq!(first, second);
    }

    // spec: MEDULLA-FINANCE-PRIMITIVES-R005.2.2
    #[test]
    fn average_cost_election_preserves_basis() {
        // Buys: 10 @ 100 then 30 @ 120 -> pool 40 units, basis 4600, avg 115.
        let activities = [
            act(1, 10, 100),
            act(2, 30, 120),
            LotActivity {
                sequence: 3,
                quantity_ticks: -10 * SCALE,
                price_ticks: 130 * SCALE,
            },
            LotActivity {
                sequence: 4,
                quantity_ticks: -30 * SCALE,
                price_ticks: 110 * SCALE,
            },
        ];
        let ledger = apply_activities_average_cost(&activities).unwrap();
        // Sell 10 of 40: remove 1150 basis; proceeds 1300; gain 150.
        assert_eq!(ledger.closed[0].basis_removed_ticks, 1_150 * SCALE);
        assert_eq!(ledger.closed[0].proceeds_ticks, 1_300 * SCALE);
        assert_eq!(ledger.closed[0].realized_gain_ticks, 150 * SCALE);
        // Final sell empties the pool: it removes exactly the remaining 3450.
        assert_eq!(ledger.closed[1].basis_removed_ticks, 3_450 * SCALE);
        assert_eq!(
            ledger.closed[1].realized_gain_ticks,
            (3_300 - 3_450) * SCALE
        );
        assert_eq!(ledger.quantity_ticks, 0);
        assert_eq!(ledger.basis_ticks, 0);
        // Replay is byte-identical.
        assert_eq!(ledger, apply_activities_average_cost(&activities).unwrap());
    }

    // spec: MEDULLA-FINANCE-PRIMITIVES-R005.2.2
    #[test]
    fn average_cost_rounds_half_up_at_the_declared_scale_and_leaves_no_residue() {
        // 3 units for 1 tick of price each: basis 3 ticks per unit pool is
        // exact; sell one third repeatedly to force a rounded basis share.
        let activities = [
            LotActivity {
                sequence: 1,
                quantity_ticks: 3 * SCALE,
                price_ticks: 1,
            },
            LotActivity {
                sequence: 2,
                quantity_ticks: -SCALE,
                price_ticks: 1,
            },
            LotActivity {
                sequence: 3,
                quantity_ticks: -2 * SCALE,
                price_ticks: 1,
            },
        ];
        let ledger = apply_activities_average_cost(&activities).unwrap();
        // Basis 3 ticks: a third is exactly 1, the remaining pool 2 is sold whole.
        assert_eq!(ledger.closed[0].basis_removed_ticks, 1);
        assert_eq!(ledger.closed[1].basis_removed_ticks, 2);
        assert_eq!(ledger.basis_ticks, 0);
        // Basis 5 ticks over 3 units: one unit is 5/3 = 1.67, rounded half up to 2.
        let up = apply_activities_average_cost(&[
            LotActivity {
                sequence: 1,
                quantity_ticks: 2 * SCALE,
                price_ticks: 1,
            },
            LotActivity {
                sequence: 2,
                quantity_ticks: SCALE,
                price_ticks: 3,
            },
            LotActivity {
                sequence: 3,
                quantity_ticks: -SCALE,
                price_ticks: 1,
            },
        ])
        .unwrap();
        assert_eq!(up.closed[0].basis_removed_ticks, 2);
        assert_eq!(up.basis_ticks, 3);
        // Basis 4 ticks over 3 units: 4/3 = 1.33, rounded to 1.
        let down = apply_activities_average_cost(&[
            LotActivity {
                sequence: 1,
                quantity_ticks: 2 * SCALE,
                price_ticks: 1,
            },
            LotActivity {
                sequence: 2,
                quantity_ticks: SCALE,
                price_ticks: 2,
            },
            LotActivity {
                sequence: 3,
                quantity_ticks: -SCALE,
                price_ticks: 1,
            },
        ])
        .unwrap();
        assert_eq!(down.closed[0].basis_removed_ticks, 1);
        assert_eq!(down.basis_ticks, 3);
    }

    // spec: MEDULLA-FINANCE-PRIMITIVES-R005.2.2
    #[test]
    fn average_cost_refuses_oversell_and_overflow() {
        let oversell = [act(1, 10, 100), act(2, -11, 100)];
        assert_eq!(
            apply_activities_average_cost(&oversell).unwrap_err(),
            LotError::Oversold {
                sequence: 2,
                requested_ticks: 11 * SCALE,
                available_ticks: 10 * SCALE,
            }
        );
        let overflow = [LotActivity {
            sequence: 1,
            quantity_ticks: i64::MAX,
            price_ticks: i64::MAX,
        }];
        assert_eq!(
            apply_activities_average_cost(&overflow).unwrap_err(),
            LotError::ArithmeticOverflow { sequence: 1 }
        );
    }

    fn open_lots() -> Vec<Lot> {
        apply_activities(&[act(1, 10, 100), act(2, 5, 110)], LotMethod::Fifo)
            .unwrap()
            .open
    }

    // spec: MEDULLA-FINANCE-PRIMITIVES-R005.2.3
    #[test]
    fn paired_transfer_conserves_quantity_cash_and_basis() {
        let source = open_lots();
        let destination = vec![Lot {
            opened_by: 9,
            remaining_ticks: 2 * SCALE,
            unit_cost_ticks: 90 * SCALE,
        }];
        let transfer = LotTransfer {
            sequence: 10,
            quantity_ticks: 12 * SCALE,
            cash_sent_ticks: 5 * SCALE,
            cash_received_ticks: 4 * SCALE,
            fee_ticks: SCALE,
        };
        let outcome = transfer_lots(&source, &destination, &transfer, LotMethod::Fifo).unwrap();
        assert_eq!(
            total_quantity_ticks(&source) + total_quantity_ticks(&destination),
            total_quantity_ticks(&outcome.source) + total_quantity_ticks(&outcome.destination)
        );
        assert_eq!(
            basis_numerator(&source) + basis_numerator(&destination),
            basis_numerator(&outcome.source) + basis_numerator(&outcome.destination)
        );
        // Lot 1 moves whole, lot 2 splits; identity and unit basis are kept.
        assert_eq!(outcome.source.len(), 1);
        assert_eq!(outcome.source[0].opened_by, 2);
        assert_eq!(outcome.source[0].remaining_ticks, 3 * SCALE);
        assert_eq!(outcome.source[0].unit_cost_ticks, 110 * SCALE);
        assert!(outcome
            .destination
            .iter()
            .any(|lot| lot.opened_by == 1 && lot.unit_cost_ticks == 100 * SCALE));
        assert!(outcome
            .destination
            .iter()
            .any(|lot| lot.opened_by == 2 && lot.remaining_ticks == 2 * SCALE));
        assert_eq!(
            outcome,
            transfer_lots(&source, &destination, &transfer, LotMethod::Fifo).unwrap()
        );
    }

    // spec: MEDULLA-FINANCE-PRIMITIVES-R005.2.3
    #[test]
    fn unbalanced_or_oversized_transfers_refuse_without_a_partial_update() {
        let source = open_lots();
        let snapshot = source.clone();
        let unbalanced = LotTransfer {
            sequence: 10,
            quantity_ticks: SCALE,
            cash_sent_ticks: 5 * SCALE,
            cash_received_ticks: 4 * SCALE,
            fee_ticks: 2 * SCALE,
        };
        assert_eq!(
            transfer_lots(&source, &[], &unbalanced, LotMethod::Fifo).unwrap_err(),
            LotError::UnbalancedTransfer {
                sequence: 10,
                cash_sent_ticks: 5 * SCALE,
                cash_received_ticks: 4 * SCALE,
                fee_ticks: 2 * SCALE,
            }
        );
        let oversized = LotTransfer {
            sequence: 11,
            quantity_ticks: 16 * SCALE,
            cash_sent_ticks: 0,
            cash_received_ticks: 0,
            fee_ticks: 0,
        };
        assert_eq!(
            transfer_lots(&source, &[], &oversized, LotMethod::Fifo).unwrap_err(),
            LotError::Oversold {
                sequence: 11,
                requested_ticks: 16 * SCALE,
                available_ticks: 15 * SCALE,
            }
        );
        assert_eq!(source, snapshot);
    }

    // spec: MEDULLA-FINANCE-PRIMITIVES-R005.2.4
    #[test]
    fn split_preserves_aggregate_basis() {
        let lots = open_lots();
        // 2:1 forward and 1:2 reverse divide these lots exactly.
        for (num, den) in [(2_u64, 1_u64), (1, 5)] {
            let outcome = apply_split(&lots, num, den).unwrap();
            assert_eq!(outcome.basis_residual, 0, "{num}:{den}");
            assert_eq!(basis_numerator(&outcome.lots), basis_numerator(&lots));
        }
        let forward = apply_split(&lots, 2, 1).unwrap();
        assert_eq!(forward.lots[0].remaining_ticks, 20 * SCALE);
        assert_eq!(forward.lots[0].unit_cost_ticks, 50 * SCALE);
        let reverse = apply_split(&lots, 1, 5).unwrap();
        assert_eq!(reverse.lots[0].remaining_ticks, 2 * SCALE);
        assert_eq!(reverse.lots[0].unit_cost_ticks, 500 * SCALE);
        assert_eq!(forward, apply_split(&lots, 2, 1).unwrap());
    }

    // spec: MEDULLA-FINANCE-PRIMITIVES-R005.2.4
    #[test]
    fn fractional_split_follows_the_declared_rounding_and_bounds_the_residual() {
        let lots = open_lots();
        let outcome = apply_split(&lots, 1, 3).unwrap();
        // 10 units / 3 = 3.33333333 (half up at 8 decimals).
        assert_eq!(outcome.lots[0].remaining_ticks, 333_333_333);
        for lot in &outcome.lots {
            // Residual per lot is at most half a unit-cost tick times its quantity.
            assert!(lot.remaining_ticks > 0);
        }
        let bound: i128 = outcome
            .lots
            .iter()
            .map(|lot| lot.remaining_ticks as i128 / 2 + 1)
            .sum();
        assert!(outcome.basis_residual.abs() <= bound);
    }

    // spec: MEDULLA-FINANCE-PRIMITIVES-R005.2.4
    #[test]
    fn invalid_split_ratio_and_overflow_refuse() {
        let lots = open_lots();
        assert_eq!(
            apply_split(&lots, 0, 1).unwrap_err(),
            LotError::InvalidSplitRatio {
                numerator: 0,
                denominator: 1
            }
        );
        assert_eq!(
            apply_split(&lots, 1, 0).unwrap_err(),
            LotError::InvalidSplitRatio {
                numerator: 1,
                denominator: 0
            }
        );
        let huge = [Lot {
            opened_by: 1,
            remaining_ticks: i64::MAX,
            unit_cost_ticks: SCALE,
        }];
        assert_eq!(
            apply_split(&huge, u64::MAX, 1).unwrap_err(),
            LotError::ArithmeticOverflow { sequence: 1 }
        );
        let tiny = [Lot {
            opened_by: 4,
            remaining_ticks: 1,
            unit_cost_ticks: SCALE,
        }];
        assert_eq!(
            apply_split(&tiny, 1, 10).unwrap_err(),
            LotError::SplitRoundsToZero { opened_by: 4 }
        );
    }
}
