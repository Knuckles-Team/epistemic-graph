// CONCEPT:EG-KG.domains.finance-compute.lot-accounting — deterministic
// fixed-point lot/cost-basis matching (EG-FINANCE-PRIMITIVES-R005.1).
//
// Money and quantity are fixed-point integers scaled by `SCALE`, never
// `f64`: replaying the same ordered activities must reproduce byte-identical
// results, which floating-point rounding cannot guarantee across hosts.
//
// Scope: the standalone FIFO/LIFO cost-basis matching engine only, over its
// own minimal `LotActivity`/`Lot` types -- not finance-v1's wire
// `Account`/`Activity`/`Lot`/`Position` records (EG-FINANCE-PRIMITIVES-R004.2,
// still landing). Wiring to those wire types, plus specific-lot/average-cost
// elections and P&L/TWR/XIRR, are later EG-FINANCE-PRIMITIVES-R005 children.

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
    /// Specific-lot election (EG-FINANCE-PRIMITIVES-R005.2.1): close exactly
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
    /// A specific-lot election (EG-FINANCE-PRIMITIVES-R005.2.1) named a
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
        }
    }
}

impl std::error::Error for LotError {}

/// Apply an ordered sequence of buy/sell activities and return the resulting
/// open lots and every lot-closing this produced, under one election rule.
/// The same input slice always produces the same output (Sec EG-FINANCE-
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
        last_sequence = Some(activity.sequence);

        if activity.quantity_ticks > 0 {
            open.push(Lot {
                opened_by: activity.sequence,
                remaining_ticks: activity.quantity_ticks,
                unit_cost_ticks: activity.price_ticks,
            });
            continue;
        }

        let mut to_close = -activity.quantity_ticks;
        let available: i64 = open.iter().map(|lot| lot.remaining_ticks).sum();
        if to_close > available {
            return Err(LotError::Oversold {
                sequence: activity.sequence,
                requested_ticks: to_close,
                available_ticks: available,
            });
        }

        let order: Vec<usize> = match method {
            LotMethod::Fifo => (0..open.len()).collect(),
            LotMethod::Lifo => (0..open.len()).rev().collect(),
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
                vec![idx]
            }
        };
        for idx in order {
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
    }

    Ok(LotLedger { open, closed })
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

    #[test]
    fn fifo_closes_the_oldest_lot_first() {
        let activities = [act(1, 10, 100), act(2, 5, 110), act(3, 12, 120)];
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

    #[test]
    fn lifo_closes_the_newest_lot_first() {
        let activities = [act(1, 10, 100), act(2, 5, 110), act(3, 12, 120)];
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

    #[test]
    fn realized_gain_is_proceeds_minus_cost_times_quantity() {
        let activities = [act(1, 10, 100), act(2, 10, 150)];
        let ledger = apply_activities(&activities, LotMethod::Fifo).unwrap();
        assert_eq!(ledger.closed[0].realized_gain_ticks, 10 * 50 * SCALE);
    }

    #[test]
    fn oversold_quantity_is_refused() {
        let activities = [act(1, 10, 100), act(2, 11, 100)];
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

    // spec: EG-FINANCE-PRIMITIVES-R005.2.1
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

    // spec: EG-FINANCE-PRIMITIVES-R005.2.1
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

    // spec: EG-FINANCE-PRIMITIVES-R005.2.1
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
}
