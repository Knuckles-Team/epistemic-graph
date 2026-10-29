//! Served finance numeric kernels: banking risk (MEDULLA-FINANCE-PRIMITIVES-R901.3),
//! regime and execution schedules (R613.3) and factor ranking (R018.3).
//!
//! Pure compute over what the request carries. Each arm calls the one shared
//! kernel (`eg_numeric::{banking, execution}`, `finance::factor`) and maps its
//! typed result to the wire type; a refusal is `INVALID_ARGUMENT` with the
//! kernel's stable code first in the detail.

use eg_types::compute_result::finance as wire;
use eg_types::result_contract::compute as results;
use serde::Serialize;

use crate::finance::banking::{self, AmlRule, CapitalInputs, CreditExposure};
use crate::finance::execution::{self, AlmgrenChriss, Ident};
use crate::finance::factor::{self, FactorInput};
use crate::protocol::{Method, Response, ResultPayload};

fn refuse(req_id: u64, error: impl std::fmt::Display) -> Response {
    Response::err(req_id, format!("INVALID_ARGUMENT: {error}"))
}

/// The serde name of a closed-enum value (`report-required`, `short_spread`, ...).
fn name<T: Serialize>(value: &T) -> String {
    serde_json::to_value(value)
        .ok()
        .and_then(|v| v.as_str().map(str::to_owned))
        .unwrap_or_default()
}

pub(super) fn handle_numeric_kernels(req_id: u64, method: Method) -> Result<Response, Method> {
    Ok(match method {
        Method::FinanceAmlScreen {
            rule_id,
            version,
            threshold_minor,
            amount_minor,
        } => {
            let rule = AmlRule {
                rule_id,
                version,
                threshold_minor,
            };
            match banking::screen_transaction(&rule, amount_minor) {
                Ok(alert) => Response::ok(
                    req_id,
                    ResultPayload::of::<results::FinanceAmlScreen>(wire::AmlScreenResult {
                        alert: alert.map(|a| wire::AmlAlertResult {
                            rule_id: a.rule_id,
                            rule_version: a.rule_version,
                            kind: name(&a.kind),
                            severity: name(&a.severity),
                            amount_minor: a.amount_minor,
                        }),
                    }),
                ),
                Err(error) => refuse(req_id, error),
            }
        }
        Method::FinanceCapitalRatios {
            cet1,
            at1,
            tier2,
            risk_weighted_assets,
            total_exposure,
        } => match banking::capital_ratios(&CapitalInputs {
            cet1,
            at1,
            tier2,
            risk_weighted_assets,
            total_exposure,
        }) {
            Ok(r) => Response::ok(
                req_id,
                ResultPayload::of::<results::FinanceCapitalRatios>(wire::CapitalRatiosResult {
                    cet1_ratio: r.cet1_ratio,
                    tier1_ratio: r.tier1_ratio,
                    total_capital_ratio: r.total_capital_ratio,
                    leverage_ratio: r.leverage_ratio,
                    cet1_meets_minimum: r.cet1_meets_minimum,
                    total_capital_meets_minimum: r.total_capital_meets_minimum,
                    leverage_meets_minimum: r.leverage_meets_minimum,
                }),
            ),
            Err(error) => refuse(req_id, error),
        },
        Method::FinanceExpectedLoss { pd, lgd, ead_minor } => {
            match banking::expected_loss(&CreditExposure { pd, lgd, ead_minor }) {
                Ok(loss) => Response::ok(
                    req_id,
                    ResultPayload::scalar::<results::FinanceExpectedLoss>(loss),
                ),
                Err(error) => refuse(req_id, error),
            }
        }
        Method::FinanceSettlementChain {
            origin,
            correspondents,
            beneficiary,
            amount_minor,
        } => {
            let hops: Vec<&str> = correspondents.iter().map(String::as_str).collect();
            match banking::settlement_chain(&origin, &hops, &beneficiary, amount_minor) {
                Ok(legs) => Response::ok(
                    req_id,
                    ResultPayload::of::<results::FinanceSettlementChain>(
                        legs.into_iter()
                            .map(|l| wire::SettlementLegResult {
                                sequence: l.sequence,
                                from: l.from,
                                to: l.to,
                                amount_minor: l.amount_minor,
                            })
                            .collect(),
                    ),
                ),
                Err(error) => refuse(req_id, error),
            }
        }
        Method::FinanceRegimeChain {
            returns,
            threshold,
            horizon,
        } => match execution::fit_regime_chain(&returns, threshold, horizon) {
            Ok(f) => Response::ok(
                req_id,
                ResultPayload::of::<results::FinanceRegimeChain>(wire::RegimeChainResult {
                    states: f
                        .matrix
                        .states
                        .iter()
                        .map(|s| s.as_str().to_owned())
                        .collect(),
                    probabilities: f.matrix.probabilities,
                    current_state: f.current_state,
                    forecast: f.forecast,
                    horizon: f.horizon,
                    threshold: f.threshold,
                    hit_rate: f.hit_rate,
                    transitions: f.transitions,
                    solver_status: name(&f.solver_status),
                }),
            ),
            Err(error) => refuse(req_id, error),
        },
        Method::FinanceExecutionSchedule {
            symbol,
            total_shares,
            horizon,
            slices,
            volatility,
            risk_aversion,
            temporary_impact,
            permanent_impact,
            continuous,
        } => {
            let params = Ident::new(&symbol).map(|symbol| AlmgrenChriss {
                symbol,
                total_shares,
                horizon,
                slices,
                volatility,
                risk_aversion,
                temporary_impact,
                permanent_impact,
            });
            match params.and_then(|p| execution::almgren_chriss_schedule(&p, continuous)) {
                Ok(plan) => Response::ok(
                    req_id,
                    ResultPayload::of::<results::FinanceExecutionSchedule>(
                        wire::ExecutionScheduleResult {
                            symbol: plan.params.symbol.as_str().to_owned(),
                            continuous: plan.continuous,
                            total_shares: plan.params.total_shares,
                            horizon: plan.params.horizon,
                            slices: plan.params.slices,
                            volatility: plan.params.volatility,
                            risk_aversion: plan.params.risk_aversion,
                            temporary_impact: plan.params.temporary_impact,
                            permanent_impact: plan.params.permanent_impact,
                            holdings: plan.holdings,
                            trades: plan.trades,
                            objective: plan.objective,
                            solver_status: name(&plan.solver_status),
                        },
                    ),
                ),
                Err(error) => refuse(req_id, error),
            }
        }
        Method::FinanceMarketMakingQuote {
            symbol,
            mid,
            inventory,
            risk_aversion,
            volatility,
            time_remaining,
            order_arrival_decay,
        } => match execution::market_making_quote(
            &symbol,
            mid,
            inventory,
            risk_aversion,
            volatility,
            time_remaining,
            order_arrival_decay,
        ) {
            Ok(q) => Response::ok(
                req_id,
                ResultPayload::of::<results::FinanceMarketMakingQuote>(
                    wire::MarketMakingQuoteResult {
                        symbol: q.symbol.as_str().to_owned(),
                        reservation_price: q.reservation_price,
                        spread: q.spread,
                        bid: q.bid,
                        ask: q.ask,
                    },
                ),
            ),
            Err(error) => refuse(req_id, error),
        },
        Method::FinancePairsSignal {
            y_name,
            x_name,
            y,
            x,
            hedge_ratio,
            entry_z,
        } => match execution::pairs_signal(&y_name, &x_name, &y, &x, hedge_ratio, entry_z) {
            Ok(s) => Response::ok(
                req_id,
                ResultPayload::of::<results::FinancePairsSignal>(wire::PairsSignalResult {
                    y: s.y.as_str().to_owned(),
                    x: s.x.as_str().to_owned(),
                    hedge_ratio: s.hedge_ratio,
                    z_score: s.z_score,
                    entry_z: s.entry_z,
                    side: name(&s.side),
                }),
            ),
            Err(error) => refuse(req_id, error),
        },
        Method::FinanceFactorRank {
            factor_ids,
            signals,
            provenance_keys,
            forward_returns,
        } => {
            if factor_ids.len() != signals.len() || factor_ids.len() != provenance_keys.len() {
                return Ok(refuse(
                    req_id,
                    "FACTOR_RECORD_INVALID: factor_ids, signals and provenance_keys differ in length",
                ));
            }
            let factors: Vec<FactorInput> = factor_ids
                .into_iter()
                .zip(signals)
                .zip(provenance_keys)
                .map(|((factor_id, signal), input_provenance_keys)| FactorInput {
                    factor_id,
                    signal,
                    input_provenance_keys,
                })
                .collect();
            match factor::rank_factors(&factors, &forward_returns) {
                Ok(records) => Response::ok(
                    req_id,
                    ResultPayload::of::<results::FinanceFactorRank>(
                        records
                            .into_iter()
                            .map(|r| wire::FactorRecordResult {
                                factor_id: r.factor_id,
                                information_coefficient: r.information_coefficient,
                                information_ratio: r.information_ratio,
                                rank: r.rank,
                                input_provenance_keys: r.input_provenance_keys,
                            })
                            .collect(),
                    ),
                ),
                Err(error) => refuse(req_id, error),
            }
        }
        other => return Err(other),
    })
}
