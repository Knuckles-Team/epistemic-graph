//! Finance operation-family handlers.

use eg_types::compute_result::finance::PosteriorCredibleInterval;
use eg_types::result_contract::compute as results;

use crate::protocol::{Method, Response, ResultPayload};

pub(super) fn handle_portfolio(req_id: u64, method: Method) -> Result<Response, Method> {
    Ok(match method {
        // ── Portfolio optimization (CONCEPT:AU-KG.memory.mementified-context) ──────────────────
        Method::FinanceOptimizePortfolio {
            expected_returns,
            cov_matrix,
            risk_free_rate,
            min_weight,
            max_weight,
        } => {
            let result = crate::finance::optimizer::mean_variance_optimization(
                &expected_returns,
                &cov_matrix,
                risk_free_rate,
                min_weight,
                max_weight,
            );
            Response::ok(
                req_id,
                ResultPayload::of::<results::FinanceOptimizePortfolio>(result),
            )
        }
        Method::FinanceRiskParity { cov_matrix } => {
            let result = crate::finance::optimizer::risk_parity(&cov_matrix);
            Response::ok(
                req_id,
                ResultPayload::of::<results::FinanceRiskParity>(result),
            )
        }
        Method::FinanceBlackLitterman {
            market_weights,
            cov_matrix,
            views,
            pick_matrix,
            tau,
            risk_aversion,
        } => {
            let result = crate::finance::optimizer::black_litterman(
                &market_weights,
                &cov_matrix,
                &views,
                &pick_matrix,
                tau,
                risk_aversion,
            );
            Response::ok(
                req_id,
                ResultPayload::of::<results::FinanceBlackLitterman>(result),
            )
        }
        Method::FinanceEfficientFrontier {
            expected_returns,
            cov_matrix,
            target_return,
        } => {
            let result = crate::finance::optimizer::efficient_frontier_target(
                &expected_returns,
                &cov_matrix,
                target_return,
            );
            Response::ok(
                req_id,
                ResultPayload::of::<results::FinanceEfficientFrontier>(result),
            )
        }
        other => return Err(other),
    })
}

pub(super) fn handle_risk(req_id: u64, method: Method) -> Result<Response, Method> {
    Ok(match method {
        // ── Extended Finance: Risk (CONCEPT:AU-KG.memory.mementified-context) ──────────────────
        Method::FinanceVar {
            returns,
            confidence,
        } => {
            let v = crate::finance::risk::historical_var(&returns, confidence);
            Response::ok(req_id, ResultPayload::scalar::<results::FinanceVar>(v))
        }
        Method::FinanceCvar {
            returns,
            confidence,
        } => {
            let v = crate::finance::risk::historical_cvar(&returns, confidence);
            Response::ok(req_id, ResultPayload::scalar::<results::FinanceCvar>(v))
        }
        Method::FinanceMaxDrawdown { returns } => {
            let v = crate::finance::risk::max_drawdown(&returns);
            Response::ok(
                req_id,
                ResultPayload::scalar::<results::FinanceMaxDrawdown>(v),
            )
        }
        Method::FinanceDrawdownSeries { returns } => {
            let v = crate::finance::risk::drawdown_series(&returns);
            Response::ok(
                req_id,
                ResultPayload::of::<results::FinanceDrawdownSeries>(v),
            )
        }
        Method::FinanceDownsideDeviation { returns, target } => {
            let v = crate::finance::risk::downside_deviation(&returns, target);
            Response::ok(
                req_id,
                ResultPayload::scalar::<results::FinanceDownsideDeviation>(v),
            )
        }
        Method::FinanceRiskMetrics {
            returns,
            risk_free_rate,
        } => {
            let result = crate::finance::risk::compute_risk_metrics(&returns, risk_free_rate);
            Response::ok(
                req_id,
                ResultPayload::of::<results::FinanceRiskMetrics>(result),
            )
        }
        Method::FinanceMonteCarloVar {
            mean,
            std_dev,
            n_simulations,
            confidence,
        } => {
            let v = crate::finance::risk::monte_carlo_var(mean, std_dev, n_simulations, confidence);
            Response::ok(
                req_id,
                ResultPayload::scalar::<results::FinanceMonteCarloVar>(v),
            )
        }
        Method::FinanceStressTest {
            weights,
            expected_returns,
            cov_matrix,
            shock_factors,
        } => {
            let v = crate::finance::risk::stress_test(
                &weights,
                &expected_returns,
                &cov_matrix,
                &shock_factors,
            );
            Response::ok(req_id, ResultPayload::of::<results::FinanceStressTest>(v))
        }
        other => return Err(other),
    })
}

pub(super) fn handle_regime(req_id: u64, method: Method) -> Result<Response, Method> {
    Ok(match method {
        // ── Extended Finance: Regime detection (HMM) ──────────────────────────────────────────
        Method::FinanceDetectRegimes {
            observations,
            n_states,
            max_iter,
            tol,
        } => {
            let result =
                crate::finance::regime::detect_regimes(&observations, n_states, max_iter, tol);
            Response::ok(
                req_id,
                ResultPayload::of::<results::FinanceDetectRegimes>(result),
            )
        }
        other => return Err(other),
    })
}

pub(super) fn handle_signals(req_id: u64, method: Method) -> Result<Response, Method> {
    Ok(match method {
        // ── Extended Finance: Signals / alpha ─────────────────────────────────────────────────
        Method::FinanceRollingZscore { values, window } => {
            let v = crate::finance::signals::rolling_zscore(&values, window);
            Response::ok(
                req_id,
                ResultPayload::of::<results::FinanceRollingZscore>(v),
            )
        }
        Method::FinanceEwma { values, span } => {
            let v = crate::finance::signals::ewma_signal(&values, span);
            Response::ok(req_id, ResultPayload::of::<results::FinanceEwma>(v))
        }
        Method::FinanceSignalDecay { signal, half_life } => {
            let v = crate::finance::signals::signal_decay(&signal, half_life);
            Response::ok(req_id, ResultPayload::of::<results::FinanceSignalDecay>(v))
        }
        Method::FinanceCombineAlphas { signals, weights } => {
            let v = crate::finance::signals::combine_alphas(&signals, &weights);
            Response::ok(
                req_id,
                ResultPayload::of::<results::FinanceCombineAlphas>(v),
            )
        }
        Method::FinanceCrossSectionalRank { cross_section } => {
            let v = crate::finance::signals::cross_sectional_rank(&cross_section);
            Response::ok(
                req_id,
                ResultPayload::of::<results::FinanceCrossSectionalRank>(v),
            )
        }
        Method::FinanceMomentum { prices, lookback } => {
            let v = crate::finance::signals::momentum(&prices, lookback);
            Response::ok(req_id, ResultPayload::of::<results::FinanceMomentum>(v))
        }
        Method::FinanceMeanReversion { values, window } => {
            let v = crate::finance::signals::mean_reversion(&values, window);
            Response::ok(
                req_id,
                ResultPayload::of::<results::FinanceMeanReversion>(v),
            )
        }
        Method::FinanceInformationCoefficient {
            signal,
            forward_returns,
        } => {
            let v = crate::finance::signals::information_coefficient(&signal, &forward_returns);
            Response::ok(
                req_id,
                ResultPayload::scalar::<results::FinanceInformationCoefficient>(v),
            )
        }
        other => return Err(other),
    })
}

pub(super) fn handle_execution(req_id: u64, method: Method) -> Result<Response, Method> {
    Ok(match method {
        // ── Extended Finance: Execution / microstructure ─────────────────────────────────────
        Method::FinanceTwap {
            total_quantity,
            n_slices,
            start_time,
            interval_secs,
        } => {
            let v = crate::finance::exchange::twap_schedule(
                total_quantity,
                n_slices,
                start_time,
                interval_secs,
            );
            Response::ok(req_id, ResultPayload::of::<results::FinanceTwap>(v))
        }
        Method::FinanceVwap {
            total_quantity,
            volume_profile,
            start_time,
            interval_secs,
        } => {
            let v = crate::finance::exchange::vwap_schedule(
                total_quantity,
                &volume_profile,
                start_time,
                interval_secs,
            );
            Response::ok(req_id, ResultPayload::of::<results::FinanceVwap>(v))
        }
        Method::FinanceMarketImpact {
            daily_volatility,
            order_quantity,
            average_daily_volume,
            impact_coefficient,
        } => {
            let v = crate::finance::exchange::estimate_market_impact(
                daily_volatility,
                order_quantity,
                average_daily_volume,
                impact_coefficient,
            );
            Response::ok(
                req_id,
                ResultPayload::scalar::<results::FinanceMarketImpact>(v),
            )
        }
        Method::FinancePairsTrading {
            prices_a,
            prices_b,
            lookback,
        } => {
            let v = crate::finance::exchange::pairs_trading_signal(&prices_a, &prices_b, lookback);
            Response::ok(req_id, ResultPayload::of::<results::FinancePairsTrading>(v))
        }
        Method::FinanceMatchOrders { orders } => {
            let v = crate::finance::exchange::match_orders(&orders);
            Response::ok(req_id, ResultPayload::of::<results::FinanceMatchOrders>(v))
        }
        other => return Err(other),
    })
}

pub(super) fn handle_market_making(req_id: u64, method: Method) -> Result<Response, Method> {
    Ok(match method {
        // ── Market Making / Microstructure (CONCEPT:EG-KG.domains.market-microstructure-sizing-backtest)
        Method::FinanceAvellanedaStoikov {
            mid,
            inventory,
            sigma,
            gamma,
            kappa,
            tau,
        } => {
            let v =
                crate::finance::quant::avellaneda_stoikov(mid, inventory, sigma, gamma, kappa, tau);
            Response::ok(
                req_id,
                ResultPayload::of::<results::FinanceAvellanedaStoikov>(v),
            )
        }
        Method::FinanceGltQuotes {
            mid,
            inventory,
            sigma,
            gamma,
            kappa,
            a,
        } => {
            let v = crate::finance::quant::glt_quotes(mid, inventory, sigma, gamma, kappa, a);
            Response::ok(req_id, ResultPayload::of::<results::FinanceGltQuotes>(v))
        }
        Method::FinanceLogitQuotes {
            p_mid,
            inventory,
            sigma,
            gamma,
            kappa,
            tau,
            boundary_m,
        } => {
            let v = crate::finance::quant::logit_space_quotes(
                p_mid, inventory, sigma, gamma, kappa, tau, boundary_m,
            );
            Response::ok(req_id, ResultPayload::of::<results::FinanceLogitQuotes>(v))
        }
        Method::FinanceGlostenMilgromSpread { alpha, p } => {
            let v = crate::finance::quant::glosten_milgrom_spread(alpha, p);
            Response::ok(
                req_id,
                ResultPayload::scalar::<results::FinanceGlostenMilgromSpread>(v),
            )
        }
        Method::FinanceExpectedPnlRate {
            delta,
            a,
            kappa,
            alpha,
            p,
            v_h,
            v_l,
        } => {
            let v = crate::finance::quant::expected_pnl_rate(delta, a, kappa, alpha, p, v_h, v_l);
            Response::ok(
                req_id,
                ResultPayload::scalar::<results::FinanceExpectedPnlRate>(v),
            )
        }
        Method::FinanceBreakevenAlpha { delta, p, v_h, v_l } => {
            let v = crate::finance::quant::breakeven_alpha(delta, p, v_h, v_l);
            Response::ok(
                req_id,
                ResultPayload::scalar::<results::FinanceBreakevenAlpha>(v),
            )
        }
        other => return Err(other),
    })
}

pub(super) fn handle_microstructure(req_id: u64, method: Method) -> Result<Response, Method> {
    Ok(match method {
        // ── Kyle insider/stealth surveillance (CONCEPT:EG-KG.domains.concept-2) ──────────────
        Method::FinanceOfiSeries {
            ts,
            bid_px,
            bid_sz,
            ask_px,
            ask_sz,
            window_secs,
        } => {
            let v = crate::finance::quant::ofi_series(
                &ts,
                &bid_px,
                &bid_sz,
                &ask_px,
                &ask_sz,
                window_secs,
            );
            Response::ok(req_id, ResultPayload::of::<results::FinanceOfiSeries>(v))
        }
        Method::FinanceMicropriceSeries {
            bid_px,
            bid_sz,
            ask_px,
            ask_sz,
        } => {
            let v = crate::finance::quant::microprice_series(&bid_px, &bid_sz, &ask_px, &ask_sz);
            Response::ok(
                req_id,
                ResultPayload::of::<results::FinanceMicropriceSeries>(v),
            )
        }
        Method::FinanceVpinPm {
            buy_vol,
            sell_vol,
            p_mean,
        } => {
            let v = crate::finance::quant::vpin_pm(&buy_vol, &sell_vol, &p_mean);
            Response::ok(req_id, ResultPayload::scalar::<results::FinanceVpinPm>(v))
        }
        Method::FinanceHawkesMle {
            times,
            t_horizon,
            max_iter,
        } => {
            let v = crate::finance::quant::hawkes_mle(&times, t_horizon, max_iter);
            Response::ok(req_id, ResultPayload::of::<results::FinanceHawkesMle>(v))
        }
        Method::FinanceHardimanBouchaud {
            times,
            t_horizon,
            n_windows,
        } => {
            let v = crate::finance::quant::hardiman_bouchaud_branching_ratio(
                &times, t_horizon, n_windows,
            );
            Response::ok(
                req_id,
                ResultPayload::scalar::<results::FinanceHardimanBouchaud>(v),
            )
        }
        Method::FinanceKyleLambda {
            price_changes,
            signed_order_flow,
        } => {
            let v = crate::finance::quant::kyle_lambda(&price_changes, &signed_order_flow);
            Response::ok(
                req_id,
                ResultPayload::scalar::<results::FinanceKyleLambda>(v),
            )
        }
        Method::FinanceSurveillanceRisk {
            buy_vol,
            sell_vol,
            p_mean,
            signed_flow,
            price_changes,
            baseline_sigma,
        } => {
            let v = crate::finance::quant::surveillance_risk(
                &buy_vol,
                &sell_vol,
                &p_mean,
                &signed_flow,
                &price_changes,
                baseline_sigma,
            );
            Response::ok(
                req_id,
                ResultPayload::of::<results::FinanceSurveillanceRisk>(v),
            )
        }
        other => return Err(other),
    })
}

pub(super) fn handle_sizing(req_id: u64, method: Method) -> Result<Response, Method> {
    Ok(match method {
        // ── Position Sizing (CONCEPT:EG-KG.domains.market-microstructure-sizing-backtest) ────
        Method::FinanceKellyFraction { q, c, fraction } => {
            let v = crate::finance::quant::kelly_fraction(q, c, fraction);
            Response::ok(
                req_id,
                ResultPayload::scalar::<results::FinanceKellyFraction>(v),
            )
        }
        Method::FinanceBayesianKelly {
            alpha,
            beta,
            c,
            n_quadrature,
        } => {
            let v = crate::finance::quant::bayesian_kelly_fraction(alpha, beta, c, n_quadrature);
            Response::ok(
                req_id,
                ResultPayload::scalar::<results::FinanceBayesianKelly>(v),
            )
        }
        Method::FinancePosteriorCredibleInterval { alpha, beta, level } => {
            let (lower, upper) =
                crate::finance::quant::posterior_credible_interval(alpha, beta, level);
            Response::ok(
                req_id,
                ResultPayload::of::<results::FinancePosteriorCredibleInterval>(
                    PosteriorCredibleInterval { lower, upper },
                ),
            )
        }
        other => return Err(other),
    })
}

pub(super) fn handle_backtest(req_id: u64, method: Method) -> Result<Response, Method> {
    Ok(match method {
        // ── Backtest Validation (CONCEPT:EG-KG.domains.market-microstructure-sizing-backtest) ─
        Method::FinancePurgedCpcv {
            n_samples,
            n_groups,
            n_test_groups,
            purge_window,
            embargo,
        } => {
            let v = crate::finance::quant::purged_cpcv_splits(
                n_samples,
                n_groups,
                n_test_groups,
                purge_window,
                embargo,
            );
            Response::ok(req_id, ResultPayload::of::<results::FinancePurgedCpcv>(v))
        }
        Method::FinanceDeflatedSharpe {
            observed_sr,
            n_trials,
            sr_returns,
        } => {
            let v =
                crate::finance::quant::deflated_sharpe_ratio(observed_sr, n_trials, &sr_returns);
            Response::ok(
                req_id,
                ResultPayload::scalar::<results::FinanceDeflatedSharpe>(v),
            )
        }
        Method::FinanceProbabilityBacktestOverfit { insample, oos } => {
            let v = crate::finance::quant::probability_of_backtest_overfit(&insample, &oos);
            Response::ok(
                req_id,
                ResultPayload::scalar::<results::FinanceProbabilityBacktestOverfit>(v),
            )
        }
        Method::FinanceDieboldMariano {
            losses_a,
            losses_b,
            h,
        } => {
            let v = crate::finance::quant::diebold_mariano(&losses_a, &losses_b, h);
            Response::ok(
                req_id,
                ResultPayload::of::<results::FinanceDieboldMariano>(v),
            )
        }
        other => return Err(other),
    })
}

pub(super) fn handle_forensic(req_id: u64, method: Method) -> Result<Response, Method> {
    Ok(match method {
        // ── Forensic Accounting (CONCEPT:EG-KG.domains.forensic-accounting-kernels) ───────────
        Method::FinanceForensicReport {
            this_year,
            prior_year,
        } => {
            let v = crate::finance::forensic::forensic_report(&this_year, &prior_year);
            Response::ok(
                req_id,
                ResultPayload::of::<results::FinanceForensicReport>(v),
            )
        }
        other => return Err(other),
    })
}

pub(super) fn handle_state_space(req_id: u64, method: Method) -> Result<Response, Method> {
    Ok(match method {
        // ── State-Space / Stat-Arb (CONCEPT:EG-KG.domains.state-space-statistical-arbitrage) ─
        Method::FinanceKalmanFilter1d {
            observations,
            f,
            q,
            h,
            r,
            x0,
            p0,
        } => {
            let v = crate::finance::statespace::kalman_filter_1d(&observations, f, q, h, r, x0, p0);
            Response::ok(
                req_id,
                ResultPayload::of::<results::FinanceKalmanFilter1d>(v),
            )
        }
        Method::FinanceKalmanBeta {
            market_returns,
            asset_returns,
            q,
            r,
            beta0,
            p0,
        } => {
            let v = crate::finance::statespace::kalman_beta(
                &market_returns,
                &asset_returns,
                q,
                r,
                beta0,
                p0,
            );
            Response::ok(req_id, ResultPayload::of::<results::FinanceKalmanBeta>(v))
        }
        Method::FinanceKalmanVolatility {
            returns,
            q,
            r,
            log_var0,
            p0,
            annualization,
        } => {
            let v = crate::finance::statespace::kalman_volatility(
                &returns,
                q,
                r,
                log_var0,
                p0,
                annualization,
            );
            Response::ok(
                req_id,
                ResultPayload::of::<results::FinanceKalmanVolatility>(v),
            )
        }
        Method::FinanceAdfTest { series, max_lag } => {
            let v = crate::finance::statespace::adf_test(&series, max_lag);
            Response::ok(req_id, ResultPayload::of::<results::FinanceAdfTest>(v))
        }
        Method::FinanceOuCalibrate { spread, dt } => {
            let v = crate::finance::statespace::ou_calibrate(&spread, dt);
            Response::ok(req_id, ResultPayload::of::<results::FinanceOuCalibrate>(v))
        }
        Method::FinanceOuOptimalThresholds {
            theta,
            mu,
            sigma,
            sigma_eq,
            cost,
        } => {
            let params = crate::finance::statespace::OuParams {
                theta,
                mu,
                sigma,
                sigma_eq,
                half_life: if theta > 1e-12 {
                    std::f64::consts::LN_2 / theta
                } else {
                    f64::INFINITY
                },
            };
            let v = crate::finance::statespace::ou_optimal_thresholds(&params, cost);
            Response::ok(
                req_id,
                ResultPayload::of::<results::FinanceOuOptimalThresholds>(v),
            )
        }
        Method::FinanceMarkovTransitionMatrix { states, n_states } => {
            let v = crate::finance::statespace::markov_transition_matrix(&states, n_states);
            Response::ok(
                req_id,
                ResultPayload::of::<results::FinanceMarkovTransitionMatrix>(v),
            )
        }
        other => return Err(other),
    })
}

#[derive(Clone, Copy)]
enum SignalCalibrationRoute {
    Market,
    Quality,
    Other,
}

fn signal_calibration_route(method: &Method) -> SignalCalibrationRoute {
    match method {
        Method::FinanceOrderBookImbalance { .. }
        | Method::FinanceQueueImbalance { .. }
        | Method::FinanceRealizedVolTick { .. }
        | Method::FinanceSpreadReversion { .. } => SignalCalibrationRoute::Market,
        Method::FinanceInformationRatio { .. }
        | Method::FinanceEffectiveIndependentN { .. }
        | Method::FinanceAlphaCombinationEngine { .. }
        | Method::FinanceBrierScore { .. }
        | Method::FinanceConvergenceGate { .. }
        | Method::FinanceEmpiricalKelly { .. } => SignalCalibrationRoute::Quality,
        _ => SignalCalibrationRoute::Other,
    }
}

pub(super) fn handle_signal_calibration(req_id: u64, method: Method) -> Result<Response, Method> {
    match signal_calibration_route(&method) {
        SignalCalibrationRoute::Market => handle_signal_market(req_id, method),
        SignalCalibrationRoute::Quality => handle_signal_quality(req_id, method),
        SignalCalibrationRoute::Other => Err(method),
    }
}

fn handle_signal_market(req_id: u64, method: Method) -> Result<Response, Method> {
    Ok(match method {
        // Signal combination and market microstructure
        Method::FinanceOrderBookImbalance { v_bid, v_ask } => {
            let v = crate::finance::quant::order_book_imbalance(&v_bid, &v_ask);
            Response::ok(
                req_id,
                ResultPayload::of::<results::FinanceOrderBookImbalance>(v),
            )
        }
        Method::FinanceQueueImbalance {
            bid_q,
            ask_q,
            bid_rate,
            ask_rate,
        } => {
            let v = crate::finance::quant::queue_imbalance(&bid_q, &ask_q, &bid_rate, &ask_rate);
            Response::ok(
                req_id,
                ResultPayload::of::<results::FinanceQueueImbalance>(v),
            )
        }
        Method::FinanceRealizedVolTick { mid, window } => {
            let v = crate::finance::quant::realized_vol_tick(&mid, window);
            Response::ok(
                req_id,
                ResultPayload::of::<results::FinanceRealizedVolTick>(v),
            )
        }
        Method::FinanceSpreadReversion {
            bid_px,
            ask_px,
            window,
        } => {
            let v = crate::finance::quant::spread_reversion(&bid_px, &ask_px, window);
            Response::ok(
                req_id,
                ResultPayload::of::<results::FinanceSpreadReversion>(v),
            )
        }
        other => return Err(other),
    })
}

fn handle_signal_quality(req_id: u64, method: Method) -> Result<Response, Method> {
    Ok(match method {
        // Signal quality, validation, and sizing
        Method::FinanceInformationRatio { ic, n_independent } => {
            let v = crate::finance::quant::information_ratio(ic, n_independent);
            Response::ok(
                req_id,
                ResultPayload::scalar::<results::FinanceInformationRatio>(v),
            )
        }
        Method::FinanceEffectiveIndependentN { returns_matrix } => {
            let v = crate::finance::quant::effective_independent_n(&returns_matrix);
            Response::ok(
                req_id,
                ResultPayload::scalar::<results::FinanceEffectiveIndependentN>(v),
            )
        }
        Method::FinanceAlphaCombinationEngine {
            returns_matrix,
            lookback,
        } => {
            let v = crate::finance::quant::alpha_combination_engine(&returns_matrix, lookback);
            Response::ok(
                req_id,
                ResultPayload::of::<results::FinanceAlphaCombinationEngine>(v),
            )
        }
        Method::FinanceBrierScore {
            forecasts,
            outcomes,
        } => {
            let v = crate::finance::quant::brier_score(&forecasts, &outcomes);
            Response::ok(
                req_id,
                ResultPayload::scalar::<results::FinanceBrierScore>(v),
            )
        }
        Method::FinanceConvergenceGate {
            strengths,
            strong_threshold,
            min_agree,
        } => {
            let v =
                crate::finance::quant::convergence_gate(&strengths, strong_threshold, min_agree);
            Response::ok(
                req_id,
                ResultPayload::of::<results::FinanceConvergenceGate>(v),
            )
        }
        Method::FinanceEmpiricalKelly {
            p,
            b,
            historical_returns,
            n_simulations,
            seed,
        } => {
            let v = crate::finance::quant::empirical_kelly(
                p,
                b,
                &historical_returns,
                n_simulations,
                seed,
            );
            Response::ok(
                req_id,
                ResultPayload::scalar::<results::FinanceEmpiricalKelly>(v),
            )
        }
        other => return Err(other),
    })
}
pub(super) fn handle_derivatives(req_id: u64, method: Method) -> Result<Response, Method> {
    Ok(match method {
        // ── Derivatives: SABR volatility surface (CONCEPT:AU-KG.domains.derivatives) ─────────
        Method::FinanceSabrImpliedVol {
            f,
            k,
            t,
            alpha,
            beta,
            rho,
            nu,
        } => {
            let v = crate::finance::derivatives::sabr_implied_vol(f, k, t, alpha, beta, rho, nu);
            Response::ok(
                req_id,
                ResultPayload::scalar::<results::FinanceSabrImpliedVol>(v),
            )
        }
        Method::FinanceSabrSmile {
            f,
            strikes,
            t,
            alpha,
            beta,
            rho,
            nu,
        } => {
            let v = crate::finance::derivatives::sabr_smile(f, &strikes, t, alpha, beta, rho, nu);
            Response::ok(req_id, ResultPayload::of::<results::FinanceSabrSmile>(v))
        }
        Method::FinanceSabrCalibrate {
            f,
            t,
            strikes,
            market_vols,
            beta,
        } => {
            let v = crate::finance::derivatives::sabr_calibrate(f, t, &strikes, &market_vols, beta);
            Response::ok(
                req_id,
                ResultPayload::of::<results::FinanceSabrCalibrate>(v),
            )
        }
        other => return Err(other),
    })
}
