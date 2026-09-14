//! Private operation-family dispatch for the finance handler.

use crate::protocol::{Method, Response};

#[derive(Clone, Copy)]
enum FinanceRoute {
    Portfolio,
    Risk,
    Regime,
    Signals,
    Execution,
    MarketMaking,
    Microstructure,
    Sizing,
    Backtest,
    Forensic,
    StateSpace,
    SignalCalibration,
    Derivatives,
    Other,
}

fn route_for(method: &Method) -> FinanceRoute {
    route_for_primary(method)
        .or_else(|| route_for_market(method))
        .or_else(|| route_for_validation(method))
        .or_else(|| route_for_calibration(method))
        .unwrap_or(FinanceRoute::Other)
}

fn route_for_primary(method: &Method) -> Option<FinanceRoute> {
    match method {
        Method::FinanceOptimizePortfolio { .. }
        | Method::FinanceRiskParity { .. }
        | Method::FinanceBlackLitterman { .. }
        | Method::FinanceEfficientFrontier { .. } => Some(FinanceRoute::Portfolio),
        Method::FinanceVar { .. }
        | Method::FinanceCvar { .. }
        | Method::FinanceMaxDrawdown { .. }
        | Method::FinanceDrawdownSeries { .. }
        | Method::FinanceDownsideDeviation { .. }
        | Method::FinanceRiskMetrics { .. }
        | Method::FinanceMonteCarloVar { .. }
        | Method::FinanceStressTest { .. } => Some(FinanceRoute::Risk),
        Method::FinanceDetectRegimes { .. } => Some(FinanceRoute::Regime),
        Method::FinanceRollingZscore { .. }
        | Method::FinanceEwma { .. }
        | Method::FinanceSignalDecay { .. }
        | Method::FinanceCombineAlphas { .. }
        | Method::FinanceCrossSectionalRank { .. }
        | Method::FinanceMomentum { .. }
        | Method::FinanceMeanReversion { .. }
        | Method::FinanceInformationCoefficient { .. } => Some(FinanceRoute::Signals),
        Method::FinanceTwap { .. }
        | Method::FinanceVwap { .. }
        | Method::FinanceMarketImpact { .. }
        | Method::FinancePairsTrading { .. }
        | Method::FinanceMatchOrders { .. } => Some(FinanceRoute::Execution),
        _ => None,
    }
}

fn route_for_market(method: &Method) -> Option<FinanceRoute> {
    match method {
        Method::FinanceAvellanedaStoikov { .. }
        | Method::FinanceGltQuotes { .. }
        | Method::FinanceLogitQuotes { .. }
        | Method::FinanceGlostenMilgromSpread { .. }
        | Method::FinanceExpectedPnlRate { .. }
        | Method::FinanceBreakevenAlpha { .. } => Some(FinanceRoute::MarketMaking),
        Method::FinanceOfiSeries { .. }
        | Method::FinanceMicropriceSeries { .. }
        | Method::FinanceVpinPm { .. }
        | Method::FinanceHawkesMle { .. }
        | Method::FinanceHardimanBouchaud { .. }
        | Method::FinanceKyleLambda { .. }
        | Method::FinanceSurveillanceRisk { .. } => Some(FinanceRoute::Microstructure),
        Method::FinanceKellyFraction { .. }
        | Method::FinanceBayesianKelly { .. }
        | Method::FinancePosteriorCredibleInterval { .. } => Some(FinanceRoute::Sizing),
        _ => None,
    }
}

fn route_for_validation(method: &Method) -> Option<FinanceRoute> {
    match method {
        Method::FinancePurgedCpcv { .. }
        | Method::FinanceDeflatedSharpe { .. }
        | Method::FinanceProbabilityBacktestOverfit { .. }
        | Method::FinanceDieboldMariano { .. } => Some(FinanceRoute::Backtest),
        Method::FinanceForensicReport { .. } => Some(FinanceRoute::Forensic),
        Method::FinanceKalmanFilter1d { .. }
        | Method::FinanceKalmanBeta { .. }
        | Method::FinanceKalmanVolatility { .. }
        | Method::FinanceAdfTest { .. }
        | Method::FinanceOuCalibrate { .. }
        | Method::FinanceOuOptimalThresholds { .. }
        | Method::FinanceMarkovTransitionMatrix { .. } => Some(FinanceRoute::StateSpace),
        _ => None,
    }
}

fn route_for_calibration(method: &Method) -> Option<FinanceRoute> {
    match method {
        Method::FinanceOrderBookImbalance { .. }
        | Method::FinanceQueueImbalance { .. }
        | Method::FinanceRealizedVolTick { .. }
        | Method::FinanceSpreadReversion { .. }
        | Method::FinanceInformationRatio { .. }
        | Method::FinanceEffectiveIndependentN { .. }
        | Method::FinanceAlphaCombinationEngine { .. }
        | Method::FinanceBrierScore { .. }
        | Method::FinanceConvergenceGate { .. }
        | Method::FinanceEmpiricalKelly { .. } => Some(FinanceRoute::SignalCalibration),
        Method::FinanceSabrImpliedVol { .. }
        | Method::FinanceSabrSmile { .. }
        | Method::FinanceSabrCalibrate { .. } => Some(FinanceRoute::Derivatives),
        _ => None,
    }
}

pub(super) fn try_handle(req_id: u64, method: Method) -> Result<Response, Method> {
    match route_for(&method) {
        FinanceRoute::Portfolio => super::route_families::handle_portfolio(req_id, method),
        FinanceRoute::Risk => super::route_families::handle_risk(req_id, method),
        FinanceRoute::Regime => super::route_families::handle_regime(req_id, method),
        FinanceRoute::Signals => super::route_families::handle_signals(req_id, method),
        FinanceRoute::Execution => super::route_families::handle_execution(req_id, method),
        FinanceRoute::MarketMaking => super::route_families::handle_market_making(req_id, method),
        FinanceRoute::Microstructure => {
            super::route_families::handle_microstructure(req_id, method)
        }
        FinanceRoute::Sizing => super::route_families::handle_sizing(req_id, method),
        FinanceRoute::Backtest => super::route_families::handle_backtest(req_id, method),
        FinanceRoute::Forensic => super::route_families::handle_forensic(req_id, method),
        FinanceRoute::StateSpace => super::route_families::handle_state_space(req_id, method),
        FinanceRoute::SignalCalibration => {
            super::route_families::handle_signal_calibration(req_id, method)
        }
        FinanceRoute::Derivatives => super::route_families::handle_derivatives(req_id, method),
        FinanceRoute::Other => Err(method),
    }
}
