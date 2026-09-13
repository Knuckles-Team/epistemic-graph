//! Declared results of the `compute` contract domain.

method_results! {
    visit_compute;
    DsKlDivergence(DsKlDivergence) => Float<f64>;
    FinanceVar(FinanceVar) => Float<f64>;
    FinanceCvar(FinanceCvar) => Float<f64>;
    FinanceMaxDrawdown(FinanceMaxDrawdown) => Float<f64>;
    FinanceDownsideDeviation(FinanceDownsideDeviation) => Float<f64>;
    FinanceMonteCarloVar(FinanceMonteCarloVar) => Float<f64>;
    FinanceInformationCoefficient(FinanceInformationCoefficient) => Float<f64>;
    FinanceMarketImpact(FinanceMarketImpact) => Float<f64>;
    FinanceGlostenMilgromSpread(FinanceGlostenMilgromSpread) => Float<f64>;
    FinanceExpectedPnlRate(FinanceExpectedPnlRate) => Float<f64>;
    FinanceBreakevenAlpha(FinanceBreakevenAlpha) => Float<f64>;
    FinanceVpinPm(FinanceVpinPm) => Float<f64>;
    FinanceHardimanBouchaud(FinanceHardimanBouchaud) => Float<f64>;
    FinanceKyleLambda(FinanceKyleLambda) => Float<f64>;
    FinanceKellyFraction(FinanceKellyFraction) => Float<f64>;
    FinanceBayesianKelly(FinanceBayesianKelly) => Float<f64>;
    FinanceDeflatedSharpe(FinanceDeflatedSharpe) => Float<f64>;
    FinanceProbabilityBacktestOverfit(FinanceProbabilityBacktestOverfit) => Float<f64>;
    FinanceInformationRatio(FinanceInformationRatio) => Float<f64>;
    FinanceEffectiveIndependentN(FinanceEffectiveIndependentN) => Float<f64>;
    FinanceBrierScore(FinanceBrierScore) => Float<f64>;
    FinanceEmpiricalKelly(FinanceEmpiricalKelly) => Float<f64>;
    FinanceSabrImpliedVol(FinanceSabrImpliedVol) => Float<f64>;
    DegreeCentrality(DegreeCentrality) => Float<f64>;
}
