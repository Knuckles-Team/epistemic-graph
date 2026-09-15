macro_rules! __eg_method_chunk_4 {
    (@acc [$($variants:tt)*]) => {
        __eg_method_chunk_5!(@acc [
$($variants)*

    FinanceStressTest {
        weights: Vec<f64>,
        expected_returns: Vec<f64>,
        cov_matrix: Vec<Vec<f64>>,
        shock_factors: Vec<f64>,
    },


    // ── Extended Finance: Regime detection (HMM) ──────────────────────
    FinanceDetectRegimes {
        observations: Vec<f64>,
        n_states: usize,
        max_iter: usize,
        tol: f64,
    },


    // ── Extended Finance: Signals / alpha ─────────────────────────────
    FinanceRollingZscore {
        values: Vec<f64>,
        window: usize,
    },

    FinanceEwma {
        values: Vec<f64>,
        span: usize,
    },

    FinanceSignalDecay {
        signal: Vec<f64>,
        half_life: f64,
    },

    FinanceCombineAlphas {
        signals: Vec<Vec<f64>>,
        weights: Vec<f64>,
    },

    FinanceCrossSectionalRank {
        cross_section: Vec<Vec<f64>>,
    },

    FinanceMomentum {
        prices: Vec<f64>,
        lookback: usize,
    },

    FinanceMeanReversion {
        values: Vec<f64>,
        window: usize,
    },

    FinanceInformationCoefficient {
        signal: Vec<f64>,
        forward_returns: Vec<f64>,
    },


    // ── Extended Finance: Execution / microstructure ──────────────────
    FinanceTwap {
        total_quantity: f64,
        n_slices: usize,
        start_time: u64,
        interval_secs: u64,
    },

    FinanceVwap {
        total_quantity: f64,
        volume_profile: Vec<f64>,
        start_time: u64,
        interval_secs: u64,
    },

    FinanceMarketImpact {
        daily_volatility: f64,
        order_quantity: f64,
        average_daily_volume: f64,
        impact_coefficient: f64,
    },

    FinancePairsTrading {
        prices_a: Vec<f64>,
        prices_b: Vec<f64>,
        lookback: usize,
    },

    // Embeds a `finance` domain type → gated with the feature.
    #[cfg(feature = "finance")]
    FinanceMatchOrders {
        orders: Vec<crate::wire::Order>,
    },


    // ── Market Making / Microstructure (CONCEPT:EG-KG.domains.market-microstructure-sizing-backtest) ─────────────
    FinanceAvellanedaStoikov {
        mid: f64,
        inventory: f64,
        sigma: f64,
        gamma: f64,
        kappa: f64,
        tau: f64,
    },

    FinanceGltQuotes {
        mid: f64,
        inventory: f64,
        sigma: f64,
        gamma: f64,
        kappa: f64,
        a: f64,
    },

    FinanceLogitQuotes {
        p_mid: f64,
        inventory: f64,
        sigma: f64,
        gamma: f64,
        kappa: f64,
        tau: f64,
        boundary_m: f64,
    },

    FinanceGlostenMilgromSpread {
        alpha: f64,
        p: f64,
    },

    FinanceExpectedPnlRate {
        delta: f64,
        a: f64,
        kappa: f64,
        alpha: f64,
        p: f64,
        v_h: f64,
        v_l: f64,
    },

    FinanceBreakevenAlpha {
        delta: f64,
        p: f64,
        v_h: f64,
        v_l: f64,
    },

    FinanceOfiSeries {
        ts: Vec<f64>,
        bid_px: Vec<f64>,
        bid_sz: Vec<f64>,
        ask_px: Vec<f64>,
        ask_sz: Vec<f64>,
        window_secs: f64,
    },

    FinanceMicropriceSeries {
        bid_px: Vec<f64>,
        bid_sz: Vec<f64>,
        ask_px: Vec<f64>,
        ask_sz: Vec<f64>,
    },

    FinanceVpinPm {
        buy_vol: Vec<f64>,
        sell_vol: Vec<f64>,
        p_mean: Vec<f64>,
    },

    FinanceHawkesMle {
        times: Vec<f64>,
        t_horizon: f64,
        max_iter: usize,
    },

    FinanceHardimanBouchaud {
        times: Vec<f64>,
        t_horizon: f64,
        n_windows: usize,
    },


    // ── Kyle insider/stealth surveillance (CONCEPT:EG-KG.domains.concept-2) ──────────
    FinanceKyleLambda {
        price_changes: Vec<f64>,
        signed_order_flow: Vec<f64>,
    },

    FinanceSurveillanceRisk {
        buy_vol: Vec<f64>,
        sell_vol: Vec<f64>,
        p_mean: Vec<f64>,
        signed_flow: Vec<f64>,
        price_changes: Vec<f64>,
        baseline_sigma: f64,
    },


    // ── Position Sizing (CONCEPT:EG-KG.domains.market-microstructure-sizing-backtest) ────────────────────────────
    FinanceKellyFraction {
        q: f64,
        c: f64,
        fraction: f64,
    },

    FinanceBayesianKelly {
        alpha: f64,
        beta: f64,
        c: f64,
        n_quadrature: usize,
    },

    FinancePosteriorCredibleInterval {
        alpha: f64,
        beta: f64,
        level: f64,
    },


    // ── Backtest Validation (CONCEPT:EG-KG.domains.market-microstructure-sizing-backtest) ────────────────────────
    FinancePurgedCpcv {
        n_samples: usize,
        n_groups: usize,
        n_test_groups: usize,
        purge_window: usize,
        embargo: usize,
    },

    FinanceDeflatedSharpe {
        observed_sr: f64,
        n_trials: usize,
        sr_returns: Vec<f64>,
    },

    FinanceProbabilityBacktestOverfit {
        insample: Vec<Vec<f64>>,
        oos: Vec<Vec<f64>>,
    },

    FinanceDieboldMariano {
        losses_a: Vec<f64>,
        losses_b: Vec<f64>,
        h: usize,
    },


    // ── Forensic Accounting (CONCEPT:EG-KG.domains.forensic-accounting-kernels) ────────────────────────
    // Embeds `finance` domain types → gated with the feature.
    #[cfg(feature = "finance")]
    FinanceForensicReport {
        this_year: crate::wire::YearData,
        prior_year: crate::wire::YearData,
    },


    // ── State-Space / Stat-Arb (CONCEPT:EG-KG.domains.state-space-statistical-arbitrage) ─────────────────────
    FinanceKalmanFilter1d {
        observations: Vec<f64>,
        f: f64,
        q: f64,
        h: f64,
        r: f64,
        x0: f64,
        p0: f64,
    },

    FinanceKalmanBeta {
        market_returns: Vec<f64>,
        asset_returns: Vec<f64>,
        q: f64,
        r: f64,
        beta0: f64,
        p0: f64,
    },

    FinanceKalmanVolatility {
        returns: Vec<f64>,
        q: f64,
        r: f64,
        log_var0: Option<f64>,
        p0: f64,
        annualization: f64,
    },

    FinanceAdfTest {
        series: Vec<f64>,
        max_lag: usize,
    },

    FinanceOuCalibrate {
        spread: Vec<f64>,
        dt: f64,
    },

    FinanceOuOptimalThresholds {
        theta: f64,
        mu: f64,
        sigma: f64,
        sigma_eq: f64,
        cost: f64,
    },

    FinanceMarkovTransitionMatrix {
        states: Vec<usize>,
        n_states: usize,
    },


    // ── Signal Combination / Sizing / Calibration (CONCEPT:EG-KG.domains.quant-finance) ──
    FinanceOrderBookImbalance {
        v_bid: Vec<f64>,
        v_ask: Vec<f64>,
    },

    FinanceQueueImbalance {
        bid_q: Vec<f64>,
        ask_q: Vec<f64>,
        bid_rate: Vec<f64>,
        ask_rate: Vec<f64>,
    },

    FinanceRealizedVolTick {
        mid: Vec<f64>,
        window: usize,
    },

    FinanceSpreadReversion {
        bid_px: Vec<f64>,
        ask_px: Vec<f64>,
        window: usize,
    },

    FinanceInformationRatio {
        ic: f64,
        n_independent: f64,
    },

    FinanceEffectiveIndependentN {
        returns_matrix: Vec<Vec<f64>>,
    },

    FinanceAlphaCombinationEngine {
        returns_matrix: Vec<Vec<f64>>,
        lookback: usize,
    },

    FinanceBrierScore {
        forecasts: Vec<f64>,
        outcomes: Vec<f64>,
    },

    FinanceConvergenceGate {
        strengths: Vec<f64>,
        strong_threshold: f64,
        min_agree: usize,
    },

    FinanceEmpiricalKelly {
        p: f64,
        b: f64,
        historical_returns: Vec<f64>,
        n_simulations: usize,
        seed: u64,
    },


    // ── Derivatives: SABR volatility surface (CONCEPT:AU-KG.domains.derivatives) ────────
    FinanceSabrImpliedVol {
        f: f64,
        k: f64,
        t: f64,
        alpha: f64,
        beta: f64,
        rho: f64,
        nu: f64,
    },

    FinanceSabrSmile {
        f: f64,
        strikes: Vec<f64>,
        t: f64,
        alpha: f64,
        beta: f64,
        rho: f64,
        nu: f64,
    },

    FinanceSabrCalibrate {
        f: f64,
        t: f64,
        strikes: Vec<f64>,
        market_vols: Vec<f64>,
        beta: f64,
    },


    // ── Zero-Trust Consensus ─────────────────────────────────────────
    RegisterIdentity {
        agent_id: String,
        role: crate::acl::AgentRole,
        teams: Vec<String>,
        signature: String,
        /// RBAC role names this agent holds (CONCEPT:EG-KG.compute.feature).
        roles: Vec<String>,
    },

    /// Read back one principal's currently-registered identity (CONCEPT:EG-KG.compute.feature).
    /// `RegisterIdentity` REPLACES an agent's whole role set on every call -- there is no
    /// merge -- so a caller that wants to ADD a role without silently dropping one already
    /// granted by a prior admission pass must read the current set back first; this closes
    /// that gap. The handler returns `Option<AgentIdentity>` (`eg_types::acl::AgentIdentity`):
    /// `None` means "no identity registered for `agent_id`" (unknown), which callers MUST
    /// keep distinct from `Some(identity)` carrying an empty `roles` Vec (registered,
    /// CONFIRMED to hold no roles) -- conflating the two would reintroduce the exact
    /// blind-upsert ambiguity this RPC exists to eliminate. Gated `security:admin`, the same
    /// scope `RegisterIdentity` already requires, so this grants no new privilege to anyone
    /// who could not already call `RegisterIdentity`.
    GetIdentity {
        agent_id: String,
    },

    /// Administer the RBAC role/grant policy (CONCEPT:EG-KG.compute.feature). Unconditional in the
    /// enum; the handler is gated behind the `security` feature (a non-security build
    /// falls to the dispatch "not available in this build" catch-all, like EG-090's
    /// backup/restore on a non-redb build).
    RbacAdmin {
        op: crate::acl::RbacAdminOp,
    },

    ApplyMultisigMutation {
        signatures: Vec<String>,
        threshold: usize,
        mutation_type: String,
        query: String,
    },


    /// The durable analytics-job plane (CONCEPT:INT-P2-1): async caller control
    /// plus verified remote-worker claim/renew/checkpoint/stage/publish/cancel over
    /// a coordinator-owned `AnalyticsJob` state machine (`eg-jobs`),
    /// whose eventual success commits a provenance'd `:Claim`/`:Evidence` pair (the
    /// SAME typed-node convention `eg-epistemic` reads). ONE variant wrapping an
    /// internal op enum — mirrors `RbacAdmin { op }` above — so the whole
    /// full surface costs exactly one `Method` arm. Gated `jobs`; the handler
    /// (`src/server/handlers/jobs.rs`)
    /// self-routes in `dispatch.rs` before the per-graph chain (jobs are keyed by
    /// `job_id` in their own `jobs.redb`, not a graph — like `TsAppend`/`Kv*`).
    #[cfg(feature = "jobs")]
    AnalyticsJob {
        op: crate::jobs::JobOp,
    },


    /// The native finite-state-machine / statechart engine (CONCEPT:INT-P2-2):
    /// define/instantiate/send_event/get_state/list over a durable, rehydratable
    /// `MachineInstance` `(state, context)` record in `statecharts.redb` (`eg-statechart`).
    /// ONE variant wrapping an internal op enum — mirrors `AnalyticsJob { op }` above —
    /// so the whole engine surface costs exactly one `Method` arm. Gated `statechart`;
    /// the handler (`src/server/handlers/statechart.rs`) self-routes in `dispatch.rs`
    /// before the per-graph chain (instances are keyed by `instance_id` in their own
    /// `statecharts.redb`, not a graph — like `AnalyticsJob`/`TsAppend`/`Kv*`).
    #[cfg(feature = "statechart")]
    Statechart {
        op: crate::statechart::StatechartOp,
    },


    /// The agent-facing quantum control-plane surface (Q8, CONCEPT:EG-KG.compute.quantum-agent-api):
    /// `quantum_rank`/`optimize_with_qaoa`/`quantum_expectation` over a registered
    /// `eg_quantum_core::backend::QuantumBackend` (today: `eg-quantum-sim`'s
    /// `sv-cpu`/`stabilizer`). ONE variant wrapping an internal op enum — mirrors
    /// `AnalyticsJob { op }`/`Statechart { op }` above — so the whole surface costs
    /// exactly one `Method` arm. Gated `quantum`; the handler
    /// (`src/server/handlers/quantum.rs`) self-routes in `dispatch.rs` before the
    /// per-graph chain (a quantum run reads no persisted graph state and writes
    /// nothing durable — every result is returned to the caller as a proposal,
    /// never committed — like `AnalyticsJob`/`Statechart`/`TsAppend`/`Kv*`).
    #[cfg(feature = "quantum")]
    Quantum {
        op: crate::quantum::QuantumOp,
    },


    /// Native ASR provider surface (GOC-33, `OWNER-VOICE-ASR`): a direct,
    /// non-durable batch-file transcription call over the whisper-rs/
    /// whisper.cpp provider in `eg-asr-whisper` — the wire surface
    /// `audio-transcriber`'s pluggable `TranscriptionProvider` seam reaches
    /// over the existing `epistemic_graph.client` transport (no second
    /// transport). ONE variant wrapping an internal op enum, mirroring
    /// `Quantum { op }`/`Statechart { op }` above. Gated `asr-native`; the
    /// handler (`src/server/handlers/asr.rs`) self-routes in `dispatch.rs`
    /// before the per-graph chain (a transcription reads no persisted graph
    /// state and commits no durable `asr.result.v1` — see
    /// `crates/eg-audio/src/asr.rs`'s module doc for that authority
    /// boundary, owned by future worker/AU-orchestration work).
    #[cfg(feature = "asr-native")]
    Asr {
        op: crate::asr_wire::AsrOp,
    },
        ]);
    };
}

pub(crate) use __eg_method_chunk_4;
