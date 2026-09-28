//! The Decide layer's Prometheus series (EH-074, DECIDE-LAYER-DESIGN §11.3).
//!
//! Every label value comes from a closed set (method name, outcome arm,
//! evidence class, refusal code token, gate pass/fail), so cardinality is
//! bounded by the contract rather than by traffic.

#[cfg(feature = "metrics")]
mod imp {
    use lazy_static::lazy_static;
    use prometheus::{HistogramOpts, HistogramVec, IntCounterVec, Opts};

    use super::super::imp::REGISTRY;

    fn counter_vec(name: &str, help: &str, labels: &[&str]) -> IntCounterVec {
        let metric = IntCounterVec::new(Opts::new(name, help), labels).expect("valid metric");
        REGISTRY
            .register(Box::new(metric.clone()))
            .expect("unique metric");
        metric
    }

    lazy_static! {
        static ref DECISIONS: IntCounterVec = counter_vec(
            "epistemic_graph_decisions_total",
            "Statistical decisions answered, by outcome and evidence class",
            &["outcome", "evidence", "reason"],
        );
        static ref REFUSALS: IntCounterVec = counter_vec(
            "epistemic_graph_decision_refusals_total",
            "Decide-layer calls refused, by method and refusal code",
            &["method", "code"],
        );
        static ref EXPLORATION: IntCounterVec = counter_vec(
            "epistemic_graph_decision_exploration_total",
            "Decisions drawn by the exploration budget versus the greedy branch",
            &["branch"],
        );
        static ref EVALUATIONS: IntCounterVec = counter_vec(
            "epistemic_graph_decision_evaluations_total",
            "Decision-head evaluations, by promotion verdict",
            &["verdict"],
        );
        static ref THRESHOLD_ASSESSMENTS: IntCounterVec = {
            let metric = counter_vec(
                "epistemic_graph_decision_threshold_assessments_total",
                "Persisted, independently labelled decision evaluations assessed against their pinned policy",
                &["signal", "result"],
            );
            for signal in ["support", "coverage", "act_risk"] {
                for result in ["ok", "breach", "unavailable"] {
                    metric.with_label_values(&[signal, result]).inc_by(0);
                }
            }
            metric
        };
        static ref LATENCY: HistogramVec = {
            let metric = HistogramVec::new(
                HistogramOpts::new(
                    "epistemic_graph_decision_duration_seconds",
                    "Decide-layer latency, by method",
                )
                .buckets(prometheus::exponential_buckets(0.0005, 2.5, 14).expect("valid buckets")),
                &["method"],
            )
            .expect("valid metric");
            REGISTRY
                .register(Box::new(metric.clone()))
                .expect("unique metric");
            metric
        };
    }

    /// One answered decision.
    pub fn decision_answered(outcome: &str, evidence: &str, reason: &str) {
        DECISIONS
            .with_label_values(&[outcome, evidence, reason])
            .inc();
    }

    /// One refused Decide-layer call.
    pub fn decision_refused(method: &str, code: &str) {
        REFUSALS.with_label_values(&[method, code]).inc();
    }

    /// One exploration draw's branch.
    pub fn decision_explored(explored: bool) {
        let branch = if explored { "explore" } else { "greedy" };
        EXPLORATION.with_label_values(&[branch]).inc();
    }

    /// One evaluation verdict.
    pub fn decision_evaluated(passed: bool) {
        let verdict = if passed { "passed" } else { "failed" };
        EVALUATIONS.with_label_values(&[verdict]).inc();
    }

    /// One policy-bound threshold signal from a newly persisted evaluation.
    /// Callers supply only the closed signal and result vocabulary.
    pub fn decision_threshold_assessed(signal: &'static str, result: &'static str) {
        THRESHOLD_ASSESSMENTS
            .with_label_values(&[signal, result])
            .inc();
    }

    /// Register all closed label pairs before the first evaluation arrives.
    pub fn initialize_threshold_assessments() {
        let _ = &*THRESHOLD_ASSESSMENTS;
    }

    /// One Decide-layer call's latency.
    pub fn decision_latency(method: &str, seconds: f64) {
        LATENCY.with_label_values(&[method]).observe(seconds);
    }
}

#[cfg(not(feature = "metrics"))]
mod imp {
    pub fn decision_answered(_outcome: &str, _evidence: &str, _reason: &str) {}
    pub fn decision_refused(_method: &str, _code: &str) {}
    pub fn decision_explored(_explored: bool) {}
    pub fn decision_evaluated(_passed: bool) {}
    pub fn decision_threshold_assessed(_signal: &'static str, _result: &'static str) {}
    pub fn initialize_threshold_assessments() {}
    pub fn decision_latency(_method: &str, _seconds: f64) {}
}

pub use imp::*;

#[cfg(all(test, feature = "metrics"))]
mod tests {
    #[test]
    fn threshold_series_have_closed_zero_baselines() {
        let text = crate::metrics::render();
        for signal in ["support", "coverage", "act_risk"] {
            for result in ["ok", "breach", "unavailable"] {
                // The text encoder orders labels by name, so match each label
                // on its own rather than one fixed `signal,result` substring.
                let signal_label = format!("signal=\"{signal}\"");
                let result_label = format!("result=\"{result}\"");
                assert!(
                    text.lines().any(|line| {
                        line.starts_with("epistemic_graph_decision_threshold_assessments_total{")
                            && line.contains(&signal_label)
                            && line.contains(&result_label)
                    }),
                    "missing zero baseline for {signal_label},{result_label}"
                );
            }
        }
    }
}
