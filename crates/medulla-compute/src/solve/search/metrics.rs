//! Bounded completion telemetry; never includes model or tenant data.

/// Absolute scalarized minimization gap. Telemetry uses an approximate f64;
/// certificate scalars and all search decisions retain their exact integers.
fn optimality_gap(incumbent: Option<i128>, lower_bound: Option<i128>) -> Option<f64> {
    let gap = incumbent?.checked_sub(lower_bound?)?;
    (gap >= 0).then_some(gap as f64)
}

pub(super) fn emit(nodes: u64, incumbent: Option<i128>, lower_bound: Option<i128>) {
    let gap = optimality_gap(incumbent, lower_bound);
    tracing::info!(
        target: "medulla_compute::solve",
        solver_nodes = nodes,
        optimality_gap_available = gap.is_some(),
        optimality_gap = gap,
        "solver completed"
    );
}

#[cfg(test)]
#[path = "../../../tests/solve/telemetry_capture.rs"]
mod capture;

#[cfg(test)]
mod tests {
    use super::{capture::Capture, emit, optimality_gap};

    #[test]
    fn absolute_gap_handles_positive_zero_and_negative_objectives() {
        assert_eq!(optimality_gap(Some(12), Some(7)), Some(5.0));
        assert_eq!(optimality_gap(Some(-3), Some(-8)), Some(5.0));
        assert_eq!(optimality_gap(Some(7), Some(7)), Some(0.0));
    }

    #[test]
    fn missing_invalid_and_overflow_gaps_are_unavailable() {
        for (incumbent, bound) in [
            (None, Some(0)),
            (Some(0), None),
            (None, None),
            (Some(2), Some(3)),
            (Some(i128::MAX), Some(i128::MIN)),
        ] {
            assert_eq!(optimality_gap(incumbent, bound), None);
        }
    }

    #[test]
    fn completion_event_records_numeric_metrics_without_fabricating_gap() {
        let capture = Capture::default();
        tracing::subscriber::with_default(capture.clone(), || {
            emit(42, Some(12), Some(7));
            emit(0, Some(12), None);
        });
        let events = capture.events();
        assert_eq!(events.len(), 2);
        assert_eq!(events[0]["solver_nodes"], "42");
        assert_eq!(events[0]["optimality_gap"], "5");
        assert_eq!(events[0]["optimality_gap_available"], "true");
        assert_eq!(events[1]["optimality_gap_available"], "false");
        assert!(!events[1].contains_key("optimality_gap"));
    }
}
