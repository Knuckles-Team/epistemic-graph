use super::*;

const DECLARED: u64 = 64;
const SECOND: u64 = 1_000;

fn policy() -> CapacityThrottlePolicy {
    CapacityThrottlePolicy {
        error_budget_ppm: 50_000, // 5 %
        recovery_ppm: 10_000,     // 1 %
        min_samples: 100,
        decrease_per_mille: 500,
        increase_step: 4,
        floor: 2,
        cooldown_ms: 10 * SECOND,
    }
}

/// The cell's throttle; the declared capacity is `DECLARED`.
struct Cell {
    capacity: u64,
    throttle: Option<CapacityThrottle>,
}

fn cell() -> Cell {
    Cell {
        capacity: DECLARED,
        throttle: Some(CapacityThrottle::open(policy(), DECLARED)),
    }
}

fn apply(
    cell: &mut Cell,
    sample: &ErrorBudgetSample,
    now_ms: u64,
) -> Result<ThrottleActionRecord, String> {
    apply_error_budget(cell.throttle.as_mut(), cell.capacity, sample, now_ms)
}

fn window(requests: u64, errors: u64, end: u64) -> ErrorBudgetSample {
    ErrorBudgetSample {
        requests,
        errors,
        window_end_ms: end,
    }
}

fn ceiling(cell: &Cell) -> u64 {
    cell.throttle.as_ref().unwrap().ceiling
}

#[test]
fn an_error_burst_narrows_multiplicatively_down_to_the_floor() {
    let mut cell = cell();
    let mut now = 100 * SECOND;
    let mut seen = Vec::new();
    for _ in 0..8 {
        let step = apply(&mut cell, &window(1000, 300, now), now).unwrap();
        seen.push((step.action, step.to));
        now += 11 * SECOND;
    }
    assert_eq!(
        seen,
        vec![
            (ThrottleAction::Narrowed, 32),
            (ThrottleAction::Narrowed, 16),
            (ThrottleAction::Narrowed, 8),
            (ThrottleAction::Narrowed, 4),
            (ThrottleAction::Narrowed, 2),
            (ThrottleAction::Held, 2),
            (ThrottleAction::Held, 2),
            (ThrottleAction::Held, 2),
        ]
    );
    assert_eq!(ceiling(&cell), 2);
}

#[test]
fn recovery_is_gradual_needs_evidence_and_stops_at_the_declared_ceiling() {
    let mut cell = cell();
    let mut now = 100 * SECOND;
    apply(&mut cell, &window(1000, 300, now), now).unwrap();
    assert_eq!(ceiling(&cell), 32);
    // Inside the budget but not healthy: no evidence, no widening.
    now += 11 * SECOND;
    let step = apply(&mut cell, &window(1000, 30, now), now).unwrap();
    assert_eq!(
        (step.action, step.reason),
        (ThrottleAction::Held, ThrottleReason::WithinBudget)
    );
    // Too few requests: no evidence either way.
    now += 11 * SECOND;
    let step = apply(&mut cell, &window(10, 0, now), now).unwrap();
    assert_eq!(step.reason, ThrottleReason::InsufficientSamples);
    // Healthy windows give capacity back additively, one step at a time.
    let mut ceilings = Vec::new();
    for _ in 0..12 {
        now += 11 * SECOND;
        apply(&mut cell, &window(1000, 1, now), now).unwrap();
        ceilings.push(ceiling(&cell));
    }
    assert_eq!(
        ceilings,
        vec![36, 40, 44, 48, 52, 56, 60, 64, 64, 64, 64, 64]
    );
    let last = cell
        .throttle
        .as_ref()
        .unwrap()
        .history
        .last()
        .unwrap()
        .clone();
    assert_eq!(last.reason, ThrottleReason::AtDeclaredCeiling);
}

#[test]
fn nothing_automatic_ever_raises_the_ceiling_above_the_declared_capacity() {
    let mut cell = cell();
    let mut now = SECOND;
    let mut max_seen = 0;
    for index in 0..500_u64 {
        now += (index % 7) * 3 * SECOND + 1;
        let errors = [0, 1, 20, 90, 400, 1000][(index % 6) as usize];
        let requests = [1000, 50, 1000, 1000, 5, 1000][(index % 6) as usize];
        let _ = apply(&mut cell, &window(requests, errors.min(requests), now), now);
        max_seen = max_seen.max(ceiling(&cell));
        assert!(ceiling(&cell) <= cell.capacity);
        assert!(ceiling(&cell) >= policy().floor);
    }
    assert!(max_seen <= DECLARED);
    assert_eq!(
        cell.capacity, DECLARED,
        "no automatic step touches the declared capacity"
    );
    assert_eq!(cell.throttle.as_ref().unwrap().policy, policy());
    assert!(cell.throttle.as_ref().unwrap().history.len() <= MAX_THROTTLE_HISTORY);
}

#[test]
fn a_counted_window_is_refused_and_the_cooldown_holds() {
    let mut cell = cell();
    let now = 100 * SECOND;
    apply(&mut cell, &window(1000, 300, now), now).unwrap();
    let replay = apply(&mut cell, &window(1000, 0, now), now + SECOND);
    assert!(replay.unwrap_err().starts_with("THROTTLE_STALE_SAMPLE"));
    let early = apply(&mut cell, &window(1000, 300, now + 1), now + SECOND).unwrap();
    assert_eq!(
        (early.action, early.reason),
        (ThrottleAction::Held, ThrottleReason::CoolingDown)
    );
    assert_eq!(ceiling(&cell), 32);
}

#[test]
fn a_cell_without_a_declared_policy_is_never_throttled() {
    let mut cell = cell();
    cell.throttle = None;
    let refused = apply(&mut cell, &window(1000, 900, SECOND), SECOND);
    assert!(refused.unwrap_err().starts_with("THROTTLE_NO_POLICY"));
}

#[test]
fn every_step_is_recorded_holds_included() {
    let mut cell = cell();
    apply(&mut cell, &window(1000, 300, SECOND), SECOND).unwrap();
    apply(&mut cell, &window(10, 0, 2 * SECOND), 2 * SECOND).unwrap();
    let history = &cell.throttle.as_ref().unwrap().history;
    assert_eq!(history.len(), 2);
    assert_eq!(history[0].error_ppm, 300_000);
    assert_eq!((history[0].from, history[0].to), (DECLARED, 32));
    assert_eq!(history[1].action, ThrottleAction::Held);
}

#[test]
fn policies_and_requests_are_bounded() {
    let mut loose = policy();
    loose.floor = 0;
    assert!(
        loose.validate(DECLARED).is_err(),
        "a zero floor could never recover"
    );
    let mut inverted = policy();
    inverted.recovery_ppm = inverted.error_budget_ppm + 1;
    assert!(inverted.validate(DECLARED).is_err());
    let mut growing = policy();
    growing.decrease_per_mille = 1000;
    assert!(
        growing.validate(DECLARED).is_err(),
        "a decrease must narrow"
    );
    let mut over = CapacityThrottle::open(policy(), DECLARED);
    over.ceiling = DECLARED + 1;
    assert!(over.validate(DECLARED).is_err());
    assert!(
        window(10, 11, 5).validate(10).is_err(),
        "more errors than requests"
    );
    assert!(
        window(10, 1, 11).validate(10).is_err(),
        "a window ending in the future"
    );
}
