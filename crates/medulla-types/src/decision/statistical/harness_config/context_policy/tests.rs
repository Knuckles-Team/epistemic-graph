use super::*;

fn decided(value: ContextPolicy) -> HarnessPart<ContextPolicy> {
    HarnessPart::Decided { value }
}

// spec: MEDULLA-DECISION-ENGINE-R128.5
#[test]
fn context_policy_none_only_when_overflow_impossible() {
    let defaults = HarnessDefaults::default();
    for (window, expected, bound, policy) in [
        (100, 50, Some(99), ContextPolicy::None),
        (100, 100, Some(100), ContextPolicy::None),
        (100, 50, Some(101), ContextPolicy::ElideThenSummarize),
        (100, 101, None, ContextPolicy::ElideThenSummarize),
        (100, 0, None, ContextPolicy::ElideThenSummarize),
        (100, 50, None, ContextPolicy::ElideThenSummarize),
        (1, 0, Some(0), ContextPolicy::None),
        (u32::MAX, u32::MAX, Some(u32::MAX), ContextPolicy::None),
    ] {
        let answer = decide_context_policy(Some(window), Some(expected), bound, &defaults).unwrap();
        assert_eq!(answer.part, decided(policy));
        assert_eq!(answer.thresholds, defaults);
    }
}

// spec: MEDULLA-DECISION-ENGINE-R128.5
#[test]
fn context_pressure_measurements_are_unclamped_and_deterministic() {
    let defaults = HarnessDefaults::default();
    for (window, expected, pressure) in [
        (8, 0, 0),
        (8, 4, Q32 / 2),
        (8, 8, Q32),
        (8, 16, Q32 * 2),
        (3, 1, Q32 / 3),
        (1, u32::MAX, u64::from(u32::MAX) << 32),
        (u32::MAX, 1, 1),
    ] {
        let answer = decide_context_policy(Some(window), Some(expected), None, &defaults).unwrap();
        assert_eq!(answer.pressure_q32, Some(pressure));
        assert_eq!(answer.part, decided(ContextPolicy::ElideThenSummarize));
        assert_eq!(
            answer,
            decide_context_policy(Some(window), Some(expected), None, &defaults).unwrap()
        );
    }
}

// spec: MEDULLA-DECISION-ENGINE-R128.5
#[test]
fn missing_context_inputs_abstain_with_named_defaults() {
    let defaults = HarnessDefaults::default();
    for (window, expected, names) in [
        (None, Some(20), vec!["contextWindowTokens"]),
        (Some(100), None, vec!["expected_trajectory_tokens"]),
        (
            None,
            None,
            vec!["contextWindowTokens", "expected_trajectory_tokens"],
        ),
    ] {
        let answer = decide_context_policy(window, expected, Some(100), &defaults).unwrap();
        assert_eq!(
            answer.part,
            HarnessPart::Abstained {
                missing_fact: BoundedVec::new(names.into_iter().map(str::to_owned).collect())
                    .unwrap(),
                conservative_default: ContextPolicy::ElideThenSummarize,
            }
        );
        assert_eq!(answer.pressure_q32, None);
        assert_eq!(answer.thresholds, defaults);
        answer.part.validate().unwrap();
    }
}

// spec: MEDULLA-DECISION-ENGINE-R128.5
#[test]
fn context_policy_refuses_zero_window_and_contradictory_bounds() {
    let defaults = HarnessDefaults::default();
    for expected in [None, Some(0), Some(u32::MAX)] {
        assert!(decide_context_policy(Some(0), expected, None, &defaults).is_err());
    }
    for window in [None, Some(1), Some(u32::MAX)] {
        assert!(decide_context_policy(window, Some(101), Some(100), &defaults).is_err());
    }
}

// spec: MEDULLA-DECISION-ENGINE-R128.5
#[test]
fn context_policy_preserves_supplied_thresholds_and_refuses_invalid_ones() {
    let defaults = HarnessDefaults::default();
    assert_eq!(defaults.soft_threshold.numerator(), Q32 * 3 / 5);
    assert_eq!(defaults.hard_threshold.numerator(), Q32 * 17 / 20);
    assert_eq!(defaults.recent_window_fraction.numerator(), Q32 * 3 / 10);
    let custom = HarnessDefaults {
        soft_threshold: UnitRationalWire::new(Q32 / 4, Q32).unwrap(),
        hard_threshold: UnitRationalWire::new(Q32 / 2, Q32).unwrap(),
        recent_window_fraction: UnitRationalWire::new(Q32, Q32).unwrap(),
    };
    let answer = decide_context_policy(Some(100), Some(75), None, &custom).unwrap();
    assert_eq!(answer.thresholds, custom);
    for invalid in [
        HarnessDefaults {
            soft_threshold: defaults.hard_threshold,
            ..defaults.clone()
        },
        HarnessDefaults {
            soft_threshold: UnitRationalWire::new(1, 2).unwrap(),
            ..defaults.clone()
        },
        HarnessDefaults {
            recent_window_fraction: UnitRationalWire::new(0, Q32).unwrap(),
            ..defaults
        },
    ] {
        assert!(decide_context_policy(Some(100), Some(50), None, &invalid).is_err());
    }
}
