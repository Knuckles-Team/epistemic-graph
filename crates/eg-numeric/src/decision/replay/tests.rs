use super::*;

fn spec(train: u32, test: u32, step: u32, purge: u32, embargo: u32) -> WalkForward {
    WalkForward {
        train,
        test,
        step,
        purge,
        embargo,
    }
}

/// Two options per step; option `a` is acceptable on even steps, `b` on odd.
fn steps(n: usize) -> Vec<ReplayStep> {
    (0..n)
        .map(|i| ReplayStep {
            at_ms: 1_000 + i as u64,
            option_ids: vec!["a".to_string(), "b".to_string()],
            utilities: if i % 2 == 0 {
                vec![1.0, 0.0]
            } else {
                vec![0.0, 1.0]
            },
        })
        .collect()
}

/// Knows the answer: all of the budget on the acceptable option.
struct Oracle<'a> {
    steps: &'a [ReplayStep],
    prepared: Vec<usize>,
}

impl ReplayPolicy for Oracle<'_> {
    fn prepare(&mut self, train: &[usize]) -> RefusalResult<String> {
        self.prepared = train.to_vec();
        Ok(format!("oracle-{}", train.len()))
    }

    fn requests(&self, index: usize) -> RefusalResult<Option<Vec<f64>>> {
        let requests = self.steps[index]
            .utilities
            .iter()
            .map(|u| 2.0 * u)
            .collect();
        Ok(Some(requests))
    }
}

#[test]
fn walk_forward_purges_embargoes_and_never_overlaps_tests() {
    let folds = walk_forward(40, &spec(10, 5, 5, 2, 3)).unwrap();
    assert_eq!(folds[0].test, 12..17);
    assert_eq!(folds[0].train, (0..10).collect::<Vec<_>>());
    // Fold 1 tests 17..22 and trains on 5..15, minus the embargo 17..20 of
    // fold 0's test window (none of it falls inside 5..15).
    assert_eq!(folds[1].test, 17..22);
    assert_eq!(folds[1].train, (5..15).collect::<Vec<_>>());
    // Fold 2 tests 22..27, trains on 10..20 minus 17..20 (after fold 0's test).
    assert_eq!(folds[2].train, (10..17).collect::<Vec<_>>());
    for pair in folds.windows(2) {
        assert!(pair[0].test.end <= pair[1].test.start);
    }
    for fold in &folds {
        assert!(fold.train.iter().all(|&i| i + 2 < fold.test.start), "purged");
    }
}

#[test]
fn walk_forward_refuses_overlapping_or_empty_folds() {
    assert!(walk_forward(40, &spec(10, 5, 4, 0, 0)).is_err());
    assert!(walk_forward(40, &spec(0, 5, 5, 0, 0)).is_err());
    let refused = walk_forward(12, &spec(10, 5, 5, 0, 0)).unwrap_err();
    assert_eq!(refused.code, "REPLAY_SPEC_INVALID");
}

#[test]
fn proportional_allocation_respects_the_cap_and_the_ratios() {
    let applied = proportional(&[3.0, -1.0, 4.0], 2.0);
    let total: f64 = applied.iter().map(|a| a.abs()).sum();
    assert!((total - 2.0).abs() < 1e-15);
    assert!((applied[0] / applied[2] - 0.75).abs() < 1e-15);
    assert!(applied[1] < 0.0);
    assert_eq!(proportional(&[0.5, 0.25], 1.0), vec![0.5, 0.25]);
    assert_eq!(proportional(&[0.0, 0.0], 1.0), vec![0.0, 0.0]);
}

#[test]
fn drawdown_and_sharpe_on_known_paths() {
    assert_eq!(max_drawdown(&[1.0, -2.0, 0.5, -1.0, 3.0]), 2.5);
    assert_eq!(max_drawdown(&[1.0, 1.0]), 0.0);
    assert_eq!(sharpe(&[1.0, 1.0]), None);
    assert_eq!(sharpe(&[1.0]), None);
    let s = sharpe(&[1.0, 3.0]).unwrap();
    assert!((s - 2.0 / std::f64::consts::SQRT_2).abs() < 1e-15);
}

#[test]
fn the_oracle_beats_uniform_and_contributions_sum_to_the_path() {
    let data = steps(60);
    let mut oracle = Oracle {
        steps: &data,
        prepared: Vec::new(),
    };
    let mut uniform = Uniform { steps: &data };
    let outcome = replay(&data, &spec(20, 10, 10, 1, 1), 1.0, &mut oracle, &mut uniform).unwrap();
    assert_eq!(outcome.folds.len(), 3);
    let path = outcome.path();
    assert!(path.iter().all(|&u| (u - 1.0).abs() < 1e-15), "capped at 1");
    assert!(outcome.incumbent_path().iter().all(|&u| (u - 0.5).abs() < 1e-15));
    let credited: f64 = outcome.contributions.values().map(|(_, u)| u).sum();
    assert!((credited - path.iter().sum::<f64>()).abs() < 1e-12);
    assert_eq!(outcome.folds[0].head_digest, "oracle-20");
    assert_eq!(oracle.prepared, outcome.folds[2].fold.train);
}

#[test]
fn a_future_fact_in_the_training_window_is_a_look_ahead() {
    let mut data = steps(40);
    // Mutation: step 5 (inside fold 0's training window) claims to have been
    // recorded after the test window opened.
    data[5].at_ms = 1_000_000;
    let mut oracle = Oracle {
        steps: &data,
        prepared: Vec::new(),
    };
    let mut uniform = Uniform { steps: &data };
    let refused = replay(&data, &spec(10, 5, 5, 0, 0), 1.0, &mut oracle, &mut uniform).unwrap_err();
    assert_eq!(refused.code, "LOOK_AHEAD");
}

#[test]
fn replay_is_deterministic_and_refuses_a_bad_cap() {
    let data = steps(40);
    let run = || {
        let mut oracle = Oracle {
            steps: &data,
            prepared: Vec::new(),
        };
        let mut uniform = Uniform { steps: &data };
        replay(&data, &spec(10, 5, 5, 0, 0), 0.5, &mut oracle, &mut uniform)
    };
    assert_eq!(run().unwrap(), run().unwrap());
    let mut uniform = Uniform { steps: &data };
    let mut other = Uniform { steps: &data };
    assert!(replay(&data, &spec(10, 5, 5, 0, 0), 0.0, &mut uniform, &mut other).is_err());
}
