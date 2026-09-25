use super::*;

struct Fixed<'a> {
    steps: &'a [ReplayStep],
}

impl ReplayPolicy for Fixed<'_> {
    fn prepare(&mut self, _train: &[usize]) -> RefusalResult<String> {
        Ok("fixed".into())
    }

    fn requests(&self, index: usize) -> RefusalResult<Option<Vec<f64>>> {
        Ok(Some(vec![1.0; self.steps[index].option_ids.len()]))
    }
}

#[test]
fn fold_outcomes_round_trip_and_reassemble_without_recomputing_prefix() {
    let steps: Vec<_> = (0..15)
        .map(|index| ReplayStep {
            at_ms: 1_000 + index,
            option_ids: vec!["left".into(), "right".into()],
            utilities: vec![index as f64 / 10.0, 1.0 - index as f64 / 10.0],
        })
        .collect();
    let spec = WalkForward {
        train: 4,
        test: 2,
        step: 2,
        purge: 1,
        embargo: 0,
    };
    let expected = replay(
        &steps,
        &spec,
        1.0,
        &mut Fixed { steps: &steps },
        &mut Uniform { steps: &steps },
    )
    .unwrap();
    let plan = walk_forward(steps.len(), &spec).unwrap();
    let first = replay_fold(
        &steps,
        plan[0].clone(),
        1.0,
        &mut Fixed { steps: &steps },
        &mut Uniform { steps: &steps },
    )
    .unwrap();
    let encoded = serde_json::to_vec(&vec![first]).unwrap();
    let mut resumed: Vec<FoldOutcome> = serde_json::from_slice(&encoded).unwrap();
    for fold in plan.into_iter().skip(1) {
        resumed.push(
            replay_fold(
                &steps,
                fold,
                1.0,
                &mut Fixed { steps: &steps },
                &mut Uniform { steps: &steps },
            )
            .unwrap(),
        );
    }
    assert_eq!(collect_replay(resumed), expected);
}
