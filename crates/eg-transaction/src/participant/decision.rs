//! Monotonic transitions for the existing cross-shard decision encoding.

/// Merge a decision inside the owner's write transaction, before replacing its row.
/// Encodings are abort (0), commit (1), retained abort (2), retained commit (3),
/// and retained pending (4). Explicit collection is a separate operation.
pub fn merge_coordinator_decision(current: Option<u8>, requested: u8) -> Result<u8, String> {
    if requested > 4 || current.is_some_and(|value| value > 4) {
        return Err("corrupt cross-shard decision value".to_string());
    }
    let Some(current) = current else {
        return Ok(requested);
    };
    if current == 4 {
        return Ok(if requested == 4 { 4 } else { requested % 2 + 2 });
    }
    if requested == 4 || current % 2 != requested % 2 {
        return Err("cross-shard decision conflicts with durable outcome".to_string());
    }
    // A retry cannot discard the parent recovery obligation.
    Ok(if current >= 2 || requested >= 2 {
        requested % 2 + 2
    } else {
        requested
    })
}

#[cfg(test)]
mod tests {
    use super::merge_coordinator_decision;

    #[test]
    fn decision_transition_matrix_preserves_terminal_outcome_and_retention() {
        let expected = [
            [Some(0), Some(1), Some(2), Some(3), Some(4)],
            [Some(0), None, Some(2), None, None],
            [None, Some(1), None, Some(3), None],
            [Some(2), None, Some(2), None, None],
            [None, Some(3), None, Some(3), None],
            [Some(2), Some(3), Some(2), Some(3), Some(4)],
        ];
        for (row, current) in [None, Some(0), Some(1), Some(2), Some(3), Some(4)]
            .into_iter()
            .enumerate()
        {
            for requested in 0..=4 {
                assert_eq!(
                    merge_coordinator_decision(current, requested).ok(),
                    expected[row][requested as usize],
                    "current={current:?}, requested={requested}"
                );
            }
        }
    }

    #[test]
    fn unknown_decision_bytes_never_authorize_replacement() {
        for invalid in 5..=u8::MAX {
            for valid in 0..=4 {
                assert!(merge_coordinator_decision(Some(invalid), valid).is_err());
                assert!(merge_coordinator_decision(Some(valid), invalid).is_err());
            }
            assert!(merge_coordinator_decision(None, invalid).is_err());
            assert!(merge_coordinator_decision(Some(invalid), invalid).is_err());
        }
    }
}
