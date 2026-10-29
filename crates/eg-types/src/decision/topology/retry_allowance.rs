//! Local retry allowance transition; durable consumption must be enforced by the caller.

/// What a denied acquisition grants next: exactly one further decision.
/// Copies are independent model values, not durable authorization tokens.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetryAllowance {
    pub further_decisions: u8,
}

/// A retry was already consumed, or the supplied allowance is malformed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetryAllowanceError {
    Exhausted,
    InvalidCount(u8),
}

impl RetryAllowance {
    /// Consume the sole retry. Refusals leave the model unchanged.
    pub fn consume(&mut self) -> Result<(), RetryAllowanceError> {
        match self.further_decisions {
            1 => {
                self.further_decisions = 0;
                Ok(())
            }
            0 => Err(RetryAllowanceError::Exhausted),
            count => Err(RetryAllowanceError::InvalidCount(count)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // spec: MEDULLA-DECISION-ENGINE-R113.3.1
    #[test]
    fn retry_allowance_consumes_once() {
        let mut allowance = RetryAllowance {
            further_decisions: 1,
        };
        assert_eq!(allowance.consume(), Ok(()));
        assert_eq!(allowance.further_decisions, 0);
        for _ in 0..3 {
            assert_eq!(allowance.consume(), Err(RetryAllowanceError::Exhausted));
            assert_eq!(allowance.further_decisions, 0);
        }
    }

    // spec: MEDULLA-DECISION-ENGINE-R113.3.1
    #[test]
    fn invalid_allowances_fail_without_mutation() {
        for count in 2..=u8::MAX {
            let mut allowance = RetryAllowance {
                further_decisions: count,
            };
            assert_eq!(
                allowance.consume(),
                Err(RetryAllowanceError::InvalidCount(count))
            );
            assert_eq!(allowance.further_decisions, count);
        }
        let mut empty = RetryAllowance {
            further_decisions: 0,
        };
        assert_eq!(empty.consume(), Err(RetryAllowanceError::Exhausted));
        assert_eq!(empty.further_decisions, 0);
    }
}
