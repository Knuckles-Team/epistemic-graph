//! Deterministic work shared by the shape and SPARQL evaluators.
use std::cell::Cell;

/// Fixed work allowance per SHACL validation call, shared across all shapes.
pub const MAX_VALIDATION_STEPS: u64 = 10_000_000;
/// Existing shape-recursion ceiling; exhaustion is a refusal, never conformance.
pub(crate) const MAX_DEPTH: usize = 40;
pub const WORK_EXCEEDED: &str = "SHACL validation exceeds the deterministic work budget";
pub const DEPTH_EXCEEDED: &str = "SHACL validation exceeds the deterministic depth budget";

/// Classify stable resource refusals without parsing arbitrary diagnostic text.
pub fn is_resource_refusal(error: &str) -> bool {
    matches!(error, WORK_EXCEEDED | DEPTH_EXCEEDED)
}

pub(crate) struct Budget {
    remaining: Cell<u64>,
    recursion: Cell<usize>,
}

impl Default for Budget {
    fn default() -> Self {
        Self::new(MAX_VALIDATION_STEPS)
    }
}

impl Budget {
    pub(crate) fn new(steps: u64) -> Self {
        Self {
            remaining: Cell::new(steps),
            recursion: Cell::new(0),
        }
    }

    pub(crate) fn charge(&self, steps: usize) -> Result<(), String> {
        let left = self
            .remaining
            .get()
            .checked_sub(steps as u64)
            .ok_or_else(|| WORK_EXCEEDED.to_string())?;
        self.remaining.set(left);
        Ok(())
    }

    pub(crate) fn enter(&self) -> Result<Frame<'_>, String> {
        Self::depth(self.recursion.get())?;
        self.charge(1)?;
        self.recursion.set(self.recursion.get() + 1);
        Ok(Frame(self))
    }

    pub(crate) fn depth(depth: usize) -> Result<(), String> {
        if depth > MAX_DEPTH {
            Err(DEPTH_EXCEEDED.to_string())
        } else {
            Ok(())
        }
    }
}

pub(crate) struct Frame<'a>(&'a Budget);
impl Drop for Frame<'_> {
    fn drop(&mut self) {
        self.0.recursion.set(self.0.recursion.get() - 1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn work_boundary_is_inclusive_and_repeatable() {
        for _ in 0..2 {
            let budget = Budget::new(3);
            assert!(budget.charge(3).is_ok());
            assert_eq!(budget.charge(1), Err(WORK_EXCEEDED.into()));
        }
    }
    #[test]
    fn recursion_guard_unwinds_on_error() {
        let budget = Budget::new(100);
        {
            let _frame = budget.enter().unwrap();
            assert_eq!(budget.recursion.get(), 1);
        }
        assert_eq!(budget.recursion.get(), 0);
        assert!(Budget::depth(MAX_DEPTH).is_ok());
        assert_eq!(Budget::depth(MAX_DEPTH + 1), Err(DEPTH_EXCEEDED.into()));
    }
}
