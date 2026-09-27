//! The per-query federation state: budget meter, fragment trace, and the LIMIT hints the
//! lookahead pass derived from the plan about to run.

use std::sync::{Arc, Mutex, MutexGuard};

use crate::algebra::Op;

use super::budget::{BudgetMeter, FederationBudget};
use super::cache::FragmentCacheScope;
use super::trace::FragmentTrace;

/// One foreign source op of the plan and the limit that immediately follows EVERY
/// occurrence of it (`None` when an occurrence has no `Limit` or the limits disagree —
/// the op is then fetched unlimited, which is always correct).
#[derive(Debug)]
struct Hint {
    op: Op,
    limit: Option<usize>,
}

#[derive(Debug)]
struct SessionState {
    meter: BudgetMeter,
    trace: Vec<FragmentTrace>,
    hints: Vec<Hint>,
    cache_scope: Option<Arc<FragmentCacheScope>>,
}

/// One federated query's optimizer state, bound on the `PlanCtx` with
/// [`crate::exec::PlanCtx::with_federation`]. A served query creates one per request;
/// without one, each foreign op runs under a fresh default budget and records no trace.
#[derive(Debug)]
pub struct FederationSession {
    state: Mutex<SessionState>,
}

impl FederationSession {
    /// A session spending at most `budget`.
    pub fn new(budget: FederationBudget) -> Self {
        Self {
            state: Mutex::new(SessionState {
                meter: BudgetMeter::new(budget),
                trace: Vec::new(),
                hints: Vec::new(),
                cache_scope: None,
            }),
        }
    }

    /// A session under the server budget ([`FederationBudget::from_env`]).
    pub fn from_env() -> Self {
        Self::new(FederationBudget::from_env())
    }

    /// The session is per query: a fragment that panicked mid-update already failed the
    /// query, so a poisoned lock is not recovered.
    fn lock(&self) -> MutexGuard<'_, SessionState> {
        self.state
            .lock()
            .expect("federation session lock poisoned: a fragment of this query panicked")
    }

    /// The budget this session enforces.
    pub fn budget(&self) -> FederationBudget {
        self.lock().meter.budget()
    }

    /// Attach the verified caller's fresh source checkpoints. Standalone plans
    /// retain no cache authority, even when they register a source by name.
    pub fn set_cache_scope(&self, scope: Option<Arc<FragmentCacheScope>>) {
        self.lock().cache_scope = scope;
    }

    pub(super) fn cache_scope(&self) -> Option<Arc<FragmentCacheScope>> {
        self.lock().cache_scope.clone()
    }

    /// Every fragment executed so far, in execution order.
    pub fn trace(&self) -> Vec<FragmentTrace> {
        self.lock().trace.clone()
    }

    /// Derive the LIMIT hints of `ops` (and of every nested `FuseRrf` branch): a source op
    /// (`ForeignScan { join: false }` / `Foreign`) immediately followed by `Limit k`.
    pub fn prepare(&self, ops: &[Op]) {
        let mut hints = Vec::new();
        collect_hints(ops, &mut hints);
        self.lock().hints = hints;
    }

    /// The limit that may be pushed into `op`'s source.
    pub(crate) fn limit_hint(&self, op: &Op) -> Option<usize> {
        self.lock()
            .hints
            .iter()
            .find(|h| &h.op == op)
            .and_then(|h| h.limit)
    }

    /// Run `f` against the budget meter.
    pub(crate) fn with_meter<R>(&self, f: impl FnOnce(&mut BudgetMeter) -> R) -> R {
        f(&mut self.lock().meter)
    }

    /// Append one fragment's trace.
    pub(crate) fn record(&self, fragment: FragmentTrace) {
        tracing::debug!(fragment = ?fragment, "federation fragment (EH-563)");
        self.lock().trace.push(fragment);
    }
}

/// Is `op` a foreign SOURCE op (one whose rows replace the input)?
fn is_foreign_source(op: &Op) -> bool {
    matches!(op, Op::ForeignScan { join: false, .. } | Op::Foreign { .. })
}

/// The nested op sequences of `op` (the `FuseRrf` branches), each its own plan.
fn nested_branches(op: &Op) -> &[Vec<Op>] {
    #[cfg(feature = "text")]
    if let Op::FuseRrf { branches, .. } = op {
        return branches;
    }
    let _ = op;
    &[]
}

fn collect_hints(ops: &[Op], hints: &mut Vec<Hint>) {
    for (i, op) in ops.iter().enumerate() {
        for branch in nested_branches(op) {
            collect_hints(branch, hints);
        }
        if matches!(op, Op::ForeignScan { .. } | Op::Foreign { .. }) {
            let limit = match ops.get(i + 1) {
                Some(Op::Limit { k }) if is_foreign_source(op) => Some(*k),
                _ => None,
            };
            record_occurrence(hints, op, limit);
        }
    }
}

/// Merge one occurrence: every occurrence of an op must agree on its limit.
fn record_occurrence(hints: &mut Vec<Hint>, op: &Op, limit: Option<usize>) {
    match hints.iter_mut().find(|h| &h.op == op) {
        Some(existing) if existing.limit != limit => existing.limit = None,
        Some(_) => {}
        None => hints.push(Hint {
            op: op.clone(),
            limit,
        }),
    }
}
