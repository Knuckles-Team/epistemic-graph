//! Kill points inside one admitted blob write.
//!
//! Every blob write passes the four [`MutationCommitPhase`] boundaries in
//! `complete_write`. Each boundary is the release-certification fault hook (an
//! environment-armed process abort, inert unless armed for this batch) and, in
//! tests, a thread-local injected failure: returning the error drops the
//! uncommitted write exactly as a process death before commit does, and after
//! commit it loses the acknowledgement, so a crash test can reopen the store and
//! prove each boundary leaves all or nothing and replays exactly once.

use crate::mutation_batch::{MutationBatch, MutationCommitPhase};

#[cfg(test)]
thread_local! {
    static ARMED: std::cell::Cell<Option<MutationCommitPhase>> = const { std::cell::Cell::new(None) };
}

/// Arm (or with `None`, disarm) one injected failure for this thread's next
/// blob write that reaches `phase`.
#[cfg(test)]
pub(crate) fn arm(phase: Option<MutationCommitPhase>) {
    ARMED.with(|armed| armed.set(phase));
}

/// Pass one boundary of the write committing `batch`.
pub(super) fn reach(batch: &MutationBatch, phase: MutationCommitPhase) -> Result<(), String> {
    #[cfg(test)]
    if ARMED.with(|armed| armed.get()) == Some(phase) {
        ARMED.with(|armed| armed.set(None));
        return Err(format!("injected blob crash at {phase:?}"));
    }
    crate::mutation_batch::apply_certification_fault(batch, phase)
}
