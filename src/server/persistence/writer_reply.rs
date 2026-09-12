//! Awaiting the single shard-owner writer thread's reply to one command.
//!
//! Every durable operation that cannot run on the caller's thread is expressed
//! the same way across [`super::redb_backend`] and [`super::online_reshard`]: a
//! `sync_channel(1)` reply pair is created, the sender is moved INTO the
//! [`super::redb_backend::Cmd`], the command is pushed to the shard's owner
//! thread, and the caller waits on the receiver. That pattern appeared fifteen
//! times, and fifteen times it waited with a bare `Receiver::recv()`.
//!
//! ## Why the bare `recv()` was not simply a bug
//!
//! `recv()` is a `disallowed_methods` entry because a producer that never sends
//! strands the consumer for the life of the process. Here, one half of that
//! failure mode is already impossible: the reply `SyncSender` is OWNED BY THE
//! COMMAND. If the writer thread panics, exits, or is shut down, every queued
//! command drops with it, the sender drops, and the receiver observes
//! `Disconnected` immediately rather than hanging. "The writer died" is
//! therefore already a prompt error, not a hang.
//!
//! What remains uncovered is the other half: a writer thread that is ALIVE but
//! wedged — deadlocked inside redb, or blocked on an fsync that will never
//! return. The channel stays connected, so `recv()` waits forever and the
//! caller's request never completes or fails. That is the case this module
//! turns into a named failure.
//!
//! ## Why the deadline is this large
//!
//! The bound has to sit above the slowest LEGITIMATE single command. The slowest
//! is a whole-graph `Cmd::ImportGraphRaw` or `Cmd::ExportGraphRaw` — an entire
//! graph's durable rows copied inside one transaction during an online reshard,
//! on shared spinning storage. That has no data-independent upper bound, so a
//! tight deadline would convert a merely large migration into a spurious
//! failure, which is worse than the hang it replaces.
//!
//! So the deadline is deliberately not a latency budget. It is a wedge detector:
//! generous enough that no healthy command can reach it, finite so that a
//! wedged writer surfaces as a failing request naming the command it was
//! waiting on, instead of a request that never returns.
use std::sync::mpsc::{Receiver, RecvTimeoutError};
use std::time::Duration;

/// How long a shard command may go unanswered before the writer is treated as
/// wedged rather than busy. See the module docs for why this is minutes, not
/// seconds.
pub(crate) const WRITER_REPLY_TIMEOUT: Duration = Duration::from_secs(15 * 60);

/// Wait for the shard owner thread's reply to one command.
///
/// `what` names the command, so a timeout says which durable operation stalled
/// rather than reporting a bare deadline.
pub(crate) fn await_writer_reply<T>(reply: &Receiver<T>, what: &str) -> Result<T, String> {
    // The bounded replacement the `Receiver::recv` entry asks for; see the
    // module docs for the deadline's derivation.
    match reply.recv_timeout(WRITER_REPLY_TIMEOUT) {
        Ok(value) => Ok(value),
        Err(RecvTimeoutError::Disconnected) => Err(format!("redb writer dropped the {what} reply")),
        Err(RecvTimeoutError::Timeout) => Err(format!(
            "redb writer did not answer {what} within {}s -- the shard owner thread is \
             wedged, not busy",
            WRITER_REPLY_TIMEOUT.as_secs()
        )),
    }
}
