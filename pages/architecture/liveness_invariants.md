# Liveness and lock-recovery invariants

`clippy.toml` bans unbounded waits (`Barrier::wait`, `JoinHandle::join`,
`Receiver::recv`), unbounded queues, and silent lock-poison recovery
(`PoisonError::into_inner`). The lint catches the method call. It cannot see
why a particular call is safe. Each local `#[allow(clippy::disallowed_methods)]`
in this workspace applies one of the named invariants below, and its comment
cites the invariant by anchor.

A new allow is legitimate only when the site is one of these invariant classes
and its comment names the class. Anything else gets fixed: bound the wait with
`recv_timeout`, `test_rendezvous::{meet, join_bounded, recv_within}`,
`bounded_join::join_within`, or `persistence::writer_reply::await_writer_reply`,
and decide about poisoning with `lock_recovery`.

## helper-confined-wait

**Invariant.** The unbounded call runs only on a throwaway helper thread. The
caller's deadline is a `recv_timeout` on a completion signal the helper sends
once the unbounded call returns. A join that happens after that signal has
arrived collects a finished thread, so it cannot block.

**Why it needs an allow.** `std` has no deadline-taking `Barrier::wait` or
`JoinHandle::join`, so the bounded helper must call the unbounded method
itself. These sites implement the replacement that the lint directs callers to.

**Sites.**
- `src/test_rendezvous.rs`: `meet`, `join_bounded` (three allows).
- `src/bounded_join.rs`: `join_within` (two allows).
- `src/performance_probe/wire.rs`: `join_probe_worker` (two allows). This
  copy is local because the server binary cannot reach the library's
  `pub(crate)` `bounded_join`.

## process-lifetime-join

**Invariant.** The joined thread is meant to run for the whole life of the
process, so the join returns when the process is asked to stop. There is no
concurrent party that might fail to arrive. Any deadline would mean "stop
serving after N seconds".

**Sites.**
- `src/server/mod.rs`: `join_engine_driver`, the engine runtime driver thread.

## idle-worker-receive

**Invariant.** A long-lived worker blocks in `recv` as its idle state, not while
waiting for a reply. "No command yet" is normal. The worker's only termination
condition is every sender dropping, and `recv` reports that promptly as `Err`.
A `recv_timeout` inside `while let Ok(..)` would make the worker exit the first
time its owner paused, losing the state it owns.

**Sites.**
- `crates/eg-query/src/sql/spill.rs`: the spill worker's command loop (the
  worker owns the spill file writer).

## deadline-on-the-operation

**Invariant.** The joined thread runs one future under `tokio::time::timeout`,
so it always finishes within that deadline plus teardown, and the join is
prompt by construction. Putting the deadline on the join instead would abandon
a live runtime thread that still holds the operation's resources, and it would
report a bare timeout instead of the operation's own error.

**Sites.**
- `crates/eg-query/src/sql/iceberg_federation.rs`: `block_on_iceberg`
  (`ICEBERG_FEDERATION_TIMEOUT` bounds the catalog or table future).

## enclosing-deadline

**Invariant.** The unbounded wait runs entirely inside an enclosing deadline
that turns a missed party into a test failure. A second deadline inside the
wait could fire only after the outer one had already failed the test.

**Sites.**
- `src/server/persistence/redb_backend.rs`: the concurrent shard fan-out
  barrier test, driven under `tokio::time::timeout(10s)`.

## panic-fixture-join

**Invariant.** In a test, the joined thread panics unconditionally and has
nothing between `spawn` and `panic!` that could block. The panic is the
fixture itself, for example the way a mutex gets poisoned. `join_bounded`
cannot be used because it re-raises the worker's panic.

**Sites.**
- `src/lock_recovery.rs`: `an_authority_lock_refuses_after_a_holder_panics`.

## reporting-recovery

**Invariant.** Poison recovery is allowed only when the guarded state can be
rebuilt, or re-derived, from its source, and the recovery is reported at the
point where it happens. The lint bans silent recovery. The report is what
turns recovery into a decision.

**Sites.**
- `src/lock_recovery.rs`: the `Mutex` and `RwLock` read `lock_recovering`
  and `write_recovering`, the reporting helpers the lint directs callers to
  (three allows).
- `crates/eg-wasm/src/lib.rs`: the UDF-registry mutex. The registry can be
  rebuilt, and the report goes to stderr.
- `crates/eg-plan/src/exec.rs`: the derived-tensor CAS mutex. The CAS is
  content-addressed and re-derivable.
- `crates/eg-plan/src/learned_cost.rs`: `recover_corrections`, the learned
  cost-correction curves. They are advisory and can be re-learned, since an
  untrained curve is the identity.

`eg-wasm` and `eg-plan` sit below the root crate in the dependency DAG, so they
cannot call `lock_recovery`. They report locally instead.

## Not an invariant: sites that were fixed instead

A wait that has a natural deadline in its own contract is bounded, not excused:

- `eg-tts-piper`'s `ChunkStream` bounds both `next` and `Drop` by the request's
  `request_deadline_ms`. It detects producer exit through a drop-guard signal
  instead of a join. It also disconnects its receiver before waiting, so a
  producer blocked on a full channel is released rather than deadlocked.
- The raft harness root allocator hands the root over as a value that removes
  itself unless it is claimed. The allocating thread no longer blocks waiting
  for the caller to confirm receipt.
- Concurrency tests in crates below the root use `eg-core`'s `test_threads`, and
  `eg-query`/`eg-transaction` use local bounded channels. Workers report
  outcomes over a bounded channel collected with `recv_timeout`, so nothing
  joins.
- Test-serialization locks that guard `()` are handled by crate. The root crate
  takes them with `lock_recovery`. Crates below it use `parking_lot`'s
  non-poisoning mutex, because a `()` token has no state for a panicking holder
  to break.
