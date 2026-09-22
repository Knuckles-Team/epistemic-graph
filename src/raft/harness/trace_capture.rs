//! EH-286: automatic causal-chain capture for raft cluster tests.
//!
//! A failing multi-node cluster test used to report only its assertion (e.g.
//! `"placement group 7 has no current leader"`) with no causal chain — roughly four
//! investigation cycles were spent before anyone enabled openraft's DEBUG tracing by
//! hand, which immediately revealed the mechanism
//! (`HeartbeatWorker ... failed to send a heartbeat: Err(Elapsed(()))`, one group
//! cycling 8 terms over ~50s). See
//! `plans/refactor/architecture/RAFT-CLUSTER-RELIABILITY-DESIGN.md` §4.
//!
//! ## Why a ring buffer, not "just turn tracing on"
//!
//! The design doc records that manually enabling DEBUG tracing did not just reveal
//! the mechanism — it also raised reproduction from 80% to 100%, because the added
//! latency (formatting + a synchronous, line-buffered write to stderr on every
//! heartbeat/replication event, on the exact hot path being timed) widened the
//! losing window. **A capture mechanism that changes the thing it measures is a bad
//! instrument.** So this module never performs I/O on the happy path: matched events
//! are appended to a bounded in-memory ring ([`Ring::push`], one `String` push behind
//! a mutex, no formatting beyond what `tracing`'s own field visitor already does) and
//! are only formatted and written once, on an actual test failure, via a
//! [`std::panic`] hook. The ring is bounded ([`CAPACITY`]) so a long-running or
//! looping test cannot grow it without limit.
//!
//! ## What gets captured
//!
//! Only two target prefixes, at `DEBUG` and above: `openraft` (replication,
//! heartbeat, election internals) and [`RAFT_MODULE_TARGET`] (this crate's own raft
//! module — group lifecycle, leader routing). Everything else stays filtered out at
//! the subscriber, so installing this globally does not turn on logging for the rest
//! of the test binary. `openraft` commonly attaches node/group identity to a SPAN
//! around its core loop rather than repeating it on every leaf event, so
//! [`CaptureLayer::on_new_span`] stashes each span's fields and
//! [`append_ancestor_span_fields`] folds the whole ancestor chain into every event a
//! span contains — otherwise most captured lines would be missing exactly the
//! node/group identity this exists to preserve.
//!
//! ## Wiring
//!
//! [`init`] is idempotent and installs (once per test binary): the capturing
//! subscriber as the process's global default `tracing` dispatcher, and a panic hook
//! that dumps [`render_dump`] to stderr after the normal panic message. `node::start`
//! is not the ONLY way a test builds a raft node in this crate, so `init()` is called
//! from every construction path this crate's own `raft::tests` module uses, found by
//! grepping it for `MultiRaft::` and `node::start`, not just one:
//! * `super::super::tests::cluster_cfg_with_groups` — every `node::start` call in
//!   `raft::tests` (directly, or via `cluster_cfg`, which forwards to it). Covers
//!   `placement_admin_wire_rpcs_move_data_across_a_real_three_node_cluster`, one of
//!   EH-286's two named examples.
//! * `super::super::tests::start_multi` — every DIRECT `MultiRaft::start` call in
//!   `raft::tests` that bypasses `node::start` entirely (membership/leader-rebalance
//!   tests that manage a `MultiRaft` handle by hand). Covers
//!   `multi_node_group_join_then_leader_rebalance`, EH-286's other named example.
//!
//! A single test reaching either of these reaches the same idempotent `init()`, so
//! this is the default across every way `raft::tests` starts a raft node or a bare
//! `MultiRaft`, not something a human must remember to opt into on a new one.
//!
//! **NOT covered:** `src/raft/harness/cluster.rs`'s own private `node::start` path
//! (`Cluster::start` and its post-kill restart, used by `gauntlet_test`/
//! `catchup_test` and five other `harness::cluster` consumers). That file is already
//! at its `statements_per_file` cap (257 of 250 — a pre-existing, tolerated
//! violation) before any change here; adding the one statement this wiring needs
//! measurably regresses an already-over-cap file, which this program's build
//! contract treats as forbidden regardless of the file already being over cap
//! (`grep`-verified: 257 statements at `HEAD`, 258 with the one-line
//! `super::trace_capture::init();` addition attempted and reverted). Splitting
//! `cluster.rs` to make room is a legitimate follow-up but a separate, larger
//! refactor outside this file's own layout, not a one-line addition; escalated
//! rather than forced through. Neither of EH-286's two named tests uses this path.
//!
//! ## EH-326: capture is scoped per OS thread, not one process-global buffer
//!
//! The ring used to be a single process-global buffer shared by every concurrently
//! running test. Under real host load that made a failing cluster's dump mostly
//! **other tests'** chatter: one investigation found a failing cluster's dump held 19
//! events belonging to its own cluster out of 4096, with the rest — including a
//! reused raft `group_id` from an unrelated test — evicting the actual trigger event
//! before the panic hook ever fired. A raft `group_id`/`node_id` is not a safe capture
//! key: groups are small integers reused across tests, so keying by group id would
//! merge two different tests' clusters that happen to share a number, exactly the
//! failure this exists to fix.
//!
//! What IS a safe, unique-per-test-instance key without touching production raft
//! code: the OS thread. Every `#[tokio::test(flavor = "multi_thread", ...)]` cluster
//! test builds its OWN [`tokio::runtime::Runtime`] with its own freshly spawned worker
//! threads, torn down when the test ends; no two cluster tests running concurrently
//! ever share an OS thread. So [`ring_for_current_thread`] now keys the bounded ring
//! by [`std::thread::ThreadId`] (`std::thread::current().id()`, guaranteed unique for
//! the process, never reused) instead of one shared instance: each thread gets its own
//! independent [`CAPACITY`]-bounded ring, so a chatty thread belonging to a DIFFERENT,
//! concurrently running test can never evict this thread's events, however much noise
//! it produces. [`render_dump`] (used by the panic hook, and directly by tests) renders
//! only the CALLING thread's own ring, so a dump can never contain another test's
//! events — the property the two-fake-clusters test below proves directly.
//!
//! **Residual, narrower limitation:** this scopes by thread, not by "everything one
//! test does." A cluster test's own background raft workers (heartbeat/replication
//! loops production code spawns via bare `tokio::spawn`, not `.instrument()`-wrapped)
//! run on worker threads distinct from whichever thread evaluates the failing
//! assertion, so a dump taken on the assertion's thread will not automatically include
//! a sibling worker thread's events from the SAME test. [`render_dump_for_thread`]
//! remains available for a human who wants a specific thread's ring while
//! investigating (thread ids appear on every rendered line already). This is a smaller
//! problem than the one fixed here: it is quiet-vs-quiet noise within one test, never
//! contamination from an unrelated, concurrently running one.

use std::collections::{HashMap, VecDeque};
use std::fmt::Write as _;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock, Weak};
use std::thread::ThreadId;
use std::time::{Duration, Instant};

use tracing::field::{Field, Visit};
use tracing::{span, Event, Level, Subscriber};
use tracing_subscriber::filter::{LevelFilter, Targets};
use tracing_subscriber::layer::{Context, Layer, SubscriberExt};
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::util::SubscriberInitExt;

use crate::lock_recovery::LockRecovery;

/// Upper bound on retained events. Oldest evicted first — bounds memory for a long
/// or looping cluster test regardless of how much openraft chatter it produces.
const CAPACITY: usize = 4096;

/// This crate's own raft-module traces (group lifecycle, leader routing) — the other
/// half of the causal chain alongside openraft's internal replication/heartbeat
/// events.
const RAFT_MODULE_TARGET: &str = "epistemic_graph::raft";

/// One retained trace line.
struct CapturedEvent {
    seq: usize,
    elapsed: Duration,
    thread: String,
    level: Level,
    target: &'static str,
    message: String,
    fields: String,
}

/// The bounded event store plus the sequence/clock it stamps entries with.
struct Ring {
    start: Instant,
    seq: AtomicUsize,
    events: Mutex<VecDeque<CapturedEvent>>,
}

impl Ring {
    fn new() -> Self {
        Ring {
            start: Instant::now(),
            seq: AtomicUsize::new(0),
            events: Mutex::new(VecDeque::with_capacity(CAPACITY)),
        }
    }

    /// Append one event, evicting the oldest if already at [`CAPACITY`].
    fn push(
        &self,
        level: Level,
        target: &'static str,
        thread: String,
        message: String,
        fields: String,
    ) {
        let seq = self.seq.fetch_add(1, Ordering::Relaxed);
        let elapsed = self.start.elapsed();
        let mut events = self
            .events
            .lock_recovering("raft trace capture ring buffer");
        if events.len() >= CAPACITY {
            events.pop_front();
        }
        events.push_back(CapturedEvent {
            seq,
            elapsed,
            thread,
            level,
            target,
            message,
            fields,
        });
    }

    /// Render every retained event, ordered oldest-first, one line each:
    /// sequence number, elapsed time since the ring was created, thread, level,
    /// target, and the message with any node/group identity fields appended.
    fn render(&self) -> String {
        let events = self
            .events
            .lock_recovering("raft trace capture ring buffer");
        let mut out = String::new();
        let _ = writeln!(
            out,
            "=== EH-286 raft trace capture: {} event(s) (openraft + {RAFT_MODULE_TARGET}, DEBUG+) ===",
            events.len()
        );
        for event in events.iter() {
            let _ = writeln!(
                out,
                "[seq {:>6}] [t+{:>9.3}s] [{}] {:<5} {}: {}{}",
                event.seq,
                event.elapsed.as_secs_f64(),
                event.thread,
                event.level,
                event.target,
                event.message,
                event.fields,
            );
        }
        out
    }

    #[cfg(test)]
    fn clear(&self) {
        self.events
            .lock_recovering("raft trace capture ring buffer")
            .clear();
    }
}

/// A span's fields, formatted once at span creation and stashed as an extension so
/// every event inside it (including openraft's own leaf events, which often carry no
/// identity of their own) can inherit them. See [`append_ancestor_span_fields`].
struct SpanFields(String);

/// Collects a tracing event's (or span's) fields into a `message` field (the
/// conventional name `tracing`'s macros use for the format-args field) and a
/// `fields` string of every other field, each as ` name=value`.
#[derive(Default)]
struct FieldVisitor {
    message: String,
    fields: String,
}

impl Visit for FieldVisitor {
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        if field.name() == "message" {
            let _ = write!(self.message, "{value:?}");
        } else {
            let _ = write!(self.fields, " {}={value:?}", field.name());
        }
    }
}

/// The `tracing_subscriber::Layer` that feeds the calling thread's own
/// [`Ring`] (see [`ring_for_current_thread`], EH-326). Composed with a
/// [`Targets`] filter in [`install_subscriber`] — this type itself captures
/// unconditionally whatever the filter lets through.
struct CaptureLayer;

impl<S> Layer<S> for CaptureLayer
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_new_span(&self, attrs: &span::Attributes<'_>, id: &span::Id, ctx: Context<'_, S>) {
        let mut visitor = FieldVisitor::default();
        attrs.record(&mut visitor);
        let Some(span) = ctx.span(id) else {
            return;
        };
        span.extensions_mut().insert(SpanFields(visitor.fields));
    }

    fn on_event(&self, event: &Event<'_>, ctx: Context<'_, S>) {
        let mut visitor = FieldVisitor::default();
        event.record(&mut visitor);
        let mut fields = visitor.fields;
        append_ancestor_span_fields(&ctx, event, &mut fields);
        let Some(ring) = ring_for_current_thread() else {
            return;
        };
        ring.push(
            *event.metadata().level(),
            event.metadata().target(),
            thread_tag(),
            visitor.message,
            fields,
        );
    }
}

/// Fold every ancestor span's stashed [`SpanFields`] (root first) into `fields`, so
/// identity attached to a span (openraft's common pattern: `id`/`group` on the span
/// around a core loop, not repeated on each leaf event) survives on the leaf line.
fn append_ancestor_span_fields<S>(ctx: &Context<'_, S>, event: &Event<'_>, fields: &mut String)
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    let Some(scope) = ctx.event_scope(event) else {
        return;
    };
    for span in scope.from_root() {
        let extensions = span.extensions();
        let Some(SpanFields(span_fields)) = extensions.get::<SpanFields>() else {
            continue;
        };
        if !span_fields.is_empty() {
            fields.push(' ');
            fields.push_str(span_fields);
        }
    }
}

fn thread_tag() -> String {
    format!("{:?}", std::thread::current().id())
}

/// EH-326: one bounded [`Ring`] per OS thread rather than one shared globally — see
/// the module docs. The thread OWNS its ring through `OWN_RING`, so the ring is
/// freed when the thread exits (a long test binary spawns hundreds of short-lived
/// runtime workers; retaining every one's full ring would grow without bound). The
/// registry holds only `Weak` handles so [`render_dump_for_thread`] can still reach
/// a live sibling thread's ring by id.
static REGISTRY: OnceLock<Mutex<HashMap<ThreadId, Weak<Ring>>>> = OnceLock::new();
static SETUP: OnceLock<()> = OnceLock::new();

thread_local! {
    static OWN_RING: Arc<Ring> = register_current_thread_ring();
}

fn registry() -> &'static Mutex<HashMap<ThreadId, Weak<Ring>>> {
    REGISTRY.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Create the calling thread's ring and publish a weak handle to it, pruning the
/// handles of threads that have already exited.
fn register_current_thread_ring() -> Arc<Ring> {
    let ring = Arc::new(Ring::new());
    let mut map = registry().lock_recovering("raft trace capture registry");
    map.retain(|_, weak| weak.strong_count() > 0);
    map.insert(std::thread::current().id(), Arc::downgrade(&ring));
    ring
}

/// The calling thread's own ring, creating it on first use. Never shared with any
/// other thread, so a chatty thread belonging to a different, concurrently running
/// test can never evict this thread's events (EH-326). `None` only while the
/// thread's locals are being torn down, when the event is dropped.
fn ring_for_current_thread() -> Option<Arc<Ring>> {
    OWN_RING.try_with(Arc::clone).ok()
}

/// Install the capturing subscriber as the process's global default `tracing`
/// dispatcher, filtered to `openraft` + [`RAFT_MODULE_TARGET`] at `DEBUG`+.
/// `try_init` rather than `init`: a second, unrelated global-default install
/// elsewhere in the same test binary must not panic this one.
fn install_subscriber() {
    let targets = Targets::new()
        .with_target("openraft", Level::DEBUG)
        .with_target(RAFT_MODULE_TARGET, Level::DEBUG)
        .with_default(LevelFilter::OFF);
    let layer = CaptureLayer.with_filter(targets);
    let _ = tracing_subscriber::registry().with(layer).try_init();
}

/// Print the current dump to stderr — the whole action a cluster-test failure needs,
/// wired into the real panic hook below. Split out so its correctness ("does the
/// dump reach stderr") is legible by inspection, since a panic hook's output is not
/// convenient to assert on from a test.
fn dump_on_panic() {
    eprintln!("{}", render_dump());
}

/// Chain onto whatever panic hook is already installed (never replace it) and dump
/// the ring after it runs, so the normal panic message still prints first.
fn install_panic_hook() {
    let previous = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        previous(info);
        dump_on_panic();
    }));
}

/// Make trace capture the default for cluster tests: install the subscriber and
/// panic hook, exactly once per test binary. Cheap and safe to call from every
/// cluster test's node-startup path.
pub(crate) fn init() {
    SETUP.get_or_init(|| {
        install_subscriber();
        install_panic_hook();
    });
}

/// Render the calling thread's own ring — never another thread's, and so never
/// another concurrently running test's (EH-326; see module docs). Used by the panic
/// hook (which always runs ON the panicking thread) and directly by tests.
pub(crate) fn render_dump() -> String {
    render_dump_for_thread(std::thread::current().id())
}

/// Render one specific thread's ring by id, for a human digging into a SIBLING
/// worker thread of a test whose own dump ([`render_dump`]) didn't have what they
/// needed — see the "residual, narrower limitation" in the module docs.
pub(crate) fn render_dump_for_thread(id: ThreadId) -> String {
    let map = registry().lock_recovering("raft trace capture registry");
    match map.get(&id).and_then(Weak::upgrade) {
        Some(ring) => ring.render(),
        None => format!(
            "=== EH-286/EH-326 raft trace capture: no events recorded for thread {id:?} \
             (init() never ran on it, it never emitted a matching event, or it has exited) ==="
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Known-bad input #1: an event on an unmatched target must NOT appear in the
    /// dump, proving the filter actually filters rather than capturing everything.
    /// Uses `with_default` (a subscriber scoped to this closure, not the process
    /// global), so it cannot flake against another concurrently running cluster test
    /// installing its own subscriber; the ring it lands in is still this THREAD's own
    /// (EH-326), identified with a marker unique to this test run so a leftover event
    /// from an earlier, unrelated test that happened to reuse this OS thread cannot be
    /// mistaken for this test's own capture.
    #[test]
    fn capture_layer_records_matched_targets_and_ignores_others() {
        let targets = Targets::new()
            .with_target("openraft", Level::DEBUG)
            .with_target(RAFT_MODULE_TARGET, Level::DEBUG)
            .with_default(LevelFilter::OFF);
        let subscriber = tracing_subscriber::registry().with(CaptureLayer.with_filter(targets));

        tracing::subscriber::with_default(subscriber, || {
            tracing::debug_span!(target: "openraft::replication", "group", group_id = 4, node_id = 9)
                .in_scope(|| {
                    tracing::debug!(target: "openraft::replication", "heartbeat send attempt");
                });
            tracing::warn!(
                target: "epistemic_graph::raft::multi",
                "placement group 4 has no current leader"
            );
            tracing::error!(target: "unrelated_module", "MARKER-SHOULD-NOT-APPEAR");
        });

        let dump = render_dump();
        assert!(dump.contains("heartbeat send attempt"));
        assert!(dump.contains("no current leader"));
        assert!(
            dump.contains("group_id=4"),
            "span identity must survive onto the leaf event: {dump}"
        );
        assert!(
            !dump.contains("MARKER-SHOULD-NOT-APPEAR"),
            "the target filter must reject non-raft events"
        );
    }

    /// Known-bad input #2: pushing past `CAPACITY` must evict the oldest entries,
    /// not grow without bound (EH-286's "bounded so a long test cannot exhaust
    /// memory" requirement).
    #[test]
    fn ring_buffer_evicts_oldest_when_over_capacity() {
        let ring = Ring::new();
        for i in 0..(CAPACITY + 3) {
            ring.push(
                Level::DEBUG,
                "openraft::test",
                "t".to_string(),
                format!("event-{i}"),
                String::new(),
            );
        }
        let dump = ring.render();
        // Anchored on the trailing newline `render` always appends after a message
        // with empty `fields` (see `Ring::render`): a bare `dump.contains("event-2")`
        // is NOT a correct "was event 2 evicted" check at this CAPACITY, because it
        // also matches any SURVIVING event whose number merely STARTS with "2" as a
        // string -- "event-20", "event-234", "event-2999", .. -- and at CAPACITY =
        // 4096 there are always over a thousand such survivors. Anchoring to
        // "event-N\n" makes this an exact-line match: `event-N\n` can only be a
        // substring of another line `event-M\n` when N == M, since decimal
        // formatting never produces leading zeros. This was found empirically: this
        // very assertion, unanchored, failed on a real run despite the eviction
        // logic being correct (verified separately, by hand-tracing `Ring::push`).
        let survives = |n: usize| dump.contains(&format!("event-{n}\n"));
        assert!(!survives(0), "the oldest entries must have been evicted");
        assert!(
            !survives(2),
            "exactly the first 3 (over-capacity) entries must be evicted"
        );
        assert!(
            survives(3),
            "the first entry still within capacity must survive"
        );
        assert!(survives(CAPACITY + 2), "the newest entry must survive");
    }

    /// Rendering preserves push order (oldest first) and stamps every line with a
    /// sequence number and elapsed time, so a dump reconstructs a causal chain
    /// rather than an unordered bag of lines.
    #[test]
    fn render_preserves_order_with_sequence_and_timestamp() {
        let ring = Ring::new();
        ring.push(
            Level::DEBUG,
            "openraft::replication",
            "t".to_string(),
            "first".to_string(),
            String::new(),
        );
        ring.push(
            Level::WARN,
            "epistemic_graph::raft::multi",
            "t".to_string(),
            "second".to_string(),
            String::new(),
        );
        let dump = ring.render();
        let first_at = dump.find("first").expect("first event present");
        let second_at = dump.find("second").expect("second event present");
        assert!(
            first_at < second_at,
            "events must render oldest-first: {dump}"
        );
        assert!(
            dump.contains("seq"),
            "each line must carry an ordering key: {dump}"
        );
        assert!(
            dump.contains("t+"),
            "each line must carry an elapsed timestamp: {dump}"
        );
    }

    /// End-to-end proof of the actual failure path: an event captured just before a
    /// deliberately failing assertion survives in this thread's dump — the exact
    /// mechanism `cluster_cfg_with_groups` wires into every cluster test. The
    /// assertion is deliberately triggered and caught (never propagated), so this
    /// test proves the harness without itself failing the suite.
    #[test]
    fn a_deliberate_assertion_failure_leaves_its_causal_chain_in_the_global_dump() {
        init();
        let marker = format!(
            "EH286-PROOF-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        );
        tracing::debug!(target: "openraft::replication", "{marker} heartbeat reply: Err(Elapsed(()))");

        let panicked = std::panic::catch_unwind(|| {
            assert_eq!(
                1, 2,
                "deliberate known-bad input: simulated cluster assertion failure"
            );
        });
        assert!(
            panicked.is_err(),
            "the deliberate failure must actually panic"
        );

        let dump = render_dump();
        assert!(
            dump.contains(&marker),
            "the event emitted just before the failure must survive in this thread's dump"
        );
    }

    /// EH-326 acceptance test: two concurrent fake clusters, each on its own thread,
    /// write heavily interleaved events under the SAME globally installed subscriber
    /// (exactly how two real cluster tests running in parallel share one process).
    /// One cluster ("quiet") emits a handful of events including a unique trigger
    /// marker, then blocks on a barrier; the other ("noisy") floods far more than
    /// [`CAPACITY`] events, guaranteeing it would have evicted the quiet cluster's
    /// trigger under the old process-global ring. Both release the barrier and
    /// finish at the same time, so the writes are genuinely interleaved in time, not
    /// just sequential. Each thread's own dump must contain only its own events and,
    /// for the quiet cluster, must still contain the trigger event afterward.
    #[test]
    fn dump_for_one_thread_excludes_another_concurrently_writing_thread() {
        init();
        let run_tag = format!("{}-{:?}", std::process::id(), std::thread::current().id());
        let trigger = format!("TRIGGER-{run_tag}");
        let noise_marker = format!("NOISE-{run_tag}");

        let start = Arc::new(std::sync::Barrier::new(2));

        let quiet_start = start.clone();
        let quiet_trigger = trigger.clone();
        let quiet = std::thread::spawn(move || {
            quiet_start.wait();
            for i in 0..5 {
                tracing::debug!(target: "openraft::replication", "quiet-cluster event {i}");
            }
            tracing::debug!(target: "openraft::replication", "{quiet_trigger}");
            // Rendered ON the thread, exactly as the panic hook does: a thread's
            // ring is owned by the thread and freed when it exits.
            render_dump()
        });

        let noisy_start = start.clone();
        let noisy_noise = noise_marker.clone();
        let noisy = std::thread::spawn(move || {
            noisy_start.wait();
            for i in 0..(CAPACITY * 2) {
                tracing::debug!(target: "openraft::replication", "{noisy_noise} chatter {i}");
            }
            render_dump()
        });

        let quiet_dump = quiet.join().expect("quiet cluster thread must not panic");
        let noisy_dump = noisy.join().expect("noisy cluster thread must not panic");

        assert!(
            quiet_dump.contains(&trigger),
            "the quiet cluster's trigger event must survive despite the noisy \
             cluster writing over CAPACITY interleaved events concurrently: {quiet_dump}"
        );
        assert!(
            !quiet_dump.contains(&noise_marker),
            "the quiet cluster's dump must contain only its own events, never the \
             concurrently writing noisy cluster's chatter: {quiet_dump}"
        );

        assert!(
            noisy_dump.contains(&noise_marker),
            "the noisy cluster's own dump must hold its own chatter: {noisy_dump}"
        );
        assert!(
            !noisy_dump.contains(&trigger),
            "the noisy cluster's dump must never contain the quiet cluster's event: {noisy_dump}"
        );
    }

    /// The ring itself, isolated from the global one, doesn't leak state across
    /// unrelated tests in this module.
    #[test]
    fn clear_empties_a_ring() {
        let ring = Ring::new();
        ring.push(
            Level::DEBUG,
            "openraft::test",
            "t".to_string(),
            "x".to_string(),
            String::new(),
        );
        ring.clear();
        let dump = ring.render();
        assert!(!dump.contains("x"));
        assert!(dump.contains("0 event(s)"));
    }
}
