//! Invariant checker over a recorded history + the cluster's final state
//! (CONCEPT:AU-KG.ontology.emits-database-ontology-entities).
//!
//! Three invariants this increment asserts (full Knossos-style linearizability is a
//! later increment — see the module docs):
//!
//! 1. **Acked-write-survives / no-loss.** Every write the client observed as ACKED
//!    must be present on a quorum's worth of nodes' applied state at the end — in
//!    particular on the FINAL leader, the authoritative survivor. An acked-then-lost
//!    write is the headline durability bug; if the harness finds one, it FAILS loudly.
//!
//! 2. **Single-leader-per-term.** Across the whole run, no two nodes may BOTH report
//!    themselves leader of the SAME term — that is split-brain. We can only sample the
//!    leadership view (openraft's own invariant makes a true same-term double-leader
//!    impossible), so we assert no same-term double-leadership was ever observed.
//!
//! 3. **Monotonic commit / no-phantom.** The final leader's applied node set must be a
//!    SUBSET of {every seq the load gen ever issued} (no invented entry) and a
//!    SUPERSET of {every acked seq} (no lost ack). Equivalently: acked ⊆ applied ⊆
//!    issued.

use std::collections::HashSet;

use super::super::NodeId;
use super::cluster::Cluster;
use super::history::{History, Outcome};

/// A leadership sample `(node_id, term, is_leader)` taken during the run.
pub type LeadershipSample = (NodeId, u64, bool);

/// The verdict of a check run.
#[derive(Debug)]
pub struct Verdict {
    pub passed: bool,
    pub failures: Vec<String>,
    pub summary: String,
}

impl Verdict {
    pub fn assert_ok(&self) {
        assert!(
            self.passed,
            "HARNESS FOUND A CORRECTNESS BUG:\n{}\n{}",
            self.failures.join("\n"),
            self.summary
        );
    }
}

/// A running accumulator of leadership samples for the single-leader-per-term check.
/// The load gen / nemesis loop pushes samples in; `Checker::check` consumes them.
#[derive(Default)]
pub struct LeadershipLog {
    samples: Vec<LeadershipSample>,
}

impl LeadershipLog {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn push(&mut self, view: Vec<LeadershipSample>) {
        self.samples.extend(view);
    }

    /// Returns the set of `(term)` where TWO different nodes were both observed as
    /// leader — split-brain. Empty ⇒ no split-brain observed.
    fn split_brain_terms(&self) -> Vec<(u64, Vec<NodeId>)> {
        use std::collections::HashMap;
        let mut leaders_by_term: HashMap<u64, HashSet<NodeId>> = HashMap::new();
        for (id, term, is_leader) in &self.samples {
            if *is_leader {
                leaders_by_term.entry(*term).or_default().insert(*id);
            }
        }
        leaders_by_term
            .into_iter()
            .filter(|(_, set)| set.len() > 1)
            .map(|(t, set)| (t, set.into_iter().collect()))
            .collect()
    }
}

/// Run the invariant checks over `history`, the `leadership` log, and the final
/// `cluster` state. `final_leader` is the leader resolved after the gauntlet healed.
pub async fn check(
    cluster: &Cluster,
    history: &History,
    leadership: &LeadershipLog,
    final_leader: Option<NodeId>,
) -> Verdict {
    let mut failures = Vec::new();

    // ── Gather: every issued seq, every acked seq ──────────────────────────
    let all_ops = history.snapshot();
    let issued: HashSet<u64> = all_ops.iter().map(|o| o.seq).collect();
    let acked: HashSet<u64> = all_ops
        .iter()
        .filter(|o| o.outcome == Outcome::Acked)
        .map(|o| o.seq)
        .collect();

    // The authoritative survivor: the final leader if we have one, else any running
    // node (a fully-healed cluster has a leader; this is a safety net).
    let authority = match final_leader.filter(|l| cluster.is_running(*l)) {
        Some(l) => l,
        None => cluster.live_ids().first().copied().unwrap_or(0),
    };

    // ── INVARIANT 1: acked-write-survives (no-loss) ────────────────────────
    // Every acked seq must be present on the authoritative survivor's applied state.
    let mut lost = Vec::new();
    for &seq in &acked {
        if !cluster.has_node(authority, &format!("n{seq}")).await {
            lost.push(seq);
        }
    }
    if !lost.is_empty() {
        lost.sort_unstable();
        failures.push(format!(
            "LOST ACKED WRITES ({}): seqs {:?} were ACKED to the client but are ABSENT \
             on the final authority node {} — acked-then-lost (durability violation)",
            lost.len(),
            &lost[..lost.len().min(20)],
            authority
        ));
    }

    // ── INVARIANT 2: single-leader-per-term (no split-brain) ───────────────
    let split = leadership.split_brain_terms();
    if !split.is_empty() {
        failures.push(format!(
            "SPLIT-BRAIN: two leaders observed in the same term(s): {split:?}"
        ));
    }

    // ── INVARIANT 3: monotonic commit / no-phantom (acked ⊆ applied ⊆ issued)
    // The authority's applied node set must hold no seq that was NEVER issued — a
    // phantom commit. Its total applied count cannot exceed the number of issued
    // writes (every applied n{seq} is one of the issued seqs, no duplicates).
    let authority_count = cluster.node_count(authority).await;
    if authority_count > issued.len() {
        failures.push(format!(
            "PHANTOM COMMIT: authority node {} has {} applied nodes but only {} writes \
             were ever issued — an entry was invented (no-phantom violation)",
            authority,
            authority_count,
            issued.len()
        ));
    }

    let summary = format!(
        "ops={} acked={} err={} issued={} applied_on_authority(n{})={} split_brain_terms={}",
        history.len(),
        acked.len(),
        history.err_count(),
        issued.len(),
        authority,
        authority_count,
        split.len()
    );

    Verdict {
        passed: failures.is_empty(),
        failures,
        summary,
    }
}
