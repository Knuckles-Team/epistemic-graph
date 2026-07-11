//! CONCEPT:EG-KG.epistemic.tms — X2: a paraconsistent, justification-based TMS +
//! Dung-style abstract argumentation semantics layered over the [`crate::BeliefGraph`]
//! attack/support topology.
//!
//! Opt-in (`epistemic-tms` feature, default OFF — this is the heavier, NP-hard-in-the-
//! worst-case sibling of the always-on, polynomial [`crate::propagate`] core). Where
//! `propagate_confidence` answers "how confident should we be in `id`, as a number in
//! `[0, 1]`", this module answers the qualitative question underneath it: "is `id`
//! *justified* at all, given the attacks against it — and if the graph is genuinely
//! contradictory, what is the principled (non-explosive) answer?"
//!
//! ## Argumentation semantics
//! Standard Dung abstract argumentation framework (AF) semantics — grounded (skeptical),
//! preferred and stable (credulous) extensions — computed over the augmented attack
//! relation defined below. Citation: P. M. Dung, *"On the acceptability of arguments and
//! its fundamental role in nonmonotonic reasoning, logic programming and n-person
//! games"*, Artificial Intelligence 77(2), 1995, 321-357.
//!
//! - **Grounded** = the unique minimal complete extension = the least fixpoint of the
//!   characteristic function `F(S) = { a : every attacker of a is attacked by S }`,
//!   computed here via the standard IN/OUT/UNDECIDED labelling iteration (an argument
//!   is IN once all its attackers are OUT; OUT once any attacker is IN; anything left
//!   over stays UNDECIDED). This is the *skeptical* answer: only what cannot be
//!   rationally denied.
//! - **Preferred** = the maximal admissible sets (admissible = conflict-free +
//!   every member defended). *Credulous*: a belief may be acceptable under one
//!   reasonable "side" without being acceptable under all of them.
//! - **Stable** = conflict-free sets that attack every argument outside themselves. A
//!   strictly stronger, sometimes-empty refinement of preferred.
//!
//! ## Bipolar handling — chosen semantics: DEDUCTIVE "supported attack" closure
//! [`crate::BeliefGraph`] is a *bipolar* argumentation graph: besides `Attacks` (and
//! `Contradicts`, treated identically here — see [`is_base_attack`]) it also carries
//! `Supports` edges. Dung's semantics are only defined over a single attack relation, so
//! bipolar support must be folded into one before grounded/preferred/stable apply.
//!
//! We use the **deductive** reading from Cayrol & Lagasquie-Schiex, *"On the
//! acceptability of arguments in bipolar argumentation frameworks"*, ECSQARU 2005: `a
//! Supports b` is read as "accepting `a` is itself a reason to accept `b`" — exactly the
//! reading [`crate::propagate::propagate_confidence`] already committed to (a
//! supporting neighbour's belief is fed into the target's posterior as evidence *for*
//! it, not merely as a precondition). Under that reading, Cayrol/Lagasquie-Schiex derive
//! the **supported attack**: if `s` supports `x` (directly, or transitively through a
//! chain of supports) and `x` attacks `b`, then `s` *supported-attacks* `b` — accepting
//! `s` is enough to license accepting `x`, which is already known to defeat `b`, so `s`
//! defeats `b` too. [`augmented_attackers`] computes exactly this closure.
//!
//! We deliberately do **not** add their dual "secondary attack" (attacking `a`'s
//! supporter merely because something attacks `a`) — that rule is licensed by the
//! opposite, *necessity* reading of support ("`b`'s acceptance requires `a`'s"), which
//! contradicts the deductive reading above, and the two are known to jointly break
//! conflict-freeness (an argument can end up supported-attacking its own supporter) in
//! cyclic BAFs. Keeping a single direction (deductive / supported-attack only) keeps the
//! augmented relation one well-defined, finite Dung AF, so the unmodified
//! grounded/preferred/stable machinery below applies to it directly.
//!
//! ## Complexity & caps
//! - [`grounded_extension`]: the label fixpoint is `O(V·E)` (at most `V` outer passes,
//!   each rescanning every argument's attacker set) and always terminates — no cap.
//! - [`preferred_extensions`] / [`stable_extensions`]: subset-maximal conflict-free-set
//!   enumeration is NP-hard in general (up to `2^V` candidate sets). We run a
//!   branch-and-bound labelling search (prune the moment an "include" branch conflicts
//!   with the set built so far) bounded by [`MAX_SEARCH_NODES`] recursion nodes, and skip
//!   the search entirely above [`MAX_PREFERRED_ARGUMENTS`] arguments. Either bound firing
//!   is `tracing::warn!`-logged — never a silent truncation of a result the caller would
//!   otherwise assume was exhaustive.
//!
//! ## Paraconsistency
//! Because grounded acceptance requires an argument's attackers to be provably OUT (not
//! merely "not IN"), a symmetric conflict — a claim and a direct contradiction of it,
//! with no other evidence either way — leaves BOTH arguments UNDECIDED under grounded:
//! neither is accepted, neither is rejected, and nothing else in the graph is affected.
//! This is the standard non-explosive behaviour of Dung semantics and is exercised by
//! [`tests::paraconsistency_no_explosion`].

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

use crate::adapter::BeliefGraph;
use crate::model::{AuthorityPolicy, EdgeKind, JustificationGraph};
use crate::propagate::explain_belief;

/// Above this many arguments, [`preferred_extensions`]/[`stable_extensions`] skip the
/// branch-and-bound search outright (even the pruned search's *shape* — not just its
/// worst case — grows with argument count) and log instead of guessing. See module docs
/// "Complexity & caps".
pub const MAX_PREFERRED_ARGUMENTS: usize = 24;

/// Branch-node budget for the admissible/stable-set search below
/// [`MAX_PREFERRED_ARGUMENTS`] arguments — belt-and-braces against a pathologically
/// under-constrained (few attacks, many arguments) AF where the pruned search still
/// approaches `2^V`.
pub const MAX_SEARCH_NODES: usize = 200_000;

/// Whether an [`EdgeKind`] counts as a base (pre-bipolar-closure) attack. Mirrors
/// [`crate::model::EdgeKind`]'s own doc ("a contradiction or attack lowers the target's
/// belief") by treating `Contradicts` and `Attacks` as the same attack relation for
/// argumentation purposes; only their propagation *weight* differs (`AuthorityPolicy`),
/// which is irrelevant to the qualitative accept/reject question this module answers.
fn is_base_attack(kind: EdgeKind) -> bool {
    matches!(kind, EdgeKind::Attacks | EdgeKind::Contradicts)
}

/// Every argument (node id) appearing anywhere in the graph — as a prior, as an edge
/// target, or as an edge source. `BTreeSet` keeps iteration order deterministic, which
/// keeps the (already NP-hard) search below reproducible across runs.
pub fn arguments(bg: &BeliefGraph) -> BTreeSet<String> {
    let mut args: BTreeSet<String> = bg.priors.keys().cloned().collect();
    for (target, edges) in &bg.in_edges {
        args.insert(target.clone());
        for (src, _) in edges {
            args.insert(src.clone());
        }
    }
    args
}

/// The transitive closure of `Supports` edges reaching `of`: every node that supports
/// `of` directly, or supports a supporter of `of`, and so on. Cycle-guarded (support
/// chains, like attack chains, are not guaranteed acyclic) via `seen`.
fn transitive_supporters(bg: &BeliefGraph, of: &str) -> BTreeSet<String> {
    let mut out = BTreeSet::new();
    let mut seen: HashSet<String> = HashSet::new();
    seen.insert(of.to_string());
    let mut stack = vec![of.to_string()];
    while let Some(cur) = stack.pop() {
        if let Some(edges) = bg.in_edges.get(&cur) {
            for (src, kind) in edges {
                if *kind == EdgeKind::Supports && seen.insert(src.clone()) {
                    out.insert(src.clone());
                    stack.push(src.clone());
                }
            }
        }
    }
    out
}

/// The augmented (bipolar-closed) attacker set of `target`: its direct `Attacks`/
/// `Contradicts` attackers, plus every transitive supporter of each such attacker (the
/// "supported attack" closure — see module docs). This is the ONE attack relation the
/// grounded/preferred/stable computations below run over.
pub fn augmented_attackers(bg: &BeliefGraph, target: &str) -> BTreeSet<String> {
    let mut result = BTreeSet::new();
    if let Some(edges) = bg.in_edges.get(target) {
        for (src, kind) in edges {
            if is_base_attack(*kind) {
                result.insert(src.clone());
                result.extend(transitive_supporters(bg, src));
            }
        }
    }
    result
}

/// Precompute the augmented attacker set for every argument once, so the (potentially
/// exponential) extension searches below never recompute [`augmented_attackers`] inside
/// a hot inner loop.
fn attacker_map(bg: &BeliefGraph) -> AttackerMap {
    arguments(bg)
        .into_iter()
        .map(|a| {
            let attackers = augmented_attackers(bg, &a);
            (a.clone(), attackers)
        })
        .collect()
}

/// argument id -> its augmented (bipolar-closed) attacker set. The one attack relation
/// every extension computation below runs over.
type AttackerMap = BTreeMap<String, BTreeSet<String>>;

fn attacks(attackers: &AttackerMap, s: &str, t: &str) -> bool {
    attackers.get(t).map(|a| a.contains(s)).unwrap_or(false)
}

/// IN/OUT/UNDECIDED — the standard Dung/Caminada grounded-labelling states. `UNDECIDED`
/// is not a failure mode: it is the correct, paraconsistent answer for arguments caught
/// in an unresolved (even-length, or otherwise self-reinstating) attack cycle.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Label {
    In,
    Out,
    Undecided,
}

/// The grounded labelling: iterate the characteristic function to its least fixpoint.
/// An argument is IN once every one of its attackers is (already known to be) OUT; OUT
/// once any attacker is IN; everything left over when no more labels change is
/// UNDECIDED. Monotonic (labels only ever move UNDECIDED -> IN/OUT, never back), so this
/// always terminates in at most `|arguments|` outer passes.
fn grounded_labels(bg: &BeliefGraph) -> BTreeMap<String, Label> {
    let attackers = attacker_map(bg);
    let mut labels: BTreeMap<String, Label> = attackers
        .keys()
        .map(|a| (a.clone(), Label::Undecided))
        .collect();
    loop {
        let mut changed = false;
        for (arg, atk) in &attackers {
            if labels[arg] != Label::Undecided {
                continue;
            }
            if atk.iter().all(|a| labels.get(a) == Some(&Label::Out)) {
                labels.insert(arg.clone(), Label::In);
                changed = true;
            } else if atk.iter().any(|a| labels.get(a) == Some(&Label::In)) {
                labels.insert(arg.clone(), Label::Out);
                changed = true;
            }
        }
        if !changed {
            return labels;
        }
    }
}

/// The unique minimal complete extension (Dung 1995) — the skeptical answer: an
/// argument is here iff it cannot be rationally denied, no matter which admissible
/// "side" of an unresolved conflict a reasoner picks. Always exists, always unique,
/// `O(V·E)`.
pub fn grounded_extension(bg: &BeliefGraph) -> BTreeSet<String> {
    grounded_labels(bg)
        .into_iter()
        .filter(|(_, l)| *l == Label::In)
        .map(|(k, _)| k)
        .collect()
}

fn conflict_free(attackers: &AttackerMap, set: &BTreeSet<String>) -> bool {
    set.iter()
        .all(|a| set.iter().all(|b| !attacks(attackers, a, b)))
}

/// `arg` is defended by `set` iff every attacker of `arg` is itself attacked by some
/// member of `set`.
fn is_defended(attackers: &AttackerMap, set: &BTreeSet<String>, arg: &str) -> bool {
    match attackers.get(arg) {
        Some(atk) => atk.iter().all(|attacker| {
            attackers
                .get(attacker)
                .is_some_and(|aa| aa.iter().any(|x| set.contains(x)))
        }),
        None => true,
    }
}

fn is_admissible(attackers: &AttackerMap, set: &BTreeSet<String>) -> bool {
    conflict_free(attackers, set) && set.iter().all(|a| is_defended(attackers, set, a))
}

/// Branch-and-bound enumeration of conflict-free subsets of `args`, pruning the
/// "include" branch the instant it would conflict with the set built so far. Still
/// worst-case exponential (a fully unattacked AF has `2^V` conflict-free subsets), so it
/// is wrapped in a node budget by both callers ([`preferred_extensions`],
/// [`stable_extensions`]).
fn search_conflict_free(
    attackers: &AttackerMap,
    args: &[String],
    idx: usize,
    current: BTreeSet<String>,
    found: &mut Vec<BTreeSet<String>>,
    budget: &mut usize,
    capped: &mut bool,
) {
    if *budget == 0 {
        *capped = true;
        return;
    }
    *budget -= 1;

    if idx == args.len() {
        found.push(current);
        return;
    }

    // Exclude args[idx].
    search_conflict_free(
        attackers,
        args,
        idx + 1,
        current.clone(),
        found,
        budget,
        capped,
    );
    if *capped {
        return;
    }

    // Include args[idx], only down branches that stay conflict-free.
    let a = &args[idx];
    let conflicts = current
        .iter()
        .any(|b| attacks(attackers, a, b) || attacks(attackers, b, a));
    if !conflicts {
        let mut next = current;
        next.insert(a.clone());
        search_conflict_free(attackers, args, idx + 1, next, found, budget, capped);
    }
}

/// Run the shared conflict-free search once, logging (never silently) if either the
/// argument-count bound or the branch-node budget fires.
fn bounded_conflict_free_sets(
    bg: &BeliefGraph,
    caller: &str,
) -> Option<(AttackerMap, Vec<BTreeSet<String>>)> {
    let attackers = attacker_map(bg);
    let args: Vec<String> = attackers.keys().cloned().collect();
    let n = args.len();
    if n > MAX_PREFERRED_ARGUMENTS {
        tracing::warn!(
            caller,
            arguments = n,
            bound = MAX_PREFERRED_ARGUMENTS,
            "eg-epistemic tms: skipping full admissible-set search ({n} arguments > \
             {MAX_PREFERRED_ARGUMENTS}); caller should fall back to grounded_extension"
        );
        return None;
    }

    let mut found = Vec::new();
    let mut budget = MAX_SEARCH_NODES;
    let mut capped = false;
    search_conflict_free(
        &attackers,
        &args,
        0,
        BTreeSet::new(),
        &mut found,
        &mut budget,
        &mut capped,
    );
    if capped {
        tracing::warn!(
            caller,
            arguments = n,
            budget = MAX_SEARCH_NODES,
            "eg-epistemic tms: admissible-set search hit its {MAX_SEARCH_NODES}-node \
             branch budget; results may be incomplete (not exhaustive)"
        );
    }
    Some((attackers, found))
}

/// Maximal admissible sets (Dung 1995) — the credulous answer: each is a self-consistent
/// "side" a reasoner could rationally commit to. See module docs for the search bound.
/// Returns `[grounded_extension(bg)]` (a single, safe, always-admissible answer) if the
/// argument-count bound fires; see the logged warning in that case.
pub fn preferred_extensions(bg: &BeliefGraph) -> Vec<BTreeSet<String>> {
    let Some((attackers, found)) = bounded_conflict_free_sets(bg, "preferred_extensions") else {
        return vec![grounded_extension(bg)];
    };

    let mut admissible: Vec<BTreeSet<String>> = found
        .into_iter()
        .filter(|s| is_admissible(&attackers, s))
        .collect();
    // Largest-first so the maximality scan below only ever compares a candidate against
    // sets that are at least as large (a strict superset must be at least as large).
    admissible.sort_by_key(|s| std::cmp::Reverse(s.len()));

    let mut maximal: Vec<BTreeSet<String>> = Vec::new();
    for cand in admissible {
        if !maximal.iter().any(|m| cand.is_subset(m)) {
            maximal.push(cand);
        }
    }
    maximal
}

/// Conflict-free sets that attack every argument outside themselves (Dung 1995) — a
/// strictly stronger notion than preferred; may legitimately be empty (e.g. an odd
/// attack cycle has no stable extension at all). See module docs for the search bound.
/// Returns `[]` (not a guess) if the argument-count bound fires; see the logged warning.
pub fn stable_extensions(bg: &BeliefGraph) -> Vec<BTreeSet<String>> {
    let Some((attackers, found)) = bounded_conflict_free_sets(bg, "stable_extensions") else {
        return Vec::new();
    };
    let args: Vec<&String> = attackers.keys().collect();

    found
        .into_iter()
        .filter(|s| {
            args.iter().all(|a| {
                s.contains(a.as_str())
                    || attackers
                        .get(a.as_str())
                        .is_some_and(|atk| atk.iter().any(|x| s.contains(x)))
            })
        })
        .collect()
}

/// Skeptical acceptance: `id` is in the (unique) grounded extension.
pub fn is_skeptically_accepted(bg: &BeliefGraph, id: &str) -> bool {
    grounded_extension(bg).contains(id)
}

/// Credulous acceptance: `id` is in at least one preferred extension. Since the grounded
/// extension is always a subset of every preferred extension (Dung 1995, Thm. 25: the
/// grounded extension is the intersection of all complete extensions, and every
/// preferred extension is complete), skeptical acceptance always implies credulous
/// acceptance — see [`tests::skeptical_subset_of_credulous`].
pub fn is_credulously_accepted(bg: &BeliefGraph, id: &str) -> bool {
    preferred_extensions(bg).iter().any(|e| e.contains(id))
}

/// The result of withdrawing an evidence node: which beliefs lost grounded acceptance
/// (the justification cascade), which gained it (removing an attacker's own attacker
/// can reinstate something else — dependency-directed retraction is not monotonic
/// removal), and the pre-retraction justification tree for each casualty so a caller can
/// show *why* it depended on the withdrawn evidence.
#[derive(Clone, Debug)]
pub struct RetractionResult {
    pub evidence_id: String,
    /// Argument ids that were grounded-accepted before retraction and are not anymore.
    pub retracted: Vec<String>,
    /// Argument ids that were NOT grounded-accepted before but became so after (their
    /// defeater's own supporting justification depended on the withdrawn evidence).
    pub reinstated: Vec<String>,
    /// `explain_belief`'s pre-retraction justification proof-tree for each retracted id
    /// (computed with the default [`AuthorityPolicy`]) — the cascade's "why".
    pub justifications: BTreeMap<String, JustificationGraph>,
}

/// A copy of `bg` with `id` (and every edge to/from it) removed.
fn remove_node(bg: &BeliefGraph, id: &str) -> BeliefGraph {
    remove_nodes(bg, std::iter::once(id))
}

/// A copy of `bg` with every id in `ids` (and every edge to/from any of them) removed —
/// the multi-node generalization [`remove_node`] delegates to, and the primitive
/// [`crate::query::what_evidence_would_change_this`] (EPI-P3-5) uses to test whether
/// retracting a WHOLE candidate evidence set (not just one node at a time) flips a
/// claim's grounded acceptance.
pub(crate) fn remove_nodes<'a>(
    bg: &BeliefGraph,
    ids: impl IntoIterator<Item = &'a str>,
) -> BeliefGraph {
    let removed: HashSet<&str> = ids.into_iter().collect();

    let mut priors = bg.priors.clone();
    priors.retain(|k, _| !removed.contains(k.as_str()));

    let mut in_edges: HashMap<String, Vec<(String, EdgeKind)>> = HashMap::new();
    for (target, edges) in &bg.in_edges {
        if removed.contains(target.as_str()) {
            continue;
        }
        let filtered: Vec<(String, EdgeKind)> = edges
            .iter()
            .filter(|(src, _)| !removed.contains(src.as_str()))
            .cloned()
            .collect();
        if !filtered.is_empty() {
            in_edges.insert(target.clone(), filtered);
        }
    }

    #[cfg(feature = "epistemic-redaction")]
    let node_visibility = {
        let mut v = bg.node_visibility.clone();
        v.retain(|k, _| !removed.contains(k.as_str()));
        v
    };
    let mut temporal = bg.temporal.clone();
    temporal.retain(|k, _| !removed.contains(k.as_str()));

    BeliefGraph {
        priors,
        in_edges,
        as_of: bg.as_of,
        #[cfg(feature = "epistemic-redaction")]
        node_visibility,
        temporal,
        bitemporal_pin: bg.bitemporal_pin,
    }
}

/// Dependency-directed retraction: withdraw evidence node `evidence_id` and recompute
/// the grounded-acceptance cascade. Reuses [`explain_belief`]'s justification structure
/// (computed against the ORIGINAL graph, before retraction) to explain each casualty.
pub fn retract(bg: &BeliefGraph, evidence_id: &str) -> RetractionResult {
    let before = grounded_labels(bg);
    let bg_after = remove_node(bg, evidence_id);
    let after = grounded_labels(&bg_after);

    let all_ids: BTreeSet<String> = before.keys().chain(after.keys()).cloned().collect();
    let mut retracted = Vec::new();
    let mut reinstated = Vec::new();
    for id in &all_ids {
        // The withdrawn evidence node itself is gone from `after` by construction — that
        // is the retraction, not a cascade casualty of it.
        if id == evidence_id {
            continue;
        }
        let was_in = before.get(id) == Some(&Label::In);
        let is_in = after.get(id) == Some(&Label::In);
        if was_in && !is_in {
            retracted.push(id.clone());
        } else if !was_in && is_in {
            reinstated.push(id.clone());
        }
    }

    let policy = AuthorityPolicy::default();
    let justifications = retracted
        .iter()
        .map(|id| (id.clone(), explain_belief(bg, id, &policy)))
        .collect();

    RetractionResult {
        evidence_id: evidence_id.to_string(),
        retracted,
        reinstated,
        justifications,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- grounded extension -------------------------------------------------------

    // A 3-cycle a->b->c->a (all Attacks) has no unattacked argument to bootstrap the
    // fixpoint from: grounded is empty, every argument stays UNDECIDED. This is the
    // textbook odd-cycle example (Dung 1995, Example 24-ish).
    #[test]
    fn grounded_three_cycle_all_undecided() {
        let bg = BeliefGraph::from_parts(
            [("a", 0.5), ("b", 0.5), ("c", 0.5)],
            [
                ("a", "b", EdgeKind::Attacks),
                ("b", "c", EdgeKind::Attacks),
                ("c", "a", EdgeKind::Attacks),
            ],
        );
        assert!(
            grounded_extension(&bg).is_empty(),
            "3-cycle must yield an empty (all-UNDECIDED) grounded extension"
        );
    }

    // a attacks b attacks c: a is unattacked (IN), which knocks out b (OUT), which
    // reinstates c (IN). Grounded = {a, c}.
    #[test]
    fn grounded_chain_reinstatement() {
        let bg = BeliefGraph::from_parts(
            [("a", 0.5), ("b", 0.5), ("c", 0.5)],
            [("a", "b", EdgeKind::Attacks), ("b", "c", EdgeKind::Attacks)],
        );
        let grounded = grounded_extension(&bg);
        let expected: BTreeSet<String> = ["a", "c"].into_iter().map(String::from).collect();
        assert_eq!(grounded, expected);
    }

    // --- preferred / stable enumeration on a textbook AF ---------------------------
    //
    // AF: a <-> b (mutual attack, symmetric conflict), c unattacked/unattacking.
    // Grounded = {c} (only c can be bootstrapped). Preferred = {a,c} and {b,c} (the two
    // maximal ways to resolve the a/b conflict). Both happen to also be stable here
    // (each attacks the one argument it excludes).
    fn mutual_conflict_af() -> BeliefGraph {
        BeliefGraph::from_parts(
            [("a", 0.5), ("b", 0.5), ("c", 0.9)],
            [("a", "b", EdgeKind::Attacks), ("b", "a", EdgeKind::Attacks)],
        )
    }

    #[test]
    fn preferred_extensions_textbook_af() {
        let bg = mutual_conflict_af();
        let mut preferred = preferred_extensions(&bg);
        preferred.sort_by_key(|s| s.iter().cloned().collect::<Vec<_>>());

        let ac: BTreeSet<String> = ["a", "c"].into_iter().map(String::from).collect();
        let bc: BTreeSet<String> = ["b", "c"].into_iter().map(String::from).collect();
        assert_eq!(preferred, vec![ac, bc]);
    }

    #[test]
    fn stable_extensions_textbook_af() {
        let bg = mutual_conflict_af();
        let mut stable = stable_extensions(&bg);
        stable.sort_by_key(|s| s.iter().cloned().collect::<Vec<_>>());

        let ac: BTreeSet<String> = ["a", "c"].into_iter().map(String::from).collect();
        let bc: BTreeSet<String> = ["b", "c"].into_iter().map(String::from).collect();
        assert_eq!(stable, vec![ac, bc]);
    }

    // --- skeptical ⊆ credulous ------------------------------------------------------

    #[test]
    fn skeptical_subset_of_credulous() {
        let bg = mutual_conflict_af();
        let grounded = grounded_extension(&bg);
        for id in &grounded {
            assert!(
                is_skeptically_accepted(&bg, id),
                "{id} should be skeptically accepted"
            );
            assert!(
                is_credulously_accepted(&bg, id),
                "grounded member {id} must also be credulously accepted"
            );
        }
        // a is credulous-only: accepted in {a,c}, not in the (empty) grounded set.
        assert!(!is_skeptically_accepted(&bg, "a"));
        assert!(is_credulously_accepted(&bg, "a"));
    }

    // --- paraconsistency -------------------------------------------------------------

    // A claim and its DIRECT contradiction, symmetric, no other evidence either way:
    // grounded semantics must not explode ("everything derivable") — both stay
    // UNDECIDED, and nothing else in the graph is affected. No panic, no crash.
    #[test]
    fn paraconsistency_no_explosion() {
        let bg = BeliefGraph::from_parts(
            [("p", 0.6), ("not_p", 0.6), ("unrelated", 0.7)],
            [
                ("not_p", "p", EdgeKind::Contradicts),
                ("p", "not_p", EdgeKind::Contradicts),
            ],
        );
        let grounded = grounded_extension(&bg);
        assert!(
            !grounded.contains("p") && !grounded.contains("not_p"),
            "a claim and its direct contradiction must both be UNDECIDED, got {grounded:?}"
        );
        // Not "all-accepted": credulous acceptance is exactly the two consistent sides,
        // never both simultaneously in the SAME extension (conflict-freeness holds).
        for ext in preferred_extensions(&bg) {
            assert!(
                !(ext.contains("p") && ext.contains("not_p")),
                "no admissible set may contain a claim and its direct contradiction together"
            );
        }
        // The unrelated argument is untouched by the contradiction.
        assert!(grounded.contains("unrelated"));
    }

    // --- retraction cascade -----------------------------------------------------------

    // ev attacks x, x attacks c: ev is unattacked (IN) -> defeats x (OUT) -> reinstates c
    // (IN). Withdraw ev: x is now unattacked (IN) -> c is defeated (OUT). c is the
    // retraction casualty; x is reinstated.
    #[test]
    fn retract_cascades_through_justification() {
        let bg = BeliefGraph::from_parts(
            [("ev", 0.9), ("x", 0.5), ("c", 0.5)],
            [
                ("ev", "x", EdgeKind::Attacks),
                ("x", "c", EdgeKind::Attacks),
            ],
        );
        // Sanity: c is grounded-accepted before retraction, thanks to ev.
        assert!(grounded_extension(&bg).contains("c"));

        let result = retract(&bg, "ev");
        assert_eq!(result.evidence_id, "ev");
        assert_eq!(result.retracted, vec!["c".to_string()]);
        assert_eq!(result.reinstated, vec!["x".to_string()]);

        let just = result
            .justifications
            .get("c")
            .expect("retracted id must carry a pre-retraction justification");
        assert_eq!(just.root.claim, "c");
        assert!(
            !just.root.premises.is_empty(),
            "c's justification must reference the premise (x) whose own defeat depended on ev"
        );
    }
}
