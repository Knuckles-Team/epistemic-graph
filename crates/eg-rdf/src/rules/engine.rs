//! Forward-chaining fact engine and confidence/equality propagation.

use std::collections::{BTreeSet, HashMap};

use super::builtins::{eval_builtin, swrl_builtin_name};
use super::syntax::{is_same_as, pred_matches};
use super::{Atom, RTerm, Rule, RuleSet};
use crate::owl::Ontology;

// ── The fact base + forward-chaining engine ──────────────────────────────────

/// A ground fact key: `(predicate, canonical-args)`.
type FactKey = (String, Vec<String>);

/// The forward-chaining engine state: the ground-fact base, per-fact confidence, the
/// `owl:sameAs` union-find, and the derivation bookkeeping.
#[derive(Default)]
struct Engine {
    /// pred → set of canonical argument tuples.
    by_pred: HashMap<String, BTreeSet<Vec<String>>>,
    /// per-fact confidence in `[0,1]`.
    conf: HashMap<FactKey, f64>,
    /// facts that were DERIVED (not asserted) — for the result's `derived` set.
    derived: BTreeSet<FactKey>,
    /// the rule label that first derived each fact.
    rule_of: HashMap<FactKey, String>,
    /// union-find parent map for `owl:sameAs` equality.
    uf: HashMap<String, String>,
    /// derived/asserted `sameAs` pairs (canonical reps).
    same_pairs: BTreeSet<(String, String)>,
    /// asserted `owl:differentFrom` pairs.
    diff_pairs: Vec<(String, String)>,
    /// instance-level clashes (a `sameAs` over a `differentFrom` pair).
    conflicts: Vec<String>,
}

/// The rest of a body-atom walk that [`Engine::eval_at`] threads unchanged through its
/// per-atom helpers: the full rule body + current position, and the solutions
/// accumulator. Bundled into one struct so those helpers stay under this workspace's
/// clippy::too_many_arguments cap (`conf_acc`/`binding` vary per attempt, so they stay
/// as separate arguments rather than living here).
struct Walk<'a, 'b> {
    body: &'a [Atom],
    idx: usize,
    out: &'b mut Vec<(HashMap<String, String>, f64)>,
}

impl Engine {
    /// Equality representative of `x` (read-only union-find find, no compression).
    fn rep(&self, x: &str) -> String {
        let mut cur = x.to_string();
        let mut guard = 0;
        while let Some(p) = self.uf.get(&cur) {
            if p == &cur || guard > 10_000 {
                break;
            }
            cur = p.clone();
            guard += 1;
        }
        cur
    }

    /// Union two individuals under `owl:sameAs`. Returns whether anything merged. The
    /// lexicographically smaller id is kept as the representative (stable output).
    fn union(&mut self, a: &str, b: &str) -> bool {
        let (ra, rb) = (self.rep(a), self.rep(b));
        if ra == rb {
            return false;
        }
        let (root, child) = if ra <= rb { (ra, rb) } else { (rb, ra) };
        self.uf.insert(child.clone(), root.clone());
        self.same_pairs.insert((root, child));
        true
    }

    /// Add (or raise the confidence of) a ground fact. Returns whether membership was
    /// added OR the confidence rose. `derived` marks an inferred (vs asserted) fact.
    fn add_fact(
        &mut self,
        pred: &str,
        args: &[String],
        conf: f64,
        label: &str,
        derived: bool,
    ) -> bool {
        let cargs: Vec<String> = args.iter().map(|a| self.rep(a)).collect();
        let key = (pred.to_string(), cargs.clone());
        let added = self
            .by_pred
            .entry(pred.to_string())
            .or_default()
            .insert(cargs);
        let prev = self.conf.get(&key).copied().unwrap_or(0.0);
        let combined = prev.max(conf.clamp(0.0, 1.0));
        let raised = combined > prev + 1e-9;
        if added || raised {
            self.conf.insert(key.clone(), combined);
        }
        if added {
            if derived {
                self.derived.insert(key.clone());
            }
            self.rule_of.entry(key).or_insert_with(|| label.to_string());
        }
        added || raised
    }

    /// Re-canonicalise every fact through the current union-find (congruence closure):
    /// facts of now-equal individuals collapse onto their representative, MAX-merging
    /// confidence. Run after a round that performed any union so subsequent rounds see
    /// the merged ABox.
    fn canonicalize(&mut self) {
        let mut new_by_pred: HashMap<String, BTreeSet<Vec<String>>> = HashMap::new();
        let mut new_conf: HashMap<FactKey, f64> = HashMap::new();
        let mut new_derived: BTreeSet<FactKey> = BTreeSet::new();
        let mut new_rule_of: HashMap<FactKey, String> = HashMap::new();
        for (pred, tuples) in &self.by_pred {
            for t in tuples {
                let ct: Vec<String> = t.iter().map(|a| self.rep(a)).collect();
                let old_key = (pred.clone(), t.clone());
                let new_key = (pred.clone(), ct.clone());
                new_by_pred.entry(pred.clone()).or_default().insert(ct);
                let c = self.conf.get(&old_key).copied().unwrap_or(1.0);
                let slot = new_conf.entry(new_key.clone()).or_insert(0.0);
                if c > *slot {
                    *slot = c;
                }
                if self.derived.contains(&old_key) {
                    new_derived.insert(new_key.clone());
                }
                if let Some(lbl) = self.rule_of.get(&old_key) {
                    new_rule_of.entry(new_key).or_insert_with(|| lbl.clone());
                }
            }
        }
        self.by_pred = new_by_pred;
        self.conf = new_conf;
        self.derived = new_derived;
        self.rule_of = new_rule_of;
    }

    /// Detect clashes: a `differentFrom` pair forced equal is an instance inconsistency.
    fn check_conflicts(&mut self) {
        self.conflicts.clear();
        for (a, b) in &self.diff_pairs {
            if self.rep(a) == self.rep(b) {
                self.conflicts.push(format!(
                    "owl:sameAs derived over owl:differentFrom pair: {a} = {b}"
                ));
            }
        }
    }

    /// Evaluate a rule body, producing every satisfying `(binding, body-confidence)`.
    fn eval_body(&self, body: &[Atom]) -> Vec<(HashMap<String, String>, f64)> {
        let mut out = Vec::new();
        self.eval_at(body, 0, &mut HashMap::new(), 1.0, &mut out);
        out
    }

    fn eval_at(
        &self,
        body: &[Atom],
        idx: usize,
        binding: &mut HashMap<String, String>,
        conf_acc: f64,
        out: &mut Vec<(HashMap<String, String>, f64)>,
    ) {
        if idx == body.len() {
            out.push((binding.clone(), conf_acc));
            return;
        }
        let atom = &body[idx];
        let mut walk = Walk { body, idx, out };
        if let Some(bn) = swrl_builtin_name(&atom.pred) {
            eval_builtin_atom(self, bn, atom, binding, conf_acc, &mut walk);
            return;
        }
        eval_fact_atom(self, atom, binding, conf_acc, &mut walk);
    }

    /// Apply one rule, deriving its head facts; returns whether anything changed.
    fn apply_rule(&mut self, rule: &Rule) -> bool {
        let mut changed = false;
        let solutions = self.eval_body(&rule.body);
        for (binding, body_conf) in solutions {
            let conf = (rule.conf * body_conf).clamp(0.0, 1.0);
            for head in &rule.head {
                if self.apply_head_atom(head, &binding, conf, &rule.name) {
                    changed = true;
                }
            }
        }
        changed
    }

    /// Instantiate + assert one rule-head atom under `binding`; returns whether it
    /// changed the fact base. A `sameAs` head unions its two args; anything else is
    /// asserted as a derived fact. Extracted from [`Engine::apply_rule`]'s inner loop.
    fn apply_head_atom(
        &mut self,
        head: &Atom,
        binding: &HashMap<String, String>,
        conf: f64,
        rule_name: &str,
    ) -> bool {
        let Some(args) = self.instantiate_head_args(head, binding) else {
            return false;
        };
        if is_same_as(&head.pred) && args.len() == 2 {
            self.union(&args[0], &args[1])
        } else {
            self.add_fact(&head.pred, &args, conf, rule_name, true)
        }
    }

    /// Resolve every head-atom argument against `binding`: a const to its canonical
    /// representative, a var to its bound value. `None` means an unsafe rule slipped
    /// past [`check_range_restricted`] (a head var with no body binding). Extracted from
    /// [`Engine::apply_rule`]'s inner loop.
    fn instantiate_head_args(
        &self,
        head: &Atom,
        binding: &HashMap<String, String>,
    ) -> Option<Vec<String>> {
        let mut args = Vec::with_capacity(head.args.len());
        for t in &head.args {
            match t {
                RTerm::Const(c) => args.push(self.rep(c)),
                RTerm::Var(v) => args.push(binding.get(v)?.clone()),
            }
        }
        Some(args)
    }

    /// Run all rules to a fixpoint with congruence re-canonicalisation between rounds.
    fn run(&mut self, rules: &[Rule]) {
        let mut guard = 0;
        loop {
            guard += 1;
            if guard > 10_000 {
                break;
            }
            let mut changed = false;
            let pre_unions = self.same_pairs.len();
            for rule in rules {
                if self.apply_rule(rule) {
                    changed = true;
                }
            }
            if self.same_pairs.len() != pre_unions {
                self.canonicalize();
                changed = true;
            }
            if !changed {
                break;
            }
        }
        self.check_conflicts();
    }
}

/// Evaluate one SWRL built-in body atom against the current binding.
fn eval_builtin_atom(
    engine: &Engine,
    bn: &str,
    atom: &Atom,
    binding: &mut HashMap<String, String>,
    conf_acc: f64,
    walk: &mut Walk,
) {
    if let Some(extra) = eval_builtin(bn, &atom.args, binding) {
        let mut newly_bound: Vec<String> = Vec::new();
        for (v, val) in extra {
            binding.insert(v.clone(), val);
            newly_bound.push(v);
        }
        engine.eval_at(walk.body, walk.idx + 1, binding, conf_acc, walk.out);
        for v in newly_bound {
            binding.remove(&v);
        }
    }
}

/// Match one body atom against every stored fact tuple it can consume.
fn eval_fact_atom(
    engine: &Engine,
    atom: &Atom,
    binding: &mut HashMap<String, String>,
    conf_acc: f64,
    walk: &mut Walk,
) {
    let matched: Vec<&String> = engine
        .by_pred
        .keys()
        .filter(|fp| pred_matches(&atom.pred, fp))
        .collect();
    for fp in matched {
        let Some(tuples) = engine.by_pred.get(fp) else {
            continue;
        };
        for tuple in tuples {
            try_bind_tuple(engine, atom, fp, tuple, binding, conf_acc, walk);
        }
    }
}

/// Bind one candidate tuple and recurse into the rest of the rule body.
fn try_bind_tuple(
    engine: &Engine,
    atom: &Atom,
    fp: &str,
    tuple: &[String],
    binding: &mut HashMap<String, String>,
    conf_acc: f64,
    walk: &mut Walk,
) {
    if tuple.len() != atom.args.len() {
        return;
    }
    let mut newly_bound: Vec<String> = Vec::new();
    let ok = atom
        .args
        .iter()
        .zip(tuple.iter())
        .all(|(arg, val)| try_bind_one_arg(engine, arg, val, binding, &mut newly_bound));
    if ok {
        let fconf = engine
            .conf
            .get(&(fp.to_string(), tuple.to_vec()))
            .copied()
            .unwrap_or(1.0);
        engine.eval_at(walk.body, walk.idx + 1, binding, conf_acc * fconf, walk.out);
    }
    for v in newly_bound {
        binding.remove(&v);
    }
}

/// Try one variable/constant binding against a stored tuple value.
fn try_bind_one_arg(
    engine: &Engine,
    arg: &RTerm,
    val: &String,
    binding: &mut HashMap<String, String>,
    newly_bound: &mut Vec<String>,
) -> bool {
    match arg {
        RTerm::Const(c) => engine.rep(c) == *val,
        RTerm::Var(v) => match binding.get(v) {
            Some(bound) => bound == val,
            None => {
                binding.insert(v.clone(), val.clone());
                newly_bound.push(v.clone());
                true
            }
        },
    }
}

/// Run the shared forward-chaining engine and project its canonical fact state.
pub(super) fn reason_facts(
    ont: &Ontology,
    facts: &[(String, Vec<String>, f64)],
    custom: &RuleSet,
) -> super::RuleReasonResult {
    let mut eng = Engine::default();
    for (pred, args, conf) in facts {
        eng.add_fact(pred, args, *conf, "asserted", false);
    }
    for (a, b) in &ont.same_as {
        eng.union(a, b);
    }
    eng.diff_pairs.extend(ont.different_from.iter().cloned());
    if !ont.same_as.is_empty() {
        eng.canonicalize();
    }

    let mut all_rules = super::builtin_rules(ont);
    all_rules.extend(custom.rules().iter().cloned());
    eng.run(&all_rules);

    let mut facts_out: Vec<(String, Vec<String>, f64)> = Vec::new();
    let mut derived_out: Vec<(String, Vec<String>, f64)> = Vec::new();
    for (pred, tuples) in &eng.by_pred {
        for tuple in tuples {
            let key = (pred.clone(), tuple.clone());
            let confidence = eng.conf.get(&key).copied().unwrap_or(1.0);
            facts_out.push((pred.clone(), tuple.clone(), confidence));
            if eng.derived.contains(&key) {
                derived_out.push((pred.clone(), tuple.clone(), confidence));
            }
        }
    }
    facts_out.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(&b.1)));
    derived_out.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(&b.1)));
    super::RuleReasonResult {
        facts: facts_out,
        derived: derived_out,
        same_as: eng.same_pairs.iter().cloned().collect(),
        consistent: eng.conflicts.is_empty(),
        conflicts: eng.conflicts,
    }
}
