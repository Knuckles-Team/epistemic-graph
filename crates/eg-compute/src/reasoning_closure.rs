//! CONCEPT:EG-KG.compute.reasoning-closure-gpu — semi-naive Datalog closure evaluator
//! with a GPU-offloadable transitive-join seam.
//!
//! This is the re-architecture the S4 deferral note in `reasoning.rs` called for. The
//! original five-rule fixpoint was a HETEROGENEOUS naive evaluation over string-keyed
//! `HashMap`/`HashSet` structures — re-scanning the ENTIRE accumulated fact set on every
//! iteration — with only Rule 5 (transitive closure) shaped like a sparse-matrix step.
//! Two changes fix that:
//!
//!   1. **Integer interning + semi-naive evaluation.** Node ids and type/predicate
//!      labels are interned to `u32` once ([`Interner`]); the fixpoint then works over
//!      integer relations and derives new facts from the per-round DELTA only (a fact can
//!      only be new if one of its premises was new last round), not by re-scanning the
//!      whole relation every iteration. Same result set, far less repeated work.
//!   2. **Rule 5 behind a [`ClosureBackend`] seam.** The transitive step is a boolean
//!      semiring join `{(x,z) | (x,y)∈A, (y,z)∈B}`. It is factored behind a trait with an
//!      always-compiled [`CpuBackend`] (hash-join) and a feature-gated
//!      `cuda::CudaBackend` (a two-pass CSR join kernel), mirroring
//!      `eg-ann::kmeans_gpu`'s `AssignBackend` seam EXACTLY — CPU-only builds link no
//!      accelerator, and any device/compile/launch failure degrades that call to CPU.
//!
//! Correctness anchors (see `tests`): [`infer_semi_naive`] derives the SAME set of facts
//! as the prior naive fixpoint ([`infer_naive_reference`], the differential oracle), and
//! the CUDA transitive-join agrees pair-for-pair with the CPU join (the parity test SKIPs
//! cleanly on a GPU-less host and auto-validates on a compatible CUDA device).
//!
//! Pi contract: identical to `eg-ann` — the CPU path needs no feature; `gpu`/`gpu-cuda`
//! are OUT of `pi`/`default`/`full` and `cudarc` links only under `gpu-cuda` (dlopen at
//! runtime, so a `gpu-cuda` build compiles with no CUDA toolkit and runs on a GPU-less
//! host).

use std::collections::{HashMap, HashSet};

/// String ↔ `u32` interner shared by node ids and type/predicate labels. One flat table:
/// a string that is both a node id and a label maps to a single id (they live in
/// different relations, so no ambiguity), which keeps interning a single pass.
#[derive(Default)]
pub struct Interner {
    to_id: HashMap<String, u32>,
    from_id: Vec<String>,
}

impl Interner {
    fn intern(&mut self, s: &str) -> u32 {
        if let Some(&id) = self.to_id.get(s) {
            return id;
        }
        let id = self.from_id.len() as u32;
        self.from_id.push(s.to_string());
        self.to_id.insert(s.to_string(), id);
        id
    }

    fn resolve(&self, id: u32) -> &str {
        &self.from_id[id as usize]
    }
}

/// The transitive-closure join backend (CONCEPT:EG-KG.compute.reasoning-closure-gpu):
/// computes the boolean-semiring join `{(x,z) | (x,y)∈left, (y,z)∈right}`. The returned
/// pairs may contain duplicates and pairs already known to the caller — the semi-naive
/// driver dedups against the accumulated relation. Every backend MUST return the SAME set
/// of pairs so a GPU-built closure is interchangeable with the CPU one.
pub trait ClosureBackend: Send + Sync {
    /// Stable backend name for logs/tests (`"cpu"`, `"cuda"`).
    fn name(&self) -> &'static str;

    /// `{(x, z) | (x, y) ∈ left, (y, z) ∈ right}` — join on the shared middle key.
    fn join_on_middle(&self, left: &[(u32, u32)], right: &[(u32, u32)]) -> Vec<(u32, u32)>;
}

/// The always-compiled pure-Rust CPU backend (CONCEPT:EG-KG.compute.reasoning-closure-gpu).
/// Hash-joins on the middle key: index `right` by its source, then for every `left`
/// `(x, y)` emit `(x, z)` for each `z` in `right[y]`.
#[derive(Debug, Default, Clone, Copy)]
pub struct CpuBackend;

impl ClosureBackend for CpuBackend {
    fn name(&self) -> &'static str {
        "cpu"
    }

    fn join_on_middle(&self, left: &[(u32, u32)], right: &[(u32, u32)]) -> Vec<(u32, u32)> {
        if left.is_empty() || right.is_empty() {
            return Vec::new();
        }
        let mut by_src: HashMap<u32, Vec<u32>> = HashMap::new();
        for &(y, z) in right {
            by_src.entry(y).or_default().push(z);
        }
        let mut out = Vec::new();
        for &(x, y) in left {
            if let Some(zs) = by_src.get(&y) {
                for &z in zs {
                    out.push((x, z));
                }
            }
        }
        out
    }
}

/// The active transitive-join backend (CONCEPT:EG-KG.compute.reasoning-closure-gpu): CUDA
/// when compiled + a device is present, else CPU. Built once and cached.
pub fn active_closure_backend() -> &'static dyn ClosureBackend {
    #[cfg(feature = "gpu-cuda")]
    {
        if let Some(b) = cuda::backend() {
            return b;
        }
    }
    static CPU: CpuBackend = CpuBackend;
    &CPU
}

/// The active backend's name (`"cpu"`/`"cuda"`) for observability/tests.
pub fn active_closure_backend_name() -> &'static str {
    active_closure_backend().name()
}

/// Rule inputs, interned to `u32`. Built once by [`Rules::intern`] from the string
/// rule lists so the fixpoint never touches strings.
struct Rules {
    subclass: HashMap<u32, Vec<u32>>,
    subprop: HashMap<u32, Vec<u32>>,
    symmetric: HashSet<u32>,
    transitive: HashSet<u32>,
    inverse: HashMap<u32, u32>,
}

/// Intern a `(sub, super)` rule list into a multimap keyed by the subordinate term.
fn intern_hierarchy(relations: Vec<(String, String)>, it: &mut Interner) -> HashMap<u32, Vec<u32>> {
    let mut hierarchy: HashMap<u32, Vec<u32>> = HashMap::new();
    for (sub, sup) in relations {
        let sub = it.intern(&sub);
        let sup = it.intern(&sup);
        hierarchy.entry(sub).or_default().push(sup);
    }
    hierarchy
}

impl Rules {
    /// Intern the five string rule lists once, before the fixpoint starts.
    fn intern(
        subclass_relations: Vec<(String, String)>,
        subproperty_relations: Vec<(String, String)>,
        symmetric_properties: Vec<String>,
        transitive_properties: Vec<String>,
        inverse_properties: Vec<(String, String)>,
        it: &mut Interner,
    ) -> Self {
        let subclass = intern_hierarchy(subclass_relations, it);
        let subprop = intern_hierarchy(subproperty_relations, it);
        let symmetric: HashSet<u32> = symmetric_properties.iter().map(|p| it.intern(p)).collect();
        let transitive: HashSet<u32> = transitive_properties.iter().map(|p| it.intern(p)).collect();
        let mut inverse: HashMap<u32, u32> = HashMap::new();
        for (p1, p2) in inverse_properties {
            let a = it.intern(&p1);
            let b = it.intern(&p2);
            inverse.insert(a, b);
            inverse.insert(b, a);
        }
        Rules {
            subclass,
            subprop,
            symmetric,
            transitive,
            inverse,
        }
    }
}

/// The accumulated integer relations of one semi-naive run: everything known, the
/// previous round's delta, and the facts derived beyond the base input.
#[derive(Default)]
struct Closure {
    known_node_types: HashSet<(u32, u32)>,
    known_edges: HashSet<(u32, u32, u32)>,
    /// Accumulated transitive edges grouped by predicate (only predicates that are
    /// transitive need grouping — that is all Rule 5 consults).
    transitive_edges: HashMap<u32, Vec<(u32, u32)>>,
    delta_node_types: Vec<(u32, u32)>,
    delta_edges: Vec<(u32, u32, u32)>,
    derived_node_types: Vec<(u32, u32)>,
    derived_edges: Vec<(u32, u32, u32)>,
}

impl Closure {
    /// Record a node-type fact if it is new; `derived` marks it as beyond the base input.
    fn accept_node_type(&mut self, fact: (u32, u32), derived: bool) -> bool {
        if !self.known_node_types.insert(fact) {
            return false;
        }
        if derived {
            self.derived_node_types.push(fact);
        }
        true
    }

    /// Record an edge fact if it is new, keeping the transitive grouping in step.
    fn accept_edge(&mut self, fact: (u32, u32, u32), rules: &Rules, derived: bool) -> bool {
        if !self.known_edges.insert(fact) {
            return false;
        }
        if derived {
            self.derived_edges.push(fact);
        }
        if rules.transitive.contains(&fact.2) {
            self.transitive_edges
                .entry(fact.2)
                .or_default()
                .push((fact.0, fact.1));
        }
        true
    }

    /// Intern the base facts; they are the fixpoint's first delta and are not derived.
    fn from_base(
        base_node_types: &[(String, String)],
        base_edge_types: &[(String, String, String)],
        rules: &Rules,
        it: &mut Interner,
    ) -> Self {
        let mut closure = Closure::default();
        for (n, t) in base_node_types {
            let fact = (it.intern(n), it.intern(t));
            if closure.accept_node_type(fact, false) {
                closure.delta_node_types.push(fact);
            }
        }
        for (s, g, p) in base_edge_types {
            let fact = (it.intern(s), it.intern(g), it.intern(p));
            if closure.accept_edge(fact, rules, false) {
                closure.delta_edges.push(fact);
            }
        }
        closure
    }

    /// Rule 1 (subclass): a node with a type gains its supertypes. Semi-naive over the
    /// previous round's node-type facts only.
    fn subclass_candidates(&self, rules: &Rules) -> Vec<(u32, u32)> {
        let mut candidates = Vec::new();
        for &(node, t) in &self.delta_node_types {
            for &sup in rules.subclass.get(&t).into_iter().flatten() {
                candidates.push((node, sup));
            }
        }
        candidates
    }

    /// Rules 2–4 (subproperty, symmetric, inverse) over the previous round's edges only.
    fn edge_rule_candidates(&self, rules: &Rules) -> Vec<(u32, u32, u32)> {
        let mut candidates = Vec::new();
        for &(s, g, p) in &self.delta_edges {
            // Rule 2 (subproperty): the edge gains its super-properties.
            for &sup in rules.subprop.get(&p).into_iter().flatten() {
                candidates.push((s, g, sup));
            }
            // Rule 3 (symmetric): the reverse edge gains the same property.
            if rules.symmetric.contains(&p) {
                candidates.push((g, s, p));
            }
            // Rule 4 (inverse): the reverse edge gains the inverse property.
            if let Some(&inv) = rules.inverse.get(&p) {
                candidates.push((g, s, inv));
            }
        }
        candidates
    }

    /// Rule 5 (transitive): boolean-semiring join for each transitive predicate,
    /// offloaded to `backend`. Semi-naive form: a new 2-path uses at least one delta
    /// edge, so join both DELTA⋈FULL and FULL⋈DELTA (FULL already contains DELTA, so
    /// this also covers DELTA⋈DELTA). This is the one sparse-matrix-shaped rule and the
    /// sole GPU seam.
    fn transitive_candidates(
        &self,
        rules: &Rules,
        backend: &dyn ClosureBackend,
    ) -> Vec<(u32, u32, u32)> {
        if rules.transitive.is_empty() {
            return Vec::new();
        }
        let mut delta_by_prop: HashMap<u32, Vec<(u32, u32)>> = HashMap::new();
        for &(s, g, p) in &self.delta_edges {
            if rules.transitive.contains(&p) {
                delta_by_prop.entry(p).or_default().push((s, g));
            }
        }
        let mut candidates = Vec::new();
        for (&p, delta) in &delta_by_prop {
            let full = self
                .transitive_edges
                .get(&p)
                .map(Vec::as_slice)
                .unwrap_or(&[]);
            for (left, right) in [(delta.as_slice(), full), (full, delta.as_slice())] {
                for (x, z) in backend.join_on_middle(left, right) {
                    candidates.push((x, z, p));
                }
            }
        }
        candidates
    }

    /// Derive one round: everything the previous delta entails becomes the next delta.
    fn advance(&mut self, rules: &Rules, backend: &dyn ClosureBackend) {
        let candidate_node_types = self.subclass_candidates(rules);
        let mut candidate_edges = self.edge_rule_candidates(rules);
        candidate_edges.extend(self.transitive_candidates(rules, backend));

        self.delta_node_types = candidate_node_types
            .into_iter()
            .filter(|&fact| self.accept_node_type(fact, true))
            .collect();
        self.delta_edges = candidate_edges
            .into_iter()
            .filter(|&fact| self.accept_edge(fact, rules, true))
            .collect();
    }

    fn exhausted(&self) -> bool {
        self.delta_node_types.is_empty() && self.delta_edges.is_empty()
    }
}

/// Derive all facts entailed by the five OWL/RDFS rules (subclass, subproperty,
/// symmetric, inverse, transitive) via SEMI-NAIVE evaluation over interned integer
/// relations, offloading the transitive join to `backend`.
///
/// Inputs are the base facts and the rule lists as strings; returns the DERIVED facts
/// (accumulated minus base) as strings:
///   * `.0` — new `(node, type)` facts (Rule 1),
///   * `.1` — new `(src, tgt, prop)` edge facts (Rules 2–5).
///
/// The result set is identical to the prior naive fixpoint (see the differential test).
#[allow(clippy::type_complexity, clippy::too_many_arguments)]
pub fn infer_semi_naive(
    base_node_types: &[(String, String)],
    base_edge_types: &[(String, String, String)],
    subclass_relations: Vec<(String, String)>,
    subproperty_relations: Vec<(String, String)>,
    symmetric_properties: Vec<String>,
    transitive_properties: Vec<String>,
    inverse_properties: Vec<(String, String)>,
    backend: &dyn ClosureBackend,
) -> (Vec<(String, String)>, Vec<(String, String, String)>) {
    let mut it = Interner::default();
    let rules = Rules::intern(
        subclass_relations,
        subproperty_relations,
        symmetric_properties,
        transitive_properties,
        inverse_properties,
        &mut it,
    );
    let mut closure = Closure::from_base(base_node_types, base_edge_types, &rules, &mut it);

    // Semi-naive fixpoint: each round derives only from the previous round's delta. The
    // 100-round cap mirrors the naive evaluator's safety bound (these monotone rules
    // converge in a handful of rounds on real ontologies).
    for _ in 0..100 {
        if closure.exhausted() {
            break;
        }
        closure.advance(&rules, backend);
    }

    // Resolve derived facts back to strings.
    let out_nt = closure
        .derived_node_types
        .into_iter()
        .map(|(n, t)| (it.resolve(n).to_string(), it.resolve(t).to_string()))
        .collect();
    let out_et = closure
        .derived_edges
        .into_iter()
        .map(|(s, g, p)| {
            (
                it.resolve(s).to_string(),
                it.resolve(g).to_string(),
                it.resolve(p).to_string(),
            )
        })
        .collect();
    (out_nt, out_et)
}

// ── CONCEPT:EG-KG.backend.real-cuda-tensor-backend — the real CUDA transitive-join backend ──

#[cfg(feature = "gpu-cuda")]
pub mod cuda;

#[cfg(test)]
mod tests {
    use super::*;
    use rand::{Rng, SeedableRng};
    use rand_chacha::ChaCha8Rng;

    /// NAIVE reference evaluator — the pre-existing string-keyed five-rule fixpoint,
    /// distilled to pure inference over the base fact lists (no graph mutation). This is
    /// the differential ORACLE: [`infer_semi_naive`] must return the same DERIVED sets.
    fn naive_relation_map(relations: &[(String, String)]) -> HashMap<String, Vec<String>> {
        let mut map: HashMap<String, Vec<String>> = HashMap::new();
        for (sub, sup) in relations {
            map.entry(sub.clone()).or_default().push(sup.clone());
        }
        map
    }

    fn naive_inverse_map(relations: &[(String, String)]) -> HashMap<String, String> {
        let mut map = HashMap::new();
        for (first, second) in relations {
            map.insert(first.clone(), second.clone());
            map.insert(second.clone(), first.clone());
        }
        map
    }

    fn naive_node_types(base: &[(String, String)]) -> HashMap<String, HashSet<String>> {
        let mut node_types: HashMap<String, HashSet<String>> = HashMap::new();
        for (node, type_name) in base {
            node_types
                .entry(node.clone())
                .or_default()
                .insert(type_name.clone());
        }
        node_types
    }

    fn naive_edge_types(
        base: &[(String, String, String)],
    ) -> HashMap<(String, String), HashSet<String>> {
        let mut edge_types: HashMap<(String, String), HashSet<String>> = HashMap::new();
        for (source, target, property) in base {
            edge_types
                .entry((source.clone(), target.clone()))
                .or_default()
                .insert(property.clone());
        }
        edge_types
    }

    fn append_node_type_pending(
        node: &str,
        types: &HashSet<String>,
        type_name: &str,
        subclass_map: &HashMap<String, Vec<String>>,
        pending: &mut Vec<(String, String)>,
    ) {
        if let Some(supers) = subclass_map.get(type_name) {
            for super_type in supers {
                if !types.contains(super_type) {
                    pending.push((node.to_owned(), super_type.clone()));
                }
            }
        }
    }

    fn append_node_pending(
        node_types: &HashMap<String, HashSet<String>>,
        subclass_map: &HashMap<String, Vec<String>>,
        pending: &mut Vec<(String, String)>,
    ) {
        for (node, types) in node_types {
            for type_name in types {
                append_node_type_pending(node, types, type_name, subclass_map, pending);
            }
        }
    }

    fn append_subproperty_pending(
        source: &str,
        target: &str,
        property: &str,
        edge_types: &HashMap<(String, String), HashSet<String>>,
        subproperty_map: &HashMap<String, Vec<String>>,
        pending: &mut Vec<(String, String, String)>,
    ) {
        if let Some(supers) = subproperty_map.get(property) {
            for super_property in supers {
                let exists = edge_types
                    .get(&(source.to_owned(), target.to_owned()))
                    .is_some_and(|properties| properties.contains(super_property));
                if !exists {
                    pending.push((source.to_owned(), target.to_owned(), super_property.clone()));
                }
            }
        }
    }

    fn append_symmetric_pending(
        source: &str,
        target: &str,
        property: &str,
        edge_types: &HashMap<(String, String), HashSet<String>>,
        symmetric: &HashSet<String>,
        pending: &mut Vec<(String, String, String)>,
    ) {
        if symmetric.contains(property) {
            let exists = edge_types
                .get(&(target.to_owned(), source.to_owned()))
                .is_some_and(|properties| properties.contains(property));
            if !exists {
                pending.push((target.to_owned(), source.to_owned(), property.to_owned()));
            }
        }
    }

    fn append_inverse_pending(
        source: &str,
        target: &str,
        property: &str,
        edge_types: &HashMap<(String, String), HashSet<String>>,
        inverse_map: &HashMap<String, String>,
        pending: &mut Vec<(String, String, String)>,
    ) {
        if let Some(inverse) = inverse_map.get(property) {
            let exists = edge_types
                .get(&(target.to_owned(), source.to_owned()))
                .is_some_and(|properties| properties.contains(inverse));
            if !exists {
                pending.push((target.to_owned(), source.to_owned(), inverse.clone()));
            }
        }
    }

    fn append_edge_pending(
        edge_types: &HashMap<(String, String), HashSet<String>>,
        subproperty_map: &HashMap<String, Vec<String>>,
        symmetric: &HashSet<String>,
        inverse_map: &HashMap<String, String>,
        pending: &mut Vec<(String, String, String)>,
    ) {
        for ((source, target), properties) in edge_types {
            for property in properties {
                append_subproperty_pending(
                    source,
                    target,
                    property,
                    edge_types,
                    subproperty_map,
                    pending,
                );
                append_symmetric_pending(source, target, property, edge_types, symmetric, pending);
                append_inverse_pending(source, target, property, edge_types, inverse_map, pending);
            }
        }
    }

    fn property_edges(
        edge_types: &HashMap<(String, String), HashSet<String>>,
        property: &str,
    ) -> Vec<(String, String)> {
        let mut edges = Vec::new();
        for ((source, target), properties) in edge_types {
            if properties.contains(property) {
                edges.push((source.clone(), target.clone()));
            }
        }
        edges
    }

    fn append_transitive_pairs(
        edge_types: &HashMap<(String, String), HashSet<String>>,
        property: &str,
        edges: &[(String, String)],
        pending: &mut Vec<(String, String, String)>,
    ) {
        for (source, middle) in edges {
            for (middle_candidate, target) in edges {
                if middle != middle_candidate {
                    continue;
                }
                let exists = edge_types
                    .get(&(source.to_owned(), target.to_owned()))
                    .is_some_and(|properties| properties.contains(property));
                if !exists {
                    pending.push((source.to_owned(), target.to_owned(), property.to_owned()));
                }
            }
        }
    }

    fn append_transitive_pending(
        edge_types: &HashMap<(String, String), HashSet<String>>,
        transitive: &HashSet<String>,
        pending: &mut Vec<(String, String, String)>,
    ) {
        for property in transitive {
            let edges = property_edges(edge_types, property);
            append_transitive_pairs(edge_types, property, &edges, pending);
        }
    }

    fn apply_node_pending(
        node_types: &mut HashMap<String, HashSet<String>>,
        pending: Vec<(String, String)>,
        derived: &mut HashSet<(String, String)>,
    ) -> bool {
        let mut changed = false;
        for (node, type_name) in pending {
            if node_types
                .entry(node.clone())
                .or_default()
                .insert(type_name.clone())
            {
                derived.insert((node, type_name));
                changed = true;
            }
        }
        changed
    }

    fn apply_edge_pending(
        edge_types: &mut HashMap<(String, String), HashSet<String>>,
        pending: Vec<(String, String, String)>,
        derived: &mut HashSet<(String, String, String)>,
    ) -> bool {
        let mut changed = false;
        for (source, target, property) in pending {
            if edge_types
                .entry((source.clone(), target.clone()))
                .or_default()
                .insert(property.clone())
            {
                derived.insert((source, target, property));
                changed = true;
            }
        }
        changed
    }

    #[allow(clippy::type_complexity)]
    fn infer_naive_reference(
        base_node_types: &[(String, String)],
        base_edge_types: &[(String, String, String)],
        subclass_relations: &[(String, String)],
        subproperty_relations: &[(String, String)],
        symmetric_properties: &[String],
        transitive_properties: &[String],
        inverse_properties: &[(String, String)],
    ) -> (HashSet<(String, String)>, HashSet<(String, String, String)>) {
        let subclass_map = naive_relation_map(subclass_relations);
        let subproperty_map = naive_relation_map(subproperty_relations);
        let symmetric_set: HashSet<String> = symmetric_properties.iter().cloned().collect();
        let transitive_set: HashSet<String> = transitive_properties.iter().cloned().collect();
        let inverse_map = naive_inverse_map(inverse_properties);
        let mut node_types = naive_node_types(base_node_types);
        let mut edge_types = naive_edge_types(base_edge_types);

        let mut new_nt: HashSet<(String, String)> = HashSet::new();
        let mut new_et: HashSet<(String, String, String)> = HashSet::new();
        let mut changed = true;
        let mut iters = 0;
        while changed && iters < 100 {
            let mut pend_nt = Vec::new();
            let mut pend_et = Vec::new();
            append_node_pending(&node_types, &subclass_map, &mut pend_nt);
            append_edge_pending(
                &edge_types,
                &subproperty_map,
                &symmetric_set,
                &inverse_map,
                &mut pend_et,
            );
            append_transitive_pending(&edge_types, &transitive_set, &mut pend_et);
            changed = apply_node_pending(&mut node_types, pend_nt, &mut new_nt);
            changed |= apply_edge_pending(&mut edge_types, pend_et, &mut new_et);
            iters += 1;
        }
        (new_nt, new_et)
    }

    #[test]
    fn cpu_join_on_middle_matches_definition() {
        let left = vec![(1u32, 2u32), (1, 3), (4, 2)];
        let right = vec![(2u32, 9u32), (2, 8), (3, 7)];
        let got: HashSet<(u32, u32)> = CpuBackend
            .join_on_middle(&left, &right)
            .into_iter()
            .collect();
        let want: HashSet<(u32, u32)> = [(1, 9), (1, 8), (1, 7), (4, 9), (4, 8)]
            .into_iter()
            .collect();
        assert_eq!(got, want);
    }

    #[test]
    fn semi_naive_transitive_closure() {
        let edges = vec![
            ("a".into(), "b".into(), "anc".into()),
            ("b".into(), "c".into(), "anc".into()),
            ("c".into(), "d".into(), "anc".into()),
        ];
        let (_nt, et) = infer_semi_naive(
            &[],
            &edges,
            vec![],
            vec![],
            vec![],
            vec!["anc".into()],
            vec![],
            &CpuBackend,
        );
        let set: HashSet<(String, String, String)> = et.into_iter().collect();
        // a→c, a→d, b→d are the transitive closures (a→b, b→c, c→d are base).
        assert!(set.contains(&("a".into(), "c".into(), "anc".into())));
        assert!(set.contains(&("a".into(), "d".into(), "anc".into())));
        assert!(set.contains(&("b".into(), "d".into(), "anc".into())));
        assert_eq!(set.len(), 3);
    }

    /// DIFFERENTIAL ORACLE: over randomized ontologies exercising all five rules, the
    /// semi-naive evaluator derives EXACTLY the naive reference's fact set.
    #[test]
    fn semi_naive_equals_naive_reference_randomized() {
        for seed in 0..40u64 {
            let mut rng = ChaCha8Rng::seed_from_u64(seed);
            let n_nodes = rng.gen_range(3..10);
            let types = ["T0", "T1", "T2", "T3"];
            let props = ["p0", "p1", "p2"];

            let mut base_nt = Vec::new();
            for i in 0..n_nodes {
                if rng.gen_bool(0.7) {
                    base_nt.push((format!("n{i}"), types[rng.gen_range(0..types.len())].into()));
                }
            }
            let mut base_et = Vec::new();
            let n_edges = rng.gen_range(2..12);
            for _ in 0..n_edges {
                let s = format!("n{}", rng.gen_range(0..n_nodes));
                let g = format!("n{}", rng.gen_range(0..n_nodes));
                base_et.push((s, g, props[rng.gen_range(0..props.len())].into()));
            }
            // Rule config, some chains to force multi-round fixpoints.
            let subclass = vec![
                ("T0".to_string(), "T1".to_string()),
                ("T1".to_string(), "T2".to_string()),
            ];
            let subprop = vec![("p0".to_string(), "p1".to_string())];
            let symmetric = if rng.gen_bool(0.5) {
                vec!["p2".to_string()]
            } else {
                vec![]
            };
            let transitive = vec!["p1".to_string()];
            let inverse = vec![("p0".to_string(), "p2".to_string())];

            let (naive_nt, naive_et) = infer_naive_reference(
                &base_nt,
                &base_et,
                &subclass,
                &subprop,
                &symmetric,
                &transitive,
                &inverse,
            );
            let (sn_nt_v, sn_et_v) = infer_semi_naive(
                &base_nt,
                &base_et,
                subclass,
                subprop,
                symmetric,
                transitive,
                inverse,
                &CpuBackend,
            );
            let sn_nt: HashSet<(String, String)> = sn_nt_v.into_iter().collect();
            let sn_et: HashSet<(String, String, String)> = sn_et_v.into_iter().collect();
            assert_eq!(sn_nt, naive_nt, "node-type set mismatch (seed {seed})");
            assert_eq!(sn_et, naive_et, "edge set mismatch (seed {seed})");
        }
    }

    #[test]
    fn dispatch_backend_is_named() {
        let name = active_closure_backend_name();
        assert!(name == "cpu" || name == "cuda");
    }

    /// The CUDA probe on a host with no usable device (every build host and CI runner):
    /// `cuda::backend()` must absorb the driver-load failure (cudarc panics when libcuda
    /// cannot be loaded) as `None`, and dispatch must then select and run the CPU backend.
    /// On a host where a device does initialise this fails, directing the run to the
    /// `eg_gpu_device_tests` parity test instead.
    #[cfg(all(feature = "gpu-cuda", not(eg_gpu_device_tests)))]
    #[test]
    fn cuda_probe_without_a_device_falls_back_to_cpu() {
        assert!(
            cuda::backend().is_none(),
            "a CUDA device initialised, so the device parity test must run: \
             RUSTFLAGS=\"--cfg eg_gpu_device_tests\" cargo test -p eg-compute --features gpu-cuda"
        );
        assert_eq!(active_closure_backend_name(), "cpu");
        let mut joined =
            active_closure_backend().join_on_middle(&[(1, 2), (3, 4)], &[(2, 5), (2, 6), (7, 8)]);
        joined.sort_unstable();
        assert_eq!(joined, vec![(1, 5), (1, 6)]);
    }

    /// GPU↔CPU parity (CONCEPT:EG-KG.compute.reasoning-closure-gpu). When a CUDA device is
    /// present the real transitive-join kernel MUST produce the SAME pair SET as the CPU
    /// hash-join for a batch spanning several thread blocks.
    /// It needs a real device, which no build host or CI runner has, so it is compiled
    /// only under the explicit opt-in `--cfg eg_gpu_device_tests` and FAILS when no
    /// device initialises. Run it on a CUDA host with
    /// `RUSTFLAGS="--cfg eg_gpu_device_tests" cargo test -p eg-compute --features gpu-cuda`.
    #[cfg(all(feature = "gpu-cuda", eg_gpu_device_tests))]
    #[test]
    fn cuda_join_matches_cpu_ground_truth() {
        let gpu = cuda::backend()
            .expect("--cfg eg_gpu_device_tests is set but no CUDA device initialised on this host");
        assert_eq!(gpu.name(), "cuda", "backend() returned a non-CUDA backend");

        let mut rng = ChaCha8Rng::seed_from_u64(9);
        let keyspace = 128u32;
        let left: Vec<(u32, u32)> = (0..5000)
            .map(|_| (rng.gen_range(0..keyspace), rng.gen_range(0..keyspace)))
            .collect();
        let right: Vec<(u32, u32)> = (0..5000)
            .map(|_| (rng.gen_range(0..keyspace), rng.gen_range(0..keyspace)))
            .collect();

        let cpu: HashSet<(u32, u32)> = CpuBackend
            .join_on_middle(&left, &right)
            .into_iter()
            .collect();
        let gpu_set: HashSet<(u32, u32)> = gpu.join_on_middle(&left, &right).into_iter().collect();
        assert_eq!(cpu, gpu_set, "GPU transitive-join set != CPU ground truth");
    }
}
