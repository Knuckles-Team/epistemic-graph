// CONCEPT:EG-KG.mining.frequent-itemset-mining — Frequent-itemset + association-rule mining.
//
// Pure-Rust, dependency-light, batch (one round-trip): given a set of
// transactions (each a set of item ids), compute the frequent itemsets and the
// association rules between them (support / confidence / lift), thresholded by
// `min_support` (a fraction of transactions) and `min_confidence`.
//
// Three interchangeable frequent-itemset engines are provided — **Apriori**
// (breadth-first candidate generation), **FP-Growth** (prefix-tree, no candidate
// generation), and **Eclat** (vertical tid-set intersection). All three are exact
// and, for the same `min_support`, produce the SAME frequent-itemset set (asserted
// by the parity test), so rule generation is shared downstream.
//
// This module is graph-agnostic: it works over interned `ItemId`s. The handler
// (`src/server/handlers/mining.rs`) does the string↔id interning and the
// graph-derived transaction construction (compute-near-data). `mine_labeled` is a
// convenience that interns `String` items, runs the chosen engine, and hands back
// string-labeled rules — used by both the handler and the unit tests.

use std::collections::HashMap;

/// An interned item id (small, dense — assigned by [`intern`]).
pub type ItemId = u32;

/// Which frequent-itemset engine to run. All are exact and agree on the output.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Algorithm {
    Apriori,
    FpGrowth,
    Eclat,
}

/// A frequent itemset: its (sorted) items, absolute count, and fractional support.
#[derive(Debug, Clone, PartialEq)]
pub struct FrequentItemset {
    pub items: Vec<ItemId>,
    pub count: usize,
    pub support: f64,
}

/// An association rule `antecedent ⇒ consequent` with the three standard metrics.
///
/// * `support`    = P(antecedent ∪ consequent) — fraction of transactions holding all items.
/// * `confidence` = P(consequent | antecedent) = support(A∪C) / support(A).
/// * `lift`       = confidence / support(consequent) — >1 ⇒ positively correlated.
#[derive(Debug, Clone, PartialEq)]
pub struct Rule {
    pub antecedent: Vec<ItemId>,
    pub consequent: Vec<ItemId>,
    pub support: f64,
    pub confidence: f64,
    pub lift: f64,
}

/// A string-labeled rule (the wire/row shape the handler and client see).
#[derive(Debug, Clone, PartialEq)]
pub struct LabeledRule {
    pub antecedent: Vec<String>,
    pub consequent: Vec<String>,
    pub support: f64,
    pub confidence: f64,
    pub lift: f64,
}

/// Convert a fractional `min_support` (0.0–1.0) into an absolute minimum count over
/// `n` transactions, clamped to at least 1 (a 0 threshold would enumerate every
/// singleton and is never useful).
fn min_count(min_support: f64, n: usize) -> usize {
    let raw = (min_support * n as f64).ceil() as usize;
    raw.max(1)
}

// ─────────────────────────── Apriori ───────────────────────────

/// Frequent itemsets via classic Apriori (CONCEPT:EG-KG.mining.apriori-levelwise): a
/// breadth-first, level-wise candidate-generation + prune loop. Deterministic.
pub fn apriori(transactions: &[Vec<ItemId>], min_count: usize) -> Vec<FrequentItemset> {
    let n = transactions.len();
    // Normalize each transaction to a sorted, deduped item vec once.
    let txns: Vec<Vec<ItemId>> = transactions.iter().map(|t| sorted_unique(t)).collect();

    let mut all: Vec<FrequentItemset> = Vec::new();

    // L1: singleton counts.
    let mut counts: HashMap<ItemId, usize> = HashMap::new();
    for t in &txns {
        for &item in t {
            *counts.entry(item).or_insert(0) += 1;
        }
    }
    let mut current: Vec<Vec<ItemId>> = Vec::new();
    let mut singletons: Vec<(ItemId, usize)> = counts
        .into_iter()
        .filter(|&(_, c)| c >= min_count)
        .collect();
    singletons.sort_unstable();
    for (item, count) in singletons {
        current.push(vec![item]);
        all.push(FrequentItemset {
            items: vec![item],
            count,
            support: count as f64 / n as f64,
        });
    }

    // Lk from L(k-1) until no frequent set remains.
    while !current.is_empty() {
        let candidates = apriori_gen(&current);
        let mut next: Vec<Vec<ItemId>> = Vec::new();
        for cand in candidates {
            let count = txns.iter().filter(|t| contains_sorted(t, &cand)).count();
            if count >= min_count {
                next.push(cand.clone());
                all.push(FrequentItemset {
                    items: cand,
                    count,
                    support: count as f64 / n as f64,
                });
            }
        }
        next.sort_unstable();
        current = next;
    }
    all
}

/// Candidate generation: join two frequent (k-1)-itemsets that share their first
/// k-2 items into a k-itemset, then prune any candidate with an infrequent
/// (k-1)-subset (the downward-closure prune). `prev` must be sorted.
fn apriori_gen(prev: &[Vec<ItemId>]) -> Vec<Vec<ItemId>> {
    let prev_set: std::collections::HashSet<&Vec<ItemId>> = prev.iter().collect();
    let mut out: Vec<Vec<ItemId>> = Vec::new();
    for i in 0..prev.len() {
        for j in (i + 1)..prev.len() {
            let a = &prev[i];
            let b = &prev[j];
            let k = a.len();
            // Join only if the first k-1 items are identical and the last differs
            // (a[k-1] < b[k-1] because `prev` is sorted) — the standard F(k-1)×F(k-1) join.
            if a[..k - 1] == b[..k - 1] && a[k - 1] < b[k - 1] {
                let mut cand = a.clone();
                cand.push(b[k - 1]);
                if all_subsets_frequent(&cand, &prev_set) {
                    out.push(cand);
                }
            }
        }
    }
    out
}

/// Prune step: every (k-1)-subset of `cand` (drop one item at a time) must be a
/// member of the frequent (k-1) set.
fn all_subsets_frequent(
    cand: &[ItemId],
    prev_set: &std::collections::HashSet<&Vec<ItemId>>,
) -> bool {
    if cand.len() <= 1 {
        return true;
    }
    for drop in 0..cand.len() {
        let subset: Vec<ItemId> = cand
            .iter()
            .enumerate()
            .filter_map(|(i, &x)| (i != drop).then_some(x))
            .collect();
        if !prev_set.contains(&subset) {
            return false;
        }
    }
    true
}

// ─────────────────────────── Eclat ───────────────────────────

/// Frequent itemsets via Eclat (CONCEPT:EG-KG.mining.eclat-vertical-tidset): a
/// depth-first search over the VERTICAL data layout — each item carries the sorted
/// tid-set of transactions containing it, and a k-itemset's support is the size of
/// the intersection of its items' tid-sets. Deterministic.
pub fn eclat(transactions: &[Vec<ItemId>], min_count: usize) -> Vec<FrequentItemset> {
    let n = transactions.len();
    // Build the vertical tid-sets for frequent singletons.
    let mut tid: HashMap<ItemId, Vec<usize>> = HashMap::new();
    for (t_idx, t) in transactions.iter().enumerate() {
        for &item in sorted_unique(t).iter() {
            tid.entry(item).or_default().push(t_idx);
        }
    }
    let mut atoms: Vec<(ItemId, Vec<usize>)> = tid
        .into_iter()
        .filter(|(_, ts)| ts.len() >= min_count)
        .collect();
    atoms.sort_by_key(|(item, _)| *item);

    let mut all: Vec<FrequentItemset> = Vec::new();
    eclat_dfs(&[], &atoms, min_count, n, &mut all);
    all
}

/// DFS extending `prefix` (the shared item path) by each atom, intersecting tid-sets.
fn eclat_dfs(
    prefix: &[ItemId],
    atoms: &[(ItemId, Vec<usize>)],
    min_count: usize,
    n: usize,
    out: &mut Vec<FrequentItemset>,
) {
    for i in 0..atoms.len() {
        let (item, ref tids) = atoms[i];
        let mut items = prefix.to_vec();
        items.push(item);
        out.push(FrequentItemset {
            items: items.clone(),
            count: tids.len(),
            support: tids.len() as f64 / n as f64,
        });
        // Build the extension atoms (only later items, keeping itemsets sorted).
        let mut children: Vec<(ItemId, Vec<usize>)> = Vec::new();
        for &(next_item, ref next_tids) in atoms.iter().skip(i + 1) {
            let inter = intersect_sorted(tids, next_tids);
            if inter.len() >= min_count {
                children.push((next_item, inter));
            }
        }
        if !children.is_empty() {
            eclat_dfs(&items, &children, min_count, n, out);
        }
    }
}

// ─────────────────────────── FP-Growth ───────────────────────────

/// A node in the FP-tree.
struct FpNode {
    item: ItemId,
    count: usize,
    parent: Option<usize>,
    children: HashMap<ItemId, usize>,
}

/// Frequent itemsets via FP-Growth (CONCEPT:EG-KG.mining.fpgrowth-prefix-tree): build a
/// frequency-ordered prefix tree (FP-tree) of the transactions, then recursively
/// mine conditional pattern bases — NO candidate generation. Deterministic.
pub fn fpgrowth(transactions: &[Vec<ItemId>], min_count: usize) -> Vec<FrequentItemset> {
    let n = transactions.len();

    // Global item frequencies → keep only frequent items, ordered by descending
    // count (ties broken by item id for determinism).
    let mut freq: HashMap<ItemId, usize> = HashMap::new();
    for t in transactions {
        for &item in sorted_unique(t).iter() {
            *freq.entry(item).or_insert(0) += 1;
        }
    }
    let mut order: Vec<(ItemId, usize)> = freq
        .iter()
        .filter(|&(_, &c)| c >= min_count)
        .map(|(&i, &c)| (i, c))
        .collect();
    order.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(&b.0)));
    let rank: HashMap<ItemId, usize> = order
        .iter()
        .enumerate()
        .map(|(r, &(item, _))| (item, r))
        .collect();

    // Project each transaction onto the frequent items in FP order.
    let projected: Vec<Vec<ItemId>> = transactions
        .iter()
        .map(|t| {
            let mut items: Vec<ItemId> = sorted_unique(t)
                .into_iter()
                .filter(|i| rank.contains_key(i))
                .collect();
            items.sort_by_key(|i| rank[i]);
            items
        })
        .filter(|t| !t.is_empty())
        .collect();

    let mut out: Vec<FrequentItemset> = Vec::new();
    fp_mine(&projected, &[], min_count, n, &mut out);
    // Determinism: sort by length then items so the result set is stable across
    // the (recursion-order-dependent) discovery sequence.
    out.sort_by(|a, b| {
        a.items
            .len()
            .cmp(&b.items.len())
            .then(a.items.cmp(&b.items))
    });
    out
}

/// Mine the conditional FP-tree built from `txns` (each a frequency-ordered item
/// path), emitting every frequent itemset that extends `suffix`.
fn fp_mine(
    txns: &[Vec<ItemId>],
    suffix: &[ItemId],
    min_count: usize,
    n: usize,
    out: &mut Vec<FrequentItemset>,
) {
    // Build the FP-tree (arena of nodes) + per-item node lists (header table).
    let mut arena: Vec<FpNode> = vec![FpNode {
        item: ItemId::MAX,
        count: 0,
        parent: None,
        children: HashMap::new(),
    }];
    let mut header: HashMap<ItemId, Vec<usize>> = HashMap::new();
    for path in txns {
        let mut cur = 0usize; // root
        for &item in path {
            let next = match arena[cur].children.get(&item) {
                Some(&idx) => {
                    arena[idx].count += 1;
                    idx
                }
                None => {
                    let idx = arena.len();
                    arena.push(FpNode {
                        item,
                        count: 1,
                        parent: Some(cur),
                        children: HashMap::new(),
                    });
                    arena[cur].children.insert(item, idx);
                    header.entry(item).or_default().push(idx);
                    idx
                }
            };
            cur = next;
        }
    }

    // Per-item total counts in this (conditional) tree.
    let mut item_counts: HashMap<ItemId, usize> = HashMap::new();
    for (&item, nodes) in &header {
        let c: usize = nodes.iter().map(|&idx| arena[idx].count).sum();
        item_counts.insert(item, c);
    }
    // Process items in a deterministic order (ascending id).
    let mut items: Vec<ItemId> = item_counts.keys().copied().collect();
    items.sort_unstable();

    for item in items {
        let count = item_counts[&item];
        if count < min_count {
            continue;
        }
        // Emit `suffix ∪ {item}` (stored sorted for a canonical key).
        let mut pattern = suffix.to_vec();
        pattern.push(item);
        pattern.sort_unstable();
        out.push(FrequentItemset {
            items: pattern.clone(),
            count,
            support: count as f64 / n as f64,
        });

        // Conditional pattern base: for each node of `item`, the prefix path
        // (root→parent) repeated `node.count` times.
        let mut cond_txns: Vec<Vec<ItemId>> = Vec::new();
        for &leaf in &header[&item] {
            let mut path: Vec<ItemId> = Vec::new();
            let mut p = arena[leaf].parent;
            while let Some(idx) = p {
                if arena[idx].item != ItemId::MAX {
                    path.push(arena[idx].item);
                }
                p = arena[idx].parent;
            }
            path.reverse(); // root→leaf order (FP order preserved)
            let c = arena[leaf].count;
            for _ in 0..c {
                cond_txns.push(path.clone());
            }
        }
        if !cond_txns.is_empty() {
            // New suffix = current pattern (unsorted-suffix order is irrelevant; we
            // re-sort on emit above).
            let mut new_suffix = suffix.to_vec();
            new_suffix.push(item);
            fp_mine(&cond_txns, &new_suffix, min_count, n, out);
        }
    }
}

// ─────────────────────────── Rule generation ───────────────────────────

/// Generate association rules from the frequent itemsets. For every frequent
/// itemset of size ≥ 2 and every non-empty PROPER subset used as the antecedent,
/// emit the rule if its confidence ≥ `min_confidence`. Support of any subset is
/// looked up from the (downward-closed) frequent-itemset map.
pub fn generate_rules(itemsets: &[FrequentItemset], min_confidence: f64) -> Vec<Rule> {
    // Canonical (sorted) key → count, for support lookups of any subset.
    let mut support: HashMap<Vec<ItemId>, usize> = HashMap::new();
    let mut n_est = 0usize;
    for fi in itemsets {
        support.insert(fi.items.clone(), fi.count);
        // Recover the transaction count from any singleton (count / support).
        if fi.items.len() == 1 && fi.support > 0.0 {
            n_est = (fi.count as f64 / fi.support).round() as usize;
        }
    }
    let n = n_est.max(1);

    let mut rules: Vec<Rule> = Vec::new();
    for fi in itemsets {
        if fi.items.len() < 2 {
            continue;
        }
        let full = fi.count;
        // Enumerate every non-empty proper subset as the antecedent (bitmask over
        // the itemset's items — itemsets are small, so 2^k is fine).
        let k = fi.items.len();
        for mask in 1u32..((1u32 << k) - 1) {
            let mut antecedent: Vec<ItemId> = Vec::new();
            let mut consequent: Vec<ItemId> = Vec::new();
            for (bit, &item) in fi.items.iter().enumerate() {
                if mask & (1 << bit) != 0 {
                    antecedent.push(item);
                } else {
                    consequent.push(item);
                }
            }
            let Some(&a_count) = support.get(&antecedent) else {
                continue;
            };
            let Some(&c_count) = support.get(&consequent) else {
                continue;
            };
            let confidence = full as f64 / a_count as f64;
            if confidence + 1e-12 < min_confidence {
                continue;
            }
            let consequent_support = c_count as f64 / n as f64;
            let lift = if consequent_support > 0.0 {
                confidence / consequent_support
            } else {
                0.0
            };
            rules.push(Rule {
                antecedent,
                consequent,
                support: full as f64 / n as f64,
                confidence,
                lift,
            });
        }
    }
    // Stable, useful ordering: by descending confidence, then lift, then support.
    rules.sort_by(|a, b| {
        b.confidence
            .partial_cmp(&a.confidence)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then(
                b.lift
                    .partial_cmp(&a.lift)
                    .unwrap_or(std::cmp::Ordering::Equal),
            )
            .then(
                b.support
                    .partial_cmp(&a.support)
                    .unwrap_or(std::cmp::Ordering::Equal),
            )
            .then(a.antecedent.cmp(&b.antecedent))
            .then(a.consequent.cmp(&b.consequent))
    });
    rules
}

/// Run the chosen frequent-itemset engine and generate rules in one call.
pub fn mine(
    transactions: &[Vec<ItemId>],
    min_support: f64,
    min_confidence: f64,
    algorithm: Algorithm,
) -> (Vec<FrequentItemset>, Vec<Rule>) {
    let mc = min_count(min_support, transactions.len().max(1));
    let mut itemsets = match algorithm {
        Algorithm::Apriori => apriori(transactions, mc),
        Algorithm::FpGrowth => fpgrowth(transactions, mc),
        Algorithm::Eclat => eclat(transactions, mc),
    };
    // Canonicalize order so callers/tests see a stable set regardless of engine.
    itemsets.sort_by(|a, b| {
        a.items
            .len()
            .cmp(&b.items.len())
            .then(a.items.cmp(&b.items))
    });
    let rules = generate_rules(&itemsets, min_confidence);
    (itemsets, rules)
}

// ─────────────────────────── String-labeled convenience ───────────────────────────

/// Intern string transactions into dense [`ItemId`]s, preserving a stable
/// id↔label mapping. Ids are assigned in first-seen order.
pub fn intern(transactions: &[Vec<String>]) -> (Vec<Vec<ItemId>>, Vec<String>) {
    let mut labels: Vec<String> = Vec::new();
    let mut index: HashMap<String, ItemId> = HashMap::new();
    let mut out: Vec<Vec<ItemId>> = Vec::with_capacity(transactions.len());
    for t in transactions {
        let mut row: Vec<ItemId> = Vec::with_capacity(t.len());
        for item in t {
            let id = *index.entry(item.clone()).or_insert_with(|| {
                let id = labels.len() as ItemId;
                labels.push(item.clone());
                id
            });
            row.push(id);
        }
        out.push(row);
    }
    (out, labels)
}

/// Mine string-labeled transactions: intern → mine → relabel. Returns the rules
/// as [`LabeledRule`]s (the row shape the handler and client consume).
pub fn mine_labeled(
    transactions: &[Vec<String>],
    min_support: f64,
    min_confidence: f64,
    algorithm: Algorithm,
) -> Vec<LabeledRule> {
    let (interned, labels) = intern(transactions);
    let (_itemsets, rules) = mine(&interned, min_support, min_confidence, algorithm);
    rules
        .into_iter()
        .map(|r| LabeledRule {
            antecedent: r
                .antecedent
                .iter()
                .map(|&i| labels[i as usize].clone())
                .collect(),
            consequent: r
                .consequent
                .iter()
                .map(|&i| labels[i as usize].clone())
                .collect(),
            support: r.support,
            confidence: r.confidence,
            lift: r.lift,
        })
        .collect()
}

// ─────────────────────────── helpers ───────────────────────────

fn sorted_unique(t: &[ItemId]) -> Vec<ItemId> {
    let mut v = t.to_vec();
    v.sort_unstable();
    v.dedup();
    v
}

/// Whether the sorted `t` contains every element of the sorted `sub` (subset test).
fn contains_sorted(t: &[ItemId], sub: &[ItemId]) -> bool {
    let mut ti = 0;
    for &s in sub {
        while ti < t.len() && t[ti] < s {
            ti += 1;
        }
        if ti >= t.len() || t[ti] != s {
            return false;
        }
        ti += 1;
    }
    true
}

/// Intersect two sorted, deduped tid vectors.
fn intersect_sorted(a: &[usize], b: &[usize]) -> Vec<usize> {
    let mut out = Vec::new();
    let (mut i, mut j) = (0, 0);
    while i < a.len() && j < b.len() {
        match a[i].cmp(&b[j]) {
            std::cmp::Ordering::Less => i += 1,
            std::cmp::Ordering::Greater => j += 1,
            std::cmp::Ordering::Equal => {
                out.push(a[i]);
                i += 1;
                j += 1;
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The classic market-basket fixture (bread=0, butter=1, milk=2), 5 baskets.
    /// Hand-computed truth table (n=5):
    ///   {0}=4/5  {1}=4/5  {2}=4/5
    ///   {0,1}=3/5  {0,2}=3/5  {1,2}=3/5
    ///   {0,1,2}=2/5
    fn fixture() -> Vec<Vec<ItemId>> {
        vec![
            vec![0, 1, 2], // bread, butter, milk
            vec![0, 1],    // bread, butter
            vec![0, 2],    // bread, milk
            vec![1, 2],    // butter, milk
            vec![0, 1, 2], // bread, butter, milk
        ]
    }

    fn support_of(sets: &[FrequentItemset], items: &[ItemId]) -> Option<f64> {
        let mut key = items.to_vec();
        key.sort_unstable();
        sets.iter().find(|f| f.items == key).map(|f| f.support)
    }

    #[test]
    fn apriori_supports_match_hand_computed() {
        let (sets, _) = mine(&fixture(), 0.4, 0.0, Algorithm::Apriori);
        assert_eq!(support_of(&sets, &[0]), Some(0.8));
        assert_eq!(support_of(&sets, &[1]), Some(0.8));
        assert_eq!(support_of(&sets, &[2]), Some(0.8));
        assert_eq!(support_of(&sets, &[0, 1]), Some(0.6));
        assert_eq!(support_of(&sets, &[0, 2]), Some(0.6));
        assert_eq!(support_of(&sets, &[1, 2]), Some(0.6));
        assert_eq!(support_of(&sets, &[0, 1, 2]), Some(0.4));
        // exactly 7 frequent itemsets at min_support 0.4
        assert_eq!(sets.len(), 7);
    }

    #[test]
    fn rule_metrics_match_hand_computed() {
        // {0,1} ⇒ {2}: support(0,1,2)=0.4, conf=0.4/0.6=2/3, lift=(2/3)/0.8=0.8333…
        let (sets, _) = mine(&fixture(), 0.4, 0.0, Algorithm::Apriori);
        let rules = generate_rules(&sets, 0.0);
        let r = rules
            .iter()
            .find(|r| r.antecedent == vec![0, 1] && r.consequent == vec![2])
            .expect("rule {0,1}=>{2} present");
        assert!((r.support - 0.4).abs() < 1e-9);
        assert!((r.confidence - 2.0 / 3.0).abs() < 1e-9);
        assert!((r.lift - (2.0 / 3.0) / 0.8).abs() < 1e-9);

        // {0} ⇒ {1}: support(0,1)=0.6, conf=0.6/0.8=0.75, lift=0.75/0.8=0.9375
        let r2 = rules
            .iter()
            .find(|r| r.antecedent == vec![0] && r.consequent == vec![1])
            .expect("rule {0}=>{1} present");
        assert!((r2.confidence - 0.75).abs() < 1e-9);
        assert!((r2.lift - 0.9375).abs() < 1e-9);
    }

    #[test]
    fn min_confidence_filters_rules() {
        let (sets, _) = mine(&fixture(), 0.4, 0.0, Algorithm::Apriori);
        let all = generate_rules(&sets, 0.0);
        let strict = generate_rules(&sets, 0.7);
        assert!(strict.len() < all.len());
        assert!(strict.iter().all(|r| r.confidence >= 0.7 - 1e-12));
    }

    /// Apriori == FP-Growth == Eclat on the same threshold (the parity gate).
    #[test]
    fn all_three_engines_agree() {
        let txns = fixture();
        let canon = |mut v: Vec<FrequentItemset>| {
            v.sort_by(|a, b| a.items.cmp(&b.items));
            v.into_iter()
                .map(|f| (f.items, f.count))
                .collect::<Vec<_>>()
        };
        let a = canon(apriori(&txns, 2));
        let f = canon(fpgrowth(&txns, 2));
        let e = canon(eclat(&txns, 2));
        assert_eq!(a, f, "apriori vs fp-growth diverged");
        assert_eq!(a, e, "apriori vs eclat diverged");
    }

    /// A larger, denser fixture to exercise deeper itemsets across engines.
    #[test]
    fn engines_agree_on_larger_fixture() {
        let txns = vec![
            vec![0, 1, 2, 3],
            vec![0, 1, 2],
            vec![0, 1, 3],
            vec![0, 2, 3],
            vec![1, 2, 3],
            vec![0, 1],
            vec![2, 3],
            vec![0, 1, 2, 3],
            vec![0, 3],
            vec![1, 2],
        ];
        let canon = |mut v: Vec<FrequentItemset>| {
            v.sort_by(|a, b| a.items.cmp(&b.items));
            v.into_iter()
                .map(|f| (f.items, f.count))
                .collect::<Vec<_>>()
        };
        for mc in 2..=4 {
            let a = canon(apriori(&txns, mc));
            let f = canon(fpgrowth(&txns, mc));
            let e = canon(eclat(&txns, mc));
            assert_eq!(a, f, "fp-growth diverged at min_count {mc}");
            assert_eq!(a, e, "eclat diverged at min_count {mc}");
        }
    }

    #[test]
    fn labeled_roundtrip_produces_string_rules() {
        let txns = vec![
            vec!["bread".into(), "butter".into(), "milk".into()],
            vec!["bread".into(), "butter".into()],
            vec!["bread".into(), "milk".into()],
            vec!["butter".into(), "milk".into()],
            vec!["bread".into(), "butter".into(), "milk".into()],
        ];
        let rules = mine_labeled(&txns, 0.4, 0.5, Algorithm::FpGrowth);
        assert!(!rules.is_empty());
        assert!(rules.iter().all(|r| r.confidence >= 0.5 - 1e-12));
        // every label round-trips to a real item name
        for r in &rules {
            for it in r.antecedent.iter().chain(r.consequent.iter()) {
                assert!(["bread", "butter", "milk"].contains(&it.as_str()));
            }
        }
    }
}
