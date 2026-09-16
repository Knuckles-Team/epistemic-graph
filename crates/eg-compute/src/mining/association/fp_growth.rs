//! FP-Growth frequent-itemset engine for the association-mining namespace.

use std::collections::HashMap;

use super::{cancelled, sorted_unique, FrequentItemset, ItemId};

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
        if cancelled() {
            return Vec::new();
        }
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

fn build_fp_tree(txns: &[Vec<ItemId>]) -> Option<(Vec<FpNode>, HashMap<ItemId, Vec<usize>>)> {
    let mut arena: Vec<FpNode> = vec![FpNode {
        item: ItemId::MAX,
        count: 0,
        parent: None,
        children: HashMap::new(),
    }];
    let mut header: HashMap<ItemId, Vec<usize>> = HashMap::new();
    for path in txns {
        if cancelled() {
            return None;
        }
        let mut cur = 0usize;
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
    Some((arena, header))
}

fn fp_item_counts(
    arena: &[FpNode],
    header: &HashMap<ItemId, Vec<usize>>,
) -> HashMap<ItemId, usize> {
    header
        .iter()
        .map(|(&item, nodes)| {
            let count = nodes.iter().map(|&idx| arena[idx].count).sum();
            (item, count)
        })
        .collect()
}

fn fp_conditional_transactions(
    item: ItemId,
    arena: &[FpNode],
    header: &HashMap<ItemId, Vec<usize>>,
) -> Option<Vec<Vec<ItemId>>> {
    let mut conditional = Vec::new();
    for &leaf in &header[&item] {
        if cancelled() {
            return None;
        }
        let mut path: Vec<ItemId> = Vec::new();
        let mut parent = arena[leaf].parent;
        while let Some(idx) = parent {
            if arena[idx].item != ItemId::MAX {
                path.push(arena[idx].item);
            }
            parent = arena[idx].parent;
        }
        path.reverse();
        for _ in 0..arena[leaf].count {
            conditional.push(path.clone());
        }
    }
    Some(conditional)
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
    let Some((arena, header)) = build_fp_tree(txns) else {
        return;
    };
    let item_counts = fp_item_counts(&arena, &header);
    // Process items in a deterministic order (ascending id).
    let mut items: Vec<ItemId> = item_counts.keys().copied().collect();
    items.sort_unstable();

    for item in items {
        if cancelled() {
            return;
        }
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
        let Some(cond_txns) = fp_conditional_transactions(item, &arena, &header) else {
            return;
        };
        if !cond_txns.is_empty() {
            // New suffix = current pattern (unsorted-suffix order is irrelevant; we
            // re-sort on emit above).
            let mut new_suffix = suffix.to_vec();
            new_suffix.push(item);
            fp_mine(&cond_txns, &new_suffix, min_count, n, out);
        }
    }
}
