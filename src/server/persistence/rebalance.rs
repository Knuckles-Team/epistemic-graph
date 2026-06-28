//! Rebalancing planner — CONCEPT:EG-032 (M3 catalog-driven resharding, R3).
//!
//! ## What this is
//!
//! A **pure, deterministic** policy layer that decides WHICH tenants/graphs to move
//! and WHERE, to even out load across the K durable shards. It READS observable
//! shard stats (per-shard, per-graph load) plus the EG-031 tenant catalog and EMITS
//! a [`RebalancePlan`] — an ordered list of `{graph, from_shard, to_shard}` moves.
//!
//! ## What this is NOT
//!
//! It does NOT execute the plan. Moving a populated graph's rows between shards
//! online (snapshot-copy → quiesce → flip the catalog route → GC the source) is
//! **R1 online resharding** (`online_reshard.rs`, sibling-owned — see
//! `docs/architecture/m3-resharding.md`). The planner just produces the plan the
//! operator / R1 consumes, so it is fully parallel-safe with R1/R2/R4 and has NO
//! dependency on M2. It is a pure function over its inputs — same inputs, same plan
//! — which is what makes it unit-testable against synthetic skew.
//!
//! ## Where the live stats come from (integration hook)
//!
//! The planner is decoupled from any live source so it can be tested in isolation.
//! In production the [`ShardLoad`] input is assembled from the EG-026 sharded
//! writer's per-shard stats (`RedbBackend` shard set) plus the KG-2.51 per-graph size
//! Prometheus gauges (`graph_node_count` etc.), grouped by the graph's CURRENT shard
//! (`TenantCatalog::resolve_shard`). That assembly is the one wiring line that belongs
//! in a backend/admin surface; see [`shard_loads_from_graph_loads`] for the pure
//! grouping half, and the "integration follow-up" note in `m3-resharding.md`.

use std::collections::HashMap;

use super::tenant_catalog::TenantCatalog;

/// One graph's observed load on its current shard. `load` is an opaque positive
/// weight — byte size, resident-node count, or an op-rate — whatever metric the
/// caller chose to balance on; the planner only compares and sums them.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GraphLoad {
    /// Sanitized graph key (the SAME key EG-026 / the catalog route on).
    pub graph: String,
    /// The load weight to balance (size / node-count / op-rate). Higher = hotter.
    pub load: u64,
}

/// The observed load of ONE durable shard: its index + the graphs resident on it.
/// The shard's total load is the sum of its graphs' loads.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ShardLoad {
    /// Durable shard index (EG-026 `graph-<shard>.redb`).
    pub shard: u32,
    /// The graphs currently routed to this shard, with their loads.
    pub graphs: Vec<GraphLoad>,
}

impl ShardLoad {
    /// Total load on this shard (sum of its graphs' loads).
    pub fn total(&self) -> u64 {
        self.graphs.iter().map(|g| g.load).sum()
    }
}

/// One planned move: route `graph` from `from_shard` to `to_shard`. The operator /
/// R1 executes it (snapshot-copy the rows, then `catalog.reassign(graph, to_shard)`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReshardMove {
    pub graph: String,
    pub from_shard: u32,
    pub to_shard: u32,
}

/// The emitted plan: an ORDERED list of moves (apply in order — later moves were
/// computed against the running load after the earlier ones). An EMPTY plan means
/// the input is already balanced within tolerance (the no-op / steady-state case).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RebalancePlan {
    pub moves: Vec<ReshardMove>,
}

impl RebalancePlan {
    /// `true` when no move is needed (already balanced within tolerance).
    pub fn is_empty(&self) -> bool {
        self.moves.is_empty()
    }
}

/// Planner knobs.
#[derive(Clone, Copy, Debug)]
pub struct RebalanceOptions {
    /// Stop once the gap between the hottest and coldest shard is `<=` this fraction
    /// of the mean shard load (e.g. `0.10` = within 10% of even). A graph is never
    /// moved if doing so would not strictly shrink the hottest-coldest gap, so the
    /// planner always converges and never thrashes.
    pub tolerance: f64,
    /// Safety cap on the number of moves (defends against a pathological input). The
    /// loop also terminates naturally when no move improves balance.
    pub max_moves: usize,
}

impl Default for RebalanceOptions {
    fn default() -> Self {
        // 10% of mean is a reasonable "balanced enough" band; 4 moves per shard is a
        // generous ceiling (the loop normally stops earlier on convergence).
        RebalanceOptions {
            tolerance: 0.10,
            max_moves: 256,
        }
    }
}

/// Group a flat `(graph, current_shard, load)` list into per-shard [`ShardLoad`]s
/// for `k` shards (the pure half of the live-stats assembly). Shards with no graphs
/// are still represented (load 0) so the planner can move work ONTO an empty shard.
/// Deterministic: shards are index-ordered and each shard's graphs are sorted by
/// `(load desc, graph asc)`.
pub fn shard_loads_from_graph_loads(
    graph_loads: &[(String, u32, u64)],
    k: usize,
) -> Vec<ShardLoad> {
    let k = k.max(1);
    let mut by_shard: HashMap<u32, Vec<GraphLoad>> = HashMap::new();
    for (graph, shard, load) in graph_loads {
        let s = (*shard as usize % k) as u32;
        by_shard.entry(s).or_default().push(GraphLoad {
            graph: graph.clone(),
            load: *load,
        });
    }
    (0..k as u32)
        .map(|s| {
            let mut graphs = by_shard.remove(&s).unwrap_or_default();
            sort_graphs(&mut graphs);
            ShardLoad { shard: s, graphs }
        })
        .collect()
}

/// Assemble [`ShardLoad`]s from observed per-graph loads, routing each graph by the
/// EG-031 catalog (explicit assignment wins, else EG-026 FNV-1a) — the integration
/// seam between the live catalog and the pure planner. `graph_loads` is
/// `(graph_fname, load)`; `k` is the live shard count.
pub fn shard_loads_from_catalog(
    catalog: &TenantCatalog,
    graph_loads: &[(String, u64)],
    k: usize,
) -> Vec<ShardLoad> {
    let routed: Vec<(String, u32, u64)> = graph_loads
        .iter()
        .map(|(g, load)| (g.clone(), catalog.resolve_shard(g, k) as u32, *load))
        .collect();
    shard_loads_from_graph_loads(&routed, k)
}

/// Deterministic graph ordering: heaviest first, ties broken by name. Used both when
/// grouping shards and when picking a graph to move, so the plan is reproducible.
fn sort_graphs(graphs: &mut [GraphLoad]) {
    graphs.sort_by(|a, b| b.load.cmp(&a.load).then_with(|| a.graph.cmp(&b.graph)));
}

/// Produce a deterministic rebalance plan that evens out shard load (CONCEPT:EG-032).
///
/// Greedy hottest→coldest: repeatedly move ONE graph off the most-loaded shard onto
/// the least-loaded shard, choosing the graph that most reduces the hottest-coldest
/// gap WITHOUT overshooting (moving `w` changes the gap from `H-C` to `|H-C-2w|`, so
/// the ideal `w` is `(H-C)/2`; among the shard's graphs we pick the one closest to
/// that, breaking ties toward the larger graph then the lexicographically smaller
/// name). Stops when the gap falls within `tolerance·mean`, when no single move
/// strictly shrinks the gap, or at `max_moves`. Minimizing the gap per step keeps the
/// move COUNT low (cheap to execute) while still converging to an even spread.
///
/// Pure + deterministic: same input ⇒ same plan. Does not mutate the inputs.
pub fn plan_rebalance(shards: &[ShardLoad], opts: RebalanceOptions) -> RebalancePlan {
    let k = shards.len();
    if k < 2 {
        return RebalancePlan::default(); // nothing to balance across <2 shards.
    }

    // Working copy: per-shard (shard_index, graphs). Loads are recomputed from the
    // graphs so a move is reflected immediately.
    let mut work: Vec<(u32, Vec<GraphLoad>)> = shards
        .iter()
        .map(|s| {
            let mut g = s.graphs.clone();
            sort_graphs(&mut g);
            (s.shard, g)
        })
        .collect();

    let total: u64 = work.iter().map(|(_, g)| sum_load(g)).sum();
    let mean = total as f64 / k as f64;
    let tol_abs = (opts.tolerance.max(0.0) * mean).max(1.0);

    let mut plan = RebalancePlan::default();
    while plan.moves.len() < opts.max_moves {
        // Hottest = max total; coldest = min total. Deterministic tie-break by index.
        let hot = argmax_load(&work);
        let cold = argmin_load(&work);
        if hot == cold {
            break;
        }
        let h_load = sum_load(&work[hot].1);
        let c_load = sum_load(&work[cold].1);
        let gap = h_load.saturating_sub(c_load);
        if (gap as f64) <= tol_abs {
            break; // balanced enough.
        }

        // Pick the graph on the hottest shard whose move most shrinks the gap. New
        // gap after moving load w = |gap - 2w|; only accept moves that strictly
        // reduce it (w in 0 < w < gap, i.e. w*2 < gap*2 and new_gap < gap).
        let pick = best_move_graph(&work[hot].1, gap);
        let Some(idx) = pick else {
            break; // no single graph improves balance — converged.
        };

        let g = work[hot].1.remove(idx);
        let from_shard = work[hot].0;
        let to_shard = work[cold].0;
        // Insert into the cold shard keeping it sorted (heaviest first).
        work[cold].1.push(g.clone());
        sort_graphs(&mut work[cold].1);
        plan.moves.push(ReshardMove {
            graph: g.graph,
            from_shard,
            to_shard,
        });
    }
    plan
}

fn sum_load(graphs: &[GraphLoad]) -> u64 {
    graphs.iter().map(|g| g.load).sum()
}

/// Index of the most-loaded shard; ties broken toward the lower shard index.
fn argmax_load(work: &[(u32, Vec<GraphLoad>)]) -> usize {
    let mut best = 0usize;
    let mut best_load = sum_load(&work[0].1);
    for (i, (_, g)) in work.iter().enumerate().skip(1) {
        let l = sum_load(g);
        if l > best_load {
            best_load = l;
            best = i;
        }
    }
    best
}

/// Index of the least-loaded shard; ties broken toward the lower shard index.
fn argmin_load(work: &[(u32, Vec<GraphLoad>)]) -> usize {
    let mut best = 0usize;
    let mut best_load = sum_load(&work[0].1);
    for (i, (_, g)) in work.iter().enumerate().skip(1) {
        let l = sum_load(g);
        if l < best_load {
            best_load = l;
            best = i;
        }
    }
    best
}

/// Among `graphs`, the index of the one whose move best shrinks `gap` (new gap
/// `|gap - 2·load|`), accepting only a STRICT improvement. Returns `None` when no
/// graph strictly reduces the gap (e.g. every graph is heavier than the gap, so
/// moving it just flips the imbalance). Deterministic: minimizes `|gap - 2·load|`,
/// ties toward the heavier graph then the lexicographically smaller name (the input
/// is pre-sorted heaviest-first, so the first qualifying minimum wins).
fn best_move_graph(graphs: &[GraphLoad], gap: u64) -> Option<usize> {
    let mut best: Option<(usize, u64)> = None; // (index, resulting_gap)
    for (i, g) in graphs.iter().enumerate() {
        if g.load == 0 {
            continue; // moving a zero-load graph never helps.
        }
        let two_w = g.load.saturating_mul(2);
        let new_gap = (gap as i128 - two_w as i128).unsigned_abs() as u64;
        if new_gap >= gap {
            continue; // not a strict improvement (overshoots or no-op).
        }
        match best {
            Some((_, bg)) if new_gap >= bg => {}
            _ => best = Some((i, new_gap)),
        }
    }
    best.map(|(i, _)| i)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn gl(name: &str, load: u64) -> GraphLoad {
        GraphLoad {
            graph: name.to_string(),
            load,
        }
    }

    /// Apply a plan to the input shards and return the per-shard totals afterward —
    /// lets a test assert the plan actually balances.
    fn apply(shards: &[ShardLoad], plan: &RebalancePlan) -> Vec<u64> {
        // graph -> (load, current_shard)
        let mut place: HashMap<String, (u64, u32)> = HashMap::new();
        for s in shards {
            for g in &s.graphs {
                place.insert(g.graph.clone(), (g.load, s.shard));
            }
        }
        for m in &plan.moves {
            let e = place.get_mut(&m.graph).expect("moved graph exists");
            assert_eq!(e.1, m.from_shard, "move's from_shard matches current");
            e.1 = m.to_shard;
        }
        let k = shards.len();
        let mut totals = vec![0u64; k];
        // shard index in input order
        let order: HashMap<u32, usize> = shards
            .iter()
            .enumerate()
            .map(|(i, s)| (s.shard, i))
            .collect();
        for (load, shard) in place.values() {
            totals[order[shard]] += load;
        }
        totals
    }

    fn imbalance(totals: &[u64]) -> u64 {
        totals.iter().max().unwrap() - totals.iter().min().unwrap()
    }

    #[test]
    fn balances_a_skewed_shard_set_deterministically() {
        // CONCEPT:EG-032 — a heavily skewed K=4 input: shard 0 holds everything. The
        // graphs are INDIVISIBLE, so the best achievable imbalance is floored by the
        // single largest graph (100) — the planner drives toward that floor, never
        // splitting a graph or thrashing.
        let shards = vec![
            ShardLoad {
                shard: 0,
                graphs: vec![
                    gl("a", 100),
                    gl("b", 80),
                    gl("c", 60),
                    gl("d", 40),
                    gl("e", 20),
                ],
            },
            ShardLoad {
                shard: 1,
                graphs: vec![],
            },
            ShardLoad {
                shard: 2,
                graphs: vec![],
            },
            ShardLoad {
                shard: 3,
                graphs: vec![],
            },
        ];
        let before = apply(&shards, &RebalancePlan::default());
        assert_eq!(imbalance(&before), 300, "starts fully skewed (300 vs 0)");

        let plan = plan_rebalance(&shards, RebalanceOptions::default());
        assert!(!plan.is_empty(), "a skewed set yields moves");
        // Every move comes off the originally-hot shard 0 (it is the only loaded one).
        assert!(plan.moves.iter().all(|m| m.from_shard == 0));

        let after = apply(&shards, &plan);
        let max_graph = 100u64;
        assert!(
            imbalance(&after) < imbalance(&before),
            "plan strictly reduces imbalance {:?}",
            after
        );
        assert!(
            imbalance(&after) <= max_graph,
            "rebalanced spread {after:?} is floored by the largest indivisible graph"
        );

        // Determinism: re-planning the same input yields the identical plan.
        let plan2 = plan_rebalance(&shards, RebalanceOptions::default());
        assert_eq!(plan, plan2, "planner is deterministic");
    }

    #[test]
    fn divisible_load_reaches_tolerance() {
        // CONCEPT:EG-032 — when the load IS evenly divisible (12 graphs of 25 across
        // K=4, mean 75), the planner balances to within the default 10% tolerance.
        let graphs: Vec<GraphLoad> = (0..12).map(|i| gl(&format!("g{i:02}"), 25)).collect();
        let shards = vec![
            ShardLoad { shard: 0, graphs },
            ShardLoad {
                shard: 1,
                graphs: vec![],
            },
            ShardLoad {
                shard: 2,
                graphs: vec![],
            },
            ShardLoad {
                shard: 3,
                graphs: vec![],
            },
        ];
        let plan = plan_rebalance(&shards, RebalanceOptions::default());
        let after = apply(&shards, &plan);
        assert!(
            imbalance(&after) <= (0.10 * 75.0) as u64 + 1,
            "evenly divisible load balances within tolerance, got {after:?}"
        );
    }

    #[test]
    fn already_balanced_yields_empty_plan() {
        let shards = vec![
            ShardLoad {
                shard: 0,
                graphs: vec![gl("a", 50)],
            },
            ShardLoad {
                shard: 1,
                graphs: vec![gl("b", 50)],
            },
        ];
        let plan = plan_rebalance(&shards, RebalanceOptions::default());
        assert!(plan.is_empty(), "even input ⇒ no moves");
    }

    #[test]
    fn single_shard_is_a_noop() {
        let shards = vec![ShardLoad {
            shard: 0,
            graphs: vec![gl("a", 100), gl("b", 50)],
        }];
        assert!(plan_rebalance(&shards, RebalanceOptions::default()).is_empty());
    }

    #[test]
    fn indivisible_hot_graph_does_not_thrash() {
        // One graph holds the whole shard's load and is heavier than the gap target;
        // moving it would just flip the imbalance, so the planner leaves it (no
        // infinite move loop), and may relocate it once if that strictly helps.
        let shards = vec![
            ShardLoad {
                shard: 0,
                graphs: vec![gl("whale", 1000)],
            },
            ShardLoad {
                shard: 1,
                graphs: vec![gl("x", 10)],
            },
        ];
        let plan = plan_rebalance(&shards, RebalanceOptions::default());
        // Moving "whale" makes gap |990 - 2000| = 1010 > 990 ⇒ not accepted; "x" off
        // shard1 doesn't help shard0. So no move strictly improves ⇒ empty.
        assert!(plan.is_empty(), "no thrash on an indivisible hot graph");
    }

    #[test]
    fn grouping_routes_and_fills_empty_shards() {
        let loads = vec![
            ("a".to_string(), 0u32, 10u64),
            ("b".to_string(), 0u32, 20u64),
            ("c".to_string(), 2u32, 5u64),
        ];
        let shards = shard_loads_from_graph_loads(&loads, 4);
        assert_eq!(shards.len(), 4, "all K shards represented incl. empties");
        assert_eq!(shards[0].total(), 30);
        assert_eq!(shards[1].total(), 0);
        assert_eq!(shards[2].total(), 5);
        // Sorted heaviest-first within a shard.
        assert_eq!(shards[0].graphs[0].graph, "b");
    }

    #[test]
    fn catalog_routing_feeds_the_planner() {
        // EG-031 integration seam: an empty catalog routes by FNV-1a; the planner then
        // sees those placements. Just assert the assembly is consistent with routing.
        let cat = TenantCatalog::in_memory();
        let loads = vec![("g1".to_string(), 7u64), ("g2".to_string(), 3u64)];
        let shards = shard_loads_from_catalog(&cat, &loads, 4);
        let placed: u64 = shards.iter().map(|s| s.total()).sum();
        assert_eq!(placed, 10, "all load is placed once");
        // Each graph lands in its catalog-resolved shard.
        for (g, load) in &loads {
            let s = cat.resolve_shard(g, 4) as u32;
            let sh = shards.iter().find(|x| x.shard == s).unwrap();
            assert!(sh.graphs.iter().any(|x| &x.graph == g && x.load == *load));
        }
    }
}
