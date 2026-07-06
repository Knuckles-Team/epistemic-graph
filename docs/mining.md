# Data Mining — the `graph_mine` / `/api/mining` surface

> CONCEPT:EG-KG.mining.frequent-itemset-mining
>
> A unified, cross-modal **data-mining** surface on the engine. Every algorithm
> runs **compute-near-data** — one round-trip over data already resident in the
> graph/snapshot — and mined patterns **write back** into the KG as typed nodes for
> OWL reasoning and the next mining pass (the discovery flywheel).

Phase 1 ships **association-rule mining** (frequent itemsets + rules with
**support / confidence / lift**). Later phases add clustering, anomaly detection,
sequential patterns, forecasting, and frequent-subgraph mining onto this *same*
surface — so every later phase is "add an algorithm", not "add a surface".

## The one surface, five layers

| Layer | Where |
|-------|-------|
| Engine impl | `crates/eg-compute/src/mining/association.rs` — Apriori, FP-Growth, Eclat (all exact, all agree) + rule generation |
| Protocol | `Method::MineAssociate` in the `// ── Mining ──` section of `crates/eg-types/src/protocol.rs` (feature `mining`) |
| Handler | `src/server/handlers/mining.rs` — graph-derived transactions + write-back over the live `GraphCore` |
| Client | `client.mining.associate(...)` (`epistemic_graph/client.py`) |
| graph-os MCP | `graph_mine action="associate"` (agent-utilities `engine_surface_tools.py`) — plus the granular `engine_mining` verb |
| REST twin | `POST /api/mining/associate` (agent-utilities `graph_api.py`, same `_execute_tool` core — surface parity is a build gate) |

## Algorithms

Three interchangeable frequent-itemset engines, selected by `algorithm`:

- **`fpgrowth`** (default) — frequency-ordered prefix tree (FP-tree) + conditional
  pattern bases; no candidate generation, fastest on dense baskets.
- **`apriori`** — breadth-first, level-wise candidate generation + downward-closure prune.
- **`eclat`** — depth-first over the vertical layout (transaction-id-set intersection).

All three are exact and, for the same `min_support`, produce the **same** frequent
itemsets (asserted by a parity test), so rule generation is shared. The metrics:

- **support**(A∪C) — fraction of transactions holding every item.
- **confidence**(A⇒C) = support(A∪C) / support(A) = P(C | A).
- **lift**(A⇒C) = confidence / support(C) — `>1` ⇒ positively correlated.

## Two ways in: explicit transactions or a graph-derived source

`MineAssociate` accepts **either** an explicit `transactions` list **or** a
graph-derived `source` — never both (explicit wins).

```python
from epistemic_graph.client import SyncEpistemicGraphClient
c = SyncEpistemicGraphClient.connect(socket_path="/run/epistemic-graph/shard-0.sock")

# (1) explicit market-basket transactions
res = c.mining.associate(
    [["bread", "butter", "milk"],
     ["bread", "butter"],
     ["bread", "milk"],
     ["butter", "milk"],
     ["bread", "butter", "milk"]],
    min_support=0.4, min_confidence=0.5, algorithm="fpgrowth",
)
for r in res["rules"]:
    print(r["antecedent"], "=>", r["consequent"],
          f"conf={r['confidence']:.2f} lift={r['lift']:.2f}")
# {bread,butter} => {milk}  conf=0.67 lift=0.83  …
```

The **graph-derived** path (`source`) turns node neighborhoods into transactions —
this is the cross-modal hook. The spec:

| field | meaning |
|-------|---------|
| `node_label` | the label whose instances each become one basket owner |
| `direction` | `out` (successors, default) · `in` (predecessors) · `any` |
| `item_field` | `label` (neighbor's type) · `prop:<key>` (a neighbor property) · omit ⇒ the neighbor's node id |
| `relation` | only follow edges whose `relation`/`type` property equals this |
| `limit` | cap the basket owners scanned (0 = uncapped) |

## Cross-modal example — mine over a graph neighborhood

Because the source reads resident graph data, `retrieve → mine` is one fused,
compute-near-data operation (no second round-trip, no client-side marshalling):

```python
# For each :Doc, mine which cited topics co-occur — traversing the CITES graph.
res = c.mining.associate(
    source={"node_label": "Doc", "direction": "out",
            "item_field": "prop:topic", "relation": "CITES"},
    min_support=0.1, min_confidence=0.6, algorithm="fpgrowth",
    writeback=True,   # materialize :AssociationRule nodes
)
```

No vector-only or stitched stack can express "traverse a neighborhood, then mine
frequent patterns over its labels/edges, then write the patterns back" as one plan.

## Write-back — the discovery flywheel

With `writeback=true`, each rule is materialized as a typed `:AssociationRule`
node (deterministic id, so re-mining is idempotent) carrying `antecedent`,
`consequent`, `support`, `confidence`, `lift` as queryable properties, and linked
(`RULE_ITEM` edges) to any item that is itself a resident node. OWL reasoning,
retrieval, and the *next* mining pass then consume these nodes — closing knowledge
discovery back into the graph.

```python
res = c.mining.associate(..., writeback=True)
rules = c.nodes.list_by_label("AssociationRule", 0)   # queryable typed nodes
```

## First consumer — agent-utilities-evolution (concept↔capability rules)

The headline use case: mine **concept↔capability co-occurrence** to auto-suggest
implementations. Seed a small graph where each `Paper` TOUCHES some `Concept`s and
IMPLEMENTS a `Capability`; each paper's neighborhood is one transaction over
`{concepts ∪ capability}`, and the mined rules read like
`{concept A, concept B} ⇒ capability Z` ("papers touching A and B usually
implement Z"):

```python
from epistemic_graph.client import SyncEpistemicGraphClient
c = SyncEpistemicGraphClient.connect(socket_path="/run/epistemic-graph/shard-0.sock")
c.graph.clear()

concepts = {"cA": "concept:cA", "cB": "concept:cB", "cC": "concept:cC"}
caps = {"capX": "capability:capX", "capZ": "capability:capZ"}
for nid in concepts.values(): c.nodes.add(nid, {"type": "Concept"})
for nid in caps.values():     c.nodes.add(nid, {"type": "Capability"})

papers = {
    "p1": (["cA", "cB"], "capZ"), "p2": (["cA", "cB"], "capZ"),
    "p3": (["cA", "cB"], "capZ"), "p4": (["cA", "cC"], "capX"),
    "p5": (["cB", "cC"], "capX"),
}
for pid, (cs, cap) in papers.items():
    c.nodes.add(pid, {"type": "Paper"})
    for x in cs: c.edges.add(pid, concepts[x], {"relation": "TOUCHES"})
    c.edges.add(pid, caps[cap], {"relation": "IMPLEMENTS"})

# Mine each Paper's neighborhood (concepts + capability) → co-occurrence rules,
# and write the rules back as :AssociationRule nodes.
res = c.mining.associate(
    source={"node_label": "Paper", "direction": "out"},
    min_support=0.4, min_confidence=0.9, algorithm="fpgrowth", writeback=True,
)
for r in res["rules"]:
    if set(r["antecedent"]) == {"concept:cA", "concept:cB"}:
        print(r["antecedent"], "=>", r["consequent"],
              f"conf={r['confidence']:.2f} lift={r['lift']:.2f}")
# ['concept:cA', 'concept:cB'] => ['capability:capZ']  conf=1.00 lift=1.67
print("wrote back", res["written_back"], ":AssociationRule nodes")
```

`graph_loops` + the `agent-utilities-expert` already read typed KG nodes, so the
mined `:AssociationRule` nodes feed the evolution queue directly.

## MCP + REST

```jsonc
// MCP (graph-os multiplexer)
graph_mine { "action": "associate",
             "params_json": "{\"transactions\":[[\"a\",\"b\"],[\"a\",\"c\"]],\"min_support\":0.5}" }

// REST twin (same _execute_tool core)
POST /api/mining/associate
{ "transactions": [["a","b"],["a","c"]], "min_support": 0.5, "algorithm": "fpgrowth" }
```

Both dispatch the identical engine call — surface parity (MCP ⇄ REST) is enforced as
a ship-together gate, not a follow-up.
