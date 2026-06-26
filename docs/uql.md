# UQL — the Unified Query Language

UQL is epistemic-graph's human- and agent-writable query language. It is a **pure
front-end** (CONCEPT:KG-2.214) over the engine's cross-modal plan algebra (CONCEPT:KG-2.208):
a UQL string parses to the *exact same* `wire::Plan` (an ordered `Vec<Op>`) that the
structured `UnifiedQuery` API executes — it adds **no** new execution path. The proof is
in the planner tests: a UQL string parses to the byte-identical plan a hand-built test
constructs, and the served query returns the same result as the structured plan *and* the
separate-surfaces oracle.

The parser is dependency-free (no DataFusion, no regex), so the whole front-end ships even
in a Pi/`--no-default-features` build — only *execution* of a given stage is feature-gated.

## The mental model: one pipeline, one RowSet currency

A UQL query is a **pipeline**. It starts with a *source* (`MATCH …`) that seeds a set of
candidate node ids, then threads that set through a sequence of `|>`-separated **stages**.
Every stage is a function `(RowSet) -> RowSet` over the cross-modal currency — a `RowSet`
is an ordered list of `(id, optional score)` rows — so SQL, graph, vector, text, temporal,
reasoning, and federation stages **compose with no impedance mismatch**. The whole pipeline
runs over **one off-lock snapshot** at a single engine version, so a cross-modal read is
snapshot-isolated for free (CONCEPT:KG-2.180).

```
MATCH (:Doc) WHERE year > 2024            # source + relational filter
  |> TRAVERSE -[:CITES]->{1,2}            # graph traversal (1..2 hops)
  |> RANK BY ~[0.1, 0.9, 0.0]             # vector re-rank by similarity
  |> AS OF @1700000000                    # keep only facts live at that instant
  |> RERANK MMR 0.5 10                    # diversify the top results
  |> LIMIT 10
```

## Grammar (EBNF)

```text
query      = source { "|>" stage } ;
source     = "MATCH" "(" [ ":" ] label ")" [ "WHERE" pred_list ]   (* property-graph scan *)
           | "REASON" class                                        (* OWL-inferred members *)
           | "FOREIGN" string ;                                     (* external source seed  *)
stage      = filter | traverse | rank | text | fuse | rerank
           | asof | window | foreign | reason | limit ;
filter     = "WHERE" pred_list ;
traverse   = "TRAVERSE" edge ;
edge       = "-" "[" ":" rel "]" "->" [ hop_range ]
           | rel [ hop_range ] ;                                    (* bare-rel shorthand *)
hop_range  = "{" int [ ( "," | ".." ) int ] "}" ;                   (* {2}={2,2}; {1,3}   *)
rank       = "RANK" "BY" "~" vector_ref ;
vector_ref = "[" num { "," num } "]" ;                              (* inline literal vec *)
text       = "TEXT" string ;                                        (* BM25 lexical rank  *)
fuse       = "FUSE" branch { branch } ;                             (* N-way RRF hybrid   *)
branch     = "[" stage { "|>" stage } "]" ;
rerank     = "RERANK" ( "NODE_DISTANCE" "FROM" id
                      | "MENTIONS"
                      | "MMR" num int ) ;                           (* graph-native rerankers *)
asof       = "AS" "OF" [ "TX" | "VALID" ] "@" num ;                 (* bi-temporal point-in-time *)
window     = "WINDOW" num [ unit ] ;                                (* s | m | h | d *)
foreign    = "FOREIGN" string ;
reason     = "REASON" class ;
limit      = "LIMIT" int ;
pred_list  = pred { "AND" pred } ;
pred       = prop ( ">" | "<" | ( "=" | "==" ) ) value ;
value      = num | string | ident ;
id         = ident | string ;                                      (* QUOTE ids with - . : @ *)
```

## Stages

### Source stages (seed the RowSet)

| Clause | Op | Feature | Meaning |
|--------|-----|---------|---------|
| `MATCH (:Label)` | `Scan{label}` | base | Seed every node whose `type == Label`. |
| `MATCH (:Label) WHERE p…` | `Scan` + `Filter` | `query` | Inline `WHERE` is sugar for a following filter. |
| `REASON <Class>` | `Reason{target_class}` | `owl` | Seed every individual the OWL 2 reasoner **infers** to be a member of `<Class>` — including those with no asserted type edge. |
| `FOREIGN "<name>"` | `Foreign{name}` | base (resolve: `federation`) | Seed from a registered external source (remote engine / HTTP-JSON / SQL). |

### Transform stages

#### `WHERE` — relational filter (real DataFusion)
`WHERE year > 2024 AND lang = 'en'` → `Filter{preds}` (feature `query`). Compiled to a SQL
`WHERE` over the schema-on-read `nodes` provider and evaluated by DataFusion. When it follows
a prior stage, it is pushed down as `id IN (…)` over just the current candidates. Predicates:
`>` / `<` (numeric), `=` / `==` (string equality).

#### `TRAVERSE` — graph hops (petgraph BFS)
`TRAVERSE -[:CITES]->{1,2}` (or the bare-rel form `TRAVERSE CITES {1,2}`) → `Traverse{rel,min,max}`.
Follows outgoing `rel` edges for `min..=max` hops. `{2}` means exactly 2; `{1,3}` means 1–3;
omitted range means 1 hop. The relationship is matched against the edge's stored
`relationship`/`type` blob field (same as Cypher's `rel_matches`).

#### `RANK BY` — vector re-rank
`RANK BY ~[0.1,0.9,0.0]` → `Rank{query}` (feature `query`). Re-orders the current candidates
by cosine similarity to the inline literal query vector (kNN over the `SemanticStore`). The
`~` sigil marks a vector. *(A named embedding handle — `~"some text"` / `~handle` — is a
reserved forward seam and currently errors: there is no server-side embedder resolver yet.
Embed in the client and pass the literal vector.)*

#### `TEXT` — lexical (BM25) re-rank
`TEXT "graph database"` → `RankText{query}` (feature `text`). Re-orders candidates by BM25
relevance to the query string over the lexical index. With no text index configured the
result is empty (degrade, never error). Sibling of the vector `RANK BY`.

#### `FUSE` — N-way hybrid (reciprocal-rank fusion)
`FUSE [ RANK BY ~[…] ] [ TEXT "…" ] [ RERANK NODE_DISTANCE FROM "x" ]` →
`FuseRrf{branches,k}` (feature `text`, CONCEPT:KG-2.253). Runs each bracketed **sub-pipeline**
over the *same* seed, then reciprocal-rank-fuses their ranked id lists into one result. RRF
fuses the **ranks** (not the incomparable cosine/BM25/distance scores), so a node strong
across *more* branches out-ranks one strong in only one — the property that makes the fused
query beat any single modality alone. Generalized past two legs: any number of branches.

#### `RERANK` — graph-native + diversity rerankers (CONCEPT:KG-2.254 / KG-2.255)
Re-score the current candidates without leaving the engine:

| Clause | Op | Meaning |
|--------|-----|---------|
| `RERANK NODE_DISTANCE FROM <id>` | `RankNodeDistance{center}` | Inverse shortest-path hop distance from a focal node (`1/(1+hops)`; unreachable → 0). Proximity-to-`<id>` ranking. |
| `RERANK MENTIONS` | `RankMentions{}` | Provenance salience: incoming-edge count, normalized to the set max. A node many things point at ranks higher. |
| `RERANK MMR <lambda> <k>` | `RankMmr{lambda,k}` | Maximal Marginal Relevance: greedily trade relevance vs. cosine similarity to already-picked items, demoting near-duplicates. `lambda∈[0,1]` (1 = pure relevance, 0 = pure diversity); `k` caps how many to re-rank (0 = all). |

All three are dependency-free and run under the base `query` feature.

#### `AS OF` — bi-temporal point-in-time (CONCEPT:KG-2.250)
`AS OF @1700000000` → `AsOf{ts, axis=Valid}`. Drops every row **not live** at the unix-seconds
instant `ts`, using a half-open window `[from, until)`:

- `AS OF @t` / `AS OF VALID @t` — **valid (event) time**: "what was **TRUE** at `t`" — filters
  `valid_from`/`valid_until`.
- `AS OF TX @t` — **transaction time**: "what we **BELIEVED** at `t`" — filters `tx_from`/`tx_to`.

Order-preserving: a `RANK …` then `AS OF` keeps the ranked survivors in rank order. When it is
the first stage it acts as a source (every node live at `t`). Dep-free (no DataFusion), so it
runs in the Pi tier. The two axes give the headline bi-temporal pair in one grammar — see
[Bi-temporal facts](architecture/engine.md).

#### `WINDOW` — trailing time window
`WINDOW 1 h` (or `30 m`, `7 d`, bare seconds) → `Window{secs}`. Declares a trailing window for a
windowed time-series aggregate; pairs with `AS OF`. A RowSet-preserving context op today (the
windowed aggregate is the eg-tsdb seam).

#### `LIMIT`
`LIMIT 10` → `Limit{k}`. Order-respecting top-k.

## Op mapping (cheat sheet)

| UQL | `wire::Op` |
|-----|-----------|
| `MATCH (:Doc)` | `Scan{label:"Doc"}` |
| `WHERE year > 2024 AND lang = 'en'` | `Filter{preds:[GtNum, Eq]}` |
| `TRAVERSE -[:CITES]->{1,2}` | `Traverse{rel:"CITES",min:1,max:2}` |
| `RANK BY ~[1.0,0.0]` | `Rank{query:[1.0,0.0]}` |
| `TEXT "graphs"` | `RankText{query:"graphs"}` |
| `FUSE [..] [..]` | `FuseRrf{branches:[..],k}` |
| `RERANK NODE_DISTANCE FROM "n1"` | `RankNodeDistance{center:"n1"}` |
| `RERANK MENTIONS` | `RankMentions{}` |
| `RERANK MMR 0.5 10` | `RankMmr{lambda:0.5,k:10}` |
| `AS OF @t` / `AS OF TX @t` | `AsOf{ts:t,axis:Valid|Transaction}` |
| `WINDOW 1 h` | `Window{secs:3600}` |
| `FOREIGN "peer"` | `Foreign{name:"peer"}` |
| `REASON Mammal` | `Reason{target_class:"Mammal"}` |
| `LIMIT 10` | `Limit{k:10}` |

## Running a query

**Python client** (the front-end over `unified`):

```python
from epistemic_graph.client import EpistemicGraphClient

c = await EpistemicGraphClient.connect(
    socket_path="/run/epistemic-graph/epistemic-graph.sock", graph_name="__commons__")
rows = await c.query.uql("MATCH (:Concept) |> AS OF @1700000000 |> RERANK MMR 0.5 5 |> LIMIT 5")
```

**MCP / REST** — the served `graph_query` / `graph_search` surfaces accept UQL through the same
`unified` core, so an agent runs UQL with no extra wiring.

## Gotchas

- **Quote ids that contain `-`, `.`, `:` or `@`.** The lexer tokenizes `-` as its own symbol,
  so `RERANK NODE_DISTANCE FROM kg-2.0` parses `kg` then a stray `-` and errors with
  *"unexpected trailing tokens, found `-`"*. Quote it: `RERANK NODE_DISTANCE FROM "kg-2.0"`.
  Concept ids, namespaced labels, and timestamps-as-ids all need quoting.
- **`AS OF` windows are half-open `[from, until)`, in unix seconds.** `AS OF @200` excludes a
  fact whose `valid_until == 200`. A missing `valid_from` reads as "has always been" (0); a
  missing `valid_until` reads as "still current".
- **`FUSE` fuses ranks, not scores** — don't expect the fused score to be a blend of cosine and
  BM25; it is `Σ 1/(k+rank)` across branches (`k=60` by convention; `0` ⇒ that default).
- **A bad `RERANK` mode errors with a help string** listing the valid forms — that is the parser
  validating, not a bug.
- **Feature gating is on *execution*, not parsing.** A build without `text` parses `FUSE`/`TEXT`
  but errors at run time ("not in this build"); the dep-free stages (`AS OF`, `RERANK`,
  `TRAVERSE`, `WHERE`) run everywhere `query` is on.

## See also

- [Engine architecture](architecture/engine.md) — the plan executor, bi-temporal model, tiers.
- [Concepts](concepts.md) — `KG-2.208` (fused executor), `KG-2.214` (UQL), `KG-2.250` (bi-temporal
  `AS OF`), `KG-2.253` (N-way FUSE), `KG-2.254/2.255` (graph-native + MMR rerankers).
- The authoritative grammar lives in `crates/eg-plan/src/uql/parser.rs` (kept in lockstep with
  this page); the op algebra in `crates/eg-types/src/wire.rs`.
