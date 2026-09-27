# UQL — the Unified Query Language

UQL is epistemic-graph's human- and agent-writable query language. It is a **pure
front-end** (CONCEPT:AU-KG.query.top-nodes-by-degree) over the engine's cross-modal plan
algebra (CONCEPT:AU-KG.compute.vector): a UQL string parses to the *exact same* `wire::Plan`
(an ordered `Vec<Op>`) that the structured `UnifiedQuery` API executes — it adds **no** new
execution path.

**Every plan operator has a UQL spelling.** The contract is executable: a test walks every
`wire::Op` and `wire::Pred` variant compiled into the build, prints it with the canonical
printer (`Plan::to_uql`) and re-parses it; any variant without a faithful spelling fails the
build's tests (the "builder-only" allowlist is empty). A property test does the same over
randomly generated plans, and the fuzz target checks it on every input that parses.

The parser is dependency-free (no DataFusion, no regex), so it is available in every build;
each clause's *executor* needs its owning cargo feature (see [Feature gating](#feature-gating)).

## The mental model: one pipeline, one RowSet currency

A query is a **pipeline**: a *source* seeds a set of candidate node ids, then `|>`-separated
**stages** transform it. Every stage is `(RowSet) -> RowSet` over one currency — an ordered
list of `(id, score)` rows, plus named score channels (see [`RETURN`](#return--score-channels))
— so SQL, graph, vector, text, temporal, reasoning, spatial, tensor, stream and federation
stages compose with no impedance mismatch. The whole pipeline runs over **one** off-lock,
RLS-filtered snapshot at a single engine version.

```uql
MATCH (:Doc) WHERE year > 2024            # source + relational filter
  |> TRAVERSE -[:CITES]->{1,2}            # graph traversal (1..2 hops)
  |> RANK BY ~[0.1, 0.9, 0.0]             # vector re-rank by similarity
  |> AS OF @1700000000                    # keep only facts live at that instant
  |> RERANK MMR 0.5 10                    # diversify the top results
  |> LIMIT 10
```

Comments start with `#`, `--` or `//` and run to the end of the line. Keywords are
case-insensitive. Strings are `'single'` (a doubled `''` escapes) or `"double"` (`\"`, `\\`).
Names with spaces, punctuation, non-ASCII letters or keyword spellings are back-quoted:
`` `research paper` ``, `` `LIMIT` ``. Numbers accept exponents (`1e-9`, `6.02E+23`).

## Grammar (EBNF)

Generated from the grammar table (`crates/eg-plan/src/uql/grammar.rs`) — the one source of
truth the parser's dispatch table, the parse-error "expected" lists and the NL planner's
system prompt are all generated from or checked against. A test fails when this block drifts;
regenerate it in place with `cargo run -q -p eg-plan --example uql_grammar -- write-docs`.

<!-- BEGIN GENERATED: uql-grammar -->
```text
statement          = [ "UQL" int ";" ] [ "EXPLAIN" | "PROFILE" ] [ federation_budget ] { binding } pipeline [ annotations ] ;
federation_budget  = "FEDERATION" "BUDGET" "(" dimension "=" int { "," dimension "=" int } ")" ;   (* feature `federation` *)
annotations        = "WITH" annotation { "," annotation } ;
annotation         = "PROOF" | "KNOWLEDGE" [ "(" name { "," name } ")" ] ;
binding            = "LET" name "=" pipeline ";" ;
pipeline           = head { "|>" stage } ;
head               = source | stage | "FROM" name | "JOIN" name "," name { "," name } ;
match              = "MATCH" "(" [ ":" name ] ")" [ "WHERE" pred ] ;
foreign            = "FOREIGN" id | "FOREIGN" "SCAN" string [ "JOIN" ] | "FOREIGN" "HTTP" string [ "PATH" string ] "ID" string [ "SCORE" string ] [ "JOIN" ] ;
decisions          = "DECISIONS" [ "WHERE" pred ] ;
sparql             = "SPARQL" string "VAR" string ;   (* feature `owl` *)
tsscan             = "TSSCAN" string_list "FROM" num "TO" num ;   (* feature `timeseries` *)
sensor             = "SENSOR" "FUSE" string_list "TOLERANCE" int | "SENSOR" "ALIGN" "[" string interp { "," string interp } "]" "CLOCK" clock [ "TOLERANCE" int ] ;   (* feature `timeseries` *)
clock              = "UNIFORM" "FROM" int "TO" int "STEP" int | "TUMBLING" "WIDTH" int "STEP" int ;
interp             = "NEAREST" | "LINEAR" | "ASOF_HOLD" ;
filter             = "WHERE" pred ;
as_of              = "AS" "OF" [ "TX" | "VALID" ] ts ;
valid_as_of        = "VALID" "AS" "OF" ts ;
reason             = "REASON" ( iri | string | name ) [ "ONTOLOGY" string ] ;   (* feature `owl` *)
evidence_for       = "EVIDENCE" "FOR" id ;   (* feature `epistemic` *)
contradicts        = "CONTRADICTS" id ;   (* feature `epistemic` *)
supported_by       = "SUPPORTED" "BY" id ;   (* feature `epistemic` *)
explain_belief     = "EXPLAIN" "BELIEF" id ;   (* feature `epistemic` *)
spatial_scan       = "SPATIAL" "SCAN" id "BBOX" "[" signed_num "," signed_num "," signed_num "," signed_num "]" ;   (* feature `geo` *)
tensor_scan        = "TENSOR" "SCAN" id ;   (* feature `tensor` *)
traverse           = "TRAVERSE" ( "-" "[" rel "]" ( "->" | "-" ) | "<-" "[" rel "]" "-" | name ) [ hops ] ;
propagate          = "PROPAGATE" ( "NOISY_OR" | "CASCADE" [ "SAMPLES" int ] [ "SEED" int ] ) edge [ "HOPS" int ] [ "DEFAULT" num ] ;
edge               = "-" "[" rel "]" ( "->" | "-" ) | "<-" "[" rel "]" "-" | name ;
rel                = ( ":" name | "*" ) [ "WHERE" pred ] ;
hops               = "{" int [ ( "," | ".." ) int ] "}" ;
rank               = "RANK" "BY" "~" ( vector | string | param ) ;
text               = "TEXT" string ;   (* feature `text` *)
fuse               = "FUSE" [ "K" num ] ( "[" branch "]" { "[" branch "]" } | "(" name { "," name } ")" ) ;   (* feature `text` *)
branch             = stage { "|>" stage } ;
rerank             = "RERANK" ( "NODE_DISTANCE" "FROM" id | "MENTIONS" | "MMR" num int ) ;
window             = "WINDOW" num [ unit ] [ agg ] ;
unit               = "ns" | "us" | "ms" | "s" | "m" | "min" | "h" | "d" (plural forms accepted) ;
agg                = "MEAN" | "AVG" | "SUM" | "MIN" | "MAX" | "COUNT" | "FIRST" | "LAST" ;
derive             = "DERIVE" sexpr "AS" name { "," sexpr "AS" name } ;   (* feature `timeseries` *)
skill              = "SKILL" name "AGAINST" name "HORIZONS" "[" int { "," int } "]" "WINDOW" int [ "BOOTSTRAP" int "SEED" int ] ;   (* feature `timeseries` *)
motif              = "MOTIF" "OF" name ( "LIKE" "[" num { "," num } "]" | "LENGTH" int ) "TOP" int [ "SEED" int ] ;   (* feature `timeseries` *)
discord            = "DISCORD" "OF" name "LENGTH" int "TOP" int [ "SEED" int ] ;   (* feature `timeseries` *)
events             = "EVENTS" name { "," name } ;   (* feature `timeseries` *)
sexpr              = signed_num | name | name "(" [ sexpr { "," sexpr } ] ")" ;
limit              = "LIMIT" ( int | param ) ;
return             = "RETURN" name { "," name } ;
udf                = "UDF" id ;   (* feature `wasm-udf` *)
reproject          = "REPROJECT" "TO" int [ "FROM" int ] ;   (* feature `geo` *)
spatial_op         = "SPATIAL" ( "BUFFER" num | "CONVEX_HULL" | "SIMPLIFY" num | "CENTROID" | ( "UNION" | "INTERSECTION" | "DIFFERENCE" ) string ) ;   (* feature `geo` *)
tensor_op          = "TENSOR" ( "SLICE" "[" int ":" int { "," int ":" int } "]" | "REDUCE" ( "SUM" | "MEAN" | "MAX" | "MIN" ) "AXIS" int | ( "ADD" | "SUB" | "MUL" | "DIV" ) num ) ;   (* feature `tensor` *)
cep                = "CEP" cep_node "WINDOW" ( "SLIDING" | "TUMBLING" ) int ;   (* feature `stream` *)
cep_node           = "SEQ" "(" [ matcher { "," matcher } ] ")" | "WITHIN" int "(" cep_node ")" | "ABSENCE" matcher "THEN" "NOT" matcher "WITHIN" int ;
matcher            = "{" [ "KEY" string ] [ "WHERE" cep_pred { "AND" cep_pred } ] "}" ;
cep_pred           = name ( "=" json | ">" signed_num | "<" signed_num | "EXISTS" ) ;
validate_shape     = "VALIDATE" "SHAPE" ( iri | string | name ) [ "USING" string ] [ "KEEP" ( "CONFORMING" | "VIOLATING" ) ] ;   (* feature `owl` *)
prob               = "PROB" ( "EXPECTATION" | "MARGINAL" [ "AT" num ] [ "LABEL" string ] | "CONDITIONAL" ( "BERNOULLI" num num | "GAUSSIAN" num_list "VARIANCE" num ) | "SAMPLE" "SEED" int ) ;   (* feature `probabilistic` *)
belief_as_of       = "BELIEF" "AS" "OF" ts ;   (* feature `epistemic` *)
source_reliability = "SOURCE" "RELIABILITY" id ;   (* feature `epistemic` *)
confidence         = "CONFIDENCE" ;   (* feature `epistemic` *)
attribute          = "ATTRIBUTE" attr_value "OF" ( "SCORE" | name ) ( "LINEAR" | "SHAPLEY" [ "SAMPLES" int "SEED" int ] | "OWEN" "BY" name ) ;   (* feature `numeric` *)
attr_value         = "SUM" | "MEAN" | "MAX" | "MIN" | percentile ;
percentile         = ident (* one word P1 .. P99, e.g. P95 *) ;
pred               = conj { "OR" conj } ;
conj               = neg { "AND" neg } ;
neg                = "NOT" neg | "(" pred ")" | atom ;
atom               = name cmp scalar | name [ "NOT" ] "IN" ( "(" scalar { "," scalar } ")" | param ) | name [ "NOT" ] "BETWEEN" scalar "AND" scalar | name "IS" [ "NOT" ] "NULL" | path ( "EXISTS" | ( "=" | "==" ) json | "@>" json ) | spatial_pred ;
cmp                = "=" | "==" | "!=" | "<>" | ">" | ">=" | "<" | "<=" ;
spatial_pred       = "SPATIAL" ( "WITHIN" | "CONTAINS" | "COVERS" | "TOUCHES" | "CROSSES" | "OVERLAPS" | "EQUALS" | "DISJOINT" ) "(" name "," string ")" | "SPATIAL" "DWITHIN" "(" name "," string "," num ")" ;   (* feature `geo` *)
path               = jsonpath | "JSONPATH" string ;
json               = scalar | "NULL" | "JSON" string ;
scalar             = string | signed_num | "TRUE" | "FALSE" | name | param ;
ts                 = "@" signed_num | param ;
vector             = "[" [ signed_num { "," signed_num } ] "]" ;
string_list        = "[" [ string { "," string } ] "]" ;
num_list           = "[" [ signed_num { "," signed_num } ] "]" ;
signed_num         = [ "-" ] number | param ;
id                 = name | string | param ;
name               = ident | quoted_ident ;
```
<!-- END GENERATED: uql-grammar -->

## Sources

A source seeds the RowSet from nothing. Start every pipeline with one.

| Clause | Op | Feature |
|--------|-----|---------|
| `MATCH (:Label)` | `Scan{label}` | — |
| `MATCH ()` | `ScanAll{}` — every node of the (RLS-filtered) snapshot | — |
| `MATCH (:Label) WHERE p` | `Scan` + `Filter` | — |
| `FOREIGN 'name'` | `Foreign{name}` — a registered external source | — (resolve: `federation`) |
| `FOREIGN SCAN 'name' [JOIN]` | `ForeignScan{Named}` | `federation` |
| `FOREIGN HTTP 'url' [PATH 'p'] ID 'f' [SCORE 's'] [JOIN]` | `ForeignScan{HttpJson}` | `federation` |
| `SPARQL 'select…' VAR 'x'` | `SparqlBgp{query,var}` | `owl` |
| `TSSCAN ['cpu'] FROM 0 TO 3600` | `TsScan{series,from,to}` | `timeseries` |
| `SENSOR FUSE ['imu','gps'] TOLERANCE 5000000` | `SensorFuse` (ns) | `timeseries` |
| `SENSOR ALIGN ['imu' LINEAR, 'gps' NEAREST] CLOCK UNIFORM FROM 0 TO 1000 STEP 10` | `SensorAlign` (ns) | `timeseries` |
| `SPATIAL SCAN 'roads' BBOX [0, 0, 10, 10]` | `SpatialScan{layer,bbox}` | `geo` |
| `TENSOR SCAN 'frames'` | `TensorScan{layer}` | `tensor` |
| `DECISIONS [WHERE p]` | `DecisionScan{preds}` — the caller's visible decision records | — (served: `decide`) |

These also seed when they lead a pipeline, and narrow when they follow one: `WHERE`,
`AS OF`, `VALID AS OF`, `REASON`, `EVIDENCE FOR`, `CONTRADICTS`, `SUPPORTED BY`,
`EXPLAIN BELIEF`.

A credential-bearing foreign source (a remote engine's shared secret, a SQL DSN) has **no**
text spelling: register it with `RegisterForeignSource` and use `FOREIGN SCAN '<name>'`.
`FOREIGN ENGINE …` / `FOREIGN SQL …` fail with `UQL_CREDENTIAL_BEARING_SPEC`.

```uql
MATCH () WHERE status IN ('open', 'new') |> LIMIT 20
```

`DECISIONS` (EH-066) reads the caller's decision log — only the records the caller may read:
the tenant's shared records and its own principal-visible ones — and keeps those whose columns
satisfy the relational predicates. Row ids are record ids (in record-id order, unscored); the
columns are those of the SQL `decisions` relation (`record_id`, `question_id`,
`question_kind`, `safety`, `source`, `outcome`, `option_id`, `resolution_kind`,
`evidence_class`, `policy_digest`, `committed_by`, `created_at_ms`, `committed_at_ms`). The
same records are also SQL relations — `decisions`, `decision_evaluations`,
`decision_resolutions` — on every served SQL surface (`Sql`, the Postgres wire), read-only and
per caller. A plan with `DECISIONS` is never result-cached (the log changes without a graph
write). The `reputation` relation (EH-525) learns from the same visible records: one row per
executed `option`, per `agent` it ran as, and per `forecaster` (a fitted decision head), with
the pooled, age-discounted success probability (`mean`, `lower`, `upper`) or, for a
forecaster, its proper scores (`brier` = `reliability` − `resolution` + `uncertainty`,
`log_loss`, `ece`). Only independent observations count (never a self-report), and a row
below the policy's support floor reports `insufficient_history` with no numbers.

```uql
DECISIONS WHERE outcome = 'acted' AND committed_at_ms >= 1700000000000 |> LIMIT 50
```

## Stages

### `WHERE` — relational filter (DataFusion)

`WHERE pred` → `Filter{preds}`. A top-level `a AND b` is the filter's conjunct list; the
predicate algebra:

| Form | Pred |
|------|------|
| `x = 'str'` / `x = name` | `Eq` |
| `x > 3` / `x < 3` | `GtNum` / `LtNum` |
| `x = 3`, `x != 'a'`, `x <> TRUE`, `x >= 1.5`, `x <= -2` | `Cmp{op, value}` — a **typed** literal: numbers compare as numbers, booleans as booleans |
| `x IN ('a', 2, TRUE)` / `x NOT IN (…)` / `x IN $list` | `In` / `Not{In}` |
| `x BETWEEN 1 AND 5` / `x NOT BETWEEN …` | `Between` (inclusive) / `Not{Between}` |
| `x IS NULL` / `x IS NOT NULL` | `IsNull` / `Not{IsNull}` |
| `a OR b`, `NOT a`, `( … )` | `Or`, `Not`, grouping — `NOT` > `AND` > `OR` |
| `$.a.b[0] EXISTS`, `$.tags = 'x'`, `$.meta @> JSON '{"k": 1}'`, `JSONPATH '$.a b' EXISTS` | `JsonPath` |
| `SPATIAL WITHIN(geom, 'POLYGON(…)')`, `SPATIAL DWITHIN(geom, 'POINT(0 0)', 2.5)`, `CONTAINS`/`COVERS`/`TOUCHES`/`CROSSES`/`OVERLAPS`/`EQUALS`/`DISJOINT` | `Spatial*` (feature `geo`) |

Semantics are SQL's three-valued logic: a comparison with a missing property is *unknown*,
and `NOT unknown` does not keep the row. `x = NULL` is refused (`UQL_NULL_LITERAL`) — use
`IS NULL`. JSONPath and spatial predicates are evaluated per row; they may appear as
top-level conjuncts but not under `OR`/`NOT`.

```uql
MATCH (:Doc)
  |> WHERE (year >= 2020 AND lang = 'en') OR pinned = TRUE
  |> WHERE rating BETWEEN 3 AND 5 AND author IS NOT NULL
  |> LIMIT 10
```

### `TRAVERSE` — graph hops

| Form | Op |
|------|-----|
| `TRAVERSE -[:CITES]->{1,2}` / `TRAVERSE CITES{1,2}` | `Traverse` (outgoing) |
| `TRAVERSE <-[:CITES]-{1,2}` | `Expand{dir: In}` |
| `TRAVERSE -[:KNOWS]-{1,3}` | `Expand{dir: Both}` |
| `TRAVERSE -[*]->` | `Expand{rel: None}` — any relationship |
| `TRAVERSE -[:CITES WHERE weight >= 0.5]->{1,2}` | `Expand{edge_preds}` — predicates over the edge's properties |

`{n}` is exactly `n` hops, `{a,b}` / `{a..b}` a range, no range one hop. `Expand` reports each
node once at its shortest hop distance, and `{0,n}` includes the seeds themselves. Every
traversal is bounded by the plan's traversal budget (`UQL_BUDGET_EXCEEDED`).

```uql
MATCH (:Paper) |> TRAVERSE <-[:CITES WHERE year > 2020]-{1,2} |> LIMIT 50
```

### Ranking

| Clause | Op | Feature |
|--------|-----|---------|
| `RANK BY ~[0.1, -0.2]` / `RANK BY ~$vec` | `Rank{query}` | — |
| `RANK BY ~'some text'` / `RANK BY ~$text` | `RankEmbed{text}` (server-side embedder) | — |
| `TEXT 'graph databases'` | `RankText{query}` (BM25) | `text` |
| `FUSE [K 60] [branch] [branch] …` | `FuseRrf{branches, k}` (`k` omitted ⇒ the RRF default) | `text` |
| `RERANK NODE_DISTANCE FROM 'id'` / `RERANK MENTIONS` / `RERANK MMR 0.5 10` | graph-native / diversity rerankers | — |
| `PROB EXPECTATION` / `PROB MARGINAL AT 0.5 [LABEL 'x']` / `PROB CONDITIONAL BERNOULLI 3 1` / `PROB CONDITIONAL GAUSSIAN [1, 2] VARIANCE 0.25` / `PROB SAMPLE SEED 7` | `Probabilistic{query}` | `probabilistic` |

A bare-name embedding handle (`RANK BY ~handle`) is a reserved seam and is refused.

```uql
MATCH (:Doc) |> FUSE K 60 [RANK BY ~[1, 0]] [TEXT 'graph'] [RERANK NODE_DISTANCE FROM 'kg-2.0'] |> LIMIT 5
```

### Time

| Clause | Op |
|--------|-----|
| `AS OF @t` / `AS OF VALID @t` / `VALID AS OF @t` | `AsOf{axis: Valid}` — what was true at `t` |
| `AS OF TX @t` | `AsOf{axis: Transaction}` — what we believed at `t` |
| `WINDOW 1 h` | `Window{secs}` — tumbling mean |
| `WINDOW 500 ms SUM` | `WindowAgg{secs, agg}` |

`t` is unix seconds, may be negative, or a `$param`. Units: `ns`, `us`, `ms`, `s`, `m`/`min`,
`h`, `d`; aggregates `MEAN`/`AVG`, `SUM`, `MIN`, `MAX`, `COUNT`, `FIRST`, `LAST`. The unit is
read before the aggregate, so `WINDOW 30 min` is thirty minutes.

```uql
TSSCAN ['cpu', 'mem'] FROM 0 TO 3600 |> WINDOW 60 s MEAN |> LIMIT 60
```

`TSSCAN` emits one row per point per series, id `series@ts` (ns), with every field of the
point as an exact `f64` value channel `v0..vk` that `RETURN` can name (the `f32` score is
`v0`, for ranking). A later committed version of a point (a correction) supersedes an
earlier one; a transaction's staged point shadows both.

### Series operators (`DERIVE`, `SKILL`)

| Clause | Op |
|--------|-----|
| `DERIVE zscore(ewma(v0, 12), 60) AS lat_z` | `Derive{columns}` — per-series incremental operators in timestamp order |
| `SKILL v0 AGAINST r HORIZONS [1, 5] WINDOW 30` | `Skill{spec}` — rolling rank-IC predictive skill per horizon |

A `DERIVE` expression calls `lag diff ret logret` (count `k`), `rmean rstd rsum rmin rmax
rrank zscore` (window `w`), `ewma` (span), `ewma_halflife`, `abs sign neg clip(x, lo, hi)`,
`add sub mul div` (`ratio` is `div`), `rcorr ic wsum` (two series and a window) and
`kalman(x, q, r)` / `kbeta(x, y, q, r)`; series arguments come first, then numbers. It reads
`v0..vk`, `score` and earlier aliases, and writes its alias as a value channel; each column
keeps its own warm-up (no value until its window fills). `rstd`/`zscore` use the population
deviation, `ewma` is the recursive form seeded with the first value. The kernels are
`eg_numeric::series` — the same ones the SQL `eg_<func>(…) OVER (PARTITION BY … ORDER BY …)`
window functions and a materialised derived series (`TsDefineSeries`) run.

```uql
TSSCAN ['px'] FROM 0 TO 86400 |> DERIVE ret(v0, 1) AS r, zscore(ewma(v0, 12), 60) AS z
  |> SKILL z AGAINST r HORIZONS [1, 5, 20] WINDOW 60 BOOTSTRAP 500 SEED 7
  |> RETURN mean_ic, icir, ic_lo, ic_hi
```

### Reasoning and epistemic stages

| Clause | Op | Feature |
|--------|-----|---------|
| `REASON <http://ex/Device> [ONTOLOGY '<turtle>']` | `Reason{target_class, ontology}` | `owl` |
| `VALIDATE SHAPE <http://ex/S> [USING '<turtle>'] [KEEP CONFORMING\|VIOLATING]` | `ValidateShape{shape, shapes, keep}` — SHACL conformance filter; without `USING`, the graph's GraphSchema shapes | `owl` |
| `EVIDENCE FOR 'c1'`, `CONTRADICTS 'c1'`, `SUPPORTED BY 'c1'` | evidence graph | `epistemic` |
| `BELIEF AS OF @t`, `SOURCE RELIABILITY 's1'`, `CONFIDENCE`, `EXPLAIN BELIEF 'c1'` | belief scoring | `epistemic` |

`SOURCE RELIABILITY 's1'` multiplies every row by the source's reliability. Its prior is the
source's propagated belief confidence; when the served query has the caller's decision log
bound and that log holds independently evaluated outcomes of `s1`, the reliability is the
prior updated with those outcomes (the `reputation` view, EH-525), and the channels
`reliability_lo` / `reliability_hi` carry its credible interval.

```uql
MATCH (:Claim) |> EVIDENCE FOR 'c1' |> BELIEF AS OF @1700000000 |> LIMIT 10
```

### Modality stages

| Clause | Op | Feature |
|--------|-----|---------|
| `UDF 'score-v2'` | `Udf{id}` (sandboxed WASM) | `wasm-udf` |
| `REPROJECT TO 3857 [FROM 4326]` | `Reproject` | `geo` |
| `SPATIAL BUFFER 2.5` / `CONVEX_HULL` / `SIMPLIFY 0.1` / `CENTROID` / `UNION 'wkt'` / `INTERSECTION 'wkt'` / `DIFFERENCE 'wkt'` | `SpatialOp` | `geo` |
| `TENSOR SLICE [0:2, 1:4]` / `TENSOR REDUCE MEAN AXIS 0` / `TENSOR ADD 1.5` (`SUB`/`MUL`/`DIV`) | `TensorOp` | `tensor` |
| `CEP SEQ ({KEY 'trade' WHERE qty > 100}, {KEY 'cancel'}) WINDOW SLIDING 60` | `Cep` | `stream` |
| `CEP WITHIN 30 (SEQ (…)) WINDOW TUMBLING 60`, `CEP ABSENCE {…} THEN NOT {…} WITHIN 10 WINDOW SLIDING 60` | `Cep` | `stream` |

```uql
SPATIAL SCAN 'roads' BBOX [0, 0, 10, 10] |> SPATIAL BUFFER 2.5 |> REPROJECT TO 3857
```

### `ATTRIBUTE` — contribution attribution

`ATTRIBUTE <agg> OF (SCORE | <property>) <method>` → `Attribute{input, value, method}`
(feature `numeric`). The incoming rows are the players: each row's value is its score or a
numeric node property, and a coalition's value is `<agg>` of its members' values
(`SUM`, `MEAN`, `MAX`, `MIN` or a percentile `P1`…`P99`; the empty coalition is 0). Every
row is re-scored with its contribution and the rows are ordered by it, descending. The
contributions always sum to the value of the whole row set.

| Method | Meaning |
|--------|---------|
| `LINEAR` | the exact split of an additive value (`SUM` only; anything else is refused with `ATTRIBUTION_NON_ADDITIVE`) |
| `SHAPLEY` | exact Shapley values over a memoised coalition table — at most 16 rows |
| `SHAPLEY SAMPLES n SEED s` | sampled Shapley over `n` antithetic permutations drawn from seed `s`; channel `attribution_ci` carries each row's 95% CI half-width |
| `OWEN BY <property>` | Owen values: rows grouped into unions by a node property (for example pods within a service) |

The score channel is `attribution`. A stage that would need more coalition evaluations than
its budget fails with `UQL_BUDGET_EXCEEDED`; a row without the attributed value is refused by
id. Replays are bit-identical: the seed is part of the stage.

```uql
MATCH (:Service) |> ATTRIBUTE P95 OF latency_ms SHAPLEY SAMPLES 4000 SEED 7 |> RETURN attribution, attribution_ci
```

### `LIMIT`

`LIMIT 10` / `LIMIT $k` → `Limit{k}`. Order-respecting top-k.

### `RETURN` — score channels

`RETURN similarity, belief` → `Project{channels}`. Every scoring stage records its score under
a named channel as well as in `score`, so the results of several scoring stages coexist
instead of the last one overwriting the others. The served result carries the named channels
per row (see [Running a query](#running-a-query)).

## Parameters

`$name` is a typed parameter. Values are bound **as values** at the literal position that uses
them — a string, number, boolean, vector (`RANK BY ~$v`) or list (`x IN $ids`) — and never
spliced into the text, so a parameter can never change a query's structure. Binding the wrong
type (`UQL_PARAMETER_TYPE`), leaving one unbound (`UQL_UNBOUND_PARAMETER`) or binding one the
query never references (`UQL_UNUSED_PARAMETER`) is an error.

```text
MATCH (:Doc) WHERE year >= $min AND lang = $lang |> RANK BY ~$v |> LIMIT $k
```

## Programs: named sub-plans and DAGs

`LET name = pipeline;` names a sub-plan. `FROM name` continues from a binding's output and
`JOIN a, b |> stage` feeds the intersection of several outputs into one stage — the program
becomes a `PlanDag` (the executor's multi-input nodes intersect their inputs). `FUSE (a, b)`
inlines bindings as RRF branches. Bindings must be defined before use and must be used.

```uql
LET recent = MATCH (:Doc) WHERE year > 2020;
LET cited  = FROM recent |> TRAVERSE -[:CITES]->;
JOIN recent, cited |> LIMIT 10
```

A statement may start with a version pragma, `UQL 1;`, and with `EXPLAIN` (plan, cost and
incremental-maintainability, no execution) or `PROFILE` (execute and report per-stage rows
and time).

```uql
UQL 1; EXPLAIN MATCH (:Doc) WHERE year > 2024 |> TRAVERSE -[:CITES]-> |> LIMIT 10
```

Programs take all three modes too (EH-449). A program runs node by node exactly as written —
there is no DAG cost reordering — so `EXPLAIN` reports the program as both its canonical and
its optimized plan, one stage per node (`#2 <- #0,#1 RERANK MENTIONS`, with the node's cost
estimate: its input's, or the smallest joined input's), and `incremental: false`. `PROFILE`
adds each node's actual rows and time, and `RETURN` channels are traced the same way; when
two branches score the same channel for a row, the node that runs later (topological order)
wins.

```uql
PROFILE LET docs = MATCH (:Doc); LET cited = FROM docs |> TRAVERSE CITES;
JOIN docs, cited |> RERANK MENTIONS |> RETURN mentions
```

## Row annotations: `WITH PROOF`, `WITH KNOWLEDGE`

A statement may end in `WITH PROOF`, `WITH KNOWLEDGE [(column, …)]`, or both. They annotate
each result row from the same snapshot the statement ran over; `EXPLAIN` has no rows to
annotate.

* `WITH KNOWLEDGE` (EH-450) attaches the row's knowledge record — the `KnowledgeSet` row:
  `kind`, `confidence`, the bitemporal window (`valid_from`/`valid_until`,
  `tx_from`/`tx_until`), the named columns as one `projection` object, and the epistemic
  neighbourhood (`source_refs`, `evidence_refs`, `policy_labels`, `contradiction_ids`,
  `proof_ids`, `transformation_ids`, `alternative_ids`; empty, never invented, without
  `epistemic`).
* `WITH PROOF` (EH-448, the UQL surface of the proof-carrying results) attaches why the row
  is in the result: one step per stage that ADMITS rows, in pipeline order. A `SPARQL` source
  proves a row with its witness — the ground triples that instantiate the query's patterns
  under a solution binding the row; a `REASON` stage with the OWL proof tree of the row's
  class membership (its asserted type and the subsumption chain). Stages that only order,
  score or cut rows (`RANK`, `TEXT`, `RERANK`, `LIMIT`, `RETURN`, `CONFIDENCE`, `SOURCE
  RELIABILITY`, a `FUSE` of such) need no proof. Any other admitting stage (`MATCH`, `WHERE`,
  `TRAVERSE`, `AS OF`, …) contributes an `Unproved` step. The proof's `coverage` is
  `complete` only when every admitting stage proved the row and each of those proofs is
  itself complete — a partial proof is never presented as a complete one.

```uql
SPARQL 'SELECT ?w WHERE { ?w a <http://example.org/Paper> }' VAR 'w'
  |> RANK BY ~[1, 0] |> LIMIT 5
  WITH PROOF, KNOWLEDGE (title)
```

## Feature gating

One rule for every clause: the parser **recognizes** every keyword in every build, and a clause
whose executor needs a cargo feature the build lacks is refused **at parse time** with
`UQL_FEATURE_NOT_IN_BUILD` naming the feature. `VALID AS OF` lowers to the always-available
`AsOf` and is available everywhere.

## Diagnostics

Every error has a stable code (`UQL_UNEXPECTED_TOKEN`, `UQL_UNKNOWN_STAGE`,
`UQL_EXPECTED_INTEGER`, `UQL_NESTING_TOO_DEEP`, `UQL_UNBOUND_PARAMETER`,
`UQL_FEATURE_NOT_IN_BUILD`, …), a byte span, the set of spellings that would have been
accepted, and — where one is known — a fix. Rendered, the caret sits under the exact span on
the right line:

```text
UQL_UNEXPECTED_TOKEN at 2:12: expected a LIMIT count (an integer), found `)`
  2 |   |> LIMIT )
    |            ^
```

A transform at the head of a pipeline parses but warns
(`UQL_W_HEAD_TRANSFORM_YIELDS_EMPTY`): it runs over an empty RowSet.

## Composition, sources & commutativity

At runtime some operators re-seed when their input is empty (`WHERE`, `AS OF`, `REASON` and the
epistemic stages act as sources on an empty RowSet); that is why they are listed as
source-capable above. It is an executor property, not something to lean on: two narrowers do
not commute when the first empties the set (`WHERE level > 9 |> AS OF @100` re-seeds;
`AS OF @100 |> WHERE level > 9` stays empty), and the cost optimizer never reorders across that
boundary (EG-405, `EG-KG.query.empty-set-commutativity`; the witnesses
`plan_proptest::empty_intermediate_reseeds_source_breaks_commute` and
`filter_and_asof_commute_in_nonempty_regime` are the spec). Start pipelines with an explicit
source.

## Bounds and read-only guarantee

Every plan runs under a budget: at most `max_result_rows` rows (default 100 000) and at most
`max_traversal_visits` nodes per traversal (default 1 000 000). Exceeding one fails with
`UQL_BUDGET_EXCEEDED` naming the budget — never a silent truncation. UQL is **read-only**: no
operator writes graph state (a test asserts every `Op` is classified read-only).

## Running a query

**Python client**:

```python
result = await client.uql(
    "MATCH (:Concept) WHERE year >= $min |> RERANK MMR 0.5 5 |> RETURN mmr |> LIMIT $k",
    params={"min": 2020, "k": 5},
)
rows = result["rows"]  # [{"id", "score", "channels": {"mmr": …}}]; result["kind"] == "rows"
```

On the wire this is `Method::Uql { text, params }` — the one query-text method — and, inside a
transaction (read your own writes), `Method::TxnUql { txn_id, text, params }`: the same
statement, grammar and result (EH-434 retired the rows-only `UnifiedQueryText` and
`TxnUnifiedQueryText`). Executed answers are result-cached like `UnifiedQuery`
answers (keyed on the text, the bound params and the caller's RLS context); `PROFILE` answers
and plans that read the decision log are not.

**MCP / REST** — the served `graph_query` / `graph_search` surfaces accept UQL through the same
`unified` core.

## DecideText

DecideText is the decision front end (DECIDE-LAYER-DESIGN §5). It is not UQL — it parses to a
typed decide or assembly request, never to a plan `Op`, and UQL refuses its clauses by name
(`UQL_DECISION_CLAUSE_IN_UQL`) — but it is in UQL's family (EH-452): the same lexer, `$name`
parameters (`@name` is refused with the fix), a `{ … }` candidate query that is ordinary UQL
(braces inside its strings do not close it; its own diagnostic points into the DecideText
source), the same structured errors, and a grammar table
(`crates/eg-plan/src/decide_text/grammar.rs`) this block is generated from:

<!-- BEGIN GENERATED: decide-text-grammar -->
```text
decide_text     = candidates { "|>" clause } ;
candidates      = "CANDIDATES" ( "AGENT" "LIBRARY" "KINDS" "[" name { "," name } "]" [ "UNDER" ( iri | string ) ] | "GRAPH" string "QUERY" "{" uql "}" ) ;
covers          = "COVERS" param ;
validate_policy = "VALIDATE" "POLICY" ( "DEFAULT" | pin ) ;
decide          = "DECIDE" name "QUESTION" string [ "SAFETY" name ] "FEATURES" pin [ "HEAD" pin ] [ "MAX" int ] ;
assemble        = "ASSEMBLE" [ "MAX" "COMPONENTS" int ] ;
pin             = string "AT" string ;
param           = "$" name ;
```
<!-- END GENERATED: decide-text-grammar -->

## Gotchas

- **Quote ids.** An id with `-`, `.`, `:` or `@` must be a string: `RERANK NODE_DISTANCE FROM 'kg-2.0'`.
- **`AS OF` windows are half-open `[from, until)`, in unix seconds.** A missing `valid_from`
  reads as 0; a missing `valid_until` as still current.
- **`FUSE` fuses ranks, not scores** — the fused score is `Σ 1/(k+rank)` across branches.
- **`x = 3` is numeric.** A number compares as a number; write `x = '3'` for a string compare.

## See also

- [Engine architecture](architecture/engine.md) — the plan executor, bi-temporal model, tiers.
- [Concepts](concepts.md) — `AU-KG.compute.vector` (fused executor), `AU-KG.query.top-nodes-by-degree` (UQL).
- The grammar lives in `crates/eg-plan/src/uql/grammar.rs`; the printer in
  `crates/eg-types/src/wire_query_uql.rs`; the op algebra in `crates/eg-types/src/wire_query_core.rs`.
