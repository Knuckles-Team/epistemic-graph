# SPARQL & RDF interface

epistemic-graph is an RDF triple-store: you load RDF (OWL/RDFS or plain triples), the engine maps it
onto the property graph, and SPARQL `SELECT` runs over it. RDF and the property graph are **the same
data** — a triple is a node, an edge, or a property depending on its object.

> Status snapshot: SPARQL `SELECT`/`ASK`/`CONSTRUCT`/`DESCRIBE`, `UPDATE`, true named graphs, and the W3C
> `/sparql` HTTP endpoint (feature `sparql-http`) are all supported. Content negotiation, rich FILTER,
> sub-SELECT, `SERVICE`, `MINUS`, `!p`, and an SPO/POS index are 🔶 in-progress. See the
> [capability matrix](../capabilities.md#sparql-eg-rdf).

## RDF ↔ property-graph mapping

| RDF construct | Maps to |
|---------------|---------|
| IRI / blank-node subject or object | a graph node (id = canonical term string) |
| triple with a **resource** object `(s, p, o)` | a typed edge `s --p--> o` |
| triple with a **literal** object | a property on `s` (value + datatype + lang preserved) |
| `rdf:type` | folded into the node `type` property **and** kept as an explicit typing edge |
| named graph | a `GraphCore` in the registry with a `:NamedGraph` marker |

Multi-valued literals for the same predicate (which a key-unique property map can't hold) go to an
opt-in lossless `quads` redb table under the `rdf-redb` feature; without it, extras are **counted** in
`LoadReport.dropped_multivalue`, never silently lost. Export round-trips to N-Triples by set-equality.

## SPARQL SELECT

```sparql
SELECT ?s ?o WHERE {
  ?s rdfs:subClassOf ?o .
  FILTER (?s != ?o)
}
ORDER BY ?s
LIMIT 100
```

Implemented algebra (compiled to scans over the property graph, not an embedded triple evaluator):

- **Patterns**: BGP, property paths (`^p`, `p/q`, `p|q`, `p+`, `p*`, `p?`), `OPTIONAL`, `UNION`.
- **Solution modifiers**: `Project`, `Distinct`, `Reduced`, `Slice` (LIMIT/OFFSET), `Group` + aggregates
  (COUNT/SUM/AVG/MIN/MAX/GROUP_CONCAT/SAMPLE), `BIND`.
- **FILTER** (a deliberate subset): `BOUND`, `=`, `!=`, `<`, `<=`, `>`, `>=`, `&&`, `||`, `!`.

Today the evaluator does a full scan per triple pattern; an SPO/POS index with selectivity-based join
ordering is a tracked performance follow-on.

`ASK` returns a boolean; `CONSTRUCT` instantiates its template; `DESCRIBE` returns a concise bounded
description. `UPDATE` (`INSERT/DELETE DATA`, `DELETE/INSERT WHERE`, `CLEAR`, `CREATE`/`DROP GRAPH`) mutates
the graph through a `GraphStore`; `LOAD` is intentionally unsupported (the write path does no HTTP fetch).

### Not yet (🔶 in-progress)

- Sub-SELECT, `SERVICE`, `MINUS`, and negated property sets (`!p`) — additional algebra arms being added.
- FILTER functions outside `BOUND`/comparison/boolean (regex, arithmetic, `IN`, `STR`/`LANG`/`DATATYPE`,
  string/type builtins) — the expression evaluator is being extended.
- `FROM` / `FROM NAMED` in-query dataset clauses (the active dataset is the server registry today).
- An SPO/POS index + selectivity join-ordering (full-scan-per-pattern today).
- Content negotiation: results JSON only today; XML/CSV/TSV + Turtle output being added.

## How it's reached

SPARQL is served two ways: the W3C **`/sparql` HTTP endpoint** (`src/server/sparql_http.rs`, feature
`sparql-http` — GET `?query=`, POST `application/sparql-query`/`application/sparql-update`), and the binary
RPC method (`Method::Sparql`) with result-cache + RLS row-filtering. Companion RDF methods: `AddTriples`
(durable, replicated) and `GetRdf` (N-Triples export).

## Reasoning

Loaded ontologies are reasoned over by the OWL 2 EL⁺/RL engine — see the
[ontology lifecycle guide](ontology.md).
</content>
