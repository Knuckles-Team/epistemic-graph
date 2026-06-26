# SPARQL & RDF interface

epistemic-graph is an RDF triple-store: you load RDF (OWL/RDFS or plain triples), the engine maps it
onto the property graph, and SPARQL `SELECT` runs over it. RDF and the property graph are **the same
data** — a triple is a node, an edge, or a property depending on its object.

> Status snapshot: SPARQL `SELECT` is supported with a broad algebra subset. `ASK`/`CONSTRUCT`/
> `DESCRIBE`/`UPDATE` and a `/sparql` HTTP endpoint are 🔶 in-progress. See the
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

### Not yet (errors honestly)

- Non-SELECT query forms error `eg-rdf SPARQL supports SELECT only`.
- Sub-SELECT / `SERVICE` / `MINUS` error `unsupported algebra node`.
- Negated property sets (`!p`) error explicitly.
- FILTER functions outside the subset above (regex, arithmetic, `IN`, `STR`/`LANG`) are not yet wired.

## How it's reached today

SPARQL is currently a binary RPC method (`Method::Sparql`) on the engine protocol, with result-cache and
RLS row-filtering applied before evaluation. A standards `/sparql` HTTP endpoint is
[in progress](../roadmap.md#sparql-toward-full-stardoggraphdb-parity). Companion RDF methods:
`AddTriples` (durable, replicated) and `GetRdf` (N-Triples export).

## Reasoning

Loaded ontologies are reasoned over by the OWL 2 EL⁺/RL engine — see the
[ontology lifecycle guide](ontology.md).
</content>
