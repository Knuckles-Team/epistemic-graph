# SPARQL & RDF interface

epistemic-graph is an RDF triple-store: you load RDF (OWL/RDFS or plain triples), the engine maps it
onto the property graph, and SPARQL `SELECT` runs over it. RDF and the property graph are **the same
data** — a triple is a node, an edge, or a property depending on its object.

> Status snapshot: SPARQL `SELECT`/`ASK`/`CONSTRUCT`/`DESCRIBE`, `UPDATE`, true named graphs, and the W3C
> `/sparql` HTTP endpoint (feature `sparql-http`) are all supported. Content negotiation, rich FILTER,
> sub-SELECT, `SERVICE` federation, `MINUS`, negated property sets (`!p`), `ORDER BY`/`VALUES`/`EXISTS`,
> `FROM`/`FROM NAMED`, and an SPO/POS index with selectivity join-ordering are **shipped**
> (EG-050..057, EG-KG.ontology.order-by-values-exists/135). GeoSPARQL + RCC8/Egenhofer (EG-KG.ontology.concept-10/155), the full RDF serialization matrix
> (JSON-LD/TriG/N-Quads/RDF-XML, EG-KG.ontology.eg-concrete-syntax-matrix/137), SHACL/ShEx/ICV validation (EG-KG.ontology.concept-6/133/146), and OBDA/R2RML
> virtual graphs (EG-101) are also in. See the [capability matrix](../capabilities.md#sparql-eg-rdf).

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

- **Patterns**: BGP, property paths (`^p`, `p/q`, `p|q`, `p+`, `p*`, `p?`, negated `!p` — EG-KG.ontology.negated-property-set), `OPTIONAL`,
  `UNION`, `MINUS` (EG-KG.ontology.minus), sub-`SELECT` (EG-KG.ontology.sub-select), `SERVICE` federation (EG-KG.query.sparql-service-federation-client), `VALUES` inline data.
- **Solution modifiers**: `Project`, `Distinct`, `Reduced`, `Slice` (LIMIT/OFFSET), `ORDER BY` (multi-key
  ASC/DESC with SPARQL term ordering, incl. top-k `ORDER BY … LIMIT` — EG-KG.ontology.order-by-values-exists/135), `Group` + aggregates
  (COUNT/SUM/AVG/MIN/MAX/GROUP_CONCAT/SAMPLE), `BIND`, `FROM`/`FROM NAMED` dataset scoping (EG-KG.ontology.from-from-named).
- **FILTER** (rich — EG-KG.ontology.rich-filter/127): arithmetic, `REGEX`, `IN`, `EXISTS`/`NOT EXISTS`, `STR`/`LANG`/`LANGMATCHES`/
  `DATATYPE`/`BOUND`, `isIRI`/`isLiteral`/`isBlank`/`isNumeric`, string funcs
  (`CONTAINS`/`STRSTARTS`/`STRENDS`/`STRLEN`/`SUBSTR`/`UCASE`/`LCASE`/`CONCAT`/`STRBEFORE`/`STRAFTER`/`REPLACE`),
  hashes (`MD5`/`SHA*`), date-time (`NOW`/`YEAR`…/`TZ`), numeric (`RAND`/`ABS`/`ROUND`/`CEIL`/`FLOOR`),
  term constructors (`IRI`/`BNODE`/`STRDT`/`STRLANG`/`UUID`), `COALESCE`, `IF`, with datatype-aware comparison.

Query execution uses **hashed SPO/POS/PSO indexes** over the snapshot with cardinality-based BGP
reordering (EG-KG.ontology.capability-catalog) — no longer a full scan per triple pattern.

`ASK` returns a boolean; `CONSTRUCT` instantiates its template; `DESCRIBE` returns a concise bounded
description. `UPDATE` (`INSERT/DELETE DATA`, `DELETE/INSERT WHERE`, `CLEAR`, `CREATE`/`DROP GRAPH`, and the
graph-management `COPY`/`MOVE`/`ADD` — EG-134) mutates the graph through a `GraphStore`; `LOAD` is
intentionally unsupported (the write path does no HTTP fetch).

## Content negotiation & serialization matrix (EG-KG.ontology.content-negotiation-serializers/136/137)

`/sparql` is `Accept`-aware, with a per-form default and an `output=`/`format=` override:

- **SELECT/ASK** → SPARQL-results JSON / XML / CSV / TSV.
- **CONSTRUCT/DESCRIBE** → Turtle, N-Triples, and — completing RDF 1.1 — **JSON-LD 1.1** (expansion/
  compaction, `@id`/`@type`/`@graph`), **TriG** + **N-Quads** (named-graph-aware), and **RDF/XML**.

The same matrix is available on ingest (parse) and via the W3C SPARQL 1.1 **Graph Store Protocol**
(GET/PUT/POST/DELETE on `/rdf-graphs/…?graph=`, EG-134) for direct RDF-tooling read/write.

## GeoSPARQL — spatial SPARQL (EG-KG.ontology.concept-10/155, feature `geosparql`)

The `geo:`/`geof:` vocabulary, WKT/GML geometry literals, and topological FILTER functions lower onto the
eg-geo spatial predicates (see [gis](gis.md)):

- **Baseline** (EG-KG.ontology.concept-10): `geof:sfWithin`/`sfIntersects`/`distance`.
- **RCC8** (EG-KG.ontology.concept-7): `geof:rcc8eq/dc/ec/po/tpp/ntpp/tppi/ntppi`.
- **Egenhofer** (EG-KG.ontology.concept-7): `geof:ehEquals/disjoint/meet/overlap/covers/coveredBy/inside/contains`.

```sparql
SELECT ?site WHERE {
  ?site geo:hasGeometry/geo:asWKT ?g .
  FILTER (geof:sfWithin(?g, "POLYGON((…))"^^geo:wktLiteral))
}
```

## Validation — SHACL / ShEx / ICV

- **SHACL** (EG-KG.ontology.concept-6, crate `eg-shacl`): node/property shapes, `sh:targetClass`/`targetNode`,
  cardinality/datatype/pattern/`sh:in`/logical + SPARQL-based constraints → an `sh:ValidationReport` via
  `Method::ShaclValidate`.
- **ShEx** (EG-KG.compute.concept-2): shape-expression schema validation (shape refs, triple constraints, cardinality, node
  constraints, AND/OR/NOT) via `Method::ShexValidate`.
- **ICV** (EG-KG.ontology.wired-into-commit-write): Stardog-style Integrity Constraint Validation — interpret SHACL shapes under the
  closed-world / unique-name assumption as database integrity constraints, report violations with a
  focus-node + failing-constraint + a SPARQL "explain" witness, with an optional **guard mode** that
  rejects a write/transaction that would violate a constraint (and also runs in the OWL-reasoned view).

## OBDA / virtual graphs — R2RML (EG-101)

R2RML-style `:TriplesMap` mappings (logicalSource → subject/predicate/object templates) expose a
foreign/relational source (via the EG-073 `ForeignSource` registry) as a **virtual** RDF graph: SPARQL over
the virtual graph rewrites to an `Op::ForeignScan` against the backing source — no materialization.

## How it's reached

SPARQL is served two ways: the W3C **`/sparql` HTTP endpoint** (`src/server/sparql_http.rs`, feature
`sparql-http` — GET `?query=`, POST `application/sparql-query`/`application/sparql-update`, content-negotiated
output), and the binary RPC method (`Method::Sparql`) with result-cache + RLS row-filtering. Companion RDF
methods: `AddTriples` (durable, replicated), `GetRdf` (serialization-matrix export), and the validation
methods above. `SERVICE` fan-out obeys the `EPISTEMIC_GRAPH_SPARQL_SERVICE_ALLOW` SSRF allowlist
(fail-closed by default).

## Reasoning

Loaded ontologies are reasoned over by the OWL 2 EL⁺/RL engine — see the
[ontology lifecycle guide](ontology.md).
</content>

---

**See also:** [Capabilities matrix](../capabilities.md) · [Ontology lifecycle](ontology.md) · [Cypher & Bolt](cypher.md) · [GraphQL](graphql.md) · [GIS / Spatial](gis.md) · [Connecting (per-wire guide)](connecting.md).
