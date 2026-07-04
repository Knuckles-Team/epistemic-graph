---
name: kg-modality-sparql
description: >-
  Query and update the engine's RDF/OWL store over the W3C SPARQL 1.1 Protocol — the engine
  IS the triplestore (epistemic-graph owns the `/sparql` wire). Use when you need SELECT/ASK/
  CONSTRUCT/DESCRIBE/UPDATE over triples/quads, named graphs, property paths, or when an
  existing rdflib/Jena/Stardog client should point at the engine; via the engine_rdf MCP tool
  or a raw SPARQL HTTP POST.
domain: modality
license: MIT
tags: [epistemic-graph, engine, sparql, rdf, owl, wire, modality]
tier: modality
wraps: [engine_rdf]
metadata:
  author: Genius
  version: '0.1.0'
---

# kg-modality-sparql — RDF/SPARQL over the engine wire

The engine is a first-class **SPARQL 1.1** endpoint (`eg-rdf` crate). It serves the full query
matrix — `SELECT`/`ASK`/`CONSTRUCT`/`DESCRIBE`, BGP + property paths, OPTIONAL/UNION/MINUS,
aggregates, sub-SELECT, `VALUES`, true named graphs (`FROM`/`FROM NAMED`/`GRAPH ?g`), SPARQL
1.1 `UPDATE` (`INSERT/DELETE DATA`, `DELETE/INSERT WHERE`, `CREATE/DROP GRAPH`), SHACL/ShEx
validation and `SERVICE` federation. RDF-star and the JSON-LD/TriG/N-Quads/RDF-XML
serialization matrix are supported. See `docs/capabilities.md` → *SPARQL (`eg-rdf`)*.

## The wire way (epistemic-graph owns it)
The `/sparql` HTTP listener (feature `sparql-http`) is **opt-in**: build with `sparql-http`
and set `EPISTEMIC_GRAPH_SPARQL_ADDR` (`--sparql-addr`, default `127.0.0.1:7878`). Any
existing SPARQL client then works unchanged (W3C SPARQL 1.1 Protocol, GET + POST):

```bash
# query
curl -s 'http://127.0.0.1:7878/sparql' \
  --data-urlencode 'query=SELECT ?s ?p ?o WHERE { ?s ?p ?o } LIMIT 5'
# update
curl -s 'http://127.0.0.1:7878/sparql' \
  --data-urlencode 'update=INSERT DATA { <urn:a> <urn:knows> <urn:b> }'
```

Content negotiation returns SPARQL-results JSON/XML/CSV/TSV (or Turtle/N-Triples for
CONSTRUCT/DESCRIBE). Full recipe + env table: `docs/interfaces/connecting.md` → *SPARQL 1.1*.

## The MCP way (through graph-os)
```
load_tools(tools=["engine_rdf"])   # then call engine_rdf with a SPARQL query/update
```
or the REST twin exposed by graph-os for the RDF modality.

## Cross-modal seam
The triplestore is not a silo: RDF triples live in the **same** store as the SQL `nodes`
table, vectors, and time-series, so a SPARQL write participates in the engine's unified
**cross-modal ACID transaction** (graph + RDF + vector + blob in one `WriteTransaction`), and
OWL reasoning (`kg-modality-reasoning`) materializes inferences over the same triples.

## Related
- `kg-modality-reasoning` — OWL-RL/DL inference + materialization over these triples.
- `kg-modality-sql` — the SQL wire onto the same nodes; JOINable across modalities.
