---
name: kg-modality-reasoning
skill_type: skill
description: >-
  Run OWL / rule reasoning inside the engine — OWL 2 EL⁺/RL completion, OWL-DL tableau, SWRL
  user rules, forward-chaining materialization, classification and consistency checking over
  the RDF/OWL store. Use when you need to infer implied facts, classify individuals, check an
  ontology for consistency, or materialize a reasoned view; via the engine_reasoning MCP tool.
domain: modality
license: MIT
tags: [epistemic-graph, engine, reasoning, owl, inference, rules, modality]
tier: modality
wraps: [engine_reasoning]
metadata:
  author: Genius
  version: '0.1.0'
---

# kg-modality-reasoning — OWL / rule inference in the engine

The engine reasons natively over its own RDF/OWL store (`eg-rdf/owl`). It performs OWL 2 EL⁺
completion (sub/conj/some/chain/subrole/bot/disjoint), OWL 2 RL property rules
(transitive/symmetric/inverse/chains/domain), classification and consistency (an unsatisfiable
class ⇒ inconsistent), and **forward-chaining materialization** with incremental `add_axioms`
(a monotone fixpoint resumed in place). DL-requiring ontologies route to a pure-Rust
description-logic **tableau** (`owl-dl`: cardinality, `complementOf`, nominals) while the
EL/RL fast path stays default. **SWRL** user rules add a Horn-rule DSL + `swrlb:` built-ins.
Confidence-weighting (per-axiom `eg:confidence`, noisy-OR), Ebbinghaus time-decay, and
distributed/cross-shard reasoning (one closure over a unioned TBox+ABox) are supported.
Reasoning is also a query-time op (`Op::Reason` under `owl-plan` seeds a RowSet). See
`docs/capabilities.md` → *OWL reasoning (`eg-rdf/owl`)*.

## The MCP way (through graph-os)
```
load_tools(tools=["engine_reasoning"])   # then classify / materialize / check-consistency
```
or the REST twin graph-os exposes for the reasoning modality.

## The wire way
Reasoning composes with the SPARQL surface (`kg-modality-sparql`): ICV integrity-constraint
validation can run over the **OWL-reasoned view**, and a reasoned RowSet is reachable from a
plan via `Op::Reason`. There is no separate reasoning port — it operates on the triples served
by `/sparql` (`EPISTEMIC_GRAPH_SPARQL_ADDR`).

## Cross-modal seam
Materialized inferences are written back into the same store the SQL/RDF/vector modalities
read, so a reasoned closure is visible to `kg-modality-sql` and `kg-modality-sparql` and is
committed under the engine's unified cross-modal transaction — inference is a first-class
citizen of the shared substrate, not a bolt-on batch job.

## Related
- `kg-modality-sparql` — the RDF store the reasoner classifies and materializes over.
