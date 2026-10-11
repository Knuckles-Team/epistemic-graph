# EG-ORCH-VOCAB-001 — Implementation plan

The architecture and vocabulary decisions are specified in [design.md](design.md).
Reuse the existing orchestration competency, decision-engine and typed-pack
contracts rather than introducing a second planner or an execution runtime in EG.

## Implementation sequence

1. Add the declared ontology terms and SHACL constraints to the existing schema
   sources. Validate missing rule scope and malformed agent descriptions with
   refusal fixtures before changing readers.
2. Write the agent-kind and required-kind projections from admitted records.
   Preserve the source record and provenance for each projection.
3. Extend the assembly reader to filter slots by the required agent kind and
   return a typed abstention when no compatible agent is available.
4. Exercise the standalone Turtle example, projection queries and negative
   fixtures defined in [test-spec.md](test-spec.md). Keep served/client acceptance
   dependencies open until the corresponding runtime evidence exists.

Each child follows [requirements.md](requirements.md) and the generated delivery
record. Regenerate that record after implementation commits; specification-only
changes do not establish that a runtime capability has been delivered.
