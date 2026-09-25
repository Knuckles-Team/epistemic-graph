# Source ingestion semantics

`epistemic_graph.ingestion.semantic_event_model` owns the validated OCEL 2.0,
temporal object-state, and neural proposal boundary used by source ingestion.
`ObjectCentricGraphSlice` converts that source truth to and from the canonical
node and edge projection without losing qualified relationships or temporal
attribute revisions. Its digest is stable across equivalent input ordering.

`epistemic_graph.ingestion.object_centric_derivation` owns deterministic
incremental derivation over those models. A newly arrived event updates only
its adjacent per-object directly-follows edges. Corrections and late arrivals
advance a derivation generation so readers can pin the derived view.

These Python types and pure derivations make no graph writes. `SourceIngest`
remains the durable commit boundary. The AU process-mining and OCEL adapters
consume these EG-owned types while the rest of the EH-498 ingestion migration
is in progress.

`epistemic_graph.ingestion.process_conformance` owns frozen conformance runs,
deviation records, and deterministic graph projections. A caller supplies the
reference model; a worker checks traces against it without deriving a model
from those same traces. AU currently commits the resulting graph slice through
its existing adapter until the remaining SourceIngest cutover lands.
