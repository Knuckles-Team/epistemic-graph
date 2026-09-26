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
is in progress. AU's OCEL adapter renders the temporary AU `ChangeEnvelope`
transport shape; the EG semantic model does not import that AU type.

`epistemic_graph.ingestion.graph_slice` owns the canonical-key validation,
whole-slice replay digest, and primary-row projection for already-derived
nodes and edges. AU's current adapter consumes those functions and commits
through EG `ApplyChangeEnvelope`. `SourceIngest` requires catalog mapping
references and provider checkpoints for raw connector records; a derived
graph slice carries neither, so it must not fabricate them.

`epistemic_graph.ingestion.citation` owns content-pinned evidence resolution.
It accepts fragment-shaped records from the current AU reader and reports
`current`, `moved`, `stale`, or `lost`; duplicate content never selects a
replacement address by guesswork.

`epistemic_graph.ingestion.evidence_address` owns stable artifact and fragment
IDs plus normalized content digests. AU's current fragmenter calls these EG
functions; source object identity and content revision remain separate fields.

`epistemic_graph.ingestion.evidence_model` owns the `Artifact` and `Fragment`
types, graph vocabulary, and evidence graph projection. Its envelope input is
a structural protocol, so the engine never imports AU's delivery DTO. AU keeps
graph reads and commit routing as adapters and consumes the engine models.
`epistemic_graph.ingestion.evidence_fragmentation` owns pure markdown, PDF,
record, and rowset fragment extraction, returning those same engine models.

`epistemic_graph.ingestion.source_positions` owns typed provider cursors,
checkpoint decoding, cursor partitions, and advancing content versions. AU's
current native commit and cursor readers consume these pure derivations while
the durable write remains in EG. A derived slice still lacks the catalog
mapping and provider checkpoint that raw-record `SourceIngest` requires.

## Derived-slice commit decision for AUD-18

Use EG `ApplyChangeEnvelope` for **already-derived** node/edge slices. Do not
add a second derived-slice commit method. EG's `SourceIngest` handler itself
prepares a `ChangeEnvelope` and delegates its final graph/cursor transaction to
the same commit authority. `SourceIngest` adds raw admission, catalog mapping,
provider checkpoint, and source reconciliation before that commit. Those are
required for SDK-delivered raw source records, but unavailable for a derived
evidence, conformance, or process graph slice. Supplying synthetic values would
claim source authority that the derivation never had.

The existing path has the needed derived-slice semantics: the generated EG
client validates the envelope and binds the request ID, verified tenant,
principal, graph, and policy revision; EG dispatch rejects a mismatch against
verified request claims, classifies `ApplyChangeEnvelope` as a write, and runs
the graph ACL gate. The durable commit is atomic across graph operations,
content version, policies, lineage, and optional cursor. Its typed
`ChangeEnvelopeApplied` receipt carries batch identity, replay status, outbox
count, and projection-pending status. AU's current `ingest_graph_slice` adapter
requires an ambient `kg:write` GraphSession, rejects tenant mismatch, writes
policy rows for every projected object, and reports the native receipt and
replay status. Keep this route limited to engine-derived slices; SDK source
pages must use `SourceIngest` and consume its mapping/admission/checkpoint
receipt.

Focused client and AU adapter tests cover verified tenant binding, governed
auxiliary nodes and edges, receipt propagation, and replay reporting. A live
EG server acceptance run remains necessary to certify deployed ACL and
durable-store behavior at the final composed head.

`epistemic_graph.ingestion.process_conformance` owns frozen conformance runs,
deviation records, and deterministic graph projections. A caller supplies the
reference model; a worker checks traces against it without deriving a model
from those same traces. AU currently commits the resulting graph slice through
its existing adapter until the remaining SourceIngest cutover lands.

`epistemic_graph.ingestion.embedding_admission` owns the deterministic
content-class table, per-unit vector admission, SQL column classification,
and exact-content deduplication. The SQL field filter requires the caller's
protected structural and priority field set. AU supplies that set from its
current text projection; this keeps EG free of AU imports while preserving
the projection's field policy during the migration.
