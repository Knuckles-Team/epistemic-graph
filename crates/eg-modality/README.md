# eg-modality

`eg-modality` is the dependency-light contract and governance layer shared by the
engine's typed modalities. It contains three complementary surfaces:

- `ModalityContract`, the registry, and the 12-point compatibility kit (TCK);
- the universal `Artifact → Occurrence → Rendition → Segment → Feature →
  EvidenceLocus` protocol; and
- `ServedModalityRuntime<T>`, a storage-adapter-neutral state machine for governed
  ingest, update, delete, lifecycle, query, CDC replay, and recovery.

The crate remains below `eg-plan` and `eg-core` in the dependency graph. Its durable
protocol accepts only validated `OpaqueRef` values. Source paths, host or user names,
URLs, mail addresses, raw personal data, and raw modality payloads are not protocol
fields.

## Universal artifact protocol

`ArtifactBundle` is the atomic, modality-neutral commit envelope:

| tier | purpose |
|---|---|
| `Artifact` | stable content identity and modality/schema |
| `Occurrence` | governed observation of an artifact |
| `Rendition` | derived representation with reproducible lineage |
| `Segment` | stable addressable part of a rendition |
| `Feature` | typed derived value referenced through an opaque value id |
| `EvidenceLocus` | exact, policy-bound location supporting a result |

`ArtifactBundle::validate_certified` requires all six tiers, valid referential
integrity, acyclic derivations, valid coordinates, policy metadata, derivation
metadata, and a passing `PrivacyAttestation`. `OpaqueRef` and every typed id have
custom deserialization so persisted input cannot bypass lexical or namespace
validation.

`EvidenceLocus` is the only located-evidence identity. Modality contracts expose
an `EvidenceAddress`; the governed ingestion boundary binds it to opaque subject,
policy, derivation, and locus identities before storage or serving.

## Served runtime

`ServedModalityRuntime<T>` provides:

- content-sensitive idempotency and optimistic version checks;
- atomic bounded/iterator ingest;
- exact tenant, access-policy, purpose, and classification checks;
- legal-hold-aware deletion and active/cold/tombstoned lifecycle states;
- modality, segment, lexical, spatial, temporal, and signature indexes with bounded
  typed predicates and monotonic CDC events; and
- deterministic snapshot/recovery with idempotency-ledger persistence and index
  rebuild validation.

`T` must implement `GovernedModality`; there is no permissive default. Each served
leaf validates its own payload fields and rejects raw text/display labels, unsafe ids,
and malformed coordinates before persistence.

The runtime is deliberately adapter-neutral. An engine adapter commits its snapshot
inside the engine's authoritative transaction; source bytes stay ephemeral or live in
an approved encrypted content-addressed store.

Document, image, audio, and video expose served/runtime features. Their native
implementations perform bounded UTF-8 layout + authority-keyed lexical extraction,
full 8-bit PNG pixel reconstruction + perceptual hashing, PCM/WAV waveform and
spectral feature extraction, and ISOBMFF track/sample/frame extraction with current
raw-RGB decode. SHA-256 binds every normalized record to its source while raw text,
pixels, PCM, encoded frames, paths, endpoints, and identity values stay out of durable
records.

## TCK meaning

Every `ConformanceTestable` implementation is evaluated against these 12 dimensions:

1. stable schema/identity;
2. batch and streaming ingest;
3. codec/unsupported-format behavior;
4. storage, secondary index, and statistics;
5. typed query operators;
6. transaction or declared saga/outbox behavior;
7. CDC, deletion, retention, and GC;
8. tenant/row/region policy;
9. provenance, evidence, and lineage;
10. backup and restore;
11. single-node recovery; and
12. interoperability/workload smoke.

`is_first_class()` permits a documented `N/A` for a modality that cannot own a
dimension. Production serving is stricter: `is_production_ready()` requires exactly
12 `PASS` results, no `N/A`, and a passing native codec/normalization/index/query/
resource-bound probe. Document, image, audio, and video are asserted as production
modalities by the fleet TCK. Tensor and geo retain
their existing first-class/non-production semantics where a dimension is genuinely
owned by a higher storage layer.

## Verification

Run these serially on a resource-constrained host:

```bash
python scripts/check_p2_modality_architecture.py
cargo test -p eg-modality --test fleet_tck -- --nocapture
cargo test -p eg-modality --test served_runtime
cargo test -p eg-document --features serving
cargo test -p eg-image --features runtime
cargo test -p eg-audio --features runtime
cargo test -p eg-video --features runtime
cargo test -p eg-plan --features knowledge-batch result_stream
```

The source-only implementation intentionally does not make local filesystem paths,
private endpoints, personal identifiers, or raw source content part of examples,
snapshots, events, or error messages.
