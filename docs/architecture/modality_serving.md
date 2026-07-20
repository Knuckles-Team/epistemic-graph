# Governed modality serving and native KnowledgeBatch

Document, image, audio, and video are production-capable modality runtimes in the
main build. Release certification is granted only by the G-14 exact-binary campaign;
the leaf TCK establishes component conformance, not release readiness. The runtimes
share one storage-neutral identity and governance protocol, one served lifecycle
state machine, and one streaming query/job result currency.

## Universal artifact protocol

`eg-modality::ArtifactBundle` is the atomic semantic payload for every modality:

| Tier | Meaning |
|---|---|
| `Artifact` | Logical content identity and content version |
| `Occurrence` | One source observation with tenant and policy |
| `Rendition` | A derived representation with reproducible derivation |
| `Segment` | A page, paragraph, region, time range, shot, frame range, row, symbol, or trace span |
| `Feature` | An embedding, statistic, signature, label, or quality metric |
| `EvidenceLocus` | An exact numeric or opaque address inside a governed resource |

The bundle also carries `PolicyEnvelope`, `Derivation`, and
`PrivacyAttestation`. Structural validation rejects duplicate identities, dangling
references, conflicting or cyclic derivations, invalid coordinates, unsupported
protocol versions, and failed privacy attestations. Production certification
additionally requires all six tiers.

Every durable identifier uses `OpaqueRef`. Its validated lexical form cannot contain
a URL, email address, local path, host/user name, display name, or whitespace. Raw
payloads remain outside served state. A separately governed content-addressed store
may resolve their irreversible addresses, but modality ingest does not write or
retain source bytes. Deserialization runs the same validation as construction, so a
decoded payload cannot bypass the privacy boundary.

## Served lifecycle

`ServedModalityRuntime<T>` supplies the common operational behavior:

- atomic batch and iterator-driven streaming ingest;
- content-sensitive durable idempotency;
- optimistic versioned update;
- policy-filtered modality/segment/native-posting query and stable paging;
- delete propagation with legal-hold enforcement;
- event-fenced tombstone collection after retention is demonstrated;
- monotonic CDC/replay events;
- active/cold/restore lifecycle transitions;
- deterministic snapshot, restart validation, and index rebuild.

Every mutation installs authoritative state before its event becomes visible. Batch
ingest uses a touched-record undo journal and commits only if the complete iterator
succeeds. A failed element therefore cannot leave a partial prefix. Delete removes the
normalized payload and every posting while retaining only the opaque governance/audit
envelope. Recovery rebuilds modality, segment, lexical, spatial, temporal, and
signature postings from validated authoritative records before queries are served.

## Live graph service

The `modality-serving` facade feature exposes one graph-scoped
`Method::ServedModality` operation on the normal authenticated MessagePack transport.
It is included in `full`; there is no sidecar, local script, filesystem convention,
or source-specific server configuration.

| Operation | Authority | Effect |
|---|---|---|
| `authority` | verified request context | Returns HMAC-derived tenant, access-policy, and purpose references for bundle construction |
| `ingest` | graph write + exact occurrence policy | Runs the concrete native decoder and atomically creates/updates the served occurrence |
| `ingest_stream` | graph write + exact occurrence policy | Validates and atomically applies two to 64 records with all-or-nothing rollback |
| `query` | graph read + exact row/classification policy | Returns bounded, stably paged typed records |
| `native_query` | graph read + exact row/classification policy | Executes a closed document-lexeme, image-region/pHash, audio-window, or video-window predicate through bounded native postings and exact filtering |
| `delete` | graph write + exact occurrence policy | Applies OCC, legal-hold, tombstone, and payload-erasure rules |
| `move_to_cold` / `restore` | graph write + exact occurrence policy | Applies the governed lifecycle transition |
| `events` | graph read + exact event occurrence policy | Returns bounded monotonic replay events |
| `stats` | management scope | Returns bounded aggregate storage/index/event counts without identifiers or source data |
| `collect_tombstones` | management scope | Collects eligible tombstones only through an explicit observed event fence |
| `capabilities` | graph read | Returns the component TCK result only: 12 PASS / 0 N/A |

Every operation requires an `eg2.` verified `RequestContext`. The server ignores the
request envelope's display identity for policy construction. It derives irreversible
tenant/policy/purpose references with the server authentication secret and keeps the
raw subject, tenant, roles, scopes, delegation chain, and policy version in request
memory only. `kg:admin` or the explicit
`modality:classification:restricted` scope permits Restricted data;
`modality:classification:confidential` permits Confidential data; otherwise the
boundary is Internal.

The producer calls `authority`, constructs a certified `ArtifactBundle` with exactly
those opaque references, then sends the bundle and source bytes to `ingest` or a
bounded `ingest_stream`. The handler verifies that every occurrence in the returned
bundle has the same authority
and that the target artifact's opaque content token equals the content address
produced by the concrete decoder. A caller therefore cannot attach a trusted envelope
to different bytes or make an authorized target carry cross-policy metadata.

### Python client contract

The full Python client exposes this method as `client.modalities`. Its public methods
map one-for-one to the operations above; there is no generic execution escape hatch and
no compatibility alias. Inputs are validated before transport: modality and segment
enums must be current, occurrence identifiers must use the opaque `occurrence`
namespace, byte payloads must be non-empty, numeric bounds must fit the Rust wire
types, and response maps must contain exactly the current fields. Returned artifact
bundles must expose the current `evidence_loci` tier; retired evidence-span shapes are
rejected.

```python
authority = await client.modalities.authority()
page = await client.modalities.query(
    "image",
    segment_kind="region",
    limit=50,
    include_cold=False,
)
documents = await client.modalities.search_documents("boundedterm", page=2)
images = await client.modalities.query_similar_images(0x1234, maximum_distance=7)
audio = await client.modalities.query_audio_window(
    start_ms=100,
    end_ms=900,
    minimum_rms=0.25,
)
video = await client.modalities.query_video_window(
    start_ms=0,
    end_ms=1000,
    keyframes_only=True,
)
events = await client.modalities.events("image", after_sequence=0, limit=100)
stats = await client.modalities.stats("image")  # management scope
collected = await client.modalities.collect_tombstones(
    "image", through_event_sequence=events[-1]["sequence"]
)
component_tck = await client.modalities.capabilities("image")
```

`authority` supplies the opaque references used while constructing a certified bundle;
it is not a deployment profile. Endpoint, credential, certificate, filesystem, and
source-system settings remain external connection configuration and never enter a
modality operation or durable bundle.

### Commit and privacy boundary

Mutations run through `commit_conditional_mutation` against a complete staged graph
image. In authoritative-redb mode the resulting graph snapshot, result, version,
fence, status, and outbox commit before the live graph projection is published.
Placement-aware deployments accept mutations only at the current leader. The leader
runs policy validation and native decoding against the authoritative pre-image, then
submits a separate `SanitizedModalityRaftCommand`: an HMAC-authenticated opaque node
id, AEAD-sealed runtime value, state digest, operation category, digest-only receipt,
and compact `ApplyOutcome`. The public source-bearing `ServedModality` method is
never placed in the Raft log. Every follower validates the HMAC, ciphertext marker,
partition shape, digest, receipt, result type, and resource ceiling, merges that one
encrypted node into its current authoritative image, and commits it through the same
state-backed MutationBatch before publishing RAM. A replay restores the committed
image/result by batch id and cannot duplicate audit, outbox, or CDC effects.

The graph stores one runtime node per opaque authority partition and modality. Its
node id is HMAC-derived and its complete runtime snapshot is sealed with
ChaCha20-Poly1305 using separate server-derived key material. Reads reject an
unsealed value. Even an ordinary graph dump therefore sees only an opaque node id and
AEAD ciphertext, while the served handler independently applies exact policy checks
to records and events.

Source bytes are decoded only in request memory. They are absent from runtime
snapshots, graph properties, audit lines, CDC events, status/outbox rows, and durable
MutationBatch operations. The durable operation is a SHA-256 descriptor over only
the operation category and already-opaque references; the independently verified
state descriptor binds the authenticated state image. Before sealing, ingest scans
the serialized plaintext normalized snapshot and rejects any surviving source
sequence. Its tamper-evident audit link retains only that digest;
CDC retains only the modality category. Neither contains an occurrence id, source
reference, local path, endpoint, user, or raw content.

The transport rejects an oversized length prefix before allocating its payload, and
the handler applies stricter modality limits before native decoding or snapshot
construction:

| Setting | Default | Hard ceiling |
|---|---:|---:|
| `EPISTEMIC_GRAPH_MAX_REQUEST_BYTES` | 64 MiB | 384 MiB |
| `EPISTEMIC_GRAPH_MODALITY_MAX_SOURCE_BYTES` | 16 MiB | 256 MiB |
| `EPISTEMIC_GRAPH_MODALITY_MAX_BUNDLE_BYTES` | 4 MiB | 32 MiB |

Values must be positive integers. Missing or invalid values use the defaults; values
above the hard ceiling are clamped. Query pages are bounded to 1,000 records and
event pages to 10,000 by `ServedModalityRuntime`.
When either modality limit is raised above its default, the request-frame limit must
also be raised enough to contain the source, bundle, and small protocol envelope.
An encrypted modality Raft state command has an independent 128 MiB hard ceiling and
a 4 KiB terminal-result ceiling; it contains neither source bytes nor their direct
content hash.

`GovernedModality` is an additional mandatory leaf-level validator. The generic
envelope cannot inspect modality-specific strings or coordinates, so each production
payload explicitly rejects raw labels/text, non-opaque ids, malformed ranges, and
non-content-addressed blob handles before the state machine can persist it. Document
table text belongs in an approved CAS/Feature value, never inline in a served
`DocumentData` record.

The artifact binding is SHA-256. The lexical index contains only authority-keyed HMAC
references; a raw query term is normalized and transformed in request memory before
posting access. Spatial predicates use normalized image coordinates, temporal
predicates are capped at 4,096 one-second posting buckets, and perceptual similarity
uses four 16-bit postings with a bounded multi-probe that guarantees recall across
the supported Hamming radius, followed by exact distance filtering.

The four public serving types are:

- `eg_document::DocumentServingRuntime` and `NativeDocumentRuntime`;
- `eg_image::ImageServingRuntime` and `NativeImageRuntime`;
- `eg_audio::AudioServingRuntime` and `NativeAudioRuntime`;
- `eg_video::VideoServingRuntime` and `NativeVideoRuntime`.

## Dependency-light native execution

The native runtimes do real work without native libraries or external processes:

| Modality | Native behavior |
|---|---|
| Document | Bounded UTF-8/form-feed pages, heading/list/paragraph/table layout, exact Unicode-scalar character spans, private lexical postings; source text is not durable |
| Image | Strict CRC-checked 8-bit PNG decode, filters and RGBA conversion, 4K-bounded pixel working set, 64-bit difference hash, spatial-grid and pHash predicates |
| Audio | Strict 8/16-bit PCM/WAV decode, bounded complete-coverage peak/RMS/spectral windows, energy VAD, opaque-channel grouping, temporal/RMS predicates |
| Video | Strict ISOBMFF brand/track/sample-table extraction, mdat range validation, frame timing/keyframes, current 24-bit raw-RGB frame decode, temporal predicates |

The runtime reports only operations it actually executes. Compressed video samples
remain exact encoded frame slices rather than being mislabeled as decoded pixels.

## Component TCK and release certification

The internal `TckReport::is_production_ready()` predicate requires all 12 core points
to be `PASS`, no `N/A`, and a passing native production probe. The public capabilities
response exposes this only as `component_ready`, `component_pass`,
`component_not_applicable`, and `component_total`; it does not claim release
readiness. The native probe executes the concrete codec,
source-free normalization, secondary-index generation, typed predicate, malformed
input rejection, and resource bound. The
fleet test registers document, image, audio, and video and asserts exactly 12 passes
and zero N/A results plus the probe for each.

Production release readiness additionally requires a passing G-14 campaign against
the sealed release binary, including exact artifact round trips, authorization,
crash/restart and restore migration, retention-fenced collection, malformed/resource
rejection, the full four-by-four fault matrix, raw-source exclusion, and same-artifact
G-37 performance evidence.

| Core proof | Served implementation |
|---|---|
| Identity/schema | Universal typed IDs and versioned bundle |
| Batch/stream ingest | Atomic `ingest` / `ingest_stream` |
| Codec/malformed input | Validated native codecs and staged codec rejection |
| Storage/index/stats | Normalized record plus modality/segment/native posting indexes |
| Typed query | Closed wire predicates, posting candidate selection, and exact filtering |
| Transaction | One staged governed record |
| CDC/delete/retention | Monotonic event stream, tombstone, policy envelope |
| Tenant/region policy | Exact policy-scope matching |
| Provenance/evidence | Derivation plus exact loci |
| Backup/migrate/recover | Snapshot round trip and validation |
| Failure/restart | Event cursor and index reconstruction |
| Interop/workload | Common contract and KnowledgeBatch stream |

`crates/eg-modality/tests/served_runtime.rs` also ingests 4,096 governed records,
executes a selective native lexical query, asserts that only the 64 posting candidates
are examined, snapshots/rebuilds the runtime, and repeats the same bound after
recovery. Leaf probes cover format-specific malformed and structural ceilings.

## Native streaming result currency

`Method::KnowledgeStream` is the single served pull protocol, and
`eg-plan::KnowledgeBatchStream` is its bounded result plane for every family:

| Family | Adapter |
|---|---|
| Graph | `graph_result_stream` |
| SQL | `sql_result_stream` |
| RDF | `rdf_result_stream` |
| Vector | `vector_result_stream` |
| Time series | `time_series_result_stream` |
| Analytics jobs | `job_result_stream` |
| Cross-modal plans | `cross_modal_result_stream` |

Each stream requires opaque tenant, access-policy, placement, snapshot, query,
derivation, and evidence-set references. Tenant and access-policy references are
keyed from the already verified RequestContext; query, snapshot, evidence-set, and
row identities are keyed as well. Raw tenant, principal, agent, role, scope,
delegation, policy, graph, query, and source-row strings never enter a cursor or
native result row. The adapter injects the opaque references into every row before
invariant validation. It rejects
invalid score schemas, non-finite values, reversed temporal or evidence ranges,
unsafe path-like evidence, and rows that bypass governance context.

The request contains one typed query variant (`Graph`, `Sql`, `Rdf`,
`Vector`, `TimeSeries`, `Job`, or `CrossModal`), a non-zero batch size, and an optional
cursor. A request without a cursor opens the snapshot; each response carries one Arrow
IPC batch and the cursor for the next pull. The dispatch point is after verified
RequestContext scope enforcement, graph ACL/RLS filtering, lazy-materialization
readiness, and authoritative placement resolution. The cursor binds result family,
tenant, access policy, placement epoch/fence, the complete result snapshot, query,
derivation, evidence set, schema version, and batch size. Changing authority,
placement, data, query, family, or batch size therefore fails closed instead of
replaying a cursor against a different view. A keyed integrity reference also covers
the cursor's row offset, batch index, and exhaustion bit, so clients cannot forge a
different position while retaining valid authority references.

`write_arrow_ipc` writes one Arrow `RecordBatch` at a time to a sink; the served method
uses the same adapter and returns one bounded batch per request. The adapter never
materializes the complete result. Some existing family executors still build their
historical intermediate before adaptation; that is an executor concern, not a second
served result contract, and callers still receive bounded batches with pull
backpressure.

Arrow IPC is the sole result projection. Every result family uses
`KnowledgeStream`, bounded pull batches, and the same authority-bound resumable
cursor. There are no direct-query aliases or alternate family payloads.

The Python `client.knowledge.pull(...)` binding fixes `schema_version` to `1` and
projection to `arrow_ipc_v1`. Callers provide exactly one current typed family query,
a batch size from 1 through 65,536, and optionally the cursor returned by the prior
pull. The binding rejects unknown query fields, a cursor for another family or batch
size, invalid opaque cursor references, non-finite vector values, and any response that
does not preserve the requested family, projection, and cursor contract. The Arrow IPC
payload remains bytes; decoding it is separate from the engine's sole result protocol.

```python
batch = await client.knowledge.pull(
    {
        "family": "vector",
        "keywords": ["governed"],
        "query_embedding": [],
        "k": 20,
    },
    batch_size=20,
)
while not batch["cursor"]["exhausted"]:
    batch = await client.knowledge.pull(
        {
            "family": "vector",
            "keywords": ["governed"],
            "query_embedding": [],
            "k": 20,
        },
        batch_size=20,
        cursor=batch["cursor"],
    )
```

The `knowledge-batch` and `modality-serving` facade features are part of `full`, and
`full` is the default deployment. `scripts/check_p2_modality_architecture.py`
prevents removal of a modality identity tier, component TCK assertion, concrete
runtime, live verified-context handler, encrypted state boundary, resource ceiling,
served family adapter, wire/cursor/projection contract, post-ACL dispatch point, or
the default feature wiring.

## Verification commands

Run these serially on a resource-constrained build host:

```bash
python3 scripts/check_p2_modality_architecture.py
cargo test -p eg-modality --test served_runtime
cargo test -p eg-modality --test fleet_tck -- --nocapture
cargo test --features modality-serving server::handlers::modality::tests::resource_gate_is_bounded_and_every_leaf_is_12_of_12
cargo test --features raft,modality-serving sanitized_modality_command_tests
cargo test -p eg-plan --features knowledge-batch,epistemic result_stream
cargo test -p eg-document --features contract,serving
cargo test -p eg-image --features contract,serving,runtime
cargo test -p eg-audio --features contract,serving,runtime
cargo test -p eg-video --features contract,serving,runtime
```

Do not run more than one compilation command concurrently on a constrained host.
