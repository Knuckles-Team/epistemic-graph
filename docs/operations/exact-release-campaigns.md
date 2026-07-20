# Exact-artifact release campaigns

Four release gates execute the supplied engine artifact without discovering or
building a replacement. They require an executable path, the artifact's lowercase
SHA-256, and a new absolute evidence destination. A symlink, digest mismatch, missing artifact,
or pre-existing evidence file fails before certification begins.

The artifact must be the current `full` tier. These campaigns intentionally fail when
a full-tier listener, served modality, KnowledgeBatch family, or reasoning feature is
absent; they do not downgrade to a smaller feature set.

## Four served production modalities

```bash
python scripts/certify_exact_multimodal.py \
  --binary "${ENGINE_BINARY:?}" \
  --binary-sha256 "${ENGINE_BINARY_SHA256:?}" \
  --performance-evidence "${G37_JSON_EVIDENCE:?}" \
  --performance-evidence-sha256 "${G37_JSON_EVIDENCE_SHA256:?}" \
  --output "${MULTIMODAL_EVIDENCE:?}"
```

The G-14 campaign requires document, image, audio, and video from the exact `full`
artifact. Each leaf must report all twelve component TCK points as PASS with zero N/A
points, but the release result is derived from twelve independently executed behavior
dimensions rather than copying that component count. Two records per modality pass an
atomic stream, exact bundle/value round-trip, cursor paging, selective and negative
native queries, idempotent replay, observation-version rollback, cold/restore lifecycle,
and ordered event checks.

Every native codec rejects malformed input, the served request boundary rejects an
explicit over-limit source and malformed MessagePack bundle, and the engine remains
queryable afterward. Invalid request authentication, classification filtering, and a
second verified tenant must all fail closed while the owner records are still live.
The first tenant's records and rebuilt native indexes must survive hard process death,
one-row lazy-open recovery, online backup, restore into a two-shard target, and index
backfill. Four commit phases are fault-injected for every modality and must recover to
either no effect or the complete effect. Finally, governed delete must erase typed and
native postings, replay idempotently, survive restart, and physically collect its
tombstones. A scan of the complete isolated campaign root proves the synthetic source
byte sequences were never persisted.

G-14 also requires a digest-pinned passing G-37 JSON report for the same engine digest.
The report must contain positive ingest, native-query, and index-growth coverage for
each of document, image, audio, and video. G-14 retains only the report digest and
categorical binding result, so performance evidence cannot be substituted from another
artifact or reduced to a document-only smoke test.

## Protocol authorization and read isolation

```bash
python scripts/certify_exact_protocol_authorization.py \
  --binary "${ENGINE_BINARY:?}" \
  --binary-sha256 "${ENGINE_BINARY_SHA256:?}" \
  --output "${PROTOCOL_EVIDENCE:?}"
```

The harness enables the native RPC service plus PostgreSQL, MySQL, MSSQL, SQLite,
Bolt, Redis, AMQP, MQTT, and STOMP on isolated ephemeral loopback listeners. Each
protocol receives a native invalid-authentication exchange and must deny it. This is
an executable enabled-protocol inventory: a feature missing from the supplied binary
causes its listener check to fail.

A registered, read-authorized peer then runs negative row/owner checks through the
shared terminal data paths used by the protocol adapters: graph existence, properties,
union reads, semantic retrieval, topology, RDF, time series, vector plans, blobs,
jobs, SQL, warmed result cache, KV, and broker queues. The peer may read the graph but
must not observe the owner's private fixture. A restart under a second verified tenant
also proves graph-row, time-series, and KV namespace isolation. Thus the physical wire
authentication matrix and the terminal row/carrier matrix are both executable; the
static universal-read gate continues to prove that every adapter reaches those shared
terminal authorities.

## Seven-family KnowledgeBatch

```bash
python scripts/certify_exact_knowledge_batch.py \
  --binary "${ENGINE_BINARY:?}" \
  --binary-sha256 "${ENGINE_BINARY_SHA256:?}" \
  --output "${KNOWLEDGE_BATCH_EVIDENCE:?}"
```

The exact family inventory is graph, SQL, RDF, vector, time series, job, and
cross-modal. Every family must:

- emit the same native Arrow IPC schema;
- honor query pushdown and a two-row batch bound;
- produce a resumable cursor and reject cursor tampering;
- resume after the original physical client is closed;
- tolerate delayed pulls without unbounded buffering; and
- bind the cursor to its source snapshot.

Mutable sources reject a cursor after the source changes. A completed job result is
immutable, so its cursor remains valid after an unrelated graph write. These distinct
outcomes prevent a graph-version shortcut from being mistaken for source-correct
snapshot binding.

## Reasoning restart, retraction, and repair

```bash
python scripts/certify_exact_reasoning_repair.py \
  --binary "${ENGINE_BINARY:?}" \
  --binary-sha256 "${ENGINE_BINARY_SHA256:?}" \
  --output "${REASONING_EVIDENCE:?}"
```

The nine-case campaign covers projection lag and convergence, restart equality,
paraconsistent contradiction, belief retraction, valid/transaction-time change,
deterministic causal recomputation, calibrated causal assumptions, epistemic and
causal counterexamples, and fenced materialization repair. It requires a stale
projection to survive process replacement exactly, rejects an old recompute fence,
repairs at the durable watermark, and proves the repaired state is fresh before
acknowledgement. A support removal must flip a belief and survive a second restart.

## Pytest release entry point

The opt-in wrapper runs the four campaigns serially against one explicitly supplied
artifact:

```bash
EPISTEMIC_GRAPH_TEST_BINARY="${ENGINE_BINARY:?}" \
EPISTEMIC_GRAPH_TEST_BINARY_SHA256="${ENGINE_BINARY_SHA256:?}" \
EPISTEMIC_GRAPH_PERFORMANCE_EVIDENCE="${G37_JSON_EVIDENCE:?}" \
EPISTEMIC_GRAPH_PERFORMANCE_EVIDENCE_SHA256="${G37_JSON_EVIDENCE_SHA256:?}" \
python -m pytest tests/test_exact_release_campaigns.py -q
```

Supplying neither environment variable skips the opt-in wrapper. Supplying only one,
or supplying an invalid artifact, fails. The shared suite engine is not started for an
exact-artifact-only selection, so the campaign does not compile another binary or run
two engine instances concurrently.

## Evidence and cleanup

All fixtures are synthetic. The campaigns run serially, disable core dumps through the
shared exact-engine launcher, reap the complete process group, and delete stores,
sockets, logs, cursors, payloads, and ephemeral authority material. Retained JSON is
limited to the binary digest, categorical outcomes, counts, and canonical result
digests. It contains no executable or temporary path, listener address, hostname,
username, credential, raw row, payload, query result, or exception text.

Run the lightweight source/CI contract independently:

```bash
python scripts/check_exact_release_campaigns.py
```
