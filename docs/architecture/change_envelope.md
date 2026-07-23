# Governed ChangeEnvelope protocol

`ChangeEnvelope` is the engine-native commit unit for an authoritative external change. It embeds a
`MutationBatch` and adds the object material and governance facts that must share its commit point:

- graph row operations;
- blob references and derived features;
- located evidence, policy, and lineage;
- a content version and an optional source cursor; and
- immutable status plus projection/CDC outbox rows.

The engine does not decompose this into a sequence of graph calls. `ApplyChangeEnvelope` validates the
whole envelope and writes every row in one redb transaction with one commit-before-ack barrier. A
failure writes none of them. The in-memory projection is also staged and published with one snapshot
swap, so a late CAS or edge-validation failure cannot expose a partial cache. A retry with the same
tenant/idempotency key returns the prior receipt; a conflicting retry is rejected.

## Typed ordering

Content versions and source cursors are tagged values:

- `sequence` for monotonic source counters;
- `timestamp_millis` for numeric source time; or
- `opaque` with a provider-defined type and value.

Only values of the same ordered type are compared. Opaque values can assert an exact predecessor but
are never sorted lexically; a token such as `page-10` is not incorrectly treated as older than
`page-9`.

## Verified authority and privacy

`eg2.` service requests bind the envelope to verified request authority: tenant, pseudonymous
principal, request id, policy version, and idempotency key. Durable principals use an opaque SHA-256
identifier. The envelope validator rejects inline machine paths, file URIs, email-shaped identifiers,
unsupported digests, ungoverned material, or policy rows for another tenant.

The Python client can carry a constructor-level verified context or a task-local override:

```python
with client.use_verified_context(verified_context):
    receipt = await client.changes.apply(envelope)
```

The override is context-local, so concurrent requests can share a client without leaking one
request's authority into another.

## Cluster and lifecycle guarantees

In a cluster, the complete envelope is one Raft log entry. The leader chooses the commit timestamp;
followers apply the same native transaction and return the same commit receipt. A projection failure
does not roll back authoritative durability: the receipt reports `projection_pending`, and the
transactional outbox supplies repair work.

Raft snapshots include both the graph image and its MutationBatch/ChangeEnvelope authority. Online
resharding, offline shard-count migration, and backup/restore also copy the replay index, batch/outbox
delivery state, versions, fences, envelopes, material, governance, and cursors. These paths therefore
cannot restore a graph projection while silently losing its reconciliation state.

## Reconciliation reads

`GetChangeEnvelope`, `GetContentVersion`, and `GetChangeCursor` are verified tenant-scoped reads. They
support connector restart, delta synchronization, and projection repair without trusting a
connector-local checkpoint as the source of truth.

For a bounded materialized-state audit, `NodeClient.list_by_label` also accepts
an exclusive node-id cursor:

```python
after = None
while True:
    rows = await client.nodes.list_by_label("SourceRecord", 1_000, after=after)
    if not rows:
        break
    verify_page(rows)
    after = rows[-1][0]
```

Rows are ordered by opaque node id. The scan observes live committed state, not
a cross-request snapshot, so one reconciliation pass must be serialized with
writers that could insert an id behind its cursor. Source run manifests should
retain only keyed endpoint/query fingerprints, counts, content roots, cursor
positions, and governance/privacy attestations; raw locations and source
identifiers do not belong in the durable manifest.
