"""BUG A2 (2026-08-12): blob cursor breaks the NEXT independent connection.

Reproduced standalone (a ~40-line script): once any client completes a blob
upload (``begin -> chunk_put -> commit``) and disconnects, the next
INDEPENDENT connection's very first ``BlobBegin`` failed server-side with
"committed blob upload cursor is missing" -- even though nothing was wrong
with the new connection's own request.

Root cause (see ``compile_blob_batch_at``'s doc in
``src/server/handlers/blob.rs``): ``BlobBegin`` compiled its durable
idempotency/mutation-batch key from ``(tenant/actor scope, wire request id,
method bytes)``. The wire request id is only LOCALLY unique within one
connection -- ``EpistemicGraphClient._next_id()`` restarts its counter from 1
on every fresh connection -- and ``CarrierAuthority`` carries no
per-connection component at all. So two INDEPENDENT connections from the
same tenant/actor calling ``BlobBegin()`` with the same default
``chunk_size`` as their first RPC produced the EXACT SAME idempotency key.
The engine's durable idempotency ledger never expires (by design, so an
acknowledgement-lost retry within one logical attempt still replays
correctly), so the second connection's ``BlobBegin`` silently REPLAYED the
first connection's already-committed cursor id -- whose durable row had
already been torn down by ITS ``BlobCommit`` (correct, per-upload teardown).
The handler then looked up that stale, already-removed id and failed.

The fix folds the freshly ALLOCATED cursor id (globally unique, monotonic,
never reused) into ``BlobBegin``'s idempotency key instead of the wire
request id, so two independent connections' first ``BlobBegin`` calls can
never collide.

This shape -- two SEQUENTIAL, INDEPENDENT connections with a real disconnect
between them -- was covered by no existing test (there was no blob test in
this suite at all), which is exactly why the bug shipped.
"""

import pytest
from conftest import request_context


@pytest.mark.concept("CONCEPT:EG-KG.storage.blob-namespace")
def test_blob_begin_on_a_fresh_connection_after_a_prior_upload_committed(
    start_epistemic_graph_server,
):
    """A second, INDEPENDENT connection's first BlobBegin must succeed after a
    prior, unrelated connection completed an upload and disconnected."""
    import os

    from epistemic_graph.client import SyncEpistemicGraphClient

    socket_path = os.environ["GRAPH_SERVICE_SOCKET"]

    # Connection #1: a complete upload (begin -> chunk_put -> commit), then a
    # real disconnect (`.close()` tears down the socket, not just drops the
    # Python object) -- exactly the repro's "any client completes an upload
    # and disconnects" precondition.
    conn1 = SyncEpistemicGraphClient.connect(
        socket_path=socket_path,
        verified_context=request_context(),
    )
    try:
        digest1 = conn1.blob.store(b"connection-one payload")
        assert digest1
    finally:
        conn1.close()

    # Connection #2: a genuinely NEW, independent connection (its own wire
    # request-id counter restarts at 1, exactly like connection #1's did).
    # Its first RPC ever is BlobBegin() with the SAME default chunk_size --
    # the precise shape that collided before the fix.
    conn2 = SyncEpistemicGraphClient.connect(
        socket_path=socket_path,
        verified_context=request_context(),
    )
    try:
        cursor = conn2.blob.begin()
        assert isinstance(cursor, int)
        # The cursor must be USABLE -- not just a number handed back from a
        # stale replay of connection #1's already-torn-down upload. Push a
        # chunk and commit to prove the whole lifecycle works end to end on
        # this brand-new connection.
        conn2.blob.chunk_put(cursor, b"connection-two payload")
        digest2 = conn2.blob.commit(cursor)
        assert digest2
    finally:
        conn2.close()


@pytest.mark.concept("CONCEPT:EG-KG.storage.blob-namespace")
def test_three_sequential_independent_connections_each_upload_successfully(
    start_epistemic_graph_server,
):
    """The fix must hold for MORE than one repetition, not just cursor #1 vs
    #2 -- every sequential connection's first BlobBegin succeeds."""
    import os

    from epistemic_graph.client import SyncEpistemicGraphClient

    socket_path = os.environ["GRAPH_SERVICE_SOCKET"]
    digests = []
    for i in range(3):
        conn = SyncEpistemicGraphClient.connect(
            socket_path=socket_path,
            verified_context=request_context(),
        )
        try:
            digests.append(conn.blob.store(f"payload-{i}".encode()))
        finally:
            conn.close()

    assert len(digests) == 3
    assert all(digests)
    # Distinct payloads must yield distinct content-addressed digests.
    assert len(set(digests)) == 3
