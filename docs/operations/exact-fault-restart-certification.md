# Exact-binary fault and restart certification

Release certification runs the shipped engine artifact itself. The harness does
not search for an engine, invoke Cargo, use a source-tree build, or downgrade to a
smaller feature set. Both the executable and its lowercase SHA-256 are mandatory.

```bash
python scripts/certify_exact_fault_restart.py \
  --binary "${ENGINE_BINARY:?}" \
  --binary-sha256 "${ENGINE_BINARY_SHA256:?}" \
  --output "${CERTIFICATION_EVIDENCE:?}"
```

An unavailable, non-executable, symlinked, or digest-mismatched artifact fails
before a server starts. The evidence destination must be new, preventing a failed
rerun from leaving an older successful artifact that could be mistaken for the
current result. The pytest wrapper is opt-in through
`EPISTEMIC_GRAPH_TEST_BINARY` and
`EPISTEMIC_GRAPH_TEST_BINARY_SHA256`; supplying only one, or supplying an
invalid artifact, is a test failure rather than a skip.

## What is certified

The process-fault matrix covers four deterministic commit boundaries:

1. before authoritative rows;
2. after rows but before terminal metadata;
3. immediately before the durable commit;
4. after the durable commit but before acknowledgement.

Representatives cover every current `MutationDomain`: graph rows and snapshots,
RDF, SQL catalog, blob, KV, time series, analytics jobs, broker, cross-modal,
multi-graph, lifecycle, and control-plane. Cypher and GraphQL mutations are
separate cases even though they share the graph-snapshot durability domain. The
matrix therefore contains 15 mutation-family representatives × 4 phases = 60
isolated process crashes and restarts.

Single-store and graph mutations must reopen with no effect for the first three
boundaries and one complete effect after acknowledgement loss. A multi-store
coordinator is replayed with the exact request after restart: a prepared parent
resumes idempotent children, while a committed parent returns its stored result.
The terminal observation must contain every child effect and never a split
result.

The same run also proves two restart-index contracts:

- identical local time-series ids written under two verified tenants reopen to
  two isolated result sets; and
- a forced one-row lazy open exposes typed `PARTIAL_MATERIALIZATION`, withholds
  the spatial index while incomplete, and eventually returns the same complete
  spatial result across two restart/lazy-open cycles.

## Safety and retained evidence

Every case uses a separate temporary durable store and runs serially. Core dumps
are disabled before each engine spawn, every process group is reaped, and all
runtime stores, sockets, and logs are removed. The process-fault control requires
an exact request id, canonical mutation domain, commit phase, and random 256-bit
nonce. Invalid controls fail before row mutation.

The JSON evidence is deterministic and aggregate-only. It contains the artifact
digest, domain/phase/family outcomes, result digests, counts, and pass state. It
does not contain executable paths, temporary paths, sockets, logs, credentials,
raw exception text, hostnames, usernames, tenant data, or mutation payloads.

Run the static architecture gate independently of the expensive live matrix:

```bash
python scripts/check_exact_fault_restart_harness.py
```
