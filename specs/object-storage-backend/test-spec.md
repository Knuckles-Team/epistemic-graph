# EG-OBJECT-STORAGE-BACKEND test-spec

## Positive

- `test_object_storage_backend_spec.py::test_backend_spec_roundtrip_and_refusals`. Both backend
  variants round-trip. A missing secret reference refuses. An unknown variant refuses. An
  unknown layout refuses. Each refusal is typed. *(EG-OBJECT-STORAGE-BACKEND-R001)*
- `test_object_storage_reads.py::test_versioned_reads_and_corruption_refusal`. Reads at three
  versions each equal that version's write set. A corrupted segment refuses with a typed error
  that names the version. Earlier versions stay readable. *(EG-OBJECT-STORAGE-BACKEND-R002)*
- `test_object_storage_commits.py::test_commit_before_ack_and_partial_publish`. A commit acks
  only after the manifest publish. An interrupted publish reopens the graph at the prior whole
  version. A concurrent CAS loss yields exactly one head.
  *(EG-OBJECT-STORAGE-BACKEND-R003)*
- `test_object_storage_shared_root.py::test_multi_node_cold_open_and_rejoin`. Two nodes open the
  same root. They serve identical state. A node that drops local state rejoins within the
  bounded re-fetch. *(EG-OBJECT-STORAGE-BACKEND-R004)*
- `test_comparative_cost_gate.py::test_regression_guard_and_claim_eligibility`. The stamped
  JSONL records all four dimensions for a clean run. An injected fixed-cost regression fails the
  release guard. *(EG-OBJECT-STORAGE-BACKEND-R005)*

## Negative

- An object-store spec without a declared durability class refuses at open.
- A manifest that names a missing segment refuses the read with a typed error. The graph's
  earlier versions stay readable.
- The engine never acks a client before the manifest publish is durable.
- A node without a placement grant for the tenant refuses before reads.
- A stale local cache behind a newer manifest discards. It never serves.
- A comparative run missing its environment stamp is claim-ineligible. A run missing its pinned
  revision is claim-ineligible. Neither enters the gate's evidence.

## Integration

- Full lifecycle on object storage: open, write, read at the prior version, second node reopens,
  commit from the second node, both nodes read the new version.
- Crash injection lands between segment write and manifest publish. The engine restarts. State
  equals the prior version exactly.
- Two nodes race the CAS on the same version publish. Exactly one publisher wins. The loser
  refuses as stale. It reconciles without a second head.

## Quality and release proof

- Clippy, test, jscpd, CCCC, dupehound, KISS gates pass on the new binding and harness.
- The no-duplicate-authority check confirms the object-store binding shares the store-authority
  chokepoint.
- Release proof follows EG-CONTRACT. Exact-head CI receipts record in this directory.

## Test bindings

Each row's acceptance test binds its ID where the test lands. The lines below record the planned
homes now. The spec-trailers gate resolves the spec PR's trailer. No delivery is claimed.

- `# spec: EG-OBJECT-STORAGE-BACKEND-R001` — `tests/test_object_storage_backend_spec.py`
- `# spec: EG-OBJECT-STORAGE-BACKEND-R002` — `tests/test_object_storage_reads.py`
- `# spec: EG-OBJECT-STORAGE-BACKEND-R003` — `tests/test_object_storage_commits.py`
- `# spec: EG-OBJECT-STORAGE-BACKEND-R004` — `tests/test_object_storage_shared_root.py`
- `# spec: EG-OBJECT-STORAGE-BACKEND-R005` — `tests/test_comparative_cost_gate.py`
