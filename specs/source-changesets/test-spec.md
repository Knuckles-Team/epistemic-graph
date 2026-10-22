# EG-SCM test-spec

## Positive

- `test_scm_blob.py::test_blob_stored_once_and_read_by_digest`. Identical bytes store once. The
  digest readback equals the original bytes. *(EG-SCM-R001)*
- `test_scm_changeset.py::test_changeset_stores_only_changed_paths`. A two-path changeset stores
  exactly two path entries. The chain readback returns the record unchanged. *(EG-SCM-R002)*
- `test_scm_accept.py::test_disjoint_writers_land_and_overlap_rebases_or_conflicts`. Two disjoint
  writers both land. Two overlapping writers land one and conflict the other with
  `SOURCE_CONFLICT{paths}`. *(EG-SCM-R003)*
- `test_scm_tree.py::test_tree_at_revision_matches_full_replay`. A subtree read at each revision
  equals the full replay. The checkpoint path returns the same entries. *(EG-SCM-R004)*
- `test_scm_history.py::test_incremental_indexes_equal_rebuild`. Incremental index state equals a
  from-scratch rebuild byte-for-byte. *(EG-SCM-R005)*
- `test_scm_code_graph.py::test_one_file_change_updates_only_its_symbols`. A one-file changeset
  updates only that file symbols. A structural no-op updates nothing. *(EG-SCM-R006)*
- `test_scm_path_access.py::test_prefix_grant_filters_reads_and_refuses_writes`. A prefix grant
  filters reads to the prefix. A write outside the prefix is refused. *(EG-SCM-R007)*
- `test_scm_gate_verdict.py::test_verdict_records_are_sealed_and_read_back`. Pass and fail
  verdicts read back byte-identically. An overwrite attempt is refused. *(EG-SCM-R008)*
- `test_scm_bisect.py::test_bisect_finds_culprit_and_replays_decision`. A seeded bisect names the
  seeded culprit. The recorded decision replays byte-identically. *(EG-SCM-R009)*
- `test_scm_git_round_trip.py::test_export_import_preserves_commit_and_tree_ids`. Export then
  import preserves every commit id and tree id. *(EG-SCM-R010)*
- `source_changesets.rs::simulate_concurrent_accept_crash_and_replay`. Concurrent accepts with
  crash and replay hold every chain invariant. *(EG-SCM-R011)*
- `test_scm_provenance.py::test_changeset_links_to_run_trace`. An agent changeset reads back with
  its run trace id. *(EG-SCM-R012)*

## Negative

- A write that overwrites an existing blob digest is refused. The store keeps the original bytes.
- A changeset with an empty path list is refused. A parent not on the chain is refused. The chain
  stays unchanged.
- A changeset whose base left the branch chain is refused as stale.
- An unclean overlap refuses with `SOURCE_CONFLICT{paths}`. The conflict lists every conflicting
  path. No partial change lands.
- A tree read at a revision not on the repository chain is refused. A cross-tenant tree read is
  refused.
- An index read past the index head is refused.
- A read under a prefix with no grant is refused. A write without a write grant is refused.
- A verdict whose input digest mismatches the changeset tree is refused. A sealed verdict cannot
  be overwritten.
- A bisect without verdict coverage is refused. A revert that later changesets depend on is
  refused.
- An export with a failing mid-stream tree read is refused. A malformed import graph is refused.
- A fuzz seed that fails to seed is refused. It records no state.
- An agent changeset with no run trace id is refused. The refusal is typed.

## Integration

- Full agent loop: submit a changeset, read the tree at the new head, read the blob by digest,
  read the history and blame for the changed paths.
- Two concurrent writers on disjoint paths both land. The same two writers on one path land one
  and conflict the other.
- A crashed accept mid-write replays to the same head. The chain stays consistent.
- Export a chain, import it into a fresh repository, and compare the commit and tree ids.
- A path-scoped principal reads its prefix, is refused outside it, and is refused on a write.

## Quality and release proof

- Clippy, test, jscpd, CCCC, dupehound, KISS gates pass on the changeset and tree code.
- The no-duplicate-authority check confirms the changeset path reuses the existing commit and
  blob authorities. It adds no parallel authority.
- The simulation harness runs in the spec gate. It gates a regression in chain invariants.
- Release proof follows EG-CONTRACT. Exact-head CI receipts record in this directory.

## Test bindings

Each row's acceptance test binds its ID where the test lands. The lines below record the planned
homes now. The spec-trailers gate resolves the spec PR's trailer. No delivery is claimed.

- `# spec: EG-SCM-R001` — `tests/test_scm_blob.py`
- `# spec: EG-SCM-R002` — `tests/test_scm_changeset.py`
- `# spec: EG-SCM-R003` — `tests/test_scm_accept.py`
- `# spec: EG-SCM-R004` — `tests/test_scm_tree.py`
- `# spec: EG-SCM-R005` — `tests/test_scm_history.py`
- `# spec: EG-SCM-R006` — `tests/test_scm_code_graph.py`
- `# spec: EG-SCM-R007` — `tests/test_scm_path_access.py`
- `# spec: EG-SCM-R008` — `tests/test_scm_gate_verdict.py`
- `# spec: EG-SCM-R009` — `tests/test_scm_bisect.py`
- `# spec: EG-SCM-R010` — `tests/test_scm_git_round_trip.py`
- `# spec: EG-SCM-R011` — `tests/simulation/source_changesets.rs`
- `# spec: EG-SCM-R012` — `tests/test_scm_provenance.py`

## Graph-native edits and structured trailers

| Row | Proving test |
|---|---|
| `EG-SCM-R002.1` | `tests/test_scm_changeset.py::test_trailers_round_trip_through_git` |
| `EG-SCM-R013` | See each child. |
| `EG-SCM-R013.1` | `crates/eg-types/tests/source_symbol_edit.rs::edit_round_trips_and_kinds_are_closed` |
| `EG-SCM-R013.2` | `crates/eg-compute/tests/source_symbol_edit.rs::bytes_outside_spans_are_unchanged` |
| `EG-SCM-R013.3` | `crates/eg-compute/tests/source_symbol_edit.rs::reparse_confirms_edit_and_untouched_symbols` |
| `EG-SCM-R013.4` | `tests/test_scm_symbol_edit_served.py::test_symbol_edit_lands_and_reindexes` |
| `EG-SCM-R013.5` | `tests/test_scm_symbol_edit_served.py::test_symbol_edit_refusals` |
| `EG-SCM-R013.6` | `tests/test_scm_symbol_edit_served.py::test_symbol_edit_evidence_reads_back` |
| `EG-SCM-R013.7` | `cargo bench --bench source_symbol_edit` |
