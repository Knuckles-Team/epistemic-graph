# EG-BRANCH-MERGE test-spec

## Positive

- `test_branch_create.py::test_branch_create_and_refusals`. Branch creation round-trips the
  identity. Each refusal (empty name, duplicate, reserved namespace, foreign tenant, unresolvable
  base) returns a typed error. Each leaves no registry entry. *(EG-BRANCH-MERGE-R001)*
- `test_branch_scoped_mutation.py::test_branch_write_invisible_until_merge`. A write on a branch
  shows on that branch. It stays invisible on main and on sibling branches. It is idempotent
  under a tenant-scoped branch idempotency key. *(EG-BRANCH-MERGE-R002)*
- `test_branch_merge_base.py::test_merge_outcomes_up_to_date_and_fast_forward`. Four fixture
  topologies (contained, descendant, diverged, disjoint) return the declared outcomes from the
  closed set. Fast-forward produces a single-parent receipt. Up-to-date publishes nothing.
  *(EG-BRANCH-MERGE-R003)*
- `test_branch_three_way_merge.py::test_entity_level_outcomes`. Disjoint, one-sided, and
  both-sides fixtures each produce the expected published target state. Unchanged entities adopt
  without row copy. *(EG-BRANCH-MERGE-R004)*
- `test_branch_epistemic_conflict.py::test_evidence_weighted_resolution_and_open_conflict`. Under
  a pinned epistemic policy, the winner selects per the policy. The resolved row keeps the
  contradiction link. It keeps both evidence refs. Without a policy, the conflict stays open with
  both candidates queryable. *(EG-BRANCH-MERGE-R005)*
- `test_branch_time_travel.py::test_as_of_reads_and_diff`. As-of reads at three versions each
  equal that version's write set. The diff between versions equals the intervening commits.
  *(EG-BRANCH-MERGE-R006)*
- `test_branch_merge_publish.py::test_atomic_two_parent_publication_and_replay`. The two-parent
  receipt reads back byte-identically. Replay against the same inputs returns the original
  receipt. *(EG-BRANCH-MERGE-R007)*

## Negative

- Foreign-tenant branch creation and reads refuse before any state change.
- A mutation naming an unknown branch refuses. A deleted branch refuses. A foreign-tenant branch
  refuses. The graph stays unchanged.
- A merge with no common ancestor refuses with a typed error. It never invents an implicit base.
- An adopted entity that fails constraint or ABox validation refuses the whole merge. The refusal
  is atomic. No partial merge publishes.
- A missing epistemic policy refuses resolution. Stale evidence refs refuse. Nonfinite confidence
  refuses. The engine leaves the entity unmerged. It never guesses.
- An as-of read at a version not on the branch chain refuses. A cross-tenant diff refuses. Both
  return typed errors.
- A target that advanced past its recorded head refuses as stale. The merge never rebases
  silently. It never claims a rollback.

## Integration

- Full agent loop: create branch, write, diff, merge (fast-forward and three-way), read the merged
  state on main with provenance intact.
- Two concurrent branches write disjoint entities. The merge is clean. The same two branches
  write the same entity. The pinned policy produces a typed conflict.
- Merge publication under the cluster feature replicates through raft. A follower catches up. It
  serves the merged state.

## Quality and release proof

- Clippy, test, jscpd, CCCC, dupehound, KISS gates pass on the merge pass.
- The no-duplicate-authority check confirms the merge reuses the existing commit and validation
  authorities. It adds no parallel authority.
- Release proof follows EG-CONTRACT. Exact-head CI receipts record in this directory.

## Test bindings

Each row's acceptance test binds its ID where the test lands. The lines below record the planned
homes now. The spec-trailers gate resolves the spec PR's trailer. No delivery is claimed.

- `# spec: EG-BRANCH-MERGE-R001` — `tests/test_branch_create.py`
- `# spec: EG-BRANCH-MERGE-R002` — `tests/test_branch_scoped_mutation.py`
- `# spec: EG-BRANCH-MERGE-R003` — `tests/test_branch_merge_base.py`
- `# spec: EG-BRANCH-MERGE-R004` — `tests/test_branch_three_way_merge.py`
- `# spec: EG-BRANCH-MERGE-R005` — `tests/test_branch_epistemic_conflict.py`
- `# spec: EG-BRANCH-MERGE-R006` — `tests/test_branch_time_travel.py`
- `# spec: EG-BRANCH-MERGE-R007` — `tests/test_branch_merge_publish.py`
