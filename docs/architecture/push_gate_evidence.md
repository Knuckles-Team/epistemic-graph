# Push-gate execution evidence

`ci_gate_replica.py` is the single producer for workflow-derived heavy checks in
the pre-push gate. It parses the checked-in workflow files and builds one
bounded plan. Identical selections in that plan execute once per invocation;
later consumers may reuse only a verified result from that same invocation (or
an explicitly declared completed prior phase).

This is an execution optimization, not a coverage waiver. A missing,
partial, stale, failed, tampered, unsigned, or differently configured record
always falls through to the normal command.

## Evidence identity

Each selection is keyed by a canonical digest of:

- the exact command argument vector and command kind;
- normalized packages, features, targets, and other flags extracted from the
  vector;
- the effective Cargo environment, including target directory and bounded
  parallelism.

The invocation source identity includes the committed revision and tree,
working-tree and index binary diffs, bounded untracked-file content, the
`Cargo.lock` digest, and pinned toolchain/configuration plus `rustc`/`cargo`
version output. A successful result is additionally bound to its selection
payload and exit code. Evidence is stored in the worktree's private Git
directory, with restrictive permissions and a per-invocation HMAC key. It is
never a release artifact and contains environment digests rather than raw
environment values.

The pre-commit coordinator's PID and Linux process-start token identify the
invocation across the per-hook shell wrappers; bounded pre-commit stage/ref
fields further distinguish separate pushes under a long-lived parent. Records
older than 24 hours, records from another process, or records whose
source/configuration identity changes are not admissible.

## Admissibility predicate

A record can be consumed only when all of the following hold:

1. the evidence schema, invocation identity, source fingerprint, file modes,
   content digest, and HMAC verify;
2. the invocation status is `complete`;
3. the selection payload is present exactly in the plan;
4. the result is `success`, has exit code `0`, and has the expected result
   digest.

The sole non-exact relation is the versioned `cargo-clippy-full` proof. The
advisory `cargo clippy --workspace --all-features --all-targets -- -D
warnings` result may cover the shipped `full`/all-targets command only when
the requested command, provider payload, and effective environment match the
declared proof. No other selection is inferred to be a subset, and mandatory
coverage remains mandatory.

## Failure and restart behavior

The producer marks a resumed cache `running` before executing any new plan
item. It records failed results as failed and leaves them non-consumable. An
interrupted or otherwise non-finalized invocation remains partial and is
executed normally on the next attempt. A corrupt marker, key, or evidence file
creates no pass path: the cache is ignored and the command runs normally.
The evidence plan also has a hard selection-count bound; an unexpectedly
expanded workflow disables reuse rather than allocating without limit.

The constrained two-core gate is intentionally a different environment and
coverage selection. Its CPU-affinity/runtime proof is not silently substituted
for the workflow replica's ordinary Cargo selection.

## Root validation

The landing agent should inspect the complete source-level plan and run the
focused admissibility tests before the repository's normal pre-push/CI gates.
The focused selector is:

```text
pytest -q tests/test_push_gate_evidence.py
```

The source-only worker did not execute this command or any Cargo/build/hook
command.
