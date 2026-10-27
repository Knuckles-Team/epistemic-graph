# Inline history layout qualification (pending)

The bounded completed-text continuation profile adds one graph-owner table,
`work_item_inline_history`, to the existing native graph transaction. It is scoped
to the graph owner and separate from query-visible `nodes`. Snapshot bytes
and retained-capacity accounting must not enter generic graph projections.

The previous 60-table layout is registered as an explicit offline-upgrade source.
Its fingerprint is the existing mainline value
`cfc47e43654abc32fbf63cf3d9d7ffd4e04c742e3c955c259d2d05f5316e0a27`, recorded by
`pinned_layout_digest` and generated `owner-store-formats.md` at
`d92c682c9edd59aa3adcc8a25816d5944d2ec4d5`. Normal open must refuse it without
changing bytes. The existing inspected offline upgrade creates the private table.
It preserves graph rows, prior private current-plan rows and all earlier upgrade paths. Registering this path does not authorize running a production migration.

## Outstanding native gates

The editing environment has no Rust toolchain. No native test, fingerprint
calculation, generated manifest fixture or schema regeneration is claimed.
The successor fingerprint in `pinned_layout_digest(GraphShard)` deliberately
remains unchanged until the native generator/test workflow calculates it. That
is a known failing lineage gate, not permission to waive or remove the assertion.

Before making the change ready:

1. Capture `dump_graph_shard_manifest` from exact base `d92c682c` in an isolated
   worktree using the procedure in `crates/eg-storage/fixtures/lineage/README.md`.
   Preserve that real base-build output as the new predecessor fixture.
2. Run the native owner-lineage tests to calculate and independently review the
   successor table fingerprint. Update the source pin only to the actual result.
3. Run `cargo run -p eg-storage --example gen_owner_store_formats`; never hand-edit
   the generated owner-format document.
4. Run storage census, lineage, old-layout refusal and data-preserving upgrade
   tests, including `inline_history_upgrade_preserves_graph_and_private_plan_rows`.
5. Run native destructive-operation refusal and owner-isolation tests. Pending,
   failed and completed profile graphs must refuse reset, retirement and restore.
   Legacy retirement must still work. Run completion/restart/replay acceptance
   on the same retained graph in the caller.
6. Run the repository's normal generation, formatting and pre-commit gates.

A blocked draft is not an installed-stack or migration-readiness claim.
Broader blob-backed memory sync, interrupted-run restore or release/GC is not delivered by
this table registration.

## Explicit lifecycle hold

The graph scope identity is fixed and retirement removes its version authority.
They do not provide a non-reusable admission generation. A private constant-size
marker is set at profiled admission in the same transaction. All `ClearGraph`/`DeleteGraph`,
retirement and snapshot replacement paths must refuse while that marker or any
private history exists. No graph deletion/recreation or quota reclamation support
is claimed. Those operations need a reviewed catalog-incarnation binding first.

## Generated consumer handoff

Native generation must include `GetConversationHead`, optional
`GetRunWorkItem.selection`, private owner-only `completed_checkpoint`, and
`NativeClaimWorkItemResult.fresh_execution_grant`. Regenerate the Arbora operation
registry from that actual artifact before enabling the profile. An old generated
client or registry must refuse the enabled path, not infer a successful empty
history. Disabled legacy operation remains compatible.

The automatic local reachability check currently detects the absent generated
method catalog. Pinned rustfmt is also unavailable. The exhaustive native contract
check is a separate manual local gate and hosted requirement. A blocked draft
exception covers neither readiness nor passing evidence for these checks.

## Operator enablement

Use a dedicated retained graph. Its first profiled admission requires authenticated
`kg:admin` or `*` operator authority and atomically records the private policy
marker. Ordinary submission grants and anonymous mode cannot opt in a shared
graph. Later owner-bound admissions may use ordinary submission authority.
Enabling the consumer configuration does not grant this native authority.

Public carriers reject nested WorkItem writes even if their serialized envelope
contains a forged internal capability. Trusted in-process Rust persistence calls
remain an engine authority boundary, not protection from hostile engine code.
