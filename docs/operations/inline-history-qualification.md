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

The canonical spec-status generator labels a committed requirement `VERIFIED`
when it finds a bound source test. For this blocked draft, those generated
`verified_by` entries identify test definitions, not successful native execution.
They do not close any acceptance gate below. Test tasks remain unchecked until
the reviewers record the exact native and installed results. Do not hand-edit generated
state to substitute for that evidence.

The editing environment initially had no discovered Rust toolchain. Hosted run
`40451526869` ran the storage suite: 132 tests passed and four failed on stale pins,
census counts and generated documentation. The native lineage test calculated
the successor fingerprint `e4aeac7d8f73cad643f5b843b542307ee3f1a4208a984bf9aba7cffd2a9baf35`.
The source pin and census expectations now use that result; a subsequent green
run is still required. The inline-history upgrade preservation test passed.
Facade tests did not run because compilation failed; scoped compiler repairs
await hosted verification. The owner-format document now contains the complete
native-rendered expected output from that storage test, independently checked
against the recorded log. It still needs a passing native freshness rerun.
Generated predecessor fixtures remain outstanding.

Before making the change ready:

1. Capture `dump_graph_shard_manifest` from exact base `d92c682c` in an isolated
   worktree using the procedure in `crates/medulla-storage/fixtures/lineage/README.md`.
   Preserve that real base-build output as the new predecessor fixture.
2. Run the native owner-lineage tests to calculate and independently review the
   successor table fingerprint. Update the source pin only to the actual result.
3. Run `cargo run -p medulla-storage --example gen_owner_store_formats`; never hand-edit
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
`GetRunWorkItem.selection`, the explicit `completed_text_history` wrapper flag,
private owner-only `completed_checkpoint`, and
`NativeClaimWorkItemResult.fresh_execution_grant`. Regenerate the Arbora operation
registry from that actual artifact before enabling the profile. An old generated
client or registry must refuse the enabled path, not infer a successful empty
history. Disabled legacy operation remains compatible.

Run `40451526869` produced canonical contract artifact `12714498727` and codec
artifact `12714466315` from source tree `d9ea4b8258614cd9e49ca7e0c9a3bd9ade586d23`.
Their exact source/recipe/file digests were checked before importing the generated
bytes. Local method reachability now passes for all 473 methods. This is not a
successful hosted parity rerun. The cached pinned toolchain is now available.
Disk capacity limits local native builds; exhaustive native checks still need hosted results.
The native-generated owner-format document retains one STE sentence-length finding.
A renderer correction and genuine regeneration remain pending.
A blocked draft exception covers neither readiness nor passing evidence for
those checks.

## Operator enablement

Use a dedicated retained graph. Its first profiled admission requires authenticated
`kg:admin` or `*` operator authority and atomically records the private policy
marker. Ordinary submission grants and anonymous mode cannot opt in a shared
graph. Later owner-bound admissions may use ordinary submission authority.
Enabling the consumer configuration does not grant this native authority.

Public carriers reject nested WorkItem writes even if their serialized envelope
contains a forged internal capability. Trusted in-process Rust persistence calls
remain an engine authority boundary, not protection from hostile engine code.

## Compatibility revision qualification

The native methods now require explicit `completed_text_history` negotiation
for profile reads and claims. Legacy profile reads refuse after owner validation.
Legacy profile claims refuse before lease mutation. Old claim receipts cannot
bypass negotiation or grant execution on replay.

Sixteen focused Python checks pass against the genuine generated opt-in schemas.
These include the two positive wrapper tests that previously rejected the flag.
Run `40550257423` produced engine artifact `12753804526` and codec artifact
`12753574522` from merge tree `903639bea68bf308613e7ce0d1de17532db4aa4f`.
All manifest file hashes and recipe inputs were checked before import.
The run exposed a feature-gated helper and two Clippy findings, now corrected.
Storage tests stopped at compilation. Some remaining jobs received runner shutdown signals.
Their cancellation does not establish a native test result.
The new native refusal and replay tests have not run in this workspace.
The renderer source fixes the recorded STE sentence. The generated document
still preserves the prior native output until genuine regeneration succeeds.

## C2 integration generation stage (blocked)

This draft combines native history with main commit `685757589075720be3aaafdc37b4ba84d5f77626`.
Its canonical contracts, generated clients and codec binaries temporarily retain
that main commit's genuine output. They are stale for the combined source.
Selecting that snapshot is not generation or qualification. Native parity must
fail until the existing hosted exporters produce reviewed replacement artifacts.

The combined method census expects 493 variants; the native test must prove it.
The integration preserves explicit history negotiation, owner binding, replay
refusal and destructive-operation holds. The read-only consumer caller remains
fail-closed and uses same-run producer artifacts. Existing consumer compatibility
exclusions remain enforced; new contract changes require a fresh exact review.

Do not merge or release this generation-stage draft. Import genuine native
contracts and codec outputs with exact source and recipe provenance first.
Then require native parity, coherent installed dependencies and both actual
process restart journeys. An unchanged public transcript dialect identifier matches the configured author
name, triggering the documented local privacy finding.
Hosted privacy checks remain required and are not suppressed.

### Genuine C2 output imported (qualification pending)

Hosted run `40816721225` generated replacement contracts and codecs from clean
source `1c4f3d1d32aeae86a5cb660115737594980817d6`, tree
`d6e771f3d24ec06dbb127e6f3a42460358eb3f9e`.
Contract artifact `12872827980` contains 74 generated files and records 493 methods.
Codec artifact `12872997263` records SHA-256
`09b34532de0654dc3fc9fb0da3433df7ce22042c96a15876b7ea8bc2b402427b`.
Owner-format artifact `12872099991` provides the native-rendered layout document.
All archive, payload and recipe hashes match their provenance and exact source.
These genuine outputs replace the temporary snapshot described above.

Fresh parity and native tests must still pass on the imported revision.
The generated method count is not a passing census-test receipt.
Installed consumer qualification and the compatibility review remain outstanding.

### Balanced-union generation stage

The genuine 493-method output exposed recursive scanner depth in its Python alias.
The generator now balances large union expressions without changing member order,
discriminators, schemas or validation. Small unions retain their existing rendering.
The imported outputs above still precede this renderer change and need regeneration.
The old generated alias still fails the new depth regression and local Vulture.
This blocked draft must receive fresh hosted output before parity can pass.
No scanner settings, compatibility refusals or release gates are relaxed.
