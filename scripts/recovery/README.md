# Fixed-source release recovery

The reusable `.github/workflows/eg-release-recovery.yml` builds only
`Knuckles-Team/epistemic-graph@29301642c205adc75d63cde2e69d7f53d85d707f`.
It tests exact-source Piper, builds two folded wheels, requires byte equality,
smokes the installed wheel, and uploads an artifact. It does not publish.

The reviewed dependency image must supply the pinned Python/Rust tools and
ONNX Runtime archive before this workflow can execute successfully. Image build,
offline smoke, and full recovery execution are separate validation steps.

## Approval and activation

1. Review the reusable workflow commit and record its full immutable SHA as `P`.
2. Through the separately approved organization settings change, restrict runner
   group `eg-release-recovery` to this repository and this selected workflow:
   `Knuckles-Team/epistemic-graph/.github/workflows/eg-release-recovery.yml@P`.
   Substitute the actual 40-character SHA; do not use a branch or tag.
3. After that review, render the inactive caller template with
   `python3 scripts/recovery/render_dispatch.py P /tmp/eg-release-recovery-dispatch.yml`.
   Review and separately publish the result at
   `.github/workflows/eg-release-recovery-dispatch.yml` on `main`.
4. A separately authorized manual dispatch from `main` may then call the pinned
   reusable workflow. The reusable job rejects other repositories, caller paths,
   branches, and event families, including both pull-request event families.

The active caller is rendered with immutable workflow commit
`4b232785086ae47791f1e58fe075c1ee74e6c877`. It is manual-only and becomes
available for the admitted main-branch dispatch after merge. Branch pushes and
PR events cannot invoke recovery. Normal PR checks may still run. No group
setting or runner registration is performed by this code change.

Run the focused contract checks with
`PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s tests -p test_recovery_workflow_contract.py -v`.
Full repository gates and native build validation remain required separately.

The caller intentionally retains approved commit `4b232785086ae47791f1e58fe075c1ee74e6c877`.
Subsequent cleanup of the reusable workflow at the branch tip does not change
the executed workflow. Selecting a later revision requires separate review and
an explicit runner-group selector update; do not repin the caller implicitly.
