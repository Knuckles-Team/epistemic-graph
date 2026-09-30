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

This change intentionally includes only a `.yml.in` caller template outside the
workflow directory. Publishing this branch or opening its draft PR does not
activate the recovery caller or the existing release build/publish jobs. Normal
PR checks may still run. No group setting or runner registration is performed.

Run the focused contract checks with
`PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s tests -p test_recovery_workflow_contract.py -v`.
Full repository gates and native build validation remain required separately.
