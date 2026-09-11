# Dupehound full-tree census

The release scanner job runs the changed-source gate in
`scripts/check_dupehound.py` and the separate full-tree census in
`scripts/check_dupehound_census.py`. The census is advisory telemetry for
function clones; it does not create a baseline and it does not replace the
changed-source gate.

The adapter binds one immutable `HEAD` commit and tree to a complete Git tree
listing. It materializes that archive, classifies every tracked path, and
passes only the explicit profile exclusions to Dupehound. The tool is pinned
by `pyproject.toml` (`0.1.2`, threshold `0.85`, minimum tokens `40`), uses
`--all`, `--exclude-tests`, `--no-default-excludes`, and a bounded
`RAYON_NUM_THREADS=8` environment. The raw JSON and stderr are retained as
separate artifacts; the normalized artifact is a `scanner-evidence/v2`
receipt containing the manifest, source-path digest, native skip-count joins,
and stable finding IDs.

The source universe follows Dupehound 0.1.2's native walker exactly: hidden
paths are omitted by its `WalkBuilder.hidden(true)` policy, C++ includes
`.c++`, and generated suffixes are limited to the pinned native patterns
(`*.generated.*`, `*.gen.go`, `*.gen.ts`, `*.d.ts`, protobuf outputs, and the
minified/bundle patterns). A name such as `foo-generated.py` remains source;
the adapter does not invent a broader generated suffix rule. Oversized files
are skipped before native content loading, while the documented generated
markers and minified/non-UTF-8 counters are joined to the native report.

`--exclude-tests` follows Dupehound 0.1.2's native `TestPolicy::Skip`: files
classified as tests by path are omitted from the scan, while inline test
functions in an otherwise production file remain valid native members. The
normalized report retains each member's `test` flag and the cluster's
`test_only` flag. Its explicit production projection emits a product finding
only when a cluster has at least two non-test members; mixed and test-only
clusters remain in native telemetry with their test-member counts. Native
copies, deletable lines, score, and function counts remain labeled native
telemetry, while projected copies and deletable lines are reported separately.

If the immutable manifest has no production files, the receipt records
`scope=not_applicable`, `exit_class=not_applicable`, and
`native_scan_executed=false`; Dupehound is not invoked. If Dupehound exits 0
with malformed JSON or a report that fails the manifest join, the receipt is
`cannot_run` and the raw JSON plus stderr are still written to their requested
artifact paths.

Run the adapter from a clean EG checkout with an already-installed binary:

```sh
python3 scripts/check_dupehound_census.py \
  --repository agent-packages/epistemic-graph \
  --commit HEAD \
  --profile dupehound-full-tree-v1:epistemic-graph \
  --raw-output /tmp/eg-dupehound.raw.json \
  --stderr-output /tmp/eg-dupehound.stderr.txt \
  --normalized-output /tmp/eg-dupehound.scanner-evidence.json \
  --manifest-output /tmp/eg-dupehound.manifest.json
```

Exit `0` means that the immutable manifest and native report joined
successfully. A non-empty cluster list is still visible as advisory findings
in the receipt. An empty production universe is explicitly reported as
`not_applicable`. Exit `2` means the census could not be proven complete,
including tool/config drift, archive or path mismatches, malformed native
JSON, skip-count mismatches, and timeout. The adapter does not run package
installation, network access, a Cargo build, or a second scanner root.
