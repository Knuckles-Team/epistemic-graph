# Trusted normal release runner

The normal release retains its eight required product gates, four hosted wheel
legs and same-run artifact consumers. The x86 wheel runs through an immutable
reusable workflow; publication credentials remain on hosted jobs. PR events never
enter the self-hosted job, including same-repository PRs and privileged PR events.

The reusable workflow admits only the canonical repository/owner IDs, a
non-deletion push of `v2.27.0`, the normal `release.yml` caller and the approved
operator `Knucklessg1` (ID 8661571), including the original sender and rerun actor.
It independently peels the canonical remote tag, checks out `github.sha`, and
checks HEAD before source scripts. Other tags/manual builds use hosted x86.
There are no source/command inputs or inherited publication secrets.

The runner group must independently select the exact reviewed reusable workflow
SHA. A job condition is not a substitute for this external policy. The current
group still selects the earlier fixed-source recovery workflow; changing it needs
separate approval. Do not broaden repository/workflow access or restore Default.

At release cut, validate one exact latest main commit, confirm that PyPI and any
GitHub release assets do not contain conflicting version bytes, coordinate a
short merge pause, and retarget the version tag with the observed old tag-object
lease. Check the peeled remote tag after the push. Use the authenticated approved
operator route that actually triggers workflows, not a repository GITHUB_TOKEN.
This is not an atomic lock across main, tag, CI and publication. Later main
advances do not invalidate an unchanged release tag.

The publication job preserves required Linux x86, Linux ARM64 and Windows wheel
completeness and engine/runtime checks. It uses a pinned shared pipelines helper
to freeze filenames/digests and run/source identity, reject conflicting remote
files, upload only missing exact files and check the complete published set.
Image publication waits for PyPI verification. Both publication paths recheck the
canonical tag against their checkout immediately before the write boundary.
No artifacts from the earlier release/recovery runs are imported.

Before the release cut, resolve the hosted ARM64 memory-pressure diagnostic and
validate its bounded build parallelism without relaxing features, LTO or codegen.
The source PR and focused tests alone do not prove a complete native release.
