# GOC-40 — epistemic-graph full-suite baseline notes

Worktree opened 2026-08-17 for GOC-40 (drive epistemic-graph Rust suite to
truthful green). This marker commit exists solely to diverge this branch
from `main` immediately on creation, because the merge-queue/prune tooling
in this workspace treats a worktree whose branch tip is still
main-reachable as "already merged" and eligible for automatic deletion —
that happened once already to this exact branch name during baseline
measurement (worktree + branch vanished mid-run with no edits lost, but
~25 minutes of build time lost). Real findings will be appended/replaced
here as the lane progresses.
