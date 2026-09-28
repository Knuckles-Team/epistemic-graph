# EG-CONTRACT-001 — Delivery tasks and evidence

**State:** PARTIAL source baseline; ACCEPTED evidence pending. A checked box means only its named task is proven, with an exact commit and test result below.

| Task | Obligation IDs | Work and exit condition | State | Evidence |
|---|---|---|---|---|
| C-01 | EH-192, EH-377 | Audit and converge all Python method DTOs on generated strict models; run EG-T-04. | TODO | PENDING |
| C-02 | EH-592, EH-593 | Complete catalog error sets, conflict mapping, scope projection and wheel contents; run EG-T-01/02/03. | TODO | PENDING |
| C-03 | EH-372, EH-383 | Rebuild/measure both WASM copies and replay cross-language vectors; run EG-T-05/06. | TODO | PENDING |
| C-04 | EH-430, EH-433, EH-643, EH-644, EH-646, EH-647 | Make reachability and current-tree static gates follow real code paths; run mutation fixtures EG-T-09. | TODO | PENDING |
| C-05 | EH-468, EH-645, EH-650 | Run root Python suite, lifecycle cases and supported feature matrix in hosted CI; run EG-T-07/10. | TODO | PENDING |
| C-06 | EH-366, EH-469, EH-519, EH-520, EH-561, EH-655, EH-656 | Prove wheel/privacy and exact-tree quality gates without new clones or live-host dependencies; run EG-T-08. | TODO | PENDING |
| C-07 | All | Build twice, install published candidate, run every consumer on one commit/digest and publish receipt; set ACCEPTED only after exact release result. | TODO | PENDING |

## Evidence record to fill for each task

Record a public commit SHA and PR URL, commands and tool versions, pass/fail outputs or hosted run links, wheel/codec/catalog hashes, target platform, and remaining failed or skipped checks. A source commit can move a task to LANDED; it cannot close C-07. Avoid checking a task solely because a file or an earlier branch exists.

## Reconciliation rule

At every merge, compare the source SHA in this table to public default branch. If an implementation is present and the focused test passes, label that requirement LANDED. If the installed artifact, consumers, hosted quality and release matrix pass on the same SHA, label it ACCEPTED. Otherwise leave the specific failed or unrun gate open.
