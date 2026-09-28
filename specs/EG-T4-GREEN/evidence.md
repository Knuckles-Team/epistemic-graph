# EG-T4-GREEN — Evidence snapshot

Captured 2026-09-28 from [PR #3](https://github.com/Knuckles-Team/epistemic-graph/pull/3); this is a point-in-time review aid, not a live status feed. PR head: `78b54cc0ad4ac5e2fd5b65ae0f2ffe54157d18b7`. State: OPEN, merge state UNSTABLE.

The PR description reports `cargo test -p epistemic-graph --features full --lib` at 2085 passed, one failed (`sqlite_file_roundtrip`, missing local `sqlite3` CLI), eight focused integration suites passing, dispatch decomposition passing, and `cargo fmt --check` clean. It says local eg-storage, Raft/slim and Python client suites were not completed. These are author-reported, not independently re-run here.

Hosted [Release run](https://github.com/Knuckles-Team/epistemic-graph/actions/runs/36441906233) at capture time had Go and JavaScript clients **FAILED** and Scanner + architecture quality **FAILED**; several gates, Python suite and Clippy remained running. Security, runtime contracts and selected Clippy jobs had passed. Check the live rollup before any merge decision.

No EH-592 wheel/consumer proof, EH-655 jscpd acceptance, EH-656 hook acceptance, or landing of this spec is claimed by this snapshot. Record the final check URLs, exact merged head, and acceptance disposition here after qualification.
