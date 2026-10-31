# [ID] — Requirements

Governing spec: [spec.md](spec.md).

Each row is one requirement. A child row `.n` is one five-minute unit of work. A row names the file it touches, its caller chain, its Shape sentence and the test that proves it as `path::name`. The generator sets each row's state in `status.json`.

| ID | Requirement | Acceptance |
|---|---|---|
| `[ID]-R001` | **[Short name.]** [The actor and the behavior it must show.] Caller chain: [entry point to the new code]. Shape: [files, types and the existing helper to reuse]. | [The test name and the observation that proves the row.] |
| `[ID]-R001.1` | **[Short name.]** [One unit of work. Name the file.] Caller chain: [entry point to the new code]. Shape: [files, types and the existing helper to reuse]. | [`path::name` of the test.] |
