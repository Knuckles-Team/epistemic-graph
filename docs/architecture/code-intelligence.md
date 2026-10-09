# Code Intelligence — type/scope-resolved call graph (CONCEPT:EG-KG.compute.type-scope-resolved-call)

The Knowledge Graph models code as `:Code` symbols linked by `:calls`, `:inherits`,
`:realizes`, `:dependsOn`, `:covers`. The accuracy of those links is set by **how
calls are resolved**. Before KG-2.100 resolution was **name-only**: a method call
`obj.run()` bound to *any* symbol named `run`, and a callee name shared by more than
ten symbols was dropped entirely. KG-2.100 makes resolution **type- and
scope-aware**, computed in the Rust engine and shipped already-resolved.

## What changed

- **Resolution is in Rust, in one round-trip.** The `epistemic-graph` engine's
  `IndexRepository` op parses every file (rayon) **and** resolves cross-file calls
  over the whole batch, returning one merged graph. The Python pipeline calls it
  once instead of doing a separate parse pass plus a per-symbol Python resolution
  loop.
- **A resolution ladder, most-specific first** (`crates/eg-compute/src/parser/resolve.rs`):
  1. `same_file` — a definition in the caller's own file.
  2. `scoped` — a `self`/`this`/`super` (or implicit-this) call, or an explicit
     receiver naming a class, binds to that class's method or an **inherited** one.
  3. `arity` — same-name overloads disambiguated by argument count.
  4. `unique` — a single definition anywhere in the batch.
  5. otherwise **unresolved** — an ambiguous callee is never guessed.
  Every resolved `calls` edge carries a `strategy` and a `confidence`.
- **Structural class edges.** Class base/interface lists produce `inherits`
  (subclass to base) and `realizes` (class to interface) edges, resolved the same
  conservative way.
- **The OWL layer reasons over them.** `:inherits` is transitive, so the reasoner
  extrapolates inheritance chains; `:calls` reachability stays available to the
  graph algorithms (PageRank / community detection) over the now-accurate edges.

## The two ingest paths, one resolver

Both code-ingest paths consume the same Rust resolver:

- **Local `EnrichmentPipeline`** (`enrichment/pipeline.py`) — when the engine
  advertises `IndexRepository`, one `index_repository` call yields the symbols and
  the resolved `CALLS`/`INHERITS`/`REALIZES` edges. Name-only resolution remains
  only as the engine-unreachable fallback.
- **GitLab / source-sync** (`core/gitlab_indexer.py`) — already shipped each
  project to `index_repository`; it now also passes the `inherits`/`realizes`
  edges through, namespaced per instance.

## Surfaces

The resolved graph is queryable on both surfaces (same `_execute_tool` core):

- **MCP:** `graph_analyze(action="call_graph", node_id=<symbol>, target=<callees|callers|inherits>)`.
- **REST:** `GET /graph/analyze/call-graph?id=<symbol>&direction=<callees|callers|inherits>`
  (the action-routed `POST /graph/analyze` also accepts it).

## Flow

<div class="admonition architecture" markdown>
<p class="admonition-title">Code intelligence pipeline</p>

Source files feed the `index_repository` RPC, which tree-sitter parses
(scope, arity, call sites, bases) and resolves through the `resolve_site`
ladder (same_file → scoped → arity → unique), producing `calls` edges
(with strategy + confidence), `inherits`, and `realizes` into the
Knowledge Graph's `:Code` symbols. OWL reasoning over those symbols
(transitive inherits, call reachability) and the KG itself both feed the
`call_graph` query (MCP action and REST twin).

</div>

## Key files

| Layer | File |
|---|---|
| Rust extraction | `epistemic-graph/crates/eg-compute/src/parser/tree_sitter.rs` |
| Rust resolution | `epistemic-graph/crates/eg-compute/src/parser/resolve.rs` |
| Pipeline consumer | `agent_utilities/knowledge_graph/enrichment/pipeline.py`, `enrichment/extractors/code_test.py` |
| GitLab consumer | `agent_utilities/knowledge_graph/core/gitlab_indexer.py` |
| Ontology | `agent_utilities/knowledge_graph/ontology_software.ttl` (`:inherits`, `:realizes`) |
| Reasoning | `agent_utilities/knowledge_graph/core/owl_bridge.py` |
| Surfaces | `agent_utilities/mcp/tools/analysis_tools.py`, `agent_utilities/mcp/kg_server.py` |

## Model-free similarity (CONCEPT:EG-KG.compute.model-free-similar-code)

Code search and clone detection must keep working when the embedder is offline (the
recurring embedding-endpoint 502s). So similarity is **model-free**, computed in the same Rust
round-trip:

- Each symbol gets a **MinHash signature** over its normalized AST-leaf trigrams —
  identifiers/strings/numbers/types are abstracted to class tokens (so a
  renamed-variable clone still matches) while keywords/operators/punctuation are
  kept verbatim (so structure is preserved).
- The resolver **LSH-bands** the signatures: symbols colliding in any band are
  candidate pairs, linked with a symmetric scored `similar_to` edge when their
  estimated Jaccard ≥ 0.5 (capped per node; mega-buckets skipped). The signature is
  a compute-only input and is stripped from the graph nodes.
- `:similarTo` is a **symmetric** OWL property; the reasoner closes it both ways.
- Query it embedder-free: `graph_analyze(action="similar_code", node_id=…)` /
  `GET /graph/analyze/similar-code?id=…`.

This is the near-clone signal B4 reuses for `CodeClone`.

## Code ↔ service linking (CONCEPT:AU-KG.compute.http-route-graph)

Routes are the seam between a service's code and the live ecosystem. From the route
decorators the parser captured, the `routes` pass emits `Route` nodes (method+path)
and `serves` edges (handler `Code` → `Route`); a best-effort name match links each
`Route` to a deployed ecosystem `Service` (`servedBy`). The OWL surpass: reasoning
chains **Code –serves→ Route –servedBy→ Service –deployedOn→ Node** — a fact a
siloed per-repo code tool can't produce because it never sees the topology. Query
it: `graph_analyze(action="routes")` / `GET /graph/analyze/routes`. (gRPC/GraphQL
detection and event channels are later increments.)

## Infra, coupling, clones, decisions (CONCEPT:AU-KG.enrichment.read-them-here-so–2.105)

The graph spans past the code itself:

- **IaC → Resource (AU-KG.enrichment.read-them-here-so).** Dockerfiles, K8s/Kustomize manifests, and
  Terraform are parsed into `Resource` nodes (image/kind/name) and linked to the
  deployed `Service` they `provision` — so code → infra → topology is one graph.
- **Git change-coupling → FILE_CHANGES_WITH (AU-KG.ingest.mine-git-history-files).** Files that keep changing
  together get a symmetric weighted edge — the hidden blast radius the AST can't
  see. `graph_analyze(action="change_coupling", target=<repo>)`.
- **Near-clones** are the `similar_to` edges from EG-KG.compute.model-free-similar-code (MinHash) — no separate
  pass needed.
- **ADRs (AU-KG.compute.adr-crud).** `graph_analyze(action="adr")` creates/lists
  `ArchitectureDecisionRecord` nodes so design decisions live in the same KG.

## Change-scoped clone gates

The repository also has a local, deterministic clone gate for code review. It
uses two complementary scanners, with their exact versions and thresholds in
`pyproject.toml` under `[tool.agent_utilities.clone_scanners]`:

- `scripts/check_dupehound.py` runs on the normal pre-commit stage. It asks
  dupehound to compare only staged functions (or the working tree when there is
  no staged delta) against `HEAD`; `--base-ref` enables merge-base/PR semantics.
  It blocks a newly changed supported-language function that duplicates an
  existing function and returns exit 2 when the binary, version, or JSON result
  cannot be trusted.
- `scripts/check_duplication.py enforce --base-ref <ref>` is the bounded jscpd
  differential pass for code, template, configuration, and documentation
  blocks. Code remains in scope because jscpd can find a copied block inside
  two different functions, which a whole-function detector cannot.
  Markdown's plain-text fragments are reported by jscpd as its virtual `text`
  format, so that report format is explicitly included alongside the
  extension-backed `txt` format.
  It recomputes the base and merged clone sets in throwaway worktrees and fails
  only on new pairs. The `census` mode is all-format, full-tree, and advisory;
  it is manual-only so a whole-repository report never adds automatic push time.

Both wrappers read the same exclusion list for generated/vendor/build output,
lockfiles, fixtures, snapshots, and examples. The dupehound wrapper mirrors the
pinned v0.1.2 `check` classifier: test paths are outside its whole-function
scope, and it passes `--exclude-tests` explicitly because `--include-tests`
does not make that command inspect them. Ordinary test code remains in the
jscpd differential scope unless it matches a deliberate fixture/snapshot
exclusion; its block-level coverage is complementary. No baseline file is
written: pre-existing clone debt stays visible in a census and is not silently
converted into a permanent exception. The walkers preserve eligible hidden
directories (including `.github` for mapped formats) and prune only configured
junk plus Git metadata. Malformed or missing reports, non-regular report files,
clone locations outside the scan roots, inconsistent counts, and incomplete
throwaway-worktree cleanup all return exit 2 rather than a false green. Live
binary/workdir overrides are read through the repository config abstraction,
not directly from `os.environ`. Dupehound runs during pre-commit; the targeted
jscpd differential remains available manually so the automatic push gate stays
bounded.
The differential list is an explicit, reviewable allowlist; formats not yet
mapped there remain visible to the manual all-format census rather than
silently expanding the blocking scope.

<div class="admonition architecture" markdown>
<p class="admonition-title">Clone-detection gate flow</p>

A staged change or PR range goes through path selection + exclusions, then
two scanners: `dupehound` (changed functions) and `jscpd` (code + non-code
blocks). Either scanner finding something new (a new duplicate, or a new
clone pair) blocks with exit 1; a pre-existing clone pair instead feeds the
real clone-count report, alongside the separate advisory manual census
(all formats).

</div>

The native binaries are intentionally not installed by pre-commit. Install the
versions recorded in `pyproject.toml` ahead of time:

```text
cargo install dupehound --version <dupehound_version>
npm install -g jscpd@<jscpd_version>
```

Missing or drifted binaries fail closed rather than passing as an empty scan.

## Grammar coverage (CONCEPT:AU-KG.compute.built-ast-extended)

The core `ast` tier parses 9 languages (Python/JS/TS/Go/Rust/Java/C/C++/C#). The
feature-gated `ast-extended` tier (folded into the engine's `full` build) adds
Ruby, PHP, Bash, Scala, and Lua — so a slim build stays lean while the deployed
engine spans the common ecosystem languages. New grammars wire in by adding the
crate to the gate, an extension→grammar arm, and any new node-kind mappings; the
resolver/similarity/route passes are language-agnostic and benefit for free.
