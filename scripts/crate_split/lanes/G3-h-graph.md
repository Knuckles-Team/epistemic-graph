# Lane G3-h-graph: move the files placed in `medulla-h-graph`

Spec: `MEDULLA-CONTRACT-R056.7`. Start after: G2 landed.

## Base branch rule
Create the worktree from `main` after the previous group landed
(`git worktree add <wt> -b lane/crate-split-h-graph origin/main`). Never branch from another lane.

## Files to move (27; exactly these, nothing else)
- `src/server/handlers/graph_ops.rs`
- `src/server/handlers/graph_ops/algorithms.rs`
- `src/server/handlers/graph_ops/broker.rs`
- `src/server/handlers/graph_ops/edges.rs`
- `src/server/handlers/graph_ops/gateway.rs`
- `src/server/handlers/graph_ops/gateway_broker.rs`
- `src/server/handlers/graph_ops/gateway_graph.rs`
- `src/server/handlers/graph_ops/gateway_graph_routes.rs`
- `src/server/handlers/graph_ops/gateway_mining.rs`
- `src/server/handlers/graph_ops/gateway_mining_derived.rs`
- `src/server/handlers/graph_ops/gateway_mining_ml.rs`
- `src/server/handlers/graph_ops/hierarchy.rs`
- `src/server/handlers/graph_ops/memory.rs`
- `src/server/handlers/graph_ops/nodes.rs`
- `src/server/handlers/graph_ops/semantic.rs`
- `src/server/handlers/graph_ops/subgraph.rs`
- `src/server/handlers/graph_ops/terminal.rs`
- `src/server/handlers/graph_ops/union.rs`
- `src/server/handlers/graphlearn.rs`
- `src/server/handlers/pipeline.rs`
- `src/server/handlers/pipeline/features.rs`
- `src/server/handlers/pipeline/pipeline_model.rs`
- `src/server/handlers/placement.rs`
- `src/server/handlers/topology.rs`
- `src/server/handlers/write_back.rs`
- `src/server/harness_admission.rs`
- `src/server/harness_admission/tests.rs`


## Codemod (run in this order, from the worktree root)
1. `python3 scripts/crate_split/rewrite_paths.py --crate medulla-h-graph --moves > $SCRATCH/moves.sh`
2. `python3 scripts/crate_split/rewrite_paths.py --crate medulla-h-graph --mods > $SCRATCH/mods.txt`
3. `python3 scripts/crate_split/rewrite_paths.py --crate medulla-h-graph` rewrites `crate::<old path>` in the
   moved files and every caller, and points `placement.toml` at the new paths. Exit 1 prints the
   BLOCKED items and writes nothing.
4. `sh $SCRATCH/moves.sh` runs the `git mv` commands.
5. Apply `$SCRATCH/mods.txt`: add the `pub mod` lines to the new crate root and delete the listed
   `mod` declarations from the old parents. Move the crate's feature forwarding into its own
   `Cargo.toml` (the scaffold created the empty crate).
6. `python3 scripts/crate_split/rewrite_paths.py --crate medulla-h-graph --check` must report nothing.

## Seams this lane uses (from SEAMS.md)
| category | item | refs | first location |
|---|---|---|---|
| extract-helper | `crate::server::dispatch::authoritative_now_ms` | 2 | src/server/handlers/graph_ops/gateway.rs:145 |
| extract-type | `crate::server::handlers::mining::WritebackOptions` | 2 | src/server/handlers/graph_ops/gateway_mining.rs:2 |
| extract-type | `crate::server::handlers::mining::AnomalyRequest` | 1 | src/server/handlers/graph_ops/gateway_mining.rs:2 |
| extract-type | `crate::server::handlers::mining::AssociationRequest` | 1 | src/server/handlers/graph_ops/gateway_mining_derived.rs:3 |
| extract-type | `crate::server::handlers::mining::ClassifyPredictRequest` | 1 | src/server/handlers/graph_ops/gateway_mining.rs:2 |
| extract-type | `crate::server::handlers::mining::ClusterRequest` | 1 | src/server/handlers/graph_ops/gateway_mining.rs:2 |
| extract-type | `crate::server::handlers::mining::CommunityRequest` | 1 | src/server/handlers/graph_ops/gateway_mining_derived.rs:3 |
| extract-type | `crate::server::handlers::mining::EntityResolutionRequest` | 1 | src/server/handlers/graph_ops/gateway_mining_derived.rs:3 |
| extract-type | `crate::server::handlers::mining::ForecastRequest` | 1 | src/server/handlers/graph_ops/gateway_mining_derived.rs:3 |
| extract-type | `crate::server::handlers::mining::ReduceRequest` | 1 | src/server/handlers/graph_ops/gateway_mining.rs:2 |
| extract-type | `crate::server::handlers::mining::RiskPropagationRequest` | 1 | src/server/handlers/graph_ops/gateway_mining_derived.rs:4 |
| extract-type | `crate::server::handlers::mining::RootCauseRequest` | 1 | src/server/handlers/graph_ops/gateway_mining_derived.rs:4 |
| extract-type | `crate::server::handlers::mining::TextRequest` | 1 | src/server/handlers/graph_ops/gateway_mining_derived.rs:4 |
| seam:AdminApi | `crate::server::graph_schema::compose::validate_and_compose` | 1 | src/server/handlers/graph_ops/gateway_graph.rs:386 |
| seam:AdminApi | `crate::server::graph_schema::handle_gateway` | 1 | src/server/handlers/graph_ops/gateway_graph_routes.rs:656 |
| seam:DecideApi | `crate::server::handlers::decide::served_adapter::adapted_query` | 1 | src/server/handlers/graph_ops/semantic.rs:233 |
| seam:DecideApi | `crate::server::handlers::decide::served_adapter::carrier_tenant` | 1 | src/server/handlers/graph_ops/semantic.rs:233 |

Use the server-api trait or extracted item named in the table. Do not invent another seam.

## Files it must not touch
Every file outside the list above, except caller rewrites from step 3. Do not change
`scripts/crate_split/*.toml` by hand; step 3 updates `placement.toml`. Do not copy helpers:
move them. Do not add re-exports. Widen `pub(crate)` to `pub` only where placement requires it.

## Gates (no local cargo)
- `python3 scripts/check_crate_graph.py` passes.
- `python3 scripts/crate_split/edges.py` shows no back-edge out of `medulla-h-graph` except the listed seams.
- `python3 scripts/check_spec_paths.py` passes; rewrite the `Spec:` test bindings and spec file paths
  that name moved files in the same commit, then regenerate spec status.
- `ruff check` and `ruff format --check` on touched Python.

## Land
```
eg-lane-run enqueue --lane land-h-graph --worktree <wt> --test '-p medulla-h-graph::all()'
```
The change lands on a passing build-train run of its changed crates. Link the train result in the
pull request. Commit trailer: `Spec: MEDULLA-CONTRACT-R056.7`.

## STOP conditions
- Any `BLOCKED` or `decide` hit (from `rewrite_paths.py` or the seam table below) means STOP. Write `BLOCKED.md` in the lane directory (outside the repository) naming the item and the file:line, then exit. Do not improvise a seam.
- Any file outside the list above, other than caller rewrites made by `rewrite_paths.py`, means STOP.
- Cold pull-request CI timing out is not a blocker (MEDULLA-CONTRACT-R056.12).
- Never run cargo locally, never `git stash`, never `git add -A` or `git add .`, never `--no-verify`, never force-push.
