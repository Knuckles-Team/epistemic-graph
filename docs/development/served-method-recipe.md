# Recipe: serve a typed kernel as a wire method (a `.3` wiring row)

A `.3` row says "the typed model and kernel exist; serve them". This recipe turns one
kernel function into a served method. The method has a generated contract, generated
clients and a served integration test. It was proven on `EG-FINANCE-PRIMITIVES-R900.3`
(`FinancePositionSize`, `FinanceDistributionShift`) and repeated for R901.3, R613.3 and
R018.3 (`src/server/handlers/finance/numeric_kernels.rs`).

Do not hand-edit anything generated. Do not run `git add -A`; stage explicit paths.

## 0. Setup

```
git fetch origin
git worktree add ~/.local/state/repository-worktrees/medulla/<NAME> -b <branch> origin/main
export RUSTC_WRAPPER=sccache CARGO_BUILD_JOBS=6 CARGO_TARGET_DIR=$PWD/target-isolated
```

Batch every method of one PR into ONE regeneration. Steps 4 and 5 cost about 13 and 8
minutes each. A cold test build costs about 35 minutes, then about 7 minutes per edit.
Write all code and the test first, then regenerate once.

## 1. Make the kernel reachable from the root crate

The handlers live in the root crate (`src/server/handlers/...`). It reaches `eg_numeric`
only through `eg-compute`:

- `crates/eg-compute/Cargo.toml`: the `finance` feature already lists `dep:eg-numeric`. Add
  any kernel feature it needs (R900 needed `eg-numeric/analytics` for `ks_2samp`).
- `crates/eg-compute/src/finance/mod.rs`: `pub use eg_numeric::{banking, execution, sizing};`
  The handler then calls `crate::finance::sizing::...`. (`crate::finance` is
  `eg_compute::finance` when the root `finance` feature is on.)

## 2. Declare the method and its result (eg-types)

1. Request: add a variant to the `Method` enum in
   `crates/eg-types/src/protocol/method/method_04.rs` (finance methods live there; any
   `method_NN.rs` chunk works). Use flat, serde-friendly fields. Use
   `#[serde(default)] Option<T>` for a field with a kernel default. Never change an
   existing variant: that breaks the wire contract.
2. Result body type, when the result is not a scalar: add a struct to
   `crates/eg-types/src/compute_result/finance.rs` with
   `#[derive(Serialize, Deserialize, Debug, Clone)]` and
   `#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]`. Mirror the
   kernel struct; the kernel crate stays free of `schemars`. Render closed enums as `String`.
3. Result contract: add one line to `crates/eg-types/src/result_contract/compute.rs`:
   - scalar: `FinanceX(FinanceX) => Float<f64>;`, answered with
     `ResultPayload::scalar::<results::FinanceX>(v)`.
   - struct or list: `FinanceX(FinanceX) => Raw<finance::Body>;`, answered with
     `ResultPayload::of::<results::FinanceX>(body)`.
   - put `#[cfg(feature = "finance")]` above any line whose body type is in `finance::`.

## 3. Register, classify, route, handle

Every new method name needs all of these, or `gen_contract` or the router refuses it:

| Where | What |
|-------|------|
| `crates/eg-capabilities/src/domains/compute.rs` | one registry tuple per method. Copy the `FinanceKellyFraction` line and rename it (pure compute, `compute:finance`, `DurabilityDomain::None`, idempotent). |
| `src/server/access/read_classification.rs` | `("FinanceX", REASON_PURE_COMPUTE),` |
| `src/server/mutation_batch/canonical.rs` | `\| Method::FinanceX { .. }` in the pure-read list, next to `FinanceKellyFraction`. |
| `src/server/handlers/finance/routes.rs` | map the variant to a `FinanceRoute` in a `route_for_*` match. |
| `src/server/handlers/finance/route_families.rs` or a new file | the handler arm (below). |
| `crates/eg-capabilities/tests/contract_registry.rs` | bump the exact wire-method census and add one comment line. |

Rules for the handler:

- The handler is the only caller of the kernel. It maps wire fields to the kernel type and
  the result back. It copies no formula.
- Refuse with `Response::err(req_id, format!("INVALID_ARGUMENT: {error}"))`. The kernel's
  stable code stays first in its `Display`. A dedicated error code is the job of the row's
  `.4` refusal child.
- `route_families.rs` is large. For a new family of methods add a file such as
  `finance/numeric_kernels.rs`, a `FinanceRoute::<Name>` variant and its `try_handle` arm in
  `routes.rs`, and `mod <name>;` in `src/server/handlers/finance.rs`.

Check that it compiles before the slow steps (about 3 minutes when warm):

```
cargo check --locked --lib --no-default-features --features "server,finance"
```

## 4. Regenerate the contract, models and Python client

```
cargo run -q --locked -p eg-capabilities --features contract --bin gen_contract
```

This writes about 74 artifacts: `contract/` (methods, schemas, fixtures, receipt),
`epistemic_graph/contract/`, `epistemic_graph/generated/*.py`,
`crates/eg-capabilities/generated/{method_catalog,catalog_digest}.rs` and
`docs/capabilities.generated.md`. `gen_contract -- --check` is the CI parity gate.

Optional hand-written convenience, as `NlQueryExplain` did: a wrapper in
`epistemic_graph/client.py` (finance wrappers are near `kelly_fraction`) that calls the
generated `_gen.compute.send_finance_x(...)`.

## 5. Rebuild the method-codec wasm and update its pin

```
python3 scripts/build_method_codec_wasm.py --target-dir "$CARGO_TARGET_DIR"
```

This rewrites `clients/go/eg_method_codec.wasm`, `clients/js/eg_method_codec.wasm` and the two
sha256 pins in `.config/generated-artifacts.toml`. CI runs it with `--check`.

Do not commit the output of `python3 scripts/gen_api_docs.py --write`: `docs/api/` and
`docs/openapi.json` drift from the base branch independently of your method. If you ran it,
run `git checkout -- docs/api docs/openapi.json`.

## 6. Served integration test

Create `tests/finance_served_<topic>.rs` by copying `tests/finance_served_sizing.rs`. It
drives the real signed `dispatch` surface: a fresh state, `CreateGraph`, then the method.

- Gate it with `#![cfg(all(feature = "server", feature = "security", feature = "finance"))]`.
- Every dispatch needs an existing graph, so the helper creates one first. An empty graph
  name returns `Graph '' not found`.
- Scalar results decode as `ResultPayload::Float(v)`. `Raw` results are MessagePack bytes:
  `rmp_serde::from_slice::<serde_json::Value>(bytes)`.
- Assert one success value, then one refusal: `error == "INVALID_ARGUMENT"` with the kernel
  code inside `error_detail`.

```
cargo test --locked --features "server,security,finance" --test finance_served_<topic>
```

## 7. Spec text and commit

- In the row's `specs/<spec>/requirements.md`, name the served method and the test file in
  the `.3` row. Leave `status.json` alone: the spec status tooling generates it.
- Stage explicit paths only: `contract/`, `epistemic_graph/`, `crates/`, `src/server`,
  `tests/<file>`, `clients/*/eg_method_codec.wasm`, `.config/generated-artifacts.toml`,
  `docs/capabilities.generated.md`, the spec file. Commit with a `Spec: <ROW-ID>` trailer.
  Delete `target-isolated/` when you stop.

## Rows this recipe does not fit

Rows whose served half is stateful, or whose caller is another engine layer, need design
first. The federated rows are of this kind:

- `EG-FEDERATED-QUERY-R703.3` changes `ContextCompile` in `crates/eg-plan/src/exec.rs` to call
  an access-check port that the Graft SDK answers.
- `EG-FEDERATED-QUERY-R704.3` makes `Discover` and `ContextCompile` read a stored agent scope.
- `EG-FEDERATED-QUERY-R705.3` adds `RetrievalFeedbackRecord`, a write that needs a durable
  feedback store, a rate limit and a read-permission check.

Their typed models (`.1`, `.2`) must first be wired into the retrieval executor, and their
tests are Python tests against a running arbora. They are not a pure wire method.
