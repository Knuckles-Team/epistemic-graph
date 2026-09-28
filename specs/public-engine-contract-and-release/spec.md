# EG-CONTRACT-001 — Public engine contract and reproducible clients

**Owner:** epistemic-graph. **Specification state:** PROPOSED. **Build state:** PARTIAL. **Acceptance:** PENDING.

## Purpose

A contributor must be able to change an engine method once and regenerate the same typed, signed, documented contract for Rust, Python, Go and JavaScript. An application must be able to discover callable methods and stable errors from a published EG package without importing private server internals. A release must prove that the published artifact behaves like the source revision.

## Delivery state legend

- **PROPOSED:** this reviewed contract describes work but gives no implementation credit.
- **QUEUED / BUILDING / BUILT:** implementation is assigned, in progress, or demonstrated on an unmerged branch. None implies a public artifact.
- **LANDED:** an exact commit containing the implementation is an ancestor of the public default branch, with a linked source test result.
- **ACCEPTED:** the same exact revision passes every required package, consumer, quality and release test in [test-spec.md](test-spec.md); evidence is recorded in [tasks.md](tasks.md).
- **PARTIAL:** some requirements have landed source while other requirements or release evidence remain. Record each requirement separately; do not promote the whole spec from a passing subset.

The current repository includes a generated method catalog, error catalog, Python typed models, Go and JavaScript codec clients and a package contract receipt. Their presence establishes a partial source baseline only. No release acceptance is claimed here.

## Normative requirements

| ID | Required behavior | Verifiable result |
|---|---|---|
| EG-C-01 | Rust method declarations are the sole authority for method identity, request/result schemas, stability, policy, replay class, consumer profiles, `is_wire_callable` and `error_set`. | `contract/methods.json` and schemas regenerate byte for byte; every declared wire method is reachable through an intentional dispatch path. |
| EG-C-02 | Rust `ErrorCode` is the sole authority for stable error code, class, retryability and HTTP status hint. Every method error set contains its actual reachable errors, including transaction conflict rather than a generic internal error. | `contract/errors.json` regenerates byte for byte and induced failures return the declared code at every supported client surface. |
| EG-C-03 | The Python wheel contains the method, error and scope catalogs, schemas and a digest receipt, with a public loader that rejects a missing or mismatched file. | A clean wheel install provides the same digest and schema as the source revision without source-tree reads. |
| EG-C-04 | Python request and result decoding uses generated strict nested models with unknown fields refused; handwritten convenience modules re-export generated types instead of defining competing wire shapes. | One class per wire shape and a typed round trip for every callable method. |
| EG-C-05 | Go and JavaScript sign the exact canonical method body produced by the Rust codec. They must not independently reimplement canonicalization. | Both replay all `contract/fixtures/method_body_vectors.json` fixtures and agree with the Python/Rust body digest. |
| EG-C-06 | Embedded WASM codec artifacts derive from a declared Rust source revision and deterministic toolchain recipe. A change in source or toolchain refreshes both copies and receipt; size is measured, not silently suppressed. | `scripts/build_method_codec_wasm.py --check` succeeds and Go/JS tests use the checked-in bytes. |
| EG-C-07 | A released artifact carries an exact source revision and catalog digest; the installed clients prove the same method/error behavior as source tests. | Rebuild, wheel completeness/privacy, consumer and package smoke evidence all identify the same commit. |
| EG-C-08 | A public PR gate uses vendored fixtures, temporary directories and provisioned local services where needed; no test requires a preexisting private deployment or a live operator host. Expensive soak and deployment tests run separately with documented provisioning. | A fresh checkout can run the blocking gate in CI, while skipped external integrations are reported as separate nonacceptance evidence. |
| EG-C-09 | CCCC, KISS, jscpd and Dupehound checks examine only relevant source and fixtures with committed configurations. A changed source tree introduces no new unsanctioned duplicate implementations or complexity regressions. | Differential scanner results and normal hosted checks pass on the exact release tree. |

## Architecture and ownership

The generator in `crates/eg-capabilities/src/bin/gen_contract.rs` reads Rust method and error declarations and writes `contract/`. `scripts/generated_artifacts.py` and the contract receipt detect drift. `epistemic_graph/contract/` is the package projection. `epistemic_graph/generated/models.py` is the Python type projection. `clients/go/codec.go` and `clients/js/codec.mjs` execute the embedded Rust codec WASM. The server owns dispatch and policy; clients only encode, send and decode. The HTTP hint is presentation metadata, never the policy decision itself.

The build graph is: Rust declarations → generated catalogs/schemas/fixtures → Rust codec WASM → Go/JS embedding and vector tests → Python wheel with receipt → clean-install consumer tests → release gate. Each arrow must have a deterministic check. Do not create a second method registry, error classifier, Python model hierarchy or signed-body encoder.

External applications consume only the published public catalog and clients. They may project EG methods into their own HTTP/MCP surfaces, but authorization must be checked by that application's own principal-aware service layer; `is_wire_callable` describes an EG capability, not a grant.

## Stable obligation coverage

| Train | IDs and disposition in this spec |
|---|---|
| T3 | EH-192 strict models; EH-372 signed Go/JS vectors. Verify exact default-branch source and package evidence. |
| T4 | EH-377 generated Python convergence; EH-383 codec WASM; EH-430 dispatch gate; EH-433 correct tree selection; EH-468 root Python CI suite; EH-592 error catalog; EH-643 generated client parity; EH-644 compiler dispatch census; EH-645 feature matrix; EH-646 current dispatch reachability; EH-647 component naming; EH-650 client lifecycle; EH-655 duplicate scan; EH-656 full tree hygiene. |
| T5 | EH-593 contract packaging and `is_wire_callable`; EH-519 bounded CI fanout; EH-520 and EH-561 KISS parser/config compatibility. |
| Cross-cutting | EH-366 wheel privacy; EH-469 complexity scoring. |

Adjacent EG capabilities specify their own semantics. This spec governs their published method/error projection and release proof.

## Completion criteria

All EG-C requirements have linked implementation commits, focused tests and an exact revision release matrix in [tasks.md](tasks.md). The published wheel and both embedded client codecs share one contract digest. Unknown/invalid payloads and policy failures have typed error parity. Hosted gates pass without private environment dependencies. Only then set **ACCEPTED**.
