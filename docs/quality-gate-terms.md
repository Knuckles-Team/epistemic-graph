# Terms of Acceptance — the cccc and KISS gates

*Measured 2026-09-11 against the full tracked tree (1,571 CCCC-supported files /
44,404 functions; 1,279 tracked Rust files / 20,011 KISS units), with
`cccc 1.6.0` and `kiss 0.4.10`.*

This page states what the two **shape** scanners accept and why, so that their
reports read as **clean-with-known-exceptions** instead of as permanent noise,
and so the number a burndown lane inherits is a real backlog rather than a raw
finding count.

## The rule about rules

The project rule is **NO RATCHETS — expose tech debt.** Everything on this page
obeys it, and the test is mechanical:

* An exception here is a **RULE about a class of code**, with a stated reason and
  the measurement behind it. It is recomputed from the source under measurement
  on every run.
* There is **no list of accepted files**, **no list of accepted functions**, **no
  frozen count**, **no `--update-baseline`**, and **no in-line suppression
  comment**. Nothing on this page can be satisfied by editing a ledger.
* A finding that is accepted is still **counted and printed**. An exemption
  nobody can see is a baseline by another name.
* `.kissconfig` must never exist. Bare `kiss check` writes one, self-calibrated
  from what the repo currently passes, and silently disables four global rules.
  The `kiss-census` hook fails if the file is present.

## CCCC — the cyclomatic cap measures the wrong thing for Rust dispatch

Caps are `scanner_contract.CCCC_MAX_CYCLOMATIC = 10` and
`CCCC_MAX_COGNITIVE = 15`, enforced on the diff by the `complexity-staged`
pre-commit hook and reported over the whole tree by `cccc-census`.

Cyclomatic complexity charges **every `match` arm** as an independent decision
point. For a flat, exhaustive `match` that is arithmetically correct and
semantically empty — there is no nesting, no interleaving, and no control-flow
surprise. Cognitive complexity, which does not charge a flat match per arm,
reports the same functions as trivial:

| function | cyclomatic | cognitive |
|---|---|---|
| `src/server/wire/mod.rs::dispatch_kind` | 32 | **1** |
| `src/server/persistence/redb_backend.rs::handle_cmd` | 54 | **8** |

Decomposing a genuinely exhaustive match is not a neutral refactor: it trades
away rustc's exhaustiveness guarantee. While the match names every variant,
adding an enum variant is a **compile error** at every dispatch site. Behind a
lookup table or a boxed closure map, the same addition becomes a silent runtime
fallthrough; behind a macro, the arms merely disappear from the scanner while
the code is unchanged. Both are worse code with a better number.

### The rule

Stated and implemented once, in `scripts/rust_exhaustive_match.py`. A Rust
function may exceed the **cyclomatic** cap — never the cognitive cap — only if
**all four** hold:

1. its **cognitive complexity is within the cognitive cap**. The flat-dispatch
   claim is falsifiable, and cognitive complexity is the falsifier;
2. its body contains **at least one `match`**. Branching that is not dispatch —
   an `if`/`else` ladder, a chain of `?` — is ordinary complexity and gets no
   relief;
3. **no arm of any `match` in the body is irrefutable**: no `_`, no bare binding
   (`other`, `ref x`, `mut x`, `x @ _`), with or without a guard, and no
   `|`-alternation containing one. An irrefutable arm is exactly what makes a
   match non-exhaustive, so decomposing it forfeits no guarantee;
4. its **residual cyclomatic complexity** — the measured value minus the number
   of match arms in the body — is within the **same cyclomatic cap every other
   function obeys**. This introduces no new threshold. It says only that once
   the exhaustive dispatch is discounted, what is left must pass the ordinary
   gate.

Everything the rule cannot prove is **not** exempt: non-Rust source, a body that
cannot be lexed, a missing start line, and an arm count that exceeds the measured
cyclomatic complexity (which means the attribution is wrong, so the residual
cannot be trusted) all fail closed.

Arm patterns are read from comment- and literal-masked source (`rust_lexer`), so
a `_ =>` inside a string or a comment cannot invent a catch-all. The whole
function body is brace-matched — **not** a fixed line window. A first attempt at
this measurement used a 400-line window and misclassified a large dispatcher
whose catch-all sat beyond that window. `tests/test_rust_exhaustive_match.py`
pins the current streaming dispatcher shape and a synthetic case whose catch-all
appears after 450 lines, preserving coverage of that regression.

Before the terms report is classified, `scripts/validate_cccc_census.py`
requires the native CCCC schema: numeric fields stay in native ranges, names
are nonempty, and summary file/function/parse counts plus recursive child
metrics exactly match the retained report tree. File totals may also include
module-level code, so they only need to be at least the recursive function
totals. A file with no functions remains valid when its summary function
metrics legitimately report zero.

### The rule is strictly tighter than what it replaces

Nothing previously checked whether a high-cyclomatic match was exhaustive at
all. A 68-arm dispatcher ending in a catch-all — which is **not** exhaustive,
whose decomposition costs no safety whatsoever, and which is therefore ordinary
debt — was exactly as unremarked as a genuinely exhaustive one. Applying this
rule makes that population visible for the first time. That is the intent.

### Measured result

```
44,404 functions measured, 437 over cyclomatic 10 or cognitive 15

  cognitive over cap          201   genuinely complex — never accepted
  ACCEPTED BY RULE             90   exhaustive dispatch, residual within cap
  catch-all arm                71   the match is NOT exhaustive — newly visible debt
  no `match` in the body       41   branching that is not dispatch
  residual over cap             7   dispatch discounted, the rest still exceeds it
  arm count > cyclomatic        3   attribution unproven — fails closed
  not Rust (Python / JS)       24   rustc exhaustiveness does not apply

  REAL BACKLOG                347
```

`scripts/report_complexity_terms.py` prints exactly this table. Its default
invocation remains an advisory report, so existing inspection commands keep
working. The `cccc-census` hook and the release scanner run it with
`--require-zero`: the complete accepted and backlog breakdown remains visible,
and the command exits 1 when `REAL BACKLOG` is nonzero. This is a live
whole-tree gate with no threshold, baseline, or suppression list, and it writes
nothing.

The **41 with no `match` at all** are worth calling out. An earlier
classification that only grepped for a catch-all pattern counted them as
"exhaustive" and would have exempted them. They are `if`/`else` ladders and `?`
chains, and they are debt.

## KISS — three rules were calibrated for OO, not for Rust

Thresholds live in `.kiss/kiss.toml`, which carries the measurement next to every
number. Three were recalibrated on 2026-09-11; the file holds the full reasoning
inline, summarised here.

| rule | was | now | measured distribution | why |
|---|---|---|---|---|
| `returns_per_function` | 5 | **8** | p90=2, p95=3, p99=5, max=29 over 16,883 functions | At 5, only **33 %** of findings are corroborated by cccc cognitive complexity > 15; at 8, **53 %**. 8 is the smallest cap at which the rule is right more often than wrong. Below it, it fires mostly on flat guard-clause validators — the exact shape its own advice tells you to write. |
| `methods_per_class` | 10 | **13** | p90=8, p95=11, p99=31, max=222 over 2,040 impl units | KISS aggregates every impl block of a type within a file, **trait impls included**, and its advice ("extract related methods into a separate type with its own impl") is **impossible** for a trait impl — the trait defines the method set. This workspace's own `ModalityContract` has 22 implementations, 8 of them at exactly 12 methods and the largest at 13. A cap below 13 demands a refactor that does not exist. |
| `concrete_types_per_file` | 8 | **20** | p90=7, p95=9, p99=18, max=140 over 1,279 files | Of 70 files over 8, **40 are already flagged oversized** by `lines_per_file` / `statements_per_file` / `functions_per_file`; for those the type count adds nothing. The 30 it flags *alone* are uniformly the Rust contract-module pattern — `eg-types/native_control.rs` is 276 lines and 19 types, one `Capacity*` / `SubmitWorkItem*` request+result vocabulary. 20 clears the largest coherent contract module measured (19) and still names 9 grab-bags. |

Costs and judgement calls, stated rather than buried:

* `returns_per_function = 8` fixes the curve by measurement, but *"the rule
  should be right more often than it is wrong"* is an acceptance criterion
  chosen by a human, not derived from the data. It is a **judgement**.
* `methods_per_class = 13` also relaxes **inherent** impls, where p95 is exactly
  10 and the cap was therefore well calibrated. 88 of the 107 impls over 10 are
  inherent. KISS has one global number per language and cannot separate the two
  populations. The inherent god-objects (222, 58, 34, 32, 26, 22 methods) and the
  over-broad interfaces `ChunkStore` (21) and `PersistenceBackend` (44) all
  remain flagged.
* Two premises that turned out to be **false** and are recorded so nobody
  re-derives them: KISS does **not** let you evade `methods_per_class` by
  splitting a type into two `impl` blocks in the same file (it aggregates them),
  and `returns_per_function` does **not** count `?` or the tail expression
  (verified on a probe) — so the "it punishes idiomatic Rust `?`" argument does
  not hold. The reasons above are the ones that survive measurement.

### Rules deliberately left alone

`nested_function_depth` (46), `max_indentation_depth` (7), `boolean_parameters`
(26), `positional_args` (90), `local_variables_per_function` (91),
`duplication` (3), `calls_per_function` (203), `statements_per_function` (152),
and the file-size family — `lines_per_file` (97), `statements_per_file` (80),
`functions_per_file` (74), `imported_names_per_file` (10) — are **real debt at
their current numbers**. Each is already set at a measured percentile in
`.kiss/kiss.toml` and none of them mismeasures a Rust idiom. They belong to the
burndown lane, not to this page.

### Measured result

Full tracked-tree census, the same 1,279 files under both configurations:

```
BEFORE (thresholds of 2026-08-27)  1,203 violations
AFTER  (thresholds of 2026-09-11)  1,010 violations

  returns_per_function      144 ->  51   (-93)
  methods_per_class         107 ->  68   (-39)
  concrete_types_per_file    70 ->   9   (-61)
  every other rule                unchanged
```

**193 accepted by rule; 1,010 real backlog.**

## `kiss-changed-rust` is diff-scoped, not whole-file (BUG-CX-136 / F6)

`kiss check <file>` always reports EVERY violation the whole file carries,
not just what a commit's diff touched. Before this rule, `kiss-changed-rust`
therefore failed a commit that added a single comment to a large,
already-violating file for debt the commit never touched — the standard
workaround was `git commit --no-verify`, which silently disables every other
pre-commit hook too, not just this one.

The hook (`scripts/check_kiss_staged.sh`) now re-runs KISS a second time on
the HEAD blob of each changed file (a second ephemeral tree,
materialized once per run via `git archive HEAD`, alongside the existing
staged-index tree) and narrows the staged report through
`scripts/kiss_diff_scope.py` before deciding pass/fail:

* **Function/item-scoped rules** (`statements_per_function`,
  `returns_per_function`, `calls_per_function`, `local_variables_per_function`,
  `max_indentation_depth`, `branches_per_function`, `boolean_parameters`,
  `positional_args`, `annotations_per_function`, and other `*_per_function`
  rules): a finding counts only if the enclosing function/method is NEW (no
  same-named function existed at HEAD) or MODIFIED (the same-named
  function's exact source text — extracted by locating `fn <name>` in a
  comment/string-masked view of the file and matching its balanced `{...}`
  body, the same lexical authority every other Rust scanner in this
  repository shares) differs from its HEAD version. A same-named function
  appearing more than once in a file is matched to its counterpart by
  ordinal position (file order), not by name alone. **Touching a violating
  function's body means fixing it** — a modification does not get to keep
  riding on "it was already broken."
* **File- or type-aggregate rules** (`lines_per_file`, `statements_per_file`,
  `functions_per_file`, `interface_types_per_file`, `concrete_types_per_file`,
  `imported_names_per_file`, and `methods_per_class` — which sums one type's
  methods across every `impl` block in the file, so it has no single
  contiguous span to diff): a finding counts only if the rule is absent from
  the HEAD report for that file (newly crossed) or its reported count is
  strictly larger than HEAD's (worsened). An unchanged or improved count is
  pre-existing debt and does not fail the commit.

Matching is **content-based, never line-number or bare-symbol-name based**:
line numbers shift under reformatting and mechanical merges, and a bare
symbol name breaks under extraction (a function moved to a new module keeps
its name but is a different "item" for lineage purposes) — see
`symbol-keyed-baselines-break-under-extraction` and
`architecture-gates-key-on-byte-offsets` in the operator's working notes.
There is **no baseline file, allowlist, or self-updating count** anywhere in
this comparison: both the staged and the HEAD report are computed fresh, from
the two Git blobs, on every hook invocation — a ratchet would let today's
`kiss.toml` thresholds erode quietly; this rule instead re-derives "was this
introduced or made worse by THIS diff" from scratch every time.
`tests/test_kiss_diff_scope.py` fixtures the four defining scenarios directly
against the matcher (comment-only change to a file with a pre-existing
violation → pass; a new violating function → fail; modifying an
already-violating function → fail; a file-level threshold newly crossed →
fail); `tests/test_kiss_staged.py` additionally proves the hook's bash-level
wiring (materializing the HEAD tree, running the second KISS pass, invoking
the filter, propagating its exit status) end-to-end with a fake KISS binary.

The pinned-version check, the `--config .kiss/kiss.toml` requirement, the
one-path-per-invocation rule below, and the `.kissconfig` prohibition are all
unchanged by this — diff-scoping narrows WHICH of KISS's own findings can
fail the commit; it never changes what KISS itself measures or how it is
invoked per file.

## Running the scanners

Never run bare `kiss check` — it writes the self-calibrating `.kissconfig`.
Always pass `--config .kiss/kiss.toml` and **one path per invocation**
(KISS 0.4.10 reports a false clean for a multi-path `check`). The two callers
that get this right are `scripts/check_kiss_staged.sh` (the pre-commit hook) and
the `kiss-census` hook; go through one of them.

```bash
pre-commit run --config .config/pre-commit.yaml complexity-staged --all-files   # cccc, on the diff
pre-commit run --config .config/pre-commit.yaml kiss-changed-rust --all-files   # KISS, on the diff
pre-commit run --config .config/pre-commit.yaml cccc-census --hook-stage manual --all-files # whole tree + the acceptance split
pre-commit run --config .config/pre-commit.yaml kiss-census --hook-stage manual --all-files
```

A whole-tree KISS census is ~9 minutes single-threaded; parallelise with
`xargs -0 -P 14` over `python3 scripts/list_scanner_sources.py kiss`.
