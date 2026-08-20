# RBAC action unification — Phase 1 design + blast-radius map (NE-047 / EG-ACCESS)

Status: **Phase 1 complete. Verdict: NOT safe to implement full unification in this
change. No behaviour changed by this document.**

## 1. Current shape of both vocabularies

### 1.1 `eg-core` graph RBAC (3-valued)

- `RbacAction` — `crates/eg-types/src/acl.rs:99-104`:
  ```rust
  pub enum RbacAction { Read, Write, Admin }
  ```
  Plain `#[derive(..., Serialize, Deserialize)]`, no `#[serde(rename..)]`. Consumed
  by `Grant { role, resource: ResourceSelector, action: RbacAction, effect:
  GrantEffect }` (`acl.rs:217-223`), evaluated by `RbacPolicy::evaluate` in
  `crates/eg-core/src/rbac.rs:93-122`: default-deny, most-specific-`ResourceSelector`-wins,
  deny-overrides-allow at a tie. `ResourceSelector` (`acl.rs:119-129`) is
  `All | Pattern(glob) | Label(String) | Graph(String)` — i.e. it addresses **graph
  resources only**; it has no notion of a SQL table, a row, or an owner.
- A second, narrower, **2-valued** enum sits in front of it purely for graph
  read/write requests: `AccessLevel { Read, Write }` (`crates/eg-core/src/isolation.rs:14-17`).
  `IsolationLayer::check_access` (`isolation.rs:873-905`) maps
  `AccessLevel::Read → RbacAction::Read`, `AccessLevel::Write → RbacAction::Write`
  1:1, then calls `self.rbac.evaluate(&identity.roles, &ctx, action)`.
  `IsolationLayer::has_admin_capability` (`isolation.rs:1105-1120`) evaluates
  `RbacAction::Admin` against a fixed `ResourceContext::graph("__admin__")`.
  **These two functions are the entire production seam** — see §2.

### 1.2 NE-003 SQL catalog ACL (5-valued, independent)

`src/server/sql_catalog_acl.rs` — **not a workspace crate**; it lives in the root
`epistemic-graph` package (`Cargo.toml:88`), a different Cargo package from
`eg-core` (`crates/eg-core`, workspace member).

```rust
pub(crate) enum SqlPrivilege { Select, Insert, Update, Delete, Alter }  // sql_catalog_acl.rs:113-120
```

The module's own doc comment (`sql_catalog_acl.rs:106-111`) states the reason
explicitly: *"Deliberately NOT `eg_types::acl::RbacAction` (Read/Write/Admin) —
that three-way split would collapse INSERT/UPDATE/DELETE into one bucket, making
them impossible to grant or revoke independently, which the spec explicitly
requires."*

Crucially, the divergence is not only the action vocabulary — the **storage and
resource model are structurally different**:

- **Ownership**: `__eg_sql_owners__` records a first-writer-wins `(table_name →
  owner agent_id)`. `RbacPolicy`/`Grant` has no ownership concept at all — only
  role-scoped allow/deny grants.
- **Grants are principal-direct, not role-mediated**: `__eg_sql_grants__` is
  `(table_name, principal, privilege)` — `grant()` takes a bare `grantee_agent_id`
  (`sql_catalog_acl.rs` `pub(crate) fn grant`), never a role name. `RbacPolicy`
  grants are always `role → resource`, resolved through `IsolationLayer::agents[...].roles`
  and `expand_roles`; there is no "grant this exact agent_id" primitive.
- **Resource addressing**: `ResourceSelector` addresses graphs/labels, not SQL
  tables. There is no `ResourceSelector::Table(String)` today.
- **Denial semantics**: `authorize()` (`sql_catalog_acl.rs:346-366`) is
  hand-tuned to avoid an existence leak (identical `ACCESS_DENIED` string
  whether the table doesn't exist or simply isn't granted) and deliberately
  performs the *same two scans* (owners then grants) on both denial paths to
  equalize the obvious timing signal. `RbacPolicy::evaluate` has no such
  invariant or test today; replicating it is new work, not reuse.
- **Row-level security**: `__eg_sql_rls__` declares one RLS discriminator column
  per table, folded into query predicates in Rust (never spliced SQL). No
  analogue in `eg-core::rbac`.

## 2. Blast radius (file:line, production vs. test)

### 2.1 `RbacAction` (grep across `src/` + `crates/`, 15 files, 102 raw hits)

Files touching `RbacAction`:
```
src/server/sql_catalog_acl.rs        (doc comment only, cross-reference — 1 hit)
src/server/graphql_sub.rs            (test-only)
src/server/mysql_wire/mod.rs         (test-only)
src/server/mod.rs                    (test-only)
src/server/mqtt_wire/mod.rs          (test-only)
src/server/bolt_wire/mod.rs          (test-only)
src/server/dispatch.rs               (test-only)
src/server/stomp_wire/mod.rs         (test-only)
src/server/persistence/cold_offload.rs (test-only)
src/server/handlers/query.rs         (test-only)
crates/eg-core/src/rbac_persist.rs   (test-only)
crates/eg-core/src/isolation.rs      (2 production call sites + tests)
crates/eg-capabilities/src/lib.rs    (comment cross-reference only)
crates/eg-core/src/rbac.rs           (type owner + its own tests)
crates/eg-types/src/acl.rs           (type definition + its own tests)
```

**Verified against each file's `#[cfg(test)] mod tests { ... }` boundary**
(`mod.rs` tests start `:594`, `dispatch.rs` tests span `:7977-8925`, `isolation.rs`
tests start `:1153`, `rbac_persist.rs` tests start `:450`): every `RbacAction`
reference in `src/server/{mysql_wire,graphql_sub,mqtt_wire,mod,bolt_wire,
dispatch,stomp_wire,persistence/cold_offload,handlers/query}.rs` and in
`crates/eg-core/src/rbac_persist.rs` sits **inside** those test modules — they
are wire-protocol test fixtures that provision `Grant`s to exercise
`IsolationLayer` end to end, not independent production logic.

**Production call sites of `RbacAction` — exactly two, both in
`crates/eg-core/src/isolation.rs`, both above its `mod tests` boundary at
`:1153`:**

| Site | What it does |
|---|---|
| `isolation.rs:900-901` (inside `check_access`, `:873-905`) | `AccessLevel::Read ⇒ RbacAction::Read`, `AccessLevel::Write ⇒ RbacAction::Write`, then `self.rbac.evaluate(...)` |
| `isolation.rs:1118` (inside `has_admin_capability`, `:1105-1120`) | hardcoded `RbacAction::Admin` against `ResourceContext::graph("__admin__")` |

Consumers of `check_access`/`has_admin_capability` (the next layer out — these
never see `RbacAction` directly, only `AccessLevel`/`bool`):
- `src/server/access.rs:813`
- `src/server/wire/mod.rs:1409-1419` (`WireSession::check_access` wrapper), called
  from `wire/mod.rs:2475`, `:4318`, `:4402`
- 2 call sites for `has_admin_capability` outside `isolation.rs` (in
  `src/server/access.rs`, gating `require_admin_capability`).

**Conclusion: widening `RbacAction`'s variant set is, mechanically, a
single-file change** (`crates/eg-types/src/acl.rs`'s enum definition) with
exactly one production consumer to extend (`isolation.rs`'s `check_access` /
a new access-level mapping) — *if* the goal were only "add variants nobody
uses yet." It is not compile-blast-radius that makes this hard (see §5); it is
the SQL-ACL rewiring described in §1.2 and §4.

### 2.2 `RbacPolicy::evaluate` (or equivalent)

- Definition + only production caller path: `crates/eg-core/src/rbac.rs:93-122`
  (`evaluate`), `:128-138` (`is_allowed`, thin wrapper).
- Production callers: **isolation.rs:904, isolation.rs:1118** (the same two
  sites as above — `evaluate` has no other production caller anywhere in the
  workspace).
- Test callers: `rbac.rs`'s own `mod tests` (7 tests, lines 155-297),
  `rbac_persist.rs::tests` (roundtrip test, line 502), `isolation.rs::tests`
  (line 2276).
- `src/raft/network.rs:184,337` calls a *different* `is_allowed` (on a raft
  peer-auth type, unrelated to `RbacPolicy` — confirmed by reading the call
  site; false positive from the initial grep, noted here so the count is
  auditable).

### 2.3 `SqlPrivilege` / SQL ACL consumer surface

- Definition + all internal machinery: `src/server/sql_catalog_acl.rs` (58
  references, the module itself — `authorize`, `authorize_ddl`,
  `authorize_insert`, `authorize_update`, `authorize_delete`, `grant`, `revoke`,
  `owner_of`, `grant_exists`, RLS get/set).
- External consumers (all outside `eg-core`, inside the root `epistemic-graph`
  package):
  - `src/server/wire/mod.rs` — ~26 references: DDL gating (`:447-500`),
    `authorize_insert`/`authorize_update`/`authorize_delete` on the DML path
    (`:513,534,551`), `SqlPrivilege::Select` for reads (`:1527`),
    `SqlPrivilege::Insert` for writes (`:2294`), plus the admin `grant()` calls
    at `:5591-5940` (tests).
  - `src/server/handlers/rdf.rs:821` — one `SqlPrivilege::Select` check (RDF
    materialized-over-SQL read path).

**This entire consumer surface (`sql_catalog_acl.rs`, `wire/mod.rs`,
`handlers/rdf.rs`) lives in the root `epistemic-graph` Cargo package, not in
`eg-core`.** The task's sanctioned validation command is scoped to
`cargo check -p eg-core` only — it does not compile any file in this list. Any
change to this surface could not be verified within the validation this task
authorizes (see §5).

## 3. Persistence / wire encoding of `RbacAction`

Two independent codecs both use vanilla `#[derive(Serialize, Deserialize)]`
with no custom variant naming, so both use serde's default **externally-tagged,
variant-name-string** representation for a unit-only enum:

1. **Durable store** — `crates/eg-core/src/rbac_persist.rs`. One redb table
   `rbac_v1` in `{persist_dir}/rbac.redb`, three keys (`policy`, `identities`,
   `bootstrap`); `policy` is `serde_json::to_vec(&RbacPolicy)`
   (`rbac_persist.rs:12-19`). A `Grant.action: RbacAction` therefore lands in
   that JSON blob as the literal string `"Read"`, `"Write"`, or `"Admin"`.
2. **Wire protocol** — `crates/eg-types/src/protocol.rs:1-5`: *"Length-prefixed
   MessagePack framing"*, confirmed by `rmp_serde::to_vec`/`to_vec_named` /
   `from_slice` call sites throughout `src/server/mod.rs`. `Method::RbacAdmin {
   op: RbacAdminOp }` (`protocol.rs:2299-2305`) carries `Grant`/`RbacAction`
   over this same codec. `rmp_serde`'s `Serializer` implements
   `serialize_unit_variant` by writing the variant **name**, not its
   discriminant index (matching `serde_json`'s behaviour) — so the wire
   encoding is the same string tokens as the durable one.

**Compatibility requirement, precisely stated:** because both codecs encode a
fieldless enum variant by its **name string**, adding new variants to
`RbacAction` is purely additive in both formats — an old `"Read"`/`"Write"`/
`"Admin"` token decodes to the identically-named variant regardless of how many
new variants exist, and Rust's `match` exhaustiveness is unaffected because
**no code anywhere matches exhaustively over `RbacAction`** (verified: the only
"match action" patterns in the tree are on unrelated types — WAL plan actions,
SQL-classify conflict actions, compute-algorithm actions; grep for
`RbacAction::Read =>`/`RbacAction::Write =>`/`RbacAction::Admin =>` as match
arms returns zero hits outside the `AccessLevel ⇒ RbacAction` *construction*
site, which is a `match` on `AccessLevel`, not on `RbacAction`).
**Renaming or removing an existing variant would be format-breaking; adding
new ones is not.** This part is safe by construction and is the one piece of
this ticket that could be done today with zero risk to existing stored grants.

## 4. Proposed unified action set, and the mapping (item 4)

For completeness — this is the *target* vocabulary if/when the SQL-ACL
resource-model gap (§1.2, §6) is separately closed. It is **not implemented**
by this change.

| Unified action (proposed) | Existing `RbacAction` it replaces | `SqlPrivilege` it replaces | Notes |
|---|---|---|---|
| `Read` | `Read` | `Select` | Graph read and SQL `SELECT` are the same "observe" authority. |
| `Insert` | *(new)* | `Insert` | Currently folded into `Write` for graph resources. |
| `Update` | *(new)* | `Update` | Currently folded into `Write` for graph resources. |
| `Delete` | *(new)* | `Delete` | Currently folded into `Write` for graph resources. |
| `Write` | `Write` | *(kept as a coarse alias = Insert+Update+Delete for graph-shaped resources, which have no independent-privilege requirement today)* | Graph mutation call sites (`check_access(AccessLevel::Write, ...)`) keep mapping to one action; only SQL callers would ever request the finer three independently. |
| `Alter` | *(new, distinct from `Admin`)* | `Alter` | DDL (schema change) is narrower than full `Admin` — an owner able to `ALTER` their own table should not thereby gain `Admin` (cluster-wide RBAC administration, backup/restore, `RbacAdmin` itself). Collapsing `Alter` into `Admin` would be an **authority increase** for existing SQL table owners and must not happen. |
| `Admin` | `Admin` | *(no SQL equivalent — table owners never get this)* | Unchanged. |

**Backward-compatibility check for the 3 existing actions** (the explicit "an
existing `Write` grant must not silently gain or lose authority" requirement):
`Read → Read` and `Admin → Admin` are identity mappings — no change. `Write`
stays a single action meaning "may Insert+Update+Delete" for every *existing*
grant (graph resources) — none of today's stored `Write` grants must be
reinterpreted as only-Insert or only-Update; the proposal above keeps `Write`
exactly as broad as it is today by construction, and only *adds* independently-
grantable finer actions rather than *splitting* `Write`'s existing meaning.

## 5. What could break, and how a reviewer would detect it

- **If `Write` were split instead of extended** (e.g. removing `Write` and
  replacing every consumer with `Insert|Update|Delete` matching), every
  existing durable `Grant { action: Write }` row in `rbac.redb` would
  deserialize to... nothing — `Write` would no longer exist as a variant,
  and `serde` would hard-fail deserializing the whole policy blob at boot
  (`IncompleteState`/`Serde` error, `rbac_persist.rs:60-62`), which is a
  detectable, fail-closed break, not a silent one — but it would still be an
  outage for every tenant with a stored `Write` grant. **This is why the
  proposal in §4 keeps `Write` as a variant and only adds new ones next to
  it**, rather than removing/renaming it.
- **If the SQL-ACL rewiring were attempted regardless**, the specific risks
  are: (a) `sql_catalog_acl.rs`'s ownership concept has no home in `RbacPolicy`
  — a naive mapping (e.g. synthesizing a per-table role) would either lose
  the first-writer-wins race semantics or require inventing a new concept in
  `eg-core` under time pressure; (b) principal-direct grants (`grant(table,
  agent_id, privilege)`) would have to become role-mediated, which either
  requires minting one throwaway role per `(table, agent_id)` pair (blows up
  the role namespace and `expand_roles` cost) or a new `Grant` shape
  entirely; (c) the timing-equalization and no-existence-leak invariants
  (`sql_catalog_acl.rs:279-345`, with its own regression test) would need to
  be re-proven against whatever new `RbacPolicy::evaluate` path replaces
  `authorize()` — `evaluate()` today has no such test or documented property;
  (d) a bug in migrating the **existing** `__eg_sql_owners__`/
  `__eg_sql_grants__` redb rows into new `Grant`s would be a live security
  regression across every tenant's existing SQL tables, and there is no way
  to synthesize this migration's correctness from first principles — it needs
  its own test fixture built from real stored rows.
- **How a reviewer would detect a regression**: (1) a boot-time policy-load
  test that seeds `rbac.redb` with the *current* on-disk `Write` JSON encoding
  (byte-for-byte, captured from this branch before any change) and asserts it
  still decodes to `RbacAction::Write` and still authorizes exactly the
  `AccessLevel::{Read,Write}` calls it authorizes today; (2) for any SQL-ACL
  migration, a fixture built from a real `__eg_sql_owners__`/
  `__eg_sql_grants__` snapshot, asserting every `(table, principal)` pair's
  effective privilege set is identical before/after; (3) the existing
  `sql_catalog_acl.rs` denial-timing/no-existence-leak test must keep passing
  unmodified against whatever new code path replaces `authorize()`.

## 6. Verdict

**Not safe to implement full unification (rewiring `sql_catalog_acl.rs` onto
`RbacAction`/`RbacPolicy`) in this change. Stopping after Phase 1, per the
task's explicit "not yet, because X" allowance.**

Reasons, in order of weight:

1. **Resource-model mismatch, not just vocabulary.** `RbacPolicy`/
   `ResourceSelector` model graph resources (graph name / label / pattern) and
   role-mediated grants. The SQL ACL model needs per-table ownership and
   principal-direct grants, neither of which exists in `eg-core` today.
   Building them is new architecture, not a mechanical enum widen — it needs
   its own design and its own review, not a rider on this ticket.
2. **A live, security-critical data migration would be required** for the
   existing `__eg_sql_owners__`/`__eg_sql_grants__` redb rows, with no
   generic way to prove correctness other than a fixture built from real
   stored data (§5). Getting this wrong is a silent authority change for
   every tenant's existing SQL tables — exactly the failure mode this task's
   brief warns against ("an old grant decodes to a different effective
   authority than before ... stop and report instead").
3. **The sanctioned validation surface does not reach the code that would
   need to change.** `sql_catalog_acl.rs`, `wire/mod.rs`, and
   `handlers/rdf.rs` all live in the root `epistemic-graph` package, not in
   `eg-core` — the bounded validation command this task authorizes
   (`cargo check -p eg-core`) cannot compile-check them, and the task
   explicitly forbids `--all-targets`/the full suite. I cannot verify
   correctness of a change I am not able to compile or test, and per the
   task's own instruction set, that alone is reason enough not to make the
   change now.
4. **The `eg-core` baseline in this worktree does not currently compile**
   (unrelated to this ticket — `crates/eg-core/src/registry.rs:1375`,
   `no method named 'value' found for tuple '(&String, &GraphEntry)'`,
   introduced by a merge already on this branch, `git log -1 -- registry.rs`
   → `a2febc9 Merge branch 'ne/eg-cold-incarnation-fence'`). This is a
   pre-existing, out-of-scope break — confirmed via `git diff --stat` showing
   zero changes from this session — but it means even the narrow, safe part
   of this proposal (widening the `RbacAction` enum, §3/§4) cannot currently
   be *verified* against the one crate this task's validation command covers,
   until that unrelated breakage is fixed by whichever lane owns it.

**What would make this safely doable, as a separately-scoped follow-up:**
(a) fix the `eg-core` baseline break so `-p eg-core` actually compiles; (b) as
its own reviewed step, widen `RbacAction` per §4 (safe by construction, §3) —
purely additive, zero consumers changed, verified with the fixture in §5(1);
(c) as a separate, larger design (its own ledger item), add a table-shaped
`ResourceSelector` variant and an ownership primitive to `eg-core`, write the
`__eg_sql_owners__`/`__eg_sql_grants__` → `Grant` migration with a
real-data fixture, then rewire `sql_catalog_acl.rs`'s consumers in
`wire/mod.rs`/`handlers/rdf.rs` atomically — validated with the root
package's own full check/test surface, not `-p eg-core`.

No code was changed by this document. `git diff --stat` in the worktree is
empty.
