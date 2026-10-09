# AIF Argumentation (I-nodes/S-nodes → Dung acceptability)

> CONCEPT:AU-KG.epistemic.aif. Layers the **Argument Interchange Format**
> (AIF — Rahwan & Reed, "The Argument Interchange Format"; AIFdb/arg-tech.org)
> as a typed interchange vocabulary over the engine's EXISTING argumentation
> machinery. It does not add a second argumentation engine.

## Why AIF, and why not a new engine

`epistemic-graph`'s `eg-epistemic` crate already does argumentation natively:
Claim/Evidence/`BeliefState` confidence propagation, plus a paraconsistent,
justification-based TMS with genuine Dung abstract-argumentation semantics
(grounded/preferred/stable extensions) reachable standalone via
`Method::ResolveConflict` (see `epistemic-graph/AGENTS.md`, and
`agent_utilities/mcp/tools/epistemic_tools.py`'s `graph_epistemic` tool).

AIF is the *interchange formalization* of exactly that argumentation model —
a community-standard graph shape for exchanging arguments: an **I-node**
(information node) is a claim; an **RA-node** (Rule of inference Application)
is a support edge reified into a scheme application; a **CA-node** (Conflict
Application) is an attack/contradiction reified the same way; a **PA-node**
(Preference Application) states one side of a conflict is preferred. AIF+
adds **TA** (Transition Application, dialogue state) and **YA**
(Illocutionary Application, locution → proposition).

So the mapping is direct:

| AIF concept | This KG / engine concept |
|---|---|
| I-node | `:Belief` (a claim-with-confidence) — Claim/Evidence/`BeliefState` |
| RA-node (premises → conclusion) | A `SUPPORTS` edge from each premise to the conclusion |
| CA-node (premises → conclusion) | An `ATTACKS` edge from each premise to the conclusion |
| PA-node (premises → preferred conclusion) | A preference that discounts one side of a *mutual* CA-node conflict |
| Scheme (RA/CA/PA fulfils) | A named `:AIFScheme` individual (`:aifFulfills`) |
| Grounded/preferred/stable acceptability | `eg-epistemic`'s `Method::ResolveConflict` — unchanged, reused as-is |

`agent_utilities/knowledge_graph/argumentation/aif.py` builds the AIF
argument-map objects and the `to_dung()` projection; it never recomputes
grounded/preferred/stable itself.

## Ontology

`ontology_argumentation.ttl` (imported by the canonical `ontology.ttl`)
declares the Upper Ontology (node hierarchy) and Forms Ontology (scheme
templates):

<div class="admonition architecture" markdown>
<p class="admonition-title">AIF class hierarchy</p>

`AIFNode` has two subclasses: `AIFInformationNode` (fields: `aifNodeText`,
also a subclass of `Belief` with a `confidence` field) and `AIFSchemeNode`
(field: `aifFulfills: AIFScheme`, which fulfills an `AIFScheme` instance).
`AIFSchemeNode` has five subclasses: `AIFRuleApplicationNode`,
`AIFConflictApplicationNode`, `AIFPreferenceApplicationNode`,
`AIFTransitionApplicationNode`, `AIFIllocutionaryApplicationNode`.
`AIFScheme` (field: `aifSchemeName`) has three subclasses:
`AIFInferenceScheme`, `AIFConflictScheme`, `AIFPreferenceScheme`.

</div>

Edges (`:aifHasPremise` / `:aifHasConclusion`, with inverses
`:aifIsPremiseOf` / `:aifIsConclusionOf`) are uniform across every S-node
kind — the Upper Ontology's edge model does not vary by scheme type: an edge
INTO an S-node is one of its premises; an edge OUT OF an S-node is its
(single) conclusion. `shapes/argumentation.shapes.ttl` enforces the arity
each kind requires (RA/CA: ≥1 premise + exactly 1 conclusion; PA: ≥2
premises + exactly 1 conclusion; I-node: non-empty text).

## Example argument graph

The rain/sprinkler textbook example — `i3` conflicts with `i1` via `ca1`;
`i1` supports `i2` via `ra1`:

<div class="admonition architecture" markdown>
<p class="admonition-title">Example argument graph</p>

`i1` ("It is raining") is a premise of `ra1` (RA: Default Inference), whose
conclusion is `i2` ("The ground is wet"). `i3` ("The sprinkler was on") is
a premise of `ca1` (CA: Default Conflict), whose conclusion is `i1` — i.e.
`i3` conflicts with `i1`, and `i1` supports `i2`.

</div>

On import, `i1`/`i2`/`i3` become `:Belief`-typed nodes; `ra1`/`ca1` become
`:AIFRuleApplicationNode`/`:AIFConflictApplicationNode` nodes; AND the engine
also gets a direct `i1 -SUPPORTS-> i2` edge and a direct `i3 -ATTACKS-> i1`
edge — the exact topology `Method::ResolveConflict` reads.

## End-to-end path: JSON → graph → Dung acceptability

<div class="admonition architecture" markdown>
<p class="admonition-title">End-to-end path: JSON → graph → Dung acceptability</p>

**Interchange** (`aif.py`, pure, no engine): AIFdb-shaped JSON
(`{nodes, edges}`) goes through `from_aifdb_json()` into an `ArgumentMap`
(`AIFNode`/`AIFEdge`), which `validate_argument_map()` checks against the
same arity rules as the SHACL shapes. A valid map also converts via
`to_dung()` into a `DungProjection` (arguments, attacks, supports,
preferences, dropped_attacks).

**Write** (`import_argument_map()`, the one connector write path): a valid
map flows through `native_authority()` → `ingest_graph_slice()` into one
atomic `ChangeEnvelope` transaction, which writes into the **Knowledge
Graph**: `:Belief` nodes (I-nodes), AIF S-node types (RA/CA/PA/TA/YA),
`aifHasPremise`/`aifHasConclusion` edges, and derived SUPPORTS/ATTACKS
edges.

**Evaluate** (`graph_argument(action="evaluate")`): the `DungProjection`'s
arguments go through `engine_tools._dispatch()` (the same dispatcher
`graph_epistemic` uses) into the **engine** (`eg-epistemic`, Rust,
unchanged) — `Method::ResolveConflict` (grounded/preferred/stable), which
also reads the derived SUPPORTS/ATTACKS belief/attack topology from the KG
— producing surviving/defeated/undecided results plus extension sets.

</div>

`export_argument_map()` (graph → AIF JSON) is the read-side mirror: a
best-effort, tag-filtered node+edge scan (mirrors the established
`ops_causal_graph.load_ops_causal_neighborhood` idiom) reconstructing an
`ArgumentMap`, rendered back to AIFdb JSON via `to_aifdb_json()`.

## Surfaces

- **MCP tool:** `graph_argument` (`agent_utilities/mcp/tools/argument_tools.py`)
  — actions `import_aif` / `export_aif` / `evaluate` / `add_scheme`.
- **REST twin:** `POST /graph/argument` — the generic `ACTION_TOOL_ROUTES`
  factory in `kg_server._build_server` dispatches through the SAME
  `_execute_tool` core every other action-routed tool uses; no bespoke
  handler.
- **Skill:** `graph-runtime-and-governance` (`agent_utilities/skills/graph-runtime-and-governance/SKILL.md`,
  "Argument evaluation (AIF)" section).

## What this deliberately does NOT do

- No second argumentation solver. `to_dung()` is a pure, structural
  projection (arguments + attacks + preferences); acceptability is always
  computed by `eg-epistemic`'s `Method::ResolveConflict`.
- No parallel claim store. Every I-node is written and read through the
  SAME `:Belief`/`ChangeEnvelope` path every other claim in the KG uses.
- PA-node preference filtering only ever resolves a *mutual* (symmetric)
  CA-node conflict — the classic motivation for preference-based
  argumentation (Amgoud & Cayrol, 2002). A one-directional attack is left
  exactly as the CA-node declared it.
