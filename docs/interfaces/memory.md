# Agent-native memory interface

epistemic-graph carries a native **agent-memory** substrate in `eg-core`: a multi-level summary/abstraction
ladder, episodic→semantic consolidation, importance decay + reinforcement, hierarchical (LeanRAG-style)
retrieval, episodic trajectory memory for policy learning, and a scene-graph world model. These are
**deterministic engine primitives** the agent-utilities memory loop schedules — the *content* work (LLM
distillation/summarization) stays in agent-utilities; the engine provides the fast, durable, provenance-
preserving operations. (Localized maintenance, not global reorganization, per arXiv 2606.24775.)

> Status snapshot: the summary tier (EG-220), consolidation (EG-221), maintenance decay/reinforcement
> (EG-222), LeanRAG retrieval (EG-195), trajectory memory (EG-099), and the scene-graph world model
> (EG-087) are shipped — and Program B **exposes them over the wire** (additive `Method`s + dispatch + WAL
> replay), so AU/MCP drive them remotely rather than in-process only (EG-318). See the
> [capability matrix](../capabilities.md).

## Hierarchical summary tier (EG-220)

A native multi-level memory abstraction ladder: `:SummaryNode` graph nodes roll up a set of source
memories with a **level** + provenance links (`SUMMARIZES`/`CONSOLIDATES`) to their children. A
`summarize`/`rollup` primitive materializes a higher level from a cluster of lower-level memories. The
engine owns the structure + provenance; the LLM distill content is supplied by agent-utilities.

## Episodic → semantic consolidation (EG-221)

A localized consolidation op promotes a cluster of episodic memory nodes into a consolidated **semantic**
node — merging properties, redirecting edges, preserving provenance and bitemporal `tx_from`/`tx_to`,
importance-weighted. Deterministic (caller-supplied `now`); no global reindex — the "localized maintenance
beats global reorganization" finding.

## Maintenance — decay + reinforcement (EG-222)

Each memory node carries **importance** + **access-count** + **last-access**:

- `reinforce(id, now)` — bump importance/recency on retrieval;
- `decay(now, half_life)` — time-based importance decay;
- `evict_below(threshold)` / `forget` — prune low-value memories locally.

Deterministic (caller-supplied `now`), so it is Raft/WAL-safe. This is the substrate the agent-utilities
loop schedules (and it composes with the engine's Ebbinghaus fact-decay knobs
`GRAPH_SERVICE_DECAY_HALF_LIFE`/`…_DECAY_FLOOR`/`…_DECAY_INTERVAL`).

## Hierarchical retrieval — LeanRAG (EG-195)

Structured hierarchical retrieval over the summary tier that beats flat top-k RAG on redundancy/coverage:
vector-retrieve at the **summary/abstraction** level (eg-ann), then drill **down** through
`SUMMARIZES`/`CONSOLIDATES` provenance edges to the specific supporting memories, assembling a concise
multi-level context (bottom-up semantic aggregation + top-down guided traversal). An eg-plan retrieval
module reusing eg-ann + the graph (not a wire Op).

## Trajectory memory (EG-099)

Episodic trajectory memory for agents/robotics: an ordered `:Trajectory` of
`:Step{ state_ref, action, reward, next_state_ref, t }` linked as a temporal chain, with:

- append + query (by trajectory, by reward, windowed returns);
- discounted-return computation;
- best / worst-trajectory retrieval.

The substrate for policy learning + replay; composes with the scene states (EG-087) and the memory tiers
(EG-220/221).

## Driving it over the wire (EG-318)

The memory / scene / trajectory primitives were originally **in-process** eg-core library calls. Program B
adds an additive **wire surface** so agent-utilities / MCP drive them **remotely** over the same
MessagePack transport as any other op: new `Method`s (CreateSummary / Consolidate / Maintain / SceneObject /
Trajectory operations), their dispatch handlers, and **WAL replay** for the new mutations (so they are
durable + replicated + crash-recoverable like every other write). This is what lets the agent-utilities
memory loop schedule summarize / consolidate / decay, scene-object, and trajectory ops against a **remote**
engine — no in-process embedding required.

## Scene-graph / 3D world model (EG-087)

A native scene-graph modality: `:SceneObject` nodes carrying a 3D pose (translation + quaternion rotation +
scale = a transform), a parent/child transform hierarchy (world-transform composition down the tree),
spatial relationships (`on`/`in`/`near`/`supports`), and bounding volumes — the substrate for
robotics / AR / urban-3D world models. Composes (read-only) with the [GIS](gis.md) geo types and the tensor
store.

!!! note "Deferred — memory → weights distillation (🗺)"
    Distilling consolidated agent-memory (EG-220/221) into **model weights** (a fine-tune / LoRA export),
    beyond the shipped retrieval-time context assembly (EG-195), is designed but not started. See the
    [forward roadmap](../roadmap.md).

---
*These primitives live in `eg-core` — durable, replicated engine state. The agent-utilities memory loop
drives the *policy* (when to summarize / consolidate / decay); the engine guarantees the *mechanics* are
fast, deterministic, and provenance-preserving.*

**See also:** [Capabilities matrix](../capabilities.md) · [Ontology lifecycle](ontology.md) ·
[Vector / ANN](vector.md) · [GIS / Spatial](gis.md) · [Client Drivers](clients.md).
