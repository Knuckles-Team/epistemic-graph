<section class="site-hero" aria-labelledby="epistemic-graph-title">
  <p class="site-hero__eyebrow">The knowledge engine</p>
  <h1 class="site-hero__title" id="epistemic-graph-title">Make connected knowledge durable, explainable, and computable.</h1>
  <p class="site-hero__summary">
    Epistemic Graph unifies graph, SQL, RDF/OWL, vectors, time, evidence, and
    multimodal data behind one Rust-native planner and transaction boundary.
  </p>
  <div class="site-hero__actions">
    <a class="md-button md-button--primary" href="get-started/">Run it locally</a>
    <a class="md-button" href="capabilities/">Inspect capabilities</a>
  </div>
</section>

<div class="site-card-grid">
  <article class="site-card">
    <p class="site-card__title">Build on it</p>
    <p class="site-card__body">Use the Python client, UQL, SQL, SPARQL, Cypher, GraphQL, Bolt, and stream interfaces against the same governed state.</p>
    <a href="interfaces/">Explore interfaces</a>
  </article>
  <article class="site-card">
    <p class="site-card__title">Understand it</p>
    <p class="site-card__body">Follow requests from authenticated ingress through planning, reasoning, computation, and commit-before-ack storage.</p>
    <a href="architecture/">Read the architecture</a>
  </article>
  <article class="site-card">
    <p class="site-card__title">Operate it</p>
    <p class="site-card__body">Run a durable single node or a replicated cluster with explicit identity, policy, persistence, and observability.</p>
    <a href="standalone_deployment/">Deploy the engine</a>
  </article>
</div>

## One authority across every data shape

<figure class="site-card">
  <img src="assets/engine-architecture.svg" alt="Authenticated interfaces enter the unified Epistemic Graph planner, compose five data and compute domains, and commit through one authoritative durable store.">
  <figcaption>Interfaces differ. Authority, planning, evidence, and commit semantics do not.</figcaption>
</figure>

The main build provides the connected data, semantic reasoning, analytical,
temporal, and multimodal surfaces shown above. Compatibility is published per
operation: the [capability matrix](capabilities.md) explains supported behavior,
while the [generated method ledger](capabilities.generated.md) records exact
authority, durability, audit, CDC, and transaction properties.

!!! info "Capability truth is part of the product"
    Status is generated from the same method contracts the engine serves. Check
    [current status](status.md) and the [capability matrix](capabilities.md)
    before selecting an interface or deployment shape.

## Where this engine fits

<figure class="site-card">
  <img src="assets/runtime-architecture.svg" alt="Clients enter through GraphOS, agent-utilities executes agents and workflows, Epistemic Graph owns durable knowledge, and connector packages synchronize external systems.">
  <figcaption>Epistemic Graph is the durable authority at the base of the shared runtime.</figcaption>
</figure>

<div class="site-ownership">
  <div class="site-ownership__grid">
    <div class="site-ownership__item">
      <div class="site-ownership__label">This repository owns</div>
      <div class="site-ownership__value">Durable data, graph and multimodal computation, semantic reasoning, query planning, schema enforcement, evidence, and engine authorization.</div>
    </div>
    <div class="site-ownership__item">
      <div class="site-ownership__label">It does not own</div>
      <div class="site-ownership__value">Agent orchestration, public gateway policy, browser experience, source credentials, or vendor API execution.</div>
    </div>
  </div>
</div>

<ol class="site-flow">
  <li class="site-flow__step">
    <span class="site-flow__title">Enter through GraphOS</span>
    <span class="site-flow__body">MCP, REST, A2A, identity, policy, and the hosted WebUI share one public door.</span>
  </li>
  <li class="site-flow__step">
    <span class="site-flow__title">Execute with agent-utilities</span>
    <span class="site-flow__body">Agents, workflows, skills, evaluation, and control-plane decisions run above the database.</span>
  </li>
  <li class="site-flow__step">
    <span class="site-flow__title">Commit in Epistemic Graph</span>
    <span class="site-flow__body">State, schema, evidence, provenance, and outcomes become durable under one transaction boundary.</span>
  </li>
  <li class="site-flow__step">
    <span class="site-flow__title">Synchronize through connectors</span>
    <span class="site-flow__body">SDK-based connectors exchange governed records with external source systems.</span>
  </li>
</ol>

## Choose an interface

| Goal | Start here |
|---|---|
| Add or query graph records from Python | [Python and native clients](interfaces/clients.md) |
| Query connected knowledge | [UQL](uql.md), [Cypher](interfaces/cypher.md), or [GraphQL](interfaces/graphql.md) |
| Work with RDF and ontologies | [RDF, SPARQL, OWL, and SHACL](interfaces/ontology.md) |
| Run relational analytics | [SQL and PostgreSQL wire](interfaces/sql.md) |
| Search vectors, text, and memory | [Memory interfaces](interfaces/memory.md) and [vector search](interfaces/vector.md) |
| Ingest documents and media | [Governed modality serving](architecture/modality_serving.md) |
| Operate a durable service | [Standalone deployment](standalone_deployment.md) and [operations](operations/runbook.md) |

## Run your first graph

The release wheel includes the in-process engine binding, so the shortest path
needs no server, port, certificate, or background process:

```bash
python -m pip install epistemic-graph
python - <<'PY'
import msgpack
from epistemic_graph.engine import Engine

engine = Engine(persist_dir=":memory:")
engine.create_graph("demo")
engine.add_node("demo", "node:hello", msgpack.packb({"kind": "Greeting"}, use_bin_type=True))
print(engine.has_node("demo", "node:hello"), engine.node_count("demo"))
PY
```

Expected output: `True 1`. This explicit `:memory:` mode is ephemeral. Continue
to the [local guide](get-started.md) for the annotated example, or go directly
to [Deploy](standalone_deployment.md) for durable service, TLS, and cluster
configuration.
