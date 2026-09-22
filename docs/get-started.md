# Run Epistemic Graph locally

The release wheel contains the Python package, the Rust server binary, and the
in-process engine binding. Start with the embedded binding when you want to
evaluate the graph API without configuring a service.

## Install and create a graph

```bash
python -m pip install epistemic-graph
python - <<'PY'
import msgpack
from epistemic_graph.engine import Engine

engine = Engine(persist_dir=":memory:")
engine.create_graph("demo")
engine.add_node(
    "demo",
    "node:alice",
    msgpack.packb({"kind": "Person", "name": "Alice"}, use_bin_type=True),
)

properties = msgpack.unpackb(
    engine.get_node_properties("demo", "node:alice"),
    raw=False,
)
print(properties)
print("nodes:", engine.node_count("demo"))
PY
```

The command prints:

```text
{'kind': 'Person', 'name': 'Alice'}
nodes: 1
```

`persist_dir=":memory:"` is an explicit ephemeral mode: the process owns the
engine and its contents disappear when the process exits. It is the smallest
way to verify the installed engine and learn the local API.

## Choose the runtime shape

| Need | Runtime shape | Next page |
|---|---|---|
| Explore the native API in one Python process | Embedded, in memory | This page |
| Run one durable database on a host | Server over a local Unix socket | [Standalone deployment](standalone_deployment.md) |
| Connect applications across hosts | Server over authenticated TLS | [Deployment reference](deployment.md) |
| Replicate and partition durable state | Cluster build | [Cluster deployment](architecture/cluster_deployment.md) |

The service paths require explicit persistence, identity, policy, and request
authority. Those settings belong in deployment configuration, so the local
example stays short without weakening the network boundary.

## Continue

- [Choose an interface](interfaces/index.md).
- [Understand the transaction and compute path](architecture/index.md).
- [Inspect current capability coverage](capabilities.md).
- [Deploy a durable service](standalone_deployment.md).
