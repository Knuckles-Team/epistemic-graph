# Epistemic Graph

epistemic-graph is a durable, Rust-native database that unifies graph, vector, SQL, RDF/OWL, and
time-series behind one engine and one query planner. Use it standalone, or as the storage and
reasoning engine behind agent-utilities. Every capability is tracked operation-by-operation and
honestly marked — see what's live before you build on it.

## Quick start

=== "Docker"

    ```bash
    : "${CONTAINER_DATA_DIR:?set to the image data directory}"
    : "${TLS_CERT_FILE:?set to a host PEM certificate file}"
    : "${TLS_KEY_FILE:?set to a host PEM private key file}"
    docker volume create eg-data
    docker run -d --name epistemic-graph \
      -e GRAPH_SERVICE_AUTH_SECRET \
      -e EPISTEMIC_GRAPH_AUDIENCE=epistemic-graph \
      -e EPISTEMIC_GRAPH_TENANT=tenant:default \
      -e EPISTEMIC_GRAPH_POLICY_VERSION=policy:initial \
      -e EPISTEMIC_GRAPH_SIGNER_KEYS_JSON \
      -e GRAPH_SERVICE_PERSIST_DIR="${CONTAINER_DATA_DIR}" \
      -e GRAPH_SERVICE_TCP_ADDR=0.0.0.0:9100 \
      -e GRAPH_SERVICE_TLS_CERT=/run/secrets/server.crt \
      -e GRAPH_SERVICE_TLS_KEY=/run/secrets/server.key \
      -p 9100:9100 \
      --mount type=bind,src="${TLS_CERT_FILE}",dst=/run/secrets/server.crt,readonly \
      --mount type=bind,src="${TLS_KEY_FILE}",dst=/run/secrets/server.key,readonly \
      -v eg-data:"${CONTAINER_DATA_DIR}" \
      <registry>/epistemic-graph:<tag>
    ```

    Populate `GRAPH_SERVICE_AUTH_SECRET` and `EPISTEMIC_GRAPH_SIGNER_KEYS_JSON` from a runtime
    secret provider before starting the container. The server accepts only `eg2.` request
    envelopes and requires the audience, tenant, policy revision, durable replay state, and
    trusted signer registry. Routable native TCP always uses TLS/mTLS (`GRAPH_SERVICE_TLS_CERT`,
    `_KEY`, optional `_CLIENT_CA`). Auxiliary listeners, including database-protocol and metrics
    listeners, are loopback-only; expose them through a co-located authenticated TLS gateway when
    needed. Full recipes (compose, HA cluster, prebuilt wheels): [deployment guide](deployment.md).

=== "Binary"

    ```bash
    # Read all secrets and policy values from deployment configuration.
    : "${GRAPH_SERVICE_AUTH_SECRET:?required}"
    : "${EPISTEMIC_GRAPH_SIGNER_KEYS_JSON:?required}"
    : "${GRAPH_SERVICE_PERSIST_DIR:?required}"
    export EPISTEMIC_GRAPH_AUDIENCE=epistemic-graph
    export EPISTEMIC_GRAPH_TENANT=tenant:default
    export EPISTEMIC_GRAPH_POLICY_VERSION=policy:initial
    epistemic-graph-server
    ```

=== "Python client"

    ```bash
    pip install epistemic-graph
    ```

    ```python
    from epistemic_graph import SyncEpistemicGraphClient

    context = {
        "principal": "service:client",
        "tenant": "tenant:default",
        "audience": "epistemic-graph",
        "agent_id": "service:client",
        "roles": ["graph-client"],
        "scopes": ["kg:read", "kg:write"],
        "policy_version": "policy:initial",
        "delegation": [],
    }
    with SyncEpistemicGraphClient.connect(verified_context=context) as graph:
        graph.nodes.add("node:a", {"node_type": "coordinator"})
        graph.nodes.add("node:b", {"node_type": "worker"})
        graph.edges.add("node:a", "node:b", {"weight": 1.5})
        print("Order:", graph.graph.topological_sort())
    ```

    The published wheel already contains the complete main Rust build **and** all runtime Python
    helpers (OWL/SPARQL, LMCache HTTP acceleration, and numeric interoperability). More entry
    points — the remote → shared-local → autostart resolver and the embedded in-process handle —
    are in [engine modes](engine_modes.md).

!!! note "Honesty first"
    Every capability is tracked operation-by-operation in the
    **[capabilities & parity matrix](capabilities.md)**; per-method authority, durability, audit,
    CDC, and transaction facts come from the **[generated capability ledger](capabilities.generated.md)**.
    External hardware and multi-host campaigns are release-certification evidence, not
    unimplemented source. See what's live before you build on it.

## How it's organized

| | |
|---|---|
| **Query it** | [Interfaces](interfaces/index.md) — one guide per wire protocol: SQL, SPARQL, Cypher, GraphQL, vector, time-series, and more. |
| **Understand internals** | [Architecture](architecture/index.md) — the commit model, analytics/reasoning plane, distribution & scaling, and hardening. |
| **Operate it** | [Deployment](deployment.md) and [Operations runbook](operations/runbook.md) — standalone, Docker, and HA-cluster recipes; day-2 procedures. |
| **Reference** | [Concept registry](concepts.md) · [UQL](uql.md) · [environment variables](deployment.md#configuration-reference). |
| **Status** | [docs/status.md](status.md) — the generated Codex/status page: what's live, in progress, or roadmap, by pillar. |

Go deeper: the engine's guiding design principle is
**[North Star: Seamless](north_star.md)** — every cross-modal read/write path is implemented at
*every* wire surface, never merely flagged at the one it was first built for.
