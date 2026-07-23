# Verified Request Authority

The graph engine accepts exactly one verified request context: the `eg2.`
authority envelope. It binds the request id, graph, method, body
digest, timestamp, nonce, idempotency key, effective ACL agent, tenant,
audience, policy version, roles, scopes, delegation chain, and trace context.
The server verifies the envelope before dispatch and rejects replay, deployment
policy mismatch, and conflicting caller identity.

## Capability contract with Graph-OS

The capability ledger remains authoritative for every method's canonical
`authz_action` and `mutates` classification. Exact scopes and domain wildcards
are evaluated first. Graph-OS may also issue three stable aggregate scopes:

| Aggregate scope | Ledger interpretation |
|---|---|
| `kg:read` | Non-mutating, non-administrative methods |
| `kg:write` | Non-administrative mutations and their precondition reads |
| `kg:admin` | Aggregate administrative access |

`kg:write` cannot authorize `admin:*`, `security:*`, `*:admin`, or
`*:control`. Administrative methods still pass the engine's isolation/RBAC
admin-capability gate; the aggregate scope does not bypass tenant, graph, or
policy enforcement. A direct client may use exact scopes such as `work:write`
without receiving unrelated capabilities.

## Privacy and provenance

The raw authenticated principal remains request-local. Durable mutation
provenance receives a stable SHA-256 subject id instead. The verified context
does not carry or persist local filesystem paths, workstation user names, or
personal display names.

## Deployment alignment

Secure deployments must align these values with the issuing Graph-OS service:

- `GRAPH_SERVICE_AUTH_SECRET`, loaded from the same runtime secret authority;
- `EPISTEMIC_GRAPH_AUDIENCE` and the issuer's configured audience;
- `EPISTEMIC_GRAPH_TENANT` and the routed tenant policy;
- `EPISTEMIC_GRAPH_POLICY_VERSION` and the active authorization revision;
- `EPISTEMIC_GRAPH_SIGNER_KEYS_JSON`, loaded from a runtime secret provider;
- `GRAPH_SERVICE_PERSIST_DIR`, which owns the durable replay ledger;
- `EPISTEMIC_GRAPH_ENVELOPE_SKEW_SECS`, the timestamp and replay-retention
  horizon.

All values are mandatory except the skew override. The server also requires a
build containing the `security` feature. Graph operations receive identity only
from verified context, never unsigned request fields. A routable native TCP
listener requires TLS, while every auxiliary listener remains loopback-only.

## Fresh durable policy bootstrap

A fresh durable store begins with no ambient graph or administrative authority.
It admits only one narrowly shaped bootstrap mutation in `__commons__`:

- `RegisterIdentity` registers the verified principal/effective agent itself;
- the requested role is `System`, with empty teams and roles;
- the verified envelope has no delegation and exactly one scope,
  `security:bootstrap`;
- the detached registration signature verifies against
  `EPISTEMIC_GRAPH_SIGNER_KEYS_JSON`, and the signer id equals the verified
  principal.

After that first identity rule is durable, the bootstrap predicate is false.
Every graph read/write and every later identity, RBAC, cluster, or backup action
must satisfy the normal capability-ledger scope plus durable graph/admin policy.

## Row-level authority

Served row-level security is always default-deny. Unowned, undecodable, or
untagged rows are invisible unless policy grants ownership/access or the row is
explicitly public. There is no runtime switch to make served reads permissive.

The companion Graph-OS contract is documented as **Graph Authority
Convergence** in agent-utilities.
