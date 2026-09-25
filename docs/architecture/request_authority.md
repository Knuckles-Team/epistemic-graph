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

The replay ledger keeps accepted read-request nonces in a bounded in-memory
window and makes only a high-water envelope timestamp durable, once per clock
second, before the request dispatches. A graceful shutdown (SIGTERM/SIGINT)
seals the window into a one-shot handoff that the next start consumes, so a
clean restart refuses only genuine replays. After a crash there is no handoff:
every envelope signed at or before the high-water is refused as a possible
replay, so a client whose request raced the crash re-signs it; signed mutations
consume their nonce inside the mutation kernel instead.

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

## Control-lease kind allowlist

`lease:write` lets a principal issue and transition native control leases of
any kind. A deputy executor that needs the scope for one feature must not gain
every other lease kind with it. That includes kinds an approver or the
two-person elevation flow owns, such as `rbac.elevation`.

`EPISTEMIC_GRAPH_CONTROL_LEASE_KIND_POLICY_JSON` narrows this per verified
`agent_id`:

```json
{"service:graph-os": ["action.approval", "finance.order-proposal"]}
```

The policy works as follows:

- A principal the policy names may issue, and transition, only leases of the
  kinds listed for it. Any other kind is refused with `ACCESS_DENIED`, and an
  empty list refuses every kind.
- A transition is checked against the stored lease's kind, which is immutable
  after issue.
- A principal the policy does not name is unaffected.
- No principal name is built into the server; the policy is deploy
  configuration.
- The policy is read once, when the process first handles a control-lease
  write.
- A malformed value refuses every control-lease write, so the restriction
  fails closed instead of being dropped.

The check runs in the control-lease write handler, before the WorkItem
kernel. It sits after the capability-scope check, the carrier-tenant binding
and, under raft, the placement-leader check. It therefore narrows
`lease:write` and never widens it.

## Row-level authority

Served row-level security is always default-deny. Unowned, undecodable, or
untagged rows are invisible unless policy grants ownership/access or the row is
explicitly public. There is no runtime switch to make served reads permissive.

The companion Graph-OS contract is documented as **Graph Authority
Convergence** in agent-utilities, and the exact claim-key-level contract
(required/optional field table, per-surface carrier status including this
file's own `authenticated_iceberg_bearer` tenant-match boundary, and the
consumer handoff for lanes minting caller identity) is frozen as
**Verified Identity Carrier Contract (GOC-15)**, also in agent-utilities
(`docs/architecture/verified-identity-carrier-contract.md`). Both should be
read before adding a new auxiliary surface's identity binding (SPARQL,
federation, observability) — the Iceberg-bearer tenant-match pattern above is
the template such a surface should follow, not a new mechanism.
