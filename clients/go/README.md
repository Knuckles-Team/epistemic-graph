# epistemic-graph — thin Go client (CONCEPT:EG-KG.ingest.broker-streams-namespaces)

A **deliberately thin** Go client for the Program-B engine `Method`s that had no client
surface. It is **not a full SDK** — the canonical, full-featured client is the Python one
in [`epistemic_graph/client.py`](../../epistemic_graph/client.py). This binding covers
only the new wire ops from B1.7:

| Domain | Methods | Engine concept |
|--------|---------|----------------|
| Broker admin | `DeclareExchange` `DeleteExchange` `DeclareQueue` `BindQueue` `UnbindQueue` | EG-275/276/277/278 |
| Broker publish | `Publish` `PublishEx` `PublishConfirmed` `PublishIdempotent` | EG-275/279/284/314 |
| Broker consume | `BrokerConsume` `BrokerAck` `BrokerReject` `BrokerAckTag` `BrokerNackTag` `BrokerRenewTag` `SweepExpired` | EG-KG.compute.groups-qos-prefetch-honoring/276/284 |
| Streams | `StreamDeclare` `StreamPublish` `StreamRead` `StreamTrim` `StreamCommitOffset` `StreamCommittedOffset` | EG-283 |
| RBAC admin | `RbacAddRole` `RbacRemoveRole` `RbacAddGrant` `RbacRemoveGrant` `RbacList` | EG-KG.compute.feature |
| Identity security | `RegisterIdentity` `BootstrapSystemIdentity` `ApplyMultisigMutation` | Signed current-context operations |
| Ops | `Backup(destination, label)` `Restore(source, targetShards)` | EG-090 |
| NL→query | `NlQuery` | EG-080 |

> **Generated-from-the-Method-list.** Every method maps 1:1 to a Rust `Method`
> variant in [`crates/eg-types/src/protocol.rs`](../../crates/eg-types/src/protocol.rs),
> using the exact serde field names that enum destructures. The full graph / vector /
> RDF / SQL API is intentionally omitted — use the Python client for that.

## Wire contract

Same framed-MessagePack transport as the Python client:

- **Framing:** 4-byte big-endian length prefix + a msgpack request
  `{ id, graph, auth_token, method, params }`.
- **Auth:** every request carries an `eg2.` signed request-context envelope. The
  MAC binds the request id, graph, canonical method body hash, explicit authority
  claims, timestamp, nonce, and idempotency key. `Dial` rejects an empty secret,
  missing claims, empty or duplicate list entries, malformed delegation, and
  implicitly omitted `Roles`, `Scopes`, or `Delegation` slices before connecting.
- **Correlation:** this client holds ONE connection and serializes each round-trip under
  a mutex, so responses are read in order (no out-of-order demux needed — unlike the
  pipelined Python client). Wrap concurrent callers accordingly.
- **Compact results:** a top-level msgpack `bin` result is a second `Raw` layer and is
  decoded once more.

## Pi-contract

Thin by design: one pure-Go dependency (`github.com/vmihailenco/msgpack/v5`) for framing;
stdlib `net` (UDS/TCP) + `crypto/hmac`. No cgo, no heavy SDK. This is a client — nothing
here belongs in the `pi` engine build.

## Usage

```go
package main

import (
	"fmt"
	"os"
	"time"

	epg "github.com/epistemic-graph/clients/go"
)

func main() {
	c, err := epg.Dial(epg.Options{
		Network:    "unix",
		Address:    os.Getenv("GRAPH_SERVICE_SOCKET"),
		AuthSecret: os.Getenv("GRAPH_SERVICE_AUTH_SECRET"),
		Graph:      "agent:planner",
		Context: &epg.RequestContextClaims{
			Principal: "service:graph-client", Tenant: "tenant:default",
			Audience: "epistemic-graph", AgentID: "service:graph-client",
			Roles: []string{"graph-client"},
			Scopes: []string{"graph:read", "graph:write"},
			PolicyVersion: "policy:current", Delegation: []string{},
		},
	})
	if err != nil {
		panic(err)
	}
	defer c.Close()

	// Broker
	_, _ = c.DeclareExchange("events", "topic")
	_, _ = c.DeclareQueue("orders", epg.QueuePolicy{})
	_, _ = c.BindQueue("events", "orders", "user.*")
	_, _ = c.Publish("events", "user.signup", []byte("payload"))

	now := uint64(time.Now().UnixMilli())
	msg, _ := c.BrokerConsume("orders", "g", "c1", now, 0, 0)
	fmt.Println(msg)

	// Streams
	_, _ = c.StreamDeclare("audit", nil, nil)
	_, _ = c.StreamPublish("audit", []byte("evt"), now)
	back, _ := c.StreamRead("audit", 0, 10)
	fmt.Println(back)

	// RBAC — ResourceSelector is "All" or a single-key map {"Graph"|"Label"|"Pattern": s}
	_, _ = c.RbacAddRole("reader", nil)
	_, _ = c.RbacAddGrant("reader", map[string]any{"Graph": "agent:planner"}, "Read", "Allow")
	policy, _ := c.RbacList()
	fmt.Println(policy)

	// Ops / NL
	report, _ := c.Backup("scheduled-001", nil)
	fmt.Println(report)
	rows, _ := c.NlQuery("all agents that cite paper X", "agent:planner")
	fmt.Println(rows)
}
```

Fresh stores use the same strict transport. Create the first identity with a
context whose only scope is `security:bootstrap`, empty roles/delegation, and
explicitly matching principal, agent, target agent, and signer id; then call
`BootstrapSystemIdentity(agentID, signerID, signerKey)`. The signer key should be
loaded from the deployment secret provider and is used only for that call. It is
never transmitted or retained by the client. `RegisterIdentity` and
`ApplyMultisigMutation` use the same detached canonical-operation signature.

Requires a server built with the matching features: `broker`, `security` (RBAC), redb
(backup/restore), `nl-query` + a configured planner. A build without one returns a clear
"not available in this build" error.

## Status

Runtime-untested in CI (no Go engine harness in this repo yet) and `go.sum` is not
vendored — run `go mod tidy` in this directory to fetch the one msgpack dep before
building. Its envelope, method-body, and detached-operation encodings mirror the
current Rust and Python contracts.
