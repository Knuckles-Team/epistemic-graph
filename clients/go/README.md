# epistemic-graph — thin Go client (CONCEPT:EG-328)

A **deliberately thin** Go client for the Program-B engine `Method`s that had no client
surface. It is **not a full SDK** — the canonical, full-featured client is the Python one
in [`epistemic_graph/client.py`](../../epistemic_graph/client.py). This binding covers
only the new wire ops from B1.7:

| Domain | Methods | Engine concept |
|--------|---------|----------------|
| Broker admin | `DeclareExchange` `DeleteExchange` `DeclareQueue` `BindQueue` `UnbindQueue` | EG-275/276/277/278 |
| Broker publish | `Publish` `PublishEx` `PublishConfirmed` `PublishIdempotent` | EG-275/279/284/314 |
| Broker consume | `BrokerConsume` `BrokerAck` `BrokerReject` `BrokerAckTag` `BrokerNackTag` `SweepExpired` | EG-280/276/284 |
| Streams | `StreamDeclare` `StreamPublish` `StreamRead` `StreamTrim` `StreamCommitOffset` `StreamCommittedOffset` | EG-283 |
| RBAC admin | `RbacAddRole` `RbacRemoveRole` `RbacAddGrant` `RbacRemoveGrant` `RbacList` | EG-092 |
| Ops | `Backup` `Restore` | EG-090 |
| NL→query | `NlQuery` | EG-080 |

> **Generated-from-the-Method-list.** Every method maps 1:1 to a Rust `Method`
> variant in [`crates/eg-types/src/protocol.rs`](../../crates/eg-types/src/protocol.rs),
> using the exact serde field names that enum destructures. The full graph / vector /
> RDF / SQL API is intentionally omitted — use the Python client for that.

## Wire contract

Same framed-MessagePack transport as the Python client:

- **Framing:** 4-byte big-endian length prefix + a msgpack request
  `{ id, graph, auth_token, method, params }`.
- **Auth:** `auth_token = hex(HMAC_SHA256(authSecret, strconv(id)))` (empty when no secret).
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
	"time"

	epg "github.com/epistemic-graph/clients/go"
)

func main() {
	c, err := epg.Dial(epg.Options{
		Network:    "unix",
		Address:    "/tmp/epistemic-graph.sock",
		AuthSecret: "my-secret",
		Graph:      "agent:planner",
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
	report, _ := c.Backup("/backups/nightly", nil)
	fmt.Println(report)
	rows, _ := c.NlQuery("all agents that cite paper X", "agent:planner")
	fmt.Println(rows)
}
```

Requires a server built with the matching features: `broker`, `security` (RBAC), redb
(backup/restore), `nl-query` + a configured planner. A build without one returns a clear
"not available in this build" error.

## Status

Runtime-untested in CI (no Go engine harness in this repo yet) and `go.sum` is not
vendored — run `go mod tidy` in this directory to fetch the one msgpack dep before
building. The framing/auth mirror the Python client's verified transport exactly. Treat
as a reference thin binding.
