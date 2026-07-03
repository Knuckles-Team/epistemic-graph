// Package epgthin is a DELIBERATELY THIN Go client (CONCEPT:EG-3.1) for the
// epistemic-graph Program-B engine Methods that had no client surface: the native
// broker + append-log streams (EG-275..284/314), RBAC admin (EG-092), online
// backup/restore (EG-090), and NL->query (EG-080).
//
// It is NOT a full SDK — the canonical, full-featured client is the Python one in
// epistemic_graph/client.py. This binding speaks the SAME framed-MessagePack transport
// (4-byte big-endian length prefix + a msgpack {id, graph, auth_token, method, params}
// request; responses demuxed by id; auth = HMAC-SHA256(secret, str(id)) hex).
//
// The method surface is GENERATED-FROM-THE-Method-LIST: each method maps 1:1 to a Rust
// Method variant in crates/eg-types/src/protocol.rs, using the exact serde field names
// that enum destructures.
//
// Pi-contract: thin. One pure-Go dep (github.com/vmihailenco/msgpack/v5) for framing;
// stdlib net + crypto/hmac. No cgo, no heavy SDK. This is a client, never part of `pi`.
package epgthin

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"net"
	"strconv"
	"sync"

	"github.com/vmihailenco/msgpack/v5"
)

// Client is a thin, single-connection epistemic-graph client. Safe for sequential use;
// wrap calls with your own mutex for concurrent callers (the transport itself serializes
// the write + read below).
type Client struct {
	conn       net.Conn
	authSecret string
	graph      string
	id         int64
	mu         sync.Mutex
}

// Options configures a Dial.
type Options struct {
	// Network+Address, e.g. ("unix", "/tmp/epistemic-graph.sock") or ("tcp", "127.0.0.1:8080").
	Network    string
	Address    string
	AuthSecret string
	Graph      string
}

// Dial connects to the engine over UDS or TCP.
func Dial(o Options) (*Client, error) {
	if o.Network == "" {
		o.Network = "unix"
	}
	conn, err := net.Dial(o.Network, o.Address)
	if err != nil {
		return nil, err
	}
	g := o.Graph
	if g == "" {
		g = "__commons__"
	}
	return &Client{conn: conn, authSecret: o.AuthSecret, graph: g}, nil
}

// Close closes the underlying connection.
func (c *Client) Close() error { return c.conn.Close() }

func (c *Client) token(id int64) string {
	if c.authSecret == "" {
		return ""
	}
	m := hmac.New(sha256.New, []byte(c.authSecret))
	m.Write([]byte(strconv.FormatInt(id, 10)))
	return hex.EncodeToString(m.Sum(nil))
}

// Send issues one request and returns the decoded result (or an engine error). A
// top-level msgpack bin result (the compact Raw layer) is decoded once more, matching
// the Python client. Because this client holds ONE connection and serializes each
// round-trip under a mutex, responses are read in order (no out-of-order demux needed).
func (c *Client) Send(method string, params map[string]any, graph string) (any, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.id++
	id := c.id
	g := graph
	if g == "" {
		g = c.graph
	}
	req := map[string]any{
		"id":         id,
		"graph":      g,
		"auth_token": c.token(id),
		"method":     method,
	}
	if params != nil {
		req["params"] = params
	}
	payload, err := msgpack.Marshal(req)
	if err != nil {
		return nil, err
	}
	frame := make([]byte, 4+len(payload))
	binary.BigEndian.PutUint32(frame[:4], uint32(len(payload)))
	copy(frame[4:], payload)
	if _, err := c.conn.Write(frame); err != nil {
		return nil, err
	}

	var lenBuf [4]byte
	if _, err := io.ReadFull(c.conn, lenBuf[:]); err != nil {
		return nil, err
	}
	body := make([]byte, binary.BigEndian.Uint32(lenBuf[:]))
	if _, err := io.ReadFull(c.conn, body); err != nil {
		return nil, err
	}
	var resp struct {
		ID     int64  `msgpack:"id"`
		Result any    `msgpack:"result"`
		Error  string `msgpack:"error"`
	}
	if err := msgpack.Unmarshal(body, &resp); err != nil {
		return nil, err
	}
	if resp.Error != "" {
		return nil, fmt.Errorf("epistemic-graph: %s", resp.Error)
	}
	// Compact result: a top-level msgpack bin is a second Raw layer.
	if b, ok := resp.Result.([]byte); ok {
		var inner any
		if err := msgpack.Unmarshal(b, &inner); err == nil {
			return inner, nil
		}
	}
	return resp.Result, nil
}

// ── Broker admin (EG-275..278) ──────────────────────────────────────────────

func (c *Client) DeclareExchange(exchange, kind string) (any, error) {
	if kind == "" {
		kind = "direct"
	}
	return c.Send("DeclareExchange", map[string]any{"exchange": exchange, "kind": kind}, "")
}

func (c *Client) DeleteExchange(exchange string) (any, error) {
	return c.Send("DeleteExchange", map[string]any{"exchange": exchange}, "")
}

func (c *Client) BindQueue(exchange, queue, routingKey string) (any, error) {
	return c.Send("BindQueue", map[string]any{"exchange": exchange, "queue": queue, "routing_key": routingKey}, "")
}

func (c *Client) UnbindQueue(exchange, queue, routingKey string) (any, error) {
	return c.Send("UnbindQueue", map[string]any{"exchange": exchange, "queue": queue, "routing_key": routingKey}, "")
}

// QueuePolicy carries the optional DLQ/TTL/priority fields (EG-276/277/278); nil fields
// are sent as msgpack nil (an all-nil policy is a no-op that keeps EG-275 behavior).
type QueuePolicy struct {
	DLExchange       *string
	DLRoutingKey     *string
	MaxDeliveryCount *uint32
	MessageTTLMs     *uint64
	QueueExpiryMs    *uint64
	MaxPriority      *uint8
}

func (c *Client) DeclareQueue(queue string, p QueuePolicy) (any, error) {
	return c.Send("DeclareQueue", map[string]any{
		"queue":              queue,
		"dl_exchange":        p.DLExchange,
		"dl_routing_key":     p.DLRoutingKey,
		"max_delivery_count": p.MaxDeliveryCount,
		"message_ttl_ms":     p.MessageTTLMs,
		"queue_expiry_ms":    p.QueueExpiryMs,
		"max_priority":       p.MaxPriority,
	}, "")
}

// ── Broker publish (EG-275/279/284/314) ─────────────────────────────────────

func (c *Client) Publish(exchange, routingKey string, payload []byte) (any, error) {
	return c.Send("Publish", map[string]any{"exchange": exchange, "routing_key": routingKey, "payload": payload}, "")
}

// PublishOpts are the optional priority/delay/ttl/now fields for policy-carrying publishes.
type PublishOpts struct {
	Priority int64
	DelayMs  *uint64
	TTLMs    *uint64
	NowMs    *uint64
}

func (c *Client) PublishEx(exchange, routingKey string, payload []byte, o PublishOpts) (any, error) {
	return c.Send("PublishEx", map[string]any{
		"exchange": exchange, "routing_key": routingKey, "payload": payload,
		"priority": o.Priority, "delay_ms": o.DelayMs, "ttl_ms": o.TTLMs, "now_ms": o.NowMs,
	}, "")
}

func (c *Client) PublishConfirmed(exchange, routingKey string, payload []byte, o PublishOpts) (any, error) {
	return c.Send("PublishConfirmed", map[string]any{
		"exchange": exchange, "routing_key": routingKey, "payload": payload,
		"priority": o.Priority, "delay_ms": o.DelayMs, "ttl_ms": o.TTLMs, "now_ms": o.NowMs,
	}, "")
}

func (c *Client) PublishIdempotent(exchange, routingKey string, payload []byte, producerID *string, seq int64, o PublishOpts) (any, error) {
	return c.Send("PublishIdempotent", map[string]any{
		"exchange": exchange, "routing_key": routingKey, "payload": payload,
		"producer_id": producerID, "seq": seq,
		"priority": o.Priority, "delay_ms": o.DelayMs, "ttl_ms": o.TTLMs, "now_ms": o.NowMs,
	}, "")
}

// ── Consume / ack / reject (EG-280/276/284) ─────────────────────────────────

func (c *Client) BrokerConsume(queue, group, consumer string, nowMs, leaseMs uint64, prefetch uint32) (any, error) {
	return c.Send("BrokerConsume", map[string]any{
		"queue": queue, "group": group, "consumer": consumer,
		"now_ms": nowMs, "lease_ms": leaseMs, "prefetch": prefetch,
	}, "")
}

func (c *Client) BrokerAck(queue, nodeID string) (any, error) {
	return c.Send("BrokerAck", map[string]any{"queue": queue, "node_id": nodeID}, "")
}

func (c *Client) BrokerReject(queue, nodeID string, requeue bool, nowMs uint64) (any, error) {
	return c.Send("BrokerReject", map[string]any{"queue": queue, "node_id": nodeID, "requeue": requeue, "now_ms": nowMs}, "")
}

func (c *Client) BrokerAckTag(deliveryTag int64) (any, error) {
	return c.Send("BrokerAckTag", map[string]any{"delivery_tag": deliveryTag}, "")
}

func (c *Client) BrokerNackTag(deliveryTag int64, requeue bool, nowMs uint64) (any, error) {
	return c.Send("BrokerNackTag", map[string]any{"delivery_tag": deliveryTag, "requeue": requeue, "now_ms": nowMs}, "")
}

func (c *Client) SweepExpired(nowMs uint64) (any, error) {
	return c.Send("SweepExpired", map[string]any{"now_ms": nowMs}, "")
}

// ── Replayable append-log streams (EG-283) ──────────────────────────────────

func (c *Client) StreamDeclare(stream string, maxMessages, maxAgeMs *uint64) (any, error) {
	return c.Send("StreamDeclare", map[string]any{"stream": stream, "max_messages": maxMessages, "max_age_ms": maxAgeMs}, "")
}

func (c *Client) StreamPublish(stream string, payload []byte, nowMs uint64) (any, error) {
	return c.Send("StreamPublish", map[string]any{"stream": stream, "payload": payload, "now_ms": nowMs}, "")
}

func (c *Client) StreamRead(stream string, fromOffset int64, max uint64) (any, error) {
	return c.Send("StreamRead", map[string]any{"stream": stream, "from_offset": fromOffset, "max": max}, "")
}

func (c *Client) StreamTrim(stream string, nowMs uint64) (any, error) {
	return c.Send("StreamTrim", map[string]any{"stream": stream, "now_ms": nowMs}, "")
}

func (c *Client) StreamCommitOffset(stream, group string, offset int64) (any, error) {
	return c.Send("StreamCommitOffset", map[string]any{"stream": stream, "group": group, "offset": offset}, "")
}

func (c *Client) StreamCommittedOffset(stream, group string) (any, error) {
	return c.Send("StreamCommittedOffset", map[string]any{"stream": stream, "group": group}, "")
}

// ── RBAC admin (EG-092) — externally-tagged RbacAdminOp / ResourceSelector ───
//
// A ResourceSelector is either the string "All" or a single-key map:
// {"Pattern": s} / {"Label": s} / {"Graph": s}. Pass it as `any` accordingly.

func (c *Client) RbacAddRole(name string, parents []string) (any, error) {
	if parents == nil {
		parents = []string{}
	}
	return c.Send("RbacAdmin", map[string]any{"op": map[string]any{"AddRole": map[string]any{"name": name, "parents": parents}}}, "")
}

func (c *Client) RbacRemoveRole(name string) (any, error) {
	return c.Send("RbacAdmin", map[string]any{"op": map[string]any{"RemoveRole": name}}, "")
}

func (c *Client) RbacAddGrant(role string, resource any, action, effect string) (any, error) {
	if effect == "" {
		effect = "Allow"
	}
	grant := map[string]any{"role": role, "resource": resource, "action": action, "effect": effect}
	return c.Send("RbacAdmin", map[string]any{"op": map[string]any{"AddGrant": grant}}, "")
}

func (c *Client) RbacRemoveGrant(role string, resource any, action, effect string) (any, error) {
	if effect == "" {
		effect = "Allow"
	}
	grant := map[string]any{"role": role, "resource": resource, "action": action, "effect": effect}
	return c.Send("RbacAdmin", map[string]any{"op": map[string]any{"RemoveGrant": grant}}, "")
}

func (c *Client) RbacList() (any, error) {
	return c.Send("RbacAdmin", map[string]any{"op": "List"}, "")
}

// ── Ops: online backup / restore (EG-090) ───────────────────────────────────

func (c *Client) Backup(destination string, label *string) (any, error) {
	return c.Send("Backup", map[string]any{"destination": destination, "label": label}, "")
}

func (c *Client) Restore(source string) (any, error) {
	return c.Send("Restore", map[string]any{"source": source}, "")
}

// ── NL->query (EG-080) ──────────────────────────────────────────────────────

func (c *Client) NlQuery(text, graph string) (any, error) {
	return c.Send("NlQuery", map[string]any{"text": text}, graph)
}
