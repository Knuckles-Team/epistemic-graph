// Package epgthin is a DELIBERATELY THIN Go client (CONCEPT:EG-328) for the
// epistemic-graph Program-B engine Methods that had no client surface: the native
// broker + append-log streams (EG-275..284/314), RBAC admin (EG-092), online
// backup/restore (EG-090), and NL->query (EG-080).
//
// It is NOT a full SDK — the canonical, full-featured client is the Python one in
// epistemic_graph/client.py. This binding speaks the SAME framed-MessagePack transport
// (4-byte big-endian length prefix + a msgpack {id, graph, auth_token, method, params}
// request; responses demuxed by id; auth = the current signed request-context
// envelope, bound to the canonical method body and replay controls).
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
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/vmihailenco/msgpack/v5"
)

const maxResponseBytes = 64 * 1024 * 1024

// Client is a thin, single-connection epistemic-graph client. Safe for sequential use;
// wrap calls with your own mutex for concurrent callers (the transport itself serializes
// the write + read below).
type Client struct {
	conn       net.Conn
	authSecret string
	context    RequestContextClaims
	graph      string
	id         int64
	mu         sync.Mutex
}

// RequestContextClaims is the complete authority context bound to every request.
// Roles, Scopes, and Delegation must be explicitly supplied, including empty slices.
type RequestContextClaims struct {
	Principal     string   `json:"principal"`
	Tenant        string   `json:"tenant"`
	Audience      string   `json:"audience"`
	AgentID       string   `json:"agent_id"`
	Roles         []string `json:"roles"`
	Scopes        []string `json:"scopes"`
	PolicyVersion string   `json:"policy_version"`
	Delegation    []string `json:"delegation"`
}

// Options configures a Dial.
type Options struct {
	// Network and Address identify the configured private engine endpoint.
	Network    string
	Address    string
	AuthSecret string
	Graph      string
	Context    *RequestContextClaims
}

// Dial connects to the engine over UDS or TCP.
func Dial(o Options) (*Client, error) {
	if o.AuthSecret == "" {
		return nil, fmt.Errorf("a non-empty authentication secret is required")
	}
	context, err := validateRequestContext(o.Context)
	if err != nil {
		return nil, err
	}
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
	return &Client{conn: conn, authSecret: o.AuthSecret, context: context, graph: g}, nil
}

// Close closes the underlying connection.
func (c *Client) Close() error { return c.conn.Close() }

func validateRequestContext(context *RequestContextClaims) (RequestContextClaims, error) {
	if context == nil {
		return RequestContextClaims{}, fmt.Errorf("a complete request context is required")
	}
	value := *context
	for name, claim := range map[string]string{
		"principal": value.Principal, "tenant": value.Tenant,
		"audience": value.Audience, "agent_id": value.AgentID,
		"policy_version": value.PolicyVersion,
	} {
		if strings.TrimSpace(claim) == "" {
			return RequestContextClaims{}, fmt.Errorf("request context %s must be non-empty", name)
		}
	}
	for name, claims := range map[string][]string{
		"roles": value.Roles, "scopes": value.Scopes, "delegation": value.Delegation,
	} {
		if claims == nil {
			return RequestContextClaims{}, fmt.Errorf("request context %s must be explicitly supplied", name)
		}
		seen := make(map[string]struct{}, len(claims))
		for _, claim := range claims {
			if strings.TrimSpace(claim) == "" {
				return RequestContextClaims{}, fmt.Errorf("request context %s entries must be non-empty", name)
			}
			if _, exists := seen[claim]; exists {
				return RequestContextClaims{}, fmt.Errorf("request context %s contains a duplicate entry", name)
			}
			seen[claim] = struct{}{}
		}
	}
	if value.Principal == value.AgentID {
		if len(value.Delegation) != 0 {
			return RequestContextClaims{}, fmt.Errorf("delegation must be empty when principal is the agent")
		}
	} else if len(value.Delegation) < 2 || value.Delegation[0] != value.Principal || value.Delegation[len(value.Delegation)-1] != value.AgentID {
		return RequestContextClaims{}, fmt.Errorf("delegation must run from principal to effective agent")
	}
	value.Roles = append([]string{}, value.Roles...)
	value.Scopes = append([]string{}, value.Scopes...)
	value.Delegation = append([]string{}, value.Delegation...)
	return value, nil
}

type methodBody struct {
	Method string `msgpack:"method"`
	Params any    `msgpack:"params"`
}

type wireRequest struct {
	ID        int64  `msgpack:"id"`
	Graph     string `msgpack:"graph"`
	AuthToken string `msgpack:"auth_token"`
	AgentID   string `msgpack:"agent_id"`
	Method    string `msgpack:"method"`
	Params    any    `msgpack:"params"`
}

type signedEnvelope struct {
	Context        RequestContextClaims `json:"context"`
	Timestamp      uint64               `json:"timestamp"`
	Nonce          string               `json:"nonce"`
	IdempotencyKey string               `json:"idempotency_key"`
	MAC            string               `json:"mac"`
}

func appendText(buf []byte, value string) []byte {
	var size [4]byte
	binary.BigEndian.PutUint32(size[:], uint32(len(value)))
	buf = append(buf, size[:]...)
	return append(buf, value...)
}

func appendList(buf []byte, values []string) []byte {
	var size [4]byte
	binary.BigEndian.PutUint32(size[:], uint32(len(values)))
	buf = append(buf, size[:]...)
	for _, value := range values {
		buf = appendText(buf, value)
	}
	return buf
}

func (c *Client) sign(id int64, graph, method string, params any, idempotencyKey string) (string, error) {
	body, err := msgpack.Marshal(methodBody{Method: method, Params: params})
	if err != nil {
		return "", fmt.Errorf("encode canonical method body: %w", err)
	}
	bodyDigest := sha256.Sum256(body)
	bodyHash := hex.EncodeToString(bodyDigest[:])
	if idempotencyKey == "" {
		idempotencyMaterial := fmt.Sprintf("%d\x00%s\x00%s\x00%s", id, graph, method, bodyHash)
		idempotencyDigest := sha256.Sum256([]byte(idempotencyMaterial))
		idempotencyKey = "rpc:sha256:" + hex.EncodeToString(idempotencyDigest[:])
	}
	nonceBytes := make([]byte, 24)
	if _, err := rand.Read(nonceBytes); err != nil {
		return "", fmt.Errorf("create request nonce: %w", err)
	}
	nonce := hex.EncodeToString(nonceBytes)
	timestamp := uint64(time.Now().Unix())

	canonical := make([]byte, 0, 512)
	canonical = appendText(canonical, "eg-envelope-v2")
	var requestID [8]byte
	binary.BigEndian.PutUint64(requestID[:], uint64(id))
	canonical = append(canonical, requestID[:]...)
	canonical = appendText(canonical, graph)
	canonical = appendText(canonical, method)
	canonical = appendText(canonical, bodyHash)
	canonical = appendText(canonical, c.context.Principal)
	canonical = appendText(canonical, c.context.Tenant)
	canonical = appendText(canonical, c.context.Audience)
	canonical = appendText(canonical, c.context.AgentID)
	canonical = appendList(canonical, c.context.Roles)
	canonical = appendList(canonical, c.context.Scopes)
	canonical = appendText(canonical, c.context.PolicyVersion)
	canonical = appendList(canonical, c.context.Delegation)
	var timestampBytes [8]byte
	binary.BigEndian.PutUint64(timestampBytes[:], timestamp)
	canonical = append(canonical, timestampBytes[:]...)
	canonical = appendText(canonical, nonce)
	canonical = appendText(canonical, idempotencyKey)
	mac := hmac.New(sha256.New, []byte(c.authSecret))
	_, _ = mac.Write(canonical)
	envelope := signedEnvelope{
		Context: c.context, Timestamp: timestamp, Nonce: nonce,
		IdempotencyKey: idempotencyKey, MAC: hex.EncodeToString(mac.Sum(nil)),
	}
	payload, err := json.Marshal(envelope)
	if err != nil {
		return "", fmt.Errorf("encode request envelope: %w", err)
	}
	return "eg2." + hex.EncodeToString(payload), nil
}

// send issues one request and returns the decoded result (or an engine error). A
// top-level msgpack bin result (the compact Raw layer) is decoded once more, matching
// the Python client. Because this client holds ONE connection and serializes each
// round-trip under a mutex, responses are read in order (no out-of-order demux needed).
func (c *Client) send(method string, params any, graph string) (any, error) {
	return c.sendWithIdempotency(method, params, graph, "")
}

func (c *Client) sendWithIdempotency(method string, params any, graph, idempotencyKey string) (any, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.id++
	id := c.id
	g := graph
	if g == "" {
		g = c.graph
	}
	token, err := c.sign(id, g, method, params, idempotencyKey)
	if err != nil {
		return nil, err
	}
	req := wireRequest{
		ID: id, Graph: g, AuthToken: token, AgentID: c.context.AgentID,
		Method: method, Params: params,
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
	bodySize := binary.BigEndian.Uint32(lenBuf[:])
	if bodySize == 0 || bodySize > maxResponseBytes {
		return nil, fmt.Errorf("epistemic-graph response exceeded the resource limit")
	}
	body := make([]byte, bodySize)
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
	if resp.ID != id {
		return nil, fmt.Errorf("epistemic-graph response correlation id mismatch")
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

func appendOperationBytes(buf []byte, value []byte) []byte {
	var size [8]byte
	binary.BigEndian.PutUint64(size[:], uint64(len(value)))
	buf = append(buf, size[:]...)
	return append(buf, value...)
}

func appendOperationList(buf []byte, values []string) []byte {
	var count [8]byte
	binary.BigEndian.PutUint64(count[:], uint64(len(values)))
	buf = appendOperationBytes(buf, count[:])
	for _, value := range values {
		buf = appendOperationBytes(buf, []byte(value))
	}
	return buf
}

func newOperationIdempotencyKey() (string, error) {
	random := make([]byte, 32)
	if _, err := rand.Read(random); err != nil {
		return "", fmt.Errorf("create operation idempotency key: %w", err)
	}
	digest := sha256.Sum256(random)
	return "operation:sha256:" + hex.EncodeToString(digest[:]), nil
}

func (c *Client) signContextOperation(domain, method string, params any, graph, idempotencyKey, signerID, signerKey string, requireContextPrincipal bool) (string, error) {
	if strings.TrimSpace(signerID) == "" || signerKey == "" {
		return "", fmt.Errorf("operation signer id and key must be non-empty")
	}
	if requireContextPrincipal && signerID != c.context.Principal {
		return "", fmt.Errorf("identity signer must match the verified principal")
	}
	body, err := msgpack.Marshal(methodBody{Method: method, Params: params})
	if err != nil {
		return "", fmt.Errorf("encode canonical operation body: %w", err)
	}
	canonical := make([]byte, 0, 512)
	for _, value := range []string{
		domain, c.context.Principal, c.context.Tenant, c.context.Audience, c.context.AgentID,
	} {
		canonical = appendOperationBytes(canonical, []byte(value))
	}
	canonical = appendOperationList(canonical, c.context.Roles)
	canonical = appendOperationList(canonical, c.context.Scopes)
	canonical = appendOperationBytes(canonical, []byte(c.context.PolicyVersion))
	canonical = appendOperationList(canonical, c.context.Delegation)
	canonical = appendOperationBytes(canonical, []byte(idempotencyKey))
	canonical = appendOperationBytes(canonical, []byte(graph))
	canonical = appendOperationBytes(canonical, body)
	digest := sha256.Sum256(canonical)
	mac := hmac.New(sha256.New, []byte(signerKey))
	_, _ = mac.Write(digest[:])
	return signerID + ":" + hex.EncodeToString(mac.Sum(nil)), nil
}

type registerIdentityParams struct {
	AgentID   string   `msgpack:"agent_id"`
	Role      any      `msgpack:"role"`
	Teams     []string `msgpack:"teams"`
	Signature string   `msgpack:"signature"`
	Roles     []string `msgpack:"roles"`
}

type managerRoleValue struct {
	Subordinates []string `msgpack:"subordinates"`
}

type multisigMutationParams struct {
	Signatures   []string `msgpack:"signatures"`
	Threshold    uint64   `msgpack:"threshold"`
	MutationType string   `msgpack:"mutation_type"`
	Query        string   `msgpack:"query"`
}

func validateStringList(name string, values []string) ([]string, error) {
	if values == nil {
		return nil, fmt.Errorf("%s must be explicitly supplied", name)
	}
	seen := make(map[string]struct{}, len(values))
	detached := make([]string, 0, len(values))
	for _, value := range values {
		if strings.TrimSpace(value) == "" {
			return nil, fmt.Errorf("%s entries must be non-empty", name)
		}
		if _, exists := seen[value]; exists {
			return nil, fmt.Errorf("%s contains a duplicate entry", name)
		}
		seen[value] = struct{}{}
		detached = append(detached, value)
	}
	return detached, nil
}

func normalizeAgentRole(role any) (any, error) {
	if name, ok := role.(string); ok {
		if name != "System" && name != "Agent" {
			return nil, fmt.Errorf("role must be System, Agent, or a Manager value")
		}
		return name, nil
	}
	managerMap, ok := role.(map[string]any)
	if !ok || len(managerMap) != 1 {
		return nil, fmt.Errorf("Manager role must contain only subordinates")
	}
	rawManager, ok := managerMap["Manager"].(map[string]any)
	if !ok || len(rawManager) != 1 {
		return nil, fmt.Errorf("Manager role must contain only subordinates")
	}
	rawSubordinates, ok := rawManager["subordinates"].([]string)
	if !ok {
		return nil, fmt.Errorf("Manager subordinates must be an explicit string list")
	}
	subordinates, err := validateStringList("Manager subordinates", rawSubordinates)
	if err != nil {
		return nil, err
	}
	return map[string]any{"Manager": managerRoleValue{Subordinates: subordinates}}, nil
}

// RegisterIdentity signs and submits a current-context identity operation.
// The signer key is used only for this call and is never transmitted or retained.
func (c *Client) RegisterIdentity(agentID string, role any, teams, roles []string, signerID, signerKey string) (any, error) {
	if strings.TrimSpace(agentID) == "" {
		return nil, fmt.Errorf("agent id must be a non-empty opaque identifier")
	}
	teams, err := validateStringList("teams", teams)
	if err != nil {
		return nil, err
	}
	roles, err = validateStringList("roles", roles)
	if err != nil {
		return nil, err
	}
	role, err = normalizeAgentRole(role)
	if err != nil {
		return nil, err
	}
	idempotencyKey, err := newOperationIdempotencyKey()
	if err != nil {
		return nil, err
	}
	params := registerIdentityParams{
		AgentID: agentID, Role: role, Teams: teams, Signature: "", Roles: roles,
	}
	signature, err := c.signContextOperation(
		"eg-register-identity-v2", "RegisterIdentity", params, "__commons__",
		idempotencyKey, signerID, signerKey, true,
	)
	if err != nil {
		return nil, err
	}
	params.Signature = signature
	return c.sendWithIdempotency("RegisterIdentity", params, "__commons__", idempotencyKey)
}

// BootstrapSystemIdentity creates the first system identity through the
// current-only, signed bootstrap gate.
func (c *Client) BootstrapSystemIdentity(agentID, signerID, signerKey string) (any, error) {
	if c.context.Principal != agentID || c.context.AgentID != agentID || signerID != agentID ||
		len(c.context.Roles) != 0 || len(c.context.Scopes) != 1 ||
		c.context.Scopes[0] != "security:bootstrap" || len(c.context.Delegation) != 0 {
		return nil, fmt.Errorf("bootstrap requires matching explicit identities and only security:bootstrap authority")
	}
	return c.RegisterIdentity(agentID, "System", []string{}, []string{}, signerID, signerKey)
}

// ApplyMultisigMutation signs one canonical operation with each explicit key.
func (c *Client) ApplyMultisigMutation(signerKeys map[string]string, threshold uint64, mutationType, query string) (any, error) {
	if threshold == 0 || uint64(len(signerKeys)) < threshold {
		return nil, fmt.Errorf("threshold requires at least that many explicit signers")
	}
	for signerID, signerKey := range signerKeys {
		if strings.TrimSpace(signerID) == "" || signerKey == "" {
			return nil, fmt.Errorf("operation signer ids and keys must be non-empty")
		}
	}
	if strings.TrimSpace(mutationType) == "" || strings.TrimSpace(query) == "" {
		return nil, fmt.Errorf("mutation type and query must be non-empty")
	}
	idempotencyKey, err := newOperationIdempotencyKey()
	if err != nil {
		return nil, err
	}
	params := multisigMutationParams{
		Signatures: []string{}, Threshold: threshold, MutationType: mutationType, Query: query,
	}
	signerIDs := make([]string, 0, len(signerKeys))
	for signerID := range signerKeys {
		signerIDs = append(signerIDs, signerID)
	}
	sort.Strings(signerIDs)
	for _, signerID := range signerIDs {
		unsigned := params
		unsigned.Signatures = []string{}
		signature, signErr := c.signContextOperation(
			"eg-multisig-mutation-v2", "ApplyMultisigMutation", unsigned, "__commons__",
			idempotencyKey, signerID, signerKeys[signerID], false,
		)
		if signErr != nil {
			return nil, signErr
		}
		params.Signatures = append(params.Signatures, signature)
	}
	return c.sendWithIdempotency("ApplyMultisigMutation", params, "__commons__", idempotencyKey)
}

// ── Broker admin (EG-275..278) ──────────────────────────────────────────────

type declareExchangeParams struct {
	Exchange string `msgpack:"exchange"`
	Kind     string `msgpack:"kind"`
}

type deleteExchangeParams struct {
	Exchange string `msgpack:"exchange"`
}

type bindingParams struct {
	Exchange   string `msgpack:"exchange"`
	Queue      string `msgpack:"queue"`
	RoutingKey string `msgpack:"routing_key"`
}

type declareQueueParams struct {
	Queue            string  `msgpack:"queue"`
	DLExchange       *string `msgpack:"dl_exchange"`
	DLRoutingKey     *string `msgpack:"dl_routing_key"`
	MaxDeliveryCount *uint32 `msgpack:"max_delivery_count"`
	MessageTTLMs     *uint64 `msgpack:"message_ttl_ms"`
	QueueExpiryMs    *uint64 `msgpack:"queue_expiry_ms"`
	MaxPriority      *uint8  `msgpack:"max_priority"`
}

type publishParams struct {
	Exchange   string `msgpack:"exchange"`
	RoutingKey string `msgpack:"routing_key"`
	Payload    []byte `msgpack:"payload"`
}

type publishExtendedParams struct {
	Exchange   string  `msgpack:"exchange"`
	RoutingKey string  `msgpack:"routing_key"`
	Payload    []byte  `msgpack:"payload"`
	Priority   int64   `msgpack:"priority"`
	DelayMs    *uint64 `msgpack:"delay_ms"`
	TTLMs      *uint64 `msgpack:"ttl_ms"`
	NowMs      *uint64 `msgpack:"now_ms"`
}

type publishIdempotentParams struct {
	Exchange   string  `msgpack:"exchange"`
	RoutingKey string  `msgpack:"routing_key"`
	Payload    []byte  `msgpack:"payload"`
	ProducerID *string `msgpack:"producer_id"`
	Seq        int64   `msgpack:"seq"`
	Priority   int64   `msgpack:"priority"`
	DelayMs    *uint64 `msgpack:"delay_ms"`
	TTLMs      *uint64 `msgpack:"ttl_ms"`
	NowMs      *uint64 `msgpack:"now_ms"`
}

type brokerConsumeParams struct {
	Queue    string `msgpack:"queue"`
	Group    string `msgpack:"group"`
	Consumer string `msgpack:"consumer"`
	NowMs    uint64 `msgpack:"now_ms"`
	LeaseMs  uint64 `msgpack:"lease_ms"`
	Prefetch uint32 `msgpack:"prefetch"`
}

type brokerAckParams struct {
	Queue  string `msgpack:"queue"`
	NodeID string `msgpack:"node_id"`
}

type brokerRejectParams struct {
	Queue   string `msgpack:"queue"`
	NodeID  string `msgpack:"node_id"`
	Requeue bool   `msgpack:"requeue"`
	NowMs   uint64 `msgpack:"now_ms"`
}

type brokerAckTagParams struct {
\tDeliveryTag int64  `msgpack:"delivery_tag"`
\tConsumer    string `msgpack:"consumer"`
}

type brokerNackTagParams struct {
\tDeliveryTag int64  `msgpack:"delivery_tag"`
\tConsumer    string `msgpack:"consumer"`
\tRequeue     bool   `msgpack:"requeue"`
\tNowMs       uint64 `msgpack:"now_ms"`
}

type brokerRenewTagParams struct {
\tDeliveryTag int64  `msgpack:"delivery_tag"`
\tConsumer    string `msgpack:"consumer"`
\tNowMs       uint64 `msgpack:"now_ms"`
\tLeaseMs     uint64 `msgpack:"lease_ms"`
}

type nowParams struct {
	NowMs uint64 `msgpack:"now_ms"`
}

type streamDeclareParams struct {
	Stream      string  `msgpack:"stream"`
	MaxMessages *uint64 `msgpack:"max_messages"`
	MaxAgeMs    *uint64 `msgpack:"max_age_ms"`
}

type streamPublishParams struct {
	Stream  string `msgpack:"stream"`
	Payload []byte `msgpack:"payload"`
	NowMs   uint64 `msgpack:"now_ms"`
}

type streamReadParams struct {
	Stream     string `msgpack:"stream"`
	FromOffset int64  `msgpack:"from_offset"`
	Max        uint64 `msgpack:"max"`
}

type streamTrimParams struct {
	Stream string `msgpack:"stream"`
	NowMs  uint64 `msgpack:"now_ms"`
}

type streamOffsetParams struct {
	Stream string `msgpack:"stream"`
	Group  string `msgpack:"group"`
	Offset int64  `msgpack:"offset"`
}

type streamGroupParams struct {
	Stream string `msgpack:"stream"`
	Group  string `msgpack:"group"`
}

type rbacAdminParams struct {
	Op any `msgpack:"op"`
}

type rbacAddRoleValue struct {
	Name    string   `msgpack:"name"`
	Parents []string `msgpack:"parents"`
}

type rbacGrantValue struct {
	Role     string `msgpack:"role"`
	Resource any    `msgpack:"resource"`
	Action   string `msgpack:"action"`
	Effect   string `msgpack:"effect"`
}

type backupParams struct {
	Destination string  `msgpack:"destination"`
	Label       *string `msgpack:"label"`
}

type restoreParams struct {
	Source       string `msgpack:"source"`
	TargetShards uint64 `msgpack:"target_shards"`
}

type nlQueryParams struct {
	Text  string `msgpack:"text"`
	Graph string `msgpack:"graph"`
}

func (c *Client) DeclareExchange(exchange, kind string) (any, error) {
	if kind == "" {
		kind = "direct"
	}
	return c.send("DeclareExchange", declareExchangeParams{Exchange: exchange, Kind: kind}, "")
}

func (c *Client) DeleteExchange(exchange string) (any, error) {
	return c.send("DeleteExchange", deleteExchangeParams{Exchange: exchange}, "")
}

func (c *Client) BindQueue(exchange, queue, routingKey string) (any, error) {
	return c.send("BindQueue", bindingParams{Exchange: exchange, Queue: queue, RoutingKey: routingKey}, "")
}

func (c *Client) UnbindQueue(exchange, queue, routingKey string) (any, error) {
	return c.send("UnbindQueue", bindingParams{Exchange: exchange, Queue: queue, RoutingKey: routingKey}, "")
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
	return c.send("DeclareQueue", declareQueueParams{
		Queue: queue, DLExchange: p.DLExchange, DLRoutingKey: p.DLRoutingKey,
		MaxDeliveryCount: p.MaxDeliveryCount, MessageTTLMs: p.MessageTTLMs,
		QueueExpiryMs: p.QueueExpiryMs, MaxPriority: p.MaxPriority,
	}, "")
}

// ── Broker publish (EG-275/279/284/314) ─────────────────────────────────────

func (c *Client) Publish(exchange, routingKey string, payload []byte) (any, error) {
	return c.send("Publish", publishParams{Exchange: exchange, RoutingKey: routingKey, Payload: payload}, "")
}

// PublishOpts are the optional priority/delay/ttl/now fields for policy-carrying publishes.
type PublishOpts struct {
	Priority int64
	DelayMs  *uint64
	TTLMs    *uint64
	NowMs    *uint64
}

func (c *Client) PublishEx(exchange, routingKey string, payload []byte, o PublishOpts) (any, error) {
	return c.send("PublishEx", publishExtendedParams{
		Exchange: exchange, RoutingKey: routingKey, Payload: payload, Priority: o.Priority,
		DelayMs: o.DelayMs, TTLMs: o.TTLMs, NowMs: o.NowMs,
	}, "")
}

func (c *Client) PublishConfirmed(exchange, routingKey string, payload []byte, o PublishOpts) (any, error) {
	return c.send("PublishConfirmed", publishExtendedParams{
		Exchange: exchange, RoutingKey: routingKey, Payload: payload, Priority: o.Priority,
		DelayMs: o.DelayMs, TTLMs: o.TTLMs, NowMs: o.NowMs,
	}, "")
}

func (c *Client) PublishIdempotent(exchange, routingKey string, payload []byte, producerID *string, seq int64, o PublishOpts) (any, error) {
	return c.send("PublishIdempotent", publishIdempotentParams{
		Exchange: exchange, RoutingKey: routingKey, Payload: payload, ProducerID: producerID,
		Seq: seq, Priority: o.Priority, DelayMs: o.DelayMs, TTLMs: o.TTLMs, NowMs: o.NowMs,
	}, "")
}

// ── Consume / ack / reject (EG-280/276/284) ─────────────────────────────────

func (c *Client) BrokerConsume(queue, group, consumer string, nowMs, leaseMs uint64, prefetch uint32) (any, error) {
	return c.send("BrokerConsume", brokerConsumeParams{
		Queue: queue, Group: group, Consumer: consumer,
		NowMs: nowMs, LeaseMs: leaseMs, Prefetch: prefetch,
	}, "")
}

func (c *Client) BrokerAck(queue, nodeID string) (any, error) {
	return c.send("BrokerAck", brokerAckParams{Queue: queue, NodeID: nodeID}, "")
}

func (c *Client) BrokerReject(queue, nodeID string, requeue bool, nowMs uint64) (any, error) {
	return c.send("BrokerReject", brokerRejectParams{
		Queue: queue, NodeID: nodeID, Requeue: requeue, NowMs: nowMs,
	}, "")
}

func (c *Client) BrokerAckTag(deliveryTag int64, consumer string) (any, error) {
\treturn c.send("BrokerAckTag", brokerAckTagParams{
\t\tDeliveryTag: deliveryTag, Consumer: consumer,
\t}, "")
}

func (c *Client) BrokerNackTag(deliveryTag int64, consumer string, requeue bool, nowMs uint64) (any, error) {
\treturn c.send("BrokerNackTag", brokerNackTagParams{
\t\tDeliveryTag: deliveryTag, Consumer: consumer, Requeue: requeue, NowMs: nowMs,
\t}, "")
}

func (c *Client) BrokerRenewTag(deliveryTag int64, consumer string, nowMs, leaseMs uint64) (any, error) {
\treturn c.send("BrokerRenewTag", brokerRenewTagParams{
\t\tDeliveryTag: deliveryTag, Consumer: consumer, NowMs: nowMs, LeaseMs: leaseMs,
\t}, "")
}

func (c *Client) SweepExpired(nowMs uint64) (any, error) {
	return c.send("SweepExpired", nowParams{NowMs: nowMs}, "")
}

// ── Replayable append-log streams (EG-283) ──────────────────────────────────

func (c *Client) StreamDeclare(stream string, maxMessages, maxAgeMs *uint64) (any, error) {
	return c.send("StreamDeclare", streamDeclareParams{
		Stream: stream, MaxMessages: maxMessages, MaxAgeMs: maxAgeMs,
	}, "")
}

func (c *Client) StreamPublish(stream string, payload []byte, nowMs uint64) (any, error) {
	return c.send("StreamPublish", streamPublishParams{
		Stream: stream, Payload: payload, NowMs: nowMs,
	}, "")
}

func (c *Client) StreamRead(stream string, fromOffset int64, max uint64) (any, error) {
	return c.send("StreamRead", streamReadParams{
		Stream: stream, FromOffset: fromOffset, Max: max,
	}, "")
}

func (c *Client) StreamTrim(stream string, nowMs uint64) (any, error) {
	return c.send("StreamTrim", streamTrimParams{Stream: stream, NowMs: nowMs}, "")
}

func (c *Client) StreamCommitOffset(stream, group string, offset int64) (any, error) {
	return c.send("StreamCommitOffset", streamOffsetParams{
		Stream: stream, Group: group, Offset: offset,
	}, "")
}

func (c *Client) StreamCommittedOffset(stream, group string) (any, error) {
	return c.send("StreamCommittedOffset", streamGroupParams{Stream: stream, Group: group}, "")
}

// ── RBAC admin (EG-092) — externally-tagged RbacAdminOp / ResourceSelector ───
//
// A ResourceSelector is either the string "All" or a single-key map:
// {"Pattern": s} / {"Label": s} / {"Graph": s}. Pass it as `any` accordingly.

func validateResourceSelector(resource any) (any, error) {
	if value, ok := resource.(string); ok {
		if value != "All" {
			return nil, fmt.Errorf("resource selector string must be All")
		}
		return value, nil
	}
	var selector map[string]string
	switch value := resource.(type) {
	case map[string]string:
		selector = value
	case map[string]any:
		selector = make(map[string]string, len(value))
		for key, raw := range value {
			text, ok := raw.(string)
			if !ok {
				return nil, fmt.Errorf("resource selector value must be a string")
			}
			selector[key] = text
		}
	default:
		return nil, fmt.Errorf("resource selector must be All or one named selector")
	}
	if len(selector) != 1 {
		return nil, fmt.Errorf("resource selector must contain exactly one entry")
	}
	for key, value := range selector {
		if key != "Pattern" && key != "Label" && key != "Graph" {
			return nil, fmt.Errorf("unsupported resource selector")
		}
		if strings.TrimSpace(value) == "" {
			return nil, fmt.Errorf("resource selector value must be non-empty")
		}
	}
	return selector, nil
}

func (c *Client) RbacAddRole(name string, parents []string) (any, error) {
	if parents == nil {
		parents = []string{}
	}
	return c.send("RbacAdmin", rbacAdminParams{
		Op: map[string]any{"AddRole": rbacAddRoleValue{Name: name, Parents: parents}},
	}, "")
}

func (c *Client) RbacRemoveRole(name string) (any, error) {
	return c.send("RbacAdmin", rbacAdminParams{Op: map[string]any{"RemoveRole": name}}, "")
}

func (c *Client) RbacAddGrant(role string, resource any, action, effect string) (any, error) {
	if effect == "" {
		effect = "Allow"
	}
	selector, err := validateResourceSelector(resource)
	if err != nil {
		return nil, err
	}
	grant := rbacGrantValue{Role: role, Resource: selector, Action: action, Effect: effect}
	return c.send("RbacAdmin", rbacAdminParams{Op: map[string]any{"AddGrant": grant}}, "")
}

func (c *Client) RbacRemoveGrant(role string, resource any, action, effect string) (any, error) {
	if effect == "" {
		effect = "Allow"
	}
	selector, err := validateResourceSelector(resource)
	if err != nil {
		return nil, err
	}
	grant := rbacGrantValue{Role: role, Resource: selector, Action: action, Effect: effect}
	return c.send("RbacAdmin", rbacAdminParams{Op: map[string]any{"RemoveGrant": grant}}, "")
}

func (c *Client) RbacList() (any, error) {
	return c.send("RbacAdmin", rbacAdminParams{Op: "List"}, "")
}

// ── Ops: online backup / restore (EG-090) ───────────────────────────────────

func (c *Client) Backup(destination string, label *string) (any, error) {
	return c.send("Backup", backupParams{Destination: destination, Label: label}, "")
}

func (c *Client) Restore(source string, targetShards uint64) (any, error) {
	if targetShards < 1 || targetShards > 64 {
		return nil, fmt.Errorf("targetShards must be between 1 and 64")
	}
	return c.send(
		"Restore",
		restoreParams{Source: source, TargetShards: targetShards},
		"",
	)
}

// ── NL->query (EG-080) ──────────────────────────────────────────────────────

func (c *Client) NlQuery(text, graph string) (any, error) {
	targetGraph := graph
	if targetGraph == "" {
		targetGraph = c.graph
	}
	return c.send("NlQuery", nlQueryParams{Text: text, Graph: targetGraph}, targetGraph)
}
