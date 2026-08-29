package epgthin

import (
	"encoding/hex"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

func testContext() *RequestContextClaims {
	return &RequestContextClaims{
		Principal: "service:test", Tenant: "tenant:test", Audience: "engine:test",
		AgentID: "service:test", Roles: []string{"client"}, Scopes: []string{"graph:read"},
		PolicyVersion: "policy:test", Delegation: []string{},
	}
}

func TestRequestContextRequiresExplicitUniqueLists(t *testing.T) {
	context := testContext()
	context.Scopes = nil
	if _, err := validateRequestContext(context); err == nil {
		t.Fatal("missing scopes were accepted")
	}
	context = testContext()
	context.Roles = []string{"client", "client"}
	if _, err := validateRequestContext(context); err == nil {
		t.Fatal("duplicate roles were accepted")
	}
}

func TestSignerEmitsCurrentBoundEnvelope(t *testing.T) {
	context, err := validateRequestContext(testContext())
	if err != nil {
		t.Fatal(err)
	}
	client := &Client{authSecret: "test-envelope-secret", context: context, graph: "graph:test"}
	token, err := client.sign(
		7,
		"graph:test",
		"DeleteExchange",
		deleteExchangeParams{Exchange: "events"},
		"request:test",
	)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(token, "eg2.") {
		t.Fatalf("unexpected token prefix: %q", token)
	}
	payload, err := hex.DecodeString(strings.TrimPrefix(token, "eg2."))
	if err != nil {
		t.Fatal(err)
	}
	var envelope signedEnvelope
	if err := json.Unmarshal(payload, &envelope); err != nil {
		t.Fatal(err)
	}
	if envelope.Context.AgentID != context.AgentID || envelope.IdempotencyKey != "request:test" {
		t.Fatal("envelope did not preserve its bound context and idempotency key")
	}
	if strings.Contains(token, "test-envelope-secret") {
		t.Fatal("authentication secret leaked into the token")
	}
}

func TestRestoreRequiresExplicitCurrentShardLayout(t *testing.T) {
	client := &Client{}
	if _, err := client.Restore("scheduled-001", 0); err == nil {
		t.Fatal("zero target shards were accepted")
	}
	if _, err := client.Restore("scheduled-001", 65); err == nil {
		t.Fatal("target shard count above the current bound was accepted")
	}
}

func TestValidateResourceSelector(t *testing.T) {
	tests := []struct {
		name    string
		input   any
		want    any
		wantErr string
	}{
		{name: "all", input: "All", want: "All"},
		{name: "pattern", input: map[string]string{"Pattern": "orders:*"}, want: map[string]string{"Pattern": "orders:*"}},
		{name: "msgpack map", input: map[string]any{"Label": "sensitive"}, want: map[string]string{"Label": "sensitive"}},
		{name: "wrong string", input: "all", wantErr: "resource selector string must be All"},
		{name: "nil", input: nil, wantErr: "resource selector must be All or one named selector"},
		{name: "multiple entries", input: map[string]string{"Pattern": "a", "Graph": "b"}, wantErr: "resource selector must contain exactly one entry"},
		{name: "unsupported key", input: map[string]string{"Other": "value"}, wantErr: "unsupported resource selector"},
		{name: "empty value", input: map[string]string{"Graph": "  "}, wantErr: "resource selector value must be non-empty"},
		{name: "non-string value", input: map[string]any{"Graph": 42}, wantErr: "resource selector value must be a string"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := validateResourceSelector(test.input)
			if test.wantErr != "" {
				if err == nil || err.Error() != test.wantErr {
					t.Fatalf("validateResourceSelector(%#v) error = %v, want %q", test.input, err, test.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("validateResourceSelector(%#v) returned error: %v", test.input, err)
			}
			if !reflect.DeepEqual(got, test.want) {
				t.Fatalf("validateResourceSelector(%#v) = %#v, want %#v", test.input, got, test.want)
			}
		})
	}
}
