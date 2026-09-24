package epgthin

// The engine MACs the canonical body it re-derives from the request it decoded.
// contract/fixtures/method_body_vectors.json is rendered by gen_contract from the
// engine's own decoder, encoder and envelope MAC: one client-shaped request per
// catalog method (alphabetical keys, defaults omitted) plus every typed contract
// sample, each with its golden body digest and the MAC the engine computes under
// one published envelope. Replaying each request through this client's signer must
// reproduce the engine's bytes and MAC exactly.

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"strings"
	"testing"

	"github.com/vmihailenco/msgpack/v5"
)

const contractRoot = "../../contract/"

type bodyVectorEnvelope struct {
	Secret         string               `json:"secret"`
	RequestID      int64                `json:"request_id"`
	Graph          string               `json:"graph"`
	Context        RequestContextClaims `json:"context"`
	Timestamp      uint64               `json:"timestamp"`
	Nonce          string               `json:"nonce"`
	IdempotencyKey string               `json:"idempotency_key"`
}

type bodyVector struct {
	Label           string `json:"label"`
	Method          string `json:"method"`
	RequestMsgpack  string `json:"request_msgpack"`
	CanonicalSHA256 string `json:"canonical_sha256"`
	CanonicalLen    int    `json:"canonical_len"`
	MAC             string `json:"mac"`
}

type bodyVectorFile struct {
	Envelope bodyVectorEnvelope `json:"envelope"`
	Vectors  []bodyVector       `json:"vectors"`
}

func readContractJSON(t *testing.T, name string, into any) {
	t.Helper()
	raw, err := os.ReadFile(contractRoot + name)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(raw, into); err != nil {
		t.Fatalf("%s: %v", name, err)
	}
}

func loadBodyVectors(t *testing.T) bodyVectorFile {
	t.Helper()
	var file bodyVectorFile
	readContractJSON(t, "fixtures/method_body_vectors.json", &file)
	if len(file.Vectors) == 0 || file.Envelope.Secret == "" {
		t.Fatal("the method-body vector file carries no vectors or no envelope")
	}
	return file
}

// vectorCall splits a vector's {method, params?} request into what a caller hands
// the signer: the params exactly as encoded; a unit method has none.
func vectorCall(t *testing.T, vector bodyVector) (string, any) {
	t.Helper()
	raw, err := hex.DecodeString(vector.RequestMsgpack)
	if err != nil {
		t.Fatal(err)
	}
	var request map[string]msgpack.RawMessage
	if err := msgpack.Unmarshal(raw, &request); err != nil {
		t.Fatal(err)
	}
	var method string
	if err := msgpack.Unmarshal(request["method"], &method); err != nil {
		t.Fatal(err)
	}
	params, ok := request["params"]
	if !ok {
		return method, nil
	}
	return method, params
}

func tokenMAC(t *testing.T, token string) string {
	t.Helper()
	payload, err := hex.DecodeString(strings.TrimPrefix(token, "eg2."))
	if err != nil {
		t.Fatal(err)
	}
	var envelope signedEnvelope
	if err := json.Unmarshal(payload, &envelope); err != nil {
		t.Fatal(err)
	}
	return envelope.MAC
}

func TestVectorsCoverEveryPublishedMethod(t *testing.T) {
	var catalog struct {
		Methods []struct {
			ID string `json:"id"`
		} `json:"methods"`
	}
	readContractJSON(t, "methods.json", &catalog)
	covered := map[string]bool{}
	for _, vector := range loadBodyVectors(t).Vectors {
		covered[vector.Method] = true
	}
	for _, method := range catalog.Methods {
		if !covered[method.ID] {
			t.Errorf("no method-body vector covers %s", method.ID)
		}
	}
}

func TestSignerMatchesTheEngineForEveryMethodBodyVector(t *testing.T) {
	file := loadBodyVectors(t)
	envelope := file.Envelope
	context, err := validateRequestContext(&envelope.Context)
	if err != nil {
		t.Fatal(err)
	}
	client := &Client{authSecret: envelope.Secret, context: context}
	seal := envelopeSeal{Timestamp: envelope.Timestamp, Nonce: envelope.Nonce}
	for _, vector := range file.Vectors {
		t.Run(vector.Label, func(t *testing.T) {
			method, params := vectorCall(t, vector)
			body, err := canonicalMethodBody(method, params)
			if err != nil {
				t.Fatal(err)
			}
			digest := sha256.Sum256(body)
			if got := hex.EncodeToString(digest[:]); got != vector.CanonicalSHA256 || len(body) != vector.CanonicalLen {
				t.Fatalf("signed body sha256 %s (%d bytes), engine body %s (%d bytes)",
					got, len(body), vector.CanonicalSHA256, vector.CanonicalLen)
			}
			token, err := client.seal(envelope.RequestID, envelope.Graph, method, body, envelope.IdempotencyKey, seal)
			if err != nil {
				t.Fatal(err)
			}
			if mac := tokenMAC(t, token); mac != vector.MAC {
				t.Fatalf("envelope MAC %s, engine MAC %s", mac, vector.MAC)
			}
		})
	}
}

func TestSignerRefusesARequestTheEngineCannotDecode(t *testing.T) {
	_, err := canonicalMethodBody("CancelRequest", map[string]any{"target_req_id": "seven"})
	if err == nil || !strings.HasPrefix(err.Error(), "request is not a valid engine request: ") {
		t.Fatalf("an undecodable request was signed: %v", err)
	}
	if _, err := canonicalMethodBody("CancelRequest", map[string]any{"target_req_id": 7}); err != nil {
		t.Fatalf("the codec did not recover after a refusal: %v", err)
	}
}
