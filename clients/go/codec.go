package epgthin

import (
	"context"
	_ "embed"
	"fmt"
	"sync"

	"github.com/tetratelabs/wazero"
	"github.com/tetratelabs/wazero/api"
	"github.com/vmihailenco/msgpack/v5"
)

// The engine MACs the canonical body it re-derives from the request it DECODED:
// Rust declaration order, serde defaults materialized, maps sorted, float and byte
// widths per field. Restating that in Go mis-signed most of the contract's
// method-body vectors, so the client runs the engine's own decoder and encoder:
// crates/eg-method-codec compiled to WebAssembly by scripts/build_method_codec_wasm.py
// (CI rebuilds it and byte-compares this copy). The module has no imports.
//
//go:embed eg_method_codec.wasm
var methodCodecModule []byte

// methodBodyCodec is the canonical-body format the module must report; the Python
// wheel's native codec reports the same identity.
const methodBodyCodec = "eg/method-body/v1"

const codecBodyStatus = 0

type methodCodec struct {
	module  api.Module
	reserve api.Function
	body    api.Function
	outPtr  api.Function
	outLen  api.Function
	// trapped is set when a call into the module failed. The module aborts on a
	// panic without unwinding, so its buffers may be left locked: a trapped
	// instance is discarded and the next call instantiates a fresh one.
	trapped bool
}

// codecMu serializes every use of the shared codec (the module has one input and
// one output buffer). The module is compiled once; an instance is replaced only
// after a trap.
var (
	codecMu       sync.Mutex
	codecRuntime  wazero.Runtime
	codecCompiled wazero.CompiledModule
	codecInstance *methodCodec
)

func instantiateMethodCodec(ctx context.Context) (*methodCodec, error) {
	if codecCompiled == nil {
		runtime := wazero.NewRuntime(ctx)
		compiled, err := runtime.CompileModule(ctx, methodCodecModule)
		if err != nil {
			_ = runtime.Close(ctx)
			return nil, fmt.Errorf("compile the method-body codec: %w", err)
		}
		codecRuntime, codecCompiled = runtime, compiled
	}
	module, err := codecRuntime.InstantiateModule(ctx, codecCompiled, wazero.NewModuleConfig().WithName(""))
	if err != nil {
		return nil, fmt.Errorf("instantiate the method-body codec: %w", err)
	}
	codec := &methodCodec{
		module:  module,
		reserve: module.ExportedFunction("eg_input_reserve"),
		body:    module.ExportedFunction("eg_canonical_body"),
		outPtr:  module.ExportedFunction("eg_output_ptr"),
		outLen:  module.ExportedFunction("eg_output_len"),
	}
	identity, err := codec.read(ctx, module.ExportedFunction("eg_codec_ptr"), module.ExportedFunction("eg_codec_len"))
	if err != nil || string(identity) != methodBodyCodec {
		_ = module.Close(ctx)
		return nil, fmt.Errorf("the embedded method-body codec is not %s", methodBodyCodec)
	}
	return codec, nil
}

func (c *methodCodec) call(ctx context.Context, fn api.Function, args ...uint64) (uint64, error) {
	if fn == nil {
		return 0, fmt.Errorf("the embedded method-body codec lacks an export")
	}
	results, err := fn.Call(ctx, args...)
	if err != nil {
		c.trapped = true
		return 0, fmt.Errorf("method-body codec call failed: %w", err)
	}
	return results[0], nil
}

func (c *methodCodec) read(ctx context.Context, ptrFn, lenFn api.Function) ([]byte, error) {
	ptr, err := c.call(ctx, ptrFn)
	if err != nil {
		return nil, err
	}
	size, err := c.call(ctx, lenFn)
	if err != nil {
		return nil, err
	}
	view, ok := c.module.Memory().Read(uint32(ptr), uint32(size))
	if !ok {
		return nil, fmt.Errorf("method-body codec output lies outside its memory")
	}
	return append([]byte(nil), view...), nil
}

// canonicalBody returns the engine's canonical body for one request frame.
func (c *methodCodec) canonicalBody(frame []byte) ([]byte, error) {
	ctx := context.Background()
	ptr, err := c.call(ctx, c.reserve, uint64(len(frame)))
	if err != nil {
		return nil, err
	}
	if !c.module.Memory().Write(uint32(ptr), frame) {
		return nil, fmt.Errorf("request frame does not fit the method-body codec memory")
	}
	status, err := c.call(ctx, c.body)
	if err != nil {
		return nil, err
	}
	output, err := c.read(ctx, c.outPtr, c.outLen)
	if err != nil {
		return nil, err
	}
	if status != codecBodyStatus {
		return nil, fmt.Errorf("request is not a valid engine request: %s", output)
	}
	return output, nil
}

// canonicalMethodBody is the body the engine binds into the eg2. MAC for one
// method call: the request frame this client sends, decoded and re-encoded by the
// engine's own codec. The frame's id, graph and token do not affect it.
func canonicalMethodBody(method string, params any) ([]byte, error) {
	frame, err := msgpack.Marshal(wireRequest{Method: method, Params: params})
	if err != nil {
		return nil, fmt.Errorf("encode %s request: %w", method, err)
	}
	codecMu.Lock()
	defer codecMu.Unlock()
	if codecInstance == nil {
		if codecInstance, err = instantiateMethodCodec(context.Background()); err != nil {
			return nil, err
		}
	}
	body, err := codecInstance.canonicalBody(frame)
	if codecInstance.trapped {
		_ = codecInstance.module.Close(context.Background())
		codecInstance = nil
	}
	return body, err
}
