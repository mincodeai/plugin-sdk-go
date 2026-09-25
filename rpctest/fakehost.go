package rpctest

import (
	"context"
	"encoding/json"
	"sort"
	"sync"

	"github.com/mincodeai/plugin-sdk-go/host"
)

// FakeHost is an in-memory host.Caller implementing host.storage.* and
// host.secrets.* for handler unit tests (wrap it with host.WithCaller).
// Results follow the desktop host: get → value or null, set/delete → true,
// list → sorted key array.
type FakeHost struct {
	mu      sync.Mutex
	Storage map[string]json.RawMessage
	Secrets map[string]string
	// Fail, when set, is consulted first; a non-nil error is returned
	// instead of performing the call (e.g. &host.Error{Code: -32001}).
	Fail func(method string, params json.RawMessage) error
	// Calls records every call as "method params".
	Calls []string
}

// NewFakeHost returns an empty FakeHost.
func NewFakeHost() *FakeHost {
	return &FakeHost{Storage: map[string]json.RawMessage{}, Secrets: map[string]string{}}
}

// Context returns ctx carrying f as the host caller.
func (f *FakeHost) Context(ctx context.Context) context.Context { return host.WithCaller(ctx, f) }

// Call implements host.Caller.
func (f *FakeHost) Call(ctx context.Context, method string, params any) (json.RawMessage, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	raw, err := json.Marshal(params)
	if err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.Calls = append(f.Calls, method+" "+string(raw))
	if f.Fail != nil {
		if err := f.Fail(method, raw); err != nil {
			return nil, err
		}
	}
	var p struct {
		Key   string          `json:"key"`
		Value json.RawMessage `json:"value"`
	}
	_ = json.Unmarshal(raw, &p)
	switch method {
	case "host.storage.get":
		if v, ok := f.Storage[p.Key]; ok {
			return v, nil
		}
		return json.RawMessage("null"), nil
	case "host.storage.set":
		f.Storage[p.Key] = append(json.RawMessage(nil), p.Value...)
		return json.RawMessage("true"), nil
	case "host.storage.delete":
		delete(f.Storage, p.Key)
		return json.RawMessage("true"), nil
	case "host.storage.list":
		return sortedKeys(f.Storage), nil
	case "host.secrets.get":
		if v, ok := f.Secrets[p.Key]; ok {
			return json.Marshal(v)
		}
		return json.RawMessage("null"), nil
	case "host.secrets.set":
		var v string
		if json.Unmarshal(p.Value, &v) != nil {
			return nil, &host.Error{Code: -32602, Message: "secret value must be a string"}
		}
		f.Secrets[p.Key] = v
		return json.RawMessage("true"), nil
	case "host.secrets.delete":
		delete(f.Secrets, p.Key)
		return json.RawMessage("true"), nil
	case "host.secrets.list":
		return sortedKeys(f.Secrets), nil
	}
	return nil, &host.Error{Code: -32601, Message: "unknown host method " + method}
}

func sortedKeys[V any](m map[string]V) json.RawMessage {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	b, _ := json.Marshal(keys)
	return b
}
