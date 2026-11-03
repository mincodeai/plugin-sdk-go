package rpc_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/mincodeai/plugin-sdk-go/host"
	"github.com/mincodeai/plugin-sdk-go/rpc"
)

func TestGlobalShortcutActivationHandler(t *testing.T) {
	type activation struct{ id, token string }
	got := make(chan activation, 2)
	o := rpc.StrictDefaults("x")
	o.Handler = func(context.Context, string, json.RawMessage) (any, error) { return nil, nil }
	o.OnGlobalShortcutActivation = func(_ context.Context, id, token string, h host.Caller) {
		if h == nil {
			t.Error("caller must be set")
		}
		got <- activation{id, token}
	}
	p := startPipe(t, o)
	if reply := p.call(`{"jsonrpc":"2.0","id":0,"method":"plugin.globalShortcut.activation","params":{"id":"capture","token":"t","expiresAt":1}}`); !strings.Contains(reply, "-32601") {
		t.Fatalf("before initialize: %s", reply)
	}
	p.call(`{"jsonrpc":"2.0","id":1,"method":"plugin.initialize","params":{}}`)
	if reply := p.call(`{"jsonrpc":"2.0","id":2,"method":"plugin.globalShortcut.activation","params":{"id":"capture","token":"0123456789abcdef0123456789abcdef","expiresAt":1800000120000}}`); !strings.Contains(reply, `"result":{}`) {
		t.Fatalf("activation reply: %s", reply)
	}
	select {
	case a := <-got:
		if a.id != "capture" || a.token != "0123456789abcdef0123456789abcdef" {
			t.Fatalf("activation %+v", a)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("callback did not run")
	}
	for _, params := range []string{`{}`, `{"id":"capture"}`, `{"token":"t"}`, `[]`} {
		if reply := p.call(`{"jsonrpc":"2.0","id":3,"method":"plugin.globalShortcut.activation","params":` + params + `}`); !strings.Contains(reply, "-32602") {
			t.Fatalf("params %s: %s", params, reply)
		}
	}
}

func TestGlobalShortcutActivationWithoutHandler(t *testing.T) {
	o := rpc.StrictDefaults("x")
	o.Handler = func(context.Context, string, json.RawMessage) (any, error) { return nil, nil }
	p := startPipe(t, o)
	p.call(`{"jsonrpc":"2.0","id":1,"method":"plugin.initialize","params":{}}`)
	if reply := p.call(`{"jsonrpc":"2.0","id":2,"method":"plugin.globalShortcut.activation","params":{"id":"capture","token":"t","expiresAt":1}}`); !strings.Contains(reply, "-32601") {
		t.Fatalf("without handler: %s", reply)
	}
}
