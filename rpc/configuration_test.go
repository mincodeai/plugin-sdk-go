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

func TestConfigurationChangedNegotiation(t *testing.T) {
	changed := make(chan []string, 2)
	o := rpc.StrictDefaults("x")
	o.Handler = func(context.Context, string, json.RawMessage) (any, error) { return nil, nil }
	o.OnConfigurationChanged = func(_ context.Context, keys []string, h host.Caller) {
		if h == nil {
			t.Error("caller must be set")
		}
		changed <- keys
	}
	p := startPipe(t, o)
	if reply := p.call(`{"jsonrpc":"2.0","id":0,"method":"plugin.configuration.changed","params":{"keys":["a"]}}`); !strings.Contains(reply, "-32601") {
		t.Fatalf("before initialize: %s", reply)
	}
	reply := p.call(`{"jsonrpc":"2.0","id":1,"method":"plugin.initialize","params":{"features":{"tasks":1,"configuration":1}}}`)
	if !strings.Contains(reply, `"configuration":1`) || strings.Contains(reply, `"tasks"`) {
		t.Fatalf("initialize: %s", reply)
	}
	if reply := p.call(`{"jsonrpc":"2.0","id":2,"method":"plugin.configuration.changed","params":{"keys":["endpoint","token"]}}`); !strings.Contains(reply, `"result":{}`) {
		t.Fatalf("changed reply: %s", reply)
	}
	select {
	case keys := <-changed:
		if strings.Join(keys, ",") != "endpoint,token" {
			t.Fatalf("keys: %v", keys)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("callback did not run")
	}
}

func TestConfigurationUnchangedWithoutCallback(t *testing.T) {
	o := rpc.StrictDefaults("x")
	o.Handler = func(context.Context, string, json.RawMessage) (any, error) { return nil, nil }
	p := startPipe(t, o)
	reply := p.call(`{"jsonrpc":"2.0","id":1,"method":"plugin.initialize","params":{"features":{"configuration":1}}}`)
	if strings.Contains(reply, "configuration") {
		t.Fatalf("initialize must not advertise configuration: %s", reply)
	}
	if reply := p.call(`{"jsonrpc":"2.0","id":2,"method":"plugin.configuration.changed","params":{"keys":[]}}`); !strings.Contains(reply, "-32601") {
		t.Fatalf("changed without feature: %s", reply)
	}
}
