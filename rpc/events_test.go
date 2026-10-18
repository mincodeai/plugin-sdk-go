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

func TestHostEventsSubscribeAndDispatch(t *testing.T) {
	type delivered struct {
		event string
		data  string
	}
	events := make(chan delivered, 2)
	o := rpc.StrictDefaults("x")
	o.Handler = func(context.Context, string, json.RawMessage) (any, error) { return nil, nil }
	o.Events = []string{host.EventWorkspaceChanged, host.EventFileSaved}
	o.OnEvent = func(_ context.Context, event string, data json.RawMessage, h host.Caller) {
		if h == nil {
			t.Error("caller must be set")
		}
		events <- delivered{event, string(data)}
	}
	p := startPipe(t, o)
	if reply := p.call(`{"jsonrpc":"2.0","id":0,"method":"plugin.event","params":{"event":"workspace.changed","data":{}}}`); !strings.Contains(reply, "-32601") {
		t.Fatalf("before initialize: %s", reply)
	}
	if reply := p.call(`{"jsonrpc":"2.0","id":1,"method":"plugin.initialize","params":{}}`); !strings.Contains(reply, `"result"`) {
		t.Fatalf("initialize: %s", reply)
	}
	if !p.out.Scan() {
		t.Fatal("no subscription request")
	}
	var request struct {
		ID     string `json:"id"`
		Method string `json:"method"`
		Params struct {
			Events []string `json:"events"`
		} `json:"params"`
	}
	if err := json.Unmarshal(p.out.Bytes(), &request); err != nil || request.Method != "host.events.subscribe" || strings.Join(request.Params.Events, ",") != "workspace.changed,editor.fileSaved" {
		t.Fatalf("subscription request: %s %v", p.out.Text(), err)
	}
	if reply := p.call(`{"jsonrpc":"2.0","id":2,"method":"plugin.event","params":{"event":"editor.fileSaved","data":{"path":"a.go"}}}`); !strings.Contains(reply, `"result":{}`) {
		t.Fatalf("event reply: %s", reply)
	}
	select {
	case got := <-events:
		if got.event != "editor.fileSaved" || got.data != `{"path":"a.go"}` {
			t.Fatalf("delivered: %+v", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("OnEvent did not run")
	}
	if reply := p.call(`{"jsonrpc":"2.0","id":3,"method":"plugin.event","params":{}}`); !strings.Contains(reply, "-32602") {
		t.Fatalf("invalid event: %s", reply)
	}
}

func TestHostEventsUnknownWithoutCallback(t *testing.T) {
	o := rpc.StrictDefaults("x")
	o.Handler = func(context.Context, string, json.RawMessage) (any, error) { return nil, nil }
	p := startPipe(t, o)
	p.call(`{"jsonrpc":"2.0","id":1,"method":"plugin.initialize","params":{}}`)
	if reply := p.call(`{"jsonrpc":"2.0","id":2,"method":"plugin.event","params":{"event":"workspace.changed"}}`); !strings.Contains(reply, "-32601") {
		t.Fatalf("event without OnEvent: %s", reply)
	}
}
