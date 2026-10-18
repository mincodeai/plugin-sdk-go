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

func TestOnInitializeRunsOnceWithBackgroundCaller(t *testing.T) {
	calls := make(chan host.Caller, 4)
	o := rpc.StrictDefaults("x")
	o.Handler = func(context.Context, string, json.RawMessage) (any, error) { return nil, nil }
	o.OnInitialize = func(ctx context.Context, h host.Caller) {
		if ctx.Err() != nil {
			t.Error("OnInitialize context must be live")
		}
		calls <- h
	}
	p := startPipe(t, o)
	p.call(`{"jsonrpc":"2.0","id":1,"method":"plugin.initialize","params":{"features":{"tasks":1}}}`)
	var caller host.Caller
	select {
	case caller = <-calls:
	case <-time.After(2 * time.Second):
		t.Fatal("OnInitialize did not run")
	}
	p.call(`{"jsonrpc":"2.0","id":2,"method":"plugin.initialize","params":{}}`)
	select {
	case <-calls:
		t.Fatal("OnInitialize must run only once")
	case <-time.After(50 * time.Millisecond):
	}

	// The caller reaches the host from outside any invoke.
	result := make(chan error, 1)
	go func() {
		_, err := host.PollAITurn(context.Background(), caller, "aiturn-1")
		result <- err
	}()
	if !p.out.Scan() {
		t.Fatal("no host request")
	}
	request := p.out.Text()
	if !strings.Contains(request, `"method":"host.ai.turn.poll"`) || !strings.Contains(request, `"aiturn-1"`) {
		t.Fatalf("unexpected host request %s", request)
	}
	var envelope struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal([]byte(request), &envelope); err != nil {
		t.Fatal(err)
	}
	reply := `{"jsonrpc":"2.0","id":"` + envelope.ID + `","result":{"id":"aiturn-1","status":"completed","done":true,"text":"ok"}}` + "\n"
	if _, err := p.in.Write([]byte(reply)); err != nil {
		t.Fatal(err)
	}
	if err := <-result; err != nil {
		t.Fatal(err)
	}
}
