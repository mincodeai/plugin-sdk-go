package rpc_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/mincodeai/plugin-sdk-go/rpc"
)

func TestUIMessagesAreDeliveredInOrderAndAnswered(t *testing.T) {
	type delivered struct {
		viewID  string
		message string
	}
	messages := make(chan delivered, 8)
	o := rpc.StrictDefaults("x")
	o.Handler = func(context.Context, string, json.RawMessage) (any, error) { return nil, nil }
	o.OnUIMessage = func(ctx context.Context, viewID string, message json.RawMessage) {
		messages <- delivered{viewID, string(message)}
		if err := rpc.PostUIMessage(ctx, map[string]string{"reply": viewID}, viewID); err != nil {
			t.Error(err)
		}
	}
	const posted = `{"jsonrpc":"2.0","method":"host.ui.postMessage","params":{"message":{"reply":"main"},"viewId":"main"}}`
	// Delivery runs after the reply, so a post from the previous message may come
	// first; read past those to the reply and count them.
	p := startPipe(t, o)
	posts := 0
	nextLine := func() string {
		if !p.out.Scan() {
			t.Fatal("no output")
		}
		return p.out.Text()
	}
	for i, message := range []string{`{"seq":1}`, `"two"`} {
		line := `{"jsonrpc":"2.0","id":` + string(rune('1'+i)) + `,"method":"plugin.ui.message","params":{"viewId":"main","message":` + message + `}}`
		reply := p.call(line)
		for reply == posted {
			posts++
			reply = nextLine()
		}
		if !strings.Contains(reply, `"result":{}`) {
			t.Fatalf("message reply: %s", reply)
		}
	}
	for i, want := range []string{`{"seq":1}`, `"two"`} {
		select {
		case got := <-messages:
			if got.viewID != "main" || got.message != want {
				t.Fatalf("delivered %+v, want %s", got, want)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("OnUIMessage did not run")
		}
		// The pipe is unbuffered: read this message's post so the next one can run.
		if posts <= i {
			if line := nextLine(); line != posted {
				t.Fatalf("posted message: %s", line)
			}
			posts++
		}
	}
	if reply := p.call(`{"jsonrpc":"2.0","id":9,"method":"plugin.ui.message","params":{"viewId":"main"}}`); !strings.Contains(reply, "-32602") {
		t.Fatalf("missing message: %s", reply)
	}
}

func TestUIMessagesWithoutHandlerAreUnknown(t *testing.T) {
	o := rpc.StrictDefaults("x")
	o.Handler = func(context.Context, string, json.RawMessage) (any, error) { return nil, nil }
	p := startPipe(t, o)
	if reply := p.call(`{"jsonrpc":"2.0","id":1,"method":"plugin.ui.message","params":{"viewId":"main","message":1}}`); !strings.Contains(reply, "-32601") {
		t.Fatalf("reply without OnUIMessage: %s", reply)
	}
}

func TestPostUIMessageValidates(t *testing.T) {
	if err := rpc.PostUIMessage(context.Background(), 1, ""); !errors.Is(err, rpc.ErrUIMessageUnavailable) {
		t.Fatalf("outside a server: %v", err)
	}
	posted := make(chan error, 3)
	o := rpc.StrictDefaults("x")
	o.Handler = func(context.Context, string, json.RawMessage) (any, error) { return nil, nil }
	o.OnUIMessage = func(ctx context.Context, _ string, _ json.RawMessage) {
		posted <- rpc.PostUIMessage(ctx, strings.Repeat("x", rpc.MaxUIMessageBytes), "")
		posted <- rpc.PostUIMessage(ctx, nil, "")
		posted <- rpc.PostUIMessage(ctx, 1, "../view")
	}
	p := startPipe(t, o)
	p.call(`{"jsonrpc":"2.0","id":1,"method":"plugin.ui.message","params":{"viewId":"main","message":1}}`)
	for _, want := range []error{rpc.ErrUIMessageTooLarge, rpc.ErrUIMessageInvalid, rpc.ErrUIMessageInvalid} {
		if err := <-posted; !errors.Is(err, want) {
			t.Fatalf("error = %v, want %v", err, want)
		}
	}
}
