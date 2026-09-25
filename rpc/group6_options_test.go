package rpc_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mincodeai/plugin-sdk-go/rpc"
)

func group6Options(h rpc.Handler) rpc.Options {
	o := rpc.NodeDefaults()
	o.StructParams, o.StrictPayload = true, true
	o.MaxInflight = 8
	o.Handler = h
	return o
}

func serveString(t *testing.T, o rpc.Options, in string) (string, error) {
	t.Helper()
	var out bytes.Buffer
	err := rpc.Serve(context.Background(), strings.NewReader(in), &out, nil, o)
	return out.String(), err
}

func TestFatalLineBytes(t *testing.T) {
	o := group6Options(func(context.Context, string, json.RawMessage) (any, error) { return nil, nil })
	o.MaxLineBytes, o.FatalLineBytes = 50, 64
	o.DrainReplies = true
	ping := `{"jsonrpc":"2.0","id":1,"method":"plugin.ping"}` + "\n"
	// Over MaxLineBytes but within the scanner buffer: invalid, the loop goes on.
	got, err := serveString(t, o, strings.Repeat("x", 63)+"\n"+ping)
	if err != nil || !strings.Contains(got, `"id":1`) {
		t.Fatalf("got %q, %v", got, err)
	}
	// A line (plus newline) that does not fit the scanner buffer is fatal.
	if _, err := serveString(t, o, strings.Repeat("x", 64)+"\n"+ping); err == nil {
		t.Fatal("want fatal error")
	}
}

func TestMaxReplyBytesAndStrictPayload(t *testing.T) {
	o := group6Options(func(_ context.Context, _ string, p json.RawMessage) (any, error) {
		return json.RawMessage(`{"echo":` + string(p) + `}`), nil
	})
	o.MaxReplyBytes = 110
	// EOF follows the input at once: drain so the async invoke replies are written.
	o.DrainReplies = true
	o.ResultTooLarge = rpc.NewError(rpc.CodeServerError, "big")
	in := strings.Join([]string{
		`{"jsonrpc":"2.0","id":1,"method":"plugin.invoke","params":{"ACTION":"a","Payload":{}}}`,
		`{"jsonrpc":"2.0","id":2,"method":"plugin.invoke","params":{"action":"a","payload":{"k":"01234567890123456789012345678901234567890123456789012345678901234567890123456789"}}}`,
		`{"jsonrpc":"2.0","id":3,"method":"plugin.invoke","params":{"action":"a","payload":{"a":1,"a":2}}}`,
		`{"jsonrpc":"2.0","id":4,"method":"plugin.invoke","params":{"action":"a","payload":"[1,1]"}}`,
		`{"jsonrpc":"2.0","id":5,"method":"plugin.invoke","params":{"action":"a","payload":"[{\"a\":1,\"a\":2}]"}}`,
	}, "\n") + "\n"
	got, err := serveString(t, o, in)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		`{"jsonrpc":"2.0","id":1,"result":{"echo":{}}}`,
		`{"jsonrpc":"2.0","id":2,"error":{"code":-32000,"message":"big"}}`,
		`{"jsonrpc":"2.0","id":3,"error":{"code":-32602,"message":"Payload must contain valid JSON"}}`,
		`{"jsonrpc":"2.0","id":4,"error":{"code":-32602,"message":"参数必须是对象"}}`,
		`{"jsonrpc":"2.0","id":5,"error":{"code":-32602,"message":"Payload must contain valid JSON"}}`,
	} {
		if !strings.Contains(got, want+"\n") {
			t.Errorf("missing %s in\n%s", want, got)
		}
	}
}

func TestShutdownReplyLastAndClearProgress(t *testing.T) {
	release := make(chan struct{})
	o := group6Options(func(ctx context.Context, action string, _ json.RawMessage) (any, error) {
		rpc.ReportProgress(ctx, map[string]int{"done": 1})
		if action == "slow" {
			<-ctx.Done()
			<-release
			return nil, ctx.Err()
		}
		return map[string]bool{"ok": true}, nil
	})
	o.ShutdownReplyLast = true
	o.Tasks = &rpc.TaskOptions{CheckPayload: true, ClearProgressOnSuccess: true}
	pr, pw := io.Pipe()
	var out g6Buffer
	done := make(chan error, 1)
	go func() { done <- rpc.Serve(context.Background(), pr, &out, nil, o) }()
	write := func(s string) { _, _ = pw.Write([]byte(s + "\n")) }
	write(`{"jsonrpc":"2.0","id":1,"method":"plugin.initialize","params":{"features":{"tasks":1}}}`)
	write(`{"jsonrpc":"2.0","id":2,"method":"plugin.task.start","params":{"taskId":"t","action":"fast","payload":{}}}`)
	time.Sleep(100 * time.Millisecond)
	write(`{"jsonrpc":"2.0","id":3,"method":"plugin.task.status","params":{"taskId":"t"}}`)
	write(`{"jsonrpc":"2.0","id":4,"method":"plugin.invoke","params":{"action":"slow"}}`)
	time.Sleep(50 * time.Millisecond)
	write(`{"jsonrpc":"2.0","id":5,"method":"plugin.shutdown"}`)
	time.Sleep(100 * time.Millisecond)
	if strings.Contains(out.String(), `"id":5`) {
		t.Fatal("shutdown replied before in-flight work drained")
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	got := out.String()
	if !strings.Contains(got, `{"jsonrpc":"2.0","id":3,"result":{"taskId":"t","state":"succeeded","cancelRequested":false,"result":{"ok":true}}}`) {
		t.Fatalf("progress not cleared:\n%s", got)
	}
	if !strings.HasSuffix(got, `{"jsonrpc":"2.0","id":5,"result":{}}`+"\n") || strings.Contains(got, `"id":4`) {
		t.Fatalf("shutdown order:\n%s", got)
	}
}

// g6Buffer is a bytes.Buffer safe for the Serve goroutine and the test.
type g6Buffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (b *g6Buffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.Write(p)
}

func (b *g6Buffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.String()
}
