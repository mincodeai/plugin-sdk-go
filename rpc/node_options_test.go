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

	"github.com/mincodeai/plugin-sdk-go/host"
	"github.com/mincodeai/plugin-sdk-go/rpc"
)

func nodeServe(t *testing.T, o rpc.Options, in io.Reader) string {
	t.Helper()
	var out bytes.Buffer
	if err := rpc.Serve(context.Background(), in, &out, io.Discard, o); err != nil {
		t.Fatal(err)
	}
	return out.String()
}

func TestErrorDataStyles(t *testing.T) {
	for style, want := range map[rpc.Style]string{
		rpc.StyleGo:     `{"jsonrpc":"2.0","id":1,"error":{"code":-32000,"message":"m","data":{"kind":"K","retryable":true}}}`,
		rpc.StyleSorted: `{"error":{"code":-32000,"message":"m","data":{"kind":"K","retryable":true}},"id":1,"jsonrpc":"2.0"}`,
		rpc.StyleJS:     `{"jsonrpc":"2.0","id":1,"error":{"code":-32000,"message":"m","data":{"kind":"K","retryable":true}}}`,
	} {
		o := rpc.NodeDefaults()
		o.Style = style
		o.DrainReplies = true // EOF right after the invoke
		o.Handler = func(context.Context, string, json.RawMessage) (any, error) {
			return nil, &rpc.Error{Code: -32000, Message: "m", Data: json.RawMessage(`{"kind":"K","retryable":true}`)}
		}
		got := nodeServe(t, o, strings.NewReader(`{"id":1,"method":"plugin.invoke","params":{"action":"a"}}`+"\n"))
		if got != want+"\n" {
			t.Errorf("style %d: got %s", style, got)
		}
	}
}

func TestMethodTypeTruthyAndHostReplyError(t *testing.T) {
	o := rpc.NodeDefaults()
	o.NonStringMethod = rpc.MethodTypeTruthy
	o.HostReplyError = func(raw json.RawMessage) *host.WireError {
		if len(raw) == 0 || string(raw) == "false" || string(raw) == "null" {
			return nil
		}
		return &host.WireError{Code: -1, Message: "custom", Data: raw}
	}
	o.Handler = func(ctx context.Context, action string, _ json.RawMessage) (any, error) {
		r, err := host.FromContext(ctx).Call(ctx, "host.x", map[string]any{})
		if err != nil {
			he := err.(*host.Error)
			return map[string]any{"code": he.Code, "msg": he.Message, "data": he.Data}, nil
		}
		return r, nil
	}
	inR, inW := io.Pipe()
	var out syncBuffer
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = rpc.Serve(context.Background(), inR, &out, io.Discard, o)
	}()
	write := func(line string) { _, _ = io.WriteString(inW, line+"\n") }
	wait := func(s string) {
		for i := 0; i < 200 && !strings.Contains(out.String(), s); i++ {
			time.Sleep(5 * time.Millisecond)
		}
	}
	write(`{"id":7,"method":0}`)
	write(`{"id":8,"method":true}`)
	write(`{"id":1,"method":"plugin.invoke","params":{"action":"a"}}`)
	wait(`"id":"host-1"`)
	write(`{"id":"host-1","method":false,"error":false,"result":5}`)
	wait(`"id":1,`)
	write(`{"id":2,"method":"plugin.invoke","params":{"action":"a"}}`)
	wait(`"id":"host-2"`)
	write(`{"id":"host-2","method":-0,"error":"boom"}`)
	wait(`"id":2,`)
	inW.Close()
	<-done
	got := out.String()
	for _, want := range []string{
		`{"jsonrpc":"2.0","id":8,"error":{"code":-32601,"message":"不支持的方法"}}`,
		`{"jsonrpc":"2.0","id":1,"result":5}`,
		`{"jsonrpc":"2.0","id":2,"result":{"code":-1,"data":"boom","msg":"custom"}}`,
	} {
		if !strings.Contains(got, want+"\n") {
			t.Errorf("missing %s in\n%s", want, got)
		}
	}
	if strings.Contains(got, `"id":7`) {
		t.Errorf("falsy method answered:\n%s", got)
	}
}

func TestTaskFlagOnlyCancelClipSnapshotJS(t *testing.T) {
	release := make(chan struct{})
	o := rpc.NodeDefaults()
	o.Style = rpc.StyleJS
	o.Tasks = &rpc.TaskOptions{SharedGate: true, FlagOnlyCancel: true, SnapshotJS: true,
		ClipError: func(m string) string { return "clipped:" + string([]rune(m)[:2]) }}
	o.Handler = func(ctx context.Context, _ string, _ json.RawMessage) (any, error) {
		<-release
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, rpc.NewError(-32000, "a bcdef")
	}
	inR, inW := io.Pipe()
	var out syncBuffer
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = rpc.Serve(context.Background(), inR, &out, io.Discard, o)
	}()
	step := func(line, want string) {
		t.Helper()
		_, _ = io.WriteString(inW, line+"\n")
		for i := 0; i < 200 && !strings.Contains(out.String(), want); i++ {
			time.Sleep(5 * time.Millisecond)
		}
		if !strings.Contains(out.String(), want) {
			t.Fatalf("missing %s in\n%s", want, out.String())
		}
	}
	step(`{"id":0,"method":"plugin.initialize","params":{"features":{"tasks":1}}}`, `"id":0`)
	step(`{"id":1,"method":"plugin.task.start","params":{"taskId":"t","action":"a"}}`, `"id":1,"result":{"taskId":"t","state":"running","cancelRequested":false}`)
	step(`{"id":2,"method":"plugin.task.cancel","params":{"taskId":"t"}}`, `"id":2,"result":{"taskId":"t","state":"running","cancelRequested":true}`)
	close(release)
	want := "\"id\":3,\"result\":{\"taskId\":\"t\",\"state\":\"failed\",\"cancelRequested\":true,\"error\":\"clipped:a \"}"
	for i := 0; i < 200 && !strings.Contains(out.String(), want); i++ {
		_, _ = io.WriteString(inW, `{"id":3,"method":"plugin.task.status","params":{"taskId":"t"}}`+"\n")
		time.Sleep(5 * time.Millisecond)
	}
	inW.Close()
	<-done
	if !strings.Contains(out.String(), want) {
		t.Fatalf("missing %s in\n%s", want, out.String())
	}
}

type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}
