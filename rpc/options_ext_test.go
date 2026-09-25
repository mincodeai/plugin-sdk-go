package rpc_test

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mincodeai/plugin-sdk-go/host"
	"github.com/mincodeai/plugin-sdk-go/rpc"
)

type pipeRPC struct {
	t    *testing.T
	in   *io.PipeWriter
	out  *bufio.Scanner
	done chan error
}

func startPipe(t *testing.T, o rpc.Options) *pipeRPC {
	inR, inW := io.Pipe()
	outR, outW := io.Pipe()
	p := &pipeRPC{t: t, in: inW, out: bufio.NewScanner(outR), done: make(chan error, 1)}
	go func() { p.done <- rpc.Serve(context.Background(), inR, outW, io.Discard, o); outW.Close() }()
	t.Cleanup(func() { inW.Close(); outR.Close() })
	return p
}

func (p *pipeRPC) call(line string) string {
	p.t.Helper()
	if _, err := io.WriteString(p.in, line+"\n"); err != nil {
		p.t.Fatal(err)
	}
	if !p.out.Scan() {
		p.t.Fatal("no reply")
	}
	return p.out.Text()
}

func (p *pipeRPC) waitState(id, state string) string {
	p.t.Helper()
	for i := 0; i < 200; i++ {
		r := p.call(`{"jsonrpc":"2.0","id":0,"method":"plugin.task.status","params":{"taskId":"` + id + `"}}`)
		if strings.Contains(r, `"state":"`+state+`"`) {
			return r
		}
		time.Sleep(5 * time.Millisecond)
	}
	p.t.Fatalf("task %s never reached %s", id, state)
	return ""
}

func TestAdmitStartTaskResultAndWaitAll(t *testing.T) {
	var gate atomic.Bool
	release := make(chan struct{})
	var closedAfterWork atomic.Bool
	var workDone atomic.Bool
	o := rpc.StrictDefaults("x")
	o.MaxInflight = 8
	o.WaitAll = true
	o.DrainTimeout = time.Millisecond
	o.OnClose = func() { closedAfterWork.Store(workDone.Load()) }
	o.Admit = func(action string, _ json.RawMessage) *rpc.Error {
		if action == "gated" && !gate.CompareAndSwap(false, true) {
			return rpc.NewError(rpc.CodeBusy, "gate busy")
		}
		return nil
	}
	o.Handler = func(ctx context.Context, action string, _ json.RawMessage) (any, error) {
		if rpc.InTask(ctx) {
			t.Error("invoke reported as task")
		}
		if action == "gated" {
			<-release
			time.Sleep(20 * time.Millisecond)
			workDone.Store(true)
		}
		return map[string]bool{"ok": true}, nil
	}
	o.Tasks = &rpc.TaskOptions{
		MaxRunning:      8,
		LookupBadParams: "Invalid task params",
		Admit: func(action string, _ json.RawMessage) *rpc.Error {
			if action == "no" {
				return rpc.NewError(rpc.CodeInvalidParams, "not a task")
			}
			return nil
		},
		Start: func(action string, _ json.RawMessage) (rpc.TaskFunc, error) {
			if action == "bad" {
				return nil, errors.New("bad start")
			}
			return func(ctx context.Context) (any, error) {
				if !rpc.InTask(ctx) {
					t.Error("task not reported as task")
				}
				rpc.ReportProgress(ctx, map[string]int{"n": 1})
				<-ctx.Done()
				return &rpc.TaskResult{State: "cancelled", Result: json.RawMessage(`{"partial":1}`), CancelRequested: true}, nil
			}, nil
		},
	}
	p := startPipe(t, o)
	p.call(`{"jsonrpc":"2.0","id":1,"method":"plugin.initialize","params":{"features":{"tasks":1}}}`)
	if r := p.call(`{"jsonrpc":"2.0","id":2,"method":"plugin.task.start","params":{"taskId":"a","action":"no","payload":"{}"}}`); !strings.Contains(r, `"message":"not a task"`) {
		t.Fatal(r)
	}
	if r := p.call(`{"jsonrpc":"2.0","id":3,"method":"plugin.task.start","params":{"taskId":"b","action":"bad","payload":"{}"}}`); !strings.Contains(r, `"state":"failed"`) || !strings.Contains(r, `"error":"bad start"`) {
		t.Fatal(r)
	}
	if r := p.call(`{"jsonrpc":"2.0","id":4,"method":"plugin.task.status","params":[]}`); !strings.Contains(r, `"message":"Invalid task params"`) {
		t.Fatal(r)
	}
	p.call(`{"jsonrpc":"2.0","id":5,"method":"plugin.task.start","params":{"taskId":"c","action":"run","payload":"{}"}}`)
	p.call(`{"jsonrpc":"2.0","id":6,"method":"plugin.task.cancel","params":{"taskId":"c"}}`)
	if r := p.waitState("c", "cancelled"); !strings.Contains(r, `"cancelRequested":true,"progress":{"n":1},"result":{"partial":1}}`) {
		t.Fatal(r)
	}
	// The first gated invoke holds the gate; the second is rejected synchronously.
	if _, err := io.WriteString(p.in, `{"jsonrpc":"2.0","id":7,"method":"plugin.invoke","params":{"action":"gated","payload":"{}"}}`+"\n"); err != nil {
		t.Fatal(err)
	}
	if r := p.call(`{"jsonrpc":"2.0","id":8,"method":"plugin.invoke","params":{"action":"gated","payload":"{}"}}`); !strings.Contains(r, `"id":8,"error":{"code":-32029,"message":"gate busy"}`) {
		t.Fatal(r)
	}
	if r := p.call(`{"jsonrpc":"2.0","id":9,"method":"plugin.shutdown"}`); r != `{"jsonrpc":"2.0","id":9,"result":{}}` {
		t.Fatal(r)
	}
	close(release)
	select {
	case <-p.done:
	case <-time.After(5 * time.Second):
		t.Fatal("serve did not return")
	}
	if !closedAfterWork.Load() {
		t.Fatal("WaitAll: OnClose ran before in-flight work returned")
	}
}

func TestLenientErrorMember(t *testing.T) {
	o := rpc.StrictDefaults("x")
	o.StructEnvelope = true
	o.LenientErrorMember = true
	o.MaxInflight = 4
	o.Host.Timeout = 200 * time.Millisecond
	o.Handler = func(ctx context.Context, action string, _ json.RawMessage) (any, error) {
		raw, err := host.FromContext(ctx).Call(ctx, "host.storage.get", map[string]any{"key": action})
		if err != nil {
			return map[string]string{"err": err.Error()}, nil
		}
		return raw, nil
	}
	p := startPipe(t, o)
	p.call(`{"jsonrpc":"2.0","id":1,"method":"plugin.initialize","params":{}}`)
	// A request's malformed error member is ignored.
	if r := p.call(`{"jsonrpc":"2.0","id":2,"method":"plugin.ping","error":"x"}`); r != `{"jsonrpc":"2.0","id":2,"result":{"runtimeProtocolVersion":1}}` {
		t.Fatalf("ping = %s", r)
	}
	// A reply whose error member does not decode is dropped (the call times out).
	io.WriteString(p.in, `{"jsonrpc":"2.0","id":3,"method":"plugin.invoke","params":{"action":"a","payload":"{}"}}`+"\n")
	p.out.Scan()
	if !strings.Contains(p.out.Text(), `"x-host-1"`) {
		t.Fatalf("host request = %s", p.out.Text())
	}
	io.WriteString(p.in, `{"jsonrpc":"2.0","id":"x-host-1","error":{"code":"bad"}}`+"\n")
	p.out.Scan()
	if r := p.out.Text(); r != `{"jsonrpc":"2.0","id":3,"result":{"err":"context deadline exceeded"}}` {
		t.Fatalf("dropped reply = %s", r)
	}
	// A well-formed error reply is delivered; member names match case-insensitively.
	io.WriteString(p.in, `{"jsonrpc":"2.0","id":4,"method":"plugin.invoke","params":{"action":"b","payload":"{}"}}`+"\n")
	p.out.Scan()
	if r := p.call(`{"jsonrpc":"2.0","ID":"x-host-2","Error":{"code":-32001,"message":"no"}}`); r != `{"jsonrpc":"2.0","id":4,"result":{"err":"host error -32001"}}` {
		t.Fatalf("error reply = %s", r)
	}
}
