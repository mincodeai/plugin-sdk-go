package rpc_test

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mincodeai/plugin-sdk-go/rpc"
)

// linePeer writes requests and collects every output line, since replies to
// concurrent invokes and notifications arrive in any order.
type linePeer struct {
	t     *testing.T
	in    *io.PipeWriter
	lines chan string
}

func startLinePeer(t *testing.T, o rpc.Options) *linePeer {
	t.Helper()
	inR, inW := io.Pipe()
	outR, outW := io.Pipe()
	p := &linePeer{t: t, in: inW, lines: make(chan string, 256)}
	go func() { _ = rpc.Serve(context.Background(), inR, outW, io.Discard, o); outW.Close() }()
	go func() {
		scanner := bufio.NewScanner(outR)
		for scanner.Scan() {
			p.lines <- scanner.Text()
		}
		close(p.lines)
	}()
	t.Cleanup(func() { inW.Close(); outR.Close() })
	return p
}

func (p *linePeer) send(line string) {
	p.t.Helper()
	if _, err := io.WriteString(p.in, line+"\n"); err != nil {
		p.t.Fatal(err)
	}
}

func (p *linePeer) next(timeout time.Duration) (string, bool) {
	select {
	case line, ok := <-p.lines:
		return line, ok
	case <-time.After(timeout):
		return "", false
	}
}

// replies reads n lines and indexes them by id.
func (p *linePeer) replies(n int) map[string]string {
	p.t.Helper()
	out := map[string]string{}
	for len(out) < n {
		line, ok := p.next(5 * time.Second)
		if !ok {
			p.t.Fatalf("missing replies; got %v", out)
		}
		var env struct {
			ID json.RawMessage `json:"id"`
		}
		_ = json.Unmarshal([]byte(line), &env)
		out[strings.Trim(string(env.ID), `"`)] = line
	}
	return out
}

func invokeLine(id, action string) string {
	return `{"jsonrpc":"2.0","id":"` + id + `","method":"plugin.invoke","params":{"action":"` + action + `","payload":"{}"}}`
}

func TestConcurrentActionsBypassTheGate(t *testing.T) {
	release := make(chan struct{})
	var running atomic.Int32
	o := rpc.StrictDefaults("x")
	o.RequireInitialize = false
	o.ConcurrentActions = []string{"read"}
	o.MaxConcurrent = 2
	o.Handler = func(_ context.Context, action string, _ json.RawMessage) (any, error) {
		running.Add(1)
		<-release
		return map[string]string{"action": action}, nil
	}
	p := startLinePeer(t, o)
	p.send(invokeLine("w1", "write")) // takes the MaxInflight = 1 gate
	p.send(invokeLine("w2", "write")) // busy: still serial
	busy := p.replies(1)
	if !strings.Contains(busy["w2"], "-32029") {
		t.Fatalf("second write must be busy: %v", busy)
	}
	p.send(invokeLine("r1", "read"))
	p.send(invokeLine("r2", "read"))
	p.send(invokeLine("r3", "read")) // over MaxConcurrent
	over := p.replies(1)
	if !strings.Contains(over["r3"], "-32029") {
		t.Fatalf("third read must exceed MaxConcurrent: %v", over)
	}
	deadline := time.Now().Add(2 * time.Second)
	for running.Load() != 3 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := running.Load(); got != 3 {
		t.Fatalf("write and two reads should run together, running = %d", got)
	}
	close(release)
	done := p.replies(3)
	for _, id := range []string{"w1", "r1", "r2"} {
		if !strings.Contains(done[id], `"result"`) {
			t.Fatalf("%s: %s", id, done[id])
		}
	}
}

func TestConcurrentActionsValidateParamsFirst(t *testing.T) {
	o := rpc.StrictDefaults("x")
	o.RequireInitialize = false
	o.ConcurrentActions = []string{"read"}
	o.KnownAction = func(a string) bool { return a == "read" || a == "write" }
	o.Handler = func(context.Context, string, json.RawMessage) (any, error) { return true, nil }
	p := startLinePeer(t, o)
	p.send(`{"jsonrpc":"2.0","id":"bad","method":"plugin.invoke","params":{"action":1}}`)
	if r := p.replies(1)["bad"]; !strings.Contains(r, "-32602") {
		t.Fatalf("malformed params: %s", r)
	}
	p.send(invokeLine("u", "nope"))
	if r := p.replies(1)["u"]; !strings.Contains(r, "-32601") {
		t.Fatalf("unknown action: %s", r)
	}
}

func TestBusyWaitQueuesInvokes(t *testing.T) {
	var running, peak atomic.Int32
	o := rpc.StrictDefaults("x")
	o.RequireInitialize = false
	o.BusyWait = 2 * time.Second
	o.MaxQueued = 3
	block := make(chan struct{})
	o.Handler = func(_ context.Context, action string, _ json.RawMessage) (any, error) {
		n := running.Add(1)
		defer running.Add(-1)
		for {
			p := peak.Load()
			if n <= p || peak.CompareAndSwap(p, n) {
				break
			}
		}
		if action == "block" {
			<-block
		}
		time.Sleep(5 * time.Millisecond)
		return action, nil
	}
	p := startLinePeer(t, o)
	p.send(invokeLine("a", "block"))
	time.Sleep(20 * time.Millisecond)
	for _, id := range []string{"b", "c", "d"} {
		p.send(invokeLine(id, "fast"))
	}
	p.send(invokeLine("e", "fast")) // MaxQueued reached: busy at once
	if r := p.replies(1)["e"]; !strings.Contains(r, "-32029") {
		t.Fatalf("queue overflow must be busy: %s", r)
	}
	close(block)
	got := p.replies(4)
	for _, id := range []string{"a", "b", "c", "d"} {
		if !strings.Contains(got[id], `"result"`) {
			t.Fatalf("%s: %s", id, got[id])
		}
	}
	if peak.Load() != 1 {
		t.Fatalf("queued invokes must still run one at a time, peak = %d", peak.Load())
	}
}

func TestBusyWaitTimesOut(t *testing.T) {
	o := rpc.StrictDefaults("x")
	o.RequireInitialize = false
	o.BusyWait = 50 * time.Millisecond
	block := make(chan struct{})
	defer close(block)
	o.Handler = func(_ context.Context, action string, _ json.RawMessage) (any, error) {
		if action == "block" {
			<-block
		}
		return action, nil
	}
	p := startLinePeer(t, o)
	p.send(invokeLine("a", "block"))
	time.Sleep(10 * time.Millisecond)
	started := time.Now()
	p.send(invokeLine("b", "fast"))
	if r := p.replies(1)["b"]; !strings.Contains(r, "-32029") {
		t.Fatalf("timed out wait must be busy: %s", r)
	}
	if waited := time.Since(started); waited < 40*time.Millisecond {
		t.Fatalf("busy answered before BusyWait: %v", waited)
	}
}

func TestTaskEventsNotifyChanges(t *testing.T) {
	o := rpc.StrictDefaults("x")
	o.Tasks = &rpc.TaskOptions{TaskEventInterval: 30 * time.Millisecond}
	release := make(chan struct{})
	o.Handler = func(ctx context.Context, _ string, _ json.RawMessage) (any, error) {
		for i := 0; i < 20; i++ {
			rpc.ReportProgress(ctx, i)
		}
		<-release
		return "done", nil
	}
	p := startLinePeer(t, o)
	p.send(`{"jsonrpc":"2.0","id":"i","method":"plugin.initialize","params":{"features":{"tasks":1,"taskEvents":1}}}`)
	if r := p.replies(1)["i"]; !strings.Contains(r, `"features":{"tasks":1,"taskEvents":1}`) {
		t.Fatalf("initialize must echo taskEvents: %s", r)
	}
	p.send(`{"jsonrpc":"2.0","id":"s","method":"plugin.task.start","params":{"taskId":"t1","action":"x","payload":"{}"}}`)
	if r := p.replies(1)["s"]; !strings.Contains(r, `"state":"running"`) {
		t.Fatalf("start: %s", r)
	}
	// 20 progress reports in a burst: one immediate and one trailing notification.
	notifications := 0
	for {
		line, ok := p.next(150 * time.Millisecond)
		if !ok {
			break
		}
		if line != `{"jsonrpc":"2.0","method":"plugin.task.changed","params":{"taskId":"t1"}}` {
			t.Fatalf("unexpected line: %s", line)
		}
		notifications++
	}
	if notifications != 2 {
		t.Fatalf("progress notifications = %d, want 2 (throttled)", notifications)
	}
	close(release)
	line, ok := p.next(2 * time.Second)
	if !ok || !strings.Contains(line, `"method":"plugin.task.changed"`) {
		t.Fatalf("terminal notification: %q", line)
	}
	p.send(`{"jsonrpc":"2.0","id":"q","method":"plugin.task.status","params":{"taskId":"t1"}}`)
	if r := p.replies(1)["q"]; !strings.Contains(r, `"state":"succeeded"`) {
		t.Fatalf("status after notification: %s", r)
	}
}

func TestTaskEventsNeedNegotiation(t *testing.T) {
	for _, tc := range []struct {
		name, offer string
		disable     bool
	}{
		{"not offered", `{"tasks":1}`, false},
		{"disabled", `{"tasks":1,"taskEvents":1}`, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			o := rpc.StrictDefaults("x")
			o.Tasks = &rpc.TaskOptions{DisableEvents: tc.disable}
			o.Handler = func(context.Context, string, json.RawMessage) (any, error) { return 1, nil }
			p := startLinePeer(t, o)
			p.send(`{"jsonrpc":"2.0","id":"i","method":"plugin.initialize","params":{"features":` + tc.offer + `}}`)
			if r := p.replies(1)["i"]; r != `{"jsonrpc":"2.0","id":"i","result":{"runtimeProtocolVersion":1,"features":{"tasks":1}}}` {
				t.Fatalf("initialize: %s", r)
			}
			p.send(`{"jsonrpc":"2.0","id":"s","method":"plugin.task.start","params":{"taskId":"t1","action":"x","payload":"{}"}}`)
			p.replies(1)
			if line, ok := p.next(100 * time.Millisecond); ok {
				t.Fatalf("no notification expected, got %s", line)
			}
		})
	}
}

func TestTaskEventsSortedStyle(t *testing.T) {
	o := rpc.StrictDefaults("x")
	o.Style = rpc.StyleSorted
	o.Tasks = &rpc.TaskOptions{}
	o.Handler = func(context.Context, string, json.RawMessage) (any, error) { return 1, nil }
	p := startLinePeer(t, o)
	p.send(`{"jsonrpc":"2.0","id":"i","method":"plugin.initialize","params":{"features":{"tasks":1,"taskEvents":1}}}`)
	if r := p.replies(1)["i"]; !strings.Contains(r, `"features":{"taskEvents":1,"tasks":1}`) {
		t.Fatalf("sorted initialize: %s", r)
	}
}

func TestSharedGateConcurrentTaskKeepsGateFree(t *testing.T) {
	o := rpc.NodeDefaults()
	o.ConcurrentActions = []string{"scan"}
	release := make(chan struct{})
	o.Tasks = &rpc.TaskOptions{SharedGate: true}
	o.Handler = func(_ context.Context, action string, _ json.RawMessage) (any, error) {
		if action == "scan" {
			<-release
		}
		return action, nil
	}
	p := startLinePeer(t, o)
	p.send(`{"jsonrpc":"2.0","id":"i","method":"plugin.initialize","params":{"features":{"tasks":1}}}`)
	p.replies(1)
	p.send(`{"jsonrpc":"2.0","id":"s","method":"plugin.task.start","params":{"taskId":"t1","action":"scan","payload":{}}}`)
	if r := p.replies(1)["s"]; !strings.Contains(r, `"state":"running"`) {
		t.Fatalf("start: %s", r)
	}
	p.send(`{"jsonrpc":"2.0","id":"w","method":"plugin.invoke","params":{"action":"write","payload":{}}}`)
	if r := p.replies(1)["w"]; !strings.Contains(r, `"result":"write"`) {
		t.Fatalf("gate must stay free while a concurrent task runs: %s", r)
	}
	close(release)
}
