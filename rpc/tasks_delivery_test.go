package rpc_test

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mincodeai/plugin-sdk-go/rpc"
)

// A finished task whose terminal snapshot the host already read does not hold
// a MaxRetained slot; one the host has not read yet still does.
func TestTaskRetentionEvictsOnlyDeliveredTasks(t *testing.T) {
	var finished sync.WaitGroup
	o := rpc.StrictDefaults("x")
	o.Handler = func(context.Context, string, json.RawMessage) (any, error) {
		defer finished.Done()
		return map[string]bool{"ok": true}, nil
	}
	o.Tasks = &rpc.TaskOptions{MaxRetained: 2, MaxRunning: 4}
	p := startPipe(t, o)
	p.call(`{"jsonrpc":"2.0","id":1,"method":"plugin.initialize","params":{"features":{"tasks":1}}}`)
	start := func(id string) string {
		return p.call(`{"jsonrpc":"2.0","id":2,"method":"plugin.task.start","params":{"taskId":"` + id + `","action":"run","payload":"{}"}}`)
	}
	// a finishes before b starts, so a is the oldest finished task.
	for _, id := range []string{"a", "b"} {
		finished.Add(1)
		start(id)
		finished.Wait()
		time.Sleep(20 * time.Millisecond) // let the worker record the terminal state
	}
	// Both finished, neither read: the table is full.
	if r := start("c"); !strings.Contains(r, "Task count limit reached") {
		t.Fatalf("undelivered results were evicted: %s", r)
	}
	// The host reads b, then a: a start now evicts the oldest delivered (a).
	p.waitState("b", "succeeded")
	p.waitState("a", "succeeded")
	finished.Add(1)
	if r := start("c"); !strings.Contains(r, `"state":"running"`) {
		t.Fatalf("delivered task still held a slot: %s", r)
	}
	finished.Wait()
	if r := p.call(`{"jsonrpc":"2.0","id":3,"method":"plugin.task.status","params":{"taskId":"a"}}`); !strings.Contains(r, "Task not found") {
		t.Fatalf("oldest delivered task was not the one evicted: %s", r)
	}
	if r := p.call(`{"jsonrpc":"2.0","id":4,"method":"plugin.task.status","params":{"taskId":"b"}}`); !strings.Contains(r, `"state":"succeeded"`) {
		t.Fatalf("newer delivered task evicted early: %s", r)
	}
}
