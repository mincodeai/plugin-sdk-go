package rpc_test

import (
	"bufio"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/mincodeai/plugin-sdk-go/host"
	"github.com/mincodeai/plugin-sdk-go/rpc"
)

// Node-compat golden presets recorded from the pre-SDK binaries:
// web-navigation (first-run seeding) and prd-studio (tasks like knowledge-base).
func init() {
	extraPresets["web-navigation"] = func(t *testing.T) rpc.Options {
		o := rpc.NodeDefaults()
		o.RejectTaskMethods = true
		seed := goldenLines(t, "web-navigation", "basic")
		value := seed(`"id":"host-3","method":"host.storage.set"`, "params")
		var set struct{ Value json.RawMessage }
		_ = json.Unmarshal(value, &set)
		state := seed(`"id":16,"result"`, "result")
		var mu sync.Mutex
		o.KnownAction = func(a string) bool { return a == "state" }
		o.Handler = func(ctx context.Context, _ string, _ json.RawMessage) (any, error) {
			mu.Lock()
			defer mu.Unlock()
			h := host.FromContext(ctx)
			version, err := h.Call(ctx, "host.storage.get", json.RawMessage(`{"key":"defaults-version"}`))
			if err != nil {
				return nil, err
			}
			if _, err := h.Call(ctx, "host.storage.get", json.RawMessage(`{"key":"navigation"}`)); err != nil {
				return nil, err
			}
			if string(version) == "null" {
				params := append(append([]byte(`{"key":"navigation","value":`), set.Value...), '}')
				for _, p := range []json.RawMessage{params, json.RawMessage(`{"key":"defaults-version","value":3}`)} {
					if _, err := h.Call(ctx, "host.storage.set", p); err != nil {
						return nil, err
					}
				}
			}
			return state, nil
		}
		return o
	}
	extraPresets["prd-studio"] = func(*testing.T) rpc.Options {
		o := rpc.NodeDefaults()
		o.RejectTaskMethods = true
		o.Style = rpc.StyleJS
		o.Tasks = &rpc.TaskOptions{SharedGate: true}
		st := &stub{ignoreCancel: true, actions: map[string]stubAction{
			"prd.list": {calls: []hostCall{{"host.storage.list", json.RawMessage(`{}`)}}, result: `[]`},
		}}
		o.KnownAction = st.known
		o.Handler = st.handle
		return o
	}
}

// goldenLines returns a lookup of the member of the first plugin-written line
// (in testdata/golden/<plugin>/<scenario>.jsonl) containing marker.
func goldenLines(t *testing.T, plugin, scenario string) func(marker, member string) json.RawMessage {
	f, err := os.Open(filepath.Join("..", "testdata", "golden", plugin, scenario+".jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	var lines []string
	sc := bufio.NewScanner(f)
	sc.Buffer(nil, 4<<20)
	for sc.Scan() {
		if line, ok := strings.CutPrefix(sc.Text(), "< "); ok {
			lines = append(lines, line)
		}
	}
	return func(marker, member string) json.RawMessage {
		for _, line := range lines {
			if strings.Contains(line, marker) {
				var m map[string]json.RawMessage
				if json.Unmarshal([]byte(line), &m) != nil {
					break
				}
				return m[member]
			}
		}
		t.Fatalf("%s/%s: no line with %s", plugin, scenario, marker)
		return nil
	}
}
