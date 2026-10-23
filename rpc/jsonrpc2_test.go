package rpc_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/mincodeai/plugin-sdk-go/rpc"
)

func serveLines(t *testing.T, o rpc.Options, lines ...string) []string {
	t.Helper()
	o.Handler = func(_ context.Context, action string, payload json.RawMessage) (any, error) {
		return map[string]string{"action": action}, nil
	}
	var out bytes.Buffer
	var errOut bytes.Buffer
	if err := rpc.Serve(context.Background(), strings.NewReader(strings.Join(lines, "\n")+"\n"), &out, &errOut, o); err != nil {
		t.Fatal(err)
	}
	if errOut.Len() > 0 {
		t.Logf("stderr: %s", errOut.String())
	}
	return strings.Split(strings.TrimSpace(out.String()), "\n")
}

const initLine = `{"jsonrpc":"2.0","id":0,"method":"plugin.initialize","params":{}}`

// §5.1: a parsable message that is not a valid request gets -32600, echoing a valid id.
func TestJSONRPC2InvalidRequests(t *testing.T) {
	got := serveLines(t, rpc.StrictDefaults("x"),
		`"hello"`,
		`{"jsonrpc":"1.0","id":2,"method":"plugin.ping"}`,
		`{"jsonrpc":"2.0","id":{"x":1},"method":"plugin.ping"}`,
		`{"jsonrpc":"2.0","id":6,"method":"plugin.ping","params":3}`,
		`{"jsonrpc":"2.0","id":7}`, // method-less: a response, never answered
		`[]`,
	)
	want := []string{
		`{"jsonrpc":"2.0","id":null,"error":{"code":-32600,"message":"Invalid Request"}}`,
		`{"jsonrpc":"2.0","id":2,"error":{"code":-32600,"message":"Invalid Request"}}`,
		`{"jsonrpc":"2.0","id":null,"error":{"code":-32600,"message":"Invalid Request"}}`,
		`{"jsonrpc":"2.0","id":6,"error":{"code":-32600,"message":"Invalid Request"}}`,
		`{"jsonrpc":"2.0","id":null,"error":{"code":-32600,"message":"Invalid Request"}}`,
	}
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("got\n%s\nwant\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
	}
}

// §6: one array per batch with a reply per request; notifications get none,
// invalid entries -32600, and an all-notification batch nothing.
func TestJSONRPC2Batches(t *testing.T) {
	for name, o := range map[string]rpc.Options{"strict": rpc.StrictDefaults("x"), "node": rpc.NodeDefaults()} {
		t.Run(name, func(t *testing.T) {
			o.Handler = func(_ context.Context, action string, _ json.RawMessage) (any, error) {
				return map[string]string{"action": action}, nil
			}
			p := startPipe(t, o)
			p.call(initLine)
			batchReply := p.call(`[{"jsonrpc":"2.0","id":"a","method":"plugin.invoke","params":{"action":"echo","payload":"{}"}},{"jsonrpc":"2.0","method":"plugin.ping"},1,{"jsonrpc":"2.0","id":"b","method":"plugin.nope"}]`)
			// An all-notification batch writes nothing: the next reply is the ping's.
			if r := p.call(`[{"jsonrpc":"2.0","method":"plugin.ping"}]` + "\n" + `{"jsonrpc":"2.0","id":"after","method":"plugin.ping"}`); !strings.Contains(r, `"after"`) {
				t.Fatalf("notification batch answered: %s", r)
			}
			got := []string{"", batchReply}
			var replies []struct {
				JSONRPC string          `json:"jsonrpc"`
				ID      json.RawMessage `json:"id"`
				Result  json.RawMessage `json:"result"`
				Error   *struct {
					Code int `json:"code"`
				} `json:"error"`
			}
			if err := json.Unmarshal([]byte(got[1]), &replies); err != nil || len(replies) != 3 {
				t.Fatalf("batch reply: %s (%v)", got[1], err)
			}
			sort.Slice(replies, func(i, j int) bool { return string(replies[i].ID) < string(replies[j].ID) })
			if string(replies[0].ID) != `"a"` || !strings.Contains(string(replies[0].Result), `"echo"`) {
				t.Fatalf("invoke: %s", got[1])
			}
			if string(replies[1].ID) != `"b"` || replies[1].Error == nil || replies[1].Error.Code != rpc.CodeMethodNotFound {
				t.Fatalf("unknown method: %s", got[1])
			}
			if string(replies[2].ID) != "null" || replies[2].Error == nil || replies[2].Error.Code != rpc.CodeInvalidRequest {
				t.Fatalf("invalid entry: %s", got[1])
			}
			for _, reply := range replies {
				if reply.JSONRPC != "2.0" {
					t.Fatalf("version: %s", got[1])
				}
			}
		})
	}
}

func batchIDs(t *testing.T, line string) []string {
	t.Helper()
	var replies []struct {
		ID json.RawMessage `json:"id"`
	}
	if err := json.Unmarshal([]byte(line), &replies); err != nil {
		t.Fatalf("not a batch array: %s (%v)", line, err)
	}
	ids := make([]string, len(replies))
	for i, r := range replies {
		ids[i] = string(r.ID)
	}
	sort.Strings(ids)
	return ids
}

// A batched plugin.db.* reply joins the array: the database fast path must not
// write it on its own and leave the batch waiting forever.
func TestJSONRPC2BatchedDatabaseCall(t *testing.T) {
	o := rpc.StrictDefaults("x")
	o.Database = &fakeDB{closed: make(chan struct{})}
	p := startPipe(t, o)
	p.call(`{"jsonrpc":"2.0","id":1,"method":"plugin.initialize","params":{"features":{"databaseDrivers":1}}}`)
	r := p.call(`[{"jsonrpc":"2.0","id":"q","method":"plugin.db.raw","params":{}},{"jsonrpc":"2.0","id":"p","method":"plugin.ping"}]`)
	if got := strings.Join(batchIDs(t, r), ","); got != `"p","q"` {
		t.Fatalf("batch reply: %s", r)
	}
	if !strings.Contains(r, `"result":{"done":true,"rows":[["\u003ca\u0026b\u003e","é"]]}`) && !strings.Contains(r, `"rows":[["<a&b>","é"]]`) {
		t.Fatalf("db result: %s", r)
	}
	// The next single request is answered on its own: nothing leaked.
	if r := p.call(`{"jsonrpc":"2.0","id":"q","method":"plugin.db.raw","params":{}}`); !strings.HasPrefix(r, `{"jsonrpc":"2.0","id":"q","result":`) {
		t.Fatalf("single db reply after batch: %s", r)
	}
}

// A batch holding plugin.shutdown is still answered with one array, holding
// the shutdown reply and the replies of the entries before it.
func TestJSONRPC2BatchedShutdown(t *testing.T) {
	for _, drain := range []bool{false, true} {
		o := rpc.StrictDefaults("x")
		o.DrainReplies = drain
		o.Handler = func(context.Context, string, json.RawMessage) (any, error) { return "ok", nil }
		out := serveRaw(t, o, initLine,
			`[{"jsonrpc":"2.0","id":"p","method":"plugin.ping"},{"jsonrpc":"2.0","id":"s","method":"plugin.shutdown"},{"jsonrpc":"2.0","id":"late","method":"plugin.ping"},2]`,
			`{"jsonrpc":"2.0","id":"after","method":"plugin.ping"}`)
		if len(out) != 2 {
			t.Fatalf("drain=%v: got %q", drain, out)
		}
		if got := strings.Join(batchIDs(t, out[1]), ","); got != `"p","s",null` {
			t.Fatalf("drain=%v: batch reply %s", drain, out[1])
		}
	}
	// Shutdown as the only entry.
	out := serveRaw(t, okOptions(), initLine, `[{"jsonrpc":"2.0","id":"s","method":"plugin.shutdown"}]`)
	if len(out) != 2 || out[1] != `[{"jsonrpc":"2.0","id":"s","result":{}}]` {
		t.Fatalf("lone batched shutdown: %q", out)
	}
}

// A request still running at a batched shutdown is not answered, and its
// batch is written with what it collected instead of waiting for it.
func TestJSONRPC2ShutdownAbandonsOpenBatch(t *testing.T) {
	release := make(chan struct{})
	o := rpc.StrictDefaults("x")
	o.MaxInflight = 4
	o.DrainTimeout = 10 * time.Millisecond
	o.Handler = func(ctx context.Context, action string, _ json.RawMessage) (any, error) {
		if action == "slow" {
			<-release
		}
		return "ok", nil
	}
	defer close(release)
	p := startPipe(t, o)
	p.call(initLine)
	if _, err := io.WriteString(p.in, `[{"jsonrpc":"2.0","id":"slow","method":"plugin.invoke","params":{"action":"slow","payload":"{}"}},{"jsonrpc":"2.0","id":"p","method":"plugin.ping"}]`+"\n"); err != nil {
		t.Fatal(err)
	}
	if r := p.call(`{"jsonrpc":"2.0","id":"s","method":"plugin.shutdown"}`); r != `[{"jsonrpc":"2.0","id":"p","result":{"runtimeProtocolVersion":1}}]` {
		t.Fatalf("open batch at shutdown: %s", r)
	}
	if !p.out.Scan() || p.out.Text() != `{"jsonrpc":"2.0","id":"s","result":{}}` {
		t.Fatalf("shutdown reply: %s", p.out.Text())
	}
}

// Repeated ids in a batch all land in its one array (§6: one reply message).
func TestJSONRPC2BatchDuplicateIDs(t *testing.T) {
	out := serveRaw(t, okOptions(), initLine,
		`[{"jsonrpc":"2.0","id":"dup","method":"plugin.ping"},{"jsonrpc":"2.0","id":"dup","method":"plugin.nope"}]`,
		`{"jsonrpc":"2.0","id":"after","method":"plugin.ping"}`)
	if len(out) != 3 {
		t.Fatalf("got %q", out)
	}
	if got := strings.Join(batchIDs(t, out[1]), ","); got != `"dup","dup"` {
		t.Fatalf("batch reply: %s", out[1])
	}
	if !strings.Contains(out[2], `"after"`) {
		t.Fatalf("after: %s", out[2])
	}
}

// An entry the server answers with -32600 without registering it (a lenient
// preset, a non-object entry) never takes the reply slot of a pending request
// with the same id.
func TestJSONRPC2BatchInvalidEntrySharesID(t *testing.T) {
	o := rpc.NodeDefaults()
	o.Handler = func(context.Context, string, json.RawMessage) (any, error) { return "ok", nil }
	out := serveRaw(t, o,
		`[{"jsonrpc":"2.0","id":"x","method":"plugin.ping"},"junk",{"jsonrpc":"2.0","id":"y","method":"plugin.ping"}]`,
		`{"jsonrpc":"2.0","id":"after","method":"plugin.ping"}`)
	if len(out) != 2 {
		t.Fatalf("got %q", out)
	}
	if got := strings.Join(batchIDs(t, out[0]), ","); got != `"x","y",null` {
		t.Fatalf("batch reply: %s", out[0])
	}
}

func serveRaw(t *testing.T, o rpc.Options, lines ...string) []string {
	t.Helper()
	var out bytes.Buffer
	_ = rpc.Serve(context.Background(), strings.NewReader(strings.Join(lines, "\n")+"\n"), &out, io.Discard, o)
	return strings.Split(strings.TrimSpace(out.String()), "\n")
}

func okOptions() rpc.Options {
	o := rpc.StrictDefaults("x")
	o.Handler = func(context.Context, string, json.RawMessage) (any, error) { return "ok", nil }
	return o
}
