package rpctest

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"testing"

	"github.com/mincodeai/plugin-sdk-go/host"
)

func TestParseFormat(t *testing.T) {
	src := "# comment\n> {\"a\":1}\n< {\"b\":2}\n<~ x\n! err\n# wait 5ms\n# eof\n"
	tr, err := Parse([]byte(src))
	if err != nil || len(tr) != 6 {
		t.Fatalf("Parse = %v, %v", tr, err)
	}
	if got := string(tr.Format()); got != src[len("# comment\n"):] {
		t.Fatalf("Format = %q", got)
	}
	for _, bad := range []string{"? x\n", "# wait soon\n"} {
		if _, err := Parse([]byte(bad)); err == nil {
			t.Errorf("Parse(%q) should fail", bad)
		}
	}
	if ExpandPad(`"@@PAD3@@"`) != `"xxx"` {
		t.Fatal("ExpandPad")
	}
}

func TestMatch(t *testing.T) {
	if !Match(`{"m":"{{*}}"}`, `{"m":"any \" text"}`) || Match(`{"m":"{{*}}"}`, `{"m":"a","x":1}`) || !Match("a", "a") {
		t.Fatal("Match")
	}
	if !MatchSet([]string{`{{*}}`, "b"}, []string{"b", "a"}) || MatchSet([]string{"a"}, []string{"a", "b"}) {
		t.Fatal("MatchSet")
	}
}

func TestReplayEcho(t *testing.T) {
	tr, _ := Parse([]byte("> hello\n< hello\n# eof\n! bye\n"))
	Replay(t, tr, func(_ context.Context, in io.Reader, out, errw io.Writer) error {
		b := make([]byte, 6)
		if _, err := io.ReadFull(in, b); err != nil {
			return err
		}
		_, _ = out.Write(b)
		_, _ = io.ReadAll(in)
		_, _ = io.WriteString(errw, "bye\n")
		return nil
	})
}

func TestReplayRepollsRunningTaskStatus(t *testing.T) {
	poll := `{"jsonrpc":"2.0","id":9,"method":"plugin.task.status","params":{"taskId":"t"}}`
	done := `{"id":9,"result":{"state":"succeeded"}}`
	tr, _ := Parse([]byte("> " + poll + "\n< " + done + "\n# eof\n"))
	Replay(t, tr, func(_ context.Context, in io.Reader, out, _ io.Writer) error {
		sc := bufio.NewScanner(in)
		for polls := 0; sc.Scan(); polls++ {
			state := "running"
			if polls >= 2 {
				state = "succeeded"
			}
			_, _ = io.WriteString(out, `{"id":9,"result":{"state":"`+state+`"}}`+"\n")
		}
		return nil
	})
}

func TestFakeHost(t *testing.T) {
	fh := NewFakeHost()
	ctx := fh.Context(context.Background())
	h := host.FromContext(ctx)
	call := func(method string, params any) string {
		r, err := h.Call(ctx, method, params)
		if err != nil {
			return "error " + err.Error()
		}
		return string(r)
	}
	steps := [][3]any{
		{"host.storage.get", map[string]any{"key": "k"}, "null"},
		{"host.storage.set", map[string]any{"key": "k", "value": map[string]int{"v": 1}}, "true"},
		{"host.storage.get", map[string]any{"key": "k"}, `{"v":1}`},
		{"host.storage.list", map[string]any{}, `["k"]`},
		{"host.storage.delete", map[string]any{"key": "k"}, "true"},
		{"host.secrets.set", map[string]any{"key": "s", "value": "pw"}, "true"},
		{"host.secrets.set", map[string]any{"key": "s", "value": 1}, "error host error -32602"},
		{"host.secrets.get", map[string]any{"key": "s"}, `"pw"`},
		{"host.secrets.list", map[string]any{}, `["s"]`},
		{"host.secrets.delete", map[string]any{"key": "s"}, "true"},
		{"host.nope", nil, "error host error -32601"},
	}
	for _, s := range steps {
		if got := call(s[0].(string), s[1]); got != s[2].(string) {
			t.Errorf("%s = %s, want %s", s[0], got, s[2])
		}
	}
	fh.Fail = func(string, json.RawMessage) error { return &host.Error{Code: -32001} }
	if got := call("host.storage.get", map[string]any{"key": "k"}); got != "error host error -32001" {
		t.Fatalf("Fail = %s", got)
	}
	if len(fh.Calls) != len(steps)+1 {
		t.Fatalf("calls = %d", len(fh.Calls))
	}
}
