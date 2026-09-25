package rpc_test

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/mincodeai/plugin-sdk-go/rpc"
)

func TestStructEnvelope(t *testing.T) {
	in := strings.Join([]string{
		`{"JSONRPC":"2.0","Id":1,"METHOD":"plugin.ping"}`,
		`{"jsonrpc":"2.0","id":"h","error":"nope"}`,
		`{"jsonrpc":"2.0","id":"h","error":{"code":1.5}}`,
		`{"jsonrpc":"2.0","id":"h","result":1,"error":null}`,
		`{"jsonrpc":"2.0","id":2,"method":"plugin.ping","Method":"plugin.nope"}`,
		`{"jsonrpc":"2.0","id":3,"method":""}`,
	}, "\n") + "\n"
	run := func(structEnv bool) string {
		o := rpc.StrictDefaults("x")
		o.StructEnvelope = structEnv
		o.Handler = func(context.Context, string, json.RawMessage) (any, error) { return nil, nil }
		var out bytes.Buffer
		if err := rpc.Serve(context.Background(), strings.NewReader(in), &out, nil, o); err != nil {
			t.Fatal(err)
		}
		return out.String()
	}
	const invalid = `{"jsonrpc":"2.0","id":null,"error":{"code":-32700,"message":"Invalid JSON-RPC request"}}`
	want := `{"jsonrpc":"2.0","id":1,"result":{"runtimeProtocolVersion":1}}` + "\n" +
		invalid + "\n" + invalid + "\n" +
		`{"jsonrpc":"2.0","id":2,"error":{"code":-32601,"message":"Method not found"}}` + "\n"
	if got := run(true); got != want {
		t.Fatalf("struct envelope:\n%s\nwant\n%s", got, want)
	}
	// The default member map is case-sensitive and ignores the error shape.
	want = invalid + "\n" + `{"jsonrpc":"2.0","id":2,"result":{"runtimeProtocolVersion":1}}` + "\n"
	if got := run(false); got != want {
		t.Fatalf("map envelope:\n%s\nwant\n%s", got, want)
	}
}
