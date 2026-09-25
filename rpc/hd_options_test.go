package rpc_test

import (
	"context"
	"encoding/json"
	"io"
	"strings"
	"testing"

	"github.com/mincodeai/plugin-sdk-go/rpc"
)

func TestCloseBeforeShutdownReply(t *testing.T) {
	o := rpc.NodeDefaults()
	o.ShutdownReplyLast, o.CloseBeforeShutdownReply = true, true
	o.Handler = func(context.Context, string, json.RawMessage) (any, error) { return nil, nil }
	var out syncBuffer
	closes := 0
	o.OnClose = func() {
		closes++
		if strings.Contains(out.String(), `"id":1`) {
			t.Error("OnClose ran after the shutdown reply")
		}
	}
	if err := rpc.Serve(context.Background(), strings.NewReader(`{"id":1,"method":"plugin.shutdown"}`+"\n"), &out, io.Discard, o); err != nil {
		t.Fatal(err)
	}
	if closes != 1 || out.String() != `{"jsonrpc":"2.0","id":1,"result":{}}`+"\n" {
		t.Fatalf("closes=%d out=%q", closes, out.String())
	}
}
