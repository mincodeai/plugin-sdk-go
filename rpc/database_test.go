package rpc_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/mincodeai/plugin-sdk-go/rpc"
)

type fakeDB struct{ closed chan struct{} }

func (f *fakeDB) DatabaseDetails() json.RawMessage {
	return json.RawMessage(`{"protocol":1,"drivers":["x"]}`)
}

func (f *fakeDB) StartDatabaseCall(_ context.Context, method string, _ json.RawMessage) (func() (any, error), *rpc.Error) {
	switch method {
	case "plugin.db.bad":
		return nil, rpc.NewError(rpc.CodeInvalidParams, "bad")
	case "plugin.db.raw":
		return func() (any, error) { return json.RawMessage(`{"rows":[["<a&b>","é"]],"done":true}`), nil }, nil
	case "plugin.db.multiline":
		return func() (any, error) { return json.RawMessage("{\"rows\":\n[[1]]}"), nil }, nil
	case "plugin.db.invalid":
		return func() (any, error) { return json.RawMessage(`{"rows":`), nil }, nil
	}
	return func() (any, error) { return map[string]string{"method": method}, nil }, nil
}

func (f *fakeDB) CloseDatabase() { close(f.closed) }

func TestDatabaseNegotiation(t *testing.T) {
	db := &fakeDB{closed: make(chan struct{})}
	o := rpc.StrictDefaults("x")
	o.Database = db // no Handler: a driver-only plugin
	p := startPipe(t, o)
	if r := p.call(`{"jsonrpc":"2.0","id":1,"method":"plugin.initialize","params":{"features":{"databaseDrivers":1,"tasks":1}}}`); r != `{"jsonrpc":"2.0","id":1,"result":{"runtimeProtocolVersion":1,"features":{"databaseDrivers":1},"databaseDrivers":{"protocol":1,"drivers":["x"]}}}` {
		t.Fatalf("initialize: %s", r)
	}
	if r := p.call(`{"jsonrpc":"2.0","id":2,"method":"plugin.db.connect","params":{}}`); r != `{"jsonrpc":"2.0","id":2,"result":{"method":"plugin.db.connect"}}` {
		t.Fatalf("call: %s", r)
	}
	if r := p.call(`{"jsonrpc":"2.0","id":3,"method":"plugin.db.bad","params":{}}`); !strings.Contains(r, `"code":-32602`) {
		t.Fatalf("start error: %s", r)
	}
	if r := p.call(`{"jsonrpc":"2.0","id":4,"method":"plugin.invoke","params":{"action":"a","payload":"{}"}}`); !strings.Contains(r, `"error"`) {
		t.Fatalf("invoke without handler: %s", r)
	}
	if r := p.call(`{"jsonrpc":"2.0","id":5,"method":"plugin.shutdown"}`); !strings.Contains(r, `"result"`) {
		t.Fatalf("shutdown: %s", r)
	}
	<-db.closed
}

// Database results are written without re-encoding when the reply stays one
// valid line; otherwise the regular encoder (or the size limit) applies.
func TestDatabaseRawResultReply(t *testing.T) {
	o := rpc.StrictDefaults("x")
	o.Database = &fakeDB{closed: make(chan struct{})}
	o.ResultTooLarge = rpc.NewError(rpc.CodeServerError, "big")
	p := startPipe(t, o)
	p.call(`{"jsonrpc":"2.0","id":1,"method":"plugin.initialize","params":{"features":{"databaseDrivers":1}}}`)
	for _, c := range []struct{ req, want string }{
		{`{"jsonrpc":"2.0","id":"a","method":"plugin.db.raw","params":{}}`, `{"jsonrpc":"2.0","id":"a","result":{"rows":[["<a&b>","é"]],"done":true}}`},
		{`{"jsonrpc":"2.0","id":2,"method":"plugin.db.multiline","params":{}}`, `{"jsonrpc":"2.0","id":2,"result":{"rows":[[1]]}}`},
		{`{"jsonrpc":"2.0","id":3,"method":"plugin.db.invalid","params":{}}`, `{"jsonrpc":"2.0","id":3,"error":{"code":-32000,"message":"big"}}`},
	} {
		if r := p.call(c.req); r != c.want {
			t.Fatalf("got %s want %s", r, c.want)
		}
	}
	o = rpc.StrictDefaults("x")
	o.Database = &fakeDB{closed: make(chan struct{})}
	o.ResultTooLarge = rpc.NewError(rpc.CodeServerError, "big")
	o.MaxReplyBytes = 60
	p = startPipe(t, o)
	p.call(`{"jsonrpc":"2.0","id":1,"method":"plugin.initialize","params":{"features":{"databaseDrivers":1}}}`)
	if r := p.call(`{"jsonrpc":"2.0","id":2,"method":"plugin.db.raw","params":{}}`); r != `{"jsonrpc":"2.0","id":2,"error":{"code":-32000,"message":"big"}}` {
		t.Fatalf("size limit: %s", r)
	}
}

func TestDatabaseNotNegotiated(t *testing.T) {
	for _, offer := range []string{`{"tasks":1}`, `{"databaseDrivers":2}`} {
		o := rpc.StrictDefaults("x")
		o.Handler = func(context.Context, string, json.RawMessage) (any, error) { return nil, nil }
		o.Database = &fakeDB{closed: make(chan struct{})}
		p := startPipe(t, o)
		if r := p.call(`{"jsonrpc":"2.0","id":1,"method":"plugin.initialize","params":{"features":` + offer + `}}`); strings.Contains(r, "databaseDrivers") {
			t.Fatalf("initialize %s: %s", offer, r)
		}
		if r := p.call(`{"jsonrpc":"2.0","id":2,"method":"plugin.db.connect","params":{}}`); !strings.Contains(r, `"code":-32601`) {
			t.Fatalf("connect without negotiation: %s", r)
		}
	}
	// Without Options.Database the offer is ignored and Handler stays required.
	o := rpc.StrictDefaults("x")
	if err := rpc.Serve(context.Background(), strings.NewReader(""), nil, nil, o); err == nil {
		t.Fatal("Serve without Handler and Database must fail")
	}
}
