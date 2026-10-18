package rpc_test

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/mincodeai/plugin-sdk-go/rpc"
)

func TestInitializeReportsIdentityOnlyWhenDeclared(t *testing.T) {
	o := rpc.StrictDefaults("x")
	o.Handler = func(context.Context, string, json.RawMessage) (any, error) { return nil, nil }
	plain := startPipe(t, o).call(`{"jsonrpc":"2.0","id":1,"method":"plugin.initialize","params":{}}`)
	if plain != `{"jsonrpc":"2.0","id":1,"result":{"runtimeProtocolVersion":1}}` {
		t.Fatalf("default initialize changed: %s", plain)
	}

	o.Identity = &rpc.Identity{ID: "vendor.example", Version: "1.2.0"}
	got := startPipe(t, o).call(`{"jsonrpc":"2.0","id":1,"method":"plugin.initialize","params":{}}`)
	if got != `{"jsonrpc":"2.0","id":1,"result":{"runtimeProtocolVersion":1,"plugin":{"id":"vendor.example","version":"1.2.0"}}}` {
		t.Fatalf("initialize with identity: %s", got)
	}

	o.Identity = nil
	rpc.BuildPluginID, rpc.BuildPluginVersion = "vendor.built", "2.0.0"
	t.Cleanup(func() { rpc.BuildPluginID, rpc.BuildPluginVersion = "", "" })
	got = startPipe(t, o).call(`{"jsonrpc":"2.0","id":1,"method":"plugin.initialize","params":{}}`)
	if !strings.Contains(got, `"plugin":{"id":"vendor.built","version":"2.0.0"}`) {
		t.Fatalf("build-time identity missing: %s", got)
	}
	ping := startPipe(t, o).call(`{"jsonrpc":"2.0","id":2,"method":"plugin.ping","params":{}}`)
	if strings.Contains(ping, `"plugin"`) {
		t.Fatalf("ping must not carry identity: %s", ping)
	}
}

func TestPluginCallRoutesRegisteredMethods(t *testing.T) {
	o := rpc.StrictDefaults("x")
	o.Handler = func(context.Context, string, json.RawMessage) (any, error) { return nil, nil }
	o.Methods = map[string]rpc.MethodFunc{
		"files/list": func(_ context.Context, params json.RawMessage) (any, error) {
			return map[string]any{"params": params}, nil
		},
		"fails": func(context.Context, json.RawMessage) (any, error) { return nil, errors.New("boom") },
	}
	p := startPipe(t, o)
	p.call(`{"jsonrpc":"2.0","id":1,"method":"plugin.initialize","params":{}}`)
	if got := p.call(`{"jsonrpc":"2.0","id":2,"method":"plugin.call","params":{"method":"files/list","params":{"prefix":"a"}}}`); got != `{"jsonrpc":"2.0","id":2,"result":{"params":{"prefix":"a"}}}` {
		t.Fatalf("call: %s", got)
	}
	if got := p.call(`{"jsonrpc":"2.0","id":3,"method":"plugin.call","params":{"method":"files/list"}}`); !strings.Contains(got, `"params":null`) {
		t.Fatalf("call without params: %s", got)
	}
	if got := p.call(`{"jsonrpc":"2.0","id":4,"method":"plugin.call","params":{"method":"missing"}}`); !strings.Contains(got, `"code":-32601`) {
		t.Fatalf("unknown method: %s", got)
	}
	if got := p.call(`{"jsonrpc":"2.0","id":5,"method":"plugin.call","params":{}}`); !strings.Contains(got, `"code":-32602`) {
		t.Fatalf("missing method: %s", got)
	}
	if got := p.call(`{"jsonrpc":"2.0","id":6,"method":"plugin.call","params":{"method":"fails"}}`); !strings.Contains(got, `"message":"boom"`) {
		t.Fatalf("method error: %s", got)
	}
}

type framedPeer struct {
	t   *testing.T
	in  *io.PipeWriter
	out *bufio.Reader
}

func (p *framedPeer) send(kind byte, payload []byte) {
	p.t.Helper()
	header := []byte{kind, 0, 0, 0, 0}
	binary.BigEndian.PutUint32(header[1:], uint32(len(payload)))
	if _, err := p.in.Write(append(header, payload...)); err != nil {
		p.t.Fatal(err)
	}
}

func (p *framedPeer) sendBinary(channel string, data []byte) {
	payload := []byte{0, byte(len(channel))}
	p.send(1, append(append(payload, channel...), data...))
}

func (p *framedPeer) read() (byte, []byte) {
	p.t.Helper()
	header := make([]byte, 5)
	if _, err := io.ReadFull(p.out, header); err != nil {
		p.t.Fatal(err)
	}
	payload := make([]byte, binary.BigEndian.Uint32(header[1:]))
	if _, err := io.ReadFull(p.out, payload); err != nil {
		p.t.Fatal(err)
	}
	return header[0], payload
}

func TestFramedTransportCarriesJSONAndBinary(t *testing.T) {
	received := make(chan string, 4)
	o := rpc.StrictDefaults("x")
	o.Transport = rpc.TransportFramed
	o.Handler = func(context.Context, string, json.RawMessage) (any, error) { return nil, nil }
	o.OnBinary = func(ctx context.Context, channel string, data []byte) {
		received <- channel + "=" + string(data)
		// Echo back from the binary handler's server context.
		if err := rpc.SendBinary(ctx, "echo/"+channel, append([]byte("echo:"), data...)); err != nil {
			t.Error(err)
		}
	}
	inR, inW := io.Pipe()
	outR, outW := io.Pipe()
	done := make(chan error, 1)
	go func() { done <- rpc.Serve(context.Background(), inR, outW, io.Discard, o); outW.Close() }()
	t.Cleanup(func() { inW.Close(); outR.Close() })
	peer := &framedPeer{t: t, in: inW, out: bufio.NewReader(outR)}

	peer.send(0, []byte(`{"jsonrpc":"2.0","id":1,"method":"plugin.initialize","params":{}}`))
	if kind, payload := peer.read(); kind != 0 || !bytes.Contains(payload, []byte(`"runtimeProtocolVersion":1`)) {
		t.Fatalf("initialize frame kind=%d %s", kind, payload)
	}
	peer.sendBinary("pty/1", []byte{0, '\n', 0xff})
	select {
	case got := <-received:
		if got != "pty/1=\x00\n\xff" {
			t.Fatalf("OnBinary got %q", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("binary frame not delivered")
	}
	kind, payload := peer.read()
	if kind != 1 || string(payload[2:2+int(payload[1])]) != "echo/pty/1" || !bytes.Equal(payload[2+int(payload[1]):], []byte("echo:\x00\n\xff")) {
		t.Fatalf("echo frame kind=%d %q", kind, payload)
	}

	peer.send(0, []byte(`{"jsonrpc":"2.0","id":2,"method":"plugin.shutdown","params":{}}`))
	if kind, payload := peer.read(); kind != 0 || !bytes.Contains(payload, []byte(`"id":2`)) {
		t.Fatalf("shutdown reply kind=%d %s", kind, payload)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestFramedTransportRejectsCorruptFrames(t *testing.T) {
	o := rpc.StrictDefaults("x")
	o.Transport = rpc.TransportFramed
	o.Handler = func(context.Context, string, json.RawMessage) (any, error) { return nil, nil }
	err := rpc.Serve(context.Background(), bytes.NewReader([]byte{7, 0, 0, 0, 0}), io.Discard, io.Discard, o)
	if err == nil {
		t.Fatal("unknown frame kind accepted")
	}
}

func TestSendBinaryNeedsFramedServer(t *testing.T) {
	if err := rpc.SendBinary(context.Background(), "x", nil); !errors.Is(err, rpc.ErrBinaryUnavailable) {
		t.Fatalf("SendBinary outside a server: %v", err)
	}
}
