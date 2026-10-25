package rpc_test

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/mincodeai/plugin-sdk-go/host"
	"github.com/mincodeai/plugin-sdk-go/rpc"
)

// largePeer is a line peer whose reader accepts the plugin's large requests.
type largePeer struct {
	t     *testing.T
	in    *io.PipeWriter
	lines chan string
}

func startLargePeer(t *testing.T, o rpc.Options) *largePeer {
	t.Helper()
	inR, inW := io.Pipe()
	outR, outW := io.Pipe()
	p := &largePeer{t: t, in: inW, lines: make(chan string, 16)}
	go func() { _ = rpc.Serve(context.Background(), inR, outW, io.Discard, o); outW.Close() }()
	go func() {
		scanner := bufio.NewScanner(outR)
		scanner.Buffer(make([]byte, 64*1024), 8<<20)
		for scanner.Scan() {
			p.lines <- scanner.Text()
		}
		close(p.lines)
	}()
	t.Cleanup(func() { inW.Close(); outR.Close() })
	return p
}

func (p *largePeer) send(line string) {
	p.t.Helper()
	if _, err := io.WriteString(p.in, line+"\n"); err != nil {
		p.t.Fatal(err)
	}
}

func (p *largePeer) next() map[string]json.RawMessage {
	p.t.Helper()
	select {
	case line, ok := <-p.lines:
		if !ok {
			p.t.Fatal("plugin output closed")
		}
		var m map[string]json.RawMessage
		if err := json.Unmarshal([]byte(line), &m); err != nil {
			p.t.Fatalf("bad line %.200s: %v", line, err)
		}
		return m
	case <-time.After(5 * time.Second):
		p.t.Fatal("no plugin output")
	}
	return nil
}

// An action reads a stored value and writes it back doubled: with
// largeStorage negotiated both directions may exceed 1 MiB.
func largeStorageOptions(optIn bool) rpc.Options {
	o := rpc.StrictDefaults("x")
	o.LargeStorage = optIn
	o.Handler = func(ctx context.Context, _ string, _ json.RawMessage) (any, error) {
		c := host.FromContext(ctx)
		raw, _, err := host.GetStorage(ctx, c, "k")
		if err != nil {
			return nil, err
		}
		var v string
		_ = json.Unmarshal(raw, &v)
		if err := host.SetStorage(ctx, c, "k", v+v); err != nil {
			return nil, err
		}
		return len(v), nil
	}
	return o
}

func TestLargeStorageRoundTrip(t *testing.T) {
	const size = 1500 * 1024 // above 1 MiB, doubled stays below 4 MiB
	p := startLargePeer(t, largeStorageOptions(true))
	p.send(`{"jsonrpc":"2.0","id":"i","method":"plugin.initialize","params":{"features":{"largeStorage":1}}}`)
	if r := p.next(); !strings.Contains(string(r["result"]), `"features":{"largeStorage":1}`) {
		t.Fatalf("initialize must echo largeStorage: %s", r["result"])
	}
	p.send(`{"jsonrpc":"2.0","id":"a","method":"plugin.invoke","params":{"action":"x","payload":"{}"}}`)
	get := p.next()
	p.send(`{"jsonrpc":"2.0","id":` + string(get["id"]) + `,"result":"` + strings.Repeat("x", size) + `"}`)
	set := p.next()
	var params struct{ Value string }
	if err := json.Unmarshal(set["params"], &params); err != nil || len(params.Value) != 2*size {
		t.Fatalf("host.storage.set value %d bytes, err %v", len(params.Value), err)
	}
	p.send(`{"jsonrpc":"2.0","id":` + string(set["id"]) + `,"result":true}`)
	if r := p.next(); !strings.Contains(string(r["result"]), strconv.Itoa(size)) {
		t.Fatalf("invoke: %s %s", r["result"], r["error"])
	}
}

// Without the plugin's opt-in (or the host's offer) the 1 MiB input limit
// stays: StrictDefaults stops the transport on the oversized reply.
func TestLargeStorageNeedsBothSides(t *testing.T) {
	for name, c := range map[string]struct {
		optIn bool
		offer string
	}{"plugin did not opt in": {false, `{"largeStorage":1}`}, "host did not offer": {true, `{}`}} {
		t.Run(name, func(t *testing.T) {
			p := startLargePeer(t, largeStorageOptions(c.optIn))
			p.send(`{"jsonrpc":"2.0","id":"i","method":"plugin.initialize","params":{"features":` + c.offer + `}}`)
			if r := p.next(); strings.Contains(string(r["result"]), "largeStorage") {
				t.Fatalf("largeStorage echoed: %s", r["result"])
			}
			p.send(`{"jsonrpc":"2.0","id":"a","method":"plugin.invoke","params":{"action":"x","payload":"{}"}}`)
			get := p.next()
			// The plugin stops reading mid-line, so write without blocking the test.
			go func() {
				_, _ = io.WriteString(p.in, `{"jsonrpc":"2.0","id":`+string(get["id"])+`,"result":"`+strings.Repeat("x", 1500*1024)+`"}`+"\n")
			}()
			for {
				select {
				case line, ok := <-p.lines:
					if !ok {
						return // transport stopped
					}
					if strings.Contains(line, `"method":"host.storage.set"`) {
						t.Fatal("oversized reply was accepted")
					}
				case <-time.After(5 * time.Second):
					t.Fatal("transport did not stop on the oversized reply")
				}
			}
		})
	}
}
