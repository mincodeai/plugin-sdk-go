package rpc_test

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/mincodeai/plugin-sdk-go/host"
	"github.com/mincodeai/plugin-sdk-go/rpc"
)

type eventErrorLog chan string

func (log eventErrorLog) Write(data []byte) (int, error) {
	log <- string(data)
	return len(data), nil
}

// Actual Serve wire with bounded stderr observation, not a subscription-helper test.
func availabilityPeer(t *testing.T) (*linePeer, eventErrorLog) {
	t.Helper()
	o := rpc.StrictDefaults("x")
	o.Handler = func(context.Context, string, json.RawMessage) (any, error) { return nil, nil }
	o.Events = []string{host.EventAppSuspend}
	o.OnEvent = func(context.Context, string, json.RawMessage, host.Caller) {}
	inR, inW := io.Pipe()
	outR, outW := io.Pipe()
	p := &linePeer{t: t, in: inW, lines: make(chan string, 16)}
	log := make(eventErrorLog, 4)
	go func() { _ = rpc.Serve(context.Background(), inR, outW, log, o); outW.Close() }()
	go func() {
		scanner := bufio.NewScanner(outR)
		for scanner.Scan() {
			p.lines <- scanner.Text()
		}
		close(p.lines)
	}()
	t.Cleanup(func() { inW.Close(); outR.Close() })
	return p, log
}

func availabilityRequest(t *testing.T, p *linePeer) map[string]json.RawMessage {
	t.Helper()
	line, ok := p.next(5 * time.Second)
	if !ok {
		t.Fatal("missing initialization RPC")
	}
	var message map[string]json.RawMessage
	if err := json.Unmarshal([]byte(line), &message); err != nil {
		t.Fatal(err)
	}
	return message
}

func TestSearchCallerInitializationSkipsEmptySubscriptions(t *testing.T) {
	for _, description := range []string{
		`{"events":[{"name":"app.suspend","available":false},{"name":"workspace.changed","available":true}]}`,
		`{"events":[{"name":"workspace.changed","available":true}]}`,
		`{"events":[{"name":"app.suspend"}]}`,
		`{}`,
	} {
		t.Run(description, func(t *testing.T) {
			p, log := availabilityPeer(t)
			p.send(`{"jsonrpc":"2.0","id":1,"method":"plugin.initialize","params":{}}`)
			availabilityRequest(t, p)
			request := availabilityRequest(t, p)
			if string(request["method"]) != `"host.api.describe"` {
				t.Fatalf("must discover first: %s", request)
			}
			p.send(`{"jsonrpc":"2.0","id":` + string(request["id"]) + `,"result":` + description + `}`)
			// No empty subscribe: it would clear an independently established set.
			if line, ok := p.next(100 * time.Millisecond); ok {
				t.Fatalf("no available requested events must not subscribe: %s", line)
			}
			p.send(`{"jsonrpc":"2.0","id":2,"method":"plugin.initialize","params":{}}`)
			if response := availabilityRequest(t, p); string(response["id"]) != "2" {
				t.Fatalf("reinitialize: %s", response)
			}
			if line, ok := p.next(50 * time.Millisecond); ok {
				t.Fatalf("automatic discovery must run once: %s", line)
			}
			select {
			case message := <-log:
				t.Fatalf("unsupported is not an RPC failure: %s", message)
			default:
			}
		})
	}
}

func TestSearchCallerInitializationErrorsRemainVisible(t *testing.T) {
	for _, step := range []string{"describe", "subscribe", "malformed", "null"} {
		t.Run(step, func(t *testing.T) {
			p, log := availabilityPeer(t)
			p.send(`{"jsonrpc":"2.0","id":1,"method":"plugin.initialize","params":{}}`)
			availabilityRequest(t, p)
			request := availabilityRequest(t, p)
			if string(request["method"]) != `"host.api.describe"` {
				t.Fatalf("must discover first: %s", request)
			}
			if step == "subscribe" {
				p.send(`{"jsonrpc":"2.0","id":` + string(request["id"]) + `,"result":{"events":[{"name":"app.suspend","available":true}]}}`)
				request = availabilityRequest(t, p)
				if string(request["method"]) != `"host.events.subscribe"` {
					t.Fatalf("subscription: %s", request)
				}
			}
			if step == "null" {
				p.send(`{"jsonrpc":"2.0","id":` + string(request["id"]) + `,"result":null}`)
			} else if step == "malformed" {
				p.send(`{"jsonrpc":"2.0","id":` + string(request["id"]) + `,"result":{"events":{}}}`)
			} else {
				p.send(`{"jsonrpc":"2.0","id":` + string(request["id"]) + `,"error":{"code":-32601,"message":"old host unavailable"}}`)
			}
			select {
			case message := <-log:
				if !strings.Contains(message, "plugin event subscription failed") {
					t.Fatalf("failure log: %q", message)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("RPC failure was swallowed")
			}
			if line, ok := p.next(50 * time.Millisecond); ok {
				t.Fatalf("must not fall back or retry unfiltered events: %s", line)
			}
		})
	}
}

// Reflection keeps RED a behavioral assertion, not a missing-field compile error.
func TestSearchCallerCombinedFeatures(t *testing.T) {
	o := rpc.StrictDefaults("x")
	o.SearchFileTargets = true
	o.ServiceCallerMetadata = true
	o.LargeStorage = true
	o.Handler = func(context.Context, string, json.RawMessage) (any, error) { return nil, nil }
	p := startLargePeer(t, o)
	p.send(`{"jsonrpc":"2.0","id":1,"method":"plugin.initialize","params":{"features":{"searchFileTargets":1,"serviceCallerMetadata":1,"largeStorage":1}}}`)
	var result struct {
		Features map[string]int `json:"features"`
	}
	if err := json.Unmarshal(p.next()["result"], &result); err != nil {
		t.Fatal(err)
	}
	want := map[string]int{"searchFileTargets": 1, "serviceCallerMetadata": 1, "largeStorage": 1}
	if !reflect.DeepEqual(result.Features, want) {
		t.Fatalf("combined features = %v, want %v", result.Features, want)
	}
}

func TestSearchCallerFeatureMatrix(t *testing.T) {
	for _, feature := range []struct{ field, wire string }{{"SearchFileTargets", "searchFileTargets"}, {"ServiceCallerMetadata", "serviceCallerMetadata"}} {
		for _, optIn := range []bool{false, true} {
			for _, offer := range []string{`{}`, `{"` + feature.wire + `":1}`, `{"` + feature.wire + `":2}`, `{"` + feature.wire + `":"1"}`, `{"` + feature.wire + `":true}`} {
				t.Run(fmt.Sprintf("%s/%v/%s", feature.field, optIn, offer), func(t *testing.T) {
					o := rpc.StrictDefaults("x")
					o.Handler = func(_ context.Context, _ string, payload json.RawMessage) (any, error) {
						var old struct{ Provider, Query, Locale string }
						decoder := json.NewDecoder(strings.NewReader(string(payload)))
						decoder.DisallowUnknownFields()
						if err := decoder.Decode(&old); err != nil {
							return nil, err
						}
						return old.Query, nil
					}
					field := reflect.ValueOf(&o).Elem().FieldByName(feature.field)
					if !field.IsValid() {
						t.Fatalf("missing explicit default-false opt-in %s", feature.field)
					}
					if field.Bool() {
						t.Fatal("opt-in enabled by default")
					}
					field.SetBool(optIn)
					p := startLargePeer(t, o)
					p.send(`{"jsonrpc":"2.0","id":1,"method":"plugin.initialize","params":{"features":` + offer + `}}`)
					response := p.next()
					var result struct {
						Features map[string]int `json:"features"`
					}
					if err := json.Unmarshal(response["result"], &result); err != nil {
						t.Fatal(err)
					}
					want := 0
					if optIn && offer == `{"`+feature.wire+`":1}` {
						want = 1
					}
					if got := result.Features[feature.wire]; got != want {
						t.Fatalf("echo = %d, want %d", got, want)
					}
					p.send(`{"jsonrpc":"2.0","id":2,"method":"plugin.invoke","params":{"action":"search","payload":"{\"provider\":\"files\",\"query\":\"main\",\"locale\":\"en\"}"}}`)
					if reply := p.next(); string(reply["result"]) != `"main"` {
						t.Fatalf("strict old payload: %s", reply)
					}
				})
			}
		}
	}
}
