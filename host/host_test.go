package host

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"
)

type wire struct {
	mu    sync.Mutex
	lines []string
	sent  chan string
}

func newWire() *wire { return &wire{sent: make(chan string, 16)} }

func (w *wire) send(line []byte) error {
	w.mu.Lock()
	w.lines = append(w.lines, string(line))
	w.mu.Unlock()
	w.sent <- string(line)
	return nil
}

func TestCallDeliver(t *testing.T) {
	w := newWire()
	c := NewClient(w.send, Options{IDPrefix: "p-host-"})
	done := make(chan error, 1)
	var got json.RawMessage
	go func() {
		var err error
		got, err = c.Call(context.Background(), "host.storage.get", map[string]any{"key": "<k>"})
		done <- err
	}()
	line := <-w.sent
	if line != `{"jsonrpc":"2.0","id":"p-host-1","method":"host.storage.get","params":{"key":"<k>"}}` {
		t.Fatalf("line = %s", line)
	}
	if c.Deliver(json.RawMessage(`1`), nil, nil) || c.Deliver(json.RawMessage(`"other"`), nil, nil) {
		t.Fatal("foreign ids must not match")
	}
	if !c.Deliver(json.RawMessage(`"p-host-1"`), json.RawMessage(`{"v":1}`), nil) {
		t.Fatal("Deliver did not match")
	}
	if err := <-done; err != nil || string(got) != `{"v":1}` {
		t.Fatalf("got %s, %v", got, err)
	}
	if c.Deliver(json.RawMessage(`"p-host-1"`), nil, nil) {
		t.Fatal("a reply is delivered once")
	}
}

func TestCallErrorAndNullResult(t *testing.T) {
	w := newWire()
	c := NewClient(w.send, Options{})
	go func() {
		<-w.sent
		c.Deliver(json.RawMessage(`"1"`), nil, &WireError{Code: CodeCapabilityMissing, Message: "denied"})
		<-w.sent
		c.Deliver(json.RawMessage(`"2"`), nil, nil)
	}()
	_, err := c.Call(context.Background(), "host.secrets.get", nil)
	var he *Error
	if !errors.As(err, &he) || he.Message != "denied" || !IsCode(err, -32002, CodeCapabilityMissing) || IsCode(err, -32002) {
		t.Fatalf("err = %v", err)
	}
	if IsCode(errors.New("x"), CodeCapabilityMissing) {
		t.Fatal("IsCode on plain error")
	}
	res, err := c.Call(context.Background(), "host.secrets.get", nil)
	if err != nil || string(res) != "null" {
		t.Fatalf("res = %s, %v", res, err)
	}
}

func TestTimeoutContextCloseDone(t *testing.T) {
	w := newWire()
	timeoutErr := errors.New("timed out")
	c := NewClient(w.send, Options{Timeout: 20 * time.Millisecond, TimeoutError: timeoutErr})
	if _, err := c.Call(context.Background(), "m", nil); err != timeoutErr {
		t.Fatalf("timeout: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	go func() { <-w.sent; <-w.sent; cancel() }()
	if _, err := c.Call(ctx, "m", nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancel: %v", err)
	}

	c = NewClient(w.send, Options{})
	go func() { <-w.sent; c.Close(true) }()
	if _, err := c.Call(context.Background(), "m", nil); err != ErrUnavailable {
		t.Fatalf("close(wake): %v", err)
	}
	if _, err := c.Call(context.Background(), "m", nil); err != ErrUnavailable {
		t.Fatalf("after close: %v", err)
	}

	stop := make(chan struct{})
	unavailable := errors.New("gone")
	c = NewClient(w.send, Options{Done: stop, Unavailable: unavailable})
	go func() { <-w.sent; close(stop) }()
	if _, err := c.Call(context.Background(), "m", nil); err != unavailable {
		t.Fatalf("done: %v", err)
	}
}

func TestEncodeOptions(t *testing.T) {
	sorted := NewClient(nil, Options{SortKeys: true, EscapeHTML: true})
	b, _ := sorted.Encode("host-1", "m", map[string]string{"a": "<"})
	bs := string(rune(0x5c))
	if string(b) != `{"id":"host-1","jsonrpc":"2.0","method":"m","params":{"a":"`+bs+`u003c"}}` {
		t.Fatalf("sorted = %s", b)
	}
	big := NewClient(func([]byte) error { t.Fatal("must not send"); return nil }, Options{MaxLineBytes: 10})
	if _, err := big.Call(context.Background(), "host.storage.set", nil); err != ErrUnavailable {
		t.Fatalf("too large: %v", err)
	}
}

func TestContext(t *testing.T) {
	if FromContext(context.Background()) != nil {
		t.Fatal("empty context")
	}
	f := CallerFunc(func(context.Context, string, any) (json.RawMessage, error) { return json.RawMessage("7"), nil })
	h := FromContext(WithCaller(context.Background(), f))
	if r, _ := h.Call(context.Background(), "m", nil); string(r) != "7" {
		t.Fatal("CallerFunc")
	}
}
