// Package host implements reverse JSON-RPC calls from a plugin to the MinCode
// host (host.storage.*, host.secrets.*, ...). The rpc package owns the stdio
// loop; it sends Client request lines and routes host replies to Deliver.
package host

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"sync"
	"time"
)

// Caller performs one reverse request and returns the raw JSON result
// ("null" when the host answered without a result).
type Caller interface {
	Call(ctx context.Context, method string, params any) (json.RawMessage, error)
}

// CallerFunc adapts a function to Caller.
type CallerFunc func(ctx context.Context, method string, params any) (json.RawMessage, error)

// Call implements Caller.
func (f CallerFunc) Call(ctx context.Context, method string, params any) (json.RawMessage, error) {
	return f(ctx, method, params)
}

// Error is a JSON-RPC error returned by the host (e.g. -32001 capability not
// granted, -32002 storage/secrets failure, -32602 invalid params).
type Error struct {
	Code    int
	Message string
	Data    json.RawMessage
}

func (e *Error) Error() string { return "host error " + strconv.Itoa(e.Code) }

// Common host error codes.
const (
	CodeCapabilityMissing = -32001
	CodeStorageFailure    = -32002
	CodeHostRequestFailed = -32003
)

// IsCode reports whether err is a host *Error with one of the codes.
func IsCode(err error, codes ...int) bool {
	var he *Error
	if !errors.As(err, &he) {
		return false
	}
	for _, c := range codes {
		if he.Code == c {
			return true
		}
	}
	return false
}

// ErrUnavailable is the default error for calls made after the transport
// stopped or whose request could not be written.
var ErrUnavailable = errors.New("host unavailable")

// Options configure a Client.
type Options struct {
	// IDPrefix is prepended to a per-client counter ("nats-client-host-" → "nats-client-host-1").
	IDPrefix string
	// Timeout bounds one call (default 10s). Expiry returns context.DeadlineExceeded
	// unless TimeoutError is set.
	Timeout      time.Duration
	TimeoutError error
	// EscapeHTML encodes request lines like json.Marshal (<, >, & → <...).
	EscapeHTML bool
	// MaxLineBytes rejects a request whose encoded line (without the newline)
	// is >= MaxLineBytes (or > when MaxLineInclusive), returning TooLarge.
	MaxLineBytes     int
	MaxLineInclusive bool
	TooLarge         error
	// Unavailable is returned after Close, when Done fires or the send fails
	// (default ErrUnavailable).
	Unavailable error
	// SendErrorIgnored keeps waiting even when the line could not be written.
	SendErrorIgnored bool
	// Done, when set, fails pending calls with Unavailable once it is closed.
	Done <-chan struct{}
	// SortKeys writes request members in sorted key order
	// ({"id","jsonrpc","method","params"}), like plugins that encoded a map.
	SortKeys bool
}

// Request is the wire form of a reverse request (field order is part of the protocol bytes).
type Request struct {
	JSONRPC string `json:"jsonrpc"`
	ID      string `json:"id"`
	Method  string `json:"method"`
	Params  any    `json:"params"`
}

// WireError is the error member of a host reply.
type WireError struct {
	Code    int             `json:"code"`
	Message string          `json:"message"`
	Data    json.RawMessage `json:"data,omitempty"`
}

type reply struct {
	result json.RawMessage
	err    *WireError
}

// Client correlates reverse requests with host replies.
type Client struct {
	o       Options
	send    func(line []byte) error
	mu      sync.Mutex
	next    uint64
	pending map[string]chan reply
	closed  chan struct{}
	once    sync.Once
}

// NewClient returns a client that writes each request line (without the
// trailing newline) through send.
func NewClient(send func(line []byte) error, o Options) *Client {
	if o.Timeout == 0 {
		o.Timeout = 10 * time.Second
	}
	if o.Unavailable == nil {
		o.Unavailable = ErrUnavailable
	}
	if o.TooLarge == nil {
		o.TooLarge = o.Unavailable
	}
	return &Client{o: o, send: send, pending: map[string]chan reply{}, closed: make(chan struct{})}
}

// Encode renders a request line exactly as the client would send it.
func (c *Client) Encode(id, method string, params any) ([]byte, error) {
	var req any = Request{JSONRPC: "2.0", ID: id, Method: method, Params: params}
	if c.o.SortKeys {
		req = map[string]any{"jsonrpc": "2.0", "id": id, "method": method, "params": params}
	}
	if c.o.EscapeHTML {
		return json.Marshal(req)
	}
	var b bytes.Buffer
	enc := json.NewEncoder(&b)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(req); err != nil {
		return nil, err
	}
	return bytes.TrimSuffix(b.Bytes(), []byte("\n")), nil
}

// Call implements Caller.
func (c *Client) Call(ctx context.Context, method string, params any) (json.RawMessage, error) {
	c.mu.Lock()
	if c.pending == nil {
		c.mu.Unlock()
		return nil, c.o.Unavailable
	}
	c.next++
	id := c.o.IDPrefix + strconv.FormatUint(c.next, 10)
	ch := make(chan reply, 1)
	c.pending[id] = ch
	c.mu.Unlock()
	defer func() {
		c.mu.Lock()
		if c.pending != nil {
			delete(c.pending, id)
		}
		c.mu.Unlock()
	}()
	line, err := c.Encode(id, method, params)
	if err != nil {
		return nil, c.o.Unavailable
	}
	if c.o.MaxLineBytes > 0 && (len(line) >= c.o.MaxLineBytes && !c.o.MaxLineInclusive || len(line) > c.o.MaxLineBytes) {
		return nil, c.o.TooLarge
	}
	if err := c.send(line); err != nil && !c.o.SendErrorIgnored {
		return nil, c.o.Unavailable
	}
	timer := time.NewTimer(c.o.Timeout)
	defer timer.Stop()
	done := c.o.Done
	select {
	case r := <-ch:
		if r.err != nil {
			return nil, &Error{Code: r.err.Code, Message: r.err.Message, Data: r.err.Data}
		}
		if len(r.result) == 0 {
			return json.RawMessage("null"), nil
		}
		return r.result, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-timer.C:
		if c.o.TimeoutError != nil {
			return nil, c.o.TimeoutError
		}
		return nil, context.DeadlineExceeded
	case <-done:
		return nil, c.o.Unavailable
	case <-c.closed:
		return nil, c.o.Unavailable
	}
}

// Deliver routes a host reply to its waiting call and reports whether the id
// belonged to an outstanding request. Non-string ids never match.
func (c *Client) Deliver(rawID json.RawMessage, result json.RawMessage, e *WireError) bool {
	var id string
	if json.Unmarshal(rawID, &id) != nil {
		return false
	}
	c.mu.Lock()
	ch, ok := c.pending[id]
	if ok {
		delete(c.pending, id)
	}
	c.mu.Unlock()
	if ok {
		ch <- reply{result: result, err: e}
	}
	return ok
}

// Close rejects new calls. When wake is true, calls already waiting return
// Unavailable immediately; otherwise they keep waiting for their reply,
// context, timeout or Done (which is how most plugins behaved).
func (c *Client) Close(wake bool) {
	c.mu.Lock()
	c.pending = nil
	c.mu.Unlock()
	if wake {
		c.once.Do(func() { close(c.closed) })
	}
}

type ctxKey struct{}

// WithCaller attaches a Caller to ctx.
func WithCaller(ctx context.Context, h Caller) context.Context {
	return context.WithValue(ctx, ctxKey{}, h)
}

// FromContext returns the Caller attached by the rpc loop (nil outside an invoke).
func FromContext(ctx context.Context) Caller {
	h, _ := ctx.Value(ctxKey{}).(Caller)
	return h
}
