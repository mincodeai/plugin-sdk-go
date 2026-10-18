// Package dbdrivertest helps test database driver plugins built with
// dbdriver: Start runs a Server behind rpc.Serve on an in-memory pipe and
// Client speaks the host side of protocol v1; Check drives a Driver through
// the host contract (connect, navigation, execute with paging, cancel,
// close); FakeSQL is a scriptable database/sql driver for FromSQL tests.
package dbdrivertest

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/mincodeai/plugin-sdk-go/dbdriver"
	"github.com/mincodeai/plugin-sdk-go/rpc"
)

// Reply is one plugin reply.
type Reply struct {
	ID     string
	Result json.RawMessage
	Error  *rpc.Error
}

// Client is the host side of a running plugin.
type Client struct {
	t      testing.TB
	in     *io.PipeWriter
	mu     sync.Mutex
	nextID int
	wait   map[string]chan Reply
	done   chan error
	Server *dbdriver.Server
}

// Start serves drivers with rpc.StrictDefaults (plus the database handler)
// and sends plugin.initialize offering features.databaseDrivers. Customize
// the Options with configure (may be nil). The plugin is shut down when the
// test ends.
func Start(t testing.TB, drivers map[string]dbdriver.Driver, so dbdriver.ServerOptions, configure func(*rpc.Options)) *Client {
	t.Helper()
	srv := dbdriver.NewServer(drivers, so)
	o := rpc.StrictDefaults("dbdrivertest")
	o.Database = srv
	if configure != nil {
		configure(&o)
	}
	inR, inW := io.Pipe()
	outR, outW := io.Pipe()
	c := &Client{t: t, in: inW, wait: map[string]chan Reply{}, done: make(chan error, 1), Server: srv}
	go func() {
		c.done <- rpc.Serve(context.Background(), inR, outW, io.Discard, o)
		_ = outW.Close()
	}()
	go c.read(outR)
	t.Cleanup(func() {
		_ = inW.Close()
		select {
		case <-c.done:
		case <-time.After(10 * time.Second):
			t.Errorf("dbdrivertest: plugin did not stop")
		}
		_ = outR.Close()
	})
	res, err := c.Call("plugin.initialize", map[string]any{
		"apiVersion": 1, "runtimeProtocolVersion": 1, "manifestApiVersion": 2,
		"features": map[string]any{"databaseDrivers": 1},
		"plugin":   map[string]any{"id": "dbdrivertest", "name": "dbdrivertest", "version": "0.0.0", "capabilities": []string{"database.driver"}},
		"host":     map[string]any{"name": "MinCode", "platform": "test"},
	})
	if err != nil {
		t.Fatalf("plugin.initialize: %v", err)
	}
	var init struct {
		Features struct {
			DatabaseDrivers int `json:"databaseDrivers"`
		} `json:"features"`
	}
	if json.Unmarshal(res, &init) != nil || init.Features.DatabaseDrivers != dbdriver.Protocol {
		t.Fatalf("plugin.initialize did not negotiate databaseDrivers: %s", res)
	}
	return c
}

func (c *Client) read(r io.Reader) {
	sc := bufio.NewScanner(r)
	// Protocol 1.1 pages reach MaxPageBytes plus the envelope; the host
	// accepts such replies up to 4 MiB.
	sc.Buffer(make([]byte, 0, 64<<10), 4<<20)
	for sc.Scan() {
		var m struct {
			ID     json.RawMessage `json:"id"`
			Result json.RawMessage `json:"result"`
			Error  *struct {
				Code    int             `json:"code"`
				Message string          `json:"message"`
				Data    json.RawMessage `json:"data"`
			} `json:"error"`
		}
		if json.Unmarshal(sc.Bytes(), &m) != nil {
			continue
		}
		var id string
		if json.Unmarshal(m.ID, &id) != nil {
			continue
		}
		rep := Reply{ID: id, Result: m.Result}
		if m.Error != nil {
			rep.Error = &rpc.Error{Code: m.Error.Code, Message: m.Error.Message, Data: m.Error.Data}
		}
		c.mu.Lock()
		ch := c.wait[id]
		delete(c.wait, id)
		c.mu.Unlock()
		if ch != nil {
			ch <- rep
		}
	}
}

// Send writes a request and returns a channel receiving its reply.
func (c *Client) Send(method string, params any) <-chan Reply {
	c.t.Helper()
	raw, err := json.Marshal(params)
	if err != nil {
		c.t.Fatalf("marshal %s params: %v", method, err)
	}
	c.mu.Lock()
	c.nextID++
	id := strconv.Itoa(c.nextID)
	ch := make(chan Reply, 1)
	c.wait[id] = ch
	c.mu.Unlock()
	line, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": id, "method": method, "params": json.RawMessage(raw)})
	if _, err := c.in.Write(append(line, '\n')); err != nil {
		c.t.Fatalf("write %s: %v", method, err)
	}
	return ch
}

// Call sends a request and waits (30 s at most) for its reply.
func (c *Client) Call(method string, params any) (json.RawMessage, *rpc.Error) {
	c.t.Helper()
	return c.Await(c.Send(method, params))
}

// Await waits for a reply returned by Send.
func (c *Client) Await(ch <-chan Reply) (json.RawMessage, *rpc.Error) {
	c.t.Helper()
	select {
	case r := <-ch:
		return r.Result, r.Error
	case <-time.After(30 * time.Second):
		c.t.Fatalf("no reply within 30s")
		return nil, nil
	}
}

// ErrorKind returns data.kind of e ("" when absent).
func ErrorKind(e *rpc.Error) string {
	if e == nil {
		return ""
	}
	var d struct {
		Kind string `json:"kind"`
	}
	_ = json.Unmarshal(e.Data, &d)
	return d.Kind
}

// Page is an execute, tableRows or fetch result.
type Page struct {
	Columns       []dbdriver.Column `json:"columns"`
	Rows          [][]any           `json:"rows"`
	AffectedRows  *int64            `json:"affectedRows"`
	Notice        string            `json:"notice"`
	Cursor        string            `json:"cursor"`
	Done          bool              `json:"done"`
	NextPageToken string            `json:"nextPageToken"`
	// Encoding and Data carry an experimental columnar page
	// (dbdriver.EncodingColumnar); DecodePage decodes Data into Rows.
	Encoding string `json:"encoding"`
	Data     string `json:"data"`
}

// DecodePage decodes a page (numbers as json.Number). A columnar page is
// decoded into Rows with DecodeColumnar, so JSON and columnar pages of the
// same result compare equal.
func DecodePage(raw json.RawMessage) (Page, error) {
	var p Page
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	if err := d.Decode(&p); err != nil {
		return p, err
	}
	if p.Encoding == "" && p.Data == "" {
		return p, nil
	}
	if p.Encoding != dbdriver.EncodingColumnar || p.Rows != nil {
		return p, errors.New("page mixes rows with an unknown or extra encoding")
	}
	rows, err := DecodeColumnar(p.Data)
	p.Rows = rows
	return p, err
}

// ConnectParams returns plugin.db.connect params with protocol default limits.
func ConnectParams(driverID string, config map[string]any) map[string]any {
	cfg := map[string]any{"ssl": false, "options": map[string]any{}}
	for k, v := range config {
		cfg[k] = v
	}
	return map[string]any{
		"driverId":  driverID,
		"config":    cfg,
		"limits":    map[string]any{"pageMaxBytes": dbdriver.DefaultPageMaxBytes, "pageMaxRows": dbdriver.DefaultPageMaxRows, "cursorIdleMs": 60000},
		"timeoutMs": 30000,
	}
}

// Connect opens a handle and fails the test on error.
func (c *Client) Connect(driverID string, config map[string]any) string {
	c.t.Helper()
	res, err := c.Call("plugin.db.connect", ConnectParams(driverID, config))
	if err != nil {
		c.t.Fatalf("plugin.db.connect: %d %s", err.Code, err.Message)
	}
	var r struct {
		Handle string `json:"handle"`
	}
	_ = json.Unmarshal(res, &r)
	if r.Handle == "" {
		c.t.Fatalf("plugin.db.connect returned no handle: %s", res)
	}
	return r.Handle
}

// Query executes statement and fetches every page (maxRows per page).
func (c *Client) Query(handle, requestID, statement string, maxRows int) (Page, *rpc.Error) {
	c.t.Helper()
	res, e := c.Call("plugin.db.execute", map[string]any{
		"handle": handle, "requestId": requestID, "statement": statement, "mode": "auto",
		"readOnly": false, "maxRows": maxRows, "maxBytes": dbdriver.DefaultPageMaxBytes, "timeoutMs": 60000,
	})
	if e != nil {
		return Page{}, e
	}
	all, err := DecodePage(res)
	if err != nil {
		c.t.Fatalf("decode execute result: %v", err)
	}
	for !all.Done {
		if all.Cursor == "" {
			c.t.Fatalf("page is not done but has no cursor")
		}
		res, e := c.Call("plugin.db.fetch", map[string]any{"handle": handle, "cursor": all.Cursor, "maxRows": maxRows, "maxBytes": dbdriver.DefaultPageMaxBytes})
		if e != nil {
			return all, e
		}
		p, err := DecodePage(res)
		if err != nil {
			c.t.Fatalf("decode fetch result: %v", err)
		}
		all.Rows = append(all.Rows, p.Rows...)
		all.Cursor, all.Done = p.Cursor, p.Done
		if p.Notice != "" {
			all.Notice = p.Notice
		}
	}
	all.Cursor = ""
	return all, nil
}

// Check describes a contract run of a Driver (see CheckDriver).
type Check struct {
	// DriverID is the id to connect with (default "driver").
	DriverID string
	// Config is the plugin.db.connect config (ssl/options default).
	Config map[string]any
	// Query returns at least WantRows rows (default "SELECT 1", 1 row).
	Query    string
	WantRows int
	// SlowQuery, when set, must run until cancelled (plugin.db.cancel must
	// end it with kind cancelled).
	SlowQuery string
	// BadQuery, when set, must fail with kind database.
	BadQuery string
}

// CheckDriver runs d through the host contract: connect, navigation
// metadata, a query paged one row at a time, optional cancel and error
// cases, close, and shutdown with no handle left open.
func CheckDriver(t *testing.T, d dbdriver.Driver, ch Check) {
	t.Helper()
	if ch.DriverID == "" {
		ch.DriverID = "driver"
	}
	if ch.Query == "" {
		ch.Query, ch.WantRows = "SELECT 1", 1
	}
	c := Start(t, map[string]dbdriver.Driver{ch.DriverID: d}, dbdriver.ServerOptions{}, nil)
	h := c.Connect(ch.DriverID, ch.Config)
	if res, e := c.Call("plugin.db.metadata", map[string]any{"handle": h, "kind": "navigation", "maxBytes": dbdriver.DefaultPageMaxBytes}); e != nil {
		t.Errorf("navigation metadata: %d %s", e.Code, e.Message)
	} else {
		var v struct {
			Value *dbdriver.SchemaInfo `json:"value"`
		}
		if err := json.Unmarshal(res, &v); err != nil || v.Value == nil {
			t.Errorf("navigation metadata is not a SchemaInfo value: %s", res)
		}
	}
	p, e := c.Query(h, "check-q", ch.Query, 1)
	if e != nil {
		t.Errorf("query: %d %s", e.Code, e.Message)
	} else if len(p.Rows) < ch.WantRows {
		t.Errorf("query returned %d rows, want at least %d", len(p.Rows), ch.WantRows)
	}
	if ch.SlowQuery != "" {
		slow := c.Send("plugin.db.execute", map[string]any{
			"handle": h, "requestId": "check-slow", "statement": ch.SlowQuery, "mode": "auto",
			"readOnly": false, "maxRows": 10, "maxBytes": dbdriver.DefaultPageMaxBytes, "timeoutMs": 60000,
		})
		var state string
		deadline := time.Now().Add(10 * time.Second)
		for time.Now().Before(deadline) {
			res, e := c.Call("plugin.db.cancel", map[string]any{"handle": h, "requestId": "check-slow"})
			if e != nil {
				t.Fatalf("cancel: %d %s", e.Code, e.Message)
			}
			var r struct {
				State string `json:"state"`
			}
			_ = json.Unmarshal(res, &r)
			if state = r.State; state != "notFound" {
				break
			}
			time.Sleep(10 * time.Millisecond)
		}
		if state != "confirmed" && state != "requested" {
			t.Errorf("cancel state %q", state)
		}
		if _, e := c.Await(slow); ErrorKind(e) != "cancelled" {
			t.Errorf("cancelled query: want kind cancelled, got %v", e)
		}
	}
	if ch.BadQuery != "" {
		if _, e := c.Query(h, "check-bad", ch.BadQuery, 10); ErrorKind(e) != "database" {
			t.Errorf("bad query: want kind database, got %v", e)
		}
	}
	if res, e := c.Call("plugin.db.close", map[string]any{"handle": h}); e != nil || string(res) != "true" {
		t.Errorf("close: %s %v", res, e)
	}
	if n := c.Server.OpenHandles(); n != 0 {
		t.Errorf("%d handles still open after close", n)
	}
}
