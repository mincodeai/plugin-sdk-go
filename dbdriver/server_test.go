package dbdriver_test

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mincodeai/plugin-sdk-go/dbdriver"
	"github.com/mincodeai/plugin-sdk-go/dbdriver/dbdrivertest"
)

// memConn is a scriptable Conn.
type memConn struct {
	mu      sync.Mutex
	closed  bool
	rowsOut int // open Rows
	exec    func(ctx context.Context, req dbdriver.ExecuteRequest) (*dbdriver.Result, error)
}

func (c *memConn) Execute(ctx context.Context, req dbdriver.ExecuteRequest) (*dbdriver.Result, error) {
	return c.exec(ctx, req)
}

func (c *memConn) Close() error {
	c.mu.Lock()
	c.closed = true
	c.mu.Unlock()
	return nil
}

func (c *memConn) Metadata(_ context.Context, req dbdriver.MetadataRequest) (*dbdriver.MetadataResult, error) {
	switch req.Kind {
	case dbdriver.MetadataNavigation:
		return &dbdriver.MetadataResult{Value: dbdriver.SchemaInfo{Databases: []dbdriver.SchemaDatabase{{Name: "d", Tables: []dbdriver.SchemaTable{}}}}}, nil
	case dbdriver.MetadataColumns:
		cols := make([]dbdriver.ColumnInfo, 50)
		for i := range cols {
			cols[i] = dbdriver.ColumnInfo{Name: fmt.Sprintf("col_%02d", i), DataType: "text", Nullable: true}
		}
		return &dbdriver.MetadataResult{Items: dbdriver.Items(cols)}, nil
	}
	return nil, dbdriver.ErrUnsupported
}

func (c *memConn) TableRows(_ context.Context, req dbdriver.TableRowsRequest) (*dbdriver.Result, error) {
	token := "next:" + req.PageToken
	if req.Table == "badtoken" {
		token = "bad\ntoken"
	}
	return &dbdriver.Result{Rows: dbdriver.SliceRows([]dbdriver.Column{{Name: "t"}}, [][]any{{req.Table}}), NextPageToken: token}, nil
}

// countRows yields n rows of (i, payload) and tracks open/closed state.
type countRows struct {
	c       *memConn
	n, i    int
	payload string
	closed  bool
}

func (r *countRows) Columns() []dbdriver.Column {
	return []dbdriver.Column{{Name: "i", Type: "int"}, {Name: "p"}}
}

func (r *countRows) Next(ctx context.Context) ([]any, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if r.i >= r.n {
		return nil, errEOF
	}
	r.i++
	return []any{r.i, r.payload}, nil
}

func (r *countRows) Close() error {
	r.c.mu.Lock()
	if !r.closed {
		r.closed = true
		r.c.rowsOut--
	}
	r.c.mu.Unlock()
	return nil
}

var errEOF = io.EOF

func newMem() *memConn {
	c := &memConn{}
	c.exec = func(ctx context.Context, req dbdriver.ExecuteRequest) (*dbdriver.Result, error) {
		var n, size int
		switch {
		case req.Statement == "block":
			<-ctx.Done()
			return nil, ctx.Err()
		case req.Statement == "fail":
			return nil, errors.New("boom with s3cret inside")
		case req.Statement == "update":
			return &dbdriver.Result{AffectedRows: dbdriver.AffectedRows(4)}, nil
		case strings.HasPrefix(req.Statement, "rows "):
			_, _ = fmt.Sscanf(req.Statement, "rows %d %d", &n, &size)
		default:
			return nil, dbdriver.Errorf(dbdriver.KindDatabase, "unknown statement")
		}
		c.mu.Lock()
		c.rowsOut++
		c.mu.Unlock()
		return &dbdriver.Result{Rows: &countRows{c: c, n: n, payload: strings.Repeat("x", size)}}, nil
	}
	return c
}

func memDriver(c *memConn) dbdriver.Driver {
	return dbdriver.DriverFunc(func(ctx context.Context, cfg dbdriver.Config) (dbdriver.Conn, dbdriver.ConnInfo, error) {
		if cfg.Password.Reveal() == "wrong" {
			return nil, dbdriver.ConnInfo{}, errors.New("access denied for password wrong")
		}
		return c, dbdriver.ConnInfo{ServerVersion: "1.0", DefaultDatabase: cfg.Database}, nil
	})
}

func execParams(h, req, stmt string, maxRows, maxBytes int) map[string]any {
	return map[string]any{"handle": h, "requestId": req, "statement": stmt, "mode": "auto", "readOnly": false,
		"maxRows": maxRows, "maxBytes": maxBytes, "timeoutMs": 60000}
}

func TestPagingWithinBudget(t *testing.T) {
	mc := newMem()
	c := dbdrivertest.Start(t, map[string]dbdriver.Driver{"mem": memDriver(mc)}, dbdriver.ServerOptions{}, nil)
	h := c.Connect("mem", map[string]any{"database": "d"})
	res, e := c.Call("plugin.db.execute", execParams(h, "q1", "rows 25 100", 10, 1<<20))
	if e != nil {
		t.Fatal(e)
	}
	p, _ := dbdrivertest.DecodePage(res)
	if len(p.Rows) != 10 || p.Done || p.Cursor == "" || len(p.Columns) != 2 || p.Columns[0].Type != "int" {
		t.Fatalf("first page %s", res)
	}
	total := len(p.Rows)
	for !p.Done {
		res, e = c.Call("plugin.db.fetch", map[string]any{"handle": h, "cursor": p.Cursor, "maxRows": 1000, "maxBytes": 1000})
		if e != nil {
			t.Fatal(e)
		}
		if len(res) > 1000 {
			t.Fatalf("page of %d bytes exceeds maxBytes", len(res))
		}
		p, _ = dbdrivertest.DecodePage(res)
		total += len(p.Rows)
	}
	if total != 25 {
		t.Fatalf("got %d rows", total)
	}
	// Exactly maxRows rows: the read-ahead marks the page done.
	res, _ = c.Call("plugin.db.execute", execParams(h, "q2", "rows 3 1", 3, 1<<20))
	if p, _ = dbdrivertest.DecodePage(res); !p.Done || p.Cursor != "" || len(p.Rows) != 3 {
		t.Fatalf("exact page %s", res)
	}
	// A row larger than the budget ends the result with a notice.
	res, e = c.Call("plugin.db.execute", execParams(h, "q3", "rows 3 5000", 10, 2000))
	if e != nil {
		t.Fatal(e)
	}
	if p, _ = dbdrivertest.DecodePage(res); !p.Done || len(p.Rows) != 0 || p.Notice == "" {
		t.Fatalf("oversized %s", res)
	}
	// Statement without rows.
	res, _ = c.Call("plugin.db.execute", execParams(h, "q4", "update", 10, 2000))
	if string(res) != `{"rows":[],"affectedRows":4,"done":true}` {
		t.Fatalf("update %s", res)
	}
	waitFor(t, func() bool { mc.mu.Lock(); defer mc.mu.Unlock(); return mc.rowsOut == 0 })
}

func TestErrorsAreClassifiedAndScrubbed(t *testing.T) {
	mc := newMem()
	c := dbdrivertest.Start(t, map[string]dbdriver.Driver{"mem": memDriver(mc)}, dbdriver.ServerOptions{}, nil)
	_, e := c.Call("plugin.db.connect", dbdrivertest.ConnectParams("mem", map[string]any{"password": "wrong"}))
	if dbdrivertest.ErrorKind(e) != "connection" || strings.Contains(e.Message, "wrong") {
		t.Fatalf("connect error %+v", e)
	}
	h := c.Connect("mem", map[string]any{"password": "s3cret"})
	_, e = c.Call("plugin.db.execute", execParams(h, "q", "fail", 10, 1000))
	if dbdrivertest.ErrorKind(e) != "database" || strings.Contains(e.Message, "s3cret") || !strings.Contains(e.Message, "[redacted]") {
		t.Fatalf("scrub %+v", e)
	}
	if _, e = c.Call("plugin.db.execute", map[string]any{"handle": h, "requestId": "q", "statement": "x", "mode": "auto", "readOnly": false, "maxRows": 1, "maxBytes": 1, "timeoutMs": 1, "extra": 1}); e == nil || e.Code != -32602 {
		t.Fatalf("unknown member %+v", e)
	}
	if _, e = c.Call("plugin.db.execute", execParams("h999", "q", "x", 1, 1)); dbdrivertest.ErrorKind(e) != "connection" {
		t.Fatalf("unknown handle %+v", e)
	}
	if _, e = c.Call("plugin.db.connect", dbdrivertest.ConnectParams("nope", nil)); dbdrivertest.ErrorKind(e) != "unsupported" {
		t.Fatalf("unknown driver %+v", e)
	}
	if _, e = c.Call("plugin.db.mutate", map[string]any{"handle": h, "requestId": "m", "op": "deleteRow", "database": "d", "table": "t", "primaryKey": map[string]any{"id": 1}}); dbdrivertest.ErrorKind(e) != "unsupported" {
		t.Fatalf("mutate unsupported %+v", e)
	}
	if _, e = c.Call("plugin.db.snapshotWrite", map[string]any{"handle": h, "requestId": "s", "database": "d", "steps": []any{map[string]any{"write": map[string]any{"statement": "update"}, "maxRows": 0}}}); dbdrivertest.ErrorKind(e) != "unsupported" {
		t.Fatalf("snapshotWrite unsupported %+v", e)
	}
	if _, e = c.Call("plugin.db.tableRows", map[string]any{"handle": h, "requestId": "t", "database": "d", "table": "t", "limit": 1, "offset": 0, "readOnly": false, "maxRows": 1, "maxBytes": 100}); e == nil || e.Code != -32602 {
		t.Fatalf("tableRows readOnly false %+v", e)
	}
	if _, e = c.Call("plugin.db.nope", map[string]any{}); e == nil || e.Code != -32601 {
		t.Fatalf("unknown method %+v", e)
	}
}

func TestCancelAndTimeout(t *testing.T) {
	mc := newMem()
	c := dbdrivertest.Start(t, map[string]dbdriver.Driver{"mem": memDriver(mc)}, dbdriver.ServerOptions{}, nil)
	h := c.Connect("mem", nil)
	slow := c.Send("plugin.db.execute", execParams(h, "q1", "block", 10, 1000))
	var state string
	for range 200 {
		res, _ := c.Call("plugin.db.cancel", map[string]any{"handle": h, "requestId": "q1"})
		var r struct{ State string }
		_ = json.Unmarshal(res, &r)
		if state = r.State; state != "notFound" {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if state != "confirmed" {
		t.Fatalf("cancel state %q", state)
	}
	if _, e := c.Await(slow); dbdrivertest.ErrorKind(e) != "cancelled" || e.Message != "query cancelled" {
		t.Fatalf("cancelled %+v", e)
	}
	p := execParams(h, "q2", "block", 10, 1000)
	p["timeoutMs"] = 20
	if _, e := c.Call("plugin.db.execute", p); dbdrivertest.ErrorKind(e) != "timeout" {
		t.Fatalf("timeout %+v", e)
	}
	// Cancelling an idle cursor closes it.
	res, _ := c.Call("plugin.db.execute", execParams(h, "q3", "rows 5 1", 1, 1000))
	pg, _ := dbdrivertest.DecodePage(res)
	res, _ = c.Call("plugin.db.cancel", map[string]any{"handle": h, "requestId": "q3"})
	if string(res) != `{"state":"confirmed"}` {
		t.Fatalf("idle cancel %s", res)
	}
	if _, e := c.Call("plugin.db.fetch", map[string]any{"handle": h, "cursor": pg.Cursor, "maxRows": 1, "maxBytes": 1000}); e == nil {
		t.Fatal("fetch after cancel succeeded")
	}
	if res, _ = c.Call("plugin.db.cancel", map[string]any{"handle": h, "requestId": "zzz"}); string(res) != `{"state":"notFound"}` {
		t.Fatalf("notFound %s", res)
	}
}

func TestBusyAndHandleLimits(t *testing.T) {
	mc := newMem()
	c := dbdrivertest.Start(t, map[string]dbdriver.Driver{"mem": memDriver(mc)}, dbdriver.ServerOptions{MaxCalls: 2, MaxHandles: 2}, nil)
	h := c.Connect("mem", nil)
	a := c.Send("plugin.db.execute", execParams(h, "a", "block", 1, 1000))
	b := c.Send("plugin.db.execute", execParams(h, "b", "block", 1, 1000))
	waitFor(t, func() bool {
		res, _ := c.Call("plugin.db.cancel", map[string]any{"handle": h, "requestId": "zz"})
		_ = res
		_, e := c.Call("plugin.db.execute", execParams(h, "c", "update", 1, 1000))
		return dbdrivertest.ErrorKind(e) == "busy" && e.Code == -32029
	})
	// cancel and close stay available at the limit.
	for _, id := range []string{"a", "b"} {
		if res, _ := c.Call("plugin.db.cancel", map[string]any{"handle": h, "requestId": id}); !strings.Contains(string(res), "confirmed") {
			t.Fatalf("cancel %s: %s", id, res)
		}
	}
	c.Await(a)
	c.Await(b)
	c.Connect("mem", nil)
	if _, e := c.Call("plugin.db.connect", dbdrivertest.ConnectParams("mem", nil)); dbdrivertest.ErrorKind(e) != "busy" {
		t.Fatalf("handle limit %+v", e)
	}
}

func TestIdleCursorExpiresAndCloseReleases(t *testing.T) {
	mc := newMem()
	c := dbdrivertest.Start(t, map[string]dbdriver.Driver{"mem": memDriver(mc)}, dbdriver.ServerOptions{}, nil)
	p := dbdrivertest.ConnectParams("mem", nil)
	p["limits"] = map[string]any{"pageMaxBytes": 524288, "pageMaxRows": 1000, "cursorIdleMs": 30}
	res, e := c.Call("plugin.db.connect", p)
	if e != nil {
		t.Fatal(e)
	}
	var cr struct{ Handle string }
	_ = json.Unmarshal(res, &cr)
	res, _ = c.Call("plugin.db.execute", execParams(cr.Handle, "q", "rows 5 1", 1, 1000))
	pg, _ := dbdrivertest.DecodePage(res)
	waitFor(t, func() bool { mc.mu.Lock(); defer mc.mu.Unlock(); return mc.rowsOut == 0 })
	if _, e := c.Call("plugin.db.fetch", map[string]any{"handle": cr.Handle, "cursor": pg.Cursor, "maxRows": 1, "maxBytes": 1000}); e == nil || e.Code != -32602 {
		t.Fatalf("expired cursor %+v", e)
	}
	// closeCursor and close are idempotent.
	if res, _ := c.Call("plugin.db.closeCursor", map[string]any{"handle": cr.Handle, "cursor": "c404"}); string(res) != "true" {
		t.Fatal(string(res))
	}
	c.Call("plugin.db.execute", execParams(cr.Handle, "q2", "rows 5 1", 1, 1000))
	slow := c.Send("plugin.db.execute", execParams(cr.Handle, "q3", "block", 1, 1000))
	time.Sleep(20 * time.Millisecond)
	if res, _ := c.Call("plugin.db.close", map[string]any{"handle": cr.Handle}); string(res) != "true" {
		t.Fatal(string(res))
	}
	if _, e := c.Await(slow); dbdrivertest.ErrorKind(e) != "cancelled" {
		t.Fatalf("running call on close %+v", e)
	}
	mc.mu.Lock()
	closed, out := mc.closed, mc.rowsOut
	mc.mu.Unlock()
	if !closed || out != 0 || c.Server.OpenHandles() != 0 {
		t.Fatalf("closed=%v rowsOut=%d", closed, out)
	}
	if res, _ := c.Call("plugin.db.close", map[string]any{"handle": cr.Handle}); string(res) != "true" {
		t.Fatal(string(res))
	}
}

func TestTableRows(t *testing.T) {
	c := dbdrivertest.Start(t, map[string]dbdriver.Driver{"mem": memDriver(newMem())}, dbdriver.ServerOptions{}, nil)
	h := c.Connect("mem", nil)
	p := map[string]any{"handle": h, "requestId": "t", "database": "d", "table": "users", "limit": 10, "offset": 0, "pageToken": "p1", "readOnly": true, "maxRows": 10, "maxBytes": 1000}
	res, e := c.Call("plugin.db.tableRows", p)
	if e != nil || string(res) != `{"columns":[{"name":"t"}],"rows":[["users"]],"nextPageToken":"next:p1","done":true}` {
		t.Fatalf("tableRows %s %+v", res, e)
	}
	p["table"] = "badtoken"
	if _, e := c.Call("plugin.db.tableRows", p); dbdrivertest.ErrorKind(e) != "database" {
		t.Fatalf("bad token %+v", e)
	}
}

func TestMetadataPaging(t *testing.T) {
	mc := newMem()
	c := dbdrivertest.Start(t, map[string]dbdriver.Driver{"mem": memDriver(mc)}, dbdriver.ServerOptions{}, nil)
	h := c.Connect("mem", nil)
	res, e := c.Call("plugin.db.metadata", map[string]any{"handle": h, "kind": "navigation", "maxBytes": 1 << 20})
	if e != nil || string(res) != `{"value":{"databases":[{"name":"d","tables":[]}]}}` {
		t.Fatalf("navigation %s %+v", res, e)
	}
	var names []string
	cursor := ""
	for {
		p := map[string]any{"handle": h, "kind": "columns", "database": "d", "table": "t", "maxBytes": 600}
		if cursor != "" {
			p["cursor"] = cursor
		}
		res, e := c.Call("plugin.db.metadata", p)
		if e != nil {
			t.Fatal(e)
		}
		if len(res) > 600 {
			t.Fatalf("metadata page %d bytes", len(res))
		}
		var r struct {
			Items  []dbdriver.ColumnInfo
			Cursor string
		}
		_ = json.Unmarshal(res, &r)
		for _, it := range r.Items {
			names = append(names, it.Name)
		}
		if cursor = r.Cursor; cursor == "" {
			break
		}
	}
	if len(names) != 50 || names[49] != "col_49" {
		t.Fatalf("items %v", names)
	}
	if _, e := c.Call("plugin.db.metadata", map[string]any{"handle": h, "kind": "triggers", "maxBytes": 600}); dbdrivertest.ErrorKind(e) != "unsupported" {
		t.Fatalf("unsupported kind %+v", e)
	}
}

func TestShutdownClosesEverything(t *testing.T) {
	mc := newMem()
	srv := dbdriver.NewServer(map[string]dbdriver.Driver{"mem": memDriver(mc)}, dbdriver.ServerOptions{})
	raw, _ := srv.StartDatabaseCall(context.Background(), "plugin.db.connect", mustJSON(dbdrivertest.ConnectParams("mem", nil)))
	if _, err := raw(); err != nil {
		t.Fatal(err)
	}
	run, _ := srv.StartDatabaseCall(context.Background(), "plugin.db.execute", mustJSON(execParams("h1", "q", "rows 5 1", 1, 1000)))
	if _, err := run(); err != nil {
		t.Fatal(err)
	}
	srv.CloseDatabase()
	if !mc.closed || mc.rowsOut != 0 || srv.OpenHandles() != 0 {
		t.Fatalf("closed=%v rowsOut=%d", mc.closed, mc.rowsOut)
	}
	if _, e := srv.StartDatabaseCall(context.Background(), "plugin.db.execute", mustJSON(execParams("h1", "q2", "update", 1, 1000))); e == nil {
		t.Fatal("call after shutdown accepted")
	}
	if string(srv.DatabaseDetails()) != `{"protocol":1,"minor":1,"drivers":["mem"],"pageLimits":{"maxRows":5000,"maxBytes":2097152}}` {
		t.Fatal(string(srv.DatabaseDetails()))
	}
	v10 := dbdriver.NewServer(map[string]dbdriver.Driver{"mem": memDriver(newMem())}, dbdriver.ServerOptions{ProtocolV10: true})
	if string(v10.DatabaseDetails()) != `{"protocol":1,"drivers":["mem"]}` {
		t.Fatal(string(v10.DatabaseDetails()))
	}
}

func TestFromSQL(t *testing.T) {
	f := &dbdrivertest.FakeSQL{Script: map[string]dbdrivertest.FakeResult{
		"SELECT id, name FROM t": {Columns: []string{"id", "name"}, Types: []string{"BIGINT", "TEXT"}, Rows: [][]driver.Value{{int64(1), []byte("a")}, {int64(9007199254740993), nil}}},
		"DELETE FROM t":          {Affected: 2},
		"SELECT sleep":           {Columns: []string{"x"}, Block: true},
	}}
	d := dbdriver.FromSQL(func(ctx context.Context, cfg dbdriver.Config) (*sql.DB, error) { return f.DB(), nil },
		dbdriver.SQLOptions{ReadOnlyTx: true, Metadata: func(ctx context.Context, db *sql.DB, req dbdriver.MetadataRequest) (*dbdriver.MetadataResult, error) {
			return &dbdriver.MetadataResult{Value: dbdriver.SchemaInfo{Databases: []dbdriver.SchemaDatabase{}}}, nil
		}})
	dbdrivertest.CheckDriver(t, d, dbdrivertest.Check{Query: "SELECT id, name FROM t", WantRows: 2, SlowQuery: "SELECT sleep", BadQuery: "SELEC 1"})

	c := dbdrivertest.Start(t, map[string]dbdriver.Driver{"sql": d}, dbdriver.ServerOptions{}, nil)
	h := c.Connect("sql", nil)
	p := execParams(h, "ro", "SELECT id, name FROM t", 10, 1000)
	p["readOnly"] = true
	res, e := c.Call("plugin.db.execute", p)
	if e != nil || string(res) != `{"columns":[{"name":"id","type":"BIGINT"},{"name":"name","type":"TEXT"}],"rows":[[1,"a"],["9007199254740993",null]],"done":true}` {
		t.Fatalf("query %s %+v", res, e)
	}
	res, _ = c.Call("plugin.db.execute", execParams(h, "w", "DELETE FROM t", 10, 1000))
	if string(res) != `{"rows":[],"affectedRows":2,"done":true}` {
		t.Fatalf("exec %s", res)
	}
	if f.ReadOnlyTx != 1 || !contains(f.Recorded(), "ro:SELECT id, name FROM t") {
		t.Fatalf("read-only tx %d %v", f.ReadOnlyTx, f.Recorded())
	}
	if !dbdriver.IsQueryStatement(" -- c\n/* x */ (select 1)") || dbdriver.IsQueryStatement("insert into t values (1)") {
		t.Fatal("IsQueryStatement")
	}
}

func contains(list []string, s string) bool {
	for _, v := range list {
		if v == s {
			return true
		}
	}
	return false
}

func mustJSON(v any) json.RawMessage {
	raw, _ := json.Marshal(v)
	return raw
}

func waitFor(t *testing.T, ok func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !ok() {
		if time.Now().After(deadline) {
			t.Fatal("condition not reached")
		}
		time.Sleep(5 * time.Millisecond)
	}
}
