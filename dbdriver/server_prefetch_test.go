package dbdriver_test

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mincodeai/plugin-sdk-go/dbdriver"
	"github.com/mincodeai/plugin-sdk-go/dbdriver/dbdrivertest"
	"github.com/mincodeai/plugin-sdk-go/rpc"
)

// gatedRows yields rows 1..n. Rows after blockAfter (0: never) wait for
// release or ctx; row failAt (0: never) fails.
type gatedRows struct {
	c          *memConn
	n, i       int
	blockAfter int
	failAt     int
	release    chan struct{}
	read       atomic.Int64
	closed     atomic.Bool
}

func (r *gatedRows) Columns() []dbdriver.Column { return []dbdriver.Column{{Name: "i"}} }

func (r *gatedRows) Next(ctx context.Context) ([]any, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if r.i >= r.n {
		return nil, errEOF
	}
	if r.failAt > 0 && r.i+1 == r.failAt {
		return nil, errors.New("row failed")
	}
	if r.blockAfter > 0 && r.i >= r.blockAfter {
		select {
		case <-r.release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	r.i++
	r.read.Store(int64(r.i))
	return []any{r.i}, nil
}

func (r *gatedRows) Close() error {
	if r.closed.CompareAndSwap(false, true) {
		r.c.mu.Lock()
		r.c.rowsOut--
		r.c.mu.Unlock()
	}
	return nil
}

func gatedConn(g *gatedRows) *memConn {
	mc := &memConn{}
	g.c = mc
	if g.release == nil {
		g.release = make(chan struct{})
	}
	mc.exec = func(context.Context, dbdriver.ExecuteRequest) (*dbdriver.Result, error) {
		mc.mu.Lock()
		mc.rowsOut++
		mc.mu.Unlock()
		return &dbdriver.Result{Rows: g}, nil
	}
	return mc
}

func rowsOut(mc *memConn) int {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	return mc.rowsOut
}

func pageNumbers(t *testing.T, raw json.RawMessage) (dbdrivertest.Page, []int) {
	t.Helper()
	p, err := dbdrivertest.DecodePage(raw)
	if err != nil {
		t.Fatalf("decode %s: %v", raw, err)
	}
	var nums []int
	for _, row := range p.Rows {
		n, _ := strconv.Atoi(string(row[0].(json.Number)))
		nums = append(nums, n)
	}
	return p, nums
}

func fetch(c *dbdrivertest.Client, h, cursor string, maxRows int) (json.RawMessage, *rpc.Error) {
	return c.Call("plugin.db.fetch", map[string]any{"handle": h, "cursor": cursor, "maxRows": maxRows, "maxBytes": 1 << 20})
}

func TestPrefetchReadsOnePageAhead(t *testing.T) {
	g := &gatedRows{n: 100}
	mc := gatedConn(g)
	c := dbdrivertest.Start(t, map[string]dbdriver.Driver{"mem": memDriver(mc)}, dbdriver.ServerOptions{}, nil)
	h := c.Connect("mem", nil)
	res, e := c.Call("plugin.db.execute", execParams(h, "q", "rows", 10, 1<<20))
	if e != nil {
		t.Fatal(e)
	}
	p, got := pageNumbers(t, res)
	// 10 rows + 1 read-ahead row, then one prefetched page (10 rows).
	waitFor(t, func() bool { return g.read.Load() == 21 })
	time.Sleep(30 * time.Millisecond)
	if n := g.read.Load(); n != 21 {
		t.Fatalf("prefetch read %d rows, want a bound of 21", n)
	}
	res, e = fetch(c, h, p.Cursor, 10)
	if e != nil {
		t.Fatal(e)
	}
	p, nums := pageNumbers(t, res)
	got = append(got, nums...)
	waitFor(t, func() bool { return g.read.Load() == 31 })
	// Budgets may change between fetches; rows stay in order.
	for i := 0; !p.Done; i++ {
		res, e = fetch(c, h, p.Cursor, []int{7, 20, 3}[i%3])
		if e != nil {
			t.Fatal(e)
		}
		p, nums = pageNumbers(t, res)
		got = append(got, nums...)
	}
	if len(got) != 100 {
		t.Fatalf("got %d rows", len(got))
	}
	for i, n := range got {
		if n != i+1 {
			t.Fatalf("row %d is %d", i, n)
		}
	}
	waitFor(t, func() bool { return rowsOut(mc) == 0 })
}

func TestPrefetchDisabled(t *testing.T) {
	g := &gatedRows{n: 100}
	mc := gatedConn(g)
	c := dbdrivertest.Start(t, map[string]dbdriver.Driver{"mem": memDriver(mc)}, dbdriver.ServerOptions{DisablePrefetch: true}, nil)
	h := c.Connect("mem", nil)
	if _, e := c.Call("plugin.db.execute", execParams(h, "q", "rows", 10, 1<<20)); e != nil {
		t.Fatal(e)
	}
	time.Sleep(50 * time.Millisecond)
	if n := g.read.Load(); n != 11 {
		t.Fatalf("read %d rows without prefetch", n)
	}
}

func TestPrefetchFetchWaitsForRunningPrefetch(t *testing.T) {
	g := &gatedRows{n: 50, blockAfter: 12}
	mc := gatedConn(g)
	c := dbdrivertest.Start(t, map[string]dbdriver.Driver{"mem": memDriver(mc)}, dbdriver.ServerOptions{}, nil)
	h := c.Connect("mem", nil)
	res, e := c.Call("plugin.db.execute", execParams(h, "q", "rows", 10, 1<<20))
	if e != nil {
		t.Fatal(e)
	}
	p, _ := pageNumbers(t, res)
	waitFor(t, func() bool { return g.read.Load() == 12 })
	reply := c.Send("plugin.db.fetch", map[string]any{"handle": h, "cursor": p.Cursor, "maxRows": 10, "maxBytes": 1 << 20})
	select {
	case r := <-reply:
		t.Fatalf("fetch answered while the prefetch was reading: %s %+v", r.Result, r.Error)
	case <-time.After(50 * time.Millisecond):
	}
	close(g.release)
	res, e = c.Await(reply)
	if e != nil {
		t.Fatal(e)
	}
	if _, nums := pageNumbers(t, res); len(nums) != 10 || nums[0] != 11 || nums[9] != 20 {
		t.Fatalf("page after prefetch %v", nums)
	}
}

// startBlockedPrefetch executes a query whose prefetch blocks in Next.
func startBlockedPrefetch(t *testing.T) (*dbdrivertest.Client, *memConn, *gatedRows, string, string) {
	t.Helper()
	g := &gatedRows{n: 50, blockAfter: 12}
	mc := gatedConn(g)
	c := dbdrivertest.Start(t, map[string]dbdriver.Driver{"mem": memDriver(mc)}, dbdriver.ServerOptions{}, nil)
	h := c.Connect("mem", nil)
	res, e := c.Call("plugin.db.execute", execParams(h, "q", "rows", 10, 1<<20))
	if e != nil {
		t.Fatal(e)
	}
	p, _ := pageNumbers(t, res)
	waitFor(t, func() bool { return g.read.Load() == 12 })
	return c, mc, g, h, p.Cursor
}

func waitPrompt(t *testing.T, ok func() bool) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for !ok() {
		if time.Now().After(deadline) {
			t.Fatal("prefetch not discarded within 1s")
		}
		time.Sleep(2 * time.Millisecond)
	}
}

func TestPrefetchDiscardedOnCloseCursor(t *testing.T) {
	c, mc, g, h, cursor := startBlockedPrefetch(t)
	if res, _ := c.Call("plugin.db.closeCursor", map[string]any{"handle": h, "cursor": cursor}); string(res) != "true" {
		t.Fatal(string(res))
	}
	waitPrompt(t, func() bool { return rowsOut(mc) == 0 })
	if g.read.Load() != 12 {
		t.Fatalf("read %d rows after close", g.read.Load())
	}
	if _, e := fetch(c, h, cursor, 10); e == nil || e.Code != -32602 {
		t.Fatalf("fetch after close %+v", e)
	}
}

func TestPrefetchDiscardedOnCancel(t *testing.T) {
	c, mc, _, h, cursor := startBlockedPrefetch(t)
	res, e := c.Call("plugin.db.cancel", map[string]any{"handle": h, "requestId": "q"})
	if e != nil || string(res) != `{"state":"confirmed"}` {
		t.Fatalf("cancel %s %+v", res, e)
	}
	waitPrompt(t, func() bool { return rowsOut(mc) == 0 })
	if _, e := fetch(c, h, cursor, 10); e == nil || e.Code != -32602 {
		t.Fatalf("fetch after cancel %+v", e)
	}
}

func TestPrefetchDiscardedOnHandleClose(t *testing.T) {
	c, mc, _, h, _ := startBlockedPrefetch(t)
	start := time.Now()
	if res, _ := c.Call("plugin.db.close", map[string]any{"handle": h}); string(res) != "true" {
		t.Fatal(string(res))
	}
	if d := time.Since(start); d > time.Second {
		t.Fatalf("close waited %v for the prefetch", d)
	}
	mc.mu.Lock()
	closed, out := mc.closed, mc.rowsOut
	mc.mu.Unlock()
	if !closed || out != 0 {
		t.Fatalf("closed=%v rowsOut=%d", closed, out)
	}
}

func TestPrefetchDiscardedOnShutdown(t *testing.T) {
	c, mc, _, _, _ := startBlockedPrefetch(t)
	start := time.Now()
	c.Server.CloseDatabase()
	if d := time.Since(start); d > time.Second {
		t.Fatalf("shutdown waited %v for the prefetch", d)
	}
	if out := rowsOut(mc); out != 0 {
		t.Fatalf("rowsOut=%d", out)
	}
}

func TestPrefetchErrorSurfacesOnNextFetch(t *testing.T) {
	g := &gatedRows{n: 50, failAt: 15}
	mc := gatedConn(g)
	c := dbdrivertest.Start(t, map[string]dbdriver.Driver{"mem": memDriver(mc)}, dbdriver.ServerOptions{}, nil)
	h := c.Connect("mem", nil)
	res, e := c.Call("plugin.db.execute", execParams(h, "q", "rows", 10, 1<<20))
	if e != nil {
		t.Fatalf("the first page must not see the prefetch error: %+v", e)
	}
	p, _ := pageNumbers(t, res)
	waitFor(t, func() bool { return g.read.Load() == 14 })
	if _, e := fetch(c, h, p.Cursor, 10); dbdrivertest.ErrorKind(e) != "database" || e.Message != "row failed" {
		t.Fatalf("fetch after a failed prefetch %+v", e)
	}
	waitFor(t, func() bool { return rowsOut(mc) == 0 })
}

func TestProtocolV11PageLimits(t *testing.T) {
	connect := func(c *dbdrivertest.Client) string {
		t.Helper()
		p := dbdrivertest.ConnectParams("mem", nil)
		// More than any host sends: the Server clamps to its ceiling.
		p["limits"] = map[string]any{"pageMaxBytes": 64 << 20, "pageMaxRows": 100000, "cursorIdleMs": 60000}
		res, e := c.Call("plugin.db.connect", p)
		if e != nil {
			t.Fatal(e)
		}
		var cr struct{ Handle string }
		_ = json.Unmarshal(res, &cr)
		return cr.Handle
	}
	c := dbdrivertest.Start(t, map[string]dbdriver.Driver{"mem": memDriver(newMem())}, dbdriver.ServerOptions{}, nil)
	h := connect(c)
	res, e := c.Call("plugin.db.execute", execParams(h, "q1", "rows 3000 1000", 100000, 64<<20))
	if e != nil {
		t.Fatal(e)
	}
	p, _ := dbdrivertest.DecodePage(res)
	if len(res) <= 1<<20 || len(res) > dbdriver.MaxPageBytes || p.Done {
		t.Fatalf("1.1 page of %d bytes, %d rows", len(res), len(p.Rows))
	}
	res, _ = c.Call("plugin.db.execute", execParams(h, "q2", "rows 6000 1", 100000, 64<<20))
	if p, _ = dbdrivertest.DecodePage(res); len(p.Rows) != dbdriver.MaxPageRows {
		t.Fatalf("1.1 page of %d rows", len(p.Rows))
	}

	v10 := dbdrivertest.Start(t, map[string]dbdriver.Driver{"mem": memDriver(newMem())}, dbdriver.ServerOptions{ProtocolV10: true}, nil)
	h = connect(v10)
	res, e = v10.Call("plugin.db.execute", execParams(h, "q1", "rows 3000 1000", 100000, 64<<20))
	if e != nil {
		t.Fatal(e)
	}
	if len(res) >= 1<<20 {
		t.Fatalf("1.0 page of %d bytes crosses the 1 MiB line cap", len(res))
	}
}
