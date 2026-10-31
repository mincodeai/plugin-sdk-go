package dbdriver_test

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/mincodeai/plugin-sdk-go/dbdriver"
	"github.com/mincodeai/plugin-sdk-go/dbdriver/dbdrivertest"
)

func snapshotDriver(f *dbdrivertest.FakeSQL, o dbdriver.SQLOptions) dbdriver.Driver {
	if o.UseDatabase == nil {
		o.UseDatabase = func(ctx context.Context, c *sql.Conn, database string) error {
			_, err := c.ExecContext(ctx, "USE "+database)
			return err
		}
	}
	return dbdriver.FromSQL(func(ctx context.Context, cfg dbdriver.Config) (*sql.DB, error) { return f.DB(), nil }, o)
}

func snapshotParams(h string, steps ...map[string]any) map[string]any {
	list := make([]any, len(steps))
	for i, s := range steps {
		list[i] = s
	}
	return map[string]any{"handle": h, "requestId": "s1", "database": "d", "steps": list}
}

func stmt(text string, args ...any) map[string]any {
	if args == nil {
		args = []any{}
	}
	return map[string]any{"statement": text, "args": args}
}

func TestSnapshotWriteCommitsAndReturnsRows(t *testing.T) {
	at := time.Date(2026, 10, 30, 8, 0, 0, 5, time.UTC)
	f := &dbdrivertest.FakeSQL{Script: map[string]dbdrivertest.FakeResult{
		"USE d": {},
		"SELECT * FROM t WHERE v > 1": {Columns: []string{"id", "b", "at", "big"}, Types: []string{"INT", "BLOB", "TIMESTAMP", "BIGINT"},
			Rows: [][]driver.Value{{int64(1), []byte("ab"), at, int64(9007199254740993)}, {int64(2), nil, at, int64(3)}}},
		"UPDATE t SET v = 0 WHERE v > ?": {Affected: 2},
		"SELECT * FROM t WHERE id IN (?, ?)": {Columns: []string{"id", "v"},
			Rows: [][]driver.Value{{int64(1), int64(0)}, {int64(2), int64(0)}}},
	}}
	c := dbdrivertest.Start(t, map[string]dbdriver.Driver{"sql": snapshotDriver(f, dbdriver.SQLOptions{})}, dbdriver.ServerOptions{}, nil)
	h := c.Connect("sql", nil)
	res, e := c.Call("plugin.db.snapshotWrite", snapshotParams(h, map[string]any{
		"snapshot": stmt("SELECT * FROM t WHERE v > 1"),
		"write":    stmt("UPDATE t SET v = 0 WHERE v > ?", 1),
		"after":    stmt("SELECT * FROM t WHERE id IN (?, ?)", 1, "2.5"),
		"maxRows":  2,
	}))
	if e != nil {
		t.Fatal(e)
	}
	want := `{"steps":[{"before":{"columns":[{"name":"id","type":"INT"},{"name":"b","type":"BLOB"},{"name":"at","type":"TIMESTAMP"},{"name":"big","type":"BIGINT"}],` +
		`"rows":[[{"t":"int","v":"1"},{"t":"bytes","v":"YWI="},{"t":"time","v":"1793347200.000000005","l":"utc"},{"t":"int","v":"9007199254740993"}],` +
		`[{"t":"int","v":"2"},{"t":"null"},{"t":"time","v":"1793347200.000000005","l":"utc"},{"t":"int","v":"3"}]]},` +
		`"affected":2,"after":{"columns":[{"name":"id"},{"name":"v"}],"rows":[[{"t":"int","v":"1"},{"t":"int","v":"0"}],[{"t":"int","v":"2"},{"t":"int","v":"0"}]]}}],"exceeded":false,"exceededStep":0}`
	if string(res) != want {
		t.Fatalf("result\n%s\nwant\n%s", res, want)
	}
	if got := f.Recorded(); !reflect.DeepEqual(got, []string{"USE d", "SELECT * FROM t WHERE v > 1", "UPDATE t SET v = 0 WHERE v > ?", "SELECT * FROM t WHERE id IN (?, ?)"}) {
		t.Fatalf("statements %v", got)
	}
	// Integer arguments arrive as int64, other numbers keep their exact text.
	if args := f.RecordedArgs(); !reflect.DeepEqual(args[2], []driver.Value{int64(1)}) || !reflect.DeepEqual(args[3], []driver.Value{int64(1), "2.5"}) {
		t.Fatalf("args %#v", args)
	}
	if begun, committed, rolledBack := f.TxCounts(); begun != 1 || committed != 1 || rolledBack != 0 {
		t.Fatalf("tx %d/%d/%d", begun, committed, rolledBack)
	}
	waitFor(t, func() bool { return f.OpenConns() <= 1 })
}

func TestSnapshotWriteExceededRollsBack(t *testing.T) {
	f := &dbdrivertest.FakeSQL{Script: map[string]dbdrivertest.FakeResult{
		"USE d":      {},
		"SELECT one": {Columns: []string{"id"}, Rows: [][]driver.Value{{int64(1)}}},
		"DELETE one": {Affected: 1},
		"SELECT two": {Columns: []string{"id"}, Rows: [][]driver.Value{{int64(1)}, {int64(2)}}},
		"DELETE two": {Affected: 2},
	}}
	c := dbdrivertest.Start(t, map[string]dbdriver.Driver{"sql": snapshotDriver(f, dbdriver.SQLOptions{})}, dbdriver.ServerOptions{}, nil)
	h := c.Connect("sql", nil)
	res, e := c.Call("plugin.db.snapshotWrite", snapshotParams(h,
		map[string]any{"snapshot": stmt("SELECT one"), "write": stmt("DELETE one"), "maxRows": 1},
		map[string]any{"snapshot": stmt("SELECT two"), "write": stmt("DELETE two"), "maxRows": 1},
	))
	if e != nil || string(res) != `{"steps":[],"exceeded":true,"exceededStep":1}` {
		t.Fatalf("exceeded %s %+v", res, e)
	}
	if got := f.Recorded(); !reflect.DeepEqual(got, []string{"USE d", "SELECT one", "DELETE one", "SELECT two"}) {
		t.Fatalf("statements %v", got)
	}
	if begun, committed, rolledBack := f.TxCounts(); begun != 1 || committed != 0 || rolledBack != 1 {
		t.Fatalf("tx %d/%d/%d", begun, committed, rolledBack)
	}
}

func TestSnapshotWriteErrorAndLimitRollBack(t *testing.T) {
	big := strings.Repeat("x", 300<<10)
	rows := make([][]driver.Value, 4)
	for i := range rows {
		rows[i] = []driver.Value{big}
	}
	f := &dbdrivertest.FakeSQL{Script: map[string]dbdrivertest.FakeResult{
		"USE d":      {},
		"UPDATE ok":  {Affected: 1},
		"SELECT big": {Columns: []string{"v"}, Rows: rows},
	}}
	c := dbdrivertest.Start(t, map[string]dbdriver.Driver{"sql": snapshotDriver(f, dbdriver.SQLOptions{})}, dbdriver.ServerOptions{}, nil)
	h := c.Connect("sql", nil)
	_, e := c.Call("plugin.db.snapshotWrite", snapshotParams(h,
		map[string]any{"write": stmt("UPDATE ok"), "maxRows": 0},
		map[string]any{"write": stmt("UPDATE broken"), "maxRows": 0},
	))
	if dbdrivertest.ErrorKind(e) != "database" {
		t.Fatalf("write error %+v", e)
	}
	_, e = c.Call("plugin.db.snapshotWrite", snapshotParams(h,
		map[string]any{"snapshot": stmt("SELECT big"), "write": stmt("UPDATE ok"), "maxRows": 10},
	))
	if dbdrivertest.ErrorKind(e) != "limit" {
		t.Fatalf("limit %+v", e)
	}
	if begun, committed, rolledBack := f.TxCounts(); begun != 2 || committed != 0 || rolledBack != 2 {
		t.Fatalf("tx %d/%d/%d", begun, committed, rolledBack)
	}
	if got := f.Recorded(); contains(got[len(got)-1:], "UPDATE ok") {
		t.Fatalf("write ran after the limit: %v", got)
	}
}

func TestSnapshotWriteMutationSteps(t *testing.T) {
	f := &dbdrivertest.FakeSQL{Script: map[string]dbdrivertest.FakeResult{"USE d": {}}}
	var got []dbdriver.Mutation
	o := dbdriver.SQLOptions{
		MutateIn: func(ctx context.Context, q dbdriver.Querier, m dbdriver.Mutation) (int64, error) {
			if _, ok := q.(*sql.Tx); !ok {
				t.Errorf("MutateIn outside the transaction: %T", q)
			}
			got = append(got, m)
			return 1, nil
		},
		InsertReturning: func(ctx context.Context, q dbdriver.Querier, m dbdriver.Mutation, columns []string) (int64, map[string]any, error) {
			got = append(got, m)
			return 1, map[string]any{columns[0]: int64(42)}, nil
		},
	}
	c := dbdrivertest.Start(t, map[string]dbdriver.Driver{"sql": snapshotDriver(f, o)}, dbdriver.ServerOptions{}, nil)
	h := c.Connect("sql", nil)
	res, e := c.Call("plugin.db.snapshotWrite", snapshotParams(h,
		map[string]any{"mutation": map[string]any{"op": "updateCell", "table": "t", "primaryKey": map[string]any{"id": 1}, "column": "v", "value": 2}, "maxRows": 0},
		map[string]any{"mutation": map[string]any{"op": "insertRow", "table": "t", "fields": map[string]any{"v": 3}}, "returning": []string{"id"}, "maxRows": 0},
	))
	if e != nil || string(res) != `{"steps":[{"affected":1},{"affected":1,"returned":{"id":{"t":"int","v":"42"}}}],"exceeded":false,"exceededStep":0}` {
		t.Fatalf("mutation steps %s %+v", res, e)
	}
	if len(got) != 2 || got[0].Op != dbdriver.MutationUpdateCell || got[0].Database != "d" || got[0].RequestID != "s1" ||
		got[0].PrimaryKey["id"] != json.Number("1") || got[1].Op != dbdriver.MutationInsertRow || got[1].Fields["v"] != json.Number("3") {
		t.Fatalf("mutations %+v", got)
	}

	// Without InsertReturning an insert still runs through MutateIn and
	// returns no keys.
	o.InsertReturning = nil
	c2 := dbdrivertest.Start(t, map[string]dbdriver.Driver{"sql": snapshotDriver(f, o)}, dbdriver.ServerOptions{}, nil)
	h2 := c2.Connect("sql", nil)
	res, e = c2.Call("plugin.db.snapshotWrite", snapshotParams(h2,
		map[string]any{"mutation": map[string]any{"op": "insertRow", "table": "t", "fields": map[string]any{"v": 3}}, "returning": []string{"id"}, "maxRows": 0},
	))
	if e != nil || string(res) != `{"steps":[{"affected":1}],"exceeded":false,"exceededStep":0}` {
		t.Fatalf("insert without returning %s %+v", res, e)
	}
}

func TestSnapshotWriteStrictParams(t *testing.T) {
	f := &dbdrivertest.FakeSQL{Script: map[string]dbdrivertest.FakeResult{}}
	c := dbdrivertest.Start(t, map[string]dbdriver.Driver{"sql": snapshotDriver(f, dbdriver.SQLOptions{})}, dbdriver.ServerOptions{}, nil)
	h := c.Connect("sql", nil)
	write := stmt("UPDATE t SET v = 1")
	many := make([]map[string]any, 1001)
	for i := range many {
		many[i] = map[string]any{"write": write, "maxRows": 0}
	}
	withExtra := snapshotParams(h, map[string]any{"write": write, "maxRows": 0})
	withExtra["extra"] = true
	cases := map[string]map[string]any{
		"unknown member":           withExtra,
		"unknown step member":      snapshotParams(h, map[string]any{"write": write, "maxRows": 0, "sql": "x"}),
		"unknown statement member": snapshotParams(h, map[string]any{"write": map[string]any{"statement": "x", "args": []any{}, "mode": "exec"}, "maxRows": 0}),
		"unknown mutation member":  snapshotParams(h, map[string]any{"mutation": map[string]any{"op": "deleteRow", "table": "t", "database": "d"}, "maxRows": 0}),
		"no steps":                 snapshotParams(h),
		"too many steps":           snapshotParams(h, many...),
		"no write":                 snapshotParams(h, map[string]any{"snapshot": stmt("SELECT 1"), "maxRows": 1}),
		"write and mutation":       snapshotParams(h, map[string]any{"write": write, "mutation": map[string]any{"op": "deleteRow", "table": "t"}, "maxRows": 0}),
		"empty statement":          snapshotParams(h, map[string]any{"write": stmt(""), "maxRows": 0}),
		"snapshot no maxRows":      snapshotParams(h, map[string]any{"snapshot": stmt("SELECT 1"), "write": write, "maxRows": 0}),
		"huge maxRows":             snapshotParams(h, map[string]any{"snapshot": stmt("SELECT 1"), "write": write, "maxRows": int64(1) << 62}),
		"negative maxRows":         snapshotParams(h, map[string]any{"write": write, "maxRows": -1}),
		"unknown op":               snapshotParams(h, map[string]any{"mutation": map[string]any{"op": "upsert", "table": "t"}, "maxRows": 0}),
		"mutation no table":        snapshotParams(h, map[string]any{"mutation": map[string]any{"op": "deleteRow"}, "maxRows": 0}),
		"returning on write":       snapshotParams(h, map[string]any{"write": write, "returning": []string{"id"}, "maxRows": 0}),
		"empty returning":          snapshotParams(h, map[string]any{"mutation": map[string]any{"op": "insertRow", "table": "t"}, "returning": []string{""}, "maxRows": 0}),
		"object argument":          snapshotParams(h, map[string]any{"write": stmt("UPDATE t SET v = ?", map[string]any{"a": 1}), "maxRows": 0}),
		"wrong type":               snapshotParams(h, map[string]any{"write": "UPDATE", "maxRows": 0}),
	}
	for name, p := range cases {
		if _, e := c.Call("plugin.db.snapshotWrite", p); e == nil || e.Code != -32602 || dbdrivertest.ErrorKind(e) != "invalid" {
			t.Errorf("%s: %+v", name, e)
		}
	}
	if got := f.Recorded(); len(got) != 0 {
		t.Fatalf("invalid params ran statements: %v", got)
	}
}

func TestSnapshotWriteUnsupportedWithoutMutateIn(t *testing.T) {
	f := &dbdrivertest.FakeSQL{Script: map[string]dbdrivertest.FakeResult{"USE d": {}, "UPDATE t": {Affected: 1}}}
	c := dbdrivertest.Start(t, map[string]dbdriver.Driver{
		"sql":  snapshotDriver(f, dbdriver.SQLOptions{}),
		"notx": snapshotDriver(f, dbdriver.SQLOptions{NoTransactions: true}),
	}, dbdriver.ServerOptions{}, nil)
	h := c.Connect("sql", nil)
	_, e := c.Call("plugin.db.snapshotWrite", snapshotParams(h,
		map[string]any{"write": stmt("UPDATE t"), "maxRows": 0},
		map[string]any{"mutation": map[string]any{"op": "deleteRow", "table": "t", "primaryKey": map[string]any{"id": 1}}, "maxRows": 0},
	))
	if dbdrivertest.ErrorKind(e) != "unsupported" {
		t.Fatalf("mutation without MutateIn %+v", e)
	}
	hn := c.Connect("notx", nil)
	if _, e = c.Call("plugin.db.snapshotWrite", snapshotParams(hn, map[string]any{"write": stmt("UPDATE t"), "maxRows": 0})); dbdrivertest.ErrorKind(e) != "unsupported" {
		t.Fatalf("NoTransactions %+v", e)
	}
	if begun, _, _ := f.TxCounts(); begun != 0 || len(f.Recorded()) != 0 {
		t.Fatalf("unsupported request touched the database: %d %v", begun, f.Recorded())
	}
}

// snapConn is a Conn whose SnapshotWrite answers a scripted response.
type snapConn struct {
	*memConn
	res *dbdriver.SnapshotResponse
}

func (c *snapConn) SnapshotWrite(context.Context, dbdriver.SnapshotRequest) (*dbdriver.SnapshotResponse, error) {
	return c.res, nil
}

func TestSnapshotWriteRejectsMalformedDriverResponses(t *testing.T) {
	sc := &snapConn{memConn: newMem()}
	d := dbdriver.DriverFunc(func(ctx context.Context, cfg dbdriver.Config) (dbdriver.Conn, dbdriver.ConnInfo, error) {
		return sc, dbdriver.ConnInfo{}, nil
	})
	c := dbdrivertest.Start(t, map[string]dbdriver.Driver{"snap": d}, dbdriver.ServerOptions{}, nil)
	h := c.Connect("snap", nil)
	p := snapshotParams(h, map[string]any{"write": stmt("UPDATE t"), "maxRows": 0})
	for name, res := range map[string]*dbdriver.SnapshotResponse{
		"nil":           nil,
		"step count":    {Steps: []dbdriver.SnapshotStepResult{{}, {}}},
		"exceeded step": {Exceeded: true, ExceededStep: 1},
		"short row":     {Steps: []dbdriver.SnapshotStepResult{{Before: &dbdriver.Result{Rows: dbdriver.SliceRows([]dbdriver.Column{{Name: "a"}, {Name: "b"}}, [][]any{{1}})}}}},
	} {
		sc.res = res
		// The writer answered success, so it may have committed: never a
		// plain failure the host would resubmit.
		if _, e := c.Call("plugin.db.snapshotWrite", p); dbdrivertest.ErrorKind(e) != "committed" {
			t.Errorf("%s: %+v", name, e)
		}
	}
	// A writer that undercounts its budget has already committed: the reply
	// that does not fit must not say "limit" (rolled back, resubmit).
	big := strings.Repeat("x", 1<<20)
	sc.res = &dbdriver.SnapshotResponse{Steps: []dbdriver.SnapshotStepResult{{Affected: 1,
		Before: &dbdriver.Result{Rows: dbdriver.SliceRows([]dbdriver.Column{{Name: "v"}}, [][]any{{big}})}}}}
	if _, e := c.Call("plugin.db.snapshotWrite", p); dbdrivertest.ErrorKind(e) != "committed" {
		t.Errorf("oversized reply after commit: %+v", e)
	}
	sc.res = &dbdriver.SnapshotResponse{Steps: []dbdriver.SnapshotStepResult{{Affected: 1, Returned: map[string]any{"id": map[string]any{"a": 1}}}}}
	if _, e := c.Call("plugin.db.snapshotWrite", p); dbdrivertest.ErrorKind(e) != "committed" {
		t.Errorf("unencodable value after commit: %+v", e)
	}
	sc.res = &dbdriver.SnapshotResponse{Steps: []dbdriver.SnapshotStepResult{{Affected: 3, Returned: map[string]any{"id": json.Number("9007199254740993")}}}}
	if res, e := c.Call("plugin.db.snapshotWrite", p); e != nil || string(res) != `{"steps":[{"affected":3,"returned":{"id":{"t":"num","v":"9007199254740993"}}}],"exceeded":false,"exceededStep":0}` {
		t.Fatalf("custom writer %s %+v", res, e)
	}
}

func TestSnapshotCellRoundTrip(t *testing.T) {
	zone := time.FixedZone("CST", 8*3600)
	values := []any{
		nil, true, false, int64(-7), int64(9007199254740993), uint64(18446744073709551615), 1.5, math.Inf(-1),
		json.Number("12345678901234567890.123"), "héllo", string([]byte{0xff, 'a'}), []byte{0xff, 0}, []byte(nil), []byte{},
		time.Date(2026, 10, 30, 8, 0, 0, 123456789, time.UTC), time.Date(2026, 10, 30, 8, 0, 0, 5, zone),
	}
	for _, v := range values {
		c, err := dbdriver.EncodeSnapshotCell(v)
		if err != nil {
			t.Fatalf("%#v: %v", v, err)
		}
		raw, _ := json.Marshal(c)
		var back dbdriver.SnapshotCell
		if err := json.Unmarshal(raw, &back); err != nil {
			t.Fatal(err)
		}
		got, err := dbdriver.DecodeSnapshotCell(back)
		if err != nil {
			t.Fatalf("%s: %v", raw, err)
		}
		if want, ok := v.(time.Time); ok {
			gt := got.(time.Time)
			gn, gz := gt.Zone()
			wn, wz := want.Zone()
			if !gt.Equal(want) || gn != wn || gz != wz {
				t.Errorf("%s: %v != %v", raw, gt, want)
			}
			continue
		}
		if !reflect.DeepEqual(got, v) && !(v == nil && got == nil) {
			t.Errorf("%s: %#v != %#v", raw, got, v)
		}
	}
	nan, _ := dbdriver.EncodeSnapshotCell(math.NaN())
	if back, _ := dbdriver.DecodeSnapshotCell(nan); nan.T != "float" || !math.IsNaN(back.(float64)) {
		t.Fatalf("NaN %+v", nan)
	}
	for v, want := range map[string]any{`{"t":"nbytes"}`: []byte(nil), `{"t":"bytes","v":"/wA="}`: []byte{0xff, 0}, `{"t":"strb","v":"/2E="}`: string([]byte{0xff, 'a'})} {
		c, _ := dbdriver.EncodeSnapshotCell(want)
		if raw, _ := json.Marshal(c); string(raw) != v {
			t.Errorf("wire form %s, want %s", raw, v)
		}
	}
	if _, err := dbdriver.EncodeSnapshotCell(map[string]any{"a": 1}); err == nil {
		t.Fatal("map encoded")
	}
}

func TestSnapshotWriteKeepsBinaryValues(t *testing.T) {
	f := &dbdrivertest.FakeSQL{Script: map[string]dbdrivertest.FakeResult{
		"USE d":    {},
		"SELECT b": {Columns: []string{"b", "s", "n"}, Rows: [][]driver.Value{{[]byte{0xff, 0}, string([]byte{0xff, 'a'}), nil}}},
		"DELETE b": {Affected: 1},
	}}
	c := dbdrivertest.Start(t, map[string]dbdriver.Driver{"sql": snapshotDriver(f, dbdriver.SQLOptions{})}, dbdriver.ServerOptions{}, nil)
	h := c.Connect("sql", nil)
	res, e := c.Call("plugin.db.snapshotWrite", snapshotParams(h, map[string]any{"snapshot": stmt("SELECT b"), "write": stmt("DELETE b"), "maxRows": 1}))
	if e != nil || !strings.Contains(string(res), `"rows":[[{"t":"bytes","v":"/wA="},{"t":"strb","v":"/2E="},{"t":"null"}]]`) {
		t.Fatalf("binary %s %+v", res, e)
	}
}

func TestSnapshotWriteProbeRowAnswersExceeded(t *testing.T) {
	f := &dbdrivertest.FakeSQL{Script: map[string]dbdrivertest.FakeResult{
		"USE d":        {},
		"SELECT three": {Columns: []string{"v"}, Rows: [][]driver.Value{{"a"}, {"b"}, {strings.Repeat("x", 2<<20)}}},
		"DELETE three": {Affected: 3},
	}}
	c := dbdrivertest.Start(t, map[string]dbdriver.Driver{"sql": snapshotDriver(f, dbdriver.SQLOptions{})}, dbdriver.ServerOptions{}, nil)
	h := c.Connect("sql", nil)
	res, e := c.Call("plugin.db.snapshotWrite", snapshotParams(h, map[string]any{"snapshot": stmt("SELECT three"), "write": stmt("DELETE three"), "maxRows": 2}))
	if e != nil || string(res) != `{"steps":[],"exceeded":true,"exceededStep":0}` {
		t.Fatalf("probe row %s %+v", res, e)
	}
}

func TestSnapshotWriteReturnedValuesChargeTheBudget(t *testing.T) {
	f := &dbdrivertest.FakeSQL{Script: map[string]dbdrivertest.FakeResult{
		"USE d":       {},
		"SELECT fill": {Columns: []string{"v"}, Rows: [][]driver.Value{{strings.Repeat("x", 900<<10)}}},
	}}
	returned := map[string]any{"id": strings.Repeat("k", 200<<10)}
	o := dbdriver.SQLOptions{
		MutateIn: func(context.Context, dbdriver.Querier, dbdriver.Mutation) (int64, error) { return 1, nil },
		InsertReturning: func(context.Context, dbdriver.Querier, dbdriver.Mutation, []string) (int64, map[string]any, error) {
			return 1, returned, nil
		},
	}
	c := dbdrivertest.Start(t, map[string]dbdriver.Driver{"sql": snapshotDriver(f, o)}, dbdriver.ServerOptions{}, nil)
	h := c.Connect("sql", nil)
	insert := map[string]any{"mutation": map[string]any{"op": "insertRow", "table": "t", "fields": map[string]any{}}, "returning": []string{"id"}, "maxRows": 1}
	insert["snapshot"] = stmt("SELECT fill")
	if _, e := c.Call("plugin.db.snapshotWrite", snapshotParams(h, insert)); dbdrivertest.ErrorKind(e) != "limit" {
		t.Fatalf("returned over budget %+v", e)
	}
	// A value without a lossless encoding fails the request before commit.
	returned = map[string]any{"id": map[string]any{"a": 1}}
	delete(insert, "snapshot")
	insert["maxRows"] = 0
	if _, e := c.Call("plugin.db.snapshotWrite", snapshotParams(h, insert)); dbdrivertest.ErrorKind(e) != "database" {
		t.Fatalf("unencodable returned %+v", e)
	}
	if begun, committed, rolledBack := f.TxCounts(); begun != 2 || committed != 0 || rolledBack != 2 {
		t.Fatalf("tx %d/%d/%d", begun, committed, rolledBack)
	}
}
