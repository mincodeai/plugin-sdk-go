package dbdriver_test

import (
	"bytes"
	"context"
	"database/sql/driver"
	"encoding/base64"
	"encoding/json"
	"math"
	"math/big"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/mincodeai/plugin-sdk-go/dbdriver"
	"github.com/mincodeai/plugin-sdk-go/dbdriver/dbdrivertest"
)

type textValuer struct{ text string }

func (v textValuer) Value() (driver.Value, error) { return v.text, nil }

type plainStruct struct {
	A int    `json:"a"`
	B string `json:"b"`
}

// mixedRows covers every v1 value class, columns of one kind with and
// without nulls, an all-null column and columns of mixed kinds.
func mixedRows() ([]dbdriver.Column, [][]any) {
	cols := []dbdriver.Column{
		{Name: "int"}, {Name: "intNull"}, {Name: "float"}, {Name: "str"}, {Name: "bool"},
		{Name: "json"}, {Name: "allNull"}, {Name: "mixed"}, {Name: "special"},
	}
	when := time.Date(2026, 1, 2, 3, 4, 5, 600700800, time.UTC)
	rows := [][]any{
		{int64(1), nil, 0.1, "alice", true, json.RawMessage(`{"tags":["a"],"n":1e400}`), nil, int8(-5), uint64(1 << 63)},
		{int32(-2), int64(9007199254740993), float32(0.1), []byte("b\xffo\xfeb"), false, map[string]any{"k": []any{1, "x", nil}}, nil, "text", math.NaN()},
		{uint16(3), int64(-9007199254740992), 1e21, "", true, []any{}, nil, 2.5, math.Inf(-1)},
		{int(4), nil, -0.0, "line sep\n\"q\"", false, nil, nil, true, when},
		{uint(5), int64(42), 123456789.125, "日本語", true, plainStruct{A: 7, B: "s"}, nil, nil, json.Number("123456789012345678901234567890")},
		{int64(6), int64(0), 1e-7, "x", false, json.RawMessage(`[1,2,{"d":true}]`), nil, map[string]any{"z": 1}, big.NewInt(-77)},
		{int64(7), int64(1), float64(7), "y", true, nil, nil, []byte("bytes"), textValuer{"valuer"}},
		{int64(8), nil, math.MaxFloat64, "z", true, json.RawMessage(`{}`), nil, int64(1 << 60), json.Number("1.5e3")},
	}
	return cols, rows
}

func sliceDriver(cols []dbdriver.Column, rows [][]any) dbdriver.Driver {
	conn := &memConn{}
	conn.exec = func(_ context.Context, req dbdriver.ExecuteRequest) (*dbdriver.Result, error) {
		switch req.Statement {
		case "empty":
			return &dbdriver.Result{Rows: dbdriver.SliceRows(cols, nil)}, nil
		case "nocols":
			return &dbdriver.Result{Rows: dbdriver.SliceRows([]dbdriver.Column{}, [][]any{{}, {}})}, nil
		case "update":
			return &dbdriver.Result{AffectedRows: dbdriver.AffectedRows(3)}, nil
		}
		return &dbdriver.Result{Rows: dbdriver.SliceRows(cols, rows)}, nil
	}
	return memDriver(conn)
}

func connectEncoding(t *testing.T, c *dbdrivertest.Client, encoding string) (string, json.RawMessage) {
	t.Helper()
	params := dbdrivertest.ConnectParams("mem", nil)
	params["limits"] = map[string]any{"pageMaxBytes": dbdriver.MaxPageBytes, "pageMaxRows": dbdriver.MaxPageRows, "cursorIdleMs": 60000}
	if encoding != "" {
		params["encoding"] = encoding
	}
	res, e := c.Call("plugin.db.connect", params)
	if e != nil {
		t.Fatalf("connect %q: %d %s", encoding, e.Code, e.Message)
	}
	var r struct {
		Handle string `json:"handle"`
	}
	_ = json.Unmarshal(res, &r)
	return r.Handle, res
}

// queryAll runs statement and returns every decoded page.
func queryAll(t *testing.T, c *dbdrivertest.Client, h, req, statement string, maxRows, maxBytes int) ([]dbdrivertest.Page, []json.RawMessage) {
	t.Helper()
	res, e := c.Call("plugin.db.execute", execParams(h, req, statement, maxRows, maxBytes))
	if e != nil {
		t.Fatalf("execute: %s", e.Message)
	}
	var pages []dbdrivertest.Page
	var raws []json.RawMessage
	for {
		p, err := dbdrivertest.DecodePage(res)
		if err != nil {
			t.Fatalf("decode %s: %v", res, err)
		}
		pages, raws = append(pages, p), append(raws, res)
		if p.Done {
			return pages, raws
		}
		if res, e = c.Call("plugin.db.fetch", map[string]any{"handle": h, "cursor": p.Cursor, "maxRows": maxRows, "maxBytes": maxBytes}); e != nil {
			t.Fatalf("fetch: %s", e.Message)
		}
	}
}

func TestColumnarNegotiation(t *testing.T) {
	cols, rows := mixedRows()
	plain := dbdrivertest.Start(t, map[string]dbdriver.Driver{"mem": sliceDriver(cols, rows)}, dbdriver.ServerOptions{}, nil)
	if details := string(plain.Server.DatabaseDetails()); strings.Contains(details, "encodings") {
		t.Fatalf("encodings announced without ColumnarEncoding: %s", details)
	}
	// A host that never saw the announcement cannot request the encoding.
	params := dbdrivertest.ConnectParams("mem", nil)
	params["encoding"] = dbdriver.EncodingColumnar
	if _, e := plain.Call("plugin.db.connect", params); e == nil || dbdrivertest.ErrorKind(e) != "invalid" {
		t.Fatalf("unannounced encoding accepted: %+v", e)
	}

	v10 := dbdrivertest.Start(t, map[string]dbdriver.Driver{"mem": sliceDriver(cols, rows)}, dbdriver.ServerOptions{ProtocolV10: true, ColumnarEncoding: true}, nil)
	if details := string(v10.Server.DatabaseDetails()); strings.Contains(details, "encodings") {
		t.Fatalf("protocol 1.0 announced encodings: %s", details)
	}
	if _, e := v10.Call("plugin.db.connect", params); e == nil {
		t.Fatal("protocol 1.0 accepted the columnar encoding")
	}

	c := dbdrivertest.Start(t, map[string]dbdriver.Driver{"mem": sliceDriver(cols, rows)}, dbdriver.ServerOptions{ColumnarEncoding: true}, nil)
	if details := string(c.Server.DatabaseDetails()); !strings.Contains(details, `"encodings":["columnar1"]`) || !strings.Contains(details, `"minor":1`) {
		t.Fatalf("details %s", details)
	}
	params["encoding"] = "columnar9"
	if _, e := c.Call("plugin.db.connect", params); e == nil || dbdrivertest.ErrorKind(e) != "invalid" {
		t.Fatalf("unknown encoding accepted: %+v", e)
	}
	// Old host (no encoding member): JSON rows, no confirmation.
	h1, res := connectEncoding(t, c, "")
	if strings.Contains(string(res), "encoding") {
		t.Fatalf("connect without encoding confirmed one: %s", res)
	}
	if _, raws := queryAll(t, c, h1, "q1", "all", 3, 1<<20); !strings.Contains(string(raws[0]), `"rows":[`) || strings.Contains(string(raws[0]), "data") {
		t.Fatalf("v1 page %s", raws[0])
	}
	// New host: confirmed, columnar pages.
	h2, res := connectEncoding(t, c, dbdriver.EncodingColumnar)
	if !strings.Contains(string(res), `"encoding":"columnar1"`) {
		t.Fatalf("connect did not confirm the encoding: %s", res)
	}
	if _, raws := queryAll(t, c, h2, "q2", "all", 3, 1<<20); !strings.Contains(string(raws[0]), `"encoding":"columnar1","data":"`) || strings.Contains(string(raws[0]), `"rows"`) {
		t.Fatalf("columnar page %s", raws[0])
	}
	// Statements without a result set keep the v1 shape.
	if res, _ := c.Call("plugin.db.execute", execParams(h2, "q3", "update", 10, 2000)); string(res) != `{"rows":[],"affectedRows":3,"done":true}` {
		t.Fatalf("update %s", res)
	}
}

// TestColumnarMatchesJSONRows pins the value space: the same result through
// a JSON handle and a columnar handle decodes to identical rows, page by
// page, for several page sizes.
func TestColumnarMatchesJSONRows(t *testing.T) {
	cols, rows := mixedRows()
	c := dbdrivertest.Start(t, map[string]dbdriver.Driver{"mem": sliceDriver(cols, rows)}, dbdriver.ServerOptions{ColumnarEncoding: true}, nil)
	hJSON, _ := connectEncoding(t, c, "")
	hCol, _ := connectEncoding(t, c, dbdriver.EncodingColumnar)
	for _, statement := range []string{"all", "empty", "nocols"} {
		for _, maxRows := range []int{1, 3, 1000} {
			jsonPages, _ := queryAll(t, c, hJSON, "j-"+statement, statement, maxRows, 1<<20)
			colPages, _ := queryAll(t, c, hCol, "c-"+statement, statement, maxRows, 1<<20)
			if len(jsonPages) != len(colPages) {
				t.Fatalf("%s/%d: %d JSON pages, %d columnar pages", statement, maxRows, len(jsonPages), len(colPages))
			}
			for i := range jsonPages {
				j, k := jsonPages[i], colPages[i]
				if len(j.Rows) == 0 && len(k.Rows) == 0 {
					j.Rows, k.Rows = nil, nil
				}
				j.Encoding, j.Data, k.Encoding, k.Data = "", "", "", ""
				j.Rows, k.Rows = canonicalRows(j.Rows), canonicalRows(k.Rows)
				if j.Cursor != "" {
					j.Cursor, k.Cursor = "c", "c"
				}
				if !reflect.DeepEqual(j, k) {
					t.Fatalf("%s/%d page %d differs\n json     %#v\n columnar %#v", statement, maxRows, i, j, k)
				}
			}
		}
	}
}

// canonicalRows maps json.Number cells to the numbers the host parses
// (int64 or float64), the value space both encodings must agree on: a JSON
// row may spell 1500 as 1.5e3.
func canonicalRows(rows [][]any) [][]any {
	var canon func(v any) any
	canon = func(v any) any {
		switch x := v.(type) {
		case json.Number:
			// Numbers compare by value (1500 and 1.5e3 are the same
			// float); integers beyond 2^53 are strings in both encodings.
			if f, err := x.Float64(); err == nil {
				return f
			}
		case []any:
			out := make([]any, len(x))
			for i := range x {
				out[i] = canon(x[i])
			}
			return out
		case map[string]any:
			out := make(map[string]any, len(x))
			for k, e := range x {
				out[k] = canon(e)
			}
			return out
		}
		return v
	}
	out := make([][]any, len(rows))
	for i, row := range rows {
		out[i] = canon(row).([]any)
	}
	return out
}

func TestColumnarPagesStayWithinBudget(t *testing.T) {
	mc := newMem()
	c := dbdrivertest.Start(t, map[string]dbdriver.Driver{"mem": memDriver(mc)}, dbdriver.ServerOptions{ColumnarEncoding: true}, nil)
	h, _ := connectEncoding(t, c, dbdriver.EncodingColumnar)
	for _, budget := range []int{600, 1000, 4096, 65536} {
		pages, raws := queryAll(t, c, h, "b", "rows 200 37", 1000, budget)
		total := 0
		for i, raw := range raws {
			if len(raw) > budget {
				t.Fatalf("budget %d: page %d is %d bytes", budget, i, len(raw))
			}
			total += len(pages[i].Rows)
		}
		if total != 200 {
			t.Fatalf("budget %d: %d rows", budget, total)
		}
	}
	// A row larger than the budget ends the result with a notice.
	pages, _ := queryAll(t, c, h, "o", "rows 3 5000", 10, 2000)
	if len(pages) != 1 || len(pages[0].Rows) != 0 || pages[0].Notice == "" {
		t.Fatalf("oversized %+v", pages)
	}
	waitFor(t, func() bool { mc.mu.Lock(); defer mc.mu.Unlock(); return mc.rowsOut == 0 })
}

// TestColumnarHandleKeepsBackupRows: native backup streams stay JSON rows
// (base64 chunks) even on a columnar handle.
func TestColumnarHandleKeepsBackupRows(t *testing.T) {
	conn := &backupConn{memConn: newMem(), n: 3, size: 100}
	c := dbdrivertest.Start(t, map[string]dbdriver.Driver{"mem": backupDriver(conn)}, dbdriver.ServerOptions{ColumnarEncoding: true}, nil)
	h, _ := connectEncoding(t, c, dbdriver.EncodingColumnar)
	res, e := c.Call("plugin.db.backup", map[string]any{"handle": h, "requestId": "b1", "database": "d", "table": "t", "maxRows": 4, "maxBytes": 200000})
	if e != nil || !strings.Contains(string(res), `"rows":[`) || strings.Contains(string(res), `"data"`) {
		t.Fatalf("backup page %s %+v", res, e)
	}
	if data, ok := readBackup(t, c, h, "b2"); !ok || !bytes.HasSuffix(data, []byte("{\"mincodeBackupComplete\":1}\n")) {
		t.Fatal("backup on a columnar handle failed")
	}
}

func TestDecodeColumnarRejectsMalformed(t *testing.T) {
	for name, bin := range map[string][]byte{
		"magic":         []byte("XXXX\x01\x01\x00"),
		"truncated":     []byte("MCC1\x02\x01\x02\x02"),
		"trailing":      []byte("MCC1\x01\x01\x00\x00"),
		"bad kind":      []byte("MCC1\x01\x01\x09"),
		"bad utf8":      []byte("MCC1\x01\x01\x04\x01\xff"),
		"string length": []byte("MCC1\x01\x01\x04\x05ab"),
		"json scalar":   []byte("MCC1\x01\x01\x05\x011"),
		"bad tag":       []byte("MCC1\x01\x01\x06\x09"),
	} {
		if _, err := dbdrivertest.DecodeColumnar(base64.StdEncoding.EncodeToString(bin)); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}
