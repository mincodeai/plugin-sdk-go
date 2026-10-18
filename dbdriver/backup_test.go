package dbdriver_test

import (
	"bytes"
	"context"
	"database/sql/driver"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"math/big"
	"strings"
	"testing"

	"github.com/mincodeai/plugin-sdk-go/dbdriver"
	"github.com/mincodeai/plugin-sdk-go/dbdriver/dbdrivertest"
	"github.com/mincodeai/plugin-sdk-go/rpc"
)

// backupConn is a Conn that streams n JSONL records of size bytes each.
type backupConn struct {
	*memConn
	n, size int
	fail    bool
}

func (c *backupConn) WriteBackup(ctx context.Context, req dbdriver.BackupRequest, out io.Writer) error {
	w := dbdriver.NewBackupWriter(out, nil)
	if err := w.Metadata(req.Database, req.Table, map[string]any{"ddl": "CREATE TABLE t (b BLOB)"}); err != nil {
		return err
	}
	for i := range c.n {
		if c.fail && i == c.n/2 {
			return errors.New("lost the server")
		}
		if err := w.Row([]any{bytes.Repeat([]byte{byte(i)}, c.size), int64(1 << 62)}); err != nil {
			return err
		}
	}
	return nil
}

func backupDriver(c dbdriver.Conn) dbdriver.Driver {
	return dbdriver.DriverFunc(func(context.Context, dbdriver.Config) (dbdriver.Conn, dbdriver.ConnInfo, error) {
		return c, dbdriver.ConnInfo{}, nil
	})
}

// readBackup fetches every chunk of plugin.db.backup and joins them.
func readBackup(t *testing.T, c *dbdrivertest.Client, h, req string) ([]byte, bool) {
	t.Helper()
	res, e := c.Call("plugin.db.backup", map[string]any{"handle": h, "requestId": req, "database": "d", "table": "t", "maxRows": 4, "maxBytes": 200000})
	if e != nil {
		return nil, false
	}
	var out []byte
	for {
		p, err := dbdrivertest.DecodePage(res)
		if err != nil {
			t.Fatal(err)
		}
		if p.Notice != "" {
			t.Fatalf("backup page carried a notice: %s", p.Notice)
		}
		for _, row := range p.Rows {
			chunk, err := base64.StdEncoding.DecodeString(row[0].(string))
			if err != nil || len(chunk) > 16*1024 {
				t.Fatalf("chunk %d bytes, %v", len(chunk), err)
			}
			out = append(out, chunk...)
		}
		if p.Done {
			return out, true
		}
		var fetchErr *rpc.Error
		res, fetchErr = c.Call("plugin.db.fetch", map[string]any{"handle": h, "cursor": p.Cursor, "maxRows": 4, "maxBytes": 200000})
		if fetchErr != nil {
			return out, false
		}
	}
}

func TestNativeBackupStream(t *testing.T) {
	conn := &backupConn{memConn: newMem(), n: 40, size: 9000}
	c := dbdrivertest.Start(t, map[string]dbdriver.Driver{"b": backupDriver(conn), "plain": memDriver(newMem())}, dbdriver.ServerOptions{}, nil)

	res, e := c.Call("plugin.db.connect", dbdrivertest.ConnectParams("b", nil))
	if e != nil || !strings.Contains(string(res), `"backupVersion":1`) {
		t.Fatalf("backup provider must announce backupVersion: %s %+v", res, e)
	}
	var connected struct{ Handle string }
	_ = json.Unmarshal(res, &connected)

	data, ok := readBackup(t, c, connected.Handle, "b1")
	if !ok {
		t.Fatal("complete backup failed")
	}
	if !bytes.HasSuffix(data, []byte("\n{\"mincodeBackupComplete\":1}\n")) {
		t.Fatal("complete stream lacks the completion marker")
	}
	lines := bytes.Split(bytes.TrimSpace(data), []byte("\n"))
	// metadata + 40 rows + blank separator line + marker.
	rows := 0
	for _, line := range lines {
		var rec struct {
			Kind   string `json:"kind"`
			Values []struct {
				Type  string `json:"type"`
				Value string `json:"value"`
			} `json:"values"`
		}
		if len(line) == 0 || json.Unmarshal(line, &rec) != nil || rec.Kind != "row" {
			continue
		}
		rows++
		if rec.Values[0].Type != "binary-base64" || rec.Values[1].Value != "4611686018427387904" {
			t.Fatalf("lossy row %s", line)
		}
		if raw, _ := base64.StdEncoding.DecodeString(rec.Values[0].Value); len(raw) != 9000 {
			t.Fatalf("binary value cut to %d bytes", len(raw))
		}
	}
	if rows != 40 {
		t.Fatalf("got %d rows, want 40 beyond any page budget", rows)
	}

	// A provider failure ends the result with an error and never a marker.
	conn.fail = true
	if partial, ok := readBackup(t, c, connected.Handle, "b2"); ok || bytes.Contains(partial, []byte("mincodeBackupComplete")) {
		t.Fatal("failed backup was reported complete")
	}

	plain := c.Connect("plain", nil)
	if _, e := c.Call("plugin.db.backup", map[string]any{"handle": plain, "requestId": "p", "database": "d", "table": "t", "maxRows": 4, "maxBytes": 200000}); dbdrivertest.ErrorKind(e) != "unsupported" {
		t.Fatalf("conn without BackupProvider %+v", e)
	}
}

type decimal struct{ text string }

func (d decimal) Value() (driver.Value, error) { return d.text, nil }

func TestBackupValueIsLossless(t *testing.T) {
	cases := []struct {
		in   any
		want string
	}{
		{[]byte{0, 0xff}, `{"type":"binary-base64","value":"AP8="}`},
		{string([]byte{0xff, 0}), `{"type":"binary-base64","value":"/wA="}`},
		{uint64(18446744073709551615), `{"type":"uint64","value":"18446744073709551615"}`},
		{new(big.Int).Lsh(big.NewInt(1), 100), `{"type":"big-int","value":"1267650600228229401496703205376"}`},
		{decimal{"12.3400"}, `{"type":"valuer:dbdriver_test.decimal","value":{"type":"string","value":"12.3400"}}`},
		{json.Number("1e400"), `{"type":"number","value":"1e400"}`},
	}
	for _, tc := range cases {
		got, err := dbdriver.BackupValue(tc.in)
		if err != nil {
			t.Fatalf("%T: %v", tc.in, err)
		}
		raw, _ := json.Marshal(got)
		if string(raw) != tc.want {
			t.Errorf("%T: %s, want %s", tc.in, raw, tc.want)
		}
	}
	if _, err := dbdriver.BackupValue(struct{ X int }{1}); err == nil {
		t.Error("unknown structs must be refused, not guessed")
	}
}
