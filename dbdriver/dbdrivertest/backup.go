package dbdrivertest

import (
	"bytes"
	"encoding/base64"
	"encoding/json"

	"github.com/mincodeai/plugin-sdk-go/rpc"
)

// BackupRecord is one JSONL record of a plugin.db.backup stream.
type BackupRecord map[string]json.RawMessage

// Backup runs plugin.db.backup for database.table and fetches every chunk.
// It returns the decoded records (without the completion marker) and
// whether the stream ended with the marker; a driver error is returned as e.
func (c *Client) Backup(handle, requestID, database, table string) (records []BackupRecord, complete bool, e *rpc.Error) {
	c.t.Helper()
	params := map[string]any{"handle": handle, "requestId": requestID, "database": database, "table": table, "maxRows": 8, "maxBytes": 1 << 20}
	res, e := c.Call("plugin.db.backup", params)
	if e != nil {
		return nil, false, e
	}
	var data []byte
	for {
		p, err := DecodePage(res)
		if err != nil {
			c.t.Fatalf("decode backup page: %v", err)
		}
		if p.Notice != "" {
			c.t.Fatalf("backup page carried a truncation notice: %s", p.Notice)
		}
		for _, row := range p.Rows {
			s, ok := row[0].(string)
			if !ok || len(row) != 1 {
				c.t.Fatalf("backup chunk is not one base64 string: %v", row)
			}
			chunk, err := base64.StdEncoding.DecodeString(s)
			if err != nil {
				c.t.Fatalf("backup chunk: %v", err)
			}
			data = append(data, chunk...)
		}
		if p.Done {
			break
		}
		if res, e = c.Call("plugin.db.fetch", map[string]any{"handle": handle, "cursor": p.Cursor, "maxRows": 8, "maxBytes": 1 << 20}); e != nil {
			return nil, false, e
		}
	}
	const marker = "\n{\"mincodeBackupComplete\":1}\n"
	complete = bytes.HasSuffix(data, []byte(marker))
	data = bytes.TrimSuffix(data, []byte(marker))
	for _, line := range bytes.Split(bytes.TrimSpace(data), []byte("\n")) {
		if len(line) == 0 {
			continue
		}
		var rec BackupRecord
		dec := json.NewDecoder(bytes.NewReader(line))
		dec.UseNumber()
		if err := dec.Decode(&rec); err != nil {
			c.t.Fatalf("backup record %q: %v", line, err)
		}
		records = append(records, rec)
	}
	return records, complete, nil
}

// Kind returns the record kind (metadata, columns, row or record).
func (r BackupRecord) Kind() string {
	var k string
	_ = json.Unmarshal(r["kind"], &k)
	return k
}
