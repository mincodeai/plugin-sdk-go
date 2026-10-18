package dbdriver

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/mincodeai/plugin-sdk-go/rpc"
	"io"
	"math"
	"math/big"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"
)

// Native backup streams (plugin.db.backup, announced by connect info
// backupVersion 1). Unlike execute/tableRows, the stream is not a display
// result: no row/byte caps, no cell normalisation and no truncation notice.
// The provider writes JSONL records; the Server cuts them into bounded
// base64 chunks and appends the completion marker only after the provider
// returned nil, so the host can tell a complete stream from a cut one.

// BackupVersion is the plugin.db.backup stream version this package serves.
const BackupVersion = 1

// backupChunkBytes bounds one raw chunk (one row of the backup result).
const backupChunkBytes = 16 * 1024

// backupCompleteMarker ends every successful stream.
const backupCompleteMarker = "\n{\"mincodeBackupComplete\":1}\n"

// BackupRequest selects one complete native object, never a preview page.
type BackupRequest struct{ RequestID, Database, Table string }

// BackupProvider is implemented by a Conn that can export one object
// losslessly. WriteBackup writes JSONL native metadata and typed records to
// out, returns an error on truncation, partial results or unsupported values,
// obeys ctx, and never falls back to preview results. It must not write the
// completion marker itself.
type BackupProvider interface {
	WriteBackup(ctx context.Context, req BackupRequest, out io.Writer) error
}

type backupParams struct {
	Handle    string `json:"handle"`
	RequestID string `json:"requestId"`
	Database  string `json:"database"`
	Table     string `json:"table"`
	MaxRows   int    `json:"maxRows"`
	MaxBytes  int    `json:"maxBytes"`
}

func (s *Server) startBackup(params json.RawMessage) (func() (any, error), *rpc.Error) {
	var p backupParams
	if e := decodeParams(params, &p); e != nil {
		return nil, e
	}
	if p.Table == "" || len(p.Table) > 4096 || len(p.Database) > 4096 {
		return nil, invalidf("invalid params: backup object is required")
	}
	// No request timeout: a complete export may run for hours; the host
	// cancels it through plugin.db.cancel.
	return s.startResult(p.Handle, p.RequestID, p.MaxRows, p.MaxBytes, 0, func(ctx context.Context, c Conn) (*Result, error) {
		provider, ok := c.(BackupProvider)
		if !ok {
			return nil, ErrUnsupported
		}
		reader, writer := io.Pipe()
		stop := context.AfterFunc(ctx, func() { _ = writer.CloseWithError(context.Cause(ctx)) })
		go func() {
			err := provider.WriteBackup(ctx, BackupRequest{p.RequestID, p.Database, p.Table}, writer)
			if err == nil {
				_, err = io.WriteString(writer, backupCompleteMarker)
			}
			_ = writer.CloseWithError(err)
		}()
		return &Result{Rows: &backupChunkRows{reader: reader, stop: stop}}, nil
	}, false, false)
}

type backupChunkRows struct {
	reader *io.PipeReader
	stop   func() bool
	once   sync.Once
}

func (r *backupChunkRows) Columns() []Column {
	return []Column{{Name: "chunk", Type: "base64"}}
}

func (r *backupChunkRows) Next(ctx context.Context) ([]any, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	buf := make([]byte, backupChunkBytes)
	n, err := io.ReadAtLeast(r.reader, buf, 1)
	if n > 0 {
		return []any{base64.StdEncoding.EncodeToString(buf[:n])}, nil
	}
	return nil, err
}

// Close stops the provider: its next write fails with io.ErrClosedPipe.
func (r *backupChunkRows) Close() error {
	r.once.Do(func() { r.stop(); _ = r.reader.Close() })
	return nil
}

// BackupConvert maps a driver-specific value (an exact decimal, a UUID, an
// ordered map) to a typed cell. handled false falls through to the generic
// rules of BackupValue.
type BackupConvert func(value any) (typed any, handled bool, err error)

// BackupCell builds a typed cell {type, value}.
func BackupCell(kind string, value any) any { return map[string]any{"type": kind, "value": value} }

// BackupValue types a value without loss: binary as base64, integers and
// floats as exact text with their Go type, nested arrays and maps
// recursively. Values it cannot represent faithfully are an error, never a
// guessed JSON rendering.
func BackupValue(value any) (any, error) { return backupValue(value, nil, 0) }

const backupMaxDepth = 64

func backupValue(value any, convert BackupConvert, depth int) (any, error) {
	if depth > backupMaxDepth {
		return nil, fmt.Errorf("native backup value nests deeper than %d levels", backupMaxDepth)
	}
	if typed, ok := value.(backupTyped); ok {
		return typed.cell, nil
	}
	if convert != nil {
		if typed, handled, err := convert(value); err != nil || handled {
			return typed, err
		}
	}
	switch v := value.(type) {
	case nil:
		return BackupCell("null", nil), nil
	case []byte:
		return BackupCell("binary-base64", base64.StdEncoding.EncodeToString(v)), nil
	case string:
		if !utf8.ValidString(v) {
			return BackupCell("binary-base64", base64.StdEncoding.EncodeToString([]byte(v))), nil
		}
		return BackupCell("string", v), nil
	case bool:
		return BackupCell("bool", v), nil
	case time.Time:
		return BackupCell("timestamp", v.Format(time.RFC3339Nano)), nil
	case time.Duration:
		return BackupCell("duration-ns", strconv.FormatInt(int64(v), 10)), nil
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return BackupCell(fmt.Sprintf("%T", v), fmt.Sprint(v)), nil
	case float32:
		return BackupCell("float32", backupFloat(float64(v), 32)), nil
	case float64:
		return BackupCell("float64", backupFloat(v, 64)), nil
	case json.Number:
		return BackupCell("number", string(v)), nil
	case json.RawMessage:
		if !json.Valid(v) {
			return nil, fmt.Errorf("invalid native JSON value")
		}
		return BackupCell("native-json", v), nil
	case *big.Int:
		if v == nil {
			return BackupCell("null", nil), nil
		}
		return BackupCell("big-int", v.String()), nil
	case *big.Rat:
		if v == nil {
			return BackupCell("null", nil), nil
		}
		return BackupCell("big-rational", v.String()), nil
	case *big.Float:
		if v == nil {
			return BackupCell("null", nil), nil
		}
		return BackupCell("big-float", v.Text('g', -1)), nil
	case []any:
		items := make([]any, len(v))
		for i, item := range v {
			next, err := backupValue(item, convert, depth+1)
			if err != nil {
				return nil, err
			}
			items[i] = next
		}
		return BackupCell("array", items), nil
	case map[string]any:
		items := make(map[string]any, len(v))
		for key, item := range v {
			if !utf8.ValidString(key) {
				return nil, fmt.Errorf("invalid UTF-8 native map key")
			}
			next, err := backupValue(item, convert, depth+1)
			if err != nil {
				return nil, err
			}
			items[key] = next
		}
		return BackupCell("map", items), nil
	case driver.Valuer:
		resolved, err := v.Value()
		if err != nil {
			return nil, fmt.Errorf("native backup value %T: %w", value, err)
		}
		if _, again := resolved.(driver.Valuer); again && depth >= 4 {
			return nil, fmt.Errorf("native backup value %T nests too many Valuer conversions", value)
		}
		cell, err := backupValue(resolved, convert, depth+1)
		if err != nil {
			return nil, err
		}
		return BackupCell(fmt.Sprintf("valuer:%T", value), cell), nil
	}
	return backupReflect(value, convert, depth)
}

// backupReflect covers typed containers and named scalar kinds that drivers
// return directly ([]string, [16]byte, map[int64]string, type Code int16).
// Map keys keep their typed form as {key, value} entries in a stable order.
// Structs are refused: guessing their fields could silently lose data.
func backupReflect(value any, convert BackupConvert, depth int) (any, error) {
	rv := reflect.ValueOf(value)
	kind := rv.Type().String()
	switch rv.Kind() {
	case reflect.Pointer:
		if rv.IsNil() {
			return BackupCell("null", nil), nil
		}
		return backupValue(rv.Elem().Interface(), convert, depth+1)
	case reflect.Bool:
		return BackupCell(kind, rv.Bool()), nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return BackupCell(kind, strconv.FormatInt(rv.Int(), 10)), nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return BackupCell(kind, strconv.FormatUint(rv.Uint(), 10)), nil
	case reflect.Float32:
		return BackupCell(kind, backupFloat(rv.Float(), 32)), nil
	case reflect.Float64:
		return BackupCell(kind, backupFloat(rv.Float(), 64)), nil
	case reflect.String:
		return backupValue(rv.String(), convert, depth+1)
	case reflect.Slice, reflect.Array:
		if rv.Kind() == reflect.Slice && rv.IsNil() {
			return BackupCell("null", nil), nil
		}
		if rv.Type().Elem().Kind() == reflect.Uint8 {
			raw := make([]byte, rv.Len())
			reflect.Copy(reflect.ValueOf(raw), rv)
			return BackupCell(kind, base64.StdEncoding.EncodeToString(raw)), nil
		}
		items := make([]any, rv.Len())
		for i := range items {
			next, err := backupValue(rv.Index(i).Interface(), convert, depth+1)
			if err != nil {
				return nil, err
			}
			items[i] = next
		}
		return BackupCell(kind, items), nil
	case reflect.Map:
		if rv.IsNil() {
			return BackupCell("null", nil), nil
		}
		type entry struct {
			sortKey    string
			key, value any
		}
		entries := make([]entry, 0, rv.Len())
		for it := rv.MapRange(); it.Next(); {
			key, err := backupValue(it.Key().Interface(), convert, depth+1)
			if err != nil {
				return nil, err
			}
			item, err := backupValue(it.Value().Interface(), convert, depth+1)
			if err != nil {
				return nil, err
			}
			sortKey, _ := json.Marshal(key)
			entries = append(entries, entry{string(sortKey), key, item})
		}
		sort.Slice(entries, func(i, j int) bool { return entries[i].sortKey < entries[j].sortKey })
		items := make([]any, len(entries))
		for i, e := range entries {
			items[i] = map[string]any{"key": e.key, "value": e.value}
		}
		return BackupCell(kind, items), nil
	}
	return nil, fmt.Errorf("unsupported native backup value %T", value)
}

func backupFloat(v float64, bits int) string {
	switch {
	case math.IsNaN(v):
		return "NaN"
	case math.IsInf(v, 1):
		return "+Inf"
	case math.IsInf(v, -1):
		return "-Inf"
	}
	return strconv.FormatFloat(v, 'g', -1, bits)
}

// BackupWriter writes the JSONL records of one object.
type BackupWriter struct {
	enc     *json.Encoder
	convert BackupConvert
}

// NewBackupWriter returns a writer typing values with convert (may be nil).
func NewBackupWriter(out io.Writer, convert BackupConvert) *BackupWriter {
	enc := json.NewEncoder(out)
	enc.SetEscapeHTML(false)
	return &BackupWriter{enc: enc, convert: convert}
}

// Metadata writes a native definition record (DDL, mapping, schema JSON).
func (w *BackupWriter) Metadata(database, object string, definition any) error {
	return w.enc.Encode(map[string]any{"kind": "metadata", "database": database, "object": object, "definition": definition})
}

// Columns writes the column record that precedes positional rows.
func (w *BackupWriter) Columns(columns []Column) error {
	return w.enc.Encode(map[string]any{"kind": "columns", "columns": columns})
}

// Row writes one positional row of typed cells.
func (w *BackupWriter) Row(values []any) error {
	row := make([]any, len(values))
	for i, value := range values {
		cell, err := backupValue(value, w.convert, 0)
		if err != nil {
			return err
		}
		row[i] = cell
	}
	return w.enc.Encode(map[string]any{"kind": "row", "values": row})
}

// Record writes one native document (search hit, item, cell) as typed data.
func (w *BackupWriter) Record(value any) error {
	cell, err := backupValue(value, w.convert, 0)
	if err != nil {
		return err
	}
	return w.enc.Encode(map[string]any{"kind": "record", "value": cell})
}

// RawRecord writes one record exactly as the server returned it (raw JSON
// keeps every numeric lexeme and nested type).
func (w *BackupWriter) RawRecord(raw json.RawMessage) error {
	if !json.Valid(raw) {
		return fmt.Errorf("invalid native JSON record")
	}
	return w.enc.Encode(map[string]any{"kind": "record", "raw": raw})
}

// Rows streams a native Rows result (from the driver itself, never a
// preview projection) as a columns record followed by typed rows.
func (w *BackupWriter) Rows(ctx context.Context, rows Rows) error {
	if rows == nil {
		return Errorf(KindDatabase, "missing native backup row stream")
	}
	defer rows.Close()
	if err := w.Columns(rows.Columns()); err != nil {
		return err
	}
	for {
		values, err := rows.Next(ctx)
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		if err := w.Row(values); err != nil {
			return err
		}
	}
}

// BackupDefinition reads the mandatory DDL of req's table. A missing
// definition fails the backup instead of exporting data alone.
func BackupDefinition(ctx context.Context, metadata MetadataProvider, req BackupRequest) (any, error) {
	if metadata == nil {
		return nil, ErrUnsupported
	}
	definition, err := metadata.Metadata(ctx, MetadataRequest{Kind: MetadataDDL, Database: req.Database, Table: req.Table})
	if err != nil {
		return nil, err
	}
	if definition == nil || definition.Value == nil {
		return nil, Errorf(KindDatabase, "missing backup object definition")
	}
	return definition.Value, nil
}

// SQLBackup describes how WriteSQLBackup reads one table.
type SQLBackup struct {
	DB *sql.DB
	// Metadata supplies the mandatory DDL.
	Metadata MetadataProvider
	// Target is the quoted, qualified table expression for SELECT * FROM.
	Target string
	// Convert types driver-specific values (optional).
	Convert BackupConvert
	// ConvertColumn types a value by its result column (optional): drivers
	// that scan some types as plain bytes (DuckDB UUID) need the column type.
	// handled false falls back to Convert and the generic rules.
	ConvertColumn func(column Column, value any) (typed any, handled bool, err error)
	// Prepare runs on the pinned connection before the query (read-only
	// transaction settings, session database); optional.
	Prepare func(ctx context.Context, conn *sql.Conn) error
	// Restore undoes Prepare on the same connection before it returns to
	// the pool (a session read-only flag); optional.
	Restore func(ctx context.Context, conn *sql.Conn) error
	// ReadOnlyTx runs the read in a read-only transaction.
	ReadOnlyTx bool
	// TransactionSQL, when set, opens a transaction and runs this statement
	// first (Oracle: SET TRANSACTION READ ONLY), instead of ReadOnlyTx.
	TransactionSQL string
}

// WriteSQLBackup scans database/sql rows into raw driver values. It
// bypasses the FromSQL result path, which renders []byte as text for
// display, and reads the whole table without LIMIT or OFFSET.
func WriteSQLBackup(ctx context.Context, b SQLBackup, req BackupRequest, out io.Writer) error {
	if b.DB == nil || b.Target == "" {
		return ErrUnsupported
	}
	if req.Table == "" || strings.ContainsRune(req.Table, 0) || strings.ContainsRune(req.Database, 0) {
		return Errorf(KindInvalid, "invalid backup object")
	}
	definition, err := BackupDefinition(ctx, b.Metadata, req)
	if err != nil {
		return err
	}
	w := NewBackupWriter(out, b.Convert)
	if err := w.Metadata(req.Database, req.Table, map[string]any{"ddl": definition}); err != nil {
		return err
	}
	conn, err := b.DB.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	if b.Prepare != nil {
		if err := b.Prepare(ctx, conn); err != nil {
			return err
		}
	}
	if b.Restore != nil {
		// A connection that cannot be restored is discarded, never pooled.
		defer func() {
			if err := b.Restore(context.WithoutCancel(ctx), conn); err != nil {
				_ = conn.Raw(func(any) error { return driver.ErrBadConn })
			}
		}()
	}
	type querier interface {
		QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	}
	var q querier = conn
	switch {
	case b.TransactionSQL != "":
		tx, err := conn.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer func() { _ = tx.Rollback() }()
		if _, err := tx.ExecContext(ctx, b.TransactionSQL); err != nil {
			return err
		}
		q = tx
	case b.ReadOnlyTx:
		tx, err := conn.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
		if err != nil {
			return err
		}
		defer func() { _ = tx.Rollback() }()
		q = tx
	}
	rows, err := q.QueryContext(ctx, "SELECT * FROM "+b.Target)
	if err != nil {
		return err
	}
	defer rows.Close()
	types, err := rows.ColumnTypes()
	if err != nil {
		return err
	}
	cols := make([]Column, len(types))
	for i, t := range types {
		cols[i] = Column{Name: t.Name(), Type: t.DatabaseTypeName()}
	}
	if err := w.Columns(cols); err != nil {
		return err
	}
	values := make([]any, len(cols))
	pointers := make([]any, len(cols))
	for i := range values {
		pointers[i] = &values[i]
	}
	for rows.Next() {
		if err := ctx.Err(); err != nil {
			return err
		}
		for i := range values {
			values[i] = nil
		}
		// Scan into *any copies driver []byte, so no value aliases the next row.
		if err := rows.Scan(pointers...); err != nil {
			return err
		}
		row := values
		if b.ConvertColumn != nil {
			row = make([]any, len(values))
			for i, v := range values {
				typed, handled, err := b.ConvertColumn(cols[i], v)
				if err != nil {
					return fmt.Errorf("column %s: %w", cols[i].Name, err)
				}
				if handled {
					row[i] = backupTyped{typed}
				} else {
					row[i] = v
				}
			}
		}
		if err := w.Row(row); err != nil {
			return err
		}
	}
	return rows.Err()
}

// backupTyped marks a cell already typed by a converter, so the generic
// rules do not wrap it a second time.
type backupTyped struct{ cell any }
