// Package dbdriver implements the plugin side of the MinCode database driver
// protocol v1.1 (docs/architecture/database-driver-protocol.md): a plugin that
// declares the database.driver capability and contributes.databaseDrivers
// implements Driver (and optionally MetadataProvider, TableRowsProvider,
// Mutator, Destroyer and SnapshotWriter on its Conn), wraps it in a Server
// and sets rpc.Options.Database. The Server owns everything protocol-shaped:
// strict params, opaque handles and cursors, pull-based paging within the
// page budget, cell encoding, requestId cancellation, timeouts, the 16-call and
// 32-session limits, idle cursor expiry and closing everything on shutdown.
//
// Credentials arrive as Secret values. Never log, print or persist them, and
// keep them in memory only for the lifetime of the connection.
package dbdriver

import (
	"context"
	"encoding/json"
	"io"
	"strconv"
)

// Protocol is the database driver protocol version this package implements;
// ProtocolMinor is its minor revision. Minor 1 (protocol 1.1) announces
// pageLimits in the initialize details so a 1.1 host may send pages of up to
// MaxPageRows rows / MaxPageBytes bytes; a 1.0 host keeps the 1.0 budget.
const (
	Protocol      = 1
	ProtocolMinor = 1
)

// Driver opens connections for one contributed driver id.
type Driver interface {
	// Connect opens a connection within ctx (the host's connect timeout).
	// Return an *Error of KindConnection (the default for other errors) when
	// the database cannot be reached.
	Connect(ctx context.Context, cfg Config) (Conn, ConnInfo, error)
}

// DriverFunc adapts a function to Driver.
type DriverFunc func(ctx context.Context, cfg Config) (Conn, ConnInfo, error)

// Connect calls f.
func (f DriverFunc) Connect(ctx context.Context, cfg Config) (Conn, ConnInfo, error) {
	return f(ctx, cfg)
}

// Config is the decrypted connection configuration the host sends in
// plugin.db.connect. Only the fields the manifest lists in connection.fields
// are set. Password and Token never print.
type Config struct {
	Host       string
	Port       int
	Database   string
	Username   string
	Password   Secret
	Token      Secret
	FilePath   string
	SSL        bool
	Org        string
	AuthSource string
	// Options are the driver options declared in the manifest; numbers are
	// json.Number. Use the Option* helpers.
	Options map[string]any
}

// OptionString returns the string option key or def.
func (c Config) OptionString(key, def string) string {
	if v, ok := c.Options[key].(string); ok {
		return v
	}
	return def
}

// OptionBool returns the boolean option key or def.
func (c Config) OptionBool(key string, def bool) bool {
	if v, ok := c.Options[key].(bool); ok {
		return v
	}
	return def
}

// OptionInt returns the integer option key or def.
func (c Config) OptionInt(key string, def int64) int64 {
	switch v := c.Options[key].(type) {
	case json.Number:
		if n, err := v.Int64(); err == nil {
			return n
		}
	case int:
		return int64(v)
	case int64:
		return v
	case float64:
		return int64(v)
	}
	return def
}

// OptionFloat returns the numeric option key or def.
func (c Config) OptionFloat(key string, def float64) float64 {
	switch v := c.Options[key].(type) {
	case json.Number:
		if n, err := v.Float64(); err == nil {
			return n
		}
	case float64:
		return v
	case int:
		return float64(v)
	case int64:
		return float64(v)
	}
	return def
}

// ConnInfo describes an open connection (plugin.db.connect result info).
type ConnInfo struct {
	// ServerVersion is at most 128 bytes (longer values are clipped).
	ServerVersion   string
	DefaultDatabase string
	// ReadOnly reports a connection that can never write.
	ReadOnly bool
}

// Conn is one open connection (a host handle). Its methods may be called
// concurrently (for example metadata while a result is being paged), so the
// implementation must be safe for concurrent use, like a database/sql pool.
type Conn interface {
	// Execute runs one statement. ctx carries the statement timeout and is
	// cancelled by plugin.db.cancel; it stays alive until the returned Rows
	// are closed, so drivers may bind streaming reads to it.
	Execute(ctx context.Context, req ExecuteRequest) (*Result, error)
	// Close releases the connection. It is called once, after every cursor
	// of the handle was closed.
	Close() error
}

// Mode is the execute mode chosen by the host.
type Mode string

// Execute modes.
const (
	ModeAuto  Mode = "auto"
	ModeQuery Mode = "query"
	ModeExec  Mode = "exec"
)

// ExecuteRequest is a plugin.db.execute call.
type ExecuteRequest struct {
	RequestID string
	Statement string
	// Database is the database to run in ("" for the connection default).
	Database string
	Mode     Mode
	// ReadOnly must be enforced (read-only transaction or engine setting)
	// when the driver declares the readOnly feature.
	ReadOnly bool
}

// Column is one result column.
type Column struct {
	Name string `json:"name"`
	Type string `json:"type,omitempty"`
}

// Rows is a forward-only result set. The Server pulls rows lazily, page by
// page, as the host fetches them, and always closes Rows.
type Rows interface {
	// Columns returns the result columns (at most 1024).
	Columns() []Column
	// Next returns the next row, one value per column, or io.EOF after the
	// last row. Values may be nil, bool, any integer or float type, string,
	// []byte (sent as a string), time.Time (RFC 3339 with nanoseconds),
	// json.Number, json.RawMessage, map[string]any, []any, a
	// database/sql/driver.Valuer, or anything encoding/json can marshal.
	// Integers beyond ±2^53 are sent as strings.
	Next(ctx context.Context) ([]any, error)
	Close() error
}

// Result is the outcome of Execute or TableRows.
type Result struct {
	// Rows is the result set, nil for statements without one.
	Rows Rows
	// AffectedRows, when set, is reported for statements without rows.
	AffectedRows *int64
	// Notice is an informational message (at most 1024 runes).
	Notice string
	// NextPageToken (TableRows only) lets the host continue after this page:
	// at most 1024 bytes of UTF-8 without control characters.
	NextPageToken string
}

// AffectedRows returns a pointer to n (for Result.AffectedRows).
func AffectedRows(n int64) *int64 { return &n }

// SliceRows returns Rows over an in-memory result.
func SliceRows(columns []Column, rows [][]any) Rows {
	return &sliceRows{columns: columns, rows: rows}
}

type sliceRows struct {
	columns []Column
	rows    [][]any
	next    int
}

func (r *sliceRows) Columns() []Column { return r.columns }

func (r *sliceRows) Next(context.Context) ([]any, error) {
	if r.next >= len(r.rows) {
		return nil, io.EOF
	}
	row := r.rows[r.next]
	r.next++
	return row, nil
}

func (r *sliceRows) Close() error { return nil }

// MetadataKind selects the plugin.db.metadata object kind.
type MetadataKind string

// Metadata kinds. Navigation, PrimaryKeys and DDL return a single Value;
// the other kinds return Items.
const (
	MetadataNavigation  MetadataKind = "navigation"
	MetadataTables      MetadataKind = "tables"
	MetadataColumns     MetadataKind = "columns"
	MetadataPrimaryKeys MetadataKind = "primaryKeys"
	MetadataIndexes     MetadataKind = "indexes"
	MetadataForeignKeys MetadataKind = "foreignKeys"
	MetadataTriggers    MetadataKind = "triggers"
	MetadataDDL         MetadataKind = "ddl"
)

// IsList reports whether the kind returns Items (else a single Value).
func (k MetadataKind) IsList() bool {
	switch k {
	case MetadataTables, MetadataColumns, MetadataIndexes, MetadataForeignKeys, MetadataTriggers:
		return true
	}
	return false
}

func (k MetadataKind) valid() bool {
	return k.IsList() || k == MetadataNavigation || k == MetadataPrimaryKeys || k == MetadataDDL
}

// MetadataRequest is a plugin.db.metadata call. Metadata calls are always
// read-only.
type MetadataRequest struct {
	Kind     MetadataKind
	Database string
	Table    string
}

// MetadataResult carries a Value (navigation: SchemaInfo, primaryKeys:
// PrimaryKeyInfo, ddl: string) or Items (tables: []SchemaTable, columns:
// []ColumnInfo, indexes: []IndexInfo, foreignKeys: []ForeignKeyInfo,
// triggers: []TriggerInfo). The Server pages Items within the page budget.
type MetadataResult struct {
	Value any
	Items []any
}

// Items converts a typed slice for MetadataResult.Items.
func Items[T any](list []T) []any {
	out := make([]any, len(list))
	for i := range list {
		out[i] = list[i]
	}
	return out
}

// MetadataProvider is implemented by a Conn that answers plugin.db.metadata
// (navigation is mandatory for every driver; other kinds follow the declared
// metadata.* features).
type MetadataProvider interface {
	Metadata(ctx context.Context, req MetadataRequest) (*MetadataResult, error)
}

// TableRowsRequest is a plugin.db.tableRows call (always read-only).
type TableRowsRequest struct {
	RequestID string
	Database  string
	Table     string
	Limit     int
	Offset    int
	PageToken string
}

// TableRowsProvider is implemented by a Conn with the tableRows feature.
type TableRowsProvider interface {
	TableRows(ctx context.Context, req TableRowsRequest) (*Result, error)
}

// MutationOp is a structured row edit.
type MutationOp string

// Row edit operations (features rows.update, rows.insert, rows.delete).
const (
	MutationUpdateCell MutationOp = "updateCell"
	MutationInsertRow  MutationOp = "insertRow"
	MutationDeleteRow  MutationOp = "deleteRow"
)

// Mutation is a plugin.db.mutate call. Values are decoded with json.Number.
type Mutation struct {
	RequestID  string
	Op         MutationOp
	Database   string
	Table      string
	PrimaryKey map[string]any
	Column     string
	Value      any
	Fields     map[string]any
}

// Mutator is implemented by a Conn with rows.* features. It returns the
// affected row count.
type Mutator interface {
	Mutate(ctx context.Context, m Mutation) (int64, error)
}

// DestructiveAction is a structured destructive operation.
type DestructiveAction string

// Destructive actions (features destructive.dropTable, destructive.clearTable,
// destructive.dropDatabase, destructive.clearDatabase). The database actions
// carry no Table; Database is the database (or schema) to drop or empty.
const (
	DestructiveDropTable     DestructiveAction = "dropTable"
	DestructiveClearTable    DestructiveAction = "clearTable"
	DestructiveDropDatabase  DestructiveAction = "dropDatabase"
	DestructiveClearDatabase DestructiveAction = "clearDatabase"
)

// DestructiveRequest is a plugin.db.destructive call; the host already
// confirmed it with the user.
type DestructiveRequest struct {
	RequestID string
	Action    DestructiveAction
	Database  string
	Table     string
}

// Destroyer is implemented by a Conn with destructive.* features.
type Destroyer interface {
	Destroy(ctx context.Context, req DestructiveRequest) error
}

// Statement is one SQL statement with positional arguments. Args are JSON
// scalars: nil, bool, string or json.Number (FromSQL passes integers as
// int64 and other numbers as their exact text).
type Statement struct {
	Text string
	Args []any
}

// SnapshotStep is one step of a snapshotWrite: read the rows the write will
// touch, apply the write, then optionally read the rows back.
type SnapshotStep struct {
	// Snapshot selects the rows before the write (nil: none).
	Snapshot *Statement
	// Write or Mutation (exactly one is set) is the change.
	Write    *Statement
	Mutation *Mutation
	// After re-reads the written rows (typically by primary key; nil: none).
	After *Statement
	// MaxRows bounds the Snapshot rows (1 to 1,000,000; 0 when Snapshot is
	// nil). A snapshot with more rows rolls the whole request back and
	// answers Exceeded.
	MaxRows int64
	// Returning names columns of the row an insertRow Mutation inserted
	// (for example generated keys) to return in SnapshotStepResult.Returned.
	Returning []string
}

// SnapshotRequest is a plugin.db.snapshotWrite call: all steps run in one
// transaction that commits only when every step succeeded.
type SnapshotRequest struct {
	RequestID string
	// Database is the database to run in ("" for the connection default).
	Database string
	Steps    []SnapshotStep
	// MaxBytes bounds the encoded size of all Before and After results and
	// Returned values together (see SnapshotResultSize and
	// SnapshotReturnedSize; a snapshot's probe row beyond MaxRows is not
	// counted). An implementation that would exceed it must roll back
	// before committing and return an *Error of KindLimit, so the host
	// retries in smaller batches; the Server checks the reply again but can
	// no longer undo a commit.
	MaxBytes int
}

// SnapshotStepResult is the outcome of one step. Before and After must be
// fully read before the transaction ends (use SliceRows); the Server reads
// them after SnapshotWrite returned. Their values and Returned are sent as
// SnapshotCell, so they must have a lossless encoding (EncodeSnapshotCell).
type SnapshotStepResult struct {
	Before   *Result
	Affected int64
	After    *Result
	// Returned holds the Returning column values (nil: none).
	Returned map[string]any
}

// SnapshotResponse answers a SnapshotRequest. Steps has one result per
// request step. When Exceeded is set the transaction was rolled back,
// ExceededStep is the 0-based step whose snapshot exceeded MaxRows and Steps
// is ignored.
type SnapshotResponse struct {
	Steps        []SnapshotStepResult
	Exceeded     bool
	ExceededStep int
}

// SnapshotWriter is implemented by a Conn with the transaction.snapshotWrite
// feature. It must run every step in one transaction and roll back on any
// error, on Exceeded and on a MaxBytes overflow.
type SnapshotWriter interface {
	SnapshotWrite(ctx context.Context, req SnapshotRequest) (*SnapshotResponse, error)
}

// Host metadata DTOs (the exact shapes of backend/db).

// SchemaInfo is the navigation tree (host DbSchemaInfo).
type SchemaInfo struct {
	Databases []SchemaDatabase `json:"databases"`
	Catalog   *string          `json:"catalog,omitempty"`
}

// SchemaDatabase is one database or schema of the navigation tree.
type SchemaDatabase struct {
	Name   string        `json:"name"`
	Tables []SchemaTable `json:"tables"`
}

// SchemaTable is one table or view.
type SchemaTable struct {
	Name      string  `json:"name"`
	Kind      string  `json:"kind"`
	RowCount  *int64  `json:"rowCount,omitempty"`
	SizeBytes *int64  `json:"sizeBytes,omitempty"`
	CreatedAt *string `json:"createdAt,omitempty"`
	UpdatedAt *string `json:"updatedAt,omitempty"`
	Comment   *string `json:"comment,omitempty"`
}

// ColumnInfo is one table column.
type ColumnInfo struct {
	Name          string  `json:"name"`
	DataType      string  `json:"dataType"`
	Nullable      bool    `json:"nullable"`
	IsPrimaryKey  bool    `json:"isPrimaryKey"`
	DefaultValue  *string `json:"defaultValue"`
	Comment       *string `json:"comment,omitempty"`
	AutoIncrement bool    `json:"autoIncrement"`
}

// PrimaryKeyInfo is a table's primary key.
type PrimaryKeyInfo struct {
	Columns        []string `json:"columns"`
	ConstraintName string   `json:"constraintName,omitempty"`
}

// IndexInfo is one index.
type IndexInfo struct {
	Name    string   `json:"name"`
	Columns []string `json:"columns"`
	Unique  bool     `json:"unique"`
}

// ForeignKeyInfo is one foreign key.
type ForeignKeyInfo struct {
	Name              string   `json:"name"`
	Columns           []string `json:"columns"`
	ReferencedTable   string   `json:"referencedTable"`
	ReferencedColumns []string `json:"referencedColumns"`
}

// TriggerInfo is one trigger.
type TriggerInfo struct {
	Name   string `json:"name"`
	Timing string `json:"timing"`
	Event  string `json:"event"`
}

func quote(s string) string { return strconv.Quote(s) }
