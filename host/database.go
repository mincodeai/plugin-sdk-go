package host

import (
	"context"
	"encoding/json"
	"errors"
	"time"
)

// Read-only schema access (database.schema capability). Plugins see saved SQL
// connections by id, name and type only; the user authorizes each connection
// the first time it is read. Credentials are never returned and no SQL from
// the plugin is executed.

// databaseSchemaTimeout is the host's limit for reading the schema itself.
const databaseSchemaTimeout = 2 * time.Minute

// DatabaseDescribeMaxTables is the most tables one DescribeTables call accepts.
const DatabaseDescribeMaxTables = 100

// DatabaseConnection is a saved connection. Granted reports whether the user
// chose "always allow" for this plugin.
type DatabaseConnection struct {
	ID       string `json:"id"`
	Name     string `json:"name"`
	Kind     string `json:"kind"`
	Database string `json:"database,omitempty"`
	Granted  bool   `json:"granted"`
}

// DatabaseTableSummary is one table or view of a database.
type DatabaseTableSummary struct {
	Name    string `json:"name"`
	Kind    string `json:"kind,omitempty"`
	Comment string `json:"comment,omitempty"`
}

// DatabaseTableListing lists the databases of a connection and the tables of
// the selected one.
type DatabaseTableListing struct {
	Databases []string               `json:"databases"`
	Database  string                 `json:"database"`
	Tables    []DatabaseTableSummary `json:"tables"`
}

// DatabaseColumn describes a column.
type DatabaseColumn struct {
	Name          string  `json:"name"`
	DataType      string  `json:"dataType"`
	Nullable      bool    `json:"nullable"`
	PrimaryKey    bool    `json:"primaryKey"`
	AutoIncrement bool    `json:"autoIncrement"`
	DefaultValue  *string `json:"defaultValue,omitempty"`
	Comment       string  `json:"comment,omitempty"`
}

// DatabaseIndex describes an index.
type DatabaseIndex struct {
	Name    string   `json:"name"`
	Columns []string `json:"columns"`
	Unique  bool     `json:"unique"`
}

// DatabaseForeignKey describes a foreign key.
type DatabaseForeignKey struct {
	Name              string   `json:"name"`
	Columns           []string `json:"columns"`
	ReferencedTable   string   `json:"referencedTable"`
	ReferencedColumns []string `json:"referencedColumns"`
}

// DatabaseTable is the structure of one table. Error is set instead when that
// table could not be read.
type DatabaseTable struct {
	Name        string               `json:"name"`
	Comment     string               `json:"comment,omitempty"`
	Columns     []DatabaseColumn     `json:"columns"`
	PrimaryKey  []string             `json:"primaryKey,omitempty"`
	Indexes     []DatabaseIndex      `json:"indexes,omitempty"`
	ForeignKeys []DatabaseForeignKey `json:"foreignKeys,omitempty"`
	Error       string               `json:"error,omitempty"`
}

// ListDatabaseConnections calls host.database.connections. It never prompts.
func ListDatabaseConnections(ctx context.Context, c Caller) ([]DatabaseConnection, error) {
	if c == nil {
		return nil, ErrUnavailable
	}
	raw, err := c.Call(ctx, "host.database.connections", map[string]any{})
	if err != nil {
		return nil, err
	}
	var result struct {
		Connections []DatabaseConnection `json:"connections"`
	}
	if err := json.Unmarshal(raw, &result); err != nil {
		return nil, errors.New("host returned an invalid connection list")
	}
	return result.Connections, nil
}

// DatabaseTablesRequest selects a connection and optionally a database (empty:
// the connection's default). TimeoutMs bounds the user's approval (0: 25 s;
// 5000-300000).
type DatabaseTablesRequest struct {
	ConnectionID string
	Database     string
	TimeoutMs    int
}

// ListDatabaseTables calls host.database.tables. A declined approval returns
// a host error with code -32003.
func ListDatabaseTables(ctx context.Context, c Caller, request DatabaseTablesRequest) (*DatabaseTableListing, error) {
	if c == nil {
		return nil, ErrUnavailable
	}
	if request.ConnectionID == "" {
		return nil, errors.New("a connection id is required")
	}
	params := map[string]any{"connectionId": request.ConnectionID}
	if request.Database != "" {
		params["database"] = request.Database
	}
	if request.TimeoutMs > 0 {
		params["timeoutMs"] = request.TimeoutMs
	}
	raw, err := c.Call(WithCallTimeout(ctx, databaseWait(request.TimeoutMs)), "host.database.tables", params)
	if err != nil {
		return nil, err
	}
	var listing DatabaseTableListing
	if err := json.Unmarshal(raw, &listing); err != nil {
		return nil, errors.New("host returned an invalid table list")
	}
	return &listing, nil
}

// DatabaseDescribeRequest names up to DatabaseDescribeMaxTables tables.
type DatabaseDescribeRequest struct {
	ConnectionID string
	Database     string
	Tables       []string
	TimeoutMs    int
}

// DescribeDatabaseTables calls host.database.describe.
func DescribeDatabaseTables(ctx context.Context, c Caller, request DatabaseDescribeRequest) ([]DatabaseTable, error) {
	if c == nil {
		return nil, ErrUnavailable
	}
	if request.ConnectionID == "" || request.Database == "" || len(request.Tables) == 0 || len(request.Tables) > DatabaseDescribeMaxTables {
		return nil, errors.New("a connection id, a database and 1-100 tables are required")
	}
	params := map[string]any{"connectionId": request.ConnectionID, "database": request.Database, "tables": request.Tables}
	if request.TimeoutMs > 0 {
		params["timeoutMs"] = request.TimeoutMs
	}
	raw, err := c.Call(WithCallTimeout(ctx, databaseWait(request.TimeoutMs)), "host.database.describe", params)
	if err != nil {
		return nil, err
	}
	var result struct {
		Tables []DatabaseTable `json:"tables"`
	}
	if err := json.Unmarshal(raw, &result); err != nil {
		return nil, errors.New("host returned invalid table structure")
	}
	return result.Tables, nil
}

func databaseWait(timeoutMs int) time.Duration {
	return promptWait(timeoutMs) + databaseSchemaTimeout + replyMargin
}
