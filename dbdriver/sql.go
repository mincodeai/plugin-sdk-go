package dbdriver

import (
	"context"
	"database/sql"
	"encoding/json"
	"io"
	"strings"
	"sync"
)

// SQLOptions adapt a database/sql driver to Driver (see FromSQL). Every hook
// is optional.
type SQLOptions struct {
	// Info describes a new connection (server version, default database).
	Info func(ctx context.Context, db *sql.DB, cfg Config) (ConnInfo, error)
	// ReadOnlyTx runs read-only executes in a sql.TxOptions{ReadOnly: true}
	// transaction that is rolled back when the result closes.
	ReadOnlyTx bool
	// ReadOnlyContext is run on the pinned connection before a read-only
	// execute when ReadOnlyTx is not enough (for example "SET SESSION
	// TRANSACTION READ ONLY"); ReadWriteContext restores it afterwards.
	ReadOnlyContext  func(ctx context.Context, c *sql.Conn) error
	ReadWriteContext func(ctx context.Context, c *sql.Conn) error
	// UseDatabase switches the pinned connection to database (non-empty)
	// before an execute, for engines that select databases per session.
	UseDatabase func(ctx context.Context, c *sql.Conn, database string) error
	// IsQuery decides whether a ModeAuto statement returns rows. The default
	// looks at the first keyword (SELECT, SHOW, WITH, …).
	IsQuery func(statement string) bool
	// Metadata, TableRows, Mutate and Destroy implement the optional Conn
	// interfaces; a nil hook makes the method unsupported.
	Metadata  func(ctx context.Context, db *sql.DB, req MetadataRequest) (*MetadataResult, error)
	TableRows func(ctx context.Context, db *sql.DB, req TableRowsRequest) (statement string, args []any, err error)
	Mutate    func(ctx context.Context, db *sql.DB, m Mutation) (int64, error)
	Destroy   func(ctx context.Context, db *sql.DB, req DestructiveRequest) error
	// MutateIn applies a structured row edit on q (the snapshotWrite
	// transaction). Without it, snapshotWrite requests that contain a
	// mutation step fail with ErrUnsupported before anything runs. A driver
	// with both hooks can implement Mutate as MutateIn(ctx, db, m).
	MutateIn func(ctx context.Context, q Querier, m Mutation) (int64, error)
	// InsertReturning inserts m (an insertRow) on q and returns the affected
	// row count and the values of columns of the inserted row (for example
	// generated keys). It is used for insert mutation steps that ask for
	// returning columns; without it those steps run through MutateIn and
	// return no values.
	InsertReturning func(ctx context.Context, q Querier, m Mutation, columns []string) (int64, map[string]any, error)
	// NoTransactions marks an engine without transactions:
	// plugin.db.snapshotWrite then answers unsupported. Do not declare
	// transaction.snapshotWrite for such a driver.
	NoTransactions bool
}

// Querier runs statements; *sql.DB, *sql.Conn and *sql.Tx satisfy it.
type Querier interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

// FromSQL returns a Driver over database/sql. open builds the *sql.DB from
// the Config (reveal secrets only there); FromSQL pings it within the
// connect timeout. Each execute runs on a connection pinned until its result
// is closed, so cancellation and read-only settings stay per request.
func FromSQL(open func(ctx context.Context, cfg Config) (*sql.DB, error), o SQLOptions) Driver {
	if o.IsQuery == nil {
		o.IsQuery = IsQueryStatement
	}
	return DriverFunc(func(ctx context.Context, cfg Config) (Conn, ConnInfo, error) {
		db, err := open(ctx, cfg)
		if err != nil {
			return nil, ConnInfo{}, err
		}
		if err := db.PingContext(ctx); err != nil {
			_ = db.Close()
			return nil, ConnInfo{}, err
		}
		var info ConnInfo
		if o.Info != nil {
			if info, err = o.Info(ctx, db, cfg); err != nil {
				_ = db.Close()
				return nil, ConnInfo{}, err
			}
		}
		if info.DefaultDatabase == "" {
			info.DefaultDatabase = cfg.Database
		}
		return newSQLConn(db, o), info, nil
	})
}

// IsQueryStatement reports whether statement starts with a keyword that
// returns rows (SELECT, WITH, SHOW, DESCRIBE, DESC, EXPLAIN, VALUES, TABLE,
// PRAGMA), skipping leading comments and parentheses.
func IsQueryStatement(statement string) bool {
	s := statement
	for {
		s = strings.TrimLeft(s, " \t\r\n(")
		switch {
		case strings.HasPrefix(s, "--"):
			i := strings.IndexByte(s, '\n')
			if i < 0 {
				return false
			}
			s = s[i+1:]
		case strings.HasPrefix(s, "/*"):
			i := strings.Index(s, "*/")
			if i < 0 {
				return false
			}
			s = s[i+2:]
		default:
			end := strings.IndexFunc(s, func(r rune) bool {
				return !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z')
			})
			if end < 0 {
				end = len(s)
			}
			switch strings.ToUpper(s[:end]) {
			case "SELECT", "WITH", "SHOW", "DESCRIBE", "DESC", "EXPLAIN", "VALUES", "TABLE", "PRAGMA":
				return true
			}
			return false
		}
	}
}

type sqlConn struct {
	db *sql.DB
	o  SQLOptions
}

func newSQLConn(db *sql.DB, o SQLOptions) Conn {
	base := &sqlConn{db: db, o: o}
	// Expose only the optional interfaces whose hooks are set, so the
	// Server reports the others as unsupported.
	var c Conn = base
	if o.Metadata != nil {
		c = &sqlMeta{Conn: c, s: base}
	}
	if o.TableRows != nil {
		c = sqlWithTableRows(c, base)
	}
	if o.Mutate != nil || o.Destroy != nil || !o.NoTransactions {
		c = sqlWithWrites(c, base)
	}
	return c
}

func (c *sqlConn) Close() error { return c.db.Close() }

func (c *sqlConn) Execute(ctx context.Context, req ExecuteRequest) (*Result, error) {
	query := req.Mode == ModeQuery || req.Mode == ModeAuto && c.o.IsQuery(req.Statement)
	return c.run(ctx, req.Statement, nil, req.Database, req.ReadOnly, query)
}

// run executes one statement on a pinned connection. For a query the
// connection (and transaction) stays pinned until the Rows close.
func (c *sqlConn) run(ctx context.Context, statement string, args []any, database string, readOnly, query bool) (*Result, error) {
	conn, err := c.db.Conn(ctx)
	if err != nil {
		return nil, err
	}
	cleanup := []func(){}
	release := func() {
		for i := len(cleanup) - 1; i >= 0; i-- {
			cleanup[i]()
		}
	}
	cleanup = append(cleanup, func() { _ = conn.Close() })
	fail := func(err error) (*Result, error) { release(); return nil, err }
	if database != "" && c.o.UseDatabase != nil {
		if err := c.o.UseDatabase(ctx, conn, database); err != nil {
			return fail(err)
		}
	}
	if readOnly && c.o.ReadOnlyContext != nil {
		if err := c.o.ReadOnlyContext(ctx, conn); err != nil {
			return fail(err)
		}
		if c.o.ReadWriteContext != nil {
			cleanup = append(cleanup, func() { _ = c.o.ReadWriteContext(context.WithoutCancel(ctx), conn) })
		}
	}
	var ex Querier = conn
	if readOnly && c.o.ReadOnlyTx {
		tx, err := conn.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
		if err != nil {
			return fail(err)
		}
		cleanup = append(cleanup, func() { _ = tx.Rollback() })
		ex = tx
	}
	if !query {
		res, err := ex.ExecContext(ctx, statement, args...)
		if err != nil {
			return fail(err)
		}
		out := &Result{}
		if n, err := res.RowsAffected(); err == nil {
			out.AffectedRows = AffectedRows(n)
		}
		release()
		return out, nil
	}
	rows, err := ex.QueryContext(ctx, statement, args...)
	if err != nil {
		return fail(err)
	}
	types, err := rows.ColumnTypes()
	if err != nil {
		_ = rows.Close()
		return fail(err)
	}
	cols := make([]Column, len(types))
	for i, t := range types {
		cols[i] = Column{Name: t.Name(), Type: t.DatabaseTypeName()}
	}
	return &Result{Rows: &sqlRows{rows: rows, cols: cols, release: release}}, nil
}

type sqlRows struct {
	rows    *sql.Rows
	cols    []Column
	release func()
	once    sync.Once
}

func (r *sqlRows) Columns() []Column { return r.cols }

func (r *sqlRows) Next(context.Context) ([]any, error) { return scanRow(r.rows, len(r.cols), false) }

// scanRow reads the next row of n columns, or io.EOF. []byte values are
// copied, as strings unless keepBytes is set.
func scanRow(rows *sql.Rows, n int, keepBytes bool) ([]any, error) {
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return nil, err
		}
		return nil, io.EOF
	}
	vals := make([]any, n)
	ptrs := make([]any, n)
	for i := range vals {
		ptrs[i] = &vals[i]
	}
	if err := rows.Scan(ptrs...); err != nil {
		return nil, err
	}
	for i, v := range vals {
		// Scan into *any reuses driver buffers only for sql.RawBytes; copy
		// []byte anyway so a value never aliases the next row.
		if b, ok := v.([]byte); ok {
			if keepBytes {
				vals[i] = append([]byte{}, b...)
			} else {
				vals[i] = string(b)
			}
		}
	}
	return vals, nil
}

func (r *sqlRows) Close() error {
	var err error
	r.once.Do(func() {
		err = r.rows.Close()
		r.release()
	})
	return err
}

type sqlMeta struct {
	Conn
	s *sqlConn
}

func (m *sqlMeta) Metadata(ctx context.Context, req MetadataRequest) (*MetadataResult, error) {
	return m.s.o.Metadata(ctx, m.s.db, req)
}

// sqlTableRowsConn and sqlWritesConn add TableRows / Mutate+Destroy to the
// wrapped Conn while keeping the interfaces already present.

func sqlWithTableRows(c Conn, s *sqlConn) Conn {
	tr := func(ctx context.Context, req TableRowsRequest) (*Result, error) {
		statement, args, err := s.o.TableRows(ctx, s.db, req)
		if err != nil {
			return nil, err
		}
		return s.run(ctx, statement, args, "", true, true)
	}
	if mp, ok := c.(MetadataProvider); ok {
		return &sqlMetaTableRows{Conn: c, MetadataProvider: mp, tr: tr}
	}
	return &sqlTableRows{Conn: c, tr: tr}
}

type sqlTableRows struct {
	Conn
	tr func(context.Context, TableRowsRequest) (*Result, error)
}

func (c *sqlTableRows) TableRows(ctx context.Context, req TableRowsRequest) (*Result, error) {
	return c.tr(ctx, req)
}

type sqlMetaTableRows struct {
	Conn
	MetadataProvider
	tr func(context.Context, TableRowsRequest) (*Result, error)
}

func (c *sqlMetaTableRows) TableRows(ctx context.Context, req TableRowsRequest) (*Result, error) {
	return c.tr(ctx, req)
}

func sqlWithWrites(c Conn, s *sqlConn) Conn {
	return &sqlWrites{Conn: c, s: s, inner: c}
}

// sqlWrites implements Mutator, Destroyer and SnapshotWriter and forwards
// the read interfaces of inner (reporting them unsupported when inner lacks
// them).
type sqlWrites struct {
	Conn
	s     *sqlConn
	inner Conn
}

func (c *sqlWrites) Mutate(ctx context.Context, m Mutation) (int64, error) {
	if c.s.o.Mutate == nil {
		return 0, ErrUnsupported
	}
	return c.s.o.Mutate(ctx, c.s.db, m)
}

func (c *sqlWrites) Destroy(ctx context.Context, req DestructiveRequest) error {
	if c.s.o.Destroy == nil {
		return ErrUnsupported
	}
	return c.s.o.Destroy(ctx, c.s.db, req)
}

func (c *sqlWrites) Metadata(ctx context.Context, req MetadataRequest) (*MetadataResult, error) {
	if mp, ok := c.inner.(MetadataProvider); ok {
		return mp.Metadata(ctx, req)
	}
	return nil, ErrUnsupported
}

func (c *sqlWrites) TableRows(ctx context.Context, req TableRowsRequest) (*Result, error) {
	if tp, ok := c.inner.(TableRowsProvider); ok {
		return tp.TableRows(ctx, req)
	}
	return nil, ErrUnsupported
}

// SnapshotWrite runs the steps in one transaction on a pinned connection
// (after UseDatabase) and commits only when all of them succeeded within
// MaxRows and MaxBytes.
func (c *sqlWrites) SnapshotWrite(ctx context.Context, req SnapshotRequest) (*SnapshotResponse, error) {
	o := c.s.o
	if o.NoTransactions {
		return nil, ErrUnsupported
	}
	for _, st := range req.Steps {
		if st.Mutation != nil && o.MutateIn == nil {
			return nil, ErrUnsupported
		}
	}
	conn, err := c.s.db.Conn(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	if req.Database != "" && o.UseDatabase != nil {
		if err := o.UseDatabase(ctx, conn, req.Database); err != nil {
			return nil, err
		}
	}
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()
	budget := req.MaxBytes
	out := &SnapshotResponse{Steps: make([]SnapshotStepResult, len(req.Steps))}
	for i, st := range req.Steps {
		res := &out.Steps[i]
		if st.Snapshot != nil {
			cols, rows, exceeded, err := collectRows(ctx, tx, st.Snapshot, st.MaxRows, &budget)
			if err != nil {
				return nil, err
			}
			if exceeded {
				return &SnapshotResponse{Exceeded: true, ExceededStep: i}, nil
			}
			res.Before = &Result{Rows: SliceRows(cols, rows)}
		}
		switch {
		case st.Write != nil:
			r, err := tx.ExecContext(ctx, st.Write.Text, sqlArgs(st.Write.Args)...)
			if err != nil {
				return nil, err
			}
			if res.Affected, err = r.RowsAffected(); err != nil {
				res.Affected = -1
			}
		case st.Mutation.Op == MutationInsertRow && len(st.Returning) > 0 && o.InsertReturning != nil:
			if res.Affected, res.Returned, err = o.InsertReturning(ctx, tx, *st.Mutation, st.Returning); err != nil {
				return nil, err
			}
			size, err := SnapshotReturnedSize(res.Returned)
			if err != nil {
				return nil, err
			}
			if budget -= size; budget < 0 {
				return nil, errSnapshotLimit
			}
		default:
			if res.Affected, err = o.MutateIn(ctx, tx, *st.Mutation); err != nil {
				return nil, err
			}
		}
		if st.After != nil {
			cols, rows, _, err := collectRows(ctx, tx, st.After, 0, &budget)
			if err != nil {
				return nil, err
			}
			res.After = &Result{Rows: SliceRows(cols, rows)}
		}
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	committed = true
	return out, nil
}

// collectRows reads the result of st into memory, charging its encoded
// size to budget. With maxRows > 0 it reads at most maxRows rows and then
// probes one more: exceeded reports that it exists (the probe row is never
// charged, so an over-long snapshot answers exceeded rather than a limit).
func collectRows(ctx context.Context, q Querier, st *Statement, maxRows int64, budget *int) (cols []Column, out [][]any, exceeded bool, err error) {
	rows, err := q.QueryContext(ctx, st.Text, sqlArgs(st.Args)...)
	if err != nil {
		return nil, nil, false, err
	}
	defer rows.Close()
	types, err := rows.ColumnTypes()
	if err != nil {
		return nil, nil, false, err
	}
	cols = make([]Column, len(types))
	for i, t := range types {
		cols[i] = Column{Name: t.Name(), Type: t.DatabaseTypeName()}
	}
	size, _ := SnapshotResultSize(cols, nil)
	for {
		row, err := scanRow(rows, len(cols), true)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, nil, false, err
		}
		if maxRows > 0 && int64(len(out)) == maxRows {
			return nil, nil, true, nil
		}
		enc, err := appendSnapshotRow(nil, 1, row)
		if err != nil {
			return nil, nil, false, err
		}
		if size += len(enc); size > *budget {
			return nil, nil, false, errSnapshotLimit
		}
		out = append(out, row)
	}
	*budget -= size
	return cols, out, false, nil
}

// sqlArgs converts protocol arguments for database/sql: integral numbers
// become int64, other numbers keep their exact text.
func sqlArgs(args []any) []any {
	out := make([]any, len(args))
	for i, a := range args {
		if n, ok := a.(json.Number); ok {
			if v, err := n.Int64(); err == nil {
				out[i] = v
			} else {
				out[i] = string(n)
			}
			continue
		}
		out[i] = a
	}
	return out
}
