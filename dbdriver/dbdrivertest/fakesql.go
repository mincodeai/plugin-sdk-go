package dbdrivertest

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"sync"
	"time"
)

// FakeResult scripts the outcome of one statement in FakeSQL.
type FakeResult struct {
	Columns []string
	Types   []string // DatabaseTypeName per column (optional)
	Rows    [][]driver.Value
	// Affected is the RowsAffected of an Exec.
	Affected int64
	// Err fails the statement.
	Err error
	// Delay waits before answering; Block waits until the context ends and
	// returns its error.
	Delay time.Duration
	Block bool
}

// FakeSQL is an in-memory database/sql driver: statements are answered
// from Script, everything run is recorded. Use DB (or Open for FromSQL).
type FakeSQL struct {
	mu sync.Mutex
	// Script maps a statement to its result; unknown statements fail with
	// "syntax error".
	Script map[string]FakeResult
	// PingErr fails connecting.
	PingErr error
	// Statements records every Query/Exec, prefixed "ro:" inside a read-only
	// transaction.
	Statements []string
	// Args holds the arguments of each recorded statement (nil without).
	Args [][]driver.Value
	// ReadOnlyTx counts BeginTx calls with ReadOnly set.
	ReadOnlyTx int
	// Transactions, Commits and Rollbacks count read-write transactions.
	Transactions, Commits, Rollbacks int
	// Open counts open connections.
	Open int
}

// DB returns a *sql.DB over f.
func (f *FakeSQL) DB() *sql.DB { return sql.OpenDB(fakeConnector{f}) }

// Recorded returns the recorded statements.
func (f *FakeSQL) Recorded() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.Statements...)
}

// RecordedArgs returns the arguments of each recorded statement.
func (f *FakeSQL) RecordedArgs() [][]driver.Value {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([][]driver.Value(nil), f.Args...)
}

// TxCounts returns how many read-write transactions began, committed and
// rolled back.
func (f *FakeSQL) TxCounts() (begun, committed, rolledBack int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.Transactions, f.Commits, f.Rollbacks
}

// OpenConns returns the number of open connections.
func (f *FakeSQL) OpenConns() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.Open
}

type fakeConnector struct{ f *FakeSQL }

func (c fakeConnector) Connect(context.Context) (driver.Conn, error) {
	c.f.mu.Lock()
	c.f.Open++
	c.f.mu.Unlock()
	return &fakeConn{f: c.f}, nil
}

func (c fakeConnector) Driver() driver.Driver { return fakeDriver{} }

type fakeDriver struct{}

func (fakeDriver) Open(string) (driver.Conn, error) { return nil, errors.New("use OpenDB") }

type fakeConn struct {
	f      *FakeSQL
	inTxRO bool
	inTxRW bool
	closed bool
}

func (c *fakeConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("prepare not supported")
}

func (c *fakeConn) Close() error {
	if !c.closed {
		c.closed = true
		c.f.mu.Lock()
		c.f.Open--
		c.f.mu.Unlock()
	}
	return nil
}

func (c *fakeConn) Begin() (driver.Tx, error) {
	return c.BeginTx(context.Background(), driver.TxOptions{})
}

func (c *fakeConn) BeginTx(_ context.Context, o driver.TxOptions) (driver.Tx, error) {
	if o.ReadOnly {
		c.f.mu.Lock()
		c.f.ReadOnlyTx++
		c.f.mu.Unlock()
		c.inTxRO = true
	} else {
		c.f.mu.Lock()
		c.f.Transactions++
		c.f.mu.Unlock()
		c.inTxRW = true
	}
	return fakeTx{c}, nil
}

func (c *fakeConn) Ping(context.Context) error { return c.f.PingErr }

func (c *fakeConn) ResetSession(context.Context) error { return nil }

type fakeTx struct{ c *fakeConn }

func (t fakeTx) Commit() error   { t.end(func(f *FakeSQL) { f.Commits++ }); return nil }
func (t fakeTx) Rollback() error { t.end(func(f *FakeSQL) { f.Rollbacks++ }); return nil }

func (t fakeTx) end(count func(*FakeSQL)) {
	if t.c.inTxRW {
		t.c.f.mu.Lock()
		count(t.c.f)
		t.c.f.mu.Unlock()
	}
	t.c.inTxRO, t.c.inTxRW = false, false
}

func (c *fakeConn) lookup(ctx context.Context, query string, args []driver.NamedValue) (FakeResult, error) {
	c.f.mu.Lock()
	rec := query
	if c.inTxRO {
		rec = "ro:" + query
	}
	c.f.Statements = append(c.f.Statements, rec)
	var vals []driver.Value
	for _, a := range args {
		vals = append(vals, a.Value)
	}
	c.f.Args = append(c.f.Args, vals)
	r, ok := c.f.Script[query]
	c.f.mu.Unlock()
	if !ok {
		return FakeResult{}, errors.New("syntax error")
	}
	if r.Block {
		<-ctx.Done()
		return FakeResult{}, ctx.Err()
	}
	if r.Delay > 0 {
		t := time.NewTimer(r.Delay)
		defer t.Stop()
		select {
		case <-t.C:
		case <-ctx.Done():
			return FakeResult{}, ctx.Err()
		}
	}
	return r, r.Err
}

func (c *fakeConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	r, err := c.lookup(ctx, query, args)
	if err != nil {
		return nil, err
	}
	return &fakeRows{r: r}, nil
}

func (c *fakeConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	r, err := c.lookup(ctx, query, args)
	if err != nil {
		return nil, err
	}
	return driver.RowsAffected(r.Affected), nil
}

type fakeRows struct {
	r    FakeResult
	next int
}

func (r *fakeRows) Columns() []string { return r.r.Columns }
func (r *fakeRows) Close() error      { return nil }

func (r *fakeRows) ColumnTypeDatabaseTypeName(i int) string {
	if i < len(r.r.Types) {
		return r.r.Types[i]
	}
	return ""
}

func (r *fakeRows) Next(dest []driver.Value) error {
	if r.next >= len(r.r.Rows) {
		return io.EOF
	}
	copy(dest, r.r.Rows[r.next])
	r.next++
	return nil
}
