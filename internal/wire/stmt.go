package wire

import (
	"context"
	"database/sql/driver"
	"errors"
)

// ErrNoTx is returned by Begin of both drivers.
var ErrNoTx = errors.New("chdb-sql: transactions are not supported")

// Result reports nothing: neither chDB nor the HTTP driver returns an affected-row count for INSERT.
type Result struct{}

func (Result) LastInsertId() (int64, error) {
	return 0, errors.New("chdb-sql: LastInsertId is not supported")
}

func (Result) RowsAffected() (int64, error) {
	return 0, errors.New("chdb-sql: RowsAffected is not available")
}

// Runner is a connection that can run a statement; Stmt hands its query text to it.
type Runner interface {
	ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error)
	QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error)
}

// Stmt keeps a query text for Prepare. There are no server-side prepared statements.
type Stmt struct {
	Conn Runner
	Text string
}

func (s *Stmt) Close() error  { return nil }
func (s *Stmt) NumInput() int { return -1 }

func (s *Stmt) Exec([]driver.Value) (driver.Result, error) {
	return nil, errors.New("chdb-sql: use ExecContext")
}

func (s *Stmt) Query([]driver.Value) (driver.Rows, error) {
	return nil, errors.New("chdb-sql: use QueryContext")
}

func (s *Stmt) CheckNamedValue(nv *driver.NamedValue) error { return CheckNamedValue(nv) }

func (s *Stmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	return s.Conn.ExecContext(ctx, s.Text, args)
}

func (s *Stmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	return s.Conn.QueryContext(ctx, s.Text, args)
}
