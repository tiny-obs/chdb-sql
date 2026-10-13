// Package chdbsql is a database/sql driver for an embedded chDB (libchdb.so) that gives every pooled
// connection its own chDB connection, so a writer and several readers run in parallel inside one process.
//
// It is built on github.com/chdb-io/chdb-go/chdb-purego and needs no Parquet or Arrow in libchdb: results are
// read as JSONCompactEachRowWithNamesAndTypes. The stock chdb-go database/sql driver cannot be used for that:
// it fixes the format to Parquet and funnels every query through one process-wide session.
//
// DSN: the chdb-purego connection string, "file:/data/dir?mode=ro&max_memory_usage=1000000000" or ":memory:".
// Every "key=value" becomes a "--key=value" argument of the embedded server; "mode=ro" becomes "--readonly=1".
// Values are not percent-decoded, so pass plain values.
//
// One embedded server exists per process and every connection must use the same path. The first connection
// starts the server: put server-wide settings (max_server_memory_usage, memory_worker_*) in its DSN. Use two
// *sql.DB handles on one path, as the gateway does today:
//
//	w := sql.OpenDB(chdbsql.NewConnector("file:/data/chdb?max_server_memory_usage=..."))
//	w.SetMaxOpenConns(1) // open and ping it before the readers
//	r := sql.OpenDB(chdbsql.NewConnector("file:/data/chdb?mode=ro&max_execution_time=10"))
//	r.SetMaxOpenConns(4)
//
// A read-only connection cannot change settings per query, so every setting a query needs goes in its DSN.
package chdbsql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"sync/atomic"

	chdbpurego "github.com/chdb-io/chdb-go/chdb-purego"
)

// DriverName is the name registered with database/sql.
const DriverName = "chdbsql"

func init() { sql.Register(DriverName, &Driver{}) }

// Driver implements driver.Driver and driver.DriverContext.
type Driver struct{}

// Open implements driver.Driver.
func (Driver) Open(dsn string) (driver.Conn, error) {
	return NewConnector(dsn).Connect(context.Background())
}

// OpenConnector implements driver.DriverContext.
func (Driver) OpenConnector(dsn string) (driver.Connector, error) { return NewConnector(dsn), nil }

// Connector opens one chDB connection per Connect call.
type Connector struct{ dsn string }

// NewConnector returns a driver.Connector for dsn. Pass it to sql.OpenDB.
func NewConnector(dsn string) *Connector { return &Connector{dsn: dsn} }

// Connect implements driver.Connector.
func (c *Connector) Connect(ctx context.Context) (driver.Conn, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	cc, err := chdbpurego.NewConnectionFromConnString(c.dsn)
	if err != nil {
		return nil, err
	}
	return &conn{c: cc}, nil
}

// Driver implements driver.Connector.
func (c *Connector) Driver() driver.Driver { return Driver{} }

var errNoTx = errors.New("chdbsql: transactions are not supported")

// conn is one chDB connection. database/sql never uses a conn from two goroutines at once.
type conn struct {
	c chdbpurego.ChdbConn
	// abandoned is closed when a statement left running by a cancelled context ends; nil if there is none.
	// It is written by the goroutine that owns the conn, before that call returns, and read by Close after.
	abandoned chan struct{}
	// broken is set when a call was abandoned; database/sql then discards the conn (driver.Validator).
	broken atomic.Bool
}

var (
	_ driver.Conn                           = (*conn)(nil)
	_ driver.ConnBeginTx                    = (*conn)(nil)
	_ driver.ExecerContext                  = (*conn)(nil)
	_ driver.QueryerContext                 = (*conn)(nil)
	_ driver.Pinger                         = (*conn)(nil)
	_ driver.Validator                      = (*conn)(nil)
	_ driver.NamedValueChecker              = (*conn)(nil)
	_ driver.StmtExecContext                = (*stmt)(nil)
	_ driver.StmtQueryContext               = (*stmt)(nil)
	_ driver.NamedValueChecker              = (*stmt)(nil)
	_ driver.RowsColumnTypeScanType         = (*rows)(nil)
	_ driver.RowsColumnTypeDatabaseTypeName = (*rows)(nil)
	_ driver.RowsColumnTypeNullable         = (*rows)(nil)
)

func (cn *conn) Prepare(query string) (driver.Stmt, error) { return &stmt{cn: cn, query: query}, nil }

// Close waits for abandoned calls, then closes the chDB connection.
func (cn *conn) Close() error {
	if finished := cn.abandoned; finished != nil {
		go func() {
			<-finished
			cn.c.Close()
		}()
		return nil
	}
	cn.c.Close()
	return nil
}

func (cn *conn) Begin() (driver.Tx, error) { return nil, errNoTx }

func (cn *conn) BeginTx(context.Context, driver.TxOptions) (driver.Tx, error) { return nil, errNoTx }

// IsValid implements driver.Validator.
func (cn *conn) IsValid() bool { return !cn.broken.Load() && cn.c.Ready() }

func (cn *conn) Ping(ctx context.Context) error {
	_, err := cn.exec(ctx, "SELECT 1")
	return err
}

func (cn *conn) CheckNamedValue(nv *driver.NamedValue) error { return checkNamedValue(nv) }

func (cn *conn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	q, err := bind(query, args)
	if err != nil {
		return nil, err
	}
	return cn.exec(ctx, q)
}

func (cn *conn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	q, err := bind(query, args)
	if err != nil {
		return nil, err
	}
	return cn.query(ctx, q)
}

// exec runs a statement and discards any output. libchdb cannot cancel a running statement through this call,
// so when ctx ends first the call is abandoned: it keeps running, the conn is marked broken and discarded.
// Set max_execution_time in the DSN to bound statements.
func (cn *conn) exec(ctx context.Context, q string) (driver.Result, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	type out struct {
		r   chdbpurego.ChdbResult
		err error
	}
	run := func() out {
		r, err := cn.c.Query(q, "Null")
		return out{r, err}
	}
	var o out
	if ctx.Done() == nil {
		o = run()
	} else {
		ch := make(chan out, 1)
		go func() { ch <- run() }()
		select {
		case o = <-ch:
		case <-ctx.Done():
			// The call keeps running inside libchdb. Close closes the chDB connection only after it ends, so the
			// connection is never closed under a running statement and Close never blocks database/sql.
			finished := make(chan struct{})
			cn.abandoned = finished
			cn.broken.Store(true)
			go func() {
				defer close(finished)
				if o := <-ch; o.r != nil {
					o.r.Free()
				}
			}()
			return nil, ctx.Err()
		}
	}
	if o.r != nil {
		defer o.r.Free()
		if err := o.r.Error(); err != nil && o.err == nil {
			o.err = err
		}
	}
	if o.err != nil {
		return nil, o.err
	}
	return result{}, nil
}

// result reports nothing: chDB does not return an affected-row count for INSERT.
type result struct{}

func (result) LastInsertId() (int64, error) {
	return 0, errors.New("chdbsql: LastInsertId is not supported")
}

func (result) RowsAffected() (int64, error) {
	return 0, errors.New("chdbsql: RowsAffected is not available")
}

type stmt struct {
	cn    *conn
	query string
}

func (s *stmt) Close() error  { return nil }
func (s *stmt) NumInput() int { return -1 }

func (s *stmt) Exec([]driver.Value) (driver.Result, error) {
	return nil, errors.New("chdbsql: use ExecContext")
}

func (s *stmt) Query([]driver.Value) (driver.Rows, error) {
	return nil, errors.New("chdbsql: use QueryContext")
}

func (s *stmt) CheckNamedValue(nv *driver.NamedValue) error { return checkNamedValue(nv) }

func (s *stmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	return s.cn.ExecContext(ctx, s.query, args)
}

func (s *stmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	return s.cn.QueryContext(ctx, s.query, args)
}
