// Package chconnect is a database/sql driver for chdb-server's ConnectRPC service (chdb.v1.Chdb).
//
// It differs from package chhttp in three ways. Reads and writes use the typed Query, Exec and Insert RPCs, so errors
// are connect errors with the ClickHouse exception code attached (*ServerError). Inserts of rows in a text format are
// sent through the Insert RPC, which the server merges with other inserts into one MergeTree part, see
// Connector.Insert. And the HTTP client is tuned for large results, see newTransport.
//
// Importing this package registers only the driver "chdbsql-connect"; it needs neither chdb-go nor libchdb.so.
package chconnect

import (
	"bufio"
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"reflect"
	"strconv"
	"strings"
	"time"

	"connectrpc.com/connect"

	chdbv1 "github.com/tiny-obs/chdb-sql/internal/gen/chdb/v1"
	"github.com/tiny-obs/chdb-sql/internal/gen/chdb/v1/chdbv1connect"

	"github.com/tiny-obs/chdb-sql/internal/wire"
)

// DriverName is the name registered with database/sql for the ConnectRPC driver.
const DriverName = "chdbsql-connect"

func init() { sql.Register(DriverName, &Driver{}) }

const (
	// exceptionCodeKey is the error metadata key under which the server puts the ClickHouse exception code.
	exceptionCodeKey = "X-ClickHouse-Exception-Code"

	// insertChunk is the size of the data messages Connector.Insert sends.
	insertChunk = 1 << 20
	// peekGrace bounds how long Close waits to learn whether a result was read to its last row.
	peekGrace = 50 * time.Millisecond
)

// Driver implements driver.Driver and driver.DriverContext for chdb-server's ConnectRPC service.
//
// DSN: "http://[user:key@]host:8123/database?max_execution_time=10&readonly=1", or https. The path is the default
// database, sent in the database field of every request. Every query parameter is a ClickHouse setting, sent in the
// settings map of every request. The password is the server's auth key (CHDB_AUTH_KEY), sent as
// "Authorization: Bearer <key>" when it is present; the user name is accepted and ignored.
//
// Results are read as JSONCompactEachRowWithNamesAndTypes and decoded exactly like the embedded and HTTP drivers do:
// arrays, maps and tuples arrive as JSON text, 64-bit integers keep every digit, DateTime values come in the column's
// time zone. "?" arguments are rendered into the text as literals. There are no transactions and no RowsAffected.
//
// ExecContext runs the Exec RPC, except for "INSERT INTO [db.]table [(columns)] FORMAT <format>" followed by data and no
// "?" arguments: that goes through the Insert RPC, see Connector.Insert.
//
// Every error from the server is a *ServerError, which wraps the *connect.Error. A statement that fails after the
// result started (an error in the middle of a large result) is reported by Rows.Next and Rows.Err, never as a short
// result. A cancelled or expired context is returned as the context's error.
type Driver struct{}

// Open implements driver.Driver.
func (Driver) Open(dsn string) (driver.Conn, error) {
	c, err := NewConnector(dsn)
	if err != nil {
		return nil, err
	}
	return c.Connect(context.Background())
}

// OpenConnector implements driver.DriverContext.
func (Driver) OpenConnector(dsn string) (driver.Connector, error) { return NewConnector(dsn) }

// Connector hands out stateless connections that share one HTTP client and its HTTP/2 connections. Pass it to
// sql.OpenDB; closing the DB closes the idle HTTP connections.
type Connector struct {
	baseURL    string
	authHeader string // "Bearer <key>", or empty when the DSN has no password
	database   string
	settings   map[string]string // read-only after NewConnector
	transport  *http.Transport
	httpc      *http.Client
	rpc        chdbv1connect.ChdbClient
}

// NewConnector parses dsn, see Driver. It does not connect.
func NewConnector(dsn string) (*Connector, error) {
	u, err := url.Parse(dsn)
	if err != nil {
		return nil, fmt.Errorf("chdbsql: bad DSN: %w", err)
	}
	if (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return nil, errors.New("chdbsql: the connect DSN must look like http://host:8123/database?setting=value")
	}
	c := &Connector{baseURL: u.Scheme + "://" + u.Host}
	if u.User != nil {
		if key, ok := u.User.Password(); ok && key != "" {
			c.authHeader = "Bearer " + key
		}
	}
	params := u.Query()
	c.database = strings.Trim(u.Path, "/")
	if db := params.Get("database"); db != "" {
		if c.database == "" {
			c.database = db
		}
		params.Del("database") // a database goes in its own field, not in the settings
	}
	if len(params) > 0 {
		c.settings = make(map[string]string, len(params))
		for k := range params {
			c.settings[k] = params.Get(k)
		}
	}
	c.transport = newTransport(u.Scheme == "https")
	// There is no client timeout: a result streams for as long as it takes, and the context of the call bounds it.
	c.httpc = &http.Client{Transport: c.transport}
	// The server does not compress responses (a compressed stream was 15 to 25 times slower) and the data is already
	// a dense text or binary format. Registering "gzip" with nil codecs removes it from Accept-Encoding, and without
	// WithSendCompression the client never gzips a request.
	c.rpc = chdbv1connect.NewChdbClient(c.httpc, c.baseURL, connect.WithAcceptCompression("gzip", nil, nil))
	return c, nil
}

// newTransport returns the tuned HTTP transport: HTTP/2 only, with bigger frames and flow-control windows than Go's
// defaults.
//
// With the defaults (16 KB frames, a window of a few hundred KB) a 139 MB result took 0.34 to 0.42 s over h2c. With a
// 1 MiB frame and 8 MiB per stream, 16 MiB per connection it took 0.08 to 0.19 s. A http:// DSN uses cleartext HTTP/2
// with prior knowledge (no upgrade, no HTTP/1.1 fallback); an https:// DSN negotiates h2 by ALPN against the system
// roots. The transport never uses a proxy: a cleartext HTTP/2 connection cannot go through one.
func newTransport(secure bool) *http.Transport {
	var p http.Protocols
	if secure {
		p.SetHTTP2(true)
	} else {
		p.SetUnencryptedHTTP2(true)
	}
	return &http.Transport{
		DialContext:         (&net.Dialer{Timeout: 30 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
		TLSHandshakeTimeout: 10 * time.Second,
		Protocols:           &p,
		HTTP2: &http.HTTP2Config{
			MaxReadFrameSize:              1 << 20,
			MaxReceiveBufferPerStream:     8 << 20,
			MaxReceiveBufferPerConnection: 16 << 20,
		},
		DisableCompression:  true, // no "Accept-Encoding: gzip" of net/http either
		MaxIdleConns:        0,    // no limit overall; the pool of database/sql bounds the requests that run at once
		MaxIdleConnsPerHost: 64,
		IdleConnTimeout:     90 * time.Second,
	}
}

// Connect implements driver.Connector. A connection holds no state, so this does not touch the network.
func (c *Connector) Connect(context.Context) (driver.Conn, error) { return &conn{c: c}, nil }

// Driver implements driver.Connector.
func (c *Connector) Driver() driver.Driver { return Driver{} }

// Close closes the idle HTTP connections. database/sql calls it from DB.Close.
func (c *Connector) Close() error {
	c.transport.CloseIdleConnections()
	return nil
}

// authorize adds the Bearer key to the headers of a request, when the DSN had one.
func (c *Connector) authorize(h http.Header) {
	if c.authHeader != "" {
		h.Set("Authorization", c.authHeader)
	}
}

// ServerError is an error from the server: a connect error, usually carrying the ClickHouse exception code. It wraps the
// *connect.Error, so errors.As finds either type.
type ServerError struct {
	// Code is the ClickHouse exception code, or 0 when the server sent none (authentication, a busy server, a
	// transport failure).
	Code int
	// ConnectCode is the connect status: Unauthenticated, NotFound, InvalidArgument, ResourceExhausted, ...
	ConnectCode connect.Code
	// Message is the server's message.
	Message string

	err *connect.Error
}

func (e *ServerError) Error() string {
	if e.Code != 0 {
		return fmt.Sprintf("chdb-server: %s (ClickHouse code %d): %s", e.ConnectCode, e.Code, e.Message)
	}
	return fmt.Sprintf("chdb-server: %s: %s", e.ConnectCode, e.Message)
}

// Unwrap returns the *connect.Error.
func (e *ServerError) Unwrap() error { return e.err }

// wrapErr turns an error from the connect client into the error the caller sees. A connect error caused by the end of
// ctx is the context's error; any other connect error becomes a *ServerError.
func wrapErr(ctx context.Context, err error) error {
	if err == nil {
		return nil
	}
	var ce *connect.Error
	if !errors.As(err, &ce) {
		return err
	}
	if ce.Code() == connect.CodeCanceled || ce.Code() == connect.CodeDeadlineExceeded {
		if cause := ctx.Err(); cause != nil {
			return cause
		}
		// The server enforces the deadline it was sent and can answer a moment before the client's own timer fires.
		if dl, ok := ctx.Deadline(); ok && ce.Code() == connect.CodeDeadlineExceeded && !time.Now().Before(dl) {
			return context.DeadlineExceeded
		}
	}
	se := &ServerError{ConnectCode: ce.Code(), Message: ce.Message(), err: ce}
	if v := ce.Meta().Get(exceptionCodeKey); v != "" {
		se.Code, _ = strconv.Atoi(v)
	}
	return se
}

// InsertOptions describes one Connector.Insert call.
type InsertOptions struct {
	// Table and Columns are plain, unquoted names; the server quotes them. No Columns means every column.
	Table   string
	Columns []string
	// Format is the input format of the data: JSONEachRow (the default when empty), CSV, TabSeparated, ... Text formats
	// with one row per line are merged by the server with other inserts into one part; Native, RowBinary and
	// RowBinaryWithNamesAndTypes are accepted too.
	Format string
	// Database defaults to the DSN's database.
	Database string
	// Settings are applied over the DSN's settings for this call.
	Settings map[string]string
}

// Insert streams data into a table through the Insert RPC and returns the number of data bytes the server wrote. The
// server holds the data in memory until the call ends and writes it in one piece, merged with other inserts: either all
// of data is written or none of it. The call returns after the rows are written.
//
// data is read in chunks of 1 MiB. A read error other than io.EOF, or the end of ctx, aborts the call and nothing is
// written. Insert is safe for concurrent use; concurrent calls are what the server merges.
func (c *Connector) Insert(ctx context.Context, opts InsertOptions, data io.Reader) (written uint64, err error) {
	if opts.Table == "" {
		return 0, errors.New("chdbsql: Insert needs a table")
	}
	db := opts.Database
	if db == "" {
		db = c.database
	}
	settings := c.settings
	if len(opts.Settings) > 0 {
		settings = make(map[string]string, len(c.settings)+len(opts.Settings))
		for k, v := range c.settings {
			settings[k] = v
		}
		for k, v := range opts.Settings {
			settings[k] = v
		}
	}
	// cancel aborts the call when data fails, so the server discards what it has received.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	stream := c.rpc.Insert(ctx)
	c.authorize(stream.RequestHeader())
	hdr := &chdbv1.InsertHeader{Table: opts.Table, Columns: opts.Columns, Format: opts.Format, Database: db, Settings: settings}
	// finish ends the request side and returns the server's answer; it is also how the error of a call that the server
	// ended early is found, after Send returned io.EOF.
	finish := func() (uint64, error) {
		res, err := stream.CloseAndReceive()
		if err != nil {
			return 0, wrapErr(ctx, err)
		}
		return res.Msg.BytesWritten, nil
	}
	if err := stream.Send(&chdbv1.InsertChunk{Payload: &chdbv1.InsertChunk_Header{Header: hdr}}); err != nil {
		if errors.Is(err, io.EOF) {
			_, err = finish()
		}
		return 0, wrapErr(ctx, err)
	}
	buf := make([]byte, insertChunk)
	for {
		n, rerr := io.ReadFull(data, buf)
		if n > 0 {
			// Send marshals the message before it returns, so buf can be reused.
			if err := stream.Send(&chdbv1.InsertChunk{Payload: &chdbv1.InsertChunk_Data{Data: buf[:n]}}); err != nil {
				if errors.Is(err, io.EOF) {
					_, err = finish()
				}
				return 0, wrapErr(ctx, err)
			}
		}
		if rerr == io.EOF || rerr == io.ErrUnexpectedEOF {
			break
		}
		if rerr != nil {
			// Cancel without closing the request side: a clean end of the request would let the server write the data.
			// The cancelled context resets the stream, and the server drops what it has received.
			cancel()
			return 0, rerr
		}
	}
	return finish()
}

// conn is a handle on the connector: every statement is its own RPC.
type conn struct{ c *Connector }

func (cn *conn) Prepare(query string) (driver.Stmt, error) {
	return &wire.Stmt{Conn: cn, Text: query}, nil
}

func (cn *conn) Close() error { return nil }

func (cn *conn) Begin() (driver.Tx, error) { return nil, wire.ErrNoTx }

func (cn *conn) BeginTx(context.Context, driver.TxOptions) (driver.Tx, error) {
	return nil, wire.ErrNoTx
}

func (cn *conn) Ping(ctx context.Context) error {
	_, err := cn.ExecContext(ctx, "SELECT 1", nil)
	return err
}

func (cn *conn) CheckNamedValue(nv *driver.NamedValue) error { return wire.CheckNamedValue(nv) }

// ExecContext runs a statement and discards its output. A statement "INSERT INTO t [(cols)] FORMAT <fmt>\n<data>"
// without arguments goes through the Insert RPC; everything else, including INSERT ... VALUES, INSERT ... SELECT and a
// FORMAT clause without data, goes through the Exec RPC.
func (cn *conn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	c := cn.c
	if len(args) == 0 {
		if ins, ok := parseInsert(query); ok {
			_, err := c.Insert(ctx, InsertOptions{
				Table: ins.table, Columns: ins.columns, Format: ins.format, Database: ins.database,
			}, strings.NewReader(ins.data))
			if err != nil {
				return nil, err
			}
			return wire.Result{}, nil
		}
	}
	q, err := wire.Bind(query, args)
	if err != nil {
		return nil, err
	}
	req := connect.NewRequest(&chdbv1.ExecRequest{Sql: q, Database: c.database, Settings: c.settings})
	c.authorize(req.Header())
	if _, err := c.rpc.Exec(ctx, req); err != nil {
		return nil, wrapErr(ctx, err)
	}
	return wire.Result{}, nil
}

// QueryContext starts a streaming read and returns once the two header lines are in, so Columns works at once.
func (cn *conn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	c := cn.c
	q, err := wire.Bind(query, args)
	if err != nil {
		return nil, err
	}
	// cancel kills the RPC, which makes the server stop the query. Close calls it when the result was not read to
	// its end.
	rctx, cancel := context.WithCancel(ctx)
	req := connect.NewRequest(&chdbv1.QueryRequest{Sql: q, Format: wire.Format, Database: c.database, Settings: c.settings})
	c.authorize(req.Header())
	stream, err := c.rpc.Query(rctx, req)
	if err != nil {
		cancel()
		return nil, wrapErr(ctx, err)
	}
	cr := &chunkReader{stream: stream}
	r := &rows{ctx: ctx, stream: stream, cr: cr, br: bufio.NewReaderSize(cr, 64<<10), cancel: cancel}
	if err := r.readHeader(); err != nil {
		r.Close()
		return nil, err
	}
	return r, nil
}

// chunkReader presents the chunks of a Query stream as one byte stream. A chunk boundary can fall inside a line, so the
// lines are framed by a bufio.Reader on top of it.
type chunkReader struct {
	stream *connect.ServerStreamForClient[chdbv1.QueryChunk]
	buf    []byte // the unread rest of the current chunk
	chunks int
	err    error
}

func (r *chunkReader) Read(p []byte) (int, error) {
	for len(r.buf) == 0 {
		if r.err != nil {
			return 0, r.err
		}
		if !r.stream.Receive() {
			if r.err = r.stream.Err(); r.err == nil {
				r.err = io.EOF
			}
			continue
		}
		r.chunks++
		r.buf = r.stream.Msg().Data
	}
	n := copy(p, r.buf)
	r.buf = r.buf[n:]
	return n, nil
}

// rows reads the result lines of one Query stream.
type rows struct {
	ctx    context.Context // the context of the call, to tell its end from a server error
	stream *connect.ServerStreamForClient[chdbv1.QueryChunk]
	cr     *chunkReader
	br     *bufio.Reader
	cancel context.CancelFunc
	cols   wire.Columns
	done   bool  // the end of the result, or an error, was seen
	err    error // the sticky error, if the result ended with one
	closed bool
}

// line returns the next row line without its newline, or io.EOF. A stream that ends with an error ends the result with
// that error; a partial last line is dropped.
func (r *rows) line() ([]byte, error) {
	if r.err != nil {
		return nil, r.err
	}
	for {
		l, err := r.br.ReadBytes('\n')
		if err != nil && err != io.EOF {
			r.done = true
			r.err = wrapErr(r.ctx, err)
			return nil, r.err
		}
		l = bytes.TrimRight(l, "\r\n")
		if len(l) == 0 {
			if err == io.EOF {
				r.done = true
				r.err = io.EOF
				return nil, io.EOF
			}
			continue
		}
		return l, nil
	}
}

func (r *rows) readHeader() error {
	names, err := r.line()
	if err == io.EOF {
		return nil // a statement without a result set
	}
	if err != nil {
		return err
	}
	types, err := r.line()
	if err != nil {
		if err == io.EOF {
			err = wire.ErrNoTypes
		}
		return err
	}
	r.cols, err = wire.ParseHeader(names, types)
	return err
}

func (r *rows) Columns() []string { return r.cols.Names }

func (r *rows) Next(dest []driver.Value) error {
	if r.closed {
		return io.EOF
	}
	l, err := r.line()
	if err != nil {
		return err
	}
	return r.cols.DecodeRow(l, dest)
}

// Close releases the stream cleanly when the result was read to its end, and cancels the RPC otherwise, which makes the
// server kill the query. A reader that stops right after the last row (QueryRow) has not seen the end yet, so Close
// waits briefly for it before it judges the result unfinished.
func (r *rows) Close() error {
	if r.closed {
		return nil
	}
	if !r.done {
		t := time.AfterFunc(peekGrace, r.cancel)
		if _, err := r.br.Peek(1); err == io.EOF {
			r.done = true
		}
		t.Stop()
	}
	r.closed = true
	if !r.done {
		r.cancel() // before Close, so the stream is reset instead of drained
	}
	r.stream.Close()
	r.cancel()
	return nil
}

func (r *rows) ColumnTypeDatabaseTypeName(i int) string { return r.cols.TypeName(i) }

func (r *rows) ColumnTypeNullable(i int) (nullable, ok bool) {
	return r.cols.Nullable(i), true
}

func (r *rows) ColumnTypeScanType(i int) reflect.Type { return r.cols.ScanType(i) }
