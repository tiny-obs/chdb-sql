package chdbsql

import (
	"bufio"
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"reflect"
	"strconv"
	"strings"
	"time"
)

// HTTPDriverName is the name registered with database/sql for the HTTP driver.
const HTTPDriverName = "chdbsql-http"

func init() { sql.Register(HTTPDriverName, &HTTPDriver{}) }

const (
	// exceptionTagHeader carries the tag that marks an error ClickHouse writes into the body of a response that already
	// started with status 200 (the format-independent "__exception__" block).
	exceptionTagHeader = "X-ClickHouse-Exception-Tag"
	exceptionMarker    = "__exception__"

	maxErrorBody = 64 << 10 // an error page is read up to this size
	maxErrorText = 4 << 10  // and kept in the error up to this size
	maxExecBody  = 1 << 20  // the output of a statement run through ExecContext is read up to this size
	maxTrailer   = 1 << 20  // the exception block at the end of a result is read up to this size
	// peekGrace bounds how long Close waits to learn whether a result was read to its last row.
	peekGrace = 50 * time.Millisecond
)

// HTTPDriver implements driver.Driver and driver.DriverContext for a ClickHouse server's HTTP interface.
//
// DSN: "http://user:password@host:8123/database?max_execution_time=10&readonly=1", or https. The path is the default
// database. Every query parameter is a ClickHouse setting that is sent with every request, so a connection that is
// read-only (readonly=1) can still carry max_execution_time, unlike an embedded read-only connection. Results are read
// as JSONCompactEachRowWithNamesAndTypes and decoded exactly like the embedded driver's: arrays, maps and tuples arrive
// as JSON text, 64-bit integers keep every digit, DateTime values come in the column's time zone.
//
// A statement is one stateless POST: the SQL text is the request body, so INSERT ... FORMAT JSONEachRow followed by the
// rows is sent verbatim. "?" arguments are rendered into the text as literals, as in the embedded driver. There are no
// transactions and no RowsAffected.
//
// A statement that fails after ClickHouse started the response (an error in the middle of a large result) is reported
// as an error by Rows.Next or Rows.Err, never as a short result.
type HTTPDriver struct{}

// Open implements driver.Driver.
func (HTTPDriver) Open(dsn string) (driver.Conn, error) {
	c, err := NewHTTPConnector(dsn)
	if err != nil {
		return nil, err
	}
	return c.Connect(context.Background())
}

// OpenConnector implements driver.DriverContext.
func (HTTPDriver) OpenConnector(dsn string) (driver.Connector, error) { return NewHTTPConnector(dsn) }

// HTTPConnector hands out stateless connections that share one HTTP client and its keep-alive connections. Pass it to
// sql.OpenDB; closing the DB closes the idle HTTP connections.
type HTTPConnector struct {
	endpoint    string
	user        string
	password    string
	hasUserInfo bool
	params      url.Values
	client      *http.Client
}

// NewHTTPConnector parses dsn, see HTTPDriver. It does not connect.
func NewHTTPConnector(dsn string) (*HTTPConnector, error) {
	u, err := url.Parse(dsn)
	if err != nil {
		return nil, fmt.Errorf("chdbsql: bad DSN: %w", err)
	}
	if (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return nil, errors.New("chdbsql: the HTTP DSN must look like http://host:8123/database?setting=value")
	}
	c := &HTTPConnector{endpoint: u.Scheme + "://" + u.Host + "/", params: u.Query()}
	if u.User != nil {
		c.hasUserInfo = true
		c.user = u.User.Username()
		c.password, _ = u.User.Password()
	}
	if db := strings.Trim(u.Path, "/"); db != "" && c.params.Get("database") == "" {
		c.params.Set("database", db)
	}
	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.MaxIdleConns = 0 // no limit overall; the pool of database/sql bounds the requests that run at once
	tr.MaxIdleConnsPerHost = 64
	// There is no client timeout: a result streams for as long as it takes, and the context of the call bounds it.
	c.client = &http.Client{Transport: tr}
	return c, nil
}

// Connect implements driver.Connector. A connection holds no state, so this does not touch the network.
func (c *HTTPConnector) Connect(context.Context) (driver.Conn, error) { return &httpConn{c: c}, nil }

// Driver implements driver.Connector.
func (c *HTTPConnector) Driver() driver.Driver { return HTTPDriver{} }

// Close closes the idle HTTP connections. database/sql calls it from DB.Close.
func (c *HTTPConnector) Close() error {
	c.client.CloseIdleConnections()
	return nil
}

// post sends query as the body of one request. withFormat asks for the result format; a statement that has its own FORMAT
// clause keeps it.
func (c *HTTPConnector) post(ctx context.Context, query string, withFormat bool) (*http.Response, error) {
	q := make(url.Values, len(c.params)+1)
	for k, v := range c.params {
		q[k] = v
	}
	if withFormat {
		q.Set("default_format", resultFormat)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint+"?"+q.Encode(), strings.NewReader(query))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "text/plain; charset=utf-8")
	// ClickHouse compresses and buffers a response for a client that accepts gzip, which Go does unless told otherwise. An
	// error in the middle of a large result then arrives as status 500 with the buffered rows as its body, and a result
	// does not stream.
	req.Header.Set("Accept-Encoding", "identity")
	if c.hasUserInfo {
		req.SetBasicAuth(c.user, c.password)
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, maxErrorBody))
		resp.Body.Close()
		if se := findException(b, resp.Header.Get(exceptionTagHeader)); se != nil {
			se.Status = resp.StatusCode
			return nil, se
		}
		if len(b) > maxErrorText {
			b = b[:maxErrorText]
		}
		return nil, newServerError(resp.StatusCode, string(b))
	}
	return resp, nil
}

// ServerError is an error ClickHouse reported: a non-200 status, or an exception inside a response that had started.
type ServerError struct {
	Status  int    // HTTP status; 200 for an exception that arrived inside the body
	Code    int    // ClickHouse error code, 0 when the text has none
	Message string // "Code: 164. DB::Exception: ... (READONLY)", as the server wrote it
}

func (e *ServerError) Error() string {
	if e.Message == "" {
		return "chdbsql: server answered " + http.StatusText(e.Status)
	}
	return e.Message
}

func newServerError(status int, msg string) *ServerError {
	msg = strings.TrimSpace(msg)
	e := &ServerError{Status: status, Message: msg}
	if rest, ok := strings.CutPrefix(msg, "Code: "); ok {
		if i := strings.IndexByte(rest, '.'); i > 0 {
			e.Code, _ = strconv.Atoi(rest[:i])
		}
	}
	return e
}

// exceptionError turns the text after an exception marker line into an error. The block, written with CRLF line ends, is
// "<tag>\r\n<message>\r\n<length> <tag>\r\n__exception__\r\n"; without the tag line or the trailer the whole text is the
// message.
func exceptionError(rest []byte, tag string) *ServerError {
	if r, ok := bytes.CutPrefix(rest, []byte(tag)); ok {
		rest = bytes.TrimPrefix(bytes.TrimPrefix(r, []byte("\r")), []byte("\n"))
	}
	if i := bytes.LastIndex(rest, []byte(" "+tag)); i >= 0 {
		if j := bytes.LastIndexByte(rest[:i], '\n'); j >= 0 {
			rest = rest[:j]
		}
	}
	return newServerError(http.StatusOK, string(rest))
}

// findException looks for an exception block that starts a line of body, or returns nil.
func findException(body []byte, tag string) *ServerError {
	if tag == "" {
		return nil
	}
	for from := 0; ; {
		i := bytes.Index(body[from:], []byte(exceptionMarker))
		if i < 0 {
			return nil
		}
		i += from
		from = i + len(exceptionMarker)
		if i > 0 && body[i-1] != '\n' {
			continue
		}
		rest := bytes.TrimPrefix(bytes.TrimPrefix(body[from:], []byte("\r")), []byte("\n"))
		if bytes.HasPrefix(rest, []byte(tag)) {
			return exceptionError(rest, tag)
		}
	}
}

// httpConn is a handle on the connector: every statement is its own request.
type httpConn struct{ c *HTTPConnector }

func (cn *httpConn) Prepare(query string) (driver.Stmt, error) {
	return &stmt{cn: cn, query: query}, nil
}

func (cn *httpConn) Close() error { return nil }

func (cn *httpConn) Begin() (driver.Tx, error) { return nil, errNoTx }

func (cn *httpConn) BeginTx(context.Context, driver.TxOptions) (driver.Tx, error) {
	return nil, errNoTx
}

func (cn *httpConn) Ping(ctx context.Context) error {
	_, err := cn.ExecContext(ctx, "SELECT 1", nil)
	return err
}

func (cn *httpConn) CheckNamedValue(nv *driver.NamedValue) error { return checkNamedValue(nv) }

// ExecContext runs a statement and discards its output. An error ClickHouse writes into the output is returned.
func (cn *httpConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	q, err := bind(query, args)
	if err != nil {
		return nil, err
	}
	resp, err := cn.c.post(ctx, q, false)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxExecBody))
	if err != nil {
		return nil, err
	}
	if se := findException(body, resp.Header.Get(exceptionTagHeader)); se != nil {
		return nil, se
	}
	return result{}, nil
}

// QueryContext starts a streaming read and returns once the two header lines are in, so Columns works at once.
func (cn *httpConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	q, err := bind(query, args)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(ctx)
	resp, err := cn.c.post(ctx, q, true)
	if err != nil {
		cancel()
		return nil, err
	}
	r := &httpRows{body: resp.Body, br: bufio.NewReaderSize(resp.Body, 64<<10), cancel: cancel, tag: resp.Header.Get(exceptionTagHeader)}
	if err := r.readHeader(); err != nil {
		r.Close()
		return nil, err
	}
	return r, nil
}

// httpRows reads the result lines of one response.
type httpRows struct {
	body   io.ReadCloser
	br     *bufio.Reader
	cancel context.CancelFunc
	tag    string
	cols   columns
	eof    bool
}

// line returns the next row line without its newline, or io.EOF. An exception block, or the text an older server appends
// to the body, ends the result with an error.
func (r *httpRows) line() ([]byte, error) {
	for {
		l, err := r.br.ReadBytes('\n')
		if err != nil && err != io.EOF {
			return nil, err
		}
		l = bytes.TrimRight(l, "\r\n")
		if len(l) == 0 {
			if err == io.EOF {
				r.eof = true
				return nil, io.EOF
			}
			continue
		}
		if l[0] == '[' {
			return l, nil // a row; the header lines are arrays too
		}
		if r.tag != "" && string(l) == exceptionMarker {
			rest, _ := io.ReadAll(io.LimitReader(r.br, maxTrailer))
			return nil, exceptionError(rest, r.tag)
		}
		if strings.HasPrefix(string(l), "Code: ") && strings.Contains(string(l), "DB::Exception") {
			rest, _ := io.ReadAll(io.LimitReader(r.br, maxTrailer))
			return nil, newServerError(http.StatusOK, string(l)+"\n"+string(rest))
		}
		return l, nil
	}
}

func (r *httpRows) readHeader() error {
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
			err = errNoTypeHeader
		}
		return err
	}
	r.cols, err = parseHeader(names, types)
	return err
}

func (r *httpRows) Columns() []string { return r.cols.names }

func (r *httpRows) Next(dest []driver.Value) error {
	if r.body == nil {
		return io.EOF
	}
	l, err := r.line()
	if err != nil {
		return err
	}
	return r.cols.decodeRow(l, dest)
}

// Close lets the HTTP connection go back to the pool when the result was read to its end, and aborts the request
// otherwise, which makes the server stop the query. A reader that stops right after the last row (QueryRow) has not
// seen the end yet, so Close looks at the next byte for a moment before it judges the result unfinished.
func (r *httpRows) Close() error {
	if r.body == nil {
		return nil
	}
	if !r.eof {
		t := time.AfterFunc(peekGrace, r.cancel)
		if _, err := r.br.Peek(1); err == io.EOF {
			r.eof = true
		}
		t.Stop()
	}
	r.body.Close()
	r.cancel()
	r.body = nil
	return nil
}

func (r *httpRows) ColumnTypeDatabaseTypeName(i int) string { return r.cols.typeName(i) }

func (r *httpRows) ColumnTypeNullable(i int) (nullable, ok bool) {
	return r.cols.types[i].nullable, true
}

func (r *httpRows) ColumnTypeScanType(i int) reflect.Type { return r.cols.types[i].scanType() }
