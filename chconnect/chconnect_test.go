package chconnect

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"os"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"connectrpc.com/connect"
)

// The tests below that talk to a server need a chdb-server. Set CHDBSQL_CONNECT_DSN to it, for example
//
//	CHDBSQL_CONNECT_DSN=http://:key@localhost:8123/default go test -race ./chconnect
//
// Without it they are skipped. A server started with CHDB_AUTH_KEY needs the key as the DSN password; set
// CHDBSQL_CONNECT_AUTH=1 as well to run the test that a wrong or missing key is refused. The parsing tests and the tests
// against the in-process fake (fake_test.go) always run.

func connectDSN(t *testing.T, settings map[string]string) string {
	t.Helper()
	dsn := os.Getenv("CHDBSQL_CONNECT_DSN")
	if dsn == "" {
		t.Skip("CHDBSQL_CONNECT_DSN is not set")
	}
	u, err := url.Parse(dsn)
	if err != nil {
		t.Fatal(err)
	}
	q := u.Query()
	for k, v := range settings {
		q.Set(k, v)
	}
	u.RawQuery = q.Encode()
	return u.String()
}

func openConnect(t *testing.T, settings map[string]string, maxConns int) (*sql.DB, *Connector) {
	t.Helper()
	c, err := NewConnector(connectDSN(t, settings))
	if err != nil {
		t.Fatal(err)
	}
	db := sql.OpenDB(c)
	db.SetMaxOpenConns(maxConns)
	if err := db.PingContext(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return db, c
}

// The connect driver decodes like the embedded and the HTTP ones: the same table, the same statements, the same Go
// values.
func TestConnectRoundTripDecodesEveryColumnType(t *testing.T) {
	w, _ := openConnect(t, nil, 1)
	ctx := context.Background()
	for _, q := range []string{
		`DROP TABLE IF EXISTS types_connect`,
		`CREATE TABLE types_connect (
			id UInt64, i Int32, big UInt64, f Float64, s String, lc LowCardinality(String), n Nullable(String), b Bool,
			d Date, dt DateTime64(6,'Europe/Berlin'), dec Decimal(10,2), tags Map(String,String), arr Array(String)
		) ENGINE = MergeTree ORDER BY id`,
	} {
		if _, err := w.ExecContext(ctx, q); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() { w.ExecContext(context.Background(), `DROP TABLE IF EXISTS types_connect`) })
	ts := time.Date(2026, 10, 1, 12, 30, 45, 123456000, time.UTC)
	if _, err := w.ExecContext(ctx, `INSERT INTO types_connect VALUES (?, ?, ?, ?, ?, ?, ?, ?, '2026-10-01', ?, 12.5, map('k','v'), ?)`,
		1, -7, uint64(math.MaxUint64), 1.5, "it's \\ ok", "lc", nil, true, ts, []string{"a", "b"}); err != nil {
		t.Fatal(err)
	}
	// Inline rows after FORMAT JSONEachRow go through the Insert RPC, a "?" in them included.
	if _, err := w.ExecContext(ctx, "INSERT INTO types_connect (id, s, n) FORMAT JSONEachRow\n{\"id\":2,\"s\":\"what? yes\",\"n\":\"x\"}\n"); err != nil {
		t.Fatal(err)
	}

	var (
		id  int64
		i   int64
		big uint64
		f   float64
		s   string
		lc  string
		n   sql.NullString
		b   bool
		d   time.Time
		dt  time.Time
		dec string
		tgs string
		arr string
	)
	row := w.QueryRowContext(ctx, `SELECT id, i, big, f, s, lc, n, b, d, dt, dec, tags, arr FROM types_connect WHERE id = ?`, 1)
	if err := row.Scan(&id, &i, &big, &f, &s, &lc, &n, &b, &d, &dt, &dec, &tgs, &arr); err != nil {
		t.Fatal(err)
	}
	if id != 1 || i != -7 || big != math.MaxUint64 || f != 1.5 || s != "it's \\ ok" || lc != "lc" || n.Valid || !b {
		t.Fatalf("scalars wrong: %v %v %v %v %q %q %v %v", id, i, big, f, s, lc, n, b)
	}
	if !d.Equal(time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)) {
		t.Fatalf("date = %v", d)
	}
	if !dt.Equal(ts) || dt.Location().String() != "Europe/Berlin" {
		t.Fatalf("datetime64 = %v (%v), want instant %v in Europe/Berlin", dt, dt.Location(), ts)
	}
	if dec != "12.5" || tgs != `{"k":"v"}` || arr != `["a","b"]` {
		t.Fatalf("dec=%q tags=%q arr=%q", dec, tgs, arr)
	}
	if err := w.QueryRowContext(ctx, `SELECT s, n FROM types_connect WHERE id = 2`).Scan(&s, &n); err != nil || s != "what? yes" || n.String != "x" {
		t.Fatalf("inline insert: s=%q n=%v err=%v", s, n, err)
	}
	var nan float64
	if err := w.QueryRowContext(ctx, `SELECT nan`).Scan(&nan); err != nil || !math.IsNaN(nan) {
		t.Fatalf("nan = %v err=%v", nan, err)
	}

	rs, err := w.QueryContext(ctx, `SELECT id, n, dt, tags FROM types_connect WHERE id = 1`)
	if err != nil {
		t.Fatal(err)
	}
	defer rs.Close()
	types, err := rs.ColumnTypes()
	if err != nil {
		t.Fatal(err)
	}
	wantNames := []string{"UInt64", "Nullable(String)", "DateTime64(6, 'Europe/Berlin')", "Map(String, String)"}
	for k, ct := range types {
		if ct.DatabaseTypeName() != wantNames[k] {
			t.Errorf("column %d type name = %q, want %q", k, ct.DatabaseTypeName(), wantNames[k])
		}
	}
	if nullable, ok := types[1].Nullable(); !ok || !nullable {
		t.Errorf("n should be nullable")
	}
	if types[0].ScanType() != reflect.TypeOf(uint64(0)) || types[2].ScanType() != reflect.TypeOf(time.Time{}) {
		t.Errorf("scan types = %v %v", types[0].ScanType(), types[2].ScanType())
	}
}

func TestConnectZeroRowResultStillReportsColumns(t *testing.T) {
	db, _ := openConnect(t, nil, 1)
	rs, err := db.QueryContext(context.Background(), `SELECT toUInt8(1) AS a, 'x' AS b WHERE 0`)
	if err != nil {
		t.Fatal(err)
	}
	defer rs.Close()
	cols, err := rs.Columns()
	if err != nil || len(cols) != 2 || cols[0] != "a" || cols[1] != "b" {
		t.Fatalf("columns = %v, %v", cols, err)
	}
	if rs.Next() {
		t.Fatal("a row came back")
	}
	if err := rs.Err(); err != nil {
		t.Fatal(err)
	}
}

// The server answers with a stream and the error comes after the first rows. A reader that stopped at the end of the
// chunks would take the rows before it for the whole result.
func TestConnectErrorInTheMiddleOfAResultIsAnError(t *testing.T) {
	db, _ := openConnect(t, nil, 1)
	rs, err := db.QueryContext(context.Background(), `SELECT number, throwIf(number = 200000) FROM numbers(1000000) SETTINGS max_block_size = 1000`)
	if err != nil {
		t.Fatal(err)
	}
	defer rs.Close()
	n := 0
	for rs.Next() {
		n++
	}
	err = rs.Err()
	var se *ServerError
	var ce *connect.Error
	if !errors.As(err, &se) || se.Code != 395 || !errors.As(err, &ce) {
		t.Fatalf("after %d rows: err = %v, want ServerError code 395", n, err)
	}
	// The server may reject SETTINGS in a query (LockSettings); that is a different failure and shows up as code 0.
	if n >= 1000000 {
		t.Fatalf("rows before the error = %d", n)
	}
}

func TestConnectErrorStatusCarriesTheServerError(t *testing.T) {
	db, _ := openConnect(t, nil, 1)
	ctx := context.Background()
	_, err := db.ExecContext(ctx, `SELEKT 1`)
	var se *ServerError
	if !errors.As(err, &se) || se.Code != 62 || se.ConnectCode != connect.CodeInvalidArgument {
		t.Fatalf("syntax error: %v", err)
	}
	_, err = db.QueryContext(ctx, `SELECT * FROM no_such_table_here`)
	if !errors.As(err, &se) || se.Code != 60 || se.ConnectCode != connect.CodeNotFound {
		t.Fatalf("unknown table: %v", err)
	}
	if err := db.QueryRowContext(ctx, `SELECT 1`).Scan(new(int)); err != nil {
		t.Fatalf("the pool is unusable after errors: %v", err)
	}
}

// A result of 300 000 rows arrives in several chunks and is complete; a chunk boundary inside a line is not an error.
func TestConnectLargeResultStreamsIntactInSeveralChunks(t *testing.T) {
	db, _ := openConnect(t, nil, 1)
	ctx := context.Background()
	c, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	var chunks int
	var sum, n int64
	err = c.Raw(func(dc any) error {
		drows, err := dc.(driver.QueryerContext).QueryContext(ctx, `SELECT number, toString(number) FROM numbers(300000)`, nil)
		if err != nil {
			return err
		}
		defer drows.Close()
		dest := make([]driver.Value, 2)
		for {
			if err := drows.Next(dest); err != nil {
				if err == io.EOF {
					break
				}
				return err
			}
			sum += int64(dest[0].(uint64))
			n++
		}
		chunks = drows.(*rows).cr.chunks
		return nil
	})
	if err != nil || n != 300000 || sum != 300000*299999/2 {
		t.Fatalf("rows=%d sum=%d err=%v", n, sum, err)
	}
	if chunks < 2 {
		t.Fatalf("the result came in %d chunk", chunks)
	}
	t.Logf("%d chunks", chunks)
}

func TestConnectLargeResultThroughDatabaseSQL(t *testing.T) {
	db, _ := openConnect(t, nil, 1)
	rs, err := db.QueryContext(context.Background(), `SELECT number, toString(number) FROM numbers(300000)`)
	if err != nil {
		t.Fatal(err)
	}
	defer rs.Close()
	var sum, count int64
	for rs.Next() {
		var n int64
		var s string
		if err := rs.Scan(&n, &s); err != nil {
			t.Fatal(err)
		}
		if s != fmt.Sprint(n) {
			t.Fatalf("row %d has %q", n, s)
		}
		sum += n
		count++
	}
	if err := rs.Err(); err != nil || count != 300000 || sum != 300000*299999/2 {
		t.Fatalf("rows=%d sum=%d err=%v", count, sum, err)
	}
}

// Closing a result before its end cancels the RPC; the server then kills the query, which leaves system.processes.
func TestConnectEarlyCloseCancelsTheQuery(t *testing.T) {
	db, _ := openConnect(t, nil, 2)
	ctx := context.Background()
	const marker = "chconnect_early_close_marker"
	running := func() int {
		var n int
		q := `SELECT count() FROM system.processes WHERE position(query, '` + marker + `') > 0 AND position(query, 'system.processes') = 0`
		if err := db.QueryRowContext(ctx, q).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	rs, err := db.QueryContext(ctx, `SELECT number FROM numbers(100000000000) /* `+marker+` */`)
	if err != nil {
		t.Fatal(err)
	}
	if !rs.Next() {
		t.Fatal("no first row")
	}
	if running() != 1 {
		t.Fatalf("the query is not running: %d", running())
	}
	start := time.Now()
	rs.Close()
	if d := time.Since(start); d > 2*time.Second {
		t.Fatalf("Close took %v", d)
	}
	deadline := time.Now().Add(10 * time.Second)
	for running() != 0 {
		if time.Now().After(deadline) {
			t.Fatal("the query still runs 10 s after the result was closed")
		}
		time.Sleep(50 * time.Millisecond)
	}
	var one int
	if err := db.QueryRowContext(ctx, `SELECT 1`).Scan(&one); err != nil || one != 1 {
		t.Fatalf("next query: %v %v", one, err)
	}
}

func TestConnectContextCancelEndsARunningStatement(t *testing.T) {
	db, _ := openConnect(t, nil, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err := db.ExecContext(ctx, `SELECT sleep(3)`)
	if !errors.Is(err, context.DeadlineExceeded) || time.Since(start) > 2*time.Second {
		t.Fatalf("err = %v after %v", err, time.Since(start))
	}
	if err := db.QueryRowContext(context.Background(), `SELECT 1`).Scan(new(int)); err != nil {
		t.Fatal(err)
	}
}

func TestConnectParallelReaders(t *testing.T) {
	db, _ := openConnect(t, nil, 8)
	var wg sync.WaitGroup
	errs := make(chan error, 32)
	for g := range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rs, err := db.QueryContext(context.Background(), `SELECT number, toString(number) FROM numbers(50000) WHERE number >= ?`, g)
			if err != nil {
				errs <- err
				return
			}
			defer rs.Close()
			var sum int64
			for rs.Next() {
				var n int64
				var s string
				if err := rs.Scan(&n, &s); err != nil {
					errs <- err
					return
				}
				sum += n
			}
			if err := rs.Err(); err != nil {
				errs <- err
			}
			if want := int64(50000*49999/2 - g*(g-1)/2); sum != want {
				errs <- fmt.Errorf("reader %d: sum %d, want %d", g, sum, want)
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}

// countingTransport counts the requests per RPC method.
type countingTransport struct {
	http.RoundTripper
	mu    sync.Mutex
	calls map[string]int
}

func (c *countingTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	c.mu.Lock()
	c.calls[r.URL.Path[strings.LastIndex(r.URL.Path, "/")+1:]]++
	c.mu.Unlock()
	return c.RoundTripper.RoundTrip(r)
}

func (c *countingTransport) count(method string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.calls[method]
}

func TestConnectInsertAPIAndInsertFormatRouting(t *testing.T) {
	db, c := openConnect(t, nil, 2)
	ct := &countingTransport{RoundTripper: c.httpc.Transport, calls: map[string]int{}}
	c.httpc.Transport = ct
	ctx := context.Background()
	for _, q := range []string{
		`DROP TABLE IF EXISTS ins_connect`,
		`CREATE TABLE ins_connect (id UInt64, s String) ENGINE = MergeTree ORDER BY id`,
	} {
		if _, err := db.ExecContext(ctx, q); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() { db.ExecContext(context.Background(), `DROP TABLE IF EXISTS ins_connect`) })
	count := func() (n, sum int64) {
		if err := db.QueryRowContext(ctx, `SELECT count(), toInt64(sum(id)) FROM ins_connect`).Scan(&n, &sum); err != nil {
			t.Fatal(err)
		}
		return n, sum
	}

	// The Insert API, several batches at once.
	const batches, perBatch = 6, 20000
	var wg sync.WaitGroup
	errs := make(chan error, batches)
	var bytesWritten atomic.Uint64
	for b := range batches {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var sb strings.Builder
			for i := range perBatch {
				fmt.Fprintf(&sb, "{\"id\":%d,\"s\":\"batch %d row %d\"}\n", b*perBatch+i, b, i)
			}
			n, err := c.Insert(ctx, InsertOptions{Table: "ins_connect", Format: "JSONEachRow"}, strings.NewReader(sb.String()))
			if err == nil && n != uint64(sb.Len()) {
				err = fmt.Errorf("batch %d: %d bytes written, sent %d", b, n, sb.Len())
			}
			bytesWritten.Add(n)
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	n, sum := count()
	if want := int64(batches * perBatch); n != want || sum != want*(want-1)/2 {
		t.Fatalf("after the Insert API: %d rows, sum %d (bytes written %d)", n, sum, bytesWritten.Load())
	}
	if got := ct.count("Insert"); got != batches {
		t.Fatalf("%d Insert calls", got)
	}

	// Columns, CSV and a quoted, database-qualified name.
	if _, err := c.Insert(ctx, InsertOptions{Table: "ins_connect", Columns: []string{"s", "id"}, Format: "CSV", Database: "default"},
		strings.NewReader("\"csv, row\",1000000\n")); err != nil {
		t.Fatal(err)
	}
	var s string
	if err := db.QueryRowContext(ctx, `SELECT s FROM ins_connect WHERE id = 1000000`).Scan(&s); err != nil || s != "csv, row" {
		t.Fatalf("csv row: %q %v", s, err)
	}

	// INSERT ... FORMAT through ExecContext goes through the Insert RPC; the other shapes use Exec.
	before := ct.count("Insert")
	if _, err := db.ExecContext(ctx, "INSERT INTO `default`.\"ins_connect\" (id, s) FORMAT JSONEachRow\n{\"id\":2000000,\"s\":\"routed\"}\n{\"id\":2000001,\"s\":\"routed\"}\n"); err != nil {
		t.Fatal(err)
	}
	if ct.count("Insert") != before+1 {
		t.Fatalf("INSERT ... FORMAT was not sent through the Insert RPC")
	}
	execs := ct.count("Exec")
	if _, err := db.ExecContext(ctx, `INSERT INTO ins_connect VALUES (?, ?)`, 3000000, "values"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO ins_connect SELECT 3000001, 'select'`); err != nil {
		t.Fatal(err)
	}
	if ct.count("Insert") != before+1 || ct.count("Exec") != execs+2 {
		t.Fatalf("VALUES and SELECT must use Exec: inserts %d, execs %d", ct.count("Insert"), ct.count("Exec")-execs)
	}
	var routed, other int
	if err := db.QueryRowContext(ctx, `SELECT countIf(s = 'routed'), countIf(s IN ('values', 'select')) FROM ins_connect`).Scan(&routed, &other); err != nil || routed != 2 || other != 2 {
		t.Fatalf("routed=%d other=%d err=%v", routed, other, err)
	}

	// A data reader that fails aborts the call and nothing of it is written.
	rowsBefore, _ := count()
	boom := errors.New("boom")
	good := strings.NewReader(strings.Repeat("{\"id\":4000000,\"s\":\"aborted\"}\n", 100000))
	if _, err := c.Insert(ctx, InsertOptions{Table: "ins_connect"}, io.MultiReader(good, &failingReader{err: boom})); !errors.Is(err, boom) {
		t.Fatalf("insert with a failing reader: %v", err)
	}
	time.Sleep(200 * time.Millisecond)
	if rowsAfter, _ := count(); rowsAfter != rowsBefore {
		t.Fatalf("an aborted insert wrote %d rows", rowsAfter-rowsBefore)
	}

	// Errors: an unknown table.
	var se *ServerError
	_, err := c.Insert(ctx, InsertOptions{Table: "no_such_table_here"}, strings.NewReader("{\"id\":1}\n"))
	if !errors.As(err, &se) || se.Code != 60 {
		t.Fatalf("insert into an unknown table: %v", err)
	}
	_, err = db.ExecContext(ctx, "INSERT INTO ins_connect FORMAT JSONEachRow\nnot json\n")
	if !errors.As(err, &se) || se.Code == 0 {
		t.Fatalf("insert of bad data: %v", err)
	}
}

// With a server that has CHDB_AUTH_KEY set, a wrong or missing key is refused with Unauthenticated, on every RPC.
func TestConnectWrongKeyIsRefused(t *testing.T) {
	dsn := connectDSN(t, nil)
	if os.Getenv("CHDBSQL_CONNECT_AUTH") != "1" {
		t.Skip("CHDBSQL_CONNECT_AUTH=1 is not set: the server is not known to require a key")
	}
	u, err := url.Parse(dsn)
	if err != nil {
		t.Fatal(err)
	}
	for name, userinfo := range map[string]*url.Userinfo{
		"wrong key": url.UserPassword("any", "definitely-not-the-key"),
		"no key":    nil,
	} {
		u.User = userinfo
		c, err := NewConnector(u.String())
		if err != nil {
			t.Fatal(err)
		}
		db := sql.OpenDB(c)
		ctx := context.Background()
		var se *ServerError
		if err := db.PingContext(ctx); !errors.As(err, &se) || se.ConnectCode != connect.CodeUnauthenticated {
			t.Errorf("%s, exec: %v", name, err)
		}
		if err := db.QueryRowContext(ctx, `SELECT 1`).Scan(new(int)); !errors.As(err, &se) || se.ConnectCode != connect.CodeUnauthenticated {
			t.Errorf("%s, query: %v", name, err)
		}
		if _, err := c.Insert(ctx, InsertOptions{Table: "t"}, strings.NewReader("{}\n")); !errors.As(err, &se) || se.ConnectCode != connect.CodeUnauthenticated {
			t.Errorf("%s, insert: %v", name, err)
		}
		db.Close()
	}
}

// ---- no server needed ----

func TestDSNMustBeAnHTTPURL(t *testing.T) {
	for _, dsn := range []string{"file:/data", "localhost:8123", "http://", ":::", "grpc://localhost:8123"} {
		if _, err := NewConnector(dsn); err == nil {
			t.Errorf("NewConnector(%q) accepted the DSN", dsn)
		}
	}
}

func TestDSNParsing(t *testing.T) {
	for _, tc := range []struct {
		dsn      string
		base     string
		auth     string
		database string
		settings map[string]string
	}{
		{"http://localhost:8123", "http://localhost:8123", "", "", nil},
		{"http://localhost:8123/", "http://localhost:8123", "", "", nil},
		{"http://:key@localhost:8123/default", "http://localhost:8123", "Bearer key", "default", nil},
		{"http://user:k%40y%2F@h:1/logs?readonly=1&max_execution_time=10", "http://h:1", "Bearer k@y/", "logs",
			map[string]string{"readonly": "1", "max_execution_time": "10"}},
		{"http://user@h:1/logs", "http://h:1", "", "logs", nil}, // no password: no header, the user name is ignored
		{"http://user:@h:1/logs", "http://h:1", "", "logs", nil},
		{"https://:key@ch.example:8443/db?async_insert=1", "https://ch.example:8443", "Bearer key", "db", map[string]string{"async_insert": "1"}},
		{"http://h:1/?database=viaparam&x=y", "http://h:1", "", "viaparam", map[string]string{"x": "y"}},
		{"http://h:1/pathdb?database=viaparam", "http://h:1", "", "pathdb", nil},
	} {
		c, err := NewConnector(tc.dsn)
		if err != nil {
			t.Errorf("%s: %v", tc.dsn, err)
			continue
		}
		if c.baseURL != tc.base || c.authHeader != tc.auth || c.database != tc.database || !reflect.DeepEqual(c.settings, tc.settings) {
			t.Errorf("%s: base=%q auth=%q database=%q settings=%v", tc.dsn, c.baseURL, c.authHeader, c.database, c.settings)
		}
	}
}

func TestTransportProtocols(t *testing.T) {
	plain := newTransport(false)
	if !plain.Protocols.UnencryptedHTTP2() || plain.Protocols.HTTP1() || plain.Protocols.HTTP2() {
		t.Errorf("http:// must be cleartext HTTP/2 only: %v", plain.Protocols)
	}
	tls := newTransport(true)
	if !tls.Protocols.HTTP2() || tls.Protocols.HTTP1() || tls.Protocols.UnencryptedHTTP2() {
		t.Errorf("https:// must be TLS HTTP/2 only: %v", tls.Protocols)
	}
	h := plain.HTTP2
	if h.MaxReadFrameSize != 1<<20 || h.MaxReceiveBufferPerStream != 8<<20 || h.MaxReceiveBufferPerConnection != 16<<20 {
		t.Errorf("HTTP/2 config = %+v", *h)
	}
}

func TestParseInsert(t *testing.T) {
	type want struct {
		database, table string
		columns         []string
		format, data    string
	}
	yes := map[string]want{
		"INSERT INTO t FORMAT JSONEachRow\n{\"a\":1}\n":                {"", "t", nil, "JSONEachRow", "{\"a\":1}\n"},
		"  insert  into  db.t  format  CSV \n1,2\n":                    {"db", "t", nil, "CSV", "1,2\n"},
		"INSERT INTO t (a, b) FORMAT TSV\r\n1\t2\n":                    {"", "t", []string{"a", "b"}, "TSV", "1\t2\n"},
		"INSERT INTO t(a,b)FORMAT TSV\n1\t2":                           {"", "t", []string{"a", "b"}, "TSV", "1\t2"},
		"INSERT INTO `my db`.`my``t` (`a b`, \"c\") FORMAT CSV\nx\n":   {"my db", "my`t", []string{"a b", "c"}, "CSV", "x\n"},
		"INSERT INTO \"d\"\"q\".t FORMAT CSV\nx\n":                     {"d\"q", "t", nil, "CSV", "x\n"},
		"INSERT INTO `a\\`b` FORMAT CSV\nx\n":                          {"", "a`b", nil, "CSV", "x\n"},
		"INSERT INTO t FORMAT JSONEachRow {\"a\":1}":                   {"", "t", nil, "JSONEachRow", "{\"a\":1}"}, // data after the blanks, no newline
		"INSERT INTO t FORMAT JSONEachRow \n\n{\"a\":1}":               {"", "t", nil, "JSONEachRow", "\n{\"a\":1}"},
		"INSERT INTO t FORMAT CSV\n  leading,blanks\n":                 {"", "t", nil, "CSV", "  leading,blanks\n"},
		"INSERT INTO _t1.x_2 FORMAT RowBinaryWithNamesAndTypes\n\x01":  {"_t1", "x_2", nil, "RowBinaryWithNamesAndTypes", "\x01"},
		"INSERT INTO t FORMAT JSONEachRow\n{\"q\":\"what? FORMAT\"}\n": {"", "t", nil, "JSONEachRow", "{\"q\":\"what? FORMAT\"}\n"},
	}
	for sql, w := range yes {
		got, ok := parseInsert(sql)
		if !ok {
			t.Errorf("%q: not recognised", sql)
			continue
		}
		if got.database != w.database || got.table != w.table || !reflect.DeepEqual(got.columns, w.columns) || got.format != w.format || got.data != w.data {
			t.Errorf("%q: got %+v, want %+v", sql, got, w)
		}
	}
	for _, sql := range []string{
		"",
		"SELECT 1",
		"INSERT INTO t VALUES (1)",
		"INSERT INTO t (a) VALUES (1)",
		"INSERT INTO t SELECT 1",
		"INSERT INTO t (a, b) SELECT 1, 2",
		"INSERT INTO t FORMAT JSONEachRow",
		"INSERT INTO t FORMAT JSONEachRow\n",
		"INSERT INTO t FORMAT JSONEachRow \n \n",
		"INSERT INTO t FORMAT",
		"INSERT INTO t FORMAT \nx",
		"INSERT INTO t FORMAT CSV;\nx",
		"INSERT INTO t SETTINGS async_insert=1 FORMAT CSV\nx\n",
		"INSERT INTO TABLE t FORMAT CSV\nx\n",
		"INSERT INTO FUNCTION file('x') FORMAT CSV\nx\n",
		"INSERT INTO t (a, ) FORMAT CSV\nx\n",
		"INSERT INTO t (a b) FORMAT CSV\nx\n",
		"INSERT INTO t (a FORMAT CSV\nx\n",
		"INSERT INTO t () FORMAT CSV\nx\n",
		"INSERT INTO `t FORMAT CSV\nx\n",
		"INSERT INTO `` FORMAT CSV\nx\n",
		"INSERT INTO `a\\qb` FORMAT CSV\nx\n",
		"INSERT INTO db. FORMAT CSV\nx\n",
		"INSERT INTO db..t FORMAT CSV\nx\n",
		"INSERT INTO 1t FORMAT CSV\nx\n",
		"INSERT INTOt FORMAT CSV\nx\n",
		"INSERTINTO t FORMAT CSV\nx\n",
		"INSERT INTO tFORMAT CSV\nx\n",
		"/* c */ INSERT INTO t FORMAT CSV\nx\n",
		"-- c\nINSERT INTO t FORMAT CSV\nx\n",
		"WITH 1 AS x INSERT INTO t FORMAT CSV\nx\n",
	} {
		if got, ok := parseInsert(sql); ok {
			t.Errorf("%q: recognised as %+v", sql, got)
		}
	}
}
