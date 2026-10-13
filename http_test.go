package chdbsql

import (
	"context"
	"database/sql"
	"errors"
	"math"
	"net"
	"net/http"
	"net/url"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// The HTTP driver tests need a ClickHouse server. Set CHDBSQL_HTTP_DSN to its HTTP interface, for example
//
//	nerdctl run -d -p 18123:8123 -e CLICKHOUSE_USER=test -e CLICKHOUSE_PASSWORD=test clickhouse/clickhouse-server
//	CHDBSQL_HTTP_DSN=http://test:test@localhost:18123/default go test -run HTTP
//
// Without it they are skipped.
func httpDSN(t *testing.T, settings map[string]string) string {
	t.Helper()
	dsn := os.Getenv("CHDBSQL_HTTP_DSN")
	if dsn == "" {
		t.Skip("CHDBSQL_HTTP_DSN is not set")
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

func openHTTP(t *testing.T, settings map[string]string, maxConns int) (*sql.DB, *HTTPConnector) {
	t.Helper()
	c, err := NewHTTPConnector(httpDSN(t, settings))
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

// The HTTP driver decodes like the embedded one: the same table, the same statements, the same Go values.
func TestHTTPRoundTripDecodesEveryColumnType(t *testing.T) {
	w, _ := openHTTP(t, nil, 1)
	ctx := context.Background()
	for _, q := range []string{
		`DROP TABLE IF EXISTS types_http`,
		`CREATE TABLE types_http (
			id UInt64, i Int32, big UInt64, f Float64, s String, lc LowCardinality(String), n Nullable(String), b Bool,
			d Date, dt DateTime64(6,'Europe/Berlin'), dec Decimal(10,2), tags Map(String,String), arr Array(String)
		) ENGINE = MergeTree ORDER BY id`,
	} {
		if _, err := w.ExecContext(ctx, q); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() { w.ExecContext(context.Background(), `DROP TABLE IF EXISTS types_http`) })
	ts := time.Date(2026, 10, 1, 12, 30, 45, 123456000, time.UTC)
	if _, err := w.ExecContext(ctx, `INSERT INTO types_http VALUES (?, ?, ?, ?, ?, ?, ?, ?, '2026-10-01', ?, 12.5, map('k','v'), ?)`,
		1, -7, uint64(math.MaxUint64), 1.5, "it's \\ ok", "lc", nil, true, ts, []string{"a", "b"}); err != nil {
		t.Fatal(err)
	}
	// Inline rows after FORMAT JSONEachRow go out verbatim, a "?" in them included.
	if _, err := w.ExecContext(ctx, "INSERT INTO types_http (id, s, n) FORMAT JSONEachRow\n{\"id\":2,\"s\":\"what? yes\",\"n\":\"x\"}\n"); err != nil {
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
	row := w.QueryRowContext(ctx, `SELECT id, i, big, f, s, lc, n, b, d, dt, dec, tags, arr FROM types_http WHERE id = ?`, 1)
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
	if err := w.QueryRowContext(ctx, `SELECT s, n FROM types_http WHERE id = 2`).Scan(&s, &n); err != nil || s != "what? yes" || n.String != "x" {
		t.Fatalf("inline insert: s=%q n=%v err=%v", s, n, err)
	}
	var nan float64
	if err := w.QueryRowContext(ctx, `SELECT nan`).Scan(&nan); err != nil || !math.IsNaN(nan) {
		t.Fatalf("nan = %v err=%v", nan, err)
	}
}

func TestHTTPZeroRowResultStillReportsColumns(t *testing.T) {
	db, _ := openHTTP(t, nil, 1)
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

// ClickHouse answers 200 and then writes the error into the body. A reader that stops at the status would take the rows
// before it for the whole result.
func TestHTTPErrorInTheMiddleOfAResultIsAnError(t *testing.T) {
	db, _ := openHTTP(t, nil, 1)
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
	if !errors.As(err, &se) || se.Code != 395 || se.Status != http.StatusOK {
		t.Fatalf("after %d rows: err = %v, want ServerError code 395 inside a 200 response", n, err)
	}
	if n == 0 || n >= 1000000 {
		t.Fatalf("rows before the error = %d", n)
	}
}

func TestHTTPErrorStatusCarriesTheServerError(t *testing.T) {
	db, _ := openHTTP(t, nil, 1)
	ctx := context.Background()
	_, err := db.ExecContext(ctx, `SELEKT 1`)
	var se *ServerError
	if !errors.As(err, &se) || se.Code != 62 || se.Status == http.StatusOK {
		t.Fatalf("syntax error: %v", err)
	}
	_, err = db.QueryContext(ctx, `SELECT * FROM no_such_table_here`)
	if !errors.As(err, &se) || se.Code != 60 {
		t.Fatalf("unknown table: %v", err)
	}
	if err := db.QueryRowContext(ctx, `SELECT 1`).Scan(new(int)); err != nil {
		t.Fatalf("the pool is unusable after errors: %v", err)
	}
}

// A read-only HTTP connection still carries the settings of its DSN, which an embedded read-only connection cannot.
func TestHTTPReadOnlyConnectionRejectsWritesAndKeepsSettings(t *testing.T) {
	w, _ := openHTTP(t, nil, 1)
	ctx := context.Background()
	if _, err := w.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS ro_http (a Int8) ENGINE = Memory`); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { w.ExecContext(context.Background(), `DROP TABLE IF EXISTS ro_http`) })
	r, _ := openHTTP(t, map[string]string{"readonly": "1", "max_execution_time": "1"}, 2)
	var se *ServerError
	if _, err := r.ExecContext(ctx, `INSERT INTO ro_http VALUES (1)`); !errors.As(err, &se) || se.Code != 164 {
		t.Fatalf("write on a read-only connection: %v", err)
	}
	if err := r.QueryRowContext(ctx, `SELECT count() FROM ro_http`).Scan(new(int)); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	err := r.QueryRowContext(ctx, `SELECT sleep(3)`).Scan(new(int))
	if !errors.As(err, &se) || se.Code != 159 || time.Since(start) > 2500*time.Millisecond {
		t.Fatalf("max_execution_time of the DSN did not apply: %v after %v", err, time.Since(start))
	}
}

// A result read to its last row, also by QueryRow, lets the HTTP connection go back to the pool; a statement run in a
// loop therefore does not open a TCP connection each time.
func TestHTTPFullyReadResultKeepsTheTCPConnection(t *testing.T) {
	db, c := openHTTP(t, nil, 1)
	var dials atomic.Int32
	tr := c.client.Transport.(*http.Transport)
	dial := tr.DialContext
	if dial == nil {
		dial = (&net.Dialer{}).DialContext
	}
	tr.DialContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
		dials.Add(1)
		return dial(ctx, network, addr)
	}
	tr.CloseIdleConnections()
	ctx := context.Background()
	for range 50 {
		if err := db.QueryRowContext(ctx, `SELECT 1`).Scan(new(int)); err != nil {
			t.Fatal(err)
		}
	}
	if n := dials.Load(); n > 2 {
		t.Fatalf("50 sequential QueryRow calls opened %d TCP connections", n)
	}
}

func TestHTTPLargeResultStreamsIntact(t *testing.T) {
	db, _ := openHTTP(t, nil, 1)
	rs, err := db.QueryContext(context.Background(), `SELECT number, toString(number) FROM numbers(300000)`)
	if err != nil {
		t.Fatal(err)
	}
	defer rs.Close()
	var sum, rows int64
	for rs.Next() {
		var n int64
		var s string
		if err := rs.Scan(&n, &s); err != nil {
			t.Fatal(err)
		}
		sum += n
		rows++
	}
	if err := rs.Err(); err != nil || rows != 300000 || sum != 300000*299999/2 {
		t.Fatalf("rows=%d sum=%d err=%v", rows, sum, err)
	}
}

// Closing a result before its end aborts the request and the pool stays usable.
func TestHTTPUnfinishedResultIsAbortedAndPoolStaysUsable(t *testing.T) {
	db, _ := openHTTP(t, nil, 1)
	ctx := context.Background()
	rs, err := db.QueryContext(ctx, `SELECT number FROM numbers(100000000)`)
	if err != nil {
		t.Fatal(err)
	}
	if !rs.Next() {
		t.Fatal("no first row")
	}
	start := time.Now()
	rs.Close()
	if time.Since(start) > 2*time.Second {
		t.Fatalf("Close took %v", time.Since(start))
	}
	var one int
	if err := db.QueryRowContext(ctx, `SELECT 1`).Scan(&one); err != nil || one != 1 {
		t.Fatalf("next query: %v %v", one, err)
	}
}

func TestHTTPContextCancelEndsARunningStatement(t *testing.T) {
	db, _ := openHTTP(t, nil, 1)
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

func TestHTTPParallelReaders(t *testing.T) {
	db, _ := openHTTP(t, map[string]string{"readonly": "1"}, 8)
	var wg sync.WaitGroup
	errs := make(chan error, 16)
	for g := range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var sum int64
			errs <- db.QueryRowContext(context.Background(), `SELECT sum(number) + ? FROM numbers(100000)`, g).Scan(&sum)
			if sum != 100000*99999/2+int64(g) {
				errs <- errors.New("wrong sum")
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestHTTPDSNMustBeAnHTTPURL(t *testing.T) {
	for _, dsn := range []string{"file:/data", "localhost:8123", "http://", ":::"} {
		if _, err := NewHTTPConnector(dsn); err == nil {
			t.Errorf("NewHTTPConnector(%q) accepted the DSN", dsn)
		}
	}
	c, err := NewHTTPConnector("https://u:p%40ss@ch.example:8443/logs?readonly=1")
	if err != nil {
		t.Fatal(err)
	}
	if c.endpoint != "https://ch.example:8443/" || c.user != "u" || c.password != "p@ss" || c.params.Get("database") != "logs" || c.params.Get("readonly") != "1" {
		t.Fatalf("parsed %+v", c)
	}
}
