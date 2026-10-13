package chdbsql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"math"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

// One embedded server exists per process and all connections must share its path, so every test uses one dir.
var dataDir string

func TestMain(m *testing.M) {
	var err error
	dataDir, err = os.MkdirTemp("", "chdbsql")
	if err != nil {
		panic(err)
	}
	code := m.Run()
	os.RemoveAll(dataDir)
	os.Exit(code)
}

func openWriter(t *testing.T) *sql.DB {
	t.Helper()
	db := sql.OpenDB(NewConnector("file:" + dataDir))
	db.SetMaxOpenConns(1)
	if err := db.PingContext(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func openReaders(t *testing.T, n int) *sql.DB {
	t.Helper()
	db := sql.OpenDB(NewConnector("file:" + dataDir + "?mode=ro"))
	db.SetMaxOpenConns(n)
	t.Cleanup(func() { db.Close() })
	return db
}

func TestBindOnlyReplacesPlaceholdersOutsideQuotesAndComments(t *testing.T) {
	args := func(v ...any) []driver.NamedValue {
		out := make([]driver.NamedValue, len(v))
		for i, x := range v {
			out[i] = driver.NamedValue{Ordinal: i + 1, Value: x}
		}
		return out
	}
	tests := []struct {
		name, q string
		args    []driver.NamedValue
		want    string
		wantErr bool
	}{
		{"plain", "SELECT ?, ?", args(int64(1), "a'b\\c"), `SELECT 1, 'a\'b\\c'`, false},
		{"question mark in string and ident", "SELECT '?', \"?\", `?`, ?", args(int64(2)), "SELECT '?', \"?\", `?`, 2", false},
		{"doubled quote stays in string", "SELECT 'it''s ?', ?", args(int64(3)), "SELECT 'it''s ?', 3", false},
		{"escaped quote stays in string", `SELECT 'it\'s ?', ?`, args(int64(4)), `SELECT 'it\'s ?', 4`, false},
		{"comments", "SELECT ? -- why ?\n, ? /* a ? */", args(int64(5), true), "SELECT 5 -- why ?\n, true /* a ? */", false},
		{"no args leaves ternary and data alone", "SELECT a ? 1 : 2", nil, "SELECT a ? 1 : 2", false},
		{"null and bytes", "SELECT ?, ?", args(nil, []byte{0xde, 0xad}), "SELECT NULL, unhex('dead')", false},
		{"nan inf", "SELECT ?, ?, ?", args(math.NaN(), math.Inf(1), math.Inf(-1)), "SELECT nan, inf, -inf", false},
		{"time micro", "SELECT ?", args(time.Date(2026, 10, 1, 1, 2, 3, 456000, time.UTC)), "SELECT toDateTime64('2026-10-01 01:02:03.000456', 6, 'UTC')", false},
		{"time nano", "SELECT ?", args(time.Date(2026, 10, 1, 1, 2, 3, 456789, time.UTC)), "SELECT toDateTime64('2026-10-01 01:02:03.000456789', 9, 'UTC')", false},
		{"slice", "SELECT ?", args([]string{"a", "b'"}), `SELECT ['a','b\'']`, false},
		{"too many args", "SELECT ?", args(int64(1), int64(2)), "", true},
		{"too few args", "SELECT ?, ?", args(int64(1)), "", true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := bind(tc.q, tc.args)
			if (err != nil) != tc.wantErr {
				t.Fatalf("err = %v, wantErr %v", err, tc.wantErr)
			}
			if got != tc.want {
				t.Fatalf("got  %s\nwant %s", got, tc.want)
			}
		})
	}
}

func TestRoundTripDecodesEveryColumnType(t *testing.T) {
	w := openWriter(t)
	ctx := context.Background()
	if _, err := w.ExecContext(ctx, `CREATE TABLE types_t (
		id UInt64, i Int32, big UInt64, f Float64, s String, lc LowCardinality(String), n Nullable(String), b Bool,
		d Date, dt DateTime64(6,'Europe/Berlin'), dec Decimal(10,2), tags Map(String,String), arr Array(String)
	) ENGINE = MergeTree ORDER BY id`); err != nil {
		t.Fatal(err)
	}
	ts := time.Date(2026, 10, 1, 12, 30, 45, 123456000, time.UTC)
	// Positional arguments and an inline data block that contains "?" with no arguments.
	if _, err := w.ExecContext(ctx, `INSERT INTO types_t VALUES (?, ?, ?, ?, ?, ?, ?, ?, '2026-10-01', ?, 12.5, map('k','v'), ?)`,
		1, -7, uint64(math.MaxUint64), 1.5, "it's \\ ok", "lc", nil, true, ts, []string{"a", "b"}); err != nil {
		t.Fatal(err)
	}
	if _, err := w.ExecContext(ctx, "INSERT INTO types_t (id, s, n) FORMAT JSONEachRow\n{\"id\":2,\"s\":\"what? yes\",\"n\":\"x\"}\n"); err != nil {
		t.Fatal(err)
	}

	row := w.QueryRowContext(ctx, `SELECT id, i, big, f, s, lc, n, b, d, dt, dec, tags, arr FROM types_t WHERE id = ?`, 1)
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
	// ClickHouse's JSON output writes Decimal(10,2) 12.50 as the number 12.5; the driver keeps that text as is.
	if dec != "12.5" || tgs != `{"k":"v"}` || arr != `["a","b"]` {
		t.Fatalf("dec=%q tags=%q arr=%q", dec, tgs, arr)
	}

	row = w.QueryRowContext(ctx, `SELECT s, n FROM types_t WHERE id = 2`)
	if err := row.Scan(&s, &n); err != nil || s != "what? yes" || n.String != "x" {
		t.Fatalf("inline insert: s=%q n=%v err=%v", s, n, err)
	}
	var nan float64
	if err := w.QueryRowContext(ctx, `SELECT nan`).Scan(&nan); err != nil || !math.IsNaN(nan) {
		t.Fatalf("nan = %v err=%v", nan, err)
	}
}

func TestZeroRowResultStillReportsColumns(t *testing.T) {
	w := openWriter(t)
	rows, err := w.QueryContext(context.Background(), `SELECT number AS a, toString(number) AS b FROM numbers(3) WHERE number > 10`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil || strings.Join(cols, ",") != "a,b" {
		t.Fatalf("cols = %v err = %v", cols, err)
	}
	ct, _ := rows.ColumnTypes()
	if ct[0].DatabaseTypeName() != "UInt64" || ct[1].DatabaseTypeName() != "String" {
		t.Fatalf("types = %v, %v", ct[0].DatabaseTypeName(), ct[1].DatabaseTypeName())
	}
	if rows.Next() {
		t.Fatal("unexpected row")
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
}

func TestLargeResultStreamsAcrossChunksIntact(t *testing.T) {
	w := openWriter(t)
	const n = 300000
	rows, err := w.QueryContext(context.Background(), `SELECT number, repeat('x', 20) FROM numbers(300000)`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var count, sum uint64
	for rows.Next() {
		var num uint64
		var pad string
		if err := rows.Scan(&num, &pad); err != nil {
			t.Fatal(err)
		}
		if pad != strings.Repeat("x", 20) || num != count {
			t.Fatalf("row %d: num=%d pad=%q", count, num, pad)
		}
		sum += num
		count++
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if count != n || sum != uint64(n)*(n-1)/2 {
		t.Fatalf("count=%d sum=%d", count, sum)
	}
}

func TestErrorsAreReturnedAndConnectionStaysUsable(t *testing.T) {
	w := openWriter(t)
	ctx := context.Background()
	if _, err := w.ExecContext(ctx, "SELEC 1"); err == nil || !strings.Contains(err.Error(), "SYNTAX_ERROR") {
		t.Fatalf("exec err = %v", err)
	}
	if _, err := w.QueryContext(ctx, "SELECT * FROM no_such_table"); err == nil || !strings.Contains(err.Error(), "UNKNOWN_TABLE") {
		t.Fatalf("query err = %v", err)
	}
	var one int64
	if err := w.QueryRowContext(ctx, "SELECT 1").Scan(&one); err != nil || one != 1 {
		t.Fatalf("after errors: %v %v", one, err)
	}
}

// A stream that fails or is closed before its end leaves the query inside libchdb; the next statement on the same
// connection used to read that query's header. The connection must be discarded instead.
func TestUnfinishedStreamDiscardsTheConnection(t *testing.T) {
	openWriter(t) // opened first: it starts the server
	r := sql.OpenDB(NewConnector("file:" + dataDir + "?mode=ro&max_memory_usage=128000000"))
	r.SetMaxOpenConns(1)
	t.Cleanup(func() { r.Close() })
	ctx := context.Background()
	next := func() {
		t.Helper()
		c, err := r.Conn(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()
		var a, b, c3, d, e, f int64
		if err := c.QueryRowContext(ctx, "SELECT 1, 2, 3, 4, 5, 6").Scan(&a, &b, &c3, &d, &e, &f); err != nil || f != 6 {
			t.Fatalf("next statement: %v %v", f, err)
		}
	}
	for round := range 3 {
		// A query that fails inside the stream (memory cap).
		c, err := r.Conn(ctx)
		if err != nil {
			t.Fatal(err)
		}
		rs, err := c.QueryContext(ctx, "SELECT length(groupArray(number)) FROM numbers(200000000)")
		if err == nil {
			for rs.Next() {
			}
			err = rs.Err()
			rs.Close()
		}
		if err == nil || !strings.Contains(err.Error(), "MEMORY_LIMIT_EXCEEDED") {
			t.Fatalf("round %d: err = %v", round, err)
		}
		c.Close()
		next()

		// A stream closed after a few rows.
		c, err = r.Conn(ctx)
		if err != nil {
			t.Fatal(err)
		}
		rs, err = c.QueryContext(ctx, "SELECT number, toString(number) FROM numbers(50000000)")
		if err != nil {
			t.Fatal(err)
		}
		for range 10 {
			rs.Next()
		}
		rs.Close()
		c.Close()
		next()
	}
}

// QueryRow stops after the first row without reading the end of the stream. That is a finished result, not an abandoned
// one, so the connection stays in the pool; a stream with more rows left than were read does not.
func TestFullyReadResultKeepsTheConnection(t *testing.T) {
	openWriter(t)
	r := openReaders(t, 1)
	ctx := context.Background()
	c, err := r.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	valid := func() bool {
		ok := false
		_ = c.Raw(func(dc any) error { ok = dc.(driver.Validator).IsValid(); return nil })
		return ok
	}
	var n int64
	if err := c.QueryRowContext(ctx, "SELECT 1").Scan(&n); err != nil || n != 1 {
		t.Fatalf("query row: %v %v", n, err)
	}
	if !valid() {
		t.Fatal("a one-row result that was read completely discarded the connection")
	}
	if err := c.QueryRowContext(ctx, "SELECT number FROM numbers(50000000)").Scan(&n); err != nil {
		t.Fatal(err)
	}
	if valid() {
		t.Fatal("a stream with rows left kept the connection")
	}
}

func TestReadOnlyConnectionRejectsWrites(t *testing.T) {
	w := openWriter(t)
	if _, err := w.Exec("CREATE TABLE IF NOT EXISTS ro_t (x UInt8) ENGINE = MergeTree ORDER BY x"); err != nil {
		t.Fatal(err)
	}
	r := openReaders(t, 1)
	if _, err := r.Exec("INSERT INTO ro_t VALUES (1)"); err == nil || !strings.Contains(strings.ToLower(err.Error()), "readonly") {
		t.Fatalf("insert on mode=ro err = %v", err)
	}
	var c int64
	if err := r.QueryRow("SELECT count() FROM ro_t").Scan(&c); err != nil || c != 0 {
		t.Fatalf("read on mode=ro: %d %v", c, err)
	}
}

// Two readers and the writer each take about a second; in parallel the wall time is about one second,
// and serialised on one connection it would be at least three.
func TestWriterAndReadersRunInParallel(t *testing.T) {
	w := openWriter(t)
	if _, err := w.Exec("CREATE TABLE IF NOT EXISTS par_t (x UInt8) ENGINE = MergeTree ORDER BY x"); err != nil {
		t.Fatal(err)
	}
	r := openReaders(t, 2)
	run := func(name string, f func() error) (time.Duration, error) {
		start := time.Now()
		err := f()
		return time.Since(start), err
	}
	var wg sync.WaitGroup
	times := make([]time.Duration, 3)
	errs := make([]error, 3)
	start := time.Now()
	for i := range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			times[i], errs[i] = run("reader", func() error {
				var z int64
				return r.QueryRow("SELECT sleep(1), 1").Scan(&z, new(int64))
			})
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		times[2], errs[2] = run("writer", func() error {
			_, err := w.Exec("INSERT INTO par_t SELECT sleep(1) + 1")
			return err
		})
	}()
	wg.Wait()
	wall := time.Since(start)
	for i, err := range errs {
		if err != nil {
			t.Fatalf("worker %d: %v", i, err)
		}
		if times[i] < 900*time.Millisecond {
			t.Fatalf("worker %d finished in %v, the sleep did not run", i, times[i])
		}
	}
	if wall > 2*time.Second {
		t.Fatalf("wall %v for three 1s calls: the connections were serialised", wall)
	}
	t.Logf("three 1s calls took %v in parallel", wall)
}

func TestExecContextCancelAbandonsAndDiscardsTheConnection(t *testing.T) {
	w := openWriter(t)
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err := w.ExecContext(ctx, "SELECT sleep(1)")
	if err != context.DeadlineExceeded {
		t.Fatalf("err = %v, want deadline exceeded", err)
	}
	if time.Since(start) > 600*time.Millisecond {
		t.Fatalf("cancel took %v", time.Since(start))
	}
	// The abandoned call finishes in the background; the pool opens a fresh connection and carries on.
	var one int64
	if err := w.QueryRow("SELECT 1").Scan(&one); err != nil || one != 1 {
		t.Fatalf("after cancel: %v %v", one, err)
	}
}

func TestNamedArgumentsAndTransactionsAreRejected(t *testing.T) {
	w := openWriter(t)
	if _, err := w.Exec("SELECT @a", sql.Named("a", 1)); err == nil || !strings.Contains(err.Error(), "named arguments") {
		t.Fatalf("named: %v", err)
	}
	if _, err := w.Begin(); err == nil || !strings.Contains(err.Error(), "transactions") {
		t.Fatalf("begin: %v", err)
	}
}
