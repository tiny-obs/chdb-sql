package chconnect

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"

	chdbv1 "github.com/tiny-obs/chdb-sql/internal/gen/chdb/v1"
	"github.com/tiny-obs/chdb-sql/internal/gen/chdb/v1/chdbv1connect"
)

// These tests run against an in-process fake of the Chdb service over cleartext HTTP/2, so they need no chdb-server.
// They cover what the driver itself decides: line framing across chunks, error mapping, cancellation, routing of
// INSERT statements, headers, and compression.

type fakeInsert struct {
	header *chdbv1.InsertHeader
	data   []byte
	chunks []int // sizes of the data messages
	auth   string
}

type fake struct {
	chdbv1connect.UnimplementedChdbHandler

	mu      sync.Mutex
	auth    []string // Authorization header of every request
	protos  []string
	encoded []string // every compression-related header seen
	queries []*chdbv1.QueryRequest
	execs   []*chdbv1.ExecRequest
	inserts []fakeInsert

	query  func(ctx context.Context, req *chdbv1.QueryRequest, s *connect.ServerStream[chdbv1.QueryChunk]) error
	insert func(h *chdbv1.InsertHeader) error // called with the header; a non-nil error ends the call
	exec   func(req *chdbv1.ExecRequest) error
}

func (f *fake) record(h http.Header) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.auth = append(f.auth, h.Get("Authorization"))
	for _, k := range []string{"Accept-Encoding", "Content-Encoding", "Connect-Accept-Encoding", "Connect-Content-Encoding", "Grpc-Accept-Encoding", "Grpc-Encoding"} {
		if v := h.Get(k); v != "" {
			f.encoded = append(f.encoded, k+": "+v)
		}
	}
}

func (f *fake) Query(ctx context.Context, req *connect.Request[chdbv1.QueryRequest], s *connect.ServerStream[chdbv1.QueryChunk]) error {
	f.record(req.Header())
	f.mu.Lock()
	f.queries = append(f.queries, req.Msg)
	f.mu.Unlock()
	return f.query(ctx, req.Msg, s)
}

func (f *fake) Exec(_ context.Context, req *connect.Request[chdbv1.ExecRequest]) (*connect.Response[chdbv1.ExecResponse], error) {
	f.record(req.Header())
	f.mu.Lock()
	f.execs = append(f.execs, req.Msg)
	f.mu.Unlock()
	if f.exec != nil {
		if err := f.exec(req.Msg); err != nil {
			return nil, err
		}
	}
	return connect.NewResponse(&chdbv1.ExecResponse{}), nil
}

func (f *fake) Insert(_ context.Context, s *connect.ClientStream[chdbv1.InsertChunk]) (*connect.Response[chdbv1.InsertResponse], error) {
	f.record(s.RequestHeader())
	if !s.Receive() {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("no header"))
	}
	in := fakeInsert{header: s.Msg().GetHeader(), auth: s.RequestHeader().Get("Authorization")}
	if f.insert != nil {
		if err := f.insert(in.header); err != nil {
			return nil, err
		}
	}
	for s.Receive() {
		d := s.Msg().GetData()
		in.data = append(in.data, d...)
		in.chunks = append(in.chunks, len(d))
	}
	if err := s.Err(); err != nil {
		return nil, err // an aborted call writes nothing
	}
	f.mu.Lock()
	f.inserts = append(f.inserts, in)
	f.mu.Unlock()
	return connect.NewResponse(&chdbv1.InsertResponse{BytesWritten: uint64(len(in.data))}), nil
}

func (f *fake) start(t *testing.T) string {
	t.Helper()
	mux := http.NewServeMux()
	path, h := chdbv1connect.NewChdbHandler(f)
	mux.Handle(path, h)
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		f.protos = append(f.protos, r.Proto)
		f.mu.Unlock()
		mux.ServeHTTP(w, r)
	}))
	srv.Config.Protocols = new(http.Protocols)
	srv.Config.Protocols.SetHTTP1(true)
	srv.Config.Protocols.SetUnencryptedHTTP2(true)
	srv.Start()
	t.Cleanup(srv.Close)
	return srv.URL
}

func openFake(t *testing.T, f *fake, dsnTail string) (*sql.DB, *Connector) {
	t.Helper()
	url := f.start(t)
	if i := strings.Index(url, "://"); dsnTail != "" && strings.HasPrefix(dsnTail, "@") {
		url = url[:i+3] + dsnTail[1:] + "@" + url[i+3:]
		dsnTail = ""
	}
	c, err := NewConnector(url + dsnTail)
	if err != nil {
		t.Fatal(err)
	}
	db := sql.OpenDB(c)
	t.Cleanup(func() { db.Close() })
	return db, c
}

// document is a two-column result of n rows in the driver's wire format.
func document(n int) []byte {
	var b bytes.Buffer
	b.WriteString("[\"id\",\"s\"]\n[\"UInt64\",\"String\"]\n")
	for i := range n {
		fmt.Fprintf(&b, "[%d,\"row-%d\"]\n", i, i)
	}
	return b.Bytes()
}

func sendInPieces(s *connect.ServerStream[chdbv1.QueryChunk], doc []byte, size int) error {
	for len(doc) > 0 {
		n := min(size, len(doc))
		if err := s.Send(&chdbv1.QueryChunk{Data: doc[:n]}); err != nil {
			return err
		}
		doc = doc[n:]
	}
	return nil
}

// A chunk boundary can fall anywhere, also inside a number or in front of a newline.
func TestFakeQueryReassemblesLinesAcrossChunks(t *testing.T) {
	doc := document(2000)
	for _, size := range []int{1, 3, 7, 100, 4096, len(doc)} {
		f := &fake{query: func(_ context.Context, _ *chdbv1.QueryRequest, s *connect.ServerStream[chdbv1.QueryChunk]) error {
			return sendInPieces(s, doc, size)
		}}
		db, _ := openFake(t, f, "/logs?readonly=1&max_execution_time=5&database=ignored")
		rs, err := db.QueryContext(context.Background(), `SELECT id, s FROM t`)
		if err != nil {
			t.Fatal(err)
		}
		n := 0
		for rs.Next() {
			var id uint64
			var s string
			if err := rs.Scan(&id, &s); err != nil {
				t.Fatalf("chunk size %d: %v", size, err)
			}
			if int(id) != n || s != fmt.Sprintf("row-%d", n) {
				t.Fatalf("chunk size %d: row %d = %d %q", size, n, id, s)
			}
			n++
		}
		if err := rs.Err(); err != nil || n != 2000 {
			t.Fatalf("chunk size %d: %d rows, err %v", size, n, err)
		}
		rs.Close()
		f.mu.Lock()
		q := f.queries[0]
		f.mu.Unlock()
		if q.Format != "JSONCompactEachRowWithNamesAndTypes" || q.Database != "logs" || q.Sql != "SELECT id, s FROM t" ||
			len(q.Settings) != 2 || q.Settings["readonly"] != "1" || q.Settings["max_execution_time"] != "5" {
			t.Fatalf("request = %v", q)
		}
	}
}

func TestFakeQueryColumnsAndTypesBeforeTheFirstRow(t *testing.T) {
	f := &fake{query: func(_ context.Context, _ *chdbv1.QueryRequest, s *connect.ServerStream[chdbv1.QueryChunk]) error {
		return s.Send(&chdbv1.QueryChunk{Data: []byte("[\"a\",\"b\"]\n[\"UInt8\",\"Nullable(String)\"]\n")})
	}}
	db, _ := openFake(t, f, "")
	rs, err := db.QueryContext(context.Background(), `SELECT 1 WHERE 0`)
	if err != nil {
		t.Fatal(err)
	}
	defer rs.Close()
	cols, _ := rs.Columns()
	types, err := rs.ColumnTypes()
	if err != nil || len(cols) != 2 || cols[1] != "b" || types[1].DatabaseTypeName() != "Nullable(String)" {
		t.Fatalf("%v %v", cols, err)
	}
	if nullable, ok := types[1].Nullable(); !nullable || !ok {
		t.Fatal("b should be nullable")
	}
	if rs.Next() || rs.Err() != nil {
		t.Fatalf("rows: %v", rs.Err())
	}
}

// An error after output started is the error of the result, never a short result; the exception code comes along.
func TestFakeErrorAfterOutputIsAServerError(t *testing.T) {
	f := &fake{query: func(_ context.Context, _ *chdbv1.QueryRequest, s *connect.ServerStream[chdbv1.QueryChunk]) error {
		doc := document(100)
		// Cut the last line in half: the partial line must not be returned as a row.
		if err := sendInPieces(s, doc[:len(doc)-5], 64); err != nil {
			return err
		}
		e := connect.NewError(connect.CodeResourceExhausted, errors.New("Code: 241. DB::Exception: Memory limit exceeded"))
		e.Meta().Set("X-ClickHouse-Exception-Code", "241")
		return e
	}}
	db, _ := openFake(t, f, "")
	rs, err := db.QueryContext(context.Background(), `SELECT id, s FROM t`)
	if err != nil {
		t.Fatal(err)
	}
	defer rs.Close()
	n := 0
	for rs.Next() {
		n++
	}
	var se *ServerError
	var ce *connect.Error
	err = rs.Err()
	if !errors.As(err, &se) || se.Code != 241 || se.ConnectCode != connect.CodeResourceExhausted || !strings.Contains(se.Message, "Memory limit") {
		t.Fatalf("err = %v", err)
	}
	if !errors.As(err, &ce) || ce.Code() != connect.CodeResourceExhausted {
		t.Fatalf("the connect error is not reachable: %v", err)
	}
	if n != 99 {
		t.Fatalf("rows before the error = %d, want 99", n)
	}
}

func TestFakeErrorBeforeOutputFailsTheQueryAndExec(t *testing.T) {
	f := &fake{
		query: func(context.Context, *chdbv1.QueryRequest, *connect.ServerStream[chdbv1.QueryChunk]) error {
			e := connect.NewError(connect.CodeNotFound, errors.New("Code: 60. DB::Exception: Unknown table"))
			e.Meta().Set("X-ClickHouse-Exception-Code", "60")
			return e
		},
		exec: func(*chdbv1.ExecRequest) error {
			return connect.NewError(connect.CodeUnauthenticated, errors.New("bad key"))
		},
	}
	db, _ := openFake(t, f, "")
	var se *ServerError
	_, err := db.QueryContext(context.Background(), `SELECT * FROM nope`)
	if !errors.As(err, &se) || se.Code != 60 || se.ConnectCode != connect.CodeNotFound {
		t.Fatalf("query: %v", err)
	}
	_, err = db.ExecContext(context.Background(), `CREATE TABLE x (a Int8) ENGINE = Memory`)
	if !errors.As(err, &se) || se.Code != 0 || se.ConnectCode != connect.CodeUnauthenticated {
		t.Fatalf("exec: %v", err)
	}
	if err := db.PingContext(context.Background()); !errors.As(err, &se) || se.ConnectCode != connect.CodeUnauthenticated {
		t.Fatalf("ping: %v", err)
	}
}

// Closing a result early cancels the RPC, so the server sees the context end and can kill the query. A result read to
// its end does not cancel anything.
func TestFakeEarlyCloseCancelsTheRPC(t *testing.T) {
	canceled := make(chan struct{})
	f := &fake{query: func(ctx context.Context, _ *chdbv1.QueryRequest, s *connect.ServerStream[chdbv1.QueryChunk]) error {
		if err := sendInPieces(s, document(10), 1000); err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			close(canceled)
			return ctx.Err()
		case <-time.After(10 * time.Second):
			return errors.New("never cancelled")
		}
	}}
	db, _ := openFake(t, f, "")
	rs, err := db.QueryContext(context.Background(), `SELECT id, s FROM huge`)
	if err != nil {
		t.Fatal(err)
	}
	if !rs.Next() {
		t.Fatal("no first row")
	}
	start := time.Now()
	rs.Close()
	if d := time.Since(start); d > 2*time.Second {
		t.Fatalf("Close took %v", d)
	}
	select {
	case <-canceled:
	case <-time.After(3 * time.Second):
		t.Fatal("the server did not see the cancellation")
	}
}

func TestFakeCancelledContextIsTheContextError(t *testing.T) {
	f := &fake{
		query: func(ctx context.Context, _ *chdbv1.QueryRequest, s *connect.ServerStream[chdbv1.QueryChunk]) error {
			<-ctx.Done()
			return ctx.Err()
		},
		exec: func(*chdbv1.ExecRequest) error { time.Sleep(3 * time.Second); return nil },
	}
	db, _ := openFake(t, f, "")
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err := db.ExecContext(ctx, `SELECT sleep(3)`)
	if !errors.Is(err, context.DeadlineExceeded) || time.Since(start) > 2*time.Second {
		t.Fatalf("exec: %v after %v", err, time.Since(start))
	}
	ctx2, cancel2 := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel2()
	_, err = db.QueryContext(ctx2, `SELECT sleep(3)`)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("query: %v", err)
	}
}

// The Bearer key is the DSN password and goes on every RPC; without a password nothing is sent, and the user name does
// not matter.
func TestFakeAuthorizationHeader(t *testing.T) {
	for _, tc := range []struct{ userinfo, want string }{
		{"@:s3cret", "Bearer s3cret"},
		{"@ignored:s3cret", "Bearer s3cret"},
		{"@onlyuser", ""},
		{"", ""},
	} {
		f := &fake{query: func(_ context.Context, _ *chdbv1.QueryRequest, s *connect.ServerStream[chdbv1.QueryChunk]) error {
			return sendInPieces(s, document(1), 100)
		}}
		db, c := openFake(t, f, tc.userinfo)
		ctx := context.Background()
		if err := db.QueryRowContext(ctx, `SELECT 1`).Scan(new(uint64), new(string)); err != nil {
			t.Fatal(err)
		}
		if _, err := db.ExecContext(ctx, `SELECT 1`); err != nil {
			t.Fatal(err)
		}
		if _, err := c.Insert(ctx, InsertOptions{Table: "t"}, strings.NewReader("{}\n")); err != nil {
			t.Fatal(err)
		}
		f.mu.Lock()
		if len(f.auth) != 3 {
			t.Fatalf("%d requests", len(f.auth))
		}
		for i, a := range f.auth {
			if a != tc.want {
				t.Errorf("%q: request %d sent Authorization %q, want %q", tc.userinfo, i, a, tc.want)
			}
		}
		f.mu.Unlock()
	}
}

// The driver always speaks HTTP/2, and never asks for or sends gzip.
func TestFakeUsesHTTP2AndNoCompression(t *testing.T) {
	f := &fake{query: func(_ context.Context, _ *chdbv1.QueryRequest, s *connect.ServerStream[chdbv1.QueryChunk]) error {
		return sendInPieces(s, document(1), 100)
	}}
	db, c := openFake(t, f, "")
	ctx := context.Background()
	if err := db.QueryRowContext(ctx, `SELECT 1`).Scan(new(uint64), new(string)); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `SELECT 1`); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Insert(ctx, InsertOptions{Table: "t"}, strings.NewReader(strings.Repeat("{\"a\":1}\n", 100000))); err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, p := range f.protos {
		if p != "HTTP/2.0" {
			t.Errorf("request over %s", p)
		}
	}
	if len(f.protos) != 3 {
		t.Errorf("%d requests", len(f.protos))
	}
	for _, e := range f.encoded {
		if strings.Contains(e, "gzip") {
			t.Errorf("compression header: %s", e)
		}
	}
}

func TestFakeExecRoutesInsertFormatToTheInsertRPC(t *testing.T) {
	f := &fake{}
	db, _ := openFake(t, f, "/dflt?async_insert=0")
	ctx := context.Background()
	rows := "{\"id\":1,\"s\":\"what? yes\"}\n{\"id\":2,\"s\":\"b\"}\n"
	if _, err := db.ExecContext(ctx, "insert into logs.`my ``t` (id, \"s\") format JSONEachRow\n"+rows); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, "INSERT INTO t FORMAT CSV\n1,\"a\"\n"); err != nil {
		t.Fatal(err)
	}
	// Not routed: arguments, VALUES, SELECT, no data, SETTINGS.
	for _, q := range []string{
		"INSERT INTO t VALUES (1)", "INSERT INTO t SELECT 1", "INSERT INTO t FORMAT JSONEachRow", "INSERT INTO t FORMAT JSONEachRow\n \n",
		"INSERT INTO t SETTINGS async_insert=1 FORMAT JSONEachRow\n{}\n",
	} {
		if _, err := db.ExecContext(ctx, q); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := db.ExecContext(ctx, "INSERT INTO t FORMAT JSONEachRow\n{\"a\":?}\n", 5); err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.inserts) != 2 {
		t.Fatalf("%d inserts", len(f.inserts))
	}
	h := f.inserts[0].header
	if h.Database != "logs" || h.Table != "my `t" || len(h.Columns) != 2 || h.Columns[0] != "id" || h.Columns[1] != "s" ||
		h.Format != "JSONEachRow" || h.Settings["async_insert"] != "0" || string(f.inserts[0].data) != rows {
		t.Fatalf("insert 0 = %v %q", h, f.inserts[0].data)
	}
	h = f.inserts[1].header
	if h.Database != "dflt" || h.Table != "t" || len(h.Columns) != 0 || h.Format != "CSV" || string(f.inserts[1].data) != "1,\"a\"\n" {
		t.Fatalf("insert 1 = %v %q", h, f.inserts[1].data)
	}
	if len(f.execs) != 6 {
		t.Fatalf("%d execs", len(f.execs))
	}
	if f.execs[5].Sql != "INSERT INTO t FORMAT JSONEachRow\n{\"a\":5}\n" || f.execs[5].Database != "dflt" {
		t.Fatalf("exec with an argument = %q", f.execs[5].Sql)
	}
}

func TestFakeInsertAPIChunksTheDataAndMergesOptions(t *testing.T) {
	f := &fake{}
	_, c := openFake(t, f, "/dflt?max_insert_threads=2&async_insert=0")
	data := bytes.Repeat([]byte("x"), 2<<20+12345)
	written, err := c.Insert(context.Background(), InsertOptions{
		Table: "t", Columns: []string{"a", "b"}, Format: "CSV", Settings: map[string]string{"async_insert": "1"},
	}, bytes.NewReader(data))
	if err != nil || written != uint64(len(data)) {
		t.Fatalf("written = %d, err = %v", written, err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	in := f.inserts[0]
	h := in.header
	if h.Table != "t" || h.Format != "CSV" || h.Database != "dflt" || len(h.Columns) != 2 ||
		h.Settings["max_insert_threads"] != "2" || h.Settings["async_insert"] != "1" {
		t.Fatalf("header = %v", h)
	}
	if len(in.chunks) != 3 || in.chunks[0] != 1<<20 || in.chunks[1] != 1<<20 || in.chunks[2] != 12345 || !bytes.Equal(in.data, data) {
		t.Fatalf("chunks = %v", in.chunks)
	}
	// An explicit database wins over the DSN's.
	f.mu.Unlock()
	_, err = c.Insert(context.Background(), InsertOptions{Table: "t", Database: "other"}, strings.NewReader("{}\n"))
	f.mu.Lock()
	if err != nil || f.inserts[1].header.Database != "other" {
		t.Fatalf("err = %v", err)
	}
	if _, err := c.Insert(context.Background(), InsertOptions{}, strings.NewReader("{}\n")); err == nil {
		t.Fatal("an insert without a table was accepted")
	}
}

// The server can end an Insert early, for example because the table is unknown. Send then reports io.EOF and the real
// error is the response.
func TestFakeInsertServerErrorIsReturned(t *testing.T) {
	f := &fake{insert: func(*chdbv1.InsertHeader) error {
		e := connect.NewError(connect.CodeNotFound, errors.New("Code: 60. DB::Exception: Unknown table"))
		e.Meta().Set("X-ClickHouse-Exception-Code", "60")
		return e
	}}
	_, c := openFake(t, f, "")
	var se *ServerError
	_, err := c.Insert(context.Background(), InsertOptions{Table: "nope"}, bytes.NewReader(bytes.Repeat([]byte("{}\n"), 3<<20)))
	if !errors.As(err, &se) || se.Code != 60 || se.ConnectCode != connect.CodeNotFound {
		t.Fatalf("err = %v", err)
	}
}

type failingReader struct {
	n   int
	err error
}

func (r *failingReader) Read(p []byte) (int, error) {
	if r.n == 0 {
		return 0, r.err
	}
	n := min(len(p), r.n)
	r.n -= n
	return n, nil
}

// A data reader that fails aborts the call and nothing is inserted.
func TestFakeInsertReaderErrorAbortsTheCall(t *testing.T) {
	f := &fake{}
	_, c := openFake(t, f, "")
	boom := errors.New("boom")
	_, err := c.Insert(context.Background(), InsertOptions{Table: "t"}, &failingReader{n: 3 << 20, err: boom})
	if !errors.Is(err, boom) {
		t.Fatalf("err = %v", err)
	}
	time.Sleep(100 * time.Millisecond)
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.inserts) != 0 {
		t.Fatalf("%d inserts were written", len(f.inserts))
	}
}

func TestFakeStatementsAreStatelessAndParallel(t *testing.T) {
	f := &fake{query: func(_ context.Context, _ *chdbv1.QueryRequest, s *connect.ServerStream[chdbv1.QueryChunk]) error {
		return sendInPieces(s, document(1000), 333)
	}}
	db, _ := openFake(t, f, "")
	db.SetMaxOpenConns(8)
	var wg sync.WaitGroup
	errs := make(chan error, 32)
	for range 32 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rs, err := db.QueryContext(context.Background(), `SELECT id, s FROM t`)
			if err != nil {
				errs <- err
				return
			}
			defer rs.Close()
			n := 0
			for rs.Next() {
				n++
			}
			if rs.Err() != nil || n != 1000 {
				errs <- fmt.Errorf("%d rows, err %v", n, rs.Err())
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	if _, err := db.Begin(); err == nil {
		t.Error("Begin worked")
	}
	st, err := db.Prepare(`SELECT id, s FROM t WHERE id = ?`)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	if err := st.QueryRow(1).Scan(new(uint64), new(string)); err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if last := f.queries[len(f.queries)-1]; last.Sql != "SELECT id, s FROM t WHERE id = 1" {
		t.Fatalf("prepared statement text = %q", last.Sql)
	}
}
