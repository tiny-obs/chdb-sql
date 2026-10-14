# chdb-sql

A `database/sql` driver for an embedded [chDB](https://github.com/chdb-io/chdb) (`libchdb.so`) from Go. It is built on
`github.com/chdb-io/chdb-go/chdb-purego` (purego, no cgo) and gives every pooled connection its own chDB connection, so
one writer and several readers run in parallel inside one process.

It exists because the stock `chdb-go` `database/sql` driver does not work with the small library built in
[chdb-core](https://github.com/tiny-obs/chdb-core) (`lite-shared` branch): the stock driver fixes the result format to
Parquet and sends every query through one process-wide session. This driver reads results as
`JSONCompactEachRowWithNamesAndTypes`, so `libchdb.so` needs no Parquet or Arrow. It should also work with a full
`libchdb.so`, but that has not been tested.

Module: `github.com/tiny-obs/chdb-sql`. Package `chdbsql` is the embedded chDB driver, name `"chdbsql"`. Package `chhttp`
drives a ClickHouse server over HTTP, name `"chdbsql-http"`, see [HTTP driver](#http-driver). Package `chconnect`
drives a [chdb-server](https://github.com/tiny-obs/chdb-server) over its ConnectRPC service, name
`"chdbsql-connect"`, see [ConnectRPC driver](#connectrpc-driver). All three share `internal/wire` (argument rendering
and result decoding).

## Use

`libchdb.so` is found through `CHDB_LIB_PATH`, then `PATH`, then `/usr/local/lib/libchdb.so` (rules of `chdb-purego`).

```go
import (
	"database/sql"

	chdbsql "github.com/tiny-obs/chdb-sql"
)

// The first connection starts the embedded server: open and ping the writer before the readers.
w := sql.OpenDB(chdbsql.NewConnector("file:/data/chdb?max_server_memory_usage=4000000000"))
w.SetMaxOpenConns(1)
if err := w.Ping(); err != nil { /* ... */ }

r := sql.OpenDB(chdbsql.NewConnector("file:/data/chdb?mode=ro&max_execution_time=10"))
r.SetMaxOpenConns(4)

w.Exec(`CREATE TABLE IF NOT EXISTS t (ts DateTime, v Float64) ENGINE = MergeTree ORDER BY ts`)
w.Exec(`INSERT INTO t VALUES (?, ?)`, time.Now(), 1.5)
rows, _ := r.Query(`SELECT toStartOfMinute(ts), avg(v) FROM t WHERE v > ? GROUP BY 1`, 0.5)
```

`sql.Open(chdbsql.DriverName, dsn)` also works.

## DSN

The DSN is the `chdb-purego` connection string: `file:/data/dir?key=value&...` or `:memory:`.

- Every `key=value` becomes a `--key=value` argument of the embedded server. `mode=ro` becomes `--readonly=1`.
- Values are not percent-decoded. Pass plain values.
- One embedded server exists per process and every connection must use the same path.
- Server-wide settings (`max_server_memory_usage`, `memory_worker_*`) go in the DSN of the first connection.
- A read-only connection cannot change settings per query, so every setting a query needs goes in its DSN. Use two
  `*sql.DB` handles on one path, one read-write and one `mode=ro`, as above.

## Behavior

**Arguments.** libchdb's query call takes SQL text only, so `?` placeholders are rendered into the text as literals.
`?` inside quotes (`'`, `"`, `` ` ``) and comments is left alone. With no arguments the query is sent untouched, so
`a ? b : c` and `INSERT ... FORMAT JSONEachRow` data containing `?` are safe. Supported: `nil`, integers, floats
(including `nan`, `inf`), `bool`, `string`, `[]byte` (as `unhex(...)`), `time.Time` (as UTC `DateTime64`, 6 or 9
digits), and slices or arrays of those (as array literals). Named arguments are rejected. The placeholder count must
match the argument count.

**Result types.** Values are decoded by the ClickHouse type of the column:

| ClickHouse type | Go value |
|---|---|
| `Int8`..`Int64`, `UInt8`..`UInt32` | `int64` |
| `UInt64` | `uint64` |
| `Float32`, `Float64` | `float64` (NULL, `nan`, `inf` become NaN or Inf) |
| `Bool` | `bool` |
| `Date`, `DateTime`, `DateTime64` | `time.Time` (in the column's time zone) |
| `Nullable(T)` | the value of `T`, or `nil` |
| `String`, `Enum`, `UUID`, IP, `Int128/256`, `UInt128/256`, `Decimal` | `string` |
| `Array`, `Tuple`, `Map`, `JSON`, `Variant`, `Dynamic` | JSON text as `string` |

`LowCardinality` is stripped. Column names, `ColumnTypeDatabaseTypeName`, `ColumnTypeNullable` and
`ColumnTypeScanType` are reported, also for zero-row results. Large results stream in chunks.

**Cancellation.** libchdb cannot cancel a running statement through this call. When the context ends first, the call
is abandoned: it keeps running, and the connection is marked broken and discarded. Set `max_execution_time` in the DSN
to bound statements.

**Streams.** A stream that fails or is closed before its end leaves its query running inside libchdb, and the next
statement on that connection would read the old query's header. The connection is therefore marked broken and
discarded, like an abandoned call. A result read to its last row keeps the connection, also when the reader stops there
without asking for the next row (`QueryRow`).

## HTTP driver

Package `github.com/tiny-obs/chdb-sql/chhttp` drives a ClickHouse server over its HTTP interface, so code written
against the embedded driver can run against an external server by changing the connector. Driver name `"chdbsql-http"`.
It does not import `chdb-go` and needs no `libchdb.so`; importing it registers only the HTTP driver.

```go
db := sql.OpenDB(must(chhttp.NewHTTPConnector("http://user:pass@ch.example:8123/logs?readonly=1&max_execution_time=10")))
db.SetMaxOpenConns(8)
```

- DSN: `http://` or `https://`, user and password in the URL, the path is the default database. Every query parameter is a
  ClickHouse setting sent with every request. Unlike an embedded read-only connection, a `readonly=1` connection can
  carry `max_execution_time`, so one `*sql.DB` per role is enough.
- Every statement is one stateless `POST`; the SQL text is the request body. `INSERT ... FORMAT JSONEachRow` followed by
  the rows goes out verbatim, as the HTTP interface expects.
- Results are read as `JSONCompactEachRowWithNamesAndTypes` and decoded by the same code as the embedded driver: the
  same Go value for every column type, arrays, maps and tuples as JSON text, `UInt64` and `Int128+` with every digit.
  `?` arguments are rendered into the text as literals, with the same rules.
- Errors are `*chhttp.ServerError` (`Status`, `Code`, `Message`). ClickHouse answers `200` and writes an error into the
  body when a query fails after its output started; the driver finds that block (tag from `X-ClickHouse-Exception-Tag`)
  and returns it from `Rows.Next` and `Rows.Err`, so a failed search is never a short, valid-looking result.
- A result read to its last row, also by `QueryRow`, returns the HTTP connection to the pool. A result closed earlier
  aborts the request, which makes the server stop the query. A cancelled context does the same.
- Compression is off on purpose: a response that is compressed is buffered, so an error in the middle of a large
  result would come as status 500 with the rows as its body and nothing would stream.

## ConnectRPC driver

Package `github.com/tiny-obs/chdb-sql/chconnect` talks to chdb-server's `chdb.v1.Chdb` service (`Query`, `Exec`,
`Insert`). Driver name `"chdbsql-connect"` (`chconnect.DriverName`). It does not import `chdb-go` and needs no
`libchdb.so`; importing it registers only this driver. It decodes results exactly like the other two drivers.

```go
c, err := chconnect.NewConnector("http://:secret-key@chdb.example:8123/logs?max_execution_time=10")
db := sql.OpenDB(c)
db.SetMaxOpenConns(8)

// A batch of rows in a text format goes to the server's batching write path:
n, err := c.Insert(ctx, chconnect.InsertOptions{Table: "events", Columns: []string{"ts", "msg"}, Format: "JSONEachRow"}, rows)
```

**DSN.** `http://[user:key@]host:8123/database?setting=value`, or `https://`. The path is the default database, sent in
the `database` field of every request (`?database=name` works too when the path is empty). Every other query parameter is
a ClickHouse setting, sent in the `settings` map of every request. The password is the server's auth key
(`CHDB_AUTH_KEY`): when the DSN has one, every request carries `Authorization: Bearer <key>`; without a password no header
is sent. The user name is accepted and ignored. A wrong or missing key gives connect code `Unauthenticated`.

**When to use it instead of `chdbsql-http`.**

- Inserts. `ExecContext` with `INSERT INTO [db.]table [(columns)] FORMAT <format>` followed by a newline (or blanks) and
  the data, without `?` arguments, is sent through the `Insert` RPC. The server merges such inserts from many callers
  into one MergeTree part and returns after the rows are written. Table and column names are parsed client-side (bare, or
  quoted with backticks or double quotes, with an optional `db.` prefix). Any other shape (`INSERT ... VALUES`,
  `INSERT ... SELECT`, `FORMAT` without data, a `SETTINGS` clause, a statement with arguments) runs through `Exec`
  unchanged.
- The streaming batch API, `func (c *Connector) Insert(ctx, InsertOptions{Table, Columns, Format, Database, Settings},
  data io.Reader) (written uint64, err error)`. It reads `data` in 1 MiB chunks, sends the header first and returns the
  number of data bytes the server wrote. `Format` defaults to `JSONEachRow` on the server; `Database` and `Settings`
  default to the DSN's (a key in `Settings` overrides the DSN's). Table and column names are plain, not quoted. The server
  takes the whole call before it writes anything (a small one in memory, a large one in a file on disk), so a failed or
  aborted call (also a `data` reader that returns an error) writes nothing.
- Typed errors. Every server error is a `*chconnect.ServerError` with `Code` (the ClickHouse exception code from the
  `X-ClickHouse-Exception-Code` metadata, 0 when there is none), `ConnectCode` and `Message`; it wraps the
  `*connect.Error`, so `errors.As` finds either. An error that arrives after rows were sent is returned by `Rows.Next` and
  `Rows.Err`, never as a short result. A cancelled or expired context comes back as the context's error.
- The tuned HTTP/2 client (below).

**Reads and cancellation.** A query is one `Query` RPC streamed in chunks; a chunk boundary can fall inside a row, and the
driver reassembles lines across chunks. A result closed before its end cancels the RPC, which makes the server kill the
query. A result read to its last row, also by `QueryRow`, releases the stream cleanly. A cancelled context does the same
as an early close.

**Tuned client.** The driver always uses HTTP/2. `http://` means cleartext HTTP/2 with prior knowledge (no upgrade, no
HTTP/1.1); `https://` means TLS with ALPN `h2` against the system roots. The transport raises Go's HTTP/2 defaults to a
1 MiB maximum frame size, 8 MiB of receive buffer per stream and 16 MiB per connection. With the defaults (16 KB frames)
the server's author measured 0.34 to 0.42 s for a 139 MB result over h2c, and 0.08 to 0.19 s with these values; I did not
repeat that measurement. There is one `http.Client` per connector, no client timeout (the context bounds each call), up to
64 idle connections per host, and no proxy. Compression is off in both directions: the server does not compress responses,
and the client neither asks for gzip nor sends it.

**Module.** `chconnect` has no dependency on the chdb-server module. It carries its own copy of the generated client code
for the `chdb.v1` service in `internal/gen`, generated from chdb-server's `proto/chdb/v1/chdb.proto` with
`buf generate ../chdb-server --template buf.gen.yaml` (check the two repositories out side by side). Regenerate after the
proto changes. Importing both this copy and chdb-server's own `gen/chdb/v1` into one binary registers `chdb/v1/chdb.proto`
twice, which panics at start-up; use only one of them.

## Limitations

- No transactions (`Begin` returns an error).
- No `LastInsertId` and no `RowsAffected`: chDB does not return an affected-row count for `INSERT`.
- No named arguments. No server-side prepared statements (`Prepare` only keeps the query text).
- One embedded server and one data path per process (embedded driver).
- The HTTP driver sends no compression (`Accept-Encoding: identity`) and has no TLS options beyond the system roots.
- The ConnectRPC driver has no TLS options beyond the system roots, ignores proxy settings, and has no `RowsAffected`
  after an insert (`Connector.Insert` returns the bytes written, not rows).

## Test

```
CHDB_LIB_PATH=/path/to/libchdb.so go test ./...
```

The tests create a temporary data directory and cover argument binding, a round trip of every column type, zero-row
and 300 000-row results, errors, read-only rejection of writes, parallel writer and readers, context cancellation, and
rejection of named arguments and transactions. They need a working `libchdb.so`. Last run: all pass against the lite
library from chdb-core (`lite-shared`).

The HTTP driver tests need a ClickHouse server and are skipped without `CHDBSQL_HTTP_DSN`:

```
nerdctl run -d --name ch -p 18123:8123 -e CLICKHOUSE_USER=test -e CLICKHOUSE_PASSWORD=test clickhouse/clickhouse-server
CHDBSQL_HTTP_DSN=http://test:test@localhost:18123/default CHDB_LIB_PATH=/path/to/libchdb.so go test ./...
```

They cover the same round trip of every column type, an error in the middle of a result, error statuses, a read-only
connection that keeps its settings, TCP connection reuse, a 300 000-row result, abandoning a result, cancellation and
parallel readers. Last run: all pass against ClickHouse 26.9 with `-race`.

The ConnectRPC driver tests need a chdb-server and are skipped without `CHDBSQL_CONNECT_DSN`; the key is the DSN password
when the server was started with `CHDB_AUTH_KEY`:

```
CHDB_LIB_PATH=/usr/local/lib/libchdb.so chdb-server --data-dir /var/lib/chdb --listen 127.0.0.1:8123
CHDBSQL_CONNECT_DSN=http://:key@localhost:8123/default go test -race ./chconnect
```

They cover the round trip of every column type, an error in the middle of a result, error codes, a 300 000-row result in
several chunks, early close (the query disappears from `system.processes`), cancellation, parallel readers, the Insert API
with concurrent batches, the `INSERT ... FORMAT` routing (checked by counting RPCs), and an aborted insert. Set
`CHDBSQL_CONNECT_AUTH=1` against a server that has an auth key to also test that a wrong key and a missing key are
refused. Parsing of the DSN and of `INSERT` statements, and the tests against an in-process fake of the service (line
reassembly across chunks, error mapping, cancellation, the `Authorization` header, HTTP/2 and no compression, Insert
chunking and routing), need no server and always run. Last run: all pass with `-race` against an unauthenticated
chdb-server built from the local tree; the auth test was not run because that server has no auth key support yet.
