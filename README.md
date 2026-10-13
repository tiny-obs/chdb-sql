# chdb-sql

A `database/sql` driver for an embedded [chDB](https://github.com/chdb-io/chdb) (`libchdb.so`) from Go. It is built on
`github.com/chdb-io/chdb-go/chdb-purego` (purego, no cgo) and gives every pooled connection its own chDB connection, so
one writer and several readers run in parallel inside one process.

It exists because the stock `chdb-go` `database/sql` driver does not work with the small library built in
[chdb-core](https://github.com/tiny-obs/chdb-core) (`lite-shared` branch): the stock driver fixes the result format to
Parquet and sends every query through one process-wide session. This driver reads results as
`JSONCompactEachRowWithNamesAndTypes`, so `libchdb.so` needs no Parquet or Arrow. It should also work with a full
`libchdb.so`, but that has not been tested.

Module: `github.com/tiny-obs/chdb-sql`, package `chdbsql`, driver name `"chdbsql"`.

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
discarded, like an abandoned call. A stream read to its end keeps the connection.

## Limitations

- No transactions (`Begin` returns an error).
- No `LastInsertId` and no `RowsAffected`: chDB does not return an affected-row count for `INSERT`.
- No named arguments. No server-side prepared statements (`Prepare` only keeps the query text).
- One embedded server and one data path per process.

## Test

```
CHDB_LIB_PATH=/path/to/libchdb.so go test ./...
```

The tests create a temporary data directory and cover argument binding, a round trip of every column type, zero-row
and 300 000-row results, errors, read-only rejection of writes, parallel writer and readers, context cancellation, and
rejection of named arguments and transactions. They need a working `libchdb.so`. Last run: all pass against the lite
library from chdb-core (`lite-shared`).
