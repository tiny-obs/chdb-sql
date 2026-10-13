package chdbsql

import (
	"bytes"
	"context"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"

	chdbpurego "github.com/chdb-io/chdb-go/chdb-purego"
)

// resultFormat has a names line and a types line before the rows, so Rows can report column names and
// decode values by ClickHouse type without a schema. It does not depend on Parquet or Arrow.
const resultFormat = "JSONCompactEachRowWithNamesAndTypes"

// query starts a streaming read and returns once the two header lines are in, so Columns works at once.
func (cn *conn) query(ctx context.Context, q string) (driver.Rows, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if cn.broken.Load() {
		return nil, driver.ErrBadConn
	}
	sr, err := cn.c.QueryStreaming(q, resultFormat)
	if err != nil {
		return nil, err
	}
	r := &rows{cn: cn, sr: sr}
	if err := r.readHeader(); err != nil {
		r.Close()
		return nil, err
	}
	return r, nil
}

type rows struct {
	cn  *conn
	sr  chdbpurego.ChdbStreamResult
	buf []byte // bytes received and not yet parsed
	hdr []byte // the two header lines of the first chunk, kept to check and strip the copy at the start of every chunk
	eof bool
	// failed is set when the stream reported an error or broke its framing.
	failed bool
	// finished is set when libchdb reported the natural end of the stream, so there is nothing left to cancel.
	finished bool
	names    []string
	types    []colType
}

// fill appends the next chunk to buf. libchdb starts every chunk with its own names and types lines and ends
// it on a row boundary, so those two lines are removed from every chunk after the first. At the end it sets
// eof: libchdb ends a stream with nil or an empty chunk, and a failed query reports its error on that chunk.
func (r *rows) fill() error {
	if r.eof {
		return nil
	}
	chunk := r.sr.GetNext()
	if chunk != nil {
		if err := chunk.Error(); err != nil {
			r.eof, r.failed = true, true
			return err
		}
	}
	if chunk == nil || chunk.Len() == 0 {
		r.eof = true
		// libchdb answers the natural end of a stream with an empty chunk and marks the stream finished.
		r.finished = chunk != nil
		if err := r.sr.Error(); err != nil {
			r.failed = true
			return err
		}
		return nil
	}
	// The chunk memory is freed by the next GetNext, so keep a copy.
	b := chunk.Buf()
	if r.hdr == nil {
		i := bytes.IndexByte(b, '\n')
		if i >= 0 {
			i2 := bytes.IndexByte(b[i+1:], '\n')
			if i2 >= 0 {
				r.hdr = append([]byte(nil), b[:i+1+i2+1]...)
			}
		}
	} else if bytes.HasPrefix(b, r.hdr) {
		b = b[len(r.hdr):]
	} else {
		r.eof, r.failed = true, true
		return errors.New("chdbsql: a result chunk does not start with the result header")
	}
	r.buf = append(r.buf, b...)
	return nil
}

// line returns the next complete line without its newline, or io.EOF.
func (r *rows) line() ([]byte, error) {
	for {
		if i := bytes.IndexByte(r.buf, '\n'); i >= 0 {
			l := r.buf[:i]
			r.buf = r.buf[i+1:]
			if len(l) == 0 {
				continue
			}
			return l, nil
		}
		if r.eof {
			if len(bytes.TrimSpace(r.buf)) == 0 {
				return nil, io.EOF
			}
			l := r.buf
			r.buf = nil
			return l, nil
		}
		if err := r.fill(); err != nil {
			return nil, err
		}
	}
}

func (r *rows) readHeader() error {
	l, err := r.line()
	if err == io.EOF {
		return nil // a statement without a result set
	}
	if err != nil {
		return err
	}
	if err := json.Unmarshal(l, &r.names); err != nil {
		return fmt.Errorf("chdbsql: bad result header: %w", err)
	}
	l, err = r.line()
	if err != nil {
		if err == io.EOF {
			err = errors.New("chdbsql: result has no type header")
		}
		return err
	}
	var ts []string
	if err := json.Unmarshal(l, &ts); err != nil || len(ts) != len(r.names) {
		return fmt.Errorf("chdbsql: bad result type header: %q", l)
	}
	r.types = make([]colType, len(ts))
	for i, t := range ts {
		r.types[i] = parseType(t)
	}
	return nil
}

func (r *rows) Columns() []string { return r.names }

// Close frees the stream only after libchdb reported its natural end. chdb-go v1.12.0 Free passes the connection
// cell where libchdb expects the connection (streaming.go: chdbStreamCancelQuery(c.curConn, ...) while every fetch
// passes c.curConn.internal_data), so for a stream that is not finished libchdb locks a mutex in the wrong
// object: a hang or SIGSEGV, seen with both the lite and the full libchdb. A finished stream returns before that
// object is touched. An unfinished stream (an error, or Close before the last row) is therefore not freed here: the
// small result handle leaks, and closing the connection cancels the query inside libchdb. The next query on that
// connection would read the abandoned stream's leftover header instead of its own, so the connection is marked
// broken and database/sql discards it. Remove this once chdb-go passes the right handle.
func (r *rows) Close() error {
	if r.sr != nil {
		// A reader that stops right after the last row (QueryRow) has not seen the end of the stream yet. The next chunk is
		// the end marker when nothing is left, so look at it before judging the stream unfinished.
		if !r.eof && len(bytes.TrimSpace(r.buf)) == 0 {
			_ = r.fill() // an error sets failed
		}
		if r.finished {
			r.sr.Free()
		}
		if !r.eof || r.failed {
			r.cn.broken.Store(true)
		}
		r.sr = nil
	}
	return nil
}

func (r *rows) Next(dest []driver.Value) error {
	if r.sr == nil {
		return io.EOF
	}
	l, err := r.line()
	if err != nil {
		return err
	}
	var cells []json.RawMessage
	if err := json.Unmarshal(l, &cells); err != nil {
		return fmt.Errorf("chdbsql: bad result row: %w", err)
	}
	if len(cells) != len(dest) {
		return fmt.Errorf("chdbsql: row has %d values, header has %d columns", len(cells), len(dest))
	}
	for i, c := range cells {
		v, err := r.types[i].decode(c)
		if err != nil {
			return fmt.Errorf("chdbsql: column %q (%s): %w", r.names[i], r.types[i].full, err)
		}
		dest[i] = v
	}
	return nil
}

func (r *rows) ColumnTypeDatabaseTypeName(i int) string { return r.types[i].full }

func (r *rows) ColumnTypeNullable(i int) (nullable, ok bool) { return r.types[i].nullable, true }

func (r *rows) ColumnTypeScanType(i int) reflect.Type { return r.types[i].scanType() }
