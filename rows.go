package chdbsql

import (
	"bytes"
	"context"
	"database/sql/driver"
	"errors"
	"io"
	"reflect"

	chdbpurego "github.com/chdb-io/chdb-go/chdb-purego"

	"github.com/tiny-obs/chdb-sql/internal/wire"
)

// query starts a streaming read and returns once the two header lines are in, so Columns works at once.
func (cn *conn) query(ctx context.Context, q string) (driver.Rows, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if cn.broken.Load() {
		return nil, driver.ErrBadConn
	}
	sr, err := cn.c.QueryStreaming(q, wire.Format)
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
	cols     wire.Columns
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
	return r.cols.DecodeRow(l, dest)
}

func (r *rows) ColumnTypeDatabaseTypeName(i int) string { return r.cols.TypeName(i) }

func (r *rows) ColumnTypeNullable(i int) (nullable, ok bool) { return r.cols.Nullable(i), true }

func (r *rows) ColumnTypeScanType(i int) reflect.Type { return r.cols.ScanType(i) }
