package wire

import (
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
)

// Format is the result format both drivers ask for. It has a names line and a types line before the rows, so a driver
// can report column names and decode values by ClickHouse type without a schema. It does not depend on Parquet or Arrow.
const Format = "JSONCompactEachRowWithNamesAndTypes"

// ErrNoTypes is returned when a result has a names line but no types line.
var ErrNoTypes = errors.New("chdb-sql: result has no type header")

// Columns is the header of a result: the column names and their types.
type Columns struct {
	Names []string
	Types []ColType
}

// ParseHeader reads the names line and the types line of a result.
func ParseHeader(names, types []byte) (Columns, error) {
	var c Columns
	if err := json.Unmarshal(names, &c.Names); err != nil {
		return Columns{}, fmt.Errorf("chdb-sql: bad result header: %w", err)
	}
	var ts []string
	if err := json.Unmarshal(types, &ts); err != nil || len(ts) != len(c.Names) {
		return Columns{}, fmt.Errorf("chdb-sql: bad result type header: %q", types)
	}
	c.Types = make([]ColType, len(ts))
	for i, t := range ts {
		c.Types[i] = ParseType(t)
	}
	return c, nil
}

// DecodeRow decodes one row line into dest, which has one slot per column.
func (c Columns) DecodeRow(line []byte, dest []driver.Value) error {
	var cells []json.RawMessage
	if err := json.Unmarshal(line, &cells); err != nil {
		return fmt.Errorf("chdb-sql: bad result row: %w", err)
	}
	if len(cells) != len(dest) {
		return fmt.Errorf("chdb-sql: row has %d values, header has %d columns", len(cells), len(dest))
	}
	for i, cell := range cells {
		v, err := c.Types[i].Decode(cell)
		if err != nil {
			return fmt.Errorf("chdb-sql: column %q (%s): %w", c.Names[i], c.Types[i].full, err)
		}
		dest[i] = v
	}
	return nil
}

// TypeName is the ClickHouse type of column i as the server reported it.
func (c Columns) TypeName(i int) string { return c.Types[i].full }

// Nullable reports whether column i is Nullable.
func (c Columns) Nullable(i int) bool { return c.Types[i].nullable }

// ScanType is the Go type of the values of column i.
func (c Columns) ScanType(i int) reflect.Type { return c.Types[i].scanType() }
