package chdbsql

import (
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
)

// resultFormat has a names line and a types line before the rows, so Rows can report column names and
// decode values by ClickHouse type without a schema. The embedded and the HTTP driver both read it. It does not
// depend on Parquet or Arrow.
const resultFormat = "JSONCompactEachRowWithNamesAndTypes"

var errNoTypeHeader = errors.New("chdbsql: result has no type header")

// columns is the header of a result: the column names and their types.
type columns struct {
	names []string
	types []colType
}

// parseHeader reads the names line and the types line of a result.
func parseHeader(names, types []byte) (columns, error) {
	var c columns
	if err := json.Unmarshal(names, &c.names); err != nil {
		return columns{}, fmt.Errorf("chdbsql: bad result header: %w", err)
	}
	var ts []string
	if err := json.Unmarshal(types, &ts); err != nil || len(ts) != len(c.names) {
		return columns{}, fmt.Errorf("chdbsql: bad result type header: %q", types)
	}
	c.types = make([]colType, len(ts))
	for i, t := range ts {
		c.types[i] = parseType(t)
	}
	return c, nil
}

// decodeRow decodes one row line into dest, which has one slot per column.
func (c columns) decodeRow(line []byte, dest []driver.Value) error {
	var cells []json.RawMessage
	if err := json.Unmarshal(line, &cells); err != nil {
		return fmt.Errorf("chdbsql: bad result row: %w", err)
	}
	if len(cells) != len(dest) {
		return fmt.Errorf("chdbsql: row has %d values, header has %d columns", len(cells), len(dest))
	}
	for i, cell := range cells {
		v, err := c.types[i].decode(cell)
		if err != nil {
			return fmt.Errorf("chdbsql: column %q (%s): %w", c.names[i], c.types[i].full, err)
		}
		dest[i] = v
	}
	return nil
}

func (c columns) typeName(i int) string { return c.types[i].full }
