// Package wire is what the embedded and the HTTP driver share: client-side rendering of "?" arguments and decoding
// of the JSONCompactEachRowWithNamesAndTypes result format. It imports nothing that needs libchdb.so.
package wire

import (
	"database/sql/driver"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"reflect"
	"strconv"
	"strings"
	"time"
)

// libchdb's query call takes SQL text only, so "?" arguments are rendered into the text here, with every
// value quoted by the rules below. With no arguments the query is sent untouched, so "a ? b : c" and data
// after "INSERT ... FORMAT JSONEachRow" containing "?" are safe.

// CheckNamedValue accepts everything literal can render and rejects named arguments.
func CheckNamedValue(nv *driver.NamedValue) error {
	if nv.Name != "" {
		return errors.New("chdb-sql: named arguments are not supported, use positional ?")
	}
	switch v := nv.Value.(type) {
	case nil, int64, uint64, float64, bool, string, []byte, time.Time:
		return nil
	default:
		rv := reflect.ValueOf(v)
		if rv.Kind() == reflect.Slice || rv.Kind() == reflect.Array {
			// Rendered as an array literal; every element must itself be renderable.
			for i := range rv.Len() {
				e := driver.NamedValue{Value: rv.Index(i).Interface()}
				if err := CheckNamedValue(&e); err != nil {
					return err
				}
			}
			return nil
		}
		cv, err := driver.DefaultParameterConverter.ConvertValue(v)
		if err != nil {
			return err
		}
		nv.Value = cv
		return nil
	}
}

// Bind replaces each "?" outside quotes and comments with the literal of the matching argument.
func Bind(query string, args []driver.NamedValue) (string, error) {
	if len(args) == 0 {
		return query, nil
	}
	var b strings.Builder
	b.Grow(len(query) + 16*len(args))
	n := 0
	for i := 0; i < len(query); {
		c := query[i]
		switch {
		case c == '\'' || c == '"' || c == '`':
			j := skipQuoted(query, i)
			b.WriteString(query[i:j])
			i = j
		case c == '-' && strings.HasPrefix(query[i:], "--"):
			j := strings.IndexByte(query[i:], '\n')
			if j < 0 {
				j = len(query) - i
			}
			b.WriteString(query[i : i+j])
			i += j
		case c == '/' && strings.HasPrefix(query[i:], "/*"):
			j := strings.Index(query[i+2:], "*/")
			if j < 0 {
				j = len(query) - i - 2
			} else {
				j += 2
			}
			b.WriteString(query[i : i+2+j])
			i += 2 + j
		case c == '?':
			if n >= len(args) {
				return "", fmt.Errorf("chdb-sql: query has more ? placeholders than the %d arguments", len(args))
			}
			lit, err := literal(args[n].Value)
			if err != nil {
				return "", fmt.Errorf("chdb-sql: argument %d: %w", n+1, err)
			}
			b.WriteString(lit)
			n++
			i++
		default:
			b.WriteByte(c)
			i++
		}
	}
	if n != len(args) {
		return "", fmt.Errorf("chdb-sql: query has %d ? placeholders but %d arguments", n, len(args))
	}
	return b.String(), nil
}

// skipQuoted returns the index after the quoted token that starts at s[i]. A backslash escapes the next byte
// and a doubled quote stands for one quote, as in ClickHouse.
func skipQuoted(s string, i int) int {
	q := s[i]
	for j := i + 1; j < len(s); j++ {
		switch s[j] {
		case '\\':
			j++
		case q:
			if j+1 < len(s) && s[j+1] == q {
				j++
				continue
			}
			return j + 1
		}
	}
	return len(s)
}

var stringEscaper = strings.NewReplacer(`\`, `\\`, `'`, `\'`, "\x00", `\0`)

// literal renders v as a ClickHouse SQL literal.
func literal(v any) (string, error) {
	switch x := v.(type) {
	case nil:
		return "NULL", nil
	case int64:
		return strconv.FormatInt(x, 10), nil
	case uint64:
		return strconv.FormatUint(x, 10), nil
	case float64:
		switch {
		case math.IsNaN(x):
			return "nan", nil
		case math.IsInf(x, 1):
			return "inf", nil
		case math.IsInf(x, -1):
			return "-inf", nil
		}
		return strconv.FormatFloat(x, 'g', -1, 64), nil
	case bool:
		if x {
			return "true", nil
		}
		return "false", nil
	case string:
		return "'" + stringEscaper.Replace(x) + "'", nil
	case []byte:
		// A String holds raw bytes; hex avoids any quoting question.
		return "unhex('" + hex.EncodeToString(x) + "')", nil
	case time.Time:
		x = x.UTC()
		if x.Nanosecond()%1000 != 0 {
			return "toDateTime64('" + x.Format("2006-01-02 15:04:05.000000000") + "', 9, 'UTC')", nil
		}
		return "toDateTime64('" + x.Format("2006-01-02 15:04:05.000000") + "', 6, 'UTC')", nil
	}
	rv := reflect.ValueOf(v)
	if rv.Kind() == reflect.Slice || rv.Kind() == reflect.Array {
		parts := make([]string, rv.Len())
		for i := range parts {
			e := rv.Index(i).Interface()
			nv := driver.NamedValue{Value: e}
			if err := CheckNamedValue(&nv); err != nil {
				return "", err
			}
			s, err := literal(nv.Value)
			if err != nil {
				return "", err
			}
			parts[i] = s
		}
		return "[" + strings.Join(parts, ",") + "]", nil
	}
	return "", fmt.Errorf("unsupported argument type %T", v)
}
