package chdbsql

import (
	"bytes"
	"encoding/json"
	"math"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"
)

// colType is a parsed ClickHouse column type.
type colType struct {
	full     string // as reported by the server, e.g. Nullable(DateTime64(6, 'UTC'))
	base     string // type name without wrappers and arguments, e.g. DateTime64
	args     string // text between the outer parentheses of base
	nullable bool
	loc      *time.Location // for DateTime and DateTime64
}

var locCache sync.Map // name -> *time.Location

// parseType strips LowCardinality and Nullable and splits the base name from its arguments.
func parseType(full string) colType {
	t := colType{full: full}
	s := strings.TrimSpace(full)
	for {
		switch {
		case strings.HasPrefix(s, "LowCardinality(") && strings.HasSuffix(s, ")"):
			s = s[len("LowCardinality(") : len(s)-1]
			continue
		case strings.HasPrefix(s, "Nullable(") && strings.HasSuffix(s, ")"):
			t.nullable = true
			s = s[len("Nullable(") : len(s)-1]
			continue
		}
		break
	}
	if i := strings.IndexByte(s, '('); i >= 0 && strings.HasSuffix(s, ")") {
		t.base, t.args = s[:i], s[i+1:len(s)-1]
	} else {
		t.base = s
	}
	if t.base == "DateTime" || t.base == "DateTime64" {
		t.loc = time.UTC
		// The zone is the single-quoted argument; without one the server zone applies, assumed UTC.
		if a := strings.IndexByte(t.args, '\''); a >= 0 {
			if b := strings.IndexByte(t.args[a+1:], '\''); b >= 0 {
				name := t.args[a+1 : a+1+b]
				if v, ok := locCache.Load(name); ok {
					t.loc = v.(*time.Location)
				} else if l, err := time.LoadLocation(name); err == nil {
					locCache.Store(name, l)
					t.loc = l
				}
			}
		}
	}
	return t
}

var (
	typeInt64   = reflect.TypeOf(int64(0))
	typeUint64  = reflect.TypeOf(uint64(0))
	typeFloat64 = reflect.TypeOf(float64(0))
	typeBool    = reflect.TypeOf(false)
	typeString  = reflect.TypeOf("")
	typeTime    = reflect.TypeOf(time.Time{})
	typeAny     = reflect.TypeOf((*any)(nil)).Elem()
)

func (t colType) scanType() reflect.Type {
	if t.nullable {
		return typeAny
	}
	switch t.base {
	case "Int8", "Int16", "Int32", "Int64", "UInt8", "UInt16", "UInt32":
		return typeInt64
	case "UInt64":
		return typeUint64
	case "Float32", "Float64":
		return typeFloat64
	case "Bool":
		return typeBool
	case "Date", "Date32", "DateTime", "DateTime64":
		return typeTime
	}
	return typeString
}

// decode turns one JSON cell into a driver.Value.
//
//	Int8..Int64, UInt8..UInt32  int64          Float32/64        float64 (JSON null, ClickHouse's nan/inf, becomes NaN)
//	UInt64                      uint64         Bool              bool
//	Date, DateTime*             time.Time      Nullable(T)       nil for NULL
//	everything else             string: text for String, Enum, UUID, IP, Int128/256, UInt128/256 and Decimal;
//	                            the JSON text for Array, Tuple, Map, JSON, Variant and Dynamic
func (t colType) decode(raw json.RawMessage) (any, error) {
	raw = bytes.TrimSpace(raw)
	isNull := string(raw) == "null"
	if isNull && t.nullable {
		return nil, nil
	}
	switch t.base {
	case "Int8", "Int16", "Int32", "Int64", "UInt8", "UInt16", "UInt32":
		return strconv.ParseInt(unquote(raw), 10, 64)
	case "UInt64":
		return strconv.ParseUint(unquote(raw), 10, 64)
	case "Float32", "Float64":
		if isNull {
			return math.NaN(), nil
		}
		return strconv.ParseFloat(unquote(raw), 64) // also reads the quoted "nan", "inf", "-inf"
	case "Bool":
		return string(raw) == "true", nil
	case "Date", "Date32":
		return time.ParseInLocation("2006-01-02", unquote(raw), time.UTC)
	case "DateTime", "DateTime64":
		return time.ParseInLocation("2006-01-02 15:04:05.999999999", unquote(raw), t.loc)
	case "String", "FixedString", "Enum8", "Enum16", "UUID", "IPv4", "IPv6":
		if isNull {
			return "", nil
		}
		var s string
		if err := json.Unmarshal(raw, &s); err != nil {
			return nil, err
		}
		return s, nil
	case "Int128", "Int256", "UInt128", "UInt256", "Decimal", "Decimal32", "Decimal64", "Decimal128", "Decimal256":
		return unquote(raw), nil // text keeps every digit
	}
	return string(raw), nil
}

// unquote returns the number text of a JSON value that ClickHouse may quote (64-bit integers by default).
func unquote(raw []byte) string {
	if len(raw) >= 2 && raw[0] == '"' {
		var s string
		if json.Unmarshal(raw, &s) == nil {
			return s
		}
	}
	return string(raw)
}
