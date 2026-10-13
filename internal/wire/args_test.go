package wire

import (
	"database/sql/driver"
	"math"
	"testing"
	"time"
)

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
			got, err := Bind(tc.q, tc.args)
			if (err != nil) != tc.wantErr {
				t.Fatalf("err = %v, wantErr %v", err, tc.wantErr)
			}
			if got != tc.want {
				t.Fatalf("got  %s\nwant %s", got, tc.want)
			}
		})
	}
}
