package dbdriver

import (
	"encoding/base64"
	"math"
	"strconv"
	"strings"
	"testing"
	"time"
)

// benchRows are 1000 rows shaped like the driver benchmark table (id, name,
// category, amount, created_at, 64-byte payload).
func benchRows() [][]any {
	rows := make([][]any, 1000)
	categories := []string{"alpha", "beta", "gamma", "delta"}
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	for i := range rows {
		rows[i] = []any{int64(i), "row-" + strconv.Itoa(i), categories[i%4], float64(i) / 7, start.Add(time.Duration(i) * time.Second), strings.Repeat("x", 64)}
	}
	return rows
}

// TestColumnarPageBound: an encoded page never exceeds the bound the page
// budget is computed from, for every column kind and null pattern.
func TestColumnarPageBound(t *testing.T) {
	values := []any{nil, true, false, int64(0), int64(-1), int64(math.MaxInt64), uint64(math.MaxUint64), 0.5, float32(1.5), math.NaN(),
		"", "text", strings.Repeat("é", 300), []byte{0xff}, map[string]any{"a": []any{1}}, []any{}, time.Unix(0, 1)}
	for columns := 1; columns <= 4; columns++ {
		for seed := range 200 {
			rows := make([][]byte, seed%17)
			sum := 0
			for r := range rows {
				raw := make([]any, columns)
				for c := range raw {
					raw[c] = values[(seed*31+r*7+c*13)%len(values)]
					if seed%5 == 0 && r%3 != 0 {
						raw[c] = values[(c*3)%4] // typed columns with nulls
					}
				}
				rows[r] = appendColumnarRow(nil, raw)
				sum += len(rows[r])
			}
			bin := encodeColumnar(rows, columns, 0)
			if bound := columnarPageBound(columns, sum); len(bin) > bound {
				t.Fatalf("columns %d seed %d: %d bytes > bound %d", columns, seed, len(bin), bound)
			}
		}
	}
}

// BenchmarkPageEncode compares the plugin-side cost of one 1000-row page:
// JSON rows (protocol 1.x) and columnar blocks plus base64 (v2 draft).
func BenchmarkPageEncode(b *testing.B) {
	rows := benchRows()
	b.Run("json", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			out := make([]byte, 0, 160*len(rows))
			for i, row := range rows {
				if i > 0 {
					out = append(out, ',')
				}
				out = append(out, '[')
				for j, v := range row {
					if j > 0 {
						out = append(out, ',')
					}
					out = appendCell(out, v)
				}
				out = append(out, ']')
			}
			b.SetBytes(int64(len(out)))
		}
	})
	b.Run("columnar", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			cells := make([][]byte, len(rows))
			size := 0
			for i, row := range rows {
				cells[i] = appendColumnarRow(make([]byte, 0, 64), row)
				size += len(cells[i])
			}
			bin := encodeColumnar(cells, 6, columnarPageBound(6, size))
			out := make([]byte, base64.StdEncoding.EncodedLen(len(bin)))
			base64.StdEncoding.Encode(out, bin)
			b.SetBytes(int64(len(out)))
		}
	})
}
