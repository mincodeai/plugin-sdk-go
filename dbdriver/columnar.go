package dbdriver

import (
	"encoding/binary"
	"encoding/json"
	"math"
	"strconv"
	"time"
	"unicode/utf8"
)

// Experimental columnar page encoding (protocol v2 draft, encoding
// "columnar1"; docs/architecture/database-driver-protocol.md §12).
//
// A Server with ServerOptions.ColumnarEncoding announces it in the initialize
// details ("encodings": ["columnar1"]). Only a host that also enables it
// requests it in plugin.db.connect ("encoding": "columnar1"); the Server then
// confirms it in the connect info and answers the execute, tableRows and
// fetch pages of that handle with
//
//	{"columns":…, "encoding":"columnar1", "data":"<base64>", "cursor":…, "done":…}
//
// instead of "rows". Everything else (budgets, cursors, notices, errors,
// backup streams, metadata) is unchanged. data is standard base64 of:
//
//	"MCC1"  uvarint rows  uvarint columns
//	per column: kind byte (bit 7: a null bitmap follows), then
//	  null bitmap  ceil(rows/8) bytes, bit r (LSB first) set = row r is null
//	  kind 0 null    every cell null, no payload (no bitmap)
//	  kind 1 bool    one byte (0/1) per non-null cell
//	  kind 2 int     zigzag uvarint per non-null cell
//	  kind 3 float   8 bytes little-endian IEEE 754 per non-null cell
//	  kind 4 string  uvarint byte length per non-null cell, then the bytes of
//	                 every non-null cell concatenated (valid UTF-8)
//	  kind 5 json    like string; each value is one JSON object or array
//	  kind 6 mixed   no bitmap; per cell a tag (0 null, 1 false, 2 true,
//	                 3 int, 4 float, 5 string, 6 json) then its payload as above
//
// Cells keep the v1 value space exactly: integers beyond ±2^53, NaN/±Inf,
// time values and binary data become the same strings as in JSON rows, and
// float32 values the float64 the host would parse from their JSON text.

// EncodingColumnar is the columnar page encoding id.
const EncodingColumnar = "columnar1"

const columnarMagic = "MCC1"

// Column kinds and mixed-cell tags.
const (
	colKindNull   = 0
	colKindBool   = 1
	colKindInt    = 2
	colKindFloat  = 3
	colKindString = 4
	colKindJSON   = 5
	colKindMixed  = 6
	colHasNulls   = 0x80

	colTagNull   = 0
	colTagFalse  = 1
	colTagTrue   = 2
	colTagInt    = 3
	colTagFloat  = 4
	colTagString = 5
	colTagJSON   = 6
)

// columnarHeaderBound bounds the page header (magic and two uvarints) and
// columnarColumnBound the per-column overhead (the kind byte). Every column
// kind costs at most 1 + Σ(1 + payload) bytes for its cells (the null bitmap
// is never larger than one tag byte per cell), so a page is at most
// columnarHeaderBound + columns·columnarColumnBound + Σ row sizes.
const (
	columnarHeaderBound = len(columnarMagic) + 2*binary.MaxVarintLen64
	columnarColumnBound = 1
)

// columnarPageBound is the binary size bound of a page with the given
// columns and summed row sizes.
func columnarPageBound(columns, rowBytes int) int {
	return columnarHeaderBound + columns*columnarColumnBound + rowBytes
}

// appendColumnarRow appends one row as tagged cells (the mixed-cell layout:
// tag, then payload). Its length is the row's size bound; encodeColumnar
// later regroups the cells of a page by column.
func appendColumnarRow(b []byte, row []any) []byte {
	for _, v := range row {
		b = appendColumnarCell(b, v)
	}
	return b
}

// appendColumnarCell appends one driver value with the same meaning as its
// v1 JSON encoding (appendCell).
func appendColumnarCell(b []byte, v any) []byte {
	switch x := v.(type) {
	case nil:
		return append(b, colTagNull)
	case bool:
		if x {
			return append(b, colTagTrue)
		}
		return append(b, colTagFalse)
	case string:
		return appendColumnarString(b, x)
	case []byte:
		if utf8.Valid(x) {
			b = binary.AppendUvarint(append(b, colTagString), uint64(len(x)))
			return append(b, x...)
		}
		return appendColumnarString(b, string(x))
	case int:
		return appendColumnarInt(b, int64(x))
	case int8:
		return appendColumnarInt(b, int64(x))
	case int16:
		return appendColumnarInt(b, int64(x))
	case int32:
		return appendColumnarInt(b, int64(x))
	case int64:
		return appendColumnarInt(b, x)
	case uint8:
		return appendColumnarInt(b, int64(x))
	case uint16:
		return appendColumnarInt(b, int64(x))
	case uint32:
		return appendColumnarInt(b, int64(x))
	case uint:
		return appendColumnarUint(b, uint64(x))
	case uint64:
		return appendColumnarUint(b, x)
	case float64:
		if math.IsNaN(x) || math.IsInf(x, 0) {
			return appendColumnarString(b, strconv.FormatFloat(x, 'g', -1, 64))
		}
		return binary.LittleEndian.AppendUint64(append(b, colTagFloat), math.Float64bits(x))
	case float32:
		f := float64(x)
		if math.IsNaN(f) || math.IsInf(f, 0) {
			return appendColumnarString(b, strconv.FormatFloat(f, 'g', -1, 32))
		}
		// The host parses the shortest float32 text as a float64.
		var tmp [32]byte
		parsed, _ := strconv.ParseFloat(string(strconv.AppendFloat(tmp[:0], f, 'g', -1, 32)), 64)
		return binary.LittleEndian.AppendUint64(append(b, colTagFloat), math.Float64bits(parsed))
	case time.Time:
		var tmp [64]byte
		text := x.AppendFormat(tmp[:0], time.RFC3339Nano)
		b = binary.AppendUvarint(append(b, colTagString), uint64(len(text)))
		return append(b, text...)
	}
	// Everything else goes through the v1 encoder, so both encodings agree.
	return appendColumnarJSON(b, appendCell(nil, v))
}

func appendColumnarString(b []byte, s string) []byte {
	s = validUTF8(s)
	b = binary.AppendUvarint(append(b, colTagString), uint64(len(s)))
	return append(b, s...)
}

func appendColumnarInt(b []byte, n int64) []byte {
	if n > maxSafeInteger || n < -maxSafeInteger {
		var tmp [24]byte
		return appendColumnarString(b, string(strconv.AppendInt(tmp[:0], n, 10)))
	}
	return binary.AppendUvarint(append(b, colTagInt), zigzag(n))
}

func appendColumnarUint(b []byte, n uint64) []byte {
	if n > maxSafeInteger {
		var tmp [24]byte
		return appendColumnarString(b, string(strconv.AppendUint(tmp[:0], n, 10)))
	}
	return binary.AppendUvarint(append(b, colTagInt), zigzag(int64(n)))
}

// appendColumnarJSON appends one normalised v1 JSON cell as a columnar cell.
func appendColumnarJSON(b []byte, enc []byte) []byte {
	if len(enc) == 0 {
		return append(b, colTagNull)
	}
	switch enc[0] {
	case 'n':
		return append(b, colTagNull)
	case 't':
		return append(b, colTagTrue)
	case 'f':
		return append(b, colTagFalse)
	case '"':
		var s string
		if json.Unmarshal(enc, &s) != nil {
			s = string(enc)
		}
		return appendColumnarString(b, s)
	case '{', '[':
		b = binary.AppendUvarint(append(b, colTagJSON), uint64(len(enc)))
		return append(b, enc...)
	}
	text := string(enc)
	for i := 0; i < len(text); i++ {
		if c := text[i]; c == '.' || c == 'e' || c == 'E' {
			if f, err := strconv.ParseFloat(text, 64); err == nil {
				return binary.LittleEndian.AppendUint64(append(b, colTagFloat), math.Float64bits(f))
			}
			return appendColumnarString(b, text)
		}
	}
	if n, err := strconv.ParseInt(text, 10, 64); err == nil {
		return appendColumnarInt(b, n)
	}
	return appendColumnarString(b, text)
}

// validUTF8 replaces every invalid byte with U+FFFD, like appendJSONString.
func validUTF8(s string) string {
	if utf8.ValidString(s) {
		return s
	}
	out := make([]byte, 0, len(s)+8)
	for i := 0; i < len(s); {
		r, size := utf8.DecodeRuneInString(s[i:])
		if r == utf8.RuneError && size == 1 {
			out = append(out, "�"...)
		} else {
			out = append(out, s[i:i+size]...)
		}
		i += size
	}
	return string(out)
}

func zigzag(n int64) uint64 { return uint64(n<<1) ^ uint64(n>>63) }

// columnarCellLen returns the length of the tagged cell at the start of b
// (b is produced by appendColumnarCell) and where its payload starts.
func columnarCellLen(b []byte) (n, payload int) {
	switch b[0] {
	case colTagInt:
		_, size := binary.Uvarint(b[1:])
		return 1 + size, 1
	case colTagFloat:
		return 9, 1
	case colTagString, colTagJSON:
		length, size := binary.Uvarint(b[1:])
		return 1 + size + int(length), 1 + size
	}
	return 1, 1
}

// columnarTagKind maps a cell tag to its column kind.
var columnarTagKind = [...]byte{
	colTagNull: colKindNull, colTagFalse: colKindBool, colTagTrue: colKindBool, colTagInt: colKindInt,
	colTagFloat: colKindFloat, colTagString: colKindString, colTagJSON: colKindJSON,
}

// encodeColumnar regroups the tagged rows of a page (appendColumnarRow)
// into column blocks. The result is at most columnarPageBound(columns,
// Σ len(row)) bytes.
func encodeColumnar(rows [][]byte, columns int, sizeHint int) []byte {
	b := make([]byte, 0, sizeHint)
	b = append(b, columnarMagic...)
	b = binary.AppendUvarint(b, uint64(len(rows)))
	b = binary.AppendUvarint(b, uint64(columns))
	pos := make([]int, len(rows)) // each row's next cell
	for c := 0; c < columns; c++ {
		kind, nulls := byte(colKindNull), false
		for r, row := range rows {
			k := columnarTagKind[row[pos[r]]]
			if k == colKindNull {
				nulls = true
			} else if kind == colKindNull {
				kind = k
			} else if kind != k {
				kind = colKindMixed
				break
			}
		}
		switch kind {
		case colKindNull:
			b = append(b, colKindNull)
			for r := range rows {
				pos[r]++
			}
			continue
		case colKindMixed:
			b = append(b, colKindMixed)
			for r, row := range rows {
				n, _ := columnarCellLen(row[pos[r]:])
				b = append(b, row[pos[r]:pos[r]+n]...)
				pos[r] += n
			}
			continue
		}
		if nulls {
			b = append(b, kind|colHasNulls)
			start := len(b)
			b = append(b, make([]byte, (len(rows)+7)/8)...)
			for r, row := range rows {
				if row[pos[r]] == colTagNull {
					b[start+r/8] |= 1 << (r % 8)
				}
			}
		} else {
			b = append(b, kind)
		}
		switch kind {
		case colKindBool:
			for r, row := range rows {
				switch row[pos[r]] {
				case colTagTrue:
					b = append(b, 1)
				case colTagFalse:
					b = append(b, 0)
				}
				pos[r]++
			}
		case colKindInt, colKindFloat:
			for r, row := range rows {
				n, payload := columnarCellLen(row[pos[r]:])
				if row[pos[r]] != colTagNull {
					b = append(b, row[pos[r]+payload:pos[r]+n]...)
				}
				pos[r] += n
			}
		case colKindString, colKindJSON:
			// Lengths first, then the bytes, so a reader converts the block once.
			for r, row := range rows {
				if row[pos[r]] != colTagNull {
					_, payload := columnarCellLen(row[pos[r]:])
					b = append(b, row[pos[r]+1:pos[r]+payload]...)
				}
			}
			for r, row := range rows {
				n, payload := columnarCellLen(row[pos[r]:])
				if row[pos[r]] != colTagNull {
					b = append(b, row[pos[r]+payload:pos[r]+n]...)
				}
				pos[r] += n
			}
		}
	}
	return b
}
