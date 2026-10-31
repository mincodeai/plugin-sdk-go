package dbdriver

import (
	"database/sql"
	"database/sql/driver"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math/big"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

// SnapshotCell is the lossless encoding of one plugin.db.snapshotWrite value
// (before/after cells and returned values). T is the tag:
//
//	null            nil
//	bool            V "true" / "false"
//	int, uint       V decimal text of an int64 / uint64
//	float           V strconv.FormatFloat(f, 'g', -1, 64) (NaN, +Inf, -Inf too)
//	num             V exact decimal text (json.Number, *big.Int)
//	str             V a valid UTF-8 string
//	strb            V standard base64 of a string that is not valid UTF-8
//	bytes           V standard base64 of a non-nil []byte
//	nbytes          a nil []byte
//	time            V "<unix seconds>.<9-digit nanoseconds>", L the location
//	                kind "utc", "local" or "fixed" (then Z the zone name and
//	                O its offset in seconds)
//
// The shape matches the host's sqlsafety stored values, so the host restores
// exactly what the driver read.
type SnapshotCell struct {
	T string `json:"t"`
	V string `json:"v,omitempty"`
	L string `json:"l,omitempty"`
	Z string `json:"z,omitempty"`
	O int    `json:"o,omitempty"`
}

// EncodeSnapshotCell encodes v. driver.Valuer values are resolved; types
// without a lossless encoding (maps, slices other than []byte, structs) are
// an error, so a snapshot never stores a guessed rendering.
func EncodeSnapshotCell(v any) (SnapshotCell, error) { return encodeSnapshotCell(v, 0) }

func encodeSnapshotCell(v any, valuers int) (SnapshotCell, error) {
	signed := func(n int64) SnapshotCell { return SnapshotCell{T: "int", V: strconv.FormatInt(n, 10)} }
	unsigned := func(n uint64) SnapshotCell { return SnapshotCell{T: "uint", V: strconv.FormatUint(n, 10)} }
	float := func(f float64) SnapshotCell { return SnapshotCell{T: "float", V: strconv.FormatFloat(f, 'g', -1, 64)} }
	switch x := v.(type) {
	case nil:
		return SnapshotCell{T: "null"}, nil
	case bool:
		return SnapshotCell{T: "bool", V: strconv.FormatBool(x)}, nil
	case int:
		return signed(int64(x)), nil
	case int8:
		return signed(int64(x)), nil
	case int16:
		return signed(int64(x)), nil
	case int32:
		return signed(int64(x)), nil
	case int64:
		return signed(x), nil
	case uint:
		return unsigned(uint64(x)), nil
	case uint8:
		return unsigned(uint64(x)), nil
	case uint16:
		return unsigned(uint64(x)), nil
	case uint32:
		return unsigned(uint64(x)), nil
	case uint64:
		return unsigned(x), nil
	case float32:
		return float(float64(x)), nil
	case float64:
		return float(x), nil
	case json.Number:
		return SnapshotCell{T: "num", V: string(x)}, nil
	case *big.Int:
		if x == nil {
			return SnapshotCell{T: "null"}, nil
		}
		return SnapshotCell{T: "num", V: x.String()}, nil
	case string:
		if utf8.ValidString(x) {
			return SnapshotCell{T: "str", V: x}, nil
		}
		return SnapshotCell{T: "strb", V: base64.StdEncoding.EncodeToString([]byte(x))}, nil
	case []byte:
		if x == nil {
			return SnapshotCell{T: "nbytes"}, nil
		}
		return SnapshotCell{T: "bytes", V: base64.StdEncoding.EncodeToString(x)}, nil
	case sql.RawBytes:
		return encodeSnapshotCell([]byte(x), valuers)
	case time.Time:
		c := SnapshotCell{T: "time", V: fmt.Sprintf("%d.%09d", x.Unix(), x.Nanosecond())}
		switch x.Location() {
		case time.UTC:
			c.L = "utc"
		case time.Local:
			c.L = "local"
		default:
			c.L = "fixed"
			c.Z, c.O = x.Zone()
		}
		return c, nil
	case *time.Time:
		if x == nil {
			return SnapshotCell{T: "null"}, nil
		}
		return encodeSnapshotCell(*x, valuers)
	case driver.Valuer:
		if rv := reflect.ValueOf(x); rv.Kind() == reflect.Pointer && rv.IsNil() {
			return SnapshotCell{T: "null"}, nil
		}
		if valuers < maxValuerDepth {
			val, err := x.Value()
			if err != nil {
				return SnapshotCell{}, err
			}
			return encodeSnapshotCell(val, valuers+1)
		}
	}
	return SnapshotCell{}, fmt.Errorf("snapshot value of type %T has no lossless encoding", v)
}

// DecodeSnapshotCell returns the value EncodeSnapshotCell encoded: nil,
// bool, int64, uint64, float64, json.Number, string, []byte or time.Time.
func DecodeSnapshotCell(c SnapshotCell) (any, error) {
	switch c.T {
	case "null":
		return nil, nil
	case "bool":
		return strconv.ParseBool(c.V)
	case "int":
		return strconv.ParseInt(c.V, 10, 64)
	case "uint":
		return strconv.ParseUint(c.V, 10, 64)
	case "float":
		return strconv.ParseFloat(c.V, 64)
	case "num":
		return json.Number(c.V), nil
	case "str":
		return c.V, nil
	case "strb":
		b, err := base64.StdEncoding.DecodeString(c.V)
		return string(b), err
	case "bytes":
		return base64.StdEncoding.DecodeString(c.V)
	case "nbytes":
		return []byte(nil), nil
	case "time":
		secText, nsecText, ok := strings.Cut(c.V, ".")
		sec, errSec := strconv.ParseInt(secText, 10, 64)
		nsec, errNsec := strconv.ParseInt(nsecText, 10, 64)
		if !ok || errSec != nil || errNsec != nil {
			return nil, fmt.Errorf("invalid snapshot time %q", c.V)
		}
		t := time.Unix(sec, nsec)
		switch c.L {
		case "utc":
			return t.UTC(), nil
		case "local":
			return t.In(time.Local), nil
		case "fixed":
			return t.In(time.FixedZone(c.Z, c.O)), nil
		}
		return nil, fmt.Errorf("invalid snapshot time location kind %q", c.L)
	}
	return nil, fmt.Errorf("unknown snapshot cell tag %q", c.T)
}

// appendSnapshotCell appends the JSON of v's SnapshotCell.
func appendSnapshotCell(b []byte, v any) ([]byte, error) {
	c, err := EncodeSnapshotCell(v)
	if err != nil {
		return nil, Errorf(KindDatabase, "%s", err.Error())
	}
	b = append(b, `{"t":`...)
	b = appendJSONString(b, c.T)
	if c.V != "" {
		b = append(b, `,"v":`...)
		b = appendJSONString(b, c.V)
	}
	if c.L != "" {
		b = append(b, `,"l":`...)
		b = appendJSONString(b, c.L)
	}
	if c.Z != "" {
		b = append(b, `,"z":`...)
		b = appendJSONString(b, c.Z)
	}
	if c.O != 0 {
		b = append(b, `,"o":`...)
		b = strconv.AppendInt(b, int64(c.O), 10)
	}
	return append(b, '}'), nil
}

// appendSnapshotRow appends row number n (0-based) of a snapshot result.
func appendSnapshotRow(b []byte, n int, row []any) ([]byte, error) {
	if n > 0 {
		b = append(b, ',')
	}
	b = append(b, '[')
	for j, v := range row {
		if j > 0 {
			b = append(b, ',')
		}
		var err error
		if b, err = appendSnapshotCell(b, v); err != nil {
			return nil, err
		}
	}
	return append(b, ']'), nil
}

// appendSnapshotReturned appends `,"returned":{column:cell,…}` (sorted).
func appendSnapshotReturned(b []byte, m map[string]any) ([]byte, error) {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	b = append(b, `,"returned":{`...)
	for i, k := range keys {
		if i > 0 {
			b = append(b, ',')
		}
		b = appendJSONString(b, k)
		b = append(b, ':')
		var err error
		if b, err = appendSnapshotCell(b, m[k]); err != nil {
			return nil, err
		}
	}
	return append(b, '}'), nil
}

// SnapshotResultSize returns the encoded size of a Before/After result with
// cols and rows, as counted against SnapshotRequest.MaxBytes; the error
// reports a value without a lossless encoding.
func SnapshotResultSize(cols []Column, rows [][]any) (int, error) {
	b, err := appendSnapshotResult(nil, cols, rows)
	return len(b), err
}

// SnapshotReturnedSize returns the encoded size of a step's Returned values
// (member name included), as counted against SnapshotRequest.MaxBytes.
func SnapshotReturnedSize(returned map[string]any) (int, error) {
	if returned == nil {
		return 0, nil
	}
	b, err := appendSnapshotReturned(nil, returned)
	return len(b), err
}
