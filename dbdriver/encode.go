package dbdriver

import (
	"bytes"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"math"
	"math/big"
	"reflect"
	"sort"
	"strconv"
	"time"
	"unicode/utf8"
)

const (
	// maxSafeInteger is 2^53: integers beyond ±maxSafeInteger are sent as
	// strings so JavaScript keeps every digit.
	maxSafeInteger = 1 << 53
	// maxCellDepth bounds object/array nesting (contract cellDepthMax).
	// Deeper values are sent as their JSON text.
	maxCellDepth = 16
	// maxValuerDepth bounds driver.Valuer chains.
	maxValuerDepth = 4
)

var hexDigits = "0123456789abcdef"

// appendJSONString appends s as a JSON string like encoding/json without
// HTML escaping: invalid UTF-8 becomes U+FFFD, U+2028/2029 are escaped.
func appendJSONString(b []byte, s string) []byte {
	b = append(b, '"')
	start := 0
	for i := 0; i < len(s); {
		if c := s[i]; c < utf8.RuneSelf {
			if c >= 0x20 && c != '"' && c != '\\' {
				i++
				continue
			}
			b = append(b, s[start:i]...)
			switch c {
			case '"', '\\':
				b = append(b, '\\', c)
			case '\n':
				b = append(b, '\\', 'n')
			case '\r':
				b = append(b, '\\', 'r')
			case '\t':
				b = append(b, '\\', 't')
			default:
				b = append(b, '\\', 'u', '0', '0', hexDigits[c>>4], hexDigits[c&0xF])
			}
			i++
			start = i
			continue
		}
		r, size := utf8.DecodeRuneInString(s[i:])
		if r == utf8.RuneError && size == 1 {
			b = append(b, s[start:i]...)
			b = append(b, `�`...)
			i += size
			start = i
			continue
		}
		if r == ' ' || r == ' ' {
			b = append(b, s[start:i]...)
			b = append(b, '\\', 'u', '2', '0', '2', hexDigits[r&0xF])
			i += size
			start = i
			continue
		}
		i += size
	}
	b = append(b, s[start:]...)
	return append(b, '"')
}

// appendCell appends one normalised cell value.
func appendCell(b []byte, v any) []byte { return appendValue(b, v, 0, 0) }

func appendValue(b []byte, v any, depth, valuers int) []byte {
	switch x := v.(type) {
	case nil:
		return append(b, "null"...)
	case bool:
		return strconv.AppendBool(b, x)
	case string:
		return appendJSONString(b, x)
	case []byte:
		return appendJSONString(b, string(x))
	case int:
		return appendInt(b, int64(x))
	case int8:
		return appendInt(b, int64(x))
	case int16:
		return appendInt(b, int64(x))
	case int32:
		return appendInt(b, int64(x))
	case int64:
		return appendInt(b, x)
	case uint:
		return appendUint(b, uint64(x))
	case uint8:
		return appendUint(b, uint64(x))
	case uint16:
		return appendUint(b, uint64(x))
	case uint32:
		return appendUint(b, uint64(x))
	case uint64:
		return appendUint(b, x)
	case float32:
		return appendFloat(b, float64(x), 32)
	case float64:
		return appendFloat(b, x, 64)
	case time.Time:
		return appendJSONString(b, x.Format(time.RFC3339Nano))
	case *time.Time:
		if x == nil {
			return append(b, "null"...)
		}
		return appendJSONString(b, x.Format(time.RFC3339Nano))
	case json.Number:
		return appendNumber(b, string(x))
	case *big.Int:
		if x == nil {
			return append(b, "null"...)
		}
		if x.IsInt64() {
			return appendInt(b, x.Int64())
		}
		return appendJSONString(b, x.String())
	case json.RawMessage:
		return appendRaw(b, x, depth)
	case map[string]any:
		if depth >= maxCellDepth {
			return appendAsText(b, x)
		}
		keys := make([]string, 0, len(x))
		for k := range x {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		b = append(b, '{')
		for i, k := range keys {
			if i > 0 {
				b = append(b, ',')
			}
			b = appendJSONString(b, k)
			b = append(b, ':')
			b = appendValue(b, x[k], depth+1, valuers)
		}
		return append(b, '}')
	case []any:
		if depth >= maxCellDepth {
			return appendAsText(b, x)
		}
		b = append(b, '[')
		for i, e := range x {
			if i > 0 {
				b = append(b, ',')
			}
			b = appendValue(b, e, depth+1, valuers)
		}
		return append(b, ']')
	case driver.Valuer:
		if valuers < maxValuerDepth {
			if rv := reflect.ValueOf(x); rv.Kind() == reflect.Pointer && rv.IsNil() {
				return append(b, "null"...)
			}
			if val, err := x.Value(); err == nil {
				return appendValue(b, val, depth, valuers+1)
			}
		}
		return appendJSONString(b, fmt.Sprint(v))
	case error:
		return appendJSONString(b, x.Error())
	}
	return appendFallback(b, v, depth)
}

func appendInt(b []byte, n int64) []byte {
	if n > maxSafeInteger || n < -maxSafeInteger {
		b = append(b, '"')
		b = strconv.AppendInt(b, n, 10)
		return append(b, '"')
	}
	return strconv.AppendInt(b, n, 10)
}

func appendUint(b []byte, n uint64) []byte {
	if n > maxSafeInteger {
		b = append(b, '"')
		b = strconv.AppendUint(b, n, 10)
		return append(b, '"')
	}
	return strconv.AppendUint(b, n, 10)
}

func appendFloat(b []byte, f float64, bits int) []byte {
	if math.IsNaN(f) || math.IsInf(f, 0) {
		return appendJSONString(b, strconv.FormatFloat(f, 'g', -1, bits))
	}
	// encoding/json float formatting (ES6 style).
	abs := math.Abs(f)
	format := byte('f')
	if abs != 0 {
		if bits == 64 && (abs < 1e-6 || abs >= 1e21) || bits == 32 && (float32(abs) < 1e-6 || float32(abs) >= 1e21) {
			format = 'e'
		}
	}
	start := len(b)
	b = strconv.AppendFloat(b, f, format, -1, bits)
	if format == 'e' {
		// clean up e-09 to e-9
		n := len(b) - start
		if n >= 4 && b[len(b)-4] == 'e' && b[len(b)-3] == '-' && b[len(b)-2] == '0' {
			b[len(b)-2] = b[len(b)-1]
			b = b[:len(b)-1]
		}
	}
	return b
}

// appendNumber appends a JSON number text, as a string when it is an
// integer beyond ±2^53 (or not a valid number at all).
func appendNumber(b []byte, s string) []byte {
	if !json.Valid([]byte(s)) || s == "" || (s[0] != '-' && (s[0] < '0' || s[0] > '9')) {
		return appendJSONString(b, s)
	}
	integer := true
	for i := 0; i < len(s); i++ {
		if c := s[i]; c == '.' || c == 'e' || c == 'E' {
			integer = false
			break
		}
	}
	if integer {
		if n, err := strconv.ParseInt(s, 10, 64); err != nil || n > maxSafeInteger || n < -maxSafeInteger {
			return appendJSONString(b, s)
		}
	}
	return append(b, s...)
}

// appendRaw re-encodes a JSON value through the normaliser (number rules,
// depth limit). Invalid JSON is sent as its text.
func appendRaw(b []byte, raw json.RawMessage, depth int) []byte {
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	var v any
	if err := d.Decode(&v); err != nil || d.More() {
		return appendJSONString(b, string(raw))
	}
	return appendValue(b, v, depth, 0)
}

// appendAsText writes a too-deep container as its JSON text.
func appendAsText(b []byte, v any) []byte {
	raw, err := json.Marshal(v)
	if err != nil {
		return appendJSONString(b, fmt.Sprint(v))
	}
	return appendJSONString(b, string(raw))
}

// appendFallback handles any other type through encoding/json (structs,
// typed slices and maps, pointers, json.Marshaler), then normalises the
// decoded value; values that do not marshal are sent as fmt.Sprint text.
func appendFallback(b []byte, v any, depth int) []byte {
	rv := reflect.ValueOf(v)
	if rv.Kind() == reflect.Pointer && rv.IsNil() {
		return append(b, "null"...)
	}
	switch rv.Kind() {
	case reflect.String:
		return appendJSONString(b, rv.String())
	case reflect.Bool:
		return strconv.AppendBool(b, rv.Bool())
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		if _, ok := v.(json.Marshaler); !ok {
			return appendInt(b, rv.Int())
		}
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		if _, ok := v.(json.Marshaler); !ok {
			return appendUint(b, rv.Uint())
		}
	case reflect.Float32, reflect.Float64:
		if _, ok := v.(json.Marshaler); !ok {
			return appendFloat(b, rv.Float(), rv.Type().Bits())
		}
	case reflect.Slice:
		if rv.Type().Elem().Kind() == reflect.Uint8 {
			if _, ok := v.(json.Marshaler); !ok {
				return appendJSONString(b, string(rv.Bytes()))
			}
		}
	}
	raw, err := json.Marshal(v)
	if err != nil {
		return appendJSONString(b, fmt.Sprint(v))
	}
	return appendRaw(b, raw, depth)
}
