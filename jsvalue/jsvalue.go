package jsvalue

// JS-compatible JSON values. The plugin was ported from Node.js; stored records
// were produced by JSON.stringify and user data must round-trip unchanged, so
// values are modelled like JavaScript sees them: ordered objects (with JS
// property order), float64 numbers, UTF-16 string lengths and JS whitespace.
//
// Value types: nil (null), bool, float64, string, []any, *Obj. A missing key is
// "undefined" and is reported by Obj.Get's second result.

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"math"
	"sort"
	"strconv"
	"strings"
	"unicode/utf16"
	"unicode/utf8"
)

// Obj is an insertion-ordered JSON object.
type Obj struct {
	keys []string
	vals map[string]any
}

func NewObj() *Obj { return &Obj{vals: map[string]any{}} }

// O builds an object from alternating key/value pairs.
func O(pairs ...any) *Obj {
	o := NewObj()
	for i := 0; i+1 < len(pairs); i += 2 {
		o.Set(pairs[i].(string), pairs[i+1])
	}
	return o
}

func (o *Obj) Get(k string) (any, bool) {
	if o == nil {
		return nil, false
	}
	v, ok := o.vals[k]
	return v, ok
}

func (o *Obj) Has(k string) bool { _, ok := o.Get(k); return ok }

// Set assigns like a JS property assignment: existing keys keep their position.
func (o *Obj) Set(k string, v any) {
	if _, ok := o.vals[k]; !ok {
		o.keys = append(o.keys, k)
	}
	o.vals[k] = v
}

// SetOpt assigns v when present, otherwise deletes k (JS `{k: undefined}` is omitted by JSON).
func (o *Obj) SetOpt(k string, v any, present bool) {
	if present {
		o.Set(k, v)
	} else {
		o.Delete(k)
	}
}

func (o *Obj) Delete(k string) {
	if _, ok := o.vals[k]; !ok {
		return
	}
	delete(o.vals, k)
	for i, key := range o.keys {
		if key == k {
			o.keys = append(o.keys[:i:i], o.keys[i+1:]...)
			break
		}
	}
}

// Keys returns keys in JS own-property order: array indices ascending, then insertion order.
func (o *Obj) Keys() []string {
	if o == nil {
		return nil
	}
	var index, named []string
	for _, k := range o.keys {
		if isArrayIndex(k) {
			index = append(index, k)
		} else {
			named = append(named, k)
		}
	}
	if len(index) == 0 {
		return named
	}
	sort.SliceStable(index, func(i, j int) bool {
		a, _ := strconv.ParseUint(index[i], 10, 64)
		b, _ := strconv.ParseUint(index[j], 10, 64)
		return a < b
	})
	return append(index, named...)
}

func (o *Obj) Len() int {
	if o == nil {
		return 0
	}
	return len(o.keys)
}

// Clone is a shallow copy, like `{...o}`.
func (o *Obj) Clone() *Obj {
	c := NewObj()
	for _, k := range o.Keys() {
		c.Set(k, o.vals[k])
	}
	return c
}

// Without is `const {a, b, ...rest} = o`.
func (o *Obj) Without(keys ...string) *Obj {
	c := o.Clone()
	for _, k := range keys {
		c.Delete(k)
	}
	return c
}

// Str returns o[k] when it is a string.
func (o *Obj) Str(k string) (string, bool) {
	v, _ := o.Get(k)
	s, ok := v.(string)
	return s, ok
}

// S returns o[k] as a string, or "" when it is not one.
func (o *Obj) S(k string) string { s, _ := o.Str(k); return s }

func (o *Obj) Truthy(k string) bool { v, ok := o.Get(k); return ok && Truthy(v) }

func isArrayIndex(k string) bool {
	if k == "" || len(k) > 10 || (len(k) > 1 && k[0] == '0') {
		return false
	}
	for i := 0; i < len(k); i++ {
		if k[i] < '0' || k[i] > '9' {
			return false
		}
	}
	n, err := strconv.ParseUint(k, 10, 64)
	return err == nil && n < math.MaxUint32
}

// truthy is JS Boolean(v) for a present value.
func Truthy(v any) bool {
	switch x := v.(type) {
	case nil:
		return false
	case bool:
		return x
	case float64:
		return x != 0 && !math.IsNaN(x)
	case string:
		return x != ""
	default:
		return true
	}
}

// nullish reports whether `v ?? fallback` falls back.
func Nullish(v any, present bool) bool { return !present || v == nil }

// coalesce implements `a ?? b` for (value, present) pairs.
func Coalesce(v any, present bool, w any, wPresent bool) (any, bool) {
	if Nullish(v, present) {
		return w, wPresent
	}
	return v, true
}

// toNumber is JS Number(v) for JSON values.
func ToNumber(v any, present bool) float64 {
	if !present {
		return math.NaN()
	}
	switch x := v.(type) {
	case nil:
		return 0
	case bool:
		if x {
			return 1
		}
		return 0
	case float64:
		return x
	case string:
		s := Trim(x)
		if s == "" {
			return 0
		}
		if strings.HasPrefix(s, "0x") || strings.HasPrefix(s, "0X") {
			if n, err := strconv.ParseUint(s[2:], 16, 64); err == nil {
				return float64(n)
			}
			return math.NaN()
		}
		switch s {
		case "Infinity", "+Infinity":
			return math.Inf(1)
		case "-Infinity":
			return math.Inf(-1)
		}
		for _, r := range s {
			if !(r >= '0' && r <= '9' || r == '.' || r == 'e' || r == 'E' || r == '+' || r == '-') {
				return math.NaN()
			}
		}
		f, err := strconv.ParseFloat(s, 64)
		if err != nil {
			return math.NaN()
		}
		return f
	case []any:
		switch len(x) {
		case 0:
			return 0
		case 1:
			if _, isArr := x[0].([]any); !isArr {
				if _, isObj := x[0].(*Obj); !isObj {
					return ToNumber(x[0], x[0] != nil)
				}
			}
		}
		return math.NaN()
	default:
		return math.NaN()
	}
}

func IsInteger(f float64) bool { return !math.IsNaN(f) && !math.IsInf(f, 0) && f == math.Trunc(f) }

// numberString is JS String(number).
func NumberString(f float64) string {
	switch {
	case math.IsNaN(f):
		return "NaN"
	case math.IsInf(f, 1):
		return "Infinity"
	case math.IsInf(f, -1):
		return "-Infinity"
	case f == 0:
		return "0"
	}
	abs := math.Abs(f)
	if abs >= 1e-6 && abs < 1e21 {
		return strconv.FormatFloat(f, 'f', -1, 64)
	}
	s := strconv.FormatFloat(f, 'e', -1, 64)
	mant, exp, _ := strings.Cut(s, "e")
	sign := exp[0]
	exp = strings.TrimLeft(exp[1:], "0")
	return mant + "e" + string(sign) + exp
}

// ---- strings ----

func IsSpace(r rune) bool {
	switch r {
	case '\t', '\n', '\v', '\f', '\r', ' ', 0xA0, 0x1680, 0x2028, 0x2029, 0x202F, 0x205F, 0x3000, 0xFEFF:
		return true
	}
	return r >= 0x2000 && r <= 0x200A
}

// jsTrim is String.prototype.trim.
func Trim(s string) string { return strings.TrimFunc(s, IsSpace) }

// collapseSpace is s.replace(/\s+/g, ' ').
func CollapseSpace(s string) string {
	var b strings.Builder
	inSpace := false
	for _, r := range s {
		if IsSpace(r) {
			if !inSpace {
				b.WriteByte(' ')
			}
			inSpace = true
			continue
		}
		inSpace = false
		b.WriteRune(r)
	}
	return b.String()
}

// splitSpace is s.split(/\s+/).filter(Boolean).
func SplitSpace(s string) []string { return strings.FieldsFunc(s, IsSpace) }

// jsLower approximates toLowerCase/toLocaleLowerCase (root locale).
func Lower(s string) string {
	if strings.ContainsRune(s, 0x130) {
		s = strings.ReplaceAll(s, "İ", "i̇")
	}
	return strings.ToLower(s)
}

// u16len is JS string .length.
func Len16(s string) int {
	n := 0
	for _, r := range s {
		if r >= 0x10000 {
			n += 2
		} else {
			n++
		}
	}
	return n
}

// u16slice is JS s.slice(start, end) with non-negative bounds. A surrogate pair
// straddling a bound is dropped rather than split (Go strings cannot hold lone
// surrogates).
func Slice16(s string, start, end int) string {
	if start < 0 {
		start = 0
	}
	if end <= start {
		return ""
	}
	pos, from, to := 0, -1, len(s)
	for i, r := range s {
		w := 1
		if r >= 0x10000 {
			w = 2
		}
		if from < 0 && pos >= start {
			from = i
		}
		if pos+w > end {
			to = i
			break
		}
		pos += w
	}
	if from < 0 {
		return ""
	}
	if to < from {
		return ""
	}
	return s[from:to]
}

// u16index converts a byte offset in s to a UTF-16 index.
func Index16(s string, byteOffset int) int { return Len16(s[:byteOffset]) }

// u16compare orders strings by UTF-16 code units, like Array.prototype.sort().
func Compare16(a, b string) int {
	for a != "" && b != "" {
		ra, na := utf8.DecodeRuneInString(a)
		rb, nb := utf8.DecodeRuneInString(b)
		if ra != rb {
			ua, ub := utf16Units(ra), utf16Units(rb)
			for i := 0; i < len(ua) && i < len(ub); i++ {
				if ua[i] != ub[i] {
					if ua[i] < ub[i] {
						return -1
					}
					return 1
				}
			}
			if len(ua) != len(ub) {
				if len(ua) < len(ub) {
					return -1
				}
				return 1
			}
		}
		a, b = a[na:], b[nb:]
	}
	switch {
	case a == "" && b == "":
		return 0
	case a == "":
		return -1
	}
	return 1
}

func utf16Units(r rune) []uint16 {
	if r >= 0x10000 {
		h, l := utf16.EncodeRune(r)
		return []uint16{uint16(h), uint16(l)}
	}
	return []uint16{uint16(r)}
}

// ---- JSON ----

var errBadJSON = errors.New("invalid JSON")

// parseJSON decodes JSON into JS-like values (JSON.parse).
func Parse(data []byte) (any, error) {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	v, err := decodeValue(dec)
	if err != nil {
		return nil, errBadJSON
	}
	if _, err := dec.Token(); err != io.EOF {
		return nil, errBadJSON
	}
	return v, nil
}

func decodeValue(dec *json.Decoder) (any, error) {
	tok, err := dec.Token()
	if err != nil {
		return nil, err
	}
	switch t := tok.(type) {
	case json.Delim:
		switch t {
		case '{':
			o := NewObj()
			for dec.More() {
				kt, err := dec.Token()
				if err != nil {
					return nil, err
				}
				k, ok := kt.(string)
				if !ok {
					return nil, errBadJSON
				}
				v, err := decodeValue(dec)
				if err != nil {
					return nil, err
				}
				o.Set(k, v)
			}
			if _, err := dec.Token(); err != nil {
				return nil, err
			}
			return o, nil
		case '[':
			arr := []any{}
			for dec.More() {
				v, err := decodeValue(dec)
				if err != nil {
					return nil, err
				}
				arr = append(arr, v)
			}
			if _, err := dec.Token(); err != nil {
				return nil, err
			}
			return arr, nil
		}
		return nil, errBadJSON
	case json.Number:
		f, err := strconv.ParseFloat(string(t), 64)
		if err != nil && !errors.Is(err, strconv.ErrRange) {
			return nil, err
		}
		return f, nil
	case string, bool, nil:
		return t, nil
	}
	return nil, errBadJSON
}

// stringify is JSON.stringify for JS-like values (also accepts int and []string / []*Obj helpers).
func Stringify(v any) []byte {
	var b bytes.Buffer
	Write(&b, v)
	return b.Bytes()
}

func Write(b *bytes.Buffer, v any) {
	switch x := v.(type) {
	case nil:
		b.WriteString("null")
	case bool:
		if x {
			b.WriteString("true")
		} else {
			b.WriteString("false")
		}
	case float64:
		if math.IsNaN(x) || math.IsInf(x, 0) {
			b.WriteString("null")
		} else {
			b.WriteString(NumberString(x))
		}
	case int:
		b.WriteString(strconv.Itoa(x))
	case string:
		WriteString(b, x)
	case []any:
		b.WriteByte('[')
		for i, item := range x {
			if i > 0 {
				b.WriteByte(',')
			}
			Write(b, item)
		}
		b.WriteByte(']')
	case []string:
		b.WriteByte('[')
		for i, item := range x {
			if i > 0 {
				b.WriteByte(',')
			}
			WriteString(b, item)
		}
		b.WriteByte(']')
	case []*Obj:
		b.WriteByte('[')
		for i, item := range x {
			if i > 0 {
				b.WriteByte(',')
			}
			Write(b, item)
		}
		b.WriteByte(']')
	case *Obj:
		if x == nil {
			b.WriteString("null")
			return
		}
		b.WriteByte('{')
		for i, k := range x.Keys() {
			if i > 0 {
				b.WriteByte(',')
			}
			WriteString(b, k)
			b.WriteByte(':')
			Write(b, x.vals[k])
		}
		b.WriteByte('}')
	case json.RawMessage:
		b.Write(x)
	default:
		raw, err := json.Marshal(x)
		if err != nil {
			b.WriteString("null")
			return
		}
		b.Write(raw)
	}
}

// writeString mirrors JSON.stringify string escaping (U+2028/2029 stay literal).
func WriteString(b *bytes.Buffer, s string) {
	const hex = "0123456789abcdef"
	b.WriteByte('"')
	for _, r := range s {
		switch r {
		case '"':
			b.WriteString(`\"`)
		case '\\':
			b.WriteString(`\\`)
		case '\b':
			b.WriteString(`\b`)
		case '\f':
			b.WriteString(`\f`)
		case '\n':
			b.WriteString(`\n`)
		case '\r':
			b.WriteString(`\r`)
		case '\t':
			b.WriteString(`\t`)
		default:
			if r < 0x20 {
				b.WriteString(`\u00`)
				b.WriteByte(hex[r>>4])
				b.WriteByte(hex[r&0xF])
			} else {
				b.WriteRune(r)
			}
		}
	}
	b.WriteByte('"')
}

// jsonEqual compares two values the way JSON.Stringify(a) === JSON.Stringify(b) does,
// where a missing value stringifies to undefined.
func Equal(a any, aPresent bool, b any, bPresent bool) bool {
	if !aPresent || !bPresent {
		return aPresent == bPresent
	}
	return bytes.Equal(Stringify(a), Stringify(b))
}

// deepClone copies a JS-like value (structuredClone for JSON data).
func DeepClone(v any) any {
	switch x := v.(type) {
	case []any:
		out := make([]any, len(x))
		for i, item := range x {
			out[i] = DeepClone(item)
		}
		return out
	case *Obj:
		if x == nil {
			return x
		}
		c := NewObj()
		for _, k := range x.Keys() {
			c.Set(k, DeepClone(x.vals[k]))
		}
		return c
	}
	return v
}
