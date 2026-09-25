// Package jsonx holds the strict JSON helpers shared by official Go plugins:
// the exact strictJSON boundary check every strict plugin used, strict object
// decoding with classified errors, and HTML-escape-free marshalling.
package jsonx

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"strconv"
	"strings"
	"unicode/utf8"
)

// Strict reports whether raw is exactly one JSON value that is valid UTF-8,
// has no unpaired \u surrogate escapes and no duplicate object keys (at any
// depth), followed only by whitespace.
func Strict(raw []byte) bool {
	if !utf8.Valid(raw) || !json.Valid(raw) {
		return false
	}
	for i := 0; i < len(raw); i++ {
		if raw[i] != '\\' {
			continue
		}
		i++
		if i >= len(raw) {
			return false
		}
		if raw[i] != 'u' {
			continue
		}
		if i+4 >= len(raw) {
			return false
		}
		n, e := strconv.ParseUint(string(raw[i+1:i+5]), 16, 16)
		if e != nil {
			return false
		}
		i += 4
		if n >= 0xdc00 && n <= 0xdfff {
			return false
		}
		if n >= 0xd800 && n <= 0xdbff {
			if i+6 >= len(raw) || raw[i+1] != '\\' || raw[i+2] != 'u' {
				return false
			}
			low, e := strconv.ParseUint(string(raw[i+3:i+7]), 16, 16)
			if e != nil || low < 0xdc00 || low > 0xdfff {
				return false
			}
			i += 6
		}
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	var value func() bool
	value = func() bool {
		tok, e := d.Token()
		if e != nil {
			return false
		}
		delim, ok := tok.(json.Delim)
		if !ok {
			return true
		}
		switch delim {
		case '{':
			seen := map[string]bool{}
			for d.More() {
				k, e := d.Token()
				if e != nil {
					return false
				}
				s, ok := k.(string)
				if !ok || seen[s] {
					return false
				}
				seen[s] = true
				if !value() {
					return false
				}
			}
		case '[':
			for d.More() {
				if !value() {
					return false
				}
			}
		default:
			return false
		}
		_, e = d.Token()
		return e == nil
	}
	if !value() {
		return false
	}
	_, e := d.Token()
	return e == io.EOF
}

// IsObject reports whether raw (after leading whitespace) starts a JSON object.
func IsObject(raw []byte) bool {
	t := bytes.TrimLeft(raw, " \t\r\n")
	return len(t) > 0 && t[0] == '{'
}

// ErrorKind classifies a DecodeObject failure so plugins can keep their own
// user-facing wording.
type ErrorKind int

const (
	// TooLarge: input exceeds DecodeOptions.MaxBytes.
	TooLarge ErrorKind = iota + 1
	// NotStrict: Strict(raw) is false.
	NotStrict
	// NotObject: the value is not a JSON object.
	NotObject
	// NullField: a top-level field is null while RejectNull is set.
	NullField
	// UnknownField: a field is not accepted by the target struct.
	UnknownField
	// WrongType: a field has the wrong JSON type (Field names it).
	WrongType
	// TrailingData: more than one JSON value.
	TrailingData
	// Malformed: any other decoding error.
	Malformed
)

// DecodeError describes why DecodeObject rejected its input.
type DecodeError struct {
	Kind  ErrorKind
	Field string // offending field for NullField/UnknownField/WrongType when known
	Err   error  // underlying encoding/json error, if any
}

func (e *DecodeError) Error() string {
	switch e.Kind {
	case TooLarge:
		return "payload too large"
	case NotStrict:
		return "payload is not strict JSON"
	case NotObject:
		return "payload must be a JSON object"
	case NullField:
		return "field " + strconv.Quote(e.Field) + " must not be null"
	case UnknownField:
		return "unknown field " + strconv.Quote(e.Field)
	case WrongType:
		return "field " + strconv.Quote(e.Field) + " has the wrong type"
	case TrailingData:
		return "unexpected data after the JSON value"
	}
	if e.Err != nil {
		return "malformed JSON: " + e.Err.Error()
	}
	return "malformed JSON"
}

func (e *DecodeError) Unwrap() error { return e.Err }

// DecodeOptions tunes DecodeObject. The zero value accepts any object that
// encoding/json accepts.
type DecodeOptions struct {
	MaxBytes        int  // 0: unlimited
	Strict          bool // require Strict(raw)
	RejectNull      bool // top-level null fields are errors
	DisallowUnknown bool
	UseNumber       bool
	EmptyAsObject   bool // empty or whitespace-only input decodes as {}
}

// DecodeObject decodes a JSON object into v with the checks in o, returning a
// *DecodeError on failure.
func DecodeObject(raw []byte, v any, o DecodeOptions) error {
	if o.MaxBytes > 0 && len(raw) > o.MaxBytes {
		return &DecodeError{Kind: TooLarge}
	}
	if o.EmptyAsObject && len(bytes.TrimSpace(raw)) == 0 {
		raw = []byte("{}")
	}
	if o.Strict && !Strict(raw) {
		return &DecodeError{Kind: NotStrict}
	}
	if !IsObject(raw) {
		if !json.Valid(raw) {
			return &DecodeError{Kind: Malformed}
		}
		return &DecodeError{Kind: NotObject}
	}
	if o.RejectNull {
		var fields map[string]json.RawMessage
		if err := json.Unmarshal(raw, &fields); err != nil {
			return &DecodeError{Kind: Malformed, Err: err}
		}
		for k, f := range fields {
			if string(f) == "null" {
				return &DecodeError{Kind: NullField, Field: k}
			}
		}
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	if o.DisallowUnknown {
		d.DisallowUnknownFields()
	}
	if o.UseNumber {
		d.UseNumber()
	}
	if err := d.Decode(v); err != nil {
		return classify(err)
	}
	if _, err := d.Token(); err != io.EOF {
		return &DecodeError{Kind: TrailingData}
	}
	return nil
}

func classify(err error) *DecodeError {
	var te *json.UnmarshalTypeError
	if errors.As(err, &te) {
		return &DecodeError{Kind: WrongType, Field: te.Field, Err: err}
	}
	if msg := err.Error(); strings.HasPrefix(msg, "json: unknown field ") {
		f, _ := strconv.Unquote(strings.TrimPrefix(msg, "json: unknown field "))
		return &DecodeError{Kind: UnknownField, Field: f, Err: err}
	}
	return &DecodeError{Kind: Malformed, Err: err}
}

// Marshal encodes v like json.Marshal but without HTML escaping of <, > and &.
func Marshal(v any) ([]byte, error) {
	var b bytes.Buffer
	enc := json.NewEncoder(&b)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return nil, err
	}
	return bytes.TrimSuffix(b.Bytes(), []byte("\n")), nil
}

// MarshalStyle encodes v with (escapeHTML=true, like json.Marshal) or without
// HTML escaping.
func MarshalStyle(v any, escapeHTML bool) ([]byte, error) {
	if escapeHTML {
		return json.Marshal(v)
	}
	return Marshal(v)
}
