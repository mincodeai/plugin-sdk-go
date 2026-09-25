package jsonx

import (
	"errors"
	"testing"
)

func TestStrict(t *testing.T) {
	cases := []struct {
		in   string
		want bool
	}{
		{`{"a":1}`, true},
		{` {"a":[1,{"b":2}]} `, true},
		{`{"a":1,"a":2}`, false},
		{`{"x":{"a":1,"a":2}}`, false},
		{`[{"a":1},{"a":2}]`, true},
		{`"😀"`, true},
		{`"\ud83d"`, false},
		{`"\ude00"`, false},
		{"\"\xff\"", false},
		{`{"a":1} {}`, false},
		{`{"a":`, false},
		{`"\\u0041"`, true},
	}
	for _, c := range cases {
		if got := Strict([]byte(c.in)); got != c.want {
			t.Errorf("Strict(%q) = %v, want %v", c.in, got, c.want)
		}
	}
}

func TestIsObject(t *testing.T) {
	if !IsObject([]byte(" \n{}")) || IsObject([]byte("[]")) || IsObject(nil) {
		t.Fatal("IsObject")
	}
}

func TestDecodeObject(t *testing.T) {
	type T struct {
		Name string `json:"name"`
		N    int    `json:"n"`
	}
	kind := func(raw string, o DecodeOptions) ErrorKind {
		var v T
		err := DecodeObject([]byte(raw), &v, o)
		if err == nil {
			return 0
		}
		var de *DecodeError
		if !errors.As(err, &de) {
			t.Fatalf("%q: not a DecodeError: %v", raw, err)
		}
		if de.Error() == "" {
			t.Fatalf("empty message")
		}
		return de.Kind
	}
	strict := DecodeOptions{Strict: true, DisallowUnknown: true}
	for _, c := range []struct {
		raw  string
		o    DecodeOptions
		want ErrorKind
	}{
		{`{"name":"a","n":1}`, strict, 0},
		{`{"name":"a","name":"b"}`, strict, NotStrict},
		{`[]`, strict, NotObject},
		{`nope`, DecodeOptions{}, Malformed},
		{`{"x":1}`, strict, UnknownField},
		{`{"x":1}`, DecodeOptions{}, 0},
		{`{"n":"s"}`, strict, WrongType},
		{`{"n":null}`, DecodeOptions{RejectNull: true}, NullField},
		{`{} {}`, DecodeOptions{}, TrailingData},
		{`{"name":"xxxxxxxx"}`, DecodeOptions{MaxBytes: 5}, TooLarge},
		{`  `, DecodeOptions{EmptyAsObject: true}, 0},
		{``, DecodeOptions{}, Malformed},
	} {
		if got := kind(c.raw, c.o); got != c.want {
			t.Errorf("%q: kind %v, want %v", c.raw, got, c.want)
		}
	}
	var v T
	var de *DecodeError
	if err := DecodeObject([]byte(`{"zz":1}`), &v, strict); !errors.As(err, &de) || de.Field != "zz" {
		t.Fatalf("unknown field name: %v", err)
	}
	if err := DecodeObject([]byte(`{"n":true}`), &v, strict); !errors.As(err, &de) || de.Field != "n" {
		t.Fatalf("wrong type field: %v", err)
	}
}

func TestMarshal(t *testing.T) {
	b, _ := Marshal(map[string]string{"a": "<&>"})
	if string(b) != `{"a":"<&>"}` {
		t.Fatalf("Marshal = %s", b)
	}
	bs := string(rune(0x5c))
	b, _ = MarshalStyle("<", true)
	if string(b) != `"`+bs+`u003c"` {
		t.Fatalf("MarshalStyle escape = %s", b)
	}
	b, _ = MarshalStyle(string(rune(0x2028)), false)
	if string(b) != `"`+bs+`u2028"` {
		t.Fatalf("U+2028 = %s", b)
	}
	if _, err := Marshal(func() {}); err == nil {
		t.Fatal("expected error")
	}
}
