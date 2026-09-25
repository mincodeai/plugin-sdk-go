package jsvalue

import (
	"math"
	"testing"
)

func TestParseStringify(t *testing.T) {
	for in, want := range map[string]string{
		`{"b":1,"a":[true,null,"x"],"1":2,"0":3}`: `{"0":3,"1":2,"b":1,"a":[true,null,"x"]}`,
		`1e3`:                    `1000`,
		`-0.0`:                   `0`,
		`1e21`:                   `1e+21`,
		`0.0000001`:              `1e-7`,
		`" <&>"`:                 "\" <&>\"",
		`"\u0001\t"`:             `"\u0001\t"`,
		`{"a":1,"a":2}`:          `{"a":2}`,
		`[1.5, 123456789012345]`: `[1.5,123456789012345]`,
	} {
		v, err := Parse([]byte(in))
		if err != nil {
			t.Fatalf("Parse(%s): %v", in, err)
		}
		if got := string(Stringify(v)); got != want {
			t.Errorf("Stringify(Parse(%s)) = %s, want %s", in, got, want)
		}
	}
	for _, bad := range []string{``, `{`, `1 2`, `{"a":}`} {
		if _, err := Parse([]byte(bad)); err == nil {
			t.Errorf("Parse(%q) should fail", bad)
		}
	}
}

func TestObj(t *testing.T) {
	o := O("a", 1.0, "b", "x")
	o.Set("a", 2.0)
	o.SetOpt("c", true, true)
	o.SetOpt("b", nil, false)
	if got := string(Stringify(o)); got != `{"a":2,"c":true}` {
		t.Fatalf("obj = %s", got)
	}
	w := o.Without("a")
	if w.Has("a") || !o.Has("a") || w.Len() != 1 {
		t.Fatal("Without must copy")
	}
	if o.S("missing") != "" || !o.Truthy("c") {
		t.Fatal("accessors")
	}
	var nilObj *Obj
	if nilObj.Len() != 0 || nilObj.Keys() != nil || string(Stringify(nilObj)) != "null" {
		t.Fatal("nil obj")
	}
	if string(Stringify([]string{"a"})) != `["a"]` || string(Stringify(3)) != "3" || string(Stringify(math.NaN())) != "null" {
		t.Fatal("helpers")
	}
}

func TestStrings(t *testing.T) {
	if Trim("　 a ") != "a" || CollapseSpace("a \n\t b") != "a b" || len(SplitSpace("  a  b ")) != 2 {
		t.Fatal("space helpers")
	}
	if Len16("a😀") != 3 || Slice16("a😀b", 0, 3) != "a😀" || Slice16("a😀b", 0, 2) != "a" {
		t.Fatal("utf16 helpers")
	}
	if NumberString(math.Inf(-1)) != "-Infinity" || NumberString(0.1) != "0.1" {
		t.Fatal("NumberString")
	}
	if !Truthy("x") || Truthy("") || Truthy(0.0) || Truthy(nil) || !Truthy(NewObj()) {
		t.Fatal("Truthy")
	}
}
