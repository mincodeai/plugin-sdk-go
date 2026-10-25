package rpc

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

func TestReplyIDEncoding(t *testing.T) {
	cases := []struct {
		name, id, want, wantHTML string
	}{
		{"separators", "\"a\u2028b\u2029c\"", `"a\u2028b\u2029c"`, `"a\u2028b\u2029c"`},
		{"escaped separators", `"a\u2028b\u2029c"`, `"a\u2028b\u2029c"`, `"a\u2028b\u2029c"`},
		{"literal escapes", `"a\\u2028b\\u2029c"`, `"a\\u2028b\\u2029c"`, `"a\\u2028b\\u2029c"`},
		{"padded string", " \"a\u2028b\u2029c\" \t", `"a\u2028b\u2029c"`, `"a\u2028b\u2029c"`},
		{"html", `"<a&b>"`, `"<a&b>"`, `"\u003ca\u0026b\u003e"`},
		{"html and separators", "\"<\u2028&\u2029>\"", `"<\u2028&\u2029>"`, `"\u003c\u2028\u0026\u2029\u003e"`},
		{"large integer", `9007199254740993`, `9007199254740993`, `9007199254740993`},
		{"decimal", `12345678901234567890.123456789`, `12345678901234567890.123456789`, `12345678901234567890.123456789`},
		{"exponent", `1.234567890123456789e+100`, `1.234567890123456789e+100`, `1.234567890123456789e+100`},
		{"negative zero", `-0`, `-0`, `-0`},
		{"null", `null`, `null`, `null`},
	}
	for _, style := range []Style{StyleGo, StyleSorted} {
		for _, escapeHTML := range []bool{false, true} {
			for _, tc := range cases {
				t.Run(fmt.Sprintf("%d/html=%t/%s", style, escapeHTML, tc.name), func(t *testing.T) {
					s := &server{o: Options{Style: style, EscapeHTML: escapeHTML}}
					wantID := tc.want
					if escapeHTML {
						wantID = tc.wantHTML
					}
					for _, rpcErr := range []*Error{nil, NewError(CodeServerError, "failure")} {
						got, err := s.encodeReply(json.RawMessage(tc.id), json.RawMessage(`{}`), rpcErr)
						if err != nil {
							t.Fatal(err)
						}
						var env map[string]json.RawMessage
						if err := json.Unmarshal(got, &env); err != nil {
							t.Fatal(err)
						}
						if string(env["id"]) != wantID {
							t.Errorf("error=%v: id = %s, want %s", rpcErr, env["id"], wantID)
						}
					}
				})
			}
		}
	}
}

func TestReplyIDEncodingJSUnchanged(t *testing.T) {
	for _, escapeHTML := range []bool{false, true} {
		s := &server{o: Options{Style: StyleJS, EscapeHTML: escapeHTML}}
		for _, id := range []string{"\"<\u2028&\u2029>\"", `"<\u2028&\u2029>"`} {
			got, err := s.encodeReply(json.RawMessage(id), json.RawMessage(`{}`), nil)
			if err != nil {
				t.Fatal(err)
			}
			want := "{\"jsonrpc\":\"2.0\",\"id\":\"<\u2028&\u2029>\",\"result\":{}}"
			if string(got) != want {
				t.Errorf("html=%t: got %s, want %s", escapeHTML, got, want)
			}
		}
		got, err := s.encodeReply(json.RawMessage(`9007199254740993`), json.RawMessage(`{}`), nil)
		if err != nil {
			t.Fatal(err)
		}
		if string(got) != `{"jsonrpc":"2.0","id":9007199254740992,"result":{}}` {
			t.Errorf("JS number semantics changed: %s", got)
		}
	}
}

func TestReplyResultFastIDEncoding(t *testing.T) {
	result := json.RawMessage("{\"value\":\"<\u2028&\u2029>\"}")
	for _, id := range []string{"\"<\u2028&\u2029>\"", `"<\u2028&\u2029>"`, `9007199254740993`, `1.234567890123456789e+100`, `-0`} {
		t.Run(id, func(t *testing.T) {
			var out bytes.Buffer
			s := &server{out: &out, o: Options{Style: StyleGo}}
			// The ID must be encoded exactly as encodeReply does; the result is
			// written as is (encodeReply would also escape its U+2028/U+2029).
			envelope, err := s.encodeReply(json.RawMessage(id), json.RawMessage(`{}`), nil)
			if err != nil {
				t.Fatal(err)
			}
			want := strings.Replace(string(envelope), `"result":{}`, `"result":`+string(result), 1)
			if !s.replyResultFastLocked(json.RawMessage(id), result) {
				t.Fatal("fastpath rejected valid result")
			}
			if got := out.String(); got != want+"\n" {
				t.Errorf("got %s, want %s", got, want)
			}
			if !bytes.Contains(out.Bytes(), result) {
				t.Fatal("fastpath changed result bytes")
			}
		})
	}
	var out bytes.Buffer
	s := &server{out: &out, o: Options{Style: StyleGo, MaxReplyBytes: len("{\"jsonrpc\":\"2.0\",\"id\":\"\u2028\",\"result\":{}}\n")}}
	if s.replyResultFastLocked(json.RawMessage("\"\u2028\""), json.RawMessage(`{}`)) || out.Len() != 0 {
		t.Fatal("fastpath must apply the reply size limit after escaping the ID")
	}
}
