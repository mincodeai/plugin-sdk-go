package dbdriver

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	"math/big"
	"strings"
	"testing"
	"time"
)

func TestSecretNeverPrints(t *testing.T) {
	s := NewSecret("hunter2")
	cfg := Config{Password: s, Token: NewSecret("tok-123")}
	var logs strings.Builder
	slog.New(slog.NewTextHandler(&logs, nil)).Info("x", "secret", s, "cfg", cfg)
	j, _ := json.Marshal(cfg)
	txt, _ := s.MarshalText()
	outs := []string{
		s.String(), s.GoString(), fmt.Sprint(s), fmt.Sprintf("%v %+v %#v %s %q %x %d", s, s, s, s, s, s, s),
		fmt.Sprintf("%v %+v %#v", cfg, cfg, cfg), fmt.Sprintf("%v", &cfg), string(j), string(txt), logs.String(),
	}
	for _, o := range outs {
		if strings.Contains(o, "hunter2") || strings.Contains(o, "tok-123") {
			t.Fatalf("secret leaked: %s", o)
		}
	}
	if s.Reveal() != "hunter2" || s.IsEmpty() || !(Secret{}).IsEmpty() || (Secret{}).String() != "" {
		t.Fatal("Reveal/IsEmpty")
	}
	var back Secret
	if err := json.Unmarshal([]byte(`"abc"`), &back); err != nil || back.Reveal() != "abc" {
		t.Fatal("UnmarshalJSON")
	}
	if err := json.Unmarshal([]byte(`12`), &back); err == nil || strings.Contains(err.Error(), "12") {
		t.Fatalf("UnmarshalJSON error %v", err)
	}
}

type valuer struct{ v any }

func (v valuer) Value() (driver.Value, error) { return v.v, nil }

func TestEncodeCells(t *testing.T) {
	ts := time.Date(2024, 1, 2, 3, 4, 5, 6, time.UTC)
	cases := []struct {
		in   any
		want string
	}{
		{nil, `null`},
		{true, `true`},
		{"a<b>&\u2028", `"a<b>&\u2028"`},
		{"bad\xff", "\"bad�\""},
		{[]byte("raw"), `"raw"`},
		{int64(1 << 53), `9007199254740992`},
		{int64(1<<53 + 1), `"9007199254740993"`},
		{int64(-1<<53 - 1), `"-9007199254740993"`},
		{uint64(math.MaxUint64), `"18446744073709551615"`},
		{1.5, `1.5`},
		{1e21, `1e+21`},
		{math.NaN(), `"NaN"`},
		{math.Inf(-1), `"-Inf"`},
		{ts, `"2024-01-02T03:04:05.000000006Z"`},
		{json.Number("12345678901234567890"), `"12345678901234567890"`},
		{json.Number("1.25"), `1.25`},
		{big.NewInt(7), `7`},
		{json.RawMessage(`{"b":1,"a":[9007199254740993]}`), `{"a":["9007199254740993"],"b":1}`},
		{map[string]any{"z": 1, "a": []any{"x"}}, `{"a":["x"],"z":1}`},
		{valuer{int64(3)}, `3`},
		{struct {
			A int `json:"a"`
		}{2}, `{"a":2}`},
		{[]int{1, 2}, `[1,2]`},
	}
	for _, c := range cases {
		if got := string(appendCell(nil, c.in)); got != c.want {
			t.Errorf("%#v: got %s want %s", c.in, got, c.want)
		}
	}
	deep := any("x")
	for range 20 {
		deep = []any{deep}
	}
	out := appendCell(nil, deep)
	if !json.Valid(out) || !strings.HasPrefix(string(out), strings.Repeat("[", maxCellDepth)+`"`) {
		t.Fatalf("deep value not limited: %s", out)
	}
}

func TestRPCErrorScrubsAndClips(t *testing.T) {
	e := rpcError(&Error{Kind: KindDatabase, Message: "auth failed for pw=hunter2 " + strings.Repeat("x", 5000)}, func(s string) string {
		return strings.ReplaceAll(s, "hunter2", Redacted)
	})
	if strings.Contains(e.Message, "hunter2") || len([]rune(e.Message)) != maxErrorRunes || string(e.Data) != `{"kind":"database"}` || e.Code != -32000 {
		t.Fatalf("%+v", e)
	}
	if rpcError(Errorf(KindBusy, "b"), nil).Code != -32029 || rpcError(Errorf(KindInvalid, "i"), nil).Code != -32602 {
		t.Fatal("codes")
	}
}
